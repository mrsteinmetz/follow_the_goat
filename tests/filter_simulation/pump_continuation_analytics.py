#!/usr/bin/env python3
"""
Pump Continuation Analytics - Buyin-Based Filter Discovery
===========================================================
Analyses ACTUAL buyins from `follow_the_goat_buyins` and their matched trail
data from `buyin_trail_minutes` to discover which filter combinations most
reliably predict that a rising price will continue up ≥ 0.2%.

How it works:
  1. Loads buyins with potential_gains + their trail data (minute=0 snapshot)
  2. Filters to "rising" buyins (pre_entry_change_1m > 0)
  3. Labels: continuation (potential_gains > threshold) vs reversal
  4. Phase 1: Section-level analysis (which data categories matter most)
  5. Phase 2: Individual filter ranking (Cohen's d + Youden's J)
  6. Phase 3: Combination discovery (pairs/triples/quads + greedy)
  7. Phase 4: Temporal stability (4-hour windows)
  8. Phase 5: "Force behind" deep-dive (buying pressure indicators)
  9. Phase 6: Save results (CSV + JSON)

Each buyin already has perfectly-matched trail data (250+ columns) covering:
  - Order book state (ob_*)      - Whale activity (wh_*)
  - Transaction flow (tx_*)      - Price momentum (pm_*)
  - Cross-asset (xa_*)           - Patterns (pat_*)
  - Micro patterns (mp_*)        - Composite scores (mm_*)

Usage:
    python tests/filter_simulation/pump_continuation_analytics.py
    python tests/filter_simulation/pump_continuation_analytics.py --hours 48 --min-gain 0.2
    python tests/filter_simulation/pump_continuation_analytics.py --test
"""

import sys
import csv
import json
import time
import logging
import argparse
import warnings
from pathlib import Path
from itertools import combinations
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings("ignore", category=FutureWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pump_continuation_analytics")

# =============================================================================
# CONSTANTS
# =============================================================================

TRADE_COST_PCT = 0.1           # Cost per wrong trade (%)
DEFAULT_MIN_GAIN_PCT = 0.2     # Min gain to label as continuation
DEFAULT_HOURS = 24             # Default lookback window
TRAIN_FRAC = 0.70              # Train/test time-based split
MIN_PRECISION = 40.0           # Min test precision for combinations
MIN_SIGNALS = 15               # Min signals for a combo to count

# Columns from buyin_trail_minutes to never use as filters
SKIP_COLUMNS = frozenset([
    'buyin_id', 'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good', 'label', 'created_at', 'pre_entry_trend',
    'max_fwd_return', 'min_fwd_return',
])

# Absolute price columns -- won't generalize across time
ABSOLUTE_PRICE_COLUMNS = frozenset([
    'pm_open_price', 'pm_close_price', 'pm_high_price', 'pm_low_price', 'pm_avg_price',
    'btc_open_price', 'btc_close_price', 'btc_high_price', 'btc_low_price',
    'eth_open_price', 'eth_close_price', 'eth_high_price', 'eth_low_price',
    'sp_min_price', 'sp_max_price', 'sp_avg_price', 'sp_start_price', 'sp_end_price',
    'sp_price_count',
    'ts_open_price', 'ts_close_price', 'ts_high_price', 'ts_low_price',
    'pre_entry_price_1m_before', 'pre_entry_price_2m_before',
    'pre_entry_price_3m_before', 'pre_entry_price_5m_before',
    'pre_entry_price_10m_before',
    'ob_mid_price', 'ob_total_liquidity', 'ob_bid_total', 'ob_ask_total',
    'tx_vwap', 'tx_total_volume_usd', 'tx_buy_volume_usd', 'tx_sell_volume_usd',
    'tx_delta_divergence', 'tx_cumulative_delta',
    'wh_total_sol_moved', 'wh_inflow_sol', 'wh_outflow_sol',
    'pat_asc_tri_resistance_level', 'pat_asc_tri_support_level',
    'pat_inv_hs_neckline', 'pat_cup_handle_rim',
])

# "Force behind" columns -- buying pressure indicators we care about most
FORCE_BEHIND_COLUMNS = {
    'Order Book Pressure': [
        'ob_volume_imbalance', 'ob_imbalance_shift_1m', 'ob_imbalance_trend_3m',
        'ob_depth_imbalance_ratio', 'ob_depth_imbalance_pct',
        'ob_bid_liquidity_share_pct', 'ob_net_flow_5m',
        'ob_net_flow_to_liquidity_ratio', 'ob_aggression_ratio',
        'ob_imbalance_velocity_1m', 'ob_imbalance_acceleration',
        'ob_cumulative_imbalance_5m', 'ob_imbalance_consistency_5m',
    ],
    'Transaction Buy Pressure': [
        'tx_buy_sell_pressure', 'tx_buy_volume_pct', 'tx_pressure_shift_1m',
        'tx_pressure_trend_3m', 'tx_long_short_ratio', 'tx_long_volume_pct',
        'tx_cumulative_buy_flow_5m', 'tx_volume_acceleration_ratio',
        'tx_volume_surge_ratio', 'tx_whale_volume_pct',
        'tx_aggressive_buy_ratio', 'tx_cumulative_delta_5m',
        'tx_volume_velocity', 'tx_volume_acceleration',
        'tx_aggression_imbalance',
    ],
    'Whale Accumulation': [
        'wh_net_flow_ratio', 'wh_flow_shift_1m', 'wh_flow_trend_3m',
        'wh_accumulation_ratio', 'wh_strong_accumulation_pct',
        'wh_net_flow_strength_pct', 'wh_cumulative_flow_5m',
        'wh_inflow_share_pct', 'wh_flow_velocity', 'wh_flow_acceleration',
        'wh_cumulative_flow_10m', 'wh_stealth_acc_score',
    ],
    'Price Momentum': [
        'pm_price_change_1m', 'pm_price_change_5m', 'pm_momentum_acceleration_1m',
        'pm_breakout_strength_10m', 'pm_price_velocity_1m', 'pm_price_velocity_30s',
        'pm_velocity_acceleration', 'pm_momentum_persistence',
        'pm_trend_strength_ema', 'pm_higher_highs_5m', 'pm_higher_lows_5m',
        'pm_breakout_imminence',
    ],
    'Cross-Asset Alignment': [
        'xa_btc_sol_divergence', 'xa_eth_sol_divergence',
        'xa_momentum_alignment', 'xa_btc_leads_sol_1', 'xa_btc_leads_sol_2',
        'xa_eth_leads_sol_1',
    ],
    'Composite Scores': [
        'mm_probability', 'mm_direction', 'mm_confidence',
        'mm_order_flow_score', 'mm_whale_alignment', 'mm_momentum_quality',
        'mm_cross_asset_score',
    ],
}

RESULTS_DIR = Path(__file__).parent / "results"


# =============================================================================
# DUCKDB CONNECTION TO POSTGRES
# =============================================================================

def _get_pg_connection_string() -> str:
    """Build a libpq connection string from project config."""
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


# =============================================================================
# PHASE 0: LOAD BUYINS + TRAIL DATA
# =============================================================================

def load_buyin_data(con: duckdb.DuckDBPyConnection, hours: int) -> Dict[str, Any]:
    """
    Load buyins and their matched trail data directly from PostgreSQL.
    Each buyin already has its own trail snapshot -- no joining by timestamp needed.
    """
    logger.info(f"[Phase 0] Loading buyins + trail data ({hours}h)...")
    t0 = time.time()

    pg_conn = _get_pg_connection_string()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    # Step 1: Load buyins with outcome data
    logger.info("  [1/3] Loading buyins with potential_gains...")
    t1 = time.time()
    con.execute(f"""
        CREATE TABLE buyins AS
        SELECT
            id AS buyin_id,
            followed_at,
            our_entry_price,
            potential_gains,
            our_status,
            price_cycle
        FROM pg.follow_the_goat_buyins
        WHERE potential_gains IS NOT NULL
          AND our_entry_price > 0
          AND followed_at >= (NOW()::TIMESTAMP - INTERVAL '{hours} hours')
        ORDER BY followed_at
    """)
    n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
    logger.info(f"    {n_buyins:,} buyins loaded in {time.time()-t1:.1f}s")

    if n_buyins < 100:
        logger.error(f"Only {n_buyins} buyins found -- aborting")
        con.execute("DETACH pg")
        return {'n_buyins': n_buyins, 'error': 'insufficient data'}

    # Step 2: Load trail data at minute=0 (entry snapshot)
    logger.info("  [2/3] Loading trail data (minute=0 entry snapshot)...")
    t2 = time.time()
    con.execute("""
        CREATE TABLE trail AS
        SELECT *
        FROM pg.buyin_trail_minutes
        WHERE buyin_id IN (SELECT buyin_id FROM buyins)
          AND minute = 0
          AND COALESCE(sub_minute, 0) = 0
    """)
    n_trail = con.execute("SELECT COUNT(*) FROM trail").fetchone()[0]
    logger.info(f"    {n_trail:,} trail rows loaded in {time.time()-t2:.1f}s")

    # Step 3: Merge buyins + trail
    logger.info("  [3/3] Merging buyins with trail data...")
    t3 = time.time()
    con.execute("""
        CREATE TABLE merged AS
        SELECT
            b.buyin_id,
            b.followed_at,
            b.our_entry_price,
            b.potential_gains,
            b.our_status,
            b.price_cycle,
            t.*
        FROM buyins b
        INNER JOIN trail t ON t.buyin_id = b.buyin_id
    """)
    n_merged = con.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
    logger.info(f"    {n_merged:,} merged rows in {time.time()-t3:.1f}s")

    con.execute("DETACH pg")

    elapsed = time.time() - t0
    logger.info(f"  All data loaded in {elapsed:.1f}s")

    return {
        'n_buyins': n_buyins,
        'n_trail': n_trail,
        'n_merged': n_merged,
    }


def label_buyins(
    con: duckdb.DuckDBPyConnection,
    min_gain_pct: float,
) -> pd.DataFrame:
    """
    Label each buyin based on whether the price continued upward.

    Labels:
      pump_continuation: price WAS rising AND continued up >= min_gain_pct
      pump_reversal:     price WAS rising but did NOT continue
      no_pump:           price was NOT rising at entry

    "Rising" = pre_entry_change_1m > 0 (price went up in the minute before entry)
    """
    logger.info(f"Labeling buyins (min_gain={min_gain_pct}%)...")
    t0 = time.time()

    df = con.execute(f"""
        SELECT
            *,
            CASE
                WHEN pre_entry_change_1m > 0
                     AND potential_gains >= {min_gain_pct}
                    THEN 'pump_continuation'
                WHEN pre_entry_change_1m > 0
                     AND potential_gains < {min_gain_pct}
                    THEN 'pump_reversal'
                ELSE 'no_pump'
            END AS label
        FROM merged
        ORDER BY followed_at
    """).fetchdf()

    # Log label distribution
    n_total = len(df)
    label_counts = df['label'].value_counts()

    logger.info(f"  Total buyins: {n_total:,}")
    for lbl in ['pump_continuation', 'pump_reversal', 'no_pump']:
        cnt = label_counts.get(lbl, 0)
        logger.info(f"    {lbl}: {cnt:,} ({cnt/max(n_total,1)*100:.1f}%)")

    n_climbing = label_counts.get('pump_continuation', 0) + label_counts.get('pump_reversal', 0)
    n_cont = label_counts.get('pump_continuation', 0)
    if n_climbing > 0:
        base_prec = n_cont / n_climbing * 100
        logger.info(f"  Baseline precision (among rising buyins): {base_prec:.1f}%")

    logger.info(f"  Labeled in {time.time()-t0:.1f}s")
    return df


# =============================================================================
# HELPERS
# =============================================================================

def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    """Return numeric columns suitable for filter analysis."""
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if col in ('label', 'followed_at', 'buyin_id', 'our_entry_price',
                    'our_status', 'price_cycle', 'potential_gains', 'minute',
                    'sub_minute', 'interval_idx', 'created_at'):
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        if df[col].isna().mean() >= 0.90:
            continue
        cols.append(col)
    return sorted(cols)


def get_section(col: str) -> str:
    """Get the section prefix for a column."""
    for prefix in ['pre_entry_', 'pm_', 'ob_', 'tx_', 'wh_', 'xa_', 'mm_',
                    'pat_', 'mp_', 'sp_', 'ts_', 'btc_', 'eth_']:
        if col.startswith(prefix):
            return prefix.rstrip('_')
    return 'other'


# =============================================================================
# PHASE 1: SECTION-LEVEL ANALYSIS
# =============================================================================

def analyze_sections(df: pd.DataFrame, min_gain_pct: float) -> Dict[str, Any]:
    """
    Analyze which data sections (ob_, wh_, tx_, etc.) matter most.
    Shows baseline stats and per-section mean differences.
    """
    logger.info("\n" + "=" * 100)
    logger.info("  PHASE 1: DATA OVERVIEW + SECTION ANALYSIS")
    logger.info("=" * 100)

    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    n_climbing = len(climbing_df)
    if n_climbing < 50:
        logger.warning(f"Only {n_climbing} rising buyins -- skipping")
        return {}

    is_cont = (climbing_df['label'] == 'pump_continuation').values
    n_cont = int(is_cont.sum())
    n_rev = n_climbing - n_cont
    base_prec = n_cont / n_climbing * 100

    print()
    print("=" * 100)
    print("  PHASE 1: DATA OVERVIEW")
    print("=" * 100)
    print(f"\n  Total buyins:          {len(df):,}")
    print(f"  Rising at entry:       {n_climbing:,} ({n_climbing/len(df)*100:.1f}%)")
    print(f"    → Continued (>{min_gain_pct}%): {n_cont:,}")
    print(f"    → Reversed:          {n_rev:,}")
    print(f"  Baseline precision:    {base_prec:.1f}%")
    print(f"  (if we buy every rising moment, {base_prec:.1f}% of the time price continues >{min_gain_pct}%)")

    # Gains distribution among rising buyins
    print(f"\n  --- Potential Gains Distribution (rising buyins) ---")
    gains = climbing_df['potential_gains']
    for pct in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    P{pct:2d}: {gains.quantile(pct/100):+.4f}%")

    # Average gains for continuation vs reversal
    avg_cont = climbing_df.loc[is_cont, 'potential_gains'].mean()
    avg_rev = climbing_df.loc[~is_cont, 'potential_gains'].mean()
    print(f"\n  Avg gain (continuation): {avg_cont:+.4f}%")
    print(f"  Avg gain (reversal):     {avg_rev:+.4f}%")

    # Section analysis: which sections have the most differentiating columns
    columns = get_filterable_columns(climbing_df)
    section_stats: Dict[str, Dict] = {}

    for col in columns:
        sec = get_section(col)
        if sec not in section_stats:
            section_stats[sec] = {'n_cols': 0, 'n_significant': 0, 'best_d': 0, 'best_col': ''}

        section_stats[sec]['n_cols'] += 1

        cont_vals = climbing_df.loc[is_cont, col].dropna()
        rev_vals = climbing_df.loc[~is_cont, col].dropna()
        if len(cont_vals) < 20 or len(rev_vals) < 20:
            continue

        pooled_std = np.sqrt(
            ((len(cont_vals) - 1) * cont_vals.std()**2 + (len(rev_vals) - 1) * rev_vals.std()**2)
            / (len(cont_vals) + len(rev_vals) - 2)
        )
        if pooled_std == 0:
            continue
        d = abs(cont_vals.mean() - rev_vals.mean()) / pooled_std
        if d > 0.05:
            section_stats[sec]['n_significant'] += 1
        if d > section_stats[sec]['best_d']:
            section_stats[sec]['best_d'] = d
            section_stats[sec]['best_col'] = col

    print(f"\n  --- Section Differentiation Summary ---")
    print(f"  {'Section':<15}  {'Columns':>8}  {'Signif':>7}  {'Best d':>8}  Best Column")
    print(f"  {'-------':<15}  {'-------':>8}  {'------':>7}  {'------':>8}  -----------")

    for sec in sorted(section_stats, key=lambda s: section_stats[s]['best_d'], reverse=True):
        s = section_stats[sec]
        print(f"  {sec:<15}  {s['n_cols']:>8}  {s['n_significant']:>7}  "
              f"{s['best_d']:>8.4f}  {s['best_col']}")

    print()
    return {
        'n_climbing': n_climbing,
        'n_cont': n_cont,
        'base_precision': base_prec,
        'section_stats': section_stats,
    }


# =============================================================================
# PHASE 2: INDIVIDUAL FILTER RANKING
# =============================================================================

def rank_filters(
    df: pd.DataFrame,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """
    Rank each filter column by expected profit per trade.
    Uses Cohen's d + Youden's J for optimal cutpoint.
    """
    is_cont = (df['label'] == 'pump_continuation').values
    is_rev = (df['label'] == 'pump_reversal').values

    results: List[Dict[str, Any]] = []

    for col in columns:
        cont_vals = df.loc[is_cont, col].dropna()
        rev_vals = df.loc[is_rev, col].dropna()

        if len(cont_vals) < 20 or len(rev_vals) < 20:
            continue

        # Cohen's d
        n_c, n_r = len(cont_vals), len(rev_vals)
        pooled_std = np.sqrt(
            ((n_c - 1) * cont_vals.std() ** 2 + (n_r - 1) * rev_vals.std() ** 2)
            / (n_c + n_r - 2)
        )
        if pooled_std == 0:
            continue
        cohens_d = abs(cont_vals.mean() - rev_vals.mean()) / pooled_std
        if cohens_d < 0.02:
            continue

        # Youden's J for optimal threshold
        all_vals = pd.concat([cont_vals, rev_vals])
        thresholds = np.quantile(all_vals.dropna(), np.linspace(0.05, 0.95, 40))

        best_j, best_cut, best_dir = -1.0, None, None
        for cut in thresholds:
            j_above = float((cont_vals >= cut).mean() + (rev_vals < cut).mean() - 1)
            j_below = float((cont_vals <= cut).mean() + (rev_vals > cut).mean() - 1)
            if j_above > best_j:
                best_j, best_cut, best_dir = j_above, cut, 'above'
            if j_below > best_j:
                best_j, best_cut, best_dir = j_below, cut, 'below'

        if best_cut is None or best_j <= 0:
            continue

        if best_dir == 'above':
            from_val = float(best_cut)
            to_val = float(cont_vals.quantile(0.98))
        else:
            from_val = float(cont_vals.quantile(0.02))
            to_val = float(best_cut)

        if from_val >= to_val:
            continue

        # Expected profit for this filter
        vals = df[col].values
        mask_pass = (vals >= from_val) & (vals <= to_val) & ~np.isnan(vals)
        cont_pass = int((is_cont & mask_pass).sum())
        rev_pass = int((is_rev & mask_pass).sum())
        total_pass = cont_pass + rev_pass
        if total_pass < 10:
            continue

        precision = cont_pass / total_pass * 100
        pass_cont_mask = is_cont & mask_pass
        avg_gain = (
            float(df.loc[pass_cont_mask, 'potential_gains'].mean())
            if pass_cont_mask.sum() > 0 else 0
        )
        expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT

        results.append({
            'column': col,
            'section': get_section(col),
            'from': round(from_val, 8),
            'to': round(to_val, 8),
            'cohens_d': round(cohens_d, 4),
            'youdens_j': round(best_j, 4),
            'direction': best_dir,
            'precision': round(precision, 2),
            'n_signals': total_pass,
            'n_cont_pass': cont_pass,
            'n_rev_pass': rev_pass,
            'avg_gain': round(avg_gain, 4),
            'expected_profit': round(expected_profit, 4),
        })

    results.sort(key=lambda x: x['expected_profit'], reverse=True)
    return results


def run_filter_ranking(df: pd.DataFrame, test_mode: bool = False) -> List[Dict[str, Any]]:
    """Phase 2: Rank individual filters on rising buyins only."""
    logger.info("\n" + "=" * 100)
    logger.info("  PHASE 2: INDIVIDUAL FILTER RANKING")
    logger.info("=" * 100)

    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 50:
        logger.warning(f"Only {len(climbing_df)} rising buyins -- skipping filter ranking")
        return []

    columns = get_filterable_columns(climbing_df)
    if test_mode:
        columns = columns[:50]
    logger.info(f"  {len(columns)} filterable columns found")

    rankings = rank_filters(climbing_df, columns)
    profitable = sum(1 for r in rankings if r['expected_profit'] > 0)
    logger.info(f"  {len(rankings)} filters ranked, {profitable} individually profitable")

    # Print top filters
    show = min(40, len(rankings))
    if show > 0:
        print()
        print("=" * 150)
        print(f"  TOP {show} INDIVIDUAL FILTERS (ranked by expected profit per trade)")
        print("=" * 150)
        print(f"  {'#':>3}  {'Column':<42}  {'Sec':<8}  {'Prec%':>6}  "
              f"{'Signals':>8}  {'AvgGain':>8}  {'E[Profit]':>10}  "
              f"{'Cohen d':>8}  {'J':>6}  Range")
        print(f"  {'--':>3}  {'------':<42}  {'---':<8}  {'-----':>6}  "
              f"{'-------':>8}  {'-------':>8}  {'---------':>10}  "
              f"{'-------':>8}  {'-':>6}  -----")

        for i, r in enumerate(rankings[:show], 1):
            name = r['column'][:42]
            rng = f"[{r['from']:.4g}, {r['to']:.4g}]"
            print(f"  {i:3d}  {name:<42}  {r['section']:<8}  {r['precision']:5.1f}%  "
                  f"{r['n_signals']:8,}  {r['avg_gain']:7.4f}%  "
                  f"{r['expected_profit']:+9.4f}%  "
                  f"{r['cohens_d']:8.4f}  {r['youdens_j']:6.4f}  {rng}")

        # Section summary
        sections: Dict[str, Dict[str, int]] = {}
        for r in rankings:
            sec = r['section']
            if sec not in sections:
                sections[sec] = {'total': 0, 'profitable': 0, 'best_profit': -999}
            sections[sec]['total'] += 1
            if r['expected_profit'] > 0:
                sections[sec]['profitable'] += 1
            sections[sec]['best_profit'] = max(sections[sec]['best_profit'], r['expected_profit'])

        print(f"\n  Section Summary:")
        print(f"  {'Section':<12}  {'Total':>6}  {'Profitable':>11}  {'Best E[P]':>10}")
        for sec in sorted(sections, key=lambda s: sections[s]['best_profit'], reverse=True):
            s = sections[sec]
            print(f"  {sec:<12}  {s['total']:>6}  {s['profitable']:>11}  {s['best_profit']:+9.4f}%")
        print()

    return rankings


# =============================================================================
# PHASE 3: COMBINATION DISCOVERY
# =============================================================================

def precompute_masks(
    df: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
) -> Dict[str, np.ndarray]:
    """Pre-compute boolean pass/fail masks for each filter."""
    masks: Dict[str, np.ndarray] = {}
    for feat in ranked_features:
        col = feat['column']
        if col not in df.columns:
            continue
        vals = df[col].values
        masks[col] = (vals >= feat['from']) & (vals <= feat['to']) & ~np.isnan(vals)
    return masks


def score_combo(
    combo_cols: Tuple[str, ...],
    masks: Dict[str, np.ndarray],
    is_cont: np.ndarray,
    is_rev: np.ndarray,
    gains: np.ndarray,
    n_climbing: int,
) -> Optional[Dict[str, Any]]:
    """Score a filter combination by expected profit per trade."""
    combined = masks[combo_cols[0]].copy()
    for col in combo_cols[1:]:
        combined &= masks[col]

    cont_pass = int((is_cont & combined).sum())
    rev_pass = int((is_rev & combined).sum())
    total_pass = cont_pass + rev_pass

    if total_pass < MIN_SIGNALS:
        return None

    precision = cont_pass / total_pass * 100
    pass_cont_mask = is_cont & combined
    avg_gain = (
        float(np.nanmean(gains[pass_cont_mask]))
        if pass_cont_mask.sum() > 0 else 0
    )
    expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT

    return {
        'precision': round(precision, 2),
        'n_signals': total_pass,
        'n_cont_pass': cont_pass,
        'n_rev_pass': rev_pass,
        'avg_gain': round(avg_gain, 4),
        'expected_profit': round(expected_profit, 4),
    }


def find_best_combinations(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
    test_mode: bool = False,
) -> List[Dict[str, Any]]:
    """
    Find optimal filter combinations via exhaustive pairs/triples + greedy.
    Validate on test set; keep only combos profitable on test.
    """
    logger.info("\n" + "=" * 100)
    logger.info("  PHASE 3: COMBINATION DISCOVERY")
    logger.info("=" * 100)

    if len(ranked_features) < 2:
        logger.info("  Not enough ranked features for combinations")
        return []

    t0 = time.time()

    # Prepare train arrays
    is_cont_train = (df_train['label'] == 'pump_continuation').values
    is_rev_train = (df_train['label'] == 'pump_reversal').values
    gains_train = df_train['potential_gains'].values
    n_climbing_train = int(is_cont_train.sum() + is_rev_train.sum())

    # Prepare test arrays
    is_cont_test = (df_test['label'] == 'pump_continuation').values
    is_rev_test = (df_test['label'] == 'pump_reversal').values
    gains_test = df_test['potential_gains'].values
    n_climbing_test = int(is_cont_test.sum() + is_rev_test.sum())

    # Baseline
    base_prec = is_cont_train.sum() / max(n_climbing_train, 1) * 100
    base_gain = float(np.nanmean(gains_train[is_cont_train])) if is_cont_train.sum() > 0 else 0
    base_profit = (base_prec / 100) * base_gain - TRADE_COST_PCT
    logger.info(f"  Train baseline: prec={base_prec:.1f}%, gain={base_gain:.4f}%, "
                f"E[profit]={base_profit:.4f}%")

    # Pre-compute masks
    masks_train = precompute_masks(df_train, ranked_features)
    masks_test = precompute_masks(df_test, ranked_features)

    results: List[Dict[str, Any]] = []
    combos_tested = 0

    def _test_combo(cols: Tuple[str, ...]):
        nonlocal combos_tested
        combos_tested += 1

        if not all(c in masks_train and c in masks_test for c in cols):
            return

        train_m = score_combo(
            cols, masks_train, is_cont_train, is_rev_train, gains_train, n_climbing_train
        )
        if train_m is None or train_m['expected_profit'] <= base_profit or train_m['n_cont_pass'] < 5:
            return

        test_m = score_combo(
            cols, masks_test, is_cont_test, is_rev_test, gains_test, n_climbing_test
        )
        if test_m is None or test_m['expected_profit'] <= 0 or test_m['precision'] < MIN_PRECISION:
            return

        # Build filter ranges
        ranges = []
        for col in cols:
            feat = next((r for r in ranked_features if r['column'] == col), None)
            if feat:
                ranges.append(f"{col}:[{feat['from']},{feat['to']}]")

        results.append({
            'n_filters': len(cols),
            'columns': cols,
            'filter_columns': '|'.join(cols),
            'filter_ranges': '|'.join(ranges),
            'train_precision': train_m['precision'],
            'train_n_signals': train_m['n_signals'],
            'train_avg_gain': train_m['avg_gain'],
            'train_expected_profit': train_m['expected_profit'],
            'test_precision': test_m['precision'],
            'test_n_signals': test_m['n_signals'],
            'test_avg_gain': test_m['avg_gain'],
            'test_expected_profit': test_m['expected_profit'],
            'overfit_delta': round(
                train_m['expected_profit'] - test_m['expected_profit'], 4
            ),
        })

    # Column lists at various top-N levels
    n_avail = len(ranked_features)
    top30 = [r['column'] for r in ranked_features[:min(30, n_avail)]]
    top20 = [r['column'] for r in ranked_features[:min(20, n_avail)]]
    top15 = [r['column'] for r in ranked_features[:min(15, n_avail)]]

    # Exhaustive pairs from top 30
    logger.info(f"  Pairs from top {len(top30)}...")
    for combo in combinations(top30, 2):
        _test_combo(combo)

    if not test_mode:
        # Exhaustive triples from top 20
        logger.info(f"  Triples from top {len(top20)}...")
        for combo in combinations(top20, 3):
            _test_combo(combo)

        # Quads from top 15
        if len(top15) >= 4:
            logger.info(f"  Quads from top {len(top15)}...")
            for combo in combinations(top15, 4):
                _test_combo(combo)

    # Greedy forward selection
    logger.info("  Greedy forward selection...")
    best_cols: List[str] = []
    available = top30[:]
    current_profit = base_profit

    for step in range(min(8, len(available))):
        best_addition = None
        best_profit = current_profit

        for col in available:
            if col in best_cols:
                continue
            candidate = tuple(sorted(best_cols + [col]))
            if not all(c in masks_train for c in candidate):
                continue
            m = score_combo(
                candidate, masks_train, is_cont_train, is_rev_train,
                gains_train, n_climbing_train,
            )
            if m is None or m['n_cont_pass'] < 5:
                continue
            if m['expected_profit'] > best_profit:
                best_profit = m['expected_profit']
                best_addition = col

        if best_addition is None:
            break

        best_cols.append(best_addition)
        current_profit = best_profit
        _test_combo(tuple(sorted(best_cols)))
        logger.info(f"    Step {step+1}: +{best_addition} (train E[profit]={best_profit:.4f}%)")

    elapsed = time.time() - t0
    logger.info(f"  {combos_tested:,} combos tested, {len(results)} profitable on test "
                f"(precision>={MIN_PRECISION}%), {elapsed:.1f}s")

    results.sort(key=lambda x: x['test_expected_profit'], reverse=True)

    # Print top combinations
    show = min(25, len(results))
    if show > 0:
        print()
        print("=" * 160)
        print(f"  TOP {show} FILTER COMBINATIONS (ranked by test expected profit)")
        print("=" * 160)
        print(f"  {'#':>3}  {'NF':>3}  {'TrPrec%':>8}  {'TrProfit':>9}  "
              f"{'TsPrec%':>8}  {'TsProfit':>9}  {'Overfit':>8}  "
              f"{'TsSig':>7}  {'TsGain':>7}  Filters")
        print(f"  {'--':>3}  {'--':>3}  {'-------':>8}  {'--------':>9}  "
              f"{'-------':>8}  {'--------':>9}  {'-------':>8}  "
              f"{'-----':>7}  {'------':>7}  -------")

        for i, r in enumerate(results[:show], 1):
            cols = r['filter_columns'].replace('|', ', ')
            if len(cols) > 60:
                cols = cols[:57] + '...'
            print(f"  {i:3d}  {r['n_filters']:>3d}  {r['train_precision']:7.1f}%  "
                  f"{r['train_expected_profit']:+8.4f}%  "
                  f"{r['test_precision']:7.1f}%  "
                  f"{r['test_expected_profit']:+8.4f}%  "
                  f"{r['overfit_delta']:+7.4f}%  "
                  f"{r['test_n_signals']:7,}  "
                  f"{r['test_avg_gain']:6.4f}%  {cols}")

        # Detail on best result
        if results:
            best = results[0]
            print()
            print("  " + "-" * 80)
            print("  BEST COMBINATION DETAIL:")
            print("  " + "-" * 80)
            print(f"  Filters ({best['n_filters']}):")
            for part in best['filter_ranges'].split('|'):
                print(f"    {part}")
            print(f"\n  Train: prec={best['train_precision']:.1f}%, "
                  f"E[profit]={best['train_expected_profit']:+.4f}%, "
                  f"signals={best['train_n_signals']:,}")
            print(f"  Test:  prec={best['test_precision']:.1f}%, "
                  f"E[profit]={best['test_expected_profit']:+.4f}%, "
                  f"signals={best['test_n_signals']:,}")
            print(f"  Overfit delta: {best['overfit_delta']:+.4f}%")
        print()

    return results


# =============================================================================
# PHASE 4: TEMPORAL STABILITY
# =============================================================================

def check_temporal_stability(
    df: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
    combo_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Test discovered rules across time windows."""
    logger.info("\n" + "=" * 100)
    logger.info("  PHASE 4: TEMPORAL STABILITY")
    logger.info("=" * 100)

    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 100 or 'followed_at' not in climbing_df.columns:
        logger.warning("  Not enough data for temporal stability analysis")
        return []

    # Split into 4-hour windows
    ts_min = climbing_df['followed_at'].min()
    ts_max = climbing_df['followed_at'].max()
    total_hours = (ts_max - ts_min).total_seconds() / 3600
    n_windows = max(2, int(total_hours / 4))
    window_size_h = total_hours / n_windows

    windows = []
    for i in range(n_windows):
        w_start = ts_min + pd.Timedelta(hours=i * window_size_h)
        w_end = ts_min + pd.Timedelta(hours=(i + 1) * window_size_h)
        w_df = climbing_df[(climbing_df['followed_at'] >= w_start) & (climbing_df['followed_at'] < w_end)]
        if len(w_df) >= 20:
            windows.append({
                'idx': i,
                'start': w_start,
                'end': w_end,
                'df': w_df,
                'n_obs': len(w_df),
            })

    if len(windows) < 2:
        logger.warning(f"  Only {len(windows)} windows with enough data -- skipping")
        return []

    logger.info(f"  {len(windows)} temporal windows of ~{window_size_h:.1f}h each")

    # Test top combo results
    stability_results = []
    combos_to_test = combo_results[:min(15, len(combo_results))]

    if not combos_to_test:
        logger.info("  No combinations to test for stability")
        return []

    print()
    print("=" * 130)
    print(f"  TEMPORAL STABILITY (top {len(combos_to_test)} combos across {len(windows)} windows)")
    print("=" * 130)

    for combo in combos_to_test:
        cols = combo['columns']
        feat_lookup = {r['column']: r for r in ranked_features}

        window_metrics = []
        for w in windows:
            w_df = w['df']
            is_cont_w = (w_df['label'] == 'pump_continuation').values
            is_rev_w = (w_df['label'] == 'pump_reversal').values

            combined = np.ones(len(w_df), dtype=bool)
            all_cols_present = True
            for col in cols:
                feat = feat_lookup.get(col)
                if feat is None or col not in w_df.columns:
                    all_cols_present = False
                    break
                vals = w_df[col].values
                combined &= (vals >= feat['from']) & (vals <= feat['to']) & ~np.isnan(vals)

            if not all_cols_present:
                window_metrics.append({'window': w['idx'], 'precision': None, 'n_signals': 0})
                continue

            cont_pass = int((is_cont_w & combined).sum())
            total_pass = int(combined.sum())
            prec = cont_pass / total_pass * 100 if total_pass > 0 else 0

            window_metrics.append({
                'window': w['idx'],
                'start': w['start'].strftime('%H:%M'),
                'end': w['end'].strftime('%H:%M'),
                'n_obs': w['n_obs'],
                'n_signals': total_pass,
                'precision': round(prec, 1) if total_pass > 0 else None,
            })

        valid_precs = [m['precision'] for m in window_metrics if m['precision'] is not None]
        if len(valid_precs) >= 2:
            stability_std = np.std(valid_precs)
            stability_min = min(valid_precs)
            stability_mean = np.mean(valid_precs)
            is_stable = stability_min >= 25.0 and stability_std < 20.0
        else:
            stability_std = float('inf')
            stability_min = 0
            stability_mean = 0
            is_stable = False

        stability_results.append({
            'filter_columns': combo['filter_columns'],
            'test_precision': combo['test_precision'],
            'window_metrics': window_metrics,
            'stability_std': round(stability_std, 2),
            'stability_min': round(stability_min, 1),
            'stability_mean': round(stability_mean, 1),
            'is_stable': is_stable,
        })

        stable_tag = "STABLE" if is_stable else "UNSTABLE"
        cols_str = combo['filter_columns'].replace('|', ', ')
        if len(cols_str) > 65:
            cols_str = cols_str[:62] + '...'
        print(f"\n  [{stable_tag}] {cols_str}")
        print(f"    Test prec: {combo['test_precision']:.1f}% | "
              f"Stability: mean={stability_mean:.1f}%, min={stability_min:.1f}%, std={stability_std:.1f}")
        for m in window_metrics:
            prec_str = f"{m['precision']:.1f}%" if m['precision'] is not None else "N/A"
            print(f"      Window {m.get('start', '?')}-{m.get('end', '?')}: "
                  f"{m['n_obs']:,} obs, {m['n_signals']} signals, prec={prec_str}")

    print()
    return stability_results


# =============================================================================
# PHASE 5: "FORCE BEHIND" DEEP-DIVE
# =============================================================================

def analyze_force_behind(
    df: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Deep analysis of buying pressure indicators specifically.
    Shows exactly which "force behind" signals predict continuation.
    """
    logger.info("\n" + "=" * 100)
    logger.info("  PHASE 5: FORCE BEHIND THE MOVE - BUYING PRESSURE DEEP-DIVE")
    logger.info("=" * 100)

    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 50:
        return {}

    is_cont = (climbing_df['label'] == 'pump_continuation').values
    n_cont = int(is_cont.sum())
    n_total = len(climbing_df)
    base_prec = n_cont / n_total * 100

    print()
    print("=" * 120)
    print("  PHASE 5: FORCE BEHIND THE MOVE - BUYING PRESSURE DEEP-DIVE")
    print("=" * 120)
    print(f"\n  Baseline: {base_prec:.1f}% of rising buyins continue >{DEFAULT_MIN_GAIN_PCT}%")
    print(f"  Question: Which buying pressure indicators push this precision higher?")

    ranked_lookup = {r['column']: r for r in ranked_features}
    force_results = {}

    for category, cols in FORCE_BEHIND_COLUMNS.items():
        print(f"\n  --- {category} ---")
        print(f"  {'Column':<45}  {'Rank':>5}  {'Prec%':>6}  {'Signals':>8}  "
              f"{'E[Profit]':>10}  {'Cohen d':>8}  Direction  Range")
        print(f"  {'------':<45}  {'----':>5}  {'-----':>6}  {'-------':>8}  "
              f"{'--------':>10}  {'-------':>8}  ---------  -----")

        category_results = []
        for col in cols:
            if col not in climbing_df.columns:
                continue
            r = ranked_lookup.get(col)
            if r:
                rank = ranked_features.index(r) + 1
                rng = f"[{r['from']:.4g}, {r['to']:.4g}]"
                print(f"  {col:<45}  {rank:5d}  {r['precision']:5.1f}%  "
                      f"{r['n_signals']:8,}  {r['expected_profit']:+9.4f}%  "
                      f"{r['cohens_d']:8.4f}  {r['direction']:<9}  {rng}")
                category_results.append(r)
            else:
                # Column exists but didn't pass threshold -- show raw stats
                cont_vals = climbing_df.loc[is_cont, col].dropna()
                rev_vals = climbing_df.loc[~is_cont, col].dropna()
                if len(cont_vals) >= 10 and len(rev_vals) >= 10:
                    cont_mean = cont_vals.mean()
                    rev_mean = rev_vals.mean()
                    diff_dir = "cont > rev" if cont_mean > rev_mean else "rev > cont"
                    print(f"  {col:<45}    ---    ---       ---        ---       ---  "
                          f"{diff_dir}")

        # Per-category summary
        if category_results:
            best = max(category_results, key=lambda x: x['expected_profit'])
            n_profitable = sum(1 for r in category_results if r['expected_profit'] > 0)
            print(f"\n  Summary: {len(category_results)} ranked, "
                  f"{n_profitable} profitable, best: {best['column']} "
                  f"(E[P]={best['expected_profit']:+.4f}%)")
        else:
            print(f"\n  Summary: No columns passed ranking threshold")

        force_results[category] = category_results

    # Overall force-behind summary
    all_force = []
    for cat_results in force_results.values():
        all_force.extend(cat_results)
    all_force.sort(key=lambda x: x['expected_profit'], reverse=True)

    if all_force:
        print(f"\n  {'=' * 80}")
        print(f"  TOP 15 'FORCE BEHIND' INDICATORS (across all pressure categories)")
        print(f"  {'=' * 80}")
        print(f"  {'#':>3}  {'Column':<42}  {'Category':<22}  {'Prec%':>6}  "
              f"{'Signals':>8}  {'E[Profit]':>10}")
        print(f"  {'--':>3}  {'------':<42}  {'--------':<22}  {'-----':>6}  "
              f"{'-------':>8}  {'---------':>10}")

        for i, r in enumerate(all_force[:15], 1):
            # Find category
            cat = 'Unknown'
            for category, cols in FORCE_BEHIND_COLUMNS.items():
                if r['column'] in cols:
                    cat = category[:22]
                    break
            print(f"  {i:3d}  {r['column']:<42}  {cat:<22}  {r['precision']:5.1f}%  "
                  f"{r['n_signals']:8,}  {r['expected_profit']:+9.4f}%")

    print()
    return force_results


# =============================================================================
# PHASE 6: SAVE RESULTS
# =============================================================================

def save_results(
    filter_rankings: List[Dict[str, Any]],
    combo_results: List[Dict[str, Any]],
    stability_results: List[Dict[str, Any]],
    section_stats: Dict[str, Any],
    force_results: Dict[str, Any],
    timestamp: str,
):
    """Save results to CSV and JSON files."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # CSV: Filter rankings
    if filter_rankings:
        fields = ['rank', 'column', 'section', 'from', 'to', 'precision', 'n_signals',
                  'avg_gain', 'expected_profit', 'cohens_d', 'youdens_j', 'direction']
        path = RESULTS_DIR / f"pump_analytics_filters_{timestamp}.csv"
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rank, r in enumerate(filter_rankings, 1):
                row = {**r, 'rank': rank}
                w.writerow({k: row.get(k, '') for k in fields})
        latest = RESULTS_DIR / "pump_analytics_filters_latest.csv"
        with open(latest, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rank, r in enumerate(filter_rankings, 1):
                row = {**r, 'rank': rank}
                w.writerow({k: row.get(k, '') for k in fields})
        logger.info(f"  Saved {len(filter_rankings)} filters -> {path.name}")

    # CSV: Combinations
    if combo_results:
        fields = ['rank', 'n_filters', 'filter_columns', 'filter_ranges',
                  'train_precision', 'train_expected_profit',
                  'test_precision', 'test_expected_profit', 'test_n_signals',
                  'test_avg_gain', 'overfit_delta']
        path = RESULTS_DIR / f"pump_analytics_combos_{timestamp}.csv"
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rank, r in enumerate(combo_results, 1):
                row = {**r, 'rank': rank}
                w.writerow({k: row.get(k, '') for k in fields})
        latest = RESULTS_DIR / "pump_analytics_combos_latest.csv"
        with open(latest, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rank, r in enumerate(combo_results, 1):
                row = {**r, 'rank': rank}
                w.writerow({k: row.get(k, '') for k in fields})
        logger.info(f"  Saved {len(combo_results)} combos -> {path.name}")

    # JSON: Actionable rules
    json_output = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'analysis_type': 'buyin_based_filter_discovery',
        'section_stats': {
            'n_climbing': section_stats.get('n_climbing', 0),
            'base_precision': section_stats.get('base_precision', 0),
        },
        'top_individual_filters': [
            {
                'column': r['column'],
                'section': r['section'],
                'from_val': r['from'],
                'to_val': r['to'],
                'precision': r['precision'],
                'expected_profit': r['expected_profit'],
                'cohens_d': r['cohens_d'],
                'n_signals': r['n_signals'],
            }
            for r in filter_rankings[:25]
        ] if filter_rankings else [],
        'top_combinations': [
            {
                'columns': list(r['columns']) if 'columns' in r else r['filter_columns'].split('|'),
                'filter_ranges': r['filter_ranges'],
                'test_precision': r['test_precision'],
                'test_expected_profit': r['test_expected_profit'],
                'test_n_signals': r['test_n_signals'],
                'overfit_delta': r['overfit_delta'],
                'is_stable': bool(next(
                    (s['is_stable'] for s in stability_results
                     if s['filter_columns'] == r['filter_columns']),
                    False
                )),
            }
            for r in combo_results[:15]
        ] if combo_results else [],
        'force_behind_summary': {},
        'recommended_rules': [],
    }

    # Force behind summary
    for category, cat_results in (force_results or {}).items():
        if cat_results:
            best = max(cat_results, key=lambda x: x['expected_profit'])
            json_output['force_behind_summary'][category] = {
                'n_ranked': len(cat_results),
                'n_profitable': sum(1 for r in cat_results if r['expected_profit'] > 0),
                'best_column': best['column'],
                'best_precision': best['precision'],
                'best_expected_profit': best['expected_profit'],
            }

    # Recommended rules: best stable combo, or best combo if none stable
    if combo_results:
        stable_combos = [
            r for r in combo_results
            if any(s['filter_columns'] == r['filter_columns'] and s['is_stable']
                   for s in stability_results)
        ]
        best = stable_combos[0] if stable_combos else combo_results[0]
        cols = list(best['columns']) if 'columns' in best else best['filter_columns'].split('|')
        rules = []
        for col in cols:
            feat = next((r for r in filter_rankings if r['column'] == col), None)
            if feat:
                rules.append({
                    'column': col,
                    'section': feat['section'],
                    'from_val': feat['from'],
                    'to_val': feat['to'],
                })
        json_output['recommended_rules'] = rules
        json_output['recommended_metadata'] = {
            'test_precision': best['test_precision'],
            'test_expected_profit': best['test_expected_profit'],
            'test_n_signals': best['test_n_signals'],
            'is_stable': bool(stable_combos),
        }

    path = RESULTS_DIR / f"pump_analytics_{timestamp}.json"
    with open(path, 'w') as f:
        json.dump(json_output, f, indent=2, default=str)
    latest = RESULTS_DIR / "pump_analytics_latest.json"
    with open(latest, 'w') as f:
        json.dump(json_output, f, indent=2, default=str)
    logger.info(f"  Saved actionable rules -> {path.name}")


def print_final_summary(
    load_stats: Dict[str, Any],
    section_stats: Dict[str, Any],
    filter_rankings: List[Dict[str, Any]],
    combo_results: List[Dict[str, Any]],
    stability_results: List[Dict[str, Any]],
    elapsed: float,
):
    """Print final summary."""
    print()
    print("=" * 100)
    print("  ANALYSIS COMPLETE")
    print("=" * 100)
    print(f"  Runtime:           {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Total buyins:      {load_stats.get('n_buyins', 0):,}")
    print(f"  With trail data:   {load_stats.get('n_merged', 0):,}")

    n_climbing = section_stats.get('n_climbing', 0)
    base_prec = section_stats.get('base_precision', 0)
    print(f"\n  Rising buyins:     {n_climbing:,}")
    print(f"  Baseline precision: {base_prec:.1f}%")

    if filter_rankings:
        profitable = sum(1 for r in filter_rankings if r['expected_profit'] > 0)
        print(f"\n  Filters ranked:    {len(filter_rankings)}")
        print(f"  Profitable:        {profitable}")
        if filter_rankings:
            print(f"  Best filter:       {filter_rankings[0]['column']} "
                  f"(prec={filter_rankings[0]['precision']:.1f}%, "
                  f"E[profit]={filter_rankings[0]['expected_profit']:+.4f}%)")

    if combo_results:
        best = combo_results[0]
        n_stable = sum(1 for s in stability_results if s['is_stable'])
        print(f"\n  Combinations:      {len(combo_results)}")
        print(f"  Temporally stable: {n_stable}")
        print(f"  Best combo:")
        print(f"    Filters:         {best['filter_columns'].replace('|', ', ')}")
        print(f"    Test precision:  {best['test_precision']:.1f}%")
        print(f"    Test E[profit]:  {best['test_expected_profit']:+.4f}%")
        print(f"    Test signals:    {best['test_n_signals']}")
        print(f"    Overfit delta:   {best['overfit_delta']:+.4f}%")
    else:
        print(f"\n  No profitable combinations found on test set.")

    print("=" * 100)
    print()


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def run_analysis(
    hours: int = DEFAULT_HOURS,
    min_gain_pct: float = DEFAULT_MIN_GAIN_PCT,
    test_mode: bool = False,
):
    """Run the full pump continuation analysis pipeline."""
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print()
    print("=" * 100)
    print("  PUMP CONTINUATION ANALYTICS  (Buyin-Based Filter Discovery)")
    print(f"  Started:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Lookback:       {hours}h")
    print(f"  Min gain:       {min_gain_pct}%")
    print(f"  Trade cost:     {TRADE_COST_PCT}%")
    print(f"  Min precision:  {MIN_PRECISION}% (for combinations)")
    print(f"  Min signals:    {MIN_SIGNALS}")
    if test_mode:
        print("  MODE:           TEST (reduced search)")
    print("=" * 100)
    print()
    sys.stdout.flush()

    # ── Phase 0: Load buyins + trail data ─────────────────────────────
    con = duckdb.connect(":memory:")
    load_stats = load_buyin_data(con, hours)

    if load_stats.get('error'):
        logger.error(f"Data loading failed: {load_stats['error']}")
        con.close()
        return

    if load_stats['n_merged'] < 100:
        logger.error(f"Only {load_stats['n_merged']} merged rows -- aborting")
        con.close()
        return
    sys.stdout.flush()

    # ── Label buyins ──────────────────────────────────────────────────
    df = label_buyins(con, min_gain_pct)
    con.close()

    if len(df) == 0:
        logger.error("Empty dataset after labeling. Aborting.")
        return
    sys.stdout.flush()

    # Filter to rising buyins
    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 50:
        logger.error(f"Only {len(climbing_df)} rising buyins (need >= 50). Aborting.")
        return

    # ── Phase 1: Section analysis ─────────────────────────────────────
    section_stats = analyze_sections(df, min_gain_pct)
    sys.stdout.flush()

    # ── Train/Test split (time-based) ─────────────────────────────────
    logger.info("Splitting train/test by time (70/30)...")
    climbing_df = climbing_df.sort_values('followed_at').copy()
    n_train = int(len(climbing_df) * TRAIN_FRAC)
    df_train = climbing_df.iloc[:n_train].copy()
    df_test = climbing_df.iloc[n_train:].copy()

    trn_cont = int((df_train['label'] == 'pump_continuation').sum())
    trn_rev = int((df_train['label'] == 'pump_reversal').sum())
    tst_cont = int((df_test['label'] == 'pump_continuation').sum())
    tst_rev = int((df_test['label'] == 'pump_reversal').sum())

    logger.info(f"  Train: {len(df_train):,} ({trn_cont} cont + {trn_rev} rev, "
                f"prec={trn_cont/max(trn_cont+trn_rev,1)*100:.1f}%)")
    logger.info(f"  Test:  {len(df_test):,} ({tst_cont} cont + {tst_rev} rev, "
                f"prec={tst_cont/max(tst_cont+tst_rev,1)*100:.1f}%)")

    if len(df_test) < 20:
        logger.error("Test set too small. Aborting.")
        return
    sys.stdout.flush()

    # ── Phase 2: Individual filter ranking ────────────────────────────
    filter_rankings = run_filter_ranking(climbing_df, test_mode=test_mode)
    sys.stdout.flush()

    # ── Phase 3: Combination discovery ────────────────────────────────
    if len(filter_rankings) >= 2:
        combo_results = find_best_combinations(
            df_train, df_test, filter_rankings, test_mode=test_mode,
        )
    else:
        combo_results = []
    sys.stdout.flush()

    # ── Phase 4: Temporal stability ───────────────────────────────────
    stability_results = check_temporal_stability(
        df, filter_rankings, combo_results,
    )
    sys.stdout.flush()

    # ── Phase 5: Force behind deep-dive ───────────────────────────────
    force_results = analyze_force_behind(df, filter_rankings)
    sys.stdout.flush()

    # ── Phase 6: Save results ─────────────────────────────────────────
    save_results(
        filter_rankings, combo_results, stability_results,
        section_stats, force_results, timestamp,
    )

    # ── Final summary ─────────────────────────────────────────────────
    elapsed = time.time() - start_time
    print_final_summary(
        load_stats, section_stats, filter_rankings,
        combo_results, stability_results, elapsed,
    )
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Pump Continuation Analytics (Buyin-Based Filter Discovery)",
    )
    parser.add_argument(
        "--hours", type=int, default=DEFAULT_HOURS,
        help=f"Hours of buyin history (default: {DEFAULT_HOURS})",
    )
    parser.add_argument(
        "--min-gain", type=float, default=DEFAULT_MIN_GAIN_PCT,
        help=f"Min gain %% for continuation label (default: {DEFAULT_MIN_GAIN_PCT})",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Quick test mode (reduced search space)",
    )
    args = parser.parse_args()

    run_analysis(
        hours=args.hours,
        min_gain_pct=args.min_gain,
        test_mode=args.test,
    )


if __name__ == "__main__":
    main()

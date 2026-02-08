#!/usr/bin/env python3
"""
Pump Entry Analyzer - Momentum Continuation Detection
======================================================
Detects active upward SOL price movements and identifies filter patterns
that predict the pump will continue -- focused on high-confidence entry timing.

DIFFERENT from overnight_sweep.py:
  - Unit of analysis: every (buyin, minute) moment, not just trade entry
  - Question: "Price IS going up right now -- will it KEEP going?"
  - Cost-aware: 0.1% trade cost baked into all scoring
  - Momentum-focused: only analyzes moments when price is actively climbing

How it works:
  1. Loads buyin trail data (price + 200 filter columns at each minute 0-15)
  2. Computes forward returns via self-join (what happens in the NEXT 1-3 minutes?)
  3. Labels moments: pump_continuation vs pump_reversal (among climbing moments)
  4. Finds filter patterns that distinguish continuation from reversal
  5. Discovers optimal filter combinations with expected profit scoring
  6. Validates on held-out test set (time-based 70/30 split)

Cost model:
  - Each wrong entry costs 0.1% of investment
  - Need >0.2% price move to be profitable
  - Expected profit = P(continuation|filter) * avg_gain - 0.1%

Usage:
    python tests/filter_simulation/pump_entry_analyzer.py
    python tests/filter_simulation/pump_entry_analyzer.py --hours 72 --min-pump-pct 0.2 --forward-window 3
    python tests/filter_simulation/pump_entry_analyzer.py --test  # quick test with fewer features

    # Background:
    nohup python3 tests/filter_simulation/pump_entry_analyzer.py > tests/filter_simulation/results/pump_log.txt 2>&1 &
"""

import sys
import csv
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
from scipy import stats

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings("ignore", category=FutureWarning)

# Setup logging - flush every line for nohup visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("pump_entry_analyzer")

# =============================================================================
# CONSTANTS
# =============================================================================

TRADE_COST_PCT = 0.1        # Cost per wrong trade (%)
DEFAULT_MIN_PUMP_PCT = 0.2  # Min forward return to label as continuation
DEFAULT_HOURS = 72          # Default lookback window
DEFAULT_FORWARD_WINDOW = 3  # Look N minutes ahead for forward returns
TRAIN_FRAC = 0.70           # Train/test time-based split

# Columns to never use as filters (metadata, labels, computed)
SKIP_COLUMNS = frozenset([
    'buyin_id', 'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good', 'label', 'created_at', 'pre_entry_trend',
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

RESULTS_DIR = Path(__file__).parent / "results"


# =============================================================================
# PHASE 1: DATA LOADING
# =============================================================================

def _get_pg_connection_string() -> str:
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


def load_data(hours: int) -> Tuple[Optional[duckdb.DuckDBPyConnection], Dict[str, Any]]:
    """Stream buyins + trail data from PostgreSQL into local DuckDB."""
    logger.info(f"Loading data from PostgreSQL (last {hours}h)...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")

    pg_conn = _get_pg_connection_string()
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    # 1) Buyins
    logger.info("  [1/3] Loading buyins...")
    con.execute(f"""
        CREATE TABLE buyins AS
        SELECT id AS buyin_id, play_id, followed_at, potential_gains, our_status
        FROM pg.follow_the_goat_buyins
        WHERE potential_gains IS NOT NULL
          AND followed_at >= NOW() - INTERVAL '{hours} hours'
    """)
    n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
    logger.info(f"    {n_buyins:,} buyins loaded")

    if n_buyins == 0:
        con.execute("DETACH pg")
        return None, {}

    # 2) Trail data (all 200+ columns)
    #    Filter to sub_minute=0 to get one row per (buyin_id, minute).
    #    This avoids cross-product inflation in the forward-return self-join.
    logger.info("  [2/3] Loading buyin_trail_minutes (sub_minute=0)...")
    t1 = time.time()
    con.execute("""
        CREATE TABLE trail AS
        SELECT *
        FROM pg.buyin_trail_minutes
        WHERE buyin_id IN (SELECT buyin_id FROM buyins)
          AND COALESCE(sub_minute, 0) = 0
    """)
    n_trail = con.execute("SELECT COUNT(*) FROM trail").fetchone()[0]
    n_buyins_trail = con.execute("SELECT COUNT(DISTINCT buyin_id) FROM trail").fetchone()[0]
    logger.info(f"    {n_trail:,} trail rows ({n_buyins_trail:,} buyins with trail) in {time.time()-t1:.1f}s")

    n_with_price = con.execute(
        "SELECT COUNT(*) FROM trail WHERE pm_close_price IS NOT NULL AND pm_close_price > 0"
    ).fetchone()[0]
    logger.info(f"    {n_with_price:,} rows with valid pm_close_price ({n_with_price/max(n_trail,1)*100:.1f}%)")

    con.execute("DETACH pg")

    # 3) Indexes
    logger.info("  [3/3] Creating indexes...")
    con.execute("CREATE INDEX idx_trail_bid ON trail(buyin_id)")
    con.execute("CREATE INDEX idx_trail_bid_min ON trail(buyin_id, minute)")

    elapsed = time.time() - t0
    logger.info(f"  All data loaded in {elapsed:.1f}s")

    return con, {
        'n_buyins': n_buyins,
        'n_trail': n_trail,
        'n_buyins_trail': n_buyins_trail,
        'n_with_price': n_with_price,
    }


# =============================================================================
# PHASE 2: FORWARD RETURN COMPUTATION
# =============================================================================

def compute_forward_returns(
    con: duckdb.DuckDBPyConnection,
    forward_window: int,
) -> Dict[str, Any]:
    """Self-join trail data to compute forward returns at each (buyin, minute)."""
    logger.info(f"Computing forward returns (window: +{forward_window} minutes)...")
    t0 = time.time()

    # Build dynamic SQL for each forward offset
    join_clauses = []
    select_parts = []
    for k in range(1, forward_window + 1):
        alias = f"t{k}"
        join_clauses.append(
            f"LEFT JOIN trail {alias} ON {alias}.buyin_id = t.buyin_id "
            f"AND {alias}.minute = t.minute + {k}"
        )
        select_parts.append(
            f"({alias}.pm_close_price - t.pm_close_price) "
            f"/ NULLIF(t.pm_close_price, 0) * 100 AS fwd_return_{k}m"
        )

    # GREATEST across all forward windows (treat NULL as -9999 so it's ignored)
    coalesce_parts = [f"COALESCE(fwd_return_{k}m, -9999)" for k in range(1, forward_window + 1)]
    greatest_expr = f"GREATEST({', '.join(coalesce_parts)})"

    # At least one forward return must be non-NULL
    any_not_null = " OR ".join(
        [f"fwd_return_{k}m IS NOT NULL" for k in range(1, forward_window + 1)]
    )

    joins_sql = "\n        ".join(join_clauses)
    selects_sql = ",\n            ".join(select_parts)

    con.execute(f"""
        CREATE TABLE fwd_returns AS
        WITH raw_fwd AS (
            SELECT
                t.buyin_id,
                t.minute,
                {selects_sql}
            FROM trail t
            {joins_sql}
            WHERE t.pm_close_price IS NOT NULL
              AND t.pm_close_price > 0
        )
        SELECT *,
            CASE WHEN {any_not_null}
                 THEN {greatest_expr}
                 ELSE NULL
            END AS max_fwd_return
        FROM raw_fwd
        WHERE ({any_not_null})
    """)

    row = con.execute("""
        SELECT
            COUNT(*)                                                     AS n_obs,
            COUNT(DISTINCT buyin_id)                                     AS n_buyins,
            AVG(max_fwd_return)                                          AS avg_fwd,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY max_fwd_return) AS med_fwd,
            MIN(max_fwd_return)                                          AS min_fwd,
            MAX(max_fwd_return)                                          AS max_fwd
        FROM fwd_returns
        WHERE max_fwd_return IS NOT NULL AND max_fwd_return > -9000
    """).fetchone()

    logger.info(f"  {row[0]:,} observations with forward returns ({row[1]:,} buyins)")
    logger.info(f"  Stats: avg={row[2]:.4f}%, median={row[3]:.4f}%, "
                f"min={row[4]:.4f}%, max={row[5]:.4f}%")
    logger.info(f"  Computed in {time.time()-t0:.1f}s")

    return {
        'n_observations': row[0],
        'n_buyins': row[1],
        'avg_fwd_return': row[2],
        'median_fwd_return': row[3],
    }


# =============================================================================
# PHASE 3: LABELING & DATA PULL
# =============================================================================

def build_labeled_dataset(
    con: duckdb.DuckDBPyConnection,
    min_pump_pct: float,
    forward_window: int,
) -> pd.DataFrame:
    """
    Join trail + forward returns, label each observation, pull to pandas.

    Labels (based on current momentum + forward return):
      pump_continuation : price IS climbing AND will keep climbing (> min_pump_pct)
      pump_reversal     : price IS climbing BUT will NOT keep climbing
      no_pump           : price is not climbing right now
    """
    logger.info(f"Building labeled dataset (min pump: {min_pump_pct}%)...")
    t0 = time.time()

    fwd_cols = ", ".join([f"f.fwd_return_{k}m" for k in range(1, forward_window + 1)])

    df = con.execute(f"""
        SELECT
            t.*,
            b.followed_at,
            b.potential_gains,
            {fwd_cols},
            f.max_fwd_return,
            CASE
                WHEN t.pm_price_change_1m IS NOT NULL
                     AND t.pm_price_change_1m > 0
                     AND f.max_fwd_return > {min_pump_pct}
                    THEN 'pump_continuation'
                WHEN t.pm_price_change_1m IS NOT NULL
                     AND t.pm_price_change_1m > 0
                     AND f.max_fwd_return <= {min_pump_pct}
                    THEN 'pump_reversal'
                ELSE 'no_pump'
            END AS label
        FROM trail t
        INNER JOIN fwd_returns f
            ON f.buyin_id = t.buyin_id AND f.minute = t.minute
        INNER JOIN buyins b
            ON b.buyin_id = t.buyin_id
        WHERE f.max_fwd_return IS NOT NULL
          AND f.max_fwd_return > -9000
        ORDER BY b.followed_at, t.buyin_id, t.minute
    """).fetchdf()

    label_counts = df['label'].value_counts()
    n_total = len(df)

    logger.info(f"  Total observations: {n_total:,}")
    for lbl in ['pump_continuation', 'pump_reversal', 'no_pump']:
        cnt = label_counts.get(lbl, 0)
        logger.info(f"    {lbl}: {cnt:,} ({cnt/max(n_total,1)*100:.1f}%)")

    n_climbing = label_counts.get('pump_continuation', 0) + label_counts.get('pump_reversal', 0)
    n_cont = label_counts.get('pump_continuation', 0)
    if n_climbing > 0:
        base_prec = n_cont / n_climbing * 100
        logger.info(f"  Baseline precision (among climbing): {base_prec:.1f}%")

    logger.info(f"  Built in {time.time()-t0:.1f}s")
    return df


# =============================================================================
# PHASE 4: INDIVIDUAL FILTER ANALYSIS
# =============================================================================

def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    """Return numeric columns suitable for filter analysis."""
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if col.startswith('fwd_return_') or col == 'max_fwd_return':
            continue
        if col in ('label', 'followed_at', 'potential_gains'):
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        if df[col].isna().mean() >= 0.90:
            continue
        cols.append(col)
    return sorted(cols)


def rank_filters_by_expected_profit(
    df: pd.DataFrame,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """
    Rank each filter column by expected profit per trade.

    For every column:
      1. Cohen's d between continuation and reversal distributions
      2. Youden's J for optimal cutpoint -> filter range
      3. Expected profit = P(continuation | pass) * avg_gain - TRADE_COST_PCT
    """
    is_cont = (df['label'] == 'pump_continuation').values
    is_rev = (df['label'] == 'pump_reversal').values

    results: List[Dict[str, Any]] = []

    for col in columns:
        cont_vals = df.loc[is_cont, col].dropna()
        rev_vals = df.loc[is_rev, col].dropna()

        if len(cont_vals) < 20 or len(rev_vals) < 20:
            continue

        # --- Cohen's d ---
        n_c, n_r = len(cont_vals), len(rev_vals)
        pooled_std = np.sqrt(
            ((n_c - 1) * cont_vals.std() ** 2 + (n_r - 1) * rev_vals.std() ** 2)
            / (n_c + n_r - 2)
        )
        if pooled_std == 0:
            continue
        cohens_d = abs(cont_vals.mean() - rev_vals.mean()) / pooled_std
        if cohens_d < 0.03:
            continue

        # --- KS test ---
        ks_stat, ks_pval = stats.ks_2samp(cont_vals.values, rev_vals.values)

        # --- Youden's J for optimal range ---
        all_vals = pd.concat([cont_vals, rev_vals])
        thresholds = np.quantile(all_vals.dropna(), np.linspace(0.05, 0.95, 40))

        best_j, best_cut, best_dir = -1, None, None
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

        # --- Expected profit for this filter ---
        vals = df[col].values
        mask_pass = (vals >= from_val) & (vals <= to_val) & ~np.isnan(vals)

        cont_pass = int((is_cont & mask_pass).sum())
        rev_pass = int((is_rev & mask_pass).sum())
        total_pass = cont_pass + rev_pass
        if total_pass < 10:
            continue

        precision = cont_pass / total_pass * 100

        # Average gain of continuation observations that pass
        pass_cont_mask = is_cont & mask_pass
        avg_gain = (
            float(df.loc[pass_cont_mask, 'max_fwd_return'].mean())
            if pass_cont_mask.sum() > 0 else 0
        )

        # Expected profit per trade = P(cont|pass) * avg_gain - cost
        expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT

        # Signal rate
        n_climbing = int(is_cont.sum() + is_rev.sum())
        signal_rate = total_pass / max(n_climbing, 1) * 100

        section = col.split('_')[0] + '_' if '_' in col else ''

        results.append({
            'column': col,
            'section': section,
            'from': round(from_val, 8),
            'to': round(to_val, 8),
            'cohens_d': round(cohens_d, 4),
            'ks_stat': round(ks_stat, 4),
            'ks_pval': ks_pval,
            'youdens_j': round(best_j, 4),
            'direction': best_dir,
            'precision': round(precision, 2),
            'n_signals': total_pass,
            'n_cont_pass': cont_pass,
            'n_rev_pass': rev_pass,
            'avg_gain_pass': round(avg_gain, 4),
            'expected_profit': round(expected_profit, 4),
            'signal_rate': round(signal_rate, 2),
        })

    results.sort(key=lambda x: x['expected_profit'], reverse=True)
    return results


# =============================================================================
# PHASE 5: COMBINATION DISCOVERY
# =============================================================================

def precompute_filter_masks(
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
    fwd_returns: np.ndarray,
    n_climbing: int,
) -> Optional[Dict[str, float]]:
    """Score a filter combination by expected profit per trade."""
    combined = masks[combo_cols[0]].copy()
    for col in combo_cols[1:]:
        combined &= masks[col]

    cont_pass = int((is_cont & combined).sum())
    rev_pass = int((is_rev & combined).sum())
    total_pass = cont_pass + rev_pass

    if total_pass < 10:
        return None

    precision = cont_pass / total_pass * 100

    pass_cont_mask = is_cont & combined
    avg_gain = (
        float(np.nanmean(fwd_returns[pass_cont_mask]))
        if pass_cont_mask.sum() > 0 else 0
    )

    expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT
    signal_rate = total_pass / max(n_climbing, 1) * 100

    return {
        'precision': round(precision, 2),
        'n_signals': total_pass,
        'n_cont_pass': cont_pass,
        'n_rev_pass': rev_pass,
        'avg_gain': round(avg_gain, 4),
        'expected_profit': round(expected_profit, 4),
        'signal_rate': round(signal_rate, 2),
    }


def find_best_combinations(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
    test_mode: bool = False,
) -> List[Dict[str, Any]]:
    """
    Find optimal filter combinations via:
      - Exhaustive pairs / triples / quads from top features
      - Greedy forward selection
    Validate on test set; keep only combos profitable on test.
    """
    logger.info("Finding best filter combinations...")
    t0 = time.time()

    if len(ranked_features) < 2:
        logger.info("  Not enough ranked features for combinations")
        return []

    # --- Prepare train arrays ---
    is_cont_train = (df_train['label'] == 'pump_continuation').values
    is_rev_train = (df_train['label'] == 'pump_reversal').values
    fwd_train = df_train['max_fwd_return'].values
    n_climbing_train = int(is_cont_train.sum() + is_rev_train.sum())

    # --- Prepare test arrays ---
    is_cont_test = (df_test['label'] == 'pump_continuation').values
    is_rev_test = (df_test['label'] == 'pump_reversal').values
    fwd_test = df_test['max_fwd_return'].values
    n_climbing_test = int(is_cont_test.sum() + is_rev_test.sum())

    # Baseline expected profit (no filter)
    base_prec = is_cont_train.sum() / max(n_climbing_train, 1) * 100
    base_gain = float(np.nanmean(fwd_train[is_cont_train])) if is_cont_train.sum() > 0 else 0
    base_profit = (base_prec / 100) * base_gain - TRADE_COST_PCT
    logger.info(f"  Train baseline: prec={base_prec:.1f}%, gain={base_gain:.4f}%, "
                f"E[profit]={base_profit:.4f}%")

    # Pre-compute masks
    masks_train = precompute_filter_masks(df_train, ranked_features)
    masks_test = precompute_filter_masks(df_test, ranked_features)

    results: List[Dict[str, Any]] = []
    combos_tested = 0

    def _test_combo(cols: Tuple[str, ...]):
        nonlocal combos_tested
        combos_tested += 1

        if not all(c in masks_train and c in masks_test for c in cols):
            return

        # Score on train
        train_m = score_combo(
            cols, masks_train, is_cont_train, is_rev_train, fwd_train, n_climbing_train
        )
        if train_m is None:
            return
        if train_m['expected_profit'] <= base_profit:
            return
        if train_m['n_cont_pass'] < 5:
            return

        # Score on test
        test_m = score_combo(
            cols, masks_test, is_cont_test, is_rev_test, fwd_test, n_climbing_test
        )
        if test_m is None:
            return
        # Only keep combos profitable on the test set
        if test_m['expected_profit'] <= 0:
            return

        # Build readable filter ranges
        ranges = []
        for col in cols:
            feat = next((r for r in ranked_features if r['column'] == col), None)
            if feat:
                ranges.append(f"{col}:[{feat['from']},{feat['to']}]")

        results.append({
            'n_filters': len(cols),
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
    top10 = [r['column'] for r in ranked_features[:min(10, n_avail)]]

    # --- Exhaustive pairs from top 30 ---
    logger.info(f"  Pairs from top {len(top30)}...")
    for combo in combinations(top30, 2):
        _test_combo(combo)

    if not test_mode:
        # --- Exhaustive triples from top 20 ---
        logger.info(f"  Triples from top {len(top20)}...")
        for combo in combinations(top20, 3):
            _test_combo(combo)

        # --- Exhaustive quads from top 15 ---
        if len(top15) >= 4:
            logger.info(f"  Quads from top {len(top15)}...")
            for combo in combinations(top15, 4):
                _test_combo(combo)

        # --- 5-combos from top 10 ---
        if len(top10) >= 5:
            logger.info(f"  5-combos from top {len(top10)}...")
            for combo in combinations(top10, 5):
                _test_combo(combo)

    # --- Greedy forward selection ---
    logger.info("  Greedy forward selection...")
    best_cols: List[str] = []
    available = [r['column'] for r in ranked_features[:min(30, n_avail)]]
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
                fwd_train, n_climbing_train,
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
        combo = tuple(sorted(best_cols))
        _test_combo(combo)
        logger.info(f"    Step {step+1}: +{best_addition} "
                    f"(train E[profit]={best_profit:.4f}%)")

    elapsed = time.time() - t0
    logger.info(f"  {combos_tested:,} combos tested, {len(results)} profitable on test, "
                f"{elapsed:.1f}s")

    results.sort(key=lambda x: x['test_expected_profit'], reverse=True)
    return results


# =============================================================================
# PHASE 6: REPORTING
# =============================================================================

def print_pump_statistics(
    df: pd.DataFrame,
    min_pump_pct: float,
    forward_window: int,
):
    """Print overview statistics about pump frequency, magnitude, and per-minute breakdown."""
    n_total = len(df)
    n_cont = int((df['label'] == 'pump_continuation').sum())
    n_rev = int((df['label'] == 'pump_reversal').sum())
    n_no_pump = int((df['label'] == 'no_pump').sum())
    n_climbing = n_cont + n_rev

    print()
    print("=" * 100)
    print("  PUMP STATISTICS OVERVIEW")
    print("=" * 100)
    print(f"\n  Observations (buyin x minute): {n_total:,}")
    print(f"  Unique buyins:                 {df['buyin_id'].nunique():,}")
    print(f"  Minutes range:                 {int(df['minute'].min())}-{int(df['minute'].max())}")
    print(f"  Forward window:                {forward_window} min")
    print(f"  Min pump threshold:            {min_pump_pct}%")
    print(f"  Trade cost:                    {TRADE_COST_PCT}%")

    print(f"\n  Label Distribution:")
    print(f"    Climbing (pm_price_change_1m > 0): {n_climbing:,} "
          f"({n_climbing/max(n_total,1)*100:.1f}%)")
    print(f"      -> Continues (fwd > {min_pump_pct}%):  {n_cont:,} "
          f"({n_cont/max(n_climbing,1)*100:.1f}% of climbing)")
    print(f"      -> Reverses  (fwd <= {min_pump_pct}%): {n_rev:,} "
          f"({n_rev/max(n_climbing,1)*100:.1f}% of climbing)")
    print(f"    Not climbing:                      {n_no_pump:,} "
          f"({n_no_pump/max(n_total,1)*100:.1f}%)")

    # Baseline metrics
    base_prec = n_cont / max(n_climbing, 1) * 100
    cont_gains = df.loc[df['label'] == 'pump_continuation', 'max_fwd_return']
    avg_gain = float(cont_gains.mean()) if len(cont_gains) > 0 else 0
    base_profit = (base_prec / 100) * avg_gain - TRADE_COST_PCT

    print(f"\n  Baseline (enter EVERY climbing moment):")
    print(f"    Precision:           {base_prec:.1f}%")
    print(f"    Avg gain when right: {avg_gain:.4f}%")
    print(f"    E[profit] per trade: {base_profit:+.4f}%")
    print(f"    Verdict:             {'PROFITABLE' if base_profit > 0 else 'NOT PROFITABLE'}")

    # Forward-return distribution among climbing moments
    if n_climbing > 0:
        fwd = df.loc[df['label'].isin(['pump_continuation', 'pump_reversal']), 'max_fwd_return']
        print(f"\n  Forward Return Distribution (climbing moments only):")
        for pct in [5, 10, 25, 50, 75, 90, 95, 99]:
            print(f"    P{pct:2d}: {fwd.quantile(pct/100):+.4f}%")

    # Per-minute breakdown
    print(f"\n  Per-Minute Breakdown:")
    header = (f"  {'Min':>4}  {'Total':>7}  {'Climbing':>9}  {'Cont':>7}  "
              f"{'Rev':>7}  {'Prec%':>7}  {'AvgGain':>8}  {'E[Profit]':>10}")
    print(header)
    print(f"  {'---':>4}  {'-----':>7}  {'--------':>9}  {'----':>7}  "
          f"{'---':>7}  {'-----':>7}  {'-------':>8}  {'---------':>10}")

    for minute in sorted(df['minute'].unique()):
        m = df[df['minute'] == minute]
        mt = len(m)
        mc = int(m['label'].isin(['pump_continuation', 'pump_reversal']).sum())
        mco = int((m['label'] == 'pump_continuation').sum())
        mre = int((m['label'] == 'pump_reversal').sum())
        mp = mco / max(mc, 1) * 100
        mg = float(m.loc[m['label'] == 'pump_continuation', 'max_fwd_return'].mean()) if mco > 0 else 0
        mpr = (mp / 100) * mg - TRADE_COST_PCT
        print(f"  {int(minute):4d}  {mt:7,}  {mc:9,}  {mco:7,}  "
              f"{mre:7,}  {mp:6.1f}%  {mg:7.4f}%  {mpr:+9.4f}%")

    print()


def print_filter_rankings(rankings: List[Dict[str, Any]], top_n: int = 30):
    """Print ranked individual filters by expected profit."""
    show = min(top_n, len(rankings))
    if show == 0:
        print("\n  No filters passed screening.\n")
        return

    print()
    print("=" * 145)
    print(f"  TOP {show} INDIVIDUAL FILTERS (ranked by expected profit per trade)")
    print("=" * 145)
    print(f"  {'#':>3}  {'Column':<40}  {'Sec':<4}  {'Prec%':>6}  "
          f"{'Signals':>8}  {'AvgGain':>8}  {'E[Profit]':>10}  "
          f"{'Cohen d':>8}  {'KS':>6}  {'J':>6}  Range")
    print(f"  {'--':>3}  {'------':<40}  {'---':<4}  {'-----':>6}  "
          f"{'-------':>8}  {'-------':>8}  {'---------':>10}  "
          f"{'-------':>8}  {'--':>6}  {'-':>6}  -----")

    for i, r in enumerate(rankings[:show], 1):
        name = r['column'][:40]
        rng = f"[{r['from']:.4g}, {r['to']:.4g}]"
        print(f"  {i:3d}  {name:<40}  {r['section']:<4}  {r['precision']:5.1f}%  "
              f"{r['n_signals']:8,}  {r['avg_gain_pass']:7.4f}%  "
              f"{r['expected_profit']:+9.4f}%  "
              f"{r['cohens_d']:8.4f}  {r['ks_stat']:6.4f}  "
              f"{r['youdens_j']:6.4f}  {rng}")

    # Section summary
    sections: Dict[str, Dict[str, int]] = {}
    for r in rankings:
        sec = r['section']
        if sec not in sections:
            sections[sec] = {'total': 0, 'profitable': 0}
        sections[sec]['total'] += 1
        if r['expected_profit'] > 0:
            sections[sec]['profitable'] += 1

    print(f"\n  Section Summary:")
    print(f"  {'Section':<10}  {'Total':>6}  {'Profitable':>11}")
    for sec in sorted(sections):
        s = sections[sec]
        print(f"  {sec:<10}  {s['total']:>6}  {s['profitable']:>11}")
    print()


def print_combination_results(results: List[Dict[str, Any]], top_n: int = 20):
    """Print top filter combinations with train/test metrics."""
    show = min(top_n, len(results))
    if show == 0:
        print("\n  No profitable filter combinations found on test set.\n")
        return

    print()
    print("=" * 155)
    print(f"  TOP {show} FILTER COMBINATIONS (ranked by test expected profit)")
    print("=" * 155)
    print(f"  {'#':>3}  {'NF':>3}  {'TrPrec%':>8}  {'TrProfit':>9}  "
          f"{'TsPrec%':>8}  {'TsProfit':>9}  {'Overfit':>8}  "
          f"{'TsSig':>7}  {'TsGain':>7}  Filters")
    print(f"  {'--':>3}  {'--':>3}  {'-------':>8}  {'--------':>9}  "
          f"{'-------':>8}  {'--------':>9}  {'-------':>8}  "
          f"{'-----':>7}  {'------':>7}  -------")

    for i, r in enumerate(results[:show], 1):
        cols = r['filter_columns'].replace('|', ', ')
        if len(cols) > 55:
            cols = cols[:52] + '...'
        print(f"  {i:3d}  {r['n_filters']:>3d}  {r['train_precision']:7.1f}%  "
              f"{r['train_expected_profit']:+8.4f}%  "
              f"{r['test_precision']:7.1f}%  "
              f"{r['test_expected_profit']:+8.4f}%  "
              f"{r['overfit_delta']:+7.4f}%  "
              f"{r['test_n_signals']:7,}  "
              f"{r['test_avg_gain']:6.4f}%  {cols}")

    # Detail on best result
    best = results[0]
    print()
    print("  " + "-" * 80)
    print("  BEST COMBINATION DETAIL:")
    print("  " + "-" * 80)
    print(f"  Filters ({best['n_filters']}):")
    for part in best['filter_ranges'].split('|'):
        print(f"    {part}")
    print(f"\n  Train set:")
    print(f"    Precision:    {best['train_precision']:.1f}%")
    print(f"    Avg gain:     {best['train_avg_gain']:.4f}%")
    print(f"    E[profit]:    {best['train_expected_profit']:+.4f}%")
    print(f"    Signals:      {best['train_n_signals']:,}")
    print(f"  Test set:")
    print(f"    Precision:    {best['test_precision']:.1f}%")
    print(f"    Avg gain:     {best['test_avg_gain']:.4f}%")
    print(f"    E[profit]:    {best['test_expected_profit']:+.4f}%")
    print(f"    Signals:      {best['test_n_signals']:,}")
    print(f"  Overfit delta:  {best['overfit_delta']:+.4f}%")

    # Expected return simulation
    ts = best['test_n_signals']
    tp = best['test_expected_profit']
    tg = best['test_avg_gain']
    if ts > 0 and tg > 0:
        breakeven = TRADE_COST_PCT / tg * 100
        print(f"\n  EXPECTED RETURN SIMULATION (test set):")
        print(f"    If you entered every signal ({ts} trades):")
        print(f"      Per-trade expected profit: {tp:+.4f}%")
        print(f"      Cumulative over {ts} trades: {tp * ts:+.2f}%")
        print(f"      Breakeven precision:       {breakeven:.1f}% "
              f"(yours: {best['test_precision']:.1f}%)")
        margin = best['test_precision'] - breakeven
        print(f"      Safety margin:             {margin:+.1f} pp")

    print()


# =============================================================================
# CSV OUTPUT
# =============================================================================

COMBO_CSV_FIELDS = [
    'rank', 'n_filters', 'filter_columns', 'filter_ranges',
    'train_precision', 'train_n_signals', 'train_avg_gain', 'train_expected_profit',
    'test_precision', 'test_n_signals', 'test_avg_gain', 'test_expected_profit',
    'overfit_delta',
]

FILTER_CSV_FIELDS = [
    'rank', 'column', 'section', 'from', 'to', 'precision', 'n_signals',
    'avg_gain_pass', 'expected_profit', 'cohens_d', 'ks_stat', 'youdens_j',
    'signal_rate', 'direction',
]


def _write_csv(rows: List[Dict], fields: List[str], path: Path):
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rank, r in enumerate(rows, 1):
            row = {**r, 'rank': rank}
            w.writerow({k: row.get(k, '') for k in fields})


def save_results_csv(
    combo_results: List[Dict[str, Any]],
    filter_rankings: List[Dict[str, Any]],
    timestamp: str,
):
    """Save results to CSV files (timestamped + latest)."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if combo_results:
        _write_csv(combo_results, COMBO_CSV_FIELDS,
                    RESULTS_DIR / f"pump_combos_{timestamp}.csv")
        _write_csv(combo_results, COMBO_CSV_FIELDS,
                    RESULTS_DIR / "pump_combos_latest.csv")
        logger.info(f"  Saved {len(combo_results)} combos -> pump_combos_{timestamp}.csv")

    if filter_rankings:
        _write_csv(filter_rankings, FILTER_CSV_FIELDS,
                    RESULTS_DIR / f"pump_filters_{timestamp}.csv")
        _write_csv(filter_rankings, FILTER_CSV_FIELDS,
                    RESULTS_DIR / "pump_filters_latest.csv")
        logger.info(f"  Saved {len(filter_rankings)} filters -> pump_filters_{timestamp}.csv")


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def run_analysis(
    hours: int = DEFAULT_HOURS,
    min_pump_pct: float = DEFAULT_MIN_PUMP_PCT,
    forward_window: int = DEFAULT_FORWARD_WINDOW,
    test_mode: bool = False,
):
    """Run the full pump entry analysis pipeline."""
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print()
    print("=" * 80)
    print("  PUMP ENTRY ANALYZER  -  Momentum Continuation Detection")
    print(f"  Started:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Lookback:       {hours}h")
    print(f"  Min pump:       {min_pump_pct}%")
    print(f"  Forward window: {forward_window} min")
    print(f"  Trade cost:     {TRADE_COST_PCT}%")
    if test_mode:
        print("  MODE:           TEST (reduced search)")
    print("=" * 80)
    print()
    sys.stdout.flush()

    # ── Phase 1: Load data ──────────────────────────────────────────────
    con, load_stats = load_data(hours)
    if con is None:
        logger.error("No data loaded. Aborting.")
        return
    sys.stdout.flush()

    # ── Phase 2: Forward returns ────────────────────────────────────────
    fwd_stats = compute_forward_returns(con, forward_window)
    if fwd_stats['n_observations'] == 0:
        logger.error("No forward-return observations. Aborting.")
        con.close()
        return
    sys.stdout.flush()

    # ── Phase 3: Label & pull to pandas ─────────────────────────────────
    df = build_labeled_dataset(con, min_pump_pct, forward_window)
    con.close()

    if len(df) == 0:
        logger.error("Empty dataset after labeling. Aborting.")
        return

    # Filter to climbing moments only
    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 50:
        logger.error(f"Only {len(climbing_df)} climbing observations (need >= 50). Aborting.")
        return

    # Print pump stats (part of Phase 6, but useful context before analysis)
    print_pump_statistics(df, min_pump_pct, forward_window)
    sys.stdout.flush()

    # ── Train / Test split (by buyin followed_at) ───────────────────────
    trade_times = climbing_df.groupby('buyin_id')['followed_at'].first().sort_values()
    n_train = int(len(trade_times) * TRAIN_FRAC)
    train_ids = set(trade_times.iloc[:n_train].index)

    df_train = climbing_df[climbing_df['buyin_id'].isin(train_ids)].copy()
    df_test = climbing_df[~climbing_df['buyin_id'].isin(train_ids)].copy()

    trn_cont = int((df_train['label'] == 'pump_continuation').sum())
    trn_rev = int((df_train['label'] == 'pump_reversal').sum())
    tst_cont = int((df_test['label'] == 'pump_continuation').sum())
    tst_rev = int((df_test['label'] == 'pump_reversal').sum())

    logger.info(f"Train/Test split: {len(df_train):,} / {len(df_test):,} observations "
                f"({len(train_ids)} / {len(trade_times) - n_train} buyins)")
    logger.info(f"  Train: {trn_cont:,} cont + {trn_rev:,} rev "
                f"(prec={trn_cont/max(trn_cont+trn_rev,1)*100:.1f}%)")
    logger.info(f"  Test:  {tst_cont:,} cont + {tst_rev:,} rev "
                f"(prec={tst_cont/max(tst_cont+tst_rev,1)*100:.1f}%)")
    sys.stdout.flush()

    if len(df_test) < 20:
        logger.error("Test set too small. Aborting.")
        return

    # ── Phase 4: Individual filter analysis ─────────────────────────────
    logger.info("Phase 4: Analyzing individual filters on train set...")
    columns = get_filterable_columns(df_train)
    logger.info(f"  {len(columns)} filterable columns found")

    if test_mode:
        columns = columns[:50]
        logger.info(f"  TEST MODE: limited to {len(columns)} columns")

    filter_rankings = rank_filters_by_expected_profit(df_train, columns)
    profitable_count = sum(1 for r in filter_rankings if r['expected_profit'] > 0)
    logger.info(f"  {len(filter_rankings)} filters ranked, {profitable_count} individually profitable")
    sys.stdout.flush()

    print_filter_rankings(filter_rankings)
    sys.stdout.flush()

    # ── Phase 5: Combination discovery ──────────────────────────────────
    if len(filter_rankings) >= 2:
        combo_results = find_best_combinations(
            df_train, df_test, filter_rankings, test_mode=test_mode,
        )
    else:
        combo_results = []
    sys.stdout.flush()

    print_combination_results(combo_results)
    sys.stdout.flush()

    # ── Save CSV ────────────────────────────────────────────────────────
    save_results_csv(combo_results, filter_rankings, timestamp)

    # ── Final summary ───────────────────────────────────────────────────
    elapsed = time.time() - start_time

    print()
    print("=" * 80)
    print("  ANALYSIS COMPLETE")
    print(f"  Finished:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Runtime:   {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Climbing moments analyzed: {len(climbing_df):,}")
    print(f"  Filters ranked:            {len(filter_rankings)}")
    print(f"  Profitable filters:        {profitable_count}")
    print(f"  Profitable combinations:   {len(combo_results)}")
    if combo_results:
        best = combo_results[0]
        print(f"  Best E[profit] per trade:  {best['test_expected_profit']:+.4f}%")
    print("=" * 80)
    print()
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Pump Entry Analyzer - Momentum Continuation Detection",
    )
    parser.add_argument(
        "--hours", type=int, default=DEFAULT_HOURS,
        help=f"Hours of historical data (default: {DEFAULT_HOURS})",
    )
    parser.add_argument(
        "--min-pump-pct", type=float, default=DEFAULT_MIN_PUMP_PCT,
        help=f"Min forward return %% to count as continuation (default: {DEFAULT_MIN_PUMP_PCT})",
    )
    parser.add_argument(
        "--forward-window", type=int, default=DEFAULT_FORWARD_WINDOW,
        help=f"Forward window in minutes (default: {DEFAULT_FORWARD_WINDOW})",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Quick test mode (reduced search space)",
    )
    args = parser.parse_args()

    run_analysis(
        hours=args.hours,
        min_pump_pct=args.min_pump_pct,
        forward_window=args.forward_window,
        test_mode=args.test,
    )


if __name__ == "__main__":
    main()

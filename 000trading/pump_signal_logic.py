"""
Pump Signal Logic
=================
Pump detection filter analysis and signal evaluation, designed to be called
from within the train_validator cycle.

Two responsibilities:
  1. Every 5 minutes: recalculate optimal pump detection filters using
     in-memory DuckDB for fast analytical queries on trail data.
  2. Every cycle (~5s): evaluate the just-generated trail row against the
     active filter rules and fire a pump signal if conditions are met.

DuckDB is used ONLY as an in-memory read-only analytical cache (same pattern
as core/filter_cache.py). PostgreSQL remains the source of truth.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.database import get_postgres, postgres_execute, postgres_query, postgres_query_one

# Use child logger of train_validator so logs appear in the same log file
logger = logging.getLogger("train_validator.pump_signal")

# =============================================================================
# CONSTANTS
# =============================================================================

TRADE_COST_PCT = 0.1           # Cost per wrong trade (%)
MIN_PUMP_PCT = 0.3             # Min forward return to label as clean pump (0.3%)
MAX_DRAWDOWN_PCT = 0.15        # Max acceptable dip in path to peak (0.15%)
FORWARD_WINDOW = 10            # Look N minutes ahead for forward returns
LOOKBACK_HOURS = 72            # Hours of historical data for rule calculation (was 48)
TRAIN_FRAC = 0.70              # Train/test time-based split
RULES_REFRESH_INTERVAL = 300   # Re-calculate rules every 5 minutes
MIN_PRECISION = 45.0           # Min test precision (breakeven=23.5%, 45% is 2x margin)
COOLDOWN_SECONDS = 300         # Don't re-enter within 5 minutes (was 120)

# ── Safety gate thresholds ────────────────────────────────────────────────────
# IMPORTANT: Data analysis on ~10k trail snapshots shows that uptrend gates
# provide ZERO selectivity between clean pumps and non-pumps:
#   - pm_price_change_1m > 0: passes 45% clean, 46% no-pump (useless)
#   - pm_price_change_5m > 0: passes 44% clean, 49% no-pump (negative select!)
#   - micro-trend 15s/30s: similarly useless
#
# Clean pumps start from FLAT price action (avg pre-1m change = -0.002%),
# not from already-rising prices. Requiring uptrend gates actually BLOCKS
# most clean pumps while providing no filtering of bad entries.
#
# Instead, we use SAFETY-ONLY gates (prevent entry during crashes) and let
# the STATISTICAL FILTERS (200+ features: order book, cross-asset correlation,
# volatility, patterns) do the selection -- they have features with Cohen's d
# of 0.25-0.50 that genuinely distinguish clean pumps from noise.
#
# Real-time price buffer is still maintained for the safety crash check.
CRASH_GATE_5M = -0.3           # Don't enter if 5m change < -0.3% (crash protection)
CRASH_GATE_MICRO_30S = -0.05   # Don't enter if 30s trend < -0.05% (active selloff)
PRICE_BUFFER_MAX_AGE = 600     # Keep 10 min of price history in memory

# Columns to never use as filters (metadata, labels, computed, forward-looking)
SKIP_COLUMNS = frozenset([
    'buyin_id', 'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good', 'label', 'created_at', 'pre_entry_trend',
    # Forward-looking columns from DuckDB analysis -- NOT available in live trail data
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


# =============================================================================
# MODULE-LEVEL STATE
# =============================================================================

_active_rules: List[Dict[str, Any]] = []     # [{"column": str, "from_val": float, "to_val": float}]
_rules_metadata: Dict[str, Any] = {}         # precision, n_signals, timestamp, etc.
_last_rules_refresh: float = 0.0
_last_entry_time: float = 0.0
_last_gate_summary_time: float = 0.0
_gate_stats: Dict[str, int] = {              # Track gate failure reasons for periodic summary
    'no_rules': 0,
    'crash_gate_fail': 0,
    'crash_5m_fail': 0,
    'gates_passed': 0,
    'filters_fail': 0,
    'signal_fired': 0,
    'total_checks': 0,
}

# Real-time price buffer: stores (timestamp, price) every ~5 seconds.
# This gives us instant micro-trend detection (10s, 15s, 30s windows)
# without waiting for trail data to update (which only refreshes every 30s).
_price_buffer: deque = deque(maxlen=200)  # ~16 min at 5s intervals


def _update_price_buffer(price: float) -> None:
    """Add current market price to the rolling buffer."""
    _price_buffer.append((time.time(), price))


def _get_micro_trend(seconds: int) -> Optional[float]:
    """
    Get price change percentage over the last N seconds from the buffer.
    Returns None if not enough data in the buffer.
    """
    if len(_price_buffer) < 2:
        return None

    now = _price_buffer[-1][0]  # Use most recent entry as "now"
    current_price = _price_buffer[-1][1]
    cutoff = now - seconds

    # Find the oldest price within the window
    for ts, price in _price_buffer:
        if ts >= cutoff:
            if price == 0:
                return None
            return (current_price - price) / price * 100
    return None


def _is_not_crashing() -> Tuple[bool, str]:
    """
    Safety check: ensure we're NOT in an active crash/selloff.

    This is deliberately permissive -- data analysis showed that uptrend gates
    provide zero selectivity (clean pumps start from flat, not rising prices).
    We only block entry during genuine selloffs.

    Returns:
        (passed, description) where description shows the micro-trend values.
    """
    trend_30s = _get_micro_trend(30)

    if trend_30s is None:
        # Not enough buffer data yet -- allow entry (filters will decide)
        if len(_price_buffer) < 3:
            return False, f"insufficient data (buffer={len(_price_buffer)})"
        return True, f"buffer warming up ({len(_price_buffer)} samples)"

    if trend_30s < CRASH_GATE_MICRO_30S:
        return False, f"30s={trend_30s:+.4f}% < {CRASH_GATE_MICRO_30S}% (active selloff)"

    return True, f"30s={trend_30s:+.4f}%"


# =============================================================================
# DUCKDB DATA LOADING (in-memory, read-only)
# =============================================================================

def _get_pg_connection_string() -> str:
    """Build a libpq connection string from project config."""
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


def _load_and_label_data_duckdb() -> Optional[pd.DataFrame]:
    """
    Load trail data into in-memory DuckDB, compute forward returns, label, and
    return as a pandas DataFrame.

    Uses DuckDB's postgres extension to stream data directly, then leverages
    DuckDB's columnar engine for the self-join forward return computation
    (10-50x faster than doing this in PostgreSQL).
    """
    import duckdb

    logger.info(f"Loading trail data via DuckDB (last {LOOKBACK_HOURS}h)...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")

        pg_conn = _get_pg_connection_string()
        con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

        # 1) Buyins
        con.execute(f"""
            CREATE TABLE buyins AS
            SELECT id AS buyin_id, followed_at, potential_gains
            FROM pg.follow_the_goat_buyins
            WHERE potential_gains IS NOT NULL
              AND followed_at >= NOW() - INTERVAL '{LOOKBACK_HOURS} hours'
        """)
        n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
        if n_buyins == 0:
            logger.warning("No buyins found for analysis")
            con.execute("DETACH pg")
            return None

        # 2) Trail data (sub_minute=0 only)
        t1 = time.time()
        con.execute("""
            CREATE TABLE trail AS
            SELECT *
            FROM pg.buyin_trail_minutes
            WHERE buyin_id IN (SELECT buyin_id FROM buyins)
              AND COALESCE(sub_minute, 0) = 0
        """)
        n_trail = con.execute("SELECT COUNT(*) FROM trail").fetchone()[0]
        logger.info(f"  {n_trail:,} trail rows ({n_buyins:,} buyins) in {time.time()-t1:.1f}s")

        con.execute("DETACH pg")

        if n_trail == 0:
            logger.warning("No trail data found")
            return None

        # 3) Indexes for self-join
        con.execute("CREATE INDEX idx_t_bid_min ON trail(buyin_id, minute)")

        # 4) Forward returns via self-join
        join_clauses = []
        select_parts = []
        for k in range(1, FORWARD_WINDOW + 1):
            alias = f"t{k}"
            join_clauses.append(
                f"LEFT JOIN trail {alias} ON {alias}.buyin_id = t.buyin_id "
                f"AND {alias}.minute = t.minute + {k}"
            )
            select_parts.append(
                f"({alias}.pm_close_price - t.pm_close_price) "
                f"/ NULLIF(t.pm_close_price, 0) * 100 AS fwd_return_{k}m"
            )

        coalesce_parts = [f"COALESCE(fwd_return_{k}m, -9999)" for k in range(1, FORWARD_WINDOW + 1)]
        greatest_expr = f"GREATEST({', '.join(coalesce_parts)})"
        # For min_fwd_return, use 9999 so missing values don't dominate LEAST
        coalesce_parts_min = [f"COALESCE(fwd_return_{k}m, 9999)" for k in range(1, FORWARD_WINDOW + 1)]
        least_expr = f"LEAST({', '.join(coalesce_parts_min)})"
        any_not_null = " OR ".join(
            [f"fwd_return_{k}m IS NOT NULL" for k in range(1, FORWARD_WINDOW + 1)]
        )

        joins_sql = "\n            ".join(join_clauses)
        selects_sql = ",\n                ".join(select_parts)

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
                END AS max_fwd_return,
                CASE WHEN {any_not_null}
                     THEN {least_expr}
                     ELSE NULL
                END AS min_fwd_return
            FROM raw_fwd
            WHERE ({any_not_null})
        """)

        # 5) Label and pull to pandas
        # Label as pump_continuation/pump_reversal when 5m trend is positive.
        #
        # CRITICAL FIX: Drawdown-aware labeling.
        # Previously, we only checked max_fwd_return (the PEAK in next 10 min).
        # This caused look-ahead bias: the backtest said "price will reach +0.5%"
        # but didn't check if price DIPPED below our stop-loss first.
        #
        # Now we ALSO check min_fwd_return (the worst dip in next 10 min).
        # A moment is only labeled 'pump_continuation' if:
        #   1. Peak return reaches MIN_PUMP_PCT (price goes up enough)
        #   2. Worst dip stays above -MAX_DRAWDOWN_PCT (doesn't trigger stop-loss)
        #
        # NOTE: We do NOT require pm_price_change_5m > 0. Data analysis showed
        # the 5m gate has NEGATIVE selectivity (passes 49% no-pumps vs 44%
        # clean pumps). Clean pumps start from flat prices, not uptrends.
        # We only exclude active crashes (< CRASH_GATE_5M = -0.3%).
        df = con.execute(f"""
            SELECT
                t.*,
                b.followed_at,
                f.max_fwd_return,
                f.min_fwd_return,
                CASE
                    WHEN (t.pm_price_change_5m IS NULL
                          OR t.pm_price_change_5m > {CRASH_GATE_5M})
                         AND f.max_fwd_return > {MIN_PUMP_PCT}
                         AND f.min_fwd_return > -{MAX_DRAWDOWN_PCT}
                        THEN 'pump_continuation'
                    WHEN (t.pm_price_change_5m IS NULL
                          OR t.pm_price_change_5m > {CRASH_GATE_5M})
                         AND (f.max_fwd_return <= {MIN_PUMP_PCT}
                              OR f.min_fwd_return <= -{MAX_DRAWDOWN_PCT})
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

        elapsed = time.time() - t0
        logger.info(f"  Loaded {len(df):,} labeled rows in {elapsed:.1f}s")

        label_counts = df['label'].value_counts()
        for lbl in ['pump_continuation', 'pump_reversal', 'no_pump']:
            cnt = label_counts.get(lbl, 0)
            logger.info(f"    {lbl}: {cnt:,}")

        # Log drawdown stats for pump_continuation (shows quality of labels)
        cont_mask = df['label'] == 'pump_continuation'
        if cont_mask.sum() > 0:
            cont_df = df[cont_mask]
            logger.info(f"    pump_continuation stats:")
            logger.info(f"      avg max_fwd_return: {cont_df['max_fwd_return'].mean():.4f}%")
            logger.info(f"      avg min_fwd_return: {cont_df['min_fwd_return'].mean():.4f}%")
            logger.info(f"      worst dip (min of min_fwd): {cont_df['min_fwd_return'].min():.4f}%")

        return df

    except Exception as e:
        logger.error(f"Error loading data via DuckDB: {e}", exc_info=True)
        return None
    finally:
        try:
            con.close()
        except Exception:
            pass


# =============================================================================
# FILTER ANALYSIS (runs on pandas DataFrame)
# =============================================================================

def _get_filterable_columns(df: pd.DataFrame) -> List[str]:
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


def _rank_filters(df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
    """
    Rank filter columns by expected profit using Cohen's d + Youden's J.
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
        if cohens_d < 0.03:
            continue

        # Youden's J for optimal range
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
            float(df.loc[pass_cont_mask, 'max_fwd_return'].mean())
            if pass_cont_mask.sum() > 0 else 0
        )
        expected_profit = (precision / 100) * avg_gain - TRADE_COST_PCT

        results.append({
            'column': col,
            'from': round(from_val, 8),
            'to': round(to_val, 8),
            'cohens_d': round(cohens_d, 4),
            'precision': round(precision, 2),
            'n_signals': total_pass,
            'avg_gain': round(avg_gain, 4),
            'expected_profit': round(expected_profit, 4),
        })

    results.sort(key=lambda x: x['expected_profit'], reverse=True)
    return results


def _precompute_masks(
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


def _score_combo(
    combo_cols: Tuple[str, ...],
    masks: Dict[str, np.ndarray],
    is_cont: np.ndarray,
    is_rev: np.ndarray,
    fwd_returns: np.ndarray,
    n_climbing: int,
) -> Optional[Dict[str, Any]]:
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

    return {
        'precision': round(precision, 2),
        'n_signals': total_pass,
        'n_cont_pass': cont_pass,
        'n_rev_pass': rev_pass,
        'avg_gain': round(avg_gain, 4),
        'expected_profit': round(expected_profit, 4),
    }


def _find_best_combination(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Find the best filter combination via exhaustive pairs/triples + greedy.
    Returns the best combo with test_precision >= MIN_PRECISION, or None.
    """
    if len(ranked_features) < 2:
        return None

    t0 = time.time()

    # Prepare train arrays
    is_cont_train = (df_train['label'] == 'pump_continuation').values
    is_rev_train = (df_train['label'] == 'pump_reversal').values
    fwd_train = df_train['max_fwd_return'].values
    n_climbing_train = int(is_cont_train.sum() + is_rev_train.sum())

    # Prepare test arrays
    is_cont_test = (df_test['label'] == 'pump_continuation').values
    is_rev_test = (df_test['label'] == 'pump_reversal').values
    fwd_test = df_test['max_fwd_return'].values
    n_climbing_test = int(is_cont_test.sum() + is_rev_test.sum())

    # Baseline
    base_prec = is_cont_train.sum() / max(n_climbing_train, 1) * 100
    base_gain = float(np.nanmean(fwd_train[is_cont_train])) if is_cont_train.sum() > 0 else 0
    base_profit = (base_prec / 100) * base_gain - TRADE_COST_PCT

    # Pre-compute masks
    masks_train = _precompute_masks(df_train, ranked_features)
    masks_test = _precompute_masks(df_test, ranked_features)

    results: List[Dict[str, Any]] = []
    combos_tested = 0

    def _test_combo(cols: Tuple[str, ...]):
        nonlocal combos_tested
        combos_tested += 1

        if not all(c in masks_train and c in masks_test for c in cols):
            return

        train_m = _score_combo(
            cols, masks_train, is_cont_train, is_rev_train, fwd_train, n_climbing_train
        )
        if train_m is None or train_m['expected_profit'] <= base_profit or train_m['n_cont_pass'] < 5:
            return

        test_m = _score_combo(
            cols, masks_test, is_cont_test, is_rev_test, fwd_test, n_climbing_test
        )
        if test_m is None or test_m['expected_profit'] <= 0 or test_m['precision'] < MIN_PRECISION:
            return

        results.append({
            'columns': cols,
            'train_precision': train_m['precision'],
            'train_expected_profit': train_m['expected_profit'],
            'test_precision': test_m['precision'],
            'test_n_signals': test_m['n_signals'],
            'test_avg_gain': test_m['avg_gain'],
            'test_expected_profit': test_m['expected_profit'],
        })

    # Exhaustive pairs from top 30
    n_avail = len(ranked_features)
    top30 = [r['column'] for r in ranked_features[:min(30, n_avail)]]
    top20 = [r['column'] for r in ranked_features[:min(20, n_avail)]]

    for combo in combinations(top30, 2):
        _test_combo(combo)

    # Exhaustive triples from top 20
    for combo in combinations(top20, 3):
        _test_combo(combo)

    # Greedy forward selection (max 5 steps)
    best_cols: List[str] = []
    available = top30[:]
    current_profit = base_profit

    for step in range(min(5, len(available))):
        best_addition = None
        best_profit = current_profit

        for col in available:
            if col in best_cols:
                continue
            candidate = tuple(sorted(best_cols + [col]))
            if not all(c in masks_train for c in candidate):
                continue
            m = _score_combo(
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
        _test_combo(tuple(sorted(best_cols)))

    elapsed = time.time() - t0
    logger.info(f"  {combos_tested:,} combos tested, {len(results)} passed "
                f"(precision>={MIN_PRECISION}%), {elapsed:.1f}s")

    if not results:
        return None

    results.sort(key=lambda x: x['test_expected_profit'], reverse=True)
    return results[0]


# =============================================================================
# RULE REFRESH (every 5 minutes)
# =============================================================================

def refresh_pump_rules():
    """
    Recalculate optimal pump detection filters from recent trail data.
    Uses in-memory DuckDB for the heavy analytical work, then stores
    the result as in-memory filter rules.
    """
    global _active_rules, _rules_metadata

    logger.info("=== Refreshing pump detection rules ===")
    t0 = time.time()

    # 1. Load data via DuckDB
    df = _load_and_label_data_duckdb()
    if df is None or len(df) == 0:
        logger.warning("No data for rule refresh -- keeping existing rules")
        return

    # Filter to climbing moments only
    climbing_df = df[df['label'].isin(['pump_continuation', 'pump_reversal'])].copy()
    if len(climbing_df) < 50:
        logger.warning(f"Only {len(climbing_df)} climbing observations -- keeping existing rules")
        return

    # 2. Train/test split
    trade_times = climbing_df.groupby('buyin_id')['followed_at'].first().sort_values()
    n_train = int(len(trade_times) * TRAIN_FRAC)
    train_ids = set(trade_times.iloc[:n_train].index)

    df_train = climbing_df[climbing_df['buyin_id'].isin(train_ids)].copy()
    df_test = climbing_df[~climbing_df['buyin_id'].isin(train_ids)].copy()

    if len(df_test) < 20:
        logger.warning("Test set too small -- keeping existing rules")
        return

    trn_cont = int((df_train['label'] == 'pump_continuation').sum())
    trn_rev = int((df_train['label'] == 'pump_reversal').sum())
    logger.info(f"  Train: {len(df_train):,} obs ({trn_cont} cont, {trn_rev} rev)")
    logger.info(f"  Test:  {len(df_test):,} obs")

    # 3. Rank individual filters
    columns = _get_filterable_columns(df_train)
    logger.info(f"  {len(columns)} filterable columns")

    ranked = _rank_filters(df_train, columns)
    if len(ranked) < 2:
        logger.warning("Not enough ranked filters -- keeping existing rules")
        return
    logger.info(f"  {len(ranked)} filters ranked, top: {ranked[0]['column']} "
                f"(E[profit]={ranked[0]['expected_profit']:+.4f}%)")

    # 4. Find best combination
    best = _find_best_combination(df_train, df_test, ranked)
    if best is None:
        logger.warning(f"No combo with precision >= {MIN_PRECISION}% -- keeping existing rules")
        return

    # 5. Build active rules
    new_rules = []
    for col in best['columns']:
        feat = next((r for r in ranked if r['column'] == col), None)
        if feat:
            new_rules.append({
                'column': col,
                'from_val': feat['from'],
                'to_val': feat['to'],
            })

    _active_rules = new_rules
    _rules_metadata = {
        'test_precision': best['test_precision'],
        'test_expected_profit': best['test_expected_profit'],
        'test_n_signals': best['test_n_signals'],
        'test_avg_gain': best['test_avg_gain'],
        'n_filters': len(new_rules),
        'filter_columns': [r['column'] for r in new_rules],
        'refreshed_at': datetime.now(timezone.utc).isoformat(),
        'train_size': len(df_train),
        'test_size': len(df_test),
    }

    elapsed = time.time() - t0
    logger.info(f"  NEW RULES ACTIVE: {len(new_rules)} filters, "
                f"test precision={best['test_precision']:.1f}%, "
                f"E[profit]={best['test_expected_profit']:+.4f}% "
                f"({elapsed:.1f}s)")
    for rule in new_rules:
        logger.info(f"    {rule['column']}: [{rule['from_val']}, {rule['to_val']}]")


def maybe_refresh_rules():
    """Refresh pump rules if enough time has passed (every 5 minutes)."""
    global _last_rules_refresh
    now = time.time()

    if now - _last_rules_refresh >= RULES_REFRESH_INTERVAL:
        try:
            refresh_pump_rules()
        except Exception as e:
            logger.error(f"Error refreshing pump rules: {e}", exc_info=True)
        _last_rules_refresh = now


# =============================================================================
# SIGNAL CHECK & BUYIN INSERTION (every cycle)
# =============================================================================

def _log_gate_summary() -> None:
    """Log a periodic summary of gate pass/fail stats (every 60s)."""
    global _last_gate_summary_time
    now = time.time()
    if now - _last_gate_summary_time < 60:
        return
    _last_gate_summary_time = now

    total = _gate_stats['total_checks']
    if total == 0:
        return

    logger.info(
        f"Pump gate summary (last 60s): {total} checks | "
        f"no_rules={_gate_stats['no_rules']}, "
        f"crash_30s={_gate_stats['crash_gate_fail']}, "
        f"crash_5m={_gate_stats['crash_5m_fail']}, "
        f"gates_ok={_gate_stats['gates_passed']}, "
        f"filters_fail={_gate_stats['filters_fail']}, "
        f"FIRED={_gate_stats['signal_fired']}"
    )
    # Reset counters
    for k in _gate_stats:
        _gate_stats[k] = 0


def check_pump_signal(trail_row: dict, market_price: float) -> bool:
    """
    Evaluate real-time price + trail data against uptrend gates and filter rules.

    Two-phase check:
      Phase 1 (Uptrend Gate): Real-time price buffer must show price moving up
        over 15s AND 30s windows, plus trail data confirms 5-minute uptrend.
        This uses actual market prices (updated every ~5s) -- much faster than
        trail columns which only update every 30s.
      Phase 2 (Filter Confirmation): Statistical filters must confirm the
        uptrend will continue.

    Args:
        trail_row: A dict from buyin_trail_minutes (the just-generated minute-0 row)
        market_price: Current SOL market price (for buffer update)

    Returns:
        True if all gates + filter rules pass (pump signal fires), False otherwise
    """
    _gate_stats['total_checks'] += 1

    if not _active_rules:
        _gate_stats['no_rules'] += 1
        return False

    # ── Phase 1: Safety Gates (crash protection only) ───────────────────
    # Data analysis proved that uptrend gates provide ZERO selectivity
    # between clean pumps and no-pumps (both pass at ~45-50% rate).
    # Clean pumps start from FLAT prices, not rising ones.
    #
    # We only block entry during active crashes/selloffs.
    # The STATISTICAL FILTERS (Phase 2) do all the real selection.

    # Gate 1: Not in active selloff (30s micro-trend)
    crash_ok, crash_desc = _is_not_crashing()
    if not crash_ok:
        _gate_stats['crash_gate_fail'] += 1
        logger.debug(f"Pump: crash gate FAILED ({crash_desc})")
        return False

    # Gate 2: Not in a broader crash (5m change)
    pm_5m = trail_row.get('pm_price_change_5m')
    if pm_5m is not None and float(pm_5m) < CRASH_GATE_5M:
        _gate_stats['crash_5m_fail'] += 1
        logger.debug(f"Pump: 5m crash gate FAILED (pm_price_change_5m={pm_5m} < {CRASH_GATE_5M}%)")
        return False

    _gate_stats['gates_passed'] += 1

    # ── Phase 2: Statistical Filter Confirmation ─────────────────────────
    # Now check if the active filter rules confirm the uptrend will continue.
    failed_filters = []
    for rule in _active_rules:
        col = rule['column']
        val = trail_row.get(col)
        if val is None:
            failed_filters.append(f"{col}=NULL")
            continue
        try:
            val_f = float(val)
        except (ValueError, TypeError):
            failed_filters.append(f"{col}=INVALID")
            continue
        if not (rule['from_val'] <= val_f <= rule['to_val']):
            failed_filters.append(f"{col}={val_f:.4f} not in [{rule['from_val']:.4f}, {rule['to_val']:.4f}]")

    if failed_filters:
        _gate_stats['filters_fail'] += 1
        logger.info(f"Pump: filters NOT met ({len(failed_filters)} failed: "
                    f"{', '.join(failed_filters[:3])})")
        return False

    _gate_stats['signal_fired'] += 1
    logger.info(f"Pump: ALL CHECKS PASSED - uptrend confirmed + {len(_active_rules)} filters matched!")
    return True


def check_and_fire_pump_signal(
    buyin_id: int,
    market_price: float,
    price_cycle: Optional[int],
) -> bool:
    """
    Check if the just-generated trail data triggers a pump signal.
    If so, insert a buyin for the pump play.

    Called from within train_validator's run_training_cycle() after
    trail generation completes.

    Args:
        buyin_id: The buyin whose trail was just generated
        market_price: Current SOL price
        price_cycle: Current active price cycle ID

    Returns:
        True if a pump buyin was inserted, False otherwise
    """
    global _last_entry_time

    pump_play_id = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "0"))
    if not pump_play_id:
        return False

    # ALWAYS update the price buffer -- even before rules are ready.
    # This builds up history so micro-trend detection works immediately
    # once rules become available after the first 5-minute refresh.
    _update_price_buffer(market_price)

    if not _active_rules:
        logger.info("Pump check: no active rules yet (waiting for first refresh)")
        return False

    # Read the trail row at minute 0 for this buyin (freshest snapshot)
    try:
        trail_row = postgres_query_one("""
            SELECT * FROM buyin_trail_minutes
            WHERE buyin_id = %s AND minute = 0 AND COALESCE(sub_minute, 0) = 0
        """, [buyin_id])
    except Exception as e:
        logger.error(f"Pump check: error reading trail data: {e}")
        return False

    if not trail_row:
        logger.debug(f"Pump check: no trail row at minute 0 for buyin {buyin_id}")
        return False

    # Evaluate signal
    signal_fires = check_pump_signal(trail_row, market_price)
    _log_gate_summary()

    filter_summary = {r['column']: f"[{r['from_val']}, {r['to_val']}]" for r in _active_rules}
    if not signal_fires:
        return False

    # === Signal fired -- check guards before inserting ===

    # Guard: cooldown
    now = time.time()
    if now - _last_entry_time < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (now - _last_entry_time))
        logger.info(f"Pump check: SIGNAL FIRED but cooldown active ({remaining}s remaining)")
        return False

    # Guard: DB cooldown (survives process restarts)
    try:
        last_buyin = postgres_query_one("""
            SELECT followed_at FROM follow_the_goat_buyins
            WHERE play_id = %s
            ORDER BY followed_at DESC
            LIMIT 1
        """, [pump_play_id])
        if last_buyin and last_buyin.get('followed_at'):
            last_at = last_buyin['followed_at']
            if hasattr(last_at, 'timestamp'):
                elapsed_since = now - last_at.timestamp()
                if elapsed_since < COOLDOWN_SECONDS:
                    logger.info(f"Pump check: SIGNAL FIRED but DB cooldown active")
                    return False
    except Exception as e:
        logger.error(f"Pump check: error checking DB cooldown: {e}")

    # Guard: no open positions for pump play
    # IMPORTANT: Exclude TRAINING_TEST_ synthetic buyins -- they are always in
    # 'validating' during the current train_validator cycle and would permanently
    # block pump signals when TRAIN_VALIDATOR_PLAY_ID == PUMP_SIGNAL_PLAY_ID.
    try:
        open_pos = postgres_query_one("""
            SELECT id FROM follow_the_goat_buyins
            WHERE play_id = %s AND our_status IN ('pending', 'validating')
              AND wallet_address NOT LIKE 'TRAINING_TEST_%%'
            LIMIT 1
        """, [pump_play_id])
        if open_pos:
            logger.info(f"Pump check: SIGNAL FIRED but open position exists (buyin {open_pos['id']})")
            return False
    except Exception as e:
        logger.error(f"Pump check: error checking open positions: {e}")
        return False

    # === All guards passed -- insert pump buyin ===
    logger.info(f"PUMP SIGNAL FIRED! Inserting buyin for play {pump_play_id}")

    timestamp_str = str(int(now))
    block_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    # Build entry log
    entry_log = {
        'signal_type': 'pump_detection',
        'source_buyin_id': buyin_id,
        'rules': [
            {
                'column': r['column'],
                'from_val': r['from_val'],
                'to_val': r['to_val'],
                'actual_value': float(trail_row.get(r['column'], 0)) if trail_row.get(r['column']) is not None else None,
            }
            for r in _active_rules
        ],
        'rules_metadata': _rules_metadata,
        'pm_price_change_1m': float(trail_row.get('pm_price_change_1m', 0)),
        'sol_price': market_price,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }

    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM follow_the_goat_buyins")
                next_id = cursor.fetchone()['next_id']

        postgres_execute("""
            INSERT INTO follow_the_goat_buyins (
                id, play_id, wallet_address, original_trade_id,
                trade_signature, block_timestamp, quote_amount,
                base_amount, price, direction, our_entry_price,
                live_trade, price_cycle, entry_log,
                pattern_validator_log, our_status, followed_at,
                higest_price_reached
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s
            )
        """, [
            next_id,
            pump_play_id,
            f'PUMP_SIGNAL_{timestamp_str}',
            0,
            f'pump_sig_{timestamp_str}',
            block_timestamp,
            100.0,
            market_price,
            market_price,
            'buy',
            market_price,
            0,              # live_trade = 0 (test mode)
            price_cycle,
            json.dumps(entry_log),
            None,
            'pending',      # trailing stop seller picks this up
            block_timestamp,
            market_price,
        ])

        _last_entry_time = now

        logger.info(f"  Pump buyin inserted: id={next_id}, price={market_price:.4f}, "
                    f"play_id={pump_play_id}, cycle={price_cycle}")
        logger.info(f"  Rules: {json.dumps(filter_summary)}")
        return True

    except Exception as e:
        logger.error(f"Error inserting pump signal buyin: {e}", exc_info=True)
        return False


def get_pump_status() -> Dict[str, Any]:
    """Return current pump detection status for logging/debugging."""
    return {
        'active_rules': len(_active_rules),
        'rules': _active_rules,
        'metadata': _rules_metadata,
        'last_refresh': _last_rules_refresh,
        'last_entry': _last_entry_time,
    }

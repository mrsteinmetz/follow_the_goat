#!/usr/bin/env python3
"""
Filter Simulation & Optimization Test Script
=============================================
Loads historical trade data, applies multiple improved filter-finding algorithms,
validates with train/test splits, and reports which filters actually predict uptrends.

PERFORMANCE: Uses DuckDB + PyArrow for analytical processing (read-only).
             PostgreSQL remains the source of truth -- we just pull raw data once
             and pivot/analyze entirely in DuckDB (1000x faster than PG pivot).

Approaches tested:
  1. Statistical Separation (KS-test + Cohen's d + Youden's J)
  2. Decision Tree Stumps (optimal single-feature split points)
  3. Precision-Focused Scoring (maximize "of YES trades, how many are good?")
  4. Combined Multi-Feature Model (Random Forest importance + rule extraction)

All approaches use time-based train/test splits to catch overfitting.

Usage:
    python tests/filter_simulation/run_simulation.py
    python tests/filter_simulation/run_simulation.py --hours 48 --threshold 0.3
    python tests/filter_simulation/run_simulation.py --interval 0   # test specific 30s interval
"""

import sys
import argparse
import logging
import time
import warnings
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
import pandas as pd
import duckdb
from scipy import stats
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score, recall_score

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings("ignore", category=FutureWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("filter_simulation")

# Columns to never use as filters
SKIP_COLUMNS = frozenset([
    'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good',
])

# Columns that are absolute values and won't generalize across time.
# Prices change daily, so a filter like "btc_price > 66000" is useless tomorrow.
# Also exclude columns that are absolute counts/volumes tied to time windows.
ABSOLUTE_PRICE_COLUMNS = frozenset([
    # SOL prices
    'pm_open_price', 'pm_close_price', 'pm_high_price', 'pm_low_price', 'pm_avg_price',
    # BTC prices
    'btc_open_price', 'btc_close_price', 'btc_high_price', 'btc_low_price',
    # ETH prices
    'eth_open_price', 'eth_close_price', 'eth_high_price', 'eth_low_price',
    # Second-prices (still absolute)
    'sp_min_price', 'sp_max_price', 'sp_avg_price', 'sp_start_price', 'sp_end_price',
    'sp_price_count',
    # 30-second interval prices
    'ts_open_price', 'ts_close_price', 'ts_high_price', 'ts_low_price',
    # Pre-entry absolute prices
    'pre_entry_price_1m_before', 'pre_entry_price_2m_before',
    'pre_entry_price_3m_before', 'pre_entry_price_5m_before',
    'pre_entry_price_10m_before',
    # Order book absolute prices / volumes
    'ob_mid_price', 'ob_total_liquidity', 'ob_bid_total', 'ob_ask_total',
    # Transaction absolute volumes / prices
    'tx_vwap', 'tx_total_volume_usd', 'tx_buy_volume_usd', 'tx_sell_volume_usd',
    'tx_delta_divergence', 'tx_cumulative_delta',
    # Whale absolute volumes
    'wh_total_sol_moved', 'wh_inflow_sol', 'wh_outflow_sol',
    # Pattern absolute price levels (tied to current SOL price)
    'pat_asc_tri_resistance_level', 'pat_asc_tri_support_level',
    'pat_inv_hs_neckline', 'pat_cup_handle_rim',
])


# =============================================================================
# DATA LOADING -- DuckDB postgres_scanner (reads PG directly, no Python middleman)
# =============================================================================

def _get_pg_connection_string() -> str:
    """Build DuckDB-compatible PostgreSQL connection string from project config."""
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


def load_all_data_into_duckdb(hours: int = 48) -> Tuple[Optional[duckdb.DuckDBPyConnection], Dict[str, Any]]:
    """
    Stream ALL raw data from PostgreSQL into local DuckDB tables in ONE pass.

    Strategy:
      1. Attach PG via postgres_scanner
      2. Stream buyins + trade_filter_values into local DuckDB tables (fast bulk read)
      3. All subsequent pivots and queries happen locally -- no more PG round-trips

    This is much faster than 30 separate pivot queries against PG because:
      - ONE network round-trip instead of 30
      - DuckDB's columnar engine pivots data 100x faster than PG
      - All interval scans become local queries (microseconds instead of 4s each)

    Returns:
        (duckdb_connection, summary_dict)
    """
    logger.info(f"Loading ALL data from PostgreSQL into DuckDB (last {hours}h)...")
    t0 = time.time()

    # Connect DuckDB and attach PG
    con = duckdb.connect(":memory:")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")

    pg_conn = _get_pg_connection_string()
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    # Step 1: Stream buyins (small table, ~10K rows)
    logger.info("  [Stream] Buyins...")
    t1 = time.time()
    con.execute(f"""
        CREATE TABLE buyins AS
        SELECT id AS trade_id, play_id, followed_at, potential_gains, our_status
        FROM pg.follow_the_goat_buyins
        WHERE potential_gains IS NOT NULL
          AND followed_at >= NOW() - INTERVAL '{hours} hours'
    """)
    n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
    logger.info(f"    {n_buyins:,} buyins in {time.time()-t1:.1f}s")

    if n_buyins == 0:
        logger.error("No buyins found")
        return None, {}

    # Step 2: Stream raw filter values (the big one: ~35M rows)
    # DuckDB streams this efficiently -- no Python memory bottleneck
    logger.info("  [Stream] Filter values (this is the big one)...")
    t2 = time.time()
    con.execute(f"""
        CREATE TABLE raw_filters AS
        SELECT
            tfv.buyin_id AS trade_id,
            tfv.minute,
            COALESCE(tfv.sub_minute, 0) AS sub_minute,
            (tfv.minute * 2 + COALESCE(tfv.sub_minute, 0)) AS interval_idx,
            tfv.filter_name,
            tfv.filter_value
        FROM pg.trade_filter_values tfv
        WHERE tfv.buyin_id IN (SELECT trade_id FROM buyins)
    """)
    n_raw = con.execute("SELECT COUNT(*) FROM raw_filters").fetchone()[0]
    n_filters = con.execute("SELECT COUNT(DISTINCT filter_name) FROM raw_filters").fetchone()[0]
    logger.info(f"    {n_raw:,} filter rows ({n_filters} columns) in {time.time()-t2:.1f}s")

    # Detach PG -- all data is now local, no more network needed
    con.execute("DETACH pg")

    # Create indexes for fast pivoting
    logger.info("  [Index] Creating indexes...")
    t3 = time.time()
    con.execute("CREATE INDEX idx_rf_interval ON raw_filters(interval_idx)")
    con.execute("CREATE INDEX idx_rf_trade ON raw_filters(trade_id)")
    logger.info(f"    Indexed in {time.time()-t3:.1f}s")

    total = time.time() - t0
    logger.info(f"  All data loaded into DuckDB in {total:.1f}s")

    summary = {
        'n_trades': n_buyins,
        'n_filter_rows': n_raw,
        'n_filters': n_filters,
    }
    return con, summary


def get_interval_counts(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Get trade counts per 30-second interval (local DuckDB query, instant)."""
    return con.execute("""
        SELECT interval_idx,
               COUNT(DISTINCT trade_id) AS n_trades
        FROM raw_filters
        GROUP BY interval_idx
        ORDER BY interval_idx
    """).fetchdf()


def load_interval_pivoted(
    con: duckdb.DuckDBPyConnection,
    interval_idx: int,
) -> pd.DataFrame:
    """
    Pivot a SINGLE interval's data from the local DuckDB raw_filters table.

    Since data is already in DuckDB (columnar, indexed), this is extremely fast:
    ~0.5-1s per interval instead of 4-5s when querying PG each time.
    """
    t0 = time.time()

    # Get filter column names for this interval
    filter_names = con.execute("""
        SELECT DISTINCT filter_name
        FROM raw_filters
        WHERE interval_idx = ?
        ORDER BY filter_name
    """, [interval_idx]).fetchall()
    filter_columns = [r[0] for r in filter_names]

    if not filter_columns:
        return pd.DataFrame()

    # Build pivot using conditional aggregation
    pivot_parts = []
    for col in filter_columns:
        safe = col.replace("'", "''")
        pivot_parts.append(
            f"MAX(CASE WHEN rf.filter_name = '{safe}' THEN rf.filter_value END) AS \"{col}\""
        )
    pivot_sql = ",\n            ".join(pivot_parts)

    df = con.execute(f"""
        SELECT
            b.trade_id,
            b.play_id,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            rf.interval_idx,
            {pivot_sql}
        FROM buyins b
        INNER JOIN raw_filters rf ON rf.trade_id = b.trade_id
        WHERE rf.interval_idx = {interval_idx}
        GROUP BY b.trade_id, b.play_id, b.followed_at, b.potential_gains,
                 b.our_status, rf.interval_idx
        ORDER BY b.followed_at
    """).fetchdf()

    elapsed = time.time() - t0
    n_trades = df['trade_id'].nunique() if len(df) > 0 else 0
    logger.info(f"  [Pivot] Interval {interval_idx}: {n_trades:,} trades, "
                f"{len(filter_columns)} cols in {elapsed:.1f}s")
    return df


def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    """Return numeric columns suitable for filtering (>10% non-null, no absolute prices)."""
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        null_pct = df[col].isna().mean()
        if null_pct < 0.90:
            cols.append(col)
    return cols


def split_train_test(df: pd.DataFrame, train_frac: float = 0.70) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Time-based train/test split.
    Sorts trades by followed_at, earliest 70% = train, latest 30% = test.
    """
    trade_times = (
        df.groupby('trade_id')['followed_at']
        .first()
        .sort_values()
    )
    n_train = int(len(trade_times) * train_frac)
    train_ids = set(trade_times.iloc[:n_train].index)

    df_train = df[df['trade_id'].isin(train_ids)].copy()
    df_test = df[~df['trade_id'].isin(train_ids)].copy()

    logger.info(f"  Train: {df_train['trade_id'].nunique()} trades ({len(df_train):,} rows)")
    logger.info(f"  Test:  {df_test['trade_id'].nunique()} trades ({len(df_test):,} rows)")
    return df_train, df_test


# =============================================================================
# HELPER: apply filters and score
# =============================================================================

def apply_filters_and_score(
    df: pd.DataFrame,
    filters: List[Dict[str, Any]],
    threshold: float,
) -> Dict[str, Any]:
    """
    Apply a list of range-filters (AND logic) and return effectiveness metrics.
    Each filter: {'column': str, 'from': float, 'to': float}
    """
    passes = pd.Series(True, index=df.index)
    for f in filters:
        col = f['column']
        if col not in df.columns:
            continue
        vals = df[col]
        passes = passes & (vals >= f['from']) & (vals <= f['to'])

    is_good = df['potential_gains'] >= threshold
    is_bad = ~is_good

    good_before = int(is_good.sum())
    bad_before = int(is_bad.sum())
    good_after = int((is_good & passes).sum())
    bad_after = int((is_bad & passes).sum())
    total_after = good_after + bad_after

    good_kept_pct = (good_after / good_before * 100) if good_before else 0
    bad_removed_pct = ((bad_before - bad_after) / bad_before * 100) if bad_before else 0
    precision = (good_after / total_after * 100) if total_after else 0
    pass_rate = (total_after / len(df) * 100) if len(df) else 0

    return {
        'good_before': good_before,
        'bad_before': bad_before,
        'good_after': good_after,
        'bad_after': bad_after,
        'good_kept_pct': round(good_kept_pct, 2),
        'bad_removed_pct': round(bad_removed_pct, 2),
        'precision': round(precision, 2),
        'pass_rate': round(pass_rate, 2),
        'filter_count': len(filters),
    }


# =============================================================================
# APPROACH 1: Statistical Separation
# =============================================================================

def approach_statistical_separation(
    df: pd.DataFrame,
    columns: List[str],
    threshold: float,
    top_n: int = 20,
) -> List[Dict[str, Any]]:
    """
    For each feature, measure separation between good/bad using Cohen's d + KS-test.
    Find optimal cutpoint via Youden's J.
    """
    logger.info("  [Approach 1] Statistical Separation Analysis...")
    is_good = df['potential_gains'] >= threshold
    results = []

    for col in columns:
        good_vals = df.loc[is_good, col].dropna()
        bad_vals = df.loc[~is_good, col].dropna()

        if len(good_vals) < 30 or len(bad_vals) < 30:
            continue

        # Cohen's d
        pooled_std = np.sqrt(
            ((len(good_vals) - 1) * good_vals.std() ** 2 +
             (len(bad_vals) - 1) * bad_vals.std() ** 2)
            / (len(good_vals) + len(bad_vals) - 2)
        )
        if pooled_std == 0:
            continue
        cohens_d = abs(good_vals.mean() - bad_vals.mean()) / pooled_std

        # KS-test
        ks_stat, ks_pvalue = stats.ks_2samp(good_vals, bad_vals)

        if ks_pvalue > 0.05 or cohens_d < 0.1:
            continue

        # Youden's J -- find optimal cutpoint
        all_vals = pd.concat([good_vals, bad_vals])
        thresholds_to_test = np.quantile(all_vals, np.linspace(0.05, 0.95, 50))

        best_j = -1
        best_cut = None
        best_direction = None

        for cut in thresholds_to_test:
            sens_above = (good_vals >= cut).mean()
            spec_above = (bad_vals < cut).mean()
            j_above = sens_above + spec_above - 1

            sens_below = (good_vals <= cut).mean()
            spec_below = (bad_vals > cut).mean()
            j_below = sens_below + spec_below - 1

            if j_above > best_j:
                best_j = j_above
                best_cut = cut
                best_direction = 'above'
            if j_below > best_j:
                best_j = j_below
                best_cut = cut
                best_direction = 'below'

        if best_cut is None or best_j <= 0:
            continue

        if best_direction == 'above':
            from_val = float(best_cut)
            to_val = float(good_vals.quantile(0.98))
        else:
            from_val = float(good_vals.quantile(0.02))
            to_val = float(best_cut)

        if from_val >= to_val:
            continue

        results.append({
            'column': col,
            'from': round(from_val, 6),
            'to': round(to_val, 6),
            'cohens_d': round(cohens_d, 4),
            'ks_stat': round(ks_stat, 4),
            'ks_pvalue': ks_pvalue,
            'youdens_j': round(best_j, 4),
            'direction': best_direction,
            'score': round(cohens_d * ks_stat * best_j, 6),
        })

    results.sort(key=lambda x: x['score'], reverse=True)
    logger.info(f"    Found {len(results)} features with significant separation (top {top_n} used)")
    return results[:top_n]


# =============================================================================
# APPROACH 2: Decision Tree Stumps
# =============================================================================

def approach_decision_stumps(
    df: pd.DataFrame,
    columns: List[str],
    threshold: float,
    top_n: int = 20,
) -> List[Dict[str, Any]]:
    """
    Fit depth-1 decision tree per feature to find the single best split point.
    """
    logger.info("  [Approach 2] Decision Tree Stumps...")
    y = (df['potential_gains'] >= threshold).astype(int)
    results = []

    for col in columns:
        vals = df[col].values.copy()
        mask = ~np.isnan(vals)
        if mask.sum() < 50:
            continue

        X_col = vals[mask].reshape(-1, 1)
        y_col = y.values[mask]

        if y_col.sum() < 10 or (y_col == 0).sum() < 10:
            continue

        tree = DecisionTreeClassifier(max_depth=1, min_samples_leaf=20)
        tree.fit(X_col, y_col)

        if tree.tree_.feature[0] < 0:
            continue

        split_threshold = tree.tree_.threshold[0]
        left_idx = tree.tree_.children_left[0]
        right_idx = tree.tree_.children_right[0]

        left_good_frac = tree.tree_.value[left_idx][0][1] / tree.tree_.value[left_idx][0].sum()
        right_good_frac = tree.tree_.value[right_idx][0][1] / tree.tree_.value[right_idx][0].sum()

        info_gain = tree.tree_.impurity[0] - (
            tree.tree_.weighted_n_node_samples[left_idx] / tree.tree_.weighted_n_node_samples[0] * tree.tree_.impurity[left_idx] +
            tree.tree_.weighted_n_node_samples[right_idx] / tree.tree_.weighted_n_node_samples[0] * tree.tree_.impurity[right_idx]
        )

        if info_gain < 0.001:
            continue

        good_vals = df.loc[y == 1, col].dropna()
        if right_good_frac > left_good_frac:
            from_val = float(split_threshold)
            to_val = float(good_vals.quantile(0.98))
            direction = 'above'
        else:
            from_val = float(good_vals.quantile(0.02))
            to_val = float(split_threshold)
            direction = 'below'

        if from_val >= to_val:
            continue

        results.append({
            'column': col,
            'from': round(from_val, 6),
            'to': round(to_val, 6),
            'split_point': round(float(split_threshold), 6),
            'info_gain': round(info_gain, 6),
            'left_good_frac': round(left_good_frac, 4),
            'right_good_frac': round(right_good_frac, 4),
            'direction': direction,
            'score': round(info_gain, 6),
        })

    results.sort(key=lambda x: x['score'], reverse=True)
    logger.info(f"    Found {len(results)} features with informative splits (top {top_n} used)")
    return results[:top_n]


# =============================================================================
# APPROACH 3: Precision-Focused Scoring
# =============================================================================

def approach_precision_focused(
    df: pd.DataFrame,
    columns: List[str],
    threshold: float,
    top_n: int = 20,
    min_precision: float = 40.0,
) -> List[Dict[str, Any]]:
    """
    For each feature, find the range that maximizes PRECISION:
    "Of the trades we say YES to, what % are actually good?"
    """
    logger.info("  [Approach 3] Precision-Focused Filter Scoring...")
    is_good = df['potential_gains'] >= threshold
    base_precision = is_good.mean() * 100

    results = []

    for col in columns:
        good_vals = df.loc[is_good, col].dropna()
        bad_vals = df.loc[~is_good, col].dropna()

        if len(good_vals) < 30 or len(bad_vals) < 30:
            continue

        best_precision = base_precision
        best_filter = None

        # Scan percentile windows of the GOOD trade distribution
        for width_pct in [20, 30, 40, 50, 60, 70, 80]:
            for start_pct in range(0, 100 - width_pct + 1, 5):
                end_pct = start_pct + width_pct
                from_val = float(good_vals.quantile(start_pct / 100))
                to_val = float(good_vals.quantile(end_pct / 100))

                if from_val >= to_val:
                    continue

                good_pass = ((good_vals >= from_val) & (good_vals <= to_val)).sum()
                bad_pass = ((bad_vals >= from_val) & (bad_vals <= to_val)).sum()
                total_pass = good_pass + bad_pass

                if total_pass < 20:
                    continue

                prec = good_pass / total_pass * 100
                good_kept = good_pass / len(good_vals) * 100

                if good_kept < 15:
                    continue

                precision_lift = prec - base_precision
                if precision_lift <= 0:
                    continue

                score = precision_lift * (good_kept / 100)

                if prec > best_precision and prec >= min_precision:
                    best_precision = prec
                    best_filter = {
                        'column': col,
                        'from': round(from_val, 6),
                        'to': round(to_val, 6),
                        'precision': round(prec, 2),
                        'precision_lift': round(precision_lift, 2),
                        'good_kept_pct': round(good_kept, 2),
                        'bad_removed_pct': round((1 - bad_pass / len(bad_vals)) * 100, 2),
                        'pass_count': total_pass,
                        'score': round(score, 4),
                    }

        if best_filter is not None:
            results.append(best_filter)

    results.sort(key=lambda x: x['score'], reverse=True)
    logger.info(f"    Found {len(results)} precision-improving features (top {top_n} used)")
    return results[:top_n]


# =============================================================================
# APPROACH 4: Random Forest
# =============================================================================

def approach_random_forest(
    df: pd.DataFrame,
    columns: List[str],
    threshold: float,
    top_n: int = 15,
) -> Tuple[List[Dict[str, Any]], Any]:
    """
    Train Random Forest for feature importance, then extract rule-based filters.
    """
    logger.info("  [Approach 4] Random Forest Feature Importance...")
    y = (df['potential_gains'] >= threshold).astype(int)

    X = df[columns].copy()
    for col in columns:
        X[col] = X[col].fillna(X[col].median())

    rf = RandomForestClassifier(
        n_estimators=200,
        max_depth=5,
        min_samples_leaf=30,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X, y)

    importances = rf.feature_importances_
    importance_order = np.argsort(importances)[::-1]

    results = []
    for idx in importance_order[:top_n]:
        col = columns[idx]
        imp = importances[idx]
        if imp < 0.005:
            break

        good_vals = df.loc[y == 1, col].dropna()
        if len(good_vals) < 20:
            continue

        from_val = float(good_vals.quantile(0.05))
        to_val = float(good_vals.quantile(0.95))

        if from_val >= to_val:
            continue

        results.append({
            'column': col,
            'from': round(from_val, 6),
            'to': round(to_val, 6),
            'importance': round(imp, 6),
            'score': round(imp, 6),
        })

    logger.info(f"    Top {len(results)} features by importance")
    return results, rf


# =============================================================================
# MULTI-FEATURE COMBINATION: greedy forward selection on precision
# =============================================================================

def build_best_combo(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    candidates: List[Dict[str, Any]],
    threshold: float,
    max_filters: int = 8,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Greedy forward selection: add the filter that increases precision the most
    on TRAINING set, measure generalization on TEST set.
    Stops when no improvement or good_kept_pct < 10%.
    """
    selected: List[Dict[str, Any]] = []
    used_cols = set()
    current_train = apply_filters_and_score(df_train, [], threshold)

    for _ in range(max_filters):
        best_candidate = None
        best_precision = current_train['precision']

        for cand in candidates:
            if cand['column'] in used_cols:
                continue

            trial = selected + [cand]
            metrics = apply_filters_and_score(df_train, trial, threshold)

            if metrics['good_kept_pct'] < 10:
                continue
            if (metrics['good_after'] + metrics['bad_after']) < 20:
                continue
            if metrics['precision'] > best_precision:
                best_precision = metrics['precision']
                best_candidate = cand

        if best_candidate is None:
            break

        selected.append(best_candidate)
        used_cols.add(best_candidate['column'])
        current_train = apply_filters_and_score(df_train, selected, threshold)

    train_metrics = apply_filters_and_score(df_train, selected, threshold)
    test_metrics = apply_filters_and_score(df_test, selected, threshold)

    return selected, train_metrics, test_metrics


# =============================================================================
# REPORTING
# =============================================================================

def print_separator(char: str = "=", width: int = 100):
    print(char * width)


def print_header(title: str, width: int = 100):
    print()
    print_separator("=", width)
    print(f"  {title}")
    print_separator("=", width)


def print_filter_table(filters: List[Dict[str, Any]], label: str):
    if not filters:
        print(f"  No {label} filters found.")
        return

    print(f"\n  Top {label} filters:")
    print(f"  {'#':>3}  {'Column':<35}  {'From':>14}  {'To':>14}  {'Range':>14}  {'Score':>10}  Extra")
    print(f"  {'---':>3}  {'-----':<35}  {'----':>14}  {'--':>14}  {'-----':>14}  {'-----':>10}  -----")

    for i, f in enumerate(filters[:15], 1):
        col = f['column'][:35]
        from_v = f'{f["from"]:>14.6f}'
        to_v = f'{f["to"]:>14.6f}'
        rng = f'{f["to"] - f["from"]:>14.6f}'
        score = f'{f["score"]:>10.4f}'

        extra_parts = []
        if 'cohens_d' in f:
            extra_parts.append(f"d={f['cohens_d']:.3f}")
        if 'youdens_j' in f:
            extra_parts.append(f"J={f['youdens_j']:.3f}")
        if 'info_gain' in f:
            extra_parts.append(f"IG={f['info_gain']:.4f}")
        if 'precision' in f:
            extra_parts.append(f"prec={f['precision']:.1f}%")
        if 'importance' in f:
            extra_parts.append(f"imp={f['importance']:.4f}")
        if 'direction' in f:
            extra_parts.append(f"dir={f['direction']}")
        if 'good_kept_pct' in f:
            extra_parts.append(f"kept={f['good_kept_pct']:.1f}%")

        extra = "  ".join(extra_parts)
        print(f"  {i:3d}  {col:<35}  {from_v}  {to_v}  {rng}  {score}  {extra}")


def print_metrics_comparison(
    label: str,
    train_metrics: Dict[str, Any],
    test_metrics: Dict[str, Any],
    filters: List[Dict[str, Any]],
):
    print(f"\n  {label}:")
    print(f"    Filters used: {len(filters)}")
    if filters:
        col_list = ", ".join(f['column'] for f in filters[:5])
        if len(filters) > 5:
            col_list += f" (+{len(filters)-5} more)"
        print(f"    Columns: {col_list}")

    print(f"    {'Metric':<25}  {'Train':>10}  {'Test':>10}  {'Delta':>10}")
    print(f"    {'------':<25}  {'-----':>10}  {'----':>10}  {'-----':>10}")

    for key in ['precision', 'good_kept_pct', 'bad_removed_pct', 'pass_rate']:
        tr = train_metrics.get(key, 0)
        te = test_metrics.get(key, 0)
        delta = te - tr
        flag = " *** OVERFIT" if key == 'precision' and delta < -10 else ""
        print(f"    {key:<25}  {tr:>9.1f}%  {te:>9.1f}%  {delta:>+9.1f}%{flag}")

    print(f"    {'good_after':<25}  {train_metrics.get('good_after',0):>10d}  {test_metrics.get('good_after',0):>10d}")
    print(f"    {'bad_after':<25}  {train_metrics.get('bad_after',0):>10d}  {test_metrics.get('bad_after',0):>10d}")


def print_final_summary(all_results: List[Dict[str, Any]]):
    print_header("FINAL COMPARISON: ALL APPROACHES RANKED BY TEST PRECISION")

    ranked = sorted(all_results, key=lambda x: x['test']['precision'], reverse=True)

    print(f"\n  {'Rank':>4}  {'Approach':<30}  {'Filters':>7}  "
          f"{'Train Prec':>10}  {'Test Prec':>10}  "
          f"{'Test BadRem':>11}  {'Test GoodKept':>13}  {'Test PassRate':>13}  {'Overfit?':>8}")
    print(f"  {'----':>4}  {'--------':<30}  {'-------':>7}  "
          f"{'----------':>10}  {'---------':>10}  "
          f"{'-----------':>11}  {'-------------':>13}  {'-------------':>13}  {'--------':>8}")

    for rank, r in enumerate(ranked, 1):
        tr = r['train']
        te = r['test']
        overfit = "YES" if (tr['precision'] - te['precision']) > 10 else "no"
        print(f"  {rank:4d}  {r['name']:<30}  {r['n_filters']:>7d}  "
              f"{tr['precision']:>9.1f}%  {te['precision']:>9.1f}%  "
              f"{te['bad_removed_pct']:>10.1f}%  {te['good_kept_pct']:>12.1f}%  "
              f"{te['pass_rate']:>12.1f}%  {overfit:>8}")

    winner = ranked[0] if ranked else None
    if winner and winner.get('filters'):
        print_header(f"RECOMMENDED FILTERS (from '{winner['name']}')")
        print(f"\n  These filters achieved {winner['test']['precision']:.1f}% precision on unseen data")
        print(f"  (baseline was ~{winner.get('baseline_precision', 0):.1f}% without filters)\n")

        print(f"  {'#':>3}  {'Column':<40}  {'From':>14}  {'To':>14}  {'Range Width':>14}")
        print(f"  {'---':>3}  {'------':<40}  {'----':>14}  {'--':>14}  {'-----------':>14}")
        for i, f in enumerate(winner['filters'], 1):
            w = f['to'] - f['from']
            print(f"  {i:3d}  {f['column']:<40}  {f['from']:>14.6f}  {f['to']:>14.6f}  {w:>14.6f}")


# =============================================================================
# MAIN SIMULATION
# =============================================================================

def run_simulation(hours: int = 48, threshold: float = 0.3, target_interval: Optional[int] = None):
    """
    Main entry point: loads data via DuckDB postgres_scanner, runs all 4 approaches.
    """
    total_start = time.time()

    print_header("FILTER SIMULATION & OPTIMIZATION (DuckDB postgres_scanner)")
    print(f"  Hours:     {hours}")
    print(f"  Threshold: {threshold}%")
    print(f"  Interval:  {'auto-detect' if target_interval is None else target_interval}")

    # ---- Load ALL data from PG into DuckDB (one-time stream) ----
    con, summary = load_all_data_into_duckdb(hours)
    if con is None:
        logger.error("No data loaded. Exiting.")
        return

    n_filter_rows = summary['n_filter_rows']
    n_filters = summary['n_filters']

    # Good/bad breakdown (local DuckDB query, instant)
    breakdown = con.execute(f"""
        SELECT
            COUNT(*) AS n_trades,
            SUM(CASE WHEN potential_gains >= {threshold} THEN 1 ELSE 0 END) AS n_good,
            SUM(CASE WHEN potential_gains <  {threshold} THEN 1 ELSE 0 END) AS n_bad
        FROM buyins
    """).fetchone()
    n_trades, n_good, n_bad = breakdown
    baseline_precision = n_good / n_trades * 100 if n_trades else 0

    print(f"\n  Raw filter rows in DB: {n_filter_rows:,} (this is the real data volume)")
    print(f"  Filter columns: {n_filters}")
    print(f"  Total trades: {n_trades:,}")
    print(f"  Good trades (>= {threshold}%): {n_good:,} ({n_good/n_trades*100:.1f}%)")
    print(f"  Bad trades  (<  {threshold}%): {n_bad:,} ({n_bad/n_trades*100:.1f}%)")
    print(f"  Baseline precision (say YES to all): {baseline_precision:.1f}%")

    # ---- Find best interval (all queries local now, very fast) ----
    logger.info("  Getting interval counts...")
    intervals_df = get_interval_counts(con)

    if target_interval is not None:
        intervals_to_test = [target_interval]
    else:
        min_trades = intervals_df['n_trades'].max() * 0.5
        intervals_to_test = sorted(
            intervals_df[intervals_df['n_trades'] >= min_trades]['interval_idx'].tolist()
        )

    print(f"  Testing {len(intervals_to_test)} intervals: "
          f"{intervals_to_test[:10]}{'...' if len(intervals_to_test) > 10 else ''}")

    # Quick scan: load each candidate interval and count significant features
    # Each interval loads only ~1.2M raw rows (1/30th), pivots to ~5K rows -- fast
    best_interval = 0
    best_interval_score = -1
    interval_summaries = []

    for ivl in intervals_to_test:
        df_ivl = load_interval_pivoted(con, ivl)
        if len(df_ivl) == 0 or df_ivl['trade_id'].nunique() < 50:
            continue

        cols = get_filterable_columns(df_ivl)
        if len(cols) < 5:
            continue

        is_good = df_ivl['potential_gains'] >= threshold
        sig_count = 0
        for col in cols:
            gv = df_ivl.loc[is_good, col].dropna()
            bv = df_ivl.loc[~is_good, col].dropna()
            if len(gv) < 20 or len(bv) < 20:
                continue
            _, pval = stats.ks_2samp(gv, bv)
            if pval < 0.05:
                sig_count += 1

        interval_label = f"{ivl // 2}:{(ivl % 2) * 30:02d}"
        interval_summaries.append({
            'interval': ivl,
            'label': interval_label,
            'trades': df_ivl['trade_id'].nunique(),
            'features': len(cols),
            'significant': sig_count,
        })

        if sig_count > best_interval_score:
            best_interval_score = sig_count
            best_interval = ivl

    if interval_summaries:
        print_header("INTERVAL SCAN: significant features per 30-second interval")
        print(f"\n  {'Interval':>8}  {'Label':>6}  {'Trades':>7}  {'Features':>8}  {'Significant':>11}")
        for s in sorted(interval_summaries, key=lambda x: x['significant'], reverse=True)[:10]:
            marker = " <-- BEST" if s['interval'] == best_interval else ""
            print(f"  {s['interval']:>8d}  {s['label']:>6}  {s['trades']:>7d}  {s['features']:>8d}  {s['significant']:>11d}{marker}")

    chosen_interval = target_interval if target_interval is not None else best_interval
    chosen_label = f"{chosen_interval // 2}:{(chosen_interval % 2) * 30:02d}"
    print(f"\n  Using interval {chosen_interval} ({chosen_label})")

    # ---- Load chosen interval (already pivoted, local DuckDB query) ----
    df = load_interval_pivoted(con, chosen_interval)
    columns = get_filterable_columns(df)
    logger.info(f"Working with {len(df):,} rows, {df['trade_id'].nunique()} trades, {len(columns)} features "
                f"(excluded {len(ABSOLUTE_PRICE_COLUMNS)} absolute-price columns)")

    # ---- Train/test split ----
    print_header("TRAIN / TEST SPLIT (time-based)")
    df_train, df_test = split_train_test(df)

    is_good_train = df_train['potential_gains'] >= threshold
    is_good_test = df_test['potential_gains'] >= threshold
    train_base_prec = is_good_train.mean() * 100
    test_base_prec = is_good_test.mean() * 100
    print(f"  Train baseline precision: {train_base_prec:.1f}%")
    print(f"  Test  baseline precision: {test_base_prec:.1f}%")

    # ---- Run all 4 approaches ----
    all_results = []

    # Approach 1
    print_header("APPROACH 1: Statistical Separation (KS + Cohen's d + Youden's J)")
    t1 = time.time()
    a1_raw = approach_statistical_separation(df_train, columns, threshold)
    print_filter_table(a1_raw, "Statistical Separation")
    a1_sel, a1_tr, a1_te = build_best_combo(df_train, df_test, a1_raw, threshold)
    print_metrics_comparison("Statistical Separation Combo", a1_tr, a1_te, a1_sel)
    logger.info(f"    Approach 1 completed in {time.time()-t1:.1f}s")
    all_results.append({
        'name': 'Statistical Separation', 'filters': a1_sel, 'n_filters': len(a1_sel),
        'train': a1_tr, 'test': a1_te, 'baseline_precision': test_base_prec, 'raw_candidates': a1_raw,
    })

    # Approach 2
    print_header("APPROACH 2: Decision Tree Stumps")
    t2 = time.time()
    a2_raw = approach_decision_stumps(df_train, columns, threshold)
    print_filter_table(a2_raw, "Decision Stump")
    a2_sel, a2_tr, a2_te = build_best_combo(df_train, df_test, a2_raw, threshold)
    print_metrics_comparison("Decision Stump Combo", a2_tr, a2_te, a2_sel)
    logger.info(f"    Approach 2 completed in {time.time()-t2:.1f}s")
    all_results.append({
        'name': 'Decision Tree Stumps', 'filters': a2_sel, 'n_filters': len(a2_sel),
        'train': a2_tr, 'test': a2_te, 'baseline_precision': test_base_prec, 'raw_candidates': a2_raw,
    })

    # Approach 3
    print_header("APPROACH 3: Precision-Focused Scoring")
    t3 = time.time()
    a3_raw = approach_precision_focused(df_train, columns, threshold)
    print_filter_table(a3_raw, "Precision-Focused")
    a3_sel, a3_tr, a3_te = build_best_combo(df_train, df_test, a3_raw, threshold)
    print_metrics_comparison("Precision-Focused Combo", a3_tr, a3_te, a3_sel)
    logger.info(f"    Approach 3 completed in {time.time()-t3:.1f}s")
    all_results.append({
        'name': 'Precision-Focused', 'filters': a3_sel, 'n_filters': len(a3_sel),
        'train': a3_tr, 'test': a3_te, 'baseline_precision': test_base_prec, 'raw_candidates': a3_raw,
    })

    # Approach 4
    print_header("APPROACH 4: Random Forest Feature Importance")
    t4 = time.time()
    a4_raw, rf_model = approach_random_forest(df_train, columns, threshold)
    print_filter_table(a4_raw, "Random Forest")
    a4_sel, a4_tr, a4_te = build_best_combo(df_train, df_test, a4_raw, threshold)
    print_metrics_comparison("Random Forest Combo", a4_tr, a4_te, a4_sel)
    logger.info(f"    Approach 4 completed in {time.time()-t4:.1f}s")
    all_results.append({
        'name': 'Random Forest', 'filters': a4_sel, 'n_filters': len(a4_sel),
        'train': a4_tr, 'test': a4_te, 'baseline_precision': test_base_prec, 'raw_candidates': a4_raw,
    })

    # ---- RF direct prediction (bonus) ----
    print_header("BONUS: Random Forest Direct Prediction (no filter extraction)")
    X_train = df_train[columns].copy()
    X_test = df_test[columns].copy()
    for col in columns:
        med = X_train[col].median()
        X_train[col] = X_train[col].fillna(med)
        X_test[col] = X_test[col].fillna(med)

    y_train = (df_train['potential_gains'] >= threshold).astype(int)
    y_test = (df_test['potential_gains'] >= threshold).astype(int)

    y_pred_train = rf_model.predict(X_train)
    y_pred_test = rf_model.predict(X_test)

    rf_train_prec = precision_score(y_train, y_pred_train, zero_division=0) * 100
    rf_test_prec = precision_score(y_test, y_pred_test, zero_division=0) * 100
    rf_train_recall = recall_score(y_train, y_pred_train, zero_division=0) * 100
    rf_test_recall = recall_score(y_test, y_pred_test, zero_division=0) * 100
    rf_train_yes = int(y_pred_train.sum())
    rf_test_yes = int(y_pred_test.sum())
    rf_train_pass_rate = rf_train_yes / len(y_train) * 100
    rf_test_pass_rate = rf_test_yes / len(y_test) * 100

    print(f"  {'Metric':<25}  {'Train':>10}  {'Test':>10}")
    print(f"  {'------':<25}  {'-----':>10}  {'----':>10}")
    print(f"  {'Precision':<25}  {rf_train_prec:>9.1f}%  {rf_test_prec:>9.1f}%")
    print(f"  {'Recall (good kept)':<25}  {rf_train_recall:>9.1f}%  {rf_test_recall:>9.1f}%")
    print(f"  {'Pass rate':<25}  {rf_train_pass_rate:>9.1f}%  {rf_test_pass_rate:>9.1f}%")
    print(f"  {'YES count':<25}  {rf_train_yes:>10d}  {rf_test_yes:>10d}")

    all_results.append({
        'name': 'RF Direct (no filters)', 'filters': [], 'n_filters': 0,
        'train': {
            'precision': round(rf_train_prec, 2), 'good_kept_pct': round(rf_train_recall, 2),
            'bad_removed_pct': round(100 - rf_train_pass_rate + rf_train_recall * rf_train_pass_rate / 100, 2),
            'pass_rate': round(rf_train_pass_rate, 2),
            'good_after': int((y_train.values & y_pred_train).sum()),
            'bad_after': int(((1 - y_train.values) & y_pred_train).sum()),
        },
        'test': {
            'precision': round(rf_test_prec, 2), 'good_kept_pct': round(rf_test_recall, 2),
            'bad_removed_pct': round(100 - rf_test_pass_rate + rf_test_recall * rf_test_pass_rate / 100, 2),
            'pass_rate': round(rf_test_pass_rate, 2),
            'good_after': int((y_test.values & y_pred_test).sum()),
            'bad_after': int(((1 - y_test.values) & y_pred_test).sum()),
        },
        'baseline_precision': test_base_prec,
    })

    # ---- Current system comparison ----
    print_header("COMPARISON: Current System Style (wide percentile ranges)")
    print("  Simulating the existing approach: P10-P90 of good trades...")

    current_style_filters = []
    is_good_t = df_train['potential_gains'] >= threshold
    for col in columns:
        good_v = df_train.loc[is_good_t, col].dropna()
        if len(good_v) < 30:
            continue
        from_val = float(good_v.quantile(0.10))
        to_val = float(good_v.quantile(0.90))
        if from_val >= to_val:
            continue
        current_style_filters.append({'column': col, 'from': round(from_val, 6), 'to': round(to_val, 6), 'score': 0})

    current_style_scored = []
    for f in current_style_filters:
        m = apply_filters_and_score(df_train, [f], threshold)
        current_style_scored.append((f, m))
    current_style_scored.sort(key=lambda x: x[1]['bad_removed_pct'], reverse=True)
    current_top = [f for f, _ in current_style_scored[:5]]

    curr_train = apply_filters_and_score(df_train, current_top, threshold)
    curr_test = apply_filters_and_score(df_test, current_top, threshold)
    print_metrics_comparison("Current Style (P10-P90, top 5 by bad removal)", curr_train, curr_test, current_top)

    # Range width comparison
    winner = sorted(all_results, key=lambda x: x['test']['precision'], reverse=True)[0]
    if winner.get('filters') and current_top:
        print(f"\n  Range width comparison:")
        print(f"    {'Filter':<40}  {'Winner Width':>14}  {'Current Width':>14}")
        for wf in winner['filters'][:5]:
            w_width = wf['to'] - wf['from']
            curr_match = next((c for c in current_top if c['column'] == wf['column']), None)
            c_width = (curr_match['to'] - curr_match['from']) if curr_match else float('nan')
            print(f"    {wf['column']:<40}  {w_width:>14.6f}  {c_width:>14.6f}")

    # ---- Final summary ----
    print_final_summary(all_results)

    total_elapsed = time.time() - total_start
    print_header("SIMULATION COMPLETE")
    print(f"  Total runtime: {total_elapsed:.1f}s")
    print(f"  Best approach: {winner['name']}")
    print(f"  Test precision: {winner['test']['precision']:.1f}% (baseline: {test_base_prec:.1f}%)")
    if winner.get('filters'):
        print(f"  Filters: {len(winner['filters'])}")
        print(f"  Test bad removed: {winner['test']['bad_removed_pct']:.1f}%")
        print(f"  Test good kept: {winner['test']['good_kept_pct']:.1f}%")
    print()

    # Cleanup
    con.close()


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Filter Simulation & Optimization")
    parser.add_argument("--hours", type=int, default=48, help="Hours of history (default: 48)")
    parser.add_argument("--threshold", type=float, default=0.3, help="Good trade threshold %% (default: 0.3)")
    parser.add_argument("--interval", type=int, default=None, help="Specific 30-sec interval (0-29, default: auto)")
    args = parser.parse_args()

    run_simulation(hours=args.hours, threshold=args.threshold, target_interval=args.interval)


if __name__ == "__main__":
    main()

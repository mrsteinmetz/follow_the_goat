#!/usr/bin/env python3
"""
Overnight Brute-Force Filter Sweep
====================================
Exhaustively tests filter combinations across ALL 30 intervals at 0.3% threshold.
Saves ranked results to CSV for morning review.

Search strategy per interval:
  1. Pre-screen: rank features by Cohen's d, keep top 40
  2. Exhaustive pairs from top 40         =    780 combos
  3. Exhaustive triples from top 30       =  4,060 combos
  4. Exhaustive quads from top 20         =  4,845 combos
  5. Exhaustive 5-combos from top 15      =  3,003 combos
  6. Random 6-8 combos from top 30        = 10,000 combos
  Total per interval: ~22,700 combos x 30 intervals = ~681,000 tested

Each combo is scored on train AND test (time-based 70/30 split).
Only combos that beat baseline precision on the test set are saved.

Usage:
    # Foreground (watch progress):
    python tests/filter_simulation/overnight_sweep.py

    # Background overnight:
    nohup python3 tests/filter_simulation/overnight_sweep.py > tests/filter_simulation/results/sweep_log.txt 2>&1 &

    # Quick test on 1 interval:
    python tests/filter_simulation/overnight_sweep.py --test
"""

import sys
import os
import csv
import time
import random
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

# Setup logging - flush every line for nohup visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("overnight_sweep")

# Fixed parameters
THRESHOLD = 0.3
HOURS = 48
TRAIN_FRAC = 0.70

# Reuse exclusion lists from run_simulation
SKIP_COLUMNS = frozenset([
    'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good',
])

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
# DATA LOADING (reuse from run_simulation.py)
# =============================================================================

def _get_pg_connection_string() -> str:
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


def load_all_data_into_duckdb() -> Tuple[Optional[duckdb.DuckDBPyConnection], Dict[str, Any]]:
    """Stream all data from PostgreSQL into local DuckDB."""
    logger.info(f"Loading ALL data from PostgreSQL into DuckDB (last {HOURS}h)...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")

    pg_conn = _get_pg_connection_string()
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

    logger.info("  [Stream] Buyins...")
    con.execute(f"""
        CREATE TABLE buyins AS
        SELECT id AS trade_id, play_id, followed_at, potential_gains, our_status
        FROM pg.follow_the_goat_buyins
        WHERE potential_gains IS NOT NULL
          AND followed_at >= NOW() - INTERVAL '{HOURS} hours'
    """)
    n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
    logger.info(f"    {n_buyins:,} buyins")

    if n_buyins == 0:
        return None, {}

    logger.info("  [Stream] Filter values...")
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

    con.execute("DETACH pg")

    logger.info("  [Index] Creating indexes...")
    con.execute("CREATE INDEX idx_rf_interval ON raw_filters(interval_idx)")
    con.execute("CREATE INDEX idx_rf_trade ON raw_filters(trade_id)")

    logger.info(f"  All data loaded in {time.time()-t0:.1f}s")
    return con, {'n_trades': n_buyins, 'n_filter_rows': n_raw, 'n_filters': n_filters}


def load_interval_pivoted(con: duckdb.DuckDBPyConnection, interval_idx: int) -> pd.DataFrame:
    """Pivot a single interval from local DuckDB."""
    filter_names = con.execute(
        "SELECT DISTINCT filter_name FROM raw_filters WHERE interval_idx = ? ORDER BY filter_name",
        [interval_idx]
    ).fetchall()
    filter_columns = [r[0] for r in filter_names]
    if not filter_columns:
        return pd.DataFrame()

    pivot_parts = []
    for col in filter_columns:
        safe = col.replace("'", "''")
        pivot_parts.append(f"MAX(CASE WHEN rf.filter_name = '{safe}' THEN rf.filter_value END) AS \"{col}\"")
    pivot_sql = ",\n            ".join(pivot_parts)

    return con.execute(f"""
        SELECT b.trade_id, b.play_id, b.followed_at, b.potential_gains, b.our_status,
               rf.interval_idx, {pivot_sql}
        FROM buyins b
        INNER JOIN raw_filters rf ON rf.trade_id = b.trade_id
        WHERE rf.interval_idx = {interval_idx}
        GROUP BY b.trade_id, b.play_id, b.followed_at, b.potential_gains, b.our_status, rf.interval_idx
        ORDER BY b.followed_at
    """).fetchdf()


def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        if df[col].isna().mean() < 0.90:
            cols.append(col)
    return cols


def split_train_test(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    trade_times = df.groupby('trade_id')['followed_at'].first().sort_values()
    n_train = int(len(trade_times) * TRAIN_FRAC)
    train_ids = set(trade_times.iloc[:n_train].index)
    return df[df['trade_id'].isin(train_ids)].copy(), df[~df['trade_id'].isin(train_ids)].copy()


# =============================================================================
# FAST FILTER EVALUATION
# =============================================================================

def precompute_filter_masks(
    df: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
) -> Dict[str, np.ndarray]:
    """Pre-compute boolean pass/fail masks for each filter. Returns dict of column -> bool array."""
    masks = {}
    for feat in ranked_features:
        col = feat['column']
        if col not in df.columns:
            continue
        vals = df[col].values
        masks[col] = (vals >= feat['from']) & (vals <= feat['to'])
    return masks


def score_combo_fast(
    combo_cols: Tuple[str, ...],
    masks: Dict[str, np.ndarray],
    is_good: np.ndarray,
    is_bad: np.ndarray,
    n_total: int,
) -> Optional[Dict[str, float]]:
    """
    Score a filter combination using pre-computed masks.
    Returns metrics dict or None if combo has too few passing trades.
    """
    combined = masks[combo_cols[0]]
    for col in combo_cols[1:]:
        combined = combined & masks[col]

    good_after = int((is_good & combined).sum())
    bad_after = int((is_bad & combined).sum())
    total_after = good_after + bad_after

    if total_after < 10:
        return None

    good_before = int(is_good.sum())
    bad_before = int(is_bad.sum())

    precision = good_after / total_after * 100 if total_after else 0
    good_kept_pct = good_after / good_before * 100 if good_before else 0
    bad_removed_pct = (bad_before - bad_after) / bad_before * 100 if bad_before else 0
    pass_rate = total_after / n_total * 100

    return {
        'precision': round(precision, 2),
        'good_kept_pct': round(good_kept_pct, 2),
        'bad_removed_pct': round(bad_removed_pct, 2),
        'pass_rate': round(pass_rate, 2),
        'good_after': good_after,
        'bad_after': bad_after,
    }


# =============================================================================
# FEATURE RANKING (Cohen's d + Youden's J for optimal range)
# =============================================================================

def rank_and_range_features(
    df: pd.DataFrame,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """
    Rank features by Cohen's d and compute optimal filter range via Youden's J.
    Returns list sorted by |Cohen's d| descending.
    """
    is_good = df['potential_gains'] >= THRESHOLD
    results = []

    for col in columns:
        good_vals = df.loc[is_good, col].dropna()
        bad_vals = df.loc[~is_good, col].dropna()

        if len(good_vals) < 20 or len(bad_vals) < 20:
            continue

        pooled_std = np.sqrt(
            ((len(good_vals) - 1) * good_vals.std() ** 2 +
             (len(bad_vals) - 1) * bad_vals.std() ** 2) /
            (len(good_vals) + len(bad_vals) - 2)
        )
        if pooled_std == 0:
            continue
        cohens_d = abs(good_vals.mean() - bad_vals.mean()) / pooled_std

        if cohens_d < 0.05:
            continue

        # Youden's J for optimal cutpoint
        all_vals = pd.concat([good_vals, bad_vals])
        test_thresholds = np.quantile(all_vals, np.linspace(0.05, 0.95, 40))

        best_j = -1
        best_cut = None
        best_dir = None

        for cut in test_thresholds:
            j_above = (good_vals >= cut).mean() + (bad_vals < cut).mean() - 1
            j_below = (good_vals <= cut).mean() + (bad_vals > cut).mean() - 1

            if j_above > best_j:
                best_j, best_cut, best_dir = j_above, cut, 'above'
            if j_below > best_j:
                best_j, best_cut, best_dir = j_below, cut, 'below'

        if best_cut is None or best_j <= 0:
            continue

        if best_dir == 'above':
            from_val = float(best_cut)
            to_val = float(good_vals.quantile(0.98))
        else:
            from_val = float(good_vals.quantile(0.02))
            to_val = float(best_cut)

        if from_val >= to_val:
            continue

        results.append({
            'column': col,
            'from': round(from_val, 8),
            'to': round(to_val, 8),
            'cohens_d': round(cohens_d, 4),
            'youdens_j': round(best_j, 4),
        })

    results.sort(key=lambda x: x['cohens_d'], reverse=True)
    return results


# =============================================================================
# SWEEP ENGINE
# =============================================================================

def sweep_interval(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    columns: List[str],
    interval_idx: int,
    baseline_test_precision: float,
) -> List[Dict[str, Any]]:
    """
    Run the full combinatorial sweep for one interval.
    Returns list of result dicts that beat the baseline on test.
    """
    t0 = time.time()

    # Step 1: Rank features on training data
    ranked = rank_and_range_features(df_train, columns)
    if len(ranked) < 2:
        logger.info(f"  Interval {interval_idx}: only {len(ranked)} rankable features, skipping")
        return []

    # Pre-compute masks on train and test
    is_good_train = (df_train['potential_gains'] >= THRESHOLD).values
    is_bad_train = ~is_good_train
    n_train = len(df_train)

    is_good_test = (df_test['potential_gains'] >= THRESHOLD).values
    is_bad_test = ~is_good_test
    n_test = len(df_test)

    masks_train = precompute_filter_masks(df_train, ranked)
    masks_test = precompute_filter_masks(df_test, ranked)

    results = []
    combos_tested = 0

    def test_combo(cols: Tuple[str, ...]):
        nonlocal combos_tested
        combos_tested += 1

        # Must have all columns in masks
        if not all(c in masks_train and c in masks_test for c in cols):
            return

        # Score on train
        train_m = score_combo_fast(cols, masks_train, is_good_train, is_bad_train, n_train)
        if train_m is None:
            return

        # Skip if train precision is worse than baseline (not worth testing on test)
        train_baseline = is_good_train.sum() / n_train * 100
        if train_m['precision'] <= train_baseline:
            return

        # Must keep at least 5% of good trades
        if train_m['good_kept_pct'] < 5:
            return

        # Score on test
        test_m = score_combo_fast(cols, masks_test, is_good_test, is_bad_test, n_test)
        if test_m is None:
            return

        # Only save if test precision beats baseline
        if test_m['precision'] <= baseline_test_precision:
            return

        # Build filter ranges string for CSV
        ranges = []
        for col in cols:
            feat = next((r for r in ranked if r['column'] == col), None)
            if feat:
                ranges.append(f"{col}:[{feat['from']},{feat['to']}]")

        results.append({
            'interval': interval_idx,
            'interval_label': f"{interval_idx // 2}:{(interval_idx % 2) * 30:02d}",
            'n_filters': len(cols),
            'filter_columns': '|'.join(cols),
            'filter_ranges': '|'.join(ranges),
            'train_precision': train_m['precision'],
            'test_precision': test_m['precision'],
            'test_good_kept': test_m['good_kept_pct'],
            'test_bad_removed': test_m['bad_removed_pct'],
            'test_pass_rate': test_m['pass_rate'],
            'test_good_after': test_m['good_after'],
            'test_bad_after': test_m['bad_after'],
            'overfit_delta': round(train_m['precision'] - test_m['precision'], 2),
        })

    # Get column names at various top-N levels
    top40 = [r['column'] for r in ranked[:40]]
    top30 = [r['column'] for r in ranked[:30]]
    top20 = [r['column'] for r in ranked[:20]]
    top15 = [r['column'] for r in ranked[:15]]

    # Step 2: Exhaustive pairs from top 40
    for combo in combinations(top40, 2):
        test_combo(combo)

    # Step 3: Exhaustive triples from top 30
    for combo in combinations(top30, 3):
        test_combo(combo)

    # Step 4: Exhaustive quads from top 20
    for combo in combinations(top20, 4):
        test_combo(combo)

    # Step 5: Exhaustive 5-combos from top 15
    for combo in combinations(top15, 5):
        test_combo(combo)

    # Step 6: Random 6-8 combos from top 30
    for _ in range(10_000):
        size = random.randint(6, 8)
        if len(top30) < size:
            continue
        combo = tuple(sorted(random.sample(top30, size)))
        test_combo(combo)

    elapsed = time.time() - t0
    beats_baseline = len(results)
    interval_label = f"{interval_idx // 2}:{(interval_idx % 2) * 30:02d}"
    logger.info(f"  Interval {interval_idx} ({interval_label}): "
                f"{combos_tested:,} combos tested, {beats_baseline} beat baseline, "
                f"top {len(ranked)} features, {elapsed:.1f}s")

    return results


# =============================================================================
# RESULTS OUTPUT
# =============================================================================

CSV_FIELDS = [
    'interval', 'interval_label', 'n_filters', 'filter_columns', 'filter_ranges',
    'train_precision', 'test_precision', 'test_good_kept', 'test_bad_removed',
    'test_pass_rate', 'test_good_after', 'test_bad_after', 'overfit_delta',
]


def save_results_csv(all_results: List[Dict[str, Any]], filepath: Path):
    """Save results to CSV sorted by test_precision descending."""
    all_results.sort(key=lambda x: x['test_precision'], reverse=True)

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_results)

    logger.info(f"Saved {len(all_results):,} results to {filepath}")


def print_top_results(all_results: List[Dict[str, Any]], top_n: int = 50):
    """Print the top N results."""
    sorted_results = sorted(all_results, key=lambda x: x['test_precision'], reverse=True)

    print()
    print("=" * 140)
    print(f"  TOP {top_n} FILTER COMBINATIONS (by test precision)")
    print("=" * 140)
    print(f"  {'Rank':>4}  {'Intv':>5}  {'#F':>3}  {'Train%':>7}  {'Test%':>7}  "
          f"{'BadRem%':>7}  {'GoodKept%':>9}  {'Overfit':>7}  Filters")
    print(f"  {'----':>4}  {'----':>5}  {'--':>3}  {'------':>7}  {'-----':>7}  "
          f"{'------':>7}  {'---------':>9}  {'-------':>7}  -------")

    for rank, r in enumerate(sorted_results[:top_n], 1):
        cols = r['filter_columns'].replace('|', ', ')
        if len(cols) > 70:
            cols = cols[:67] + '...'
        print(f"  {rank:4d}  {r['interval_label']:>5}  {r['n_filters']:>3d}  "
              f"{r['train_precision']:>6.1f}%  {r['test_precision']:>6.1f}%  "
              f"{r['test_bad_removed']:>6.1f}%  {r['test_good_kept']:>8.1f}%  "
              f"{r['overfit_delta']:>+6.1f}%  {cols}")

    print()


def print_interval_summary(all_results: List[Dict[str, Any]]):
    """Print best result per interval."""
    by_interval = {}
    for r in all_results:
        ivl = r['interval']
        if ivl not in by_interval or r['test_precision'] > by_interval[ivl]['test_precision']:
            by_interval[ivl] = r

    print()
    print("=" * 120)
    print("  BEST RESULT PER INTERVAL")
    print("=" * 120)
    print(f"  {'Intv':>5}  {'Label':>6}  {'#F':>3}  {'Train%':>7}  {'Test%':>7}  "
          f"{'BadRem%':>7}  {'GoodKept%':>9}  {'Total Hits':>10}  Filters")
    print(f"  {'----':>5}  {'-----':>6}  {'--':>3}  {'------':>7}  {'-----':>7}  "
          f"{'------':>7}  {'---------':>9}  {'----------':>10}  -------")

    for ivl in sorted(by_interval.keys()):
        r = by_interval[ivl]
        total_combos = sum(1 for x in all_results if x['interval'] == ivl)
        cols = r['filter_columns'].replace('|', ', ')
        if len(cols) > 50:
            cols = cols[:47] + '...'
        print(f"  {ivl:>5d}  {r['interval_label']:>6}  {r['n_filters']:>3d}  "
              f"{r['train_precision']:>6.1f}%  {r['test_precision']:>6.1f}%  "
              f"{r['test_bad_removed']:>6.1f}%  {r['test_good_kept']:>8.1f}%  "
              f"{total_combos:>10d}  {cols}")
    print()


# =============================================================================
# MAIN
# =============================================================================

def run_sweep(test_mode: bool = False):
    """Run the full overnight sweep."""
    sweep_start = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print()
    print("=" * 80)
    print("  OVERNIGHT BRUTE-FORCE FILTER SWEEP")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Threshold: {THRESHOLD}%")
    print(f"  Hours: {HOURS}")
    print(f"  Train/Test split: {TRAIN_FRAC*100:.0f}/{(1-TRAIN_FRAC)*100:.0f}")
    if test_mode:
        print("  MODE: TEST (1 interval only)")
    print("=" * 80)
    print()
    sys.stdout.flush()

    # Load data
    con, summary = load_all_data_into_duckdb()
    if con is None:
        logger.error("No data loaded, aborting")
        return

    logger.info(f"Data: {summary['n_trades']:,} trades, {summary['n_filter_rows']:,} filter rows, "
                f"{summary['n_filters']} columns")

    # Determine intervals to test
    intervals_df = con.execute("""
        SELECT interval_idx, COUNT(DISTINCT trade_id) AS n_trades
        FROM raw_filters GROUP BY interval_idx ORDER BY interval_idx
    """).fetchdf()

    if test_mode:
        # Just test interval 0
        intervals_to_test = [0]
    else:
        # All intervals with at least 50 trades
        intervals_to_test = sorted(
            intervals_df[intervals_df['n_trades'] >= 50]['interval_idx'].tolist()
        )

    logger.info(f"Testing {len(intervals_to_test)} intervals: {intervals_to_test}")
    sys.stdout.flush()

    all_results = []
    total_combos = 0

    for i, ivl in enumerate(intervals_to_test, 1):
        logger.info(f"\n[{i}/{len(intervals_to_test)}] Processing interval {ivl} "
                    f"({ivl // 2}:{(ivl % 2) * 30:02d})...")
        sys.stdout.flush()

        # Pivot this interval
        df = load_interval_pivoted(con, ivl)
        if len(df) == 0:
            continue

        columns = get_filterable_columns(df)
        if len(columns) < 5:
            logger.info(f"  Skipping: only {len(columns)} filterable columns")
            continue

        # Train/test split
        df_train, df_test = split_train_test(df)
        if len(df_train) < 50 or len(df_test) < 20:
            logger.info(f"  Skipping: too few trades (train={len(df_train)}, test={len(df_test)})")
            continue

        baseline_test_precision = (df_test['potential_gains'] >= THRESHOLD).mean() * 100

        # Run sweep
        results = sweep_interval(df_train, df_test, columns, ivl, baseline_test_precision)
        all_results.extend(results)
        total_combos += len(results)

        # Save intermediate results every 5 intervals
        if i % 5 == 0 or i == len(intervals_to_test):
            csv_path = RESULTS_DIR / f"overnight_sweep_{timestamp}.csv"
            save_results_csv(all_results, csv_path)
            elapsed = time.time() - sweep_start
            logger.info(f"  Progress: {i}/{len(intervals_to_test)} intervals, "
                        f"{len(all_results):,} results, {elapsed/60:.1f} min elapsed")

        sys.stdout.flush()

    # Final save
    csv_path = RESULTS_DIR / f"overnight_sweep_{timestamp}.csv"
    save_results_csv(all_results, csv_path)

    # Also save as "latest" for easy access
    latest_path = RESULTS_DIR / "overnight_sweep_latest.csv"
    save_results_csv(all_results, latest_path)

    # Print summary
    total_elapsed = time.time() - sweep_start

    print()
    print("=" * 80)
    print("  SWEEP COMPLETE")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Total runtime: {total_elapsed/60:.1f} minutes ({total_elapsed/3600:.1f} hours)")
    print(f"  Intervals tested: {len(intervals_to_test)}")
    print(f"  Combos that beat baseline: {len(all_results):,}")
    print(f"  Results saved to: {csv_path}")
    print("=" * 80)

    if all_results:
        print_interval_summary(all_results)
        print_top_results(all_results, top_n=50)

        # Print the #1 result in detail
        best = sorted(all_results, key=lambda x: x['test_precision'], reverse=True)[0]
        print("=" * 80)
        print("  BEST OVERALL RESULT")
        print("=" * 80)
        print(f"  Interval: {best['interval']} ({best['interval_label']})")
        print(f"  Filters: {best['n_filters']}")
        print(f"  Train precision: {best['train_precision']:.1f}%")
        print(f"  Test precision:  {best['test_precision']:.1f}%")
        print(f"  Bad removed:     {best['test_bad_removed']:.1f}%")
        print(f"  Good kept:       {best['test_good_kept']:.1f}%")
        print(f"  Pass rate:       {best['test_pass_rate']:.1f}%")
        print(f"  Overfit:         {best['overfit_delta']:+.1f}%")
        print(f"  Filter details:")
        for part in best['filter_ranges'].split('|'):
            print(f"    {part}")
        print("=" * 80)
    else:
        print("\n  No combinations beat the baseline on the test set.")
        print("  This suggests the signal is very weak at 0.3% threshold with 48h of data.")

    print()
    sys.stdout.flush()

    con.close()


def main():
    parser = argparse.ArgumentParser(description="Overnight Brute-Force Filter Sweep")
    parser.add_argument("--test", action="store_true", help="Quick test on 1 interval only")
    args = parser.parse_args()

    run_sweep(test_mode=args.test)


if __name__ == "__main__":
    main()

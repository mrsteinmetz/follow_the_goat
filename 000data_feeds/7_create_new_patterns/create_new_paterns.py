#!/usr/bin/env python3
"""
Auto Pattern Filter Generator
=============================
Analyzes trade data to find optimal filter combinations that maximize
precision (% of passing trades that are good) using:

  - Cohen's d for feature ranking (statistical separation)
  - Youden's J for optimal cutpoints (not arbitrary percentiles)
  - Exhaustive combinatorial search (pairs, triples, quads from top features)
  - Time-based train/test validation (70/30 chronological split)

This script:
1. Loads trade data from DuckDB cache (synced from PostgreSQL)
2. Splits data chronologically into train (70%) / test (30%)
3. Ranks features by Cohen's d statistical separation
4. Finds optimal ranges via Youden's J statistic
5. Exhaustively tests filter combinations (pairs, triples, quads)
6. Validates on test set to prevent overfitting
7. Syncs best filters to pattern_config_filters
8. Updates plays with pattern_update_by_ai=1

Usage:
    # Run standalone
    python create_new_paterns.py
    
    # Integrated via scheduler (runs every 10 minutes)
"""

import json
import logging
import sys
import time
from itertools import combinations as iter_combinations
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import uuid

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.trading_engine import get_engine
from core.database import get_postgres
from core.filter_cache import sync_cache_incremental, get_cached_trades, get_cache_stats

# Setup logging
logger = logging.getLogger("create_new_patterns")


# =============================================================================
# Constants
# =============================================================================

# Columns to never use as filters
SKIP_COLUMNS = frozenset([
    'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good',
])

# Absolute value columns that don't generalize across time periods.
# Prices/volumes change daily, so filters like "sol_price > 150" break tomorrow.
ABSOLUTE_PRICE_COLUMNS = frozenset([
    # SOL prices
    'pm_open_price', 'pm_close_price', 'pm_high_price', 'pm_low_price', 'pm_avg_price',
    # BTC prices
    'btc_open_price', 'btc_close_price', 'btc_high_price', 'btc_low_price',
    # ETH prices
    'eth_open_price', 'eth_close_price', 'eth_high_price', 'eth_low_price',
    # Second-prices (absolute)
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
    # Pattern absolute price levels
    'pat_asc_tri_resistance_level', 'pat_asc_tri_support_level',
    'pat_inv_hs_neckline', 'pat_cup_handle_rim',
])

# Train/test split ratio
TRAIN_FRAC = 0.70


def _read_from_postgres(query: str, params: list = None) -> list:
    """Execute a read query on PostgreSQL.
    
    Returns list of dictionaries.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                results = cursor.fetchall()
                return results
    except Exception as e:
        logger.warning(f"PostgreSQL query failed: {e}")
        return []


def _get_filter_columns(hours: int, ratio_only: bool = False) -> List[str]:
    """Get distinct filter names from trade_filter_values for recent trades."""
    ratio_clause = "AND tfv.is_ratio = 1" if ratio_only else ""
    query = f"""
        SELECT DISTINCT tfv.filter_name
        FROM trade_filter_values tfv
        INNER JOIN follow_the_goat_buyins b ON b.id = tfv.buyin_id
        WHERE b.potential_gains IS NOT NULL
          AND b.followed_at >= NOW() - INTERVAL '%s hours'
          {ratio_clause}
        ORDER BY tfv.filter_name
    """
    results = _read_from_postgres(query, [hours])
    return [r['filter_name'] for r in results] if results else []


# =============================================================================
# Configuration
# =============================================================================

def ensure_settings_table():
    """Ensure auto_filter_settings table exists in PostgreSQL."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS auto_filter_settings (
                        id SERIAL PRIMARY KEY,
                        setting_key VARCHAR(100) UNIQUE NOT NULL,
                        setting_value TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_auto_filter_settings_key 
                    ON auto_filter_settings(setting_key)
                """)
                conn.commit()
    except Exception as e:
        logger.warning(f"Could not ensure settings table exists: {e}")


def load_config() -> Dict[str, Any]:
    """
    Load configuration from PostgreSQL table.
    
    NEVER CACHED - Always reads fresh from database.
    Falls back to defaults if table doesn't exist or setting not found.
    """
    ensure_settings_table()
    
    # Default config values
    defaults = {
        "good_trade_threshold": 0.3,
        "analysis_hours": 24,
        "min_filters_in_combo": 2,
        "max_filters_in_combo": 6,
        "min_good_trades_kept_pct": 50,
        "min_bad_trades_removed_pct": 10,
        "combo_min_good_kept_pct": 25,
        "combo_min_improvement": 1.0,
        "auto_project_name": "AutoFilters",
        "percentile_low": 10,
        "percentile_high": 90,
        "skip_columns": [],
        "is_ratio": False,
        "section_prefixes": {
            "pm_": "price_movements",
            "tx_": "transactions",
            "ob_": "order_book",
            "wh_": "whale_activity",
            "mp_": "micro_patterns",
            "sp_": "second_prices",
            "pat_": "patterns",
            "btc_": "btc_correlation",
            "eth_": "eth_correlation",
            "xa_": "cross_asset",
            "ts_": "thirty_second",
            "mm_": "micro_move",
            "pre_entry_": "pre_entry"
        }
    }
    
    config = defaults.copy()
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT setting_key, setting_value FROM auto_filter_settings")
                rows = cursor.fetchall()
                
                for row in rows:
                    key = row['setting_key']
                    value = row['setting_value']
                    
                    # Parse JSON fields
                    if key in ['skip_columns', 'section_prefixes']:
                        try:
                            config[key] = json.loads(value) if value else defaults[key]
                        except (json.JSONDecodeError, TypeError):
                            config[key] = defaults[key]
                    # Parse numeric fields
                    elif key in ['good_trade_threshold', 'min_good_trades_kept_pct', 
                                'min_bad_trades_removed_pct', 'combo_min_good_kept_pct',
                                'combo_min_improvement', 'percentile_low', 'percentile_high']:
                        try:
                            config[key] = float(value) if value else defaults[key]
                        except (ValueError, TypeError):
                            config[key] = defaults[key]
                    # Parse boolean fields
                    elif key in ['is_ratio']:
                        if isinstance(value, bool):
                            config[key] = value
                        else:
                            config[key] = str(value).strip().lower() in ['1', 'true', 'yes', 'on']
                    # Parse integer fields
                    elif key in ['analysis_hours', 'min_filters_in_combo', 'max_filters_in_combo']:
                        try:
                            config[key] = int(value) if value else defaults[key]
                        except (ValueError, TypeError):
                            config[key] = defaults[key]
                    # String fields
                    else:
                        config[key] = value if value else defaults.get(key, '')
                        
    except Exception as e:
        logger.warning(f"Could not load config from database, using defaults: {e}")
        return defaults
    
    return config


# =============================================================================
# Data Loading
# =============================================================================

def load_trade_data(engine, hours: int = 24) -> pd.DataFrame:
    """
    Load trade data from DuckDB cache (with auto-sync).
    
    OPTIMIZED: Uses DuckDB cache for 10-50x faster queries vs PostgreSQL.
    Cache is automatically synced incrementally on each run.
    """
    start_time = time.time()
    
    config = load_config()
    ratio_only = bool(config.get('is_ratio', False))
    
    # Ensure cache exists and is up-to-date
    logger.info("  Checking DuckDB cache...")
    try:
        cache_age_seconds = sync_cache_incremental()
        
        if cache_age_seconds < 60:
            logger.info(f"  Cache is fresh ({cache_age_seconds:.0f}s old)")
        elif cache_age_seconds < 3600:
            logger.info(f"  Cache synced ({cache_age_seconds/60:.1f} minutes old)")
        else:
            logger.info(f"  Cache synced ({cache_age_seconds/3600:.1f} hours old)")
        
    except Exception as e:
        logger.error(f"  Cache sync failed: {e}", exc_info=True)
        logger.warning("  Falling back to PostgreSQL direct query...")
        return load_trade_data_fallback(hours, ratio_only)
    
    # Query DuckDB cache
    logger.info("  Loading data from DuckDB cache...")
    query_start = time.time()
    
    try:
        df = get_cached_trades(hours=hours, ratio_only=ratio_only)
        
        if len(df) == 0:
            logger.warning("No trade data found in cache")
            return pd.DataFrame()
        
        logger.info(f"  Query returned {len(df)} rows in {time.time() - query_start:.1f}s")
        
        unique_trades = df['trade_id'].nunique()
        total_time = time.time() - start_time
        logger.info(f"  Loaded {len(df)} rows ({unique_trades} unique trades) in {total_time:.1f}s total")
        
        return df
        
    except Exception as e:
        logger.error(f"  DuckDB query failed: {e}", exc_info=True)
        logger.warning("  Falling back to PostgreSQL direct query...")
        return load_trade_data_fallback(hours, ratio_only)


def load_trade_data_fallback(hours: int = 24, ratio_only: bool = False) -> pd.DataFrame:
    """
    Fallback to load trade data directly from PostgreSQL (if DuckDB cache fails).
    """
    start_time = time.time()
    
    # Step 1: Get distinct filter columns (fast query)
    logger.info("  Getting distinct filter columns from PostgreSQL...")
    filter_columns = _get_filter_columns(hours, ratio_only=ratio_only)
    
    if not filter_columns:
        logger.warning("No filter columns found")
        return pd.DataFrame()
    
    logger.info(f"  Found {len(filter_columns)} filter columns in {time.time() - start_time:.1f}s")
    
    # Step 2: Build pivoted query using conditional aggregation
    pivot_columns = []
    for col in filter_columns:
        safe_col = col.replace("'", "''")
        pivot_columns.append(
            f"MAX(tfv.filter_value) FILTER (WHERE tfv.filter_name = '{safe_col}') AS \"{col}\""
        )
    
    pivot_sql = ",\n                ".join(pivot_columns)
    
    query = f"""
        SELECT 
            b.id as trade_id,
            b.play_id,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            tfv.minute,
            COALESCE(tfv.sub_minute, 0) as sub_minute,
            (tfv.minute * 2 + COALESCE(tfv.sub_minute, 0)) as interval_idx,
            {pivot_sql}
        FROM follow_the_goat_buyins b
        INNER JOIN trade_filter_values tfv ON tfv.buyin_id = b.id
        WHERE b.potential_gains IS NOT NULL
          AND b.followed_at >= NOW() - INTERVAL '{hours} hours'
        GROUP BY b.id, b.play_id, b.followed_at, b.potential_gains, b.our_status, tfv.minute, tfv.sub_minute
        ORDER BY b.id, tfv.minute, tfv.sub_minute
    """
    
    logger.info("  Executing pivoted query in PostgreSQL...")
    query_start = time.time()
    results = _read_from_postgres(query, [])
    
    if not results:
        logger.warning("No trade data found with resolved outcomes")
        return pd.DataFrame()
    
    logger.info(f"  Query returned {len(results)} rows in {time.time() - query_start:.1f}s")
    
    df = pd.DataFrame(results)
    
    for col in filter_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    unique_trades = df['trade_id'].nunique()
    total_time = time.time() - start_time
    logger.info(f"  Loaded {len(df)} rows ({unique_trades} unique trades) in {total_time:.1f}s total")
    
    return df


def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    """
    Get list of numeric columns suitable for filtering.
    
    Excludes:
    - ID / timestamp / string columns (SKIP_COLUMNS)
    - Absolute price/volume columns (ABSOLUTE_PRICE_COLUMNS) that don't generalize
    - Columns with >90% null values
    - Columns in user's skip_columns config
    """
    config = load_config()
    skip_columns = set(config.get('skip_columns', []))
    skip_columns.update(SKIP_COLUMNS)
    
    filterable = []
    for col in df.columns:
        if col in skip_columns:
            continue
        if col in ABSOLUTE_PRICE_COLUMNS:
            continue
        
        if df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            null_pct = df[col].isna().sum() / len(df) * 100
            if null_pct < 90:
                filterable.append(col)
    
    return filterable


def get_section_from_column(column_name: str) -> str:
    """Determine section from column prefix."""
    config = load_config()
    section_prefixes = config.get('section_prefixes', {})
    for prefix, section in section_prefixes.items():
        if column_name.startswith(prefix):
            return section
    return "unknown"


def get_field_name_from_column(column_name: str) -> str:
    """Extract field name by removing prefix."""
    config = load_config()
    section_prefixes = config.get('section_prefixes', {})
    for prefix in section_prefixes.keys():
        if column_name.startswith(prefix):
            return column_name[len(prefix):]
    return column_name


# =============================================================================
# Filter Effectiveness Testing
# =============================================================================

def test_filter_effectiveness(
    df: pd.DataFrame,
    column_name: str,
    from_val: float,
    to_val: float,
    threshold: float
) -> Optional[Dict[str, Any]]:
    """
    Test how effective a filter would be at removing bad trades.
    
    Returns dictionary with effectiveness metrics or None if column not found.
    """
    if column_name not in df.columns:
        return None
    
    values = df[column_name]
    potential_gains = df['potential_gains']
    
    is_good = potential_gains >= threshold
    is_bad = potential_gains < threshold
    
    is_negative = potential_gains < 0
    is_0_to_01 = (potential_gains >= 0) & (potential_gains < 0.1)
    is_01_to_02 = (potential_gains >= 0.1) & (potential_gains < 0.2)
    is_02_to_03 = (potential_gains >= 0.2) & (potential_gains < threshold)
    
    good_before = int(is_good.sum())
    bad_before = int(is_bad.sum())
    total = good_before + bad_before
    
    if total == 0:
        return None
    
    passes_filter = (values >= from_val) & (values <= to_val)
    
    good_after = int((is_good & passes_filter).sum())
    bad_after = int((is_bad & passes_filter).sum())
    
    bad_negative_after = int((is_negative & passes_filter).sum())
    bad_0_to_01_after = int((is_0_to_01 & passes_filter).sum())
    bad_01_to_02_after = int((is_01_to_02 & passes_filter).sum())
    bad_02_to_03_after = int((is_02_to_03 & passes_filter).sum())
    
    good_kept_pct = (good_after / good_before * 100) if good_before > 0 else 0
    bad_removed = bad_before - bad_after
    bad_removed_pct = (bad_removed / bad_before * 100) if bad_before > 0 else 0
    
    return {
        'total_trades': total,
        'good_trades_before': good_before,
        'bad_trades_before': bad_before,
        'good_trades_after': good_after,
        'bad_trades_after': bad_after,
        'good_trades_kept_pct': round(good_kept_pct, 2),
        'bad_trades_removed_pct': round(bad_removed_pct, 2),
        'bad_negative_count': bad_negative_after,
        'bad_0_to_01_count': bad_0_to_01_after,
        'bad_01_to_02_count': bad_01_to_02_after,
        'bad_02_to_03_count': bad_02_to_03_after,
    }


# =============================================================================
# NEW Algorithm Core: Statistical Ranking + Exhaustive Combo Search
# =============================================================================

def split_train_test(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Time-based train/test split.
    Sorts trades by followed_at, earliest 70% = train, latest 30% = test.
    """
    trade_times = df.groupby('trade_id')['followed_at'].first().sort_values()
    n_train = int(len(trade_times) * TRAIN_FRAC)
    train_ids = set(trade_times.iloc[:n_train].index)
    return df[df['trade_id'].isin(train_ids)].copy(), df[~df['trade_id'].isin(train_ids)].copy()


def rank_and_range_features(
    df: pd.DataFrame,
    columns: List[str],
    threshold: float,
) -> List[Dict[str, Any]]:
    """
    Rank features by Cohen's d (statistical separation) and compute optimal
    filter ranges via Youden's J statistic.
    
    This replaces the old percentile-based approach. Cohen's d measures how
    well a feature separates good from bad trades. Youden's J finds the
    statistically optimal cutpoint rather than arbitrary P10/P90.
    
    Returns list sorted by |Cohen's d| descending.
    """
    is_good = df['potential_gains'] >= threshold
    results = []

    for col in columns:
        good_vals = df.loc[is_good, col].dropna()
        bad_vals = df.loc[~is_good, col].dropna()

        if len(good_vals) < 20 or len(bad_vals) < 20:
            continue

        # Cohen's d: effect size measuring separation between distributions
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


def precompute_filter_masks(
    df: pd.DataFrame,
    ranked_features: List[Dict[str, Any]],
) -> Dict[str, np.ndarray]:
    """Pre-compute boolean pass/fail masks for each filter."""
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

    if total_after < 5:
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


def find_best_combo_for_minute(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    columns: List[str],
    minute: int,
    threshold: float,
) -> Optional[Dict[str, Any]]:
    """
    Find the best filter combination for a specific minute using:
    1. Cohen's d ranking on train data
    2. Exhaustive combo search (pairs, triples, quads)
    3. Validation on test data (must beat baseline precision)
    
    Returns the best combo dict or None.
    """
    # Rank features on training data
    ranked = rank_and_range_features(df_train, columns, threshold)
    if len(ranked) < 2:
        return None

    # Pre-compute masks
    is_good_train = (df_train['potential_gains'] >= threshold).values
    is_bad_train = ~is_good_train
    n_train = len(df_train)

    is_good_test = (df_test['potential_gains'] >= threshold).values
    is_bad_test = ~is_good_test
    n_test = len(df_test)

    baseline_train = is_good_train.sum() / n_train * 100 if n_train > 0 else 0
    baseline_test = is_good_test.sum() / n_test * 100 if n_test > 0 else 0

    masks_train = precompute_filter_masks(df_train, ranked)
    masks_test = precompute_filter_masks(df_test, ranked)

    candidates = []  # (test_precision, combo_dict)

    def test_combo(cols: Tuple[str, ...]):
        if not all(c in masks_train and c in masks_test for c in cols):
            return

        # Score on train -- must beat baseline
        train_m = score_combo_fast(cols, masks_train, is_good_train, is_bad_train, n_train)
        if train_m is None or train_m['precision'] <= baseline_train:
            return
        # Must keep at least 3% of good trades on train
        if train_m['good_kept_pct'] < 3:
            return

        # Score on test -- must beat baseline
        test_m = score_combo_fast(cols, masks_test, is_good_test, is_bad_test, n_test)
        if test_m is None or test_m['precision'] <= baseline_test:
            return

        overfit = round(train_m['precision'] - test_m['precision'], 2)

        candidates.append({
            'minute': minute,
            'columns': list(cols),
            'n_filters': len(cols),
            'train_precision': train_m['precision'],
            'test_precision': test_m['precision'],
            'test_good_kept': test_m['good_kept_pct'],
            'test_bad_removed': test_m['bad_removed_pct'],
            'test_pass_rate': test_m['pass_rate'],
            'test_good_after': test_m['good_after'],
            'test_bad_after': test_m['bad_after'],
            'overfit_delta': overfit,
            'ranked_features': ranked,  # Keep for range lookup
        })

    # Get column names at various top-N levels
    top30 = [r['column'] for r in ranked[:30]]
    top20 = [r['column'] for r in ranked[:20]]
    top15 = [r['column'] for r in ranked[:15]]

    # Exhaustive pairs from top 30
    for combo in iter_combinations(top30, 2):
        test_combo(combo)

    # Exhaustive triples from top 20
    for combo in iter_combinations(top20, 3):
        test_combo(combo)

    # Exhaustive quads from top 15
    for combo in iter_combinations(top15, 4):
        test_combo(combo)

    # Greedy 5-6 expansion from best quads
    quad_candidates = [c for c in candidates if c['n_filters'] == 4]
    if quad_candidates:
        best_quads = sorted(quad_candidates, key=lambda x: x['test_precision'], reverse=True)[:5]
        for base in best_quads:
            base_cols = set(base['columns'])
            for extra_col in top20:
                if extra_col in base_cols:
                    continue
                test_combo(tuple(sorted(base_cols | {extra_col})))
                # Try adding 2 more
                for extra_col2 in top20:
                    if extra_col2 in base_cols or extra_col2 == extra_col:
                        continue
                    test_combo(tuple(sorted(base_cols | {extra_col, extra_col2})))

    if not candidates:
        return None

    # Select best by test precision, then by lower overfit
    candidates.sort(key=lambda x: (x['test_precision'], -abs(x['overfit_delta'])), reverse=True)
    return candidates[0]


# =============================================================================
# Saving Results
# =============================================================================

def save_filter_catalog(engine, columns: List[str]) -> Dict[str, int]:
    """
    Save/update filter fields catalog directly to PostgreSQL.
    
    Returns dict mapping column_name to id.
    """
    column_to_id = {}
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM filter_fields_catalog")
                
                field_name_to_info = {}
                for col in columns:
                    section = get_section_from_column(col)
                    field_name = get_field_name_from_column(col)
                    
                    if field_name not in field_name_to_info:
                        field_name_to_info[field_name] = {
                            'column_name': col,
                            'section': section
                        }
                
                next_id = 1
                for field_name, info in field_name_to_info.items():
                    cursor.execute("""
                        INSERT INTO filter_fields_catalog 
                        (id, section, field_name, field_type, description)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (field_name) DO UPDATE SET
                            section = EXCLUDED.section,
                            field_type = EXCLUDED.field_type,
                            description = EXCLUDED.description
                    """, [
                        next_id,
                        info['section'],
                        field_name,
                        'numeric',
                        f"Filter field: {info['column_name']}"
                    ])
                    
                    for col in columns:
                        if get_field_name_from_column(col) == field_name:
                            column_to_id[col] = next_id
                    
                    next_id += 1
            conn.commit()
        logger.info(f"Saved {len(field_name_to_info)} unique fields (from {len(columns)} columns) to filter_fields_catalog")
    except Exception as e:
        logger.error(f"Failed to save filter catalog to PostgreSQL: {e}", exc_info=True)
    
    return column_to_id


def save_suggestions(engine, suggestions: List[Dict[str, Any]], column_to_id: Dict[str, int], hours: int):
    """Save filter suggestions directly to PostgreSQL."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM filter_reference_suggestions")
                
                for i, s in enumerate(suggestions, 1):
                    cursor.execute("""
                        INSERT INTO filter_reference_suggestions 
                        (id, filter_field_id, column_name, from_value, to_value,
                         total_trades, good_trades_before, bad_trades_before,
                         good_trades_after, bad_trades_after,
                         good_trades_kept_pct, bad_trades_removed_pct,
                         bad_negative_count, bad_0_to_01_count, bad_01_to_02_count, bad_02_to_03_count,
                         analysis_hours, minute_analyzed, section)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        i,
                        column_to_id.get(s['column_name'], 0),
                        s['column_name'],
                        s['from_value'],
                        s['to_value'],
                        s.get('total_trades', 0),
                        s.get('good_trades_before', 0),
                        s.get('bad_trades_before', 0),
                        s.get('good_trades_after', 0),
                        s.get('bad_trades_after', 0),
                        s.get('good_trades_kept_pct', 0),
                        s.get('bad_trades_removed_pct', 0),
                        s.get('bad_negative_count', 0),
                        s.get('bad_0_to_01_count', 0),
                        s.get('bad_01_to_02_count', 0),
                        s.get('bad_02_to_03_count', 0),
                        hours,
                        s.get('minute_analyzed', 0),
                        s.get('section', get_section_from_column(s['column_name'])),
                    ])
            conn.commit()
        logger.info(f"Saved {len(suggestions)} filter suggestions to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to save suggestions to PostgreSQL: {e}", exc_info=True)


def save_combinations(engine, combinations: List[Dict[str, Any]], hours: int):
    """Save filter combinations directly to PostgreSQL."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM filter_combinations")
            conn.commit()
    except Exception as e:
        logger.warning(f"Could not clear existing combinations: {e}")
    
    if not combinations:
        logger.info("No combinations to save")
        return
    
    best_single_removed = combinations[0]['bad_trades_removed_pct'] if combinations else 0
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for i, combo in enumerate(combinations, 1):
                    minute = combo.get('minute_analyzed', 0)
                    name = f"[M{minute}] {len(combo['filter_columns'])}-filter combo: " + " + ".join(
                        col.replace('_', ' ').title()[:20] for col in combo['filter_columns'][:3]
                    )
                    if len(combo['filter_columns']) > 3:
                        name += f" (+{len(combo['filter_columns']) - 3} more)"
                    
                    improvement = combo['bad_trades_removed_pct'] - best_single_removed
                    
                    cursor.execute("""
                        INSERT INTO filter_combinations 
                        (id, combination_name, filter_count, filter_ids, filter_columns,
                         total_trades, good_trades_before, bad_trades_before,
                         good_trades_after, bad_trades_after,
                         good_trades_kept_pct, bad_trades_removed_pct,
                         best_single_bad_removed_pct, improvement_over_single,
                         bad_negative_count, bad_0_to_01_count, bad_01_to_02_count, bad_02_to_03_count,
                         minute_analyzed, analysis_hours)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        i,
                        name,
                        len(combo['filter_columns']),
                        json.dumps(list(range(1, len(combo['filter_columns']) + 1))),
                        json.dumps(combo['filter_columns']),
                        combo.get('total_trades', 0),
                        combo.get('good_trades_before', 0),
                        combo.get('bad_trades_before', 0),
                        combo.get('good_trades_after', 0),
                        combo.get('bad_trades_after', 0),
                        combo.get('good_trades_kept_pct', 0),
                        combo.get('bad_trades_removed_pct', 0),
                        best_single_removed,
                        round(improvement, 2),
                        combo.get('bad_negative_count', 0),
                        combo.get('bad_0_to_01_count', 0),
                        combo.get('bad_01_to_02_count', 0),
                        combo.get('bad_02_to_03_count', 0),
                        combo.get('minute_analyzed', 0),
                        hours,
                    ])
            conn.commit()
        logger.info(f"Saved {len(combinations)} filter combinations to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to save combinations to PostgreSQL: {e}", exc_info=True)


# =============================================================================
# Sync to Pattern Config
# =============================================================================

def get_or_create_auto_project(engine) -> int:
    """Get or create the AutoFilters project."""
    config = load_config()
    project_name = config.get('auto_project_name', 'AutoFilters')
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM pattern_config_projects WHERE name = %s",
                    [project_name]
                )
                result = cursor.fetchone()
                if result:
                    return result['id']
    except Exception as e:
        logger.warning(f"Could not query pattern_config_projects: {e}")
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM pattern_config_projects")
                max_id_result = cursor.fetchone()
                new_id = max_id_result['coalesce'] if max_id_result else 1
                
                cursor.execute("""
                    INSERT INTO pattern_config_projects (id, name, description)
                    VALUES (%s, %s, %s)
                """, [new_id, project_name, 'Auto-generated filters updated every 15 minutes based on trade analysis'])
            conn.commit()
        
        logger.info(f"Created new AutoFilters project: id={new_id}")
        return new_id
    except Exception as e:
        logger.warning(f"Could not create project: {e}")
        return 1


def sync_best_filters_to_project(engine, combo: Dict[str, Any], project_id: int) -> Dict[str, Any]:
    """
    Sync the best filter combination to the project's pattern_config_filters.
    
    Args:
        engine: TradingDataEngine instance
        combo: Best combo dict with 'filters' list and metrics
        project_id: Pattern config project ID
    """
    # Clear existing filters
    logger.info("Clearing existing filters for project...")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
            conn.commit()
        logger.info("Existing filters cleared")
    except Exception as e:
        logger.warning(f"Could not clear existing filters: {e}")
    
    if not combo or 'filters' not in combo or not combo['filters']:
        logger.warning("No filters to sync")
        return {"success": False, "error": "No filters to sync"}
    
    logger.info(f"Syncing {len(combo['filters'])} filters "
                f"(test precision: {combo.get('test_precision', 0):.1f}%, "
                f"bad removed: {combo.get('bad_trades_removed_pct', 0):.1f}%, "
                f"good kept: {combo.get('good_trades_kept_pct', 0):.1f}%)")
    
    filters_inserted = 0
    for i, f in enumerate(combo['filters'], 1):
        try:
            # FIX: Convert interval index to actual minute for pattern_validator.py
            # pattern_validator matches on minute_span_from (0-14), not interval (0-29)
            minute_val = f.get('minute_analyzed', 0)
            
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO pattern_config_filters
                        (id, project_id, name, section, minute, field_name, field_column,
                         from_value, to_value, include_null, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        i + (project_id * 1000),
                        project_id,
                        f"Auto: {f['column_name']}",
                        f.get('section', get_section_from_column(f['column_name'])),
                        minute_val,
                        f.get('field_name', get_field_name_from_column(f['column_name'])),
                        f['column_name'],
                        f['from_value'],
                        f['to_value'],
                        0,
                        1
                    ])
                conn.commit()
            filters_inserted += 1
            logger.info(f"  Added filter: {f['column_name']} [{f['from_value']:.6f} - {f['to_value']:.6f}] (minute={minute_val})")
        except Exception as e:
            logger.error(f"Failed to insert filter {f['column_name']}: {e}")
    
    return {
        "success": True,
        "filters_synced": filters_inserted,
        "test_precision": combo.get('test_precision', 0),
        "bad_removed_pct": combo.get('bad_trades_removed_pct', 0),
        "good_kept_pct": combo.get('good_trades_kept_pct', 0),
    }


def update_ai_plays(engine, project_id: int, run_id: str, pattern_count: int = 0) -> int:
    """Update all plays with pattern_update_by_ai=1 to use the AutoFilters project and log updates."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, name FROM follow_the_goat_plays WHERE pattern_update_by_ai = 1"
                )
                plays = cursor.fetchall()
        
        if not plays:
            logger.info("No AI-enabled plays to update")
            return 0
        
        project_ids_json = json.dumps([project_id])
        config = load_config()
        project_name = config.get('auto_project_name', 'AutoFilters')
        
        updated_count = 0
        for play in plays:
            play_id = play['id']
            play_name = play['name']
            try:
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            UPDATE follow_the_goat_plays 
                            SET project_ids = %s 
                            WHERE id = %s AND pattern_update_by_ai = 1
                        """, [project_ids_json, play_id])
                        
                        cursor.execute("""
                            INSERT INTO ai_play_updates 
                            (play_id, play_name, project_id, project_name, pattern_count, filters_applied, run_id, status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, [play_id, play_name, project_id, project_name, pattern_count, pattern_count, run_id, 'success'])
                    conn.commit()
                
                updated_count += 1
                logger.info(f"  Updated play #{play_id} ({play_name}) with project_ids=[{project_id}]")
            except Exception as e:
                logger.error(f"Failed to update play {play_id}: {e}")
                try:
                    with get_postgres() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                INSERT INTO ai_play_updates 
                                (play_id, play_name, project_id, project_name, pattern_count, filters_applied, run_id, status)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, [play_id, play_name, project_id, project_name, 0, 0, run_id, 'failed'])
                        conn.commit()
                except:
                    pass
        
        logger.info(f"Updated {updated_count} AI-enabled plays with AutoFilters project")
        return updated_count
        
    except Exception as e:
        logger.warning(f"Could not update plays: {e}")
        return 0


# =============================================================================
# Main Entry Point
# =============================================================================

def run() -> Dict[str, Any]:
    """
    Main entry point - called by scheduler every 10 minutes.
    
    Uses the sweep-proven algorithm:
    1. Load data, split train/test chronologically
    2. For each minute, rank features (Cohen's d) and find optimal ranges (Youden's J)
    3. Exhaustive combo search (pairs, triples, quads) on train
    4. Validate on test set -- only sync if test precision beats baseline
    5. Sync best combo to pattern_config_filters
    """
    run_id = str(uuid.uuid4())[:8]
    
    try:
        logger.info("=" * 80)
        logger.info(f"AUTO FILTER PATTERN GENERATOR [SWEEP-PROVEN ALGORITHM] - Run ID: {run_id}")
        logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
        logger.info("=" * 80)
    except Exception as e:
        import traceback
        sys.stderr.write(f"CRITICAL: Logging initialization failed: {e}\n")
        traceback.print_exc()
        return {'run_id': run_id, 'success': False, 'error': f'Logging init failed: {e}'}
    
    # Load config
    try:
        config = load_config()
        logger.info("Config loaded successfully")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load config: {e}", exc_info=True)
        return {'run_id': run_id, 'success': False, 'error': f'Config load failed: {e}'}
    
    threshold = config.get('good_trade_threshold', 0.3)
    max_hours = 48
    
    logger.info(f"Threshold: {threshold}% | Hours: {max_hours} | Train/Test: {TRAIN_FRAC*100:.0f}/{(1-TRAIN_FRAC)*100:.0f}")
    logger.info("Algorithm: Cohen's d ranking + Youden's J cutpoints + Exhaustive combo search")
    
    result = {
        'run_id': run_id,
        'success': False,
        'suggestions_count': 0,
        'combinations_count': 0,
        'filters_synced': 0,
        'plays_updated': 0,
        'pre_entry_filters_synced': 0,
        'error': None
    }
    
    try:
        # Step 1: Get engine
        logger.info("\n[Step 1/6] Getting engine...")
        try:
            engine = get_engine()
            logger.info(f"Engine acquired: {type(engine).__name__}")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to get engine: {e}", exc_info=True)
            result['error'] = f'Engine acquisition failed: {e}'
            return result
        
        if not engine._running:
            logger.warning("TradingDataEngine not running, starting it...")
            try:
                engine.start()
                logger.info("Engine started")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to start engine: {e}", exc_info=True)
                result['error'] = f'Engine start failed: {e}'
                return result
        
        # Step 2: Load trade data
        logger.info("\n[Step 2/6] Loading trade data...")
        try:
            df = load_trade_data(engine, max_hours)
            logger.info(f"Data loaded: {len(df)} rows")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to load trade data: {e}", exc_info=True)
            result['error'] = f'Data load failed: {e}'
            return result
        
        if len(df) == 0:
            result['error'] = "No trade data found"
            logger.error(result['error'])
            return result
        
        # Show data summary
        good_count = (df['potential_gains'] >= threshold).sum()
        bad_count = (df['potential_gains'] < threshold).sum()
        unique_trades = df['trade_id'].nunique()
        logger.info(f"  Total rows: {len(df):,} | Unique trades: {unique_trades:,}")
        logger.info(f"  Good trades (>= {threshold}%): {good_count:,} ({good_count/len(df)*100:.1f}%)")
        logger.info(f"  Bad trades (< {threshold}%): {bad_count:,} ({bad_count/len(df)*100:.1f}%)")
        
        # Step 3: Build catalog + get filterable columns
        logger.info("\n[Step 3/6] Building filter catalog and splitting train/test...")
        filterable_columns = get_filterable_columns(df)
        column_to_id = save_filter_catalog(engine, filterable_columns)
        logger.info(f"  {len(filterable_columns)} filterable columns (absolute prices excluded)")
        
        # Train/test split (chronological by trade)
        df_train_all, df_test_all = split_train_test(df)
        train_trades = df_train_all['trade_id'].nunique()
        test_trades = df_test_all['trade_id'].nunique()
        logger.info(f"  Train: {train_trades} trades | Test: {test_trades} trades")
        
        # Step 4: Search for best combo across all minutes
        logger.info("\n[Step 4/6] Exhaustive combo search across minutes...")
        
        all_suggestions = []
        best_combo_overall = None
        best_minute = 0
        minute_results = {}
        
        for minute in range(15):
            # Filter to this minute
            if 'minute' in df_train_all.columns:
                df_train_min = df_train_all[df_train_all['minute'] == minute]
                df_test_min = df_test_all[df_test_all['minute'] == minute]
            elif 'interval_idx' in df_train_all.columns:
                # Fallback: use interval_idx, take both sub-intervals for this minute
                df_train_min = df_train_all[df_train_all['interval_idx'].isin([minute * 2, minute * 2 + 1])]
                df_test_min = df_test_all[df_test_all['interval_idx'].isin([minute * 2, minute * 2 + 1])]
            else:
                continue
            
            if len(df_train_min) < 50 or len(df_test_min) < 20:
                continue
            
            # Generate individual suggestions for this minute (for save_suggestions)
            ranked = rank_and_range_features(df_train_min, filterable_columns, threshold)
            for feat in ranked[:40]:  # Top 40 features
                metrics = test_filter_effectiveness(
                    df_train_min, feat['column'], feat['from'], feat['to'], threshold
                )
                if metrics:
                    all_suggestions.append({
                        'column_name': feat['column'],
                        'section': get_section_from_column(feat['column']),
                        'field_name': get_field_name_from_column(feat['column']),
                        'from_value': feat['from'],
                        'to_value': feat['to'],
                        'minute_analyzed': minute,
                        'cohens_d': feat['cohens_d'],
                        **metrics
                    })
            
            # Find best combo for this minute
            combo = find_best_combo_for_minute(
                df_train_min, df_test_min, filterable_columns, minute, threshold
            )
            
            if combo:
                minute_results[minute] = combo
                logger.info(f"  Minute {minute:2d}: test_prec={combo['test_precision']:.1f}%, "
                           f"bad_rem={combo['test_bad_removed']:.1f}%, "
                           f"good_kept={combo['test_good_kept']:.1f}%, "
                           f"filters={combo['n_filters']}, "
                           f"overfit={combo['overfit_delta']:+.1f}%")
                
                if (best_combo_overall is None or 
                    combo['test_precision'] > best_combo_overall['test_precision'] or
                    (combo['test_precision'] == best_combo_overall['test_precision'] and
                     combo['test_good_kept'] > best_combo_overall['test_good_kept'])):
                    best_combo_overall = combo
                    best_minute = minute
            else:
                logger.info(f"  Minute {minute:2d}: no combo beat baseline on test")
        
        result['suggestions_count'] = len(all_suggestions)
        
        # Save suggestions
        if all_suggestions:
            save_suggestions(engine, all_suggestions, column_to_id, max_hours)
        
        # Step 5: Sync best combo to pattern config
        logger.info("\n[Step 5/6] Syncing best filters to pattern config...")
        
        if not best_combo_overall:
            result['error'] = "No filter combinations beat baseline on test set"
            logger.warning(result['error'])
            save_combinations(engine, [], max_hours)
            
            # Still clear old filters to prevent stale filters
            project_id = get_or_create_auto_project(engine)
            try:
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
                    conn.commit()
                logger.info("Old filters cleared (no new combo qualified)")
            except Exception as e:
                logger.error(f"Failed to clear old filters: {e}")
        else:
            # Build combo in format expected by save_combinations and sync
            ranked_features = best_combo_overall['ranked_features']
            combo_columns = best_combo_overall['columns']
            
            # Build filter dicts for each column
            filters_list = []
            for col in combo_columns:
                feat = next((r for r in ranked_features if r['column'] == col), None)
                if feat:
                    filters_list.append({
                        'column_name': col,
                        'section': get_section_from_column(col),
                        'field_name': get_field_name_from_column(col),
                        'from_value': feat['from'],
                        'to_value': feat['to'],
                        'minute_analyzed': best_minute,
                    })
            
            # Compute full metrics on test data for saving
            if 'minute' in df_test_all.columns:
                df_test_best = df_test_all[df_test_all['minute'] == best_minute]
            elif 'interval_idx' in df_test_all.columns:
                df_test_best = df_test_all[df_test_all['interval_idx'].isin([best_minute * 2, best_minute * 2 + 1])]
            else:
                df_test_best = df_test_all
            
            # Apply filter combination on test data for detailed metrics
            passes = pd.Series([True] * len(df_test_best), index=df_test_best.index)
            for f in filters_list:
                if f['column_name'] in df_test_best.columns:
                    passes = passes & (df_test_best[f['column_name']] >= f['from_value']) & (df_test_best[f['column_name']] <= f['to_value'])
            
            potential_gains = df_test_best['potential_gains']
            is_good = potential_gains >= threshold
            is_bad = potential_gains < threshold
            is_negative = potential_gains < 0
            is_0_to_01 = (potential_gains >= 0) & (potential_gains < 0.1)
            is_01_to_02 = (potential_gains >= 0.1) & (potential_gains < 0.2)
            is_02_to_03 = (potential_gains >= 0.2) & (potential_gains < threshold)
            
            good_before = int(is_good.sum())
            bad_before = int(is_bad.sum())
            good_after = int((is_good & passes).sum())
            bad_after = int((is_bad & passes).sum())
            
            combo_for_save = {
                'filters': filters_list,
                'filter_columns': combo_columns,
                'minute_analyzed': best_minute,
                'total_trades': len(df_test_best),
                'good_trades_before': good_before,
                'bad_trades_before': bad_before,
                'good_trades_after': good_after,
                'bad_trades_after': bad_after,
                'good_trades_kept_pct': round(good_after / good_before * 100, 2) if good_before > 0 else 0,
                'bad_trades_removed_pct': round((bad_before - bad_after) / bad_before * 100, 2) if bad_before > 0 else 0,
                'bad_negative_count': int((is_negative & passes).sum()),
                'bad_0_to_01_count': int((is_0_to_01 & passes).sum()),
                'bad_01_to_02_count': int((is_01_to_02 & passes).sum()),
                'bad_02_to_03_count': int((is_02_to_03 & passes).sum()),
                'test_precision': best_combo_overall['test_precision'],
            }
            
            save_combinations(engine, [combo_for_save], max_hours)
            result['combinations_count'] = 1
            
            # Sync to pattern_config_filters
            project_id = get_or_create_auto_project(engine)
            sync_result = sync_best_filters_to_project(engine, combo_for_save, project_id)
            result['filters_synced'] = sync_result.get('filters_synced', 0)
        
        # Step 6: Update AI-enabled plays
        logger.info("\n[Step 6/6] Updating AI-enabled plays...")
        if 'project_id' not in dir():
            project_id = get_or_create_auto_project(engine)
        filters_synced = result.get('filters_synced', 0)
        result['plays_updated'] = update_ai_plays(engine, project_id, run_id, filters_synced)
        
        result['success'] = True
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("PATTERN GENERATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Run ID: {run_id}")
        logger.info(f"  Suggestions generated: {result['suggestions_count']}")
        logger.info(f"  Filters synced: {result['filters_synced']}")
        logger.info(f"  Plays updated: {result['plays_updated']}")
        if best_combo_overall:
            logger.info(f"  Best minute: {best_minute}")
            logger.info(f"  Test precision: {best_combo_overall['test_precision']:.1f}%")
            logger.info(f"  Bad removed: {best_combo_overall['test_bad_removed']:.1f}%")
            logger.info(f"  Good kept: {best_combo_overall['test_good_kept']:.1f}%")
            logger.info(f"  Overfit delta: {best_combo_overall['overfit_delta']:+.1f}%")
            logger.info(f"  Filters: {', '.join(best_combo_overall['columns'])}")
        else:
            logger.info("  No combo qualified -- old filters cleared")
        
        # Log per-minute comparison
        if minute_results:
            logger.info("\n  MINUTE COMPARISON:")
            for m in sorted(minute_results.keys()):
                r = minute_results[m]
                marker = " <-- BEST" if m == best_minute else ""
                logger.info(f"    M{m:2d}: test_prec={r['test_precision']:5.1f}%, "
                           f"bad_rem={r['test_bad_removed']:5.1f}%, "
                           f"good_kept={r['test_good_kept']:5.1f}%, "
                           f"n={r['n_filters']}, overfit={r['overfit_delta']:+5.1f}%{marker}")
        
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        import traceback
        result['error'] = str(e)
        error_trace = traceback.format_exc()
        logger.error("=" * 80)
        logger.error("CRITICAL ERROR IN PATTERN GENERATION")
        logger.error("=" * 80)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {e}")
        logger.error(f"Run ID: {run_id}")
        logger.error("Full traceback:")
        logger.error(error_trace)
        logger.error("=" * 80)
        return result


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    result = run()
    
    if result['success']:
        logger.info("Pattern generation completed successfully!")
    else:
        logger.error(f"Pattern generation failed: {result.get('error')}")
        sys.exit(1)

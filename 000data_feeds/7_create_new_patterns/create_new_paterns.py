#!/usr/bin/env python3
"""
Auto Pattern Filter Generator
=============================
Analyzes trade data to find optimal filter combinations that maximize
bad trade removal while preserving good trades.

This script:
1. Loads trade data from PostgreSQL (follow_the_goat_buyins + trade_filter_values)
2. Analyzes each filter field to find optimal ranges
3. Generates filter combinations using a greedy algorithm
4. Syncs best filters to pattern_config_filters
5. Updates plays with pattern_update_by_ai=1

PERFORMANCE: Uses PostgreSQL pivot (crosstab) to avoid loading millions of rows into Python.

Usage:
    # Run standalone
    python create_new_paterns.py
    
    # Integrated via scheduler/master2.py (runs every 5 minutes)
"""

import json
import logging
import sys
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
            "eth_": "eth_correlation"
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
    
    Args:
        engine: TradingDataEngine instance (not used, kept for compatibility)
        hours: Number of hours to look back
        
    Returns:
        DataFrame with trade data and all trail minute features (pivoted to wide format)
    """
    import time
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
    
    This is the original implementation preserved for reliability.
    """
    import time
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
        safe_col = col.replace("'", "''")  # Escape single quotes
        pivot_columns.append(
            f"MAX(tfv.filter_value) FILTER (WHERE tfv.filter_name = '{safe_col}') AS \"{col}\""
        )
    
    pivot_sql = ",\n                ".join(pivot_columns)
    
    # Build query with all buyins in the time window
    query = f"""
        SELECT 
            b.id as trade_id,
            b.play_id,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            tfv.minute,
            {pivot_sql}
        FROM follow_the_goat_buyins b
        INNER JOIN trade_filter_values tfv ON tfv.buyin_id = b.id
        WHERE b.potential_gains IS NOT NULL
          AND b.followed_at >= NOW() - INTERVAL '{hours} hours'
        GROUP BY b.id, b.play_id, b.followed_at, b.potential_gains, b.our_status, tfv.minute
        ORDER BY b.id, tfv.minute
    """
    
    # Step 3: Execute pivoted query
    logger.info("  Executing pivoted query in PostgreSQL...")
    query_start = time.time()
    results = _read_from_postgres(query, [])
    
    if not results:
        logger.warning("No trade data found with resolved outcomes")
        return pd.DataFrame()
    
    logger.info(f"  Query returned {len(results)} rows in {time.time() - query_start:.1f}s")
    
    # Step 4: Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Convert numeric columns
    for col in filter_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    unique_trades = df['trade_id'].nunique()
    total_time = time.time() - start_time
    logger.info(f"  Loaded {len(df)} rows ({unique_trades} unique trades) in {total_time:.1f}s total")
    
    return df


def get_filterable_columns(df: pd.DataFrame) -> List[str]:
    """
    Get list of numeric columns that can be used as filters.
    
    Excludes:
    - ID columns
    - Timestamp columns
    - String columns
    - Columns in skip_columns config
    """
    config = load_config()
    skip_columns = set(config.get('skip_columns', []))
    skip_columns.update(['trade_id', 'play_id', 'wallet_address', 'followed_at', 
                         'our_status', 'minute', 'potential_gains', 'pat_detected_list',
                         'pat_swing_trend'])
    
    filterable = []
    for col in df.columns:
        if col in skip_columns:
            continue
        
        # Check if column is numeric
        if df[col].dtype in ['float64', 'int64', 'float32', 'int32']:
            # Check if it has enough non-null values
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
# Filter Analysis
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
    
    Args:
        df: Trade data DataFrame
        column_name: Column to filter on
        from_val: Minimum value
        to_val: Maximum value
        threshold: Good trade threshold (e.g., 0.3%)
        
    Returns:
        Dictionary with effectiveness metrics or None if column not found
    """
    if column_name not in df.columns:
        return None
    
    values = df[column_name]
    potential_gains = df['potential_gains']
    
    # Classify trades
    is_good = potential_gains >= threshold
    is_bad = potential_gains < threshold
    
    # Bad trade breakdown
    is_negative = potential_gains < 0
    is_0_to_01 = (potential_gains >= 0) & (potential_gains < 0.1)
    is_01_to_02 = (potential_gains >= 0.1) & (potential_gains < 0.2)
    is_02_to_03 = (potential_gains >= 0.2) & (potential_gains < threshold)
    
    # Before filter counts
    good_before = int(is_good.sum())
    bad_before = int(is_bad.sum())
    total = good_before + bad_before
    
    if total == 0:
        return None
    
    # Apply filter (trade passes if value is within range)
    passes_filter = (values >= from_val) & (values <= to_val)
    
    # After filter counts
    good_after = int((is_good & passes_filter).sum())
    bad_after = int((is_bad & passes_filter).sum())
    
    # Bad trade breakdown after filter
    bad_negative_after = int((is_negative & passes_filter).sum())
    bad_0_to_01_after = int((is_0_to_01 & passes_filter).sum())
    bad_01_to_02_after = int((is_01_to_02 & passes_filter).sum())
    bad_02_to_03_after = int((is_02_to_03 & passes_filter).sum())
    
    # Calculate percentages
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


def find_optimal_threshold(
    df: pd.DataFrame,
    column_name: str,
    threshold: float,
    override_settings: Optional[Dict[str, Any]] = None
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    """
    Find optimal from/to values that maximize bad trade removal while keeping good trades.
    
    Tests multiple percentile combinations and returns the best one.
    Uses settings-driven percentiles with additional test ranges for better coverage.
    
    Args:
        df: Trade data filtered to specific minute
        column_name: Column to analyze
        threshold: Good trade threshold
        override_settings: Optional dict to override config settings
        
    Returns:
        Tuple of (from_val, to_val, metrics) or None if no valid threshold found
    """
    if column_name not in df.columns:
        return None
    
    values = df[column_name].dropna()
    is_good = df['potential_gains'] >= threshold
    good_values = df.loc[is_good, column_name].dropna()
    
    # Need minimum good trades for reliable percentile calculations
    # With < 100 good trades, P10-P90 range can be artificially narrow
    MIN_GOOD_TRADES = 100
    if len(good_values) < MIN_GOOD_TRADES or len(values) < 20:
        if len(good_values) < MIN_GOOD_TRADES and len(good_values) >= 10:
            logger.debug(f"Skipping {column_name}: only {len(good_values)} good trades (need {MIN_GOOD_TRADES} for reliable percentiles)")
        return None
    
    # Use override settings if provided (for scenario testing)
    # Otherwise use VERY PERMISSIVE defaults for initial suggestion generation
    if override_settings:
        config = load_config()
        config.update(override_settings)
        min_good_kept = config.get('min_good_trades_kept_pct', 50)
        min_bad_removed = config.get('min_bad_trades_removed_pct', 10)
    else:
        # PERMISSIVE: Generate lots of candidates, let scenario testing filter
        min_good_kept = 10  # Accept filters that keep just 10% of good trades
        min_bad_removed = 10  # Accept filters that remove just 10% of bad trades
    
    config = load_config()  # Still need config for percentiles
    
    # Get user's preferred percentiles from settings
    user_p_low = config.get('percentile_low', 10)
    user_p_high = config.get('percentile_high', 90)
    
    best_score = -1
    best_result = None
    
    # Build list of percentile pairs to test
    # Start with user's preferred range, then test variations
    percentile_pairs = [
        (user_p_low, user_p_high),           # User's preferred range
    ]
    
    # Add wider ranges if possible
    if user_p_low >= 5:
        percentile_pairs.append((user_p_low - 5, min(user_p_high + 5, 99)))
    
    # Add tighter ranges if possible
    if user_p_low <= 20 and user_p_high >= 80:
        percentile_pairs.append((user_p_low + 5, max(user_p_high - 5, user_p_low + 10)))
    
    # Add very aggressive range for maximum coverage
    if (1, 99) not in percentile_pairs:
        percentile_pairs.append((1, 99))
    
    # Add conservative fallback
    if (10, 90) not in percentile_pairs:
        percentile_pairs.append((10, 90))
    
    # Add moderate options
    if (5, 95) not in percentile_pairs:
        percentile_pairs.append((5, 95))
    if (15, 85) not in percentile_pairs:
        percentile_pairs.append((15, 85))
    
    # Calculate the overall data range to detect suspiciously narrow filters
    all_values = df[column_name].dropna()
    data_range = float(all_values.max() - all_values.min()) if len(all_values) > 0 else 0
    min_acceptable_range = data_range * 0.05  # Filter range should be at least 5% of data range
    
    # Test each percentile combination
    for p_low, p_high in percentile_pairs:
        try:
            from_val = float(np.percentile(good_values, p_low))
            to_val = float(np.percentile(good_values, p_high))
            
            if from_val >= to_val:
                continue
            
            # VALIDATION: Reject suspiciously narrow ranges
            # If the filter range is < 5% of total data range, it's likely due to insufficient good trades
            filter_range = to_val - from_val
            if data_range > 0 and filter_range < min_acceptable_range:
                logger.debug(f"Rejecting narrow filter for {column_name}: range {filter_range:.6f} < min {min_acceptable_range:.6f}")
                continue
            
            metrics = test_filter_effectiveness(df, column_name, from_val, to_val, threshold)
            
            if metrics is None:
                continue
            
            good_kept = metrics['good_trades_kept_pct']
            bad_removed = metrics['bad_trades_removed_pct']
            
            # Skip if doesn't meet minimum requirements
            if good_kept < min_good_kept:
                continue
            if bad_removed < min_bad_removed:
                continue
            
            # Score: prioritize bad removal while keeping good trades
            score = bad_removed * (good_kept / 100)
            
            if score > best_score:
                best_score = score
                best_result = (from_val, to_val, metrics)
                
        except Exception:
            continue
    
    return best_result


def analyze_field(
    df: pd.DataFrame,
    column_name: str,
    minute: Optional[int] = None,
    override_settings: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Analyze a single field and generate filter suggestion.
    
    Args:
        df: Trade data DataFrame (may include multiple minutes)
        column_name: Column name to analyze
        minute: If provided, filter to this minute before analysis
        override_settings: Optional dict to override config settings
        
    Returns:
        Dictionary with suggestion details or None if no valid suggestion
    """
    # Filter by minute if specified
    if minute is not None and 'minute' in df.columns:
        df_filtered = df[df['minute'] == minute].copy()
        if len(df_filtered) == 0:
            return None
    else:
        df_filtered = df
    
    config = load_config()
    if override_settings:
        config.update(override_settings)
    
    threshold = config.get('good_trade_threshold', 0.3)
    result = find_optimal_threshold(df_filtered, column_name, threshold, override_settings)
    
    if result is None:
        return None
    
    from_val, to_val, metrics = result
    
    return {
        'column_name': column_name,
        'section': get_section_from_column(column_name),
        'field_name': get_field_name_from_column(column_name),
        'from_value': round(from_val, 6),
        'to_value': round(to_val, 6),
        'minute_analyzed': minute if minute is not None else 0,
        **metrics
    }


def find_best_minute_for_field(
    df: pd.DataFrame,
    column_name: str
) -> Optional[Tuple[int, Dict[str, Any]]]:
    """
    Find the best minute (0-14) for a filter field by testing all 15 minutes.
    
    Returns the minute that produces the best effectiveness score.
    
    Args:
        df: DataFrame with ALL minutes of trade data
        column_name: Column name to analyze
        
    Returns:
        Tuple of (best_minute, suggestion_dict) or None if no valid suggestion found
    """
    if 'minute' not in df.columns:
        return None
    
    best_score = -1
    best_minute = None
    best_suggestion = None
    
    for minute in range(15):
        suggestion = analyze_field(df, column_name, minute=minute)
        
        if suggestion is None:
            continue
        
        good_kept = suggestion.get('good_trades_kept_pct', 0)
        bad_removed = suggestion.get('bad_trades_removed_pct', 0)
        
        score = bad_removed * (good_kept / 100)
        
        if score > best_score:
            best_score = score
            best_minute = minute
            best_suggestion = suggestion.copy()
            best_suggestion['effectiveness_score'] = round(score, 4)
    
    if best_suggestion is None:
        return None
    
    return (best_minute, best_suggestion)


# =============================================================================
# Filter Combinations
# =============================================================================

def apply_filter(df: pd.DataFrame, column_name: str, from_val: float, to_val: float) -> pd.Series:
    """Apply a filter and return boolean series of trades that pass."""
    if column_name not in df.columns:
        return pd.Series([True] * len(df), index=df.index)
    
    values = df[column_name]
    return (values >= from_val) & (values <= to_val)


def apply_filter_combination(df: pd.DataFrame, filters: List[Dict]) -> pd.Series:
    """Apply multiple filters (AND logic) and return trades that pass ALL."""
    passes = pd.Series([True] * len(df), index=df.index)
    
    for f in filters:
        filter_passes = apply_filter(df, f['column_name'], f['from_value'], f['to_value'])
        passes = passes & filter_passes
    
    return passes


def calculate_combination_metrics(df: pd.DataFrame, passes: pd.Series) -> Dict[str, Any]:
    """Calculate effectiveness metrics for a filter combination."""
    config = load_config()
    threshold = config.get('good_trade_threshold', 0.3)
    potential_gains = df['potential_gains']
    
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
    
    good_kept_pct = (good_after / good_before * 100) if good_before > 0 else 0
    bad_removed_pct = ((bad_before - bad_after) / bad_before * 100) if bad_before > 0 else 0
    
    return {
        'total_trades': len(df),
        'good_trades_before': good_before,
        'bad_trades_before': bad_before,
        'good_trades_after': good_after,
        'bad_trades_after': bad_after,
        'good_trades_kept_pct': round(good_kept_pct, 2),
        'bad_trades_removed_pct': round(bad_removed_pct, 2),
        'bad_negative_count': int((is_negative & passes).sum()),
        'bad_0_to_01_count': int((is_0_to_01 & passes).sum()),
        'bad_01_to_02_count': int((is_01_to_02 & passes).sum()),
        'bad_02_to_03_count': int((is_02_to_03 & passes).sum()),
    }


def find_best_combinations(
    df: pd.DataFrame,
    suggestions: List[Dict[str, Any]],
    minute: int = 0,
    override_settings: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Find optimal filter combinations using a greedy algorithm.
    
    Args:
        df: DataFrame with trade data (should be filtered to specific minute)
        suggestions: List of filter suggestions
        minute: The minute being analyzed
        override_settings: Optional dict to override config settings for testing scenarios
        
    Returns:
        List of combination dicts
    """
    config = load_config()
    if override_settings:
        config.update(override_settings)
    
    max_filters = config.get('max_filters_in_combo', 6)
    min_good_kept = config.get('combo_min_good_kept_pct', 25)
    min_improvement = config.get('combo_min_improvement', 1.0)
    
    results = []
    
    if not suggestions:
        logger.warning("No suggestions to combine")
        return results
    
    logger.info(f"[Minute {minute}] Testing combinations of up to {max_filters} filters from {len(suggestions)} candidates")
    
    # Pre-compute which trades pass each filter
    filter_passes = {}
    for s in suggestions:
        passes = apply_filter(df, s['column_name'], s['from_value'], s['to_value'])
        filter_passes[s['column_name']] = passes
    
    # Track best combination at each size
    best_at_size = {}
    
    # Start with single filters
    for s in suggestions:
        passes = filter_passes[s['column_name']]
        metrics = calculate_combination_metrics(df, passes)
        
        if metrics['good_trades_kept_pct'] >= min_good_kept:
            combo = {
                'filters': [s],
                'filter_columns': [s['column_name']],
                'passes': passes,
                'minute_analyzed': minute,
                **metrics
            }
            
            if 1 not in best_at_size or combo['bad_trades_removed_pct'] > best_at_size[1]['bad_trades_removed_pct']:
                best_at_size[1] = combo
    
    if 1 not in best_at_size:
        logger.warning(f"[Minute {minute}] No single filter meets the minimum good trade retention threshold ({min_good_kept}%)")
        return results
    
    best_single = best_at_size[1]
    results.append(best_single)
    
    logger.info(f"[Minute {minute}] Best single filter: {best_single['filter_columns'][0]} "
                f"({best_single['bad_trades_removed_pct']:.1f}% bad removed, {best_single['good_trades_kept_pct']:.1f}% good kept)")
    
    # Greedy expansion
    current_best = best_single
    
    for size in range(2, max_filters + 1):
        best_next = None
        best_improvement = 0
        
        current_columns = set(current_best['filter_columns'])
        
        # Try adding each unused filter
        for s in suggestions:
            if s['column_name'] in current_columns:
                continue
            
            # Combine passes (AND logic)
            combined_passes = current_best['passes'] & filter_passes[s['column_name']]
            
            metrics = calculate_combination_metrics(df, combined_passes)
            
            # Check if meets minimum retention
            if metrics['good_trades_kept_pct'] < min_good_kept:
                continue
            
            # Calculate improvement
            improvement = metrics['bad_trades_removed_pct'] - current_best['bad_trades_removed_pct']
            
            if improvement >= min_improvement and improvement > best_improvement:
                best_improvement = improvement
                best_next = {
                    'filters': current_best['filters'] + [s],
                    'filter_columns': current_best['filter_columns'] + [s['column_name']],
                    'passes': combined_passes,
                    'minute_analyzed': minute,
                    **metrics
                }
        
        if best_next:
            best_at_size[size] = best_next
            current_best = best_next
            results.append(best_next)
            
            logger.info(f"[Minute {minute}] Best {size}-filter combo: +{best_next['filter_columns'][-1]} "
                        f"({best_next['bad_trades_removed_pct']:.1f}% bad removed, "
                        f"{best_next['good_trades_kept_pct']:.1f}% good kept, +{best_improvement:.1f}% improvement)")
        else:
            logger.debug(f"[Minute {minute}] No beneficial {size}-filter combination found")
            break
    
    return results


def find_best_combinations_all_minutes(
    df: pd.DataFrame,
    suggestions_by_minute: Dict[int, List[Dict[str, Any]]]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Find the best filter combinations by testing across all minutes.
    
    Returns combinations from the minute that produces the best results.
    """
    config = load_config()
    min_filters = config.get('min_filters_in_combo', 2)
    
    if 'minute' not in df.columns:
        logger.warning("No 'minute' column found, falling back to minute 0")
        suggestions = suggestions_by_minute.get(0, [])
        return find_best_combinations(df, suggestions, minute=0), 0
    
    best_combinations = []
    best_minute = 0
    best_score = -1
    
    minute_results = {}
    
    for minute in range(15):
        minute_df = df[df['minute'] == minute]
        
        if len(minute_df) < 20:
            continue
        
        suggestions = suggestions_by_minute.get(minute, [])
        if not suggestions:
            continue
        
        combinations = find_best_combinations(minute_df, suggestions, minute=minute)
        
        if not combinations:
            continue
        
        # Filter to only include combinations with minimum required filters
        valid_combinations = [c for c in combinations if len(c['filter_columns']) >= min_filters]
        
        if not valid_combinations:
            continue
        
        # Score by best combination's effectiveness
        final_combo = valid_combinations[-1]
        score = final_combo['bad_trades_removed_pct'] * (final_combo['good_trades_kept_pct'] / 100)
        
        minute_results[minute] = {
            'combinations': valid_combinations,
            'score': score,
            'bad_removed': final_combo['bad_trades_removed_pct'],
            'good_kept': final_combo['good_trades_kept_pct'],
            'filter_count': len(final_combo['filter_columns'])
        }
        
        if score > best_score:
            best_score = score
            best_minute = minute
            best_combinations = valid_combinations
    
    # Log summary
    if minute_results:
        logger.info("=" * 60)
        logger.info("MINUTE COMPARISON SUMMARY")
        logger.info("=" * 60)
        for m in sorted(minute_results.keys()):
            r = minute_results[m]
            marker = " <-- BEST" if m == best_minute else ""
            logger.info(f"  Minute {m:2d}: {r['bad_removed']:.1f}% bad removed, "
                        f"{r['good_kept']:.1f}% good kept, {r['filter_count']} filters (score: {r['score']:.2f}){marker}")
    
    return best_combinations, best_minute


# =============================================================================
# Saving Results
# =============================================================================

def save_filter_catalog(engine, columns: List[str]) -> Dict[str, int]:
    """
    Save/update filter fields catalog directly to PostgreSQL.
    
    Returns dict mapping column_name to id.
    
    Note: Multiple columns can map to the same field_name (e.g., pm_price_change_10m and tx_price_change_10m
    both become price_change_10m). We deduplicate by field_name and use ON CONFLICT to handle this.
    """
    column_to_id = {}
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Clear existing catalog
                cursor.execute("DELETE FROM filter_fields_catalog")
                
                # Build mapping of field_name -> (column_name, section) to handle duplicates
                field_name_to_info = {}
                for col in columns:
                    config = load_config()
                    section = get_section_from_column(col)
                    field_name = get_field_name_from_column(col)
                    
                    # If multiple columns map to same field_name, keep the first one encountered
                    # (or we could merge sections, but keeping first is simpler)
                    if field_name not in field_name_to_info:
                        field_name_to_info[field_name] = {
                            'column_name': col,
                            'section': section
                        }
                
                # Insert unique field_names
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
                    
                    # Map all columns that share this field_name to the same ID
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
                # Clear existing suggestions
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
    # Clear existing combinations
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
    
    # Check pattern_config_projects table
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
    
    # Create project
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


def sync_best_filters_to_project(engine, combinations: List[Dict[str, Any]], project_id: int) -> Dict[str, Any]:
    """Sync the best filter combination to the project's pattern_config_filters."""
    
    # ALWAYS clear existing filters first (regardless of whether we have new combinations)
    # This ensures old absolute filters are removed when switching to ratio-only mode
    logger.info("Clearing existing filters for project...")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
            conn.commit()
        logger.info("Existing filters cleared")
    except Exception as e:
        logger.warning(f"Could not clear existing filters: {e}")
    
    if not combinations:
        logger.warning("No filter combinations to sync")
        return {"success": False, "error": "No combinations found"}
    
    # Get the best combination (last one has most filters)
    config = load_config()
    min_filters = config.get('min_filters_in_combo', 2)
    valid_combos = [c for c in combinations if len(c['filter_columns']) >= min_filters]
    
    if not valid_combos:
        logger.warning(f"No combinations with >= {min_filters} filters")
        return {"success": False, "error": f"No combinations with >= {min_filters} filters"}
    
    best_combo = valid_combos[-1]  # Most filters that still meets criteria
    
    logger.info(f"Best combination: {len(best_combo['filter_columns'])} filters "
                f"(bad removed: {best_combo['bad_trades_removed_pct']:.1f}%, "
                f"good kept: {best_combo['good_trades_kept_pct']:.1f}%)")
    
    # Insert new filters
    filters_inserted = 0
    for i, f in enumerate(best_combo['filters'], 1):
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO pattern_config_filters
                        (id, project_id, name, section, minute, field_name, field_column,
                         from_value, to_value, include_null, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        i + (project_id * 1000),  # Ensure unique ID
                        project_id,
                        f"Auto: {f['column_name']}",
                        f.get('section', get_section_from_column(f['column_name'])),
                        f.get('minute_analyzed', 0),
                        f.get('field_name', get_field_name_from_column(f['column_name'])),
                        f['column_name'],
                        f['from_value'],
                        f['to_value'],
                        0,  # include_null = SMALLINT
                        1   # is_active = SMALLINT
                    ])
                conn.commit()
            filters_inserted += 1
            logger.info(f"  Added filter: {f['column_name']} [{f['from_value']:.6f} - {f['to_value']:.6f}]")
        except Exception as e:
            logger.error(f"Failed to insert filter {f['column_name']}: {e}")
    
    return {
        "success": True,
        "filters_synced": filters_inserted,
        "bad_removed_pct": best_combo['bad_trades_removed_pct'],
        "good_kept_pct": best_combo['good_trades_kept_pct'],
    }


def update_ai_plays(engine, project_id: int, run_id: str, pattern_count: int = 0) -> int:
    """Update all plays with pattern_update_by_ai=1 to use the AutoFilters project and log updates."""
    try:
        # Get plays with AI updates enabled
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
                        # Update the play
                        cursor.execute("""
                            UPDATE follow_the_goat_plays 
                            SET project_ids = %s 
                            WHERE id = %s AND pattern_update_by_ai = 1
                        """, [project_ids_json, play_id])
                        
                        # Log the update to ai_play_updates table
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
                # Log failure
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
# Multi-Scenario Testing
# =============================================================================

def calculate_scenario_score(bad_removed_pct: float, good_kept_pct: float) -> float:
    """
    Calculate score for a scenario, heavily prioritizing bad trade removal.
    
    Target: 95%+ bad removal (VERY AGGRESSIVE)
    Accept: 20-30% good retention (MORE AGGRESSIVE - prefer fewer but better trades)
    
    Scoring:
    - Bad removal weighted 15x (1425 points for 95% removal) - INCREASED from 10x
    - Good retention weighted 1x (20-30 points for 20-30% retention)
    - Penalty for bad removal < 90% (STRICTER - was 85%)
    """
    bad_removal_score = bad_removed_pct * 15  # 1425 points for 95% removal (was 10x)
    good_retention_score = good_kept_pct * 1  # 20-30 points for 20-30% retention
    
    # Penalty if bad removal < 90% (STRICTER - was 85%)
    if bad_removed_pct < 90:
        bad_removal_score *= 0.3  # Harsher penalty (was 0.5)
    
    return bad_removal_score + good_retention_score


def generate_test_scenarios() -> List[Dict[str, Any]]:
    """
    Generate optimized set of test scenarios.
    
    Uses smart filtering to test ~48 combinations instead of all 192.
    Focus on configurations likely to achieve 95%+ bad removal.
    """
    scenarios = []
    
    # Aggressive configurations for maximum bad removal
    # Minimum 2 filters for more robust filtering
    min_filters_options = [2, 3, 4]
    analysis_hours_options = [6, 12, 24, 48]
    
    # For each time window, test filter count combinations
    for hours in analysis_hours_options:
        for min_filters in min_filters_options:
            # Very aggressive: prioritize bad removal over good retention (MORE AGGRESSIVE)
            scenarios.append({
                'name': f'H{hours}_F{min_filters}_VeryAgg',
                'settings': {
                    'analysis_hours': hours,
                    'min_filters_in_combo': min_filters,
                    'min_good_trades_kept_pct': 15,  # LOWER - accept fewer good trades (was 20)
                    'min_bad_trades_removed_pct': 97,  # HIGHER - remove more bad trades (was 95)
                    'percentile_low': 1,
                    'percentile_high': 99,
                }
            })
            
            # Aggressive: good balance (MORE AGGRESSIVE)
            scenarios.append({
                'name': f'H{hours}_F{min_filters}_Agg',
                'settings': {
                    'analysis_hours': hours,
                    'min_filters_in_combo': min_filters,
                    'min_good_trades_kept_pct': 25,  # LOWER (was 30)
                    'min_bad_trades_removed_pct': 95,  # HIGHER (was 90)
                    'percentile_low': 5,
                    'percentile_high': 95,
                }
            })
            
            # Moderate: fallback if aggressive fails (MORE AGGRESSIVE)
            scenarios.append({
                'name': f'H{hours}_F{min_filters}_Mod',
                'settings': {
                    'analysis_hours': hours,
                    'min_filters_in_combo': min_filters,
                    'min_good_trades_kept_pct': 30,  # LOWER (was 40)
                    'min_bad_trades_removed_pct': 90,  # HIGHER (was 85)
                    'percentile_low': 10,
                    'percentile_high': 90,
                }
            })
    
    return scenarios


def test_scenario(
    df: pd.DataFrame,
    scenario: Dict[str, Any],
    suggestions_by_minute: Dict[int, List[Dict[str, Any]]],
    threshold: float
) -> Optional[Dict[str, Any]]:
    """
    Test a single scenario configuration.
    
    Returns results dict or None if no valid combinations found.
    """
    settings = scenario['settings']
    
    # Filter dataframe by analysis hours
    analysis_hours = settings.get('analysis_hours', 24)
    cutoff_time = df['followed_at'].max() - pd.Timedelta(hours=analysis_hours)
    df_filtered = df[df['followed_at'] >= cutoff_time].copy()
    
    if len(df_filtered) < 100:  # Need minimum data
        return None
    
    # Test across all minutes with these settings
    best_combinations = []
    best_minute = 0
    best_score = -1
    
    for minute in range(15):
        minute_df = df_filtered[df_filtered['minute'] == minute]
        
        if len(minute_df) < 20:
            continue
        
        suggestions = suggestions_by_minute.get(minute, [])
        if not suggestions:
            continue
        
        # Find combinations with scenario settings
        combinations = find_best_combinations(minute_df, suggestions, minute=minute, override_settings=settings)
        
        if not combinations:
            continue
        
        # Filter by min_filters requirement
        min_filters = settings['min_filters_in_combo']
        valid_combinations = [c for c in combinations if len(c['filter_columns']) >= min_filters]
        
        if not valid_combinations:
            continue
        
        # Score the best combination from this minute
        final_combo = valid_combinations[-1]
        score = calculate_scenario_score(
            final_combo['bad_trades_removed_pct'],
            final_combo['good_trades_kept_pct']
        )
        
        if score > best_score:
            best_score = score
            best_minute = minute
            best_combinations = valid_combinations
    
    if not best_combinations:
        return None
    
    best_combo = best_combinations[-1]
    
    return {
        'scenario': scenario,
        'combinations': best_combinations,
        'best_combo': best_combo,
        'best_minute': best_minute,
        'score': best_score,
        'bad_removed_pct': best_combo['bad_trades_removed_pct'],
        'good_kept_pct': best_combo['good_trades_kept_pct'],
        'filter_count': len(best_combo['filter_columns']),
        'filter_columns': best_combo['filter_columns'],
        'filters': best_combo['filters']
    }


def save_scenario_results(run_id: str, results: List[Dict[str, Any]], selected_idx: int):
    """Save scenario testing results to database."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for idx, result in enumerate(results):
                    scenario = result['scenario']
                    best_combo = result['best_combo']
                    
                    cursor.execute("""
                        INSERT INTO filter_scenario_results
                        (run_id, scenario_name, settings, filter_count, 
                         bad_trades_removed_pct, good_trades_kept_pct, score,
                         filters_applied, rank, was_selected)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        run_id,
                        scenario['name'],
                        json.dumps(scenario['settings']),
                        result['filter_count'],
                        result['bad_removed_pct'],
                        result['good_kept_pct'],
                        result['score'],
                        json.dumps(result['filter_columns']),
                        idx + 1,
                        idx == selected_idx
                    ])
            conn.commit()
        logger.info(f"Saved {len(results)} scenario results to database")
    except Exception as e:
        logger.error(f"Failed to save scenario results: {e}", exc_info=True)


def run_multi_scenario_analysis(
    df: pd.DataFrame,
    suggestions_by_minute: Dict[int, List[Dict[str, Any]]],
    threshold: float,
    run_id: str
) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
    """
    Run multi-scenario testing to find optimal filter configuration.
    
    Returns:
        - List of best combinations from winning scenario
        - Best minute
        - Selected scenario details
    """
    logger.info("\n" + "=" * 80)
    logger.info("MULTI-SCENARIO ANALYSIS - Testing configurations for 95%+ bad removal")
    logger.info("=" * 80)
    
    # Generate test scenarios
    scenarios = generate_test_scenarios()
    logger.info(f"Generated {len(scenarios)} test scenarios")
    logger.info(f"Testing on {len(df):,} data points ({df['trade_id'].nunique():,} unique trades)")
    
    # Test each scenario
    results = []
    for idx, scenario in enumerate(scenarios, 1):
        result = test_scenario(df, scenario, suggestions_by_minute, threshold)
        if result:
            results.append(result)
            logger.debug(f"  [{idx:3d}/{len(scenarios)}] {scenario['name']:<25} "
                        f"Score: {result['score']:6.1f} | "
                        f"Bad: {result['bad_removed_pct']:5.1f}% | "
                        f"Good: {result['good_kept_pct']:5.1f}% | "
                        f"Filters: {result['filter_count']}")
    
    if not results:
        logger.warning("No scenarios produced valid filter combinations!")
        return [], 0, {}
    
    # Sort by score (highest first)
    results.sort(key=lambda x: x['score'], reverse=True)
    
    # Log top results
    logger.info("\n" + "-" * 80)
    logger.info("TOP 5 SCENARIOS BY SCORE:")
    logger.info("-" * 80)
    
    for idx, result in enumerate(results[:5], 1):
        scenario = result['scenario']
        marker = " ⭐ SELECTED" if idx == 1 else ""
        logger.info(f"  #{idx}: {scenario['name']:<25} | "
                   f"Score: {result['score']:6.1f} | "
                   f"Bad: {result['bad_removed_pct']:5.1f}% | "
                   f"Good: {result['good_kept_pct']:5.1f}% | "
                   f"Filters: {result['filter_count']}{marker}")
        if idx == 1:
            logger.info(f"       Settings: {json.dumps(scenario['settings'], indent=15)[1:]}")
            logger.info(f"       Filters: {', '.join(result['filter_columns'][:3])}"
                       f"{' + ' + str(result['filter_count'] - 3) + ' more' if result['filter_count'] > 3 else ''}")
    
    logger.info("-" * 80)
    
    # Select best scenario
    best_result = results[0]
    
    # Save all scenario results to database
    save_scenario_results(run_id, results[:10], 0)  # Save top 10
    
    return best_result['combinations'], best_result['best_minute'], best_result


# =============================================================================
# Main Entry Point
# =============================================================================

def run() -> Dict[str, Any]:
    """
    Main entry point - called by scheduler every 10 minutes.
    
    Auto-tests multiple scenarios to find optimal filter configuration.
    Only Good Trade Threshold is user-locked, all other settings auto-optimized.
    
    Returns dict with run results.
    """
    run_id = str(uuid.uuid4())[:8]
    
    # START: Critical error logging wrapper
    try:
        logger.info("=" * 80)
        logger.info(f"AUTO FILTER PATTERN GENERATOR [AUTO-OPTIMIZE MODE] - Run ID: {run_id}")
        logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
        logger.info("=" * 80)
    except Exception as e:
        # If even logging fails, write to stderr
        import sys
        sys.stderr.write(f"CRITICAL: Logging initialization failed: {e}\n")
        import traceback
        traceback.print_exc()
        return {
            'run_id': run_id,
            'success': False,
            'error': f'Logging initialization failed: {e}'
        }
    
    # Load config fresh from database (never cached)
    try:
        config = load_config()
        logger.info("✓ Config loaded successfully")
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load config: {e}", exc_info=True)
        return {
            'run_id': run_id,
            'success': False,
            'error': f'Config load failed: {e}'
        }
    
    threshold = config.get('good_trade_threshold', 0.6)
    ratio_only = config.get('is_ratio', False)
    
    logger.info(f"User-locked threshold: {threshold}% (good trades must exceed this)")
    logger.info(f"Ratio-only mode: {'ENABLED ✅' if ratio_only else 'DISABLED (using absolute values)'}")
    logger.info("AUTO-OPTIMIZING: Testing ~48 scenarios (various hours, filters, thresholds)")
    logger.info("TARGET: 95%+ bad trade removal (VERY AGGRESSIVE)")
    
    if ratio_only:
        logger.info("  ⚠️  RATIO MODE: Only percentage/ratio filters will be created")
        logger.info("  ⚠️  This prevents filters from breaking when market prices change")
    else:
        logger.info("  ⚠️  ABSOLUTE MODE: Filters will use actual price values")
        logger.info("  ⚠️  These filters may break if market prices change significantly")
    
    result = {
        'run_id': run_id,
        'success': False,
        'suggestions_count': 0,
        'combinations_count': 0,
        'filters_synced': 0,
        'plays_updated': 0,
        'error': None
    }
    
    try:
        # Get engine
        logger.info("Getting TradingDataEngine instance...")
        try:
            engine = get_engine()
            logger.info(f"✓ Engine acquired: {type(engine).__name__}")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to get engine: {e}", exc_info=True)
            result['error'] = f'Engine acquisition failed: {e}'
            return result
        
        if not engine._running:
            logger.warning("TradingDataEngine not running, starting it...")
            try:
                engine.start()
                logger.info("✓ Engine started")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to start engine: {e}", exc_info=True)
                result['error'] = f'Engine start failed: {e}'
                return result
        
        # Step 1: Load trade data (use maximum window for multi-scenario testing)
        logger.info("\n[Step 1/6] Loading trade data...")
        max_hours = 48  # Load 48 hours - crypto is volatile, this provides enough data
        try:
            df = load_trade_data(engine, max_hours)
            logger.info(f"✓ Data loaded: {len(df)} rows")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to load trade data: {e}", exc_info=True)
            result['error'] = f'Data load failed: {e}'
            return result
        
        if len(df) == 0:
            result['error'] = "No trade data found"
            logger.error(result['error'])
            return result
        
        # Show data summary
        config = load_config()
        threshold = config.get('good_trade_threshold', 0.3)
        good_count = (df['potential_gains'] >= threshold).sum()
        bad_count = (df['potential_gains'] < threshold).sum()
        logger.info(f"  Total rows: {len(df):,}")
        logger.info(f"  Unique trades: {df['trade_id'].nunique():,}")
        logger.info(f"  Good trades (>= {threshold}%): {good_count:,} ({good_count/len(df)*100:.1f}%)")
        logger.info(f"  Bad trades (< {threshold}%): {bad_count:,} ({bad_count/len(df)*100:.1f}%)")
        
        # Step 2: Build filter catalog
        logger.info("\n[Step 2/6] Building filter fields catalog...")
        filterable_columns = get_filterable_columns(df)
        column_to_id = save_filter_catalog(engine, filterable_columns)
        logger.info(f"  Found {len(filterable_columns)} filterable columns")
        
        # Step 3: Generate filter suggestions for each minute
        logger.info("\n[Step 3/6] Generating filter suggestions...")
        suggestions_by_minute = {}
        all_suggestions = []
        
        for column in filterable_columns:
            result_tuple = find_best_minute_for_field(df, column)
            if result_tuple:
                best_minute, suggestion = result_tuple
                if best_minute not in suggestions_by_minute:
                    suggestions_by_minute[best_minute] = []
                suggestions_by_minute[best_minute].append(suggestion)
                all_suggestions.append(suggestion)
        
        result['suggestions_count'] = len(all_suggestions)
        logger.info(f"  Generated {len(all_suggestions)} filter suggestions")
        
        if not all_suggestions:
            result['error'] = "No filter suggestions could be generated"
            logger.error(result['error'])
            return result
        
        # Save suggestions
        save_suggestions(engine, all_suggestions, column_to_id, 48)  # Use max hours for suggestions
        
        # Step 4: Run multi-scenario analysis to find optimal configuration
        logger.info("\n[Step 4/6] Running multi-scenario analysis...")
        combinations, best_minute, selected_scenario = run_multi_scenario_analysis(
            df, suggestions_by_minute, threshold, run_id
        )
        
        result['combinations_count'] = len(combinations)
        result['selected_scenario'] = selected_scenario.get('scenario', {}).get('name', 'none') if selected_scenario else 'none'
        result['scenario_score'] = selected_scenario.get('score', 0) if selected_scenario else 0
        
        if not combinations:
            result['error'] = "No valid filter combinations found"
            logger.warning(result['error'])
            # Still save what we have
            save_combinations(engine, [], max_hours)
        else:
            save_combinations(engine, combinations, max_hours)
            logger.info(f"  Best minute: {best_minute}")
            logger.info(f"  Found {len(combinations)} valid combinations")
        
        # Step 5: Sync to pattern config
        logger.info("\n[Step 5/6] Syncing filters to pattern config...")
        project_id = get_or_create_auto_project(engine)
        
        if not combinations:
            # No new combinations found, but still clear old filters if in ratio_only mode
            # This prevents absolute filters from lingering when switching to ratio mode
            config = load_config()
            if config.get('is_ratio', False):
                logger.warning("No valid combinations found in ratio-only mode")
                logger.info("Clearing old absolute filters to prevent market price issues...")
                try:
                    with get_postgres() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
                        conn.commit()
                    logger.info("Old filters cleared successfully")
                except Exception as e:
                    logger.error(f"Failed to clear old filters: {e}")
            
            result['error'] = "No valid filter combinations found"
            logger.warning(result['error'])
            save_combinations(engine, [], max_hours)
        else:
            sync_result = sync_best_filters_to_project(engine, combinations, project_id)
            result['filters_synced'] = sync_result.get('filters_synced', 0)
            save_combinations(engine, combinations, max_hours)
            logger.info(f"  Best minute: {best_minute}")
            logger.info(f"  Found {len(combinations)} valid combinations")
        
        # Step 6: Update AI-enabled plays
        logger.info("\n[Step 6/6] Updating AI-enabled plays...")
        filters_synced = result.get('filters_synced', 0)
        result['plays_updated'] = update_ai_plays(engine, project_id, run_id, filters_synced)
        
        result['success'] = True
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("PATTERN GENERATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Run ID: {run_id}")
        logger.info(f"  Suggestions generated: {result['suggestions_count']}")
        logger.info(f"  Combinations found: {result['combinations_count']}")
        logger.info(f"  Filters synced: {result['filters_synced']}")
        logger.info(f"  Plays updated: {result['plays_updated']}")
        if combinations:
            best = combinations[-1]
            logger.info(f"  Best result: {best['bad_trades_removed_pct']:.1f}% bad removed, "
                        f"{best['good_trades_kept_pct']:.1f}% good kept")
        if result.get('selected_scenario'):
            logger.info(f"  Selected scenario: {result['selected_scenario']}")
            logger.info(f"  Scenario score: {result.get('scenario_score', 0):.1f}")
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


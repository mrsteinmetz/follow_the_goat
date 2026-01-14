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
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import uuid

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.trading_engine import get_engine
from core.database import get_postgres

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
    Load trade data by joining follow_the_goat_buyins with trade_filter_values.
    
    OPTIMIZED: Does the pivot directly in PostgreSQL using conditional aggregation,
    avoiding loading millions of rows into Python memory.
    
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
    
    # Step 1: Get distinct filter columns (fast query)
    logger.info("  Getting distinct filter columns...")
    filter_columns = _get_filter_columns(hours, ratio_only=ratio_only)
    
    if not filter_columns:
        logger.warning("No filter columns found")
        return pd.DataFrame()
    
    logger.info(f"  Found {len(filter_columns)} filter columns in {time.time() - start_time:.1f}s")
    
    # Step 2: Build pivoted query using conditional aggregation
    # This is MUCH faster than loading all rows and pivoting in Python
    pivot_columns = []
    for col in filter_columns:
        # Use MAX with FILTER for conditional aggregation (PostgreSQL 9.4+)
        safe_col = col.replace("'", "''")  # Escape single quotes
        pivot_columns.append(
            f"MAX(tfv.filter_value) FILTER (WHERE tfv.filter_name = '{safe_col}') AS \"{col}\""
        )
    
    pivot_sql = ",\n                ".join(pivot_columns)
    
    # Build query with all buyins in the time window (no artificial limit)
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
    
    # Step 4: Convert to DataFrame (already pivoted, no need for pandas pivot)
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
    threshold: float
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    """
    Find optimal from/to values that maximize bad trade removal while keeping good trades.
    
    Tests multiple percentile combinations and returns the best one.
    
    Args:
        df: Trade data filtered to specific minute
        column_name: Column to analyze
        threshold: Good trade threshold
        
    Returns:
        Tuple of (from_val, to_val, metrics) or None if no valid threshold found
    """
    if column_name not in df.columns:
        return None
    
    values = df[column_name].dropna()
    is_good = df['potential_gains'] >= threshold
    good_values = df.loc[is_good, column_name].dropna()
    
    if len(good_values) < 10 or len(values) < 20:
        return None
    
    config = load_config()
    min_good_kept = config.get('min_good_trades_kept_pct', 50)
    min_bad_removed = config.get('min_bad_trades_removed_pct', 10)
    
    best_score = -1
    best_result = None
    
    # Try different percentile combinations
    percentile_pairs = [
        (10, 90),
        (5, 95),
        (15, 85),
        (20, 80),
        (25, 75),
    ]
    
    for p_low, p_high in percentile_pairs:
        try:
            from_val = float(np.percentile(good_values, p_low))
            to_val = float(np.percentile(good_values, p_high))
            
            if from_val >= to_val:
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
    minute: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Analyze a single field and generate filter suggestion.
    
    Args:
        df: Trade data DataFrame (may include multiple minutes)
        column_name: Column name to analyze
        minute: If provided, filter to this minute before analysis
        
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
    threshold = config.get('good_trade_threshold', 0.3)
    result = find_optimal_threshold(df_filtered, column_name, threshold)
    
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
    minute: int = 0
) -> List[Dict[str, Any]]:
    """
    Find optimal filter combinations using a greedy algorithm.
    
    Args:
        df: DataFrame with trade data (should be filtered to specific minute)
        suggestions: List of filter suggestions
        minute: The minute being analyzed
        
    Returns:
        List of combination dicts
    """
    config = load_config()
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
    
    # Clear existing filters for this project
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
            conn.commit()
    except Exception as e:
        logger.warning(f"Could not clear existing filters: {e}")
    
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
# Main Entry Point
# =============================================================================

def run() -> Dict[str, Any]:
    """
    Main entry point - called by scheduler every 15 minutes.
    
    Returns dict with run results.
    """
    run_id = str(uuid.uuid4())[:8]
    logger.info("=" * 60)
    logger.info(f"AUTO FILTER PATTERN GENERATOR - Run ID: {run_id}")
    logger.info("=" * 60)
    
    # Load config fresh from database (never cached)
    config = load_config()
    
    hours = config.get('analysis_hours', 24)
    logger.info(f"Analysis window: {hours} hours")
    
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
        engine = get_engine()
        
        if not engine._running:
            logger.warning("TradingDataEngine not running, starting it...")
            engine.start()
        
        # Step 1: Load trade data
        logger.info("\n[Step 1/6] Loading trade data...")
        df = load_trade_data(engine, hours)
        
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
        save_suggestions(engine, all_suggestions, column_to_id, hours)
        
        # Step 4: Find best filter combinations
        logger.info("\n[Step 4/6] Finding best filter combinations...")
        combinations, best_minute = find_best_combinations_all_minutes(df, suggestions_by_minute)
        
        result['combinations_count'] = len(combinations)
        
        if not combinations:
            result['error'] = "No valid filter combinations found"
            logger.warning(result['error'])
            # Still save what we have
            save_combinations(engine, [], hours)
        else:
            save_combinations(engine, combinations, hours)
            logger.info(f"  Best minute: {best_minute}")
            logger.info(f"  Found {len(combinations)} valid combinations")
        
        # Step 5: Sync to pattern config
        logger.info("\n[Step 5/6] Syncing filters to pattern config...")
        project_id = get_or_create_auto_project(engine)
        sync_result = sync_best_filters_to_project(engine, combinations, project_id)
        
        result['filters_synced'] = sync_result.get('filters_synced', 0)
        
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
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        import traceback
        result['error'] = str(e)
        logger.error(f"Pattern generation failed: {e}")
        logger.error(traceback.format_exc())
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


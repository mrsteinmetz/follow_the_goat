#!/usr/bin/env python3
"""
Filter Optimization Testing Script
==================================
Analyzes today's missed opportunities and tests different strategies to find better filter combinations.

This script will:
1. Find all trades from today with potential_gains data
2. Identify which ones passed current filter settings vs which didn't
3. Test alternative filter strategies:
   - More relaxed thresholds
   - Different minute windows
   - Different percentile ranges
   - Hybrid approaches (combining multiple strategies)
4. Recommend optimal settings based on actual performance
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Data Loading
# =============================================================================

def get_todays_trades() -> pd.DataFrame:
    """Load all trades from today with potential_gains and filter values."""
    logger.info("Loading today's trade data...")
    
    query = """
        SELECT 
            b.id,
            b.wallet_address,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            b.play_id
        FROM follow_the_goat_buyins b
        WHERE b.followed_at >= CURRENT_DATE
          AND b.potential_gains IS NOT NULL
        ORDER BY b.followed_at DESC
    """
    
    trades = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                trades = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to load trades: {e}")
        return pd.DataFrame()
    
    if not trades:
        logger.warning("No trades found for today")
        return pd.DataFrame()
    
    df = pd.DataFrame(trades)
    logger.info(f"Loaded {len(df)} trades from today")
    return df


def get_trade_filter_values(trade_ids: List[int]) -> pd.DataFrame:
    """Load filter values for specific trades."""
    if not trade_ids:
        return pd.DataFrame()
    
    logger.info(f"Loading filter values for {len(trade_ids)} trades...")
    
    # Get all filter columns
    query_filters = """
        SELECT DISTINCT filter_name
        FROM trade_filter_values
        WHERE buyin_id = ANY(%s)
        ORDER BY filter_name
    """
    
    filter_columns = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query_filters, [trade_ids])
                results = cursor.fetchall()
                filter_columns = [r['filter_name'] for r in results]
    except Exception as e:
        logger.error(f"Failed to get filter columns: {e}")
        return pd.DataFrame()
    
    if not filter_columns:
        logger.warning("No filter columns found")
        return pd.DataFrame()
    
    # Build pivot query
    pivot_columns = []
    for col in filter_columns:
        safe_col = col.replace("'", "''")
        pivot_columns.append(
            f"MAX(filter_value) FILTER (WHERE filter_name = '{safe_col}') AS \"{col}\""
        )
    
    pivot_sql = ",\n            ".join(pivot_columns)
    
    query = f"""
        SELECT 
            buyin_id,
            minute,
            {pivot_sql}
        FROM trade_filter_values
        WHERE buyin_id = ANY(%s)
        GROUP BY buyin_id, minute
        ORDER BY buyin_id, minute
    """
    
    results = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [trade_ids])
                results = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to load filter values: {e}")
        return pd.DataFrame()
    
    if not results:
        logger.warning("No filter values found")
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    
    # Convert numeric columns
    for col in filter_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    logger.info(f"Loaded {len(df)} filter value rows (pivoted: one row per buyin-minute)")
    logger.info(f"  Note: Raw database has ~{len(trade_ids) * 15 * len(filter_columns)} filter value rows")
    logger.info(f"  Pivoted format groups by buyin_id + minute, creating one row with all filters as columns")
    return df


def get_current_filter_settings() -> List[Dict[str, Any]]:
    """Get currently active filter settings from pattern_config_filters."""
    logger.info("Loading current filter settings...")
    
    query = """
        SELECT 
            pcf.id,
            pcf.project_id,
            pcf.name,
            pcf.section,
            pcf.minute,
            pcf.field_name,
            pcf.field_column,
            pcf.from_value,
            pcf.to_value,
            pcf.include_null,
            pcf.is_active,
            pcp.name as project_name
        FROM pattern_config_filters pcf
        JOIN pattern_config_projects pcp ON pcp.id = pcf.project_id
        WHERE pcf.is_active = 1
        ORDER BY pcf.project_id, pcf.minute, pcf.id
    """
    
    filters = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                filters = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to load filters: {e}")
        return []
    
    logger.info(f"Loaded {len(filters)} active filters from {len(set(f['project_id'] for f in filters))} projects")
    return filters


def check_if_trade_passes_filters(filter_values: pd.DataFrame, filters: List[Dict], minute: int) -> bool:
    """Check if a trade passes all filters for a given minute."""
    if minute not in filter_values['minute'].values:
        return False
    
    minute_data = filter_values[filter_values['minute'] == minute].iloc[0]
    
    for f in filters:
        filter_minute = f.get('minute', 0)
        if filter_minute != minute:
            continue
        
        column = f['field_column']
        from_val = f['from_value']
        to_val = f['to_value']
        include_null = f.get('include_null', 0)
        
        if column not in minute_data:
            if include_null:
                continue
            else:
                return False
        
        value = minute_data[column]
        
        if pd.isna(value):
            if include_null:
                continue
            else:
                return False
        
        # Check if value is in range
        if value < from_val or value > to_val:
            return False
    
    return True


# =============================================================================
# Analysis Functions
# =============================================================================

def analyze_missed_opportunities(trades_df: pd.DataFrame, filter_values_df: pd.DataFrame, 
                                 current_filters: List[Dict], good_threshold: float = 0.6) -> Dict[str, Any]:
    """Analyze which good trades were missed by current filters."""
    
    logger.info("\n" + "="*80)
    logger.info("ANALYZING MISSED OPPORTUNITIES")
    logger.info("="*80)
    
    good_trades = trades_df[trades_df['potential_gains'] >= good_threshold].copy()
    bad_trades = trades_df[trades_df['potential_gains'] < good_threshold].copy()
    
    logger.info(f"\nTotal trades today: {len(trades_df)}")
    logger.info(f"Good trades (>= {good_threshold}%): {len(good_trades)} ({len(good_trades)/len(trades_df)*100:.1f}%)")
    logger.info(f"Bad trades (< {good_threshold}%): {len(bad_trades)} ({len(bad_trades)/len(trades_df)*100:.1f}%)")
    
    # Group filters by project
    filters_by_project = {}
    for f in current_filters:
        project_id = f['project_id']
        if project_id not in filters_by_project:
            filters_by_project[project_id] = {
                'name': f['project_name'],
                'filters': []
            }
        filters_by_project[project_id]['filters'].append(f)
    
    results = {}
    
    for project_id, project_data in filters_by_project.items():
        project_name = project_data['name']
        filters = project_data['filters']
        
        logger.info(f"\n--- Project: {project_name} (ID: {project_id}) ---")
        logger.info(f"Active filters: {len(filters)}")
        
        # Test each minute (0-14)
        best_minute_results = None
        best_minute = None
        best_score = -1
        
        for minute in range(15):
            minute_filters = [f for f in filters if f.get('minute', 0) == minute]
            if not minute_filters:
                continue
            
            good_passed = 0
            good_failed = 0
            bad_passed = 0
            bad_failed = 0
            
            # Check good trades
            for _, trade in good_trades.iterrows():
                trade_filter_values = filter_values_df[filter_values_df['buyin_id'] == trade['id']]
                if len(trade_filter_values) == 0:
                    continue
                
                if check_if_trade_passes_filters(trade_filter_values, minute_filters, minute):
                    good_passed += 1
                else:
                    good_failed += 1
            
            # Check bad trades
            for _, trade in bad_trades.iterrows():
                trade_filter_values = filter_values_df[filter_values_df['buyin_id'] == trade['id']]
                if len(trade_filter_values) == 0:
                    continue
                
                if check_if_trade_passes_filters(trade_filter_values, minute_filters, minute):
                    bad_passed += 1
                else:
                    bad_failed += 1
            
            total_good = good_passed + good_failed
            total_bad = bad_passed + bad_failed
            
            if total_good == 0:
                continue
            
            good_kept_pct = (good_passed / total_good * 100) if total_good > 0 else 0
            bad_removed_pct = (bad_failed / total_bad * 100) if total_bad > 0 else 0
            
            score = bad_removed_pct * (good_kept_pct / 100)
            
            logger.info(f"  Minute {minute:2d}: Good kept: {good_passed}/{total_good} ({good_kept_pct:.1f}%), "
                       f"Bad removed: {bad_failed}/{total_bad} ({bad_removed_pct:.1f}%), Score: {score:.2f}")
            
            if score > best_score:
                best_score = score
                best_minute = minute
                best_minute_results = {
                    'minute': minute,
                    'good_passed': good_passed,
                    'good_failed': good_failed,
                    'bad_passed': bad_passed,
                    'bad_failed': bad_failed,
                    'good_kept_pct': good_kept_pct,
                    'bad_removed_pct': bad_removed_pct,
                    'score': score
                }
        
        if best_minute_results:
            results[project_id] = {
                'project_name': project_name,
                'best_minute': best_minute,
                'results': best_minute_results,
                'filters': filters
            }
            
            logger.info(f"\n  BEST MINUTE: {best_minute}")
            logger.info(f"  Good trades caught: {best_minute_results['good_passed']}/{best_minute_results['good_passed'] + best_minute_results['good_failed']} ({best_minute_results['good_kept_pct']:.1f}%)")
            logger.info(f"  Bad trades filtered: {best_minute_results['bad_failed']}/{best_minute_results['bad_passed'] + best_minute_results['bad_failed']} ({best_minute_results['bad_removed_pct']:.1f}%)")
    
    return results


def test_alternative_strategies(trades_df: pd.DataFrame, filter_values_df: pd.DataFrame, 
                                good_threshold: float = 0.6) -> Dict[str, Any]:
    """Test alternative filter strategies to find better approaches."""
    
    logger.info("\n" + "="*80)
    logger.info("TESTING ALTERNATIVE FILTER STRATEGIES")
    logger.info("="*80)
    
    good_trades = trades_df[trades_df['potential_gains'] >= good_threshold].copy()
    bad_trades = trades_df[trades_df['potential_gains'] < good_threshold].copy()
    
    # Get all numeric filter columns
    filter_columns = [col for col in filter_values_df.columns 
                     if col not in ['buyin_id', 'minute'] and filter_values_df[col].dtype in ['float64', 'int64']]
    
    logger.info(f"\nTesting {len(filter_columns)} filter columns")
    
    strategies = {}
    
    # Strategy 1: Looser percentiles (5-95 instead of 10-90)
    logger.info("\n--- Strategy 1: Looser Percentiles (5-95) ---")
    strategy1_results = test_percentile_strategy(
        good_trades, bad_trades, filter_values_df, filter_columns,
        percentile_low=5, percentile_high=95, good_threshold=good_threshold
    )
    strategies['looser_percentiles'] = strategy1_results
    
    # Strategy 2: Even looser (1-99)
    logger.info("\n--- Strategy 2: Very Loose Percentiles (1-99) ---")
    strategy2_results = test_percentile_strategy(
        good_trades, bad_trades, filter_values_df, filter_columns,
        percentile_low=1, percentile_high=99, good_threshold=good_threshold
    )
    strategies['very_loose_percentiles'] = strategy2_results
    
    # Strategy 3: Focus on top performers only
    logger.info("\n--- Strategy 3: Top Performers Only (>= 0.6%) ---")
    top_performers = trades_df[trades_df['potential_gains'] >= 0.6].copy()
    if len(top_performers) > 5:
        strategy3_results = test_percentile_strategy(
            top_performers, bad_trades, filter_values_df, filter_columns,
            percentile_low=5, percentile_high=95, good_threshold=0.6
        )
        strategies['top_performers_only'] = strategy3_results
    
    # Strategy 4: Multi-minute approach (test if trade passes at ANY minute)
    logger.info("\n--- Strategy 4: Multi-Minute Approach (Pass if ANY minute matches) ---")
    strategy4_results = test_multi_minute_strategy(
        good_trades, bad_trades, filter_values_df, filter_columns, good_threshold
    )
    strategies['multi_minute'] = strategy4_results
    
    return strategies


def test_percentile_strategy(good_trades: pd.DataFrame, bad_trades: pd.DataFrame,
                             filter_values_df: pd.DataFrame, filter_columns: List[str],
                             percentile_low: float, percentile_high: float,
                             good_threshold: float = 0.6) -> Dict[str, Any]:
    """Test a filter strategy using specific percentiles."""
    
    best_filters = []
    
    # Test each minute
    for minute in range(15):
        minute_data = filter_values_df[filter_values_df['minute'] == minute].copy()
        
        if len(minute_data) < 20:
            continue
        
        # Merge with trade data
        minute_data = minute_data.merge(
            trades_df[['id', 'potential_gains']], 
            left_on='buyin_id', 
            right_on='id',
            how='left'
        )
        
        if len(minute_data) < 20:
            continue
        
        # Test each column
        for col in filter_columns:
            if col not in minute_data.columns:
                continue
            
            good_values = minute_data[minute_data['potential_gains'] >= good_threshold][col].dropna()
            
            if len(good_values) < 5:
                continue
            
            try:
                from_val = float(np.percentile(good_values, percentile_low))
                to_val = float(np.percentile(good_values, percentile_high))
                
                if from_val >= to_val:
                    continue
                
                # Test effectiveness
                passes_filter = (minute_data[col] >= from_val) & (minute_data[col] <= to_val)
                
                is_good = minute_data['potential_gains'] >= good_threshold
                is_bad = minute_data['potential_gains'] < good_threshold
                
                good_before = is_good.sum()
                bad_before = is_bad.sum()
                
                good_after = (is_good & passes_filter).sum()
                bad_after = (is_bad & passes_filter).sum()
                
                if good_before == 0 or bad_before == 0:
                    continue
                
                good_kept_pct = (good_after / good_before * 100)
                bad_removed_pct = ((bad_before - bad_after) / bad_before * 100)
                
                # Score: prioritize catching good trades
                score = good_kept_pct * (bad_removed_pct / 100)
                
                best_filters.append({
                    'column': col,
                    'minute': minute,
                    'from_val': from_val,
                    'to_val': to_val,
                    'good_kept_pct': good_kept_pct,
                    'bad_removed_pct': bad_removed_pct,
                    'score': score,
                    'good_passed': int(good_after),
                    'good_total': int(good_before),
                    'bad_passed': int(bad_after),
                    'bad_total': int(bad_before)
                })
                
            except Exception as e:
                continue
    
    # Sort by score
    best_filters.sort(key=lambda x: x['score'], reverse=True)
    
    if best_filters:
        top_10 = best_filters[:10]
        logger.info(f"\nTop 10 filters:")
        for i, f in enumerate(top_10, 1):
            logger.info(f"  {i}. {f['column']} (M{f['minute']}): "
                       f"Good {f['good_passed']}/{f['good_total']} ({f['good_kept_pct']:.1f}%), "
                       f"Bad removed {f['bad_removed_pct']:.1f}%, Score: {f['score']:.2f}")
    
    return {
        'all_filters': best_filters,
        'top_10': best_filters[:10] if best_filters else []
    }


def test_multi_minute_strategy(good_trades: pd.DataFrame, bad_trades: pd.DataFrame,
                               filter_values_df: pd.DataFrame, filter_columns: List[str],
                               good_threshold: float = 0.6) -> Dict[str, Any]:
    """Test if allowing trades to pass if ANY minute matches improves results."""
    
    results = []
    
    for col in filter_columns:
        # Get range from good trades across ALL minutes
        good_ids = good_trades['id'].tolist()
        good_filter_values = filter_values_df[filter_values_df['buyin_id'].isin(good_ids)][col].dropna()
        
        if len(good_filter_values) < 10:
            continue
        
        try:
            from_val = float(np.percentile(good_filter_values, 10))
            to_val = float(np.percentile(good_filter_values, 90))
            
            if from_val >= to_val:
                continue
            
            # Check if each trade passes at ANY minute
            good_passed = 0
            for trade_id in good_trades['id']:
                trade_values = filter_values_df[filter_values_df['buyin_id'] == trade_id][col].dropna()
                if any((trade_values >= from_val) & (trade_values <= to_val)):
                    good_passed += 1
            
            bad_passed = 0
            for trade_id in bad_trades['id']:
                trade_values = filter_values_df[filter_values_df['buyin_id'] == trade_id][col].dropna()
                if any((trade_values >= from_val) & (trade_values <= to_val)):
                    bad_passed += 1
            
            good_kept_pct = (good_passed / len(good_trades) * 100) if len(good_trades) > 0 else 0
            bad_removed_pct = ((len(bad_trades) - bad_passed) / len(bad_trades) * 100) if len(bad_trades) > 0 else 0
            
            score = good_kept_pct * (bad_removed_pct / 100)
            
            results.append({
                'column': col,
                'from_val': from_val,
                'to_val': to_val,
                'good_kept_pct': good_kept_pct,
                'bad_removed_pct': bad_removed_pct,
                'score': score,
                'good_passed': good_passed,
                'good_total': len(good_trades),
                'bad_passed': bad_passed,
                'bad_total': len(bad_trades)
            })
            
        except Exception:
            continue
    
    results.sort(key=lambda x: x['score'], reverse=True)
    
    if results:
        logger.info(f"\nTop 10 multi-minute filters:")
        for i, f in enumerate(results[:10], 1):
            logger.info(f"  {i}. {f['column']}: "
                       f"Good {f['good_passed']}/{f['good_total']} ({f['good_kept_pct']:.1f}%), "
                       f"Bad removed {f['bad_removed_pct']:.1f}%, Score: {f['score']:.2f}")
    
    return {
        'all_filters': results,
        'top_10': results[:10] if results else []
    }


def test_filter_combinations(trades_df: pd.DataFrame, filter_values_df: pd.DataFrame,
                              good_threshold: float = 0.6, max_combinations: int = 3,
                              target_good_trades: Tuple[int, int] = (2, 5)) -> Dict[str, Any]:
    """Test combinations of filters with AND logic - prioritize avoiding bad trades.
    
    Args:
        target_good_trades: Tuple of (min, max) good trades to catch per day
    """
    
    logger.info("\n" + "="*80)
    logger.info("TESTING FILTER COMBINATIONS (Conservative Strategy)")
    logger.info("="*80)
    logger.info(f"Strategy: Better to say NO than YES - prioritize avoiding bad trades")
    logger.info(f"Target: Catch {target_good_trades[0]}-{target_good_trades[1]} good trades per day")
    
    good_trades = trades_df[trades_df['potential_gains'] >= good_threshold].copy()
    bad_trades = trades_df[trades_df['potential_gains'] < good_threshold].copy()
    
    # Get all numeric filter columns
    filter_columns = [col for col in filter_values_df.columns 
                     if col not in ['buyin_id', 'minute'] and filter_values_df[col].dtype in ['float64', 'int64']]
    
    logger.info(f"\nTesting combinations of up to {max_combinations} filters from {len(filter_columns)} columns")
    
    # Step 1: Find top individual filters (using conservative scoring)
    logger.info("\n--- Step 1: Finding top individual filters ---")
    individual_filters = []
    
    for minute in range(15):
        minute_data = filter_values_df[filter_values_df['minute'] == minute].copy()
        
        if len(minute_data) < 20:
            continue
        
        minute_data = minute_data.merge(
            trades_df[['id', 'potential_gains']], 
            left_on='buyin_id', 
            right_on='id',
            how='left'
        )
        
        if len(minute_data) < 20:
            continue
        
        for col in filter_columns:
            if col not in minute_data.columns:
                continue
            
            good_values = minute_data[minute_data['potential_gains'] >= good_threshold][col].dropna()
            
            if len(good_values) < 5:
                continue
            
            try:
                # Use tighter percentiles for conservative approach (10-90)
                from_val = float(np.percentile(good_values, 10))
                to_val = float(np.percentile(good_values, 90))
                
                if from_val >= to_val:
                    continue
                
                passes_filter = (minute_data[col] >= from_val) & (minute_data[col] <= to_val)
                is_good = minute_data['potential_gains'] >= good_threshold
                is_bad = minute_data['potential_gains'] < good_threshold
                
                good_before = is_good.sum()
                bad_before = is_bad.sum()
                
                good_after = (is_good & passes_filter).sum()
                bad_after = (is_bad & passes_filter).sum()
                
                if good_before == 0 or bad_before == 0:
                    continue
                
                good_kept_pct = (good_after / good_before * 100)
                bad_removed_pct = ((bad_before - bad_after) / bad_before * 100)
                
                # Conservative score: heavily weight bad trade removal
                # Penalize bad trades getting through more than missing good trades
                precision = (good_after / (good_after + bad_after)) * 100 if (good_after + bad_after) > 0 else 0
                
                # Score prioritizes: 1) Bad trade removal, 2) Precision, 3) Good trade capture
                score = (bad_removed_pct * 0.5) + (precision * 0.3) + (good_kept_pct * 0.2)
                
                individual_filters.append({
                    'column': col,
                    'minute': minute,
                    'from_val': from_val,
                    'to_val': to_val,
                    'good_kept_pct': good_kept_pct,
                    'bad_removed_pct': bad_removed_pct,
                    'precision': precision,
                    'score': score,
                    'good_passed': int(good_after),
                    'good_total': int(good_before),
                    'bad_passed': int(bad_after),
                    'bad_total': int(bad_before)
                })
                
            except Exception:
                continue
    
    # Sort by conservative score
    individual_filters.sort(key=lambda x: x['score'], reverse=True)
    
    logger.info(f"\nTop 20 individual filters (conservative scoring):")
    for i, f in enumerate(individual_filters[:20], 1):
        logger.info(f"  {i}. {f['column']} (M{f['minute']}): "
                   f"Good {f['good_passed']}/{f['good_total']} ({f['good_kept_pct']:.1f}%), "
                   f"Bad removed {f['bad_removed_pct']:.1f}%, "
                   f"Precision {f['precision']:.1f}%, Score: {f['score']:.2f}")
    
    # Step 2: Test combinations (AND logic - all filters must pass)
    logger.info(f"\n--- Step 2: Testing combinations of {max_combinations} filters (AND logic) ---")
    
    # Use top 50 filters to get more diversity
    top_filters = individual_filters[:50]
    
    # Group by column to avoid redundant combinations
    filters_by_column = {}
    for f in top_filters:
        col = f['column']
        if col not in filters_by_column:
            filters_by_column[col] = []
        filters_by_column[col].append(f)
    
    # Select diverse filters - get top 2-3 filters per column for more variety
    diverse_filters = []
    for col, col_filters in list(filters_by_column.items())[:25]:  # More columns
        # Get top 2 filters for this column (different minutes)
        sorted_col_filters = sorted(col_filters, key=lambda x: x['score'], reverse=True)
        diverse_filters.extend(sorted_col_filters[:2])  # Top 2 per column
    
    logger.info(f"Selected {len(diverse_filters)} diverse filters from {len(filters_by_column)} columns")
    
    combinations = []
    min_good, max_good = target_good_trades
    
    # Test 2-filter combinations - test more combinations
    logger.info(f"Testing 2-filter combinations from {len(diverse_filters)} diverse filters...")
    tested = 0
    for i, filter1 in enumerate(diverse_filters[:20]):  # Test more filters
        for filter2 in diverse_filters[i+1:20]:
            if tested >= 150:  # Increased limit
                break
            combo_result = test_filter_combo(
                trades_df, filter_values_df, [filter1, filter2], good_threshold,
                min_bad_removed=60,  # Lower threshold to find more combinations
                target_good_range=(min_good, max_good)
            )
            if combo_result:
                combinations.append(combo_result)
            tested += 1
    
    # Test 3-filter combinations (if requested)
    if max_combinations >= 3:
        logger.info("Testing 3-filter combinations...")
        # Use top individual filters + best 2-filter combos
        top_2_combos = sorted(combinations, key=lambda x: x['score'], reverse=True)[:10]
        
        for combo in top_2_combos[:5]:  # Top 5 two-filter combos
            for filter3 in diverse_filters[:25]:
                if filter3['column'] not in [f['column'] for f in combo['filters']]:
                    combo_result = test_filter_combo(
                        trades_df, filter_values_df, combo['filters'] + [filter3], good_threshold,
                        min_bad_removed=60,
                        target_good_range=(min_good, max_good)
                    )
                    if combo_result:
                        combinations.append(combo_result)
                    if len(combinations) >= 50:  # More combinations
                        break
                if len(combinations) >= 50:
                    break
            if len(combinations) >= 50:
                break
    
    # Sort combinations by score
    combinations.sort(key=lambda x: x['score'], reverse=True)
    
    logger.info(f"\nTop 10 filter combinations:")
    for i, combo in enumerate(combinations[:10], 1):
        filter_names = [f"{f['column']} (M{f['minute']})" for f in combo['filters']]
        logger.info(f"\n  {i}. Combination of {len(combo['filters'])} filters:")
        logger.info(f"     Filters: {', '.join(filter_names)}")
        logger.info(f"     Good trades caught: {combo['good_passed']}/{combo['good_total']} ({combo['good_kept_pct']:.1f}%)")
        logger.info(f"     Bad trades filtered: {combo['bad_removed_pct']:.1f}% ({combo['bad_passed']} bad trades still pass)")
        logger.info(f"     Precision: {combo['precision']:.1f}%")
        logger.info(f"     Conservative Score: {combo['score']:.2f}")
    
    return {
        'individual_filters': individual_filters[:20],
        'combinations': combinations[:10]
    }


def test_filter_combo(trades_df: pd.DataFrame, filter_values_df: pd.DataFrame,
                      filters: List[Dict], good_threshold: float) -> Dict[str, Any]:
    """Test a combination of filters with AND logic - queries database directly for accuracy."""
    
    from core.database import get_postgres
    
    # Get all trade IDs
    all_trade_ids = trades_df['id'].tolist()
    good_ids_set = set(trades_df[trades_df['potential_gains'] >= good_threshold]['id'].tolist())
    bad_ids_set = set(trades_df[trades_df['potential_gains'] < good_threshold]['id'].tolist())
    
    good_total = len(good_ids_set)
    bad_total = len(bad_ids_set)
    
    if good_total == 0 or bad_total == 0:
        return None
    
    # Query database directly to get accurate filter values
    # For each filter, find which trades pass it
    passing_trades_by_filter = []
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for f in filters:
                    col = f['column']
                    minute = f['minute']
                    from_val = f['from_val']
                    to_val = f['to_val']
                    
                    # Query database directly for trades that pass this filter
                    cursor.execute("""
                        SELECT DISTINCT buyin_id
                        FROM trade_filter_values
                        WHERE buyin_id = ANY(%s)
                          AND minute = %s
                          AND filter_name = %s
                          AND filter_value IS NOT NULL
                          AND filter_value >= %s
                          AND filter_value <= %s
                    """, [all_trade_ids, minute, col, from_val, to_val])
                    
                    results = cursor.fetchall()
                    if not results:
                        return None  # No trades pass this filter
                    
                    # Get unique buyin_ids that pass this filter
                    passing_buyins = set(r['buyin_id'] for r in results)
                    passing_trades_by_filter.append(passing_buyins)
    except Exception as e:
        logger.error(f"Error querying filter combination: {e}")
        return None
    
    # Find trades that pass ALL filters (intersection)
    passing_trades = set.intersection(*passing_trades_by_filter) if passing_trades_by_filter else set()
    
    if len(passing_trades) == 0:
        return None
    
    # Count good vs bad trades that pass
    good_passed = len(passing_trades & good_ids_set)
    bad_passed = len(passing_trades & bad_ids_set)
    
    good_kept_pct = (good_passed / good_total * 100)
    bad_removed_pct = ((bad_total - bad_passed) / bad_total * 100)
    precision = (good_passed / (good_passed + bad_passed)) * 100 if (good_passed + bad_passed) > 0 else 0
    
    # Conservative score: prioritize avoiding bad trades
    score = (bad_removed_pct * 0.5) + (precision * 0.3) + (good_kept_pct * 0.2)
    
    # Only return if it filters out at least 70% of bad trades
    if bad_removed_pct < 70:
        return None
    
    return {
        'filters': filters,
        'good_passed': good_passed,
        'good_total': good_total,
        'bad_passed': bad_passed,
        'bad_total': bad_total,
        'good_kept_pct': good_kept_pct,
        'bad_removed_pct': bad_removed_pct,
        'precision': precision,
        'score': score
    }


# =============================================================================
# Main
# =============================================================================

def main():
    logger.info("="*80)
    logger.info("FILTER OPTIMIZATION ANALYSIS")
    logger.info("="*80)
    logger.info(f"Analyzing trades from: {datetime.now().date()}")
    
    # Load data
    global trades_df  # Make it global so strategies can access it
    trades_df = get_todays_trades()
    
    if len(trades_df) == 0:
        logger.error("No trades found for today. Exiting.")
        return
    
    trade_ids = trades_df['id'].tolist()
    filter_values_df = get_trade_filter_values(trade_ids)
    
    if len(filter_values_df) == 0:
        logger.error("No filter values found. Exiting.")
        return
    
    current_filters = get_current_filter_settings()
    
    # Analyze current performance
    missed_analysis = analyze_missed_opportunities(trades_df, filter_values_df, current_filters)
    
    # Test alternative strategies
    alternative_strategies = test_alternative_strategies(trades_df, filter_values_df)
    
    # Test filter combinations (conservative approach - target 2-5 good trades per day)
    combination_results = test_filter_combinations(
        trades_df, filter_values_df, 
        max_combinations=3,
        target_good_trades=(2, 5)
    )
    
    # Summary report
    logger.info("\n" + "="*80)
    logger.info("SUMMARY & RECOMMENDATIONS")
    logger.info("="*80)
    
    # Compare strategies
    logger.info("\nStrategy Comparison:")
    
    for strategy_name, strategy_data in alternative_strategies.items():
        if 'top_10' in strategy_data and strategy_data['top_10']:
            best = strategy_data['top_10'][0]
            logger.info(f"\n{strategy_name.replace('_', ' ').title()}:")
            logger.info(f"  Best filter: {best['column']}")
            if 'minute' in best:
                logger.info(f"  Minute: {best['minute']}")
            logger.info(f"  Good trades caught: {best['good_passed']}/{best['good_total']} ({best['good_kept_pct']:.1f}%)")
            logger.info(f"  Bad trades removed: {best['bad_removed_pct']:.1f}%")
            logger.info(f"  Score: {best['score']:.2f}")
    
    # Show best filter combinations (conservative approach)
    if combination_results and combination_results.get('combinations'):
        logger.info("\n" + "="*80)
        logger.info("BEST FILTER COMBINATIONS (Target: 2-5 Good Trades Per Day)")
        logger.info("="*80)
        
        # Sort by score, then by precision, then by bad removal
        sorted_combos = sorted(
            combination_results['combinations'],
            key=lambda x: (x['score'], x['precision'], x['bad_removed_pct']),
            reverse=True
        )
        
        best_combo = sorted_combos[0]
        filter_names = [f"{f['column']} (M{f['minute']})" for f in best_combo['filters']]
        
        logger.info(f"\n🏆 Top Combination ({len(best_combo['filters'])} filters):")
        logger.info(f"   Filters: {', '.join(filter_names)}")
        logger.info(f"   ✅ Good trades caught: {best_combo['good_passed']}/{best_combo['good_total']} ({best_combo['good_kept_pct']:.1f}%)")
        logger.info(f"   ❌ Bad trades filtered: {best_combo['bad_removed_pct']:.1f}% ({best_combo['bad_passed']} bad trades still pass)")
        logger.info(f"   🎯 Precision: {best_combo['precision']:.1f}% (of trades that pass, {best_combo['precision']:.1f}% are good)")
        logger.info(f"   📊 Conservative Score: {best_combo['score']:.2f}")
        
        logger.info(f"\n📊 Top 10 Combinations (Target: 2-5 good trades per day):")
        for i, combo in enumerate(sorted_combos[:10], 1):
            filter_names = [f"{f['column']} (M{f['minute']})" for f in combo['filters']]
            logger.info(f"\n   {i}. {len(combo['filters'])} filters: {', '.join(filter_names[:3])}{'...' if len(filter_names) > 3 else ''}")
            logger.info(f"      ✅ Good: {combo['good_passed']} trades ({combo['good_kept_pct']:.1f}%) | "
                       f"❌ Bad removed: {combo['bad_removed_pct']:.1f}% | "
                       f"🎯 Precision: {combo['precision']:.1f}% | "
                       f"Score: {combo['score']:.2f}")
        
        # Show SQL for top 3 combinations
        logger.info(f"\n" + "="*80)
        logger.info("SQL TO IMPLEMENT TOP COMBINATIONS:")
        logger.info("="*80)
        for i, combo in enumerate(sorted_combos[:3], 1):
            logger.info(f"\n-- Combination {i}: {combo['good_passed']} good trades, {combo['bad_removed_pct']:.1f}% bad removed, {combo['precision']:.1f}% precision")
            logger.info("DELETE FROM pattern_config_filters WHERE project_id = 5;")
            for j, f in enumerate(combo['filters'], 1):
                sql = f"""
INSERT INTO pattern_config_filters 
(project_id, name, section, minute, field_name, field_column, from_value, to_value, include_null, is_active)
VALUES 
(5, 'Auto: {f['column']}', 'auto', {f['minute']}, '{f['column']}', '{f['column']}', {f['from_val']:.6f}, {f['to_val']:.6f}, 0, 1);
"""
                logger.info(sql.strip())
    else:
        logger.warning("\n⚠️  No filter combinations found that meet criteria (2-5 good trades, 60%+ bad removed)")
        logger.info("   Try lowering the bad trade removal threshold or adjusting target range")
    
    logger.info("\n" + "="*80)
    logger.info("Analysis complete!")
    logger.info("="*80)


if __name__ == "__main__":
    main()

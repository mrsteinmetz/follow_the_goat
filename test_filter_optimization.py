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
from typing import Dict, List, Any, Tuple
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
    
    logger.info(f"Loaded {len(df)} filter value rows")
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
                                 current_filters: List[Dict], good_threshold: float = 0.3) -> Dict[str, Any]:
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
                                good_threshold: float = 0.3) -> Dict[str, Any]:
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
    logger.info("\n--- Strategy 3: Top Performers Only (>= 0.5%) ---")
    top_performers = trades_df[trades_df['potential_gains'] >= 0.5].copy()
    if len(top_performers) > 5:
        strategy3_results = test_percentile_strategy(
            top_performers, bad_trades, filter_values_df, filter_columns,
            percentile_low=5, percentile_high=95, good_threshold=0.5
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
                             good_threshold: float = 0.3) -> Dict[str, Any]:
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
                               good_threshold: float = 0.3) -> Dict[str, Any]:
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
    
    logger.info("\n" + "="*80)
    logger.info("Analysis complete!")
    logger.info("="*80)


if __name__ == "__main__":
    main()

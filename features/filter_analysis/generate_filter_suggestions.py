#!/usr/bin/env python3
"""
Generate Filter Suggestions for PostgreSQL System
==================================================
Analyzes trade data from follow_the_goat_buyins and trade_filter_values
to generate optimal filter suggestions.

This script:
1. Loads buyins with resolved outcomes (our_profit_loss)
2. Gets filter values from trade_filter_values
3. Analyzes which filter ranges work best
4. Saves results to filter_reference_suggestions table

Usage:
    python features/filter_analysis/generate_filter_suggestions.py --hours 24
    python features/filter_analysis/generate_filter_suggestions.py --hours 12 --all-minutes
"""

import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from core.database import get_postgres, postgres_execute

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Gain threshold for "good" trades (in percentage, not decimal)
GOOD_TRADE_THRESHOLD = 0.3  # >= 0.3% profit

# Minimum requirements for a filter to be saved
MIN_GOOD_TRADES_KEPT_PCT = 50.0  # Must keep at least 50% of good trades
MIN_BAD_TRADES_REMOVED_PCT = 10.0  # Must remove at least 10% of bad trades


def load_buyin_data(hours: int) -> pd.DataFrame:
    """Load buyins with resolved outcomes from the last N hours."""
    logger.info(f"Loading buyins from last {hours} hours...")
    
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    id as buyin_id,
                    potential_gains as our_profit_loss,
                    our_status,
                    followed_at
                FROM follow_the_goat_buyins
                WHERE followed_at >= NOW() - INTERVAL '%s hours'
                  AND potential_gains IS NOT NULL
                ORDER BY followed_at DESC
            """, [hours])
            
            results = cur.fetchall()
    
    df = pd.DataFrame(results)
    logger.info(f"Loaded {len(df)} buyins with resolved outcomes (potential_gains)")
    return df


def load_filter_values(buyin_ids: List[int], minute: Optional[int] = None) -> pd.DataFrame:
    """Load filter values for the given buyins."""
    if not buyin_ids:
        return pd.DataFrame()
    
    logger.info(f"Loading filter values for {len(buyin_ids)} buyins...")
    
    with get_postgres() as conn:
        with conn.cursor() as cur:
            if minute is not None:
                placeholders = ','.join(['%s'] * len(buyin_ids))
                cur.execute(f"""
                    SELECT 
                        buyin_id,
                        minute,
                        filter_name,
                        filter_value,
                        section
                    FROM trade_filter_values
                    WHERE buyin_id IN ({placeholders})
                      AND minute = %s
                """, buyin_ids + [minute])
            else:
                placeholders = ','.join(['%s'] * len(buyin_ids))
                cur.execute(f"""
                    SELECT 
                        buyin_id,
                        minute,
                        filter_name,
                        filter_value,
                        section
                    FROM trade_filter_values
                    WHERE buyin_id IN ({placeholders})
                """, buyin_ids)
            
            results = cur.fetchall()
    
    df = pd.DataFrame(results)
    logger.info(f"Loaded {len(df)} filter values")
    return df


def pivot_filter_data(buyins_df: pd.DataFrame, filters_df: pd.DataFrame, minute: Optional[int] = None) -> pd.DataFrame:
    """
    Pivot filter data so each row is a buyin with filter values as columns.
    
    Args:
        buyins_df: DataFrame with buyin data (id, our_profit_loss, etc.)
        filters_df: DataFrame with filter values (buyin_id, minute, filter_name, filter_value)
        minute: If specified, only include this minute's data
    """
    if minute is not None:
        filters_df = filters_df[filters_df['minute'] == minute].copy()
    
    # Pivot so each filter_name becomes a column
    pivoted = filters_df.pivot_table(
        index='buyin_id',
        columns='filter_name',
        values='filter_value',
        aggfunc='first'  # Take first value if duplicates
    ).reset_index()
    
    # Merge with buyin data
    merged = buyins_df.merge(pivoted, on='buyin_id', how='left')
    
    return merged


def test_filter_effectiveness(trades_df: pd.DataFrame, 
                              column_name: str,
                              from_val: Optional[float], 
                              to_val: Optional[float]) -> Optional[Dict[str, Any]]:
    """
    Test how effective a filter would be.
    
    Returns metrics about good/bad trade counts before and after filter.
    """
    if column_name not in trades_df.columns:
        return None
    
    values = trades_df[column_name]
    profit_loss = trades_df['our_profit_loss']
    
    # Classify all trades
    is_good = profit_loss >= GOOD_TRADE_THRESHOLD
    is_bad = profit_loss < GOOD_TRADE_THRESHOLD
    
    # Bad trade breakdown
    is_negative = profit_loss < 0
    is_0_to_01 = (profit_loss >= 0) & (profit_loss < 0.1)
    is_01_to_02 = (profit_loss >= 0.1) & (profit_loss < 0.2)
    is_02_to_03 = (profit_loss >= 0.2) & (profit_loss < 0.3)
    
    # Before filter counts
    good_before = int(is_good.sum())
    bad_before = int(is_bad.sum())
    total = good_before + bad_before
    
    if total == 0:
        return None
    
    # Apply filter (trade passes if value is within range)
    passes_filter = pd.Series([False] * len(trades_df))
    
    if from_val is not None and to_val is not None:
        passes_filter = (values >= from_val) & (values <= to_val) & values.notna()
    elif from_val is not None:
        passes_filter = (values >= from_val) & values.notna()
    elif to_val is not None:
        passes_filter = (values <= to_val) & values.notna()
    
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


def find_optimal_threshold(trades_df: pd.DataFrame, 
                           column_name: str) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    """
    Find optimal from/to values that maximize bad trade removal while keeping good trades.
    """
    if column_name not in trades_df.columns:
        return None
    
    values = trades_df[column_name].dropna()
    is_good = trades_df['our_profit_loss'] >= GOOD_TRADE_THRESHOLD
    good_values = trades_df.loc[is_good, column_name].dropna()
    
    if len(good_values) < 10 or len(values) < 20:
        return None
    
    best_score = -1
    best_result = None
    
    # Try different percentile combinations
    percentile_pairs = [
        (10, 90),  # Original
        (5, 95),   # Wider
        (15, 85),  # Tighter
        (20, 80),  # Even tighter
        (25, 75),  # Very tight
    ]
    
    for p_low, p_high in percentile_pairs:
        try:
            from_val = float(np.percentile(good_values, p_low))
            to_val = float(np.percentile(good_values, p_high))
            
            if from_val >= to_val:
                continue
            
            # Test this filter
            metrics = test_filter_effectiveness(trades_df, column_name, from_val, to_val)
            
            if metrics is None:
                continue
            
            good_kept = metrics['good_trades_kept_pct']
            bad_removed = metrics['bad_trades_removed_pct']
            
            # Skip if doesn't meet minimum requirements
            if good_kept < MIN_GOOD_TRADES_KEPT_PCT:
                continue
            if bad_removed < MIN_BAD_TRADES_REMOVED_PCT:
                continue
            
            # Score: prioritize bad removal while keeping good trades
            score = bad_removed * (good_kept / 100)
            
            if score > best_score:
                best_score = score
                best_result = (from_val, to_val, metrics)
        except Exception:
            continue
    
    return best_result


def analyze_filter(trades_df: pd.DataFrame, 
                   filter_name: str,
                   section: str,
                   minute: int) -> Optional[Dict[str, Any]]:
    """
    Analyze a single filter and generate suggestion.
    """
    if filter_name not in trades_df.columns:
        return None
    
    # Check for too many NULLs
    null_pct = trades_df[filter_name].isna().sum() / len(trades_df) * 100
    if null_pct > 90:
        logger.debug(f"Filter {filter_name} has too many NULLs ({null_pct:.1f}%)")
        return None
    
    # Find optimal threshold
    result = find_optimal_threshold(trades_df, filter_name)
    
    if result is None:
        return None
    
    from_val, to_val, metrics = result
    
    return {
        'column_name': filter_name,
        'section': section,
        'from_value': round(from_val, 6),
        'to_value': round(to_val, 6),
        'minute_analyzed': minute,
        **metrics
    }


def save_suggestion(suggestion: Dict[str, Any], hours: int) -> int:
    """Save a filter suggestion to the database."""
    postgres_execute("""
        INSERT INTO filter_reference_suggestions (
            column_name, section, from_value, to_value,
            total_trades, good_trades_before, bad_trades_before,
            good_trades_after, bad_trades_after,
            good_trades_kept_pct, bad_trades_removed_pct,
            bad_negative_count, bad_0_to_01_count, bad_01_to_02_count, bad_02_to_03_count,
            analysis_hours, minute_analyzed
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
    """, [
        suggestion['column_name'],
        suggestion['section'],
        suggestion['from_value'],
        suggestion['to_value'],
        suggestion['total_trades'],
        suggestion['good_trades_before'],
        suggestion['bad_trades_before'],
        suggestion['good_trades_after'],
        suggestion['bad_trades_after'],
        suggestion['good_trades_kept_pct'],
        suggestion['bad_trades_removed_pct'],
        suggestion['bad_negative_count'],
        suggestion['bad_0_to_01_count'],
        suggestion['bad_01_to_02_count'],
        suggestion['bad_02_to_03_count'],
        hours,
        suggestion['minute_analyzed']
    ])
    
    return 1


def main():
    parser = argparse.ArgumentParser(description="Generate filter suggestions from trade data")
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to analyze')
    parser.add_argument('--minute', type=int, default=0, choices=range(15), help='Minute to analyze (0-14)')
    parser.add_argument('--all-minutes', action='store_true', help='Analyze all 15 minutes')
    parser.add_argument('--clear', action='store_true', help='Clear existing suggestions first')
    args = parser.parse_args()
    
    try:
        # Clear old suggestions if requested
        if args.clear:
            deleted = postgres_execute("DELETE FROM filter_reference_suggestions", [])
            logger.info(f"Cleared {deleted} existing suggestions")
        
        # Load buyin data
        buyins_df = load_buyin_data(args.hours)
        
        if len(buyins_df) == 0:
            logger.error("No buyins found with resolved outcomes")
            return
        
        buyin_ids = buyins_df['buyin_id'].tolist()
        
        # Load all filter values
        filters_df = load_filter_values(buyin_ids, minute=None if args.all_minutes else args.minute)
        
        if len(filters_df) == 0:
            logger.error("No filter values found")
            return
        
        # Get unique filter names and sections
        filter_info = filters_df[['filter_name', 'section']].drop_duplicates()
        
        # Summary
        good_count = (buyins_df['our_profit_loss'] >= GOOD_TRADE_THRESHOLD).sum()
        bad_count = (buyins_df['our_profit_loss'] < GOOD_TRADE_THRESHOLD).sum()
        print(f"\n{'='*60}")
        print("TRADE DATA SUMMARY")
        print('='*60)
        print(f"  Total buyins: {len(buyins_df):,}")
        print(f"  Good trades (>= {GOOD_TRADE_THRESHOLD}%): {good_count:,} ({good_count/len(buyins_df)*100:.1f}%)")
        print(f"  Bad trades (< {GOOD_TRADE_THRESHOLD}%): {bad_count:,} ({bad_count/len(buyins_df)*100:.1f}%)")
        print(f"  Time range: Last {args.hours} hours")
        print(f"  Unique filters: {len(filter_info)}")
        
        # Process filters
        saved_count = 0
        processed_count = 0
        
        minutes_to_analyze = range(15) if args.all_minutes else [args.minute]
        
        for minute in minutes_to_analyze:
            print(f"\n{'='*60}")
            print(f"ANALYZING MINUTE {minute}")
            print('='*60)
            
            # Pivot data for this minute
            trades_df = pivot_filter_data(buyins_df, filters_df, minute=minute)
            
            if len(trades_df) == 0:
                logger.warning(f"No data for minute {minute}")
                continue
            
            for _, row in filter_info.iterrows():
                filter_name = row['filter_name']
                section = row['section'] or 'unknown'
                
                processed_count += 1
                
                suggestion = analyze_filter(trades_df, filter_name, section, minute)
                
                if suggestion:
                    save_suggestion(suggestion, args.hours)
                    saved_count += 1
                    print(f"  ✓ {filter_name} [M{minute}]: "
                          f"Bad removed: {suggestion['bad_trades_removed_pct']:.1f}%, "
                          f"Good kept: {suggestion['good_trades_kept_pct']:.1f}%")
        
        # Final summary
        print(f"\n{'='*60}")
        print("GENERATION COMPLETE")
        print('='*60)
        print(f"  Filters processed: {processed_count}")
        print(f"  Suggestions saved: {saved_count}")
        print('='*60)
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

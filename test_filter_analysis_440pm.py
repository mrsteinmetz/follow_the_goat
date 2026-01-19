#!/usr/bin/env python3
"""
Detailed analysis of why trades around 4:40 PM were not triggered.

This script provides a comprehensive breakdown of:
1. What the current filters are
2. Which filter(s) are blocking good trades
3. How many good trades each filter is rejecting
4. Recommendations for filter adjustments
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def get_active_filters(project_id: int):
    """Get active filters for a project."""
    query = """
        SELECT 
            id,
            name,
            section,
            minute,
            field_name,
            field_column,
            from_value,
            to_value,
            include_null,
            is_active
        FROM pattern_config_filters
        WHERE project_id = %s
          AND is_active = 1
        ORDER BY minute, field_name
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [project_id])
            return cursor.fetchall()


def get_trades_around_time(target_hour: int, target_minute: int, window_minutes: int = 10):
    """Get trades around a specific time today."""
    today = datetime.now().date()
    start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute - window_minutes)
    end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute + window_minutes)
    
    query = """
        SELECT 
            b.id,
            b.play_id,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            p.project_ids
        FROM follow_the_goat_buyins b
        LEFT JOIN follow_the_goat_plays p ON p.id = b.play_id
        WHERE b.followed_at >= %s 
          AND b.followed_at <= %s
          AND b.potential_gains IS NOT NULL
        ORDER BY b.potential_gains DESC
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [start_time, end_time])
            return cursor.fetchall()


def get_trade_filter_values(buyin_id: int, minute: int = 0):
    """Get all filter values for a specific trade at a specific minute."""
    query = """
        SELECT 
            filter_name,
            filter_value,
            minute
        FROM trade_filter_values
        WHERE buyin_id = %s
          AND minute = %s
        ORDER BY filter_name
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [buyin_id, minute])
            return cursor.fetchall()


def check_filter_match(filter_value, from_value, to_value, include_null):
    """Check if a filter value matches the filter criteria."""
    if filter_value is None:
        return bool(include_null)
    return from_value <= filter_value <= to_value


def main():
    print("="*100)
    print("DETAILED FILTER ANALYSIS - 4:40 PM MISSED OPPORTUNITIES")
    print("="*100)
    
    # Get AutoFilters project
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name FROM pattern_config_projects WHERE name = 'AutoFilters'")
            project = cursor.fetchone()
    
    if not project:
        print("ERROR: AutoFilters project not found!")
        return
    
    project_id = project['id']
    
    # Get active filters
    filters = get_active_filters(project_id)
    
    print(f"\nCURRENT ACTIVE FILTERS ({len(filters)} total):")
    print("-" * 100)
    for f in filters:
        print(f"  Filter: {f['field_column']}")
        print(f"    Range: [{f['from_value']:.6f} - {f['to_value']:.6f}]")
        print(f"    Minute: {f['minute']}")
        print()
    
    # Get trades around 4:40 PM
    trades = get_trades_around_time(16, 40, window_minutes=10)
    good_trades = [t for t in trades if t['potential_gains'] >= 0.5]
    
    print(f"\nTRADES ANALYSIS:")
    print("-" * 100)
    print(f"  Total trades in window (4:30 PM - 4:50 PM): {len(trades)}")
    print(f"  Trades with >= 0.5% potential gains: {len(good_trades)}")
    print(f"  Trades with < 0.5% potential gains: {len(trades) - len(good_trades)}")
    
    # Analyze which filters are blocking trades
    filter_rejection_stats = {}
    for f in filters:
        filter_rejection_stats[f['field_column']] = {
            'filter': f,
            'good_trades_rejected': 0,
            'bad_trades_rejected': 0,
            'good_trade_ids': [],
            'bad_trade_ids': [],
            'values_seen': []
        }
    
    # Track overall pass/fail for each trade
    trades_analysis = []
    
    for trade in good_trades:
        trade_values = get_trade_filter_values(trade['id'], minute=0)
        value_dict = {tv['filter_name']: tv['filter_value'] for tv in trade_values}
        
        passed_count = 0
        failed_filters = []
        
        for f in filters:
            field_column = f['field_column']
            filter_value = value_dict.get(field_column)
            
            passed = check_filter_match(
                filter_value,
                f['from_value'],
                f['to_value'],
                f['include_null']
            )
            
            # Record value seen
            filter_rejection_stats[field_column]['values_seen'].append(filter_value)
            
            if passed:
                passed_count += 1
            else:
                failed_filters.append(field_column)
                filter_rejection_stats[field_column]['good_trades_rejected'] += 1
                filter_rejection_stats[field_column]['good_trade_ids'].append(trade['id'])
        
        trades_analysis.append({
            'id': trade['id'],
            'potential_gains': trade['potential_gains'],
            'passed': passed_count,
            'total': len(filters),
            'failed_filters': failed_filters,
            'passed_all': passed_count == len(filters)
        })
    
    # Count how many good trades would have passed if filters were removed
    print(f"\n\nFILTER REJECTION ANALYSIS:")
    print("="*100)
    print(f"{'Filter Name':<30} {'Good Blocked':<15} {'% of Good':<15} {'Value Range':<40}")
    print("-" * 100)
    
    for field_column, stats in sorted(filter_rejection_stats.items(), 
                                      key=lambda x: x[1]['good_trades_rejected'], 
                                      reverse=True):
        good_blocked = stats['good_trades_rejected']
        pct_good = (good_blocked / len(good_trades) * 100) if len(good_trades) > 0 else 0
        
        # Calculate actual value range seen
        values = [v for v in stats['values_seen'] if v is not None]
        if values:
            min_val = min(values)
            max_val = max(values)
            value_range = f"[{min_val:.6f} - {max_val:.6f}]"
        else:
            value_range = "No values"
        
        print(f"{field_column:<30} {good_blocked:<15} {pct_good:<14.1f}% {value_range:<40}")
    
    print("\n\nFILTER CONFIGURATION vs ACTUAL DATA:")
    print("="*100)
    for field_column, stats in sorted(filter_rejection_stats.items(), 
                                      key=lambda x: x[1]['good_trades_rejected'], 
                                      reverse=True):
        if stats['good_trades_rejected'] == 0:
            continue
            
        f = stats['filter']
        values = [v for v in stats['values_seen'] if v is not None]
        
        print(f"\n{field_column}:")
        print(f"  Filter Range: [{float(f['from_value']):.6f} - {float(f['to_value']):.6f}]")
        if values:
            min_val = min(values)
            max_val = max(values)
            print(f"  Actual Range: [{min_val:.6f} - {max_val:.6f}]")
            
            # Show how much to adjust
            from_val = float(f['from_value'])
            to_val = float(f['to_value'])
            
            if min_val < from_val:
                print(f"  ⚠ Minimum value ({min_val:.6f}) is BELOW filter range")
                print(f"     Need to LOWER from_value by {from_val - min_val:.6f} (currently {from_val:.6f})")
            if max_val > to_val:
                print(f"  ⚠ Maximum value ({max_val:.6f}) is ABOVE filter range")
                print(f"     Need to RAISE to_value by {max_val - to_val:.6f} (currently {to_val:.6f})")
                
        print(f"  Blocking: {stats['good_trades_rejected']} good trades ({stats['good_trades_rejected']/len(good_trades)*100:.1f}%)")
    
    # Summary of trade outcomes
    passed_all = sum(1 for t in trades_analysis if t['passed_all'])
    
    print("\n\nTRADE OUTCOMES:")
    print("="*100)
    print(f"  Good trades that PASSED all filters: {passed_all}/{len(good_trades)} ({passed_all/len(good_trades)*100:.1f}%)")
    print(f"  Good trades that FAILED at least 1 filter: {len(good_trades)-passed_all}/{len(good_trades)} ({(len(good_trades)-passed_all)/len(good_trades)*100:.1f}%)")
    
    # Show distribution of filter failures
    failure_counts = {}
    for t in trades_analysis:
        failed_count = len(t['failed_filters'])
        failure_counts[failed_count] = failure_counts.get(failed_count, 0) + 1
    
    print(f"\n  Distribution of failures:")
    for failed_count in sorted(failure_counts.keys()):
        count = failure_counts[failed_count]
        pct = count / len(good_trades) * 100
        print(f"    Failed {failed_count} filter(s): {count} trades ({pct:.1f}%)")
    
    # Show which specific filter combinations are most problematic
    print(f"\n\nMOST COMMON FILTER FAILURE PATTERNS:")
    print("="*100)
    failure_patterns = {}
    for t in trades_analysis:
        if not t['failed_filters']:
            continue
        pattern = tuple(sorted(t['failed_filters']))
        failure_patterns[pattern] = failure_patterns.get(pattern, 0) + 1
    
    for pattern, count in sorted(failure_patterns.items(), key=lambda x: x[1], reverse=True)[:5]:
        pct = count / len(good_trades) * 100
        print(f"  {count} trades ({pct:.1f}%) failed on: {', '.join(pattern)}")
    
    print("\n" + "="*100)
    print("KEY FINDINGS:")
    print("="*100)
    
    # Find the most problematic filter
    most_problematic = max(filter_rejection_stats.items(), key=lambda x: x[1]['good_trades_rejected'])
    print(f"\n1. MOST PROBLEMATIC FILTER: {most_problematic[0]}")
    print(f"   - Blocking {most_problematic[1]['good_trades_rejected']} out of {len(good_trades)} good trades ({most_problematic[1]['good_trades_rejected']/len(good_trades)*100:.1f}%)")
    
    # Show how many would pass if we removed top blocker
    would_pass_without_top = sum(1 for t in trades_analysis 
                                  if most_problematic[0] not in t['failed_filters'] or len(t['failed_filters']) == 1)
    print(f"\n2. If we removed '{most_problematic[0]}' filter:")
    print(f"   - {would_pass_without_top} out of {len(good_trades)} good trades would pass ({would_pass_without_top/len(good_trades)*100:.1f}%)")
    
    # Show single-filter failures
    single_filter_failures = [t for t in trades_analysis if len(t['failed_filters']) == 1]
    if single_filter_failures:
        print(f"\n3. TRADES FAILING ONLY 1 FILTER: {len(single_filter_failures)} trades")
        single_filter_dist = {}
        for t in single_filter_failures:
            f = t['failed_filters'][0]
            single_filter_dist[f] = single_filter_dist.get(f, 0) + 1
        
        for f, count in sorted(single_filter_dist.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {f}: {count} trades")
    
    print("\n" + "="*100)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Test script to investigate why a 0.5%+ opportunity around 4:40 PM was not triggered.

This script:
1. Finds trades around 4:40 PM today
2. Identifies trades with potential_gains >= 0.5%
3. Checks what filters are currently active
4. Tests whether those trades would pass the current filters
5. Shows why they were not triggered
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def get_trades_around_time(target_hour: int, target_minute: int, window_minutes: int = 10):
    """Get trades around a specific time today."""
    # Today's date
    today = datetime.now().date()
    
    # Target time window
    start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute - window_minutes)
    end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=target_hour, minutes=target_minute + window_minutes)
    
    query = """
        SELECT 
            b.id,
            b.play_id,
            b.followed_at,
            b.potential_gains,
            b.our_status,
            b.wallet_address,
            b.our_entry_price,
            b.higest_price_reached,
            p.name as play_name,
            p.project_ids,
            p.pattern_update_by_ai
        FROM follow_the_goat_buyins b
        LEFT JOIN follow_the_goat_plays p ON p.id = b.play_id
        WHERE b.followed_at >= %s 
          AND b.followed_at <= %s
          AND b.potential_gains IS NOT NULL
        ORDER BY b.potential_gains DESC, b.followed_at DESC
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [start_time, end_time])
            return cursor.fetchall()


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


def analyze_trade_filters(trade, filters):
    """Analyze why a trade didn't pass the filters."""
    buyin_id = trade['id']
    
    print(f"\n{'='*80}")
    print(f"ANALYZING TRADE #{buyin_id}")
    print(f"{'='*80}")
    print(f"  Followed at: {trade['followed_at']}")
    print(f"  Potential gains: {trade['potential_gains']:.4f}%")
    print(f"  Status: {trade['our_status']}")
    print(f"  Wallet: {trade['wallet_address']}")
    print(f"  Entry price: {trade['our_entry_price']}")
    print(f"  Highest price: {trade['higest_price_reached']}")
    print(f"  Play: {trade['play_name']} (ID: {trade['play_id']})")
    print(f"  Project IDs: {trade['project_ids']}")
    print(f"  Pattern Update By AI: {trade['pattern_update_by_ai']}")
    
    # Group filters by minute
    filters_by_minute = {}
    for f in filters:
        minute = f['minute']
        if minute not in filters_by_minute:
            filters_by_minute[minute] = []
        filters_by_minute[minute].append(f)
    
    print(f"\n  Active filters across {len(filters_by_minute)} minutes:")
    for minute, minute_filters in sorted(filters_by_minute.items()):
        print(f"    Minute {minute}: {len(minute_filters)} filters")
    
    # Check each minute
    for minute in sorted(filters_by_minute.keys()):
        minute_filters = filters_by_minute[minute]
        
        print(f"\n  {'='*76}")
        print(f"  MINUTE {minute} - {len(minute_filters)} filters")
        print(f"  {'='*76}")
        
        # Get trade filter values for this minute
        trade_values = get_trade_filter_values(buyin_id, minute)
        
        if not trade_values:
            print(f"    ⚠ No filter values found for this trade at minute {minute}")
            continue
        
        # Create lookup dict
        value_dict = {tv['filter_name']: tv['filter_value'] for tv in trade_values}
        
        # Check each filter
        passed_count = 0
        failed_filters = []
        
        for f in minute_filters:
            field_column = f['field_column']
            filter_value = value_dict.get(field_column)
            
            passed = check_filter_match(
                filter_value,
                f['from_value'],
                f['to_value'],
                f['include_null']
            )
            
            if passed:
                passed_count += 1
            else:
                failed_filters.append({
                    'name': f['name'],
                    'field': field_column,
                    'value': filter_value,
                    'range': f"[{f['from_value']:.6f} - {f['to_value']:.6f}]",
                    'include_null': f['include_null']
                })
        
        print(f"    Passed: {passed_count}/{len(minute_filters)} filters")
        
        if failed_filters:
            print(f"\n    FAILED FILTERS ({len(failed_filters)}):")
            for ff in failed_filters:
                value_str = f"{ff['value']:.6f}" if ff['value'] is not None else "NULL"
                print(f"      ❌ {ff['name']}")
                print(f"         Field: {ff['field']}")
                print(f"         Value: {value_str}")
                print(f"         Required: {ff['range']}")
                if ff['value'] is None:
                    print(f"         Problem: Value is NULL (include_null={ff['include_null']})")
                elif ff['value'] < float(ff['range'].split('-')[0].strip('[').strip()):
                    print(f"         Problem: Value TOO LOW")
                else:
                    print(f"         Problem: Value TOO HIGH")
    
    print(f"\n{'='*80}\n")


def main():
    print("="*80)
    print("INVESTIGATING MISSED OPPORTUNITY AROUND 4:40 PM")
    print("="*80)
    print(f"Current time: {datetime.now()}")
    
    # Get trades around 4:40 PM (16:40)
    target_hour = 16
    target_minute = 40
    
    print(f"\nSearching for trades around {target_hour}:{target_minute:02d} (±10 minutes)...")
    trades = get_trades_around_time(target_hour, target_minute, window_minutes=10)
    
    if not trades:
        print("  ⚠ No trades found in this time window")
        return
    
    print(f"  Found {len(trades)} trades")
    
    # Filter to trades with >= 0.5% potential gains
    good_trades = [t for t in trades if t['potential_gains'] >= 0.5]
    
    print(f"\n  Trades with potential_gains >= 0.5%: {len(good_trades)}")
    
    if not good_trades:
        print("\n  ⚠ No trades with >= 0.5% potential gains found")
        print("\n  All trades in window:")
        for t in trades:
            print(f"    ID {t['id']}: {t['potential_gains']:.4f}% at {t['followed_at']}")
        return
    
    # Show summary
    print("\n  OPPORTUNITIES FOUND:")
    for t in good_trades:
        print(f"    ID {t['id']}: {t['potential_gains']:.4f}% at {t['followed_at']} "
              f"(Play: {t['play_name']})")
    
    # Get AutoFilters project
    print("\n  Getting AutoFilters project...")
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM pattern_config_projects WHERE name = 'AutoFilters'")
            project = cursor.fetchone()
    
    if not project:
        print("    ⚠ AutoFilters project not found")
        return
    
    project_id = project['id']
    print(f"    AutoFilters project ID: {project_id}")
    
    # Get active filters
    print("\n  Loading active filters...")
    filters = get_active_filters(project_id)
    print(f"    Found {len(filters)} active filters")
    
    if not filters:
        print("    ⚠ No active filters found - this is why no trades were triggered!")
        return
    
    # Analyze each good trade
    print("\n" + "="*80)
    print("DETAILED ANALYSIS OF EACH MISSED OPPORTUNITY")
    print("="*80)
    
    for trade in good_trades:
        analyze_trade_filters(trade, filters)
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"  Total trades in window: {len(trades)}")
    print(f"  Trades with >= 0.5% gains: {len(good_trades)}")
    print(f"  Active filters in AutoFilters: {len(filters)}")
    print("\n  CONCLUSION:")
    print("  The above analysis shows which specific filter criteria were not met")
    print("  for each missed opportunity. Look for the ❌ FAILED FILTERS sections.")
    print("="*80)


if __name__ == "__main__":
    main()

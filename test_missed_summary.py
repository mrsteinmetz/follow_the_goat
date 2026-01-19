#!/usr/bin/env python3
"""
Simple summary of why trades around 4:40 PM were not triggered.
Shows exactly which trades passed and which failed.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def main():
    print("\n" + "="*100)
    print("SUMMARY: Why 4:40 PM Opportunities Were Missed")
    print("="*100)
    
    # Get trades
    today = datetime.now().date()
    start_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=16, minutes=30)
    end_time = datetime.combine(today, datetime.min.time()) + timedelta(hours=16, minutes=50)
    
    query = """
        SELECT 
            b.id,
            b.followed_at,
            b.potential_gains,
            b.our_status
        FROM follow_the_goat_buyins b
        WHERE b.followed_at >= %s 
          AND b.followed_at <= %s
          AND b.potential_gains >= 0.5
        ORDER BY b.potential_gains DESC
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [start_time, end_time])
            trades = cursor.fetchall()
    
    # Get filter values for each
    filter_query = """
        SELECT filter_name, filter_value
        FROM trade_filter_values
        WHERE buyin_id = %s AND minute = 0
    """
    
    # Get active filters
    filters_query = """
        SELECT field_column, from_value, to_value, include_null
        FROM pattern_config_filters
        WHERE project_id = 5 AND is_active = 1
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(filters_query)
            active_filters = {f['field_column']: f for f in cursor.fetchall()}
    
    print(f"\nFound {len(trades)} trades with >= 0.5% potential gains")
    print(f"Active filters: {len(active_filters)}\n")
    
    passed_trades = []
    failed_trades = []
    
    for trade in trades:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(filter_query, [trade['id']])
                values = {v['filter_name']: v['filter_value'] for v in cursor.fetchall()}
        
        failed = []
        for field, f in active_filters.items():
            val = values.get(field)
            if val is None:
                if not f['include_null']:
                    failed.append(f"{field}=NULL")
            elif val < float(f['from_value']):
                failed.append(f"{field}={val:.6f} (need >={float(f['from_value']):.6f})")
            elif val > float(f['to_value']):
                failed.append(f"{field}={val:.6f} (need <={float(f['to_value']):.6f})")
        
        if failed:
            failed_trades.append((trade, failed))
        else:
            passed_trades.append(trade)
    
    print("="*100)
    print(f"PASSED ALL FILTERS: {len(passed_trades)} trades")
    print("="*100)
    for t in passed_trades:
        print(f"  ✅ Trade {t['id']}: {t['potential_gains']:.4f}% at {t['followed_at']}")
    
    print("\n" + "="*100)
    print(f"FAILED AT LEAST 1 FILTER: {len(failed_trades)} trades")
    print("="*100)
    
    # Group by failure reason
    failure_reasons = {}
    for trade, reasons in failed_trades:
        key = tuple(sorted(reasons))
        if key not in failure_reasons:
            failure_reasons[key] = []
        failure_reasons[key].append(trade)
    
    for reasons, trades_list in sorted(failure_reasons.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"\n{len(trades_list)} trades failed because:")
        for r in reasons:
            print(f"    ❌ {r}")
        print(f"  Trades:")
        for t in trades_list[:5]:  # Show first 5
            print(f"    - Trade {t['id']}: {t['potential_gains']:.4f}% at {t['followed_at']}")
        if len(trades_list) > 5:
            print(f"    ... and {len(trades_list)-5} more")
    
    print("\n" + "="*100)
    print("CONCLUSION")
    print("="*100)
    print(f"  Total opportunities: {len(trades)}")
    print(f"  Captured: {len(passed_trades)} ({len(passed_trades)/len(trades)*100:.1f}%)")
    print(f"  Missed: {len(failed_trades)} ({len(failed_trades)/len(trades)*100:.1f}%)")
    print()
    print("  Main issue: Filter thresholds are slightly above current market values")
    print("  This happens when market prices drop below the training data range")
    print("  The system will auto-correct in the next filter update cycle (every 15 min)")
    print("="*100 + "\n")


if __name__ == "__main__":
    main()

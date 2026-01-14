#!/usr/bin/env python3
"""
Real-Time Filter Performance Monitor
=====================================
Shows live performance of current filter settings as new trades come in.
Run this in a separate terminal to monitor improvements.
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def get_recent_performance(minutes_back: int = 60):
    """Get filter performance for recent trades."""
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get recent trades
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN potential_gains >= 0.3 THEN 1 ELSE 0 END) as good,
                        SUM(CASE WHEN potential_gains < 0.3 THEN 1 ELSE 0 END) as bad,
                        AVG(CASE WHEN potential_gains >= 0.3 THEN potential_gains END) as avg_good_gain,
                        MAX(potential_gains) as max_gain
                    FROM follow_the_goat_buyins
                    WHERE followed_at >= NOW() - INTERVAL '%s minutes'
                      AND potential_gains IS NOT NULL
                """, [minutes_back])
                
                stats = cursor.fetchone()
                
                if stats['total'] == 0:
                    return None
                
                return {
                    'total': stats['total'],
                    'good': stats['good'],
                    'bad': stats['bad'],
                    'good_pct': (stats['good'] / stats['total'] * 100) if stats['total'] > 0 else 0,
                    'avg_good_gain': stats['avg_good_gain'],
                    'max_gain': stats['max_gain']
                }
    except Exception as e:
        print(f"Error: {e}")
        return None


def get_filter_info():
    """Get current active filters."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT name, section, minute, field_column, from_value, to_value
                    FROM pattern_config_filters
                    WHERE project_id = 5 AND is_active = 1
                    ORDER BY minute, id
                """)
                
                return cursor.fetchall()
    except Exception as e:
        print(f"Error: {e}")
        return []


def display_dashboard():
    """Display performance dashboard."""
    
    # Clear screen
    print("\033[2J\033[H")
    
    print("="*80)
    print(" REAL-TIME FILTER PERFORMANCE MONITOR".center(80))
    print("="*80)
    print(f" Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(80))
    print("="*80)
    
    # Show active filters
    print("\n📊 ACTIVE FILTERS (AutoFilters Project)")
    print("-"*80)
    
    filters = get_filter_info()
    if filters:
        for f in filters:
            print(f"  M{f['minute']:2d} | {f['name']:40s} | "
                  f"[{f['from_value']:10.6f} to {f['to_value']:10.6f}]")
    else:
        print("  No active filters found!")
    
    # Performance windows
    windows = [
        (15, "Last 15 minutes"),
        (60, "Last 1 hour"),
        (240, "Last 4 hours"),
        (1440, "Last 24 hours"),
    ]
    
    print("\n📈 PERFORMANCE METRICS")
    print("-"*80)
    print(f"{'Period':<20s} {'Trades':<10s} {'Good':<15s} {'Bad':<15s} {'Avg Gain':<12s} {'Max Gain':<12s}")
    print("-"*80)
    
    for minutes, label in windows:
        stats = get_recent_performance(minutes)
        
        if stats and stats['total'] > 0:
            good_str = f"{stats['good']} ({stats['good_pct']:.1f}%)"
            bad_str = f"{stats['bad']} ({100-stats['good_pct']:.1f}%)"
            avg_gain_str = f"{stats['avg_good_gain']:.3f}%" if stats['avg_good_gain'] else "N/A"
            max_gain_str = f"{stats['max_gain']:.3f}%" if stats['max_gain'] else "N/A"
            
            print(f"{label:<20s} {stats['total']:<10d} {good_str:<15s} {bad_str:<15s} "
                  f"{avg_gain_str:<12s} {max_gain_str:<12s}")
        else:
            print(f"{label:<20s} {'0':<10s} {'N/A':<15s} {'N/A':<15s} {'N/A':<12s} {'N/A':<12s}")
    
    print("-"*80)
    print("\n💡 INTERPRETATION:")
    print("  • Good trades = potential_gains >= 0.3%")
    print("  • Target: 90%+ of trades should be 'good' with new filters")
    print("  • If good trade % is low, filters might be too loose")
    print("\n⏳ Press Ctrl+C to exit. Refreshing every 30 seconds...")
    print("="*80)


def main():
    """Main loop."""
    print("Starting Real-Time Filter Performance Monitor...")
    print("This will refresh every 30 seconds. Press Ctrl+C to exit.\n")
    
    try:
        while True:
            display_dashboard()
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n\nMonitor stopped by user.")
        print("="*80)


if __name__ == "__main__":
    main()

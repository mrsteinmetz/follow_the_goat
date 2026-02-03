"""
Test pre-entry filter for trade 20260203184619631
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime

# Import pre-entry functions
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))
from pre_entry_price_movement import (
    calculate_pre_entry_metrics,
    should_enter_based_on_price_movement,
    log_pre_entry_analysis
)

def main():
    buyin_id = '20260203184619631'
    
    print("=" * 80)
    print("TESTING PRE-ENTRY FILTER")
    print("=" * 80)
    
    # Get buyin details
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    followed_at,
                    our_entry_price
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [buyin_id])
            buyin = cursor.fetchone()
    
    if not buyin:
        print("ERROR: Buyin not found!")
        return
    
    entry_time = buyin['followed_at']
    entry_price = float(buyin['our_entry_price'])
    
    print(f"\nBuyin ID: {buyin_id}")
    print(f"Entry Time: {entry_time}")
    print(f"Entry Price: ${entry_price:.4f}")
    
    # Calculate pre-entry metrics
    print("\nCalculating pre-entry metrics...")
    metrics = calculate_pre_entry_metrics(entry_time, entry_price)
    
    print("\nPRE-ENTRY METRICS:")
    for key, value in metrics.items():
        if value is not None:
            if isinstance(value, float):
                print(f"  {key}: {value:.6f}")
            else:
                print(f"  {key}: {value}")
    
    # Test filter with default threshold (0.08%)
    print("\nTEST 1: Default threshold (0.08%)")
    should_enter, reason = should_enter_based_on_price_movement(metrics, min_change_3m=0.08)
    print(f"  Should enter: {should_enter}")
    print(f"  Reason: {reason}")
    print(f"  Expected: False (because change_3m = {metrics.get('pre_entry_change_3m')}% < 0.08%)")
    
    # Test filter with lower threshold
    print("\nTEST 2: Lower threshold (0.03%)")
    should_enter2, reason2 = should_enter_based_on_price_movement(metrics, min_change_3m=0.03)
    print(f"  Should enter: {should_enter2}")
    print(f"  Reason: {reason2}")
    
    # Check what's in trail data
    print("\nCHECKING TRAIL DATA (minute 0):")
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    pre_entry_trend,
                    pre_entry_change_1m,
                    pre_entry_change_2m,
                    pre_entry_change_3m,
                    pre_entry_change_5m,
                    pre_entry_change_10m
                FROM buyin_trail_minutes
                WHERE buyin_id = %s AND minute = 0 AND sub_minute = 0
            """, [buyin_id])
            trail = cursor.fetchone()
    
    if trail:
        print("  Stored in trail:")
        for key, value in trail.items():
            if value is not None:
                print(f"    {key}: {value}")
    
    print("\n" + "=" * 80)
    print("CONCLUSION:")
    print("=" * 80)
    
    change_3m = metrics.get('pre_entry_change_3m')
    if change_3m and change_3m < 0.08:
        print(f"✗ FILTER SHOULD HAVE REJECTED THIS TRADE")
        print(f"  Reason: change_3m ({change_3m:.4f}%) < 0.08% threshold")
        print(f"\n  WHY DID IT PASS?")
        print(f"  Possible causes:")
        print(f"  1. Filter not enabled in pattern_validator.py")
        print(f"  2. Filter bypassed for training trades")
        print(f"  3. Filter added AFTER this trade was made")
        print(f"  4. Exception/error in filter logic")
    else:
        print(f"✓ This trade would pass the filter")


if __name__ == "__main__":
    main()

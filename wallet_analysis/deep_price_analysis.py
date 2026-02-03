"""
Deep analysis of pre-entry price movement for trade 20260203184619631
Check actual price data before entry
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime, timedelta

def main():
    buyin_id = '20260203184619631'
    
    print("=" * 80)
    print("PRE-ENTRY PRICE MOVEMENT DEEP DIVE")
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
    
    print(f"\nEntry Time: {entry_time}")
    print(f"Entry Price: ${entry_price:.4f}")
    
    # Get price data for 15 minutes before entry
    start_time = entry_time - timedelta(minutes=15)
    
    print(f"\nFETCHING PRICES FROM {start_time} TO {entry_time}...")
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    timestamp,
                    price
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 100
            """, [start_time, entry_time])
            prices = cursor.fetchall()
    
    if not prices:
        print("ERROR: No price data found!")
        return
    
    print(f"\nFound {len(prices)} price records\n")
    
    # Calculate time deltas and show prices
    print("TIME BEFORE ENTRY | PRICE    | CHANGE FROM ENTRY")
    print("-" * 60)
    
    for p in prices:
        delta = entry_time - p['timestamp']
        minutes_ago = delta.total_seconds() / 60
        price = float(p['price'])
        change = ((entry_price - price) / price) * 100
        
        marker = ""
        if abs(minutes_ago - 1) < 0.1:
            marker = " <- 1m before"
        elif abs(minutes_ago - 3) < 0.1:
            marker = " <- 3m before (FILTER CHECKS THIS)"
        elif abs(minutes_ago - 5) < 0.1:
            marker = " <- 5m before"
        elif abs(minutes_ago - 10) < 0.1:
            marker = " <- 10m before"
        
        print(f"{minutes_ago:6.2f}m ago     | ${price:7.4f} | {change:+7.4f}%{marker}")
    
    # Find minimum price in the window
    min_price = min(float(p['price']) for p in prices)
    max_price = max(float(p['price']) for p in prices)
    min_time = [p for p in prices if float(p['price']) == min_price][0]['timestamp']
    max_time = [p for p in prices if float(p['price']) == max_price][0]['timestamp']
    
    min_delta = (entry_time - min_time).total_seconds() / 60
    max_delta = (entry_time - max_time).total_seconds() / 60
    
    print("\n" + "=" * 80)
    print("ANALYSIS:")
    print("=" * 80)
    print(f"\nLowest price in 15m window: ${min_price:.4f} ({min_delta:.2f}m before entry)")
    print(f"Highest price in 15m window: ${max_price:.4f} ({max_delta:.2f}m before entry)")
    print(f"Entry price: ${entry_price:.4f}")
    
    change_from_low = ((entry_price - min_price) / min_price) * 100
    change_from_high = ((entry_price - max_price) / max_price) * 100
    
    print(f"\nEntry vs Lowest: {change_from_low:+.2f}%")
    print(f"Entry vs Highest: {change_from_high:+.2f}%")
    
    # Determine if this was a good entry
    print("\n" + "-" * 80)
    print("ENTRY TIMING ASSESSMENT:")
    print("-" * 80)
    
    if min_delta < 5:
        print(f"⚠️  WARNING: Lowest price was only {min_delta:.1f}m before entry!")
        print(f"   This suggests price was still falling/bottoming when we entered")
        print(f"   Better to wait for clear reversal confirmation (higher lows)")
    
    if change_from_low < 0.3:
        print(f"⚠️  WARNING: Entry was only {change_from_low:.2f}% above recent low!")
        print(f"   High risk of further downside - need bigger cushion")
    
    if change_from_high > -0.1:
        print(f"✓ Entry near local high - good entry point")
    else:
        print(f"⚠️  Entry was {abs(change_from_high):.2f}% below recent high")
        print(f"   Price had pulled back before we entered")
    
    # Check the image observation (entry during falling period)
    print("\n" + "-" * 80)
    print("IMAGE ANALYSIS (from user):")
    print("-" * 80)
    print("User noted entry occurred during a FALLING period (red circled area)")
    print("Chart shows price declining from ~18:48 to ~19:00 UTC")
    print("\nLet's check if our 3m filter caught this:")
    
    # Get price 3m before entry
    target_3m = entry_time - timedelta(minutes=3)
    price_3m_candidates = [p for p in prices if abs((p['timestamp'] - target_3m).total_seconds()) < 30]
    if price_3m_candidates:
        price_3m = float(price_3m_candidates[0]['price'])
        change_3m = ((entry_price - price_3m) / price_3m) * 100
        print(f"\nPrice 3m before: ${price_3m:.4f}")
        print(f"Change over 3m: {change_3m:+.4f}%")
        print(f"Filter threshold: 0.08%")
        print(f"Result: {'PASS ✓' if change_3m >= 0.08 else 'FAIL ✗'}")
        
        if change_3m >= 0.08 and change_3m < 0.2:
            print(f"\n⚠️  DIAGNOSIS: Price was barely rising (only {change_3m:.3f}%)")
            print(f"   Filter passed but momentum was weak")
            print(f"   RECOMMENDATION: Increase threshold to 0.15-0.25% for safer entries")


if __name__ == "__main__":
    main()

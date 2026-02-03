"""
Investigation of Trade 20260203184619631 - Why was it given a "go" signal too early?
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime
import json

def main():
    buyin_id = '20260203184619631'
    play_id = 46
    
    print("=" * 80)
    print(f"INVESTIGATING TRADE: {buyin_id}")
    print("=" * 80)
    
    # 1. Get buyin details
    print("\n1. BUYIN DETAILS:")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    followed_at,
                    our_entry_price,
                    our_status,
                    our_exit_price,
                    our_exit_timestamp,
                    our_profit_loss,
                    play_id,
                    wallet_address,
                    higest_price_reached,
                    current_price,
                    entry_log
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [buyin_id])
            buyin = cursor.fetchone()
    
    if not buyin:
        print(f"ERROR: Buyin {buyin_id} not found!")
        return
    
    for key, value in buyin.items():
        print(f"  {key}: {value}")
    
    # 2. Get play details
    print(f"\n2. PLAY DETAILS (ID: {play_id}):")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    wallet,
                    token,
                    play_name,
                    created_at
                FROM follow_the_goat_plays
                WHERE id = %s
            """, [play_id])
            play = cursor.fetchone()
    
    if play:
        for key, value in play.items():
            print(f"  {key}: {value}")
    else:
        print("  Play not found!")
    
    # 3. Get trail data
    print(f"\n3. TRAIL DATA (15-minute window):")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    minute,
                    sub_minute,
                    -- Price Movements
                    pm_price_change_1m,
                    pm_price_change_5m,
                    pm_price_change_10m,
                    pm_volatility_pct,
                    pm_momentum_volatility_ratio,
                    pm_close_price,
                    -- Order Book
                    ob_volume_imbalance,
                    ob_imbalance_shift_1m,
                    ob_spread_bps,
                    ob_depth_imbalance_pct,
                    -- Transactions
                    tx_buy_sell_pressure,
                    tx_pressure_shift_1m,
                    tx_volume_surge_ratio,
                    tx_whale_volume_pct,
                    -- Whale Activity
                    wh_net_flow_ratio,
                    wh_flow_shift_1m,
                    wh_accumulation_ratio,
                    -- Pattern Detection
                    pat_breakout_score,
                    -- Micro-patterns
                    mp_volume_divergence_detected,
                    mp_momentum_acceleration_detected,
                    -- Pre-entry data (minute 0 only)
                    pre_entry_trend,
                    pre_entry_change_1m,
                    pre_entry_change_2m,
                    pre_entry_change_5m,
                    pre_entry_change_10m
                FROM buyin_trail_minutes
                WHERE buyin_id = %s
                ORDER BY minute ASC, sub_minute ASC
            """, [buyin_id])
            trail_rows = cursor.fetchall()
    
    if not trail_rows:
        print("  No trail data found!")
        print("\n  Checking if trail table exists...")
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as total_trails
                    FROM buyin_trail_minutes
                """)
                count = cursor.fetchone()
                print(f"  Total trails in database: {count['total_trails']}")
        return
    
    print(f"\n  Found {len(trail_rows)} trail records (30-second intervals)\n")
    
    # Display minute 0 (entry decision point)
    print("  MINUTE 0 (Entry Decision):")
    print("  " + "-" * 76)
    minute_0_rows = [r for r in trail_rows if r['minute'] == 0]
    for row in minute_0_rows:
        sub = row['sub_minute']
        print(f"  Sub-minute {sub} (0-30s):")
        print(f"    Price Change 1m: {row['pm_price_change_1m']}")
        print(f"    Price Change 5m: {row['pm_price_change_5m']}")
        print(f"    Volatility: {row['pm_volatility_pct']}")
        print(f"    Momentum/Vol Ratio: {row['pm_momentum_volatility_ratio']}")
        print(f"    Buy/Sell Pressure: {row['tx_buy_sell_pressure']}")
        print(f"    Volume Surge Ratio: {row['tx_volume_surge_ratio']}")
        print(f"    Whale Net Flow: {row['wh_net_flow_ratio']}")
        print(f"    Breakout Score: {row['pat_breakout_score']}")
        
        if row['pre_entry_trend']:
            print(f"    PRE-ENTRY TREND: {row['pre_entry_trend']}")
            print(f"    Pre-entry 1m change: {row['pre_entry_change_1m']}%")
            print(f"    Pre-entry 2m change: {row['pre_entry_change_2m']}%")
            print(f"    Pre-entry 5m change: {row['pre_entry_change_5m']}%")
            print(f"    Pre-entry 10m change: {row['pre_entry_change_10m']}%")
        print()
    
    # Display first few minutes to see trend
    print("  MINUTES 1-5 (Post-Entry Trend):")
    print("  " + "-" * 76)
    for minute in range(1, 6):
        minute_rows = [r for r in trail_rows if r['minute'] == minute and r['sub_minute'] == 0]
        if minute_rows:
            row = minute_rows[0]
            print(f"  Minute {minute}:")
            print(f"    Price Change 1m: {row['pm_price_change_1m']}")
            print(f"    Close Price: {row['pm_close_price']}")
            print(f"    Buy/Sell Pressure: {row['tx_buy_sell_pressure']}")
            print(f"    Whale Net Flow: {row['wh_net_flow_ratio']}")
    
    # 4. Check filter analysis
    print(f"\n4. FILTER VALUES AT ENTRY:")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    filter_name,
                    filter_value,
                    is_ratio,
                    section
                FROM trade_filter_values
                WHERE buyin_id = %s
                    AND minute = 0
                    AND sub_minute = 0
                    AND filter_name IN (
                        'pm_price_change_1m',
                        'pm_price_change_5m',
                        'pm_momentum_volatility_ratio',
                        'tx_buy_sell_pressure',
                        'tx_volume_surge_ratio',
                        'wh_net_flow_ratio',
                        'ob_volume_imbalance',
                        'pat_breakout_score'
                    )
                ORDER BY section, filter_name
            """, [buyin_id])
            filters = cursor.fetchall()
    
    if filters:
        for f in filters:
            print(f"  {f['filter_name']}: {f['filter_value']} (section: {f['section']})")
    else:
        print("  No filter values found at entry point")
    
    # 5. Analyze what went wrong
    print(f"\n5. ANALYSIS:")
    print("-" * 80)
    
    if buyin:
        entry_time = buyin['followed_at']
        entry_price = float(buyin['our_entry_price'])
        exit_price = float(buyin['our_exit_price']) if buyin['our_exit_price'] else None
        profit_loss = float(buyin['our_profit_loss']) if buyin['our_profit_loss'] else None
        highest_price = float(buyin['higest_price_reached']) if buyin['higest_price_reached'] else None
        
        print(f"  Entry Time: {entry_time}")
        print(f"  Entry Price: ${entry_price}")
        if highest_price:
            print(f"  Highest Price Reached: ${highest_price}")
            gain_to_peak = ((highest_price - entry_price) / entry_price) * 100
            print(f"  Peak Gain: {gain_to_peak:.2f}%")
        if exit_price:
            print(f"  Exit Price: ${exit_price}")
            print(f"  Profit/Loss: ${profit_loss}")
        print(f"  Status: {buyin['our_status']}")
        
        # Check pre-entry trend
        if minute_0_rows and minute_0_rows[0]['pre_entry_trend']:
            pre_trend = minute_0_rows[0]['pre_entry_trend']
            pre_change_10m = minute_0_rows[0]['pre_entry_change_10m']
            
            print(f"\n  PRE-ENTRY ANALYSIS:")
            print(f"    10-minute trend before entry: {pre_trend}")
            print(f"    Price change (10m before entry): {pre_change_10m}%")
            
            if pre_trend == 'falling' or (pre_change_10m and pre_change_10m < -0.5):
                print(f"    ⚠️  WARNING: Price was FALLING before entry!")
                print(f"    This suggests entry was too early - should have waited for reversal")
    
    print("\n" + "=" * 80)
    print("INVESTIGATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()

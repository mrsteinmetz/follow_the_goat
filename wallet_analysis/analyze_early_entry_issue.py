"""
Analysis of Trade 20260203184619631 - Early Entry Problem
Based on chart image showing entry during a falling period
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime, timedelta
import json

def main():
    buyin_id = '20260203184619631'
    
    print("=" * 80)
    print("EARLY ENTRY ANALYSIS")
    print("=" * 80)
    
    # Get buyin details
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
                    higest_price_reached,
                    entry_log
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [buyin_id])
            buyin = cursor.fetchone()
    
    if not buyin:
        print("ERROR: Buyin not found!")
        return
    
    entry_time = buyin['followed_at']
    entry_price = float(buyin['our_entry_price'])
    exit_price = float(buyin['our_exit_price']) if buyin['our_exit_price'] else None
    profit_loss = float(buyin['our_profit_loss']) if buyin['our_profit_loss'] else None
    highest_price = float(buyin['higest_price_reached']) if buyin['higest_price_reached'] else None
    
    print(f"\nTRADE OUTCOME:")
    print(f"  Entry Time: {entry_time}")
    print(f"  Entry Price: ${entry_price:.2f}")
    if highest_price:
        print(f"  Highest Price: ${highest_price:.2f}")
    if exit_price:
        print(f"  Exit Price: ${exit_price:.2f}")
        actual_gain = ((exit_price - entry_price) / entry_price) * 100
        print(f"  Actual P/L: {actual_gain:.2f}% (${profit_loss:.2f})")
    print(f"  Status: {buyin['our_status']}")
    
    # Check entry log for validator decision
    if buyin['entry_log']:
        entry_log = buyin['entry_log'] if isinstance(buyin['entry_log'], list) else json.loads(buyin['entry_log'])
        print(f"\nVALIDATOR DECISION:")
        for step in entry_log:
            if step.get('step') == 'pattern_validator':
                print(f"  Decision: {step.get('details', {}).get('decision')}")
                print(f"  Play: {step.get('details', {}).get('play_id')}")
                print(f"  Projects: {step.get('details', {}).get('project_ids')}")
                print(f"  Validator Version: {step.get('details', {}).get('validator_version')}")
    
    # Get trail data at entry (minute 0)
    print(f"\nTRAIL DATA AT ENTRY (Minute 0):")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    minute,
                    sub_minute,
                    pm_price_change_1m,
                    pm_price_change_5m,
                    pm_price_change_10m,
                    pm_volatility_pct,
                    pm_momentum_volatility_ratio,
                    pm_close_price,
                    pm_open_price,
                    pm_high_price,
                    pm_low_price,
                    tx_buy_sell_pressure,
                    tx_volume_surge_ratio,
                    wh_net_flow_ratio,
                    wh_accumulation_ratio,
                    ob_volume_imbalance,
                    pat_breakout_score,
                    pre_entry_trend,
                    pre_entry_change_1m,
                    pre_entry_change_2m,
                    pre_entry_change_5m,
                    pre_entry_change_10m
                FROM buyin_trail_minutes
                WHERE buyin_id = %s AND minute = 0
                ORDER BY sub_minute ASC
            """, [buyin_id])
            minute_0_data = cursor.fetchall()
    
    if minute_0_data:
        for row in minute_0_data:
            print(f"\n  Sub-Minute {row['sub_minute']} (0-30s interval):")
            print(f"    Price Movement:")
            print(f"      1m change: {row['pm_price_change_1m']}%")
            print(f"      5m change: {row['pm_price_change_5m']}%")
            print(f"      10m change: {row['pm_price_change_10m']}%")
            print(f"      Volatility: {row['pm_volatility_pct']}%")
            print(f"      Momentum/Vol Ratio: {row['pm_momentum_volatility_ratio']}")
            print(f"    Trading Signals:")
            print(f"      Buy/Sell Pressure: {row['tx_buy_sell_pressure']}")
            print(f"      Volume Surge: {row['tx_volume_surge_ratio']}")
            print(f"      Whale Net Flow: {row['wh_net_flow_ratio']}")
            print(f"      OB Imbalance: {row['ob_volume_imbalance']}")
            print(f"      Breakout Score: {row['pat_breakout_score']}")
            
            if row['pre_entry_trend']:
                print(f"    PRE-ENTRY ANALYSIS (Looking Back):")
                print(f"      Trend: {row['pre_entry_trend']}")
                print(f"      1m before: {row['pre_entry_change_1m']}%")
                print(f"      2m before: {row['pre_entry_change_2m']}%")
                print(f"      5m before: {row['pre_entry_change_5m']}%")
                print(f"      10m before: {row['pre_entry_change_10m']}%")
    else:
        print("  No trail data found!")
    
    # Get next few minutes to see what happened after entry
    print(f"\nPOST-ENTRY PRICE MOVEMENT (Minutes 1-5):")
    print("-" * 80)
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    minute,
                    pm_price_change_1m,
                    pm_close_price,
                    tx_buy_sell_pressure,
                    wh_net_flow_ratio
                FROM buyin_trail_minutes
                WHERE buyin_id = %s 
                    AND minute BETWEEN 1 AND 5
                    AND sub_minute = 0
                ORDER BY minute ASC
            """, [buyin_id])
            post_entry = cursor.fetchall()
    
    if post_entry:
        for row in post_entry:
            price_vs_entry = ((row['pm_close_price'] - entry_price) / entry_price) * 100 if row['pm_close_price'] else None
            print(f"  Minute {row['minute']}:")
            print(f"    Close: ${row['pm_close_price']:.2f} ({price_vs_entry:+.2f}% from entry)")
            print(f"    1m Change: {row['pm_price_change_1m']}%")
            print(f"    Buy Pressure: {row['tx_buy_sell_pressure']}")
    
    # ANALYSIS
    print(f"\n{'=' * 80}")
    print("DIAGNOSIS:")
    print("=" * 80)
    
    if minute_0_data and minute_0_data[0]['pre_entry_trend']:
        pre_trend = minute_0_data[0]['pre_entry_trend']
        pre_10m = minute_0_data[0]['pre_entry_change_10m']
        pre_5m = minute_0_data[0]['pre_entry_change_5m']
        
        print(f"\n1. PRE-ENTRY TREND ANALYSIS:")
        print(f"   - 10m trend before entry: {pre_trend}")
        print(f"   - Price change 10m before: {pre_10m}%")
        print(f"   - Price change 5m before: {pre_5m}%")
        
        if pre_trend == 'falling' or (pre_10m and pre_10m < -0.3):
            print(f"   ⚠️  WARNING: Price was FALLING before entry!")
            print(f"   ⚠️  This suggests the entry signal fired too early")
            print(f"   ⚠️  Should have waited for:")
            print(f"       - Reversal confirmation (higher lows)")
            print(f"       - Sustained buying pressure")
            print(f"       - Break above recent resistance")
        
        # Check immediate post-entry movement
        if post_entry and len(post_entry) >= 2:
            first_min = post_entry[0]
            if first_min['pm_price_change_1m'] and first_min['pm_price_change_1m'] < 0:
                print(f"\n2. IMMEDIATE POST-ENTRY:")
                print(f"   - Price continued falling in minute 1: {first_min['pm_price_change_1m']}%")
                print(f"   ⚠️  Confirms entry was too early - no reversal yet")
    
    print(f"\n3. ROOT CAUSE:")
    print(f"   The trail_generator.py likely does not check for:")
    print(f"   - Pre-entry trend direction")
    print(f"   - Recent price momentum (10m lookback)")
    print(f"   - Reversal confirmation signals")
    print(f"\n4. RECOMMENDED FIX:")
    print(f"   Add pre-entry filters to trail_generator.py:")
    print(f"   - Reject if pre_entry_trend == 'falling'")
    print(f"   - Reject if pre_entry_change_5m < -0.5%")
    print(f"   - Require positive momentum shift (last 1-2 minutes)")
    print(f"   - Add minimum breakout_score threshold")
    
    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    main()

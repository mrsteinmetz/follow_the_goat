"""
Investigation script for trade 20260203185120190
Investigating why the trade was sold despite drop being less than tolerance.
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
import json
from datetime import datetime

def investigate_trade():
    """Investigate the specific trade and its price checks."""
    
    trade_id = '20260203185120190'
    play_id = 2
    
    print("=" * 80)
    print(f"INVESTIGATING TRADE: {trade_id}")
    print("=" * 80)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get the buyin details
            print("\n1. BUYIN DETAILS")
            print("-" * 80)
            cursor.execute("""
                SELECT 
                    id,
                    play_id,
                    wallet_address,
                    price as entry_price,
                    our_entry_price,
                    our_exit_price,
                    our_exit_timestamp,
                    our_profit_loss,
                    our_status,
                    higest_price_reached,
                    tolerance,
                    followed_at,
                    live_trade
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [trade_id])
            buyin = cursor.fetchone()
            
            if not buyin:
                print(f"Trade {trade_id} not found!")
                return
            
            for key, value in buyin.items():
                print(f"  {key}: {value}")
            
            # Get the play's sell_logic
            print("\n2. PLAY SELL LOGIC (Tolerance Rules)")
            print("-" * 80)
            cursor.execute("""
                SELECT sell_logic, name
                FROM follow_the_goat_plays
                WHERE id = %s
            """, [play_id])
            play = cursor.fetchone()
            
            if play:
                print(f"  Play Name: {play['name']}")
                if play['sell_logic']:
                    logic = json.loads(play['sell_logic']) if isinstance(play['sell_logic'], str) else play['sell_logic']
                    print(f"  Sell Logic: {json.dumps(logic, indent=2)}")
                    
                    # Extract tolerance rules
                    if 'tolerance_rules' in logic:
                        rules = logic['tolerance_rules']
                        print("\n  TOLERANCE RULES:")
                        
                        if 'decreases' in rules:
                            print("    Decreases (stop-loss from entry):")
                            for rule in rules['decreases']:
                                range_vals = rule.get('range', [])
                                tolerance = rule.get('tolerance', 0)
                                print(f"      Range: {range_vals[0]*100:.2f}% to {range_vals[1]*100:.2f}% -> Tolerance: {tolerance*100:.4f}%")
                        
                        if 'increases' in rules:
                            print("    Increases (trailing stop from highest):")
                            for rule in rules['increases']:
                                range_vals = rule.get('range', [])
                                tolerance = rule.get('tolerance', 0)
                                print(f"      Range: {range_vals[0]*100:.2f}% to {range_vals[1]*100:.2f}% -> Tolerance: {tolerance*100:.4f}%")
            
            # Get all price checks for this trade
            print("\n3. PRICE CHECK HISTORY")
            print("-" * 80)
            cursor.execute("""
                SELECT 
                    id,
                    checked_at,
                    current_price,
                    entry_price,
                    highest_price,
                    reference_price,
                    gain_from_entry,
                    drop_from_high,
                    drop_from_entry,
                    drop_from_reference,
                    tolerance,
                    basis,
                    bucket,
                    should_sell,
                    applied_rule
                FROM follow_the_goat_buyins_price_checks
                WHERE buyin_id = %s
                ORDER BY checked_at DESC
                LIMIT 50
            """, [trade_id])
            checks = cursor.fetchall()
            
            print(f"  Found {len(checks)} price checks")
            print("\n  LAST 20 CHECKS (most recent first):")
            print("  " + "-" * 76)
            
            for i, check in enumerate(checks[:20], 1):
                checked_at = check['checked_at']
                current_price = float(check['current_price'])
                entry_price = float(check['entry_price'])
                highest_price = float(check['highest_price'])
                gain_from_entry = float(check['gain_from_entry']) if check['gain_from_entry'] else 0
                drop_from_high = float(check['drop_from_high']) if check['drop_from_high'] else 0
                drop_from_entry = float(check['drop_from_entry']) if check['drop_from_entry'] else 0
                tolerance = float(check['tolerance']) if check['tolerance'] else 0
                should_sell = check['should_sell']
                basis = check['basis']
                bucket = check['bucket']
                
                print(f"\n  Check #{i} - {checked_at}")
                print(f"    Current: ${current_price:.6f}  Entry: ${entry_price:.6f}  Highest: ${highest_price:.6f}")
                print(f"    Gain from entry: {gain_from_entry*100:+.4f}%")
                print(f"    Drop from highest: {drop_from_high*100:+.4f}%")
                print(f"    Drop from entry: {drop_from_entry*100:+.4f}%")
                print(f"    Tolerance: {tolerance*100:.4f}%  Basis: {basis}  Bucket: {bucket}")
                print(f"    Should Sell: {should_sell} {'← SELL SIGNAL' if should_sell else ''}")
                
                if check['applied_rule']:
                    try:
                        rule = json.loads(check['applied_rule']) if isinstance(check['applied_rule'], str) else check['applied_rule']
                        print(f"    Applied Rule: {rule}")
                    except:
                        pass
            
            # Find the sell signal
            print("\n4. SELL SIGNAL ANALYSIS")
            print("-" * 80)
            sell_checks = [c for c in checks if c['should_sell']]
            if sell_checks:
                sell_check = sell_checks[0]  # Most recent sell signal
                
                checked_at = sell_check['checked_at']
                current_price = float(sell_check['current_price'])
                entry_price = float(sell_check['entry_price'])
                highest_price = float(sell_check['highest_price'])
                gain_from_entry = float(sell_check['gain_from_entry']) if sell_check['gain_from_entry'] else 0
                drop_from_high = float(sell_check['drop_from_high']) if sell_check['drop_from_high'] else 0
                drop_from_entry = float(sell_check['drop_from_entry']) if sell_check['drop_from_entry'] else 0
                tolerance = float(sell_check['tolerance']) if sell_check['tolerance'] else 0
                basis = sell_check['basis']
                bucket = sell_check['bucket']
                
                print(f"  Sell signal at: {checked_at}")
                print(f"  Current Price: ${current_price:.6f}")
                print(f"  Entry Price: ${entry_price:.6f}")
                print(f"  Highest Price: ${highest_price:.6f}")
                print(f"  Gain from entry: {gain_from_entry*100:+.4f}%")
                print(f"  Drop from highest: {drop_from_high*100:+.4f}%")
                print(f"  Drop from entry: {drop_from_entry*100:+.4f}%")
                print(f"  Tolerance: {tolerance*100:.4f}%")
                print(f"  Basis: {basis}")
                print(f"  Bucket: {bucket}")
                
                # Calculate if it should have sold
                print("\n  ANALYSIS:")
                if bucket == 'decreases':
                    # Below entry price - check stop-loss
                    print(f"    Trade was BELOW entry price (bucket: decreases)")
                    print(f"    Drop from entry: {abs(drop_from_entry)*100:.4f}%")
                    print(f"    Stop-loss tolerance: {tolerance*100:.4f}%")
                    if abs(drop_from_entry) > tolerance:
                        print(f"    ✓ Drop EXCEEDED stop-loss tolerance -> SHOULD SELL")
                    else:
                        print(f"    ✗ Drop WITHIN stop-loss tolerance -> SHOULD NOT SELL")
                        print(f"    THIS IS A BUG!")
                else:
                    # Above entry price - check trailing stop
                    print(f"    Trade was ABOVE entry price (bucket: increases)")
                    print(f"    Drop from highest: {abs(drop_from_high)*100:.4f}%")
                    print(f"    Trailing tolerance: {tolerance*100:.4f}%")
                    if abs(drop_from_high) > tolerance:
                        print(f"    ✓ Drop EXCEEDED trailing tolerance -> SHOULD SELL")
                    else:
                        print(f"    ✗ Drop WITHIN trailing tolerance -> SHOULD NOT SELL")
                        print(f"    THIS IS A BUG!")
                
                # Check the highest gain achieved
                highest_gain = ((highest_price - entry_price) / entry_price) if entry_price else 0
                print(f"\n    Highest gain achieved: {highest_gain*100:.4f}%")
                print(f"    This determines which tolerance tier applies")
                
                if check['applied_rule']:
                    try:
                        rule = json.loads(check['applied_rule']) if isinstance(check['applied_rule'], str) else check['applied_rule']
                        print(f"    Applied rule: {rule}")
                    except:
                        pass
            else:
                print("  No sell signals found in price check history!")
    
    print("\n" + "=" * 80)
    print("INVESTIGATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    investigate_trade()

#!/usr/bin/env python3
"""
Investigate why follow_the_goat.py is only triggering 8 times in 24 hours
when 50 wallets are making 30,000 trades each.
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime, timedelta

def investigate():
    print("=" * 80)
    print("FOLLOW THE GOAT INVESTIGATION")
    print("=" * 80)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # 1. Check play 2 configuration
            print("\n1. PLAY 2 CONFIGURATION:")
            cursor.execute("""
                SELECT id, name, is_active, max_buys_per_cycle,
                       pattern_validator_enable, cashe_wallets, bundle_trades
                FROM follow_the_goat_plays 
                WHERE id = 2
            """)
            play = cursor.fetchone()
            if play:
                print(f"   Play ID: {play['id']}")
                print(f"   Name: {play['name']}")
                print(f"   Active: {play['is_active']}")
                print(f"   Max buys per cycle: {play['max_buys_per_cycle']}")
                print(f"   Pattern validator: {play['pattern_validator_enable']}")
            else:
                print("   ❌ Play 2 not found!")
                return
            
            # 2. Check how many wallets the query finds
            print("\n2. WALLET DISCOVERY:")
            cursor.execute("""
                SELECT id, name, find_wallets_sql FROM follow_the_goat_plays WHERE id = 2
            """)
            play_data = cursor.fetchone()
            
            if play_data and play_data['find_wallets_sql']:
                import json
                # Handle both string and dict formats
                find_wallets_sql = play_data['find_wallets_sql']
                if isinstance(find_wallets_sql, str):
                    query_data = json.loads(find_wallets_sql)
                else:
                    query_data = find_wallets_sql
                wallet_query = query_data.get('query')
                
                if wallet_query:
                    print(f"   Running wallet discovery query...")
                    cursor.execute(wallet_query)
                    wallets = cursor.fetchall()
                    print(f"   ✓ Found {len(wallets)} wallets")
                    
                    if len(wallets) > 0:
                        wallet_addresses = [w['wallet_address'] for w in wallets[:5]]
                        print(f"   Sample wallets: {', '.join([w[:8] + '...' for w in wallet_addresses])}")
            
            # 3. Check trades in last 24 hours
            print("\n3. TRADE ACTIVITY (Last 24 hours):")
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(DISTINCT wallet_address) as unique_wallets,
                    MIN(trade_timestamp) as earliest,
                    MAX(trade_timestamp) as latest
                FROM sol_stablecoin_trades 
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                  AND direction = 'buy'
            """)
            trade_stats = cursor.fetchone()
            print(f"   Total buy trades: {trade_stats['total_trades']}")
            print(f"   Unique wallets: {trade_stats['unique_wallets']}")
            print(f"   Time range: {trade_stats['earliest']} to {trade_stats['latest']}")
            
            # 4. Check buyins created for play 2
            print("\n4. BUYINS CREATED (Last 24 hours):")
            cursor.execute("""
                SELECT 
                    COUNT(*) as buyin_count,
                    COUNT(DISTINCT wallet_address) as unique_wallets,
                    MIN(followed_at) as earliest,
                    MAX(followed_at) as latest
                FROM follow_the_goat_buyins 
                WHERE play_id = 2 
                  AND followed_at >= NOW() - INTERVAL '24 hours'
            """)
            buyin_stats = cursor.fetchone()
            print(f"   Total buyins: {buyin_stats['buyin_count']}")
            print(f"   Unique wallets: {buyin_stats['unique_wallets']}")
            print(f"   Time range: {buyin_stats['earliest']} to {buyin_stats['latest']}")
            
            # 5. Check tracking table (last processed trade IDs)
            print("\n5. TRACKING STATE:")
            cursor.execute("""
                SELECT COUNT(*) as tracked_wallets
                FROM follow_the_goat_tracking
            """)
            tracking = cursor.fetchone()
            print(f"   Wallets in tracking table: {tracking['tracked_wallets']}")
            
            cursor.execute("""
                SELECT wallet_address, last_trade_id, last_checked_at
                FROM follow_the_goat_tracking
                ORDER BY last_checked_at DESC
                LIMIT 5
            """)
            recent_tracking = cursor.fetchall()
            print(f"   Recent tracking entries:")
            for t in recent_tracking:
                print(f"     {t['wallet_address'][:8]}... | last_trade_id={t['last_trade_id']} | checked={t['last_checked_at']}")
            
            # 6. CRITICAL: Check if wallets from play 2 actually have NEW trades
            print("\n6. TRADE DETECTION ANALYSIS:")
            if play_data and play_data['find_wallets_sql']:
                # Handle both string and dict formats
                find_wallets_sql = play_data['find_wallets_sql']
                if isinstance(find_wallets_sql, str):
                    query_data = json.loads(find_wallets_sql)
                else:
                    query_data = find_wallets_sql
                wallet_query = query_data.get('query')
                
                if wallet_query:
                    cursor.execute(wallet_query)
                    wallets = cursor.fetchall()
                    wallet_addresses = [w['wallet_address'] for w in wallets]
                    
                    # Check if these wallets have trades
                    cursor.execute("""
                        SELECT 
                            wallet_address,
                            COUNT(*) as trade_count,
                            MAX(id) as max_trade_id,
                            MAX(trade_timestamp) as latest_trade
                        FROM sol_stablecoin_trades
                        WHERE wallet_address = ANY(%s)
                          AND direction = 'buy'
                          AND trade_timestamp >= NOW() - INTERVAL '24 hours'
                        GROUP BY wallet_address
                        ORDER BY trade_count DESC
                        LIMIT 10
                    """, [wallet_addresses])
                    
                    wallet_trades = cursor.fetchall()
                    print(f"   Top 10 most active wallets from play 2:")
                    for wt in wallet_trades:
                        # Check what their last_trade_id is in tracking
                        cursor.execute("""
                            SELECT last_trade_id 
                            FROM follow_the_goat_tracking 
                            WHERE wallet_address = %s
                        """, [wt['wallet_address']])
                        tracking_row = cursor.fetchone()
                        last_processed = tracking_row['last_trade_id'] if tracking_row else 0
                        
                        unprocessed = wt['max_trade_id'] - last_processed if wt['max_trade_id'] > last_processed else 0
                        
                        print(f"     {wt['wallet_address'][:8]}... | {wt['trade_count']} trades | "
                              f"max_id={wt['max_trade_id']} | last_processed={last_processed} | "
                              f"unprocessed={unprocessed} | latest={wt['latest_trade']}")
            
            # 7. Check price cycles
            print("\n7. PRICE CYCLES:")
            cursor.execute("""
                SELECT id, cycle_start_time, cycle_end_time
                FROM cycle_tracker
                WHERE threshold = 0.3
                  AND cycle_start_time >= NOW() - INTERVAL '24 hours'
                ORDER BY id DESC
                LIMIT 5
            """)
            cycles = cursor.fetchall()
            print(f"   Recent cycles (threshold=0.3):")
            for c in cycles:
                cursor.execute("""
                    SELECT COUNT(*) as buyin_count
                    FROM follow_the_goat_buyins
                    WHERE play_id = 2 AND price_cycle = %s
                """, [c['id']])
                cycle_buyins = cursor.fetchone()
                
                print(f"     Cycle {c['id']}: {c['cycle_start_time']} to {c['cycle_end_time']} | "
                      f"buyins={cycle_buyins['buyin_count']}")
            
            # 8. Check if there's a current active cycle
            print("\n8. CURRENT PRICE CYCLE:")
            cursor.execute("""
                SELECT id, cycle_start_time, cycle_end_time
                FROM cycle_tracker
                WHERE threshold = 0.3
                  AND cycle_start_time <= NOW()
                  AND (cycle_end_time IS NULL OR cycle_end_time > NOW())
                ORDER BY id DESC
                LIMIT 1
            """)
            current_cycle = cursor.fetchone()
            if current_cycle:
                print(f"   Current cycle: {current_cycle['id']}")
                print(f"   Started: {current_cycle['cycle_start_time']}")
                print(f"   End: {current_cycle['cycle_end_time']}")
                
                # Check max_buys for this cycle
                cursor.execute("""
                    SELECT COUNT(*) as buyin_count
                    FROM follow_the_goat_buyins
                    WHERE play_id = 2 AND price_cycle = %s
                """, [current_cycle['id']])
                cycle_count = cursor.fetchone()
                print(f"   Buyins in current cycle: {cycle_count['buyin_count']}/{play['max_buys_per_cycle']}")
            else:
                print("   ❌ No active cycle found!")
    
    print("\n" + "=" * 80)
    print("DIAGNOSIS:")
    print("=" * 80)
    
    # Now provide diagnosis
    if buyin_stats['buyin_count'] == 8:
        print("\n✓ Confirmed: Only 8 buyins in last 24 hours")
        print("\nPossible causes:")
        print("1. max_buys_per_cycle is being hit (check cycle analysis above)")
        print("2. Pattern validator is blocking trades")
        print("3. Bundle filter is excluding wallets")
        print("4. Tracking table has stale last_trade_id values")
        print("5. Price cycles are ending too quickly (not enough time to catch trades)")
        print("6. The check_for_new_trades() query isn't finding trades")
        
        if wallet_trades:
            total_unprocessed = sum(max(0, wt['max_trade_id'] - last_processed) 
                                   for wt in wallet_trades 
                                   if wt['max_trade_id'] > (last_processed := 0))
            if total_unprocessed > 0:
                print(f"\n⚠️  CRITICAL: {total_unprocessed} unprocessed trades detected!")
                print("   This indicates check_for_new_trades() is NOT picking them up.")

if __name__ == "__main__":
    investigate()

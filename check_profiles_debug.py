"""Debug script to check why profiles aren't being created."""
import sys
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb

def main():
    print("=" * 60)
    print("PROFILE CREATION DEBUG")
    print("=" * 60)
    
    with get_duckdb("central", read_only=True) as conn:
        # 1. Check trades
        print("\n1. TRADES:")
        buy_count = conn.execute("SELECT COUNT(*) FROM sol_stablecoin_trades WHERE direction = 'buy'").fetchone()[0]
        total_count = conn.execute("SELECT COUNT(*) FROM sol_stablecoin_trades").fetchone()[0]
        print(f"   - Buy trades: {buy_count}")
        print(f"   - Total trades: {total_count}")
        
        # Check recent trades
        recent = conn.execute("""
            SELECT COUNT(*), MAX(trade_timestamp) 
            FROM sol_stablecoin_trades 
            WHERE trade_timestamp > ?
        """, [datetime.now() - timedelta(hours=24)]).fetchone()
        print(f"   - Trades in last 24h: {recent[0]}")
        print(f"   - Latest trade: {recent[1]}")
        
        # 2. Check eligible wallets
        print("\n2. ELIGIBLE WALLETS (>= 3 buy trades):")
        eligible_wallets = conn.execute("""
            SELECT COUNT(DISTINCT wallet_address)
            FROM sol_stablecoin_trades
            WHERE direction = 'buy'
            GROUP BY wallet_address
            HAVING COUNT(id) >= 3
        """).fetchone()
        print(f"   - Eligible wallets: {eligible_wallets[0] if eligible_wallets else 0}")
        
        # 3. Check cycles
        print("\n3. CYCLES:")
        for threshold in [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]:
            completed = conn.execute("""
                SELECT COUNT(*), MAX(cycle_end_time)
                FROM cycle_tracker 
                WHERE threshold = ? AND cycle_end_time IS NOT NULL
            """, [threshold]).fetchone()
            print(f"   - Threshold {threshold}: {completed[0]} completed cycles (latest: {completed[1]})")
        
        # 4. Check price_points
        print("\n4. PRICE POINTS:")
        price_count = conn.execute("SELECT COUNT(*) FROM price_points WHERE coin_id = 5").fetchone()[0]
        print(f"   - SOL price points: {price_count}")
        
        recent_price = conn.execute("""
            SELECT MAX(created_at), COUNT(*) 
            FROM price_points 
            WHERE coin_id = 5 AND created_at > ?
        """, [datetime.now() - timedelta(hours=1)]).fetchone()
        print(f"   - Price points in last hour: {recent_price[1]} (latest: {recent_price[0]})")
        
        # 5. Check profiles
        print("\n5. PROFILES:")
        profile_count = conn.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0]
        print(f"   - Total profiles: {profile_count}")
        
        for threshold in [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]:
            count = conn.execute("""
                SELECT COUNT(*) FROM wallet_profiles WHERE threshold = ?
            """, [threshold]).fetchone()[0]
            print(f"   - Threshold {threshold}: {count} profiles")
        
        # 6. Test the profile query manually for threshold 0.3
        print("\n6. TESTING PROFILE QUERY (threshold 0.3):")
        test_result = conn.execute("""
            WITH eligible_wallets AS (
                SELECT wallet_address
                FROM sol_stablecoin_trades
                WHERE direction = 'buy'
                GROUP BY wallet_address
                HAVING COUNT(id) >= 3
            )
            SELECT 
                COUNT(*) as potential_profiles,
                MIN(t.trade_timestamp) as earliest_trade,
                MAX(t.trade_timestamp) as latest_trade
            FROM sol_stablecoin_trades t
            INNER JOIN eligible_wallets ew ON t.wallet_address = ew.wallet_address
            INNER JOIN cycle_tracker c ON (
                c.threshold = 0.3
                AND c.cycle_start_time <= t.trade_timestamp
                AND c.cycle_end_time >= t.trade_timestamp
                AND c.cycle_end_time IS NOT NULL
            )
            WHERE t.direction = 'buy'
        """).fetchone()
        print(f"   - Potential profiles (trades in completed cycles): {test_result[0]}")
        print(f"   - Date range: {test_result[1]} to {test_result[2]}")
        
        # 7. Check state tracking
        print("\n7. STATE TRACKING:")
        try:
            states = conn.execute("""
                SELECT threshold, last_trade_id, last_updated
                FROM wallet_profiles_state
                ORDER BY threshold
            """).fetchall()
            if states:
                for state in states:
                    print(f"   - Threshold {state[0]}: last_trade_id={state[1]}, updated={state[2]}")
            else:
                print("   - No state records found")
        except Exception as e:
            print(f"   - State table doesn't exist: {e}")
    
    print("\n" + "=" * 60)
    print("DIAGNOSIS:")
    print("=" * 60)
    
    if buy_count == 0:
        print("❌ NO BUY TRADES FOUND - Trades are not being synced!")
        print("   Fix: Check if sync_trades_from_webhook is running")
    elif eligible_wallets is None or eligible_wallets[0] == 0:
        print("❌ NO ELIGIBLE WALLETS - Need wallets with >= 3 buy trades")
        print("   Fix: Wait for more trade data to accumulate")
    elif completed[0] == 0:
        print("❌ NO COMPLETED CYCLES - Cycles haven't finished yet")
        print("   Fix: Wait for price cycles to complete")
    elif price_count == 0:
        print("❌ NO PRICE POINTS - Price data is missing")
        print("   Fix: Check if fetch_jupiter_prices is running")
    elif test_result[0] == 0:
        print("⚠️ NO MATCHING TRADES - Trades don't overlap with completed cycles")
        print("   This might be a timing issue")
    else:
        print("✅ DATA LOOKS GOOD - Profiles should be created")
        print(f"   {test_result[0]} potential profiles are waiting to be processed")
    
    print("=" * 60)

if __name__ == "__main__":
    main()

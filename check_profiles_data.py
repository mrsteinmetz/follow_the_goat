"""
Quick diagnostic script to check wallet_profiles data
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.engine_client import get_engine_client

def main():
    print("=" * 60)
    print("WALLET PROFILES DIAGNOSTIC")
    print("=" * 60)
    
    client = get_engine_client()
    
    # Check if table exists
    print("\n1. Checking if wallet_profiles table exists...")
    try:
        tables = client.get_tables()
        # Handle both dict and string responses
        table_names = []
        if isinstance(tables, list):
            table_names = [t.get('table_name') if isinstance(t, dict) else str(t) for t in tables]
        
        if 'wallet_profiles' in table_names:
            print("   [OK] wallet_profiles table exists")
            
            # Get table info
            if isinstance(tables, list):
                for t in tables:
                    if isinstance(t, dict) and t.get('table_name') == 'wallet_profiles':
                        print(f"   - Row count: {t.get('row_count', 'unknown')}")
        else:
            print("   [ERROR] wallet_profiles table NOT FOUND")
            print(f"   Available tables: {table_names}")
            return
    except Exception as e:
        print(f"   [ERROR] Error checking tables: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Check total profiles
    print("\n2. Checking total profile records...")
    try:
        result = client.query("SELECT COUNT(*) as count FROM wallet_profiles")
        total = result[0]['count'] if result else 0
        print(f"   Total records: {total}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check unique wallets
    print("\n3. Checking unique wallets...")
    try:
        result = client.query("SELECT COUNT(DISTINCT wallet_address) as count FROM wallet_profiles")
        unique = result[0]['count'] if result else 0
        print(f"   Unique wallets: {unique}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check thresholds
    print("\n4. Checking available thresholds...")
    try:
        result = client.query("SELECT DISTINCT threshold FROM wallet_profiles ORDER BY threshold")
        thresholds = [r['threshold'] for r in result]
        print(f"   Thresholds: {thresholds}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check stablecoin_amount values
    print("\n5. Checking stablecoin_amount (invested amounts)...")
    try:
        result = client.query("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(stablecoin_amount) as non_null_count,
                SUM(stablecoin_amount) as total_invested,
                AVG(stablecoin_amount) as avg_invested,
                MIN(stablecoin_amount) as min_invested,
                MAX(stablecoin_amount) as max_invested
            FROM wallet_profiles
        """)
        if result:
            r = result[0]
            print(f"   Total records: {r.get('total_records', 0)}")
            print(f"   Non-null stablecoin_amount: {r.get('non_null_count', 0)}")
            print(f"   Total invested: ${r.get('total_invested', 0):,.2f}")
            print(f"   Avg invested: ${r.get('avg_invested', 0):,.2f}")
            print(f"   Min invested: ${r.get('min_invested', 0):,.2f}")
            print(f"   Max invested: ${r.get('max_invested', 0):,.2f}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check recent profiles
    print("\n6. Checking recent profiles...")
    try:
        result = client.query("""
            SELECT 
                wallet_address, 
                threshold,
                stablecoin_amount,
                trade_timestamp,
                trade_entry_price,
                highest_price_reached,
                lowest_price_reached
            FROM wallet_profiles
            ORDER BY trade_timestamp DESC
            LIMIT 5
        """)
        print(f"   Found {len(result)} recent records:")
        for r in result:
            print(f"   - Wallet: {r.get('wallet_address', '')[:8]}... | "
                  f"Invested: ${r.get('stablecoin_amount', 0):,.2f} | "
                  f"Threshold: {r.get('threshold')} | "
                  f"Time: {r.get('trade_timestamp')}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check aggregated profiles (what the website should show)
    print("\n7. Checking aggregated profiles (per wallet, threshold=0.3)...")
    try:
        result = client.query("""
            SELECT 
                wallet_address,
                COUNT(*) as trade_count,
                SUM(COALESCE(stablecoin_amount, 0)) as total_invested,
                MAX(trade_timestamp) as latest_trade
            FROM wallet_profiles
            WHERE threshold = 0.3
            GROUP BY wallet_address
            ORDER BY total_invested DESC
            LIMIT 5
        """)
        print(f"   Found {len(result)} unique wallets:")
        for r in result:
            print(f"   - Wallet: {r.get('wallet_address', '')[:8]}... | "
                  f"Trades: {r.get('trade_count')} | "
                  f"Invested: ${r.get('total_invested', 0):,.2f} | "
                  f"Latest: {r.get('latest_trade')}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check if there's data in sol_stablecoin_trades
    print("\n8. Checking sol_stablecoin_trades (source data)...")
    try:
        result = client.query("""
            SELECT COUNT(*) as count
            FROM sol_stablecoin_trades
            WHERE direction = 'buy'
        """)
        count = result[0]['count'] if result else 0
        print(f"   Buy trades: {count}")
        
        # Check stablecoin_amount in trades
        result = client.query("""
            SELECT 
                COUNT(stablecoin_amount) as with_amount,
                SUM(CASE WHEN stablecoin_amount IS NULL THEN 1 ELSE 0 END) as null_amount
            FROM sol_stablecoin_trades
            WHERE direction = 'buy'
        """)
        if result:
            print(f"   - With stablecoin_amount: {result[0].get('with_amount', 0)}")
            print(f"   - NULL stablecoin_amount: {result[0].get('null_amount', 0)}")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    # Check cycle_tracker
    print("\n9. Checking cycle_tracker (for completed cycles)...")
    try:
        result = client.query("""
            SELECT 
                threshold,
                COUNT(*) as total_cycles,
                SUM(CASE WHEN cycle_end_time IS NOT NULL THEN 1 ELSE 0 END) as completed_cycles
            FROM cycle_tracker
            GROUP BY threshold
            ORDER BY threshold
        """)
        print(f"   Found {len(result)} thresholds:")
        for r in result:
            print(f"   - Threshold {r.get('threshold')}: "
                  f"{r.get('completed_cycles')} completed / {r.get('total_cycles')} total cycles")
    except Exception as e:
        print(f"   [ERROR] Error: {e}")
    
    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()

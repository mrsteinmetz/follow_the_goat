"""
Data Status Checker - Verify wallet_profiles table health
==========================================================

This script checks the status of the wallet_profiles table to ensure
data is fresh and queries will return meaningful results.

Usage:
    python3 check_data_status.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def check_data_status():
    """Check the health and freshness of wallet_profiles data."""
    print("=" * 80)
    print("WALLET PROFILES DATA STATUS CHECK")
    print("=" * 80)
    print()
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'wallet_profiles'
                    ) as exists
                """)
                result = cursor.fetchone()
                table_exists = result['exists'] if result else False
                
                if not table_exists:
                    print("❌ ERROR: wallet_profiles table does not exist!")
                    print("   The table should be created by create_profiles.py")
                    return False
                
                print("✓ Table exists")
                print()
                
                # Get total record count
                cursor.execute("SELECT COUNT(*) as count FROM wallet_profiles")
                result = cursor.fetchone()
                total_records = result['count'] if result else 0
                print(f"Total records: {total_records:,}")
                
                if total_records == 0:
                    print("❌ WARNING: No data in wallet_profiles table!")
                    print("   Wait for create_profiles.py job to populate data")
                    return False
                
                # Get date range
                cursor.execute("""
                    SELECT 
                        MIN(trade_timestamp) as oldest,
                        MAX(trade_timestamp) as newest
                    FROM wallet_profiles
                """)
                result = cursor.fetchone()
                oldest = result['oldest'] if result else None
                newest = result['newest'] if result else None
                
                print(f"Date range: {oldest} to {newest}")
                print()
                
                # Check freshness (data in last hour)
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '1 hour'
                """)
                result = cursor.fetchone()
                recent_count = result['count'] if result else 0
                
                if recent_count > 0:
                    print(f"✓ Fresh data: {recent_count:,} records in last hour")
                else:
                    print("⚠ WARNING: No data in the last hour")
                    print("  This might be normal if markets are quiet")
                
                print()
                
                # Check data by time period
                print("Records by time period:")
                print("-" * 40)
                
                periods = [
                    ("Last 1 hour", "1 hour"),
                    ("Last 6 hours", "6 hours"),
                    ("Last 24 hours", "24 hours"),
                    ("Last 7 days", "7 days"),
                ]
                
                for label, interval in periods:
                    cursor.execute(f"""
                        SELECT COUNT(*) as count
                        FROM wallet_profiles
                        WHERE trade_timestamp >= NOW() - INTERVAL '{interval}'
                    """)
                    result = cursor.fetchone()
                    count = result['count'] if result else 0
                    print(f"  {label:<15}: {count:>10,} records")
                
                print()
                
                # Check unique wallets
                cursor.execute("""
                    SELECT COUNT(DISTINCT wallet_address) as count
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                unique_wallets = result['count'] if result else 0
                print(f"Unique wallets (24h): {unique_wallets:,}")
                print()
                
                # Check thresholds
                print("Records by threshold:")
                print("-" * 40)
                cursor.execute("""
                    SELECT 
                        threshold,
                        COUNT(*) as count
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY threshold
                    ORDER BY threshold
                """)
                thresholds = cursor.fetchall()
                
                for t in thresholds:
                    print(f"  Threshold {t['threshold']}: {t['count']:>10,} records")
                
                print()
                
                # Sample statistics
                print("Sample statistics (24h, threshold 0.3):")
                print("-" * 40)
                cursor.execute("""
                    SELECT 
                        AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as avg_gain,
                        MIN(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as min_gain,
                        MAX(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as max_gain,
                        AVG(stablecoin_amount) as avg_size,
                        SUM(
                            CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END
                        )::FLOAT / COUNT(*) * 100 as win_rate
                    FROM wallet_profiles
                    WHERE threshold = 0.3
                    AND trade_timestamp >= NOW() - INTERVAL '24 hours'
                """)
                stats = cursor.fetchone()
                
                if stats:
                    print(f"  Avg potential gain: {stats['avg_gain']:.2f}%")
                    print(f"  Min potential gain: {stats['min_gain']:.2f}%")
                    print(f"  Max potential gain: {stats['max_gain']:.2f}%")
                    print(f"  Avg trade size:     ${stats['avg_size']:.2f}")
                    print(f"  Overall win rate:   {stats['win_rate']:.1f}%")
                
                print()
                print("=" * 80)
                print("DATA STATUS: ✓ OK")
                print("=" * 80)
                print()
                print("RECOMMENDATIONS:")
                print("  - Use 24h lookback for best results")
                print("  - Require min 5-10 trades per wallet")
                print(f"  - Expected avg gain is around {stats['avg_gain']:.2f}%")
                print("  - Use threshold 0.3 (most data available)")
                print()
                
                return True
                
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    success = check_data_status()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

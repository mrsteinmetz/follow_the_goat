"""
Compare Query Results - Your Query vs My Queries
=================================================

This script compares the results of the user's original query (looking for 50%+ gains)
with more realistic queries that match the actual data characteristics.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def run_original_query():
    """Run the user's original query (50%+ gains)."""
    print("=" * 80)
    print("YOUR ORIGINAL QUERY (Looking for >50% gains)")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    wallet_address,
                    COUNT(*) AS total_trades,
                    COUNT(*) FILTER (WHERE (100*(1-(trade_entry_price/highest_price_reached))) > 50) AS trades_over_50_percent,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE (100*(1-(trade_entry_price/highest_price_reached))) > 50) / COUNT(*)::numeric, 2) AS win_rate_50plus,
                    ROUND(AVG(100*(1-(trade_entry_price/highest_price_reached)))::numeric, 2) AS avg_potential_gain
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY wallet_address
                HAVING COUNT(*) >= 10
                    AND AVG(100*(1-(trade_entry_price/highest_price_reached))) > 0.3  
                ORDER BY win_rate_50plus DESC, avg_potential_gain DESC
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            
            if results:
                print(f"{'Wallet':<44} {'Total':>6} {'>50%':>6} {'WinRate':>8} {'AvgGain':>8}")
                print("-" * 80)
                for r in results:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['total_trades']:6} "
                        f"{r['trades_over_50_percent']:6} "
                        f"{r['win_rate_50plus']:8.2f} "
                        f"{r['avg_potential_gain']:8.2f}"
                    )
                print(f"\nFound {len(results)} wallets")
            else:
                print("❌ NO RESULTS FOUND")
                print("   Reason: No wallets have trades with >50% gains")
                print("   This is expected because 0.3% price cycles can't produce 50% gains!")
            
            print()


def run_realistic_query():
    """Run a more realistic query (>0.5% gains)."""
    print("=" * 80)
    print("REALISTIC QUERY (Looking for >0.5% gains)")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    wallet_address,
                    COUNT(*) as total_trades,
                    SUM(
                        CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                        THEN 1 ELSE 0 END
                    ) as trades_over_0_5_percent,
                    ROUND((SUM(
                        CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                        THEN 1 ELSE 0 END
                    )::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate,
                    ROUND(AVG(
                        ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                    )::numeric, 2) as avg_gain_pct,
                    ROUND((COUNT(*) * AVG(
                        ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                    ) * (SUM(
                        CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                        THEN 1 ELSE 0 END
                    )::FLOAT / COUNT(*) * 100) / 100)::numeric, 2) as score
                FROM wallet_profiles
                WHERE threshold = 0.3
                AND trade_timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY wallet_address
                HAVING COUNT(*) >= 10
                ORDER BY score DESC
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            
            if results:
                print(f"{'Wallet':<44} {'Total':>6} {'>0.5%':>6} {'WinRate':>8} {'AvgGain':>8} {'Score':>8}")
                print("-" * 80)
                for r in results:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['total_trades']:6} "
                        f"{r['trades_over_0_5_percent']:6} "
                        f"{r['win_rate']:8.1f} "
                        f"{r['avg_gain_pct']:8.2f} "
                        f"{r['score']:8.2f}"
                    )
                print(f"\nFound {len(results)} wallets")
            else:
                print("No results found")
            
            print()


def run_comparison_stats():
    """Show statistics comparing different thresholds."""
    print("=" * 80)
    print("THRESHOLD COMPARISON - What % of trades meet each threshold?")
    print("=" * 80)
    print()
    
    thresholds = [
        (0.5, "0.5%"),
        (1.0, "1.0%"),
        (2.0, "2.0%"),
        (5.0, "5.0%"),
        (10.0, "10.0%"),
        (50.0, "50.0%"),
    ]
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
            """)
            total = cursor.fetchone()['total']
            
            print(f"Total trades in last 24h: {total:,}")
            print()
            print(f"{'Threshold':<12} {'# Trades':>12} {'% of Total':>12}")
            print("-" * 40)
            
            for threshold_pct, label in thresholds:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                    AND threshold = 0.3
                    AND ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s
                """, [threshold_pct])
                
                count = cursor.fetchone()['count']
                pct = (count / total * 100) if total > 0 else 0
                
                print(f">{label:<11} {count:>12,} {pct:>11.2f}%")
            
            print()


def show_recommendations():
    """Show recommendations for better query."""
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()
    print("Your original query looks for >50% gains, which is unrealistic for 0.3% cycles.")
    print()
    print("SUGGESTED CHANGES:")
    print()
    print("1. USE REALISTIC THRESHOLD")
    print("   Change from: >50% gains")
    print("   To: >0.5% or >1.0% gains")
    print()
    print("2. USE STANDARD GAIN FORMULA")
    print("   Old: 100*(1-(trade_entry_price/highest_price_reached))")
    print("   New: ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100")
    print()
    print("3. IMPROVED QUERY:")
    print()
    print("""
    SELECT
        wallet_address,
        COUNT(*) AS total_trades,
        SUM(CASE WHEN highest_price_reached > trade_entry_price * 1.005 
            THEN 1 ELSE 0 END) AS trades_over_0_5pct,
        ROUND((SUM(CASE WHEN highest_price_reached > trade_entry_price * 1.005 
            THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100)::numeric, 1) AS win_rate,
        ROUND(AVG(((highest_price_reached - trade_entry_price) / 
            trade_entry_price) * 100)::numeric, 2) AS avg_gain_pct,
        ROUND((COUNT(*) * AVG(((highest_price_reached - trade_entry_price) / 
            trade_entry_price) * 100))::numeric, 2) AS score
    FROM wallet_profiles
    WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
    AND threshold = 0.3
    GROUP BY wallet_address
    HAVING COUNT(*) >= 10
    ORDER BY score DESC
    LIMIT 100;
    """)
    print()
    print("4. OR USE THE PRE-BUILT SCRIPTS")
    print("   - quick_wallet_query.py")
    print("   - find_high_potential_wallets.py")
    print("   - advanced_wallet_filter.py")
    print()


def main():
    print()
    run_original_query()
    run_realistic_query()
    run_comparison_stats()
    show_recommendations()


if __name__ == "__main__":
    main()

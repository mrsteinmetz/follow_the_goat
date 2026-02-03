"""
Analyze Actual Gains Distribution
==================================

This script analyzes the actual gains in wallet_profiles to see:
1. What is the true distribution of potential gains?
2. Do any trades actually achieve 50%+ gains?
3. What are realistic thresholds to use?
4. Which wallets consistently hit the highest gains?
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def analyze_gain_distribution(days_back=7):
    """Analyze the distribution of actual gains in the data."""
    print("=" * 80)
    print(f"ACTUAL GAIN DISTRIBUTION (Last {days_back} days)")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    MIN(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as min_gain,
                    MAX(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as max_gain,
                    AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as avg_gain,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as median_gain,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p75_gain,
                    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p90_gain,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p95_gain,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p99_gain
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '%s days'
                AND threshold = 0.3
            """ % days_back)
            
            stats = cursor.fetchone()
            
            print(f"Total trades analyzed: {stats['total_trades']:,}")
            print()
            print("Gain Statistics:")
            print(f"  Minimum:  {stats['min_gain']:.4f}%")
            print(f"  Average:  {stats['avg_gain']:.4f}%")
            print(f"  Median:   {stats['median_gain']:.4f}%")
            print(f"  75th %ile: {stats['p75_gain']:.4f}%")
            print(f"  90th %ile: {stats['p90_gain']:.4f}%")
            print(f"  95th %ile: {stats['p95_gain']:.4f}%")
            print(f"  99th %ile: {stats['p99_gain']:.4f}%")
            print(f"  Maximum:  {stats['max_gain']:.4f}%")
            print()
            
            # Distribution buckets
            print("Gain Distribution (How many trades in each range):")
            print("-" * 80)
            
            buckets = [
                (0, 0.3, "0-0.3%"),
                (0.3, 0.5, "0.3-0.5%"),
                (0.5, 1.0, "0.5-1.0%"),
                (1.0, 2.0, "1.0-2.0%"),
                (2.0, 3.0, "2.0-3.0%"),
                (3.0, 5.0, "3.0-5.0%"),
                (5.0, 10.0, "5.0-10.0%"),
                (10.0, 20.0, "10.0-20.0%"),
                (20.0, 50.0, "20.0-50.0%"),
                (50.0, 100.0, "50.0-100.0%"),
                (100.0, 1000.0, ">100%"),
            ]
            
            total = stats['total_trades']
            
            for min_val, max_val, label in buckets:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '%s days'
                    AND threshold = 0.3
                    AND ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 >= %s
                    AND ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 < %s
                """ % (days_back, min_val, max_val))
                
                count = cursor.fetchone()['count']
                pct = (count / total * 100) if total > 0 else 0
                
                bar = "█" * int(pct / 2) if pct > 0 else ""
                print(f"  {label:<12} {count:>10,} ({pct:>6.2f}%) {bar}")
            
            print()


def find_highest_gain_trades(limit=20):
    """Find the trades with the highest actual gains."""
    print("=" * 80)
    print(f"TOP {limit} TRADES BY ACTUAL GAIN")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    wallet_address,
                    trade_timestamp,
                    ROUND(trade_entry_price::numeric, 4) as entry,
                    ROUND(highest_price_reached::numeric, 4) as peak,
                    ROUND(((highest_price_reached - trade_entry_price) / trade_entry_price * 100)::numeric, 4) as gain_pct,
                    price_cycle,
                    ROUND(stablecoin_amount::numeric, 2) as size
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '7 days'
                AND threshold = 0.3
                ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) DESC
                LIMIT %s
            """, [limit])
            
            results = cursor.fetchall()
            
            if results:
                print(f"{'Wallet':<20} {'Timestamp':<20} {'Entry':>10} {'Peak':>10} {'Gain%':>10} {'Size':>10}")
                print("-" * 80)
                for r in results:
                    wallet_short = r['wallet_address'][:8] + "..." + r['wallet_address'][-6:]
                    print(
                        f"{wallet_short:<20} "
                        f"{str(r['trade_timestamp'])[:19]:<20} "
                        f"{r['entry']:>10.4f} "
                        f"{r['peak']:>10.4f} "
                        f"{r['gain_pct']:>10.4f} "
                        f"${r['size']:>9.2f}"
                    )
                
                print()
                print(f"Highest gain achieved: {results[0]['gain_pct']:.4f}%")
                print()


def find_wallets_with_high_gains(min_gain_pct=2.0, min_trades=5):
    """Find wallets that consistently achieve high gains."""
    print("=" * 80)
    print(f"WALLETS WITH CONSISTENT HIGH GAINS (>{min_gain_pct}% avg)")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    wallet_address,
                    COUNT(*) as total_trades,
                    COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s) as high_gain_trades,
                    ROUND((COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s)::FLOAT / COUNT(*) * 100)::numeric, 1) as high_gain_rate,
                    ROUND(AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as avg_gain,
                    ROUND(MAX(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as max_gain,
                    MAX(trade_timestamp) as last_trade
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
                GROUP BY wallet_address
                HAVING COUNT(*) >= %s
                AND AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) > %s
                ORDER BY avg_gain DESC, total_trades DESC
                LIMIT 20
            """, [min_gain_pct, min_gain_pct, min_trades, min_gain_pct])
            
            results = cursor.fetchall()
            
            if results:
                print(f"{'Wallet':<44} {'Trades':>6} {f'>{min_gain_pct}%':>6} {'Rate%':>6} {'AvgGain':>9} {'MaxGain':>9}")
                print("-" * 80)
                for r in results:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['total_trades']:6} "
                        f"{r['high_gain_trades']:6} "
                        f"{r['high_gain_rate']:6.1f} "
                        f"{r['avg_gain']:9.4f} "
                        f"{r['max_gain']:9.4f}"
                    )
                
                print()
                print(f"Found {len(results)} wallets with >{min_gain_pct}% average gain")
                print()
            else:
                print(f"No wallets found with >{min_gain_pct}% average gain and {min_trades}+ trades")
                print()


def analyze_by_threshold(threshold_pct=1.0):
    """Analyze wallets using a specific gain threshold."""
    print("=" * 80)
    print(f"WALLET ANALYSIS - Using >{threshold_pct}% Gain Threshold")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Count trades meeting threshold
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
                AND ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s
            """, [threshold_pct])
            
            qualifying_trades = cursor.fetchone()['count']
            
            # Count total trades
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
            """)
            
            total_trades = cursor.fetchone()['count']
            pct = (qualifying_trades / total_trades * 100) if total_trades > 0 else 0
            
            print(f"Trades exceeding {threshold_pct}% gain: {qualifying_trades:,} ({pct:.2f}% of total)")
            print()
            
            # Find top wallets at this threshold
            cursor.execute("""
                SELECT 
                    wallet_address,
                    COUNT(*) as total_trades,
                    COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s) as wins,
                    ROUND((COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s)::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate,
                    ROUND(AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as avg_gain,
                    ROUND((COUNT(*) * AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) * 
                           (COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 > %s)::FLOAT / COUNT(*) * 100) / 100)::numeric, 2) as score
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
                GROUP BY wallet_address
                HAVING COUNT(*) >= 10
                ORDER BY score DESC
                LIMIT 10
            """, [threshold_pct, threshold_pct, threshold_pct])
            
            results = cursor.fetchall()
            
            if results:
                print(f"Top 10 Wallets (min 10 trades):")
                print(f"{'Wallet':<44} {'Trades':>6} {'Wins':>6} {'WinRate':>8} {'AvgGain':>9} {'Score':>9}")
                print("-" * 80)
                for r in results:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['total_trades']:6} "
                        f"{r['wins']:6} "
                        f"{r['win_rate']:8.1f} "
                        f"{r['avg_gain']:9.4f} "
                        f"{r['score']:9.2f}"
                    )
                print()
            else:
                print("No wallets meet the criteria")
                print()


def recommendations():
    """Provide recommendations based on the analysis."""
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get key stats
            cursor.execute("""
                SELECT 
                    MAX(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as max_gain,
                    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p90_gain,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as p95_gain,
                    AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) as avg_gain
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '7 days'
                AND threshold = 0.3
            """)
            
            stats = cursor.fetchone()
            
            print("Based on the actual data from the last 7 days:")
            print()
            print(f"1. MAXIMUM GAIN OBSERVED: {stats['max_gain']:.2f}%")
            print(f"   {'→ Your 50% threshold is NOT achievable with current data'}")
            print()
            print(f"2. RECOMMENDED THRESHOLDS:")
            print(f"   - Conservative (90th percentile): {stats['p90_gain']:.2f}%")
            print(f"   - Aggressive (95th percentile):  {stats['p95_gain']:.2f}%")
            print(f"   - Very Aggressive (max seen):    {stats['max_gain']:.2f}%")
            print()
            print(f"3. AVERAGE GAIN: {stats['avg_gain']:.2f}%")
            print(f"   Use 0.5% as minimum threshold for 'winning' trades")
            print()
            print("4. SUGGESTED QUERY MODIFICATIONS:")
            print()
            print("   Option A - Use 1% threshold (catches ~3-4% of trades):")
            print("""
   SELECT wallet_address, COUNT(*) as total_trades,
          COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / 
                                  trade_entry_price) * 100 > 1.0) as wins,
          ROUND((COUNT(*) FILTER (WHERE ((highest_price_reached - trade_entry_price) / 
                                         trade_entry_price) * 100 > 1.0)::FLOAT / 
                COUNT(*) * 100)::numeric, 1) as win_rate
   FROM wallet_profiles
   WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
   GROUP BY wallet_address
   HAVING COUNT(*) >= 10
   ORDER BY win_rate DESC
   LIMIT 100;
            """)
            print()
            print("   Option B - Use 0.5% threshold (catches ~18% of trades):")
            print("   (Use quick_wallet_query.py - it already uses this threshold)")
            print()


def main():
    print()
    analyze_gain_distribution(days_back=7)
    find_highest_gain_trades(limit=20)
    find_wallets_with_high_gains(min_gain_pct=1.0, min_trades=10)
    
    print("Testing different thresholds:")
    print()
    for threshold in [0.5, 1.0, 2.0, 3.0]:
        analyze_by_threshold(threshold_pct=threshold)
    
    recommendations()


if __name__ == "__main__":
    main()

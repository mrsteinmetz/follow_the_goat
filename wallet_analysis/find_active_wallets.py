"""
Find ACTIVE Wallets with Good Historical Performance
=====================================================

This script finds wallets that:
1. Have good historical performance (from wallet_profiles)
2. Are STILL ACTIVELY TRADING (recent trades in sol_stablecoin_trades)

This is crucial because we need to follow their FUTURE trades, not just analyze past performance.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def find_active_high_performers(min_historical_trades=10, min_gain_threshold=0.5, hours_back=24, recent_activity_hours=1):
    """
    Find wallets with good historical performance that are STILL ACTIVELY TRADING.
    
    Args:
        min_historical_trades: Minimum historical trades in wallet_profiles
        min_gain_threshold: Minimum gain % threshold for "wins"
        hours_back: Lookback for historical performance
        recent_activity_hours: How recent should their last trade be?
    """
    print("=" * 80)
    print(f"ACTIVE HIGH-PERFORMING WALLETS")
    print(f"Historical lookback: {hours_back}h | Recent activity: Last {recent_activity_hours}h")
    print("=" * 80)
    print()
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=recent_activity_hours)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Find wallets with good performance AND recent activity
            cursor.execute("""
                WITH historical_performance AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as historical_trades,
                        ROUND(AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as avg_gain,
                        ROUND((SUM(CASE WHEN highest_price_reached > trade_entry_price * (1 + %s/100) 
                            THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate,
                        MAX(trade_timestamp) as last_profile_trade
                    FROM wallet_profiles
                    WHERE trade_timestamp >= %s
                    AND threshold = 0.3
                    GROUP BY wallet_address
                    HAVING COUNT(*) >= %s
                ),
                recent_activity AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as recent_trades,
                        MAX(trade_timestamp) as last_trade,
                        MIN(trade_timestamp) as first_recent_trade
                    FROM sol_stablecoin_trades
                    WHERE direction = 'buy'
                    AND trade_timestamp >= %s
                    GROUP BY wallet_address
                )
                SELECT 
                    hp.wallet_address,
                    hp.historical_trades,
                    hp.avg_gain,
                    hp.win_rate,
                    COALESCE(ra.recent_trades, 0) as recent_trades,
                    ra.last_trade,
                    hp.last_profile_trade,
                    ROUND((hp.historical_trades * hp.avg_gain * hp.win_rate / 100)::numeric, 2) as score
                FROM historical_performance hp
                LEFT JOIN recent_activity ra ON hp.wallet_address = ra.wallet_address
                ORDER BY 
                    CASE WHEN ra.recent_trades > 0 THEN 1 ELSE 2 END,  -- Active wallets first
                    score DESC
                LIMIT 100
            """, [min_gain_threshold, cutoff, min_historical_trades, recent_cutoff])
            
            results = cursor.fetchall()
            
            # Split into active and inactive
            active_wallets = [r for r in results if r['recent_trades'] > 0]
            inactive_wallets = [r for r in results if r['recent_trades'] == 0]
            
            print(f"ACTIVE WALLETS (trading in last {recent_activity_hours}h): {len(active_wallets)}")
            print("-" * 80)
            if active_wallets:
                print(f"{'Wallet':<44} {'Hist':>5} {'Avg%':>7} {'Win%':>6} {'Recent':>7} {'Score':>8}")
                print("-" * 80)
                for r in active_wallets[:20]:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['historical_trades']:5} "
                        f"{r['avg_gain']:7.4f} "
                        f"{r['win_rate']:6.1f} "
                        f"{r['recent_trades']:7} "
                        f"{r['score']:8.2f}"
                    )
                if len(active_wallets) > 20:
                    print(f"... and {len(active_wallets) - 20} more")
            else:
                print("❌ NO ACTIVE WALLETS FOUND")
            
            print()
            print(f"INACTIVE WALLETS (no trades in last {recent_activity_hours}h): {len(inactive_wallets)}")
            if len(inactive_wallets) > 0:
                print(f"  (These had good historical performance but stopped trading)")
            print()
            
            return active_wallets, inactive_wallets


def analyze_activity_by_timeframe():
    """Check how many high-performing wallets are active at different timeframes."""
    print("=" * 80)
    print("ACTIVITY ANALYSIS - How many good wallets are still trading?")
    print("=" * 80)
    print()
    
    timeframes = [
        (1, "Last 1 hour"),
        (2, "Last 2 hours"),
        (6, "Last 6 hours"),
        (12, "Last 12 hours"),
        (24, "Last 24 hours"),
    ]
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get wallets with good historical performance
            cursor.execute("""
                SELECT wallet_address
                FROM wallet_profiles
                WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                AND threshold = 0.3
                GROUP BY wallet_address
                HAVING COUNT(*) >= 10
                AND AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) > 0.3
            """)
            
            good_wallets = [r['wallet_address'] for r in cursor.fetchall()]
            total_good_wallets = len(good_wallets)
            
            print(f"Total wallets with good historical performance (24h): {total_good_wallets:,}")
            print()
            print(f"{'Timeframe':<20} {'Active Wallets':>15} {'% Still Active':>15}")
            print("-" * 50)
            
            for hours, label in timeframes:
                cursor.execute("""
                    SELECT COUNT(DISTINCT wallet_address) as count
                    FROM sol_stablecoin_trades
                    WHERE wallet_address = ANY(%s)
                    AND direction = 'buy'
                    AND trade_timestamp >= NOW() - INTERVAL '%s hours'
                """, [good_wallets, hours])
                
                active_count = cursor.fetchone()['count']
                pct = (active_count / total_good_wallets * 100) if total_good_wallets > 0 else 0
                
                print(f"{label:<20} {active_count:>15,} {pct:>14.1f}%")
            
            print()


def find_consistently_active_wallets(min_trades_per_hour=5, hours_back=24):
    """Find wallets that are consistently active (not just occasional traders)."""
    print("=" * 80)
    print(f"CONSISTENTLY ACTIVE WALLETS (≥{min_trades_per_hour} trades/hour)")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH historical_performance AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as historical_trades,
                        ROUND(AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as avg_gain,
                        ROUND((SUM(CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '%s hours'
                    AND threshold = 0.3
                    GROUP BY wallet_address
                    HAVING COUNT(*) >= 10
                ),
                recent_activity AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as recent_trades,
                        COUNT(*) / %s::float as trades_per_hour
                    FROM sol_stablecoin_trades
                    WHERE direction = 'buy'
                    AND trade_timestamp >= NOW() - INTERVAL '%s hours'
                    GROUP BY wallet_address
                    HAVING COUNT(*) / %s::float >= %s
                )
                SELECT 
                    hp.wallet_address,
                    hp.historical_trades,
                    hp.avg_gain,
                    hp.win_rate,
                    ra.recent_trades,
                    ROUND(ra.trades_per_hour::numeric, 1) as trades_per_hour,
                    ROUND((hp.historical_trades * hp.avg_gain * hp.win_rate / 100)::numeric, 2) as score
                FROM historical_performance hp
                INNER JOIN recent_activity ra ON hp.wallet_address = ra.wallet_address
                ORDER BY score DESC
                LIMIT 50
            """ % (hours_back, hours_back, hours_back, hours_back, min_trades_per_hour))
            
            results = cursor.fetchall()
            
            if results:
                print(f"Found {len(results)} consistently active wallets")
                print()
                print(f"{'Wallet':<44} {'Hist':>5} {'Avg%':>7} {'Win%':>6} {'Recent':>7} {'Per/hr':>7} {'Score':>8}")
                print("-" * 80)
                for r in results[:20]:
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{r['historical_trades']:5} "
                        f"{r['avg_gain']:7.4f} "
                        f"{r['win_rate']:6.1f} "
                        f"{r['recent_trades']:7} "
                        f"{r['trades_per_hour']:7.1f} "
                        f"{r['score']:8.2f}"
                    )
                if len(results) > 20:
                    print(f"... and {len(results) - 20} more")
            else:
                print(f"❌ NO wallets found with ≥{min_trades_per_hour} trades/hour")
                print("   Try lowering the min_trades_per_hour threshold")
            
            print()
            return results


def get_follow_ready_wallets(limit=50):
    """
    Get wallets that are ready to follow RIGHT NOW.
    
    Criteria:
    - Good historical performance (last 24h)
    - Active in last hour
    - Minimum trade frequency
    """
    print("=" * 80)
    print(f"TOP {limit} WALLETS TO FOLLOW RIGHT NOW")
    print("=" * 80)
    print()
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH historical_performance AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as historical_trades,
                        ROUND(AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 4) as avg_gain,
                        ROUND((SUM(CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate,
                        ROUND((COUNT(*) * AVG(((highest_price_reached - trade_entry_price) / trade_entry_price) * 100) * 
                               (SUM(CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                                   THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100) / 100)::numeric, 2) as score
                    FROM wallet_profiles
                    WHERE trade_timestamp >= NOW() - INTERVAL '24 hours'
                    AND threshold = 0.3
                    GROUP BY wallet_address
                    HAVING COUNT(*) >= 10
                ),
                recent_activity AS (
                    SELECT 
                        wallet_address,
                        COUNT(*) as trades_last_hour,
                        MAX(trade_timestamp) as last_trade
                    FROM sol_stablecoin_trades
                    WHERE direction = 'buy'
                    AND trade_timestamp >= NOW() - INTERVAL '1 hour'
                    GROUP BY wallet_address
                )
                SELECT 
                    hp.wallet_address,
                    hp.historical_trades,
                    hp.avg_gain,
                    hp.win_rate,
                    ra.trades_last_hour,
                    ra.last_trade,
                    hp.score
                FROM historical_performance hp
                INNER JOIN recent_activity ra ON hp.wallet_address = ra.wallet_address
                WHERE ra.trades_last_hour >= 1
                ORDER BY hp.score DESC
                LIMIT %s
            """, [limit])
            
            results = cursor.fetchall()
            
            if results:
                print(f"✓ {len(results)} wallets ready to follow")
                print()
                print(f"{'Wallet':<44} {'24h Perf':>10} {'Last 1h':>8} {'Score':>8}")
                print("-" * 80)
                for r in results:
                    perf = f"{r['historical_trades']}t/{r['avg_gain']:.2f}%/{r['win_rate']:.0f}%"
                    last_trade_ago = (datetime.now(timezone.utc) - r['last_trade']).total_seconds() / 60
                    last_trade_str = f"{int(last_trade_ago)}m ago"
                    
                    print(
                        f"{r['wallet_address'][:42]:44} "
                        f"{perf:>10} "
                        f"{last_trade_str:>8} "
                        f"{r['score']:8.2f}"
                    )
                
                print()
                print("These wallets can be added to your follow_the_goat play configuration!")
                print()
                
                # Export list
                print("WALLET ADDRESSES (copy to your config):")
                print("-" * 80)
                for r in results[:10]:
                    print(f"  '{r['wallet_address']}',")
                print()
                
            else:
                print("❌ NO wallets are currently active")
                print("   Wait for more trading activity or increase the lookback period")
            
            print()
            return results


def main():
    print()
    
    # Analysis 1: Find active wallets with different thresholds
    print("ANALYSIS 1: Active vs Inactive Wallets")
    print()
    active, inactive = find_active_high_performers(
        min_historical_trades=10,
        min_gain_threshold=0.5,
        hours_back=24,
        recent_activity_hours=1
    )
    
    # Analysis 2: Activity by timeframe
    print()
    analyze_activity_by_timeframe()
    
    # Analysis 3: Consistently active traders
    print()
    find_consistently_active_wallets(min_trades_per_hour=5, hours_back=24)
    
    # Analysis 4: Ready to follow NOW
    print()
    wallets_to_follow = get_follow_ready_wallets(limit=50)
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()
    print(f"Active wallets (trading in last hour): {len(active)}")
    print(f"Inactive wallets (stopped trading): {len(inactive)}")
    print(f"Ready to follow NOW: {len(wallets_to_follow) if wallets_to_follow else 0}")
    print()
    print("💡 TIP: Use the 'Ready to follow' list for your play configuration")
    print("   These wallets have good historical performance AND are currently active")
    print()


if __name__ == "__main__":
    main()

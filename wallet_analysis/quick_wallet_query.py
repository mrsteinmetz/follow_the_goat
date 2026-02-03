"""
Quick Wallet Query - Simple CLI for Finding Top Wallets
========================================================

Usage:
    python quick_wallet_query.py                    # Show top 10 wallets
    python quick_wallet_query.py 20                 # Show top 20 wallets
    python quick_wallet_query.py <wallet_address>  # Show specific wallet details
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def quick_top_wallets(limit: int = 10, hours: int = 24):
    """Show top wallets by score (frequency × potential × win rate)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        wallet_address,
                        COUNT(*) as trades,
                        ROUND(AVG(
                            ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                        )::numeric, 2) as avg_gain_pct,
                        ROUND((SUM(
                            CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END
                        )::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate_pct,
                        MAX(trade_timestamp) as last_trade
                    FROM wallet_profiles
                    WHERE threshold = 0.3
                    AND trade_timestamp >= %s
                    GROUP BY wallet_address
                    HAVING COUNT(*) >= 5
                    ORDER BY (COUNT(*) * AVG(
                        ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                    ) * (SUM(
                        CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                        THEN 1 ELSE 0 END
                    )::FLOAT / COUNT(*) * 100) / 100) DESC
                    LIMIT %s
                """, [cutoff, limit])
                
                return cursor.fetchall()
    except Exception as e:
        print(f"Error: {e}")
        return []


def quick_wallet_details(wallet: str, hours: int = 24):
    """Show recent trades for a specific wallet."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get overall stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as trades,
                        ROUND(AVG(
                            ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                        )::numeric, 2) as avg_gain_pct,
                        ROUND((SUM(
                            CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END
                        )::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate_pct,
                        ROUND(AVG(stablecoin_amount)::numeric, 2) as avg_size,
                        MAX(trade_timestamp) as last_trade
                    FROM wallet_profiles
                    WHERE wallet_address = %s
                    AND threshold = 0.3
                    AND trade_timestamp >= %s
                """, [wallet, cutoff])
                
                stats = cursor.fetchone()
                
                # Get recent trades
                cursor.execute("""
                    SELECT 
                        trade_timestamp,
                        ROUND(trade_entry_price::numeric, 2) as entry,
                        ROUND(highest_price_reached::numeric, 2) as peak,
                        ROUND(
                            (((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 
                            2
                        ) as gain_pct,
                        ROUND(stablecoin_amount::numeric, 2) as size
                    FROM wallet_profiles
                    WHERE wallet_address = %s
                    AND threshold = 0.3
                    AND trade_timestamp >= %s
                    ORDER BY trade_timestamp DESC
                    LIMIT 20
                """, [wallet, cutoff])
                
                trades = cursor.fetchall()
                
                return stats, trades
    except Exception as e:
        print(f"Error: {e}")
        return None, []


def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        
        # Check if argument looks like a wallet address (long alphanumeric)
        if len(arg) > 30:
            # Show wallet details
            print(f"Wallet Details: {arg}")
            print("=" * 80)
            
            stats, trades = quick_wallet_details(arg, hours=24)
            
            if stats:
                print(f"\nLast 24h Stats:")
                print(f"  Trades:      {stats['trades']}")
                print(f"  Avg Gain:    {stats['avg_gain_pct']}%")
                print(f"  Win Rate:    {stats['win_rate_pct']}%")
                print(f"  Avg Size:    ${stats['avg_size']}")
                print(f"  Last Trade:  {stats['last_trade']}")
                
                if trades:
                    print(f"\nRecent Trades:")
                    print(f"{'Time':<20} {'Entry':>8} {'Peak':>8} {'Gain%':>8} {'Size':>10}")
                    print("-" * 80)
                    for t in trades:
                        print(
                            f"{str(t['trade_timestamp'])[:19]:20} "
                            f"{t['entry']:8.2f} "
                            f"{t['peak']:8.2f} "
                            f"{t['gain_pct']:8.2f} "
                            f"${t['size']:9.2f}"
                        )
            else:
                print("No data found for this wallet")
        else:
            # Treat as limit number
            try:
                limit = int(arg)
                print(f"Top {limit} Wallets (Last 24h)")
                print("=" * 80)
                
                results = quick_top_wallets(limit=limit, hours=24)
                
                if results:
                    print(f"{'Wallet':<44} {'Trades':>6} {'Avg%':>7} {'Win%':>6}")
                    print("-" * 80)
                    for r in results:
                        print(
                            f"{r['wallet_address'][:42]:44} "
                            f"{r['trades']:6} "
                            f"{r['avg_gain_pct']:7.2f} "
                            f"{r['win_rate_pct']:6.1f}"
                        )
                else:
                    print("No results found")
            except ValueError:
                print(f"Invalid argument: {arg}")
                print("Usage: python quick_wallet_query.py [limit|wallet_address]")
    else:
        # Default: show top 10
        print("Top 10 Wallets (Last 24h)")
        print("=" * 80)
        
        results = quick_top_wallets(limit=10, hours=24)
        
        if results:
            print(f"{'Wallet':<44} {'Trades':>6} {'Avg%':>7} {'Win%':>6}")
            print("-" * 80)
            for r in results:
                print(
                    f"{r['wallet_address'][:42]:44} "
                    f"{r['trades']:6} "
                    f"{r['avg_gain_pct']:7.2f} "
                    f"{r['win_rate_pct']:6.1f}"
                )
            print()
            print("Tip: Run with a number to see more (e.g., python quick_wallet_query.py 20)")
            print("     Run with a wallet address to see details")
        else:
            print("No results found")


if __name__ == "__main__":
    main()

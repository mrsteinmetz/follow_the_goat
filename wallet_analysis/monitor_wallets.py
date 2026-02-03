"""
Monitor Top Wallets - Real-time Tracking
=========================================

This script monitors the top-performing wallets and alerts when they make new trades.

Usage:
    python3 monitor_wallets.py                    # Monitor top 10 wallets
    python3 monitor_wallets.py --top 20           # Monitor top 20 wallets
    python3 monitor_wallets.py --wallet ABC123... # Monitor specific wallet
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Set, Dict, List

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def get_top_wallets(limit: int = 10, hours: int = 24) -> List[str]:
    """Get list of top performing wallet addresses."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        wallet_address,
                        COUNT(*) as trades,
                        AVG(
                            ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                        ) as avg_gain
                    FROM wallet_profiles
                    WHERE threshold = 0.3
                    AND trade_timestamp >= %s
                    GROUP BY wallet_address
                    HAVING COUNT(*) >= 5
                    ORDER BY (COUNT(*) * AVG(
                        ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                    )) DESC
                    LIMIT %s
                """, [cutoff, limit])
                
                results = cursor.fetchall()
                return [r['wallet_address'] for r in results]
    except Exception as e:
        print(f"Error getting top wallets: {e}")
        return []


def get_latest_trades(wallet_addresses: List[str], since_timestamp: datetime = None) -> List[Dict]:
    """Get latest trades from sol_stablecoin_trades for specified wallets."""
    if not wallet_addresses:
        return []
    
    if since_timestamp is None:
        since_timestamp = datetime.now(timezone.utc) - timedelta(minutes=5)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Use ANY for array comparison
                cursor.execute("""
                    SELECT 
                        wallet_address,
                        trade_timestamp,
                        direction,
                        price,
                        stablecoin_amount,
                        transaction_id
                    FROM sol_stablecoin_trades
                    WHERE wallet_address = ANY(%s)
                    AND direction = 'buy'
                    AND trade_timestamp > %s
                    ORDER BY trade_timestamp DESC
                    LIMIT 100
                """, [wallet_addresses, since_timestamp])
                
                return cursor.fetchall()
    except Exception as e:
        print(f"Error getting trades: {e}")
        return []


def get_wallet_stats(wallet_address: str, hours: int = 24) -> Dict:
    """Get current stats for a wallet."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as trades,
                        ROUND(AVG(
                            ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                        )::numeric, 2) as avg_gain_pct,
                        ROUND((SUM(
                            CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                            THEN 1 ELSE 0 END
                        )::FLOAT / COUNT(*) * 100)::numeric, 1) as win_rate_pct
                    FROM wallet_profiles
                    WHERE wallet_address = %s
                    AND threshold = 0.3
                    AND trade_timestamp >= %s
                """, [wallet_address, cutoff])
                
                result = cursor.fetchone()
                return result if result else {}
    except Exception as e:
        print(f"Error getting wallet stats: {e}")
        return {}


def format_alert(trade: Dict, stats: Dict) -> str:
    """Format a trade alert with wallet stats."""
    wallet = trade['wallet_address'][:10] + "..." + trade['wallet_address'][-6:]
    timestamp = str(trade['trade_timestamp'])[:19]
    price = trade['price']
    size = trade['stablecoin_amount']
    
    alert = f"[{timestamp}] {wallet} bought ${size:.2f} @ ${price:.2f}"
    
    if stats:
        alert += f" | Stats: {stats['trades']} trades, {stats['avg_gain_pct']}% avg, {stats['win_rate_pct']}% win"
    
    return alert


def monitor_continuous(wallet_addresses: List[str], interval_seconds: int = 30):
    """Monitor wallets continuously for new trades."""
    print(f"Monitoring {len(wallet_addresses)} wallets...")
    print(f"Checking every {interval_seconds} seconds")
    print("=" * 80)
    
    seen_trades: Set[int] = set()  # Track trade IDs we've already alerted on
    last_check = datetime.now(timezone.utc) - timedelta(minutes=1)
    
    try:
        while True:
            # Get new trades since last check
            trades = get_latest_trades(wallet_addresses, since_timestamp=last_check)
            
            for trade in trades:
                trade_id = f"{trade['wallet_address']}_{trade['trade_timestamp']}_{trade['transaction_id']}"
                
                if trade_id not in seen_trades:
                    seen_trades.add(trade_id)
                    
                    # Get wallet stats
                    stats = get_wallet_stats(trade['wallet_address'])
                    
                    # Print alert
                    print(format_alert(trade, stats))
            
            last_check = datetime.now(timezone.utc)
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped")


def show_current_status(wallet_addresses: List[str]):
    """Show current stats for monitored wallets."""
    print(f"Current Status of {len(wallet_addresses)} Wallets (Last 24h)")
    print("=" * 80)
    print(f"{'Wallet':<44} {'Trades':>6} {'Avg%':>7} {'Win%':>6}")
    print("-" * 80)
    
    for wallet in wallet_addresses:
        stats = get_wallet_stats(wallet, hours=24)
        if stats and stats['trades'] > 0:
            print(
                f"{wallet[:42]:44} "
                f"{stats['trades']:6} "
                f"{stats['avg_gain_pct']:7.2f} "
                f"{stats['win_rate_pct']:6.1f}"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Monitor top-performing wallets for new trades",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor top 10 wallets
  python3 monitor_wallets.py
  
  # Monitor top 20 wallets
  python3 monitor_wallets.py --top 20
  
  # Monitor specific wallet
  python3 monitor_wallets.py --wallet ABC123...XYZ
  
  # Show status only (no continuous monitoring)
  python3 monitor_wallets.py --status-only
  
  # Change check interval
  python3 monitor_wallets.py --interval 60
        """
    )
    
    parser.add_argument('--top', type=int, default=10,
                       help='Monitor top N wallets (default: 10)')
    parser.add_argument('--wallet', type=str, action='append',
                       help='Monitor specific wallet (can specify multiple times)')
    parser.add_argument('--interval', type=int, default=30,
                       help='Check interval in seconds (default: 30)')
    parser.add_argument('--status-only', action='store_true',
                       help='Show current status and exit (no monitoring)')
    parser.add_argument('--lookback', type=int, default=24,
                       help='Lookback hours for selecting top wallets (default: 24)')
    
    args = parser.parse_args()
    
    # Determine which wallets to monitor
    if args.wallet:
        wallet_addresses = args.wallet
        print(f"Monitoring {len(wallet_addresses)} specified wallet(s)")
    else:
        print(f"Finding top {args.top} wallets from last {args.lookback}h...")
        wallet_addresses = get_top_wallets(limit=args.top, hours=args.lookback)
        print(f"Selected {len(wallet_addresses)} top wallets")
    
    if not wallet_addresses:
        print("No wallets to monitor")
        return
    
    # Show current status
    show_current_status(wallet_addresses)
    print()
    
    if not args.status_only:
        # Start continuous monitoring
        print("Starting real-time monitoring (Ctrl+C to stop)...")
        print()
        monitor_continuous(wallet_addresses, interval_seconds=args.interval)


if __name__ == "__main__":
    main()

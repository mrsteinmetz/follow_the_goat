"""
Advanced Wallet Filtering - Find Wallets with Specific Characteristics
=======================================================================

This script allows you to filter wallets based on multiple criteria:
- Minimum/maximum trade count
- Minimum average potential gain %
- Minimum win rate %
- Entry timing preferences (early, mid, late)
- Minimum trade size

Usage:
    python3 advanced_wallet_filter.py --min-trades 10 --min-gain 2.0 --min-winrate 70
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def filter_wallets(
    min_trades: int = 5,
    max_trades: Optional[int] = None,
    min_avg_gain: Optional[float] = None,
    max_avg_gain: Optional[float] = None,
    min_win_rate: Optional[float] = None,
    max_win_rate: Optional[float] = None,
    min_entry_timing: Optional[float] = None,  # 0-100 (early to late)
    max_entry_timing: Optional[float] = None,
    min_trade_size: Optional[float] = None,
    max_trade_size: Optional[float] = None,
    lookback_hours: int = 24,
    threshold: float = 0.3,
    limit: int = 50
):
    """
    Filter wallets based on multiple criteria.
    
    Args:
        min_trades: Minimum number of trades (default: 5)
        max_trades: Maximum number of trades (optional)
        min_avg_gain: Minimum average potential gain % (optional)
        max_avg_gain: Maximum average potential gain % (optional)
        min_win_rate: Minimum win rate % (optional)
        max_win_rate: Maximum win rate % (optional)
        min_entry_timing: Minimum entry timing % - 0=start, 100=end (optional)
        max_entry_timing: Maximum entry timing % (optional)
        min_trade_size: Minimum average trade size in USD (optional)
        max_trade_size: Maximum average trade size in USD (optional)
        lookback_hours: How far back to look (default: 24)
        threshold: Price cycle threshold (default: 0.3)
        limit: Maximum results to return (default: 50)
    
    Returns:
        List of matching wallets with their stats
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    # Build WHERE conditions dynamically
    conditions = ["threshold = %s", "trade_timestamp >= %s"]
    having_conditions = ["COUNT(*) >= %s"]
    params = [float(threshold), cutoff]
    having_params = [min_trades]
    
    if max_trades:
        having_conditions.append("COUNT(*) <= %s")
        having_params.append(max_trades)
    
    # Build the query
    query = f"""
        WITH wallet_stats AS (
            SELECT 
                wallet_address,
                COUNT(*) as trade_count,
                AVG(
                    ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                ) as avg_gain_pct,
                SUM(
                    CASE WHEN highest_price_reached > trade_entry_price * 1.005 
                    THEN 1 ELSE 0 END
                )::FLOAT / COUNT(*) * 100 as win_rate_pct,
                AVG(
                    EXTRACT(EPOCH FROM (trade_timestamp - price_cycle_start_time)) / 
                    EXTRACT(EPOCH FROM (price_cycle_end_time - price_cycle_start_time))
                ) * 100 as avg_entry_timing_pct,
                AVG(stablecoin_amount) as avg_trade_size,
                MAX(trade_timestamp) as last_trade,
                MIN(trade_timestamp) as first_trade
            FROM wallet_profiles
            WHERE {' AND '.join(conditions)}
            GROUP BY wallet_address
            HAVING {' AND '.join(having_conditions)}
        )
        SELECT 
            wallet_address,
            trade_count,
            ROUND(avg_gain_pct::numeric, 2) as avg_gain_pct,
            ROUND(win_rate_pct::numeric, 1) as win_rate_pct,
            ROUND(avg_entry_timing_pct::numeric, 1) as avg_entry_timing_pct,
            ROUND(avg_trade_size::numeric, 2) as avg_trade_size,
            last_trade,
            first_trade
        FROM wallet_stats
        WHERE 1=1
    """
    
    # Add filters for computed metrics
    if min_avg_gain is not None:
        query += " AND avg_gain_pct >= %s"
        params.append(min_avg_gain)
    if max_avg_gain is not None:
        query += " AND avg_gain_pct <= %s"
        params.append(max_avg_gain)
    if min_win_rate is not None:
        query += " AND win_rate_pct >= %s"
        params.append(min_win_rate)
    if max_win_rate is not None:
        query += " AND win_rate_pct <= %s"
        params.append(max_win_rate)
    if min_entry_timing is not None:
        query += " AND avg_entry_timing_pct >= %s"
        params.append(min_entry_timing)
    if max_entry_timing is not None:
        query += " AND avg_entry_timing_pct <= %s"
        params.append(max_entry_timing)
    if min_trade_size is not None:
        query += " AND avg_trade_size >= %s"
        params.append(min_trade_size)
    if max_trade_size is not None:
        query += " AND avg_trade_size <= %s"
        params.append(max_trade_size)
    
    # Order by score (trades × gain × winrate)
    query += """
        ORDER BY (trade_count * avg_gain_pct * win_rate_pct / 100) DESC
        LIMIT %s
    """
    params.append(limit)
    
    # Combine all params
    all_params = params + having_params
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, all_params)
                return cursor.fetchall()
    except Exception as e:
        print(f"Query error: {e}")
        return []


def export_to_csv(results, filename: str = "wallet_analysis.csv"):
    """Export results to CSV file."""
    if not results:
        print("No results to export")
        return
    
    import csv
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Exported {len(results)} wallets to {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Find wallets with specific trading characteristics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find aggressive traders (many trades, high gains)
  python3 advanced_wallet_filter.py --min-trades 15 --min-gain 2.5
  
  # Find conservative winners (high win rate, moderate gains)
  python3 advanced_wallet_filter.py --min-trades 10 --min-winrate 80 --min-gain 1.0 --max-gain 3.0
  
  # Find early entry specialists
  python3 advanced_wallet_filter.py --min-trades 8 --max-entry-timing 20 --min-gain 1.5
  
  # Find whales (large trade sizes)
  python3 advanced_wallet_filter.py --min-trade-size 1000 --min-trades 5
  
  # Export results to CSV
  python3 advanced_wallet_filter.py --min-trades 10 --min-gain 2.0 --export results.csv
        """
    )
    
    parser.add_argument('--min-trades', type=int, default=5,
                       help='Minimum number of trades (default: 5)')
    parser.add_argument('--max-trades', type=int,
                       help='Maximum number of trades')
    parser.add_argument('--min-gain', type=float,
                       help='Minimum average potential gain %%')
    parser.add_argument('--max-gain', type=float,
                       help='Maximum average potential gain %%')
    parser.add_argument('--min-winrate', type=float,
                       help='Minimum win rate %%')
    parser.add_argument('--max-winrate', type=float,
                       help='Maximum win rate %%')
    parser.add_argument('--min-entry-timing', type=float,
                       help='Minimum entry timing %% (0=early, 100=late)')
    parser.add_argument('--max-entry-timing', type=float,
                       help='Maximum entry timing %% (0=early, 100=late)')
    parser.add_argument('--min-trade-size', type=float,
                       help='Minimum average trade size in USD')
    parser.add_argument('--max-trade-size', type=float,
                       help='Maximum average trade size in USD')
    parser.add_argument('--hours', type=int, default=24,
                       help='Lookback period in hours (default: 24)')
    parser.add_argument('--threshold', type=float, default=0.3,
                       help='Price cycle threshold (default: 0.3)')
    parser.add_argument('--limit', type=int, default=50,
                       help='Maximum results to return (default: 50)')
    parser.add_argument('--export', type=str,
                       help='Export results to CSV file')
    
    args = parser.parse_args()
    
    # Build filter description
    filters = []
    if args.min_trades:
        filters.append(f"trades≥{args.min_trades}")
    if args.max_trades:
        filters.append(f"trades≤{args.max_trades}")
    if args.min_gain:
        filters.append(f"gain≥{args.min_gain}%")
    if args.max_gain:
        filters.append(f"gain≤{args.max_gain}%")
    if args.min_winrate:
        filters.append(f"winrate≥{args.min_winrate}%")
    if args.max_winrate:
        filters.append(f"winrate≤{args.max_winrate}%")
    if args.min_entry_timing is not None:
        filters.append(f"entry≥{args.min_entry_timing}%")
    if args.max_entry_timing is not None:
        filters.append(f"entry≤{args.max_entry_timing}%")
    if args.min_trade_size:
        filters.append(f"size≥${args.min_trade_size}")
    if args.max_trade_size:
        filters.append(f"size≤${args.max_trade_size}")
    
    print("=" * 80)
    print(f"WALLET FILTER - Last {args.hours}h")
    print(f"Filters: {', '.join(filters) if filters else 'None'}")
    print("=" * 80)
    print()
    
    results = filter_wallets(
        min_trades=args.min_trades,
        max_trades=args.max_trades,
        min_avg_gain=args.min_gain,
        max_avg_gain=args.max_gain,
        min_win_rate=args.min_winrate,
        max_win_rate=args.max_winrate,
        min_entry_timing=args.min_entry_timing,
        max_entry_timing=args.max_entry_timing,
        min_trade_size=args.min_trade_size,
        max_trade_size=args.max_trade_size,
        lookback_hours=args.hours,
        threshold=args.threshold,
        limit=args.limit
    )
    
    if results:
        print(f"{'Wallet':<44} {'Trades':>6} {'Gain%':>7} {'Win%':>6} {'Entry%':>7} {'AvgSize':>10}")
        print("-" * 80)
        for r in results:
            print(
                f"{r['wallet_address'][:42]:44} "
                f"{r['trade_count']:6} "
                f"{r['avg_gain_pct']:7.2f} "
                f"{r['win_rate_pct']:6.1f} "
                f"{r['avg_entry_timing_pct']:7.1f} "
                f"${r['avg_trade_size']:9.2f}"
            )
        print()
        print(f"Total: {len(results)} wallets")
        
        if args.export:
            export_to_csv(results, args.export)
    else:
        print("No wallets match the specified criteria")
        print()
        print("Try relaxing some filters (e.g., lower --min-trades or --min-gain)")


if __name__ == "__main__":
    main()

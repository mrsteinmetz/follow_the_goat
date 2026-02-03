"""
Find Wallets with Frequent Buys and High Potential Gains
==========================================================

This script analyzes wallet_profiles to find wallets that:
1. Buy frequently (high trade count)
2. Have high average potential gains
3. Time their entries well (buy near cycle starts)

The wallet_profiles table already contains pre-computed data joining:
- sol_stablecoin_trades (wallet buy transactions)
- cycle_tracker (completed price cycles)
- prices (to get actual entry prices)

Each profile record shows:
- When a wallet bought
- What cycle the trade was in
- The potential gain (highest_price_reached vs trade_entry_price)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def find_high_potential_wallets(
    min_trades: int = 10,
    min_avg_potential: float = 1.5,  # 1.5% average potential gain
    lookback_hours: int = 24,
    threshold: float = 0.3
) -> List[Dict[str, Any]]:
    """
    Find wallets with frequent, high-potential trades.
    
    Args:
        min_trades: Minimum number of trades in the period
        min_avg_potential: Minimum average potential gain % (e.g., 1.5 for 1.5%)
        lookback_hours: How far back to look (default 24 hours)
        threshold: Which price cycle threshold to analyze (0.3 = 0.3% cycles)
    
    Returns:
        List of dicts with wallet stats
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Query to find high-performing wallets
                # Calculates potential gains as % from entry to cycle peak
                cursor.execute("""
                    WITH wallet_stats AS (
                        SELECT 
                            wallet_address,
                            COUNT(*) as trade_count,
                            -- Average potential gain % (entry to cycle peak)
                            AVG(
                                ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                            ) as avg_potential_pct,
                            -- Win rate: trades that could have been profitable
                            SUM(
                                CASE 
                                    WHEN highest_price_reached > trade_entry_price * 1.005 
                                    THEN 1 
                                    ELSE 0 
                                END
                            )::FLOAT / COUNT(*) * 100 as win_rate_pct,
                            -- Average entry timing (how early in cycle)
                            AVG(
                                EXTRACT(EPOCH FROM (trade_timestamp - price_cycle_start_time)) / 
                                EXTRACT(EPOCH FROM (price_cycle_end_time - price_cycle_start_time))
                            ) * 100 as avg_entry_timing_pct,
                            -- Recent activity
                            MAX(trade_timestamp) as last_trade,
                            -- Average trade size
                            AVG(stablecoin_amount) as avg_trade_size,
                            -- Best single trade potential
                            MAX(
                                ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                            ) as best_potential_pct
                        FROM wallet_profiles
                        WHERE threshold = %s
                        AND trade_timestamp >= %s
                        GROUP BY wallet_address
                    )
                    SELECT 
                        wallet_address,
                        trade_count,
                        ROUND(avg_potential_pct::numeric, 2) as avg_potential_pct,
                        ROUND(win_rate_pct::numeric, 1) as win_rate_pct,
                        ROUND(avg_entry_timing_pct::numeric, 1) as avg_entry_timing_pct,
                        last_trade,
                        ROUND(avg_trade_size::numeric, 2) as avg_trade_size,
                        ROUND(best_potential_pct::numeric, 2) as best_potential_pct,
                        -- Score: combines frequency, potential, and win rate
                        ROUND(
                            (trade_count * avg_potential_pct * win_rate_pct / 100)::numeric, 
                            2
                        ) as score
                    FROM wallet_stats
                    WHERE trade_count >= %s
                    AND avg_potential_pct >= %s
                    ORDER BY score DESC
                    LIMIT 50
                """, [float(threshold), cutoff_time, min_trades, min_avg_potential])
                
                results = cursor.fetchall()
                return results
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return []


def find_consistent_winners(
    min_trades: int = 5,
    min_win_rate: float = 70.0,  # 70% win rate
    lookback_hours: int = 24,
    threshold: float = 0.3
) -> List[Dict[str, Any]]:
    """
    Find wallets with consistently profitable trades (high win rate).
    
    Args:
        min_trades: Minimum number of trades
        min_win_rate: Minimum win rate % (e.g., 70 for 70%)
        lookback_hours: How far back to look
        threshold: Which price cycle threshold to analyze
    
    Returns:
        List of dicts with wallet stats
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    WITH wallet_performance AS (
                        SELECT 
                            wallet_address,
                            COUNT(*) as trade_count,
                            -- Win rate: % of trades with >0.5% potential gain
                            SUM(
                                CASE 
                                    WHEN highest_price_reached > trade_entry_price * 1.005 
                                    THEN 1 
                                    ELSE 0 
                                END
                            )::FLOAT / COUNT(*) * 100 as win_rate_pct,
                            -- Average potential of winning trades only
                            AVG(
                                CASE 
                                    WHEN highest_price_reached > trade_entry_price * 1.005 
                                    THEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                                    ELSE NULL
                                END
                            ) as avg_winner_pct,
                            -- Average potential of losing trades
                            AVG(
                                CASE 
                                    WHEN highest_price_reached <= trade_entry_price * 1.005 
                                    THEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                                    ELSE NULL
                                END
                            ) as avg_loser_pct,
                            MAX(trade_timestamp) as last_trade,
                            AVG(stablecoin_amount) as avg_trade_size
                        FROM wallet_profiles
                        WHERE threshold = %s
                        AND trade_timestamp >= %s
                        GROUP BY wallet_address
                    )
                    SELECT 
                        wallet_address,
                        trade_count,
                        ROUND(win_rate_pct::numeric, 1) as win_rate_pct,
                        ROUND(avg_winner_pct::numeric, 2) as avg_winner_pct,
                        ROUND(COALESCE(avg_loser_pct, 0)::numeric, 2) as avg_loser_pct,
                        last_trade,
                        ROUND(avg_trade_size::numeric, 2) as avg_trade_size
                    FROM wallet_performance
                    WHERE trade_count >= %s
                    AND win_rate_pct >= %s
                    ORDER BY win_rate_pct DESC, trade_count DESC
                    LIMIT 50
                """, [float(threshold), cutoff_time, min_trades, min_win_rate])
                
                results = cursor.fetchall()
                return results
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return []


def find_early_entry_wallets(
    min_trades: int = 5,
    max_entry_timing: float = 25.0,  # Enter in first 25% of cycle
    min_potential: float = 1.0,
    lookback_hours: int = 24,
    threshold: float = 0.3
) -> List[Dict[str, Any]]:
    """
    Find wallets that consistently enter early in price cycles.
    
    Args:
        min_trades: Minimum number of trades
        max_entry_timing: Max % into cycle (0=start, 100=end)
        min_potential: Minimum average potential gain %
        lookback_hours: How far back to look
        threshold: Which price cycle threshold to analyze
    
    Returns:
        List of dicts with wallet stats
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    WITH wallet_timing AS (
                        SELECT 
                            wallet_address,
                            COUNT(*) as trade_count,
                            -- Average entry timing (0=start, 100=end)
                            AVG(
                                EXTRACT(EPOCH FROM (trade_timestamp - price_cycle_start_time)) / 
                                EXTRACT(EPOCH FROM (price_cycle_end_time - price_cycle_start_time))
                            ) * 100 as avg_entry_timing_pct,
                            -- How early was earliest trade
                            MIN(
                                EXTRACT(EPOCH FROM (trade_timestamp - price_cycle_start_time)) / 
                                EXTRACT(EPOCH FROM (price_cycle_end_time - price_cycle_start_time))
                            ) * 100 as earliest_entry_pct,
                            -- Average potential
                            AVG(
                                ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                            ) as avg_potential_pct,
                            MAX(trade_timestamp) as last_trade,
                            AVG(stablecoin_amount) as avg_trade_size
                        FROM wallet_profiles
                        WHERE threshold = %s
                        AND trade_timestamp >= %s
                        GROUP BY wallet_address
                    )
                    SELECT 
                        wallet_address,
                        trade_count,
                        ROUND(avg_entry_timing_pct::numeric, 1) as avg_entry_timing_pct,
                        ROUND(earliest_entry_pct::numeric, 1) as earliest_entry_pct,
                        ROUND(avg_potential_pct::numeric, 2) as avg_potential_pct,
                        last_trade,
                        ROUND(avg_trade_size::numeric, 2) as avg_trade_size
                    FROM wallet_timing
                    WHERE trade_count >= %s
                    AND avg_entry_timing_pct <= %s
                    AND avg_potential_pct >= %s
                    ORDER BY avg_entry_timing_pct ASC, avg_potential_pct DESC
                    LIMIT 50
                """, [float(threshold), cutoff_time, min_trades, max_entry_timing, min_potential])
                
                results = cursor.fetchall()
                return results
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return []


def analyze_wallet_details(wallet_address: str, lookback_hours: int = 24, threshold: float = 0.3):
    """
    Get detailed trade history for a specific wallet.
    
    Args:
        wallet_address: The wallet to analyze
        lookback_hours: How far back to look
        threshold: Which price cycle threshold to analyze
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        trade_id,
                        trade_timestamp,
                        price_cycle,
                        trade_entry_price,
                        highest_price_reached,
                        lowest_price_reached,
                        stablecoin_amount,
                        -- Potential gain %
                        ROUND(
                            (((highest_price_reached - trade_entry_price) / trade_entry_price) * 100)::numeric, 
                            2
                        ) as potential_gain_pct,
                        -- Entry timing in cycle
                        ROUND(
                            (EXTRACT(EPOCH FROM (trade_timestamp - price_cycle_start_time)) / 
                            EXTRACT(EPOCH FROM (price_cycle_end_time - price_cycle_start_time)) * 100)::numeric,
                            1
                        ) as entry_timing_pct,
                        long_short
                    FROM wallet_profiles
                    WHERE wallet_address = %s
                    AND threshold = %s
                    AND trade_timestamp >= %s
                    ORDER BY trade_timestamp DESC
                    LIMIT 100
                """, [wallet_address, float(threshold), cutoff_time])
                
                results = cursor.fetchall()
                return results
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        return []


def main():
    """Run all wallet analysis queries."""
    print("=" * 80)
    print("WALLET ANALYSIS - Finding High-Potential Wallets")
    print("=" * 80)
    print()
    
    # Query 1: High potential wallets (frequent + high gains)
    print("1. HIGH-POTENTIAL WALLETS (Frequent trades + High average gains)")
    print("-" * 80)
    results = find_high_potential_wallets(
        min_trades=10,
        min_avg_potential=1.5,
        lookback_hours=24
    )
    
    if results:
        print(f"{'Wallet':<44} {'Trades':>6} {'Avg%':>7} {'Win%':>6} {'Entry%':>7} {'Score':>8}")
        print("-" * 80)
        for row in results[:20]:
            print(
                f"{row['wallet_address'][:42]:44} "
                f"{row['trade_count']:6} "
                f"{row['avg_potential_pct']:7.2f} "
                f"{row['win_rate_pct']:6.1f} "
                f"{row['avg_entry_timing_pct']:7.1f} "
                f"{row['score']:8.2f}"
            )
        print(f"\nTotal found: {len(results)}")
    else:
        print("No results found")
    
    print("\n")
    
    # Query 2: Consistent winners (high win rate)
    print("2. CONSISTENT WINNERS (High win rate)")
    print("-" * 80)
    results = find_consistent_winners(
        min_trades=5,
        min_win_rate=70.0,
        lookback_hours=24
    )
    
    if results:
        print(f"{'Wallet':<44} {'Trades':>6} {'Win%':>6} {'AvgWin%':>8} {'AvgLoss%':>9}")
        print("-" * 80)
        for row in results[:20]:
            print(
                f"{row['wallet_address'][:42]:44} "
                f"{row['trade_count']:6} "
                f"{row['win_rate_pct']:6.1f} "
                f"{row['avg_winner_pct']:8.2f} "
                f"{row['avg_loser_pct']:9.2f}"
            )
        print(f"\nTotal found: {len(results)}")
    else:
        print("No results found")
    
    print("\n")
    
    # Query 3: Early entry specialists
    print("3. EARLY ENTRY SPECIALISTS (Buy near cycle starts)")
    print("-" * 80)
    results = find_early_entry_wallets(
        min_trades=5,
        max_entry_timing=25.0,
        min_potential=1.0,
        lookback_hours=24
    )
    
    if results:
        print(f"{'Wallet':<44} {'Trades':>6} {'AvgEntry%':>10} {'Earliest%':>10} {'AvgGain%':>9}")
        print("-" * 80)
        for row in results[:20]:
            print(
                f"{row['wallet_address'][:42]:44} "
                f"{row['trade_count']:6} "
                f"{row['avg_entry_timing_pct']:10.1f} "
                f"{row['earliest_entry_pct']:10.1f} "
                f"{row['avg_potential_pct']:9.2f}"
            )
        print(f"\nTotal found: {len(results)}")
    else:
        print("No results found")
    
    print("\n")
    print("=" * 80)
    print("Analysis complete!")
    print()
    print("Column explanations:")
    print("  Trades: Number of buy trades in the period")
    print("  Avg%: Average potential gain % (entry to cycle peak)")
    print("  Win%: % of trades with >0.5% potential gain")
    print("  Entry%: Average timing in cycle (0%=start, 100%=end)")
    print("  Score: Combined metric (trades × avg% × win%)")
    print("  AvgWin%: Average gain on winning trades")
    print("  AvgLoss%: Average gain on losing trades")
    print("  Earliest%: Earliest entry timing observed")


if __name__ == "__main__":
    main()

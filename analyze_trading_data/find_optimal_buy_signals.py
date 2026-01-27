"""
Find Optimal Buy Signals Analysis
==================================
This script analyzes price cycles to find what filter values spike
at the optimal buy time (the bottom of a good cycle).

Goal: Identify 2-5 high-quality buy signals per day, avoiding bad trades.

Approach:
1. Find "good" cycles (0.3% threshold, 0.8%+ gain)
2. For each good cycle, find the exact lowest price point (optimal buy time)
3. Look at trades/buyins that happened around that time
4. Analyze which filter values were distinctive at the bottom
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
import logging
import json
from decimal import Decimal

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("optimal_buy_signals")

# Configuration
THRESHOLD = 0.3  # Price cycle threshold
MIN_GAIN = 0.8   # Minimum gain % to consider a "good" cycle
HOURS_LOOKBACK = 48  # Hours of data to analyze


def get_good_cycles(hours: int = HOURS_LOOKBACK, min_gain: float = MIN_GAIN) -> List[Dict]:
    """
    Get cycles that achieved the target gain.
    These represent successful buy opportunities.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    cycle_start_time,
                    cycle_end_time,
                    sequence_start_price,
                    highest_price_reached,
                    lowest_price_reached,
                    max_percent_increase,
                    max_percent_increase_from_lowest,
                    total_data_points
                FROM cycle_tracker
                WHERE threshold = %s
                  AND cycle_end_time IS NOT NULL
                  AND max_percent_increase >= %s
                  AND cycle_start_time >= NOW() - INTERVAL '%s hours'
                ORDER BY cycle_start_time DESC
            """, [THRESHOLD, min_gain, hours])
            return cursor.fetchall()


def get_price_data_for_cycle(cycle: Dict) -> List[Dict]:
    """
    Get all price points during a cycle to find the exact low.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, timestamp, price
                FROM prices
                WHERE timestamp >= %s
                  AND timestamp <= %s
                  AND token = 'SOL'
                ORDER BY timestamp ASC
            """, [cycle['cycle_start_time'], cycle['cycle_end_time']])
            return cursor.fetchall()


def find_cycle_bottom(prices: List[Dict]) -> Tuple[datetime, float, int]:
    """
    Find the exact timestamp and price of the cycle bottom (lowest point).
    Returns: (timestamp, price, price_id)
    """
    if not prices:
        return None, None, None
    
    min_price = float('inf')
    min_time = None
    min_id = None
    
    for p in prices:
        price = float(p['price'])
        if price < min_price:
            min_price = price
            min_time = p['timestamp']
            min_id = p['id']
    
    return min_time, min_price, min_id


def get_trades_around_time(target_time: datetime, window_minutes: int = 5) -> List[Dict]:
    """
    Get buyins that occurred around the target time (the cycle bottom).
    """
    start = target_time - timedelta(minutes=window_minutes)
    end = target_time + timedelta(minutes=window_minutes)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    b.id as buyin_id,
                    b.followed_at,
                    b.our_entry_price,
                    b.potential_gains,
                    b.our_status,
                    b.price_cycle
                FROM follow_the_goat_buyins b
                WHERE b.followed_at >= %s
                  AND b.followed_at <= %s
                ORDER BY b.followed_at ASC
            """, [start, end])
            return cursor.fetchall()


def get_trail_data_for_buyin(buyin_id: int) -> List[Dict]:
    """
    Get all trail minute data for a specific buyin.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT *
                FROM buyin_trail_minutes
                WHERE buyin_id = %s
                ORDER BY minute ASC
            """, [buyin_id])
            return cursor.fetchall()


def get_all_buyins_last_n_hours(hours: int = 24) -> List[Dict]:
    """
    Get all buyins from the last N hours with their trail data.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    b.id as buyin_id,
                    b.followed_at,
                    b.our_entry_price,
                    b.potential_gains,
                    b.our_status,
                    b.price_cycle
                FROM follow_the_goat_buyins b
                WHERE b.followed_at >= NOW() - INTERVAL '%s hours'
                ORDER BY b.followed_at DESC
            """, [hours])
            return cursor.fetchall()


def analyze_filter_distribution(good_buyins: List[int], bad_buyins: List[int], minute: int = 0) -> Dict:
    """
    Compare filter values between good and bad buyins at a specific minute.
    Returns filters that show the biggest difference between good and bad.
    """
    # Key ratio columns to analyze
    ratio_columns = [
        'pm_price_change_1m', 'pm_momentum_volatility_ratio', 'pm_momentum_acceleration_1m',
        'pm_volatility_pct', 'pm_body_range_ratio', 'pm_trend_consistency_3m',
        'ob_volume_imbalance', 'ob_imbalance_shift_1m', 'ob_depth_imbalance_ratio',
        'ob_bid_liquidity_share_pct', 'ob_ask_liquidity_share_pct', 'ob_liquidity_change_3m',
        'ob_microprice_deviation', 'ob_spread_bps', 'ob_vwap_spread_bps',
        'ob_net_flow_to_liquidity_ratio', 'tx_buy_sell_pressure', 'tx_buy_volume_pct',
        'tx_pressure_shift_1m', 'tx_long_short_ratio', 'tx_volume_surge_ratio',
        'wh_net_flow_ratio', 'wh_accumulation_ratio', 'wh_strong_accumulation_pct',
        'wh_massive_move_pct', 'wh_movement_imbalance_pct', 'wh_distribution_pressure_pct',
        'pat_breakout_score', 'pat_asc_tri_compression_ratio',
        'sp_price_range_pct', 'sp_total_change_pct', 'sp_volatility_pct',
        'btc_price_change_1m', 'btc_price_change_5m', 'btc_volatility_pct',
        'eth_price_change_1m', 'eth_price_change_5m', 'eth_volatility_pct'
    ]
    
    cols_str = ', '.join(ratio_columns)
    
    results = {}
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get stats for good buyins
            if good_buyins:
                good_ids_str = ','.join(str(b) for b in good_buyins)
                cursor.execute(f"""
                    SELECT 
                        {', '.join([f'AVG({c}) as avg_{c}, STDDEV({c}) as std_{c}' for c in ratio_columns])}
                    FROM buyin_trail_minutes
                    WHERE buyin_id IN ({good_ids_str})
                      AND minute = %s
                """, [minute])
                good_stats = cursor.fetchone()
            else:
                good_stats = None
            
            # Get stats for bad buyins
            if bad_buyins:
                bad_ids_str = ','.join(str(b) for b in bad_buyins)
                cursor.execute(f"""
                    SELECT 
                        {', '.join([f'AVG({c}) as avg_{c}, STDDEV({c}) as std_{c}' for c in ratio_columns])}
                    FROM buyin_trail_minutes
                    WHERE buyin_id IN ({bad_ids_str})
                      AND minute = %s
                """, [minute])
                bad_stats = cursor.fetchone()
            else:
                bad_stats = None
    
    if good_stats and bad_stats:
        for col in ratio_columns:
            good_avg = float(good_stats[f'avg_{col}']) if good_stats[f'avg_{col}'] else 0
            bad_avg = float(bad_stats[f'avg_{col}']) if bad_stats[f'avg_{col}'] else 0
            good_std = float(good_stats[f'std_{col}']) if good_stats[f'std_{col}'] else 1
            
            # Calculate separation score (how different are good vs bad)
            diff = abs(good_avg - bad_avg)
            separation = diff / good_std if good_std > 0 else 0
            
            results[col] = {
                'good_avg': good_avg,
                'bad_avg': bad_avg,
                'difference': good_avg - bad_avg,
                'separation_score': separation
            }
    
    return results


def analyze_optimal_buy_patterns():
    """
    Main analysis function.
    """
    print("=" * 80)
    print("OPTIMAL BUY SIGNAL ANALYSIS")
    print("=" * 80)
    print(f"Looking for cycles with {MIN_GAIN}%+ gain (threshold: {THRESHOLD}%)")
    print(f"Analyzing last {HOURS_LOOKBACK} hours of data")
    print()
    
    # Step 1: Get good cycles
    good_cycles = get_good_cycles()
    print(f"Found {len(good_cycles)} good cycles ({MIN_GAIN}%+ gain)")
    print()
    
    if not good_cycles:
        print("No good cycles found in the time period.")
        return
    
    # Step 2: For each good cycle, find the bottom and analyze
    cycle_bottoms = []
    
    for cycle in good_cycles:
        cycle_id = cycle['id']
        print(f"--- Cycle {cycle_id} ---")
        print(f"  Start: {cycle['cycle_start_time']}")
        print(f"  End:   {cycle['cycle_end_time']}")
        print(f"  Gain:  {cycle['max_percent_increase']:.2f}%")
        print(f"  Start Price: ${float(cycle['sequence_start_price']):.4f}")
        print(f"  Lowest: ${float(cycle['lowest_price_reached']):.4f}")
        print(f"  Highest: ${float(cycle['highest_price_reached']):.4f}")
        
        # Find exact bottom
        prices = get_price_data_for_cycle(cycle)
        bottom_time, bottom_price, bottom_id = find_cycle_bottom(prices)
        
        if bottom_time:
            print(f"  Bottom Time: {bottom_time} @ ${bottom_price:.4f}")
            
            # Get trades around the bottom
            trades_at_bottom = get_trades_around_time(bottom_time, window_minutes=3)
            print(f"  Trades near bottom: {len(trades_at_bottom)}")
            
            cycle_bottoms.append({
                'cycle_id': cycle_id,
                'bottom_time': bottom_time,
                'bottom_price': bottom_price,
                'gain': float(cycle['max_percent_increase']),
                'trades': trades_at_bottom
            })
        print()
    
    # Step 3: Get ALL buyins and classify them
    print("\n" + "=" * 80)
    print("ANALYZING ALL BUYINS")
    print("=" * 80)
    
    all_buyins = get_all_buyins_last_n_hours(24)
    print(f"Total buyins in last 24h: {len(all_buyins)}")
    
    # Classify buyins by potential_gains
    good_buyins = []
    bad_buyins = []
    
    for buyin in all_buyins:
        gains = buyin['potential_gains']
        if gains is not None:
            gains = float(gains)
            if gains >= MIN_GAIN:
                good_buyins.append(buyin['buyin_id'])
            else:
                bad_buyins.append(buyin['buyin_id'])
    
    print(f"Good buyins (>= {MIN_GAIN}% gain): {len(good_buyins)}")
    print(f"Bad buyins (< {MIN_GAIN}% gain): {len(bad_buyins)}")
    
    if len(good_buyins) < 10:
        print("WARNING: Not enough good buyins for reliable analysis")
    
    # Step 4: Analyze filter differences at each minute
    print("\n" + "=" * 80)
    print("FILTER ANALYSIS BY MINUTE")
    print("=" * 80)
    
    best_filters_by_minute = {}
    
    for minute in range(15):  # Minutes 0-14
        print(f"\n--- Minute {minute} ---")
        
        filter_analysis = analyze_filter_distribution(good_buyins, bad_buyins, minute)
        
        if not filter_analysis:
            print("  No data available")
            continue
        
        # Sort by separation score
        sorted_filters = sorted(
            filter_analysis.items(),
            key=lambda x: abs(x[1]['separation_score']),
            reverse=True
        )
        
        print(f"  Top 10 differentiating filters:")
        top_filters = []
        for name, stats in sorted_filters[:10]:
            direction = "↑" if stats['difference'] > 0 else "↓"
            print(f"    {name}: {direction} good={stats['good_avg']:.4f}, bad={stats['bad_avg']:.4f}, sep={stats['separation_score']:.2f}")
            top_filters.append({
                'name': name,
                'good_avg': stats['good_avg'],
                'bad_avg': stats['bad_avg'],
                'separation': stats['separation_score'],
                'direction': 'higher' if stats['difference'] > 0 else 'lower'
            })
        
        best_filters_by_minute[minute] = top_filters
    
    # Step 5: Summary - which filters are consistently good across minutes
    print("\n" + "=" * 80)
    print("OVERALL BEST FILTERS (appearing in top 10 across multiple minutes)")
    print("=" * 80)
    
    filter_counts = {}
    filter_avg_separation = {}
    
    for minute, filters in best_filters_by_minute.items():
        for f in filters:
            name = f['name']
            if name not in filter_counts:
                filter_counts[name] = 0
                filter_avg_separation[name] = []
            filter_counts[name] += 1
            filter_avg_separation[name].append(abs(f['separation']))
    
    # Sort by count then by average separation
    sorted_overall = sorted(
        filter_counts.items(),
        key=lambda x: (x[1], sum(filter_avg_separation[x[0]])/len(filter_avg_separation[x[0]])),
        reverse=True
    )
    
    print("\nFilters that consistently differentiate good from bad trades:")
    for name, count in sorted_overall[:15]:
        avg_sep = sum(filter_avg_separation[name]) / len(filter_avg_separation[name])
        print(f"  {name}: appears in {count}/15 minutes, avg separation: {avg_sep:.2f}")
    
    # Step 6: Deep dive on trades at cycle bottoms
    print("\n" + "=" * 80)
    print("TRADES AT CYCLE BOTTOMS (Optimal Buy Moments)")
    print("=" * 80)
    
    bottom_trade_ids = []
    for cb in cycle_bottoms:
        for trade in cb['trades']:
            bottom_trade_ids.append(trade['buyin_id'])
            print(f"  Trade {trade['buyin_id']} @ {trade['followed_at']}")
            print(f"    Entry: ${float(trade['our_entry_price']):.4f}, Status: {trade['our_status']}")
            if trade['potential_gains']:
                print(f"    Potential Gain: {float(trade['potential_gains']):.2f}%")
    
    print(f"\nTotal trades at cycle bottoms: {len(bottom_trade_ids)}")
    
    # Analyze these specific trades
    if bottom_trade_ids:
        print("\nFilter values for trades at cycle bottoms (minute 0):")
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                ids_str = ','.join(str(b) for b in bottom_trade_ids[:50])  # Limit to 50
                cursor.execute(f"""
                    SELECT 
                        buyin_id,
                        ob_spread_bps,
                        ob_volume_imbalance,
                        wh_net_flow_ratio,
                        wh_accumulation_ratio,
                        tx_buy_sell_pressure,
                        pm_momentum_acceleration_1m,
                        pat_breakout_score,
                        sp_volatility_pct
                    FROM buyin_trail_minutes
                    WHERE buyin_id IN ({ids_str})
                      AND minute = 0
                """)
                
                for row in cursor.fetchall():
                    print(f"\n  Buyin {row['buyin_id']}:")
                    print(f"    ob_spread_bps: {row['ob_spread_bps']}")
                    print(f"    ob_volume_imbalance: {row['ob_volume_imbalance']}")
                    print(f"    wh_net_flow_ratio: {row['wh_net_flow_ratio']}")
                    print(f"    wh_accumulation_ratio: {row['wh_accumulation_ratio']}")
                    print(f"    tx_buy_sell_pressure: {row['tx_buy_sell_pressure']}")
                    print(f"    pm_momentum_acceleration_1m: {row['pm_momentum_acceleration_1m']}")
                    print(f"    pat_breakout_score: {row['pat_breakout_score']}")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    analyze_optimal_buy_patterns()

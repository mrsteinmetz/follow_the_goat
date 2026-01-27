"""
Analyze Cycle Bottoms - Find What Makes the PERFECT Buy Moment
==============================================================
Instead of comparing all good vs bad trades, this script focuses on:
1. Finding the EXACT bottom of each good cycle (the perfect entry)
2. Looking at what the filter values were at that exact moment
3. Comparing those "perfect moment" values to "bad moment" values

This is more targeted - we want to identify the UNIQUE signature 
of a cycle bottom, not just general good trade characteristics.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
import logging
from decimal import Decimal
import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("cycle_bottoms")

MIN_GAIN = 0.8
HOURS_LOOKBACK = 48


def get_good_cycles(hours: int = HOURS_LOOKBACK) -> List[Dict]:
    """Get cycles with good gains."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id, cycle_start_time, cycle_end_time,
                    sequence_start_price, highest_price_reached, lowest_price_reached,
                    max_percent_increase
                FROM cycle_tracker
                WHERE threshold = 0.3
                  AND cycle_end_time IS NOT NULL
                  AND max_percent_increase >= %s
                  AND cycle_start_time >= NOW() - INTERVAL '%s hours'
                ORDER BY cycle_start_time DESC
            """, [MIN_GAIN, hours])
            return cursor.fetchall()


def find_exact_bottom_time(cycle: Dict) -> Tuple[datetime, float]:
    """Find the exact timestamp when price hit its lowest in the cycle."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, price
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY price ASC
                LIMIT 1
            """, [cycle['cycle_start_time'], cycle['cycle_end_time']])
            result = cursor.fetchone()
            if result:
                return result['timestamp'], float(result['price'])
            return None, None


def get_closest_buyin_to_time(target_time: datetime, window_seconds: int = 30) -> Optional[Dict]:
    """Get the buyin closest to the target time."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    b.id as buyin_id,
                    b.followed_at,
                    b.our_entry_price,
                    b.potential_gains,
                    ABS(EXTRACT(EPOCH FROM (b.followed_at - %s))) as time_diff_seconds
                FROM follow_the_goat_buyins b
                WHERE b.followed_at >= %s - INTERVAL '%s seconds'
                  AND b.followed_at <= %s + INTERVAL '%s seconds'
                ORDER BY ABS(EXTRACT(EPOCH FROM (b.followed_at - %s))) ASC
                LIMIT 1
            """, [target_time, target_time, window_seconds, target_time, window_seconds, target_time])
            return cursor.fetchone()


def get_trail_data(buyin_id: int, minute: int = 0) -> Optional[Dict]:
    """Get trail data for a specific buyin at a specific minute."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT *
                FROM buyin_trail_minutes
                WHERE buyin_id = %s AND minute = %s
            """, [buyin_id, minute])
            return cursor.fetchone()


def get_random_bad_buyins(n: int = 100) -> List[Dict]:
    """Get random buyins that are definitively bad (low potential gains)."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    b.id as buyin_id,
                    b.potential_gains
                FROM follow_the_goat_buyins b
                WHERE b.followed_at >= NOW() - INTERVAL '48 hours'
                  AND b.potential_gains IS NOT NULL
                  AND b.potential_gains < 0.3
                ORDER BY RANDOM()
                LIMIT %s
            """, [n])
            return cursor.fetchall()


def compare_bottom_vs_bad():
    """Main comparison of cycle bottom values vs random bad trade values."""
    
    print("=" * 80)
    print("CYCLE BOTTOM ANALYSIS")
    print("=" * 80)
    print(f"Finding exact bottoms of good cycles (>{MIN_GAIN}% gain)")
    print()
    
    # Get good cycles
    cycles = get_good_cycles()
    print(f"Found {len(cycles)} good cycles")
    
    bottom_buyins = []
    
    for cycle in cycles:
        bottom_time, bottom_price = find_exact_bottom_time(cycle)
        if not bottom_time:
            continue
            
        # Find closest buyin to bottom
        buyin = get_closest_buyin_to_time(bottom_time, window_seconds=60)
        if buyin:
            trail = get_trail_data(buyin['buyin_id'], minute=0)
            if trail:
                bottom_buyins.append({
                    'cycle_id': cycle['id'],
                    'bottom_time': bottom_time,
                    'bottom_price': bottom_price,
                    'cycle_gain': float(cycle['max_percent_increase']),
                    'buyin_id': buyin['buyin_id'],
                    'time_diff': buyin['time_diff_seconds'],
                    'trail': trail
                })
                print(f"  Cycle {cycle['id']}: bottom at {bottom_time} (${bottom_price:.4f}), gain: {cycle['max_percent_increase']:.2f}%")
                print(f"    Closest buyin: {buyin['buyin_id']}, {buyin['time_diff_seconds']:.0f}s away")
    
    print(f"\nFound {len(bottom_buyins)} buyins at cycle bottoms")
    
    if len(bottom_buyins) < 3:
        print("Not enough data for analysis")
        return
    
    # Get random bad buyins for comparison
    bad_buyins_raw = get_random_bad_buyins(200)
    bad_trails = []
    for b in bad_buyins_raw:
        trail = get_trail_data(b['buyin_id'], minute=0)
        if trail:
            bad_trails.append(trail)
    
    print(f"Got {len(bad_trails)} random bad trades for comparison")
    
    # Key filters to compare
    key_filters = [
        'sp_total_change_pct',
        'sp_price_range_pct',
        'sp_volatility_pct',
        'ob_spread_bps',
        'ob_depth_imbalance_ratio',
        'ob_volume_imbalance',
        'ob_bid_liquidity_share_pct',
        'ob_liquidity_change_3m',
        'pm_volatility_pct',
        'pm_momentum_acceleration_1m',
        'pm_price_change_1m',
        'pm_price_change_5m',
        'eth_price_change_5m',
        'btc_price_change_5m',
        'wh_net_flow_ratio',
        'wh_accumulation_ratio',
        'wh_distribution_pressure_pct',
        'tx_buy_sell_pressure',
        'pat_breakout_score'
    ]
    
    print("\n" + "=" * 80)
    print("FILTER VALUES AT CYCLE BOTTOMS vs BAD TRADES")
    print("=" * 80)
    print(f"{'Filter':<35} {'Bottom Avg':>12} {'Bottom Med':>12} {'Bad Avg':>12} {'Bad Med':>12} {'Diff%':>8}")
    print("-" * 95)
    
    filter_diffs = []
    
    for f in key_filters:
        bottom_vals = [float(b['trail'][f]) for b in bottom_buyins if b['trail'][f] is not None]
        bad_vals = [float(t[f]) for t in bad_trails if t[f] is not None]
        
        if not bottom_vals or not bad_vals:
            continue
        
        bottom_avg = np.mean(bottom_vals)
        bottom_med = np.median(bottom_vals)
        bad_avg = np.mean(bad_vals)
        bad_med = np.median(bad_vals)
        
        # Calculate difference significance
        diff_pct = ((bottom_med - bad_med) / (abs(bad_med) + 0.001)) * 100
        
        print(f"{f:<35} {bottom_avg:>12.4f} {bottom_med:>12.4f} {bad_avg:>12.4f} {bad_med:>12.4f} {diff_pct:>7.1f}%")
        
        filter_diffs.append({
            'filter': f,
            'bottom_median': bottom_med,
            'bad_median': bad_med,
            'diff_pct': diff_pct
        })
    
    # Sort by absolute difference
    filter_diffs.sort(key=lambda x: abs(x['diff_pct']), reverse=True)
    
    print("\n" + "=" * 80)
    print("TOP DIFFERENTIATING FILTERS (by % difference)")
    print("=" * 80)
    
    for fd in filter_diffs[:10]:
        direction = "HIGHER" if fd['diff_pct'] > 0 else "LOWER"
        print(f"  {fd['filter']:<35}: {direction} at bottoms ({fd['diff_pct']:+.1f}%)")
        print(f"    Bottom median: {fd['bottom_median']:.4f}, Bad median: {fd['bad_median']:.4f}")
    
    # Create specific rules based on findings
    print("\n" + "=" * 80)
    print("SUGGESTED FILTER RULES FOR DETECTING CYCLE BOTTOMS")
    print("=" * 80)
    
    for fd in filter_diffs[:5]:
        f = fd['filter']
        if fd['diff_pct'] > 20:  # Significantly higher at bottoms
            threshold = fd['bad_median'] + (fd['bottom_median'] - fd['bad_median']) * 0.3
            print(f"  {f} > {threshold:.4f}  (bottoms are higher)")
        elif fd['diff_pct'] < -20:  # Significantly lower at bottoms
            threshold = fd['bad_median'] + (fd['bottom_median'] - fd['bad_median']) * 0.3
            print(f"  {f} < {threshold:.4f}  (bottoms are lower)")
    
    # Show actual values at each bottom
    print("\n" + "=" * 80)
    print("ACTUAL VALUES AT EACH CYCLE BOTTOM")
    print("=" * 80)
    
    for bb in bottom_buyins:
        print(f"\nCycle {bb['cycle_id']} bottom ({bb['cycle_gain']:.2f}% gain):")
        print(f"  Time: {bb['bottom_time']}, Price: ${bb['bottom_price']:.4f}")
        
        trail = bb['trail']
        print(f"  sp_total_change_pct: {trail['sp_total_change_pct']}")
        print(f"  pm_price_change_5m: {trail['pm_price_change_5m']}")
        print(f"  eth_price_change_5m: {trail['eth_price_change_5m']}")
        print(f"  ob_depth_imbalance_ratio: {trail['ob_depth_imbalance_ratio']}")
        print(f"  wh_accumulation_ratio: {trail['wh_accumulation_ratio']}")


if __name__ == "__main__":
    compare_bottom_vs_bad()

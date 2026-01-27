"""
Analyze Filter Thresholds for Optimal Buy Timing
=================================================
Based on the initial analysis, this script digs deeper into
the specific threshold values that separate good from bad trades.

Key Findings from Initial Analysis:
1. sp_total_change_pct: LOWER in good trades (-0.20 vs +0.03)
2. ob_spread_bps: HIGHER in good trades  
3. ob_depth_imbalance_ratio: LOWER in good trades
4. eth_price_change_5m: LOWER in good trades (negative)
5. ob_volume_imbalance: LOWER (more negative) in good trades

Goal: Find exact cutoff values to identify buy opportunities.
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
logger = logging.getLogger("filter_thresholds")

# Configuration
MIN_GAIN = 0.8  # Good trade threshold
HOURS_LOOKBACK = 24


def get_buyins_with_trail_data(hours: int = HOURS_LOOKBACK) -> tuple:
    """Get buyins classified as good/bad with their minute 0 trail data."""
    
    # Key filters to analyze based on initial findings
    filters_to_analyze = [
        'sp_total_change_pct',
        'ob_spread_bps', 
        'ob_depth_imbalance_ratio',
        'ob_volume_imbalance',
        'ob_bid_liquidity_share_pct',
        'ob_ask_liquidity_share_pct',
        'eth_price_change_5m',
        'btc_price_change_5m',
        'pm_volatility_pct',
        'pm_momentum_acceleration_1m',
        'wh_net_flow_ratio',
        'wh_accumulation_ratio',
        'wh_distribution_pressure_pct',
        'wh_massive_move_pct',
        'tx_buy_sell_pressure',
        'pat_breakout_score',
        'pat_asc_tri_compression_ratio',
        'sp_price_range_pct',
        'sp_volatility_pct'
    ]
    
    cols_str = ', '.join([f't.{c}' for c in filters_to_analyze])
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    b.id as buyin_id,
                    b.potential_gains,
                    b.our_entry_price,
                    b.followed_at,
                    b.our_status,
                    CASE WHEN b.potential_gains >= %s THEN 'good' ELSE 'bad' END as trade_quality,
                    {cols_str}
                FROM follow_the_goat_buyins b
                JOIN buyin_trail_minutes t ON t.buyin_id = b.id AND t.minute = 0
                WHERE b.followed_at >= NOW() - INTERVAL '%s hours'
                  AND b.potential_gains IS NOT NULL
                ORDER BY b.followed_at DESC
            """, [MIN_GAIN, hours])
            
            return cursor.fetchall(), filters_to_analyze


def analyze_percentiles(data: List[Dict], filters: List[str]):
    """Analyze percentile distributions for good vs bad trades."""
    
    good_trades = [d for d in data if d['trade_quality'] == 'good']
    bad_trades = [d for d in data if d['trade_quality'] == 'bad']
    
    print(f"\n{'='*80}")
    print(f"PERCENTILE ANALYSIS")
    print(f"{'='*80}")
    print(f"Good trades: {len(good_trades)}")
    print(f"Bad trades: {len(bad_trades)}")
    print()
    
    results = {}
    
    for f in filters:
        good_vals = [float(d[f]) for d in good_trades if d[f] is not None]
        bad_vals = [float(d[f]) for d in bad_trades if d[f] is not None]
        
        if len(good_vals) < 10 or len(bad_vals) < 10:
            continue
        
        good_p10, good_p50, good_p90 = np.percentile(good_vals, [10, 50, 90])
        bad_p10, bad_p50, bad_p90 = np.percentile(bad_vals, [10, 50, 90])
        
        # Calculate overlap
        good_min, good_max = min(good_vals), max(good_vals)
        bad_min, bad_max = min(bad_vals), max(bad_vals)
        
        # Check if good trades are clearly higher or lower
        if good_p50 > bad_p50:
            direction = "HIGHER"
            # Threshold: P25 of good trades (catches 75% of good)
            threshold = np.percentile(good_vals, 25)
            # How many bad trades would pass this threshold
            bad_passing = sum(1 for v in bad_vals if v >= threshold)
            bad_pass_pct = bad_passing / len(bad_vals) * 100
        else:
            direction = "LOWER"
            threshold = np.percentile(good_vals, 75)
            bad_passing = sum(1 for v in bad_vals if v <= threshold)
            bad_pass_pct = bad_passing / len(bad_vals) * 100
        
        separation = abs(good_p50 - bad_p50) / (np.std(good_vals) + 0.001)
        
        results[f] = {
            'direction': direction,
            'good_median': good_p50,
            'bad_median': bad_p50,
            'threshold': threshold,
            'bad_pass_pct': bad_pass_pct,
            'separation': separation
        }
    
    # Sort by separation score
    sorted_results = sorted(results.items(), key=lambda x: x[1]['separation'], reverse=True)
    
    print(f"{'Filter':<35} {'Dir':<7} {'Good P50':>10} {'Bad P50':>10} {'Threshold':>12} {'Bad Pass%':>10}")
    print("-" * 95)
    
    for name, stats in sorted_results:
        print(f"{name:<35} {stats['direction']:<7} {stats['good_median']:>10.4f} {stats['bad_median']:>10.4f} {stats['threshold']:>12.4f} {stats['bad_pass_pct']:>9.1f}%")
    
    return sorted_results


def find_optimal_filter_combination(data: List[Dict], filters: List[str]):
    """Find the best combination of filters to maximize good trade selection."""
    
    print(f"\n{'='*80}")
    print(f"OPTIMAL FILTER COMBINATION SEARCH")
    print(f"{'='*80}")
    
    good_trades = [d for d in data if d['trade_quality'] == 'good']
    bad_trades = [d for d in data if d['trade_quality'] == 'bad']
    
    # Based on analysis, test specific thresholds
    filter_rules = [
        ('sp_total_change_pct', '<', 0.0),  # Price just dropped
        ('sp_total_change_pct', '<', -0.1),  # Price dropped more
        ('ob_depth_imbalance_ratio', '<', 1.0),  # More ask pressure
        ('ob_depth_imbalance_ratio', '<', 0.9),  # Much more ask pressure
        ('ob_volume_imbalance', '<', 0.0),  # Selling pressure
        ('ob_volume_imbalance', '<', -0.05),  # More selling pressure
        ('eth_price_change_5m', '<', 0.0),  # ETH dropped
        ('btc_price_change_5m', '<', 0.0),  # BTC dropped
        ('ob_spread_bps', '>', 0.805),  # Wider spread
        ('wh_accumulation_ratio', '<', 0.5),  # Whales distributing
        ('wh_distribution_pressure_pct', '>', 40),  # Distribution pressure
    ]
    
    print("\nTesting individual rules:")
    print(f"{'Rule':<50} {'Good Pass':>10} {'Bad Pass':>10} {'Precision':>10}")
    print("-" * 85)
    
    rule_results = []
    
    for col, op, threshold in filter_rules:
        if op == '<':
            good_pass = sum(1 for d in good_trades if d[col] is not None and float(d[col]) < threshold)
            bad_pass = sum(1 for d in bad_trades if d[col] is not None and float(d[col]) < threshold)
        else:  # '>'
            good_pass = sum(1 for d in good_trades if d[col] is not None and float(d[col]) > threshold)
            bad_pass = sum(1 for d in bad_trades if d[col] is not None and float(d[col]) > threshold)
        
        good_pct = good_pass / len(good_trades) * 100 if good_trades else 0
        bad_pct = bad_pass / len(bad_trades) * 100 if bad_trades else 0
        
        total_pass = good_pass + bad_pass
        precision = good_pass / total_pass * 100 if total_pass > 0 else 0
        
        rule_str = f"{col} {op} {threshold}"
        print(f"{rule_str:<50} {good_pct:>9.1f}% {bad_pct:>9.1f}% {precision:>9.1f}%")
        
        rule_results.append({
            'rule': (col, op, threshold),
            'good_pass': good_pass,
            'bad_pass': bad_pass,
            'good_pct': good_pct,
            'bad_pct': bad_pct,
            'precision': precision
        })
    
    # Test combinations
    print("\n\nTesting rule combinations (AND logic):")
    print(f"{'Combination':<70} {'Good':>6} {'Bad':>6} {'Prec':>6}")
    print("-" * 90)
    
    best_combos = []
    
    # Test pairs of rules
    for i, r1 in enumerate(filter_rules):
        for j, r2 in enumerate(filter_rules):
            if i >= j:
                continue
            
            col1, op1, th1 = r1
            col2, op2, th2 = r2
            
            def passes_rule(d, col, op, th):
                if d[col] is None:
                    return False
                val = float(d[col])
                return val < th if op == '<' else val > th
            
            good_pass = sum(1 for d in good_trades 
                          if passes_rule(d, col1, op1, th1) and passes_rule(d, col2, op2, th2))
            bad_pass = sum(1 for d in bad_trades 
                         if passes_rule(d, col1, op1, th1) and passes_rule(d, col2, op2, th2))
            
            total = good_pass + bad_pass
            if total < 5:  # Need at least 5 trades
                continue
                
            precision = good_pass / total * 100 if total > 0 else 0
            
            if precision > 20:  # Only show decent combos
                combo_str = f"{col1}{op1}{th1} AND {col2}{op2}{th2}"
                print(f"{combo_str:<70} {good_pass:>5} {bad_pass:>5} {precision:>5.1f}%")
                
                best_combos.append({
                    'rules': [r1, r2],
                    'good_pass': good_pass,
                    'bad_pass': bad_pass,
                    'precision': precision
                })
    
    # Sort by precision
    best_combos.sort(key=lambda x: (-x['precision'], -x['good_pass']))
    
    print("\n\nBEST COMBINATIONS (sorted by precision):")
    print("-" * 90)
    for combo in best_combos[:10]:
        r1, r2 = combo['rules']
        print(f"  {r1[0]} {r1[1]} {r1[2]} AND {r2[0]} {r2[1]} {r2[2]}")
        print(f"    Good: {combo['good_pass']}, Bad: {combo['bad_pass']}, Precision: {combo['precision']:.1f}%")
        print()


def visualize_good_vs_bad(data: List[Dict]):
    """Show distribution of key filters for good vs bad trades."""
    
    print(f"\n{'='*80}")
    print(f"DISTRIBUTION COMPARISON")
    print(f"{'='*80}")
    
    good = [d for d in data if d['trade_quality'] == 'good']
    bad = [d for d in data if d['trade_quality'] == 'bad']
    
    key_filters = ['sp_total_change_pct', 'ob_depth_imbalance_ratio', 'ob_volume_imbalance', 
                   'eth_price_change_5m', 'wh_distribution_pressure_pct']
    
    for f in key_filters:
        good_vals = [float(d[f]) for d in good if d[f] is not None]
        bad_vals = [float(d[f]) for d in bad if d[f] is not None]
        
        if not good_vals or not bad_vals:
            continue
        
        print(f"\n{f}:")
        print(f"  Good trades: min={min(good_vals):.4f}, median={np.median(good_vals):.4f}, max={max(good_vals):.4f}")
        print(f"  Bad trades:  min={min(bad_vals):.4f}, median={np.median(bad_vals):.4f}, max={max(bad_vals):.4f}")
        
        # Simple histogram
        good_hist = {}
        for v in good_vals:
            bucket = round(v, 2)
            good_hist[bucket] = good_hist.get(bucket, 0) + 1
        
        print(f"  Good distribution (top 5 buckets):")
        for bucket, count in sorted(good_hist.items(), key=lambda x: -x[1])[:5]:
            print(f"    {bucket:.2f}: {'#' * min(count, 50)} ({count})")


def main():
    print("=" * 80)
    print("FILTER THRESHOLD ANALYSIS")
    print("=" * 80)
    print(f"Looking for trades with {MIN_GAIN}%+ potential gain")
    print(f"Analyzing last {HOURS_LOOKBACK} hours")
    
    data, filters = get_buyins_with_trail_data()
    
    if not data:
        print("No data found!")
        return
    
    print(f"Loaded {len(data)} trades with trail data")
    
    # Run analyses
    percentile_results = analyze_percentiles(data, filters)
    find_optimal_filter_combination(data, filters)
    visualize_good_vs_bad(data)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print("\nKEY TAKEAWAYS:")
    print("  - Good trades tend to happen when price just DROPPED (sp_total_change_pct < 0)")
    print("  - Look for selling pressure in order book (ob_volume_imbalance < 0)")
    print("  - ETH/BTC also dropping is a good sign (buying the dip together)")
    print("  - Higher spread (ob_spread_bps) often precedes reversals")


if __name__ == "__main__":
    main()

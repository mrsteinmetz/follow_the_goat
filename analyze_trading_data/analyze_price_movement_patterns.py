"""
Analyze Price Movement Patterns Before Entry
=============================================
This script enhances the existing filter analysis by adding price movement
analysis BEFORE entry. Goal: Filter out trades where price was falling before entry.

Key Insight from Image:
When price goes DOWN before entry, trades almost always fail.
When price goes UP before entry, trades have better success rate.

Approach:
1. Fetch all trades from last 24 hours
2. For each trade, analyze price movement 1-10 minutes BEFORE entry
3. Calculate metrics:
   - Price change 1m before entry
   - Price change 5m before entry  
   - Price change 10m before entry
   - Price trend direction (up/down/flat)
   - Volatility before entry
4. Correlate with trade outcomes (potential_gains)
5. Run 100 simulations with different filter combinations including price movement
6. Find optimal combination that only catches rising price entries
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
import logging
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
logger = logging.getLogger("price_movement_analysis")

# Configuration
MIN_GAIN = 0.5   # Minimum gain % to consider a "good" trade (lowered to get more samples)
HOURS_LOOKBACK = 24  # Hours of data to analyze
NUM_SIMULATIONS = 100  # Number of filter combinations to test


def get_price_before_entry(entry_time: datetime, minutes_before: int) -> Optional[float]:
    """
    Get the price N minutes before entry time.
    Returns None if price data not available.
    """
    target_time = entry_time - timedelta(minutes=minutes_before)
    start_time = target_time - timedelta(seconds=30)  # 30 second window
    end_time = target_time + timedelta(seconds=30)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT price, timestamp
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY timestamp ASC
                LIMIT 1
            """, [start_time, end_time])
            result = cursor.fetchone()
            
            if result:
                return float(result['price'])
            return None


def analyze_price_movement_before_entry(trade: Dict) -> Dict:
    """
    Analyze price movement patterns before trade entry.
    Returns dict with price movement metrics.
    """
    entry_time = trade['followed_at']
    entry_price = float(trade['our_entry_price'])
    
    # Get prices at various points before entry
    price_1m_before = get_price_before_entry(entry_time, 1)
    price_2m_before = get_price_before_entry(entry_time, 2)
    price_5m_before = get_price_before_entry(entry_time, 5)
    price_10m_before = get_price_before_entry(entry_time, 10)
    
    result = {
        'entry_price': entry_price,
        'price_1m_before': price_1m_before,
        'price_2m_before': price_2m_before,
        'price_5m_before': price_5m_before,
        'price_10m_before': price_10m_before,
    }
    
    # Calculate price changes (% change from before to entry)
    if price_1m_before:
        result['change_1m'] = ((entry_price - price_1m_before) / price_1m_before) * 100
    else:
        result['change_1m'] = None
        
    if price_2m_before:
        result['change_2m'] = ((entry_price - price_2m_before) / price_2m_before) * 100
    else:
        result['change_2m'] = None
    
    if price_5m_before:
        result['change_5m'] = ((entry_price - price_5m_before) / price_5m_before) * 100
    else:
        result['change_5m'] = None
    
    if price_10m_before:
        result['change_10m'] = ((entry_price - price_10m_before) / price_10m_before) * 100
    else:
        result['change_10m'] = None
    
    # Determine trend direction
    if price_1m_before and price_5m_before:
        if result['change_1m'] > 0.05 and result['change_5m'] > 0.1:
            result['trend'] = 'rising'
        elif result['change_1m'] < -0.05 and result['change_5m'] < -0.1:
            result['trend'] = 'falling'
        else:
            result['trend'] = 'flat'
    else:
        result['trend'] = 'unknown'
    
    return result


def get_all_trades_with_analysis(hours: int = HOURS_LOOKBACK) -> List[Dict]:
    """
    Get all trades from last N hours with complete analysis:
    - Trade details (entry, outcome, filters)
    - Price movement before entry
    - Trail minute data (existing filters)
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    b.id,
                    b.followed_at,
                    b.our_entry_price,
                    b.potential_gains,
                    b.our_status,
                    b.price_cycle,
                    b.play_id
                FROM follow_the_goat_buyins b
                WHERE b.followed_at >= NOW() - INTERVAL '%s hours'
                  AND b.potential_gains IS NOT NULL
                  AND b.play_id = 46
                ORDER BY b.followed_at DESC
            """, [hours])
            
            trades = cursor.fetchall()
    
    logger.info(f"Fetched {len(trades)} trades from last {hours} hours")
    
    # Enrich each trade with price movement analysis
    enriched_trades = []
    for i, trade in enumerate(trades):
        if i % 10 == 0:
            logger.info(f"Analyzing trade {i+1}/{len(trades)}...")
        
        # Add price movement analysis
        price_analysis = analyze_price_movement_before_entry(trade)
        
        # Get trail minute 0 data (existing filters)
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        pm_price_change_1m,
                        pm_price_change_5m,
                        pm_volatility_pct,
                        sp_total_change_pct,
                        sp_volatility_pct,
                        ob_volume_imbalance,
                        ob_spread_bps,
                        wh_accumulation_ratio,
                        tx_buy_sell_pressure,
                        eth_price_change_5m,
                        btc_price_change_5m
                    FROM buyin_trail_minutes
                    WHERE buyin_id = %s AND minute = 0
                """, [trade['id']])
                trail_data = cursor.fetchone()
        
        # Combine all data
        enriched = {
            'id': trade['id'],
            'followed_at': trade['followed_at'],
            'entry_price': float(trade['our_entry_price']),
            'potential_gains': float(trade['potential_gains']) if trade['potential_gains'] else 0,
            'outcome': 'good' if float(trade['potential_gains'] or 0) >= MIN_GAIN else 'bad',
            **price_analysis,
        }
        
        # Add trail data if available
        if trail_data:
            for key, val in trail_data.items():
                enriched[f'filter_{key}'] = float(val) if val is not None else None
        
        enriched_trades.append(enriched)
    
    return enriched_trades


def analyze_price_movement_correlation(trades: List[Dict]):
    """
    Analyze correlation between price movement before entry and trade outcomes.
    """
    print("\n" + "="*80)
    print("PRICE MOVEMENT CORRELATION ANALYSIS")
    print("="*80)
    
    good_trades = [t for t in trades if t['outcome'] == 'good']
    bad_trades = [t for t in trades if t['outcome'] == 'bad']
    
    print(f"\nTotal trades: {len(trades)}")
    print(f"Good trades (>= {MIN_GAIN}% gain): {len(good_trades)}")
    print(f"Bad trades (< {MIN_GAIN}% gain): {len(bad_trades)}")
    
    # Analyze price change distributions
    for timeframe in ['change_1m', 'change_2m', 'change_5m', 'change_10m']:
        good_changes = [t[timeframe] for t in good_trades if t[timeframe] is not None]
        bad_changes = [t[timeframe] for t in bad_trades if t[timeframe] is not None]
        
        if not good_changes or not bad_changes:
            continue
        
        good_avg = sum(good_changes) / len(good_changes)
        bad_avg = sum(bad_changes) / len(bad_changes)
        
        good_positive = sum(1 for c in good_changes if c > 0)
        bad_positive = sum(1 for c in bad_changes if c > 0)
        
        print(f"\n{timeframe.upper()} (price change before entry):")
        print(f"  Good trades: avg={good_avg:.3f}%, positive={good_positive}/{len(good_changes)} ({good_positive/len(good_changes)*100:.1f}%)")
        print(f"  Bad trades:  avg={bad_avg:.3f}%, positive={bad_positive}/{len(bad_changes)} ({bad_positive/len(bad_changes)*100:.1f}%)")
        print(f"  Difference:  {good_avg - bad_avg:.3f}%")
    
    # Analyze by trend direction
    print(f"\n\nTREND DIRECTION ANALYSIS:")
    for trend in ['rising', 'falling', 'flat', 'unknown']:
        trend_trades = [t for t in trades if t['trend'] == trend]
        if not trend_trades:
            continue
        
        trend_good = sum(1 for t in trend_trades if t['outcome'] == 'good')
        win_rate = trend_good / len(trend_trades) * 100
        
        print(f"  {trend.upper()}: {len(trend_trades)} trades, {trend_good} good, win rate: {win_rate:.1f}%")


def test_filter_combination(trades: List[Dict], filters: Dict[str, Tuple[str, float]]) -> Dict:
    """
    Test a specific combination of filters.
    
    filters: Dict of filter_name -> (operator, threshold)
    Example: {'change_5m': ('>', 0), 'filter_sp_total_change_pct': ('<', -0.1)}
    
    Returns: Dict with performance metrics
    """
    passing_trades = []
    
    for trade in trades:
        passes_all = True
        
        for filter_name, (operator, threshold) in filters.items():
            value = trade.get(filter_name)
            
            if value is None:
                passes_all = False
                break
            
            if operator == '>':
                if value <= threshold:
                    passes_all = False
                    break
            elif operator == '<':
                if value >= threshold:
                    passes_all = False
                    break
            elif operator == '>=':
                if value < threshold:
                    passes_all = False
                    break
            elif operator == '<=':
                if value > threshold:
                    passes_all = False
                    break
        
        if passes_all:
            passing_trades.append(trade)
    
    if not passing_trades:
        return {
            'total_signals': 0,
            'good_signals': 0,
            'bad_signals': 0,
            'win_rate': 0,
            'avg_gain': 0,
            'filters': filters
        }
    
    good_signals = sum(1 for t in passing_trades if t['outcome'] == 'good')
    bad_signals = len(passing_trades) - good_signals
    win_rate = good_signals / len(passing_trades) * 100
    avg_gain = sum(t['potential_gains'] for t in passing_trades) / len(passing_trades)
    
    return {
        'total_signals': len(passing_trades),
        'good_signals': good_signals,
        'bad_signals': bad_signals,
        'win_rate': win_rate,
        'avg_gain': avg_gain,
        'filters': filters
    }


def run_simulations(trades: List[Dict], num_sims: int = NUM_SIMULATIONS):
    """
    Run multiple simulations with different filter combinations.
    Focus on price movement + existing filters.
    
    Goal: Find combinations that:
    1. Only trigger on RISING price before entry
    2. Have high win rate (>50%)
    3. Generate 2-10 signals per day
    """
    print("\n" + "="*80)
    print(f"RUNNING {num_sims} SIMULATIONS")
    print("="*80)
    print("\nSearching for optimal filter combinations...")
    print("Criteria:")
    print("  - Price must be RISING before entry")
    print("  - Win rate > 50%")
    print("  - 2-10 signals per 24h")
    print()
    
    # Define filter candidates with various thresholds
    filter_candidates = []
    
    # Price movement filters (CRITICAL - must be rising)
    for threshold in [0, 0.05, 0.1, 0.15, 0.2]:
        filter_candidates.append(('change_1m', '>', threshold))
        filter_candidates.append(('change_2m', '>', threshold))
        filter_candidates.append(('change_5m', '>', threshold))
        filter_candidates.append(('change_10m', '>', threshold))
    
    # Additional existing filters that help
    # Session price change
    for threshold in [-0.5, -0.3, -0.2, -0.1, 0]:
        filter_candidates.append(('filter_sp_total_change_pct', '<', threshold))
    
    # Volatility
    for threshold in [0.05, 0.1, 0.15, 0.2]:
        filter_candidates.append(('filter_pm_volatility_pct', '>', threshold))
    
    # Order book
    for threshold in [-0.1, -0.05, 0]:
        filter_candidates.append(('filter_ob_volume_imbalance', '<', threshold))
    
    # ETH/BTC correlation
    filter_candidates.append(('filter_eth_price_change_5m', '<', 0))
    filter_candidates.append(('filter_btc_price_change_5m', '<', 0))
    
    # Whale accumulation
    for threshold in [0.3, 0.4, 0.5]:
        filter_candidates.append(('filter_wh_accumulation_ratio', '<', threshold))
    
    results = []
    
    # Test combinations of 2-4 filters
    # ALWAYS include at least one price movement filter
    import random
    random.seed(42)  # Reproducible results
    
    for sim_num in range(num_sims):
        # Pick 2-4 filters
        num_filters = random.choice([2, 3, 4])
        
        # MUST include price movement filter
        price_filter = random.choice([f for f in filter_candidates if f[0].startswith('change_')])
        
        # Pick remaining filters
        other_filters = [f for f in filter_candidates if not f[0].startswith('change_')]
        selected_filters = [price_filter] + random.sample(other_filters, num_filters - 1)
        
        # Build filter dict
        filter_dict = {}
        for filter_name, operator, threshold in selected_filters:
            filter_dict[filter_name] = (operator, threshold)
        
        # Test this combination
        result = test_filter_combination(trades, filter_dict)
        
        # Only keep if meets criteria
        if result['total_signals'] >= 2 and result['win_rate'] >= 50:
            results.append(result)
            
            if sim_num % 20 == 0:
                print(f"Simulation {sim_num}/{num_sims}: {result['total_signals']} signals, {result['win_rate']:.1f}% win rate")
    
    print(f"\nCompleted {num_sims} simulations")
    print(f"Found {len(results)} promising combinations\n")
    
    # Sort by win rate, then by number of signals
    results.sort(key=lambda x: (x['win_rate'], x['total_signals']), reverse=True)
    
    return results


def print_top_results(results: List[Dict], top_n: int = 20):
    """
    Print the top N filter combinations.
    """
    print("\n" + "="*80)
    print(f"TOP {top_n} FILTER COMBINATIONS")
    print("="*80)
    print("\nBest combinations (sorted by win rate, then signal count):\n")
    
    for i, result in enumerate(results[:top_n], 1):
        print(f"#{i} - Win Rate: {result['win_rate']:.1f}% | Signals: {result['total_signals']} (Good: {result['good_signals']}, Bad: {result['bad_signals']}) | Avg Gain: {result['avg_gain']:.2f}%")
        print("   Filters:")
        for filter_name, (operator, threshold) in result['filters'].items():
            display_name = filter_name.replace('filter_', '')
            print(f"     - {display_name} {operator} {threshold}")
        print()


def main():
    """
    Main analysis function.
    """
    print("="*80)
    print("PRICE MOVEMENT PATTERN ANALYSIS")
    print("="*80)
    print(f"Goal: Find trades where price goes UP before entry")
    print(f"Analyzing last {HOURS_LOOKBACK} hours")
    print(f"Running {NUM_SIMULATIONS} simulations")
    print(f"Good trade threshold: {MIN_GAIN}%+ gain\n")
    
    # Step 1: Get all trades with complete analysis
    logger.info("Fetching and analyzing trades...")
    trades = get_all_trades_with_analysis(HOURS_LOOKBACK)
    
    if len(trades) < 20:
        print(f"\nWARNING: Only {len(trades)} trades found. Need at least 20 for reliable analysis.")
        print("Consider increasing HOURS_LOOKBACK or lowering MIN_GAIN threshold.")
        return
    
    print(f"Analyzed {len(trades)} trades\n")
    
    # Step 2: Analyze price movement correlation
    analyze_price_movement_correlation(trades)
    
    # Step 3: Run simulations
    results = run_simulations(trades, NUM_SIMULATIONS)
    
    if not results:
        print("\nNo filter combinations met the criteria (2+ signals, 50%+ win rate)")
        print("Try adjusting thresholds or increasing data window.")
        return
    
    # Step 4: Print top results
    print_top_results(results, top_n=20)
    
    # Step 5: Summary
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    
    if results:
        best = results[0]
        print(f"\nBEST COMBINATION:")
        print(f"  Win Rate: {best['win_rate']:.1f}%")
        print(f"  Signals: {best['total_signals']} trades ({best['good_signals']} good, {best['bad_signals']} bad)")
        print(f"  Average Gain: {best['avg_gain']:.2f}%")
        print(f"  Filters:")
        for filter_name, (operator, threshold) in best['filters'].items():
            display_name = filter_name.replace('filter_', '')
            print(f"    - {display_name} {operator} {threshold}")
        
        print("\n🎯 KEY INSIGHT:")
        print("   Price movement BEFORE entry is critical!")
        print("   Only enter trades when price is RISING before entry.")
        print("   This filters out the falling-price entries shown in your image.")


if __name__ == "__main__":
    main()

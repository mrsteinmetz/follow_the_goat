#!/usr/bin/env python3
"""
Early Minutes (M1-M5) vs Late Minutes (M8-M11) Filter Comparison
=================================================================
Tests if filters at Minutes 1-5 (closer to entry at M0) catch more 
0.3-0.5% SOL gains than the current Minutes 8-11 filters.

Hypothesis: By M0 (entry), whales may have already acted. M1-M5 might
capture the "building momentum" phase better.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Metrics to test (same ones that work well at M8-M11)
TEST_METRICS = [
    'tx_whale_volume_pct',
    'ob_volume_imbalance',
    'ob_depth_imbalance_ratio',
    'tx_buy_trade_pct',
    'tx_total_volume_usd',
    'wh_net_flow_ratio',
    'wh_accumulation_ratio',
]

# Current best performers (baseline to beat)
CURRENT_BEST = {
    'tx_whale_volume_pct': {'minute': 8, 'good_pct': 90.6, 'bad_removed_pct': 20.1, 'score': 18.20},
    'ob_volume_imbalance': {'minute': 11, 'good_pct': 90.0, 'bad_removed_pct': 20.4, 'score': 18.37},
    'ob_depth_imbalance_ratio': {'minute': 11, 'good_pct': 90.3, 'bad_removed_pct': 20.2, 'score': 18.20},
}


def test_metric_at_minutes(metric: str, minutes: List[int], 
                           percentile_low: float = 5, percentile_high: float = 95) -> Dict[int, Dict]:
    """Test a metric at specific minutes."""
    
    results = {}
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for minute in minutes:
                    # Get percentiles for good trades at this minute
                    cursor.execute(f"""
                        SELECT 
                            PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY tfv.filter_value) as p_low,
                            PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY tfv.filter_value) as p_high
                        FROM trade_filter_values tfv
                        JOIN follow_the_goat_buyins b ON b.id = tfv.buyin_id
                        WHERE b.followed_at >= CURRENT_DATE
                          AND b.potential_gains >= 0.3
                          AND tfv.filter_name = %s
                          AND tfv.minute = %s
                          AND tfv.filter_value IS NOT NULL
                    """, [percentile_low / 100, percentile_high / 100, metric, minute])
                    
                    percentiles = cursor.fetchone()
                    if not percentiles or percentiles['p_low'] is None:
                        continue
                    
                    from_val = float(percentiles['p_low'])
                    to_val = float(percentiles['p_high'])
                    
                    if from_val >= to_val:
                        continue
                    
                    # Test effectiveness
                    cursor.execute(f"""
                        WITH filter_test AS (
                            SELECT 
                                b.id,
                                b.potential_gains,
                                tfv.filter_value,
                                CASE WHEN b.potential_gains >= 0.3 THEN 1 ELSE 0 END as is_good,
                                CASE WHEN tfv.filter_value >= %s AND tfv.filter_value <= %s THEN 1 ELSE 0 END as passes
                            FROM trade_filter_values tfv
                            JOIN follow_the_goat_buyins b ON b.id = tfv.buyin_id
                            WHERE b.followed_at >= CURRENT_DATE
                              AND tfv.filter_name = %s
                              AND tfv.minute = %s
                              AND tfv.filter_value IS NOT NULL
                        )
                        SELECT 
                            SUM(is_good) as good_total,
                            SUM(CASE WHEN is_good = 1 AND passes = 1 THEN 1 ELSE 0 END) as good_passed,
                            SUM(CASE WHEN is_good = 0 THEN 1 ELSE 0 END) as bad_total,
                            SUM(CASE WHEN is_good = 0 AND passes = 1 THEN 1 ELSE 0 END) as bad_passed
                        FROM filter_test
                    """, [from_val, to_val, metric, minute])
                    
                    metrics_result = cursor.fetchone()
                    
                    if not metrics_result or metrics_result['good_total'] == 0:
                        continue
                    
                    good_kept_pct = (metrics_result['good_passed'] / metrics_result['good_total'] * 100)
                    bad_removed_pct = ((metrics_result['bad_total'] - metrics_result['bad_passed']) / 
                                      metrics_result['bad_total'] * 100) if metrics_result['bad_total'] > 0 else 0
                    
                    score = good_kept_pct * (bad_removed_pct / 100)
                    
                    results[minute] = {
                        'from_val': from_val,
                        'to_val': to_val,
                        'good_kept_pct': good_kept_pct,
                        'bad_removed_pct': bad_removed_pct,
                        'score': score,
                        'good_passed': metrics_result['good_passed'],
                        'good_total': metrics_result['good_total'],
                        'bad_passed': metrics_result['bad_passed'],
                        'bad_total': metrics_result['bad_total']
                    }
    
    except Exception as e:
        logger.error(f"Error testing {metric}: {e}")
    
    return results


def compare_time_windows():
    """Compare M1-M5 vs M8-M11 for each metric."""
    
    logger.info("="*80)
    logger.info("EARLY MINUTES (M1-M5) vs LATE MINUTES (M8-M11) COMPARISON")
    logger.info("="*80)
    logger.info(f"Date: {datetime.now().date()}")
    logger.info(f"Hypothesis: M1-M5 catches 'building momentum' better than M8-M11")
    logger.info("="*80)
    
    # Get total trades count
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN potential_gains >= 0.3 THEN 1 ELSE 0 END) as good_trades,
                           SUM(CASE WHEN potential_gains < 0.3 THEN 1 ELSE 0 END) as bad_trades
                    FROM follow_the_goat_buyins
                    WHERE followed_at >= CURRENT_DATE
                      AND potential_gains IS NOT NULL
                """)
                summary = cursor.fetchone()
                logger.info(f"\nTotal trades analyzed: {summary['total']}")
                logger.info(f"Good trades (>= 0.3%): {summary['good_trades']} ({summary['good_trades']/summary['total']*100:.1f}%)")
                logger.info(f"Bad trades (< 0.3%): {summary['bad_trades']} ({summary['bad_trades']/summary['total']*100:.1f}%)")
    except Exception as e:
        logger.error(f"Failed to get trade summary: {e}")
    
    early_minutes = [1, 2, 3, 4, 5]
    late_minutes = [8, 9, 10, 11]
    
    all_results = {}
    early_winners = {}
    late_winners = {}
    
    for metric in TEST_METRICS:
        logger.info(f"\n{'='*80}")
        logger.info(f"TESTING: {metric}")
        logger.info(f"{'='*80}")
        
        # Test early minutes
        logger.info("\n📊 EARLY MINUTES (M1-M5):")
        logger.info("-"*80)
        early_results = test_metric_at_minutes(metric, early_minutes)
        
        if early_results:
            for m in sorted(early_results.keys()):
                r = early_results[m]
                logger.info(f"  M{m}: Good {r['good_passed']}/{r['good_total']} ({r['good_kept_pct']:.1f}%), "
                           f"Bad removed {r['bad_removed_pct']:.1f}%, Score: {r['score']:.2f}")
            
            # Find best in early range
            best_early = max(early_results.items(), key=lambda x: x[1]['score'])
            early_winners[metric] = {'minute': best_early[0], **best_early[1]}
            logger.info(f"\n  ⭐ BEST EARLY: M{best_early[0]} (Score: {best_early[1]['score']:.2f})")
        else:
            logger.warning(f"  ⚠️  No data for early minutes")
        
        # Test late minutes
        logger.info("\n📊 LATE MINUTES (M8-M11):")
        logger.info("-"*80)
        late_results = test_metric_at_minutes(metric, late_minutes)
        
        if late_results:
            for m in sorted(late_results.keys()):
                r = late_results[m]
                logger.info(f"  M{m}: Good {r['good_passed']}/{r['good_total']} ({r['good_kept_pct']:.1f}%), "
                           f"Bad removed {r['bad_removed_pct']:.1f}%, Score: {r['score']:.2f}")
            
            # Find best in late range
            best_late = max(late_results.items(), key=lambda x: x[1]['score'])
            late_winners[metric] = {'minute': best_late[0], **best_late[1]}
            logger.info(f"\n  ⭐ BEST LATE: M{best_late[0]} (Score: {best_late[1]['score']:.2f})")
        else:
            logger.warning(f"  ⚠️  No data for late minutes")
        
        # Compare
        if metric in early_winners and metric in late_winners:
            early_score = early_winners[metric]['score']
            late_score = late_winners[metric]['score']
            diff = early_score - late_score
            
            logger.info(f"\n🔍 VERDICT:")
            if diff > 0.5:
                logger.info(f"  ✅ EARLY WINS! M{early_winners[metric]['minute']} beats M{late_winners[metric]['minute']} "
                           f"by {diff:.2f} points")
            elif diff < -0.5:
                logger.info(f"  ✅ LATE WINS! M{late_winners[metric]['minute']} beats M{early_winners[metric]['minute']} "
                           f"by {abs(diff):.2f} points")
            else:
                logger.info(f"  ⚖️  TIE: Difference {diff:.2f} points (not significant)")
        
        all_results[metric] = {
            'early': early_results,
            'late': late_results
        }
    
    return early_winners, late_winners, all_results


def generate_recommendations(early_winners: Dict, late_winners: Dict):
    """Generate final recommendations and SQL if needed."""
    
    logger.info("\n" + "="*80)
    logger.info("FINAL RECOMMENDATIONS")
    logger.info("="*80)
    
    # Score comparison
    early_total_score = sum(w['score'] for w in early_winners.values())
    late_total_score = sum(w['score'] for w in late_winners.values())
    
    early_avg_good = sum(w['good_kept_pct'] for w in early_winners.values()) / len(early_winners) if early_winners else 0
    late_avg_good = sum(w['good_kept_pct'] for w in late_winners.values()) / len(late_winners) if late_winners else 0
    
    early_avg_bad_removed = sum(w['bad_removed_pct'] for w in early_winners.values()) / len(early_winners) if early_winners else 0
    late_avg_bad_removed = sum(w['bad_removed_pct'] for w in late_winners.values()) / len(late_winners) if late_winners else 0
    
    logger.info(f"\n📊 AGGREGATE SCORES:")
    logger.info("-"*80)
    logger.info(f"  Early Minutes (M1-M5):")
    logger.info(f"    Total Score: {early_total_score:.2f}")
    logger.info(f"    Avg Good Kept: {early_avg_good:.1f}%")
    logger.info(f"    Avg Bad Removed: {early_avg_bad_removed:.1f}%")
    logger.info(f"\n  Late Minutes (M8-M11):")
    logger.info(f"    Total Score: {late_total_score:.2f}")
    logger.info(f"    Avg Good Kept: {late_avg_good:.1f}%")
    logger.info(f"    Avg Bad Removed: {late_avg_bad_removed:.1f}%")
    
    score_diff = early_total_score - late_total_score
    
    logger.info(f"\n🎯 DECISION:")
    logger.info("-"*80)
    
    if score_diff > 5.0:
        logger.info(f"  ✅ SWITCH TO EARLY MINUTES!")
        logger.info(f"     Early minutes are {score_diff:.2f} points better")
        logger.info(f"     Hypothesis CONFIRMED: M1-M5 catches building momentum better")
        
        # Generate SQL
        logger.info(f"\n📝 SQL TO APPLY EARLY MINUTE FILTERS:")
        logger.info("="*80)
        logger.info("\n-- Clear existing AutoFilters")
        logger.info("DELETE FROM pattern_config_filters WHERE project_id = 5;")
        logger.info("\n-- Insert best early-minute filters")
        
        top_3_early = sorted(early_winners.items(), key=lambda x: x[1]['score'], reverse=True)[:3]
        
        for i, (metric, data) in enumerate(top_3_early, 1):
            section = 'order_book' if metric.startswith('ob_') else 'transactions' if metric.startswith('tx_') else 'whale_activity'
            field_name = metric.replace('tx_', '').replace('ob_', '').replace('wh_', '')
            
            logger.info(f"""
INSERT INTO pattern_config_filters 
(id, project_id, name, section, minute, field_name, field_column, from_value, to_value, include_null, is_active)
VALUES 
({5000 + i}, 5, 'Auto: {metric}', '{section}', {data['minute']}, '{field_name}', '{metric}', {data['from_val']:.6f}, {data['to_val']:.6f}, 0, 1);""")
        
        logger.info("\n✅ Run the SQL above to switch to early-minute filters!")
        
    elif score_diff < -5.0:
        logger.info(f"  ✅ KEEP LATE MINUTES!")
        logger.info(f"     Late minutes are {abs(score_diff):.2f} points better")
        logger.info(f"     Hypothesis REJECTED: M8-M11 performs better than M1-M5")
        logger.info(f"     Current filters are already optimal - no changes needed.")
        
    else:
        logger.info(f"  ⚖️  NO CLEAR WINNER")
        logger.info(f"     Difference: {score_diff:.2f} points (not significant)")
        logger.info(f"     Both time windows perform similarly")
        logger.info(f"     Recommendation: Stick with current M8-M11 filters (proven to work)")
    
    # Detailed comparison table
    logger.info(f"\n📋 SIDE-BY-SIDE COMPARISON:")
    logger.info("="*80)
    logger.info(f"{'Metric':<30s} {'Early Minute':<15s} {'Late Minute':<15s} {'Winner':<10s}")
    logger.info("-"*80)
    
    for metric in TEST_METRICS:
        if metric in early_winners and metric in late_winners:
            early = early_winners[metric]
            late = late_winners[metric]
            
            early_str = f"M{early['minute']} ({early['score']:.1f})"
            late_str = f"M{late['minute']} ({late['score']:.1f})"
            
            if early['score'] > late['score'] + 0.5:
                winner = "EARLY ✅"
            elif late['score'] > early['score'] + 0.5:
                winner = "LATE ✅"
            else:
                winner = "TIE ⚖️"
            
            logger.info(f"{metric:<30s} {early_str:<15s} {late_str:<15s} {winner:<10s}")
    
    logger.info("="*80)


def main():
    logger.info("="*80)
    logger.info("TESTING HYPOTHESIS: Do M1-M5 filters beat M8-M11?")
    logger.info("="*80)
    
    early_winners, late_winners, all_results = compare_time_windows()
    
    if early_winners and late_winners:
        generate_recommendations(early_winners, late_winners)
    else:
        logger.error("Insufficient data to make comparison")
    
    logger.info("\n" + "="*80)
    logger.info("Analysis complete!")
    logger.info("="*80)


if __name__ == "__main__":
    main()

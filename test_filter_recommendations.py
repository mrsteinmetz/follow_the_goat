#!/usr/bin/env python3
"""
Quick Filter Recommendations
=============================
Analyzes today's trades and generates specific filter recommendations.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def get_quick_analysis():
    """Get quick analysis of today's trades with filter values."""
    
    logger.info("Loading today's trades with filter values...")
    
    # Get a sample of filter columns (most commonly useful ones)
    filter_columns = [
        'tx_whale_volume_pct',
        'tx_buy_trade_pct', 
        'tx_vwap',
        'tx_avg_trade_size',
        'tx_total_volume_usd',
        'ob_volume_imbalance',
        'ob_depth_imbalance_ratio',
        'pm_price_change_5m',
        'pm_volatility_pct',
        'pat_asc_tri_compression_ratio'
    ]
    
    results = {}
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get trades from today
                cursor.execute("""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN potential_gains >= 0.6 THEN 1 ELSE 0 END) as good_trades,
                           SUM(CASE WHEN potential_gains < 0.6 THEN 1 ELSE 0 END) as bad_trades
                    FROM follow_the_goat_buyins
                    WHERE followed_at >= CURRENT_DATE
                      AND potential_gains IS NOT NULL
                """)
                summary = cursor.fetchone()
                
                logger.info(f"Total trades: {summary['total']}")
                logger.info(f"Good trades (>= 0.6%): {summary['good_trades']} ({summary['good_trades']/summary['total']*100:.1f}%)")
                logger.info(f"Bad trades (< 0.6%): {summary['bad_trades']} ({summary['bad_trades']/summary['total']*100:.1f}%)")
                
                # Test each filter at different minutes
                for col in filter_columns:
                    logger.info(f"\nAnalyzing {col}...")
                    
                    best_result = None
                    best_score = -1
                    
                    for minute in range(15):
                        # Get percentiles for good trades
                        cursor.execute(f"""
                            SELECT 
                                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY tfv.filter_value) as p05,
                                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tfv.filter_value) as p95
                            FROM trade_filter_values tfv
                            JOIN follow_the_goat_buyins b ON b.id = tfv.buyin_id
                            WHERE b.followed_at >= CURRENT_DATE
                              AND b.potential_gains >= 0.6
                              AND tfv.filter_name = %s
                              AND tfv.minute = %s
                              AND tfv.filter_value IS NOT NULL
                        """, [col, minute])
                        
                        percentiles = cursor.fetchone()
                        if not percentiles or percentiles['p05'] is None:
                            continue
                        
                        from_val = float(percentiles['p05'])
                        to_val = float(percentiles['p95'])
                        
                        if from_val >= to_val:
                            continue
                        
                        # Test effectiveness
                        cursor.execute(f"""
                            WITH filter_test AS (
                                SELECT 
                                    b.id,
                                    b.potential_gains,
                                    tfv.filter_value,
                                    CASE WHEN b.potential_gains >= 0.6 THEN 1 ELSE 0 END as is_good,
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
                        """, [from_val, to_val, col, minute])
                        
                        metrics = cursor.fetchone()
                        
                        if metrics['good_total'] == 0:
                            continue
                        
                        good_kept_pct = (metrics['good_passed'] / metrics['good_total'] * 100)
                        bad_removed_pct = ((metrics['bad_total'] - metrics['bad_passed']) / metrics['bad_total'] * 100) if metrics['bad_total'] > 0 else 0
                        
                        score = good_kept_pct * (bad_removed_pct / 100)
                        
                        if score > best_score:
                            best_score = score
                            best_result = {
                                'minute': minute,
                                'from_val': from_val,
                                'to_val': to_val,
                                'good_kept_pct': good_kept_pct,
                                'bad_removed_pct': bad_removed_pct,
                                'score': score,
                                'good_passed': metrics['good_passed'],
                                'good_total': metrics['good_total'],
                                'bad_passed': metrics['bad_passed'],
                                'bad_total': metrics['bad_total']
                            }
                    
                    if best_result:
                        results[col] = best_result
                        logger.info(f"  Best at Minute {best_result['minute']}: "
                                   f"Good {best_result['good_passed']}/{best_result['good_total']} ({best_result['good_kept_pct']:.1f}%), "
                                   f"Bad removed {best_result['bad_removed_pct']:.1f}%, Score: {best_result['score']:.2f}")
                        logger.info(f"  Range: [{best_result['from_val']:.6f} - {best_result['to_val']:.6f}]")
    
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        return results
    
    return results


def generate_recommendations(results: dict):
    """Generate filter recommendations."""
    
    if not results:
        logger.warning("No results to analyze")
        return
    
    logger.info("\n" + "="*80)
    logger.info("RECOMMENDATIONS")
    logger.info("="*80)
    
    # Sort by score
    sorted_filters = sorted(results.items(), key=lambda x: x[1]['score'], reverse=True)
    
    logger.info("\nTop 5 Single Filters:")
    for i, (col, data) in enumerate(sorted_filters[:5], 1):
        logger.info(f"\n{i}. {col}")
        logger.info(f"   Minute: {data['minute']}")
        logger.info(f"   Range: [{data['from_val']:.6f} to {data['to_val']:.6f}]")
        logger.info(f"   Good trades caught: {data['good_passed']}/{data['good_total']} ({data['good_kept_pct']:.1f}%)")
        logger.info(f"   Bad trades filtered: {data['bad_total'] - data['bad_passed']}/{data['bad_total']} ({data['bad_removed_pct']:.1f}%)")
        logger.info(f"   Effectiveness score: {data['score']:.2f}")
    
    # Generate SQL for top 3
    logger.info("\n" + "="*80)
    logger.info("SQL TO UPDATE AutoFilters PROJECT (ID=5):")
    logger.info("="*80)
    
    logger.info("\n-- Clear existing filters")
    logger.info("DELETE FROM pattern_config_filters WHERE project_id = 5;")
    
    logger.info("\n-- Insert new filters")
    for i, (col, data) in enumerate(sorted_filters[:3], 1):
        sql = f"""
INSERT INTO pattern_config_filters 
(id, project_id, name, section, minute, field_name, field_column, from_value, to_value, include_null, is_active)
VALUES 
({5000 + i}, 5, 'Auto: {col}', 'transactions', {data['minute']}, '{col}', '{col}', {data['from_val']:.6f}, {data['to_val']:.6f}, 0, 1);
"""
        logger.info(sql)
    
    logger.info("\n" + "="*80)
    logger.info("CONFIGURATION RECOMMENDATION:")
    logger.info("="*80)
    logger.info("\nTo implement looser percentiles (5-95 instead of 10-90):")
    logger.info("UPDATE auto_filter_settings SET setting_value = '5' WHERE setting_key = 'percentile_low';")
    logger.info("UPDATE auto_filter_settings SET setting_value = '95' WHERE setting_key = 'percentile_high';")
    logger.info("\nOr to be even more aggressive (1-99):")
    logger.info("UPDATE auto_filter_settings SET setting_value = '1' WHERE setting_key = 'percentile_low';")
    logger.info("UPDATE auto_filter_settings SET setting_value = '99' WHERE setting_key = 'percentile_high';")


def main():
    logger.info("="*80)
    logger.info("FILTER RECOMMENDATIONS ANALYSIS")
    logger.info(f"Date: {datetime.now().date()}")
    logger.info("="*80)
    
    results = get_quick_analysis()
    generate_recommendations(results)
    
    logger.info("\n" + "="*80)
    logger.info("Analysis complete!")
    logger.info("="*80)


if __name__ == "__main__":
    main()

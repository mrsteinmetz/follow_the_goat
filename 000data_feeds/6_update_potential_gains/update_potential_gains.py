"""
Update Potential Gains
======================
PostgreSQL Version - Jan 2026

Calculates and updates potential_gains for buyins where the price cycle has completed.
Formula: ((cycle_end_price - our_entry_price) / our_entry_price) * 100

Uses the price at cycle_end_time (from the prices table) as the outcome price.
This gives the REAL outcome: price at cycle close vs entry, which CAN be negative.

Previous (WRONG) formula used highest_price_reached, which always gave positive results
because it measured the peak, not what actually happened when the cycle closed.

Uses threshold = 0.3 for cycle_tracker lookup.
Only updates records where:
- potential_gains IS NULL
- cycle_end_time IS NOT NULL (completed cycles only)
- our_entry_price > 0

CRITICAL: Also includes no_go trades!
- no_go trades are valid for filter analysis since they have measurable outcomes
- They tell us what filter values would have avoided bad trades

Fallback mechanism:
- If cycle not found, looks up price 15 minutes after followed_at (trail window)
- Falls back to trade's own higest_price_reached only as last resort
"""

import sys
from pathlib import Path
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute

# Configure logger
logger = logging.getLogger("update_potential_gains")

# Threshold for cycle_tracker lookup
THRESHOLD = 0.3


def get_records_to_update():
    """
    Fetch buyins that need potential_gains calculated.
    
    Returns records where:
    - potential_gains IS NULL
    - Linked cycle_tracker has cycle_end_time IS NOT NULL (completed)
    - Uses threshold = 0.3 (trades should always reference 0.3 threshold cycles)
    - our_entry_price is valid (not null, > 0)
    - Includes both 'sold', 'completed', AND 'no_go' trades
    
    Returns:
        list: List of tuples (buyin_id, calculated_potential_gains)
    """
    # Query 1: Trades with existing cycle_tracker records
    # Uses LATERAL JOIN to look up the actual SOL price at cycle_end_time.
    # This gives the REAL outcome (can be negative) instead of the peak.
    query_with_cycle = """
    SELECT 
        buyins.id,
        ((p_end.price - buyins.our_entry_price) / buyins.our_entry_price) * 100 AS potential_gains
    FROM follow_the_goat_buyins buyins
    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
    CROSS JOIN LATERAL (
        SELECT price FROM prices
        WHERE token = 'SOL'
          AND timestamp <= ct.cycle_end_time
        ORDER BY timestamp DESC
        LIMIT 1
    ) p_end
    WHERE buyins.potential_gains IS NULL 
      AND ct.cycle_end_time IS NOT NULL
      AND ct.threshold = %s
      AND buyins.our_entry_price IS NOT NULL
      AND buyins.our_entry_price > 0
    """
    
    # Query 2: Orphaned trades (cycle archived/deleted)
    # Look up SOL price 15 minutes after entry (the trail window duration).
    # This gives the real outcome at the point the system would have evaluated.
    # Falls back to higest_price_reached only if no price data is available.
    query_orphaned = """
    SELECT 
        buyins.id,
        COALESCE(
            ((p_end.price - buyins.our_entry_price) / buyins.our_entry_price) * 100,
            ((buyins.higest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100
        ) AS potential_gains
    FROM follow_the_goat_buyins buyins
    LEFT JOIN LATERAL (
        SELECT price FROM prices
        WHERE token = 'SOL'
          AND timestamp <= buyins.followed_at + INTERVAL '15 minutes'
        ORDER BY timestamp DESC
        LIMIT 1
    ) p_end ON true
    WHERE buyins.potential_gains IS NULL
      AND buyins.price_cycle IS NOT NULL
      AND buyins.our_status IN ('sold', 'completed', 'no_go')
      AND buyins.our_entry_price IS NOT NULL
      AND buyins.our_entry_price > 0
      AND NOT EXISTS (
          SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins.price_cycle
      )
      AND (p_end.price IS NOT NULL OR (buyins.higest_price_reached IS NOT NULL AND buyins.higest_price_reached > 0))
    """
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Diagnostic queries to understand why no records match
                try:
                    cursor.execute("SELECT COUNT(*) as count FROM follow_the_goat_buyins")
                    total_buyins = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM follow_the_goat_buyins WHERE price_cycle IS NOT NULL")
                    buyins_with_price_cycle = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM follow_the_goat_buyins WHERE potential_gains IS NULL")
                    buyins_null_potential = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM follow_the_goat_buyins WHERE our_entry_price IS NOT NULL AND our_entry_price > 0")
                    buyins_valid_entry = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM follow_the_goat_buyins WHERE higest_price_reached IS NOT NULL")
                    buyins_with_highest = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM cycle_tracker WHERE cycle_end_time IS NOT NULL AND threshold = %s", [THRESHOLD])
                    completed_cycles = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM cycle_tracker WHERE threshold = %s", [THRESHOLD])
                    total_cycles = cursor.fetchone()['count']
                    
                    # Check what threshold values actually exist
                    cursor.execute("SELECT DISTINCT threshold FROM cycle_tracker ORDER BY threshold")
                    threshold_values = cursor.fetchall()
                    threshold_list = [str(t['threshold']) for t in threshold_values] if threshold_values else []
                    
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM follow_the_goat_buyins buyins
                        INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                        WHERE ct.cycle_end_time IS NOT NULL AND ct.threshold = %s
                    """, [THRESHOLD])
                    buyins_linked_to_completed = cursor.fetchone()['count']
                    
                    # Check for orphaned price_cycle references
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM follow_the_goat_buyins buyins
                        WHERE buyins.price_cycle IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins.price_cycle
                          )
                    """)
                    orphaned_buyins = cursor.fetchone()['count']
                    
                    # Check oldest completed cycle
                    cursor.execute("""
                        SELECT MIN(cycle_end_time) as oldest
                        FROM cycle_tracker
                        WHERE cycle_end_time IS NOT NULL AND threshold = %s
                    """, [THRESHOLD])
                    oldest_cycle_result = cursor.fetchone()
                    oldest_cycle_str = str(oldest_cycle_result['oldest']) if oldest_cycle_result and oldest_cycle_result['oldest'] else "N/A"
                    
                    # CRITICAL CHECK: Are cycles assigned to trades using the WRONG threshold?
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM follow_the_goat_buyins buyins
                        INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                        WHERE ct.threshold != %s
                          AND buyins.potential_gains IS NULL
                          AND ct.cycle_end_time IS NOT NULL
                    """, [THRESHOLD])
                    wrong_threshold_cycles = cursor.fetchone()['count']
                    
                    if wrong_threshold_cycles > 0:
                        # Get sample of wrong thresholds
                        cursor.execute("""
                            SELECT ct.threshold, COUNT(*) as cnt
                            FROM follow_the_goat_buyins buyins
                            INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                            WHERE ct.threshold != %s
                              AND buyins.potential_gains IS NULL
                              AND ct.cycle_end_time IS NOT NULL
                            GROUP BY ct.threshold
                        """, [THRESHOLD])
                        wrong_samples = cursor.fetchall()
                        wrong_threshold_str = ", ".join([f"{t['threshold']}({t['cnt']})" for t in wrong_samples])
                    else:
                        wrong_threshold_str = "none"
                    
                    logger.debug(f"Diagnostics - Total buyins: {total_buyins}, "
                               f"With price_cycle: {buyins_with_price_cycle}, "
                               f"NULL potential_gains: {buyins_null_potential}, "
                               f"Valid entry price: {buyins_valid_entry}, "
                               f"With higest_price_reached: {buyins_with_highest}, "
                               f"Completed cycles (threshold={THRESHOLD}): {completed_cycles}/{total_cycles}, "
                               f"Buyins linked to completed: {buyins_linked_to_completed}, "
                               f"Buyins with WRONG threshold: {wrong_threshold_cycles} ({wrong_threshold_str}), "
                               f"Orphaned price_cycle refs: {orphaned_buyins}, "
                               f"Oldest completed cycle: {oldest_cycle_str}, "
                               f"Available thresholds: {', '.join(threshold_list)}")
                    
                    # Warning if we have trades referencing cycles with wrong threshold
                    if wrong_threshold_cycles > 0:
                        logger.warning(f"Found {wrong_threshold_cycles} trades with cycles at WRONG threshold (not {THRESHOLD}): {wrong_threshold_str}. "
                                     f"Trade creation uses hardcoded threshold={THRESHOLD} but cycle might have different threshold!")
                    
                    # Warning if we have orphaned references (cycles were cleaned up too early)
                    if orphaned_buyins > 0:
                        logger.warning(f"Found {orphaned_buyins} orphaned price_cycle references - cycles may have been cleaned up before trades were processed. "
                                     f"Check cycle retention settings (should be 72h to match trades).")
                except Exception as diag_error:
                    logger.debug(f"Diagnostic queries failed (non-critical): {diag_error}")
                
                # Execute both queries and combine results
                results = []
                
                # Get trades with existing cycles
                cursor.execute(query_with_cycle, [THRESHOLD])
                results_with_cycle = cursor.fetchall()
                results.extend([(r['id'], r['potential_gains']) for r in results_with_cycle])
                logger.debug(f"Found {len(results_with_cycle)} buyins with existing cycles")
                
                # Get orphaned trades (cycle archived/deleted)
                cursor.execute(query_orphaned)
                results_orphaned = cursor.fetchall()
                results.extend([(r['id'], r['potential_gains']) for r in results_orphaned])
                if results_orphaned:
                    logger.info(f"Found {len(results_orphaned)} orphaned buyins (cycle archived) - using trade's own highest price")
                
                # CRITICAL: Check if there are trades that SHOULD match but don't
                cursor.execute("""
                    SELECT 
                        buyins.id,
                        buyins.price_cycle,
                        ct.id as cycle_found,
                        ct.threshold,
                        ct.cycle_end_time,
                        buyins.our_entry_price
                    FROM follow_the_goat_buyins buyins
                    LEFT JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                    WHERE buyins.potential_gains IS NULL
                      AND buyins.price_cycle IS NOT NULL
                      AND buyins.our_entry_price IS NOT NULL
                      AND buyins.our_entry_price > 0
                      AND buyins.our_status IN ('sold', 'completed', 'no_go')
                    LIMIT 5
                """)
                potential_missing = cursor.fetchall()
                
                if potential_missing:
                    logger.warning(f"Found {len(potential_missing)} trades with NULL potential_gains that should be calculated:")
                    for row in potential_missing:
                        buyin_id = row['id']
                        price_cycle = row['price_cycle']
                        cycle_found = row['cycle_found']
                        threshold = row['threshold']
                        cycle_end_time = row['cycle_end_time']
                        
                        if cycle_found is None:
                            logger.warning(f"  Buyin #{buyin_id}: price_cycle={price_cycle} NOT FOUND in cycle_tracker (orphaned)")
                        elif cycle_end_time is None:
                            logger.warning(f"  Buyin #{buyin_id}: price_cycle={price_cycle} threshold={threshold} - cycle NOT CLOSED yet")
                        elif threshold != THRESHOLD:
                            logger.warning(f"  Buyin #{buyin_id}: price_cycle={price_cycle} has WRONG threshold={threshold} (expected {THRESHOLD})")
                        else:
                            logger.warning(f"  Buyin #{buyin_id}: price_cycle={price_cycle} threshold={threshold} end={cycle_end_time} - SHOULD work but doesn't?")
                
                return results
    except Exception as e:
        logger.error(f"Error fetching records to update: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def update_potential_gains_postgres(record_id: int, potential_gains_value: float) -> bool:
    """
    Update potential_gains in PostgreSQL for a specific record.
    
    Args:
        record_id: ID of the buyin record
        potential_gains_value: Calculated potential gains percentage
        
    Returns:
        bool: True if update successful
    """
    try:
        rows = postgres_execute(
            "UPDATE follow_the_goat_buyins SET potential_gains = %s WHERE id = %s",
            [potential_gains_value, record_id]
        )
        return rows > 0
    except Exception as e:
        logger.error(f"PostgreSQL update error for record {record_id}: {e}")
        return False


def update_all_potential_gains() -> dict:
    """
    Update potential_gains for all eligible records (PostgreSQL only).
    
    Returns:
        dict: Statistics about the update operation
    """
    records = get_records_to_update()
    
    if not records:
        logger.debug("No records to update for potential_gains")
        return {
            "success": True,
            "total_records": 0,
            "updated": 0,
            "failed": 0
        }
    
    updated_count = 0
    failed_count = 0
    
    for record_id, potential_gains_value in records:
        if potential_gains_value is not None:
            # PostgreSQL only
            pg_ok = update_potential_gains_postgres(record_id, potential_gains_value)
            
            if pg_ok:
                updated_count += 1
                logger.debug(f"Updated record {record_id} with potential_gains: {potential_gains_value:.2f}%")
            else:
                failed_count += 1
                logger.warning(f"Update failed for record {record_id}")
        else:
            logger.debug(f"Skipping record {record_id} - calculated value is NULL")
            failed_count += 1
    
    result = {
        "success": True,
        "total_records": len(records),
        "updated": updated_count,
        "failed": failed_count
    }
    
    if updated_count > 0:
        logger.info(f"Potential gains update: {updated_count}/{len(records)} records updated")
    
    return result


def run():
    """
    Main entry point for the scheduler.
    Called every 15 seconds to update potential_gains.
    """
    return update_all_potential_gains()


if __name__ == "__main__":
    # Configure logging for standalone run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    logger.info("=" * 60)
    logger.info("Starting Potential Gains Update (standalone)")
    logger.info(f"Threshold: {THRESHOLD}")
    logger.info("=" * 60)
    
    result = run()
    
    logger.info("=" * 60)
    logger.info("Update Summary:")
    logger.info(f"  Total Records Found: {result.get('total_records', 0)}")
    logger.info(f"  Successfully Updated: {result.get('updated', 0)}")
    logger.info(f"  Failed Updates: {result.get('failed', 0)}")
    logger.info("=" * 60)


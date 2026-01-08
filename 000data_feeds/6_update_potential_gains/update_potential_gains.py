"""
Update Potential Gains
======================
Migrated from: 000old_code/solana_node/00trades/potential_grains/add_potential_gains.py

Calculates and updates potential_gains for buyins where the price cycle has completed.
Formula: ((highest_price_reached - our_entry_price) / our_entry_price) * 100

Uses threshold = 0.3 for cycle_tracker lookup.
Only updates records where:
- potential_gains IS NULL
- cycle_end_time IS NOT NULL (completed cycles only)
- our_entry_price > 0

CRITICAL FIX (Jan 2026):
- Cycles now have 72-hour retention (matches trades_hot_storage_hours)
- Trades can be active for up to 72 hours, so cycles MUST persist that long
- Before this fix, cycles were cleaned up after 24 hours, leaving orphaned references
- Fallback mechanism uses trade's own higest_price_reached field (updated by trailing_stop)
- All new trades initialize higest_price_reached = our_entry_price at creation

Dual-writes to both DuckDB and MySQL.
"""

import sys
from pathlib import Path
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, duckdb_execute_write

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
    
    Returns:
        list: List of tuples (buyin_id, calculated_potential_gains)
    """
    # Query 1: Trades with existing cycle_tracker records (use cycle's highest_price)
    query_with_cycle = """
    SELECT 
        buyins.id,
        ((ct.highest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100 AS potential_gains
    FROM follow_the_goat_buyins buyins
    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
    WHERE buyins.potential_gains IS NULL 
      AND ct.cycle_end_time IS NOT NULL
      AND ct.threshold = ?
      AND buyins.our_entry_price IS NOT NULL
      AND buyins.our_entry_price > 0
    """
    
    # Query 2: Orphaned trades (cycle archived/deleted) - use trade's own higest_price_reached
    # This handles trades where the cycle was cleaned up from hot storage
    query_orphaned = """
    SELECT 
        buyins.id,
        ((buyins.higest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100 AS potential_gains
    FROM follow_the_goat_buyins buyins
    WHERE buyins.potential_gains IS NULL
      AND buyins.price_cycle IS NOT NULL
      AND buyins.our_status IN ('sold', 'no_go')
      AND buyins.higest_price_reached IS NOT NULL
      AND buyins.higest_price_reached > 0
      AND buyins.our_entry_price IS NOT NULL
      AND buyins.our_entry_price > 0
      AND NOT EXISTS (
          SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins.price_cycle
      )
    """
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            # Diagnostic queries to understand why no records match
            try:
                total_buyins = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins").fetchone()[0]
                buyins_with_price_cycle = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins WHERE price_cycle IS NOT NULL").fetchone()[0]
                buyins_null_potential = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins WHERE potential_gains IS NULL").fetchone()[0]
                buyins_valid_entry = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins WHERE our_entry_price IS NOT NULL AND our_entry_price > 0").fetchone()[0]
                buyins_with_highest = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins WHERE higest_price_reached IS NOT NULL").fetchone()[0]
                
                completed_cycles = cursor.execute("SELECT COUNT(*) FROM cycle_tracker WHERE cycle_end_time IS NOT NULL AND threshold = ?", [THRESHOLD]).fetchone()[0]
                total_cycles = cursor.execute("SELECT COUNT(*) FROM cycle_tracker WHERE threshold = ?", [THRESHOLD]).fetchone()[0]
                
                # Check what threshold values actually exist
                threshold_values = cursor.execute("SELECT DISTINCT threshold FROM cycle_tracker ORDER BY threshold").fetchall()
                threshold_list = [str(t[0]) for t in threshold_values] if threshold_values else []
                
                buyins_linked_to_completed = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins buyins
                    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                    WHERE ct.cycle_end_time IS NOT NULL AND ct.threshold = ?
                """, [THRESHOLD]).fetchone()[0]
                
                # Check for orphaned price_cycle references
                orphaned_buyins = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins buyins
                    WHERE buyins.price_cycle IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins.price_cycle
                      )
                """).fetchone()[0]
                
                # Check oldest completed cycle
                oldest_cycle = cursor.execute("""
                    SELECT MIN(cycle_end_time) as oldest
                    FROM cycle_tracker
                    WHERE cycle_end_time IS NOT NULL AND threshold = ?
                """, [THRESHOLD]).fetchone()
                oldest_cycle_str = str(oldest_cycle[0]) if oldest_cycle and oldest_cycle[0] else "N/A"
                
                # CRITICAL CHECK: Are cycles assigned to trades using the WRONG threshold?
                wrong_threshold_cycles = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins buyins
                    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                    WHERE ct.threshold != ?
                      AND buyins.potential_gains IS NULL
                      AND ct.cycle_end_time IS NOT NULL
                """, [THRESHOLD]).fetchone()[0]
                
                if wrong_threshold_cycles > 0:
                    # Get sample of wrong thresholds
                    wrong_samples = cursor.execute("""
                        SELECT DISTINCT ct.threshold, COUNT(*) as cnt
                        FROM follow_the_goat_buyins buyins
                        INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                        WHERE ct.threshold != ?
                          AND buyins.potential_gains IS NULL
                          AND ct.cycle_end_time IS NOT NULL
                        GROUP BY ct.threshold
                    """, [THRESHOLD]).fetchall()
                    wrong_threshold_str = ", ".join([f"{t[0]}({t[1]})" for t in wrong_samples])
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
            results_with_cycle = cursor.execute(query_with_cycle, [THRESHOLD]).fetchall()
            results.extend(results_with_cycle)
            logger.debug(f"Found {len(results_with_cycle)} buyins with existing cycles")
            
            # Get orphaned trades (cycle archived/deleted)
            results_orphaned = cursor.execute(query_orphaned).fetchall()
            results.extend(results_orphaned)
            if results_orphaned:
                logger.info(f"Found {len(results_orphaned)} orphaned buyins (cycle archived) - using trade's own highest price")
            
            # CRITICAL: Check if there are trades that SHOULD match but don't
            # This catches the case where cycle exists but doesn't meet criteria
            potential_missing = cursor.execute("""
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
                  AND buyins.our_status IN ('sold', 'no_go')
                LIMIT 5
            """).fetchall()
            
            if potential_missing:
                logger.warning(f"Found {len(potential_missing)} trades with NULL potential_gains that should be calculated:")
                for row in potential_missing:
                    buyin_id, price_cycle, cycle_found, threshold, cycle_end_time, entry_price = row
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


def update_potential_gains_duckdb(record_id: int, potential_gains_value: float) -> bool:
    """
    Update potential_gains in DuckDB for a specific record.
    
    Args:
        record_id: ID of the buyin record
        potential_gains_value: Calculated potential gains percentage
        
    Returns:
        bool: True if update successful
    """
    try:
        duckdb_execute_write("central",
            "UPDATE follow_the_goat_buyins SET potential_gains = ? WHERE id = ?",
            [potential_gains_value, record_id]
        )
        return True
    except Exception as e:
        logger.error(f"DuckDB update error for record {record_id}: {e}")
        return False


def update_all_potential_gains() -> dict:
    """
    Update potential_gains for all eligible records (DuckDB only).
    
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
            # DuckDB only - no MySQL
            duck_ok = update_potential_gains_duckdb(record_id, potential_gains_value)
            
            if duck_ok:
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


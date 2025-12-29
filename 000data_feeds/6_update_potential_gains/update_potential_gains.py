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

Dual-writes to both DuckDB and MySQL.
"""

import sys
from pathlib import Path
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, get_mysql

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
    - Uses threshold = 0.3
    - our_entry_price is valid (not null, > 0)
    
    Returns:
        list: List of tuples (buyin_id, calculated_potential_gains)
    """
    query = """
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
    
    try:
        with get_duckdb("central") as conn:
            results = conn.execute(query, [THRESHOLD]).fetchall()
            return results
    except Exception as e:
        logger.error(f"Error fetching records to update: {e}")
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
        with get_duckdb("central") as conn:
            conn.execute(
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


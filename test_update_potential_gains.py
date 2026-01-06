#!/usr/bin/env python3
"""
Diagnostic script to test update_potential_gains functionality.
Checks if there are records that should be updated and tests the query.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, duckdb_execute_write
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("test_update_potential_gains")

THRESHOLD = 0.3

def test_query():
    """Test the query to see if there are records that match the criteria."""
    
    # Test 1: Check if tables exist
    logger.info("=" * 60)
    logger.info("TEST 1: Checking if tables exist")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            # Check follow_the_goat_buyins
            result = cursor.execute("""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_name = 'follow_the_goat_buyins'
            """).fetchone()
            
            if result and result[0] > 0:
                logger.info("✓ follow_the_goat_buyins table exists")
            else:
                logger.error("✗ follow_the_goat_buyins table does NOT exist")
                return
            
            # Check cycle_tracker
            result = cursor.execute("""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_name = 'cycle_tracker'
            """).fetchone()
            
            if result and result[0] > 0:
                logger.info("✓ cycle_tracker table exists")
            else:
                logger.error("✗ cycle_tracker table does NOT exist")
                return
                
    except Exception as e:
        logger.error(f"Error checking tables: {e}")
        return
    
    # Test 2: Count total buyins
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Counting buyins")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins").fetchone()
            total_buyins = result[0] if result else 0
            logger.info(f"Total buyins: {total_buyins}")
            
            if total_buyins == 0:
                logger.warning("⚠ No buyins found in database")
                return
                
    except Exception as e:
        logger.error(f"Error counting buyins: {e}")
        return
    
    # Test 3: Count buyins with NULL potential_gains
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Counting buyins with NULL potential_gains")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("""
                SELECT COUNT(*) 
                FROM follow_the_goat_buyins 
                WHERE potential_gains IS NULL
            """).fetchone()
            null_potential_gains = result[0] if result else 0
            logger.info(f"Buyins with NULL potential_gains: {null_potential_gains}")
            
    except Exception as e:
        logger.error(f"Error counting NULL potential_gains: {e}")
        return
    
    # Test 4: Count buyins with valid entry price
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Counting buyins with valid entry price")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("""
                SELECT COUNT(*) 
                FROM follow_the_goat_buyins 
                WHERE our_entry_price IS NOT NULL 
                  AND our_entry_price > 0
            """).fetchone()
            valid_entry_price = result[0] if result else 0
            logger.info(f"Buyins with valid entry price: {valid_entry_price}")
            
    except Exception as e:
        logger.error(f"Error counting valid entry prices: {e}")
        return
    
    # Test 5: Count completed cycles
    logger.info("\n" + "=" * 60)
    logger.info("TEST 5: Counting completed cycles")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("""
                SELECT COUNT(*) 
                FROM cycle_tracker 
                WHERE cycle_end_time IS NOT NULL 
                  AND threshold = ?
            """, [THRESHOLD]).fetchone()
            completed_cycles = result[0] if result else 0
            logger.info(f"Completed cycles (threshold={THRESHOLD}): {completed_cycles}")
            
    except Exception as e:
        logger.error(f"Error counting completed cycles: {e}")
        return
    
    # Test 6: Count buyins linked to completed cycles
    logger.info("\n" + "=" * 60)
    logger.info("TEST 6: Counting buyins linked to completed cycles")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("""
                SELECT COUNT(*) 
                FROM follow_the_goat_buyins buyins
                INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                WHERE ct.cycle_end_time IS NOT NULL
                  AND ct.threshold = ?
                  AND buyins.our_entry_price IS NOT NULL
                  AND buyins.our_entry_price > 0
            """, [THRESHOLD]).fetchone()
            linked_to_completed = result[0] if result else 0
            logger.info(f"Buyins linked to completed cycles: {linked_to_completed}")
            
    except Exception as e:
        logger.error(f"Error counting linked buyins: {e}")
        return
    
    # Test 7: The actual query - records that should be updated
    logger.info("\n" + "=" * 60)
    logger.info("TEST 7: Records that SHOULD be updated (main query)")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            query = """
            SELECT 
                buyins.id,
                buyins.our_entry_price,
                ct.highest_price_reached,
                ct.cycle_end_time,
                ((ct.highest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100 AS calculated_potential_gains
            FROM follow_the_goat_buyins buyins
            INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
            WHERE buyins.potential_gains IS NULL 
              AND ct.cycle_end_time IS NOT NULL
              AND ct.threshold = ?
              AND buyins.our_entry_price IS NOT NULL
              AND buyins.our_entry_price > 0
            ORDER BY buyins.id DESC
            LIMIT 10
            """
            
            results = cursor.execute(query, [THRESHOLD]).fetchall()
            
            if results:
                logger.info(f"Found {len(results)} records that should be updated (showing first 10):")
                for row in results:
                    buyin_id, entry_price, highest_price, cycle_end, calculated_gains = row
                    logger.info(f"  ID: {buyin_id}, Entry: {entry_price}, High: {highest_price}, "
                              f"Cycle End: {cycle_end}, Calculated Gains: {calculated_gains:.2f}%")
            else:
                logger.warning("⚠ No records found that match the update criteria")
                
                # Debug: Check why no records match
                logger.info("\nDebugging why no records match:")
                
                # Check buyins with price_cycle but NULL potential_gains
                result = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins 
                    WHERE potential_gains IS NULL 
                      AND price_cycle IS NOT NULL
                """).fetchone()
                logger.info(f"  - Buyins with NULL potential_gains AND price_cycle: {result[0] if result else 0}")
                
                # Check if price_cycle values exist in cycle_tracker
                result = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins buyins
                    WHERE buyins.price_cycle IS NOT NULL
                      AND EXISTS (
                          SELECT 1 FROM cycle_tracker ct 
                          WHERE ct.id = buyins.price_cycle
                      )
                """).fetchone()
                logger.info(f"  - Buyins with valid price_cycle references: {result[0] if result else 0}")
                
                # Check buyins with price_cycle pointing to completed cycles
                result = cursor.execute("""
                    SELECT COUNT(*) 
                    FROM follow_the_goat_buyins buyins
                    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                    WHERE ct.cycle_end_time IS NOT NULL
                      AND ct.threshold = ?
                """, [THRESHOLD]).fetchone()
                logger.info(f"  - Buyins linked to completed cycles (threshold={THRESHOLD}): {result[0] if result else 0}")
                
    except Exception as e:
        logger.error(f"Error running main query: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test 8: Sample some buyins to see their state
    logger.info("\n" + "=" * 60)
    logger.info("TEST 8: Sample buyins (last 5)")
    logger.info("=" * 60)
    
    try:
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute("""
                SELECT 
                    id,
                    price_cycle,
                    our_entry_price,
                    potential_gains,
                    our_status
                FROM follow_the_goat_buyins
                ORDER BY id DESC
                LIMIT 5
            """).fetchall()
            
            if result:
                logger.info("Sample buyins:")
                for row in result:
                    buyin_id, price_cycle, entry_price, potential_gains, status = row
                    logger.info(f"  ID: {buyin_id}, Cycle: {price_cycle}, Entry: {entry_price}, "
                              f"Potential Gains: {potential_gains}, Status: {status}")
            else:
                logger.warning("No buyins found")
                
    except Exception as e:
        logger.error(f"Error sampling buyins: {e}")
        return
    
    logger.info("\n" + "=" * 60)
    logger.info("Diagnostic complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    test_query()


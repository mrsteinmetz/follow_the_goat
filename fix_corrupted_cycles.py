#!/usr/bin/env python3
"""
Quick fix script to delete corrupted cycles and verify profile creation.

Problem: Cycles 1-7 have end_time < start_time (data corruption)
Solution: Delete these cycles so only valid completed cycles are used for profiles
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import duckdb_execute_write
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_corrupted_cycles():
    """Delete cycles where end_time is before start_time."""
    
    logger.info("Deleting corrupted cycles from master2's local DuckDB...")
    
    # Delete corrupted cycles via write queue
    duckdb_execute_write(
        "central",
        """
        DELETE FROM cycle_tracker 
        WHERE cycle_end_time IS NOT NULL 
        AND cycle_end_time < cycle_start_time
        """,
        sync=True  # Wait for completion
    )
    
    logger.info("✓ Corrupted cycles deleted")
    
    # Now manually close one active cycle to create test data
    # Get the current latest timestamp
    from core.database import get_duckdb
    from datetime import datetime, timezone
    
    with get_duckdb("central", read_only=True) as cursor:
        # Get latest price timestamp
        result = cursor.execute("""
            SELECT MAX(ts) FROM prices WHERE token = 'SOL'
        """).fetchone()
        
        if result and result[0]:
            latest_ts = result[0]
            logger.info(f"Latest price timestamp: {latest_ts}")
            
            # Close the 0.3% threshold cycle for testing
            duckdb_execute_write(
                "central",
                """
                UPDATE cycle_tracker 
                SET cycle_end_time = ?
                WHERE threshold = 0.3 
                AND cycle_end_time IS NULL
                """,
                [latest_ts],
                sync=True
            )
            
            logger.info("✓ Closed 0.3% cycle for testing")
            
            # Verify we have a valid completed cycle
            with get_duckdb("central", read_only=True) as cursor2:
                result = cursor2.execute("""
                    SELECT COUNT(*) FROM cycle_tracker
                    WHERE cycle_end_time IS NOT NULL
                    AND cycle_end_time >= cycle_start_time
                """).fetchone()
                
                valid_cycles = result[0] if result else 0
                logger.info(f"✓ Valid completed cycles: {valid_cycles}")
                
                if valid_cycles > 0:
                    # Check if profiles can now be created
                    result2 = cursor2.execute("""
                        SELECT COUNT(*) 
                        FROM sol_stablecoin_trades t
                        INNER JOIN cycle_tracker c ON (
                            c.threshold = 0.3
                            AND c.cycle_start_time <= t.trade_timestamp
                            AND c.cycle_end_time >= t.trade_timestamp
                            AND c.cycle_end_time IS NOT NULL
                        )
                        WHERE t.direction = 'buy'
                    """).fetchone()
                    
                    matching_trades = result2[0] if result2 else 0
                    logger.info(f"✓ Trades matching completed cycles: {matching_trades}")
                    
                    if matching_trades > 0:
                        logger.info("SUCCESS! Profile creation should work now.")
                        logger.info("Wait 5-10 seconds for the next scheduler run.")
                    else:
                        logger.warning("No trades match the completed cycle time range")
        else:
            logger.error("No prices found in database")

if __name__ == "__main__":
    try:
        fix_corrupted_cycles()
    except Exception as e:
        logger.error(f"Fix failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


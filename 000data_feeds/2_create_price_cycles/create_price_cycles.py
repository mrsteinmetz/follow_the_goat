"""
Price Cycle Analysis - Using DuckDB Connection Pool
====================================================
Migrated from: 000old_code/solana_node/analyze/00price_analysis/price_analysis_simple.py

Reads price data from DuckDB (via connection pool) and tracks price cycles
at multiple thresholds, writing results to the same DuckDB instance.

When run from master2.py: Uses master2.py's local in-memory DuckDB (registered as "central")
When run standalone: Uses the default DuckDB connection

Thresholds: 0.2%, 0.25%, 0.3%, 0.35%, 0.4%, 0.45%, 0.5%
Coin: SOL only (coin_id = 5)

CYCLE LOGIC:
- A cycle tracks price movement from a start point
- A cycle ENDS when price drops X% below the HIGHEST price reached in that cycle
- There can only be 7 active cycles at any time (one per threshold)
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb
from core.config import settings

# Configure logging
logger = logging.getLogger("price_cycles")

# --- Configuration ---
THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]
COIN_ID = 5  # SOL
BATCH_SIZE = 100  # Process up to 100 price points per run


# =============================================================================
# Data Access Functions (Using DuckDB connection pool)
# =============================================================================

def get_last_processed_ts() -> Optional[datetime]:
    """Get the timestamp of the last processed price point from DuckDB."""
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("""
                SELECT MAX(created_at) as max_ts FROM price_analysis WHERE coin_id = ?
            """, [COIN_ID]).fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        logger.debug(f"No previous data found: {e}")
        return None


def get_new_price_points(last_ts: Optional[datetime], limit: int = BATCH_SIZE) -> List[Dict]:
    """Get new price points from DuckDB.
    
    Reads from the prices table (synced from master.py).
    """
    try:
        with get_duckdb("central", read_only=True) as conn:
            if last_ts:
                # Continue from where we left off
                results = conn.execute("""
                    SELECT ts as created_at, token, price as value
                    FROM prices
                    WHERE token = 'SOL' AND ts > ?
                    ORDER BY ts ASC
                    LIMIT ?
                """, [last_ts, limit]).fetchall()
            else:
                # FRESH START: Only process data from the last 1 hour
                logger.info("Fresh start detected - only processing last 1 hour of price data")
                results = conn.execute("""
                    SELECT ts as created_at, token, price as value
                    FROM prices
                    WHERE token = 'SOL' AND ts >= NOW() - INTERVAL 1 HOUR
                    ORDER BY ts ASC
                    LIMIT ?
                """, [limit]).fetchall()
            
            # Convert to list of dicts with sequential IDs
            return [
                {'id': i, 'ts': row[0], 'token': row[1], 'price': float(row[2])}
                for i, row in enumerate(results)
            ]
    except Exception as e:
        logger.error(f"Failed to get price points: {e}")
        return []


def get_threshold_states() -> Dict[float, Dict]:
    """Load current state for each threshold from DuckDB."""
    states = {}
    try:
        with get_duckdb("central", read_only=True) as conn:
            for threshold in THRESHOLDS:
                result = conn.execute("""
                    SELECT 
                        sequence_start_id,
                        sequence_start_price,
                        highest_price_recorded,
                        lowest_price_recorded,
                        price_cycle
                    FROM price_analysis 
                    WHERE coin_id = ? AND percent_threshold = ?
                    ORDER BY id DESC
                    LIMIT 1
                """, [COIN_ID, threshold]).fetchone()
                
                if result:
                    states[threshold] = {
                        'sequence_start_id': result[0],
                        'sequence_start_price': float(result[1]),
                        'highest_price_recorded': float(result[2]),
                        'lowest_price_recorded': float(result[3]),
                        'price_cycle': result[4]
                    }
                
    except Exception as e:
        logger.error(f"Failed to load threshold states: {e}")
    
    return states


# =============================================================================
# Cycle Management
# =============================================================================

def get_next_cycle_id() -> int:
    """Get the next available cycle ID from DuckDB."""
    max_id = 0
    
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("SELECT MAX(id) as max_id FROM cycle_tracker").fetchone()
            if result and result[0]:
                max_id = result[0]
    except:
        pass
    
    return max_id + 1


def get_next_analysis_id() -> int:
    """Get the next available price_analysis ID from DuckDB."""
    max_id = 0
    
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("SELECT MAX(id) as max_id FROM price_analysis").fetchone()
            if result and result[0]:
                max_id = result[0]
    except:
        pass
    
    return max_id + 1


def get_active_cycle_for_threshold(threshold: float) -> Optional[int]:
    """Get the active cycle ID for a threshold (if any)."""
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("""
                SELECT id FROM cycle_tracker
                WHERE coin_id = ? AND threshold = ? AND cycle_end_time IS NULL
                ORDER BY cycle_start_time DESC
                LIMIT 1
            """, [COIN_ID, threshold]).fetchone()
            return result[0] if result else None
    except:
        return None


def close_all_active_cycles_for_threshold(threshold: float, end_time: datetime):
    """Close ALL active cycles for a threshold (cleanup duplicates)."""
    try:
        with get_duckdb("central") as conn:
            conn.execute("""
                UPDATE cycle_tracker 
                SET cycle_end_time = ?
                WHERE coin_id = ? AND threshold = ? AND cycle_end_time IS NULL
            """, [end_time, COIN_ID, threshold])
    except Exception as e:
        logger.debug(f"Failed to close active cycles for {threshold}%: {e}")


def create_new_cycle(
    threshold: float,
    start_time: datetime,
    sequence_start_id: int,
    start_price: float
) -> Optional[int]:
    """
    Create a new cycle in DuckDB.
    
    IMPORTANT: First closes any existing active cycles for this threshold
    to ensure only ONE active cycle per threshold at any time.
    """
    # Close any existing active cycles for this threshold first
    # This prevents duplicate active cycles
    close_all_active_cycles_for_threshold(threshold, start_time)
    
    cycle_id = get_next_cycle_id()
    
    try:
        with get_duckdb("central") as conn:
            conn.execute("""
                INSERT INTO cycle_tracker (
                    id, coin_id, threshold, cycle_start_time, cycle_end_time,
                    sequence_start_id, sequence_start_price, highest_price_reached,
                    lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                    total_data_points, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                cycle_id, COIN_ID, threshold, start_time, None,
                sequence_start_id, start_price, start_price,
                start_price, 0.0, 0.0,
                1, datetime.now()
            ])
            logger.info(f"Created cycle #{cycle_id} for threshold {threshold}%")
            return cycle_id
    except Exception as e:
        logger.error(f"Cycle insert failed: {e}")
        return None


def close_cycle(cycle_id: int, end_time: datetime):
    """Close a cycle by setting its end time."""
    try:
        with get_duckdb("central") as conn:
            conn.execute("""
                UPDATE cycle_tracker SET cycle_end_time = ? WHERE id = ?
            """, [end_time, cycle_id])
        logger.debug(f"Closed cycle #{cycle_id}")
    except Exception as e:
        logger.error(f"Cycle close failed: {e}")


def update_cycle_stats(
    cycle_id: int,
    highest_price: float,
    lowest_price: float,
    max_increase: float,
    max_from_lowest: float
):
    """Update cycle statistics."""
    try:
        with get_duckdb("central") as conn:
            conn.execute("""
                UPDATE cycle_tracker SET
                    total_data_points = total_data_points + 1,
                    highest_price_reached = GREATEST(highest_price_reached, ?),
                    lowest_price_reached = LEAST(lowest_price_reached, ?),
                    max_percent_increase = GREATEST(max_percent_increase, ?),
                    max_percent_increase_from_lowest = GREATEST(max_percent_increase_from_lowest, ?)
                WHERE id = ?
            """, [highest_price, lowest_price, max_increase, max_from_lowest, cycle_id])
    except Exception as e:
        logger.debug(f"Cycle stats update skipped: {e}")


# =============================================================================
# Price Analysis Processing
# =============================================================================

def insert_price_analysis_batch(records: List[tuple]) -> bool:
    """Batch insert price analysis records to DuckDB."""
    if not records:
        return True
    
    try:
        with get_duckdb("central") as conn:
            for record in records:
                conn.execute("""
                    INSERT INTO price_analysis (
                        id, coin_id, price_point_id, sequence_start_id, sequence_start_price,
                        current_price, percent_threshold, percent_increase, highest_price_recorded,
                        lowest_price_recorded, procent_change_from_highest_price_recorded,
                        percent_increase_from_lowest, price_cycle, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, list(record))
            return True
    except Exception as e:
        logger.error(f"Price analysis insert failed: {e}")
        return False


def process_price_point(
    price_data: Dict,
    threshold_states: Dict[float, Dict],
    next_analysis_id: int
) -> tuple[List[tuple], int]:
    """
    Process a single price point across all thresholds.
    
    Returns:
        Tuple of (records_to_insert, updated_next_id)
    """
    current_price = price_data['price']
    created_at = price_data['ts']
    # Use actual database ID for sequence tracking (matching old logic)
    price_point_id = price_data['id']
    
    records = []
    current_id = next_analysis_id
    
    for threshold in THRESHOLDS:
        state = threshold_states.get(threshold)
        
        if state:
            # Continue existing sequence
            sequence_start_id = state['sequence_start_id']
            sequence_start_price = state['sequence_start_price']
            previous_highest = state['highest_price_recorded']
            previous_lowest = state['lowest_price_recorded']
            price_cycle = state['price_cycle']
            
            # Update highest/lowest FIRST (within current cycle)
            current_highest = max(previous_highest, current_price)
            current_lowest = min(previous_lowest, current_price)
            
            # Check if we need to reset the cycle
            # A cycle ends when price drops X% from the HIGHEST price reached in this cycle
            reset_cycle = False
            drop_percentage = ((current_price - current_highest) / current_highest) * 100
            if drop_percentage <= -threshold:
                reset_cycle = True
                logger.debug(f"Cycle reset: {threshold}% threshold, price ${current_price:.4f} dropped {drop_percentage:.4f}% from highest ${current_highest:.4f}")
            
            if reset_cycle:
                # Close the previous cycle
                close_cycle(price_cycle, created_at)
                
                # Start new cycle
                sequence_start_id = price_point_id
                sequence_start_price = current_price
                highest_price_recorded = current_price
                lowest_price_recorded = current_price
                price_cycle = create_new_cycle(threshold, created_at, sequence_start_id, current_price)
                if price_cycle is None:
                    continue
            else:
                # Continue current cycle - use the updated highest/lowest
                highest_price_recorded = current_highest
                lowest_price_recorded = current_lowest
                
                # Update cycle stats
                percent_increase = ((highest_price_recorded - sequence_start_price) / sequence_start_price) * 100 if sequence_start_price > 0 else 0.0
                percent_from_lowest = ((current_price - lowest_price_recorded) / lowest_price_recorded) * 100 if lowest_price_recorded > 0 else 0.0
                update_cycle_stats(price_cycle, highest_price_recorded, lowest_price_recorded, percent_increase, percent_from_lowest)
        else:
            # First record for this threshold
            sequence_start_id = price_point_id
            sequence_start_price = current_price
            highest_price_recorded = current_price
            lowest_price_recorded = current_price
            price_cycle = create_new_cycle(threshold, created_at, sequence_start_id, current_price)
            if price_cycle is None:
                continue
        
        # Calculate percentages
        percent_increase = ((highest_price_recorded - sequence_start_price) / sequence_start_price) * 100 if sequence_start_price > 0 else 0.0
        change_from_highest = ((current_price - highest_price_recorded) / highest_price_recorded) * 100 if highest_price_recorded > 0 else 0.0
        increase_from_lowest = ((current_price - lowest_price_recorded) / lowest_price_recorded) * 100 if lowest_price_recorded > 0 else 0.0
        
        # Prepare record
        records.append((
            current_id,
            COIN_ID,
            price_point_id,
            sequence_start_id,
            sequence_start_price,
            current_price,
            threshold,
            percent_increase,
            highest_price_recorded,
            lowest_price_recorded,
            change_from_highest,
            increase_from_lowest,
            price_cycle,
            created_at
        ))
        current_id += 1
        
        # Update in-memory state for next iteration
        threshold_states[threshold] = {
            'sequence_start_id': sequence_start_id,
            'sequence_start_price': sequence_start_price,
            'highest_price_recorded': highest_price_recorded,
            'lowest_price_recorded': lowest_price_recorded,
            'price_cycle': price_cycle
        }
    
    return records, current_id


# =============================================================================
# Main Entry Point
# =============================================================================

def process_price_cycles() -> int:
    """
    Main entry point for the scheduler.
    Process new price points and create price cycle analysis.
    
    Returns:
        Number of price points processed
    """
    # Get last processed timestamp
    last_ts = get_last_processed_ts()
    
    # Get new price points
    price_points = get_new_price_points(last_ts, BATCH_SIZE)
    
    if not price_points:
        logger.debug("No new price points to process")
        return 0
    
    logger.info(f"Processing {len(price_points)} new price points")
    
    # Load current threshold states
    threshold_states = get_threshold_states()
    
    # Get starting ID for new records
    next_id = get_next_analysis_id()
    
    # Process all price points
    all_records = []
    for price_data in price_points:
        records, next_id = process_price_point(price_data, threshold_states, next_id)
        all_records.extend(records)
    
    # Batch insert all records
    if all_records:
        success = insert_price_analysis_batch(all_records)
        if success:
            logger.info(f"Inserted {len(all_records)} price analysis records ({len(price_points)} price points x {len(THRESHOLDS)} thresholds)")
        else:
            logger.error("Failed to insert price analysis records")
    
    return len(price_points)


def run_continuous(interval_seconds: int = 5):
    """Run price cycle processing continuously (for testing)."""
    import time
    
    logger.info(f"Starting continuous processing (interval: {interval_seconds}s)")
    logger.info(f"Thresholds: {THRESHOLDS}")
    logger.info(f"Reading from: DuckDB 'central' connection")
    
    try:
        while True:
            processed = process_price_cycles()
            if processed > 0:
                logger.info(f"Processed {processed} price points")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    # Configure logging for standalone run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        run_continuous()
    else:
        # Single run
        processed = process_price_cycles()
        print(f"Processed {processed} price points")

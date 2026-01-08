"""
Price Cycle Analysis - PostgreSQL Only
========================================
Migrated from: 000old_code/solana_node/analyze/00price_analysis/price_analysis_simple.py

Reads price data from PostgreSQL and tracks price cycles at multiple 
thresholds, writing results back to PostgreSQL.

ARCHITECTURE:
- ALL reads/writes go directly to PostgreSQL
- No in-memory database or caching layer
- Simple, reliable, persistent

Thresholds: 0.2%, 0.25%, 0.3%, 0.35%, 0.4%, 0.45%, 0.5%
Coin: SOL only (coin_id = 5)

CYCLE LOGIC:
- A cycle tracks price movement from a start point
- A cycle ENDS when price drops X% below the HIGHEST price reached in that cycle
- There can only be 7 active cycles at any time (one per threshold)
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute, postgres_query
from core.config import settings

# Configure logging
logger = logging.getLogger("price_cycles")

# --- Configuration ---
THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]
COIN_ID = 5  # SOL
BATCH_SIZE = 100  # Process up to 100 price points per run


# =============================================================================
# Data Access Functions (Using PostgreSQL)
# =============================================================================

def get_last_processed_price_point_id() -> tuple[Optional[int], int]:
    """
    Get the lowest max processed price_point_id across all thresholds and
    return how many thresholds currently have data.

    Using IDs prevents skipping rows that share the same timestamp, which
    ensures we never miss a drop that should close a cycle.
    """
    thresholds_with_data = 0
    last_id = None
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(DISTINCT percent_threshold) 
                    FROM price_analysis 
                    WHERE coin_id = %s
                """, [COIN_ID])
                result = cursor.fetchone()
                thresholds_with_data = result['count'] if result else 0

                # If any threshold has no data, force a fresh start (recent window)
                if thresholds_with_data < len(THRESHOLDS):
                    return None, thresholds_with_data

                cursor.execute("""
                    SELECT MIN(max_price_point_id) as min_id FROM (
                        SELECT MAX(price_point_id) AS max_price_point_id
                        FROM price_analysis
                        WHERE coin_id = %s
                        GROUP BY percent_threshold
                    ) sub
                """, [COIN_ID])
                result = cursor.fetchone()
                last_id = int(result['min_id']) if result and result['min_id'] is not None else None
    except Exception as e:
        logger.debug(f"No previous data found: {e}")
        last_id = None
    
    return last_id, thresholds_with_data


def get_new_price_points(last_price_point_id: Optional[int], limit: int = BATCH_SIZE) -> List[Dict]:
    """Get new price points from PostgreSQL using strictly increasing IDs."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                if last_price_point_id is not None:
                    # Continue from the last processed ID
                    cursor.execute("""
                        SELECT id, timestamp as ts, token, price
                        FROM prices
                        WHERE token = 'SOL' AND id > %s
                        ORDER BY id ASC
                        LIMIT %s
                    """, [last_price_point_id, limit])
                else:
                    # FRESH START: Process recent data window to avoid blocking on startup
                    logger.info("Fresh start detected - processing recent price data (last 24 hours)")
                    cursor.execute("""
                        SELECT id, timestamp as ts, token, price
                        FROM prices
                        WHERE token = 'SOL' AND timestamp >= NOW() - INTERVAL '24 hours'
                        ORDER BY id ASC
                        LIMIT %s
                    """, [limit])

                results = cursor.fetchall()
                return [
                    {'id': row['id'], 'ts': row['ts'], 'token': row['token'], 'price': float(row['price'])}
                    for row in results
                ]
    except Exception as e:
        logger.error(f"Failed to get price points: {e}")
        return []


def get_current_price() -> Optional[float]:
    """Get the current SOL price from PostgreSQL."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT price, timestamp
                    FROM prices
                    WHERE token = 'SOL'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                return float(result['price']) if result and result['price'] else None
    except Exception as e:
        logger.debug(f"Failed to get current price: {e}")
        return None


def get_threshold_states() -> Dict[float, Dict]:
    """
    Load current state for each threshold from DuckDB.
    
    CRITICAL: Always uses the ACTIVE cycle for each threshold from cycle_tracker.
    This ensures we never write to closed cycles after a restart or cycle reset.
    """
    states = {}
    try:
        with get_postgres() as conn:
            for threshold in THRESHOLDS:
                # STEP 1: Get the active cycle ID for this threshold
                cycle_result = conn.execute("""
                    SELECT id, sequence_start_id, sequence_start_price
                    FROM cycle_tracker
                    WHERE coin_id = %s AND threshold = ? AND cycle_end_time IS NULL
                    ORDER BY cycle_start_time DESC
                    LIMIT 1
                """, [COIN_ID, threshold]).fetchone()
                
                if not cycle_result:
                    # No active cycle exists for this threshold (shouldn't happen after ensure_all_cycles_exist)
                    continue
                
                active_cycle_id = cycle_result[0]
                sequence_start_id = cycle_result[1]
                sequence_start_price = float(cycle_result[2])
                
                # STEP 2: Get the latest price analysis for this active cycle
                # This gives us the current highest/lowest prices
                analysis_result = conn.execute("""
                    SELECT 
                        highest_price_recorded,
                        lowest_price_recorded
                    FROM price_analysis 
                    WHERE price_cycle = %s AND coin_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, [active_cycle_id, COIN_ID]).fetchone()
                
                if analysis_result:
                    # Use the tracked highest/lowest from price_analysis
                    highest_price = float(analysis_result[0])
                    lowest_price = float(analysis_result[1])
                else:
                    # No price_analysis records yet for this cycle - use cycle_tracker values
                    cycle_tracker_result = conn.execute("""
                        SELECT highest_price_reached, lowest_price_reached
                        FROM cycle_tracker
                        WHERE id = %s
                    """, [active_cycle_id]).fetchone()
                    
                    if cycle_tracker_result:
                        highest_price = float(cycle_tracker_result[0])
                        lowest_price = float(cycle_tracker_result[1])
                    else:
                        # Fallback to sequence_start_price
                        highest_price = sequence_start_price
                        lowest_price = sequence_start_price
                
                states[threshold] = {
                    'sequence_start_id': sequence_start_id,
                    'sequence_start_price': sequence_start_price,
                    'highest_price_recorded': highest_price,
                    'lowest_price_recorded': lowest_price,
                    'price_cycle': active_cycle_id
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
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(id) as max_id FROM cycle_tracker")
                result = cursor.fetchone()
                if result and result['max_id']:
                    max_id = result['max_id']
    except:
        pass
    
    return max_id + 1


def get_next_analysis_id() -> int:
    """Get the next available price_analysis ID from DuckDB."""
    max_id = 0
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(id) as max_id FROM price_analysis")
                result = cursor.fetchone()
                if result and result['max_id']:
                    max_id = result['max_id']
    except:
        pass
    
    return max_id + 1


def get_active_cycle_for_threshold(threshold: float) -> Optional[int]:
    """Get the active cycle ID for a threshold (if any)."""
    try:
        with get_postgres() as conn:
            result = conn.execute("""
                SELECT id FROM cycle_tracker
                WHERE coin_id = %s AND threshold = ? AND cycle_end_time IS NULL
                ORDER BY cycle_start_time DESC
                LIMIT 1
            """, [COIN_ID, threshold]).fetchone()
            return result[0] if result else None
    except:
        return None


def close_all_active_cycles_for_threshold(threshold: float, end_time: datetime):
    """Close ALL active cycles for a threshold (cleanup duplicates)."""
    try:
        postgres_execute("""
            UPDATE cycle_tracker 
            SET cycle_end_time = ?
            WHERE coin_id = %s AND threshold = ? AND cycle_end_time IS NULL
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
    
    NOTE: When closing previous cycles during a reset, we use the current price's timestamp
    as the end time. This means the old cycle's end_time equals the new cycle's start_time.
    This is correct because the price that triggers the reset becomes the start of the new cycle.
    """
    # Close any existing active cycles for this threshold first
    # This prevents duplicate active cycles
    # IMPORTANT: Don't close cycles if this is first-time initialization
    # Check if any active cycle exists before trying to close
    active_cycle_id = get_active_cycle_for_threshold(threshold)
    if active_cycle_id is not None:
        # Close the existing active cycle with the new cycle's start time
        # This is correct: the price that ends one cycle starts the next
        close_cycle(active_cycle_id, start_time)
    
    cycle_id = get_next_cycle_id()
    
    try:
        postgres_execute("""
            INSERT INTO cycle_tracker (
                id, coin_id, threshold, cycle_start_time, cycle_end_time,
                sequence_start_id, sequence_start_price, highest_price_reached,
                lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                total_data_points, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
        """, [
            cycle_id, COIN_ID, threshold, start_time, None,
            sequence_start_id, start_price, start_price,
            start_price, 0.0, 0.0,
            1, datetime.now(timezone.utc)
        ])
        logger.info(f"Created cycle #{cycle_id} for threshold {threshold}%")
        return cycle_id
    except Exception as e:
        logger.error(f"Cycle insert failed: {e}")
        return None


def close_cycle(cycle_id: int, end_time: datetime):
    """Close a cycle by setting its end time."""
    try:
        # CRITICAL VALIDATION: Prevent closing cycles with end_time < start_time
        # This was causing data corruption during initialization
        with get_postgres() as conn:
            result = conn.execute("""
                SELECT cycle_start_time FROM cycle_tracker WHERE id = %s
            """, [cycle_id]).fetchone()
            
            if result:
                start_time = result[0]
                if end_time < start_time:
                    logger.error(f"PREVENTED DATA CORRUPTION: Cycle #{cycle_id} end_time ({end_time}) < start_time ({start_time})")
                    return
        
        postgres_execute("""
            UPDATE cycle_tracker SET cycle_end_time = ? WHERE id = %s
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
        postgres_execute("""
            UPDATE cycle_tracker SET
                total_data_points = total_data_points + 1,
                highest_price_reached = GREATEST(highest_price_reached, ?),
                lowest_price_reached = LEAST(lowest_price_reached, ?),
                max_percent_increase = GREATEST(max_percent_increase, ?),
                max_percent_increase_from_lowest = GREATEST(max_percent_increase_from_lowest, ?)
            WHERE id = %s
        """, [highest_price, lowest_price, max_increase, max_from_lowest, cycle_id])
    except Exception as e:
        logger.debug(f"Cycle stats update skipped: {e}")


# =============================================================================
# Price Analysis Processing
# =============================================================================

def insert_price_analysis_batch(records: List[tuple]) -> bool:
    """
    Batch insert price analysis records to DuckDB using PyArrow for maximum speed.
    
    PyArrow achieves ~1000x faster inserts than executemany():
    - executemany: ~70,000 records in 10-20 minutes
    - PyArrow: ~70,000 records in 0.5-1 second
    
    IMPORTANT: Writes go to FILE-BASED DuckDB for persistence and webpage visibility.
    """
    if not records:
        return True
    
    try:
        import pyarrow as pa
        import pandas as pd
        from datetime import datetime
        
        # Convert records (tuples) to DataFrame for PyArrow conversion
        columns = [
            'id', 'coin_id', 'price_point_id', 'sequence_start_id', 'sequence_start_price',
            'current_price', 'percent_threshold', 'percent_increase', 'highest_price_recorded',
            'lowest_price_recorded', 'procent_change_from_highest_price_recorded',
            'percent_increase_from_lowest', 'price_cycle', 'created_at'
        ]
        
        # Convert to dict for pandas (columnar format)
        data_dict = {col: [] for col in columns}
        for record in records:
            for i, col in enumerate(columns):
                data_dict[col].append(record[i])
        
        # Create pandas DataFrame (fast for PyArrow conversion)
        df = pd.DataFrame(data_dict)
        
        # Convert to PyArrow Table with explicit schema
        schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('coin_id', pa.int32()),
            pa.field('price_point_id', pa.int64()),
            pa.field('sequence_start_id', pa.int64()),
            pa.field('sequence_start_price', pa.float64()),
            pa.field('current_price', pa.float64()),
            pa.field('percent_threshold', pa.float64()),
            pa.field('percent_increase', pa.float64()),
            pa.field('highest_price_recorded', pa.float64()),
            pa.field('lowest_price_recorded', pa.float64()),
            pa.field('procent_change_from_highest_price_recorded', pa.float64()),
            pa.field('percent_increase_from_lowest', pa.float64()),
            pa.field('price_cycle', pa.int64()),
            pa.field('created_at', pa.timestamp('us')),
        ])
        
        # Convert datetime column if needed
        if df['created_at'].dtype == 'object':
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        arrow_table = pa.Table.from_pandas(df, schema=schema)
        
        # Insert using TradingDataEngine's connection (in-memory DuckDB)
        # Use engine.get_connection() which provides raw DuckDB access for PyArrow
        global _global_engine
        if _global_engine and hasattr(_global_engine, 'get_connection'):
            with _global_engine.get_connection() as conn:
                conn.register('_temp_price_analysis', arrow_table)
                cols_str = ', '.join(columns)
                conn.execute(f"INSERT INTO price_analysis ({cols_str}) SELECT {cols_str} FROM _temp_price_analysis")
                conn.unregister('_temp_price_analysis')
        else:
            # Fallback for standalone testing - use get_duckdb
            with get_postgres() as conn:
                # Note: EngineConnectionWrapper may not support .register()
                # Use individual inserts instead
                for record in records:
                    conn.execute("""
                        INSERT INTO price_analysis (
                            id, coin_id, price_point_id, sequence_start_id, sequence_start_price,
                            current_price, percent_threshold, percent_increase, highest_price_recorded,
                            lowest_price_recorded, procent_change_from_highest_price_recorded,
                            percent_increase_from_lowest, price_cycle, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, list(record))
        
        logger.debug(f"PyArrow insert complete: {len(records)} price_analysis records")
        return True
        
    except ImportError:
        # Fallback to executemany if PyArrow not available
        logger.warning("PyArrow not available, falling back to slower executemany()")
        try:
            for record in records:
                postgres_execute("""
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
            
    except Exception as e:
        import traceback
        logger.error(f"Price analysis PyArrow insert failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
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
                # CRITICAL: Update cycle stats BEFORE closing to capture the peak
                # Use the highest/lowest prices reached BEFORE the reset-triggering price
                # 
                # Why use previous_highest/previous_lowest instead of current_highest/current_lowest?
                # - current_lowest includes the reset-triggering price (if it's a new low)
                # - But you can't buy at the reset price and sell at the peak (reset price comes AFTER peak)
                # - So the achievable gain is from previous_lowest to previous_highest
                # 
                # Note: current_highest = previous_highest in all reset scenarios because:
                # - Reset triggers when current_price drops X% from highest
                # - If current_price > previous_highest, drop would be 0%, no reset
                # - So we use previous_highest for clarity (same value, but clearer intent)
                highest_price_recorded = previous_highest
                lowest_price_recorded = previous_lowest
                percent_increase = ((highest_price_recorded - sequence_start_price) / sequence_start_price) * 100 if sequence_start_price > 0 else 0.0
                # Calculate max achievable gain: from cycle's low to cycle's peak
                # Both must have occurred BEFORE the reset to be a valid trading opportunity
                percent_from_lowest = ((highest_price_recorded - lowest_price_recorded) / lowest_price_recorded) * 100 if lowest_price_recorded > 0 else 0.0
                update_cycle_stats(price_cycle, highest_price_recorded, lowest_price_recorded, percent_increase, percent_from_lowest)
                
                # Close the previous cycle (now with final stats recorded)
                # Use created_at as the end time (the timestamp when the drop occurred)
                close_cycle(price_cycle, created_at)
                
                # Start new cycle
                # NOTE: New cycle's start_time = created_at (same as old cycle's end_time)
                # This is correct: the price that ends one cycle starts the next
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
# Cycle Initialization
# =============================================================================

def ensure_all_cycles_exist():
    """
    Ensure all 7 thresholds have active cycles.
    
    This is called even when there are no new price points to process,
    ensuring that cycles are always initialized for all thresholds.
    """
    current_price = get_current_price()
    if current_price is None:
        logger.debug("Cannot ensure cycles exist - no current price available")
        return
    
    current_time = datetime.now(timezone.utc)
    cycles_created = 0
    
    # FIRST: Clean up any corrupted cycles (end_time < start_time)
    cleanup_corrupted_cycles()
    
    # SECOND: Clean up duplicate active cycles (race condition fix)
    cleanup_duplicate_active_cycles()
    
    # Get the latest price_point_id to use as sequence_start_id
    # This ensures we have a valid reference point
    sequence_start_id = 1  # Default fallback
    try:
        with get_postgres() as conn:
            # Use the latest ID from the prices table to align with processing cursor
            result = conn.execute("""
                SELECT MAX(id) as max_id FROM prices WHERE token = 'SOL'
            """).fetchone()
            if result and result[0]:
                sequence_start_id = result[0]
            else:
                # Fallback: use count from prices table as approximation
                result = conn.execute("""
                    SELECT COUNT(*) as cnt FROM prices WHERE token = 'SOL'
                """).fetchone()
                if result and result[0]:
                    sequence_start_id = result[0]
    except Exception as e:
        logger.debug(f"Could not determine sequence_start_id: {e}, using default")
    
    for threshold in THRESHOLDS:
        # Check if this threshold has an active cycle
        active_cycle_id = get_active_cycle_for_threshold(threshold)
        
        if active_cycle_id is None:
            # No active cycle exists - create one
            cycle_id = create_new_cycle(threshold, current_time, sequence_start_id, current_price)
            if cycle_id:
                cycles_created += 1
                logger.debug(f"Created missing cycle #{cycle_id} for threshold {threshold}%")
    
    if cycles_created > 0:
        logger.info(f"Initialized {cycles_created} missing cycles (current price: ${current_price:.4f})")


def cleanup_corrupted_cycles():
    """
    Delete corrupted cycles where end_time < start_time.
    
    This is a safety cleanup that runs on startup to fix any data corruption
    from previous bugs. Should normally not find any issues after the fix.
    """
    try:
        # First, count how many corrupted cycles exist
        with get_postgres() as conn:
            result = conn.execute("""
                SELECT COUNT(*) FROM cycle_tracker
                WHERE cycle_end_time IS NOT NULL
                AND cycle_end_time < cycle_start_time
            """).fetchone()
            
            corrupted_count = result[0] if result else 0
        
        if corrupted_count > 0:
            logger.warning(f"Found {corrupted_count} corrupted cycles (end_time < start_time) - deleting...")
            
            postgres_execute("""
                DELETE FROM cycle_tracker
                WHERE cycle_end_time IS NOT NULL
                AND cycle_end_time < cycle_start_time
            """, [])
            
            logger.info(f"✓ Deleted {corrupted_count} corrupted cycles from master.py TradingDataEngine")
    except Exception as e:
        logger.error(f"Failed to cleanup corrupted cycles: {e}")


def cleanup_duplicate_active_cycles():
    """
    Fix duplicate active cycles for the same threshold.
    
    CRITICAL: There should only be ONE active cycle per threshold at any time.
    If multiple active cycles exist for a threshold, keep the one with the most
    data points and close the others.
    
    This can happen during startup if historical processing and ensure_all_cycles_exist()
    run simultaneously.
    """
    try:
        thresholds = THRESHOLDS
        fixed_count = 0
        
        for threshold in thresholds:
            # Get all active cycles for this threshold
            with get_postgres() as conn:
                result = conn.execute("""
                    SELECT id, total_data_points, cycle_start_time
                    FROM cycle_tracker
                    WHERE threshold = %s AND cycle_end_time IS NULL
                    ORDER BY total_data_points DESC, id ASC
                """, [threshold]).fetchall()
            
            if len(result) > 1:
                # Keep the first one (most data points), close the rest
                keep_cycle_id = result[0][0]
                logger.warning(f"Threshold {threshold}%: Found {len(result)} active cycles - keeping #{keep_cycle_id}, closing {len(result)-1} duplicates")
                
                for row in result[1:]:
                    cycle_id = row[0]
                    start_time = row[2]
                    
                    # Close with the same start_time (it never really processed any data)
                    postgres_execute("""
                        UPDATE cycle_tracker
                        SET cycle_end_time = ?
                        WHERE id = %s
                    """, [start_time, cycle_id])
                    
                    logger.info(f"  Closed duplicate cycle #{cycle_id} for threshold {threshold}%")
                    fixed_count += 1
        
        if fixed_count > 0:
            logger.info(f"✓ Closed {fixed_count} duplicate active cycles")
        
        return fixed_count
    except Exception as e:
        logger.error(f"Failed to cleanup duplicate cycles: {e}")
        return 0


# =============================================================================
# Main Entry Point
# =============================================================================

def process_all_historical_prices(batch_size: int = 1000) -> int:
    """
    Process ALL unprocessed historical price points (for startup after backfill).
    
    CRITICAL: This processes ALL price points in chronological order, not just a batch.
    It loops continuously until every single price point has been processed into cycles.
    
    OPTIMIZED: Releases DuckDB lock between batches to avoid blocking API/scheduler.
    
    Args:
        batch_size: Number of price points to process per batch (default: 1000)
                    This is just for memory efficiency - ALL prices will be processed.
    
    Returns:
        Total number of price points processed
    """
    logger.info("Processing ALL historical price points for cycle calculation...")
    logger.info(f"Will process in batches of {batch_size} until ALL prices are processed")
    
    # SKIP the expensive COUNT query - just process until empty
    # This avoids a full table scan that might block other operations
    
    # Ensure all 7 cycles exist FIRST
    ensure_all_cycles_exist()
    
    total_processed = 0
    batch_count = 0
    max_iterations = 100000  # Safety limit to prevent infinite loops
    
    next_id = get_next_analysis_id()
    
    iteration = 0
    while iteration < max_iterations:
        iteration += 1
        
        threshold_states = get_threshold_states()
        
        last_price_point_id, _ = get_last_processed_price_point_id()
        
        price_points = get_new_price_points(last_price_point_id, batch_size)
        
        if not price_points:
            # No more unprocessed prices - we're done!
            logger.info(f"No more unprocessed prices found - all historical data processed")
            break
        
        batch_count += 1
        logger.info(f"Processing batch {batch_count}: {len(price_points)} price points (total processed so far: {total_processed})")
        
        # Process all price points in this batch
        all_records = []
        for price_data in price_points:
            records, next_id = process_price_point(price_data, threshold_states, next_id)
            all_records.extend(records)
        
        # Batch insert all records using PyArrow (FAST!)
        if all_records:
            success = insert_price_analysis_batch(all_records)
            if success:
                total_processed += len(price_points)
                logger.info(f"  Batch {batch_count}: Inserted {len(all_records)} price_analysis records (total: {total_processed} price points)")
            else:
                logger.error(f"  Batch {batch_count}: Failed to insert price analysis records")
                break
        else:
            logger.warning(f"  Batch {batch_count}: No records generated (skipping)")
            break
        
        # CRITICAL: Small sleep to allow other threads/processes to access DuckDB
        # This prevents blocking the API server and scheduler
        import time
        time.sleep(0.01)  # 10ms pause between batches - allows lock release
    
    if iteration >= max_iterations:
        logger.error(f"Safety limit reached ({max_iterations} iterations) - stopping to prevent infinite loop")
    
    if total_processed > 0:
        logger.info(f"Historical processing complete: {total_processed} price points processed in {batch_count} batches")
    else:
        logger.info("No historical prices to process (all cycles already calculated)")
    
    return total_processed


def process_price_cycles() -> int:
    """
    Main entry point for the scheduler.
    Process new price points and create price cycle analysis.
    
    CRITICAL: Always ensures all thresholds have active cycles,
    even when there are no new price points to process.
    
    Returns:
    """
    # CRITICAL: Cleanup any duplicate active cycles first (fix race conditions from startup)
    cleanup_duplicate_active_cycles()
    
    # CRITICAL: Ensure all cycles exist FIRST (before processing new points)
    # This guarantees that all thresholds always have active cycles
    ensure_all_cycles_exist()
    
    # Get the earliest last processed price_point_id across all thresholds
    # to avoid skipping any drops when thresholds are fully populated.
    last_price_point_id, thresholds_with_data = get_last_processed_price_point_id()
    if thresholds_with_data < len(THRESHOLDS):
        logger.info(f"New thresholds detected ({thresholds_with_data}/{len(THRESHOLDS)}) - starting from recent history")
    
    # Get new price points
    price_points = get_new_price_points(last_price_point_id, BATCH_SIZE)
    
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

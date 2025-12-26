"""
Price Cycle Analysis - Using TradingDataEngine (In-Memory DuckDB)
=================================================================
Migrated from: 000old_code/solana_node/analyze/00price_analysis/price_analysis_simple.py

Reads price data from MySQL (master source) and tracks price cycles
at multiple thresholds, writing results to:
- TradingDataEngine (in-memory DuckDB for fast 24hr hot storage)
- MySQL (historical persistence)

Thresholds: 0.2%, 0.25%, 0.3%, 0.35%, 0.4%, 0.45%, 0.5%
Coin: SOL only (coin_id = 5)

IMPORTANT: This module uses the TradingDataEngine singleton which runs in-memory.
No file locks, no contention - just fast reads and writes.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_mysql, get_trading_engine
from core.config import settings

# Configure logging
logger = logging.getLogger("price_cycles")

# --- Configuration ---
THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]
COIN_ID = 5  # SOL
BATCH_SIZE = 100  # Process up to 100 price points per run


# =============================================================================
# MySQL Table Initialization
# =============================================================================

def ensure_mysql_tables():
    """Ensure MySQL tables exist for price_analysis and cycle_tracker."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Create price_analysis table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS price_analysis (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        coin_id INT NOT NULL,
                        price_point_id BIGINT NOT NULL,
                        sequence_start_id BIGINT NOT NULL,
                        sequence_start_price DECIMAL(20, 8) NOT NULL,
                        current_price DECIMAL(20, 8) NOT NULL,
                        percent_threshold DECIMAL(5, 2) NOT NULL DEFAULT 0.1,
                        percent_increase DECIMAL(10, 4) NOT NULL,
                        highest_price_recorded DECIMAL(20, 8) NOT NULL,
                        lowest_price_recorded DECIMAL(20, 8) NOT NULL,
                        procent_change_from_highest_price_recorded DECIMAL(10, 4) NOT NULL DEFAULT 0.0,
                        percent_increase_from_lowest DECIMAL(10, 4) NOT NULL DEFAULT 0.0,
                        price_cycle BIGINT NOT NULL,
                        created_at DATETIME NOT NULL,
                        processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        highest_climb FLOAT NULL,
                        INDEX idx_price_point_id (price_point_id),
                        INDEX idx_price_cycle (price_cycle),
                        INDEX idx_created_at (created_at),
                        INDEX idx_coin_threshold_id (coin_id, percent_threshold, id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # Alter existing table if sequence_start_id is INT (migration)
                try:
                    cursor.execute("ALTER TABLE price_analysis MODIFY COLUMN sequence_start_id BIGINT NOT NULL")
                    cursor.execute("ALTER TABLE price_analysis MODIFY COLUMN price_point_id BIGINT NOT NULL")
                except:
                    pass  # Column already correct type
                
                # Create cycle_tracker table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cycle_tracker (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        coin_id INT NOT NULL,
                        threshold DECIMAL(5, 2) NOT NULL,
                        cycle_start_time DATETIME NOT NULL,
                        cycle_end_time DATETIME NULL,
                        sequence_start_id BIGINT NOT NULL,
                        sequence_start_price DECIMAL(20, 8) NOT NULL,
                        highest_price_reached DECIMAL(20, 8) NOT NULL,
                        lowest_price_reached DECIMAL(20, 8) NOT NULL,
                        max_percent_increase DECIMAL(10, 4) NOT NULL,
                        max_percent_increase_from_lowest DECIMAL(10, 4) NOT NULL,
                        total_data_points INT NOT NULL DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_coin_threshold (coin_id, threshold),
                        INDEX idx_cycle_start (cycle_start_time)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                # Alter existing table if sequence_start_id is INT (migration)
                try:
                    cursor.execute("ALTER TABLE cycle_tracker MODIFY COLUMN sequence_start_id BIGINT NOT NULL")
                except:
                    pass  # Column already correct type
                
                logger.debug("MySQL tables verified/created")
                return True
    except Exception as e:
        logger.error(f"Failed to ensure MySQL tables: {e}")
        return False


# =============================================================================
# Data Access Functions (Using TradingDataEngine for reads)
# =============================================================================

def get_last_processed_ts() -> Optional[datetime]:
    """Get the timestamp of the last processed price point from TradingDataEngine."""
    try:
        engine = get_trading_engine()
        if not engine._running:
            # Fall back to MySQL if engine not running
            return get_last_processed_ts_mysql()
        
        result = engine.read_one("""
            SELECT MAX(created_at) as max_ts FROM price_analysis WHERE coin_id = ?
        """, [COIN_ID])
        return result['max_ts'] if result and result['max_ts'] else None
    except Exception as e:
        logger.debug(f"No previous data found in engine, trying MySQL: {e}")
        return get_last_processed_ts_mysql()


def get_last_processed_ts_mysql() -> Optional[datetime]:
    """Fallback: Get the timestamp of the last processed price point from MySQL."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT MAX(created_at) as max_ts FROM price_analysis WHERE coin_id = %s
                """, [COIN_ID])
                result = cursor.fetchone()
                return result['max_ts'] if result and result.get('max_ts') else None
    except Exception as e:
        logger.debug(f"No previous data found in MySQL: {e}")
        return None


def get_new_price_points(last_ts: Optional[datetime], limit: int = BATCH_SIZE) -> List[Dict]:
    """Get new price points from MySQL.
    
    We read from MySQL because:
    1. MySQL handles multi-process access well
    2. The Jupiter fetcher writes to MySQL (dual-write with TradingEngine)
    3. MySQL is the master source of truth for historical data
    
    IMPORTANT: When starting fresh (no last_ts), we only process recent data
    (last 1 hour) to avoid processing months of historical data.
    """
    from datetime import timedelta
    
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                if last_ts:
                    # Continue from where we left off
                    cursor.execute("""
                        SELECT id, created_at, 'SOL' as token, value
                        FROM price_points
                        WHERE coin_id = %s AND created_at > %s
                        ORDER BY created_at ASC
                        LIMIT %s
                    """, [COIN_ID, last_ts, limit])
                else:
                    # FRESH START: Only process data from the last 1 hour
                    # This prevents processing months of historical data
                    logger.info("Fresh start detected - only processing last 1 hour of price data")
                    cursor.execute("""
                        SELECT id, created_at, 'SOL' as token, value
                        FROM price_points
                        WHERE coin_id = %s AND created_at >= NOW() - INTERVAL 1 HOUR
                        ORDER BY created_at ASC
                        LIMIT %s
                    """, [COIN_ID, limit])
                
                rows = cursor.fetchall()
                return [
                    {'id': row['id'], 'ts': row['created_at'], 'token': row['token'], 'price': float(row['value'])}
                    for row in rows
                ]
    except Exception as e:
        logger.error(f"Failed to get price points from MySQL: {e}")
        return []


def get_threshold_states() -> Dict[float, Dict]:
    """Load current state for each threshold from TradingDataEngine."""
    states = {}
    try:
        engine = get_trading_engine()
        if not engine._running:
            return get_threshold_states_mysql()
        
        for threshold in THRESHOLDS:
            result = engine.read_one("""
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
            """, [COIN_ID, threshold])
            
            if result:
                states[threshold] = {
                    'sequence_start_id': result['sequence_start_id'],
                    'sequence_start_price': float(result['sequence_start_price']),
                    'highest_price_recorded': float(result['highest_price_recorded']),
                    'lowest_price_recorded': float(result['lowest_price_recorded']),
                    'price_cycle': result['price_cycle']
                }
        
        # If engine returned no states, fall back to MySQL
        if not states:
            return get_threshold_states_mysql()
            
    except Exception as e:
        logger.error(f"Failed to load threshold states from engine, trying MySQL: {e}")
        return get_threshold_states_mysql()
    
    return states


def get_threshold_states_mysql() -> Dict[float, Dict]:
    """Fallback: Load current state for each threshold from MySQL."""
    states = {}
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                for threshold in THRESHOLDS:
                    cursor.execute("""
                        SELECT 
                            sequence_start_id,
                            sequence_start_price,
                            highest_price_recorded,
                            lowest_price_recorded,
                            price_cycle
                        FROM price_analysis 
                        WHERE coin_id = %s AND percent_threshold = %s
                        ORDER BY id DESC
                        LIMIT 1
                    """, [COIN_ID, threshold])
                    result = cursor.fetchone()
                    
                    if result:
                        states[threshold] = {
                            'sequence_start_id': result['sequence_start_id'],
                            'sequence_start_price': float(result['sequence_start_price']),
                            'highest_price_recorded': float(result['highest_price_recorded']),
                            'lowest_price_recorded': float(result['lowest_price_recorded']),
                            'price_cycle': result['price_cycle']
                        }
    except Exception as e:
        logger.error(f"Failed to load threshold states from MySQL: {e}")
    
    return states


# =============================================================================
# Cycle Management
# =============================================================================

def get_next_cycle_id() -> int:
    """Get the next available cycle ID."""
    max_id = 0
    
    # Check TradingDataEngine first
    try:
        engine = get_trading_engine()
        if engine._running:
            result = engine.read_one("SELECT MAX(id) as max_id FROM cycle_tracker", [])
            if result and result['max_id']:
                max_id = max(max_id, result['max_id'])
    except:
        pass
    
    # Check MySQL (master source)
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(id) as max_id FROM cycle_tracker")
                result = cursor.fetchone()
                if result and result.get('max_id'):
                    max_id = max(max_id, result['max_id'])
    except:
        pass
    
    return max_id + 1


def get_next_analysis_id() -> int:
    """Get the next available price_analysis ID."""
    max_id = 0
    
    # Check TradingDataEngine first
    try:
        engine = get_trading_engine()
        if engine._running:
            result = engine.read_one("SELECT MAX(id) as max_id FROM price_analysis", [])
            if result and result['max_id']:
                max_id = max(max_id, result['max_id'])
    except:
        pass
    
    # Check MySQL (master source)
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT MAX(id) as max_id FROM price_analysis")
                result = cursor.fetchone()
                if result and result.get('max_id'):
                    max_id = max(max_id, result['max_id'])
    except:
        pass
    
    return max_id + 1


def create_new_cycle(
    threshold: float,
    start_time: datetime,
    sequence_start_id: int,
    start_price: float
) -> Optional[int]:
    """Create a new cycle with dual-write to TradingDataEngine and MySQL."""
    cycle_id = get_next_cycle_id()
    
    engine_ok = False
    mysql_ok = False
    
    # Write to TradingDataEngine (in-memory DuckDB)
    try:
        engine = get_trading_engine()
        if engine._running:
            engine.write('cycle_tracker', {
                'id': cycle_id,
                'coin_id': COIN_ID,
                'threshold': threshold,
                'cycle_start_time': start_time,
                'cycle_end_time': None,
                'sequence_start_id': sequence_start_id,
                'sequence_start_price': start_price,
                'highest_price_reached': start_price,
                'lowest_price_reached': start_price,
                'max_percent_increase': 0.0,
                'max_percent_increase_from_lowest': 0.0,
                'total_data_points': 1,
                'created_at': datetime.now()
            })
            engine_ok = True
    except Exception as e:
        logger.error(f"TradingEngine cycle insert failed: {e}")
    
    # Write to MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cycle_tracker 
                    (id, coin_id, threshold, cycle_start_time, sequence_start_id,
                     sequence_start_price, highest_price_reached, lowest_price_reached,
                     max_percent_increase, max_percent_increase_from_lowest, total_data_points)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 1)
                """, [cycle_id, COIN_ID, threshold, start_time, sequence_start_id,
                      start_price, start_price, start_price])
                mysql_ok = True
    except Exception as e:
        logger.error(f"MySQL cycle insert failed: {e}")
    
    if engine_ok or mysql_ok:
        logger.info(f"Created cycle #{cycle_id} for threshold {threshold}% (Engine: {engine_ok}, MySQL: {mysql_ok})")
        return cycle_id
    
    return None


def close_cycle(cycle_id: int, end_time: datetime):
    """Close a cycle by setting its end time."""
    # Update TradingDataEngine
    try:
        engine = get_trading_engine()
        if engine._running:
            engine.execute("""
                UPDATE cycle_tracker SET cycle_end_time = ? WHERE id = ?
            """, [end_time, cycle_id])
    except Exception as e:
        logger.error(f"TradingEngine cycle close failed: {e}")
    
    # Update MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE cycle_tracker SET cycle_end_time = %s WHERE id = %s
                """, [end_time, cycle_id])
    except Exception as e:
        logger.error(f"MySQL cycle close failed: {e}")
    
    logger.debug(f"Closed cycle #{cycle_id}")


def update_cycle_stats(
    cycle_id: int,
    highest_price: float,
    lowest_price: float,
    max_increase: float,
    max_from_lowest: float
):
    """Update cycle statistics."""
    # Update TradingDataEngine
    try:
        engine = get_trading_engine()
        if engine._running:
            engine.execute("""
                UPDATE cycle_tracker SET
                    total_data_points = total_data_points + 1,
                    highest_price_reached = GREATEST(highest_price_reached, ?),
                    lowest_price_reached = LEAST(lowest_price_reached, ?),
                    max_percent_increase = GREATEST(max_percent_increase, ?),
                    max_percent_increase_from_lowest = GREATEST(max_percent_increase_from_lowest, ?)
                WHERE id = ?
            """, [highest_price, lowest_price, max_increase, max_from_lowest, cycle_id])
    except Exception as e:
        logger.debug(f"TradingEngine cycle stats update skipped: {e}")
    
    # Update MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE cycle_tracker SET
                        total_data_points = total_data_points + 1,
                        highest_price_reached = GREATEST(highest_price_reached, %s),
                        lowest_price_reached = LEAST(lowest_price_reached, %s),
                        max_percent_increase = GREATEST(max_percent_increase, %s),
                        max_percent_increase_from_lowest = GREATEST(max_percent_increase_from_lowest, %s)
                    WHERE id = %s
                """, [highest_price, lowest_price, max_increase, max_from_lowest, cycle_id])
    except Exception as e:
        logger.error(f"MySQL cycle stats update failed: {e}")


# =============================================================================
# Price Analysis Processing
# =============================================================================

def insert_price_analysis_batch(records: List[tuple]) -> bool:
    """Batch insert price analysis records with dual-write."""
    if not records:
        return True
    
    engine_ok = False
    mysql_ok = False
    
    # Write to TradingDataEngine (in-memory DuckDB)
    try:
        engine = get_trading_engine()
        if engine._running:
            for record in records:
                # Convert tuple to dict for engine.write()
                engine.write('price_analysis', {
                    'id': record[0],
                    'coin_id': record[1],
                    'price_point_id': record[2],
                    'sequence_start_id': record[3],
                    'sequence_start_price': record[4],
                    'current_price': record[5],
                    'percent_threshold': record[6],
                    'percent_increase': record[7],
                    'highest_price_recorded': record[8],
                    'lowest_price_recorded': record[9],
                    'procent_change_from_highest_price_recorded': record[10],
                    'percent_increase_from_lowest': record[11],
                    'price_cycle': record[12],
                    'created_at': record[13]
                })
            engine_ok = True
    except Exception as e:
        logger.error(f"TradingEngine price_analysis insert failed: {e}")
    
    # Write to MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO price_analysis 
                    (id, coin_id, price_point_id, sequence_start_id, sequence_start_price,
                     current_price, percent_threshold, percent_increase, highest_price_recorded,
                     lowest_price_recorded, procent_change_from_highest_price_recorded,
                     percent_increase_from_lowest, price_cycle, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, records)
                mysql_ok = True
    except Exception as e:
        logger.error(f"MySQL price_analysis insert failed: {e}")
    
    return engine_ok or mysql_ok


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
            
            # Check if we need to reset the cycle
            reset_cycle = False
            if current_price < previous_highest:
                drop_percentage = ((current_price - previous_highest) / previous_highest) * 100
                if drop_percentage < -threshold:
                    reset_cycle = True
                    logger.debug(f"Cycle reset: {threshold}% threshold, drop {drop_percentage:.4f}%")
            
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
                # Continue current sequence
                highest_price_recorded = max(previous_highest, current_price)
                lowest_price_recorded = min(previous_lowest, current_price)
                
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
    # Ensure MySQL tables exist (TradingDataEngine tables are created at engine startup)
    ensure_mysql_tables()
    
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
    logger.info(f"Reading from: MySQL price_points (coin_id={COIN_ID})")
    
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

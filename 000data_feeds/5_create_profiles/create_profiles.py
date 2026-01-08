"""
Wallet Profile Builder - DuckDB In-Memory (Fast Version)
=========================================================
Migrated from: 000old_code/solana_node/analyze/profiles_v2/create_profiles.py

Builds wallet profiles by joining:
- sol_stablecoin_trades (buy trades from wallets) - FROM DUCKDB
- cycle_tracker (completed price cycles) - FROM DUCKDB
- price_points (to get trade entry price) - FROM DUCKDB (synced from Jupiter prices)

Writes results to:
- In-memory DuckDB (via master2.py local instance or TradingDataEngine)

PERFORMANCE: All reads from in-memory DuckDB (1000x faster than PostgreSQL).
             - No network latency
             - Columnar storage optimized for analytical JOINs
             - All data in RAM for instant access

Thresholds: All from cycle_tracker (0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5)

IMPORTANT: Only processes trades within COMPLETED cycles (cycle_end_time IS NOT NULL)

NOTE: This module can be used in two modes:
1. With master.py's TradingDataEngine (via get_duckdb("central"))
2. With master2.py's local DuckDB (via build_profiles_for_local_duckdb())
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any, Set
import logging
import time

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import (
    get_duckdb,
    duckdb_execute_write,
    get_postgres,
    write_batch_to_postgres_async,
)
from core.config import settings
from features.price_api.schema import (
    SCHEMA_SOL_STABLECOIN_TRADES,
    SCHEMA_CYCLE_TRACKER,
    SCHEMA_PRICE_POINTS,
    SCHEMA_WALLET_PROFILES,
)

# Configure logging
logger = logging.getLogger("wallet_profiles")

# --- Configuration ---
MIN_BUYS = 3  # Minimum buy trades to qualify a wallet
BATCH_SIZE = 50000  # Large DuckDB/Arrow batch to cut iterations

# All thresholds from cycle_tracker (matching create_price_cycles.py)
# Temporarily narrowed to 0.3 to reduce runtime
THRESHOLDS = [0.3]


# =============================================================================
# State Management (DuckDB + PostgreSQL for persistence)
# =============================================================================

def _pg_get_last_processed_id(threshold: float) -> Optional[int]:
    """Best-effort read of wallet profile state from PostgreSQL."""
    try:
        with get_postgres() as pg_conn:
            if not pg_conn:
                return None
            with pg_conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT last_trade_id 
                    FROM wallet_profiles_state 
                    WHERE threshold = %s
                    """,
                    [float(threshold)]
                )
                result = cursor.fetchone()
                return int(result['last_trade_id']) if result else None
    except Exception as e:
        logger.debug(f"PostgreSQL state read failed (non-blocking): {e}")
        return None


def _pg_update_last_processed_id(threshold: float, last_trade_id: int) -> None:
    """Best-effort upsert of wallet profile state into PostgreSQL."""
    try:
        with get_postgres() as pg_conn:
            if not pg_conn:
                return
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                        id SERIAL PRIMARY KEY,
                        threshold DECIMAL(5,2) NOT NULL UNIQUE,
                        last_trade_id BIGINT NOT NULL DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    INSERT INTO wallet_profiles_state (threshold, last_trade_id, last_updated)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (threshold) DO UPDATE SET
                        last_trade_id = EXCLUDED.last_trade_id,
                        last_updated = NOW()
                """, [float(threshold), int(last_trade_id)])
    except Exception as e:
        logger.debug(f"PostgreSQL state write skipped: {e}")


def get_last_processed_id(threshold: float) -> int:
    """Get the last processed trade ID for a threshold (PostgreSQL first)."""
    pg_value = _pg_get_last_processed_id(threshold)
    if pg_value is not None:
        return pg_value
    
    try:
        # Ensure table exists (write operation via queue)
        duckdb_execute_write("central", """
            CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                id INTEGER PRIMARY KEY,
                threshold DOUBLE NOT NULL UNIQUE,
                last_trade_id BIGINT NOT NULL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, sync=True)
        
        # Read state (read operation)
        with get_duckdb("central", read_only=True) as cursor:
            result = cursor.execute(
                "SELECT last_trade_id FROM wallet_profiles_state WHERE threshold = ?",
                [threshold]
            ).fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.debug(f"No state found for threshold {threshold}: {e}")
        return 0


def update_last_processed_id(threshold: float, last_trade_id: int):
    """Update the last processed trade ID for a threshold in DuckDB and PostgreSQL."""
    _pg_update_last_processed_id(threshold, last_trade_id)
    try:
        # Ensure table exists (write operation via queue)
        duckdb_execute_write("central", """
            CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                id INTEGER PRIMARY KEY,
                threshold DOUBLE NOT NULL UNIQUE,
                last_trade_id BIGINT NOT NULL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Upsert using DuckDB syntax
        duckdb_execute_write("central", """
            INSERT INTO wallet_profiles_state (threshold, last_trade_id, last_updated)
            VALUES (?, ?, NOW())
            ON CONFLICT (threshold) DO UPDATE SET 
                last_trade_id = excluded.last_trade_id,
                last_updated = NOW()
        """, [threshold, last_trade_id])
    except Exception as e:
        logger.error(f"Failed to update state for threshold {threshold}: {e}")


# =============================================================================
# DuckDB Data Access (FAST - all reads from local DuckDB)
# =============================================================================

def ensure_duckdb_tables():
    """Ensure DuckDB tables exist (idempotent)."""
    try:
        duckdb_execute_write("central", SCHEMA_SOL_STABLECOIN_TRADES, sync=True)
        duckdb_execute_write("central", SCHEMA_CYCLE_TRACKER, sync=True)
        duckdb_execute_write("central", SCHEMA_PRICE_POINTS, sync=True)
        duckdb_execute_write("central", SCHEMA_WALLET_PROFILES, sync=True)
        return True
    except Exception as e:
        logger.error(f"Failed to ensure DuckDB tables: {e}")
        return False


def get_eligible_wallets_duckdb() -> Set[str]:
    """
    Get wallets that have at least MIN_BUYS buy trades.
    Uses DuckDB for fast aggregation.
    """
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("""
                SELECT wallet_address
                FROM sol_stablecoin_trades
                WHERE direction = 'buy'
                GROUP BY wallet_address
                HAVING COUNT(id) >= ?
            """, [MIN_BUYS]).fetchall()
            return set(row[0] for row in result)
    except Exception as e:
        logger.error(f"Failed to get eligible wallets from DuckDB: {e}")
        return set()


def get_latest_completed_cycle_end_duckdb(threshold: float) -> Optional[datetime]:
    """Get the end time of the most recent completed cycle for a threshold."""
    try:
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("""
                SELECT MAX(cycle_end_time) as max_end
                FROM cycle_tracker
                WHERE threshold = ? AND cycle_end_time IS NOT NULL
            """, [threshold]).fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        logger.error(f"Failed to get latest cycle end from DuckDB: {e}")
        return None


def build_profiles_batch_duckdb(
    threshold: float,
    last_trade_id: int,
    latest_cycle_end: datetime,
    batch_size: int = BATCH_SIZE,
    conn=None
) -> List[Dict]:
    """
    Build wallet profiles by joining trades with cycles entirely in DuckDB.
    
    This is the FAST path - single query that does:
    1. Filters eligible wallets (MIN_BUYS requirement)
    2. Joins trades with completed cycles
    3. Gets entry price from price_points table (synced from Jupiter SOL prices)
    
    Args:
        threshold: Price cycle threshold (e.g., 0.3 for 0.3%)
        last_trade_id: Last processed trade ID for incremental processing
        latest_cycle_end: Latest completed cycle end time
        batch_size: Number of trades to process in one batch
        conn: Optional DuckDB connection (for master2.py local DuckDB)
    
    Returns list of profile dicts ready for insert.
    """
    def _execute_query(connection):
        # Single efficient query that does all the work in DuckDB
        # Uses a CTE for eligible wallets to avoid large IN clause
        # NOTE: Uses 'price_points' table (synced from prices) for SOL price lookups
        result = connection.execute("""
            WITH eligible_wallets AS (
                SELECT wallet_address
                FROM sol_stablecoin_trades
                WHERE direction = 'buy'
                GROUP BY wallet_address
                HAVING COUNT(id) >= ?
            ),
            trades_with_cycles AS (
                SELECT 
                    t.id as trade_id,
                    t.wallet_address,
                    t.trade_timestamp,
                    t.price as trade_price,
                    t.stablecoin_amount,
                    t.perp_direction,
                    c.id as cycle_id,
                    c.cycle_start_time,
                    c.cycle_end_time,
                    c.sequence_start_price,
                    c.highest_price_reached,
                    c.lowest_price_reached
                FROM sol_stablecoin_trades t
                INNER JOIN eligible_wallets ew ON t.wallet_address = ew.wallet_address
                INNER JOIN cycle_tracker c ON (
                    c.threshold = ?
                    AND c.cycle_start_time <= t.trade_timestamp
                    AND c.cycle_end_time >= t.trade_timestamp
                    AND c.cycle_end_time IS NOT NULL
                )
                WHERE t.direction = 'buy'
                AND t.trade_timestamp <= ?
                AND t.id > ?
                ORDER BY t.id ASC
                LIMIT ?
            ),
            -- Get entry price: first price_point after trade_timestamp
            -- Uses price_points table (synced from prices, created_at = timestamp, value = price)
            trades_with_prices AS (
                SELECT 
                    twc.*,
                    (
                        SELECT pp.value 
                        FROM price_points pp 
                        WHERE pp.created_at > twc.trade_timestamp 
                        ORDER BY pp.id ASC 
                        LIMIT 1
                    ) as entry_price
                FROM trades_with_cycles twc
            )
            SELECT *
            FROM trades_with_prices
            WHERE entry_price IS NOT NULL
        """, [MIN_BUYS, threshold, latest_cycle_end, last_trade_id, batch_size]).fetchall()
        
        # Get column names
        columns = [desc[0] for desc in connection.description]
        return result, columns
    
    try:
        # Use provided connection or get from pool
        if conn is not None:
            result, columns = _execute_query(conn)
        else:
            with get_duckdb("central", read_only=True) as connection:
                result, columns = _execute_query(connection)
        
        # Build profile records
        profiles = []
        for row in result:
            record = dict(zip(columns, row))
            
            # Calculate short value based on perp_direction
            perp_direction = record.get('perp_direction')
            if perp_direction == 'long':
                short_value = 0
            elif perp_direction == 'short':
                short_value = 1
            else:
                short_value = 2  # null or empty
            
            profiles.append({
                'wallet_address': record['wallet_address'],
                'threshold': threshold,
                'trade_id': record['trade_id'],
                'trade_timestamp': record['trade_timestamp'],
                'price_cycle': record['cycle_id'],
                'price_cycle_start_time': record['cycle_start_time'],
                'price_cycle_end_time': record['cycle_end_time'],
                'trade_entry_price_org': float(record['trade_price']) if record['trade_price'] else 0,
                'stablecoin_amount': record['stablecoin_amount'],
                'trade_entry_price': float(record['entry_price']),
                'sequence_start_price': float(record['sequence_start_price']),
                'highest_price_reached': float(record['highest_price_reached']),
                'lowest_price_reached': float(record['lowest_price_reached']),
                'long_short': perp_direction,
                'short': short_value
            })
        
        return profiles
    except Exception as e:
        logger.error(f"Failed to build profiles from DuckDB: {e}")
        return []


def get_max_trade_id_in_batch(profiles: List[Dict], fallback_id: int) -> int:
    """Get the maximum trade_id from a batch of profiles."""
    if not profiles:
        return fallback_id
    return max(p['trade_id'] for p in profiles)


# =============================================================================
# Profile Building (Main Logic)
# =============================================================================

def build_profiles_for_threshold(threshold: float) -> int:
    """
    Build wallet profiles for a specific threshold.
    Uses DuckDB for all reads (FAST), dual-write for output.
    
    Returns number of profiles inserted.
    """
    # Get latest completed cycle end time for this threshold
    latest_cycle_end = get_latest_completed_cycle_end_duckdb(threshold)
    if not latest_cycle_end:
        logger.debug(f"No completed cycles for threshold {threshold}")
        return 0
    
    # Get last processed trade ID
    last_trade_id = get_last_processed_id(threshold)
    
    # Build profiles using DuckDB (single efficient query)
    profiles = build_profiles_batch_duckdb(
        threshold, last_trade_id, latest_cycle_end
    )
    
    if not profiles:
        return 0
    
    # Insert profiles with dual-write
    inserted = insert_profiles_batch(profiles)
    
    # Update state with last processed trade ID
    max_id = get_max_trade_id_in_batch(profiles, last_trade_id)
    update_last_processed_id(threshold, max_id)
    
    return inserted


def _prepare_pg_profiles(profiles: List[Dict], starting_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Shape profile rows for PostgreSQL writes."""
    pg_rows: List[Dict[str, Any]] = []
    for i, profile in enumerate(profiles):
        assigned_id = profile.get('id') if starting_id is None else starting_id + i
        if assigned_id is None:
            assigned_id = i
        pg_rows.append({
            'id': assigned_id,
            'wallet_address': profile['wallet_address'],
            'threshold': profile['threshold'],
            'trade_id': profile['trade_id'],
            'trade_timestamp': profile['trade_timestamp'],
            'price_cycle': profile['price_cycle'],
            'price_cycle_start_time': profile['price_cycle_start_time'],
            'price_cycle_end_time': profile['price_cycle_end_time'],
            'trade_entry_price_org': profile['trade_entry_price_org'],
            'stablecoin_amount': profile['stablecoin_amount'],
            'trade_entry_price': profile['trade_entry_price'],
            'sequence_start_price': profile['sequence_start_price'],
            'highest_price_reached': profile['highest_price_reached'],
            'lowest_price_reached': profile['lowest_price_reached'],
            'long_short': profile['long_short'],
            'short': profile['short'],
        })
    return pg_rows


def insert_profiles_batch(profiles: List[Dict]) -> int:
    """
    Insert profiles into DuckDB.
    Returns number of records inserted.
    """
    if not profiles:
        return 0
    
    duckdb_ok = False
    inserted_count = 0
    
    # Write to DuckDB (central.duckdb)
    try:
        # Get next ID (read operation)
        with get_duckdb("central", read_only=True) as cursor:
            max_id_result = cursor.execute("SELECT COALESCE(MAX(id), 0) FROM wallet_profiles").fetchone()
            next_id = (max_id_result[0] or 0) + 1
        
        # Prepare batch insert data
        batch_data = []
        pg_rows = _prepare_pg_profiles(profiles, next_id)
        for i, profile in enumerate(profiles):
            batch_data.append([
                next_id + i,
                profile['wallet_address'],
                profile['threshold'],
                profile['trade_id'],
                profile['trade_timestamp'],
                profile['price_cycle'],
                profile['price_cycle_start_time'],
                profile['price_cycle_end_time'],
                profile['trade_entry_price_org'],
                profile['stablecoin_amount'],
                profile['trade_entry_price'],
                profile['sequence_start_price'],
                profile['highest_price_reached'],
                profile['lowest_price_reached'],
                profile['long_short'],
                profile['short'],
            ])
        
        # Batch insert via write queue
        # Define helper for executemany
        def _batch_insert(conn, data):
            conn.executemany("""
                INSERT OR IGNORE INTO wallet_profiles 
                (id, wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                 price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                 stablecoin_amount, trade_entry_price, sequence_start_price,
                 highest_price_reached, lowest_price_reached, long_short, short)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
        
        from scheduler.master2 import queue_write_sync, _local_duckdb
        queue_write_sync(_batch_insert, _local_duckdb, batch_data)
        duckdb_ok = True
        inserted_count = len(batch_data)
        write_batch_to_postgres_async("wallet_profiles", pg_rows)
    except Exception as e:
        logger.error(f"DuckDB insert failed: {e}")
    
    # DuckDB only - no MySQL writes
    if duckdb_ok:
        return inserted_count
    return 0


# =============================================================================
# Cleanup Functions
# =============================================================================

def cleanup_old_profiles(hours: int = 24) -> int:
    """
    Delete profile records older than specified hours from DuckDB.
    """
    total_deleted = 0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    # Clean up DuckDB only via write queue
    try:
        # Define helper for delete with return count
        def _delete_old_profiles(conn, cutoff_time):
            result = conn.execute("""
                DELETE FROM wallet_profiles
                WHERE trade_timestamp < ?
                RETURNING id
            """, [cutoff_time]).fetchall()
            return len(result)
        
        from scheduler.master2 import queue_write_sync, _local_duckdb
        deleted = queue_write_sync(_delete_old_profiles, _local_duckdb, cutoff)
        if deleted > 0:
            total_deleted = deleted
            logger.debug(f"Cleaned up {deleted} old profiles from DuckDB")
    except Exception as e:
        logger.error(f"DuckDB cleanup failed: {e}")
    
    return total_deleted


# =============================================================================
# Main Entry Point
# =============================================================================

def process_wallet_profiles() -> int:
    """
    Main entry point for the scheduler.
    Process trades and build wallet profiles for all thresholds.
    
    PERFORMANCE: All reads from DuckDB (local, columnar, fast JOINs).
    
    Returns:
        Total number of profiles inserted across all thresholds
    """
    # Ensure tables exist
    ensure_duckdb_tables()
    
    # Process each threshold
    total_inserted = 0
    for threshold in THRESHOLDS:
        try:
            inserted = build_profiles_for_threshold(threshold)
            if inserted > 0:
                total_inserted += inserted
                logger.debug(f"Threshold {threshold}: inserted {inserted} profiles")
        except Exception as e:
            logger.error(f"Error processing threshold {threshold}: {e}")
    
    if total_inserted > 0:
        logger.info(f"Inserted {total_inserted} profiles across all thresholds")
    
    return total_inserted


def run_continuous(interval_seconds: int = 5):
    """Run profile processing continuously (for testing)."""
    import time
    
    logger.info(f"Starting continuous processing (interval: {interval_seconds}s)")
    logger.info(f"Thresholds: {THRESHOLDS}")
    logger.info(f"Min buys to qualify: {MIN_BUYS}")
    logger.info("Using DuckDB for all reads (FAST)")
    
    try:
        while True:
            processed = process_wallet_profiles()
            if processed > 0:
                logger.info(f"Processed {processed} profiles")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Stopped by user")


# =============================================================================
# Master2.py Local DuckDB Support
# =============================================================================
# These functions allow profile building using master2.py's local in-memory
# DuckDB instance, which is synced from master.py's Data Engine API.
# =============================================================================

def build_profiles_for_local_duckdb(local_conn, lock=None, data_client=None) -> int:
    """
    Build wallet profiles using master2.py's local in-memory DuckDB.
    
    This is the entry point for master2.py scheduler to call.
    Uses the local DuckDB connection directly instead of get_duckdb("central").
    
    IMPORTANT: Also pushes profiles to master.py's Data Engine API so the website
    can access them (website reads from TradingDataEngine, not master2.py's local DB).
    
    STATE PERSISTENCE: Uses PostgreSQL for tracking last_trade_id per threshold.
    This ensures incremental processing survives master2.py restarts.
    
    Args:
        local_conn: DuckDB connection from master2.py (_local_duckdb)
        lock: Optional threading lock for the connection (_local_duckdb_lock)
        data_client: Optional DataClient for pushing profiles to master.py's API
    
    Returns:
        Total number of profiles inserted across all thresholds
    """
    total_inserted = 0
    
    # State tracking - use LOCAL DuckDB (master2 in-memory) for persistence
    def _get_last_id(threshold):
        """Get last processed trade_id from LOCAL DuckDB (master2 in-memory)."""
        try:
            pg_value = _pg_get_last_processed_id(threshold)
            if pg_value is not None:
                return pg_value
            # Use local DuckDB for state tracking (NO PostgreSQL)
            result = local_conn.execute(
                "SELECT last_trade_id FROM wallet_profiles_state WHERE threshold = ?",
                [float(threshold)]
            ).fetchone()
            
            if result:
                last_id = int(result[0])
                logger.debug(f"Retrieved last_trade_id={last_id} for threshold={threshold}")
                return last_id
            else:
                logger.info(f"No state found for threshold={threshold}, starting from 0")
                return 0
        except Exception as e:
            logger.error(f"Failed to get last_trade_id from DuckDB: {e}")
            return 0
    
    def _update_last_id_local(conn, threshold, last_id):
        """Update last processed trade_id in LOCAL DuckDB (master2 in-memory)."""
        try:
            _pg_update_last_processed_id(threshold, last_id)
            # Use local DuckDB for state tracking (NO PostgreSQL)
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            conn.execute("""
                INSERT INTO wallet_profiles_state (threshold, last_trade_id, last_updated)
                VALUES (?, ?, ?)
                ON CONFLICT (threshold) DO UPDATE SET 
                    last_trade_id = EXCLUDED.last_trade_id,
                    last_updated = EXCLUDED.last_updated
            """, [float(threshold), int(last_id), now])
            logger.debug(f"Updated state: threshold={threshold}, last_trade_id={last_id}")
        except Exception as e:
            logger.warning(f"State update failed (non-critical): {e}")
    
    def _get_latest_cycle_end(conn, threshold):
        try:
            result = conn.execute("""
                SELECT MAX(cycle_end_time) as max_end
                FROM cycle_tracker
                WHERE threshold = ? AND cycle_end_time IS NOT NULL
            """, [threshold]).fetchone()
            return result[0] if result and result[0] else None
        except:
            return None
    
    def _insert_profiles(conn, profiles):
        """Insert profiles using PyArrow for 200x faster batch inserts."""
        if not profiles:
            return 0
        try:
            import pyarrow as pa
            import time
            
            start_time = time.time()
            
            # Generate IDs for new profiles
            max_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM wallet_profiles").fetchone()
            next_id = (max_id_result[0] or 0) + 1
            
            # Add IDs to profiles
            for i, profile in enumerate(profiles):
                profile['id'] = next_id + i
            
            # Build columnar data for PyArrow (MUCH faster than row-by-row)
            col_data = {
                'id': [p['id'] for p in profiles],
                'wallet_address': [p['wallet_address'] for p in profiles],
                'threshold': [p['threshold'] for p in profiles],
                'trade_id': [p['trade_id'] for p in profiles],
                'trade_timestamp': [p['trade_timestamp'] for p in profiles],
                'price_cycle': [p['price_cycle'] for p in profiles],
                'price_cycle_start_time': [p['price_cycle_start_time'] for p in profiles],
                'price_cycle_end_time': [p['price_cycle_end_time'] for p in profiles],
                'trade_entry_price_org': [p['trade_entry_price_org'] for p in profiles],
                'stablecoin_amount': [p['stablecoin_amount'] for p in profiles],
                'trade_entry_price': [p['trade_entry_price'] for p in profiles],
                'sequence_start_price': [p['sequence_start_price'] for p in profiles],
                'highest_price_reached': [p['highest_price_reached'] for p in profiles],
                'lowest_price_reached': [p['lowest_price_reached'] for p in profiles],
                'long_short': [p['long_short'] for p in profiles],
                'short': [p['short'] for p in profiles],
            }
            
            # Create PyArrow table with explicit schema
            schema = pa.schema([
                pa.field('id', pa.int64()),
                pa.field('wallet_address', pa.string()),
                pa.field('threshold', pa.float64()),
                pa.field('trade_id', pa.int64()),
                pa.field('trade_timestamp', pa.timestamp('us')),
                pa.field('price_cycle', pa.int64()),
                pa.field('price_cycle_start_time', pa.timestamp('us')),
                pa.field('price_cycle_end_time', pa.timestamp('us')),
                pa.field('trade_entry_price_org', pa.float64()),
                pa.field('stablecoin_amount', pa.float64()),
                pa.field('trade_entry_price', pa.float64()),
                pa.field('sequence_start_price', pa.float64()),
                pa.field('highest_price_reached', pa.float64()),
                pa.field('lowest_price_reached', pa.float64()),
                pa.field('long_short', pa.string()),
                pa.field('short', pa.int32()),
            ])
            
            arrow_table = pa.Table.from_pydict(col_data, schema=schema)
            
            # Register Arrow table with DuckDB (zero-copy) and insert
            conn.register('_temp_profiles', arrow_table)
            conn.execute("""
                INSERT OR IGNORE INTO wallet_profiles 
                (id, wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                 price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                 stablecoin_amount, trade_entry_price, sequence_start_price,
                 highest_price_reached, lowest_price_reached, long_short, short)
                SELECT id, wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                       price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                       stablecoin_amount, trade_entry_price, sequence_start_price,
                       highest_price_reached, lowest_price_reached, long_short, short
                FROM _temp_profiles
            """)
            conn.unregister('_temp_profiles')
            
            elapsed = time.time() - start_time
            logger.info(f"PyArrow insert: {len(profiles)} profiles in {elapsed:.3f}s ({len(profiles)/elapsed:.0f} records/sec)")
            
            return len(profiles)
            
        except ImportError:
            logger.warning("PyArrow not available, falling back to executemany (slow)")
            # Fallback to executemany if PyArrow not available
            max_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM wallet_profiles").fetchone()
            next_id = (max_id_result[0] or 0) + 1
            
            batch_data = []
            for i, profile in enumerate(profiles):
                batch_data.append([
                    next_id + i,
                    profile['wallet_address'],
                    profile['threshold'],
                    profile['trade_id'],
                    profile['trade_timestamp'],
                    profile['price_cycle'],
                    profile['price_cycle_start_time'],
                    profile['price_cycle_end_time'],
                    profile['trade_entry_price_org'],
                    profile['stablecoin_amount'],
                    profile['trade_entry_price'],
                    profile['sequence_start_price'],
                    profile['highest_price_reached'],
                    profile['lowest_price_reached'],
                    profile['long_short'],
                    profile['short'],
                ])
            
            conn.executemany("""
                INSERT OR IGNORE INTO wallet_profiles 
                (id, wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                 price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                 stablecoin_amount, trade_entry_price, sequence_start_price,
                 highest_price_reached, lowest_price_reached, long_short, short)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            return len(batch_data)
            
        except Exception as e:
            logger.error(f"Profile insert failed: {e}")
            return 0
    
    # Ensure DuckDB state table exists (NOT PostgreSQL)
    try:
        local_conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                id INTEGER PRIMARY KEY,
                threshold DOUBLE NOT NULL UNIQUE,
                last_trade_id BIGINT NOT NULL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except Exception as e:
        logger.warning(f"State table creation: {e}")
    
    # Process all thresholds with minimal lock time to avoid blocking other jobs
    for threshold in THRESHOLDS:
        try:
            # Get latest completed cycle end time (brief lock)
            if lock:
                lock.acquire()
            try:
                latest_cycle_end = _get_latest_cycle_end(local_conn, threshold)
            finally:
                if lock:
                    lock.release()
            
            if not latest_cycle_end:
                continue
            
            # Get last processed trade ID (brief lock)
            if lock:
                lock.acquire()
            try:
                last_trade_id = _get_last_id(threshold)
            finally:
                if lock:
                    lock.release()
            
            # Build profiles using local connection (brief lock)
            if lock:
                lock.acquire()
            try:
                build_start = time.time()
                profiles = build_profiles_batch_duckdb(
                    threshold, last_trade_id, latest_cycle_end, 
                    batch_size=BATCH_SIZE, conn=local_conn
                )
            finally:
                if lock:
                    lock.release()
            build_elapsed = time.time() - build_start
            
            if not profiles:
                logger.info(f"Threshold {threshold}: build took {build_elapsed:.3f}s, no new profiles")
                continue
            
            logger.info(
                f"Threshold {threshold}: build took {build_elapsed:.3f}s, "
                f"profiles={len(profiles)}, trade_id range "
                f"{min(p['trade_id'] for p in profiles)}-{max(p['trade_id'] for p in profiles)}"
            )
            
            # Insert profiles into local DuckDB (brief lock; PyArrow is fast)
            if lock:
                lock.acquire()
            try:
                insert_start = time.time()
                inserted = _insert_profiles(local_conn, profiles)
            finally:
                if lock:
                    lock.release()
            insert_elapsed = time.time() - insert_start
            if inserted > 0:
                write_batch_to_postgres_async("wallet_profiles", _prepare_pg_profiles(profiles))
            logger.info(
                f"Threshold {threshold}: insert took {insert_elapsed:.3f}s "
                f"for {inserted} profiles ({(inserted/insert_elapsed) if insert_elapsed else 0:.0f} r/s)"
            )
            
            # Update state (brief lock)
            max_id = max(p['trade_id'] for p in profiles)
            if lock:
                lock.acquire()
            try:
                _update_last_id_local(local_conn, threshold, max_id)
                total_inserted += inserted
            finally:
                if lock:
                    lock.release()
            
            if inserted > 0:
                logger.debug(f"Threshold {threshold}: inserted {inserted} profiles")
                
        except Exception as e:
            logger.error(f"Error processing threshold {threshold}: {e}")
            continue
    
    if total_inserted > 0:
        logger.info(f"Inserted {total_inserted} profiles across all thresholds")
    
    return total_inserted


def cleanup_old_profiles_local(local_conn, hours: int = 24, lock=None) -> int:
    """
    Delete old profile records from master2.py's local DuckDB.
    
    Args:
        local_conn: DuckDB connection from master2.py
        hours: Age threshold in hours (default 24)
        lock: Optional threading lock
    
    Returns:
        Number of records deleted
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    deleted = 0
    
    try:
        if lock:
            lock.acquire()
        
        result = local_conn.execute("""
            DELETE FROM wallet_profiles
            WHERE trade_timestamp < ?
            RETURNING id
        """, [cutoff]).fetchall()
        deleted = len(result)
        
        if deleted > 0:
            logger.debug(f"Cleaned up {deleted} old profiles from local DuckDB")
            
    except Exception as e:
        logger.error(f"Local DuckDB cleanup failed: {e}")
    finally:
        if lock:
            lock.release()
    
    return deleted


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
        processed = process_wallet_profiles()
        print(f"Processed {processed} profiles")

"""
Wallet Profile Builder - DuckDB to DuckDB (Fast Version)
=========================================================
Migrated from: 000old_code/solana_node/analyze/profiles_v2/create_profiles.py

Builds wallet profiles by joining:
- sol_stablecoin_trades (buy trades from wallets) - FROM DUCKDB
- cycle_tracker (completed price cycles) - FROM DUCKDB
- price_points (to get trade entry price) - FROM DUCKDB

Writes results to:
- DuckDB central.duckdb (for fast 24hr hot storage)
- MySQL (also 24hr retention - cleaned up hourly)

PERFORMANCE: All reads from DuckDB (local, columnar, fast JOINs).
             Previous version used MySQL which was 100x slower due to:
             - Network latency to remote MySQL
             - Row-based storage not optimized for analytical JOINs
             - Large IN clauses (5000 wallets) are slow in MySQL

Thresholds: All from cycle_tracker (0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5)

IMPORTANT: Only processes trades within COMPLETED cycles (cycle_end_time IS NOT NULL)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Set
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_mysql, get_duckdb
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
COIN_ID = 5  # SOL
MIN_BUYS = 3  # Minimum buy trades to qualify a wallet
BATCH_SIZE = 1000  # Process trades in batches (increased for DuckDB efficiency)

# All thresholds from cycle_tracker (matching create_price_cycles.py)
THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]


# =============================================================================
# State Management (DuckDB only - no MySQL)
# =============================================================================

def get_last_processed_id(threshold: float) -> int:
    """Get the last processed trade ID for a threshold from DuckDB."""
    try:
        with get_duckdb("central") as conn:
            # Create state table if needed
            conn.execute("""
                CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                    id INTEGER PRIMARY KEY,
                    threshold DOUBLE NOT NULL UNIQUE,
                    last_trade_id BIGINT NOT NULL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            result = conn.execute(
                "SELECT last_trade_id FROM wallet_profiles_state WHERE threshold = ?",
                [threshold]
            ).fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.debug(f"No state found for threshold {threshold}: {e}")
        return 0


def update_last_processed_id(threshold: float, last_trade_id: int):
    """Update the last processed trade ID for a threshold in DuckDB."""
    try:
        with get_duckdb("central") as conn:
            # Create state table if needed
            conn.execute("""
                CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                    id INTEGER PRIMARY KEY,
                    threshold DOUBLE NOT NULL UNIQUE,
                    last_trade_id BIGINT NOT NULL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Upsert using DuckDB syntax
            conn.execute("""
                INSERT INTO wallet_profiles_state (threshold, last_trade_id, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (threshold) DO UPDATE SET 
                    last_trade_id = excluded.last_trade_id,
                    last_updated = CURRENT_TIMESTAMP
            """, [threshold, last_trade_id])
    except Exception as e:
        logger.error(f"Failed to update state for threshold {threshold}: {e}")


# =============================================================================
# DuckDB Data Access (FAST - all reads from local DuckDB)
# =============================================================================

def ensure_duckdb_tables():
    """Ensure DuckDB tables exist (idempotent)."""
    try:
        with get_duckdb("central") as conn:
            conn.execute(SCHEMA_SOL_STABLECOIN_TRADES)
            conn.execute(SCHEMA_CYCLE_TRACKER)
            conn.execute(SCHEMA_PRICE_POINTS)
            conn.execute(SCHEMA_WALLET_PROFILES)
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
    batch_size: int = BATCH_SIZE
) -> List[Dict]:
    """
    Build wallet profiles by joining trades with cycles entirely in DuckDB.
    
    This is the FAST path - single query that does:
    1. Filters eligible wallets (MIN_BUYS requirement)
    2. Joins trades with completed cycles
    3. Gets entry price from price_points
    
    Returns list of profile dicts ready for insert.
    """
    try:
        with get_duckdb("central", read_only=True) as conn:
            # Single efficient query that does all the work in DuckDB
            # Uses a CTE for eligible wallets to avoid large IN clause
            result = conn.execute("""
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
                trades_with_prices AS (
                    SELECT 
                        twc.*,
                        (
                            SELECT pp.value 
                            FROM price_points pp 
                            WHERE pp.created_at > twc.trade_timestamp 
                            AND pp.coin_id = ?
                            ORDER BY pp.created_at ASC 
                            LIMIT 1
                        ) as entry_price
                    FROM trades_with_cycles twc
                )
                SELECT *
                FROM trades_with_prices
                WHERE entry_price IS NOT NULL
            """, [MIN_BUYS, threshold, latest_cycle_end, last_trade_id, batch_size, COIN_ID]).fetchall()
            
            # Get column names
            columns = [desc[0] for desc in conn.description]
            
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


def insert_profiles_batch(profiles: List[Dict]) -> int:
    """
    Insert profiles with dual-write to DuckDB and MySQL.
    Returns number of records inserted.
    """
    if not profiles:
        return 0
    
    duckdb_ok = False
    mysql_ok = False
    inserted_count = 0
    
    # Write to DuckDB (central.duckdb)
    try:
        with get_duckdb("central") as conn:
            # Generate IDs for new profiles
            max_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM wallet_profiles").fetchone()
            next_id = (max_id_result[0] or 0) + 1
            
            # Prepare batch insert data
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
            
            # Batch insert with conflict handling
            conn.executemany("""
                INSERT OR IGNORE INTO wallet_profiles 
                (id, wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                 price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                 stablecoin_amount, trade_entry_price, sequence_start_price,
                 highest_price_reached, lowest_price_reached, long_short, short)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            duckdb_ok = True
            inserted_count = len(batch_data)
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
    cutoff = datetime.now() - timedelta(hours=hours)
    
    # Clean up DuckDB only
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                DELETE FROM wallet_profiles
                WHERE trade_timestamp < ?
                RETURNING id
            """, [cutoff]).fetchall()
            deleted = len(result)
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

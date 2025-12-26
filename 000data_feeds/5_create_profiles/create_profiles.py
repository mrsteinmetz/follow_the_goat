"""
Wallet Profile Builder - Using TradingDataEngine (In-Memory DuckDB)
====================================================================
Migrated from: 000old_code/solana_node/analyze/profiles_v2/create_profiles.py

Builds wallet profiles by joining:
- sol_stablecoin_trades (buy trades from wallets)
- cycle_tracker (completed price cycles)
- price_points (to get trade entry price)

Writes results to:
- TradingDataEngine (in-memory DuckDB for fast 24hr hot storage)
- MySQL (also 24hr retention - cleaned up hourly)

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

from core.database import get_mysql, get_trading_engine
from core.config import settings

# Configure logging
logger = logging.getLogger("wallet_profiles")

# --- Configuration ---
COIN_ID = 5  # SOL
MIN_BUYS = 10  # Minimum buy trades to qualify a wallet
BATCH_SIZE = 500  # Process trades in batches

# All thresholds from cycle_tracker (matching create_price_cycles.py)
THRESHOLDS = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]


# =============================================================================
# MySQL Table Initialization
# =============================================================================

def ensure_mysql_tables():
    """Ensure MySQL tables exist for wallet_profiles and state tracking."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Create wallet_profiles table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_profiles (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        wallet_address VARCHAR(255) NOT NULL,
                        threshold DECIMAL(5, 2) NOT NULL,
                        trade_id BIGINT NOT NULL,
                        trade_timestamp DATETIME NOT NULL,
                        price_cycle BIGINT NOT NULL,
                        price_cycle_start_time DATETIME NULL,
                        price_cycle_end_time DATETIME NULL,
                        trade_entry_price_org DECIMAL(20, 8) NOT NULL,
                        stablecoin_amount DOUBLE NULL,
                        trade_entry_price DECIMAL(20, 8) NOT NULL,
                        sequence_start_price DECIMAL(20, 8) NOT NULL,
                        highest_price_reached DECIMAL(20, 8) NOT NULL,
                        lowest_price_reached DECIMAL(20, 8) NOT NULL,
                        long_short VARCHAR(10) NULL,
                        short TINYINT NOT NULL DEFAULT 2,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_wallet_address (wallet_address),
                        INDEX idx_threshold (threshold),
                        INDEX idx_trade_timestamp (trade_timestamp),
                        INDEX idx_price_cycle (price_cycle),
                        INDEX idx_wallet_threshold (wallet_address, threshold),
                        INDEX idx_short (short),
                        INDEX idx_created_at (created_at),
                        UNIQUE KEY idx_unique_trade (trade_id, threshold)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                # Create state tracking table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_profiles_state (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        threshold DECIMAL(5, 2) NOT NULL,
                        last_trade_id BIGINT NOT NULL DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY idx_threshold (threshold)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                logger.debug("MySQL tables verified/created")
                return True
    except Exception as e:
        logger.error(f"Failed to ensure MySQL tables: {e}")
        return False


# =============================================================================
# State Management
# =============================================================================

def get_last_processed_id(threshold: float) -> int:
    """Get the last processed trade ID for a threshold."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT last_trade_id FROM wallet_profiles_state WHERE threshold = %s",
                    [threshold]
                )
                result = cursor.fetchone()
                return result['last_trade_id'] if result else 0
    except Exception as e:
        logger.debug(f"No state found for threshold {threshold}: {e}")
        return 0


def update_last_processed_id(threshold: float, last_trade_id: int):
    """Update the last processed trade ID for a threshold."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO wallet_profiles_state (threshold, last_trade_id)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE last_trade_id = VALUES(last_trade_id)
                """, [threshold, last_trade_id])
    except Exception as e:
        logger.error(f"Failed to update state for threshold {threshold}: {e}")


# =============================================================================
# Data Access Functions
# =============================================================================

def get_eligible_wallets() -> Set[str]:
    """Get wallets that have at least MIN_BUYS buy trades."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT wallet_address
                    FROM sol_stablecoin_trades
                    WHERE direction = 'buy'
                    GROUP BY wallet_address
                    HAVING COUNT(id) >= %s
                """, [MIN_BUYS])
                return set(row['wallet_address'] for row in cursor.fetchall())
    except Exception as e:
        logger.error(f"Failed to get eligible wallets: {e}")
        return set()


def get_latest_completed_cycle_end(threshold: float) -> Optional[datetime]:
    """Get the end time of the most recent completed cycle for a threshold."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT MAX(cycle_end_time) as max_end
                    FROM cycle_tracker
                    WHERE threshold = %s AND cycle_end_time IS NOT NULL
                """, [threshold])
                result = cursor.fetchone()
                return result['max_end'] if result and result['max_end'] else None
    except Exception as e:
        logger.error(f"Failed to get latest cycle end: {e}")
        return None


def get_trades_with_cycles(
    eligible_wallets: Set[str],
    last_trade_id: int,
    latest_cycle_end: datetime,
    threshold: float,
    batch_size: int = BATCH_SIZE
) -> List[Dict]:
    """
    Get buy trades joined with completed cycle data for a specific threshold.
    Only returns trades that fall within completed cycles.
    """
    if not eligible_wallets:
        return []
    
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Limit wallet list to avoid huge IN clause
                wallet_list = list(eligible_wallets)[:5000]
                placeholders = ','.join(['%s'] * len(wallet_list))
                
                query = f"""
                    SELECT 
                        t.id,
                        t.wallet_address,
                        t.trade_timestamp,
                        t.price,
                        t.stablecoin_amount,
                        t.perp_direction,
                        c.id as cycle_id,
                        c.cycle_start_time,
                        c.cycle_end_time,
                        c.sequence_start_price,
                        c.highest_price_reached,
                        c.lowest_price_reached
                    FROM sol_stablecoin_trades t
                    INNER JOIN cycle_tracker c ON (
                        c.threshold = %s
                        AND c.cycle_start_time <= t.trade_timestamp
                        AND c.cycle_end_time >= t.trade_timestamp
                        AND c.cycle_end_time IS NOT NULL
                    )
                    WHERE t.direction = 'buy'
                    AND t.wallet_address IN ({placeholders})
                    AND t.trade_timestamp <= %s
                    AND t.id > %s
                    ORDER BY t.id ASC
                    LIMIT %s
                """
                
                params = [threshold] + wallet_list + [latest_cycle_end, last_trade_id, batch_size]
                cursor.execute(query, params)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to get trades with cycles: {e}")
        return []


def get_price_points_bulk(trade_timestamps: List[datetime]) -> Dict[datetime, float]:
    """
    Get the first price_point value after each trade timestamp.
    Returns a dict mapping trade_timestamp -> price value.
    """
    if not trade_timestamps:
        return {}
    
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                min_ts = min(trade_timestamps)
                
                # Get all price points after the minimum timestamp for SOL (coin_id = 5)
                cursor.execute("""
                    SELECT created_at, value
                    FROM price_points
                    WHERE created_at >= %s AND coin_id = %s
                    ORDER BY created_at ASC
                """, [min_ts, COIN_ID])
                price_points = cursor.fetchall()
                
                if not price_points:
                    return {}
                
                # For each trade timestamp, find the first price_point after it
                result = {}
                for ts in trade_timestamps:
                    for pp in price_points:
                        if pp['created_at'] > ts:
                            result[ts] = float(pp['value'])
                            break
                
                return result
    except Exception as e:
        logger.error(f"Failed to get price points: {e}")
        return {}


# =============================================================================
# Profile Building
# =============================================================================

def build_profiles_for_threshold(threshold: float, eligible_wallets: Set[str]) -> int:
    """
    Build wallet profiles for a specific threshold.
    Returns number of profiles inserted.
    """
    # Get latest completed cycle end time for this threshold
    latest_cycle_end = get_latest_completed_cycle_end(threshold)
    if not latest_cycle_end:
        logger.debug(f"No completed cycles for threshold {threshold}")
        return 0
    
    # Get last processed trade ID
    last_trade_id = get_last_processed_id(threshold)
    
    # Get trades with cycle data
    trades = get_trades_with_cycles(
        eligible_wallets, last_trade_id, latest_cycle_end, threshold
    )
    
    if not trades:
        return 0
    
    # Get price points for all trades in bulk
    trade_timestamps = [t['trade_timestamp'] for t in trades]
    price_points_map = get_price_points_bulk(trade_timestamps)
    
    # Build profile records
    profiles = []
    for trade in trades:
        # Skip if no price point found
        trade_entry_price = price_points_map.get(trade['trade_timestamp'])
        if trade_entry_price is None:
            continue
        
        # Calculate short value based on perp_direction
        perp_direction = trade['perp_direction']
        if perp_direction == 'long':
            short_value = 0
        elif perp_direction == 'short':
            short_value = 1
        else:
            short_value = 2  # null or empty
        
        profiles.append({
            'wallet_address': trade['wallet_address'],
            'threshold': threshold,
            'trade_id': trade['id'],
            'trade_timestamp': trade['trade_timestamp'],
            'price_cycle': trade['cycle_id'],
            'price_cycle_start_time': trade['cycle_start_time'],
            'price_cycle_end_time': trade['cycle_end_time'],
            'trade_entry_price_org': float(trade['price']),
            'stablecoin_amount': trade['stablecoin_amount'],
            'trade_entry_price': trade_entry_price,
            'sequence_start_price': float(trade['sequence_start_price']),
            'highest_price_reached': float(trade['highest_price_reached']),
            'lowest_price_reached': float(trade['lowest_price_reached']),
            'long_short': perp_direction,
            'short': short_value
        })
    
    if not profiles:
        # Update state even if no profiles matched (to advance past these trades)
        if trades:
            update_last_processed_id(threshold, trades[-1]['id'])
        return 0
    
    # Insert profiles with dual-write
    inserted = insert_profiles_batch(profiles)
    
    # Update state with last processed trade ID
    if trades:
        update_last_processed_id(threshold, trades[-1]['id'])
    
    return inserted


def insert_profiles_batch(profiles: List[Dict]) -> int:
    """
    Insert profiles with dual-write to TradingDataEngine and MySQL.
    Returns number of records inserted.
    """
    if not profiles:
        return 0
    
    engine_ok = False
    mysql_ok = False
    inserted_count = 0
    
    # Write to TradingDataEngine (in-memory DuckDB)
    try:
        engine = get_trading_engine()
        if engine._running:
            for profile in profiles:
                engine.write('wallet_profiles', profile)
            engine_ok = True
    except Exception as e:
        logger.error(f"TradingEngine insert failed: {e}")
    
    # Write to MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT IGNORE INTO wallet_profiles 
                    (wallet_address, threshold, trade_id, trade_timestamp, price_cycle,
                     price_cycle_start_time, price_cycle_end_time, trade_entry_price_org,
                     stablecoin_amount, trade_entry_price, sequence_start_price,
                     highest_price_reached, lowest_price_reached, long_short, short)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                data = [
                    (
                        p['wallet_address'],
                        p['threshold'],
                        p['trade_id'],
                        p['trade_timestamp'],
                        p['price_cycle'],
                        p['price_cycle_start_time'],
                        p['price_cycle_end_time'],
                        p['trade_entry_price_org'],
                        p['stablecoin_amount'],
                        p['trade_entry_price'],
                        p['sequence_start_price'],
                        p['highest_price_reached'],
                        p['lowest_price_reached'],
                        p['long_short'],
                        p['short']
                    )
                    for p in profiles
                ]
                
                cursor.executemany(insert_query, data)
                inserted_count = cursor.rowcount
                mysql_ok = True
    except Exception as e:
        logger.error(f"MySQL insert failed: {e}")
    
    if engine_ok or mysql_ok:
        return inserted_count
    return 0


# =============================================================================
# Cleanup Functions
# =============================================================================

def cleanup_old_profiles(hours: int = 24) -> int:
    """
    Delete profile records older than specified hours from BOTH DuckDB and MySQL.
    This is different from standard architecture - both databases only keep 24 hours.
    """
    total_deleted = 0
    
    # Clean up TradingDataEngine (in-memory DuckDB)
    try:
        engine = get_trading_engine()
        if engine._running:
            cutoff = datetime.now() - timedelta(hours=hours)
            engine.execute("""
                DELETE FROM wallet_profiles
                WHERE trade_timestamp < ?
            """, [cutoff])
            logger.debug("Cleaned up old profiles from TradingEngine")
    except Exception as e:
        logger.error(f"TradingEngine cleanup failed: {e}")
    
    # Clean up MySQL (non-standard: also 24hr retention)
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM wallet_profiles
                    WHERE trade_timestamp < NOW() - INTERVAL %s HOUR
                """, [hours])
                deleted = cursor.rowcount
                total_deleted += deleted
                logger.info(f"Deleted {deleted} old profiles from MySQL")
    except Exception as e:
        logger.error(f"MySQL cleanup failed: {e}")
    
    return total_deleted


# =============================================================================
# Main Entry Point
# =============================================================================

def process_wallet_profiles() -> int:
    """
    Main entry point for the scheduler.
    Process trades and build wallet profiles for all thresholds.
    
    Returns:
        Total number of profiles inserted across all thresholds
    """
    # Ensure MySQL tables exist
    ensure_mysql_tables()
    
    # Get eligible wallets (cached for all thresholds)
    eligible_wallets = get_eligible_wallets()
    if not eligible_wallets:
        logger.debug("No eligible wallets found")
        return 0
    
    logger.debug(f"Found {len(eligible_wallets)} eligible wallets")
    
    # Process each threshold
    total_inserted = 0
    for threshold in THRESHOLDS:
        try:
            inserted = build_profiles_for_threshold(threshold, eligible_wallets)
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


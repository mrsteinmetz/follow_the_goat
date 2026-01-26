#!/usr/bin/env python3
"""
DuckDB Filter Cache Manager
============================
Performance optimization for filter analysis by caching trade data in DuckDB.

IMPORTANT: This is the ONLY module allowed to use DuckDB in this project.
PostgreSQL remains the source of truth. DuckDB is used purely for read-only
analysis caching to speed up filter pattern generation.

Cache Strategy:
- Rolling 7-day window
- Incremental updates (only new trades)
- Automatic cleanup of old data
- Fast columnar queries for analysis

Usage:
    from core.filter_cache import sync_cache_incremental, get_cached_trades
    
    # Sync cache (fast if recent, loads only new data)
    cache_age = sync_cache_incremental()
    
    # Query cached data
    df = get_cached_trades(hours=24)
"""

import logging
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from core.database import get_postgres

logger = logging.getLogger(__name__)

# Cache file location
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"
CACHE_FILE = CACHE_DIR / "filter_analysis.duckdb"

# Ensure cache directory exists
CACHE_DIR.mkdir(exist_ok=True)


def get_duckdb_connection():
    """Get a DuckDB connection to the cache file."""
    return duckdb.connect(str(CACHE_FILE))


def init_cache():
    """Initialize DuckDB cache tables and indexes."""
    logger.info("Initializing DuckDB cache tables...")
    
    conn = get_duckdb_connection()
    
    try:
        # Metadata table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR,
                updated_at TIMESTAMP
            )
        """)
        
        # Cached buyins (trades)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cached_buyins (
                id BIGINT PRIMARY KEY,
                play_id INTEGER,
                wallet_address VARCHAR,
                followed_at TIMESTAMP,
                potential_gains DOUBLE,
                our_status VARCHAR
            )
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cached_buyins_time 
            ON cached_buyins(followed_at)
        """)
        
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cached_buyins_gains 
            ON cached_buyins(potential_gains)
        """)
        
        logger.info("Cache tables initialized successfully")
        
    finally:
        conn.close()


def get_cache_metadata(key: str) -> Optional[str]:
    """Get a metadata value from cache."""
    conn = get_duckdb_connection()
    
    try:
        result = conn.execute(
            "SELECT value FROM cache_metadata WHERE key = ?",
            [key]
        ).fetchone()
        
        return result[0] if result else None
        
    finally:
        conn.close()


def set_cache_metadata(key: str, value: str):
    """Set a metadata value in cache."""
    conn = get_duckdb_connection()
    
    try:
        conn.execute("""
            INSERT OR REPLACE INTO cache_metadata (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, [key, value])
        
    finally:
        conn.close()


def get_cache_age() -> float:
    """
    Get age of cache in seconds.
    
    Returns:
        Seconds since last sync, or infinity if never synced
    """
    last_sync = get_cache_metadata('last_sync_timestamp')
    
    if not last_sync:
        return float('inf')
    
    last_sync_dt = datetime.fromisoformat(last_sync)
    age_seconds = (datetime.now() - last_sync_dt).total_seconds()
    
    return age_seconds


def cleanup_old_data():
    """Remove trades older than 7 days from cache."""
    conn = get_duckdb_connection()
    
    try:
        # Calculate cutoff (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)
        
        # Count trades to be deleted
        count_result = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM cached_buyins 
            WHERE followed_at < ?
        """, [cutoff_date]).fetchone()
        
        to_delete = count_result[0] if count_result else 0
        
        if to_delete > 0:
            # Delete old trades
            conn.execute("""
                DELETE FROM cached_buyins 
                WHERE followed_at < ?
            """, [cutoff_date])
            
            logger.info(f"Cleaned up {to_delete} trades older than 7 days")
        
    finally:
        conn.close()


def get_max_timestamp_in_cache() -> Optional[datetime]:
    """Get the latest followed_at timestamp in cache."""
    conn = get_duckdb_connection()
    
    try:
        result = conn.execute("""
            SELECT MAX(followed_at) as max_ts
            FROM cached_buyins
        """).fetchone()
        
        if result and result[0]:
            return result[0]
        
        return None
        
    finally:
        conn.close()


def sync_buyins_incremental():
    """
    Sync buyins table incrementally from PostgreSQL to DuckDB.
    
    Strategy:
    - First run: Load last 7 days
    - Subsequent runs: Load only new trades since max timestamp in cache
    """
    conn = get_duckdb_connection()
    
    try:
        # Get max timestamp currently in cache
        max_ts = get_max_timestamp_in_cache()
        
        if max_ts is None:
            # First sync - load last 7 days
            logger.info("First sync - loading last 7 days of trades...")
            cutoff = datetime.now() - timedelta(days=7)
            
            query = """
                SELECT 
                    id,
                    play_id,
                    wallet_address,
                    followed_at,
                    potential_gains,
                    our_status
                FROM follow_the_goat_buyins
                WHERE followed_at >= %s
                  AND potential_gains IS NOT NULL
                ORDER BY followed_at
            """
            params = [cutoff]
        else:
            # Incremental sync - load only new trades
            logger.info(f"Incremental sync from {max_ts}...")
            
            query = """
                SELECT 
                    id,
                    play_id,
                    wallet_address,
                    followed_at,
                    potential_gains,
                    our_status
                FROM follow_the_goat_buyins
                WHERE followed_at > %s
                  AND potential_gains IS NOT NULL
                ORDER BY followed_at
            """
            params = [max_ts]
        
        # Load from PostgreSQL
        with get_postgres() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
        
        if not results:
            logger.info("No new trades to sync")
            return 0
        
        # Convert to DataFrame for easy insertion
        df = pd.DataFrame(results)
        
        # Insert into DuckDB (ON CONFLICT REPLACE for any duplicates)
        conn.execute("""
            INSERT OR REPLACE INTO cached_buyins 
            SELECT * FROM df
        """)
        
        logger.info(f"Synced {len(df)} trades to cache")
        return len(df)
        
    finally:
        conn.close()


def sync_filter_values_incremental():
    """
    Sync filter values incrementally from PostgreSQL to DuckDB.
    
    This creates a wide table with one column per filter field.
    Uses PostgreSQL's FILTER clause for efficient pivoting.
    
    Note: Only syncs filters where is_ratio=1 to avoid duplicates and follow ratio_only settings.
    """
    conn = get_duckdb_connection()
    
    try:
        # Get list of trade IDs currently in cache
        cached_trade_ids = conn.execute("""
            SELECT id FROM cached_buyins ORDER BY id
        """).fetchall()
        
        if not cached_trade_ids:
            logger.info("No trades in cache, skipping filter sync")
            return 0
        
        trade_ids = [row[0] for row in cached_trade_ids]
        
        # Check which trades already have filter values in cache
        # First, check if table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'cached_filter_values'
        """).fetchone()[0] > 0
        
        if table_exists:
            # Get trades that already have filters
            existing_trades = conn.execute("""
                SELECT DISTINCT buyin_id FROM cached_filter_values
            """).fetchall()
            existing_ids = set([row[0] for row in existing_trades])
            
            # Only sync new trades
            new_trade_ids = [tid for tid in trade_ids if tid not in existing_ids]
            
            if not new_trade_ids:
                logger.info("All cached trades already have filter values")
                return 0
            
            logger.info(f"Loading filter values for {len(new_trade_ids)} new trades...")
            trades_to_sync = new_trade_ids
        else:
            logger.info(f"Loading filter values for all {len(trade_ids)} cached trades...")
            trades_to_sync = trade_ids
        
        # Get distinct filter names (ONLY ratio filters to avoid duplicates)
        with get_postgres() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT filter_name
                    FROM trade_filter_values
                    WHERE buyin_id = ANY(%s)
                      AND is_ratio = 1
                    ORDER BY filter_name
                """, [trades_to_sync])
                
                filter_columns = [row['filter_name'] for row in cursor.fetchall()]
        
        if not filter_columns:
            logger.warning("No ratio filter columns found")
            return 0
        
        logger.info(f"Found {len(filter_columns)} ratio-based filter columns")
        
        # Build pivoted query using PostgreSQL's FILTER clause (only is_ratio=1)
        pivot_cols = []
        for col in filter_columns:
            safe_col = col.replace("'", "''")
            pivot_cols.append(
                f"MAX(filter_value) FILTER (WHERE filter_name = '{safe_col}' AND is_ratio = 1) AS \"{col}\""
            )
        
        pivot_sql = ",\n            ".join(pivot_cols)
        
        query = f"""
            SELECT 
                buyin_id,
                minute,
                {pivot_sql}
            FROM trade_filter_values
            WHERE buyin_id = ANY(%s)
              AND is_ratio = 1
            GROUP BY buyin_id, minute
            ORDER BY buyin_id, minute
        """
        
        # Load from PostgreSQL
        with get_postgres() as pg_conn:
            with pg_conn.cursor() as cursor:
                cursor.execute(query, [trades_to_sync])
                results = cursor.fetchall()
        
        if not results:
            logger.warning("No filter values found for trades")
            return 0
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Create table if it doesn't exist (dynamic schema based on filter columns)
        if not table_exists:
            # Build CREATE TABLE statement
            col_defs = ["buyin_id BIGINT", "minute INTEGER"]
            for col in filter_columns:
                col_defs.append(f'"{col}" DOUBLE')
            
            create_sql = f"""
                CREATE TABLE cached_filter_values (
                    {', '.join(col_defs)},
                    PRIMARY KEY (buyin_id, minute)
                )
            """
            conn.execute(create_sql)
            logger.info("Created cached_filter_values table (ratio filters only)")
        else:
            # Check for schema mismatch (new columns added)
            existing_cols = conn.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'cached_filter_values'
            """).fetchall()
            existing_col_names = set(row[0] for row in existing_cols)
            new_col_names = set(['buyin_id', 'minute'] + filter_columns)
            
            if new_col_names != existing_col_names:
                # Schema changed - drop and recreate table
                missing = new_col_names - existing_col_names
                extra = existing_col_names - new_col_names
                logger.warning(f"Schema mismatch detected! Missing: {len(missing)}, Extra: {len(extra)}")
                logger.info("Recreating cached_filter_values table with new schema...")
                
                conn.execute("DROP TABLE cached_filter_values")
                
                col_defs = ["buyin_id BIGINT", "minute INTEGER"]
                for col in filter_columns:
                    col_defs.append(f'"{col}" DOUBLE')
                
                create_sql = f"""
                    CREATE TABLE cached_filter_values (
                        {', '.join(col_defs)},
                        PRIMARY KEY (buyin_id, minute)
                    )
                """
                conn.execute(create_sql)
                logger.info("Recreated cached_filter_values table")
                
                # Reset trades_to_sync to sync ALL trades since table is empty
                trades_to_sync = trade_ids
                
                # Re-fetch data for ALL trades
                with get_postgres() as pg_conn:
                    with pg_conn.cursor() as cursor:
                        cursor.execute(query, [trades_to_sync])
                        results = cursor.fetchall()
                
                if not results:
                    logger.warning("No filter values found after schema rebuild")
                    return 0
                
                df = pd.DataFrame(results)
        
        # Insert data
        conn.execute("""
            INSERT OR REPLACE INTO cached_filter_values 
            SELECT * FROM df
        """)
        
        logger.info(f"Synced filter values for {len(df)} buyin-minute combinations (ratio filters only)")
        return len(df)
        
    finally:
        conn.close()


def sync_cache_incremental() -> float:
    """
    Perform incremental cache sync from PostgreSQL to DuckDB.
    
    Returns:
        Cache age in seconds after sync
    """
    import time
    start_time = time.time()
    
    # Initialize tables if needed
    init_cache()
    
    # Sync buyins
    buyins_synced = sync_buyins_incremental()
    
    # Sync filter values (only for new trades)
    if buyins_synced > 0:
        sync_filter_values_incremental()
    
    # Cleanup old data
    cleanup_old_data()
    
    # Update metadata
    set_cache_metadata('last_sync_timestamp', datetime.now().isoformat())
    
    sync_time = time.time() - start_time
    logger.info(f"Cache sync completed in {sync_time:.2f}s")
    
    return get_cache_age()


def get_cached_trades(hours: int = 24, ratio_only: bool = False) -> pd.DataFrame:
    """
    Load trade data from DuckDB cache for analysis.
    
    Args:
        hours: Number of hours to look back
        ratio_only: If True, only include ratio filters (is_ratio=1)
        
    Returns:
        DataFrame with trades and pivoted filter values
    """
    conn = get_duckdb_connection()
    
    try:
        # Calculate cutoff time
        cutoff = datetime.now() - timedelta(hours=hours)
        
        # Check if cached_filter_values table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'cached_filter_values'
        """).fetchone()[0] > 0
        
        if not table_exists:
            logger.warning("cached_filter_values table doesn't exist yet")
            return pd.DataFrame()
        
        # Query to join buyins with filter values
        query = """
            SELECT 
                b.id as trade_id,
                b.play_id,
                b.followed_at,
                b.potential_gains,
                b.our_status,
                f.minute,
                f.* EXCLUDE (buyin_id, minute)
            FROM cached_buyins b
            INNER JOIN cached_filter_values f ON f.buyin_id = b.id
            WHERE b.followed_at >= ?
            ORDER BY b.id, f.minute
        """
        
        df = conn.execute(query, [cutoff]).df()
        
        if len(df) == 0:
            logger.warning(f"No cached data found for last {hours} hours")
            return pd.DataFrame()
        
        # Filter columns if ratio_only is enabled
        if ratio_only:
            # Get list of ratio-only columns from PostgreSQL
            from core.database import get_postgres
            
            with get_postgres() as pg_conn:
                with pg_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT DISTINCT filter_name
                        FROM trade_filter_values
                        WHERE is_ratio = 1
                    """)
                    ratio_columns = [row['filter_name'] for row in cursor.fetchall()]
            
            # Keep only metadata columns + ratio columns
            metadata_cols = ['trade_id', 'play_id', 'followed_at', 'potential_gains', 'our_status', 'minute']
            columns_to_keep = metadata_cols + [col for col in df.columns if col in ratio_columns]
            
            # Filter dataframe
            df = df[columns_to_keep]
            
            logger.info(f"Ratio-only mode: filtered to {len(columns_to_keep) - len(metadata_cols)} ratio columns")
        
        logger.info(f"Loaded {len(df)} rows from cache ({df['trade_id'].nunique()} unique trades)")
        
        return df
        
    finally:
        conn.close()


def get_cache_stats() -> Dict[str, Any]:
    """Get statistics about the cache."""
    conn = get_duckdb_connection()
    
    try:
        # Check if tables exist
        buyins_exists = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'cached_buyins'
        """).fetchone()[0] > 0
        
        filters_exists = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables 
            WHERE table_name = 'cached_filter_values'
        """).fetchone()[0] > 0
        
        stats = {
            'cache_file': str(CACHE_FILE),
            'cache_exists': CACHE_FILE.exists(),
            'tables_initialized': buyins_exists and filters_exists,
        }
        
        if buyins_exists:
            # Count trades
            count = conn.execute("SELECT COUNT(*) FROM cached_buyins").fetchone()[0]
            stats['trades_cached'] = count
            
            # Date range
            date_range = conn.execute("""
                SELECT 
                    MIN(followed_at) as min_date,
                    MAX(followed_at) as max_date
                FROM cached_buyins
            """).fetchone()
            
            if date_range[0]:
                stats['date_range'] = {
                    'min': date_range[0].isoformat(),
                    'max': date_range[1].isoformat()
                }
        
        if filters_exists:
            # Count filter rows
            count = conn.execute("SELECT COUNT(*) FROM cached_filter_values").fetchone()[0]
            stats['filter_rows'] = count
            
            # Count columns (filter fields)
            cols = conn.execute("""
                SELECT COUNT(*) as cnt
                FROM information_schema.columns
                WHERE table_name = 'cached_filter_values'
                  AND column_name NOT IN ('buyin_id', 'minute')
            """).fetchone()[0]
            stats['filter_columns'] = cols
        
        # Cache age
        stats['cache_age_seconds'] = get_cache_age()
        
        # Last sync time
        last_sync = get_cache_metadata('last_sync_timestamp')
        if last_sync:
            stats['last_sync'] = last_sync
        
        return stats
        
    finally:
        conn.close()


def clear_cache():
    """Clear all cache data (for testing/maintenance)."""
    logger.warning("Clearing entire cache...")
    
    conn = get_duckdb_connection()
    
    try:
        # Drop tables
        conn.execute("DROP TABLE IF EXISTS cached_filter_values")
        conn.execute("DROP TABLE IF EXISTS cached_buyins")
        conn.execute("DROP TABLE IF EXISTS cache_metadata")
        
        logger.info("Cache cleared successfully")
        
    finally:
        conn.close()


if __name__ == "__main__":
    # Test the cache
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    logger.info("Testing DuckDB filter cache...")
    
    # Show current stats
    stats = get_cache_stats()
    logger.info(f"Current cache stats: {stats}")
    
    # Sync cache
    cache_age = sync_cache_incremental()
    logger.info(f"Cache synced, age: {cache_age:.1f}s")
    
    # Show updated stats
    stats = get_cache_stats()
    logger.info(f"Updated cache stats: {stats}")
    
    # Test query
    df = get_cached_trades(hours=24)
    logger.info(f"Query test: loaded {len(df)} rows")

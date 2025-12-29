"""
MySQL to DuckDB Initial Sync Script
===================================
Syncs data from MySQL to DuckDB (in-memory TradingDataEngine when available).

When TradingDataEngine is running (scheduler mode), syncs to in-memory.
When standalone, syncs to file-based DuckDB.

Usage:
    python sync_from_mysql.py           # Sync last 24 hours
    python sync_from_mysql.py --hours 48  # Sync last 48 hours
    python sync_from_mysql.py --full    # Sync all data for plays table
"""

import sys
from pathlib import Path
import json
from datetime import datetime
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, get_mysql, init_duckdb_tables
from core.config import settings
from features.price_api.schema import HOT_TABLES, TIMESTAMP_COLUMNS, ALL_SCHEMAS

# Configure logger for this module
logger = logging.getLogger("sync_from_mysql")


def _get_engine_if_running():
    """Get TradingDataEngine if it's running, otherwise return None."""
    try:
        from core.trading_engine import _engine_instance
        if _engine_instance is not None and _engine_instance._running:
            return _engine_instance
    except Exception:
        pass
    return None


def sync_table(table_name: str, hours: int = 24, full_sync: bool = False):
    """
    Sync a single table from MySQL to DuckDB (in-memory TradingDataEngine if running).
    
    Args:
        table_name: Name of the table to sync
        hours: Number of hours of data to sync (for time-based tables)
        full_sync: If True, sync all data regardless of time
    """
    engine = _get_engine_if_running()
    target = "TradingDataEngine (in-memory)" if engine else "file-based DuckDB"
    logger.info(f"Syncing table: {table_name} -> {target}")
    
    # Get schema SQL for this table (only needed for file-based DuckDB)
    schema_sql = None
    for name, sql in ALL_SCHEMAS:
        if name == table_name:
            schema_sql = sql
            break
    
    ts_col = TIMESTAMP_COLUMNS.get(table_name, 'created_at')
    
    # Build query for MySQL
    if table_name == 'follow_the_goat_plays' or full_sync:
        query = f"SELECT * FROM {table_name}"
        logger.debug(f"  Mode: Full sync (all data)")
    else:
        query = f"SELECT * FROM {table_name} WHERE {ts_col} >= NOW() - INTERVAL {hours} HOUR"
        logger.debug(f"  Mode: Last {hours} hours (column: {ts_col})")
    
    # Fetch from MySQL
    try:
        with get_mysql() as mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                logger.debug(f"  Found {len(rows)} rows in MySQL")
    except Exception as e:
        logger.error(f"ERROR fetching {table_name} from MySQL: {e}")
        return 0
    
    if not rows:
        logger.debug(f"  No data to sync for {table_name}")
        return 0
    
    # Get columns from first row
    columns = list(rows[0].keys())
    
    # Map column names (MySQL '15_min_trail' -> DuckDB 'fifteen_min_trail')
    column_mapping = {'15_min_trail': 'fifteen_min_trail'}
    duckdb_columns = [column_mapping.get(c, c) for c in columns]
    
    success_count = 0
    error_count = 0
    
    if engine is not None:
        # Use TradingDataEngine (in-memory, zero locks)
        # Clear existing data first
        try:
            engine.execute(f"DELETE FROM {table_name}")
        except Exception as e:
            logger.debug(f"Could not clear table {table_name} in engine: {e}")
        
        # Insert new data
        placeholders = ", ".join(["?" for _ in duckdb_columns])
        columns_str = ", ".join(duckdb_columns)
        
        for row in rows:
            values = []
            for col in columns:
                val = row[col]
                
                # Convert datetime to string
                if hasattr(val, 'strftime'):
                    val = val.strftime('%Y-%m-%d %H:%M:%S')
                # Convert dict/list to JSON string
                elif isinstance(val, (dict, list)):
                    val = json.dumps(val)
                
                values.append(val)
            
            try:
                engine.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})", values)
                success_count += 1
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    logger.error(f"Error inserting row into {table_name} (engine): {e}")
    else:
        # Fallback to file-based DuckDB (standalone mode)
        with get_duckdb("central") as conn:
            # Ensure table exists
            if schema_sql:
                try:
                    conn.execute(schema_sql)
                    logger.debug(f"  Table {table_name} ensured to exist")
                except Exception as e:
                    logger.error(f"Failed to create table {table_name}: {e}")
                    return 0
            else:
                logger.error(f"No schema found for table {table_name} in ALL_SCHEMAS")
                return 0
            
            # Clear existing data
            try:
                conn.execute(f"DELETE FROM {table_name}")
            except Exception as e:
                logger.warning(f"Could not clear table {table_name}: {e}")
            
            # Insert new data
            placeholders = ", ".join(["?" for _ in duckdb_columns])
            columns_str = ", ".join(duckdb_columns)
            
            for row in rows:
                values = []
                for col in columns:
                    val = row[col]
                    
                    # Convert datetime to string
                    if hasattr(val, 'strftime'):
                        val = val.strftime('%Y-%m-%d %H:%M:%S')
                    # Convert dict/list to JSON string
                    elif isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    
                    values.append(val)
                
                try:
                    conn.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})", values)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:
                        logger.error(f"Error inserting row into {table_name}: {e}")
    
    if error_count > 0:
        logger.warning(f"Sync {table_name}: {success_count} inserted, {error_count} errors")
    else:
        logger.info(f"Sync {table_name}: {success_count} rows inserted")
    return success_count


def main():
    import argparse
    
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    parser = argparse.ArgumentParser(description='Sync MySQL data to DuckDB')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to sync (default: 24)')
    parser.add_argument('--full', action='store_true', help='Full sync for all tables')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync (default: all)')
    parser.add_argument('--init', action='store_true', help='Initialize DuckDB tables before sync')
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("MySQL to DuckDB Sync")
    logger.info("="*60)
    logger.info(f"MySQL: {settings.mysql.host}/{settings.mysql.database}")
    logger.info(f"DuckDB: {settings.central_db_path}")
    logger.info(f"Hours: {args.hours}")
    logger.info(f"Full sync: {args.full}")
    
    # Initialize tables if requested
    if args.init:
        logger.info("Initializing DuckDB tables...")
        init_duckdb_tables("central")
        logger.info("Tables initialized.")
    
    # Determine tables to sync
    tables_to_sync = args.tables if args.tables else ['follow_the_goat_plays'] + HOT_TABLES
    
    logger.info(f"Tables to sync: {tables_to_sync}")
    
    # Sync each table
    total_synced = 0
    for table in tables_to_sync:
        try:
            # Plays table always gets full sync
            full = args.full or table == 'follow_the_goat_plays'
            synced = sync_table(table, args.hours, full_sync=full)
            total_synced += synced
        except Exception as e:
            logger.error(f"ERROR syncing {table}: {e}")
    
    logger.info("="*60)
    logger.info(f"SYNC COMPLETE - Total rows synced: {total_synced}")
    logger.info("="*60)


if __name__ == "__main__":
    main()


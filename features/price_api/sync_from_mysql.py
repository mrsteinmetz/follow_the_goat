"""
MySQL to DuckDB Initial Sync Script
===================================
One-time script to populate DuckDB with the last 24 hours of data from MySQL.
Run this after setting up the new architecture.

Usage:
    python sync_from_mysql.py           # Sync last 24 hours
    python sync_from_mysql.py --hours 48  # Sync last 48 hours
    python sync_from_mysql.py --full    # Sync all data for plays table
"""

import sys
from pathlib import Path
import json
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, get_mysql, init_duckdb_tables
from core.config import settings
from features.price_api.schema import HOT_TABLES, TIMESTAMP_COLUMNS, ALL_SCHEMAS


def sync_table(table_name: str, hours: int = 24, full_sync: bool = False):
    """
    Sync a single table from MySQL to DuckDB.
    
    Args:
        table_name: Name of the table to sync
        hours: Number of hours of data to sync (for time-based tables)
        full_sync: If True, sync all data regardless of time
    """
    print(f"\n{'='*60}")
    print(f"Syncing table: {table_name}")
    print(f"{'='*60}")
    
    ts_col = TIMESTAMP_COLUMNS.get(table_name, 'created_at')
    
    # Build query for MySQL
    if table_name == 'follow_the_goat_plays' or full_sync:
        query = f"SELECT * FROM {table_name}"
        print(f"  Mode: Full sync (all data)")
    else:
        query = f"SELECT * FROM {table_name} WHERE {ts_col} >= NOW() - INTERVAL {hours} HOUR"
        print(f"  Mode: Last {hours} hours (column: {ts_col})")
    
    # Fetch from MySQL
    print(f"  Fetching from MySQL...")
    try:
        with get_mysql() as mysql_conn:
            with mysql_conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                print(f"  Found {len(rows)} rows in MySQL")
    except Exception as e:
        print(f"  ERROR fetching from MySQL: {e}")
        return 0
    
    if not rows:
        print(f"  No data to sync")
        return 0
    
    # Get columns from first row
    columns = list(rows[0].keys())
    
    # Map column names (MySQL '15_min_trail' -> DuckDB 'fifteen_min_trail')
    column_mapping = {'15_min_trail': 'fifteen_min_trail'}
    duckdb_columns = [column_mapping.get(c, c) for c in columns]
    
    # Clear existing data in DuckDB
    print(f"  Clearing existing data in DuckDB...")
    try:
        with get_duckdb("central") as conn:
            conn.execute(f"DELETE FROM {table_name}")
    except Exception as e:
        print(f"  Warning: Could not clear table (may not exist yet): {e}")
    
    # Insert into DuckDB
    print(f"  Inserting into DuckDB...")
    success_count = 0
    error_count = 0
    
    with get_duckdb("central") as conn:
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
                # Handle None
                elif val is None:
                    pass  # Keep as None
                
                values.append(val)
            
            try:
                conn.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})", values)
                success_count += 1
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Only show first 5 errors
                    print(f"  Error inserting row: {e}")
    
    print(f"  Sync complete: {success_count} rows inserted, {error_count} errors")
    return success_count


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync MySQL data to DuckDB')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to sync (default: 24)')
    parser.add_argument('--full', action='store_true', help='Full sync for all tables')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync (default: all)')
    parser.add_argument('--init', action='store_true', help='Initialize DuckDB tables before sync')
    args = parser.parse_args()
    
    print("="*60)
    print("MySQL to DuckDB Sync")
    print("="*60)
    print(f"MySQL: {settings.mysql.host}/{settings.mysql.database}")
    print(f"DuckDB: {settings.central_db_path}")
    print(f"Hours: {args.hours}")
    print(f"Full sync: {args.full}")
    
    # Initialize tables if requested
    if args.init:
        print("\nInitializing DuckDB tables...")
        init_duckdb_tables("central")
        print("Tables initialized.")
    
    # Determine tables to sync
    tables_to_sync = args.tables if args.tables else ['follow_the_goat_plays'] + HOT_TABLES
    
    print(f"\nTables to sync: {tables_to_sync}")
    
    # Sync each table
    total_synced = 0
    for table in tables_to_sync:
        try:
            # Plays table always gets full sync
            full = args.full or table == 'follow_the_goat_plays'
            synced = sync_table(table, args.hours, full_sync=full)
            total_synced += synced
        except Exception as e:
            print(f"ERROR syncing {table}: {e}")
    
    print("\n" + "="*60)
    print(f"SYNC COMPLETE")
    print(f"Total rows synced: {total_synced}")
    print("="*60)


if __name__ == "__main__":
    main()


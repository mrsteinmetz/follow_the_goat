"""
Keep 24 Hours of Data - Archive to Parquet
===========================================
Archives PostgreSQL data older than 24 hours to Parquet files.

This script:
1. Identifies transactional data older than 24 hours
2. Exports to compressed Parquet files (organized by table and date)
3. Deletes archived data from PostgreSQL
4. Preserves all configuration tables (never archived)

Triggered by: master2.py (hourly via APScheduler)
Storage: /root/follow_the_goat/archived_data/

Usage:
    # Manual execution
    python 000data_feeds/8_keep_24_hours_of_data/keep_24_hours_of_data.py
    
    # Or via master2.py (automated)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# =============================================================================
# CONFIGURATION
# =============================================================================

ARCHIVE_BASE_DIR = PROJECT_ROOT / "archived_data"
RETENTION_HOURS = 24  # Keep last 24 hours in PostgreSQL
COMPRESSION = 'snappy'  # Parquet compression algorithm
DRY_RUN = os.getenv("ARCHIVE_DRY_RUN", "0") == "1"  # Set to 1 to test without deleting

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# TABLE CLASSIFICATION
# =============================================================================

# Configuration tables - NEVER archived (kept forever)
CONFIG_TABLES = {
    'follow_the_goat_plays',
    'follow_the_goat_tracking',
    'pattern_config_projects',
    'pattern_config_filters',
    'wallet_profiles_state',
    'filter_fields_catalog',
    'filter_reference_suggestions',
    'filter_combinations',
    'ai_play_updates'
}

# Transactional tables with timestamp columns
# Format: {table_name: (timestamp_column, subdirectory)}
ARCHIVABLE_TABLES = {
    'prices': ('timestamp', 'prices'),
    'sol_stablecoin_trades': ('trade_timestamp', 'trades'),
    'order_book_features': ('timestamp', 'order_book'),
    'whale_movements': ('timestamp', 'whale_movements'),
    'cycle_tracker': ('cycle_start_time', 'cycles'),
    'follow_the_goat_buyins': ('followed_at', 'buyins'),
    'follow_the_goat_buyins_price_checks': ('checked_at', 'buyins'),
    'price_points': ('created_at', 'prices'),
    'price_analysis': ('created_at', 'prices'),
    'wallet_profiles': ('trade_timestamp', 'profiles'),
    'buyin_trail_minutes': ('created_at', 'buyins'),
    'trade_filter_values': ('created_at', 'trades'),
    'job_execution_metrics': ('started_at', 'metrics'),
}

# =============================================================================
# DIRECTORY MANAGEMENT
# =============================================================================

def ensure_archive_directories():
    """Create archive directory structure if it doesn't exist."""
    subdirs = set(info[1] for info in ARCHIVABLE_TABLES.values())
    
    for subdir in subdirs:
        dir_path = ARCHIVE_BASE_DIR / subdir
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {dir_path}")
    
    logger.info(f"Archive base directory: {ARCHIVE_BASE_DIR}")

# =============================================================================
# ARCHIVAL LOGIC
# =============================================================================

def archive_table_data(
    table_name: str,
    timestamp_column: str,
    subdirectory: str
) -> Dict[str, any]:
    """
    Archive data from a single table to Parquet.
    
    Process:
    1. Query data older than RETENTION_HOURS
    2. Save to Parquet file (date-based naming)
    3. Delete archived rows from PostgreSQL
    4. Return stats
    
    Args:
        table_name: Name of the table to archive
        timestamp_column: Column containing timestamp
        subdirectory: Subdirectory for organizing files
    
    Returns:
        Dictionary with stats (rows_archived, rows_deleted, file_size, etc.)
    """
    stats = {
        'table': table_name,
        'rows_queried': 0,
        'rows_archived': 0,
        'rows_deleted': 0,
        'file_size_bytes': 0,
        'file_path': None,
        'error': None
    }
    
    try:
        # STEP 1: Query old data
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)
        
        logger.info(f"Archiving {table_name}: data older than {cutoff_time.isoformat()}")
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Query old data
                query = f"""
                    SELECT * FROM {table_name}
                    WHERE {timestamp_column} < %s
                    ORDER BY {timestamp_column}
                """
                cursor.execute(query, [cutoff_time])
                rows = cursor.fetchall()
                stats['rows_queried'] = len(rows)
        
        if stats['rows_queried'] == 0:
            logger.debug(f"No old data to archive from {table_name}")
            return stats
        
        logger.info(f"Found {stats['rows_queried']} rows to archive from {table_name}")
        
        # STEP 2: Convert to DataFrame and save to Parquet
        df = pd.DataFrame(rows)
        
        # Generate filename with current date
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        filename = f"{table_name}_{date_str}.parquet"
        file_path = ARCHIVE_BASE_DIR / subdirectory / filename
        
        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists (append mode)
        if file_path.exists():
            logger.info(f"Appending to existing file: {file_path}")
            # Read existing data
            existing_df = pd.read_parquet(file_path)
            # Combine with new data
            df = pd.concat([existing_df, df], ignore_index=True)
            logger.info(f"Combined {len(existing_df)} existing + {stats['rows_queried']} new rows")
        
        # Write to Parquet with compression
        df.to_parquet(
            file_path,
            compression=COMPRESSION,
            index=False,
            engine='pyarrow'
        )
        
        stats['rows_archived'] = stats['rows_queried']
        stats['file_size_bytes'] = file_path.stat().st_size
        stats['file_path'] = str(file_path)
        
        logger.info(
            f"Archived {stats['rows_archived']} rows to {file_path} "
            f"({stats['file_size_bytes'] / 1024 / 1024:.2f} MB)"
        )
        
        # STEP 3: Delete archived data from PostgreSQL
        if not DRY_RUN:
            delete_query = f"""
                DELETE FROM {table_name}
                WHERE {timestamp_column} < %s
            """
            rows_deleted = postgres_execute(delete_query, [cutoff_time])
            stats['rows_deleted'] = rows_deleted
            
            logger.info(f"Deleted {rows_deleted} rows from {table_name}")
            
            if rows_deleted != stats['rows_archived']:
                logger.warning(
                    f"Mismatch: archived {stats['rows_archived']} but deleted {rows_deleted} rows"
                )
        else:
            logger.info(f"DRY RUN: Would delete {stats['rows_archived']} rows from {table_name}")
            stats['rows_deleted'] = 0
        
        return stats
        
    except Exception as e:
        stats['error'] = str(e)
        logger.error(f"Error archiving {table_name}: {e}", exc_info=True)
        return stats

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run() -> Dict[str, any]:
    """
    Main entry point for data archival.
    
    Archives all transactional tables, preserving configuration tables.
    
    Returns:
        Summary statistics for the archival run
    """
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 70)
    logger.info("Starting data archival process")
    logger.info(f"Retention period: {RETENTION_HOURS} hours")
    logger.info(f"Archive location: {ARCHIVE_BASE_DIR}")
    logger.info(f"Compression: {COMPRESSION}")
    logger.info(f"Dry run mode: {DRY_RUN}")
    logger.info("=" * 70)
    
    # Ensure directories exist
    ensure_archive_directories()
    
    # Archive each table
    results = []
    total_rows_archived = 0
    total_rows_deleted = 0
    total_size_bytes = 0
    errors = []
    
    for table_name, (timestamp_col, subdir) in ARCHIVABLE_TABLES.items():
        # Safety check: never archive config tables
        if table_name in CONFIG_TABLES:
            logger.error(f"SAFETY: Skipping {table_name} (configuration table)")
            continue
        
        try:
            stats = archive_table_data(table_name, timestamp_col, subdir)
            results.append(stats)
            
            total_rows_archived += stats['rows_archived']
            total_rows_deleted += stats['rows_deleted']
            total_size_bytes += stats['file_size_bytes']
            
            if stats['error']:
                errors.append(f"{table_name}: {stats['error']}")
        
        except Exception as e:
            logger.error(f"Unexpected error processing {table_name}: {e}", exc_info=True)
            errors.append(f"{table_name}: {str(e)}")
    
    # Summary
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 70)
    logger.info("Data archival complete")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"Tables processed: {len(results)}")
    logger.info(f"Total rows archived: {total_rows_archived}")
    logger.info(f"Total rows deleted: {total_rows_deleted}")
    logger.info(f"Total archive size: {total_size_bytes / 1024 / 1024:.2f} MB")
    
    if errors:
        logger.warning(f"Errors encountered: {len(errors)}")
        for error in errors:
            logger.warning(f"  - {error}")
    else:
        logger.info("No errors encountered")
    
    logger.info("=" * 70)
    
    return {
        'success': len(errors) == 0,
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': duration,
        'tables_processed': len(results),
        'total_rows_archived': total_rows_archived,
        'total_rows_deleted': total_rows_deleted,
        'total_size_bytes': total_size_bytes,
        'errors': errors,
        'details': results
    }

# =============================================================================
# CLI EXECUTION
# =============================================================================

if __name__ == "__main__":
    result = run()
    
    # Exit with error code if archival failed
    if not result['success']:
        sys.exit(1)

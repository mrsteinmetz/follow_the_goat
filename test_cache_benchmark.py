#!/usr/bin/env python3
"""
DuckDB Cache Benchmark
======================
Test and benchmark the DuckDB cache performance vs direct PostgreSQL queries.

This script will:
1. Clear and rebuild cache
2. Benchmark PostgreSQL direct query
3. Benchmark DuckDB cached query
4. Show performance comparison
5. Verify data integrity between both methods
"""

import logging
import sys
import time
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.filter_cache import (
    sync_cache_incremental, 
    get_cached_trades, 
    get_cache_stats,
    clear_cache
)
from core.database import get_postgres

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def benchmark_postgres_direct(hours: int = 24) -> tuple:
    """
    Benchmark loading trade data directly from PostgreSQL (old method).
    
    Returns:
        (execution_time, row_count, trade_count)
    """
    logger.info("=" * 80)
    logger.info("BENCHMARK: PostgreSQL Direct Query")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Get distinct filter columns
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT tfv.filter_name
                    FROM trade_filter_values tfv
                    INNER JOIN follow_the_goat_buyins b ON b.id = tfv.buyin_id
                    WHERE b.potential_gains IS NOT NULL
                      AND b.followed_at >= NOW() - INTERVAL '%s hours'
                    ORDER BY tfv.filter_name
                """, [hours])
                filter_columns = [r['filter_name'] for r in cursor.fetchall()]
        
        if not filter_columns:
            logger.warning("No filter columns found")
            return (0, 0, 0)
        
        logger.info(f"Found {len(filter_columns)} filter columns")
        
        # Build pivoted query
        pivot_cols = []
        for col in filter_columns:
            safe_col = col.replace("'", "''")
            pivot_cols.append(
                f"MAX(tfv.filter_value) FILTER (WHERE tfv.filter_name = '{safe_col}') AS \"{col}\""
            )
        
        pivot_sql = ",\n            ".join(pivot_cols)
        
        query = f"""
            SELECT 
                b.id as trade_id,
                b.play_id,
                b.followed_at,
                b.potential_gains,
                b.our_status,
                tfv.minute,
                {pivot_sql}
            FROM follow_the_goat_buyins b
            INNER JOIN trade_filter_values tfv ON tfv.buyin_id = b.id
            WHERE b.potential_gains IS NOT NULL
              AND b.followed_at >= NOW() - INTERVAL '{hours} hours'
            GROUP BY b.id, b.play_id, b.followed_at, b.potential_gains, b.our_status, tfv.minute
            ORDER BY b.id, tfv.minute
        """
        
        # Execute query
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        
        execution_time = time.time() - start_time
        
        row_count = len(results)
        trade_count = len(set(r['trade_id'] for r in results)) if results else 0
        
        logger.info(f"Loaded {row_count:,} rows ({trade_count:,} trades)")
        logger.info(f"Execution time: {execution_time:.2f}s")
        
        return (execution_time, row_count, trade_count)
        
    except Exception as e:
        logger.error(f"PostgreSQL benchmark failed: {e}", exc_info=True)
        return (0, 0, 0)


def benchmark_duckdb_cache(hours: int = 24) -> tuple:
    """
    Benchmark loading trade data from DuckDB cache.
    
    Returns:
        (execution_time, row_count, trade_count)
    """
    logger.info("=" * 80)
    logger.info("BENCHMARK: DuckDB Cache Query")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Sync cache (should be fast if already synced)
        sync_time_start = time.time()
        cache_age = sync_cache_incremental()
        sync_time = time.time() - sync_time_start
        
        logger.info(f"Cache sync took {sync_time:.2f}s (age: {cache_age:.0f}s)")
        
        # Query cache
        query_time_start = time.time()
        df = get_cached_trades(hours=hours)
        query_time = time.time() - query_time_start
        
        execution_time = time.time() - start_time
        
        row_count = len(df)
        trade_count = df['trade_id'].nunique() if not df.empty else 0
        
        logger.info(f"Loaded {row_count:,} rows ({trade_count:,} trades)")
        logger.info(f"Query time: {query_time:.2f}s")
        logger.info(f"Total time (including sync): {execution_time:.2f}s")
        
        return (execution_time, row_count, trade_count)
        
    except Exception as e:
        logger.error(f"DuckDB benchmark failed: {e}", exc_info=True)
        return (0, 0, 0)


def run_benchmark(hours: int = 24, clear_cache_first: bool = False):
    """
    Run complete benchmark comparing PostgreSQL vs DuckDB.
    
    Args:
        hours: Number of hours to analyze
        clear_cache_first: If True, clears cache before benchmarking
    """
    logger.info("\n")
    logger.info("*" * 80)
    logger.info("FILTER CACHE PERFORMANCE BENCHMARK")
    logger.info("*" * 80)
    logger.info(f"Analysis window: {hours} hours")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("*" * 80)
    logger.info("\n")
    
    # Show current cache stats
    logger.info("Current cache status:")
    stats = get_cache_stats()
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    logger.info("\n")
    
    # Clear cache if requested
    if clear_cache_first:
        logger.info("Clearing cache for clean benchmark...")
        clear_cache()
        logger.info("Cache cleared\n")
    
    # Benchmark 1: PostgreSQL direct
    pg_time, pg_rows, pg_trades = benchmark_postgres_direct(hours)
    logger.info("\n")
    
    # Benchmark 2: DuckDB cache (this will rebuild cache if cleared)
    duck_time, duck_rows, duck_trades = benchmark_duckdb_cache(hours)
    logger.info("\n")
    
    # Show updated cache stats
    logger.info("Final cache status:")
    stats = get_cache_stats()
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    logger.info("\n")
    
    # Comparison
    logger.info("=" * 80)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 80)
    logger.info(f"PostgreSQL Direct:")
    logger.info(f"  Time: {pg_time:.2f}s")
    logger.info(f"  Rows: {pg_rows:,}")
    logger.info(f"  Trades: {pg_trades:,}")
    logger.info("")
    logger.info(f"DuckDB Cache:")
    logger.info(f"  Time: {duck_time:.2f}s")
    logger.info(f"  Rows: {duck_rows:,}")
    logger.info(f"  Trades: {duck_trades:,}")
    logger.info("")
    
    if pg_time > 0 and duck_time > 0:
        speedup = pg_time / duck_time
        time_saved = pg_time - duck_time
        logger.info(f"Performance Improvement:")
        logger.info(f"  Speedup: {speedup:.1f}x faster")
        logger.info(f"  Time saved: {time_saved:.2f}s ({time_saved/60:.1f} minutes)")
        logger.info("")
        
        if speedup >= 10:
            logger.info("✅ SUCCESS: Achieved 10x+ speedup target!")
        elif speedup >= 5:
            logger.info("✅ GOOD: Achieved 5x+ speedup")
        elif speedup >= 2:
            logger.info("⚠️  MODERATE: Achieved 2x+ speedup")
        else:
            logger.info("❌ NEEDS IMPROVEMENT: Speedup less than 2x")
    
    # Data integrity check
    logger.info("")
    logger.info("Data Integrity Check:")
    if pg_rows == duck_rows and pg_trades == duck_trades:
        logger.info("✅ PASS: Row counts match exactly")
    else:
        logger.info(f"⚠️  WARNING: Row count mismatch")
        logger.info(f"  PostgreSQL: {pg_rows:,} rows, {pg_trades:,} trades")
        logger.info(f"  DuckDB: {duck_rows:,} rows, {duck_trades:,} trades")
        logger.info(f"  Difference: {abs(pg_rows - duck_rows):,} rows")
    
    logger.info("=" * 80)


def test_incremental_sync():
    """Test that incremental sync only loads new data."""
    logger.info("\n")
    logger.info("=" * 80)
    logger.info("TEST: Incremental Sync")
    logger.info("=" * 80)
    
    # First sync
    logger.info("First sync (should load all data)...")
    start = time.time()
    cache_age = sync_cache_incremental()
    first_sync_time = time.time() - start
    logger.info(f"First sync took {first_sync_time:.2f}s")
    
    stats = get_cache_stats()
    first_count = stats.get('trades_cached', 0)
    logger.info(f"Trades in cache: {first_count:,}")
    
    # Second sync immediately after (should be fast)
    logger.info("\nSecond sync (should be instant - no new data)...")
    start = time.time()
    cache_age = sync_cache_incremental()
    second_sync_time = time.time() - start
    logger.info(f"Second sync took {second_sync_time:.2f}s")
    
    stats = get_cache_stats()
    second_count = stats.get('trades_cached', 0)
    logger.info(f"Trades in cache: {second_count:,}")
    
    if second_sync_time < first_sync_time / 10:
        logger.info("✅ PASS: Incremental sync is much faster than initial sync")
    else:
        logger.info("⚠️  WARNING: Incremental sync not significantly faster")
    
    logger.info("=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark DuckDB cache performance")
    parser.add_argument(
        "--hours", 
        type=int, 
        default=24, 
        help="Number of hours to analyze (default: 24)"
    )
    parser.add_argument(
        "--clear-cache", 
        action="store_true",
        help="Clear cache before benchmarking"
    )
    parser.add_argument(
        "--test-incremental",
        action="store_true",
        help="Test incremental sync performance"
    )
    
    args = parser.parse_args()
    
    try:
        # Run main benchmark
        run_benchmark(hours=args.hours, clear_cache_first=args.clear_cache)
        
        # Test incremental sync if requested
        if args.test_incremental:
            test_incremental_sync()
        
        logger.info("\n✅ Benchmark complete!\n")
        
    except KeyboardInterrupt:
        logger.info("\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Benchmark failed: {e}", exc_info=True)
        sys.exit(1)

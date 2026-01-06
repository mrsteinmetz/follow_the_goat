#!/usr/bin/env python3
"""
Simple fix: Delete corrupted cycles via master2's local DuckDB
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import scheduler to get access to local DuckDB
from scheduler.master2 import _local_duckdb, _local_duckdb_lock, queue_write_sync
from datetime import datetime

print("Fixing corrupted cycles...")

if _local_duckdb is None:
    print("ERROR: master2.py not running or local DuckDB not initialized")
    sys.exit(1)

# Delete corrupted cycles
def delete_corrupted(conn):
    result = conn.execute("""
        DELETE FROM cycle_tracker 
        WHERE cycle_end_time IS NOT NULL 
        AND cycle_end_time < cycle_start_time
        RETURNING id
    """).fetchall()
    return len(result)

deleted = queue_write_sync(delete_corrupted, _local_duckdb)
print(f"✓ Deleted {deleted} corrupted cycles")

# Close one active cycle for testing
def close_test_cycle(conn):
    # Get latest price timestamp
    latest = conn.execute("SELECT MAX(ts) FROM prices WHERE token = 'SOL'").fetchone()
    if not latest or not latest[0]:
        return 0
    
    latest_ts = latest[0]
    print(f"  Latest price timestamp: {latest_ts}")
    
    # Close the 0.3% threshold cycle
    result = conn.execute("""
        UPDATE cycle_tracker 
        SET cycle_end_time = ?
        WHERE threshold = 0.3 
        AND cycle_end_time IS NULL
        RETURNING id
    """, [latest_ts]).fetchall()
    
    return len(result)

closed = queue_write_sync(close_test_cycle, _local_duckdb)
print(f"✓ Closed {closed} cycle(s) for testing")

# Verify
def verify(conn):
    # Count valid completed cycles
    result = conn.execute("""
        SELECT COUNT(*) FROM cycle_tracker
        WHERE cycle_end_time IS NOT NULL
        AND cycle_end_time >= cycle_start_time
    """).fetchone()
    valid = result[0] if result else 0
    print(f"✓ Valid completed cycles: {valid}")
    
    # Count matching trades
    if valid > 0:
        result2 = conn.execute("""
            SELECT COUNT(*) 
            FROM sol_stablecoin_trades t
            INNER JOIN cycle_tracker c ON (
                c.threshold = 0.3
                AND c.cycle_start_time <= t.trade_timestamp
                AND c.cycle_end_time >= t.trade_timestamp
                AND c.cycle_end_time IS NOT NULL
            )
            WHERE t.direction = 'buy'
        """).fetchone()
        matches = result2[0] if result2 else 0
        print(f"✓ Trades matching cycles: {matches}")
        return matches
    return 0

with _local_duckdb_lock:
    matches = verify(_local_duckdb)

if matches > 0:
    print("\nSUCCESS! Profiles should be created in next scheduler run (5-10 seconds)")
else:
    print("\nWARNING: No trades match the completed cycle time range")


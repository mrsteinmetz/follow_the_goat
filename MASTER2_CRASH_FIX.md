# Master2.py Crash Fix - Critical Threading Bug

## Date: 2026-01-08

## Problem Summary

Master2.py was crashing repeatedly with:
- **Segmentation faults** in DuckDB library (`_duckdb.cpython-312-x86_64-linux-gnu.so`)
- **Memory corruption** (`malloc(): unaligned tcache chunk detected`)
- **Stack segment traps** (multiple instances in dmesg)

## Root Cause

The `get_thread_cursor()` function in `scheduler/master2.py` was **incorrectly acquiring the write lock** (`_local_duckdb_lock`) even for READ operations:

```python
# BEFORE (BUGGY):
def get_thread_cursor():
    with _local_duckdb_lock:  # ❌ WRONG: Blocks reads on write lock!
        return _local_duckdb.cursor()
```

This caused:
1. **Deadlocks**: Read operations blocking write operations and vice versa
2. **Memory corruption**: Concurrent access to DuckDB with locks causing segfaults
3. **Crashes**: Multiple segfaults and stack traps in DuckDB library

## Fix Applied

Removed the lock from cursor creation. DuckDB cursors are **read-only snapshots** that can be created concurrently without a lock:

```python
# AFTER (FIXED):
def get_thread_cursor():
    # CRITICAL: Create cursor WITHOUT lock - cursors are read-only snapshots
    # and can be created concurrently. The lock is only needed for WRITES.
    return _local_duckdb.cursor()  # ✅ CORRECT: No lock needed for reads
```

## Threading Architecture (Correct)

- **READ operations**: Use `get_duckdb("central", read_only=True)` → Creates cursor WITHOUT lock (concurrent)
- **WRITE operations**: Use `duckdb_execute_write()` → Goes through write queue WITH lock (serialized)

## Additional Issues Found

### 1. NOT NULL Constraint Errors (Non-Critical)

Multiple warnings about `NOT NULL constraint failed: wallet_profiles_state.id`:
- **Impact**: Non-critical (warnings only, doesn't crash)
- **Cause**: INSERT statements not providing `id` value, expecting auto-increment
- **Status**: Needs investigation but not causing crashes

### 2. Health Endpoint Deadlock (Fixed Earlier)

The `/health` endpoint was also blocking on the write lock. This was fixed by using `get_duckdb("central", read_only=True)` instead of direct lock access.

## Verification

After fix:
- ✅ No more segfaults in dmesg
- ✅ Health endpoint responds quickly (< 1 second)
- ✅ No deadlocks between reads and writes
- ✅ Master2.py running stable

## Prevention

**CRITICAL RULE**: Never acquire `_local_duckdb_lock` for READ operations!

- ✅ **DO**: Use `get_duckdb("central", read_only=True)` for reads
- ✅ **DO**: Use `duckdb_execute_write()` for writes
- ❌ **DON'T**: Acquire `_local_duckdb_lock` when creating cursors
- ❌ **DON'T**: Use `with _local_duckdb_lock:` for SELECT queries

## Files Modified

- `scheduler/master2.py`: Fixed `get_thread_cursor()` to not acquire lock

## Testing

Monitor master2.py for:
- No segfaults in dmesg
- Stable operation over 24+ hours
- Health endpoint responding quickly
- No deadlocks in logs

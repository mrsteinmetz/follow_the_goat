# Master2.py PostgreSQL Migration Guide

## Overview

The master2.py file is approximately 4000 lines and requires systematic refactoring to remove all DuckDB code and use PostgreSQL directly. This guide provides a step-by-step approach.

## Phase 1: Update File Header and Imports

### Lines 1-75: Update Documentation and Imports

**REMOVE these imports:**
```python
import duckdb
from core.data_client import DataClient, get_client
```

**ADD these imports:**
```python
from core.database import get_postgres, postgres_execute, postgres_query, postgres_query_one, postgres_insert, postgres_update
```

**UPDATE docstring (lines 1-21):**
```python
"""
Master2 Scheduler - Trading Logic
=================================
Trading logic that reads/writes directly to PostgreSQL.

Usage:
    python scheduler/master2.py

This script (TRADING LOGIC):
1. Connects to PostgreSQL database (shared with master.py)
2. Runs trading jobs: follow_the_goat, trailing_stop, train_validator, etc.
3. Provides Local API (port 5052) for website

Prerequisites:
- PostgreSQL must be running and schema initialized
- master.py should be running (for data ingestion)

Shutdown:
    Press Ctrl+C to gracefully stop.
"""
```

## Phase 2: Remove DuckDB Infrastructure (Lines 77-850)

### DELETE these entire sections:

**Lines 77-113: Thread-local cursor system**
```python
# Global references
_local_duckdb = None
_local_duckdb_lock = threading.Lock()
_thread_local = threading.local()

def get_thread_cursor():
    # ... entire function
```

**Lines 121-326: Write queue infrastructure**
```python
from queue import Queue, Empty

_write_queue = Queue()
_writer_thread = None
_writer_running = threading.Event()

def queue_write(...):
def queue_write_sync(...):
def background_writer():
def start_write_queue():
def stop_write_queue():
def get_write_queue_stats():
```

**Lines 454-850: Local DuckDB initialization**
```python
def init_local_duckdb():
def get_local_duckdb(use_cursor=True):
def get_master2_db_for_writes():
```

## Phase 3: Simplify Local API Server (Lines 855-1200)

The Local API server (port 5052) should remain but query PostgreSQL directly instead of local DuckDB.

### UPDATE each endpoint to use PostgreSQL:

**Example transformation:**
```python
# OLD (lines ~990-1074)
@app.get("/cycle_tracker")
async def get_cycle_tracker(...):
    global _local_duckdb, _local_duckdb_lock
    
    with _local_duckdb_lock:
        results = _local_duckdb.execute(f"""
            SELECT * FROM cycle_tracker WHERE {where_clause}
        """).fetchall()
        columns = [desc[0] for desc in _local_duckdb.description]

# NEW
@app.get("/cycle_tracker")
async def get_cycle_tracker(...):
    from core.database import get_postgres
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT * FROM cycle_tracker WHERE {where_clause}
            """)
            results = cursor.fetchall()
```

**Endpoints to update:**
- `/health` - query PostgreSQL for table counts
- `/cycle_tracker` - query cycle_tracker table
- `/price_analysis` - query price_analysis table
- `/profiles` - query wallet_profiles table
- `/profiles/stats` - aggregate wallet_profiles
- `/buyins` - query follow_the_goat_buyins
- `/plays` - query follow_the_goat_plays
- `/patterns/*` - query pattern tables
- `/scheduler/status` - keep as-is (reads from status file)
- `/query_sql` - execute user SQL on PostgreSQL

## Phase 4: Remove Backfill Logic (Lines 1200-1600)

### DELETE these entire functions:

```python
def backfill_from_data_engine()
def sync_new_data_from_engine()
def _insert_records_fast()
def _sync_prices_incremental()
def _sync_trades_incremental()
def _sync_cycles_incremental()
# ... all sync functions
```

## Phase 5: Update Trading Job Functions

### UPDATE each trading job to use PostgreSQL:

**Pattern:**
```python
# OLD
@track_job("job_name", "Description")
def job_function():
    with get_duckdb("central", read_only=True) as cursor:
        result = cursor.execute("SELECT * FROM table WHERE id = ?", [123]).fetchall()
    
    # Process...
    
    duckdb_execute_write("central", "UPDATE table SET x = ? WHERE id = ?", [val, id])

# NEW
@track_job("job_name", "Description")
def job_function():
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM table WHERE id = %s", [123])
            result = cursor.fetchall()
    
    # Process...
    
    postgres_execute("UPDATE table SET x = %s WHERE id = %s", [val, id])
```

**Jobs to update:**
- `follow_the_goat_job()`
- `trailing_stop_seller_job()`
- `train_validator_job()`
- `update_potential_gains_job()`
- `create_wallet_profiles_job()`
- `cleanup_wallet_profiles_job()`
- `create_new_patterns_job()`

## Phase 6: Update Scheduler Configuration

### REMOVE backfill/sync jobs:

**DELETE from create_scheduler():**
```python
# Remove these job registrations:
scheduler.add_job(backfill_from_data_engine, ...)
scheduler.add_job(sync_new_data_from_engine, ...)
```

### KEEP trading jobs:

```python
# follow_the_goat
scheduler.add_job(
    func=follow_the_goat_job,
    trigger=IntervalTrigger(seconds=1),
    id="follow_the_goat",
    ...
)

# trailing_stop_seller
scheduler.add_job(
    func=trailing_stop_seller_job,
    trigger=IntervalTrigger(seconds=1),
    id="trailing_stop_seller",
    ...
)

# ... etc
```

## Phase 7: Simplify main() Function

### REMOVE DuckDB initialization:

**OLD main() structure:**
```python
def main():
    # 1. Initialize local DuckDB
    init_local_duckdb()
    start_write_queue()
    
    # 2. Backfill from master.py
    backfill_from_data_engine()
    
    # 3. Start Local API server
    start_local_api_server_in_background()
    
    # 4. Create scheduler with sync jobs
    _scheduler = create_scheduler()
    ...
```

**NEW main() structure:**
```python
def main():
    # 1. Verify PostgreSQL connection
    from core.database import verify_tables_exist
    if not verify_tables_exist():
        logger.error("PostgreSQL not ready!")
        sys.exit(1)
    
    # 2. Start Local API server
    start_local_api_server_in_background()
    
    # 3. Create scheduler (trading jobs only)
    _scheduler = create_scheduler()
    
    # 4. Start scheduler
    _scheduler.start()
    
    # Keep alive
    while True:
        time.sleep(1)
```

### REMOVE from shutdown_all():

```python
# Remove:
stop_write_queue()
if _local_duckdb:
    _local_duckdb.close()
```

## Phase 8: Update Parameter Syntax Throughout

### Global Search/Replace:

Use your editor's find/replace with regex:

**Find:** `\.execute\("([^"]*)\?`
**Replace:** `.execute("$1%s`

**Find:** `\.execute\('([^']*)\?`
**Replace:** `.execute('$1%s`

This will change all `?` placeholders to `%s` in SQL queries.

## Phase 9: Update Data Type Conversions

### Find and update these patterns:

**DuckDB types → PostgreSQL types:**
```python
# OLD
UBIGINT, UINTEGER, TINYINT, DOUBLE

# NEW
BIGINT, INTEGER, SMALLINT, DOUBLE PRECISION
```

## Testing Checklist

After refactoring master2.py:

1. **Syntax check:**
   ```bash
   python -m py_compile scheduler/master2.py
   ```

2. **Import test:**
   ```python
   python -c "from scheduler import master2; print('OK')"
   ```

3. **Start test:**
   ```bash
   python scheduler/master2.py
   # Should start without errors
   # No backfill messages
   # Trading jobs should register
   ```

4. **API test:**
   ```bash
   curl http://localhost:5052/health
   # Should return table counts from PostgreSQL
   ```

5. **Job execution:**
   - Check logs for job execution
   - Verify no DuckDB errors
   - Verify PostgreSQL queries working

## Common Issues and Solutions

### Issue: ImportError for duckdb
**Solution:** Remove all `import duckdb` lines

### Issue: "get_duckdb" not found
**Solution:** Replace with `get_postgres()`

### Issue: SQL syntax errors with `?`
**Solution:** Change all `?` to `%s`

### Issue: cursor.fetchall() empty
**Solution:** Check that data exists in PostgreSQL first

### Issue: "Connection pool exhausted"
**Solution:** Make sure to use context managers (`with get_postgres()`)

## Estimated Effort

- **Phase 1-2:** 1 hour (remove infrastructure)
- **Phase 3:** 2 hours (update API endpoints)
- **Phase 4:** 30 minutes (remove backfill)
- **Phase 5:** 2 hours (update trading jobs)
- **Phase 6:** 30 minutes (update scheduler)
- **Phase 7:** 30 minutes (simplify main)
- **Phase 8:** 1 hour (parameter syntax)
- **Phase 9:** 30 minutes (data types)
- **Testing:** 2 hours

**Total:** ~10 hours for thorough refactoring and testing

## Next File After master2.py

Once master2.py is complete, move to:
1. **Trading modules** (simpler, just parameter updates)
2. **Data ingestion modules** (Jupiter, Binance, cycles)
3. **Website API** (remove proxying)

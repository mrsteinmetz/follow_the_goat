# Debug Prompt: Fix "cursor already closed" Error in sell_trailing_stop.py

## Context
After migrating from DuckDB to PostgreSQL, the trade detail page (`/goats/unique/trade/`) was successfully fixed, but price checks are not being generated because `sell_trailing_stop.py` is failing with a persistent database cursor error.

---

## What Was Already Fixed ✅

1. **Trade Page Loading (FIXED)**
   - File: `000website/goats/unique/trade/index.php`
   - Issue: PHP was calling `trim()` on `entry_log` field which is now an array (PostgreSQL) instead of JSON string
   - Fix: Added logic to handle both array and string formats
   - Status: **Working** - page loads successfully with HTTP 200

2. **Price Checks API Endpoint (ADDED)**
   - File: `scheduler/website_api.py`
   - Added new endpoint: `GET /price_checks?buyin_id=X&hours=all&limit=100`
   - Returns price check history for trades
   - Status: **Working** - endpoint returns 200 but no data (because trailing stop isn't writing)

3. **Price Checks Writing Logic (ENABLED)**
   - File: `000trading/sell_trailing_stop.py` line 673-703
   - Removed the `if not backfilled: return True` logic that was skipping normal price check writes
   - Now ALL price checks should be written to database, not just backfills
   - Status: **Code updated** but not working due to cursor error

4. **Wrong Table Name (FIXED)**
   - File: `000trading/sell_trailing_stop.py` line 193-199
   - Changed from querying `price_points` table to `prices` table
   - Changed query: `SELECT price, timestamp, id FROM prices WHERE token = 'SOL'`
   - Status: **Fixed** but still not working due to cursor error

---

## Current Problem ❌

### Error Message
```
ERROR - Error getting current SOL price: cursor already closed
WARNING - Could not get current price, skipping this check
```

### Location
- File: `000trading/sell_trailing_stop.py`
- Method: `TrailingStopSeller.get_current_price()` (lines 187-212)
- Called by: `run_single_cycle()` every 1 second via APScheduler in `scheduler/master2.py`

### The Issue
The `get_current_price()` method uses the connection pool pattern:
```python
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT price, timestamp, id FROM prices WHERE token = 'SOL' ORDER BY timestamp DESC LIMIT 1")
        result = cursor.fetchone()  # ← FAILS HERE: cursor already closed
```

**However**: The EXACT SAME pattern works perfectly in:
- `scheduler/website_api.py` (all 39 endpoints work fine)
- Standalone test script (`test_connection_pool.py`) - works 5/5 times
- Direct psycopg2 connection (no pool) - works

### What We've Tried (ALL FAILED)

1. ❌ Removed `conn.autocommit = True` from connection pool (line 113 in `core/database.py`)
2. ❌ Added `conn.rollback()` to reset connection state when getting from pool
3. ❌ Changed table name from `price_points` to `prices`
4. ❌ Fixed SQL syntax (changed `value` to `price`, `coin_id=5` to `token='SOL'`)
5. ❌ Cleared Python `__pycache__` files
6. ❌ Killed and restarted all Python processes multiple times
7. ❌ Started services one at a time (not using `start_all.sh`)
8. ❌ Waited for old connection pool instances to expire

### Observations

1. **The error is PERSISTENT** - happens every single second, never succeeds
2. **Timing pattern**: Logs show TWO timestamps running simultaneously:
   ```
   16:57:10,736 - ERROR - cursor already closed
   16:57:10,184 - ERROR - cursor already closed
   ```
   This suggests **TWO TrailingStopSeller instances** might be running

3. **website_api.py works** - Uses identical `with get_postgres() as conn: with conn.cursor()` pattern with NO errors

4. **Test script works** - Standalone script using connection pool works perfectly

5. **Only fails in APScheduler context** - The error ONLY happens when called from `scheduler/master2.py`

---

## Files Modified (Summary)

### 1. `000website/goats/unique/trade/index.php`
**Lines 208-241**: Handle `entry_log` as both array (PostgreSQL) and string (legacy)
**Lines 119-135**: Added error suppression for optional project name lookups

### 2. `000website/includes/DatabaseClient.php`
**Lines 119-127**: Only log 500+ errors, not 404s (reduces noise)

### 3. `scheduler/website_api.py`
**Lines 747-808**: Added new `/price_checks` endpoint

### 4. `000trading/sell_trailing_stop.py`
**Lines 190-212**: Changed table from `price_points` to `prices`, updated column names
**Lines 673-703**: Removed `if not backfilled: return True` - now writes ALL price checks

### 5. `core/database.py`
**Line 113**: Removed `conn.autocommit = True` (tried to fix cursor issue)
**Line 114**: Added `conn.rollback()` to reset state (tried to fix cursor issue)

---

## Task for Fresh Agent

### Primary Goal
Fix the "cursor already closed" error in `sell_trailing_stop.py` so that price checks are written to the database and displayed on the trade detail page.

### Investigation Steps

1. **Check if multiple TrailingStopSeller instances exist**
   - Search for where `TrailingStopSeller` is instantiated
   - Check if APScheduler is creating multiple instances
   - Look for singleton pattern or global instance issues

2. **Compare with working code**
   - `scheduler/website_api.py` uses IDENTICAL pattern and works
   - Find the difference between how it's called vs `sell_trailing_stop.py`
   - Check if there's a threading/concurrency issue

3. **Check APScheduler configuration**
   - File: `scheduler/master2.py` lines 537-542
   - Look for executor configuration that might affect database connections
   - Check if jobs are running in separate threads/processes

4. **Test hypothesis: Connection pool per-thread issue**
   - psycopg2.pool.SimpleConnectionPool is thread-safe
   - But maybe APScheduler executors are creating process pools?
   - Try using ThreadPoolExecutor explicitly

5. **Alternative: Bypass connection pool**
   - As a last resort, try creating a fresh connection each time in `get_current_price()`
   - This would confirm if the pool is the problem

### Success Criteria

✅ `sell_trailing_stop.py` successfully queries current price from `prices` table  
✅ Price checks are written to `follow_the_goat_buyins_price_checks` table  
✅ Trade detail page displays price check timeline  
✅ No "cursor already closed" errors in logs

---

## Database Schema Reference

```sql
-- Prices table (source of current price)
CREATE TABLE prices (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    token VARCHAR(20) NOT NULL,  -- 'SOL'
    price DOUBLE PRECISION NOT NULL,
    source VARCHAR(20) DEFAULT 'jupiter',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Price checks table (destination for trailing stop data)
CREATE TABLE follow_the_goat_buyins_price_checks (
    id BIGSERIAL PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    current_price DOUBLE PRECISION,
    entry_price DOUBLE PRECISION,
    highest_price DOUBLE PRECISION,
    reference_price DOUBLE PRECISION,
    gain_from_entry DOUBLE PRECISION,
    drop_from_high DOUBLE PRECISION,
    drop_from_entry DOUBLE PRECISION,
    drop_from_reference DOUBLE PRECISION,
    tolerance DOUBLE PRECISION,
    basis VARCHAR(20),
    bucket VARCHAR(20),
    applied_rule TEXT,
    should_sell BOOLEAN,
    is_backfill BOOLEAN DEFAULT FALSE
);
```

---

## How to Test

```bash
# 1. Check if trailing stop is running
tail -f /root/follow_the_goat/000trading/logs/sell_trailing_stop.log

# 2. Check if price checks are being created
curl -s "http://127.0.0.1:5051/price_checks?buyin_id=55712&hours=all&limit=10"

# 3. Test connection pool directly
cd /root/follow_the_goat && python3 test_connection_pool.py

# 4. Check running processes
ps aux | grep "master2.py" | grep -v grep

# 5. Test the trade page
curl -s "http://195.201.84.5/goats/unique/trade/?id=55705&play_id=46" | grep "Trade #55705"
```

---

## Likely Root Cause (Hypothesis)

Based on the evidence, the most likely cause is:

**APScheduler is creating multiple job instances or running in a way that reuses cursors across executions**

The fact that:
- Standalone scripts work ✅
- website_api endpoints work ✅  
- Only APScheduler-scheduled `sell_trailing_stop` fails ❌

...suggests the issue is in how APScheduler interacts with the connection pool, NOT in the connection pool itself.

**Recommended Fix**: 
1. Check `scheduler/master2.py` executor configuration
2. Ensure `trailing_stop_seller` job uses `executor='realtime'` (single-threaded)
3. Consider creating a fresh connection in `get_current_price()` instead of using pool
4. Or implement a singleton TrailingStopSeller instance that's reused

---

## Additional Context

- System: PostgreSQL on Ubuntu
- Python: 3.12
- psycopg2: Using RealDictCursor
- APScheduler: version unknown (check requirements.txt)
- Connection pool: psycopg2.pool.SimpleConnectionPool (1-10 connections)
- Services: master.py (data), master2.py (trading), website_api.py (API)
- Trade page URL: http://195.201.84.5/goats/unique/trade/?id=55705&play_id=46

Good luck! 🚀

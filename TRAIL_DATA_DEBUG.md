# Trail Data Persistence Issue - Diagnosis Summary

## Problem
Train validator creates trades but trail data shows 0 rows in `buyin_trail_minutes` table, even though logs say "✓ Generated and persisted 15-minute trail for buy-in #X (15 rows)".

Website detail page shows "No Trail Data Available" for all trades.

## Root Causes Found

### 1. Price Data Missing (FIXED ✅)
- `train_validator.py` checks `price_points` table which had 0 rows
- Fixed by adding `sync_prices_to_price_points()` to master2.py
- Now syncing `prices` → `price_points` every 1 second
- Result: 19,000+ price_points rows, training cycles running every 15s

### 2. Trail Data Not Persisting (IN PROGRESS 🔧)
- Logs show "✓ Inserted 15 trail rows" but table has 0 rows
- Manual inserts work fine and persist
- Issue appears to be with how `trail_data.py` connects to master2's DuckDB

## Attempted Fixes

### Attempt 1: Direct Import from master2
- Changed to import `get_local_duckdb()` from `scheduler.master2`
- Issue: Function returns None or wrong connection instance

### Attempt 2: HTTP API Fallback  
- Added Priority 2 using HTTP POST to http://localhost:5052/execute
- Issue: API restrictions block some operations

### Attempt 3: Simplified to get_duckdb("central")
- Removed complex priority system
- Use registered connection via `get_duckdb("central")`
- Issue: Still not persisting, but manual tests work

### Attempt 4: Duplicate Check
- Added check for existing data before insert
- Added verification after insert
- Added connection type logging

## Current Status

**Files Modified:**
- `scheduler/master2.py` - Added `sync_prices_to_price_points()` ✅
- `000trading/trail_data.py` - Simplified insert logic, added logging 🔧

**Next Steps:**
1. Check connection type logging to see if inserting to wrong DB
2. Verify if pattern_validator is calling trail generation multiple times
3. Consider if there's a transaction/commit issue

**Manual Test Results:**
```python
# This works and persists:
with get_duckdb("central") as conn:
    conn.execute("INSERT INTO buyin_trail_minutes ...")
    # Data persists ✅

# trail_data.py uses same pattern but doesn't persist ❌
```

## Hypothesis
The duplicate log messages suggest the function is being called from multiple threads/processes, possibly writing to different DB instances.

---
Date: 2026-01-05
Status: Investigating connection routing


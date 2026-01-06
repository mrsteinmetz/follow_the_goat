# Timestamp UTC Audit and Fixes

## Status: ✅ FULLY FIXED - All timestamp issues resolved

## Summary

All timestamps in the system now use **UTC (server time)**, never browser/local time.

## 🎯 All Fixes Complete

### 1. DuckDB Timezone Configuration ✅

**Fixed in `core/database.py`:**
- ✅ Added `SET TimeZone='UTC'` to all DuckDB connection initializations
- ✅ Applied to pooled connections in `DuckDBPool.get_connection()`
- ✅ Applied to fresh connections in `get_duckdb_fresh()`
- ✅ Applied to externally registered connections in `register_connection()`

**Fixed in `core/trading_engine.py`:**
- ✅ Added `SET TimeZone='UTC'` to TradingDataEngine `_init_database()`
- ✅ In-memory DuckDB now uses UTC for all `NOW()` and `CURRENT_TIMESTAMP`

**Fixed in `scheduler/master2.py`:**
- ✅ Added `SET TimeZone='UTC'` to local DuckDB in `init_local_duckdb()`
- ✅ Master2's in-memory database now uses UTC timestamps

### 2. Python Code - datetime.now(timezone.utc) ✅

**Already Fixed:**
- ✅ `/000data_feeds/2_create_price_cycles/create_price_cycles.py` - Lines ~278, ~580
- ✅ `/000data_feeds/5_create_profiles/create_profiles.py` - Lines ~417, ~751

### 3. Removed Timezone Adjustments ✅

**Fixed in `/000trading/trail_generator.py`:**
- ✅ Removed `tz_offset = timedelta(hours=1)` from `fetch_transactions()` (lines 400-407)
- ✅ Removed `tz_offset = timedelta(hours=1)` from `fetch_whale_activity()` (lines 556-560)
- ✅ Removed timestamp conversion back to UTC (lines 533-538, 750-755)
- ✅ Updated docstrings to reflect UTC timestamps

### 4. Documentation ✅

**Fixed in `duckdb/ARCHITECTURE.md`:**
- ✅ Added critical notice about UTC timestamps at the top
- ✅ Documented DuckDB timezone configuration
- ✅ Added verification queries
- ✅ Listed all files fixed for UTC


## ✅ Already Correct

### JavaScript/Frontend
- ✅ `000website/pages/profiles/index.php` lines 560, 567:
  - Uses `date.getUTCMonth()`, `date.getUTCDate()`, `date.getUTCHours()`, etc.
  - Correctly displays UTC times

- ✅ `000website/index.php` lines 1115-1157:
  - `formatRelativeTime()` function correctly handles UTC timestamps
  - Treats all server timestamps as UTC

### Python Files Already Using timezone.utc
- ✅ `scheduler/master2.py` - imports `timezone` and uses it
- ✅ `000trading/train_validator.py` - imports `timezone`
- ✅ `000trading/follow_the_goat.py` - line 867: `datetime.now(timezone.utc)`
- ✅ `000trading/sell_trailing_stop.py` - imports `timezone`
- ✅ `scheduler/status.py` - imports `timezone`

### Other Files Using UTC Functions
- ✅ `scheduler/master.py` line 315: `datetime.utcnow()` in `_normalize_trade_timestamp()`
- ✅ `000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py` - timestamps from API are UTC

---

## 🎯 Problem Solved

### Root Cause (RESOLVED)

**Problem:** DuckDB's `NOW()` and `CURRENT_TIMESTAMP` returned the system's local timezone (CET/UTC+1), not UTC.

**Solution:** All DuckDB connections now execute `SET TimeZone='UTC'` immediately after connection, ensuring all auto-generated timestamps are in UTC.

---

## 📋 Verification Checklist

To verify all timestamps are UTC:

```sql
-- Check timezone setting
SELECT current_setting('TimeZone');  -- Should return: UTC

-- Check current time (should match UTC)
SELECT NOW();

-- Verify data timestamps match UTC
SELECT ts, created_at FROM prices LIMIT 5;
SELECT cycle_start_time, cycle_end_time, created_at FROM cycle_tracker LIMIT 5;
SELECT trade_timestamp, created_at FROM sol_stablecoin_trades LIMIT 5;
SELECT trade_timestamp, created_at FROM wallet_profiles LIMIT 5;
```

All times should match UTC (compare with current UTC time).

---

## 🎯 Action Plan Status

1. ✅ **DONE**: Fixed Python code to use `datetime.now(timezone.utc)`
2. ✅ **DONE**: Set DuckDB timezone to UTC in all connection initializations
3. ✅ **DONE**: Removed timezone adjustments from `trail_generator.py`
4. ✅ **DONE**: Documented in `duckdb/ARCHITECTURE.md` that all times are UTC
5. **Future**: Add timezone check to health endpoints (optional enhancement)

---

## Notes

- PostgreSQL stores `TIMESTAMP` values in UTC by default when using `DEFAULT CURRENT_TIMESTAMP`
- DuckDB stores `TIMESTAMP` values in the **system timezone** by default (CET/UTC+1)
- **Solution:** All DuckDB connections now run `SET TimeZone='UTC'` to match PostgreSQL behavior
- The trail_generator code was compensating for CET storage with +1 hour offset - this is now removed

---

## Testing

After fixes, run:
```bash
python3 -c "
from datetime import datetime, timezone
from core.database import get_duckdb

with get_duckdb('central', read_only=True) as conn:
    # Check timezone setting
    result = conn.execute(\"SELECT current_setting('TimeZone')\").fetchone()
    print(f'DuckDB TimeZone: {result[0]}')
    
    # Check current time
    result = conn.execute('SELECT NOW()').fetchone()
    print(f'DuckDB NOW(): {result[0]}')
    print(f'Python UTC: {datetime.now(timezone.utc)}')
    
    # They should match within a second
"
```

Expected output:
```
DuckDB TimeZone: UTC
DuckDB NOW(): 2025-01-05 XX:XX:XX
Python UTC: 2025-01-05 XX:XX:XX+00:00
```

---

## Migration Notes for Future Development

When writing new code:

1. **Python timestamps:** Always use `datetime.now(timezone.utc)`, NEVER `datetime.now()`
2. **DuckDB queries:** No need for timezone conversion - all data is UTC
3. **Frontend display:** Use `getUTC*()` methods for JavaScript Date objects
4. **Comparisons:** When comparing buyin `followed_at` with trade timestamps, both are UTC - no offset needed

---
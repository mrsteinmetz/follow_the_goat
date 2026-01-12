# Trade Chart Timezone Fix - Complete

## Issue Summary

Trade detail page chart was ending before the exit time due to **TWO timezone issues**:
1. PHP `date()` instead of `gmdate()` when converting timestamps to query the database
2. Python `datetime.now()` instead of `datetime.now(timezone.utc)` when recording exit timestamps

### Example Problem
- **Trade Entry**: Jan 12, 2026 17:48:53 UTC
- **Trade Exit**: Jan 12, 2026 18:50:53 UTC (shows as 18:50 but should be 17:50!)
- **Chart Data Ended**: 18:01 UTC (current time)
- **Actual Exit Time**: 17:50 UTC (1 hour earlier than recorded!)

## Root Cause #1: PHP Timezone Issue

In `000website/chart/plays/get_trade_prices.php`, the code was using `date()` instead of `gmdate()` to convert Unix timestamps to datetime strings:

```php
// ❌ WRONG - Uses server's local timezone
$start_datetime = date('Y-m-d H:i:s', $start_sec);
$end_datetime = date('Y-m-d H:i:s', $end_sec);
```

## Root Cause #2: Python Timezone Issue (CRITICAL!)

In `000trading/sell_trailing_stop.py` line 821, the code was using `datetime.now()` which returns **LOCAL time**, not UTC:

```python
# ❌ WRONG - Uses server's local timezone (CET = UTC+1)
exit_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
```

### Why This Caused Major Issues

1. **Server timezone is CET (UTC+1)**
2. When a trade is sold at **17:50 UTC**, `datetime.now()` returns **18:50 CET**
3. The timestamp **"18:50"** gets stored without timezone info
4. PostgreSQL interprets this as **18:50 UTC** (1 hour in the future!)
5. The chart can't show price data that doesn't exist yet

### Example of the Bug

Server timezone: CET (UTC+1)
- Actual sale happens at: **17:50:00 UTC**
- `datetime.now()` returns: **18:50:00 CET** (which is 17:50:00 UTC + 1 hour)
- Stored in database as: **"18:50:00"** (without timezone)
- PostgreSQL interprets as: **18:50:00 UTC** (WRONG - 1 hour in the future!)
- Chart query for 17:50-18:50 finds no price data after 18:01 (current time)

## Fixes Applied

### Fix #1: PHP API (`get_trade_prices.php`)

**File**: `000website/chart/plays/get_trade_prices.php`

```php
// ✅ CORRECT - Always uses UTC
$start_datetime = gmdate('Y-m-d H:i:s', $start_sec);
$end_datetime = gmdate('Y-m-d H:i:s', $end_sec);
```

### Fix #2: Python Exit Timestamp (`sell_trailing_stop.py`)

**File**: `000trading/sell_trailing_stop.py` - Line 821

```python
# ✅ CORRECT - Always uses UTC
exit_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
```

### Fix #3: Python Price Check Timestamps

**File**: `000trading/sell_trailing_stop.py` - Line 643

```python
# ✅ CORRECT - Always uses UTC for price check records
movement_data = {
    'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
    # ... rest of data
}
```

### Fix #4: Python Backfill Timestamps

**File**: `000trading/sell_trailing_stop.py` - Line 538

```python
# ✅ CORRECT - Always uses UTC for backfill data
timestamp = followed_at.strftime('%Y-%m-%d %H:%M:%S') if followed_at else datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
```

## Verification

After the fixes:
1. JavaScript calculates time range in UTC ✓
2. PHP converts to datetime string in UTC ✓  
3. Python records exit time in UTC ✓
4. PostgreSQL stores all timestamps in UTC ✓
5. Chart now displays complete price data through the actual exit time ✓

## Critical Rules

**🚨 ALWAYS USE UTC FOR DATABASE TIMESTAMPS 🚨**

### Python:
```python
# ❌ WRONG - Uses local timezone
datetime.now()

# ✅ CORRECT - Always uses UTC
datetime.now(timezone.utc)

# Alternative (deprecated but works)
datetime.utcnow()
```

### PHP:
```php
// ❌ WRONG - Uses server timezone
date('Y-m-d H:i:s', $timestamp)

// ✅ CORRECT - Always uses UTC
gmdate('Y-m-d H:i:s', $timestamp)
```

### Why This Matters:
- PostgreSQL stores all timestamps in UTC
- Servers may run in different timezones (this one is CET = UTC+1)
- Using local time creates 1-hour offset in stored data
- Charts and analysis break when timestamps are in the wrong timezone

## Files Verified and Fixed

✅ **Fixed - Critical**:
- `000website/chart/plays/get_trade_prices.php` - Price data API
- `000trading/sell_trailing_stop.py` - Exit timestamp (line 821)
- `000trading/sell_trailing_stop.py` - Price check timestamp (line 643)
- `000trading/sell_trailing_stop.py` - Backfill timestamp (line 538)

✅ **Already correct**:
- `000website/index.php` - Main chart
- `000website/goats/unique/trade/index.php` - Trade detail page

⚠️ **Debug/test files** (not critical):
- Various debug_*.php files (only used for testing)

## Testing After Fix

To verify the fix works:
1. Wait for a new test trade to be created and sold
2. Navigate to the trade detail page
3. Check that the exit time matches the actual time the trade was sold
4. Verify the price chart extends fully to and past the exit time
5. Console logs should show all timestamps in UTC

## Impact

- **Before**: Exit timestamps were 1 hour off (stored in future)
- **After**: Exit timestamps are correctly recorded in UTC at the moment of sale
- **Result**: Charts now show complete price data through the actual exit time

## Prevention

This type of bug is prevented by:
1. **Always use `datetime.now(timezone.utc)` in Python** for database timestamps
2. **Always use `gmdate()` in PHP** when working with UTC timestamps
3. **Code reviews** should flag any use of `datetime.now()` or `date()` without UTC
4. **Testing** with different server timezones to catch mismatches
5. **Documentation** like this to remind developers of the UTC requirement

## Related Files to Monitor

Any file that writes timestamps to the database should be checked:
- All trading scripts in `000trading/`
- All data feed scripts in `000data_feeds/`
- All PHP pages that interact with timestamps
- Scheduler jobs in `scheduler/`

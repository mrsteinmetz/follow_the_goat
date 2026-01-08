# Cycle Retention Fix - January 2026

## Problem Summary

The `update_potential_gains.py` script was unable to calculate potential gains for trades because the referenced price cycles were being deleted before the trades completed their lifecycle.

### Root Cause

1. **Price cycles created in master.py** (TradingDataEngine) with 24-hour retention
2. **Trades can be active for up to 72 hours** (trades_hot_storage_hours setting)
3. **Cleanup mismatch:** Cycles deleted after 24h, but trades still referenced them
4. **Result:** Orphaned `price_cycle` references → potential_gains could not be calculated

### Example Timeline (Before Fix)

```
10:00 AM - Trade created with price_cycle=100 (cycle is active)
10:30 AM - Cycle closes (cycle_end_time is set)
Next day 10:30 AM - Cycle deleted from DuckDB (24h after cycle_end_time)
Next day 11:00 AM - Trade still pending/active
Next day 12:00 PM - update_potential_gains.py tries to calculate gains
                    → cycle_id 100 not found → potential_gains stays NULL
```

## Solution

Extended cycle retention from **24 hours to 72 hours** to match trade lifetime.

### Changes Made

#### 1. **core/database.py** - `archive_old_data()`
- Added special handling for `cycle_tracker` table
- Override retention to 72 hours (regardless of `hours` parameter)
- Updated log message to indicate 72h retention for cycles

```python
if table_name == "cycle_tracker":
    cycle_hours = 72  # Match trades retention
    # ... cleanup logic using cycle_hours instead of hours
```

#### 2. **core/trading_engine.py** - `_cleanup_old_data()`
- Updated TradingDataEngine cleanup to use 72h cutoff for cycles
- Added separate cutoff calculation for cycle_tracker

```python
if table == "cycle_tracker":
    cycle_cutoff = datetime.now() - timedelta(hours=72)
    # ... delete using cycle_cutoff
```

#### 3. **000trading/follow_the_goat.py** - Trade Creation
- Initialize `higest_price_reached` field with `our_entry_price` at trade creation
- Provides fallback data if cycle gets archived before trade is sold
- Updated both TradingDataEngine write and DuckDB insert paths

#### 4. **000trading/train_validator.py** - Synthetic Trade Creation
- Initialize `higest_price_reached` for test trades
- Ensures consistency with production trade creation

#### 5. **000data_feeds/6_update_potential_gains/update_potential_gains.py**
- Enhanced diagnostic logging to detect orphaned cycles
- Added warning when orphaned references are found
- Added tracking of `higest_price_reached` population
- Added oldest cycle timestamp to diagnostics

#### 6. **.cursorrules** - Documentation Update
- Documented critical requirement: cycles MUST have 72h retention
- Explained why cycles must outlive trades
- Removed outdated "24-hour cleanup never affects active cycles" statement

## Why 72 Hours?

Trades have a 72-hour hot storage retention period (`trades_hot_storage_hours`). During this time:
- Trade might be pending (waiting for price movement)
- Trade might be sold (our_status = 'sold')
- `update_potential_gains.py` needs the cycle's `highest_price_reached` to calculate gains

**Price cycles themselves close quickly** (typically within 2 hours), but their **data must persist for 72 hours** to serve trades throughout their lifecycle.

## Fallback Mechanism

The system has TWO sources for calculating potential gains:

### Primary: Cycle's highest_price_reached
```sql
SELECT ((ct.highest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100
FROM follow_the_goat_buyins buyins
INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
WHERE ct.cycle_end_time IS NOT NULL
```

### Fallback: Trade's own higest_price_reached
```sql
SELECT ((buyins.higest_price_reached - buyins.our_entry_price) / buyins.our_entry_price) * 100
FROM follow_the_goat_buyins buyins
WHERE buyins.higest_price_reached IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cycle_tracker WHERE id = buyins.price_cycle)
```

This fallback is now more reliable because:
1. `higest_price_reached` is initialized with `our_entry_price` at trade creation
2. `trailing_stop_seller.py` updates it as price increases
3. Even if cycle is archived, we can still calculate gains

## Testing Verification

To verify the fix is working:

1. **Check cycle retention:**
```sql
SELECT MIN(cycle_end_time) as oldest_completed
FROM cycle_tracker 
WHERE cycle_end_time IS NOT NULL;
```
Should show cycles up to 72 hours old.

2. **Check orphaned references:**
```sql
SELECT COUNT(*) FROM follow_the_goat_buyins
WHERE price_cycle IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cycle_tracker WHERE id = price_cycle);
```
Should be 0 (or very low if trades are > 72h old).

3. **Check potential_gains calculation:**
```sql
SELECT COUNT(*) FROM follow_the_goat_buyins
WHERE potential_gains IS NULL
  AND our_status IN ('sold', 'no_go')
  AND our_entry_price > 0;
```
Should be 0 (all completed trades should have gains calculated).

4. **Monitor logs:**
```bash
# Check for orphaned warnings in update_potential_gains logs
grep "orphaned" logs/scheduler_errors.log
```

## Architecture Alignment

This fix aligns with the dual-master architecture:

- **master.py (Data Engine):** Creates cycles, retains for 72h
- **master2.py (Trading Logic):** Syncs cycles from master.py, uses them for trade decisions
- **Sync mechanism:** Cycles sync every 1s via HTTP API with `INSERT OR REPLACE` (handles updates)
- **Website:** Queries master2.py's local DuckDB for cycle data

All parts of the system now use consistent 72-hour retention for cycle data.

## Monitoring

Watch for these indicators:

✅ **Good signs:**
- No orphaned cycle warnings in logs
- All completed trades have potential_gains calculated
- Oldest cycle in database is ~72 hours old

⚠️ **Warning signs:**
- Orphaned cycle warnings appearing regularly
- Many completed trades with NULL potential_gains
- Cycles disappearing after 24 hours (indicates cleanup not working)

## Files Modified

1. `core/database.py` - Extended cycle retention in `archive_old_data()`
2. `core/trading_engine.py` - Extended cycle retention in `_cleanup_old_data()`
3. `000trading/follow_the_goat.py` - Initialize `higest_price_reached` at trade creation
4. `000trading/train_validator.py` - Initialize `higest_price_reached` for test trades
5. `000data_feeds/6_update_potential_gains/update_potential_gains.py` - Enhanced diagnostics
6. `.cursorrules` - Updated documentation

## Migration Notes

**No database migration required** - the schema already has the necessary fields:
- `cycle_tracker.cycle_end_time` (cleanup uses this + 72h)
- `follow_the_goat_buyins.higest_price_reached` (fallback mechanism)
- `follow_the_goat_buyins.potential_gains` (calculated field)

The fix takes effect immediately when master.py and master2.py are restarted.


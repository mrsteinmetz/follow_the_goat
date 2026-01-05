# Train Validator Fix - Price Data Issue

## Problem Diagnosed

**Issue**: Train validator was skipping every cycle with message "Waiting for price data (0/10 points)"

**Root Cause**: 
- Master2.py's in-memory DuckDB has 12,000+ rows in `prices` table (token='SOL') ✅
- But `train_validator.py` checks `price_points` table (coin_id=5) which had 0 rows ❌
- Legacy modules like `create_price_cycles.py` and `train_validator.py` expect data in `price_points` format

## Solution Implemented

Added automatic sync of `prices` → `price_points` in master2.py:

### 1. Real-time Sync (every 1 second)
Added `sync_prices_to_price_points()` function that:
- Runs after each `sync_from_engine()` call
- Transforms prices (token='SOL') → price_points (coin_id=5)
- Uses incremental sync to avoid duplicates

### 2. Startup Backfill
Added Phase 4 to `backfill_from_data_engine()`:
- Populates price_points from all existing prices on startup
- Ensures historical data is available immediately

## Files Modified

- `scheduler/master2.py`:
  - Added `sync_prices_to_price_points()` function (after line 2767)
  - Modified `run_sync_from_engine()` to call price_points sync (line 2936)
  - Added Phase 4 to backfill process (line 2693)

## To Apply Fix

Restart master2.py:
```bash
# Find PID
ps aux | grep "python scheduler/master2.py" | grep -v grep

# Kill gracefully
kill -TERM <PID>

# Restart
cd /root/follow_the_goat
nohup /root/follow_the_goat/venv/bin/python scheduler/master2.py > logs/master2.log 2>&1 &
```

## Expected Result

After restart:
- `price_points` table will have 12,000+ rows (same as `prices`)
- Train validator will create trades every 15 seconds
- Website at http://195.201.84.5/pages/features/trades/ will show new trades

## Verification

Check that price_points is populated:
```bash
curl -s http://localhost:5052/tables | python3 -m json.tool | grep price_points
```

Should show `"price_points": 12000+` (not 0)

Monitor train_validator logs:
```bash
tail -f /root/follow_the_goat/000trading/logs/train_validator.log
```

Should see "✓ Training #<ID>: GO @ $135.xx" messages every 15 seconds.

---

**Date**: 2026-01-05
**Status**: Fix implemented, awaiting master2.py restart


# Master2 Sync Fix - Unlimited Backfill

## Problem Identified

Master2's in-memory DuckDB had only **~25,000 trades** while master.py had **~377,000 trades**.

The root cause: **Backfill limits were too restrictive**

## Changes Made

### 1. `core/data_client.py`
- Changed `get_backfill()` limit parameter default from `10000` to `None`
- `None` means **no limit** - get ALL data in the time window

### 2. `core/data_api.py` (master.py's API)
- Changed `/backfill/{table}` endpoint limit parameter:
  - OLD: `default=10000, le=100000` (max 100k)
  - NEW: `default=None` (no limit)
- Updated SQL queries to handle `None` limit (no LIMIT clause when None)

### 3. `scheduler/master2.py`
- Changed all table backfill limits from specific numbers to `None`:
  ```python
  # OLD:
  ("sol_stablecoin_trades", 20000),  # Only 20k trades!
  
  # NEW:
  ("sol_stablecoin_trades", None),  # Get ALL trades in 24h window
  ```

## Expected Result

After restarting both services:
- Master2 will backfill **ALL ~377,000 trades** from master.py (not just 20k)
- Master2 will backfill **ALL data** for every table (no artificial limits)
- Ongoing sync continues to work incrementally (only new records)

## Restart Required

**Both services must be restarted:**

```bash
# 1. Restart master.py (Data Engine - port 5050)
#    This rarely needs restart, but required for API endpoint changes
pkill -f 'scheduler/master.py'
cd /root/follow_the_goat && source venv/bin/activate && \
  nohup python scheduler/master.py > /tmp/master.log 2>&1 &

# 2. Restart master2.py (Trading Logic - port 5052)  
pkill -f 'scheduler/master2.py'
cd /root/follow_the_goat && source venv/bin/activate && \
  nohup python scheduler/master2.py > /tmp/master2.log 2>&1 &

# 3. website_api.py does NOT need restart (no changes)
```

## Verification

After restart, check master2 trade count:

```bash
# Should match master.py's count (~377k)
curl http://127.0.0.1:5052/health | python3 -m json.tool | grep sol_stablecoin_trades

# SQL Tester should also show ~377k
SELECT COUNT(*) FROM sol_stablecoin_trades;
```

## Files Modified

1. `/root/follow_the_goat/core/data_client.py` - Client-side limit removal
2. `/root/follow_the_goat/core/data_api.py` - Server-side limit removal  
3. `/root/follow_the_goat/scheduler/master2.py` - Backfill configuration

## Note

This fix ensures master2 and master.py stay **100% in sync** with the same 24-hour data window, as intended by the architecture.

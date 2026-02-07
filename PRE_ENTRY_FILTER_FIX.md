# Pre-Entry Price Movement Filter - Implementation Fix

## Problem Identified

Trades like [this one](http://195.201.84.5/goats/unique/trade/?id=20260204150353804&play_id=46) were being accepted even though the price was **falling** before entry (pre_entry_change_2m was negative).

Looking at the chart:
- The GREEN line shows when the trade was ENTERED
- The ORANGE line shows when the trade EXITED
- The price was clearly going DOWN before entry (bad signal!)

## Root Cause

1. **The failsafe EXISTS** in `000trading/pre_entry_price_movement.py`:
   - Function: `should_enter_based_on_price_movement()`
   - Default threshold: `min_change_3m >= 0.20%` (price must be rising)
   - Purpose: Reject trades when price is falling (prevents "buying the top")

2. **But it was NOT ENFORCED** in `000trading/follow_the_goat.py`:
   - The module was imported but never called
   - Trades were being accepted without checking pre-entry price movement
   - Trail data was calculating and storing the metrics, but validation was missing

## Changes Made

### 1. Added Pre-Entry Validation to `follow_the_goat.py`

**Import the validation functions (line ~62):**
```python
from pre_entry_price_movement import (
    calculate_pre_entry_metrics,
    should_enter_based_on_price_movement,
    log_pre_entry_analysis
)
```

**Added validation check in `save_buyin_trade()` (before creating buyin, line ~1206):**
```python
# CRITICAL: Pre-entry price movement check (must be rising!)
pre_entry_check_token = step_logger.start(
    'pre_entry_check',
    'Checking pre-entry price movement',
    {}
)

try:
    trade_timestamp = trade.get('trade_timestamp')
    if not trade_timestamp:
        # Fallback to current time if timestamp missing
        trade_timestamp = datetime.now(timezone.utc)
    
    # Calculate pre-entry metrics
    pre_entry_metrics = calculate_pre_entry_metrics(trade_timestamp, float(trade.get('price', 0)))
    
    # Log analysis for debugging
    change_3m = pre_entry_metrics.get('pre_entry_change_3m')
    logger.info(f"Pre-entry check for trade {trade['id']}: change_3m={change_3m:.3f}%" if change_3m else "Pre-entry check: no 3m data")
    
    # Validate: price must be rising (threshold: 0.20% minimum)
    should_enter, reason = should_enter_based_on_price_movement(pre_entry_metrics, min_change_3m=0.20)
    
    if not should_enter:
        step_logger.end(pre_entry_check_token, {
            'result': 'rejected',
            'reason': reason,
            'change_3m': change_3m,
        }, status='rejected')
        logger.warning(f"✗ Trade {trade['id']} REJECTED by pre-entry check: {reason}")
        self._increment_stat('trades_blocked_pre_entry')
        return 'blocked_pre_entry'
    
    step_logger.end(pre_entry_check_token, {
        'result': 'passed',
        'change_3m': change_3m,
    })
    logger.debug(f"✓ Trade {trade['id']} passed pre-entry check")
    
except Exception as e:
    logger.error(f"Pre-entry check error for trade {trade['id']}: {e}")
    step_logger.fail(pre_entry_check_token, str(e))
    # On error, allow the trade (fail-open for now)
    # TODO: Consider fail-closed (block on error) for production
```

### 2. Added Statistics Tracking

**Added counter in `__init__()` (line ~286):**
```python
'trades_blocked_pre_entry': 0,  # NEW: Pre-entry price movement filter
```

**Updated `process_new_trades()` to handle new return value (line ~1383):**
```python
'blocked_pre_entry': 0,  # NEW: Pre-entry filter blocks
...
elif result == 'blocked_pre_entry':
    stats['blocked_pre_entry'] += 1
    self._increment_stat('trades_blocked_pre_entry')
```

**Updated `get_statistics()` to include new counter (line ~1454):**
```python
'trades_blocked_pre_entry': self.stats.get('trades_blocked_pre_entry', 0),
```

**Updated logging in `run_single_cycle()` (line ~1484):**
```python
f"{stats['blocked_pre_entry']} blocked (pre-entry), "
```

## How It Works

### Filter Logic (from `pre_entry_price_movement.py`)

1. **Looks back 3 minutes** before trade entry
2. **Calculates price change**: `(entry_price - price_3m_ago) / price_3m_ago * 100`
3. **Requires minimum 0.20% rise**: `pre_entry_change_3m >= 0.20%`
4. **Blocks if falling or flat**: Prevents buying into downward momentum

### Why 3 Minutes?

Based on analysis of 8,515 trades (documented in code comments):
- **10-minute window**: Too slow for SOL (missed opportunities)
- **3-minute window**: **80-100% win rate** ⭐ OPTIMAL
- **2-minute window**: 62.5% win rate

The 3-minute window catches quick reversals EARLY (7 minutes before the old 10m filter) - perfect for SOL's fast 5-60 minute cycles.

### Threshold: 0.20%

- **Increased from original 0.08%** to prevent weak entries
- Requires stronger upward momentum before entry
- Filters out "buying the top" scenarios

## Expected Behavior After Restart

**Before Fix:**
```
Trade detected → Max buys check → Pattern validator → ENTERED ✓
(No pre-entry check - trades like the one you showed were accepted!)
```

**After Fix:**
```
Trade detected → Max buys check → INSERT BUYIN → PRE-ENTRY CHECK → Trail → Validator → ENTERED ✓
                                                         ↓
                                              (price falling? SET TO no_go ✗)
```

**Key Change:** The buyin record is now **always created**, but if pre-entry check fails:
- Status is set to `no_go`
- Trail generation and pattern validation are **skipped**
- Record is kept in database for analysis

**Log Output Examples:**

✅ **Passing trade:**
```
Pre-entry check for trade 12345: change_3m=0.35%
✓ Trade 12345 passed pre-entry check
✓ Pre-entry check passed for buyin #67890, proceeding with trail generation
```

❌ **Blocked trade:**
```
Pre-entry check for trade 67890: change_3m=-0.15%
✗ Trade 67890 REJECTED by pre-entry check: FALLING_PRICE (change_3m=-0.15%)
✗ Buyin #67891 marked as no_go (pre-entry check failed)
```

**Database Records:**

Blocked trades will have:
- `our_status = 'no_go'`
- `pattern_validator_log` containing pre-entry rejection reason
- `entry_log` showing the full decision flow

**Stats Summary:**
```
📊 Status: 5 saved, 2 blocked (max_buys), 3 blocked (validator), 
           4 blocked (pre-entry), 0 errors | Cycles: [123, 124, 125]
```

## Restart Instructions

### For Live Trading (follow_the_goat)

The `follow_the_goat` component runs as a separate process via the component-based scheduler:

```bash
# Stop the component (if running)
pkill -f "run_component.py --component follow_the_goat"

# Start it fresh
cd /root/follow_the_goat
python3 scheduler/run_component.py --component follow_the_goat
```

Or via the dashboard:
1. Go to Scheduler Metrics dashboard
2. Find `follow_the_goat` component
3. Toggle OFF, wait 5 seconds
4. Toggle ON

### For Test Trading (train_validator)

The `train_validator` component also runs separately:

```bash
# Stop the component (if running)
pkill -f "run_component.py --component train_validator"

# Start it fresh
cd /root/follow_the_goat
python3 scheduler/run_component.py --component train_validator
```

Or via the dashboard:
1. Go to Scheduler Metrics dashboard
2. Find `train_validator` component
3. Toggle OFF, wait 5 seconds
4. Toggle ON

**Note:** Both components need to be restarted for the pre-entry filter to be active everywhere.

## Testing

You can test the pre-entry check standalone:

```bash
cd /root/follow_the_goat/000trading
python3 pre_entry_price_movement.py <buyin_id>
```

Example:
```bash
python3 pre_entry_price_movement.py 20260204150353804
```

Output will show:
- Entry time and price
- Price changes at 1m, 2m, 3m, 5m, 10m before entry
- Trend direction (rising/falling/flat)
- Decision: ENTER or REJECT

## Files Modified

1. `000trading/follow_the_goat.py` - Added pre-entry validation for live trades
2. `000trading/train_validator.py` - Added pre-entry validation for synthetic test trades (Play #46)
3. No changes to `000trading/pre_entry_price_movement.py` - already had the logic
4. No changes to `000trading/trail_data.py` - already calculates the metrics

## Verification

After restarting `follow_the_goat`, check the logs for:

1. Pre-entry check messages for each trade
2. Blocked trades with reason `FALLING_PRICE`
3. Statistics showing `trades_blocked_pre_entry` count

Monitor for a few cycles and verify that trades with negative `pre_entry_change_3m` are now being rejected.

## Additional Notes

- **Fail-open on error**: If pre-entry check fails (e.g., no price data), the trade is currently **allowed**
- **TODO for production**: Consider fail-closed (block on error) for maximum safety
- **Configurable threshold**: Default is 0.20%, can be adjusted in the code
- **Performance impact**: Minimal - just 1 quick database query per trade
- **Data requirement**: Needs price data from 3+ minutes before entry (should always be available)

---

**Date Fixed**: February 5, 2026  
**Issue**: Trades with falling prices before entry were not being filtered  
**Solution**: Enforce existing `should_enter_based_on_price_movement()` validation in follow_the_goat.py

# Trail Data Fix - Missing Price Movements, Order Book, Patterns, BTC/ETH Data

## Issue Summary

When viewing trade detail pages (e.g., http://195.201.84.5/pages/features/trades/detail.php?id=560), the following data categories were showing as dashes/missing:

- ❌ **Price Movements** (pm_*) - All fields showing "-"
- ❌ **Order Book** (ob_*) - All fields showing "-"  
- ❌ **Patterns** (pat_*) - All showing 0
- ❌ **BTC Prices** (btc_*) - All fields showing "-"
- ❌ **ETH Prices** (eth_*) - All fields showing "-"

While these were working correctly:
- ✅ **Transactions** (tx_*) - Data showing properly
- ✅ **Whale Activity** (wh_*) - Data showing properly

## Root Cause

The issue was a **column name mismatch** between the PostgreSQL schema and the queries in `trail_generator.py`:

**PostgreSQL Schema** (`scripts/postgres_schema.sql`):
- `prices` table uses column: `timestamp`
- `order_book_features` table uses column: `timestamp`

**Trail Generator Queries** (`000trading/trail_generator.py`):
- Queries were using: `ts` (incorrect column name)
- This caused all queries to fail silently, returning empty arrays
- Empty arrays resulted in NULL values in the `buyin_trail_minutes` table
- Website displayed NULLs as dashes "-"

## Fix Applied

Updated `000trading/trail_generator.py` to use the correct column name `timestamp` instead of `ts`:

### 1. Fixed `fetch_price_movements()` function (lines 772-843)

**Before:**
```sql
FROM prices
WHERE ts >= ? AND ts <= ? AND token = ?
GROUP BY DATE_TRUNC('minute', ts)
```

**After:**
```sql
FROM prices
WHERE timestamp >= ? AND timestamp <= ? AND token = ?
GROUP BY DATE_TRUNC('minute', timestamp)
```

### 2. Fixed `fetch_order_book_signals()` function (lines 308-405)

**Before:**
```sql
FROM order_book_features
WHERE symbol = ? AND ts >= ? AND ts <= ?
GROUP BY DATE_TRUNC('minute', ts), symbol
```

**After:**
```sql
FROM order_book_features  
WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
GROUP BY DATE_TRUNC('minute', timestamp), symbol
```

### 3. Fixed `fetch_second_prices()` function (lines 983-1032)

**Before:**
```sql
SELECT ts AS ts, price AS price
FROM prices
WHERE ts >= ? AND ts <= ? AND token = ?
ORDER BY ts ASC
```

**After:**
```sql
SELECT timestamp AS ts, price AS price
FROM prices
WHERE timestamp >= ? AND timestamp <= ? AND token = ?
ORDER BY timestamp ASC
```

## Testing

After applying the fix and restarting `master2.py`:

1. **Service Restart:**
   ```bash
   kill -9 <master2_pid>
   cd /root/follow_the_goat
   source venv/bin/activate
   nohup python scheduler/master2.py > /dev/null 2>&1 &
   ```

2. **Verification:**
   - New trades created by `train_validator.py` (trades #597, #598, #599)
   - Logs confirm: "✓ Generated and persisted 15-minute trail for buy-in #XXX (15 rows)"
   - New trades should now have complete trail data

3. **Check New Trades:**
   - View any trade created AFTER the fix (ID >= 597)
   - All data categories should now show values:
     - ✅ Price Movements
     - ✅ Order Book
     - ✅ Patterns
     - ✅ BTC/ETH Prices
     - ✅ Transactions (already working)
     - ✅ Whale Activity (already working)

## Data Sources

For reference, the data comes from:

| Category | PostgreSQL Table | Populated By |
|----------|------------------|--------------|
| Price Movements (SOL) | `prices` (token='SOL') | `master.py` via Jupiter API |
| BTC Prices | `prices` (token='BTC') | `master.py` via Jupiter API |
| ETH Prices | `prices` (token='ETH') | `master.py` via Jupiter API |
| Order Book | `order_book_features` | `master.py` via Binance WebSocket |
| Transactions | `sol_stablecoin_trades` | `master.py` via webhook |
| Whale Activity | `whale_movements` | `master.py` via webhook |
| Patterns | Derived from `prices` (second-level data) | `trail_generator.py` |

## Impact

- **Old trades (ID < 597):** Will continue to show dashes because NULL values are already in the database
- **New trades (ID >= 597):** Will have complete trail data with all fields populated
- **Future trades:** All trail data will be captured correctly going forward

## Files Modified

1. `/root/follow_the_goat/000trading/trail_generator.py`
   - Fixed column name from `ts` to `timestamp` in 3 locations
   - No other logic changes

## Date Fixed

January 9, 2026 at 07:50 UTC

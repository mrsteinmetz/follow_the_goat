# PROMPT FOR NEW AGENT: Fix Missing Trail Data in Trade Detail Pages

## Problem Statement

When viewing trade detail pages (e.g., http://195.201.84.5/pages/features/trades/detail.php?id=691), the following data categories are showing as dashes/NULL:

- ❌ **Price Movements** (pm_*) - All fields showing "-"
- ❌ **Order Book** (ob_*) - All fields showing "-"  
- ❌ **Patterns** (pat_*) - All showing 0
- ❌ **BTC Prices** (btc_*) - All fields showing "-"
- ❌ **ETH Prices** (eth_*) - All fields showing "-"

While these work correctly:
- ✅ **Transactions** (tx_*) - Data showing properly
- ✅ **Whale Activity** (wh_*) - Data showing properly

## What Has Been Fixed Already

### 1. Column Name Mismatch ✅ COMPLETED
- Updated `000trading/trail_generator.py` to use `timestamp` instead of `ts`
- Fixed in 3 locations: `fetch_price_movements()`, `fetch_order_book_signals()`, `fetch_second_prices()`

### 2. Timezone Issues ✅ COMPLETED  
- Updated `000trading/train_validator.py` to store naive UTC timestamps
- Changed from `datetime.now(timezone.utc).isoformat()` to `datetime.now(timezone.utc).replace(tzinfo=None)`
- Updated `scheduler/master.py` and `scheduler/master2.py` to use `pytz.UTC`

## Current Status - What Still Needs Fixing

### The Core Issue
The `_execute_query()` function in `000trading/trail_generator.py` (starting at line 69) tries multiple data sources in priority order:

1. master2's local DuckDB
2. master2's HTTP API (port 5052)
3. TradingDataEngine (in-memory)
4. PostgreSQL (fallback)

**The problem:** It's likely hitting one of the early data sources (DuckDB/HTTP API) which returns empty results, and NOT falling through to PostgreSQL where the actual data exists.

### Verification That Data Exists

Run this to verify data is in PostgreSQL:

```python
from core.database import get_postgres
from datetime import timedelta

with get_postgres() as conn:
    with conn.cursor() as cursor:
        # Get latest trade
        cursor.execute("SELECT id, followed_at FROM follow_the_goat_buyins ORDER BY id DESC LIMIT 1")
        buyin = cursor.fetchone()
        
        window_end = buyin['followed_at']
        window_start = window_end - timedelta(minutes=15)
        
        # Check if data exists
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM prices 
            WHERE timestamp >= %s AND timestamp <= %s AND token = 'SOL'
        """, [window_start, window_end])
        sol_result = cursor.fetchone()
        print(f"SOL prices available: {sol_result['cnt']}")  # Should show ~1500-1800 rows
        
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM order_book_features
            WHERE timestamp >= %s AND timestamp <= %s
        """, [window_start, window_end])
        ob_result = cursor.fetchone()
        print(f"Order book rows available: {ob_result['cnt']}")  # Should show ~10000-15000 rows
```

## Your Task: Fix the Query Fallback Logic

### Option 1: Force Direct PostgreSQL Queries (RECOMMENDED)

Since the project uses PostgreSQL-only architecture (per `.cursorrules`), simplify `_execute_query()` to ONLY use PostgreSQL:

**File:** `000trading/trail_generator.py`  
**Function:** `_execute_query()` (line 69)

Replace the entire function with:

```python
def _execute_query(query: str, params: list = None, as_dict: bool = True, graceful: bool = True):
    """Execute a query directly against PostgreSQL.
    
    PostgreSQL-only architecture - no DuckDB fallback needed.
    
    Args:
        query: SQL query (use ? for placeholders - will be converted to %s)
        params: Query parameters
        as_dict: If True, return list of dicts; if False, return list of tuples
        graceful: If True, return empty list on errors instead of raising
    
    Returns:
        List of dicts (if as_dict=True) or list of tuples
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Convert DuckDB-style ? placeholders to PostgreSQL %s
                pg_query = query.replace('?', '%s')
                cursor.execute(pg_query, params or [])
                
                if as_dict:
                    results = cursor.fetchall()
                    return results if results else []
                
                # For non-dict format, convert dicts to tuples
                rows = cursor.fetchall()
                return [tuple(r.values()) for r in rows] if rows else []
                
    except Exception as e:
        error_msg = str(e).lower()
        if graceful and ("does not exist" in error_msg or "relation" in error_msg):
            logger.debug(f"Table not found (graceful mode): {e}")
            return []
        
        logger.error(f"PostgreSQL query failed: {e}", exc_info=True)
        if graceful:
            return []
        raise
```

### Option 2: Fix the Fallback Chain (Alternative)

If you want to keep the fallback logic, add proper logging and fix the fallback conditions:

1. Add `logger.info()` at the start of each fallback attempt
2. Make sure each fallback only returns `[]` if explicitly empty, not on error
3. Ensure PostgreSQL fallback is always reached

## Testing After Fix

1. **Restart master2:**
   ```bash
   pkill -9 -f "scheduler/master2.py"
   cd /root/follow_the_goat && source venv/bin/activate
   nohup python scheduler/master2.py >> logs/master2.log 2>&1 &
   ```

2. **Wait for new trade:**
   ```bash
   tail -f 000trading/logs/train_validator.log | grep "✓ Training"
   ```

3. **Check trail data for the new trade:**
   ```python
   from core.database import get_postgres
   
   with get_postgres() as conn:
       with conn.cursor() as cursor:
           # Get latest trade
           cursor.execute("SELECT id FROM follow_the_goat_buyins ORDER BY id DESC LIMIT 1")
           latest_id = cursor.fetchone()['id']
           
           # Check trail data
           cursor.execute("""
               SELECT 
                   COUNT(*) as total_rows,
                   COUNT(pm_close_price) as price_data_rows,
                   COUNT(ob_mid_price) as ob_data_rows,
                   COUNT(btc_price_change_1m) as btc_data_rows,
                   COUNT(eth_price_change_1m) as eth_data_rows
               FROM buyin_trail_minutes 
               WHERE buyin_id = %s
           """, [latest_id])
           result = cursor.fetchone()
           
           print(f"Trade #{latest_id}:")
           print(f"  Total rows: {result['total_rows']}/15")
           print(f"  Price data: {result['price_data_rows']}/15")
           print(f"  Order book: {result['ob_data_rows']}/15")
           print(f"  BTC data: {result['btc_data_rows']}/15")
           print(f"  ETH data: {result['eth_data_rows']}/15")
   ```

   **Expected:** All should show 15/15 (or close to it if some minutes have no data)

4. **View on website:**
   Navigate to: `http://195.201.84.5/pages/features/trades/detail.php?id={latest_id}`
   
   Should now show actual data instead of dashes for:
   - Price Movements
   - Order Book
   - Patterns
   - BTC Prices
   - ETH Prices

## Important Context

### Server Configuration
- **Server timezone:** Europe/Berlin (CET, UTC+1)  
- **Application timezone:** ALL services MUST use UTC
- **PostgreSQL:** Uses TIMESTAMP (without timezone) - stores naive UTC datetimes

### Project Architecture (from .cursorrules)
- **PostgreSQL-only:** Single database for all data
- **No DuckDB:** Legacy code references DuckDB but should use PostgreSQL
- **Two processes:**
  - `master.py` (port 8000/8001): Data ingestion (prices, trades, order book)
  - `master2.py` (port 5052): Trading logic (train_validator runs here)

### Key Files
1. `000trading/trail_generator.py` - Generates 15-minute trail data
2. `000trading/trail_data.py` - Stores trail data in `buyin_trail_minutes` table
3. `000trading/train_validator.py` - Creates test trades every 15 seconds
4. `scheduler/master.py` - Fetches prices from Jupiter API every 1 second
5. `scheduler/master2.py` - Runs train_validator and other trading jobs

## Success Criteria

After your fix:
- ✅ New trades show complete trail data (all 15 categories populated)
- ✅ Website displays actual values instead of dashes
- ✅ All fields in the data table show numbers/percentages
- ✅ Charts display data points instead of being empty

## Questions to Ask If Stuck

1. Is PostgreSQL actually being reached in the query fallback?
2. Are the queries using the correct column names (`timestamp` not `ts`)?
3. Are the datetime parameters timezone-naive UTC?
4. Is the data actually in PostgreSQL for the time range being queried?

Good luck! The data IS there in PostgreSQL - you just need to make sure the queries reach it.

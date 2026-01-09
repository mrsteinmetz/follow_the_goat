# Binance Order Book Display Fix

## Issue
The Binance order book page at http://195.201.84.5/data-streams/binance-order-book/ was showing "No Data" even though:
- Master.py was running and collecting order book data
- PostgreSQL had 291,000+ order book records
- The Binance WebSocket stream was active

## Root Cause
After migrating from DuckDB to PostgreSQL, the `website_api.py` API server had two issues:

1. **Missing Structured Query Support**: The `/query` endpoint only accepted raw SQL queries, but the PHP `DatabaseClient` was sending structured queries with `table`, `columns`, `where`, `order_by` parameters.

2. **Column Name Mismatches**: The PHP was requesting columns that didn't exist in PostgreSQL:
   - `ts` → PostgreSQL has `timestamp`
   - `best_bid` / `best_ask` → Not stored as columns, need to extract from `bids_json` / `asks_json`
   - `relative_spread_bps` → PostgreSQL has `spread_bps`
   - `bid_depth_10` / `ask_depth_10` → PostgreSQL has `bid_liquidity` / `ask_liquidity`
   - `symbol` → Not stored in table (only tracking SOLUSDT)

3. **Incorrect Source Display**: The PHP was always showing "DuckDB API" even when data came from PostgreSQL.

## Solution

### 1. Enhanced `/query` Endpoint (website_api.py)
Added support for both structured queries AND raw SQL queries:

```python
@app.route('/query', methods=['POST'])
def query():
    # Check if raw SQL or structured query
    if 'sql' in data:
        # Handle raw SQL queries
    else:
        # Handle structured queries (table, columns, where, order_by)
```

### 2. Column Name Mapping
Added automatic column mapping for `order_book_features` table:

```python
column_mappings = {
    'ts': 'timestamp',
    'symbol': "'SOLUSDT'",  # Hardcoded constant
    'relative_spread_bps': 'spread_bps',
    'bid_depth_10': 'bid_liquidity',
    'ask_depth_10': 'ask_liquidity',
    'best_bid': "CAST((bids_json::json->0->>0) AS DOUBLE PRECISION)",
    'best_ask': "CAST((asks_json::json->0->>0) AS DOUBLE PRECISION)"
}
```

The mapping:
- Renames columns using SQL aliases
- Extracts `best_bid` and `best_ask` from the first element of JSON arrays
- Provides a constant `'SOLUSDT'` for the symbol field

### 3. Fixed Source Display (binance-order-book/index.php)
Updated the display logic to show "🐘 PostgreSQL" instead of "DuckDB":

```php
switch ($actual_source) {
    case 'postgres':
    case 'postgresql':
        $data_source = "🐘 PostgreSQL";
        break;
    // ... other cases
}
```

## Verification

After the fix:
```bash
curl "http://195.201.84.5/data-streams/binance-order-book/?ajax=refresh"
```

Returns:
- **100 records** of order book data
- **Source: 🐘 PostgreSQL**
- All columns properly mapped and displaying

## Files Modified

1. `/root/follow_the_goat/scheduler/website_api.py`
   - Enhanced `/query` endpoint for structured queries
   - Added column mappings for PostgreSQL compatibility
   - Added `/order_book_features` dedicated endpoint

2. `/root/follow_the_goat/000website/data-streams/binance-order-book/index.php`
   - Fixed data source display logic
   - Now correctly shows "🐘 PostgreSQL"

## Architecture Notes

**Data Flow:**
```
Binance WebSocket (100ms updates)
    ↓
stream_binance_order_book_data.py (in master.py)
    ↓
PostgreSQL (order_book_features table)
    ↓
website_api.py (Flask API on port 5051)
    ↓
PHP Website (port 8000)
    ↓
User Browser
```

**Key Points:**
- Master.py handles ALL data ingestion (runs continuously)
- Master2.py handles trading logic (can restart independently)
- website_api.py serves website data from PostgreSQL
- All processes share the same PostgreSQL database

## Status
✅ **RESOLVED** - Order book data is now displaying correctly on the website with proper PostgreSQL branding.

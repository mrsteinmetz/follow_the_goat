# Trail Data Storage - Dual-Write Architecture

## Overview

Trail data (15-minute analytics for each trade) is automatically stored in **both DuckDB and MySQL** using a dual-write pattern.

## How It Works

### 1. Data Generation
When `generate_trail_payload()` is called with `persist=True` (default):
- Fetches order book, transactions, whale activity, and price data
- Computes derived metrics and pattern detection
- Flattens the data into 15 rows (one per minute)

### 2. Dual-Write Storage
The `insert_trail_data()` function automatically:
- ✓ Writes to **DuckDB** (`buyin_trail_minutes` table) - PRIMARY
- ✓ Writes to **MySQL** (`buyin_trail_minutes` table) - SECONDARY

### 3. Data Structure
Each buyin gets **15 rows** (minutes 0-14) with:
- **Price Movements** (22 columns) - SOL price metrics
- **BTC Price Movements** (6 columns) - BTC correlation data
- **ETH Price Movements** (6 columns) - ETH correlation data
- **Order Book Signals** (22 columns) - Liquidity and spread metrics
- **Transactions** (24 columns) - Trading volume and pressure
- **Whale Activity** (28 columns) - Large wallet movements
- **Pattern Detection** (25 columns) - Chart pattern analysis
- **Second Prices Summary** (9 columns) - 1-second price stats

**Total: ~140 columns per row**

## Database Schema

### DuckDB Table
Defined in: `features/price_api/schema.py` → `SCHEMA_BUYIN_TRAIL_MINUTES`

### MySQL Table
Defined in: `000trading/trail_data.py` → `ensure_trail_table_exists_mysql()`

Both schemas are kept in sync automatically.

## Querying Trail Data

### From PHP (via API)
```php
// Get trail data for a specific buyin
$trail_response = $duckdb->getTrailForBuyin($buyin_id, 'duckdb');  // or 'mysql'
$trail_data = $trail_response['trail_data'] ?? [];
```

### From Python
```python
from trail_data import get_trail_for_buyin

# Query from DuckDB (default)
trail_rows = get_trail_for_buyin(buyin_id=123)

# Returns list of 15 dicts, one per minute
```

### Direct SQL
```sql
-- Get all trail data for a specific buyin
SELECT * FROM buyin_trail_minutes 
WHERE buyin_id = 236587 
ORDER BY minute ASC;

-- Get specific minute
SELECT * FROM buyin_trail_minutes 
WHERE buyin_id = 236587 AND minute = 5;

-- Filter by pattern detection
SELECT buyin_id, minute, pat_breakout_score 
FROM buyin_trail_minutes 
WHERE pat_breakout_score > 0.7 
ORDER BY pat_breakout_score DESC;
```

## Upgrading Existing Tables

If you have an existing `buyin_trail_minutes` table without BTC/ETH columns:

```bash
# Run the migration script
mysql -u username -p database_name < 000trading/add_btc_eth_columns.sql
```

This adds the 12 missing columns for BTC and ETH price movements.

## Re-generating Trail Data

To regenerate trail data for a specific buyin:

```bash
# Regenerate and persist
python 000trading/trail_generator.py 236587 --persist

# Preview without persisting
python 000trading/trail_generator.py 236587
```

## Data Flow

```
generate_trail_payload(buyin_id=123, persist=True)
  ↓
persist_trail(buyin_id, payload)
  ↓
insert_trail_data(buyin_id, payload)
  ↓
flatten_trail_to_rows(buyin_id, payload)  [15 rows created]
  ↓
  ├─→ insert_trail_rows_duckdb(buyin_id, rows)  ✓
  └─→ insert_trail_rows_mysql(buyin_id, rows)   ✓
```

## Persistence Behavior

- **Automatic Cleanup**: Before inserting new trail data, existing rows for that `buyin_id` are deleted
- **Atomic per Database**: Each database write is independent (DuckDB success is required, MySQL is best-effort)
- **Logging**: Success/failure is logged for both databases

## Performance Notes

- **DuckDB**: In-memory or file-based, extremely fast reads
- **MySQL**: Persistent storage, survives process restarts
- **Indexing**: Both tables have indexes on `(buyin_id, minute)` for fast lookups

## Troubleshooting

### Empty Values in MySQL Mode

**Symptom**: When viewing trades with `?source=mysql`, all trail fields show as empty.

**Cause**: Trail data wasn't written to MySQL.

**Solution**: The dual-write is now enabled by default. Regenerate trail data:
```bash
python 000trading/trail_generator.py <buyin_id> --persist
```

### Table Schema Mismatch

**Symptom**: Missing columns or SQL errors when inserting.

**Solution**: Run the migration script to add missing columns:
```bash
mysql -u username -p database_name < 000trading/add_btc_eth_columns.sql
```

## Related Files

- `000trading/trail_generator.py` - Main trail generation logic
- `000trading/trail_data.py` - Database persistence layer
- `features/price_api/schema.py` - DuckDB schema definitions
- `features/price_api/api.py` - REST API endpoints for trail data
- `000website/includes/DuckDBClient.php` - PHP client for trail queries


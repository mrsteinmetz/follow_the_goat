# PostgreSQL Migration Progress

## ✅ COMPLETED

### 1. PostgreSQL Schema (`scripts/postgres_schema.sql`)
- Created complete schema for all tables
- Converted DuckDB syntax to PostgreSQL
- Added indexes for performance
- Ready to run with: `psql -U postgres -d follow_the_goat -f scripts/postgres_schema.sql`

### 2. Core Database Module (`core/database.py`)
- Removed all DuckDB code (DuckDBPool, get_duckdb, register_connection, etc.)
- Added PostgreSQL connection pooling (5-20 connections)
- New helper functions:
  - `get_postgres()` - context manager for connections
  - `postgres_execute()` - run INSERT/UPDATE/DELETE
  - `postgres_query()` - run SELECT queries
  - `postgres_insert()` - insert single record
  - `postgres_insert_many()` - bulk insert
  - `postgres_update()` - update records
  - `cleanup_old_data()` - delete old records
  - `verify_tables_exist()` - check schema

### 3. Master.py Data Engine (`scheduler/master.py`)
- Removed TradingDataEngine (in-memory DuckDB)
- Removed Data API server (port 5050 - no longer needed)
- Updated all data ingestion to write directly to PostgreSQL:
  - Jupiter price fetcher
  - Trade backfill from webhook
  - Binance order book (module needs update)
  - Price cycles (module needs update)
- Simplified startup: just verify PostgreSQL connection
- Simplified shutdown: no engine cleanup needed

## 🚧 IN PROGRESS / TODO

### 4. Master2.py Trading Logic (`scheduler/master2.py`) - CRITICAL
**Status:** Needs major refactoring (lines ~1-4000)

**Remove:**
- Lines 79-850: Local DuckDB infrastructure
  - `_local_duckdb` global
  - `init_local_duckdb()`
  - Write queue system
  - Thread-local cursors
- Lines 1200-1600: Backfill logic
  - `backfill_from_data_engine()`
  - `sync_new_data_from_engine()`
  - All data sync jobs
- Line 62: DataClient import
- Lines 855-1000: Local API server (keep but update to query PostgreSQL)

**Update:**
- All `get_duckdb("central", read_only=True)` → `get_postgres()`
- All `duckdb_execute_write()` → `postgres_execute()`
- All `cursor.execute("...", [?])` → `cursor.execute("...", [%s])`
- Import from `core.database` instead of managing local DB

**Example transformation:**
```python
# OLD
with get_duckdb("central", read_only=True) as cursor:
    result = cursor.execute("SELECT * FROM prices WHERE token = ?", ['SOL']).fetchall()

# NEW
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM prices WHERE token = %s", ['SOL'])
        result = cursor.fetchall()
```

### 5. Data Ingestion Modules
These need PostgreSQL syntax updates:

#### `000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py`
- Update `fetch_and_store_once()` to write to PostgreSQL
- Remove DuckDB writes
- Change `?` to `%s` in SQL

#### `000data_feeds/2_create_price_cycles/create_price_cycles.py`
- Update to read prices from PostgreSQL
- Write cycles to PostgreSQL
- Change parameter syntax

#### `000data_feeds/3_binance_order_book_data/stream_binance_order_book_data.py`
- Update to write order book data to PostgreSQL
- Remove TradingDataEngine references

#### `features/webhook/app.py`
- Update to write trades to PostgreSQL
- Remove DuckDB writes

### 6. Trading Modules (`000trading/`)
All need parameter syntax updates (`?` → `%s`):

- `1_follow_the_goat/follow_the_goat.py`
- `2_sell_trailing_stop/trailing_stop.py`
- `3_train_validator/train_validator.py`
- `4_update_potential_gains/update_potential_gains.py`

**Common changes:**
```python
# OLD DuckDB syntax
cursor.execute("SELECT * FROM table WHERE id = ?", [123])

# NEW PostgreSQL syntax
cursor.execute("SELECT * FROM table WHERE id = %s", [123])
```

### 7. Website API (`scheduler/website_api.py`)
**Current:** Proxies requests to master2.py API
**Needed:** Query PostgreSQL directly

Remove these proxy patterns:
```python
# OLD
response = requests.get(f"{MASTER2_LOCAL_API_URL}/cycle_tracker")

# NEW
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM cycle_tracker WHERE ...")
        cycles = cursor.fetchall()
```

### 8. Profile and Pattern Modules (`features/`)
- Profile builders
- Pattern creation
- Filter analysis

All need similar updates to use PostgreSQL.

## 📋 MIGRATION CHECKLIST

### Before Running
- [ ] Run `scripts/postgres_schema.sql` on PostgreSQL
- [ ] Verify all tables exist: `SELECT * FROM information_schema.tables WHERE table_schema = 'public'`
- [ ] Configure PostgreSQL credentials in environment
- [ ] Back up any existing DuckDB data if needed

### Testing After Migration
- [ ] master.py starts successfully
- [ ] Prices being written to PostgreSQL
- [ ] Trades being received and stored
- [ ] master2.py starts without backfill
- [ ] Trading jobs can read from PostgreSQL
- [ ] Website loads data correctly
- [ ] No DuckDB references in logs

## 🔧 KEY SYNTAX CHANGES

### SQL Parameters
```python
# DuckDB used ?
cursor.execute("SELECT * FROM table WHERE id = ?", [123])

# PostgreSQL uses %s
cursor.execute("SELECT * FROM table WHERE id = %s", [123])
```

### Data Types
```python
# DuckDB
BIGINT, UBIGINT, UINTEGER, DOUBLE, TINYINT

# PostgreSQL
BIGSERIAL, BIGINT, INTEGER, DOUBLE PRECISION, SMALLINT
```

### JSON
```python
# DuckDB
JSON column type

# PostgreSQL
JSONB column type (binary JSON, faster)
```

### Upsert
```python
# DuckDB
INSERT OR REPLACE INTO table ...
INSERT OR IGNORE INTO table ...

# PostgreSQL
INSERT INTO table ... ON CONFLICT DO UPDATE ...
INSERT INTO table ... ON CONFLICT DO NOTHING
```

## 📊 ARCHITECTURE COMPARISON

### Before (DuckDB)
```
master.py → TradingDataEngine (DuckDB in-memory)
              ↓ (port 5050 API)
          master2.py → Local DuckDB (separate instance)
              ↓ (port 5052 API)
          Website API → Queries master2
```

### After (PostgreSQL)
```
master.py → PostgreSQL ←
              ↑            ↓ (direct queries)
          master2.py → PostgreSQL
              ↑            ↓ (direct queries)
          Website API → PostgreSQL
```

## 🎯 NEXT STEPS

1. **Finish master2.py refactoring** (highest priority)
   - This is the most complex change
   - Affects all trading logic
   
2. **Update data ingestion modules**
   - Jupiter price fetcher
   - Price cycles
   - Binance order book
   - Webhook receiver

3. **Update website_api.py**
   - Remove proxying
   - Query PostgreSQL directly

4. **Update trading modules**
   - Systematic search/replace of `?` → `%s`
   - Test each module

5. **Update .cursorrules**
   - Document new PostgreSQL-only architecture
   - Remove DuckDB references

6. **Test everything**
   - Start master.py - verify data ingestion
   - Start master2.py - verify trading logic
   - Check website - verify display
   - Monitor logs for errors

## 💡 TIPS

- Use global search/replace for common patterns
- Test one module at a time
- Keep PostgreSQL logs visible during testing
- Use `\d+ table_name` in psql to inspect tables
- Monitor connection pool usage

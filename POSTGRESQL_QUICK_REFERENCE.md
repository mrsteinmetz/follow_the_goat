# PostgreSQL Migration - Quick Reference Guide

## SQL Parameter Syntax Change

### The Critical Change: `?` → `%s`

**Every SQL query in the codebase needs this change:**

```python
# ❌ OLD (DuckDB syntax)
cursor.execute("SELECT * FROM table WHERE id = ?", [123])
cursor.execute("INSERT INTO table (a, b) VALUES (?, ?)", [1, 2])
cursor.execute("UPDATE table SET x = ? WHERE y = ?", [val1, val2])

# ✅ NEW (PostgreSQL syntax)
cursor.execute("SELECT * FROM table WHERE id = %s", [123])
cursor.execute("INSERT INTO table (a, b) VALUES (%s, %s)", [1, 2])
cursor.execute("UPDATE table SET x = %s WHERE y = %s", [val1, val2])
```

## Database Access Pattern Changes

### Reading Data

```python
# ❌ OLD (DuckDB)
from core.database import get_duckdb

with get_duckdb("central", read_only=True) as cursor:
    result = cursor.execute("SELECT * FROM prices WHERE token = ?", ['SOL']).fetchall()

# ✅ NEW (PostgreSQL)
from core.database import get_postgres

with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM prices WHERE token = %s", ['SOL'])
        result = cursor.fetchall()
```

### Writing Data

```python
# ❌ OLD (DuckDB)
from core.database import duckdb_execute_write

duckdb_execute_write("central", 
    "UPDATE follow_the_goat_buyins SET our_status = ? WHERE id = ?",
    ['sold', 123])

# ✅ NEW (PostgreSQL)
from core.database import postgres_execute

postgres_execute(
    "UPDATE follow_the_goat_buyins SET our_status = %s WHERE id = %s",
    ['sold', 123])
```

### Bulk Insert

```python
# ❌ OLD (DuckDB)
with get_duckdb("central") as conn:
    conn.executemany(
        "INSERT INTO prices (ts, token, price) VALUES (?, ?, ?)",
        batch_data
    )

# ✅ NEW (PostgreSQL)
from core.database import postgres_insert_many

postgres_insert_many("prices", [
    {"timestamp": ts1, "token": "SOL", "price": 123.45},
    {"timestamp": ts2, "token": "BTC", "price": 50000.00},
])
```

## SQL Syntax Differences

### Upsert (Insert or Update)

```python
# ❌ OLD (DuckDB)
cursor.execute("INSERT OR REPLACE INTO table (id, val) VALUES (?, ?)", [1, 'x'])
cursor.execute("INSERT OR IGNORE INTO table (id, val) VALUES (?, ?)", [1, 'x'])

# ✅ NEW (PostgreSQL)
cursor.execute("""
    INSERT INTO table (id, val) VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val
""", [1, 'x'])

cursor.execute("""
    INSERT INTO table (id, val) VALUES (%s, %s)
    ON CONFLICT DO NOTHING
""", [1, 'x'])
```

### Data Types

```sql
-- ❌ OLD (DuckDB)
CREATE TABLE example (
    id UBIGINT PRIMARY KEY,
    count UINTEGER,
    flag TINYINT,
    amount DOUBLE
);

-- ✅ NEW (PostgreSQL)
CREATE TABLE example (
    id BIGSERIAL PRIMARY KEY,
    count INTEGER,
    flag SMALLINT,
    amount DOUBLE PRECISION
);
```

### JSON Columns

```sql
-- ❌ OLD (DuckDB)
CREATE TABLE example (
    data JSON
);

-- ✅ NEW (PostgreSQL)
CREATE TABLE example (
    data JSONB  -- Binary JSON, faster
);
```

### Boolean Literals

```python
# ✅ Both work the same
cursor.execute("UPDATE table SET active = %s WHERE id = %s", [True, 123])
cursor.execute("UPDATE table SET active = %s WHERE id = %s", [False, 123])
```

### Date/Time Functions

```sql
-- ✅ Both work the same
SELECT NOW()
SELECT CURRENT_TIMESTAMP
SELECT timestamp - INTERVAL '1 hour'
```

## Files That Need Updates

### High Priority (Core Infrastructure)

1. ✅ `core/database.py` - **COMPLETED** (fully rewritten)
2. ✅ `scheduler/master.py` - **COMPLETED** (PostgreSQL only)
3. 🚧 `scheduler/master2.py` - **IN PROGRESS** (~4000 lines)
4. 🚧 `scheduler/website_api.py` - **TODO** (remove proxying)

### Medium Priority (Data Ingestion)

5. 🚧 `000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py`
6. 🚧 `000data_feeds/2_create_price_cycles/create_price_cycles.py`
7. 🚧 `000data_feeds/3_binance_order_book_data/stream_binance_order_book_data.py`
8. 🚧 `features/webhook/app.py`

### Medium Priority (Trading Logic)

9. 🚧 `000trading/follow_the_goat.py`
10. 🚧 `000trading/sell_trailing_stop.py`
11. 🚧 `000trading/train_validator.py`
12. 🚧 `000trading/trail_generator.py`
13. 🚧 `000trading/pattern_validator.py`

### Lower Priority (Support Modules)

14. Profile builders in `features/`
15. Pattern creation modules
16. Filter analysis modules

## Automated Search/Replace Commands

### Using ripgrep (rg) to find patterns:

```bash
# Find all ? placeholders in SQL
rg '\.execute\(["\'][^"\']*\?' --type py

# Find all get_duckdb calls
rg 'get_duckdb' --type py

# Find all duckdb_execute_write calls
rg 'duckdb_execute_write' --type py

# Find DuckDB imports
rg 'import duckdb|from.*duckdb' --type py
```

### Using sed for bulk replacement:

```bash
# Replace ? with %s in Python files (CAREFUL - test first!)
find . -name "*.py" -type f -exec sed -i 's/execute("\([^"]*\)?/execute("\1%s/g' {} +
find . -name "*.py" -type f -exec sed -i "s/execute('\([^']*\)?/execute('\1%s/g" {} +
```

**⚠️ WARNING:** Always review changes after automated replacement!

## Testing Strategy

### 1. Syntax Validation
```bash
# Check Python syntax
find . -name "*.py" -type f -exec python -m py_compile {} \;
```

### 2. Import Testing
```bash
# Test imports work
python -c "from scheduler import master, master2; print('OK')"
python -c "from core import database; print('OK')"
```

### 3. Database Connection Testing
```python
from core.database import get_postgres, verify_tables_exist

# Test connection
with get_postgres() as conn:
    print("PostgreSQL connected!")

# Verify schema
if verify_tables_exist():
    print("All tables exist!")
```

### 4. Function Testing
```python
from core.database import postgres_query_one, postgres_execute

# Test read
price = postgres_query_one(
    "SELECT price FROM prices WHERE token = %s ORDER BY timestamp DESC LIMIT 1",
    ['SOL']
)
print(f"Latest SOL price: {price}")

# Test write
rows = postgres_execute(
    "INSERT INTO prices (timestamp, token, price) VALUES (NOW(), %s, %s)",
    ['TEST', 999.99]
)
print(f"Inserted {rows} rows")
```

## Common Mistakes to Avoid

### ❌ Mistake 1: Mixing ? and %s
```python
# DON'T DO THIS:
cursor.execute("SELECT * FROM table WHERE a = ? AND b = %s", [1, 2])
```

### ❌ Mistake 2: Not using context managers
```python
# DON'T DO THIS:
conn = get_postgres_connection()  # Never closed!
cursor = conn.cursor()
cursor.execute("SELECT ...")
```

### ❌ Mistake 3: String formatting instead of parameters
```python
# DON'T DO THIS:
cursor.execute(f"SELECT * FROM table WHERE id = {user_id}")  # SQL injection!

# DO THIS:
cursor.execute("SELECT * FROM table WHERE id = %s", [user_id])
```

### ❌ Mistake 4: Forgetting to commit (when not using autocommit)
```python
# If autocommit is off:
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("INSERT ...")
    conn.commit()  # Don't forget!

# Our pool uses autocommit=True, so this is automatic
```

## Performance Tips

### Use Bulk Operations

```python
# ❌ Slow: Individual inserts
for record in records:
    postgres_insert("prices", record)

# ✅ Fast: Bulk insert
postgres_insert_many("prices", records)
```

### Use Connection Pooling

```python
# ✅ Pool automatically manages connections
with get_postgres() as conn:
    # Connection from pool
    pass
# Connection returned to pool
```

### Use Indexes

```sql
-- Create indexes on frequently queried columns
CREATE INDEX idx_prices_token_timestamp ON prices(token, timestamp);
CREATE INDEX idx_buyins_status ON follow_the_goat_buyins(our_status);
```

## Summary Checklist

Before marking a file as "migrated":

- [ ] All `import duckdb` removed
- [ ] All `get_duckdb()` replaced with `get_postgres()`
- [ ] All `duckdb_execute_write()` replaced with `postgres_execute()`
- [ ] All `?` placeholders changed to `%s`
- [ ] All SQL uses parameterized queries (not f-strings)
- [ ] Python syntax check passes
- [ ] Manual code review completed
- [ ] Tested with actual PostgreSQL database
- [ ] No DuckDB-specific types (UBIGINT, etc.)
- [ ] Proper context managers used
- [ ] Error handling in place

## Getting Help

If you encounter issues:

1. **Check logs:** PostgreSQL errors are usually descriptive
2. **Test query in psql:** Run the query directly in PostgreSQL
3. **Check connection:** Verify PostgreSQL is running and accessible
4. **Review schema:** Make sure tables exist with correct structure
5. **Check parameters:** Verify parameter count matches placeholders
6. **Enable query logging:** Set PostgreSQL log_statement='all'

## Next Steps After Migration

1. **Run schema migration:** `psql -f scripts/postgres_schema.sql`
2. **Test master.py:** Start and verify data ingestion
3. **Test master2.py:** Start and verify trading logic
4. **Test website:** Verify all pages load
5. **Monitor performance:** Check query execution times
6. **Optimize queries:** Add indexes where needed
7. **Set up backups:** Configure PostgreSQL backups
8. **Document changes:** Update team documentation

# DuckDB Architecture - Central Reference

> **This is the single source of truth for all database schemas in this project.**  
> All features must reference and update this document when adding/modifying tables.

---

## Database Files

| Database | Location | Purpose |
|----------|----------|---------|
| `prices.duckdb` | `000data_feeds/1_jupiter_get_prices/` | Token price data from Jupiter |

---

## Hot/Cold Storage Pattern

Every time-series table follows this pattern:

```
┌─────────────────────────────────────────────────────────────────┐
│                        INCOMING DATA                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HOT TABLE (24 hours)                          │
│  • Fast queries for recent data                                  │
│  • Indexes optimized for real-time access                        │
│  • Auto-purged after 24 hours                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                    (after 24 hours)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    COLD TABLE (archive)                          │
│  • Historical data storage                                       │
│  • Compressed for space efficiency                               │
│  • Queried only for analytics/backtesting                        │
└─────────────────────────────────────────────────────────────────┘
```

### Archive Query (run hourly via scheduler)
```sql
-- Move data older than 24 hours from hot to cold
INSERT INTO {table}_archive 
SELECT * FROM {table} 
WHERE created_at < NOW() - INTERVAL 24 HOUR;

DELETE FROM {table} 
WHERE created_at < NOW() - INTERVAL 24 HOUR;
```

---

## Database: prices.duckdb

### Table: price_points (HOT)
Real-time price data for the last 24 hours.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | TIMESTAMP | When price was fetched |
| `token` | VARCHAR(10) | Token symbol (BTC, ETH, SOL) |
| `price` | DOUBLE | USD price |

**Indexes:**
- `idx_price_points_ts` on `(ts)`
- `idx_price_points_token` on `(token)`
- `idx_price_points_token_ts` on `(token, ts)`

### Table: price_points_archive (COLD)
Historical price data beyond 24 hours.

| Column | Type | Description |
|--------|------|-------------|
| `ts` | TIMESTAMP | When price was fetched |
| `token` | VARCHAR(10) | Token symbol (BTC, ETH, SOL) |
| `price` | DOUBLE | USD price |

---

## Common Query Patterns

### Get Latest Price per Token
```sql
SELECT token, price, ts 
FROM price_points 
WHERE (token, ts) IN (
    SELECT token, MAX(ts) 
    FROM price_points 
    GROUP BY token
);
```

### Price History (Last Hour)
```sql
SELECT ts, price 
FROM price_points 
WHERE token = 'SOL' 
  AND ts >= NOW() - INTERVAL 1 HOUR 
ORDER BY ts;
```

### OHLC Candles (5-minute)
```sql
SELECT 
    token,
    time_bucket(INTERVAL '5 minutes', ts) AS bucket,
    FIRST(price) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    LAST(price) AS close
FROM price_points
WHERE token = 'BTC'
GROUP BY token, bucket
ORDER BY bucket DESC
LIMIT 12;
```

### Hot vs Cold Stats
```sql
SELECT 'hot' AS storage, COUNT(*) as rows FROM price_points
UNION ALL
SELECT 'cold', COUNT(*) FROM price_points_archive;
```

---

## DuckDB CLI Access

```bash
# From project root
./duckdb/duckdb.exe 000data_feeds/1_jupiter_get_prices/prices.duckdb

# Common commands
.tables              -- List all tables
.schema table_name   -- Show table schema
.mode markdown       -- Pretty output
.quit                -- Exit
```

---

## Python Connection Pattern

```python
import duckdb
from pathlib import Path

# Standard connection
DB_PATH = Path(__file__).parent.parent / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"

def get_connection():
    return duckdb.connect(str(DB_PATH))

# Usage with context manager
with get_connection() as conn:
    result = conn.execute("SELECT * FROM price_points LIMIT 10").fetchdf()
```

---

## Adding New Tables

When adding a new table:

1. **Update this document** with the new schema
2. **Create both hot and cold versions** if it's time-series data
3. **Add indexes** for query performance
4. **Register archive job** in `scheduler/master.py`

### Template for New Time-Series Table
```sql
-- HOT table
CREATE TABLE IF NOT EXISTS new_table (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- your columns here
);

CREATE INDEX IF NOT EXISTS idx_new_table_created_at ON new_table(created_at);

-- COLD table (same schema)
CREATE TABLE IF NOT EXISTS new_table_archive (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- your columns here
);
```

---

## Migration Notes

When migrating from MySQL (000old_code):

| MySQL | DuckDB |
|-------|--------|
| `INT AUTO_INCREMENT` | `INTEGER PRIMARY KEY` (use sequences if needed) |
| `DATETIME` | `TIMESTAMP` |
| `TEXT` | `VARCHAR` or `TEXT` |
| `DECIMAL(10,2)` | `DECIMAL(10,2)` or `DOUBLE` |
| `NOW()` | `CURRENT_TIMESTAMP` or `NOW()` |
| `DATE_SUB(NOW(), INTERVAL 24 HOUR)` | `NOW() - INTERVAL 24 HOUR` |


# PostgreSQL Historical Data Archival

## ✅ System Status: CONFIGURED AND READY

### Architecture:
```
┌─────────────────────────────────────────────────┐
│  DuckDB (In-Memory) - HOT STORAGE               │
│  - Recent 24h data (general tables)              │
│  - Recent 72h data (trading tables)              │
│  - Lightning fast queries                        │
│  - Used for real-time trading                    │
└────────────────┬────────────────────────────────┘
                 │ 
                 │ Automatic archival (every hour)
                 │ When data expires from hot storage
                 ▼
┌─────────────────────────────────────────────────┐
│  PostgreSQL (Disk) - HISTORICAL STORAGE         │
│  - Data older than 24-72h                        │
│  - Same table names as DuckDB                    │
│  - Used for analytics & backtesting              │
└─────────────────────────────────────────────────┘
```

## Configuration:

**Database:** `solcatcher`
**User:** `ftg_user`
**Host:** `127.0.0.1:5432`

**Retention Periods:**
- General tables: 24 hours in DuckDB
- Trading tables (buyins): 72 hours in DuckDB
- Everything older: Automatically moved to PostgreSQL

## Tables (Same Names in Both Systems):

1. `prices` - Jupiter price data
2. `price_analysis` - Price analysis records
3. `cycle_tracker` - Price cycle tracking
4. `wallet_profiles` - Wallet analysis
5. `follow_the_goat_buyins` - Trading positions
6. `buyin_trail_minutes` - Minute-by-minute trade tracking
7. `sol_stablecoin_trades` - Trade data
8. `order_book_features` - Order book metrics
9. `follow_the_goat_plays` - Wallet plays (persistent)
10. `job_execution_metrics` - System metrics

## How It Works:

1. **Data Collection:** 
   - New data flows into DuckDB (in-memory, fast)
   - Trading system uses DuckDB for real-time decisions

2. **Automatic Archival:**
   - Cleanup job runs **every hour**
   - Selects data older than retention period
   - Copies to PostgreSQL (async, non-blocking)
   - Deletes from DuckDB (frees memory)

3. **Query Strategy:**
   - Recent data (last 24-72h): Query DuckDB via API
   - Historical data: Query PostgreSQL directly

## Current Status:

```bash
# Check system health
curl http://127.0.0.1:5050/health | python3 -m json.tool

# Expected output:
# "postgres_archive": "ok"  ✅
```

## Testing Archival:

Since archival happens automatically after 24-72 hours, you won't see data in PostgreSQL immediately.

**To verify it's working:**

1. Wait 24+ hours
2. Check PostgreSQL for archived data:
```bash
PGPASSWORD='jjJH!la9823JKJsdfjk76jH' psql -h 127.0.0.1 -U ftg_user -d solcatcher -c "
SELECT 
    'prices' as table_name, 
    COUNT(*) as count, 
    MIN(timestamp) as oldest, 
    MAX(timestamp) as newest 
FROM prices;
"
```

## For Analytics & Backtesting:

Connect to PostgreSQL with any analytics tool:

```bash
# Command line
PGPASSWORD='jjJH!la9823JKJsdfjk76jH' psql -h 127.0.0.1 -U ftg_user -d solcatcher

# Python
import psycopg2
conn = psycopg2.connect(
    host="127.0.0.1",
    database="solcatcher",
    user="ftg_user",
    password="jjJH!la9823JKJsdfjk76jH"
)

# Example query
SELECT 
    DATE_TRUNC('day', timestamp) as day,
    token,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as samples
FROM prices
WHERE timestamp >= NOW() - INTERVAL '30 days'
GROUP BY day, token
ORDER BY day DESC;
```

## Key Differences from V1 (MySQL):

### ❌ OLD WAY (V1):
- MySQL with `_archive` tables
- Slow queries
- Duplicate table structure
- Complex maintenance

### ✅ NEW WAY (V2):
- DuckDB (hot) + PostgreSQL (cold)
- Same table names (no `_archive` suffix)
- Fast queries on recent data
- Automatic, transparent archival
- Zero maintenance

## Logs:

Archive activity is logged in:
- `/root/follow_the_goat/logs/scheduler_errors.log`

Look for messages like:
```
[ASYNC] Archived 1500 rows to prices
```

## Notes:

- Archival is **fire-and-forget** (non-blocking)
- If PostgreSQL is down, old data is simply deleted (trading continues)
- Trading speed is NEVER compromised for archival
- PostgreSQL connection is monitored and auto-reconnects


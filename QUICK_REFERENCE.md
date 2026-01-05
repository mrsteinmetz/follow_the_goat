# Follow The Goat - Quick Reference

## 🚨 CRITICAL: Read This First

**This system has TWO separate processes with TWO separate in-memory databases:**

1. **master.py** - Data ingestion (port 5050) - TradingDataEngine DuckDB
2. **master2.py** - Trading logic (port 5052) - Local DuckDB (separate instance)

They communicate via **HTTP API**, NOT shared memory.

---

## Decision Tree: Where Does My Code Go?

```
Does it fetch/receive NEW raw data from external sources?
│
├─ YES → master.py (Data Engine)
│   Examples:
│   - Fetching prices from Jupiter API
│   - Receiving trades from webhook
│   - Streaming order book from Binance
│   - Data cleanup/archiving
│
└─ NO → master2.py (Trading Logic)
    Examples:
    - Analyzing price patterns
    - Building wallet profiles
    - Making buy/sell decisions
    - Pattern detection
    - Any computation on existing data
```

---

## Quick Code Patterns

### I'm in master.py - How do I store data?

```python
from core.database import get_trading_engine

engine = get_trading_engine()
engine.execute("INSERT INTO prices VALUES (?, ?, ?)", [ts, token, price])
```

### I'm in master2.py - How do I get data?

```python
# STEP 1: Fetch from master.py API
from core.data_client import get_client
client = get_client()
prices = client.get_backfill("prices", hours=2)

# STEP 2: Store in master2's local DuckDB
from scheduler.master2 import queue_write, _local_duckdb
for price in prices:
    queue_write(_local_duckdb.execute,
        "INSERT INTO prices VALUES (?, ?, ?)",
        [price['ts'], price['token'], price['price']])
```

### I'm in master2.py - How do I read data?

```python
from scheduler.master2 import get_local_duckdb

cursor = get_local_duckdb(use_cursor=True)
results = cursor.execute("SELECT * FROM prices").fetchall()
```

### I'm writing a feature module - How do I access data?

```python
from core.database import get_duckdb

# This returns master2's local DuckDB (if registered)
with get_duckdb("central") as conn:
    data = conn.execute("SELECT * FROM my_table").fetchall()
```

---

## Port Reference

| Port | Service | Master | Purpose |
|------|---------|--------|---------|
| 5050 | FastAPI Data API | master.py | Raw data access |
| 5052 | FastAPI Local API | master2.py | Computed data |
| 8000 | PHP Server | master.py | Website |
| 8001 | Webhook Server | master.py | Trade ingestion |

---

## Common Commands

### Start System

```bash
# Terminal 1: Start data engine
cd /root/follow_the_goat
python scheduler/master.py

# Terminal 2: Start trading logic (after master.py is running)
cd /root/follow_the_goat
python scheduler/master2.py
```

### Check Health

```bash
# Check master.py data engine
curl http://localhost:5050/health | jq

# Check master2.py trading logic
curl http://localhost:5052/health | jq
```

### View Logs

```bash
# master.py errors
tail -f logs/scheduler_errors.log

# master2.py errors  
tail -f logs/master2_errors.log
```

### Restart Trading Logic (Without Stopping Data)

```bash
# In master2.py terminal: Ctrl+C
# master.py keeps running

# Make code changes
vim 000trading/follow_the_goat.py

# Restart master2.py
python scheduler/master2.py
# Automatically re-syncs data from master.py
```

---

## API Quick Reference

### master.py Data API (Port 5050)

```bash
# Get health status
curl http://localhost:5050/health

# Get latest SOL price
curl http://localhost:5050/price/SOL

# Get last 2 hours of prices (for master2 startup)
curl "http://localhost:5050/backfill/prices?hours=2"

# Query trades
curl -X POST http://localhost:5050/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM sol_stablecoin_trades LIMIT 10"}'

# Incremental sync (get new records since ID 1000)
curl "http://localhost:5050/sync/prices?since_id=1000&limit=1000"
```

### master2.py Local API (Port 5052)

```bash
# Get health status
curl http://localhost:5052/health

# Get price cycles
curl "http://localhost:5052/cycles?limit=10"

# Get wallet profiles
curl "http://localhost:5052/profiles?wallet_address=ABC123"

# Get active buyins
curl http://localhost:5052/buyins
```

---

## Common Mistakes (Don't Do This!)

### ❌ Trying to import between masters

```python
# In master2.py
from scheduler.master import _trading_engine  # WRONG! Separate processes!
```

### ❌ Adding trading logic to master.py

```python
# In master.py
def analyze_wallet_performance():  # WRONG! This belongs in master2.py
    pass
```

### ❌ Direct file access from master2

```python
# In master2.py
conn = duckdb.connect("data.duckdb")  # WRONG! Use API!
```

### ❌ Sharing database connections

```python
# These are SEPARATE in-memory databases, not shared!
master.py: _trading_engine (DuckDB #1)
master2.py: _local_duckdb (DuckDB #2)
# They are different objects in different processes!
```

---

## Troubleshooting

### "Connection refused on port 5050"

**Problem:** master2.py can't connect to master.py  
**Solution:** Start master.py first, wait 5 seconds, then start master2.py

```bash
# Terminal 1
python scheduler/master.py
# Wait for "All systems running!"

# Terminal 2
python scheduler/master2.py
```

### "No data in master2.py"

**Problem:** master2's local DuckDB is empty  
**Solution:** Check sync job is running, verify master.py has data

```bash
# Check master.py has data
curl http://localhost:5050/health | jq '.tables'

# Check master2.py health
curl http://localhost:5052/health | jq '.tables'

# Check master2 logs for sync errors
tail -f logs/master2.log | grep sync
```

### "Trading jobs not running"

**Problem:** master2.py scheduler issues  
**Solution:** Check master2.py logs and restart

```bash
# Check what's running
curl http://localhost:5052/health

# Restart master2.py (safe - master.py keeps running)
# Ctrl+C in master2.py terminal
python scheduler/master2.py
```

### "Data is stale in master2"

**Problem:** Sync job stopped or master.py is down  
**Solution:** Check both processes are running

```bash
# Check master.py
curl http://localhost:5050/health

# Check master2.py  
curl http://localhost:5052/health

# If master.py is down, restart it
python scheduler/master.py

# master2.py will reconnect automatically
```

---

## File Structure Quick Lookup

```
follow_the_goat/
├── scheduler/
│   ├── master.py          ← Data ingestion scheduler
│   ├── master2.py         ← Trading logic scheduler
│   └── status.py          ← Shared job tracking
├── core/
│   ├── database.py        ← DuckDB/PostgreSQL connections
│   ├── data_client.py     ← HTTP client for master.py API
│   ├── data_api.py        ← FastAPI app (port 5050)
│   └── config.py          ← Settings
├── features/              ← Feature modules (trading logic)
├── 000trading/            ← Core trading algorithms
├── 000data_feeds/         ← Data ingestion modules
├── .cursorrules           ← Project rules (READ THIS!)
├── ARCHITECTURE.md        ← Detailed architecture docs
└── QUICK_REFERENCE.md     ← This file
```

---

## Database Tables Quick Reference

### In master.py (TradingDataEngine)

**Hot storage - real-time data:**
- `prices` - SOL/BTC/ETH prices (Jupiter API)
- `order_book_snapshots` - Binance order book
- `sol_stablecoin_trades` - Trade data from webhook

### In master2.py (Local DuckDB)

**Synced from master.py:**
- `prices` - Copy of price data
- `sol_stablecoin_trades` - Copy of trade data  
- `order_book_snapshots` - Copy of order book

**Computed locally:**
- `price_cycles` - Price cycle analysis
- `wallet_profiles` - Wallet performance profiles
- `follow_the_goat_buyins` - Active positions
- `follow_the_goat_tracking` - Tracked wallets
- `pattern_analysis` - Detected patterns

### In PostgreSQL (Archive)

**Cold storage - historical data:**
- Same table names as DuckDB
- Data older than 24-72 hours
- Used for backtesting and analysis

---

## Need More Detail?

- **Full architecture:** See `ARCHITECTURE.md`
- **Project rules:** See `.cursorrules`
- **Database schema:** See `duckdb/ARCHITECTURE.md`
- **API documentation:** See docstrings in `core/data_api.py`

---

**Remember:** master.py = data in, master2.py = analysis/decisions out


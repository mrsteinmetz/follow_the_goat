# Follow The Goat - System Architecture

## 🚨 Critical: Dual-Master Architecture

This system uses **TWO SEPARATE PROCESSES** with **TWO SEPARATE IN-MEMORY DATABASES** that communicate via HTTP API.

---

## Overview

```
┌──────────────┐                    ┌──────────────┐
│  MASTER.PY   │  HTTP API (5050)  │  MASTER2.PY  │
│              │─────────────────→  │              │
│ Data Engine  │                    │Trading Logic │
│              │                    │              │
│  DuckDB #1   │                    │  DuckDB #2   │
│ (in-memory)  │                    │ (in-memory)  │
│              │                    │              │
│ + PostgreSQL │                    │              │
│  (archive)   │                    │              │
└──────────────┘                    └──────────────┘
```

---

## Master.py - Data Engine

**Purpose:** Raw data ingestion ONLY - runs indefinitely without restarts

### Database Setup

#### 1. TradingDataEngine (In-Memory DuckDB)
- **Type:** In-memory DuckDB instance
- **Purpose:** Hot storage for real-time data access
- **Data Retention:** 24-72 hours only
- **Tables:**
  - `prices` - SOL, BTC, ETH prices from Jupiter API
  - `order_book_snapshots` - Binance order book data
  - `sol_stablecoin_trades` - Trade data from QuickNode webhook
  - Other hot data tables

#### 2. PostgreSQL Archive
- **Type:** PostgreSQL database (local)
- **Purpose:** Cold storage for historical data
- **Data:** Anything older than 24-72 hours
- **Usage:** Historical analysis, backtesting

### Services

| Service | Port | Purpose |
|---------|------|---------|
| FastAPI Data API | 5050 | Serves data to master2.py |
| FastAPI Webhook | 8001 | Receives trade data from QuickNode |
| PHP Web Server | 8000 | Serves website frontend |
| Binance WebSocket | - | Order book stream |

### Data Ingestion Jobs (APScheduler)

```python
# Runs in master.py
- fetch_jupiter_prices      # Every 1 second
- sync_trades_from_webhook  # Every 1 second  
- cleanup_duckdb_hot_tables # Every hour (archive to PostgreSQL)
- cleanup_jupiter_prices    # Every hour
```

### FastAPI Data API Endpoints (Port 5050)

```
GET  /health                      - Health check with table counts
POST /query                       - Execute SELECT query
GET  /backfill/{table}            - Get historical data (startup)
GET  /sync/{table}                - Incremental sync by ID
POST /insert                      - Queue write to DuckDB
POST /insert/batch                - Queue batch write
POST /insert/sync                 - Insert and return ID
GET  /latest/{table}              - Get latest records
GET  /price/{token}               - Get current price
GET  /tables                      - List all tables
```

---

## Master2.py - Trading Logic

**Purpose:** All trading computation and decision making - CAN restart independently

### Database Setup

#### Local In-Memory DuckDB (SEPARATE INSTANCE)
- **Type:** Completely separate in-memory DuckDB
- **NOT shared with master.py** - they communicate via HTTP API
- **Tables:**
  - **Synced from master.py:** prices, trades, order_book
  - **Computed locally:** price_cycles, wallet_profiles, patterns, buyins

### Data Flow

```
1. STARTUP:
   master2.py starts
   → Fetches 2 hours of data from master.py API (port 5050)
   → Loads into local in-memory DuckDB

2. RUNTIME:
   Every 1-5 seconds:
   → Fetch new data from master.py API
   → Insert into local DuckDB
   → Trading jobs read from local DuckDB
   → Computed results stay in local DuckDB

3. RESTART:
   master2.py restarts (master.py keeps running)
   → Re-fetches data from master.py API
   → Trading logic resumes with fresh data
```

### Thread-Safe Database Access

```python
# WRITES: All serialized through background queue
from scheduler.master2 import queue_write, _local_duckdb

queue_write(_local_duckdb.execute, 
    "INSERT INTO prices VALUES (?, ?, ?)", 
    [ts, token, price])

# READS: Concurrent using thread-local cursors
from scheduler.master2 import get_local_duckdb

cursor = get_local_duckdb(use_cursor=True)
result = cursor.execute("SELECT * FROM prices").fetchall()
```

### Trading Jobs (APScheduler)

```python
# Runs in master2.py
- sync_from_master           # Every 1 second (fetch new data)
- process_price_cycles       # Every 2 seconds
- process_wallet_profiles    # Every 10 seconds
- train_validator_job        # Every 5 seconds
- follow_the_goat_job        # Every 5 seconds
- trailing_stop_monitor      # Every 5 seconds
- update_potential_gains     # Every 30 seconds
- create_new_patterns        # Every 60 seconds
```

### FastAPI Local API (Port 5052)

Serves computed trading data from master2's local DuckDB:

```
GET  /health           - Health check with local table counts
GET  /cycles           - Price cycle analysis
GET  /profiles         - Wallet profiles
GET  /buyins           - Active positions
GET  /patterns         - Detected patterns
```

---

## Data Access Patterns

### From master.py (Data Engine)

```python
from core.database import get_trading_engine

# Direct access to master.py's in-memory DuckDB
engine = get_trading_engine()

# Insert data
engine.execute("INSERT INTO prices VALUES (?, ?, ?)", [ts, token, price])

# Query data
results = engine.read_all("SELECT * FROM prices WHERE token = ?", ["SOL"])
```

### From master2.py (Trading Logic)

```python
from core.data_client import get_client
from scheduler.master2 import queue_write, _local_duckdb, get_local_duckdb

# STEP 1: Fetch from master.py via API
client = get_client()  # Connects to http://localhost:5050
prices = client.get_backfill("prices", hours=2)

# STEP 2: Insert into master2's local DuckDB (non-blocking)
for price in prices:
    queue_write(_local_duckdb.execute,
        "INSERT INTO prices VALUES (?, ?, ?)",
        [price['ts'], price['token'], price['price']])

# STEP 3: Read from master2's local DuckDB (concurrent-safe)
cursor = get_local_duckdb(use_cursor=True)
result = cursor.execute("SELECT * FROM prices ORDER BY ts DESC LIMIT 10").fetchall()
```

### From Feature Modules

After master2 registers its connection, feature modules can use the standard pattern:

```python
from core.database import get_duckdb

# This returns master2's local DuckDB (if registered)
with get_duckdb("central") as conn:
    cycles = conn.execute("SELECT * FROM price_cycles").fetchall()
```

---

## Why Two Separate Processes?

### Problem Solved

Without this architecture:
- ❌ Data ingestion stops when restarting trading logic
- ❌ Trading logic bugs could crash data feeds
- ❌ Can't update trading algorithms without data loss
- ❌ Database locks between data ingestion and analysis

With dual-master architecture:
- ✅ Data feeds run continuously (master.py never restarts)
- ✅ Trading logic can restart independently (master2.py)
- ✅ Update algorithms without stopping data collection
- ✅ No database lock contention (separate instances)
- ✅ Clear separation of concerns (data vs logic)

### Restart Independence

```
Scenario: Need to update trading algorithm

Before:
  Stop entire system → Update code → Restart → Miss 5 minutes of data

After:
  master.py keeps running (data continues)
  Stop master2.py → Update code → Restart master2.py
  master2.py fetches missed data from master.py API
```

---

## Communication Flow

```
External APIs/Webhooks
    ↓
master.py (TradingDataEngine)
    ↓ Writes
In-Memory DuckDB #1
    ↓ Exposes via
FastAPI (port 5050)
    ↓ HTTP API
master2.py (DataClient)
    ↓ Writes  
In-Memory DuckDB #2
    ↓ Reads
Trading Jobs
    ↓ Computes
Results in DuckDB #2
    ↓ Exposes via
FastAPI (port 5052)
    ↓
Website/Frontend
```

---

## Deployment

### Starting the System

1. **Start master.py first:**
   ```bash
   cd /root/follow_the_goat
   python scheduler/master.py
   ```
   - Starts data ingestion
   - Starts FastAPI on port 5050
   - Starts PHP server on port 8000
   - Starts webhook server on port 8001

2. **Then start master2.py:**
   ```bash
   cd /root/follow_the_goat
   python scheduler/master2.py
   ```
   - Connects to master.py API
   - Loads historical data
   - Starts trading jobs
   - Starts local API on port 5052

### Restarting Trading Logic

```bash
# Stop master2.py (Ctrl+C)
# master.py continues running

# Update trading code
vim 000trading/follow_the_goat.py

# Restart master2.py
python scheduler/master2.py

# master2.py fetches missed data from master.py automatically
```

### Health Checks

```bash
# Check master.py data engine
curl http://localhost:5050/health

# Check master2.py trading logic
curl http://localhost:5052/health
```

---

## Common Pitfalls (READ THIS!)

### ❌ WRONG: Trying to share DuckDB between processes

```python
# This does NOT work - they are separate processes
from master import _trading_engine  # ❌ Cross-process access
```

### ✅ CORRECT: Use HTTP API

```python
# In master2.py
from core.data_client import get_client

client = get_client()
prices = client.get_backfill("prices", hours=2)
```

### ❌ WRONG: Adding trading logic to master.py

```python
# In master.py
def analyze_trades():  # ❌ Trading logic doesn't belong here
    # This should be in master2.py
```

### ✅ CORRECT: Keep master.py focused on data ingestion

```python
# In master.py - Data ingestion only
def fetch_jupiter_prices():
    prices = fetch_from_api()
    engine.execute("INSERT INTO prices ...")

# In master2.py - Trading logic
def analyze_trades():
    trades = cursor.execute("SELECT * FROM trades").fetchall()
    # Analyze and make decisions
```

### ❌ WRONG: Direct database file access from master2

```python
# In master2.py
conn = duckdb.connect("000data_feeds/central.duckdb")  # ❌ Wrong!
```

### ✅ CORRECT: Master2 uses its own in-memory DuckDB

```python
# In master2.py
_local_duckdb = duckdb.connect(":memory:")  # ✅ Separate instance
# Sync data via API from master.py
```

---

## Summary Table

| Aspect | master.py | master2.py |
|--------|-----------|------------|
| **Purpose** | Raw data ingestion | Trading computation |
| **Database** | TradingDataEngine (in-memory DuckDB) | Local in-memory DuckDB (separate) |
| **Archive** | PostgreSQL | None |
| **Port** | 5050 (data API) | 5052 (local API) |
| **Restart** | Never (data feeds must continue) | Yes (trading logic updates) |
| **Jobs** | Fetch prices, sync trades, cleanup | Analysis, profiles, trading decisions |
| **Data Source** | External APIs, webhooks | master.py via HTTP API |
| **Exposes** | Raw data via API | Computed data via API |

---

## Questions?

If you're an AI agent working on this codebase:

1. **"Should this go in master.py or master2.py?"**
   - Does it fetch/receive NEW raw data? → master.py
   - Does it process/analyze existing data? → master2.py

2. **"How do I access data from master2?"**
   - Use `get_client()` from `core.data_client`
   - Fetch data via HTTP API from master.py
   - Insert into master2's local DuckDB

3. **"Can I share data between master.py and master2.py?"**
   - No direct sharing (separate processes, separate databases)
   - Communication via HTTP API only (port 5050)

4. **"What happens if master2 crashes?"**
   - master.py keeps running (data collection continues)
   - Restart master2.py, it re-syncs from master.py
   - No data loss

5. **"What happens if master.py crashes?"**
   - master2.py stops getting new data (stale data)
   - Restart master.py immediately
   - master2.py will reconnect automatically

---

**Last Updated:** January 2026  
**Maintainer:** Follow The Goat Team


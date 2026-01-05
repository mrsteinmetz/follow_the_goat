# Follow The Goat - Visual Architecture Diagrams

## System Overview - Bird's Eye View

```
┌────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCES                          │
│                                                                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐    │
│  │  Jupiter API │  │  Binance WS  │  │  QuickNode Webhook       │    │
│  │  (Prices)    │  │  (OrderBook) │  │  (Trades)                │    │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────────┘    │
└─────────┼──────────────────┼──────────────────────┼────────────────────┘
          │                  │                      │
          └──────────────────┴──────────────────────┘
                             │
                ┌────────────▼────────────┐
                │   MASTER.PY PROCESS     │
                │   (Data Engine)         │
                │   Port: 5050            │
                └────────────┬────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼────────┐  ┌────────▼─────────┐  ┌──────▼──────┐
│ TradingData    │  │ PostgreSQL       │  │ FastAPI     │
│ Engine         │  │ Archive          │  │ (port 5050) │
│ (In-Memory     │  │ (Cold Storage)   │  │             │
│  DuckDB #1)    │  │                  │  │ Serves Data │
└────────────────┘  └──────────────────┘  └──────┬──────┘
                                                  │
                                         HTTP API │
                                                  │
                             ┌────────────────────▼────────────────────┐
                             │      MASTER2.PY PROCESS                 │
                             │      (Trading Logic)                    │
                             │      Port: 5052                         │
                             └────────────┬────────────────────────────┘
                                          │
                   ┌──────────────────────┼──────────────────────┐
                   │                      │                      │
           ┌───────▼───────┐    ┌────────▼──────────┐   ┌───────▼──────┐
           │ Local DuckDB  │    │ Trading Jobs      │   │ FastAPI      │
           │ (In-Memory    │    │ - Cycles          │   │ (port 5052)  │
           │  DuckDB #2)   │    │ - Profiles        │   │              │
           │               │    │ - Follow Goat     │   │ Computed     │
           │ SEPARATE!     │    │ - Validator       │   │ Data API     │
           └───────────────┘    └───────────────────┘   └──────────────┘
```

---

## Data Flow - Step by Step

```
STEP 1: External Data Arrives at master.py
═══════════════════════════════════════════

External API → master.py → TradingDataEngine (DuckDB #1)
   (1s)           ↓
                Write to in-memory DuckDB
                  ↓
                Expose via FastAPI (port 5050)


STEP 2: master2.py Syncs Data
══════════════════════════════

master2.py → HTTP GET request → master.py API (port 5050)
              ↓
            Fetch new data (backfill/incremental)
              ↓
            Write to Local DuckDB (DuckDB #2)
              ↓
            Now available for trading jobs


STEP 3: Trading Jobs Process Data
══════════════════════════════════

Trading Job → Read from Local DuckDB (DuckDB #2)
     ↓
   Compute analysis (cycles, profiles, patterns)
     ↓
   Write results to Local DuckDB (DuckDB #2)
     ↓
   Expose via FastAPI (port 5052)


STEP 4: Archiving (Background)
═══════════════════════════════

TradingDataEngine (DuckDB #1) → Data older than 24-72h
                    ↓
              Archive to PostgreSQL
                    ↓
              Delete from DuckDB #1
              (master2.py unaffected - has its own copy)
```

---

## Process Separation - Why It Matters

```
WITHOUT Dual-Master (Old Architecture):
═══════════════════════════════════════

┌──────────────────────────────────┐
│    Single Process (master.py)    │
│                                  │
│  Data Ingestion ───┐             │
│                    ├──→ DuckDB   │
│  Trading Logic ────┘             │
└──────────────────────────────────┘

Problems:
❌ Restart for code updates = miss data
❌ Trading bug crashes data ingestion
❌ Database locks between jobs
❌ No separation of concerns


WITH Dual-Master (New Architecture):
═══════════════════════════════════

┌──────────────────────────────────┐
│   PROCESS 1: master.py           │
│   Data Ingestion ONLY            │
│                                  │
│   DuckDB #1 (Raw Data)           │
│   Runs Forever                   │
└────────────┬─────────────────────┘
             │ HTTP API (5050)
             │
┌────────────▼─────────────────────┐
│   PROCESS 2: master2.py          │
│   Trading Logic ONLY             │
│                                  │
│   DuckDB #2 (Synced + Computed)  │
│   Can Restart Anytime            │
└──────────────────────────────────┘

Benefits:
✅ Restart master2 = data keeps flowing
✅ Trading bug can't crash data feeds
✅ No database lock contention
✅ Clear separation of concerns
✅ Independent scaling
```

---

## Database Instances - Critical Distinction

```
THESE ARE NOT SHARED - THEY ARE SEPARATE INSTANCES:
═══════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────┐
│ PROCESS 1: master.py (PID 1234)                         │
│                                                         │
│   _trading_engine = TradingDataEngine()                 │
│      ↓                                                  │
│   In-Memory DuckDB Instance #1                          │
│   Address: 0x7f8a1c000000 (example)                     │
│                                                         │
│   Tables: prices, order_book_snapshots, trades          │
└─────────────────────────────────────────────────────────┘

        │ HTTP API (port 5050)
        │ JSON over TCP/IP
        ▼

┌─────────────────────────────────────────────────────────┐
│ PROCESS 2: master2.py (PID 5678)                        │
│                                                         │
│   _local_duckdb = duckdb.connect(":memory:")            │
│      ↓                                                  │
│   In-Memory DuckDB Instance #2                          │
│   Address: 0x7f9c3d000000 (example)                     │
│                                                         │
│   Tables: prices (synced), cycles, profiles, patterns   │
└─────────────────────────────────────────────────────────┘

KEY POINTS:
• Different memory addresses (separate processes)
• Different data (master2 has synced copy + computed data)
• Communication via HTTP API (not direct memory access)
• master2 restarts = new DuckDB instance, re-sync from master
```

---

## Thread Safety in master2.py

```
WRITE QUEUE PATTERN:
════════════════════

Job 1 ──┐
Job 2 ──┼──→ Write Queue ──→ Background Writer Thread
Job 3 ──┘                         ↓
                            Serialized Writes
                                   ↓
                            Local DuckDB (#2)


READ PATTERN:
═════════════

Job 1 ──→ Thread-Local Cursor 1 ──┐
                                   ├──→ Concurrent Reads
Job 2 ──→ Thread-Local Cursor 2 ──┤    (No Blocking)
                                   │
Job 3 ──→ Thread-Local Cursor 3 ──┘
                  ↓
           Local DuckDB (#2)


WHY THIS MATTERS:
• Writes are serialized (no conflicts)
• Reads are concurrent (no blocking)
• Jobs don't wait for each other (fast)
```

---

## Communication Pattern

```
master2.py CANNOT directly access master.py's memory:
═════════════════════════════════════════════════════

❌ WRONG (Cross-Process Memory Access):
from scheduler.master import _trading_engine
result = _trading_engine.read_all("SELECT * FROM prices")
# This doesn't work - different processes!


✅ CORRECT (HTTP API):
from core.data_client import get_client

client = get_client()  # HTTP client to localhost:5050
result = client.query("SELECT * FROM prices")
# This works - HTTP API communication


ANALOGY:
master.py and master2.py are like two separate computers
connected by a network cable (HTTP API).

They CAN'T share RAM.
They CAN send messages via HTTP.
```

---

## Restart Scenario - Visual Walkthrough

```
SCENARIO: Update trading algorithm without stopping data feeds
═══════════════════════════════════════════════════════════════

TIME: 10:00 AM
─────────────
master.py:  Running (fetching prices every 1s)
master2.py: Running (analyzing trades)

┌─────────────┐     ┌─────────────┐
│  master.py  │────→│ master2.py  │
│  (Running)  │ API │  (Running)  │
└─────────────┘     └─────────────┘


TIME: 10:05 AM - Need to update trading algorithm
──────────────────────────────────────────────────
Developer: Stops master2.py (Ctrl+C)

┌─────────────┐     ┌─────────────┐
│  master.py  │  X  │ master2.py  │
│  (Running)  │     │  (STOPPED)  │
└─────────────┘     └─────────────┘
     │
     └──→ Still fetching prices! (No data loss)


TIME: 10:06 AM - Edit code
───────────────────────────
$ vim 000trading/follow_the_goat.py
# Make changes to trading algorithm

┌─────────────┐     
│  master.py  │     (master2.py still stopped)
│  (Running)  │     
└─────────────┘     
     │
     └──→ Still fetching prices! (5 minutes of data accumulated)


TIME: 10:10 AM - Restart master2.py
────────────────────────────────────
$ python scheduler/master2.py

┌─────────────┐     ┌─────────────┐
│  master.py  │────→│ master2.py  │
│  (Running)  │ API │  (STARTING) │
└─────────────┘     └─────────────┘
                           │
                           └──→ 1. Fetch last 2 hours from master.py
                                2. Load into local DuckDB
                                3. Resume trading jobs
                                4. All caught up!


TIME: 10:11 AM - System fully operational
──────────────────────────────────────────
master.py:  Running (never stopped)
master2.py: Running (with updated algorithm)

┌─────────────┐     ┌─────────────┐
│  master.py  │────→│ master2.py  │
│  (Running)  │ API │  (Running)  │
└─────────────┘     └─────────────┘

Result:
✅ No data loss (master.py kept running)
✅ New algorithm deployed
✅ System caught up in 1 minute
```

---

## Port Map - Visual Reference

```
┌────────────────────────────────────────────────┐
│            Network Port Layout                 │
└────────────────────────────────────────────────┘

     0.0.0.0 (All Interfaces)
          │
   ┌──────┴──────┬──────────┬──────────┐
   │             │          │          │
   │             │          │          │
  5050          5052       8000       8001
   │             │          │          │
┌──▼────────┐ ┌─▼──────┐ ┌─▼──────┐ ┌─▼──────┐
│ master.py │ │master2 │ │  PHP   │ │Webhook │
│ Data API  │ │Local   │ │ Server │ │ Server │
│           │ │API     │ │        │ │        │
│ Raw Data  │ │Computed│ │Website │ │Trades  │
└───────────┘ └────────┘ └────────┘ └────────┘


CONNECTIONS:
════════════

master2.py ────→ master.py:5050 (Fetch data)
Website    ────→ master2.py:5052 (Get computed data)
QuickNode  ────→ master.py:8001 (Send trades)
Browser    ────→ PHP:8000 (View website)


ACCESS CONTROL:
═══════════════

5050: Internal only (master2 ↔ master)
5052: External OK (website frontend)
8000: External OK (public website)
8001: External only (QuickNode webhook)
```

---

## Summary Comparison Chart

```
┌──────────────────┬──────────────────────┬──────────────────────┐
│     ASPECT       │      MASTER.PY       │      MASTER2.PY      │
├──────────────────┼──────────────────────┼──────────────────────┤
│ Purpose          │ Data Ingestion       │ Trading Logic        │
│ Restart Allowed? │ NO (data loss)       │ YES (re-syncs)       │
│ Database         │ DuckDB #1 (in-mem)   │ DuckDB #2 (in-mem)   │
│ Archive DB       │ PostgreSQL           │ None                 │
│ API Port         │ 5050 (data)          │ 5052 (computed)      │
│ Data Source      │ External APIs        │ master.py API        │
│ Data Type        │ Raw (prices, trades) │ Computed (cycles)    │
│ Jobs             │ Fetch, cleanup       │ Analyze, decide      │
│ Uptime           │ 24/7 (critical)      │ Can restart          │
│ Depends On       │ External services    │ master.py            │
│ Services         │ 4 (API, webhook,     │ 1 (local API)        │
│                  │    PHP, Binance WS)  │                      │
└──────────────────┴──────────────────────┴──────────────────────┘
```

---

## Decision Flowchart

```
                    ┌─────────────────────────┐
                    │  New Feature/Code       │
                    │  Where does it go?      │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼────────────┐
                    │ Does it fetch/receive  │
                    │ NEW raw data from      │
                    │ external sources?      │
                    └───────────┬────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
             ┌──▼──┐                         ┌──▼──┐
             │ YES │                         │ NO  │
             └──┬──┘                         └──┬──┘
                │                               │
      ┌─────────▼──────────┐        ┌──────────▼─────────┐
      │   MASTER.PY        │        │   MASTER2.PY       │
      │   (Data Engine)    │        │   (Trading Logic)  │
      └────────────────────┘        └────────────────────┘
                │                               │
      ┌─────────▼──────────┐        ┌──────────▼─────────┐
      │ Examples:          │        │ Examples:          │
      │ • Jupiter API      │        │ • Cycle analysis   │
      │ • Binance stream   │        │ • Wallet profiles  │
      │ • QuickNode webhook│        │ • Trade decisions  │
      │ • Data cleanup     │        │ • Pattern detection│
      └────────────────────┘        └────────────────────┘
```

---

**Remember: Two processes, two databases, HTTP API communication!**

For more details, see:
- `.cursorrules` - AI agent rules
- `ARCHITECTURE.md` - Detailed technical docs
- `QUICK_REFERENCE.md` - Fast lookup guide


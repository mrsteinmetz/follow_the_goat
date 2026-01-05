# Documentation Update Summary

## What Was Done

I've investigated your dual-master architecture and created comprehensive documentation to help AI agents (and developers) understand the critical setup.

---

## Files Created/Updated

### 1. `.cursorrules` (UPDATED)
**Primary rule file for AI agents** - This is what Cursor AI reads first.

**New sections added:**
- 🚨 **CRITICAL: DUAL-MASTER ARCHITECTURE** section at the top
- Clear explanation of master.py (Data Engine) vs master2.py (Trading Logic)
- Database access patterns for each context
- Port assignments table
- Decision tree for "which master for what"
- Critical reminders for AI agents with 🔴 markers
- ASCII architecture diagram

### 2. `ARCHITECTURE.md` (NEW)
**Detailed technical documentation** - 11 KB comprehensive guide.

**Contents:**
- System overview with visual diagrams
- Complete master.py architecture (TradingDataEngine, PostgreSQL, services)
- Complete master2.py architecture (Local DuckDB, sync strategy, jobs)
- Data flow explanations
- Thread-safe database access patterns
- Why two separate processes? (problem/solution)
- Communication flow diagram
- Deployment instructions
- Common pitfalls with ❌ WRONG / ✅ CORRECT examples
- Summary comparison table

### 3. `QUICK_REFERENCE.md` (NEW)
**Fast lookup guide** - Perfect for quick questions.

**Contents:**
- Decision tree: "Where does my code go?"
- Quick code patterns (copy-paste ready)
- Port reference table
- Common commands (start, health checks, restart)
- API quick reference with curl examples
- Common mistakes (with explanations)
- Troubleshooting section
- Database tables quick reference
- File structure lookup

---

## Key Concepts Documented

### 🔴 MOST CRITICAL: Two Separate Databases

```
master.py:   TradingDataEngine (In-Memory DuckDB #1)
             ↓ Exposes via FastAPI (port 5050)
             ↓ HTTP API
master2.py:  Local DuckDB (In-Memory DuckDB #2)
             ↓ Completely separate instance
             ↓ Syncs data from master.py
```

**They are NOT shared memory - they communicate via HTTP API!**

### Decision Rule

```
Does it CREATE new raw data from external sources?
├─ YES → master.py (Data Engine)
└─ NO  → master2.py (Trading Logic)
```

### Database Access

**In master.py:**
```python
from core.database import get_trading_engine
engine = get_trading_engine()  # Direct access to TradingDataEngine
```

**In master2.py:**
```python
from core.data_client import get_client
client = get_client()  # HTTP client to master.py API
prices = client.get_backfill("prices", hours=2)
# Then insert into master2's local DuckDB
```

### Port Assignments

| Port | Service | Master | Purpose |
|------|---------|--------|---------|
| 5050 | FastAPI Data API | master.py | Serves raw data |
| 5052 | FastAPI Local API | master2.py | Serves computed data |
| 8000 | PHP Server | master.py | Website |
| 8001 | Webhook Server | master.py | Trade ingestion |

---

## What This Solves

### Before Documentation:

❌ AI agents confused about which master to use  
❌ Attempted direct database sharing between processes  
❌ Added trading logic to master.py (data engine)  
❌ Added data ingestion to master2.py (trading logic)  
❌ Tried to import `_trading_engine` from master.py into master2.py  
❌ Didn't understand they are separate in-memory databases

### After Documentation:

✅ Clear decision tree: data ingestion → master.py, processing → master2.py  
✅ Explicit: "They communicate via HTTP API, NOT shared memory"  
✅ Code examples for each context (master.py, master2.py, features)  
✅ Visual diagrams showing the separation  
✅ Common pitfalls section with WRONG vs CORRECT examples  
✅ Critical reminders with 🔴 markers  
✅ Quick reference for fast lookups

---

## How AI Agents Should Use This

### On Every Task:

1. **Read `.cursorrules` first** - especially the 🚨 CRITICAL section
2. **Check "Which Master for What"** section - data ingestion or processing?
3. **Use the correct code pattern** - master.py vs master2.py vs features
4. **Verify ports** - 5050 (data API), 5052 (local API), 8000 (web), 8001 (webhook)

### When Confused:

1. **Check `QUICK_REFERENCE.md`** - decision tree and quick patterns
2. **Check `ARCHITECTURE.md`** - detailed explanations
3. **Check "Common Pitfalls"** section - see WRONG vs CORRECT examples

### Before Making Changes:

1. **Verify context** - Am I in master.py or master2.py?
2. **Use correct database access** - `get_trading_engine()` vs `get_client()`
3. **Don't try to share database connections** - they are separate processes

---

## Documentation Structure

```
.cursorrules (PRIMARY - AI agents read this)
├─ 🚨 CRITICAL: DUAL-MASTER ARCHITECTURE
├─ Database Access Patterns
├─ Core Principles
├─ Port Assignments
├─ Project Structure
├─ When to Use Which Master
├─ Code Patterns (master.py, master2.py, features)
├─ Migration Workflow
├─ Forbidden Practices
├─ Critical Reminders for AI Agents
└─ Architecture Diagram

ARCHITECTURE.md (DETAILED - deep dive)
├─ Overview
├─ Master.py - Data Engine (complete spec)
├─ Master2.py - Trading Logic (complete spec)
├─ Data Access Patterns
├─ Why Two Separate Processes?
├─ Communication Flow
├─ Deployment
└─ Common Pitfalls

QUICK_REFERENCE.md (FAST - quick lookup)
├─ Decision Tree
├─ Quick Code Patterns
├─ Port Reference
├─ Common Commands
├─ API Quick Reference
├─ Common Mistakes
├─ Troubleshooting
└─ File Structure Lookup
```

---

## Testing the Documentation

### Verify AI agent understanding by asking:

**Q: "Where should I add code that fetches prices from an API?"**  
**A: master.py (data ingestion) - creates new raw data**

**Q: "Where should I add code that analyzes wallet performance?"**  
**A: master2.py (trading logic) - processes existing data**

**Q: "How do I access data from master.py in master2.py?"**  
**A: Use `get_client()` from `core.data_client` - HTTP API, not direct access**

**Q: "Can I import `_trading_engine` from master.py into master2.py?"**  
**A: NO - separate processes, separate databases, use HTTP API**

**Q: "What port does master.py data API run on?"**  
**A: 5050**

**Q: "What port does master2.py local API run on?"**  
**A: 5052**

---

## Key Phrases for AI Agents to Recognize

When you see these in AI agent responses, they understand the architecture:

✅ "Master.py handles data ingestion, master2.py handles trading logic"  
✅ "They communicate via HTTP API on port 5050"  
✅ "Master2 has its own separate in-memory DuckDB"  
✅ "I'll use get_client() to fetch data from master.py"  
✅ "This is data ingestion, so it goes in master.py"  
✅ "This is data processing, so it goes in master2.py"  
✅ "Master2 can restart without stopping master.py"

When you see these, they're confused (need to re-read docs):

❌ "I'll access the shared database"  
❌ "I'll import _trading_engine from master.py"  
❌ "I'll add this data fetching job to master2.py"  
❌ "I'll add this analysis job to master.py"  
❌ "They share the same DuckDB instance"

---

## Maintenance Notes

### When adding new features:

1. Update `.cursorrules` - keep the critical sections up to date
2. Update `ARCHITECTURE.md` - add new services/tables
3. Update `QUICK_REFERENCE.md` - add new common patterns

### When ports change:

Update in **3 places**:
1. `.cursorrules` - Port Assignments table
2. `ARCHITECTURE.md` - Services section
3. `QUICK_REFERENCE.md` - Port Reference table

### When adding new masters:

**DON'T** - The dual-master architecture is intentional and sufficient.

If you think you need a third master:
1. Check if it fits in master.py (data ingestion)
2. Check if it fits in master2.py (trading logic)
3. Consider if it should be a standalone service with its own API

---

## Summary for the User

**What changed:**
- `.cursorrules` - Completely rewritten with dual-master architecture at the top
- `ARCHITECTURE.md` - NEW comprehensive technical documentation (11 KB)
- `QUICK_REFERENCE.md` - NEW fast lookup guide

**What's documented:**
- TWO separate processes with TWO separate in-memory databases
- HTTP API communication (port 5050) between them
- Clear decision rules for which master to use
- Code patterns for each context
- Common pitfalls with WRONG vs CORRECT examples
- Visual diagrams and decision trees

**What AI agents will now understand:**
- master.py = data ingestion (runs forever)
- master2.py = trading logic (can restart)
- Separate in-memory DuckDBs (not shared)
- HTTP API communication (not direct access)
- Port assignments (5050, 5052, 8000, 8001)
- Correct database access patterns for each context

**Next time an AI agent is confused:**
- Point them to `.cursorrules` (they should read this first)
- Point them to `QUICK_REFERENCE.md` for fast lookup
- Point them to `ARCHITECTURE.md` for deep dive

---

**The documentation is now clear, comprehensive, and hard to misunderstand!** 🎉


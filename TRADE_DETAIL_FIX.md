# Trade Detail Page Fix - Complete Summary

## Problem
When clicking on a trade detail link (e.g., `/pages/features/trades/detail.php?id=20260103074424711`), the page showed "Trade not found."

## Root Causes Found

### 1. Missing API Endpoints
The `website_api.py` (port 5051) was missing two critical endpoints:
- `GET /buyins/<id>` - to fetch a single trade by ID  
- `GET /trail/buyin/<id>` - to fetch 15-minute trail data

### 2. Wrong Database Architecture  
- Buyins and trail data are stored in **master2.py's local DuckDB** (in-memory)
- The initial attempt tried to query master.py's engine (port 5050), which doesn't have this data
- Master2.py has its own Local API on port 5052 that can query its database

### 3. Website API Not Starting Properly
- The website API process needs to run within the Python virtual environment
- Without the venv, Flask and other dependencies are not available
- This causes the PHP client's `isAvailable()` check to fail

## Solution Implemented

### Changes Made to `scheduler/website_api.py`:

1. **Added constant for master2's API:**
```python
MASTER2_LOCAL_API_URL = "http://127.0.0.1:5052"
```

2. **Added `/buyins/<id>` endpoint:**
   - Proxies requests to master2's `/query` endpoint
   - Queries: `SELECT * FROM follow_the_goat_buyins WHERE id = {id}`
   - Returns: `{status: 'ok', buyin: {...}}`

3. **Added `/trail/buyin/<id>` endpoint:**
   - Proxies requests to master2's `/query` endpoint  
   - Queries: `SELECT * FROM buyin_trail_minutes WHERE buyin_id = {id} ORDER BY minute ASC`
   - Returns: `{status: 'ok', trail_data: [...], count: 15}`

## How to Fix

### Step 1: Verify All Processes Are Running

Check which processes are running:
```bash
wsl bash -c "ps aux | grep -E 'master|website_api' | grep -v grep"
```

You should see:
- `python scheduler/master.py` (port 5050) - Data Engine
- `python scheduler/master2.py` (port 5052) - Trading Logic + Local API  
- `python scheduler/website_api.py` (port 5051) - Website API Proxy

### Step 2: Start Website API with Virtual Environment

If website_api is not running or not responding, restart it:

```bash
# Kill any hung instances
wsl bash -c "pkill -f 'python scheduler/website_api.py'"

# Start properly with venv
wsl bash -c "source ~/follow_the_goat_venv/bin/activate && cd /mnt/c/0000websites/00phpsites/follow_the_goat && python scheduler/website_api.py"
```

### Step 3: Verify APIs Are Responding

Test each API:
```bash
# Test Data Engine (master.py - port 5050)
curl http://127.0.0.1:5050/health

# Test Trading API (master2.py - port 5052)  
curl http://127.0.0.1:5052/health

# Test Website API (website_api.py - port 5051)
curl http://127.0.0.1:5051/health
```

All should return JSON with `"status": "ok"`.

### Step 4: Test The New Endpoints

Test the buyin endpoint:
```bash
curl http://127.0.0.1:5051/buyins/20260103074424711
```

Should return a JSON object with the buyin data.

Test the trail endpoint:
```bash
curl http://127.0.0.1:5051/trail/buyin/20260103074424711
```

Should return 15 rows of trail data (one per minute, 0-14).

## Expected Results

After properly starting the website API:

1. **Trades list page** (`/pages/features/trades/`) should load and show recent trades
2. **Trade detail page** (`/pages/features/trades/detail.php?id=20260103074424711`) should show:
   - Trade header with ID, status, entry price, P/L
   - Price change overview for all 15 minutes
   - Interactive charts for all data sections
   - Complete data table with all fields across 15 minutes
   - Scatter plot visualization

## Architecture Diagram

```
Website (PHP on port 8000)
    ↓ HTTP
website_api.py (port 5051) 
    ↓ HTTP to port 5050          ↓ HTTP to port 5052
master.py (Data Engine)      master2.py (Trading Logic)
    ↓                             ↓
In-Memory DuckDB              In-Memory DuckDB
(prices, cycles, etc.)        (buyins, trails, etc.)
```

## Files Modified

1. `scheduler/website_api.py` - Added two new endpoints with proper proxying to master2's API

## Testing

Run the test script to verify endpoints work:
```bash
wsl bash -c "cd /mnt/c/0000websites/00phpsites/follow_the_goat && python3 test_endpoint.py"
```

Expected output:
```
1. Testing GET /buyins/20260103074424711
   Status: 200
   ✓ PASS: Buyin endpoint working

2. Testing GET /trail/buyin/20260103074424711  
   Status: 200
   Trail data rows: 15
   ✓ PASS: Trail endpoint working
```

## Common Issues

### Issue: "DuckDB API is not available"
- **Cause**: website_api.py is not running or not responding
- **Fix**: Start it with the virtual environment activated (see Step 2)

### Issue: "/health endpoint times out"
- **Cause**: API is hung, usually from trying to access wrong database
- **Fix**: Kill and restart: `pkill -f website_api.py` then start properly

### Issue: "Trade not found" even with API running
- **Cause**: The trade might not exist in master2's database
- **Fix**: Check master2's buyin count: `curl http://127.0.0.1:5052/health | grep buyins`

## Notes

- Master2.py stores its data in an **in-memory DuckDB** that only exists while the process is running
- If you restart master2.py, all training trades will be lost (they are not persisted to disk by default)
- Trail data validation errors (`pattern_config_filters table missing`) are non-critical - trades still work

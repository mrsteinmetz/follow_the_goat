# Data Streams Pages - PostgreSQL Migration Check

## Context
We recently migrated from DuckDB to PostgreSQL. The website at http://195.201.84.5 has three data stream pages:

1. ✅ **Binance Order Book** - `/data-streams/binance-order-book/` - **FIXED**
2. ❓ **Whale Activity** - `/data-streams/whale-activity/` - **NEEDS CHECK**
3. ❓ **Transactions** - `/data-streams/transactions/` - **NEEDS CHECK**

## Issue Found on Order Book Page

The order book page was showing "No Data" even though:
- Master.py was running and feeding data to PostgreSQL
- 291,000+ records existed in the `order_book_features` table

**Root Cause:** The `website_api.py` API endpoint wasn't properly handling structured queries, and column names didn't match between what PHP expected (old DuckDB schema) and what PostgreSQL has.

**Fix Applied:**
- Enhanced `/query` endpoint in `website_api.py` to support structured queries
- Added column name mappings for PostgreSQL compatibility
- Fixed source display to show "🐘 PostgreSQL" instead of "DuckDB"

## Task: Check Other Data Stream Pages

Please check if the **Whale Activity** and **Transactions** pages have the same issue:

### 1. Check if Pages Show Data

Visit these URLs:
- http://195.201.84.5/data-streams/whale-activity/
- http://195.201.84.5/data-streams/transactions/

Check for:
- Are records displaying?
- Does it say "No Data" or show an error?
- What does the data source badge say? (Should be "🐘 PostgreSQL" not "DuckDB")

### 2. Verify Data Exists in PostgreSQL

Check if the underlying tables have data:

```bash
cd /root/follow_the_goat
python3 -c "
from core.database import get_postgres
with get_postgres() as conn:
    with conn.cursor() as cursor:
        # Check whale activity data (likely sol_stablecoin_trades with large amounts)
        cursor.execute('SELECT COUNT(*) as cnt FROM sol_stablecoin_trades')
        trades = cursor.fetchone()
        print(f'sol_stablecoin_trades: {trades[\"cnt\"]} records')
"
```

### 3. Check PHP Files

The PHP files are located at:
- `/root/follow_the_goat/000website/data-streams/whale-activity/index.php`
- `/root/follow_the_goat/000website/data-streams/transactions/index.php`

Look for:
1. What table/columns are they querying?
2. Do they use the same `DatabaseClient->query()` method?
3. What column names are they requesting?

### 4. If Issues Found

Apply the same fix pattern as order book:
1. Add column mappings to `website_api.py` for the relevant tables
2. Fix the data source display in the PHP files (search for "DuckDB" and replace with proper PostgreSQL detection)

## Key Files

- **API Backend**: `/root/follow_the_goat/scheduler/website_api.py` (port 5051)
- **PHP Pages**: `/root/follow_the_goat/000website/data-streams/*/index.php`
- **PHP Client**: `/root/follow_the_goat/000website/includes/DatabaseClient.php`
- **Config**: `/root/follow_the_goat/000website/includes/config.php`

## Architecture Notes

**Current System (PostgreSQL Only):**
```
Master.py (Data Engine) → PostgreSQL ← Master2.py (Trading Logic)
                              ↑
                              ↓
                        website_api.py (Port 5051)
                              ↓
                        PHP Website (Port 8000)
```

- **All data** is in PostgreSQL
- **Master.py** feeds: prices, trades, order book data
- **Master2.py** processes: buyins, positions, patterns
- **website_api.py** serves data to the website

## Quick Test Command

```bash
# Test whale activity page
curl -s "http://195.201.84.5/data-streams/whale-activity/?ajax=refresh" | python3 -c "import sys, json; data = json.load(sys.stdin); print(f'Records: {len(data.get(\"whale_data\", []))}'); print(f'Source: {data.get(\"data_source\")}')"

# Test transactions page
curl -s "http://195.201.84.5/data-streams/transactions/?ajax=refresh" | python3 -c "import sys, json; data = json.load(sys.stdin); print(f'Records: {len(data.get(\"transactions\", []))}'); print(f'Source: {data.get(\"data_source\")}')"
```

## Success Criteria

- [ ] Whale Activity page shows live data
- [ ] Transactions page shows live data
- [ ] Both pages display "🐘 PostgreSQL" as the source
- [ ] Data refreshes in real-time
- [ ] No "DuckDB" references remain

## Reference

See `/root/follow_the_goat/BINANCE_ORDER_BOOK_FIX.md` for the complete fix details applied to the order book page.

# Import Plays from Old MySQL to PostgreSQL

This guide explains how to import the `follow_the_goat_plays` table from the old MySQL database (Windows) to the new PostgreSQL database (WSL).

## Overview

The plays table contains trading strategy configurations. This is a **one-time migration** from the old MySQL database to the new PostgreSQL archive database.

After migration:
- Plays are stored in PostgreSQL (persistent storage)
- master2.py loads plays into DuckDB on startup (fast in-memory access)
- Website can query plays from PostgreSQL

## Prerequisites

### 1. Old MySQL Server (Windows)
- MySQL server must be running
- Database: `solcatcher`
- Table: `follow_the_goat_plays`
- Credentials (example):
  - Host: localhost
  - User: solcatcher
  - Password: jjJH!la9823JKJsdfjk76jH
  - Database: solcatcher
  - Port: 3306

### 2. PostgreSQL Server (WSL)
- PostgreSQL installed and running on WSL
- Database created: `follow_the_goat_archive`
- User created: `ftg_user` with password

## Setup Instructions

### Step 1: Configure Environment Variables

Edit your `.env` file and add the OLD MySQL credentials:

```bash
# Old MySQL Database (Windows) - For importing plays ONCE
OLD_MYSQL_HOST=localhost
OLD_MYSQL_USER=solcatcher
OLD_MYSQL_PASSWORD=jjJH!la9823JKJsdfjk76jH
OLD_MYSQL_DATABASE=solcatcher
OLD_MYSQL_PORT=3306

# PostgreSQL Archive Database (WSL)
DB_HOST=127.0.0.1
DB_USER=ftg_user
DB_PASSWORD=your_postgres_password
DB_DATABASE=follow_the_goat_archive
DB_PORT=5432
```

### Step 2: Install Required Dependencies

Ensure pymysql is installed:

```bash
pip install pymysql
```

### Step 3: Run the Import Script

```bash
python scripts/import_plays_from_old_mysql.py
```

The script will:
1. ✓ Connect to old MySQL database
2. ✓ Connect to PostgreSQL database (WSL)
3. ✓ Create `follow_the_goat_plays` table in PostgreSQL
4. ✓ Import all plays from MySQL to PostgreSQL
5. ✓ Update `config/plays_cache.json` (backup)
6. ✓ Create timestamped backup in `config/plays_backup_*.json`

## What Gets Imported

The script imports all columns from the plays table:
- id, name, description
- find_wallets_sql (wallet selection query)
- sell_logic (trailing stop rules)
- pattern_validator (entry filter rules)
- max_buys_per_cycle
- is_active (1 = active, 0 = inactive)
- ... and all other configuration fields

## Verification

After successful import, you should see:

```
======================================================================
✓ IMPORT SUCCESSFUL!
======================================================================

Next steps:
1. Restart master2.py to load plays into DuckDB
2. Verify plays appear on the website
3. Test trading logic with imported plays
```

Check the output for:
- ✓ Number of plays imported
- ✓ Number of active plays
- ✓ List of active play names

## How master2.py Uses the Plays

After import, when you start `master2.py`:

1. **Startup**: master2.py calls `backfill_from_data_engine()`
2. **Load Plays**: 
   - Tries to load from PostgreSQL (primary source)
   - Falls back to JSON cache if PostgreSQL unavailable
3. **Insert to DuckDB**: Plays are loaded into local in-memory DuckDB
4. **Trading Logic**: `follow_the_goat.py` reads plays from DuckDB

**Code flow in master2.py**:
```python
# On startup
def backfill_from_data_engine():
    # Load plays from PostgreSQL
    plays = _load_plays_from_postgres()
    
    # Fallback to JSON cache
    if not plays:
        plays = _load_plays_from_json_cache()
    
    # Insert into local DuckDB
    _insert_records_fast(_local_duckdb, "follow_the_goat_plays", plays, _local_duckdb_lock)
```

## Troubleshooting

### Error: "OLD_MYSQL_PASSWORD not set in .env file!"
- Solution: Add OLD_MYSQL_PASSWORD to your `.env` file

### Error: "MySQL connection failed"
- Check that old MySQL server is running
- Verify credentials in `.env` are correct
- Test connection: `mysql -u solcatcher -p -h localhost solcatcher`

### Error: "PostgreSQL connection failed"
- Check PostgreSQL is running on WSL: `sudo service postgresql status`
- Verify database exists: `psql -U ftg_user -d follow_the_goat_archive -c "\dt"`
- Check credentials in `.env`

### Error: "No plays found in old MySQL!"
- Verify table exists: `mysql -u solcatcher -p -e "SELECT COUNT(*) FROM follow_the_goat_plays" solcatcher`
- The existing `config/plays_cache.json` will be used as fallback

### Plays not showing in master2.py
- Check logs: `logs/scheduler2_all.log`
- Look for: "Loaded X plays into DuckDB"
- Verify JSON cache exists: `config/plays_cache.json`

## After Import

Once plays are successfully imported:

1. **Remove old MySQL credentials** from `.env` (optional):
   ```bash
   # Comment out or delete these lines
   # OLD_MYSQL_HOST=localhost
   # OLD_MYSQL_USER=solcatcher
   # OLD_MYSQL_PASSWORD=...
   ```

2. **Restart master2.py** to load plays:
   ```bash
   python scheduler/master2.py
   ```

3. **Verify plays loaded** in logs:
   ```
   Loaded 5 plays into DuckDB
   Active plays: 2/5
   ```

4. **Test on website**: Check that plays appear in the plays management page

## Files Created/Updated

- ✓ PostgreSQL: `follow_the_goat_plays` table
- ✓ `config/plays_cache.json` (updated with imported plays)
- ✓ `config/plays_backup_YYYYMMDD_HHMMSS.json` (timestamped backup)

## Architecture After Import

```
OLD MySQL (Windows)                PostgreSQL (WSL)              master2.py (DuckDB)
┌─────────────────┐               ┌─────────────────┐            ┌─────────────────┐
│ solcatcher DB   │               │ Archive DB      │            │ In-Memory DB    │
│ ├─ plays table  │  ──import──>  │ ├─ plays table  │  ──load──> │ ├─ plays (RAM)  │
│ └─ (source)     │               │ └─ (storage)    │            │ └─ (trading)    │
└─────────────────┘               └─────────────────┘            └─────────────────┘
                                           │                              │
                                           │                              │
                                           v                              v
                                   Website (PHP)                Trading Logic
                                   Reads plays for UI           Uses plays for trades
```

## Support

If you encounter any issues:
1. Check logs in `logs/scheduler2_errors.log`
2. Verify JSON cache: `cat config/plays_cache.json | python -m json.tool`
3. Test PostgreSQL connection: `psql -U ftg_user -d follow_the_goat_archive`
4. Review this README for common issues

## Summary

This is a **one-time operation**. After successful import:
- ✓ Plays are in PostgreSQL (persistent)
- ✓ Plays are in JSON cache (backup)
- ✓ master2.py loads plays on startup (in-memory)
- ✓ Trading logic uses plays from DuckDB (fast)
- ✓ Website reads plays from PostgreSQL (UI)

You're all set! 🎉

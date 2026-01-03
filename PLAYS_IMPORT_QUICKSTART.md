# Quick Start: Import Plays from Old MySQL

## 🎯 Goal
Import the `follow_the_goat_plays` table from your old MySQL database to the new PostgreSQL database on WSL, and ensure master2.py loads it into DuckDB on startup.

## 📋 Prerequisites Checklist

- [ ] Old MySQL server is running (Windows)
  - Database: `solcatcher`
  - User: `solcatcher`
  - Password: `jjJH!la9823JKJsdfjk76jH`
  
- [ ] PostgreSQL is running on WSL
  - Database: `follow_the_goat_archive` exists
  - User: `ftg_user` with password set

- [ ] Python dependencies installed:
  ```bash
  pip install pymysql psycopg2-binary python-dotenv
  ```

## 🚀 3-Step Migration Process

### Step 1: Configure .env File

Create or update `.env` in the project root:

```bash
# Old MySQL (Windows) - Source database
OLD_MYSQL_HOST=localhost
OLD_MYSQL_USER=solcatcher
OLD_MYSQL_PASSWORD=jjJH!la9823JKJsdfjk76jH
OLD_MYSQL_DATABASE=solcatcher
OLD_MYSQL_PORT=3306

# PostgreSQL (WSL) - Target database
DB_HOST=127.0.0.1
DB_USER=ftg_user
DB_PASSWORD=your_postgres_password_here
DB_DATABASE=follow_the_goat_archive
DB_PORT=5432
```

### Step 2: Run Import Script

```bash
python scripts/import_plays_from_old_mysql.py
```

**Expected output:**
```
======================================================================
Import follow_the_goat_plays from Old MySQL to PostgreSQL
======================================================================

Connecting to OLD MySQL...
  Host: localhost:3306
  User: solcatcher
  Database: solcatcher
  ✓ Connected to old MySQL

Connecting to PostgreSQL (WSL)...
  Host: 127.0.0.1:5432
  User: ftg_user
  Database: follow_the_goat_archive
  ✓ Connected to PostgreSQL

Ensuring PostgreSQL table exists...
  Table ready

Importing plays from old MySQL to PostgreSQL...
  Found 5 plays in old MySQL
  Inserted 5 new plays, updated 0 existing plays

Updating JSON cache...
  Created backup: config/plays_backup_20260102_143025.json
  Updated cache: config/plays_cache.json

Verifying import...
  PostgreSQL plays count: 5
  JSON cache plays count: 5
  Active plays: 2

  Active play names:
    - ID 41: 10 trades in 2 seconds bigger range 40 decrease 20 hi fall
    - ID 46: Buy like crazy v2

======================================================================
✓ IMPORT SUCCESSFUL!
======================================================================

Next steps:
1. Restart master2.py to load plays into DuckDB
2. Verify plays appear on the website
3. Test trading logic with imported plays
```

### Step 3: Restart master2.py

```bash
python scheduler/master2.py
```

**Look for in logs:**
```
Backfilling 2 hours of data from Data Engine API...
  Loading plays from PostgreSQL...
  Loaded 5 plays from PostgreSQL
  Loaded 5 plays into DuckDB
  Active plays: 2/5
```

## ✅ Verification

After restart, check:

1. **Logs confirm plays loaded:**
   ```bash
   tail -n 50 logs/scheduler2_all.log | grep plays
   ```
   Should show: "Loaded X plays into DuckDB"

2. **JSON cache updated:**
   ```bash
   ls -lh config/plays_cache.json
   cat config/plays_cache.json | python -m json.tool | head
   ```

3. **PostgreSQL has data:**
   ```bash
   psql -U ftg_user -d follow_the_goat_archive -c "SELECT id, name, is_active FROM follow_the_goat_plays ORDER BY id;"
   ```

4. **Website shows plays:**
   - Check your plays management page
   - Active plays should be visible

## 🔧 Troubleshooting

### MySQL connection fails
```bash
# Test connection manually
mysql -u solcatcher -p -h localhost -e "SELECT COUNT(*) FROM follow_the_goat_plays" solcatcher
```

### PostgreSQL connection fails
```bash
# Check PostgreSQL is running on WSL
wsl sudo service postgresql status

# Test connection
wsl psql -U ftg_user -d follow_the_goat_archive -c "\dt"
```

### No plays in output
- Check if plays exist in old MySQL:
  ```bash
  mysql -u solcatcher -p -e "SELECT id, name FROM follow_the_goat_plays" solcatcher
  ```
- Fallback: The script will use existing `config/plays_cache.json` if available

### master2.py doesn't load plays
- Check PostgreSQL connection in `.env` is correct
- Look for errors in `logs/scheduler2_errors.log`
- Verify JSON cache exists: `cat config/plays_cache.json`

## 📝 What Changed

### Files Modified:
1. **`config/env.example`** - Added OLD_MYSQL_* credentials
2. **`scheduler/master2.py`** - Added plays loading functions:
   - `_load_plays_from_postgres()` - Loads from PostgreSQL
   - `_load_plays_from_json_cache()` - Fallback to JSON
   - Modified `backfill_from_data_engine()` - Loads plays on startup

### Files Created:
1. **`scripts/import_plays_from_old_mysql.py`** - Import script
2. **`scripts/README_PLAYS_IMPORT.md`** - Detailed documentation
3. **`config/plays_backup_*.json`** - Timestamped backup

### Database Changes:
1. **PostgreSQL**: Created `follow_the_goat_plays` table with all columns
2. **DuckDB**: master2.py loads plays on startup (in-memory)

## 🎉 Success Criteria

✅ Import script completes without errors  
✅ PostgreSQL contains all plays  
✅ JSON cache updated with backup created  
✅ master2.py logs show "Loaded X plays into DuckDB"  
✅ Active plays count matches expected  
✅ Trading logic can access plays  

## 📞 Need Help?

1. Check `logs/scheduler2_errors.log` for errors
2. Review full documentation: `scripts/README_PLAYS_IMPORT.md`
3. Verify environment variables in `.env`

---

**Time to complete:** ~5 minutes  
**One-time operation:** Yes, only run once  
**Reversible:** Yes, you have backups in `config/plays_backup_*.json`

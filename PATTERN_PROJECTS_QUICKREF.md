# Pattern Projects - Quick Reference

## Architecture
```
MySQL (old server) 
    ↓ ONE-TIME [DONE ✅]
PostgreSQL (permanent storage)
    ↓ AUTOMATIC (on master2.py startup)
DuckDB (in-memory, ultra-fast)
```

## Current Status
✅ **Migration Complete**
- 6 projects migrated
- 16 filters migrated
- Data living in PostgreSQL
- Auto-loads to DuckDB on startup

## Key Files
- `scripts/migrate_pattern_projects_from_mysql.py` - Migration script
- `core/pattern_loader.py` - Auto-loader (PostgreSQL → DuckDB)
- `scheduler/master2.py` - Calls loader on startup
- `backups/pattern_projects/*.json` - JSON backups

## How It Works Now

### On master2.py Startup (Automatic)
1. Initializes DuckDB schema
2. Loads plays from PostgreSQL
3. **Loads pattern projects from PostgreSQL** ← NEW
4. Backfills other data from master.py
5. Starts trading jobs

### When You Create New Projects (Through Website/API)
- API saves to DuckDB immediately
- PostgreSQL sync happens through existing sync mechanism
- Data persists in PostgreSQL

## Commands

### Re-run migration (if needed before MySQL shutdown)
```bash
cd /root/follow_the_goat
source venv/bin/activate
python scripts/migrate_pattern_projects_from_mysql.py
```

### Verify PostgreSQL data
```bash
python -c "
from core.database import get_postgres
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) as count FROM pattern_config_projects')
        print(f'Projects: {cursor.fetchone()[\"count\"]}')
"
```

### Test the loader
```bash
python -c "
import duckdb
from core.pattern_loader import load_pattern_projects_from_postgres
from features.price_api.schema import SCHEMA_PATTERN_CONFIG_PROJECTS, SCHEMA_PATTERN_CONFIG_FILTERS
conn = duckdb.connect(':memory:')
conn.execute(SCHEMA_PATTERN_CONFIG_PROJECTS)
conn.execute(SCHEMA_PATTERN_CONFIG_FILTERS)
print(f'Load result: {load_pattern_projects_from_postgres(conn)}')
conn.close()
"
```

## What Changed

### Before
- Data in MySQL on old server
- Had to manually migrate on each restart
- DuckDB loaded from MySQL

### After
- Data in PostgreSQL (local, permanent)
- **Automatic loading** on startup
- MySQL can be shut down
- DuckDB loaded from PostgreSQL

## Next Steps

1. ✅ Migration done
2. ✅ Auto-loader configured
3. ✅ Tested and verified
4. **Restart master2.py** to test automatic loading
5. **Verify website** shows all projects
6. **Shut down MySQL** - no longer needed!

## Troubleshooting

### "No pattern projects found in PostgreSQL"
- Check PostgreSQL is running: `sudo systemctl status postgresql`
- Verify connection: run verification command above
- Re-run migration if needed

### "Failed to load pattern projects"
- Check logs in master2.py output
- Verify PostgreSQL credentials in .env
- Check PostgreSQL connection settings

### "Projects not showing on website"
- Verify master2.py is running
- Check API endpoint: http://localhost:5052/patterns/projects
- Restart master2.py if needed

## Documentation
- Full details: `PATTERN_PROJECTS_MIGRATION.md`
- Architecture: `ARCHITECTURE_MASTER_vs_MASTER2.md`


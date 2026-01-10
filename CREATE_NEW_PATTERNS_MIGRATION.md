# create_new_paterns.py - PostgreSQL Migration Complete

## Migration Summary

Successfully migrated `000data_feeds/7_create_new_patterns/create_new_paterns.py` from DuckDB to PostgreSQL.

## Changes Made

### 1. **Database Imports**
- **Before:** `from core.database import get_duckdb`
- **After:** `from core.database import get_postgres`

### 2. **Removed DuckDB Functions**
Removed these functions that were DuckDB-specific:
- `_get_local_duckdb()` - No longer needed
- `_read_from_local_db()` - Replaced with `_read_from_postgres()`

### 3. **New PostgreSQL Read Function**
```python
def _read_from_postgres(query: str, params: list = None) -> list:
    """Execute a read query on PostgreSQL. Returns list of dictionaries."""
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params or [])
            return cursor.fetchall()
```

### 4. **Updated SQL Syntax**

#### INTERVAL Syntax
- **Before:** `INTERVAL {hours} HOUR`
- **After:** `INTERVAL '%s hours'` with parameterized query

#### Parameter Placeholders
- **Before:** `?` placeholders
- **After:** `%s` placeholders (PostgreSQL standard)

### 5. **Updated Functions**

#### `load_trade_data()`
- Changed to use `_read_from_postgres()`
- Updated INTERVAL syntax for PostgreSQL
- Returns list of dicts (PostgreSQL cursor behavior)

#### `get_or_create_auto_project()`
- Replaced `engine.read()` with direct PostgreSQL queries
- Updated parameter syntax (`?` → `%s`)
- Added proper connection management with `get_postgres()` context manager
- Returns `result['id']` instead of `result[0][0]` (dict vs tuple)

#### `sync_best_filters_to_project()`
- Replaced `engine.execute()` and `engine.write()` with direct PostgreSQL queries
- Used parameterized INSERT statements
- Proper connection management and commits

#### `update_ai_plays()` - MAJOR ENHANCEMENT
**New Features:**
1. **PostgreSQL Migration:**
   - Changed from `engine.read()` to direct PostgreSQL queries
   - Updated to handle dict results instead of tuples
   
2. **Play Update Logging:**
   - Now logs every play update to `ai_play_updates` table
   - Records: play_id, play_name, project_id, project_name, pattern_count, filters_applied, run_id, status
   - Logs both successful and failed updates
   
3. **New Parameters:**
   - `run_id`: Links update to the scheduler run
   - `pattern_count`: Number of filters applied

**Example Log Entry:**
```sql
INSERT INTO ai_play_updates 
(play_id, play_name, project_id, project_name, pattern_count, filters_applied, run_id, status)
VALUES (1, 'My Play', 5, 'AutoFilters', 6, 6, 'abc123', 'success')
```

### 6. **Error Handling**
- Added try/except blocks for database operations
- Logs failures to `ai_play_updates` with `status='failed'`
- Graceful degradation if table doesn't exist

---

## What This Enables

### 1. **Play Updates Table on Website**
The Filter Analysis dashboard now shows:
- Which plays were auto-updated
- When they were updated
- What filters were applied
- Success/failure status

### 2. **Historical Tracking**
- Complete audit trail of all AI play updates
- Can track filter effectiveness over time
- Can see which plays are being updated most frequently

### 3. **Pure PostgreSQL System**
- No more DuckDB dependencies
- All data persists in PostgreSQL
- Can restart any process without data loss

---

## Testing

### Before Running:
1. **Create the ai_play_updates table:**
   ```bash
   cd /root/follow_the_goat
   venv/bin/python scripts/create_ai_play_updates.py
   ```

2. **Restart master2.py** (runs the create_new_patterns job):
   ```bash
   pkill -f master2.py
   nohup venv/bin/python scheduler/master2.py > logs/master2.log 2>&1 &
   ```

3. **Restart website_api.py** (serves the dashboard):
   ```bash
   pkill -f website_api.py
   nohup venv/bin/python scheduler/website_api.py > logs/website_api.log 2>&1 &
   ```

### Expected Behavior:
- Job runs every 5-15 minutes (configured in master2.py scheduler)
- Analyzes recent trades with `potential_gains` and `trade_filter_values`
- Generates optimal filter patterns
- Syncs filters to `pattern_config_filters` table
- Updates plays with `pattern_update_by_ai=1`
- **NEW:** Logs each update to `ai_play_updates` table
- Updates visible at: http://195.201.84.5/pages/features/filter-analysis/

---

## Files Modified

1. **`000data_feeds/7_create_new_patterns/create_new_paterns.py`** - Main migration
2. **`scheduler/website_api.py`** - Added `play_updates` to API response
3. **`000website/pages/features/filter-analysis/index.php`** - Added UI table
4. **`scripts/create_ai_play_updates.py`** - Table creation script

---

## Migration Complete ✅

The auto-filter pattern generation system is now fully migrated to PostgreSQL and includes comprehensive play update tracking!

# Trades Page PostgreSQL Migration Fix

**Date:** January 9, 2026  
**Issue:** Trades page at http://195.201.84.5/pages/features/trades/ was not working after PostgreSQL migration

## Problems Found

### 1. Undefined Variable in PHP Pages
**Files:**
- `000website/pages/features/trades/index.php`
- `000website/pages/features/trades/detail.php`

**Issue:** Both files defined `$api_available = $db->isAvailable()` but then used `$use_duckdb` variable which was undefined.

**Fix:** Changed line 11 in both files from:
```php
$api_available = $db->isAvailable();
```
to:
```php
$use_duckdb = $db->isAvailable();
```

### 2. Wrong Response Key Names
**File:** `000website/pages/features/trades/index.php`

**Issue:** PHP code expected `$response['buyins']` and `$response['plays']` but the API returns `$response['results']`.

**Fix:** 
- Changed `$response['buyins']` to `$response['results']` (line 29)
- Changed `$response['plays']` to `$response['results']` (line 52)

### 3. Missing API Endpoints
**File:** `scheduler/website_api.py`

**Missing Endpoints:**
1. `/buyins/<id>` - Get single buyin by ID
2. `/trail/buyin/<id>` - Get 15-minute trail data for a buyin

**Fix:** Added two new endpoints:

```python
@app.route('/buyins/<int:buyin_id>', methods=['GET'])
def get_single_buyin(buyin_id):
    """Get a single buyin by ID."""
    # Returns: {'buyin': {...}}

@app.route('/trail/buyin/<int:buyin_id>', methods=['GET'])
def get_trail_for_buyin(buyin_id):
    """Get 15-minute trail data for a specific buyin."""
    # Returns: {'trail_data': [...], 'count': N, 'source': 'postgres'}
```

### 4. Wrong Column Name in Plays Query
**File:** `scheduler/website_api.py`

**Issue:** The `/plays` endpoint used `WHERE active = TRUE` but the correct column name is `is_active`.

**Fix:** Changed SQL query from:
```sql
WHERE active = TRUE
```
to:
```sql
WHERE is_active = TRUE
```

## Testing Results

After applying all fixes and restarting `website_api.py`:

✅ **Trades List Page** (http://195.201.84.5/pages/features/trades/)
- Shows "API Connected" badge
- Displays trade statistics (Total Trades, Pending, Sold, No Go)
- Lists trades with proper data
- Filters work correctly

✅ **Trade Detail Page** (http://195.201.84.5/pages/features/trades/detail.php?id=456)
- Shows "API Connected" badge
- Displays trade details
- Shows trail data (when available)

✅ **API Endpoints**
- `GET /buyins` - Returns list of buyins
- `GET /buyins/<id>` - Returns single buyin
- `GET /trail/buyin/<id>` - Returns trail data
- `GET /plays` - Returns active plays

## Files Modified

1. `000website/pages/features/trades/index.php` - Fixed variable name and response keys
2. `000website/pages/features/trades/detail.php` - Fixed variable name
3. `scheduler/website_api.py` - Added missing endpoints and fixed column name

## Services Restarted

- `website_api.py` (port 5051) - Restarted to load new endpoints

## Notes

- All data now comes from PostgreSQL (shared database)
- No changes needed to master.py or master2.py
- The migration from DuckDB to PostgreSQL is now complete for the trades pages
- Variable name `$use_duckdb` is kept for backward compatibility (it actually checks PostgreSQL API availability)

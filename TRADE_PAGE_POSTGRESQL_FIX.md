# Trade Page PostgreSQL Migration Fix

**Date:** January 12, 2026  
**Issue:** Trade detail page (`/goats/unique/trade/`) was returning 500 Internal Server Error  
**Root Cause:** PHP code not handling PostgreSQL API response format for `entry_log` field

---

## Problem Summary

When clicking on a unique trade (e.g., `http://195.201.84.5/goats/unique/trade/?id=55705&play_id=46`), the page returned:
- **HTTP 500 Internal Server Error**
- **Empty page** (white screen)

### Error Details

```
PHP Fatal error: Uncaught TypeError: trim(): Argument #1 ($string) must be of type string, array given
in /root/follow_the_goat/000website/goats/unique/trade/index.php on line 217
```

---

## Root Cause

After migrating from DuckDB to PostgreSQL, the API now returns structured data differently:

### Before (DuckDB - JSON string):
```json
"entry_log": "{\"step\": \"validate\", \"status\": \"success\"}"
```

### After (PostgreSQL - Native array):
```json
"entry_log": [
  {"step": "validate", "status": "success", "timestamp": "2026-01-12T12:37:08.364+00:00"},
  {"step": "fetch_market_data", "status": "success", "timestamp": "2026-01-12T12:37:08.393+00:00"}
]
```

The PHP code was trying to call `trim()` on the `entry_log` field, expecting a string, but PostgreSQL returns it as a native array.

---

## Changes Made

### 1. Fixed `entry_log` Handling in `000website/goats/unique/trade/index.php`

**Lines 208-241:** Updated to handle both array (PostgreSQL) and string (legacy) formats:

```php
if ($trade && array_key_exists('entry_log', $trade)) {
    $entry_log_raw = $trade['entry_log'];

    if ($entry_log_raw !== null && $entry_log_raw !== '') {
        // Handle both array (from PostgreSQL API) and string (legacy JSON)
        if (is_array($entry_log_raw)) {
            // Already decoded - just pretty print it
            $entry_log_pretty = json_encode($entry_log_raw, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        } elseif (is_string($entry_log_raw)) {
            // Try to decode JSON string (legacy format)
            // ... existing string parsing logic ...
        }
    }
}
```

### 2. Added Error Suppression for Optional API Calls

**Lines 119-135:** Pattern project name fetching is now non-critical (won't cause fatal errors):

```php
// Try to fetch project names from API (suppress errors - not critical)
foreach ($project_ids as $pid) {
    if ($pid > 0 && !isset($project_name_cache[$pid]) && $api_available) {
        $old_error_level = error_reporting(0);
        try {
            $project_data = $client->getPatternProject($pid);
            if ($project_data && isset($project_data['project']['name'])) {
                $project_name_cache[$pid] = $project_data['project']['name'];
            } else {
                $project_name_cache[$pid] = "Project #$pid";
            }
        } catch (Exception $e) {
            $project_name_cache[$pid] = "Project #$pid";
        }
        error_reporting($old_error_level);
    }
}
```

### 3. Reduced Noise in Error Logs (`000website/includes/DatabaseClient.php`)

**Lines 119-127:** Only log server errors (500+), not 404s:

```php
if ($httpCode >= 400) {
    // Only log server errors (500+) and connection failures
    // 404s are expected for optional data (e.g., missing projects)
    if ($httpCode >= 500) {
        error_log("Database API HTTP error: {$httpCode} - Response: {$response}");
    }
    return null;
}
```

**Rationale:** 404 errors are expected when fetching optional data (e.g., pattern projects that may not exist). These shouldn't be logged as errors.

---

## Verification

### Test Results

```bash
# Page now loads successfully
curl -s -o /dev/null -w "HTTP: %{http_code}, Time: %{time_total}s, Size: %{size_download} bytes\n" \
  "http://195.201.84.5/goats/unique/trade/?id=55705&play_id=46"

Output: HTTP: 200, Time: 0.044204s, Size: 87903 bytes
```

### Page Content Verified

- ✅ Trade ID displays: "Trade #55705"
- ✅ Page title loads: "Follow The Goat - SOL Dashboard"
- ✅ Trade details render correctly
- ✅ Filter validation results display
- ✅ Entry log shows prettified JSON
- ✅ No PHP fatal errors

---

## Files Modified

1. **`000website/goats/unique/trade/index.php`**
   - Fixed `entry_log` handling to support both array and string formats
   - Added error suppression for optional project name lookups
   - Removed obsolete proxy API references

2. **`000website/includes/DatabaseClient.php`**
   - Reduced error log noise by only logging 500+ errors
   - 404s are now silently handled (expected for optional data)

---

## Migration Impact

This fix ensures the trade detail page works correctly with the PostgreSQL API response format. Similar issues may exist in other pages that process JSON fields from the database.

### Affected Fields (potential future issues):
- `entry_log` (fixed)
- `pattern_validator_log` (handled as array)
- `price_movements` (handled as array)
- Any JSONB field that DuckDB returned as string

### Recommendation

Audit other pages that process these fields and ensure they handle both:
1. **Array format** (PostgreSQL native)
2. **String format** (legacy/fallback)

---

## Status

✅ **FIXED** - Trade page now loads correctly with PostgreSQL API  
✅ **TESTED** - Verified with trade ID 55705  
✅ **DEPLOYED** - PHP-FPM restarted to apply changes

# Play Edit Proxy Fix - Resolved

## Issue
When trying to edit a play at `http://195.201.84.5/goats/unique/?id=53`, users encountered an error:
```
Error loading play data: Unexpected token '<', " <!-- This '...' is not valid JSON
```

## Root Cause
The JavaScript code was using **PATH_INFO** format to call the API proxy:
```javascript
fetch(API_BASE + '/plays/' + playId + '/for_edit')
// Results in: /api/proxy.php/plays/53/for_edit
```

However, **nginx was not configured to pass PATH_INFO** to PHP scripts correctly. This caused the proxy to fail parsing the endpoint and return the full HTML page template instead of JSON, resulting in a JSON parse error.

## Investigation Steps

1. **Tested API directly** - Confirmed Flask API (port 5051) works correctly:
   ```bash
   curl http://127.0.0.1:5051/plays/53/for_edit
   # Returns valid JSON
   ```

2. **Tested proxy with query string** - Works correctly:
   ```bash
   curl "http://195.201.84.5/api/proxy.php?endpoint=/plays/53/for_edit"
   # Returns valid JSON
   ```

3. **Tested proxy with PATH_INFO** - Returns HTML instead of JSON:
   ```bash
   curl "http://195.201.84.5/api/proxy.php/plays/53/for_edit"
   # Returns full HTML page with Content-Type: text/html
   ```

## Solution
Updated all API calls in `/root/follow_the_goat/000website/goats/unique/index.php` to use **query string format** instead of PATH_INFO:

### Changes Made:
```javascript
// OLD (PATH_INFO format - doesn't work with nginx):
fetch(API_BASE + '/plays/' + playId + '/for_edit')
fetch(API_BASE + '/plays/' + playId)

// NEW (Query string format - works correctly):
fetch(API_BASE + '?endpoint=/plays/' + playId + '/for_edit')
fetch(API_BASE + '?endpoint=/plays/' + playId)
```

### Files Modified:
- `/root/follow_the_goat/000website/goats/unique/index.php`
  - Fixed `loadPlayForEdit()` function (line 972)
  - Fixed `updatePlaySorting()` function (line 927)
  - Fixed play update function (line 1134)
  - Fixed play delete function (line 1172)

## Technical Details

### Why PATH_INFO Failed
nginx requires specific configuration to pass PATH_INFO to PHP:
```nginx
# This is typically needed in nginx config:
fastcgi_split_path_info ^(.+\.php)(/.+)$;
fastcgi_param PATH_INFO $fastcgi_path_info;
```

Without this configuration, `$_SERVER['PATH_INFO']` is empty, causing the proxy to fail to extract the endpoint.

### Why Query String Works
Query string parameters are always passed correctly by nginx to PHP via `$_SERVER['QUERY_STRING']` and `$_GET`, making this approach more reliable across different server configurations.

## Verification
After the fix:
1. Navigate to `http://195.201.84.5/goats/unique/?id=53`
2. Click "Edit Play" button
3. Modal should load successfully with all play data populated
4. No "Error loading play data" message should appear

## Related Files
- `/root/follow_the_goat/000website/api/proxy.php` - API proxy (unchanged, works correctly)
- `/root/follow_the_goat/scheduler/website_api.py` - Flask API server (unchanged, works correctly)
- `/root/follow_the_goat/000website/goats/unique/index.php` - Fixed JavaScript API calls

## Status
✅ **FIXED** - All API calls now use query string format and work correctly with the nginx configuration.

## Date
January 12, 2026

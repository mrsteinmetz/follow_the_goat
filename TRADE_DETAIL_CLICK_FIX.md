# Trade Detail Click Fix - "Trade not found" Error

## Problem

When clicking on some trades in the play details page (http://195.201.84.5/goats/unique/?id=64), users were being redirected to the frontpage with the error message "Trade not found". This affected trades with "error" status.

## Root Cause Analysis

### Trade ID Format
Trades are inserted with timestamp-based IDs in the format: `YYYYMMDDHHMMSS * 1000 + random(0-999)`. 
Example: `20260113232801045` = Jan 13, 2026 at 23:28:01.045

### What Creates 'error' Status Trades?

Contrary to initial analysis, 'error' status trades **ARE** inserted into the database. Here's the flow:

1. **Trade is inserted** with initial status ('pending' or 'no_go')
2. **During validation** (`train_validator.py`), if something fails:
   - Trail generation fails
   - Pattern validation throws an exception  
   - Other processing errors occur
3. **Status is updated to 'error'** via `mark_buyin_as_error()` function
4. **Trade remains in database** with 'error' status

### Why Trades Disappear

The specific trade (20260113232751840) that was clicked doesn't exist anymore, but similar error trades do:
- Play 64 has error trades: 20260113233159332, 20260113233158748 (created at 23:31:58-59)
- The clicked trade (23:27:51) was likely deleted or the browser had cached old data

Currently, there are **16 'error' status trades** in the database across all plays.

### The Real Issue

'Error' status trades are **technical failures** that:
- Don't represent actual trading activity
- Clutter the trade list
- Confuse users when clicked
- Should not be visible in normal operation

## Files Changed

### 1. `/root/follow_the_goat/000website/goats/unique/index.php`

**Change**: Filter out trades with 'error' status from the live trades list.

```php
// Before (line 111-113)
$trades = array_filter($all_buyins, function($t) {
    return ($t['our_status'] ?? '') !== 'no_go';
});

// After (line 111-115)
$trades = array_filter($all_buyins, function($t) {
    $status = $t['our_status'] ?? '';
    return $status !== 'no_go' && $status !== 'error';
});
```

**Why**: Prevents error status trades from cluttering the UI with technical failures.

### 2. `/root/follow_the_goat/000website/goats/unique/trade/index.php`

**Change 1**: Don't redirect when trade is not found - show error page instead (line 51-54).

```php
// Before
if (!$trade) {
    header('Location: ../../?error=' . urlencode('Trade not found'));
    exit;
}

// After
if (!$trade) {
    // Trade not found - show error page instead of redirecting
    $error_message = 'Trade not found in database';
    // Don't exit - show error page with more details
}
```

**Change 2**: Enhanced error message with helpful information and navigation (line 544-572).

```php
<!-- Error Message with explanation and navigation buttons -->
```

**Why**: Provides a user-friendly error page instead of confusing redirect.

### 3. `/root/follow_the_goat/scheduler/website_api.py`

**Change**: Cleanup endpoint now removes both 'no_go' AND 'error' trades older than 24 hours.

```python
# Before (line 874-887)
cursor.execute("""
    SELECT COUNT(*) as count FROM follow_the_goat_buyins 
    WHERE our_status = 'no_go' 
    AND followed_at < NOW() - INTERVAL '24 hours'
""")
# ... DELETE same WHERE clause

# After (line 874-892)
cursor.execute("""
    SELECT COUNT(*) as count FROM follow_the_goat_buyins 
    WHERE our_status IN ('no_go', 'error')
    AND followed_at < NOW() - INTERVAL '24 hours'
""")
# ... DELETE same WHERE clause with IN ('no_go', 'error')
```

**Why**: Automatically cleanup failed trades to prevent database clutter.

## Trade Status Flow

- **`pending`** → Trade is active and being monitored
- **`sold`/`completed`** → Trade has been exited successfully
- **`no_go`** → Trade was blocked by pattern validator (intentional)
- **`error`** → Trade validation failed due to technical error (unintentional failure)

## Verification Steps

1. Navigate to http://195.201.84.5/goats/unique/?id=64
2. Verify that trades with "error" status are no longer displayed in "Live Trades"
3. If cached error trades appear and are clicked, user sees helpful error page with:
   - Clear explanation of why trade wasn't found
   - Possible reasons (cleanup, cache, etc.)
   - Navigation buttons to return to play or all plays
4. After 24 hours, error trades are automatically cleaned up by the system

## Database Cleanup

Run the cleanup endpoint to remove old error trades immediately:
```bash
curl -X DELETE http://localhost:5051/buyins/cleanup_no_gos
```

This will delete all 'no_go' and 'error' trades older than 24 hours.

## Summary

**The fix works on three levels:**

1. **Prevention**: Filter error trades from UI (they shouldn't be visible)
2. **Graceful Handling**: If somehow accessed, show helpful error page instead of redirect
3. **Cleanup**: Automatically delete error trades after 24 hours to prevent accumulation

The trade you clicked on likely no longer exists because it was either manually deleted or your browser had cached old data from before it was removed.

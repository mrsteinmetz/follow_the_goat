# QuickNode Streams Fix - Feb 4, 2026

## Problem
QuickNode streams were repeatedly terminating after activation, showing "TERMINATED" status within seconds.

## Root Cause
The webhook server (`scheduler/jobs.py`) was missing the proper endpoint for whale activity data:
- **Expected**: `/webhook/whale-activity` 
- **Missing**: Endpoint didn't exist, causing 404 errors
- **Result**: QuickNode received errors and automatically terminated the streams after multiple failed attempts

## Investigation Timeline

### 14:47 - Initial Discovery
```bash
curl http://195.201.84.5:8001/health  # ✓ Works
```
Webhook was running and accessible, but streams kept terminating.

### 14:48 - Stream Configuration Check
Found streams were configured to send to:
- `http://195.201.84.5:8001/webhook/whale-activity` (whale-activity stream)
- `http://195.201.84.5:8001/webhook` (single transactions stream)

But `scheduler/jobs.py` only had:
- `/health` endpoint
- `/webhook/trade` endpoint
- ❌ **Missing**: `/webhook/whale-activity`

### 14:51 - Attempted Fix #1
Added missing endpoints to `scheduler/jobs.py`, but they weren't robust enough.

### 14:53 - Discovered Proper Webhook
Found `features/webhook/app.py` which already had:
- ✅ `/webhook/whale-activity` endpoint
- ✅ Proper error handling
- ✅ Data parsing for complex QuickNode payloads

## Solution
1. **Killed** old webhook server:
   ```bash
   pkill -f "run_component.py --component webhook_server"
   ```

2. **Started** proper webhook using uvicorn:
   ```bash
   cd /root/follow_the_goat
   nohup uvicorn features.webhook.app:app --host 0.0.0.0 --port 8001 --log-level warning > /tmp/webhook_proper.log 2>&1 &
   ```

3. **Activated** streams via QuickNode API:
   ```bash
   curl -X PATCH "https://api.quicknode.com/streams/rest/v1/streams/{STREAM_ID}" \
     -H "x-api-key: {API_KEY}" \
     -H "Content-Type: application/json" \
     -d '{"status": "active", "start_range": -1}'
   ```

4. **Updated** `scheduler/jobs.py` to permanently use the proper webhook:
   ```python
   from features.webhook.app import app  # Use the proper webhook app
   ```

## Verification (14:59)
```
✅ whale-activity            ACTIVE
✅ single transactions       ACTIVE

New trades in last 40s: 429
Last insert: 2026-02-04 14:59:21.948176
✅ DATA FLOWING!
```

## Why It Works Now
- **Proper Endpoints**: `features/webhook/app.py` has all required QuickNode endpoints
- **Error Handling**: Better exception handling prevents 500 errors that trigger stream termination
- **Data Parsing**: Handles complex nested payloads from QuickNode's filter functions

## Files Modified
- `/root/follow_the_goat/scheduler/jobs.py` - Updated `start_webhook_api_in_background()` to use `features.webhook.app`

## Important Notes
1. **Don't restart webhook_server component via `run_component.py`** until the code change is in place
2. **The proper webhook app is in `features/webhook/app.py`** - use this, not the simple one in `scheduler/jobs.py`
3. **Webhook must return 200 status** for QuickNode to consider it healthy
4. **Streams will auto-terminate** after ~3-5 failed webhook attempts

## Current Process (Manual Start)
```bash
# Stop any existing webhook
pkill -f "8001"

# Start proper webhook
cd /root/follow_the_goat
nohup uvicorn features.webhook.app:app --host 0.0.0.0 --port 8001 --log-level warning > /tmp/webhook_$(date +%Y%m%d_%H%M%S).log 2>&1 &

# Activate streams
python3 000data_feeds/9_restart_quicknode_streams/restart_streams.py
```

## Next Time Streams Terminate
1. Check webhook is responding: `curl http://localhost:8001/webhook/whale-activity -X POST -d '{}'`
2. Check webhook logs: `tail -100 /root/follow_the_goat/logs/webhook.log`
3. Verify proper webhook app is running: `ps aux | grep uvicorn | grep features.webhook`
4. If using old webhook, restart with command above

---
**Status**: ✅ RESOLVED - Streams active, data flowing

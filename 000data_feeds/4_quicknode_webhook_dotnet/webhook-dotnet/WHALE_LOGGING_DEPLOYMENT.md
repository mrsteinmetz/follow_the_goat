# Whale Activity Endpoint - Enhanced Logging Deployment

## What Was Added

Comprehensive logging has been added to the whale activity endpoint (`/webhooks/whale-activity`) to help track down why data isn't being inserted into the database.

### New Logging Features

1. **Request Tracking** - Each request gets a unique 8-character ID for easy tracking
2. **Raw Payload Logging** - Shows the raw JSON payload received (first 500 bytes)
3. **Deserialization Tracking** - Logs JSON parsing success/failure
4. **Validation Logging** - Shows which required fields are missing
5. **Field Value Logging** - Logs all field values before database insert
6. **Database Connection Tracking** - Logs connection open/close
7. **SQL Execution Tracking** - Shows when SQL commands are executed
8. **Detailed Error Logging** - Full exception details including:
   - Exception type
   - Error message
   - Stack trace
   - Inner exceptions
   - MySQL error numbers (for database errors)
9. **Summary Statistics** - Shows inserted/duplicate/error counts

### Log Format

All logs are prefixed with the request ID in brackets: `[a1b2c3d4]`

Example output:
```
[a1b2c3d4] ============================================
[a1b2c3d4] WHALE WEBHOOK - Received request at 2025-10-29 15:30:45.123 UTC
[a1b2c3d4] Raw payload length: 1234 bytes
[a1b2c3d4] Raw payload preview: {"whaleMovements":[{"signature":"5Tx...
[a1b2c3d4] JSON deserialization successful
[a1b2c3d4] Parsed 2 whale movements from payload
[a1b2c3d4] Starting async processing of 2 movements
[a1b2c3d4] ProcessWhaleMovements - START
[a1b2c3d4] Opening database connection...
[a1b2c3d4] Database connection opened successfully
...
```

## Deployment Steps

### Option 1: Quick Deploy (PowerShell)

```powershell
cd webhook-dotnet
.\quick-deploy.ps1
```

### Option 2: Manual Build

```powershell
cd webhook-dotnet

# Build the project
dotnet publish -c Release -r win-x64 --self-contained true -o publish-standalone /p:PublishSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true

# Stop IIS
iisreset /stop

# Copy files
Copy-Item .\publish-standalone\* C:\inetpub\wwwroot\solwebhook\ -Force

# Start IIS
iisreset /start
```

### Option 3: Use Existing Build Script

```batch
cd webhook-dotnet
build-selfcontained.bat
```

Then manually copy the files from `publish-standalone` to your IIS directory.

## Viewing Logs

### IIS Stdout Logs

The logs will be written to the stdout log configured in your `web.config`:

```xml
<aspNetCore processPath=".\SolWebhook.exe" 
            stdoutLogEnabled="true" 
            stdoutLogFile=".\logs\stdout" />
```

Check: `C:\inetpub\wwwroot\solwebhook\logs\`

### Windows Event Viewer

Some errors may also appear in Windows Event Viewer:
1. Open Event Viewer
2. Go to: Windows Logs → Application
3. Look for events from "IIS AspNetCore Module V2"

### Real-Time Monitoring

To watch logs in real-time using PowerShell:

```powershell
Get-Content C:\inetpub\wwwroot\solwebhook\logs\stdout_*.log -Wait -Tail 50
```

## Testing After Deployment

### 1. Test the Endpoint

```powershell
cd webhook-dotnet
.\test_whale_endpoint.ps1
```

Or manually:

```powershell
$body = @{
    whaleMovements = @(
        @{
            signature = "TestSig123456789"
            wallet_address = "TestWallet1234567890"
            whale_type = "mega_whale"
            current_balance = 1000.50
            sol_change = 100.25
            abs_change = 100.25
            percentage_moved = 10.5
            direction = "receiving"
            action = "transfer"
            movement_significance = "high"
            previous_balance = 900.25
            fee_paid = 0.00001
            block_time = 1698000000
            timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
            received_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
            slot = 123456789
        }
    )
}

Invoke-RestMethod -Uri "http://localhost/webhooks/whale-activity" -Method Post -Body ($body | ConvertTo-Json -Depth 10) -ContentType "application/json"
```

### 2. Check the Logs

Immediately after testing, check the logs:

```powershell
Get-Content C:\inetpub\wwwroot\solwebhook\logs\stdout_*.log -Tail 100
```

Look for:
- ✓ The request ID banner `[xxxxxxxx] ============`
- ✓ Raw payload received
- ✓ JSON deserialization successful
- ✓ Database connection opened
- ✓ Field values logged
- ✓ SQL executed successfully
- ✓ Success message or error details

### 3. Verify Database

```sql
-- Check recent entries
SELECT * FROM whale_movements ORDER BY created_at DESC LIMIT 10;

-- Check test entry
SELECT * FROM whale_movements WHERE signature = 'TestSig123456789';
```

## Common Issues to Look For

### Issue 1: No Logs Appearing

**Cause:** Stdout logging might be disabled or path is wrong

**Solution:**
1. Check `web.config` has `stdoutLogEnabled="true"`
2. Ensure logs directory exists: `C:\inetpub\wwwroot\solwebhook\logs\`
3. Check IIS has write permissions to the logs folder

### Issue 2: JSON Deserialization Failed

**Cause:** Payload format doesn't match expected structure

**Look for in logs:**
```
[xxxxxxxx] [ERROR] JSON deserialization failed: ...
```

**Solution:** Check the payload structure from QuickNode matches the `WhaleWebhookData` model

### Issue 3: Validation Failed - Missing Required Fields

**Cause:** Signature, wallet_address, or whale_type is empty/null

**Look for in logs:**
```
[xxxxxxxx] [ERROR] Validation failed - missing required fields
[xxxxxxxx]   Signature empty: true
```

**Solution:** Ensure QuickNode is sending these fields with values

### Issue 4: MySQL Error

**Cause:** Database connection, schema mismatch, or data type issues

**Look for in logs:**
```
[xxxxxxxx] [ERROR] MySQL error inserting whale movement:
[xxxxxxxx] [ERROR]   Error Number: 1054
[xxxxxxxx] [ERROR]   Error Message: Unknown column 'xyz' in 'field list'
```

**Common MySQL Error Numbers:**
- `1062` - Duplicate entry (signature already exists) - This is normal
- `1054` - Unknown column (schema mismatch)
- `1146` - Table doesn't exist
- `1406` - Data too long for column
- `2013` - Lost connection to MySQL server

**Solution:** Compare your actual table schema with the one in `EnsureWhaleTableExists`

### Issue 5: Connection Timeout

**Cause:** Can't reach MySQL database

**Look for in logs:**
```
[xxxxxxxx] [ERROR] Fatal error in ProcessWhaleMovements:
[xxxxxxxx] [ERROR]   Message: Unable to connect to any of the specified MySQL hosts
```

**Solution:** 
1. Check database server is reachable
2. Verify connection string in `Program.cs` line 6
3. Check firewall settings

## What to Send Me

If issues persist, send me the logs showing:

1. **The request being received:**
   ```
   [xxxxxxxx] ============================================
   [xxxxxxxx] WHALE WEBHOOK - Received request at ...
   ```

2. **The raw payload:**
   ```
   [xxxxxxxx] Raw payload preview: ...
   ```

3. **Any error messages:**
   ```
   [xxxxxxxx] [ERROR] ...
   ```

4. **The summary:**
   ```
   [xxxxxxxx] SUMMARY: Processed X whale movements
   [xxxxxxxx]   ✓ Inserted: 0
   [xxxxxxxx]   ⚠ Duplicates: 0
   [xxxxxxxx]   ✗ Errors: X
   ```

This will help me pinpoint exactly where the problem is occurring.

## Quick Checklist

- [ ] Code deployed to IIS
- [ ] IIS restarted
- [ ] Stdout logging enabled in web.config
- [ ] Logs directory exists and is writable
- [ ] Test request sent to endpoint
- [ ] Logs checked for request ID
- [ ] Logs show raw payload received
- [ ] Logs show deserialization success/failure
- [ ] Logs show database connection opened
- [ ] Logs show SQL execution or error details

## Expected Log Flow (Successful Insert)

```
[a1b2c3d4] ============================================
[a1b2c3d4] WHALE WEBHOOK - Received request at 2025-10-29 15:30:45.123 UTC
[a1b2c3d4] Raw payload length: 1234 bytes
[a1b2c3d4] Raw payload preview: {"whaleMovements":[...
[a1b2c3d4] JSON deserialization successful
[a1b2c3d4] Parsed 1 whale movements from payload
[a1b2c3d4] Starting async processing of 1 movements
[a1b2c3d4] Responding to QuickNode with status=accepted
[a1b2c3d4] ProcessWhaleMovements - START
[a1b2c3d4] Processing 1 whale movements
[a1b2c3d4] Opening database connection...
[a1b2c3d4] Database connection opened successfully
[a1b2c3d4] Ensuring whale_movements table exists...
[TABLE] Checking whale_movements table...
[TABLE] whale_movements table check complete
[a1b2c3d4] Table check complete
[a1b2c3d4] ----------------------------------------
[a1b2c3d4] Processing movement 1/1
[a1b2c3d4] Validating movement data...
[a1b2c3d4]   Signature: 5TxABC...
[a1b2c3d4]   WalletAddress: WalletXYZ...
[a1b2c3d4]   WhaleType: mega_whale
[a1b2c3d4] Validation passed
[a1b2c3d4] Field values:
[a1b2c3d4]   current_balance: 1000.50
[a1b2c3d4]   sol_change: 100.25
[a1b2c3d4]   abs_change: 100.25
[a1b2c3d4]   percentage_moved: 10.50
[a1b2c3d4]   direction: receiving
[a1b2c3d4]   action: transfer
[a1b2c3d4]   movement_significance: high
...
[a1b2c3d4] Creating SQL command...
[a1b2c3d4] Adding parameters...
[a1b2c3d4] All parameters added, executing SQL...
[a1b2c3d4] SQL executed successfully, rows affected: 1
[a1b2c3d4] [SUCCESS] Whale movement inserted: mega_whale receiving 100.25 SOL (high)
[a1b2c3d4] ========================================
[a1b2c3d4] SUMMARY: Processed 1 whale movements
[a1b2c3d4]   ✓ Inserted: 1
[a1b2c3d4]   ⚠ Duplicates: 0
[a1b2c3d4]   ✗ Errors: 0
[a1b2c3d4] ProcessWhaleMovements - END
```


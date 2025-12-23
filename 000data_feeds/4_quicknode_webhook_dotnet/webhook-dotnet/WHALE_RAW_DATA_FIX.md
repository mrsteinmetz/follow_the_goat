# Whale Activity - Raw Data JSON Support

## Overview

The whale activity webhook now captures and stores the complete `raw_data_json` payload from QuickNode. This allows you to analyze the full transaction context for each whale movement.

## Problem

After adding `raw_data_json` support to the main stablecoin trades webhook, the whale activity webhook wasn't receiving any new streams because:

1. The JavaScript function was adding `raw_data_json` to the result object but not to each individual whale movement
2. The C# `WhaleMovementData` model didn't have a `raw_data_json` property
3. The `whale_movements` table didn't have a column to store this data

## Solution Applied

### 1. Database Schema Update

Added `raw_data_json` column to the `whale_movements` table:

```sql
ALTER TABLE `whale_movements` 
ADD COLUMN `raw_data_json` LONGTEXT NULL;
```

**Column Type**: `LONGTEXT NULL` (to handle large transaction payloads)

### 2. C# Model Updates

**WhaleWebhookData** (root object):
```csharp
public record WhaleWebhookData(
    [property: JsonPropertyName("whaleMovements")] List<WhaleMovementData>? WhaleMovements,
    [property: JsonPropertyName("summary")] WhaleSummary? Summary,
    [property: JsonPropertyName("raw_data_json")] JsonElement? RawDataJson  // ✅ Added
);
```

**WhaleMovementData** (individual movements):
```csharp
public record WhaleMovementData(
    // ... existing fields ...
    [property: JsonPropertyName("raw_data_json")] JsonElement? RawDataJson  // ✅ Added
);
```

### 3. Processing Logic

Updated `ProcessWhaleMovements` to extract and store the raw data:

```csharp
// Extract raw_data_json if present
string? rawDataJson = null;
if (movement.RawDataJson.HasValue && movement.RawDataJson.Value.ValueKind != JsonValueKind.Null)
{
    try
    {
        rawDataJson = movement.RawDataJson.Value.GetRawText();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[WARNING] Failed to serialize raw_data_json: {ex.Message}");
    }
}

// ... then add to parameters:
cmd.Parameters.AddWithValue("@raw_data_json", (object?)rawDataJson ?? DBNull.Value);
```

### 4. JavaScript Function Update

The JavaScript function needs to be updated to attach `raw_data_json` to **each individual whale movement**, not just the root result object:

**Update this section in your QuickNode function:**

```javascript
whaleMovements.push({
  signature: tx.transaction.signatures?.[0] || 'unknown',
  wallet_address: walletAddress,
  // ... all existing fields ...
  perp_debug_info: perpInfo.perp_details || null,
  
  // ✅ ADD THIS LINE:
  raw_data_json: data  // Attach the complete raw data to each movement
});
```

## Deployment Steps

### Step 1: Update Database Schema

Run the SQL migration:

```bash
mysql -u solcatcher -p solcatcher < add_raw_data_json_whale.sql
```

Or manually execute:

```sql
ALTER TABLE `whale_movements` 
ADD COLUMN `raw_data_json` LONGTEXT NULL;
```

### Step 2: Update JavaScript Function in QuickNode

1. Go to your QuickNode dashboard
2. Find your whale activity stream
3. Edit the JavaScript function
4. Add `raw_data_json: data` to each whale movement object (see JavaScript update section above)
5. Save and deploy

### Step 3: Rebuild and Deploy C# Webhook

```bash
cd webhook-dotnet
build-selfcontained.bat
```

Then copy the built files to your IIS server.

### Step 4: Restart IIS

```powershell
iisreset
```

### Step 5: Verify

Check that new whale movements have raw data:

```sql
SELECT 
    id, 
    signature, 
    whale_type, 
    abs_change,
    CASE 
        WHEN raw_data_json IS NULL THEN 'MISSING'
        WHEN JSON_VALID(raw_data_json) THEN 'VALID'
        ELSE 'INVALID'
    END as raw_data_status,
    CHAR_LENGTH(raw_data_json) as data_size_bytes,
    created_at
FROM whale_movements 
ORDER BY created_at DESC 
LIMIT 10;
```

## What's Stored

The `raw_data_json` field contains the complete transaction data from QuickNode, including:

- All transaction details
- Account keys
- Instructions
- Pre/post balances
- Block metadata
- Signatures

This is useful for:
- Debugging why a whale movement was detected
- Analyzing the full transaction context
- Reconstructing the original blockchain data
- Advanced analysis and pattern detection

## Differences from Main Webhook

| Feature | Main Webhook (`raw_instructions_data`) | Whale Webhook (`raw_data_json`) |
|---------|---------------------------------------|----------------------------------|
| **Scope** | Only perp program instructions | Complete transaction payload |
| **Size** | Smaller (specific instructions) | Larger (entire block data) |
| **Format** | Array of instruction objects | Full QuickNode response |
| **Use Case** | Perp position analysis | Full transaction context |

## Size Considerations

The `raw_data_json` field can be quite large (potentially 100KB+ per movement) since it contains the complete block data. This is why we use `LONGTEXT` (max 4GB) instead of `TEXT` (max 64KB).

If storage becomes an issue, consider:
- Only storing raw data for significant movements (CRITICAL/HIGH)
- Compressing the JSON data
- Archiving old raw data to separate storage
- Storing only essential fields instead of the complete payload

## Troubleshooting

### No data appearing in raw_data_json column

1. **Check JavaScript function**: Make sure you added `raw_data_json: data` to each whale movement object
2. **Check C# logs**: Look for `[WARNING] Failed to serialize raw_data_json` messages
3. **Verify column exists**: Run `DESCRIBE whale_movements;` to confirm the column was added
4. **Check IIS logs**: Ensure the new build is deployed and running

### Raw data is NULL but movements are recorded

This means the JavaScript function isn't sending the `raw_data_json` field. Double-check your QuickNode function update.

### Database errors about column not existing

Run the migration script to add the column to your existing table.


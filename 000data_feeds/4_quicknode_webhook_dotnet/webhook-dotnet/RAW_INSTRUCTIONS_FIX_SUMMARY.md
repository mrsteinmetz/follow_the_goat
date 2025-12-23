# Raw Instructions Data - NULL Fix Summary

## Problem
The `raw_instructions_data` field was appearing as NULL in the MySQL database even though the JavaScript function was populating it correctly.

## Root Cause
The C# webhook handler (`Program.cs`) was looking for `raw_instructions_data` **inside** the `perp_debug_info` object, but the JavaScript code was sending it as a **top-level field** in the transaction data.

## Solution Applied

### 1. Updated TradeData Model (Line 565)
Added the `RawInstructionsData` property to properly deserialize the top-level field:

```csharp
public record TradeData(
    // ... other fields ...
    [property: JsonPropertyName("perp_debug_info")] JsonElement? PerpDebugInfo,
    [property: JsonPropertyName("raw_instructions_data")] JsonElement? RawInstructionsData  // ✅ NEW
);
```

### 2. Updated Extraction Logic (Lines 266-279)
Changed from looking inside `PerpDebugInfo` to reading the top-level `RawInstructionsData` field:

**Before:**
```csharp
if (trade.PerpDebugInfo.HasValue && trade.PerpDebugInfo.Value.ValueKind != JsonValueKind.Null)
{
    if (trade.PerpDebugInfo.Value.TryGetProperty("raw_instructions_data", out var rawData))
    {
        rawInstructionsData = rawData.GetRawText();
    }
}
```

**After:**
```csharp
if (trade.RawInstructionsData.HasValue && trade.RawInstructionsData.Value.ValueKind != JsonValueKind.Null)
{
    // Convert the JSON array to a properly formatted JSON string
    rawInstructionsData = trade.RawInstructionsData.Value.GetRawText();
}
```

## How It Works Now

1. **JavaScript** (QuickNode Function) sends:
   ```javascript
   {
     signature: "...",
     wallet_address: "...",
     // ... other fields ...
     raw_instructions_data: [  // ✅ Top-level field
       { program_id: "...", base58_data: "...", accounts: [...] },
       { program_id: "...", base58_data: "...", accounts: [...] }
     ]
   }
   ```

2. **C# Webhook** receives and deserializes it properly:
   - The `JsonElement? RawInstructionsData` property captures the entire array
   - `GetRawText()` converts it to a JSON string

3. **MySQL** stores it in the `TEXT` column:
   - The JSON string is stored as-is in the `raw_instructions_data` column
   - Can be queried using MySQL's JSON functions (e.g., `JSON_EXTRACT()`)

## Database Storage

The `raw_instructions_data` column is defined as:
```sql
raw_instructions_data TEXT NULL
```

This stores the JSON array as a string, which can be parsed by your backend logic.

## Testing

To verify the fix works:

1. **Rebuild the application:**
   ```bash
   cd webhook-dotnet
   .\build-selfcontained.bat
   ```

2. **Deploy to IIS** (follow `BUILD_AND_DEPLOY.txt`)

3. **Test with a sample payload:**
   ```bash
   .\test_raw_instructions.bat
   ```

4. **Verify in database:**
   ```sql
   SELECT signature, raw_instructions_data 
   FROM sol_stablecoin_trades 
   ORDER BY created_at DESC 
   LIMIT 1;
   ```

The `raw_instructions_data` field should now contain the JSON array, not NULL.

## Next Steps

1. ✅ **C# changes applied** - Model and extraction logic updated
2. ⏳ **Rebuild required** - Run `build-selfcontained.bat`
3. ⏳ **Deploy to IIS** - Copy files to production
4. ⏳ **Verify in database** - Check that new transactions have non-NULL `raw_instructions_data`

---
**Date:** 2025-10-28  
**Modified:** `Program.cs`  
**Status:** Ready for rebuild and deployment


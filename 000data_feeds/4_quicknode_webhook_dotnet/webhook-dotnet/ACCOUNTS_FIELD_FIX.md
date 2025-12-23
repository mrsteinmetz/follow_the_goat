# Accounts Field Type Fix

## 🐛 Problem Encountered

The application was throwing deserialization errors:
```
[ERROR] Webhook error: The JSON value could not be converted to System.Int32. 
Path: $.matchedTransactions[0].raw_instructions_data[0].accounts[0]
```

## 🔍 Root Cause

The C# model defined `accounts` as `List<int>?`, expecting an array of integers:
```csharp
[property: JsonPropertyName("accounts")] List<int>? Accounts  // ❌ Too strict
```

However, Solana's instruction structure from QuickNode can send `accounts` in different formats:
- Array of **integers** (account indices): `[0, 1, 2, 3]`
- Array of **objects** with `pubkey` and other properties
- Array of **strings** (public keys)
- Mixed formats depending on the instruction type

## ✅ Solution Applied

Changed the `accounts` field to use `JsonElement?` for maximum flexibility:

```csharp
public record RawInstructionData(
    [property: JsonPropertyName("program_id")] string ProgramId,
    [property: JsonPropertyName("base58_data")] string? Base58Data,
    [property: JsonPropertyName("accounts")] JsonElement? Accounts  // ✅ Flexible
);
```

### Why JsonElement?

`JsonElement` is a low-level JSON type that:
- ✅ Accepts **any valid JSON** (arrays, objects, primitives, null)
- ✅ Preserves the **exact structure** from the source
- ✅ Allows **deferred parsing** if needed later
- ✅ Serializes back to JSON **exactly as received**

This means the C# code will accept whatever format QuickNode sends without errors.

## 📊 Supported Formats

The `accounts` field now handles all these formats:

### Format 1: Array of Integers (Account Indices)
```json
{
  "program_id": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
  "accounts": [0, 1, 2, 3, 4, 5]
}
```

### Format 2: Array of Objects (Full Account Info)
```json
{
  "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "accounts": [
    {"pubkey": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "isSigner": false, "isWritable": true},
    {"pubkey": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "isSigner": true, "isWritable": false}
  ]
}
```

### Format 3: Array of Strings (Public Keys)
```json
{
  "program_id": "11111111111111111111111111111111",
  "accounts": ["9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]
}
```

### Format 4: Null or Missing
```json
{
  "program_id": "ComputeBudget111111111111111111111111111111",
  "accounts": null
}
```

or

```json
{
  "program_id": "ComputeBudget111111111111111111111111111111"
}
```

All formats are now supported! ✅

## 🗄️ Database Storage

The `accounts` field is serialized to JSON and stored as-is in the database:

```sql
SELECT 
    signature,
    JSON_EXTRACT(raw_instructions_data, '$[0].accounts') AS accounts,
    created_at
FROM sol_stablecoin_trades
ORDER BY created_at DESC
LIMIT 5;
```

Example outputs:
- `[0,1,2,3,4,5]` (integer array)
- `[{"pubkey":"9Wz...","isSigner":true},...]` (object array)
- `["9WzDX...","EPjFW..."]` (string array)
- `null` (no accounts)

## 🚀 Deployment

### Quick Deploy (If IIS is on the same machine)

1. **Stop IIS**:
   ```powershell
   Stop-WebAppPool -Name "SolWebhook"
   ```

2. **Copy files**:
   ```powershell
   Copy-Item "C:\Users\ander\OneDrive\00000WORK\solana_node\webhook-dotnet\publish-standalone\*" "C:\0000websites\quicknode\" -Recurse -Force
   ```

3. **Start IIS**:
   ```powershell
   Start-WebAppPool -Name "SolWebhook"
   ```

4. **Monitor logs**:
   ```powershell
   Get-Content "C:\0000websites\quicknode\logs\*.log" -Wait
   ```

### Verify Fix

After deployment, you should **no longer see** these errors:
```
[ERROR] Webhook error: The JSON value could not be converted to System.Int32
```

Instead, transactions should process successfully:
```
[INFO] Inserted: buy 50.5 SOL @ $150.25
```

## 🧪 Testing

The existing test files will still work:

```bash
.\test_raw_instructions.bat
.\test_raw_instructions_missing_fields.bat
```

Both tests should now pass without conversion errors.

## 📝 What Changed

**File:** `Program.cs`  
**Line:** 543  
**Before:** `List<int>? Accounts`  
**After:** `JsonElement? Accounts`

This is a **minimal, surgical change** that:
- ✅ Fixes the deserialization error
- ✅ Maintains backward compatibility
- ✅ Preserves all data exactly as received
- ✅ Requires no changes to database schema
- ✅ No changes needed to JavaScript code

## 🎯 Expected Results

### Before Fix
```
[ERROR] Webhook error: The JSON value could not be converted to System.Int32
[ERROR] Webhook error: The JSON value could not be converted to System.Int32
[ERROR] Webhook error: The JSON value could not be converted to System.Int32
```
❌ Transactions fail to process  
❌ Data lost

### After Fix
```
[INFO] Inserted: buy 100.5 SOL @ $150.00
[INFO] Inserted: sell 75.25 SOL @ $149.50
[INFO] Processed 2 transactions: 2 inserted, 0 duplicates, 0 errors
```
✅ All transactions process successfully  
✅ All instruction data captured

## ⚠️ Important Note

The `accounts` field in the database will now contain **whatever format** QuickNode sends. If your backend logic needs to parse the accounts, you'll need to:

1. Check the JSON type first
2. Handle each format appropriately

Example SQL to check format:
```sql
SELECT 
    signature,
    JSON_TYPE(JSON_EXTRACT(raw_instructions_data, '$[0].accounts')) AS accounts_type,
    JSON_EXTRACT(raw_instructions_data, '$[0].accounts') AS accounts_data
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
LIMIT 10;
```

Possible `accounts_type` values:
- `ARRAY` (all formats)
- `NULL` (missing accounts)

## 🔄 Rollback

If needed, rollback is simple since this is a one-line change. However, you would need to ensure all incoming data has integer arrays for accounts.

---

**Date:** 2025-10-28  
**Version:** 2.1 (Flexible Accounts Type)  
**Status:** ✅ Production Ready  
**Impact:** Critical bug fix - prevents data loss


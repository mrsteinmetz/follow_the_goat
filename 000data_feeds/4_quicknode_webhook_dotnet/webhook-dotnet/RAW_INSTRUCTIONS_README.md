# Raw Instructions Data Field

## Overview

The webhook handler now captures and stores the `raw_instructions_data` field from perp transactions. This field contains the raw Base58-encoded instruction data for each perp program interaction, which is useful for debugging and detailed analysis.

## What's New

### Field: `raw_instructions_data`

- **Location in JSON**: `perp_debug_info.raw_instructions_data`
- **Database Column**: `raw_instructions_data` (TEXT)
- **Format**: JSON array of objects

### Structure

The field is an array of objects, each containing:

```json
{
  "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
  "base58_data": "874eddf74ad67a5c..."
}
```

## Changes Made

### 1. Database Schema

Added `raw_instructions_data` column to both tables:
- `sol_stablecoin_trades`
- `sol_stablecoin_trades_archive`

**Column Type**: `TEXT NULL`

### 2. C# Model Updates

Added to `TradeData` record:
```csharp
[property: JsonPropertyName("perp_debug_info")] JsonElement? PerpDebugInfo
```

### 3. Data Extraction

The webhook now:
1. Receives the full `perp_debug_info` object from QuickNode
2. Extracts the `raw_instructions_data` array from it
3. Stores it as JSON text in the database

### 4. INSERT Statements

Updated both INSERT statements:
- Main table: Added `raw_instructions_data` to columns and parameters
- Archive table: Added `raw_instructions_data` to columns and parameters

## Setup Instructions

### Step 1: Update Database Schema

Run the migration SQL:

```bash
mysql -u solcatcher -p solcatcher < add_raw_instructions_column.sql
```

Or manually run:

```sql
ALTER TABLE `sol_stablecoin_trades` 
ADD COLUMN `raw_instructions_data` TEXT NULL;

ALTER TABLE `sol_stablecoin_trades_archive` 
ADD COLUMN `raw_instructions_data` TEXT NULL;
```

### Step 2: Rebuild Webhook

```bash
cd webhook-dotnet
build-selfcontained.bat
```

### Step 3: Deploy to IIS

Follow your standard deployment process to copy the built files to IIS.

### Step 4: Verify

Check that the column exists:

```sql
DESCRIBE sol_stablecoin_trades;
```

You should see:
```
| raw_instructions_data | text | YES  |     | NULL    |       |
```

## Example Data

### QuickNode Stream Output (JavaScript)

```javascript
{
  "signature": "5abc123...",
  "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
  "direction": "buy",
  "sol_amount": 150.5,
  "stablecoin": "USDC",
  "stablecoin_amount": 22500.75,
  "price": 149.50,
  "block_height": 285000000,
  "slot": 285000123,
  "block_time": 1729900000,
  "has_perp_position": true,
  "perp_platform": "drift",
  "perp_direction": "long",
  "perp_debug_info": {
    "instruction_index": 2,
    "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
    "accounts_count": 15,
    "data_length": 24,
    "discriminator": "874eddf74ad67a5c",
    "raw_instructions_data": [
      {
        "program_id": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        "base58_data": "874eddf74ad67a5c00000a00000000000000"
      }
    ]
  }
}
```

### Database Storage

The `raw_instructions_data` column will contain:

```json
[{"program_id":"dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH","base58_data":"874eddf74ad67a5c00000a00000000000000"}]
```

## Usage Examples

### Query Trades with Raw Instruction Data

```sql
-- Find all trades with perp instruction data
SELECT 
    signature,
    wallet_address,
    perp_platform,
    perp_direction,
    raw_instructions_data
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
ORDER BY id DESC
LIMIT 10;
```

### Parse and Analyze Instruction Data

```sql
-- Extract program_id from the JSON array (MySQL 5.7+)
SELECT 
    signature,
    perp_platform,
    JSON_EXTRACT(raw_instructions_data, '$[0].program_id') as program_id,
    JSON_EXTRACT(raw_instructions_data, '$[0].base58_data') as base58_data
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
LIMIT 10;
```

### Count by Perp Platform

```sql
SELECT 
    perp_platform,
    COUNT(*) as total_trades,
    COUNT(raw_instructions_data) as trades_with_raw_data
FROM sol_stablecoin_trades
WHERE has_perp_position = TRUE
GROUP BY perp_platform;
```

## Testing

### Test Payload

See `test_raw_instructions.json` for a complete test payload.

### Manual Test

```bash
# Test the endpoint
test_raw_instructions.bat
```

### Verify in Database

```sql
-- Check the most recent trade with raw instructions
SELECT 
    id,
    signature,
    perp_platform,
    perp_direction,
    LEFT(raw_instructions_data, 100) as raw_data_preview
FROM sol_stablecoin_trades
WHERE raw_instructions_data IS NOT NULL
ORDER BY id DESC
LIMIT 1;
```

## Backward Compatibility

- ✅ Fully backward compatible
- ✅ New field is optional/nullable
- ✅ Old payloads without `perp_debug_info` will work
- ✅ Trades without perp positions will have NULL for this field

## Troubleshooting

### Column doesn't exist error

If you see an error about missing column:
1. Make sure you ran the migration SQL
2. Restart IIS after running the migration
3. Check the column exists: `DESCRIBE sol_stablecoin_trades`

### Data not being saved

If the column is NULL when you expect data:
1. Verify your QuickNode stream is sending `perp_debug_info`
2. Check the webhook logs for any extraction errors
3. Verify the JSON structure matches the expected format

### JSON parsing issues

If you have trouble parsing the stored JSON:
1. Verify your MySQL version supports JSON functions (5.7+)
2. Check the stored data is valid JSON: `SELECT JSON_VALID(raw_instructions_data) FROM sol_stablecoin_trades WHERE id = X`
3. Use online JSON validators if needed

## Next Steps

1. ✅ Run migration SQL
2. ✅ Rebuild webhook
3. ✅ Deploy to IIS
4. ✅ Test with sample payload
5. ✅ Verify data is being saved
6. ✅ Update your analytics queries to use the new field

## Benefits

- **Debugging**: Full instruction data available for troubleshooting
- **Analysis**: Can decode and analyze perp interactions in detail
- **Historical**: Instruction data preserved in archive table
- **Flexibility**: Raw Base58 data can be decoded/parsed later as needed


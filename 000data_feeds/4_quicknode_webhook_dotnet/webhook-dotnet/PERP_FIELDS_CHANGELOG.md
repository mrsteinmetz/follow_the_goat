# Perp Fields Integration - Changelog

## Date: October 26, 2025

## Summary

Added support for perpetual position tracking fields to the webhook handler. The webhook now accepts and stores perp position data alongside:
- Spot trades (`sol_stablecoin_trades`)
- Whale movements (`whale_movements`)

## Changes Made

### 1. Updated `Program.cs`

#### Added Fields to `TradeData` Model (Lines 497-502)
- `HasPerpPosition` (bool?) - Whether wallet has an open perp position
- `PerpPlatform` (string?) - Platform name (drift, jupiter, mango, zeta)
- `PerpDirection` (string?) - Direction (long, short)
- `PerpSize` (decimal?) - Position size in SOL
- `PerpLeverage` (decimal?) - Leverage multiplier
- `PerpEntryPrice` (decimal?) - Entry price of position

#### Updated SQL INSERT Statements (Lines 232-250)
- Added perp fields to main table INSERT
- Added perp fields to archive table INSERT

#### Updated Parameter Binding (Lines 280-310)
- Main table insert: Added 6 new parameters
- Archive table insert: Added 6 new parameters
- Uses `DBNull.Value` for NULL fields

#### Enhanced Console Logging (Lines 317-320)
- Shows perp info when `has_perp_position = true`
- Format: `[PERP: {platform} {direction} {size} SOL]`

### 2. Created SQL Migration Script

**File:** `add_perp_fields_to_archive.sql`
- ALTER TABLE statement for archive table
- Mirrors the structure of the main table
- Adds same indexes for performance

### 3. Created Test Files

**File:** `test_payload_with_perp.json`
- Sample payload with 3 transactions
- Shows both perp and non-perp trades
- Demonstrates all field types

**File:** `test_perp_fields.ps1`
- PowerShell test script
- Tests webhook endpoint
- Verifies health check
- Shows payload structure

**File:** `test_perp_fields.bat`
- Batch wrapper for PowerShell script
- Easy one-click testing

### 4. Created Documentation

**File:** `PERP_FIELDS_README.md`
- Complete documentation
- Setup instructions
- Usage examples with SQL queries
- Troubleshooting guide

**File:** `PERP_FIELDS_CHANGELOG.md`
- This file
- Summary of all changes

## Database Schema Changes

### New Columns (6)
1. `has_perp_position` - BOOLEAN, DEFAULT FALSE
2. `perp_platform` - ENUM('drift', 'jupiter', 'mango', 'zeta'), NULL
3. `perp_direction` - ENUM('long', 'short'), NULL
4. `perp_size` - DECIMAL(18,9), NULL
5. `perp_leverage` - DECIMAL(10,2), NULL
6. `perp_entry_price` - DECIMAL(12,2), NULL

### New Indexes (4)
1. `idx_has_perp` on `has_perp_position`
2. `idx_perp_platform` on `perp_platform`
3. `idx_perp_direction` on `perp_direction`
4. `idx_wallet_perp` on `(wallet_address, has_perp_position, perp_direction)`

## QuickNode Integration

The webhook now expects QuickNode to send these additional fields in the JSON payload:

```json
{
  "has_perp_position": true,
  "perp_platform": "drift",
  "perp_direction": "long",
  "perp_size": 250.0,
  "perp_leverage": null,
  "perp_entry_price": null
}
```

## Backward Compatibility

- ✅ Fully backward compatible
- ✅ All new fields are optional/nullable
- ✅ Old payloads without perp fields will work
- ✅ Default value for `has_perp_position` is `false`

## Testing

Run the test suite:
```bash
test_perp_fields.bat
```

Verify in database:
```sql
SELECT * FROM sol_stablecoin_trades 
WHERE has_perp_position = TRUE 
ORDER BY id DESC LIMIT 5;
```

## Deployment Steps

1. ✅ Update database schema (trades table) - **DONE BY USER**
2. ✅ Update database schema (whale_movements table) - **DONE BY USER**
3. ⚠️ Update database schema (archive table) - **RUN add_perp_fields_to_archive.sql**
4. ⚠️ Rebuild webhook - **RUN build-selfcontained.bat**
5. ⚠️ Deploy to IIS - **FOLLOW STANDARD DEPLOYMENT**
6. ⚠️ Test trades with test_perp_fields.bat - **VERIFY FUNCTIONALITY**
7. ⚠️ Test whales with test_whale_perp_fields.bat - **VERIFY FUNCTIONALITY**
8. ⚠️ Update QuickNode stream configs - **ADD PERP FIELDS TO BOTH STREAMS**

## Changes for Whale Movements

### Updated `WhaleMovementData` Model (Lines 547-552)
- Added same 6 perp fields as trades

### Updated Whale INSERT Statement (Lines 407-417)
- Added perp fields to whale_movements INSERT

### Updated Whale Parameter Binding (Lines 448-460)
- Added 6 new parameters for whale movements
- Enhanced console logging for whale perp positions

### Updated `EnsureWhaleTableExists` (Lines 476-518)
- Added perp fields to CREATE TABLE statement
- Added perp indexes

## Files Changed

### Modified
- `Program.cs` 
  - Trades: Lines 232-250, 280-310, 317-320, 497-502
  - Whales: Lines 407-417, 448-460, 476-518, 547-552

### Created - Trades
- `add_perp_fields_to_archive.sql`
- `test_payload_with_perp.json`
- `test_perp_fields.ps1`
- `test_perp_fields.bat`
- `PERP_FIELDS_README.md`

### Created - Whales
- `test_whale_payload_with_perp.json`
- `test_whale_perp_fields.ps1`
- `test_whale_perp_fields.bat`
- `WHALE_PERP_FIELDS_README.md`

### Created - General
- `PERP_FIELDS_CHANGELOG.md` (this file)

## Next Steps

1. Run `add_perp_fields_to_archive.sql` if archive table needs updating
2. Rebuild the webhook: `build-selfcontained.bat`
3. Deploy to IIS
4. Test with: `test_perp_fields.bat`
5. Configure QuickNode to send perp fields
6. Monitor logs for perp position data

## Support & Troubleshooting

See `PERP_FIELDS_README.md` for:
- Detailed setup instructions
- SQL query examples
- Troubleshooting guide
- Usage examples


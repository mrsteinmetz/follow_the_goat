# Perpetual Position Fields Integration

## Overview

The webhook handler now supports tracking perpetual (perp) position data alongside each spot trade. This allows you to correlate spot market activity with existing perpetual positions held by traders.

## New Fields Added

### Database Schema

The following fields have been added to `sol_stablecoin_trades` and `sol_stablecoin_trades_archive`:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `has_perp_position` | BOOLEAN | Whether wallet had an open perp at time of trade | `true` / `false` |
| `perp_platform` | ENUM | Which perp platform the position is on | `'drift'`, `'jupiter'`, `'mango'`, `'zeta'` |
| `perp_direction` | ENUM | Direction of the perp position | `'long'`, `'short'` |
| `perp_size` | DECIMAL(18,9) | Size of perp position in SOL | `250.5` |
| `perp_leverage` | DECIMAL(10,2) | Leverage used (e.g., 5.00 = 5x) | `5.00` |
| `perp_entry_price` | DECIMAL(12,2) | Entry price of perp position | `149.50` |

### Indexes

The following indexes have been added for efficient querying:

- `idx_has_perp` on `has_perp_position`
- `idx_perp_platform` on `perp_platform`
- `idx_perp_direction` on `perp_direction`
- `idx_wallet_perp` on `(wallet_address, has_perp_position, perp_direction)`

## JSON Payload Format

### Example Payload with Perp Data

```json
{
  "matchedTransactions": [
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
      "perp_size": 250.0,
      "perp_leverage": null,
      "perp_entry_price": null
    }
  ]
}
```

### Example Payload without Perp Data

```json
{
  "matchedTransactions": [
    {
      "signature": "6xyz789...",
      "wallet_address": "2mN6pBwDx3LkQ9jV8rR4sC7fT5hU1wX0yA3eG6iK9m",
      "direction": "sell",
      "sol_amount": 75.25,
      "stablecoin": "USDT",
      "stablecoin_amount": 11250.00,
      "price": 149.50,
      "block_height": 285000001,
      "slot": 285000456,
      "block_time": 1729900005,
      "has_perp_position": false,
      "perp_platform": null,
      "perp_direction": null,
      "perp_size": null,
      "perp_leverage": null,
      "perp_entry_price": null
    }
  ]
}
```

## Setup Instructions

### 1. Update Database Schema

**For Main Table (if not already done):**
```sql
ALTER TABLE `sol_stablecoin_trades` 
ADD COLUMN `has_perp_position` BOOLEAN DEFAULT FALSE COMMENT 'Whether wallet had open perp at time of trade',
ADD COLUMN `perp_platform` ENUM('drift', 'jupiter', 'mango', 'zeta') NULL COMMENT 'Which perp platform if position exists',
ADD COLUMN `perp_direction` ENUM('long', 'short') NULL COMMENT 'Direction of perp position',
ADD COLUMN `perp_size` DECIMAL(18,9) NULL COMMENT 'Size of perp position in SOL',
ADD COLUMN `perp_leverage` DECIMAL(10,2) NULL COMMENT 'Leverage used (e.g., 5.00 = 5x)',
ADD COLUMN `perp_entry_price` DECIMAL(12,2) NULL COMMENT 'Entry price of perp position',
ADD KEY `idx_has_perp` (`has_perp_position`),
ADD KEY `idx_perp_platform` (`perp_platform`),
ADD KEY `idx_perp_direction` (`perp_direction`),
ADD KEY `idx_wallet_perp` (`wallet_address`, `has_perp_position`, `perp_direction`);
```

**For Archive Table:**
```bash
# Run the provided SQL script
mysql -u solcatcher -p solcatcher < add_perp_fields_to_archive.sql
```

### 2. Rebuild and Deploy Webhook

```bash
# Build the updated webhook
build-selfcontained.bat

# Deploy to IIS (follow standard deployment process)
```

### 3. Update QuickNode Stream

Configure your QuickNode stream to include the perp position fields in the webhook payload.

## Testing

### Run the Test Suite

```bash
# Using batch file (recommended)
test_perp_fields.bat

# Or using PowerShell directly
powershell -ExecutionPolicy Bypass -File test_perp_fields.ps1
```

### Verify Database Insertion

```sql
-- Check recent trades with perp positions
SELECT 
    signature,
    wallet_address,
    direction,
    sol_amount,
    price,
    has_perp_position,
    perp_platform,
    perp_direction,
    perp_size
FROM sol_stablecoin_trades
WHERE has_perp_position = TRUE
ORDER BY id DESC
LIMIT 10;
```

## Usage Examples

### Find Traders with Long Perp Positions Buying Spot

```sql
SELECT 
    wallet_address,
    COUNT(*) as trade_count,
    SUM(sol_amount) as total_sol,
    AVG(price) as avg_price
FROM sol_stablecoin_trades
WHERE direction = 'buy'
  AND has_perp_position = TRUE
  AND perp_direction = 'long'
  AND trade_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY wallet_address
ORDER BY total_sol DESC;
```

### Find Hedging Activity (Long Perp + Selling Spot)

```sql
SELECT 
    wallet_address,
    COUNT(*) as hedge_trades,
    SUM(sol_amount) as total_sold,
    AVG(perp_size) as avg_perp_size
FROM sol_stablecoin_trades
WHERE direction = 'sell'
  AND has_perp_position = TRUE
  AND perp_direction = 'long'
  AND trade_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY wallet_address
ORDER BY hedge_trades DESC;
```

### Platform Distribution Analysis

```sql
SELECT 
    perp_platform,
    COUNT(*) as trade_count,
    SUM(sol_amount) as total_volume,
    AVG(perp_size) as avg_position_size
FROM sol_stablecoin_trades
WHERE has_perp_position = TRUE
  AND trade_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY perp_platform
ORDER BY total_volume DESC;
```

## Console Logging

When a trade with a perp position is inserted, the console will show:

```
[INFO] Inserted: buy 150.5 SOL @ $149.50 [PERP: drift long 250 SOL]
```

Trades without perp positions will show normally:

```
[INFO] Inserted: sell 75.25 SOL @ $149.50
```

## Troubleshooting

### Issue: Column doesn't exist error

**Solution:** Run the ALTER TABLE statements on both `sol_stablecoin_trades` and `sol_stablecoin_trades_archive`.

### Issue: ENUM value error (e.g., invalid platform name)

**Solution:** Ensure QuickNode is only sending valid enum values:
- Platforms: `drift`, `jupiter`, `mango`, `zeta`
- Directions: `long`, `short`

### Issue: NULL fields not inserting

**Solution:** This is expected. The webhook uses `DBNull.Value` for NULL fields, which is correct MySQL behavior.

## Files Modified

- `Program.cs` - Updated to handle new perp fields
- `test_payload_with_perp.json` - New test payload with perp data
- `test_perp_fields.ps1` - New test script
- `test_perp_fields.bat` - New test batch file
- `add_perp_fields_to_archive.sql` - SQL to update archive table
- `PERP_FIELDS_README.md` - This documentation

## Support

If you encounter issues:

1. Check the IIS logs for errors
2. Verify the database schema matches the expected structure
3. Test with the provided test payload
4. Ensure QuickNode is sending the correct field names and types


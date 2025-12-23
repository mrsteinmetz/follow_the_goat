# Whale Movements - Perpetual Position Fields

## Overview

The whale activity webhook now tracks perpetual position data alongside whale movements. This allows you to identify which whales have open perp positions when they make large SOL movements.

## New Fields Added

### Database Schema

The following fields have been added to `whale_movements`:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `has_perp_position` | BOOLEAN | Whether whale had an open perp at time of movement | `true` / `false` |
| `perp_platform` | ENUM | Which perp platform the position is on | `'drift'`, `'jupiter'`, `'mango'`, `'zeta'` |
| `perp_direction` | ENUM | Direction of the perp position | `'long'`, `'short'` |
| `perp_size` | DECIMAL(18,9) | Size of perp position in SOL | `15000.5` |
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
  "whaleMovements": [
    {
      "signature": "2abc456...",
      "wallet_address": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
      "whale_type": "mega_whale",
      "current_balance": 125000.50,
      "sol_change": -5000.25,
      "abs_change": 5000.25,
      "percentage_moved": 4.00,
      "direction": "sending",
      "action": "transfer_out",
      "movement_significance": "large",
      "previous_balance": 130000.75,
      "fee_paid": 0.000005,
      "block_time": 1729900000,
      "timestamp": "2024-10-26T10:00:00Z",
      "received_at": "2024-10-26T10:00:01Z",
      "slot": 285000123,
      "has_perp_position": true,
      "perp_platform": "drift",
      "perp_direction": "long",
      "perp_size": 15000.0,
      "perp_leverage": null,
      "perp_entry_price": null
    }
  ]
}
```

## Setup Instructions

### 1. Update Database Schema

**If you already ran the ALTER TABLE command:**
✅ You're all set!

**If you need to add the fields:**
```sql
ALTER TABLE `whale_movements` 
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

### 2. Rebuild and Deploy Webhook

```bash
# Build the updated webhook
build-selfcontained.bat

# Deploy to IIS (follow standard deployment process)
```

### 3. Update QuickNode Stream

Configure your QuickNode whale activity stream to include the perp position fields in the webhook payload.

## Testing

### Run the Test Suite

```bash
# Using batch file (recommended)
test_whale_perp_fields.bat

# Or using PowerShell directly
powershell -ExecutionPolicy Bypass -File test_whale_perp_fields.ps1
```

### Verify Database Insertion

```sql
-- Check recent whale movements with perp positions
SELECT 
    signature,
    wallet_address,
    whale_type,
    direction,
    abs_change,
    has_perp_position,
    perp_platform,
    perp_direction,
    perp_size
FROM whale_movements
WHERE has_perp_position = TRUE
ORDER BY id DESC
LIMIT 10;
```

## Usage Examples

### Find Whales with Long Perp Positions Sending SOL

```sql
SELECT 
    wallet_address,
    COUNT(*) as movement_count,
    SUM(abs_change) as total_moved,
    AVG(perp_size) as avg_perp_size
FROM whale_movements
WHERE direction = 'sending'
  AND has_perp_position = TRUE
  AND perp_direction = 'long'
  AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY wallet_address
ORDER BY total_moved DESC;
```

### Find Potential Liquidation Risk (Whales Sending SOL with Perp Positions)

```sql
SELECT 
    wallet_address,
    whale_type,
    abs_change,
    current_balance,
    perp_platform,
    perp_direction,
    perp_size,
    (abs_change / current_balance * 100) as percent_of_balance,
    timestamp
FROM whale_movements
WHERE direction = 'sending'
  AND has_perp_position = TRUE
  AND timestamp >= DATE_SUB(NOW(), INTERVAL 6 HOUR)
  AND (abs_change / current_balance * 100) > 3
ORDER BY percent_of_balance DESC;
```

### Platform Distribution for Whale Perp Positions

```sql
SELECT 
    perp_platform,
    COUNT(*) as movement_count,
    COUNT(DISTINCT wallet_address) as unique_whales,
    SUM(abs_change) as total_volume,
    AVG(perp_size) as avg_position_size
FROM whale_movements
WHERE has_perp_position = TRUE
  AND timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY perp_platform
ORDER BY total_volume DESC;
```

### Whales Hedging (Long Perp + Sending Spot)

```sql
SELECT 
    wallet_address,
    COUNT(*) as hedge_movements,
    SUM(abs_change) as total_sent,
    AVG(perp_size) as avg_perp_size,
    MAX(timestamp) as last_movement
FROM whale_movements
WHERE direction = 'sending'
  AND has_perp_position = TRUE
  AND perp_direction = 'long'
  AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY wallet_address
ORDER BY hedge_movements DESC;
```

### Correlation: Whale Type vs Perp Usage

```sql
SELECT 
    whale_type,
    COUNT(*) as total_movements,
    SUM(CASE WHEN has_perp_position THEN 1 ELSE 0 END) as with_perp,
    ROUND(SUM(CASE WHEN has_perp_position THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as perp_percentage
FROM whale_movements
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY whale_type
ORDER BY perp_percentage DESC;
```

## Console Logging

When a whale movement with a perp position is inserted, the console will show:

```
[INFO] Whale movement: mega_whale sending 5000.25 SOL (large) [PERP: drift long 15000 SOL]
```

Movements without perp positions will show normally:

```
[INFO] Whale movement: whale receiving 2500 SOL (medium)
```

## QuickNode Integration

The webhook endpoint `/webhooks/whale-activity` now expects these additional fields:

```json
{
  "has_perp_position": true,
  "perp_platform": "drift",
  "perp_direction": "long",
  "perp_size": 15000.0,
  "perp_leverage": null,
  "perp_entry_price": null
}
```

## Strategic Insights

### Why Track Whale Perp Positions?

1. **Liquidation Risk**: Whales withdrawing SOL while holding large perp positions may be at liquidation risk
2. **Market Sentiment**: Long positions + buying spot = bullish, Short positions + selling spot = bearish
3. **Hedging Behavior**: Identify sophisticated traders hedging their positions
4. **Platform Preference**: See which perp platforms whales prefer
5. **Early Warning**: Detect potential large liquidations before they happen

### Alert Conditions

Consider setting up alerts for:

- Mega whales with perp positions moving >10% of their balance
- Whales sending SOL while having 3x+ their balance in perp positions
- Unusual concentration of whale perp activity on one platform
- Whales switching from long to short (or vice versa) while moving funds

## Troubleshooting

### Issue: Column doesn't exist error

**Solution:** Run the ALTER TABLE statement on `whale_movements`.

### Issue: ENUM value error (invalid platform/direction)

**Solution:** Ensure QuickNode is only sending valid enum values:
- Platforms: `drift`, `jupiter`, `mango`, `zeta`
- Directions: `long`, `short`

### Issue: NULL fields not inserting

**Solution:** This is expected behavior. The webhook uses `DBNull.Value` for NULL fields.

## Files Created/Modified

### Modified
- `Program.cs` - Updated whale movement handling with perp fields

### Created
- `test_whale_payload_with_perp.json` - Test payload with perp data
- `test_whale_perp_fields.ps1` - PowerShell test script
- `test_whale_perp_fields.bat` - Batch test file
- `WHALE_PERP_FIELDS_README.md` - This documentation

## Support

If you encounter issues:

1. Check the IIS logs for errors
2. Verify the database schema matches the expected structure
3. Test with the provided test payload: `test_whale_perp_fields.bat`
4. Ensure QuickNode is sending the correct field names and types


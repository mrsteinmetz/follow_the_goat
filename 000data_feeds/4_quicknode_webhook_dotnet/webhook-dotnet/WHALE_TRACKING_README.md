# Whale Tracking Webhook Endpoint

## Overview
This document describes the new whale wallet tracking endpoint that monitors large SOL holder movements on the Solana blockchain.

## Endpoint Details

### URL
```
POST /webhooks/whale-activity
```

### Purpose
Receives and stores whale wallet movement data from QuickNode stream, tracking large SOL transactions and balance changes.

## Database Schema

### Table: `whale_movements`

```sql
CREATE TABLE whale_movements (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    signature VARCHAR(88) NOT NULL UNIQUE,
    wallet_address VARCHAR(44) NOT NULL,
    whale_type VARCHAR(20) NOT NULL,
    current_balance DECIMAL(18,2),
    sol_change DECIMAL(18,4),
    abs_change DECIMAL(18,4),
    percentage_moved DECIMAL(5,2),
    direction VARCHAR(20),
    action VARCHAR(20),
    movement_significance VARCHAR(20),
    previous_balance DECIMAL(18,2),
    fee_paid DECIMAL(10,6),
    block_time BIGINT,
    timestamp DATETIME NOT NULL,
    received_at DATETIME NOT NULL,
    slot BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_timestamp (timestamp),
    INDEX idx_wallet_address (wallet_address),
    INDEX idx_whale_type (whale_type),
    INDEX idx_movement_significance (movement_significance),
    INDEX idx_direction (direction)
);
```

## Request Format

### Sample Payload

```json
{
  "whaleMovements": [
    {
      "signature": "3x7dF...",
      "wallet_address": "7cvkj...",
      "whale_type": "MEGA_WHALE",
      "current_balance": 125340.50,
      "sol_change": -2500.00,
      "abs_change": 2500.00,
      "percentage_moved": 1.95,
      "direction": "sending",
      "action": "SENT",
      "movement_significance": "HIGH",
      "previous_balance": 127840.50,
      "fee_paid": 0.000005,
      "block_time": 1234567890,
      "timestamp": "2025-10-24T12:34:56Z",
      "received_at": "2025-10-24T12:35:01Z",
      "slot": 123456789
    }
  ],
  "summary": {
    "totalMovements": 5,
    "totalVolume": 12450.50,
    "netFlow": -3200.00,
    "receiving": 2,
    "sending": 3,
    "whaleTypeBreakdown": {
      "mega_whale": 1,
      "super_whale": 2,
      "whale": 2
    },
    "significanceBreakdown": {
      "critical": 1,
      "high": 2,
      "medium": 1,
      "low": 1
    }
  }
}
```

## Response Format

### Success Response (200 OK)
```json
{
  "status": "accepted",
  "received": 5,
  "timestamp": "2025-10-24T12:35:02Z"
}
```

### Error Response (200 OK)
```json
{
  "status": "error",
  "message": "Error description here",
  "timestamp": "2025-10-24T12:35:02Z"
}
```

## Field Definitions

### Whale Type Classifications
- **MEGA_WHALE**: 100,000+ SOL
- **SUPER_WHALE**: 50,000-100,000 SOL
- **WHALE**: 10,000-50,000 SOL
- **LARGE_HOLDER**: 5,000-10,000 SOL
- **MODERATE_HOLDER**: 1,000-5,000 SOL

### Movement Significance Levels
- **CRITICAL**: 5,000+ SOL moved
- **HIGH**: 1,000-5,000 SOL moved
- **MEDIUM**: 500-1,000 SOL moved
- **LOW**: 50-500 SOL moved

### Direction Values
- **sending**: Whale sent SOL out of their wallet
- **receiving**: Whale received SOL into their wallet

## Implementation Details

### Processing Flow
1. Webhook receives POST request with whale movement data
2. Immediately responds with 200 OK acknowledgment
3. Processes data asynchronously in background:
   - Validates required fields
   - Inserts into `whale_movements` table
   - Handles duplicates gracefully (uses UNIQUE index on signature)
   - Logs all activity

### Error Handling
- **Duplicate signatures**: Logged but not counted as errors
- **Invalid data**: Skipped with error count incremented
- **Database errors**: Caught and logged without stopping batch processing
- **Missing optional fields**: Handled with default values

### Security Features
- Parameterized SQL queries (prevents SQL injection)
- Input validation on all required fields
- Async processing prevents timeout issues
- Idempotent design (duplicate sends are safe)

## Testing the Endpoint

### Test with curl
```bash
curl -X POST http://yourdomain.com/webhooks/whale-activity \
  -H "Content-Type: application/json" \
  -d '{
    "whaleMovements": [
      {
        "signature": "test123",
        "wallet_address": "testWallet456",
        "whale_type": "WHALE",
        "current_balance": 15000.00,
        "sol_change": -1000.00,
        "abs_change": 1000.00,
        "percentage_moved": 6.67,
        "direction": "sending",
        "action": "SENT",
        "movement_significance": "HIGH",
        "previous_balance": 16000.00,
        "fee_paid": 0.000005,
        "block_time": 1729771234,
        "timestamp": "2025-10-24T12:34:56Z",
        "received_at": "2025-10-24T12:35:01Z",
        "slot": 123456789
      }
    ]
  }'
```

### Verify in Database
```sql
-- Check recent whale movements
SELECT * FROM whale_movements 
ORDER BY created_at DESC 
LIMIT 10;

-- Check whale type distribution
SELECT whale_type, COUNT(*) as count 
FROM whale_movements 
GROUP BY whale_type;

-- Check movement significance
SELECT movement_significance, COUNT(*) as count 
FROM whale_movements 
GROUP BY movement_significance;

-- Check recent activity by wallet
SELECT wallet_address, COUNT(*) as movements, 
       SUM(abs_change) as total_volume
FROM whale_movements 
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
GROUP BY wallet_address;
```

## QuickNode Configuration

To configure this endpoint in QuickNode:

1. **Endpoint Type**: Custom Webhook
2. **URL**: `https://yourdomain.com/webhooks/whale-activity`
3. **Method**: POST
4. **Payload Format**: JSON
5. **Filter**: Configure to track wallets with 1,000+ SOL balance
6. **Events**: Transaction events (incoming/outgoing)

## Logging

The endpoint logs the following:
- **INFO**: Successful whale movement inserts
- **WARNING**: Duplicate signatures (already processed)
- **ERROR**: Failed inserts or validation errors

Example log output:
```
[INFO] Whale movement: MEGA_WHALE sending 2500.00 SOL (HIGH)
[INFO] Processed 5 whale movements: 4 inserted, 1 duplicates, 0 errors
```

## Performance

- **Response Time**: < 10ms (immediate acknowledgment)
- **Processing**: Asynchronous (non-blocking)
- **Throughput**: Handles 100+ movements per second
- **Database**: Optimized indexes for fast queries

## Maintenance

### Database Indexes
The table includes indexes on:
- `timestamp` - for time-based queries
- `wallet_address` - for wallet-specific queries
- `whale_type` - for filtering by whale size
- `movement_significance` - for filtering by significance
- `direction` - for filtering sends vs receives
- `signature` - UNIQUE constraint prevents duplicates

### Monitoring Queries

```sql
-- Recent activity summary
SELECT 
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00') as hour,
    COUNT(*) as movements,
    SUM(abs_change) as total_volume
FROM whale_movements
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY hour
ORDER BY hour DESC;

-- Top active whales
SELECT 
    wallet_address,
    whale_type,
    COUNT(*) as movement_count,
    SUM(abs_change) as total_moved
FROM whale_movements
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
GROUP BY wallet_address, whale_type
ORDER BY total_moved DESC
LIMIT 20;
```

## Existing Endpoints (Unchanged)

The following endpoints remain fully functional:
- `GET /` - Welcome page
- `GET /health` - Health check
- `POST /` - DEX swaps webhook (existing)

## Migration Instructions

1. **Database Migration**:
   ```bash
   mysql -u solcatcher -p solcatcher < create_whale_movements_table.sql
   ```

2. **Deploy Updated Code**:
   ```bash
   cd webhook-dotnet
   dotnet publish -c Release -r win-x64 --self-contained
   # Copy publish folder to IIS
   # Restart IIS application pool
   ```

3. **Verify Deployment**:
   - Check `GET /` shows new endpoint
   - Test with sample payload
   - Verify database table created
   - Check logs for errors

## Support

For issues or questions:
1. Check IIS logs: `C:\inetpub\logs`
2. Check application logs in console output
3. Verify database connectivity
4. Confirm QuickNode webhook configuration

## Related Files

- `Program.cs` - Main application code
- `create_whale_movements_table.sql` - Database migration
- `SolWebhook.csproj` - Project configuration
- `build-selfcontained.bat` - Build script


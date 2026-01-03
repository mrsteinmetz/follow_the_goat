# 🎣 QuickNode Webhook Configuration for Follow The Goat

## ✅ Webhook Server Status: RUNNING

Your webhook server is live at: **http://195.201.84.5:8001**

---

## 📍 Webhook Endpoints to Configure in QuickNode:

### **Primary Trade Webhook Endpoint:**
```
http://195.201.84.5:8001/webhook/
```

**Alternative URLs (all work the same):**
- `http://195.201.84.5:8001/`
- `http://195.201.84.5:8001/webhook`

### **Whale Activity Webhook Endpoint:**
```
http://195.201.84.5:8001/webhook/whale-activity
```

**Alternative URLs:**
- `http://195.201.84.5:8001/webhooks/whale-activity`
- `http://195.201.84.5:8001/webhook/whale-activity/`

---

## 🔧 QuickNode Setup Instructions:

### 1. **For SOL/Stablecoin Trades:**

**Webhook URL:** `http://195.201.84.5:8001/webhook/`

**Method:** `POST`

**Expected Payload Fields:**
- `wallet_address` (or `wallet`, `owner`, `walletAddress`)
- `signature` (or `tx_signature`, `transaction`)
- `direction` (or `side`, `action`) - "buy" or "sell"
- `sol_amount` (or `amount_sol`, `sol`)
- `stablecoin_amount` (or `usdc_amount`, `amount_usdc`)
- `price` (or `mark_price`, `avg_price`)
- `trade_timestamp` (or `timestamp`, `block_time`)
- `perp_direction` (optional)

**Payload Format:** Can send single object or array of objects

**Example Payload:**
```json
{
  "wallet_address": "7BgBvyjrZX1YKz4oh9mjb8ZScatkkwb8DzFx4n8kZSKS",
  "signature": "5j7s...",
  "direction": "buy",
  "sol_amount": 10.5,
  "stablecoin_amount": 1375.50,
  "price": 131.00,
  "trade_timestamp": "2026-01-03T20:30:00Z"
}
```

Or batch:
```json
[
  { "wallet_address": "...", ... },
  { "wallet_address": "...", ... }
]
```

---

### 2. **For Whale Activity:**

**Webhook URL:** `http://195.201.84.5:8001/webhook/whale-activity`

**Method:** `POST`

**Expected Payload Fields:**
- `wallet_address` (required)
- `signature` (transaction signature)
- `whale_type` (or `type`)
- `current_balance`
- `sol_change` (delta)
- `percentage_moved`
- `direction` (in/out)
- `timestamp`
- Plus other optional fields for perp positions

---

## ✅ Health Check & Testing:

### Test if webhook is working:
```bash
# Health check
curl http://195.201.84.5:8001/webhook/health

# Expected response:
{
  "status": "ok",
  "timestamp": "2026-01-03T20:30:00",
  "duckdb": {
    "trades_in_hot_storage": 0,
    "whale_movements_in_hot_storage": 0,
    "retention": "24 hours"
  }
}
```

### Test sending a trade:
```bash
curl -X POST http://195.201.84.5:8001/webhook/ \
  -H "Content-Type: application/json" \
  -d '{
    "wallet_address": "test123",
    "signature": "testsig",
    "direction": "buy",
    "sol_amount": 10,
    "stablecoin_amount": 1300,
    "price": 130,
    "trade_timestamp": "2026-01-03T20:30:00Z"
  }'

# Expected response:
{
  "status": "success",
  "trades_received": 1,
  "trades_saved": 1
}
```

---

## 📊 API Endpoints (Read Data):

### Get recent trades:
```
GET http://195.201.84.5:8001/webhook/api/trades?limit=100
```

### Get whale movements:
```
GET http://195.201.84.5:8001/webhook/api/whale-movements?limit=100
```

---

## 🔥 Features:

✅ **Tolerant Parsing**: Multiple field name variations supported
✅ **Batch Support**: Can receive single object or array
✅ **24h Hot Storage**: Data kept in DuckDB for 24 hours
✅ **Always Returns 200**: Even on parsing errors (prevents QuickNode retries)
✅ **Real-time**: Instantly available for trading decisions
✅ **No Downtime**: Fast in-memory storage

---

## 🔒 Security Notes:

- Currently running on **HTTP** (no SSL)
- Server is publicly accessible on port 8001
- Consider adding authentication if needed
- Firewall is NOT blocking port 8001 (accessible)

---

## 📝 Summary for QuickNode Configuration:

| Setting | Value |
|---------|-------|
| **Trade Webhook URL** | `http://195.201.84.5:8001/webhook/` |
| **Whale Webhook URL** | `http://195.201.84.5:8001/webhook/whale-activity` |
| **Method** | `POST` |
| **Content-Type** | `application/json` |
| **Timeout** | 30 seconds recommended |

**That's it!** Just add the URL to QuickNode and your webhook will start receiving real-time trade data. 🚀

---

## 🧪 Verification After Setup:

1. Configure webhook in QuickNode
2. Wait for a trade to occur
3. Check if data arrived:
```bash
curl http://195.201.84.5:8001/webhook/api/trades?limit=10
```

4. Check system logs:
```bash
tail -f /root/follow_the_goat/features/logs/webhook.log
```

5. Check DuckDB counts:
```bash
curl http://195.201.84.5:8001/webhook/health
# Look for "trades_in_hot_storage" > 0
```


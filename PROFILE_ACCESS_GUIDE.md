# Accessing Wallet Profiles from Trading Logic

## ✅ Profiles are in Master2's In-Memory DuckDB (INSTANT ACCESS)

The PostgreSQL state tracking change **only** moved the state tracking (last_trade_id) to PostgreSQL.
**All profile data remains in master2's local in-memory DuckDB** for instant trading access.

## Current Setup:

```
┌─────────────────────────────────────────────────────┐
│              MASTER2.PY (Trading Logic)             │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌────────────────────────────────────────────┐    │
│  │  Local In-Memory DuckDB                    │    │
│  │  ✓ 24,000+ wallet_profiles (FAST!)        │    │
│  │  ✓ sol_stablecoin_trades                  │    │
│  │  ✓ cycle_tracker                          │    │
│  │  ✓ prices                                 │    │
│  └────────────────────────────────────────────┘    │
│                                                     │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│              PostgreSQL (Archive)                   │
├─────────────────────────────────────────────────────┤
│  ✓ wallet_profiles_state (7 rows)                  │
│    - Just tracks last_trade_id per threshold       │
│    - Used for incremental processing only          │
└─────────────────────────────────────────────────────┘
```

## How to Access Profiles in Your Trading Code:

### Option 1: From Feature Modules (Recommended)

```python
from core.database import get_duckdb

# This uses master2's registered "central" connection
with get_duckdb("central") as conn:
    # Get profiles for a specific wallet
    profiles = conn.execute("""
        SELECT * FROM wallet_profiles
        WHERE wallet_address = ?
        ORDER BY trade_timestamp DESC
        LIMIT 10
    """, ['wallet_address_here']).fetchall()
    
    # Get profiles by threshold
    profiles = conn.execute("""
        SELECT * FROM wallet_profiles
        WHERE threshold = 0.3
        AND trade_timestamp >= NOW() - INTERVAL 1 HOUR
    """).fetchall()
```

### Option 2: From Master2.py Context (Within Scheduler)

```python
from scheduler.master2 import get_local_duckdb

# Direct access to master2's local DuckDB
cursor = get_local_duckdb(use_cursor=True)
profiles = cursor.execute("""
    SELECT * FROM wallet_profiles
    WHERE wallet_address = ?
""", ['wallet_address']).fetchall()
```

### Option 3: Via Master2's Local API (Port 5052)

```python
import requests

response = requests.post('http://localhost:5052/query', json={
    'sql': 'SELECT * FROM wallet_profiles WHERE threshold = 0.3 LIMIT 100'
})
profiles = response.json()['results']
```

## Available Profile Fields:

- `id` - Profile ID
- `wallet_address` - Wallet address
- `threshold` - Threshold (0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5)
- `trade_id` - Original trade ID
- `trade_timestamp` - When the trade happened
- `price_cycle` - Cycle ID
- `price_cycle_start_time` - Cycle start
- `price_cycle_end_time` - Cycle end
- `trade_entry_price_org` - Original entry price
- `stablecoin_amount` - Trade amount
- `trade_entry_price` - Entry price from Jupiter
- `sequence_start_price` - Cycle start price
- `highest_price_reached` - Cycle high
- `lowest_price_reached` - Cycle low
- `long_short` - Position direction
- `short` - Short flag

## Performance:

- **Query Speed:** Sub-millisecond (in-memory DuckDB)
- **No Network Calls:** Direct memory access
- **No Locks:** Uses thread-local cursors for concurrent reads
- **Hot Data:** Only last 24 hours (always fast)

## What Changed:

### Before:
```
wallet_profiles_state → In-memory DuckDB
  ❌ Lost on restart
  ❌ Caused full rebuild (20-30s lock)
```

### After:
```
wallet_profiles_state → PostgreSQL
  ✅ Persists across restarts
  ✅ Incremental updates only (~1-2s)

wallet_profiles (data) → Still in-memory DuckDB
  ✅ Same instant access as before
  ✅ No performance impact on trading
```

## Summary:

**Your trading logic has the EXACT SAME fast access to profiles as before!**

The only difference is the state tracking is now persistent, which means:
- No more 20-30 second delays on restart
- Only process new trades incrementally
- Better performance overall

Nothing changed in how you query or use the profiles for trading decisions.


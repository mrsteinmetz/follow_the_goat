# Architecture Clarification: master.py vs master2.py

## Summary

**master.py** = Raw Data Ingestion ONLY
**master2.py** = All Computation & Trading Logic

This separation allows trading logic to be restarted without stopping data feeds.

---

## master.py - Data Engine (Port 5050)

### Purpose
Runs indefinitely to ingest raw market data.

### Responsibilities
- ✅ Jupiter price fetching (every 1s)
- ✅ Binance order book stream
- ✅ Trade sync from webhook (every 1s)
- ✅ Data cleanup (hourly)
- ✅ Serves raw data via API (port 5050)

### What it does NOT do
- ❌ NO price cycle analysis
- ❌ NO wallet profile building
- ❌ NO trading decisions
- ❌ NO pattern validation

### Jobs
1. `fetch_jupiter_prices` - Get SOL/BTC/ETH prices
2. `sync_trades_from_webhook` - Import trades from .NET webhook
3. `cleanup_jupiter_prices` - Remove old price data
4. `cleanup_duckdb_hot_tables` - 24hr hot storage cleanup

---

## master2.py - Trading Logic (Port 5052)

### Purpose
Computes trading signals and makes trading decisions. Can be restarted without affecting data ingestion.

### Responsibilities
- ✅ Gets raw data from master.py's API
- ✅ Computes price cycles from prices (`create_price_cycles.py`)
- ✅ Builds wallet profiles
- ✅ Validates trading patterns
- ✅ Follows profitable wallets
- ✅ Manages trailing stops
- ✅ Pushes computed data back to master.py (for website)

### Critical: Fresh Start Behavior
When master2.py restarts:
1. Fetches 2 hours of raw data (prices, trades, order book)
2. **Does NOT fetch old cycles** - computes them fresh from prices
3. Creates exactly 7 active cycles (one per threshold)
4. Processes price points continuously to update cycles

### Jobs
1. `sync_from_engine` - Pull raw data from master.py
2. `process_price_cycles` - Compute cycles from prices
3. `follow_the_goat` - Track profitable wallets
4. `trailing_stop_seller` - Monitor exit conditions
5. `create_wallet_profiles` - Analyze wallet behavior
6. `train_validator` - Pattern validation
7. `update_potential_gains` - Calculate trade outcomes

---

## Data Flow

```
Jupiter API → master.py (raw prices)
                ↓
         master2.py (reads)
                ↓
      create_price_cycles.py (computes)
                ↓
         7 active cycles
                ↓
    Syncs back to master.py
                ↓
         website_api.py (displays)
```

---

## Key Files

### Data Ingestion (master.py)
- `scheduler/master.py` - Main data engine
- `000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py` - Price fetching

### Trading Computation (master2.py)
- `scheduler/master2.py` - Trading scheduler
- `000data_feeds/2_create_price_cycles/create_price_cycles.py` - Cycle analysis
- `000data_feeds/5_create_profiles/create_profiles.py` - Wallet profiling
- `000trading/follow_the_goat.py` - Trade following
- `000trading/sell_trailing_stop.py` - Exit management

### APIs
- Port 5050: master.py Data Engine API (raw data)
- Port 5051: website_api.py (serves website with deduplication)
- Port 5052: master2.py Local API (computed data for debugging)

---

## Common Mistakes (Now Fixed)

### ❌ WRONG: Backfilling cycle_tracker
```python
# This was causing 33 duplicate cycles!
tables_to_backfill = [
    ("cycle_tracker", 1000),  # ❌ WRONG
]
```

### ✅ CORRECT: Compute cycles from prices
```python
# Cycles are COMPUTED, not fetched
tables_to_backfill = [
    ("prices", 10000),  # ✅ Get raw data
    # cycle_tracker NOT included
]

# Then compute fresh cycles
process_price_cycles()  # Creates exactly 7 cycles
```

---

## Testing Clean Start

```bash
# 1. Stop trading logic
screen -S master2 -X quit

# 2. Restart (gets fresh 2hr data, computes cycles from scratch)
screen -dmS master2 bash -c "source venv/bin/activate && python scheduler/master2.py"

# 3. Verify exactly 7 active cycles
curl -s http://localhost:5052/cycle_tracker?active_only=true | python3 -c "import sys, json; print(json.load(sys.stdin)['count'])"
# Should print: 7
```

---

## Summary

- **master.py**: Never stops, never computes, only ingests
- **master2.py**: Restartable, gets raw data, computes everything
- **Cycles**: Always computed fresh from prices, never backfilled
- **Result**: Exactly 7 active cycles, no duplicates


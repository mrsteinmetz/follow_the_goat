# Price Cycle Issue - Root Cause Analysis
**Date:** 2026-01-29  
**Issue:** follow_the_goat.py only creating 8 buyins per 24 hours

---

## Executive Summary

**Root Cause:** Price cycles are staying open for 60-110 minutes instead of closing within 10-30 minutes, which limits the number of buying opportunities.

---

## Facts Verified

✅ **Prices ARE being fetched** - Latest price ~2 seconds old
✅ **Cycles ARE being created** - 79 cycles in last 24 hours  
✅ **follow_the_goat.py IS detecting new cycles** - Sees cycle 27952
✅ **Trades ARE being detected** - Script runs every 0.5s, finds trades
✅ **max_buys_per_cycle=1 is working as designed** - Intentional limit

❌ **Cycles stay open too long** - Current cycle is 62+ minutes old
❌ **Pattern validator blocks 100% of play 2 trades** - All marked "no_go"

---

## The Problem

### Play 2 Performance (Last 24 Hours)
- **Wallets tracked:** 10 (recently updated from 4)
- **Buyins attempted:** 8
- **Buyins approved:** 0 (100% blocked by pattern validator)
- **Status:** All 8 marked as "no_go"

### Cycle Behavior
```
Cycle 27952: Started 08:00:40, Still active after 62+ minutes
Cycle 27944: 06:12:57 - 08:00:40 (110 minutes!)
Cycle 27936: 05:46:57 - 06:12:57 (26 minutes)
Cycle 27933: 05:08:10 - 05:46:57 (39 minutes)
Cycle 27929: 04:22:21 - 05:08:10 (46 minutes)
```

**Average cycle duration:** ~40-50 minutes (should be 10-30 minutes)

### Impact
With cycles lasting 40-110 minutes:
- 24 hours = 1440 minutes
- 1440 / 50 minutes average = ~29 potential cycles per day
- But only getting ~15-20 actual closed cycles with buy opportunities
- With `max_buys_per_cycle=1`, maximum possible is 29 buyins/day
- But pattern validator blocks most, resulting in ~8 actual trades/day

---

## Why Cycles Stay Open Too Long

**Expected behavior:**
1. Cycle starts when price rises
2. Tracks highest price reached
3. Closes when price drops X% below highest (e.g., 0.3% = threshold)
4. New cycle starts immediately

**Actual behavior:**
- Cycles are staying open much longer than expected
- Possible causes:
  1. Price isn't dropping enough to trigger cycle close (0.3% threshold)
  2. create_price_cycles.py may have a bug/delay
  3. High price volatility keeping cycles "alive"

---

## The Pattern Validator Problem

**Play 2 results (last 24 hours):**
- 8 buyins created
- 8 marked "no_go" (blocked)
- 0 approved for trading

This means even when a wallet from play 2 trades:
1. ✅ Trade detected
2. ✅ Buyin created
3. ✅ Trail generated (15-minute price history)
4. ❌ Pattern validator rejects it
5. Status set to "no_go" instead of "pending"

**Pattern validator settings for play 2:**
- Enabled: Yes
- Project IDs filter: 1 project configured
- Validator is extremely strict (100% rejection rate)

---

## Solutions

### Option 1: Quick Fix (Recommended)
**Temporarily disable pattern validator for play 2 to test**

This will:
- Allow all wallet trades to go through
- Show true potential of 10 wallets
- Verify if the issue is validator strictness vs. wallet activity

**Expected result:** 20-40 buyins per day (limited by cycle frequency)

### Option 2: Investigate Cycle Duration
**Why are cycles staying open so long?**

Need to check:
1. Is SOL price moving in < 0.3% range for extended periods?
2. Is create_price_cycles.py running properly?
3. Are there bugs in cycle close logic?

### Option 3: Relax Pattern Validator
**Adjust pattern_validator settings for play 2**

Current settings are blocking 100% of trades, which suggests:
- Criteria are too strict
- Project filter is too narrow
- Trail data doesn't match expected patterns

---

## Recommended Actions

### Immediate (5 minutes):
1. **Check if wallets are actually trading**
   ```sql
   SELECT wallet_address, COUNT(*) as trades_last_24h
   FROM sol_stablecoin_trades
   WHERE wallet_address IN (
     -- Play 2 wallets from find_wallets_sql query
     SELECT wallet_address FROM wallet_profiles
     WHERE ... play 2 criteria
   )
   AND trade_timestamp >= NOW() - INTERVAL '24 hours'
   AND direction = 'buy'
   GROUP BY wallet_address
   ORDER BY trades_last_24h DESC;
   ```

2. **Disable pattern validator for play 2 temporarily**
   - Set `pattern_validator_enable = 0` for play_id=2
   - Wait 1-2 hours
   - Check if buyins increase

### Short-term (1 hour):
1. **Investigate why cycle 27952 is 62+ minutes old**
   - Check current SOL price vs cycle high price
   - Verify create_price_cycles.py is running
   - Check for errors in master.py logs

2. **Review pattern validator logs**
   - Check `pattern_validator_log` field in rejected buyins
   - Understand why 100% are blocked
   - Adjust criteria or disable for play 2

### Long-term:
1. Consider adding more wallets to play 2 (increase from 10 to 50+)
2. Adjust cycle thresholds (0.3% may be too tight)
3. Review pattern validator effectiveness across all plays

---

## Questions for User

1. **Is 100% rejection by pattern validator expected for play 2?**
   - If no: We should disable or adjust it
   - If yes: Then 8 buyins/day is correct behavior

2. **Do you want to temporarily disable pattern validator to test?**

3. **Should we investigate why cycles are lasting 40-110 minutes?**
   - This seems like the bigger issue limiting opportunities

4. **Do you expect play 2 wallets to trade more frequently?**
   - If they only trade 8-10 times per day, system is working correctly
   - If they should trade 100+ times per day, something else is wrong

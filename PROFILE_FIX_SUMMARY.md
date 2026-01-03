# Wallet Profiles Page - Issue Analysis & Fix

## Problem Report
The profiles page (`000website/pages/profiles/index.php`) shows:
- "Zero profiles created" in the stats
- Table shows profiles but all say "zero invested"

## Root Cause Analysis

### Issue #1: Missing `/profiles/stats` Endpoint ✓ FIXED
**Problem**: The PHP page was calling `getProfilesStats()` which expects a `/profiles/stats` API endpoint, but this endpoint didn't exist in `scheduler/website_api.py`.

**Impact**: Without this endpoint, the stats would return `null`, causing all stat values to default to 0:
- Total Profiles: 0
- Unique Wallets: 0
- Unique Cycles: 0
- Total Invested: $0

**Fix**: Added the `/profiles/stats` endpoint to `scheduler/website_api.py` that:
- Aggregates data from the `wallet_profiles` table
- Returns: `total_profiles`, `unique_wallets`, `unique_cycles`, `total_invested`, `avg_entry_price`
- Respects the `threshold` and `hours` filters

### Issue #2: Raw Data Instead of Aggregated Data ✓ FIXED
**Problem**: The `/profiles` endpoint was returning raw `wallet_profiles` records (one row per trade) instead of aggregated data (one row per wallet).

**Impact**: 
- The page would show duplicate wallet entries
- The "total invested" would be per-trade, not per-wallet
- Trade counts would be incorrect

**Fix**: Rewrote the `/profiles` endpoint to:
- GROUP BY `wallet_address` to show one row per wallet
- Calculate `avg_potential_gain` across all trades for that wallet
- SUM `stablecoin_amount` to get total invested per wallet
- Count trades below/above threshold for each wallet
- Support ordering by `avg_gain`, `trade_count`, or `recent` (latest trade)

## Changes Made

### File: `scheduler/website_api.py`

#### 1. Enhanced `/profiles` Endpoint (lines 426-517)
```python
@app.route('/profiles', methods=['GET'])
@engine_required
def get_profiles():
    """
    Get wallet profiles with aggregated data per wallet.
    
    This endpoint aggregates the wallet_profiles table to show:
    - One row per wallet (instead of one per trade)
    - Average potential gain across all trades
    - Total invested amount
    - Trade counts above/below threshold
    """
    # ... implementation with GROUP BY wallet_address
```

**Key Features**:
- Aggregates by wallet_address
- Calculates average potential gain (handling both long and short positions)
- Sums total invested (`stablecoin_amount`)
- Counts trades below/above threshold
- Returns latest trade timestamp per wallet
- Supports filtering by threshold, hours, and specific wallet
- Supports ordering by avg_gain, trade_count, or recent

#### 2. New `/profiles/stats` Endpoint (lines 519-568)
```python
@app.route('/profiles/stats', methods=['GET'])
@engine_required
def get_profiles_stats():
    """Get aggregated statistics for wallet profiles."""
    # ... implementation
```

**Returns**:
- `total_profiles`: Total number of profile records
- `unique_wallets`: Count of distinct wallet addresses
- `unique_cycles`: Count of distinct price cycles
- `total_invested`: Sum of all stablecoin amounts
- `avg_entry_price`: Average entry price across all trades

## Potential Remaining Issues

### Issue #3: Missing Data (NEEDS VERIFICATION)
Even with the fixes above, if the page still shows zeros, it could mean:

1. **No data in `wallet_profiles` table**
   - Check if the profile creation job is running in `master2.py`
   - Check if there are completed price cycles (`cycle_end_time IS NOT NULL`)
   - Check if there are buy trades in `sol_stablecoin_trades`

2. **Missing `stablecoin_amount` values**
   - The `stablecoin_amount` column might be NULL in trades
   - This would cause "Total Invested" to show $0 even with profiles

3. **Scheduler not running**
   - The `create_wallet_profiles` job in `master2.py` might not be running
   - Check: `python scheduler/master2.py` should be active

## Diagnostic Script

Run the diagnostic script to check data:
```bash
python check_profiles_data.py
```

This will check:
1. If `wallet_profiles` table exists and has data
2. Total profiles and unique wallets
3. Available thresholds
4. Stablecoin amounts (invested values)
5. Recent profiles
6. Aggregated data (what the website shows)
7. Source data in `sol_stablecoin_trades`
8. Completed cycles in `cycle_tracker`

## Testing the Fix

1. **Restart the website API** (to load the new endpoints):
   ```bash
   # Stop website_api.py if running
   # Then restart:
   python scheduler/website_api.py
   ```

2. **Check the API directly**:
   ```bash
   # Test stats endpoint
   curl http://127.0.0.1:5051/profiles/stats?threshold=0.3&hours=24
   
   # Test profiles endpoint
   curl http://127.0.0.1:5051/profiles?threshold=0.3&hours=24&order_by=avg_gain&limit=10
   ```

3. **Reload the profiles page** in the browser:
   - Go to: http://your-website/pages/profiles/
   - Should now show correct stats and aggregated wallet data

## Expected Behavior After Fix

### Stats Cards (Top of Page)
- **Total Profiles**: Should show total number of trade records
- **Unique Wallets**: Should show count of distinct wallets
- **Unique Cycles**: Should show count of distinct price cycles
- **Total Invested**: Should show sum of all stablecoin amounts (in dollars)

### Profiles Table
- **One row per wallet** (not per trade)
- **Avg Gain**: Average potential gain across all trades for that wallet
- **Trade counts**: Separated by below/above threshold
- **Total Invested**: Sum of stablecoin_amount for all trades by that wallet
- **Latest Trade**: Most recent trade timestamp for that wallet

## Next Steps If Still Not Working

1. Run the diagnostic script: `python check_profiles_data.py`
2. Check if `master2.py` is running and creating profiles
3. Check logs for errors: `scheduler/master2.py` logs
4. Verify `sol_stablecoin_trades` has data with non-null `stablecoin_amount`
5. Verify `cycle_tracker` has completed cycles (where `cycle_end_time IS NOT NULL`)

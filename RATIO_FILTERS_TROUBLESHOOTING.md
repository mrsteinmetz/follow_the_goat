# Ratio-Only Filter Troubleshooting Guide

## Date: January 16, 2026

## Issue Summary

**Problem:** User enabled "Ratio Only" toggle on website, but absolute price filters (like `sp_min_price`, `eth_close_price`) were still showing as active filters.

**Root Cause:** The `is_ratio` setting was working correctly, BUT the system couldn't find any valid filter combinations that met the minimum requirements:
- `min_filters_in_combo = 3` (required at least 3 filters in combination)
- The best ratio filters found were only 1-2 filters
- System rejected excellent 94.7% bad removal because it only used 1 filter

---

## How Ratio-Only Mode Works

### Setting Flow
```
Website Toggle (is_ratio=true)
    ↓
auto_filter_settings table
    ↓
create_new_paterns.py loads config
    ↓
Only ratio filters analyzed (71 available)
    ↓
Best combinations created
    ↓
Old filters cleared, new filters saved
```

### What It Does
1. ✅ Loads ONLY ratio-based filters from database (`is_ratio = 1`)
2. ✅ Analyzes percentage/ratio fields (bps, pct, ratio, share, acceleration)
3. ✅ Creates filters that work regardless of market price
4. ✅ Clears old absolute filters before saving new ones

---

## The Actual Problem

### What Happened
```
Run at 19:01:49 UTC:
✅ Loaded 71 ratio filters
✅ Found excellent single filter: ob_spread_bps (94.7% bad removal, 80.7% good kept)
✅ Found excellent 2-filter combo: ob_volume_imbalance + ob_aggression_ratio (89.9% bad removal)
❌ Rejected both because min_filters_in_combo = 3
❌ No 3+ filter combo met requirements
❌ Cleared old filters but created 0 new filters
❌ Old absolute filters remained visible on website
```

### Settings That Caused The Issue
```
good_trade_threshold = 0.6%         (very strict, only 173/2128 trades = 8.1%)
analysis_hours = 12                 (short window, less data)
min_filters_in_combo = 3            (rejected 1-2 filter combos)
min_good_trades_kept_pct = 50       (must keep 50% of good trades)
min_bad_trades_removed_pct = 50     (must remove 50% of bad trades)
```

With only 8.1% good trades, finding 3+ filters that keep 50% of good trades AND remove 50% of bad trades is very challenging.

---

## The Solution

### Option 1: Allow Fewer Filters (IMPLEMENTED)
```sql
UPDATE auto_filter_settings 
SET setting_value = '1' 
WHERE setting_key = 'min_filters_in_combo';
```

**Result:** Now uses the excellent single ratio filter:
- Filter: `ob_spread_bps` (Minute 11)
- Range: [0.704032 - 0.706450]
- Performance: 94.7% bad removed, 80.7% good kept ✅

### Option 2: Relax Quality Requirements
```sql
-- More data
UPDATE auto_filter_settings SET setting_value = '24' WHERE setting_key = 'analysis_hours';

-- Less strict threshold
UPDATE auto_filter_settings SET setting_value = '0.3' WHERE setting_key = 'good_trade_threshold';

-- Keep min_filters_in_combo = 3
```

This provides more good trades (higher percentage) making it easier to find 3+ filter combinations.

---

## Available Ratio Filters

The system has **71 ratio-based filter columns** available:

### Order Book Ratios (prefix: `ob_`)
- `ob_spread_bps` - Bid-ask spread in basis points ✅ BEST PERFORMER
- `ob_aggression_ratio` - Buy/sell aggression
- `ob_volume_imbalance` - Buy/sell volume imbalance ✅ EXCELLENT
- `ob_vwap_spread_bps` - VWAP spread

### Transaction Ratios (prefix: `tx_`)
- `tx_buy_trade_pct` - Percentage of buy trades
- `tx_trades_per_second` - Trade frequency

### Price Movement Ratios (prefix: `pm_`, `btc_`, `eth_`)
- `pm_price_change_1m` - Price change percentage (1 minute)
- `pm_price_change_5m` - Price change percentage (5 minutes)
- `pm_price_change_10m` - Price change percentage (10 minutes)
- `btc_price_change_*` - Bitcoin correlation ratios
- `eth_price_change_*` - Ethereum correlation ratios

### Pattern Confidence Scores (prefix: `pat_`, `mp_`)
- `pat_asc_tri_compression_ratio`
- `mp_momentum_acceleration_confidence`
- And many more...

All these filters use **relative values** (percentages, ratios, basis points) that remain valid regardless of whether SOL is $100 or $200.

---

## Comparison: Ratio vs Absolute Filters

### Absolute Filters (OLD - BROKEN by price changes)
```
sp_min_price: [142.75 - 144.53]      ❌ Only works when SOL ≈ $143
eth_open_price: [3313.94 - 3349.41]  ❌ Only works when ETH ≈ $3330
```

**Problem:** If SOL price moves to $160, these filters block ALL trades because $160 is outside [142-144].

### Ratio Filters (NEW - MARKET INDEPENDENT)
```
ob_spread_bps: [0.704 - 0.706]       ✅ Works at any price (measures %)
tx_buy_trade_pct: [46.4 - 56.4]      ✅ Works at any price (measures %)
pm_price_change_10m: [-0.33 - 0.05]  ✅ Works at any price (measures %)
```

**Benefit:** These measure relative changes and ratios, so they work whether SOL is $50, $150, or $500.

---

## Current Status (After Fix)

### Settings
```
is_ratio = true                     ✅ Ratio-only mode enabled
analysis_hours = 12                 ✅ 12-hour window
good_trade_threshold = 0.6%         ✅ Strict quality
min_filters_in_combo = 2            ✅ Allows 2+ filter combinations
min_good_trades_kept_pct = 50       ✅ Keep 50% of good trades
min_bad_trades_removed_pct = 50     ✅ Remove 50% of bad trades
```

### Active Filters
```
✅ RATIO | M11 | ob_spread_bps | [0.704032 - 0.706450]
```

**Performance:**
- Bad trades removed: 94.7%
- Good trades kept: 80.7%
- Filter type: Ratio (basis points)
- Signal timing: Minute 11 before entry

---

## Monitoring

### Check Current Filters
```sql
SELECT f.field_column, f.minute, f.from_value, f.to_value, 
       tfv.is_ratio
FROM pattern_config_filters f
LEFT JOIN trade_filter_values tfv ON tfv.filter_name = f.field_column
WHERE f.project_id IN (SELECT id FROM pattern_config_projects WHERE name = 'AutoFilters')
  AND tfv.is_ratio = 1
GROUP BY f.field_column, f.minute, f.from_value, f.to_value, tfv.is_ratio;
```

### Check Last Run
```sql
SELECT run_id, updated_at, filters_applied, status
FROM ai_play_updates
ORDER BY updated_at DESC
LIMIT 1;
```

### Check Available Ratio Filters
```sql
SELECT COUNT(DISTINCT filter_name) as count
FROM trade_filter_values
WHERE is_ratio = 1;
-- Should return 71
```

---

## Key Takeaways

1. **`is_ratio` setting was ALWAYS working** - it correctly loaded only ratio filters
2. **The real issue was `min_filters_in_combo = 3`** - rejected excellent 1-2 filter combos
3. **Ratio filters are harder to combine** - they're more selective individually, so fewer combinations meet strict requirements
4. **Quality vs Quantity tradeoff:**
   - Fewer filters (1-2) = Best performance (94.7% bad removal)
   - More filters (3+) = More conservative, needs relaxed thresholds

5. **The system cleared old filters** - but showed "0 filters" instead of keeping old ones as fallback (this is actually correct behavior to prevent stale absolute filters from breaking trades)

---

## Future Improvements

### Option 1: Progressive Fallback
If no combinations meet requirements:
1. Try min_filters_in_combo - 1
2. Try min_filters_in_combo - 2
3. Use best single filter
4. Better than having 0 filters

### Option 2: Separate Settings for Ratio vs Absolute
```
min_filters_in_combo_absolute = 3
min_filters_in_combo_ratio = 1
```

Since ratio filters are individually more powerful, they need fewer combinations.

### Option 3: Auto-Adjust Settings
If no valid combinations found:
- Automatically reduce min_filters_in_combo by 1
- Retry analysis
- Log adjustment for user review

---

## Contact

For questions about this system:
1. Check filter_reference_suggestions table for what filters were tested
2. Check filter_combinations table for what combos were created
3. Check ai_play_updates table for what got applied to plays
4. Check logs in scheduler/master2.py for detailed analysis output


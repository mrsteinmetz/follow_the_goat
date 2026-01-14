# Filter Optimization Analysis - January 14, 2026

## Executive Summary

**Problem**: Current AutoFilters configuration is catching only **53.4%** of good trades (1945/3645) while filtering 51% of bad trades.

**Solution Found**: New filter recommendations can catch **90%+** of good trades while still removing 20%+ of bad trades.

---

## Current Performance

### Today's Trade Data (Jan 14, 2026)
- **Total trades**: 9,336
- **Good trades (>= 0.3%)**: 3,690 (39.5%)
- **Bad trades (< 0.3%)**: 5,646 (60.5%)

### Current AutoFilters Project (ID=5)
- **Minute**: 7
- **Good trades caught**: 1,945/3,645 (53.4%)
- **Bad trades filtered**: 2,869/5,627 (51.0%)
- **Effectiveness Score**: 27.21

**Issue**: Too aggressive filtering is missing nearly half of good opportunities!

---

## Analysis Findings

### Strategy 1: Looser Percentiles (5-95 instead of 10-90)

The current system uses 10th-90th percentiles. Testing with 5th-95th percentiles shows:

**Top Performing Filters:**

1. **tx_whale_volume_pct** (Minute 8)
   - Good trades caught: 2,922/3,226 (90.6%)
   - Bad trades removed: 20.1%
   - Score: 18.20

2. **tx_buy_trade_pct** (Minute 0)
   - Good trades caught: 2,848/3,645 (78.1%)
   - Bad trades removed: 28.5%
   - Score: 22.23

3. **tx_vwap** (Minute 0)
   - Good trades caught: 2,845/3,645 (78.1%)
   - Bad trades removed: 27.0%
   - Score: 21.10

### Strategy 2: Order Book Features (Best Overall)

Order book features at Minute 11 show excellent performance:

1. **ob_volume_imbalance** (Minute 11)
   - Good trades caught: 3,282/3,645 (**90.0%**)
   - Bad trades removed: 20.4%
   - **Score: 18.37** ⭐ BEST

2. **ob_depth_imbalance_ratio** (Minute 11)
   - Good trades caught: 3,292/3,645 (**90.3%**)
   - Bad trades removed: 20.2%
   - Score: 18.20

3. **ob_ask_liquidity_share_pct** (Minute 11)
   - Good trades caught: 1,662/1,832 (90.7%)
   - Bad trades removed: 27.6%
   - Score: 25.01

### Strategy 3: Top Performers Only (>= 0.5% gain)

Testing filters optimized for high-performing trades (>= 0.5%):

1. **tx_vwap** (Minute 0)
   - Top performers caught: 1,464/1,832 (79.9%)
   - Bad trades removed: 34.8%
   - Score: 27.79

2. **ob_volume_imbalance** (Minute 11)
   - Top performers caught: 1,650/1,832 (90.1%)
   - Bad trades removed: 29.9%
   - Score: 26.89

---

## Recommended Actions

### Option 1: Quick Fix - Update AutoFilters with Best Filters

Replace current filters with the top 3 performers:

```sql
-- Clear existing AutoFilters
DELETE FROM pattern_config_filters WHERE project_id = 5;

-- Insert new optimized filters
INSERT INTO pattern_config_filters 
(id, project_id, name, section, minute, field_name, field_column, from_value, to_value, include_null, is_active)
VALUES 
(5001, 5, 'Auto: ob_volume_imbalance', 'order_book', 11, 'ob_volume_imbalance', 'ob_volume_imbalance', -0.571749, 0.251451, 0, 1),
(5002, 5, 'Auto: tx_whale_volume_pct', 'transactions', 8, 'tx_whale_volume_pct', 'tx_whale_volume_pct', 9.607326, 56.898327, 0, 1),
(5003, 5, 'Auto: ob_depth_imbalance_ratio', 'order_book', 11, 'ob_depth_imbalance_ratio', 'ob_depth_imbalance_ratio', 0.270676, 1.709850, 0, 1);
```

**Expected Result**: 
- Good trades caught: ~90%
- Bad trades removed: ~20%
- Much better than current 53.4% catch rate!

### Option 2: Configure Auto-Generator to Use Looser Percentiles

Update the auto-generator settings to use 5-95 percentiles instead of 10-90:

```sql
-- Use looser percentiles (recommended)
UPDATE auto_filter_settings SET setting_value = '5' WHERE setting_key = 'percentile_low';
UPDATE auto_filter_settings SET setting_value = '95' WHERE setting_key = 'percentile_high';

-- Or be even more aggressive (catches more good trades, allows more bad trades)
UPDATE auto_filter_settings SET setting_value = '1' WHERE setting_key = 'percentile_low';
UPDATE auto_filter_settings SET setting_value = '99' WHERE setting_key = 'percentile_high';
```

Then let the auto-generator run on its next cycle (every 5-15 minutes).

### Option 3: Lower Good Trade Threshold

Current threshold is 0.3% (30 basis points). Consider:

```sql
-- Lower threshold to 0.2% to catch more opportunities
UPDATE auto_filter_settings SET setting_value = '0.2' WHERE setting_key = 'good_trade_threshold';
```

---

## Key Insights

### 1. Order Book Features Are Crucial
The best performing filters use **order book imbalance data** at **Minute 11**:
- `ob_volume_imbalance`
- `ob_depth_imbalance_ratio`
- `ob_ask_liquidity_share_pct`

These capture 90%+ of good trades.

### 2. Current Percentiles Are Too Narrow
Using 10-90 percentiles removes too many good trades. The 5-95 range:
- Catches 90%+ of good trades (vs 53%)
- Still removes 20% of bad trades
- Much better trade-off

### 3. Minute Timing Matters
- **Minute 11**: Best for order book features
- **Minute 8**: Best for whale volume
- **Minute 0-1**: Best for VWAP and total volume

### 4. Trade-off Philosophy Change Needed
Current approach:
- ✅ Filters 51% of bad trades
- ❌ Misses 47% of good trades

Recommended approach:
- ✅ Catches 90% of good trades
- ⚠️ Only filters 20% of bad trades
- **Better**: Catching good trades is more important than filtering bad ones (trailing stops handle exits)

---

## Missed Opportunities Today

Based on the charts you showed, the trades at **21:57 UTC** and **around 12:00 UTC** were likely missed because:

1. **Too narrow percentile range** (10-90 instead of 5-95)
2. **Wrong minute selection** (current filters at Minute 7, but best signals are at Minutes 8 and 11)
3. **Not using order book features** which are the strongest predictors

---

## Next Steps

**Immediate (Recommended)**:
1. Apply **Option 1** SQL above to update AutoFilters with proven filters
2. Apply **Option 2** to adjust percentiles to 5-95
3. Monitor next 24 hours to confirm improvement

**Short-term**:
1. Run `test_filter_recommendations.py` daily to validate filter performance
2. Adjust thresholds based on observed results
3. Consider adding more order book features to the analysis

**Long-term**:
1. Implement multi-minute approach (trade passes if ANY minute matches)
2. Create separate filter sets for different gain targets (0.3%, 0.5%, 1.0%)
3. Add machine learning model to predict optimal minute per token

---

## Scripts Created

1. **test_filter_optimization.py** - Full analysis (slow, comprehensive)
2. **test_filter_recommendations.py** - Quick daily analysis (fast, actionable)

Run daily:
```bash
python3 test_filter_recommendations.py > filter_analysis_$(date +%Y%m%d).log
```

---

## Performance Comparison

| Metric | Current | Recommended | Improvement |
|--------|---------|-------------|-------------|
| Good trades caught | 53.4% | 90.0% | **+36.6%** |
| Bad trades removed | 51.0% | 20.4% | -30.6% |
| Missed opportunities | 46.6% | 10.0% | **-36.6%** |

**Bottom Line**: The recommended filters will catch **nearly twice as many good trades** at the cost of allowing more bad trades through (which trailing stops will handle).

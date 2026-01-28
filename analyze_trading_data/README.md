# Trading Data Analysis

This folder contains scripts to analyze trading patterns and find optimal buy signals.

## 🎯 LATEST FINDINGS (Jan 28, 2026) ⭐ UPDATED

### **10 Minutes is TOO LONG - Switch to 3 Minutes!**

**Major Discovery:** For SOL's fast-moving cycles, a **3-MINUTE window** dramatically outperforms the 10-minute window.

See **[TIMEFRAME_OPTIMIZATION_RESULTS.md](./TIMEFRAME_OPTIMIZATION_RESULTS.md)** for complete details.

#### Quick Summary (8,515 trades analyzed):
- **3-minute window:** 80-100% win rate ⭐
- **2-minute window:** 62.5% win rate
- **10-minute window:** NOT in any top combinations ❌

#### Best Filter Combination (100% win rate):
```
✅ change_3m > 0.08%           (Price up >0.08% in last 3 minutes)
✅ wh_accumulation_ratio < 0.5  (Whales not overbought)
✅ pm_volatility_pct > 0.2%     (High volatility - capitulation)
✅ sp_total_change_pct < -0.2%  (Session price down >0.2%)
```

This catches **quick reversals** after capitulation - perfect for SOL's 5-60 minute cycles!

---

## Key Findings (Previous Analysis)

### What Makes a Cycle Bottom (Best Buy Moment)

Based on analysis of 6 good cycles (0.8%+ gains) over 48 hours:

| Filter | At Bottoms | Normal | Meaning |
|--------|-----------|--------|---------|
| `pm_price_change_1m` | -0.14% | -0.002% | Price just dropped sharply |
| `pm_price_change_5m` | -0.25% | -0.01% | 5-minute decline |
| `sp_total_change_pct` | -0.29% | +0.09% | Session price dropped significantly |
| `pm_volatility_pct` | 0.16% | 0.04% | High volatility (capitulation) |
| `eth_price_change_5m` | -0.04% | +0.002% | ETH also dropping (correlated) |
| `ob_liquidity_change_3m` | -11.7% | -2.4% | Liquidity draining from book |

### Suggested Filter Rules for Detecting Bottoms

```
pm_price_change_1m < -0.04          # Price dropping fast
pm_price_change_5m < -0.09          # 5-min decline
sp_total_change_pct < -0.02         # Session down
eth_price_change_5m < -0.01         # ETH also down
pm_volatility_pct > 0.10            # High volatility
```

### The Pattern

Cycle bottoms are characterized by:
1. **Rapid price drop** - price just fell significantly in last 1-5 minutes
2. **High volatility** - market is in "capitulation" mode
3. **Correlated drops** - ETH/BTC also dropping
4. **Liquidity draining** - order book showing stress

This is the "fear peak" moment right before reversal.

## Scripts

### 1. analyze_price_movement_patterns.py ⭐ NEW
**Enhanced analysis that includes price movement BEFORE entry.**

Analyzes:
- Price change 1m, 2m, 5m, 10m before entry
- Trend direction (rising/falling/flat)
- Correlation with trade outcomes
- Runs 100+ simulations to find optimal filter combinations

**Usage:**
```bash
python3 analyze_trading_data/analyze_price_movement_patterns.py
```

**Output:** Complete analysis showing best filter combinations with price movement checks.

### 2. find_optimal_buy_signals.py
Main analysis comparing good vs bad trades across all filter values.

### 3. analyze_filter_thresholds.py  
Detailed percentile analysis and threshold testing.

### 4. analyze_cycle_bottoms.py
Focused analysis of exact cycle bottom moments.

## Next Steps

1. **✅ DONE:** Analyze price movement before entry
2. **TODO:** Implement price movement check in `follow_the_goat.py`
3. **TODO:** Add `change_10m` to `buyin_trail_minutes` table
4. **TODO:** Create new pattern config with best combination
5. **TODO:** Test on live data for 24-48 hours

## Key Insights

### Why Price Movement Matters
- **Price at $126 falling** = Bad entry (likely to continue falling)
- **Price at $125 rising** = Good entry (reversal confirmed)

### Optimal Detection Window
- 1-2 minutes: Too noisy
- **3 minutes: OPTIMAL signal** ⭐⭐⭐ (80-100% win rate)
- **2 minutes: Good alternative** (62.5% win rate, more signals)
- 5-7 minutes: Signal quality drops
- 10+ minutes: Too late (missed entry point)

### Combine Two Strategies
- **Mean reversion:** Session price down (opportunity)
- **Momentum confirmation:** Recent **3-min** up (reversal confirmed) ⭐
- **Sweet spot:** Enter when BOTH conditions are true
- **Why 3 minutes:** Catches quick reversals EARLY (not 10 minutes late!)

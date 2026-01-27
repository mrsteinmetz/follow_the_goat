# Trading Data Analysis

This folder contains scripts to analyze trading patterns and find optimal buy signals.

## Key Findings

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

### 1. find_optimal_buy_signals.py
Main analysis comparing good vs bad trades across all filter values.

### 2. analyze_filter_thresholds.py  
Detailed percentile analysis and threshold testing.

### 3. analyze_cycle_bottoms.py
Focused analysis of exact cycle bottom moments.

## Next Steps

1. Add these patterns to `create_new_patterns.py` as candidate filters
2. Test precision of combined rules
3. Consider adding minute-by-minute analysis (the bottom signature may appear at minute 1-3, not minute 0)

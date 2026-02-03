WALLET ANALYSIS QUERIES
=======================

This folder contains SQL queries and scripts for finding high-potential wallets based on 
their trading history in the wallet_profiles table.

How It Works
------------
The wallet_profiles table is automatically populated by create_profiles.py. It joins:
1. sol_stablecoin_trades - Buy transactions from wallets
2. cycle_tracker - Completed price cycles (0.3% threshold by default)
3. prices - SOL price data to calculate actual entry prices

Each profile record contains:
- wallet_address: The wallet that made the trade
- trade_entry_price: The actual SOL price when they bought
- highest_price_reached: The peak SOL price in that cycle
- lowest_price_reached: The lowest SOL price in that cycle
- Timing data: When they entered relative to the cycle start/end

From this, we can calculate:
- Potential gain %: (peak - entry) / entry × 100
- Win rate: % of trades with >0.5% potential gain
- Entry timing: How early they buy in each cycle (0%=start, 100%=end)

Scripts
-------

1. find_high_potential_wallets.py
   - Comprehensive analysis with 3 different query strategies
   - Run: python3 find_high_potential_wallets.py
   - Shows:
     * High-potential wallets (frequent + high gains)
     * Consistent winners (high win rate)
     * Early entry specialists (buy near cycle starts)

2. quick_wallet_query.py
   - Simple CLI for quick queries
   - Run: python3 quick_wallet_query.py           # Top 10 wallets
   - Run: python3 quick_wallet_query.py 20        # Top 20 wallets
   - Run: python3 quick_wallet_query.py <wallet>  # Wallet details

Key Metrics Explained
---------------------

- Trades: Number of buy trades in the time period
- Avg%: Average potential gain % (from entry to cycle peak)
- Win%: Percentage of trades with >0.5% potential gain
- Entry%: Average timing in cycle (0%=start of cycle, 100%=end of cycle)
- Score: Combined metric = trades × avg% × win% (higher is better)

Example Output
--------------
Wallet                                       Trades    Avg%   Win%  Entry%    Score
--------------------------------------------------------------------------------
ABC123...XYZ                                     15   2.50   85.0    20.5   318.75
DEF456...UVW                                     12   3.20   75.0    15.2   288.00

This shows:
- ABC123 made 15 trades with 2.5% average potential gain and 85% win rate
- They typically entered at 20.5% into the cycle (early-ish)
- Score of 318.75 makes them a top candidate

Customization
-------------
Edit the scripts to change:
- min_trades: Minimum number of trades to qualify (default: 5-10)
- min_avg_potential: Minimum average gain % (default: 1.5%)
- min_win_rate: Minimum win rate % (default: 70%)
- lookback_hours: How far back to analyze (default: 24 hours)
- threshold: Which cycle threshold to use (default: 0.3 for 0.3% cycles)

Database Schema
---------------
The queries read from the wallet_profiles table in PostgreSQL:

CREATE TABLE wallet_profiles (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    price_cycle BIGINT NOT NULL,
    price_cycle_start_time TIMESTAMP NOT NULL,
    price_cycle_end_time TIMESTAMP NOT NULL,
    trade_entry_price_org DOUBLE PRECISION,
    stablecoin_amount DOUBLE PRECISION,
    trade_entry_price DOUBLE PRECISION NOT NULL,
    sequence_start_price DOUBLE PRECISION NOT NULL,
    highest_price_reached DOUBLE PRECISION NOT NULL,
    lowest_price_reached DOUBLE PRECISION NOT NULL,
    long_short VARCHAR(10),
    short SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Next Steps
----------
1. Run the scripts to see which wallets are performing well
2. Use the wallet addresses to track their future trades
3. Consider following their buy signals in follow_the_goat system
4. Adjust thresholds based on your risk tolerance

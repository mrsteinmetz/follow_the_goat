-- =============================================================================
-- PostgreSQL Schema for Follow The Goat (DuckDB-Compatible)
-- =============================================================================
-- Tables match DuckDB structure exactly. Data flows from DuckDB -> PostgreSQL
-- when it expires from hot storage (24h for most, 72h for trades).
-- =============================================================================

-- Trading Plays (persistent, never expires from DuckDB)
CREATE TABLE IF NOT EXISTS follow_the_goat_plays (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL UNIQUE,
    tolerance DOUBLE PRECISION DEFAULT 0.3,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_plays_wallet ON follow_the_goat_plays(wallet_address);

-- Trading Buyins (72h hot storage)
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins (
    id BIGSERIAL PRIMARY KEY,
    play_id INT,
    wallet_address VARCHAR(255) NOT NULL,
    original_trade_id BIGINT,
    tolerance DOUBLE PRECISION DEFAULT 0.3,
    price_cycle BIGINT,
    trade_signature VARCHAR(255),
    block_timestamp TIMESTAMP,
    quote_amount DECIMAL(20,8),
    base_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    is_buy SMALLINT DEFAULT 1,
    followed_at TIMESTAMP,
    our_entry_price DECIMAL(20,8),
    our_position_size DECIMAL(20,8),
    our_exit_price DECIMAL(20,8),
    our_exit_timestamp TIMESTAMP,
    our_profit_loss DECIMAL(20,8),
    our_status VARCHAR(20) DEFAULT 'pending',
    swap_response JSONB,
    sell_swap_response JSONB,
    price_movements JSONB,
    live_trade INT DEFAULT 0,
    higest_price_reached DECIMAL(20,8),
    current_price DECIMAL(20,8),
    entry_log JSONB,
    fifteen_min_trail JSONB,
    pattern_validator_log JSONB,
    potential_gains FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_buyins_wallet ON follow_the_goat_buyins(wallet_address);
CREATE INDEX IF NOT EXISTS idx_buyins_followed_at ON follow_the_goat_buyins(followed_at);
CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status);
CREATE INDEX IF NOT EXISTS idx_buyins_play_id ON follow_the_goat_buyins(play_id);

-- Buyin Trail Minutes (24h hot storage)
CREATE TABLE IF NOT EXISTS buyin_trail_minutes (
    id BIGSERIAL PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    minute SMALLINT NOT NULL,
    pm_price_change_1m DOUBLE PRECISION,
    pm_momentum_volatility_ratio DOUBLE PRECISION,
    pm_momentum_acceleration_1m DOUBLE PRECISION,
    pm_price_change_5m DOUBLE PRECISION,
    pm_price_change_10m DOUBLE PRECISION,
    pm_volatility_pct DOUBLE PRECISION,
    pm_body_range_ratio DOUBLE PRECISION,
    pm_volatility_surge_ratio DOUBLE PRECISION,
    pm_price_stddev_pct DOUBLE PRECISION,
    pm_trend_consistency_3m DOUBLE PRECISION,
    pm_cumulative_return_5m DOUBLE PRECISION,
    pm_candle_body_pct DOUBLE PRECISION,
    pm_upper_wick_pct DOUBLE PRECISION,
    pm_lower_wick_pct DOUBLE PRECISION,
    pm_wick_balance_ratio DOUBLE PRECISION,
    pm_price_vs_ma5_pct DOUBLE PRECISION,
    pm_breakout_strength_10m DOUBLE PRECISION,
    pm_open_price DOUBLE PRECISION,
    pm_high_price DOUBLE PRECISION,
    pm_low_price DOUBLE PRECISION,
    pm_close_price DOUBLE PRECISION,
    pm_avg_price DOUBLE PRECISION,
    ob_mid_price DOUBLE PRECISION,
    ob_price_change_1m DOUBLE PRECISION,
    ob_price_change_5m DOUBLE PRECISION,
    ob_price_change_10m DOUBLE PRECISION,
    ob_volume_imbalance DOUBLE PRECISION,
    ob_imbalance_shift_1m DOUBLE PRECISION,
    ob_imbalance_trend_3m DOUBLE PRECISION,
    ob_depth_imbalance_ratio DOUBLE PRECISION,
    ob_bid_liquidity_share_pct DOUBLE PRECISION,
    ob_ask_liquidity_share_pct DOUBLE PRECISION,
    ob_depth_imbalance_pct DOUBLE PRECISION,
    ob_total_liquidity DOUBLE PRECISION,
    ob_liquidity_change_3m DOUBLE PRECISION,
    ob_microprice_deviation DOUBLE PRECISION,
    ob_microprice_acceleration_2m DOUBLE PRECISION,
    ob_spread_bps DOUBLE PRECISION,
    ob_aggression_ratio DOUBLE PRECISION,
    ob_vwap_spread_bps DOUBLE PRECISION,
    ob_net_flow_5m DOUBLE PRECISION,
    ob_net_flow_to_liquidity_ratio DOUBLE PRECISION,
    ob_sample_count INT,
    ob_coverage_seconds INT,
    tx_buy_sell_pressure DOUBLE PRECISION,
    tx_buy_volume_pct DOUBLE PRECISION,
    tx_sell_volume_pct DOUBLE PRECISION,
    tx_pressure_shift_1m DOUBLE PRECISION,
    tx_pressure_trend_3m DOUBLE PRECISION,
    tx_long_short_ratio DOUBLE PRECISION,
    tx_long_volume_pct DOUBLE PRECISION,
    tx_short_volume_pct DOUBLE PRECISION,
    tx_perp_position_skew_pct DOUBLE PRECISION,
    tx_long_ratio_shift_1m DOUBLE PRECISION,
    tx_perp_dominance_pct DOUBLE PRECISION,
    tx_total_volume_usd DOUBLE PRECISION,
    tx_volume_acceleration_ratio DOUBLE PRECISION,
    tx_volume_surge_ratio DOUBLE PRECISION,
    tx_whale_volume_pct DOUBLE PRECISION,
    tx_avg_trade_size DOUBLE PRECISION,
    tx_trades_per_second DOUBLE PRECISION,
    tx_buy_trade_pct DOUBLE PRECISION,
    tx_price_change_1m DOUBLE PRECISION,
    tx_price_volatility_pct DOUBLE PRECISION,
    tx_cumulative_buy_flow_5m DOUBLE PRECISION,
    tx_trade_count INT,
    tx_large_trade_count INT,
    tx_vwap DOUBLE PRECISION,
    pattern_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trail_buyin_id ON buyin_trail_minutes(buyin_id);
CREATE INDEX IF NOT EXISTS idx_trail_minute ON buyin_trail_minutes(minute);
CREATE INDEX IF NOT EXISTS idx_trail_created_at ON buyin_trail_minutes(created_at);

-- Wallet Profiles (24h hot storage)
CREATE TABLE IF NOT EXISTS wallet_profiles (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    price_cycle BIGINT NOT NULL,
    price_cycle_start_time TIMESTAMP,
    price_cycle_end_time TIMESTAMP,
    trade_entry_price_org DECIMAL(20,8) NOT NULL,
    stablecoin_amount DOUBLE PRECISION,
    trade_entry_price DECIMAL(20,8) NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    long_short VARCHAR(10),
    short SMALLINT DEFAULT 2,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_profiles_wallet ON wallet_profiles(wallet_address);
CREATE INDEX IF NOT EXISTS idx_profiles_trade_ts ON wallet_profiles(trade_timestamp);
CREATE INDEX IF NOT EXISTS idx_profiles_created_at ON wallet_profiles(created_at);

-- Wallet Profiles State (PERSISTENT - survives master2 restarts)
-- Tracks last processed trade_id per threshold for incremental processing
CREATE TABLE IF NOT EXISTS wallet_profiles_state (
    id SERIAL PRIMARY KEY,
    threshold DECIMAL(5,2) NOT NULL UNIQUE,
    last_trade_id BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_state_threshold ON wallet_profiles_state(threshold);

-- Jupiter Prices (24h hot storage)
CREATE TABLE IF NOT EXISTS prices (
    id BIGSERIAL PRIMARY KEY,
    token VARCHAR(50) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    source VARCHAR(50) DEFAULT 'jupiter'
);

CREATE INDEX IF NOT EXISTS idx_prices_token ON prices(token);
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp);

-- Price Analysis (24h hot storage)
CREATE TABLE IF NOT EXISTS price_analysis (
    id BIGSERIAL PRIMARY KEY,
    coin_id INT NOT NULL,
    price_point_id BIGINT,
    sequence_start_id BIGINT,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    current_price DECIMAL(20,8) NOT NULL,
    percent_threshold DECIMAL(5,2) DEFAULT 0.10,
    percent_increase DECIMAL(10,4),
    highest_price_recorded DECIMAL(20,8),
    lowest_price_recorded DECIMAL(20,8),
    procent_change_from_highest_price_recorded DECIMAL(10,4) DEFAULT 0.0,
    percent_increase_from_lowest DECIMAL(10,4) DEFAULT 0.0,
    price_cycle BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    highest_climb DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_analysis_coin ON price_analysis(coin_id);
CREATE INDEX IF NOT EXISTS idx_analysis_created ON price_analysis(created_at);
CREATE INDEX IF NOT EXISTS idx_analysis_cycle ON price_analysis(price_cycle);

-- Cycle Tracker (24h hot storage for completed cycles)
CREATE TABLE IF NOT EXISTS cycle_tracker (
    id BIGSERIAL PRIMARY KEY,
    coin_id INT NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    cycle_start_time TIMESTAMP NOT NULL,
    cycle_end_time TIMESTAMP,
    sequence_start_id BIGINT,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    max_percent_increase DECIMAL(10,4) NOT NULL,
    max_percent_increase_from_lowest DECIMAL(10,4) NOT NULL,
    total_data_points INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cycle_coin ON cycle_tracker(coin_id);
CREATE INDEX IF NOT EXISTS idx_cycle_start ON cycle_tracker(cycle_start_time);
CREATE INDEX IF NOT EXISTS idx_cycle_created_at ON cycle_tracker(created_at);

-- SOL Stablecoin Trades (24h hot storage)
CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
    id BIGSERIAL PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    signature VARCHAR(255),
    trade_timestamp TIMESTAMP NOT NULL,
    stablecoin_amount DECIMAL(20,8),
    sol_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    perp_direction VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trades_wallet ON sol_stablecoin_trades(wallet_address);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON sol_stablecoin_trades(trade_timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_created_at ON sol_stablecoin_trades(created_at);

-- Order Book Features (24h hot storage)
CREATE TABLE IF NOT EXISTS order_book_features (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    mid_price DOUBLE PRECISION,
    spread_bps DOUBLE PRECISION,
    bid_liquidity DOUBLE PRECISION,
    ask_liquidity DOUBLE PRECISION,
    volume_imbalance DOUBLE PRECISION,
    depth_imbalance_ratio DOUBLE PRECISION,
    microprice DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ob_timestamp ON order_book_features(timestamp);
CREATE INDEX IF NOT EXISTS idx_ob_created_at ON order_book_features(created_at);

-- Job Execution Metrics (persistent)
CREATE TABLE IF NOT EXISTS job_execution_metrics (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    execution_time DOUBLE PRECISION,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_job ON job_execution_metrics(job_name);
CREATE INDEX IF NOT EXISTS idx_metrics_executed_at ON job_execution_metrics(executed_at);

-- Whale Movements (24h hot storage, dual-write for history)
CREATE TABLE IF NOT EXISTS whale_movements (
    id BIGSERIAL PRIMARY KEY,
    signature VARCHAR(255),
    wallet_address VARCHAR(255) NOT NULL,
    whale_type VARCHAR(50),
    current_balance DECIMAL(20,8),
    sol_change DECIMAL(20,8),
    abs_change DECIMAL(20,8),
    percentage_moved DECIMAL(10,4),
    direction VARCHAR(10),
    action VARCHAR(50),
    movement_significance VARCHAR(50),
    previous_balance DECIMAL(20,8),
    fee_paid DECIMAL(20,8),
    block_time BIGINT,
    timestamp TIMESTAMP NOT NULL,
    received_at TIMESTAMP,
    slot BIGINT,
    has_perp_position BOOLEAN,
    perp_platform VARCHAR(50),
    perp_direction VARCHAR(10),
    perp_size DECIMAL(20,8),
    perp_leverage DECIMAL(10,2),
    perp_entry_price DECIMAL(20,8),
    raw_data_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_whale_wallet ON whale_movements(wallet_address);
CREATE INDEX IF NOT EXISTS idx_whale_timestamp ON whale_movements(timestamp);
CREATE INDEX IF NOT EXISTS idx_whale_created_at ON whale_movements(created_at);


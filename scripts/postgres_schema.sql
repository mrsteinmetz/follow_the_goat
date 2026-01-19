-- PostgreSQL Schema Migration
-- ============================
-- Complete schema for all tables in the Follow The Goat system
-- Migrated from DuckDB schema definitions
--
-- Run this script to create all necessary tables in PostgreSQL:
--   psql -U postgres -d follow_the_goat -f scripts/postgres_schema.sql

-- =============================================================================
-- PRICES TABLE (already exists from master.py dual-write, but included for completeness)
-- =============================================================================

CREATE TABLE IF NOT EXISTS prices (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    token VARCHAR(20) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    source VARCHAR(20) DEFAULT 'jupiter',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_prices_token_timestamp ON prices(token, timestamp);
CREATE INDEX IF NOT EXISTS idx_prices_created_at ON prices(created_at);

-- =============================================================================
-- SOL STABLECOIN TRADES (already exists from master.py dual-write)
-- =============================================================================

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
CREATE INDEX IF NOT EXISTS idx_trades_direction ON sol_stablecoin_trades(direction);
CREATE INDEX IF NOT EXISTS idx_trades_wallet_id ON sol_stablecoin_trades(wallet_address, id);

-- =============================================================================
-- ORDER BOOK FEATURES (already exists from master.py dual-write)
-- =============================================================================

CREATE TABLE IF NOT EXISTS order_book_features (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    mid_price DOUBLE PRECISION,
    spread_bps DOUBLE PRECISION,
    bid_ask_ratio DOUBLE PRECISION,
    total_bid_volume DOUBLE PRECISION,
    total_ask_volume DOUBLE PRECISION,
    volume_imbalance DOUBLE PRECISION,
    depth_imbalance DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON order_book_features(timestamp);
CREATE INDEX IF NOT EXISTS idx_orderbook_symbol ON order_book_features(symbol);

-- =============================================================================
-- WHALE MOVEMENTS (already exists from master.py dual-write)
-- =============================================================================

CREATE TABLE IF NOT EXISTS whale_movements (
    id BIGSERIAL PRIMARY KEY,
    signature VARCHAR(255),
    wallet_address VARCHAR(255) NOT NULL,
    whale_type VARCHAR(50),
    current_balance DOUBLE PRECISION,
    sol_change DOUBLE PRECISION,
    abs_change DOUBLE PRECISION,
    percentage_moved DOUBLE PRECISION,
    direction VARCHAR(10),
    action VARCHAR(50),
    movement_significance VARCHAR(50),
    previous_balance DOUBLE PRECISION,
    fee_paid DOUBLE PRECISION,
    block_time BIGINT,
    timestamp TIMESTAMP,
    received_at TIMESTAMP,
    slot BIGINT,
    has_perp_position BOOLEAN,
    perp_platform VARCHAR(50),
    perp_direction VARCHAR(10),
    perp_size DOUBLE PRECISION,
    perp_leverage DOUBLE PRECISION,
    perp_entry_price DOUBLE PRECISION,
    raw_data_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_whale_wallet ON whale_movements(wallet_address);
CREATE INDEX IF NOT EXISTS idx_whale_timestamp ON whale_movements(timestamp);
CREATE INDEX IF NOT EXISTS idx_whale_signature ON whale_movements(signature);
CREATE INDEX IF NOT EXISTS idx_whale_type ON whale_movements(whale_type);

-- =============================================================================
-- CYCLE TRACKER (new table for price cycle analysis)
-- =============================================================================

CREATE TABLE IF NOT EXISTS cycle_tracker (
    id BIGSERIAL PRIMARY KEY,
    coin_id INTEGER NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    cycle_start_time TIMESTAMP NOT NULL,
    cycle_end_time TIMESTAMP,
    sequence_start_id BIGINT NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    max_percent_increase DECIMAL(10,4) NOT NULL,
    max_percent_increase_from_lowest DECIMAL(10,4) NOT NULL,
    total_data_points INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cycle_tracker_coin ON cycle_tracker(coin_id);
CREATE INDEX IF NOT EXISTS idx_cycle_tracker_start ON cycle_tracker(cycle_start_time);
CREATE INDEX IF NOT EXISTS idx_cycle_tracker_threshold ON cycle_tracker(threshold);
CREATE INDEX IF NOT EXISTS idx_cycle_tracker_end ON cycle_tracker(cycle_end_time);

-- =============================================================================
-- FOLLOW THE GOAT PLAYS (config table - full data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS follow_the_goat_plays (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    find_wallets_sql JSONB,
    max_buys_per_cycle INTEGER DEFAULT 1,
    sell_logic JSONB,
    live_trades INTEGER DEFAULT 0,
    name VARCHAR(60),
    description VARCHAR(500),
    sorting INTEGER DEFAULT 10,
    short_play INTEGER DEFAULT 0,
    tricker_on_perp JSONB,
    timing_conditions JSONB,
    bundle_trades JSONB,
    play_log JSONB,
    cashe_wallets JSONB,
    cashe_wallets_settings JSONB,
    pattern_validator JSONB,
    pattern_validator_enable INTEGER DEFAULT 0,
    pattern_update_by_ai INTEGER DEFAULT 1,
    pattern_version_id INTEGER,
    is_active INTEGER DEFAULT 1,
    project_id INTEGER,
    project_ids JSONB,
    project_version INTEGER
);

-- =============================================================================
-- FOLLOW THE GOAT BUYINS (trading table)
-- =============================================================================

CREATE TABLE IF NOT EXISTS follow_the_goat_buyins (
    id BIGSERIAL PRIMARY KEY,
    play_id INTEGER,
    wallet_address VARCHAR(255) NOT NULL,
    original_trade_id BIGINT NOT NULL,
    tolerance DOUBLE PRECISION DEFAULT 0.3,
    price_cycle INTEGER,
    trade_signature VARCHAR(255),
    block_timestamp TIMESTAMP,
    quote_amount DECIMAL(20,8),
    base_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    is_buy BOOLEAN DEFAULT TRUE,
    followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    our_entry_price DECIMAL(20,8),
    our_position_size DECIMAL(20,8),
    our_exit_price DECIMAL(20,8),
    our_exit_timestamp TIMESTAMP,
    our_profit_loss DECIMAL(20,8),
    our_status VARCHAR(20) DEFAULT 'pending',
    swap_response JSONB,
    sell_swap_response JSONB,
    price_movements JSONB,
    live_trade INTEGER DEFAULT 0,
    higest_price_reached DECIMAL(20,8),
    current_price DECIMAL(20,8),
    entry_log JSONB,
    fifteen_min_trail JSONB,
    pattern_validator_log JSONB,
    potential_gains DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_buyins_wallet ON follow_the_goat_buyins(wallet_address);
CREATE INDEX IF NOT EXISTS idx_buyins_followed_at ON follow_the_goat_buyins(followed_at);
CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status);
CREATE INDEX IF NOT EXISTS idx_buyins_play_id ON follow_the_goat_buyins(play_id);
CREATE INDEX IF NOT EXISTS idx_buyins_query_opt ON follow_the_goat_buyins(followed_at DESC, our_status, play_id);

-- =============================================================================
-- FOLLOW THE GOAT BUYINS PRICE CHECKS (trailing stop data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_price_checks (
    id BIGSERIAL PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    checked_at TIMESTAMP NOT NULL,
    current_price DECIMAL(20,8) NOT NULL,
    entry_price DECIMAL(20,8),
    highest_price DECIMAL(20,8),
    reference_price DECIMAL(20,8),
    gain_from_entry DECIMAL(10,6) NOT NULL,
    drop_from_high DECIMAL(10,6) NOT NULL,
    drop_from_entry DECIMAL(10,6),
    drop_from_reference DECIMAL(10,6),
    tolerance DECIMAL(10,6) NOT NULL,
    basis VARCHAR(10),
    bucket VARCHAR(10),
    applied_rule JSONB,
    should_sell BOOLEAN DEFAULT FALSE,
    is_backfill BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_price_checks_buyin ON follow_the_goat_buyins_price_checks(buyin_id);
CREATE INDEX IF NOT EXISTS idx_price_checks_checked_at ON follow_the_goat_buyins_price_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_price_checks_should_sell ON follow_the_goat_buyins_price_checks(should_sell);

-- =============================================================================
-- FOLLOW THE GOAT TRACKING (tracks last processed trade per wallet)
-- =============================================================================

CREATE TABLE IF NOT EXISTS follow_the_goat_tracking (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL UNIQUE,
    last_trade_id BIGINT DEFAULT 0,
    last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tracking_wallet ON follow_the_goat_tracking(wallet_address);

-- =============================================================================
-- PRICE POINTS (legacy compatibility table)
-- =============================================================================

CREATE TABLE IF NOT EXISTS price_points (
    id BIGSERIAL PRIMARY KEY,
    ts_idx BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    coin_id INTEGER DEFAULT 5
);

CREATE INDEX IF NOT EXISTS idx_price_points_created_at ON price_points(created_at);
CREATE INDEX IF NOT EXISTS idx_price_points_coin_id ON price_points(coin_id);

-- =============================================================================
-- PRICE ANALYSIS (computed analysis data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS price_analysis (
    id SERIAL PRIMARY KEY,
    coin_id INTEGER NOT NULL,
    price_point_id BIGINT NOT NULL,
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
    highest_climb DOUBLE PRECISION,
    highest_climb_01 DECIMAL(10,4),
    highest_climb_02 DECIMAL(10,4),
    highest_climb_03 DECIMAL(10,4),
    highest_climb_04 DECIMAL(10,4),
    highest_climb_05 DECIMAL(10,4)
);

CREATE INDEX IF NOT EXISTS idx_price_analysis_coin ON price_analysis(coin_id);
CREATE INDEX IF NOT EXISTS idx_price_analysis_created_at ON price_analysis(created_at);
CREATE INDEX IF NOT EXISTS idx_price_analysis_price_cycle ON price_analysis(price_cycle);

-- =============================================================================
-- WALLET PROFILES (computed wallet behavior data)
-- =============================================================================

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

CREATE INDEX IF NOT EXISTS idx_wallet_profiles_wallet ON wallet_profiles(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_threshold ON wallet_profiles(threshold);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_trade_timestamp ON wallet_profiles(trade_timestamp);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_price_cycle ON wallet_profiles(price_cycle);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_short ON wallet_profiles(short);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_trade_threshold ON wallet_profiles(trade_id, threshold);

-- =============================================================================
-- WALLET PROFILES STATE (aggregated profile statistics)
-- =============================================================================

CREATE TABLE IF NOT EXISTS wallet_profiles_state (
    threshold DECIMAL(5,2) PRIMARY KEY,
    total_profiles BIGINT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- PATTERN CONFIG PROJECTS (full data - not time-based)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pattern_config_projects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pattern_projects_name ON pattern_config_projects(name);
CREATE INDEX IF NOT EXISTS idx_pattern_projects_created_at ON pattern_config_projects(created_at);

-- =============================================================================
-- PATTERN CONFIG FILTERS (full data - not time-based)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pattern_config_filters (
    id SERIAL PRIMARY KEY,
    project_id INTEGER,
    name VARCHAR(255) NOT NULL,
    section VARCHAR(100),
    minute SMALLINT,
    field_name VARCHAR(100) NOT NULL,
    field_column VARCHAR(100),
    from_value DECIMAL(20,8),
    to_value DECIMAL(20,8),
    include_null SMALLINT DEFAULT 0,
    exclude_mode SMALLINT DEFAULT 0,
    play_id INTEGER,
    is_active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pattern_filters_project_id ON pattern_config_filters(project_id);
CREATE INDEX IF NOT EXISTS idx_pattern_filters_section_minute ON pattern_config_filters(section, minute);
CREATE INDEX IF NOT EXISTS idx_pattern_filters_is_active ON pattern_config_filters(is_active);

-- =============================================================================
-- BUYIN TRAIL MINUTES (flattened 15-minute trail data)
-- =============================================================================

CREATE TABLE IF NOT EXISTS buyin_trail_minutes (
    buyin_id BIGINT NOT NULL,
    minute SMALLINT NOT NULL,
    
    -- Price Movements (pm_) - 22 columns
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
    
    -- Order Book Signals (ob_) - 22 columns
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
    ob_sample_count INTEGER,
    ob_coverage_seconds INTEGER,
    
    -- Transactions (tx_) - 24 columns
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
    tx_trade_count INTEGER,
    tx_large_trade_count INTEGER,
    tx_vwap DOUBLE PRECISION,
    
    -- Whale Activity (wh_) - 28 columns
    wh_net_flow_ratio DOUBLE PRECISION,
    wh_flow_shift_1m DOUBLE PRECISION,
    wh_flow_trend_3m DOUBLE PRECISION,
    wh_accumulation_ratio DOUBLE PRECISION,
    wh_strong_accumulation DOUBLE PRECISION,
    wh_cumulative_flow_5m DOUBLE PRECISION,
    wh_total_sol_moved DOUBLE PRECISION,
    wh_inflow_share_pct DOUBLE PRECISION,
    wh_outflow_share_pct DOUBLE PRECISION,
    wh_net_flow_strength_pct DOUBLE PRECISION,
    wh_strong_accumulation_pct DOUBLE PRECISION,
    wh_strong_distribution_pct DOUBLE PRECISION,
    wh_activity_surge_ratio DOUBLE PRECISION,
    wh_movement_count INTEGER,
    wh_massive_move_pct DOUBLE PRECISION,
    wh_avg_wallet_pct_moved DOUBLE PRECISION,
    wh_largest_move_dominance DOUBLE PRECISION,
    wh_distribution_pressure_pct DOUBLE PRECISION,
    wh_outflow_surge_pct DOUBLE PRECISION,
    wh_movement_imbalance_pct DOUBLE PRECISION,
    wh_inflow_sol DOUBLE PRECISION,
    wh_outflow_sol DOUBLE PRECISION,
    wh_net_flow_sol DOUBLE PRECISION,
    wh_inflow_count INTEGER,
    wh_outflow_count INTEGER,
    wh_massive_move_count INTEGER,
    wh_max_move_size DOUBLE PRECISION,
    wh_strong_distribution DOUBLE PRECISION,
    
    -- Pattern Detection (pat_) - 27 columns
    pat_breakout_score DOUBLE PRECISION,
    pat_detected_count INTEGER,
    pat_detected_list VARCHAR(255),
    pat_asc_tri_detected BOOLEAN,
    pat_asc_tri_confidence DOUBLE PRECISION,
    pat_asc_tri_resistance_level DOUBLE PRECISION,
    pat_asc_tri_support_level DOUBLE PRECISION,
    pat_asc_tri_compression_ratio DOUBLE PRECISION,
    pat_bull_flag_detected BOOLEAN,
    pat_bull_flag_confidence DOUBLE PRECISION,
    pat_bull_flag_pole_height_pct DOUBLE PRECISION,
    pat_bull_flag_retracement_pct DOUBLE PRECISION,
    pat_bull_pennant_detected BOOLEAN,
    pat_bull_pennant_confidence DOUBLE PRECISION,
    pat_bull_pennant_compression_ratio DOUBLE PRECISION,
    pat_fall_wedge_detected BOOLEAN,
    pat_fall_wedge_confidence DOUBLE PRECISION,
    pat_fall_wedge_contraction DOUBLE PRECISION,
    pat_cup_handle_detected BOOLEAN,
    pat_cup_handle_confidence DOUBLE PRECISION,
    pat_cup_handle_depth_pct DOUBLE PRECISION,
    pat_inv_hs_detected BOOLEAN,
    pat_inv_hs_confidence DOUBLE PRECISION,
    pat_inv_hs_neckline DOUBLE PRECISION,
    pat_swing_trend VARCHAR(20),
    pat_swing_higher_lows BOOLEAN,
    pat_swing_lower_highs BOOLEAN,

    -- Micro Patterns (mp_) - 10 columns
    mp_volume_divergence_detected BOOLEAN,
    mp_volume_divergence_confidence DOUBLE PRECISION,
    mp_order_book_squeeze_detected BOOLEAN,
    mp_order_book_squeeze_confidence DOUBLE PRECISION,
    mp_whale_stealth_accumulation_detected BOOLEAN,
    mp_whale_stealth_accumulation_confidence DOUBLE PRECISION,
    mp_momentum_acceleration_detected BOOLEAN,
    mp_momentum_acceleration_confidence DOUBLE PRECISION,
    mp_microstructure_shift_detected BOOLEAN,
    mp_microstructure_shift_confidence DOUBLE PRECISION,
    
    -- Second Prices Summary (sp_) - 9 columns
    sp_price_count INTEGER,
    sp_min_price DOUBLE PRECISION,
    sp_max_price DOUBLE PRECISION,
    sp_start_price DOUBLE PRECISION,
    sp_end_price DOUBLE PRECISION,
    sp_price_range_pct DOUBLE PRECISION,
    sp_total_change_pct DOUBLE PRECISION,
    sp_volatility_pct DOUBLE PRECISION,
    sp_avg_price DOUBLE PRECISION,
    
    -- BTC Price Movements (btc_) - 6 columns
    btc_price_change_1m DOUBLE PRECISION,
    btc_price_change_5m DOUBLE PRECISION,
    btc_price_change_10m DOUBLE PRECISION,
    btc_volatility_pct DOUBLE PRECISION,
    btc_open_price DOUBLE PRECISION,
    btc_close_price DOUBLE PRECISION,
    
    -- ETH Price Movements (eth_) - 6 columns
    eth_price_change_1m DOUBLE PRECISION,
    eth_price_change_5m DOUBLE PRECISION,
    eth_price_change_10m DOUBLE PRECISION,
    eth_volatility_pct DOUBLE PRECISION,
    eth_open_price DOUBLE PRECISION,
    eth_close_price DOUBLE PRECISION,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (buyin_id, minute)
);

CREATE INDEX IF NOT EXISTS idx_trail_buyin_id ON buyin_trail_minutes(buyin_id);
CREATE INDEX IF NOT EXISTS idx_trail_minute ON buyin_trail_minutes(minute);
CREATE INDEX IF NOT EXISTS idx_trail_created_at ON buyin_trail_minutes(created_at);
CREATE INDEX IF NOT EXISTS idx_trail_breakout_score ON buyin_trail_minutes(pat_breakout_score);

-- =============================================================================
-- TRADE FILTER VALUES (normalized filter storage)
-- =============================================================================

CREATE TABLE IF NOT EXISTS trade_filter_values (
    id BIGSERIAL PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    minute INTEGER NOT NULL,
    filter_name VARCHAR(100) NOT NULL,
    filter_value DOUBLE PRECISION,
    is_ratio SMALLINT DEFAULT 0,
    section VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tfv_unique ON trade_filter_values(buyin_id, minute, filter_name);
CREATE INDEX IF NOT EXISTS idx_tfv_buyin_id ON trade_filter_values(buyin_id);
CREATE INDEX IF NOT EXISTS idx_tfv_filter_name ON trade_filter_values(filter_name);
CREATE INDEX IF NOT EXISTS idx_tfv_minute ON trade_filter_values(minute);
CREATE INDEX IF NOT EXISTS idx_tfv_section ON trade_filter_values(section);
CREATE INDEX IF NOT EXISTS idx_tfv_is_ratio ON trade_filter_values(is_ratio);

-- =============================================================================
-- FILTER ANALYSIS TABLES (for auto filter management)
-- =============================================================================

CREATE TABLE IF NOT EXISTS filter_fields_catalog (
    id SERIAL PRIMARY KEY,
    field_name VARCHAR(100) NOT NULL,
    section VARCHAR(100),
    minute SMALLINT,
    field_type VARCHAR(50) DEFAULT 'numeric',
    description TEXT,
    column_name VARCHAR(100),
    column_prefix VARCHAR(20),
    data_type VARCHAR(50),
    value_type VARCHAR(50),
    is_filterable BOOLEAN DEFAULT TRUE,
    display_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_filter_catalog_section ON filter_fields_catalog(section);
CREATE INDEX IF NOT EXISTS idx_filter_catalog_column ON filter_fields_catalog(column_name);

CREATE TABLE IF NOT EXISTS filter_reference_suggestions (
    id SERIAL PRIMARY KEY,
    filter_field_id INTEGER,
    column_name VARCHAR(100) NOT NULL,
    from_value DOUBLE PRECISION,
    to_value DOUBLE PRECISION,
    total_trades INTEGER,
    good_trades_before INTEGER,
    bad_trades_before INTEGER,
    good_trades_after INTEGER,
    bad_trades_after INTEGER,
    good_trades_kept_pct DOUBLE PRECISION,
    bad_trades_removed_pct DOUBLE PRECISION,
    bad_negative_count INTEGER,
    bad_0_to_01_count INTEGER,
    bad_01_to_02_count INTEGER,
    bad_02_to_03_count INTEGER,
    analysis_hours INTEGER,
    minute_analyzed INTEGER,
    section VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_filter_suggestions_column ON filter_reference_suggestions(column_name);
CREATE INDEX IF NOT EXISTS idx_filter_suggestions_section ON filter_reference_suggestions(section);
CREATE INDEX IF NOT EXISTS idx_filter_suggestions_minute ON filter_reference_suggestions(minute_analyzed);

CREATE TABLE IF NOT EXISTS filter_combinations (
    id SERIAL PRIMARY KEY,
    combination_name VARCHAR(255),
    filter_count INTEGER,
    filter_ids TEXT,
    filter_columns TEXT,
    total_trades INTEGER,
    good_trades_before INTEGER,
    bad_trades_before INTEGER,
    good_trades_after INTEGER,
    bad_trades_after INTEGER,
    good_trades_kept_pct DOUBLE PRECISION,
    bad_trades_removed_pct DOUBLE PRECISION,
    best_single_bad_removed_pct DOUBLE PRECISION,
    improvement_over_single DOUBLE PRECISION,
    bad_negative_count INTEGER,
    bad_0_to_01_count INTEGER,
    bad_01_to_02_count INTEGER,
    bad_02_to_03_count INTEGER,
    analysis_hours INTEGER,
    minute_analyzed INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_filter_combos_filter_count ON filter_combinations(filter_count);
CREATE INDEX IF NOT EXISTS idx_filter_combos_minute ON filter_combinations(minute_analyzed);

-- =============================================================================
-- AUTO FILTER SETTINGS (configuration for pattern generator)
-- =============================================================================

CREATE TABLE IF NOT EXISTS auto_filter_settings (
    id SERIAL PRIMARY KEY,
    setting_key VARCHAR(100) UNIQUE NOT NULL,
    setting_value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_auto_filter_settings_key ON auto_filter_settings(setting_key);

-- =============================================================================
-- AI PLAY UPDATES (tracks automatic filter updates to plays)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ai_play_updates (
    id SERIAL PRIMARY KEY,
    play_id INTEGER NOT NULL,
    play_name VARCHAR(255),
    project_id INTEGER,
    project_name VARCHAR(255),
    pattern_count INTEGER DEFAULT 0,
    filters_applied INTEGER DEFAULT 0,
    run_id VARCHAR(50),
    status VARCHAR(50) DEFAULT 'success',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_play_updates_play_id ON ai_play_updates(play_id);
CREATE INDEX IF NOT EXISTS idx_ai_play_updates_updated_at ON ai_play_updates(updated_at);

-- =============================================================================
-- FILTER SCENARIO RESULTS (tracks multi-scenario testing)
-- =============================================================================

CREATE TABLE IF NOT EXISTS filter_scenario_results (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(50),
    scenario_name VARCHAR(200),
    settings JSONB,
    filter_count INTEGER,
    bad_trades_removed_pct DOUBLE PRECISION,
    good_trades_kept_pct DOUBLE PRECISION,
    score DOUBLE PRECISION,
    filters_applied JSONB,
    rank INTEGER,
    was_selected BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scenario_results_run_id ON filter_scenario_results(run_id);
CREATE INDEX IF NOT EXISTS idx_scenario_results_score ON filter_scenario_results(score DESC);
CREATE INDEX IF NOT EXISTS idx_scenario_results_created_at ON filter_scenario_results(created_at);

-- =============================================================================
-- JOB EXECUTION METRICS (for scheduler monitoring)
-- =============================================================================

CREATE TABLE IF NOT EXISTS job_execution_metrics (
    id BIGSERIAL PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_metrics_job_id ON job_execution_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_job_metrics_started_at ON job_execution_metrics(started_at);
CREATE INDEX IF NOT EXISTS idx_job_metrics_job_started ON job_execution_metrics(job_id, started_at);

-- =============================================================================
-- SCHEMA COMPLETE
-- =============================================================================

-- Verify all tables exist
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name IN (
        'prices', 'sol_stablecoin_trades', 'order_book_features', 'whale_movements',
        'cycle_tracker', 'follow_the_goat_plays', 'follow_the_goat_buyins',
        'follow_the_goat_buyins_price_checks', 'follow_the_goat_tracking',
        'price_points', 'price_analysis', 'wallet_profiles', 'wallet_profiles_state',
        'pattern_config_projects', 'pattern_config_filters', 'buyin_trail_minutes',
        'trade_filter_values', 'filter_fields_catalog', 'filter_reference_suggestions',
        'filter_combinations', 'ai_play_updates', 'job_execution_metrics'
    );
    
    RAISE NOTICE 'Schema migration complete! Created % tables.', table_count;
END $$;

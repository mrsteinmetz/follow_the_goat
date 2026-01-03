-- =============================================================================
-- MySQL Archive Schema for Follow The Goat
-- =============================================================================
-- These tables store archived data from DuckDB when it exceeds the hot storage
-- retention period (24h for most tables, 72h for trades).
--
-- Run this after creating the database:
--   mysql follow_the_goat_archive < scripts/mysql_archive_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Archive: follow_the_goat_buyins (72h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_archive (
    id BIGINT PRIMARY KEY,
    play_id INT,
    wallet_address VARCHAR(255) NOT NULL,
    original_trade_id BIGINT NOT NULL,
    tolerance DOUBLE DEFAULT 0.3,
    price_cycle BIGINT,
    trade_signature VARCHAR(255),
    block_timestamp DATETIME,
    quote_amount DECIMAL(20,8),
    base_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    is_buy TINYINT(1) DEFAULT 1,
    followed_at DATETIME,
    our_entry_price DECIMAL(20,8),
    our_position_size DECIMAL(20,8),
    our_exit_price DECIMAL(20,8),
    our_exit_timestamp DATETIME,
    our_profit_loss DECIMAL(20,8),
    our_status VARCHAR(20) DEFAULT 'pending',
    swap_response JSON,
    sell_swap_response JSON,
    price_movements JSON,
    live_trade INT DEFAULT 0,
    higest_price_reached DECIMAL(20,8),
    current_price DECIMAL(20,8),
    entry_log JSON,
    fifteen_min_trail JSON,
    pattern_validator_log JSON,
    potential_gains FLOAT,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_wallet (wallet_address),
    INDEX idx_archive_followed_at (followed_at),
    INDEX idx_archive_status (our_status),
    INDEX idx_archive_play_id (play_id),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: follow_the_goat_buyins_price_checks (72h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_price_checks_archive (
    id BIGINT UNSIGNED PRIMARY KEY,
    buyin_id INT UNSIGNED NOT NULL,
    checked_at DATETIME NOT NULL,
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
    applied_rule JSON,
    should_sell TINYINT(1) DEFAULT 0,
    is_backfill TINYINT(1) DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_buyin (buyin_id),
    INDEX idx_archive_checked_at (checked_at),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: buyin_trail_minutes (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buyin_trail_minutes_archive (
    id BIGINT PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    minute TINYINT NOT NULL,
    
    -- Price Movements (pm_)
    pm_price_change_1m DOUBLE,
    pm_momentum_volatility_ratio DOUBLE,
    pm_momentum_acceleration_1m DOUBLE,
    pm_price_change_5m DOUBLE,
    pm_price_change_10m DOUBLE,
    pm_volatility_pct DOUBLE,
    pm_body_range_ratio DOUBLE,
    pm_volatility_surge_ratio DOUBLE,
    pm_price_stddev_pct DOUBLE,
    pm_trend_consistency_3m DOUBLE,
    pm_cumulative_return_5m DOUBLE,
    pm_candle_body_pct DOUBLE,
    pm_upper_wick_pct DOUBLE,
    pm_lower_wick_pct DOUBLE,
    pm_wick_balance_ratio DOUBLE,
    pm_price_vs_ma5_pct DOUBLE,
    pm_breakout_strength_10m DOUBLE,
    pm_open_price DOUBLE,
    pm_high_price DOUBLE,
    pm_low_price DOUBLE,
    pm_close_price DOUBLE,
    pm_avg_price DOUBLE,
    
    -- Order Book Signals (ob_)
    ob_mid_price DOUBLE,
    ob_price_change_1m DOUBLE,
    ob_price_change_5m DOUBLE,
    ob_price_change_10m DOUBLE,
    ob_volume_imbalance DOUBLE,
    ob_imbalance_shift_1m DOUBLE,
    ob_imbalance_trend_3m DOUBLE,
    ob_depth_imbalance_ratio DOUBLE,
    ob_bid_liquidity_share_pct DOUBLE,
    ob_ask_liquidity_share_pct DOUBLE,
    ob_depth_imbalance_pct DOUBLE,
    ob_total_liquidity DOUBLE,
    ob_liquidity_change_3m DOUBLE,
    ob_microprice_deviation DOUBLE,
    ob_microprice_acceleration_2m DOUBLE,
    ob_spread_bps DOUBLE,
    ob_aggression_ratio DOUBLE,
    ob_vwap_spread_bps DOUBLE,
    ob_net_flow_5m DOUBLE,
    ob_net_flow_to_liquidity_ratio DOUBLE,
    ob_sample_count INT,
    ob_coverage_seconds INT,
    
    -- Transactions (tx_)
    tx_buy_sell_pressure DOUBLE,
    tx_buy_volume_pct DOUBLE,
    tx_sell_volume_pct DOUBLE,
    tx_pressure_shift_1m DOUBLE,
    tx_pressure_trend_3m DOUBLE,
    tx_long_short_ratio DOUBLE,
    tx_long_volume_pct DOUBLE,
    tx_short_volume_pct DOUBLE,
    tx_perp_position_skew_pct DOUBLE,
    tx_long_ratio_shift_1m DOUBLE,
    tx_perp_dominance_pct DOUBLE,
    tx_total_volume_usd DOUBLE,
    tx_volume_acceleration_ratio DOUBLE,
    tx_volume_surge_ratio DOUBLE,
    tx_whale_volume_pct DOUBLE,
    tx_avg_trade_size DOUBLE,
    tx_trades_per_second DOUBLE,
    tx_buy_trade_pct DOUBLE,
    tx_price_change_1m DOUBLE,
    tx_price_volatility_pct DOUBLE,
    tx_cumulative_buy_flow_5m DOUBLE,
    tx_trade_count INT,
    tx_large_trade_count INT,
    tx_vwap DOUBLE,
    
    -- Pattern fields (simplified - store as JSON for flexibility)
    pattern_data JSON,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_buyin_id (buyin_id),
    INDEX idx_archive_minute (minute),
    INDEX idx_archive_created_at (created_at),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: wallet_profiles (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallet_profiles_archive (
    id BIGINT PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_timestamp DATETIME NOT NULL,
    price_cycle BIGINT NOT NULL,
    price_cycle_start_time DATETIME,
    price_cycle_end_time DATETIME,
    trade_entry_price_org DECIMAL(20,8) NOT NULL,
    stablecoin_amount DOUBLE,
    trade_entry_price DECIMAL(20,8) NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    long_short VARCHAR(10),
    short TINYINT DEFAULT 2,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_wallet (wallet_address),
    INDEX idx_archive_threshold (threshold),
    INDEX idx_archive_trade_timestamp (trade_timestamp),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: price_points (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_points_archive (
    id BIGINT PRIMARY KEY,
    ts_idx BIGINT NOT NULL,
    value DOUBLE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    coin_id INT DEFAULT 5,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_created_at (created_at),
    INDEX idx_archive_coin_id (coin_id),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: price_analysis (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_analysis_archive (
    id BIGINT PRIMARY KEY,
    coin_id INT NOT NULL,
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
    created_at DATETIME NOT NULL,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    highest_climb DOUBLE,
    highest_climb_01 DECIMAL(10,4),
    highest_climb_02 DECIMAL(10,4),
    highest_climb_03 DECIMAL(10,4),
    highest_climb_04 DECIMAL(10,4),
    highest_climb_05 DECIMAL(10,4),
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_coin (coin_id),
    INDEX idx_archive_created_at (created_at),
    INDEX idx_archive_price_cycle (price_cycle),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: cycle_tracker (24h hot storage for completed cycles, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cycle_tracker_archive (
    id BIGINT PRIMARY KEY,
    coin_id INT NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    cycle_start_time DATETIME NOT NULL,
    cycle_end_time DATETIME,
    sequence_start_id BIGINT NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    max_percent_increase DECIMAL(10,4) NOT NULL,
    max_percent_increase_from_lowest DECIMAL(10,4) NOT NULL,
    total_data_points INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_coin (coin_id),
    INDEX idx_archive_start (cycle_start_time),
    INDEX idx_archive_threshold (threshold),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: sol_stablecoin_trades (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sol_stablecoin_trades_archive (
    id BIGINT PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    signature VARCHAR(255),
    trade_timestamp DATETIME NOT NULL,
    stablecoin_amount DECIMAL(20,8),
    sol_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    perp_direction VARCHAR(10),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_wallet (wallet_address),
    INDEX idx_archive_timestamp (trade_timestamp),
    INDEX idx_archive_direction (direction),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Archive: job_execution_metrics (24h hot storage, then archived)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_execution_metrics_archive (
    id BIGINT PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    started_at DATETIME NOT NULL,
    ended_at DATETIME NOT NULL,
    duration_ms DOUBLE NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(500),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    archived_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_archive_job_id (job_id),
    INDEX idx_archive_started_at (started_at),
    INDEX idx_archive_archived_at (archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT 'Archive tables created successfully!' AS status;
SHOW TABLES;


"""
DuckDB Schema Definitions
=========================
Central schema file for all DuckDB tables.
Mirrors MySQL tables for 24-hour hot storage.

Tables:
- follow_the_goat_plays (full data, not time-based)
- follow_the_goat_tracking (full data - tracks last processed trade per wallet)
- follow_the_goat_buyins (24hr hot)
- follow_the_goat_buyins_price_checks (24hr hot)
- price_points (24hr hot)
- price_analysis (24hr hot)
- cycle_tracker (24hr hot)
- wallet_profiles (24hr hot)
- sol_stablecoin_trades (1hr hot - for fast trade detection)
"""

# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================

SCHEMA_FOLLOW_THE_GOAT_PLAYS = """
CREATE TABLE IF NOT EXISTS follow_the_goat_plays (
    id INTEGER PRIMARY KEY,
    created_at TIMESTAMP,
    find_wallets_sql JSON,
    max_buys_per_cycle INTEGER DEFAULT 1,
    sell_logic JSON,
    live_trades INTEGER DEFAULT 0,
    name VARCHAR(60),
    description VARCHAR(500),
    sorting INTEGER DEFAULT 10,
    short_play INTEGER DEFAULT 0,
    tricker_on_perp JSON,
    timing_conditions JSON,
    bundle_trades JSON,
    play_log JSON,
    cashe_wallets JSON,
    cashe_wallets_settings JSON,
    pattern_validator JSON,
    pattern_validator_enable INTEGER DEFAULT 0,
    pattern_update_by_ai INTEGER DEFAULT 1,
    pattern_version_id INTEGER,
    is_active INTEGER DEFAULT 1,
    project_id INTEGER,
    project_ids JSON,
    project_version INTEGER
);
"""

SCHEMA_FOLLOW_THE_GOAT_BUYINS = """
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins (
    id BIGINT PRIMARY KEY,
    play_id INTEGER,
    wallet_address VARCHAR(255) NOT NULL,
    original_trade_id BIGINT NOT NULL,
    tolerance DOUBLE DEFAULT 0.3,
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
    swap_response JSON,
    sell_swap_response JSON,
    price_movements JSON,
    live_trade INTEGER DEFAULT 0,
    higest_price_reached DECIMAL(20,8),
    current_price DECIMAL(20,8),
    entry_log JSON,
    fifteen_min_trail JSON,
    pattern_validator_log JSON,
    potential_gains FLOAT
);

CREATE INDEX IF NOT EXISTS idx_buyins_wallet ON follow_the_goat_buyins(wallet_address);
CREATE INDEX IF NOT EXISTS idx_buyins_followed_at ON follow_the_goat_buyins(followed_at);
CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status);
CREATE INDEX IF NOT EXISTS idx_buyins_play_id ON follow_the_goat_buyins(play_id);
-- Composite index for common query pattern (ORDER BY followed_at + filters)
CREATE INDEX IF NOT EXISTS idx_buyins_query_opt ON follow_the_goat_buyins(followed_at DESC, our_status, play_id);
"""

SCHEMA_FOLLOW_THE_GOAT_BUYINS_PRICE_CHECKS = """
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_price_checks (
    id UBIGINT PRIMARY KEY,
    buyin_id UINTEGER NOT NULL,
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
    applied_rule JSON,
    should_sell BOOLEAN DEFAULT FALSE,
    is_backfill BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_price_checks_buyin ON follow_the_goat_buyins_price_checks(buyin_id);
CREATE INDEX IF NOT EXISTS idx_price_checks_checked_at ON follow_the_goat_buyins_price_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_price_checks_should_sell ON follow_the_goat_buyins_price_checks(should_sell);
"""

SCHEMA_PRICE_POINTS = """
CREATE TABLE IF NOT EXISTS price_points (
    id BIGINT PRIMARY KEY,
    ts_idx BIGINT NOT NULL,
    value DOUBLE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    coin_id INTEGER DEFAULT 5
);

CREATE INDEX IF NOT EXISTS idx_price_points_created_at ON price_points(created_at);
CREATE INDEX IF NOT EXISTS idx_price_points_coin_id ON price_points(coin_id);
"""

SCHEMA_PRICE_ANALYSIS = """
CREATE TABLE IF NOT EXISTS price_analysis (
    id INTEGER PRIMARY KEY,
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
    highest_climb DOUBLE,
    highest_climb_01 DECIMAL(10,4),
    highest_climb_02 DECIMAL(10,4),
    highest_climb_03 DECIMAL(10,4),
    highest_climb_04 DECIMAL(10,4),
    highest_climb_05 DECIMAL(10,4)
);

CREATE INDEX IF NOT EXISTS idx_price_analysis_coin ON price_analysis(coin_id);
CREATE INDEX IF NOT EXISTS idx_price_analysis_created_at ON price_analysis(created_at);
CREATE INDEX IF NOT EXISTS idx_price_analysis_price_cycle ON price_analysis(price_cycle);
"""

SCHEMA_CYCLE_TRACKER = """
CREATE TABLE IF NOT EXISTS cycle_tracker (
    id BIGINT PRIMARY KEY,
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
"""

SCHEMA_WALLET_PROFILES = """
CREATE TABLE IF NOT EXISTS wallet_profiles (
    id BIGINT PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_timestamp TIMESTAMP NOT NULL,
    price_cycle BIGINT NOT NULL,
    price_cycle_start_time TIMESTAMP,
    price_cycle_end_time TIMESTAMP,
    trade_entry_price_org DECIMAL(20,8) NOT NULL,
    stablecoin_amount DOUBLE,
    trade_entry_price DECIMAL(20,8) NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    long_short VARCHAR(10),
    short TINYINT DEFAULT 2,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallet_profiles_wallet ON wallet_profiles(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_threshold ON wallet_profiles(threshold);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_trade_timestamp ON wallet_profiles(trade_timestamp);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_price_cycle ON wallet_profiles(price_cycle);
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_short ON wallet_profiles(short);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_trade_threshold ON wallet_profiles(trade_id, threshold);
"""

# =============================================================================
# PATTERN CONFIG TABLES (Full data, not time-based)
# =============================================================================

SCHEMA_PATTERN_CONFIG_PROJECTS = """
CREATE TABLE IF NOT EXISTS pattern_config_projects (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pattern_projects_name ON pattern_config_projects(name);
CREATE INDEX IF NOT EXISTS idx_pattern_projects_created_at ON pattern_config_projects(created_at);
"""

SCHEMA_PATTERN_CONFIG_FILTERS = """
CREATE TABLE IF NOT EXISTS pattern_config_filters (
    id INTEGER PRIMARY KEY,
    project_id INTEGER,
    name VARCHAR(255) NOT NULL,
    section VARCHAR(100),
    minute TINYINT,
    field_name VARCHAR(100) NOT NULL,
    field_column VARCHAR(100),
    from_value DECIMAL(20,8),
    to_value DECIMAL(20,8),
    include_null TINYINT DEFAULT 0,
    exclude_mode TINYINT DEFAULT 0,
    play_id INTEGER,
    is_active TINYINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pattern_filters_project_id ON pattern_config_filters(project_id);
CREATE INDEX IF NOT EXISTS idx_pattern_filters_section_minute ON pattern_config_filters(section, minute);
CREATE INDEX IF NOT EXISTS idx_pattern_filters_is_active ON pattern_config_filters(is_active);
"""

# =============================================================================
# BUYIN TRAIL MINUTES - Flattened 15-minute trail data
# =============================================================================

SCHEMA_BUYIN_TRAIL_MINUTES = """
CREATE TABLE IF NOT EXISTS buyin_trail_minutes (
    buyin_id BIGINT NOT NULL,
    minute TINYINT NOT NULL,
    
    -- Price Movements (pm_) - 22 columns
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
    
    -- Order Book Signals (ob_) - 22 columns
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
    ob_sample_count INTEGER,
    ob_coverage_seconds INTEGER,
    
    -- Transactions (tx_) - 24 columns
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
    tx_trade_count INTEGER,
    tx_large_trade_count INTEGER,
    tx_vwap DOUBLE,
    
    -- Whale Activity (wh_) - 28 columns
    wh_net_flow_ratio DOUBLE,
    wh_flow_shift_1m DOUBLE,
    wh_flow_trend_3m DOUBLE,
    wh_accumulation_ratio DOUBLE,
    wh_strong_accumulation DOUBLE,
    wh_cumulative_flow_5m DOUBLE,
    wh_total_sol_moved DOUBLE,
    wh_inflow_share_pct DOUBLE,
    wh_outflow_share_pct DOUBLE,
    wh_net_flow_strength_pct DOUBLE,
    wh_strong_accumulation_pct DOUBLE,
    wh_strong_distribution_pct DOUBLE,
    wh_activity_surge_ratio DOUBLE,
    wh_movement_count INTEGER,
    wh_massive_move_pct DOUBLE,
    wh_avg_wallet_pct_moved DOUBLE,
    wh_largest_move_dominance DOUBLE,
    wh_distribution_pressure_pct DOUBLE,
    wh_outflow_surge_pct DOUBLE,
    wh_movement_imbalance_pct DOUBLE,
    wh_inflow_sol DOUBLE,
    wh_outflow_sol DOUBLE,
    wh_net_flow_sol DOUBLE,
    wh_inflow_count INTEGER,
    wh_outflow_count INTEGER,
    wh_massive_move_count INTEGER,
    wh_max_move_size DOUBLE,
    wh_strong_distribution DOUBLE,
    
    -- Pattern Detection (pat_) - 25 columns
    pat_breakout_score DOUBLE,
    pat_detected_count INTEGER,
    pat_detected_list VARCHAR(255),
    pat_asc_tri_detected BOOLEAN,
    pat_asc_tri_confidence DOUBLE,
    pat_asc_tri_resistance_level DOUBLE,
    pat_asc_tri_support_level DOUBLE,
    pat_asc_tri_compression_ratio DOUBLE,
    pat_bull_flag_detected BOOLEAN,
    pat_bull_flag_confidence DOUBLE,
    pat_bull_flag_pole_height_pct DOUBLE,
    pat_bull_flag_retracement_pct DOUBLE,
    pat_bull_pennant_detected BOOLEAN,
    pat_bull_pennant_confidence DOUBLE,
    pat_bull_pennant_compression_ratio DOUBLE,
    pat_fall_wedge_detected BOOLEAN,
    pat_fall_wedge_confidence DOUBLE,
    pat_fall_wedge_contraction DOUBLE,
    pat_cup_handle_detected BOOLEAN,
    pat_cup_handle_confidence DOUBLE,
    pat_cup_handle_depth_pct DOUBLE,
    pat_inv_hs_detected BOOLEAN,
    pat_inv_hs_confidence DOUBLE,
    pat_inv_hs_neckline DOUBLE,
    pat_swing_trend VARCHAR(20),
    pat_swing_higher_lows BOOLEAN,
    pat_swing_lower_highs BOOLEAN,
    
    -- Second Prices Summary (sp_) - 9 columns
    sp_price_count INTEGER,
    sp_min_price DOUBLE,
    sp_max_price DOUBLE,
    sp_start_price DOUBLE,
    sp_end_price DOUBLE,
    sp_price_range_pct DOUBLE,
    sp_total_change_pct DOUBLE,
    sp_volatility_pct DOUBLE,
    sp_avg_price DOUBLE,
    
    -- BTC Price Movements (btc_) - 6 columns for cross-market correlation
    btc_price_change_1m DOUBLE,
    btc_price_change_5m DOUBLE,
    btc_price_change_10m DOUBLE,
    btc_volatility_pct DOUBLE,
    btc_open_price DOUBLE,
    btc_close_price DOUBLE,
    
    -- ETH Price Movements (eth_) - 6 columns for cross-market correlation
    eth_price_change_1m DOUBLE,
    eth_price_change_5m DOUBLE,
    eth_price_change_10m DOUBLE,
    eth_volatility_pct DOUBLE,
    eth_open_price DOUBLE,
    eth_close_price DOUBLE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (buyin_id, minute)
);

CREATE INDEX IF NOT EXISTS idx_trail_buyin_id ON buyin_trail_minutes(buyin_id);
CREATE INDEX IF NOT EXISTS idx_trail_minute ON buyin_trail_minutes(minute);
CREATE INDEX IF NOT EXISTS idx_trail_created_at ON buyin_trail_minutes(created_at);
CREATE INDEX IF NOT EXISTS idx_trail_breakout_score ON buyin_trail_minutes(pat_breakout_score);
"""

# =============================================================================
# TRADE FILTER VALUES - Normalized filter storage (one row per filter-minute)
# =============================================================================

SCHEMA_TRADE_FILTER_VALUES = """
CREATE TABLE IF NOT EXISTS trade_filter_values (
    id BIGINT PRIMARY KEY,
    buyin_id BIGINT NOT NULL,
    minute INTEGER NOT NULL,
    filter_name VARCHAR(100) NOT NULL,
    filter_value DOUBLE,
    section VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tfv_unique ON trade_filter_values(buyin_id, minute, filter_name);
CREATE INDEX IF NOT EXISTS idx_tfv_buyin_id ON trade_filter_values(buyin_id);
CREATE INDEX IF NOT EXISTS idx_tfv_filter_name ON trade_filter_values(filter_name);
CREATE INDEX IF NOT EXISTS idx_tfv_minute ON trade_filter_values(minute);
CREATE INDEX IF NOT EXISTS idx_tfv_section ON trade_filter_values(section);
"""

# =============================================================================
# SOL STABLECOIN TRADES (for fast trade detection - 1hr hot storage)
# =============================================================================

SCHEMA_SOL_STABLECOIN_TRADES = """
CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
    id BIGINT PRIMARY KEY,
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
"""

# =============================================================================
# WHALE MOVEMENTS (24h hot storage)
# =============================================================================

SCHEMA_WHALE_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS whale_movements (
    id BIGINT PRIMARY KEY,
    signature VARCHAR(255),
    wallet_address VARCHAR(255) NOT NULL,
    whale_type VARCHAR(50),
    current_balance DOUBLE,
    sol_change DOUBLE,
    abs_change DOUBLE,
    percentage_moved DOUBLE,
    direction VARCHAR(10),
    action VARCHAR(50),
    movement_significance VARCHAR(50),
    previous_balance DOUBLE,
    fee_paid DOUBLE,
    block_time BIGINT,
    timestamp TIMESTAMP,
    received_at TIMESTAMP,
    slot BIGINT,
    has_perp_position BOOLEAN,
    perp_platform VARCHAR(50),
    perp_direction VARCHAR(10),
    perp_size DOUBLE,
    perp_leverage DOUBLE,
    perp_entry_price DOUBLE,
    raw_data_json VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_whale_wallet ON whale_movements(wallet_address);
CREATE INDEX IF NOT EXISTS idx_whale_timestamp ON whale_movements(timestamp);
CREATE INDEX IF NOT EXISTS idx_whale_signature ON whale_movements(signature);
CREATE INDEX IF NOT EXISTS idx_whale_type ON whale_movements(whale_type);
"""

# =============================================================================
# JOB EXECUTION METRICS (for scheduler monitoring - 24hr hot storage)
# =============================================================================

SCHEMA_JOB_EXECUTION_METRICS = """
CREATE TABLE IF NOT EXISTS job_execution_metrics (
    id BIGINT PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    duration_ms DOUBLE NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_metrics_job_id ON job_execution_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_job_metrics_started_at ON job_execution_metrics(started_at);
CREATE INDEX IF NOT EXISTS idx_job_metrics_job_started ON job_execution_metrics(job_id, started_at);
"""

# =============================================================================
# FOLLOW THE GOAT TRACKING (tracks last processed trade per wallet)
# =============================================================================

SCHEMA_FOLLOW_THE_GOAT_TRACKING = """
CREATE TABLE IF NOT EXISTS follow_the_goat_tracking (
    id INTEGER PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL UNIQUE,
    last_trade_id BIGINT DEFAULT 0,
    last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tracking_wallet ON follow_the_goat_tracking(wallet_address);
"""

# Expected intervals for each job (in milliseconds) - used to determine "slow" jobs
# Updated 2026-01-04 to match actual scheduler configuration in master.py and master2.py
JOB_EXPECTED_INTERVALS_MS = {
    # === MASTER.PY JOBS (Data Engine - port 5050) ===
    "fetch_jupiter_prices": 1000,           # 1 second
    "sync_trades_from_webhook": 1000,       # 1 second
    "cleanup_jupiter_prices": 3600000,      # 1 hour
    "cleanup_duckdb_hot_tables": 3600000,   # 1 hour
    
    # === MASTER2.PY JOBS (Trading Logic - port 5052) ===
    "sync_from_engine": 1000,               # 1 second (incremental sync by ID)
    "follow_the_goat": 1000,                # 1 second
    "trailing_stop_seller": 1000,           # 1 second
    "train_validator": 15000,               # 15 seconds
    "update_potential_gains": 15000,        # 15 seconds
    "create_wallet_profiles": 5000,         # 5 seconds
    "cleanup_wallet_profiles": 3600000,     # 1 hour
    "process_price_cycles": 2000,           # 2 seconds
    "export_job_status": 5000,              # 5 seconds
    "create_new_patterns": 900000,          # 15 minutes
    
    # === DEPRECATED (kept for backwards compatibility) ===
    "process_wallet_profiles": 5000,        # Renamed to create_wallet_profiles
    "sync_plays_from_mysql": 300000,        # 5 minutes (if still used)
    "sync_pattern_config_from_mysql": 300000,  # 5 minutes (if still used)
}

# =============================================================================
# ALL SCHEMAS COMBINED
# =============================================================================

ALL_SCHEMAS = [
    ("follow_the_goat_plays", SCHEMA_FOLLOW_THE_GOAT_PLAYS),
    ("follow_the_goat_buyins", SCHEMA_FOLLOW_THE_GOAT_BUYINS),
    ("follow_the_goat_buyins_price_checks", SCHEMA_FOLLOW_THE_GOAT_BUYINS_PRICE_CHECKS),
    ("price_points", SCHEMA_PRICE_POINTS),
    ("price_analysis", SCHEMA_PRICE_ANALYSIS),
    ("cycle_tracker", SCHEMA_CYCLE_TRACKER),
    ("wallet_profiles", SCHEMA_WALLET_PROFILES),
    ("pattern_config_projects", SCHEMA_PATTERN_CONFIG_PROJECTS),
    ("pattern_config_filters", SCHEMA_PATTERN_CONFIG_FILTERS),
    ("buyin_trail_minutes", SCHEMA_BUYIN_TRAIL_MINUTES),
    ("sol_stablecoin_trades", SCHEMA_SOL_STABLECOIN_TRADES),
    ("whale_movements", SCHEMA_WHALE_MOVEMENTS),
    ("job_execution_metrics", SCHEMA_JOB_EXECUTION_METRICS),
    ("follow_the_goat_tracking", SCHEMA_FOLLOW_THE_GOAT_TRACKING),
]

# Tables that use 24-hour hot storage (time-based cleanup)
# Note: order_book_features is NOT included here because it only exists 
# in the in-memory TradingDataEngine, not in file-based DuckDB.
# The TradingDataEngine handles its own cleanup automatically.
HOT_TABLES = [
    "follow_the_goat_buyins",
    "follow_the_goat_buyins_price_checks",
    "price_points",
    "price_analysis",
    "cycle_tracker",
    "wallet_profiles",
    "buyin_trail_minutes",
    "sol_stablecoin_trades",
    "whale_movements",
    "job_execution_metrics",
]

# Tables that keep full data (no time-based cleanup)
FULL_DATA_TABLES = [
    "follow_the_goat_plays",
    "pattern_config_projects",
    "pattern_config_filters",
    "follow_the_goat_tracking",
]

# Timestamp column used for cleanup in each table
# Note: cycle_tracker uses cycle_end_time (only completed cycles are cleaned up)
TIMESTAMP_COLUMNS = {
    "follow_the_goat_buyins": "followed_at",
    "follow_the_goat_buyins_price_checks": "checked_at",
    "price_points": "created_at",
    "price_analysis": "created_at",
    "cycle_tracker": "cycle_end_time",  # Only completed cycles older than 24h are cleaned
    "wallet_profiles": "trade_timestamp",  # Profiles older than 24h are cleaned from both DuckDB and MySQL
    "buyin_trail_minutes": "created_at",  # Trail data cleaned after 24h
    "sol_stablecoin_trades": "trade_timestamp",  # Keep only 1 hour for fast lookup
    "whale_movements": "timestamp",
    "job_execution_metrics": "started_at",  # Metrics older than 24h are cleaned
}


def init_all_tables(conn):
    """Initialize all tables in the database."""
    for table_name, schema in ALL_SCHEMAS:
        print(f"Creating table: {table_name}")
        conn.execute(schema)
    
    # Additional optimization indexes
    print("Creating optimization indexes...")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_buyins_query_opt 
        ON follow_the_goat_buyins(followed_at DESC, our_status, play_id)
    """)
    print("All tables initialized successfully.")


def get_cleanup_query(table_name: str, hours: int = 24) -> str:
    """Get the SQL query to delete old data from a hot table."""
    if table_name not in HOT_TABLES:
        raise ValueError(f"Table {table_name} is not a hot table")
    
    ts_col = TIMESTAMP_COLUMNS[table_name]
    return f"DELETE FROM {table_name} WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR"


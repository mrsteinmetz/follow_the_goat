"""
DuckDB Schema Definitions
=========================
Central schema file for all DuckDB tables.
Mirrors MySQL tables for 24-hour hot storage.

Tables:
- follow_the_goat_plays (full data, not time-based)
- follow_the_goat_buyins (24hr hot)
- follow_the_goat_buyins_price_checks (24hr hot)
- price_points (24hr hot)
- price_analysis (24hr hot)
- cycle_tracker (24hr hot)
- wallet_profiles (24hr hot)
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
    pattern_validator_log JSON
);

CREATE INDEX IF NOT EXISTS idx_buyins_wallet ON follow_the_goat_buyins(wallet_address);
CREATE INDEX IF NOT EXISTS idx_buyins_followed_at ON follow_the_goat_buyins(followed_at);
CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status);
CREATE INDEX IF NOT EXISTS idx_buyins_play_id ON follow_the_goat_buyins(play_id);
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
"""

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
]

# Tables that keep full data (no time-based cleanup)
FULL_DATA_TABLES = [
    "follow_the_goat_plays",
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
}


def init_all_tables(conn):
    """Initialize all tables in the database."""
    for table_name, schema in ALL_SCHEMAS:
        print(f"Creating table: {table_name}")
        conn.execute(schema)
    print("All tables initialized successfully.")


def get_cleanup_query(table_name: str, hours: int = 24) -> str:
    """Get the SQL query to delete old data from a hot table."""
    if table_name not in HOT_TABLES:
        raise ValueError(f"Table {table_name} is not a hot table")
    
    ts_col = TIMESTAMP_COLUMNS[table_name]
    return f"DELETE FROM {table_name} WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR"


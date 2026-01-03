"""
One-Time Migration: MySQL to PostgreSQL
========================================
Migrates plays data from MySQL to PostgreSQL archive database.

Run this ONCE before switching to PostgreSQL:
    python scripts/migrate_mysql_to_postgres.py

Prerequisites:
1. MySQL server running with plays data
2. PostgreSQL server installed and running
3. PostgreSQL database 'follow_the_goat_archive' created
4. PostgreSQL user 'ftg_user' with password set in DB_PASSWORD env var

PostgreSQL setup (run as postgres user):
    CREATE USER ftg_user WITH PASSWORD 'your_password';
    CREATE DATABASE follow_the_goat_archive OWNER ftg_user;
    GRANT ALL PRIVILEGES ON DATABASE follow_the_goat_archive TO ftg_user;
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import before psycopg2 to load env
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import pymysql
import psycopg2
import psycopg2.extras

CONFIG_DIR = PROJECT_ROOT / "config"
PLAYS_CACHE_FILE = CONFIG_DIR / "plays_cache.json"

# PostgreSQL archive schema (converted from MySQL)
POSTGRES_ARCHIVE_SCHEMA = """
-- =============================================================================
-- PostgreSQL Archive Schema for Follow The Goat
-- =============================================================================

-- Archive: follow_the_goat_buyins (72h hot storage, then archived)
CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_archive (
    id BIGINT PRIMARY KEY,
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
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_buyins_archive_wallet ON follow_the_goat_buyins_archive(wallet_address);
CREATE INDEX IF NOT EXISTS idx_buyins_archive_followed_at ON follow_the_goat_buyins_archive(followed_at);
CREATE INDEX IF NOT EXISTS idx_buyins_archive_status ON follow_the_goat_buyins_archive(our_status);
CREATE INDEX IF NOT EXISTS idx_buyins_archive_play_id ON follow_the_goat_buyins_archive(play_id);

-- Archive: buyin_trail_minutes (24h hot storage, then archived)
CREATE TABLE IF NOT EXISTS buyin_trail_minutes_archive (
    id BIGINT PRIMARY KEY,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trail_archive_buyin_id ON buyin_trail_minutes_archive(buyin_id);
CREATE INDEX IF NOT EXISTS idx_trail_archive_minute ON buyin_trail_minutes_archive(minute);

-- Archive: wallet_profiles (24h hot storage, then archived)
CREATE TABLE IF NOT EXISTS wallet_profiles_archive (
    id BIGINT PRIMARY KEY,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_profiles_archive_wallet ON wallet_profiles_archive(wallet_address);
CREATE INDEX IF NOT EXISTS idx_profiles_archive_trade_ts ON wallet_profiles_archive(trade_timestamp);

-- Archive: price_points (24h hot storage, then archived)
CREATE TABLE IF NOT EXISTS price_points_archive (
    id BIGINT PRIMARY KEY,
    ts_idx BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    coin_id INT DEFAULT 5,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_price_points_archive_created ON price_points_archive(created_at);
CREATE INDEX IF NOT EXISTS idx_price_points_archive_coin ON price_points_archive(coin_id);

-- Archive: price_analysis (24h hot storage, then archived)
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
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    highest_climb DOUBLE PRECISION,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_analysis_archive_coin ON price_analysis_archive(coin_id);
CREATE INDEX IF NOT EXISTS idx_analysis_archive_created ON price_analysis_archive(created_at);

-- Archive: cycle_tracker (24h hot storage for completed cycles)
CREATE TABLE IF NOT EXISTS cycle_tracker_archive (
    id BIGINT PRIMARY KEY,
    coin_id INT NOT NULL,
    threshold DECIMAL(5,2) NOT NULL,
    cycle_start_time TIMESTAMP NOT NULL,
    cycle_end_time TIMESTAMP,
    sequence_start_id BIGINT NOT NULL,
    sequence_start_price DECIMAL(20,8) NOT NULL,
    highest_price_reached DECIMAL(20,8) NOT NULL,
    lowest_price_reached DECIMAL(20,8) NOT NULL,
    max_percent_increase DECIMAL(10,4) NOT NULL,
    max_percent_increase_from_lowest DECIMAL(10,4) NOT NULL,
    total_data_points INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cycle_archive_coin ON cycle_tracker_archive(coin_id);
CREATE INDEX IF NOT EXISTS idx_cycle_archive_start ON cycle_tracker_archive(cycle_start_time);

-- Archive: sol_stablecoin_trades (24h hot storage, then archived)
CREATE TABLE IF NOT EXISTS sol_stablecoin_trades_archive (
    id BIGINT PRIMARY KEY,
    wallet_address VARCHAR(255) NOT NULL,
    signature VARCHAR(255),
    trade_timestamp TIMESTAMP NOT NULL,
    stablecoin_amount DECIMAL(20,8),
    sol_amount DECIMAL(20,8),
    price DECIMAL(20,8),
    direction VARCHAR(10),
    perp_direction VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trades_archive_wallet ON sol_stablecoin_trades_archive(wallet_address);
CREATE INDEX IF NOT EXISTS idx_trades_archive_timestamp ON sol_stablecoin_trades_archive(trade_timestamp);

-- Archive: job_execution_metrics (24h hot storage, then archived)
CREATE TABLE IF NOT EXISTS job_execution_metrics_archive (
    id BIGINT PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_archive_job ON job_execution_metrics_archive(job_id);
CREATE INDEX IF NOT EXISTS idx_metrics_archive_started ON job_execution_metrics_archive(started_at);

-- Plays table (for reference - plays are loaded from JSON cache)
CREATE TABLE IF NOT EXISTS follow_the_goat_plays (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
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
"""


def get_mysql_connection():
    """Get MySQL connection using old credentials."""
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        user=os.getenv("DB_USER", "ftg_user"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_DATABASE", "follow_the_goat_archive"),
        port=int(os.getenv("MYSQL_PORT", os.getenv("DB_PORT", "3306"))),  # Try MYSQL_PORT first
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_postgres_connection():
    """Get PostgreSQL connection using same credentials (different port)."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        user=os.getenv("DB_USER", "ftg_user"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_DATABASE", "follow_the_goat_archive"),
        port=int(os.getenv("DB_PORT", "5432")),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def create_postgres_schema(pg_conn):
    """Create PostgreSQL archive tables."""
    print("Creating PostgreSQL archive schema...")
    
    with pg_conn.cursor() as cursor:
        # Execute schema in chunks (split by CREATE TABLE/INDEX)
        for statement in POSTGRES_ARCHIVE_SCHEMA.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"  Warning: {e}")
    
    pg_conn.commit()
    print("  Schema created successfully")


def migrate_plays(mysql_conn, pg_conn):
    """Migrate plays from MySQL to PostgreSQL and update JSON cache."""
    print("\nMigrating plays from MySQL to PostgreSQL...")
    
    # Fetch plays from MySQL
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM follow_the_goat_plays ORDER BY id")
        plays = cursor.fetchall()
    
    print(f"  Found {len(plays)} plays in MySQL")
    
    if not plays:
        print("  WARNING: No plays found in MySQL!")
        return 0
    
    # Insert into PostgreSQL
    inserted = 0
    with pg_conn.cursor() as cursor:
        for play in plays:
            try:
                columns = list(play.keys())
                values = []
                for col in columns:
                    val = play[col]
                    # Convert JSON strings to proper format for PostgreSQL
                    if isinstance(val, str) and (val.startswith('{') or val.startswith('[')):
                        try:
                            val = json.loads(val)
                            val = json.dumps(val)
                        except:
                            pass
                    elif hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    values.append(val)
                
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                cursor.execute(
                    f"INSERT INTO follow_the_goat_plays ({columns_str}) VALUES ({placeholders}) "
                    f"ON CONFLICT (id) DO UPDATE SET {', '.join([f'{c} = EXCLUDED.{c}' for c in columns if c != 'id'])}",
                    values
                )
                inserted += 1
            except Exception as e:
                print(f"  Error inserting play {play.get('id')}: {e}")
    
    pg_conn.commit()
    print(f"  Inserted {inserted} plays into PostgreSQL")
    
    # Update JSON cache
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    plays_serializable = []
    for play in plays:
        play_dict = dict(play)
        for key, value in play_dict.items():
            if hasattr(value, 'isoformat'):
                play_dict[key] = value.isoformat()
        plays_serializable.append(play_dict)
    
    # Create backup
    backup_file = CONFIG_DIR / f"plays_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(plays_serializable, f, indent=2, ensure_ascii=False)
    print(f"  Created backup: {backup_file}")
    
    # Update main cache
    with open(PLAYS_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(plays_serializable, f, indent=2, ensure_ascii=False)
    print(f"  Updated cache: {PLAYS_CACHE_FILE}")
    
    return inserted


def verify_migration(pg_conn):
    """Verify the migration was successful."""
    print("\nVerifying migration...")
    
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as cnt FROM follow_the_goat_plays")
        result = cursor.fetchone()
        pg_count = result['cnt']
    
    print(f"  PostgreSQL plays count: {pg_count}")
    
    # Verify JSON cache
    if PLAYS_CACHE_FILE.exists():
        with open(PLAYS_CACHE_FILE, 'r', encoding='utf-8') as f:
            cache_plays = json.load(f)
        print(f"  JSON cache plays count: {len(cache_plays)}")
    
    return pg_count > 0


def main():
    print("=" * 60)
    print("MySQL to PostgreSQL Migration")
    print("=" * 60)
    print(f"MySQL port: {os.getenv('MYSQL_PORT', os.getenv('DB_PORT', '3306'))}")
    print(f"PostgreSQL port: {os.getenv('DB_PORT', '5432')}")
    print()
    
    mysql_conn = None
    pg_conn = None
    
    try:
        # Connect to MySQL
        print("Connecting to MySQL...")
        try:
            mysql_conn = get_mysql_connection()
            print("  Connected to MySQL")
        except Exception as e:
            print(f"  MySQL connection failed: {e}")
            print("  Proceeding with PostgreSQL setup only (plays from JSON cache)")
            mysql_conn = None
        
        # Connect to PostgreSQL
        print("\nConnecting to PostgreSQL...")
        pg_conn = get_postgres_connection()
        pg_conn.autocommit = True
        print("  Connected to PostgreSQL")
        
        # Create PostgreSQL schema
        create_postgres_schema(pg_conn)
        
        # Migrate plays if MySQL is available
        if mysql_conn:
            migrate_plays(mysql_conn, pg_conn)
        else:
            print("\nSkipping plays migration (MySQL not available)")
            print("Plays will be loaded from existing JSON cache on startup")
        
        # Verify
        if verify_migration(pg_conn):
            print("\n" + "=" * 60)
            print("MIGRATION SUCCESSFUL!")
            print("=" * 60)
            print("\nNext steps:")
            print("1. Update DB_PORT in .env to 5432 (PostgreSQL default)")
            print("2. Restart the trading bot")
            print("3. Verify data in PostgreSQL with: psql -U ftg_user -d follow_the_goat_archive")
        else:
            print("\nWARNING: Migration may be incomplete - please verify manually")
        
    except Exception as e:
        print(f"\nERROR: Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

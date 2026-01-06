"""
In-Memory DuckDB Trading Data Engine
=====================================
High-performance trading data engine with zero lock contention.

Architecture:
- In-memory DuckDB for 24h data window (instant reads, no file locks)
- Queue-based batch writing (non-blocking writes)
- Parquet files for local persistence (hourly flush)
- Plays config loaded from JSON cache file (config/plays_cache.json)
- Auto-cleanup of data older than 24h
- OLD data is archived to MySQL (local Ubuntu) via scheduler cleanup jobs

Usage:
    from core.trading_engine import get_engine
    
    engine = get_engine()
    engine.start()
    
    # Non-blocking write
    engine.write('prices', {'ts': datetime.now(), 'token': 'SOL', 'price': 123.45})
    
    # Instant read
    results = engine.read("SELECT * FROM prices WHERE token = ?", ['SOL'])
    
    # Shutdown (auto-saves to Parquet)
    engine.stop()
"""

import duckdb
import pymysql
import threading
import queue
import time
import logging
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager

from core.config import settings

# Paths for local storage
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
PARQUET_DIR = PROJECT_ROOT / "000data_feeds" / "parquet"
PLAYS_CACHE_FILE = CONFIG_DIR / "plays_cache.json"

logger = logging.getLogger("trading_engine")


# =============================================================================
# Table Schemas
# =============================================================================

TABLE_SCHEMAS = {
    "prices": """
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            token VARCHAR(20) NOT NULL,
            price DOUBLE NOT NULL
        )
    """,
    "transactions": """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            tx_hash VARCHAR(100),
            from_token VARCHAR(20),
            to_token VARCHAR(20),
            amount DOUBLE,
            price DOUBLE,
            wallet VARCHAR(100)
        )
    """,
    "orderbook": """
        CREATE TABLE IF NOT EXISTS orderbook (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            token VARCHAR(20) NOT NULL,
            side VARCHAR(10) NOT NULL,
            price DOUBLE NOT NULL,
            quantity DOUBLE NOT NULL,
            exchange VARCHAR(50)
        )
    """,
    "trades": """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            token VARCHAR(20) NOT NULL,
            side VARCHAR(10) NOT NULL,
            price DOUBLE NOT NULL,
            quantity DOUBLE NOT NULL,
            trade_id VARCHAR(100),
            exchange VARCHAR(50)
        )
    """,
    "price_analysis": """
        CREATE TABLE IF NOT EXISTS price_analysis (
            id INTEGER PRIMARY KEY,
            coin_id INTEGER NOT NULL,
            price_point_id BIGINT NOT NULL,
            sequence_start_id BIGINT,
            sequence_start_price DOUBLE NOT NULL,
            current_price DOUBLE NOT NULL,
            percent_threshold DOUBLE DEFAULT 0.10,
            percent_increase DOUBLE,
            highest_price_recorded DOUBLE,
            lowest_price_recorded DOUBLE,
            procent_change_from_highest_price_recorded DOUBLE DEFAULT 0.0,
            percent_increase_from_lowest DOUBLE DEFAULT 0.0,
            price_cycle BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            highest_climb DOUBLE
        )
    """,
    "cycle_tracker": """
        CREATE TABLE IF NOT EXISTS cycle_tracker (
            id BIGINT PRIMARY KEY,
            coin_id INTEGER NOT NULL,
            threshold DOUBLE NOT NULL,
            cycle_start_time TIMESTAMP NOT NULL,
            cycle_end_time TIMESTAMP,
            sequence_start_id BIGINT NOT NULL,
            sequence_start_price DOUBLE NOT NULL,
            highest_price_reached DOUBLE NOT NULL,
            lowest_price_reached DOUBLE NOT NULL,
            max_percent_increase DOUBLE NOT NULL,
            max_percent_increase_from_lowest DOUBLE NOT NULL,
            total_data_points INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "sol_stablecoin_trades": """
        CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
            id BIGINT PRIMARY KEY,
            wallet_address VARCHAR(255) NOT NULL,
            signature VARCHAR(255),
            trade_timestamp TIMESTAMP NOT NULL,
            stablecoin_amount DOUBLE,
            sol_amount DOUBLE,
            price DOUBLE,
            direction VARCHAR(10),
            perp_direction VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "order_book_features": """
        CREATE TABLE IF NOT EXISTS order_book_features (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            venue VARCHAR(20) NOT NULL,
            quote_asset VARCHAR(10) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            best_bid DOUBLE NOT NULL,
            best_ask DOUBLE NOT NULL,
            mid_price DOUBLE NOT NULL,
            absolute_spread DOUBLE NOT NULL,
            relative_spread_bps DOUBLE NOT NULL,
            bid_depth_10 DOUBLE NOT NULL,
            ask_depth_10 DOUBLE NOT NULL,
            total_depth_10 DOUBLE NOT NULL,
            volume_imbalance DOUBLE NOT NULL,
            bid_vwap_10 DOUBLE,
            ask_vwap_10 DOUBLE,
            bid_slope DOUBLE,
            ask_slope DOUBLE,
            microprice DOUBLE,
            microprice_dev_bps DOUBLE,
            bid_depth_bps_5 DOUBLE,
            ask_depth_bps_5 DOUBLE,
            bid_depth_bps_10 DOUBLE,
            ask_depth_bps_10 DOUBLE,
            bid_depth_bps_25 DOUBLE,
            ask_depth_bps_25 DOUBLE,
            net_liquidity_change_1s DOUBLE,
            bids_json VARCHAR,
            asks_json VARCHAR,
            source VARCHAR(20) NOT NULL
        )
    """,
    "wallet_profiles": """
        CREATE TABLE IF NOT EXISTS wallet_profiles (
            id BIGINT PRIMARY KEY,
            wallet_address VARCHAR(255) NOT NULL,
            threshold DOUBLE NOT NULL,
            trade_id BIGINT NOT NULL,
            trade_timestamp TIMESTAMP NOT NULL,
            price_cycle BIGINT NOT NULL,
            price_cycle_start_time TIMESTAMP,
            price_cycle_end_time TIMESTAMP,
            trade_entry_price_org DOUBLE NOT NULL,
            stablecoin_amount DOUBLE,
            trade_entry_price DOUBLE NOT NULL,
            sequence_start_price DOUBLE NOT NULL,
            highest_price_reached DOUBLE NOT NULL,
            lowest_price_reached DOUBLE NOT NULL,
            long_short VARCHAR(10),
            short TINYINT NOT NULL DEFAULT 2,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "follow_the_goat_plays": """
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
        )
    """,
    "price_points": """
        CREATE TABLE IF NOT EXISTS price_points (
            id BIGINT PRIMARY KEY,
            ts_idx BIGINT,
            coin_id INTEGER NOT NULL,
            value DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "follow_the_goat_buyins": """
        CREATE TABLE IF NOT EXISTS follow_the_goat_buyins (
            id BIGINT PRIMARY KEY,
            play_id INTEGER NOT NULL,
            wallet_address VARCHAR(255) NOT NULL,
            original_trade_id BIGINT,
            trade_signature VARCHAR(255),
            block_timestamp TIMESTAMP,
            quote_amount DOUBLE,
            base_amount DOUBLE,
            price DOUBLE,
            direction VARCHAR(10),
            our_entry_price DOUBLE,
            our_position_size DOUBLE,
            our_exit_price DOUBLE,
            price_movements TEXT,
            swap_response TEXT,
            live_trade TINYINT DEFAULT 0,
            price_cycle BIGINT,
            entry_log TEXT,
            pattern_validator_log TEXT,
            our_status VARCHAR(50) DEFAULT 'validating',
            followed_at TIMESTAMP,
            sold_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            higest_price_reached DOUBLE,
            current_price DOUBLE,
            tolerance DOUBLE,
            potential_gains DOUBLE,
            our_profit_loss DOUBLE,
            our_exit_timestamp TIMESTAMP
        )
    """,
    "buyin_trail_minutes": """
        CREATE TABLE IF NOT EXISTS buyin_trail_minutes (
            id BIGINT PRIMARY KEY,
            buyin_id BIGINT NOT NULL,
            minute_offset INTEGER NOT NULL,
            price_start DOUBLE,
            price_end DOUBLE,
            price_high DOUBLE,
            price_low DOUBLE,
            price_change_pct DOUBLE,
            volume_imbalance DOUBLE,
            bid_depth DOUBLE,
            ask_depth DOUBLE,
            spread_bps DOUBLE,
            order_book_pressure DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "job_execution_metrics": """
        CREATE TABLE IF NOT EXISTS job_execution_metrics (
            id BIGINT PRIMARY KEY,
            job_id VARCHAR(100) NOT NULL,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP NOT NULL,
            duration_ms DOUBLE NOT NULL,
            status VARCHAR(20) NOT NULL,
            error_message VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "sol_stablecoin_trades": """
        CREATE TABLE IF NOT EXISTS sol_stablecoin_trades (
            id BIGINT PRIMARY KEY,
            wallet_address VARCHAR(255),
            signature VARCHAR(255),
            trade_timestamp TIMESTAMP,
            stablecoin_amount DOUBLE,
            sol_amount DOUBLE,
            price DOUBLE,
            direction VARCHAR(10),
            perp_direction VARCHAR(10),
            processed TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "filter_fields_catalog": """
        CREATE TABLE IF NOT EXISTS filter_fields_catalog (
            id INTEGER PRIMARY KEY,
            section VARCHAR(50) NOT NULL,
            field_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100) NOT NULL,
            column_prefix VARCHAR(10),
            data_type VARCHAR(20) DEFAULT 'DOUBLE',
            value_type VARCHAR(20) DEFAULT 'numeric',
            description VARCHAR(255),
            is_filterable BOOLEAN DEFAULT TRUE,
            display_order INTEGER DEFAULT 100,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "filter_reference_suggestions": """
        CREATE TABLE IF NOT EXISTS filter_reference_suggestions (
            id INTEGER PRIMARY KEY,
            filter_field_id INTEGER,
            column_name VARCHAR(100) NOT NULL,
            from_value DOUBLE,
            to_value DOUBLE,
            total_trades INTEGER,
            good_trades_before INTEGER,
            bad_trades_before INTEGER,
            good_trades_after INTEGER,
            bad_trades_after INTEGER,
            good_trades_kept_pct DOUBLE,
            bad_trades_removed_pct DOUBLE,
            bad_negative_count INTEGER,
            bad_0_to_01_count INTEGER,
            bad_01_to_02_count INTEGER,
            bad_02_to_03_count INTEGER,
            analysis_hours INTEGER DEFAULT 24,
            minute_analyzed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "filter_combinations": """
        CREATE TABLE IF NOT EXISTS filter_combinations (
            id INTEGER PRIMARY KEY,
            combination_name VARCHAR(255) NOT NULL,
            filter_count INTEGER NOT NULL,
            filter_ids JSON NOT NULL,
            filter_columns JSON NOT NULL,
            total_trades INTEGER,
            good_trades_before INTEGER,
            bad_trades_before INTEGER,
            good_trades_after INTEGER,
            bad_trades_after INTEGER,
            good_trades_kept_pct DOUBLE,
            bad_trades_removed_pct DOUBLE,
            best_single_bad_removed_pct DOUBLE,
            improvement_over_single DOUBLE,
            bad_negative_count INTEGER,
            bad_0_to_01_count INTEGER,
            bad_01_to_02_count INTEGER,
            bad_02_to_03_count INTEGER,
            minute_analyzed INTEGER DEFAULT 0,
            analysis_hours INTEGER DEFAULT 24,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "trade_filter_values": """
        CREATE TABLE IF NOT EXISTS trade_filter_values (
            id BIGINT PRIMARY KEY,
            buyin_id BIGINT NOT NULL,
            minute INTEGER NOT NULL,
            filter_name VARCHAR(100) NOT NULL,
            filter_value DOUBLE,
            section VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
}

# Timestamp column for each table (used for cleanup and sync)
TIMESTAMP_COLUMNS = {
    "prices": "ts",
    "transactions": "ts",
    "orderbook": "ts",
    "trades": "ts",
    "price_analysis": "created_at",
    "cycle_tracker": "cycle_start_time",
    "order_book_features": "ts",
    "wallet_profiles": "trade_timestamp",
    "follow_the_goat_plays": "created_at",
    "follow_the_goat_buyins": "followed_at",
    "buyin_trail_minutes": "created_at",
    "job_execution_metrics": "started_at",
    "sol_stablecoin_trades": "trade_timestamp",
    "price_points": "created_at",
    "filter_fields_catalog": "created_at",
    "filter_reference_suggestions": "created_at",
    "filter_combinations": "created_at",
    "trade_filter_values": "created_at",
}

# MySQL table mappings (if different from DuckDB table names)
MYSQL_TABLE_MAPPINGS = {
    "prices": "price_points",  # MySQL uses price_points table
}


@dataclass
class WriteOperation:
    """Represents a queued write operation."""
    table: str
    data: Dict[str, Any]
    timestamp: datetime


# =============================================================================
# Trading Data Engine
# =============================================================================

class TradingDataEngine:
    """
    In-memory DuckDB trading data engine with zero lock contention.
    
    Features:
    - In-memory DuckDB (no file locks)
    - Queue-based non-blocking writes
    - Instant reads with shared connection
    - Background MySQL sync for persistence
    - Auto-cleanup of data older than 24h
    
    Thread Safety:
    - Write queue is thread-safe (queue.Queue)
    - DuckDB connection uses a lock for thread safety
    - MySQL connections are created per-sync operation
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        batch_timeout_ms: int = 100,
        sync_interval_sec: int = 30,
        cleanup_interval_sec: int = 300,
        retention_hours: int = 24,
        bootstrap_from_mysql: bool = False,
    ):
        """
        Initialize the trading data engine.
        
        Args:
            batch_size: Max records to batch before writing
            batch_timeout_ms: Max time to wait before flushing batch
            sync_interval_sec: How often to sync to MySQL
            cleanup_interval_sec: How often to clean old data
            retention_hours: How long to keep data in memory
            bootstrap_from_mysql: If True, load last 24h from MySQL on startup.
                                  Default False (only live data).
        """
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.sync_interval_sec = sync_interval_sec
        self.cleanup_interval_sec = cleanup_interval_sec
        self.retention_hours = retention_hours
        self.bootstrap_from_mysql = bootstrap_from_mysql
        
        # In-memory DuckDB connection
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._conn_lock = threading.Lock()
        
        # Write queue
        self._write_queue: queue.Queue[WriteOperation] = queue.Queue()
        
        # Background threads
        self._batch_writer_thread: Optional[threading.Thread] = None
        self._parquet_flush_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        
        # Control flags
        self._running = False
        self._stop_event = threading.Event()
        
        # Auto-increment IDs per table
        self._next_ids: Dict[str, int] = {}
        self._id_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            "writes_queued": 0,
            "writes_committed": 0,
            "reads_executed": 0,
            "parquet_flushes": 0,
            "cleanups": 0,
        }
    
    # =========================================================================
    # Lifecycle
    # =========================================================================
    
    def start(self):
        """Start the engine and all background threads."""
        if self._running:
            logger.warning("Engine already running")
            return
        
        logger.info("Starting TradingDataEngine...")
        
        # Initialize in-memory DuckDB
        self._init_database()
        
        # 1. Load plays from MySQL (with JSON cache fallback) - always needed
        self._bootstrap_plays()
        
        # 2. Load today's trading data from Parquet (if exists)
        # DISABLED: For live trading bot, always start fresh with new data
        # self._bootstrap_from_parquet()
        
        # Start background threads
        self._running = True
        self._stop_event.clear()
        
        self._batch_writer_thread = threading.Thread(
            target=self._batch_writer_loop,
            name="BatchWriter",
            daemon=True
        )
        self._batch_writer_thread.start()
        
        # Parquet flush thread (replaces MySQL sync)
        self._parquet_flush_thread = threading.Thread(
            target=self._parquet_flush_loop,
            name="ParquetFlush",
            daemon=True
        )
        self._parquet_flush_thread.start()
        
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name="Cleanup",
            daemon=True
        )
        self._cleanup_thread.start()
        
        logger.info("TradingDataEngine started successfully")
    
    def stop(self):
        """Stop the engine gracefully."""
        if not self._running:
            return
        
        logger.info("Stopping TradingDataEngine...")
        
        # Signal threads to stop
        self._running = False
        self._stop_event.set()
        
        # Flush remaining writes
        self._flush_write_queue()
        
        # Final Parquet save (persist all data)
        logger.info("Saving data to Parquet before shutdown...")
        self.save_to_parquet()
        
        # Wait for threads
        if self._batch_writer_thread:
            self._batch_writer_thread.join(timeout=5)
        if hasattr(self, '_parquet_flush_thread') and self._parquet_flush_thread:
            self._parquet_flush_thread.join(timeout=5)
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        
        # Close DuckDB connection
        if self._conn:
            self._conn.close()
            self._conn = None
        
        logger.info("TradingDataEngine stopped")
    
    def _init_database(self):
        """Initialize in-memory DuckDB with all tables."""
        logger.info("Initializing in-memory DuckDB...")
        
        self._conn = duckdb.connect(":memory:")
        
        # CRITICAL: Set timezone to UTC for all timestamps
        # DuckDB defaults to system timezone (CET/UTC+1), we need UTC
        try:
            self._conn.execute("SET TimeZone='UTC'")
            logger.info("Set DuckDB timezone to UTC in TradingDataEngine")
        except Exception as e:
            logger.warning(f"Failed to set UTC timezone in TradingDataEngine: {e}")
        
        # Create tables
        for table_name, schema in TABLE_SCHEMAS.items():
            self._conn.execute(schema)
            logger.debug(f"Created table: {table_name}")
        
        # Create indexes for fast queries
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_token ON prices(token)")
        # Composite index for common query pattern: WHERE token = ? AND ts >= ? AND ts <= ?
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_token_ts ON prices(token, ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_ts ON orderbook(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_price_analysis_created ON price_analysis(created_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cycle_tracker_start ON cycle_tracker(cycle_start_time)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_order_book_features_ts ON order_book_features(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_order_book_features_symbol ON order_book_features(symbol)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profiles_ts ON wallet_profiles(trade_timestamp)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profiles_wallet ON wallet_profiles(wallet_address)")
        # Trading tables
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_buyins_play ON follow_the_goat_buyins(play_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_buyins_followed ON follow_the_goat_buyins(followed_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_trail_buyin ON buyin_trail_minutes(buyin_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_price_points_coin ON price_points(coin_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_price_points_created ON price_points(created_at)")
        
        logger.info("In-memory DuckDB initialized")
    
    def _bootstrap_plays(self):
        """Load plays into memory from cache only (no MySQL)."""
        logger.info("Loading plays configuration from cache...")
        
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        
        if not PLAYS_CACHE_FILE.exists():
            logger.error(f"No plays cache found at {PLAYS_CACHE_FILE}")
            return
        
        try:
            with open(PLAYS_CACHE_FILE, 'r') as f:
                plays_data = json.load(f)
            logger.info(f"Loaded {len(plays_data)} plays from cache: {PLAYS_CACHE_FILE}")
        except Exception as cache_err:
            logger.error(f"Could not load plays cache: {cache_err}")
            return
        
        if not plays_data:
            logger.warning("Plays cache is empty - trading may not work correctly!")
            return
        
        with self._conn_lock:
            for play in plays_data:
                columns = list(play.keys())
                placeholders = ", ".join(["?" for _ in columns])
                columns_str = ", ".join(columns)
                values = [play[c] for c in columns]
                
                try:
                    self._conn.execute(f"""
                        INSERT INTO follow_the_goat_plays ({columns_str})
                        VALUES ({placeholders})
                    """, values)
                except Exception as insert_err:
                    logger.debug(f"Play insert error: {insert_err}")
        
        self._next_ids['follow_the_goat_plays'] = max(p['id'] for p in plays_data) + 1
        logger.info(f"Loaded {len(plays_data)} plays into in-memory DuckDB")
    
    def _bootstrap_from_parquet(self):
        """Load today's data from Parquet files if they exist.
        
        This restores state after a restart during the same day.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        today_dir = PARQUET_DIR / today
        
        if not today_dir.exists():
            logger.info(f"No Parquet data for today ({today}) - starting fresh")
            return
        
        logger.info(f"Loading data from Parquet: {today_dir}")
        
        # Tables to load from Parquet (trading data only, not plays)
        parquet_tables = [
            'prices', 'price_analysis', 'cycle_tracker', 'order_book_features',
            'follow_the_goat_buyins', 'buyin_trail_minutes'
        ]
        
        with self._conn_lock:
            for table in parquet_tables:
                parquet_file = today_dir / f"{table}.parquet"
                if parquet_file.exists():
                    try:
                        # DuckDB can read Parquet directly
                        count = self._conn.execute(f"""
                            INSERT INTO {table}
                            SELECT * FROM read_parquet('{parquet_file}')
                        """).fetchone()
                        
                        # Get row count
                        result = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        row_count = result[0] if result else 0
                        
                        # Update next ID
                        max_id = self._conn.execute(f"SELECT MAX(id) FROM {table}").fetchone()
                        if max_id and max_id[0]:
                            self._next_ids[table] = max_id[0] + 1
                        
                        logger.info(f"  Loaded {row_count} rows into {table} from Parquet")
                    except Exception as e:
                        logger.warning(f"  Could not load {table} from Parquet: {e}")
        
        logger.info("Parquet bootstrap complete")
    
    # =========================================================================
    # Write Operations (Non-blocking)
    # =========================================================================
    
    def write(self, table: str, data: Dict[str, Any]) -> None:
        """
        Queue a write operation (non-blocking).
        
        Args:
            table: Table name (prices, transactions, orderbook, trades)
            data: Dictionary of column -> value
        """
        if table not in TABLE_SCHEMAS:
            raise ValueError(f"Unknown table: {table}")
        
        op = WriteOperation(table=table, data=data, timestamp=datetime.now())
        self._write_queue.put(op)
        self._stats["writes_queued"] += 1
    
    def write_batch(self, table: str, records: List[Dict[str, Any]]) -> None:
        """
        Queue multiple write operations (non-blocking).
        
        Args:
            table: Table name
            records: List of dictionaries
        """
        for record in records:
            self.write(table, record)
    
    def write_sync(self, table: str, data: Dict[str, Any]) -> int:
        """
        Write synchronously and return the ID (blocking).
        Use this when you need the ID immediately.
        
        Args:
            table: Table name
            data: Dictionary of column -> value
            
        Returns:
            The auto-generated ID
        """
        if table not in TABLE_SCHEMAS:
            raise ValueError(f"Unknown table: {table}")
        
        # Get next ID
        with self._id_lock:
            next_id = self._next_ids.get(table, 1)
            self._next_ids[table] = next_id + 1
        
        data['id'] = next_id
        
        # Build INSERT statement
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        values = [data[col] for col in columns]
        
        sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        with self._conn_lock:
            self._conn.execute(sql, values)
        
        self._stats["writes_committed"] += 1
        return next_id
    
    def _get_next_id(self, table: str) -> int:
        """Get the next auto-increment ID for a table."""
        with self._id_lock:
            next_id = self._next_ids.get(table, 1)
            self._next_ids[table] = next_id + 1
            return next_id
    
    # =========================================================================
    # Read Operations (Instant)
    # =========================================================================
    
    @contextmanager
    def get_connection(self):
        """
        Get a context-managed connection for direct SQL execution.
        
        This is used by price cycles and other code that needs raw DuckDB access.
        The connection is thread-safe via the internal lock.
        
        Usage:
            with engine.get_connection() as conn:
                result = conn.execute("SELECT * FROM prices").fetchall()
        """
        with self._conn_lock:
            yield self._conn
    
    def read(self, query: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a read query instantly.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        if params is None:
            params = []
        
        with self._conn_lock:
            result = self._conn.execute(query, params)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        
        self._stats["reads_executed"] += 1
        return [dict(zip(columns, row)) for row in rows]
    
    def read_one(self, query: str, params: List[Any] = None) -> Optional[Dict[str, Any]]:
        """Execute a query and return the first result or None."""
        results = self.read(query, params)
        return results[0] if results else None
    
    def get_latest(self, table: str, token: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get the latest records from a table.
        
        Args:
            table: Table name
            token: Optional token filter
            limit: Max records to return
        """
        ts_col = TIMESTAMP_COLUMNS.get(table, "ts")
        
        if token and table in ["prices", "trades", "orderbook"]:
            return self.read(
                f"SELECT * FROM {table} WHERE token = ? ORDER BY {ts_col} DESC LIMIT ?",
                [token, limit]
            )
        else:
            return self.read(
                f"SELECT * FROM {table} ORDER BY {ts_col} DESC LIMIT ?",
                [limit]
            )
    
    def get_price(self, token: str = "SOL") -> Optional[float]:
        """Get the current price of a token."""
        result = self.read_one(
            "SELECT price FROM prices WHERE token = ? ORDER BY ts DESC LIMIT 1",
            [token]
        )
        return result["price"] if result else None
    
    def get_prices_range(
        self, 
        token: str = "SOL", 
        start: datetime = None, 
        end: datetime = None
    ) -> List[Dict[str, Any]]:
        """Get prices for a token within a time range."""
        if start is None:
            start = datetime.now() - timedelta(hours=24)
        if end is None:
            end = datetime.now()
        
        return self.read(
            "SELECT * FROM prices WHERE token = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
            [token, start, end]
        )
    
    def execute(self, sql: str, params: List[Any] = None) -> None:
        """Execute a SQL statement (UPDATE, DELETE, etc.)."""
        if params is None:
            params = []
        
        with self._conn_lock:
            self._conn.execute(sql, params)
    
    # =========================================================================
    # Background Threads
    # =========================================================================
    
    def _batch_writer_loop(self):
        """Background thread that batches and commits writes."""
        logger.info("Batch writer thread started")
        
        batch: List[WriteOperation] = []
        last_flush = time.time()
        
        while self._running or not self._write_queue.empty():
            try:
                # Try to get from queue with timeout
                try:
                    op = self._write_queue.get(timeout=0.01)
                    batch.append(op)
                except queue.Empty:
                    pass
                
                # Flush if batch is full or timeout reached
                now = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or
                    (len(batch) > 0 and (now - last_flush) * 1000 >= self.batch_timeout_ms)
                )
                
                if should_flush:
                    self._commit_batch(batch)
                    batch = []
                    last_flush = now
                    
            except Exception as e:
                logger.error(f"Batch writer error: {e}")
        
        # Flush remaining
        if batch:
            self._commit_batch(batch)
        
        logger.info("Batch writer thread stopped")
    
    def _commit_batch(self, batch: List[WriteOperation]):
        """Commit a batch of writes to DuckDB AND queue for PostgreSQL (dual-write)."""
        if not batch:
            return
        
        # Group by table
        by_table: Dict[str, List[WriteOperation]] = {}
        for op in batch:
            if op.table not in by_table:
                by_table[op.table] = []
            by_table[op.table].append(op)
        
        # Collect rows for PostgreSQL dual-write (per table)
        postgres_batches: Dict[str, List[Dict[str, Any]]] = {}
        
        with self._conn_lock:
            for table, ops in by_table.items():
                table_rows = []
                for op in ops:
                    # Assign ID if not present
                    if 'id' not in op.data:
                        op.data['id'] = self._get_next_id(table)
                    
                    columns = list(op.data.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    columns_str = ", ".join(columns)
                    values = [op.data[col] for col in columns]
                    
                    # Use INSERT OR IGNORE for tables with primary keys to handle duplicates gracefully
                    # This prevents errors when master2.py syncs data back to the engine
                    sql = f"INSERT OR IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})"
                    try:
                        self._conn.execute(sql, values)
                        self._stats["writes_committed"] += 1
                        # Collect for PostgreSQL dual-write
                        table_rows.append(op.data.copy())
                    except Exception as e:
                        logger.error(f"Failed to insert into {table}: {e}")
                
                if table_rows:
                    postgres_batches[table] = table_rows
        
        # DUAL-WRITE: Queue for async PostgreSQL write (fire-and-forget, never blocks)
        # This ensures we have a complete historical record in PostgreSQL
        try:
            from core.database import write_batch_to_postgres_async
            for table, rows in postgres_batches.items():
                write_batch_to_postgres_async(table, rows)
        except Exception as e:
            # PostgreSQL write is optional - don't fail the batch if it errors
            logger.debug(f"PostgreSQL dual-write skipped: {e}")
    
    def _flush_write_queue(self):
        """Flush all pending writes."""
        batch = []
        while not self._write_queue.empty():
            try:
                op = self._write_queue.get_nowait()
                batch.append(op)
            except queue.Empty:
                break
        
        if batch:
            self._commit_batch(batch)
    
    def _parquet_flush_loop(self):
        """Background thread that periodically saves data to Parquet."""
        logger.info("Parquet flush thread started")
        
        # Flush every hour (3600 seconds)
        flush_interval = 3600
        
        while not self._stop_event.wait(flush_interval):
            try:
                self.save_to_parquet()
            except Exception as e:
                logger.error(f"Parquet flush error: {e}")
        
        logger.info("Parquet flush thread stopped")
    
    def save_to_parquet(self):
        """Save all in-memory data to Parquet files.
        
        Creates date-partitioned Parquet files:
        000data_feeds/parquet/YYYY-MM-DD/table_name.parquet
        """
        today = datetime.now().strftime("%Y-%m-%d")
        today_dir = PARQUET_DIR / today
        today_dir.mkdir(parents=True, exist_ok=True)
        
        # Tables to save (trading data, not config)
        tables_to_save = [
            'prices', 'price_analysis', 'cycle_tracker', 'order_book_features',
            'follow_the_goat_buyins', 'buyin_trail_minutes', 'wallet_profiles',
            'transactions', 'trades', 'orderbook'
        ]
        
        saved_count = 0
        
        with self._conn_lock:
            for table in tables_to_save:
                try:
                    # Check if table has data
                    result = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    row_count = result[0] if result else 0
                    
                    if row_count == 0:
                        continue
                    
                    # Export to Parquet
                    parquet_file = today_dir / f"{table}.parquet"
                    self._conn.execute(f"""
                        COPY {table} TO '{parquet_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                    
                    saved_count += 1
                    logger.debug(f"Saved {row_count} rows to {parquet_file}")
                    
                except Exception as e:
                    logger.warning(f"Could not save {table} to Parquet: {e}")
        
        if saved_count > 0:
            logger.info(f"Parquet flush complete: {saved_count} tables saved to {today_dir}")
            self._stats["parquet_flushes"] = self._stats.get("parquet_flushes", 0) + 1
    
    def _cleanup_loop(self):
        """Background thread that removes old data."""
        logger.info("Cleanup thread started")
        
        while not self._stop_event.wait(self.cleanup_interval_sec):
            try:
                self._cleanup_old_data()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
        
        logger.info("Cleanup thread stopped")
    
    def _cleanup_old_data(self):
        """Remove data older than retention period."""
        cutoff = datetime.now() - timedelta(hours=self.retention_hours)
        total_deleted = 0
        
        with self._conn_lock:
            for table, ts_col in TIMESTAMP_COLUMNS.items():
                try:
                    # Special handling for cycle_tracker: use cycle_end_time and only delete completed cycles
                    if table == "cycle_tracker":
                        result = self._conn.execute(
                            "DELETE FROM cycle_tracker WHERE cycle_end_time IS NOT NULL AND cycle_end_time < ? RETURNING id",
                            [cutoff]
                        ).fetchall()
                    else:
                        result = self._conn.execute(
                            f"DELETE FROM {table} WHERE {ts_col} < ? RETURNING id",
                            [cutoff]
                        ).fetchall()
                    
                    deleted = len(result)
                    if deleted > 0:
                        total_deleted += deleted
                        logger.debug(f"Cleaned up {deleted} old records from {table}")
                except Exception as e:
                    logger.debug(f"Cleanup error for {table}: {e}")
        
        if total_deleted > 0:
            logger.info(f"Cleanup complete: {total_deleted} records removed")
            self._stats["cleanups"] += 1
    
    # =========================================================================
    # Statistics & Health
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        with self._conn_lock:
            table_counts = {}
            for table in TABLE_SCHEMAS.keys():
                try:
                    result = self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    table_counts[table] = result[0]
                except:
                    table_counts[table] = 0
        
        return {
            "running": self._running,
            "queue_size": self._write_queue.qsize(),
            "writes_queued": self._stats["writes_queued"],
            "writes_committed": self._stats["writes_committed"],
            "reads_executed": self._stats["reads_executed"],
            "parquet_flushes": self._stats.get("parquet_flushes", 0),
            "cleanups": self._stats["cleanups"],
            "table_counts": table_counts,
            "retention_hours": self.retention_hours,
        }
    
    def health_check(self) -> Dict[str, str]:
        """Check engine health."""
        status = {"engine": "ok"}
        
        # Check DuckDB (primary database)
        try:
            with self._conn_lock:
                self._conn.execute("SELECT 1").fetchone()
            status["duckdb"] = "ok"
        except Exception as e:
            status["duckdb"] = f"error: {e}"
        
        # PostgreSQL archive is optional - check but don't fail if unavailable
        try:
            from core.database import _check_postgres_available
            if _check_postgres_available():
                status["postgres_archive"] = "ok"
            else:
                status["postgres_archive"] = "not configured"
        except Exception as e:
            status["postgres_archive"] = f"error: {e}"
        
        # Check threads
        status["batch_writer"] = "running" if self._batch_writer_thread and self._batch_writer_thread.is_alive() else "stopped"
        status["parquet_flush"] = "running" if self._parquet_flush_thread and self._parquet_flush_thread.is_alive() else "stopped"
        status["cleanup"] = "running" if self._cleanup_thread and self._cleanup_thread.is_alive() else "stopped"
        
        return status


# =============================================================================
# Singleton Instance
# =============================================================================

_engine_instance: Optional[TradingDataEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> TradingDataEngine:
    """Get the global TradingDataEngine singleton instance.
    
    Bootstrap from MySQL is ENABLED by default to load plays and recent data.
    """
    global _engine_instance
    
    if _engine_instance is None:
        with _engine_lock:
            if _engine_instance is None:
                _engine_instance = TradingDataEngine(bootstrap_from_mysql=True)
    
    return _engine_instance


def start_engine() -> TradingDataEngine:
    """Get and start the engine (convenience function)."""
    engine = get_engine()
    if not engine._running:
        engine.start()
    return engine


def stop_engine():
    """Stop the global engine instance."""
    global _engine_instance
    if _engine_instance is not None:
        _engine_instance.stop()


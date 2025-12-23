"""
In-Memory DuckDB Trading Data Engine
=====================================
High-performance trading data engine with zero lock contention.

Architecture:
- In-memory DuckDB for 24h data window (instant reads, no file locks)
- Queue-based batch writing (non-blocking writes)
- Background MySQL sync for historical persistence
- Auto-cleanup of data older than 24h

Usage:
    from core.trading_engine import get_engine
    
    engine = get_engine()
    engine.start()
    
    # Non-blocking write
    engine.write('prices', {'ts': datetime.now(), 'token': 'SOL', 'price': 123.45})
    
    # Instant read
    results = engine.read("SELECT * FROM prices WHERE token = ?", ['SOL'])
    
    # Shutdown
    engine.stop()
"""

import duckdb
import pymysql
import threading
import queue
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager

from core.config import settings

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
        self._mysql_sync_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        
        # Control flags
        self._running = False
        self._stop_event = threading.Event()
        
        # Sync watermarks (track what's been synced to MySQL)
        self._sync_watermarks: Dict[str, datetime] = {}
        
        # Auto-increment IDs per table
        self._next_ids: Dict[str, int] = {}
        self._id_lock = threading.Lock()
        
        # Statistics
        self._stats = {
            "writes_queued": 0,
            "writes_committed": 0,
            "reads_executed": 0,
            "mysql_syncs": 0,
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
        
        # Optionally bootstrap from MySQL (disabled by default - only live data)
        if self.bootstrap_from_mysql:
            self._bootstrap_from_mysql()
        else:
            logger.info("Skipping MySQL bootstrap (live data only mode)")
        
        # Start background threads
        self._running = True
        self._stop_event.clear()
        
        self._batch_writer_thread = threading.Thread(
            target=self._batch_writer_loop,
            name="BatchWriter",
            daemon=True
        )
        self._batch_writer_thread.start()
        
        self._mysql_sync_thread = threading.Thread(
            target=self._mysql_sync_loop,
            name="MySQLSync",
            daemon=True
        )
        self._mysql_sync_thread.start()
        
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
        
        # Final MySQL sync
        self._sync_to_mysql()
        
        # Wait for threads
        if self._batch_writer_thread:
            self._batch_writer_thread.join(timeout=5)
        if self._mysql_sync_thread:
            self._mysql_sync_thread.join(timeout=5)
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
        
        # Create tables
        for table_name, schema in TABLE_SCHEMAS.items():
            self._conn.execute(schema)
            logger.debug(f"Created table: {table_name}")
        
        # Create indexes for fast queries
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_token ON prices(token)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_ts ON orderbook(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_price_analysis_created ON price_analysis(created_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_cycle_tracker_start ON cycle_tracker(cycle_start_time)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_order_book_features_ts ON order_book_features(ts)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_order_book_features_symbol ON order_book_features(symbol)")
        
        logger.info("In-memory DuckDB initialized")
    
    def _bootstrap_from_mysql(self):
        """Load last 24h of data from MySQL into memory."""
        logger.info(f"Bootstrapping from MySQL (last {self.retention_hours}h)...")
        
        cutoff = datetime.now() - timedelta(hours=self.retention_hours)
        
        try:
            conn = self._get_mysql_connection()
            cursor = conn.cursor()
            
            # Load prices (from price_points table in MySQL)
            try:
                cursor.execute("""
                    SELECT id, created_at, 
                           CASE coin_id WHEN 5 THEN 'SOL' WHEN 6 THEN 'BTC' ELSE 'ETH' END as token,
                           value
                    FROM price_points
                    WHERE created_at >= %s
                    ORDER BY created_at ASC
                """, [cutoff])
                rows = cursor.fetchall()
                if rows:
                    with self._conn_lock:
                        for row in rows:
                            self._conn.execute(
                                "INSERT INTO prices (id, ts, token, price) VALUES (?, ?, ?, ?)",
                                [row['id'], row['created_at'], row['token'], float(row['value'])]
                            )
                    logger.info(f"Loaded {len(rows)} price records from MySQL")
                    # Set next ID
                    self._next_ids['prices'] = max(r['id'] for r in rows) + 1
            except Exception as e:
                logger.warning(f"Could not load prices: {e}")
            
            # Load price_analysis
            try:
                cursor.execute("""
                    SELECT * FROM price_analysis
                    WHERE created_at >= %s
                    ORDER BY created_at ASC
                """, [cutoff])
                rows = cursor.fetchall()
                if rows:
                    with self._conn_lock:
                        for row in rows:
                            self._conn.execute("""
                                INSERT INTO price_analysis 
                                (id, coin_id, price_point_id, sequence_start_id, sequence_start_price,
                                 current_price, percent_threshold, percent_increase, highest_price_recorded,
                                 lowest_price_recorded, procent_change_from_highest_price_recorded,
                                 percent_increase_from_lowest, price_cycle, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                row['id'], row['coin_id'], row['price_point_id'], 
                                row['sequence_start_id'], float(row['sequence_start_price']),
                                float(row['current_price']), float(row['percent_threshold']),
                                float(row['percent_increase']), float(row['highest_price_recorded']),
                                float(row['lowest_price_recorded']), 
                                float(row['procent_change_from_highest_price_recorded']),
                                float(row['percent_increase_from_lowest']), row['price_cycle'],
                                row['created_at']
                            ])
                    logger.info(f"Loaded {len(rows)} price_analysis records from MySQL")
                    self._next_ids['price_analysis'] = max(r['id'] for r in rows) + 1
            except Exception as e:
                logger.warning(f"Could not load price_analysis: {e}")
            
            # Load cycle_tracker
            try:
                cursor.execute("""
                    SELECT * FROM cycle_tracker
                    WHERE cycle_start_time >= %s
                    ORDER BY cycle_start_time ASC
                """, [cutoff])
                rows = cursor.fetchall()
                if rows:
                    with self._conn_lock:
                        for row in rows:
                            self._conn.execute("""
                                INSERT INTO cycle_tracker
                                (id, coin_id, threshold, cycle_start_time, cycle_end_time,
                                 sequence_start_id, sequence_start_price, highest_price_reached,
                                 lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                                 total_data_points, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                row['id'], row['coin_id'], float(row['threshold']),
                                row['cycle_start_time'], row['cycle_end_time'],
                                row['sequence_start_id'], float(row['sequence_start_price']),
                                float(row['highest_price_reached']), float(row['lowest_price_reached']),
                                float(row['max_percent_increase']), float(row['max_percent_increase_from_lowest']),
                                row['total_data_points'], row['created_at']
                            ])
                    logger.info(f"Loaded {len(rows)} cycle_tracker records from MySQL")
                    self._next_ids['cycle_tracker'] = max(r['id'] for r in rows) + 1
            except Exception as e:
                logger.warning(f"Could not load cycle_tracker: {e}")
            
            cursor.close()
            conn.close()
            
            # Set sync watermarks to now (don't re-sync bootstrapped data)
            now = datetime.now()
            for table in TABLE_SCHEMAS.keys():
                self._sync_watermarks[table] = now
            
            logger.info("Bootstrap from MySQL complete")
            
        except Exception as e:
            logger.error(f"Bootstrap from MySQL failed: {e}")
    
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
        """Commit a batch of writes to DuckDB."""
        if not batch:
            return
        
        # Group by table
        by_table: Dict[str, List[WriteOperation]] = {}
        for op in batch:
            if op.table not in by_table:
                by_table[op.table] = []
            by_table[op.table].append(op)
        
        with self._conn_lock:
            for table, ops in by_table.items():
                for op in ops:
                    # Assign ID if not present
                    if 'id' not in op.data:
                        op.data['id'] = self._get_next_id(table)
                    
                    columns = list(op.data.keys())
                    placeholders = ", ".join(["?" for _ in columns])
                    columns_str = ", ".join(columns)
                    values = [op.data[col] for col in columns]
                    
                    sql = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
                    try:
                        self._conn.execute(sql, values)
                        self._stats["writes_committed"] += 1
                    except Exception as e:
                        logger.error(f"Failed to insert into {table}: {e}")
    
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
    
    def _mysql_sync_loop(self):
        """Background thread that syncs data to MySQL."""
        logger.info("MySQL sync thread started")
        
        while not self._stop_event.wait(self.sync_interval_sec):
            try:
                self._sync_to_mysql()
            except Exception as e:
                logger.error(f"MySQL sync error: {e}")
        
        logger.info("MySQL sync thread stopped")
    
    def _sync_to_mysql(self):
        """Sync recent data to MySQL."""
        try:
            conn = self._get_mysql_connection()
            cursor = conn.cursor()
            
            # Sync prices -> price_points
            self._sync_table_to_mysql(cursor, "prices", "price_points", 
                transform_fn=self._transform_price_for_mysql)
            
            # Sync price_analysis
            self._sync_table_to_mysql(cursor, "price_analysis", "price_analysis")
            
            # Sync cycle_tracker
            self._sync_table_to_mysql(cursor, "cycle_tracker", "cycle_tracker")
            
            # Sync order_book_features
            self._sync_table_to_mysql(cursor, "order_book_features", "order_book_features",
                transform_fn=self._transform_order_book_for_mysql)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self._stats["mysql_syncs"] += 1
            
        except Exception as e:
            logger.error(f"MySQL sync failed: {e}")
    
    def _sync_table_to_mysql(
        self, 
        cursor, 
        duck_table: str, 
        mysql_table: str,
        transform_fn=None
    ):
        """Sync a single table to MySQL."""
        ts_col = TIMESTAMP_COLUMNS.get(duck_table, "ts")
        watermark = self._sync_watermarks.get(duck_table, datetime.now() - timedelta(hours=1))
        
        # Get new records from DuckDB
        with self._conn_lock:
            result = self._conn.execute(f"""
                SELECT * FROM {duck_table}
                WHERE {ts_col} > ?
                ORDER BY {ts_col} ASC
            """, [watermark])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        
        if not rows:
            return
        
        # Transform and insert
        for row in rows:
            record = dict(zip(columns, row))
            
            if transform_fn:
                record = transform_fn(record)
            
            if record is None:
                continue
            
            # Build INSERT IGNORE statement
            cols = list(record.keys())
            placeholders = ", ".join(["%s" for _ in cols])
            cols_str = ", ".join(cols)
            values = [record[c] for c in cols]
            
            try:
                cursor.execute(
                    f"INSERT IGNORE INTO {mysql_table} ({cols_str}) VALUES ({placeholders})",
                    values
                )
            except Exception as e:
                logger.debug(f"MySQL insert error (may be duplicate): {e}")
        
        # Update watermark
        last_ts = dict(zip(columns, rows[-1]))[ts_col]
        self._sync_watermarks[duck_table] = last_ts
        
        logger.debug(f"Synced {len(rows)} records from {duck_table} to {mysql_table}")
    
    def _transform_price_for_mysql(self, record: Dict) -> Dict:
        """Transform DuckDB prices record to MySQL price_points format."""
        token_to_coin = {"SOL": 5, "BTC": 6, "ETH": 7}
        return {
            "id": record["id"],
            "ts_idx": int(record["ts"].timestamp() * 1000),
            "value": record["price"],
            "created_at": record["ts"],
            "coin_id": token_to_coin.get(record["token"], 5),
        }
    
    def _transform_order_book_for_mysql(self, record: Dict) -> Dict:
        """Transform DuckDB order_book_features record for MySQL."""
        # MySQL table uses 'timestamp' column instead of 'ts'
        return {
            "id": record["id"],
            "timestamp": record["ts"],
            "venue": record["venue"],
            "quote_asset": record["quote_asset"],
            "symbol": record["symbol"],
            "best_bid": record["best_bid"],
            "best_ask": record["best_ask"],
            "mid_price": record["mid_price"],
            "absolute_spread": record["absolute_spread"],
            "relative_spread_bps": record["relative_spread_bps"],
            "bid_depth_10": record["bid_depth_10"],
            "ask_depth_10": record["ask_depth_10"],
            "total_depth_10": record["total_depth_10"],
            "volume_imbalance": record["volume_imbalance"],
            "bid_vwap_10": record.get("bid_vwap_10"),
            "ask_vwap_10": record.get("ask_vwap_10"),
            "bid_slope": record.get("bid_slope"),
            "ask_slope": record.get("ask_slope"),
            "microprice": record.get("microprice"),
            "microprice_dev_bps": record.get("microprice_dev_bps"),
            "bid_depth_bps_5": record.get("bid_depth_bps_5"),
            "ask_depth_bps_5": record.get("ask_depth_bps_5"),
            "bid_depth_bps_10": record.get("bid_depth_bps_10"),
            "ask_depth_bps_10": record.get("ask_depth_bps_10"),
            "bid_depth_bps_25": record.get("bid_depth_bps_25"),
            "ask_depth_bps_25": record.get("ask_depth_bps_25"),
            "net_liquidity_change_1s": record.get("net_liquidity_change_1s"),
            "bids_json": record.get("bids_json"),
            "asks_json": record.get("asks_json"),
            "source": record["source"],
        }
    
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
    # MySQL Connection
    # =========================================================================
    
    def _get_mysql_connection(self):
        """Get a MySQL connection for sync operations."""
        return pymysql.connect(
            host=settings.mysql.host,
            user=settings.mysql.user,
            password=settings.mysql.password,
            database=settings.mysql.database,
            port=settings.mysql.port,
            charset=settings.mysql.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
    
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
            "mysql_syncs": self._stats["mysql_syncs"],
            "cleanups": self._stats["cleanups"],
            "table_counts": table_counts,
            "retention_hours": self.retention_hours,
        }
    
    def health_check(self) -> Dict[str, str]:
        """Check engine health."""
        status = {"engine": "ok"}
        
        # Check DuckDB
        try:
            with self._conn_lock:
                self._conn.execute("SELECT 1").fetchone()
            status["duckdb"] = "ok"
        except Exception as e:
            status["duckdb"] = f"error: {e}"
        
        # Check MySQL
        try:
            conn = self._get_mysql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            status["mysql"] = "ok"
        except Exception as e:
            status["mysql"] = f"error: {e}"
        
        # Check threads
        status["batch_writer"] = "running" if self._batch_writer_thread and self._batch_writer_thread.is_alive() else "stopped"
        status["mysql_sync"] = "running" if self._mysql_sync_thread and self._mysql_sync_thread.is_alive() else "stopped"
        status["cleanup"] = "running" if self._cleanup_thread and self._cleanup_thread.is_alive() else "stopped"
        
        return status


# =============================================================================
# Singleton Instance
# =============================================================================

_engine_instance: Optional[TradingDataEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> TradingDataEngine:
    """Get the global TradingDataEngine singleton instance."""
    global _engine_instance
    
    if _engine_instance is None:
        with _engine_lock:
            if _engine_instance is None:
                _engine_instance = TradingDataEngine()
    
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


"""
Master2 Scheduler - Trading Logic
=================================
Trading logic that can be restarted independently without stopping data ingestion.
NO .bat files - everything runs through here.

Usage:
    python scheduler/master2.py

This script (TRADING LOGIC - can restart):
1. Connects to master.py's Data Engine API (port 5050)
2. Loads 2 hours of historical data on startup
3. Creates own DuckDB in-memory instance for trading decisions
4. Runs trading jobs: follow_the_goat, trailing_stop, train_validator, etc.

Prerequisites:
- master.py must be running first (Data Engine)

Shutdown:
    Press Ctrl+C to gracefully stop.
"""

import sys
import os
import signal
import threading
import atexit
import duckdb
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import traceback
import pandas as pd

# Try to import PyArrow for fast DuckDB insertion (zero-copy)
try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

# Try to import Polars for ultra-fast data processing
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

from core.config import settings
from core.data_client import DataClient, get_client

# Import schemas
from features.price_api.schema import SCHEMA_BUYIN_TRAIL_MINUTES

# Import job status tracking from shared module
from scheduler.status import track_job, update_job_status, set_scheduler_start_time, stop_metrics_writer

# FastAPI imports for Local API Server (port 5052)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import uvicorn

# Global references
_scheduler = None
_local_duckdb = None
_local_duckdb_lock = threading.Lock()
_data_client = None
_local_api_server = None  # Uvicorn server thread for port 5052

# Thread-local storage for cursors (allows concurrent reads)
_thread_local = threading.local()


def get_thread_cursor():
    """
    Get a thread-specific cursor for concurrent read operations.
    
    Each thread gets its own cursor from the shared connection.
    DuckDB cursors can safely perform concurrent reads on the same
    underlying data without blocking each other.
    
    Returns:
        DuckDB cursor for the current thread
    """
    global _local_duckdb, _local_duckdb_lock
    
    if _local_duckdb is None:
        raise RuntimeError("Local DuckDB not initialized")
    
    if not hasattr(_thread_local, 'cursor') or _thread_local.cursor is None:
        # Create cursor with lock (cursor creation needs serialization)
        with _local_duckdb_lock:
            _thread_local.cursor = _local_duckdb.cursor()
    
    return _thread_local.cursor


# =============================================================================
# WRITE QUEUE INFRASTRUCTURE
# =============================================================================
# Serializes ALL write operations through a single background thread.
# This eliminates write conflicts while keeping reads concurrent.

from queue import Queue, Empty
from typing import Callable, Any

_write_queue = Queue()
_writer_thread = None
_writer_running = threading.Event()
_write_queue_stats = {
    'total_writes': 0,
    'failed_writes': 0,
    'queue_size': 0
}


def queue_write(func: Callable, *args, **kwargs) -> None:
    """
    Queue a write operation to be executed by the background writer thread.
    
    This ensures all writes are serialized (no conflicts) while reads remain concurrent.
    All thread-local cursors will see the data immediately after write completes.
    
    Args:
        func: Function to call (usually _local_duckdb.execute or _insert_records_fast)
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func
    
    Example:
        queue_write(_local_duckdb.execute, "INSERT INTO prices VALUES (?, ?)", [1, 100.5])
        queue_write(_insert_records_fast, _local_duckdb, "prices", records)
    """
    _write_queue.put((func, args, kwargs))
    _write_queue_stats['queue_size'] = _write_queue.qsize()


def queue_write_sync(func: Callable, *args, **kwargs) -> Any:
    """
    Queue a write and WAIT for it to complete (blocking).
    
    Use this ONLY when you need to read data you just wrote in the same job.
    Most jobs should use queue_write() (non-blocking) instead.
    
    Returns:
        The return value from func
    """
    result_queue = Queue()
    
    def wrapper():
        try:
            result = func(*args, **kwargs)
            result_queue.put(('success', result))
        except Exception as e:
            result_queue.put(('error', e))
    
    _write_queue.put((wrapper, [], {}))
    
    # Wait for result (120s timeout for large PyArrow inserts)
    status, value = result_queue.get(timeout=120)
    if status == 'error':
        raise value
    return value


def background_writer():
    """
    Background thread that processes ALL write operations sequentially.
    
    This runs continuously, processing writes from the queue.
    Ensures no write conflicts and all jobs see consistent data.
    """
    global _local_duckdb, _write_queue_stats
    
    logger.info("Write queue processor started")
    _writer_running.set()
    
    batch_writes = []
    last_batch_time = time.time()
    
    while _writer_running.is_set():
        try:
            # Try to get a write operation (short timeout to check running flag)
            try:
                func, args, kwargs = _write_queue.get(timeout=0.1)
                batch_writes.append((func, args, kwargs))
            except Empty:
                # No writes pending - process any batched writes
                if batch_writes and (time.time() - last_batch_time > 0.05):
                    # Process batch if 50ms elapsed
                    pass
                else:
                    continue
            
            # Process all pending writes in a batch (up to 100 at once)
            while len(batch_writes) < 100:
                try:
                    func, args, kwargs = _write_queue.get_nowait()
                    batch_writes.append((func, args, kwargs))
                except Empty:
                    break
            
            if not batch_writes:
                continue
            
            # Execute all writes in batch with lock
            with _local_duckdb_lock:
                for func, args, kwargs in batch_writes:
                    try:
                        func(*args, **kwargs)
                        _write_queue_stats['total_writes'] += 1
                    except Exception as e:
                        _write_queue_stats['failed_writes'] += 1
                        logger.error(f"Write queue operation failed: {e}", exc_info=True)
                        logger.error(f"  Function: {func.__name__ if hasattr(func, '__name__') else str(func)}")
            
            batch_writes.clear()
            last_batch_time = time.time()
            _write_queue_stats['queue_size'] = _write_queue.qsize()
                
        except Exception as e:
            logger.error(f"Write queue processor error: {e}", exc_info=True)
            batch_writes.clear()
    
    logger.info("Write queue processor stopped")


def start_write_queue():
    """Start the background writer thread."""
    global _writer_thread
    
    if _writer_thread is not None:
        logger.warning("Write queue already started")
        return
    
    _writer_thread = threading.Thread(
        target=background_writer,
        name="DuckDB-Writer",
        daemon=True
    )
    _writer_thread.start()
    
    # Wait for thread to start
    time.sleep(0.1)
    
    logger.info("Write queue processor initialized")


def stop_write_queue():
    """Stop the background writer thread gracefully."""
    global _writer_thread
    
    if _writer_thread is None:
        return
    
    logger.info("Stopping write queue processor...")
    _writer_running.clear()
    
    # Wait for queue to drain (max 5 seconds)
    for i in range(50):
        if _write_queue.empty():
            break
        time.sleep(0.1)
    
    if _writer_thread.is_alive():
        _writer_thread.join(timeout=5)
    
    logger.info(f"Write queue stopped. Stats: {_write_queue_stats}")
    _writer_thread = None


def get_write_queue_stats():
    """Get write queue statistics for monitoring."""
    return dict(_write_queue_stats)


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Trading modules have their own logs directory
TRADING_LOGS_DIR = PROJECT_ROOT / "000trading" / "logs"
TRADING_LOGS_DIR.mkdir(exist_ok=True)

ERROR_LOG_FILE = LOGS_DIR / "scheduler2_errors.log"
ALL_LOG_FILE = LOGS_DIR / "scheduler2_all.log"  # Captures everything for debugging

def clear_all_logs_on_startup():
    """
    Clear ALL relevant log files on startup so we only see fresh errors.
    This includes scheduler logs AND trading module logs.
    """
    # Log files to clear (main files only, not rotated backups)
    logs_to_clear = [
        # Scheduler logs
        LOGS_DIR / "scheduler2_errors.log",
        LOGS_DIR / "scheduler2_all.log",
        # Trading module logs (these modules run under master2.py)
        TRADING_LOGS_DIR / "sell_trailing_stop.log",
        TRADING_LOGS_DIR / "follow_the_goat.log",
        TRADING_LOGS_DIR / "train_validator.log",
    ]
    
    for log_file in logs_to_clear:
        try:
            if log_file.exists():
                log_file.write_text("")
        except Exception:
            pass  # Ignore errors clearing logs

# Clear logs immediately on import (before anything else runs)
clear_all_logs_on_startup()

# Configure root logger to capture ALL logs (DEBUG and above)
logging.basicConfig(
    level=logging.DEBUG,  # Capture everything
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

from logging.handlers import RotatingFileHandler

# Handler 1: ERROR-only log (for quick error checking)
error_file_handler = RotatingFileHandler(
    ERROR_LOG_FILE,
    maxBytes=5 * 1024 * 1024,
    backupCount=0,  # No backups - we clear on startup anyway
    encoding='utf-8'
)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s\n"
    "  File: %(pathname)s:%(lineno)d\n"
    "  Function: %(funcName)s\n",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Handler 2: ALL logs (DEBUG+) for full debugging
all_file_handler = RotatingFileHandler(
    ALL_LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=0,  # No backups - we clear on startup anyway
    encoding='utf-8'
)
all_file_handler.setLevel(logging.DEBUG)
all_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Add both handlers to root logger (captures ALL modules)
logging.getLogger().addHandler(error_file_handler)
logging.getLogger().addHandler(all_file_handler)

logger = logging.getLogger("scheduler2")


# =============================================================================
# GLOBAL EXCEPTION HANDLING
# =============================================================================

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Global handler for uncaught exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    tb_text = ''.join(tb_lines)
    logger.error(f"UNCAUGHT EXCEPTION:\n{tb_text}")

sys.excepthook = global_exception_handler


def apscheduler_error_listener(event: JobExecutionEvent):
    """APScheduler event listener for job errors."""
    if event.exception:
        tb_text = ''.join(traceback.format_exception(
            type(event.exception), 
            event.exception, 
            event.exception.__traceback__
        ))
        logger.error(
            f"SCHEDULER JOB FAILED: {event.job_id}\n"
            f"  Scheduled run time: {event.scheduled_run_time}\n"
            f"  Exception: {event.exception}\n"
            f"  Traceback:\n{tb_text}"
        )


def apscheduler_missed_listener(event: JobExecutionEvent):
    """APScheduler event listener for missed jobs."""
    logger.warning(
        f"SCHEDULER JOB MISSED: {event.job_id}\n"
        f"  Scheduled run time: {event.scheduled_run_time}"
    )


# =============================================================================
# LOCAL DUCKDB INSTANCE
# =============================================================================

def init_local_duckdb():
    """Initialize local in-memory DuckDB for trading decisions."""
    global _local_duckdb
    
    logger.info("Initializing local in-memory DuckDB...")
    _local_duckdb = duckdb.connect(":memory:")
    
    # Create essential tables for trading
    schemas = [
        """
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            token VARCHAR(20) NOT NULL,
            price DOUBLE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_points (
            id BIGINT PRIMARY KEY,
            ts_idx BIGINT,
            coin_id INTEGER NOT NULL,
            value DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
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
        """
        CREATE TABLE IF NOT EXISTS price_analysis (
            id BIGINT PRIMARY KEY,
            coin_id INTEGER NOT NULL,
            price_point_id BIGINT NOT NULL,
            sequence_start_id BIGINT NOT NULL,
            sequence_start_price DOUBLE NOT NULL,
            current_price DOUBLE NOT NULL,
            percent_threshold DOUBLE NOT NULL,
            percent_increase DOUBLE NOT NULL,
            highest_price_recorded DOUBLE NOT NULL,
            lowest_price_recorded DOUBLE NOT NULL,
            procent_change_from_highest_price_recorded DOUBLE NOT NULL,
            percent_increase_from_lowest DOUBLE NOT NULL,
            price_cycle BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
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
            our_exit_timestamp TIMESTAMP,
            fifteen_min_trail JSON
        )
        """,
        """
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
        """
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
        """
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
        SCHEMA_BUYIN_TRAIL_MINUTES,  # Use full schema from features/price_api/schema.py
        """
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
            microprice DOUBLE,
            source VARCHAR(20) NOT NULL,
            bid_depth_bps_5 DOUBLE,
            bid_depth_bps_10 DOUBLE,
            bid_depth_bps_25 DOUBLE,
            ask_depth_bps_5 DOUBLE,
            ask_depth_bps_10 DOUBLE,
            ask_depth_bps_25 DOUBLE,
            bid_vwap_10 DOUBLE,
            ask_vwap_10 DOUBLE,
            bid_slope DOUBLE,
            ask_slope DOUBLE,
            microprice_dev_bps DOUBLE,
            net_liquidity_change_1s DOUBLE,
            bids_json TEXT,
            asks_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS follow_the_goat_buyins_price_checks (
            id BIGINT PRIMARY KEY,
            buyin_id INTEGER NOT NULL,
            checked_at TIMESTAMP NOT NULL,
            current_price DOUBLE NOT NULL,
            entry_price DOUBLE,
            highest_price DOUBLE,
            reference_price DOUBLE,
            gain_from_entry DOUBLE NOT NULL,
            drop_from_high DOUBLE NOT NULL,
            drop_from_entry DOUBLE,
            drop_from_reference DOUBLE,
            tolerance DOUBLE NOT NULL,
            basis VARCHAR(10),
            bucket VARCHAR(10),
            applied_rule JSON,
            should_sell BOOLEAN DEFAULT FALSE,
            is_backfill BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS follow_the_goat_tracking (
            id INTEGER PRIMARY KEY,
            wallet_address VARCHAR(255) NOT NULL UNIQUE,
            last_trade_id BIGINT DEFAULT 0,
            last_checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pattern_config_projects (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wallet_profiles_state (
            id INTEGER PRIMARY KEY,
            threshold DOUBLE NOT NULL UNIQUE,
            last_trade_id BIGINT NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]
    
    for schema in schemas:
        _local_duckdb.execute(schema)
    
    # Create indexes
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices(ts)")
    # Composite index for common query pattern: WHERE token = ? AND ts >= ? AND ts <= ?
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_prices_token_ts ON prices(token, ts)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_price_points_created ON price_points(created_at)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_buyins_status ON follow_the_goat_buyins(our_status)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON sol_stablecoin_trades(trade_timestamp)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_price_analysis_threshold ON price_analysis(coin_id, percent_threshold)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_cycle_tracker_threshold ON cycle_tracker(threshold, cycle_end_time)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_whale_timestamp ON whale_movements(timestamp)")
    _local_duckdb.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tracking_wallet ON follow_the_goat_tracking(wallet_address)")
    _local_duckdb.execute("CREATE INDEX IF NOT EXISTS idx_pattern_filters_project_id ON pattern_config_filters(project_id)")
    _local_duckdb.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_state_threshold ON wallet_profiles_state(threshold)")
    
    # CRITICAL: Register this connection into the pool so that all modules
    # using get_duckdb("central") will use THIS in-memory DB with the data.
    # Without this, trading modules would create NEW empty in-memory DBs!
    #
    # THREAD-SAFE ARCHITECTURE:
    # - READ operations: Use thread-local cursors (concurrent, no lock needed)
    # - WRITE operations: Use connection with lock (serialized)
    #
    # The cursor_factory allows get_duckdb("central", read_only=True) to return
    # a thread-local cursor for concurrent reads without blocking other threads.
    from core.database import register_connection
    register_connection("central", _local_duckdb, _local_duckdb_lock, cursor_factory=get_thread_cursor)
    
    logger.info("Local DuckDB initialized and registered as 'central' with thread-local cursor support")


def get_local_duckdb(use_cursor=True):
    """
    Get database access for the current thread.
    
    Args:
        use_cursor: If True (default), returns a thread-local cursor for READ operations.
                   Cursors can perform concurrent reads without blocking.
                   If False, returns the raw connection (caller MUST hold _local_duckdb_lock for WRITES).
    
    Returns:
        Thread-local cursor (for reads) or connection (for writes with lock)
    
    Usage:
        # For READ operations (concurrent, no lock needed):
        cursor = get_local_duckdb(use_cursor=True)
        result = cursor.execute("SELECT ...").fetchall()
        
        # For WRITE operations (must hold lock):
        with _local_duckdb_lock:
            conn = get_local_duckdb(use_cursor=False)
            conn.execute("INSERT ...")
    """
    global _local_duckdb
    
    if use_cursor:
        return get_thread_cursor()
    else:
        return _local_duckdb


def get_master2_db_for_writes():
    """
    Get master2's local DuckDB connection and lock for WRITE operations.
    
    This is the OFFICIAL way for external modules to write to master2's database.
    
    Returns:
        Tuple of (connection, lock) or (None, None) if not available
        
    Usage:
        conn, lock = get_master2_db_for_writes()
        if conn:
            with lock:
                conn.execute("INSERT ...")
    """
    global _local_duckdb, _local_duckdb_lock
    
    if _local_duckdb is not None:
        return (_local_duckdb, _local_duckdb_lock)
    return (None, None)


# =============================================================================
# LOCAL API SERVER (Port 5052) - Serves data from _local_duckdb
# =============================================================================

def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a row for JSON response (handle datetime, etc.)."""
    result = {}
    for key, value in row.items():
        if hasattr(value, 'isoformat'):
            result[key] = value.isoformat()
        elif isinstance(value, bytes):
            result[key] = value.decode('utf-8', errors='replace')
        else:
            result[key] = value
    return result


def _safe_int(value, default=0):
    """Safely convert value to int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    """Safely convert value to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class QueryRequest(BaseModel):
    """Request model for /query endpoint."""
    sql: str
    params: Optional[List[Any]] = None


class PricePointsRequest(BaseModel):
    """Request model for /price_points endpoint."""
    token: str = "SOL"
    start_datetime: Optional[str] = None
    end_datetime: Optional[str] = None


def create_local_api() -> FastAPI:
    """Create the FastAPI app for serving data from local DuckDB."""
    app = FastAPI(
        title="Follow The Goat - Master2 Local API",
        description="API for serving computed trading data from master2.py's local DuckDB",
        version="1.0.0"
    )
    
    # CORS for web access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.get("/health")
    async def health_check():
        """Health check with table counts from local DuckDB."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            return {
                "status": "error",
                "engine_running": False,
                "message": "Local DuckDB not initialized",
                "timestamp": datetime.now().isoformat()
            }
        
        try:
            tables = {}
            table_names = [
                "prices", "cycle_tracker", "price_analysis", "wallet_profiles",
                "sol_stablecoin_trades", "follow_the_goat_plays", "follow_the_goat_buyins",
                "order_book_features"
            ]
            
            with _local_duckdb_lock:
                for table in table_names:
                    try:
                        result = _local_duckdb.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        tables[table] = result[0] if result else 0
                    except:
                        tables[table] = 0
            
            return {
                "status": "ok",
                "engine_running": True,
                "tables": tables,
                "source": "master2_local_duckdb",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    @app.get("/cycle_tracker")
    async def get_cycle_tracker(
        threshold: Optional[float] = Query(default=None),
        hours: str = Query(default="24"),
        limit: int = Query(default=100),
        active_only: bool = Query(default=True)
    ):
        """
        Get cycle tracker data.
        
        By default, returns only ACTIVE cycles (cycle_end_time IS NULL).
        There should be max 7 active cycles (one per threshold: 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5%).
        
        Set active_only=false to include completed cycles.
        """
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            if active_only:
                # Get only ONE active cycle per threshold (max 7 total)
                # Use window function to get the most recent active cycle per threshold
                threshold_filter = f"AND threshold = {threshold}" if threshold is not None else ""
                
                with _local_duckdb_lock:
                    results = _local_duckdb.execute(f"""
                        WITH ranked_cycles AS (
                            SELECT 
                                id, coin_id, threshold, cycle_start_time, cycle_end_time,
                                sequence_start_id, sequence_start_price, highest_price_reached,
                                lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                                total_data_points, created_at,
                                ROW_NUMBER() OVER (PARTITION BY threshold ORDER BY cycle_start_time DESC) as rn
                            FROM cycle_tracker
                            WHERE coin_id = 5 
                              AND cycle_end_time IS NULL
                              {threshold_filter}
                        )
                        SELECT 
                            id, coin_id, threshold, cycle_start_time, cycle_end_time,
                            sequence_start_id, sequence_start_price, highest_price_reached,
                            lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                            total_data_points, created_at
                        FROM ranked_cycles
                        WHERE rn = 1
                        ORDER BY threshold ASC
                    """).fetchall()
                    columns = [desc[0] for desc in _local_duckdb.description]
            else:
                # Return all cycles (including completed) with optional filters
                conditions = ["coin_id = 5"]  # SOL
                
                if threshold is not None:
                    conditions.append(f"threshold = {threshold}")
                
                if hours != 'all':
                    hours_int = _safe_int(hours, 24)
                    conditions.append(f"cycle_start_time >= NOW() - INTERVAL {hours_int} HOUR")
                
                where_clause = " AND ".join(conditions)
                
                with _local_duckdb_lock:
                    results = _local_duckdb.execute(f"""
                        SELECT 
                            id, coin_id, threshold, cycle_start_time, cycle_end_time,
                            sequence_start_id, sequence_start_price, highest_price_reached,
                            lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                            total_data_points, created_at
                        FROM cycle_tracker
                        WHERE {where_clause}
                        ORDER BY cycle_start_time DESC
                        LIMIT {limit}
                    """).fetchall()
                    columns = [desc[0] for desc in _local_duckdb.description]
            
            cycles = [_serialize_row(dict(zip(columns, row))) for row in results]
            
            return {
                "status": "ok",
                "cycles": cycles,
                "count": len(cycles),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/price_analysis")
    async def get_price_analysis(
        coin_id: int = Query(default=5),
        hours: str = Query(default="24"),
        limit: int = Query(default=100)
    ):
        """Get price analysis data."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        conditions = [f"coin_id = {coin_id}"]
        
        if hours != 'all':
            hours_int = _safe_int(hours, 24)
            conditions.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_clause = " AND ".join(conditions)
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT *
                    FROM price_analysis
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT {limit}
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            analysis = [_serialize_row(dict(zip(columns, row))) for row in results]
            
            return {
                "status": "ok",
                "price_analysis": analysis,
                "count": len(analysis),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/profiles")
    async def get_profiles(
        threshold: Optional[float] = Query(default=None),
        hours: str = Query(default="24"),
        limit: int = Query(default=100),
        wallet: Optional[str] = Query(default=None),
        order_by: str = Query(default="recent")
    ):
        """Get wallet profiles with aggregated data per wallet."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        # Build WHERE conditions
        conditions = []
        
        if threshold is not None:
            conditions.append(f"threshold = {threshold}")
        
        if wallet:
            conditions.append(f"wallet_address = '{wallet}'")
        
        if hours != 'all':
            hours_int = _safe_int(hours, 24)
            conditions.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        # Determine ORDER BY
        if order_by == 'avg_gain':
            order_clause = "ORDER BY avg_potential_gain DESC"
        elif order_by == 'trade_count':
            order_clause = "ORDER BY trade_count DESC"
        else:  # 'recent'
            order_clause = "ORDER BY latest_trade DESC"
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT 
                        wallet_address,
                        COUNT(*) as trade_count,
                        AVG(
                            CASE 
                                WHEN short = 1 THEN 
                                    ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100
                                ELSE 
                                    ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                            END
                        ) as avg_potential_gain,
                        SUM(COALESCE(stablecoin_amount, 0)) as total_invested,
                        SUM(
                            CASE 
                                WHEN short = 1 THEN 
                                    CASE WHEN ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100 < threshold THEN 1 ELSE 0 END
                                ELSE 
                                    CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 < threshold THEN 1 ELSE 0 END
                            END
                        ) as trades_below_threshold,
                        SUM(
                            CASE 
                                WHEN short = 1 THEN 
                                    CASE WHEN ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100 >= threshold THEN 1 ELSE 0 END
                                ELSE 
                                    CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 >= threshold THEN 1 ELSE 0 END
                            END
                        ) as trades_at_above_threshold,
                        MAX(trade_timestamp) as latest_trade,
                        ANY_VALUE(threshold) as threshold_value
                    FROM wallet_profiles
                    {where_clause}
                    GROUP BY wallet_address
                    {order_clause}
                    LIMIT {limit}
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            profiles = [_serialize_row(dict(zip(columns, row))) for row in results]
            
            return {
                "status": "ok",
                "profiles": profiles,
                "count": len(profiles),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/profiles/stats")
    async def get_profiles_stats(
        threshold: Optional[float] = Query(default=None),
        hours: str = Query(default="all")
    ):
        """Get aggregated statistics for wallet profiles."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        # Build WHERE conditions
        conditions = []
        
        if threshold is not None:
            conditions.append(f"threshold = {threshold}")
        
        if hours != 'all':
            hours_int = _safe_int(hours, 24)
            conditions.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(DISTINCT wallet_address) as unique_wallets,
                        COUNT(DISTINCT price_cycle) as unique_cycles,
                        SUM(COALESCE(stablecoin_amount, 0)) as total_invested,
                        AVG(trade_entry_price) as avg_entry_price
                    FROM wallet_profiles
                    {where_clause}
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            stats = _serialize_row(dict(zip(columns, results[0]))) if results else {
                'total_profiles': 0,
                'unique_wallets': 0,
                'unique_cycles': 0,
                'total_invested': 0,
                'avg_entry_price': 0
            }
            
            return {
                "status": "ok",
                "stats": stats,
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/price_points")
    async def get_price_points(request: PricePointsRequest):
        """Get price points for charting."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        token = request.token.upper()
        end_datetime = request.end_datetime or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_datetime = request.start_datetime or (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT ts, price, token
                    FROM prices
                    WHERE token = '{token}'
                      AND ts >= '{start_datetime}'
                      AND ts <= '{end_datetime}'
                    ORDER BY ts ASC
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            prices = []
            for row in results:
                row_dict = dict(zip(columns, row))
                prices.append({
                    'x': row_dict.get('ts').isoformat() if hasattr(row_dict.get('ts'), 'isoformat') else row_dict.get('ts'),
                    'y': row_dict.get('price')
                })
            
            return {
                "status": "ok",
                "token": token,
                "prices": prices,
                "count": len(prices),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/recent_trades")
    async def get_recent_trades(
        limit: int = Query(default=100),
        minutes: int = Query(default=5),
        direction: str = Query(default="all")
    ):
        """Get recent trades from sol_stablecoin_trades."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        conditions = [f"trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE"]
        
        if direction != 'all':
            conditions.append(f"direction = '{direction}'")
        
        where_clause = " AND ".join(conditions)
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT *
                    FROM sol_stablecoin_trades
                    WHERE {where_clause}
                    ORDER BY trade_timestamp DESC
                    LIMIT {limit}
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            trades = [_serialize_row(dict(zip(columns, row))) for row in results]
            
            return {
                "status": "ok",
                "trades": trades,
                "count": len(trades),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/plays")
    async def get_plays(
        full: bool = Query(default=False, description="Return all columns if True, else only id and name")
    ):
        """Get all plays - optimized with PyArrow zero-copy reads."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                # For dropdowns and lists, only fetch id and name (fast)
                # For editing, use full=true to get all columns
                if full:
                    select_clause = "*"
                else:
                    select_clause = "id, name, is_active"
                
                # Use fetch_arrow() for zero-copy read
                arrow_result = _local_duckdb.execute(f"""
                    SELECT {select_clause}
                    FROM follow_the_goat_plays
                    ORDER BY sorting ASC, id DESC
                """).fetch_arrow_table()
            
            # Convert Arrow table to list of dicts
            plays = arrow_result.to_pylist()
            
            # Serialize special types
            plays = [_serialize_row(play) for play in plays]
            
            return {
                "status": "ok",
                "plays": plays,
                "count": len(plays),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/plays/{play_id}")
    async def get_play(play_id: int):
        """Get a single play by ID."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                results = _local_duckdb.execute(f"""
                    SELECT *
                    FROM follow_the_goat_plays
                    WHERE id = {play_id}
                """).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            if results:
                play = _serialize_row(dict(zip(columns, results[0])))
                return {
                    "status": "ok",
                    "play": play
                }
            else:
                raise HTTPException(status_code=404, detail=f"Play {play_id} not found")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/buyins")
    async def get_buyins(
        play_id: Optional[int] = Query(default=None),
        status: Optional[str] = Query(default=None),
        hours: str = Query(default="24"),
        limit: int = Query(default=100)
    ):
        """Get buyins/trades - optimized with PyArrow zero-copy reads."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        conditions = []
        
        if play_id:
            conditions.append(f"play_id = {play_id}")
        
        if status:
            conditions.append(f"our_status = '{status}'")
        
        if hours != 'all':
            hours_int = _safe_int(hours, 24)
            conditions.append(f"followed_at >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        try:
            with _local_duckdb_lock:
                # Use followed_at (correct column name) and leverage composite index idx_buyins_query_opt
                # Only select columns needed by the UI (avoid large JSON fields like fifteen_min_trail)
                # Use fetch_arrow() for zero-copy read (10-100x faster than fetchall())
                arrow_result = _local_duckdb.execute(f"""
                    SELECT 
                        id, play_id, wallet_address, followed_at,
                        our_entry_price, our_exit_price, our_exit_timestamp,
                        our_profit_loss, our_status,
                        higest_price_reached, current_price,
                        CASE WHEN fifteen_min_trail IS NOT NULL THEN 1 ELSE 0 END as has_trail
                    FROM follow_the_goat_buyins
                    {where_clause}
                    ORDER BY followed_at DESC
                    LIMIT {limit}
                """).fetch_arrow_table()
            
            # Convert Arrow table to list of dicts (fast columnar-to-row conversion)
            buyins = arrow_result.to_pylist()
            
            # Serialize timestamps and other special types
            buyins = [_serialize_row(buyin) for buyin in buyins]
            
            return {
                "status": "ok",
                "buyins": buyins,
                "count": len(buyins),
                "source": "master2_local_duckdb"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/query")
    async def execute_query(request: QueryRequest):
        """Execute a generic SELECT query."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        # Basic security: only allow SELECT queries (including CTEs starting with WITH)
        sql_upper = request.sql.strip().upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            raise HTTPException(
                status_code=400,
                detail="Only SELECT queries are allowed."
            )
        
        try:
            with _local_duckdb_lock:
                if request.params:
                    results = _local_duckdb.execute(request.sql, request.params).fetchall()
                else:
                    results = _local_duckdb.execute(request.sql).fetchall()
                columns = [desc[0] for desc in _local_duckdb.description]
            
            serialized = [_serialize_row(dict(zip(columns, row))) for row in results]
            
            return {
                "success": True,
                "count": len(serialized),
                "results": serialized
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    
    @app.post("/execute")
    async def execute_write(request: QueryRequest):
        """Execute INSERT/UPDATE/DELETE queries for trail data persistence."""
        global _local_duckdb
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        # Allow INSERT, UPDATE, DELETE for trail data management
        sql_upper = request.sql.strip().upper()
        allowed_ops = ["INSERT", "UPDATE", "DELETE"]
        if not any(sql_upper.startswith(op) for op in allowed_ops):
            raise HTTPException(
                status_code=400,
                detail="Only INSERT, UPDATE, DELETE queries are allowed."
            )
        
        # Security: only allow operations on trail-related tables
        allowed_tables = ["buyin_trail_minutes"]
        table_check = sql_upper
        if not any(table.upper() in table_check for table in allowed_tables):
            raise HTTPException(
                status_code=400,
                detail=f"Only operations on {allowed_tables} are allowed."
            )
        
        try:
            # Queue write operation (sync to wait for completion)
            if request.params:
                queue_write_sync(_local_duckdb.execute, request.sql, request.params)
            else:
                queue_write_sync(_local_duckdb.execute, request.sql)
            
            return {
                "success": True,
                "message": "Query executed successfully"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Execute failed: {e}")
    
    @app.get("/tables")
    async def list_tables():
        """List all available tables and their row counts."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            tables = {}
            table_names = [
                "prices", "cycle_tracker", "price_analysis", "wallet_profiles",
                "sol_stablecoin_trades", "follow_the_goat_plays", "follow_the_goat_buyins",
                "order_book_features", "price_points", "buyin_trail_minutes"
            ]
            
            with _local_duckdb_lock:
                for table in table_names:
                    try:
                        result = _local_duckdb.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                        tables[table] = result[0] if result else 0
                    except:
                        tables[table] = 0
            
            return {
                "success": True,
                "tables": tables,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/price/{token}")
    async def get_current_price(token: str = "SOL"):
        """Get the current price of a token."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                result = _local_duckdb.execute(f"""
                    SELECT price, ts
                    FROM prices
                    WHERE token = '{token.upper()}'
                    ORDER BY ts DESC
                    LIMIT 1
                """).fetchone()
            
            if result:
                return {
                    "success": True,
                    "token": token.upper(),
                    "price": result[0],
                    "timestamp": result[1].isoformat() if hasattr(result[1], 'isoformat') else result[1]
                }
            else:
                return {
                    "success": True,
                    "token": token.upper(),
                    "price": None,
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/job_metrics")
    async def get_job_metrics_endpoint(hours: float = Query(default=1.0)):
        """
        Get job execution metrics from master.py's TradingDataEngine.
        
        This queries the job_execution_metrics table in master.py's DuckDB
        via the DataClient.
        """
        if _data_client is None:
            raise HTTPException(status_code=503, detail="Data client not initialized")
        
        try:
            minutes = int(hours * 60)
            
            # Query master.py's DuckDB for job execution metrics
            results = _data_client.query(f"""
                SELECT 
                    job_id,
                    COUNT(*) as execution_count,
                    AVG(duration_ms) as avg_duration_ms,
                    MAX(duration_ms) as max_duration_ms,
                    MIN(duration_ms) as min_duration_ms,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                    MAX(started_at) as last_execution
                FROM job_execution_metrics
                WHERE started_at >= NOW() - INTERVAL {minutes} MINUTE
                GROUP BY job_id
                ORDER BY job_id
            """)
            
            # Import expected intervals for determining if jobs are slow
            from features.price_api.schema import JOB_EXPECTED_INTERVALS_MS
            
            jobs = {}
            for row in results:
                job_id = row.get('job_id', 'unknown')
                avg_ms = row.get('avg_duration_ms', 0) or 0
                expected_interval = JOB_EXPECTED_INTERVALS_MS.get(job_id, 60000)
                
                jobs[job_id] = {
                    'job_id': job_id,
                    'execution_count': row.get('execution_count', 0),
                    'avg_duration_ms': round(avg_ms, 2),
                    'max_duration_ms': round(row.get('max_duration_ms', 0) or 0, 2),
                    'min_duration_ms': round(row.get('min_duration_ms', 0) or 0, 2),
                    'error_count': row.get('error_count', 0),
                    'expected_interval_ms': expected_interval,
                    'is_slow': avg_ms > expected_interval * 0.8,
                    'last_execution': row.get('last_execution').isoformat() if row.get('last_execution') else None,
                    'recent_executions': []  # Not implemented in this simplified version
                }
            
            return {
                "status": "ok",
                "hours": hours,
                "jobs": jobs,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get job metrics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "jobs": {},
                "timestamp": datetime.now().isoformat()
            }
    
    @app.get("/scheduler/status")
    async def get_scheduler_status():
        """
        Get scheduler status including uptime and job statuses.
        
        This proxies to the status.py module which tracks job status in-memory.
        """
        from scheduler.status import get_job_status
        return get_job_status()
    
    @app.get("/write_queue_stats")
    async def get_write_queue_stats_endpoint():
        """
        Get write queue statistics for monitoring.
        
        Returns:
            - total_writes: Total number of write operations processed
            - failed_writes: Number of failed write operations
            - queue_size: Current number of pending writes in queue
        """
        return {
            "status": "ok",
            "stats": get_write_queue_stats(),
            "timestamp": datetime.now().isoformat()
        }
    
    # =========================================================================
    # PATTERN CONFIG ENDPOINTS
    # =========================================================================
    
    @app.get("/patterns/projects")
    async def get_pattern_projects():
        """Get all pattern config projects with filter counts."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                # Get all projects
                projects_result = _local_duckdb.execute("""
                    SELECT id, name, description, created_at, updated_at
                    FROM pattern_config_projects
                    ORDER BY id
                """).fetchall()
                projects_cols = [desc[0] for desc in _local_duckdb.description]
                
                # Get filter counts per project
                filter_counts = _local_duckdb.execute("""
                    SELECT project_id, COUNT(*) as filter_count
                    FROM pattern_config_filters
                    GROUP BY project_id
                """).fetchall()
                
                filter_count_map = {row[0]: row[1] for row in filter_counts}
            
            projects = []
            for row in projects_result:
                project = _serialize_row(dict(zip(projects_cols, row)))
                project['filter_count'] = filter_count_map.get(project['id'], 0)
                projects.append(project)
            
            return {
                "status": "ok",
                "projects": projects,
                "count": len(projects)
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/patterns/projects/{project_id}")
    async def get_pattern_project(project_id: int):
        """Get a single pattern project with its filters."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                # Get project
                project_result = _local_duckdb.execute(f"""
                    SELECT id, name, description, created_at, updated_at
                    FROM pattern_config_projects
                    WHERE id = {project_id}
                """).fetchall()
                
                if not project_result:
                    raise HTTPException(status_code=404, detail=f"Project {project_id} not found")
                
                project_cols = [desc[0] for desc in _local_duckdb.description]
                project = _serialize_row(dict(zip(project_cols, project_result[0])))
                
                # Get filters for this project
                filters_result = _local_duckdb.execute(f"""
                    SELECT id, project_id, name, section, minute, field_name, field_column,
                           from_value, to_value, include_null, exclude_mode, play_id,
                           is_active, created_at, updated_at
                    FROM pattern_config_filters
                    WHERE project_id = {project_id}
                    ORDER BY id
                """).fetchall()
                filters_cols = [desc[0] for desc in _local_duckdb.description]
            
            filters = [_serialize_row(dict(zip(filters_cols, row))) for row in filters_result]
            
            return {
                "status": "ok",
                "project": project,
                "filters": filters
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/patterns/projects")
    async def create_pattern_project(data: Dict[str, Any]):
        """Create a new pattern project."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        name = data.get('name')
        description = data.get('description', '')
        
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        
        try:
            with _local_duckdb_lock:
                _local_duckdb.execute("""
                    INSERT INTO pattern_config_projects (name, description)
                    VALUES (?, ?)
                """, [name, description])
                
                # Get the new project ID
                result = _local_duckdb.execute("SELECT last_insert_rowid()").fetchone()
                new_id = result[0] if result else None
            
            return {
                "status": "ok",
                "message": "Project created successfully",
                "id": new_id
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/patterns/projects/{project_id}")
    async def delete_pattern_project(project_id: int):
        """Delete a pattern project and all its filters."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                # Delete filters first
                _local_duckdb.execute(f"""
                    DELETE FROM pattern_config_filters WHERE project_id = {project_id}
                """)
                
                # Delete project
                _local_duckdb.execute(f"""
                    DELETE FROM pattern_config_projects WHERE id = {project_id}
                """)
            
            return {
                "status": "ok",
                "message": f"Project {project_id} deleted successfully"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/patterns/filters")
    async def create_pattern_filter(data: Dict[str, Any]):
        """Create a new pattern filter."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        required_fields = ['project_id', 'name', 'field_name']
        for field in required_fields:
            if field not in data:
                raise HTTPException(status_code=400, detail=f"{field} is required")
        
        try:
            with _local_duckdb_lock:
                _local_duckdb.execute("""
                    INSERT INTO pattern_config_filters 
                    (project_id, name, section, minute, field_name, field_column,
                     from_value, to_value, include_null, exclude_mode, play_id, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    data.get('project_id'),
                    data.get('name'),
                    data.get('section'),
                    data.get('minute'),
                    data.get('field_name'),
                    data.get('field_column'),
                    data.get('from_value'),
                    data.get('to_value'),
                    data.get('include_null', 0),
                    data.get('exclude_mode', 0),
                    data.get('play_id'),
                    data.get('is_active', 1)
                ])
                
                result = _local_duckdb.execute("SELECT last_insert_rowid()").fetchone()
                new_id = result[0] if result else None
            
            return {
                "status": "ok",
                "message": "Filter created successfully",
                "id": new_id
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.put("/patterns/filters/{filter_id}")
    async def update_pattern_filter(filter_id: int, data: Dict[str, Any]):
        """Update a pattern filter."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            # Build UPDATE query dynamically
            set_clauses = []
            values = []
            
            updateable_fields = [
                'name', 'section', 'minute', 'field_name', 'field_column',
                'from_value', 'to_value', 'include_null', 'exclude_mode', 
                'play_id', 'is_active'
            ]
            
            for field in updateable_fields:
                if field in data:
                    set_clauses.append(f"{field} = ?")
                    values.append(data[field])
            
            if not set_clauses:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            # Add updated_at
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            values.append(filter_id)
            
            with _local_duckdb_lock:
                _local_duckdb.execute(f"""
                    UPDATE pattern_config_filters
                    SET {', '.join(set_clauses)}
                    WHERE id = ?
                """, values)
            
            return {
                "status": "ok",
                "message": f"Filter {filter_id} updated successfully"
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.delete("/patterns/filters/{filter_id}")
    async def delete_pattern_filter(filter_id: int):
        """Delete a pattern filter."""
        global _local_duckdb, _local_duckdb_lock
        
        if _local_duckdb is None:
            raise HTTPException(status_code=503, detail="Local DuckDB not initialized")
        
        try:
            with _local_duckdb_lock:
                _local_duckdb.execute(f"""
                    DELETE FROM pattern_config_filters WHERE id = {filter_id}
                """)
            
            return {
                "status": "ok",
                "message": f"Filter {filter_id} deleted successfully"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    # =========================================================================
    # FILTER ANALYSIS ENDPOINTS (reads from TradingDataEngine)
    # =========================================================================
    
    # Config path and defaults for filter analysis
    FILTER_CONFIG_PATH = PROJECT_ROOT / "000data_feeds" / "7_create_new_patterns" / "config.json"
    
    FILTER_SETTINGS_DEFAULTS = {
        'good_trade_threshold': {'value': 0.3, 'description': 'Minimum % gain for a trade to be considered good.', 'type': 'decimal', 'min': 0.1, 'max': 5.0},
        'analysis_hours': {'value': 24, 'description': 'Hours of historical trade data to analyze.', 'type': 'number', 'min': 1, 'max': 168},
        'min_filters_in_combo': {'value': 1, 'description': 'Minimum filters required in a combination.', 'type': 'number', 'min': 1, 'max': 10},
        'max_filters_in_combo': {'value': 6, 'description': 'Maximum filters to combine.', 'type': 'number', 'min': 2, 'max': 15},
        'min_good_trades_kept_pct': {'value': 50, 'description': 'Min % of good trades a single filter must keep.', 'type': 'number', 'min': 10, 'max': 100},
        'min_bad_trades_removed_pct': {'value': 10, 'description': 'Min % of bad trades a single filter must remove.', 'type': 'number', 'min': 5, 'max': 100},
        'combo_min_good_kept_pct': {'value': 25, 'description': 'Min % of good trades a combination must keep.', 'type': 'number', 'min': 5, 'max': 100},
        'combo_min_improvement': {'value': 1.0, 'description': 'Min % improvement to add another filter.', 'type': 'decimal', 'min': 0.1, 'max': 10.0},
    }
    
    def _load_filter_config():
        """Load filter analysis config from JSON file."""
        try:
            if FILTER_CONFIG_PATH.exists():
                with open(FILTER_CONFIG_PATH, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load filter config: {e}")
        return {}
    
    def _save_filter_config(config):
        """Save filter analysis config to JSON file."""
        try:
            with open(FILTER_CONFIG_PATH, 'w') as f:
                json.dump(config, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save filter config: {e}")
            return False
    
    def _get_trading_engine_safe():
        """Get TradingDataEngine, start it if needed."""
        try:
            from core.trading_engine import get_engine
            engine = get_engine()
            if not engine._running:
                logger.info("Starting TradingDataEngine for filter analysis...")
                engine.start()
            return engine
        except Exception as e:
            logger.debug(f"TradingDataEngine not available: {e}")
        return None
    
    @app.get("/filter-analysis/dashboard")
    async def get_filter_analysis_dashboard():
        """Get all data for the filter analysis dashboard."""
        result = {
            'success': True,
            'summary': {},
            'suggestions': [],
            'combinations': [],
            'minute_distribution': [],
            'scheduler_runs': [],
            'filter_consistency': [],
            'trend_chart_data': [],
            'scheduler_stats': {'runs_today': 0, 'last_run': None, 'avg_filters': 0},
            'settings': [],
            'rolling_avgs': {}
        }
        
        # Load settings from config.json
        config = _load_filter_config()
        settings_list = []
        for key, meta in FILTER_SETTINGS_DEFAULTS.items():
            value = config.get(key, meta['value'])
            settings_list.append({
                'setting_key': key,
                'setting_value': str(value),
                'description': meta['description'],
                'setting_type': meta['type'],
                'min_value': meta['min'],
                'max_value': meta['max']
            })
        result['settings'] = settings_list
        
        # Get TradingDataEngine
        engine = _get_trading_engine_safe()
        if engine is None:
            result['error_info'] = 'TradingDataEngine not running. Run create_new_patterns job first.'
            return result
        
        # Summary statistics
        try:
            summary_rows = engine.read("""
                SELECT 
                    COUNT(*) as total_filters, 
                    ROUND(AVG(good_trades_kept_pct), 1) as avg_good_kept,
                    ROUND(AVG(bad_trades_removed_pct), 1) as avg_bad_removed, 
                    ROUND(MAX(bad_trades_removed_pct), 1) as best_bad_removed,
                    MAX(good_trades_before) as total_good_trades, 
                    MAX(bad_trades_before) as total_bad_trades,
                    MAX(created_at) as last_updated, 
                    MAX(analysis_hours) as analysis_hours
                FROM filter_reference_suggestions
            """)
            if summary_rows:
                cols = ['total_filters', 'avg_good_kept', 'avg_bad_removed', 'best_bad_removed',
                       'total_good_trades', 'total_bad_trades', 'last_updated', 'analysis_hours']
                summary = dict(zip(cols, summary_rows[0]))
                if summary.get('last_updated'):
                    summary['last_updated'] = str(summary['last_updated'])[:19]
                result['summary'] = summary
        except Exception as e:
            logger.debug(f"Summary query failed: {e}")
        
        # Minute distribution
        try:
            minute_rows = engine.read("""
                SELECT minute_analyzed, COUNT(*) as filter_count, 
                    ROUND(AVG(bad_trades_removed_pct), 1) as avg_bad_removed, 
                    ROUND(AVG(good_trades_kept_pct), 1) as avg_good_kept
                FROM filter_reference_suggestions 
                GROUP BY minute_analyzed 
                ORDER BY avg_bad_removed DESC
            """)
            if minute_rows:
                result['minute_distribution'] = [
                    dict(zip(['minute_analyzed', 'filter_count', 'avg_bad_removed', 'avg_good_kept'], row))
                    for row in minute_rows
                ]
        except Exception as e:
            logger.debug(f"Minute distribution query failed: {e}")
        
        # All suggestions
        try:
            suggestions_rows = engine.read("""
                SELECT 
                    frs.id, frs.filter_field_id, frs.column_name, frs.from_value, frs.to_value,
                    frs.total_trades, frs.good_trades_before, frs.bad_trades_before,
                    frs.good_trades_after, frs.bad_trades_after,
                    frs.good_trades_kept_pct, frs.bad_trades_removed_pct,
                    frs.analysis_hours, frs.minute_analyzed, frs.created_at,
                    ffc.section, ffc.field_name, ffc.value_type
                FROM filter_reference_suggestions frs 
                LEFT JOIN filter_fields_catalog ffc ON frs.filter_field_id = ffc.id
                ORDER BY frs.bad_trades_removed_pct DESC
            """)
            if suggestions_rows:
                cols = ['id', 'filter_field_id', 'column_name', 'from_value', 'to_value',
                       'total_trades', 'good_trades_before', 'bad_trades_before',
                       'good_trades_after', 'bad_trades_after',
                       'good_trades_kept_pct', 'bad_trades_removed_pct',
                       'analysis_hours', 'minute_analyzed', 'created_at',
                       'section', 'field_name', 'value_type']
                suggestions = []
                for row in suggestions_rows:
                    s = dict(zip(cols, row))
                    for key in ['from_value', 'to_value', 'good_trades_kept_pct', 'bad_trades_removed_pct']:
                        if s.get(key) is not None:
                            s[key] = float(s[key])
                    if s.get('created_at'):
                        s['created_at'] = str(s['created_at'])[:19]
                    suggestions.append(s)
                result['suggestions'] = suggestions
        except Exception as e:
            logger.debug(f"Suggestions query failed: {e}")
        
        # Filter combinations
        try:
            combo_rows = engine.read("""
                SELECT id, combination_name, filter_count, filter_ids, filter_columns,
                    total_trades, good_trades_before, bad_trades_before,
                    good_trades_after, bad_trades_after,
                    good_trades_kept_pct, bad_trades_removed_pct,
                    COALESCE(minute_analyzed, 0) as minute_analyzed, analysis_hours
                FROM filter_combinations 
                ORDER BY bad_trades_removed_pct DESC
            """)
            if combo_rows:
                cols = ['id', 'combination_name', 'filter_count', 'filter_ids', 'filter_columns',
                       'total_trades', 'good_trades_before', 'bad_trades_before',
                       'good_trades_after', 'bad_trades_after',
                       'good_trades_kept_pct', 'bad_trades_removed_pct',
                       'minute_analyzed', 'analysis_hours']
                combinations = []
                for row in combo_rows:
                    combo = dict(zip(cols, row))
                    for key in ['good_trades_kept_pct', 'bad_trades_removed_pct']:
                        if combo.get(key) is not None:
                            combo[key] = float(combo[key])
                    if isinstance(combo.get('filter_ids'), str):
                        combo['filter_ids'] = json.loads(combo['filter_ids'])
                    if isinstance(combo.get('filter_columns'), str):
                        combo['filter_columns'] = json.loads(combo['filter_columns'])
                    combo['filter_details'] = {}
                    combinations.append(combo)
                result['combinations'] = combinations
        except Exception as e:
            logger.debug(f"Combinations query failed: {e}")
        
        # Filter consistency from suggestions
        try:
            if result['suggestions']:
                consistency_data = []
                for s in result['suggestions'][:30]:
                    consistency_data.append({
                        'filter_column': s['column_name'],
                        'total_runs': 1,
                        'times_in_best_combo': 1,
                        'consistency_pct': 100.0,
                        'avg_bad_removed': s['bad_trades_removed_pct'],
                        'avg_good_kept': s['good_trades_kept_pct'],
                        'avg_effectiveness': round((s['bad_trades_removed_pct'] * s['good_trades_kept_pct']) / 100, 2),
                        'last_seen': s.get('created_at'),
                        'latest_minute': s['minute_analyzed'],
                        'latest_from': s['from_value'],
                        'latest_to': s['to_value']
                    })
                result['filter_consistency'] = consistency_data
        except Exception as e:
            logger.debug(f"Consistency derivation failed: {e}")
        
        # Rolling averages
        try:
            for s in result['suggestions']:
                result['rolling_avgs'][s['column_name']] = s['bad_trades_removed_pct']
        except:
            pass
        
        return result
    
    @app.get("/filter-analysis/settings")
    async def get_filter_settings():
        """Get auto filter settings from config.json."""
        config = _load_filter_config()
        settings_list = []
        for key, meta in FILTER_SETTINGS_DEFAULTS.items():
            value = config.get(key, meta['value'])
            settings_list.append({
                'setting_key': key,
                'setting_value': str(value),
                'description': meta['description'],
                'setting_type': meta['type'],
                'min_value': meta['min'],
                'max_value': meta['max']
            })
        return {'success': True, 'settings': settings_list}
    
    @app.post("/filter-analysis/settings")
    async def save_filter_settings(data: dict):
        """Save auto filter settings to config.json."""
        new_settings = data.get('settings', {})
        config = _load_filter_config()
        
        errors = []
        for key, value in new_settings.items():
            if key in FILTER_SETTINGS_DEFAULTS:
                meta = FILTER_SETTINGS_DEFAULTS[key]
                try:
                    if meta['type'] == 'number':
                        config[key] = int(float(value))
                    elif meta['type'] == 'decimal':
                        config[key] = float(value)
                    else:
                        config[key] = value
                except Exception as e:
                    errors.append(f"{key}: {str(e)}")
            else:
                errors.append(f"{key}: unknown setting")
        
        if not _save_filter_config(config):
            raise HTTPException(status_code=500, detail="Failed to save config file")
        
        current = {key: config.get(key, meta['value']) 
                  for key, meta in FILTER_SETTINGS_DEFAULTS.items()}
        
        return {
            'success': True,
            'message': 'Settings saved to config.json',
            'errors': errors,
            'current_settings': current
        }
    
    return app


def start_local_api(port: int = 5052, host: str = "0.0.0.0"):
    """
    Start the Local API server in a background thread.
    
    This runs uvicorn in a separate thread so it doesn't block the scheduler.
    """
    global _local_api_server
    
    if _local_api_server is not None:
        logger.warning("Local API server already running")
        return
    
    try:
        logger.debug(f"Creating Local API app...")
        app = create_local_api()
        logger.debug(f"Local API app created successfully")
    except Exception as e:
        logger.error(f"Failed to create Local API app: {e}", exc_info=True)
        return
    
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",  # Reduce noise in logs
        access_log=False
    )
    server = uvicorn.Server(config)
    
    def run_server():
        """Run uvicorn server in thread."""
        try:
            logger.debug(f"Starting uvicorn server in thread...")
            server.run()
            logger.debug(f"Uvicorn server stopped")
        except Exception as e:
            logger.error(f"Local API server error: {e}", exc_info=True)
    
    _local_api_server = threading.Thread(
        target=run_server,
        name="LocalAPI-5052",
        daemon=True
    )
    _local_api_server.start()
    
    # Give the server a moment to start
    time.sleep(1.5)
    
    logger.info(f"Local API server started on http://{host}:{port}")
    logger.info(f"  Endpoints: /health, /cycle_tracker, /price_analysis, /profiles, /plays, /buyins, /query")


def stop_local_api():
    """Stop the Local API server."""
    global _local_api_server
    
    if _local_api_server is None:
        return
    
    # The server thread is a daemon, so it will be killed when main thread exits
    # We just need to clear our reference
    logger.info("Local API server stopping (daemon thread will terminate with main process)")
    _local_api_server = None


# =============================================================================
# DATA BACKFILL FROM MASTER.PY
# =============================================================================


def _fetch_table_data(client, table: str, hours: int, limit: int) -> tuple:
    """Fetch data for a single table (for parallel execution)."""
    try:
        start_time = time.time()
        records = client.get_backfill(table, hours=hours, limit=limit)
        fetch_time = time.time() - start_time
        return table, records, fetch_time, None
    except Exception as e:
        return table, None, 0, str(e)


def _insert_records_fast(conn, table: str, records: list) -> int:
    """
    Fast batch insert using DuckDB's native capabilities.
    
    Insertion priority (tries in order):
    1. Polars DataFrame (FASTEST - native Rust, ~0.5s for 50K records)
    2. PyArrow zero-copy (fast, ~1s for 50K records)
    3. Pandas DataFrame (slower, ~30s for 50K records)
    4. executemany (slowest, for small batches or fallback)
    
    NOTE: This function is called from the write queue thread which already
    holds _local_duckdb_lock. Do NOT acquire any locks here.
    
    Args:
        conn: DuckDB connection (must be _local_duckdb)
        table: Table name
        records: List of dict records
    
    Returns:
        Number of records inserted
    """
    if not records:
        return 0
    
    def _do_insert():
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        num_records = len(records)
        
        # =================================================================
        # METHOD 1: Polars DataFrame (PRIMARY - FASTEST!)
        # =================================================================
        # Polars is MUCH faster than PyArrow for large datasets:
        # - Native Rust implementation (no Python overhead)
        # - Optimized Arrow conversion
        # - Better datetime/JSON handling
        # Use for any batch > 100 records
        if HAS_POLARS and num_records > 100:
            try:
                start_time = time.time()
                
                # Create Polars DataFrame directly from list of dicts
                # This is MUCH faster than building columnar data manually
                df = pl.DataFrame(records)
                
                # Convert to Arrow (zero-copy, native Rust speed)
                arrow_table = df.to_arrow()
                
                # Register and insert (same as PyArrow method)
                conn.register('_temp_polars', arrow_table)
                conn.execute(f"INSERT OR IGNORE INTO {table} ({columns_str}) SELECT {columns_str} FROM _temp_polars")
                conn.unregister('_temp_polars')
                
                elapsed = time.time() - start_time
                
                # Get current count for logging
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                current_count = result[0] if result else 0
                
                logger.info(f"Polars insert for {table}: {num_records} records in {elapsed:.2f}s (table now has {current_count} rows)")
                return num_records
                
            except Exception as e:
                logger.warning(f"Polars insert failed for {table}: {e}, falling back to PyArrow")
                # Fall through to PyArrow method
        
        # =================================================================
        # METHOD 2: PyArrow zero-copy insertion (FALLBACK #1)
        # =================================================================
        # Use PyArrow for batches > 20 records (even small batches benefit)
        if HAS_PYARROW and num_records > 20:
            try:
                start_time = time.time()
                
                # Build columnar data for Arrow (more efficient than row-by-row)
                col_data = {col: [r.get(col) for r in records] for col in columns}
                
                # Build explicit schema to handle JSON strings and datetimes properly
                # This prevents DuckDB from trying to cast JSON arrays to numeric types
                schema_fields = []
                for col in columns:
                    sample_values = [v for v in col_data[col][:10] if v is not None]
                    if not sample_values:
                        # No non-null samples, default to string
                        schema_fields.append(pa.field(col, pa.string()))
                        continue
                    
                    sample = sample_values[0]
                    
                    # Check type of first non-null value
                    if isinstance(sample, bool):
                        schema_fields.append(pa.field(col, pa.bool_()))
                    elif isinstance(sample, int):
                        schema_fields.append(pa.field(col, pa.int64()))
                    elif isinstance(sample, float):
                        schema_fields.append(pa.field(col, pa.float64()))
                    elif isinstance(sample, str):
                        # Check if it's a JSON array/object (starts with [ or {)
                        sample_stripped = sample.strip()
                        if sample_stripped.startswith('[') or sample_stripped.startswith('{'):
                            # JSON data - keep as string
                            schema_fields.append(pa.field(col, pa.string()))
                        elif len(sample) >= 10 and ('T' in sample or (sample.count('-') >= 2 and ':' in sample)):
                            # Looks like a datetime string - convert to timestamp
                            try:
                                col_data[col] = pd.to_datetime(col_data[col], errors='coerce', format='ISO8601').values
                                schema_fields.append(pa.field(col, pa.timestamp('us')))
                            except Exception:
                                schema_fields.append(pa.field(col, pa.string()))
                        else:
                            # Regular string
                            schema_fields.append(pa.field(col, pa.string()))
                    else:
                        # Unknown type - let Arrow infer
                        schema_fields.append(pa.field(col, pa.string()))
                
                # Create PyArrow table with explicit schema
                schema = pa.schema(schema_fields)
                arrow_table = pa.Table.from_pydict(col_data, schema=schema)
                
                # Register Arrow table with DuckDB (zero-copy) and insert
                # Use explicit column names to avoid column order mismatch issues
                conn.register('_temp_arrow', arrow_table)
                conn.execute(f"INSERT OR IGNORE INTO {table} ({columns_str}) SELECT {columns_str} FROM _temp_arrow")
                conn.unregister('_temp_arrow')
                
                elapsed = time.time() - start_time
                
                # Get current count for logging
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                current_count = result[0] if result else 0
                
                logger.info(f"PyArrow insert for {table}: {num_records} records in {elapsed:.2f}s (table now has {current_count} rows)")
                return num_records
                
            except Exception as e:
                logger.warning(f"PyArrow insert failed for {table}: {e}, falling back to pandas")
                # Fall through to pandas method
        
        # =================================================================
        # METHOD 2: Pandas DataFrame insertion (FALLBACK)
        # =================================================================
        # For very large batches (>10K records), use chunked pandas insert
        if num_records > 10000 or (table == "order_book_features" and num_records > 5000):
            logger.info(f"Large batch for {table} ({num_records} records), using chunked pandas insert...")
            inserted = 0
            chunk_size = 5000
            start_time = time.time()
            
            for i in range(0, num_records, chunk_size):
                chunk = records[i:i + chunk_size]
                try:
                    # Use pandas for each chunk
                    df = pd.DataFrame(chunk)
                    df = df[columns]
                    
                    # Convert datetime columns
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            try:
                                df[col] = pd.to_datetime(df[col], errors='ignore')
                            except:
                                pass
                    
                    conn.register('_temp_import', df)
                    conn.execute(f"INSERT OR IGNORE INTO {table} ({columns_str}) SELECT {columns_str} FROM _temp_import")
                    conn.unregister('_temp_import')
                    inserted += len(chunk)
                    logger.debug(f"  Chunk {i//chunk_size + 1}: inserted {len(chunk)} records")
                except Exception as e:
                    logger.warning(f"Chunk insert failed for {table} chunk {i//chunk_size}: {e}")
            
            elapsed = time.time() - start_time
            logger.info(f"Pandas chunked insert for {table}: {inserted}/{num_records} records in {elapsed:.2f}s")
            return inserted
        
        # For medium batches (500-10K), use single pandas DataFrame
        if num_records > 500:
            try:
                start_time = time.time()
                
                # DuckDB can efficiently insert from pandas DataFrame
                df = pd.DataFrame(records)
                df = df[columns]  # Ensure column order matches
                
                # Convert datetime columns to proper format
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_datetime(df[col], errors='ignore')
                        except:
                            pass
                
                # Register as temp view and insert (use explicit columns to avoid order mismatch)
                conn.register('_temp_import', df)
                conn.execute(f"INSERT OR IGNORE INTO {table} ({columns_str}) SELECT {columns_str} FROM _temp_import")
                conn.unregister('_temp_import')
                
                elapsed = time.time() - start_time
                
                # Count actual inserted rows
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                current_count = result[0] if result else 0
                logger.info(f"Pandas insert for {table}: {num_records} records in {elapsed:.2f}s (table now has {current_count} rows)")
                return num_records
            except Exception as e:
                logger.warning(f"Pandas insert failed for {table}: {e}, falling back to executemany")
                logger.debug(f"Sample record columns: {list(records[0].keys())}")
                # Fall through to executemany
        
        # =================================================================
        # METHOD 3: executemany (LAST RESORT - for small batches or fallback)
        # =================================================================
        placeholders = ", ".join(["?" for _ in columns])
        
        # Pre-allocate and use list comprehension (faster than generator)
        all_values = [tuple(record[col] for col in columns) for record in records]
        
        before_count = 0
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            before_count = result[0] if result else 0
        except:
            pass
        
        try:
            start_time = time.time()
            conn.executemany(
                f"INSERT OR IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})",
                all_values
            )
            elapsed = time.time() - start_time
            
            # Check how many were actually inserted
            after_count = 0
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                after_count = result[0] if result else 0
                inserted = after_count - before_count
                if inserted == 0 and num_records > 0:
                    logger.warning(f"Executemany for {table}: 0 rows inserted (all duplicates or schema mismatch?)")
                else:
                    logger.info(f"Executemany insert for {table}: {num_records} records in {elapsed:.2f}s")
                return num_records
            except:
                return num_records
        except Exception as e:
            logger.warning(f"Executemany insert failed for {table}: {e}")
            # Fallback: smaller batches
            inserted = 0
            batch_size = 1000
            for i in range(0, len(all_values), batch_size):
                batch = all_values[i:i + batch_size]
                try:
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})",
                        batch
                    )
                    inserted += len(batch)
                except Exception as e2:
                    logger.error(f"Batch insert failed for {table} (batch {i//batch_size}): {e2}")
                    if i == 0:  # Log first record of first failed batch
                        logger.error(f"First record sample: {records[0]}")
            
            if inserted == 0 and num_records > 0:
                logger.error(f"CRITICAL: Could not insert any of {num_records} records into {table}")
            return inserted
    
    # Execute directly - caller (write queue thread) already holds the lock
    return _do_insert()


def _load_plays_from_postgres():
    """
    Load plays from PostgreSQL into local DuckDB.
    
    Plays are stored in PostgreSQL and need to be loaded into the local
    in-memory DuckDB for trading logic to access them.
    
    Returns:
        List of play records or None if PostgreSQL unavailable
    """
    try:
        import psycopg2
        import psycopg2.extras
        
        # Get PostgreSQL credentials from env
        from core.config import settings
        
        conn = psycopg2.connect(
            host=settings.postgres.host,
            user=settings.postgres.user,
            password=settings.postgres.password,
            database=settings.postgres.database,
            port=settings.postgres.port,
            cursor_factory=psycopg2.extras.RealDictCursor,
            connect_timeout=3,
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM follow_the_goat_plays ORDER BY id")
            plays = cursor.fetchall()
        
        conn.close()
        
        # Convert to list of dicts
        plays_list = [dict(play) for play in plays]
        logger.info(f"  Loaded {len(plays_list)} plays from PostgreSQL")
        return plays_list
        
    except Exception as e:
        logger.warning(f"  Could not load plays from PostgreSQL: {e}")
        return None


def _load_plays_from_json_cache():
    """
    Load plays from JSON cache file (fallback if PostgreSQL unavailable).
    
    Returns:
        List of play records
    """
    cache_file = PROJECT_ROOT / "config" / "plays_cache.json"
    
    if not cache_file.exists():
        logger.warning(f"  Plays cache file not found: {cache_file}")
        return []
    
    try:
        import json
        with open(cache_file, 'r', encoding='utf-8') as f:
            plays = json.load(f)
        
        logger.info(f"  Loaded {len(plays)} plays from JSON cache")
        return plays
    except Exception as e:
        logger.error(f"  Error reading plays cache: {e}")
        return []


def backfill_from_data_engine(hours: int = 24):
    """
    Load historical data from master.py's Data Engine API.
    
    This is called on startup to populate the local DuckDB with recent data
    so trading decisions can be made immediately.
    
    Special handling for plays:
    - Plays are loaded from PostgreSQL (primary source)
    - Falls back to JSON cache if PostgreSQL unavailable
    - Plays are NOT time-limited (loads all active plays)
    
    Optimizations:
    - Parallel HTTP fetching (all tables fetched concurrently)
    - Fast batch inserts using pandas DataFrame (bypasses Python iteration)
    - Sequential inserts to avoid DuckDB lock contention
    """
    global _data_client, _local_duckdb
    
    logger.info(f"Backfilling {hours} hours of data from Data Engine API...")
    
    # Wait for Data Engine to be available
    max_retries = 30
    for i in range(max_retries):
        if _data_client.is_available():
            break
        logger.info(f"Waiting for Data Engine API... ({i+1}/{max_retries})")
        time.sleep(2)
    else:
        logger.error("Data Engine API not available after 60 seconds!")
        return False
    
    # =========================================================================
    # SPECIAL: Load plays from PostgreSQL (not from Data Engine API)
    # =========================================================================
    logger.info("  Loading plays from PostgreSQL...")
    plays = _load_plays_from_postgres()
    
    if plays is None or len(plays) == 0:
        # Fallback to JSON cache
        logger.info("  Falling back to JSON cache for plays...")
        plays = _load_plays_from_json_cache()
    
    if plays:
        try:
            # Insert plays into local DuckDB via write queue
            inserted = queue_write_sync(_insert_records_fast, _local_duckdb, "follow_the_goat_plays", plays)
            logger.info(f"  Loaded {inserted} plays into DuckDB")
            
            # Show active plays count
            active_plays = [p for p in plays if p.get('is_active', 0) == 1]
            logger.info(f"  Active plays: {len(active_plays)}/{len(plays)}")
        except Exception as e:
            logger.error(f"  Error loading plays into DuckDB: {e}")
    else:
        logger.warning("  WARNING: No plays loaded! Trading logic may not work.")
    
    # =========================================================================
    # SPECIAL: Load pattern projects from PostgreSQL (not from Data Engine API)
    # =========================================================================
    logger.info("  Loading pattern projects from PostgreSQL...")
    try:
        from core.pattern_loader import load_pattern_projects_from_postgres
        if load_pattern_projects_from_postgres(_local_duckdb):
            logger.info("  ✓ Pattern projects loaded successfully")
        else:
            logger.info("  Pattern projects not available (skipping)")
    except Exception as e:
        logger.warning(f"  Failed to load pattern projects: {e}")
    
    # =========================================================================
    # Tables to backfill from Data Engine API
    # =========================================================================
    # NOTE: follow_the_goat_plays is NOT included here - loaded from PostgreSQL above
    # NOTE: cycle_tracker IS backfilled from master.py where it's now computed incrementally
    tables_to_backfill = [
        ("prices", 10000),
        ("price_points", 10000),
        ("cycle_tracker", 1000),  # Copy cycles computed by master.py
        ("follow_the_goat_buyins", 5000),
        ("sol_stablecoin_trades", 20000),
        ("wallet_profiles", 10000),
        ("buyin_trail_minutes", 10000),
        ("order_book_features", 50000),  # Higher limit for order book data
        ("whale_movements", 10000),  # Whale activity data for trail generation
    ]
    
    total_loaded = 0
    fetch_results = {}
    
    # =========================================================================
    # PHASE 1: Parallel HTTP fetching (network I/O bound - parallelize!)
    # =========================================================================
    logger.info("  Phase 1: Fetching data in parallel...")
    phase1_start = time.time()
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_fetch_table_data, _data_client, table, hours, limit): table
            for table, limit in tables_to_backfill
        }
        
        for future in as_completed(futures):
            table, records, fetch_time, error = future.result()
            if error:
                logger.warning(f"    {table}: fetch failed - {error}")
            elif records:
                fetch_results[table] = records
                logger.info(f"    {table}: fetched {len(records)} records ({fetch_time:.2f}s)")
            else:
                logger.info(f"    {table}: no records")
    
    phase1_elapsed = time.time() - phase1_start
    logger.info(f"  Phase 1 complete: fetched from {len(fetch_results)} tables ({phase1_elapsed:.2f}s)")
    
    # =========================================================================
    # PHASE 2: Sequential inserts (DuckDB writes - MUST use lock for safety)
    # =========================================================================
    logger.info("  Phase 2: Inserting into local DuckDB...")
    phase2_start = time.time()
    
    # For in-memory shared connections, we MUST lock during writes
    # to prevent memory corruption (even on Linux)
    for table, records in fetch_results.items():
        try:
            start_time = time.time()
            # Use sync write during backfill to ensure data is ready before scheduler starts
            inserted = queue_write_sync(_insert_records_fast, _local_duckdb, table, records)
            elapsed = time.time() - start_time
            total_loaded += inserted
            logger.info(f"    {table}: inserted {inserted} records ({elapsed:.2f}s)")
        except Exception as e:
            logger.warning(f"    {table}: insert failed - {e}")
    
    phase2_elapsed = time.time() - phase2_start
    logger.info(f"  Phase 2 complete: inserted records ({phase2_elapsed:.2f}s)")
    
    # =========================================================================
    # PHASE 3: Initialize last_synced_ids for incremental sync
    # =========================================================================
    logger.info("  Phase 3: Initializing incremental sync positions...")
    _initialize_sync_positions()
    
    # =========================================================================
    # PHASE 4: Populate price_points from prices for legacy compatibility
    # =========================================================================
    logger.info("  Phase 4: Syncing prices → price_points (legacy compatibility)...")
    try:
        with _local_duckdb_lock:
            # Check if we have prices data
            result = _local_duckdb.execute("SELECT COUNT(*) FROM prices WHERE token = 'SOL'").fetchone()
            prices_count = result[0] if result else 0
            
            if prices_count > 0:
                # Populate price_points from prices (coin_id=5 for SOL)
                _local_duckdb.execute("""
                    INSERT INTO price_points (id, ts_idx, coin_id, value, created_at)
                    SELECT 
                        ROW_NUMBER() OVER (ORDER BY id) as id,
                        id as ts_idx,
                        5 as coin_id,
                        price as value,
                        ts as created_at
                    FROM prices 
                    WHERE token = 'SOL'
                    ORDER BY id
                """)
                
                result = _local_duckdb.execute("SELECT COUNT(*) FROM price_points").fetchone()
                pp_count = result[0] if result else 0
                logger.info(f"    Synced {pp_count} price points (coin_id=5 for SOL)")
            else:
                logger.info("    No prices to sync yet")
    except Exception as e:
        logger.warning(f"    Failed to sync prices→price_points: {e}")
    
    total_elapsed = phase1_elapsed + phase2_elapsed
    logger.info(f"Backfill complete: {total_loaded} total records loaded ({total_elapsed:.2f}s)")
    return True


def _initialize_sync_positions():
    """
    Initialize _last_synced_ids with the max ID from each table.
    
    This ensures incremental sync starts from where backfill ended,
    not from ID 0 (which would re-sync all backfilled data).
    """
    global _local_duckdb, _local_duckdb_lock, _last_synced_ids
    
    sync_tables = ["prices", "order_book_features", "sol_stablecoin_trades", "whale_movements"]
    
    try:
        with _local_duckdb_lock:
            for table in sync_tables:
                try:
                    result = _local_duckdb.execute(f"SELECT MAX(id) FROM {table}").fetchone()
                    max_id = result[0] if result and result[0] else 0
                    _last_synced_ids[table] = max_id
                    logger.debug(f"    {table}: sync position set to ID {max_id}")
                except Exception as e:
                    logger.debug(f"    {table}: could not get max ID - {e}")
                    _last_synced_ids[table] = 0
    except Exception as e:
        logger.warning(f"Failed to initialize sync positions: {e}")


# Track last synced IDs for incremental sync
_last_synced_ids = {
    "prices": 0,
    "order_book_features": 0,
    "sol_stablecoin_trades": 0,
    "whale_movements": 0
}


def sync_new_data_from_engine():
    """
    Sync NEW data from the Data Engine (runs every 1 second).
    
    INCREMENTAL SYNC: Only fetches records with ID > last_synced_id.
    This is much faster than time-based backfill for real-time trading.
    
    CRITICAL FOR TRADING: Uses PyArrow batch insert for maximum speed.
    """
    global _data_client, _local_duckdb, _local_duckdb_lock, _last_synced_ids
    
    # Tables to sync - ordered by priority for trading decisions
    # prices & order_book_features are most critical for trading signals
    # NOTE: cycle_tracker is NOT synced - it's computed locally by create_price_cycles.py
    sync_tables = ["prices", "order_book_features", "sol_stablecoin_trades", "whale_movements"]
    
    for table in sync_tables:
        try:
            since_id = _last_synced_ids.get(table, 0)
            
            # Get only NEW records since last sync
            records, max_id = _data_client.get_new_since_id(table, since_id=since_id, limit=1000)
            
            if not records:
                continue
            
            # Queue the batch insert (non-blocking, no conflicts!)
            queue_write(_insert_records_fast, _local_duckdb, table, records)
            
            # Update sync position immediately (write queue processes in FIFO order)
            _last_synced_ids[table] = max_id
            
            if len(records) > 10:
                logger.debug(f"Queued {len(records)} records from {table} (max_id: {max_id})")
                
        except Exception as e:
            # Only log at debug level to avoid spam (sync runs every 1s)
            logger.debug(f"Sync error for {table}: {e}")


def sync_prices_to_price_points():
    """
    Sync prices table → price_points table with proper coin_id format.
    
    This bridges the gap between:
    - prices table (token='SOL', used by master.py Data Engine)
    - price_points table (coin_id=5, used by legacy modules like create_price_cycles)
    
    Runs after prices sync to ensure price_points is always up to date.
    """
    global _local_duckdb, _local_duckdb_lock
    
    try:
        # Get latest price_point ID to avoid duplicates
        with _local_duckdb_lock:
            result = _local_duckdb.execute("""
                SELECT COALESCE(MAX(id), 0) as max_id FROM price_points
            """).fetchone()
            last_price_point_id = result[0] if result else 0
        
        # Get latest prices.id that we've already synced
        with _local_duckdb_lock:
            result = _local_duckdb.execute("""
                SELECT COALESCE(MAX(id), 0) as max_prices_id 
                FROM price_points 
                WHERE ts_idx IS NOT NULL
            """).fetchone()
            last_synced_prices_id = result[0] if result else 0
        
        # Fetch new prices that haven't been synced to price_points yet
        cursor = get_thread_cursor()
        new_prices = cursor.execute("""
            SELECT id, ts, price 
            FROM prices 
            WHERE token = 'SOL' 
              AND id > ?
            ORDER BY id ASC
            LIMIT 1000
        """, [last_synced_prices_id]).fetchall()
        
        if not new_prices:
            return  # No new prices to sync
        
        # Transform to price_points format
        price_points = []
        for idx, (prices_id, ts, price) in enumerate(new_prices):
            price_points.append({
                'id': last_price_point_id + idx + 1,  # Sequential ID
                'ts_idx': prices_id,  # Reference to prices.id
                'coin_id': 5,  # SOL = coin_id 5 (legacy format)
                'value': price,
                'created_at': ts
            })
        
        # Insert into price_points via write queue
        if price_points:
            queue_write(_insert_records_fast, _local_duckdb, "price_points", price_points)
            logger.debug(f"Synced {len(price_points)} prices → price_points (SOL coin_id=5)")
    
    except Exception as e:
        logger.debug(f"Sync prices→price_points error: {e}")


# =============================================================================
# TRADING JOB FUNCTIONS
# =============================================================================

@track_job("train_validator", "Validator training cycle (every 15s)")
def run_train_validator():
    """Run a single validator training cycle."""
    enabled = os.getenv("TRAIN_VALIDATOR_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Train validator disabled via TRAIN_VALIDATOR_ENABLED=0")
        return
    
    trading_path = PROJECT_ROOT / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from train_validator import run_training_cycle
    
    success = run_training_cycle()
    if not success:
        logger.warning("Train validator cycle failed")


@track_job("follow_the_goat", "Wallet tracker cycle (every 1s)")
def run_follow_the_goat():
    """Run a single wallet tracking cycle."""
    enabled = os.getenv("FOLLOW_THE_GOAT_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Follow the goat disabled via FOLLOW_THE_GOAT_ENABLED=0")
        return
    
    trading_path = PROJECT_ROOT / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from follow_the_goat import run_single_cycle
    
    trades_found = run_single_cycle()
    if trades_found:
        logger.debug("Follow the goat: new trades processed")


@track_job("trailing_stop_seller", "Trailing stop seller (every 1s)")
def run_trailing_stop_seller():
    """Run a single trailing stop monitoring cycle."""
    enabled = os.getenv("TRAILING_STOP_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Trailing stop seller disabled via TRAILING_STOP_ENABLED=0")
        return
    
    trading_path = PROJECT_ROOT / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from sell_trailing_stop import run_single_cycle
    
    positions_checked = run_single_cycle()
    if positions_checked > 0:
        logger.debug(f"Trailing stop: checked {positions_checked} position(s)")


@track_job("update_potential_gains", "Update potential gains (every 15s)")
def run_update_potential_gains():
    """Update potential_gains for buyins with completed price cycles."""
    enabled = os.getenv("UPDATE_POTENTIAL_GAINS_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Update potential gains disabled via UPDATE_POTENTIAL_GAINS_ENABLED=0")
        return
    
    data_feeds_path = PROJECT_ROOT / "000data_feeds" / "6_update_potential_gains"
    if str(data_feeds_path) not in sys.path:
        sys.path.insert(0, str(data_feeds_path))
    from update_potential_gains import run as update_gains
    
    result = update_gains()
    if result.get('updated', 0) > 0:
        logger.debug(f"Potential gains: updated {result['updated']} records")


@track_job("create_new_patterns", "Auto-generate filter patterns (every 15 min)")
def run_create_new_patterns():
    """Auto-generate filter patterns from trade data analysis."""
    enabled = os.getenv("CREATE_NEW_PATTERNS_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Create new patterns disabled via CREATE_NEW_PATTERNS_ENABLED=0")
        return
    
    patterns_path = PROJECT_ROOT / "000data_feeds" / "7_create_new_patterns"
    if str(patterns_path) not in sys.path:
        sys.path.insert(0, str(patterns_path))
    from create_new_paterns import run as run_pattern_generator
    
    result = run_pattern_generator()
    if result.get('success'):
        logger.info(f"Pattern generation: {result.get('suggestions_count', 0)} suggestions, "
                    f"{result.get('combinations_count', 0)} combinations, "
                    f"{result.get('filters_synced', 0)} filters synced")
    else:
        logger.warning(f"Pattern generation failed: {result.get('error', 'Unknown error')}")


@track_job("sync_from_engine", "Sync data from Data Engine (every 1s)")
def run_sync_from_engine():
    """Sync NEW data from the Data Engine API (incremental sync by ID)."""
    sync_new_data_from_engine()
    
    # After syncing prices, also sync to price_points table for legacy compatibility
    sync_prices_to_price_points()


@track_job("create_wallet_profiles", "Build wallet profiles (every 2s)")
def run_create_wallet_profiles():
    """
    Build wallet profiles from trades and price cycles.
    
    Uses local in-memory DuckDB which is synced from master.py's Data Engine API.
    This ensures 1000x faster reads compared to PostgreSQL.
    
    The profile builder joins:
    - sol_stablecoin_trades (buy trades from wallets)
    - cycle_tracker (completed price cycles)
    - prices (Jupiter SOL prices for entry price lookup)
    
    Profiles are ALSO pushed to master.py's Data Engine API so the website can see them.
    """
    global _local_duckdb, _local_duckdb_lock, _data_client
    
    enabled = os.getenv("CREATE_WALLET_PROFILES_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Wallet profiles disabled via CREATE_WALLET_PROFILES_ENABLED=0")
        return
    
    if _local_duckdb is None:
        logger.warning("Local DuckDB not initialized - skipping profile creation")
        return
    
    profiles_path = PROJECT_ROOT / "000data_feeds" / "5_create_profiles"
    if str(profiles_path) not in sys.path:
        sys.path.insert(0, str(profiles_path))
    from create_profiles import build_profiles_for_local_duckdb
    
    try:
        # Pass data_client so profiles are pushed to Data Engine API (for website)
        processed = build_profiles_for_local_duckdb(_local_duckdb, _local_duckdb_lock, _data_client)
        if processed > 0:
            logger.debug(f"Wallet profiles: created {processed} profiles")
    except Exception as e:
        logger.error(f"Wallet profiles error: {e}")


@track_job("cleanup_wallet_profiles", "Clean up old profiles (every hour)")
def run_cleanup_wallet_profiles():
    """
    Clean up wallet profiles older than 24 hours from local DuckDB.
    """
    global _local_duckdb, _local_duckdb_lock
    
    if _local_duckdb is None:
        return
    
    profiles_path = PROJECT_ROOT / "000data_feeds" / "5_create_profiles"
    if str(profiles_path) not in sys.path:
        sys.path.insert(0, str(profiles_path))
    from create_profiles import cleanup_old_profiles_local
    
    try:
        deleted = cleanup_old_profiles_local(_local_duckdb, hours=24, lock=_local_duckdb_lock)
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old wallet profiles")
    except Exception as e:
        logger.error(f"Wallet profiles cleanup error: {e}")


@track_job("export_job_status", "Export job status to file (every 5s)")
def export_job_status_to_file():
    """Export current job status to JSON file for website_api.py to read."""
    import json
    from scheduler.status import _job_status, _job_status_lock
    
    try:
        status_file = LOGS_DIR / "master2_job_status.json"
        
        with _job_status_lock:
            status_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'jobs': dict(_job_status)
            }
        
        # Write atomically (write to temp file, then rename)
        temp_file = status_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(status_data, f, indent=2)
        
        temp_file.replace(status_file)
        logger.debug(f"Exported {len(status_data['jobs'])} job statuses")
        
    except Exception as e:
        logger.error(f"Failed to export job status: {e}")


# NOTE: sync_cycles_to_engine() REMOVED
# 
# Architecture clarification:
#   - master.py (5050) = Raw data ingestion API ONLY (prices, whale, order book, transactions)
#   - master2.py (5052) = MAIN database with ALL computed data (cycles, profiles, buyins, etc.)
#   - website_api.py (5051) = Proxies ALL queries to master2.py (5052)
#
# There is NO need to sync data back to master.py. All data lives in master2's DuckDB
# and is queried directly via the Local API (port 5052).


@track_job("process_price_cycles", "Process price cycles (every 2s)")
def run_process_price_cycles():
    """
    Process price points into price cycle analysis.
    
    Reads price data from local DuckDB and tracks price cycles at multiple thresholds:
    0.2%, 0.25%, 0.3%, 0.35%, 0.4%, 0.45%, 0.5%
    
    A cycle ends when price drops X% below the highest price reached in that cycle.
    There can only be 7 active cycles at any time (one per threshold).
    
    All cycle data stays in master2's local DuckDB and is served via Local API (port 5052).
    Website queries master2 directly - no need to sync back to master.py.
    """
    enabled = os.getenv("PRICE_CYCLES_ENABLED", "1") == "1"
    if not enabled:
        return
    
    cycles_path = PROJECT_ROOT / "000data_feeds" / "2_create_price_cycles"
    if str(cycles_path) not in sys.path:
        sys.path.insert(0, str(cycles_path))
    from create_price_cycles import process_price_cycles
    
    try:
        processed = process_price_cycles()
        if processed > 0:
            logger.debug(f"Price cycles: processed {processed} price points")
        
    except Exception as e:
        logger.error(f"Price cycles error: {e}")


# =============================================================================
# SCHEDULER CREATION
# =============================================================================

def create_scheduler() -> BackgroundScheduler:
    """Create and configure the trading scheduler."""
    executors = {
        'realtime': APThreadPoolExecutor(max_workers=10),
        'heavy': APThreadPoolExecutor(max_workers=4),
    }
    
    job_defaults = {
        'coalesce': True,
        'max_instances': 1,
        'misfire_grace_time': 30
    }
    
    scheduler = BackgroundScheduler(
        timezone=settings.scheduler_timezone,
        executors=executors,
        job_defaults=job_defaults
    )
    
    scheduler.add_listener(apscheduler_error_listener, EVENT_JOB_ERROR)
    scheduler.add_listener(apscheduler_missed_listener, EVENT_JOB_MISSED)
    
    # =====================================================
    # DATA SYNC JOB (keeps local DuckDB up to date)
    # INCREMENTAL SYNC: Only fetches records with ID > last_synced_id
    # =====================================================
    scheduler.add_job(
        func=run_sync_from_engine,
        trigger=IntervalTrigger(seconds=1),
        id="sync_from_engine",
        name="Sync data from Data Engine (every 1s)",
        replace_existing=True,
        executor='realtime',
    )
    
    # =====================================================
    # TRADING JOBS
    # =====================================================
    
    # Follow The Goat - Wallet Tracker (every 1s)
    scheduler.add_job(
        func=run_follow_the_goat,
        trigger=IntervalTrigger(seconds=1),
        id="follow_the_goat",
        name="Wallet tracker cycle (every 1s)",
        replace_existing=True,
        executor='realtime',
    )
    
    # Trailing Stop Seller (every 1s)
    scheduler.add_job(
        func=run_trailing_stop_seller,
        trigger=IntervalTrigger(seconds=1),
        id="trailing_stop_seller",
        name="Trailing stop seller (every 1s)",
        replace_existing=True,
        executor='realtime',
    )
    
    # Validator Training (every 15s)
    scheduler.add_job(
        func=run_train_validator,
        trigger=IntervalTrigger(seconds=15),
        id="train_validator",
        name="Validator training cycle (every 15s)",
        replace_existing=True,
        executor='heavy',
    )
    
    # Update Potential Gains (every 15s)
    scheduler.add_job(
        func=run_update_potential_gains,
        trigger=IntervalTrigger(seconds=15),
        id="update_potential_gains",
        name="Update potential gains (every 15s)",
        replace_existing=True,
        executor='heavy',
    )
    
    # Create New Patterns (every 15 min)
    scheduler.add_job(
        func=run_create_new_patterns,
        trigger=IntervalTrigger(minutes=15),
        id="create_new_patterns",
        name="Auto-generate filter patterns (every 15 min)",
        replace_existing=True,
        executor='heavy',
    )
    
    # =====================================================
    # WALLET PROFILE JOBS (moved from master.py)
    # =====================================================
    
    # Wallet Profile Builder (every 5 seconds) - INCREASED from 2s
    # Builds profiles from trades + completed cycles at all thresholds
    # Uses local in-memory DuckDB (synced from master.py's Data Engine API)
    scheduler.add_job(
        func=run_create_wallet_profiles,
        trigger=IntervalTrigger(seconds=5),
        id="create_wallet_profiles",
        name="Build wallet profiles (every 5s)",
        replace_existing=True,
        executor='heavy',
    )
    
    # Wallet Profile Cleanup (every hour)
    # Cleans up profiles older than 24 hours from local DuckDB
    scheduler.add_job(
        func=run_cleanup_wallet_profiles,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_wallet_profiles",
        name="Clean up wallet profiles (24hr window)",
        replace_existing=True,
        executor='heavy',
    )
    
    # =====================================================
    # PRICE CYCLE ANALYSIS (moved from master.py)
    # =====================================================
    
    # Process Price Cycles (every 2 seconds) - INCREASED from 1s
    # Tracks price cycles at 7 thresholds (0.2% to 0.5%)
    # A cycle ends when price drops X% below highest price in that cycle
    # Max 7 active cycles at any time (one per threshold)
    # ==============================================
    # REMOVED: Process price cycles (now runs in master.py)
    # ==============================================
    # Price cycles are now computed in master.py (Data Engine) where all the
    # historical price data lives. Master2 copies the computed cycles via backfill.
    # This eliminates the blocking issue on master2 startup.
    
    # scheduler.add_job(
    #     func=run_process_price_cycles,
    #     trigger=IntervalTrigger(seconds=2),
    #     id="process_price_cycles",
    #     name="Process price cycles (every 2s)",
    #     replace_existing=True,
    #     executor='heavy',
    # )
    
    # Export job status to file (every 5 seconds)
    # Writes current job status to JSON file for website_api.py to read
    scheduler.add_job(
        func=export_job_status_to_file,
        trigger=IntervalTrigger(seconds=5),
        id="export_job_status",
        name="Export job status (every 5s)",
        replace_existing=True,
        executor='realtime',
    )
    
    return scheduler


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Start the trading logic scheduler."""
    global _scheduler, _data_client
    
    def handle_shutdown(signum, frame):
        sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        logger.info(f"\nReceived {sig_name} signal")
        shutdown_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, handle_shutdown)
    
    logger.info("=" * 60)
    logger.info("Starting Follow The Goat - Trading Logic (master2)")
    logger.info("=" * 60)
    logger.info(f"Timezone: {settings.scheduler_timezone}")
    logger.info(f"Data Engine API: http://localhost:5050")
    logger.info(f"Error Log: {ERROR_LOG_FILE}")
    
    set_scheduler_start_time()
    
    # =====================================================
    # STEP 1: Initialize Data Client
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 1: Connecting to Data Engine API...")
    _data_client = DataClient(base_url="http://localhost:5050")
    
    # =====================================================
    # STEP 2: Initialize Local DuckDB
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2: Initializing local DuckDB (in-memory)...")
    init_local_duckdb()
    
    # =====================================================
    # STEP 2.5: Start Write Queue Processor
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2.5: Starting write queue processor...")
    start_write_queue()
    
    # Register write queue with core.database so trading modules can use duckdb_execute_write()
    from core.database import register_write_queue
    register_write_queue("central", queue_write, queue_write_sync)
    
    # =====================================================
    # STEP 3: Backfill Historical Data
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 3: Backfilling 24 hours of historical data...")
    if not backfill_from_data_engine(hours=24):
        logger.error("Failed to backfill data - master.py may not be running!")
        logger.error("Please start master.py first, then restart master2.py")
        return

    # =====================================================
    # STEP 3.5: Start Local API Server (port 5052)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 3.5: Starting Local API Server (port 5052)...")
    start_local_api(port=5052)
    logger.info(f"Website can now connect to http://localhost:5052")

    # =====================================================
    # STEP 3.25: SKIP Historical Price Cycles Processing
    # =====================================================
    # NO LONGER NEEDED: Cycles are built incrementally by the scheduler job (every 2s)
    # Historical processing was causing write queue backlog and timeouts
    logger.info("-" * 60)
    logger.info("STEP 3.25: Skipping historical cycle processing (handled by scheduler)")
    logger.info("  Cycles will be built incrementally as new prices arrive")
    logger.info("  This avoids write queue backlog on startup")

    atexit.register(shutdown_all)

    # =====================================================
    # STEP 4: Start Scheduler
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 4: Starting Trading Scheduler...")
    _scheduler = create_scheduler()
    
    jobs = _scheduler.get_jobs()
    logger.info(f"Registered {len(jobs)} trading jobs:")
    for job in sorted(jobs, key=lambda j: j.id):
        logger.info(f"  {job.id}: {job.name}")
    
    logger.info("-" * 60)
    logger.info("Trading logic running! Press Ctrl+C to stop.")
    logger.info("=" * 60)
    
    _scheduler.start()
    
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown_all()


def shutdown_all():
    """Gracefully shutdown all services."""
    global _scheduler, _local_duckdb, _data_client, _local_api_server

    logger.info("=" * 60)
    logger.info("SHUTTING DOWN master2...")
    logger.info("=" * 60)

    if _scheduler is not None:
        try:
            logger.info("[1/5] Stopping scheduler...")
            _scheduler.shutdown(wait=False)
            _scheduler = None
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")

    # Stop write queue BEFORE closing database (allows pending writes to complete)
    logger.info("[2/5] Stopping write queue processor...")
    stop_write_queue()

    logger.info("[3/5] Stopping Local API server...")
    stop_local_api()

    logger.info("[4/5] Stopping metrics writer...")
    stop_metrics_writer()

    if _local_duckdb is not None:
        try:
            logger.info("[5/5] Closing local DuckDB...")
            _local_duckdb.close()
            _local_duckdb = None
        except Exception as e:
            logger.error(f"Error closing DuckDB: {e}")

    if _data_client is not None:
        _data_client.close()
        _data_client = None
    
    logger.info("=" * 60)
    logger.info("SHUTDOWN COMPLETE - Goodbye!")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Capture ANY startup crash to the error log
        import traceback
        tb_text = traceback.format_exc()
        logger.error(f"FATAL STARTUP CRASH:\n{tb_text}")
        print(f"\n{'='*60}")
        print("FATAL ERROR - Check logs/scheduler2_errors.log for details")
        print(f"{'='*60}")
        print(tb_text)
        sys.exit(1)

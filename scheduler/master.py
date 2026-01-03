"""
Master Scheduler - Data Engine
==============================
Data ingestion engine that runs continuously without restarts.
NO .bat files - everything runs through here.

Usage:
    python scheduler/master.py

This script (DATA ENGINE - runs indefinitely):
1. Starts the TradingDataEngine (in-memory DuckDB with zero locks)
2. Starts the FastAPI data API server (port 5050)
3. Starts the FastAPI webhook server (port 8001)
4. Starts the PHP webserver (port 8000)
5. Starts Binance order book stream
6. Schedules data jobs: Jupiter prices, trade sync, cleanup

IMPORTANT: This is ONLY for RAW DATA ingestion.
All computation (cycles, profiles, trading logic) is in master2.py.

Trading logic is handled by master2.py which can be restarted independently.

Shutdown:
    Press Ctrl+C to gracefully stop all services.
"""

import sys
import os
import signal
import threading
import atexit
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import traceback

from core.database import cleanup_all_hot_tables, start_trading_engine, stop_trading_engine
from core.config import settings

# Import job status tracking from shared module (avoids circular imports with API)
from scheduler.status import track_job, update_job_status, set_scheduler_start_time, stop_metrics_writer

# Global reference to the trading engine
_trading_engine = None

# Global reference to Binance stream collector
_binance_collector = None

# Global reference to FastAPI data API server (port 5050)
_data_api_server = None

# Global reference to FastAPI webhook server (port 8001)
_webhook_server = None

# Global reference to PHP server process (port 8000)
_php_server_process = None

# Global reference to the scheduler (for clean shutdown)
_scheduler = None

# Import price fetcher
sys.path.insert(0, str(PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices"))

# Import Binance stream module
sys.path.insert(0, str(PROJECT_ROOT / "000data_feeds" / "3_binance_order_book_data"))

# =============================================================================
# LOGGING CONFIGURATION - Console + Error File
# =============================================================================

# Create logs directory if it doesn't exist
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Error log file path
ERROR_LOG_FILE = LOGS_DIR / "scheduler_errors.log"

# Truncate error log on each startup (fresh start)
# This ensures you only see errors from the current run
if ERROR_LOG_FILE.exists():
    ERROR_LOG_FILE.write_text("")  # Clear the file

# Configure root logger
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Add rotating file handler for ERROR level logs only
from logging.handlers import RotatingFileHandler

error_file_handler = RotatingFileHandler(
    ERROR_LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB max per file
    backupCount=5,              # Keep 5 backup files
    encoding='utf-8'
)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s\n"
    "  File: %(pathname)s:%(lineno)d\n"
    "  Function: %(funcName)s\n",
    datefmt="%Y-%m-%d %H:%M:%S"
))

# Add error handler to root logger (captures all errors from all modules)
logging.getLogger().addHandler(error_file_handler)

logger = logging.getLogger("scheduler")
logger.info(f"Error logging enabled: {ERROR_LOG_FILE}")


# =============================================================================
# GLOBAL EXCEPTION HANDLING - Catch uncaught exceptions
# =============================================================================

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """
    Global handler for uncaught exceptions.
    Logs the full traceback to both console and error log file.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        # Don't log keyboard interrupts (Ctrl+C)
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    # Format the full traceback
    tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    tb_text = ''.join(tb_lines)
    
    logger.error(f"UNCAUGHT EXCEPTION:\n{tb_text}")

# Install global exception handler
sys.excepthook = global_exception_handler


def apscheduler_error_listener(event: JobExecutionEvent):
    """
    APScheduler event listener for job errors.
    Logs detailed error information when a scheduled job fails.
    """
    if event.exception:
        # Get the full traceback
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
    """
    APScheduler event listener for missed jobs.
    Logs when a job execution was missed (system was too busy).
    """
    logger.warning(
        f"SCHEDULER JOB MISSED: {event.job_id}\n"
        f"  Scheduled run time: {event.scheduled_run_time}"
    )


def thread_exception_handler(args):
    """
    Handler for uncaught exceptions in threads.
    Python 3.8+ threading.excepthook support.
    """
    if args.exc_type == SystemExit:
        return
    
    # Format the full traceback
    tb_lines = traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)
    tb_text = ''.join(tb_lines)
    
    logger.error(
        f"UNCAUGHT THREAD EXCEPTION in '{args.thread.name}':\n{tb_text}"
    )

# Install thread exception handler (Python 3.8+)
if hasattr(threading, 'excepthook'):
    threading.excepthook = thread_exception_handler


# =============================================================================
# JOB FUNCTIONS (wrapped with @track_job for status monitoring)
# =============================================================================

@track_job("fetch_jupiter_prices", "Fetch prices from Jupiter API (every 1s)")
def fetch_jupiter_prices():
    """Fetch prices from Jupiter API and store in DuckDB + MySQL."""
    from get_prices_from_jupiter import fetch_and_store_once
    count, duck_ok, mysql_ok = fetch_and_store_once()
    if count > 0 and (not duck_ok or not mysql_ok):
        logger.warning(f"Partial price write - DuckDB: {duck_ok}, MySQL: {mysql_ok}")


@track_job("cleanup_jupiter_prices", "Clean up Jupiter DuckDB (every hour)")
def cleanup_jupiter_prices():
    """Clean up old price data from Jupiter DuckDB."""
    from get_prices_from_jupiter import cleanup_old_data
    deleted = cleanup_old_data()
    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old Jupiter price records")


@track_job("cleanup_duckdb_hot_tables", "Clean up DuckDB hot tables (every hour)")
def cleanup_duckdb_hot_tables():
    """
    Clean up all DuckDB hot tables with archive to MySQL.
    
    Process:
    1. Select data older than retention threshold
    2. Archive to MySQL (if configured)
    3. Delete from DuckDB
    
    Retention periods:
    - Trades (buyins): 72 hours (settings.trades_hot_storage_hours)
    - Other tables: 24 hours (settings.hot_storage_hours)
    """
    logger.info(f"Running DuckDB hot table cleanup with archive (trades: {settings.trades_hot_storage_hours}h, others: {settings.hot_storage_hours}h)...")
    total_cleaned = cleanup_all_hot_tables("central")  # Archives then deletes
    logger.info(f"Cleanup complete: {total_cleaned} records archived and removed")


# NOTE: archive_legacy_price_points was removed because:
# 1. The legacy prices.duckdb uses 'ts' column, not 'created_at'
# 2. cleanup_jupiter_prices already handles cleanup correctly via cleanup_old_data()
# 3. See get_prices_from_jupiter.py for the correct cleanup implementation


# NOTE: sync_plays_from_mysql has been removed.
# Plays are now loaded from config/plays_cache.json at startup.
# No MySQL sync is performed during runtime.


# Global state for incremental trade sync (tracks last synced ID)
_last_synced_trade_id = 0
_last_sync_initialized = False

# Webhook base URL for trade backfill/sync
WEBHOOK_TRADES_URL = "http://195.201.84.5/api/trades"


def _get_last_synced_trade_id() -> int:
    """Get the last synced trade ID from TradingDataEngine (for startup recovery)."""
    global _last_synced_trade_id, _last_sync_initialized
    
    if _last_sync_initialized:
        return _last_synced_trade_id
    
    try:
        from core.database import get_trading_engine
        engine = get_trading_engine()
        if engine and engine._running:
            result = engine.read_one("SELECT COALESCE(MAX(id), 0) as max_id FROM sol_stablecoin_trades")
            _last_synced_trade_id = result['max_id'] if result else 0
            _last_sync_initialized = True
            
            # Also log how many trades are in the engine
            count_result = engine.read_one("SELECT COUNT(*) as cnt FROM sol_stablecoin_trades")
            trade_count = count_result['cnt'] if count_result else 0
            logger.info(f"Trade sync initialized: last_id={_last_synced_trade_id}, existing_trades={trade_count}")
            return _last_synced_trade_id
    except Exception as e:
        logger.warning(f"Engine not available for last trade ID: {e}")
    
    # Fallback to file-based DuckDB if engine not available
    try:
        from core.database import get_duckdb
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sol_stablecoin_trades").fetchone()
            _last_synced_trade_id = result[0] if result else 0
            _last_sync_initialized = True
            logger.info(f"Trade sync initialized from file-DB: last_id={_last_synced_trade_id}")
    except Exception as e:
        logger.debug(f"Could not get last trade ID: {e}")
        _last_synced_trade_id = 0
        _last_sync_initialized = True
    
    return _last_synced_trade_id


@track_job("sync_trades_from_webhook", "Sync trades from Webhook DuckDB (every 1s)")
def sync_trades_from_webhook():
    """No-op: trades are now pushed directly via FastAPI webhook (port 8000)."""
    logger.debug("sync_trades_from_webhook skipped (push-based webhook)")


# NOTE: _sync_trades_from_mysql_fallback has been removed.
# Trades are sourced exclusively from the webhook (DuckDB-to-DuckDB).
# MySQL is only used for archiving old data, not as a data source.


def _normalize_trade_timestamp(ts_value):
    """Convert webhook trade_timestamp to datetime."""
    if ts_value is None:
        return datetime.utcnow()
    if isinstance(ts_value, datetime):
        return ts_value
    if isinstance(ts_value, str):
        clean = ts_value.replace("T", " ").replace("Z", "")
        try:
            return datetime.strptime(clean[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        try:
            return datetime.fromisoformat(clean)
        except Exception:
            return datetime.utcnow()
    try:
        return datetime.fromtimestamp(ts_value)
    except Exception:
        return datetime.utcnow()


def _has_recent_trades(hours: int = 24) -> bool:
    """Check if DuckDB already has trades within the last N hours."""
    try:
        from core.database import get_duckdb
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        with get_duckdb("central", read_only=True) as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM sol_stablecoin_trades WHERE trade_timestamp >= ?",
                [cutoff],
            ).fetchone()
            return (result[0] if result else 0) > 0
    except Exception as e:
        logger.debug(f"Recent trade check failed: {e}")
        return False


def fetch_trades_last_24h_from_webhook(hours: int = 24, limit: int = 5000):
    """Page trades from webhook covering the last `hours`."""
    import requests

    start_time = datetime.utcnow() - timedelta(hours=hours)
    after_id = 0
    fetched = []

    while True:
        params = {
            "start": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "limit": limit,
        }
        if after_id > 0:
            params["after_id"] = after_id

        resp = requests.get(WEBHOOK_TRADES_URL, params=params, timeout=8)
        if resp.status_code != 200:
            raise RuntimeError(f"Webhook HTTP {resp.status_code}")

        payload = resp.json()
        results = payload.get("results") or []
        if not results:
            break

        fetched.extend(results)
        max_id = payload.get("max_id")
        if not max_id:
            try:
                max_id = max(r.get("id", 0) for r in results)
            except Exception:
                max_id = after_id

        if not max_id or max_id <= after_id:
            break

        after_id = max_id
        if len(results) < limit:
            break

    return fetched


def _insert_trades_into_duckdb(trades) -> int:
    """Insert trades into DuckDB hot storage with dedupe."""
    if not trades:
        return 0

    from core.database import get_duckdb
    from features.price_api.schema import SCHEMA_SOL_STABLECOIN_TRADES

    try:
        with get_duckdb("central") as conn:
            conn.execute(SCHEMA_SOL_STABLECOIN_TRADES)
            batch = []
            now_ts = datetime.utcnow()
            for row in trades:
                try:
                    batch.append(
                        [
                            row.get("id"),
                            row.get("wallet_address"),
                            row.get("signature"),
                            _normalize_trade_timestamp(row.get("trade_timestamp")),
                            row.get("stablecoin_amount"),
                            row.get("sol_amount"),
                            row.get("price"),
                            row.get("direction"),
                            row.get("perp_direction"),
                            now_ts,
                        ]
                    )
                except Exception:
                    continue

            if not batch:
                return 0

            conn.executemany(
                """
                INSERT OR IGNORE INTO sol_stablecoin_trades
                (id, wallet_address, signature, trade_timestamp,
                 stablecoin_amount, sol_amount, price, direction,
                 perp_direction, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            return len(batch)
    except Exception as e:
        logger.error(f"DuckDB backfill insert failed: {e}")
        return 0


def _insert_trades_into_engine(trades) -> int:
    """Insert trades into TradingDataEngine (best-effort)."""
    try:
        from core.database import get_trading_engine

        engine = get_trading_engine()
        if not engine or not getattr(engine, "_running", False):
            return 0
    except Exception:
        return 0

    inserted = 0
    now_ts = datetime.utcnow()
    for row in trades:
        try:
            trade_id = row.get("id")
            vals = [
                trade_id,
                row.get("wallet_address"),
                row.get("signature"),
                _normalize_trade_timestamp(row.get("trade_timestamp")),
                row.get("stablecoin_amount"),
                row.get("sol_amount"),
                row.get("price"),
                row.get("direction"),
                row.get("perp_direction"),
                now_ts,
            ]
            try:
                engine.execute(
                    """
                    INSERT INTO sol_stablecoin_trades
                    (id, wallet_address, signature, trade_timestamp,
                     stablecoin_amount, sol_amount, price, direction,
                     perp_direction, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    vals,
                )
                inserted += 1
            except Exception as insert_err:
                err_str = str(insert_err).lower()
                if "duplicate" in err_str or "primary key" in err_str or "constraint" in err_str:
                    engine.execute(
                        """
                        UPDATE sol_stablecoin_trades
                        SET wallet_address = ?, signature = ?, trade_timestamp = ?,
                            stablecoin_amount = ?, sol_amount = ?, price = ?,
                            direction = ?, perp_direction = ?, created_at = ?
                        WHERE id = ?
                        """,
                        [
                            vals[1],
                            vals[2],
                            vals[3],
                            vals[4],
                            vals[5],
                            vals[6],
                            vals[7],
                            vals[8],
                            vals[9],
                            trade_id,
                        ],
                    )
                    inserted += 1
                else:
                    logger.debug(f"Engine insert failed for {trade_id}: {insert_err}")
        except Exception as e:
            logger.debug(f"Engine backfill skip: {e}")
            continue

    return inserted


def run_startup_trade_backfill():
    """Fetch last 24h trades from webhook and seed DuckDB (and engine)."""
    global _last_synced_trade_id, _last_sync_initialized

    try:
        if _has_recent_trades(hours=24):
            logger.info("Startup backfill skipped: trades already within 24h window")
            return

        logger.info("Startup backfill: fetching trades from webhook (last 24h)...")
        trades = fetch_trades_last_24h_from_webhook(hours=24)
        if not trades:
            logger.error("Startup backfill: webhook returned no trades")
            return

        duckdb_inserted = _insert_trades_into_duckdb(trades)
        engine_inserted = _insert_trades_into_engine(trades)
        max_id = max((t.get("id", 0) or 0) for t in trades)

        if max_id > 0:
            _last_synced_trade_id = max_id
            _last_sync_initialized = True

        logger.info(
            f"Startup backfill complete: fetched={len(trades)}, "
            f"duckdb_inserted={duckdb_inserted}, engine_inserted={engine_inserted}, "
            f"max_id={max_id}"
        )
    except Exception as e:
        logger.error(f"Startup backfill failed: {e}")

# NOTE: sync_pattern_config_from_mysql has been removed.
# Pattern config is managed locally in DuckDB only.
# MySQL is used only for archiving expired data.


# =============================================================================
# TRADING LOGIC MOVED TO master2.py
# =============================================================================
# ALL trading computation is now handled by master2.py:
# - Price cycle analysis (create_price_cycles.py)
# - Wallet profile building
# - Trade validation (train_validator)
# - Trade following (follow_the_goat)
# - Trailing stop monitoring
#
# master.py handles ONLY raw data ingestion:
# - Jupiter price fetching
# - Trade sync from webhook
# - Order book stream from Binance
# - Data cleanup
#
# This separation allows trading logic to be restarted without stopping data feeds.
# =============================================================================


# =============================================================================
# BINANCE ORDER BOOK STREAM (runs once at startup)
# =============================================================================

def start_binance_stream_in_background(symbol: str = "SOLUSDT", mode: str = "conservative"):
    """
    Start the Binance order book WebSocket stream.
    
    This runs as a continuous WebSocket connection, not an interval job.
    Data is written to TradingDataEngine (in-memory DuckDB) with auto MySQL sync.
    """
    global _binance_collector
    
    try:
        from stream_binance_order_book_data import start_binance_stream, get_binance_collector
        
        logger.info(f"Starting Binance order book stream ({symbol}, {mode} mode)...")
        _binance_collector = start_binance_stream(symbol=symbol, mode=mode)
        
        # Track Binance stream as a special "job"
        update_job_status(
            'binance_stream',
            status='running',
            description=f'Binance Order Book Stream ({symbol})',
            is_stream=True
        )
        
        logger.info("Binance stream started successfully")
        return _binance_collector
        
    except Exception as e:
        update_job_status(
            'binance_stream',
            status='error',
            description=f'Binance Order Book Stream ({symbol})',
            error_message=str(e),
            is_stream=True
        )
        logger.error(f"Failed to start Binance stream: {e}")
        return None


def stop_binance_stream():
    """Stop the Binance order book stream."""
    global _binance_collector
    
    if _binance_collector is not None:
        try:
            from stream_binance_order_book_data import stop_binance_stream
            stop_binance_stream()
            _binance_collector = None
            
            update_job_status('binance_stream', status='stopped')
            
            logger.info("Binance stream stopped")
        except Exception as e:
            logger.error(f"Error stopping Binance stream: {e}")


# =============================================================================
# API SERVER (runs once at startup in background thread)
# =============================================================================

def start_data_api_server(host: str = "0.0.0.0", port: int = 5050):
    """
    Start the FastAPI Data Engine API server.
    
    This is a MINIMAL API that provides core data access for:
    - master2.py to sync data from the engine
    - Direct data queries via /query endpoint
    
    NOTE: The website should connect to the Flask API (port 5051) which runs separately.
    This separation ensures master.py never needs to restart for website changes.
    
    Endpoints:
    - POST /insert - Queue write to DuckDB
    - POST /query - Execute SELECT query
    - GET /backfill/{table} - Get historical data for startup
    - GET /health - Health check
    """
    global _data_api_server
    
    try:
        from core.data_api import app as data_api_app
        import uvicorn
        
        logger.info(f"Starting FastAPI Data Engine API on http://{host}:{port}")
        config = uvicorn.Config(data_api_app, host=host, port=port, log_level="warning")
        _data_api_server = uvicorn.Server(config)
        _data_api_server.run()
        
    except Exception as e:
        logger.error(f"Failed to start Data API server: {e}")


def start_data_api_in_background(host: str = "0.0.0.0", port: int = 5050):
    """Start the Data Engine API server in a background thread."""
    api_thread = threading.Thread(
        target=start_data_api_server,
        args=(host, port),
        name="FastAPI-Data-Engine",
        daemon=True
    )
    api_thread.start()
    logger.info(f"Data Engine API thread started on {host}:{port}")
    return api_thread


def start_webhook_api_server(host: str = "0.0.0.0", port: int = 8001):
    """
    Start the FastAPI webhook server (QuickNode sink) in a background thread.
    """
    global _webhook_server

    try:
        from features.webhook.app import app as webhook_app
        import uvicorn

        config = uvicorn.Config(webhook_app, host=host, port=port, log_level="info")
        _webhook_server = uvicorn.Server(config)
        _webhook_server.run()
    except Exception as e:
        logger.error(f"Failed to start webhook API server: {e}")


def start_webhook_api_in_background(host: str = "0.0.0.0", port: int = 8001):
    """Start FastAPI webhook server in background."""
    webhook_thread = threading.Thread(
        target=start_webhook_api_server,
        args=(host, port),
        name="FastAPI-Webhook-Server",
        daemon=True
    )
    webhook_thread.start()
    logger.info(f"Webhook API server thread started on {host}:{port}")
    return webhook_thread


def stop_data_api_server():
    """Stop the FastAPI Data Engine server cleanly."""
    global _data_api_server
    
    if _data_api_server is not None:
        try:
            logger.info("Stopping FastAPI Data Engine server...")
            _data_api_server.should_exit = True
            _data_api_server.force_exit = True
            _data_api_server = None
            logger.info("FastAPI Data Engine server stopped")
        except Exception as e:
            logger.error(f"Error stopping Data API server: {e}")


# =============================================================================
# PHP SERVER (runs once at startup in background)
# =============================================================================

def start_php_server(host: str = "0.0.0.0", port: int = 8000):
    """
    Start PHP's built-in development server for the website.
    
    Runs: php -S 0.0.0.0:8000 -t 000website
    """
    global _php_server_process
    import subprocess
    
    website_dir = PROJECT_ROOT / "000website"
    
    if not website_dir.exists():
        logger.warning(f"Website directory not found: {website_dir}")
        return None
    
    try:
        # Check if PHP is available
        result = subprocess.run(["php", "-v"], capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            logger.warning("PHP not found or not working - skipping PHP server")
            return None
        
        logger.info(f"Starting PHP server on http://{host}:{port}")
        logger.info(f"  Document root: {website_dir}")
        
        # Start PHP built-in server
        _php_server_process = subprocess.Popen(
            ["php", "-S", f"{host}:{port}"],
            cwd=str(website_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        )
        
        logger.info(f"PHP server started (PID: {_php_server_process.pid})")
        return _php_server_process
        
    except FileNotFoundError:
        logger.warning("PHP not installed - skipping PHP server")
        return None
    except Exception as e:
        logger.error(f"Failed to start PHP server: {e}")
        return None


def stop_php_server():
    """Stop the PHP server process."""
    global _php_server_process
    
    if _php_server_process is not None:
        try:
            logger.info("Stopping PHP server...")
            _php_server_process.terminate()
            _php_server_process.wait(timeout=5)
            _php_server_process = None
            logger.info("PHP server stopped")
        except Exception as e:
            logger.error(f"Error stopping PHP server: {e}")
            if _php_server_process:
                _php_server_process.kill()


def stop_webhook_api():
    """Request shutdown of FastAPI webhook server."""
    global _webhook_server
    if _webhook_server is not None:
        try:
            _webhook_server.should_exit = True
            _webhook_server.force_exit = True
            _webhook_server = None
            logger.info("Webhook API server stop requested")
        except Exception as e:
            logger.error(f"Error stopping webhook API server: {e}")


# =============================================================================
# SCHEDULER CREATION
# =============================================================================

def create_scheduler() -> BackgroundScheduler:
    """
    Create and configure the scheduler for DATA INGESTION only.
    
    Executors:
    - 'realtime': ThreadPoolExecutor(10) - Fast jobs (Jupiter prices, trade sync)
    - 'maintenance': ThreadPoolExecutor(2) - Hourly cleanup jobs
    
    ALL trading computation (cycles, profiles, trading logic) runs in master2.py.
    This scheduler handles ONLY raw data ingestion.
    """
    # Configure executors for parallel job execution
    executors = {
        'realtime': ThreadPoolExecutor(max_workers=10),   # Fast jobs (prices, trades)
        'maintenance': ThreadPoolExecutor(max_workers=2), # Cleanup jobs
    }
    
    # Job defaults
    job_defaults = {
        'coalesce': True,        # Combine missed runs
        'max_instances': 1,      # Prevent overlapping by default
        'misfire_grace_time': 30 # Allow 30s grace for misfires
    }
    
    scheduler = BackgroundScheduler(
        timezone=settings.scheduler_timezone,
        executors=executors,
        job_defaults=job_defaults
    )
    
    # Add error listeners for comprehensive error logging
    scheduler.add_listener(apscheduler_error_listener, EVENT_JOB_ERROR)
    scheduler.add_listener(apscheduler_missed_listener, EVENT_JOB_MISSED)
    
    # =====================================================
    # JUPITER PRICE FETCHER (runs every 1 second)
    # =====================================================
    
    # Fetch prices from Jupiter API every 1 second (bundled BTC+ETH+SOL)
    # One bundled request = 3 prices = 60 req/min (exactly at free tier limit)
    scheduler.add_job(
        func=fetch_jupiter_prices,
        trigger=IntervalTrigger(seconds=1),
        id="fetch_jupiter_prices",
        name="Fetch Jupiter prices (every 1s)",
        replace_existing=True,
        executor='realtime',
    )
    
    # =====================================================
    # DATA MAINTENANCE JOBS
    # =====================================================
    
    # Clean up old Jupiter price data (every hour)
    scheduler.add_job(
        func=cleanup_jupiter_prices,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_jupiter_prices",
        name="Clean up Jupiter DuckDB (24hr window)",
        replace_existing=True,
        executor='maintenance',
    )
    
    # Clean up DuckDB hot tables (every hour)
    # Removes data older than 24 hours from in-memory DuckDB
    scheduler.add_job(
        func=cleanup_duckdb_hot_tables,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_duckdb_hot_tables",
        name="Clean up DuckDB hot tables (24hr window)",
        replace_existing=True,
        executor='maintenance',
    )
    
    # =====================================================
    # TRADE SYNC FROM WEBHOOK (runs every 1 second)
    # =====================================================
    
    # Sync trades from Webhook DuckDB - direct DuckDB→DuckDB for speed
    scheduler.add_job(
        func=sync_trades_from_webhook,
        trigger=IntervalTrigger(seconds=1),
        id="sync_trades_from_webhook",
        name="Sync trades from Webhook (every 1s)",
        replace_existing=True,
        executor='realtime',
    )
    
    # =====================================================
    # LEGACY JOBS (for backward compatibility)
    # =====================================================
    
    # NOTE: archive_legacy_price_points job was removed.
    # Cleanup for legacy prices.duckdb is handled by cleanup_jupiter_prices
    # which correctly uses the 'ts' column via get_prices_from_jupiter.cleanup_old_data()
    
    # =====================================================
    # FEATURE JOBS - MOVED TO master2.py
    # =====================================================
    
    # NOTE: Price Cycle Analysis moved to master2.py
    # - process_price_cycles: Now runs in master2.py with synced price data
    
    # NOTE: Wallet Profile jobs moved to master2.py
    # - process_wallet_profiles: Now runs in master2.py with local in-memory DuckDB
    # - cleanup_wallet_profiles: Now runs in master2.py
    # This allows profiles to use synced data and be part of trading logic restarts.
    
    # =====================================================
    # TRADING MODULE JOBS - MOVED TO master2.py
    # =====================================================
    # The following jobs now run in master2.py (can be restarted independently):
    # - train_validator
    # - follow_the_goat
    # - trailing_stop_seller
    # - update_potential_gains
    # - create_new_patterns
    #
    # This separation allows trading logic to be updated/restarted
    # without interrupting data ingestion.
    # =====================================================
    
    return scheduler


def main():
    """Start the trading engine, API server, and scheduler."""
    global _trading_engine, _scheduler
    
    # =====================================================
    # SETUP SIGNAL HANDLERS FOR CLEAN SHUTDOWN
    # =====================================================
    def handle_shutdown(signum, frame):
        """Handle shutdown signals gracefully."""
        sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        logger.info(f"\nReceived {sig_name} signal")
        shutdown_all()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)   # Ctrl+C
    signal.signal(signal.SIGTERM, handle_shutdown)  # kill command
    
    # Windows-specific: handle Ctrl+Break
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, handle_shutdown)
    
    logger.info("=" * 60)
    logger.info("Starting Follow The Goat - Data Engine")
    logger.info("=" * 60)
    logger.info(f"Timezone: {settings.scheduler_timezone}")
    logger.info(f"Hot storage: {settings.hot_storage_hours}h (general), {settings.trades_hot_storage_hours}h (trades)")
    logger.info(f"PostgreSQL Archive: {settings.postgres.host}/{settings.postgres.database}")
    logger.info(f"DuckDB Central: {settings.central_db_path}")
    logger.info(f"Error Log: {ERROR_LOG_FILE}")
    logger.info("")
    logger.info("NOTE: Trading jobs run in master2.py (can restart independently)")
    
    # Record scheduler start time
    set_scheduler_start_time()
    
    # =====================================================
    # STEP 1: Start the TradingDataEngine (in-memory DuckDB)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 1: Starting TradingDataEngine (in-memory DuckDB)...")
    _trading_engine = start_trading_engine()
    
    # Register shutdown handler for unexpected exits
    atexit.register(shutdown_all)
    
    # Log engine stats
    stats = _trading_engine.get_stats()
    logger.info(f"Engine started - Tables loaded: {stats['table_counts']}")
    
    # Track trading engine status
    update_job_status(
        'trading_engine',
        status='running',
        description='TradingDataEngine (in-memory DuckDB)',
        is_service=True
    )
    
    # =====================================================
    # STEP 2: Start the FastAPI Data Engine (background thread)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2: Starting FastAPI Data Engine API (port 5050)...")
    # Listen on all interfaces so master2.py can connect
    api_thread = start_data_api_in_background(host="0.0.0.0", port=5050)
    
    # Track API server status
    update_job_status(
        'data_api_server',
        status='running',
        description='FastAPI Data Engine API (port 5050)',
        is_service=True
    )
    
    # Give the API server a moment to start
    import time
    time.sleep(1)

    # =====================================================
    # STEP 2b: Start FastAPI Webhook Server (QuickNode sink)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2b: Starting FastAPI Webhook Server (port 8001)...")
    start_webhook_api_in_background(host="0.0.0.0", port=8001)
    
    update_job_status(
        'webhook_server',
        status='running',
        description='FastAPI Webhook Server (port 8001)',
        is_service=True
    )
    
    # Give the webhook server a moment to start
    time.sleep(1)
    
    # =====================================================
    # STEP 2c: Start PHP Built-in Server (website)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2c: Starting PHP Built-in Server (port 8000)...")
    php_proc = start_php_server(host="0.0.0.0", port=8000)
    
    if php_proc:
        update_job_status(
            'php_server',
            status='running',
            description='PHP Built-in Server (port 8000)',
            is_service=True
        )
    else:
        update_job_status(
            'php_server',
            status='skipped',
            description='PHP Server (not installed or failed)',
            is_service=True
        )
    
    # =====================================================
    # STEP 3: Start the Binance Order Book Stream
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 3: Starting Binance Order Book Stream...")
    binance_collector = start_binance_stream_in_background(symbol="SOLUSDT", mode="conservative")
    
    # Give the stream a moment to connect
    time.sleep(2)
    
    # =====================================================
    # STEP 4: Initialize DuckDB tables and sync config from MySQL
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 4: Initializing DuckDB tables and syncing config...")
    
    # FIRST: Initialize ALL file-based DuckDB tables BEFORE any syncs
    # This ensures follow_the_goat_tracking, follow_the_goat_buyins, etc. exist with correct schema
    try:
        from core.database import init_duckdb_tables
        init_duckdb_tables("central")
        logger.info("All file-based DuckDB tables initialized (including migrations)")
    except Exception as e:
        logger.error(f"Failed to initialize DuckDB tables: {e}")
    
    # Initialize pattern config tables before sync
    try:
        from features.price_api.schema import SCHEMA_PATTERN_CONFIG_PROJECTS, SCHEMA_PATTERN_CONFIG_FILTERS
        from core.database import get_duckdb
        with get_duckdb("central") as conn:
            # Drop existing pattern_config_filters to ensure schema is up-to-date
            # (new columns like exclude_mode need to be added)
            conn.execute("DROP TABLE IF EXISTS pattern_config_filters")
            conn.execute(SCHEMA_PATTERN_CONFIG_PROJECTS)
            conn.execute(SCHEMA_PATTERN_CONFIG_FILTERS)
        logger.info("Pattern config tables initialized (recreated)")
    except Exception as e:
        logger.error(f"Failed to initialize pattern config tables: {e}")
    
    # Backfill last 24h of trades from webhook so profiles have data immediately
    try:
        run_startup_trade_backfill()
    except Exception as e:
        logger.error(f"Failed to run startup trade backfill: {e}")

    # NOTE: TradingDataEngine handles in-memory tables for prices, orderbook, etc.
    # File-based DuckDB tables are initialized above via init_duckdb_tables()
    logger.info("Tables initialized (file-based + TradingDataEngine in-memory)")
    
    # MySQL sync skipped (DuckDB-only mode; plays/config must be pre-cached)
    
    # Sync trades (critical for follow_the_goat DuckDB-only operation)
    # Uses fast Webhook DuckDB→DuckDB path, falls back to MySQL
    try:
        sync_trades_from_webhook()
        logger.info("Trades sync skipped (push-based FastAPI webhook)")
    except Exception as e:
        logger.error(f"Failed to sync trades on startup: {e}")
    
    # =====================================================
    # STEP 5: Create and start the scheduler
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 5: Starting Scheduler...")
    _scheduler = create_scheduler()
    
    # Log executor configuration
    logger.info("Executors configured for parallel job execution:")
    logger.info("  - realtime (10 threads): Jupiter prices, trade sync")
    logger.info("  - maintenance (2 threads): Hourly cleanup jobs")
    logger.info("")
    logger.info("Trading jobs run in master2.py (can restart independently)")
    
    # Log all registered jobs grouped by executor
    jobs = _scheduler.get_jobs()
    logger.info(f"Registered {len(jobs)} jobs:")
    for job in sorted(jobs, key=lambda j: (j.executor or 'default', j.id)):
        executor = job.executor or 'default'
        logger.info(f"  [{executor}] {job.id}: {job.name}")
    
    logger.info("-" * 60)
    logger.info("All systems running! Press Ctrl+C to stop cleanly.")
    logger.info("=" * 60)
    
    # Start scheduler in background
    _scheduler.start()
    
    # Keep main thread alive (BackgroundScheduler runs in background thread)
    # This allows better signal handling on Windows
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        shutdown_all()


def shutdown_all():
    """Gracefully shutdown all services in the correct order."""
    global _trading_engine, _binance_collector, _scheduler
    
    logger.info("=" * 60)
    logger.info("SHUTTING DOWN - Please wait...")
    logger.info("=" * 60)
    
    # 1. Stop the scheduler first (stops new jobs from running)
    if _scheduler is not None:
        try:
            logger.info("[1/7] Stopping scheduler...")
            _scheduler.shutdown(wait=False)
            _scheduler = None
            logger.info("      Scheduler stopped")
        except Exception as e:
            logger.error(f"      Error stopping scheduler: {e}")
    
    # 2. Stop metrics writer
    logger.info("[2/7] Stopping metrics writer...")
    stop_metrics_writer()
    
    # 3. Stop the Data API server
    logger.info("[3/7] Stopping FastAPI Data Engine server...")
    stop_data_api_server()

    # 4. Stop webhook server
    logger.info("[4/7] Stopping FastAPI webhook server...")
    stop_webhook_api()
    
    # 5. Stop PHP server
    logger.info("[5/7] Stopping PHP server...")
    stop_php_server()
    
    # 6. Stop Binance stream
    if _binance_collector is not None:
        logger.info("[6/7] Stopping Binance stream...")
        stop_binance_stream()
    else:
        logger.info("[6/7] Binance stream not running")
    
    # 7. Stop the trading engine
    if _trading_engine is not None:
        logger.info("[7/7] Stopping TradingDataEngine...")
        stop_trading_engine()
        _trading_engine = None
        logger.info("      TradingDataEngine stopped")
    else:
        logger.info("[7/7] TradingDataEngine not running")
    
    logger.info("Note: Plays are preserved in config/plays_cache.json")
    
    logger.info("=" * 60)
    logger.info("SHUTDOWN COMPLETE - Goodbye!")
    logger.info("=" * 60)


# Keep old name for backward compatibility
def shutdown_engine():
    """Alias for shutdown_all()."""
    shutdown_all()


if __name__ == "__main__":
    main()

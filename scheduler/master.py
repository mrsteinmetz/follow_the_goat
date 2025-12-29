"""
Master Scheduler - APScheduler
==============================
Single entry point for all scheduled tasks.
NO .bat files - everything runs through here.

Usage:
    python scheduler/master.py

This script:
1. Starts the TradingDataEngine (in-memory DuckDB with zero locks)
2. Starts the Flask API server in a background thread
3. Starts all scheduled jobs via APScheduler

Shutdown:
    Press Ctrl+C to gracefully stop all services.
"""

import sys
import os
import signal
import threading
import atexit
from pathlib import Path
from datetime import datetime

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

# Global reference to Flask server (for clean shutdown)
_flask_server = None

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
    Clean up all DuckDB hot tables.
    - Trades: 72 hours retention (settings.trades_hot_storage_hours)
    - Other tables: 24 hours retention (settings.hot_storage_hours)
    """
    logger.info(f"Running DuckDB hot table cleanup (trades: {settings.trades_hot_storage_hours}h, others: {settings.hot_storage_hours}h)...")
    total_cleaned = cleanup_all_hot_tables("central")  # Uses per-table settings
    logger.info(f"Cleanup complete: {total_cleaned} records removed")


# NOTE: archive_legacy_price_points was removed because:
# 1. The legacy prices.duckdb uses 'ts' column, not 'created_at'
# 2. cleanup_jupiter_prices already handles cleanup correctly via cleanup_old_data()
# 3. See get_prices_from_jupiter.py for the correct cleanup implementation


@track_job("sync_plays_from_mysql", "Sync plays from MySQL (every 5 min)")
def sync_plays_from_mysql():
    """Sync plays table from MySQL to DuckDB (master data refresh)."""
    logger.info("Syncing plays from MySQL...")
    from features.price_api.sync_from_mysql import sync_table
    synced = sync_table("follow_the_goat_plays", full_sync=True)
    logger.info(f"Plays sync complete: {synced} records")


# Global state for incremental trade sync (tracks last synced ID)
_last_synced_trade_id = 0
_last_sync_initialized = False


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
    """Sync new trades from .NET Webhook's DuckDB using incremental sync.
    
    CRITICAL FIX: Uses SYNCHRONOUS writes directly to DuckDB, bypassing the queue.
    The queue-based writes were getting backed up with 200k+ items, causing trades
    to never appear in reads (stuck behind order book data).
    
    - Tracks last_id and only fetches WHERE id > last_id (no duplicate fetching)
    - Direct INSERT OR REPLACE for immediate availability
    - Typically fetches 0-10 new trades per call
    
    Falls back to MySQL if webhook is unavailable.
    """
    global _last_synced_trade_id
    import requests
    from core.database import get_trading_engine
    from datetime import datetime
    
    WEBHOOK_URL = "http://quicknode.smz.dk/api/trades"
    
    try:
        # Get last synced ID (initializes from engine on first call)
        last_id = _get_last_synced_trade_id()
        
        # Fetch only NEW trades (id > last_id) - FAST incremental sync
        response = requests.get(
            WEBHOOK_URL,
            params={'after_id': last_id, 'limit': 1000},
            timeout=3
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                rows = data.get('results', [])
                webhook_max_id = data.get('max_id', 0)
                
                if not rows:
                    # No new trades - nothing to do
                    return
                
                logger.debug(f"Webhook returned {len(rows)} trades (max_id={webhook_max_id}, after_id={last_id})")
                
                # Write DIRECTLY to TradingDataEngine (bypass queue for instant availability)
                engine = get_trading_engine()
                if not engine or not engine._running:
                    logger.error("TradingDataEngine not running! Cannot sync trades.")
                    return
                
                max_id = last_id
                count = 0
                
                # Prepare batch data for bulk insert
                batch_data = []
                for row in rows:
                    try:
                        trade_id = row.get('id', 0)
                        if trade_id > max_id:
                            max_id = trade_id
                        
                        ts = row.get('trade_timestamp', '')
                        if isinstance(ts, str) and ts:
                            # Parse timestamp string
                            ts = ts.replace('Z', '').replace('T', ' ')[:19]
                            try:
                                ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                            except:
                                ts = datetime.now()
                        
                        batch_data.append((
                            trade_id,
                            row.get('wallet_address'),
                            row.get('signature'),
                            ts,
                            row.get('stablecoin_amount'),
                            row.get('sol_amount'),
                            row.get('price'),
                            row.get('direction'),
                            row.get('perp_direction'),
                            datetime.now()
                        ))
                        count += 1
                    except Exception as e:
                        logger.debug(f"Trade prep skip: {e}")
                
                # SYNCHRONOUS inserts - bypasses queue for immediate availability
                # Each insert is done directly to ensure trades appear instantly in reads
                inserted_count = 0
                failed_count = 0
                actual_max_id = last_id  # Only update to IDs we actually inserted
                
                if batch_data:
                    for data in batch_data:
                        trade_id = data[0]
                        try:
                            # Try INSERT first (most common case for new trades)
                            engine.execute("""
                                INSERT INTO sol_stablecoin_trades 
                                (id, wallet_address, signature, trade_timestamp,
                                 stablecoin_amount, sol_amount, price, direction, 
                                 perp_direction, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, list(data))
                            inserted_count += 1
                            if trade_id > actual_max_id:
                                actual_max_id = trade_id
                        except Exception as insert_err:
                            err_str = str(insert_err).lower()
                            # If duplicate key, try UPDATE instead
                            if 'duplicate' in err_str or 'primary key' in err_str or 'constraint' in err_str:
                                try:
                                    engine.execute("""
                                        UPDATE sol_stablecoin_trades 
                                        SET wallet_address = ?, signature = ?, trade_timestamp = ?,
                                            stablecoin_amount = ?, sol_amount = ?, price = ?,
                                            direction = ?, perp_direction = ?, created_at = ?
                                        WHERE id = ?
                                    """, [data[1], data[2], data[3], data[4], data[5], 
                                          data[6], data[7], data[8], data[9], data[0]])
                                    inserted_count += 1
                                    if trade_id > actual_max_id:
                                        actual_max_id = trade_id
                                except Exception as update_err:
                                    failed_count += 1
                                    logger.warning(f"Trade {trade_id} failed INSERT and UPDATE: {update_err}")
                            else:
                                failed_count += 1
                                logger.error(f"Trade {trade_id} INSERT failed: {insert_err}")
                
                # Only update last synced ID if we actually inserted some trades
                if inserted_count > 0:
                    _last_synced_trade_id = actual_max_id
                    logger.info(f"Synced {inserted_count} trades DIRECTLY to engine (id {last_id} -> {actual_max_id})")
                elif failed_count > 0:
                    logger.error(f"All {failed_count} trade inserts failed! Not updating last_synced_id")
                
                return
        
        # Fallback to MySQL if webhook is unavailable
        logger.warning(f"Webhook unavailable (HTTP {response.status_code}), falling back to MySQL")
        _sync_trades_from_mysql_fallback()
        
    except requests.exceptions.RequestException as e:
        # Webhook connection failed, fallback to MySQL
        logger.warning(f"Webhook connection failed: {e}, falling back to MySQL")
        _sync_trades_from_mysql_fallback()
    except Exception as e:
        logger.error(f"Trade sync error: {e}")


def _sync_trades_from_mysql_fallback():
    """Fallback: Sync from MySQL if webhook is unavailable.
    Uses incremental sync based on last_id for efficiency.
    """
    global _last_synced_trade_id
    from core.database import get_duckdb, get_mysql
    from features.price_api.schema import SCHEMA_SOL_STABLECOIN_TRADES
    
    try:
        last_id = _get_last_synced_trade_id()
        
        with get_mysql() as mysql_conn:
            with mysql_conn.cursor() as cursor:
                # Incremental sync: only fetch records with id > last_id
                cursor.execute("""
                    SELECT id, wallet_address, signature, trade_timestamp,
                           stablecoin_amount, sol_amount, price, direction, perp_direction
                    FROM sol_stablecoin_trades
                    WHERE id > %s
                    ORDER BY id ASC
                    LIMIT 1000
                """, [last_id])
                rows = cursor.fetchall()
        
        if not rows:
            return
        
        # Prepare batch data (outside DB connection)
        batch_data = []
        max_id = last_id
        for row in rows:
            try:
                trade_id = row['id']
                if trade_id > max_id:
                    max_id = trade_id
                
                ts = row['trade_timestamp']
                if hasattr(ts, 'strftime'):
                    ts = ts.strftime('%Y-%m-%d %H:%M:%S')
                
                batch_data.append([
                    trade_id, row['wallet_address'], row.get('signature'),
                    ts, row.get('stablecoin_amount'), row.get('sol_amount'),
                    row.get('price'), row.get('direction'), row.get('perp_direction')
                ])
            except Exception:
                pass
        
        if not batch_data:
            return
        
        # Batch insert into DuckDB (INSERT OR IGNORE since these are new records)
        with get_duckdb("central") as conn:
            conn.execute(SCHEMA_SOL_STABLECOIN_TRADES)
            conn.executemany("""
                INSERT OR IGNORE INTO sol_stablecoin_trades
                (id, wallet_address, signature, trade_timestamp,
                 stablecoin_amount, sol_amount, price, direction, perp_direction)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
        
        # Update last synced ID
        _last_synced_trade_id = max_id
        
        logger.debug(f"Synced {len(batch_data)} trades from MySQL fallback (id {last_id} -> {max_id})")
            
    except Exception as e:
        logger.error(f"MySQL fallback sync error: {e}")


@track_job("sync_pattern_config_from_mysql", "Sync pattern config from MySQL (every 5 min)")
def sync_pattern_config_from_mysql():
    """Sync pattern config tables from MySQL to DuckDB (full data, not time-based)."""
    logger.info("Syncing pattern config from MySQL...")
    from features.price_api.sync_from_mysql import sync_table
    
    # Sync projects
    synced_projects = sync_table("pattern_config_projects", full_sync=True)
    
    # Sync filters  
    synced_filters = sync_table("pattern_config_filters", full_sync=True)
    
    logger.info(f"Pattern config sync complete: {synced_projects} projects, {synced_filters} filters")


@track_job("process_price_cycles", "Process price cycles (every 15s)")
def process_price_cycles():
    """Process price points into price cycle analysis (dual-write to DuckDB + MySQL)."""
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "000data_feeds" / "2_create_price_cycles"))
    from create_price_cycles import process_price_cycles as run_price_cycles
    
    processed = run_price_cycles()
    if processed > 0:
        logger.debug(f"Price cycles: processed {processed} price points")


@track_job("process_wallet_profiles", "Build wallet profiles (every 10s)")
def process_wallet_profiles():
    """Build wallet profiles from trades and price cycles (dual-write to DuckDB + MySQL)."""
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "000data_feeds" / "5_create_profiles"))
    from create_profiles import process_wallet_profiles as run_profiles
    
    processed = run_profiles()
    if processed > 0:
        logger.debug(f"Wallet profiles: processed {processed} profiles")


@track_job("cleanup_wallet_profiles", "Clean up wallet profiles (every hour)")
def cleanup_wallet_profiles():
    """Clean up old wallet profiles from BOTH DuckDB and MySQL (24hr retention)."""
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "000data_feeds" / "5_create_profiles"))
    from create_profiles import cleanup_old_profiles
    
    deleted = cleanup_old_profiles(hours=24)
    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old wallet profiles")


@track_job("train_validator", "Validator training cycle (every 30s)")
def run_train_validator():
    """Run a single validator training cycle.
    
    Creates a synthetic trade, generates a 15-minute trail, runs pattern 
    validation, and updates the trade record with the validation result.
    
    Set TRAIN_VALIDATOR_ENABLED=0 in .env to disable.
    """
    import os
    enabled = os.getenv("TRAIN_VALIDATOR_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Train validator disabled via TRAIN_VALIDATOR_ENABLED=0")
        return
    
    # Add 000trading to path and import directly
    import sys
    from pathlib import Path
    trading_path = Path(__file__).parent.parent / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from train_validator import run_training_cycle
    success = run_training_cycle()
    if not success:
        logger.warning("Train validator cycle failed")


@track_job("follow_the_goat", "Wallet tracker cycle (every 1s)")
def run_follow_the_goat():
    """Run a single wallet tracking cycle.
    
    Monitors target wallets for new buy transactions, generates 15-minute
    trails, runs pattern validation, and creates buy-in records.
    
    Set FOLLOW_THE_GOAT_ENABLED=0 in .env to disable.
    """
    import os
    enabled = os.getenv("FOLLOW_THE_GOAT_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Follow the goat disabled via FOLLOW_THE_GOAT_ENABLED=0")
        return
    
    # Add 000trading to path and import directly
    import sys
    from pathlib import Path
    trading_path = Path(__file__).parent.parent / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from follow_the_goat import run_single_cycle
    
    trades_found = run_single_cycle()
    if trades_found:
        logger.debug("Follow the goat: new trades processed")


@track_job("update_potential_gains", "Update potential gains (every 15s)")
def run_update_potential_gains():
    """Update potential_gains for buyins with completed price cycles.
    
    Calculates: ((highest_price_reached - our_entry_price) / our_entry_price) * 100
    Only updates records where cycle_end_time IS NOT NULL (completed cycles).
    Uses threshold = 0.3 for cycle_tracker lookup.
    
    Set UPDATE_POTENTIAL_GAINS_ENABLED=0 in .env to disable.
    """
    import os
    enabled = os.getenv("UPDATE_POTENTIAL_GAINS_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Update potential gains disabled via UPDATE_POTENTIAL_GAINS_ENABLED=0")
        return
    
    # Add data feeds path and import
    import sys
    from pathlib import Path
    data_feeds_path = Path(__file__).parent.parent / "000data_feeds" / "6_update_potential_gains"
    if str(data_feeds_path) not in sys.path:
        sys.path.insert(0, str(data_feeds_path))
    from update_potential_gains import run as update_gains
    
    result = update_gains()
    if result.get('updated', 0) > 0:
        logger.debug(f"Potential gains: updated {result['updated']} records")


@track_job("trailing_stop_seller", "Trailing stop seller (every 1s)")
def run_trailing_stop_seller():
    """Run a single trailing stop monitoring cycle.
    
    Monitors open positions for trailing stop conditions, tracks highest
    prices, and marks positions as 'sold' when tolerance is exceeded.
    
    Set TRAILING_STOP_ENABLED=0 in .env to disable.
    """
    import os
    enabled = os.getenv("TRAILING_STOP_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Trailing stop seller disabled via TRAILING_STOP_ENABLED=0")
        return
    
    # Add 000trading to path and import directly
    import sys
    from pathlib import Path
    trading_path = Path(__file__).parent.parent / "000trading"
    if str(trading_path) not in sys.path:
        sys.path.insert(0, str(trading_path))
    from sell_trailing_stop import run_single_cycle
    
    positions_checked = run_single_cycle()
    if positions_checked > 0:
        logger.debug(f"Trailing stop: checked {positions_checked} position(s)")


@track_job("create_new_patterns", "Auto-generate filter patterns (every 15 min)")
def run_create_new_patterns():
    """Auto-generate filter patterns from trade data analysis.
    
    Analyzes the last 24 hours of trade data to find optimal filter combinations
    that maximize bad trade removal while preserving good trades.
    
    This job:
    1. Loads trade data from in-memory DuckDB (follow_the_goat_buyins + buyin_trail_minutes)
    2. Analyzes each filter field to find optimal ranges
    3. Generates filter combinations using a greedy algorithm
    4. Syncs best filters to pattern_config_filters
    5. Updates plays with pattern_update_by_ai=1
    
    Migrated from: 000old_code/solana_node/chart/build_pattern_config/auto_filter_scheduler.py
    
    Set CREATE_NEW_PATTERNS_ENABLED=0 in .env to disable.
    """
    import os
    enabled = os.getenv("CREATE_NEW_PATTERNS_ENABLED", "1") == "1"
    if not enabled:
        logger.debug("Create new patterns disabled via CREATE_NEW_PATTERNS_ENABLED=0")
        return
    
    # Add data feeds path and import
    import sys
    from pathlib import Path
    patterns_path = Path(__file__).parent.parent / "000data_feeds" / "7_create_new_patterns"
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

def start_api_server(host: str = "127.0.0.1", port: int = 5050):
    """
    Start the DuckDB API server in a background thread.
    This allows the entire project to be started with a single command.
    
    Note: Table initialization is skipped because TradingDataEngine handles
    all data in-memory with zero lock contention.
    """
    global _flask_server
    
    try:
        # Disable Flask's auto-loading of .env files (avoids encoding issues)
        os.environ['FLASK_SKIP_DOTENV'] = '1'
        
        # Suppress Flask/Werkzeug request logs (too noisy)
        werkzeug_logger = logging.getLogger('werkzeug')
        werkzeug_logger.setLevel(logging.WARNING)
        
        from features.price_api.api import app
        from werkzeug.serving import make_server
        
        # Note: We skip init_duckdb_tables() because TradingDataEngine handles
        # all tables in-memory. The API reads from the engine, not file-based DuckDB.
        logger.info("Skipping file-based DuckDB init (using TradingDataEngine)")
        
        # Create server that can be shut down cleanly
        logger.info(f"Starting DuckDB API server on http://{host}:{port}")
        _flask_server = make_server(host, port, app, threaded=True)
        _flask_server.serve_forever()
        
    except Exception as e:
        logger.error(f"Failed to start API server: {e}")


def start_api_in_background(host: str = "127.0.0.1", port: int = 5050):
    """Start the API server in a background thread."""
    api_thread = threading.Thread(
        target=start_api_server,
        args=(host, port),
        name="DuckDB-API-Server",
        daemon=True
    )
    api_thread.start()
    logger.info(f"API server thread started")
    return api_thread


def stop_api_server():
    """Stop the Flask API server cleanly."""
    global _flask_server
    
    if _flask_server is not None:
        try:
            logger.info("Stopping Flask API server...")
            _flask_server.shutdown()
            _flask_server = None
            logger.info("Flask API server stopped")
        except Exception as e:
            logger.error(f"Error stopping API server: {e}")


# =============================================================================
# SCHEDULER CREATION
# =============================================================================

def create_scheduler() -> BackgroundScheduler:
    """
    Create and configure the scheduler with multiple executors for parallelism.
    
    Executors:
    - 'realtime': ThreadPoolExecutor(10) - Fast jobs that must run on schedule
                  (Jupiter prices, trailing stop, follow the goat, trade sync)
    - 'heavy': ThreadPoolExecutor(4) - Slower jobs that can take longer
               (wallet profiles, price cycles, validator training)
    - 'maintenance': ThreadPoolExecutor(2) - Hourly cleanup jobs
    
    This prevents slow jobs from blocking fast real-time jobs.
    """
    # Configure executors for parallel job execution
    executors = {
        'realtime': ThreadPoolExecutor(max_workers=10),   # Fast jobs (prices, trading)
        'heavy': ThreadPoolExecutor(max_workers=4),       # Slow jobs (profiles, cycles)
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
    # JUPITER PRICE FETCHER (runs every 1 second) - REALTIME EXECUTOR
    # =====================================================
    
    # Fetch prices from Jupiter API every 1 second (bundled BTC+ETH+SOL in single call)
    # Using v3 API: https://api.jup.ag/price/v3?ids=SOL,BTC,ETH
    # One bundled request = 3 prices = 60 req/min (exactly at free tier limit)
    # Uses 'realtime' executor to avoid blocking by slow jobs
    scheduler.add_job(
        func=fetch_jupiter_prices,
        trigger=IntervalTrigger(seconds=1),
        id="fetch_jupiter_prices",
        name="Fetch Jupiter prices (every 1s)",
        replace_existing=True,
        executor='realtime',  # Dedicated fast executor
    )
    
    # Clean up old Jupiter price data (every hour)
    scheduler.add_job(
        func=cleanup_jupiter_prices,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_jupiter_prices",
        name="Clean up Jupiter DuckDB (24hr window)",
        replace_existing=True,
        executor='maintenance',  # Cleanup executor
    )
    
    # =====================================================
    # DUCKDB MAINTENANCE JOBS - MAINTENANCE EXECUTOR
    # =====================================================
    
    # Clean up DuckDB hot tables (every hour)
    # Removes data older than 24 hours from DuckDB
    # Data is preserved in MySQL (master storage)
    scheduler.add_job(
        func=cleanup_duckdb_hot_tables,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_duckdb_hot_tables",
        name="Clean up central DuckDB hot tables (24hr window)",
        replace_existing=True,
        executor='maintenance',  # Cleanup executor
    )
    
    # Sync plays from MySQL (every 5 minutes)
    # Keeps DuckDB plays table in sync with MySQL master
    scheduler.add_job(
        func=sync_plays_from_mysql,
        trigger=IntervalTrigger(minutes=5),
        id="sync_plays_from_mysql",
        name="Sync plays table from MySQL to DuckDB",
        replace_existing=True,
        executor='maintenance',  # Cleanup executor
    )
    
    # Sync trades from Webhook DuckDB (every 0.5 seconds) - ULTRA FAST PATH
    # Direct DuckDB→DuckDB sync, bypassing MySQL for real-time trading
    # Uses 'realtime' executor - critical for trading detection
    scheduler.add_job(
        func=sync_trades_from_webhook,
        trigger=IntervalTrigger(seconds=1),
        id="sync_trades_from_webhook",
        name="Sync trades from Webhook DuckDB (every 1s)",
        replace_existing=True,
        executor='realtime',  # Fast executor - trading critical
    )
    
    # Sync pattern config from MySQL (every 5 minutes)
    # Keeps DuckDB pattern_config_projects and pattern_config_filters in sync
    scheduler.add_job(
        func=sync_pattern_config_from_mysql,
        trigger=IntervalTrigger(minutes=5),
        id="sync_pattern_config_from_mysql",
        name="Sync pattern config tables from MySQL to DuckDB",
        replace_existing=True,
        executor='maintenance',  # Cleanup executor
    )
    
    # =====================================================
    # LEGACY JOBS (for backward compatibility)
    # =====================================================
    
    # NOTE: archive_legacy_price_points job was removed.
    # Cleanup for legacy prices.duckdb is handled by cleanup_jupiter_prices
    # which correctly uses the 'ts' column via get_prices_from_jupiter.cleanup_old_data()
    
    # =====================================================
    # FEATURE JOBS - HEAVY EXECUTOR (can be slow, won't block realtime)
    # =====================================================
    
    # Price Cycle Analysis (runs every 5 seconds)
    # Processes price data into cycles at 5 thresholds (0.1-0.5%)
    # Dual-writes to DuckDB (24hr hot) + MySQL (full history)
    scheduler.add_job(
        func=process_price_cycles,
        trigger=IntervalTrigger(seconds=15),  # Increased from 5s - job takes ~11s
        id="process_price_cycles",
        name="Process price cycles (every 15s)",
        replace_existing=True,
        executor='heavy',  # Slow job - separate from realtime
    )
    
    # Wallet Profile Builder (runs every 5 seconds)
    # Builds profiles from trades + completed cycles at all thresholds
    # Dual-writes to DuckDB (24hr hot) + MySQL (also 24hr - special case)
    # WARNING: This job is very slow (97s avg) - runs in heavy executor
    scheduler.add_job(
        func=process_wallet_profiles,
        trigger=IntervalTrigger(seconds=10),  # Increased from 5s - job can take 8s+
        id="process_wallet_profiles",
        name="Build wallet profiles (every 10s)",
        replace_existing=True,
        executor='heavy',  # VERY slow job - must not block realtime
    )
    
    # Wallet Profile Cleanup (every hour)
    # Cleans up profiles older than 24 hours from BOTH DuckDB and MySQL
    # This is different from standard architecture - both databases only keep 24 hours
    scheduler.add_job(
        func=cleanup_wallet_profiles,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_wallet_profiles",
        name="Clean up wallet profiles (24hr window in both DuckDB and MySQL)",
        replace_existing=True,
        executor='maintenance',  # Cleanup executor
    )
    
    # =====================================================
    # TRADING MODULE JOBS
    # =====================================================
    
    # Validator Training (runs every 15 seconds)
    # Creates synthetic trades and validates them against pattern schemas
    # CRITICAL: Generates training data for AI pattern learning
    # Enabled by default - set TRAIN_VALIDATOR_ENABLED=0 to disable
    scheduler.add_job(
        func=run_train_validator,
        trigger=IntervalTrigger(seconds=15),
        id="train_validator",
        name="Validator training cycle (every 15s)",
        replace_existing=True,
        executor='heavy',  # Can be slow - separate from realtime
    )
    
    # Follow The Goat - Wallet Tracker (runs every 0.5 seconds)
    # Monitors target wallets for new buy transactions, generates trails,
    # runs pattern validation, and creates buy-in records.
    # CRITICAL: Must run every 0.5s for fastest trade detection
    # Enabled by default - set FOLLOW_THE_GOAT_ENABLED=0 to disable
    scheduler.add_job(
        func=run_follow_the_goat,
        trigger=IntervalTrigger(seconds=1),
        id="follow_the_goat",
        name="Wallet tracker cycle (every 1s)",
        replace_existing=True,
        executor='realtime',  # CRITICAL - fast trading detection
    )
    
    # Trailing Stop Seller (runs every 0.5 seconds)
    # Monitors open positions for trailing stop conditions, tracks highest
    # prices, and marks positions as 'sold' when tolerance is exceeded.
    # Uses price_points (coin_id=5) for SOL prices updated every 1s.
    # Enabled by default - set TRAILING_STOP_ENABLED=0 to disable
    scheduler.add_job(
        func=run_trailing_stop_seller,
        trigger=IntervalTrigger(seconds=1),
        id="trailing_stop_seller",
        name="Trailing stop seller (every 1s)",
        replace_existing=True,
        executor='realtime',  # CRITICAL - must sell on time
    )
    
    # Update Potential Gains (runs every 15 seconds)
    # Calculates potential_gains for buyins where price cycle has completed.
    # Formula: ((highest_price_reached - our_entry_price) / our_entry_price) * 100
    # Uses threshold = 0.3 for cycle_tracker lookup.
    # Enabled by default - set UPDATE_POTENTIAL_GAINS_ENABLED=0 to disable
    scheduler.add_job(
        func=run_update_potential_gains,
        trigger=IntervalTrigger(seconds=15),
        id="update_potential_gains",
        name="Update potential gains (every 15s)",
        replace_existing=True,
        executor='heavy',  # Can be slow - separate from realtime
    )
    
    # Create New Patterns (runs every 15 minutes)
    # Auto-generates filter patterns from trade data analysis.
    # Analyzes last 24h of trades to find optimal filter combinations.
    # Syncs best filters to pattern_config_filters and updates AI-enabled plays.
    # Migrated from: 000old_code/solana_node/chart/build_pattern_config/auto_filter_scheduler.py
    # Enabled by default - set CREATE_NEW_PATTERNS_ENABLED=0 to disable
    scheduler.add_job(
        func=run_create_new_patterns,
        trigger=IntervalTrigger(minutes=15),
        id="create_new_patterns",
        name="Auto-generate filter patterns (every 15 min)",
        replace_existing=True,
        executor='heavy',  # Analysis is slow - runs in heavy executor
    )
    
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
    logger.info("Starting Follow The Goat - Master Controller")
    logger.info("=" * 60)
    logger.info(f"Timezone: {settings.scheduler_timezone}")
    logger.info(f"Hot storage: {settings.hot_storage_hours}h (general), {settings.trades_hot_storage_hours}h (trades)")
    logger.info(f"MySQL: {settings.mysql.host}/{settings.mysql.database}")
    logger.info(f"DuckDB Central: {settings.central_db_path}")
    logger.info(f"Error Log: {ERROR_LOG_FILE}")
    
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
    # STEP 2: Start the DuckDB API server (background thread)
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 2: Starting DuckDB API Server...")
    # Listen on all interfaces so Windows/PHP can connect to WSL Flask API
    api_thread = start_api_in_background(host="0.0.0.0", port=5050)
    
    # Track API server status
    update_job_status(
        'api_server',
        status='running',
        description='DuckDB API Server (port 5050)',
        is_service=True
    )
    
    # Give the API server a moment to start
    import time
    time.sleep(1)
    
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
    
    # NOTE: TradingDataEngine handles in-memory tables for prices, orderbook, etc.
    # File-based DuckDB tables are initialized above via init_duckdb_tables()
    logger.info("Tables initialized (file-based + TradingDataEngine in-memory)")
    
    # NOW sync data from MySQL (tables are guaranteed to exist)
    # Sync plays
    try:
        sync_plays_from_mysql()
        logger.info("Plays sync complete")
    except Exception as e:
        logger.error(f"Failed to sync plays on startup: {e}")
    
    # Sync pattern config
    try:
        sync_pattern_config_from_mysql()
        logger.info("Pattern config sync complete")
    except Exception as e:
        logger.error(f"Failed to sync pattern config on startup: {e}")
    
    # Sync trades (critical for follow_the_goat DuckDB-only operation)
    # Uses fast Webhook DuckDB→DuckDB path, falls back to MySQL
    try:
        sync_trades_from_webhook()
        logger.info("Trades sync complete (DuckDB→DuckDB fast path)")
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
    logger.info("  - realtime (10 threads): Jupiter prices, trailing stop, follow_the_goat, trade sync")
    logger.info("  - heavy (4 threads): Wallet profiles, price cycles, validator training")
    logger.info("  - maintenance (2 threads): Hourly cleanup jobs")
    
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
            logger.info("[1/5] Stopping scheduler...")
            _scheduler.shutdown(wait=False)
            _scheduler = None
            logger.info("      Scheduler stopped")
        except Exception as e:
            logger.error(f"      Error stopping scheduler: {e}")
    
    # 2. Stop metrics writer
    logger.info("[2/5] Stopping metrics writer...")
    stop_metrics_writer()
    
    # 3. Stop the API server
    logger.info("[3/5] Stopping API server...")
    stop_api_server()
    
    # 4. Stop Binance stream
    if _binance_collector is not None:
        logger.info("[4/5] Stopping Binance stream...")
        stop_binance_stream()
    else:
        logger.info("[4/5] Binance stream not running")
    
    # 5. Stop the trading engine
    if _trading_engine is not None:
        logger.info("[5/5] Stopping TradingDataEngine...")
        stop_trading_engine()
        _trading_engine = None
        logger.info("      TradingDataEngine stopped")
    else:
        logger.info("[5/5] TradingDataEngine not running")
    
    logger.info("Note: Plays were synced on startup and are preserved in MySQL")
    
    logger.info("=" * 60)
    logger.info("SHUTDOWN COMPLETE - Goodbye!")
    logger.info("=" * 60)


# Keep old name for backward compatibility
def shutdown_engine():
    """Alias for shutdown_all()."""
    shutdown_all()


if __name__ == "__main__":
    main()

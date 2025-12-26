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
import logging

from core.database import cleanup_all_hot_tables, start_trading_engine, stop_trading_engine
from core.config import settings

# Import job status tracking from shared module (avoids circular imports with API)
from scheduler.status import track_job, update_job_status, set_scheduler_start_time

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

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")


# =============================================================================
# JOB FUNCTIONS (wrapped with @track_job for status monitoring)
# =============================================================================

@track_job("fetch_jupiter_prices", "Fetch prices from Jupiter API (every 2s)")
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


@track_job("process_price_cycles", "Process price cycles (every 5s)")
def process_price_cycles():
    """Process price points into price cycle analysis (dual-write to DuckDB + MySQL)."""
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "000data_feeds" / "2_create_price_cycles"))
    from create_price_cycles import process_price_cycles as run_price_cycles
    
    processed = run_price_cycles()
    if processed > 0:
        logger.debug(f"Price cycles: processed {processed} price points")


@track_job("process_wallet_profiles", "Build wallet profiles (every 5s)")
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
    """Create and configure the scheduler."""
    scheduler = BackgroundScheduler(timezone=settings.scheduler_timezone)
    
    # =====================================================
    # JUPITER PRICE FETCHER (runs every 1 second)
    # =====================================================
    
    # Fetch prices from Jupiter API every 1 second (bundled BTC+ETH+SOL)
    # Using v3 API: https://api.jup.ag/price/v3 with API key (60 req/min free tier)
    scheduler.add_job(
        func=fetch_jupiter_prices,
        trigger=IntervalTrigger(seconds=1),
        id="fetch_jupiter_prices",
        name="Fetch Jupiter prices (every 1s)",
        replace_existing=True,
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,    # Combine missed runs
    )
    
    # Clean up old Jupiter price data (every hour)
    scheduler.add_job(
        func=cleanup_jupiter_prices,
        trigger=IntervalTrigger(hours=1),
        id="cleanup_jupiter_prices",
        name="Clean up Jupiter DuckDB (24hr window)",
        replace_existing=True,
    )
    
    # =====================================================
    # DUCKDB MAINTENANCE JOBS
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
    )
    
    # Sync plays from MySQL (every 5 minutes)
    # Keeps DuckDB plays table in sync with MySQL master
    scheduler.add_job(
        func=sync_plays_from_mysql,
        trigger=IntervalTrigger(minutes=5),
        id="sync_plays_from_mysql",
        name="Sync plays table from MySQL to DuckDB",
        replace_existing=True,
    )
    
    # =====================================================
    # LEGACY JOBS (for backward compatibility)
    # =====================================================
    
    # NOTE: archive_legacy_price_points job was removed.
    # Cleanup for legacy prices.duckdb is handled by cleanup_jupiter_prices
    # which correctly uses the 'ts' column via get_prices_from_jupiter.cleanup_old_data()
    
    # =====================================================
    # FEATURE JOBS - Add new features here
    # =====================================================
    
    # Price Cycle Analysis (runs every 5 seconds)
    # Processes price data into cycles at 5 thresholds (0.1-0.5%)
    # Dual-writes to DuckDB (24hr hot) + MySQL (full history)
    scheduler.add_job(
        func=process_price_cycles,
        trigger=IntervalTrigger(seconds=5),
        id="process_price_cycles",
        name="Process price cycles (every 5s)",
        replace_existing=True,
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,    # Combine missed runs
    )
    
    # Wallet Profile Builder (runs every 5 seconds)
    # Builds profiles from trades + completed cycles at all thresholds
    # Dual-writes to DuckDB (24hr hot) + MySQL (also 24hr - special case)
    scheduler.add_job(
        func=process_wallet_profiles,
        trigger=IntervalTrigger(seconds=5),
        id="process_wallet_profiles",
        name="Build wallet profiles (every 5s)",
        replace_existing=True,
        max_instances=1,  # Prevent overlapping runs
        coalesce=True,    # Combine missed runs
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
    api_thread = start_api_in_background(host="127.0.0.1", port=5050)
    
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
    # STEP 4: Sync plays from MySQL immediately on startup
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 4: Syncing plays from MySQL (startup sync)...")
    try:
        sync_plays_from_mysql()
        logger.info("Plays sync complete")
    except Exception as e:
        logger.error(f"Failed to sync plays on startup: {e}")
    
    # =====================================================
    # STEP 5: Create and start the scheduler
    # =====================================================
    logger.info("-" * 60)
    logger.info("STEP 5: Starting Scheduler...")
    _scheduler = create_scheduler()
    
    # Log all registered jobs
    jobs = _scheduler.get_jobs()
    logger.info(f"Registered {len(jobs)} jobs:")
    for job in jobs:
        logger.info(f"  - {job.id}: {job.name}")
    
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
            logger.info("[1/4] Stopping scheduler...")
            _scheduler.shutdown(wait=False)
            _scheduler = None
            logger.info("      Scheduler stopped")
        except Exception as e:
            logger.error(f"      Error stopping scheduler: {e}")
    
    # 2. Stop the API server
    logger.info("[2/4] Stopping API server...")
    stop_api_server()
    
    # 3. Stop Binance stream
    if _binance_collector is not None:
        logger.info("[3/4] Stopping Binance stream...")
        stop_binance_stream()
    else:
        logger.info("[3/4] Binance stream not running")
    
    # 4. Stop the trading engine
    if _trading_engine is not None:
        logger.info("[4/4] Stopping TradingDataEngine...")
        stop_trading_engine()
        _trading_engine = None
        logger.info("      TradingDataEngine stopped")
    else:
        logger.info("[4/4] TradingDataEngine not running")
    
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

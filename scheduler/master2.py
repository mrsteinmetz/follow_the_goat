"""
Master2 Scheduler - Trading Logic
=================================
Trading logic that queries PostgreSQL directly.

Usage:
    python scheduler/master2.py

This script (TRADING LOGIC):
1. Connects to PostgreSQL database (shared with master.py)
2. Runs trading jobs: follow_the_goat, trailing_stop, train_validator, etc.
3. Provides Local API (port 5052) for website

Prerequisites:
- PostgreSQL must be running and schema initialized
- master.py should be running (for data ingestion)

Shutdown:
    Press Ctrl+C to gracefully stop.
"""

import sys
import os
import signal
import threading
import atexit
import time
import json
import fcntl  # For atomic file locking
from pathlib import Path
from datetime import datetime, timedelta, timezone, date

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobExecutionEvent
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor
import logging
import traceback

from core.config import settings
from core.database import get_postgres, postgres_execute, postgres_query, verify_tables_exist

# Import job status tracking from shared module
from scheduler.status import track_job, update_job_status, set_scheduler_start_time, stop_metrics_writer

# FastAPI imports for Local API Server (port 5052)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import uvicorn

# =============================================================================
# LOGGING SETUP
# =============================================================================

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "scheduler2_errors.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Global references
_scheduler = None
_local_api_server = None  # Uvicorn server thread for port 5052


# =============================================================================
# EXCEPTION HANDLING
# =============================================================================

def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Handle uncaught exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logger.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback)
    )

sys.excepthook = global_exception_handler


def apscheduler_error_listener(event: JobExecutionEvent):
    """Listen for job errors."""
    if event.exception:
        logger.error(
            f"Job {event.job_id} crashed: {event.exception}",
            exc_info=event.exception
        )


def apscheduler_missed_listener(event: JobExecutionEvent):
    """Listen for missed jobs."""
    logger.warning(f"Job {event.job_id} missed its scheduled time")


# =============================================================================
# LOCAL API SERVER (PORT 5052)
# =============================================================================

def create_local_api() -> FastAPI:
    """Create FastAPI app for local API server."""
    app = FastAPI(
        title="Follow The Goat - Trading Logic API",
        description="PostgreSQL-backed trading analysis API",
        version="2.0.0"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    # Check key tables
                    cursor.execute("SELECT COUNT(*) FROM prices")
                    prices_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM cycle_tracker")
                    cycles_count = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT COUNT(*) FROM follow_the_goat_buyins")
                    buyins_count = cursor.fetchone()[0]
            
            return {
                "status": "healthy",
                "database": "PostgreSQL",
                "tables": {
                    "prices": prices_count,
                    "cycles": cycles_count,
                    "buyins": buyins_count
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/cycles")
    async def get_cycles(
        limit: int = Query(100, ge=1, le=1000),
        status: Optional[str] = None
    ):
        """Get price cycles."""
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    if status:
                        cursor.execute("""
                            SELECT * FROM cycle_tracker
                            WHERE (cycle_end_time IS NULL AND %s = 'active')
                               OR (cycle_end_time IS NOT NULL AND %s = 'completed')
                            ORDER BY id DESC LIMIT %s
                        """, [status, status, limit])
                    else:
                        cursor.execute("""
                            SELECT * FROM cycle_tracker
                            ORDER BY id DESC LIMIT %s
                        """, [limit])
                    
                    results = cursor.fetchall()
            
            return {"cycles": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get cycles failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/buyins")
    async def get_buyins(
        limit: int = Query(100, ge=1, le=1000),
        status: Optional[str] = None
    ):
        """Get buyins."""
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    if status:
                        cursor.execute("""
                            SELECT * FROM follow_the_goat_buyins
                            WHERE our_status = %s
                            ORDER BY id DESC LIMIT %s
                        """, [status, limit])
                    else:
                        cursor.execute("""
                            SELECT * FROM follow_the_goat_buyins
                            ORDER BY id DESC LIMIT %s
                        """, [limit])
                    
                    results = cursor.fetchall()
            
            return {"buyins": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get buyins failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/plays")
    async def get_plays(limit: int = Query(100, ge=1, le=1000)):
        """Get active plays."""
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT * FROM follow_the_goat_plays
                        WHERE active = TRUE
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                    
                    results = cursor.fetchall()
            
            return {"plays": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get plays failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/profiles")
    async def get_profiles(limit: int = Query(100, ge=1, le=1000)):
        """Get wallet profiles."""
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT * FROM wallet_profiles
                        ORDER BY score DESC LIMIT %s
                    """, [limit])
                    
                    results = cursor.fetchall()
            
            return {"profiles": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get profiles failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/query_sql")
    async def query_sql(sql: str):
        """Execute arbitrary SQL (read-only)."""
        try:
            # Security: only allow SELECT
            if not sql.strip().upper().startswith('SELECT'):
                raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
            
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    results = cursor.fetchall()
            
            return {"results": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Query SQL failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return app


def start_local_api(port: int = 5052, host: str = "0.0.0.0"):
    """Start Local API server in background thread."""
    global _local_api_server
    
    app = create_local_api()
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False
    )
    server = uvicorn.Server(config)
    
    def run_server():
        try:
            server.run()
        except Exception as e:
            logger.error(f"Local API server crashed: {e}", exc_info=True)
    
    thread = threading.Thread(target=run_server, daemon=True, name="LocalAPIThread")
    thread.start()
    
    _local_api_server = server
    logger.info(f"✓ Local API server started on http://{host}:{port}")
    time.sleep(0.5)  # Let server initialize


def stop_local_api():
    """Stop Local API server."""
    global _local_api_server
    
    if _local_api_server:
        try:
            logger.info("Stopping Local API server...")
            _local_api_server.should_exit = True
            time.sleep(1)
            logger.info("✓ Local API server stopped")
        except Exception as e:
            logger.error(f"Error stopping Local API server: {e}")


# =============================================================================
# TRADING JOB WRAPPERS
# =============================================================================

@track_job("train_validator", "Train validator (every 10s)")
def run_train_validator():
    """Run a single validator training cycle."""
    try:
        enabled = os.getenv("TRAIN_VALIDATOR_ENABLED", "1") == "1"
        if not enabled:
            return
        
        trading_path = PROJECT_ROOT / "000trading"
        if str(trading_path) not in sys.path:
            sys.path.insert(0, str(trading_path))
        from train_validator import run_training_cycle
        
        success = run_training_cycle()
        if not success:
            logger.warning("Train validator cycle failed")
    except Exception as e:
        logger.error(f"Train validator job error: {e}", exc_info=True)


@track_job("follow_the_goat", "Wallet tracker cycle (every 1s)")
def run_follow_the_goat():
    """Run a single wallet tracking cycle."""
    try:
        enabled = os.getenv("FOLLOW_THE_GOAT_ENABLED", "1") == "1"
        if not enabled:
            return
        
        trading_path = PROJECT_ROOT / "000trading"
        if str(trading_path) not in sys.path:
            sys.path.insert(0, str(trading_path))
        from follow_the_goat import run_single_cycle
        
        trades_found = run_single_cycle()
        if trades_found:
            logger.debug("Follow the goat: new trades processed")
    except Exception as e:
        logger.error(f"Follow the goat job error: {e}", exc_info=True)


@track_job("trailing_stop_seller", "Trailing stop seller (every 1s)")
def run_trailing_stop_seller():
    """Run a single trailing stop monitoring cycle."""
    try:
        enabled = os.getenv("TRAILING_STOP_ENABLED", "1") == "1"
        if not enabled:
            return
        
        trading_path = PROJECT_ROOT / "000trading"
        if str(trading_path) not in sys.path:
            sys.path.insert(0, str(trading_path))
        from sell_trailing_stop import run_single_cycle
        
        positions_checked = run_single_cycle()
        if positions_checked > 0:
            logger.debug(f"Trailing stop: checked {positions_checked} position(s)")
    except Exception as e:
        logger.error(f"Trailing stop seller job error: {e}", exc_info=True)


@track_job("update_potential_gains", "Update potential gains (every 15s)")
def run_update_potential_gains():
    """Update potential_gains for buyins with completed price cycles."""
    try:
        enabled = os.getenv("UPDATE_POTENTIAL_GAINS_ENABLED", "1") == "1"
        if not enabled:
            return
        
        data_feeds_path = PROJECT_ROOT / "000data_feeds" / "6_update_potential_gains"
        if str(data_feeds_path) not in sys.path:
            sys.path.insert(0, str(data_feeds_path))
        from update_potential_gains import run as update_gains
        
        result = update_gains()
        if result.get('updated', 0) > 0:
            logger.debug(f"Potential gains: updated {result['updated']} records")
    except Exception as e:
        logger.error(f"Update potential gains job error: {e}", exc_info=True)


@track_job("create_new_patterns", "Auto-generate filter patterns (every 5 min)")
def run_create_new_patterns():
    """Auto-generate filter patterns from trade data analysis."""
    try:
        enabled = os.getenv("CREATE_NEW_PATTERNS_ENABLED", "1") == "1"
        if not enabled:
            logger.debug("Create new patterns disabled via CREATE_NEW_PATTERNS_ENABLED=0")
            return
        
        logger.info("Starting create_new_patterns job...")
        patterns_path = PROJECT_ROOT / "000data_feeds" / "7_create_new_patterns"
        if str(patterns_path) not in sys.path:
            sys.path.insert(0, str(patterns_path))
        from create_new_paterns import run as run_pattern_generator
        
        result = run_pattern_generator()
        if result.get('success'):
            logger.info(f"Pattern generation completed: {result.get('suggestions_count', 0)} suggestions, "
                       f"{result.get('combinations_count', 0)} combinations, "
                       f"{result.get('plays_updated', 0)} plays updated")
        else:
            logger.warning(f"Pattern generation failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Create new patterns job error: {e}", exc_info=True)


@track_job("create_profiles", "Build wallet profiles (every 30s)")
def run_create_profiles():
    """Build wallet profiles from trades and price cycles."""
    try:
        enabled = os.getenv("CREATE_PROFILES_ENABLED", "1") == "1"
        if not enabled:
            return
        
        profiles_path = PROJECT_ROOT / "000data_feeds" / "5_create_profiles"
        if str(profiles_path) not in sys.path:
            sys.path.insert(0, str(profiles_path))
        from create_profiles import process_wallet_profiles
        
        inserted = process_wallet_profiles()
        if inserted > 0:
            logger.info(f"Created {inserted} new wallet profiles")
    except Exception as e:
        logger.error(f"Create profiles job error: {e}", exc_info=True)


@track_job("archive_old_data", "Archive data older than 24h to Parquet (hourly)")
def run_archive_old_data():
    """Archive PostgreSQL data older than 24 hours to Parquet files."""
    try:
        enabled = os.getenv("DATA_ARCHIVAL_ENABLED", "1") == "1"
        if not enabled:
            return
        
        archival_path = PROJECT_ROOT / "000data_feeds" / "8_keep_24_hours_of_data"
        if str(archival_path) not in sys.path:
            sys.path.insert(0, str(archival_path))
        from keep_24_hours_of_data import run as run_archival
        
        result = run_archival()
        if result.get('success'):
            logger.info(
                f"Data archival: {result['total_rows_archived']} rows archived, "
                f"{result['total_rows_deleted']} rows deleted, "
                f"{result['total_size_bytes'] / 1024 / 1024:.2f} MB saved"
            )
        else:
            logger.warning(f"Data archival completed with errors: {result.get('errors', [])}")
    except Exception as e:
        logger.error(f"Archive old data job error: {e}", exc_info=True)


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
        
    except Exception as e:
        logger.error(f"Failed to export job status: {e}")


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
    
    # CRITICAL: Force UTC timezone for all operations
    # Even though settings.scheduler_timezone defaults to UTC,
    # we explicitly use pytz.UTC to ensure no system timezone interference
    import pytz
    scheduler = BackgroundScheduler(
        timezone=pytz.UTC,
        executors=executors,
        job_defaults=job_defaults
    )
    
    scheduler.add_listener(apscheduler_error_listener, EVENT_JOB_ERROR)
    scheduler.add_listener(apscheduler_missed_listener, EVENT_JOB_MISSED)
    
    # Register jobs
    scheduler.add_job(
        func=run_follow_the_goat,
        trigger=IntervalTrigger(seconds=1),
        id="follow_the_goat",
        name="Follow The Goat - Wallet Tracker",
        executor='realtime'
    )
    
    scheduler.add_job(
        func=run_trailing_stop_seller,
        trigger=IntervalTrigger(seconds=1),
        id="trailing_stop_seller",
        name="Trailing Stop Seller",
        executor='realtime'
    )
    
    scheduler.add_job(
        func=run_train_validator,
        trigger=IntervalTrigger(seconds=20),
        id="train_validator",
        name="Train Validator",
        executor='realtime'
    )
    
    scheduler.add_job(
        func=run_update_potential_gains,
        trigger=IntervalTrigger(seconds=15),
        id="update_potential_gains",
        name="Update Potential Gains",
        executor='realtime'
    )
    
    # Create New Patterns - runs every 5 minutes, starts immediately
    scheduler.add_job(
        func=run_create_new_patterns,
        trigger=IntervalTrigger(minutes=5),
        id="create_new_patterns",
        name="Create New Patterns",
        executor='heavy',
        next_run_time=datetime.now(timezone.utc),  # Run immediately on startup
        replace_existing=True
    )
    
    scheduler.add_job(
        func=run_create_profiles,
        trigger=IntervalTrigger(seconds=30),
        id="create_profiles",
        name="Create Wallet Profiles",
        executor='heavy'
    )
    
    scheduler.add_job(
        func=export_job_status_to_file,
        trigger=IntervalTrigger(seconds=5),
        id="export_job_status",
        name="Export Job Status",
        executor='realtime'
    )
    
    scheduler.add_job(
        func=run_archive_old_data,
        trigger=CronTrigger(minute=0),
        id="archive_old_data",
        name="Archive Old Data to Parquet",
        executor='heavy'
    )
    
    return scheduler


# =============================================================================
# INSTANCE LOCKING - Using fcntl for atomic locking
# =============================================================================

_lock_file_handle = None  # Keep file handle open to maintain lock

def acquire_lock():
    """
    Acquire an exclusive lock to prevent multiple instances.
    Uses fcntl for atomic file locking (prevents race conditions).
    Returns True if lock acquired, False if another instance is running.
    """
    global _lock_file_handle
    
    lock_file = PROJECT_ROOT / "scheduler" / "master2.lock"
    
    try:
        # Open lock file (create if doesn't exist)
        _lock_file_handle = open(lock_file, 'w')
        
        # Try to acquire exclusive lock (non-blocking)
        # LOCK_EX = exclusive lock, LOCK_NB = non-blocking
        fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        
        # Write our PID to the lock file
        _lock_file_handle.write(str(os.getpid()))
        _lock_file_handle.flush()
        
        logger.info(f"✓ Lock acquired (PID: {os.getpid()})")
        return True
        
    except BlockingIOError:
        # Another process holds the lock
        try:
            # Try to read the PID of the process holding the lock
            with open(lock_file, 'r') as f:
                pid = f.read().strip()
            logger.error(f"✗ Another instance of master2.py is already running (PID: {pid})")
        except:
            logger.error(f"✗ Another instance of master2.py is already running")
            
        logger.error("To force start, kill the existing process or delete the lock file:")
        logger.error(f"  pkill -f 'scheduler/master2.py' && rm {lock_file}")
        
        if _lock_file_handle:
            _lock_file_handle.close()
            _lock_file_handle = None
        return False
        
    except Exception as e:
        logger.error(f"Failed to acquire lock: {e}")
        if _lock_file_handle:
            _lock_file_handle.close()
            _lock_file_handle = None
        return False


def release_lock():
    """Release the exclusive lock and remove lock file."""
    global _lock_file_handle
    
    lock_file = PROJECT_ROOT / "scheduler" / "master2.lock"
    
    try:
        if _lock_file_handle:
            # Release the lock
            fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_UN)
            _lock_file_handle.close()
            _lock_file_handle = None
            logger.info("Lock released")
        
        # Remove lock file
        if lock_file.exists():
            lock_file.unlink()
            
    except Exception as e:
        logger.warning(f"Failed to release lock: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for trading logic scheduler."""
    global _scheduler
    
    # =====================================================
    # CHECK FOR EXISTING INSTANCE
    # =====================================================
    if not acquire_lock():
        sys.exit(1)
    
    # Register lock cleanup on exit
    atexit.register(release_lock)
    
    print("=" * 60)
    print("Follow The Goat - Trading Logic Scheduler (master2.py)")
    print("=" * 60)
    print(f"Database: PostgreSQL (shared with master.py)")
    print(f"Local API: Port 5052")
    print(f"Trading Jobs: Enabled")
    print("=" * 60)
    
    # STEP 1: Verify PostgreSQL connection
    logger.info("STEP 1: Verifying PostgreSQL connection and schema...")
    if not verify_tables_exist():
        logger.error("PostgreSQL schema not ready! Run scripts/postgres_schema.sql first.")
        sys.exit(1)
    logger.info("✓ PostgreSQL connection verified")
    
    # STEP 2: Start Local API server
    logger.info("STEP 2: Starting Local API server (port 5052)...")
    start_local_api(port=5052)
    
    # STEP 3: Create and start scheduler
    logger.info("STEP 3: Creating trading scheduler...")
    _scheduler = create_scheduler()
    set_scheduler_start_time()
    
    logger.info("STEP 4: Starting scheduler...")
    _scheduler.start()
    logger.info("✓ Scheduler started successfully")
    
    # Print job summary
    jobs = _scheduler.get_jobs()
    logger.info(f"\nRegistered {len(jobs)} trading jobs:")
    for job in jobs:
        logger.info(f"  - {job.id}: {job.name}")
    
    print("\n" + "=" * 60)
    print("Trading Logic Scheduler is running!")
    print("Press Ctrl+C to stop.")
    print("=" * 60 + "\n")
    
    # Keep alive
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nShutdown signal received...")
        shutdown_all()


def shutdown_all():
    """Gracefully shutdown all services."""
    global _scheduler
    
    # Release lock first
    release_lock()
    
    logger.info("Shutting down...")
    
    # Stop scheduler
    if _scheduler:
        logger.info("Stopping scheduler...")
        try:
            _scheduler.shutdown(wait=True)
            logger.info("✓ Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    # Stop Local API
    stop_local_api()
    
    # Stop metrics writer
    stop_metrics_writer()
    
    # Close PostgreSQL connections
    from core.database import close_all_postgres
    close_all_postgres()
    
    logger.info("✓ Shutdown complete")


# Register shutdown handler
atexit.register(shutdown_all)
signal.signal(signal.SIGTERM, lambda sig, frame: shutdown_all())
signal.signal(signal.SIGINT, lambda sig, frame: shutdown_all())


if __name__ == "__main__":
    main()

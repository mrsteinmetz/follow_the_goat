"""
Scheduler Job Functions
=======================
All job functions used by run_component.py are defined here.
This module contains NO scheduler startup code - just the job implementations.

Each job function is a single unit of work that can be called by run_component.py.
"""

from __future__ import annotations

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scheduler.status import track_job

logger = logging.getLogger("scheduler.jobs")


# =============================================================================
# MASTER JOBS (Data Ingestion)
# =============================================================================

@track_job("fetch_jupiter_prices", "Fetch prices from Jupiter API (every 1s)")
def fetch_jupiter_prices():
    """Fetch prices from Jupiter API and store in PostgreSQL."""
    jupiter_path = PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices"
    if str(jupiter_path) not in sys.path:
        sys.path.insert(0, str(jupiter_path))
    
    from get_prices_from_jupiter import fetch_and_store_once
    count, pg_ok = fetch_and_store_once()
    if count > 0 and not pg_ok:
        logger.warning(f"Failed to write {count} prices to PostgreSQL")


@track_job("sync_trades_from_webhook", "Sync trades from webhook (every 1s)")
def sync_trades_from_webhook():
    """No-op: trades are now pushed directly via FastAPI webhook (port 8001)."""
    logger.debug("sync_trades_from_webhook skipped (push-based webhook)")


@track_job("process_price_cycles", "Process price cycles (PostgreSQL)")
def process_price_cycles_job():
    """Process price cycles and track cycle states."""
    try:
        cycles_path = PROJECT_ROOT / "000data_feeds" / "2_create_price_cycles"
        if str(cycles_path) not in sys.path:
            sys.path.insert(0, str(cycles_path))

        from create_price_cycles import process_price_cycles as process_price_cycles_run
        process_price_cycles_run()
    except Exception as e:
        logger.error(f"Price cycles error: {e}", exc_info=True)


# =============================================================================
# MASTER2 JOBS (Trading Logic)
# =============================================================================

@track_job("train_validator", "Train validator (every 20s)")
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


@track_job("create_new_patterns", "Auto-generate filter patterns (every 10 min)")
def run_create_new_patterns():
    """Auto-generate filter patterns from trade data analysis."""
    logger.info("[PATTERN GENERATOR] Function run_create_new_patterns() called")
    try:
        enabled = os.getenv("CREATE_NEW_PATTERNS_ENABLED", "1") == "1"
        logger.info(f"[PATTERN GENERATOR] Environment check: CREATE_NEW_PATTERNS_ENABLED={os.getenv('CREATE_NEW_PATTERNS_ENABLED', 'NOT_SET')}, enabled={enabled}")
        if not enabled:
            logger.warning("[PATTERN GENERATOR] DISABLED via CREATE_NEW_PATTERNS_ENABLED=0")
            return
        
        logger.info("[PATTERN GENERATOR] Starting create_new_patterns job...")
        patterns_path = PROJECT_ROOT / "000data_feeds" / "7_create_new_patterns"
        if str(patterns_path) not in sys.path:
            sys.path.insert(0, str(patterns_path))
        from create_new_paterns import run as run_pattern_generator
        
        result = run_pattern_generator()
        if result.get('success'):
            logger.info(f"[PATTERN GENERATOR] Completed: {result.get('suggestions_count', 0)} suggestions, "
                       f"{result.get('combinations_count', 0)} combinations, "
                       f"{result.get('plays_updated', 0)} plays updated")
        else:
            logger.error(f"[PATTERN GENERATOR] FAILED: {result.get('error', 'Unknown error')}")
    except Exception as e:
        logger.error(f"[PATTERN GENERATOR] Exception in job wrapper: {e}", exc_info=True)


@track_job("recalculate_pump_filters", "Recalculate pump continuation filters (every 5 min)")
def run_recalculate_pump_filters():
    """Discover best filter combo from recent buyins and write to pump_continuation_rules."""
    try:
        enabled = os.getenv("RECALCULATE_PUMP_FILTERS_ENABLED", "1") == "1"
        if not enabled:
            return

        features_path = PROJECT_ROOT / "features" / "pump_continuation"
        if str(features_path) not in sys.path:
            sys.path.insert(0, str(features_path))
        from features.pump_continuation.recalculate import recalculate

        result = recalculate()
        if result.get('status') == 'ok':
            logger.info(f"Pump filters recalculated: prec={result['best_test_precision']:.1f}%, "
                        f"cols={result['columns']}")
        elif result.get('status') == 'no_combos':
            logger.info("Pump filters: no profitable combos found, keeping existing rules")
        else:
            logger.info(f"Pump filters: {result.get('status', 'unknown')} - {result.get('reason', '')}")
    except Exception as e:
        logger.error(f"Recalculate pump filters job error: {e}", exc_info=True)


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


@track_job("restart_quicknode_streams", "Monitor stream latency (every 15s)")
def run_restart_quicknode_streams():
    """Monitor QuickNode stream latency and restart if needed."""
    try:
        enabled = os.getenv("STREAM_MONITOR_ENABLED", "1") == "1"
        if not enabled:
            return
        
        streams_path = PROJECT_ROOT / "000data_feeds" / "9_restart_quicknode_streams"
        if str(streams_path) not in sys.path:
            sys.path.insert(0, str(streams_path))
        from restart_streams import run_monitoring_cycle
        
        result = run_monitoring_cycle()
        if result.get('success'):
            if result.get('action_taken'):
                restart_success = result.get('restart_success', False)
                status = "successfully" if restart_success else "with errors"
                logger.info(f"Stream restart triggered {status} (latency: {result['latency']:.2f}s)")
        else:
            logger.error(f"Stream monitoring failed: {result.get('error', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Restart QuickNode streams job error: {e}", exc_info=True)


@track_job("export_job_status", "Export job status to file (every 5s)")
def export_job_status_to_file():
    """Export current job status to JSON file for website_api.py to read."""
    import json
    from scheduler.status import _job_status, _job_status_lock
    
    LOGS_DIR = PROJECT_ROOT / "logs"
    
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
# SERVICE MANAGEMENT (for run_component.py managed services)
# =============================================================================

# Global references for services
_webhook_server = None
_php_server_process = None
_binance_collector = None
_local_api_server = None


def start_webhook_api_in_background(host: str = "0.0.0.0", port: int = 8001):
    """Start FastAPI webhook server in background thread."""
    global _webhook_server
    
    import threading
    import uvicorn
    from features.webhook.app import app  # Use the proper webhook app
    
    # The app from features/webhook/app.py already has CORS middleware and all endpoints
    
    config = uvicorn.Config(app, host=host, port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    
    def run_server():
        try:
            server.run()
        except Exception as e:
            logger.error(f"Webhook server crashed: {e}", exc_info=True)
    
    thread = threading.Thread(target=run_server, daemon=True, name="WebhookServerThread")
    thread.start()
    
    _webhook_server = server
    logger.info(f"✓ Webhook server started on http://{host}:{port}")
    import time
    time.sleep(0.5)


def stop_webhook_api():
    """Stop webhook server."""
    global _webhook_server
    if _webhook_server:
        try:
            _webhook_server.should_exit = True
            import time
            time.sleep(1)
            logger.info("✓ Webhook server stopped")
        except Exception as e:
            logger.error(f"Error stopping webhook server: {e}")


def start_php_server(host: str = "0.0.0.0", port: int = 8000):
    """Start PHP built-in server for website."""
    global _php_server_process
    import subprocess
    
    website_dir = PROJECT_ROOT / "000website"
    
    cmd = ["php", "-S", f"{host}:{port}", "-t", str(website_dir)]
    _php_server_process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(website_dir)
    )
    logger.info(f"✓ PHP server started on http://{host}:{port}")


def stop_php_server():
    """Stop PHP server."""
    global _php_server_process
    if _php_server_process:
        try:
            _php_server_process.terminate()
            _php_server_process.wait(timeout=5)
            logger.info("✓ PHP server stopped")
        except Exception as e:
            logger.error(f"Error stopping PHP server: {e}")
            try:
                _php_server_process.kill()
            except:
                pass


def start_binance_stream_in_background(symbol: str = "SOLUSDT", mode: str = "conservative"):
    """Start Binance order book stream."""
    global _binance_collector
    
    binance_path = PROJECT_ROOT / "000data_feeds" / "3_binance_order_book_data"
    if str(binance_path) not in sys.path:
        sys.path.insert(0, str(binance_path))
    
    from binance_order_book_stream import OrderBookCollector
    
    _binance_collector = OrderBookCollector(symbol=symbol, mode=mode)
    _binance_collector.start()
    logger.info(f"✓ Binance stream started for {symbol}")


def stop_binance_stream():
    """Stop Binance stream."""
    global _binance_collector
    if _binance_collector:
        try:
            _binance_collector.stop()
            logger.info("✓ Binance stream stopped")
        except Exception as e:
            logger.error(f"Error stopping Binance stream: {e}")


def start_local_api(port: int = 5052, host: str = "0.0.0.0"):
    """Start Local API server in background thread."""
    global _local_api_server
    
    import threading
    import uvicorn
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from typing import Optional
    from core.database import get_postgres
    
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
                    cursor.execute("SELECT COUNT(*) FROM prices")
                    prices_count = cursor.fetchone()[0]
            
            return {
                "status": "healthy",
                "database": "PostgreSQL",
                "prices_count": prices_count,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/cycles")
    async def get_cycles(limit: int = Query(100, ge=1, le=1000), status: Optional[str] = None):
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
                        cursor.execute("SELECT * FROM cycle_tracker ORDER BY id DESC LIMIT %s", [limit])
                    results = cursor.fetchall()
            return {"cycles": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get cycles failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/buyins")
    async def get_buyins(limit: int = Query(100, ge=1, le=1000), status: Optional[str] = None):
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
                        cursor.execute("SELECT * FROM follow_the_goat_buyins ORDER BY id DESC LIMIT %s", [limit])
                    results = cursor.fetchall()
            return {"buyins": results, "count": len(results)}
        except Exception as e:
            logger.error(f"Get buyins failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    config = uvicorn.Config(app, host=host, port=port, log_level="warning", access_log=False)
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
    import time
    time.sleep(0.5)


def stop_local_api():
    """Stop Local API server."""
    global _local_api_server
    if _local_api_server:
        try:
            logger.info("Stopping Local API server...")
            _local_api_server.should_exit = True
            import time
            time.sleep(1)
            logger.info("✓ Local API server stopped")
        except Exception as e:
            logger.error(f"Error stopping Local API server: {e}")

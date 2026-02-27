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

@track_job("train_validator", "Train validator (every 5s)")
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


@track_job("wallet_executor", "Paper wallet executor (every 1s)")
def run_wallet_executor():
    """Run a single wallet executor cycle."""
    try:
        trading_path = PROJECT_ROOT / "000trading"
        if str(trading_path) not in sys.path:
            sys.path.insert(0, str(trading_path))
        from wallet_executor import run_wallet_cycle
        run_wallet_cycle()
    except Exception as e:
        logger.error(f"Wallet executor job error: {e}", exc_info=True)


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
                trigger = result.get('latency') or result.get('trigger_seconds')
                trigger_str = f"{trigger:.2f}s" if trigger is not None else "n/a"
                logger.info(f"Stream restart triggered {status} (trigger: {trigger_str}, reason: {result.get('reason', '?')})")
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

    # If already responding on this port, nothing to do — avoids "address already in use" errors
    # when the process is restarted while the old uvicorn thread is still winding down.
    if is_webhook_responding():
        logger.info(f"Webhook already responding on port {port}, skipping start")
        return

    import threading
    import uvicorn
    from features.webhook.app import app

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
    logger.info(f"✓ Webhook server starting on http://{host}:{port}")

    import time
    time.sleep(1.5)  # Give uvicorn time to bind

    if not is_webhook_responding():
        _webhook_server = None
        raise RuntimeError(
            f"Webhook server failed to start on port {port} — port may already be in use by another process"
        )


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


def is_webhook_responding() -> bool:
    """Return True if webhook responds to GET /health (used by run_component to detect crashes)."""
    try:
        import urllib.request
        req = urllib.request.urlopen("http://127.0.0.1:8001/health", timeout=2)
        return req.status == 200
    except Exception:
        return False


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
    
    from stream_binance_order_book_data import BinanceOrderBookCollector
    
    _binance_collector = BinanceOrderBookCollector(symbol=symbol, mode=mode)
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


# ---------------------------------------------------------------------------
# Pump Signal V4 Fingerprint Refresh (separate process from train_validator)
# ---------------------------------------------------------------------------

def run_refresh_pump_model():
    """Run pump fingerprint analysis and refresh V4 rules.

    Runs in its own process every 5 minutes. Analyzes 7 days of trail data
    to discover repeatable pump patterns and combination rules.
    train_validator reads the resulting JSON rules via maybe_refresh_rules().
    """
    try:
        import logging as _logging
        _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        trading_path = str(PROJECT_ROOT / "000trading")
        if trading_path not in sys.path:
            sys.path.insert(0, trading_path)

        # Sync high-freq DuckDB cache before retraining (feeds readiness score)
        try:
            from pump_highfreq_cache import sync_highfreq_cache
            sync_highfreq_cache(lookback_minutes=30)
        except Exception as hf_err:
            logger.warning(f"HF cache sync skipped: {hf_err}")

        from pump_signal_logic import refresh_pump_rules
        refresh_pump_rules()
        logger.info("V4 fingerprint refresh cycle complete.")

    except Exception as e:
        logger.error(f"V4 fingerprint refresh error: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Signal Discovery Engine (SDE) — Overnight Sweep
# ---------------------------------------------------------------------------

def run_sde_overnight_sweep():
    """Run the Signal Discovery Engine sweep and auto-apply better filters.

    This is CPU-heavy (can take 2-3 hours with 24h of data) and runs as its
    own isolated component process so it doesn't block train_validator or
    other real-time components.
    """
    try:
        trading_path = str(PROJECT_ROOT / "000trading")
        if trading_path not in sys.path:
            sys.path.insert(0, trading_path)

        from signal_discovery_engine import run_sweep, apply_best_filters, setup_logging
        from datetime import datetime, timezone

        hours = int(os.getenv("SDE_HOURS", "24"))
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        output_path = f"/tmp/sde_overnight_{date_str}.json"
        log_path = f"/tmp/sde_overnight_{date_str}.log"

        setup_logging(log_path)
        logger.info(f"SDE sweep starting: hours={hours}, output={output_path}")

        results = run_sweep(hours, output_path)

        if results:
            applied = apply_best_filters(results)
            if applied:
                logger.info("SDE: New pump continuation rules applied.")
            else:
                logger.info("SDE: Current rules kept (no improvement found).")
        else:
            logger.info("SDE: No profitable results found.")

        logger.info("SDE sweep finished.")

    except Exception as e:
        logger.error(f"SDE overnight sweep error: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Backfill Raw Cache — populate Parquet from PostgreSQL (startup + hourly)
# ---------------------------------------------------------------------------

def run_backfill_raw_cache():
    """Backfill raw OB/trade/whale Parquet cache from PostgreSQL.

    Runs once on startup and then hourly to ensure the cache covers the last 24h.
    This is the source of truth for both the pump model training and the
    live signal detection.  The live data feeds (binance_stream, webhook_server)
    keep it fresh in between.
    """
    try:
        from datetime import datetime, timezone, timedelta
        from core.raw_data_cache import OB_PARQUET, TRADE_PARQUET, WHALE_PARQUET, _CACHE_DIR
        import pyarrow as pa
        import pyarrow.parquet as pq
        from core.database import get_postgres

        hours = 24
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        _CACHE_DIR.mkdir(exist_ok=True)

        logger.info(f"[backfill] Starting raw cache backfill (last {hours}h)...")

        def _f(rows, key):
            return pa.array([float(r[key]) if r[key] is not None else None for r in rows],
                            type=pa.float64())

        # OB snapshots
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, mid_price, spread_bps,
                           bid_liquidity AS bid_liq, ask_liquidity AS ask_liq,
                           volume_imbalance AS vol_imb, depth_imbalance_ratio AS depth_ratio,
                           microprice, microprice_dev_bps AS microprice_dev,
                           net_liquidity_change_1s AS net_liq_1s,
                           bid_slope, ask_slope,
                           bid_depth_bps_5 AS bid_dep_5bps, ask_depth_bps_5 AS ask_dep_5bps
                    FROM order_book_features WHERE timestamp >= %s ORDER BY timestamp
                """, [cutoff])
                ob_rows = cur.fetchall()
        if ob_rows:
            tbl = pa.table({
                'ts': pa.array([r['timestamp'] for r in ob_rows], type=pa.timestamp('us', tz='UTC')),
                'mid_price': _f(ob_rows, 'mid_price'), 'spread_bps': _f(ob_rows, 'spread_bps'),
                'bid_liq': _f(ob_rows, 'bid_liq'), 'ask_liq': _f(ob_rows, 'ask_liq'),
                'vol_imb': _f(ob_rows, 'vol_imb'), 'depth_ratio': _f(ob_rows, 'depth_ratio'),
                'microprice': _f(ob_rows, 'microprice'), 'microprice_dev': _f(ob_rows, 'microprice_dev'),
                'net_liq_1s': _f(ob_rows, 'net_liq_1s'), 'bid_slope': _f(ob_rows, 'bid_slope'),
                'ask_slope': _f(ob_rows, 'ask_slope'),
                'bid_dep_5bps': _f(ob_rows, 'bid_dep_5bps'), 'ask_dep_5bps': _f(ob_rows, 'ask_dep_5bps'),
            })
            tmp = _CACHE_DIR / "ob_latest.bfill.parquet"
            pq.write_table(tbl, str(tmp), compression='snappy')
            tmp.replace(OB_PARQUET)

        # Trades
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT trade_timestamp, sol_amount, stablecoin_amount, price,
                           direction, (perp_direction IS NOT NULL) AS is_perp
                    FROM sol_stablecoin_trades WHERE trade_timestamp >= %s ORDER BY trade_timestamp
                """, [cutoff])
                tr_rows = cur.fetchall()
        if tr_rows:
            tbl = pa.table({
                'ts': pa.array([r['trade_timestamp'] for r in tr_rows], type=pa.timestamp('us', tz='UTC')),
                'sol_amount': pa.array([float(r['sol_amount'] or 0) for r in tr_rows], type=pa.float64()),
                'stable_amt': pa.array([float(r['stablecoin_amount'] or 0) for r in tr_rows], type=pa.float64()),
                'price': pa.array([float(r['price'] or 0) for r in tr_rows], type=pa.float64()),
                'direction': pa.array([str(r['direction'] or 'buy') for r in tr_rows], type=pa.string()),
                'is_perp': pa.array([bool(r['is_perp']) for r in tr_rows], type=pa.bool_()),
            })
            tmp = _CACHE_DIR / "trade_latest.bfill.parquet"
            pq.write_table(tbl, str(tmp), compression='snappy')
            tmp.replace(TRADE_PARQUET)

        # Whales
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT timestamp,
                           abs_change AS sol_moved, direction,
                           CASE WHEN movement_significance ~ '^[0-9.]+$'
                                THEN movement_significance::DOUBLE PRECISION
                                ELSE NULL END AS significance,
                           percentage_moved
                    FROM whale_movements WHERE timestamp >= %s ORDER BY timestamp
                """, [cutoff])
                wh_rows = cur.fetchall()
        if wh_rows:
            tbl = pa.table({
                'ts': pa.array([r['timestamp'] for r in wh_rows], type=pa.timestamp('us', tz='UTC')),
                'sol_moved': pa.array([float(r['sol_moved'] or 0) for r in wh_rows], type=pa.float64()),
                'direction': pa.array([str(r['direction'] or 'out') for r in wh_rows], type=pa.string()),
                'significance': pa.array([float(r['significance']) if r['significance'] is not None else None
                                           for r in wh_rows], type=pa.float64()),
                'pct_moved': pa.array([float(r['percentage_moved'] or 0) for r in wh_rows], type=pa.float64()),
            })
            tmp = _CACHE_DIR / "whale_latest.bfill.parquet"
            pq.write_table(tbl, str(tmp), compression='snappy')
            tmp.replace(WHALE_PARQUET)

        logger.info(
            f"[backfill] Complete — OB={len(ob_rows):,} trades={len(tr_rows):,} whales={len(wh_rows):,}"
        )
    except Exception as e:
        logger.error(f"Backfill raw cache error: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Mega Signal Simulator — continuous GA + exit grid optimizer (standalone)
# ---------------------------------------------------------------------------

def run_mega_simulator():
    """Run one full loop of the Mega Signal Simulator (GA + exit grid search).

    Each run:
      1. Builds a dense 30-second feature matrix from DuckDB + PostgreSQL
      2. Runs the genetic algorithm to find optimal entry signal combinations
      3. Grid-searches 1,200 exit tier configurations for each top signal
      4. Validates with walk-forward holdout, saves results to simulation_results
    """
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "mega_simulator",
            PROJECT_ROOT / "scripts" / "mega_simulator.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.run_one_loop(run_number=1)
    except Exception as e:
        logger.error(f"Mega simulator error: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Email Report — System Health Summary (every 12h)
# ---------------------------------------------------------------------------

def run_send_email_report():
    """Generate and email the system health report (every 12 hours)."""
    try:
        from features.email_report.mailer import send_report
        sent = send_report()
        if sent:
            logger.info("System health email report sent successfully.")
        else:
            logger.info("System health email report skipped (SMTP not configured or error).")
    except Exception as e:
        logger.error(f"Email report job error: {e}", exc_info=True)
        raise

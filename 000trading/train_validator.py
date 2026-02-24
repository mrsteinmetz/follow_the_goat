"""
Train Validator
===============
Pump signal detection loop that runs every 5 seconds.

Each cycle:
  1. Checks that raw Parquet cache is fresh (populated by binance_stream + webhook_server)
  2. Fetches current SOL market price and cycle
  3. Refreshes fingerprint rules if stale (every 5 min)
  4. Fires pump signal if live features match any approved rule

No synthetic buyins, no trail generation. Features come directly from the
raw Parquet cache written by the data feed processes.

Usage:
    python 000trading/train_validator.py
    python3 scheduler/run_component.py --component train_validator
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
PROJECT_ROOT = Path(__file__).parent.parent
MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_DIR))

from core.database import get_postgres

# Configuration
TRAINING_INTERVAL_SECONDS = int(os.getenv("TRAIN_VALIDATOR_INTERVAL", "5"))
PUMP_SIGNAL_PLAY_ID = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "3"))

# How stale the OB Parquet can be before we skip the cycle (seconds)
MAX_PARQUET_AGE_SECONDS = int(os.getenv("MAX_PARQUET_AGE_SECONDS", "60"))

# Setup logging
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    log_file = LOGS_DIR / "train_validator.log"
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    ))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# =============================================================================
# DATA READINESS
# =============================================================================

def check_data_readiness() -> tuple[bool, str]:
    """Check that raw Parquet cache files are fresh enough to use.

    The OB Parquet is written by binance_stream every ~1s, so a mtime older
    than MAX_PARQUET_AGE_SECONDS means the data feed is likely down.
    """
    try:
        from core.raw_data_cache import OB_PARQUET
        if not OB_PARQUET.exists():
            return False, f"OB Parquet missing: {OB_PARQUET}"
        age = time.time() - OB_PARQUET.stat().st_mtime
        if age > MAX_PARQUET_AGE_SECONDS:
            return False, f"OB Parquet stale ({age:.0f}s old, max {MAX_PARQUET_AGE_SECONDS}s)"
        return True, f"Parquet fresh ({age:.1f}s old)"
    except Exception as e:
        return False, f"Readiness check error: {e}"


# =============================================================================
# MARKET DATA
# =============================================================================

def get_current_market_price() -> Optional[float]:
    """Get current SOL price from PostgreSQL prices table."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT price FROM prices WHERE token = 'SOL' ORDER BY timestamp DESC LIMIT 1"
                )
                result = cursor.fetchone()
                return float(result['price']) if result else None
    except Exception as e:
        logger.error(f"Error getting market price: {e}")
        return None


def get_current_price_cycle() -> Optional[int]:
    """Get the active cycle_tracker ID (threshold=0.3, no end time)."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM cycle_tracker
                    WHERE threshold = 0.3 AND cycle_end_time IS NULL
                    ORDER BY id DESC LIMIT 1
                """)
                result = cursor.fetchone()
                return result['id'] if result else None
    except Exception as e:
        logger.error(f"Error getting price cycle: {e}")
        return None


# =============================================================================
# STUCK TRADE CLEANUP
# =============================================================================

def cleanup_stuck_validating_trades(max_age_seconds: int = 120) -> int:
    """Clean up real pump buyins that are stuck in 'validating' status."""
    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM follow_the_goat_buyins
                    WHERE our_status = 'validating'
                      AND followed_at < %s
                      AND wallet_address NOT LIKE 'TRAINING_TEST_%%'
                """, [cutoff_time])
                stuck = [r['id'] for r in cursor.fetchall()]

        if not stuck:
            return 0

        logger.warning(f"Cleaning up {len(stuck)} stuck 'validating' buyins: {stuck}")
        error_log = json.dumps({
            'decision': 'ERROR',
            'error': f'Stuck in validating status for >{max_age_seconds}s — auto-cleaned',
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        for bid in stuck:
            try:
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "UPDATE follow_the_goat_buyins SET our_status='error', pattern_validator_log=%s WHERE id=%s",
                            [error_log, bid]
                        )
            except Exception as e:
                logger.debug(f"Cleanup update failed for #{bid}: {e}")
        return len(stuck)
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        return 0


# =============================================================================
# TRAINING CYCLE
# =============================================================================

class TrainingStats:
    """Track per-session statistics."""
    def __init__(self):
        self.cycles = 0
        self.signals_fired = 0
        self.skipped = 0
        self.errors = 0
        self.start_time = datetime.now(timezone.utc)

    def log_summary(self) -> None:
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        logger.info(
            f"Stats | cycles={self.cycles} fired={self.signals_fired} "
            f"skipped={self.skipped} errors={self.errors} "
            f"uptime={uptime/60:.1f}m"
        )


_stats = TrainingStats()


def run_training_cycle() -> bool:
    """Execute one pump signal detection cycle.

    Flow:
      1. Check Parquet cache freshness
      2. Get market price + active cycle
      3. Refresh fingerprint rules if stale
      4. Fire pump signal if live features match

    Returns True if cycle completed (regardless of signal fired).
    """
    _stats.cycles += 1
    cycle_start = time.time()

    # 1. Verify raw cache is fresh
    is_ready, reason = check_data_readiness()
    if not is_ready:
        logger.debug(f"Skipping cycle: {reason}")
        _stats.skipped += 1
        return False

    # 2. Cleanup any stuck real buyins (low-frequency)
    if _stats.cycles % 10 == 0:
        cleanup_stuck_validating_trades(max_age_seconds=120)

    # 3. Market data
    market_price = get_current_market_price()
    if not market_price:
        logger.debug("No market price — skipping")
        _stats.skipped += 1
        return False

    price_cycle = get_current_price_cycle()

    # 4. Pump signal check
    pump_fired = False
    if PUMP_SIGNAL_PLAY_ID:
        try:
            from pump_signal_logic import (
                maybe_refresh_rules,
                check_and_fire_pump_signal,
                PUMP_OBSERVATION_MODE,
            )
            maybe_refresh_rules()
            pump_fired = check_and_fire_pump_signal(
                market_price=market_price,
                price_cycle=price_cycle,
            )
            if pump_fired:
                _stats.signals_fired += 1
        except Exception as pump_err:
            logger.error(f"Pump signal error: {pump_err}", exc_info=True)
            _stats.errors += 1

    # 5. Fast-path readiness (skip normal wait if something is happening)
    readiness_triggered = False
    try:
        from pump_signal_logic import should_trigger_fast_path
        readiness_triggered = should_trigger_fast_path()
    except Exception:
        pass

    cycle_ms = round((time.time() - cycle_start) * 1000)
    tags = []
    if pump_fired:
        tags.append("PUMP!")
    if readiness_triggered:
        tags.append("READY!")
    try:
        if PUMP_OBSERVATION_MODE:  # type: ignore[name-defined]
            tags.append("[OBS]")
    except NameError:
        pass

    tag_str = " ".join(tags)
    logger.info(f"✓ ${market_price:.2f} cycle={price_cycle} [{cycle_ms}ms]{' ' + tag_str if tag_str else ''}")

    return True


def run_continuous_training(interval_seconds: Optional[int] = None) -> None:
    """Run the pump detection loop continuously."""
    interval = interval_seconds or TRAINING_INTERVAL_SECONDS

    logger.info("=" * 70)
    logger.info("PUMP SIGNAL DETECTOR STARTED")
    logger.info(f"  Interval:          {interval}s")
    logger.info(f"  Pump play ID:      {PUMP_SIGNAL_PLAY_ID}")
    logger.info(f"  Max Parquet age:   {MAX_PARQUET_AGE_SECONDS}s")
    logger.info("=" * 70)

    while True:
        try:
            run_training_cycle()

            if _stats.cycles % 60 == 0:
                _stats.log_summary()

            time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — shutting down")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            _stats.errors += 1
            time.sleep(interval)

    _stats.log_summary()
    logger.info("Pump signal detector stopped")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run pump signal detector")
    parser.add_argument("--interval", type=int, default=TRAINING_INTERVAL_SECONDS,
                        help="Interval between cycles (seconds)")
    parser.add_argument("--once", action="store_true", help="Run single cycle and exit")
    args = parser.parse_args()

    if args.once:
        success = run_training_cycle()
        sys.exit(0 if success else 1)
    else:
        run_continuous_training(args.interval)

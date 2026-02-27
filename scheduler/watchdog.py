"""
Watchdog - Component Auto-Restart
===================================
Runs every 2 minutes (via cron). Checks heartbeats for all critical components
and restarts any that have gone silent.

Usage:
    python3 scheduler/watchdog.py               # check + restart dead ones
    python3 scheduler/watchdog.py --dry-run     # check only, don't restart

Cron setup (every 2 minutes):
    */2 * * * * cd /root/follow_the_goat && venv/bin/python3 scheduler/watchdog.py >> /tmp/watchdog.log 2>&1
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [watchdog] %(levelname)s %(message)s",
)
logger = logging.getLogger("watchdog")

# Components that must always be running and should be auto-restarted.
# Ordered by priority — most critical first.
CRITICAL_COMPONENTS = [
    "trailing_stop_seller",    # exits open positions — MUST NEVER BE DOWN
    "follow_the_goat",         # enters new positions
    "train_validator",         # fires pump entry signals
    "fetch_jupiter_prices",    # price feed
    "process_price_cycles",    # detects price cycles
    "sync_trades_from_webhook",# syncs trade data from webhook
    "binance_stream",          # order book data feed
    "wallet_executor",         # executes buy/sell swaps
    "create_profiles",         # wallet profiles for signal quality
    "update_potential_gains",  # analytics (affects dashboard)
    "create_new_patterns",     # generates filter patterns
    "archive_old_data",        # keeps DB clean — prevents disk bloat
    "restart_quicknode_streams",
]

# A component is considered dead if its heartbeat is older than this (seconds).
STALE_THRESHOLD_SECONDS = 60

# After restarting, wait this many seconds before checking the next one.
RESTART_COOLDOWN_SECONDS = 3

PYTHON = str(PROJECT_ROOT / "venv/bin/python")
RUNNER = str(PROJECT_ROOT / "scheduler/run_component.py")


def get_stale_components() -> list[str]:
    """Return component IDs whose latest heartbeat is older than STALE_THRESHOLD_SECONDS."""
    try:
        from core.database import get_postgres
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT component_id,
                           MAX(last_heartbeat_at) as latest_hb,
                           EXTRACT(EPOCH FROM (NOW() - MAX(last_heartbeat_at))) as age_seconds
                    FROM scheduler_component_heartbeats
                    GROUP BY component_id
                """)
                rows = cursor.fetchall()
        known = {r["component_id"]: r["age_seconds"] for r in rows}
    except Exception as e:
        logger.error(f"DB error reading heartbeats: {e}")
        return []

    stale = []
    for comp in CRITICAL_COMPONENTS:
        age = known.get(comp)
        if age is None:
            logger.warning(f"  {comp}: never seen in heartbeats — queuing for start")
            stale.append(comp)
        elif age > STALE_THRESHOLD_SECONDS:
            logger.warning(f"  {comp}: stale ({age:.0f}s) — queuing for restart")
            stale.append(comp)
        else:
            logger.debug(f"  {comp}: ok ({age:.0f}s)")
    return stale


def is_process_running(component_id: str) -> bool:
    """Check if a run_component.py process for this component is already in ps."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"run_component.py --component {component_id}"],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def restart_component(component_id: str) -> bool:
    """Start a component in the background. Advisory lock in run_component.py
    ensures only one instance runs — safe to call even if already running."""
    log_file = f"/tmp/{component_id}.log"
    try:
        proc = subprocess.Popen(
            [PYTHON, RUNNER, "--component", component_id],
            stdout=open(log_file, "a"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=str(PROJECT_ROOT),
        )
        logger.info(f"  Started {component_id} (PID {proc.pid}) → log: {log_file}")
        return True
    except Exception as e:
        logger.error(f"  Failed to start {component_id}: {e}")
        return False


def main(dry_run: bool = False) -> None:
    logger.info(f"=== Watchdog check (dry_run={dry_run}) ===")

    stale = get_stale_components()

    if not stale:
        logger.info("All critical components are healthy.")
        return

    logger.info(f"{len(stale)} component(s) need attention: {stale}")

    for comp in stale:
        if is_process_running(comp):
            # Process is alive but not heartbeating — might be stuck or DB issue.
            logger.warning(f"  {comp}: process IS running but heartbeat is stale — skipping restart")
            continue

        if dry_run:
            logger.info(f"  [dry-run] Would restart: {comp}")
        else:
            restart_component(comp)
            time.sleep(RESTART_COOLDOWN_SECONDS)

    logger.info("Watchdog check complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Component watchdog")
    parser.add_argument("--dry-run", action="store_true", help="Check only, don't restart")
    args = parser.parse_args()
    main(dry_run=args.dry_run)

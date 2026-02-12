#!/usr/bin/env python3
"""
Per-Component Service Runner
===========================
Runs exactly ONE scheduler component as an independent process.

Features:
- PostgreSQL advisory lock (singleton across processes)
- PostgreSQL enable/disable toggle
- PostgreSQL heartbeat for dashboard (red/green dot)
- Structured error events
- Interval loop for jobs; lifecycle management for services/streams
"""

from __future__ import annotations

import argparse
import os
import sys
import signal
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Tuple

# Ensure project root is on sys.path even when executed as "python3 scheduler/run_component.py"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scheduler.control import (
    acquire_component_lock,
    get_component_enabled,
    record_error_event,
    safe_capture_traceback,
    upsert_heartbeat,
)
from scheduler.component_registry import ensure_default_components_registered


HOST = socket.gethostname()


@dataclass(frozen=True)
class IntervalJobSpec:
    component_id: str
    interval_seconds: float
    run_once: Callable[[], None]


@dataclass(frozen=True)
class ManagedServiceSpec:
    component_id: str
    start: Callable[[], None]
    stop: Callable[[], None]
    is_running: Callable[[], bool]
    restart_on_crash: bool = True


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _register_default_components() -> None:
    ensure_default_components_registered()


def _interval_job_specs() -> dict[str, IntervalJobSpec]:
    # Import lazily so running one component doesn't auto-import everything.
    from scheduler.jobs import (
        # Master jobs (data ingestion)
        fetch_jupiter_prices,
        sync_trades_from_webhook,
        process_price_cycles_job,
        # Master2 jobs (trading logic)
        run_follow_the_goat,
        run_trailing_stop_seller,
        run_train_validator,
        run_update_potential_gains,
        run_create_new_patterns,
        run_create_profiles,
        run_archive_old_data,
        run_restart_quicknode_streams,
        export_job_status_to_file,
        run_recalculate_pump_filters,
        run_refresh_pump_model,
        # Standalone jobs
        run_sde_overnight_sweep,
    )

    return {
        "fetch_jupiter_prices": IntervalJobSpec("fetch_jupiter_prices", 1.0, fetch_jupiter_prices),
        "sync_trades_from_webhook": IntervalJobSpec("sync_trades_from_webhook", 1.0, sync_trades_from_webhook),
        "process_price_cycles": IntervalJobSpec("process_price_cycles", 2.0, process_price_cycles_job),
        "follow_the_goat": IntervalJobSpec("follow_the_goat", 1.0, run_follow_the_goat),
        "trailing_stop_seller": IntervalJobSpec("trailing_stop_seller", 1.0, run_trailing_stop_seller),
        "train_validator": IntervalJobSpec("train_validator", 5.0, run_train_validator),
        "update_potential_gains": IntervalJobSpec("update_potential_gains", 15.0, run_update_potential_gains),
        "create_new_patterns": IntervalJobSpec("create_new_patterns", 600.0, run_create_new_patterns),
        "create_profiles": IntervalJobSpec("create_profiles", 30.0, run_create_profiles),
        "archive_old_data": IntervalJobSpec("archive_old_data", 3600.0, run_archive_old_data),
        "restart_quicknode_streams": IntervalJobSpec("restart_quicknode_streams", 15.0, run_restart_quicknode_streams),
        "recalculate_pump_filters": IntervalJobSpec("recalculate_pump_filters", 300.0, run_recalculate_pump_filters),
        "refresh_pump_model": IntervalJobSpec("refresh_pump_model", 300.0, run_refresh_pump_model),
        "export_job_status": IntervalJobSpec("export_job_status", 5.0, export_job_status_to_file),
        # Standalone jobs
        "sde_overnight_sweep": IntervalJobSpec("sde_overnight_sweep", 43200.0, run_sde_overnight_sweep),
    }


def _service_specs() -> dict[str, ManagedServiceSpec]:
    # Service helpers from jobs.py
    import scheduler.jobs as jobs

    def webhook_start() -> None:
        jobs.start_webhook_api_in_background(host="0.0.0.0", port=8001)

    def webhook_stop() -> None:
        jobs.stop_webhook_api()

    def webhook_is_running() -> bool:
        srv = getattr(jobs, "_webhook_server", None)
        return srv is not None and getattr(srv, "should_exit", False) is False

    def php_start() -> None:
        jobs.start_php_server(host="0.0.0.0", port=8000)

    def php_stop() -> None:
        jobs.stop_php_server()

    def php_is_running() -> bool:
        proc = getattr(jobs, "_php_server_process", None)
        return proc is not None and getattr(proc, "poll", lambda: 1)() is None

    def binance_start() -> None:
        jobs.start_binance_stream_in_background(symbol="SOLUSDT", mode="conservative")

    def binance_stop() -> None:
        jobs.stop_binance_stream()

    def binance_is_running() -> bool:
        collector = getattr(jobs, "_binance_collector", None)
        return collector is not None

    def local_api_start() -> None:
        jobs.start_local_api(port=5052, host="0.0.0.0")

    def local_api_stop() -> None:
        jobs.stop_local_api()

    def local_api_is_running() -> bool:
        srv = getattr(jobs, "_local_api_server", None)
        return srv is not None and getattr(srv, "should_exit", False) is False

    return {
        "webhook_server": ManagedServiceSpec("webhook_server", webhook_start, webhook_stop, webhook_is_running),
        "php_server": ManagedServiceSpec("php_server", php_start, php_stop, php_is_running),
        "binance_stream": ManagedServiceSpec("binance_stream", binance_start, binance_stop, binance_is_running),
        "local_api_5052": ManagedServiceSpec("local_api_5052", local_api_start, local_api_stop, local_api_is_running),
    }


def run_interval_component(component_id: str, instance_id: str, spec: IntervalJobSpec) -> int:
    started_at = _utcnow()
    upsert_heartbeat(component_id, instance_id, status="running", host=HOST, pid=os.getpid(), started_at=started_at)

    next_run = time.time()
    heartbeat_every = 5.0
    last_hb = 0.0

    while True:
        now = time.time()

        # Heartbeat (even if disabled)
        if now - last_hb >= heartbeat_every:
            enabled = get_component_enabled(component_id)
            hb_status = "disabled" if enabled is False else "running"
            upsert_heartbeat(component_id, instance_id, status=hb_status, host=HOST, pid=os.getpid(), started_at=started_at)
            last_hb = now

        # Sleep until next scheduled tick
        sleep_for = max(0.0, next_run - now)
        if sleep_for > 0:
            time.sleep(min(sleep_for, 0.5))
            continue

        # Schedule next tick
        next_run = max(next_run + spec.interval_seconds, now + spec.interval_seconds)

        enabled = get_component_enabled(component_id)
        if enabled is False:
            continue

        try:
            spec.run_once()
        except Exception as e:
            tb_text = safe_capture_traceback(e)
            record_error_event(
                component_id=component_id,
                instance_id=instance_id,
                host=HOST,
                pid=os.getpid(),
                message=str(e),
                traceback_text=tb_text,
                context={"component_id": component_id, "type": "interval_job"},
            )
            upsert_heartbeat(
                component_id,
                instance_id,
                status="error",
                host=HOST,
                pid=os.getpid(),
                started_at=started_at,
                last_error_at=_utcnow(),
                last_error_message=str(e),
            )
            # Keep running; next tick will try again


def run_managed_service(component_id: str, instance_id: str, spec: ManagedServiceSpec) -> int:
    started_at = _utcnow()
    upsert_heartbeat(component_id, instance_id, status="idle", host=HOST, pid=os.getpid(), started_at=started_at)

    heartbeat_every = 5.0

    while True:
        enabled = get_component_enabled(component_id)

        if enabled is False:
            # Ensure stopped
            if spec.is_running():
                try:
                    spec.stop()
                except Exception as e:
                    record_error_event(
                        component_id=component_id,
                        instance_id=instance_id,
                        host=HOST,
                        pid=os.getpid(),
                        message=f"Stop failed: {e}",
                        traceback_text=safe_capture_traceback(e),
                        context={"component_id": component_id, "type": "service_stop"},
                    )
            upsert_heartbeat(component_id, instance_id, status="disabled", host=HOST, pid=os.getpid(), started_at=started_at)
            time.sleep(heartbeat_every)
            continue

        # Enabled: ensure running
        if not spec.is_running():
            try:
                spec.start()
            except Exception as e:
                record_error_event(
                    component_id=component_id,
                    instance_id=instance_id,
                    host=HOST,
                    pid=os.getpid(),
                    message=f"Start failed: {e}",
                    traceback_text=safe_capture_traceback(e),
                    context={"component_id": component_id, "type": "service_start"},
                )
                upsert_heartbeat(
                    component_id,
                    instance_id,
                    status="error",
                    host=HOST,
                    pid=os.getpid(),
                    started_at=started_at,
                    last_error_at=_utcnow(),
                    last_error_message=str(e),
                )
                time.sleep(heartbeat_every)
                continue

        # Running
        upsert_heartbeat(component_id, instance_id, status="running", host=HOST, pid=os.getpid(), started_at=started_at)
        time.sleep(heartbeat_every)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Follow The Goat component as a standalone service.")
    parser.add_argument("--component", dest="component_id", default=os.getenv("COMPONENT_ID"), help="Component ID to run")
    args = parser.parse_args()

    component_id = (args.component_id or "").strip()
    if not component_id:
        print("Missing --component or COMPONENT_ID env var", file=sys.stderr)
        return 2

    _register_default_components()

    instance_id = uuid.uuid4().hex

    # Acquire global singleton lock (session-level)
    lock_conn = acquire_component_lock(component_id)
    if not lock_conn:
        upsert_heartbeat(component_id, instance_id, status="locked", host=HOST, pid=os.getpid(), started_at=_utcnow())
        record_error_event(
            component_id=component_id,
            instance_id=instance_id,
            host=HOST,
            pid=os.getpid(),
            message="Another instance already holds the component lock",
            traceback_text=None,
            context={"component_id": component_id, "type": "lock"},
        )
        return 1

    shutting_down = {"flag": False}

    def _shutdown_handler(signum, frame):
        shutting_down["flag"] = True
        try:
            upsert_heartbeat(component_id, instance_id, status="idle", host=HOST, pid=os.getpid(), started_at=None)
        except Exception:
            pass
        try:
            lock_conn.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    # Dispatch to component type
    interval_specs = _interval_job_specs()
    service_specs = _service_specs()

    if component_id in interval_specs:
        return run_interval_component(component_id, instance_id, interval_specs[component_id]) or 0
    if component_id in service_specs:
        return run_managed_service(component_id, instance_id, service_specs[component_id]) or 0

    record_error_event(
        component_id=component_id,
        instance_id=instance_id,
        host=HOST,
        pid=os.getpid(),
        message=f"Unknown component_id: {component_id}",
        traceback_text=None,
        context={"component_id": component_id, "type": "dispatch"},
    )
    upsert_heartbeat(component_id, instance_id, status="error", host=HOST, pid=os.getpid(), started_at=_utcnow(), last_error_at=_utcnow(), last_error_message="Unknown component_id")
    lock_conn.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

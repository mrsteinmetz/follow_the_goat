#!/usr/bin/env python3
"""
Run the signal discovery overnight sweep with a heartbeat file so you can see it's still running.

Usage:
    python3 000trading/run_sweep_with_heartbeat.py --sweep --hours 24 --output /tmp/sde_overnight.json

While running, the script updates a heartbeat file every 30 seconds. To check progress:

    cat /tmp/sde_heartbeat.txt
    # or watch it live:
    watch -n 10 cat /tmp/sde_heartbeat.txt

The heartbeat file shows: last update time, combo progress, ETA, profitable count, status.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HEARTBEAT_FILE = os.environ.get("SDE_HEARTBEAT", "/tmp/sde_heartbeat.txt")
HEARTBEAT_INTERVAL_SEC = 30
CHECKPOINT_FILE = "/tmp/sde_overnight_checkpoint.json"


def _read_checkpoint() -> dict | None:
    try:
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _format_heartbeat(cp: dict | None, status: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if cp is None:
        return f"[{now}] status={status} (waiting for checkpoint...)\n"
    progress = cp.get("progress", "?/?")
    elapsed = cp.get("elapsed_sec", 0)
    n_results = cp.get("n_results", 0)
    try:
        a, b = progress.split("/")
        done, total = int(a), int(b)
        rate = done / elapsed if elapsed > 0 else 0
        eta_sec = (total - done) / rate if rate > 0 else 0
        eta_min = eta_sec / 60
    except Exception:
        eta_min = 0
    return (
        f"[{now}] status={status}\n"
        f"  progress={progress} | elapsed={elapsed/60:.1f}m | ETA={eta_min:.0f}m\n"
        f"  profitable_so_far={n_results}\n"
    )


def heartbeat_loop(proc: subprocess.Popen, stop: threading.Event, path: str, interval_sec: int):
    """Background thread: every interval_sec write heartbeat file."""
    while not stop.is_set() and proc.poll() is None:
        cp = _read_checkpoint()
        status = "running"
        line = _format_heartbeat(cp, status)
        try:
            with open(path, "w") as f:
                f.write(line)
        except Exception:
            pass
        stop.wait(interval_sec)

    # Final heartbeat
    exit_code = proc.poll()
    if exit_code is None:
        status = "running"
    elif exit_code == 0:
        status = "COMPLETED"
    else:
        status = f"EXITED({exit_code})"
    cp = _read_checkpoint()
    line = _format_heartbeat(cp, status)
    try:
        with open(path, "w") as f:
            f.write(line)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Run signal discovery sweep with heartbeat")
    parser.add_argument("--sweep", action="store_true", help="Run overnight sweep")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--heartbeat-file", type=str, default=HEARTBEAT_FILE,
                        help="Heartbeat file path")
    parser.add_argument("--heartbeat-interval", type=int, default=HEARTBEAT_INTERVAL_SEC,
                        help="Seconds between heartbeat updates")
    parser.add_argument("--watch", action="store_true",
                        help="Only watch checkpoint and write heartbeat (sweep runs elsewhere)")
    parser.add_argument("--engine", type=str, default=None,
                        help="Path to signal_discovery_engine.py (default: same dir as this script)")
    args = parser.parse_args()

    heartbeat_file = args.heartbeat_file
    heartbeat_interval = max(10, args.heartbeat_interval)

    if args.watch:
        # Watch-only: just poll checkpoint and write heartbeat until Ctrl+C
        print(f"Watching {CHECKPOINT_FILE}, writing to {heartbeat_file} every {heartbeat_interval}s. Ctrl+C to stop.")
        try:
            while True:
                cp = _read_checkpoint()
                status = "running" if cp else "waiting_for_checkpoint"
                with open(heartbeat_file, "w") as f:
                    f.write(_format_heartbeat(cp, status))
                time.sleep(heartbeat_interval)
        except KeyboardInterrupt:
            with open(heartbeat_file, "w") as f:
                f.write(_format_heartbeat(_read_checkpoint(), "watch_stopped"))
        sys.exit(0)

    # Resolve engine script
    script_dir = Path(__file__).resolve().parent
    engine = Path(args.engine) if args.engine else (script_dir / "signal_discovery_engine.py")
    if not engine.is_absolute():
        engine = script_dir / engine
    if not engine.exists():
        print(f"Error: {engine} not found.", file=sys.stderr)
        print("Run with --watch in a second terminal to see heartbeat while sweep runs elsewhere.", file=sys.stderr)
        sys.exit(2)

    cmd = [sys.executable, str(engine)]
    if args.sweep:
        cmd.append("--sweep")
    cmd.extend(["--hours", str(args.hours)])
    if args.output:
        cmd.extend(["--output", args.output])

    print(f"Heartbeat file: {heartbeat_file} (updates every {heartbeat_interval}s)")
    print(f"Check progress: cat {heartbeat_file}  OR  watch -n 10 cat {heartbeat_file}")
    print(f"Command: {' '.join(cmd)}")
    print()

    # Write initial heartbeat
    with open(heartbeat_file, "w") as f:
        f.write(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] status=starting\n")

    proc = subprocess.Popen(cmd, cwd=str(PROJECT_ROOT))
    stop = threading.Event()
    t = threading.Thread(target=heartbeat_loop, args=(proc, stop, heartbeat_file, heartbeat_interval))
    t.daemon = True
    t.start()

    try:
        proc.wait()
    finally:
        stop.set()
        t.join(timeout=heartbeat_interval + 5)

    with open(heartbeat_file, "w") as f:
        f.write(_format_heartbeat(_read_checkpoint(), "COMPLETED" if proc.returncode == 0 else f"FAILED(exit={proc.returncode})"))

    sys.exit(proc.returncode or 0)


if __name__ == "__main__":
    main()

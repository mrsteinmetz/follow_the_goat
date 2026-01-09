#!/usr/bin/env python3
"""Restart master.py to apply webhook fixes"""

import os
import sys
import time
import subprocess
import signal

def run_command(cmd):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return -1, "", str(e)

print("=== Restarting master.py to apply webhook fixes ===\n")

# Stop master.py
print("Stopping master.py...")
code, out, err = run_command("pkill -f 'scheduler/master.py'")
time.sleep(3)

# Verify it stopped
code, out, err = run_command("ps aux | grep 'scheduler/master.py' | grep -v grep")
if out.strip():
    print("Warning: master.py still running, force killing...")
    run_command("pkill -9 -f 'scheduler/master.py'")
    time.sleep(2)

print("✓ master.py stopped\n")

# Start master.py
print("Starting master.py...")
os.chdir('/root/follow_the_goat')
subprocess.Popen(
    ['nohup', 'venv/bin/python', 'scheduler/master.py'],
    stdout=open('logs/master.log', 'w'),
    stderr=subprocess.STDOUT,
    start_new_session=True
)

time.sleep(5)

# Check if it started
code, out, err = run_command("ps aux | grep 'scheduler/master.py' | grep -v grep")
if out.strip():
    print("✓ master.py started successfully\n")
    print("Services starting:")
    print("  - Webhook API (port 8001)")
    print("  - PHP Server (port 8000)")
    print("  - Price cycles job")
    print("  - Jupiter price fetcher\n")
    print("Recent logs:")
    print("-" * 60)
    code, out, err = run_command("tail -20 logs/master.log")
    print(out)
else:
    print("✗ ERROR: master.py failed to start")
    print("Check logs/master.log for errors\n")
    code, out, err = run_command("tail -50 logs/master.log")
    print(out)

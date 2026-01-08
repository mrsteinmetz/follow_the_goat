#!/usr/bin/env python3
"""Restart all Follow The Goat services."""
import subprocess
import sys
import time
import os

def run_cmd(cmd, check=False):
    """Run a command and return result."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        if check and result.returncode != 0:
            print(f"Error running: {cmd}")
            print(f"Output: {result.stderr}")
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        print(f"Exception running {cmd}: {e}")
        return False, "", str(e)

def stop_services():
    """Stop all services."""
    print("🛑 Stopping all services...")
    
    # Stop screen sessions
    run_cmd("screen -S master -X quit 2>/dev/null")
    run_cmd("screen -S website_api -X quit 2>/dev/null")
    run_cmd("screen -S master2 -X quit 2>/dev/null")
    
    time.sleep(2)
    
    # Kill processes
    run_cmd("pkill -f 'scheduler/master.py' 2>/dev/null")
    run_cmd("pkill -f 'scheduler/master2.py' 2>/dev/null")
    run_cmd("pkill -f 'scheduler/website_api.py' 2>/dev/null")
    run_cmd("pkill -f 'php -S' 2>/dev/null")
    
    time.sleep(1)
    print("✅ Services stopped")
    print()

def start_services():
    """Start all services."""
    print("🚀 Starting all services...")
    print()
    
    # Start master.py
    print("1️⃣  Starting master.py (Data Engine)...")
    success, out, err = run_cmd(
        "screen -dmS master bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py'"
    )
    if success:
        print("   ✅ master.py started")
    else:
        print(f"   ❌ Failed to start master.py: {err}")
    time.sleep(3)
    
    # Start website_api.py
    print("2️⃣  Starting website_api.py (Website API)...")
    success, out, err = run_cmd(
        "screen -dmS website_api bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py'"
    )
    if success:
        print("   ✅ website_api.py started")
    else:
        print(f"   ❌ Failed to start website_api.py: {err}")
    time.sleep(2)
    
    # Start master2.py
    print("3️⃣  Starting master2.py (Trading Logic)...")
    success, out, err = run_cmd(
        "screen -dmS master2 bash -c 'source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py'"
    )
    if success:
        print("   ✅ master2.py started")
    else:
        print(f"   ❌ Failed to start master2.py: {err}")
    time.sleep(2)
    
    print()

def check_status():
    """Check service status."""
    print("📋 Checking service status...")
    print()
    
    # Check screen sessions
    success, out, err = run_cmd("screen -ls 2>&1")
    if success and "master" in out:
        print("📺 Screen sessions:")
        for line in out.split('\n'):
            if 'master' in line or 'website_api' in line:
                print(f"   {line}")
    else:
        print("   ⚠️  No screen sessions found")
    print()
    
    # Check processes
    print("🔄 Running processes:")
    success, out, err = run_cmd("pgrep -af 'scheduler/master.py' 2>/dev/null")
    if success and out.strip():
        print(f"   ✅ master.py: RUNNING")
    else:
        print(f"   ❌ master.py: NOT RUNNING")
    
    success, out, err = run_cmd("pgrep -af 'scheduler/website_api.py' 2>/dev/null")
    if success and out.strip():
        print(f"   ✅ website_api.py: RUNNING")
    else:
        print(f"   ❌ website_api.py: NOT RUNNING")
    
    success, out, err = run_cmd("pgrep -af 'scheduler/master2.py' 2>/dev/null")
    if success and out.strip():
        print(f"   ✅ master2.py: RUNNING")
    else:
        print(f"   ❌ master2.py: NOT RUNNING")
    print()
    
    # Check ports
    print("🔌 Port status:")
    import socket
    ports = {
        5051: 'website_api',
        5052: 'master2',
        8000: 'PHP',
        8001: 'webhook'
    }
    for port, name in ports.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        if result == 0:
            print(f"   ✅ Port {port} ({name}): LISTENING")
        else:
            print(f"   ❌ Port {port} ({name}): NOT LISTENING")
    print()

def main():
    print("=" * 60)
    print("Follow The Goat - Service Restart")
    print("=" * 60)
    print()
    
    stop_services()
    start_services()
    check_status()
    
    print("=" * 60)
    print("✅ Restart complete!")
    print()
    print("To view logs:")
    print("  screen -r master")
    print("  screen -r website_api")
    print("  screen -r master2")
    print()
    print("To check health:")
    print("  curl http://127.0.0.1:5051/health")
    print("  curl http://127.0.0.1:8001/webhook/health")
    print("  curl http://127.0.0.1:5052/health")

if __name__ == '__main__':
    main()

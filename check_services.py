#!/usr/bin/env python3
"""Check status of all Follow The Goat services."""
import subprocess
import sys
import requests
import json

def run_cmd(cmd):
    """Run a shell command and return output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
        return result.stdout.strip(), result.returncode
    except Exception as e:
        return str(e), 1

def check_process(name):
    """Check if a process is running."""
    stdout, code = run_cmd(f"pgrep -f '{name}'")
    return len(stdout) > 0

def check_port(port):
    """Check if a port is listening."""
    stdout, code = run_cmd(f"ss -tlnp 2>/dev/null | grep ':{port}' || netstat -tlnp 2>/dev/null | grep ':{port}'")
    return len(stdout) > 0

def check_health(url):
    """Check health endpoint."""
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            try:
                data = response.json()
                return True, data.get('status', 'unknown')
            except:
                return True, f"HTTP {response.status_code}"
        return False, f"HTTP {response.status_code}"
    except Exception as e:
        return False, str(e)

print("=" * 60)
print("Follow The Goat - Service Status Check")
print("=" * 60)
print()

# Check processes
print("📋 PROCESSES:")
master_running = check_process("scheduler/master.py")
master2_running = check_process("scheduler/master2.py")
website_api_running = check_process("scheduler/website_api.py")

print(f"  master.py (Data Engine):      {'✅ RUNNING' if master_running else '❌ NOT RUNNING'}")
print(f"  master2.py (Trading Logic):   {'✅ RUNNING' if master2_running else '❌ NOT RUNNING'}")
print(f"  website_api.py (Website API): {'✅ RUNNING' if website_api_running else '❌ NOT RUNNING'}")
print()

# Check ports
print("🔌 PORTS:")
port_5051 = check_port("5051")
port_5052 = check_port("5052")
port_8000 = check_port("8000")
port_8001 = check_port("8001")

print(f"  Port 5051 (website_api): {'✅ LISTENING' if port_5051 else '❌ NOT LISTENING'}")
print(f"  Port 5052 (master2):     {'✅ LISTENING' if port_5052 else '❌ NOT LISTENING'}")
print(f"  Port 8000 (PHP):         {'✅ LISTENING' if port_8000 else '❌ NOT LISTENING'}")
print(f"  Port 8001 (webhook):     {'✅ LISTENING' if port_8001 else '❌ NOT LISTENING'}")
print()

# Check health endpoints
print("🏥 HEALTH CHECKS:")
health_5051_ok, health_5051_status = check_health("http://127.0.0.1:5051/health")
health_5052_ok, health_5052_status = check_health("http://127.0.0.1:5052/health")
health_8001_ok, health_8001_status = check_health("http://127.0.0.1:8001/webhook/health")

print(f"  Port 5051 (website_api): {'✅ ' + str(health_5051_status) if health_5051_ok else '❌ ' + str(health_5051_status)}")
print(f"  Port 5052 (master2):     {'✅ ' + str(health_5052_status) if health_5052_ok else '❌ ' + str(health_5052_status)}")
print(f"  Port 8001 (webhook):     {'✅ ' + str(health_8001_status) if health_8001_ok else '❌ ' + str(health_8001_status)}")
print()

# Check screen sessions
print("📺 SCREEN SESSIONS:")
stdout, _ = run_cmd("screen -ls 2>&1")
if "No Sockets" in stdout or "No screen" in stdout:
    print("  ❌ No screen sessions running")
else:
    lines = [l for l in stdout.split('\n') if 'master' in l or 'website_api' in l]
    if lines:
        for line in lines:
            print(f"  {line}")
    else:
        print("  ⚠️  Screen sessions exist but no master/website_api sessions found")

print()
print("=" * 60)

# Summary
all_running = master_running and master2_running and website_api_running
if all_running:
    print("✅ All services appear to be running!")
else:
    print("❌ Some services are not running!")
    print()
    print("To start services:")
    print("  bash /root/follow_the_goat/start_all.sh")
    print("  OR")
    print("  bash /root/follow_the_goat/restart_all.sh")

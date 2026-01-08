#!/usr/bin/env python3
import subprocess
import sys
import os

def check_status():
    print("=" * 60)
    print("Follow The Goat - Service Status")
    print("=" * 60)
    print()
    
    # Check systemd services
    print("📋 SYSTEMD SERVICES:")
    services = ['ftg-master', 'ftg-master2', 'ftg-website-api']
    for svc in services:
        try:
            result = subprocess.run(['systemctl', 'is-active', svc], 
                                  capture_output=True, text=True, timeout=2)
            status = result.stdout.strip()
            if status == 'active':
                print(f"  ✅ {svc}: ACTIVE")
            else:
                print(f"  ❌ {svc}: {status.upper()}")
        except:
            print(f"  ⚠️  {svc}: Unable to check")
    print()
    
    # Check processes
    print("🔄 RUNNING PROCESSES:")
    try:
        result = subprocess.run(['pgrep', '-af', 'scheduler/master.py'], 
                              capture_output=True, text=True, timeout=2)
        if result.stdout.strip():
            print(f"  ✅ master.py: RUNNING")
            for line in result.stdout.strip().split('\n'):
                if 'master.py' in line:
                    print(f"     {line[:80]}")
        else:
            print(f"  ❌ master.py: NOT RUNNING")
    except:
        print(f"  ⚠️  master.py: Unable to check")
    
    try:
        result = subprocess.run(['pgrep', '-af', 'scheduler/master2.py'], 
                              capture_output=True, text=True, timeout=2)
        if result.stdout.strip():
            print(f"  ✅ master2.py: RUNNING")
            for line in result.stdout.strip().split('\n'):
                if 'master2.py' in line:
                    print(f"     {line[:80]}")
        else:
            print(f"  ❌ master2.py: NOT RUNNING")
    except:
        print(f"  ⚠️  master2.py: Unable to check")
    
    try:
        result = subprocess.run(['pgrep', '-af', 'scheduler/website_api.py'], 
                              capture_output=True, text=True, timeout=2)
        if result.stdout.strip():
            print(f"  ✅ website_api.py: RUNNING")
            for line in result.stdout.strip().split('\n'):
                if 'website_api.py' in line:
                    print(f"     {line[:80]}")
        else:
            print(f"  ❌ website_api.py: NOT RUNNING")
    except:
        print(f"  ⚠️  website_api.py: Unable to check")
    print()
    
    # Check ports
    print("🔌 PORT STATUS:")
    ports = {
        '5051': 'website_api',
        '5052': 'master2',
        '8000': 'PHP',
        '8001': 'webhook'
    }
    for port, name in ports.items():
        try:
            result = subprocess.run(['ss', '-tlnp'], capture_output=True, text=True, timeout=2)
            if f':{port}' in result.stdout:
                print(f"  ✅ Port {port} ({name}): LISTENING")
            else:
                print(f"  ❌ Port {port} ({name}): NOT LISTENING")
        except:
            try:
                result = subprocess.run(['netstat', '-tlnp'], capture_output=True, text=True, timeout=2)
                if f':{port}' in result.stdout:
                    print(f"  ✅ Port {port} ({name}): LISTENING")
                else:
                    print(f"  ❌ Port {port} ({name}): NOT LISTENING")
            except:
                print(f"  ⚠️  Port {port} ({name}): Unable to check")
    print()
    
    # Check recent errors
    print("⚠️  RECENT ERRORS:")
    error_log = '/root/follow_the_goat/logs/scheduler_errors.log.1'
    if os.path.exists(error_log):
        try:
            with open(error_log, 'r') as f:
                lines = f.readlines()
                if lines:
                    print("  Last 3 error lines:")
                    for line in lines[-3:]:
                        print(f"     {line.strip()[:100]}")
                else:
                    print("  No errors found")
        except Exception as e:
            print(f"  Unable to read log: {e}")
    else:
        print("  No error log found")
    print()
    
    print("=" * 60)
    print("To restart services:")
    print("  sudo systemctl restart ftg-master ftg-master2 ftg-website-api")
    print("  OR")
    print("  bash /root/follow_the_goat/start_all.sh")

if __name__ == '__main__':
    check_status()

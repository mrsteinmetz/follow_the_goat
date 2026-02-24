#!/usr/bin/env python3
"""
Component Startup Script
========================
Starts all enabled components that are not currently running.

Usage:
    python3 scripts/start_missing_components.py [--dry-run]
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import time

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def get_running_components():
    """Get list of currently running component PIDs."""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True
        )
        running = {}
        for line in result.stdout.split('\n'):
            if 'run_component.py' in line and '--component' in line:
                parts = line.split()
                pid = int(parts[1])
                # Extract component name
                for i, part in enumerate(parts):
                    if part == '--component' and i + 1 < len(parts):
                        component_name = parts[i + 1]
                        running[component_name] = pid
                        break
        return running
    except Exception as e:
        print(f"Error checking running components: {e}")
        return {}

def get_enabled_components():
    """Get list of enabled components from database."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        c.component_id,
                        c.kind,
                        c.group_name,
                        COALESCE(s.enabled, c.default_enabled) as enabled
                    FROM scheduler_components c
                    LEFT JOIN scheduler_component_settings s ON c.component_id = s.component_id
                    WHERE COALESCE(s.enabled, c.default_enabled) = true
                    ORDER BY c.component_id
                """)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    except Exception as e:
        print(f"Error fetching enabled components: {e}")
        return []

def _python_cmd():
    """Use same Python as this script (e.g. venv) when starting child processes."""
    venv_py = PROJECT_ROOT / "venv" / "bin" / "python"
    if venv_py.exists():
        return str(venv_py)
    return sys.executable


def start_component(component_name, dry_run=False):
    """Start a component."""
    log_file = f"/tmp/{component_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    if dry_run:
        print(f"  [DRY-RUN] Would start: {component_name}")
        return None
    
    try:
        cmd = [
            "nohup",
            _python_cmd(),
            "scheduler/run_component.py",
            "--component",
            component_name
        ]
        
        with open(log_file, 'w') as log:
            process = subprocess.Popen(
                cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
                start_new_session=True
            )
        
        print(f"  ✓ Started {component_name} (PID: {process.pid}, log: {log_file})")
        return process.pid
    except Exception as e:
        print(f"  ✗ Failed to start {component_name}: {e}")
        return None

def main():
    dry_run = '--dry-run' in sys.argv
    
    print("=" * 80)
    print("COMPONENT STARTUP SCRIPT")
    print("=" * 80)
    
    if dry_run:
        print("\n⚠️  DRY-RUN MODE (no components will be started)")
    
    print("\n1. Checking running components...")
    running = get_running_components()
    print(f"   Found {len(running)} running components")
    
    print("\n2. Checking enabled components...")
    enabled = get_enabled_components()
    print(f"   Found {len(enabled)} enabled components")
    
    print("\n3. Starting missing components...")
    started_count = 0
    skipped_count = 0
    
    for component in enabled:
        component_name = component['component_id']
        
        if component_name in running:
            print(f"  ⏭️  {component_name} (already running, PID: {running[component_name]})")
            skipped_count += 1
        else:
            pid = start_component(component_name, dry_run)
            if pid or dry_run:
                started_count += 1
                if not dry_run:
                    time.sleep(1)  # Brief delay between starts
    
    print("\n" + "=" * 80)
    print(f"SUMMARY: Started {started_count}, Skipped {skipped_count}, Total {len(enabled)}")
    print("=" * 80)
    
    if not dry_run:
        print("\n✓ All enabled components are now running!")
        print("  Check logs in /tmp/<component_name>_*.log")

if __name__ == "__main__":
    main()

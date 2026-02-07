#!/bin/bash
# Start component via run_component.py
# Usage: ./start_component.sh <component_name>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <component_name>"
    echo ""
    echo "Available components:"
    echo "  Data Ingestion:"
    echo "    - fetch_jupiter_prices"
    echo "    - sync_trades_from_webhook"
    echo "    - process_price_cycles"
    echo "    - webhook_server"
    echo "    - php_server"
    echo "    - binance_stream"
    echo ""
    echo "  Trading Logic:"
    echo "    - follow_the_goat"
    echo "    - trailing_stop_seller"
    echo "    - train_validator"
    echo "    - update_potential_gains"
    echo "    - create_new_patterns"
    echo "    - create_profiles"
    echo "    - archive_old_data"
    echo "    - restart_quicknode_streams"
    echo "    - local_api_5052"
    echo "    - export_job_status"
    exit 1
fi

COMPONENT=$1
cd /root/follow_the_goat

# Check if component is already running
EXISTING_PID=$(ps aux | grep "run_component.py --component $COMPONENT" | grep -v grep | awk '{print $2}' | head -1)

if [ ! -z "$EXISTING_PID" ]; then
    echo "Component '$COMPONENT' is already running with PID: $EXISTING_PID"
    echo "Kill it first if you want to restart: kill $EXISTING_PID"
    exit 1
fi

# Start the component
echo "Starting $COMPONENT..."
nohup python3 scheduler/run_component.py --component $COMPONENT > /tmp/${COMPONENT}_$(date +%Y%m%d_%H%M%S).log 2>&1 &
NEW_PID=$!

echo "Started $COMPONENT with PID: $NEW_PID"
sleep 2

# Check if it's still running
if ps -p $NEW_PID > /dev/null; then
    echo "✓ Component is running"
    
    # Wait for heartbeat
    sleep 3
    echo ""
    echo "Checking heartbeat..."
    python3 <<PYTHON
from core.database import get_postgres

with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                component_id, pid, status, 
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) as age_seconds
            FROM scheduler_component_heartbeats 
            WHERE component_id = %s
            ORDER BY last_heartbeat_at DESC 
            LIMIT 1
        """, ['$COMPONENT'])
        
        result = cursor.fetchone()
        if result:
            age = result.get('age_seconds', 9999)
            status = result['status']
            if age < 30:
                print(f"✓ Heartbeat detected (age: {age:.1f}s, status: {status})")
            else:
                print(f"⚠ Heartbeat is stale (age: {age:.1f}s, status: {status})")
        else:
            print("⚠ No heartbeat found yet (may take up to 5 seconds)")
PYTHON
else
    echo "✗ Component exited - check logs at /tmp/${COMPONENT}_*.log"
    exit 1
fi

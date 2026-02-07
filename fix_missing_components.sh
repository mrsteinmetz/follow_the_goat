#!/bin/bash
# Fix webhook_server and restart_quicknode_streams components
# These need to be started via run_component.py to send proper heartbeats

set -e

cd /root/follow_the_goat

echo "=== Fixing Missing Components ==="
echo ""

# 1. Kill the manual uvicorn process (PID 1516316)
echo "1. Stopping manual uvicorn webhook server..."
if kill -0 1516316 2>/dev/null; then
    kill 1516316
    echo "   ✓ Killed PID 1516316"
    sleep 2
else
    echo "   - PID 1516316 not running"
fi

# 2. Start webhook_server via run_component.py
echo ""
echo "2. Starting webhook_server via run_component.py..."
nohup python3 scheduler/run_component.py --component webhook_server > /tmp/webhook_server_$(date +%Y%m%d_%H%M%S).log 2>&1 &
WEBHOOK_PID=$!
echo "   ✓ Started webhook_server with PID: $WEBHOOK_PID"
sleep 2

# Verify it's running
if ps -p $WEBHOOK_PID > /dev/null; then
    echo "   ✓ webhook_server is running"
else
    echo "   ✗ WARNING: webhook_server may have exited - check logs at /tmp/webhook_server_*.log"
fi

# 3. Start restart_quicknode_streams via run_component.py
echo ""
echo "3. Starting restart_quicknode_streams via run_component.py..."
nohup python3 scheduler/run_component.py --component restart_quicknode_streams > /tmp/restart_quicknode_streams_$(date +%Y%m%d_%H%M%S).log 2>&1 &
STREAMS_PID=$!
echo "   ✓ Started restart_quicknode_streams with PID: $STREAMS_PID"
sleep 2

# Verify it's running
if ps -p $STREAMS_PID > /dev/null; then
    echo "   ✓ restart_quicknode_streams is running"
else
    echo "   ✗ WARNING: restart_quicknode_streams may have exited - check logs at /tmp/restart_quicknode_streams_*.log"
fi

echo ""
echo "=== Verification ==="
echo ""

# Wait for heartbeats to appear
echo "Waiting 10 seconds for heartbeats to register..."
sleep 10

# Check heartbeat status
echo ""
python3 <<'PYTHON'
from core.database import get_postgres
import sys

with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                component_id, 
                pid, 
                status, 
                last_heartbeat_at,
                EXTRACT(EPOCH FROM (NOW() - last_heartbeat_at)) as age_seconds
            FROM scheduler_component_heartbeats 
            WHERE component_id IN ('webhook_server', 'restart_quicknode_streams')
            ORDER BY last_heartbeat_at DESC 
            LIMIT 2
        """)
        
        results = cursor.fetchall()
        
        all_good = True
        for row in results:
            age = row.get('age_seconds', 9999)
            status = row['status']
            component = row['component_id']
            
            if age < 30 and status == 'running':
                print(f"✓ {component:<30} - HEALTHY (age: {age:.1f}s, status: {status})")
            else:
                print(f"✗ {component:<30} - ISSUE (age: {age:.1f}s, status: {status})")
                all_good = False
        
        if len(results) < 2:
            print("✗ WARNING: Not all components have heartbeats yet")
            all_good = False
        
        sys.exit(0 if all_good else 1)
PYTHON

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ All components are healthy and sending heartbeats!"
else
    echo ""
    echo "✗ Some components may have issues - check the logs:"
    echo "   tail -f /tmp/webhook_server_*.log"
    echo "   tail -f /tmp/restart_quicknode_streams_*.log"
fi

echo ""
echo "=== Running Processes ==="
ps aux | grep -E "(webhook_server|restart_quicknode)" | grep run_component | grep -v grep

echo ""
echo "Done!"

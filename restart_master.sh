#!/bin/bash
# Restart master.py to apply webhook fixes

echo "=== Restarting master.py to apply webhook fixes ==="

# Stop master.py
echo "Stopping master.py..."
pkill -f "scheduler/master.py"
sleep 3

# Verify it stopped
if ps aux | grep -v grep | grep "scheduler/master.py" > /dev/null; then
    echo "Warning: master.py still running, force killing..."
    pkill -9 -f "scheduler/master.py"
    sleep 2
fi

echo "✓ master.py stopped"

# Start master.py
echo "Starting master.py..."
cd /root/follow_the_goat
nohup venv/bin/python scheduler/master.py > logs/master.log 2>&1 &

sleep 5

# Check if it started
if ps aux | grep -v grep | grep "scheduler/master.py" > /dev/null; then
    echo "✓ master.py started successfully"
    echo ""
    echo "Services starting:"
    echo "  - Webhook API (port 8001)"
    echo "  - PHP Server (port 8000)"
    echo "  - Price cycles job"
    echo "  - Jupiter price fetcher"
    echo ""
    echo "Checking recent logs..."
    tail -20 logs/master.log
else
    echo "✗ ERROR: master.py failed to start"
    echo "Check logs/master.log for errors"
    tail -50 logs/master.log
fi

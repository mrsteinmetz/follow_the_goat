#!/bin/bash
# Restart master.py to apply the corrupted cycle fix

echo "=========================================="
echo "Restarting master.py with cycle fix"
echo "=========================================="

# Stop master.py
echo "1. Stopping master.py..."
pkill -f "python.*scheduler/master.py"
sleep 2

# Start master.py
echo "2. Starting master.py..."
cd /root/follow_the_goat
nohup python3 scheduler/master.py > /tmp/master.log 2>&1 &

sleep 3

# Check if it started
if curl -s http://localhost:5050/health > /dev/null 2>&1; then
    echo "✓ master.py started successfully"
    echo "  API responding on port 5050"
else
    echo "⚠ master.py may still be starting..."
    echo "  Check logs: tail -f /tmp/master.log"
fi

echo ""
echo "The fix will:"
echo "  1. Delete corrupted cycles (end_time < start_time)"
echo "  2. Prevent future corrupted cycles with validation"
echo "  3. Allow profiles to be created once cycles complete naturally"
echo ""
echo "Monitor: tail -f /tmp/master.log | grep -i 'corrupted\|cycle'"
echo "=========================================="


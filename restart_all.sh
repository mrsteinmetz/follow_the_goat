#!/bin/bash
# Restart all Follow The Goat services

echo "Stopping all services..."
pkill -f 'scheduler/master.py'
pkill -f 'scheduler/master2.py'
pkill -f 'scheduler/website_api.py'
pkill -f 'php -S'
sleep 2

echo "Starting master.py (Data Engine)..."
screen -dmS master bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py"

sleep 3

echo "Starting website_api.py (Website API)..."
screen -dmS website_api bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py"

sleep 2

echo "Starting master2.py (Trading Logic)..."
screen -dmS master2 bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py"

sleep 5

echo ""
echo "=== Service Status ==="
screen -ls

echo ""
echo "=== Health Checks ==="
echo -n "Port 5051 (website_api): "
curl -s http://127.0.0.1:5051/health 2>/dev/null | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('status', 'ERROR'))" 2>/dev/null || echo "NOT RESPONDING"

echo -n "Port 5052 (master2): "
curl -s http://127.0.0.1:5052/health 2>/dev/null | head -1 || echo "NOT RESPONDING"

echo -n "Port 8001 (webhook): "
curl -s http://127.0.0.1:8001/webhook/health 2>/dev/null | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('status', 'ERROR'))" 2>/dev/null || echo "NOT RESPONDING"

echo ""
echo "Done! Services restarted."
echo "To view logs: screen -r master (or website_api, master2)"

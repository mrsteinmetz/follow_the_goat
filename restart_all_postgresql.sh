#!/bin/bash
# Restart all services with new PostgreSQL versions

set -e

echo "=============================================="
echo "Restarting All Services - PostgreSQL Version"
echo "=============================================="
echo ""

# Stop all old processes
echo "Step 1: Stopping old services..."
pkill -9 -f "python.*scheduler/master.py" 2>&1 || true
pkill -9 -f "python.*scheduler/master2.py" 2>&1 || true  
pkill -9 -f "python.*scheduler/website_api.py" 2>&1 || true
sleep 3
echo "✓ Old services stopped"
echo ""

# Clean up old screen sessions
echo "Step 2: Cleaning up screen sessions..."
screen -wipe 2>&1 || true
echo "✓ Screen sessions cleaned"
echo ""

# Start master.py (data ingestion)
echo "Step 3: Starting master.py (Data Ingestion)..."
cd /root/follow_the_goat
screen -dmS master bash -c "source venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py"
sleep 3
echo "✓ master.py started"
echo ""

# Start master2.py (trading logic)
echo "Step 4: Starting master2.py (Trading Logic)..."
screen -dmS master2 bash -c "source venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py"
sleep 5
echo "✓ master2.py started"
echo ""

# Start website_api.py
echo "Step 5: Starting website_api.py (Website API)..."
screen -dmS website_api bash -c "source venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py"
sleep 3
echo "✓ website_api.py started"
echo ""

# Verify services
echo "Step 6: Verifying services..."
echo ""
ps aux | grep -E "python.*scheduler/(master|master2|website_api)\.py" | grep -v grep
echo ""

# Test health endpoints
echo "Step 7: Testing health endpoints..."
echo ""

echo "Testing master2.py (port 5052)..."
sleep 3
curl -s http://localhost:5052/health 2>&1 | python3 -m json.tool || echo "  ⚠ Not responding yet (may need more time)"
echo ""

echo "Testing website_api.py (port 5051)..."
curl -s http://localhost:5051/health 2>&1 | python3 -m json.tool || echo "  ⚠ Not responding yet (may need more time)"
echo ""

echo "=============================================="
echo "Restart Complete!"
echo "=============================================="
echo ""
echo "Check logs:"
echo "  tail -f logs/scheduler_errors.log"
echo "  tail -f logs/scheduler2_errors.log"
echo ""
echo "Check screen sessions:"
echo "  screen -list"
echo "  screen -r master"
echo "  screen -r master2"
echo "  screen -r website_api"

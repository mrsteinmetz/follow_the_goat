#!/bin/bash
# ================================================================
# FOLLOW THE GOAT - STOP ALL SERVICES
# ================================================================

echo "🛑 Stopping Follow The Goat Services..."
echo ""

# Stop screen sessions
echo "Stopping master.py..."
screen -S master -X quit 2>/dev/null

echo "Stopping website_api.py..."
screen -S website_api -X quit 2>/dev/null

echo "Stopping master2.py..."
screen -S master2 -X quit 2>/dev/null

sleep 2

# Force kill any remaining processes
echo "Cleaning up any remaining processes..."
pkill -f 'scheduler/master.py' 2>/dev/null
pkill -f 'scheduler/master2.py' 2>/dev/null
pkill -f 'scheduler/website_api.py' 2>/dev/null
pkill -f 'php -S' 2>/dev/null

# Free up ports if still in use
fuser -k 5050/tcp 5051/tcp 8000/tcp 8001/tcp 2>/dev/null

echo ""
echo "✅ All services stopped!"
echo ""
echo "📋 Remaining screen sessions:"
screen -ls 2>/dev/null || echo "  None"


#!/bin/bash
# ================================================================
# FOLLOW THE GOAT - QUICK START SCRIPT FOR LINUX/UBUNTU
# ================================================================
# This script helps you start all services with screen sessions
# Usage: bash start_all.sh

echo "🐐 Starting Follow The Goat Services..."
echo ""

# Check if virtual environment exists
if [ ! -d "/root/follow_the_goat/venv" ]; then
    echo "❌ Virtual environment not found at /root/follow_the_goat/venv"
    echo "Please run: cd /root/follow_the_goat && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Kill existing screens if they exist
screen -S master -X quit 2>/dev/null
screen -S website_api -X quit 2>/dev/null
screen -S master2 -X quit 2>/dev/null

sleep 2

echo "1️⃣  Starting Data Engine (master.py)..."
screen -dmS master bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master.py"
sleep 3

echo "2️⃣  Starting Website API (website_api.py)..."
screen -dmS website_api bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/website_api.py"
sleep 2

echo "3️⃣  Starting Trading Logic (master2.py)..."
screen -dmS master2 bash -c "source /root/follow_the_goat/venv/bin/activate && cd /root/follow_the_goat && python scheduler/master2.py"
sleep 2

echo ""
echo "✅ All services started in screen sessions!"
echo ""
echo "📋 Screen sessions:"
screen -ls
echo ""
echo "🔍 View logs:"
echo "  screen -r master       # Detach with Ctrl+A then D"
echo "  screen -r website_api  # Detach with Ctrl+A then D"
echo "  screen -r master2      # Detach with Ctrl+A then D"
echo ""
echo "🌐 Check status:"
echo "  Data Engine:  curl http://127.0.0.1:5050/health"
echo "  Website API:  curl http://127.0.0.1:5051/health"
echo "  Website:      http://195.201.84.5"
echo ""
echo "🛑 To stop all:"
echo "  bash stop_all.sh"


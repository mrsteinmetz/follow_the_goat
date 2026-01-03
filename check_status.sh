#!/bin/bash
# ================================================================
# FOLLOW THE GOAT - CHECK STATUS OF ALL SERVICES
# ================================================================

echo "🐐 Follow The Goat - System Status Check"
echo "=========================================="
echo ""

# Check screen sessions
echo "📺 Screen Sessions:"
screen -ls 2>/dev/null || echo "  ❌ No screen sessions running"
echo ""

# Check processes
echo "🔄 Running Processes:"
ps aux | grep -E "scheduler/(master|website_api|master2)" | grep -v grep || echo "  ❌ No Python schedulers running"
echo ""

# Check ports
echo "🔌 Port Status:"
echo -n "  Port 80 (Nginx):      "
netstat -tln | grep -q ":80 " && echo "✅ LISTENING" || echo "❌ NOT LISTENING"

echo -n "  Port 5050 (Master):   "
netstat -tln | grep -q ":5050 " && echo "✅ LISTENING" || echo "❌ NOT LISTENING"

echo -n "  Port 5051 (Web API):  "
netstat -tln | grep -q ":5051 " && echo "✅ LISTENING" || echo "❌ NOT LISTENING"

echo -n "  Port 8000 (PHP Dev):  "
netstat -tln | grep -q ":8000 " && echo "✅ LISTENING" || echo "❌ NOT LISTENING"

echo -n "  Port 8001 (Webhook):  "
netstat -tln | grep -q ":8001 " && echo "✅ LISTENING" || echo "❌ NOT LISTENING"
echo ""

# Check API health
echo "🏥 API Health Checks:"

echo -n "  Data Engine (5050):   "
if curl -s -f http://127.0.0.1:5050/health > /dev/null 2>&1; then
    echo "✅ HEALTHY"
else
    echo "❌ NOT RESPONDING"
fi

echo -n "  Website API (5051):   "
if curl -s -f http://127.0.0.1:5051/health > /dev/null 2>&1; then
    echo "✅ HEALTHY"
else
    echo "❌ NOT RESPONDING"
fi

echo -n "  Webhook (8001):       "
if curl -s -f http://127.0.0.1:8001/webhook/health > /dev/null 2>&1; then
    echo "✅ HEALTHY"
else
    echo "❌ NOT RESPONDING"
fi

echo -n "  Nginx Website:        "
if curl -s -f http://127.0.0.1/ > /dev/null 2>&1; then
    echo "✅ HEALTHY"
else
    echo "❌ NOT RESPONDING"
fi
echo ""

# Check Nginx
echo "🌐 Nginx Status:"
if systemctl is-active --quiet nginx; then
    echo "  ✅ Nginx is running"
    echo "  Public URL: http://195.201.84.5"
else
    echo "  ❌ Nginx is not running"
    echo "  Start with: sudo systemctl start nginx"
fi
echo ""

# Show disk usage
echo "💾 Disk Usage (project folder):"
du -sh /root/follow_the_goat 2>/dev/null || echo "  Unable to check"
echo ""

# Recent errors
echo "⚠️  Recent Errors (last 5 lines):"
if [ -f "/root/follow_the_goat/logs/scheduler_errors.log.1" ]; then
    tail -5 /root/follow_the_goat/logs/scheduler_errors.log.1 2>/dev/null || echo "  No recent errors"
else
    echo "  No error log found"
fi
echo ""

echo "=========================================="
echo "For detailed logs, use: screen -r <session>"
echo "  screen -r master"
echo "  screen -r website_api"
echo "  screen -r master2"


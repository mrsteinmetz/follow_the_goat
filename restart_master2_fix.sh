#!/bin/bash
# Fix for auto-generator not running

echo "================================================================================"
echo " RESTARTING MASTER2.PY TO FIX AUTO-GENERATOR"
echo "================================================================================"

cd /root/follow_the_goat

# Get current PID
CURRENT_PID=$(ps aux | grep "python3 scheduler/master2.py" | grep -v grep | awk '{print $2}')

if [ -n "$CURRENT_PID" ]; then
    echo "1. Stopping current master2.py (PID: $CURRENT_PID)..."
    kill $CURRENT_PID
    sleep 3
    
    # Force kill if still running
    if ps -p $CURRENT_PID > /dev/null 2>&1; then
        echo "   Force killing..."
        kill -9 $CURRENT_PID
        sleep 1
    fi
    
    echo "   ✅ Stopped"
else
    echo "1. No running master2.py found"
fi

# Clean lock
echo "2. Cleaning lock file..."
rm -f scheduler/master2.lock
echo "   ✅ Lock removed"

# Start master2
echo "3. Starting master2.py..."
nohup python3 scheduler/master2.py > logs/master2_startup.log 2>&1 &
NEW_PID=$!
sleep 2

# Verify it started
if ps -p $NEW_PID > /dev/null 2>&1; then
    echo "   ✅ Master2.py started (PID: $NEW_PID)"
else
    echo "   ❌ Failed to start master2.py"
    echo "   Check logs: tail -f logs/master2_startup.log"
    exit 1
fi

echo ""
echo "4. Waiting 30 seconds for auto-generator to run..."
sleep 30

# Check if it ran
echo "5. Verifying auto-generator executed..."
python3 << 'PYEOF'
from core.database import get_postgres
from datetime import datetime

with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT created_at 
            FROM filter_reference_suggestions 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            last = result['created_at'].replace(tzinfo=None)
            now = datetime.now()
            mins = (now - last).total_seconds() / 60
            
            print(f"   Last run: {last}")
            print(f"   Minutes ago: {mins:.1f}")
            
            if mins < 2:
                print("\n   ✅ SUCCESS! Auto-generator is running!")
            else:
                print(f"\n   ⚠️  WARNING: Last run was {mins:.1f} minutes ago")
                print("   It should have run immediately on startup")
                print("   Check logs: tail -f logs/master2_startup.log | grep pattern")
PYEOF

echo ""
echo "================================================================================"
echo " RESTART COMPLETE"
echo "================================================================================"
echo ""
echo "Next steps:"
echo "  • Wait 25 minutes and verify it runs again"
echo "  • Monitor: tail -f logs/master2_startup.log | grep pattern"
echo "  • Check status: ps aux | grep master2.py"
echo ""

#!/bin/bash
# Monitor wallet profile creation progress

echo "=========================================="
echo "Wallet Profile Creation Monitor"
echo "=========================================="
echo ""

# Check current price vs cycle highs
echo "Current Cycle Status:"
curl -s "http://127.0.0.1:5050/query" -H "Content-Type: application/json" -d '{
  "sql": "SELECT 
    threshold,
    highest_price_reached,
    (SELECT price FROM prices WHERE token = '\''SOL'\'' ORDER BY ts DESC LIMIT 1) as current_price,
    ROUND(((highest_price_reached - (SELECT price FROM prices WHERE token = '\''SOL'\'' ORDER BY ts DESC LIMIT 1)) / highest_price_reached * 100), 3) as drop_pct
  FROM cycle_tracker 
  WHERE cycle_end_time IS NULL 
  GROUP BY threshold, highest_price_reached
  ORDER BY threshold 
  LIMIT 1"
}' | python3 -c "
import json, sys
data = json.load(sys.stdin)
if data['results']:
    r = data['results'][0]
    print(f\"  Threshold: {r['threshold']}%\")
    print(f\"  Highest: \${r['highest_price_reached']:.4f}\")
    print(f\"  Current: \${r['current_price']:.4f}\")
    print(f\"  Drop:    {r['drop_pct']:.3f}%\")
    print(f\"  Need:    {r['threshold']}% drop to complete\")
    if float(r['drop_pct']) >= float(r['threshold']):
        print(f\"  🎯 Ready to complete!\")
    else:
        need_more = float(r['threshold']) - float(r['drop_pct'])
        print(f\"  ⏳ Need {need_more:.3f}% more drop\")
"

echo ""
echo "Completed Cycles:"
COMPLETED=$(curl -s "http://127.0.0.1:5052/query" -H "Content-Type: application/json" -d '{"sql": "SELECT COUNT(*) as cnt FROM cycle_tracker WHERE cycle_end_time IS NOT NULL AND cycle_end_time >= cycle_start_time"}' | python3 -c "import json, sys; print(json.load(sys.stdin)['results'][0]['cnt'])")
echo "  Valid completed cycles: $COMPLETED"

if [ "$COMPLETED" -gt 0 ]; then
    echo ""
    echo "Wallet Profile Stats:"
    curl -s http://127.0.0.1:5052/profiles/stats | python3 -c "
import json, sys
data = json.load(sys.stdin)
stats = data['stats']
print(f\"  Total profiles: {stats['total_profiles']}\")
print(f\"  Unique wallets: {stats['unique_wallets']}\")
print(f\"  Unique cycles: {stats['unique_cycles']}\")
if stats['total_invested']:
    print(f\"  Total invested: \${stats['total_invested']:.2f}\")
"
    
    if [ "$COMPLETED" -gt 0 ]; then
        echo ""
        echo "✅ Cycles completed! Profiles should be creating now."
        echo "   Check: http://195.201.84.5/pages/profiles/"
    fi
else
    echo ""
    echo "⏳ Waiting for cycles to complete naturally..."
    echo "   Once price drops 0.2-0.5% from peak, profiles will appear."
fi

echo ""
echo "=========================================="
echo "Monitor: watch -n 5 ./monitor_profiles.sh"
echo "=========================================="


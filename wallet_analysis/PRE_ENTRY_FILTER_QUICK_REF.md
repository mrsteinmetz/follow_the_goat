# Pre-Entry Filter Quick Reference

## ⚠️ CRITICAL: This filter prevents entering trades on falling prices

## Current Configuration
- **Threshold**: Price must be up **+0.20%** in the 3 minutes before entry
- **Window**: 3 minutes (optimal for SOL's fast cycles)
- **Fail-safe**: REJECT trade if filter fails or throws error

## How to Check if Filter is Active

### Method 1: Test with Python
```bash
cd /root/follow_the_goat
python3 -c "
import sys
sys.path.insert(0, '000trading')
from pattern_validator import PRE_ENTRY_AVAILABLE
print(f'Pre-entry filter: {\"ACTIVE ✓\" if PRE_ENTRY_AVAILABLE else \"DISABLED ✗\"}')"
```

### Method 2: Check Recent Trades
```bash
cd /root/follow_the_goat && python3 << 'PYEOF'
from core.database import get_postgres

with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                id,
                followed_at,
                pattern_validator_log::json->>'validator_version' as version,
                pattern_validator_log::json ? 'pre_entry_metrics' as has_pre_entry
            FROM follow_the_goat_buyins
            WHERE followed_at >= NOW() - INTERVAL '1 hour'
            ORDER BY followed_at DESC
            LIMIT 10
        """)
        results = cursor.fetchall()
        
        with_pre_entry = sum(1 for r in results if r['has_pre_entry'])
        total = len(results)
        
        print(f"Last {total} trades: {with_pre_entry} have pre-entry checks")
        if with_pre_entry < total * 0.5:
            print("⚠️  WARNING: Less than 50% of trades have pre-entry checks!")
PYEOF
```

### Method 3: Test with Specific Trade
```bash
cd /root/follow_the_goat
python3 000trading/pre_entry_price_movement.py <BUYIN_ID>
```

## What to Look For in Logs

### ✅ Good Signs
```
INFO:pattern_validator:Validating buy-in signal #...
INFO:pattern_validator:✓ Buyin #... passes pre-entry filter: PASS
```
or
```
INFO:pattern_validator:✗ Buyin #... REJECTED by pre-entry filter: FALLING_PRICE (change_3m=-0.3%)
```

### ⚠️ Warning Signs
```
WARNING:pattern_validator:⚠️  Pre-entry filter is DISABLED for buyin #... - trade may enter on falling price!
```

### 🚨 Critical Issues
```
ERROR:pattern_validator:⚠️  CRITICAL: pre_entry_price_movement module not available - pre-entry filtering disabled!
ERROR:pattern_validator:⚠️  CRITICAL ERROR in pre-entry check for buyin #...
```

## Quick Health Check Script

Save as `/root/follow_the_goat/scripts/check_pre_entry_health.py`:

```python
#!/usr/bin/env python3
"""Quick health check for pre-entry filter."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from pattern_validator import PRE_ENTRY_AVAILABLE
from core.database import get_postgres

def main():
    print("=" * 60)
    print("PRE-ENTRY FILTER HEALTH CHECK")
    print("=" * 60)
    
    # Check 1: Module available
    print(f"\n1. Module Status: ", end="")
    if PRE_ENTRY_AVAILABLE:
        print("✅ ACTIVE")
    else:
        print("❌ DISABLED")
        print("   ACTION REQUIRED: Check import errors in logs!")
        return False
    
    # Check 2: Recent trades
    print(f"\n2. Recent Trades: ", end="")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN pattern_validator_log::json ? 'pre_entry_metrics' THEN 1 ELSE 0 END) as with_pre_entry
                    FROM follow_the_goat_buyins
                    WHERE followed_at >= NOW() - INTERVAL '1 hour'
                """)
                result = cursor.fetchone()
                
                if result:
                    total = result['total']
                    with_pre_entry = result['with_pre_entry']
                    
                    if total == 0:
                        print("⚠️  No trades in last hour")
                    else:
                        pct = (with_pre_entry / total) * 100
                        print(f"{with_pre_entry}/{total} ({pct:.0f}%)")
                        
                        if pct < 50:
                            print("   ⚠️  WARNING: Less than 50% have pre-entry checks!")
                            return False
                        elif pct < 100:
                            print("   ⚠️  WARNING: Not all trades have pre-entry checks!")
                        else:
                            print("   ✅ All trades have pre-entry checks")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False
    
    print(f"\n" + "=" * 60)
    print("RESULT: ✅ Pre-entry filter is HEALTHY")
    print("=" * 60)
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
```

## Emergency: If Filter is Not Working

### Step 1: Check if module is importable
```bash
python3 -c "from 000trading.pre_entry_price_movement import calculate_pre_entry_metrics; print('✓ OK')"
```

### Step 2: Restart the validator component
```bash
# Find the process
ps aux | grep train_validator

# Restart it
python3 scheduler/run_component.py --component train_validator
```

### Step 3: Check for recent code changes
```bash
git log -5 --oneline -- 000trading/pattern_validator.py 000trading/pre_entry_price_movement.py
```

### Step 4: Review error logs
```bash
tail -100 logs/train_validator.log | grep -i "pre.entry\|import\|error"
```

## Monitoring Recommendation

Add to crontab to run every 5 minutes:
```bash
*/5 * * * * cd /root/follow_the_goat && python3 scripts/check_pre_entry_health.py || echo "⚠️  PRE-ENTRY FILTER DOWN" | mail -s "ALERT: Pre-Entry Filter" admin@example.com
```

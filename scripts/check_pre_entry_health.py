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
                        SUM(CASE WHEN pattern_validator_log::jsonb ? 'pre_entry_metrics' THEN 1 ELSE 0 END) as with_pre_entry
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

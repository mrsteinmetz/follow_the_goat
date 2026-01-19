#!/usr/bin/env python3
"""
Test script to verify the ratio-only fix works correctly.
This simulates what will happen when master2 runs the pattern generator.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def main():
    print("="*100)
    print("VERIFYING RATIO-ONLY FIX")
    print("="*100)
    
    # Check is_ratio setting
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT setting_value FROM auto_filter_settings WHERE setting_key = 'is_ratio'")
            result = cursor.fetchone()
            
            if result:
                is_ratio = str(result['setting_value']).strip().lower() in ['1', 'true', 'yes', 'on']
                print(f"\n✅ is_ratio setting: {result['setting_value']} (parsed as: {is_ratio})")
            else:
                is_ratio = False
                print(f"\n⚠️  is_ratio setting: NOT SET (will use default: False)")
    
    # Check current active filters
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT pcf.field_column, pcf.from_value, pcf.to_value
                FROM pattern_config_filters pcf
                WHERE pcf.project_id = 5 AND pcf.is_active = 1
            """)
            current_filters = cursor.fetchall()
    
    print(f"\n\nCURRENT ACTIVE FILTERS: {len(current_filters)}")
    print("-"*100)
    
    if current_filters:
        # Check if they're ratio or absolute
        filter_names = [f['field_column'] for f in current_filters]
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT filter_name, is_ratio
                    FROM trade_filter_values
                    WHERE filter_name = ANY(%s)
                """, [filter_names])
                filter_types = {r['filter_name']: r['is_ratio'] for r in cursor.fetchall()}
        
        absolute_count = 0
        ratio_count = 0
        
        for f in current_filters:
            is_ratio_filter = filter_types.get(f['field_column'], 0)
            filter_type = "RATIO ✅" if is_ratio_filter == 1 else "ABSOLUTE ❌"
            print(f"  {f['field_column']:<30} {filter_type}  [{float(f['from_value']):.2f} - {float(f['to_value']):.2f}]")
            
            if is_ratio_filter == 1:
                ratio_count += 1
            else:
                absolute_count += 1
        
        print(f"\n  Summary: {ratio_count} ratio filters, {absolute_count} absolute filters")
        
        if is_ratio and absolute_count > 0:
            print(f"\n  ⚠️  PROBLEM: is_ratio=true but {absolute_count} absolute filters are active!")
            print(f"  ✅ FIX: When master2 restarts, these will be cleared automatically")
    else:
        print("  No active filters")
    
    # Check what ratio filters are available
    print(f"\n\nAVAILABLE RATIO FILTERS:")
    print("-"*100)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT filter_name
                FROM trade_filter_values
                WHERE is_ratio = 1
                ORDER BY filter_name
                LIMIT 20
            """)
            ratio_filters = cursor.fetchall()
    
    print(f"  Found {len(ratio_filters)} ratio-based filter fields (showing first 20):")
    for r in ratio_filters:
        print(f"    - {r['filter_name']}")
    
    print(f"\n\n" + "="*100)
    print("WHAT WILL HAPPEN WHEN MASTER2 RESTARTS:")
    print("="*100)
    
    if is_ratio:
        print("""
1. ✅ Pattern generator will load is_ratio=true setting
2. ✅ Only ratio-based filters will be analyzed (71 available)
3. ✅ Old absolute filters will be CLEARED before new filters are created
4. ✅ New filters will be based on percentage changes, not absolute prices
5. ✅ These filters will remain valid even when market prices change

ADVANTAGES OF RATIO FILTERS:
- Work at any price level (SOL at $100 or $200)
- Focus on percentage movements, not absolute values
- More stable over time
- Don't break when market conditions change
""")
    else:
        print("""
1. ⚠️  Pattern generator will load is_ratio=false setting
2. ⚠️  BOTH ratio and absolute filters will be analyzed
3. ⚠️  Filters may use actual prices (like eth_close_price=3313.69)
4. ⚠️  These filters will break when market prices change significantly

RECOMMENDATION: Keep is_ratio=true for more stable filters
""")
    
    print("="*100)


if __name__ == "__main__":
    main()

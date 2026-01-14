#!/usr/bin/env python3
"""
Apply Filter Improvements
==========================
Applies the recommended filter optimizations to the database.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def apply_improvements():
    """Apply filter optimization improvements."""
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                print("Step 1: Updating percentile settings...")
                
                # Update percentile settings
                cursor.execute("""
                    UPDATE auto_filter_settings 
                    SET setting_value = '5', updated_at = CURRENT_TIMESTAMP
                    WHERE setting_key = 'percentile_low'
                """)
                
                cursor.execute("""
                    UPDATE auto_filter_settings 
                    SET setting_value = '95', updated_at = CURRENT_TIMESTAMP
                    WHERE setting_key = 'percentile_high'
                """)
                
                print("✓ Percentile settings updated to 5-95")
                
                print("\nStep 2: Replacing AutoFilters with optimized filters...")
                
                # Clear existing AutoFilters
                cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = 5")
                print("✓ Cleared existing AutoFilters")
                
                # Insert top 3 performing filters
                filters = [
                    (5001, 5, 'Auto: ob_volume_imbalance', 'order_book', 11, 'volume_imbalance', 
                     'ob_volume_imbalance', -0.571749, 0.251451, 0, 1),
                    (5002, 5, 'Auto: tx_whale_volume_pct', 'transactions', 8, 'whale_volume_pct', 
                     'tx_whale_volume_pct', 9.607326, 56.898327, 0, 1),
                    (5003, 5, 'Auto: ob_depth_imbalance_ratio', 'order_book', 11, 'depth_imbalance_ratio', 
                     'ob_depth_imbalance_ratio', 0.270676, 1.709850, 0, 1)
                ]
                
                for f in filters:
                    cursor.execute("""
                        INSERT INTO pattern_config_filters 
                        (id, project_id, name, section, minute, field_name, field_column, 
                         from_value, to_value, include_null, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, f)
                    print(f"✓ Added filter: {f[2]}")
                
                conn.commit()
                
                print("\n" + "="*80)
                print("VERIFICATION")
                print("="*80)
                
                # Verify percentile settings
                cursor.execute("""
                    SELECT setting_key, setting_value 
                    FROM auto_filter_settings 
                    WHERE setting_key IN ('percentile_low', 'percentile_high')
                    ORDER BY setting_key
                """)
                
                print("\nPercentile Settings:")
                for row in cursor.fetchall():
                    print(f"  {row['setting_key']}: {row['setting_value']}")
                
                # Verify filters
                cursor.execute("""
                    SELECT id, name, section, minute, field_column, 
                           from_value, to_value 
                    FROM pattern_config_filters 
                    WHERE project_id = 5 AND is_active = 1
                    ORDER BY minute, id
                """)
                
                print("\nActive AutoFilters:")
                for row in cursor.fetchall():
                    print(f"  [{row['minute']:2d}] {row['name']:40s} "
                          f"[{row['from_value']:10.6f} to {row['to_value']:10.6f}]")
                
                print("\n" + "="*80)
                print("SUCCESS! Filter improvements applied.")
                print("="*80)
                print("\nExpected improvements:")
                print("  - Good trade capture: 53.4% → 90.0% (+36.6%)")
                print("  - Missed opportunities: 46.6% → 10.0% (-36.6%)")
                print("\nNext: Monitor trades over next 24 hours to confirm improvement.")
                
    except Exception as e:
        print(f"ERROR: Failed to apply improvements: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    apply_improvements()

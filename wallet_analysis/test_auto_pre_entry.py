"""
Test Auto Pre-Entry Filter Integration
=======================================
Checks if pattern generator is learning pre-entry thresholds from historical data.

This script verifies:
1. Trail data has pre-entry columns
2. Pattern generator created pre-entry filters
3. Filters are stored in pattern_config_filters
4. Thresholds are reasonable (0.15-0.30% range)
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from datetime import datetime

def main():
    print("=" * 80)
    print("AUTO PRE-ENTRY FILTER INTEGRATION TEST")
    print("=" * 80)
    
    # Test 1: Check trail data has pre-entry columns
    print("\n[Test 1] Checking trail data structure...")
    print("-" * 80)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'buyin_trail_minutes'
                  AND column_name LIKE 'pre_entry%'
                ORDER BY column_name
            """)
            pre_entry_columns = cursor.fetchall()
    
    if pre_entry_columns:
        print(f"✓ Found {len(pre_entry_columns)} pre-entry columns in buyin_trail_minutes:")
        for col in pre_entry_columns:
            print(f"  - {col['column_name']}")
        
        # Check if data exists
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM buyin_trail_minutes
                    WHERE pre_entry_change_3m IS NOT NULL
                """)
                count = cursor.fetchone()['count']
        
        print(f"\n  Data availability: {count} records with pre_entry_change_3m")
        
        if count == 0:
            print("  ⚠️  WARNING: No pre-entry data populated yet!")
            print("  Trails need to be regenerated to include pre-entry metrics")
    else:
        print("✗ NO pre-entry columns found in buyin_trail_minutes!")
        print("  The schema needs to be updated")
        return
    
    # Test 2: Check for pre-entry filters in pattern_config_filters
    print("\n[Test 2] Checking for auto-generated pre-entry filters...")
    print("-" * 80)
    
    # Find auto-filters project
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, description
                FROM pattern_config_projects
                WHERE name LIKE '%AutoFilter%' OR name LIKE '%Auto%'
                ORDER BY id DESC
                LIMIT 1
            """)
            project = cursor.fetchone()
    
    if not project:
        print("✗ No AutoFilters project found!")
        print("  Pattern generator may not have run yet")
        return
    
    project_id = project['id']
    project_name = project['name']
    
    print(f"Found project: {project_name} (ID: {project_id})")
    
    # Check for pre-entry filters
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    name,
                    section,
                    field_name,
                    field_column,
                    from_value,
                    to_value,
                    is_active,
                    created_at
                FROM pattern_config_filters
                WHERE project_id = %s
                  AND (section = 'pre_entry' OR field_name LIKE 'pre_entry%')
                ORDER BY field_name
            """, [project_id])
            pre_entry_filters = cursor.fetchall()
    
    if pre_entry_filters:
        print(f"\n✓ Found {len(pre_entry_filters)} pre-entry filters:")
        for f in pre_entry_filters:
            status = "ACTIVE" if f['is_active'] else "INACTIVE"
            print(f"\n  Filter: {f['name']}")
            print(f"    Field: {f['field_name']}")
            print(f"    Threshold: >= {f['from_value']:.6f}")
            print(f"    Status: {status}")
            print(f"    Created: {f['created_at']}")
            
            # Validate threshold is reasonable
            if f['field_name'] == 'pre_entry_change_3m':
                threshold = float(f['from_value'])
                if 0.15 <= threshold <= 0.30:
                    print(f"    ✓ Threshold is reasonable ({threshold:.3f}%)")
                elif threshold < 0.08:
                    print(f"    ⚠️  Threshold very low ({threshold:.3f}%) - may allow weak entries")
                elif threshold > 0.30:
                    print(f"    ⚠️  Threshold very high ({threshold:.3f}%) - may miss opportunities")
    else:
        print("\n✗ NO pre-entry filters found!")
        print("\n  Possible reasons:")
        print("  1. Pattern generator hasn't been updated with pre-entry logic")
        print("  2. Pattern generator hasn't run since update")
        print("  3. Not enough data to generate meaningful thresholds")
        
        # Check if regular filters exist
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM pattern_config_filters
                    WHERE project_id = %s
                """, [project_id])
                total_filters = cursor.fetchone()['count']
        
        print(f"\n  Total filters in project: {total_filters}")
        
        if total_filters > 0:
            print("  ✓ Regular filters exist - generator is working")
            print("  → Just needs pre-entry integration added")
        else:
            print("  ✗ No filters at all - generator may not be running")
    
    # Test 3: Check plays using these filters
    print("\n[Test 3] Checking plays using auto-filters project...")
    print("-" * 80)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    name,
                    pattern_update_by_ai,
                    project_ids
                FROM follow_the_goat_plays
                WHERE pattern_update_by_ai = 1
                  AND project_ids::text LIKE %s
                ORDER BY id
            """, [f'%{project_id}%'])
            plays = cursor.fetchall()
    
    if plays:
        print(f"✓ Found {len(plays)} plays using auto-filters:")
        for play in plays:
            print(f"  - Play #{play['id']}: {play['name']}")
    else:
        print("✗ No plays using auto-filters project")
        print("  Set pattern_update_by_ai=1 on plays to enable")
    
    # Test 4: Simulate validation with learned threshold
    print("\n[Test 4] Testing validator integration...")
    print("-" * 80)
    
    if pre_entry_filters:
        # Get the pre_entry_change_3m threshold
        change_3m_filter = [f for f in pre_entry_filters if f['field_name'] == 'pre_entry_change_3m']
        
        if change_3m_filter:
            learned_threshold = float(change_3m_filter[0]['from_value'])
            print(f"Learned threshold: {learned_threshold:.4f}%")
            
            # Test on the problematic trade
            test_trade_id = '20260203184619631'
            
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT pre_entry_change_3m
                        FROM buyin_trail_minutes
                        WHERE buyin_id = %s AND minute = 0 AND sub_minute = 0
                    """, [test_trade_id])
                    trail = cursor.fetchone()
            
            if trail and trail['pre_entry_change_3m'] is not None:
                actual_change = float(trail['pre_entry_change_3m'])
                would_pass = actual_change >= learned_threshold
                
                print(f"\nTest Trade {test_trade_id}:")
                print(f"  Actual 3m change: {actual_change:.4f}%")
                print(f"  Learned threshold: {learned_threshold:.4f}%")
                print(f"  Result: {'✓ PASS' if would_pass else '✗ REJECT'}")
                
                if not would_pass:
                    print(f"  ✓ CORRECT: Would prevent loss (trade lost -0.37%)")
            else:
                print(f"\nTest trade {test_trade_id} has no pre-entry data")
    else:
        print("No learned threshold to test")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    status = []
    
    if pre_entry_columns:
        status.append("✓ Trail schema has pre-entry columns")
    else:
        status.append("✗ Trail schema missing pre-entry columns")
    
    if pre_entry_filters:
        status.append(f"✓ Auto-filters project has {len(pre_entry_filters)} pre-entry filters")
    else:
        status.append("✗ No pre-entry filters generated yet")
    
    if plays:
        status.append(f"✓ {len(plays)} plays using auto-filters")
    else:
        status.append("⚠️  No plays using auto-filters yet")
    
    for s in status:
        print(f"  {s}")
    
    # Next steps
    print("\n" + "=" * 80)
    print("NEXT STEPS")
    print("=" * 80)
    
    if not pre_entry_filters:
        print("\n1. Update create_new_paterns.py with pre-entry logic")
        print("   (See PRE_ENTRY_AUTO_INTEGRATION_GUIDE.txt)")
        print("\n2. Run pattern generator:")
        print("   python3 000data_feeds/7_create_new_patterns/create_new_paterns.py")
        print("\n3. Verify filters created:")
        print("   python3 wallet_analysis/test_auto_pre_entry.py")
    else:
        print("\n✓ Integration complete!")
        print("\nMonitor performance:")
        print("- Track rejection rate (should increase 20-30%)")
        print("- Track P/L per trade (should improve)")
        print("- Review rejected trades manually")


if __name__ == "__main__":
    main()

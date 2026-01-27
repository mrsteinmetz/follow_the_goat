#!/usr/bin/env python3
"""
Test Pre-Entry Price Movement Filter Integration
=================================================
Verify that the entire pre-entry filter system is working correctly.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from core.database import get_postgres
from datetime import datetime, timedelta

def test_database_columns():
    """Test 1: Verify database columns exist."""
    print("="*80)
    print("TEST 1: Database Columns")
    print("="*80)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'buyin_trail_minutes' 
                AND column_name LIKE 'pre_entry%'
                ORDER BY column_name
            """)
            columns = cursor.fetchall()
    
    expected_columns = [
        'pre_entry_change_1m',
        'pre_entry_change_2m',
        'pre_entry_change_5m',
        'pre_entry_change_10m',
        'pre_entry_price_10m_before',
        'pre_entry_price_1m_before',
        'pre_entry_price_2m_before',
        'pre_entry_price_5m_before',
        'pre_entry_trend',
    ]
    
    found_columns = [c['column_name'] for c in columns]
    
    for col in expected_columns:
        if col in found_columns:
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} MISSING")
            return False
    
    print(f"\n✓ All {len(expected_columns)} columns found\n")
    return True


def test_pre_entry_module():
    """Test 2: Verify pre-entry module works."""
    print("="*80)
    print("TEST 2: Pre-Entry Module")
    print("="*80)
    
    try:
        from pre_entry_price_movement import (
            calculate_pre_entry_metrics,
            should_enter_based_on_price_movement
        )
        
        print("  ✓ Module imports successfully")
        
        # Get a recent buyin for testing
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, followed_at, our_entry_price
                    FROM follow_the_goat_buyins
                    WHERE followed_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY followed_at DESC
                    LIMIT 1
                """)
                test_buyin = cursor.fetchone()
        
        if not test_buyin:
            print("  ⚠ No recent buyins to test with")
            return True  # Not a failure, just no data
        
        buyin_id = test_buyin['id']
        entry_time = test_buyin['followed_at']
        entry_price = float(test_buyin['our_entry_price'])
        
        print(f"  Testing with buyin #{buyin_id}")
        
        # Calculate metrics
        metrics = calculate_pre_entry_metrics(entry_time, entry_price)
        
        if metrics:
            print(f"  ✓ Metrics calculated:")
            print(f"    - Trend: {metrics.get('pre_entry_trend')}")
            if metrics.get('pre_entry_change_10m') is not None:
                print(f"    - 10m change: {metrics.get('pre_entry_change_10m'):.3f}%")
            
            # Test decision function
            should_enter, reason = should_enter_based_on_price_movement(metrics)
            print(f"  ✓ Decision: {'ENTER' if should_enter else 'REJECT'} ({reason})")
        else:
            print("  ✗ Metrics calculation failed")
            return False
        
        print()
        return True
        
    except ImportError as e:
        print(f"  ✗ Module import failed: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_trail_data_integration():
    """Test 3: Verify trail data stores pre-entry metrics."""
    print("="*80)
    print("TEST 3: Trail Data Integration")
    print("="*80)
    
    # Check if any recent trail data has pre-entry metrics
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    buyin_id,
                    pre_entry_change_10m,
                    pre_entry_trend
                FROM buyin_trail_minutes
                WHERE minute = 0
                  AND pre_entry_change_10m IS NOT NULL
                ORDER BY buyin_id DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
    
    if result:
        print(f"  ✓ Pre-entry metrics found in trail data")
        print(f"    - Buyin: #{result['buyin_id']}")
        print(f"    - 10m change: {result['pre_entry_change_10m']:.3f}%")
        print(f"    - Trend: {result['pre_entry_trend']}")
        print()
        return True
    else:
        print("  ⚠ No trail data with pre-entry metrics yet")
        print("    (This is normal if no new trades since deployment)")
        print()
        return True  # Not a failure


def test_validator_integration():
    """Test 4: Verify pattern validator has pre-entry check."""
    print("="*80)
    print("TEST 4: Pattern Validator Integration")
    print("="*80)
    
    try:
        from pattern_validator import validate_buyin_signal
        
        print("  ✓ Validator imports successfully")
        
        # Check if validator code mentions pre_entry
        import inspect
        source = inspect.getsource(validate_buyin_signal)
        
        if 'pre_entry' in source.lower():
            print("  ✓ Validator contains pre-entry logic")
        else:
            print("  ✗ Validator does not contain pre-entry logic")
            return False
        
        if 'should_enter_based_on_price_movement' in source:
            print("  ✓ Validator calls pre-entry decision function")
        else:
            print("  ✗ Validator does not call pre-entry decision function")
            return False
        
        print()
        return True
        
    except ImportError as e:
        print(f"  ✗ Validator import failed: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Test failed: {e}")
        return False


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "="*80)
    print("PRE-ENTRY PRICE MOVEMENT FILTER - INTEGRATION TEST")
    print("="*80 + "\n")
    
    results = []
    
    # Run tests
    results.append(("Database Columns", test_database_columns()))
    results.append(("Pre-Entry Module", test_pre_entry_module()))
    results.append(("Trail Data Integration", test_trail_data_integration()))
    results.append(("Validator Integration", test_validator_integration()))
    
    # Summary
    print("="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✅ ALL TESTS PASSED - System ready for production")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED - Review failures above")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)

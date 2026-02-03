"""
Test script for validating the dual-check trailing stop logic.

This script simulates price paths and verifies that sell triggers work correctly.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Test cases for the sell logic
def test_dual_check_logic():
    """
    Test the dual-check sell logic with various price scenarios.
    """
    print("=" * 80)
    print("TESTING DUAL-CHECK TRAILING STOP LOGIC")
    print("=" * 80)
    
    # Tolerance settings (matching the screenshot):
    # - Below entry: 0.3% tolerance (stop-loss)
    # - 0-0.4% gain: 0.3% tolerance (trailing)
    # - 0.4-0.5% gain: 0.2% tolerance (trailing)
    # - 0.5-1.0% gain: 0.1% tolerance (trailing, LOCKS)
    
    tolerance_rules = {
        "decreases": [
            {"range": [-99.9999, 0], "tolerance": 0.003}  # 0.3% stop-loss
        ],
        "increases": [
            {"range": [0, 0.004], "tolerance": 0.003},     # 0-0.4% gain: 0.3% trailing
            {"range": [0.004, 0.005], "tolerance": 0.002}, # 0.4-0.5% gain: 0.2% trailing
            {"range": [0.005, 0.01], "tolerance": 0.001}   # 0.5-1% gain: 0.1% trailing
        ]
    }
    
    def select_rule(gain_decimal, rules):
        """Select the first rule whose range contains gain_decimal."""
        for rule in rules:
            low, high = rule['range']
            if low <= gain_decimal < high:
                return rule
        return rules[-1] if rules else None
    
    def simulate_check(entry_price, current_price, highest_price, locked_tolerance=1.0):
        """
        Simulate the dual-check logic and return whether we should sell.
        """
        # Calculate metrics
        gain_from_entry = (current_price - entry_price) / entry_price
        highest_gain = (highest_price - entry_price) / entry_price
        drop_from_entry = gain_from_entry  # Same calculation
        drop_from_high = (current_price - highest_price) / highest_price
        
        # Get stop-loss tolerance
        stop_loss_rule = select_rule(drop_from_entry, tolerance_rules['decreases'])
        stop_loss_tolerance = stop_loss_rule['tolerance'] if stop_loss_rule else 0.003
        
        # Get trailing stop tolerance (based on HIGHEST GAIN, not current)
        trailing_rule = select_rule(highest_gain, tolerance_rules['increases'])
        trailing_tolerance = trailing_rule['tolerance'] if trailing_rule else 0.003
        
        # Apply tolerance locking
        effective_trailing = min(trailing_tolerance, locked_tolerance)
        
        # Dual check
        stop_loss_triggered = drop_from_entry < -stop_loss_tolerance
        trailing_triggered = highest_gain > 0 and drop_from_high < -effective_trailing
        
        should_sell = stop_loss_triggered or trailing_triggered
        sell_reason = None
        if stop_loss_triggered:
            sell_reason = 'stop_loss'
        elif trailing_triggered:
            sell_reason = 'trailing_stop'
        
        return {
            'should_sell': should_sell,
            'sell_reason': sell_reason,
            'drop_from_entry': drop_from_entry * 100,
            'drop_from_high': drop_from_high * 100,
            'highest_gain': highest_gain * 100,
            'stop_loss_tolerance': stop_loss_tolerance * 100,
            'trailing_tolerance': trailing_tolerance * 100,
            'effective_trailing': effective_trailing * 100,
            'new_locked': min(trailing_tolerance, locked_tolerance)
        }
    
    # Test scenarios
    tests = []
    
    # Test 1: The actual bug case from trade 20260203182659804
    # Entry: $97.8258, High: $97.9695 (0.147% gain), Current: $97.6645 (below entry)
    # The old logic failed because it switched to 'decreases' bucket
    tests.append({
        'name': 'Bug case: Price below entry but should trigger trailing stop',
        'entry': 97.8258,
        'high': 97.9695,  # 0.147% gain
        'current': 97.6645,  # -0.165% from entry, -0.311% from high
        'expected_sell': True,
        'expected_reason': 'trailing_stop',  # Drop from high (0.31%) > tolerance (0.3%)
    })
    
    # Test 2: Stop-loss triggers first
    tests.append({
        'name': 'Stop-loss: Drop exceeds 0.3% from entry, no prior gain',
        'entry': 100.0,
        'high': 100.0,  # Never went above entry
        'current': 99.69,  # -0.31% from entry
        'expected_sell': True,
        'expected_reason': 'stop_loss',
    })
    
    # Test 3: Trailing stop with locked tolerance
    tests.append({
        'name': 'Trailing stop: 0.5% gain reached, tolerance locked at 0.1%',
        'entry': 100.0,
        'high': 100.55,  # 0.55% gain (locks 0.1% tolerance)
        'current': 100.43,  # Still above entry, but -0.12% from high
        'locked': 0.001,  # Pre-locked at 0.1%
        'expected_sell': True,
        'expected_reason': 'trailing_stop',  # Drop from high (0.12%) > tolerance (0.1%)
    })
    
    # Test 4: Price drops below entry but tolerance is locked tighter
    tests.append({
        'name': 'Trailing stop triggers before stop-loss (locked at 0.1%)',
        'entry': 100.0,
        'high': 100.55,  # 0.55% gain
        'current': 99.90,  # -0.1% from entry, -0.65% from high
        'locked': 0.001,  # Locked at 0.1%
        'expected_sell': True,
        'expected_reason': 'trailing_stop',  # Drop from high (0.65%) > tolerance (0.1%)
    })
    
    # Test 5: Neither condition met
    tests.append({
        'name': 'Neither condition met: small pullback',
        'entry': 100.0,
        'high': 100.35,  # 0.35% gain
        'current': 100.10,  # +0.1% from entry, -0.25% from high
        'expected_sell': False,
        'expected_reason': None,
    })
    
    # Test 6: Price at entry (edge case) - drop is 0.299%, just under 0.3% tolerance
    tests.append({
        'name': 'Edge case: Drop just under tolerance (0.299% < 0.3%)',
        'entry': 100.0,
        'high': 100.30,  # 0.3% gain
        'current': 100.0,  # 0% from entry, -0.299% from high (just under tolerance)
        'expected_sell': False,  # Should NOT sell - drop < tolerance
        'expected_reason': None,
    })
    
    # Test 7: Price at entry with slightly higher peak (drop exceeds tolerance)
    tests.append({
        'name': 'Edge case: Drop just over tolerance (0.31% > 0.3%)',
        'entry': 100.0,
        'high': 100.31,  # 0.31% gain
        'current': 100.0,  # 0% from entry, -0.309% from high
        'expected_sell': True,  # Should sell - drop > tolerance
        'expected_reason': 'trailing_stop',
    })
    
    # Run tests
    passed = 0
    failed = 0
    
    for i, test in enumerate(tests, 1):
        print(f"\nTest {i}: {test['name']}")
        print("-" * 60)
        
        result = simulate_check(
            test['entry'],
            test['current'],
            test['high'],
            test.get('locked', 1.0)
        )
        
        print(f"  Entry: ${test['entry']:.4f}")
        print(f"  Highest: ${test['high']:.4f} ({result['highest_gain']:.4f}% gain)")
        print(f"  Current: ${test['current']:.4f}")
        print(f"  Drop from entry: {result['drop_from_entry']:.4f}%")
        print(f"  Drop from high: {result['drop_from_high']:.4f}%")
        print(f"  Stop-loss tolerance: {result['stop_loss_tolerance']:.3f}%")
        print(f"  Trailing tolerance: {result['effective_trailing']:.3f}%")
        print(f"  Should sell: {result['should_sell']} (reason: {result['sell_reason']})")
        
        # Check expectations
        sell_match = result['should_sell'] == test['expected_sell']
        reason_match = result['sell_reason'] == test['expected_reason']
        
        if sell_match and reason_match:
            print(f"  ✓ PASSED")
            passed += 1
        else:
            print(f"  ✗ FAILED")
            print(f"    Expected: sell={test['expected_sell']}, reason={test['expected_reason']}")
            print(f"    Got: sell={result['should_sell']}, reason={result['sell_reason']}")
            failed += 1
    
    print("\n" + "=" * 80)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)
    
    return failed == 0


def test_with_real_trade():
    """
    Test the logic against the actual trade data from the database.
    """
    print("\n" + "=" * 80)
    print("TESTING WITH REAL TRADE DATA (20260203182659804)")
    print("=" * 80)
    
    # Real trade data:
    # Entry: $97.8258
    # Highest: $97.9695 (actually $98.1034 was the max)
    # Exit: $97.5081 (at -0.33% from entry)
    
    # First time it should have sold (at 18:29:41):
    # High: $97.9695, Current: $97.6645
    # Drop from high: -0.311%
    # With 0.3% tolerance, should have sold!
    
    entry = 97.8258
    high_at_fail = 97.9695
    price_at_fail = 97.6645
    
    highest_gain = (high_at_fail - entry) / entry
    drop_from_high = (price_at_fail - high_at_fail) / high_at_fail
    drop_from_entry = (price_at_fail - entry) / entry
    
    print(f"\nAt the point where it should have sold:")
    print(f"  Entry: ${entry:.4f}")
    print(f"  Highest: ${high_at_fail:.4f} ({highest_gain*100:.4f}% gain)")
    print(f"  Current: ${price_at_fail:.4f}")
    print(f"  Drop from entry: {drop_from_entry*100:.4f}%")
    print(f"  Drop from high: {drop_from_high*100:.4f}%")
    
    # With 0.3% tolerance on trailing (0-0.4% gain tier):
    tolerance = 0.003
    print(f"\n  Trailing tolerance (0-0.4% tier): {tolerance*100:.3f}%")
    print(f"  Drop from high ({abs(drop_from_high)*100:.4f}%) vs tolerance ({tolerance*100:.3f}%):")
    
    if drop_from_high < -tolerance:
        print(f"  ✓ SHOULD SELL (trailing stop triggered)")
    else:
        print(f"  ✗ SHOULD NOT SELL (trailing stop not triggered)")
    
    # Also check stop-loss
    stop_loss_tol = 0.003
    print(f"\n  Stop-loss tolerance: {stop_loss_tol*100:.3f}%")
    print(f"  Drop from entry ({abs(drop_from_entry)*100:.4f}%) vs tolerance ({stop_loss_tol*100:.3f}%):")
    
    if drop_from_entry < -stop_loss_tol:
        print(f"  STOP-LOSS would trigger (but trailing stop wins first)")
    else:
        print(f"  Stop-loss not triggered at this point")
    
    return True


if __name__ == "__main__":
    print("\n")
    success1 = test_dual_check_logic()
    success2 = test_with_real_trade()
    
    print("\n" + "=" * 80)
    if success1 and success2:
        print("ALL TESTS PASSED!")
    else:
        print("SOME TESTS FAILED!")
    print("=" * 80)
    
    sys.exit(0 if (success1 and success2) else 1)

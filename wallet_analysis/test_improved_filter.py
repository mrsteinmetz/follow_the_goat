"""
Test Proposed Pre-Entry Filter Improvements
============================================
Tests the recommended changes to pre_entry_price_movement.py

This script simulates the improved filter logic WITHOUT modifying production code.
"""

import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from core.database import get_postgres
from datetime import datetime
from typing import Dict, Any, Tuple


def improved_should_enter_based_on_price_movement(
    pre_entry_metrics: Dict[str, Any],
    min_change_3m: float = 0.20,  # NEW: Increased from 0.08
    require_acceleration: bool = True  # NEW: Check for momentum
) -> Tuple[bool, str]:
    """
    IMPROVED VERSION with stricter filters.
    
    Changes from original:
    1. min_change_3m: 0.08 → 0.20 (higher threshold)
    2. Check for deceleration (prevents buying the top)
    3. Check signal divergence (price up but pressure down)
    
    Args:
        pre_entry_metrics: Dict from calculate_pre_entry_metrics()
        min_change_3m: Minimum 3m price change required
        require_acceleration: If True, reject if momentum is fading
    
    Returns:
        Tuple of (should_enter: bool, reason: str)
    """
    change_3m = pre_entry_metrics.get('pre_entry_change_3m')
    change_1m = pre_entry_metrics.get('pre_entry_change_1m')
    change_2m = pre_entry_metrics.get('pre_entry_change_2m')
    
    if change_3m is None:
        return True, "NO_PRICE_DATA"
    
    # TEST 1: Minimum momentum check
    if change_3m < min_change_3m:
        return False, f"WEAK_MOMENTUM (change_3m={change_3m:.3f}% < {min_change_3m}%)"
    
    # TEST 2: Deceleration check (prevent buying the top)
    if require_acceleration and change_1m is not None and change_2m is not None:
        # Expected rate: if price rose X% over 3m, should rise X/3 per minute
        expected_1m_rate = change_3m / 3
        
        # If recent 1m change is much less than expected rate, momentum is fading
        if change_1m < expected_1m_rate * 0.8:  # Allow 20% tolerance
            return False, f"DECELERATION (topping: 1m={change_1m:.3f}% vs expected={expected_1m_rate:.3f}%)"
    
    return True, "PASS"


def check_signal_divergence(buyin_id: str) -> Tuple[bool, str]:
    """
    NEW FILTER: Check if trading signals align with price movement.
    
    Reject if:
    - Buy/sell pressure is negative (more selling)
    - Whale net flow is negative (whales distributing)
    - Both are weak despite rising price
    
    Args:
        buyin_id: The buyin ID to check
    
    Returns:
        Tuple of (signals_ok: bool, reason: str)
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    tx_buy_sell_pressure,
                    wh_net_flow_ratio,
                    pat_breakout_score,
                    ob_volume_imbalance
                FROM buyin_trail_minutes
                WHERE buyin_id = %s AND minute = 0 AND sub_minute = 0
            """, [buyin_id])
            signals = cursor.fetchone()
    
    if not signals:
        return True, "NO_SIGNAL_DATA"
    
    buy_pressure = signals['tx_buy_sell_pressure'] or 0
    whale_flow = signals['wh_net_flow_ratio'] or 0
    breakout_score = signals['pat_breakout_score'] or 0
    ob_imbalance = signals['ob_volume_imbalance'] or 0
    
    # TEST 1: Check buy/sell pressure
    if buy_pressure < 0:
        return False, f"NEGATIVE_BUY_PRESSURE ({buy_pressure:.3f}) - more sellers than buyers"
    
    # TEST 2: Check whale activity
    if whale_flow < -0.02:
        return False, f"WHALE_DISTRIBUTION ({whale_flow:.3f}) - whales selling"
    
    # TEST 3: Check if signals are too weak
    if buy_pressure < 0.1 and whale_flow < 0.05:
        return False, f"WEAK_SIGNALS (pressure={buy_pressure:.3f}, whale={whale_flow:.3f})"
    
    return True, "SIGNALS_ALIGN"


def test_trade(buyin_id: str):
    """Test a trade against old and new filters."""
    print("=" * 80)
    print(f"TESTING BUYIN: {buyin_id}")
    print("=" * 80)
    
    # Get buyin details
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    followed_at,
                    our_entry_price,
                    our_exit_price,
                    our_profit_loss,
                    our_status
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [buyin_id])
            buyin = cursor.fetchone()
    
    if not buyin:
        print("ERROR: Buyin not found!")
        return
    
    entry_time = buyin['followed_at']
    entry_price = float(buyin['our_entry_price'])
    exit_price = float(buyin['our_exit_price']) if buyin['our_exit_price'] else None
    profit_loss = float(buyin['our_profit_loss']) if buyin['our_profit_loss'] else None
    
    print(f"\nTRADE OUTCOME:")
    print(f"  Entry: ${entry_price:.4f}")
    if exit_price:
        print(f"  Exit: ${exit_price:.4f}")
        print(f"  P/L: {profit_loss:.2f} ({((exit_price - entry_price) / entry_price * 100):+.2f}%)")
    print(f"  Status: {buyin['our_status']}")
    
    # Calculate pre-entry metrics
    from pre_entry_price_movement import calculate_pre_entry_metrics
    metrics = calculate_pre_entry_metrics(entry_time, entry_price)
    
    print(f"\nPRE-ENTRY METRICS:")
    print(f"  3m change: {metrics.get('pre_entry_change_3m'):.4f}%")
    print(f"  2m change: {metrics.get('pre_entry_change_2m'):.4f}%")
    print(f"  1m change: {metrics.get('pre_entry_change_1m'):.4f}%")
    print(f"  Trend: {metrics.get('pre_entry_trend')}")
    
    # Test OLD filter (0.08% threshold, no acceleration check)
    print(f"\n{'=' * 80}")
    print("OLD FILTER (Current Production):")
    print(f"{'=' * 80}")
    from pre_entry_price_movement import should_enter_based_on_price_movement as old_filter
    old_result, old_reason = old_filter(metrics, min_change_3m=0.08)
    print(f"  Threshold: 0.08%")
    print(f"  Acceleration Check: No")
    print(f"  Result: {'✓ PASS' if old_result else '✗ FAIL'}")
    print(f"  Reason: {old_reason}")
    
    # Test NEW filter (0.20% threshold + acceleration check)
    print(f"\n{'=' * 80}")
    print("NEW FILTER (Proposed Improvement):")
    print(f"{'=' * 80}")
    new_result, new_reason = improved_should_enter_based_on_price_movement(
        metrics,
        min_change_3m=0.20,
        require_acceleration=True
    )
    print(f"  Threshold: 0.20%")
    print(f"  Acceleration Check: Yes")
    print(f"  Result: {'✓ PASS' if new_result else '✗ FAIL'}")
    print(f"  Reason: {new_reason}")
    
    # Test signal divergence check
    print(f"\n{'=' * 80}")
    print("SIGNAL DIVERGENCE CHECK (NEW):")
    print(f"{'=' * 80}")
    signals_ok, signal_reason = check_signal_divergence(buyin_id)
    print(f"  Result: {'✓ PASS' if signals_ok else '✗ FAIL'}")
    print(f"  Reason: {signal_reason}")
    
    # Final verdict
    print(f"\n{'=' * 80}")
    print("FINAL VERDICT:")
    print(f"{'=' * 80}")
    old_decision = "ENTER" if old_result else "REJECT"
    new_decision = "ENTER" if (new_result and signals_ok) else "REJECT"
    
    print(f"  Old Filter: {old_decision}")
    print(f"  New Filter: {new_decision}")
    
    if old_decision != new_decision:
        print(f"\n  ⭐ FILTER CHANGE: {old_decision} → {new_decision}")
        if new_decision == "REJECT" and profit_loss and profit_loss < 0:
            print(f"  ✓ CORRECT: Would have prevented loss of ${abs(profit_loss):.2f}")
        elif new_decision == "REJECT" and profit_loss and profit_loss > 0:
            print(f"  ⚠️  CAUTION: Would have missed profit of ${profit_loss:.2f}")
    else:
        print(f"  No change in decision")
    
    print(f"\n{'=' * 80}\n")


def main():
    """Test the problematic trade and a few others."""
    print("\n" + "=" * 80)
    print("PRE-ENTRY FILTER IMPROVEMENT TEST")
    print("=" * 80 + "\n")
    
    # Test the problematic trade
    test_trade('20260203184619631')
    
    # Optionally test more trades
    print("\nTo test more trades, run:")
    print("  python3 wallet_analysis/test_improved_filter.py <buyin_id>")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test specific buyin
        test_trade(sys.argv[1])
    else:
        main()

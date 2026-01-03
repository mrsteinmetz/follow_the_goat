#!/usr/bin/env python3
"""
Cleanup Duplicate Active Cycles
================================
Closes duplicate active cycles in master.py's Data Engine,
keeping only the most recent cycle per threshold.

Usage:
    python scripts/cleanup_duplicate_cycles.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_trading_engine

def cleanup_duplicate_cycles():
    """Close duplicate active cycles, keeping only the most recent per threshold."""
    engine = get_trading_engine()
    
    if not engine:
        print("ERROR: TradingDataEngine not running!")
        print("Please start master.py first")
        return False
    
    print("Checking for duplicate active cycles...")
    
    # Get all active cycles grouped by threshold
    cycles = engine.read_all("""
        SELECT id, threshold, cycle_start_time
        FROM cycle_tracker
        WHERE coin_id = 5 AND cycle_end_time IS NULL
        ORDER BY threshold, cycle_start_time DESC
    """)
    
    if not cycles:
        print("No active cycles found")
        return True
    
    # Group by threshold
    by_threshold = {}
    for cycle in cycles:
        threshold = cycle['threshold']
        if threshold not in by_threshold:
            by_threshold[threshold] = []
        by_threshold[threshold].append(cycle)
    
    print(f"\nFound {len(cycles)} active cycles across {len(by_threshold)} thresholds:")
    for threshold, threshold_cycles in sorted(by_threshold.items()):
        print(f"  {threshold}%: {len(threshold_cycles)} cycle(s)")
    
    # Close duplicates (keep most recent, close older ones)
    total_closed = 0
    for threshold, threshold_cycles in sorted(by_threshold.items()):
        if len(threshold_cycles) <= 1:
            continue  # No duplicates
        
        # Keep the first one (most recent), close the rest
        to_keep = threshold_cycles[0]
        to_close = threshold_cycles[1:]
        
        print(f"\n{threshold}%: Keeping cycle #{to_keep['id']}, closing {len(to_close)} older cycle(s)...")
        
        for old_cycle in to_close:
            try:
                # Close the old cycle
                close_time = to_keep['cycle_start_time']  # Use new cycle's start as close time
                
                # Use engine.write to update
                engine.write("""
                    UPDATE cycle_tracker
                    SET cycle_end_time = ?
                    WHERE id = ?
                """, [close_time, old_cycle['id']])
                
                print(f"  ✓ Closed cycle #{old_cycle['id']}")
                total_closed += 1
                
            except Exception as e:
                print(f"  ✗ Failed to close cycle #{old_cycle['id']}: {e}")
    
    print(f"\n{'='*60}")
    print(f"Cleanup complete: Closed {total_closed} duplicate cycle(s)")
    print(f"Result: {len(by_threshold)} active cycles (one per threshold)")
    print(f"{'='*60}")
    
    return True


if __name__ == "__main__":
    success = cleanup_duplicate_cycles()
    sys.exit(0 if success else 1)


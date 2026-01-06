#!/usr/bin/env python3
"""
Fix corrupted cycles using CORRECT database access patterns from .cursorrules

This script:
1. Deletes cycles where end_time < start_time (data corruption)
2. Uses duckdb_execute_write() for writes (thread-safe write queue)
3. Uses get_duckdb("central", read_only=True) for reads (fresh cursor)
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Use CORRECT database access patterns
from core.database import get_duckdb, duckdb_execute_write

print("=" * 60)
print("FIXING CORRUPTED CYCLES")
print("=" * 60)

# Step 1: Check how many corrupted cycles exist
print("\n1. Checking for corrupted cycles...")
with get_duckdb("central", read_only=True) as cursor:
    result = cursor.execute("""
        SELECT COUNT(*) FROM cycle_tracker
        WHERE cycle_end_time IS NOT NULL
        AND cycle_end_time < cycle_start_time
    """).fetchone()
    corrupted_count = result[0] if result else 0
    
print(f"   Found {corrupted_count} corrupted cycles")

if corrupted_count > 0:
    # Step 2: Delete corrupted cycles using write queue
    print("\n2. Deleting corrupted cycles...")
    duckdb_execute_write("central", """
        DELETE FROM cycle_tracker 
        WHERE cycle_end_time IS NOT NULL 
        AND cycle_end_time < cycle_start_time
    """, sync=True)  # sync=True to wait for completion
    
    print(f"   ✓ Deleted {corrupted_count} corrupted cycles")

# Step 3: Check current state
print("\n3. Current cycle state:")
with get_duckdb("central", read_only=True) as cursor:
    # Valid completed cycles
    result = cursor.execute("""
        SELECT COUNT(*) FROM cycle_tracker
        WHERE cycle_end_time IS NOT NULL
        AND cycle_end_time >= cycle_start_time
    """).fetchone()
    valid_completed = result[0] if result else 0
    
    # Active cycles
    result2 = cursor.execute("""
        SELECT COUNT(*) FROM cycle_tracker
        WHERE cycle_end_time IS NULL
    """).fetchone()
    active = result2[0] if result2 else 0
    
    print(f"   Valid completed cycles: {valid_completed}")
    print(f"   Active cycles: {active}")

# Step 4: Check if profiles can be created (once cycles complete)
print("\n4. Checking profile creation readiness:")
with get_duckdb("central", read_only=True) as cursor:
    # Check for eligible wallets
    result = cursor.execute("""
        SELECT COUNT(DISTINCT wallet_address) 
        FROM sol_stablecoin_trades 
        WHERE direction = 'buy'
        GROUP BY wallet_address
        HAVING COUNT(*) >= 3
    """).fetchone()
    eligible_wallets = result[0] if result else 0
    
    print(f"   Eligible wallets (>=3 trades): {eligible_wallets}")
    
    # Check total buy trades
    result2 = cursor.execute("""
        SELECT COUNT(*) FROM sol_stablecoin_trades WHERE direction = 'buy'
    """).fetchone()
    buy_trades = result2[0] if result2 else 0
    
    print(f"   Total buy trades: {buy_trades}")
    
    # If there are valid completed cycles, check matching trades
    if valid_completed > 0:
        result3 = cursor.execute("""
            SELECT COUNT(DISTINCT t.wallet_address)
            FROM sol_stablecoin_trades t
            INNER JOIN cycle_tracker c ON (
                c.cycle_start_time <= t.trade_timestamp
                AND c.cycle_end_time >= t.trade_timestamp
                AND c.cycle_end_time IS NOT NULL
            )
            WHERE t.direction = 'buy'
        """).fetchone()
        matching_wallets = result3[0] if result3 else 0
        print(f"   Wallets with trades in completed cycles: {matching_wallets}")

print("\n" + "=" * 60)
if valid_completed > 0:
    print("✓ SUCCESS! Profiles should be created in next scheduler run")
    print("  (Runs every 5 seconds)")
else:
    print("⏳ WAITING: No completed cycles yet")
    print("  Cycles will complete when price drops 0.2-0.5% from peak")
    print("  Check current cycles:")
    
    with get_duckdb("central", read_only=True) as cursor:
        result = cursor.execute("""
            SELECT threshold, 
                   highest_price_reached, 
                   (SELECT price FROM prices WHERE token = 'SOL' ORDER BY ts DESC LIMIT 1) as current_price,
                   ((highest_price_reached - (SELECT price FROM prices WHERE token = 'SOL' ORDER BY ts DESC LIMIT 1)) / highest_price_reached * 100) as drop_pct
            FROM cycle_tracker
            WHERE cycle_end_time IS NULL
            ORDER BY threshold
            LIMIT 1
        """).fetchone()
        
        if result:
            threshold, highest, current, drop_pct = result
            print(f"\n  Active cycle status:")
            print(f"    Threshold: {threshold}%")
            print(f"    Highest price: ${highest:.4f}")
            print(f"    Current price: ${current:.4f}")
            print(f"    Drop from peak: {drop_pct:.3f}%")
            print(f"    Need: {threshold}% drop to complete")

print("=" * 60)


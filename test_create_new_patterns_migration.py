#!/usr/bin/env python3
"""
Test script for create_new_paterns.py PostgreSQL migration
"""
import sys
from pathlib import Path

# Add project root
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 60)
print("Testing create_new_paterns.py PostgreSQL Migration")
print("=" * 60)

# Test 1: Create ai_play_updates table
print("\n[Test 1] Creating ai_play_updates table...")
try:
    from core.database import get_postgres
    
    sql = """
    CREATE TABLE IF NOT EXISTS ai_play_updates (
        id SERIAL PRIMARY KEY,
        play_id INTEGER NOT NULL,
        play_name VARCHAR(255),
        project_id INTEGER,
        project_name VARCHAR(255),
        pattern_count INTEGER,
        filters_applied INTEGER,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        run_id VARCHAR(50),
        status VARCHAR(20) DEFAULT 'success'
    );

    CREATE INDEX IF NOT EXISTS idx_ai_play_updates_play_id ON ai_play_updates(play_id);
    CREATE INDEX IF NOT EXISTS idx_ai_play_updates_updated_at ON ai_play_updates(updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ai_play_updates_run_id ON ai_play_updates(run_id);
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()
    
    print("✓ Table created successfully")
except Exception as e:
    print(f"✗ Failed to create table: {e}")
    sys.exit(1)

# Test 2: Check for data
print("\n[Test 2] Checking for trade data...")
try:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM follow_the_goat_buyins 
                WHERE potential_gains IS NOT NULL
            """)
            result = cursor.fetchone()
            count = result['count']
            print(f"✓ Found {count} trades with potential_gains")
            
            if count == 0:
                print("⚠ No trades with potential_gains - script will exit early")
except Exception as e:
    print(f"✗ Failed to check data: {e}")
    sys.exit(1)

# Test 3: Check for trade_filter_values
print("\n[Test 3] Checking for filter values...")
try:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM trade_filter_values
            """)
            result = cursor.fetchone()
            count = result['count']
            print(f"✓ Found {count} filter value records")
            
            if count == 0:
                print("⚠ No filter values - pattern generation may not work")
except Exception as e:
    print(f"✗ Failed to check filter values: {e}")
    sys.exit(1)

# Test 4: Test PostgreSQL read function
print("\n[Test 4] Testing _read_from_postgres function...")
try:
    sys.path.insert(0, str(PROJECT_ROOT / "000data_feeds" / "7_create_new_patterns"))
    from create_new_paterns import _read_from_postgres
    
    results = _read_from_postgres("""
        SELECT id, play_id, potential_gains 
        FROM follow_the_goat_buyins 
        WHERE potential_gains IS NOT NULL 
        LIMIT 5
    """)
    
    print(f"✓ Function works, returned {len(results)} rows")
    if results:
        print(f"  Sample: trade_id={results[0].get('id')}, potential_gains={results[0].get('potential_gains')}")
except Exception as e:
    print(f"✗ Failed: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)

# Test 5: Check plays with pattern_update_by_ai
print("\n[Test 5] Checking plays with pattern_update_by_ai=1...")
try:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, pattern_update_by_ai 
                FROM follow_the_goat_plays 
                WHERE pattern_update_by_ai = 1
            """)
            plays = cursor.fetchall()
            print(f"✓ Found {len(plays)} plays with AI updates enabled")
            for play in plays:
                print(f"  - Play #{play['id']}: {play['name']}")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 6: Run the main function (with small timeout)
print("\n[Test 6] Running create_new_paterns.run()...")
print("  (This may take a while...)")
try:
    from create_new_paterns import run
    import logging
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    result = run()
    
    print("\n" + "=" * 60)
    print("RUN RESULTS:")
    print("=" * 60)
    print(f"Success: {result.get('success')}")
    print(f"Run ID: {result.get('run_id')}")
    print(f"Suggestions: {result.get('suggestions_count')}")
    print(f"Combinations: {result.get('combinations_count')}")
    print(f"Filters Synced: {result.get('filters_synced')}")
    print(f"Plays Updated: {result.get('plays_updated')}")
    if result.get('error'):
        print(f"Error: {result.get('error')}")
    print("=" * 60)
    
except Exception as e:
    print(f"✗ Failed to run: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)

# Test 7: Check if updates were logged
print("\n[Test 7] Checking ai_play_updates table...")
try:
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT play_id, play_name, project_name, pattern_count, status, updated_at
                FROM ai_play_updates 
                ORDER BY updated_at DESC 
                LIMIT 5
            """)
            updates = cursor.fetchall()
            print(f"✓ Found {len(updates)} play update records")
            for update in updates:
                print(f"  - Play #{update['play_id']} ({update['play_name']}): "
                      f"{update['pattern_count']} patterns, status={update['status']}")
except Exception as e:
    print(f"✗ Failed: {e}")

print("\n" + "=" * 60)
print("✓ ALL TESTS COMPLETE!")
print("=" * 60)

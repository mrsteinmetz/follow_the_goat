#!/usr/bin/env python3
"""
Apply database migration to add pre-entry price movement columns.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def apply_migration():
    """Apply the pre-entry price movement migration."""
    
    migration_sql = """
-- Add pre-entry price movement analysis columns to buyin_trail_minutes
-- These columns store price movement BEFORE entry to filter out falling-price entries

ALTER TABLE buyin_trail_minutes 
ADD COLUMN IF NOT EXISTS pre_entry_price_1m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_2m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_5m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_price_10m_before DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_1m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_2m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_5m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_change_10m DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS pre_entry_trend VARCHAR(20);

-- Add index on pre_entry_change_10m for filtering queries
CREATE INDEX IF NOT EXISTS idx_buyin_trail_pre_entry_change_10m 
ON buyin_trail_minutes(pre_entry_change_10m) 
WHERE minute = 0;
"""
    
    try:
        print("Applying pre-entry price movement migration...")
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Split by semicolon and execute each statement
                for statement in migration_sql.split(';'):
                    statement = statement.strip()
                    if statement:
                        print(f"Executing: {statement[:100]}...")
                        cursor.execute(statement)
        
        print("✓ Migration applied successfully")
        print("\nAdded columns:")
        print("  - pre_entry_price_1m_before")
        print("  - pre_entry_price_2m_before")
        print("  - pre_entry_price_5m_before")
        print("  - pre_entry_price_10m_before")
        print("  - pre_entry_change_1m")
        print("  - pre_entry_change_2m")
        print("  - pre_entry_change_5m")
        print("  - pre_entry_change_10m")
        print("  - pre_entry_trend")
        print("\nAdded index:")
        print("  - idx_buyin_trail_pre_entry_change_10m")
        
        return True
        
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = apply_migration()
    sys.exit(0 if success else 1)

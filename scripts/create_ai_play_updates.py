#!/usr/bin/env python3
"""Quick script to create ai_play_updates table"""
import sys
from pathlib import Path

# Add project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def create_table():
    """Create ai_play_updates table if it doesn't exist."""
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
    
    print("✓ Table ai_play_updates created successfully")

if __name__ == "__main__":
    create_table()

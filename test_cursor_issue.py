#!/usr/bin/env python3
"""
Test script to debug cursor closing issue
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def test_connection():
    """Test connection and cursor behavior"""
    print("Testing PostgreSQL connection...")
    
    try:
        with get_postgres() as conn:
            print(f"Got connection: {conn}")
            print(f"Connection closed? {conn.closed}")
            
            with conn.cursor() as cursor:
                print(f"Got cursor: {cursor}")
                print(f"Cursor closed? {cursor.closed}")
                
                cursor.execute("""
                    SELECT price, timestamp, id
                    FROM prices 
                    WHERE token = 'SOL'
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)
                
                print(f"After execute - Cursor closed? {cursor.closed}")
                print(f"After execute - Connection closed? {conn.closed}")
                
                result = cursor.fetchone()
                
                print(f"After fetchone - Cursor closed? {cursor.closed}")
                print(f"Result: {result}")
                
                if result:
                    print(f"✓ Success! Got SOL price: ${result.get('price')}")
                    return True
                else:
                    print("✗ No data returned")
                    return False
                    
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

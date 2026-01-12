#!/usr/bin/env python3
"""
Test connection pool cursor issue
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def test_get_price():
    """Test getting price using connection pool."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT price, timestamp, id
                    FROM prices 
                    WHERE token = 'SOL'
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                
                if result:
                    price = float(result.get('price'))
                    print(f"SUCCESS: Current SOL price: ${price:.6f} (ID: {result.get('id')})")
                    return price
                else:
                    print("WARNING: No SOL price data found")
                    return None
                    
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("Testing connection pool...")
    for i in range(5):
        print(f"\nTest {i+1}:")
        test_get_price()
    print("\nAll tests complete!")

#!/usr/bin/env python3
"""
Clear order book and price data from PostgreSQL archive to force fresh collection with UTC timestamps.
"""

import sys
sys.path.insert(0, '/root/follow_the_goat')

from core.database import get_postgres

print("Clearing old CET-timestamped data from PostgreSQL archive...")

with get_postgres() as conn:
    cursor = conn.cursor()
    
    # Clear order_book_features
    print("Truncating order_book_features...")
    cursor.execute("TRUNCATE TABLE order_book_features")
    
    # Clear prices 
    print("Truncating prices...")
    cursor.execute("TRUNCATE TABLE prices")
    
    conn.commit()
    print("✅ Cleared old data from PostgreSQL")

print("\nNow restart the master service to rebuild with UTC timestamps")


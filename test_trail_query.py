"""Test trail generator queries directly"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

# Test the actual query that trail_generator uses
from core.database import get_postgres

# Get a recent buyin
with get_postgres() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, followed_at FROM follow_the_goat_buyins WHERE id = 679")
        buyin = cursor.fetchone()
        
        if buyin:
            print(f"Testing trail generation for buyin #{buyin['id']}")
            print(f"Followed at: {buyin['followed_at']}")
            
            window_end = buyin['followed_at']
            window_start = window_end - timedelta(minutes=15)
            
            print(f"\nQuerying prices from {window_start} to {window_end}")
            
            # Test the exact query from trail_generator (with PostgreSQL column names)
            query = """
                SELECT COUNT(*) as cnt, 
                       MIN(timestamp) as min_ts, 
                       MAX(timestamp) as max_ts
                FROM prices 
                WHERE timestamp >= %s
                  AND timestamp <= %s
                  AND token = %s
            """
            
            for token in ['SOL', 'BTC', 'ETH']:
                cursor.execute(query, [window_start, window_end, token])
                result = cursor.fetchone()
                print(f"  {token}: {result['cnt']} prices (range: {result['min_ts']} to {result['max_ts']})")
            
            # Test order book query
            cursor.execute("""
                SELECT COUNT(*) as cnt,
                       MIN(timestamp) as min_ts,
                       MAX(timestamp) as max_ts
                FROM order_book_features
                WHERE timestamp >= %s
                  AND timestamp <= %s
            """, [window_start, window_end])
            result = cursor.fetchone()
            print(f"  Order Book: {result['cnt']} rows (range: {result['min_ts']} to {result['max_ts']})")

"""Check MySQL for price cycle data."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_mysql

print("=== CHECKING MYSQL ===")

with get_mysql() as conn:
    with conn.cursor() as cursor:
        # Check price_analysis
        cursor.execute("SELECT COUNT(*) as cnt FROM price_analysis")
        pa_count = cursor.fetchone()['cnt']
        print(f"price_analysis records: {pa_count}")
        
        if pa_count > 0:
            cursor.execute("SELECT id, coin_id, current_price, percent_threshold, price_cycle, created_at FROM price_analysis ORDER BY id DESC LIMIT 5")
            rows = cursor.fetchall()
            print("Latest price_analysis:")
            for row in rows:
                print(f"  {row}")
        
        # Check cycle_tracker
        cursor.execute("SELECT COUNT(*) as cnt FROM cycle_tracker")
        ct_count = cursor.fetchone()['cnt']
        print(f"\ncycle_tracker records: {ct_count}")
        
        if ct_count > 0:
            cursor.execute("SELECT id, threshold, cycle_start_time, cycle_end_time, max_percent_increase_from_lowest, total_data_points FROM cycle_tracker ORDER BY id DESC LIMIT 10")
            rows = cursor.fetchall()
            print("Latest cycles:")
            for row in rows:
                print(f"  ID: {row['id']}, threshold: {row['threshold']}, start: {row['cycle_start_time']}, increase: {row['max_percent_increase_from_lowest']}%")
        
        # Check price_points too
        cursor.execute("SELECT COUNT(*) as cnt FROM price_points WHERE coin_id = 5")
        pp_count = cursor.fetchone()['cnt']
        print(f"\nprice_points (SOL): {pp_count}")
        
        if pp_count > 0:
            cursor.execute("SELECT id, value, created_at FROM price_points WHERE coin_id = 5 ORDER BY id DESC LIMIT 3")
            rows = cursor.fetchall()
            print("Latest prices:")
            for row in rows:
                print(f"  {row}")

print("\nDone!")


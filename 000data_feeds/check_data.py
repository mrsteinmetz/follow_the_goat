"""Quick diagnostic to check price and cycle data."""
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# Check prices.duckdb for price data
prices_db = PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"
print("=== PRICES DATABASE ===")
if prices_db.exists():
    with duckdb.connect(str(prices_db), read_only=True) as conn:
        count = conn.execute("SELECT COUNT(*) FROM price_points WHERE token = 'SOL'").fetchone()[0]
        print(f"SOL price points: {count}")
        if count > 0:
            latest = conn.execute("SELECT ts, price FROM price_points WHERE token = 'SOL' ORDER BY ts DESC LIMIT 3").fetchall()
            print(f"Latest prices: {latest}")
else:
    print("prices.duckdb does not exist!")

# Check central.duckdb for cycle data
central_db = PROJECT_ROOT / "000data_feeds" / "central.duckdb"
print()
print("=== CENTRAL DATABASE ===")
if central_db.exists():
    with duckdb.connect(str(central_db), read_only=True) as conn:
        # Check tables
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        print(f"Tables: {[t[0] for t in tables]}")
        
        # Check price_analysis
        try:
            pa_count = conn.execute("SELECT COUNT(*) FROM price_analysis").fetchone()[0]
            print(f"price_analysis records: {pa_count}")
            if pa_count > 0:
                latest_pa = conn.execute("SELECT id, coin_id, current_price, percent_threshold, price_cycle, created_at FROM price_analysis ORDER BY created_at DESC LIMIT 5").fetchall()
                print("Latest price_analysis:")
                for row in latest_pa:
                    print(f"  {row}")
        except Exception as e:
            print(f"price_analysis error: {e}")
        
        # Check cycle_tracker
        try:
            ct_count = conn.execute("SELECT COUNT(*) FROM cycle_tracker").fetchone()[0]
            print(f"\ncycle_tracker records: {ct_count}")
            if ct_count > 0:
                latest_ct = conn.execute("SELECT id, threshold, cycle_start_time, cycle_end_time, max_percent_increase_from_lowest, total_data_points FROM cycle_tracker ORDER BY id DESC LIMIT 10").fetchall()
                print("Latest cycles:")
                for row in latest_ct:
                    print(f"  {row}")
        except Exception as e:
            print(f"cycle_tracker error: {e}")
else:
    print("central.duckdb does not exist!")

# Now try to run the price cycle processor once
print()
print("=== TRYING TO PROCESS PRICE CYCLES ===")
import sys
sys.path.insert(0, str(PROJECT_ROOT))

from create_price_cycles import process_price_cycles, get_new_price_points, get_last_processed_ts

last_ts = get_last_processed_ts()
print(f"Last processed timestamp: {last_ts}")

price_points = get_new_price_points(last_ts, 10)
print(f"New price points found: {len(price_points)}")
if price_points:
    print(f"First few: {price_points[:3]}")

# Actually run it
result = process_price_cycles()
print(f"Processed: {result} price points")


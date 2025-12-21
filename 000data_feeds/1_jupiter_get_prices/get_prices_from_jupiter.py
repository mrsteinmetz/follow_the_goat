"""
Jupiter Price Fetcher with DuckDB Storage
Optimized for trading bot performance with hot (24h) and cold (archive) tables.
"""

import requests
import time
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# --- Configuration ---
FETCH_INTERVAL_SECONDS = 1.0
JUPITER_API_URL = "https://lite-api.jup.ag/price/v3"
CLEANUP_INTERVAL = 3600  # Run cleanup every hour (in iterations based on fetch interval)

# Token mint addresses on Solana
TOKENS = {
    "BTC": "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  # Wrapped BTC (Portal)
    "ETH": "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # Wrapped ETH (Portal)
    "SOL": "So11111111111111111111111111111111111111112",   # Native SOL
}

# Reverse lookup
MINT_TO_TOKEN = {v: k for k, v in TOKENS.items()}

# Database path
DB_PATH = Path(__file__).parent / "prices.duckdb"


def init_database(con: duckdb.DuckDBPyConnection) -> None:
    """Initialize database tables optimized for time-series price data."""
    
    # Hot table - last 24 hours of data, optimized for fast reads
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_points (
            ts TIMESTAMP NOT NULL,
            token VARCHAR(10) NOT NULL,
            price DOUBLE NOT NULL
        )
    """)
    
    # Archive table - all historical data
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_points_archive (
            ts TIMESTAMP NOT NULL,
            token VARCHAR(10) NOT NULL,
            price DOUBLE NOT NULL
        )
    """)
    
    # Create indexes for fast queries
    con.execute("CREATE INDEX IF NOT EXISTS idx_price_points_ts ON price_points(ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_price_points_token ON price_points(token)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_price_points_token_ts ON price_points(token, ts)")
    
    con.execute("CREATE INDEX IF NOT EXISTS idx_archive_ts ON price_points_archive(ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_archive_token ON price_points_archive(token)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_archive_token_ts ON price_points_archive(token, ts)")
    
    print(f"Database initialized at: {DB_PATH}")


def fetch_prices() -> dict | None:
    """Fetch token prices from Jupiter API with retry logic."""
    ids = ",".join(TOKENS.values())
    
    for attempt in range(3):
        try:
            response = requests.get(
                JUPITER_API_URL, 
                params={"ids": ids}, 
                timeout=5
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(1)
    
    return None


def insert_prices(con: duckdb.DuckDBPyConnection, prices_data: dict) -> int:
    """Insert prices into both hot and archive tables. Returns count of records inserted."""
    if not prices_data:
        return 0
    
    ts = datetime.now()
    records = []
    
    for mint, data in prices_data.items():
        if data and "usdPrice" in data:
            token = MINT_TO_TOKEN.get(mint)
            if token:
                price = float(data["usdPrice"])
                records.append((ts, token, price))
    
    if not records:
        return 0
    
    # Batch insert into both tables
    con.executemany(
        "INSERT INTO price_points (ts, token, price) VALUES (?, ?, ?)",
        records
    )
    con.executemany(
        "INSERT INTO price_points_archive (ts, token, price) VALUES (?, ?, ?)",
        records
    )
    
    return len(records)


def cleanup_old_data(con: duckdb.DuckDBPyConnection) -> int:
    """Remove data older than 24 hours from hot table."""
    cutoff = datetime.now() - timedelta(hours=24)
    
    result = con.execute(
        "DELETE FROM price_points WHERE ts < ? RETURNING *",
        [cutoff]
    ).fetchall()
    
    deleted = len(result)
    if deleted > 0:
        print(f"Cleaned up {deleted} old records from hot table")
    
    return deleted


def display_prices(prices_data: dict) -> None:
    """Display current prices to console."""
    if not prices_data:
        print("No price data received")
        return
    
    parts = []
    for mint, data in prices_data.items():
        token = MINT_TO_TOKEN.get(mint, "???")
        if data and "usdPrice" in data:
            price = float(data["usdPrice"])
            parts.append(f"{token}: ${price:,.2f}")
        else:
            parts.append(f"{token}: --")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {' | '.join(parts)}")


def get_latest_prices(con: duckdb.DuckDBPyConnection) -> dict:
    """Get the most recent price for each token (utility function for trading bot)."""
    result = con.execute("""
        SELECT token, price, ts
        FROM price_points
        WHERE (token, ts) IN (
            SELECT token, MAX(ts) FROM price_points GROUP BY token
        )
    """).fetchall()
    
    return {row[0]: {"price": row[1], "ts": row[2]} for row in result}


def get_price_history(con: duckdb.DuckDBPyConnection, token: str, hours: float = 1.0) -> list:
    """Get price history for a token (utility function for trading bot)."""
    cutoff = datetime.now() - timedelta(hours=hours)
    
    result = con.execute("""
        SELECT ts, price FROM price_points
        WHERE token = ? AND ts >= ?
        ORDER BY ts ASC
    """, [token, cutoff]).fetchall()
    
    return [(row[0], row[1]) for row in result]


def main():
    """Main loop to fetch and store Jupiter prices."""
    print("=" * 60)
    print("Jupiter Price Fetcher - DuckDB Edition")
    print(f"Tokens: {', '.join(TOKENS.keys())}")
    print(f"Fetch interval: {FETCH_INTERVAL_SECONDS}s")
    print("=" * 60)
    
    # Initialize database
    con = duckdb.connect(str(DB_PATH))
    init_database(con)
    
    iteration = 0
    cleanup_iterations = int(CLEANUP_INTERVAL / FETCH_INTERVAL_SECONDS)
    
    try:
        while True:
            loop_start = time.perf_counter()
            
            # Fetch and store prices
            prices = fetch_prices()
            if prices:
                display_prices(prices)
                count = insert_prices(con, prices)
                if count == 0:
                    print("Warning: No valid prices to insert")
            
            # Periodic cleanup
            iteration += 1
            if iteration >= cleanup_iterations:
                cleanup_old_data(con)
                iteration = 0
            
            # Precise sleep to maintain interval
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0, FETCH_INTERVAL_SECONDS - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        con.close()
        print("Database connection closed.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        
        if cmd == "--cleanup":
            print("Running manual cleanup...")
            con = duckdb.connect(str(DB_PATH))
            init_database(con)
            cleanup_old_data(con)
            con.close()
            
        elif cmd == "--stats":
            print("Database statistics:")
            con = duckdb.connect(str(DB_PATH))
            init_database(con)
            
            hot = con.execute("SELECT COUNT(*) FROM price_points").fetchone()[0]
            archive = con.execute("SELECT COUNT(*) FROM price_points_archive").fetchone()[0]
            
            print(f"  Hot table (24h): {hot:,} records")
            print(f"  Archive table:   {archive:,} records")
            
            if hot > 0:
                latest = get_latest_prices(con)
                print("\nLatest prices:")
                for token, data in latest.items():
                    print(f"  {token}: ${data['price']:,.2f} @ {data['ts']}")
            
            con.close()
            
        elif cmd == "--help":
            print("Usage: python get_prices_from_jupiter.py [OPTIONS]")
            print("\nOptions:")
            print("  --cleanup   Run manual cleanup of old data")
            print("  --stats     Show database statistics")
            print("  --help      Show this help message")
            print("\nNo arguments starts the price fetcher loop.")
    else:
        main()


"""
Jupiter Price Fetcher with TradingDataEngine
=============================================
High-performance price fetcher using in-memory DuckDB with zero locks.

Architecture:
- TradingDataEngine: In-memory DuckDB (24hr hot storage, zero lock contention)
- MySQL: Full historical master storage (via background sync)

Data flow:
1. Fetch prices from Jupiter API every 2 seconds
2. Write to TradingDataEngine (non-blocking, queued)
3. Engine auto-syncs to MySQL every 30s
4. Engine auto-cleans data older than 24 hours

Jupiter API Migration (effective Jan 31, 2026):
- Old: https://lite-api.jup.ag/price/v3 (deprecated)
- New: https://api.jup.ag/price/v2/price (requires API key)
- Docs: https://dev.jup.ag/portal/migrate-from-lite-api
"""

import sys
import os
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path, encoding='utf-8')
    else:
        load_dotenv()  # Try default locations
except ImportError:
    pass  # dotenv not installed, use regular env vars

import requests
import time
import duckdb
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from core.database import get_duckdb, get_trading_engine
from core.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("jupiter_prices")

# --- Configuration ---
# 1 request/second with 3 bundled tokens = 60 req/min (exactly at free tier limit)
# Bundled call: GET /price/v3?ids=SOL,BTC,ETH returns all 3 in one response
FETCH_INTERVAL_SECONDS = 1.0  # 1 second = 60 req/min (bundled, within free tier)
CLEANUP_INTERVAL = 3600  # Run cleanup every hour

# Jupiter Price API v3 (requires API key from portal.jup.ag)
# Migration guide: https://dev.jup.ag/portal/migrate-from-lite-api
JUPITER_API_URL = "https://api.jup.ag/price/v3"

# API Key - Get yours free at https://portal.jup.ag
# Free tier: 60 requests/minute
# Set via environment variable or .env file (supports both JUPITER_API_KEY and jupiter_api_key)
JUPITER_API_KEY = (
    os.getenv("JUPITER_API_KEY", "") or 
    os.getenv("jupiter_api_key", "") or 
    getattr(settings, 'jupiter_api_key', "")
)

# Token mint addresses on Solana
TOKENS = {
    "SOL": "So11111111111111111111111111111111111111112",   # Native SOL
    "BTC": "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  # Wrapped BTC (Portal)
    "ETH": "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # Wrapped ETH (Portal)
}

# Reverse lookup
MINT_TO_TOKEN = {v: k for k, v in TOKENS.items()}

# Legacy database path (for backward compatibility)
LEGACY_DB_PATH = Path(__file__).parent / "prices.duckdb"

# Rate limiting state
_last_rate_limit_time = 0
_backoff_seconds = 0
_consecutive_errors = 0


def init_legacy_database(con: duckdb.DuckDBPyConnection) -> None:
    """Initialize DuckDB hot table (24hr fast storage).
    
    Uses the standard schema: id, ts_idx, coin_id, value, created_at
    This matches TradingDataEngine and master2.py schemas.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS price_points (
            id BIGINT PRIMARY KEY,
            ts_idx BIGINT,
            coin_id INTEGER NOT NULL,
            value DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_price_points_created_at ON price_points(created_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_price_points_coin_id ON price_points(coin_id)")


def fetch_prices() -> dict | None:
    """
    Fetch token prices from Jupiter Price API v2.
    
    API Docs: https://dev.jup.ag/docs/price-api
    Migration: https://dev.jup.ag/portal/migrate-from-lite-api
    
    Batch request format:
        GET https://api.jup.ag/price/v2/price?ids=MINT1,MINT2,MINT3
        Header: x-api-key: YOUR_API_KEY
    
    Response format:
        {
            "data": {
                "MINT1": {"id": "...", "price": "123.45", ...},
                "MINT2": {"id": "...", "price": "456.78", ...}
            },
            "timeTaken": 0.001
        }
    
    Rate limits (free tier): 60 requests/minute
    
    Returns:
        Dict mapping mint address to price data, or None on failure
    """
    global _last_rate_limit_time, _backoff_seconds, _consecutive_errors
    
    # Check if API key is configured
    if not JUPITER_API_KEY:
        logger.error(
            "[ERROR] JUPITER_API_KEY not set! "
            "Get a free API key at https://portal.jup.ag and set JUPITER_API_KEY environment variable"
        )
        return None
    
    # Check if we're in backoff period (reduced from 60s to max 5s for trading accuracy)
    if _backoff_seconds > 0:
        elapsed = time.time() - _last_rate_limit_time
        if elapsed < _backoff_seconds:
            remaining = _backoff_seconds - elapsed
            # Log at ERROR level so it appears in scheduler_errors.log
            logger.error(f"[RATE LIMIT BACKOFF] {remaining:.1f}s remaining - NO PRICE RECORDED THIS CYCLE")
            return None
        else:
            logger.info(f"Backoff period ended after {_backoff_seconds}s, resuming API calls")
            _backoff_seconds = 0
            _consecutive_errors = 0  # Reset on resume
    
    # Build the batch request URL
    ids = ",".join(TOKENS.values())
    url = f"{JUPITER_API_URL}?ids={ids}"
    
    # Headers with API key (required for api.jup.ag)
    headers = {
        "x-api-key": JUPITER_API_KEY,
        "Accept": "application/json"
    }
    
    logger.debug(f"API Request: {url}")
    
    # Single attempt - no retries (fail fast, try again next cycle)
    # Retries can cause rate limiting when running at high frequency
    try:
        start_time = time.perf_counter()
        response = requests.get(url, headers=headers, timeout=5)  # Reduced timeout
        elapsed = (time.perf_counter() - start_time) * 1000
        
        # Log rate limit headers if present (for debugging)
        rate_limit_headers = {
            k: v for k, v in response.headers.items() 
            if k.lower().startswith('x-ratelimit') or k.lower().startswith('retry-after')
        }
        if rate_limit_headers:
            logger.debug(f"Rate limit headers: {rate_limit_headers}")
        
        # Check for authentication errors (401)
        if response.status_code == 401:
            logger.error(
                "[AUTH ERROR] UNAUTHORIZED (401)! Invalid or missing API key. "
                "Get a free key at https://portal.jup.ag"
            )
            return None
        
        # Check for rate limiting (429)
        if response.status_code == 429:
            _consecutive_errors += 1
            _last_rate_limit_time = time.time()
            
            # CRITICAL: Short backoff for trading accuracy
            # Max 5 seconds backoff (was 60s) - trading bots need continuous data
            # Linear backoff: 2s, 3s, 4s, 5s max
            _backoff_seconds = min(1 + _consecutive_errors, 5)
            
            # Only use Retry-After if it's reasonable (< 10s)
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    server_wait = int(retry_after)
                    if server_wait <= 10:
                        _backoff_seconds = server_wait
                except ValueError:
                    pass
            
            # Log at ERROR level so it appears in scheduler_errors.log
            logger.error(
                f"[RATE LIMIT 429] Jupiter API rate limited! Backing off {_backoff_seconds}s "
                f"(consecutive errors: {_consecutive_errors}). Missing price data!"
            )
            return None
        
        # Check for other errors
        if response.status_code != 200:
            logger.error(f"[API ERROR {response.status_code}] Jupiter API failed - NO PRICE RECORDED")
            return None
        
        # Parse response
        data = response.json()
        
        # Reset error counter on success
        _consecutive_errors = 0
        
        # Log success with timing
        logger.debug(f"API OK ({elapsed:.0f}ms)")
        
        # Parse response - v3 API returns mint address as key with usdPrice field
        prices = {}
        
        # Check if response has "data" wrapper (some API versions)
        price_data_dict = data.get("data", data) if isinstance(data, dict) else data
        
        for mint, price_data in price_data_dict.items():
            if isinstance(price_data, dict):
                # Try usdPrice first (v3), then price (older versions)
                price_val = price_data.get("usdPrice") or price_data.get("price")
                if price_val is not None:
                    prices[mint] = {"price": float(price_val)}
        
        if prices:
            return prices
        else:
            logger.error("[PARSE ERROR] No valid prices in Jupiter response - NO PRICE RECORDED")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("[TIMEOUT] Jupiter API timeout - NO PRICE RECORDED")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"[CONNECTION ERROR] Jupiter API failed: {e} - NO PRICE RECORDED")
        return None
    except Exception as e:
        logger.error(f"[UNEXPECTED ERROR] Jupiter fetch failed: {e}")
        return None


def insert_prices_via_engine(prices_data: dict) -> tuple[int, bool]:
    """
    Insert prices via TradingDataEngine (non-blocking).
    
    Returns:
        Tuple of (record_count, success)
    """
    if not prices_data:
        return 0, False
    
    ts = datetime.utcnow()
    
    try:
        engine = get_trading_engine()
        count = 0
        
        for mint, data in prices_data.items():
            if data and "price" in data:
                token = MINT_TO_TOKEN.get(mint)
                if token:
                    price = float(data["price"])
                    engine.write('prices', {
                        'ts': ts,
                        'token': token,
                        'price': price
                    })
                    count += 1
        
        return count, True
        
    except Exception as e:
        logger.error(f"Engine write error: {e}")
        return 0, False


def insert_prices_dual_write(prices_data: dict) -> tuple[int, bool, bool]:
    """
    Insert prices - OPTIMIZED for speed.
    
    Hot path: Only TradingDataEngine (in-memory, non-blocking)
    Background: Engine auto-syncs to MySQL every 30s
    
    File-based DuckDB writes removed from hot path - they were causing
    5+ second latencies due to file I/O and lock contention.
    
    Returns:
        Tuple of (record_count, duckdb_success, mysql_success)
    """
    if not prices_data:
        return 0, False, False
    
    ts = datetime.utcnow()
    records = []
    
    for mint, data in prices_data.items():
        if data and "price" in data:
            token = MINT_TO_TOKEN.get(mint)
            if token:
                price = float(data["price"])
                records.append((ts, token, price))
    
    if not records:
        return 0, False, False
    
    duckdb_success = False
    mysql_success = False
    
    # HOT PATH: Write ONLY to TradingDataEngine (in-memory, instant)
    # The engine handles background MySQL sync automatically
    try:
        engine = get_trading_engine()
        if engine._running:
            for ts_val, token, price in records:
                engine.write('prices', {
                    'ts': ts_val,
                    'token': token,
                    'price': price
                })
            duckdb_success = True
            mysql_success = True  # Engine handles MySQL sync in background
            return len(records), duckdb_success, mysql_success
    except Exception as e:
        logger.debug(f"Engine write skipped: {e}")
    
    # FALLBACK: If engine not available, write to file-based DuckDB
    # This path should rarely be hit when scheduler is running
    if not duckdb_success:
        try:
            with get_duckdb("central") as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS price_points (id BIGINT PRIMARY KEY, ts_idx BIGINT, value DOUBLE, created_at TIMESTAMP, coin_id INTEGER)")
                base_id = int(ts.timestamp() * 1000000)
                for i, (ts_val, token, price) in enumerate(records):
                    ts_idx = int(ts_val.timestamp() * 1000)
                    coin_id = 5 if token == 'SOL' else (6 if token == 'BTC' else 7)
                    unique_id = base_id + (coin_id * 100) + i
                    conn.execute(
                        "INSERT OR IGNORE INTO price_points (id, ts_idx, value, created_at, coin_id) VALUES (?, ?, ?, ?, ?)",
                        [unique_id, ts_idx, price, ts_val, coin_id]
                    )
                duckdb_success = True
        except Exception as e:
            logger.error(f"Central DuckDB write error: {e}")
    
    return len(records), duckdb_success, mysql_success


def cleanup_old_data() -> int:
    """Remove data older than 24 hours from DuckDB hot table."""
    cutoff = datetime.now() - timedelta(hours=24)
    deleted = 0
    
    try:
        with get_duckdb("prices") as con:
            result = con.execute(
                "DELETE FROM price_points WHERE ts < ? RETURNING *",
                [cutoff]
            ).fetchall()
            deleted = len(result)
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old records from DuckDB")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
    
    return deleted


def display_prices(prices_data: dict) -> None:
    """Display current prices to console."""
    if not prices_data:
        return
    
    parts = []
    for mint, data in prices_data.items():
        token = MINT_TO_TOKEN.get(mint, "???")
        if data and "price" in data:
            price = float(data["price"])
            parts.append(f"{token}: ${price:,.2f}")
        else:
            parts.append(f"{token}: --")
    
    logger.info(f"[PRICES] {' | '.join(parts)}")


def get_latest_prices() -> dict:
    """Get the most recent price for each token."""
    try:
        engine = get_trading_engine()
        if engine._running:
            results = engine.read("""
                SELECT token, price, ts
                FROM prices
                WHERE (token, ts) IN (
                    SELECT token, MAX(ts) FROM prices GROUP BY token
                )
            """)
            return {row['token']: {"price": row['price'], "ts": row['ts']} for row in results}
    except:
        pass
    
    try:
        with get_duckdb("prices") as con:
            result = con.execute("""
                SELECT token, price, ts
                FROM price_points
                WHERE (token, ts) IN (
                    SELECT token, MAX(ts) FROM price_points GROUP BY token
                )
            """).fetchall()
            return {row[0]: {"price": row[1], "ts": row[2]} for row in result}
    except:
        return {}


def get_price_history(token: str, hours: float = 1.0) -> list:
    """Get price history for a token."""
    cutoff = datetime.now() - timedelta(hours=hours)
    
    try:
        engine = get_trading_engine()
        if engine._running:
            results = engine.read("""
                SELECT ts, price FROM prices
                WHERE token = ? AND ts >= ?
                ORDER BY ts ASC
            """, [token, cutoff])
            return [(row['ts'], row['price']) for row in results]
    except:
        pass
    
    try:
        with get_duckdb("prices") as con:
            result = con.execute("""
                SELECT ts, price FROM price_points
                WHERE token = ? AND ts >= ?
                ORDER BY ts ASC
            """, [token, cutoff]).fetchall()
            return [(row[0], row[1]) for row in result]
    except:
        return []


def fetch_and_store_once() -> tuple[int, bool, bool]:
    """
    Fetch prices once and store them (for scheduler use).
    
    Returns:
        Tuple of (record_count, duckdb_success, mysql_success)
    """
    prices = fetch_prices()
    if prices:
        count, duck_ok, mysql_ok = insert_prices_dual_write(prices)
        if count > 0:
            # Log success with prices
            price_str = ", ".join([
                f"{MINT_TO_TOKEN.get(m, '?')}=${d['price']:.2f}" 
                for m, d in prices.items() if d
            ])
            logger.debug(f"Stored {count} prices: {price_str}")
        return count, duck_ok, mysql_ok
    return 0, False, False


def test_api_connection() -> None:
    """Test the Jupiter API connection and show detailed info."""
    print("=" * 60)
    print("Jupiter API Connection Test")
    print("=" * 60)
    
    ids = ",".join(TOKENS.values())
    url = f"{JUPITER_API_URL}?ids={ids}"
    
    print(f"\nEndpoint: {JUPITER_API_URL}")
    print(f"Full URL: {url}")
    print(f"\nAPI Key: {'[OK] Set (' + JUPITER_API_KEY[:8] + '...)' if JUPITER_API_KEY else '[ERROR] NOT SET'}")
    print(f"\nTokens:")
    for token, mint in TOKENS.items():
        print(f"  {token}: {mint}")
    
    if not JUPITER_API_KEY:
        logger.warning("JUPITER_API_KEY not set! Get a free API key at: https://portal.jup.ag")
        print(f"\n   Free tier: 60 requests/minute")
        return
    
    headers = {
        "x-api-key": JUPITER_API_KEY,
        "Accept": "application/json"
    }
    
    print(f"\nMaking request with API key...")
    
    try:
        start = time.perf_counter()
        response = requests.get(url, headers=headers, timeout=10)
        elapsed = (time.perf_counter() - start) * 1000
        
        print(f"\nResponse:")
        print(f"  Status: {response.status_code}")
        print(f"  Time: {elapsed:.0f}ms")
        
        # Show all headers
        print(f"\nHeaders:")
        for key, value in response.headers.items():
            if any(x in key.lower() for x in ['rate', 'limit', 'retry', 'x-']):
                print(f"  {key}: {value}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n[OK] Success! Response:")
            
            if "data" in data:
                print(f"  Format: v6 (data field)")
                for mint, price_data in data["data"].items():
                    token = MINT_TO_TOKEN.get(mint, mint[:8])
                    if price_data and "price" in price_data:
                        print(f"  {token}: ${float(price_data['price']):,.4f}")
            else:
                print(f"  Format: legacy")
                print(f"  Raw: {str(data)[:500]}")
        else:
            print(f"\n[ERROR] Response:")
            print(f"  {response.text[:500]}")
            
    except Exception as e:
        logger.error(f"Request Failed: {e}")


def main():
    """Main loop to fetch and store Jupiter prices."""
    print("=" * 60)
    print("Jupiter Price Fetcher - Dual Write (DuckDB + MySQL)")
    print(f"Tokens: {', '.join(TOKENS.keys())}")
    print(f"Fetch interval: {FETCH_INTERVAL_SECONDS}s")
    print(f"API: {JUPITER_API_URL}")
    print(f"API Key: {'[OK] Set' if JUPITER_API_KEY else '[ERROR] NOT SET'}")
    print("=" * 60)
    
    if not JUPITER_API_KEY:
        print("\n[ERROR] JUPITER_API_KEY environment variable not set!")
        print("   Jupiter requires an API key as of Jan 31, 2026.")
        print("   Get a FREE key at: https://portal.jup.ag")
        print("   Then set: JUPITER_API_KEY=your_key_here")
        print("\n   Free tier provides 60 requests/minute.")
        return
    
    # Initialize legacy database
    with get_duckdb("prices") as con:
        init_legacy_database(con)
        logger.info(f"Database initialized at: {LEGACY_DB_PATH}")
    
    iteration = 0
    cleanup_iterations = int(CLEANUP_INTERVAL / FETCH_INTERVAL_SECONDS)
    success_count = 0
    error_count = 0
    
    try:
        while True:
            loop_start = time.perf_counter()
            
            # Fetch and store prices
            prices = fetch_prices()
            if prices:
                display_prices(prices)
                count, duck_ok, mysql_ok = insert_prices_dual_write(prices)
                if count > 0:
                    success_count += 1
                else:
                    error_count += 1
                    logger.warning("No valid prices to insert")
                if not duck_ok or not mysql_ok:
                    logger.warning(f"Partial write - DuckDB: {duck_ok}, MySQL: {mysql_ok}")
            else:
                error_count += 1
            
            # Periodic status report (every 60 iterations)
            if iteration > 0 and iteration % 60 == 0:
                total = success_count + error_count
                rate = (success_count / total * 100) if total > 0 else 0
                logger.info(f"[STATS] {success_count}/{total} successful ({rate:.1f}%)")
            
            # Periodic cleanup
            iteration += 1
            if iteration >= cleanup_iterations:
                cleanup_old_data()
                iteration = 0
            
            # Precise sleep to maintain interval
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0, FETCH_INTERVAL_SECONDS - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
        total = success_count + error_count
        if total > 0:
            print(f"Final stats: {success_count}/{total} successful ({success_count/total*100:.1f}%)")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        
        if cmd == "--test":
            test_api_connection()
            
        elif cmd == "--cleanup":
            print("Running manual cleanup...")
            cleanup_old_data()
            
        elif cmd == "--stats":
            print("Database statistics:")
            
            try:
                with get_duckdb("prices") as con:
                    hot = con.execute("SELECT COUNT(*) FROM price_points").fetchone()[0]
                    print(f"  DuckDB Hot (24h): {hot:,} records")
            except Exception as e:
                logger.error(f"DuckDB error: {e}")
            
            # MySQL archive stats not shown (archive is optional)
            
            latest = get_latest_prices()
            if latest:
                print("\nLatest prices:")
                for token, data in latest.items():
                    print(f"  {token}: ${data['price']:,.2f} @ {data['ts']}")
            
        elif cmd == "--once":
            print("Fetching prices once...")
            count, duck_ok, mysql_ok = fetch_and_store_once()
            print(f"Inserted {count} records - DuckDB: {duck_ok}, MySQL: {mysql_ok}")
            
        elif cmd == "--help":
            print("Usage: python get_prices_from_jupiter.py [OPTIONS]")
            print("\nOptions:")
            print("  --test      Test API connection and show detailed info")
            print("  --cleanup   Run manual cleanup of old data")
            print("  --stats     Show database statistics")
            print("  --once      Fetch and store prices once (for scheduler)")
            print("  --help      Show this help message")
            print("\nNo arguments starts the price fetcher loop.")
        else:
            print(f"Unknown command: {cmd}")
            print("Use --help for usage information")
    else:
        main()

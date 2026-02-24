"""
Jupiter Price Fetcher (PostgreSQL-only)
======================================
Fetches token prices from Jupiter and writes directly to PostgreSQL.

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
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from core.database import get_postgres
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

# Rate limiting state
_last_rate_limit_time = 0
_backoff_seconds = 0
_consecutive_errors = 0


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


def insert_prices_postgres(prices_data: dict) -> tuple[int, bool]:
    """
    Insert prices into PostgreSQL (source of truth).

    Returns:
        Tuple of (record_count, postgres_success)
    """
    if not prices_data:
        return 0, False
    
    ts = datetime.now(timezone.utc)
    records = []
    
    for mint, data in prices_data.items():
        if data and "price" in data:
            token = MINT_TO_TOKEN.get(mint)
            if token:
                price = float(data["price"])
                records.append((ts, token, price))
    
    if not records:
        return 0, False
    
    postgres_success = False
    
    try:
        from core.database import postgres_insert_many
        price_records = [
            {
                'timestamp': ts_val,
                'token': token,
                'price': price,
                'source': 'jupiter'
            }
            for ts_val, token, price in records
        ]
        inserted = postgres_insert_many('prices', price_records)
        if inserted > 0:
            postgres_success = True
            logger.debug(f"Wrote {inserted} prices directly to PostgreSQL")
    except Exception as e:
        logger.error(f"PostgreSQL write error: {e}", exc_info=True)
    
    return len(records), postgres_success


# Backward-compatible name (older callers expected dual-write semantics)
def insert_prices_dual_write(prices_data: dict) -> tuple[int, bool]:
    return insert_prices_postgres(prices_data)


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
        from core.database import get_postgres
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (token) token, price, timestamp
                    FROM prices
                    WHERE source = 'jupiter'
                    ORDER BY token, timestamp DESC
                    """
                )
                rows = cursor.fetchall()
        return {r["token"]: {"price": r["price"], "ts": r["timestamp"]} for r in rows or []}
    except Exception:
        return {}


def get_price_history(token: str, hours: float = 1.0) -> list:
    """Get price history for a token."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    try:
        from core.database import get_postgres
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT timestamp, price
                    FROM prices
                    WHERE token = %s AND timestamp >= %s AND source = 'jupiter'
                    ORDER BY timestamp ASC
                    """,
                    [token, cutoff],
                )
                rows = cursor.fetchall()
        return [(r["timestamp"], r["price"]) for r in rows or []]
    except Exception:
        return []


def fetch_and_store_once() -> tuple[int, bool]:
    """
    Fetch prices once and store them (for scheduler use).
    
    Returns:
        Tuple of (record_count, postgres_success)
    """
    prices = fetch_prices()
    if prices:
        count, pg_ok = insert_prices_postgres(prices)
        if count > 0:
            # Log success with prices
            price_str = ", ".join([
                f"{MINT_TO_TOKEN.get(m, '?')}=${d['price']:.2f}" 
                for m, d in prices.items() if d
            ])
            logger.debug(f"Stored {count} prices: {price_str}")
        return count, pg_ok
    return 0, False


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
    print("Jupiter Price Fetcher - PostgreSQL Only")
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
    
    iteration = 0
    success_count = 0
    error_count = 0
    
    try:
        while True:
            loop_start = time.perf_counter()
            
            # Fetch and store prices
            prices = fetch_prices()
            if prices:
                display_prices(prices)
                count, pg_ok = insert_prices_postgres(prices)
                if count > 0:
                    success_count += 1
                else:
                    error_count += 1
                    logger.warning("No valid prices to insert")
                if not pg_ok:
                    logger.warning("PostgreSQL write failed for this cycle")
            else:
                error_count += 1
            
            # Periodic status report (every 60 iterations)
            if iteration > 0 and iteration % 60 == 0:
                total = success_count + error_count
                rate = (success_count / total * 100) if total > 0 else 0
                logger.info(f"[STATS] {success_count}/{total} successful ({rate:.1f}%)")
            
            iteration += 1
            
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
            
        elif cmd == "--stats":
            print("Database statistics:")
            
            try:
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) AS cnt FROM prices WHERE source = 'jupiter'")
                        total = (cursor.fetchone() or {}).get("cnt", 0)
                        cursor.execute("SELECT MAX(timestamp) AS max_ts FROM prices WHERE source = 'jupiter'")
                        max_ts = (cursor.fetchone() or {}).get("max_ts")
                print(f"  PostgreSQL prices (jupiter): {total:,} records")
                if max_ts:
                    print(f"  Latest timestamp: {max_ts}")
            except Exception as e:
                logger.error(f"PostgreSQL stats error: {e}")
            
            latest = get_latest_prices()
            if latest:
                print("\nLatest prices:")
                for token, data in latest.items():
                    print(f"  {token}: ${data['price']:,.2f} @ {data['ts']}")
            
        elif cmd == "--once":
            print("Fetching prices once...")
            count, pg_ok = fetch_and_store_once()
            print(f"Inserted {count} records - PostgreSQL: {pg_ok}")
            
        elif cmd == "--help":
            print("Usage: python get_prices_from_jupiter.py [OPTIONS]")
            print("\nOptions:")
            print("  --test      Test API connection and show detailed info")
            print("  --stats     Show database statistics")
            print("  --once      Fetch and store prices once (for scheduler)")
            print("  --help      Show this help message")
            print("\nNo arguments starts the price fetcher loop.")
        else:
            print(f"Unknown command: {cmd}")
            print("Use --help for usage information")
    else:
        main()

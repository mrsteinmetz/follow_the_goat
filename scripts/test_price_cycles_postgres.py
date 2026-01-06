"""
Test price cycle logic in-memory using recent prices from PostgreSQL.

This script:
- Pulls recent prices from the PostgreSQL archive (default: last 24h, token SOL)
- Seeds an in-memory DuckDB (central) with a clean prices/price_analysis/cycle_tracker
- Runs the existing price cycle processor over all loaded prices
- Prints a concise summary of inserted cycles/analysis rows

SAFE: Works entirely in-memory (central DuckDB). Does NOT touch master.py or the
running TradingDataEngine. Intended to validate cycle resets before a restart.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, get_duckdb  # noqa: E402

# Import create_price_cycles via its directory (numeric path)
CYCLES_PATH = PROJECT_ROOT / "000data_feeds" / "2_create_price_cycles"
if str(CYCLES_PATH) not in sys.path:
    sys.path.insert(0, str(CYCLES_PATH))
import create_price_cycles as cycles  # noqa: E402


def fetch_prices_from_postgres(hours: int, limit: int | None, token: str) -> List[Dict[str, Any]]:
    """Fetch recent prices from PostgreSQL."""
    with get_postgres() as pg:
        if not pg:
            raise RuntimeError("PostgreSQL unavailable (check settings.postgres)")
        with pg.cursor() as cur:
            query = """
                SELECT id, timestamp AS ts, token, price
                FROM prices
                WHERE token = %s AND timestamp >= NOW() - (%s || ' hours')::interval
                ORDER BY id ASC
            """
            params = [token, hours]
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, params)
            rows = cur.fetchall()
            return rows or []


def seed_duckdb_with_prices(prices: List[Dict[str, Any]]) -> int:
    """Create a clean in-memory DuckDB dataset and bulk load prices."""
    with get_duckdb("central", read_only=False) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                id BIGINT,
                ts TIMESTAMP,
                token VARCHAR,
                price DOUBLE
            )
        """)
        conn.execute("DELETE FROM prices")
        conn.execute("DELETE FROM price_analysis WHERE coin_id = 5")
        conn.execute("DELETE FROM cycle_tracker WHERE coin_id = 5")

        if not prices:
            return 0

        try:
            import pandas as pd
            import pyarrow as pa

            df = pd.DataFrame(prices)
            df["price"] = df["price"].astype(float)
            schema = pa.schema([
                pa.field("id", pa.int64()),
                pa.field("ts", pa.timestamp("us")),
                pa.field("token", pa.string()),
                pa.field("price", pa.float64()),
            ])
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            conn.register("_temp_prices", table)
            conn.execute("""
                INSERT INTO prices (id, ts, token, price)
                SELECT id, ts, token, price FROM _temp_prices
            """)
            conn.unregister("_temp_prices")
        except ImportError:
            # Fallback: simple executemany if pandas/pyarrow unavailable
            tuples = [(r["id"], r["ts"], r["token"], float(r["price"])) for r in prices]
            conn.executemany(
                "INSERT INTO prices (id, ts, token, price) VALUES (?, ?, ?, ?)",
                tuples,
            )
        return len(prices)


def summarize_results() -> Dict[str, Any]:
    """Return simple counts and a snapshot of recent cycles."""
    summary: Dict[str, Any] = {}
    with get_duckdb("central", read_only=True) as conn:
        price_count = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        analysis_count = conn.execute(
            "SELECT COUNT(*) FROM price_analysis WHERE coin_id = 5"
        ).fetchone()[0]
        cycle_count = conn.execute(
            "SELECT COUNT(*) FROM cycle_tracker WHERE coin_id = 5"
        ).fetchone()[0]
        latest_cycles = conn.execute(
            """
            SELECT id, threshold, cycle_start_time, cycle_end_time,
                   highest_price_reached, lowest_price_reached,
                   max_percent_increase, max_percent_increase_from_lowest,
                   total_data_points
            FROM cycle_tracker
            WHERE coin_id = 5
            ORDER BY id DESC
            LIMIT 5
            """
        ).fetchall()
    summary["prices"] = price_count
    summary["price_analysis"] = analysis_count
    summary["cycles"] = cycle_count
    summary["latest_cycles"] = latest_cycles
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Validate price cycle logic using PostgreSQL prices in-memory."
    )
    parser.add_argument("--hours", type=int, default=24, help="Hours of data to load (default: 24)")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on price rows")
    parser.add_argument("--token", type=str, default="SOL", help="Token symbol to filter (default: SOL)")
    args = parser.parse_args()

    print(f"Loading prices from PostgreSQL (token={args.token}, last {args.hours}h, limit={args.limit or 'none'})...")
    prices = fetch_prices_from_postgres(args.hours, args.limit, args.token)
    print(f"Fetched {len(prices)} price rows.")

    loaded = seed_duckdb_with_prices(prices)
    print(f"Seeded DuckDB 'central' with {loaded} prices (cleaned price_analysis/cycle_tracker for coin_id=5).")

    if loaded == 0:
        print("No prices loaded; nothing to process.")
        return

    # Ensure cycles exist and process everything
    cycles.ensure_all_cycles_exist()
    processed = cycles.process_all_historical_prices(batch_size=1000)
    print(f"Processed {processed} price points into cycles.")

    summary = summarize_results()
    print(f"Rows -> prices: {summary['prices']}, price_analysis: {summary['price_analysis']}, cycles: {summary['cycles']}")
    if summary["latest_cycles"]:
        print("Latest cycles (most recent first):")
        for row in summary["latest_cycles"]:
            print(
                f"  id={row[0]} thr={row[1]} start={row[2]} end={row[3]} "
                f"high={row[4]} low={row[5]} max%={row[6]} max_from_low%={row[7]} points={row[8]}"
            )
    else:
        print("No cycles created.")


if __name__ == "__main__":
    main()


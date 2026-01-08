"""
Benchmark DuckDB vs PostgreSQL for critical trading workloads.

Two modes:
- Core DB ops (default): run the hot queries/writes shared by
  train_validator and follow_the_goat against both DuckDB and PostgreSQL.
- Full cycles (optional): actually invoke train_validator.run_training_cycle()
  and follow_the_goat.run_single_cycle() on DuckDB to measure real end-to-end
  runtime. PostgreSQL doesn't have a full-cycle path yet, so it still runs the
  core ops there for comparison.

Usage examples:
  python scripts/benchmark_duckdb_vs_postgres.py
  python scripts/benchmark_duckdb_vs_postgres.py --iterations 3 --include-writes
  python scripts/benchmark_duckdb_vs_postgres.py --full-cycles --iterations 1
"""

import argparse
import json
import importlib.util
import statistics
import time
import uuid
from pathlib import Path
from typing import Callable, List, Tuple

from core.database import (
    get_duckdb,
    duckdb_execute_write,
    get_postgres,
    init_duckdb_tables,
)


def _time_call(fn: Callable[[], None]) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _ensure_duckdb_schema() -> None:
    """Create DuckDB tables if missing (idempotent)."""
    try:
        init_duckdb_tables("central")
        with get_duckdb("central") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS prices (
                    id BIGINT,
                    ts TIMESTAMP,
                    token VARCHAR,
                    price DOUBLE
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_token_ts ON prices(token, ts)")
    except Exception:
        # Safe to proceed; individual queries will still fail loudly if schema missing
        pass


def _duckdb_core_ops(include_writes: bool) -> None:
    """Hot-path DuckDB operations (reads + optional write/update/delete)."""
    _ensure_duckdb_schema()
    with get_duckdb("central", read_only=True) as cursor:
        cursor.execute(
            "SELECT price, ts FROM prices WHERE token = 'SOL' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        cursor.execute(
            "SELECT id FROM cycle_tracker WHERE cycle_end_time IS NULL ORDER BY id DESC LIMIT 1"
        ).fetchone()
        cursor.execute(
            "SELECT id FROM follow_the_goat_buyins ORDER BY id DESC LIMIT 1"
        ).fetchone()

    if not include_writes:
        return

    bench_wallet = f"BENCH_{uuid.uuid4().hex[:10]}"
    with get_duckdb("central", read_only=True) as cursor:
        next_id = cursor.execute(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM follow_the_goat_buyins"
        ).fetchone()[0]

    duckdb_execute_write(
        "central",
        """
        INSERT INTO follow_the_goat_buyins (
            id, play_id, wallet_address, original_trade_id, trade_signature,
            block_timestamp, quote_amount, base_amount, price, direction,
            our_entry_price, swap_response, live_trade, price_cycle,
            entry_log, pattern_validator_log, our_status, followed_at,
            higest_price_reached
        ) VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            next_id,
            0,
            bench_wallet,
            0,
            f"bench_sig_{bench_wallet}",
            10.0,
            10.0,
            10.0,
            "buy",
            10.0,
            None,
            0,
            None,
            json.dumps({"bench": True}),
            None,
            "benchmark",
            None,
            10.0,
        ],
        sync=True,
    )
    duckdb_execute_write(
        "central",
        "UPDATE follow_the_goat_buyins SET our_status = 'benchmark_done' WHERE id = ?",
        [next_id],
    )
    duckdb_execute_write(
        "central",
        "DELETE FROM follow_the_goat_buyins WHERE id = ?",
        [next_id],
    )


def _postgres_core_ops(include_writes: bool) -> None:
    """Hot-path PostgreSQL operations mirroring the DuckDB queries."""
    with get_postgres() as pg_conn:
        if not pg_conn:
            raise RuntimeError("PostgreSQL connection not available")
        with pg_conn.cursor() as cursor:
            cursor.execute(
                "SELECT price, timestamp FROM prices WHERE token = 'SOL' ORDER BY id DESC LIMIT 1"
            )
            cursor.fetchone()
            cursor.execute(
                "SELECT id FROM cycle_tracker WHERE cycle_end_time IS NULL ORDER BY id DESC LIMIT 1"
            )
            cursor.fetchone()
            cursor.execute(
                "SELECT id FROM follow_the_goat_buyins ORDER BY id DESC LIMIT 1"
            )
            cursor.fetchone()

            if not include_writes:
                return

            bench_wallet = f"BENCH_{uuid.uuid4().hex[:10]}"
            cursor.execute("""
                SELECT setval(
                    pg_get_serial_sequence('follow_the_goat_buyins','id'),
                    (SELECT COALESCE(MAX(id), 0) + 1 FROM follow_the_goat_buyins),
                    false
                )
            """)
            cursor.execute(
                """
                INSERT INTO follow_the_goat_buyins (
                    play_id, wallet_address, original_trade_id, trade_signature,
                    block_timestamp, quote_amount, base_amount, price, direction,
                    our_entry_price, live_trade, price_cycle, entry_log,
                    pattern_validator_log, our_status, followed_at, higest_price_reached
                ) VALUES (
                    %(play_id)s, %(wallet)s, %(orig_trade)s, %(signature)s, NOW(),
                    %(quote)s, %(base)s, %(price)s, %(direction)s,
                    %(our_entry)s, %(live)s, %(price_cycle)s, %(entry_log)s,
                    %(pattern_log)s, %(status)s, NOW(), %(hpr)s
                )
                RETURNING id
                """,
                {
                    "play_id": 0,
                    "wallet": bench_wallet,
                    "orig_trade": 0,
                    "signature": f"bench_sig_{bench_wallet}",
                    "quote": 10.0,
                    "base": 10.0,
                    "price": 10.0,
                    "direction": "buy",
                    "our_entry": 10.0,
                    "live": 0,
                    "price_cycle": None,
                    "entry_log": json.dumps({"bench": True}),
                    "pattern_log": None,
                    "status": "benchmark",
                    "hpr": 10.0,
                },
            )
            buyin_id = cursor.fetchone()["id"]
            cursor.execute(
                """
                UPDATE follow_the_goat_buyins
                SET our_status = 'benchmark_done'
                WHERE id = %s
                """,
                [buyin_id],
            )
            cursor.execute(
                "DELETE FROM follow_the_goat_buyins WHERE id = %s",
                [buyin_id],
            )


def _load_module_from_path(module_name: str, path: Path):
    """Load a module from an arbitrary path (handles 000trading package)."""
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if not spec or not spec.loader:
        raise ImportError(f"Could not load module {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _duckdb_full_cycles(include_writes: bool) -> None:
    """Invoke the real trading cycles on DuckDB."""
    project_root = Path(__file__).parent.parent
    follow_mod = _load_module_from_path(
        "follow_the_goat_bench", project_root / "000trading" / "follow_the_goat.py"
    )
    train_mod = _load_module_from_path(
        "train_validator_bench", project_root / "000trading" / "train_validator.py"
    )

    train_mod.run_training_cycle()
    follow_mod.run_single_cycle()
    if include_writes:
        # The cycles already write; nothing extra needed here.
        return


def _run_benchmark(
    label: str, fn: Callable[[], None], iterations: int
) -> Tuple[str, List[float]]:
    timings: List[float] = []
    for _ in range(iterations):
        timings.append(_time_call(fn))
    return label, timings


def _print_summary(label: str, timings: List[float]) -> None:
    if not timings:
        print(f"{label}: no timings recorded")
        return
    avg = statistics.mean(timings)
    p95 = statistics.quantiles(timings, n=20)[-1] if len(timings) > 1 else timings[0]
    print(f"{label}: avg={avg*1000:.1f}ms  p95={p95*1000:.1f}ms  runs={len(timings)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="DuckDB vs PostgreSQL benchmark")
    parser.add_argument(
        "--iterations", type=int, default=1, help="Number of runs per backend"
    )
    parser.add_argument(
        "--include-writes",
        action="store_true",
        help="Include insert/update/delete operations in each run",
    )
    parser.add_argument(
        "--full-cycles",
        action="store_true",
        help="Invoke train_validator + follow_the_goat on DuckDB as part of the benchmark.",
    )
    args = parser.parse_args()

    # Ensure DuckDB schema exists so benchmark queries don't fail if no tables are present.
    _ensure_duckdb_schema()

    runs: List[Tuple[str, List[float]]] = []

    # DuckDB core ops or full cycles
    if args.full_cycles:
        runs.append(
            _run_benchmark(
                "DuckDB-full-cycles",
                lambda: _duckdb_full_cycles(args.include_writes),
                args.iterations,
            )
        )
    else:
        runs.append(
            _run_benchmark(
                "DuckDB-core",
                lambda: _duckdb_core_ops(args.include_writes),
                args.iterations,
            )
        )

    # PostgreSQL core ops
    try:
        runs.append(
            _run_benchmark(
                "PostgreSQL-core",
                lambda: _postgres_core_ops(args.include_writes),
                args.iterations,
            )
        )
    except RuntimeError as exc:
        print(f"PostgreSQL-core: skipped ({exc})")

    print("\n=== Benchmark Summary ===")
    for label, timings in runs:
        _print_summary(label, timings)


if __name__ == "__main__":
    main()

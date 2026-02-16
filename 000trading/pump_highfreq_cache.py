"""
Pump High-Frequency DuckDB Cache
=================================
Read-only DuckDB cache for order book, transaction, and whale data.
Supports 1-5 second calculations for the pump signal fast path.

APPROVED DUCKDB EXCEPTION (see .cursorrules):
- PostgreSQL remains the single source of truth
- DuckDB is used purely as a read-only cache (rolling window)
- Cache is automatically synced incrementally from PostgreSQL (watermark-based)
- Strict single-writer discipline: sync opens read-write with PG advisory lock;
  all readers open read-only via duckdb.connect(path, read_only=True)

Tables cached from PostgreSQL:
- order_book_features  (timestamp, mid_price, spread_bps, depths, imbalance, ...)
- sol_stablecoin_trades (trade_timestamp, direction, price, sol_amount, ...)
- whale_movements      (timestamp, sol_change, direction, ...)
- prices               (timestamp, token, price) — SOL only

Usage:
    from pump_highfreq_cache import sync_highfreq_cache, get_highfreq_reader

    # Writer path (holds PG advisory lock):
    sync_highfreq_cache(lookback_minutes=30)

    # Reader path (read-only, safe for 5-10s polling):
    with get_highfreq_reader() as con:
        df = con.execute("SELECT ... FROM cached_order_book WHERE ...").df()

All timestamps are stored and queried in UTC.
"""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pyarrow as pa

from core.database import get_postgres

logger = logging.getLogger("pump_highfreq_cache")

# ── Paths ────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_DIR = _PROJECT_ROOT / "cache"
_CACHE_DIR.mkdir(exist_ok=True)
_HF_CACHE_FILE = _CACHE_DIR / "pump_highfreq.duckdb"

# ── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_LOOKBACK_MINUTES = 30       # rolling window kept in cache
_SYNC_LOCK_KEY = "pump_highfreq_sync"


# =============================================================================
# ADVISORY LOCK (single-writer discipline)
# =============================================================================

def _try_acquire_sync_lock():
    """Acquire a PostgreSQL advisory lock for the cache writer.

    Returns a dedicated psycopg2 connection holding the lock, or None.
    The caller MUST close this connection when done to release the lock.
    """
    try:
        from core.database import get_postgres_dedicated_connection

        digest = hashlib.blake2b(_SYNC_LOCK_KEY.encode("utf-8"), digest_size=8).digest()
        n = int.from_bytes(digest, byteorder="big", signed=False)
        if n >= 2**63:
            n -= 2**64

        conn = get_postgres_dedicated_connection(application_name="ftg_hf_cache_sync")
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS locked", [n])
            row = cur.fetchone()
            locked = bool(row["locked"]) if row else False
        if not locked:
            conn.close()
            return None
        return conn
    except Exception as e:
        logger.warning(f"Failed to acquire HF cache sync lock: {e}")
        return None


# =============================================================================
# SCHEMA INIT
# =============================================================================

def _init_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create DuckDB cache tables if they don't exist."""

    # -- order_book_features cache --
    con.execute("""
        CREATE TABLE IF NOT EXISTS cached_order_book (
            id             BIGINT PRIMARY KEY,
            ts             TIMESTAMP NOT NULL,
            mid_price      DOUBLE,
            spread_bps     DOUBLE,
            volume_imbalance DOUBLE,
            bid_liquidity  DOUBLE,
            ask_liquidity  DOUBLE,
            total_depth_10 DOUBLE,
            microprice     DOUBLE,
            microprice_dev_bps DOUBLE,
            bid_vwap_10    DOUBLE,
            ask_vwap_10    DOUBLE,
            bid_slope      DOUBLE,
            ask_slope      DOUBLE,
            bid_depth_bps_10 DOUBLE,
            ask_depth_bps_10 DOUBLE,
            net_liquidity_change_1s DOUBLE,
            depth_imbalance_ratio DOUBLE
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cob_ts ON cached_order_book(ts)")

    # -- sol_stablecoin_trades cache --
    con.execute("""
        CREATE TABLE IF NOT EXISTS cached_trades (
            id               BIGINT PRIMARY KEY,
            trade_timestamp  TIMESTAMP NOT NULL,
            direction        VARCHAR,
            price            DOUBLE,
            sol_amount       DOUBLE,
            stablecoin_amount DOUBLE
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_ct_ts ON cached_trades(trade_timestamp)")

    # -- whale_movements cache --
    con.execute("""
        CREATE TABLE IF NOT EXISTS cached_whales (
            id             BIGINT PRIMARY KEY,
            ts             TIMESTAMP NOT NULL,
            sol_change     DOUBLE,
            direction      VARCHAR,
            abs_change     DOUBLE,
            whale_type     VARCHAR,
            percentage_moved DOUBLE
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cw_ts ON cached_whales(ts)")

    # -- SOL prices cache --
    con.execute("""
        CREATE TABLE IF NOT EXISTS cached_prices (
            id        BIGINT PRIMARY KEY,
            ts        TIMESTAMP NOT NULL,
            price     DOUBLE NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cp_ts ON cached_prices(ts)")

    # -- watermark metadata --
    con.execute("""
        CREATE TABLE IF NOT EXISTS sync_watermarks (
            table_name VARCHAR PRIMARY KEY,
            max_id     BIGINT NOT NULL,
            synced_at  TIMESTAMP NOT NULL
        )
    """)


# =============================================================================
# READER (read-only connection — safe for fast-path polling)
# =============================================================================

@contextmanager
def get_highfreq_reader():
    """Context manager for a read-only DuckDB connection.

    Safe to call every 5-10s from the fast path without risking
    write-lock contention with the sync writer.
    """
    if not _HF_CACHE_FILE.exists():
        con = duckdb.connect(str(_HF_CACHE_FILE))
        _init_schema(con)
        con.close()
    con = duckdb.connect(str(_HF_CACHE_FILE), read_only=True)
    try:
        yield con
    finally:
        con.close()


# =============================================================================
# SYNC (write path — holds advisory lock)
# =============================================================================

def _get_watermark(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    """Get the max synced ID for a source table."""
    row = con.execute(
        "SELECT max_id FROM sync_watermarks WHERE table_name = ?",
        [table_name]
    ).fetchone()
    return row[0] if row else 0


def _set_watermark(con: duckdb.DuckDBPyConnection, table_name: str, max_id: int) -> None:
    """Update the watermark after successful sync."""
    con.execute("""
        INSERT OR REPLACE INTO sync_watermarks (table_name, max_id, synced_at)
        VALUES (?, ?, ?)
    """, [table_name, max_id, datetime.now(timezone.utc).replace(tzinfo=None)])


def _sync_order_book(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> int:
    """Sync order_book_features from PostgreSQL."""
    wm = _get_watermark(con, "order_book_features")

    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, mid_price, spread_bps, volume_imbalance,
                       bid_liquidity, ask_liquidity, total_depth_10,
                       microprice, microprice_dev_bps,
                       bid_vwap_10, ask_vwap_10, bid_slope, ask_slope,
                       bid_depth_bps_10, ask_depth_bps_10,
                       net_liquidity_change_1s, depth_imbalance_ratio
                FROM order_book_features
                WHERE id > %s AND timestamp >= %s
                ORDER BY id
                LIMIT 50000
            """, [wm, cutoff])
            rows = cur.fetchall()

    if not rows:
        return 0

    ids = pa.array([r["id"] for r in rows], type=pa.int64())
    tbl = pa.table({
        "id": ids,
        "ts":                      pa.array([r["timestamp"] for r in rows], type=pa.timestamp("us")),
        "mid_price":               pa.array([float(r["mid_price"]) if r["mid_price"] is not None else None for r in rows], type=pa.float64()),
        "spread_bps":              pa.array([float(r["spread_bps"]) if r["spread_bps"] is not None else None for r in rows], type=pa.float64()),
        "volume_imbalance":        pa.array([float(r["volume_imbalance"]) if r["volume_imbalance"] is not None else None for r in rows], type=pa.float64()),
        "bid_liquidity":           pa.array([float(r["bid_liquidity"]) if r.get("bid_liquidity") is not None else None for r in rows], type=pa.float64()),
        "ask_liquidity":           pa.array([float(r["ask_liquidity"]) if r.get("ask_liquidity") is not None else None for r in rows], type=pa.float64()),
        "total_depth_10":          pa.array([float(r["total_depth_10"]) if r.get("total_depth_10") is not None else None for r in rows], type=pa.float64()),
        "microprice":              pa.array([float(r["microprice"]) if r.get("microprice") is not None else None for r in rows], type=pa.float64()),
        "microprice_dev_bps":      pa.array([float(r["microprice_dev_bps"]) if r.get("microprice_dev_bps") is not None else None for r in rows], type=pa.float64()),
        "bid_vwap_10":             pa.array([float(r["bid_vwap_10"]) if r.get("bid_vwap_10") is not None else None for r in rows], type=pa.float64()),
        "ask_vwap_10":             pa.array([float(r["ask_vwap_10"]) if r.get("ask_vwap_10") is not None else None for r in rows], type=pa.float64()),
        "bid_slope":               pa.array([float(r["bid_slope"]) if r.get("bid_slope") is not None else None for r in rows], type=pa.float64()),
        "ask_slope":               pa.array([float(r["ask_slope"]) if r.get("ask_slope") is not None else None for r in rows], type=pa.float64()),
        "bid_depth_bps_10":        pa.array([float(r["bid_depth_bps_10"]) if r.get("bid_depth_bps_10") is not None else None for r in rows], type=pa.float64()),
        "ask_depth_bps_10":        pa.array([float(r["ask_depth_bps_10"]) if r.get("ask_depth_bps_10") is not None else None for r in rows], type=pa.float64()),
        "net_liquidity_change_1s": pa.array([float(r["net_liquidity_change_1s"]) if r.get("net_liquidity_change_1s") is not None else None for r in rows], type=pa.float64()),
        "depth_imbalance_ratio":   pa.array([float(r.get("depth_imbalance_ratio") or r.get("depth_imbalance", 0)) if r.get("depth_imbalance_ratio") is not None or r.get("depth_imbalance") is not None else None for r in rows], type=pa.float64()),
    })

    con.register("_ob_arrow", tbl)
    con.execute("INSERT OR REPLACE INTO cached_order_book SELECT * FROM _ob_arrow")
    con.unregister("_ob_arrow")

    max_id = int(rows[-1]["id"])
    _set_watermark(con, "order_book_features", max_id)
    return len(rows)


def _sync_trades(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> int:
    """Sync sol_stablecoin_trades from PostgreSQL."""
    wm = _get_watermark(con, "sol_stablecoin_trades")

    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT id, trade_timestamp, direction, price, sol_amount, stablecoin_amount
                FROM sol_stablecoin_trades
                WHERE id > %s AND trade_timestamp >= %s
                ORDER BY id
                LIMIT 100000
            """, [wm, cutoff])
            rows = cur.fetchall()

    if not rows:
        return 0

    tbl = pa.table({
        "id":                pa.array([r["id"] for r in rows], type=pa.int64()),
        "trade_timestamp":   pa.array([r["trade_timestamp"] for r in rows], type=pa.timestamp("us")),
        "direction":         pa.array([r["direction"] for r in rows], type=pa.string()),
        "price":             pa.array([float(r["price"]) if r["price"] is not None else None for r in rows], type=pa.float64()),
        "sol_amount":        pa.array([float(r["sol_amount"]) if r["sol_amount"] is not None else None for r in rows], type=pa.float64()),
        "stablecoin_amount": pa.array([float(r["stablecoin_amount"]) if r["stablecoin_amount"] is not None else None for r in rows], type=pa.float64()),
    })

    con.register("_tr_arrow", tbl)
    con.execute("INSERT OR REPLACE INTO cached_trades SELECT * FROM _tr_arrow")
    con.unregister("_tr_arrow")

    max_id = int(rows[-1]["id"])
    _set_watermark(con, "sol_stablecoin_trades", max_id)
    return len(rows)


def _sync_whales(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> int:
    """Sync whale_movements from PostgreSQL."""
    wm = _get_watermark(con, "whale_movements")

    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, sol_change, direction, abs_change,
                       whale_type, percentage_moved
                FROM whale_movements
                WHERE id > %s AND timestamp >= %s
                ORDER BY id
                LIMIT 50000
            """, [wm, cutoff])
            rows = cur.fetchall()

    if not rows:
        return 0

    tbl = pa.table({
        "id":              pa.array([r["id"] for r in rows], type=pa.int64()),
        "ts":              pa.array([r["timestamp"] for r in rows], type=pa.timestamp("us")),
        "sol_change":      pa.array([float(r["sol_change"]) if r["sol_change"] is not None else None for r in rows], type=pa.float64()),
        "direction":       pa.array([r["direction"] for r in rows], type=pa.string()),
        "abs_change":      pa.array([float(r["abs_change"]) if r["abs_change"] is not None else None for r in rows], type=pa.float64()),
        "whale_type":      pa.array([r["whale_type"] for r in rows], type=pa.string()),
        "percentage_moved": pa.array([float(r["percentage_moved"]) if r["percentage_moved"] is not None else None for r in rows], type=pa.float64()),
    })

    con.register("_wh_arrow", tbl)
    con.execute("INSERT OR REPLACE INTO cached_whales SELECT * FROM _wh_arrow")
    con.unregister("_wh_arrow")

    max_id = int(rows[-1]["id"])
    _set_watermark(con, "whale_movements", max_id)
    return len(rows)


def _sync_prices(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> int:
    """Sync SOL prices from PostgreSQL."""
    wm = _get_watermark(con, "prices_sol")

    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT id, timestamp, price
                FROM prices
                WHERE id > %s AND timestamp >= %s AND token = 'SOL'
                ORDER BY id
                LIMIT 100000
            """, [wm, cutoff])
            rows = cur.fetchall()

    if not rows:
        return 0

    tbl = pa.table({
        "id":    pa.array([r["id"] for r in rows], type=pa.int64()),
        "ts":    pa.array([r["timestamp"] for r in rows], type=pa.timestamp("us")),
        "price": pa.array([float(r["price"]) for r in rows], type=pa.float64()),
    })

    con.register("_pr_arrow", tbl)
    con.execute("INSERT OR REPLACE INTO cached_prices SELECT * FROM _pr_arrow")
    con.unregister("_pr_arrow")

    max_id = int(rows[-1]["id"])
    _set_watermark(con, "prices_sol", max_id)
    return len(rows)


def _cleanup_old_data(con: duckdb.DuckDBPyConnection, cutoff: datetime) -> None:
    """Remove data older than the rolling window."""
    con.execute("DELETE FROM cached_order_book WHERE ts < ?", [cutoff])
    con.execute("DELETE FROM cached_trades WHERE trade_timestamp < ?", [cutoff])
    con.execute("DELETE FROM cached_whales WHERE ts < ?", [cutoff])
    con.execute("DELETE FROM cached_prices WHERE ts < ?", [cutoff])


def sync_highfreq_cache(lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES) -> Dict[str, int]:
    """Perform an incremental sync of high-frequency data into DuckDB.

    Acquires a PostgreSQL advisory lock to ensure single-writer discipline.
    Returns a dict with row counts synced per table, or empty dict if lock not acquired.
    """
    lock_conn = _try_acquire_sync_lock()
    if lock_conn is None:
        logger.debug("HF cache sync skipped (another instance holds lock)")
        return {}

    t0 = time.time()
    con = None
    try:
        # Use naive UTC for DuckDB comparisons (DuckDB TIMESTAMP is tz-naive;
        # PG stores naive-UTC timestamps; using tz-aware datetimes here would
        # cause DuckDB to convert to local time, deleting valid data)
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=lookback_minutes)

        con = duckdb.connect(str(_HF_CACHE_FILE))
        _init_schema(con)

        n_ob = _sync_order_book(con, cutoff)
        n_tr = _sync_trades(con, cutoff)
        n_wh = _sync_whales(con, cutoff)
        n_pr = _sync_prices(con, cutoff)

        _cleanup_old_data(con, cutoff)

        # Force WAL checkpoint so read_only connections see the new data
        con.execute("CHECKPOINT")

        elapsed = time.time() - t0
        if n_ob + n_tr + n_wh + n_pr > 0:
            logger.info(
                f"HF cache sync: OB={n_ob}, trades={n_tr}, whales={n_wh}, "
                f"prices={n_pr} in {elapsed:.2f}s"
            )
        else:
            logger.debug(f"HF cache sync: no new data ({elapsed:.2f}s)")

        return {"order_book": n_ob, "trades": n_tr, "whales": n_wh, "prices": n_pr}

    except Exception as e:
        logger.error(f"HF cache sync error: {e}", exc_info=True)
        if _HF_CACHE_FILE.exists():
            try:
                _HF_CACHE_FILE.unlink()
                logger.info("Deleted corrupted HF cache — will rebuild next sync")
            except OSError:
                pass
        return {}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        try:
            lock_conn.close()
        except Exception:
            pass


# =============================================================================
# CONVENIENCE QUERY HELPERS (all use read-only connections)
# =============================================================================

def query_recent_trades_arrow(seconds: int = 30) -> Optional[pa.Table]:
    """Return raw trades from the last N seconds as a PyArrow table."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds)
    try:
        with get_highfreq_reader() as con:
            result = con.execute(
                "SELECT * FROM cached_trades WHERE trade_timestamp >= ? ORDER BY trade_timestamp",
                [cutoff]
            ).arrow()
            return result.read_all() if hasattr(result, 'read_all') else result
    except Exception as e:
        logger.debug(f"query_recent_trades_arrow error: {e}")
        return None


def query_recent_order_book(seconds: int = 60) -> Optional[pa.Table]:
    """Return order book snapshots from the last N seconds as PyArrow."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds)
    try:
        with get_highfreq_reader() as con:
            result = con.execute(
                "SELECT * FROM cached_order_book WHERE ts >= ? ORDER BY ts",
                [cutoff]
            ).arrow()
            return result.read_all() if hasattr(result, 'read_all') else result
    except Exception as e:
        logger.debug(f"query_recent_order_book error: {e}")
        return None


def query_recent_whales(seconds: int = 300) -> Optional[pa.Table]:
    """Return whale movements from the last N seconds as PyArrow."""
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds)
    try:
        with get_highfreq_reader() as con:
            result = con.execute(
                "SELECT * FROM cached_whales WHERE ts >= ? ORDER BY ts",
                [cutoff]
            ).arrow()
            return result.read_all() if hasattr(result, 'read_all') else result
    except Exception as e:
        logger.debug(f"query_recent_whales error: {e}")
        return None


def get_cache_stats() -> Dict[str, Any]:
    """Return row counts and date ranges for each cached table."""
    try:
        with get_highfreq_reader() as con:
            stats = {}
            for tbl, ts_col in [
                ("cached_order_book", "ts"),
                ("cached_trades", "trade_timestamp"),
                ("cached_whales", "ts"),
                ("cached_prices", "ts"),
            ]:
                row = con.execute(
                    f"SELECT COUNT(*) AS n, MIN({ts_col}) AS mn, MAX({ts_col}) AS mx FROM {tbl}"
                ).fetchone()
                stats[tbl] = {"count": row[0], "min_ts": row[1], "max_ts": row[2]}
            return stats
    except Exception as e:
        logger.debug(f"get_cache_stats error: {e}")
        return {}

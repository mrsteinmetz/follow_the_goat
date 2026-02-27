"""
core/raw_data_cache.py
======================
Real-time DuckDB cache for raw market data (order book, trades, whales).

Architecture
------------
Two separate DuckDB files, each owned by exactly one writer process:

  cache/ob_data.duckdb    → written by  binance_stream  (OB snapshots ~1/s)
  cache/trade_data.duckdb → written by  webhook_server  (trades + whale events)

Any other process (pump_fingerprint, train_validator) opens both files
READ-ONLY and queries them via ATTACH — no write-lock contention.

PyArrow is used for all writes: rows are buffered in memory and flushed
to DuckDB in small batches (default: every 10 rows or 10 seconds).
Single-row flushes are also supported for latency-sensitive paths.

Public API
----------
Writer side (call from the process that owns the file):

    from core.raw_data_cache import OBCache, TradeCache

    ob    = OBCache()            # opens cache/ob_data.duckdb for writing
    trade = TradeCache()         # opens cache/trade_data.duckdb for writing

    ob.append(ts, mid_price, spread_bps, ...)   # non-blocking, auto-flushes
    trade.append_trade(ts, sol_amount, ...)
    trade.append_whale(ts, sol_moved, ...)

Reader side (fingerprint, train_validator):

    from core.raw_data_cache import open_reader

    con = open_reader()          # in-memory DuckDB with both files ATTACHed
    df  = con.execute("SELECT ... FROM ob.ob_snapshots WHERE ...").df()
    con.close()

Feature helper (used in check_pump_signal every 5 s):

    from core.raw_data_cache import get_live_features
    feats = get_live_features(window_min=5)   # returns dict or None
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)

# ── file paths ────────────────────────────────────────────────────────────────
_CACHE_DIR  = Path(__file__).parent.parent / "cache"
OB_FILE     = _CACHE_DIR / "ob_data.duckdb"
TRADE_FILE  = _CACHE_DIR / "trade_data.duckdb"

# Parquet snapshot paths (cross-process readable, no locking)
OB_PARQUET     = _CACHE_DIR / "ob_latest.parquet"
TRADE_PARQUET  = _CACHE_DIR / "trade_latest.parquet"
WHALE_PARQUET  = _CACHE_DIR / "whale_latest.parquet"

# Rolling retention (delete rows older than this)
# 96h gives the mega_simulator 4 days of pattern history to learn from,
# which drastically reduces overfitting vs the previous 25h window.
RETENTION_HOURS = 96

# Flush settings
_FLUSH_ROWS    = 10     # flush after this many buffered rows
_FLUSH_SECS    = 10.0   # flush after this many seconds even if buffer not full
_PARQUET_SECS  = 15.0   # re-export Parquet snapshot every N seconds


# =============================================================================
# SCHEMAS
# =============================================================================

_OB_SCHEMA = pa.schema([
    ('ts',             pa.timestamp('us', tz='UTC')),
    ('mid_price',      pa.float64()),
    ('spread_bps',     pa.float64()),
    ('bid_liq',        pa.float64()),
    ('ask_liq',        pa.float64()),
    ('vol_imb',        pa.float64()),
    ('depth_ratio',    pa.float64()),
    ('microprice',     pa.float64()),
    ('microprice_dev', pa.float64()),
    ('net_liq_1s',     pa.float64()),
    ('bid_slope',      pa.float64()),
    ('ask_slope',      pa.float64()),
    ('bid_dep_5bps',   pa.float64()),
    ('ask_dep_5bps',   pa.float64()),
])

_TRADE_SCHEMA = pa.schema([
    ('ts',         pa.timestamp('us', tz='UTC')),
    ('sol_amount', pa.float64()),
    ('stable_amt', pa.float64()),
    ('price',      pa.float64()),
    ('direction',  pa.string()),   # 'buy' | 'sell'
    ('is_perp',    pa.bool_()),
])

_WHALE_SCHEMA = pa.schema([
    ('ts',           pa.timestamp('us', tz='UTC')),
    ('sol_moved',    pa.float64()),
    ('direction',    pa.string()),  # 'in' | 'out'
    ('significance', pa.float64()),
    ('pct_moved',    pa.float64()),
])


# =============================================================================
# BASE CACHE CLASS
# =============================================================================

class _BaseCache:
    """Thread-safe DuckDB cache with PyArrow batch writes."""

    def __init__(self, path: Path, init_sql: str) -> None:
        _CACHE_DIR.mkdir(exist_ok=True)
        self._path = path
        self._con  = duckdb.connect(str(path))
        self._con.execute(init_sql)
        self._lock          = threading.Lock()
        self._buffer: List[dict] = []
        self._last_flush    = time.monotonic()
        self._cleanup_after = time.monotonic() + 3600  # cleanup once per hour
        logger.info(f"[raw_cache] Opened {path.name} for writing")

    # ── internal flush ────────────────────────────────────────────────────────

    def _flush(self, rows: List[dict], schema: pa.Schema, table: str) -> None:
        if not rows:
            return
        cols: Dict[str, list] = {f.name: [] for f in schema}
        for row in rows:
            for f in schema:
                cols[f.name].append(row.get(f.name))
        arrays = [pa.array(cols[f.name], type=f.type) for f in schema]
        tbl = pa.table(arrays, schema=schema)
        self._con.register('_batch', tbl)
        self._con.execute(f"INSERT INTO {table} SELECT * FROM _batch")
        self._con.unregister('_batch')

    def _export_parquet(self, table: str, parquet_path: Path) -> None:
        """Export DuckDB table to Parquet, preserving older historical rows that
        pre-date what this DuckDB instance currently holds (e.g. written by backfill).

        Strategy:
          1. Export current DuckDB rows (since cutoff) to a temp Parquet.
          2. If the existing Parquet has rows OLDER than the DuckDB's earliest ts,
             merge them in using PyArrow (no extra DuckDB connection needed).
          3. Atomically replace the Parquet file.
        """
        import pyarrow.parquet as pq
        import pyarrow as pa
        tmp = parquet_path.with_suffix('.tmp.parquet')
        new_tmp = parquet_path.with_suffix('.new.parquet')
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)

            # Step 1: export fresh DuckDB rows
            self._con.execute(
                f"COPY (SELECT * FROM {table} WHERE ts >= ?) TO '{new_tmp}' (FORMAT PARQUET)",
                [cutoff]
            )

            # Step 2: check if existing Parquet has older historical rows to keep
            if parquet_path.exists():
                row = self._con.execute(f"SELECT MIN(ts), COUNT(*) FROM {table}").fetchone()
                db_min_ts = row[0] if row and row[0] else None
                db_count  = row[1] if row else 0

                # If our DuckDB table is empty but the parquet has data, preserve it —
                # this keeps backfilled history until live writes start flowing in.
                if db_count == 0:
                    if new_tmp.exists():
                        new_tmp.unlink(missing_ok=True)
                    return

                if db_min_ts is not None:
                    try:
                        old_tbl = pq.read_table(str(parquet_path), filters=[
                            ('ts', '>=', cutoff),
                            ('ts', '<', db_min_ts),
                        ])
                        if old_tbl.num_rows > 0:
                            new_tbl = pq.read_table(str(new_tmp))
                            merged = pa.concat_tables([old_tbl, new_tbl])
                            # sort by ts to keep chronological order
                            import pyarrow.compute as pc
                            order = pc.sort_indices(merged, sort_keys=[('ts', 'ascending')])
                            merged = merged.take(order)
                            pq.write_table(merged, str(tmp), compression='snappy')
                            if new_tmp.exists():
                                new_tmp.unlink(missing_ok=True)
                            tmp.replace(parquet_path)
                            return
                    except Exception as merge_err:
                        logger.debug(f"[raw_cache] parquet merge skipped ({table}): {merge_err}")

            # No merge needed — just use the fresh export
            if new_tmp.exists():
                new_tmp.replace(parquet_path)
        except Exception as e:
            logger.debug(f"[raw_cache] parquet export {table}: {e}")
        finally:
            for f in (tmp, new_tmp):
                try:
                    if f.exists():
                        f.unlink(missing_ok=True)
                except Exception:
                    pass

    def _maybe_cleanup(self) -> None:
        now = time.monotonic()
        if now < self._cleanup_after:
            return
        self._cleanup_after = now + 3600
        cutoff = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)
        for tbl in self._tables():
            try:
                self._con.execute(f"DELETE FROM {tbl} WHERE ts < ?", [cutoff])
            except Exception as e:
                logger.warning(f"[raw_cache] cleanup {tbl}: {e}")

    def _tables(self) -> List[str]:
        raise NotImplementedError

    def close(self) -> None:
        with self._lock:
            self._con.close()


# =============================================================================
# ORDER BOOK CACHE  (ob_data.duckdb)
# =============================================================================

_OB_INIT = """
CREATE TABLE IF NOT EXISTS ob_snapshots (
    ts             TIMESTAMPTZ NOT NULL,
    mid_price      DOUBLE,
    spread_bps     DOUBLE,
    bid_liq        DOUBLE,
    ask_liq        DOUBLE,
    vol_imb        DOUBLE,
    depth_ratio    DOUBLE,
    microprice     DOUBLE,
    microprice_dev DOUBLE,
    net_liq_1s     DOUBLE,
    bid_slope      DOUBLE,
    ask_slope      DOUBLE,
    bid_dep_5bps   DOUBLE,
    ask_dep_5bps   DOUBLE
);
CREATE INDEX IF NOT EXISTS ob_ts_idx ON ob_snapshots (ts);
"""


class OBCache(_BaseCache):
    """Writes order book snapshots to ob_data.duckdb."""

    def __init__(self) -> None:
        super().__init__(OB_FILE, _OB_INIT)
        self._last_parquet = 0.0

    def _tables(self) -> List[str]:
        return ['ob_snapshots']

    def append(
        self,
        ts:             datetime,
        mid_price:      Optional[float],
        spread_bps:     Optional[float],
        bid_liq:        Optional[float],
        ask_liq:        Optional[float],
        vol_imb:        Optional[float],
        depth_ratio:    Optional[float],
        microprice:     Optional[float] = None,
        microprice_dev: Optional[float] = None,
        net_liq_1s:     Optional[float] = None,
        bid_slope:      Optional[float] = None,
        ask_slope:      Optional[float] = None,
        bid_dep_5bps:   Optional[float] = None,
        ask_dep_5bps:   Optional[float] = None,
    ) -> None:
        row = dict(
            ts=ts, mid_price=mid_price, spread_bps=spread_bps,
            bid_liq=bid_liq, ask_liq=ask_liq, vol_imb=vol_imb,
            depth_ratio=depth_ratio, microprice=microprice,
            microprice_dev=microprice_dev, net_liq_1s=net_liq_1s,
            bid_slope=bid_slope, ask_slope=ask_slope,
            bid_dep_5bps=bid_dep_5bps, ask_dep_5bps=ask_dep_5bps,
        )
        now = time.monotonic()
        with self._lock:
            self._buffer.append(row)
            should_flush = (
                len(self._buffer) >= _FLUSH_ROWS
                or (now - self._last_flush) >= _FLUSH_SECS
            )
            if should_flush:
                buf, self._buffer = self._buffer, []
                self._last_flush = now
                self._flush(buf, _OB_SCHEMA, 'ob_snapshots')
                self._maybe_cleanup()
                # Export Parquet snapshot for cross-process readers
                if now - self._last_parquet >= _PARQUET_SECS:
                    self._last_parquet = now
                    self._export_parquet('ob_snapshots', OB_PARQUET)


# =============================================================================
# TRADE + WHALE CACHE  (trade_data.duckdb)
# =============================================================================

_TRADE_INIT = """
CREATE TABLE IF NOT EXISTS raw_trades (
    ts         TIMESTAMPTZ NOT NULL,
    sol_amount DOUBLE,
    stable_amt DOUBLE,
    price      DOUBLE,
    direction  VARCHAR,
    is_perp    BOOLEAN
);
CREATE INDEX IF NOT EXISTS trade_ts_idx ON raw_trades (ts);

CREATE TABLE IF NOT EXISTS whale_events (
    ts           TIMESTAMPTZ NOT NULL,
    sol_moved    DOUBLE,
    direction    VARCHAR,
    significance DOUBLE,
    pct_moved    DOUBLE
);
CREATE INDEX IF NOT EXISTS whale_ts_idx ON whale_events (ts);
"""


class TradeCache(_BaseCache):
    """Writes stablecoin trades + whale events to trade_data.duckdb."""

    def __init__(self) -> None:
        super().__init__(TRADE_FILE, _TRADE_INIT)
        self._whale_buffer: List[dict] = []
        self._whale_last_flush = time.monotonic()
        self._last_parquet = 0.0

    def _tables(self) -> List[str]:
        return ['raw_trades', 'whale_events']

    def append_trade(
        self,
        ts:         datetime,
        sol_amount: float,
        stable_amt: float,
        price:      float,
        direction:  str,
        is_perp:    bool = False,
    ) -> None:
        row = dict(ts=ts, sol_amount=float(sol_amount), stable_amt=float(stable_amt),
                   price=float(price), direction=direction, is_perp=is_perp)
        now = time.monotonic()
        with self._lock:
            self._buffer.append(row)
            should_flush = (
                len(self._buffer) >= _FLUSH_ROWS
                or (now - self._last_flush) >= _FLUSH_SECS
            )
            if should_flush:
                buf, self._buffer = self._buffer, []
                self._last_flush = now
                self._flush(buf, _TRADE_SCHEMA, 'raw_trades')
                self._maybe_cleanup()
                if now - self._last_parquet >= _PARQUET_SECS:
                    self._last_parquet = now
                    self._export_parquet('raw_trades',   TRADE_PARQUET)
                    self._export_parquet('whale_events', WHALE_PARQUET)

    def append_whale(
        self,
        ts:           datetime,
        sol_moved:    float,
        direction:    str,
        significance: Optional[float] = None,
        pct_moved:    Optional[float] = None,
    ) -> None:
        row = dict(ts=ts, sol_moved=float(sol_moved), direction=direction,
                   significance=significance, pct_moved=pct_moved)
        now = time.monotonic()
        with self._lock:
            self._whale_buffer.append(row)
            should_flush = (
                len(self._whale_buffer) >= _FLUSH_ROWS
                or (now - self._whale_last_flush) >= _FLUSH_SECS
            )
            if should_flush:
                buf, self._whale_buffer = self._whale_buffer, []
                self._whale_last_flush = now
                self._flush(buf, _WHALE_SCHEMA, 'whale_events')


# =============================================================================
# READER  (fingerprint + train_validator)
# =============================================================================

def open_reader() -> duckdb.DuckDBPyConnection:
    """
    Return an in-memory DuckDB connection backed by Parquet snapshots.

    Reads from OB_PARQUET / TRADE_PARQUET / WHALE_PARQUET — no file locks,
    safe to call from any process while writers are active.

    Tables created:
      ob_snapshots, raw_trades, whale_events

    Caller must call .close() when done.
    """
    con = duckdb.connect(":memory:")

    # OB snapshots
    if OB_PARQUET.exists():
        con.execute(f"CREATE TABLE ob_snapshots AS SELECT * FROM '{OB_PARQUET}'")
    else:
        logger.warning("[raw_cache] ob_latest.parquet not yet created — using empty stub")
        con.execute("""
            CREATE TABLE ob_snapshots (
                ts TIMESTAMPTZ, mid_price DOUBLE, spread_bps DOUBLE,
                bid_liq DOUBLE, ask_liq DOUBLE, vol_imb DOUBLE,
                depth_ratio DOUBLE, microprice DOUBLE, microprice_dev DOUBLE,
                net_liq_1s DOUBLE, bid_slope DOUBLE, ask_slope DOUBLE,
                bid_dep_5bps DOUBLE, ask_dep_5bps DOUBLE
            )
        """)

    # Trades
    if TRADE_PARQUET.exists():
        con.execute(f"CREATE TABLE raw_trades AS SELECT * FROM '{TRADE_PARQUET}'")
    else:
        con.execute("""
            CREATE TABLE raw_trades (
                ts TIMESTAMPTZ, sol_amount DOUBLE, stable_amt DOUBLE,
                price DOUBLE, direction VARCHAR, is_perp BOOLEAN
            )
        """)

    # Whale events
    if WHALE_PARQUET.exists():
        con.execute(f"CREATE TABLE whale_events AS SELECT * FROM '{WHALE_PARQUET}'")
    else:
        con.execute("""
            CREATE TABLE whale_events (
                ts TIMESTAMPTZ, sol_moved DOUBLE, direction VARCHAR,
                significance DOUBLE, pct_moved DOUBLE
            )
        """)

    return con


# =============================================================================
# LIVE FEATURE COMPUTATION  (used by check_pump_signal every 5 s)
# =============================================================================

# Lock for get_live_features (called every 5 s from train_validator)
_reader_lock = threading.Lock()


def _get_fresh_reader() -> duckdb.DuckDBPyConnection:
    """Open a fresh in-memory connection from Parquet snapshots.

    Parquet files are updated every ~15 s by the writer processes.
    Opening a fresh in-memory DuckDB each call takes ~2 ms for 24h of data
    — well within the 5-second budget for train_validator.
    """
    return open_reader()


def get_live_features(window_min: int = 5) -> Optional[Dict[str, Any]]:
    """
    Compute current market features from the last `window_min` minutes
    of raw OB, trade, and whale data.

    Returns a dict of float features, or None if insufficient data.
    Called every 5 seconds by train_validator / check_pump_signal.
    """
    try:
        with _reader_lock:
            con = _get_fresh_reader()
            win_sec = window_min * 60
            row = con.execute(f"""
                WITH now AS (SELECT NOW() AS t),

                ob_feats AS (
                    SELECT
                        COUNT(*)                                               AS ob_n,
                        AVG(vol_imb)                                           AS ob_avg_vol_imb,
                        AVG(depth_ratio)                                       AS ob_avg_depth_ratio,
                        AVG(spread_bps)                                        AS ob_avg_spread_bps,
                        AVG(net_liq_1s)                                        AS ob_net_liq_change,
                        AVG(bid_liq / NULLIF(ask_liq, 0))                      AS ob_bid_ask_ratio,
                        -- 1-min rolling vs 5-min window
                        AVG(CASE WHEN EPOCH(n.t) - EPOCH(o.ts) < 60
                                 THEN vol_imb END)                             AS ob_imb_1m,
                        AVG(CASE WHEN EPOCH(n.t) - EPOCH(o.ts) < 60
                                 THEN depth_ratio END)                         AS ob_depth_1m,
                        AVG(CASE WHEN EPOCH(n.t) - EPOCH(o.ts) < 60
                                 THEN bid_liq / NULLIF(ask_liq, 0) END)        AS ob_bid_ask_1m,
                        -- microstructure: slope asymmetry (bid builds steeper = bullish)
                        AVG(bid_slope / NULLIF(ABS(ask_slope), 0))             AS ob_slope_ratio,
                        -- depth within 5 bps of mid (tight book pressure)
                        AVG(bid_dep_5bps / NULLIF(ask_dep_5bps, 0))            AS ob_depth_5bps_ratio,
                        -- microprice deviation from mid (positive = buy pressure)
                        AVG(microprice_dev)                                    AS ob_microprice_dev
                    FROM ob_snapshots o, now n
                    WHERE EPOCH(n.t) - EPOCH(o.ts) BETWEEN 0 AND {win_sec}
                ),

                trade_feats AS (
                    SELECT
                        COUNT(*)                                               AS tr_n,
                        SUM(sol_amount)                                        AS tr_total_sol,
                        SUM(CASE WHEN direction='buy' THEN sol_amount ELSE 0 END)
                          / NULLIF(SUM(sol_amount), 0)                        AS tr_buy_ratio,
                        SUM(CASE WHEN sol_amount > 50 THEN sol_amount ELSE 0 END)
                          / NULLIF(SUM(sol_amount), 0)                        AS tr_large_ratio,
                        AVG(sol_amount)                                        AS tr_avg_size,
                        -- buy acceleration: last 60s vs window avg
                        AVG(CASE WHEN EPOCH(n.t) - EPOCH(t.ts) < 60
                                 THEN CASE WHEN direction='buy' THEN sol_amount ELSE 0 END END)
                          / NULLIF(AVG(CASE WHEN direction='buy' THEN sol_amount ELSE 0 END), 0)
                                                                               AS tr_buy_accel
                    FROM raw_trades t, now n
                    WHERE EPOCH(n.t) - EPOCH(t.ts) BETWEEN 0 AND {win_sec}
                ),

                whale_feats AS (
                    SELECT
                        COUNT(*)                                               AS wh_n,
                        -- net flow: positive = more SOL flowing IN (accumulation)
                        -- ABS() handles legacy data where sol_moved may be signed
                        SUM(CASE WHEN direction IN ('in','receiving')  THEN ABS(sol_moved) ELSE 0 END)
                          - SUM(CASE WHEN direction IN ('out','sending') THEN ABS(sol_moved) ELSE 0 END)
                                                                               AS wh_net_flow,
                        SUM(CASE WHEN direction IN ('in','receiving') THEN ABS(sol_moved) ELSE 0 END)
                          / NULLIF(SUM(ABS(sol_moved)), 0)                    AS wh_inflow_ratio,
                        COUNT(CASE WHEN significance > 0.5 THEN 1 END)        AS wh_large_count,
                        -- pct_moved: how much of each whale's wallet was moved (conviction)
                        AVG(pct_moved)                                         AS wh_avg_pct_moved,
                        -- urgency: fraction of events moving >50% of wallet
                        COUNT(CASE WHEN pct_moved > 50 THEN 1 END) * 1.0
                          / NULLIF(COUNT(*), 0)                                AS wh_urgency_ratio
                    FROM whale_events w, now n
                    WHERE EPOCH(n.t) - EPOCH(w.ts) BETWEEN 0 AND {win_sec}
                ),

                -- Price momentum from OB mid_price snapshots
                -- Matches exactly how mega_simulator computes pm_* features
                price_now AS (
                    SELECT AVG(mid_price) AS p
                    FROM ob_snapshots o, now n
                    WHERE EPOCH(n.t) - EPOCH(o.ts) BETWEEN 0 AND 10
                ),
                price_30s AS (
                    SELECT AVG(mid_price) AS p
                    FROM ob_snapshots o, now n
                    WHERE EPOCH(n.t) - EPOCH(o.ts) BETWEEN 25 AND 35
                ),
                price_1m AS (
                    SELECT AVG(mid_price) AS p
                    FROM ob_snapshots o, now n
                    WHERE EPOCH(n.t) - EPOCH(o.ts) BETWEEN 55 AND 65
                ),
                price_5m AS (
                    SELECT AVG(mid_price) AS p
                    FROM ob_snapshots o, now n
                    WHERE EPOCH(n.t) - EPOCH(o.ts) BETWEEN 295 AND 305
                )

                SELECT
                    o.ob_n,
                    o.ob_avg_vol_imb, o.ob_avg_depth_ratio,
                    o.ob_avg_spread_bps, o.ob_net_liq_change, o.ob_bid_ask_ratio,
                    o.ob_imb_1m, o.ob_depth_1m, o.ob_bid_ask_1m,
                    o.ob_imb_1m      - o.ob_avg_vol_imb      AS ob_imb_trend,
                    o.ob_depth_1m    - o.ob_avg_depth_ratio   AS ob_depth_trend,
                    o.ob_bid_ask_1m  - o.ob_bid_ask_ratio     AS ob_liq_accel,
                    o.ob_slope_ratio, o.ob_depth_5bps_ratio, o.ob_microprice_dev,
                    t.tr_n, t.tr_total_sol, t.tr_buy_ratio,
                    t.tr_large_ratio, t.tr_avg_size, t.tr_buy_accel,
                    w.wh_n, w.wh_net_flow, w.wh_inflow_ratio, w.wh_large_count,
                    w.wh_avg_pct_moved, w.wh_urgency_ratio,
                    -- Price momentum (% change, matching mega_simulator feature names)
                    CASE WHEN p30s.p > 0
                         THEN (pnow.p - p30s.p) / p30s.p * 100 ELSE 0 END    AS pm_price_change_30s,
                    CASE WHEN p1m.p  > 0
                         THEN (pnow.p - p1m.p)  / p1m.p  * 100 ELSE 0 END    AS pm_price_change_1m,
                    CASE WHEN p5m.p  > 0
                         THEN (pnow.p - p5m.p)  / p5m.p  * 100 ELSE 0 END    AS pm_price_change_5m,
                    CASE WHEN p1m.p  > 0 AND p5m.p > 0
                         THEN (pnow.p - p1m.p) / p1m.p * 100
                            - (pnow.p - p5m.p) / p5m.p * 100 ELSE 0 END      AS pm_velocity_30s
                FROM ob_feats o, trade_feats t, whale_feats w,
                     price_now pnow, price_30s p30s, price_1m p1m, price_5m p5m
            """).fetchone()

        con.close()
        if row is None or row[0] < 3:  # need at least 3 OB snapshots
            return None

        cols = [
            'ob_n',
            'ob_avg_vol_imb', 'ob_avg_depth_ratio',
            'ob_avg_spread_bps', 'ob_net_liq_change', 'ob_bid_ask_ratio',
            'ob_imb_1m', 'ob_depth_1m', 'ob_bid_ask_1m',
            'ob_imb_trend', 'ob_depth_trend', 'ob_liq_accel',
            'ob_slope_ratio', 'ob_depth_5bps_ratio', 'ob_microprice_dev',
            'tr_n', 'tr_total_sol', 'tr_buy_ratio',
            'tr_large_ratio', 'tr_avg_size', 'tr_buy_accel',
            'wh_n', 'wh_net_flow', 'wh_inflow_ratio', 'wh_large_count',
            'wh_avg_pct_moved', 'wh_urgency_ratio',
            # Price momentum — must match mega_simulator FEATURES list exactly
            'pm_price_change_30s', 'pm_price_change_1m',
            'pm_price_change_5m', 'pm_velocity_30s',
        ]
        return dict(zip(cols, row))

    except Exception as e:
        logger.error(f"[raw_cache] get_live_features error: {e}")
        try:
            con.close()
        except Exception:
            pass
        return None


# =============================================================================
# TRAINING DATA LOADER  (used by pump_fingerprint.py)
# =============================================================================

def load_training_data(
    lookback_hours: int   = 24,
    pump_threshold: float = 0.3,
    window_min:     int   = 5,
    non_pump_ratio: int   = 3,
) -> Optional["pd.DataFrame"]:  # type: ignore[name-defined]
    """
    Build a labelled DataFrame for fingerprint training using raw cache data.

    Pump events  = cycle_tracker rows with max_percent_increase >= pump_threshold
    Non-pump windows = quiet 5-minute windows with no nearby pump

    Returns DataFrame with columns: event_time, is_pump, <feature cols>
    """
    import pandas as pd
    from core.database import get_postgres

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

        # ── Pull pump events from PostgreSQL (cycle_tracker) ─────────────────
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT cycle_start_time AS event_time,
                           max_percent_increase
                    FROM cycle_tracker
                    WHERE cycle_start_time >= %s
                      AND max_percent_increase >= %s
                    ORDER BY cycle_start_time
                """, [cutoff, pump_threshold])
                pump_rows = cur.fetchall()

        if len(pump_rows) < 3:
            logger.warning(f"[raw_cache] Only {len(pump_rows)} pump events — need ≥3")
            return None

        pumps_df = pd.DataFrame(pump_rows)
        pumps_df['is_pump'] = 1
        pumps_df['event_time'] = pd.to_datetime(pumps_df['event_time'], utc=True)
        # max_fwd = the actual pump gain (used by fingerprint quality scoring)
        pumps_df['max_fwd'] = pumps_df['max_percent_increase'].astype(float)
        logger.info(f"[raw_cache] Training: {len(pumps_df)} pump events "
                    f"(≥{pump_threshold}%) in last {lookback_hours}h")

        # ── Generate non-pump windows from DuckDB ─────────────────────────────
        con = open_reader()
        try:
            ts_range = con.execute("SELECT MIN(ts), MAX(ts) FROM ob_snapshots").fetchone()
            if ts_range[0] is None:
                logger.warning("[raw_cache] No OB data in cache")
                con.close()
                return None
            ts_start, ts_end = ts_range

            pump_times_df = pumps_df[['event_time']].copy()
            pump_times_df.columns = ['event_time']
            con.register('pump_times', pump_times_df)

            nonpro_df = con.execute(f"""
                WITH RECURSIVE times(t) AS (
                    SELECT TIMESTAMPTZ '{ts_start}'
                    UNION ALL
                    SELECT t + INTERVAL '5 minutes'
                    FROM times WHERE t < TIMESTAMPTZ '{ts_end}' - INTERVAL '{window_min} minutes'
                )
                SELECT t AS event_time, 0 AS is_pump
                FROM times
                WHERE NOT EXISTS (
                    SELECT 1 FROM pump_times p
                    WHERE ABS(EPOCH(t) - EPOCH(p.event_time::TIMESTAMPTZ)) < 600
                )
                ORDER BY RANDOM()
                LIMIT {len(pumps_df) * non_pump_ratio}
            """).df()
            con.unregister('pump_times')
            nonpro_df['event_time'] = pd.to_datetime(nonpro_df['event_time'], utc=True)

            # ── Combine and compute features ─────────────────────────────────
            # Include max_fwd (pump gain) for quality scoring in fingerprint
            nonpro_df['max_fwd'] = 0.0
            events = pd.concat([
                pumps_df[['event_time', 'is_pump', 'max_fwd']],
                nonpro_df[['event_time', 'is_pump', 'max_fwd']],
            ], ignore_index=True).sort_values('event_time').reset_index(drop=True)

            win_sec = window_min * 60
            con.register('events', events)
            con.execute("CREATE OR REPLACE TEMP TABLE _events AS SELECT event_time::TIMESTAMPTZ AS event_time, is_pump, max_fwd FROM events")
            con.unregister('events')

            feats_df = con.execute(f"""
                SELECT
                    e.event_time,
                    e.is_pump,
                    ANY_VALUE(e.max_fwd) AS max_fwd,
                    -- OB features
                    AVG(o.vol_imb)                                          AS ob_avg_vol_imb,
                    AVG(o.depth_ratio)                                      AS ob_avg_depth_ratio,
                    AVG(o.spread_bps)                                       AS ob_avg_spread_bps,
                    SUM(o.net_liq_1s)                                       AS ob_net_liq_change,
                    AVG(o.bid_liq / NULLIF(o.ask_liq, 0))                   AS ob_bid_ask_ratio,
                    AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.ts) < 60
                             THEN o.vol_imb END)                            AS ob_imb_1m,
                    AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.ts) < 60
                             THEN o.depth_ratio END)                        AS ob_depth_1m,
                    AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.ts) < 60
                             THEN o.bid_liq / NULLIF(o.ask_liq,0) END)      AS ob_bid_ask_1m,
                    -- microstructure: slope asymmetry and near-top depth
                    AVG(o.bid_slope / NULLIF(ABS(o.ask_slope), 0))          AS ob_slope_ratio,
                    AVG(o.bid_dep_5bps / NULLIF(o.ask_dep_5bps, 0))         AS ob_depth_5bps_ratio,
                    AVG(o.microprice_dev)                                   AS ob_microprice_dev,
                    -- Trade features
                    COUNT(t.ts)                                             AS tr_n,
                    SUM(t.sol_amount)                                       AS tr_total_sol,
                    SUM(CASE WHEN t.direction='buy' THEN t.sol_amount ELSE 0 END)
                      / NULLIF(SUM(t.sol_amount), 0)                       AS tr_buy_ratio,
                    SUM(CASE WHEN t.sol_amount > 50 THEN t.sol_amount ELSE 0 END)
                      / NULLIF(SUM(t.sol_amount), 0)                       AS tr_large_ratio,
                    AVG(t.sol_amount)                                       AS tr_avg_size,
                    -- Whale features (ABS handles legacy signed sol_moved; both direction labels supported)
                    COUNT(w.ts)                                             AS wh_n,
                    SUM(CASE WHEN w.direction IN ('in','receiving')  THEN ABS(w.sol_moved) ELSE 0 END)
                      - SUM(CASE WHEN w.direction IN ('out','sending') THEN ABS(w.sol_moved) ELSE 0 END)
                                                                            AS wh_net_flow,
                    SUM(CASE WHEN w.direction IN ('in','receiving') THEN ABS(w.sol_moved) ELSE 0 END)
                      / NULLIF(SUM(ABS(w.sol_moved)), 0)                   AS wh_inflow_ratio,
                    AVG(w.pct_moved)                                        AS wh_avg_pct_moved,
                    COUNT(CASE WHEN w.pct_moved > 50 THEN 1 END) * 1.0
                      / NULLIF(COUNT(w.ts), 0)                              AS wh_urgency_ratio
                FROM _events e
                LEFT JOIN ob_snapshots o
                       ON o.ts >= e.event_time - INTERVAL '{win_sec} seconds'
                      AND o.ts <  e.event_time
                LEFT JOIN raw_trades t
                       ON t.ts >= e.event_time - INTERVAL '{win_sec} seconds'
                      AND t.ts <  e.event_time
                LEFT JOIN whale_events w
                       ON w.ts >= e.event_time - INTERVAL '{win_sec} seconds'
                      AND w.ts <  e.event_time
                GROUP BY e.event_time, e.is_pump
            """).df()

        finally:
            con.close()

        # Derived trend features (short-term vs 5-min average)
        feats_df['ob_imb_trend']   = feats_df['ob_imb_1m']    - feats_df['ob_avg_vol_imb']
        feats_df['ob_depth_trend'] = feats_df['ob_depth_1m']   - feats_df['ob_avg_depth_ratio']
        feats_df['ob_liq_accel']   = feats_df['ob_bid_ask_1m'] - feats_df['ob_bid_ask_ratio']

        n_p  = int(feats_df['is_pump'].sum())
        n_np = int((feats_df['is_pump'] == 0).sum())
        logger.info(f"[raw_cache] Training set: {n_p} pumps, {n_np} non-pumps, "
                    f"{len(feats_df.columns)-2} features")
        return feats_df

    except Exception as e:
        logger.error(f"[raw_cache] load_training_data error: {e}", exc_info=True)
        return None


# Feature names for external use — keep in sync with get_live_features() and mega_simulator.py
FEATURE_NAMES = [
    # Order book — 5-min averages
    'ob_avg_vol_imb', 'ob_avg_depth_ratio', 'ob_avg_spread_bps',
    'ob_net_liq_change', 'ob_bid_ask_ratio',
    # Order book — 1-min vs 5-min trend
    'ob_imb_1m', 'ob_depth_1m', 'ob_bid_ask_1m',
    'ob_imb_trend', 'ob_depth_trend', 'ob_liq_accel',
    # Order book — microstructure
    'ob_slope_ratio', 'ob_depth_5bps_ratio', 'ob_microprice_dev',
    # Trades
    'tr_n', 'tr_total_sol', 'tr_buy_ratio', 'tr_large_ratio', 'tr_avg_size',
    # Whale — flow
    'wh_n', 'wh_net_flow', 'wh_inflow_ratio',
    # Whale — conviction
    'wh_avg_pct_moved', 'wh_urgency_ratio',
    # Price momentum (matches mega_simulator FEATURES list)
    'pm_price_change_30s', 'pm_price_change_1m',
    'pm_price_change_5m', 'pm_velocity_30s',
]

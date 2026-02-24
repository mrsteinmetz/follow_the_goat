#!/usr/bin/env python3
"""
Backfill raw DuckDB Parquet cache from PostgreSQL historical data.

Reads the last N hours of order_book_features, sol_stablecoin_trades,
and whale_movements from PostgreSQL and writes them to the three Parquet
snapshot files used by the raw cache pipeline.

Usage:
    python3 scripts/backfill_raw_cache.py
    python3 scripts/backfill_raw_cache.py --hours 48
"""

import sys
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq

from core.database import get_postgres
from core.raw_data_cache import OB_PARQUET, TRADE_PARQUET, WHALE_PARQUET, _CACHE_DIR


def backfill_ob(cutoff: datetime) -> int:
    print("  Fetching order_book_features...", flush=True)
    t0 = time.time()
    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT timestamp, mid_price, spread_bps,
                       bid_liquidity      AS bid_liq,
                       ask_liquidity      AS ask_liq,
                       volume_imbalance   AS vol_imb,
                       depth_imbalance_ratio AS depth_ratio,
                       microprice,
                       microprice_dev_bps AS microprice_dev,
                       net_liquidity_change_1s AS net_liq_1s,
                       bid_slope,
                       ask_slope,
                       bid_depth_bps_5    AS bid_dep_5bps,
                       ask_depth_bps_5    AS ask_dep_5bps
                FROM order_book_features
                WHERE timestamp >= %s
                ORDER BY timestamp
            """, [cutoff])
            rows = cur.fetchall()

    n = len(rows)
    print(f"    {n:,} rows in {time.time()-t0:.1f}s", flush=True)
    if not n:
        return 0

    def _f(rows, key):
        return pa.array([float(r[key]) if r[key] is not None else None for r in rows],
                        type=pa.float64())

    tbl = pa.table({
        'ts':             pa.array([r['timestamp'] for r in rows],
                                   type=pa.timestamp('us', tz='UTC')),
        'mid_price':      _f(rows, 'mid_price'),
        'spread_bps':     _f(rows, 'spread_bps'),
        'bid_liq':        _f(rows, 'bid_liq'),
        'ask_liq':        _f(rows, 'ask_liq'),
        'vol_imb':        _f(rows, 'vol_imb'),
        'depth_ratio':    _f(rows, 'depth_ratio'),
        'microprice':     _f(rows, 'microprice'),
        'microprice_dev': _f(rows, 'microprice_dev'),
        'net_liq_1s':     _f(rows, 'net_liq_1s'),
        'bid_slope':      _f(rows, 'bid_slope'),
        'ask_slope':      _f(rows, 'ask_slope'),
        'bid_dep_5bps':   _f(rows, 'bid_dep_5bps'),
        'ask_dep_5bps':   _f(rows, 'ask_dep_5bps'),
    })

    tmp = _CACHE_DIR / "ob_latest.bfill.parquet"
    pq.write_table(tbl, str(tmp), compression='snappy')
    tmp.replace(OB_PARQUET)
    print(f"    → {OB_PARQUET.name}  ({OB_PARQUET.stat().st_size / 1024:.0f} KB)", flush=True)
    return n


def backfill_trades(cutoff: datetime) -> int:
    print("  Fetching sol_stablecoin_trades...", flush=True)
    t0 = time.time()
    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT trade_timestamp,
                       sol_amount, stablecoin_amount, price,
                       direction,
                       (perp_direction IS NOT NULL) AS is_perp
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= %s
                ORDER BY trade_timestamp
            """, [cutoff])
            rows = cur.fetchall()

    n = len(rows)
    print(f"    {n:,} rows in {time.time()-t0:.1f}s", flush=True)
    if not n:
        return 0

    tbl = pa.table({
        'ts':         pa.array([r['trade_timestamp'] for r in rows],
                               type=pa.timestamp('us', tz='UTC')),
        'sol_amount': pa.array([float(r['sol_amount'] or 0) for r in rows], type=pa.float64()),
        'stable_amt': pa.array([float(r['stablecoin_amount'] or 0) for r in rows], type=pa.float64()),
        'price':      pa.array([float(r['price'] or 0) for r in rows], type=pa.float64()),
        'direction':  pa.array([str(r['direction'] or 'buy') for r in rows], type=pa.string()),
        'is_perp':    pa.array([bool(r['is_perp']) for r in rows], type=pa.bool_()),
    })

    tmp = _CACHE_DIR / "trade_latest.bfill.parquet"
    pq.write_table(tbl, str(tmp), compression='snappy')
    tmp.replace(TRADE_PARQUET)
    print(f"    → {TRADE_PARQUET.name}  ({TRADE_PARQUET.stat().st_size / 1024:.0f} KB)", flush=True)
    return n


def backfill_whales(cutoff: datetime) -> int:
    print("  Fetching whale_movements...", flush=True)
    t0 = time.time()
    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(timestamp, created_at) AS ts,
                       ABS(COALESCE(sol_change, abs_change, 0)) AS sol_moved,
                       CASE LOWER(direction)
                           WHEN 'receiving' THEN 'in'
                           WHEN 'buy'       THEN 'in'
                           WHEN 'inflow'    THEN 'in'
                           WHEN 'sending'   THEN 'out'
                           WHEN 'sell'      THEN 'out'
                           WHEN 'outflow'   THEN 'out'
                           ELSE 'out'
                       END AS direction,
                       CASE
                           WHEN movement_significance ~ '^[0-9.]+$'
                               THEN movement_significance::DOUBLE PRECISION
                           WHEN UPPER(movement_significance) = 'MAJOR'       THEN 1.0
                           WHEN UPPER(movement_significance) = 'SIGNIFICANT' THEN 0.7
                           WHEN UPPER(movement_significance) = 'MINOR'       THEN 0.3
                           ELSE 0.5
                       END AS significance,
                       percentage_moved
                FROM whale_movements
                WHERE COALESCE(timestamp, created_at) >= %s
                ORDER BY COALESCE(timestamp, created_at)
            """, [cutoff])
            rows = cur.fetchall()

    n = len(rows)
    print(f"    {n:,} rows in {time.time()-t0:.1f}s", flush=True)
    if not n:
        return 0

    tbl = pa.table({
        'ts':           pa.array([r['ts'] for r in rows],
                                 type=pa.timestamp('us', tz='UTC')),
        'sol_moved':    pa.array([float(r['sol_moved'] or 0) for r in rows], type=pa.float64()),
        'direction':    pa.array([str(r['direction'] or 'out') for r in rows], type=pa.string()),
        'significance': pa.array([float(r['significance']) if r['significance'] is not None else None
                                  for r in rows], type=pa.float64()),
        'pct_moved':    pa.array([float(r['percentage_moved'] or 0) for r in rows], type=pa.float64()),
    })

    tmp = _CACHE_DIR / "whale_latest.bfill.parquet"
    pq.write_table(tbl, str(tmp), compression='snappy')
    tmp.replace(WHALE_PARQUET)
    print(f"    → {WHALE_PARQUET.name}  ({WHALE_PARQUET.stat().st_size / 1024:.0f} KB)", flush=True)
    return n


def verify():
    from core.raw_data_cache import open_reader
    con = open_reader()
    ob = con.execute("SELECT COUNT(*), MIN(ts)::VARCHAR, MAX(ts)::VARCHAR FROM ob_snapshots").fetchone()
    tr = con.execute("SELECT COUNT(*) FROM raw_trades").fetchone()[0]
    wh = con.execute("SELECT COUNT(*) FROM whale_events").fetchone()[0]
    con.close()
    print(f"\n  ob_snapshots : {ob[0]:>8,}  ({ob[1][:16]} → {ob[2][:16]})")
    print(f"  raw_trades   : {tr:>8,}")
    print(f"  whale_events : {wh:>8,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hours', type=int, default=24,
                        help='Hours of history to backfill (default: 24)')
    args = parser.parse_args()

    _CACHE_DIR.mkdir(exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    print(f"\nBackfilling last {args.hours}h → Parquet (cutoff: {cutoff.strftime('%Y-%m-%d %H:%M UTC')})\n")
    t_total = time.time()

    n_ob    = backfill_ob(cutoff)
    n_tr    = backfill_trades(cutoff)
    n_wh    = backfill_whales(cutoff)

    print(f"\nTotal: {n_ob+n_tr+n_wh:,} rows in {time.time()-t_total:.1f}s")
    verify()


if __name__ == '__main__':
    main()

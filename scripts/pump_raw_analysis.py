#!/usr/bin/env python3
"""
Pump Raw Analysis
-----------------
Bypasses pre-computed buyin trail summaries entirely.

Loads raw data (OB, trades, whales, prices) directly into DuckDB and labels
pumps from cycle_tracker — the same source the dashboard uses. This gives us
62+ clean pump events instead of 2-4, and lets us compute features without
the strict context filters that were killing 99% of candidates.

Flow:
  1. Pull raw tables from PostgreSQL → DuckDB (in-memory)
  2. Label pump events from cycle_tracker (max_percent_increase >= PUMP_THRESHOLD)
  3. Generate equal-count non-pump windows from quiet periods
  4. For each event window, compute features from raw OB / trades / whales
  5. Run separation analysis (Mann-Whitney) per feature
  6. Discover threshold rules
  7. Print a clear report

Usage:
  python3 scripts/pump_raw_analysis.py
  python3 scripts/pump_raw_analysis.py --hours 24 --threshold 0.3
"""

import sys
import time
import argparse
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
from scipy import stats

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.database import get_postgres

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
PUMP_THRESHOLD      = 0.3   # % gain to count as a pump (cycle_tracker)
LOOKBACK_HOURS      = 24
FEATURE_WINDOW_MIN  = 5     # minutes of data before each event to compute features
MIN_SEPARATION      = 0.10  # minimum Mann-Whitney U statistic to keep a feature
MIN_PUMP_PRECISION  = 0.45  # minimum precision for a rule to be approved
MIN_RULE_FIRES      = 3     # minimum times a rule must fire in the dataset


# =============================================================================
# 1. LOAD RAW DATA INTO DUCKDB
# =============================================================================

def load_raw_data(hours: int = LOOKBACK_HOURS) -> duckdb.DuckDBPyConnection:
    """Pull raw tables from PostgreSQL into an in-memory DuckDB instance."""
    con = duckdb.connect(":memory:")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    logger.info(f"Loading raw data for last {hours}h from PostgreSQL...")
    t0 = time.time()

    with get_postgres() as pg:
        with pg.cursor() as cur:

            # ── Order book snapshots ──────────────────────────────────────────
            cur.execute("""
                SELECT timestamp, mid_price, spread_bps,
                       bid_liquidity, ask_liquidity,
                       volume_imbalance, depth_imbalance_ratio,
                       microprice, microprice_dev_bps,
                       net_liquidity_change_1s,
                       bid_depth_bps_5, ask_depth_bps_5
                FROM order_book_features
                WHERE timestamp >= %s
                ORDER BY timestamp
            """, [cutoff])
            ob_rows = cur.fetchall()
            ob_df = pd.DataFrame(ob_rows, columns=[d.name for d in cur.description])
            con.register("raw_ob", ob_df)
            con.execute("CREATE TABLE ob AS SELECT * FROM raw_ob")
            con.unregister("raw_ob")
            logger.info(f"  order_book_features: {len(ob_df):,} rows")

            # ── Stablecoin ↔ SOL trades ───────────────────────────────────────
            cur.execute("""
                SELECT trade_timestamp AS ts,
                       sol_amount, stablecoin_amount, price,
                       direction,
                       CASE WHEN perp_direction IS NOT NULL THEN 1 ELSE 0 END AS is_perp
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= %s
                ORDER BY trade_timestamp
            """, [cutoff])
            tr_rows = cur.fetchall()
            tr_df = pd.DataFrame(tr_rows, columns=[d.name for d in cur.description])
            # cast Decimal columns to float to avoid DuckDB overflow
            for col in ['sol_amount', 'stablecoin_amount', 'price']:
                tr_df[col] = tr_df[col].astype(float)
            con.register("raw_tr", tr_df)
            con.execute("CREATE TABLE trades AS SELECT * FROM raw_tr")
            con.unregister("raw_tr")
            logger.info(f"  sol_stablecoin_trades: {len(tr_df):,} rows")

            # ── Whale movements ───────────────────────────────────────────────
            cur.execute("""
                SELECT timestamp AS ts,
                       abs_change AS sol_moved,
                       direction,
                       movement_significance,
                       percentage_moved
                FROM whale_movements
                WHERE timestamp >= %s
                ORDER BY timestamp
            """, [cutoff])
            wh_rows = cur.fetchall()
            wh_df = pd.DataFrame(wh_rows, columns=[d.name for d in cur.description])
            con.register("raw_wh", wh_df)
            con.execute("CREATE TABLE whales AS SELECT * FROM raw_wh")
            con.unregister("raw_wh")
            logger.info(f"  whale_movements: {len(wh_df):,} rows")

            # ── Cycle tracker (pump labels) ───────────────────────────────────
            cur.execute("""
                SELECT id, cycle_start_time, cycle_end_time,
                       sequence_start_price, highest_price_reached,
                       max_percent_increase, threshold
                FROM cycle_tracker
                WHERE cycle_start_time >= %s
                ORDER BY cycle_start_time
            """, [cutoff])
            cy_rows = cur.fetchall()
            cy_df = pd.DataFrame(cy_rows, columns=[d.name for d in cur.description])
            con.register("raw_cy", cy_df)
            con.execute("CREATE TABLE cycles AS SELECT * FROM raw_cy")
            con.unregister("raw_cy")
            logger.info(f"  cycle_tracker: {len(cy_df):,} rows "
                        f"(>={PUMP_THRESHOLD}%: {int((cy_df['max_percent_increase'] >= PUMP_THRESHOLD).sum() if len(cy_df) else 0)})")

    logger.info(f"  Data loaded in {time.time()-t0:.1f}s")
    return con


# =============================================================================
# 2. BUILD EVENT TABLE (pump + non-pump windows)
# =============================================================================

def build_events(con: duckdb.DuckDBPyConnection,
                 pump_threshold: float = PUMP_THRESHOLD,
                 window_min: int = FEATURE_WINDOW_MIN) -> pd.DataFrame:
    """
    Label pump events from cycle_tracker and generate non-pump windows.

    Pump   = cycle where max_percent_increase >= PUMP_THRESHOLD
    Non-pump = windows where no pump cycle starts within ±10 minutes
    """
    # Pump events: use cycle_start_time as the event time
    pumps = con.execute(f"""
        SELECT cycle_start_time AS event_time,
               max_percent_increase,
               1 AS is_pump
        FROM cycles
        WHERE max_percent_increase >= {pump_threshold}
        ORDER BY cycle_start_time
    """).df()

    n_pumps = len(pumps)
    logger.info(f"Pump events: {n_pumps}")

    if n_pumps < 3:
        logger.error("Not enough pump events. Try lowering --threshold.")
        return pd.DataFrame()

    # Non-pump windows: every 5 minutes through the period, excluding ±10m around any pump
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pump_times AS
        SELECT cycle_start_time AS event_time
        FROM cycles
        WHERE max_percent_increase >= {pump_threshold}
    """)

    # Get time range from OB data
    ts_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ob").fetchone()
    ts_start, ts_end = ts_range

    # Build a candidate grid every 5 minutes
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE candidate_windows AS
        WITH RECURSIVE times(t) AS (
            SELECT TIMESTAMPTZ '{ts_start}'
            UNION ALL
            SELECT t + INTERVAL '5 minutes'
            FROM times WHERE t < TIMESTAMPTZ '{ts_end}' - INTERVAL '{window_min} minutes'
        )
        SELECT t AS event_time FROM times
    """)

    # Exclude any candidate within 10 minutes of a pump
    non_pumps_all = con.execute("""
        SELECT c.event_time, 0 AS is_pump, NULL::DOUBLE AS max_percent_increase
        FROM candidate_windows c
        WHERE NOT EXISTS (
            SELECT 1 FROM pump_times p
            WHERE ABS(EPOCH(c.event_time) - EPOCH(p.event_time)) < 600
        )
        ORDER BY RANDOM()
    """).df()

    # Downsample non-pumps to 3× pump count for balance
    n_nonpro = min(len(non_pumps_all), n_pumps * 3)
    non_pumps = non_pumps_all.sample(n=n_nonpro, random_state=42)

    events = pd.concat([pumps[['event_time', 'is_pump', 'max_percent_increase']],
                        non_pumps[['event_time', 'is_pump', 'max_percent_increase']]],
                       ignore_index=True)
    events['event_time'] = pd.to_datetime(events['event_time'], utc=True)
    events = events.sort_values('event_time').reset_index(drop=True)

    logger.info(f"Events: {int(events['is_pump'].sum())} pumps, "
                f"{int((events['is_pump']==0).sum())} non-pumps")
    return events


# =============================================================================
# 3. COMPUTE FEATURES FOR EACH EVENT WINDOW
# =============================================================================

def compute_features(con: duckdb.DuckDBPyConnection,
                     events: pd.DataFrame,
                     window_min: int = FEATURE_WINDOW_MIN) -> pd.DataFrame:
    """
    For each event, compute features from the raw data in the N minutes before it.
    All computation happens in DuckDB SQL — no pre-computed summaries needed.
    """
    logger.info(f"Computing features for {len(events)} events ({window_min}m window)...")
    t0 = time.time()

    # Normalise timestamps to UTC TIMESTAMPTZ so DuckDB comparisons work
    events = events.copy()
    events['event_time'] = pd.to_datetime(events['event_time'], utc=True)
    con.register("events_df", events[['event_time', 'is_pump', 'max_percent_increase']])
    con.execute("CREATE OR REPLACE TEMP TABLE events AS SELECT event_time::TIMESTAMPTZ AS event_time, is_pump, max_percent_increase FROM events_df")
    con.unregister("events_df")
    # Normalise ob/trades/whales timestamps to TIMESTAMPTZ
    con.execute("ALTER TABLE ob ALTER COLUMN timestamp TYPE TIMESTAMPTZ")
    con.execute("ALTER TABLE trades ALTER COLUMN ts TYPE TIMESTAMPTZ")
    con.execute("ALTER TABLE whales ALTER COLUMN ts TYPE TIMESTAMPTZ")

    win_sec = window_min * 60

    # ── Order book features ───────────────────────────────────────────────────
    ob_feats = con.execute(f"""
        SELECT
            e.event_time,
            -- mean values over the window
            AVG(o.volume_imbalance)        AS ob_avg_volume_imbalance,
            STDDEV(o.volume_imbalance)     AS ob_std_volume_imbalance,
            AVG(o.depth_imbalance_ratio)   AS ob_avg_depth_ratio,
            AVG(o.spread_bps)              AS ob_avg_spread_bps,
            STDDEV(o.spread_bps)           AS ob_std_spread_bps,
            AVG(o.microprice_dev_bps)      AS ob_avg_microprice_dev,
            SUM(o.net_liquidity_change_1s) AS ob_net_liq_change,
            -- trend: last 1m vs full window
            AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.timestamp) < 60
                     THEN o.volume_imbalance END) AS ob_imbalance_last1m,
            AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.timestamp) < 60
                     THEN o.depth_imbalance_ratio END) AS ob_depth_ratio_last1m,
            -- bid/ask liquidity ratio and change
            AVG(o.bid_liquidity / NULLIF(o.ask_liquidity, 0)) AS ob_bid_ask_liq_ratio,
            AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.timestamp) < 60
                     THEN o.bid_liquidity / NULLIF(o.ask_liquidity, 0) END) AS ob_bid_ask_liq_ratio_1m,
            COUNT(o.timestamp) AS ob_snapshot_count
        FROM events e
        LEFT JOIN ob o
               ON o.timestamp >= e.event_time - INTERVAL '{win_sec} seconds'
              AND o.timestamp <  e.event_time
        GROUP BY e.event_time
    """).df()

    # ── Trade features ────────────────────────────────────────────────────────
    tr_feats = con.execute(f"""
        SELECT
            e.event_time,
            COUNT(t.ts)                                   AS tr_trade_count,
            SUM(t.sol_amount)                             AS tr_total_sol,
            -- buy/sell ratio (SOL amount)
            SUM(CASE WHEN t.direction = 'buy' THEN t.sol_amount ELSE 0 END)
              / NULLIF(SUM(t.sol_amount), 0)              AS tr_buy_ratio,
            -- large trade ratio (>50 SOL)
            SUM(CASE WHEN t.sol_amount > 50 THEN t.sol_amount ELSE 0 END)
              / NULLIF(SUM(t.sol_amount), 0)              AS tr_large_trade_ratio,
            -- avg trade size
            AVG(t.sol_amount)                             AS tr_avg_trade_size,
            -- buy pressure acceleration: last 1m vs window avg
            AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(t.ts) < 60
                     THEN CASE WHEN t.direction='buy' THEN t.sol_amount ELSE 0 END END)
              / NULLIF(AVG(CASE WHEN t.direction='buy' THEN t.sol_amount ELSE 0 END), 0)
                                                          AS tr_buy_acceleration,
            -- trade count in last 1m vs avg
            COUNT(CASE WHEN EPOCH(e.event_time) - EPOCH(t.ts) < 60 THEN 1 END)
              * 1.0 / NULLIF(COUNT(t.ts), 0) * {win_sec}
                                                          AS tr_recent_trade_density,
            -- perp activity
            AVG(t.is_perp::FLOAT)                        AS tr_perp_ratio
        FROM events e
        LEFT JOIN trades t
               ON t.ts >= e.event_time - INTERVAL '{win_sec} seconds'
              AND t.ts <  e.event_time
        GROUP BY e.event_time
    """).df()

    # ── Whale features ────────────────────────────────────────────────────────
    wh_feats = con.execute(f"""
        SELECT
            e.event_time,
            COUNT(w.ts)                AS wh_movement_count,
            SUM(w.sol_moved)           AS wh_total_sol_moved,
            -- net directional flow
            SUM(CASE WHEN w.direction = 'in'  THEN w.sol_moved ELSE 0 END)
              - SUM(CASE WHEN w.direction = 'out' THEN w.sol_moved ELSE 0 END)
                                       AS wh_net_flow,
            -- inflow ratio
            SUM(CASE WHEN w.direction = 'in' THEN w.sol_moved ELSE 0 END)
              / NULLIF(SUM(w.sol_moved), 0) AS wh_inflow_ratio,
            -- large whale moves (significance > 0.5)
            COUNT(CASE WHEN TRY_CAST(w.movement_significance AS DOUBLE) > 0.5 THEN 1 END)
                                       AS wh_large_move_count,
            -- recent vs full-window ratio
            COUNT(CASE WHEN EPOCH(e.event_time) - EPOCH(w.ts) < 60 THEN 1 END)
              * 1.0 / NULLIF(COUNT(w.ts), 0) AS wh_recent_activity_ratio
        FROM events e
        LEFT JOIN whales w
               ON w.ts >= e.event_time - INTERVAL '{win_sec} seconds'
              AND w.ts <  e.event_time
        GROUP BY e.event_time
    """).df()

    # ── Price momentum from OB mid_price ─────────────────────────────────────
    pr_feats = con.execute(f"""
        SELECT
            e.event_time,
            -- price at event start vs N minutes earlier
            LAST(o.mid_price ORDER BY o.timestamp) AS pr_price_now,
            FIRST(o.mid_price ORDER BY o.timestamp) AS pr_price_start,
            (LAST(o.mid_price ORDER BY o.timestamp) - FIRST(o.mid_price ORDER BY o.timestamp))
              / NULLIF(FIRST(o.mid_price ORDER BY o.timestamp), 0)
                                                   AS pr_change_window,
            -- 1m price change (last 1m only)
            LAST(o.mid_price ORDER BY o.timestamp)
              / NULLIF(
                  AVG(CASE WHEN EPOCH(e.event_time) - EPOCH(o.timestamp) BETWEEN 60 AND 120
                           THEN o.mid_price END), 0) - 1
                                                   AS pr_change_1m,
            -- price volatility (std/mean)
            STDDEV(o.mid_price) / NULLIF(AVG(o.mid_price), 0) AS pr_volatility
        FROM events e
        LEFT JOIN ob o
               ON o.timestamp >= e.event_time - INTERVAL '{win_sec} seconds'
              AND o.timestamp <  e.event_time
        GROUP BY e.event_time
    """).df()

    # ── Merge all feature sets ────────────────────────────────────────────────
    df = events[['event_time', 'is_pump', 'max_percent_increase']].copy()
    for feat_df in [ob_feats, tr_feats, wh_feats, pr_feats]:
        feat_df['event_time'] = pd.to_datetime(feat_df['event_time'], utc=True)
        df = df.merge(feat_df, on='event_time', how='left')

    # Derived features
    df['ob_imbalance_trend'] = df['ob_imbalance_last1m'] - df['ob_avg_volume_imbalance']
    df['ob_depth_trend'] = df['ob_depth_ratio_last1m'] - df['ob_avg_depth_ratio']
    df['ob_liq_acceleration'] = df['ob_bid_ask_liq_ratio_1m'] - df['ob_bid_ask_liq_ratio']

    logger.info(f"  Features computed in {time.time()-t0:.1f}s — {len(df)} events, {len(df.columns)-3} features")
    return df


# =============================================================================
# 4. SEPARATION ANALYSIS
# =============================================================================

FEATURE_COLS = [
    'ob_avg_volume_imbalance', 'ob_std_volume_imbalance', 'ob_avg_depth_ratio',
    'ob_avg_spread_bps', 'ob_std_spread_bps', 'ob_avg_microprice_dev',
    'ob_net_liq_change', 'ob_imbalance_last1m', 'ob_depth_ratio_last1m',
    'ob_bid_ask_liq_ratio', 'ob_bid_ask_liq_ratio_1m',
    'ob_imbalance_trend', 'ob_depth_trend', 'ob_liq_acceleration',
    'tr_trade_count', 'tr_total_sol', 'tr_buy_ratio', 'tr_large_trade_ratio',
    'tr_avg_trade_size', 'tr_buy_acceleration', 'tr_recent_trade_density', 'tr_perp_ratio',
    'wh_movement_count', 'wh_total_sol_moved', 'wh_net_flow',
    'wh_inflow_ratio', 'wh_large_move_count', 'wh_recent_activity_ratio',
    'pr_change_window', 'pr_change_1m', 'pr_volatility',
]


def separation_score(pump_vals: np.ndarray, non_pump_vals: np.ndarray) -> float:
    """Mann-Whitney U normalised to 0–1 (0.5 = no separation, 1.0 = perfect)."""
    clean_p = pump_vals[~np.isnan(pump_vals)]
    clean_n = non_pump_vals[~np.isnan(non_pump_vals)]
    if len(clean_p) < 2 or len(clean_n) < 2:
        return 0.0
    u, _ = stats.mannwhitneyu(clean_p, clean_n, alternative='two-sided')
    return abs(u / (len(clean_p) * len(clean_n)) - 0.5) * 2  # 0=random, 1=perfect


def analyse_features(df: pd.DataFrame) -> pd.DataFrame:
    """Rank all features by pump/non-pump separation."""
    pumps     = df[df['is_pump'] == 1]
    non_pumps = df[df['is_pump'] == 0]

    rows = []
    for feat in FEATURE_COLS:
        if feat not in df.columns:
            continue
        pv  = pumps[feat].dropna().values.astype(float)
        npv = non_pumps[feat].dropna().values.astype(float)
        sep = separation_score(pv, npv)
        rows.append({
            'feature': feat,
            'sep': sep,
            'pump_median':     float(np.nanmedian(pv)) if len(pv) else float('nan'),
            'non_pump_median': float(np.nanmedian(npv)) if len(npv) else float('nan'),
            'pump_n':          len(pv),
            'non_pump_n':      len(npv),
        })
    return pd.DataFrame(rows).sort_values('sep', ascending=False).reset_index(drop=True)


# =============================================================================
# 5. THRESHOLD DISCOVERY
# =============================================================================

def find_threshold(feature_vals: np.ndarray, labels: np.ndarray,
                   direction: str) -> Optional[dict]:
    """Find the threshold that maximises precision × sqrt(recall)."""
    clean = ~np.isnan(feature_vals)
    vals, labs = feature_vals[clean], labels[clean]
    if len(vals) < 5:
        return None
    thresholds = np.percentile(vals, np.arange(10, 91, 10))
    best = None
    for thr in thresholds:
        mask = (vals < thr) if direction == 'below' else (vals > thr)
        n_sig = mask.sum()
        if n_sig < MIN_RULE_FIRES:
            continue
        tp = (mask & (labs == 1)).sum()
        prec = tp / n_sig
        recall = tp / max((labs == 1).sum(), 1)
        score = prec * (recall ** 0.5)
        if best is None or score > best['score']:
            best = {'threshold': float(thr), 'precision': float(prec),
                    'recall': float(recall), 'n_fires': int(n_sig),
                    'score': score, 'direction': direction}
    return best


def discover_rules(df: pd.DataFrame, sep_df: pd.DataFrame,
                   top_n: int = 10) -> pd.DataFrame:
    """Discover single-feature threshold rules from the top-separation features."""
    labels = df['is_pump'].values.astype(float)
    rules = []

    for _, row in sep_df.head(top_n).iterrows():
        feat = row['feature']
        if feat not in df.columns or row['sep'] < MIN_SEPARATION:
            continue
        vals = df[feat].values.astype(float)
        direction = 'above' if row['pump_median'] > row['non_pump_median'] else 'below'
        result = find_threshold(vals, labels, direction)
        if result and result['precision'] >= MIN_PUMP_PRECISION:
            rules.append({
                'feature':    feat,
                'sep':        round(row['sep'], 3),
                'direction':  result['direction'],
                'threshold':  round(result['threshold'], 5),
                'precision':  round(result['precision'], 3),
                'recall':     round(result['recall'], 3),
                'n_fires':    result['n_fires'],
                'pump_median':     round(row['pump_median'], 5),
                'non_pump_median': round(row['non_pump_median'], 5),
            })

    return pd.DataFrame(rules).sort_values('precision', ascending=False).reset_index(drop=True)


# =============================================================================
# 6. REPORT
# =============================================================================

def print_report(df: pd.DataFrame, sep_df: pd.DataFrame,
                 rules_df: pd.DataFrame, hours: int) -> None:
    n_pumps = int(df['is_pump'].sum())
    n_non   = int((df['is_pump'] == 0).sum())

    print("\n" + "="*70)
    print(f"  PUMP RAW ANALYSIS  |  {hours}h lookback  |  {PUMP_THRESHOLD}% threshold")
    print("="*70)
    print(f"  Events: {n_pumps} pumps  +  {n_non} non-pumps  =  {len(df)} total")
    print(f"  Pump rate: {n_pumps/(n_pumps+n_non)*100:.1f}%")
    print()

    print("── TOP FEATURES BY SEPARATION ──────────────────────────────────────")
    print(f"  {'feature':<38}  {'sep':>5}  {'pump_med':>9}  {'non_p_med':>9}")
    for _, r in sep_df.head(15).iterrows():
        marker = " ✓" if r['sep'] >= MIN_SEPARATION else "  "
        print(f"  {r['feature']:<38}  {r['sep']:>5.3f}  {r['pump_median']:>+9.5f}  {r['non_pump_median']:>+9.5f}{marker}")

    if len(rules_df):
        print()
        print("── APPROVED RULES ───────────────────────────────────────────────────")
        for _, r in rules_df.iterrows():
            print(f"  {r['feature']} {r['direction'].upper()} {r['threshold']:.5f}")
            print(f"    precision={r['precision']:.0%}  recall={r['recall']:.0%}  fires={r['n_fires']}")
    else:
        print()
        print("  (no rules passed threshold — see features above for candidates)")

    print()
    print("="*70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Pump raw signal analysis")
    parser.add_argument('--hours',     type=int,   default=LOOKBACK_HOURS,   help="Lookback window in hours")
    parser.add_argument('--threshold', type=float, default=PUMP_THRESHOLD,   help="Min %% gain for pump label")
    parser.add_argument('--window',    type=int,   default=FEATURE_WINDOW_MIN, help="Feature window in minutes")
    args = parser.parse_args()

    con = load_raw_data(args.hours)
    events = build_events(con, pump_threshold=args.threshold, window_min=args.window)

    if events.empty:
        logger.error("No events to analyse.")
        return

    df = compute_features(con, events, args.window)
    sep_df = analyse_features(df)
    rules_df = discover_rules(df, sep_df)
    print_report(df, sep_df, rules_df, args.hours)
    con.close()


if __name__ == "__main__":
    main()

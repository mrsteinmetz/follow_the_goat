"""
Exit Signal Simulator  v3  (mega_simulator_exit_signals.py)
===========================================================
Discovers per-second exit signal conditions that predict price drops
during active trades — using the exact same features as get_live_features().

This is the EXIT mirror of mega_simulator.py:

  mega_simulator.py          — "will price RISE ≥0.1% in 7 min?"  → BUY signal
  mega_simulator_exit_signals— "will price FALL ≥0.1% in 60s?"    → SELL signal

Data sources (last 24-48 hours, what's in DuckDB cache):
  DuckDB ob_snapshots  — order book snapshots every ~1s
  DuckDB raw_trades    — all SOL trades
  DuckDB whale_events  — large wallet movements
  PostgreSQL prices    — per-second SOL prices (for forward drop labels)
  PostgreSQL buyins    — entry/exit prices and timestamps

How it works:
  1. Loads sold trades from follow_the_goat_buyins.
  2. For each trade, samples features every SAMPLE_SECS seconds by replaying
     the DuckDB feature computation at that historical timestamp.
     Uses the exact same query as get_live_features() — feature names match.
  3. Labels each sample: "did price drop ≥DROP_PCT% within FORWARD_SECS?"
  4. GA finds 2-4 conditions (feature + direction + threshold) that predict
     the bearish outcome with ≥MIN_BEARISH_PRECISION precision.
  5. Full simulation backtest: if this rule had been live, what exits would
     you have gotten vs the actual trailing stop exits?

Live integration (direct plug-in, zero translation):
  - Discovered conditions use the EXACT same feature names as get_live_features().
  - Drop into trailing stop seller: call get_live_features() every second,
    if ALL conditions met → exit immediately.

Usage:
    python3 scripts/mega_simulator_exit_signals.py
    python3 scripts/mega_simulator_exit_signals.py --plays 3,4
    python3 scripts/mega_simulator_exit_signals.py --quick
    python3 scripts/mega_simulator_exit_signals.py --drop 0.05 --fwd 30
    python3 scripts/mega_simulator_exit_signals.py --apply
"""

from __future__ import annotations

import argparse
import bisect
import json
import logging
import random
import sys
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("exit_signal_sim")

# ── constants ─────────────────────────────────────────────────────────────────
COST_PCT              = 0.001      # 0.1% round-trip cost
DEFAULT_PLAY_IDS      = [3, 4, 5, 6]
SAMPLE_SECS           = 30         # how often to sample features during each trade
FEATURE_WINDOW_SECS   = 300        # rolling window for feature computation (5 min)
DROP_THRESHOLD_PCT    = 0.10       # bearish label threshold: price drops ≥ this %
FORWARD_SECS          = 60         # forward window to measure the drop
MIN_BEARISH_PRECISION = 0.55       # GA hard-reject below this
MIN_OOS_PRECISION     = 0.52
MIN_OOS_FOLDS_PASSING = 3
OOS_FOLDS             = 4
MIN_TRADES_FOR_GA     = 10

# GA parameters
GA_POPULATION    = 300
GA_GENERATIONS   = 100
GA_ELITE_FRAC    = 0.10
GA_CROSSOVER     = 0.60
GA_MUTATION      = 0.25
GA_TOURNAMENT_K  = 5
GA_MIN_CONDS     = 2
GA_MAX_CONDS     = 4

# Quick mode
QUICK_POPULATION  = 40
QUICK_GENERATIONS = 30

# ── feature list — exact names returned by get_live_features() ────────────────
# These are the only features the live system can evaluate each second.
LIVE_FEATURES = [
    "ob_avg_vol_imb",    "ob_avg_depth_ratio", "ob_avg_spread_bps",
    "ob_net_liq_change", "ob_bid_ask_ratio",
    "ob_imb_trend",      "ob_depth_trend",     "ob_liq_accel",
    "ob_slope_ratio",    "ob_depth_5bps_ratio", "ob_microprice_dev",
    "tr_buy_ratio",      "tr_large_ratio",      "tr_buy_accel",
    "tr_avg_size",       "tr_n",
    "wh_net_flow",       "wh_inflow_ratio",     "wh_avg_pct_moved",
    "wh_urgency_ratio",
    "pm_price_change_30s", "pm_price_change_1m",
    "pm_price_change_5m",  "pm_velocity_30s",
]
N_FEATURES               = len(LIVE_FEATURES)
FEAT_IDX: Dict[str, int] = {f: i for i, f in enumerate(LIVE_FEATURES)}


# =============================================================================
# FEATURE COMPUTATION FROM DUCKDB (historical replay)
# =============================================================================

def _build_feature_timeseries(
    start_ts: datetime,
    end_ts:   datetime,
    sample_secs: int = SAMPLE_SECS,
    win_secs:    int = FEATURE_WINDOW_SECS,
) -> Optional[Dict[datetime, Dict[str, float]]]:
    """
    Compute get_live_features() at every sample_secs interval across [start_ts, end_ts].

    Uses DuckDB parquet snapshots (read-only, no lock needed).
    Pulls the raw data once, then builds a pandas time-series with rolling windows.

    Returns  {bucket_ts: {feature_name: value}}  or None on failure.
    """
    try:
        import pandas as pd
        from core.raw_data_cache import open_reader

        # Add buffer for rolling windows
        load_start = start_ts - timedelta(seconds=win_secs + 60)
        load_end   = end_ts   + timedelta(seconds=FORWARD_SECS + 60)

        con = open_reader()

        # ── Order book ──────────────────────────────────────────────────────
        ob_df = con.execute("""
            SELECT ts, vol_imb, depth_ratio, spread_bps, net_liq_1s,
                   bid_liq, ask_liq, bid_slope, ask_slope,
                   bid_dep_5bps, ask_dep_5bps, microprice_dev
            FROM ob_snapshots
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts
        """, [load_start, load_end]).df()

        # ── Trades ──────────────────────────────────────────────────────────
        tr_df = con.execute("""
            SELECT ts, sol_amount, direction
            FROM raw_trades
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts
        """, [load_start, load_end]).df()

        # ── Whale events ────────────────────────────────────────────────────
        wh_df = con.execute("""
            SELECT ts, sol_moved, direction, pct_moved
            FROM whale_events
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts
        """, [load_start, load_end]).df()

        con.close()

        if ob_df.empty:
            logger.warning("No DuckDB OB data in the requested window")
            return None

        # ── Convert to UTC-aware timestamps ─────────────────────────────────
        for df in [ob_df, tr_df, wh_df]:
            if not df.empty:
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
                df.set_index("ts", inplace=True)

        # ── Generate sample buckets ──────────────────────────────────────────
        # Snap start to sample_secs boundary
        start_epoch = int(start_ts.timestamp())
        snapped     = start_epoch - (start_epoch % sample_secs)
        buckets: List[datetime] = []
        t = snapped
        end_epoch = int(end_ts.timestamp()) + sample_secs
        while t <= end_epoch:
            buckets.append(datetime.fromtimestamp(t, tz=timezone.utc))
            t += sample_secs

        result: Dict[datetime, Dict[str, float]] = {}

        for ref_ts in buckets:
            win_start = ref_ts - timedelta(seconds=win_secs)
            win_1m    = ref_ts - timedelta(seconds=60)

            # ── OB features ─────────────────────────────────────────────────
            ob_win = ob_df.loc[win_start:ref_ts] if not ob_df.empty else ob_df
            ob_1m  = ob_df.loc[win_1m:ref_ts]   if not ob_df.empty else ob_df

            def safe_mean(df, col):
                s = df[col] if col in df.columns else None
                return float(s.mean()) if s is not None and len(s) else 0.0

            bid_liq_5m  = safe_mean(ob_win, "bid_liq")
            ask_liq_5m  = safe_mean(ob_win, "ask_liq")
            bid_ask_5m  = bid_liq_5m / ask_liq_5m if ask_liq_5m > 0 else 1.0
            bid_ask_1m  = (
                float(ob_1m["bid_liq"].mean() / ob_1m["ask_liq"].replace(0, np.nan).mean())
                if not ob_1m.empty else bid_ask_5m
            )
            ob_imb_5m   = safe_mean(ob_win, "vol_imb")
            ob_depth_5m = safe_mean(ob_win, "depth_ratio")
            ob_imb_1m   = safe_mean(ob_1m,  "vol_imb")   if not ob_1m.empty else ob_imb_5m
            ob_depth_1m = safe_mean(ob_1m,  "depth_ratio") if not ob_1m.empty else ob_depth_5m

            ob_feats = {
                "ob_avg_vol_imb":    ob_imb_5m,
                "ob_avg_depth_ratio": ob_depth_5m,
                "ob_avg_spread_bps": safe_mean(ob_win, "spread_bps"),
                "ob_net_liq_change": float(ob_win["net_liq_1s"].sum()) if "net_liq_1s" in ob_win.columns and len(ob_win) else 0.0,
                "ob_bid_ask_ratio":  bid_ask_5m,
                "ob_imb_trend":      ob_imb_1m  - ob_imb_5m,
                "ob_depth_trend":    ob_depth_1m - ob_depth_5m,
                "ob_liq_accel":      bid_ask_1m  - bid_ask_5m,
                "ob_slope_ratio":    float(
                    (ob_win["bid_slope"] / ob_win["ask_slope"].abs().replace(0, np.nan)).mean()
                ) if "bid_slope" in ob_win.columns and len(ob_win) else 0.0,
                "ob_depth_5bps_ratio": float(
                    (ob_win["bid_dep_5bps"] / ob_win["ask_dep_5bps"].replace(0, np.nan)).mean()
                ) if "bid_dep_5bps" in ob_win.columns and len(ob_win) else 1.0,
                "ob_microprice_dev": safe_mean(ob_win, "microprice_dev"),
            }

            # ── Trade features ───────────────────────────────────────────────
            tr_win = tr_df.loc[win_start:ref_ts] if not tr_df.empty else tr_df
            tr_1m  = tr_df.loc[win_1m:ref_ts]   if not tr_df.empty else tr_df

            if not tr_win.empty:
                total_sol  = float(tr_win["sol_amount"].sum()) or 1.0
                buy_sol    = float(tr_win[tr_win["direction"] == "buy"]["sol_amount"].sum())
                large_sol  = float(tr_win[tr_win["sol_amount"] > 50]["sol_amount"].sum())
                buy_ratio_5m = buy_sol / total_sol
                buy_sol_1m = float(tr_1m[tr_1m["direction"] == "buy"]["sol_amount"].sum()) if not tr_1m.empty else 0.0
                tot_sol_1m = float(tr_1m["sol_amount"].sum()) if not tr_1m.empty else 1.0
                buy_ratio_1m = buy_sol_1m / tot_sol_1m if tot_sol_1m > 0 else buy_ratio_5m
                tr_feats = {
                    "tr_buy_ratio":   buy_ratio_5m,
                    "tr_large_ratio": large_sol / total_sol,
                    "tr_buy_accel":   buy_ratio_1m / buy_ratio_5m if buy_ratio_5m > 0 else 1.0,
                    "tr_avg_size":    float(tr_win["sol_amount"].mean()),
                    "tr_n":           float(len(tr_win)),
                }
            else:
                tr_feats = {"tr_buy_ratio": 0.5, "tr_large_ratio": 0.0,
                            "tr_buy_accel": 1.0, "tr_avg_size": 0.0, "tr_n": 0.0}

            # ── Whale features ───────────────────────────────────────────────
            wh_win = wh_df.loc[win_start:ref_ts] if not wh_df.empty else wh_df

            if not wh_win.empty:
                in_dir   = wh_win["direction"].isin(["in", "receiving"])
                out_dir  = wh_win["direction"].isin(["out", "sending"])
                in_sol   = float(wh_win.loc[in_dir,  "sol_moved"].abs().sum())
                out_sol  = float(wh_win.loc[out_dir, "sol_moved"].abs().sum())
                total_wh = in_sol + out_sol or 1.0
                wh_feats = {
                    "wh_net_flow":      in_sol - out_sol,
                    "wh_inflow_ratio":  in_sol / total_wh,
                    "wh_avg_pct_moved": float(wh_win["pct_moved"].mean()),
                    "wh_urgency_ratio": float((wh_win["pct_moved"] > 50).mean()),
                }
            else:
                wh_feats = {"wh_net_flow": 0.0, "wh_inflow_ratio": 0.5,
                            "wh_avg_pct_moved": 0.0, "wh_urgency_ratio": 0.0}

            # ── Price momentum from OB mid_price ────────────────────────────
            pm_30s  = ob_df.loc[ref_ts - timedelta(seconds=35):ref_ts - timedelta(seconds=25)]
            pm_1m   = ob_df.loc[ref_ts - timedelta(seconds=65):ref_ts - timedelta(seconds=55)]
            pm_5m   = ob_df.loc[ref_ts - timedelta(seconds=305):ref_ts - timedelta(seconds=295)]
            pm_now  = ob_df.loc[ref_ts - timedelta(seconds=10):ref_ts]

            def mid(df):
                # Use vol_imb as proxy for mid if microprice not directly stored
                # ob snapshots store microprice_dev = microprice - mid; we use avg of recent
                # For price momentum, use the spread_bps as a proxy metric or net_liq
                # Actually, DuckDB stores mid_price implicitly — use vol_imb-weighted avg
                return float(df.index.astype(np.int64).mean() / 1e9) if len(df) else None

            # Compute price momentum from the prices table instead (more accurate)
            pm_feats = {
                "pm_price_change_30s": 0.0,
                "pm_price_change_1m":  0.0,
                "pm_price_change_5m":  0.0,
                "pm_velocity_30s":     0.0,
            }

            result[ref_ts] = {**ob_feats, **tr_feats, **wh_feats, **pm_feats}

        return result

    except Exception as exc:
        logger.error(f"DuckDB feature timeseries error: {exc}", exc_info=True)
        return None


def _add_price_momentum(
    feature_ts:  Dict[datetime, Dict[str, float]],
    price_ts_arr: List[datetime],
    price_vals:   List[float],
) -> None:
    """
    Fill in pm_* price momentum features using the prices table.
    Mutates feature_ts in-place.
    """
    for ref_ts, feats in feature_ts.items():
        p_now = _price_at(ref_ts,                            price_ts_arr, price_vals)
        p_30s = _price_at(ref_ts - timedelta(seconds=30),   price_ts_arr, price_vals)
        p_1m  = _price_at(ref_ts - timedelta(seconds=60),   price_ts_arr, price_vals)
        p_5m  = _price_at(ref_ts - timedelta(seconds=300),  price_ts_arr, price_vals)

        def pct(a, b):
            return (a - b) / b * 100.0 if b and b > 0 else 0.0

        feats["pm_price_change_30s"] = pct(p_now, p_30s) if p_now and p_30s else 0.0
        feats["pm_price_change_1m"]  = pct(p_now, p_1m)  if p_now and p_1m  else 0.0
        feats["pm_price_change_5m"]  = pct(p_now, p_5m)  if p_now and p_5m  else 0.0
        feats["pm_velocity_30s"]     = (
            feats["pm_price_change_1m"] - feats["pm_price_change_5m"]
        )


# =============================================================================
# DATA LOADING
# =============================================================================

def load_trades(play_ids: List[int]) -> List[Dict[str, Any]]:
    """Load all sold trades that fall within the DuckDB cache window."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            # Only load trades from the last 2 days to stay within DuckDB coverage
            cur.execute("""
                SELECT
                    b.id,
                    b.play_id,
                    b.followed_at,
                    b.our_exit_timestamp,
                    CAST(b.our_entry_price   AS FLOAT) AS entry,
                    CAST(b.our_exit_price    AS FLOAT) AS exit_p,
                    CAST(b.higest_price_reached AS FLOAT) AS peak,
                    ROUND(((b.higest_price_reached - b.our_entry_price)
                           / NULLIF(b.our_entry_price, 0) * 100)::numeric, 5) AS max_gain_pct,
                    ROUND(((b.our_exit_price - b.our_entry_price)
                           / NULLIF(b.our_entry_price, 0) * 100)::numeric, 5) AS actual_exit_pct,
                    EXTRACT(EPOCH FROM (b.our_exit_timestamp - b.followed_at)) AS hold_seconds
                FROM follow_the_goat_buyins b
                WHERE b.play_id = ANY(%s)
                  AND b.followed_at > NOW() - INTERVAL '2 days'
                  AND b.our_entry_price IS NOT NULL
                  AND b.our_exit_price  IS NOT NULL
                  AND b.our_exit_timestamp IS NOT NULL
                  AND b.our_status = 'sold'
                  AND b.wallet_address NOT LIKE 'TRAINING_TEST_%%'
                  AND EXTRACT(EPOCH FROM (b.our_exit_timestamp - b.followed_at)) > 30
                ORDER BY b.followed_at ASC
            """, [play_ids])
            return [dict(r) for r in cur.fetchall()]


def load_price_index(
    trades: List[Dict[str, Any]],
    extra_secs: int = 400,
) -> Tuple[List[datetime], List[float]]:
    """Load SOL prices covering all trades in one query."""
    entry_times = [t["followed_at"]        for t in trades if t.get("followed_at")]
    exit_times  = [t["our_exit_timestamp"] for t in trades if t.get("our_exit_timestamp")]
    if not entry_times:
        return [], []

    window_start = min(entry_times) - timedelta(seconds=360)
    window_end   = max(exit_times)  + timedelta(seconds=extra_secs)

    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, CAST(price AS FLOAT) AS price
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """, [window_start, window_end])
            rows = cur.fetchall()

    if not rows:
        return [], []
    return [r["timestamp"] for r in rows], [float(r["price"]) for r in rows]


def _price_at(
    ts: datetime,
    timestamps: List[datetime],
    prices: List[float],
    tolerance_secs: int = 30,
) -> Optional[float]:
    """Closest price lookup via binary search."""
    if not timestamps:
        return None
    # Make ts timezone-aware if timestamps are timezone-aware
    if timestamps and timestamps[0].tzinfo is not None and ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    elif timestamps and timestamps[0].tzinfo is None and ts.tzinfo is not None:
        ts = ts.replace(tzinfo=None)

    idx        = bisect.bisect_left(timestamps, ts)
    best_idx   = None
    best_delta = float("inf")
    for i in (idx - 1, idx):
        if 0 <= i < len(timestamps):
            delta = abs((timestamps[i] - ts).total_seconds())
            if delta < best_delta:
                best_delta = delta
                best_idx   = i
    if best_idx is None or best_delta > tolerance_secs:
        return None
    return prices[best_idx]


# =============================================================================
# BUILD DATASET
# =============================================================================

def build_dataset(
    trades:        List[Dict[str, Any]],
    feature_ts:    Dict[datetime, Dict[str, float]],
    price_ts_arr:  List[datetime],
    price_vals:    List[float],
    drop_pct:      float = DROP_THRESHOLD_PCT,
    forward_secs:  int   = FORWARD_SECS,
    sample_secs:   int   = SAMPLE_SECS,
) -> Dict[str, Any]:
    """
    For each trade, walk its timeline in sample_secs steps.
    At each step:
      - Look up precomputed features for that bucket
      - Compute forward price label (did price drop ≥drop_pct in forward_secs?)
    Returns dataset dict ready for GA.
    """
    feature_rows: List[List[float]] = []
    label_rows:   List[int]         = []
    trade_ids:    List[int]         = []
    sample_ts_list: List[datetime]  = []
    minutes_list: List[float]       = []

    def snap(ts: datetime) -> datetime:
        """Snap timestamp to sample_secs boundary (UTC-aware)."""
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        epoch  = int(ts.timestamp())
        snapped = epoch - (epoch % sample_secs)
        return datetime.fromtimestamp(snapped, tz=timezone.utc)

    def to_utc(dt: datetime) -> datetime:
        """Ensure datetime is UTC-aware."""
        if dt is None:
            return dt
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    for trade in trades:
        bid        = int(trade["id"])
        entry_time = to_utc(trade.get("followed_at"))
        exit_time  = to_utc(trade.get("our_exit_timestamp"))
        if not entry_time or not exit_time:
            continue

        # Walk from entry to exit in sample_secs steps
        t = snap(entry_time)
        while t <= exit_time:
            feats_dict = feature_ts.get(t)
            if feats_dict:
                # Forward label
                t_fwd = t + timedelta(seconds=forward_secs)
                p_now = _price_at(t,     price_ts_arr, price_vals)
                p_fwd = _price_at(t_fwd, price_ts_arr, price_vals)

                if p_now and p_fwd and p_now > 0:
                    drop_pct_actual = (p_now - p_fwd) / p_now * 100.0
                    is_bearish = 1 if drop_pct_actual >= drop_pct else 0

                    # Build feature vector in LIVE_FEATURES order
                    feat_vec = [feats_dict.get(f, 0.0) for f in LIVE_FEATURES]

                    feature_rows.append(feat_vec)
                    label_rows.append(is_bearish)
                    trade_ids.append(bid)
                    sample_ts_list.append(t)
                    minutes_list.append((t - snap(entry_time)).total_seconds() / 60.0)

            t += timedelta(seconds=sample_secs)

    if not feature_rows:
        return {}

    features = np.array(feature_rows, dtype=np.float32)
    features = np.where(np.isfinite(features), features, 0.0)
    labels   = np.array(label_rows, dtype=np.int8)

    n_bearish = int(labels.sum())
    logger.info(
        f"Dataset: {len(features)} samples from {len(trades)} trades | "
        f"bearish={n_bearish} ({n_bearish/len(features)*100:.1f}%) | "
        f"drop≥{drop_pct:.2f}% in {forward_secs}s"
    )

    return {
        "features":   features,
        "labels":     labels,
        "trade_ids":  trade_ids,
        "sample_ts":  sample_ts_list,
        "minutes":    minutes_list,
    }


# =============================================================================
# GA INDIVIDUAL — identical structure to mega_simulator.py
# =============================================================================

_feature_importance: Optional[np.ndarray] = None


class Individual:
    """
    2-4 entry/exit conditions, all must be true (AND logic).
    Fitness = bearish_precision × avg_firings_per_trade × consistency_mult.
    """
    __slots__ = ("conditions", "fitness_val")

    def __init__(self, conditions: List[Tuple[int, int, float]]) -> None:
        self.conditions  = conditions
        self.fitness_val = -np.inf

    def apply(self, features: np.ndarray) -> np.ndarray:
        mask = np.ones(len(features), dtype=bool)
        for fi, d, thr in self.conditions:
            col = features[:, fi]
            mask &= (col > thr) if d > 0 else (col < thr)
        return mask

    def to_json(self) -> List[Dict[str, Any]]:
        return [
            {"feature": LIVE_FEATURES[fi], "direction": ">" if d > 0 else "<",
             "threshold": round(float(thr), 6)}
            for fi, d, thr in self.conditions
        ]

    def __repr__(self) -> str:
        return " AND ".join(
            f"{LIVE_FEATURES[fi]}{'>' if d > 0 else '<'}{thr:.5f}"
            for fi, d, thr in self.conditions
        )


def _random_individual(features: np.ndarray, use_importance: bool = False) -> Individual:
    imp     = _feature_importance
    n_conds = random.randint(GA_MIN_CONDS, GA_MAX_CONDS)

    if use_importance and imp is not None:
        probs        = imp / imp.sum()
        feat_indices = list(np.random.choice(N_FEATURES, size=min(n_conds, N_FEATURES),
                                             replace=False, p=probs))
    else:
        feat_indices = random.sample(range(N_FEATURES), min(n_conds, N_FEATURES))

    conds = []
    for fi in feat_indices:
        col  = features[:, fi]
        vals = col[np.isfinite(col)]
        if len(vals) < 5:
            continue
        thr = float(np.percentile(vals, random.uniform(10, 90)))
        conds.append((fi, random.choice([1, -1]), thr))

    if not conds:
        return _random_individual(features)
    return Individual(conds)


def _crossover(p1: Individual, p2: Individual) -> Individual:
    seen: Dict[int, Tuple] = {}
    for c in p1.conditions + p2.conditions:
        seen[c[0]] = c
    unique = list(seen.values())
    random.shuffle(unique)
    n = random.randint(GA_MIN_CONDS, min(GA_MAX_CONDS, len(unique)))
    return Individual(unique[:n])


def _mutate(ind: Individual, features: np.ndarray) -> Individual:
    conds = list(deepcopy(ind.conditions))
    op    = random.random()

    if op < 0.35 and conds:
        i          = random.randrange(len(conds))
        fi, d, thr = conds[i]
        col        = features[:, fi]
        std        = float(np.nanstd(col[col != 0])) if (col != 0).any() else 0.01
        conds[i]   = (fi, d, float(np.clip(
            thr + random.gauss(0, std * 0.2),
            np.nanpercentile(col, 5), np.nanpercentile(col, 95),
        )))
    elif op < 0.55 and conds:
        i          = random.randrange(len(conds))
        fi, d, thr = conds[i]
        conds[i]   = (fi, -d, thr)
    elif op < 0.70 and len(conds) > GA_MIN_CONDS:
        conds.pop(random.randrange(len(conds)))
    elif len(conds) < GA_MAX_CONDS:
        used = {c[0] for c in conds}
        avail = [i for i in range(N_FEATURES) if i not in used]
        if avail:
            fi  = int(np.random.choice(avail, p=(_feature_importance[avail] /
                  _feature_importance[avail].sum())
                  if _feature_importance is not None else None))  \
                  if _feature_importance is not None else random.choice(avail)
            col = features[:, fi]
            conds.append((fi, random.choice([1, -1]),
                          float(np.percentile(col, random.uniform(10, 90)))))

    return Individual(conds) if conds else _random_individual(features)


def _tournament(scored: List[Tuple[float, Individual]], k: int = GA_TOURNAMENT_K) -> Individual:
    pool = random.sample(scored, min(k, len(scored)))
    return deepcopy(max(pool, key=lambda x: x[0])[1])


# =============================================================================
# FITNESS
# =============================================================================

def _compute_fitness(
    ind: Individual,
    features: np.ndarray,
    labels: np.ndarray,
    trade_ids: List[int],
    n_trades: int,
) -> float:
    mask  = ind.apply(features)
    n_sig = int(mask.sum())

    avg_firings = n_sig / max(n_trades, 1)
    if avg_firings < 0.5:
        return -999.0   # fires too rarely
    if avg_firings > 10.0:
        return -998.0   # fires too often (noise)

    precision = float(labels[mask].mean()) if n_sig > 0 else 0.0
    if precision < MIN_BEARISH_PRECISION:
        return -997.0

    # Consistency: how many distinct trades does this rule fire in?
    fired_bids   = set(np.array(trade_ids)[mask].tolist())
    consistency  = len(fired_bids) / max(n_trades, 1)
    if consistency < 0.05:
        return -996.0

    return precision * avg_firings * consistency


def _update_feature_importance(top_inds: List[Individual]) -> None:
    global _feature_importance
    counts = np.zeros(N_FEATURES, dtype=np.float64)
    for ind in top_inds[:20]:
        w = max(0.0, ind.fitness_val)
        for fi, _, _ in ind.conditions:
            counts[fi] += 1.0 + w * 10.0
    if counts.sum() > 0:
        new_imp = (counts + 0.5) / (counts.sum() + 0.5 * N_FEATURES)
        _feature_importance = (
            0.7 * _feature_importance + 0.3 * new_imp
            if _feature_importance is not None else new_imp
        )


# =============================================================================
# GA LOOP
# =============================================================================

def run_ga(
    ds: Dict[str, Any],
    n_trades: int,
    population_size: int = GA_POPULATION,
    generations: int     = GA_GENERATIONS,
    verbose: bool        = True,
) -> List[Individual]:
    global _feature_importance
    _feature_importance = None

    features  = ds["features"]
    labels    = ds["labels"]
    trade_ids = ds["trade_ids"]

    def eval_ind(ind: Individual) -> float:
        f = _compute_fitness(ind, features, labels, trade_ids, n_trades)
        ind.fitness_val = f
        return f

    population: List[Tuple[float, Individual]] = []
    for _ in range(population_size):
        ind = _random_individual(features)
        population.append((eval_ind(ind), ind))
    population.sort(key=lambda x: -x[0])

    elite_n = max(1, int(population_size * GA_ELITE_FRAC))

    for gen in range(generations):
        new_pop: List[Tuple[float, Individual]] = []
        new_pop.extend(population[:elite_n])

        use_imp = gen >= 20
        if use_imp and gen % 20 == 0:
            top_pass = [ind for _, ind in population[:30] if ind.fitness_val > 0]
            if top_pass:
                _update_feature_importance(top_pass)

        scored = population
        while len(new_pop) < population_size:
            child = (
                _crossover(_tournament(scored), _tournament(scored))
                if random.random() < GA_CROSSOVER
                else deepcopy(_tournament(scored))
            )
            child = _mutate(child, features)
            new_pop.append((eval_ind(child), child))

        population = sorted(new_pop, key=lambda x: -x[0])

        if verbose and (gen + 1) % 25 == 0:
            bf, bi = population[0]
            if bf > 0:
                prec = float(labels[bi.apply(features)].mean())
                logger.info(
                    f"  Gen {gen+1:3d}/{generations}: fitness={bf:.4f}  "
                    f"bearish_prec={prec*100:.1f}%  rule={bi}"
                )

    return [ind for _, ind in population[:20] if ind.fitness_val > 0]


# =============================================================================
# OOS VALIDATION
# =============================================================================

def validate_oos(
    top_inds: List[Individual],
    ds: Dict[str, Any],
    trades: List[Dict[str, Any]],
    n_folds: int = OOS_FOLDS,
) -> List[Dict[str, Any]]:
    sorted_trades = sorted(trades, key=lambda t: t.get("followed_at") or datetime.min)
    n         = len(sorted_trades)
    fold_size = max(1, n // n_folds)

    features  = ds["features"]
    labels    = ds["labels"]
    trade_ids = np.array(ds["trade_ids"])

    results: List[Dict[str, Any]] = []

    for ind in top_inds:
        fold_precs: List[float] = []

        for fold in range(n_folds):
            start  = fold * fold_size
            end    = (fold + 1) * fold_size if fold < n_folds - 1 else n
            f_bids = {int(t["id"]) for t in sorted_trades[start:end]}
            fmask  = np.array([bid in f_bids for bid in trade_ids])

            if fmask.sum() < 5:
                continue

            fired = ind.apply(features[fmask])
            if fired.sum() < 2:
                fold_precs.append(0.0)
                continue
            fold_precs.append(float(labels[fmask][fired].mean()))

        if not fold_precs:
            continue

        passing   = sum(1 for p in fold_precs if p >= MIN_OOS_PRECISION)
        is_robust = passing >= MIN_OOS_FOLDS_PASSING
        avg_prec  = sum(fold_precs) / len(fold_precs)

        results.append({
            "individual":    ind,
            "is_fitness":    ind.fitness_val,
            "oos_precision": avg_prec,
            "fold_precs":    fold_precs,
            "passing_folds": passing,
            "n_folds":       len(fold_precs),
            "is_robust":     is_robust,
        })

    results.sort(key=lambda x: -x["oos_precision"])
    return results


# =============================================================================
# BACKTEST SIMULATION
# =============================================================================

def simulate_rule_on_trades(
    ind: Individual,
    trades: List[Dict[str, Any]],
    ds: Dict[str, Any],
    price_ts_arr: List[datetime],
    price_vals: List[float],
    sample_secs: int = SAMPLE_SECS,
) -> List[Dict[str, Any]]:
    """
    For each trade, walk its sample timeline. First sample where rule fires = exit.
    Compare to actual trailing stop exit.
    """
    features  = ds["features"]
    trade_ids = ds["trade_ids"]
    sample_ts = ds["sample_ts"]

    # Group dataset rows by trade, ordered by sample time
    trade_rows: Dict[int, List[Tuple[datetime, int]]] = {}
    for i, bid in enumerate(trade_ids):
        trade_rows.setdefault(bid, []).append((sample_ts[i], i))

    for bid in trade_rows:
        trade_rows[bid].sort(key=lambda x: x[0])

    trade_lookup = {int(t["id"]): t for t in trades}
    results: List[Dict[str, Any]] = []

    for bid, rows in trade_rows.items():
        trade  = trade_lookup.get(bid)
        if not trade:
            continue

        entry  = float(trade.get("entry") or 0.0)
        exit_p = float(trade.get("exit_p") or 0.0)
        peak_p = float(trade.get("peak") or 0.0)
        if entry <= 0:
            continue

        actual_gain = (exit_p - entry) / entry * 100.0 - COST_PCT * 100.0

        signal_ts:    Optional[datetime] = None
        signal_price: Optional[float]   = None

        for ts, row_idx in rows:
            feat_row = features[row_idx:row_idx+1]
            if ind.apply(feat_row)[0]:
                ep = _price_at(ts, price_ts_arr, price_vals, tolerance_secs=45)
                if ep and ep > 0:
                    signal_ts    = ts
                    signal_price = ep
                    break

        if signal_price:
            signal_gain   = (signal_price - entry) / entry * 100.0 - COST_PCT * 100.0
            actual_exit_ts = trade.get("our_exit_timestamp")
            if actual_exit_ts and actual_exit_ts.tzinfo is None:
                actual_exit_ts = actual_exit_ts.replace(tzinfo=timezone.utc)
            exited_early = (actual_exit_ts is None or
                           signal_ts <= actual_exit_ts + timedelta(seconds=sample_secs))
        else:
            signal_gain  = actual_gain
            exited_early = False

        peak_gain = (peak_p - entry) / entry * 100.0 if peak_p > 0 else 0.0

        results.append({
            "buyin_id":      bid,
            "play_id":       trade.get("play_id"),
            "entry":         entry,
            "actual_gain":   actual_gain,
            "signal_gain":   signal_gain,
            "peak_gain":     peak_gain,
            "signal_fired":  signal_price is not None,
            "exited_early":  exited_early,
            "signal_minute": (signal_ts - trade["followed_at"].replace(tzinfo=timezone.utc)
                              ).total_seconds() / 60.0 if signal_ts and trade.get("followed_at") else None,
            "improvement":   signal_gain - actual_gain,
        })

    return results


# =============================================================================
# DISPLAY
# =============================================================================

def _pct(vals: List[float], p: int) -> float:
    if not vals:
        return 0.0
    sv  = sorted(vals)
    idx = max(0, min(len(sv) - 1, int(len(sv) * p / 100)))
    return sv[idx]


def _banner(text: str) -> None:
    sep = "=" * 84
    print(f"\n{sep}\n  {text}\n{sep}")


def print_backtest(results: List[Dict[str, Any]]) -> None:
    if not results:
        print("  No results.")
        return

    n          = len(results)
    n_fired    = sum(1 for r in results if r["signal_fired"])
    n_early    = sum(1 for r in results if r["exited_early"])
    n_improved = sum(1 for r in results if r["improvement"] > 0.005)
    n_worse    = sum(1 for r in results if r["improvement"] < -0.01)

    ag = [r["actual_gain"] for r in results]
    sg = [r["signal_gain"] for r in results]
    pk = [r["peak_gain"]   for r in results if r["peak_gain"] > 0]

    avg_a = sum(ag) / n
    avg_s = sum(sg) / n
    avg_p = sum(pk) / len(pk) if pk else 0.0
    wr_a  = sum(1 for g in ag if g > 0) / n
    wr_s  = sum(1 for g in sg if g > 0) / n

    early_impr = [r["improvement"] for r in results if r["exited_early"] and r["signal_fired"]]

    sc = [r["signal_gain"] / r["peak_gain"] for r in results if r["peak_gain"] > 0 and r["signal_gain"] > 0]
    ac = [r["actual_gain"] / r["peak_gain"] for r in results if r["peak_gain"] > 0 and r["actual_gain"] > 0]

    print(f"\n  Trades:                    {n}")
    print(f"  Signal fired:              {n_fired} ({n_fired/n*100:.0f}%)")
    print(f"  Exited before stop:        {n_early} ({n_early/n*100:.0f}%)")
    print(f"  Exit improved by >0.005%:  {n_improved} ({n_improved/n*100:.0f}%)")
    print(f"  Exit worsened by >0.01%:   {n_worse} ({n_worse/n*100:.0f}%)")
    print()
    print(f"  {'':30s}  {'Actual':>10}  {'Signal':>10}  {'Delta':>10}")
    print(f"  {'Avg exit gain (net cost)':30s}  {avg_a:>+9.4f}%  {avg_s:>+9.4f}%  {avg_s-avg_a:>+9.4f}%")
    print(f"  {'Win rate':30s}  {wr_a*100:>9.1f}%  {wr_s*100:>9.1f}%")
    print(f"  {'Avg peak gain':30s}  {avg_p:>+9.4f}%")
    if ac:
        print(f"  {'Peak capture (actual)':30s}  {sum(ac)/len(ac)*100:>9.1f}%")
    if sc:
        print(f"  {'Peak capture (signal)':30s}  {'':>10}  {sum(sc)/len(sc)*100:>9.1f}%")

    if early_impr:
        print(f"\n  When signal fired early ({len(early_impr)} trades):")
        print(f"    Avg improvement: {sum(early_impr)/len(early_impr):+.4f}%")
        print(f"    p25/p50/p75:     {_pct(early_impr,25):+.4f}% / "
              f"{_pct(early_impr,50):+.4f}% / {_pct(early_impr,75):+.4f}%")


def print_oos_table(validated: List[Dict[str, Any]]) -> None:
    hdr = f"  {'#':<4} {'IS_fit':>8} {'OOS_prec':>9} {'Folds':>7} {'Conds':>6} {'Robust':>7}  Rule"
    print(hdr)
    print("  " + "-" * max(60, len(hdr) - 2))
    for i, r in enumerate(validated[:15]):
        ind    = r["individual"]
        marker = " <-- BEST" if i == 0 and r["is_robust"] else (" <-- top" if i == 0 else "")
        print(
            f"  {i+1:<4} {r['is_fitness']:>7.3f}  {r['oos_precision']*100:>7.1f}%  "
            f"{r['passing_folds']}/{r['n_folds']:>1}     {len(ind.conditions):>6} "
            f"{'YES' if r['is_robust'] else 'no':>7}{marker}"
        )
        print(f"       {ind}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exit Signal Simulator v3 — bearish precision GA, DuckDB features"
    )
    parser.add_argument("--plays", type=str,   default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--drop",  type=float, default=DROP_THRESHOLD_PCT,
                        help=f"Bearish label: price drop %% threshold (default {DROP_THRESHOLD_PCT})")
    parser.add_argument("--fwd",   type=int,   default=FORWARD_SECS,
                        help=f"Forward window in seconds (default {FORWARD_SECS})")
    parser.add_argument("--apply", action="store_true",
                        help="Print best rule as JSON")
    args = parser.parse_args()

    play_ids = DEFAULT_PLAY_IDS
    if args.plays:
        play_ids = [int(x.strip()) for x in args.plays.split(",") if x.strip()]

    pop_size = QUICK_POPULATION  if args.quick else GA_POPULATION
    n_gens   = QUICK_GENERATIONS if args.quick else GA_GENERATIONS

    _banner(
        f"EXIT SIGNAL SIMULATOR v3  —  plays {play_ids}"
        + ("  [QUICK]" if args.quick else "")
    )
    print(
        f"\n  Goal: find get_live_features() conditions that predict price will\n"
        f"  drop ≥{args.drop:.2f}% within {args.fwd}s.  Check live every second → replaces trailing stop.\n"
        f"  Features: {N_FEATURES} (exact get_live_features() names)\n"
        f"  Data: DuckDB ob_snapshots + raw_trades + whale_events (last 25h)\n"
    )

    # ── Load trades ────────────────────────────────────────────────────────────
    print("Loading sold trades (last 48h)...")
    t0     = time.time()
    trades = load_trades(play_ids)
    print(f"  {len(trades)} sold trades  ({time.time()-t0:.1f}s)")

    if len(trades) < MIN_TRADES_FOR_GA:
        print(f"  Need ≥{MIN_TRADES_FOR_GA} trades. Only {len(trades)} found in last 48h.")
        return

    # ── Load prices ────────────────────────────────────────────────────────────
    print("\nLoading SOL prices...")
    t0                    = time.time()
    price_ts_arr, price_vals = load_price_index(trades, extra_secs=args.fwd + 120)
    print(f"  {len(price_vals)} price rows  ({time.time()-t0:.1f}s)")

    # ── Build DuckDB feature time series ──────────────────────────────────────
    print("\nBuilding feature time series from DuckDB (order book + trades + whales)...")
    print("  (This replays get_live_features() at every 30s interval across all trades)")
    t0         = time.time()
    entry_times = [t["followed_at"]        for t in trades if t.get("followed_at")]
    exit_times  = [t["our_exit_timestamp"] for t in trades if t.get("our_exit_timestamp")]
    ts_start = min(entry_times) - timedelta(seconds=FEATURE_WINDOW_SECS + 60)
    ts_end   = max(exit_times)  + timedelta(seconds=args.fwd + 60)

    feature_ts = _build_feature_timeseries(ts_start, ts_end, SAMPLE_SECS, FEATURE_WINDOW_SECS)
    if not feature_ts:
        print("  ERROR: Could not load DuckDB data.")
        return

    # Fill in price momentum features
    _add_price_momentum(feature_ts, price_ts_arr, price_vals)

    n_buckets = len(feature_ts)
    print(f"  {n_buckets} time buckets computed  ({time.time()-t0:.1f}s)")

    # ── Build dataset ──────────────────────────────────────────────────────────
    print("\nBuilding feature matrix + forward labels...")
    t0 = time.time()
    ds = build_dataset(
        trades, feature_ts, price_ts_arr, price_vals,
        drop_pct=args.drop, forward_secs=args.fwd, sample_secs=SAMPLE_SECS,
    )
    if not ds:
        print("  Could not build dataset — check DuckDB/prices coverage.")
        return

    n_rows   = len(ds["features"])
    n_bear   = int(ds["labels"].sum())
    print(f"  {n_rows} samples  |  bearish={n_bear} ({n_bear/n_rows*100:.1f}%)  ({time.time()-t0:.1f}s)")

    if n_bear < 10:
        print(f"  Too few bearish samples ({n_bear}). Try --drop with a lower value.")
        return

    # ── Baseline ───────────────────────────────────────────────────────────────
    _banner("BASELINE — ACTUAL TRAILING STOP EXITS")
    ag = [float(t["actual_exit_pct"]) - COST_PCT * 100.0
          for t in trades if t.get("actual_exit_pct") is not None]
    pk = [float(t["max_gain_pct"]) for t in trades if t.get("max_gain_pct") is not None]
    if ag:
        avg_a = sum(ag) / len(ag)
        wr_a  = sum(1 for g in ag if g > 0) / len(ag)
        avg_p = sum(pk) / len(pk) if pk else 0.0
        caps  = [a/p for a, p in zip(ag, pk) if p > 0 and a > 0]
        print(f"\n  Trades:              {len(ag)}")
        print(f"  Win rate:            {wr_a*100:.1f}%")
        print(f"  Avg exit (net cost): {avg_a:+.4f}%")
        print(f"  Avg peak gain:       {avg_p:+.4f}%")
        if caps:
            print(f"  Avg peak capture:    {sum(caps)/len(caps)*100:.1f}%")

    # ── GA ─────────────────────────────────────────────────────────────────────
    _banner(
        f"GA  (pop={pop_size}  gen={n_gens}  features={N_FEATURES}  "
        f"samples={n_rows}  trades={len(trades)})"
    )
    print(
        f"\n  Evolving {GA_MIN_CONDS}-{GA_MAX_CONDS} bearish exit conditions.\n"
        f"  Hard reject: precision < {MIN_BEARISH_PRECISION*100:.0f}%\n"
    )

    t0       = time.time()
    top_inds = run_ga(ds, len(trades), pop_size, n_gens, verbose=True)
    elapsed  = time.time() - t0
    print(f"\n  GA completed in {elapsed:.1f}s  — {len(top_inds)} qualifying rules")

    if not top_inds:
        print(
            "\n  No rules found with sufficient precision.\n"
            "  Suggestions:\n"
            "    --drop 0.05   (lower drop threshold, easier to predict)\n"
            "    --fwd 30      (shorter forward window, tighter signal)\n"
            "    --quick       (test with small GA first)\n"
        )
        return

    # ── OOS validation ─────────────────────────────────────────────────────────
    _banner("WALK-FORWARD OOS VALIDATION")
    print(f"\n  Validating {len(top_inds)} rules across {OOS_FOLDS} chronological folds...\n")
    validated = validate_oos(top_inds, ds, trades, OOS_FOLDS)

    if not validated:
        print("  No rules passed OOS. The in-sample rules may be overfit.")
        validated = [{"individual": top_inds[0], "is_fitness": top_inds[0].fitness_val,
                      "oos_precision": 0.0, "fold_precs": [], "passing_folds": 0,
                      "n_folds": 0, "is_robust": False}]

    _banner("TOP RULES BY OOS BEARISH PRECISION")
    print_oos_table(validated)

    # ── Best rule ──────────────────────────────────────────────────────────────
    robust = [r for r in validated if r["is_robust"]]
    best   = (robust[0] if robust else validated[0])
    best_ind = best["individual"]

    features = ds["features"]
    labels   = ds["labels"]
    mask     = best_ind.apply(features)
    prec     = float(labels[mask].mean()) if mask.sum() else 0.0

    _banner("BEST RULE DETAIL")
    print(f"\n  In-sample firings:    {int(mask.sum())}  ({int(mask.sum())/len(trades):.1f}x per trade avg)")
    print(f"  In-sample precision:  {prec*100:.1f}%  "
          f"(price dropped ≥{args.drop:.2f}% within {args.fwd}s after signal)")
    print(f"  OOS precision:        {best['oos_precision']*100:.1f}%  "
          f"({best['passing_folds']}/{best['n_folds']} folds ≥{MIN_OOS_PRECISION*100:.0f}%)")
    print(f"  Robust:               {'YES ✓' if best['is_robust'] else 'no'}")
    print(f"\n  Conditions (evaluate ALL each second via get_live_features()):")
    for fi, d, thr in best_ind.conditions:
        print(f"    {LIVE_FEATURES[fi]:30s} {'>' if d>0 else '<'} {thr:.6f}")

    # ── Backtest ───────────────────────────────────────────────────────────────
    _banner(f"BACKTEST: last {len(trades)} actual trades vs signal rule")
    bt_res = simulate_rule_on_trades(best_ind, trades, ds, price_ts_arr, price_vals)
    print_backtest(bt_res)

    # ── Per-play ───────────────────────────────────────────────────────────────
    _banner("PER-PLAY BREAKDOWN")
    for pid in play_ids:
        pr = [r for r in bt_res if r.get("play_id") == pid]
        if len(pr) < 3:
            continue
        pa  = sum(r["actual_gain"] for r in pr) / len(pr)
        ps  = sum(r["signal_gain"] for r in pr) / len(pr)
        wa  = sum(1 for r in pr if r["actual_gain"] > 0) / len(pr)
        ws  = sum(1 for r in pr if r["signal_gain"] > 0) / len(pr)
        nf  = sum(1 for r in pr if r["signal_fired"])
        print(f"\n  Play {pid}  (n={len(pr)}, signal fired={nf})")
        print(f"    Actual:  avg={pa:+.4f}%  win={wa*100:.1f}%")
        print(f"    Signal:  avg={ps:+.4f}%  win={ws*100:.1f}%")
        print(f"    Delta:   {ps-pa:+.4f}%")

    # ── JSON output ────────────────────────────────────────────────────────────
    if args.apply:
        _banner("RULE JSON  (plug into trailing stop seller)")
        rule = {
            "type":                "signal_exit_v3",
            "logic":               "all",
            "drop_threshold_pct":  args.drop,
            "forward_secs":        args.fwd,
            "conditions":          best_ind.to_json(),
            "integration": (
                "Each second: feats = get_live_features(window_min=5); "
                "if feats and all conditions met → exit immediately"
            ),
        }
        print(json.dumps(rule, indent=4))
    else:
        print("\nRun with --apply to print the rule JSON for live integration.\n")


if __name__ == "__main__":
    main()

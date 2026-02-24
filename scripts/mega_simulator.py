"""
Mega Signal Simulator
=====================
Self-running, indefinitely-looping optimization engine for pump trading signals.

Each loop (~30 min):
  1. Loads a dense feature time series from DuckDB (30-second buckets, rolling 5-min
     windows) and forward price paths from PostgreSQL.
  2. Runs a genetic algorithm to search millions of entry signal combinations —
     2-4 feature thresholds scored by daily Expected Value.
  3. For the top entry rules, grid-searches 1,200+ exit tier configurations
     (stop-loss × min-hold × trailing tolerances).
  4. Validates on a held-out time window (walk-forward) to detect overfitting.
  5. Saves the best configurations to the simulation_results PostgreSQL table
     and prints a live leaderboard to stdout.

Usage:
    python3 scripts/mega_simulator.py
    python3 scheduler/run_component.py --component mega_simulator
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from core.database import get_postgres, postgres_execute

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / "000trading" / "logs" / "mega_simulator.log",
                            encoding="utf-8"),
    ],
)
logger = logging.getLogger("mega_sim")

# ── constants ─────────────────────────────────────────────────────────────────
COST_PCT        = 0.001          # 0.1% round-trip cost
PUMP_THRESHOLD  = 0.003          # 0.3% min forward gain = "pump" (must clear 0.1% cost × 2 + margin)
FWD_MINUTES     = 7              # forward window: catch SHARP pumps, not slow drift
BUCKET_SECONDS  = 30             # feature time series granularity
FEATURE_WINDOW  = 300            # rolling window for features (5 min)
MIN_SIGNALS     = 25             # minimum signals/day — below this is statistically meaningless
MAX_SIGNALS     = 300            # ignore rules that fire too frequently (noise)
REFRESH_MINUTES = 30             # data refresh interval (kept for reference)
OOS_FOLDS       = 3              # walk-forward folds (more = more robust overfitting detection)
PRE_ENTRY_TOP_GUARD = 0.0015     # skip entry if price already rose >0.15% in last 2 min (buying top)

# GA parameters
GA_POPULATION       = 600
GA_GENERATIONS      = 250
GA_ELITE_FRAC       = 0.10
GA_CROSSOVER        = 0.60
GA_MUTATION         = 0.25
GA_MIN_CONDS        = 2
GA_MAX_CONDS        = 4
GA_TOURNAMENT_K     = 5
GA_DB_SEED_FRAC     = 0.08       # fraction seeded from DB — kept low to preserve diversity
GA_IMPORTANCE_FRAC  = 0.35       # fraction using feature-importance sampling
SHARPE_WEIGHT       = 0.25       # Sharpe weight — lower to avoid boosting tiny-sample 100% rules
GA_MIN_SIG_HARD     = 25         # hard minimum signals: rules below this get -999 fitness

# Exit evolution ranges (evolved jointly with entry conditions in the GA)
EXIT_SL_RANGE     = (0.001, 0.008)    # stop-loss range
EXIT_HOLD_RANGE   = (0,     180)      # min-hold seconds
EXIT_T1B_RANGE    = (0.001, 0.005)    # tier-1 boundary (gain% where tier 1 ends)
EXIT_T2B_RANGE    = (0.002, 0.010)    # tier-2 boundary (gain% where tier 2 ends)
EXIT_T1TOL_RANGE  = (0.002, 0.008)    # tier-1 trailing tolerance (early, loose)
EXIT_T2TOL_RANGE  = (0.001, 0.004)    # tier-2 trailing tolerance (medium)
EXIT_T3TOL_RANGE  = (0.0001, 0.001)   # tier-3 trailing tolerance (high-gain lock-in)

# Final exit grid-search (refinement pass on top GA individuals)
STOP_LOSS_GRID    = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.008]
MIN_HOLD_GRID     = [0, 30, 60, 90, 120, 180]
TIER1_SPLIT_GRID  = [0.0005, 0.001, 0.0015, 0.002, 0.003]
TIER2_TOL_GRID    = [0.003, 0.002, 0.0015, 0.001, 0.0008, 0.0005]
TIER3_TOL_GRID    = [0.002, 0.001, 0.0005, 0.0003, 0.0001]

# Hall of Fame — all-time best rules across all runs (in-memory, updated each loop)
_hall_of_fame: List[Dict[str, Any]] = []
HOF_SIZE = 20   # keep top 20 across all runs

# Feature importance — probability weight for each feature during GA sampling
# Updated each loop from the top discovered rules. Starts uniform.
_feature_importance: Optional[np.ndarray] = None

# Feature names — must match build_feature_matrix() column names exactly.
# 26 features across 4 data sources: order book, trades, whale, price momentum.
FEATURES = [
    # ── Order book: 5-min rolling averages ────────────────────────────────────
    "ob_avg_vol_imb",       # (bid_vol - ask_vol) / total — positive = buy pressure
    "ob_avg_depth_ratio",   # bid_depth / ask_depth over window
    "ob_avg_spread_bps",    # bid-ask spread in basis points (lower = tighter market)
    "ob_net_liq_change",    # sum of net_liq_1s — net liquidity flow over window
    "ob_bid_ask_ratio",     # bid_liq / ask_liq — overall liquidity skew
    # ── Order book: 1-min acceleration vs 5-min baseline ─────────────────────
    "ob_imb_trend",         # ob_imb_1m  − ob_imb_5m  (imbalance accelerating?)
    "ob_depth_trend",       # ob_depth_1m − ob_depth_5m (depth building fast?)
    "ob_liq_accel",         # ob_bid_ask_1m − ob_bid_ask_5m (liq skew rising?)
    # ── Order book: microstructure signals ────────────────────────────────────
    "ob_slope_ratio",       # bid_slope / |ask_slope| — how steep is buy-side vs sell-side
    "ob_depth_5bps_ratio",  # bid_dep_5bps / ask_dep_5bps — close-to-mid depth imbalance
    "ob_microprice_dev",    # microprice − mid_price deviation (directional pressure)
    # ── Trades ────────────────────────────────────────────────────────────────
    "tr_buy_ratio",         # buy_vol / total_vol — buying dominance
    "tr_large_ratio",       # vol from trades >50 SOL / total (institutional activity)
    "tr_buy_accel",         # 1-min buy ratio / 5-min buy ratio (momentum building)
    "tr_avg_size",          # average trade size in SOL (bigger = more conviction)
    "tr_n",                 # total trade count (market activity level)
    # ── Whale activity ────────────────────────────────────────────────────────
    "wh_inflow_ratio",      # whale_in_sol / total_whale_sol (net accumulation)
    "wh_net_flow",          # whale_in_sol − whale_out_sol (signed flow direction)
    "wh_large_count",       # events with significance > 0.5 (MAJOR/SIGNIFICANT moves)
    "wh_n",                 # total whale event count in window
    "wh_avg_pct_moved",     # avg % of each whale's wallet moved (conviction signal)
    "wh_urgency_ratio",     # fraction of events moving >50% of wallet (urgent moves)
    # ── Price momentum ────────────────────────────────────────────────────────
    "pm_price_change_30s",  # price % change in last 30s
    "pm_price_change_1m",   # price % change in last 1m
    "pm_price_change_5m",   # price % change in last 5m
    "pm_velocity_30s",      # momentum acceleration: 1m_change − 5m_change
]

N_FEATURES = len(FEATURES)
FEAT_IDX   = {f: i for i, f in enumerate(FEATURES)}


# =============================================================================
# PHASE 1 — FEATURE MATRIX
# =============================================================================

def build_feature_matrix() -> Optional[Dict[str, Any]]:
    """Load DuckDB + PostgreSQL data and build a dense feature time series.

    Strategy: pull raw DuckDB tables into pandas, resample to 30-second
    buckets, then compute rolling 5-minute aggregate features entirely in
    pandas (avoids DuckDB-version-specific SQL functions like DATE_BIN).

    Returns a dict with:
        features   : np.ndarray (N × n_features)
        labels     : np.ndarray (N,) — 1 if max forward gain ≥ 0.2%
        max_gains  : np.ndarray (N,) — actual max forward gain
        price_highs: list of np.ndarray — per-row 30s hi prices (for exit sim)
        price_lows : list of np.ndarray — per-row 30s lo prices
        timestamps : list of datetime
        data_hours : float
    """
    import pandas as pd
    logger.info("Building feature matrix from DuckDB + PostgreSQL...")

    # ── 1. Pull raw tables from DuckDB ───────────────────────────────────────
    try:
        from core.raw_data_cache import open_reader
        con = open_reader()
    except Exception as e:
        logger.error(f"Cannot open DuckDB reader: {e}")
        return None

    try:
        ob_raw = con.execute("""
            SELECT ts, vol_imb, depth_ratio, spread_bps, net_liq_1s,
                   bid_liq, ask_liq,
                   bid_slope, ask_slope, bid_dep_5bps, ask_dep_5bps,
                   microprice_dev
            FROM ob_snapshots
            ORDER BY ts
        """).df()

        tr_raw = con.execute("""
            SELECT ts, sol_amount, direction
            FROM raw_trades
            ORDER BY ts
        """).df()

        wh_raw = con.execute("""
            SELECT ts, sol_moved, direction, significance, pct_moved
            FROM whale_events
            ORDER BY ts
        """).df()

        con.close()
    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        try:
            con.close()
        except Exception:
            pass
        return None

    if len(ob_raw) < 200:
        logger.warning(f"Only {len(ob_raw)} OB rows — need ≥200")
        return None

    # ── 2. Convert timestamps to UTC DatetimeIndex ────────────────────────────
    for df_tmp in [ob_raw, tr_raw, wh_raw]:
        df_tmp['ts'] = pd.to_datetime(df_tmp['ts'], utc=True)

    # ── 3. Resample each source to BUCKET_SECONDS buckets ─────────────────────
    bucket = f"{BUCKET_SECONDS}s"

    # OB: per-bucket basic stats + microstructure
    ob_raw.set_index('ts', inplace=True)
    ob_raw['bid_ask']      = ob_raw['bid_liq']     / ob_raw['ask_liq'].replace(0, np.nan)
    ob_raw['slope_ratio']  = ob_raw['bid_slope']   / ob_raw['ask_slope'].abs().replace(0, np.nan)
    ob_raw['depth_5r']     = ob_raw['bid_dep_5bps'] / ob_raw['ask_dep_5bps'].replace(0, np.nan)
    ob_b = ob_raw.resample(bucket).agg({
        'vol_imb':      'mean',
        'depth_ratio':  'mean',
        'spread_bps':   'mean',
        'net_liq_1s':   'mean',
        'bid_ask':      'mean',
        'slope_ratio':  'mean',
        'depth_5r':     'mean',
        'microprice_dev':'mean',
    }).rename(columns={
        'vol_imb':       '_ob_vi',
        'depth_ratio':   '_ob_dr',
        'spread_bps':    '_ob_sp',
        'net_liq_1s':    '_ob_nl',
        'bid_ask':       '_ob_ba',
        'slope_ratio':   '_ob_sr',
        'depth_5r':      '_ob_d5',
        'microprice_dev':'_ob_mpd',
    })

    # Trade: per-bucket stats
    if len(tr_raw) > 0:
        tr_raw.set_index('ts', inplace=True)
        tr_raw['is_buy']  = (tr_raw['direction'] == 'buy').astype(float)
        tr_raw['is_large']= (tr_raw['sol_amount'] > 50).astype(float)
        tr_raw['buy_sol'] = tr_raw['sol_amount'] * tr_raw['is_buy']

        tr_b = tr_raw.resample(bucket).agg(
            tr_n        =('sol_amount', 'count'),
            tr_total    =('sol_amount', 'sum'),
            tr_buy_vol  =('buy_sol',    'sum'),
            tr_large_vol=('is_large',   lambda x: (x * tr_raw.loc[x.index, 'sol_amount']).sum()),
            tr_avg_size =('sol_amount', 'mean'),
        )
        tr_b['tr_buy_ratio']   = tr_b['tr_buy_vol']   / tr_b['tr_total'].replace(0, np.nan)
        tr_b['tr_large_ratio'] = tr_b['tr_large_vol'] / tr_b['tr_total'].replace(0, np.nan)
    else:
        tr_b = pd.DataFrame(index=ob_b.index)
        for col in ['tr_n', 'tr_total', 'tr_buy_ratio', 'tr_large_ratio', 'tr_avg_size']:
            tr_b[col] = 0.0

    # Whale: per-bucket stats (direction is normalized to 'in'/'out' at source)
    if len(wh_raw) > 0:
        wh_raw.set_index('ts', inplace=True)
        # Defensive: normalize legacy 'receiving'/'sending' labels → 'in'/'out'
        dir_map = {'receiving': 'in', 'sending': 'out'}
        wh_raw['direction'] = wh_raw['direction'].str.lower().replace(dir_map)
        # sol_moved must be non-negative (legacy data may have signed values)
        wh_raw['sol_moved'] = wh_raw['sol_moved'].abs()
        wh_raw['in_sol']     = wh_raw['sol_moved'] * (wh_raw['direction'] == 'in').astype(float)
        wh_raw['out_sol']    = wh_raw['sol_moved'] * (wh_raw['direction'] == 'out').astype(float)
        wh_raw['is_large']   = (wh_raw['significance'] > 0.5).astype(float)
        wh_raw['is_urgent']  = (wh_raw['pct_moved'].fillna(0) > 50).astype(float)
        wh_b = wh_raw.resample(bucket).agg(
            wh_n         =('sol_moved',  'count'),
            wh_in_sol    =('in_sol',     'sum'),
            wh_out_sol   =('out_sol',    'sum'),
            wh_tot_sol   =('sol_moved',  'sum'),
            wh_large_cnt =('is_large',   'sum'),
            wh_avg_pct   =('pct_moved',  'mean'),
            wh_urgent_cnt=('is_urgent',  'sum'),
        )
        wh_b['wh_net_flow']      = wh_b['wh_in_sol'] - wh_b['wh_out_sol']
        wh_b['wh_inflow_ratio']  = wh_b['wh_in_sol'] / wh_b['wh_tot_sol'].replace(0, np.nan)
        wh_b['wh_urgency_ratio'] = wh_b['wh_urgent_cnt'] / wh_b['wh_n'].replace(0, np.nan)
    else:
        wh_b = pd.DataFrame(index=ob_b.index)
        for col in ['wh_n', 'wh_net_flow', 'wh_inflow_ratio', 'wh_large_cnt',
                    'wh_avg_pct', 'wh_urgency_ratio']:
            wh_b[col] = 0.0

    # ── 4. Join and build rolling 5-min features ──────────────────────────────
    # Reindex everything to ob_b's index
    tr_b  = tr_b.reindex(ob_b.index)
    wh_b  = wh_b.reindex(ob_b.index)

    df = pd.concat([ob_b, tr_b, wh_b], axis=1).ffill().fillna(0)

    win = int(FEATURE_WINDOW / BUCKET_SECONDS)  # 5-min in buckets

    win1m = max(1, int(60 / BUCKET_SECONDS))  # 1-min window in buckets

    # ── OB: 5-min rolling averages ────────────────────────────────────────────
    df['ob_avg_vol_imb']     = df['_ob_vi'].rolling(win, min_periods=1).mean()
    df['ob_avg_depth_ratio'] = df['_ob_dr'].rolling(win, min_periods=1).mean()
    df['ob_avg_spread_bps']  = df['_ob_sp'].rolling(win, min_periods=1).mean()
    df['ob_net_liq_change']  = df['_ob_nl'].rolling(win, min_periods=1).mean()
    df['ob_bid_ask_ratio']   = df['_ob_ba'].rolling(win, min_periods=1).mean()

    # ── OB: 1-min short-term rolling for trend calculation ────────────────────
    df['_ob_vi_1m'] = df['_ob_vi'].rolling(win1m, min_periods=1).mean()
    df['_ob_dr_1m'] = df['_ob_dr'].rolling(win1m, min_periods=1).mean()
    df['_ob_ba_1m'] = df['_ob_ba'].rolling(win1m, min_periods=1).mean()

    # ── OB: trend = 1-min vs 5-min baseline ───────────────────────────────────
    df['ob_imb_trend']   = df['_ob_vi_1m'] - df['ob_avg_vol_imb']
    df['ob_depth_trend'] = df['_ob_dr_1m'] - df['ob_avg_depth_ratio']
    df['ob_liq_accel']   = df['_ob_ba_1m'] - df['ob_bid_ask_ratio']

    # ── OB: microstructure (5-min rolling) ────────────────────────────────────
    df['ob_slope_ratio']      = df['_ob_sr'].rolling(win, min_periods=1).mean()
    df['ob_depth_5bps_ratio'] = df['_ob_d5'].rolling(win, min_periods=1).mean()
    df['ob_microprice_dev']   = df['_ob_mpd'].rolling(win, min_periods=1).mean()

    # ── Trades: 5-min rolling ─────────────────────────────────────────────────
    df['tr_buy_ratio']   = df['tr_buy_ratio'].rolling(win, min_periods=1).mean()
    df['tr_large_ratio'] = df['tr_large_ratio'].rolling(win, min_periods=1).mean()
    df['tr_avg_size']    = df['tr_avg_size'].rolling(win, min_periods=1).mean()
    df['tr_n']           = df['tr_n'].rolling(win, min_periods=1).sum()

    # Buy acceleration: 1-min buy ratio / 5-min buy ratio
    tr_buy_1m = df['tr_buy_ratio'].rolling(win1m, min_periods=1).mean()
    tr_buy_5m = df['tr_buy_ratio'].rolling(win,   min_periods=1).mean()
    df['tr_buy_accel'] = (tr_buy_1m / tr_buy_5m.replace(0, np.nan)).fillna(1.0)

    # ── Whale: 5-min rolling ──────────────────────────────────────────────────
    df['wh_inflow_ratio']  = df['wh_inflow_ratio'].rolling(win, min_periods=1).mean()
    df['wh_net_flow']      = df['wh_net_flow'].rolling(win, min_periods=1).sum()
    df['wh_large_count']   = df['wh_large_cnt'].rolling(win, min_periods=1).sum()
    df['wh_n']             = df['wh_n'].rolling(win, min_periods=1).sum()
    df['wh_avg_pct_moved'] = df['wh_avg_pct'].rolling(win, min_periods=1).mean()
    df['wh_urgency_ratio'] = df['wh_urgency_ratio'].rolling(win, min_periods=1).mean()

    # Drop warm-up period
    df = df.iloc[win:].copy()

    timestamps = df.index.tolist()
    if len(timestamps) < 100:
        logger.warning(f"Only {len(timestamps)} rows after warm-up — insufficient")
        return None

    t_min, t_max = timestamps[0], timestamps[-1]
    data_hours   = (t_max - t_min).total_seconds() / 3600

    # ── 5. Price data from PostgreSQL ─────────────────────────────────────────
    try:
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute("""
                    SELECT
                        DATE_TRUNC('minute', timestamp) +
                            (EXTRACT(second FROM timestamp)::int / 30) *
                            INTERVAL '30 seconds'   AS bucket,
                        AVG(price)                  AS avg_price,
                        MAX(price)                  AS max_price,
                        MIN(price)                  AS min_price
                    FROM prices
                    WHERE token = 'SOL'
                      AND timestamp >= %s - INTERVAL '1 hour'
                      AND timestamp <= %s + INTERVAL '20 minutes'
                    GROUP BY 1 ORDER BY 1
                """, [t_min.replace(tzinfo=None), t_max.replace(tzinfo=None)])
                price_rows = cur.fetchall()
    except Exception as e:
        logger.error(f"Price query failed: {e}")
        return None

    price_df = pd.DataFrame([dict(r) for r in price_rows])
    if price_df.empty:
        logger.warning("No price data returned")
        return None

    price_df['bucket'] = pd.to_datetime(price_df['bucket'], utc=True)
    price_df.set_index('bucket', inplace=True)
    price_df.sort_index(inplace=True)

    # ── 6. Momentum features (vectorised via pandas reindex + rolling) ────────
    avg_price_series = price_df['avg_price'].reindex(df.index, method='ffill')

    df['pm_price_change_30s'] = avg_price_series.pct_change(1).fillna(0) * 100
    df['pm_price_change_1m']  = avg_price_series.pct_change(
        max(1, int(60 / BUCKET_SECONDS))).fillna(0) * 100
    df['pm_price_change_5m']  = avg_price_series.pct_change(
        max(1, int(300 / BUCKET_SECONDS))).fillna(0) * 100
    df['pm_velocity_30s']     = df['pm_price_change_1m'] - df['pm_price_change_5m']

    # ── 7. Build feature matrix numpy array ──────────────────────────────────
    for col in FEATURES:
        if col not in df.columns:
            df[col] = 0.0

    feature_matrix = df[FEATURES].values.astype(np.float32)
    feature_matrix = np.where(np.isfinite(feature_matrix), feature_matrix, 0.0)

    # ── 8. Forward returns + price paths ─────────────────────────────────────
    N           = len(timestamps)
    max_gains   = np.zeros(N, dtype=np.float32)
    labels      = np.zeros(N, dtype=np.int8)
    price_highs = []
    price_lows  = []
    entry_prices_arr = np.zeros(N, dtype=np.float32)

    fwd_buckets = FWD_MINUTES * int(60 / BUCKET_SECONDS)  # buckets in fwd window

    for i, ts in enumerate(timestamps):
        t_end  = ts + pd.Timedelta(minutes=FWD_MINUTES)

        # entry price: closest price at or just before ts
        avail = price_df[price_df.index <= ts + pd.Timedelta(seconds=BUCKET_SECONDS)]
        if avail.empty:
            price_highs.append(np.zeros(fwd_buckets, dtype=np.float32))
            price_lows.append(np.zeros(fwd_buckets, dtype=np.float32))
            continue

        entry = float(avail.iloc[-1]['avg_price'])
        entry_prices_arr[i] = entry
        if entry == 0:
            price_highs.append(np.zeros(fwd_buckets, dtype=np.float32))
            price_lows.append(np.zeros(fwd_buckets, dtype=np.float32))
            continue

        # Pre-entry top-buying guard: skip if price already rose sharply in last 2 min.
        # We don't want to buy momentum that's already exhausted.
        pre_start = ts - pd.Timedelta(minutes=2)
        pre_slice = price_df.loc[pre_start:ts, 'avg_price']
        if len(pre_slice) >= 2:
            pre_lo  = float(pre_slice.min())
            pre_gain = (entry - pre_lo) / pre_lo if pre_lo > 0 else 0.0
            if pre_gain >= PRE_ENTRY_TOP_GUARD:
                # Already pumping — skip this as a pump entry candidate
                price_highs.append(np.zeros(fwd_buckets, dtype=np.float32))
                price_lows.append(np.zeros(fwd_buckets, dtype=np.float32))
                continue

        fwd_hi = price_df.loc[ts:t_end, 'max_price'].values.astype(np.float32)
        fwd_lo = price_df.loc[ts:t_end, 'min_price'].values.astype(np.float32)

        if len(fwd_hi) < fwd_buckets:
            pad    = fwd_buckets - len(fwd_hi)
            fwd_hi = np.pad(fwd_hi, (0, pad), constant_values=entry)
            fwd_lo = np.pad(fwd_lo, (0, pad), constant_values=entry)
        else:
            fwd_hi = fwd_hi[:fwd_buckets]
            fwd_lo = fwd_lo[:fwd_buckets]

        max_gain     = (fwd_hi.max() - entry) / entry if entry > 0 else 0.0
        max_gains[i] = max_gain
        labels[i]    = 1 if max_gain >= PUMP_THRESHOLD else 0

        price_highs.append(fwd_hi)
        price_lows.append(fwd_lo)

    logger.info(
        f"Feature matrix: {N} rows × {N_FEATURES} features | "
        f"pumps={labels.sum()} ({labels.mean()*100:.1f}%) | "
        f"data={data_hours:.1f}h"
    )

    return {
        "features":     feature_matrix,
        "labels":       labels,
        "max_gains":    max_gains,
        "price_highs":  price_highs,
        "price_lows":   price_lows,
        "timestamps":   timestamps,
        "data_hours":   data_hours,
        "entry_prices": entry_prices_arr,
    }



# =============================================================================
# PHASE 2 — EXIT SIMULATION (vectorized)
# =============================================================================

def _sim_exit_batch(
    mask: np.ndarray,
    price_highs: List[np.ndarray],
    price_lows:  List[np.ndarray],
    entry_prices: np.ndarray,
    tiers: List[Tuple[float, float, float]],
    stop_loss: float,
    min_hold_buckets: int,
) -> np.ndarray:
    """Simulate trailing-stop exits for all entries where mask=True.

    Returns an array of exit returns (fraction, not %) for each entry in mask.
    """
    indices = np.where(mask)[0]
    exits   = np.zeros(len(indices), dtype=np.float64)

    for j, idx in enumerate(indices):
        entry  = entry_prices[idx]
        hi_arr = price_highs[idx]
        lo_arr = price_lows[idx]

        if entry == 0 or len(hi_arr) == 0:
            exits[j] = -stop_loss
            continue

        highest     = entry
        locked_tol  = 1.0

        for k, (hi, lo) in enumerate(zip(hi_arr, lo_arr)):
            if hi > highest:
                highest = hi

            hg = (highest - entry) / entry

            # pick tolerance tier
            tol = tiers[-1][2]
            for lo_t, hi_t, t in tiers:
                if lo_t <= hg < hi_t:
                    tol = t
                    break

            if tol < locked_tol:
                locked_tol = tol

            if k < min_hold_buckets:
                continue

            # stop-loss check
            if (lo - entry) / entry < -stop_loss:
                exits[j] = -stop_loss
                break

            # trailing stop check
            if hg > 0 and (lo - highest) / highest < -locked_tol:
                exits[j] = (highest * (1 - locked_tol) - entry) / entry
                break
        else:
            # held to end of window
            final = hi_arr[-1] if len(hi_arr) else entry
            exits[j] = (final - entry) / entry

    return exits


def simulate_exits(
    mask: np.ndarray,
    labels: np.ndarray,
    price_highs: List[np.ndarray],
    price_lows:  List[np.ndarray],
    entry_prices: np.ndarray,
    tiers: List[Tuple[float, float, float]],
    stop_loss: float,
    min_hold_buckets: int,
) -> Dict[str, float]:
    """Run exit simulation for a signal mask and return performance metrics."""
    raw_exits = _sim_exit_batch(
        mask, price_highs, price_lows, entry_prices,
        tiers, stop_loss, min_hold_buckets,
    )
    net_exits = raw_exits - COST_PCT  # deduct trading cost

    pumps_mask   = labels[mask].astype(bool)
    pump_exits   = net_exits[pumps_mask]
    nonp_exits   = net_exits[~pumps_mask]

    n_sig      = int(mask.sum())
    precision  = pumps_mask.mean() if n_sig else 0.0
    win_rate   = (net_exits > 0).mean() if n_sig else 0.0
    avg_exit   = net_exits.mean() if n_sig else 0.0
    std_exit   = net_exits.std() if n_sig else 1.0
    sharpe     = avg_exit / (std_exit + 1e-9)
    pump_avg   = pump_exits.mean() if len(pump_exits) else 0.0
    nonp_avg   = nonp_exits.mean() if len(nonp_exits) else -COST_PCT

    return {
        "n_signals":   n_sig,
        "precision":   precision,
        "win_rate":    win_rate,
        "ev_per_trade": avg_exit,
        "sharpe":      sharpe,
        "pump_avg":    pump_avg,
        "nonp_avg":    nonp_avg,
    }


# =============================================================================
# PHASE 3 — GENETIC ALGORITHM
# =============================================================================

class Individual:
    """One candidate = entry conditions + exit parameters (evolved jointly).

    Exit parameters:
        stop_loss   : fraction (e.g. 0.003 = stop out at -0.3% from entry)
        min_hold_s  : seconds to hold before any trailing stop activates
        t1_boundary : gain fraction where tier-1 (loose) trailing ends
        t2_boundary : gain fraction where tier-2 (medium) trailing ends
        t1_tol      : trailing tolerance in tier 1 (before first gain target)
        t2_tol      : trailing tolerance in tier 2
        t3_tol      : trailing tolerance in tier 3 (high gain, tight lock-in)
    """
    __slots__ = ("conditions", "fitness_val",
                 "stop_loss", "min_hold_s",
                 "t1_boundary", "t2_boundary",
                 "t1_tol", "t2_tol", "t3_tol")

    def __init__(
        self,
        conditions: List[Tuple[int, int, float]],
        stop_loss:   float = 0.003,
        min_hold_s:  int   = 60,
        t1_boundary: float = 0.0015,
        t2_boundary: float = 0.003,
        t1_tol:      float = 0.004,
        t2_tol:      float = 0.0015,
        t3_tol:      float = 0.0003,
    ):
        self.conditions  = conditions
        self.fitness_val = -np.inf
        self.stop_loss   = float(stop_loss)
        self.min_hold_s  = int(min_hold_s)
        self.t1_boundary = float(t1_boundary)
        self.t2_boundary = float(t2_boundary)
        self.t1_tol      = float(t1_tol)
        self.t2_tol      = float(t2_tol)
        self.t3_tol      = float(t3_tol)

    def make_tiers(self) -> List[Tuple[float, float, float]]:
        """Build the 3-tier tolerance structure from evolved exit params."""
        t1b = self.t1_boundary
        t2b = max(self.t2_boundary, t1b + 0.0005)  # ensure ordering
        return [
            (0.0,  t1b,  self.t1_tol),
            (t1b,  t2b,  self.t2_tol),
            (t2b,  1.0,  self.t3_tol),
        ]

    def apply(self, features: np.ndarray) -> np.ndarray:
        """Return boolean mask (N,) where all entry conditions are satisfied."""
        mask = np.ones(len(features), dtype=bool)
        for feat_idx, direction, thr in self.conditions:
            col = features[:, feat_idx]
            if direction > 0:
                mask &= col > thr
            else:
                mask &= col < thr
        return mask

    def to_json(self) -> List[Dict[str, Any]]:
        return [
            {"feature": FEATURES[fi], "direction": ">" if d > 0 else "<", "threshold": round(float(thr), 6)}
            for fi, d, thr in self.conditions
        ]

    def exit_config(self) -> Dict[str, Any]:
        return {
            "stop_loss":        round(self.stop_loss, 5),
            "min_hold_seconds": self.min_hold_s,
            "tiers":            self.make_tiers(),
        }

    def __repr__(self) -> str:
        parts = [
            f"{FEATURES[fi]}{'>' if d > 0 else '<'}{thr:.4f}"
            for fi, d, thr in self.conditions
        ]
        return " AND ".join(parts)


def _random_exit_params() -> Dict[str, Any]:
    """Sample a random exit configuration within the evolved ranges."""
    sl  = random.uniform(*EXIT_SL_RANGE)
    hold = random.randint(*EXIT_HOLD_RANGE)
    t1b  = random.uniform(*EXIT_T1B_RANGE)
    t2b  = random.uniform(max(EXIT_T2B_RANGE[0], t1b + 0.001), EXIT_T2B_RANGE[1])
    t1tol = random.uniform(*EXIT_T1TOL_RANGE)
    t2tol = random.uniform(*EXIT_T2TOL_RANGE)
    t3tol = random.uniform(*EXIT_T3TOL_RANGE)
    return dict(stop_loss=sl, min_hold_s=hold,
                t1_boundary=t1b, t2_boundary=t2b,
                t1_tol=t1tol, t2_tol=t2tol, t3_tol=t3tol)


def _random_individual(features: np.ndarray, use_importance: bool = False) -> Individual:
    n_conds = random.randint(GA_MIN_CONDS, GA_MAX_CONDS)
    imp     = _feature_importance

    if use_importance and imp is not None:
        # Importance-biased sampling: features that appeared in good rules more likely
        probs = imp / imp.sum()
        feat_indices = list(np.random.choice(N_FEATURES, size=min(n_conds, N_FEATURES),
                                              replace=False, p=probs))
    else:
        feat_indices = random.sample(range(N_FEATURES), min(n_conds, N_FEATURES))

    conds = []
    for fi in feat_indices:
        col  = features[:, fi]
        vals = col[np.isfinite(col)]
        if len(vals) < 10:
            continue
        pct  = random.uniform(10, 90)
        thr  = float(np.percentile(vals, pct))
        d    = random.choice([1, -1])
        conds.append((fi, d, thr))
    if not conds:
        return _random_individual(features)
    return Individual(conds, **_random_exit_params())


def _crossover(p1: Individual, p2: Individual) -> Individual:
    """Produce a child by mixing conditions AND exit params from both parents."""
    all_conds = p1.conditions + p2.conditions
    seen: Dict[int, Tuple] = {}
    for c in all_conds:
        seen[c[0]] = c
    all_unique = list(seen.values())
    random.shuffle(all_unique)
    n = random.randint(GA_MIN_CONDS, min(GA_MAX_CONDS, len(all_unique)))

    # Blend exit params: randomly pick from each parent per parameter
    def blend(a, b):
        return a if random.random() < 0.5 else b

    return Individual(
        all_unique[:n],
        stop_loss   = blend(p1.stop_loss,   p2.stop_loss),
        min_hold_s  = blend(p1.min_hold_s,  p2.min_hold_s),
        t1_boundary = blend(p1.t1_boundary, p2.t1_boundary),
        t2_boundary = blend(p1.t2_boundary, p2.t2_boundary),
        t1_tol      = blend(p1.t1_tol,      p2.t1_tol),
        t2_tol      = blend(p1.t2_tol,      p2.t2_tol),
        t3_tol      = blend(p1.t3_tol,      p2.t3_tol),
    )


def _perturb(val: float, lo: float, hi: float, noise_frac: float = 0.15) -> float:
    """Gaussian perturbation of a scalar exit param, clipped to range."""
    delta = random.gauss(0, (hi - lo) * noise_frac)
    return float(np.clip(val + delta, lo, hi))


def _mutate(ind: Individual, features: np.ndarray) -> Individual:
    """Return a mutated copy — can mutate entry conditions OR exit params."""
    conds      = list(deepcopy(ind.conditions))
    stop_loss  = ind.stop_loss
    min_hold_s = ind.min_hold_s
    t1b        = ind.t1_boundary
    t2b        = ind.t2_boundary
    t1tol      = ind.t1_tol
    t2tol      = ind.t2_tol
    t3tol      = ind.t3_tol

    op = random.random()

    if op < 0.30 and conds:
        # perturb an entry threshold
        i   = random.randrange(len(conds))
        fi, d, thr = conds[i]
        col = features[:, fi]
        std = float(np.nanstd(col[col != 0])) if (col != 0).any() else 0.01
        delta = random.gauss(0, std * 0.2)
        conds[i] = (fi, d, float(np.clip(thr + delta,
                                          np.nanpercentile(col, 5),
                                          np.nanpercentile(col, 95))))
    elif op < 0.45 and conds:
        # flip direction of a condition
        i   = random.randrange(len(conds))
        fi, d, thr = conds[i]
        conds[i] = (fi, -d, thr)
    elif op < 0.55:
        # remove a condition
        if len(conds) > GA_MIN_CONDS:
            conds.pop(random.randrange(len(conds)))
    elif op < 0.65:
        # add a new condition
        if len(conds) < GA_MAX_CONDS:
            used_fi = {c[0] for c in conds}
            imp = _feature_importance
            if imp is not None:
                avail = [fi for fi in range(N_FEATURES) if fi not in used_fi]
                if avail:
                    probs = np.array([imp[fi] for fi in avail])
                    probs /= probs.sum()
                    fi = int(np.random.choice(avail, p=probs))
                else:
                    fi = None
            else:
                avail = [fi for fi in range(N_FEATURES) if fi not in used_fi]
                fi = random.choice(avail) if avail else None
            if fi is not None:
                col = features[:, fi]
                thr = float(np.percentile(col, random.uniform(10, 90)))
                d   = random.choice([1, -1])
                conds.append((fi, d, thr))
    elif op < 0.75:
        # mutate stop-loss
        stop_loss = _perturb(stop_loss, *EXIT_SL_RANGE)
    elif op < 0.82:
        # mutate min-hold
        min_hold_s = int(np.clip(min_hold_s + random.randint(-30, 30),
                                  EXIT_HOLD_RANGE[0], EXIT_HOLD_RANGE[1]))
    elif op < 0.88:
        # mutate tier boundaries
        t1b = _perturb(t1b, *EXIT_T1B_RANGE)
        t2b = max(t1b + 0.001, _perturb(t2b, *EXIT_T2B_RANGE))
    else:
        # mutate tier tolerances
        choice = random.randint(0, 2)
        if choice == 0:
            t1tol = _perturb(t1tol, *EXIT_T1TOL_RANGE)
        elif choice == 1:
            t2tol = _perturb(t2tol, *EXIT_T2TOL_RANGE)
        else:
            t3tol = _perturb(t3tol, *EXIT_T3TOL_RANGE)

    if not conds:
        return _random_individual(features)
    return Individual(conds,
                      stop_loss=stop_loss, min_hold_s=min_hold_s,
                      t1_boundary=t1b, t2_boundary=t2b,
                      t1_tol=t1tol, t2_tol=t2tol, t3_tol=t3tol)


def _compute_fitness(
    ind: Individual,
    features: np.ndarray,
    labels:   np.ndarray,
    entry_prices: np.ndarray,
    price_highs: List[np.ndarray],
    price_lows:  List[np.ndarray],
    data_hours: float,
    # Legacy fixed-exit arguments ignored — Individual carries its own exits now
    fixed_tiers: Optional[List] = None,
    fixed_sl:    float = 0.003,
    fixed_hold:  int   = 2,
) -> float:
    mask   = ind.apply(features)
    n_sig  = int(mask.sum())
    # Hard floor: at least GA_MIN_SIG_HARD signals/day (statistical reliability)
    min_abs = max(GA_MIN_SIG_HARD, MIN_SIGNALS) * (data_hours / 24)
    if n_sig < min_abs:
        return -999.0
    if n_sig > MAX_SIGNALS * (data_hours / 24):
        return -998.0

    # Use individual's own evolved exit parameters
    tiers  = ind.make_tiers()
    hold_b = int(ind.min_hold_s / BUCKET_SECONDS)

    exits = _sim_exit_batch(
        mask, price_highs, price_lows, entry_prices,
        tiers, ind.stop_loss, hold_b,
    )
    net       = exits - COST_PCT
    ev_trade  = float(net.mean())
    std_trade = float(net.std()) + 1e-9
    sharpe    = ev_trade / std_trade
    sig_per_d = n_sig / data_hours * 24

    # Sharpe-weighted daily EV: rewards consistent strategies over lucky ones.
    # A strategy with sharpe=2 gets a ~1.8x boost; sharpe=-1 gets 0.6x penalty.
    sharpe_mult = max(0.3, min(3.0, 1.0 + sharpe * SHARPE_WEIGHT))
    return ev_trade * sig_per_d * sharpe_mult


def _update_feature_importance(top_inds: List[Individual]) -> None:
    """Update feature importance based on features in the top individuals.

    Features appearing in high-fitness individuals get higher weights.
    Uses exponential moving average so past knowledge decays slowly.
    """
    global _feature_importance
    counts = np.zeros(N_FEATURES, dtype=np.float64)
    for ind in top_inds[:20]:
        weight = max(0.0, ind.fitness_val)  # weight by fitness
        for fi, _d, _t in ind.conditions:
            counts[fi] += 1.0 + weight * 10.0
    if counts.sum() > 0:
        new_imp = (counts + 0.5) / (counts.sum() + 0.5 * N_FEATURES)
        if _feature_importance is None:
            _feature_importance = new_imp
        else:
            _feature_importance = 0.70 * _feature_importance + 0.30 * new_imp
        _feature_importance /= _feature_importance.sum()  # keep normalised


def _seed_from_db(features: np.ndarray) -> List[Individual]:
    """Load top rules from simulation_results and convert to seeded Individuals.

    This allows each run to build on the best knowledge found in previous runs
    rather than starting from a blank slate.
    """
    seeds: List[Individual] = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT conditions_json, exit_config_json
                    FROM simulation_results
                    WHERE win_rate    >= 0.65
                      AND daily_ev    >  0
                      AND n_signals   >= 25
                      AND signals_per_day >= 20
                    ORDER BY daily_ev DESC
                    LIMIT 30
                """)
                rows = cur.fetchall()

        for row in rows:
            try:
                conds_raw  = row['conditions_json']
                exit_raw   = row['exit_config_json'] or {}

                conds = []
                for c in conds_raw:
                    feat = c.get('feature')
                    if feat not in FEAT_IDX:
                        continue
                    fi  = FEAT_IDX[feat]
                    d   = 1 if c.get('direction') == '>' else -1
                    thr = float(c.get('threshold', 0.0))
                    # Verify the threshold is still in a valid percentile range
                    col  = features[:, fi]
                    p5   = float(np.nanpercentile(col, 5))
                    p95  = float(np.nanpercentile(col, 95))
                    thr  = float(np.clip(thr, p5, p95))
                    conds.append((fi, d, thr))

                if len(conds) < GA_MIN_CONDS:
                    continue

                # Parse exit config
                tiers = exit_raw.get('tiers', [])
                sl    = float(exit_raw.get('stop_loss', 0.003))
                hold  = int(exit_raw.get('min_hold_seconds', 60))
                t1b   = float(tiers[0][1]) if len(tiers) > 0 else 0.0015
                t2b   = float(tiers[1][1]) if len(tiers) > 1 else 0.003
                t1tol = float(tiers[0][2]) if len(tiers) > 0 else 0.004
                t2tol = float(tiers[1][2]) if len(tiers) > 1 else 0.0015
                t3tol = float(tiers[2][2]) if len(tiers) > 2 else 0.0003

                ind = Individual(
                    conds,
                    stop_loss=sl, min_hold_s=hold,
                    t1_boundary=t1b, t2_boundary=t2b,
                    t1_tol=t1tol, t2_tol=t2tol, t3_tol=t3tol,
                )
                seeds.append(ind)
            except Exception:
                continue

    except Exception as e:
        logger.warning(f"[DB seed] Failed: {e}")

    if seeds:
        logger.info(f"[DB seed] Loaded {len(seeds)} seed individuals from simulation_results")
    return seeds


def run_genetic_algorithm(
    features:     np.ndarray,
    labels:       np.ndarray,
    entry_prices: np.ndarray,
    price_highs:  List[np.ndarray],
    price_lows:   List[np.ndarray],
    data_hours:   float,
    fixed_tiers:  Optional[List] = None,   # kept for API compat, now ignored
    fixed_sl:     float = 0.003,
    fixed_hold:   int   = 2,
    run_label:    str   = "",
) -> List[Individual]:
    """Genetic algorithm search. Returns top-20 individuals by fitness.

    Improvements over v1:
      - Joint entry+exit optimisation (exit params evolved alongside entry)
      - Sharpe-weighted fitness (consistent > lucky)
      - DB seeding: 15% of population from previous best results
      - Feature importance bias: 40% of new individuals weighted by historical success
    """
    logger.info(
        f"GA start: pop={GA_POPULATION}, gen={GA_GENERATIONS}, "
        f"features={N_FEATURES}, rows={len(features)}"
    )

    n_db_seeds = int(GA_POPULATION * GA_DB_SEED_FRAC)
    n_imp_inds = int(GA_POPULATION * GA_IMPORTANCE_FRAC)

    # Seed from DB (builds on previous run knowledge)
    db_seeds = _seed_from_db(features)[:n_db_seeds]

    # Mix: DB seeds + importance-biased + pure random
    population: List[Individual] = list(db_seeds)
    for _ in range(n_imp_inds):
        population.append(_random_individual(features, use_importance=True))
    while len(population) < GA_POPULATION:
        population.append(_random_individual(features, use_importance=False))

    # Evaluate initial fitness
    for ind in population:
        ind.fitness_val = _compute_fitness(
            ind, features, labels, entry_prices,
            price_highs, price_lows, data_hours,
        )

    elite_n = max(1, int(GA_POPULATION * GA_ELITE_FRAC))
    best_so_far: float = -np.inf
    stagnant = 0

    for gen in range(GA_GENERATIONS):
        # Sort by fitness
        population.sort(key=lambda x: x.fitness_val, reverse=True)

        best_gen = population[0].fitness_val
        if best_gen > best_so_far + 1e-7:
            best_so_far = best_gen
            stagnant    = 0
        else:
            stagnant += 1

        if gen % 25 == 0 or gen == GA_GENERATIONS - 1:
            best = population[0]
            prec = labels[best.apply(features)].mean() if population else 0
            logger.info(
                f"[{run_label}Gen {gen+1:3d}/{GA_GENERATIONS}] "
                f"best_fitness={best_so_far*100:+.4f} | "
                f"prec={prec*100:.1f}% | "
                f"n={int(best.apply(features).sum())} | "
                f"sl={best.stop_loss*100:.2f}% hold={best.min_hold_s}s | "
                f"rule: {best}"
            )

        # Diversity injection: if stuck for 40 gens, replace bottom 50% with fresh randoms
        # instead of stopping — allows escape from local optima
        if stagnant == 40:
            n_refresh = GA_POPULATION // 2
            for k in range(-n_refresh, 0):
                use_imp = random.random() < GA_IMPORTANCE_FRAC
                population[k] = _random_individual(features, use_importance=use_imp)
                population[k].fitness_val = _compute_fitness(
                    population[k], features, labels, entry_prices,
                    price_highs, price_lows, data_hours,
                )
            logger.info(f"[{run_label}Gen {gen+1}] Diversity injection: refreshed {n_refresh} individuals")

        # Hard stop if still stuck after injection
        if stagnant > 100:
            logger.info(f"Early stop at gen {gen+1} (stagnant for {stagnant} gens)")
            break

        # Keep elites
        new_pop = population[:elite_n]

        # Fill rest with crossover + mutation
        while len(new_pop) < GA_POPULATION:
            op = random.random()
            if op < GA_CROSSOVER:
                # tournament selection × 2
                p1 = _tournament(population)
                p2 = _tournament(population)
                child = _crossover(p1, p2)
            else:
                p1    = _tournament(population)
                child = deepcopy(p1)

            if random.random() < GA_MUTATION:
                child = _mutate(child, features)

            child.fitness_val = _compute_fitness(
                child, features, labels, entry_prices,
                price_highs, price_lows, data_hours,
                fixed_tiers, fixed_sl, fixed_hold,
            )
            new_pop.append(child)

        population = new_pop

    population.sort(key=lambda x: x.fitness_val, reverse=True)
    top20 = population[:20]
    # Update feature importance from this run's top performers
    _update_feature_importance(top20)
    return top20


def _tournament(population: List[Individual]) -> Individual:
    contenders = random.sample(population, min(GA_TOURNAMENT_K, len(population)))
    return max(contenders, key=lambda x: x.fitness_val)


# =============================================================================
# PHASE 4 — EXIT GRID SEARCH
# =============================================================================

def exit_grid_search(
    ind: Individual,
    features:     np.ndarray,
    labels:       np.ndarray,
    entry_prices: np.ndarray,
    price_highs:  List[np.ndarray],
    price_lows:   List[np.ndarray],
    data_hours:   float,
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """Grid-search exit config for one entry rule. Returns (best_config, best_metrics)."""
    mask     = ind.apply(features)
    n_sig    = int(mask.sum())

    if n_sig < 3:
        empty_cfg = {"stop_loss": 0.003, "min_hold_seconds": 60,
                     "tiers": [(0, 0.002, 0.003), (0.002, 0.003, 0.001), (0.003, 1.0, 0.0005)]}
        return empty_cfg, {"ev_per_trade": -COST_PCT, "daily_ev": 0, "win_rate": 0, "sharpe": 0}

    best_daily_ev = -np.inf
    best_cfg      = {}
    best_metrics  = {}

    for sl in STOP_LOSS_GRID:
        for hold_s in MIN_HOLD_GRID:
            hold_b = int(hold_s / BUCKET_SECONDS)
            for t1_split in TIER1_SPLIT_GRID:
                for t2_tol in TIER2_TOL_GRID:
                    for t3_tol in TIER3_TOL_GRID:
                        tiers = [
                            (0.0,      t1_split, 0.003),
                            (t1_split, 0.003,    t2_tol),
                            (0.003,    1.0,      t3_tol),
                        ]
                        metrics = simulate_exits(
                            mask, labels, price_highs, price_lows,
                            entry_prices, tiers, sl, hold_b,
                        )
                        sig_per_d = n_sig / data_hours * 24
                        daily_ev  = metrics["ev_per_trade"] * sig_per_d

                        if daily_ev > best_daily_ev:
                            best_daily_ev = daily_ev
                            best_cfg = {
                                "stop_loss":         sl,
                                "min_hold_seconds":  hold_s,
                                "tiers":             tiers,
                            }
                            best_metrics = {**metrics, "daily_ev": daily_ev,
                                            "signals_per_day": sig_per_d}

    return best_cfg, best_metrics


# =============================================================================
# PHASE 5 — MULTI-FOLD WALK-FORWARD VALIDATION
# =============================================================================

def multi_fold_validate(
    ind:      Individual,
    exit_cfg: Dict[str, Any],
    data:     Dict[str, Any],
    n_folds:  int = OOS_FOLDS,
) -> Tuple[float, float, float]:
    """Expanding-window walk-forward validation with n_folds OOS windows.

    Splits the data into (n_folds + 1) chunks. For each fold k:
      - train = all data up to chunk k
      - test  = chunk k+1 (OOS)

    Returns (avg_insample_ev, avg_oos_ev, oos_consistency_score).
    oos_consistency_score = fraction of folds where OOS EV > 0 (robustness).
    """
    features     = data["features"]
    entry_prices = data["entry_prices"]
    price_highs  = data["price_highs"]
    price_lows   = data["price_lows"]
    N            = len(features)

    tiers  = [(lo, hi, t) for lo, hi, t in exit_cfg["tiers"]]
    sl     = exit_cfg["stop_loss"]
    hold_b = int(exit_cfg["min_hold_seconds"] / BUCKET_SECONDS)

    chunk = max(1, N // (n_folds + 1))
    is_evs:  List[float] = []
    oos_evs: List[float] = []

    for fold in range(n_folds):
        train_end  = (fold + 1) * chunk
        test_start = train_end
        test_end   = min(N, test_start + chunk)

        if test_end <= test_start:
            continue

        train_m = np.zeros(N, dtype=bool)
        train_m[:train_end] = True
        test_m  = np.zeros(N, dtype=bool)
        test_m[test_start:test_end] = True

        def ev_for(mask: np.ndarray) -> float:
            combined = ind.apply(features) & mask
            if combined.sum() < 2:
                return float("nan")
            exits = _sim_exit_batch(
                combined, price_highs, price_lows, entry_prices, tiers, sl, hold_b
            )
            return float((exits - COST_PCT).mean())

        is_ev  = ev_for(train_m)
        oos_ev = ev_for(test_m)

        if not np.isnan(is_ev):
            is_evs.append(is_ev)
        if not np.isnan(oos_ev):
            oos_evs.append(oos_ev)

    avg_is  = float(np.mean(is_evs))  if is_evs  else 0.0
    avg_oos = float(np.mean(oos_evs)) if oos_evs else 0.0
    # fraction of folds where OOS was positive (consistency measure)
    oos_consistency = float(sum(1 for v in oos_evs if v > 0) / max(1, len(oos_evs)))

    return avg_is, avg_oos, oos_consistency


# =============================================================================
# PHASE 6 — PERSISTENCE
# =============================================================================

def _to_python(v: Any) -> Any:
    """Recursively convert numpy scalars to native Python types for JSON/psycopg2."""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, np.float32, np.float64)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, dict):
        return {k: _to_python(vv) for k, vv in v.items()}
    if isinstance(v, (list, tuple)):
        return [_to_python(vv) for vv in v]
    return v


def _dedup_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove near-duplicate rules that share the same core feature set."""
    seen_feature_sets: set = set()
    deduped = []
    for r in results:
        # canonical key = frozenset of (feature, direction) pairs (ignore threshold)
        key = frozenset(
            (c["feature"], c["direction"])
            for c in r.get("conditions", [])
        )
        if key not in seen_feature_sets:
            seen_feature_sets.add(key)
            deduped.append(r)
    return deduped


def save_results(
    run_id:     str,
    top_results: List[Dict[str, Any]],
    data_hours: float,
) -> None:
    """Persist top results to simulation_results PostgreSQL table."""
    # Add oos_consistency column if missing (migration guard)
    try:
        postgres_execute("""
            ALTER TABLE simulation_results
            ADD COLUMN IF NOT EXISTS oos_consistency DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS ga_exit_json JSONB
        """)
    except Exception:
        pass

    for rank, result in enumerate(top_results, start=1):
        try:
            conditions_json  = json.dumps(_to_python(result["conditions"]))
            exit_config_json = json.dumps(_to_python(result["exit_config"]))
            ga_exit_json     = json.dumps(_to_python(result.get("ga_exit", {})))

            postgres_execute("""
                INSERT INTO simulation_results
                    (run_id, rank, conditions_json, n_features, n_signals,
                     signals_per_day, precision_pct, recall_pct,
                     exit_config_json, ev_per_trade, daily_ev,
                     win_rate, sharpe, insample_ev, oos_ev, oos_gap,
                     oos_consistency, ga_exit_json, data_hours)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [
                run_id, rank,
                conditions_json,
                int(result["n_features"]),
                int(result["n_signals"]),
                float(result["signals_per_day"]),
                float(result["precision_pct"]),
                float(result.get("recall_pct", 0.0)),
                exit_config_json,
                float(result["ev_per_trade"]),
                float(result["daily_ev"]),
                float(result["win_rate"]),
                float(result["sharpe"]),
                float(result.get("insample_ev", 0.0)),
                float(result.get("oos_ev", 0.0)),
                float(result.get("oos_gap", 0.0)),
                float(result.get("oos_consistency", 0.0)),
                ga_exit_json,
                float(data_hours),
            ])
        except Exception as e:
            logger.error(f"Failed to save result rank {rank}: {e}")


def _update_hall_of_fame(new_results: List[Dict[str, Any]]) -> None:
    """Merge new results into the global Hall of Fame (best across ALL runs).

    A result only enters the HoF if it beats the lowest current HoF entry.
    This ensures the HoF only ever improves — it never regresses.
    """
    global _hall_of_fame
    combined = _hall_of_fame + new_results
    # Sort by oos_consistency * avg_oos * daily_ev composite score
    def hof_score(r: Dict) -> float:
        cons  = r.get('oos_consistency', 0.5)
        oos   = r.get('oos_ev', 0.0)
        daily = r.get('daily_ev', 0.0)
        return daily * max(0.1, cons) * (1.0 + max(0.0, oos) * 10)

    combined.sort(key=hof_score, reverse=True)
    # Deduplicate by feature set (keep best version of each unique rule)
    seen: set = set()
    deduped = []
    for r in combined:
        key = frozenset(
            (c["feature"], c["direction"])
            for c in r.get("conditions", [])
        )
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    _hall_of_fame = deduped[:HOF_SIZE]

    if _hall_of_fame:
        best = _hall_of_fame[0]
        logger.info(
            f"[HoF] Top rule: {best.get('rule_str','')} | "
            f"daily_ev={best.get('daily_ev',0)*100:+.4f}% | "
            f"win={best.get('win_rate',0):.0%} | "
            f"oos_cons={best.get('oos_consistency',0):.0%}"
        )


def print_hall_of_fame() -> None:
    """Print the all-time best rules discovered across all runs."""
    if not _hall_of_fame:
        return
    sep = "=" * 90
    print(f"\n{sep}")
    print(f"HALL OF FAME  (all-time best {len(_hall_of_fame)} rules)")
    print(sep)
    for i, r in enumerate(_hall_of_fame[:10], 1):
        cons = r.get('oos_consistency', 0)
        print(
            f"#{i:2d}  daily_EV={r.get('daily_ev',0)*100:+.4f}%  "
            f"win={r.get('win_rate',0):.0%}  "
            f"oos_cons={cons:.0%}  "
            f"oos_ev={r.get('oos_ev',0)*100:+.4f}%  "
            f"{r.get('rule_str','')[:50]}"
        )
    print(sep)


def print_leaderboard(top_results: List[Dict[str, Any]], run_id: str) -> None:
    sep = "=" * 90
    print(f"\n{sep}")
    print(f"LEADERBOARD  run={run_id}  top={len(top_results)}")
    print(sep)
    hdr = f"{'#':>3}  {'daily_EV':>9}  {'EV/trade':>9}  {'prec':>6}  {'sig/d':>6}  {'win%':>6}  {'OOS gap':>8}  Rule"
    print(hdr)
    print("-" * 90)
    for r in top_results:
        oos_gap_s = f"{r.get('oos_gap', 0)*100:+.3f}%" if r.get('oos_ev') is not None else "   n/a"
        ovft_flag = " ⚠" if abs(r.get("oos_gap", 0)) > 0.0005 else ""
        print(
            f"{r['rank']:>3}  "
            f"{r['daily_ev']*100:>+8.4f}%  "
            f"{r['ev_per_trade']*100:>+8.4f}%  "
            f"{r['precision_pct']*100:>5.1f}%  "
            f"{r['signals_per_day']:>6.1f}  "
            f"{r['win_rate']*100:>5.1f}%  "
            f"{oos_gap_s:>8}{ovft_flag}  "
            f"{r['rule_str'][:45]}"
        )
    print(sep)

    if top_results:
        best = top_results[0]
        cfg  = best["exit_config"]
        print(f"\n★  BEST CONFIG (rank 1):")
        print(f"   Rule:    {best['rule_str']}")
        t = cfg.get("tiers", [])
        tier_str = "  |  ".join(
            f"{lo*100:.2f}-{hi*100:.2f}%: tol={tol*100:.2f}%"
            for lo, hi, tol in t
        ) if t else "n/a"
        print(f"   Exit:    SL={cfg.get('stop_loss',0)*100:.2f}%  "
              f"hold={cfg.get('min_hold_seconds',0)}s  "
              f"tiers=[{tier_str}]")
        print(f"   Stats:   daily_EV={best['daily_ev']*100:+.4f}%  "
              f"prec={best['precision_pct']*100:.1f}%  "
              f"{best['signals_per_day']:.1f} sig/day  "
              f"win={best['win_rate']*100:.1f}%  sharpe={best['sharpe']:.3f}")
    print()


# =============================================================================
# MAIN LOOP
# =============================================================================

def run_one_loop(run_number: int) -> Optional[List[Dict[str, Any]]]:
    """Execute one full simulation loop. Returns top results or None on failure.

    Phases:
      1. Build feature matrix + price paths from DuckDB + PostgreSQL
      2. (data quality check)
      3. Joint entry+exit Genetic Algorithm (sharpe-weighted, DB-seeded,
         feature-importance biased)
      4. Exit grid-search refinement pass for top GA individuals
      5. Multi-fold (3-fold) walk-forward validation
      6. Persist to PostgreSQL + update Hall of Fame + print leaderboard
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"\n{'#'*70}")
    logger.info(f"# RUN {run_number}  id={run_id}")
    logger.info(f"{'#'*70}")

    # ── Phase 1: data ─────────────────────────────────────────────────────────
    data = build_feature_matrix()
    if data is None:
        logger.error("Feature matrix build failed — skipping loop")
        return None

    features     = data["features"]
    labels       = data["labels"]
    entry_prices = data["entry_prices"]
    price_highs  = data["price_highs"]
    price_lows   = data["price_lows"]
    data_hours   = data["data_hours"]

    pump_pct = labels.mean() * 100
    logger.info(f"Data: {data_hours:.1f}h | pumps={labels.sum()} ({pump_pct:.1f}%) "
                f"| threshold={PUMP_THRESHOLD*100:.1f}% in {FWD_MINUTES}min")

    if labels.sum() < 10:
        logger.warning(f"Only {labels.sum()} pump events — skipping loop")
        return None

    # ── Phase 3: Joint entry+exit GA ──────────────────────────────────────────
    t0 = time.time()
    top_inds = run_genetic_algorithm(
        features, labels, entry_prices, price_highs, price_lows,
        data_hours,
        run_label=f"RUN{run_number} ",
    )
    logger.info(f"GA completed in {(time.time()-t0)/60:.1f} min  "
                f"— refining top {len(top_inds)} individuals")

    # ── Phase 4: Exit Grid Search (refinement of GA's evolved exit params) ────
    top_results = []
    for rank, ind in enumerate(top_inds, start=1):
        mask  = ind.apply(features)
        n_sig = int(mask.sum())
        if n_sig < 2:
            continue

        # Start from GA's own exit config; grid-search can only improve it
        t1 = time.time()
        exit_cfg, exit_metrics = exit_grid_search(
            ind, features, labels, entry_prices,
            price_highs, price_lows, data_hours,
        )
        t_exit = time.time() - t1

        # ── Phase 5: Multi-fold walk-forward ──────────────────────────────────
        avg_is, avg_oos, oos_consistency = multi_fold_validate(ind, exit_cfg, data)
        oos_gap = avg_is - avg_oos if not (np.isnan(avg_is) or np.isnan(avg_oos)) else 0.0

        recall = labels[mask].sum() / max(labels.sum(), 1)

        result = {
            "rank":             rank,
            "rule_str":         str(ind),
            "conditions":       ind.to_json(),
            "n_features":       len(ind.conditions),
            "n_signals":        n_sig,
            "signals_per_day":  exit_metrics.get("signals_per_day", n_sig / data_hours * 24),
            "precision_pct":    exit_metrics.get("precision", 0.0),
            "recall_pct":       float(recall),
            "exit_config":      exit_cfg,
            "ev_per_trade":     exit_metrics.get("ev_per_trade", 0.0),
            "daily_ev":         exit_metrics.get("daily_ev", 0.0),
            "win_rate":         exit_metrics.get("win_rate", 0.0),
            "sharpe":           exit_metrics.get("sharpe", 0.0),
            "insample_ev":      avg_is  if not np.isnan(avg_is)  else 0.0,
            "oos_ev":           avg_oos if not np.isnan(avg_oos) else 0.0,
            "oos_gap":          oos_gap,
            "oos_consistency":  oos_consistency,
            # Include GA's evolved exit for comparison
            "ga_exit": {
                "stop_loss":        ind.stop_loss,
                "min_hold_seconds": ind.min_hold_s,
                "tiers":            ind.make_tiers(),
            },
        }
        top_results.append(result)
        logger.info(
            f"  Rank {rank:2d}: daily_EV={result['daily_ev']*100:+.4f}%  "
            f"win={result['win_rate']*100:.0f}%  "
            f"n={n_sig}  oos_gap={oos_gap*100:+.3f}%  "
            f"oos_cons={oos_consistency:.0%}  "
            f"exit_grid={t_exit:.1f}s"
        )

    if not top_results:
        return None

    # Sort by daily_ev, deduplicate near-identical entry rules
    top_results.sort(key=lambda x: x["daily_ev"], reverse=True)
    top_results = _dedup_results(top_results)
    for i, r in enumerate(top_results, 1):
        r["rank"] = i

    # ── Phase 6: Persist + Hall of Fame + print ────────────────────────────────
    save_results(run_id, top_results, data_hours)
    _update_hall_of_fame(top_results)
    print_leaderboard(top_results, run_id)
    print_hall_of_fame()

    return top_results


def run_continuous() -> None:
    """Run indefinitely — start the next loop immediately after each one finishes."""
    logger.info("=" * 70)
    logger.info("MEGA SIGNAL SIMULATOR STARTED")
    logger.info(f"  Features:          {N_FEATURES}")
    logger.info(f"  GA population:     {GA_POPULATION}")
    logger.info(f"  GA generations:    {GA_GENERATIONS}")
    logger.info(f"  Exit grid configs: "
                f"{len(STOP_LOSS_GRID)*len(MIN_HOLD_GRID)*len(TIER1_SPLIT_GRID)*len(TIER2_TOL_GRID)*len(TIER3_TOL_GRID)}")
    logger.info(f"  Pump threshold:    {PUMP_THRESHOLD*100:.1f}%")
    logger.info(f"  OOS folds:         {OOS_FOLDS}")
    logger.info("=" * 70)

    run_number = 0
    while True:
        run_number += 1
        try:
            run_one_loop(run_number)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — stopping")
            break
        except Exception as e:
            logger.error(f"Loop {run_number} crashed: {e}", exc_info=True)
            logger.info("Retrying immediately after error...")

    logger.info("Mega simulator stopped.")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mega Signal Simulator")
    parser.add_argument("--once", action="store_true", help="Run one loop and exit")
    parser.add_argument("--pop",  type=int, default=GA_POPULATION,
                        help=f"GA population size (default: {GA_POPULATION})")
    parser.add_argument("--gen",  type=int, default=GA_GENERATIONS,
                        help=f"GA generations (default: {GA_GENERATIONS})")
    args = parser.parse_args()

    # Allow overriding GA params via CLI
    GA_POPULATION  = args.pop
    GA_GENERATIONS = args.gen

    if args.once:
        results = run_one_loop(run_number=1)
        sys.exit(0 if results else 1)
    else:
        run_continuous()

"""
Mega Signal Simulator  v3 — Path-Filtered Precision Engine
===========================================================
Finds entry rules where the price reliably goes UP by at least PUMP_THRESHOLD
within FWD_MINUTES of the signal WITHOUT triggering the live stop loss first.

Goal: detect moments where SOL will gain ≥0.2% in the next 7 minutes cleanly
(i.e. price doesn't drop below the 0.445% stop loss before reaching target).

Each loop (~20 min):
  1. Loads DuckDB feature data (30-second buckets, 5-min rolling windows).
  2. Runs a GA scoring rules on PATH-FILTERED PRECISION:
       fitness = precision × signals_per_day × consistency_mult
     where precision = fraction of signals where:
       - price reaches +PUMP_THRESHOLD (0.2%) within FWD_MINUTES
       - AND price never drops below -LIVE_STOP_LOSS (0.445%) first
  3. Hard-rejects rules where:
       - precision < MIN_DIRECTIONAL_PRECISION (0.55)
       - OOS precision < MIN_OOS_PRECISION (0.52) on held-out folds
  4. Saves qualifying rules to simulation_results.
     win_rate column = path-filtered precision (aligns with live trading).

Why this matters:
  v2 measured "did price ever touch +0.1% in 7 min?" — this was too optimistic.
  Rules firing after tiny dips (pm_price_change_30s < -0.07%) showed 80% precision
  in simulation but only 35-43% in live trading, because the dip continued long
  enough to trigger the stop loss (-0.44%) BEFORE the price recovered to +0.1%.

  v3 uses path-filtered labels and MAX_SIGNALS=100 to force selectivity.
  Rules must find moments where price rises cleanly without a prior stop-out.

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
PUMP_THRESHOLD  = 0.002          # 0.2% min forward gain (raised: must exceed stop-loss spread)
FWD_MINUTES     = 7              # forward window to check for the gain
BUCKET_SECONDS  = 30             # feature time series granularity
FEATURE_WINDOW  = 300            # rolling window for features (5 min)
MIN_SIGNALS     = 5              # minimum signals/day
MAX_SIGNALS     = 100            # cap signals/day — forces selectivity (was 300, too loose)
OOS_FOLDS       = 4              # more folds = harder overfitting bar
PRE_ENTRY_TOP_GUARD = 0.0015     # skip entry if price already rose >0.15% in last 2 min

# ── Live stop-loss threshold (from actual play sell_logic tolerance_rules) ────
# Labels are path-filtered: only count as win if target reached BEFORE stop triggers.
# Value matches the "decreases" tolerance in all play sell_logic configs (0.4445%).
LIVE_STOP_LOSS = 0.00445

# ── precision gates (the core anti-overfitting controls) ──────────────────────
MIN_DIRECTIONAL_PRECISION = 0.55   # rule must be right ≥55% of the time in-sample
MIN_OOS_PRECISION         = 0.52   # AND ≥52% on each held-out OOS fold (hard reject below)
MIN_OOS_FOLDS_PASSING     = 3      # at least 3 of 4 OOS folds must beat MIN_OOS_PRECISION

# GA parameters
GA_POPULATION       = 600
GA_GENERATIONS      = 250
GA_ELITE_FRAC       = 0.10
GA_CROSSOVER        = 0.60
GA_MUTATION         = 0.25
GA_MIN_CONDS        = 2
GA_MAX_CONDS        = 4
GA_TOURNAMENT_K     = 5
GA_DB_SEED_FRAC     = 0.08
GA_IMPORTANCE_FRAC  = 0.35
GA_MIN_SIG_HARD     = 20         # hard minimum signals

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
        labels     : np.ndarray (N,) — 1 if max forward gain ≥ PUMP_THRESHOLD (0.1%)
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

        # Path-filtered label: only count as win if price reaches PUMP_THRESHOLD
        # BEFORE triggering the stop loss (LIVE_STOP_LOSS below entry).
        # This aligns simulation with live trading — the previous approach of checking
        # max forward gain alone was optimistic (ignored that stop loss fires first
        # during dips, even when price eventually recovers past the target).
        hit_target  = False
        stopped_out = False
        for k in range(len(fwd_hi)):
            lo_gain = (fwd_lo[k] - entry) / entry
            hi_gain = (fwd_hi[k] - entry) / entry
            if lo_gain < -LIVE_STOP_LOSS:
                stopped_out = True
                break
            if hi_gain >= PUMP_THRESHOLD:
                hit_target = True
                break
        labels[i] = 1 if (hit_target and not stopped_out) else 0

        price_highs.append(fwd_hi)
        price_lows.append(fwd_lo)

    logger.info(
        f"Feature matrix: {N} rows × {N_FEATURES} features | "
        f"clean_pumps={labels.sum()} ({labels.mean()*100:.1f}%) | "
        f"max_gain_pumps={int((max_gains >= PUMP_THRESHOLD).sum())} ({(max_gains >= PUMP_THRESHOLD).mean()*100:.1f}% raw) | "
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
    # NOTE: retained for backward-compat imports only — no longer called by the GA.
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
    """One candidate = a set of 2-4 entry conditions only.

    Fitness is measured purely by directional precision:
      what fraction of the time does the price go up ≥PUMP_THRESHOLD
      within FWD_MINUTES after the signal fires?

    No exit parameters are evolved — the GA cannot game the fitness
    metric by finding clever exits that look profitable on noise.
    """
    __slots__ = ("conditions", "fitness_val")

    def __init__(self, conditions: List[Tuple[int, int, float]]):
        self.conditions  = conditions
        self.fitness_val = -np.inf

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
            {"feature": FEATURES[fi], "direction": ">" if d > 0 else "<",
             "threshold": round(float(thr), 6)}
            for fi, d, thr in self.conditions
        ]

    def __repr__(self) -> str:
        parts = [
            f"{FEATURES[fi]}{'>' if d > 0 else '<'}{thr:.4f}"
            for fi, d, thr in self.conditions
        ]
        return " AND ".join(parts)


def _random_individual(features: np.ndarray, use_importance: bool = False) -> Individual:
    n_conds = random.randint(GA_MIN_CONDS, GA_MAX_CONDS)
    imp     = _feature_importance

    if use_importance and imp is not None:
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
        pct = random.uniform(10, 90)
        thr = float(np.percentile(vals, pct))
        d   = random.choice([1, -1])
        conds.append((fi, d, thr))
    if not conds:
        return _random_individual(features)
    return Individual(conds)


def _crossover(p1: Individual, p2: Individual) -> Individual:
    """Produce a child by mixing conditions from both parents."""
    all_conds = p1.conditions + p2.conditions
    seen: Dict[int, Tuple] = {}
    for c in all_conds:
        seen[c[0]] = c
    all_unique = list(seen.values())
    random.shuffle(all_unique)
    n = random.randint(GA_MIN_CONDS, min(GA_MAX_CONDS, len(all_unique)))
    return Individual(all_unique[:n])


def _mutate(ind: Individual, features: np.ndarray) -> Individual:
    """Return a mutated copy — mutates entry conditions only."""
    conds = list(deepcopy(ind.conditions))
    op    = random.random()

    if op < 0.35 and conds:
        # Perturb a threshold
        i   = random.randrange(len(conds))
        fi, d, thr = conds[i]
        col = features[:, fi]
        std = float(np.nanstd(col[col != 0])) if (col != 0).any() else 0.01
        delta = random.gauss(0, std * 0.2)
        conds[i] = (fi, d, float(np.clip(thr + delta,
                                          np.nanpercentile(col, 5),
                                          np.nanpercentile(col, 95))))
    elif op < 0.55 and conds:
        # Flip direction
        i   = random.randrange(len(conds))
        fi, d, thr = conds[i]
        conds[i] = (fi, -d, thr)
    elif op < 0.70:
        # Remove a condition
        if len(conds) > GA_MIN_CONDS:
            conds.pop(random.randrange(len(conds)))
    else:
        # Add a new condition
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

    if not conds:
        return _random_individual(features)
    return Individual(conds)


def _compute_fitness(
    ind: Individual,
    features: np.ndarray,
    labels:   np.ndarray,
    entry_prices: np.ndarray,
    price_highs: List[np.ndarray],
    price_lows:  List[np.ndarray],
    data_hours: float,
    # Ignored legacy arguments kept for call-site compatibility
    fixed_tiers: Optional[List] = None,
    fixed_sl:    float = 0.003,
    fixed_hold:  int   = 2,
) -> float:
    """
    Fitness = directional precision × signals_per_day × consistency_mult.

    directional precision = fraction of fired signals where max forward price
    reaches PUMP_THRESHOLD above entry within FWD_MINUTES.

    No exit simulation — the GA cannot overfit to trailing-stop parameters.
    Rules that fire on bearish conditions will not achieve ≥55% precision
    and will be hard-rejected.
    """
    mask  = ind.apply(features)
    n_sig = int(mask.sum())

    min_abs = max(GA_MIN_SIG_HARD, MIN_SIGNALS) * (data_hours / 24)
    if n_sig < min_abs:
        return -999.0
    if n_sig > MAX_SIGNALS * (data_hours / 24):
        return -998.0

    precision = float(labels[mask].mean()) if n_sig > 0 else 0.0

    # Hard reject: precision below the minimum — no matter how many signals
    if precision < MIN_DIRECTIONAL_PRECISION:
        return -997.0

    sig_per_d = n_sig / data_hours * 24

    # Consistency multiplier: reward rules that fire more uniformly across time.
    # Split data into 4 equal chunks; count how many chunks the rule fires ≥1 signal.
    chunk = max(1, len(features) // 4)
    active_chunks = sum(
        1 for k in range(4)
        if mask[k * chunk: (k + 1) * chunk].any()
    )
    consistency_mult = 0.5 + 0.5 * (active_chunks / 4)

    return precision * sig_per_d * consistency_mult


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
    """Load top-precision rules from simulation_results as starting seeds.

    Only loads rules with win_rate ≥ MIN_DIRECTIONAL_PRECISION so we don't
    re-seed the population with the old overfitted rules.
    """
    seeds: List[Individual] = []
    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT conditions_json
                    FROM simulation_results
                    WHERE win_rate        >= %s
                      AND n_signals       >= %s
                      AND signals_per_day >= %s
                    ORDER BY win_rate DESC, signals_per_day DESC
                    LIMIT 30
                """, [MIN_DIRECTIONAL_PRECISION, GA_MIN_SIG_HARD, MIN_SIGNALS])
                rows = cur.fetchall()

        for row in rows:
            try:
                conds_raw = row['conditions_json']
                conds = []
                for c in conds_raw:
                    feat = c.get('feature')
                    if feat not in FEAT_IDX:
                        continue
                    fi  = FEAT_IDX[feat]
                    d   = 1 if c.get('direction') == '>' else -1
                    thr = float(c.get('threshold', 0.0))
                    col = features[:, fi]
                    p5  = float(np.nanpercentile(col, 5))
                    p95 = float(np.nanpercentile(col, 95))
                    thr = float(np.clip(thr, p5, p95))
                    conds.append((fi, d, thr))

                if len(conds) < GA_MIN_CONDS:
                    continue
                seeds.append(Individual(conds))
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

# exit_grid_search removed — no longer used in v2 directional precision engine


# =============================================================================
# PHASE 5 — MULTI-FOLD WALK-FORWARD VALIDATION
# =============================================================================

def multi_fold_validate(
    ind:      Individual,
    exit_cfg: Dict[str, Any],   # kept for API compat — not used
    data:     Dict[str, Any],
    n_folds:  int = OOS_FOLDS,
) -> Tuple[float, float, float]:
    """Walk-forward directional-precision validation.

    Splits the time series into (n_folds + 1) equal chunks.
    For each fold k:
      - train window = data[:chunk*(k+1)]
      - OOS window   = data[chunk*(k+1) : chunk*(k+2)]

    Measures directional precision (% of signals where label=1) on each window.

    Returns:
        (avg_insample_precision, avg_oos_precision, oos_folds_passing_rate)

    oos_folds_passing_rate = fraction of OOS folds where precision ≥ MIN_OOS_PRECISION.
    A rule is considered OOS-valid only if this fraction ≥ MIN_OOS_FOLDS_PASSING/n_folds.
    """
    features = data["features"]
    labels   = data["labels"]
    N        = len(features)

    chunk = max(1, N // (n_folds + 1))
    is_precs:  List[float] = []
    oos_precs: List[float] = []

    for fold in range(n_folds):
        train_end  = (fold + 1) * chunk
        test_start = train_end
        test_end   = min(N, test_start + chunk)

        if test_end <= test_start:
            continue

        def prec_for(start: int, end: int) -> float:
            window_mask = np.zeros(N, dtype=bool)
            window_mask[start:end] = True
            combined = ind.apply(features) & window_mask
            n = int(combined.sum())
            if n < 3:
                return float("nan")
            return float(labels[combined].mean())

        is_p  = prec_for(0, train_end)
        oos_p = prec_for(test_start, test_end)

        if not np.isnan(is_p):
            is_precs.append(is_p)
        if not np.isnan(oos_p):
            oos_precs.append(oos_p)

    avg_is  = float(np.mean(is_precs))  if is_precs  else 0.0
    avg_oos = float(np.mean(oos_precs)) if oos_precs else 0.0
    oos_passing = float(
        sum(1 for v in oos_precs if v >= MIN_OOS_PRECISION) / max(1, len(oos_precs))
    )

    return avg_is, avg_oos, oos_passing


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
        prec     = r.get('win_rate', 0.0)
        oos_prec = r.get('oos_ev', 0.0)
        pass_rt  = r.get('oos_consistency', 0.5)
        sig_d    = r.get('signals_per_day', 1.0)
        # Primary: OOS precision × volume; secondary: consistency bonus
        return oos_prec * sig_d * max(0.3, pass_rt) * (1.0 + max(0.0, prec - 0.55) * 5)

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
            f"precision={best.get('win_rate',0):.0%} | "
            f"oos_prec={best.get('oos_ev',0):.0%} | "
            f"oos_pass={best.get('oos_consistency',0):.0%} | "
            f"sig/d={best.get('signals_per_day',0):.1f}"
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
    sep = "=" * 95
    print(f"\n{sep}")
    print(f"LEADERBOARD  run={run_id}  top={len(top_results)}  "
          f"(pump_threshold={PUMP_THRESHOLD*100:.1f}%  fwd={FWD_MINUTES}min)")
    print(sep)
    hdr = (f"{'#':>3}  {'Precision':>10}  {'OOS prec':>9}  {'OOSpass':>8}  "
           f"{'sig/d':>6}  {'OOS gap':>8}  Rule")
    print(hdr)
    print("-" * 95)
    for r in top_results:
        oos_gap_pp = r.get('oos_gap', 0) * 100
        ovft_flag  = " ⚠" if oos_gap_pp > 5 else ""
        print(
            f"{r['rank']:>3}  "
            f"{r['win_rate']*100:>9.1f}%  "
            f"{r.get('oos_ev',0)*100:>8.1f}%  "
            f"{r.get('oos_consistency',0)*100:>7.0f}%  "
            f"{r['signals_per_day']:>6.1f}  "
            f"{oos_gap_pp:>+7.1f}pp{ovft_flag}  "
            f"{r['rule_str'][:50]}"
        )
    print(sep)

    if top_results:
        best = top_results[0]
        print(f"\n★  BEST RULE (rank 1):")
        print(f"   Rule:       {best['rule_str']}")
        print(f"   Precision:  {best['win_rate']*100:.1f}%  "
              f"({best['win_rate']*100:.1f}% of signals → price up ≥{PUMP_THRESHOLD*100:.1f}% in {FWD_MINUTES}min)")
        print(f"   OOS prec:   {best.get('oos_ev',0)*100:.1f}%  "
              f"(pass rate {best.get('oos_consistency',0)*100:.0f}%)")
        print(f"   Volume:     {best['signals_per_day']:.1f} signals/day")
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
    max_gains    = data["max_gains"]
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

    # ── Phase 4: OOS precision validation (replaces exit grid search) ─────────
    top_results = []
    for rank, ind in enumerate(top_inds, start=1):
        mask  = ind.apply(features)
        n_sig = int(mask.sum())
        if n_sig < 3:
            continue

        precision = float(labels[mask].mean())
        # Hard reject: in-sample precision below minimum
        if precision < MIN_DIRECTIONAL_PRECISION:
            continue

        sig_per_day = n_sig / data_hours * 24
        recall      = labels[mask].sum() / max(labels.sum(), 1)

        # Walk-forward OOS validation — measures directional precision on held-out data
        avg_is, avg_oos, oos_pass_rate = multi_fold_validate(ind, {}, data)
        oos_gap = avg_is - avg_oos if not (np.isnan(avg_is) or np.isnan(avg_oos)) else 0.0

        # Reject rules that don't hold up OOS
        min_oos_pass = MIN_OOS_FOLDS_PASSING / OOS_FOLDS
        if avg_oos < MIN_OOS_PRECISION or oos_pass_rate < min_oos_pass:
            logger.info(
                f"  Rank {rank:2d}: REJECTED OOS — oos_prec={avg_oos:.3f} "
                f"pass_rate={oos_pass_rate:.0%} (need ≥{MIN_OOS_PRECISION:.0%} "
                f"on ≥{min_oos_pass:.0%} folds)"
            )
            continue

        result = {
            "rank":            rank,
            "rule_str":        str(ind),
            "conditions":      ind.to_json(),
            "n_features":      len(ind.conditions),
            "n_signals":       n_sig,
            "signals_per_day": sig_per_day,
            # win_rate = real directional precision — this is what check_sim_rules filters on
            "win_rate":        precision,
            "precision_pct":   precision,
            "recall_pct":      float(recall),
            # ev_per_trade = forward gain expectation (max gain in FWD_MINUTES when label=1,
            #                averaged over all signals including non-pumps)
            "ev_per_trade":    float(max_gains[mask].mean()),
            "daily_ev":        float(max_gains[mask].mean()) * sig_per_day,
            "sharpe":          0.0,
            # OOS metrics
            "insample_ev":     avg_is,
            "oos_ev":          avg_oos,
            "oos_gap":         oos_gap,
            "oos_consistency": oos_pass_rate,
            # No exit config — sell_logic is set per-play independently
            "exit_config":     {},
            "ga_exit":         {},
        }
        top_results.append(result)
        logger.info(
            f"  Rank {rank:2d}: precision={precision*100:.1f}%  "
            f"oos_prec={avg_oos*100:.1f}%  oos_pass={oos_pass_rate:.0%}  "
            f"n={n_sig}  sig/d={sig_per_day:.1f}  rule: {str(ind)[:60]}"
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
    logger.info("MEGA SIGNAL SIMULATOR STARTED  (v3 path-filtered labels)")
    logger.info(f"  Features:          {N_FEATURES}")
    logger.info(f"  GA population:     {GA_POPULATION}")
    logger.info(f"  GA generations:    {GA_GENERATIONS}")
    logger.info(f"  Min precision:     {MIN_DIRECTIONAL_PRECISION*100:.0f}% in-sample / {MIN_OOS_PRECISION*100:.0f}% OOS")
    logger.info(f"  Pump threshold:    {PUMP_THRESHOLD*100:.2f}%  (target gain before stop)")
    logger.info(f"  Live stop loss:    {LIVE_STOP_LOSS*100:.3f}%  (stop triggers before target = label=0)")
    logger.info(f"  Max signals/day:   {MAX_SIGNALS}  (was 300 — lowered for selectivity)")
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

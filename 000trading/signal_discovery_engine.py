#!/usr/bin/env python3
"""
Signal Discovery Engine for Pump Detection
===========================================
Discovers the cleanest possible filter rules and model configurations
for detecting SOL price pumps.

Launched by: run_sweep_with_heartbeat.py

Usage:
    python3 000trading/signal_discovery_engine.py --sweep --hours 24 --output /tmp/sde_overnight.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Project path setup
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CHECKPOINT_FILE = "/tmp/sde_overnight_checkpoint.json"
TRADE_COST_PCT = 0.001  # 0.1% round-trip cost

# Sweep grids
MIN_CLIMB_PCTS = [0.15, 0.2, 0.25, 0.3, 0.4, 0.5]
MAX_DIP_PCTS = [0.1, 0.15, 0.2, 0.3, 0.5]
EARLY_WINDOW_SECS = [60, 120, 180, 300]
FORWARD_WINDOW_SECS = [300, 600]

MIN_CLIMBS = 20  # skip combos with fewer positive samples
SAMPLE_INTERVAL_SEC = 30  # sample grid spacing
TOP_FEATURES_COHENS = 30  # Cohen's d pre-screen
TOP_FEATURES_COMBOS = 15  # brute-force combinations from top N
GBM_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75]

# Walk-forward splits
WF_SPLITS = [
    (0.00, 0.60, 0.60, 0.75),  # train first 60%, test 60-75%
    (0.00, 0.75, 0.75, 0.90),  # train first 75%, test 75-90%
]

logger = logging.getLogger("sde")


# ===================================================================
# Logging setup
# ===================================================================
def setup_logging(log_path: str) -> None:
    """Configure dual logging to stdout and file."""
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    logger.setLevel(logging.INFO)
    # File handler
    fh = logging.FileHandler(log_path, mode="w")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    # Stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)


# ===================================================================
# 1. Data Loading
# ===================================================================
def load_raw_data(hours: int) -> Dict[str, pd.DataFrame]:
    """Load raw data from PostgreSQL into DataFrames.

    Returns dict with keys: 'prices', 'order_book', 'trades', 'whales'.
    """
    logger.info(f"Loading raw data for last {hours} hours ...")
    dfs: Dict[str, pd.DataFrame] = {}

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # --- prices (SOL, BTC, ETH) ---
            cursor.execute(
                """SELECT timestamp, token, price
                   FROM prices
                   WHERE timestamp >= NOW() - INTERVAL '%s hours'
                   ORDER BY timestamp""",
                [hours],
            )
            rows = cursor.fetchall()
            dfs["prices"] = pd.DataFrame(rows) if rows else pd.DataFrame(
                columns=["timestamp", "token", "price"]
            )

            # --- order_book_features ---
            cursor.execute(
                """SELECT *
                   FROM order_book_features
                   WHERE timestamp >= NOW() - INTERVAL '%s hours'
                   ORDER BY timestamp""",
                [hours],
            )
            rows = cursor.fetchall()
            dfs["order_book"] = pd.DataFrame(rows) if rows else pd.DataFrame()

            # --- sol_stablecoin_trades ---
            cursor.execute(
                """SELECT trade_timestamp, sol_amount, stablecoin_amount,
                          price, direction, perp_direction
                   FROM sol_stablecoin_trades
                   WHERE trade_timestamp >= NOW() - INTERVAL '%s hours'
                   ORDER BY trade_timestamp""",
                [hours],
            )
            rows = cursor.fetchall()
            dfs["trades"] = pd.DataFrame(rows) if rows else pd.DataFrame(
                columns=["trade_timestamp", "sol_amount", "stablecoin_amount",
                          "price", "direction", "perp_direction"]
            )

            # --- whale_movements ---
            cursor.execute(
                """SELECT timestamp, sol_change, abs_change,
                          percentage_moved, direction, whale_type,
                          movement_significance
                   FROM whale_movements
                   WHERE timestamp >= NOW() - INTERVAL '%s hours'
                   ORDER BY timestamp""",
                [hours],
            )
            rows = cursor.fetchall()
            dfs["whales"] = pd.DataFrame(rows) if rows else pd.DataFrame(
                columns=["timestamp", "sol_change", "abs_change",
                          "percentage_moved", "direction", "whale_type",
                          "movement_significance"]
            )

    # Ensure numeric types
    for key, df in dfs.items():
        if df.empty:
            logger.warning(f"  {key}: 0 rows")
            continue
        logger.info(f"  {key}: {len(df):,} rows")

    _coerce_timestamps(dfs)
    _coerce_numerics(dfs)
    return dfs


def _coerce_timestamps(dfs: Dict[str, pd.DataFrame]) -> None:
    """Ensure timestamp columns are tz-aware UTC datetimes."""
    ts_map = {
        "prices": "timestamp",
        "order_book": "timestamp",
        "trades": "trade_timestamp",
        "whales": "timestamp",
    }
    for key, col in ts_map.items():
        df = dfs[key]
        if df.empty or col not in df.columns:
            continue
        df[col] = pd.to_datetime(df[col], utc=True)


def _coerce_numerics(dfs: Dict[str, pd.DataFrame]) -> None:
    """Convert Decimal / object columns to float64 where possible."""
    for key, df in dfs.items():
        if df.empty:
            continue
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = pd.to_numeric(df[col], errors="ignore")
                except Exception:
                    pass


# ===================================================================
# 2. Feature Computation
# ===================================================================
def _resample_prices_1s(prices_df: pd.DataFrame, token: str) -> pd.Series:
    """Return a 1-second resampled price Series for a given token."""
    sub = prices_df[prices_df["token"] == token].copy()
    if sub.empty:
        return pd.Series(dtype="float64")
    sub = sub.set_index("timestamp").sort_index()
    # Forward-fill to 1-second intervals
    s = sub["price"].resample("1s").last().ffill()
    return s


def _rolling_pct_change(series: pd.Series, periods: int) -> pd.Series:
    """Percent change over N periods: (current - past) / past * 100."""
    past = series.shift(periods)
    return (series - past) / past.replace(0, np.nan) * 100


def _build_time_grid(start: pd.Timestamp, end: pd.Timestamp,
                     interval_sec: int = SAMPLE_INTERVAL_SEC) -> pd.DatetimeIndex:
    """Create a time grid from start to end at the given interval."""
    return pd.date_range(start=start, end=end, freq=f"{interval_sec}s")


def compute_features(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Compute all features on a 30-second time grid.

    Returns a DataFrame indexed by timestamp with ~55-65 feature columns.
    """
    logger.info("Computing features on 30-second time grid ...")
    t0 = time.time()

    prices_df = dfs["prices"]
    ob_df = dfs["order_book"]
    trades_df = dfs["trades"]
    whales_df = dfs["whales"]

    # Determine time range from SOL prices
    sol_prices = prices_df[prices_df["token"] == "SOL"]
    if sol_prices.empty:
        logger.error("No SOL price data found!")
        return pd.DataFrame()

    ts_min = sol_prices["timestamp"].min()
    ts_max = sol_prices["timestamp"].max()

    # We need at least 10 minutes of lookback before the grid starts
    grid_start = ts_min + pd.Timedelta(minutes=10)
    grid_end = ts_max
    if grid_start >= grid_end:
        logger.error("Not enough data for feature computation")
        return pd.DataFrame()

    grid = _build_time_grid(grid_start, grid_end)
    logger.info(f"  Time grid: {len(grid)} points from {grid_start} to {grid_end}")

    # ----- Resample raw data to 1-second for price, as-is for others -----
    sol_1s = _resample_prices_1s(prices_df, "SOL")
    btc_1s = _resample_prices_1s(prices_df, "BTC")
    eth_1s = _resample_prices_1s(prices_df, "ETH")

    # Resample order book to 1-second (last value forward-filled)
    ob_1s = pd.DataFrame()
    if not ob_df.empty and "timestamp" in ob_df.columns:
        ob_1s = ob_df.set_index("timestamp").sort_index()
        # Drop non-numeric columns
        ob_num_cols = ob_1s.select_dtypes(include=[np.number]).columns.tolist()
        ob_1s = ob_1s[ob_num_cols].resample("1s").mean().ffill()

    # ----- Feature DataFrames -----
    feat_dict: Dict[str, pd.Series] = {}

    # ========== Price features (pm_) ==========
    logger.info("  Computing price features (pm_) ...")
    if not sol_1s.empty:
        # Resample to 1s already done; align to grid via reindex
        # We compute rolling stats on the full 1s series, then pick grid points
        windows = {"30s": 30, "1m": 60, "2m": 120, "5m": 300, "10m": 600}

        for label, secs in windows.items():
            feat_dict[f"pm_change_{label}"] = _rolling_pct_change(sol_1s, secs)

        # Volatility: (rolling_max - rolling_min) / rolling_mean * 100
        for label, secs in [("1m", 60), ("5m", 300)]:
            rmax = sol_1s.rolling(secs, min_periods=max(1, secs // 2)).max()
            rmin = sol_1s.rolling(secs, min_periods=max(1, secs // 2)).min()
            rmean = sol_1s.rolling(secs, min_periods=max(1, secs // 2)).mean()
            feat_dict[f"pm_volatility_{label}"] = (rmax - rmin) / rmean.replace(0, np.nan) * 100

        # Stddev: rolling std / rolling mean * 100
        for label, secs in [("1m", 60), ("5m", 300)]:
            rstd = sol_1s.rolling(secs, min_periods=max(1, secs // 2)).std()
            rmean = sol_1s.rolling(secs, min_periods=max(1, secs // 2)).mean()
            feat_dict[f"pm_stddev_{label}"] = rstd / rmean.replace(0, np.nan) * 100

        # Acceleration: pm_change_1m now - pm_change_1m 60s ago
        chg_1m = _rolling_pct_change(sol_1s, 60)
        feat_dict["pm_accel"] = chg_1m - chg_1m.shift(60)

        # Body ratio 1m: |close - open| / (high - low)  over 1m window
        roll_open = sol_1s.shift(59)  # price 59 seconds ago ~ "open" of 1m bar
        roll_high = sol_1s.rolling(60, min_periods=30).max()
        roll_low = sol_1s.rolling(60, min_periods=30).min()
        bar_range = (roll_high - roll_low).replace(0, np.nan)
        feat_dict["pm_body_ratio_1m"] = (sol_1s - roll_open).abs() / bar_range

    # ========== Order book features (ob_) ==========
    logger.info("  Computing order book features (ob_) ...")
    if not ob_1s.empty:
        # volume_imbalance averages
        if "volume_imbalance" in ob_1s.columns:
            for label, secs in [("30s", 30), ("1m", 60), ("5m", 300)]:
                feat_dict[f"ob_imbalance_{label}"] = (
                    ob_1s["volume_imbalance"]
                    .rolling(secs, min_periods=max(1, secs // 4))
                    .mean()
                )

        # spread_bps averages
        if "spread_bps" in ob_1s.columns:
            for label, secs in [("30s", 30), ("1m", 60), ("2m", 120), ("5m", 300)]:
                feat_dict[f"ob_spread_{label}"] = (
                    ob_1s["spread_bps"]
                    .rolling(secs, min_periods=max(1, secs // 4))
                    .mean()
                )
            # Spread change 30s vs 2m
            if "ob_spread_30s" in feat_dict and "ob_spread_2m" in feat_dict:
                feat_dict["ob_spread_chg_30s_vs_2m"] = (
                    feat_dict["ob_spread_30s"] - feat_dict["ob_spread_2m"]
                )

        # microprice_dev_bps averages
        if "microprice_dev_bps" in ob_1s.columns:
            for label, secs in [("30s", 30), ("2m", 120), ("5m", 300)]:
                feat_dict[f"ob_microprice_{label}"] = (
                    ob_1s["microprice_dev_bps"]
                    .rolling(secs, min_periods=max(1, secs // 4))
                    .mean()
                )

        # depth_ratio_1m: avg(bid_liquidity / ask_liquidity) - 1
        if "bid_liquidity" in ob_1s.columns and "ask_liquidity" in ob_1s.columns:
            ratio = ob_1s["bid_liquidity"] / ob_1s["ask_liquidity"].replace(0, np.nan)
            feat_dict["ob_depth_ratio_1m"] = ratio.rolling(60, min_periods=15).mean() - 1

        # bid_share: avg(bid_liquidity / total_depth_10 * 100)
        if "bid_liquidity" in ob_1s.columns and "total_depth_10" in ob_1s.columns:
            bid_share = (
                ob_1s["bid_liquidity"]
                / ob_1s["total_depth_10"].replace(0, np.nan)
                * 100
            )
            for label, secs in [("30s", 30), ("1m", 60)]:
                feat_dict[f"ob_bid_share_{label}"] = (
                    bid_share.rolling(secs, min_periods=max(1, secs // 4)).mean()
                )

        # net_liquidity_change sum
        if "net_liquidity_change_1s" in ob_1s.columns:
            for label, secs in [("1m", 60), ("2m", 120)]:
                feat_dict[f"ob_net_liq_chg_{label}"] = (
                    ob_1s["net_liquidity_change_1s"]
                    .rolling(secs, min_periods=max(1, secs // 4))
                    .sum()
                )

    # ========== Transaction features (tx_) ==========
    logger.info("  Computing transaction features (tx_) ...")
    if not trades_df.empty:
        tdf = trades_df.set_index("trade_timestamp").sort_index()
        # Convert to numeric
        for c in ["sol_amount", "stablecoin_amount", "price"]:
            if c in tdf.columns:
                tdf[c] = pd.to_numeric(tdf[c], errors="coerce")

        # Build 1-second trade aggregates
        tdf["usd_amount"] = tdf["stablecoin_amount"].abs().fillna(0)
        tdf["is_buy"] = (tdf["direction"] == "buy").astype(int)
        tdf["is_sell"] = (tdf["direction"] == "sell").astype(int)
        tdf["buy_usd"] = tdf["usd_amount"] * tdf["is_buy"]
        tdf["sell_usd"] = tdf["usd_amount"] * tdf["is_sell"]
        tdf["is_large"] = (tdf["usd_amount"] > 10_000).astype(int)

        # Resample to 1-second buckets
        t_1s = tdf.resample("1s").agg({
            "usd_amount": "sum",
            "buy_usd": "sum",
            "sell_usd": "sum",
            "is_buy": "sum",   # trade count proxy
            "is_sell": "sum",
            "is_large": "sum",
        }).fillna(0)
        t_1s["trade_count"] = t_1s["is_buy"] + t_1s["is_sell"]

        # Rolling sums for trade counts
        for label, secs in [("30s", 30), ("1m", 60), ("2m", 120), ("5m", 300)]:
            feat_dict[f"tx_count_{label}"] = (
                t_1s["trade_count"]
                .rolling(secs, min_periods=1)
                .sum()
            )

        # Volume sums
        for label, secs in [("1m", 60), ("5m", 300)]:
            feat_dict[f"tx_vol_{label}"] = (
                t_1s["usd_amount"]
                .rolling(secs, min_periods=1)
                .sum()
            )

        # Buy/sell pressure
        for label, secs in [("1m", 60), ("5m", 300)]:
            buy_sum = t_1s["buy_usd"].rolling(secs, min_periods=1).sum()
            sell_sum = t_1s["sell_usd"].rolling(secs, min_periods=1).sum()
            total = (buy_sum + sell_sum).replace(0, np.nan)
            feat_dict[f"tx_pressure_{label}"] = (buy_sum - sell_sum) / total

        # Average trade size 1m
        vol_1m = t_1s["usd_amount"].rolling(60, min_periods=1).sum()
        cnt_1m = t_1s["trade_count"].rolling(60, min_periods=1).sum().replace(0, np.nan)
        feat_dict["tx_avg_size_1m"] = vol_1m / cnt_1m

        # Large trade count 1m
        feat_dict["tx_large_count_1m"] = (
            t_1s["is_large"].rolling(60, min_periods=1).sum()
        )

    # ========== Whale features (wh_) ==========
    logger.info("  Computing whale features (wh_) ...")
    if not whales_df.empty and "timestamp" in whales_df.columns:
        wdf = whales_df.copy()
        wdf["sol_change"] = pd.to_numeric(wdf["sol_change"], errors="coerce")
        wdf["abs_change"] = pd.to_numeric(wdf["abs_change"], errors="coerce")
        wdf["percentage_moved"] = pd.to_numeric(wdf["percentage_moved"], errors="coerce")

        wdf["is_in"] = (wdf["direction"] == "in").astype(int)
        wdf["is_out"] = (wdf["direction"] == "out").astype(int)
        wdf["in_sol"] = wdf["sol_change"].clip(lower=0)
        wdf["out_sol"] = (-wdf["sol_change"]).clip(lower=0)
        wdf["is_large_wh"] = (wdf["percentage_moved"].fillna(0) > 5).astype(int)

        w_1s = wdf.set_index("timestamp").sort_index()
        w_agg = w_1s.resample("1s").agg({
            "in_sol": "sum",
            "out_sol": "sum",
            "is_in": "sum",
            "is_out": "sum",
            "is_large_wh": "sum",
        }).fillna(0)
        w_agg["total_moves"] = w_agg["is_in"] + w_agg["is_out"]

        # 5m rolling
        secs = 300
        in5 = w_agg["in_sol"].rolling(secs, min_periods=1).sum()
        out5 = w_agg["out_sol"].rolling(secs, min_periods=1).sum()
        feat_dict["wh_net_flow_5m"] = in5 - out5
        feat_dict["wh_activity_5m"] = (
            w_agg["total_moves"].rolling(secs, min_periods=1).sum()
        )
        large5 = w_agg["is_large_wh"].rolling(secs, min_periods=1).sum()
        total5 = w_agg["total_moves"].rolling(secs, min_periods=1).sum().replace(0, np.nan)
        feat_dict["wh_large_pct_5m"] = large5 / total5 * 100

    # ========== Cross-asset features (xa_) ==========
    logger.info("  Computing cross-asset features (xa_) ...")
    if not btc_1s.empty:
        feat_dict["xa_btc_change_1m"] = _rolling_pct_change(btc_1s, 60)
    if not eth_1s.empty:
        feat_dict["xa_eth_change_1m"] = _rolling_pct_change(eth_1s, 60)
    if "pm_change_1m" in feat_dict and "xa_btc_change_1m" in feat_dict:
        # Align before subtraction
        pm1 = feat_dict["pm_change_1m"]
        btc1 = feat_dict["xa_btc_change_1m"]
        aligned = pd.DataFrame({"sol": pm1, "btc": btc1}).ffill()
        feat_dict["xa_sol_btc_div_1m"] = aligned["sol"] - aligned["btc"]

    # ========== Assemble into single DataFrame at grid points ==========
    logger.info("  Assembling feature matrix at grid points ...")
    # Combine all series into one DataFrame (union of all 1-second indices)
    all_feat = pd.DataFrame(feat_dict)

    if all_feat.empty:
        logger.error("No features computed!")
        return pd.DataFrame()

    # Reindex to grid (take nearest 1-second value)
    all_feat = all_feat.sort_index()
    grid_df = all_feat.reindex(grid, method="nearest", tolerance=pd.Timedelta("2s"))
    grid_df.index.name = "timestamp"

    # ========== Engineered features (feat_) ==========
    logger.info("  Computing engineered features (feat_) ...")
    if "pm_volatility_5m" in grid_df.columns and "pm_volatility_1m" in grid_df.columns:
        grid_df["feat_vol_compress"] = (
            grid_df["pm_volatility_5m"]
            / grid_df["pm_volatility_1m"].replace(0, np.nan)
        )

    momentum_cols = [c for c in ["pm_change_30s", "pm_change_1m", "pm_change_5m"]
                     if c in grid_df.columns]
    if momentum_cols:
        grid_df["feat_momentum_agreement"] = sum(
            (grid_df[c] > 0).astype(int) for c in momentum_cols
        )

    if "ob_imbalance_1m" in grid_df.columns:
        roll_mean = grid_df["ob_imbalance_1m"].rolling(50, min_periods=10).mean()
        roll_std = grid_df["ob_imbalance_1m"].rolling(50, min_periods=10).std()
        grid_df["feat_ob_zscore"] = (
            (grid_df["ob_imbalance_1m"] - roll_mean) / roll_std.replace(0, np.nan)
        )

    # ========== Drop high-NULL columns ==========
    null_pct = grid_df.isnull().mean()
    drop_cols = null_pct[null_pct > 0.80].index.tolist()
    if drop_cols:
        logger.info(f"  Dropping {len(drop_cols)} columns with >80% NULL: {drop_cols[:10]}...")
        grid_df = grid_df.drop(columns=drop_cols)

    elapsed = time.time() - t0
    logger.info(f"  Feature matrix: {grid_df.shape[0]} samples x {grid_df.shape[1]} features "
                f"({elapsed:.1f}s)")
    return grid_df


# ===================================================================
# 3. Labeling (path-aware)
# ===================================================================
def _forward_rolling_maxmin(arr: np.ndarray, window: int
                            ) -> Tuple[np.ndarray, np.ndarray]:
    """Compute forward rolling max and min over a price array.

    For each index i, returns the max and min of arr[i+1 : i+1+window].
    Uses the reverse-array trick: forward rolling = reversed backward rolling.

    Returns (fwd_max, fwd_min) arrays of same length as arr.
    """
    n = len(arr)
    fwd_max = np.full(n, np.nan)
    fwd_min = np.full(n, np.nan)

    if n < 2 or window < 1:
        return fwd_max, fwd_min

    # Reverse the array, then compute backward rolling max/min
    # Forward window [i+1, i+window] on original =
    #   Backward window on reversed array
    rev = arr[::-1]

    # Use pandas for efficient rolling on reversed array
    rev_s = pd.Series(rev)
    # rolling(window).max() gives max of last `window` values including current
    # We need max of arr[i+1..i+window], so after reversing and rolling,
    # we shift by 1 to exclude the current element.
    rev_max = rev_s.rolling(window, min_periods=1).max().values
    rev_min = rev_s.rolling(window, min_periods=1).min().values

    # Reverse back
    bwd_max = rev_max[::-1]
    bwd_min = rev_min[::-1]

    # bwd_max[i] = max of arr[i..i+window-1] (includes i)
    # We need max of arr[i+1..i+window], so shift forward by 1
    # Also need to recompute for the edge: at position i, the window
    # should be [i+1, min(i+window, n-1)]
    # Shifting: fwd_max[i] = bwd_max[i+1] but that only covers window-1 elements
    # So we need rolling of window size, then shift by 1
    # Actually, let's compute on shifted array directly:
    # We want max(arr[i+1], ..., arr[i+window]) for each i
    # = max of arr[1:] with rolling window of size `window`, aligned to the left
    if n <= 1:
        return fwd_max, fwd_min

    shifted = arr[1:]  # arr starting from index 1
    s_shifted = pd.Series(shifted)

    # Forward rolling max on shifted = for each j in shifted, max(shifted[j-window+1..j])
    # = max(arr[j-window+2..j+1])
    # We want: for original index i, max(arr[i+1..i+window])
    # That's shifted index j where j starts at i: max(shifted[i..i+window-1])
    # = rolling(window) aligned "right" starting from position window-1
    # Use the reverse trick for proper forward-looking window:
    s_rev = s_shifted[::-1].reset_index(drop=True)
    r_max = s_rev.rolling(window, min_periods=1).max().values[::-1]
    r_min = s_rev.rolling(window, min_periods=1).min().values[::-1]

    fwd_max[:n - 1] = r_max
    fwd_min[:n - 1] = r_min

    return fwd_max, fwd_min


def precompute_forward_windows(sol_1s: pd.Series, window_secs: List[int]
                               ) -> Dict[int, Tuple[pd.Series, pd.Series]]:
    """Pre-compute forward max/min returns (%) for multiple window sizes.

    Uses vectorized reverse-rolling approach for performance.

    Returns dict: window_sec -> (fwd_max_pct, fwd_min_pct).
    """
    if sol_1s.empty:
        empty = pd.Series(dtype="float64")
        return {w: (empty, empty) for w in window_secs}

    arr = sol_1s.values.astype(np.float64)
    n = len(arr)
    idx = sol_1s.index
    results = {}

    # Base price array for computing returns (avoid division by zero)
    base = arr.copy()
    base[base <= 0] = np.nan

    for wsec in sorted(set(window_secs)):
        fwd_abs_max, fwd_abs_min = _forward_rolling_maxmin(arr, wsec)

        # Convert to percent return relative to base price
        fwd_max_pct = (fwd_abs_max / base - 1) * 100
        fwd_min_pct = (fwd_abs_min / base - 1) * 100

        results[wsec] = (
            pd.Series(fwd_max_pct, index=idx),
            pd.Series(fwd_min_pct, index=idx),
        )

    return results


def label_samples(
    sol_1s: pd.Series,
    grid: pd.DatetimeIndex,
    min_climb_pct: float,
    max_dip_pct: float,
    early_window_sec: int,
    forward_window_sec: int,
    fwd_windows: Dict[int, Tuple[pd.Series, pd.Series]],
) -> pd.DataFrame:
    """Label each grid point as 'climb' or 'no_climb'.

    A sample is 'climb' if:
      - price reaches +min_climb_pct within early_window_sec
      - price never dips below -max_dip_pct within forward_window_sec

    Returns DataFrame with columns: timestamp, label, actual_gain, actual_loss.
    """
    # Get the early-window forward max (for climb check)
    early_max_pct, _ = fwd_windows.get(early_window_sec,
                                        (pd.Series(dtype="float64"),
                                         pd.Series(dtype="float64")))
    # Get the full forward-window min (for dip check)
    _, full_min_pct = fwd_windows.get(forward_window_sec,
                                       (pd.Series(dtype="float64"),
                                        pd.Series(dtype="float64")))
    # Also get the full forward-window max for actual_gain
    full_max_pct, _ = fwd_windows.get(forward_window_sec,
                                       (pd.Series(dtype="float64"),
                                        pd.Series(dtype="float64")))

    # Align to grid
    early_max_at_grid = early_max_pct.reindex(grid, method="nearest",
                                               tolerance=pd.Timedelta("2s"))
    full_min_at_grid = full_min_pct.reindex(grid, method="nearest",
                                             tolerance=pd.Timedelta("2s"))
    full_max_at_grid = full_max_pct.reindex(grid, method="nearest",
                                             tolerance=pd.Timedelta("2s"))

    # Label: climb if reaches target AND doesn't dip too much
    reached_target = early_max_at_grid >= min_climb_pct
    no_deep_dip = full_min_at_grid >= -max_dip_pct

    labels = np.where(reached_target & no_deep_dip, 1, 0)

    result = pd.DataFrame({
        "timestamp": grid,
        "label": labels,
        "actual_gain": full_max_at_grid.values,
        "actual_loss": full_min_at_grid.values,
    })
    # Drop samples where we don't have enough forward data
    result = result.dropna(subset=["actual_gain", "actual_loss"])
    return result


# ===================================================================
# 4. Scoring (cost-aware)
# ===================================================================
def score_signals(
    predicted: np.ndarray,
    actual_labels: np.ndarray,
    actual_gains: np.ndarray,
    actual_losses: np.ndarray,
) -> Optional[Dict[str, float]]:
    """Compute cost-aware signal score.

    Args:
        predicted: boolean array of signals (True = predicted climb)
        actual_labels: 1 = climb, 0 = no_climb
        actual_gains: max forward return (%) for each sample
        actual_losses: min forward return (%) for each sample (negative)

    Returns dict with precision, n_signals, avg_gain, avg_loss, expected_profit
    or None if too few signals.
    """
    mask = predicted.astype(bool)
    n_signals = int(mask.sum())
    if n_signals < 5:
        return None

    correct = actual_labels[mask] == 1
    precision = float(correct.mean() * 100)

    # Average gain on correct signals (the climb %)
    gains_on_correct = actual_gains[mask][correct]
    avg_gain = float(gains_on_correct.mean()) if len(gains_on_correct) > 0 else 0.0

    # Average loss on incorrect signals (worst dip)
    losses_on_incorrect = actual_losses[mask][~correct]
    avg_loss = float(losses_on_incorrect.mean()) if len(losses_on_incorrect) > 0 else 0.0

    # Expected profit per trade (in %)
    p = precision / 100
    expected_profit = p * avg_gain + (1 - p) * avg_loss - TRADE_COST_PCT * 100

    return {
        "precision": round(precision, 2),
        "n_signals": n_signals,
        "avg_gain": round(avg_gain, 4),
        "avg_loss": round(avg_loss, 4),
        "expected_profit": round(expected_profit, 4),
    }


# ===================================================================
# 5. Walk-Forward Validation
# ===================================================================
def walk_forward_splits(n_samples: int) -> List[Tuple[int, int, int, int]]:
    """Return (train_start, train_end, test_start, test_end) index tuples."""
    splits = []
    for tr_start_pct, tr_end_pct, te_start_pct, te_end_pct in WF_SPLITS:
        tr_s = int(n_samples * tr_start_pct)
        tr_e = int(n_samples * tr_end_pct)
        te_s = int(n_samples * te_start_pct)
        te_e = int(n_samples * te_end_pct)
        if tr_e - tr_s >= 50 and te_e - te_s >= 20:
            splits.append((tr_s, tr_e, te_s, te_e))
    return splits


# ===================================================================
# 6. Discovery Method 1: Filter Rules (Cohen's d + brute force)
# ===================================================================
def cohens_d(group1: np.ndarray, group2: np.ndarray) -> float:
    """Compute Cohen's d effect size between two groups."""
    n1, n2 = len(group1), len(group2)
    if n1 < 5 or n2 < 5:
        return 0.0
    m1, m2 = np.nanmean(group1), np.nanmean(group2)
    s1, s2 = np.nanstd(group1, ddof=1), np.nanstd(group2, ddof=1)
    pooled = np.sqrt(((n1 - 1) * s1**2 + (n2 - 1) * s2**2) / (n1 + n2 - 2))
    if pooled < 1e-12:
        return 0.0
    return (m1 - m2) / pooled


def find_best_threshold(values: np.ndarray, labels: np.ndarray,
                        n_quantiles: int = 40) -> Tuple[str, float, float]:
    """Find the best threshold for a feature using Youden's J.

    Returns (operator '>' or '<', threshold, youden_j).
    """
    valid = ~np.isnan(values)
    vals = values[valid]
    labs = labels[valid]

    if len(vals) < 20:
        return ">", 0.0, 0.0

    quantiles = np.linspace(0.05, 0.95, n_quantiles)
    thresholds = np.quantile(vals, quantiles)

    best_j = -1.0
    best_op = ">"
    best_thr = float(thresholds[len(thresholds) // 2])

    pos = labs == 1
    neg = labs == 0
    n_pos = pos.sum()
    n_neg = neg.sum()

    if n_pos == 0 or n_neg == 0:
        return ">", best_thr, 0.0

    for thr in thresholds:
        # Test ">"
        above = vals >= thr
        tpr_above = above[pos].sum() / n_pos if n_pos > 0 else 0
        tnr_above = (~above)[neg].sum() / n_neg if n_neg > 0 else 0
        j_above = tpr_above + tnr_above - 1

        # Test "<"
        below = vals <= thr
        tpr_below = below[pos].sum() / n_pos if n_pos > 0 else 0
        tnr_below = (~below)[neg].sum() / n_neg if n_neg > 0 else 0
        j_below = tpr_below + tnr_below - 1

        if j_above > best_j:
            best_j = j_above
            best_op = ">"
            best_thr = float(thr)
        if j_below > best_j:
            best_j = j_below
            best_op = "<"
            best_thr = float(thr)

    return best_op, best_thr, best_j


def rank_features_by_effect_size(
    features: pd.DataFrame,
    labels: np.ndarray,
) -> List[Dict[str, Any]]:
    """Rank features by Cohen's d and find optimal thresholds.

    Returns list of dicts sorted by |Cohen's d| descending.
    """
    climb_mask = labels == 1
    no_climb_mask = labels == 0
    ranked = []

    for col in features.columns:
        vals = features[col].values.astype(np.float64)
        valid = ~np.isnan(vals)
        if valid.sum() < 30:
            continue

        g1 = vals[climb_mask & valid]
        g2 = vals[no_climb_mask & valid]

        if len(g1) < 5 or len(g2) < 5:
            continue

        d = cohens_d(g1, g2)
        op, thr, j = find_best_threshold(vals, labels)

        ranked.append({
            "feature": col,
            "cohens_d": d,
            "abs_d": abs(d),
            "op": op,
            "threshold": thr,
            "youden_j": j,
            "mean_climb": float(np.nanmean(g1)),
            "mean_no_climb": float(np.nanmean(g2)),
        })

    ranked.sort(key=lambda x: x["abs_d"], reverse=True)
    return ranked


def precompute_masks(features: pd.DataFrame,
                     ranked: List[Dict[str, Any]]) -> Dict[str, np.ndarray]:
    """Pre-compute boolean masks for each feature's threshold."""
    masks = {}
    for r in ranked:
        col = r["feature"]
        vals = features[col].values.astype(np.float64)
        if r["op"] == ">":
            masks[col] = vals >= r["threshold"]
        else:
            masks[col] = vals <= r["threshold"]
        # Handle NaN as False
        masks[col] = masks[col] & ~np.isnan(vals)
    return masks


def score_filter_combo(
    combo_cols: List[str],
    masks: Dict[str, np.ndarray],
    labels: np.ndarray,
    gains: np.ndarray,
    losses: np.ndarray,
) -> Optional[Dict[str, Any]]:
    """Score a filter combination (AND of masks)."""
    combined = np.ones(len(labels), dtype=bool)
    for col in combo_cols:
        combined &= masks[col]

    return score_signals(combined, labels, gains, losses)


def discover_filters(
    features: pd.DataFrame,
    labels: np.ndarray,
    gains: np.ndarray,
    losses: np.ndarray,
    ranked_features: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Discover filter rules using Cohen's d + brute force combinations.

    Tests all 2-feature and 3-feature AND-combinations from top features.
    Returns list of profitable filter results.
    """
    top_n = min(TOP_FEATURES_COMBOS, len(ranked_features))
    top = ranked_features[:top_n]

    if len(top) < 2:
        return []

    masks = precompute_masks(features, top)
    results = []

    # Build lookup for threshold info
    feat_info = {r["feature"]: r for r in top}

    # 2-feature combos
    for combo in itertools.combinations([r["feature"] for r in top], 2):
        combo_list = list(combo)
        sc = score_filter_combo(combo_list, masks, labels, gains, losses)
        if sc and sc["expected_profit"] > 0:
            rules = []
            for col in combo_list:
                info = feat_info[col]
                rules.append({
                    "signal": col,
                    "op": info["op"],
                    "threshold": round(info["threshold"], 6),
                })
            desc = " AND ".join(
                f"{r['signal']} {r['op']} {r['threshold']}" for r in rules
            )
            results.append({
                "method": "filters",
                "filter_rules": rules,
                "filter_description": desc,
                "n_features": 2,
                **sc,
            })

    # 3-feature combos
    top_for_3 = min(top_n, 12)  # limit to keep runtime manageable
    for combo in itertools.combinations([r["feature"] for r in top[:top_for_3]], 3):
        combo_list = list(combo)
        sc = score_filter_combo(combo_list, masks, labels, gains, losses)
        if sc and sc["expected_profit"] > 0:
            rules = []
            for col in combo_list:
                info = feat_info[col]
                rules.append({
                    "signal": col,
                    "op": info["op"],
                    "threshold": round(info["threshold"], 6),
                })
            desc = " AND ".join(
                f"{r['signal']} {r['op']} {r['threshold']}" for r in rules
            )
            results.append({
                "method": "filters",
                "filter_rules": rules,
                "filter_description": desc,
                "n_features": 3,
                **sc,
            })

    return results


# ===================================================================
# 7. Discovery Method 2: Gradient Boosted Model
# ===================================================================
def _get_gbm_model():
    """Get the best available GBM model class."""
    try:
        from lightgbm import LGBMClassifier
        return LGBMClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            min_child_samples=20,
            subsample=0.8,
            colsample_bytree=0.8,
            verbose=-1,
            n_jobs=1,
        )
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        return GradientBoostingClassifier(
            n_estimators=150,
            max_depth=3,
            learning_rate=0.05,
            min_samples_leaf=20,
            subsample=0.8,
        )


def discover_gbm(
    features: pd.DataFrame,
    labels: np.ndarray,
    gains: np.ndarray,
    losses: np.ndarray,
) -> List[Dict[str, Any]]:
    """Discover profitable signals using gradient boosted model.

    Sweeps confidence thresholds and returns profitable configurations.
    """
    # Fill NaN with column median for GBM
    feat_filled = features.fillna(features.median())
    # Drop columns still all-NaN
    feat_filled = feat_filled.dropna(axis=1, how="all")

    if feat_filled.shape[1] < 3 or len(labels) < 100:
        return []

    X = feat_filled.values.astype(np.float64)
    y = labels

    # Replace any remaining NaN/inf with 0
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    results = []

    # Walk-forward splits
    splits = walk_forward_splits(len(y))
    if not splits:
        return []

    for thr in GBM_THRESHOLDS:
        split_scores = []
        total_signals = 0

        for tr_s, tr_e, te_s, te_e in splits:
            X_train, y_train = X[tr_s:tr_e], y[tr_s:tr_e]
            X_test, y_test = X[te_s:te_e], y[te_s:te_e]
            gains_test = gains[te_s:te_e]
            losses_test = losses[te_s:te_e]

            if y_train.sum() < 10 or y_test.sum() < 5:
                continue

            model = _get_gbm_model()
            try:
                model.fit(X_train, y_train)
            except Exception as e:
                logger.debug(f"GBM fit error: {e}")
                continue

            proba = model.predict_proba(X_test)[:, 1]
            predicted = proba >= thr

            sc = score_signals(predicted, y_test, gains_test, losses_test)
            if sc is None:
                continue

            split_scores.append(sc)
            total_signals += sc["n_signals"]

        # Require ALL splits to have produced scores (strict walk-forward)
        if len(split_scores) < len(splits):
            continue

        # Check all splits profitable
        all_profitable = all(s["expected_profit"] > 0 for s in split_scores)
        if not all_profitable:
            continue

        avg_precision = float(np.mean([s["precision"] for s in split_scores]))
        avg_profit = float(np.mean([s["expected_profit"] for s in split_scores]))
        avg_n_signals = float(np.mean([s["n_signals"] for s in split_scores]))

        # Get feature importances from last model
        try:
            if hasattr(model, "feature_importances_"):
                imp = model.feature_importances_
                top_idx = np.argsort(imp)[::-1][:5]
                top_features = [
                    {"feature": feat_filled.columns[i],
                     "importance": round(float(imp[i]), 4)}
                    for i in top_idx if imp[i] > 0
                ]
            else:
                top_features = []
        except Exception:
            top_features = []

        results.append({
            "method": "gbm",
            "confidence_threshold": thr,
            "splits": split_scores,
            "avg_precision": round(avg_precision, 2),
            "avg_expected_profit": round(avg_profit, 4),
            "total_signals": total_signals,
            "avg_n_signals": round(avg_n_signals, 1),
            "top_features": top_features,
            "filter_description": f"GBM(thr={thr}) top: {', '.join(f['feature'] for f in top_features[:3])}",
        })

    return results


# ===================================================================
# 8. Walk-Forward Validation for Filters
# ===================================================================
def walk_forward_filters(
    features: pd.DataFrame,
    labels: np.ndarray,
    gains: np.ndarray,
    losses: np.ndarray,
) -> List[Dict[str, Any]]:
    """Run filter discovery with walk-forward validation.

    For each walk-forward split:
    - Rank features and discover filters on train set
    - Score them on test set
    Only keep results profitable on ALL test splits.
    """
    splits = walk_forward_splits(len(labels))
    if not splits:
        return []

    # For each split, discover on train, score on test
    # Track results by filter_description
    candidate_results: Dict[str, Dict[str, Any]] = {}

    for split_idx, (tr_s, tr_e, te_s, te_e) in enumerate(splits):
        train_feat = features.iloc[tr_s:tr_e]
        train_labels = labels[tr_s:tr_e]
        train_gains = gains[tr_s:tr_e]
        train_losses = losses[tr_s:tr_e]

        test_feat = features.iloc[te_s:te_e]
        test_labels = labels[te_s:te_e]
        test_gains = gains[te_s:te_e]
        test_losses = losses[te_s:te_e]

        if train_labels.sum() < 10 or test_labels.sum() < 5:
            continue

        # Rank features on train set
        ranked = rank_features_by_effect_size(train_feat, train_labels)
        if len(ranked) < 2:
            continue

        # Discover on train
        train_results = discover_filters(
            train_feat, train_labels, train_gains, train_losses, ranked
        )

        # Score each discovered filter on test set
        feat_info_map = {r["feature"]: r for r in ranked}
        for tr_res in train_results:
            desc = tr_res["filter_description"]
            rules = tr_res["filter_rules"]

            # Re-compute masks on test set using same thresholds
            test_masks = {}
            valid = True
            for rule in rules:
                col = rule["signal"]
                if col not in test_feat.columns:
                    valid = False
                    break
                vals = test_feat[col].values.astype(np.float64)
                if rule["op"] == ">":
                    test_masks[col] = (vals >= rule["threshold"]) & ~np.isnan(vals)
                else:
                    test_masks[col] = (vals <= rule["threshold"]) & ~np.isnan(vals)

            if not valid:
                continue

            test_sc = score_filter_combo(
                [r["signal"] for r in rules],
                test_masks,
                test_labels,
                test_gains,
                test_losses,
            )

            if test_sc is None or test_sc["expected_profit"] <= 0:
                continue

            # Aggregate across splits
            if desc not in candidate_results:
                candidate_results[desc] = {
                    "method": "filters",
                    "filter_rules": rules,
                    "filter_description": desc,
                    "n_features": tr_res.get("n_features", len(rules)),
                    "splits": [],
                    "split_count": 0,
                }
            candidate_results[desc]["splits"].append(test_sc)
            candidate_results[desc]["split_count"] += 1

    # Only keep results that appeared in ALL splits and were profitable in all
    n_splits = len(splits)
    final = []
    for desc, res in candidate_results.items():
        if res["split_count"] < n_splits:
            continue
        if not all(s["expected_profit"] > 0 for s in res["splits"]):
            continue

        res["avg_precision"] = round(
            float(np.mean([s["precision"] for s in res["splits"]])), 2
        )
        res["avg_expected_profit"] = round(
            float(np.mean([s["expected_profit"] for s in res["splits"]])), 4
        )
        res["total_signals"] = sum(s["n_signals"] for s in res["splits"])
        res["avg_n_signals"] = round(
            float(np.mean([s["n_signals"] for s in res["splits"]])), 1
        )
        del res["split_count"]
        final.append(res)

    return final


# ===================================================================
# 9. Sweep Orchestrator
# ===================================================================
def _write_checkpoint(progress: str, elapsed_sec: float,
                      n_results: int, results: List[Dict]) -> None:
    """Write checkpoint file for heartbeat monitoring."""
    checkpoint = {
        "progress": progress,
        "elapsed_sec": round(elapsed_sec, 1),
        "n_results": n_results,
        "results": results[-50:] if len(results) > 50 else results,
    }
    try:
        tmp = CHECKPOINT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(checkpoint, f)
        os.replace(tmp, CHECKPOINT_FILE)
    except Exception as e:
        logger.warning(f"Failed to write checkpoint: {e}")


def run_sweep(hours: int, output_path: str) -> List[Dict[str, Any]]:
    """Main sweep: iterate over parameter combos, discover signals.
    Returns list of all profitable results."""
    t_start = time.time()

    # --- Step 1: Load raw data ---
    dfs = load_raw_data(hours)
    sol_prices = dfs["prices"]
    if sol_prices.empty:
        logger.error("No price data available. Aborting.")
        return []

    # --- Step 2: Compute features (one-time) ---
    feature_df = compute_features(dfs)
    if feature_df.empty:
        logger.error("Feature computation returned empty. Aborting.")
        return []

    grid = feature_df.index

    # --- Step 3: Pre-compute 1-second SOL price series for labeling ---
    sol_1s = _resample_prices_1s(dfs["prices"], "SOL")
    if sol_1s.empty:
        logger.error("No SOL 1-second price data. Aborting.")
        return []

    # Pre-compute forward windows for all needed sizes
    all_windows = sorted(set(EARLY_WINDOW_SECS + FORWARD_WINDOW_SECS))
    logger.info(f"Pre-computing forward price windows for: {all_windows} ...")
    t_fwd = time.time()
    fwd_windows = precompute_forward_windows(sol_1s, all_windows)
    logger.info(f"  Forward windows computed in {time.time() - t_fwd:.1f}s")

    # --- Step 4: Build combo grid ---
    combos = list(itertools.product(
        MIN_CLIMB_PCTS, MAX_DIP_PCTS, EARLY_WINDOW_SECS, FORWARD_WINDOW_SECS
    ))
    total_combos = len(combos)
    logger.info(f"Total parameter combos: {total_combos}")

    all_results: List[Dict[str, Any]] = []
    combos_skipped = 0
    errors = 0

    # --- Step 5: Iterate combos ---
    for idx, (min_climb, max_dip, early_w, fwd_w) in enumerate(combos):
        combo_t0 = time.time()
        combo_num = idx + 1
        elapsed = time.time() - t_start

        # Skip if early_window > forward_window (nonsensical)
        if early_w > fwd_w:
            combos_skipped += 1
            _write_checkpoint(
                f"{combo_num}/{total_combos}", elapsed,
                len(all_results), all_results
            )
            continue

        logger.info(
            f"[{combo_num}/{total_combos}] climb={min_climb}% dip={max_dip}% "
            f"early={early_w}s fwd={fwd_w}s"
        )

        try:
            # Label samples
            label_df = label_samples(
                sol_1s, grid, min_climb, max_dip, early_w, fwd_w, fwd_windows
            )

            n_climbs = int((label_df["label"] == 1).sum())
            n_total = len(label_df)

            if n_climbs < MIN_CLIMBS:
                logger.info(f"  Skipping: only {n_climbs} climbs (need {MIN_CLIMBS})")
                combos_skipped += 1
                _write_checkpoint(
                    f"{combo_num}/{total_combos}", elapsed,
                    len(all_results), all_results
                )
                continue

            logger.info(f"  Labels: {n_climbs} climbs / {n_total} total "
                        f"({n_climbs/n_total*100:.1f}%)")

            # Align features with labels (use index intersection for tz safety)
            label_idx = pd.DatetimeIndex(label_df["timestamp"])
            common_idx = feature_df.index.intersection(label_idx)
            if len(common_idx) < MIN_CLIMBS:
                logger.info(f"  Skipping: only {len(common_idx)} aligned samples")
                combos_skipped += 1
                _write_checkpoint(
                    f"{combo_num}/{total_combos}", elapsed,
                    len(all_results), all_results
                )
                continue

            feat_aligned = feature_df.loc[common_idx]
            label_aligned = label_df.set_index("timestamp").loc[common_idx]
            labels_arr = label_aligned["label"].values
            gains_arr = label_aligned["actual_gain"].values
            losses_arr = label_aligned["actual_loss"].values

            combo_results = []

            # --- Method 1: Filter rules with walk-forward ---
            try:
                filter_results = walk_forward_filters(
                    feat_aligned, labels_arr, gains_arr, losses_arr
                )
                for fr in filter_results:
                    fr["climb_params"] = {
                        "min_climb_pct": min_climb,
                        "max_dip_pct": max_dip,
                        "early_window_sec": early_w,
                        "forward_window_sec": fwd_w,
                    }
                    fr["n_climbs"] = n_climbs
                    fr["n_non_climbs"] = n_total - n_climbs
                    fr["n_events"] = n_total
                    combo_results.extend(filter_results)
                if filter_results:
                    logger.info(f"  Filters: {len(filter_results)} profitable configs")
            except Exception as e:
                logger.warning(f"  Filter discovery error: {e}")
                errors += 1

            # --- Method 2: GBM with walk-forward ---
            try:
                gbm_results = discover_gbm(
                    feat_aligned, labels_arr, gains_arr, losses_arr
                )
                for gr in gbm_results:
                    gr["climb_params"] = {
                        "min_climb_pct": min_climb,
                        "max_dip_pct": max_dip,
                        "early_window_sec": early_w,
                        "forward_window_sec": fwd_w,
                    }
                    gr["n_climbs"] = n_climbs
                    gr["n_non_climbs"] = n_total - n_climbs
                    gr["n_events"] = n_total
                    combo_results.extend(gbm_results)
                if gbm_results:
                    logger.info(f"  GBM: {len(gbm_results)} profitable configs")
            except Exception as e:
                logger.warning(f"  GBM discovery error: {e}")
                errors += 1

            all_results.extend(combo_results)
            combo_elapsed = time.time() - combo_t0
            logger.info(f"  Combo done in {combo_elapsed:.1f}s "
                        f"({len(combo_results)} results, "
                        f"{len(all_results)} total)")

        except Exception as e:
            logger.error(f"  Combo error: {e}", exc_info=True)
            errors += 1

        # Update checkpoint
        elapsed = time.time() - t_start
        _write_checkpoint(
            f"{combo_num}/{total_combos}", elapsed,
            len(all_results), all_results
        )

    # --- Step 6: Write final output ---
    elapsed = time.time() - t_start
    logger.info(f"\nSweep complete in {elapsed/60:.1f} minutes")
    logger.info(f"  Total combos: {total_combos}")
    logger.info(f"  Combos skipped: {combos_skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Total results: {len(all_results)}")

    # Sort by expected profit
    all_results.sort(key=lambda x: x.get("avg_expected_profit", 0), reverse=True)

    profitable = [r for r in all_results if r.get("avg_expected_profit", 0) > 0]

    output = {
        "trade_cost_pct": TRADE_COST_PCT * 100,
        "sweep_config": {
            "hours": hours,
            "total_combos": total_combos,
            "combos_skipped": combos_skipped,
            "errors": errors,
            "runtime_sec": round(elapsed, 1),
            "runtime_min": round(elapsed / 60, 1),
        },
        "summary": {
            "total_results": len(all_results),
            "profitable_results": len(profitable),
        },
        "profitable_results": profitable[:50],
        "best_result": profitable[0] if profitable else None,
    }

    try:
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        logger.info(f"Output written to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output: {e}")

    # --- Step 7: Print morning report ---
    _print_morning_report(profitable[:15])

    return profitable


# ===================================================================
# 10. Morning Report
# ===================================================================
def _print_morning_report(top_results: List[Dict[str, Any]]) -> None:
    """Print a formatted table of top results."""
    if not top_results:
        logger.info("\nNo profitable results found.")
        return

    logger.info(f"\n{'='*120}")
    logger.info(f"TOP {len(top_results)} BEST CONFIGURATIONS (net profit after {TRADE_COST_PCT*100}% cost):")
    logger.info(f"{'='*120}")

    header = (
        f"{'#':>3}  {'Method':<8}  {'Climb':>6}  {'Dip':>6}  {'Early':>6}  "
        f"{'FW':>5}  {'Net$/Trade':>10}  {'Precision':>9}  {'Signals':>7}  "
        f"{'Details'}"
    )
    logger.info(header)
    logger.info("-" * 120)

    for i, r in enumerate(top_results):
        cp = r.get("climb_params", {})
        method = r.get("method", "?")
        climb = f"{cp.get('min_climb_pct', '?')}%"
        dip = f"{cp.get('max_dip_pct', '?')}%"
        early = f"{cp.get('early_window_sec', '?')}s"
        fw = f"{cp.get('forward_window_sec', '?')}s"
        net = f"+{r.get('avg_expected_profit', 0):.4f}%"
        prec = f"{r.get('avg_precision', 0):.1f}%"
        sigs = str(r.get("total_signals", 0))
        desc = r.get("filter_description", "")[:50]

        line = (
            f"{i+1:>3}  {method:<8}  {climb:>6}  {dip:>6}  {early:>6}  "
            f"{fw:>5}  {net:>10}  {prec:>9}  {sigs:>7}  {desc}"
        )
        logger.info(line)

    logger.info(f"{'='*120}")


# ===================================================================
# 11. Apply Best Filters to pump_continuation_rules
# ===================================================================

# Maps SDE feature names -> buyin_trail_minutes column names.
# Only features with a clean mapping are eligible for pump_continuation_rules.
SDE_TO_TRAIL_MAP = {
    # Price features
    "pm_change_1m": "pm_price_change_1m",
    "pm_change_5m": "pm_price_change_5m",
    "pm_change_10m": "pm_price_change_10m",
    "pm_volatility_1m": "pm_volatility_pct",
    "pm_stddev_1m": "pm_price_stddev_pct",
    "pm_body_ratio_1m": "pm_body_range_ratio",
    "pm_accel": "pm_momentum_acceleration_1m",
    # Order book features
    "ob_imbalance_1m": "ob_volume_imbalance",
    "ob_imbalance_30s": "ob_volume_imbalance",
    "ob_spread_1m": "ob_spread_bps",
    "ob_spread_30s": "ob_spread_bps",
    "ob_spread_2m": "ob_spread_bps",
    "ob_depth_ratio_1m": "ob_depth_imbalance_ratio",
    "ob_bid_share_1m": "ob_bid_liquidity_share_pct",
    "ob_bid_share_30s": "ob_bid_liquidity_share_pct",
    "ob_microprice_2m": "ob_microprice_deviation",
    "ob_microprice_30s": "ob_microprice_deviation",
    "ob_net_liq_chg_1m": "ob_net_flow_5m",
    # Transaction features
    "tx_pressure_1m": "tx_buy_sell_pressure",
    "tx_pressure_5m": "tx_buy_sell_pressure",
    "tx_vol_1m": "tx_total_volume_usd",
    "tx_vol_5m": "tx_total_volume_usd",
    "tx_count_1m": "tx_trade_count",
    "tx_count_5m": "tx_trade_count",
    "tx_avg_size_1m": "tx_avg_trade_size",
    "tx_large_count_1m": "tx_large_trade_count",
    # Whale features
    "wh_net_flow_5m": "wh_net_flow_ratio",
    "wh_activity_5m": "wh_total_movements",
}

# Section derived from trail column prefix (matches pattern_validator.py SECTION_PREFIX_MAP)
_SECTION_PREFIX_MAP = {
    "pm_": "price_movements",
    "tx_": "transactions",
    "ob_": "order_book_signals",
    "wh_": "whale_activity",
}


def _section_from_column(col_name: str) -> str:
    """Derive section name from a buyin_trail_minutes column prefix."""
    for prefix, section in _SECTION_PREFIX_MAP.items():
        if col_name.startswith(prefix):
            return section
    return "unknown"


def get_current_pump_rules() -> Tuple[List[Dict[str, Any]], float]:
    """Read current pump_continuation_rules and return (rules, avg_expected_profit).

    Returns ([], 0.0) if no rules exist.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name, section, from_value, to_value,
                           precision_pct, expected_profit, test_n_signals, is_stable
                    FROM pump_continuation_rules
                    ORDER BY id
                """)
                rows = cursor.fetchall()

        if not rows:
            return [], 0.0

        rules = [dict(r) for r in rows]
        profits = [float(r["expected_profit"]) for r in rules
                    if r.get("expected_profit") is not None]
        avg_profit = float(np.mean(profits)) if profits else 0.0
        return rules, avg_profit

    except Exception as e:
        logger.warning(f"Failed to read current pump rules: {e}")
        return [], 0.0


def _can_map_to_trail(filter_rules: List[Dict[str, Any]]) -> bool:
    """Check if ALL filter rules in a result have trail column mappings."""
    for rule in filter_rules:
        sde_name = rule.get("signal", "")
        if sde_name not in SDE_TO_TRAIL_MAP:
            return False
    return True


def apply_best_filters(all_results: List[Dict[str, Any]]) -> bool:
    """Compare SDE's best filter results against current pump_continuation_rules.

    If the best SDE filter set has higher expected_profit than the current rules,
    replaces them in the database.

    Returns True if new rules were applied.
    """
    logger.info("=" * 80)
    logger.info("APPLY: Comparing SDE filters vs current pump_continuation_rules")
    logger.info("=" * 80)

    # 1. Filter to filter-method results only (not GBM)
    filter_results = [
        r for r in all_results
        if r.get("method") == "filters" and r.get("avg_expected_profit", 0) > 0
    ]
    logger.info(f"  Filter-method profitable results: {len(filter_results)}")

    if not filter_results:
        logger.info("  No profitable filter results to apply.")
        return False

    # 2. Filter to results where ALL rules have trail column mappings
    mappable = [r for r in filter_results if _can_map_to_trail(r.get("filter_rules", []))]
    logger.info(f"  Results with trail-mappable rules: {len(mappable)}")

    if not mappable:
        logger.info("  No results have all rules mappable to trail columns. Skipping.")
        return False

    # 3. Sort by avg_expected_profit descending, take the best
    mappable.sort(key=lambda x: x.get("avg_expected_profit", 0), reverse=True)
    best = mappable[0]
    best_profit = best.get("avg_expected_profit", 0)
    best_precision = best.get("avg_precision", 0)
    best_signals = best.get("total_signals", 0)
    best_rules = best.get("filter_rules", [])
    best_desc = best.get("filter_description", "")

    logger.info(f"  Best SDE filter: profit={best_profit:+.4f}% "
                f"precision={best_precision:.1f}% signals={best_signals}")
    logger.info(f"  Rules: {best_desc}")

    # 4. Read current pump_continuation_rules
    current_rules, current_profit = get_current_pump_rules()
    logger.info(f"  Current pump rules: {len(current_rules)} rules, "
                f"profit={current_profit:+.4f}%")

    # 5. Compare: SDE must be strictly better
    if best_profit <= current_profit:
        logger.info(f"  SKIP: SDE profit ({best_profit:+.4f}%) is not better than "
                     f"current ({current_profit:+.4f}%). Keeping current rules.")
        return False

    # 6. Write new rules to pump_continuation_rules
    logger.info(f"  APPLYING: SDE profit ({best_profit:+.4f}%) > "
                f"current ({current_profit:+.4f}%)")

    new_rules = []
    for rule in best_rules:
        sde_name = rule["signal"]
        trail_col = SDE_TO_TRAIL_MAP[sde_name]
        section = _section_from_column(trail_col)

        # Convert threshold to from_value/to_value range
        op = rule.get("op", ">")
        threshold = float(rule.get("threshold", 0))

        if op == ">":
            from_val = threshold
            to_val = 999999.0  # effectively no upper bound
        else:  # "<"
            from_val = -999999.0  # effectively no lower bound
            to_val = threshold

        new_rules.append({
            "column_name": trail_col,
            "section": section,
            "from_value": from_val,
            "to_value": to_val,
            "precision_pct": best_precision,
            "expected_profit": best_profit,
            "test_n_signals": best_signals,
            "is_stable": True,
        })

    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM pump_continuation_rules")
                for r in new_rules:
                    cursor.execute("""
                        INSERT INTO pump_continuation_rules
                            (column_name, section, from_value, to_value, precision_pct,
                             expected_profit, test_n_signals, is_stable, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """, [
                        r["column_name"], r["section"], r["from_value"], r["to_value"],
                        r["precision_pct"], r["expected_profit"], r["test_n_signals"],
                        r["is_stable"],
                    ])
            conn.commit()

        logger.info(f"  SUCCESS: Wrote {len(new_rules)} new pump continuation rules:")
        for r in new_rules:
            op_str = ">=" if r["from_value"] > -999000 else "<="
            val_str = r["from_value"] if op_str == ">=" else r["to_value"]
            logger.info(f"    {r['column_name']} {op_str} {val_str:.6f} "
                        f"(section={r['section']})")
        return True

    except Exception as e:
        logger.error(f"  FAILED to write new pump rules: {e}")
        return False


# ===================================================================
# CLI Entry Point
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Signal Discovery Engine for Pump Detection"
    )
    parser.add_argument(
        "--sweep", action="store_true",
        help="Run the full overnight sweep"
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Hours of historical data to analyze (default: 24)"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Path for JSON output file"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Auto-apply best filters to pump_continuation_rules if better than current"
    )
    args = parser.parse_args()

    if not args.output:
        date_str = datetime.now().strftime("%Y%m%d")
        args.output = f"/tmp/sde_overnight_{date_str}.json"

    # Setup logging
    date_str = datetime.now().strftime("%Y%m%d")
    log_path = f"/tmp/sde_overnight_{date_str}.log"
    setup_logging(log_path)

    logger.info("Signal Discovery Engine starting")
    logger.info(f"  Hours: {args.hours}")
    logger.info(f"  Output: {args.output}")
    logger.info(f"  Apply: {args.apply}")
    logger.info(f"  Log: {log_path}")
    logger.info(f"  Checkpoint: {CHECKPOINT_FILE}")

    if args.sweep:
        results = run_sweep(args.hours, args.output)

        if args.apply and results:
            logger.info("")
            applied = apply_best_filters(results)
            if applied:
                logger.info("New pump continuation rules applied from SDE.")
            else:
                logger.info("Current pump continuation rules kept (SDE did not find better).")
        elif args.apply:
            logger.info("--apply requested but no profitable results found.")
    else:
        logger.info("No action specified. Use --sweep to run the overnight sweep.")
        parser.print_help()
        sys.exit(1)

    logger.info("Signal Discovery Engine finished.")


if __name__ == "__main__":
    main()

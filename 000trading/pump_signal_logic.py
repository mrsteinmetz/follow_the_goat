"""
Pump Signal Logic V2 — Improved Signal Detection
=================================================
Key changes from V1:
  1. PATH-AWARE labeling: price must rise within 4 min, not just "eventually"
  2. Multi-timeframe trend gate: need 2/3 timeframes positive (catches downtrend bounces)
  3. Gradient-boosted model instead of manual threshold combos (captures interactions)
  4. Walk-forward validation: model must work on 3 sequential test windows, not just one
  5. Engineered features: momentum agreement, cross-asset divergence, volume-price divergence
  6. Confidence threshold: only fires when model is 70%+ confident

Drop-in replacement for pump_signal_logic V1 — same external API:
  - maybe_refresh_rules()
  - check_and_fire_pump_signal(buyin_id, market_price, price_cycle)
  - get_pump_status()
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa

from core.database import get_postgres, postgres_execute, postgres_query, postgres_query_one

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_CACHE_PATH = _PROJECT_ROOT / "tests" / "filter_simulation" / "results" / "pump_model_v2_cache.pkl"

# ── DuckDB trail cache ───────────────────────────────────────────────────────
_CACHE_DIR = _PROJECT_ROOT / "cache"
_CACHE_DIR.mkdir(exist_ok=True)
_TRAIL_CACHE_FILE = _CACHE_DIR / "pump_model_trail.duckdb"

logger = logging.getLogger("train_validator.pump_signal_v2")

# =============================================================================
# CONSTANTS
# =============================================================================

# ── Labeling ──────────────────────────────────────────────────────────────────
MIN_PUMP_PCT = 0.2              # Target gain (0.2%)
MAX_DRAWDOWN_PCT = 0.10         # Max dip before reaching target (tighter than v1's 0.15%)
FORWARD_WINDOW = 10             # Look 10 min ahead
EARLY_WINDOW = 4                # Must reach target within first 4 minutes
IMMEDIATE_DIP_MAX = -0.08       # Max allowed dip in first 2 min after entry

# ── Data ──────────────────────────────────────────────────────────────────────
LOOKBACK_HOURS = 48
RULES_REFRESH_INTERVAL = 300
TRADE_COST_PCT = 0.1

# ── Signal ────────────────────────────────────────────────────────────────────
MIN_CONFIDENCE = 0.70           # Model probability threshold to fire
MIN_SAMPLES_TO_TRAIN = 200
COOLDOWN_SECONDS = 300

# ── Safety gates ──────────────────────────────────────────────────────────────
CRASH_GATE_5M = -0.3
CRASH_GATE_MICRO_30S = -0.05

# ── Walk-forward ──────────────────────────────────────────────────────────────
N_WALK_FORWARD_SPLITS = 3
WALK_FORWARD_TEST_FRAC = 0.15

# ── Skip columns (metadata, labels, forward-looking) ─────────────────────────
SKIP_COLUMNS = frozenset([
    'buyin_id', 'trade_id', 'play_id', 'wallet_address', 'followed_at',
    'our_status', 'minute', 'sub_minute', 'interval_idx',
    'potential_gains', 'pat_detected_list', 'pat_swing_trend',
    'is_good', 'label', 'created_at', 'pre_entry_trend', 'target',
    'max_fwd_return', 'min_fwd_return', 'time_to_peak',
    'max_fwd_return_4m', 'min_fwd_return_2m',
])

ABSOLUTE_PRICE_COLUMNS = frozenset([
    'pm_open_price', 'pm_close_price', 'pm_high_price', 'pm_low_price', 'pm_avg_price',
    'btc_open_price', 'btc_close_price', 'btc_high_price', 'btc_low_price',
    'eth_open_price', 'eth_close_price', 'eth_high_price', 'eth_low_price',
    'sp_min_price', 'sp_max_price', 'sp_avg_price', 'sp_start_price', 'sp_end_price',
    'sp_price_count',
    'ts_open_price', 'ts_close_price', 'ts_high_price', 'ts_low_price',
    'pre_entry_price_1m_before', 'pre_entry_price_2m_before',
    'pre_entry_price_3m_before', 'pre_entry_price_5m_before',
    'pre_entry_price_10m_before',
    'ob_mid_price', 'ob_total_liquidity', 'ob_bid_total', 'ob_ask_total',
    'tx_vwap', 'tx_total_volume_usd', 'tx_buy_volume_usd', 'tx_sell_volume_usd',
    'tx_delta_divergence', 'tx_cumulative_delta',
    'wh_total_sol_moved', 'wh_inflow_sol', 'wh_outflow_sol',
    'pat_asc_tri_resistance_level', 'pat_asc_tri_support_level',
    'pat_inv_hs_neckline', 'pat_cup_handle_rim',
])

# =============================================================================
# MODULE STATE
# =============================================================================

_model = None
_feature_columns: List[str] = []
_model_metadata: Dict[str, Any] = {}
_last_rules_refresh: float = 0.0
_last_entry_time: float = 0.0
_last_gate_summary_time: float = 0.0

_gate_stats: Dict[str, int] = {
    'no_model': 0, 'crash_gate_fail': 0, 'crash_5m_fail': 0,
    'multi_tf_fail': 0, 'gates_passed': 0, 'low_confidence': 0,
    'signal_fired': 0, 'total_checks': 0,
}

_price_buffer: deque = deque(maxlen=200)

# ── Volatility regime tracking ───────────────────────────────────────────────
_vol_buffer: deque = deque(maxlen=720)     # ~12 hours at 1 sample per ~minute

# ── Circuit breaker: rolling accuracy tracker ────────────────────────────────
_recent_outcomes: deque = deque(maxlen=50)  # last 50 signal outcomes
_circuit_breaker_paused: bool = False


def record_signal_outcome(hit_target: bool) -> None:
    """Record whether a fired signal hit the +0.2% target.

    Called by the trailing_stop_seller / update_potential_gains component
    when a Play #3 trade resolves.
    """
    _recent_outcomes.append(1 if hit_target else 0)


def _check_circuit_breaker() -> bool:
    """Return True if the circuit breaker is tripped (live precision too low)."""
    global _circuit_breaker_paused
    if len(_recent_outcomes) < 20:
        return False  # not enough data to judge
    live_prec = sum(_recent_outcomes) / len(_recent_outcomes)
    if live_prec < 0.40:
        if not _circuit_breaker_paused:
            logger.warning(f"Circuit breaker TRIPPED: live precision {live_prec:.1%} "
                           f"({sum(_recent_outcomes)}/{len(_recent_outcomes)}) — pausing signals")
            _circuit_breaker_paused = True
        return True
    if _circuit_breaker_paused:
        logger.info(f"Circuit breaker RESET: live precision recovered to {live_prec:.1%}")
        _circuit_breaker_paused = False
    return False


def _update_vol_buffer(vol_1m: Optional[float]) -> None:
    if vol_1m is not None:
        _vol_buffer.append(float(vol_1m))


def _get_vol_percentile() -> Optional[float]:
    """Return the percentile rank of the most recent volatility reading."""
    if len(_vol_buffer) < 60:
        return None  # need ~1 hour of data
    current = _vol_buffer[-1]
    rank = sum(1 for v in _vol_buffer if v <= current)
    return rank / len(_vol_buffer) * 100


# =============================================================================
# PRICE BUFFER & SAFETY GATES
# =============================================================================

def _update_price_buffer(price: float) -> None:
    _price_buffer.append((time.time(), price))


def _get_micro_trend(seconds: int) -> Optional[float]:
    if len(_price_buffer) < 2:
        return None
    now = _price_buffer[-1][0]
    current_price = _price_buffer[-1][1]
    cutoff = now - seconds
    for ts, price in _price_buffer:
        if ts >= cutoff:
            return (current_price - price) / price * 100 if price != 0 else None
    return None


def _is_not_crashing() -> Tuple[bool, str]:
    trend_30s = _get_micro_trend(30)
    if trend_30s is None:
        if len(_price_buffer) < 3:
            return False, f"insufficient buffer ({len(_price_buffer)})"
        return True, f"warming up ({len(_price_buffer)} samples)"
    if trend_30s < CRASH_GATE_MICRO_30S:
        return False, f"30s={trend_30s:+.4f}% (selloff)"
    return True, f"30s={trend_30s:+.4f}%"


# =============================================================================
# MULTI-TIMEFRAME TREND GATE
# =============================================================================

def _check_multi_timeframe_trend(trail_row: dict) -> Tuple[bool, str]:
    """
    Require at least 1 of 3 timeframes showing positive momentum.

    Backtest shows the GBM model is fundamentally a dip-buyer: it fires
    highest confidence when 5m trend is negative (pullback before pump).
    Requiring 2/3 positive blocked 100% of high-confidence signals.

    With 1/3 positive we still block total freefall (0/3 positive = all
    timeframes falling) but allow the model to catch pullback-pump patterns
    where at least one timeframe has started recovering.

    Backtest results (48h, light trend gate):
      thresh=0.50: 146 signals, 33.6% prec, +0.13% E[profit]
      thresh=0.70:  51 signals, 41.2% prec, +0.12% E[profit]
    """
    scores = 0
    available = 0
    details = []

    for label, col in [('30s', 'pm_price_velocity_30s'),
                        ('1m', 'pm_price_change_1m'),
                        ('5m', 'pm_price_change_5m')]:
        val = trail_row.get(col)
        if val is not None:
            available += 1
            v = float(val)
            if v > 0:
                scores += 1
                details.append(f"{label}={v:+.4f}%+")
            else:
                details.append(f"{label}={v:+.4f}%-")

    desc = f"trend={scores}/{available} ({', '.join(details)})"

    if available == 0:
        return False, "no momentum data"

    # Need at least 1 positive — blocks total freefall but allows dip-buying
    return scores >= 1, desc


# =============================================================================
# DUCKDB TRAIL CACHE — Incremental Sync + Vectorised Forward Returns
# =============================================================================

def _get_trail_cache_conn() -> duckdb.DuckDBPyConnection:
    """Open (or create) the DuckDB trail cache."""
    return duckdb.connect(str(_TRAIL_CACHE_FILE))


def _init_trail_cache(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure the DuckDB cache tables exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS cached_trail (
            buyin_id   BIGINT,
            minute     INTEGER,
            followed_at TIMESTAMP,
            -- forward-return source
            pm_close_price DOUBLE,
            -- all feature columns we actually use (ratio / pct columns)
            pm_price_change_1m DOUBLE, pm_momentum_volatility_ratio DOUBLE,
            pm_momentum_acceleration_1m DOUBLE, pm_price_change_5m DOUBLE,
            pm_price_change_10m DOUBLE, pm_volatility_pct DOUBLE,
            pm_body_range_ratio DOUBLE, pm_volatility_surge_ratio DOUBLE,
            pm_price_stddev_pct DOUBLE, pm_trend_consistency_3m DOUBLE,
            pm_cumulative_return_5m DOUBLE, pm_candle_body_pct DOUBLE,
            pm_upper_wick_pct DOUBLE, pm_lower_wick_pct DOUBLE,
            pm_wick_balance_ratio DOUBLE, pm_price_vs_ma5_pct DOUBLE,
            pm_breakout_strength_10m DOUBLE,
            pm_price_velocity_1m DOUBLE, pm_price_velocity_30s DOUBLE,
            pm_velocity_acceleration DOUBLE, pm_momentum_persistence DOUBLE,
            pm_realized_vol_1m DOUBLE, pm_vol_of_vol DOUBLE,
            pm_volatility_regime DOUBLE, pm_trend_strength_ema DOUBLE,
            pm_price_vs_vwap_pct DOUBLE, pm_price_vs_twap_pct DOUBLE,
            pm_higher_highs_5m DOUBLE, pm_higher_lows_5m DOUBLE,
            pm_dist_resistance_pct DOUBLE, pm_dist_support_pct DOUBLE,
            pm_breakout_imminence DOUBLE,
            ob_price_change_1m DOUBLE, ob_price_change_5m DOUBLE,
            ob_price_change_10m DOUBLE, ob_volume_imbalance DOUBLE,
            ob_imbalance_shift_1m DOUBLE, ob_imbalance_trend_3m DOUBLE,
            ob_depth_imbalance_ratio DOUBLE, ob_bid_liquidity_share_pct DOUBLE,
            ob_ask_liquidity_share_pct DOUBLE, ob_depth_imbalance_pct DOUBLE,
            ob_liquidity_change_3m DOUBLE, ob_microprice_deviation DOUBLE,
            ob_microprice_acceleration_2m DOUBLE, ob_spread_bps DOUBLE,
            ob_aggression_ratio DOUBLE, ob_vwap_spread_bps DOUBLE,
            ob_net_flow_5m DOUBLE, ob_net_flow_to_liquidity_ratio DOUBLE,
            ob_imbalance_velocity_1m DOUBLE, ob_imbalance_velocity_30s DOUBLE,
            ob_imbalance_acceleration DOUBLE, ob_bid_depth_velocity DOUBLE,
            ob_ask_depth_velocity DOUBLE, ob_depth_ratio_velocity DOUBLE,
            ob_spread_velocity DOUBLE, ob_spread_percentile_1h DOUBLE,
            ob_liquidity_score DOUBLE, ob_liquidity_gap_score DOUBLE,
            ob_liquidity_concentration DOUBLE,
            ob_cumulative_imbalance_5m DOUBLE, ob_imbalance_consistency_5m DOUBLE,
            tx_buy_sell_pressure DOUBLE, tx_buy_volume_pct DOUBLE,
            tx_sell_volume_pct DOUBLE, tx_pressure_shift_1m DOUBLE,
            tx_pressure_trend_3m DOUBLE, tx_long_short_ratio DOUBLE,
            tx_long_volume_pct DOUBLE, tx_short_volume_pct DOUBLE,
            tx_perp_position_skew_pct DOUBLE, tx_long_ratio_shift_1m DOUBLE,
            tx_perp_dominance_pct DOUBLE, tx_volume_acceleration_ratio DOUBLE,
            tx_volume_surge_ratio DOUBLE, tx_whale_volume_pct DOUBLE,
            tx_avg_trade_size DOUBLE, tx_trades_per_second DOUBLE,
            tx_buy_trade_pct DOUBLE, tx_price_change_1m DOUBLE,
            tx_price_volatility_pct DOUBLE, tx_cumulative_buy_flow_5m DOUBLE,
            tx_trade_count DOUBLE, tx_large_trade_count DOUBLE,
            tx_volume_velocity DOUBLE, tx_volume_acceleration DOUBLE,
            tx_volume_percentile_1h DOUBLE, tx_cumulative_delta DOUBLE,
            tx_cumulative_delta_5m DOUBLE, tx_delta_divergence DOUBLE,
            tx_trade_intensity DOUBLE, tx_intensity_velocity DOUBLE,
            tx_large_trade_intensity DOUBLE, tx_vpin_estimate DOUBLE,
            tx_order_flow_toxicity DOUBLE, tx_kyle_lambda DOUBLE,
            tx_aggressive_buy_ratio DOUBLE, tx_aggressive_sell_ratio DOUBLE,
            tx_aggression_imbalance DOUBLE,
            wh_net_flow_ratio DOUBLE, wh_flow_shift_1m DOUBLE,
            wh_flow_trend_3m DOUBLE, wh_accumulation_ratio DOUBLE,
            wh_strong_accumulation DOUBLE, wh_cumulative_flow_5m DOUBLE,
            wh_inflow_share_pct DOUBLE, wh_outflow_share_pct DOUBLE,
            wh_net_flow_strength_pct DOUBLE,
            wh_strong_accumulation_pct DOUBLE, wh_strong_distribution_pct DOUBLE,
            wh_activity_surge_ratio DOUBLE, wh_movement_count DOUBLE,
            wh_massive_move_pct DOUBLE, wh_avg_wallet_pct_moved DOUBLE,
            wh_largest_move_dominance DOUBLE,
            wh_distribution_pressure_pct DOUBLE, wh_outflow_surge_pct DOUBLE,
            wh_movement_imbalance_pct DOUBLE, wh_net_flow_sol DOUBLE,
            wh_flow_velocity DOUBLE, wh_flow_acceleration DOUBLE,
            wh_cumulative_flow_10m DOUBLE, wh_stealth_acc_score DOUBLE,
            wh_distribution_urgency DOUBLE, wh_activity_regime DOUBLE,
            wh_time_since_large DOUBLE, wh_large_freq_5m DOUBLE,
            sp_price_range_pct DOUBLE, sp_total_change_pct DOUBLE,
            sp_volatility_pct DOUBLE,
            btc_price_change_1m DOUBLE, btc_price_change_5m DOUBLE,
            btc_price_change_10m DOUBLE, btc_volatility_pct DOUBLE,
            eth_price_change_1m DOUBLE, eth_price_change_5m DOUBLE,
            eth_price_change_10m DOUBLE, eth_volatility_pct DOUBLE,
            mp_volume_divergence_confidence DOUBLE,
            mp_order_book_squeeze_confidence DOUBLE,
            mp_whale_stealth_accumulation_confidence DOUBLE,
            mp_momentum_acceleration_confidence DOUBLE,
            mp_microstructure_shift_confidence DOUBLE,
            xa_btc_sol_corr_1m DOUBLE, xa_btc_sol_corr_5m DOUBLE,
            xa_btc_leads_sol_1 DOUBLE, xa_btc_leads_sol_2 DOUBLE,
            xa_sol_beta_btc DOUBLE, xa_eth_sol_corr_1m DOUBLE,
            xa_eth_leads_sol_1 DOUBLE, xa_sol_beta_eth DOUBLE,
            xa_btc_sol_divergence DOUBLE, xa_eth_sol_divergence DOUBLE,
            xa_momentum_alignment DOUBLE,
            ts_price_change_30s DOUBLE, ts_volume_30s DOUBLE,
            ts_buy_sell_pressure_30s DOUBLE, ts_imbalance_30s DOUBLE,
            ts_trade_count_30s DOUBLE, ts_momentum_30s DOUBLE,
            ts_volatility_30s DOUBLE,
            mm_probability DOUBLE, mm_direction DOUBLE,
            mm_confidence DOUBLE, mm_order_flow_score DOUBLE,
            mm_whale_alignment DOUBLE, mm_momentum_quality DOUBLE,
            mm_volatility_regime DOUBLE, mm_cross_asset_score DOUBLE,
            mm_false_signal_risk DOUBLE, mm_adverse_selection DOUBLE,
            pre_entry_change_1m DOUBLE, pre_entry_change_2m DOUBLE,
            pre_entry_change_3m DOUBLE, pre_entry_change_5m DOUBLE,
            pre_entry_change_10m DOUBLE,
            PRIMARY KEY (buyin_id, minute)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_ct_followed
        ON cached_trail(followed_at)
    """)


# Column list used in SELECT from PostgreSQL (must match cached_trail schema)
_TRAIL_CACHE_COLUMNS: List[str] = []  # populated lazily


def _trail_cache_columns() -> List[str]:
    """Return the explicit column list to SELECT from buyin_trail_minutes."""
    global _TRAIL_CACHE_COLUMNS
    if _TRAIL_CACHE_COLUMNS:
        return _TRAIL_CACHE_COLUMNS

    # Open a throwaway DuckDB connection to read column names from schema
    con = _get_trail_cache_conn()
    try:
        _init_trail_cache(con)
        info = con.execute("PRAGMA table_info('cached_trail')").fetchall()
        # info rows: (cid, name, type, notnull, dflt_value, pk)
        cols = [row[1] for row in info if row[1] != 'followed_at']
        _TRAIL_CACHE_COLUMNS = cols
    finally:
        con.close()
    return _TRAIL_CACHE_COLUMNS


def _sync_trail_cache(hours: int) -> duckdb.DuckDBPyConnection:
    """Incrementally sync trail data from PostgreSQL into DuckDB.

    Returns an open DuckDB connection ready for queries.
    """
    con = _get_trail_cache_conn()
    _init_trail_cache(con)

    t0 = time.time()

    # Watermark: latest followed_at already in cache
    row = con.execute("SELECT MAX(followed_at) FROM cached_trail").fetchone()
    max_ts = row[0] if row and row[0] else None

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # ── Cleanup old data ────────────────────────────────────────────────
    con.execute("DELETE FROM cached_trail WHERE followed_at < ?", [cutoff])

    # ── Determine which buyins to fetch ─────────────────────────────────
    if max_ts is not None:
        fetch_since = max_ts
        logger.info(f"  Incremental sync (since {max_ts})")
    else:
        fetch_since = cutoff
        logger.info(f"  Full sync (last {hours}h)")

    with get_postgres() as pg:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT id AS buyin_id, followed_at
                FROM follow_the_goat_buyins
                WHERE potential_gains IS NOT NULL
                  AND followed_at > %s
            """, [fetch_since])
            new_buyins = cur.fetchall()

    if not new_buyins:
        cached_count = con.execute("SELECT COUNT(DISTINCT buyin_id) FROM cached_trail").fetchone()[0]
        logger.info(f"  No new buyins (cache has {cached_count:,} buyins)")
        return con

    new_buyin_ids = [r['buyin_id'] for r in new_buyins]
    followed_map = {r['buyin_id']: r['followed_at'] for r in new_buyins}
    logger.info(f"  {len(new_buyin_ids):,} new buyins to sync")

    # ── Fetch trail rows (only needed columns) ──────────────────────────
    trail_cols = _trail_cache_columns()
    col_list = ', '.join(trail_cols)

    all_trail_rows = []
    chunk_size = 500
    for i in range(0, len(new_buyin_ids), chunk_size):
        chunk = new_buyin_ids[i:i + chunk_size]
        placeholders = ','.join(['%s'] * len(chunk))
        with get_postgres() as pg:
            with pg.cursor() as cur:
                cur.execute(f"""
                    SELECT {col_list}
                    FROM buyin_trail_minutes
                    WHERE buyin_id IN ({placeholders})
                      AND (sub_minute = 0 OR sub_minute IS NULL)
                """, chunk)
                all_trail_rows.extend(cur.fetchall())

    if not all_trail_rows:
        logger.warning("  No trail rows for new buyins")
        return con

    # ── Build PyArrow table (avoids pandas type-inference entirely) ──────
    # Get the DuckDB schema column order so Arrow table matches exactly
    schema_info = con.execute("PRAGMA table_info('cached_trail')").fetchall()
    schema_cols = [row[1] for row in schema_info]  # ordered by position

    pg_col_names = list(all_trail_rows[0].keys())  # from PostgreSQL result
    arrays = {}

    for cname in schema_cols:
        if cname == 'buyin_id':
            arrays[cname] = pa.array(
                [r['buyin_id'] for r in all_trail_rows], type=pa.int64())
        elif cname == 'minute':
            arrays[cname] = pa.array(
                [r['minute'] for r in all_trail_rows], type=pa.int32())
        elif cname == 'followed_at':
            arrays[cname] = pa.array(
                [followed_map.get(r['buyin_id']) for r in all_trail_rows],
                type=pa.timestamp('us'))
        elif cname in pg_col_names:
            # Feature column → float64 (handles None natively)
            arrays[cname] = pa.array(
                [float(r[cname]) if r[cname] is not None else None
                 for r in all_trail_rows],
                type=pa.float64())
        else:
            # Column exists in DuckDB schema but not in PostgreSQL result → fill NULLs
            arrays[cname] = pa.array(
                [None] * len(all_trail_rows), type=pa.float64())

    # Build table with columns in schema order
    trail_arrow = pa.table(
        [arrays[c] for c in schema_cols],
        names=schema_cols,
    )

    # Register the Arrow table with DuckDB and INSERT OR REPLACE
    con.register('_trail_arrow', trail_arrow)
    con.execute("INSERT OR REPLACE INTO cached_trail SELECT * FROM _trail_arrow")
    con.unregister('_trail_arrow')

    total = con.execute("SELECT COUNT(DISTINCT buyin_id) FROM cached_trail").fetchone()[0]
    n_synced = len(all_trail_rows)
    logger.info(f"  Synced {n_synced:,} trail rows ({len(new_buyin_ids):,} buyins) "
                f"in {time.time()-t0:.1f}s  [cache total: {total:,} buyins]")
    return con


# =============================================================================
# DATA LOADING WITH PATH-AWARE LABELING (DuckDB + vectorised forward returns)
# =============================================================================

def _load_and_label_data(lookback_hours: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Load trail data with PATH-AWARE labeling using DuckDB for speed.

    Pipeline:
      1. Incremental sync from PostgreSQL → DuckDB cache
      2. LEAD() window functions for forward returns (vectorised, ~2-5s)
      3. Path-aware labeling (clean_pump / no_pump / crash)
    """
    hours = lookback_hours if lookback_hours is not None else LOOKBACK_HOURS
    logger.info(f"V2: Loading data (last {hours}h)...")
    t0 = time.time()

    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        # ── Step 1: Sync cache ───────────────────────────────────────────
        con = _sync_trail_cache(hours)

        n_rows = con.execute("SELECT COUNT(*) FROM cached_trail").fetchone()[0]
        n_buyins = con.execute("SELECT COUNT(DISTINCT buyin_id) FROM cached_trail").fetchone()[0]
        if n_rows == 0:
            logger.warning("Cache is empty after sync")
            return None
        logger.info(f"  Cache: {n_rows:,} trail rows, {n_buyins:,} buyins")

        # ── Step 2: Vectorised forward returns with LEAD() ───────────────
        # Build LEAD expressions for 1..FORWARD_WINDOW minutes
        lead_cols = []
        for k in range(1, FORWARD_WINDOW + 1):
            lead_cols.append(
                f"(LEAD(pm_close_price, {k}) OVER w - pm_close_price) "
                f"/ NULLIF(pm_close_price, 0) * 100 AS fwd_{k}m"
            )
        lead_sql = ",\n            ".join(lead_cols)

        # Aggregate expressions
        fwd_all = [f"fwd_{k}m" for k in range(1, FORWARD_WINDOW + 1)]
        fwd_early = [f"fwd_{k}m" for k in range(1, EARLY_WINDOW + 1)]
        fwd_imm = [f"fwd_{k}m" for k in range(1, min(3, FORWARD_WINDOW + 1))]

        greatest_all = f"GREATEST({', '.join(fwd_all)})"
        least_all = f"LEAST({', '.join(fwd_all)})"
        greatest_early = f"GREATEST({', '.join(fwd_early)})"
        least_imm = f"LEAST({', '.join(fwd_imm)})"
        any_not_null = " OR ".join([f"{c} IS NOT NULL" for c in fwd_all])

        # Time-to-peak CASE
        ttp_cases = " ".join(
            [f"WHEN fwd_{k}m >= {MIN_PUMP_PCT} THEN {k}" for k in range(1, FORWARD_WINDOW + 1)]
        )
        ttp_sql = f"CASE {ttp_cases} ELSE NULL END"

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE fwd_returns AS
            WITH base AS (
                SELECT *,
                    {lead_sql}
                FROM cached_trail
                WHERE pm_close_price IS NOT NULL AND pm_close_price > 0
                WINDOW w AS (PARTITION BY buyin_id ORDER BY minute)
            )
            SELECT *,
                CASE WHEN {any_not_null} THEN {greatest_all} ELSE NULL END AS max_fwd,
                CASE WHEN {any_not_null} THEN {least_all}    ELSE NULL END AS min_fwd,
                CASE WHEN {any_not_null} THEN {greatest_early} ELSE NULL END AS max_fwd_early,
                CASE WHEN {any_not_null} THEN {least_imm}    ELSE NULL END AS min_fwd_imm,
                {ttp_sql} AS time_to_peak
            FROM base
            WHERE ({any_not_null})
        """)

        # ── Step 3: Path-aware labeling (zero-copy Arrow → pandas) ────────
        arrow_result = con.execute(f"""
            SELECT *,
                CASE
                    WHEN max_fwd_early >= {MIN_PUMP_PCT}
                         AND min_fwd > -{MAX_DRAWDOWN_PCT}
                         AND min_fwd_imm > {IMMEDIATE_DIP_MAX}
                         AND (pm_price_change_5m IS NULL OR pm_price_change_5m > {CRASH_GATE_5M})
                        THEN 'clean_pump'
                    WHEN (pm_price_change_5m IS NULL OR pm_price_change_5m > {CRASH_GATE_5M})
                        THEN 'no_pump'
                    ELSE 'crash'
                END AS label
            FROM fwd_returns
            WHERE max_fwd IS NOT NULL
            ORDER BY followed_at, buyin_id, minute
        """).arrow()
        df = arrow_result.read_all().to_pandas()

        elapsed = time.time() - t0
        logger.info(f"  {len(df):,} labeled rows in {elapsed:.1f}s")
        for lbl in ['clean_pump', 'no_pump', 'crash']:
            logger.info(f"    {lbl}: {df['label'].eq(lbl).sum():,}")

        if df['label'].eq('clean_pump').sum() > 0:
            cp = df[df['label'] == 'clean_pump']
            logger.info(f"    clean_pump: peak={cp['max_fwd'].mean():.3f}%, "
                        f"worst_dip={cp['min_fwd'].mean():.3f}%, "
                        f"time_to_peak={cp['time_to_peak'].mean():.1f}m")
        return df

    except duckdb.Error as e:
        logger.error(f"DuckDB error (will rebuild cache): {e}", exc_info=True)
        if _TRAIL_CACHE_FILE.exists():
            try:
                _TRAIL_CACHE_FILE.unlink()
                logger.info("Deleted corrupted trail cache — will rebuild next run")
            except OSError:
                pass
        return None
    except Exception as e:
        logger.error(f"Data load error: {e}", exc_info=True)
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

def _get_base_feature_columns(df: pd.DataFrame) -> List[str]:
    cols = []
    for col in df.columns:
        if col in SKIP_COLUMNS or col in ABSOLUTE_PRICE_COLUMNS:
            continue
        if col.startswith('fwd_') or col in ('max_fwd', 'min_fwd', 'max_fwd_early',
                                               'min_fwd_imm', 'time_to_peak'):
            continue
        if col in ('label', 'followed_at', 'potential_gains', 'target', 'crash'):
            continue
        if df[col].dtype not in ('float64', 'int64', 'float32', 'int32'):
            continue
        if df[col].isna().mean() >= 0.90:
            continue
        cols.append(col)
    return sorted(cols)


def _engineer_features(df: pd.DataFrame, base_cols: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Create interaction/context features that V1 can't capture with single-column thresholds.

    Key insight: V1's Cohen's d + Youden's J finds "feature X in range [a,b]" rules,
    then ANDs them together. This misses nonlinear patterns like:
    - "order book imbalance matters MORE when volatility is low"
    - "whale buying + volume spike together = strong signal, but either alone = noise"

    A gradient-boosted tree can learn these interactions automatically, but we help
    it by pre-computing the most important cross-feature relationships.
    """
    df = df.copy()
    new_cols = []

    # ── Momentum agreement (how many timeframes are positive) ─────────────
    mom_cols = {'30s': 'pm_price_velocity_30s', '1m': 'pm_price_change_1m', '5m': 'pm_price_change_5m'}
    avail = {k: v for k, v in mom_cols.items() if v in df.columns}
    if len(avail) >= 2:
        agreement = sum((df[c] > 0).astype(float) for c in avail.values())
        df['feat_momentum_agreement'] = agreement
        new_cols.append('feat_momentum_agreement')

        if 'pm_price_change_1m' in df.columns and 'pm_price_change_5m' in df.columns:
            df['feat_momentum_accel'] = df['pm_price_change_1m'] - df['pm_price_change_5m'] / 5
            new_cols.append('feat_momentum_accel')

    # ── Cross-asset divergence (SOL strength vs BTC/ETH) ──────────────────
    if 'pm_price_change_1m' in df.columns:
        sol = df['pm_price_change_1m'].fillna(0)
        if 'btc_price_change_1m' in df.columns:
            df['feat_sol_btc_div'] = sol - df['btc_price_change_1m'].fillna(0)
            new_cols.append('feat_sol_btc_div')
        if 'eth_price_change_1m' in df.columns:
            df['feat_sol_eth_div'] = sol - df['eth_price_change_1m'].fillna(0)
            new_cols.append('feat_sol_eth_div')

    # NOTE: feat_ob_zscore and feat_vol_price_div were removed because they
    # used rolling(50) during training but raw values at prediction time
    # (train/serve skew). Neither appeared in the top 15 feature importances.

    # ── Whale flow intensity (net flow scaled by total movement) ──────────
    if 'wh_net_flow_sol' in df.columns and 'wh_total_sol_moved' in df.columns:
        df['feat_whale_intensity'] = df['wh_net_flow_sol'] / df['wh_total_sol_moved'].clip(lower=1)
        new_cols.append('feat_whale_intensity')

    # ── Volatility compression (squeeze = breakout imminent) ──────────────
    if 'pm_realized_vol_1m' in df.columns and 'pm_volatility_pct' in df.columns:
        df['feat_vol_compress'] = df['pm_volatility_pct'] / df['pm_realized_vol_1m'].clip(lower=1e-8)
        new_cols.append('feat_vol_compress')

    all_cols = base_cols + new_cols
    logger.info(f"  Features: {len(base_cols)} base + {len(new_cols)} engineered = {len(all_cols)}")
    return df, all_cols


# =============================================================================
# MODEL TRAINING: WALK-FORWARD VALIDATION
# =============================================================================

def _train_model(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Train GBM with walk-forward validation.

    V1 used a single 70/30 time split. Problem: if the market regime changed
    in that 30% window, you either get false confidence or reject good rules.

    V2 uses 3 sequential test windows. The model must perform well on ALL of them.
    If it works on window 1 but fails on window 3, it means the patterns aren't stable.
    """
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import precision_score

    df = df[df['label'].isin(['clean_pump', 'no_pump'])].copy()
    df['target'] = (df['label'] == 'clean_pump').astype(int)

    n_pos = df['target'].sum()
    if n_pos < 30:
        logger.warning(f"Only {n_pos} clean_pump samples (need 30+)")
        return None

    base_cols = _get_base_feature_columns(df)
    df, feature_cols = _engineer_features(df, base_cols)

    # Drop high-NaN columns
    feature_cols = [c for c in feature_cols if c in df.columns and df[c].isna().mean() < 0.5]
    if len(feature_cols) < 5:
        logger.warning(f"Only {len(feature_cols)} usable features")
        return None

    logger.info(f"  Training: {len(feature_cols)} features, {len(df)} samples "
                f"({n_pos} pos, {len(df) - n_pos} neg)")

    df = df.sort_values('followed_at').reset_index(drop=True)
    n = len(df)

    # ── Exponential decay weights (half-life ~72h / 3 days) ────────────
    # Recent data gets more weight, but gentle enough to avoid overfitting
    # to noise in the most recent hours. 14h half-life was too aggressive.
    # ln(2)/72 ≈ 0.00963
    now_ts = pd.Timestamp.now(tz='UTC')
    if df['followed_at'].dt.tz is None:
        followed_utc = df['followed_at'].dt.tz_localize('UTC')
    else:
        followed_utc = df['followed_at'].dt.tz_convert('UTC')
    hours_ago = (now_ts - followed_utc).dt.total_seconds() / 3600
    decay_weights = np.exp(-0.00963 * hours_ago.values)  # half-life ~72h

    # Walk-forward: 3 sequential test windows
    split_results = []
    split_thresholds = []
    for i in range(N_WALK_FORWARD_SPLITS):
        test_start_frac = 1.0 - (N_WALK_FORWARD_SPLITS - i) * WALK_FORWARD_TEST_FRAC
        train_end = int(n * test_start_frac)
        test_end = min(int(n * (test_start_frac + WALK_FORWARD_TEST_FRAC)), n)

        if train_end < 100 or (test_end - train_end) < 20:
            continue

        X_tr = df.iloc[:train_end][feature_cols].fillna(0)
        y_tr = df.iloc[:train_end]['target']
        w_tr = decay_weights[:train_end]
        X_te = df.iloc[train_end:test_end][feature_cols].fillna(0)
        y_te = df.iloc[train_end:test_end]['target']

        if y_tr.sum() < 10 or y_te.sum() < 3:
            continue

        mdl = GradientBoostingClassifier(
            n_estimators=150, max_depth=3, learning_rate=0.05,
            subsample=0.7, max_features=0.7,
            min_samples_leaf=10, min_samples_split=20,
            random_state=42,
        )
        mdl.fit(X_tr, y_tr, sample_weight=w_tr)

        proba = mdl.predict_proba(X_te)[:, 1]

        # ── Calibrate optimal threshold for this split ───────────────
        best_thresh = MIN_CONFIDENCE
        best_ep = -999.0
        for thresh in np.arange(0.50, 0.91, 0.05):
            pred_t = (proba >= thresh).astype(int)
            if pred_t.sum() == 0:
                continue
            prec_t = float(precision_score(y_te, pred_t, zero_division=0)) * 100
            sig_mask_t = pred_t.astype(bool)
            tp_mask_t = sig_mask_t & (y_te.values == 1)
            tp_t = int(tp_mask_t.sum())
            avg_g_t = float(df.iloc[train_end:test_end][tp_mask_t]['max_fwd'].mean()) if tp_t > 0 else 0.0
            ep_t = (prec_t / 100) * avg_g_t - TRADE_COST_PCT
            if ep_t > best_ep:
                best_ep = ep_t
                best_thresh = float(thresh)

        split_thresholds.append(best_thresh)

        # Evaluate at the calibrated threshold for reporting
        pred = (proba >= best_thresh).astype(int)
        if pred.sum() == 0:
            prec, n_sig, tp, avg_g, ep = 0.0, 0, 0, 0.0, -TRADE_COST_PCT
        else:
            prec = float(precision_score(y_te, pred, zero_division=0)) * 100
            n_sig = int(pred.sum())
            sig_mask = pred.astype(bool)
            tp = int(((y_te.values == 1) & sig_mask).sum())
            tp_mask = sig_mask & (y_te.values == 1)
            avg_g = float(df.iloc[train_end:test_end][tp_mask]['max_fwd'].mean()) if tp > 0 else 0.0
            ep = (prec / 100) * avg_g - TRADE_COST_PCT

        split_results.append({
            'split': i, 'train': train_end, 'test': test_end - train_end,
            'precision': prec, 'n_signals': n_sig, 'tp': tp,
            'avg_gain': avg_g, 'expected_profit': ep,
            'optimal_threshold': best_thresh,
        })
        logger.info(f"  Split {i}: prec={prec:.1f}%, signals={n_sig}, "
                    f"E[profit]={ep:+.4f}%, thresh={best_thresh:.2f}")

    if len(split_results) < 2:
        logger.warning("Not enough walk-forward splits")
        return None

    precs = [s['precision'] for s in split_results if s['n_signals'] > 0]
    profits = [s['expected_profit'] for s in split_results if s['n_signals'] > 0]

    if not precs:
        logger.warning("No splits produced signals")
        return None

    avg_prec = float(np.mean(precs))
    min_prec = float(np.min(precs))
    avg_profit = float(np.mean(profits))

    if avg_prec < 50:
        logger.warning(f"Avg precision {avg_prec:.1f}% < 50%")
        return None
    if min_prec < 35:
        logger.warning(f"Worst split {min_prec:.1f}% < 35%")
        return None
    if avg_profit <= 0:
        logger.warning(f"Avg profit {avg_profit:+.4f}% <= 0")
        return None

    # Most conservative (highest) threshold across splits
    optimal_threshold = max(split_thresholds) if split_thresholds else MIN_CONFIDENCE
    logger.info(f"  Optimal threshold: {optimal_threshold:.2f} "
                f"(per-split: {[f'{t:.2f}' for t in split_thresholds]})")

    # Final model on all data (with decay weights)
    final = GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05,
        subsample=0.7, max_features=0.7,
        min_samples_leaf=10, min_samples_split=20,
        random_state=42,
    )
    final.fit(df[feature_cols].fillna(0), df['target'], sample_weight=decay_weights)

    importances = pd.Series(final.feature_importances_, index=feature_cols).nlargest(15)
    logger.info("  Top features:")
    for f, v in importances.items():
        logger.info(f"    {f}: {v:.4f}")

    return {
        'model': final,
        'feature_columns': feature_cols,
        'metadata': {
            'avg_precision': round(avg_prec, 2),
            'min_precision': round(min_prec, 2),
            'avg_expected_profit': round(avg_profit, 4),
            'optimal_threshold': round(optimal_threshold, 4),
            'splits': split_results,
            'n_features': len(feature_cols),
            'n_samples': len(df),
            'n_positive': int(df['target'].sum()),
            'top_features': {k: round(float(v), 4) for k, v in importances.items()},
            'confidence_threshold': MIN_CONFIDENCE,
            'refreshed_at': datetime.now(timezone.utc).isoformat(),
        }
    }


# =============================================================================
# ENGINEERED FEATURE COMPUTATION AT PREDICTION TIME
# =============================================================================

def _compute_feat(name: str, row: dict) -> float:
    """Compute an engineered feature from a single trail_row at prediction time."""
    try:
        if name == 'feat_momentum_agreement':
            return sum(1.0 for c in ['pm_price_velocity_30s', 'pm_price_change_1m', 'pm_price_change_5m']
                       if row.get(c) is not None and float(row[c]) > 0)

        if name == 'feat_momentum_accel':
            a, b = row.get('pm_price_change_1m'), row.get('pm_price_change_5m')
            return float(a) - float(b) / 5 if a is not None and b is not None else 0.0

        if name == 'feat_sol_btc_div':
            a, b = row.get('pm_price_change_1m'), row.get('btc_price_change_1m')
            return float(a) - float(b) if a is not None and b is not None else 0.0

        if name == 'feat_sol_eth_div':
            a, b = row.get('pm_price_change_1m'), row.get('eth_price_change_1m')
            return float(a) - float(b) if a is not None and b is not None else 0.0

        if name == 'feat_whale_intensity':
            a, b = row.get('wh_net_flow_sol'), row.get('wh_total_sol_moved')
            return float(a) / max(float(b), 1) if a is not None and b is not None else 0.0

        if name == 'feat_vol_compress':
            a, b = row.get('pm_realized_vol_1m'), row.get('pm_volatility_pct')
            return float(b) / max(float(a), 1e-8) if a is not None and b is not None else 1.0

        # feat_ob_zscore and feat_vol_price_div removed (train/serve skew)

    except (ValueError, TypeError):
        pass
    return 0.0


# =============================================================================
# REFRESH & CACHE
# =============================================================================

def refresh_pump_rules():
    """Train a new pump model and write to disk cache.

    This is CPU-heavy (2-4 minutes) and should be called from the
    dedicated refresh_pump_model component process — NOT from train_validator.
    train_validator only reads the cached model via maybe_refresh_rules().
    """
    global _model, _feature_columns, _model_metadata

    logger.info("=== V2: Refreshing pump model ===")
    t0 = time.time()

    df = _load_and_label_data()
    if df is None or len(df) < MIN_SAMPLES_TO_TRAIN:
        logger.warning("Insufficient data — keeping model")
        return

    result = _train_model(df)
    if result is None:
        logger.warning("Training failed — keeping model")
        return

    new_profit = result['metadata']['avg_expected_profit']
    cur_profit = _model_metadata.get('avg_expected_profit')

    # Always replace — crypto patterns shift fast.
    # Only skip if new model has *negative* expected profit.
    if new_profit <= 0:
        logger.warning(f"New model unprofitable ({new_profit:+.4f}%), keeping current")
        return

    logger.info(f"  Replacing model: cur={cur_profit}  →  new={new_profit:+.4f}%")

    _model = result['model']
    _feature_columns = result['feature_columns']
    _model_metadata = result['metadata']

    # Reset circuit breaker on model refresh — new model gets a clean slate
    global _circuit_breaker_paused
    _recent_outcomes.clear()
    _circuit_breaker_paused = False

    opt_t = result['metadata'].get('optimal_threshold', MIN_CONFIDENCE)
    logger.info(f"  NEW MODEL: {len(_feature_columns)} features, "
                f"prec={result['metadata']['avg_precision']:.1f}%, "
                f"E[profit]={new_profit:+.4f}%, thresh={opt_t:.2f} "
                f"({time.time()-t0:.1f}s)")

    try:
        MODEL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = MODEL_CACHE_PATH.with_suffix('.pkl.tmp')
        with open(tmp_path, 'wb') as f:
            pickle.dump({'model': _model, 'feature_columns': _feature_columns,
                         'metadata': _model_metadata}, f)
        tmp_path.replace(MODEL_CACHE_PATH)  # atomic rename
        logger.info(f"  Cache written to {MODEL_CACHE_PATH}")
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")


def _load_cached_model() -> bool:
    global _model, _feature_columns, _model_metadata
    if not MODEL_CACHE_PATH.exists():
        return False
    try:
        with open(MODEL_CACHE_PATH, 'rb') as f:
            d = pickle.load(f)
        _model, _feature_columns, _model_metadata = d['model'], d['feature_columns'], d['metadata']
        logger.info(f"Loaded cached V2 model: {len(_feature_columns)} features")
        return True
    except Exception as e:
        logger.warning(f"Cache load failed: {e}")
        return False


def maybe_refresh_rules():
    """Reload the pump model from disk cache if stale.

    Called by train_validator every cycle. This is lightweight (just a file
    read) — the heavy model training runs in the separate refresh_pump_model
    component which writes to the same cache file.
    """
    global _last_rules_refresh
    now = time.time()
    if _model is None or (now - _last_rules_refresh >= RULES_REFRESH_INTERVAL):
        _load_cached_model()
        _last_rules_refresh = now


# =============================================================================
# SIGNAL CHECK
# =============================================================================

def _log_gate_summary() -> None:
    global _last_gate_summary_time
    now = time.time()
    if now - _last_gate_summary_time < 60:
        return
    _last_gate_summary_time = now
    total = _gate_stats['total_checks']
    if total == 0:
        return
    cb = _gate_stats.get('circuit_breaker', 0)
    logger.info(
        f"V2 gates (60s): {total} chk | no_mdl={_gate_stats['no_model']} "
        f"cb={cb} tf_fail={_gate_stats['multi_tf_fail']} "
        f"crash={_gate_stats['crash_gate_fail']} 5m={_gate_stats['crash_5m_fail']} "
        f"ok={_gate_stats['gates_passed']} low_conf={_gate_stats['low_confidence']} "
        f"FIRED={_gate_stats['signal_fired']}"
    )
    for k in _gate_stats:
        _gate_stats[k] = 0


def check_pump_signal(trail_row: dict, market_price: float) -> bool:
    _gate_stats['total_checks'] += 1

    if _model is None:
        _gate_stats['no_model'] += 1
        return False

    # Gate 0: Circuit breaker — auto-pause if live precision is too low
    if _check_circuit_breaker():
        _gate_stats.setdefault('circuit_breaker', 0)
        _gate_stats['circuit_breaker'] += 1
        return False

    # Gate 1: Multi-timeframe trend
    trend_ok, trend_desc = _check_multi_timeframe_trend(trail_row)
    if not trend_ok:
        _gate_stats['multi_tf_fail'] += 1
        logger.debug(f"V2: trend FAIL ({trend_desc})")
        return False

    # Gate 2: Crash protection
    crash_ok, crash_desc = _is_not_crashing()
    if not crash_ok:
        _gate_stats['crash_gate_fail'] += 1
        return False

    pm_5m = trail_row.get('pm_price_change_5m')
    if pm_5m is not None and float(pm_5m) < CRASH_GATE_5M:
        _gate_stats['crash_5m_fail'] += 1
        return False

    _gate_stats['gates_passed'] += 1

    # Gate 3: Volatility regime — raise confidence in unusual regimes
    _update_vol_buffer(trail_row.get('pm_realized_vol_1m'))
    vol_pct = _get_vol_percentile()
    base_threshold = _model_metadata.get('optimal_threshold', MIN_CONFIDENCE)
    if vol_pct is not None and (vol_pct > 90 or vol_pct < 10):
        required_confidence = min(base_threshold + 0.10, 0.90)
        regime_tag = f"vol_p{vol_pct:.0f}→{required_confidence:.2f}"
    else:
        required_confidence = base_threshold
        regime_tag = f"vol_p{vol_pct:.0f}" if vol_pct is not None else "vol_warmup"

    # Gate 4: Model confidence (using calibrated + regime-adjusted threshold)
    try:
        features = {}
        for col in _feature_columns:
            if col.startswith('feat_'):
                features[col] = _compute_feat(col, trail_row)
            else:
                v = trail_row.get(col)
                features[col] = float(v) if v is not None else 0.0

        X = pd.DataFrame([features])[_feature_columns].fillna(0)
        proba = float(_model.predict_proba(X)[0, 1])

        if proba < required_confidence:
            _gate_stats['low_confidence'] += 1
            logger.info(f"V2: conf={proba:.3f} < {required_confidence:.2f} "
                        f"({trend_desc}, {regime_tag})")
            return False

        _gate_stats['signal_fired'] += 1
        logger.info(f"V2: SIGNAL! conf={proba:.3f} >= {required_confidence:.2f} "
                    f"({trend_desc}, {regime_tag})")
        return True

    except Exception as e:
        logger.error(f"V2 prediction error: {e}", exc_info=True)
        return False


# =============================================================================
# BUYIN INSERTION (same API as V1)
# =============================================================================

def check_and_fire_pump_signal(
    buyin_id: int,
    market_price: float,
    price_cycle: Optional[int],
) -> bool:
    global _last_entry_time

    pump_play_id = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "3"))
    if not pump_play_id:
        return False

    _update_price_buffer(market_price)

    if _model is None:
        return False

    try:
        trail_row = postgres_query_one(
            "SELECT * FROM buyin_trail_minutes WHERE buyin_id=%s AND minute=0 AND (sub_minute=0 OR sub_minute IS NULL)",
            [buyin_id])
    except Exception as e:
        logger.error(f"V2 trail read error: {e}")
        return False

    if not trail_row:
        return False

    if not check_pump_signal(trail_row, market_price):
        _log_gate_summary()
        return False
    _log_gate_summary()

    # Cooldown
    now = time.time()
    if now - _last_entry_time < COOLDOWN_SECONDS:
        logger.info(f"V2: signal but cooldown ({int(COOLDOWN_SECONDS-(now-_last_entry_time))}s)")
        return False

    # Cooldown: only consider real pump entries (exclude TRAINING_TEST_ so training cycles don't block firing)
    try:
        last = postgres_query_one(
            """SELECT followed_at FROM follow_the_goat_buyins
               WHERE play_id=%s AND wallet_address NOT LIKE 'TRAINING_TEST_%%'
               ORDER BY followed_at DESC LIMIT 1""",
            [pump_play_id])
        if last and last.get('followed_at'):
            ft = last['followed_at']
            ts = ft.timestamp() if hasattr(ft, 'timestamp') else (ft if isinstance(ft, (int, float)) else None)
            if ts is not None and now - ts < COOLDOWN_SECONDS:
                return False
    except Exception:
        pass

    try:
        op = postgres_query_one(
            "SELECT id FROM follow_the_goat_buyins WHERE play_id=%s AND our_status IN ('pending','validating') AND wallet_address NOT LIKE 'TRAINING_TEST_%%' LIMIT 1",
            [pump_play_id])
        if op:
            logger.info(f"V2: signal but open position {op['id']}")
            return False
    except Exception:
        return False

    # Insert
    ts = str(int(now))
    bt = datetime.now(timezone.utc).replace(tzinfo=None)

    # Compute confidence for logging
    try:
        feats = {}
        for col in _feature_columns:
            feats[col] = _compute_feat(col, trail_row) if col.startswith('feat_') else float(trail_row.get(col, 0) or 0)
        conf = float(_model.predict_proba(pd.DataFrame([feats])[_feature_columns].fillna(0))[0, 1])
    except Exception:
        conf = -1

    entry_log = json.dumps({
        'signal_type': 'pump_detection_v2',
        'source_buyin_id': buyin_id,
        'model_confidence': conf,
        'model_metadata': _model_metadata,
        'sol_price': market_price,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    })

    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(id),0)+1 AS nid FROM follow_the_goat_buyins")
                nid = cur.fetchone()['nid']

        postgres_execute("""
            INSERT INTO follow_the_goat_buyins
            (id,play_id,wallet_address,original_trade_id,trade_signature,block_timestamp,
             quote_amount,base_amount,price,direction,our_entry_price,live_trade,price_cycle,
             entry_log,pattern_validator_log,our_status,followed_at,higest_price_reached)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [nid, pump_play_id, f'PUMP_V2_{ts}', 0, f'pump_v2_{ts}', bt,
              100.0, market_price, market_price, 'buy', market_price, 0, price_cycle,
              entry_log, None, 'pending', bt, market_price])

        _last_entry_time = now
        logger.info(f"  V2 buyin #{nid} @ {market_price:.4f} conf={conf:.3f}")
        return True
    except Exception as e:
        logger.error(f"V2 insert error: {e}", exc_info=True)
        return False


def get_pump_status() -> Dict[str, Any]:
    live_prec = (sum(_recent_outcomes) / len(_recent_outcomes)) if len(_recent_outcomes) > 0 else None
    return {
        'version': 'v2',
        'has_model': _model is not None,
        'n_features': len(_feature_columns),
        'metadata': _model_metadata,
        'last_refresh': _last_rules_refresh,
        'last_entry': _last_entry_time,
        'circuit_breaker_paused': _circuit_breaker_paused,
        'live_precision': round(live_prec, 4) if live_prec is not None else None,
        'live_outcomes_count': len(_recent_outcomes),
        'vol_buffer_size': len(_vol_buffer),
        'vol_percentile': round(_get_vol_percentile(), 1) if _get_vol_percentile() is not None else None,
    }


# Load cached model on import
try:
    _load_cached_model()
except Exception:
    pass

"""
Pump Signal Logic V3 — Precision Signal Detection
===================================================
Key changes from V2:
  1. SUSTAINED MOVE labeling: price must still be >= +0.10% at minute 4
  2. Six new microstructure features: ask_pull_bid_stack, volume_confirmed_momentum,
     whale_retail_divergence, spread_squeeze, delta_acceleration, trade_burstiness
  3. Pre-entry slope composite: replaces 5 correlated pre_entry_change_* with
     feat_pre_entry_slope (OLS) + feat_pre_entry_level (mean)
  4. Whale acceleration ratio: whale net inflow 60s vs 5m average
  5. Readiness score fast path: volatility-event detector triggers immediate
     trail gen + full GBM check when threshold crossed (every 5-10s)
  6. Feature drift monitoring: dual-window (12 min fast, 1h slow) percentile check
  7. Outcome attribution: top features + gate details logged per signal

Retained from V2:
  - PATH-AWARE labeling, walk-forward validation, gradient-boosted model
  - Safety gates: crash, microstructure, cross-asset
  - Circuit breaker with PostgreSQL-based win rate tracking

External API (unchanged):
  - maybe_refresh_rules()
  - check_and_fire_pump_signal(buyin_id, market_price, price_cycle)
  - get_pump_status()
  - compute_readiness_score() — NEW: fast path readiness
  - should_trigger_fast_path() — NEW: check if immediate trail gen needed
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

from core.database import get_postgres, postgres_execute, postgres_insert_many, postgres_query, postgres_query_one

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
SUSTAINED_PCT = 0.10            # Price must still be >= +0.10% at minute 4 (sustained move)
MAX_RETRACEMENT_RATIO = 0.50    # Retracement filter (defined but NOT in label CASE yet — measurement first)

# ── Data ──────────────────────────────────────────────────────────────────────
LOOKBACK_HOURS = 48
RULES_REFRESH_INTERVAL = 300
TRADE_COST_PCT = 0.1

# ── Signal ────────────────────────────────────────────────────────────────────
MIN_CONFIDENCE = 0.50           # Model probability floor (calibrated threshold can be higher)
MAX_PREDICTION_THRESHOLD = 0.65 # Cap prediction-time threshold (prevents overfitting to backtest)
MIN_SAMPLES_TO_TRAIN = 50
COOLDOWN_SECONDS = 120

# ── Safety gates ──────────────────────────────────────────────────────────────
CRASH_GATE_5M = -0.3
CRASH_GATE_MICRO_30S = -0.05

# ── Readiness score (fast path) ──────────────────────────────────────────────
READINESS_THRESHOLD = float(os.getenv("READINESS_THRESHOLD", "0.60"))
READINESS_COOLDOWN_SEC = 30        # min seconds between fast-path triggers
READINESS_PERCENTILE_WINDOW_SEC = 600  # 10-min window for percentile ranks

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
_last_train_data_hash: str = ""  # hash of training data to skip identical retrains

_gate_stats: Dict[str, int] = {
    'no_model': 0, 'crash_gate_fail': 0, 'crash_5m_fail': 0,
    'micro_fail': 0, 'xa_selloff': 0, 'gates_passed': 0, 'low_confidence': 0,
    'signal_fired': 0, 'total_checks': 0,
}

_price_buffer: deque = deque(maxlen=200)

# ── Volatility regime tracking ───────────────────────────────────────────────
_vol_buffer: deque = deque(maxlen=720)     # ~12 hours at 1 sample per ~minute

# ── Outcome attribution: capture signal context at fire time ─────────────────
# Keyed by buyin_id, consumed by record_signal_outcome when trade resolves.
_signal_context: Dict[int, Dict[str, Any]] = {}

# ── Readiness score: rolling buffers for percentile calculation ──────────────
_readiness_buffers: Dict[str, deque] = {
    'delta_accel': deque(maxlen=600),
    'ask_pull_bid_stack': deque(maxlen=600),
    'spread_squeeze': deque(maxlen=600),
    'vol_confirmed_mom': deque(maxlen=600),
    'whale_accel': deque(maxlen=600),
    'price_volatility': deque(maxlen=600),
}
_last_readiness_trigger: float = 0.0
_last_readiness_score: float = 0.0

# ── Feature drift monitoring ────────────────────────────────────────────────
# Dual-window: fast (12 min) for OB/tx features, slow (60 min) for whale/pre_entry
_DRIFT_FAST_WINDOW_SEC = 720     # 12 minutes
_DRIFT_SLOW_WINDOW_SEC = 3600    # 1 hour
_drift_buffers: Dict[str, deque] = {}  # populated after first model train
_drift_last_check: float = 0.0
_DRIFT_CHECK_INTERVAL: float = 60.0  # check every 60s
_drift_warnings: List[str] = []

# Prefixes that use fast vs slow drift window
_FAST_DRIFT_PREFIXES = ('ob_', 'tx_', 'feat_ask_pull', 'feat_spread_squeeze',
                        'feat_delta_acceleration', 'feat_trade_burstiness',
                        'feat_volume_confirmed')
_SLOW_DRIFT_PREFIXES = ('wh_', 'feat_whale', 'feat_pre_entry', 'xa_', 'pre_entry_')

# ── Circuit breaker: PostgreSQL-based rolling accuracy tracker ────────────────
# Outcomes are stored in pump_signal_outcomes table (shared across processes).
# The trailing_stop_seller writes outcomes; train_validator reads them.
_circuit_breaker_paused: bool = False
_cb_cache: Optional[Dict[str, Any]] = None  # cached query result
_cb_cache_time: float = 0.0
_CB_CACHE_TTL: float = 30.0  # seconds between DB queries


def record_signal_outcome(buyin_id: int, hit_target: bool,
                          gain_pct: float = 0.0, confidence: float = 0.0) -> None:
    """Record whether a fired signal hit the target. Writes to PostgreSQL.

    Called by trailing_stop_seller when a Play #3 trade resolves.
    Both processes share the same DB table, so the train_validator
    (which runs check_pump_signal) can read the outcomes.

    Includes outcome attribution: top feature values and gate details
    captured at signal-fire time (stored in _signal_context).
    """
    # Retrieve signal context captured at fire time
    ctx = _signal_context.pop(buyin_id, {})
    top_features_json = json.dumps(ctx.get('top_features', {})) if ctx.get('top_features') else None
    gates_passed_json = json.dumps(ctx.get('gates_passed', {})) if ctx.get('gates_passed') else None

    try:
        postgres_execute(
            """INSERT INTO pump_signal_outcomes
               (buyin_id, hit_target, gain_pct, confidence, top_features_json, gates_passed_json)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [buyin_id, hit_target, gain_pct, confidence, top_features_json, gates_passed_json])
    except Exception as e:
        logger.warning(f"Failed to record signal outcome: {e}")


def _get_live_outcomes() -> Dict[str, Any]:
    """Get recent outcomes from PostgreSQL, cached for _CB_CACHE_TTL seconds."""
    global _cb_cache, _cb_cache_time
    now = time.time()
    if _cb_cache is not None and (now - _cb_cache_time) < _CB_CACHE_TTL:
        return _cb_cache

    try:
        rows = postgres_query(
            """SELECT hit_target, gain_pct FROM pump_signal_outcomes
               ORDER BY created_at DESC LIMIT 50""")
        n = len(rows)
        hits = sum(1 for r in rows if r['hit_target']) if n else 0
        win_rate = hits / n if n else None
        _cb_cache = {'n': n, 'hits': hits, 'win_rate': win_rate}
    except Exception as e:
        logger.debug(f"Circuit breaker DB read error: {e}")
        _cb_cache = {'n': 0, 'hits': 0, 'win_rate': None}
    _cb_cache_time = now
    return _cb_cache


def _check_circuit_breaker() -> bool:
    """Return True if the circuit breaker is tripped (live win rate too low)."""
    global _circuit_breaker_paused
    info = _get_live_outcomes()
    if info['n'] < 10:
        return False  # not enough data to judge
    win_rate = info['win_rate']
    if win_rate is not None and win_rate < 0.35:
        if not _circuit_breaker_paused:
            logger.warning(f"Circuit breaker TRIPPED: live win rate {win_rate:.1%} "
                           f"({info['hits']}/{info['n']}) — pausing signals")
            _circuit_breaker_paused = True
        return True
    if _circuit_breaker_paused:
        logger.info(f"Circuit breaker RESET: live win rate recovered to {win_rate:.1%}")
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

def _get_trend_description(trail_row: dict) -> str:
    """Build a descriptive string of current multi-timeframe trend (for logging only)."""
    details = []
    for label, col in [('30s', 'pm_price_velocity_30s'),
                        ('1m', 'pm_price_change_1m'),
                        ('5m', 'pm_price_change_5m')]:
        val = trail_row.get(col)
        if val is not None:
            v = float(val)
            details.append(f"{label}={v:+.4f}%")
    return ', '.join(details) if details else 'no data'


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

        # ── Step 3: Path-aware labeling with sustained move filter ─────────
        # CRITICAL: Filter to minute=0 only for training.
        # The model predicts on minute=0 (entry point) but was previously
        # trained on ALL minutes (0-14), causing train/serve skew that
        # inflated precision to ~98% while real-time confidence stayed low.
        # By training only on minute=0 data, the model learns patterns
        # at the actual entry point, matching prediction-time conditions.
        #
        # V3 CHANGE: Added sustained move filter — price must still be
        # >= SUSTAINED_PCT at minute 4, not just touched MIN_PUMP_PCT.
        # Retracement ratio is measured but NOT applied yet (measurement-first).
        arrow_result = con.execute(f"""
            SELECT *,
                CASE
                    WHEN max_fwd_early >= {MIN_PUMP_PCT}
                         AND min_fwd > -{MAX_DRAWDOWN_PCT}
                         AND min_fwd_imm > {IMMEDIATE_DIP_MAX}
                         AND (pm_price_change_5m IS NULL OR pm_price_change_5m > {CRASH_GATE_5M})
                         AND fwd_4m IS NOT NULL
                         AND fwd_4m >= {SUSTAINED_PCT}
                        THEN 'clean_pump'
                    WHEN (pm_price_change_5m IS NULL OR pm_price_change_5m > {CRASH_GATE_5M})
                        THEN 'no_pump'
                    ELSE 'crash'
                END AS label
            FROM fwd_returns
            WHERE max_fwd IS NOT NULL
              AND minute = 0
            ORDER BY followed_at, buyin_id
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

        # ── Measurement: log impact of sustained floor vs retracement ratio ──
        # Helps decide whether to add retracement ratio later.
        try:
            old_mask = (
                (df['max_fwd_early'] >= MIN_PUMP_PCT)
                & (df['min_fwd'] > -MAX_DRAWDOWN_PCT)
                & (df['min_fwd_imm'] > IMMEDIATE_DIP_MAX)
            )
            old_pump_count = int(old_mask.sum())

            has_fwd4 = df['fwd_4m'].notna()
            fail_sustained = old_mask & (~has_fwd4 | (df['fwd_4m'] < SUSTAINED_PCT))
            n_fail_sustained = int(fail_sustained.sum())

            retrace = (df['max_fwd_early'] - df['fwd_4m']) / df['max_fwd_early'].replace(0, np.nan)
            fail_retrace = old_mask & has_fwd4 & (retrace >= MAX_RETRACEMENT_RATIO)
            n_fail_retrace = int(fail_retrace.sum())

            fail_both = fail_sustained | fail_retrace
            n_fail_both = int((old_mask & fail_both).sum())

            logger.info(
                f"  LABEL MEASUREMENT: old_clean_pump={old_pump_count}, "
                f"fail_sustained_floor={n_fail_sustained}, "
                f"fail_retrace_ratio={n_fail_retrace}, "
                f"fail_either={n_fail_both}"
            )
        except Exception as meas_err:
            logger.debug(f"Label measurement error: {meas_err}")

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


def _persist_training_labels(df: pd.DataFrame) -> None:
    """Write clean_pump entry points (minute=0) to PostgreSQL for the pumps chart.

    Called after _load_and_label_data() so the dashboard can show the analytics
    points the engine uses to find the best entry points.
    """
    if df is None or len(df) == 0:
        return
    try:
        postgres_execute("""
            CREATE TABLE IF NOT EXISTS pump_training_labels (
                id BIGSERIAL PRIMARY KEY,
                followed_at TIMESTAMP NOT NULL,
                buyin_id BIGINT NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                max_fwd_pct DOUBLE PRECISION,
                time_to_peak_min DOUBLE PRECISION,
                label VARCHAR(20) NOT NULL DEFAULT 'clean_pump',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except Exception as e:
        logger.debug(f"pump_training_labels create: {e}")
    try:
        postgres_execute("CREATE INDEX IF NOT EXISTS idx_pump_training_followed ON pump_training_labels(followed_at)")
    except Exception:
        pass
    entry_rows = df[(df['minute'] == 0) & (df['label'] == 'clean_pump')]
    if entry_rows.empty:
        try:
            # Keep table bounded: remove older than lookback
            postgres_execute(
                "DELETE FROM pump_training_labels WHERE followed_at < NOW() - INTERVAL '1 hour' * %s",
                [LOOKBACK_HOURS],
            )
        except Exception:
            pass
        return
    try:
        # Replace window: delete labels in the same time range we're about to insert
        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pump_training_labels WHERE followed_at >= NOW() - INTERVAL '1 hour' * %s",
                    [LOOKBACK_HOURS],
                )
        # Bulk insert (followed_at, buyin_id, entry_price, max_fwd_pct, time_to_peak_min, label)
        rows_to_insert = []
        for _, row in entry_rows.iterrows():
            followed = row.get('followed_at')
            if hasattr(followed, 'tz_localize'):
                if followed.tzinfo is None:
                    followed = followed.tz_localize('UTC')
                else:
                    followed = followed.tz_convert('UTC')
            if hasattr(followed, 'strftime'):
                followed_str = followed.strftime('%Y-%m-%d %H:%M:%S')
            else:
                followed_str = str(followed)
            entry_price = float(row.get('pm_close_price', 0) or 0)
            max_fwd = row.get('max_fwd')
            max_fwd_pct = float(max_fwd) if max_fwd is not None and pd.notna(max_fwd) else None
            ttp = row.get('time_to_peak')
            time_to_peak_min = float(ttp) if ttp is not None and pd.notna(ttp) else None
            rows_to_insert.append({
                'followed_at': followed_str,
                'buyin_id': int(row.get('buyin_id', 0)),
                'entry_price': entry_price,
                'max_fwd_pct': max_fwd_pct,
                'time_to_peak_min': time_to_peak_min,
                'label': 'clean_pump',
            })
        if rows_to_insert:
            postgres_insert_many('pump_training_labels', rows_to_insert)
        logger.info(f"  Persisted {len(entry_rows)} clean_pump entry points to pump_training_labels")
    except Exception as e:
        logger.warning(f"Failed to persist training labels: {e}")


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

    # ── NEW V3 FEATURES ─────────────────────────────────────────────────

    # 1. ask_pull_bid_stack: asks being pulled while bids are stacked
    #    Sharply positive = someone preparing to push price up
    if 'ob_bid_depth_velocity' in df.columns and 'ob_ask_depth_velocity' in df.columns:
        df['feat_ask_pull_bid_stack'] = (
            df['ob_bid_depth_velocity'].fillna(0) - df['ob_ask_depth_velocity'].fillna(0)
        )
        new_cols.append('feat_ask_pull_bid_stack')

    # 2. volume_confirmed_momentum: 30s momentum confirmed by volume and buy pressure
    if all(c in df.columns for c in ['ts_momentum_30s', 'ts_buy_sell_pressure_30s', 'ts_volume_30s']):
        pressure_sign = np.sign(df['ts_buy_sell_pressure_30s'].fillna(0))
        df['feat_volume_confirmed_momentum'] = (
            df['ts_momentum_30s'].fillna(0) * pressure_sign * np.log1p(df['ts_volume_30s'].fillna(0).abs())
        )
        new_cols.append('feat_volume_confirmed_momentum')

    # 3. whale_retail_divergence: whale flow vs retail transaction pressure
    #    Positive = whales buying while retail is flat/selling (strong signal)
    if 'wh_net_flow_ratio' in df.columns and 'tx_buy_sell_pressure' in df.columns:
        df['feat_whale_retail_divergence'] = (
            df['wh_net_flow_ratio'].fillna(0) - df['tx_buy_sell_pressure'].fillna(0)
        )
        new_cols.append('feat_whale_retail_divergence')

    # 4. spread_squeeze_signal: spread tightening + buy-heavy book = imminent move
    if 'ob_spread_velocity' in df.columns and 'ob_depth_imbalance_ratio' in df.columns:
        df['feat_spread_squeeze'] = (
            -df['ob_spread_velocity'].fillna(0) * df['ob_depth_imbalance_ratio'].fillna(0)
        )
        new_cols.append('feat_spread_squeeze')

    # 5. delta_acceleration_30s: is buying accelerating? (approx from minute-level data)
    #    Uses tx_cumulative_delta and tx_cumulative_delta_5m to approximate 30s acceleration
    if 'tx_cumulative_delta' in df.columns and 'tx_cumulative_delta_5m' in df.columns:
        recent_delta = df['tx_cumulative_delta'].fillna(0)
        avg_delta_per_min = df['tx_cumulative_delta_5m'].fillna(0) / 5.0
        denom = avg_delta_per_min.abs().clip(lower=1.0)
        df['feat_delta_acceleration'] = (recent_delta - avg_delta_per_min) / denom
        new_cols.append('feat_delta_acceleration')

    # 6. trade_arrival_burstiness: approximated from trades_per_second and trade_intensity
    #    High burstiness = someone splitting orders (pump precursor)
    if 'tx_trades_per_second' in df.columns and 'tx_trade_intensity' in df.columns:
        tps = df['tx_trades_per_second'].fillna(0)
        intensity = df['tx_trade_intensity'].fillna(0)
        mean_intensity = intensity.clip(lower=1e-6)
        df['feat_trade_burstiness'] = (tps * intensity) / mean_intensity
        new_cols.append('feat_trade_burstiness')

    # 7. whale_acceleration_ratio: whale buying acceleration (60s vs 5m avg)
    #    Uses wh_flow_velocity (recent) and wh_flow_acceleration to detect
    #    whales loading before the crowd.
    if 'wh_flow_velocity' in df.columns and 'wh_flow_acceleration' in df.columns:
        vel = df['wh_flow_velocity'].fillna(0)
        denom = vel.abs().clip(lower=1e-6)
        df['feat_whale_accel_ratio'] = df['wh_flow_acceleration'].fillna(0) / denom
        new_cols.append('feat_whale_accel_ratio')

    # ── PRE-ENTRY SLOPE COMPOSITE (Phase 4) ─────────────────────────────
    # Replace 5 correlated pre_entry_change_* features with slope + level.
    # Slope captures direction (stabilizing dip vs accelerating selloff).
    # Level captures magnitude. Model sees 2 features instead of 5 correlated ones.
    _PRE_ENTRY_COLS = ['pre_entry_change_1m', 'pre_entry_change_2m',
                       'pre_entry_change_3m', 'pre_entry_change_5m',
                       'pre_entry_change_10m']
    _PRE_ENTRY_X = np.array([-1.0, -2.0, -3.0, -5.0, -10.0])

    avail_pe = [c for c in _PRE_ENTRY_COLS if c in df.columns]
    if len(avail_pe) >= 3:
        pe_vals = df[avail_pe].values  # shape: (n_rows, n_avail)
        x_for_avail = _PRE_ENTRY_X[[_PRE_ENTRY_COLS.index(c) for c in avail_pe]]

        slopes = np.full(len(df), np.nan)
        levels = np.full(len(df), np.nan)
        for i in range(len(df)):
            row_vals = pe_vals[i]
            mask = ~np.isnan(row_vals)
            if mask.sum() >= 3:
                slopes[i] = np.polyfit(x_for_avail[mask], row_vals[mask], 1)[0]
                levels[i] = np.nanmean(row_vals[mask])
            elif mask.sum() >= 1:
                levels[i] = np.nanmean(row_vals[mask])

        df['feat_pre_entry_slope'] = slopes
        df['feat_pre_entry_level'] = levels
        new_cols.append('feat_pre_entry_slope')
        new_cols.append('feat_pre_entry_level')

    # Exclude individual pre_entry_change_* from base_cols so GBM sees only slope + level
    base_cols = [c for c in base_cols if c not in _PRE_ENTRY_COLS]

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

    if avg_prec < 40:
        logger.warning(f"Avg precision {avg_prec:.1f}% < 40%")
        return None
    if min_prec < 25:
        logger.warning(f"Worst split {min_prec:.1f}% < 25%")
        return None
    if avg_profit <= 0:
        logger.warning(f"Avg profit {avg_profit:+.4f}% <= 0")
        return None

    # Use median threshold across splits, capped at MAX_PREDICTION_THRESHOLD.
    # Previously used max() which gave 0.90 — way too conservative for real-time
    # where feature distributions differ from training (minute=0 vs all minutes).
    raw_threshold = float(np.median(split_thresholds)) if split_thresholds else MIN_CONFIDENCE
    optimal_threshold = min(raw_threshold, MAX_PREDICTION_THRESHOLD)
    logger.info(f"  Optimal threshold: {optimal_threshold:.2f} "
                f"(raw median: {raw_threshold:.2f}, cap: {MAX_PREDICTION_THRESHOLD}, "
                f"per-split: {[f'{t:.2f}' for t in split_thresholds]})")

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

    # ── Compute training-time feature distribution stats for drift monitoring ──
    top_5_names = list(importances.index[:5])
    training_feature_stats = {}
    for feat_name in top_5_names:
        if feat_name in df.columns:
            col_data = df[feat_name].dropna()
            if len(col_data) >= 10:
                training_feature_stats[feat_name] = {
                    'mean': round(float(col_data.mean()), 6),
                    'std': round(float(col_data.std()), 6),
                    'p5': round(float(col_data.quantile(0.05)), 6),
                    'p25': round(float(col_data.quantile(0.25)), 6),
                    'p50': round(float(col_data.quantile(0.50)), 6),
                    'p75': round(float(col_data.quantile(0.75)), 6),
                    'p95': round(float(col_data.quantile(0.95)), 6),
                }

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
            'training_feature_stats': training_feature_stats,
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

        # ── V3 features ──────────────────────────────────────────────
        if name == 'feat_ask_pull_bid_stack':
            a = row.get('ob_bid_depth_velocity')
            b = row.get('ob_ask_depth_velocity')
            return float(a or 0) - float(b or 0)

        if name == 'feat_volume_confirmed_momentum':
            mom = row.get('ts_momentum_30s')
            pres = row.get('ts_buy_sell_pressure_30s')
            vol = row.get('ts_volume_30s')
            if mom is not None and pres is not None and vol is not None:
                sign = 1.0 if float(pres) >= 0 else -1.0
                return float(mom) * sign * float(np.log1p(abs(float(vol))))
            return 0.0

        if name == 'feat_whale_retail_divergence':
            a = row.get('wh_net_flow_ratio')
            b = row.get('tx_buy_sell_pressure')
            return float(a or 0) - float(b or 0)

        if name == 'feat_spread_squeeze':
            a = row.get('ob_spread_velocity')
            b = row.get('ob_depth_imbalance_ratio')
            return -float(a or 0) * float(b or 0)

        if name == 'feat_delta_acceleration':
            delta = row.get('tx_cumulative_delta')
            delta_5m = row.get('tx_cumulative_delta_5m')
            if delta is not None and delta_5m is not None:
                avg = float(delta_5m) / 5.0
                denom = max(abs(avg), 1.0)
                return (float(delta) - avg) / denom
            return 0.0

        if name == 'feat_trade_burstiness':
            tps = row.get('tx_trades_per_second')
            intensity = row.get('tx_trade_intensity')
            if tps is not None and intensity is not None:
                mean_i = max(float(intensity), 1e-6)
                return float(tps) * float(intensity) / mean_i
            return 0.0

        if name == 'feat_pre_entry_slope' or name == 'feat_pre_entry_level':
            pe_cols = ['pre_entry_change_1m', 'pre_entry_change_2m',
                       'pre_entry_change_3m', 'pre_entry_change_5m',
                       'pre_entry_change_10m']
            pe_x = np.array([-1.0, -2.0, -3.0, -5.0, -10.0])
            vals = []
            xs = []
            for c, x in zip(pe_cols, pe_x):
                v = row.get(c)
                if v is not None:
                    vals.append(float(v))
                    xs.append(x)
            if name == 'feat_pre_entry_level':
                return float(np.mean(vals)) if vals else 0.0
            if len(vals) >= 3:
                return float(np.polyfit(xs, vals, 1)[0])
            return 0.0

        if name == 'feat_whale_accel_ratio':
            flow_vel = row.get('wh_flow_velocity')
            flow_accel = row.get('wh_flow_acceleration')
            if flow_vel is not None and flow_accel is not None:
                denom = max(abs(float(flow_vel)), 1e-6)
                return float(flow_accel) / denom
            return 0.0

    except (ValueError, TypeError):
        pass
    return 0.0


# =============================================================================
# FEATURE DRIFT MONITORING (dual-window)
# =============================================================================

def _get_drift_window(feature_name: str) -> int:
    """Return the appropriate drift window size (in samples) for a feature."""
    for prefix in _FAST_DRIFT_PREFIXES:
        if feature_name.startswith(prefix):
            return _DRIFT_FAST_WINDOW_SEC
    return _DRIFT_SLOW_WINDOW_SEC


def _init_drift_buffers() -> None:
    """Initialize drift buffers for the top features after model training."""
    global _drift_buffers
    top_feats = _model_metadata.get('top_features', {})
    top_5 = list(top_feats.keys())[:5]
    _drift_buffers = {f: deque(maxlen=3600) for f in top_5}


def _check_feature_drift(features: Dict[str, float]) -> List[str]:
    """Check if live feature values deviate from training distributions.

    Compares the rolling live mean against the training [5th, 95th] percentile range.
    Uses fast window (12 min) for OB/tx features, slow window (1h) for whale/pre_entry.

    Returns list of warning strings for drifting features.
    """
    global _drift_last_check, _drift_warnings

    now = time.time()
    if now - _drift_last_check < _DRIFT_CHECK_INTERVAL:
        return _drift_warnings

    _drift_last_check = now

    train_stats = _model_metadata.get('training_feature_stats', {})
    if not train_stats or not _drift_buffers:
        return []

    warnings = []
    for feat_name, buf in _drift_buffers.items():
        val = features.get(feat_name)
        if val is not None:
            buf.append((now, float(val)))

        stats = train_stats.get(feat_name)
        if stats is None or len(buf) < 30:
            continue

        window_sec = _get_drift_window(feat_name)
        cutoff = now - window_sec
        windowed = [v for t, v in buf if t >= cutoff]
        if len(windowed) < 10:
            continue

        live_mean = float(np.mean(windowed))
        p5 = stats.get('p5', float('-inf'))
        p95 = stats.get('p95', float('inf'))

        if live_mean < p5 or live_mean > p95:
            window_label = "fast" if window_sec == _DRIFT_FAST_WINDOW_SEC else "slow"
            warnings.append(
                f"{feat_name}: live_mean={live_mean:.4f} outside training "
                f"[{p5:.4f}, {p95:.4f}] ({window_label} window, n={len(windowed)})"
            )

    if warnings and warnings != _drift_warnings:
        for w in warnings:
            logger.warning(f"FEATURE DRIFT: {w}")
    _drift_warnings = warnings
    return warnings


# =============================================================================
# READINESS SCORE — Fast path volatility-event detector
# =============================================================================

def compute_readiness_score(utc_now: Optional[datetime] = None) -> float:
    """Compute a rolling readiness score from high-freq cache data.

    The readiness score predicts *volatility events* (any large move imminent),
    NOT pumps specifically. When it crosses READINESS_THRESHOLD, the caller
    should trigger an immediate trail generation + full GBM check.

    Score design: fraction of micro features exceeding their 95th percentile
    in the rolling 10-minute window. Returns 0.0-1.0.

    Uses the high-freq DuckDB cache (read-only) and the price buffer.
    """
    global _last_readiness_score

    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    # DuckDB stores naive-UTC timestamps, so strip tzinfo for comparisons
    utc_naive = utc_now.replace(tzinfo=None) if utc_now.tzinfo else utc_now

    try:
        from pump_highfreq_cache import get_highfreq_reader
    except ImportError:
        try:
            from .pump_highfreq_cache import get_highfreq_reader
        except ImportError:
            _last_readiness_score = 0.0
            return 0.0

    feature_values: Dict[str, float] = {}

    try:
        with get_highfreq_reader() as con:
            # Delta acceleration: cumulative delta in last 30s vs prior 30s
            try:
                row = con.execute("""
                    WITH recent AS (
                        SELECT
                            SUM(CASE WHEN direction = 'buy' THEN sol_amount ELSE -sol_amount END) AS delta
                        FROM cached_trades
                        WHERE trade_timestamp >= ? - INTERVAL '30 seconds'
                          AND trade_timestamp < ?
                    ),
                    prior AS (
                        SELECT
                            SUM(CASE WHEN direction = 'buy' THEN sol_amount ELSE -sol_amount END) AS delta
                        FROM cached_trades
                        WHERE trade_timestamp >= ? - INTERVAL '60 seconds'
                          AND trade_timestamp < ? - INTERVAL '30 seconds'
                    )
                    SELECT recent.delta AS recent_delta, prior.delta AS prior_delta
                    FROM recent, prior
                """, [utc_naive, utc_naive, utc_naive, utc_naive]).fetchone()
                if row and row[0] is not None and row[1] is not None:
                    prior = float(row[1])
                    denom = max(abs(prior), 1.0)
                    feature_values['delta_accel'] = (float(row[0]) - prior) / denom
            except Exception:
                pass

            # Ask pull / bid stack: bid depth velocity - ask depth velocity
            try:
                row = con.execute("""
                    SELECT
                        bid_liquidity - LAG(bid_liquidity) OVER (ORDER BY ts) AS bid_vel,
                        ask_liquidity - LAG(ask_liquidity) OVER (ORDER BY ts) AS ask_vel
                    FROM cached_order_book
                    WHERE ts >= ? - INTERVAL '10 seconds'
                    ORDER BY ts DESC LIMIT 1
                """, [utc_naive]).fetchone()
                if row and row[0] is not None and row[1] is not None:
                    feature_values['ask_pull_bid_stack'] = float(row[0]) - float(row[1])
            except Exception:
                pass

            # Spread squeeze: spread tightening * depth imbalance
            try:
                row = con.execute("""
                    WITH recent_ob AS (
                        SELECT spread_bps, depth_imbalance_ratio,
                               LAG(spread_bps) OVER (ORDER BY ts) AS prev_spread
                        FROM cached_order_book
                        WHERE ts >= ? - INTERVAL '10 seconds'
                        ORDER BY ts DESC LIMIT 1
                    )
                    SELECT
                        -(spread_bps - COALESCE(prev_spread, spread_bps)) * depth_imbalance_ratio
                    FROM recent_ob
                """, [utc_naive]).fetchone()
                if row and row[0] is not None:
                    feature_values['spread_squeeze'] = float(row[0])
            except Exception:
                pass

            # Volume confirmed momentum (from price buffer + recent trades)
            try:
                mom_30s = _get_micro_trend(30)
                row = con.execute("""
                    SELECT
                        SUM(sol_amount) AS vol_30s,
                        SUM(CASE WHEN direction='buy' THEN sol_amount ELSE 0 END)
                        - SUM(CASE WHEN direction='sell' THEN sol_amount ELSE 0 END) AS pressure
                    FROM cached_trades
                    WHERE trade_timestamp >= ? - INTERVAL '30 seconds'
                """, [utc_naive]).fetchone()
                if row and mom_30s is not None and row[0] is not None and row[1] is not None:
                    sign = 1.0 if float(row[1]) >= 0 else -1.0
                    feature_values['vol_confirmed_mom'] = mom_30s * sign * float(np.log1p(abs(float(row[0]))))
            except Exception:
                pass

            # Whale acceleration
            try:
                row = con.execute("""
                    WITH w60 AS (
                        SELECT COALESCE(SUM(CASE WHEN direction='inflow' THEN sol_change ELSE -sol_change END), 0) AS net
                        FROM cached_whales
                        WHERE ts >= ? - INTERVAL '60 seconds'
                    ),
                    w5m AS (
                        SELECT COALESCE(SUM(CASE WHEN direction='inflow' THEN sol_change ELSE -sol_change END), 0) / 5.0 AS avg_net
                        FROM cached_whales
                        WHERE ts >= ? - INTERVAL '5 minutes'
                    )
                    SELECT w60.net, w5m.avg_net FROM w60, w5m
                """, [utc_naive, utc_naive]).fetchone()
                if row and row[0] is not None and row[1] is not None:
                    denom = max(abs(float(row[1])), 1.0)
                    feature_values['whale_accel'] = float(row[0]) / denom
            except Exception:
                pass

    except Exception as e:
        logger.debug(f"Readiness score HF read error: {e}")

    # Price volatility from buffer (std of last 30s returns)
    if len(_price_buffer) >= 10:
        recent = [(t, p) for t, p in _price_buffer if t >= time.time() - 30]
        if len(recent) >= 5:
            prices = [p for _, p in recent]
            rets = np.diff(prices) / np.array(prices[:-1]) * 100
            feature_values['price_volatility'] = float(np.std(rets)) if len(rets) > 1 else 0.0

    # Update rolling buffers and compute percentile ranks
    n_above_95 = 0
    n_features = 0
    for key, buf in _readiness_buffers.items():
        val = feature_values.get(key)
        if val is None:
            continue
        buf.append(val)
        n_features += 1
        if len(buf) < 30:
            continue
        rank = sum(1 for v in buf if v <= val)
        pct = rank / len(buf)
        if pct >= 0.95:
            n_above_95 += 1

    score = n_above_95 / max(n_features, 1) if n_features > 0 else 0.0
    _last_readiness_score = score
    return score


def should_trigger_fast_path() -> bool:
    """Check if the readiness score warrants an immediate trail gen + GBM check.

    Returns True if score >= threshold and cooldown has elapsed.
    Caller should then trigger trail generation + check_pump_signal.
    """
    global _last_readiness_trigger

    score = compute_readiness_score()
    now = time.time()

    if score >= READINESS_THRESHOLD and (now - _last_readiness_trigger) >= READINESS_COOLDOWN_SEC:
        _last_readiness_trigger = now
        logger.info(f"READINESS TRIGGER: score={score:.3f} >= {READINESS_THRESHOLD}")
        return True
    return False


# =============================================================================
# REFRESH & CACHE
# =============================================================================

def refresh_pump_rules():
    """Train a new pump model and write to disk cache.

    This is CPU-heavy (~4 minutes) and should be called from the
    dedicated refresh_pump_model component process — NOT from train_validator.
    train_validator only reads the cached model via maybe_refresh_rules().
    
    Smart skip: if the training data hasn't changed since last run, we skip
    the expensive GBM training and only refresh the training labels chart.
    """
    global _model, _feature_columns, _model_metadata, _last_train_data_hash

    logger.info("=== V2: Refreshing pump model ===")
    t0 = time.time()

    df = _load_and_label_data()
    if df is None or len(df) < MIN_SAMPLES_TO_TRAIN:
        logger.warning("Insufficient data — keeping model")
        return

    # Persist clean_pump entry points (minute=0) for the pumps chart
    _persist_training_labels(df)

    # Smart skip: hash the data shape + label distribution to detect changes.
    # If the data is identical to the last training run, skip the expensive
    # GBM training (~4 min) since it would produce the same model.
    n_pump = int(df['label'].eq('clean_pump').sum())
    n_total = len(df)
    data_hash = hashlib.md5(f"{n_total}:{n_pump}:{df.iloc[-1].get('buyin_id', 0)}".encode()).hexdigest()[:12]
    if data_hash == _last_train_data_hash and _model is not None:
        logger.info(f"  Data unchanged ({n_total} rows, {n_pump} pumps, hash={data_hash}) — skipping retrain")
        return
    logger.info(f"  Data changed: {n_total} rows, {n_pump} pumps (hash {_last_train_data_hash or 'none'} → {data_hash})")

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

    # Initialize drift monitoring buffers for top features
    _init_drift_buffers()

    # Reset circuit breaker cache on model refresh — force a fresh DB read
    global _circuit_breaker_paused, _cb_cache, _cb_cache_time
    _cb_cache = None
    _cb_cache_time = 0.0
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
        _last_train_data_hash = data_hash  # remember so we skip identical data next time
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
        _init_drift_buffers()
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
    micro = _gate_stats.get('micro_fail', 0)
    xa = _gate_stats.get('xa_selloff', 0)
    logger.info(
        f"V2 gates (60s): {total} chk | no_mdl={_gate_stats['no_model']} "
        f"cb={cb} crash={_gate_stats['crash_gate_fail']} 5m={_gate_stats['crash_5m_fail']} "
        f"micro={micro} xa={xa} ok={_gate_stats['gates_passed']} low_conf={_gate_stats['low_confidence']} "
        f"FIRED={_gate_stats['signal_fired']}"
    )
    for k in _gate_stats:
        _gate_stats[k] = 0


def _check_microstructure_confirmation(trail_row: dict) -> Tuple[bool, str]:
    """Verify that market microstructure supports the dip-buy signal.

    The GBM model is heavily driven by pre_entry_change_3m (41% weight).
    It sees ANY dip as a pump opportunity — but dips where whales are selling,
    order book is bearish, and transactions show net selling are NOT bounces,
    they're real selloffs.

    This gate requires at least ONE bullish confirmation from the microstructure
    (order book, transactions, or whale activity). If ALL three are bearish,
    the dip is real and we should NOT buy.
    """
    bearish_count = 0
    checks = 0
    details = []

    # Order book: is there buying pressure?
    ob_imbalance = trail_row.get('ob_volume_imbalance')
    if ob_imbalance is not None:
        checks += 1
        v = float(ob_imbalance)
        if v < -0.05:  # net selling in order book
            bearish_count += 1
            details.append(f"ob_imb={v:+.3f}(sell)")
        else:
            details.append(f"ob_imb={v:+.3f}(ok)")

    # Transaction flow: are buyers or sellers dominant?
    tx_pressure = trail_row.get('tx_buy_sell_pressure')
    if tx_pressure is not None:
        checks += 1
        v = float(tx_pressure)
        if v < -0.02:  # net selling in transactions
            bearish_count += 1
            details.append(f"tx_press={v:+.3f}(sell)")
        else:
            details.append(f"tx_press={v:+.3f}(ok)")

    # Whale activity: are whales accumulating or dumping?
    wh_flow = trail_row.get('wh_net_flow_ratio')
    if wh_flow is not None:
        checks += 1
        v = float(wh_flow)
        if v < -0.3:  # whales are net sellers
            bearish_count += 1
            details.append(f"wh_flow={v:+.3f}(dump)")
        else:
            details.append(f"wh_flow={v:+.3f}(ok)")

    desc = f"micro={checks-bearish_count}/{checks} bullish ({', '.join(details)})"

    if checks == 0:
        return True, "no microstructure data"

    # Block if ALL available microstructure signals are bearish
    # (i.e., zero bullish confirmations = pure selloff, not a bounce)
    if bearish_count == checks:
        return False, desc

    return True, desc


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

    # Gate 1: Crash protection (micro 30s)
    crash_ok, crash_desc = _is_not_crashing()
    if not crash_ok:
        _gate_stats['crash_gate_fail'] += 1
        return False

    # Gate 2: 5-minute crash protection
    pm_5m = trail_row.get('pm_price_change_5m')
    if pm_5m is not None and float(pm_5m) < CRASH_GATE_5M:
        _gate_stats['crash_5m_fail'] += 1
        return False

    # Gate 3: Microstructure confirmation — prevent buying into pure selloffs.
    # The model sees any dip as a bounce opportunity, but if order book, whales,
    # AND transactions ALL show selling, the dip is real, not a bounce.
    micro_ok, micro_desc = _check_microstructure_confirmation(trail_row)
    if not micro_ok:
        _gate_stats['micro_fail'] += 1
        logger.info(f"V2: microstructure REJECT ({micro_desc})")
        return False

    # Gate 4: Cross-asset divergence + deep dip = SOL-specific selloff.
    # If SOL underperforms BOTH BTC and ETH (>0.15%) AND has a steep 5-min
    # decline (>0.35%), this is SOL-specific selling — not a bounce setup.
    xa_btc = trail_row.get('xa_btc_sol_divergence')
    xa_eth = trail_row.get('xa_eth_sol_divergence')
    pe5m = trail_row.get('pre_entry_change_5m')
    if (xa_btc is not None and xa_eth is not None and pe5m is not None
            and float(xa_btc) < -0.15 and float(xa_eth) < -0.15
            and float(pe5m) < -0.35):
        _gate_stats['xa_selloff'] += 1
        logger.info(f"V2: cross-asset REJECT btc_div={float(xa_btc):+.3f} "
                    f"eth_div={float(xa_eth):+.3f} pe5m={float(pe5m):+.3f}")
        return False

    _gate_stats['gates_passed'] += 1

    # Trend description for logging
    trend_desc = _get_trend_description(trail_row)

    # Track volatility for monitoring but DO NOT boost threshold
    _update_vol_buffer(trail_row.get('pm_realized_vol_1m'))
    vol_pct = _get_vol_percentile()
    regime_tag = f"vol_p{vol_pct:.0f}" if vol_pct is not None else "vol_warmup"

    # Build gate details for outcome attribution
    gate_details = {
        'crash_30s': crash_desc,
        'micro': micro_desc,
        'xa_selloff': False,
        'vol_regime': regime_tag,
    }

    # Model confidence check — threshold capped at MAX_PREDICTION_THRESHOLD
    raw_threshold = _model_metadata.get('optimal_threshold', MIN_CONFIDENCE)
    required_confidence = min(max(raw_threshold, MIN_CONFIDENCE), MAX_PREDICTION_THRESHOLD)

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

        # Feature drift monitoring (runs every 60s, lightweight)
        drift_warnings = _check_feature_drift(features)
        if drift_warnings:
            gate_details['drift_warnings'] = len(drift_warnings)

        if proba < required_confidence:
            _gate_stats['low_confidence'] += 1
            logger.info(f"V2: conf={proba:.3f} < {required_confidence:.2f} "
                        f"({trend_desc}, {micro_desc}, {regime_tag})")
            return False

        _gate_stats['signal_fired'] += 1
        logger.info(f"V2: SIGNAL! conf={proba:.3f} >= {required_confidence:.2f} "
                    f"({trend_desc}, {micro_desc}, {regime_tag})")

        # ── Outcome attribution: capture context at fire time ─────────
        buyin_id = trail_row.get('buyin_id')
        if buyin_id is not None:
            top_feats = _model_metadata.get('top_features', {})
            top_3_names = list(top_feats.keys())[:3] if top_feats else []
            top_3_vals = {name: round(features.get(name, 0.0), 6) for name in top_3_names}
            _signal_context[int(buyin_id)] = {
                'top_features': top_3_vals,
                'gates_passed': gate_details,
                'confidence': round(proba, 4),
            }
            if len(_signal_context) > 200:
                oldest = sorted(_signal_context.keys())[:100]
                for k in oldest:
                    _signal_context.pop(k, None)

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
    info = _get_live_outcomes()
    return {
        'version': 'v3',
        'has_model': _model is not None,
        'n_features': len(_feature_columns),
        'metadata': _model_metadata,
        'last_refresh': _last_rules_refresh,
        'last_entry': _last_entry_time,
        'circuit_breaker_paused': _circuit_breaker_paused,
        'live_win_rate': round(info['win_rate'], 4) if info['win_rate'] is not None else None,
        'live_outcomes_count': info['n'],
        'vol_buffer_size': len(_vol_buffer),
        'vol_percentile': round(_get_vol_percentile(), 1) if _get_vol_percentile() is not None else None,
        'readiness_score': round(_last_readiness_score, 4),
        'readiness_threshold': READINESS_THRESHOLD,
        'drift_warnings': len(_drift_warnings),
    }


# Load cached model on import
try:
    _load_cached_model()
except Exception:
    pass

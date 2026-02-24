"""
Pump Signal Logic V4 — Fingerprint-Based Signal Detection
==========================================================
V4 replaces the GBM model with empirical fingerprint rules:
  1. Fingerprint analysis (pump_fingerprint.py) runs every 5 minutes
  2. Discovers repeatable patterns from 7 days of data
  3. Only fires on patterns that have appeared AND succeeded multiple times
  4. Transparent, debuggable rules instead of black-box ML

Key changes from V3:
  - GBM model replaced by rule matching (pattern clusters + feature combinations)
  - 7 safety gates reduced to 3 (crash, chase, circuit breaker)
  - Removed gates (trend, microstructure, whale dump, cross-asset) are now
    encoded in the fingerprint rules themselves
  - Per-rule hit rate tracking for adaptive rule enable/disable
  - Lookback extended from 48h to 168h (7 days) for better pattern discovery

Retained from V3:
  - DuckDB trail cache + incremental sync (_sync_trail_cache)
  - Circuit breaker with PostgreSQL-based win rate tracking
  - Price buffer and micro trend detection
  - Cooldown logic (COOLDOWN_SECONDS)
  - Readiness score fast path (volatility-event detector)
  - Outcome attribution via _signal_context

V3 GBM code is kept as fallback (not deleted, just unreferenced).

External API (unchanged signatures):
  - maybe_refresh_rules()
  - check_and_fire_pump_signal(buyin_id, market_price, price_cycle)
  - get_pump_status()
  - record_signal_outcome(buyin_id, hit_target, gain_pct, confidence)
  - compute_readiness_score()
  - should_trigger_fast_path()
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
IMMEDIATE_DIP_MAX = -0.12       # Max allowed dip in first 2 min after entry
SUSTAINED_PCT = 0.10            # Price must still be >= +0.10% at minute 4 (sustained move)
MAX_RETRACEMENT_RATIO = 0.50    # Retracement filter (defined but NOT in label CASE yet — measurement first)

# ── Data ──────────────────────────────────────────────────────────────────────
LOOKBACK_HOURS = 24
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
TREND_GATE_EMA_THRESHOLD = -0.02       # pm_trend_strength_ema below this = downtrend (was -0.03)
TREND_GATE_5M_THRESHOLD = -0.05        # pm_price_change_5m below this + EMA = confirmed downtrend (was -0.08)
TREND_GATE_1M_STANDALONE = -0.10       # pm_price_change_1m below this = block regardless of EMA
WHALE_DUMP_THRESHOLD = -0.5            # wh_net_flow_ratio below this = heavy whale selling
WHALE_DUMP_TX_REQUIRED = 0.20          # tx_buy_sell_pressure must exceed this when whales dump

# ── Chase prevention gate ────────────────────────────────────────────────────
# Block entries right after a spike — buying local tops that often reverse.
CHASE_GATE_1M_MAX = 0.15               # pm_price_change_1m above this = chasing a spike
CHASE_GATE_30S_MAX = 0.10              # pm_price_velocity_30s above this = chasing micro spike

# ── Readiness score (fast path) ──────────────────────────────────────────────
READINESS_THRESHOLD = float(os.getenv("READINESS_THRESHOLD", "0.60"))
READINESS_COOLDOWN_SEC = 30        # min seconds between fast-path triggers
READINESS_PERCENTILE_WINDOW_SEC = 600  # 10-min window for percentile ranks

# ── Observation mode ─────────────────────────────────────────────────────────
# Log signals but don't execute trades. Set to "0" to enable live trading.
PUMP_OBSERVATION_MODE = os.getenv("PUMP_OBSERVATION_MODE", "1") == "1"

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
    'no_rules': 0, 'crash_gate_fail': 0, 'crash_5m_fail': 0,
    'chase_gate_fail': 0, 'gates_passed': 0,
    'no_pattern': 0, 'no_combo': 0,
    'signal_fired': 0, 'total_checks': 0,
    'circuit_breaker': 0,
}

_price_buffer: deque = deque(maxlen=200)

# ── Simulator rules cache (loaded from simulation_results PostgreSQL table) ──
# Keyed by play_id so each play maintains its own filtered rule set.
_sim_rules: Dict[int, List[Dict[str, Any]]] = {}
_sim_rules_loaded_at: Dict[int, float] = {}
SIM_RULES_TTL: float = 300.0  # refresh every 5 min

# ── Per-play last-entry timestamp (replaces single global _last_entry_time) ──
_last_entry_time_per_play: Dict[int, float] = {}

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

# ── V4 Fingerprint rules (replaces GBM model) ────────────────────────────────
_rules: Optional[Dict[str, Any]] = None
_rules_metadata: Dict[str, Any] = {}
_FINGERPRINT_REPORT_PATH = _PROJECT_ROOT / "cache" / "pump_fingerprint_report.json"
_current_volume_regime: str = "unknown"  # regime when rules were discovered
_rule_performance: Dict[str, Dict[str, Any]] = {}  # per-rule hit rates / disabled+boosted flags


def record_signal_outcome(buyin_id: int, hit_target: bool,
                          gain_pct: float = 0.0, confidence: float = 0.0) -> None:
    """Record whether a fired signal hit the target. Writes to PostgreSQL.

    Called by trailing_stop_seller when a Play #3 trade resolves.
    Both processes share the same DB table, so the train_validator
    (which runs check_pump_signal) can read the outcomes.

    V4: Also stores rule_id and pattern_id for per-rule tracking.
    """
    # Retrieve signal context captured at fire time
    ctx = _signal_context.pop(buyin_id, {})
    top_features_json = json.dumps(ctx.get('top_features', {})) if ctx.get('top_features') else None
    gates_passed_json = json.dumps(ctx.get('gates_passed', {})) if ctx.get('gates_passed') else None
    rule_id = ctx.get('rule_id')
    pattern_id = ctx.get('pattern_id')

    try:
        postgres_execute(
            """INSERT INTO pump_signal_outcomes
               (buyin_id, hit_target, gain_pct, confidence,
                top_features_json, gates_passed_json, rule_id, pattern_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [buyin_id, hit_target, gain_pct, confidence,
             top_features_json, gates_passed_json, rule_id, pattern_id])
    except Exception as e:
        logger.warning(f"Failed to record signal outcome: {e}")


def _get_live_outcomes() -> Dict[str, Any]:
    """Get recent outcomes from PostgreSQL, cached for _CB_CACHE_TTL seconds.

    Uses last 30 signals (reduced from 50) with time-weighted win rate:
    outcomes from the last 48h are weighted 2x vs older outcomes, so
    recent regime shifts dominate the circuit breaker decision.
    """
    global _cb_cache, _cb_cache_time
    now = time.time()
    if _cb_cache is not None and (now - _cb_cache_time) < _CB_CACHE_TTL:
        return _cb_cache

    try:
        rows = postgres_query(
            """SELECT hit_target, gain_pct, created_at FROM pump_signal_outcomes
               ORDER BY created_at DESC LIMIT 30""")
        n = len(rows)
        hits = sum(1 for r in rows if r['hit_target']) if n else 0

        # Time-weighted win rate: 2x weight for outcomes in last 48h
        cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
        cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
        w_hits, w_total = 0.0, 0.0
        n_recent_24h = 0
        for r in rows:
            created = r.get('created_at')
            if created is not None:
                # psycopg2 returns timezone-aware datetime; ensure comparable
                if hasattr(created, 'tzinfo') and created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                w = 2.0 if created >= cutoff_48h else 1.0
                if created >= cutoff_24h:
                    n_recent_24h += 1
            else:
                w = 1.0
            w_total += w
            if r.get('hit_target'):
                w_hits += w

        win_rate = w_hits / w_total if w_total > 0 else None
        _cb_cache = {'n': n, 'hits': hits, 'win_rate': win_rate, 'n_recent_24h': n_recent_24h}
    except Exception as e:
        logger.debug(f"Circuit breaker DB read error: {e}")
        _cb_cache = {'n': 0, 'hits': 0, 'win_rate': None, 'n_recent_24h': 0}
    _cb_cache_time = now
    return _cb_cache


def _check_circuit_breaker() -> bool:
    """Return True if the circuit breaker is tripped (live win rate too low).

    The CB only activates when there are at least 5 recent outcomes in the
    last 24h.  This prevents stale historical data from a prior system or
    rule set from permanently blocking new signals — a common issue after
    rule refreshes or system migrations.
    """
    global _circuit_breaker_paused
    info = _get_live_outcomes()
    n_recent = info.get('n_recent_24h', 0)
    if n_recent < 5:
        # Not enough fresh signal history — let new rules prove themselves
        if _circuit_breaker_paused:
            logger.info(f"Circuit breaker RESET: only {n_recent} outcomes in last 24h "
                        f"(need 5 to engage CB)")
            _circuit_breaker_paused = False
        return False
    win_rate = info['win_rate']
    if win_rate is not None and win_rate < 0.35:
        if not _circuit_breaker_paused:
            logger.warning(f"Circuit breaker TRIPPED: live win rate {win_rate:.1%} "
                           f"({info['hits']}/{info['n']}, {n_recent} in 24h) — pausing signals")
            _circuit_breaker_paused = True
        return True
    if _circuit_breaker_paused:
        logger.info(f"Circuit breaker RESET: live win rate recovered to {win_rate:.1%}")
        _circuit_breaker_paused = False
    return False


def _compute_rule_performance() -> Dict[str, Dict[str, Any]]:
    """Query pump_signal_outcomes to get per-rule hit rates over last 24h.

    Returns: {rule_id: {n_fires, n_hits, hit_rate, avg_gain, disabled}}

    Rules with hit_rate < 35% over 10+ fires: marked as disabled.
    Rules with hit_rate > 60% over 5+ fires: marked as boosted.
    """
    try:
        rows = postgres_query("""
            SELECT rule_id, pattern_id, hit_target, gain_pct
            FROM pump_signal_outcomes
            WHERE created_at >= NOW() - INTERVAL '24 hours'
              AND (rule_id IS NOT NULL OR pattern_id IS NOT NULL)
            ORDER BY created_at DESC
        """)
    except Exception as e:
        logger.debug(f"Rule performance query failed: {e}")
        return {}

    if not rows:
        return {}

    # Aggregate by rule_id and pattern_id
    perf: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        for key_col in ('rule_id', 'pattern_id'):
            key = row.get(key_col)
            if not key:
                continue
            if key not in perf:
                perf[key] = {'n_fires': 0, 'n_hits': 0, 'total_gain': 0.0}
            perf[key]['n_fires'] += 1
            if row.get('hit_target'):
                perf[key]['n_hits'] += 1
            gain = row.get('gain_pct') or 0.0
            perf[key]['total_gain'] += float(gain)

    # Compute rates and disable/boost flags
    for key, stats in perf.items():
        n = stats['n_fires']
        hits = stats['n_hits']
        stats['hit_rate'] = hits / n if n > 0 else 0.0
        stats['avg_gain'] = stats['total_gain'] / n if n > 0 else 0.0
        stats['disabled'] = (n >= 10 and stats['hit_rate'] < 0.35)
        stats['boosted'] = (n >= 5 and stats['hit_rate'] > 0.60)

    return perf


def _ensure_outcome_columns() -> None:
    """Add rule_id and pattern_id columns to pump_signal_outcomes if missing."""
    try:
        postgres_execute(
            "ALTER TABLE pump_signal_outcomes ADD COLUMN IF NOT EXISTS rule_id TEXT")
        postgres_execute(
            "ALTER TABLE pump_signal_outcomes ADD COLUMN IF NOT EXISTS pattern_id TEXT")
    except Exception as e:
        logger.debug(f"Outcome column migration: {e}")


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
# V4 FINGERPRINT RULE LOADING & MATCHING
# =============================================================================

# Features where LOWER values are bullish (must match pump_fingerprint.py)
_BEARISH_POLARITY_FEATURES = frozenset([
    'ob_ask_depth_velocity',
    'ob_spread_bps',
    'ob_spread_velocity',
])

# The ~30 features used by V4 signal detection
_V4_SIGNAL_FEATURES = [
    'ob_volume_imbalance', 'ob_depth_imbalance_ratio', 'ob_bid_depth_velocity',
    'ob_ask_depth_velocity', 'ob_spread_bps', 'ob_spread_velocity',
    'ob_microprice_deviation', 'ob_aggression_ratio', 'ob_imbalance_velocity_30s',
    'ob_cumulative_imbalance_5m', 'ob_imbalance_shift_1m', 'ob_net_flow_5m',
    'tx_buy_sell_pressure', 'tx_buy_volume_pct', 'tx_cumulative_delta',
    'tx_cumulative_delta_5m', 'tx_volume_surge_ratio', 'tx_aggressive_buy_ratio',
    'tx_trade_intensity', 'tx_large_trade_count', 'tx_whale_volume_pct',
    'wh_net_flow_ratio', 'wh_accumulation_ratio', 'wh_flow_velocity',
    'wh_flow_acceleration', 'wh_cumulative_flow_5m', 'wh_stealth_acc_score',
    'pm_price_velocity_30s', 'pm_price_change_1m', 'pm_trend_strength_ema',
    'pm_momentum_acceleration_1m',
]


def _load_fingerprint_rules() -> Optional[Dict[str, Any]]:
    """Load rules from pump_fingerprint_report.json.

    Called by refresh_pump_rules() instead of _train_model().
    Returns dict with combinations, patterns, thresholds, metadata.
    Also updates _current_volume_regime from the report.
    """
    global _current_volume_regime

    if not _FINGERPRINT_REPORT_PATH.exists():
        logger.warning(f"No fingerprint report at {_FINGERPRINT_REPORT_PATH}")
        return None

    try:
        with open(_FINGERPRINT_REPORT_PATH) as f:
            report = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to load fingerprint report: {e}")
        return None

    _current_volume_regime = report.get('volume_regime', 'unknown')

    return {
        'combinations': report.get('best_combinations', []),
        'patterns': report.get('approved_patterns', []),
        'thresholds': report.get('single_feature_thresholds', {}),
        'metadata': report.get('data_summary', {}),
        'generated_at': report.get('generated_at', ''),
    }


def extract_signal_features(trail_row: dict) -> Dict[str, float]:
    """Extract the ~30 V4 features from a trail row dict.

    Returns a dict of feature_name -> float value (NaN for missing).
    """
    features = {}
    for feat in _V4_SIGNAL_FEATURES:
        val = trail_row.get(feat)
        if val is not None:
            try:
                features[feat] = float(val)
            except (TypeError, ValueError):
                features[feat] = float('nan')
        else:
            features[feat] = float('nan')
    return features


def match_approved_pattern(features: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """Check if current features fall within any approved pattern's feature ranges.

    For each approved pattern, checks if ALL features in the pattern's
    feature_ranges have current values within [min, max].
    Skips patterns marked disabled in _rule_performance.
    Annotates the result with 'boosted' flag when applicable.

    Returns the best matching pattern (highest precision), or None.
    """
    if _rules is None:
        return None

    patterns = _rules.get('patterns', [])
    if not patterns:
        return None

    best_match = None
    best_precision = -1.0

    for pattern in patterns:
        pattern_id = pattern.get('pattern_id') or pattern.get('label')
        if pattern_id and _rule_performance.get(pattern_id, {}).get('disabled', False):
            continue

        feature_ranges = pattern.get('feature_ranges', {})
        if not feature_ranges:
            continue

        all_match = True
        n_checked = 0
        for feat, (low, high) in feature_ranges.items():
            val = features.get(feat)
            if val is None or np.isnan(val):
                continue
            n_checked += 1
            if not (low <= val <= high):
                all_match = False
                break

        if all_match and n_checked >= 2:
            prec = pattern.get('precision', 0.0)
            if prec > best_precision:
                best_precision = prec
                best_match = dict(pattern)  # copy so we can annotate safely
                best_match['boosted'] = bool(
                    pattern_id and _rule_performance.get(pattern_id, {}).get('boosted', False)
                )

    return best_match


def match_combination_rule(features: Dict[str, float]) -> Optional[Dict[str, Any]]:
    """Check if current features satisfy any top combination rule.

    For each combination rule, checks if ALL feature conditions are met
    (value > threshold for bullish features, < threshold for bearish).
    Skips rules marked disabled in _rule_performance.
    Annotates the result with 'boosted' flag when applicable.

    Returns the best matching rule (highest score = precision * n_signals), or None.
    """
    if _rules is None:
        return None

    combos = _rules.get('combinations', [])
    if not combos:
        return None

    best_match = None
    best_score = -1.0

    for combo in combos:
        rule_id = combo.get('rule_id')
        if rule_id and _rule_performance.get(rule_id, {}).get('disabled', False):
            continue

        feat_names = combo.get('features', [])
        thresholds = combo.get('thresholds', [])
        directions = combo.get('directions', [])

        if not feat_names or len(feat_names) != len(thresholds):
            continue

        all_met = True
        n_checked = 0
        for i, feat in enumerate(feat_names):
            val = features.get(feat)
            if val is None or np.isnan(val):
                all_met = False
                break

            thr = thresholds[i]
            direction = directions[i] if i < len(directions) else 'above'

            if direction == 'below':
                if val >= thr:
                    all_met = False
                    break
            else:
                if val <= thr:
                    all_met = False
                    break
            n_checked += 1

        if all_met and n_checked >= 2:
            score = combo.get('score', 0.0)
            if score > best_score:
                best_score = score
                best_match = dict(combo)  # copy so we can annotate safely
                best_match['boosted'] = bool(
                    rule_id and _rule_performance.get(rule_id, {}).get('boosted', False)
                )

    return best_match


# =============================================================================
# SIMULATOR RULES — direct check against raw_data_cache feature names
# =============================================================================

def _load_sim_rules(
    win_rate_min: float = 0.65,
    oos_gap_max: float = 0.004,
    daily_ev_min: float = 0.0,
    n_signals_min: int = 10,
) -> List[Dict[str, Any]]:
    """Load top simulation rules from simulation_results (PostgreSQL).

    Per-play thresholds allow each play to apply different quality bars
    when selecting which simulator rules to activate.
    Feature names match raw_data_cache.get_live_features() exactly —
    no remapping needed.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT conditions_json, win_rate, ev_per_trade, daily_ev,
                           exit_config_json, sharpe, oos_gap,
                           COALESCE(oos_consistency, 0.5) AS oos_consistency
                    FROM simulation_results
                    WHERE win_rate   >= %s
                      AND n_signals  >= %s
                      AND oos_gap    <= %s
                      AND daily_ev   >= %s
                      AND COALESCE(oos_consistency, 0) >= 0.33
                    ORDER BY daily_ev * COALESCE(oos_consistency, 0.5) DESC
                    LIMIT 8
                """, [win_rate_min, n_signals_min, oos_gap_max, daily_ev_min])
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.warning(f"[sim_rules] Failed to load: {e}")
        return []


def check_sim_rules(
    live_features: Dict[str, Any],
    play_id: int = 3,
    sim_filter: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Check live raw-cache features against best simulation_results rules.

    Feature names in conditions_json use the same naming as
    raw_data_cache.get_live_features() — no translation required.

    Each play passes its own sim_filter thresholds so rules are filtered
    differently per play (e.g. aggressive play uses lower win_rate_min).

    Returns the first matching rule dict (highest daily_ev), or None.
    """
    global _sim_rules, _sim_rules_loaded_at

    now = time.time()
    play_last_loaded = _sim_rules_loaded_at.get(play_id, 0.0)
    if now - play_last_loaded > SIM_RULES_TTL or play_id not in _sim_rules:
        sf = sim_filter or {}
        rules = _load_sim_rules(
            win_rate_min=sf.get('win_rate_min', 0.65),
            oos_gap_max=sf.get('oos_gap_max', 0.004),
            daily_ev_min=sf.get('daily_ev_min', 0.0),
            n_signals_min=sf.get('n_signals_min', 10),
        )
        _sim_rules[play_id] = rules
        _sim_rules_loaded_at[play_id] = now
        logger.debug(f"[sim_rules] play={play_id} loaded {len(rules)} rules")

    rules = _sim_rules.get(play_id, [])
    if not rules:
        return None

    for rule in rules:
        conditions = rule.get('conditions_json') or []
        if not conditions:
            continue

        all_met = True
        n_checked = 0
        for cond in conditions:
            feat      = cond.get('feature')
            direction = cond.get('direction')   # '<' or '>'
            threshold = cond.get('threshold')

            if feat is None or direction is None or threshold is None:
                continue

            val = live_features.get(feat)
            if val is None:
                all_met = False
                break
            try:
                val = float(val)
            except (TypeError, ValueError):
                all_met = False
                break

            if direction == '<' and val >= threshold:
                all_met = False
                break
            elif direction == '>' and val <= threshold:
                all_met = False
                break
            n_checked += 1

        if all_met and n_checked >= 2:
            return rule  # first match (ordered by daily_ev DESC)

    return None


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


_TRAIL_CACHE_MIN_RETENTION_HOURS = 24  # matches PostgreSQL 24h retention window


def _sync_trail_cache(hours: int) -> duckdb.DuckDBPyConnection:
    """Incrementally sync trail data from PostgreSQL into DuckDB.

    The DuckDB cache acts as the long-term data store since PostgreSQL
    archives data after 24 hours. Retention is at least
    _TRAIL_CACHE_MIN_RETENTION_HOURS (14 days) regardless of the caller's
    lookback, so short-lookback callers (48h) don't destroy data that the
    fingerprint analysis (336h) needs.

    Returns an open DuckDB connection ready for queries.
    """
    con = _get_trail_cache_conn()
    _init_trail_cache(con)

    t0 = time.time()

    # Watermark: latest followed_at already in cache
    row = con.execute("SELECT MAX(followed_at) FROM cached_trail").fetchone()
    max_ts = row[0] if row and row[0] else None

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # ── Cleanup old data (respect minimum retention for fingerprint) ────
    retention_hours = max(hours, _TRAIL_CACHE_MIN_RETENTION_HOURS)
    retention_cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    con.execute("DELETE FROM cached_trail WHERE followed_at < ?", [retention_cutoff])

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

        # ── Measurement: IMMEDIATE_DIP_MAX widening (-0.08 → -0.12) ──────────
        if 'min_fwd_imm' in df.columns:
            try:
                dip_old_mask = df['min_fwd_imm'] > -0.08
                dip_new_mask = df['min_fwd_imm'] > -0.12
                dip_added_pumps = int(
                    (dip_new_mask & ~dip_old_mask & df['label'].eq('clean_pump')).sum()
                )
                dip_added_total = int((dip_new_mask & ~dip_old_mask).sum())
                logger.info(
                    f"  IMMEDIATE_DIP_MAX widening (-0.08 → -0.12): "
                    f"+{dip_added_pumps} clean_pump labels, +{dip_added_total} total rows "
                    f"now passing dip filter "
                    f"({int(dip_old_mask.sum())} → {int(dip_new_mask.sum())})"
                )
            except Exception:
                pass

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
# V3 GBM FALLBACK — commented out 2026-02-17
# The following functions (_get_base_feature_columns, _engineer_features,
# _train_model, _compute_feat, _check_feature_drift, _init_drift_buffers,
# _get_drift_window, _check_microstructure_confirmation) are kept as fallback.
# They were part of the V3 GBM pipeline. V4 uses fingerprint-based rules instead.
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
    """Regenerate fingerprint analysis and load rules.

    Called every 5 minutes by the scheduler (refresh_pump_model component).
    Runs the full fingerprint analysis on 24h of data, writes the
    JSON report, then loads the resulting rules.

    Also persists training labels for the dashboard and computes
    per-rule performance from recent outcomes.
    """
    global _rules, _rules_metadata

    logger.info("=== V4: Refreshing fingerprint rules ===")
    t0 = time.time()

    # Persist training labels for the pumps chart (uses existing data loading)
    try:
        df = _load_and_label_data(lookback_hours=24)
        if df is not None and len(df) > 0:
            _persist_training_labels(df)
    except Exception as e:
        logger.warning(f"Training label persistence failed: {e}")

    # Run fingerprint analysis (writes to cache/pump_fingerprint_report.json)
    try:
        from pump_fingerprint import run_fingerprint_analysis
        report = run_fingerprint_analysis(lookback_hours=24)
    except Exception as e:
        logger.error(f"Fingerprint analysis failed: {e}", exc_info=True)
        report = None

    if report is None:
        logger.warning("Fingerprint analysis failed — keeping current rules")
        return

    # Load rules from the report
    rules = _load_fingerprint_rules()
    if rules is None:
        logger.warning("Failed to load fingerprint rules after analysis")
        return

    n_patterns = len(rules.get('patterns', []))
    n_combos = len(rules.get('combinations', []))

    _rules = rules
    _rules_metadata = rules.get('metadata', {})

    # Reset circuit breaker cache on rule refresh
    global _circuit_breaker_paused, _cb_cache, _cb_cache_time
    _cb_cache = None
    _cb_cache_time = 0.0
    _circuit_breaker_paused = False

    # Compute per-rule performance from recent outcomes and cache globally
    global _rule_performance
    try:
        _rule_performance = _compute_rule_performance()
        if _rule_performance:
            n_disabled = sum(1 for rp in _rule_performance.values()
                            if rp.get('disabled', False))
            n_boosted = sum(1 for rp in _rule_performance.values()
                           if rp.get('boosted', False))
            logger.info(f"  Rule performance: {len(_rule_performance)} tracked, "
                        f"{n_disabled} disabled, {n_boosted} boosted")
    except Exception as e:
        logger.debug(f"Rule performance computation failed: {e}")

    logger.info(f"  V4 rules refreshed: {n_patterns} patterns, {n_combos} combos "
                f"({time.time()-t0:.1f}s)")


def _load_cached_model() -> bool:
    """V4: Load fingerprint rules from JSON cache (replaces GBM pickle load)."""
    global _rules, _rules_metadata
    rules = _load_fingerprint_rules()
    if rules is None:
        return False
    _rules = rules
    _rules_metadata = rules.get('metadata', {})
    n_pat = len(rules.get('patterns', []))
    n_combo = len(rules.get('combinations', []))
    logger.info(f"Loaded V4 fingerprint rules: {n_pat} patterns, {n_combo} combos")
    return True


def maybe_refresh_rules():
    """Reload fingerprint rules from disk cache if stale.

    Called by train_validator every cycle. This is lightweight (just a JSON
    file read) — the heavy fingerprint analysis runs in the separate
    refresh_pump_model component which writes the JSON report.
    """
    global _last_rules_refresh, _rule_performance
    now = time.time()
    if _rules is None or (now - _last_rules_refresh >= RULES_REFRESH_INTERVAL):
        _load_cached_model()
        _last_rules_refresh = now
        try:
            _rule_performance = _compute_rule_performance()
        except Exception as e:
            logger.debug(f"Rule performance refresh failed: {e}")


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
        f"V4 gates (60s): {total} chk | no_rules={_gate_stats['no_rules']} "
        f"cb={cb} crash={_gate_stats['crash_gate_fail']} 5m={_gate_stats['crash_5m_fail']} "
        f"chase={_gate_stats['chase_gate_fail']} "
        f"ok={_gate_stats['gates_passed']} no_pat={_gate_stats['no_pattern']} "
        f"no_combo={_gate_stats['no_combo']} "
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

    # Block if majority of microstructure signals are bearish.
    # Previously required ALL to be bearish, but that almost never triggered
    # because order book imbalance is usually slightly positive from market-making.
    # Now blocks if 2+ of 3 are bearish — catches the common case where whales
    # are dumping and transactions are selling but order book hasn't caught up.
    if checks >= 2 and bearish_count >= 2:
        return False, desc

    # Also block if all signals present are bearish (handles 1-signal case)
    if bearish_count > 0 and bearish_count == checks:
        return False, desc

    return True, desc


def check_pump_signal(trail_row: dict, market_price: float) -> bool:
    """V4 rule-based signal detection using fingerprint analysis.

    Simplified flow:
    1. Safety gates (crash, chase, circuit breaker — 3 hard gates)
    2. Check if current features match any approved pattern (cluster match)
    3. Check if current features satisfy any top combination rule
    4. Require: strong pattern OR (pattern + combo) OR strong combo
    5. Log exactly which pattern/rule triggered and why
    """
    _gate_stats['total_checks'] += 1

    if _rules is None:
        _gate_stats['no_rules'] += 1
        return False

    # Gate 1: Circuit breaker — auto-pause if live precision is too low
    if _check_circuit_breaker():
        _gate_stats['circuit_breaker'] += 1
        return False

    # Gate 2: Crash protection (micro 30s + 5m)
    crash_ok, crash_desc = _is_not_crashing()
    if not crash_ok:
        _gate_stats['crash_gate_fail'] += 1
        return False

    pm_5m = trail_row.get('pm_price_change_5m')
    if pm_5m is not None and float(pm_5m) < CRASH_GATE_5M:
        _gate_stats['crash_5m_fail'] += 1
        return False

    # Gate 3: Chase prevention — block entries at local highs
    pm_1m = trail_row.get('pm_price_change_1m')
    pm_1m_val = float(pm_1m) if pm_1m is not None else 0.0
    pm_vel_30s = trail_row.get('pm_price_velocity_30s')
    pm_vel_30s_val = float(pm_vel_30s) if pm_vel_30s is not None else 0.0

    if pm_1m is not None and pm_1m_val > CHASE_GATE_1M_MAX:
        _gate_stats['chase_gate_fail'] += 1
        return False
    if pm_vel_30s is not None and pm_vel_30s_val > CHASE_GATE_30S_MAX:
        _gate_stats['chase_gate_fail'] += 1
        return False

    _gate_stats['gates_passed'] += 1

    # Track volatility for monitoring
    _update_vol_buffer(trail_row.get('pm_realized_vol_1m'))

    # --- VOLUME REGIME CHECK (warning only — no hard block) ---
    tx_intensity = trail_row.get('tx_trade_intensity')
    if tx_intensity is not None:
        try:
            tx_val = float(tx_intensity)
            if tx_val < 0.8:
                live_regime = "low"
            elif tx_val < 1.5:
                live_regime = "medium"
            else:
                live_regime = "high"
            if (_current_volume_regime != "unknown"
                    and live_regime != _current_volume_regime):
                logger.warning(
                    f"Volume regime drift: rules discovered in '{_current_volume_regime}' "
                    f"regime, current market is '{live_regime}' "
                    f"(tx_trade_intensity={tx_val:.3f})"
                )
        except (TypeError, ValueError):
            pass

    # --- PATTERN MATCHING ---
    features = extract_signal_features(trail_row)

    pattern_match = match_approved_pattern(features)
    combo_match = match_combination_rule(features)

    # Fire logic:
    # 1. Strong pattern (precision >= 55%, or >= 45% if boosted) — fire even without combo
    # 2. Pattern + combo confirmation — fire
    # 3. Very strong combo (precision >= 60%, or >= 50% if boosted) — fire without pattern
    fired = False
    fire_reason = ""

    pattern_boosted = pattern_match and pattern_match.get('boosted', False)
    combo_boosted = combo_match and combo_match.get('boosted', False)
    pattern_prec_threshold = 0.45 if pattern_boosted else 0.55
    combo_prec_threshold = 0.50 if combo_boosted else 0.60

    if pattern_match and pattern_match.get('precision', 0) >= pattern_prec_threshold:
        boost_tag = " [BOOSTED]" if pattern_boosted else ""
        fired = True
        fire_reason = (f"Pattern: {pattern_match.get('label', '?')} "
                       f"(prec={pattern_match['precision']:.0%}, "
                       f"n={pattern_match.get('n_occurrences', '?')}){boost_tag}")

    elif pattern_match and combo_match:
        boost_tag = " [BOOSTED]" if (pattern_boosted or combo_boosted) else ""
        fired = True
        combo_feats = ' + '.join(combo_match.get('features', []))
        fire_reason = (f"Pattern: {pattern_match.get('label', '?')} "
                       f"(prec={pattern_match['precision']:.0%}) + "
                       f"Combo: [{combo_feats}] "
                       f"(prec={combo_match.get('precision', 0):.0%}){boost_tag}")

    elif combo_match and combo_match.get('precision', 0) >= combo_prec_threshold:
        boost_tag = " [BOOSTED]" if combo_boosted else ""
        fired = True
        combo_feats = ' + '.join(combo_match.get('features', []))
        fire_reason = (f"Combo: [{combo_feats}] "
                       f"(prec={combo_match['precision']:.0%}, "
                       f"n={combo_match.get('n_signals', '?')}){boost_tag}")

    if not fired:
        if not pattern_match:
            _gate_stats['no_pattern'] += 1
        if not combo_match:
            _gate_stats['no_combo'] += 1
        return False

    _gate_stats['signal_fired'] += 1
    logger.info(f"V4: SIGNAL! {fire_reason}")

    # Outcome attribution: capture context at fire time
    buyin_id = trail_row.get('buyin_id')
    if buyin_id is not None:
        rule_id = None
        pattern_id = None
        if combo_match:
            rule_id = f"combo_{'_'.join(combo_match.get('features', []))}"
        if pattern_match:
            pattern_id = f"pat_{pattern_match.get('cluster_id', '?')}"

        top_feat_vals = {k: round(v, 6) for k, v in sorted(
            features.items(), key=lambda x: abs(x[1]) if not np.isnan(x[1]) else 0,
            reverse=True,
        )[:5]}

        _signal_context[int(buyin_id)] = {
            'top_features': top_feat_vals,
            'gates_passed': {
                'crash_30s': crash_desc,
                'chase': f"1m={pm_1m_val:+.4f}%,30s={pm_vel_30s_val:+.4f}%",
            },
            'fire_reason': fire_reason,
            'rule_id': rule_id,
            'pattern_id': pattern_id,
            'pattern_precision': pattern_match.get('precision') if pattern_match else None,
            'combo_precision': combo_match.get('precision') if combo_match else None,
        }
        if len(_signal_context) > 200:
            oldest = sorted(_signal_context.keys())[:100]
            for k in oldest:
                _signal_context.pop(k, None)

    return True


# =============================================================================
# OBSERVATION MODE HELPERS
# =============================================================================

_obs_table_created = False

def _ensure_observation_table() -> None:
    """Create pump_signal_observations table if it doesn't exist (once per process)."""
    global _obs_table_created
    if _obs_table_created:
        return
    try:
        postgres_execute("""
            CREATE TABLE IF NOT EXISTS pump_signal_observations (
                id BIGSERIAL PRIMARY KEY,
                buyin_id BIGINT,
                market_price DOUBLE PRECISION,
                rule_id TEXT,
                pattern_id TEXT,
                confidence DOUBLE PRECISION,
                fire_reason TEXT,
                features_json TEXT,
                signal_fired BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Add signal_fired column to existing tables that pre-date this change
        postgres_execute(
            "ALTER TABLE pump_signal_observations "
            "ADD COLUMN IF NOT EXISTS signal_fired BOOLEAN DEFAULT FALSE"
        )
        _obs_table_created = True
    except Exception:
        pass


def _record_observation(buyin_id: int, market_price: float, trail_row: dict,
                        confidence: float, matched_rule: dict,
                        signal_fired: bool = True) -> None:
    """Record one observation-mode cycle snapshot for later analysis.

    Called every 5s cycle in PUMP_OBSERVATION_MODE — for both fired signals
    (signal_fired=True) and non-firing cycles (signal_fired=False).  This
    gives a continuous audit trail so you can inspect what the system saw
    each cycle and why it did or didn't fire.

    Writes to pump_signal_observations table.
    """
    _ensure_observation_table()
    try:
        features_snapshot = {}
        for feat in ['ob_volume_imbalance', 'ob_spread_velocity', 'ob_bid_depth_velocity',
                     'tx_buy_sell_pressure', 'tx_cumulative_delta', 'wh_net_flow_ratio',
                     'wh_accumulation_ratio', 'pm_price_change_1m', 'pm_price_change_5m',
                     'pm_price_velocity_30s', 'pm_momentum_acceleration_1m',
                     'tx_large_trade_count', 'tx_whale_volume_pct']:
            v = trail_row.get(feat)
            if v is not None:
                features_snapshot[feat] = round(float(v), 6)

        postgres_execute(
            """INSERT INTO pump_signal_observations
               (buyin_id, market_price, rule_id, pattern_id, confidence,
                fire_reason, features_json, signal_fired)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            [buyin_id, market_price,
             matched_rule.get('rule_id', ''),
             matched_rule.get('pattern_id', ''),
             confidence,
             matched_rule.get('fire_reason', ''),
             json.dumps(features_snapshot),
             signal_fired]
        )
    except Exception as e:
        logger.warning(f"Failed to record observation: {e}")


# =============================================================================
# BUYIN INSERTION (same API as V1)
# =============================================================================

def check_and_fire_pump_signal(
    play_config: Optional[Dict[str, Any]] = None,
    buyin_id: Optional[int] = None,
    market_price: float = 0.0,
    price_cycle: Optional[int] = None,
) -> bool:
    global _last_entry_time, _last_entry_time_per_play

    # Resolve play config — support both new dict API and legacy env-var fallback
    if play_config is not None:
        pump_play_id = int(play_config['play_id'])
        sim_filter: Dict[str, Any] = play_config.get('sim_filter') or {}
        play_cooldown = float(sim_filter.get('cooldown_seconds', COOLDOWN_SECONDS))
    else:
        pump_play_id = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "3"))
        sim_filter = {}
        play_cooldown = COOLDOWN_SECONDS

    if not pump_play_id:
        return False

    _update_price_buffer(market_price)

    if _rules is None:
        return False

    # Get live features from raw Parquet cache (fresh, no pre-computation lag).
    # Raw cache is populated by binance_stream (OB) and webhook_server (trades/whales).
    # If cache is not yet warm enough, skip this cycle.
    trail_row = None
    try:
        from core.raw_data_cache import get_live_features
        live = get_live_features(window_min=5)
        if live and live.get('ob_n', 0) >= 3:
            # Use a timestamp-based signal key so context is retrievable without buyin_id
            signal_key = int(time.time())
            trail_row = {
                'buyin_id':                  signal_key,
                'minute':                    0,
                'ob_volume_imbalance':        live.get('ob_avg_vol_imb'),
                'ob_depth_imbalance_ratio':   live.get('ob_avg_depth_ratio'),
                'ob_spread_bps':              live.get('ob_avg_spread_bps'),
                'ob_net_flow_5m':             live.get('ob_net_liq_change'),
                'ob_aggression_ratio':        live.get('ob_bid_ask_ratio'),
                'ob_imbalance_shift_1m':      live.get('ob_imb_1m'),
                'ob_imbalance_velocity_30s':  live.get('ob_imb_trend'),
                'ob_depth_ratio_velocity':    live.get('ob_depth_trend'),
                'ob_liquidity_score':         live.get('ob_liq_accel'),
                'tx_buy_volume_pct':          live.get('tr_buy_ratio'),
                'tx_whale_volume_pct':        live.get('tr_large_ratio'),
                'tx_avg_trade_size':          live.get('tr_avg_size'),
                'tx_aggressive_buy_ratio':    live.get('tr_buy_accel'),
                'tx_trade_count':             live.get('tr_n'),
                'tx_trade_intensity':         live.get('tr_n'),  # proxy
                'wh_net_flow_sol':            live.get('wh_net_flow'),
                'wh_accumulation_ratio':      live.get('wh_inflow_ratio'),
                'wh_movement_count':          live.get('wh_n'),
            }
    except Exception as e:
        logger.debug(f"Raw cache features unavailable: {e}")

    if trail_row is None:
        logger.debug("V4: skipping cycle — raw cache not yet warm (ob_n < 3)")
        return False

    # ── Path A: Simulator rules (primary — uses raw feature names directly) ───
    # `live` is guaranteed non-None and ob_n >= 3 here (trail_row guard passed)
    sim_match = check_sim_rules(live, play_id=pump_play_id, sim_filter=sim_filter)
    signal_fires_sim = sim_match is not None

    # ── Path B: Fingerprint rules ─────────────────────────────────────────────
    # Only run if no sim rules are loaded (i.e. simulation_results is empty for
    # this play). When the simulator has qualifying rules, fingerprint fallback
    # is DISABLED — live performance shows fingerprint combos fire on noise and
    # generate losing trades at ~21% win rate vs simulator rules at 78-81%.
    play_has_sim_rules = bool(_sim_rules.get(pump_play_id))
    if play_has_sim_rules:
        signal_fires_fp = False
        _log_gate_summary()
    else:
        signal_fires_fp = check_pump_signal(trail_row, market_price)
        _log_gate_summary()

    # Signal fires if EITHER path matches (Path B only fires when no sim rules)
    signal_fires = signal_fires_sim or signal_fires_fp

    if signal_fires_sim:
        logger.info(
            f"[sim_rules] play={pump_play_id} Signal via simulator rule | "
            f"win_rate={sim_match.get('win_rate', 0):.0%} "
            f"ev/trade={sim_match.get('ev_per_trade', 0):.4f} "
            f"daily_ev={sim_match.get('daily_ev', 0):.3f}"
        )
    elif signal_fires_fp and not play_has_sim_rules:
        logger.info(f"[fingerprint] play={pump_play_id} Signal via fingerprint (no sim rules loaded)")

    # Observation mode: record EVERY cycle (fired or not) so there is a
    # continuous 5-second reference trail in pump_signal_observations.
    if PUMP_OBSERVATION_MODE:
        sig_key = int(trail_row.get('buyin_id', 0))
        ctx = _signal_context.get(sig_key, {}) if signal_fires_fp else {}
        if signal_fires_sim and sim_match:
            ctx['sim_rule'] = {
                'win_rate':     sim_match.get('win_rate'),
                'ev_per_trade': sim_match.get('ev_per_trade'),
                'daily_ev':     sim_match.get('daily_ev'),
                'conditions':   sim_match.get('conditions_json'),
            }
        confidence = ctx.get('pattern_precision') or ctx.get('combo_precision') or 0.0
        if signal_fires:
            logger.info(f"OBSERVATION: Signal would fire @ {market_price:.4f}")
        _record_observation(
            sig_key, market_price, trail_row,
            confidence=confidence,
            matched_rule=ctx,
            signal_fired=signal_fires,
        )
        return False

    if not signal_fires:
        return False

    # Cooldown — checked per play so plays don't block each other
    now = time.time()
    play_last = _last_entry_time_per_play.get(pump_play_id, 0.0)
    if now - play_last < play_cooldown:
        remaining = int(play_cooldown - (now - play_last))
        logger.info(f"V4 play={pump_play_id}: signal but cooldown ({remaining}s)")
        return False

    # DB-level cooldown: guard against multiple processes / restarts
    try:
        last = postgres_query_one(
            """SELECT followed_at FROM follow_the_goat_buyins
               WHERE play_id=%s AND wallet_address NOT LIKE 'TRAINING_TEST_%%'
               ORDER BY followed_at DESC LIMIT 1""",
            [pump_play_id])
        if last and last.get('followed_at'):
            ft = last['followed_at']
            ts = ft.timestamp() if hasattr(ft, 'timestamp') else (ft if isinstance(ft, (int, float)) else None)
            if ts is not None and now - ts < play_cooldown:
                return False
    except Exception:
        pass

    try:
        op = postgres_query_one(
            "SELECT id FROM follow_the_goat_buyins WHERE play_id=%s AND our_status IN ('pending','validating') AND wallet_address NOT LIKE 'TRAINING_TEST_%%' LIMIT 1",
            [pump_play_id])
        if op:
            logger.info(f"V4: signal but open position {op['id']}")
            return False
    except Exception:
        return False

    # Insert
    ts = str(int(now))
    bt = datetime.now(timezone.utc).replace(tzinfo=None)

    # Get signal context for the entry log
    sig_key = int(trail_row.get('buyin_id', 0))
    ctx = _signal_context.get(sig_key, {})
    fire_reason = ctx.get('fire_reason', 'unknown')
    pat_prec = ctx.get('pattern_precision')
    combo_prec = ctx.get('combo_precision')

    entry_log = json.dumps({
        'signal_type': 'pump_detection_v4',
        'play_id': pump_play_id,
        'signal_key': sig_key,
        'fire_reason': fire_reason,
        'pattern_precision': pat_prec,
        'combo_precision': combo_prec,
        'rule_id': ctx.get('rule_id'),
        'pattern_id': ctx.get('pattern_id'),
        'sim_rule': {
            'win_rate': sim_match.get('win_rate') if sim_match else None,
            'ev_per_trade': sim_match.get('ev_per_trade') if sim_match else None,
            'daily_ev': sim_match.get('daily_ev') if sim_match else None,
            'conditions': sim_match.get('conditions_json') if sim_match else None,
        } if signal_fires_sim else None,
        'sim_filter': sim_filter or None,
        'rules_metadata': _rules_metadata,
        'sol_price': market_price,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    })

    wallet_tag = f'PUMP_V4_P{pump_play_id}_{ts}'
    sig_tag    = f'pump_v4_p{pump_play_id}_{ts}'

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
        """, [nid, pump_play_id, wallet_tag, 0, sig_tag, bt,
              100.0, market_price, market_price, 'buy', market_price, 0, price_cycle,
              entry_log, None, 'pending', bt, market_price])

        _last_entry_time = now
        _last_entry_time_per_play[pump_play_id] = now
        logger.info(f"  V4 play={pump_play_id} buyin #{nid} @ {market_price:.4f} [{fire_reason}]")
        return True
    except Exception as e:
        logger.error(f"V4 insert error (play={pump_play_id}): {e}", exc_info=True)
        return False


def get_pump_status() -> Dict[str, Any]:
    info = _get_live_outcomes()
    n_patterns = len(_rules.get('patterns', [])) if _rules else 0
    n_combos = len(_rules.get('combinations', [])) if _rules else 0

    # Observation mode stats
    try:
        obs_stats = postgres_query_one(
            """SELECT COUNT(*) as n_observations,
                      MIN(created_at) as first_obs,
                      MAX(created_at) as last_obs
               FROM pump_signal_observations"""
        )
    except Exception:
        obs_stats = None

    return {
        'version': 'v4',
        'has_rules': _rules is not None,
        'n_patterns': n_patterns,
        'n_combinations': n_combos,
        'rules_metadata': _rules_metadata,
        'last_refresh': _last_rules_refresh,
        'last_entry': _last_entry_time,
        'last_entry_per_play': dict(_last_entry_time_per_play),
        'sim_rules_loaded': {pid: len(rules) for pid, rules in _sim_rules.items()},
        'circuit_breaker_paused': _circuit_breaker_paused,
        'live_win_rate': round(info['win_rate'], 4) if info['win_rate'] is not None else None,
        'live_outcomes_count': info['n'],
        'vol_buffer_size': len(_vol_buffer),
        'vol_percentile': round(_get_vol_percentile(), 1) if _get_vol_percentile() is not None else None,
        'readiness_score': round(_last_readiness_score, 4),
        'readiness_threshold': READINESS_THRESHOLD,
        'observation_mode': PUMP_OBSERVATION_MODE,
        'observation_count': obs_stats['n_observations'] if obs_stats else 0,
    }


# =============================================================================
# OBSERVATION ANALYSIS (manual review after data accumulation)
# =============================================================================

def analyze_observations() -> Dict[str, Any]:
    """Analyze observation-mode signals against actual price outcomes.

    For each recorded observation:
    1. Find the price at observation time (market_price in the record)
    2. Find prices at +1m, +2m, +4m, +6m, +10m after observation
    3. Compute forward returns
    4. Report: hit rate, avg gain on hits, avg loss on misses, EV

    Call this manually after accumulating 14 days of data:
        from pump_signal_logic import analyze_observations
        results = analyze_observations()
    """
    try:
        observations = postgres_query(
            """SELECT id, market_price, rule_id, pattern_id, created_at
               FROM pump_signal_observations
               ORDER BY created_at"""
        )
    except Exception as e:
        return {'error': str(e)}

    if not observations:
        return {'n_observations': 0, 'message': 'No observations recorded yet'}

    results = []
    for obs in observations:
        obs_time = obs['created_at']
        entry_price = obs['market_price']

        forward_prices = {}
        for minutes in [1, 2, 4, 6, 10]:
            try:
                future = postgres_query_one(
                    """SELECT price FROM prices
                       WHERE token = 'SOL'
                         AND timestamp >= %s + make_interval(mins => %s)
                       ORDER BY timestamp ASC LIMIT 1""",
                    [obs_time, minutes]
                )
                if future:
                    fwd_return = (float(future['price']) - entry_price) / entry_price * 100
                    forward_prices[f'fwd_{minutes}m'] = round(fwd_return, 4)
            except Exception:
                pass

        max_fwd = max(forward_prices.values()) if forward_prices else None
        is_hit = max_fwd is not None and max_fwd >= 0.2  # same target as labeling

        results.append({
            'obs_id': obs['id'],
            'entry_price': entry_price,
            'rule_id': obs['rule_id'],
            'pattern_id': obs['pattern_id'],
            'forward_returns': forward_prices,
            'max_fwd': max_fwd,
            'is_hit': is_hit,
        })

    n = len(results)
    hits = sum(1 for r in results if r['is_hit'])
    hit_rate = hits / n if n > 0 else 0

    gains = [r['max_fwd'] for r in results if r['is_hit'] and r['max_fwd'] is not None]
    losses = [r['max_fwd'] for r in results if not r['is_hit'] and r['max_fwd'] is not None]

    avg_gain = sum(gains) / len(gains) if gains else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    ev_raw = hit_rate * avg_gain + (1 - hit_rate) * avg_loss
    ev_after_costs = ev_raw - 0.1  # 0.1% round-trip cost

    # Per-rule breakdown
    rule_ids = set(r['rule_id'] for r in results if r['rule_id'])
    per_rule_stats = {}
    for rid in rule_ids:
        rule_results = [r for r in results if r['rule_id'] == rid]
        rn = len(rule_results)
        rhits = sum(1 for r in rule_results if r['is_hit'])
        rgains = [r['max_fwd'] for r in rule_results if r['is_hit'] and r['max_fwd'] is not None]
        rlosses = [r['max_fwd'] for r in rule_results if not r['is_hit'] and r['max_fwd'] is not None]
        r_avg_gain = sum(rgains) / len(rgains) if rgains else 0
        r_avg_loss = sum(rlosses) / len(rlosses) if rlosses else 0
        r_hit_rate = rhits / rn if rn > 0 else 0
        r_ev = r_hit_rate * r_avg_gain + (1 - r_hit_rate) * r_avg_loss - 0.1
        per_rule_stats[rid] = {
            'n': rn, 'hits': rhits,
            'hit_rate': round(r_hit_rate, 4),
            'avg_gain': round(r_avg_gain, 4),
            'avg_loss': round(r_avg_loss, 4),
            'ev_after_costs': round(r_ev, 4),
        }

    return {
        'n_observations': n,
        'n_hits': hits,
        'hit_rate': round(hit_rate, 4),
        'avg_gain_on_hits': round(avg_gain, 4),
        'avg_loss_on_misses': round(avg_loss, 4),
        'ev_raw': round(ev_raw, 4),
        'ev_after_costs': round(ev_after_costs, 4),
        'per_rule_stats': per_rule_stats,
        'details': results,
    }


# V4: Ensure DB schema has rule_id/pattern_id columns
try:
    _ensure_outcome_columns()
except Exception:
    pass

# Load cached fingerprint rules on import
try:
    _load_cached_model()
except Exception:
    pass

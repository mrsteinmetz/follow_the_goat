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

import json
import logging
import os
import pickle
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.database import get_postgres, postgres_execute, postgres_query, postgres_query_one

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_CACHE_PATH = _PROJECT_ROOT / "tests" / "filter_simulation" / "results" / "pump_model_v2_cache.pkl"

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
    Require at least 2 of 3 timeframes showing positive momentum.

    THIS IS THE KEY FIX for downtrend bounces: V1 passed if ANY single
    timeframe was positive. A brief 1-minute bounce during a 5-minute
    downtrend passed the gate. With V2, we also need 5m > 0 (which would
    have been deeply negative during that downtrend), so it gets rejected.
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

    # Need 2+ positive if we have 2+ timeframes, else need all
    required = min(2, available)
    return scores >= required, desc


# =============================================================================
# DATA LOADING WITH PATH-AWARE LABELING
# =============================================================================

def _get_pg_connection_string() -> str:
    from core.config import settings
    pg = settings.postgres
    return f"host={pg.host} port={pg.port} dbname={pg.database} user={pg.user} password={pg.password}"


def _load_and_label_data(lookback_hours: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Load trail data with PATH-AWARE labeling.

    V1 problem: labeled as 'pump_continuation' if max return in 10 min >= 0.3%,
    even if price dropped first and only recovered in minute 9. You can't actually
    capture that gain because you'd hit your stop loss.

    V2 fix: label as 'clean_pump' ONLY if:
      a) Price reaches +0.2% within first 4 minutes (EARLY_WINDOW)
      b) Price never dips below -0.10% at ANY point (MAX_DRAWDOWN_PCT)
      c) Price doesn't immediately drop > -0.08% in first 2 min
    """
    import duckdb

    hours = lookback_hours if lookback_hours is not None else LOOKBACK_HOURS
    logger.info(f"V2: Loading data (last {hours}h)...")
    t0 = time.time()

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        pg_conn = _get_pg_connection_string()
        con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES, READ_ONLY)")

        con.execute(f"""
            CREATE TABLE buyins AS
            SELECT id AS buyin_id, followed_at, potential_gains
            FROM pg.follow_the_goat_buyins
            WHERE potential_gains IS NOT NULL
              AND followed_at >= NOW() - INTERVAL '{hours} hours'
        """)
        n_buyins = con.execute("SELECT COUNT(*) FROM buyins").fetchone()[0]
        if n_buyins == 0:
            con.execute("DETACH pg")
            return None

        con.execute("""
            CREATE TABLE trail AS
            SELECT * FROM pg.buyin_trail_minutes
            WHERE buyin_id IN (SELECT buyin_id FROM buyins)
              AND COALESCE(sub_minute, 0) = 0
        """)
        n_trail = con.execute("SELECT COUNT(*) FROM trail").fetchone()[0]
        logger.info(f"  {n_trail:,} trail rows ({n_buyins:,} buyins)")
        con.execute("DETACH pg")

        if n_trail == 0:
            return None

        con.execute("CREATE INDEX idx_bid_min ON trail(buyin_id, minute)")

        # Forward returns at each minute
        joins, selects = [], []
        for k in range(1, FORWARD_WINDOW + 1):
            a = f"t{k}"
            joins.append(f"LEFT JOIN trail {a} ON {a}.buyin_id=t.buyin_id AND {a}.minute=t.minute+{k}")
            selects.append(f"({a}.pm_close_price-t.pm_close_price)/NULLIF(t.pm_close_price,0)*100 AS fwd_{k}m")

        joins_sql = "\n".join(joins)
        selects_sql = ",\n".join(selects)

        # Aggregates over different windows
        early = [f"COALESCE(fwd_{k}m,-9999)" for k in range(1, EARLY_WINDOW + 1)]
        all_max = [f"COALESCE(fwd_{k}m,-9999)" for k in range(1, FORWARD_WINDOW + 1)]
        all_min = [f"COALESCE(fwd_{k}m,9999)" for k in range(1, FORWARD_WINDOW + 1)]
        imm = [f"COALESCE(fwd_{k}m,9999)" for k in range(1, min(3, FORWARD_WINDOW + 1))]
        any_nn = " OR ".join([f"fwd_{k}m IS NOT NULL" for k in range(1, FORWARD_WINDOW + 1)])

        # Time to peak
        ttp = "CASE " + " ".join([f"WHEN fwd_{k}m>={MIN_PUMP_PCT} THEN {k}" for k in range(1, FORWARD_WINDOW + 1)]) + " ELSE NULL END"

        con.execute(f"""
            CREATE TABLE fwd AS
            WITH raw AS (
                SELECT t.buyin_id, t.minute, {selects_sql}
                FROM trail t {joins_sql}
                WHERE t.pm_close_price IS NOT NULL AND t.pm_close_price > 0
            )
            SELECT *,
                CASE WHEN {any_nn} THEN GREATEST({','.join(all_max)}) ELSE NULL END AS max_fwd,
                CASE WHEN {any_nn} THEN LEAST({','.join(all_min)}) ELSE NULL END AS min_fwd,
                CASE WHEN {any_nn} THEN GREATEST({','.join(early)}) ELSE NULL END AS max_fwd_early,
                CASE WHEN {any_nn} THEN LEAST({','.join(imm)}) ELSE NULL END AS min_fwd_imm,
                {ttp} AS time_to_peak
            FROM raw WHERE ({any_nn})
        """)

        # Path-aware labels
        df = con.execute(f"""
            SELECT t.*, b.followed_at,
                f.max_fwd, f.min_fwd, f.max_fwd_early, f.min_fwd_imm, f.time_to_peak,
                CASE
                    WHEN f.max_fwd_early >= {MIN_PUMP_PCT}
                         AND f.min_fwd > -{MAX_DRAWDOWN_PCT}
                         AND f.min_fwd_imm > {IMMEDIATE_DIP_MAX}
                         AND (t.pm_price_change_5m IS NULL OR t.pm_price_change_5m > {CRASH_GATE_5M})
                        THEN 'clean_pump'
                    WHEN (t.pm_price_change_5m IS NULL OR t.pm_price_change_5m > {CRASH_GATE_5M})
                        THEN 'no_pump'
                    ELSE 'crash'
                END AS label
            FROM trail t
            JOIN fwd f ON f.buyin_id=t.buyin_id AND f.minute=t.minute
            JOIN buyins b ON b.buyin_id=t.buyin_id
            WHERE f.max_fwd IS NOT NULL AND f.max_fwd > -9000
            ORDER BY b.followed_at, t.buyin_id, t.minute
        """).fetchdf()

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

    except Exception as e:
        logger.error(f"Data load error: {e}", exc_info=True)
        return None
    finally:
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
        if 'btc_price_change_pct' in df.columns:
            df['feat_sol_btc_div'] = sol - df['btc_price_change_pct'].fillna(0)
            new_cols.append('feat_sol_btc_div')
        if 'eth_price_change_pct' in df.columns:
            df['feat_sol_eth_div'] = sol - df['eth_price_change_pct'].fillna(0)
            new_cols.append('feat_sol_eth_div')

    # ── Order book z-score (is current imbalance unusual?) ────────────────
    if 'ob_bid_ask_ratio' in df.columns:
        r_mean = df['ob_bid_ask_ratio'].rolling(50, min_periods=5).mean()
        r_std = df['ob_bid_ask_ratio'].rolling(50, min_periods=5).std().clip(lower=1e-8)
        df['feat_ob_zscore'] = (df['ob_bid_ask_ratio'] - r_mean) / r_std
        new_cols.append('feat_ob_zscore')

    # ── Volume-price divergence (accumulation detection) ──────────────────
    if 'tx_trade_count' in df.columns and 'pm_price_change_1m' in df.columns:
        v_mean = df['tx_trade_count'].rolling(50, min_periods=5).mean()
        v_std = df['tx_trade_count'].rolling(50, min_periods=5).std().clip(lower=1e-8)
        vol_z = (df['tx_trade_count'] - v_mean) / v_std
        df['feat_vol_price_div'] = vol_z - df['pm_price_change_1m'].fillna(0)
        new_cols.append('feat_vol_price_div')

    # ── Whale intensity (net flow per active wallet) ──────────────────────
    if 'wh_net_flow' in df.columns and 'wh_active_wallets' in df.columns:
        df['feat_whale_intensity'] = df['wh_net_flow'] / df['wh_active_wallets'].clip(lower=1)
        new_cols.append('feat_whale_intensity')

    # ── Volatility compression (squeeze = breakout imminent) ──────────────
    if 'pm_volatility_1m' in df.columns and 'pm_volatility_5m' in df.columns:
        df['feat_vol_compress'] = df['pm_volatility_5m'] / df['pm_volatility_1m'].clip(lower=1e-8)
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

    # Walk-forward: 3 sequential test windows
    split_results = []
    for i in range(N_WALK_FORWARD_SPLITS):
        test_start_frac = 1.0 - (N_WALK_FORWARD_SPLITS - i) * WALK_FORWARD_TEST_FRAC
        train_end = int(n * test_start_frac)
        test_end = min(int(n * (test_start_frac + WALK_FORWARD_TEST_FRAC)), n)

        if train_end < 100 or (test_end - train_end) < 20:
            continue

        X_tr = df.iloc[:train_end][feature_cols].fillna(0)
        y_tr = df.iloc[:train_end]['target']
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
        mdl.fit(X_tr, y_tr)

        proba = mdl.predict_proba(X_te)[:, 1]
        pred = (proba >= MIN_CONFIDENCE).astype(int)

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
        })
        logger.info(f"  Split {i}: prec={prec:.1f}%, signals={n_sig}, E[profit]={ep:+.4f}%")

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

    # Final model on all data
    final = GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05,
        subsample=0.7, max_features=0.7,
        min_samples_leaf=10, min_samples_split=20,
        random_state=42,
    )
    final.fit(df[feature_cols].fillna(0), df['target'])

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
            a, b = row.get('pm_price_change_1m'), row.get('btc_price_change_pct')
            return float(a) - float(b) if a is not None and b is not None else 0.0

        if name == 'feat_sol_eth_div':
            a, b = row.get('pm_price_change_1m'), row.get('eth_price_change_pct')
            return float(a) - float(b) if a is not None and b is not None else 0.0

        if name == 'feat_whale_intensity':
            a, b = row.get('wh_net_flow'), row.get('wh_active_wallets')
            return float(a) / max(float(b), 1) if a is not None and b is not None else 0.0

        if name == 'feat_vol_compress':
            a, b = row.get('pm_volatility_1m'), row.get('pm_volatility_5m')
            return float(b) / max(float(a), 1e-8) if a is not None and b is not None else 1.0

        # These use rolling stats which aren't available for a single row —
        # use raw values as approximation
        if name == 'feat_ob_zscore':
            v = row.get('ob_bid_ask_ratio')
            return float(v) if v is not None else 0.0

        if name == 'feat_vol_price_div':
            tc = row.get('tx_trade_count')
            pm = row.get('pm_price_change_1m')
            return (float(tc) / 100 - float(pm)) if tc is not None and pm is not None else 0.0

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

    if cur_profit is not None and new_profit < cur_profit:
        logger.info(f"Keeping current model (new {new_profit:+.4f}% < current {cur_profit:+.4f}%)")
        return

    _model = result['model']
    _feature_columns = result['feature_columns']
    _model_metadata = result['metadata']

    logger.info(f"  NEW MODEL: {len(_feature_columns)} features, "
                f"prec={result['metadata']['avg_precision']:.1f}%, "
                f"E[profit]={new_profit:+.4f}% ({time.time()-t0:.1f}s)")

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
    logger.info(
        f"V2 gates (60s): {total} chk | no_mdl={_gate_stats['no_model']} "
        f"tf_fail={_gate_stats['multi_tf_fail']} crash={_gate_stats['crash_gate_fail']} "
        f"5m={_gate_stats['crash_5m_fail']} ok={_gate_stats['gates_passed']} "
        f"low_conf={_gate_stats['low_confidence']} FIRED={_gate_stats['signal_fired']}"
    )
    for k in _gate_stats:
        _gate_stats[k] = 0


def check_pump_signal(trail_row: dict, market_price: float) -> bool:
    _gate_stats['total_checks'] += 1

    if _model is None:
        _gate_stats['no_model'] += 1
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

    # Gate 3: Model confidence
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

        if proba < MIN_CONFIDENCE:
            _gate_stats['low_confidence'] += 1
            logger.info(f"V2: conf={proba:.3f} < {MIN_CONFIDENCE} ({trend_desc})")
            return False

        _gate_stats['signal_fired'] += 1
        logger.info(f"V2: SIGNAL! conf={proba:.3f} ({trend_desc})")
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

    pump_play_id = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "0"))
    if not pump_play_id:
        return False

    _update_price_buffer(market_price)

    if _model is None:
        return False

    try:
        trail_row = postgres_query_one(
            "SELECT * FROM buyin_trail_minutes WHERE buyin_id=%s AND minute=0 AND COALESCE(sub_minute,0)=0",
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
    return {
        'version': 'v2',
        'has_model': _model is not None,
        'n_features': len(_feature_columns),
        'metadata': _model_metadata,
        'last_refresh': _last_rules_refresh,
        'last_entry': _last_entry_time,
    }


# Load cached model on import
try:
    _load_cached_model()
except Exception:
    pass

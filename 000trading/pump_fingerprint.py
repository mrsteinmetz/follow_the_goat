"""
Pump Fingerprint Analysis V4 — Empirical Rule Discovery
=========================================================
Standalone analysis module that finds what distinguishes pumps from non-pumps,
clusters similar pump setups, and outputs actionable rules.

Instead of a black-box GBM, this module:
  1. Looks at every confirmed pump in the last 7 days
  2. Groups them by what the order book, transactions, and whale activity
     looked like before the move
  3. Only fires on patterns that have appeared AND succeeded multiple times
  4. Auto-updates every 5 minutes as new data comes in

Usage:
  python pump_fingerprint.py --lookback 168

Output:
  cache/pump_fingerprint_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger("pump_fingerprint")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_DIR = _PROJECT_ROOT / "cache"
_CACHE_DIR.mkdir(exist_ok=True)
_REPORT_PATH = _CACHE_DIR / "pump_fingerprint_report.json"

# =============================================================================
# CONSTANTS
# =============================================================================

DEFAULT_LOOKBACK_HOURS = 336  # 14 days

# Labeling — same thresholds as pump_signal_logic.py
MIN_PUMP_PCT = 0.2
MAX_DRAWDOWN_PCT = 0.10
FORWARD_WINDOW = 10
EARLY_WINDOW = 4
IMMEDIATE_DIP_MAX = -0.08
SUSTAINED_PCT = 0.10
CRASH_GATE_5M = -0.3

# Context filters — a real pump starts from a stable base, not a dip bounce
CONTEXT_5M_MIN = -0.15       # pm_price_change_5m must be above this
CONTEXT_1M_MIN = -0.05       # pm_price_change_1m must be above this
SUSTAINED_6M_MIN = 0.05      # fwd_6m must stay positive (V-shape rejection)
HIGH_VOL_THRESHOLD = 0.3     # pm_realized_vol_1m above this = high volatility
HIGH_VOL_MIN_TARGET = 0.3    # require higher target in high-vol conditions

# Pump classification: single label with adaptive vol threshold
# Base target = MIN_PUMP_PCT (0.2%). In high-vol (pm_realized_vol_1m > HIGH_VOL_THRESHOLD): HIGH_VOL_MIN_TARGET (0.3%)

# Combination discovery constraints
MIN_COMBO_PRECISION = 0.45
MIN_COMBO_SIGNALS = 5      # At least ~1x/day over 7 days
MIN_PATTERN_OCCURRENCES = 3
MIN_PATTERN_PRECISION = 0.50
MIN_SINGLE_PRECISION = 0.45

# =============================================================================
# FEATURE GROUPS — the ~30 features that matter for pump detection
# =============================================================================

ORDER_BOOK_FEATURES = [
    'ob_volume_imbalance',
    'ob_depth_imbalance_ratio',
    'ob_bid_depth_velocity',
    'ob_ask_depth_velocity',
    'ob_spread_bps',
    'ob_spread_velocity',
    'ob_microprice_deviation',
    'ob_aggression_ratio',
    'ob_imbalance_velocity_30s',
    'ob_cumulative_imbalance_5m',
    'ob_imbalance_shift_1m',
    'ob_net_flow_5m',
]

TRANSACTION_FEATURES = [
    'tx_buy_sell_pressure',
    'tx_buy_volume_pct',
    'tx_cumulative_delta',
    'tx_cumulative_delta_5m',
    'tx_volume_surge_ratio',
    'tx_aggressive_buy_ratio',
    'tx_trade_intensity',
    'tx_large_trade_count',
    'tx_whale_volume_pct',
]

WHALE_FEATURES = [
    'wh_net_flow_ratio',
    'wh_accumulation_ratio',
    'wh_flow_velocity',
    'wh_flow_acceleration',
    'wh_cumulative_flow_5m',
    'wh_stealth_acc_score',
]

MOMENTUM_FEATURES = [
    'pm_price_velocity_30s',
    'pm_price_change_1m',
    'pm_trend_strength_ema',
    'pm_momentum_acceleration_1m',
]

ALL_SIGNAL_FEATURES = (
    ORDER_BOOK_FEATURES
    + TRANSACTION_FEATURES
    + WHALE_FEATURES
    + MOMENTUM_FEATURES
)

# Features where LOWER values are bullish (bearish-polarity features)
BEARISH_POLARITY_FEATURES = frozenset([
    'ob_ask_depth_velocity',   # Asks decreasing = bullish
    'ob_spread_bps',           # Tighter spread = bullish
    'ob_spread_velocity',      # Negative = spread tightening = bullish
])

# Primary signal features — OB, TX, and whale groups are leading indicators
PRIMARY_SIGNAL_FEATURES = frozenset(ORDER_BOOK_FEATURES + TRANSACTION_FEATURES + WHALE_FEATURES)

# Momentum features allowed as CONFIRMATION (3rd slot in combos only)
ALLOWED_MOMENTUM_FEATURES = frozenset([
    'pm_price_velocity_30s',
    'pm_momentum_acceleration_1m',
])

# Dip-proxy features: DISQUALIFIED from rule building because they measure
# "is this a dip?" rather than "is this a breakout?". Computed for transparency
# but excluded from the combination/pattern feature pool.
DIP_PROXY_FEATURES = frozenset({
    'pm_trend_strength_ema',
    'pm_price_change_1m',
    'pm_price_change_5m',
    'pm_price_change_10m',
    'pm_cumulative_return_5m',
    'pm_price_vs_ma5_pct',
    'pm_price_vs_vwap_pct',
    'pm_price_vs_twap_pct',
    'pre_entry_change_1m',
    'pre_entry_change_2m',
    'pre_entry_change_3m',
    'pre_entry_change_5m',
    'pre_entry_change_10m',
})

# Signal deduplication window
DEDUP_WINDOW_MINUTES = 10
NON_PUMP_DOWNSAMPLE_RATIO = 5  # keep ~5 non-pumps per independent pump


# =============================================================================
# SIGNAL DEDUPLICATION
# =============================================================================

def deduplicate_signals(df: pd.DataFrame,
                        window_minutes: int = DEDUP_WINDOW_MINUTES,
                        ) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Group pump signals within window_minutes of each other.

    For each cluster of pump signals within the window, keeps only the FIRST
    signal (earliest followed_at). All subsequent pump signals in the cluster
    are duplicates from the same market event.

    Non-pump rows are always kept (baseline comparison), then downsampled to
    ~NON_PUMP_DOWNSAMPLE_RATIO:1 ratio vs independent pumps.

    Returns (deduped_df, stats_dict).
    """
    df = df.sort_values('followed_at').reset_index(drop=True)

    raw_pumps = int((df['is_pump'] == 1).sum())
    raw_non_pumps = int((df['is_pump'] == 0).sum())

    keep_mask = pd.Series(False, index=df.index)
    last_kept_time = None
    window = timedelta(minutes=window_minutes)

    for idx in df.index:
        if df.at[idx, 'is_pump'] != 1:
            keep_mask[idx] = True
            continue

        row_time = pd.Timestamp(df.at[idx, 'followed_at'])
        if last_kept_time is None or (row_time - last_kept_time) >= window:
            keep_mask[idx] = True
            last_kept_time = row_time

    deduped = df[keep_mask].reset_index(drop=True)

    independent_pumps = int((deduped['is_pump'] == 1).sum())
    kept_non_pumps = int((deduped['is_pump'] == 0).sum())

    # Downsample non-pumps to ~NON_PUMP_DOWNSAMPLE_RATIO:1
    target_non_pumps = independent_pumps * NON_PUMP_DOWNSAMPLE_RATIO
    if kept_non_pumps > target_non_pumps and target_non_pumps > 0:
        pump_rows = deduped[deduped['is_pump'] == 1]
        non_pump_rows = deduped[deduped['is_pump'] == 0].sample(
            n=target_non_pumps, random_state=42,
        )
        deduped = pd.concat([pump_rows, non_pump_rows]).sort_values(
            'followed_at',
        ).reset_index(drop=True)
        sampled_non_pumps = target_non_pumps
    else:
        sampled_non_pumps = kept_non_pumps

    stats = {
        'raw_pumps': raw_pumps,
        'independent_pumps': independent_pumps,
        'duplicates_removed': raw_pumps - independent_pumps,
        'raw_non_pumps': raw_non_pumps,
        'sampled_non_pumps': sampled_non_pumps,
        'window_minutes': window_minutes,
    }

    logger.info(f"  Deduplication: {raw_pumps} raw pumps -> {independent_pumps} independent "
                f"({raw_pumps - independent_pumps} duplicates, {window_minutes}min window)")
    logger.info(f"  Non-pump downsample: {raw_non_pumps} -> {sampled_non_pumps} "
                f"(ratio {sampled_non_pumps / max(independent_pumps, 1):.1f}:1)")

    return deduped, stats


# =============================================================================
# 1A: DATA LOADING
# =============================================================================

def load_trail_data(lookback_hours: int = DEFAULT_LOOKBACK_HOURS) -> Optional[pd.DataFrame]:
    """Load trail data for fingerprint analysis.

    Uses the existing DuckDB trail cache from pump_signal_logic.py with a
    longer lookback window (168h vs 48h). Computes forward returns and
    multi-tier labels.

    Returns DataFrame with features + forward returns + tier labels, or None.
    """
    logger.info(f"Loading trail data (last {lookback_hours}h)...")
    t0 = time.time()

    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        from pump_signal_logic import _sync_trail_cache

        con = _sync_trail_cache(lookback_hours)

        n_rows = con.execute("SELECT COUNT(*) FROM cached_trail").fetchone()[0]
        n_buyins = con.execute(
            "SELECT COUNT(DISTINCT buyin_id) FROM cached_trail"
        ).fetchone()[0]

        if n_rows == 0:
            logger.warning("Cache is empty after sync")
            return None
        logger.info(f"  Cache: {n_rows:,} trail rows, {n_buyins:,} buyins")

        # Forward returns via LEAD()
        lead_cols = []
        for k in range(1, FORWARD_WINDOW + 1):
            lead_cols.append(
                f"(LEAD(pm_close_price, {k}) OVER w - pm_close_price) "
                f"/ NULLIF(pm_close_price, 0) * 100 AS fwd_{k}m"
            )
        lead_sql = ",\n            ".join(lead_cols)

        fwd_all = [f"fwd_{k}m" for k in range(1, FORWARD_WINDOW + 1)]
        fwd_early = [f"fwd_{k}m" for k in range(1, EARLY_WINDOW + 1)]
        fwd_imm = [f"fwd_{k}m" for k in range(1, min(3, FORWARD_WINDOW + 1))]

        greatest_all = f"GREATEST({', '.join(fwd_all)})"
        least_all = f"LEAST({', '.join(fwd_all)})"
        greatest_early = f"GREATEST({', '.join(fwd_early)})"
        least_imm = f"LEAST({', '.join(fwd_imm)})"
        any_not_null = " OR ".join([f"{c} IS NOT NULL" for c in fwd_all])

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
                CASE WHEN {any_not_null} THEN {least_imm}    ELSE NULL END AS min_fwd_imm
            FROM base
            WHERE ({any_not_null})
        """)

        # Single pump label with adaptive vol threshold (minute=0 only).
        # Context filters eliminate dip bounces: a real pump starts from a
        # stable or rising base, not from a -0.5% dip bouncing back 0.2%.
        # In high volatility, require a higher target to avoid labeling
        # normal fluctuations as pumps.
        arrow_result = con.execute(f"""
            SELECT *,
                CASE
                    WHEN max_fwd_early >= (
                             CASE WHEN pm_realized_vol_1m IS NOT NULL
                                       AND pm_realized_vol_1m > {HIGH_VOL_THRESHOLD}
                                  THEN {HIGH_VOL_MIN_TARGET}
                                  ELSE {MIN_PUMP_PCT}
                             END
                         )
                         AND min_fwd > -{MAX_DRAWDOWN_PCT}
                         AND min_fwd_imm > {IMMEDIATE_DIP_MAX}
                         AND fwd_4m IS NOT NULL AND fwd_4m >= {SUSTAINED_PCT}
                         AND (pm_price_change_5m IS NULL OR pm_price_change_5m > {CONTEXT_5M_MIN})
                         AND (pm_price_change_1m IS NULL OR pm_price_change_1m > {CONTEXT_1M_MIN})
                         AND (fwd_6m IS NULL OR fwd_6m >= {SUSTAINED_6M_MIN})
                        THEN 1
                    ELSE 0
                END AS is_pump
            FROM fwd_returns
            WHERE max_fwd IS NOT NULL
              AND minute = 0
            ORDER BY followed_at, buyin_id
        """).arrow()
        df = arrow_result.read_all().to_pandas()

        elapsed = time.time() - t0
        n_pump = int(df['is_pump'].sum())
        logger.info(f"  {len(df):,} entries in {elapsed:.1f}s  "
                    f"(pumps: {n_pump}, non-pumps: {len(df) - n_pump})")

        return df

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
# 1B: FEATURE SEPARATION ANALYSIS
# =============================================================================

def compute_separation(pump_values: np.ndarray,
                       non_pump_values: np.ndarray) -> float:
    """How well does this feature separate pumps from non-pumps?

    Uses robust separation: (median_pump - median_non_pump) / pooled_IQR.
    Higher absolute value = better separator.
    """
    pump_clean = pump_values[~np.isnan(pump_values)]
    non_pump_clean = non_pump_values[~np.isnan(non_pump_values)]

    if len(pump_clean) < 5 or len(non_pump_clean) < 5:
        return 0.0

    med_p = np.median(pump_clean)
    med_np = np.median(non_pump_clean)
    iqr_p = np.subtract(*np.percentile(pump_clean, [75, 25]))
    iqr_np = np.subtract(*np.percentile(non_pump_clean, [75, 25]))
    pooled_iqr = (iqr_p + iqr_np) / 2

    if pooled_iqr < 1e-10:
        return 0.0

    return (med_p - med_np) / pooled_iqr


def rank_features(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Compute separation scores for all signal features, ranked by |score|.

    Computes separation for ALL features (transparency), but marks features
    in DIP_PROXY_FEATURES as rule_eligible=False so they are excluded from
    the combination/pattern rule-building pool.

    Returns list of dicts with feature name, separation score, medians, rule_eligible.
    """
    pump_mask = df['is_pump'] == 1
    results = []

    for feat in ALL_SIGNAL_FEATURES:
        if feat not in df.columns:
            continue

        pump_vals = df.loc[pump_mask, feat].values.astype(float)
        non_pump_vals = df.loc[~pump_mask, feat].values.astype(float)

        sep = compute_separation(pump_vals, non_pump_vals)

        pump_clean = pump_vals[~np.isnan(pump_vals)]
        non_pump_clean = non_pump_vals[~np.isnan(non_pump_vals)]

        eligible = feat not in DIP_PROXY_FEATURES

        results.append({
            'feature': feat,
            'separation': round(float(sep), 4),
            'abs_separation': round(abs(float(sep)), 4),
            'median_pump': round(float(np.median(pump_clean)), 6) if len(pump_clean) > 0 else 0.0,
            'median_non_pump': round(float(np.median(non_pump_clean)), 6) if len(non_pump_clean) > 0 else 0.0,
            'n_pump_valid': int(len(pump_clean)),
            'n_non_pump_valid': int(len(non_pump_clean)),
            'rule_eligible': eligible,
        })

    results.sort(key=lambda x: x['abs_separation'], reverse=True)

    logger.info("Feature separation rankings (top 20):")
    for i, r in enumerate(results[:20]):
        elig = " " if r['rule_eligible'] else "X"
        logger.info(f"  {i+1:2d}. [{elig}] {r['feature']:40s}  sep={r['separation']:+.4f}  "
                    f"med_pump={r['median_pump']:.6f}  med_np={r['median_non_pump']:.6f}")

    n_eligible = sum(1 for r in results if r['rule_eligible'])
    n_excluded = sum(1 for r in results if not r['rule_eligible'])
    logger.info(f"  Rule-eligible: {n_eligible}, Excluded (dip proxies): {n_excluded}")

    return results


# =============================================================================
# 1C: THRESHOLD DISCOVERY
# =============================================================================

def find_optimal_threshold(
    feature_values: np.ndarray,
    labels: np.ndarray,
    feature_name: str,
    separation_score: float = 0.0,
    min_precision: float = MIN_SINGLE_PRECISION,
) -> Optional[Dict[str, Any]]:
    """Find the threshold that maximizes precision * sqrt(recall).

    Tests thresholds at 10th, 20th, ... 90th percentile of feature values.
    Direction is auto-detected from separation score:
    - Positive separation (pump median > non-pump): test "above" thresholds
    - Negative separation (pump median < non-pump): test "below" thresholds
    - Tests BOTH directions and picks the best to handle edge cases

    Returns dict with threshold, precision, recall, n_signals, or None.
    """
    clean_mask = ~np.isnan(feature_values)
    vals = feature_values[clean_mask]
    labs = labels[clean_mask]

    if len(vals) < 20 or labs.sum() < 3:
        return None

    percentiles = np.arange(10, 100, 10)
    thresholds = np.percentile(vals, percentiles)

    best = None
    best_score = -1.0

    total_pumps = labs.sum()

    # Test both directions; pick whichever produces higher score
    for direction in ('above', 'below'):
        for thr in thresholds:
            if direction == 'below':
                mask = vals < thr
            else:
                mask = vals > thr

            n_signals = mask.sum()
            if n_signals < 3:
                continue

            n_hits = labs[mask].sum()
            precision = n_hits / n_signals if n_signals > 0 else 0.0
            recall = n_hits / total_pumps if total_pumps > 0 else 0.0

            if precision < min_precision:
                continue

            score = precision * np.sqrt(recall) if recall > 0 else 0.0

            if score > best_score:
                best_score = score
                best = {
                    'threshold': round(float(thr), 6),
                    'precision': round(float(precision), 4),
                    'recall': round(float(recall), 4),
                    'n_signals': int(n_signals),
                    'n_hits': int(n_hits),
                    'score': round(float(score), 4),
                    'direction': direction,
                }

    return best


def discover_thresholds(
    df: pd.DataFrame,
    feature_rankings: List[Dict[str, Any]],
    top_n: int = 15,
) -> Dict[str, Dict[str, Any]]:
    """Find optimal thresholds for the top N rule-eligible features by separation.

    Only considers features marked rule_eligible=True (excludes dip proxies).
    Returns dict mapping feature name to threshold info.
    """
    labels = df['is_pump'].values.astype(float)
    thresholds = {}

    eligible_rankings = [r for r in feature_rankings if r.get('rule_eligible', True)]

    for rank_info in eligible_rankings[:top_n]:
        feat = rank_info['feature']
        if feat not in df.columns:
            continue

        result = find_optimal_threshold(
            df[feat].values.astype(float),
            labels,
            feat,
            separation_score=rank_info.get('separation', 0.0),
        )
        if result is not None:
            thresholds[feat] = result
            logger.info(f"  Threshold: {feat:40s}  "
                        f"{result['direction']} {result['threshold']:+.6f}  "
                        f"prec={result['precision']:.2%}  "
                        f"recall={result['recall']:.2%}  "
                        f"n={result['n_signals']}")

    return thresholds


# =============================================================================
# 1D: COMBINATION DISCOVERY
# =============================================================================

def _check_feature_condition(values: np.ndarray, threshold: float,
                             direction: str) -> np.ndarray:
    """Return boolean mask for feature passing its threshold."""
    if direction == 'below':
        return values < threshold
    return values > threshold


def find_best_combinations(
    df: pd.DataFrame,
    feature_thresholds: Dict[str, Dict[str, Any]],
    top_n: int = 10,
) -> List[Dict[str, Any]]:
    """Find 2-feature and 3-feature combinations with highest hit rate.

    Feature pool restrictions (to prevent dip-buying patterns):
      - Pairs: both features must be from PRIMARY_SIGNAL_FEATURES (OB/TX/whale)
      - Triples: first 2 from PRIMARY_SIGNAL_FEATURES, 3rd can be from
        ALLOWED_MOMENTUM_FEATURES (pm_price_velocity_30s, pm_momentum_acceleration_1m)
      - DIP_PROXY_FEATURES are never used in any slot

    Returns combinations sorted by score (precision * n_signals).
    """
    labels = df['is_pump'].values.astype(float)

    # Get features with valid thresholds, sorted by single-feature score
    feat_items = sorted(
        feature_thresholds.items(),
        key=lambda x: x[1].get('score', 0),
        reverse=True,
    )[:top_n]

    if len(feat_items) < 2:
        logger.warning("Not enough features with thresholds for combinations")
        return []

    # Split features into primary (OB/TX/whale) and allowed momentum
    primary_names = [f for f, _ in feat_items if f in PRIMARY_SIGNAL_FEATURES]
    momentum_names = [f for f, _ in feat_items if f in ALLOWED_MOMENTUM_FEATURES]
    all_combo_names = list(dict.fromkeys(primary_names + momentum_names))

    logger.info(f"  Combo pool: {len(primary_names)} primary (OB/TX/whale), "
                f"{len(momentum_names)} allowed momentum")

    # Precompute masks for all eligible features
    masks = {}
    for fname in all_combo_names:
        if fname not in df.columns:
            continue
        vals = df[fname].values.astype(float)
        thr = feature_thresholds[fname]['threshold']
        direction = feature_thresholds[fname].get('direction', 'above')
        masks[fname] = _check_feature_condition(vals, thr, direction)

    results = []

    # Test pairs (both must be primary OB/TX/whale features)
    primary_in_masks = [f for f in primary_names if f in masks]
    good_pairs = []
    for fa, fb in combinations(primary_in_masks, 2):
        combined = masks[fa] & masks[fb]
        n_signals = int(combined.sum())
        if n_signals < MIN_COMBO_SIGNALS:
            continue

        n_hits = int(labels[combined].sum())
        precision = n_hits / n_signals if n_signals > 0 else 0.0

        if precision >= MIN_COMBO_PRECISION:
            avg_gain = float(df.loc[combined & (labels == 1), 'max_fwd'].mean()) \
                if (combined & (labels == 1)).sum() > 0 else 0.0

            entry = {
                'features': [fa, fb],
                'thresholds': [
                    feature_thresholds[fa]['threshold'],
                    feature_thresholds[fb]['threshold'],
                ],
                'directions': [
                    feature_thresholds[fa]['direction'],
                    feature_thresholds[fb]['direction'],
                ],
                'precision': round(precision, 4),
                'n_signals': n_signals,
                'n_hits': n_hits,
                'score': round(precision * n_signals, 2),
                'avg_gain_on_hit': round(avg_gain, 4),
            }
            results.append(entry)

            if precision >= 0.40:
                good_pairs.append((fa, fb))

    # Test triples: extend good pairs with primary OR allowed momentum as 3rd
    allowed_third = [f for f in masks.keys()
                     if f in PRIMARY_SIGNAL_FEATURES or f in ALLOWED_MOMENTUM_FEATURES]
    seen_triples = set()
    for fa, fb in good_pairs:
        for fc in allowed_third:
            if fc in (fa, fb):
                continue
            triple_key = tuple(sorted([fa, fb, fc]))
            if triple_key in seen_triples:
                continue
            seen_triples.add(triple_key)
            combined = masks[fa] & masks[fb] & masks[fc]
            n_signals = int(combined.sum())
            if n_signals < MIN_COMBO_SIGNALS:
                continue

            n_hits = int(labels[combined].sum())
            precision = n_hits / n_signals if n_signals > 0 else 0.0

            if precision >= MIN_COMBO_PRECISION:
                avg_gain = float(df.loc[combined & (labels == 1), 'max_fwd'].mean()) \
                    if (combined & (labels == 1)).sum() > 0 else 0.0

                entry = {
                    'features': [fa, fb, fc],
                    'thresholds': [
                        feature_thresholds[fa]['threshold'],
                        feature_thresholds[fb]['threshold'],
                        feature_thresholds[fc]['threshold'],
                    ],
                    'directions': [
                        feature_thresholds[fa]['direction'],
                        feature_thresholds[fb]['direction'],
                        feature_thresholds[fc]['direction'],
                    ],
                    'precision': round(precision, 4),
                    'n_signals': n_signals,
                    'n_hits': n_hits,
                    'score': round(precision * n_signals, 2),
                    'avg_gain_on_hit': round(avg_gain, 4),
                }
                results.append(entry)

    results.sort(key=lambda x: x['score'], reverse=True)

    logger.info(f"Found {len(results)} valid combinations")
    for i, c in enumerate(results[:10]):
        feats = ' + '.join(c['features'])
        logger.info(f"  {i+1:2d}. [{feats}]  "
                    f"prec={c['precision']:.2%}  n={c['n_signals']}  "
                    f"score={c['score']:.1f}  avg_gain={c['avg_gain_on_hit']:.3f}%")

    return results


# =============================================================================
# 1E: PATTERN CLUSTERING
# =============================================================================

def _bucket_value(value: float, p33: float, p66: float) -> str:
    """Bucket a value into low/mid/high based on tertile thresholds."""
    if value <= p33:
        return 'low'
    elif value <= p66:
        return 'mid'
    else:
        return 'high'


def _generate_pattern_label(buckets: Dict[str, str]) -> str:
    """Generate a human-readable pattern label from feature buckets."""
    parts = []
    for feat, bucket in buckets.items():
        short_name = feat.replace('ob_', '').replace('tx_', '') \
                         .replace('wh_', '').replace('pm_', '')
        parts.append(f"{bucket}_{short_name}")
    return ' + '.join(parts)


def cluster_pump_patterns(
    df: pd.DataFrame,
    feature_rankings: List[Dict[str, Any]],
    top_n_cluster: int = 3,
    top_n_range: int = 8,
) -> List[Dict[str, Any]]:
    """Cluster confirmed pumps by their feature fingerprint.

    Uses quantile-based bucketing on the top features to create pattern labels.
    Each pump gets assigned to a pattern based on whether its top feature
    values are low/mid/high. Patterns that appear >= 3 times with >= 50%
    precision become "approved patterns".

    Args:
        df: Full dataset with is_pump column
        feature_rankings: Sorted feature rankings from rank_features()
        top_n_cluster: Number of top features for bucketing (creates labels)
        top_n_range: Number of top features for range computation

    Returns:
        List of approved pattern dicts.
    """
    # Get top rule-eligible features that exist in the dataframe
    cluster_features = []
    range_features = []
    for r in feature_rankings:
        fname = r['feature']
        if fname not in df.columns:
            continue
        if not r.get('rule_eligible', True):
            continue
        if len(cluster_features) < top_n_cluster:
            cluster_features.append(fname)
        if len(range_features) < top_n_range:
            range_features.append(fname)
        if len(range_features) >= top_n_range:
            break

    if len(cluster_features) < 2:
        logger.warning("Not enough features for clustering")
        return []

    pump_df = df[df['is_pump'] == 1].copy()
    all_df = df.copy()

    if len(pump_df) < 5:
        logger.warning(f"Only {len(pump_df)} pumps — not enough for clustering")
        return []

    # Compute tertile thresholds from ALL data (not just pumps)
    tertiles = {}
    for feat in cluster_features:
        vals = all_df[feat].dropna()
        if len(vals) < 10:
            continue
        tertiles[feat] = {
            'p33': float(np.percentile(vals, 33.33)),
            'p66': float(np.percentile(vals, 66.67)),
        }

    cluster_features = [f for f in cluster_features if f in tertiles]
    if len(cluster_features) < 2:
        logger.warning("Not enough features with valid tertiles")
        return []

    # Assign pattern labels to ALL rows
    def row_pattern(row):
        buckets = {}
        for feat in cluster_features:
            val = row[feat]
            if pd.isna(val):
                buckets[feat] = 'mid'
            else:
                buckets[feat] = _bucket_value(
                    float(val), tertiles[feat]['p33'], tertiles[feat]['p66']
                )
        return _generate_pattern_label(buckets)

    all_df['pattern_label'] = all_df.apply(row_pattern, axis=1)

    # Compute per-pattern statistics
    pattern_stats = all_df.groupby('pattern_label').agg(
        n_occurrences=('is_pump', 'count'),
        n_pumps=('is_pump', 'sum'),
    ).reset_index()

    pattern_stats['precision'] = pattern_stats['n_pumps'] / pattern_stats['n_occurrences']

    # Filter to approved patterns
    approved_mask = (
        (pattern_stats['n_occurrences'] >= MIN_PATTERN_OCCURRENCES)
        & (pattern_stats['precision'] >= MIN_PATTERN_PRECISION)
    )
    approved = pattern_stats[approved_mask].sort_values('precision', ascending=False)

    results = []
    for idx, row in approved.iterrows():
        label = row['pattern_label']
        pattern_rows = all_df[all_df['pattern_label'] == label]
        pump_rows = pattern_rows[pattern_rows['is_pump'] == 1]

        avg_gain = float(pump_rows['max_fwd'].mean()) if len(pump_rows) > 0 else 0.0

        # Compute feature ranges from rows matching this pattern
        feature_ranges = {}
        for feat in range_features:
            if feat not in pattern_rows.columns:
                continue
            vals = pattern_rows[feat].dropna()
            if len(vals) == 0:
                continue
            feature_ranges[feat] = [
                round(float(vals.quantile(0.10)), 6),
                round(float(vals.quantile(0.90)), 6),
            ]

        # Generate a descriptive label
        parts = label.split(' + ')
        desc_parts = []
        for p in parts:
            bucket, feat_short = p.split('_', 1)
            if bucket == 'high':
                desc_parts.append(f"{feat_short} high")
            elif bucket == 'low':
                desc_parts.append(f"{feat_short} low")

        description = ' + '.join(desc_parts) if desc_parts else label

        results.append({
            'cluster_id': len(results),
            'label': label,
            'description': description,
            'n_occurrences': int(row['n_occurrences']),
            'n_pumps': int(row['n_pumps']),
            'precision': round(float(row['precision']), 4),
            'avg_gain': round(avg_gain, 4),
            'feature_ranges': feature_ranges,
            'cluster_features': cluster_features,
        })

    logger.info(f"Found {len(results)} approved patterns "
                f"(from {len(pattern_stats)} total patterns)")
    for p in results:
        logger.info(f"  Pattern: {p['label']}  "
                    f"prec={p['precision']:.2%}  "
                    f"n={p['n_occurrences']}  "
                    f"pumps={p['n_pumps']}  "
                    f"avg_gain={p['avg_gain']:.3f}%")

    return results


# =============================================================================
# 1F: STRESS TEST — Reject rules that fire on crash days
# =============================================================================

STRESS_TEST_N_WORST_DAYS = 3
STRESS_TEST_MIN_PRECISION = 0.30
STRESS_TEST_MAX_BAD_DAY_SIGNALS = 20


def stress_test_rules(
    df: pd.DataFrame,
    rules: List[Dict[str, Any]],
    feature_thresholds: Dict[str, Dict[str, Any]],
    n_worst_days: int = STRESS_TEST_N_WORST_DAYS,
    source_type: str = 'combination',
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Test rules against the worst N days in the dataset.

    For each of the N days with the largest SOL price decline:
      1. Count how many signals the rule would fire
      2. Compute precision on that day alone
      3. If precision < 30% on ANY worst day -> FAILED
      4. If n_signals > 20 on a single bad day -> EXCESSIVE_BAD_DAY_FIRING

    Args:
        df: Full dataset with is_pump, followed_at, pm_close_price columns
        rules: List of combination or pattern rule dicts
        feature_thresholds: Threshold info for combination rules
        n_worst_days: Number of worst days to test against
        source_type: 'combination' or 'cluster'

    Returns:
        (passed_rules, stress_results) where stress_results has per-rule details
    """
    if 'followed_at' not in df.columns or 'pm_close_price' not in df.columns:
        logger.warning("Cannot stress test: missing followed_at or pm_close_price")
        return rules, []

    df_with_date = df.copy()
    df_with_date['_date'] = pd.to_datetime(df_with_date['followed_at']).dt.date

    daily_prices = df_with_date.groupby('_date').agg(
        start_price=('pm_close_price', 'first'),
        end_price=('pm_close_price', 'last'),
    )
    daily_prices['return_pct'] = (
        (daily_prices['end_price'] / daily_prices['start_price'].clip(lower=1e-10)) - 1
    ) * 100

    if len(daily_prices) < n_worst_days:
        logger.warning(f"Only {len(daily_prices)} days of data, need {n_worst_days} for stress test")
        return rules, []

    worst_days = daily_prices.nsmallest(n_worst_days, 'return_pct')
    worst_day_dates = set(worst_days.index)

    logger.info(f"Stress test: {n_worst_days} worst days:")
    for d, row in worst_days.iterrows():
        logger.info(f"  {d}: {row['return_pct']:+.2f}%")

    passed_rules = []
    stress_results = []

    for rule in rules:
        rule_passed = True
        worst_day_details = []

        for day_date in worst_day_dates:
            day_df = df_with_date[df_with_date['_date'] == day_date]
            if len(day_df) == 0:
                continue

            if source_type == 'combination':
                mask = pd.Series(True, index=day_df.index)
                for feat, thr, direction in zip(
                    rule['features'], rule['thresholds'], rule['directions']
                ):
                    if feat not in day_df.columns:
                        continue
                    vals = day_df[feat].values.astype(float)
                    mask &= pd.Series(
                        _check_feature_condition(vals, thr, direction),
                        index=day_df.index,
                    )
            else:
                # Pattern/cluster rules use feature_ranges
                mask = pd.Series(True, index=day_df.index)
                for feat, (lo, hi) in rule.get('feature_ranges', {}).items():
                    if feat not in day_df.columns:
                        continue
                    vals = day_df[feat].astype(float)
                    mask &= (vals >= lo) & (vals <= hi)

            n_signals = int(mask.sum())
            n_hits = int(day_df.loc[mask, 'is_pump'].sum()) if n_signals > 0 else 0
            day_precision = n_hits / n_signals if n_signals > 0 else 1.0

            day_return = float(worst_days.loc[day_date, 'return_pct'])

            worst_day_details.append({
                'date': str(day_date),
                'day_return_pct': round(day_return, 2),
                'n_signals': n_signals,
                'n_hits': n_hits,
                'precision': round(day_precision, 4),
            })

            if n_signals > 0 and day_precision < STRESS_TEST_MIN_PRECISION:
                rule_passed = False
            if n_signals > STRESS_TEST_MAX_BAD_DAY_SIGNALS:
                rule_passed = False

        min_worst_prec = min(
            (d['precision'] for d in worst_day_details if d['n_signals'] > 0),
            default=1.0,
        )
        max_worst_signals = max(
            (d['n_signals'] for d in worst_day_details),
            default=0,
        )

        stress_results.append({
            'rule_features': rule.get('features', rule.get('label', '?')),
            'passed': rule_passed,
            'worst_day_min_precision': round(min_worst_prec, 4),
            'worst_day_max_signals': max_worst_signals,
            'day_details': worst_day_details,
        })

        if rule_passed:
            passed_rules.append(rule)

    n_passed = len(passed_rules)
    n_failed = len(rules) - n_passed
    logger.info(f"  Stress test ({source_type}): {n_passed} passed, {n_failed} failed "
                f"(of {len(rules)} total)")

    return passed_rules, stress_results


# =============================================================================
# 1G: QUALITY SCORING
# =============================================================================

MIN_QUALITY_SCORE = 0.06
MIN_QUALITY_PRECISION = 0.40


def compute_rule_quality(
    precision: float,
    n_independent_signals: int,
    avg_gain: float,
    worst_day_precision: float,
    lookback_days: float,
) -> Dict[str, Any]:
    """Composite quality score balancing precision, frequency, gain, and robustness.

    Components:
      - precision: overall hit rate (target: > 45%)
      - signal_rate: independent signals per day (full credit at 1/day)
      - avg_gain: average gain on hits (target: > 0.3%)
      - robustness: worst-day precision (target: > 25%)

    Score = precision * min(avg_gain, 1.0) * robustness_factor * frequency_factor

    Frequency: full credit at >= 1 signal/day. Hard zero below 0.5/day
    (1 signal every 2 days is too sparse to trust).

    Returns dict with score and component breakdown.
    """
    signal_rate = n_independent_signals / max(lookback_days, 1)
    robustness_factor = min(max(worst_day_precision / 0.30, 0), 1.0)
    capped_gain = min(avg_gain, 1.0)

    if signal_rate < 0.5:
        frequency_factor = 0.0
    else:
        frequency_factor = min(signal_rate / 1.0, 1.0)

    score = precision * capped_gain * robustness_factor * frequency_factor
    approved = score > MIN_QUALITY_SCORE and precision > MIN_QUALITY_PRECISION

    return {
        'quality_score': round(score, 4),
        'approved': approved,
        'precision': round(precision, 4),
        'signal_rate_per_day': round(signal_rate, 2),
        'avg_gain': round(avg_gain, 4),
        'robustness_factor': round(robustness_factor, 4),
        'frequency_factor': round(frequency_factor, 4),
        'worst_day_precision': round(worst_day_precision, 4),
    }


# =============================================================================
# 1H: OUTPUT — Build and Write Report
# =============================================================================

def build_report(
    df: pd.DataFrame,
    feature_rankings: List[Dict[str, Any]],
    thresholds: Dict[str, Dict[str, Any]],
    best_combinations: List[Dict[str, Any]],
    approved_patterns: List[Dict[str, Any]],
    lookback_hours: int,
    dedup_stats: Optional[Dict[str, int]] = None,
    combo_stress_results: Optional[List[Dict[str, Any]]] = None,
    pattern_stress_results: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the full fingerprint report dict with quality gating.

    Rules must pass both stress test AND quality scoring to be recommended.
    """
    lookback_days = lookback_hours / 24.0

    n_pumps = int(df['is_pump'].sum())
    n_non_pump = len(df) - n_pumps

    def _worst_day_prec(features_key, stress_results):
        """Look up worst-day precision from stress test results."""
        if not stress_results:
            return 1.0
        for sr in stress_results:
            if sr.get('rule_features') == features_key:
                return sr.get('worst_day_min_precision', 1.0)
        return 1.0

    recommended_rules = []
    quality_scores = []
    rule_id = 0

    for combo in best_combinations[:20]:
        rule_id += 1
        conditions = {}
        for feat, thr, direction in zip(
            combo['features'], combo['thresholds'], combo['directions']
        ):
            conditions[feat] = {
                'threshold': thr,
                'direction': direction,
            }
        signals_per_day = combo['n_signals'] / lookback_days if lookback_days > 0 else 0.0

        worst_prec = _worst_day_prec(combo['features'], combo_stress_results)
        quality = compute_rule_quality(
            precision=combo['precision'],
            n_independent_signals=combo['n_signals'],
            avg_gain=combo.get('avg_gain_on_hit', 0.0),
            worst_day_precision=worst_prec,
            lookback_days=lookback_days,
        )

        rule_entry = {
            'rule_id': rule_id,
            'conditions': conditions,
            'expected_precision': combo['precision'],
            'expected_signals_per_day': round(signals_per_day, 1),
            'n_signals_in_window': combo['n_signals'],
            'avg_gain_on_hit': combo.get('avg_gain_on_hit', 0.0),
            'source': 'combination',
            'quality_score': quality['quality_score'],
            'quality_approved': quality['approved'],
        }

        if quality['approved']:
            recommended_rules.append(rule_entry)

        quality_scores.append({
            'rule_id': rule_id,
            'source': 'combination',
            'features': combo['features'],
            **quality,
        })

    for pattern in approved_patterns:
        rule_id += 1
        signals_per_day = pattern['n_occurrences'] / lookback_days if lookback_days > 0 else 0.0

        worst_prec = _worst_day_prec(pattern.get('label', '?'), pattern_stress_results)
        quality = compute_rule_quality(
            precision=pattern['precision'],
            n_independent_signals=pattern['n_occurrences'],
            avg_gain=pattern.get('avg_gain', 0.0),
            worst_day_precision=worst_prec,
            lookback_days=lookback_days,
        )

        rule_entry = {
            'rule_id': rule_id,
            'pattern_id': pattern['cluster_id'],
            'pattern_label': pattern['label'],
            'feature_ranges': pattern['feature_ranges'],
            'expected_precision': pattern['precision'],
            'expected_signals_per_day': round(signals_per_day, 1),
            'n_signals_in_window': pattern['n_occurrences'],
            'avg_gain_on_hit': pattern.get('avg_gain', 0.0),
            'source': 'cluster',
            'quality_score': quality['quality_score'],
            'quality_approved': quality['approved'],
        }

        if quality['approved']:
            recommended_rules.append(rule_entry)

        quality_scores.append({
            'rule_id': rule_id,
            'source': 'cluster',
            'pattern_label': pattern.get('label'),
            **quality,
        })

    report = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'lookback_hours': lookback_hours,
        'data_summary': {
            'total_entries': len(df),
            'n_pumps': n_pumps,
            'n_independent_pumps': (dedup_stats or {}).get('independent_pumps', n_pumps),
            'non_pumps': n_non_pump,
            'pump_rate_pct': round(n_pumps / max(len(df), 1) * 100, 2),
        },
        'deduplication_stats': dedup_stats or {},
        'excluded_dip_proxies': {
            'features': sorted(DIP_PROXY_FEATURES),
            'reason': 'These features are proxies for "price just dropped" and would '
                      'cause the system to label dip bounces as pump signals.',
        },
        'feature_rankings': feature_rankings,
        'single_feature_thresholds': thresholds,
        'best_combinations': best_combinations[:30],
        'approved_patterns': approved_patterns,
        'stress_test_results': {
            'combination_results': combo_stress_results or [],
            'pattern_results': pattern_stress_results or [],
        },
        'quality_scores': quality_scores,
        'recommended_rules': recommended_rules,
    }

    n_approved = len(recommended_rules)
    n_total_candidates = len(quality_scores)
    logger.info(f"  Quality gating: {n_approved} approved of {n_total_candidates} candidates "
                f"(min_score={MIN_QUALITY_SCORE}, min_prec={MIN_QUALITY_PRECISION})")
    if n_approved == 0:
        logger.warning("  NO rules passed quality gating. The current feature set may not "
                       "contain a reliable pump signal. System will fire zero signals.")

    return report


def write_report(report: Dict[str, Any]) -> Path:
    """Write the fingerprint report to JSON on disk."""
    _CACHE_DIR.mkdir(exist_ok=True)

    with open(_REPORT_PATH, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Report written to {_REPORT_PATH}")
    return _REPORT_PATH


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_fingerprint_analysis(
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
) -> Optional[Dict[str, Any]]:
    """Run the full fingerprint analysis pipeline.

    1. Load trail data with context-filtered multi-tier labeling
    2. Deduplicate pump signals (10-min window) + downsample non-pumps
    3. Rank features by pump/non-pump separation (flag dip proxies)
    4. Discover optimal thresholds per rule-eligible feature
    5. Find best 2- and 3-feature combinations (OB/TX/whale primary)
    6. Cluster pumps into repeatable patterns
    7. Stress-test rules against worst days
    8. Quality-score and gate rules
    9. Build and write the report

    Returns the report dict, or None on failure.
    """
    logger.info(f"=== Fingerprint Analysis (lookback={lookback_hours}h) ===")
    t0 = time.time()

    # 1A: Load data (context-filtered labels exclude dip bounces)
    df = load_trail_data(lookback_hours)
    if df is None or len(df) < 50:
        logger.error(f"Insufficient data: {len(df) if df is not None else 0} rows")
        return None

    n_pumps = int(df['is_pump'].sum())
    if n_pumps < 5:
        logger.error(f"Only {n_pumps} pumps found — need at least 5")
        return None

    # Compute actual data span (may be less than requested lookback)
    actual_span_hours = lookback_hours
    if 'followed_at' in df.columns:
        ts = pd.to_datetime(df['followed_at'])
        span = (ts.max() - ts.min()).total_seconds() / 3600.0
        if span > 0:
            actual_span_hours = span
    logger.info(f"  Actual data span: {actual_span_hours:.1f}h "
                f"({actual_span_hours / 24:.1f} days)")

    # 1A+: Deduplicate signals and downsample non-pumps
    df, dedup_stats = deduplicate_signals(df)

    n_pumps = int(df['is_pump'].sum())
    if n_pumps < 5:
        logger.error(f"Only {n_pumps} independent pumps after dedup — need at least 5")
        return None

    # 1B: Feature separation (computes for all, flags dip proxies)
    feature_rankings = rank_features(df)
    if not feature_rankings:
        logger.error("No features could be ranked")
        return None

    # 1C: Threshold discovery (rule-eligible features only)
    thresholds = discover_thresholds(df, feature_rankings, top_n=15)

    # 1D: Combination discovery (OB/TX/whale primary, momentum secondary)
    best_combinations = find_best_combinations(df, thresholds, top_n=10)

    # 1E: Pattern clustering (rule-eligible features only)
    approved_patterns = cluster_pump_patterns(df, feature_rankings)

    # 1F: Stress test against worst days
    combo_stress_results = []
    pattern_stress_results = []

    if best_combinations:
        best_combinations, combo_stress_results = stress_test_rules(
            df, best_combinations, thresholds, source_type='combination',
        )

    if approved_patterns:
        approved_patterns, pattern_stress_results = stress_test_rules(
            df, approved_patterns, thresholds, source_type='cluster',
        )

    # 1G+1H: Quality-score, gate, build and write report
    # Use actual_span_hours for signal rate calculations (may be < lookback_hours
    # if the database doesn't have the full requested window)
    report = build_report(
        df, feature_rankings, thresholds, best_combinations,
        approved_patterns, actual_span_hours,
        dedup_stats=dedup_stats,
        combo_stress_results=combo_stress_results,
        pattern_stress_results=pattern_stress_results,
    )
    write_report(report)

    elapsed = time.time() - t0
    n_rules = len(report.get('recommended_rules', []))
    logger.info(f"=== Fingerprint complete: {n_rules} approved rules, "
                f"{len(approved_patterns)} patterns, "
                f"{len(best_combinations)} combos ({elapsed:.1f}s) ===")
    if n_rules == 0:
        logger.warning("=== No rules approved — system will fire zero signals ===")

    return report


# =============================================================================
# CLI
# =============================================================================

def main():
    # Ensure project root and trading dir are on sys.path for standalone CLI usage
    import sys as _sys
    _project_root = str(_PROJECT_ROOT)
    _trading_dir = str(_PROJECT_ROOT / "000trading")
    if _project_root not in _sys.path:
        _sys.path.insert(0, _project_root)
    if _trading_dir not in _sys.path:
        _sys.path.insert(0, _trading_dir)

    parser = argparse.ArgumentParser(description="Pump Fingerprint Analysis")
    parser.add_argument(
        '--lookback', type=int, default=DEFAULT_LOOKBACK_HOURS,
        help=f"Lookback window in hours (default: {DEFAULT_LOOKBACK_HOURS})"
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help="Enable debug logging"
    )
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    report = run_fingerprint_analysis(lookback_hours=args.lookback)
    if report is None:
        print("Fingerprint analysis FAILED — see logs above")
        sys.exit(1)

    # Print summary
    summary = report['data_summary']
    print(f"\n{'='*60}")
    print(f"FINGERPRINT ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"Data: {summary['total_entries']:,} entries over {args.lookback}h")
    print(f"Pumps: {summary['n_pumps']} ({summary['pump_rate_pct']:.1f}%)")
    print(f"Non-pumps: {summary['non_pumps']:,}")

    # Deduplication stats
    dedup = report.get('deduplication_stats', {})
    if dedup:
        print(f"\nDeduplication ({dedup.get('window_minutes', '?')}min window):")
        print(f"  Raw pumps: {dedup.get('raw_pumps', '?')} -> "
              f"Independent: {dedup.get('independent_pumps', '?')} "
              f"({dedup.get('duplicates_removed', '?')} duplicates removed)")
        print(f"  Non-pumps: {dedup.get('raw_non_pumps', '?')} -> "
              f"Sampled: {dedup.get('sampled_non_pumps', '?')}")

    print(f"\nTop 10 features by separation:")
    for i, r in enumerate(report['feature_rankings'][:10]):
        elig = " " if r.get('rule_eligible', True) else "X"
        print(f"  {i+1:2d}. [{elig}] {r['feature']:40s}  sep={r['separation']:+.4f}")

    n_combos = len(report.get('best_combinations', []))
    n_patterns = len(report.get('approved_patterns', []))
    n_rules = len(report.get('recommended_rules', []))
    n_candidates = len(report.get('quality_scores', []))
    print(f"\nDiscovered: {n_combos} combos, {n_patterns} patterns "
          f"(stress-tested)")
    print(f"Quality-approved rules: {n_rules} of {n_candidates} candidates")

    if n_rules == 0:
        print("\n*** NO RULES APPROVED — system will fire zero signals ***")
        print("The current feature set may not contain a reliable pump signal.")

    if report.get('best_combinations'):
        print(f"\nTop 5 combinations (stress-test passed):")
        for i, c in enumerate(report['best_combinations'][:5]):
            feats = ' + '.join(c['features'])
            print(f"  {i+1}. [{feats}]  prec={c['precision']:.1%}  "
                  f"n={c['n_signals']}  avg_gain={c['avg_gain_on_hit']:.3f}%")

    if report.get('approved_patterns'):
        print(f"\nApproved patterns (stress-test passed):")
        for p in report['approved_patterns']:
            print(f"  - {p['label']}  prec={p['precision']:.1%}  "
                  f"n={p['n_occurrences']}  avg_gain={p['avg_gain']:.3f}%")

    # Stress test summary
    stress = report.get('stress_test_results', {})
    combo_stress = stress.get('combination_results', [])
    pattern_stress = stress.get('pattern_results', [])
    if combo_stress or pattern_stress:
        n_combo_pass = sum(1 for s in combo_stress if s.get('passed'))
        n_pattern_pass = sum(1 for s in pattern_stress if s.get('passed'))
        print(f"\nStress test: {n_combo_pass}/{len(combo_stress)} combos passed, "
              f"{n_pattern_pass}/{len(pattern_stress)} patterns passed")

    print(f"\nReport: {_REPORT_PATH}")


if __name__ == '__main__':
    main()

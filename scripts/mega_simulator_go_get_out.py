"""
Exit Strategy Optimizer  (mega_simulator_go_get_out.py)
=======================================================
Analyzes ALL historical buy-in positions to find the optimal exit strategy.

Two key questions answered:
  1. FIXED TOLERANCES — what stop-loss / trailing-stop settings capture more of
     each trade's peak gain?
  2. SMART SELL — does a "consecutive-drop gate" (require N successive drops
     before selling) reduce premature exits caused by single-tick noise?

The consec_drops parameter is the core new idea:
  Instead of selling the MOMENT tolerance is breached, require the breach to
  persist for N consecutive price checks.  This filters micro-volatility that
  causes the current system to exit too early.

KEY DESIGN DECISION — Forward price paths:
  The simulation uses the FULL FORWARD price path from the `prices` table
  (SOL per-second prices) starting at entry and covering FORWARD_MINUTES.
  This is critical: using only price_checks (which end at the actual exit)
  would prevent the GA from discovering that wider tolerances let you hold
  through dips for bigger gains.  The full path shows what COULD have happened.

  price_checks data is still used for the diagnostics section only
  (showing what actually happened in live trading).

Usage:
    python3 scripts/mega_simulator_go_get_out.py
    python3 scripts/mega_simulator_go_get_out.py --days 14
    python3 scripts/mega_simulator_go_get_out.py --plays 3,4
    python3 scripts/mega_simulator_go_get_out.py --quick        # 50 pop / 50 gen
    python3 scripts/mega_simulator_go_get_out.py --apply        # write best to DB
"""

from __future__ import annotations

import argparse
import bisect
import json
import logging
import random
import sys
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("exit_optimizer")

# ── constants ─────────────────────────────────────────────────────────────────
COST_PCT          = 0.001          # 0.1% round-trip trading cost
DEFAULT_PLAY_IDS  = [3, 4, 5, 6]
OOS_FOLDS         = 4
MIN_TRADES_FOR_GA = 20
FORWARD_MINUTES   = 15             # forward price window for simulation

# GA parameters — sized so full run completes in ~15 min on this hardware
# (each evaluation runs simulate_exit on ~700 trades × ~300 price points)
GA_POPULATION     = 150
GA_GENERATIONS    = 60
GA_ELITE_FRAC     = 0.10
GA_CROSSOVER      = 0.60
GA_MUTATION       = 0.25
GA_TOURNAMENT_K   = 5

# Quick mode: smoke-test only — completes in ~30-60 s
QUICK_POPULATION  = 10
QUICK_GENERATIONS = 10

# ── GA parameter space ────────────────────────────────────────────────────────
# 9 parameters encoded as floats; integers are rounded during decode.
PARAM_NAMES = [
    'stop_loss',       # drop-from-entry threshold before stop-loss fires
    't1_tol',          # trailing tolerance while highest_gain < t1_bnd
    't1_bnd',          # gain boundary between tier 1 and tier 2
    't2_tol',          # trailing tolerance while t1_bnd <= highest_gain < t2_bnd
    't2_bnd',          # gain boundary between tier 2 and tier 3
    't3_tol',          # trailing tolerance once highest_gain >= t2_bnd
    'min_hold_steps',  # suppress any sells for first N price checks (integer)
    'consec_drops',    # require N consecutive violations before triggering sell (integer)
    'grace_steps',     # widen tolerance for N steps after each new high (integer)
]
PARAM_BOUNDS: List[Tuple[float, float]] = [
    (0.0003, 0.005),    # stop_loss
    (0.0003, 0.006),    # t1_tol
    (0.001,  0.015),    # t1_bnd
    (0.0001, 0.004),    # t2_tol
    (0.002,  0.030),    # t2_bnd
    (0.00005, 0.002),   # t3_tol
    (0.0,    10.0),     # min_hold_steps
    (1.0,     5.0),     # consec_drops
    (0.0,     5.0),     # grace_steps
]


# =============================================================================
# DATA LOADING
# =============================================================================

def load_trades(play_ids: List[int], days: int) -> List[Dict[str, Any]]:
    """Load all closed trades with metadata."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    b.id,
                    b.play_id,
                    b.followed_at,
                    b.our_exit_timestamp,
                    CAST(b.our_entry_price  AS FLOAT) AS entry,
                    CAST(b.our_exit_price   AS FLOAT) AS exit_p,
                    CAST(b.higest_price_reached AS FLOAT) AS peak,
                    b.our_status,
                    EXTRACT(EPOCH FROM (b.our_exit_timestamp - b.followed_at)) AS hold_seconds,
                    ROUND(((b.higest_price_reached - b.our_entry_price)
                           / NULLIF(b.our_entry_price, 0) * 100)::numeric, 5) AS max_gain_pct,
                    ROUND(((b.our_exit_price - b.our_entry_price)
                           / NULLIF(b.our_entry_price, 0) * 100)::numeric, 5) AS exit_pct,
                    b.fifteen_min_trail
                FROM follow_the_goat_buyins b
                WHERE b.play_id = ANY(%s)
                  AND b.followed_at > NOW() - (%s || ' days')::INTERVAL
                  AND b.our_entry_price IS NOT NULL
                  AND b.our_exit_price  IS NOT NULL
                  AND b.our_status = 'sold'
                  AND b.wallet_address NOT LIKE 'TRAINING_TEST_%%'
                ORDER BY b.followed_at ASC
            """, [play_ids, str(days)])
            return [dict(r) for r in cur.fetchall()]


def load_price_series_batch(buyin_ids: List[int]) -> Dict[int, List[float]]:
    """
    Batch-load ACTUAL price_checks series for diagnostics.
    Returns  buyin_id -> [price, ...]  ordered by checked_at.
    Used only for reporting what actually happened (not for GA simulation).
    """
    if not buyin_ids:
        return {}

    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT buyin_id, CAST(current_price AS FLOAT) AS price
                FROM follow_the_goat_buyins_price_checks
                WHERE buyin_id = ANY(%s)
                  AND is_backfill = FALSE
                ORDER BY buyin_id, checked_at ASC
            """, [buyin_ids])

            series: Dict[int, List[float]] = defaultdict(list)
            for row in cur.fetchall():
                series[int(row['buyin_id'])].append(float(row['price']))

    return dict(series)


def load_forward_price_index(
    trades: List[Dict[str, Any]],
    forward_minutes: int = FORWARD_MINUTES,
) -> Tuple[List[datetime], List[float]]:
    """
    Load all SOL prices covering every trade's entry + forward window in ONE query.

    Returns (timestamps_list, prices_list) as parallel sorted arrays so that
    per-trade slices can be extracted with a binary search in O(log n).
    """
    if not trades:
        return [], []

    # Compute the overall window to fetch
    entry_times = [t['followed_at'] for t in trades if t.get('followed_at')]
    if not entry_times:
        return [], []

    window_start = min(entry_times) - timedelta(seconds=2)
    window_end   = max(entry_times) + timedelta(minutes=forward_minutes) + timedelta(seconds=2)

    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, CAST(price AS FLOAT) AS price
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY timestamp ASC
            """, [window_start, window_end])
            rows = cur.fetchall()

    if not rows:
        return [], []

    timestamps = [row['timestamp'] for row in rows]
    prices     = [float(row['price']) for row in rows]
    return timestamps, prices


def extract_forward_series(
    entry_time: datetime,
    timestamps: List[datetime],
    prices: List[float],
    forward_minutes: int = FORWARD_MINUTES,
) -> List[float]:
    """
    Slice the global price index to get prices from entry_time for forward_minutes.
    Uses binary search for O(log n) lookup.
    """
    if not timestamps:
        return []

    end_time = entry_time + timedelta(minutes=forward_minutes)

    # Binary search for start and end positions
    start_idx = bisect.bisect_left(timestamps, entry_time)
    end_idx   = bisect.bisect_right(timestamps, end_time)

    return prices[start_idx:end_idx]


def precompute_forward_series(
    trades: List[Dict[str, Any]],
    fwd_timestamps: List[datetime],
    fwd_prices: List[float],
) -> Dict[int, List[float]]:
    """
    Pre-compute and cache the forward price series for every trade.
    This is called ONCE before the GA so that evaluation is a simple dict lookup
    instead of calling extract_forward_series 1M+ times.
    Returns  buyin_id -> List[float]
    """
    cache: Dict[int, List[float]] = {}
    for t in trades:
        bid        = int(t['id'])
        entry_time = t.get('followed_at')
        entry      = float(t.get('entry') or 1.0)
        peak       = float(t.get('peak')  or entry)
        exit_p     = float(t.get('exit_p') or entry)
        if entry_time and fwd_timestamps:
            series = extract_forward_series(entry_time, fwd_timestamps, fwd_prices)
            cache[bid] = series if len(series) >= 3 else [entry, peak, exit_p]
        else:
            cache[bid] = [entry, peak, exit_p]
    return cache


def load_play_configs(play_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Load current sell_logic for each play."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, sell_logic
                FROM follow_the_goat_plays
                WHERE id = ANY(%s)
                ORDER BY id
            """, [play_ids])
            configs: Dict[int, Dict[str, Any]] = {}
            for row in cur.fetchall():
                raw = row['sell_logic']
                sell = json.loads(raw) if isinstance(raw, str) else raw or {}
                configs[row['id']] = {'name': row['name'], 'sell_logic': sell}
    return configs


def get_forward_series(
    trade: Dict[str, Any],
    fwd_cache: Dict[int, List[float]],
) -> List[float]:
    """
    Look up the pre-computed forward price series for a trade (O(1) dict lookup).
    Falls back to synthetic [entry, peak, exit] if not in cache.
    """
    bid = int(trade['id'])
    if bid in fwd_cache:
        return fwd_cache[bid]
    entry  = float(trade.get('entry')  or 1.0)
    peak   = float(trade.get('peak')   or entry)
    exit_p = float(trade.get('exit_p') or entry)
    return [entry, peak, exit_p]


def get_actual_series(
    trade: Dict[str, Any],
    price_checks: Dict[int, List[float]],
) -> List[float]:
    """
    Get the ACTUAL observed price path (used for diagnostics only).
    Priority: price_checks -> fifteen_min_trail -> synthetic [entry, peak, exit].
    """
    buyin_id = int(trade['id'])

    if buyin_id in price_checks and len(price_checks[buyin_id]) >= 3:
        return price_checks[buyin_id]

    trail = trade.get('fifteen_min_trail')
    if trail:
        try:
            data = trail if isinstance(trail, list) else json.loads(trail)
            prices = [float(p['price']) for p in data if 'price' in p]
            if len(prices) >= 3:
                return prices
        except Exception:
            pass

    entry  = float(trade.get('entry')  or 1.0)
    peak   = float(trade.get('peak')   or entry)
    exit_p = float(trade.get('exit_p') or entry)
    return [entry, peak, exit_p]


# =============================================================================
# EXIT SIMULATION ENGINE
# =============================================================================

def simulate_exit(
    entry: float,
    price_series: List[float],
    stop_loss: float,
    t1_tol: float,
    t1_bnd: float,
    t2_tol: float,
    t2_bnd: float,
    t3_tol: float,
    min_hold_steps: int = 0,
    consec_drops: int = 1,
    grace_steps: int = 0,
) -> Tuple[float, str]:
    """
    Replay a price series against a tiered trailing stop.

    Matches the sell_trailing_stop.py dual-check semantics exactly, plus two
    new optional enhancements:

      consec_drops  — require the sell condition to be true for N consecutive
                      checks before triggering.  consec_drops=1 is the current
                      live behaviour (fire immediately on first breach).

      grace_steps   — after each new high, double the trailing tolerance for
                      the next N steps.  Prevents the stop from firing on the
                      immediate tick-back right after a peak.

    Tolerance locking: once a tighter tier is reached it never loosens
    (identical to sell_trailing_stop.py).

    Returns (exit_pct_gain_after_entry, reason).
    reason is one of: 'stop_loss', 'trailing', 'timeout', 'no_data'.
    """
    if not entry or entry <= 0 or not price_series:
        return 0.0, "no_data"

    highest             = entry
    locked_tol          = 1.0   # starts very loose; tightens as price climbs
    sl_consecutive      = 0     # consecutive stop-loss violations
    trail_consecutive   = 0     # consecutive trailing-stop violations
    steps_since_new_high = 999  # grace period counter

    for step, price in enumerate(price_series):

        # ── Track new high ────────────────────────────────────────────────────
        if price > highest:
            highest              = price
            steps_since_new_high = 0
            sl_consecutive       = 0
            trail_consecutive    = 0
        else:
            steps_since_new_high += 1

        highest_gain = (highest - entry) / entry if entry > 0 else 0.0

        # ── Select tier tolerance (based on HIGHEST gain, not current) ────────
        if highest_gain < t1_bnd:
            trail_tol = t1_tol
        elif highest_gain < t2_bnd:
            trail_tol = t2_tol
        else:
            trail_tol = t3_tol

        # ── Tolerance ratchet — only tightens ─────────────────────────────────
        if trail_tol < locked_tol:
            locked_tol = trail_tol

        # ── Grace period: loosen tolerance just after a new high ──────────────
        effective_tol = locked_tol
        if grace_steps > 0 and steps_since_new_high < grace_steps:
            effective_tol = locked_tol * 2.0

        # ── Min-hold guard ────────────────────────────────────────────────────
        if step < min_hold_steps:
            continue

        # ── Stop-loss check (drop from ENTRY) ─────────────────────────────────
        drop_from_entry = (price - entry) / entry
        if drop_from_entry < -stop_loss:
            sl_consecutive += 1
            if sl_consecutive >= consec_drops:
                return drop_from_entry * 100, "stop_loss"
        else:
            sl_consecutive = 0

        # ── Trailing-stop check (drop from HIGHEST, only after any gain) ──────
        if highest_gain > 0:
            drop_from_high = (price - highest) / highest
            if drop_from_high < -effective_tol:
                trail_consecutive += 1
                if trail_consecutive >= consec_drops:
                    return (price - entry) / entry * 100, "trailing"
            else:
                trail_consecutive = 0

    # Held to end of price series — exit at last price
    final = price_series[-1]
    return (final - entry) / entry * 100, "timeout"


def _extract_current_config_params(sell_logic: Dict[str, Any]) -> Tuple:
    """Extract (stop_loss, t1_tol, t1_bnd, t2_tol, t2_bnd, t3_tol, min_hold) from sell_logic."""
    tr  = sell_logic.get('tolerance_rules', {})
    dec = tr.get('decreases', [{'range': [-999999, 0], 'tolerance': 0.003}])
    inc = tr.get('increases', [
        {'range': [0.0, 0.005], 'tolerance': 0.003},
        {'range': [0.005, 1.0], 'tolerance': 0.001},
    ])

    stop_loss = float((dec[0] if dec else {}).get('tolerance', 0.003))

    tiers = sorted(
        [(float(r.get('range', [0, 0])[1]), float(r.get('tolerance', 0.001))) for r in inc],
        key=lambda x: x[0],
    )

    if len(tiers) >= 3:
        t1_bnd, t1_tol = tiers[0]
        t2_bnd, t2_tol = tiers[1]
        t3_tol          = tiers[2][1]
    elif len(tiers) == 2:
        t1_bnd, t1_tol = tiers[0]
        t2_bnd, t2_tol = tiers[1]
        t3_tol          = t2_tol * 0.5
    elif len(tiers) == 1:
        t1_bnd, t1_tol = tiers[0]
        t2_bnd          = max(t1_bnd * 2, 0.01)
        t2_tol          = t1_tol * 0.5
        t3_tol          = t1_tol * 0.25
    else:
        t1_bnd, t1_tol = 0.005, 0.003
        t2_bnd, t2_tol = 0.01,  0.001
        t3_tol          = 0.0005

    min_hold = int(sell_logic.get('min_hold_seconds', 0))
    return stop_loss, t1_tol, t1_bnd, t2_tol, t2_bnd, t3_tol, min_hold


def simulate_current_config(
    trade: Dict[str, Any],
    sell_logic: Dict[str, Any],
    fwd_cache: Dict[int, List[float]],
) -> float:
    """Simulate the current play sell_logic on the forward price path and return exit_pct."""
    sl, t1t, t1b, t2t, t2b, t3t, mh = _extract_current_config_params(sell_logic)
    series = get_forward_series(trade, fwd_cache)
    exit_pct, _ = simulate_exit(
        float(trade['entry']), series,
        sl, t1t, t1b, t2t, t2b, t3t,
        min_hold_steps=mh, consec_drops=1, grace_steps=0,
    )
    return exit_pct


# =============================================================================
# DIAGNOSTICS
# =============================================================================

def _percentile(vals: List[float], p: int) -> float:
    if not vals:
        return 0.0
    sv  = sorted(vals)
    idx = max(0, min(len(sv) - 1, int(len(sv) * p / 100)))
    return sv[idx]


def run_diagnostics(
    trades: List[Dict[str, Any]],
    price_checks: Dict[int, List[float]],
) -> Dict[str, Any]:
    """Compute early-exit diagnostics for a list of trades."""
    n = len(trades)
    if n == 0:
        return {'n': 0}

    exit_pcts = [float(t['exit_pct'])    for t in trades if t['exit_pct']    is not None]
    peak_pcts = [float(t['max_gain_pct']) for t in trades if t['max_gain_pct'] is not None]
    hold_secs = [float(t['hold_seconds']) for t in trades if t['hold_seconds'] is not None]

    winners = [e for e in exit_pcts if e > 0]
    losers  = [e for e in exit_pcts if e <= 0]

    gave_back = [
        t for t in trades
        if t['max_gain_pct'] and float(t['max_gain_pct']) > 0
        and t['exit_pct']    and float(t['exit_pct'])    < 0
    ]
    gb_peaks = [float(t['max_gain_pct']) for t in gave_back]
    gb_exits = [float(t['exit_pct'])     for t in gave_back]

    capture_rates = [
        float(t['exit_pct']) / float(t['max_gain_pct'])
        for t in trades
        if t['max_gain_pct'] and float(t['max_gain_pct']) > 0
        and t['exit_pct']    and float(t['exit_pct'])    > 0
    ]

    # Premature: exited at <50% of peak (only for trades with meaningful peak)
    premature = [
        t for t in trades
        if t['max_gain_pct'] and float(t['max_gain_pct']) > 0.1
        and t['exit_pct']    and float(t['exit_pct'])    < float(t['max_gain_pct']) * 0.5
    ]

    n_with_checks = sum(
        1 for t in trades
        if int(t['id']) in price_checks and len(price_checks[int(t['id'])]) >= 3
    )

    return {
        'n':                   n,
        'win_rate':            len(winners) / n,
        'avg_exit':            sum(exit_pcts)  / len(exit_pcts)  if exit_pcts  else 0.0,
        'avg_win':             sum(winners)    / len(winners)    if winners    else 0.0,
        'avg_loss':            sum(losers)     / len(losers)     if losers     else 0.0,
        'p25_exit':            _percentile(exit_pcts, 25),
        'p50_exit':            _percentile(exit_pcts, 50),
        'p75_exit':            _percentile(exit_pcts, 75),
        'p25_peak':            _percentile(peak_pcts, 25),
        'p50_peak':            _percentile(peak_pcts, 50),
        'p75_peak':            _percentile(peak_pcts, 75),
        'p90_peak':            _percentile(peak_pcts, 90),
        'avg_hold_s':          sum(hold_secs) / len(hold_secs) if hold_secs else 0.0,
        'gave_back_count':     len(gave_back),
        'gave_back_pct':       len(gave_back) / n,
        'avg_gb_peak':         sum(gb_peaks) / len(gb_peaks) if gb_peaks else 0.0,
        'avg_gb_exit':         sum(gb_exits) / len(gb_exits) if gb_exits else 0.0,
        'avg_peak_capture':    sum(capture_rates) / len(capture_rates) if capture_rates else 0.0,
        'premature_count':     len(premature),
        'premature_pct':       len(premature) / n,
        'n_with_price_checks': n_with_checks,
    }


# =============================================================================
# GENETIC ALGORITHM
# =============================================================================

def _random_individual() -> List[float]:
    return [random.uniform(lo, hi) for lo, hi in PARAM_BOUNDS]


def _clamp(ind: List[float]) -> List[float]:
    """Clamp to bounds and enforce t1_bnd < t2_bnd."""
    result = [max(lo, min(hi, v)) for v, (lo, hi) in zip(ind, PARAM_BOUNDS)]
    if result[2] >= result[4]:        # t1_bnd must be less than t2_bnd
        result[2] = result[4] * 0.5
    return result


def _decode(ind: List[float]) -> Dict[str, Any]:
    return {
        'stop_loss':      ind[0],
        't1_tol':         ind[1],
        't1_bnd':         ind[2],
        't2_tol':         ind[3],
        't2_bnd':         ind[4],
        't3_tol':         ind[5],
        'min_hold_steps': int(round(ind[6])),
        'consec_drops':   max(1, int(round(ind[7]))),
        'grace_steps':    max(0, int(round(ind[8]))),
    }


def _evaluate(
    ind: List[float],
    trades: List[Dict[str, Any]],
    fwd_cache: Dict[int, List[float]],
) -> Tuple[float, Dict[str, Any]]:
    """Evaluate one GA individual on full forward price paths. Returns (fitness, stats)."""
    p = _decode(ind)
    exits: List[float] = []

    for t in trades:
        entry = float(t['entry'])
        if entry <= 0:
            continue
        series = get_forward_series(t, fwd_cache)
        ep, _ = simulate_exit(
            entry, series,
            p['stop_loss'], p['t1_tol'], p['t1_bnd'],
            p['t2_tol'],    p['t2_bnd'], p['t3_tol'],
            p['min_hold_steps'], p['consec_drops'], p['grace_steps'],
        )
        exits.append(ep)

    if not exits:
        return -999.0, {}

    net = [e - COST_PCT * 100 for e in exits]
    winners  = [e for e in net if e > 0]
    win_rate = len(winners) / len(net)
    avg_exit = sum(net) / len(net)

    # Fitness: maximise avg_exit with a hard floor on win_rate.
    # IMPORTANT: when avg_exit is negative, multiplying by a fraction < 1 makes it
    # LESS negative (appears better) — that would reward low-win-rate configs unfairly.
    # Instead, we add a flat penalty for unacceptable win rates.
    if win_rate < 0.25:
        fitness = avg_exit - 1.0   # hard penalty: always worse than a reasonable config
    elif win_rate < 0.35:
        fitness = avg_exit - 0.10  # moderate penalty
    else:
        fitness = avg_exit         # pure objective: maximise average exit

    stats = {
        'win_rate': win_rate,
        'avg_exit': avg_exit,
        'p50_exit': _percentile(net, 50),
        'p75_exit': _percentile(net, 75),
        'n':        len(exits),
    }
    return fitness, stats


def _tournament(scored: List[Tuple[float, List[float]]], k: int = 5) -> List[float]:
    pool = random.sample(scored, min(k, len(scored)))
    return deepcopy(max(pool, key=lambda x: x[0])[1])


def _crossover(p1: List[float], p2: List[float]) -> List[float]:
    return [p1[i] if random.random() < 0.5 else p2[i] for i in range(len(p1))]


def _mutate(ind: List[float], rate: float = 0.25) -> List[float]:
    result = deepcopy(ind)
    for i, (lo, hi) in enumerate(PARAM_BOUNDS):
        if random.random() < rate:
            result[i] += random.gauss(0, (hi - lo) * 0.15)
    return _clamp(result)


def run_ga(
    trades: List[Dict[str, Any]],
    fwd_cache: Dict[int, List[float]],
    population_size: int = GA_POPULATION,
    generations: int = GA_GENERATIONS,
    verbose: bool = True,
) -> List[Tuple[float, List[float], Dict[str, Any]]]:
    """
    Run GA to find optimal exit parameters using pre-computed forward price paths.
    Returns top 20 results as (fitness, individual, stats) sorted best-first.
    """
    if len(trades) < MIN_TRADES_FOR_GA:
        logger.warning(f"Only {len(trades)} trades — GA results may not generalise well")

    # Initialise
    population: List[Tuple[float, List[float], Dict[str, Any]]] = []
    for _ in range(population_size):
        ind = _random_individual()
        f, s = _evaluate(ind, trades, fwd_cache)
        population.append((f, ind, s))
    population.sort(key=lambda x: -x[0])

    elite_n = max(1, int(population_size * GA_ELITE_FRAC))

    for gen in range(generations):
        new_pop: List[Tuple[float, List[float], Dict[str, Any]]] = []

        # Carry elites forward
        new_pop.extend((f, deepcopy(ind), s) for f, ind, s in population[:elite_n])

        scored = [(f, ind) for f, ind, _ in population]

        while len(new_pop) < population_size:
            if random.random() < GA_CROSSOVER:
                child = _crossover(_tournament(scored, GA_TOURNAMENT_K),
                                   _tournament(scored, GA_TOURNAMENT_K))
            else:
                child = deepcopy(_tournament(scored, GA_TOURNAMENT_K))
            child = _mutate(child, GA_MUTATION)
            f, s  = _evaluate(child, trades, fwd_cache)
            new_pop.append((f, child, s))

        population = sorted(new_pop, key=lambda x: -x[0])

        if verbose and (gen + 1) % 25 == 0:
            bf, _, bs = population[0]
            logger.info(
                f"  Gen {gen+1:3d}/{generations}: fitness={bf:.5f}  "
                f"avg_exit={bs.get('avg_exit', 0):+.4f}%  "
                f"win_rate={bs.get('win_rate', 0)*100:.1f}%"
            )

    return [(f, ind, s) for f, ind, s in population[:20]]


# =============================================================================
# WALK-FORWARD OOS VALIDATION
# =============================================================================

def validate_oos(
    top_results: List[Tuple[float, List[float], Dict[str, Any]]],
    trades: List[Dict[str, Any]],
    fwd_cache: Dict[int, List[float]],
    n_folds: int = OOS_FOLDS,
) -> List[Dict[str, Any]]:
    """
    Validate top GA individuals against chronological OOS folds.
    A config is marked 'robust' if at least (n_folds - 1) folds show positive avg.
    """
    sorted_trades = sorted(trades, key=lambda t: t['followed_at'] or datetime.min)
    n         = len(sorted_trades)
    fold_size = n // n_folds

    results: List[Dict[str, Any]] = []

    for _, ind, is_stats in top_results:
        p = _decode(ind)

        oos_exits_per_fold: List[List[float]] = []

        for fold in range(n_folds):
            start = fold * fold_size
            end   = (fold + 1) * fold_size if fold < n_folds - 1 else n
            oos_trades = sorted_trades[start:end]
            if len(oos_trades) < 3:
                continue

            fold_exits: List[float] = []
            for t in oos_trades:
                entry = float(t['entry'])
                if entry <= 0:
                    continue
                series = get_forward_series(t, fwd_cache)
                ep, _ = simulate_exit(
                    entry, series,
                    p['stop_loss'], p['t1_tol'], p['t1_bnd'],
                    p['t2_tol'],    p['t2_bnd'], p['t3_tol'],
                    p['min_hold_steps'], p['consec_drops'], p['grace_steps'],
                )
                fold_exits.append(ep - COST_PCT * 100)
            oos_exits_per_fold.append(fold_exits)

        all_oos  = [e for fold in oos_exits_per_fold for e in fold]
        oos_avg  = sum(all_oos) / len(all_oos) if all_oos else 0.0
        oos_wr   = sum(1 for e in all_oos if e > 0) / len(all_oos) if all_oos else 0.0
        pos_folds = sum(1 for fold in oos_exits_per_fold if fold and sum(fold)/len(fold) > 0)
        is_robust = pos_folds >= max(1, n_folds - 1)

        results.append({
            'params':          p,
            'individual':      ind,
            'is_stats':        is_stats,
            'oos_avg_exit':    oos_avg,
            'oos_win_rate':    oos_wr,
            'oos_pos_folds':   pos_folds,
            'oos_n_folds':     n_folds,
            'is_robust':       is_robust,
        })

    results.sort(key=lambda x: -x['oos_avg_exit'])
    return results


# =============================================================================
# OUTPUT / DISPLAY
# =============================================================================

def _banner(text: str) -> None:
    sep = "=" * 84
    print(f"\n{sep}")
    print(f"  {text}")
    print(sep)


def print_diagnostics(diag: Dict[str, Any]) -> None:
    n = diag.get('n', 0)
    if n == 0:
        print("  No trades to analyse.")
        return

    print(f"  Trades analysed:        {n}  (using price_checks: {diag['n_with_price_checks']})")
    print(f"  Win rate:               {diag['win_rate']*100:.1f}%")
    print(f"  Avg exit:               {diag['avg_exit']:+.4f}%")
    print(f"  Avg win / Avg loss:     {diag['avg_win']:+.4f}% / {diag['avg_loss']:+.4f}%")
    print(f"  Exit p25/p50/p75:       {diag['p25_exit']:+.3f}% / {diag['p50_exit']:+.3f}% / {diag['p75_exit']:+.3f}%")
    print(f"  Peak p25/p50/p75/p90:   {diag['p25_peak']:+.3f}% / {diag['p50_peak']:+.3f}% / {diag['p75_peak']:+.3f}% / {diag['p90_peak']:+.3f}%")
    print(f"  Avg hold time:          {diag['avg_hold_s']:.0f}s")

    gb  = diag['gave_back_pct'] * 100
    sev = "CRITICAL" if gb > 50 else "HIGH" if gb > 35 else "OK"
    print(f"\n  GAIN GIVEBACKS [{sev}]")
    print(f"  Gave-back trades:       {diag['gave_back_count']:3d} / {n}  ({gb:.0f}%)")
    print(f"  Avg peak before flip:   {diag['avg_gb_peak']:+.4f}%")
    print(f"  Avg exit after flip:    {diag['avg_gb_exit']:+.4f}%")

    pre = diag['premature_pct'] * 100
    print(f"\n  PREMATURE EXITS (exited at <50% of peak gain, peak >0.1%)")
    print(f"  Premature trades:       {diag['premature_count']:3d} / {n}  ({pre:.0f}%)")
    print(f"  Avg peak-capture (wins):{diag['avg_peak_capture']*100:.1f}%")


def print_results_table(validated: List[Dict[str, Any]], baseline_avg: float, n_show: int = 15) -> None:
    hdr = (
        f"  {'#':<4} {'SL%':>6} {'T1tol':>7} {'T1bnd':>7} "
        f"{'T2tol':>7} {'T2bnd':>7} {'T3tol':>8} "
        f"{'Con':>4} {'Grc':>4} {'Hold':>5} "
        f"{'IS_avg':>8} {'OOS_avg':>9} {'OOS_wr':>7} {'Robust':>7}"
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for i, r in enumerate(validated[:n_show]):
        p       = r['params']
        is_avg  = r['is_stats'].get('avg_exit', 0.0)
        oos_avg = r['oos_avg_exit']
        oos_wr  = r['oos_win_rate']
        robust  = 'YES' if r['is_robust'] else 'no'
        marker  = " <-- BEST" if i == 0 and r['is_robust'] else (" <-- top" if i == 0 else "")

        print(
            f"  {i+1:<4} {p['stop_loss']*100:>5.3f}% {p['t1_tol']*100:>6.3f}% "
            f"{p['t1_bnd']*100:>6.3f}% {p['t2_tol']*100:>6.3f}% "
            f"{p['t2_bnd']*100:>6.3f}% {p['t3_tol']*100:>7.4f}% "
            f"{p['consec_drops']:>4d} {p['grace_steps']:>4d} {p['min_hold_steps']:>5d} "
            f"{is_avg:>+7.4f}% {oos_avg:>+8.4f}% {oos_wr*100:>6.1f}% {robust:>7}{marker}"
        )


# =============================================================================
# APPLY TO DATABASE
# =============================================================================

def build_sell_logic(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert GA params to the sell_logic JSON format used by sell_trailing_stop.py."""
    return {
        "tolerance_rules": {
            "decreases": [
                {"range": [-999999, 0], "tolerance": round(params['stop_loss'], 6)}
            ],
            "increases": [
                {
                    "range": [0.0, round(params['t1_bnd'], 5)],
                    "tolerance": round(params['t1_tol'], 6),
                },
                {
                    "range": [round(params['t1_bnd'], 5), round(params['t2_bnd'], 5)],
                    "tolerance": round(params['t2_tol'], 6),
                },
                {
                    "range": [round(params['t2_bnd'], 5), 1.0],
                    "tolerance": round(params['t3_tol'], 6),
                },
            ],
        },
        "min_hold_seconds": params['min_hold_steps'],
    }


def apply_to_plays(play_ids: List[int], sell_logic: Dict[str, Any]) -> None:
    payload = json.dumps(sell_logic)
    for play_id in play_ids:
        try:
            postgres_execute(
                "UPDATE follow_the_goat_plays SET sell_logic = %s WHERE id = %s",
                [payload, play_id],
            )
            print(f"  Play {play_id}: updated")
        except Exception as exc:
            print(f"  Play {play_id}: ERROR — {exc}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exit Strategy Optimizer — find best trailing-stop settings from historical data"
    )
    parser.add_argument("--days",  type=int, default=30,
                        help="Lookback window in days (default: 30)")
    parser.add_argument("--plays", type=str, default=None,
                        help="Comma-separated play IDs (default: 3,4,5,6)")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode: 50 pop / 50 gen for fast testing")
    parser.add_argument("--apply", action="store_true",
                        help="Apply best config to follow_the_goat_plays in DB")
    args = parser.parse_args()

    play_ids = DEFAULT_PLAY_IDS
    if args.plays:
        play_ids = [int(x.strip()) for x in args.plays.split(",") if x.strip()]

    pop_size = QUICK_POPULATION  if args.quick else GA_POPULATION
    n_gens   = QUICK_GENERATIONS if args.quick else GA_GENERATIONS

    _banner(
        f"EXIT STRATEGY OPTIMIZER  —  last {args.days} days  —  plays {play_ids}"
        + ("  [QUICK MODE]" if args.quick else "")
    )

    # ── Load data ──────────────────────────────────────────────────────────────
    print("\nLoading trades from PostgreSQL...")
    t0     = time.time()
    trades  = load_trades(play_ids, args.days)
    configs = load_play_configs(play_ids)
    print(f"Loaded {len(trades)} closed trades  ({time.time()-t0:.1f}s)")

    if not trades:
        print("No trades found in the specified window. Exiting.")
        return

    # Load actual price_checks (diagnostics only — truncated at real exit)
    print("Loading actual price_checks (for diagnostics)...")
    t0              = time.time()
    buyin_ids       = [int(t['id']) for t in trades]
    price_checks_map = load_price_series_batch(buyin_ids)
    n_good           = sum(
        1 for bid in buyin_ids if bid in price_checks_map and len(price_checks_map[bid]) >= 3
    )
    print(f"price_checks loaded: {n_good}/{len(trades)} with ≥3 records  ({time.time()-t0:.1f}s)")

    # Load full forward price paths from prices table (for GA simulation)
    print(f"Loading {FORWARD_MINUTES}-min forward price paths from prices table...")
    t0                         = time.time()
    fwd_timestamps, fwd_prices = load_forward_price_index(trades, FORWARD_MINUTES)
    print(f"  {len(fwd_prices)} price rows loaded for window  ({time.time()-t0:.1f}s)")

    print("Pre-computing per-trade forward series...")
    t0        = time.time()
    fwd_cache = precompute_forward_series(trades, fwd_timestamps, fwd_prices)
    n_fwd     = sum(1 for s in fwd_cache.values() if len(s) >= 3)
    print(f"  {n_fwd}/{len(trades)} trades with ≥3 forward price points  ({time.time()-t0:.1f}s)")

    # ── Diagnostics (uses actual price_checks — truncated at real exit) ─────────
    _banner("EARLY EXIT DIAGNOSTICS — ALL PLAYS")
    diag_all = run_diagnostics(trades, price_checks_map)
    print_diagnostics(diag_all)

    for play_id in play_ids:
        play_trades = [t for t in trades if t['play_id'] == play_id]
        if not play_trades:
            continue
        cfg = configs.get(play_id, {})
        print(f"\n  --- Play {play_id}  ({cfg.get('name','?')},  n={len(play_trades)}) ---")
        diag_play = run_diagnostics(play_trades, price_checks_map)
        print_diagnostics(diag_play)

    # ── Baseline: current config on FULL FORWARD paths ────────────────────────
    _banner(f"CURRENT CONFIG BASELINE (simulated on {FORWARD_MINUTES}-min forward paths)")
    baseline_exits: List[float] = []
    for t in trades:
        cfg = configs.get(t['play_id'], {})
        ep  = simulate_current_config(t, cfg.get('sell_logic', {}), fwd_cache)
        baseline_exits.append(ep - COST_PCT * 100)

    baseline_avg = sum(baseline_exits) / len(baseline_exits) if baseline_exits else 0.0
    baseline_wr  = sum(1 for e in baseline_exits if e > 0) / len(baseline_exits) if baseline_exits else 0.0
    print(f"\n  Simulated avg exit:  {baseline_avg:+.4f}%")
    print(f"  Simulated win rate:  {baseline_wr*100:.1f}%")
    print(f"  Total trades:        {len(baseline_exits)}")
    print(f"  (Using full {FORWARD_MINUTES}-min forward window — not truncated at actual exit)")

    # ── Genetic algorithm ──────────────────────────────────────────────────────
    _banner(f"GENETIC ALGORITHM  (pop={pop_size}  gen={n_gens})")
    print(
        f"\n  Optimising 9 parameters:\n"
        f"    stop_loss, t1_tol, t1_bnd, t2_tol, t2_bnd, t3_tol,\n"
        f"    min_hold_steps, consec_drops (new), grace_steps (new)\n"
        f"  Using {FORWARD_MINUTES}-min forward price paths (SOL/s from prices table)\n"
    )

    t0          = time.time()
    top_results = run_ga(trades, fwd_cache, pop_size, n_gens, verbose=True)
    print(f"\n  GA completed in {time.time()-t0:.1f}s")

    # ── Walk-forward validation ────────────────────────────────────────────────
    _banner("WALK-FORWARD OOS VALIDATION")
    print(f"\n  Validating top {len(top_results)} configs against {OOS_FOLDS} chronological folds...")
    validated = validate_oos(top_results, trades, fwd_cache, OOS_FOLDS)

    # ── Results table ──────────────────────────────────────────────────────────
    _banner(f"BEST EXIT CONFIGS  (baseline: {baseline_avg:+.4f}%)")
    print_results_table(validated, baseline_avg)

    # ── Best config detail ─────────────────────────────────────────────────────
    robust_configs = [r for r in validated if r['is_robust']]
    best           = robust_configs[0] if robust_configs else validated[0]
    p              = best['params']

    _banner("BEST CONFIG DETAIL")
    print(f"\n  In-sample avg exit:   {best['is_stats'].get('avg_exit',0):+.4f}%")
    print(f"  OOS avg exit:         {best['oos_avg_exit']:+.4f}%"
          f"  ({best['oos_pos_folds']}/{best['oos_n_folds']} folds positive)")
    print(f"  OOS win rate:         {best['oos_win_rate']*100:.1f}%")
    print(f"  vs baseline:          {best['oos_avg_exit'] - baseline_avg:+.4f}% improvement")
    print(f"  Robust (≥{OOS_FOLDS-1}/{OOS_FOLDS} folds): {'YES' if best['is_robust'] else 'no'}")
    print()
    print(f"  Stop-loss:                    {p['stop_loss']*100:.3f}%")
    print(f"  Tier 1  [0 – {p['t1_bnd']*100:.3f}%]:  "
          f"trailing tol = {p['t1_tol']*100:.3f}%")
    print(f"  Tier 2  [{p['t1_bnd']*100:.3f}% – {p['t2_bnd']*100:.3f}%]:  "
          f"trailing tol = {p['t2_tol']*100:.3f}%")
    print(f"  Tier 3  [>{p['t2_bnd']*100:.3f}%]:  "
          f"trailing tol = {p['t3_tol']*100:.4f}%")
    print(f"  Min-hold steps:               {p['min_hold_steps']}")
    print(f"  Consecutive drops required:   {p['consec_drops']}")
    print(f"  Grace steps after new high:   {p['grace_steps']}")

    best_sell_logic = build_sell_logic(p)
    print(f"\n  sell_logic JSON (written to DB with --apply):")
    print(json.dumps(best_sell_logic, indent=4))

    if p['consec_drops'] > 1:
        print(
            f"\n  NOTE on consec_drops={p['consec_drops']}:\n"
            f"  The current sell_trailing_stop.py fires on the FIRST tolerance breach.\n"
            f"  Implementing consec_drops requires a small change to check_position().\n"
            f"  The tolerances above are calibrated to work well with the current\n"
            f"  behaviour (consec_drops=1) — they are simply wider to absorb noise.\n"
            f"  For maximum gain you would also patch sell_trailing_stop.py."
        )

    if p['grace_steps'] > 0:
        print(
            f"\n  NOTE on grace_steps={p['grace_steps']}:\n"
            f"  Grace steps after a new high also require a change to check_position().\n"
            f"  The sell_logic JSON does not encode this; it is optimizer-internal."
        )

    # ── Per-play comparison ────────────────────────────────────────────────────
    _banner(f"PER-PLAY COMPARISON  (current config vs best GA config, {FORWARD_MINUTES}-min window)")
    for play_id in play_ids:
        play_trades = [t for t in trades if t['play_id'] == play_id]
        if len(play_trades) < 3:
            continue
        cfg = configs.get(play_id, {})

        curr_exits: List[float] = []
        for t in play_trades:
            ep = simulate_current_config(t, cfg.get('sell_logic', {}), fwd_cache)
            curr_exits.append(ep - COST_PCT * 100)

        best_exits: List[float] = []
        for t in play_trades:
            entry = float(t['entry'])
            if entry <= 0:
                continue
            series = get_forward_series(t, fwd_cache)
            ep, _ = simulate_exit(
                entry, series,
                p['stop_loss'], p['t1_tol'], p['t1_bnd'],
                p['t2_tol'],    p['t2_bnd'], p['t3_tol'],
                p['min_hold_steps'], p['consec_drops'], p['grace_steps'],
            )
            best_exits.append(ep - COST_PCT * 100)

        curr_avg = sum(curr_exits) / len(curr_exits) if curr_exits else 0.0
        best_avg = sum(best_exits) / len(best_exits) if best_exits else 0.0
        curr_wr  = sum(1 for e in curr_exits if e > 0) / len(curr_exits) if curr_exits else 0.0
        best_wr  = sum(1 for e in best_exits if e > 0) / len(best_exits) if best_exits else 0.0
        delta    = best_avg - curr_avg

        print(f"\n  Play {play_id}  ({cfg.get('name','?')},  n={len(play_trades)})")
        print(f"    Current config:  avg={curr_avg:+.4f}%   win={curr_wr*100:.1f}%")
        print(f"    Best GA config:  avg={best_avg:+.4f}%   win={best_wr*100:.1f}%")
        print(f"    Improvement:     {delta:+.4f}%")

    # ── Apply ──────────────────────────────────────────────────────────────────
    if args.apply:
        _banner("APPLYING BEST CONFIG TO DATABASE")
        apply_to_plays(play_ids, best_sell_logic)
        print("\n  Done. Trailing stop seller picks up changes within 60 s (cache TTL).")
    else:
        print(f"\nRun with --apply to write the best sell_logic to the database.")

    print()


if __name__ == "__main__":
    main()

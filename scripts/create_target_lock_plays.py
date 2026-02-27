"""
Target-and-Lock Play Creator
=============================
Creates 3 new "patience" plays that use a two-phase exit strategy:

  Phase 1 — PATIENCE  (below target gain):
    - Wide pre-target trailing tolerance (effectively inactive — stop-loss is the floor)
    - Hard stop-loss from entry (-0.12 to -0.18%) limits max loss on failed pumps
    - Trusts the signal: holds through noise while the pump is developing

  Phase 2 — LOCK-IN  (once target gain is hit):
    - Extremely tight trailing tolerance (0.07-0.10%) kicks in immediately
    - Locks in the majority of the gain as soon as the target is reached

Three plays for A/B comparison:
    - Play TL-02: target = 0.20%  stop-loss = 0.12%  lock-tol = 0.08%
    - Play TL-03: target = 0.30%  stop-loss = 0.15%  lock-tol = 0.09%
    - Play TL-04: target = 0.40%  stop-loss = 0.20%  lock-tol = 0.10%

Signal alignment rationale:
    The mega_simulator signals predict *direction* — something is about to move up.
    Current plays get shaken out by noise before the pump develops (trailing stop fires
    on a tiny peak, exiting at a loss). The target-and-lock approach trusts the signal
    until proven wrong (stop-loss) or proven right (target hit → lock in).

Simulation uses actual 1-second price_checks data for all historical trades.

Usage:
    python3 scripts/create_target_lock_plays.py           # simulate only, compare results
    python3 scripts/create_target_lock_plays.py --create  # also insert plays into DB
    python3 scripts/create_target_lock_plays.py --days 14 # wider lookback
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute

COST_PCT = 0.001  # 0.1% round-trip

# ── Target-and-lock play definitions ─────────────────────────────────────────
# (label, target_gain, stop_loss, pre_target_tol, lock_tol, sim_filter)
TARGET_LOCK_PLAYS: List[Dict[str, Any]] = [
    {
        "name":            "Target-Lock 0.2%",
        "target":          0.002,   # 0.20%
        "stop_loss":       0.0012,  # -0.12% hard floor
        "pre_target_tol":  0.50,    # effectively inactive (stop-loss fires first)
        "lock_tol":        0.0008,  # 0.08% tight trailing once target hit
        "sim_filter": {             # same signal quality as Play 3 balanced
            "win_rate_min":     0.60,
            "daily_ev_min":     0.0,
            "n_signals_min":    10,
            "oos_gap_max":      0.008,
            "cooldown_seconds": 90,
        },
    },
    {
        "name":            "Target-Lock 0.3%",
        "target":          0.003,   # 0.30%
        "stop_loss":       0.0015,  # -0.15% hard floor
        "pre_target_tol":  0.50,    # effectively inactive
        "lock_tol":        0.0009,  # 0.09% trailing once target hit
        "sim_filter": {
            "win_rate_min":     0.60,
            "daily_ev_min":     0.0,
            "n_signals_min":    10,
            "oos_gap_max":      0.008,
            "cooldown_seconds": 90,
        },
    },
    {
        "name":            "Target-Lock 0.4%",
        "target":          0.004,   # 0.40%
        "stop_loss":       0.002,   # -0.20% hard floor (need more room to wait for bigger move)
        "pre_target_tol":  0.50,    # effectively inactive
        "lock_tol":        0.0010,  # 0.10% trailing once target hit
        "sim_filter": {
            "win_rate_min":     0.60,
            "daily_ev_min":     0.0,
            "n_signals_min":    10,
            "oos_gap_max":      0.008,
            "cooldown_seconds": 90,
        },
    },
]


# =============================================================================
# DATA LOADING — uses actual 1-second price series from price_checks
# =============================================================================

def load_trade_price_series(days: int, play_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Load every 1-second price observation for each closed trade.
    Returns: {buyin_id: {entry, prices: [float, ...], max_gain_pct, actual_exit_pct, play_id}}
    """
    print(f"Loading 1-second price series for all trades (last {days} days)...")

    with get_postgres() as conn:
        with conn.cursor() as cur:
            # Load trade metadata
            cur.execute("""
                SELECT id, play_id, our_entry_price AS entry, higest_price_reached AS peak,
                       our_exit_price AS exit_p, our_status,
                       ROUND(((higest_price_reached - our_entry_price)
                              / NULLIF(our_entry_price, 0) * 100)::numeric, 5) AS max_gain_pct,
                       ROUND(((our_exit_price - our_entry_price)
                              / NULLIF(our_entry_price, 0) * 100)::numeric, 5) AS exit_pct
                FROM follow_the_goat_buyins
                WHERE play_id = ANY(%s)
                  AND followed_at > NOW() - (%s || ' days')::INTERVAL
                  AND our_entry_price IS NOT NULL
                  AND our_exit_price IS NOT NULL
                  AND wallet_address NOT LIKE 'TRAINING_TEST_%%'
                ORDER BY id
            """, [play_ids, str(days)])
            trades = {row['id']: dict(row) for row in cur.fetchall()}

            if not trades:
                return {}

            trade_ids = list(trades.keys())
            print(f"  Found {len(trade_ids)} closed trades — loading price series...")

            # Load price series in one batch (ordered by buyin_id, checked_at)
            cur.execute("""
                SELECT buyin_id, current_price
                FROM follow_the_goat_buyins_price_checks
                WHERE buyin_id = ANY(%s)
                  AND is_backfill = FALSE
                ORDER BY buyin_id, checked_at
            """, [trade_ids])

            for row in cur.fetchall():
                bid = row['buyin_id']
                if bid in trades:
                    if 'prices' not in trades[bid]:
                        trades[bid]['prices'] = []
                    trades[bid]['prices'].append(float(row['current_price']))

    # Filter to trades that have usable price series
    usable = {}
    for tid, t in trades.items():
        prices = t.get('prices', [])
        if len(prices) >= 3 and t.get('entry') and float(t['entry']) > 0:
            t['prices'] = prices
            usable[tid] = t

    print(f"  {len(usable)} trades have usable price series (≥3 ticks)")
    return usable


# =============================================================================
# SIMULATION ENGINE
# =============================================================================

def simulate_target_lock(
    entry: float,
    prices: List[float],
    target: float,
    stop_loss: float,
    pre_target_tol: float,
    lock_tol: float,
    min_hold_steps: int = 0,
) -> Tuple[float, str]:
    """
    Simulate the target-and-lock strategy on a 1-second price series.

    Phase 1 (highest_gain < target):
        - If price drops from entry by > stop_loss → sell (stop-loss)
        - Trailing stop is pre_target_tol (very wide, effectively inactive)

    Phase 2 (highest_gain >= target):
        - Trailing stop tightens to lock_tol (very tight)
        - Ratchet ensures it never loosens back

    Returns (exit_pct_from_entry, reason)
    """
    if entry <= 0 or not prices:
        return 0.0, "no_data"

    highest = entry
    locked_tol = pre_target_tol  # starts wide
    target_hit = False

    for step, price in enumerate(prices):
        if price > highest:
            highest = price
            highest_gain = (highest - entry) / entry

            # Phase transition: target just reached
            if not target_hit and highest_gain >= target:
                target_hit = True
                locked_tol = lock_tol   # LOCK IN immediately
                # Don't sell yet — just update the tolerance

        highest_gain = (highest - entry) / entry

        if step < min_hold_steps:
            continue

        # ── Stop-loss check (from entry, always active) ──────────────────────
        drop_from_entry = (price - entry) / entry
        if drop_from_entry < -stop_loss:
            return drop_from_entry * 100, "stop_loss"

        # ── Trailing stop (from highest, only once we've had any gain) ────────
        if highest_gain > 0:
            drop_from_high = (price - highest) / highest
            if drop_from_high < -locked_tol:
                exit_p = highest * (1.0 - locked_tol)
                return (exit_p - entry) / entry * 100, "trailing"

    # Held entire window — exit at last price
    final = prices[-1]
    return (final - entry) / entry * 100, "timeout"


def simulate_current(
    entry: float,
    prices: List[float],
    sell_logic: Dict[str, Any],
    min_hold_steps: int = 0,
) -> Tuple[float, str]:
    """Simulate the current sell_logic against a price series for comparison."""
    if entry <= 0 or not prices:
        return 0.0, "no_data"

    tr = sell_logic.get('tolerance_rules', {})
    dec_rules = tr.get('decreases', [])
    inc_rules  = tr.get('increases', [])

    def _get_tol(gain: float, rules: List[Dict]) -> float:
        for r in rules:
            lo, hi = float(r['range'][0]), float(r['range'][1])
            if lo <= gain < hi:
                return float(r['tolerance'])
        return float(rules[-1]['tolerance']) if rules else 0.003

    stop_loss_tol = _get_tol(0.0, dec_rules) if dec_rules else 0.003

    highest = entry
    locked_trail_tol = 1.0

    for step, price in enumerate(prices):
        if price > highest:
            highest = price

        highest_gain = (highest - entry) / entry

        # Select tier from HIGHEST gain achieved
        trail_tol = _get_tol(highest_gain, inc_rules)
        if trail_tol < locked_trail_tol:
            locked_trail_tol = trail_tol

        if step < min_hold_steps:
            continue

        drop_from_entry = (price - entry) / entry
        if drop_from_entry < -stop_loss_tol:
            return drop_from_entry * 100, "stop_loss"

        if highest_gain > 0:
            drop_from_high = (price - highest) / highest
            if drop_from_high < -locked_trail_tol:
                exit_p = highest * (1.0 - locked_trail_tol)
                return (exit_p - entry) / entry * 100, "trailing"

    final = prices[-1]
    return (final - entry) / entry * 100, "timeout"


# =============================================================================
# STATISTICS
# =============================================================================

def compute_stats(exits_net: List[float]) -> Dict[str, float]:
    if not exits_net:
        return {}
    n = len(exits_net)
    wins = [e for e in exits_net if e > 0]
    losses = [e for e in exits_net if e <= 0]
    sorted_e = sorted(exits_net)

    def pct(p: int) -> float:
        idx = max(0, min(n - 1, int(n * p / 100)))
        return sorted_e[idx]

    return {
        'n':        n,
        'win_rate': len(wins) / n,
        'avg_exit': sum(exits_net) / n,
        'avg_win':  sum(wins) / len(wins) if wins else 0.0,
        'avg_loss': sum(losses) / len(losses) if losses else 0.0,
        'p25':      pct(25),
        'p50':      pct(50),
        'p75':      pct(75),
        'daily_ev': (sum(exits_net) / n) * (n / 7),  # rough daily EV proxy
    }


# =============================================================================
# PLAY CREATION
# =============================================================================

def build_sell_logic(play_def: Dict[str, Any]) -> Dict[str, Any]:
    """Convert play definition to the sell_logic JSON stored in follow_the_goat_plays."""
    target = play_def['target']
    return {
        "tolerance_rules": {
            "decreases": [
                {"range": [-999999, 0], "tolerance": play_def['stop_loss']}
            ],
            "increases": [
                # Pre-target: very wide trail (stop-loss acts as the floor)
                {"range": [0.0,    target], "tolerance": play_def['pre_target_tol']},
                # Post-target: extremely tight lock-in
                {"range": [target, 1.0],    "tolerance": play_def['lock_tol']},
            ],
        },
        # Metadata fields for reference
        "_strategy":   "target_lock",
        "_target_pct": target * 100,
        "_lock_tol":   play_def['lock_tol'] * 100,
    }


def build_pattern_validator(play_def: Dict[str, Any]) -> Dict[str, Any]:
    """Build pattern_validator JSON (contains sim_filter for signal selection)."""
    return {
        "sim_filter": play_def['sim_filter'],
    }


def create_plays_in_db(plays: List[Dict[str, Any]]) -> List[int]:
    """Insert the new plays into follow_the_goat_plays. Returns list of new IDs."""
    new_ids = []
    for play_def in plays:
        sell_logic = build_sell_logic(play_def)
        pv         = build_pattern_validator(play_def)

        with get_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO follow_the_goat_plays
                        (name, is_active, sell_logic, pattern_validator)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, [
                    play_def['name'],
                    0,                         # start INACTIVE — enable via dashboard when ready
                    json.dumps(sell_logic),
                    json.dumps(pv),
                ])
                new_id = cur.fetchone()['id']
                conn.commit()
        new_ids.append(new_id)
        print(f"  ✓ Created Play #{new_id}: {play_def['name']}")

    return new_ids


# =============================================================================
# REPORTING
# =============================================================================

def print_sep(title: str = "") -> None:
    print(f"\n{'='*80}")
    if title:
        print(f"  {title}")
        print("=" * 80)


def print_stats_row(label: str, stats: Dict[str, float], reason_counts: Dict[str, int] = None) -> None:
    if not stats:
        print(f"  {label:<30} (no data)")
        return
    rc = reason_counts or {}
    sl_n   = rc.get('stop_loss', 0)
    trail_n = rc.get('trailing', 0)
    to_n   = rc.get('timeout', 0)
    print(
        f"  {label:<32} "
        f"win={stats['win_rate']*100:5.1f}%  "
        f"avg={stats['avg_exit']:+7.4f}%  "
        f"p50={stats['p50']:+7.4f}%  "
        f"win_avg={stats['avg_win']:+7.4f}%  "
        f"loss_avg={stats['avg_loss']:+7.4f}%"
        + (f"  [SL={sl_n} TR={trail_n} TO={to_n}]" if rc else "")
    )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Target-and-lock play simulator + creator")
    parser.add_argument("--days",   type=int, default=7, help="Lookback days (default 7)")
    parser.add_argument("--create", action="store_true",  help="Insert plays into DB")
    parser.add_argument("--plays",  type=str, default="3,4,5,6",
                        help="Reference play IDs for comparison (default: 3,4,5,6)")
    args = parser.parse_args()

    ref_play_ids = [int(x.strip()) for x in args.plays.split(",") if x.strip()]

    print_sep(f"TARGET-AND-LOCK PLAY SIMULATOR  —  last {args.days} days")

    # ── Load data ─────────────────────────────────────────────────────────────
    trades = load_trade_price_series(args.days, ref_play_ids)
    if not trades:
        print("No trades found — exiting")
        sys.exit(1)

    # Load current play sell_logic for comparison
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, sell_logic
                FROM follow_the_goat_plays
                WHERE id = ANY(%s)
            """, [ref_play_ids])
            current_plays = {row['id']: dict(row) for row in cur.fetchall()}

    trade_list = list(trades.values())
    n_total = len(trade_list)

    # ── Entry quality overview ─────────────────────────────────────────────────
    print_sep("ENTRY QUALITY — how often does the price reach each target?")
    for target_pct in [0.1, 0.2, 0.3, 0.4, 0.5]:
        hit = sum(1 for t in trade_list
                  if t.get('max_gain_pct') and float(t['max_gain_pct']) >= target_pct)
        print(f"  Trades reaching ≥{target_pct:.1f}% peak:  {hit:4d} / {n_total}  ({hit/n_total*100:.1f}%)")

    # ── Simulate current plays ─────────────────────────────────────────────────
    print_sep("CURRENT PLAY PERFORMANCE (simulated on real 1-second price series)")
    current_sim_stats: Dict[int, Dict] = {}

    for pid in ref_play_ids:
        cfg = current_plays.get(pid, {})
        sell_raw = cfg.get('sell_logic')
        sell = (json.loads(sell_raw) if isinstance(sell_raw, str) else sell_raw) or {}
        min_hold_s = sell.get('min_hold_seconds', 0)
        min_hold_steps = int(min_hold_s)  # 1 step = ~1 second in price_checks

        play_trades = [t for t in trade_list if t['play_id'] == pid]
        if not play_trades:
            continue

        exits = []
        reasons: Dict[str, int] = {}
        for t in play_trades:
            entry  = float(t['entry'])
            prices = t['prices']
            ep, reason = simulate_current(entry, prices, sell, min_hold_steps)
            net = ep - COST_PCT * 100
            exits.append(net)
            reasons[reason] = reasons.get(reason, 0) + 1

        stats = compute_stats(exits)
        current_sim_stats[pid] = stats
        name = cfg.get('name', f'Play {pid}')
        print_stats_row(f"Play {pid} ({name[:16]})", stats, reasons)

    # ── Simulate target-and-lock plays ────────────────────────────────────────
    print_sep("TARGET-AND-LOCK SIMULATION (all reference-play trades as test bed)")

    tl_results = []
    for play_def in TARGET_LOCK_PLAYS:
        target       = play_def['target']
        stop_loss    = play_def['stop_loss']
        pre_tol      = play_def['pre_target_tol']
        lock_tol     = play_def['lock_tol']

        exits = []
        reasons: Dict[str, int] = {}
        target_hit_count = 0

        for t in trade_list:
            entry  = float(t['entry'])
            prices = t['prices']
            ep, reason = simulate_target_lock(
                entry, prices, target, stop_loss, pre_tol, lock_tol
            )
            # Check if target was hit
            max_gain = max((p - entry) / entry for p in prices) if prices else 0.0
            if max_gain >= target:
                target_hit_count += 1

            net = ep - COST_PCT * 100
            exits.append(net)
            reasons[reason] = reasons.get(reason, 0) + 1

        stats = compute_stats(exits)
        tl_results.append({
            'play_def':          play_def,
            'stats':             stats,
            'reasons':           reasons,
            'target_hit_count':  target_hit_count,
        })

        label = f"{play_def['name']}  (SL={stop_loss*100:.2f}% lock={lock_tol*100:.3f}%)"
        print_stats_row(label, stats, reasons)
        print(f"    → Target hit: {target_hit_count}/{n_total} ({target_hit_count/n_total*100:.1f}%)")

    # ── Head-to-head comparison ────────────────────────────────────────────────
    print_sep("HEAD-TO-HEAD: TARGET-LOCK vs CURRENT (best current play per metric)")

    best_current_win_rate = max((s.get('win_rate', 0) for s in current_sim_stats.values()), default=0)
    best_current_avg_exit = max((s.get('avg_exit', -99) for s in current_sim_stats.values()), default=-99)

    print(f"\n  Best current win_rate: {best_current_win_rate*100:.1f}%")
    print(f"  Best current avg_exit: {best_current_avg_exit:+.4f}%\n")

    best_tl = None
    for r in tl_results:
        s = r['stats']
        win_delta = s['win_rate'] - best_current_win_rate
        ev_delta  = s['avg_exit'] - best_current_avg_exit
        print(
            f"  {r['play_def']['name']:<25}  "
            f"win_rate Δ={win_delta*100:+.1f}pp  "
            f"avg_exit Δ={ev_delta:+.4f}%"
        )
        if best_tl is None or s['avg_exit'] > best_tl['stats']['avg_exit']:
            best_tl = r

    # ── Signal alignment analysis ──────────────────────────────────────────────
    print_sep("SIGNAL ALIGNMENT — do target-lock plays trust the signals better?")
    print("""
  Current play problem:
    - Signal fires → entry made
    - Price makes tiny move (+0.05-0.2%) → trailing stop fires from that tiny peak
    - Exit is BELOW entry (gave back gain + cross into loss)
    - The signal was "right" (price did go up) but exit was wrong

  Target-and-lock philosophy:
    - Signal fires → entry made
    - Price drifts around → stop-loss holds as the only floor (patient)
    - If price reaches TARGET → tolerance locks immediately to 0.07-0.10%
    - Capture majority of the pump when signal is truly correct
    - On failed pumps: clean exit at stop-loss level (predictable loss)
""")

    for r in tl_results:
        pd_ = r['play_def']
        s   = r['stats']
        hit = r['target_hit_count']
        hit_pct = hit / n_total * 100
        # When target IS hit: what's avg exit?
        exits_when_hit = []
        exits_when_miss = []
        for t in trade_list:
            entry  = float(t['entry'])
            prices = t['prices']
            max_gain = max((p - entry) / entry for p in prices) if prices else 0.0
            ep, _ = simulate_target_lock(
                entry, prices, pd_['target'], pd_['stop_loss'], pd_['pre_target_tol'], pd_['lock_tol']
            )
            net = ep - COST_PCT * 100
            if max_gain >= pd_['target']:
                exits_when_hit.append(net)
            else:
                exits_when_miss.append(net)

        avg_hit  = sum(exits_when_hit) / len(exits_when_hit)   if exits_when_hit  else 0.0
        avg_miss = sum(exits_when_miss) / len(exits_when_miss) if exits_when_miss else 0.0

        print(f"  {pd_['name']}  (target={pd_['target']*100:.1f}%)")
        print(f"    Signal correct (price hit target): {hit:3d} trades ({hit_pct:.0f}%)  avg exit = {avg_hit:+.4f}%")
        print(f"    Signal wrong   (price missed):     {n_total-hit:3d} trades ({100-hit_pct:.0f}%)  avg exit = {avg_miss:+.4f}%")
        implied_ev = hit_pct/100 * avg_hit + (1 - hit_pct/100) * avg_miss
        print(f"    Implied EV = {hit_pct:.0f}% × {avg_hit:+.4f}% + {100-hit_pct:.0f}% × {avg_miss:+.4f}% = {implied_ev:+.4f}%")
        print()

    # ── Play sell_logic configs ────────────────────────────────────────────────
    print_sep("NEW SELL_LOGIC CONFIGS (these will be stored in follow_the_goat_plays)")
    for play_def in TARGET_LOCK_PLAYS:
        sl = build_sell_logic(play_def)
        print(f"\n  {play_def['name']}:")
        print(f"    Stop-loss:      -{play_def['stop_loss']*100:.2f}% from entry")
        print(f"    Pre-target tol:  {play_def['pre_target_tol']*100:.0f}% trail  (inactive below target)")
        print(f"    Target:         +{play_def['target']*100:.2f}%  ← tolerance locks here")
        print(f"    Lock-in tol:     {play_def['lock_tol']*100:.3f}% trail  (very tight above target)")
        print(f"    sell_logic JSON: {json.dumps({k: v for k, v in sl.items() if not k.startswith('_')})}")

    # ── Create plays in DB ─────────────────────────────────────────────────────
    if args.create:
        print_sep("CREATING PLAYS IN DATABASE")
        print("  Plays will be inserted with is_active=0 (disabled).")
        print("  Enable via the Scheduler dashboard when ready to test.\n")
        new_ids = create_plays_in_db(TARGET_LOCK_PLAYS)
        print(f"\n  Created play IDs: {new_ids}")
        print("  Next steps:")
        print("    1. Check the plays in the dashboard → follow_the_goat_plays")
        print("    2. Set is_active=1 for one play at a time to A/B test")
        print("    3. Compare against plays 3,4,5,6 over 24-48h")
        print(f"    4. Re-run:  python3 scripts/analyze_play_performance.py --plays {','.join(str(i) for i in ref_play_ids + new_ids)}")
    else:
        print("\n  Run with --create to insert these plays into the database.")
        print("  They start inactive (is_active=0) — enable via dashboard.")

    print()


if __name__ == "__main__":
    main()

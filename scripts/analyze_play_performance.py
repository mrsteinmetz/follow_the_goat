"""
Play Performance Analyzer
=========================
Diagnoses why plays are underperforming by analyzing:

  1. Entry quality — are signals hitting pumps at all?
  2. Exit bleed — trades that peaked positive but exited negative (gain giveback)
  3. Tolerance simulation — what sell configs would have performed better?
  4. Per-play recommendations — concrete suggested sell_logic updates

Usage:
    python3 scripts/analyze_play_performance.py
    python3 scripts/analyze_play_performance.py --days 14
    python3 scripts/analyze_play_performance.py --apply       # write recommended configs to DB
    python3 scripts/analyze_play_performance.py --play 3      # single play
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

# ── Plays to analyse by default ───────────────────────────────────────────────
DEFAULT_PLAY_IDS = [3, 4, 5, 6]

# ── Simulation grid: tolerances to test ───────────────────────────────────────
# Each config is: (stop_loss_pct, tier1_tol_pct, tier1_boundary_pct, tier2_tol_pct, tier2_boundary_pct, tier3_tol_pct)
SIM_CONFIGS = [
    # label,           stop_loss,  t1_tol,  t1_boundary, t2_tol, t2_boundary, t3_tol
    ("current_p3",     0.003,      0.003,   0.002,       0.001,  0.003,       0.0005),
    ("current_p4",     0.0015,     0.0015,  0.003,       0.0008, 0.006,       0.0005),
    ("tight_sl",       0.001,      0.002,   0.001,       0.001,  0.003,       0.0005),
    ("very_tight",     0.001,      0.001,   0.001,       0.0008, 0.002,       0.0003),
    ("scalp_mode",     0.001,      0.0008,  0.001,       0.0005, 0.002,       0.0003),
    ("ultra_tight",    0.0008,     0.0006,  0.0008,      0.0004, 0.0015,      0.0002),
    ("wide_winner",    0.002,      0.003,   0.003,       0.0015, 0.006,       0.0008),
    ("balanced_new",   0.0012,     0.0012,  0.0015,      0.0008, 0.003,       0.0004),
]

COST_PCT = 0.001  # 0.1% round-trip trading cost


# =============================================================================
# DATA LOADING
# =============================================================================

def load_trades(play_ids: List[int], days: int) -> List[Dict[str, Any]]:
    """Load closed trades with price movement series for simulation."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    b.id,
                    b.play_id,
                    b.our_entry_price    AS entry,
                    b.our_exit_price     AS exit_p,
                    b.higest_price_reached AS peak,
                    b.our_status,
                    b.followed_at,
                    b.our_exit_timestamp,
                    EXTRACT(EPOCH FROM (b.our_exit_timestamp - b.followed_at)) AS hold_seconds,
                    ROUND(((b.higest_price_reached - b.our_entry_price)
                           / NULLIF(b.our_entry_price,0) * 100)::numeric, 5) AS max_gain_pct,
                    ROUND(((b.our_exit_price - b.our_entry_price)
                           / NULLIF(b.our_entry_price,0) * 100)::numeric, 5) AS exit_pct,
                    b.fifteen_min_trail
                FROM follow_the_goat_buyins b
                WHERE b.play_id = ANY(%s)
                  AND b.followed_at > NOW() - (%s || ' days')::INTERVAL
                  AND b.our_entry_price IS NOT NULL
                  AND b.our_exit_price IS NOT NULL
                  AND b.wallet_address NOT LIKE 'TRAINING_TEST_%%'
                ORDER BY b.followed_at DESC
            """, [play_ids, str(days)])
            return [dict(r) for r in cur.fetchall()]


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


# =============================================================================
# EXIT SIMULATION
# =============================================================================

def _get_price_series_from_trail(trail_json: Any, entry: float) -> List[float]:
    """
    Extract an ordered list of observed prices from the fifteen_min_trail JSONB.
    Falls back to [entry] if data is missing.
    """
    if not trail_json:
        return [entry]
    try:
        data = trail_json if isinstance(trail_json, list) else json.loads(trail_json)
        prices = [float(p['price']) for p in data if 'price' in p]
        return prices if prices else [entry]
    except Exception:
        return [entry]


def simulate_exit(
    entry: float,
    price_series: List[float],
    stop_loss: float,
    t1_tol: float,
    t1_boundary: float,
    t2_tol: float,
    t2_boundary: float,
    t3_tol: float,
    min_hold_steps: int = 0,
) -> Tuple[float, str]:
    """
    Replay a price series against a tiered trailing stop.

    Returns (exit_pct_gain, reason).
    Tiers: (0, t1_boundary) → t1_tol, (t1_boundary, t2_boundary) → t2_tol, else → t3_tol
    """
    if entry <= 0 or not price_series:
        return 0.0, "no_data"

    highest = entry
    locked_tol = 1.0  # starts very loose, tightens as price rises

    for step, price in enumerate(price_series):
        if price > highest:
            highest = price

        highest_gain = (highest - entry) / entry

        # Select tier tolerance
        if highest_gain < t1_boundary:
            trail_tol = t1_tol
        elif highest_gain < t2_boundary:
            trail_tol = t2_tol
        else:
            trail_tol = t3_tol

        # Lock ratchet — tolerance only tightens
        if trail_tol < locked_tol:
            locked_tol = trail_tol

        if step < min_hold_steps:
            continue

        # Stop-loss check (from entry)
        drop_from_entry = (price - entry) / entry
        if drop_from_entry < -stop_loss:
            return drop_from_entry * 100, "stop_loss"

        # Trailing stop check (from highest, only if we've had a gain)
        if highest_gain > 0:
            drop_from_high = (price - highest) / highest
            if drop_from_high < -locked_tol:
                exit_p = highest * (1.0 - locked_tol)
                return (exit_p - entry) / entry * 100, "trailing"

    # Held to end — exit at last price
    final = price_series[-1]
    return (final - entry) / entry * 100, "timeout"


# =============================================================================
# ANALYSIS
# =============================================================================

def analyze_play(
    trades: List[Dict[str, Any]],
    play_id: int,
    play_name: str,
    sell_logic: Dict[str, Any],
    sim_configs: List[Tuple],
) -> Dict[str, Any]:
    """Full analysis for one play."""
    my_trades = [t for t in trades if t['play_id'] == play_id]
    n = len(my_trades)
    if n == 0:
        return {'play_id': play_id, 'n': 0}

    # ── Actual performance ───────────────────────────────────────────────────
    exit_pcts  = [float(t['exit_pct']) for t in my_trades if t['exit_pct'] is not None]
    peak_pcts  = [float(t['max_gain_pct']) for t in my_trades if t['max_gain_pct'] is not None]
    hold_secs  = [float(t['hold_seconds']) for t in my_trades if t['hold_seconds'] is not None]

    winners    = [e for e in exit_pcts if e > 0]
    losers     = [e for e in exit_pcts if e <= 0]

    # Trades that peaked positive but exited negative
    gave_back  = [t for t in my_trades
                  if t['max_gain_pct'] is not None and float(t['max_gain_pct']) > 0
                  and t['exit_pct'] is not None and float(t['exit_pct']) < 0]
    gb_peaks   = [float(t['max_gain_pct']) for t in gave_back]
    gb_exits   = [float(t['exit_pct']) for t in gave_back]

    # Peak-capture ratio on winners (exit / peak)
    capture_rates = []
    for t in my_trades:
        if t['max_gain_pct'] and float(t['max_gain_pct']) > 0 and t['exit_pct'] and float(t['exit_pct']) > 0:
            capture_rates.append(float(t['exit_pct']) / float(t['max_gain_pct']))

    def _pct(vals: List[float], p: int) -> float:
        if not vals:
            return 0.0
        sorted_v = sorted(vals)
        idx = max(0, min(len(sorted_v) - 1, int(len(sorted_v) * p / 100)))
        return sorted_v[idx]

    actual_stats = {
        'n': n,
        'win_rate': len(winners) / n,
        'avg_exit': sum(exit_pcts) / len(exit_pcts) if exit_pcts else 0,
        'avg_win': sum(winners) / len(winners) if winners else 0,
        'avg_loss': sum(losers) / len(losers) if losers else 0,
        'p25_exit': _pct(exit_pcts, 25),
        'p50_exit': _pct(exit_pcts, 50),
        'p75_exit': _pct(exit_pcts, 75),
        'p25_peak': _pct(peak_pcts, 25),
        'p50_peak': _pct(peak_pcts, 50),
        'p75_peak': _pct(peak_pcts, 75),
        'p90_peak': _pct(peak_pcts, 90),
        'gave_back_count': len(gave_back),
        'gave_back_pct': len(gave_back) / n,
        'avg_gb_peak': sum(gb_peaks) / len(gb_peaks) if gb_peaks else 0,
        'avg_gb_exit': sum(gb_exits) / len(gb_exits) if gb_exits else 0,
        'avg_peak_capture': sum(capture_rates) / len(capture_rates) if capture_rates else 0,
        'avg_hold_s': sum(hold_secs) / len(hold_secs) if hold_secs else 0,
    }

    # ── Tolerance simulation ─────────────────────────────────────────────────
    sim_results = []

    for label, sl, t1t, t1b, t2t, t2b, t3t in sim_configs:
        sim_exits = []

        for t in my_trades:
            entry = float(t['entry'])
            trail_data = t.get('fifteen_min_trail')
            price_series = _get_price_series_from_trail(trail_data, entry)

            # If no price series, use a 2-point series: entry → peak → exit
            if len(price_series) <= 1 and t['peak'] and t['exit_p']:
                price_series = [entry, float(t['peak']), float(t['exit_p'])]

            sim_exit_pct, _reason = simulate_exit(
                entry, price_series, sl, t1t, t1b, t2t, t2b, t3t
            )
            sim_exits.append(sim_exit_pct)

        net_exits = [e - COST_PCT * 100 for e in sim_exits]
        wins = [e for e in net_exits if e > 0]
        sim_results.append({
            'config': label,
            'stop_loss': sl,
            't1_tol': t1t,
            't1_boundary': t1b,
            't2_tol': t2t,
            't2_boundary': t2b,
            't3_tol': t3t,
            'win_rate': len(wins) / len(net_exits) if net_exits else 0,
            'avg_exit': sum(net_exits) / len(net_exits) if net_exits else 0,
            'p50_exit': _pct(sorted(net_exits), 50),
        })

    # Sort sim results by avg_exit descending
    sim_results.sort(key=lambda x: x['avg_exit'], reverse=True)

    return {
        'play_id': play_id,
        'name': play_name,
        'sell_logic': sell_logic,
        'actual': actual_stats,
        'sim': sim_results,
    }


# =============================================================================
# RECOMMENDATIONS
# =============================================================================

def recommend_sell_logic(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Derive a recommended sell_logic based on what the data tells us.

    Key heuristics:
      - Stop-loss should be ≤ p25 of max_gain (don't risk more than median pump)
      - Tier 1 tolerance should be ≤ p25 max_gain (capture small moves)
      - Tier 1 boundary = p50 max_gain (transition point)
      - Tier 2 tolerance should allow riding to p75 max_gain
      - Tier 3 tolerance very tight once we're in big-win territory
    """
    actual = analysis.get('actual', {})
    if not actual or actual.get('n', 0) < 5:
        return {}

    p25_peak = max(0.0005, actual['p25_peak'] / 100)
    p50_peak = max(0.001, actual['p50_peak'] / 100)
    p75_peak = max(0.002, actual['p75_peak'] / 100)

    # Stop-loss: tight enough to limit bleed, loose enough for spread noise
    rec_sl = round(min(max(p25_peak * 0.8, 0.0008), 0.002), 4)

    # Tier 1: below p50 peak — use tolerance equal to ~70% of the boundary
    rec_t1_boundary = round(p50_peak, 4)
    rec_t1_tol = round(min(p25_peak * 1.2, rec_t1_boundary * 0.7), 4)

    # Tier 2: p50–p75 — allow a bit more room but protect gains
    rec_t2_boundary = round(p75_peak, 4)
    rec_t2_tol = round(min(rec_t1_tol * 0.6, 0.002), 4)

    # Tier 3: above p75 — lock in tightly
    rec_t3_tol = round(min(rec_t2_tol * 0.3, 0.0005), 4)

    return {
        "tolerance_rules": {
            "decreases": [
                {"range": [-999999, 0], "tolerance": rec_sl}
            ],
            "increases": [
                {"range": [0.0,              rec_t1_boundary], "tolerance": rec_t1_tol},
                {"range": [rec_t1_boundary,  rec_t2_boundary], "tolerance": rec_t2_tol},
                {"range": [rec_t2_boundary,  1.0],             "tolerance": rec_t3_tol},
            ]
        },
        "_rec_basis": {
            "p25_peak_pct": actual['p25_peak'],
            "p50_peak_pct": actual['p50_peak'],
            "p75_peak_pct": actual['p75_peak'],
        }
    }


# =============================================================================
# PRETTY PRINTING
# =============================================================================

def print_banner(text: str) -> None:
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  {text}")
    print(sep)


def print_analysis(analysis: Dict[str, Any], recommended: Dict[str, Any]) -> None:
    play_id = analysis['play_id']
    name = analysis.get('name', f'Play {play_id}')
    n = analysis.get('actual', {}).get('n', 0)

    if n == 0:
        print(f"\nPlay {play_id}: no closed trades in window")
        return

    a = analysis['actual']
    print_banner(f"PLAY {play_id} — {name}  (n={n})")

    print(f"\n  ACTUAL PERFORMANCE")
    print(f"  {'Win rate':<30} {a['win_rate']*100:.1f}%")
    print(f"  {'Avg exit':<30} {a['avg_exit']:+.4f}%")
    print(f"  {'Avg win / Avg loss':<30} {a['avg_win']:+.4f}% / {a['avg_loss']:+.4f}%")
    print(f"  {'Exit p25/p50/p75':<30} {a['p25_exit']:+.3f}% / {a['p50_exit']:+.3f}% / {a['p75_exit']:+.3f}%")
    print(f"  {'Avg hold time':<30} {a['avg_hold_s']:.0f}s")

    print(f"\n  MAX-GAIN DISTRIBUTION (what the price reached)")
    print(f"  {'Peak p25/p50/p75/p90':<30} {a['p25_peak']:+.3f}% / {a['p50_peak']:+.3f}% / {a['p75_peak']:+.3f}% / {a['p90_peak']:+.3f}%")
    print(f"  {'Avg peak-capture (wins)':<30} {a['avg_peak_capture']*100:.1f}%")

    # The critical "giveback" section
    gb_pct = a['gave_back_pct'] * 100
    severity = "🔴 CRITICAL" if gb_pct > 50 else "🟡 HIGH" if gb_pct > 35 else "🟢 OK"
    print(f"\n  GAIN GIVEBACKS  {severity}")
    print(f"  {'Gave-back trades':<30} {a['gave_back_count']} / {n}  ({gb_pct:.0f}%)")
    print(f"  {'  avg peak (before reversal)':<30} {a['avg_gb_peak']:+.4f}%")
    print(f"  {'  avg exit (after reversal)':<30} {a['avg_gb_exit']:+.4f}%")

    # Current sell logic
    sell = analysis.get('sell_logic', {})
    tr = sell.get('tolerance_rules', {})
    dec = tr.get('decreases', [{}])
    inc = tr.get('increases', [])
    dec_tol = dec[0].get('tolerance', '?') if dec else '?'
    print(f"\n  CURRENT SELL LOGIC")
    print(f"  {'Stop-loss (decreases tol)':<30} {float(dec_tol)*100:.2f}%")
    for i, tier in enumerate(inc):
        rng = tier.get('range', [0, 0])
        tol = tier.get('tolerance', 0)
        print(f"  {'Tier %d gain [%.3f%%-%.3f%%]' % (i+1, float(rng[0])*100, float(rng[1])*100):<30} trail tol = {float(tol)*100:.3f}%")
    mhs = sell.get('min_hold_seconds', 0)
    if mhs:
        print(f"  {'Min hold':<30} {mhs}s")

    # Check: is the tier-1 tolerance bigger than p50 peak? (the core problem)
    if inc:
        t1_tol = float(inc[0].get('tolerance', 0))
        if t1_tol > a['p50_peak'] / 100:
            print(f"\n  ⚠️  DIAGNOSIS: Tier-1 trailing tolerance ({t1_tol*100:.3f}%) is LARGER than p50 peak")
            print(f"      ({a['p50_peak']:.3f}%). Trades peak, then trail stop lets them fall past")
            print(f"      entry before triggering. Stop-loss then fires at a loss.")
        elif dec_tol != '?' and float(dec_tol) >= t1_tol and a['avg_gb_peak'] < float(dec_tol) * 100:
            print(f"\n  ⚠️  DIAGNOSIS: Stop-loss ({float(dec_tol)*100:.3f}%) wider than typical peak")
            print(f"      ({a['avg_gb_peak']:.3f}%). Trades hit stop-loss before trailing stop locks in.")

    # Simulation results
    print(f"\n  TOLERANCE SIMULATION (replayed against historical price paths)")
    print(f"  {'Config':<18} {'SL%':>6} {'T1tol%':>7} {'T1bnd%':>7} {'WinRate':>8} {'AvgExit%':>10}")
    print(f"  {'-'*60}")
    for s in analysis['sim'][:8]:
        marker = " ◀ BEST" if s == analysis['sim'][0] else ""
        print(f"  {s['config']:<18} {s['stop_loss']*100:>6.3f} {s['t1_tol']*100:>7.4f} "
              f"{s['t1_boundary']*100:>7.4f} {s['win_rate']*100:>7.1f}%  {s['avg_exit']:>+9.4f}%{marker}")

    # Recommendation
    if recommended:
        print(f"\n  RECOMMENDED NEW SELL LOGIC")
        basis = recommended.get('_rec_basis', {})
        tr_new = recommended.get('tolerance_rules', {})
        dec_new = tr_new.get('decreases', [{}])[0]
        inc_new = tr_new.get('increases', [])
        print(f"  (based on: p25={basis.get('p25_peak_pct',0):.3f}% p50={basis.get('p50_peak_pct',0):.3f}% p75={basis.get('p75_peak_pct',0):.3f}% peak)")
        print(f"  {'New stop-loss':<30} {float(dec_new.get('tolerance',0))*100:.3f}%  (was {float(dec_tol)*100:.3f}%)")
        for i, tier in enumerate(inc_new):
            rng = tier.get('range', [0, 0])
            tol = tier.get('tolerance', 0)
            old_tol = float(inc[i].get('tolerance', 0)) if i < len(inc) else 0
            delta = float(tol)*100 - old_tol*100
            print(f"  {'New Tier %d [%.3f%%-%.3f%%]' % (i+1, float(rng[0])*100, float(rng[1])*100):<30} "
                  f"trail tol = {float(tol)*100:.3f}%  (was {old_tol*100:.3f}%  Δ={delta:+.3f}%)")


def print_summary_table(analyses: List[Dict[str, Any]]) -> None:
    print_banner("SUMMARY TABLE — ALL PLAYS")
    hdr = f"{'Play':<6} {'Name':<22} {'N':>5} {'WinRate':>8} {'AvgExit':>9} {'p50Exit':>9} {'GiveBack':>9} {'p50Peak':>9} {'AvgHold':>9}"
    print(hdr)
    print("-" * 95)
    for a in analyses:
        if a.get('actual', {}).get('n', 0) == 0:
            continue
        ac = a['actual']
        print(
            f"  {a['play_id']:<4} {a.get('name','')[:20]:<22} {ac['n']:>5} "
            f"{ac['win_rate']*100:>7.1f}% "
            f"{ac['avg_exit']:>+8.4f}% "
            f"{ac['p50_exit']:>+8.4f}% "
            f"{ac['gave_back_pct']*100:>8.0f}% "
            f"{ac['p50_peak']:>+8.4f}% "
            f"{ac['avg_hold_s']:>8.0f}s"
        )


# =============================================================================
# APPLY RECOMMENDATIONS
# =============================================================================

def apply_recommendation(play_id: int, new_sell_logic: Dict[str, Any]) -> bool:
    """Write the recommended sell_logic back to follow_the_goat_plays."""
    # Strip internal metadata before saving
    clean = {k: v for k, v in new_sell_logic.items() if not k.startswith('_')}
    try:
        rows = postgres_execute(
            "UPDATE follow_the_goat_plays SET sell_logic = %s WHERE id = %s",
            [json.dumps(clean), play_id]
        )
        return True
    except Exception as e:
        print(f"  ERROR applying to Play {play_id}: {e}")
        return False


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze play performance and suggest exit improvements")
    parser.add_argument("--days",  type=int, default=7,  help="Lookback window in days (default: 7)")
    parser.add_argument("--plays", type=str, default=None, help="Comma-separated play IDs (default: 3,4,5,6)")
    parser.add_argument("--apply", action="store_true", help="Apply recommended sell_logic to DB")
    args = parser.parse_args()

    play_ids = DEFAULT_PLAY_IDS
    if args.plays:
        play_ids = [int(x.strip()) for x in args.plays.split(",") if x.strip()]

    print_banner(f"PLAY PERFORMANCE ANALYZER  —  last {args.days} days  —  plays {play_ids}")

    # Load data
    print("\nLoading trades from PostgreSQL...")
    trades = load_trades(play_ids, args.days)
    configs = load_play_configs(play_ids)
    print(f"Loaded {len(trades)} closed trades across {len(play_ids)} plays")

    # Quick entry quality check
    print("\n── Entry Quality Check ──────────────────────────────────────────────────")
    total_pumped = sum(1 for t in trades if t['max_gain_pct'] and float(t['max_gain_pct']) >= 0.3)
    total_micro  = sum(1 for t in trades if t['max_gain_pct'] and 0 < float(t['max_gain_pct']) < 0.3)
    total_flat   = sum(1 for t in trades if t['max_gain_pct'] and float(t['max_gain_pct']) <= 0)
    n_total = len(trades)
    print(f"  Proper pump (≥0.3% peak): {total_pumped:5d}  ({total_pumped/max(n_total,1)*100:.1f}%)")
    print(f"  Micro move  (0–0.3%):     {total_micro:5d}  ({total_micro/max(n_total,1)*100:.1f}%)")
    print(f"  Flat/down   (≤0%):        {total_flat:5d}  ({total_flat/max(n_total,1)*100:.1f}%)")

    # Per-play analysis
    analyses = []
    recommendations = {}

    for play_id in play_ids:
        cfg = configs.get(play_id, {})
        analysis = analyze_play(
            trades, play_id,
            cfg.get('name', f'Play {play_id}'),
            cfg.get('sell_logic', {}),
            SIM_CONFIGS,
        )
        recommended = recommend_sell_logic(analysis)
        analyses.append(analysis)
        recommendations[play_id] = recommended
        print_analysis(analysis, recommended)

    # Summary table
    print_summary_table(analyses)

    # Apply recommendations
    if args.apply:
        print_banner("APPLYING RECOMMENDATIONS TO DATABASE")
        for play_id, rec in recommendations.items():
            if not rec:
                print(f"  Play {play_id}: skipped (insufficient data)")
                continue
            ok = apply_recommendation(play_id, rec)
            status = "✓ applied" if ok else "✗ failed"
            print(f"  Play {play_id}: {status}")
        print("\nRecommendations applied. Trailing stop seller will pick up changes within 60s (cache TTL).")
    else:
        print("\nRun with --apply to write recommended sell_logic to the database.")

    print()


if __name__ == "__main__":
    main()

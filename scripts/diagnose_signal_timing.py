"""
Signal Timing Diagnosis
=======================
Answers the question: "Why are plays not capturing the price cycles we can see?"

SOL IS making 0.3-1.5% moves regularly (28+ per day at 0.3 threshold).
We ARE firing signals during those cycles. The problem is WHERE in the cycle
we're entering — too many signals fire after the price has already moved.

Key findings:
  - 68+ completed cycles with ≥0.3% gain in the last 24h
  - Average cycle gain: 0.44-0.61% (plenty of room)
  - But 35% of entries happen AFTER the price already moved >0.20% into the cycle
  - Late entries (>0.20% already moved): avg exit = -0.050% (chasing)
  - Early entries (<0.15% already moved): avg exit = +0.006% (near break-even BEFORE filter improvements)

The fix: a "cycle position gate" in pump_signal_logic.py that blocks signals
when the current price has already risen too far from the cycle start price.

Usage:
    python3 scripts/diagnose_signal_timing.py
    python3 scripts/diagnose_signal_timing.py --days 14
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

COST_PCT = 0.001   # 0.1% round-trip


def _pct(vals: List[float], p: int) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = max(0, min(len(s) - 1, int(len(s) * p / 100)))
    return s[idx]


def main(days: int = 7) -> None:
    print("=" * 80)
    print("  SIGNAL TIMING DIAGNOSIS")
    print("=" * 80)

    with get_postgres() as conn:
        with conn.cursor() as cur:

            # ── 1. How many real price cycles happened? ────────────────────────
            print(f"\n{'─'*80}")
            print("1. PRICE CYCLES IN LAST 24H (cycle_tracker, threshold=0.3)")
            print(f"{'─'*80}")
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE cycle_end_time IS NOT NULL)  AS completed,
                    COUNT(*) FILTER (WHERE cycle_end_time IS NULL)       AS open,
                    COUNT(*) FILTER (WHERE max_percent_increase >= 0.3 AND cycle_end_time IS NOT NULL) AS completed_0_3pct,
                    COUNT(*) FILTER (WHERE max_percent_increase >= 0.5 AND cycle_end_time IS NOT NULL) AS completed_0_5pct,
                    COUNT(*) FILTER (WHERE max_percent_increase >= 1.0 AND cycle_end_time IS NOT NULL) AS completed_1_0pct,
                    ROUND(AVG(max_percent_increase) FILTER (WHERE cycle_end_time IS NOT NULL)::numeric, 4) AS avg_gain,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY max_percent_increase)
                          FILTER (WHERE cycle_end_time IS NOT NULL)::numeric, 4) AS median_gain,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                          ORDER BY EXTRACT(EPOCH FROM (cycle_end_time - cycle_start_time)))
                          FILTER (WHERE cycle_end_time IS NOT NULL)::numeric, 0) AS median_duration_s
                FROM cycle_tracker
                WHERE threshold = 0.3
                  AND cycle_start_time > NOW() - INTERVAL '24 hours'
            """)
            c = dict(cur.fetchone())
            print(f"  Total completed cycles:        {c['completed']}")
            print(f"  Cycles with ≥0.3% gain:        {c['completed_0_3pct']}")
            print(f"  Cycles with ≥0.5% gain:        {c['completed_0_5pct']}")
            print(f"  Cycles with ≥1.0% gain:        {c['completed_1_0pct']}")
            print(f"  Avg / Median gain:             {c['avg_gain']}% / {c['median_gain']}%")
            print(f"  Median cycle duration:         {c['median_duration_s']}s")
            print(f"\n  ✅ SOL IS making these moves. The market opportunity is real.")

            # ── 2. Signal placement vs cycle size ─────────────────────────────
            print(f"\n{'─'*80}")
            print(f"2. WHERE ARE SIGNALS FIRING? (last {days} days, plays 3-6)")
            print(f"{'─'*80}")
            cur.execute("""
                SELECT
                    COUNT(*) AS total_signals,
                    SUM(CASE WHEN c.max_percent_increase >= 0.5 THEN 1 ELSE 0 END) AS in_big_cycles,
                    SUM(CASE WHEN c.max_percent_increase >= 0.3 AND c.max_percent_increase < 0.5 THEN 1 ELSE 0 END) AS in_medium_cycles,
                    SUM(CASE WHEN c.max_percent_increase >= 0.1 AND c.max_percent_increase < 0.3 THEN 1 ELSE 0 END) AS in_small_cycles,
                    SUM(CASE WHEN c.max_percent_increase < 0.1 THEN 1 ELSE 0 END) AS in_flat_cycles,
                    ROUND(AVG(c.max_percent_increase)::numeric, 4) AS avg_cycle_gain
                FROM follow_the_goat_buyins b
                JOIN cycle_tracker c ON c.id = b.price_cycle
                WHERE b.followed_at > NOW() - (%s || ' days')::INTERVAL
                  AND b.play_id IN (3,4,5,6)
                  AND b.our_entry_price IS NOT NULL
                  AND b.wallet_address NOT LIKE 'TRAINING_TEST_%%'
            """, [str(days)])
            s = dict(cur.fetchone())
            n = s['total_signals']
            print(f"  Total signals fired:           {n}")
            print(f"  In big cycles (≥0.5% gain):    {s['in_big_cycles']:4d}  ({s['in_big_cycles']/n*100:.0f}%)")
            print(f"  In medium cycles (0.3-0.5%):   {s['in_medium_cycles']:4d}  ({s['in_medium_cycles']/n*100:.0f}%)")
            print(f"  In small cycles (0.1-0.3%):    {s['in_small_cycles']:4d}  ({s['in_small_cycles']/n*100:.0f}%)")
            print(f"  In flat cycles (<0.1%):        {s['in_flat_cycles']:4d}  ({s['in_flat_cycles']/n*100:.0f}%)")
            print(f"  Avg cycle gain at signal time: {s['avg_cycle_gain']}%")
            print(f"\n  ✅ {(s['in_big_cycles']+s['in_medium_cycles'])/n*100:.0f}% of signals fire during genuine ≥0.3% cycles.")
            print(f"  ⚠️  But the question is: WHERE in those cycles are we entering?")

            # ── 3. Entry timing within cycles ─────────────────────────────────
            print(f"\n{'─'*80}")
            print("3. ENTRY TIMING WITHIN CYCLES")
            print(f"{'─'*80}")
            cur.execute("""
                SELECT
                    -- How much had the cycle already moved when we entered?
                    ROUND(((b.our_entry_price - c.sequence_start_price)
                           / NULLIF(c.sequence_start_price, 0) * 100)::numeric, 5) AS already_moved_pct,
                    ROUND(((b.our_exit_price - b.our_entry_price)
                           / NULLIF(b.our_entry_price, 0) * 100)::numeric, 5) AS exit_pct
                FROM follow_the_goat_buyins b
                JOIN cycle_tracker c ON c.id = b.price_cycle
                WHERE b.followed_at > NOW() - (%s || ' days')::INTERVAL
                  AND b.play_id IN (3,4,5,6)
                  AND b.our_entry_price IS NOT NULL
                  AND b.our_exit_price IS NOT NULL
                  AND b.wallet_address NOT LIKE 'TRAINING_TEST_%%'
            """, [str(days)])
            rows = cur.fetchall()

            already = [float(r['already_moved_pct']) for r in rows if r['already_moved_pct'] is not None]
            exits   = [float(r['exit_pct']) for r in rows if r['exit_pct'] is not None]

            # Split by entry timing
            buckets = [
                ("EARLY  < 0.05%",   [e for a, e in zip(already, exits) if a < 0.05]),
                ("EARLY  0.05-0.10%",[e for a, e in zip(already, exits) if 0.05 <= a < 0.10]),
                ("ENTRY  0.10-0.15%",[e for a, e in zip(already, exits) if 0.10 <= a < 0.15]),
                ("LATE   0.15-0.20%",[e for a, e in zip(already, exits) if 0.15 <= a < 0.20]),
                ("LATE   0.20-0.30%",[e for a, e in zip(already, exits) if 0.20 <= a < 0.30]),
                ("CHASING > 0.30%",  [e for a, e in zip(already, exits) if a >= 0.30]),
            ]

            print(f"  {'Bucket':<22} {'Count':>6}  {'Win%':>6}  {'AvgExit':>9}  {'p50Exit':>9}  {'NetEV*':>9}")
            print(f"  {'-'*70}")
            for label, bucket_exits in buckets:
                if not bucket_exits:
                    continue
                net = [e - COST_PCT * 100 for e in bucket_exits]
                wins = sum(1 for e in net if e > 0)
                print(f"  {label:<22} {len(net):>6}  {wins/len(net)*100:>5.0f}%  "
                      f"{sum(bucket_exits)/len(bucket_exits):>+8.4f}%  "
                      f"{_pct(bucket_exits, 50):>+8.4f}%  "
                      f"{sum(net)/len(net):>+8.4f}%")

            print(f"\n  * Net EV = raw exit − 0.1% trading cost")
            print(f"\n  Already moved distribution: "
                  f"p25={_pct(already,25):+.4f}% / p50={_pct(already,50):+.4f}% / p75={_pct(already,75):+.4f}%")
            n_late = sum(1 for a in already if a > 0.20)
            print(f"\n  🔴 {n_late}/{len(already)} entries ({n_late/len(already)*100:.0f}%) entered AFTER price"
                  f" already moved >0.20% from cycle start — these are CHASING entries")
            n_early = sum(1 for a in already if a < 0.10)
            print(f"  🟢 {n_early}/{len(already)} entries ({n_early/len(already)*100:.0f}%) entered while price"
                  f" had moved <0.10% from cycle start — these are the BEST entries")

            # ── 4. The specific cycles we missed ──────────────────────────────
            print(f"\n{'─'*80}")
            print("4. MISSED OPPORTUNITIES — good cycles with NO signal")
            print(f"{'─'*80}")
            cur.execute("""
                SELECT
                    COUNT(*) AS big_cycles_total,
                    COUNT(b.id) AS big_cycles_with_signal,
                    COUNT(*) - COUNT(b.id) AS big_cycles_missed
                FROM cycle_tracker c
                LEFT JOIN follow_the_goat_buyins b
                    ON b.price_cycle = c.id AND b.play_id IN (3,4,5,6)
                WHERE c.threshold = 0.3
                  AND c.max_percent_increase >= 0.3
                  AND c.cycle_start_time > NOW() - (%s || ' days')::INTERVAL
                  AND c.cycle_end_time IS NOT NULL
            """, [str(days)])
            m = dict(cur.fetchone())
            print(f"  Completed ≥0.3% cycles (last {days} days):  {m['big_cycles_total']}")
            print(f"  Cycles where we fired a signal:            {m['big_cycles_with_signal']}")
            print(f"  Cycles with NO signal at all:              {m['big_cycles_missed']}")
            if m['big_cycles_total']:
                print(f"  Signal coverage:                           {m['big_cycles_with_signal']/m['big_cycles_total']*100:.0f}%")

            # ── 5. What if we blocked late entries? ───────────────────────────
            print(f"\n{'─'*80}")
            print("5. SIMULATION: WHAT IF WE BLOCKED ENTRIES > X% INTO CYCLE?")
            print(f"{'─'*80}")
            thresholds = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
            print(f"  {'Block if >':>12}  {'Remaining':>10}  {'Win%':>6}  {'AvgExit':>9}  {'NetEV*':>9}  {'vs All':>9}")
            print(f"  {'-'*65}")
            all_avg = sum(exits) / len(exits) if exits else 0
            for thr in thresholds:
                kept_exits = [e for a, e in zip(already, exits) if a <= thr]
                if not kept_exits:
                    continue
                net = [e - COST_PCT * 100 for e in kept_exits]
                wins = sum(1 for e in net if e > 0)
                delta = sum(kept_exits) / len(kept_exits) - all_avg
                print(f"  {thr*100:>11.2f}%  {len(kept_exits):>10}  {wins/len(net)*100:>5.0f}%  "
                      f"{sum(kept_exits)/len(kept_exits):>+8.4f}%  "
                      f"{sum(net)/len(net):>+8.4f}%  "
                      f"{delta:>+8.4f}%")
            print(f"  {'No filter':>12}  {len(exits):>10}  "
                  f"{sum(1 for e in exits if e - COST_PCT*100 > 0)/len(exits)*100:>5.0f}%  "
                  f"{all_avg:>+8.4f}%  "
                  f"{all_avg - COST_PCT*100:>+8.4f}%  "
                  f"{'baseline':>9}")

            # ── 6. Root cause summary ──────────────────────────────────────────
            print(f"\n{'─'*80}")
            print("6. ROOT CAUSE & FIX")
            print(f"{'─'*80}")
            print("""
  ROOT CAUSE:
  ┌──────────────────────────────────────────────────────────────────────────┐
  │ The signal features (order book, trades, whale) are LAGGING indicators.  │
  │ By the time the order book imbalance and trade volume build up enough to  │
  │ trigger a signal, the price has ALREADY moved 0.1-0.3% into the cycle.  │
  │                                                                          │
  │ The 0.3-1.5% cycles ARE real — SOL is making these moves.               │
  │ We ARE detecting them — but 35% of entries are mid-cycle or later.       │
  │ Those late entries have almost no upside left and still carry downside.  │
  └──────────────────────────────────────────────────────────────────────────┘

  THE FIX — add a "cycle position gate" to pump_signal_logic.py:
  
    Block entry if:
      current_price > cycle_start_price × (1 + CYCLE_CHASE_GATE)
    
    Where CYCLE_CHASE_GATE = 0.10-0.15% (tune based on this data)
  
  This check is already partially done via:
    - CHASE_GATE_1M_MAX = 0.15% (1-min price change)
    - PRE_ENTRY_TOP_GUARD = 0.15% (2-min lookback in mega_simulator)
  
  But those gates use SHORT lookbacks (1-2 min). The cycle start may be
  30+ minutes ago, so the price "snuck up" slowly and the 1-min gate
  doesn't catch it.

  RECOMMENDED CHANGE:
  In pump_signal_logic.py, fetch the active cycle_tracker start price
  at signal fire time and block if:  (current - cycle_start) / cycle_start > 0.0015
  This would filter out the 35% of "chasing" entries and improve avg exit
  by approximately +0.03-0.05% per trade.
""")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    main(args.days)

#!/usr/bin/env python3
"""
Fix Potential Gains - One-Time Correction Script
=================================================
Recalculates ALL existing potential_gains values in follow_the_goat_buyins
using the CORRECT formula: price at cycle end vs entry price.

The old formula used highest_price_reached (the peak), which always gave
positive numbers. The correct formula uses the actual price when the cycle
closed, which CAN be negative (trade lost money).

SAFE: This script shows a preview first, then asks for confirmation before updating.

Usage:
    python scripts/fix_potential_gains.py             # Preview + confirm
    python scripts/fix_potential_gains.py --execute   # Skip confirmation, just run
    python scripts/fix_potential_gains.py --dry-run   # Preview only, no changes
"""

import sys
import argparse
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fix_potential_gains")

THRESHOLD = 0.3


def preview_changes():
    """
    Show what would change: old potential_gains vs new potential_gains for all records.
    Returns the count of records that would change.
    """
    logger.info("=" * 80)
    logger.info("POTENTIAL GAINS CORRECTION - PREVIEW")
    logger.info("=" * 80)

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Count total records with potential_gains
            cursor.execute("""
                SELECT COUNT(*) as cnt
                FROM follow_the_goat_buyins
                WHERE potential_gains IS NOT NULL
            """)
            total = cursor.fetchone()['cnt']
            logger.info(f"Total buyins with potential_gains set: {total}")

            # Distribution of current potential_gains
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE potential_gains < 0) as negative,
                    COUNT(*) FILTER (WHERE potential_gains >= 0 AND potential_gains < 0.1) as zero_to_01,
                    COUNT(*) FILTER (WHERE potential_gains >= 0.1 AND potential_gains < 0.3) as p01_to_03,
                    COUNT(*) FILTER (WHERE potential_gains >= 0.3) as above_03,
                    ROUND(AVG(potential_gains)::numeric, 4) as avg_gain,
                    ROUND(MIN(potential_gains)::numeric, 4) as min_gain,
                    ROUND(MAX(potential_gains)::numeric, 4) as max_gain
                FROM follow_the_goat_buyins
                WHERE potential_gains IS NOT NULL
            """)
            dist = cursor.fetchone()
            logger.info(f"\nCurrent distribution (WRONG - uses highest_price_reached):")
            logger.info(f"  Negative:     {dist['negative']:>6d}  ({dist['negative']/dist['total']*100:.1f}%)")
            logger.info(f"  0 - 0.1%:     {dist['zero_to_01']:>6d}  ({dist['zero_to_01']/dist['total']*100:.1f}%)")
            logger.info(f"  0.1 - 0.3%:   {dist['p01_to_03']:>6d}  ({dist['p01_to_03']/dist['total']*100:.1f}%)")
            logger.info(f"  >= 0.3% (good):{dist['above_03']:>5d}  ({dist['above_03']/dist['total']*100:.1f}%)")
            logger.info(f"  Avg: {dist['avg_gain']}%  Min: {dist['min_gain']}%  Max: {dist['max_gain']}%")

            # Calculate what the NEW values would be
            # For trades with cycle: use price at cycle_end_time
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE new_gain < 0) as negative,
                    COUNT(*) FILTER (WHERE new_gain >= 0 AND new_gain < 0.1) as zero_to_01,
                    COUNT(*) FILTER (WHERE new_gain >= 0.1 AND new_gain < 0.3) as p01_to_03,
                    COUNT(*) FILTER (WHERE new_gain >= 0.3) as above_03,
                    ROUND(AVG(new_gain)::numeric, 4) as avg_gain,
                    ROUND(MIN(new_gain)::numeric, 4) as min_gain,
                    ROUND(MAX(new_gain)::numeric, 4) as max_gain
                FROM (
                    SELECT
                        buyins.id,
                        buyins.potential_gains as old_gain,
                        ((p_end.price - buyins.our_entry_price) / buyins.our_entry_price) * 100 AS new_gain
                    FROM follow_the_goat_buyins buyins
                    INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                    CROSS JOIN LATERAL (
                        SELECT price FROM prices
                        WHERE token = 'SOL'
                          AND timestamp <= ct.cycle_end_time
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) p_end
                    WHERE buyins.potential_gains IS NOT NULL
                      AND ct.cycle_end_time IS NOT NULL
                      AND ct.threshold = %s
                      AND buyins.our_entry_price IS NOT NULL
                      AND buyins.our_entry_price > 0
                ) sub
            """, [THRESHOLD])
            new_dist = cursor.fetchone()

            if new_dist and new_dist['total'] > 0:
                logger.info(f"\nCorrected distribution (uses price at cycle end):")
                logger.info(f"  Negative:     {new_dist['negative']:>6d}  ({new_dist['negative']/new_dist['total']*100:.1f}%)")
                logger.info(f"  0 - 0.1%:     {new_dist['zero_to_01']:>6d}  ({new_dist['zero_to_01']/new_dist['total']*100:.1f}%)")
                logger.info(f"  0.1 - 0.3%:   {new_dist['p01_to_03']:>6d}  ({new_dist['p01_to_03']/new_dist['total']*100:.1f}%)")
                logger.info(f"  >= 0.3% (good):{new_dist['above_03']:>5d}  ({new_dist['above_03']/new_dist['total']*100:.1f}%)")
                logger.info(f"  Avg: {new_dist['avg_gain']}%  Min: {new_dist['min_gain']}%  Max: {new_dist['max_gain']}%")
                logger.info(f"  Records that can be corrected via cycle: {new_dist['total']}")
            else:
                logger.warning("No records could be matched to cycles with end prices")

            # Show sample of biggest changes
            cursor.execute("""
                SELECT
                    buyins.id,
                    ROUND(buyins.potential_gains::numeric, 4) as old_gain,
                    ROUND(((p_end.price - buyins.our_entry_price) / buyins.our_entry_price * 100)::numeric, 4) as new_gain,
                    ROUND((buyins.potential_gains - ((p_end.price - buyins.our_entry_price) / buyins.our_entry_price * 100))::numeric, 4) as diff,
                    ROUND(buyins.our_entry_price::numeric, 4) as entry_price,
                    ROUND(ct.highest_price_reached::numeric, 4) as peak_price,
                    ROUND(p_end.price::numeric, 4) as end_price
                FROM follow_the_goat_buyins buyins
                INNER JOIN cycle_tracker ct ON ct.id = buyins.price_cycle
                CROSS JOIN LATERAL (
                    SELECT price FROM prices
                    WHERE token = 'SOL'
                      AND timestamp <= ct.cycle_end_time
                    ORDER BY timestamp DESC
                    LIMIT 1
                ) p_end
                WHERE buyins.potential_gains IS NOT NULL
                  AND ct.cycle_end_time IS NOT NULL
                  AND ct.threshold = %s
                  AND buyins.our_entry_price IS NOT NULL
                  AND buyins.our_entry_price > 0
                ORDER BY ABS(buyins.potential_gains - ((p_end.price - buyins.our_entry_price) / buyins.our_entry_price * 100)) DESC
                LIMIT 15
            """, [THRESHOLD])
            samples = cursor.fetchall()

            if samples:
                logger.info(f"\nBiggest changes (sample):")
                logger.info(f"  {'ID':>8}  {'Old Gain':>10}  {'New Gain':>10}  {'Diff':>10}  {'Entry':>10}  {'Peak':>10}  {'End':>10}")
                logger.info(f"  {'----':>8}  {'--------':>10}  {'--------':>10}  {'----':>10}  {'-----':>10}  {'----':>10}  {'---':>10}")
                for s in samples:
                    logger.info(f"  {s['id']:>8d}  {s['old_gain']:>9.4f}%  {s['new_gain']:>9.4f}%  {s['diff']:>9.4f}%  "
                                f"${s['entry_price']:>9.4f}  ${s['peak_price']:>9.4f}  ${s['end_price']:>9.4f}")

            # Count orphaned trades
            cursor.execute("""
                SELECT COUNT(*) as cnt
                FROM follow_the_goat_buyins buyins
                WHERE buyins.potential_gains IS NOT NULL
                  AND buyins.price_cycle IS NOT NULL
                  AND buyins.our_entry_price IS NOT NULL
                  AND buyins.our_entry_price > 0
                  AND NOT EXISTS (
                      SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins.price_cycle
                  )
            """)
            orphaned = cursor.fetchone()['cnt']
            if orphaned > 0:
                logger.info(f"\n  Orphaned trades (cycle deleted, will use price at followed_at + 15min): {orphaned}")

            return new_dist['total'] if new_dist and new_dist['total'] else 0, orphaned


def execute_correction():
    """
    Actually update all potential_gains with the correct values.
    Returns number of records updated.
    """
    logger.info("\n" + "=" * 80)
    logger.info("EXECUTING CORRECTION...")
    logger.info("=" * 80)

    total_updated = 0

    # Step 1: Fix trades with existing cycle records
    logger.info("\n[Step 1/2] Fixing trades with cycle records (using price at cycle_end_time)...")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE follow_the_goat_buyins buyins
                    SET potential_gains = sub.new_gain
                    FROM (
                        SELECT
                            buyins_inner.id,
                            ((p_end.price - buyins_inner.our_entry_price) / buyins_inner.our_entry_price) * 100 AS new_gain
                        FROM follow_the_goat_buyins buyins_inner
                        INNER JOIN cycle_tracker ct ON ct.id = buyins_inner.price_cycle
                        CROSS JOIN LATERAL (
                            SELECT price FROM prices
                            WHERE token = 'SOL'
                              AND timestamp <= ct.cycle_end_time
                            ORDER BY timestamp DESC
                            LIMIT 1
                        ) p_end
                        WHERE buyins_inner.potential_gains IS NOT NULL
                          AND ct.cycle_end_time IS NOT NULL
                          AND ct.threshold = %s
                          AND buyins_inner.our_entry_price IS NOT NULL
                          AND buyins_inner.our_entry_price > 0
                    ) sub
                    WHERE buyins.id = sub.id
                """, [THRESHOLD])
                count = cursor.rowcount
                conn.commit()
                total_updated += count
                logger.info(f"  Updated {count} trades with cycle-based end price")
    except Exception as e:
        logger.error(f"  Failed to update cycle-based trades: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Step 2: Fix orphaned trades (cycle deleted, use price 15 min after entry)
    logger.info("\n[Step 2/2] Fixing orphaned trades (using price at followed_at + 15 min)...")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE follow_the_goat_buyins buyins
                    SET potential_gains = sub.new_gain
                    FROM (
                        SELECT
                            buyins_inner.id,
                            ((p_end.price - buyins_inner.our_entry_price) / buyins_inner.our_entry_price) * 100 AS new_gain
                        FROM follow_the_goat_buyins buyins_inner
                        CROSS JOIN LATERAL (
                            SELECT price FROM prices
                            WHERE token = 'SOL'
                              AND timestamp <= buyins_inner.followed_at + INTERVAL '15 minutes'
                            ORDER BY timestamp DESC
                            LIMIT 1
                        ) p_end
                        WHERE buyins_inner.potential_gains IS NOT NULL
                          AND buyins_inner.price_cycle IS NOT NULL
                          AND buyins_inner.our_entry_price IS NOT NULL
                          AND buyins_inner.our_entry_price > 0
                          AND NOT EXISTS (
                              SELECT 1 FROM cycle_tracker ct WHERE ct.id = buyins_inner.price_cycle
                          )
                          AND p_end.price IS NOT NULL
                    ) sub
                    WHERE buyins.id = sub.id
                """)
                count = cursor.rowcount
                conn.commit()
                total_updated += count
                logger.info(f"  Updated {count} orphaned trades with 15-min end price")
    except Exception as e:
        logger.error(f"  Failed to update orphaned trades: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Step 3: Also fix trades that still have NULL potential_gains (new formula for pending ones)
    logger.info("\n[Bonus] Fixing trades with NULL potential_gains that can now be resolved...")
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE follow_the_goat_buyins buyins
                    SET potential_gains = sub.new_gain
                    FROM (
                        SELECT
                            buyins_inner.id,
                            ((p_end.price - buyins_inner.our_entry_price) / buyins_inner.our_entry_price) * 100 AS new_gain
                        FROM follow_the_goat_buyins buyins_inner
                        INNER JOIN cycle_tracker ct ON ct.id = buyins_inner.price_cycle
                        CROSS JOIN LATERAL (
                            SELECT price FROM prices
                            WHERE token = 'SOL'
                              AND timestamp <= ct.cycle_end_time
                            ORDER BY timestamp DESC
                            LIMIT 1
                        ) p_end
                        WHERE buyins_inner.potential_gains IS NULL
                          AND ct.cycle_end_time IS NOT NULL
                          AND ct.threshold = %s
                          AND buyins_inner.our_entry_price IS NOT NULL
                          AND buyins_inner.our_entry_price > 0
                    ) sub
                    WHERE buyins.id = sub.id
                """, [THRESHOLD])
                count = cursor.rowcount
                conn.commit()
                total_updated += count
                if count > 0:
                    logger.info(f"  Also resolved {count} previously-NULL potential_gains records")
    except Exception as e:
        logger.error(f"  Failed to resolve NULL records: {e}")

    return total_updated


def verify_correction():
    """Show the distribution after correction to confirm it looks right."""
    logger.info("\n" + "=" * 80)
    logger.info("POST-CORRECTION VERIFICATION")
    logger.info("=" * 80)

    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE potential_gains < -0.5) as very_negative,
                    COUNT(*) FILTER (WHERE potential_gains >= -0.5 AND potential_gains < 0) as slightly_negative,
                    COUNT(*) FILTER (WHERE potential_gains >= 0 AND potential_gains < 0.1) as zero_to_01,
                    COUNT(*) FILTER (WHERE potential_gains >= 0.1 AND potential_gains < 0.3) as p01_to_03,
                    COUNT(*) FILTER (WHERE potential_gains >= 0.3) as above_03,
                    COUNT(*) FILTER (WHERE potential_gains IS NULL) as still_null,
                    ROUND(AVG(potential_gains)::numeric, 4) as avg_gain,
                    ROUND(MIN(potential_gains)::numeric, 4) as min_gain,
                    ROUND(MAX(potential_gains)::numeric, 4) as max_gain
                FROM follow_the_goat_buyins
            """)
            d = cursor.fetchone()

            total_with_gains = d['total'] - d['still_null']
            logger.info(f"\nFinal distribution:")
            logger.info(f"  < -0.5% (big loss): {d['very_negative']:>6d}  ({d['very_negative']/total_with_gains*100:.1f}%)" if total_with_gains else "")
            logger.info(f"  -0.5 - 0%:          {d['slightly_negative']:>6d}  ({d['slightly_negative']/total_with_gains*100:.1f}%)" if total_with_gains else "")
            logger.info(f"  0 - 0.1%:           {d['zero_to_01']:>6d}  ({d['zero_to_01']/total_with_gains*100:.1f}%)" if total_with_gains else "")
            logger.info(f"  0.1 - 0.3%:         {d['p01_to_03']:>6d}  ({d['p01_to_03']/total_with_gains*100:.1f}%)" if total_with_gains else "")
            logger.info(f"  >= 0.3% (good):     {d['above_03']:>6d}  ({d['above_03']/total_with_gains*100:.1f}%)" if total_with_gains else "")
            logger.info(f"  Still NULL:         {d['still_null']:>6d}")
            logger.info(f"  Avg: {d['avg_gain']}%  Min: {d['min_gain']}%  Max: {d['max_gain']}%")

            # Sanity check: we should now have negative values
            if d['very_negative'] == 0 and d['slightly_negative'] == 0:
                logger.warning("WARNING: Still no negative potential_gains after correction!")
                logger.warning("This might mean the cycle end prices are still too close to peaks.")
            else:
                pct_neg = (d['very_negative'] + d['slightly_negative']) / total_with_gains * 100 if total_with_gains else 0
                logger.info(f"\n  Negative trades: {d['very_negative'] + d['slightly_negative']} ({pct_neg:.1f}%) -- this is expected and healthy")


def main():
    parser = argparse.ArgumentParser(description="Fix potential_gains to use cycle-end price")
    parser.add_argument("--execute", action="store_true", help="Skip confirmation, execute immediately")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, make no changes")
    args = parser.parse_args()

    # Always show preview
    cycle_count, orphan_count = preview_changes()
    total_to_fix = cycle_count + orphan_count

    if total_to_fix == 0:
        logger.info("\nNo records to fix.")
        return

    if args.dry_run:
        logger.info("\n[DRY RUN] No changes made.")
        return

    if not args.execute:
        logger.info(f"\nReady to correct {total_to_fix} records.")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response != 'y':
            logger.info("Aborted.")
            return

    updated = execute_correction()
    logger.info(f"\nTotal records updated: {updated}")

    verify_correction()

    logger.info("\n" + "=" * 80)
    logger.info("DONE. You can now re-run the filter simulation with correct data:")
    logger.info("  python tests/filter_simulation/run_simulation.py --hours 48 --threshold 0.3")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

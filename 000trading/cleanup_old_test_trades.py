"""
Cleanup Old Test Trades
========================
Automatically cancels test (training) trades that have been pending for too long.

This prevents the trailing stop monitor from being overwhelmed by thousands
of old test trades that never got sold.

Configuration:
- TEST_TRADE_MAX_AGE_HOURS: Cancel test trades older than this (default: 6 hours)
- LIVE_TRADE_MAX_AGE_HOURS: Cancel live trades older than this (default: 72 hours)

Usage:
    # Run once
    python 000trading/cleanup_old_test_trades.py
    
    # Run with custom age threshold
    python 000trading/cleanup_old_test_trades.py --max-age-hours 12
    
    # Scheduled via master2.py (runs every 1 hour)
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import postgres_execute, get_postgres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("cleanup_old_test_trades")

# Default configuration
DEFAULT_TEST_TRADE_MAX_AGE_HOURS = float(os.getenv('TEST_TRADE_MAX_AGE_HOURS', '6'))
DEFAULT_LIVE_TRADE_MAX_AGE_HOURS = float(os.getenv('LIVE_TRADE_MAX_AGE_HOURS', '72'))


def cleanup_old_trades(
    test_max_age_hours: float = None,
    live_max_age_hours: float = None,
    dry_run: bool = False
) -> dict:
    """
    Cancel pending trades that are older than the specified age.
    
    Args:
        test_max_age_hours: Max age for test trades (default: 6 hours)
        live_max_age_hours: Max age for live trades (default: 72 hours)
        dry_run: If True, don't actually cancel, just report what would be done
        
    Returns:
        Dictionary with cleanup statistics
    """
    if test_max_age_hours is None:
        test_max_age_hours = DEFAULT_TEST_TRADE_MAX_AGE_HOURS
    if live_max_age_hours is None:
        live_max_age_hours = DEFAULT_LIVE_TRADE_MAX_AGE_HOURS
    
    test_cutoff = datetime.now() - timedelta(hours=test_max_age_hours)
    live_cutoff = datetime.now() - timedelta(hours=live_max_age_hours)
    
    stats = {
        'test_trades_cancelled': 0,
        'live_trades_cancelled': 0,
        'total_cancelled': 0,
        'test_cutoff': test_cutoff,
        'live_cutoff': live_cutoff,
        'dry_run': dry_run
    }
    
    logger.info("=" * 60)
    logger.info(f"{'DRY RUN: ' if dry_run else ''}Cleanup Old Pending Trades")
    logger.info("=" * 60)
    logger.info(f"Test trade cutoff: {test_cutoff} ({test_max_age_hours}h ago)")
    logger.info(f"Live trade cutoff: {live_cutoff} ({live_max_age_hours}h ago)")
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Count test trades to cancel
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'pending'
                    AND live_trade = 0
                    AND followed_at < %s
                """, [test_cutoff])
                test_count = cursor.fetchone()['count']
                
                # Count live trades to cancel
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'pending'
                    AND live_trade = 1
                    AND followed_at < %s
                """, [live_cutoff])
                live_count = cursor.fetchone()['count']
                
                logger.info(f"Found {test_count} old test trade(s) to cancel")
                logger.info(f"Found {live_count} old live trade(s) to cancel")
                
                if dry_run:
                    logger.info("DRY RUN: Not executing cancellations")
                    stats['test_trades_cancelled'] = test_count
                    stats['live_trades_cancelled'] = live_count
                    stats['total_cancelled'] = test_count + live_count
                    return stats
                
                # Cancel old test trades
                if test_count > 0:
                    rows_affected = postgres_execute("""
                        UPDATE follow_the_goat_buyins
                        SET our_status = 'cancelled',
                            our_exit_timestamp = NOW(),
                            our_profit_loss = 0
                        WHERE our_status = 'pending'
                        AND live_trade = 0
                        AND followed_at < %s
                    """, [test_cutoff])
                    stats['test_trades_cancelled'] = rows_affected
                    logger.info(f"✓ Cancelled {rows_affected} old test trade(s)")
                
                # Cancel old live trades (if any)
                if live_count > 0:
                    rows_affected = postgres_execute("""
                        UPDATE follow_the_goat_buyins
                        SET our_status = 'cancelled',
                            our_exit_timestamp = NOW(),
                            our_profit_loss = 0
                        WHERE our_status = 'pending'
                        AND live_trade = 1
                        AND followed_at < %s
                    """, [live_cutoff])
                    stats['live_trades_cancelled'] = rows_affected
                    logger.info(f"✓ Cancelled {rows_affected} old live trade(s)")
                
                stats['total_cancelled'] = stats['test_trades_cancelled'] + stats['live_trades_cancelled']
                
                # Show remaining active trades
                cursor.execute("""
                    SELECT 
                        live_trade,
                        COUNT(*) as count
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'pending'
                    GROUP BY live_trade
                """)
                remaining = cursor.fetchall()
                
                logger.info("=" * 60)
                logger.info("Remaining active trades:")
                for row in remaining:
                    trade_type = 'LIVE' if row['live_trade'] else 'TEST'
                    logger.info(f"  {trade_type}: {row['count']}")
                logger.info("=" * 60)
                
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)
        raise
    
    return stats


def run_cleanup_job():
    """Main entry point for scheduled job (called by master2.py)."""
    try:
        stats = cleanup_old_trades(dry_run=False)
        if stats['total_cancelled'] > 0:
            logger.info(f"Cleanup complete: Cancelled {stats['total_cancelled']} old trade(s)")
        return stats
    except Exception as e:
        logger.error(f"Cleanup job error: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup old pending test trades")
    parser.add_argument("--test-max-age-hours", type=float, help=f"Max age for test trades (default: {DEFAULT_TEST_TRADE_MAX_AGE_HOURS}h)")
    parser.add_argument("--live-max-age-hours", type=float, help=f"Max age for live trades (default: {DEFAULT_LIVE_TRADE_MAX_AGE_HOURS}h)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be cancelled without actually doing it")
    args = parser.parse_args()
    
    stats = cleanup_old_trades(
        test_max_age_hours=args.test_max_age_hours,
        live_max_age_hours=args.live_max_age_hours,
        dry_run=args.dry_run
    )
    
    if stats:
        print("\nCleanup Summary:")
        print(f"  Test trades cancelled: {stats['test_trades_cancelled']}")
        print(f"  Live trades cancelled: {stats['live_trades_cancelled']}")
        print(f"  Total cancelled: {stats['total_cancelled']}")

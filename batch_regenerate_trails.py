#!/usr/bin/env python3
"""Batch regenerate trail data for all trades missing it."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from core.database import get_postgres
from trail_generator import generate_trail_payload
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def batch_regenerate_missing_trails(hours_back: int = 24, limit: int = None):
    """Regenerate trail data for all trades missing it."""
    
    cutoff = datetime.now() - timedelta(hours=hours_back)
    
    # Find all trades without trail data
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            query = """
                SELECT b.id, b.followed_at, b.our_status, b.our_entry_price
                FROM follow_the_goat_buyins b
                WHERE b.created_at > %s
                  AND NOT EXISTS (
                      SELECT 1 FROM buyin_trail_minutes t WHERE t.buyin_id = b.id
                  )
                ORDER BY b.created_at DESC
            """
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, [cutoff])
            missing_trades = cursor.fetchall()
    
    if not missing_trades:
        logger.info(f"✓ No trades missing trail data in the last {hours_back} hours")
        return
    
    logger.info(f"Found {len(missing_trades)} trades missing trail data")
    
    success_count = 0
    error_count = 0
    
    for idx, trade in enumerate(missing_trades, 1):
        buyin_id = trade['id']
        logger.info(f"\n[{idx}/{len(missing_trades)}] Processing buyin {buyin_id} (status: {trade['our_status']}, price: ${trade['our_entry_price']})")
        
        try:
            trail_payload = generate_trail_payload(buyin_id=buyin_id, persist=True)
            
            if trail_payload:
                # Verify
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) as count FROM buyin_trail_minutes WHERE buyin_id = %s", [buyin_id])
                        count = cursor.fetchone()['count']
                
                if count > 0:
                    logger.info(f"  ✓ Success: {count} trail rows inserted")
                    success_count += 1
                else:
                    logger.error(f"  ✗ Failed: No rows inserted")
                    error_count += 1
            else:
                logger.error(f"  ✗ Failed: Empty payload")
                error_count += 1
                
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            error_count += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Batch regeneration complete:")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info(f"  Total: {len(missing_trades)}")
    logger.info(f"{'='*60}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch regenerate trail data for trades missing it")
    parser.add_argument('--hours', type=int, default=24, help='Hours back to search (default: 24)')
    parser.add_argument('--limit', type=int, help='Maximum number of trades to process')
    
    args = parser.parse_args()
    
    batch_regenerate_missing_trails(hours_back=args.hours, limit=args.limit)

#!/usr/bin/env python3
"""Regenerate trail data for a specific trade."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "000trading"))

from core.database import get_postgres
from trail_generator import generate_trail_payload
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def regenerate_trail(buyin_id: str, force: bool = False):
    """Regenerate trail data for a specific buyin."""
    
    # Check if buyin exists
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, followed_at, our_entry_price, our_status 
                FROM follow_the_goat_buyins 
                WHERE id = %s
            """, [buyin_id])
            buyin = cursor.fetchone()
            
            if not buyin:
                logger.error(f"Buyin {buyin_id} not found")
                return False
            
            logger.info(f"Found buyin {buyin_id} (status: {buyin['our_status']}, price: ${buyin['our_entry_price']})")
    
    # Check existing trail data
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM buyin_trail_minutes 
                WHERE buyin_id = %s
            """, [buyin_id])
            existing_count = cursor.fetchone()['count']
            
            if existing_count > 0:
                if not force:
                    logger.warning(f"Trail data already exists ({existing_count} rows) - use --force to regenerate")
                    return False
                
                # Delete existing trail data
                cursor.execute("DELETE FROM buyin_trail_minutes WHERE buyin_id = %s", [buyin_id])
                cursor.execute("DELETE FROM trade_filter_values WHERE buyin_id = %s", [buyin_id])
                conn.commit()
                logger.info(f"Deleted {existing_count} existing trail rows")
    
    # Generate trail data
    logger.info(f"Generating trail data for buyin {buyin_id}...")
    try:
        trail_payload = generate_trail_payload(buyin_id=buyin_id, persist=True)
        
        if trail_payload:
            logger.info(f"✓ Trail data generated successfully!")
            
            # Verify
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) as count FROM buyin_trail_minutes WHERE buyin_id = %s", [buyin_id])
                    new_count = cursor.fetchone()['count']
                    logger.info(f"✓ Verified: {new_count} trail rows inserted")
            
            return True
        else:
            logger.error("Trail generation returned empty payload")
            return False
            
    except Exception as e:
        logger.error(f"Trail generation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Regenerate trail data for a specific trade")
    parser.add_argument('buyin_id', help='Buyin ID to regenerate trail data for')
    parser.add_argument('--force', action='store_true', help='Force regeneration even if trail data exists')
    
    args = parser.parse_args()
    
    success = regenerate_trail(args.buyin_id, force=args.force)
    
    sys.exit(0 if success else 1)

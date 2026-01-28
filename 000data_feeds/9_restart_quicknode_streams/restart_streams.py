"""
QuickNode Stream Latency Monitor & Auto-Restart
================================================
Monitors trade data latency and automatically restarts QuickNode streams
when data falls too far behind.

This script:
1. Checks the average latency of the last 10 trades (created_at - trade_timestamp)
2. If latency exceeds 30 seconds, restarts both QuickNode streams via API
3. Logs all actions to the 'actions' table for monitoring

Usage:
    python restart_streams.py

Schedule:
    Runs every 15 seconds via master2.py scheduler
"""

import os
import sys
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables from .env file
load_dotenv(PROJECT_ROOT / '.env')

from core.database import get_postgres
from core.config import settings

# =============================================================================
# CONFIGURATION
# =============================================================================

# QuickNode API configuration
QUICKNODE_API_KEY = os.getenv("quicknode_key")
QUICKNODE_STREAM_1 = os.getenv("quicknode_stream_1")
QUICKNODE_STREAM_2 = os.getenv("quicknode_stream_2")
QUICKNODE_API_BASE = "https://api.quicknode.com/streams/rest/v1"

# Latency threshold (in seconds)
LATENCY_THRESHOLD = 30.0

# Number of recent trades to check
TRADES_SAMPLE_SIZE = 10

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def check_trade_latency() -> Optional[float]:
    """
    Check the average latency of recent trades.
    
    Returns:
        Average latency in seconds, or None if error/no data
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        AVG(EXTRACT(EPOCH FROM (created_at - trade_timestamp))) AS difference_in_seconds
                    FROM (
                        SELECT created_at, trade_timestamp
                        FROM sol_stablecoin_trades
                        ORDER BY id DESC
                        LIMIT %s
                    ) AS recent_trades
                """, [TRADES_SAMPLE_SIZE])
                
                result = cursor.fetchone()
                
                if result and result['difference_in_seconds'] is not None:
                    latency = float(result['difference_in_seconds'])
                    logger.debug(f"Current trade latency: {latency:.2f}s")
                    return latency
                else:
                    logger.warning("No trade data available for latency check")
                    return None
                    
    except Exception as e:
        logger.error(f"Error checking trade latency: {e}", exc_info=True)
        return None


def log_action(
    event_type: str,
    success: bool,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Log an action to the actions table.
    
    Args:
        event_type: Type of event (e.g., 'stream_restart')
        success: Whether the action succeeded
        error_message: Error details if failed
        metadata: Additional event data (JSON)
    
    Returns:
        True if logged successfully, False otherwise
    """
    try:
        # Convert metadata dict to JSON string for PostgreSQL
        metadata_json = json.dumps(metadata) if metadata else None
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO actions (event_type, success, error_message, metadata)
                    VALUES (%s, %s, %s, %s::jsonb)
                """, [event_type, success, error_message, metadata_json])
                
        logger.debug(f"Logged action: {event_type} (success={success})")
        return True
        
    except Exception as e:
        logger.error(f"Error logging action: {e}", exc_info=True)
        return False


# =============================================================================
# QUICKNODE API OPERATIONS
# =============================================================================

def restart_stream(stream_id: str) -> Dict[str, Any]:
    """
    Restart a QuickNode stream by updating it to start from the latest block.
    
    Args:
        stream_id: QuickNode stream ID
    
    Returns:
        Dict with 'success' (bool) and optional 'error' or 'response' data
    """
    if not QUICKNODE_API_KEY:
        return {
            'success': False,
            'error': 'QuickNode API key not configured'
        }
    
    url = f"{QUICKNODE_API_BASE}/streams/{stream_id}"
    
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'x-api-key': QUICKNODE_API_KEY
    }
    
    try:
        logger.info(f"Restarting stream {stream_id}...")
        
        # Step 1: Pause the stream first (QuickNode requires pausing before updating)
        pause_payload = {'status': 'paused'}
        pause_response = requests.patch(url, headers=headers, json=pause_payload, timeout=10)
        
        if pause_response.status_code != 200:
            error_msg = f"Failed to pause stream (status {pause_response.status_code}): {pause_response.text}"
            logger.error(f"✗ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'status_code': pause_response.status_code
            }
        
        logger.debug(f"Stream {stream_id} paused")
        
        # Step 2: Update to start from latest block and reactivate
        restart_payload = {
            'start_range': -1,  # Start from latest block
            'status': 'active'
        }
        restart_response = requests.patch(url, headers=headers, json=restart_payload, timeout=10)
        
        if restart_response.status_code == 200:
            logger.info(f"✓ Stream {stream_id} restarted successfully")
            return {
                'success': True,
                'response': restart_response.json()
            }
        else:
            error_msg = f"API returned status {restart_response.status_code}: {restart_response.text}"
            logger.error(f"✗ Failed to restart stream {stream_id}: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'status_code': restart_response.status_code
            }
            
    except requests.exceptions.Timeout:
        error_msg = "API request timed out"
        logger.error(f"✗ Failed to restart stream {stream_id}: {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error: {str(e)}"
        logger.error(f"✗ Failed to restart stream {stream_id}: {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"✗ Failed to restart stream {stream_id}: {error_msg}", exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }


def restart_all_streams(latency: float) -> Dict[str, Any]:
    """
    Restart all configured QuickNode streams.
    
    Args:
        latency: The latency value that triggered the restart
    
    Returns:
        Dict with overall success status and details
    """
    if not QUICKNODE_STREAM_1 or not QUICKNODE_STREAM_2:
        error_msg = "QuickNode stream IDs not configured in .env"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg
        }
    
    results = {}
    
    # Restart stream 1
    logger.info(f"Latency threshold exceeded ({latency:.2f}s > {LATENCY_THRESHOLD}s)")
    logger.info("Restarting all QuickNode streams...")
    
    results['stream_1'] = restart_stream(QUICKNODE_STREAM_1)
    results['stream_2'] = restart_stream(QUICKNODE_STREAM_2)
    
    # Determine overall success
    all_success = all(r['success'] for r in results.values())
    
    if all_success:
        logger.info("✓ All streams restarted successfully")
    else:
        failed = [k for k, v in results.items() if not v['success']]
        logger.error(f"✗ Failed to restart: {', '.join(failed)}")
    
    return {
        'success': all_success,
        'results': results,
        'latency': latency,
        'stream_ids': [QUICKNODE_STREAM_1, QUICKNODE_STREAM_2]
    }


# =============================================================================
# MAIN MONITORING LOGIC
# =============================================================================

def run_monitoring_cycle() -> Dict[str, Any]:
    """
    Run a single monitoring cycle.
    
    Returns:
        Dict with cycle results
    """
    try:
        # Step 1: Check latency
        latency = check_trade_latency()
        
        if latency is None:
            return {
                'success': False,
                'error': 'Unable to check latency'
            }
        
        # Step 2: Check if restart needed
        if latency > LATENCY_THRESHOLD:
            # Restart streams
            restart_result = restart_all_streams(latency)
            
            # Log to actions table
            log_action(
                event_type='stream_restart',
                success=restart_result['success'],
                error_message=restart_result.get('error'),
                metadata={
                    'latency_seconds': latency,
                    'threshold_seconds': LATENCY_THRESHOLD,
                    'stream_1_id': QUICKNODE_STREAM_1,
                    'stream_2_id': QUICKNODE_STREAM_2,
                    'results': restart_result.get('results', {})
                }
            )
            
            return {
                'success': True,
                'action_taken': True,
                'restart_success': restart_result['success'],
                'latency': latency
            }
        else:
            # No action needed
            logger.debug(f"Latency OK ({latency:.2f}s <= {LATENCY_THRESHOLD}s)")
            return {
                'success': True,
                'action_taken': False,
                'latency': latency
            }
            
    except Exception as e:
        logger.error(f"Error in monitoring cycle: {e}", exc_info=True)
        
        # Log error to actions table
        log_action(
            event_type='stream_monitor_error',
            success=False,
            error_message=str(e),
            metadata={'error_type': type(e).__name__}
        )
        
        return {
            'success': False,
            'error': str(e)
        }


def main():
    """
    Main entry point for standalone execution.
    """
    logger.info("Starting QuickNode stream monitoring cycle...")
    
    # Verify configuration
    if not QUICKNODE_API_KEY:
        logger.error("QuickNode API key not found in environment (quicknode_key)")
        sys.exit(1)
    
    if not QUICKNODE_STREAM_1 or not QUICKNODE_STREAM_2:
        logger.error("QuickNode stream IDs not found in environment (quicknode_stream_1, quicknode_stream_2)")
        sys.exit(1)
    
    logger.info(f"Configuration:")
    logger.info(f"  - Latency threshold: {LATENCY_THRESHOLD}s")
    logger.info(f"  - Sample size: {TRADES_SAMPLE_SIZE} trades")
    logger.info(f"  - Stream 1: {QUICKNODE_STREAM_1}")
    logger.info(f"  - Stream 2: {QUICKNODE_STREAM_2}")
    
    result = run_monitoring_cycle()
    
    if result['success']:
        if result.get('action_taken'):
            logger.info(f"Monitoring cycle complete: Stream restart triggered (latency: {result['latency']:.2f}s)")
        else:
            logger.info(f"Monitoring cycle complete: No action needed (latency: {result['latency']:.2f}s)")
    else:
        logger.error(f"Monitoring cycle failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()

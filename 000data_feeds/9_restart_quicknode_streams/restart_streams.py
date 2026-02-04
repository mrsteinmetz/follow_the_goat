"""
QuickNode Stream Latency Monitor & Auto-Restart
================================================
Monitors trade data latency and automatically restarts QuickNode streams
when data falls too far behind.

This script:
1. Checks the average latency of the last 10 trades (created_at - trade_timestamp)
2. Checks if trade ingestion has stalled (no new inserts / stale timestamps)
3. If any threshold exceeds 30 seconds, restarts both QuickNode streams via API
4. Logs all actions to the 'actions' table for monitoring

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

# If no new transactions within this window, restart streams (seconds)
STALE_TRANSACTION_THRESHOLD = 30.0

# Minimum time between restarts (seconds) to avoid hammering QuickNode
RESTART_COOLDOWN_SECONDS = 60.0

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


def check_trade_staleness() -> Dict[str, Any]:
    """
    Check whether new trades are still arriving.
    
    We check BOTH:
    - seconds_since_last_insert: NOW - MAX(created_at)
    - seconds_since_last_trade_timestamp: NOW - MAX(trade_timestamp)
    
    Returns:
        Dict with seconds_since_last_insert/seconds_since_last_trade_timestamp and timestamps.
        If there is no data, seconds will be None.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        MAX(created_at) AS last_created_at,
                        MAX(trade_timestamp) AS last_trade_timestamp,
                        EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - MAX(created_at))) AS seconds_since_last_insert,
                        EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - MAX(trade_timestamp))) AS seconds_since_last_trade_timestamp
                    FROM sol_stablecoin_trades
                    """
                )
                row = cursor.fetchone() or {}

        return {
            "last_created_at": row.get("last_created_at"),
            "last_trade_timestamp": row.get("last_trade_timestamp"),
            "seconds_since_last_insert": float(row["seconds_since_last_insert"]) if row.get("seconds_since_last_insert") is not None else None,
            "seconds_since_last_trade_timestamp": float(row["seconds_since_last_trade_timestamp"]) if row.get("seconds_since_last_trade_timestamp") is not None else None,
        }
    except Exception as e:
        logger.error(f"Error checking trade staleness: {e}", exc_info=True)
        return {
            "error": str(e),
            "last_created_at": None,
            "last_trade_timestamp": None,
            "seconds_since_last_insert": None,
            "seconds_since_last_trade_timestamp": None,
        }


def seconds_since_last_restart() -> Optional[float]:
    """
    Return seconds since last successful stream restart, or None if no history.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(created_at) AS last_restart_at
                    FROM actions
                    WHERE event_type = 'stream_restart' AND success = TRUE
                    """
                )
                row = cursor.fetchone() or {}
                last = row.get("last_restart_at")
                if not last:
                    return None
                cursor.execute(
                    "SELECT EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - %s)) AS seconds",
                    [last],
                )
                age_row = cursor.fetchone() or {}
                return float(age_row["seconds"]) if age_row.get("seconds") is not None else None
    except Exception as e:
        logger.warning(f"Failed to check last restart age: {e}")
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

def check_webhook_health() -> bool:
    """
    Check if the webhook server is responding.
    
    Returns:
        True if webhook is healthy, False otherwise
    """
    try:
        response = requests.get("http://localhost:8001/health", timeout=5)
        if response.status_code == 200:
            logger.info("✓ Webhook server is healthy")
            return True
        else:
            logger.warning(f"⚠️  Webhook server returned status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Webhook server is not responding: {e}")
        return False


def get_stream_status(stream_id: str) -> Dict[str, Any]:
    """
    Get the current status of a QuickNode stream.
    
    Args:
        stream_id: QuickNode stream ID
    
    Returns:
        Dict with 'success' (bool), 'status' (str), and optional 'error' or 'data'
    """
    if not QUICKNODE_API_KEY:
        return {
            'success': False,
            'error': 'QuickNode API key not configured'
        }
    
    url = f"{QUICKNODE_API_BASE}/streams/{stream_id}"
    headers = {
        'accept': 'application/json',
        'x-api-key': QUICKNODE_API_KEY
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            status = data.get('status', 'unknown')
            logger.info(f"Stream {stream_id[:8]}... status: {status}")
            return {
                'success': True,
                'status': status,
                'data': data
            }
        else:
            return {
                'success': False,
                'error': f"API returned status {response.status_code}",
                'status': 'unknown'
            }
    except Exception as e:
        logger.error(f"Failed to get stream status: {e}")
        return {
            'success': False,
            'error': str(e),
            'status': 'unknown'
        }


def restart_stream(stream_id: str) -> Dict[str, Any]:
    """
    Restart a QuickNode stream by updating it to start from the latest block.
    Handles both active and terminated streams.
    
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
        
        # Step 1: Check current stream status
        status_result = get_stream_status(stream_id)
        current_status = status_result.get('status', 'unknown')
        
        # Step 2: Handle based on current status
        if current_status == 'terminated':
            logger.warning(f"⚠️  Stream {stream_id[:8]}... is TERMINATED - activating directly")
            # For terminated streams, directly activate them
            activate_payload = {
                'start_range': -1,  # Start from latest block
                'status': 'active'
            }
            activate_response = requests.patch(url, headers=headers, json=activate_payload, timeout=10)
            
            if activate_response.status_code == 200:
                logger.info(f"✓ Stream {stream_id[:8]}... activated successfully from terminated state")
                return {
                    'success': True,
                    'response': activate_response.json(),
                    'was_terminated': True
                }
            else:
                error_msg = f"Failed to activate terminated stream (status {activate_response.status_code}): {activate_response.text}"
                logger.error(f"✗ {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'status_code': activate_response.status_code
                }
        
        # For active/paused streams, use the normal restart flow
        # Step 2a: Pause the stream first (QuickNode requires pausing before updating)
        if current_status != 'paused':
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
            
            logger.debug(f"Stream {stream_id[:8]}... paused")
        
        # Step 2b: Update to start from latest block and reactivate
        restart_payload = {
            'start_range': -1,  # Start from latest block
            'status': 'active'
        }
        restart_response = requests.patch(url, headers=headers, json=restart_payload, timeout=10)
        
        if restart_response.status_code == 200:
            logger.info(f"✓ Stream {stream_id[:8]}... restarted successfully")
            return {
                'success': True,
                'response': restart_response.json()
            }
        else:
            error_msg = f"API returned status {restart_response.status_code}: {restart_response.text}"
            logger.error(f"✗ Failed to restart stream {stream_id[:8]}...: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'status_code': restart_response.status_code
            }
            
    except requests.exceptions.Timeout:
        error_msg = "API request timed out"
        logger.error(f"✗ Failed to restart stream {stream_id[:8]}...: {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error: {str(e)}"
        logger.error(f"✗ Failed to restart stream {stream_id[:8]}...: {error_msg}")
        return {
            'success': False,
            'error': error_msg
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"✗ Failed to restart stream {stream_id[:8]}...: {error_msg}", exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }


def restart_all_streams(trigger_seconds: float, reason: str = "latency") -> Dict[str, Any]:
    """
    Restart all configured QuickNode streams.
    Checks webhook health first before attempting restart.
    
    Args:
        trigger_seconds: The value that triggered the restart
        reason: Reason for restart (latency, no_transactions, etc.)
    
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
    
    # CRITICAL: Check webhook health first
    logger.info("Checking webhook server health before restarting streams...")
    webhook_healthy = check_webhook_health()
    
    if not webhook_healthy:
        error_msg = "Webhook server is not responding - cannot restart streams safely"
        logger.error(f"❌ {error_msg}")
        logger.error("   Please check if webhook_server component is running!")
        return {
            'success': False,
            'error': error_msg,
            'webhook_healthy': False
        }
    
    results = {}
    
    # Restart streams
    logger.info(f"Restart trigger ({reason}): {trigger_seconds:.2f}s")
    logger.info("Restarting all QuickNode streams...")
    
    results['stream_1'] = restart_stream(QUICKNODE_STREAM_1)
    results['stream_2'] = restart_stream(QUICKNODE_STREAM_2)
    
    # Determine overall success
    all_success = all(r['success'] for r in results.values())
    
    if all_success:
        logger.info("✓ All streams restarted successfully")
        logger.info("  Streams should now be sending data to webhook...")
    else:
        failed = [k for k, v in results.items() if not v['success']]
        logger.error(f"✗ Failed to restart: {', '.join(failed)}")
    
    return {
        'success': all_success,
        'results': results,
        'trigger_seconds': trigger_seconds,
        'reason': reason,
        'stream_ids': [QUICKNODE_STREAM_1, QUICKNODE_STREAM_2],
        'webhook_healthy': webhook_healthy
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
        # Cooldown guard (avoid restarting too frequently)
        last_restart_age = seconds_since_last_restart()
        if last_restart_age is not None and last_restart_age < RESTART_COOLDOWN_SECONDS:
            logger.info(
                f"Restart cooldown active ({last_restart_age:.1f}s < {RESTART_COOLDOWN_SECONDS}s) - skipping restart"
            )
            return {
                'success': True,
                'action_taken': False,
                'reason': 'cooldown',
                'seconds_since_last_restart': last_restart_age
            }

        # Step 1: staleness checks (no transactions / stale timestamps)
        staleness = check_trade_staleness()
        sec_since_insert = staleness.get("seconds_since_last_insert")
        sec_since_trade_ts = staleness.get("seconds_since_last_trade_timestamp")

        # If we have no usable data, treat as stalled
        if sec_since_insert is None or sec_since_trade_ts is None:
            reason = "no_transactions"
            trigger_val = max(sec_since_insert or 0.0, sec_since_trade_ts or 0.0, STALE_TRANSACTION_THRESHOLD + 1.0)
            restart_result = restart_all_streams(trigger_val, reason=reason)
            log_action(
                event_type='stream_restart',
                success=restart_result['success'],
                error_message=restart_result.get('error'),
                metadata={
                    'reason': reason,
                    'trigger_seconds': trigger_val,
                    'stale_transaction_threshold': STALE_TRANSACTION_THRESHOLD,
                    'last_created_at': staleness.get("last_created_at").isoformat() if staleness.get("last_created_at") else None,
                    'last_trade_timestamp': staleness.get("last_trade_timestamp").isoformat() if staleness.get("last_trade_timestamp") else None,
                    'seconds_since_last_insert': sec_since_insert,
                    'seconds_since_last_trade_timestamp': sec_since_trade_ts,
                    'results': restart_result.get('results', {}),
                }
            )
            return {
                'success': True,
                'action_taken': True,
                'restart_success': restart_result['success'],
                'reason': reason,
                'trigger_seconds': trigger_val,
            }

        # If no new transactions (insert) within threshold, restart
        if sec_since_insert > STALE_TRANSACTION_THRESHOLD:
            reason = "no_recent_transactions"
            trigger_val = float(sec_since_insert)
            restart_result = restart_all_streams(trigger_val, reason=reason)
            log_action(
                event_type='stream_restart',
                success=restart_result['success'],
                error_message=restart_result.get('error'),
                metadata={
                    'reason': reason,
                    'trigger_seconds': trigger_val,
                    'stale_transaction_threshold': STALE_TRANSACTION_THRESHOLD,
                    'last_created_at': staleness.get("last_created_at").isoformat() if staleness.get("last_created_at") else None,
                    'results': restart_result.get('results', {}),
                }
            )
            return {
                'success': True,
                'action_taken': True,
                'restart_success': restart_result['success'],
                'reason': reason,
                'trigger_seconds': trigger_val,
            }

        # If trade_timestamp is stale (older than threshold), restart
        if sec_since_trade_ts > STALE_TRANSACTION_THRESHOLD:
            reason = "stale_trade_timestamp"
            trigger_val = float(sec_since_trade_ts)
            restart_result = restart_all_streams(trigger_val, reason=reason)
            log_action(
                event_type='stream_restart',
                success=restart_result['success'],
                error_message=restart_result.get('error'),
                metadata={
                    'reason': reason,
                    'trigger_seconds': trigger_val,
                    'stale_transaction_threshold': STALE_TRANSACTION_THRESHOLD,
                    'last_trade_timestamp': staleness.get("last_trade_timestamp").isoformat() if staleness.get("last_trade_timestamp") else None,
                    'results': restart_result.get('results', {}),
                }
            )
            return {
                'success': True,
                'action_taken': True,
                'restart_success': restart_result['success'],
                'reason': reason,
                'trigger_seconds': trigger_val,
            }

        # Step 2: latency check (created_at - trade_timestamp)
        latency = check_trade_latency()
        if latency is None:
            return {
                'success': False,
                'error': 'Unable to check latency'
            }

        if latency > LATENCY_THRESHOLD:
            reason = "latency"
            restart_result = restart_all_streams(float(latency), reason=reason)
            log_action(
                event_type='stream_restart',
                success=restart_result['success'],
                error_message=restart_result.get('error'),
                metadata={
                    'reason': reason,
                    'latency_seconds': latency,
                    'threshold_seconds': LATENCY_THRESHOLD,
                    'stale_transaction_threshold': STALE_TRANSACTION_THRESHOLD,
                    'seconds_since_last_insert': sec_since_insert,
                    'seconds_since_last_trade_timestamp': sec_since_trade_ts,
                    'stream_1_id': QUICKNODE_STREAM_1,
                    'stream_2_id': QUICKNODE_STREAM_2,
                    'results': restart_result.get('results', {})
                }
            )
            return {
                'success': True,
                'action_taken': True,
                'restart_success': restart_result['success'],
                'reason': reason,
                'latency': latency
            }

        logger.debug(
            f"OK: insert_age={sec_since_insert:.1f}s trade_ts_age={sec_since_trade_ts:.1f}s latency={latency:.2f}s"
        )
        return {
            'success': True,
            'action_taken': False,
            'latency': latency,
            'seconds_since_last_insert': sec_since_insert,
            'seconds_since_last_trade_timestamp': sec_since_trade_ts,
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
    
    if result.get('action_taken'):
        logger.info(f"✓ Restart action taken")
    elif result.get('reason') == 'cooldown':
        logger.info(f"Monitoring cycle complete: In cooldown (no action taken)")
    else:
        logger.info(f"Monitoring cycle complete: No action needed")


if __name__ == "__main__":
    main()

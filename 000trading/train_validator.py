"""
Train Validator
===============
Training script that creates synthetic trades every 30 seconds for testing
the pattern validation pipeline.

This module:
- Creates synthetic buy-in records
- Generates 15-minute trails
- Runs pattern validation
- Updates records with validation results

Usage:
    # Standalone execution
    python 000trading/train_validator.py
    
    # As scheduled job
    from _000trading.train_validator import run_training_cycle
    run_training_cycle()
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
PROJECT_ROOT = Path(__file__).parent.parent
MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_DIR))

from core.database import get_postgres

# Try to get TradingDataEngine for in-memory queries (when running under scheduler)
def _get_engine_if_running():
    """Get TradingDataEngine if it's running, otherwise return None."""
    try:
        from core.trading_engine import _engine_instance
        if _engine_instance is not None and _engine_instance._running:
            logger.debug(f"TradingDataEngine found and running: {_engine_instance}")
            return _engine_instance
        else:
            logger.debug(f"TradingDataEngine not running (_engine_instance={_engine_instance})")
    except Exception as e:
        logger.debug(f"TradingDataEngine not available: {e}")
        pass
    return None

# Import our modules (direct imports after adding module dir to path)
from trail_generator import generate_trail_payload, TrailError
from pattern_validator import (
    validate_buyin_signal,
    clear_schema_cache,
)
from pre_entry_price_movement import (
    calculate_pre_entry_metrics,
)

# Configuration
PLAY_ID = int(os.getenv("TRAIN_VALIDATOR_PLAY_ID", "46"))
TRAINING_INTERVAL_SECONDS = int(os.getenv("TRAIN_VALIDATOR_INTERVAL", "15"))
TRAINING_ENABLED = os.getenv("TRAIN_VALIDATOR_ENABLED", "1") == "1"
PUMP_SIGNAL_PLAY_ID = int(os.getenv("PUMP_SIGNAL_PLAY_ID", "3"))
# Fast mode: skip pattern validation, only do pump signal detection (saves ~1.5s per cycle)
PUMP_FAST_MODE = os.getenv("TRAIN_VALIDATOR_PUMP_FAST_MODE", "1") == "1"

# Setup logging
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler
    log_file = LOGS_DIR / "train_validator.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# =============================================================================
# UTILITIES
# =============================================================================

def _utc_now_iso() -> str:
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def make_json_safe(value: Any) -> Any:
    """Convert values to JSON-serializable format."""
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [make_json_safe(v) for v in value]
    if isinstance(value, set):
        return [make_json_safe(v) for v in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class StepLogger:
    """Structured step logger that captures durations and metadata for each operation."""
    
    def __init__(self) -> None:
        self.steps: List[Dict[str, Any]] = []
    
    def start(
        self,
        step_name: str,
        description: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        token: Dict[str, Any] = {
            'step': step_name,
            'description': description,
            'details': make_json_safe(details) if details else {},
            'start_time': time.time()
        }
        return token
    
    def end(
        self,
        token: Dict[str, Any],
        extra_details: Optional[Dict[str, Any]] = None,
        status: str = 'success'
    ) -> None:
        end_time = time.time()
        entry: Dict[str, Any] = {
            'step': token.get('step'),
            'status': status,
            'description': token.get('description'),
            'duration_ms': round((end_time - token['start_time']) * 1000, 3),
            'timestamp': _utc_now_iso()
        }
        
        details: Dict[str, Any] = {}
        if token.get('details'):
            details.update(token['details'])
        if extra_details:
            details.update(make_json_safe(extra_details))
        if details:
            entry['details'] = details
        
        self.steps.append(entry)
    
    def fail(
        self,
        token: Dict[str, Any],
        error_message: str,
        extra_details: Optional[Dict[str, Any]] = None
    ) -> None:
        details: Dict[str, Any] = {'error': error_message}
        if extra_details:
            details.update(make_json_safe(extra_details))
        self.end(token, details, status='error')
    
    def add(
        self,
        step_name: str,
        description: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        duration_ms: Optional[float] = None,
        status: str = 'success'
    ) -> None:
        entry: Dict[str, Any] = {
            'step': step_name,
            'status': status,
            'description': description,
            'timestamp': _utc_now_iso()
        }
        if duration_ms is not None:
            entry['duration_ms'] = round(duration_ms, 3)
        if details:
            entry['details'] = make_json_safe(details)
        self.steps.append(entry)
    
    def to_json(self) -> List[Dict[str, Any]]:
        return self.steps.copy()


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================


def _pg_execute(query: str, params: list) -> None:
    """Best-effort helper for short PostgreSQL writes."""
    try:
        with get_postgres() as pg_conn:
            if not pg_conn:
                return
            with pg_conn.cursor() as cursor:
                cursor.execute(query, params)
    except Exception as e:
        logger.debug(f"PostgreSQL write skipped: {e}")


def _pg_upsert_buyin(row: Dict[str, Any]) -> None:
    """Insert or update a buyin row in PostgreSQL."""
    if not row:
        return
    columns = list(row.keys())
    values = [row[c] for c in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    # Avoid empty update clause when only id is provided
    update_columns = [col for col in columns if col != "id"]
    if update_columns:
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    else:
        update_clause = "id = EXCLUDED.id"
    query = f"""
        INSERT INTO follow_the_goat_buyins ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
    """
    _pg_execute(query, values)


def _pg_update_buyin(buyin_id: int, fields: Dict[str, Any]) -> None:
    """Update a buyin row in PostgreSQL by id."""
    if not fields:
        return
    set_clause = ", ".join([f"{col} = %s" for col in fields.keys()])
    params = list(fields.values()) + [buyin_id]
    query = f"""
        UPDATE follow_the_goat_buyins
        SET {set_clause}
        WHERE id = %s
    """
    _pg_execute(query, params)

def get_play_config(play_id: int) -> Optional[Dict[str, Any]]:
    """Fetch play configuration from PostgreSQL.
    
    Queries the follow_the_goat_plays table directly from PostgreSQL.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, pattern_validator_enable, pattern_validator, project_ids
                    FROM follow_the_goat_plays
                    WHERE id = %s
                """, [play_id])
                
                result = cursor.fetchone()
                
                if not result:
                    logger.error(f"Play #{play_id} not found in PostgreSQL")
                    return None
                
                # Result is already a dict (RealDictCursor)
                play = dict(result)
        
        # Parse pattern validator config
        pattern_validator_raw = play.get('pattern_validator')
        pattern_validator_config = None
        
        if pattern_validator_raw:
            if isinstance(pattern_validator_raw, str):
                try:
                    pattern_validator_config = json.loads(pattern_validator_raw)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON in pattern_validator field")
            elif isinstance(pattern_validator_raw, dict):
                pattern_validator_config = pattern_validator_raw
        
        play['pattern_validator_config'] = pattern_validator_config
        
        # Coerce pattern_validator_enable
        enable_flag = play.get('pattern_validator_enable')
        play['pattern_validator_enable'] = int(enable_flag) if enable_flag else 0
        
        # Process project_ids
        project_ids_raw = play.get('project_ids')
        if project_ids_raw:
            if isinstance(project_ids_raw, str):
                try:
                    play['project_ids'] = json.loads(project_ids_raw)
                except json.JSONDecodeError:
                    play['project_ids'] = []
            elif isinstance(project_ids_raw, list):
                play['project_ids'] = project_ids_raw
            else:
                play['project_ids'] = []
        else:
            play['project_ids'] = []
        
        logger.debug(f"Loaded play #{play_id}: {play.get('name')} (from PostgreSQL)")
        
        return play
            
    except Exception as e:
        logger.error(f"Error fetching play config: {e}")
        return None


def check_data_readiness() -> tuple[bool, str]:
    """Check if sufficient data exists to run a training cycle.
    
    Queries PostgreSQL directly for price and order book data.
    The PRIMARY price table is `prices` (not `price_points` which is legacy).
    
    Returns:
        Tuple of (is_ready, reason_if_not_ready)
    """
    logger.debug("Checking data readiness using PostgreSQL")
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Check `prices` table - this is the PRIMARY price data source
                # Schema: id, timestamp, token, price
                # Query: WHERE token = 'SOL' for SOL prices
                cursor.execute("""
                    SELECT COUNT(*) as count FROM prices WHERE token = 'SOL'
                """)
                result = cursor.fetchone()
                price_count = result['count'] if result else 0
                logger.debug(f"Prices table check: {price_count} SOL prices")
                
                if price_count < 10:
                    return False, f"Waiting for price data ({price_count}/10 prices)"
                
                # Check order_book_features (optional for train_validator)
                try:
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM order_book_features
                    """)
                    result = cursor.fetchone()
                    ob_count = result['count'] if result else 0
                    logger.debug(f"Order book check: {ob_count} rows")
                    
                    if ob_count < 5:
                        logger.info(f"Order book data limited ({ob_count}/5 rows) - continuing anyway")
                except Exception as e:
                    logger.debug(f"Order book check error: {e} - continuing anyway")
                
                logger.info(f"Data ready: {price_count} prices in PostgreSQL")
                return True, f"Data ready ({price_count} prices)"
            
    except Exception as e:
        logger.error(f"Database check error: {e}")
        return False, f"Database not ready: {e}"


def get_current_market_price() -> Optional[float]:
    """Get current SOL price from PostgreSQL.
    
    Uses `prices` table (PRIMARY) - NOT `price_points` (legacy).
    Schema: prices(id, timestamp, token, price)
    Query: WHERE token = 'SOL' ORDER BY timestamp DESC LIMIT 1
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT price
                    FROM prices
                    WHERE token = 'SOL'
                    ORDER BY timestamp DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if result:
                    return float(result['price'])
                logger.warning("No SOL prices found in database")
                return None
                
    except Exception as e:
        logger.error(f"Error getting current market price: {e}")
        return None


def get_current_price_cycle() -> Optional[int]:
    """
    Get current active price_cycle ID from cycle_tracker table in PostgreSQL.
    
    Returns the active cycle (cycle_end_time IS NULL) for threshold 0.3.
    """
    query = """
        SELECT id
        FROM cycle_tracker
        WHERE threshold = 0.3
          AND cycle_end_time IS NULL
        ORDER BY id DESC
        LIMIT 1
    """
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result['id']
                return None
                
    except Exception as e:
        logger.error(f"Error getting current price_cycle: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def insert_synthetic_buyin(
    play_id: int,
    our_entry_price: float,
    price_cycle: Optional[int],
    step_logger: StepLogger
) -> Optional[int]:
    """Insert a synthetic buyin record for training.
    
    CRITICAL: Always uses master2.py's local DuckDB (via get_postgres())
    since price_cycle references cycle_tracker which lives in master2.py.
    """
    timestamp = int(time.time())
    wallet_address = f"TRAINING_TEST_{timestamp}"
    signature = f"training_sig_{timestamp}"
    # CRITICAL: Use naive UTC timestamp for PostgreSQL TIMESTAMP column
    # PostgreSQL TIMESTAMP (without time zone) stores naive datetimes
    # We generate UTC time and strip timezone info for storage
    block_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    block_timestamp_str = block_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    pre_insert_log = json.dumps(step_logger.to_json())
    
    insert_token = step_logger.start(
        'insert_synthetic_buyin',
        'Inserting synthetic buyin record for training',
        {
            'wallet_address': wallet_address,
            'our_entry_price': our_entry_price,
            'price_cycle': price_cycle
        }
    )
    
    buyin_id = None
    
    try:
        # Get next ID from PostgreSQL
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM follow_the_goat_buyins")
                result = cursor.fetchone()
                buyin_id = result['next_id'] if result else 1
        
        # Insert into PostgreSQL
        _pg_upsert_buyin({
            'id': buyin_id,
            'play_id': play_id,
            'wallet_address': wallet_address,
            'original_trade_id': 0,
            'trade_signature': signature,
            'block_timestamp': block_timestamp,
            'quote_amount': 100.0,
            'base_amount': our_entry_price if our_entry_price else 0.0,
            'price': our_entry_price,
            'direction': 'buy',
            'our_entry_price': our_entry_price,
            'live_trade': 0,
            'price_cycle': price_cycle,
            'entry_log': pre_insert_log,
            'pattern_validator_log': None,
            'our_status': 'validating',
            'followed_at': block_timestamp,
            'higest_price_reached': our_entry_price,
        })
        
        logger.debug(f"PostgreSQL insert successful, buyin_id={buyin_id}")
        
        step_logger.end(
            insert_token,
            {
                'buyin_id': buyin_id,
                'wallet_address': wallet_address,
                'our_entry_price': our_entry_price,
                'price_cycle': price_cycle
            }
        )
        
        logger.info(f"✓ Inserted synthetic buyin #{buyin_id} (wallet: {wallet_address[:20]}...)")
        return buyin_id
        
    except Exception as e:
        logger.error(f"Error inserting synthetic buyin: {e}")
        step_logger.fail(insert_token, str(e))
        return None


def generate_trail(buyin_id: int, step_logger: StepLogger) -> bool:
    """Generate 15-minute trail for the buyin.
    
    Trail data is stored in the buyin_trail_minutes table (15 rows per buyin).
    """
    trail_token = step_logger.start(
        'generate_15_minute_trail',
        'Generating 15-minute analytics trail',
        {'buyin_id': buyin_id}
    )
    
    try:
        trail_payload = generate_trail_payload(buyin_id=buyin_id, persist=True)
        
        # Check if data was persisted to the table
        persisted = trail_payload.get('persisted', False)
        
        step_logger.end(
            trail_token,
            {
                'persisted_to_table': persisted,
                'table': 'buyin_trail_minutes',
                'rows_inserted': 15 if persisted else 0,
                'minute_spans': len(trail_payload.get('minute_spans', [])),
                'order_book_rows': len(trail_payload.get('order_book_signals', [])),
                'transaction_rows': len(trail_payload.get('transactions', [])),
                'whale_rows': len(trail_payload.get('whale_activity', [])),
            }
        )
        
        if persisted:
            logger.info(f"✓ Generated and persisted 15-minute trail for buy-in #{buyin_id} (15 rows)")
        else:
            logger.warning(f"⚠ Generated trail for buy-in #{buyin_id} but persistence failed")
        
        return persisted
        
    except TrailError as trail_err:
        step_logger.fail(trail_token, str(trail_err))
        logger.error(f"Trail generation failed for buy-in #{buyin_id}: {trail_err}")
        return False
        
    except Exception as trail_exc:
        step_logger.fail(trail_token, str(trail_exc))
        logger.error(f"Trail generation error for buy-in #{buyin_id}: {trail_exc}")
        return False


def run_validation(
    buyin_id: int,
    play_config: Dict[str, Any],
    step_logger: StepLogger
) -> Optional[Dict[str, Any]]:
    """Run pattern validation on the buyin."""
    play_id = play_config.get('id')
    project_ids = play_config.get('project_ids', [])
    
    if project_ids:
        validation_mode = f'project filters ({len(project_ids)} projects)'
    else:
        validation_mode = 'schema-based'
    
    validation_token = step_logger.start(
        'pattern_validator',
        f'Running pattern validator ({validation_mode})',
        {'buyin_id': buyin_id, 'play_id': play_id, 'project_ids': project_ids}
    )
    
    try:
        validate_kwargs = {
            'buyin_id': buyin_id,
            'play_id': play_id,
        }
        
        if project_ids:
            validate_kwargs['project_ids'] = project_ids
            logger.info(f"Calling validate_buyin_signal with: buyin_id={buyin_id}, play_id={play_id}, project_ids={project_ids}")
        else:
            logger.info(f"Calling validate_buyin_signal with: buyin_id={buyin_id}, play_id={play_id}")
        
        pattern_validator_result = validate_buyin_signal(**validate_kwargs)
        
        if not isinstance(pattern_validator_result, dict):
            logger.warning(f"Pattern validator returned unexpected payload")
            pattern_validator_result = {
                'decision': 'ERROR',
                'error': 'Pattern validator returned non-dict payload',
            }
        
        decision = pattern_validator_result.get('decision', 'UNKNOWN')
        schema_source = pattern_validator_result.get('schema_source', 'UNKNOWN')
        
        logger.info(f"Validation completed for buy-in #{buyin_id}: {decision}")
        logger.info(f"  Schema source: {schema_source}")
        
        if schema_source in ('project_filters', 'multi_project_filters'):
            decision_quality = pattern_validator_result.get('decision_quality', {})
            filters_passed = decision_quality.get('filters_passed', 0)
            filters_total = decision_quality.get('filters_total', 0)
            winning_project_id = pattern_validator_result.get('winning_project_id')
            logger.info(f"  Filters: {filters_passed}/{filters_total} passed")
            if winning_project_id:
                logger.info(f"  Winning project: #{winning_project_id}")
        
        step_logger.end(
            validation_token,
            make_json_safe({
                'decision': decision,
                'schema_source': schema_source,
                'validator_version': pattern_validator_result.get('validator_version'),
            })
        )
        
        return pattern_validator_result
        
    except Exception as exc:
        step_logger.fail(validation_token, str(exc))
        logger.error(f"Validation error for buy-in #{buyin_id}: {exc}")
        return {
            'decision': 'ERROR',
            'error': str(exc),
            'error_type': type(exc).__name__
        }


def update_validation_result(
    buyin_id: int,
    validation_result: Dict[str, Any],
    step_logger: StepLogger
) -> bool:
    """Update the buyin record with validation results.
    
    CRITICAL: Always uses master2.py's local DuckDB (via duckdb_execute_write)
    since buyins are stored there with cycle_tracker references.
    """
    decision = validation_result.get('decision', 'UNKNOWN')
    should_follow = decision == 'GO'
    
    # Determine new status
    if should_follow:
        new_status = 'pending'
    else:
        new_status = 'no_go'
    
    pattern_validator_log_json = json.dumps(validation_result, default=str)
    
    update_token = step_logger.start(
        'update_validation_result',
        'Updating buyin with validation results',
        {'buyin_id': buyin_id, 'decision': decision, 'new_status': new_status}
    )
    
    try:
        # Build update fields
        followed_at = None
        fresh_price = None
        if new_status == 'pending':
            fresh_price = get_current_market_price()
            # CRITICAL: Store timestamp in UTC, not local time
            # Use .replace(tzinfo=None) to store as naive UTC timestamp
            # PostgreSQL TIMESTAMP columns store naive datetimes, so we convert UTC to naive
            followed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        
        pg_fields = {
            'pattern_validator_log': pattern_validator_log_json,
            'our_status': new_status,
        }
        if followed_at:
            pg_fields['followed_at'] = followed_at
        if fresh_price is not None:
            pg_fields['our_entry_price'] = fresh_price
        
        # Update PostgreSQL
        _pg_update_buyin(buyin_id, pg_fields)
        
        step_logger.end(
            update_token,
            {'new_status': new_status}
        )
        
        logger.info(f"✓ Updated buy-in #{buyin_id} with validation result: {decision} -> {new_status}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating validation result for buy-in #{buyin_id}: {e}")
        step_logger.fail(update_token, str(e))
        return False


def update_entry_log(buyin_id: int, step_logger: StepLogger) -> None:
    """Update the entry log with final step logger data.
    
    Updates PostgreSQL with the complete entry log.
    """
    try:
        final_log_json = json.dumps(step_logger.to_json())
        
        # Update PostgreSQL
        _pg_update_buyin(buyin_id, {'entry_log': final_log_json})
        
    except Exception as e:
        logger.error(f"Error updating entry log for buy-in #{buyin_id}: {e}")


def mark_buyin_as_error(buyin_id: int, error_reason: str, step_logger: Optional[StepLogger] = None) -> None:
    """Mark a buyin as 'error' status when validation pipeline fails.
    
    Updates PostgreSQL with error status and logs.
    """
    try:
        error_log = json.dumps({
            'decision': 'ERROR',
            'error': error_reason,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        
        # Build update fields
        update_fields = {
            'our_status': 'error',
            'pattern_validator_log': error_log,
        }
        
        # Update entry_log if step_logger provided
        if step_logger:
            update_fields['entry_log'] = json.dumps(step_logger.to_json())
        
        # Update PostgreSQL
        _pg_update_buyin(buyin_id, update_fields)
        
        logger.info(f"Marked buyin #{buyin_id} as 'error': {error_reason}")
        
    except Exception as e:
        logger.error(f"Failed to mark buyin #{buyin_id} as error: {e}")


# =============================================================================
# TRAINING CYCLE
# =============================================================================

class TrainingStats:
    """Track training statistics."""
    
    def __init__(self):
        self.trades_inserted = 0
        self.validations_run = 0
        self.validations_passed = 0
        self.validations_failed = 0
        self.errors = 0
        self.start_time = datetime.now()
    
    def log_summary(self, cycle_count: int) -> None:
        """Log current statistics."""
        uptime = (datetime.now() - self.start_time).total_seconds()
        logger.info("\n" + "="*80)
        logger.info("STATISTICS")
        logger.info(f"  Cycles: {cycle_count}")
        logger.info(f"  Trades inserted: {self.trades_inserted}")
        logger.info(f"  Validations run: {self.validations_run}")
        logger.info(f"  Validations passed: {self.validations_passed}")
        logger.info(f"  Validations failed: {self.validations_failed}")
        logger.info(f"  Errors: {self.errors}")
        logger.info(f"  Uptime: {uptime:.0f}s ({uptime/60:.1f}m)")
        logger.info("="*80 + "\n")


# Global stats instance
_stats = TrainingStats()


def cleanup_stuck_validating_trades(max_age_seconds: int = 120) -> int:
    """Clean up trades stuck in 'validating' status for too long.
    
    Uses PostgreSQL directly to find and update stuck trades.
    
    Args:
        max_age_seconds: Max seconds a trade can be in 'validating' status (default: 2 minutes)
        
    Returns:
        Number of trades cleaned up
    """
    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
        
        # Find stuck trades in PostgreSQL
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, followed_at
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'validating'
                      AND followed_at < %s
                """, [cutoff_time])
                
                raw_result = cursor.fetchall()
                
                if not raw_result:
                    return 0
                
                stuck_ids = [row['id'] for row in raw_result]
        
        logger.warning(f"Found {len(stuck_ids)} trades stuck in 'validating' status: {stuck_ids}")
        
        error_log = json.dumps({
            'decision': 'ERROR',
            'error': f'Stuck in validating status for >{max_age_seconds}s - auto-cleaned',
            'timestamp': datetime.now(timezone.utc).isoformat(),
        })
        
        # Update PostgreSQL
        for buyin_id in stuck_ids:
            _pg_update_buyin(buyin_id, {
                'our_status': 'error',
                'pattern_validator_log': error_log,
            })
        
        logger.info(f"Cleaned up {len(stuck_ids)} stuck 'validating' trades")
        return len(stuck_ids)
        
    except Exception as e:
        logger.error(f"Error cleaning up stuck validating trades: {e}")
        return 0


def run_training_cycle(play_id: Optional[int] = None) -> bool:
    """Execute one training cycle.
    
    In PUMP_FAST_MODE (default): skips pattern validation to maximize pump
    signal detection speed. The cycle is: insert buyin → generate trail →
    pump signal check. This saves ~1.5 seconds per cycle.
    
    In full mode: also runs pattern validation (pump_continuation + project
    filters) for training data collection.
    
    Args:
        play_id: Optional override for play ID (default: PLAY_ID from env)
        
    Returns:
        True if cycle completed successfully, False otherwise
    """
    play_id = play_id or PLAY_ID
    buyin_id = None

    # Clean up stuck 'validating' trades (less frequently to save time)
    cleanup_stuck_validating_trades(max_age_seconds=120)
    
    # Check if sufficient data exists
    is_ready, reason = check_data_readiness()
    if not is_ready:
        logger.info(f"Skipping training cycle: {reason}")
        return False
    
    cycle_start = time.time()
    step_logger = StepLogger()
    cycle_token = step_logger.start('training_cycle', 'Training cycle')
    
    try:
        # 1. Get market price (fast — essential for pump detection)
        market_price = get_current_market_price()
        price_cycle = get_current_price_cycle()
        
        if not market_price:
            logger.error("No market price available - aborting cycle")
            _stats.errors += 1
            return False
        
        step_logger.add(
            'fetch_market_data',
            'Market data fetched',
            {'market_price': market_price, 'price_cycle': price_cycle}
        )
        
        # 2. Insert synthetic buyin
        buyin_id = insert_synthetic_buyin(play_id, market_price, price_cycle, step_logger)
        
        if not buyin_id:
            logger.error("Failed to insert synthetic buyin - aborting cycle")
            _stats.errors += 1
            return False
        
        _stats.trades_inserted += 1
        
        # 3. Generate trail (the most time-consuming step ~2s)
        trail_success = generate_trail(buyin_id, step_logger)
        
        if not trail_success:
            logger.warning("Trail generation failed")
            mark_buyin_as_error(buyin_id, "Trail generation failed", step_logger)
            _stats.errors += 1
            return False
        
        # 3b. Sync high-freq cache (fast incremental, feeds readiness score)
        try:
            from pump_highfreq_cache import sync_highfreq_cache
            sync_highfreq_cache(lookback_minutes=30)
        except Exception as hf_err:
            logger.debug(f"HF cache sync skipped: {hf_err}")

        # 4. PUMP SIGNAL CHECK — the primary purpose of this cycle
        # Run BEFORE validation so pump detection is as fast as possible
        pump_fired = False
        if PUMP_SIGNAL_PLAY_ID:
            try:
                from pump_signal_logic import maybe_refresh_rules, check_and_fire_pump_signal
                maybe_refresh_rules()
                pump_fired = check_and_fire_pump_signal(
                    buyin_id=buyin_id,
                    market_price=market_price,
                    price_cycle=price_cycle,
                )
                if pump_fired:
                    logger.info(f"PUMP SIGNAL FIRED for play #{PUMP_SIGNAL_PLAY_ID} (source buyin #{buyin_id})")
            except Exception as pump_err:
                logger.error(f"Pump signal check error: {pump_err}", exc_info=True)
        
        # 4b. READINESS SCORE — fast path volatility-event detector
        # If the readiness score exceeds threshold, it means "something is
        # happening" and we should check again ASAP (skip normal 15s wait).
        # This doesn't run a second model — it just decides when to check
        # the full GBM more frequently.
        readiness_triggered = False
        try:
            from pump_signal_logic import should_trigger_fast_path
            readiness_triggered = should_trigger_fast_path()
        except Exception:
            pass

        # 5. Pattern validation (skip in fast mode to save ~1.5s)
        validation_result = None
        if not PUMP_FAST_MODE:
            # Full mode: also run pattern validation
            cache_result = clear_schema_cache(play_id=play_id)
            step_logger.add('clear_schema_cache', 'Cleared cache', cache_result)
            
            play_config = get_play_config(play_id)
            if play_config:
                enable_flag = play_config.get('pattern_validator_enable')
                pattern_validator_enabled = (enable_flag == 1 or enable_flag is True)
                
                if pattern_validator_enabled:
                    validation_result = run_validation(buyin_id, play_config, step_logger)
                    if validation_result:
                        update_validation_result(buyin_id, validation_result, step_logger)
        
        # If no validation ran, mark the synthetic buyin as no_go
        if validation_result is None:
            _pg_update_buyin(buyin_id, {'our_status': 'no_go'})
            decision = 'PUMP_CHECK'
        else:
            decision = validation_result.get('decision', 'UNKNOWN')
        
        # Update entry log
        update_entry_log(buyin_id, step_logger)
        step_logger.end(cycle_token, {'buyin_id': buyin_id, 'success': True})
        
        _stats.validations_run += 1
        if decision == 'GO':
            _stats.validations_passed += 1
        else:
            _stats.validations_failed += 1
        
        cycle_ms = round((time.time() - cycle_start) * 1000)
        pump_tag = " PUMP!" if pump_fired else ""
        mode_tag = "fast" if PUMP_FAST_MODE else "full"
        readiness_tag = " READY!" if readiness_triggered else ""
        logger.info(f"✓ #{buyin_id}: {decision}{pump_tag}{readiness_tag} @ ${market_price:.2f} (cycle {price_cycle}) [{cycle_ms}ms] [{mode_tag}]")
        
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in training cycle: {e}", exc_info=True)
        _stats.errors += 1
        step_logger.fail(cycle_token, str(e))
        if buyin_id:
            mark_buyin_as_error(buyin_id, f"Unexpected error: {str(e)}", step_logger)
        return False


def run_continuous_training(
    play_id: Optional[int] = None,
    interval_seconds: Optional[int] = None
) -> None:
    """Run continuous training loop.
    
    Args:
        play_id: Override for play ID
        interval_seconds: Override for interval between cycles
    """
    play_id = play_id or PLAY_ID
    interval = interval_seconds or TRAINING_INTERVAL_SECONDS
    
    logger.info("\n" + "="*80)
    logger.info("VALIDATOR TRAINING SCRIPT STARTED")
    logger.info(f"Play ID: {play_id}")
    logger.info(f"Interval: {interval} seconds")
    logger.info("="*80 + "\n")
    
    cycle_count = 0
    
    while True:
        try:
            cycle_count += 1
            logger.info(f"\n--- CYCLE #{cycle_count} ---")
            
            run_training_cycle(play_id)
            
            # Show stats every 5 cycles
            if cycle_count % 5 == 0:
                _stats.log_summary(cycle_count)
            
            logger.info(f"Sleeping {interval} seconds until next cycle...")
            time.sleep(interval)
            
        except KeyboardInterrupt:
            logger.info("\n\nKeyboard interrupt received - shutting down...")
            break
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            _stats.errors += 1
            logger.info(f"Waiting {interval} seconds before retry...")
            time.sleep(interval)
    
    # Final statistics
    _stats.log_summary(cycle_count)
    logger.info("Validator training script stopped")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run validator training")
    parser.add_argument("--play-id", type=int, default=PLAY_ID, help="Play ID to train against")
    parser.add_argument("--interval", type=int, default=TRAINING_INTERVAL_SECONDS, help="Interval between cycles (seconds)")
    parser.add_argument("--once", action="store_true", help="Run single cycle and exit")
    
    args = parser.parse_args()
    
    if args.once:
        success = run_training_cycle(args.play_id)
        exit(0 if success else 1)
    else:
        run_continuous_training(args.play_id, args.interval)


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
from datetime import datetime, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, get_mysql, dual_write_insert, dual_write_update

# Import our modules
from _000trading.trail_generator import generate_trail_payload, TrailError
from _000trading.pattern_validator import (
    validate_buyin_signal,
    clear_schema_cache,
)

# Configuration
PLAY_ID = int(os.getenv("TRAIN_VALIDATOR_PLAY_ID", "46"))
TRAINING_INTERVAL_SECONDS = int(os.getenv("TRAIN_VALIDATOR_INTERVAL", "30"))
TRAINING_ENABLED = os.getenv("TRAIN_VALIDATOR_ENABLED", "1") == "1"

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

def get_play_config(play_id: int) -> Optional[Dict[str, Any]]:
    """Fetch play configuration from database."""
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT id, name, pattern_validator_enable, pattern_validator, project_ids
                FROM follow_the_goat_plays
                WHERE id = ?
            """, [play_id]).fetchone()
            
            if not result:
                logger.error(f"Play #{play_id} not found in database")
                return None
            
            play = {
                'id': result[0],
                'name': result[1],
                'pattern_validator_enable': result[2],
                'pattern_validator': result[3],
                'project_ids': result[4],
            }
            
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
            
            logger.info(f"Loaded play #{play_id}: {play.get('name')}")
            logger.info(f"  Validator enabled: {play['pattern_validator_enable']}")
            logger.info(f"  Has schema: {bool(pattern_validator_config)}")
            logger.info(f"  Project IDs: {play.get('project_ids', [])}")
            
            return play
            
    except Exception as e:
        logger.error(f"Error fetching play config: {e}")
        return None


def get_current_market_price() -> Optional[float]:
    """Get current SOL price from price_points table."""
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT value
                FROM price_points
                WHERE coin_id = 5
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()
            
            if result:
                return float(result[0])
            else:
                logger.warning("No price data found in price_points")
                return None
                
    except Exception as e:
        logger.error(f"Error getting current market price: {e}")
        return None


def get_current_price_cycle() -> Optional[int]:
    """Get current price_cycle ID from cycle_tracker table."""
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT id
                FROM cycle_tracker
                WHERE threshold = 0.3
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()
            
            if result:
                return result[0]
            else:
                logger.warning("No cycle data found in cycle_tracker")
                return None
                
    except Exception as e:
        logger.error(f"Error getting current price_cycle: {e}")
        return None


def insert_synthetic_buyin(
    play_id: int,
    our_entry_price: float,
    price_cycle: Optional[int],
    step_logger: StepLogger
) -> Optional[int]:
    """Insert a synthetic buyin record for training."""
    timestamp = int(time.time())
    wallet_address = f"TRAINING_TEST_{timestamp}"
    signature = f"training_sig_{timestamp}"
    block_timestamp = datetime.now(timezone.utc)
    
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
    
    try:
        data = {
            'play_id': play_id,
            'wallet_address': wallet_address,
            'original_trade_id': 0,
            'trade_signature': signature,
            'block_timestamp': block_timestamp.isoformat(),
            'quote_amount': 100.0,
            'base_amount': our_entry_price if our_entry_price else 0.0,
            'price': our_entry_price,
            'direction': 'buy',
            'our_entry_price': our_entry_price,
            'swap_response': None,
            'live_trade': 0,
            'price_cycle': price_cycle,
            'entry_log': pre_insert_log,
            'pattern_validator_log': None,
            'our_status': 'validating',
            'followed_at': block_timestamp.isoformat(),
        }
        
        # Insert using dual-write
        buyin_id = dual_write_insert(
            table="follow_the_goat_buyins",
            data=data
        )
        
        step_logger.end(
            insert_token,
            {
                'buyin_id': buyin_id,
                'wallet_address': wallet_address
            }
        )
        
        logger.info(f"✓ Inserted synthetic buyin #{buyin_id} (wallet: {wallet_address[:20]}...)")
        return buyin_id
        
    except Exception as e:
        logger.error(f"Error inserting synthetic buyin: {e}")
        step_logger.fail(insert_token, str(e))
        return None


def generate_trail(buyin_id: int, step_logger: StepLogger) -> bool:
    """Generate 15-minute trail for the buyin."""
    trail_token = step_logger.start(
        'generate_15_minute_trail',
        'Generating 15-minute analytics trail',
        {'buyin_id': buyin_id}
    )
    
    try:
        trail_payload = generate_trail_payload(buyin_id=buyin_id, persist=True)
        step_logger.end(
            trail_token,
            {
                'minute_spans': len(trail_payload.get('minute_spans', [])),
                'order_book_rows': len(trail_payload.get('order_book_signals', [])),
                'transaction_rows': len(trail_payload.get('transactions', [])),
                'whale_rows': len(trail_payload.get('whale_activity', [])),
            }
        )
        logger.info(f"✓ Generated 15-minute trail for buy-in #{buyin_id}")
        return True
        
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
    """Update the buyin record with validation results."""
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
        update_data = {
            'pattern_validator_log': pattern_validator_log_json,
            'our_status': new_status,
        }
        
        # If transitioning to 'pending', update followed_at and fresh price
        if new_status == 'pending':
            fresh_price = get_current_market_price()
            update_data['followed_at'] = datetime.now(timezone.utc).isoformat()
            if fresh_price:
                update_data['our_entry_price'] = fresh_price
        
        dual_write_update(
            table="follow_the_goat_buyins",
            data=update_data,
            where={"id": buyin_id}
        )
        
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
    """Update the entry log with final step logger data."""
    try:
        final_log_json = json.dumps(step_logger.to_json())
        dual_write_update(
            table="follow_the_goat_buyins",
            data={"entry_log": final_log_json},
            where={"id": buyin_id}
        )
    except Exception as e:
        logger.error(f"Error updating entry log for buy-in #{buyin_id}: {e}")


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


def run_training_cycle(play_id: Optional[int] = None) -> bool:
    """Execute one training cycle.
    
    Args:
        play_id: Optional override for play ID (default: PLAY_ID from env)
        
    Returns:
        True if cycle completed successfully, False otherwise
    """
    play_id = play_id or PLAY_ID
    
    logger.info("\n" + "="*80)
    logger.info("STARTING TRAINING CYCLE")
    logger.info("="*80)
    
    step_logger = StepLogger()
    cycle_token = step_logger.start('training_cycle', 'Complete training cycle execution')
    
    try:
        # Clear schema cache to get latest
        cache_result = clear_schema_cache(play_id=play_id)
        logger.info(f"✓ Schema cache cleared: {cache_result.get('message', 'success')}")
        step_logger.add('clear_schema_cache', 'Cleared validator schema cache', cache_result)
        
        # 1. Get play config
        logger.info("Step 1: Fetching play configuration...")
        play_config = get_play_config(play_id)
        
        if not play_config:
            logger.error("Failed to load play configuration - aborting cycle")
            _stats.errors += 1
            return False
        
        # Check if pattern validator is enabled
        enable_flag = play_config.get('pattern_validator_enable')
        pattern_validator_enabled = (enable_flag == 1 or enable_flag is True)
        
        if not pattern_validator_enabled:
            logger.error("Pattern validator not enabled for this play - aborting cycle")
            _stats.errors += 1
            return False
        
        # Allow validation if either schema OR project_ids is available
        project_ids = play_config.get('project_ids', [])
        if not play_config.get('pattern_validator_config') and not project_ids:
            logger.error("Pattern validator schema missing and no project_ids configured - aborting cycle")
            _stats.errors += 1
            return False
        
        validation_mode = 'multi_project_filters' if len(project_ids) > 1 else \
                         'project_filters' if project_ids else 'schema_based'
        
        step_logger.add(
            'load_play_config',
            'Play configuration loaded',
            make_json_safe({
                'play_id': play_config.get('id'),
                'play_name': play_config.get('name'),
                'validator_enabled': pattern_validator_enabled,
                'project_ids': project_ids,
                'validation_mode': validation_mode,
            })
        )
        
        # 2. Get market price and price cycle
        logger.info("Step 2: Fetching market data...")
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
        
        # 3. Insert synthetic buyin
        logger.info("Step 3: Inserting synthetic buyin...")
        buyin_id = insert_synthetic_buyin(play_id, market_price, price_cycle, step_logger)
        
        if not buyin_id:
            logger.error("Failed to insert synthetic buyin - aborting cycle")
            _stats.errors += 1
            return False
        
        _stats.trades_inserted += 1
        
        # 4. Generate trail
        logger.info("Step 4: Generating 15-minute trail...")
        trail_success = generate_trail(buyin_id, step_logger)
        
        if not trail_success:
            logger.error("Trail generation failed - aborting cycle")
            _stats.errors += 1
            return False
        
        # 5. Run validation
        logger.info("Step 5: Running pattern validation...")
        validation_result = run_validation(buyin_id, play_config, step_logger)
        
        if not validation_result:
            logger.error("Validation failed - aborting cycle")
            _stats.errors += 1
            return False
        
        # 6. Update validation result
        logger.info("Step 6: Updating validation result...")
        update_success = update_validation_result(buyin_id, validation_result, step_logger)
        
        if not update_success:
            logger.error("Failed to update validation result")
            _stats.errors += 1
            return False
        
        # 7. Update entry log
        update_entry_log(buyin_id, step_logger)
        
        step_logger.end(cycle_token, {'buyin_id': buyin_id, 'success': True})
        
        # Update stats
        _stats.validations_run += 1
        decision = validation_result.get('decision', 'UNKNOWN')
        if decision == 'GO':
            _stats.validations_passed += 1
        else:
            _stats.validations_failed += 1
        
        logger.info("="*80)
        logger.info(f"✓ TRAINING CYCLE COMPLETED SUCCESSFULLY (buyin_id: {buyin_id})")
        logger.info(f"  Decision: {decision}")
        logger.info(f"  Status: {'pending' if decision == 'GO' else 'no_go'}")
        logger.info("="*80 + "\n")
        
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in training cycle: {e}", exc_info=True)
        _stats.errors += 1
        step_logger.fail(cycle_token, str(e))
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


"""
Trailing Stop Seller - PostgreSQL Version
=========================================
Monitors open buy-in positions and applies dynamic trailing stop logic.

This module:
- Monitors positions with our_status='pending' from PostgreSQL
- Fetches SOL prices from price_points table (coin_id=5, updated every 1s)
- Applies tolerance rules from play sell_logic configuration
- Tracks highest price reached for each position
- Records price checks to follow_the_goat_buyins_price_checks
- Marks positions as 'sold' when tolerance is exceeded

Trailing Stop Logic (DECIMAL-BASED):
- Entry price is "ground zero" - determines which tolerance rules apply
- If current price > entry: Use 'increases' rules (track drop from highest)
- If current price < entry: Use 'decreases' rules (track drop from entry)
- All calculations use decimal values (0.005 = 0.5%, 0.001 = 0.1%)

Usage:
    # Standalone execution
    python 000trading/sell_trailing_stop.py
    
    # As scheduled job (via scheduler/master2.py)
    from sell_trailing_stop import run_single_cycle
    run_single_cycle()
    
    # Continuous mode
    python 000trading/sell_trailing_stop.py --continuous
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from decimal import Decimal
from logging.handlers import RotatingFileHandler, MemoryHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
PROJECT_ROOT = Path(__file__).parent.parent
MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_DIR))

from core.database import get_postgres, postgres_execute

# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_MONITOR_INTERVAL_SECONDS = float(
    os.getenv('TRAILING_STOP_INTERVAL_SECONDS', '0.5')
)

# Setup logging
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    # Console handler - INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # File handler with rotation
    log_file = LOGS_DIR / "sell_trailing_stop.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(file_format)
    
    # Memory handler - buffers logs, flushes on ERROR
    memory_handler = MemoryHandler(
        capacity=1000,
        flushLevel=logging.ERROR,
        target=file_handler,
        flushOnClose=False
    )
    
    logger.addHandler(console_handler)
    logger.addHandler(memory_handler)


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


# =============================================================================
# TRAILING STOP SELLER CLASS
# =============================================================================

class TrailingStopSeller:
    """
    Monitors open buy-in positions and applies dynamic trailing stop using DECIMAL-BASED tolerance rules:
    - Entry price is "ground zero" - determines which tolerance rules to apply
    - If current price > entry: Use 'increases' rules (track highest, measure drop from highest)
    - If current price < entry: Use 'decreases' rules (measure drop from entry)
    - All calculations use decimal values (e.g., 0.005 = 0.5%, 0.001 = 0.1%)
    """
    
    def __init__(
        self,
        live_trade: bool = False,
        monitor_live: Optional[bool] = None,
        cache_ttl: int = 60
    ):
        """
        Initialize TrailingStopSeller
        
        Args:
            live_trade: If True, this instance monitors live trades. If False, test mode.
            monitor_live: None = monitor both live and test, True = live only, False = test only
            cache_ttl: Cache time-to-live in seconds for sell_logic (default: 60)
        """
        self.live_trade = live_trade
        self.monitor_live = monitor_live
        self.cache_ttl = cache_ttl
        
        # In-memory tracking for positions
        self.position_tracking: Dict[int, Dict[str, Any]] = {}
        self.position_tracking_lock = threading.Lock()
        
        # Cache for sell_logic per play
        self.sell_logic_cache: Dict[int, Dict[str, Any]] = {}
        self.cache_timestamps: Dict[int, float] = {}
        self.cache_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'positions_monitored': 0,
            'positions_sold': 0,
            'total_profit_loss': 0.0,
            'winning_trades': 0,
            'losing_trades': 0,
            'errors': 0,
            'cycles': 0,
            'start_time': datetime.now(),
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"TrailingStopSeller initialized (live_trade={live_trade}, monitor_live={monitor_live})")
    
    def get_current_sol_price(self) -> Optional[float]:
        """
        Get the latest SOL price from prices table.
        
        Returns:
            Current SOL price as float, or None if not found.
        """
        try:
            # Create fresh connection with autocommit to avoid stale reads
            import psycopg2
            import psycopg2.extras
            from core.config import settings
            
            conn = psycopg2.connect(
                host=settings.postgres.host,
                user=settings.postgres.user,
                password=settings.postgres.password,
                database=settings.postgres.database,
                port=settings.postgres.port,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=5
            )
            
            # Enable autocommit to always see the latest data
            conn.autocommit = True
            
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT price, timestamp, id
                    FROM prices 
                    WHERE token = 'SOL'
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)
                    result = cursor.fetchone()
                    
                    if result:
                        price = float(result.get('price'))
                        logger.debug(f"Current SOL price: ${price:.6f} (ID: {result.get('id')})")
                        return price
                    else:
                        logger.warning("No SOL price data found in prices table")
                        return None
            finally:
                conn.close()
                    
        except Exception as e:
            import traceback
            logger.error(f"Error getting current SOL price: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def get_open_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions that we're tracking (matching live_trade mode)."""
        try:
            # Create fresh connection with autocommit to avoid stale reads
            import psycopg2
            import psycopg2.extras
            from core.config import settings
            
            conn = psycopg2.connect(
                host=settings.postgres.host,
                user=settings.postgres.user,
                password=settings.postgres.password,
                database=settings.postgres.database,
                port=settings.postgres.port,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=5
            )
            
            # Enable autocommit to always see the latest data
            conn.autocommit = True
            
            try:
                with conn.cursor() as cursor:
                    # Build query depending on monitoring filter
                    base_sql = """
                    SELECT 
                        id,
                        play_id,
                        wallet_address,
                        original_trade_id,
                        price as entry_price,
                        quote_amount,
                        base_amount,
                        followed_at,
                        our_entry_price,
                        our_position_size,
                        price_movements,
                        live_trade,
                        higest_price_reached,
                        tolerance
                    FROM follow_the_goat_buyins 
                    WHERE our_status = 'pending'
                """
                
                    if self.monitor_live is True:
                        base_sql += " AND live_trade = 1"
                    elif self.monitor_live is False:
                        base_sql += " AND live_trade = 0"
                
                    base_sql += " ORDER BY followed_at ASC"
                
                    cursor.execute(base_sql)
                    result = cursor.fetchall()
                    return [dict(row) for row in (result or [])]
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error getting open positions: {e}")
            return []
    
    def get_sell_logic_for_play(self, play_id: int) -> Dict[str, Any]:
        """Load and cache sell_logic JSON for a given play_id with TTL."""
        current_time = time.time()
        
        # Thread-safe cache check
        with self.cache_lock:
            if play_id in self.sell_logic_cache:
                cache_age = current_time - self.cache_timestamps.get(play_id, 0)
                if cache_age < self.cache_ttl:
                    return self.sell_logic_cache[play_id]
                else:
                    logger.debug(f"Cache expired for play {play_id} (age: {cache_age:.1f}s)")
        
        # Cache miss or expired - fetch from database
        try:
            # Create fresh connection with autocommit to avoid stale reads
            import psycopg2
            import psycopg2.extras
            from core.config import settings
            
            conn = psycopg2.connect(
                host=settings.postgres.host,
                user=settings.postgres.user,
                password=settings.postgres.password,
                database=settings.postgres.database,
                port=settings.postgres.port,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=5
            )
            
            # Enable autocommit to always see the latest data
            conn.autocommit = True
            
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    SELECT sell_logic
                    FROM follow_the_goat_plays
                    WHERE id = %s
                    LIMIT 1
                """, [play_id])
                    result = cursor.fetchone()
                
                    logic = None
                    if result and result.get('sell_logic'):
                        raw = result['sell_logic']
                        try:
                            logic = json.loads(raw) if isinstance(raw, str) else raw
                        except Exception:
                            logic = None
                
                    if not logic:
                        # Use default tolerance rules
                        logic = self._get_default_tolerance_rules()
                
                    # Thread-safe cache update
                    with self.cache_lock:
                        self.sell_logic_cache[play_id] = logic
                        self.cache_timestamps[play_id] = current_time
                
                    logger.debug(f"Cache refreshed for play {play_id}")
                    return logic
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error loading sell_logic for play {play_id}: {e}")
            return self._get_default_tolerance_rules()
    
    def _get_default_tolerance_rules(self) -> Dict[str, Any]:
        """Return default tolerance rules."""
        return {
            "tolerance_rules": {
                "decreases": [
                    {"range": [-999999, 0], "tolerance": 0.0001}  # Any loss, tight 0.01% tolerance
                ],
                "increases": [
                    {"range": [0.0, 0.005], "tolerance": 0.003},  # 0% to 0.5% gain
                    {"range": [0.005, 1.0], "tolerance": 0.001}   # 0.5% to 100% gain
                ]
            }
        }
    
    def _select_rule(self, gain_decimal: float, rules: List[Dict]) -> Optional[Dict]:
        """
        Select the first rule whose range contains gain_decimal.
        Ranges must be properly ordered: [low, high] where low <= high mathematically.
        """
        if not rules:
            return None
        
        for rule in rules:
            try:
                range_values = rule.get('range', [None, None])
                if len(range_values) != 2 or range_values[0] is None or range_values[1] is None:
                    continue
                
                low = float(range_values[0])
                high = float(range_values[1])
                
                # Validate range is properly ordered
                if low > high:
                    logger.warning(f"Invalid range [{low}, {high}] - low should be <= high. Skipping rule.")
                    continue
                
                # Check if gain_decimal falls within [low, high)
                if low <= gain_decimal < high:
                    logger.debug(f"Rule matched: gain {gain_decimal:.6f} in range [{low}, {high}], tolerance {rule.get('tolerance')}")
                    return rule
                    
            except Exception as e:
                logger.warning(f"Error processing rule {rule}: {e}")
                continue
        
        # If no match, return the last rule as fallback
        if rules:
            logger.debug(f"No exact match for gain {gain_decimal:.6f}, using fallback (last rule)")
            return rules[-1]
        return None
    
    def _get_historical_highest_price(self, position: Dict[str, Any]) -> Optional[float]:
        """
        Get the highest price ever reached for this position.
        Uses the dedicated higest_price_reached column for fast retrieval.
        """
        try:
            # Use dedicated database column
            higest_price = position.get('higest_price_reached')
            if higest_price is not None:
                return float(higest_price)
            return None
        except Exception as e:
            logger.warning(f"Error getting historical highest price: {e}")
            return None
    
    def _update_highest_price_in_db(self, position_id: int, highest_price: float) -> bool:
        """
        Update the higest_price_reached column in PostgreSQL.
        """
        try:
            postgres_execute(
                "UPDATE follow_the_goat_buyins SET higest_price_reached = %s WHERE id = %s",
                [highest_price, position_id]
            )
            logger.debug(f"Highest price updated for position #{position_id} to ${highest_price:.6f}")
            return True
        except Exception as e:
            logger.error(f"Error updating highest price for position {position_id}: {e}")
            return False
    
    def _get_locked_tolerance(self, position: Dict[str, Any]) -> Optional[float]:
        """
        Get the locked tolerance for this position from the database.
        Returns None if not set (will use default high value).
        """
        try:
            tolerance = position.get('tolerance')
            if tolerance is not None and float(tolerance) > 0:
                return float(tolerance)
            return None
        except Exception as e:
            logger.warning(f"Error getting locked tolerance: {e}")
            return None
    
    def _update_locked_tolerance_in_db(self, position_id: int, tolerance: float) -> bool:
        """
        Update the locked tolerance in PostgreSQL.
        """
        try:
            postgres_execute(
                "UPDATE follow_the_goat_buyins SET tolerance = %s WHERE id = %s",
                [tolerance, position_id]
            )
            logger.info(f"TOLERANCE LOCKED for position #{position_id}: {tolerance} ({tolerance*100:.4f}%)")
            return True
        except Exception as e:
            logger.error(f"Error updating locked tolerance for position {position_id}: {e}")
            return False
    
    def initialize_position_tracking(
        self,
        position: Dict[str, Any],
        current_price: float
    ) -> Optional[Dict[str, Any]]:
        """
        Initialize tracking data for a position.
        
        Returns:
            Backfill movement data if this is a new position, None otherwise.
        """
        position_id = position['id']
        entry_price = float(position['our_entry_price']) if position.get('our_entry_price') else current_price
        
        with self.position_tracking_lock:
            if position_id not in self.position_tracking:
                # Restore highest_price from history if available
                historical_highest = self._get_historical_highest_price(position)
                
                if historical_highest is not None:
                    highest_price = max(historical_highest, entry_price, current_price)
                    if highest_price > historical_highest:
                        self._update_highest_price_in_db(position_id, highest_price)
                        logger.info(f"Position {position_id}: NEW HIGH during init ${highest_price:.6f} (was: ${historical_highest:.6f})")
                    else:
                        logger.debug(f"Restored position {position_id}: highest=${highest_price:.6f}")
                else:
                    highest_price = max(entry_price, current_price)
                    self._update_highest_price_in_db(position_id, highest_price)
                    logger.info(f"New position tracked: ID {position_id} @ ${entry_price:.4f}")
                
                # Load locked tolerance from database (or use high default so first rule wins)
                locked_tolerance = self._get_locked_tolerance(position)
                if locked_tolerance is None:
                    locked_tolerance = 1.0  # 100% - any rule will be tighter than this
                    logger.debug(f"Position {position_id}: No locked tolerance, starting at 1.0 (100%)")
                else:
                    logger.info(f"Position {position_id}: Restored locked tolerance {locked_tolerance} ({locked_tolerance*100:.4f}%)")
                
                self.position_tracking[position_id] = {
                    'entry_price': entry_price,
                    'highest_price': highest_price,
                    'current_tolerance': locked_tolerance,
                    'locked_tolerance': locked_tolerance,  # Persisted locked value
                    'wallet_address': position.get('wallet_address', ''),
                }
                
                # Create backfill entry for new positions
                if historical_highest is None:
                    return self._create_backfill_movement(position, entry_price, current_price)
        
        return None
    
    def _create_backfill_movement(
        self,
        position: Dict[str, Any],
        entry_price: float,
        current_price: float
    ) -> Dict[str, Any]:
        """Create backfill movement data for a new position."""
        followed_at = position.get('followed_at')
        # CRITICAL: Use UTC time for database storage
        timestamp = followed_at.strftime('%Y-%m-%d %H:%M:%S') if followed_at else datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        gain_decimal = ((current_price - entry_price) / entry_price) if entry_price > 0 else 0.0
        
        return {
            'timestamp': timestamp,
            'current_price': round(current_price, 8),
            'entry_price': round(entry_price, 8),
            'highest_price': round(current_price, 8),
            'gain_from_entry_decimal': round(gain_decimal, 6),
            'drop_from_high_decimal': 0.0,
            'tolerance_decimal': 0.003,
            'should_sell': False,
            'backfilled': True
        }
    
    def check_position(
        self,
        position: Dict[str, Any],
        current_price: float
    ) -> Dict[str, Any]:
        """
        Dynamic trailing stop logic using DECIMAL-BASED JSON rules per play.
        
        Returns:
            Dictionary with check results including should_sell flag.
        """
        position_id = position['id']
        
        # Initialize if needed
        backfill_data = self.initialize_position_tracking(position, current_price)
        
        with self.position_tracking_lock:
            tracking = self.position_tracking[position_id]
            entry_price = tracking['entry_price']
            highest_price = tracking['highest_price']
        
        play_id = position.get('play_id')
        
        # Step 1: Determine which tolerance set to use based on entry price
        rules_bucket = 'increases' if current_price > entry_price else 'decreases'
        
        # Step 2: Update highest price if new high reached
        if current_price > highest_price:
            with self.position_tracking_lock:
                old_highest = tracking['highest_price']
                tracking['highest_price'] = current_price
                highest_price = current_price
            
            if self._update_highest_price_in_db(position_id, current_price):
                logger.info(f"Position {position_id}: NEW HIGH ${current_price:.6f} (was: ${old_highest:.6f})")
        
        # Step 3: Calculate gain/loss from entry price (DECIMAL)
        gain_from_entry_decimal = ((current_price - entry_price) / entry_price) if entry_price else 0.0
        
        # Step 4: Load rules and select applicable tolerance
        logic = self.get_sell_logic_for_play(play_id) if play_id is not None else None
        tolerance_rules = (logic or {}).get('tolerance_rules', {})
        selected_rule = self._select_rule(gain_from_entry_decimal, tolerance_rules.get(rules_bucket, []))
        rule_tolerance = float(selected_rule.get('tolerance')) if selected_rule and selected_rule.get('tolerance') is not None else 0.001
        
        # Step 4b: TOLERANCE LOCKING - use the tighter of rule tolerance vs locked tolerance
        # Once a tighter tolerance is triggered, it never loosens for this position
        with self.position_tracking_lock:
            locked_tolerance = tracking.get('locked_tolerance', 1.0)
            
            # Use the minimum (tighter) of rule tolerance and locked tolerance
            tolerance_decimal = min(rule_tolerance, locked_tolerance)
            
            # If rule tolerance is tighter than locked, update the lock
            if rule_tolerance < locked_tolerance:
                old_locked = locked_tolerance
                tracking['locked_tolerance'] = rule_tolerance
                tracking['current_tolerance'] = rule_tolerance
                tolerance_decimal = rule_tolerance
                
                # Persist the new tighter tolerance to database
                self._update_locked_tolerance_in_db(position_id, rule_tolerance)
                logger.info(f"Position {position_id}: Tolerance TIGHTENED from {old_locked*100:.4f}% to {rule_tolerance*100:.4f}% (LOCKED)")
            elif tracking['current_tolerance'] != tolerance_decimal:
                # Just update current display tolerance (locked stays the same)
                tracking['current_tolerance'] = tolerance_decimal
                logger.debug(f"Position {position_id}: Using locked tolerance {tolerance_decimal*100:.4f}% (rule would be {rule_tolerance*100:.4f}%)")
        
        # Step 5: Calculate drops based on reference point
        reference_price = highest_price if rules_bucket == 'increases' else entry_price
        basis = 'highest' if rules_bucket == 'increases' else 'entry'
        
        drop_from_reference_decimal = ((current_price - reference_price) / reference_price) if reference_price else 0.0
        drop_from_high_decimal = ((current_price - highest_price) / highest_price) if highest_price else 0.0
        drop_from_entry_decimal = ((current_price - entry_price) / entry_price) if entry_price else 0.0
        
        # Step 6: Sell decision - check if drop exceeds tolerance
        should_sell = False
        if drop_from_reference_decimal < -tolerance_decimal:
            should_sell = True
            logger.info(f"SELL SIGNAL - Position {position_id} (basis: {basis}, tolerance {tolerance_decimal*100:.2f}%):")
            logger.info(f"   Entry: ${entry_price:.6f}, Current: ${current_price:.6f}, Highest: ${highest_price:.6f}")
            logger.info(f"   Drop from {basis}: {drop_from_reference_decimal*100:.4f}% - EXCEEDED TOLERANCE")
        
        # Step 7: Create movement data
        with self.position_tracking_lock:
            locked_tol = tracking.get('locked_tolerance', tolerance_decimal)
        
        # CRITICAL: Use UTC time for database storage
        movement_data = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'current_price': round(current_price, 8),
            'entry_price': round(entry_price, 8),
            'highest_price': round(highest_price, 8),
            'reference_price': round(reference_price, 8),
            'gain_from_entry_decimal': round(gain_from_entry_decimal, 6),
            'drop_from_reference_decimal': round(drop_from_reference_decimal, 6),
            'drop_from_high_decimal': round(drop_from_high_decimal, 6),
            'drop_from_entry_decimal': round(drop_from_entry_decimal, 6),
            'tolerance_decimal': tolerance_decimal,
            'rule_tolerance_decimal': rule_tolerance,  # Original rule tolerance before locking
            'locked_tolerance_decimal': locked_tol,    # Locked (persisted) tolerance
            'basis': basis,
            'bucket': rules_bucket,
            'applied_rule': selected_rule,
            'should_sell': should_sell
        }
        
        return {
            'should_sell': should_sell,
            'current_price': current_price,
            'entry_price': entry_price,
            'highest_price': highest_price,
            'gain_pct': gain_from_entry_decimal * 100,
            'drop_from_high_pct': drop_from_high_decimal * 100,
            'drop_from_entry_pct': drop_from_entry_decimal * 100,
            'current_tolerance': tolerance_decimal * 100,
            'profit_loss': current_price - entry_price,
            'profit_loss_pct': gain_from_entry_decimal * 100,
            'movement_data': movement_data,
            'backfill_data': backfill_data
        }
    
    def batch_update_current_prices(self, price_updates: List[Dict[str, Any]]) -> bool:
        """
        Batch update current_price for multiple positions in a single PostgreSQL query.
        
        Args:
            price_updates: List of dicts with 'position_id' and 'current_price'
            
        Returns:
            True if PostgreSQL was updated successfully.
        """
        if not price_updates:
            return True
        
        # Build batch UPDATE using CASE/WHEN for efficiency
        position_ids = [u['position_id'] for u in price_updates]
        
        # PostgreSQL batch update
        try:
            case_clauses = " ".join(["WHEN id = %s THEN %s" for _ in price_updates])
            params: List[Any] = []
            for update in price_updates:
                params.extend([update['position_id'], update['current_price']])
            
            ids_placeholders = ", ".join(["%s"] * len(position_ids))
            params.extend(position_ids)
            
            postgres_execute(f"""
                UPDATE follow_the_goat_buyins
                SET current_price = CASE {case_clauses} END
                WHERE id IN ({ids_placeholders})
            """, params)
            logger.debug(f"PostgreSQL batch updated {len(price_updates)} positions")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL batch update error: {e}")
            return False
    
    def save_price_movement(self, position_id: int, movement_data: Dict[str, Any], skip_price_update: bool = False) -> bool:
        """
        Save price movement data to PostgreSQL.
        
        Args:
            position_id: The buyin position ID
            movement_data: Movement data dict
            skip_price_update: If True, skip the current_price update (handled by batch)
        """
        try:
            # Create fresh connection with autocommit for immediate writes
            import psycopg2
            import psycopg2.extras
            from core.config import settings
            
            conn = psycopg2.connect(
                host=settings.postgres.host,
                user=settings.postgres.user,
                password=settings.postgres.password,
                database=settings.postgres.database,
                port=settings.postgres.port,
                cursor_factory=psycopg2.extras.RealDictCursor,
                connect_timeout=5
            )
            
            # Enable autocommit for immediate writes
            conn.autocommit = True
            
            try:
                current_price = movement_data.get('current_price')
                should_sell = movement_data.get('should_sell', False)
            
                # Only update current_price if not being batched
                if not skip_price_update:
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "UPDATE follow_the_goat_buyins SET current_price = %s WHERE id = %s",
                                [current_price, position_id]
                            )
                            # Autocommit handles the commit automatically
                    except Exception as e:
                        logger.error(f"Error updating current_price for position {position_id}: {e}")
            
                # Write price check to database for historical tracking
                checked_at = movement_data.get('timestamp')
                entry_price = movement_data.get('entry_price')
                highest_price = movement_data.get('highest_price')
                reference_price = movement_data.get('reference_price')
                gain_from_entry = movement_data.get('gain_from_entry_decimal')
                drop_from_high = movement_data.get('drop_from_high_decimal')
                drop_from_entry = movement_data.get('drop_from_entry_decimal')
                drop_from_reference = movement_data.get('drop_from_reference_decimal')
                tolerance = movement_data.get('tolerance_decimal')
                basis = movement_data.get('basis')
                bucket = movement_data.get('bucket')
                applied_rule = json.dumps(movement_data.get('applied_rule')) if movement_data.get('applied_rule') else None
                is_backfill = movement_data.get('backfilled', False)
            
                # Insert price check to PostgreSQL
                try:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO follow_the_goat_buyins_price_checks (
                                buyin_id, checked_at, current_price, entry_price, highest_price,
                                reference_price, gain_from_entry, drop_from_high, drop_from_entry,
                                drop_from_reference, tolerance, basis, bucket, applied_rule,
                                should_sell, is_backfill
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, [
                            position_id, checked_at, current_price, entry_price, highest_price,
                            reference_price, gain_from_entry, drop_from_high, drop_from_entry,
                            drop_from_reference, tolerance, basis, bucket, applied_rule,
                            should_sell, is_backfill
                        ])
                        # Autocommit handles the commit automatically
                        if not is_backfill:
                            logger.info(f"✓ Price check recorded for position {position_id}: ${current_price:.6f}")
                except Exception as e:
                    logger.error(f"Price_checks insert failed for position {position_id}: {e}")
            
                logger.debug(f"Price movement saved for position {position_id}: ${current_price:.6f}")
                return True
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error saving price movement for position {position_id}: {e}")
            return False
    
    def execute_sell(self, position: Dict[str, Any], check_result: Dict[str, Any]) -> bool:
        """
        Mark a position as sold and persist data to PostgreSQL.
        """
        position_id = position['id']
        logger.info(f"Executing sell for position #{position_id}")
        
        try:
            # Get fresh price for exit
            actual_exit_price = self.get_current_sol_price()
            if actual_exit_price is None:
                actual_exit_price = check_result['current_price']
            
            # Compute profit/loss percentage
            entry_price_value = float(position['our_entry_price']) if position.get('our_entry_price') else check_result['entry_price']
            actual_profit_loss_amount = actual_exit_price - entry_price_value
            percent_change = ((actual_profit_loss_amount / entry_price_value) * 100) if entry_price_value else 0.0
            
            # CRITICAL: Use UTC time, not local time (server is CET, database is UTC)
            exit_timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            # Get tracking data for this position
            with self.position_tracking_lock:
                tracking = self.position_tracking.get(position_id, {})
                highest_price = tracking.get('highest_price', check_result.get('highest_price', actual_exit_price))
                locked_tolerance = tracking.get('locked_tolerance')
            
            # Update PostgreSQL with sold status
            try:
                postgres_execute("""
                    UPDATE follow_the_goat_buyins 
                    SET our_exit_price = %s,
                        our_exit_timestamp = %s,
                        our_profit_loss = %s,
                        our_status = 'sold',
                        current_price = %s,
                        higest_price_reached = %s,
                        tolerance = %s
                    WHERE id = %s
                """, [actual_exit_price, exit_timestamp, percent_change, actual_exit_price, 
                      highest_price, locked_tolerance, position_id])
                logger.debug(f"PostgreSQL updated position {position_id} to sold")
            except Exception as e:
                logger.error(f"PostgreSQL sell update failed for {position_id}: {e}")
                return False
            
            # Update statistics
            with self.stats_lock:
                self.stats['positions_sold'] += 1
                self.stats['total_profit_loss'] += actual_profit_loss_amount
                
                if actual_profit_loss_amount > 0:
                    self.stats['winning_trades'] += 1
                    result_icon = "WIN"
                else:
                    self.stats['losing_trades'] += 1
                    result_icon = "LOSS"
            
            # Remove from tracking
            with self.position_tracking_lock:
                if position_id in self.position_tracking:
                    del self.position_tracking[position_id]
            
            logger.info(f"SOLD Position {position_id} - {result_icon}")
            logger.info(f"  Entry: ${entry_price_value:.4f}")
            logger.info(f"  Exit: ${actual_exit_price:.4f}")
            logger.info(f"  High: ${highest_price:.4f}")
            logger.info(f"  P&L: ${actual_profit_loss_amount:.4f} ({percent_change:.4f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error executing sell for position {position_id}: {e}")
            with self.stats_lock:
                self.stats['errors'] += 1
            return False
    
    def monitor_positions(self) -> int:
        """
        Check all open positions against current price.
        Uses batched database updates for performance.
        
        Returns:
            Number of positions checked.
        """
        cycle_start = time.time()
        
        # Step 1: Get current SOL price
        current_price = self.get_current_sol_price()
        if current_price is None:
            logger.warning("Could not get current price, skipping this check")
            return 0
        
        # Step 2: Get all open positions
        positions = self.get_open_positions()
        
        if not positions:
            return 0
        
        # Track count
        with self.stats_lock:
            self.stats['positions_monitored'] = len(positions)
            self.stats['cycles'] += 1
        
        logger.debug(f"Checking {len(positions)} position(s) @ ${current_price:.6f}")
        
        # Step 3: Check each position and collect updates
        positions_to_sell = []
        price_updates = []  # Collect all price updates for batch operation
        check_results = []  # Store results for post-processing
        
        for position in positions:
            try:
                check_result = self.check_position(position, current_price)
                
                # Collect price update for batch operation
                price_updates.append({
                    'position_id': position['id'],
                    'current_price': current_price
                })
                
                # Store check result for potential additional processing
                check_results.append((position, check_result))
                
                # Flag for selling if needed
                if check_result['should_sell']:
                    positions_to_sell.append((position, check_result))
                    
            except Exception as e:
                logger.error(f"Error checking position {position.get('id')}: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
        
        # Step 4: Batch update all current_prices in ONE query (CRITICAL for performance)
        if price_updates:
            batch_start = time.time()
            self.batch_update_current_prices(price_updates)
            batch_duration = time.time() - batch_start
            logger.debug(f"Batch price update for {len(price_updates)} positions took {batch_duration:.3f}s")
        
        # Step 5: Save price checks for ALL positions (for tracking/display on website)
        for position, check_result in check_results:
            try:
                # Save backfill data if present (new position)
                if check_result.get('backfill_data'):
                    self.save_price_movement(position['id'], check_result['backfill_data'], skip_price_update=True)
                
                # Save regular price check for this cycle (for website timeline display)
                if check_result.get('movement_data'):
                    self.save_price_movement(position['id'], check_result['movement_data'], skip_price_update=True)
                    
            except Exception as e:
                logger.error(f"Error saving movement for position {position.get('id')}: {e}")
        
        # Step 6: Execute sells
        for position, check_result in positions_to_sell:
            try:
                self.execute_sell(position, check_result)
            except Exception as e:
                logger.error(f"Error executing sell for position {position.get('id')}: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
        
        cycle_duration = time.time() - cycle_start
        if cycle_duration > 0.5:
            logger.warning(f"Cycle took {cycle_duration:.2f}s (>0.5s target)")
        else:
            logger.debug(f"Cycle completed in {cycle_duration:.3f}s")
        
        return len(positions)
    
    def print_status(self):
        """Print current status summary."""
        with self.stats_lock:
            uptime = (datetime.now() - self.stats['start_time']).total_seconds()
            stats_snapshot = self.stats.copy()
        
        logger.info("=" * 60)
        logger.info("STATUS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Open Positions: {stats_snapshot['positions_monitored']}")
        logger.info(f"Positions Sold: {stats_snapshot['positions_sold']}")
        logger.info(f"  Winning: {stats_snapshot['winning_trades']}")
        logger.info(f"  Losing: {stats_snapshot['losing_trades']}")
        logger.info(f"Total P&L: ${stats_snapshot['total_profit_loss']:.4f}")
        logger.info(f"Cycles: {stats_snapshot['cycles']}")
        logger.info(f"Errors: {stats_snapshot['errors']}")
        logger.info(f"Uptime: {uptime:.0f}s ({uptime/60:.1f}m)")
        logger.info("=" * 60)
    
    def run(self, interval_seconds: float = None):
        """Main loop - run continuously checking at specified interval."""
        if interval_seconds is None:
            interval_seconds = DEFAULT_MONITOR_INTERVAL_SECONDS
        
        logger.info("=" * 80)
        logger.info("TRAILING STOP SELLER - CONTINUOUS MODE")
        logger.info("=" * 80)
        logger.info("DECIMAL-BASED Trailing Stop Logic:")
        logger.info("  - Entry price is 'ground zero' - determines which rules apply")
        logger.info("  - Price > Entry: Use 'increases' rules (track drop from highest)")
        logger.info("  - Price < Entry: Use 'decreases' rules (track drop from entry)")
        logger.info(f"Monitoring interval: {interval_seconds}s")
        
        if self.monitor_live is None:
            logger.info("Monitoring filter: ALL (live and test trades)")
        else:
            logger.info(f"Monitoring filter: {'LIVE only' if self.monitor_live else 'TEST only'}")
        
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        last_status_time = time.time()
        
        while True:
            try:
                cycle_start = time.time()
                
                # Monitor all positions
                positions_checked = self.monitor_positions()
                
                # Show stats every 30 seconds
                if time.time() - last_status_time >= 30:
                    with self.stats_lock:
                        stats_snapshot = self.stats.copy()
                    logger.info(f"Status: {stats_snapshot['positions_monitored']} open, "
                              f"{stats_snapshot['positions_sold']} sold, "
                              f"P&L: ${stats_snapshot['total_profit_loss']:.4f}, "
                              f"Cycles: {stats_snapshot['cycles']}")
                    last_status_time = time.time()
                
                # Calculate sleep time to maintain interval
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0.1, interval_seconds - cycle_duration)
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("\nKeyboard interrupt received - shutting down...")
                break
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                time.sleep(5)  # Wait before retrying
        
        # Final status
        logger.info("\nFINAL STATISTICS")
        self.print_status()
        logger.info("Shutdown complete")


# =============================================================================
# MODULE-LEVEL FUNCTIONS FOR SCHEDULER
# =============================================================================

# Global seller instance for scheduler
_seller_instance: Optional[TrailingStopSeller] = None
_seller_lock = threading.Lock()


def get_seller() -> TrailingStopSeller:
    """Get or create the global seller instance."""
    global _seller_instance
    
    with _seller_lock:
        if _seller_instance is None:
            # Determine mode from environment
            live_mode = os.getenv('TRAILING_STOP_LIVE_MODE', '0') == '1'
            monitor_filter = os.getenv('TRAILING_STOP_MONITOR_FILTER', 'all').lower()
            
            if monitor_filter == 'live':
                monitor_live = True
            elif monitor_filter == 'test':
                monitor_live = False
            else:
                monitor_live = None  # Monitor all (both live and test)
            
            _seller_instance = TrailingStopSeller(
                live_trade=live_mode,
                monitor_live=monitor_live
            )
        
        return _seller_instance


def run_single_cycle() -> int:
    """
    Run a single monitoring cycle.
    Called by scheduler every 1 second.
    
    Returns:
        Number of positions checked.
    """
    try:
        seller = get_seller()
        return seller.monitor_positions()
    except Exception as e:
        logger.error(f"Error in trailing stop cycle: {e}")
        return 0


def run_trailing_stop_seller() -> int:
    """Alias for run_single_cycle for scheduler registration."""
    return run_single_cycle()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trailing Stop Seller - DuckDB Version")
    parser.add_argument("--once", action="store_true", help="Run a single monitoring cycle and exit")
    parser.add_argument("--continuous", action="store_true", help="Run continuously (default)")
    parser.add_argument("--live", action="store_true", help="Monitor LIVE trades only")
    parser.add_argument("--test", action="store_true", help="Monitor TEST trades only")
    parser.add_argument("--interval", type=float, default=1.0, help="Monitoring interval in seconds (default: 1.0)")
    args = parser.parse_args()
    
    # Determine monitoring filter
    if args.live:
        monitor_live = True
    elif args.test:
        monitor_live = False
    else:
        monitor_live = None  # Monitor all
    
    seller = TrailingStopSeller(
        live_trade=args.live,
        monitor_live=monitor_live
    )
    
    if args.once:
        logger.info("Running single cycle...")
        positions = seller.monitor_positions()
        seller.print_status()
        logger.info(f"Checked {positions} position(s). Exiting.")
    else:
        seller.run(interval_seconds=args.interval)


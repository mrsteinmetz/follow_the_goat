"""
Follow The Goat - Wallet Tracker
================================
Multi-play wallet tracker that follows top-performing wallets and their buy transactions.

This module:
- Loads play configurations from DuckDB
- Discovers wallets matching play criteria
- Monitors for new buy transactions (from DuckDB for speed)
- Generates 15-minute analytics trails
- Runs pattern validation
- Optionally executes Jupiter swaps

Architecture (DuckDB-first for maximum speed):
- DuckDB: Hot storage for ALL data (plays, buyins, trades, price data)
- MySQL: Archive only (writes go to both, reads from DuckDB)
- Trade detection: Runs every 1 second from DuckDB
- Trades synced from MySQL to DuckDB every 5 seconds by scheduler

Performance:
- Trade detection: <10ms (DuckDB in-memory queries)
- Bundle filtering: <50ms (DuckDB)
- Full cycle: <100ms typical

Usage:
    # Standalone execution
    python 000trading/follow_the_goat.py
    
    # As scheduled job (via scheduler/master.py)
    from follow_the_goat import run_single_cycle
    run_single_cycle()
    
    # Continuous mode
    python 000trading/follow_the_goat.py --continuous
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from logging.handlers import RotatingFileHandler, MemoryHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sys
PROJECT_ROOT = Path(__file__).parent.parent
MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_DIR))

from core.database import get_duckdb, get_mysql, dual_write_insert, dual_write_update, get_trading_engine

# Import our modules (direct imports after adding module dir to path)
from trail_generator import generate_trail_payload, TrailError
from trail_data import insert_trail_data
from pattern_validator import (
    validate_buyin_signal,
    clear_schema_cache,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_MONITOR_INTERVAL_SECONDS = float(
    os.getenv('FOLLOW_THE_GOAT_INTERVAL_SECONDS', '0.5')
)
DEFAULT_CONFIG_REFRESH_SECONDS = float(
    os.getenv('FOLLOW_THE_GOAT_CONFIG_REFRESH_SECONDS', '60.0')
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
    log_file = LOGS_DIR / "follow_the_goat.log"
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


def _str_to_bool(value: Optional[str], default: bool = False) -> bool:
    """Convert string to boolean."""
    if value is None:
        return default
    return value.strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


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
    """Structured step logger that captures durations and metadata."""
    
    def __init__(self) -> None:
        self.steps: List[Dict[str, Any]] = []
    
    def start(
        self,
        step_name: str,
        description: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            'step': step_name,
            'description': description,
            'details': make_json_safe(details) if details else {},
            'start_time': time.time()
        }
    
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


class PatternValidatorCriticalError(Exception):
    """Raised when pattern validator requirements are not satisfied."""


# =============================================================================
# WALLET FOLLOWER CLASS
# =============================================================================

class WalletFollower:
    """
    Follows top-performing wallets and tracks their buy transactions.
    
    Features:
    - Multi-play support with per-play configuration
    - Wallet caching with TTL
    - Bundle filtering (concurrent trade detection)
    - Perp mode filtering (long_only, short_only, any)
    - Max buys per cycle limiting
    - Pattern validation with project filters
    - Optional Jupiter swap execution
    """
    
    def __init__(
        self,
        enable_swaps: bool = False,
        swap_mode: str = 'simulate',
        live_trade: bool = False,
        monitor_interval_seconds: float = DEFAULT_MONITOR_INTERVAL_SECONDS,
        config_refresh_interval_seconds: float = DEFAULT_CONFIG_REFRESH_SECONDS,
    ):
        """
        Initialize WalletFollower.
        
        Args:
            enable_swaps: If True, executes Jupiter swaps when trades detected
            swap_mode: 'simulate' for simulation only, 'execute' for real swaps
            live_trade: If True, trades are live. If False, test mode
            monitor_interval_seconds: Seconds between monitoring loop iterations
            config_refresh_interval_seconds: Seconds between configuration refreshes
        """
        self.plays: List[Dict[str, Any]] = []
        self.target_wallets: List[str] = []
        self.wallet_play_map: Dict[str, List[Dict[str, Any]]] = {}
        self.last_trade_ids: Dict[str, int] = {}
        self._trade_id_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        
        self.stats = {
            'trades_followed': 0,
            'trades_blocked_max_buys': 0,
            'trades_blocked_validator': 0,
            'trades_blocked_perp_mode': 0,
            'swaps_executed': 0,
            'swap_errors': 0,
            'errors': 0,
            'start_time': datetime.now(),
            'wallets_tracked': 0,
            'plays_loaded': 0,
            'cycles_seen': set(),  # Track unique price cycles
        }
        
        # Configuration
        self.enable_swaps = enable_swaps
        self.swap_mode = swap_mode
        self.live_trade = live_trade
        self.monitor_interval_seconds = max(0.1, float(monitor_interval_seconds))
        self.config_refresh_interval_seconds = max(0.0, float(config_refresh_interval_seconds))
        self.shutdown_requested = False
        
        self._last_config_refresh: float = 0.0
        self._plays_signature: Optional[str] = None
        
        # Jupiter integration placeholder (can be enabled later)
        self.jupiter = None
        
        logger.info("WalletFollower initialized (live_trade=%s, swaps=%s)", 
                    live_trade, enable_swaps)
    
    def _increment_stat(self, stat_name: str, amount: int = 1) -> None:
        """Thread-safe stats increment."""
        with self._stats_lock:
            self.stats[stat_name] = self.stats.get(stat_name, 0) + amount
    
    def _update_last_trade_id(self, wallet_address: str, trade_id: int) -> None:
        """Thread-safe last_trade_ids update."""
        with self._trade_id_lock:
            self.last_trade_ids[wallet_address] = trade_id
    
    def _get_last_trade_id(self, wallet_address: str) -> int:
        """Thread-safe last_trade_ids read."""
        with self._trade_id_lock:
            return self.last_trade_ids.get(wallet_address, 0)
    
    # =========================================================================
    # PLAY CONFIGURATION
    # =========================================================================
    
    @staticmethod
    def _parse_config_field(field_value, field_name: str = '', play_id: Optional[int] = None):
        """Parse JSON configuration fields from database."""
        if field_value in (None, ''):
            return None
        if isinstance(field_value, (dict, list)):
            return field_value
        if isinstance(field_value, (bytes, bytearray)):
            try:
                field_value = field_value.decode('utf-8')
            except Exception:
                return None
        if isinstance(field_value, str):
            try:
                return json.loads(field_value)
            except json.JSONDecodeError as exc:
                context = f" for play {play_id}" if play_id is not None else ''
                logger.error(f"Invalid JSON in {field_name}{context}: {exc}")
                return None
        return None
    
    @staticmethod
    def _normalize_perp_mode(perp_config: Optional[Dict[str, Any]]) -> str:
        """Normalize perp mode configuration."""
        if not perp_config or not isinstance(perp_config, dict):
            return 'any'
        mode = perp_config.get('mode', 'any')
        if isinstance(mode, str):
            mode_lower = mode.lower()
            if mode_lower in {'long_only', 'short_only', 'any'}:
                return mode_lower
        return 'any'
    
    def get_all_plays(self, log_summary: bool = True) -> List[Dict[str, Any]]:
        """Fetch all active plays from DuckDB."""
        try:
            with get_duckdb("central") as conn:
                result = conn.execute("""
                    SELECT id, name, find_wallets_sql, max_buys_per_cycle,
                           pattern_validator_enable, pattern_validator,
                           tricker_on_perp, bundle_trades, 
                           cashe_wallets, cashe_wallets_settings,
                           project_ids, is_active
                    FROM follow_the_goat_plays
                    WHERE is_active = 1
                """)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                plays = [dict(zip(columns, row)) for row in rows]
            
            if not plays:
                logger.warning("No active plays found in follow_the_goat_plays")
                return []
            
            log_fn = logger.info if log_summary else logger.debug
            log_fn(f"Loaded {len(plays)} play(s)")
            
            for play in plays:
                play_id = play['id']
                
                # Parse pattern validator config
                raw_pattern = play.get('pattern_validator')
                parsed_pattern = self._parse_config_field(raw_pattern, 'pattern_validator', play_id)
                play['pattern_validator_config'] = parsed_pattern if isinstance(parsed_pattern, dict) else None
                
                # Normalize enable flag
                enable_flag = play.get('pattern_validator_enable')
                play['pattern_validator_enable'] = int(enable_flag) if enable_flag else 0
                
                # Parse project_ids
                project_ids_raw = play.get('project_ids')
                if project_ids_raw:
                    parsed = self._parse_config_field(project_ids_raw, 'project_ids', play_id)
                    play['project_ids'] = parsed if isinstance(parsed, list) else []
                else:
                    play['project_ids'] = []
                
                # Parse perp config
                perp_config = self._parse_config_field(
                    play.get('tricker_on_perp'), 'tricker_on_perp', play_id
                )
                play['perp_config'] = perp_config
                play['perp_mode'] = self._normalize_perp_mode(perp_config)
                
                # Parse bundle config
                bundle_config = self._parse_config_field(
                    play.get('bundle_trades'), 'bundle_trades', play_id
                )
                play['bundle_config'] = bundle_config if isinstance(bundle_config, dict) else None
                
                # Parse cache config
                cache_config = self._parse_config_field(
                    play.get('cashe_wallets'), 'cashe_wallets', play_id
                )
                cache_settings = self._parse_config_field(
                    play.get('cashe_wallets_settings'), 'cashe_wallets_settings', play_id
                )
                play['cache_config'] = cache_config if isinstance(cache_config, dict) else None
                play['cache_settings'] = cache_settings if isinstance(cache_settings, dict) else None
                
                # Log play summary
                play_name = play.get('name', f"Play {play_id}")
                perp_summary = '' if play['perp_mode'] == 'any' else f", perp: {play['perp_mode']}"
                validator_summary = ", validator: enabled" if play['pattern_validator_enable'] == 1 else ""
                if validator_summary and play['project_ids']:
                    validator_summary += f" ({len(play['project_ids'])} projects)"
                
                log_fn(
                    f"  Play #{play_id}: {play_name} (max {play['max_buys_per_cycle']} buys/cycle"
                    f"{perp_summary}{validator_summary})"
                )
            
            return plays
            
        except Exception as e:
            logger.error(f"Error fetching plays: {e}")
            return []
    
    # =========================================================================
    # WALLET CACHING
    # =========================================================================
    
    def is_cache_valid(
        self, 
        play_id: int, 
        cache_config: Optional[Dict[str, Any]], 
        cache_settings: Optional[Dict[str, Any]]
    ) -> bool:
        """Check if cached wallets are still valid for this play."""
        if not cache_config or not cache_config.get('enabled'):
            return False
        
        if not cache_settings or not isinstance(cache_settings, dict):
            return False
        
        cache_timestamp_str = cache_settings.get('timestamp')
        if not cache_timestamp_str:
            return False
        
        try:
            cache_timestamp = datetime.fromisoformat(
                cache_timestamp_str.replace('Z', '+00:00')
            )
            cache_seconds = int(cache_config.get('seconds', 60))
            expiration_time = cache_timestamp + timedelta(seconds=cache_seconds)
            now = datetime.now(timezone.utc)
            
            is_valid = now < expiration_time
            if is_valid:
                remaining = (expiration_time - now).total_seconds()
                logger.debug(f"Play #{play_id}: Cache valid (expires in {remaining:.0f}s)")
            
            return is_valid
        except (ValueError, TypeError) as e:
            logger.warning(f"Play #{play_id}: Error parsing cache timestamp: {e}")
            return False
    
    def save_wallets_to_cache(self, play_id: int, wallet_addresses: List[str]) -> None:
        """Save wallet addresses to cache with current timestamp."""
        try:
            cache_data = {
                'play_id': play_id,
                'timestamp': _utc_now_iso(),
                'wallets': wallet_addresses,
                'count': len(wallet_addresses)
            }
            cache_json = json.dumps(cache_data)
            
            # Update in DuckDB only (no MySQL writes)
            with get_duckdb("central") as conn:
                conn.execute("""
                    UPDATE follow_the_goat_plays
                    SET cashe_wallets_settings = ?
                    WHERE id = ?
                """, [cache_json, play_id])
            
            # Update in-memory
            for play in self.plays:
                if play.get('id') == play_id:
                    play['cache_settings'] = cache_data
                    break
            
            sample = wallet_addresses[:3] if wallet_addresses else []
            sample_str = ', '.join(w[:8] + '...' for w in sample) if sample else 'none'
            logger.info(f"Play #{play_id}: Cached {len(wallet_addresses)} wallet(s) [sample: {sample_str}]")
            
        except Exception as e:
            logger.error(f"Play #{play_id}: Error saving cache: {e}")
    
    # =========================================================================
    # WALLET DISCOVERY
    # =========================================================================
    
    def get_wallets_for_plays(self) -> List[Dict[str, Any]]:
        """Execute wallet discovery queries for all plays."""
        all_wallets_with_plays: List[Dict[str, Any]] = []
        run_start = time.time()
        
        for play in self.plays:
            play_id = play['id']
            play_name = play.get('name', f"Play {play_id}")
            max_buys = play['max_buys_per_cycle']
            perp_mode = play.get('perp_mode', 'any')
            bundle_config = play.get('bundle_config')
            cache_config = play.get('cache_config')
            cache_settings = play.get('cache_settings')
            
            # Check cache first
            if self.is_cache_valid(play_id, cache_config, cache_settings):
                cached_wallets = cache_settings.get('wallets', [])
                
                # Apply bundle filter on cached wallets
                filtered_wallets = cached_wallets
                if bundle_config and bundle_config.get('enabled'):
                    filtered_wallets, _ = self.filter_wallets_by_bundle(
                        cached_wallets, bundle_config, perp_mode, play_id, play_name
                    )
                
                for wallet_address in filtered_wallets:
                    if wallet_address:
                        all_wallets_with_plays.append({
                            'wallet_address': wallet_address,
                            'play_id': play_id,
                            'play_name': play_name,
                            'max_buys_per_cycle': max_buys,
                            'perp_mode': perp_mode,
                            'pattern_validator_enable': bool(play.get('pattern_validator_enable')),
                            'project_ids': play.get('project_ids', []),
                        })
                
                continue
            
            # Execute wallet discovery query against MySQL
            find_wallets_json = play.get('find_wallets_sql')
            if not find_wallets_json:
                logger.warning(f"Play #{play_id}: No find_wallets_sql configured")
                continue
            
            try:
                if isinstance(find_wallets_json, str):
                    query_data = json.loads(find_wallets_json)
                else:
                    query_data = find_wallets_json
                
                query = query_data.get('query')
                if not query:
                    logger.error(f"Play #{play_id}: Missing 'query' in find_wallets_sql")
                    continue
                
                # Execute against MySQL (source of wallet trades)
                with get_mysql() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        results = cursor.fetchall()
                
                initial_wallet_addresses = [
                    r.get('wallet_address') for r in results if r.get('wallet_address')
                ]
                
                if not initial_wallet_addresses:
                    logger.warning(f"Play #{play_id}: No wallets found")
                    continue
                
                logger.info(f"Play #{play_id}: Initial query returned {len(initial_wallet_addresses)} wallet(s)")
                
                # Apply bundle filter
                filtered_wallets = initial_wallet_addresses
                if bundle_config and bundle_config.get('enabled'):
                    filtered_wallets, _ = self.filter_wallets_by_bundle(
                        initial_wallet_addresses, bundle_config, perp_mode, play_id, play_name
                    )
                    logger.info(f"Play #{play_id}: Bundle filter kept {len(filtered_wallets)}/{len(initial_wallet_addresses)}")
                
                # Save to cache
                if cache_config and cache_config.get('enabled') and initial_wallet_addresses:
                    self.save_wallets_to_cache(play_id, initial_wallet_addresses)
                
                # Add to results
                for wallet_address in filtered_wallets:
                    if wallet_address:
                        all_wallets_with_plays.append({
                            'wallet_address': wallet_address,
                            'play_id': play_id,
                            'play_name': play_name,
                            'max_buys_per_cycle': max_buys,
                            'perp_mode': perp_mode,
                            'pattern_validator_enable': bool(play.get('pattern_validator_enable')),
                            'project_ids': play.get('project_ids', []),
                        })
                
            except json.JSONDecodeError as e:
                logger.error(f"Play #{play_id}: Invalid JSON in find_wallets_sql: {e}")
            except Exception as e:
                logger.error(f"Play #{play_id}: Error executing wallet query: {e}")
        
        total_ms = round((time.time() - run_start) * 1000, 3)
        logger.info(f"Wallet discovery completed in {total_ms}ms: {len(all_wallets_with_plays)} wallet-play combinations")
        
        return all_wallets_with_plays
    
    def filter_wallets_by_bundle(
        self,
        wallet_addresses: List[str],
        bundle_config: Dict[str, Any],
        perp_mode: str,
        play_id: int,
        play_name: str
    ) -> Tuple[List[str], Optional[Dict[str, Any]]]:
        """Filter wallets to only those trading together in a time window.
        
        Uses DuckDB for maximum speed.
        """
        if not wallet_addresses:
            return [], None
        
        if not bundle_config.get('enabled'):
            return wallet_addresses, None
        
        try:
            required_wallets = int(bundle_config.get('num_wallets', bundle_config.get('num_trades', 3)))
            seconds_threshold = int(bundle_config.get('seconds', 5))
            window_seconds = int(bundle_config.get('window_seconds', 600))
        except (TypeError, ValueError):
            return wallet_addresses, None
        
        required_wallets = max(required_wallets, 1)
        seconds_threshold = max(seconds_threshold, 1)
        window_seconds = max(window_seconds, seconds_threshold)
        
        # Build perp condition
        perp_condition = ''
        if perp_mode == 'long_only':
            perp_condition = " AND perp_direction = 'long'"
        elif perp_mode == 'short_only':
            perp_condition = " AND perp_direction = 'short'"
        
        # Build wallet list for DuckDB
        wallet_list = ", ".join([f"'{w}'" for w in wallet_addresses])
        
        # DuckDB query - simplified for speed
        # Find wallets that traded together within the time window
        try:
            with get_duckdb("central") as conn:
                # Get recent buy trades from target wallets
                result = conn.execute(f"""
                    SELECT wallet_address, trade_timestamp, 
                           EPOCH(trade_timestamp) AS ts_unix
                    FROM sol_stablecoin_trades
                    WHERE trade_timestamp >= NOW() - INTERVAL {window_seconds} SECOND
                      AND direction = 'buy'
                      AND wallet_address IN ({wallet_list})
                      {perp_condition}
                    ORDER BY trade_timestamp DESC
                """)
                trades = result.fetchall()
            
            if not trades:
                logger.info(f"Play #{play_id}: Bundle filter found no qualifying clusters")
                return [], None
            
            # Find clustering: wallets trading within seconds_threshold of each other
            # Group trades by time windows
            from collections import defaultdict
            
            # Find the best window with most wallets
            best_wallets = set()
            best_timestamp = None
            
            for i, (wallet, ts, ts_unix) in enumerate(trades):
                # Find all wallets that traded within seconds_threshold of this trade
                window_wallets = {wallet}
                for j, (w2, ts2, ts_unix2) in enumerate(trades):
                    if i != j and abs(ts_unix - ts_unix2) <= seconds_threshold:
                        window_wallets.add(w2)
                
                if len(window_wallets) >= required_wallets and len(window_wallets) > len(best_wallets):
                    best_wallets = window_wallets
                    best_timestamp = ts
            
            if len(best_wallets) < required_wallets:
                logger.info(f"Play #{play_id}: Bundle filter found no qualifying clusters")
                return [], None
            
            # Preserve original order
            ordered_filtered = [w for w in wallet_addresses if w in best_wallets]
            
            bundle_context = {
                'window_start': best_timestamp,
                'wallet_count': len(ordered_filtered),
                'participating_wallets': list(best_wallets),
                'required_wallet_count': required_wallets,
            }
            
            return ordered_filtered, bundle_context
            
        except Exception as e:
            logger.error(f"Play #{play_id}: Bundle filter error: {e}")
            return wallet_addresses, None
    
    # =========================================================================
    # CONFIGURATION REFRESH
    # =========================================================================
    
    def refresh_configuration(self, force: bool = False) -> bool:
        """Refresh play configuration and target wallet list."""
        now = time.time()
        if (
            not force
            and self.config_refresh_interval_seconds > 0.0
            and (now - self._last_config_refresh) < self.config_refresh_interval_seconds
        ):
            return False
        
        try:
            # Clear schema cache on forced refresh
            if force:
                logger.info("Forced refresh - clearing pattern validator schema cache...")
                clear_schema_cache(play_id=None)
            
            plays = self.get_all_plays(log_summary=force)
            if not plays:
                if force:
                    logger.error("No active plays found")
                self.plays = []
                self.target_wallets = []
                self.wallet_play_map = {}
                self._last_config_refresh = now
                return not force
            
            # Check if plays changed
            plays_signature = json.dumps(
                [{'id': p.get('id'), 'max_buys': p.get('max_buys_per_cycle')} for p in plays],
                sort_keys=True
            )
            plays_changed = plays_signature != self._plays_signature
            self._plays_signature = plays_signature
            self.plays = plays
            self.stats['plays_loaded'] = len(plays)
            
            # Clear schema cache if configuration changed
            if plays_changed:
                logger.info("Play configuration changed - clearing schema cache...")
                for play in plays:
                    clear_schema_cache(play_id=play.get('id'))
            
            # Discover wallets
            wallets_with_plays = self.get_wallets_for_plays()
            
            # Rebuild wallet maps
            ordered_wallets: List[str] = []
            seen_wallets = set()
            self.wallet_play_map = {}
            
            for entry in wallets_with_plays:
                wallet_address = entry.get('wallet_address')
                if not wallet_address or wallet_address in seen_wallets:
                    continue
                seen_wallets.add(wallet_address)
                ordered_wallets.append(wallet_address)
                
                play_id = entry.get('play_id')
                if wallet_address not in self.wallet_play_map:
                    self.wallet_play_map[wallet_address] = []
                
                self.wallet_play_map[wallet_address].append({
                    'play_id': play_id,
                    'play_name': entry.get('play_name'),
                    'max_buys_per_cycle': entry.get('max_buys_per_cycle'),
                    'perp_mode': entry.get('perp_mode', 'any'),
                    'pattern_validator_enable': entry.get('pattern_validator_enable'),
                    'project_ids': entry.get('project_ids', []),
                })
            
            # Update target wallets
            previous_wallets = set(self.target_wallets)
            self.target_wallets = ordered_wallets
            self.stats['wallets_tracked'] = len(self.target_wallets)
            
            # Initialize last_trade_ids for new wallets
            for wallet in ordered_wallets:
                if wallet not in previous_wallets:
                    self.last_trade_ids[wallet] = self.get_last_processed_trade_id(wallet)
            
            # Remove old wallets
            for wallet in previous_wallets - set(ordered_wallets):
                self.last_trade_ids.pop(wallet, None)
            
            self._last_config_refresh = now
            
            if force or plays_changed:
                logger.info(f"Configuration refreshed: {len(self.target_wallets)} wallet(s), {len(self.plays)} play(s)")
            
            return True
            
        except Exception as e:
            logger.error(f"Configuration refresh failed: {e}")
            return False
    
    def save_target_wallets(self, wallets_with_plays: List[Dict[str, Any]]) -> None:
        """Save target wallets to tracking table."""
        # This is handled by wallet_play_map in memory
        # The old MySQL version is not needed since we use DuckDB hot storage
        pass
    
    def get_last_processed_trade_id(self, wallet_address: str) -> int:
        """Get the last trade ID we processed for this wallet (from DuckDB)."""
        try:
            with get_duckdb("central") as conn:
                result = conn.execute("""
                    SELECT last_trade_id 
                    FROM follow_the_goat_tracking 
                    WHERE wallet_address = ?
                """, [wallet_address]).fetchone()
            
            if result:
                return result[0] if result[0] else 0
            return 0
        except Exception as e:
            logger.debug(f"Error getting last trade ID: {e}")
            return 0
    
    def update_last_processed_trade_id(self, wallet_address: str, trade_id: int) -> None:
        """Update the last processed trade ID (DuckDB only - no MySQL)."""
        try:
            with get_duckdb("central") as conn:
                # DuckDB upsert syntax
                conn.execute("""
                    INSERT INTO follow_the_goat_tracking 
                    (wallet_address, last_trade_id, last_checked_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (wallet_address) DO UPDATE SET
                        last_trade_id = excluded.last_trade_id,
                        last_checked_at = CURRENT_TIMESTAMP
                """, [wallet_address, trade_id])
        except Exception as e:
            logger.debug(f"DuckDB error updating last trade ID: {e}")
    
    # =========================================================================
    # TRADE DETECTION
    # =========================================================================
    
    def check_for_new_trades(self) -> List[Dict[str, Any]]:
        """Check for new buy transactions from all target wallets.
        
        Uses TradingDataEngine (in-memory) for instant trade detection.
        Falls back to file-based DuckDB if engine not available.
        """
        if not self.target_wallets:
            logger.debug("No target wallets configured - skipping trade check")
            return []
        
        try:
            # Find minimum last_id for efficient filtering
            min_last_id = min(
                self.last_trade_ids.get(wallet, 0) for wallet in self.target_wallets
            )
            
            # Build wallet list for query
            wallet_list = ", ".join([f"'{w}'" for w in self.target_wallets])
            
            # Try TradingDataEngine first (in-memory, instant)
            all_trades = []
            try:
                engine = get_trading_engine()
                if engine and engine._running:
                    results = engine.read(f"""
                        SELECT id, signature, trade_timestamp, stablecoin_amount, sol_amount, 
                               price, direction, wallet_address, perp_direction
                        FROM sol_stablecoin_trades 
                        WHERE wallet_address IN ({wallet_list})
                          AND direction = 'buy' 
                          AND id > ?
                          AND trade_timestamp >= NOW() - INTERVAL 5 MINUTE
                        ORDER BY wallet_address, id ASC
                    """, [min_last_id])
                    all_trades = list(results)
            except Exception as e:
                logger.debug(f"Engine read failed, falling back to file-DB: {e}")
            
            # Fallback to file-based DuckDB if engine didn't return results
            if not all_trades:
                with get_duckdb("central") as conn:
                    result = conn.execute(f"""
                        SELECT id, signature, trade_timestamp, stablecoin_amount, sol_amount, 
                               price, direction, wallet_address, perp_direction
                        FROM sol_stablecoin_trades 
                        WHERE wallet_address IN ({wallet_list})
                          AND direction = 'buy' 
                          AND id > ?
                          AND trade_timestamp >= NOW() - INTERVAL 5 MINUTE
                        ORDER BY wallet_address, id ASC
                    """, [min_last_id])
                    columns = [desc[0] for desc in result.description]
                    rows = result.fetchall()
                    all_trades = [dict(zip(columns, row)) for row in rows]
            
            # Filter by per-wallet last_id
            filtered_trades = []
            for trade in all_trades:
                wallet = trade['wallet_address']
                wallet_last_id = self.last_trade_ids.get(wallet, 0)
                if trade['id'] > wallet_last_id:
                    filtered_trades.append(trade)
            
            # Diagnostic logging for trade detection
            if filtered_trades:
                logger.debug(f"Trade check: {len(filtered_trades)} new trades from {len(set(t['wallet_address'] for t in filtered_trades))} wallets (min_last_id={min_last_id})")
            elif all_trades:
                # Trades exist but all filtered out (already processed)
                logger.debug(f"Trade check: {len(all_trades)} trades in window but all already processed (min_last_id={min_last_id})")
            
            return filtered_trades
            
        except Exception as e:
            logger.error(f"Error checking for new trades: {e}")
            return []
    
    @staticmethod
    def _trade_matches_perp_mode(trade: Dict[str, Any], perp_mode: str) -> bool:
        """Check if trade matches the play's perp mode."""
        if perp_mode not in {'long_only', 'short_only'}:
            return True
        
        trade_direction = trade.get('perp_direction')
        if not isinstance(trade_direction, str):
            return False
        
        trade_direction = trade_direction.lower()
        if perp_mode == 'long_only':
            return trade_direction == 'long'
        if perp_mode == 'short_only':
            return trade_direction == 'short'
        return True
    
    # =========================================================================
    # PRICE DATA
    # =========================================================================
    
    def get_current_market_price(self) -> Optional[float]:
        """Get current SOL price from price_points table."""
        try:
            with get_duckdb("central") as conn:
                result = conn.execute("""
                    SELECT value, id, created_at 
                    FROM price_points 
                    WHERE coin_id = 5
                    ORDER BY id DESC 
                    LIMIT 1
                """).fetchone()
            
            if result:
                return float(result[0])
            return None
        except Exception as e:
            logger.error(f"Error getting market price: {e}")
            return None
    
    def get_current_price_cycle(self, at_timestamp: Optional[datetime] = None) -> Optional[int]:
        """Get price cycle ID for a specific timestamp.
        
        Uses TradingDataEngine (in-memory DuckDB) since create_price_cycles.py
        writes cycle data there.
        """
        if at_timestamp is None:
            at_timestamp = datetime.now(timezone.utc)
        
        timestamp_str = at_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Use TradingDataEngine for cycle_tracker (it's written there by create_price_cycles.py)
            engine = get_trading_engine()
            if engine and engine._running:
                result = engine.read_one("""
                    SELECT id, cycle_start_time, cycle_end_time 
                    FROM cycle_tracker
                    WHERE threshold = 0.3
                      AND cycle_start_time <= ?
                      AND (cycle_end_time IS NULL OR cycle_end_time > ?)
                    ORDER BY id DESC
                    LIMIT 1
                """, [timestamp_str, timestamp_str])
                
                if result:
                    cycle_id = result['id']
                    cycle_start = result.get('cycle_start_time')
                    
                    # Log cycle age if it's suspiciously old (>1 hour)
                    if cycle_start:
                        try:
                            if hasattr(cycle_start, 'timestamp'):
                                start_ts = cycle_start.timestamp()
                            else:
                                start_ts = datetime.fromisoformat(str(cycle_start).replace('Z', '+00:00')).timestamp()
                            age_minutes = (at_timestamp.timestamp() - start_ts) / 60
                            if age_minutes > 60:
                                logger.warning(f"Price cycle {cycle_id} is {age_minutes:.0f} minutes old (started: {cycle_start})")
                        except Exception:
                            pass
                    
                    return cycle_id
                
                # No active cycle found - try to get count for diagnostics
                count_result = engine.read_one("SELECT COUNT(*) as cnt FROM cycle_tracker WHERE threshold = 0.3", [])
                total_cycles = count_result['cnt'] if count_result else 0
                logger.warning(f"No active price cycle found for timestamp {timestamp_str} (total 0.3% cycles: {total_cycles})")
                return None
            
            # Fallback to file-based DuckDB if engine not running
            logger.warning("TradingDataEngine not running - falling back to file-based DuckDB for cycle lookup")
            with get_duckdb("central") as conn:
                result = conn.execute("""
                    SELECT id, cycle_start_time, cycle_end_time 
                    FROM cycle_tracker
                    WHERE threshold = 0.3
                      AND cycle_start_time <= ?
                      AND (cycle_end_time IS NULL OR cycle_end_time > ?)
                    ORDER BY id DESC
                    LIMIT 1
                """, [timestamp_str, timestamp_str]).fetchone()
            
            if result:
                return result[0]
            
            logger.warning(f"No active price cycle found for timestamp {timestamp_str}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting price cycle: {e}")
            return None
    
    # =========================================================================
    # MAX BUYS CHECKING
    # =========================================================================
    
    def check_max_buys_per_cycle(
        self,
        play_id: int,
        price_cycle: Optional[int],
        max_buys_per_cycle: int,
        wallet_address: str
    ) -> Tuple[bool, str]:
        """Check if we can buy for this play + price_cycle combination.
        
        Returns:
            Tuple of (can_buy: bool, reason: str)
        """
        if price_cycle is None:
            logger.warning(f"No price_cycle - allowing buy for Play #{play_id}")
            return True, "no_cycle"
        
        try:
            with get_duckdb("central") as conn:
                # Check wallet already bought in this cycle
                result = conn.execute("""
                    SELECT COUNT(*) as wallet_buy_count
                    FROM follow_the_goat_buyins
                    WHERE play_id = ?
                      AND price_cycle = ?
                      AND wallet_address = ?
                """, [play_id, price_cycle, wallet_address]).fetchone()
                
                wallet_buy_count = result[0] if result else 0
                
                if wallet_buy_count > 0:
                    logger.debug(f"Play #{play_id}: Wallet {wallet_address[:8]}... already bought in cycle {price_cycle}")
                    return False, "wallet_already_bought"
                
                # Check total buys in cycle
                result = conn.execute("""
                    SELECT COUNT(*) as buy_count
                    FROM follow_the_goat_buyins
                    WHERE play_id = ?
                      AND price_cycle = ?
                """, [play_id, price_cycle]).fetchone()
                
                buy_count = result[0] if result else 0
                can_buy = buy_count < max_buys_per_cycle
                
                if not can_buy:
                    logger.debug(f"Play #{play_id}: Max buys reached ({buy_count}/{max_buys_per_cycle}) for cycle {price_cycle}")
                    return False, f"max_buys_reached:{buy_count}/{max_buys_per_cycle}"
                
                return True, "ok"
                
        except Exception as e:
            logger.error(f"Error checking max_buys_per_cycle: {e}")
            return True, "error"  # Allow on error
    
    # =========================================================================
    # BUYIN PROCESSING
    # =========================================================================
    
    def save_buyin_trade(
        self,
        trade: Dict[str, Any],
        play_id: int,
        play_info: Dict[str, Any]
    ) -> str:
        """Save a buy-in trade with trail generation and pattern validation.
        
        Returns:
            str: One of 'saved', 'blocked_max_buys', 'blocked_validator', 'error'
        """
        step_logger = StepLogger()
        max_buys_per_cycle = play_info.get('max_buys_per_cycle', 1)
        pattern_validator_enabled = play_info.get('pattern_validator_enable', False)
        project_ids = play_info.get('project_ids', [])
        
        overall_token = step_logger.start(
            'process_new_buyin',
            'Processing detected trade',
            make_json_safe({
                'play_id': play_id,
                'trade_id': trade.get('id'),
                'wallet_address': trade.get('wallet_address'),
                'live_trade': self.live_trade,
                'pattern_validator_enabled': pattern_validator_enabled,
            })
        )
        
        # Get price data
        current_market_price = self.get_current_market_price()
        current_price_cycle = self.get_current_price_cycle()
        
        # Track price cycle for diagnostics
        if current_price_cycle:
            with self._stats_lock:
                self.stats['cycles_seen'].add(current_price_cycle)
        
        # Check max buys
        can_buy, block_reason = self.check_max_buys_per_cycle(
            play_id, current_price_cycle, max_buys_per_cycle, trade['wallet_address']
        )
        if not can_buy:
            step_logger.end(overall_token, {
                'result': 'skipped', 
                'reason': block_reason,
                'price_cycle': current_price_cycle,
            }, status='skipped')
            return 'blocked_max_buys'
        
        # Determine entry price
        our_entry_price = current_market_price if current_market_price else float(trade.get('price', 0))
        
        # Initial status
        initial_status = 'validating' if pattern_validator_enabled else 'pending'
        
        insert_timestamp = datetime.now(timezone.utc)
        block_timestamp_str = insert_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format block_timestamp for database insertion
        block_ts = trade.get('trade_timestamp')
        if block_ts and hasattr(block_ts, 'strftime'):
            block_ts = block_ts.strftime('%Y-%m-%d %H:%M:%S')
        
        # Generate unique ID using timestamp (microseconds) - no MySQL needed
        # Format: YYYYMMDDHHMMSS + microseconds (ensures uniqueness)
        import random
        buyin_id = int(insert_timestamp.strftime('%Y%m%d%H%M%S')) * 1000 + random.randint(0, 999)
        
        # Insert directly into DuckDB (in-memory via TradingDataEngine)
        try:
            engine = get_trading_engine()
            if engine and engine._running:
                # Use in-memory engine for instant write
                engine.write('follow_the_goat_buyins', {
                    'id': buyin_id,
                    'play_id': play_id,
                    'wallet_address': trade['wallet_address'],
                    'original_trade_id': trade['id'],
                    'trade_signature': trade.get('signature'),
                    'block_timestamp': block_ts,
                    'quote_amount': trade.get('stablecoin_amount'),
                    'base_amount': trade.get('sol_amount'),
                    'price': trade.get('price'),
                    'direction': trade.get('direction', 'buy'),
                    'our_entry_price': our_entry_price,
                    'live_trade': 1 if self.live_trade else 0,
                    'price_cycle': current_price_cycle,
                    'our_status': initial_status,
                    'followed_at': block_timestamp_str
                })
                logger.debug(f"Engine insert successful, buyin_id={buyin_id}")
            else:
                # Fallback to file-based DuckDB
                with get_duckdb("central") as conn:
                    conn.execute("""
                        INSERT INTO follow_the_goat_buyins (
                            id, play_id, wallet_address, original_trade_id, trade_signature,
                            block_timestamp, quote_amount, base_amount, price, direction,
                            our_entry_price, live_trade, price_cycle, our_status, followed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        buyin_id, play_id, trade['wallet_address'], trade['id'], trade.get('signature'),
                        block_ts, trade.get('stablecoin_amount'), trade.get('sol_amount'),
                        trade.get('price'), trade.get('direction', 'buy'), our_entry_price,
                        1 if self.live_trade else 0, current_price_cycle, initial_status, block_timestamp_str
                    ])
                logger.debug(f"DuckDB insert successful, buyin_id={buyin_id}")
        except Exception as e:
            logger.error(f"Buyin insert failed: {e}")
            step_logger.fail(overall_token, str(e))
            return 'error'
        
        logger.info(f"Inserted buyin #{buyin_id} for trade {trade['id']} (Play #{play_id})")
        
        # Generate trail
        trail_generated = False
        try:
            trail_token = step_logger.start('generate_trail', 'Generating 15-minute trail')
            trail_payload = generate_trail_payload(buyin_id=buyin_id, persist=True)
            trail_generated = True
            step_logger.end(trail_token, {
                'minute_spans': len(trail_payload.get('price_movements', [])),
            })
            logger.info(f"Generated trail for buyin #{buyin_id}")
        except TrailError as e:
            step_logger.fail(trail_token, str(e))
            logger.warning(f"Trail generation failed for buyin #{buyin_id}: {e}")
        except Exception as e:
            step_logger.fail(trail_token, str(e))
            logger.error(f"Trail error for buyin #{buyin_id}: {e}")
        
        # Pattern validation
        validation_result = None
        final_status = initial_status
        should_follow = True
        
        if pattern_validator_enabled:
            if not trail_generated:
                # Cannot validate without trail
                validation_result = {
                    'decision': 'NO_GO',
                    'error': 'Trail generation failed',
                }
                should_follow = False
                final_status = 'no_go'
            else:
                try:
                    validation_token = step_logger.start('validate', 'Running pattern validation')
                    
                    validation_result = validate_buyin_signal(
                        buyin_id=buyin_id,
                        play_id=play_id,
                        project_ids=project_ids if project_ids else None,
                    )
                    
                    decision = validation_result.get('decision', 'UNKNOWN')
                    should_follow = decision == 'GO'
                    final_status = 'pending' if should_follow else 'no_go'
                    
                    step_logger.end(validation_token, {
                        'decision': decision,
                        'should_follow': should_follow,
                    })
                    
                    logger.info(f"Validation for buyin #{buyin_id}: {decision}")
                    
                except Exception as e:
                    step_logger.fail(validation_token, str(e))
                    validation_result = {'decision': 'ERROR', 'error': str(e)}
                    should_follow = False
                    final_status = 'no_go'
                    logger.error(f"Validation error for buyin #{buyin_id}: {e}")
        
        # Update buyin with validation results
        update_data = {
            'our_status': final_status,
        }
        
        if validation_result:
            update_data['pattern_validator_log'] = json.dumps(validation_result, default=str)
        
        # Re-fetch fresh price and cycle at final status
        if should_follow and pattern_validator_enabled:
            fresh_price = self.get_current_market_price()
            fresh_cycle = self.get_current_price_cycle()
            if fresh_price:
                update_data['our_entry_price'] = fresh_price
            if fresh_cycle:
                update_data['price_cycle'] = fresh_cycle
            update_data['followed_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            dual_write_update(
                'follow_the_goat_buyins',
                update_data,
                {'id': buyin_id}
            )
        except Exception as e:
            logger.error(f"Failed to update buyin #{buyin_id}: {e}")
        
        # Update entry log
        try:
            step_logger.end(overall_token, {
                'result': 'saved' if should_follow else 'blocked',
                'buyin_id': buyin_id,
                'decision': validation_result.get('decision') if validation_result else 'SKIPPED',
            })
            
            entry_log = json.dumps(step_logger.to_json())
            dual_write_update(
                'follow_the_goat_buyins',
                {'entry_log': entry_log},
                {'id': buyin_id}
            )
        except Exception:
            pass
        
        # Update stats and return result
        if should_follow:
            self._increment_stat('trades_followed')
            tracked_price = float(trade.get('price', 0))
            logger.info(
                f"✓ Buyin #{buyin_id} saved (Play #{play_id}): "
                f"Trade {trade['id']} @ ${tracked_price:.4f} | "
                f"Entry: ${update_data.get('our_entry_price', our_entry_price):.4f}"
            )
            return 'saved'
        else:
            logger.info(f"✗ Buyin #{buyin_id} blocked by validator (Play #{play_id})")
            return 'blocked_validator'
    
    # =========================================================================
    # TRADE PROCESSING
    # =========================================================================
    
    def process_new_trades(self, trades: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process new trades found.
        
        Returns:
            Dict with processing stats: {'processed': N, 'saved': N, 'blocked_max_buys': N, ...}
        """
        stats = {
            'processed': 0,
            'saved': 0,
            'blocked_max_buys': 0,
            'blocked_validator': 0,
            'blocked_perp_mode': 0,
            'errors': 0,
        }
        
        if not trades:
            return stats
        
        # Group by wallet
        trades_by_wallet: Dict[str, List[Dict[str, Any]]] = {}
        for trade in trades:
            wallet = trade['wallet_address']
            if wallet not in trades_by_wallet:
                trades_by_wallet[wallet] = []
            trades_by_wallet[wallet].append(trade)
        
        for wallet, wallet_trades in trades_by_wallet.items():
            logger.info(f"Found {len(wallet_trades)} new trade(s) from {wallet[:8]}...")
            
            play_infos = self.wallet_play_map.get(wallet)
            if not play_infos:
                logger.warning(f"No play info for {wallet[:8]}...")
                continue
            
            max_trade_id = max(t['id'] for t in wallet_trades)
            
            for trade in wallet_trades:
                for play_info in play_infos:
                    play_id = play_info['play_id']
                    perp_mode = play_info.get('perp_mode', 'any')
                    stats['processed'] += 1
                    
                    if not self._trade_matches_perp_mode(trade, perp_mode):
                        logger.debug(f"Trade {trade['id']} skipped for play {play_id} (perp_mode={perp_mode})")
                        stats['blocked_perp_mode'] += 1
                        self._increment_stat('trades_blocked_perp_mode')
                        continue
                    
                    try:
                        result = self.save_buyin_trade(trade, play_id, play_info)
                        if result == 'saved':
                            stats['saved'] += 1
                        elif result == 'blocked_max_buys':
                            stats['blocked_max_buys'] += 1
                            self._increment_stat('trades_blocked_max_buys')
                        elif result == 'blocked_validator':
                            stats['blocked_validator'] += 1
                            self._increment_stat('trades_blocked_validator')
                    except Exception as e:
                        logger.error(f"Error processing trade {trade['id']} for play {play_id}: {e}")
                        stats['errors'] += 1
                        self._increment_stat('errors')
            
            # Update last trade ID
            self._update_last_trade_id(wallet, max_trade_id)
            self.update_last_processed_trade_id(wallet, max_trade_id)
        
        return stats
    
    # =========================================================================
    # MAIN LOOP
    # =========================================================================
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current running statistics."""
        uptime = (datetime.now() - self.stats['start_time']).total_seconds()
        cycles_seen = self.stats.get('cycles_seen', set())
        return {
            'wallets_tracked': len(self.target_wallets),
            'trades_followed': self.stats['trades_followed'],
            'trades_blocked_max_buys': self.stats.get('trades_blocked_max_buys', 0),
            'trades_blocked_validator': self.stats.get('trades_blocked_validator', 0),
            'trades_blocked_perp_mode': self.stats.get('trades_blocked_perp_mode', 0),
            'swaps_executed': self.stats['swaps_executed'],
            'errors': self.stats['errors'],
            'uptime_seconds': uptime,
            'plays_loaded': self.stats['plays_loaded'],
            'unique_cycles_seen': len(cycles_seen),
            'recent_cycles': sorted(list(cycles_seen))[-5:] if cycles_seen else [],
        }
    
    def run_single_cycle(self) -> bool:
        """Run a single monitoring cycle. Returns True if trades were found."""
        try:
            self.refresh_configuration()
            
            if not self.target_wallets:
                logger.debug("No target wallets - skipping cycle")
                return False
            
            new_trades = self.check_for_new_trades()
            
            if new_trades:
                stats = self.process_new_trades(new_trades)
                
                # Log summary if any blocking happened
                if stats['blocked_max_buys'] > 0 or stats['blocked_validator'] > 0:
                    # Get current cycle info for context
                    current_cycle = self.get_current_price_cycle()
                    cycles_seen = list(self.stats.get('cycles_seen', set()))
                    
                    logger.info(
                        f"📊 Cycle summary: {stats['processed']} processed, "
                        f"{stats['saved']} saved, "
                        f"{stats['blocked_max_buys']} blocked (max_buys), "
                        f"{stats['blocked_validator']} blocked (validator), "
                        f"{stats['blocked_perp_mode']} blocked (perp_mode) | "
                        f"Current cycle: {current_cycle}, "
                        f"Cycles seen: {cycles_seen[-5:] if len(cycles_seen) > 5 else cycles_seen}"
                    )
                
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            self._increment_stat('errors')
            return False
    
    def run(self, run_once: bool = False) -> bool:
        """Main loop - run continuously or once."""
        self.shutdown_requested = False
        target_interval = max(0.1, self.monitor_interval_seconds)
        
        logger.info("=" * 80)
        logger.info("FOLLOW THE GOAT - WALLET TRACKER")
        logger.info(f"Mode: {'SINGLE RUN' if run_once else 'CONTINUOUS'}")
        logger.info(f"Live trade: {self.live_trade}")
        logger.info(f"Interval: {target_interval:.1f}s")
        logger.info("=" * 80)
        
        # Initial configuration load
        if not self.refresh_configuration(force=True):
            logger.error("Initial configuration load failed")
            return True
        
        if not self.target_wallets:
            logger.error("No target wallets found")
            return True
        
        logger.info(f"Tracking {len(self.target_wallets)} wallet(s) across {len(self.plays)} play(s)")
        
        cycle_count = 0
        last_status_time = time.time()
        
        while not self.shutdown_requested:
            try:
                cycle_count += 1
                cycle_start = time.time()
                
                self.run_single_cycle()
                
                # Show stats periodically
                if not run_once and time.time() - last_status_time >= 30:
                    stats = self.get_statistics()
                    logger.info(
                        f"📊 Status: {stats['trades_followed']} saved, "
                        f"{stats['trades_blocked_max_buys']} blocked (max_buys), "
                        f"{stats['trades_blocked_validator']} blocked (validator), "
                        f"{stats['errors']} errors | "
                        f"Cycles: {stats['recent_cycles']}"
                    )
                    last_status_time = time.time()
                
                if run_once:
                    logger.info("Single run complete")
                    break
                
                # Sleep to maintain interval
                cycle_duration = time.time() - cycle_start
                sleep_time = max(0.1, target_interval - cycle_duration)
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt - shutting down...")
                self.shutdown_requested = True
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                self._increment_stat('errors')
                time.sleep(5)
        
        # Final stats
        stats = self.get_statistics()
        logger.info("=" * 80)
        logger.info("FINAL STATISTICS")
        logger.info(f"Trades followed: {stats['trades_followed']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Uptime: {stats['uptime_seconds']:.0f}s")
        logger.info("=" * 80)
        
        return self.shutdown_requested


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_follower_instance: Optional[WalletFollower] = None
_follower_lock = threading.Lock()


def get_follower() -> WalletFollower:
    """Get or create the singleton WalletFollower instance."""
    global _follower_instance
    if _follower_instance is None:
        with _follower_lock:
            if _follower_instance is None:
                _follower_instance = WalletFollower(
                    enable_swaps=_str_to_bool(os.getenv('FOLLOW_THE_GOAT_ENABLE_SWAPS'), False),
                    live_trade=_str_to_bool(os.getenv('FOLLOW_THE_GOAT_LIVE_TRADE'), False),
                )
    return _follower_instance


def run_single_cycle() -> bool:
    """Run a single monitoring cycle. Called by scheduler."""
    follower = get_follower()
    return follower.run_single_cycle()


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Follow The Goat - Wallet Tracker")
    parser.add_argument("--once", action="store_true", help="Run single cycle and exit")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_MONITOR_INTERVAL_SECONDS,
        help=f"Monitor interval in seconds (default: {DEFAULT_MONITOR_INTERVAL_SECONDS})"
    )
    parser.add_argument("--live-trade", action="store_true", help="Enable live trading")
    args = parser.parse_args()
    
    follower = WalletFollower(
        enable_swaps=False,
        live_trade=args.live_trade,
        monitor_interval_seconds=args.interval,
    )
    
    if args.once:
        follower.refresh_configuration(force=True)
        follower.run_single_cycle()
    else:
        follower.run(run_once=not args.continuous)


if __name__ == "__main__":
    main()


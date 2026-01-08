"""
Binance Order Book Stream
=========================
Streams SOL/USDT order book data from Binance WebSocket.
Dual-writes to in-memory DuckDB (TradingDataEngine) and MySQL.

Migrated from: 000old_code/solana_node/binance/order_book_streaming.py

Architecture:
- WebSocket connection to Binance (100ms updates)
- Non-blocking writes via TradingDataEngine
- Auto-reconnect on disconnect
- Background MySQL sync (handled by engine)

Usage:
    # Start stream (called by scheduler/master.py)
    from stream_binance_order_book_data import start_binance_stream
    collector = start_binance_stream()
    
    # Stop stream
    collector.stop()
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import websocket
import json
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from collections import deque

from core.database import postgres_insert
from core.config import settings

# Configure logging
logger = logging.getLogger("binance_stream")


def _compute_microprice(best_bid: float, bid_size: float, best_ask: float, ask_size: float) -> float:
    """Compute microprice (size-weighted mid price)."""
    denom = bid_size + ask_size
    if denom <= 0:
        return (best_bid + best_ask) / 2.0
    return (best_bid * ask_size + best_ask * bid_size) / denom


def _depth_within_bps(levels: List[List[float]], mid: float, bps: float, is_bids: bool) -> float:
    """Calculate total depth within X basis points of mid price."""
    if mid <= 0:
        return 0.0
    limit = mid * (1.0 - bps / 10000.0) if is_bids else mid * (1.0 + bps / 10000.0)
    total = 0.0
    for price, size in levels:
        if is_bids:
            if price >= limit:
                total += float(size)
            else:
                break
        else:
            if price <= limit:
                total += float(size)
            else:
                break
    return float(total)


class BinanceOrderBookCollector:
    """
    Binance order book collector with WebSocket streaming.
    
    Streams order book data and writes to TradingDataEngine (in-memory DuckDB).
    The engine handles MySQL sync automatically in the background.
    """
    
    def __init__(self, symbol: str = "SOLUSDT", mode: str = "conservative"):
        """
        Initialize the collector.
        
        Args:
            symbol: Trading pair symbol (default: SOLUSDT)
            mode: Rate limiting mode - "conservative" (2% of limits) or "aggressive" (12%)
        """
        self.symbol = symbol
        self.mode = mode
        
        # Rate limiting configuration
        self.rate_limits = {
            'conservative': {
                'websocket_depth': 20,
                'websocket_interval': '100ms',
                'description': 'Safe mode: WebSocket depth@100ms, 2% of limit'
            },
            'aggressive': {
                'websocket_depth': 50,
                'websocket_interval': '100ms',
                'description': 'Aggressive mode: WebSocket depth@100ms, 12% of limit'
            }
        }
        
        self.config = self.rate_limits[self.mode]
        logger.info(f"Initialized {self.mode} mode: {self.config['description']}")
        
        # WebSocket URLs
        self.ws_urls = {
            'depth5': f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth5@100ms",
            'depth10': f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth10@100ms",
            'depth20': f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth20@100ms",
            'depth50': f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth50@100ms"
        }
        
        # Data buffers (for feature calculations like net_liquidity_change_1s)
        self.features_buffer = deque(maxlen=1000)
        
        # State
        self.is_streaming = False
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.message_count = 0
        self.start_time = time.time()
        
        # Statistics
        self.stats = {
            'websocket_messages': 0,
            'writes_queued': 0,
            'errors': 0,
            'last_update': None
        }
    
    def calculate_features(self, orderbook_data: Dict) -> Optional[Dict]:
        """
        Calculate trading features from order book data.
        
        Returns a dict with all feature columns for the order_book_features table.
        """
        if not orderbook_data or 'bids' not in orderbook_data or 'asks' not in orderbook_data:
            return None
        
        bids = orderbook_data['bids']
        asks = orderbook_data['asks']
        
        if not bids or not asks:
            return None
        
        # Basic price levels
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2
        absolute_spread = max(0.0, best_ask - best_bid)
        relative_spread_bps = (absolute_spread / mid_price * 10000) if mid_price > 0 else 0.0
        
        # Depth calculations (top 10 levels)
        bid_depth_10 = float(sum(b[1] for b in bids[:10]))
        ask_depth_10 = float(sum(a[1] for a in asks[:10]))
        total_depth_10 = bid_depth_10 + ask_depth_10
        volume_imbalance = (bid_depth_10 - ask_depth_10) / total_depth_10 if total_depth_10 > 0 else 0.0
        
        # VWAP calculations
        bid_vwap = float(sum(b[0] * b[1] for b in bids[:10]) / bid_depth_10) if bid_depth_10 > 0 else 0.0
        ask_vwap = float(sum(a[0] * a[1] for a in asks[:10]) / ask_depth_10) if ask_depth_10 > 0 else 0.0
        
        # Slope calculations (price-size regression)
        def _slope(levels: List[List[float]]) -> float:
            if len(levels) < 2:
                return 0.0
            prices = [lvl[0] for lvl in levels]
            sizes = [lvl[1] for lvl in levels]
            n = len(prices)
            sum_x = sum(prices)
            sum_y = sum(sizes)
            sum_xy = sum(x * y for x, y in zip(prices, sizes))
            sum_x2 = sum(x * x for x in prices)
            denom = (n * sum_x2 - sum_x * sum_x)
            if denom == 0:
                return 0.0
            return float((n * sum_xy - sum_x * sum_y) / denom)
        
        bid_slope = _slope(bids[:5]) if len(bids) >= 5 else 0.0
        ask_slope = _slope(asks[:5]) if len(asks) >= 5 else 0.0
        
        # Microprice (size-weighted)
        top_bid_size = float(bids[0][1]) if bids else 0.0
        top_ask_size = float(asks[0][1]) if asks else 0.0
        microprice = _compute_microprice(best_bid, top_bid_size, best_ask, top_ask_size)
        microprice_dev_bps = ((microprice - mid_price) / mid_price * 10000) if mid_price > 0 else 0.0
        
        # Depth within basis points
        bid_depth_bps_5 = _depth_within_bps(bids, mid_price, 5.0, True)
        ask_depth_bps_5 = _depth_within_bps(asks, mid_price, 5.0, False)
        bid_depth_bps_10 = _depth_within_bps(bids, mid_price, 10.0, True)
        ask_depth_bps_10 = _depth_within_bps(asks, mid_price, 10.0, False)
        bid_depth_bps_25 = _depth_within_bps(bids, mid_price, 25.0, True)
        ask_depth_bps_25 = _depth_within_bps(asks, mid_price, 25.0, False)
        
        # Depth imbalance ratio (10bps)
        depth_imbalance_10bps = (bid_depth_bps_10 - ask_depth_bps_10) / (bid_depth_bps_10 + ask_depth_bps_10) if (bid_depth_bps_10 + ask_depth_bps_10) > 0 else 0.0
        
        # VWAP within 10bps
        vwap_10bps = (bid_vwap * bid_depth_10 + ask_vwap * ask_depth_10) / total_depth_10 if total_depth_10 > 0 else 0.0
        
        # Net liquidity change over ~1 second
        net_liquidity_change_1s = None
        try:
            cutoff = orderbook_data['timestamp'] - timedelta(seconds=1)
            prev = None
            for f in reversed(self.features_buffer):
                if f.get('timestamp', f.get('ts', datetime.min)) <= cutoff:
                    prev = f
                    break
            if prev:
                prev_total = float(prev.get('total_depth_10', 0.0))
                net_liquidity_change_1s = float(total_depth_10 - prev_total)
        except Exception:
            net_liquidity_change_1s = None
        
        # Venue and quote asset
        venue = 'binance'
        quote_asset = 'USDT' if self.symbol.endswith('USDT') else 'BUSD'
        
        features = {
            'timestamp': orderbook_data['timestamp'],  # Changed from 'ts' to match table schema
            'mid_price': mid_price,
            'spread_bps': relative_spread_bps,
            'bid_liquidity': bid_depth_10,
            'ask_liquidity': ask_depth_10,
            'volume_imbalance': volume_imbalance,
            'depth_imbalance_ratio': depth_imbalance_10bps,
            'microprice': microprice,
            'vwap': vwap_10bps,
            'total_depth_10': total_depth_10,
            'bid_vwap_10': bid_vwap,
            'ask_vwap_10': ask_vwap,
            'bid_slope': bid_slope,
            'ask_slope': ask_slope,
            'microprice_dev_bps': microprice_dev_bps,
            'bid_depth_bps_5': bid_depth_bps_5,
            'ask_depth_bps_5': ask_depth_bps_5,
            'bid_depth_bps_10': bid_depth_bps_10,
            'ask_depth_bps_10': ask_depth_bps_10,
            'bid_depth_bps_25': bid_depth_bps_25,
            'ask_depth_bps_25': ask_depth_bps_25,
            'net_liquidity_change_1s': net_liquidity_change_1s,
            'bids_json': json.dumps(bids[:20]),
            'asks_json': json.dumps(asks[:20]),
            'source': orderbook_data['source']
        }
        
        return features
    
    def _write_to_engine(self, features: Dict) -> bool:
        """Write features to PostgreSQL."""
        try:
            # Write to PostgreSQL
            postgres_insert('order_book_features', features)
            self.stats['writes_queued'] += 1
            return True
        except Exception as e:
            logger.error(f"PostgreSQL write error: {e}")
            self.stats['errors'] += 1
            return False
    
    def start(self, callback_func: Optional[Callable] = None, auto_restart: bool = True):
        """
        Start WebSocket streaming.
        
        Args:
            callback_func: Optional callback for each update (features, orderbook)
            auto_restart: Auto-reconnect on disconnect (default: True)
        """
        def on_message(ws, message):
            try:
                self.message_count += 1
                data = json.loads(message)
                
                orderbook = {
                    'timestamp': datetime.utcnow(),
                    'bids': [[float(bid[0]), float(bid[1])] for bid in data['bids']],
                    'asks': [[float(ask[0]), float(ask[1])] for ask in data['asks']],
                    'source': 'WEBSOCKET'
                }
                
                features = self.calculate_features(orderbook)
                if features:
                    # Add to buffer for net_liquidity_change calculation
                    self.features_buffer.append(features)
                    
                    # Write to engine (non-blocking)
                    self._write_to_engine(features)

                    self.stats['websocket_messages'] += 1
                    self.stats['last_update'] = features.get('timestamp', features.get('ts'))
                
                # Call user callback if provided
                if callback_func:
                    callback_func(features, orderbook)
                else:
                    # Log every 100th message
                    if self.stats['websocket_messages'] % 100 == 0:
                        logger.info(
                            f"WS: {self.symbol} @ ${features.get('mid_price', 0):.2f} | "
                            f"Spread: {features.get('relative_spread_bps', 0):.2f}bps | "
                            f"[{self.stats['websocket_messages']} messages]"
                        )
                
            except Exception as e:
                logger.error(f"WebSocket message error: {e}")
                self.stats['errors'] += 1
        
        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")
            self.stats['errors'] += 1
        
        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
            self.is_streaming = False
            
            if auto_restart and not getattr(self, '_stopping', False):
                logger.info("Auto-restarting WebSocket in 5 seconds...")
                time.sleep(5)
                self.start(callback_func, auto_restart)
        
        def on_open(ws):
            logger.info(f"WebSocket connected: {self.symbol} ({self.mode} mode)")
            self.is_streaming = True
            self.start_time = time.time()
            self.message_count = 0
        
        # Select WebSocket URL based on depth config
        depth_key = f"depth{self.config['websocket_depth']}"
        ws_url = self.ws_urls.get(depth_key, self.ws_urls['depth20'])
        
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Run WebSocket in background thread
        self.ws_thread = threading.Thread(
            target=self.ws.run_forever,
            name="BinanceWebSocket",
            daemon=True
        )
        self.ws_thread.start()
        
        logger.info(f"WebSocket stream started: {ws_url}")
    
    def stop(self):
        """Stop the WebSocket stream."""
        self._stopping = True
        if self.ws:
            self.ws.close()
        self.is_streaming = False
        logger.info("WebSocket stream stopped")
    
    def get_statistics(self) -> Dict:
        """Get current streaming statistics."""
        uptime = time.time() - self.start_time if hasattr(self, 'start_time') else 0
        
        return {
            'mode': self.mode,
            'symbol': self.symbol,
            'uptime_seconds': uptime,
            'websocket_messages': self.stats['websocket_messages'],
            'writes_queued': self.stats['writes_queued'],
            'errors': self.stats['errors'],
            'buffer_size': len(self.features_buffer),
            'is_streaming': self.is_streaming,
            'last_update': self.stats['last_update'],
            'messages_per_minute': (self.stats['websocket_messages'] / uptime * 60) if uptime > 0 else 0
        }


# =============================================================================
# Module-level functions for scheduler integration
# =============================================================================

_collector: Optional[BinanceOrderBookCollector] = None


def start_binance_stream(symbol: str = "SOLUSDT", mode: str = "conservative") -> BinanceOrderBookCollector:
    """
    Start the Binance order book stream.
    
    Called by scheduler/master.py at startup.
    
    Args:
        symbol: Trading pair (default: SOLUSDT)
        mode: Rate limit mode (default: conservative)
        
    Returns:
        The collector instance
    """
    global _collector
    
    if _collector is not None and _collector.is_streaming:
        logger.warning("Binance stream already running")
        return _collector
    
    _collector = BinanceOrderBookCollector(symbol=symbol, mode=mode)
    _collector.start()
    
    return _collector


def stop_binance_stream():
    """Stop the Binance order book stream."""
    global _collector
    
    if _collector is not None:
        _collector.stop()
        _collector = None


def get_binance_collector() -> Optional[BinanceOrderBookCollector]:
    """Get the current collector instance."""
    return _collector


# =============================================================================
# Standalone execution for testing
# =============================================================================

if __name__ == "__main__":
    import signal
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    print("=" * 60)
    print("Binance Order Book Stream - Test Mode")
    print("=" * 60)
    print("Starting stream... Press Ctrl+C to stop.")
    print()
    
    def signal_handler(sig, frame):
        print("\nStopping stream...")
        stop_binance_stream()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start stream
    collector = start_binance_stream()
    
    # Keep running and show stats every 30 seconds
    try:
        while True:
            time.sleep(30)
            stats = collector.get_statistics()
            print(f"\n--- Stats ---")
            print(f"Messages: {stats['websocket_messages']:,}")
            print(f"Writes: {stats['writes_queued']:,}")
            print(f"Errors: {stats['errors']}")
            print(f"Rate: {stats['messages_per_minute']:.1f} msg/min")
            print(f"Uptime: {stats['uptime_seconds']/60:.1f} min")
    except KeyboardInterrupt:
        pass
    
    stop_binance_stream()
    print("\nStream stopped.")


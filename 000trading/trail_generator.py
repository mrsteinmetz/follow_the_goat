"""
15-Minute Trail Generator - Enhanced for Micro-Movement Detection
==================================================================
Generate analytics trail data for buy-in signals using PostgreSQL.

This module fetches order book, transactions, whale activity, and price data
and computes derived metrics optimized for detecting 0.5% price climbs.

Enhanced Features:
- Micro-pattern detection for sub-1% moves
- Field type tracking (is_ratio metadata)
- Multi-signal confirmation scoring
- Time-series derivatives and acceleration metrics
- Cross-market correlation analysis

Trail data is stored in the `buyin_trail_minutes` table (one row per minute,
15 rows per buyin) for efficient querying and pattern validation.

Usage:
    from trail_generator import generate_trail_payload
    
    payload = generate_trail_payload(buyin_id=123)
    # Automatically persists to buyin_trail_minutes table
    
    # To skip table persistence:
    payload = generate_trail_payload(buyin_id=123, persist=False)
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timedelta, date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Add project root to path
import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from core.webhook_client import WebhookClient
from trail_data import insert_trail_data

logger = logging.getLogger(__name__)

# Singleton webhook client for fetching trades/whale data from .NET webhook
_webhook_client: Optional[WebhookClient] = None

def _get_webhook_client() -> WebhookClient:
    """Get or create the webhook client singleton."""
    global _webhook_client
    if _webhook_client is None:
        _webhook_client = WebhookClient()
    return _webhook_client

# Try to get TradingDataEngine for in-memory queries (when running under scheduler)
def _get_engine_if_running():
    """Get TradingDataEngine if it's running, otherwise return None."""
    try:
        from core.trading_engine import _engine_instance
        if _engine_instance is not None and _engine_instance._running:
            return _engine_instance
    except Exception:
        pass
    return None


def _execute_query(query: str, params: list = None, as_dict: bool = True, graceful: bool = True):
    """Execute a query directly against PostgreSQL.
    
    PostgreSQL-only architecture - no DuckDB fallback needed.
    
    Args:
        query: SQL query (use ? for placeholders - will be converted to %s)
        params: Query parameters
        as_dict: If True, return list of dicts; if False, return list of tuples
        graceful: If True, return empty list on errors instead of raising
    
    Returns:
        List of dicts (if as_dict=True) or list of tuples
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Convert DuckDB-style ? placeholders to PostgreSQL %s
                pg_query = query.replace('?', '%s')
                cursor.execute(pg_query, params or [])
                
                if as_dict:
                    results = cursor.fetchall()
                    return results if results else []
                
                # For non-dict format, convert dicts to tuples
                rows = cursor.fetchall()
                return [tuple(r.values()) for r in rows] if rows else []
                
    except Exception as e:
        error_msg = str(e).lower()
        if graceful and ("does not exist" in error_msg or "relation" in error_msg):
            logger.debug(f"Table not found (graceful mode): {e}")
            return []
        
        logger.error(f"PostgreSQL query failed: {e}", exc_info=True)
        if graceful:
            return []
        raise

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "SOLUSDT")
DEFAULT_LOOKBACK_MINUTES = int(os.getenv("TRAIL_LOOKBACK_MINUTES", "15"))
TRAIL_COLUMN_NAME = "fifteen_min_trail"  # DuckDB uses this name (no numeric prefix)


# =============================================================================
# FIELD TYPE SCHEMA - RATIO VS VALUE TRACKING
# =============================================================================

FIELD_TYPE_SCHEMA = {
    "order_book_signals": {
        # Ratios (1)
        "volume_imbalance": 1,
        "relative_spread_bps": 1,
        "microprice_dev_bps": 1,
        "bid_depth_bps_10": 1,
        "ask_depth_bps_10": 1,
        "depth_imbalance_ratio": 1,
        "bid_liquidity_share_pct": 1,
        "ask_liquidity_share_pct": 1,
        "depth_imbalance_pct": 1,
        "liquidity_change_3m": 1,
        "microprice_acceleration_2m": 1,
        "aggression_ratio": 1,
        "vwap_spread_bps": 1,
        "net_flow_to_liquidity_ratio": 1,
        "price_change_1m": 1,
        "price_change_5m": 1,
        "price_change_10m": 1,
        "imbalance_shift_1m": 1,
        # Values (0)
        "mid_price": 0,
        "microprice": 0,
        "bid_depth_10": 0,
        "ask_depth_10": 0,
        "total_depth_10": 0,
        "total_liquidity": 0,
        "bid_slope": 0,
        "ask_slope": 0,
        "bid_vwap_10": 0,
        "ask_vwap_10": 0,
        "net_liquidity_change_sum": 0,
        "net_flow_5m": 0,
        "sample_count": 0,
        "coverage_seconds": 0,
    },
    "transactions": {
        # Ratios (1)
        "buy_sell_pressure": 1,
        "buy_volume_pct": 1,
        "sell_volume_pct": 1,
        "pressure_shift_1m": 1,
        "long_short_ratio": 1,
        "long_volume_pct": 1,
        "short_volume_pct": 1,
        "perp_position_skew_pct": 1,
        "perp_dominance_pct": 1,
        "volume_acceleration_ratio": 1,
        "whale_volume_pct": 1,
        "trades_per_second": 1,
        "buy_trade_pct": 1,
        "price_change_1m": 1,
        "price_volatility_pct": 1,
        # Values (0)
        "total_volume_usd": 0,
        "avg_trade_size": 0,
        "trade_count": 0,
        "large_trade_count": 0,
        "vwap": 0,
    },
    "whale_activity": {
        # Ratios (1)
        "net_flow_ratio": 1,
        "flow_shift_1m": 1,
        "accumulation_ratio": 1,
        "inflow_share_pct": 1,
        "outflow_share_pct": 1,
        "net_flow_strength_pct": 1,
        "strong_accumulation_pct": 1,
        "strong_distribution_pct": 1,
        "massive_move_pct": 1,
        "avg_wallet_pct_moved": 1,
        "distribution_pressure_pct": 1,
        "outflow_surge_pct": 1,
        "movement_imbalance_pct": 1,
        # Values (0)
        "strong_accumulation": 0,
        "total_sol_moved": 0,
        "inflow_sol": 0,
        "outflow_sol": 0,
        "net_flow_sol": 0,
        "inflow_count": 0,
        "outflow_count": 0,
        "movement_count": 0,
        "massive_move_count": 0,
        "max_move_size": 0,
        "strong_distribution": 0,
    },
    "price_movements": {
        # Ratios (1)
        "price_change_1m": 1,
        "momentum_volatility_ratio": 1,
        "momentum_acceleration_1m": 1,
        "price_change_5m": 1,
        "price_change_10m": 1,
        "volatility_pct": 1,
        "body_range_ratio": 1,
        "price_stddev_pct": 1,
        "candle_body_pct": 1,
        "upper_wick_pct": 1,
        "lower_wick_pct": 1,
        # Values (0)
        "open_price": 0,
        "high_price": 0,
        "low_price": 0,
        "close_price": 0,
        "avg_price": 0,
        "price_updates": 0,
    },
}


def annotate_field_types(data: List[Dict[str, Any]], data_type: str) -> List[Dict[str, Any]]:
    """Add is_ratio metadata to each field in the data.
    
    Args:
        data: List of records (order_book, transactions, whale, or price data)
        data_type: Type of data ("order_book_signals", "transactions", "whale_activity", "price_movements")
    
    Returns:
        Enhanced list with field_types metadata added to each record
    """
    if not data or data_type not in FIELD_TYPE_SCHEMA:
        return data
    
    schema = FIELD_TYPE_SCHEMA[data_type]
    
    for record in data:
        field_types = {}
        for field_name, value in record.items():
            if field_name in schema:
                field_types[field_name] = schema[field_name]
            elif field_name in ["minute_timestamp", "minute_number", "minute_span_from", "minute_span_to"]:
                field_types[field_name] = 0  # Metadata fields are values
            else:
                # Unknown field - guess based on name
                if any(x in field_name.lower() for x in ["pct", "ratio", "bps", "share", "acceleration"]):
                    field_types[field_name] = 1
                else:
                    field_types[field_name] = 0
        
        record["field_types"] = field_types
    
    return data


# =============================================================================
# EXCEPTIONS
# =============================================================================

class TrailError(Exception):
    """Base exception for trail generation errors."""


class BuyinNotFoundError(TrailError):
    """Raised when a requested buy-in record is missing."""


class MissingFollowedAtError(TrailError):
    """Raised when a buy-in lacks a followed_at timestamp."""


class TrailColumnMissingError(TrailError):
    """Raised when persisting is requested but the storage column is absent."""


# =============================================================================
# DATABASE FETCH FUNCTIONS
# =============================================================================

def fetch_buyin(buyin_id: int) -> Dict[str, Any]:
    """Return buy-in metadata needed for the trail from PostgreSQL.
    
    PostgreSQL-only architecture.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, followed_at, fifteen_min_trail as existing_trail
                    FROM follow_the_goat_buyins
                    WHERE id = %s
                    LIMIT 1
                """, [buyin_id])
                result = cursor.fetchone()
    except Exception as e:
        raise TrailError(f"Failed to fetch buyin #{buyin_id}: {e}")
    
    if not result:
        raise BuyinNotFoundError(f"Buy-in #{buyin_id} not found")
    
    # PostgreSQL RealDictCursor returns dict
    row = {
        'id': result.get('id'),
        'followed_at': result.get('followed_at'),
        'existing_trail': result.get('existing_trail')
    }
    
    # Parse followed_at if it's a string (handles ISO format from inserts)
    followed_at = row.get("followed_at")
    if followed_at and isinstance(followed_at, str):
        try:
            # Handle ISO format with timezone
            if '+' in followed_at or followed_at.endswith('Z'):
                row["followed_at"] = datetime.fromisoformat(followed_at.replace('Z', '+00:00'))
            else:
                row["followed_at"] = datetime.fromisoformat(followed_at)
            logger.debug("Parsed followed_at string to datetime for buyin %s", buyin_id)
        except ValueError as e:
            logger.warning("Failed to parse followed_at for buy-in %s: %s", buyin_id, e)
    
    # Parse existing trail if it's a JSON string
    if row.get("existing_trail") and isinstance(row["existing_trail"], str):
        try:
            row["existing_trail"] = json.loads(row["existing_trail"])
        except json.JSONDecodeError:
            logger.warning("Existing trail for buy-in %s is not valid JSON", buyin_id)
    
    return row


def fetch_order_book_signals(
    symbol: str,
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch order book signals from PostgreSQL with computed metrics."""
    # PostgreSQL query for order book data with minute aggregation
    query = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                CAST((ARRAY_AGG(mid_price ORDER BY timestamp DESC))[1] AS NUMERIC) AS mid_price,
                CAST((ARRAY_AGG(microprice ORDER BY timestamp DESC))[1] AS NUMERIC) AS microprice,
                CAST(AVG(volume_imbalance) AS NUMERIC) AS volume_imbalance,
                CAST(AVG(spread_bps) AS NUMERIC) AS relative_spread_bps,
                CAST(AVG(microprice_dev_bps) AS NUMERIC) AS microprice_dev_bps,
                CAST(AVG(bid_liquidity) AS NUMERIC) AS bid_depth_10,
                CAST(AVG(ask_liquidity) AS NUMERIC) AS ask_depth_10,
                CAST(AVG(total_depth_10) AS NUMERIC) AS total_depth_10,
                CAST(AVG(bid_depth_bps_10) AS NUMERIC) AS bid_depth_bps_10,
                CAST(AVG(ask_depth_bps_10) AS NUMERIC) AS ask_depth_bps_10,
                CAST(AVG(bid_slope) AS NUMERIC) AS bid_slope,
                CAST(AVG(ask_slope) AS NUMERIC) AS ask_slope,
                CAST(AVG(bid_vwap_10) AS NUMERIC) AS bid_vwap_10,
                CAST(AVG(ask_vwap_10) AS NUMERIC) AS ask_vwap_10,
                CAST(SUM(COALESCE(net_liquidity_change_1s, 0)) AS NUMERIC) AS net_liquidity_change_sum,
                COUNT(*) AS sample_count,
                MIN(timestamp) AS period_start,
                MAX(timestamp) AS period_end
            FROM order_book_features
            WHERE timestamp >= ?
                AND timestamp <= ?
            GROUP BY DATE_TRUNC('minute', timestamp)
        ),
        numbered_minutes AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY minute_timestamp) AS row_num
            FROM minute_aggregates
        )
        SELECT 
            m0.minute_timestamp,
            m0.mid_price,
            m0.row_num AS minute_number,
            ROUND(((m0.mid_price - COALESCE(m1.mid_price, m0.mid_price)) / 
                COALESCE(m1.mid_price, m0.mid_price) * 100), 6) AS price_change_1m,
            ROUND(((m0.mid_price - COALESCE(m5.mid_price, m0.mid_price)) / 
                COALESCE(m5.mid_price, m0.mid_price) * 100), 6) AS price_change_5m,
            ROUND(((m0.mid_price - COALESCE(m10.mid_price, m0.mid_price)) / 
                COALESCE(m10.mid_price, m0.mid_price) * 100), 6) AS price_change_10m,
            ROUND(m0.volume_imbalance, 6) AS volume_imbalance,
            ROUND((m0.volume_imbalance - COALESCE(m1.volume_imbalance, m0.volume_imbalance)), 6) AS imbalance_shift_1m,
            ROUND((m0.bid_depth_10 / NULLIF(m0.ask_depth_10, 0)), 6) AS depth_imbalance_ratio,
            ROUND((m0.bid_depth_10 / NULLIF(m0.total_depth_10, 0) * 100), 6) AS bid_liquidity_share_pct,
            ROUND((m0.ask_depth_10 / NULLIF(m0.total_depth_10, 0) * 100), 6) AS ask_liquidity_share_pct,
            ROUND(((m0.bid_depth_10 - m0.ask_depth_10) / NULLIF(m0.total_depth_10, 0) * 100), 6) AS depth_imbalance_pct,
            ROUND(m0.total_depth_10, 2) AS total_liquidity,
            ROUND(((m0.total_depth_10 - COALESCE(m3.total_depth_10, m0.total_depth_10)) / 
                NULLIF(COALESCE(m3.total_depth_10, m0.total_depth_10), 0) * 100), 6) AS liquidity_change_3m,
            ROUND(m0.microprice_dev_bps, 6) AS microprice_deviation,
            ROUND((m0.microprice_dev_bps - COALESCE(m2.microprice_dev_bps, m0.microprice_dev_bps)), 6) AS microprice_acceleration_2m,
            ROUND(m0.relative_spread_bps, 6) AS spread_bps,
            ROUND((ABS(m0.bid_slope) / NULLIF(ABS(m0.ask_slope), 0)), 6) AS aggression_ratio,
            ROUND(((m0.ask_vwap_10 - m0.bid_vwap_10) / NULLIF(m0.bid_vwap_10, 0) * 10000), 6) AS vwap_spread_bps,
            ROUND(m0.net_liquidity_change_sum, 2) AS net_flow_5m,
            ROUND(m0.net_liquidity_change_sum / NULLIF(m0.total_depth_10, 0), 6) AS net_flow_to_liquidity_ratio,
            m0.sample_count,
            EXTRACT(EPOCH FROM (m0.period_end - m0.period_start)) AS coverage_seconds
        FROM numbered_minutes m0
        LEFT JOIN numbered_minutes m1 
            ON m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        LEFT JOIN numbered_minutes m2
            ON m0.row_num > 2
            AND m2.row_num = m0.row_num - 2
        LEFT JOIN numbered_minutes m3
            ON m0.row_num > 3
            AND m3.row_num = m0.row_num - 3
        LEFT JOIN numbered_minutes m5
            ON m0.row_num > 5
            AND m5.row_num = m0.row_num - 5
        LEFT JOIN numbered_minutes m10
            ON m0.row_num > 10
            AND m10.row_num = m0.row_num - 10
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
        """
    return _execute_query(query, [start_time, end_time])


def fetch_transactions(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch transaction data from local DuckDB and compute per-minute metrics.
    
    Data source: master2's local DuckDB (sol_stablecoin_trades table)
    Priority: master2 local DB > TradingDataEngine > file-based DuckDB
    
    Note: All timestamps are in UTC (server time).
    """
    # Query sol_stablecoin_trades from local DuckDB
    query = """
        SELECT 
            trade_timestamp,
            sol_amount,
            stablecoin_amount,
            price,
            direction,
            perp_direction
        FROM sol_stablecoin_trades
        WHERE trade_timestamp >= ?
            AND trade_timestamp <= ?
        ORDER BY trade_timestamp ASC
    """
    raw_trades = _execute_query(query, [start_time, end_time])
    
    if not raw_trades:
        logger.warning("No trades found in time range %s to %s", start_time, end_time)
        return []
    
    logger.info("Fetched %d trades from local DB for range %s to %s", len(raw_trades), start_time, end_time)
    
    # Convert to DataFrame for aggregation
    df = pd.DataFrame(raw_trades)
    
    # Parse trade_timestamp if it's a string
    if 'trade_timestamp' in df.columns:
        df['trade_timestamp'] = pd.to_datetime(df['trade_timestamp'])
    else:
        logger.warning("No trade_timestamp column in trades data")
        return []
    
    # Truncate to minute for grouping
    df['minute_timestamp'] = df['trade_timestamp'].dt.floor('min')
    
    # Ensure numeric columns and sensible fallbacks
    for col in ['sol_amount', 'stablecoin_amount', 'price']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Fallback: if sol_amount missing but stablecoin_amount and price exist, derive sol_amount
    if 'sol_amount' in df.columns and 'stablecoin_amount' in df.columns and 'price' in df.columns:
        missing_sol = df['sol_amount'].fillna(0) == 0
        df.loc[missing_sol, 'sol_amount'] = df.loc[missing_sol, 'stablecoin_amount'] / df.loc[missing_sol, 'price'].replace(0, np.nan)
        df['sol_amount'] = df['sol_amount'].fillna(0)
    
    # Normalize direction strings
    if 'direction' in df.columns:
        df['direction'] = df['direction'].astype(str).str.lower()
    if 'perp_direction' in df.columns:
        df['perp_direction'] = df['perp_direction'].astype(str).str.lower()
    
    # Aggregate by minute
    minute_agg = df.groupby('minute_timestamp').agg(
        total_sol_volume=('sol_amount', 'sum'),
        total_usd_volume=('stablecoin_amount', 'sum'),
        trade_count=('sol_amount', 'count'),
        buy_volume=('sol_amount', lambda x: x[df.loc[x.index, 'direction'] == 'buy'].sum()),
        sell_volume=('sol_amount', lambda x: x[df.loc[x.index, 'direction'] == 'sell'].sum()),
        buy_count=('direction', lambda x: (x == 'buy').sum()),
        sell_count=('direction', lambda x: (x == 'sell').sum()),
        long_volume=('sol_amount', lambda x: x[df.loc[x.index, 'perp_direction'] == 'long'].sum() if 'perp_direction' in df.columns else 0),
        short_volume=('sol_amount', lambda x: x[df.loc[x.index, 'perp_direction'] == 'short'].sum() if 'perp_direction' in df.columns else 0),
        large_trade_volume=('stablecoin_amount', lambda x: x[x > 10000].sum()),
        large_trade_count=('stablecoin_amount', lambda x: (x > 10000).sum()),
        avg_trade_size=('stablecoin_amount', 'mean'),
        min_price=('price', 'min'),
        max_price=('price', 'max'),
        avg_price=('price', 'mean'),
        open_price=('price', 'first'),
        close_price=('price', 'last'),
    ).reset_index()
    
    # Calculate VWAP
    minute_agg['vwap'] = minute_agg['total_usd_volume'] / minute_agg['total_sol_volume'].replace(0, np.nan)
    
    # Sort by minute and add row numbers
    minute_agg = minute_agg.sort_values('minute_timestamp').reset_index(drop=True)
    minute_agg['row_num'] = range(1, len(minute_agg) + 1)
    
    # Compute derived metrics
    results = []
    for i, row in minute_agg.iterrows():
        prev_row = minute_agg.iloc[i-1] if i > 0 else row
        
        buy_vol = row['buy_volume']
        sell_vol = row['sell_volume']
        total_vol = buy_vol + sell_vol
        long_vol = row['long_volume']
        short_vol = row['short_volume']
        perp_total = long_vol + short_vol
        
        # Buy/sell pressure
        buy_sell_pressure = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0
        prev_buy_sell_pressure = (prev_row['buy_volume'] - prev_row['sell_volume']) / (prev_row['buy_volume'] + prev_row['sell_volume']) if (prev_row['buy_volume'] + prev_row['sell_volume']) > 0 else 0
        
        results.append({
            'minute_timestamp': row['minute_timestamp'],
            'minute_number': row['row_num'],
            'buy_sell_pressure': round(buy_sell_pressure, 6),
            'buy_volume_pct': round(buy_vol / total_vol * 100, 6) if total_vol > 0 else 0,
            'sell_volume_pct': round(sell_vol / total_vol * 100, 6) if total_vol > 0 else 0,
            'pressure_shift_1m': round(buy_sell_pressure - prev_buy_sell_pressure, 6),
            'long_short_ratio': round(long_vol / short_vol, 6) if short_vol > 0 else 0,
            'long_volume_pct': round(long_vol / perp_total * 100, 6) if perp_total > 0 else 0,
            'short_volume_pct': round(short_vol / perp_total * 100, 6) if perp_total > 0 else 0,
            'perp_position_skew_pct': round((long_vol - short_vol) / perp_total * 100, 6) if perp_total > 0 else 0,
            'perp_dominance_pct': round(perp_total / row['total_sol_volume'] * 100, 6) if row['total_sol_volume'] > 0 else 0,
            'total_volume_usd': round(row['total_usd_volume'], 2),
            'volume_acceleration_ratio': round(row['total_usd_volume'] / prev_row['total_usd_volume'], 6) if prev_row['total_usd_volume'] > 0 else 1,
            'whale_volume_pct': round(row['large_trade_volume'] / row['total_usd_volume'] * 100, 6) if row['total_usd_volume'] > 0 else 0,
            'avg_trade_size': round(row['avg_trade_size'], 2),
            'trades_per_second': round(row['trade_count'] / 60.0, 2),
            'buy_trade_pct': round(row['buy_count'] / row['trade_count'] * 100, 6) if row['trade_count'] > 0 else 0,
            'price_change_1m': round((row['close_price'] - row['open_price']) / row['open_price'] * 100, 6) if row['open_price'] > 0 else 0,
            'price_volatility_pct': round((row['max_price'] - row['min_price']) / row['avg_price'] * 100, 6) if row['avg_price'] > 0 else 0,
            'trade_count': int(row['trade_count']),
            'large_trade_count': int(row['large_trade_count']),
            'vwap': round(row['vwap'], 2) if pd.notna(row['vwap']) else 0,
        })
    
    # Return latest 15 minutes, sorted descending
    final_results = sorted(results, key=lambda x: x['minute_timestamp'], reverse=True)[:15]
    
    logger.info("Aggregated transactions into %d minutes of data", len(final_results))
    return final_results


def fetch_whale_activity(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch whale movement data from local DuckDB and compute per-minute metrics.
    
    Data source: master2's local DuckDB or TradingDataEngine (whale_movements table)
    Priority: master2 local DB > TradingDataEngine > file-based DuckDB
    
    Note: All timestamps are in UTC (server time).
    """
    # Query whale_movements from local DuckDB
    query = """
        SELECT 
            timestamp,
            sol_change,
            abs_change,
            percentage_moved,
            direction,
            whale_type,
            movement_significance
        FROM whale_movements
        WHERE timestamp >= ?
            AND timestamp <= ?
        ORDER BY timestamp ASC
    """
    raw_whales = _execute_query(query, [start_time, end_time])
    
    if not raw_whales:
        logger.warning("No whale movements found in time range %s to %s", start_time, end_time)
        return []
    
    logger.info("Fetched %d whale movements from local DB for range %s to %s", len(raw_whales), start_time, end_time)
    
    # Convert to DataFrame for aggregation
    df = pd.DataFrame(raw_whales)
    
    # Parse timestamp if it's a string
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        logger.warning("No timestamp column in whale data")
        return []
    
    # Normalize direction; map webhook values to in/out
    if 'direction' in df.columns:
        df['direction'] = df['direction'].astype(str).str.lower()
        df['direction'] = df['direction'].replace({
            'sending': 'out',
            'sent': 'out',
            'outbound': 'out',
            'receiving': 'in',
            'received': 'in',
            'inbound': 'in'
        })
    
    # Truncate to minute for grouping
    df['minute_timestamp'] = df['timestamp'].dt.floor('min')
    
    # Ensure numeric columns
    for col in ['sol_change', 'abs_change', 'percentage_moved']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0
    
    # Prefer abs_change when non-zero; otherwise fall back to sol_change magnitude
    df['abs_sol_change'] = np.where(
        df['abs_change'].abs() > 0,
        df['abs_change'].abs(),
        df['sol_change'].abs()
    )
    
    # Aggregate by minute
    def agg_by_minute(group):
        direction_col = 'direction' if 'direction' in group.columns else None
        pct_col = 'percentage_moved' if 'percentage_moved' in group.columns else None
        
        inflow_mask = group[direction_col] == 'in' if direction_col else pd.Series([False] * len(group))
        outflow_mask = group[direction_col] == 'out' if direction_col else pd.Series([False] * len(group))
        
        inflow_sol = group.loc[inflow_mask, 'abs_sol_change'].sum() if inflow_mask.any() else 0
        outflow_sol = group.loc[outflow_mask, 'abs_sol_change'].sum() if outflow_mask.any() else 0
        net_flow = inflow_sol - outflow_sol
        total_moved = group['abs_sol_change'].sum()
        
        # Percentage-based categorization
        if pct_col:
            massive_mask = group[pct_col] > 10
            large_mask = (group[pct_col] > 5) & (group[pct_col] <= 10)
            strong_acc_mask = inflow_mask & (group[pct_col] > 5)
            strong_dist_mask = outflow_mask & (group[pct_col] > 5)
        else:
            massive_mask = pd.Series([False] * len(group))
            large_mask = pd.Series([False] * len(group))
            strong_acc_mask = pd.Series([False] * len(group))
            strong_dist_mask = pd.Series([False] * len(group))
        
        return pd.Series({
            'inflow_sol': inflow_sol,
            'inflow_count': inflow_mask.sum(),
            'outflow_sol': outflow_sol,
            'outflow_count': outflow_mask.sum(),
            'net_flow_sol': net_flow,
            'total_sol_moved': total_moved,
            'total_movements': len(group),
            'massive_move_sol': group.loc[massive_mask, 'abs_sol_change'].sum() if massive_mask.any() else 0,
            'massive_move_count': massive_mask.sum(),
            'strong_accumulation_sol': group.loc[strong_acc_mask, 'abs_sol_change'].sum() if strong_acc_mask.any() else 0,
            'strong_distribution_sol': group.loc[strong_dist_mask, 'abs_sol_change'].sum() if strong_dist_mask.any() else 0,
            'avg_move_size': group['abs_sol_change'].mean(),
            'max_move_size': group['abs_sol_change'].max(),
            'avg_percentage_moved': group[pct_col].mean() if pct_col else 0,
        })
    
    minute_agg = df.groupby('minute_timestamp').apply(agg_by_minute, include_groups=False).reset_index()
    
    # Sort by minute and add row numbers
    minute_agg = minute_agg.sort_values('minute_timestamp').reset_index(drop=True)
    minute_agg['row_num'] = range(1, len(minute_agg) + 1)
    
    # Compute derived metrics
    results = []
    for i, row in minute_agg.iterrows():
        prev_row = minute_agg.iloc[i-1] if i > 0 else row
        
        inflow = row['inflow_sol']
        outflow = row['outflow_sol']
        total_flow = inflow + outflow
        net_flow = row['net_flow_sol']
        total_moved = row['total_sol_moved']
        
        # Net flow ratio
        if total_flow > 0:
            net_flow_ratio = net_flow / total_flow
        elif outflow > 0:
            net_flow_ratio = -1.0
        elif inflow > 0:
            net_flow_ratio = 1.0
        else:
            net_flow_ratio = 0
        
        # Previous net flow ratio
        prev_total_flow = prev_row['inflow_sol'] + prev_row['outflow_sol']
        if prev_total_flow > 0:
            prev_net_flow_ratio = prev_row['net_flow_sol'] / prev_total_flow
        else:
            prev_net_flow_ratio = 0
        
        # Accumulation ratio
        if outflow > 0 and inflow > 0:
            acc_ratio = inflow / outflow
        elif inflow > 0:
            acc_ratio = 999.0
        elif outflow > 0:
            acc_ratio = 0.0
        else:
            acc_ratio = 1.0
        
        # Outflow surge
        if prev_row['outflow_sol'] > 0:
            outflow_surge = (outflow - prev_row['outflow_sol']) / prev_row['outflow_sol'] * 100
        elif outflow > 0:
            outflow_surge = 100.0
        else:
            outflow_surge = 0
        
        results.append({
            'minute_timestamp': row['minute_timestamp'],
            'minute_number': int(row['row_num']),
            'net_flow_ratio': round(net_flow_ratio, 6),
            'flow_shift_1m': round(net_flow_ratio - prev_net_flow_ratio, 6),
            'accumulation_ratio': round(acc_ratio, 6),
            'strong_accumulation': round(row['strong_accumulation_sol'], 2),
            'total_sol_moved': round(total_moved, 2),
            'inflow_share_pct': round(inflow / total_moved * 100, 6) if total_moved > 0 else 0,
            'outflow_share_pct': round(outflow / total_moved * 100, 6) if total_moved > 0 else 0,
            'net_flow_strength_pct': round(net_flow / total_moved * 100, 6) if total_moved > 0 else 0,
            'strong_accumulation_pct': round(row['strong_accumulation_sol'] / total_moved * 100, 6) if total_moved > 0 else 0,
            'strong_distribution_pct': round(row['strong_distribution_sol'] / total_moved * 100, 6) if total_moved > 0 else 0,
            'movement_count': int(row['total_movements']),
            'massive_move_pct': round(row['massive_move_sol'] / total_moved * 100, 6) if total_moved > 0 else 0,
            'avg_wallet_pct_moved': round(row['avg_percentage_moved'], 6),
            'distribution_pressure_pct': round(row['strong_distribution_sol'] / total_moved * 100, 6) if total_moved > 0 else 0,
            'outflow_surge_pct': round(outflow_surge, 6),
            'movement_imbalance_pct': round(abs(row['inflow_count'] - row['outflow_count']) / (row['inflow_count'] + row['outflow_count']) * 100, 6) if (row['inflow_count'] + row['outflow_count']) > 0 else 0,
            'inflow_sol': round(inflow, 2),
            'outflow_sol': round(outflow, 2),
            'net_flow_sol': round(net_flow, 2),
            'inflow_count': int(row['inflow_count']),
            'outflow_count': int(row['outflow_count']),
            'massive_move_count': int(row['massive_move_count']),
            'max_move_size': round(row['max_move_size'], 2),
            'strong_distribution': round(row['strong_distribution_sol'], 2),
        })
    
    # Return latest 15 minutes, sorted descending
    final_results = sorted(results, key=lambda x: x['minute_timestamp'], reverse=True)[:15]
    
    logger.info("Aggregated whale activity into %d minutes of data", len(final_results))
    return final_results


def fetch_price_movements(
    start_time: datetime,
    end_time: datetime,
    token: str = "SOL",
    coin_id: int = 5
) -> List[Dict[str, Any]]:
    """Fetch price movement data with legacy-equivalent calculations.
    
    Priority order:
    1. master2's local DuckDB (prices table with token column)
    2. TradingDataEngine (master.py's in-memory prices table)
    3. File-based DuckDB price_points table (legacy fallback)
    
    Args:
        start_time: Start of the time window
        end_time: End of the time window
        token: Token symbol for DuckDB (SOL, BTC, ETH)
        coin_id: Coin ID for legacy price_points (1=BTC, 2=ETH, 5=SOL)
    """
    # Query for prices table (modern format: timestamp, token, price)
    query_prices = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                CAST(MIN(price) AS NUMERIC) AS low_price,
                CAST(MAX(price) AS NUMERIC) AS high_price,
                CAST(AVG(price) AS NUMERIC) AS avg_price,
                CAST((ARRAY_AGG(price ORDER BY timestamp ASC))[1] AS NUMERIC) AS true_open,
                CAST((ARRAY_AGG(price ORDER BY timestamp DESC))[1] AS NUMERIC) AS true_close,
                CAST(MAX(price) - MIN(price) AS NUMERIC) AS price_range,
                CAST(STDDEV(price) AS NUMERIC) AS price_stddev,
                COUNT(*) AS price_updates
            FROM prices
            WHERE timestamp >= ?
                AND timestamp <= ?
                AND token = ?
            GROUP BY DATE_TRUNC('minute', timestamp)
        ),
        numbered_minutes AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY minute_timestamp) AS row_num
            FROM minute_aggregates
        )
        SELECT 
            m0.minute_timestamp,
            m0.row_num AS minute_number,
            ROUND((m0.true_close - m0.true_open) / NULLIF(m0.true_open, 0) * 100, 6) AS price_change_1m,
            ROUND(
                ((m0.true_close - m0.true_open) / NULLIF(m0.true_open, 0) * 100) /
                NULLIF((m0.price_range / NULLIF(m0.avg_price, 0) * 100), 0),
            6) AS momentum_volatility_ratio,
            ROUND(
                CASE WHEN m1.true_close > 0 AND m1.true_open > 0 THEN
                    ((m0.true_close - m0.true_open) / m0.true_open) -
                    ((m1.true_close - m1.true_open) / m1.true_open)
                ELSE 0 END * 100,
            6) AS momentum_acceleration_1m,
            ROUND(CASE WHEN m5.true_close > 0 THEN
                (m0.true_close - m5.true_close) / m5.true_close * 100
            ELSE 0 END, 6) AS price_change_5m,
            ROUND(CASE WHEN m10.true_close > 0 THEN
                (m0.true_close - m10.true_close) / m10.true_close * 100
            ELSE 0 END, 6) AS price_change_10m,
            ROUND(m0.price_range / NULLIF(m0.avg_price, 0) * 100, 6) AS volatility_pct,
            ROUND(
                (ABS(m0.true_close - m0.true_open) / NULLIF(m0.avg_price, 0) * 100) /
                NULLIF((m0.price_range / NULLIF(m0.avg_price, 0) * 100), 0),
            6) AS body_range_ratio,
            ROUND(m0.price_stddev / NULLIF(m0.avg_price, 0) * 100, 6) AS price_stddev_pct,
            ROUND(ABS(m0.true_close - m0.true_open) / NULLIF(m0.avg_price, 0) * 100, 6) AS candle_body_pct,
            ROUND((m0.high_price - GREATEST(m0.true_open, m0.true_close)) / 
                NULLIF(m0.avg_price, 0) * 100, 6) AS upper_wick_pct,
            ROUND((LEAST(m0.true_open, m0.true_close) - m0.low_price) / 
                NULLIF(m0.avg_price, 0) * 100, 6) AS lower_wick_pct,
            ROUND(m0.true_open, 4) AS open_price,
            ROUND(m0.high_price, 4) AS high_price,
            ROUND(m0.low_price, 4) AS low_price,
            ROUND(m0.true_close, 4) AS close_price,
            ROUND(m0.avg_price, 4) AS avg_price,
            m0.price_updates
        FROM numbered_minutes m0
        LEFT JOIN numbered_minutes m1 
            ON m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        LEFT JOIN numbered_minutes m5
            ON m0.row_num > 5
            AND m5.row_num = m0.row_num - 5
        LEFT JOIN numbered_minutes m10
            ON m0.row_num > 10
            AND m10.row_num = m0.row_num - 10
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
    """
    
    # Use _execute_query which now directly queries PostgreSQL
    results = _execute_query(query_prices, [start_time, end_time, token])
    if results:
        logger.debug(f"Got {len(results)} price movements from PostgreSQL for {token}")
        return results
    
    # Fallback: PostgreSQL price_points table (legacy)
    query_duckdb = """
        WITH minute_aggregates AS (
            SELECT 
                TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:00') AS minute_timestamp,
                CAST(MIN(value) AS NUMERIC) AS low_price,
                CAST(MAX(value) AS NUMERIC) AS high_price,
                CAST(AVG(value) AS NUMERIC) AS avg_price,
                CAST((ARRAY_AGG(value ORDER BY created_at ASC))[1] AS NUMERIC) AS true_open,
                CAST((ARRAY_AGG(value ORDER BY created_at DESC))[1] AS NUMERIC) AS true_close,
                CAST(MAX(value) - MIN(value) AS NUMERIC) AS price_range,
                CAST(STDDEV(value) AS NUMERIC) AS price_stddev,
                COUNT(*) AS price_updates
            FROM price_points
            WHERE created_at >= %s
                AND created_at <= %s
                AND coin_id = %s
            GROUP BY TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:00')
        ),
        numbered_minutes AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY minute_timestamp) AS row_num
            FROM minute_aggregates
        )
        SELECT 
            m0.minute_timestamp,
            m0.row_num AS minute_number,
            ROUND((m0.true_close - m0.true_open) / NULLIF(m0.true_open, 0) * 100, 6) AS price_change_1m,
            ROUND(
                ((m0.true_close - m0.true_open) / NULLIF(m0.true_open, 0) * 100) /
                NULLIF((m0.price_range / NULLIF(m0.avg_price, 0) * 100), 0),
            6) AS momentum_volatility_ratio,
            ROUND(
                CASE WHEN m1.true_close > 0 AND m1.true_open > 0 THEN
                    ((m0.true_close - m0.true_open) / m0.true_open) -
                    ((m1.true_close - m1.true_open) / m1.true_open)
                ELSE 0 END * 100,
            6) AS momentum_acceleration_1m,
            ROUND(CASE WHEN m5.true_close > 0 THEN
                (m0.true_close - m5.true_close) / m5.true_close * 100
            ELSE 0 END, 6) AS price_change_5m,
            ROUND(CASE WHEN m10.true_close > 0 THEN
                (m0.true_close - m10.true_close) / m10.true_close * 100
            ELSE 0 END, 6) AS price_change_10m,
            ROUND(m0.price_range / NULLIF(m0.avg_price, 0) * 100, 6) AS volatility_pct,
            ROUND(
                (ABS(m0.true_close - m0.true_open) / NULLIF(m0.avg_price, 0) * 100) /
                NULLIF((m0.price_range / NULLIF(m0.avg_price, 0) * 100), 0),
            6) AS body_range_ratio,
            ROUND(m0.price_stddev / NULLIF(m0.avg_price, 0) * 100, 6) AS price_stddev_pct,
            ROUND(ABS(m0.true_close - m0.true_open) / NULLIF(m0.avg_price, 0) * 100, 6) AS candle_body_pct,
            ROUND((m0.high_price - GREATEST(m0.true_open, m0.true_close)) / 
                NULLIF(m0.avg_price, 0) * 100, 6) AS upper_wick_pct,
            ROUND((LEAST(m0.true_open, m0.true_close) - m0.low_price) / 
                NULLIF(m0.avg_price, 0) * 100, 6) AS lower_wick_pct,
            ROUND(m0.true_open, 4) AS open_price,
            ROUND(m0.high_price, 4) AS high_price,
            ROUND(m0.low_price, 4) AS low_price,
            ROUND(m0.true_close, 4) AS close_price,
            ROUND(m0.avg_price, 4) AS avg_price,
            m0.price_updates
        FROM numbered_minutes m0
        LEFT JOIN numbered_minutes m1 
            ON m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        LEFT JOIN numbered_minutes m5
            ON m0.row_num > 5
            AND m5.row_num = m0.row_num - 5
        LEFT JOIN numbered_minutes m10
            ON m0.row_num > 10
            AND m10.row_num = m0.row_num - 10
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
    """
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query_duckdb, [start_time, end_time, coin_id])
                rows = cursor.fetchall()
                return rows if rows else []
    except Exception as e:
        logger.error("PostgreSQL price_movements query failed for %s (coin_id=%s): %s", token, coin_id, e)
        return []


def fetch_second_prices(
    start_time: datetime,
    end_time: datetime
) -> pd.DataFrame:
    """Fetch 1-second price data for pattern detection.
    
    Uses _execute_query which handles HTTP API fallback for standalone execution.
    """
    # First try the prices table (via _execute_query which handles HTTP API)
    query = """
        SELECT timestamp AS ts, price AS price
        FROM prices
        WHERE timestamp >= ? AND timestamp <= ? AND token = ?
        ORDER BY timestamp ASC
    """
    results = _execute_query(query, [start_time, end_time, "SOL"])
    
    if results:
        df = pd.DataFrame(results)
        if not df.empty and "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
            df = df.set_index("ts")
            df["price"] = df["price"].astype(float)
            return df
    
    # Fallback to PostgreSQL price_points if prices table returned no results
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT created_at AS ts, value AS price
                    FROM price_points
                    WHERE created_at >= %s AND created_at <= %s
                        AND coin_id = 5
                    ORDER BY created_at ASC
                """, [start_time, end_time])
                fallback_results = cursor.fetchall()
    except Exception as e:
        logger.error("PostgreSQL second_prices query failed: %s", e)
        fallback_results = []
    
    if not fallback_results:
        return pd.DataFrame(columns=["price"])
    
    df = pd.DataFrame(fallback_results)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.set_index("ts")
    df["price"] = df["price"].astype(float)
    return df


# =============================================================================
# MICRO-PATTERN DETECTION FUNCTIONS (Optimized for 0.5% Moves)
# =============================================================================

def detect_volume_divergence(
    transaction_rows: List[Dict[str, Any]],
    price_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Detect when volume increases while price consolidates (accumulation signal).
    
    Key signal: Rising volume + flat/slight down price = smart money accumulating
    This is a strong precursor to 0.5% climbs.
    
    Args:
        transaction_rows: Latest 15 minutes of transaction data
        price_rows: Latest 15 minutes of price movement data
    
    Returns:
        Detection result with confidence score
    """
    result = {"detected": False, "confidence": 0.0, "signal_type": "volume_divergence"}
    
    if not transaction_rows or not price_rows or len(transaction_rows) < 5 or len(price_rows) < 5:
        result["error"] = "insufficient_data"
        return result
    
    # Sort by minute number (ascending for analysis)
    tx_sorted = sorted(transaction_rows, key=lambda x: x.get('minute_number', 0))
    price_sorted = sorted(price_rows, key=lambda x: x.get('minute_number', 0))
    
    # Split into early and late periods
    mid_point = len(tx_sorted) // 2
    early_tx = tx_sorted[:mid_point]
    late_tx = tx_sorted[mid_point:]
    early_price = price_sorted[:mid_point]
    late_price = price_sorted[mid_point:]
    
    # Calculate average volume for each period
    early_vol = np.mean([x.get('total_volume_usd', 0) for x in early_tx])
    late_vol = np.mean([x.get('total_volume_usd', 0) for x in late_tx])
    
    if early_vol <= 0:
        result["error"] = "no_early_volume"
        return result
    
    volume_increase_pct = ((late_vol - early_vol) / early_vol) * 100
    
    # Calculate price movement for each period
    early_price_change = np.mean([x.get('price_change_1m', 0) for x in early_price])
    late_price_change = np.mean([x.get('price_change_1m', 0) for x in late_price])
    
    # Calculate price volatility
    early_volatility = np.mean([x.get('volatility_pct', 0) for x in early_price])
    late_volatility = np.mean([x.get('volatility_pct', 0) for x in late_price])
    
    # Divergence occurs when:
    # 1. Volume is increasing (>15% increase)
    # 2. Price is flat or slightly declining (-0.3% to +0.2%)
    # 3. Volatility is decreasing (consolidation)
    
    volume_increasing = volume_increase_pct > 15
    price_consolidating = -0.3 <= late_price_change <= 0.2
    volatility_decreasing = late_volatility < early_volatility
    
    if not volume_increasing:
        result["error"] = "volume_not_increasing"
        return result
    
    # Calculate buy pressure trend
    buy_pressure_early = np.mean([x.get('buy_sell_pressure', 0) for x in early_tx])
    buy_pressure_late = np.mean([x.get('buy_sell_pressure', 0) for x in late_tx])
    buy_pressure_improving = buy_pressure_late > buy_pressure_early
    
    # Scoring
    volume_score = min(1.0, volume_increase_pct / 50)  # Scale: 50% increase = 1.0
    consolidation_score = 1.0 if price_consolidating else max(0, 1.0 - abs(late_price_change) / 0.5)
    volatility_score = 0.5 if volatility_decreasing else 0.0
    pressure_score = 0.3 if buy_pressure_improving else 0.0
    
    confidence = (volume_score * 0.4 + consolidation_score * 0.35 + volatility_score * 0.15 + pressure_score * 0.1)
    confidence = min(1.0, max(0.0, confidence))
    
    if confidence >= 0.5:
        result["detected"] = True
        result["confidence"] = round(confidence, 3)
        result["volume_increase_pct"] = round(volume_increase_pct, 2)
        result["late_price_change"] = round(late_price_change, 4)
        result["buy_pressure_trend"] = "improving" if buy_pressure_improving else "declining"
        result["interpretation"] = "Smart money accumulation - volume rising while price stable"
    
    return result


def detect_order_book_squeeze(order_book_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect when bid depth increases faster than ask depth (pressure building).
    
    Key signal: Bid/ask ratio increasing + spread tightening = breakout imminent
    This indicates buyers are stepping up while sellers back off.
    
    Args:
        order_book_rows: Latest 15 minutes of order book data
    
    Returns:
        Detection result with confidence score
    """
    result = {"detected": False, "confidence": 0.0, "signal_type": "order_book_squeeze"}
    
    if not order_book_rows or len(order_book_rows) < 5:
        result["error"] = "insufficient_data"
        return result
    
    # Sort by minute number (ascending)
    sorted_rows = sorted(order_book_rows, key=lambda x: x.get('minute_number', 0))
    
    # Split into early and late periods
    mid_point = len(sorted_rows) // 2
    early = sorted_rows[:mid_point]
    late = sorted_rows[mid_point:]
    
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    # Calculate average depth imbalance ratio (bid/ask)
    early_ratio = np.mean([_to_float(x.get('depth_imbalance_ratio', 1.0), 1.0) for x in early])
    late_ratio = np.mean([_to_float(x.get('depth_imbalance_ratio', 1.0), 1.0) for x in late])
    
    # Calculate spread tightening
    early_spread = np.mean([_to_float(x.get('spread_bps', 10), 10.0) for x in early])
    late_spread = np.mean([_to_float(x.get('spread_bps', 10), 10.0) for x in late])
    
    # Calculate liquidity changes
    liquidity_changes = [_to_float(x.get('liquidity_change_3m', 0), 0.0) for x in late]
    avg_liquidity_change = np.mean(liquidity_changes) if liquidity_changes else 0
    
    # Squeeze occurs when:
    # 1. Bid/ask ratio is increasing (bids building up)
    # 2. Spread is tightening (market getting more efficient)
    # 3. Total liquidity is stable or increasing
    
    if early_ratio == 0:
        result["error"] = "invalid_ratio_baseline"
        return result
    
    ratio_improving = late_ratio > early_ratio * 1.05  # 5% improvement
    spread_tightening = late_spread < early_spread * 0.95 if early_spread else False  # 5% tighter
    liquidity_stable = avg_liquidity_change > -5  # Not dropping significantly
    
    if not ratio_improving:
        result["error"] = "ratio_not_improving"
        return result
    
    # Calculate volume imbalance trend
    early_vol_imbalance = np.mean([_to_float(x.get('volume_imbalance', 0), 0.0) for x in early])
    late_vol_imbalance = np.mean([_to_float(x.get('volume_imbalance', 0), 0.0) for x in late])
    vol_imbalance_improving = late_vol_imbalance > early_vol_imbalance
    
    # Scoring
    ratio_change_pct = ((late_ratio - early_ratio) / early_ratio) * 100
    ratio_score = min(1.0, ratio_change_pct / 20)  # 20% improvement = 1.0
    
    if early_spread:
        spread_change_pct = ((early_spread - late_spread) / early_spread) * 100
        spread_score = min(1.0, spread_change_pct / 10) if spread_tightening else 0
    else:
        spread_change_pct = 0
        spread_score = 0
    
    liquidity_score = min(1.0, (avg_liquidity_change + 10) / 20) if liquidity_stable else 0
    imbalance_score = 0.2 if vol_imbalance_improving else 0
    
    confidence = (ratio_score * 0.45 + spread_score * 0.30 + liquidity_score * 0.15 + imbalance_score * 0.10)
    confidence = min(1.0, max(0.0, confidence))
    
    if confidence >= 0.5:
        result["detected"] = True
        result["confidence"] = round(confidence, 3)
        result["ratio_change_pct"] = round(ratio_change_pct, 2)
        result["spread_tightening_pct"] = round(spread_change_pct, 2) if spread_tightening else 0
        result["interpretation"] = "Order book squeeze - buyers stepping up, sellers backing off"
    
    return result


def detect_whale_stealth_accumulation(
    whale_rows: List[Dict[str, Any]],
    price_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Detect whale buying without price impact (smart money signal).
    
    Key signal: Whale inflow increasing + price stable/down = accumulation before pump
    Whales often accumulate quietly before significant moves.
    
    Args:
        whale_rows: Latest 15 minutes of whale activity data
        price_rows: Latest 15 minutes of price movement data
    
    Returns:
        Detection result with confidence score
    """
    result = {"detected": False, "confidence": 0.0, "signal_type": "whale_stealth_accumulation"}
    
    if not whale_rows or not price_rows or len(whale_rows) < 3 or len(price_rows) < 3:
        result["error"] = "insufficient_data"
        return result
    
    # Sort by minute number (ascending)
    whale_sorted = sorted(whale_rows, key=lambda x: x.get('minute_number', 0))
    price_sorted = sorted(price_rows, key=lambda x: x.get('minute_number', 0))
    
    # Get recent whale activity (last 5-7 minutes)
    recent_whale = whale_sorted[-7:] if len(whale_sorted) >= 7 else whale_sorted[-5:]
    recent_price = price_sorted[-7:] if len(price_sorted) >= 7 else price_sorted[-5:]
    
    # Calculate net flow ratio trend
    net_flow_ratios = [x.get('net_flow_ratio', 0) for x in recent_whale]
    net_flow_trend = np.mean(net_flow_ratios)
    
    # Calculate strong accumulation percentage
    strong_acc_pcts = [x.get('strong_accumulation_pct', 0) for x in recent_whale]
    avg_strong_acc = np.mean(strong_acc_pcts)
    
    # Calculate price stability
    price_changes = [x.get('price_change_1m', 0) for x in recent_price]
    avg_price_change = np.mean(price_changes)
    price_volatility = np.std(price_changes)
    
    # Stealth accumulation occurs when:
    # 1. Net flow ratio is increasingly positive (>0.3)
    # 2. Strong accumulation is significant (>10%)
    # 3. Price is stable or declining (-0.3% to +0.2%)
    # 4. Price volatility is low
    
    net_flow_positive = net_flow_trend > 0.3
    accumulation_significant = avg_strong_acc > 10
    price_stable = -0.3 <= avg_price_change <= 0.2
    low_volatility = price_volatility < 0.3
    
    if not net_flow_positive:
        result["error"] = "net_flow_not_positive"
        return result
    
    # Calculate accumulation ratio trend
    acc_ratios = [x.get('accumulation_ratio', 1.0) for x in recent_whale]
    avg_acc_ratio = np.mean(acc_ratios)
    acc_ratio_improving = avg_acc_ratio > 2.0  # More than 2x inflow vs outflow
    
    # Scoring
    net_flow_score = min(1.0, net_flow_trend / 0.7)  # 0.7 net flow = 1.0
    accumulation_score = min(1.0, avg_strong_acc / 30)  # 30% strong acc = 1.0
    stability_score = 1.0 if price_stable else max(0, 1.0 - abs(avg_price_change) / 0.5)
    volatility_score = max(0, 1.0 - price_volatility / 0.5) if low_volatility else 0
    ratio_score = 0.2 if acc_ratio_improving else 0
    
    confidence = (
        net_flow_score * 0.35 +
        accumulation_score * 0.30 +
        stability_score * 0.20 +
        volatility_score * 0.10 +
        ratio_score * 0.05
    )
    confidence = min(1.0, max(0.0, confidence))
    
    if confidence >= 0.5:
        result["detected"] = True
        result["confidence"] = round(confidence, 3)
        result["net_flow_ratio"] = round(net_flow_trend, 3)
        result["strong_accumulation_pct"] = round(avg_strong_acc, 2)
        result["price_change"] = round(avg_price_change, 4)
        result["accumulation_ratio"] = round(avg_acc_ratio, 2)
        result["interpretation"] = "Whale stealth accumulation - smart money buying without price impact"
    
    return result


def detect_momentum_acceleration(price_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect when rate of change is accelerating (second derivative).
    
    Key signal: momentum_acceleration_1m increasing = trend strengthening
    This helps identify when small moves are gaining steam.
    
    Args:
        price_rows: Latest 15 minutes of price movement data
    
    Returns:
        Detection result with confidence score
    """
    result = {"detected": False, "confidence": 0.0, "signal_type": "momentum_acceleration"}
    
    if not price_rows or len(price_rows) < 5:
        result["error"] = "insufficient_data"
        return result
    
    # Sort by minute number (ascending)
    sorted_rows = sorted(price_rows, key=lambda x: x.get('minute_number', 0))
    
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    # Get momentum acceleration values
    momentum_accel = [_to_float(x.get('momentum_acceleration_1m', 0), 0.0) for x in sorted_rows]
    
    # Split into early and late periods
    mid_point = len(momentum_accel) // 2
    early_accel = momentum_accel[:mid_point]
    late_accel = momentum_accel[mid_point:]
    
    avg_early_accel = np.mean(early_accel)
    avg_late_accel = np.mean(late_accel)
    
    # Get price changes for context
    price_changes_1m = [_to_float(x.get('price_change_1m', 0), 0.0) for x in sorted_rows[-5:]]
    price_changes_5m = [_to_float(x.get('price_change_5m', 0), 0.0) for x in sorted_rows[-5:]]
    
    avg_price_change_1m = np.mean(price_changes_1m)
    avg_price_change_5m = np.mean(price_changes_5m)
    
    # Get volatility trend
    volatilities = [_to_float(x.get('volatility_pct', 0), 0.0) for x in sorted_rows]
    early_vol = np.mean(volatilities[:mid_point])
    late_vol = np.mean(volatilities[mid_point:])
    
    # Momentum acceleration detected when:
    # 1. Late period acceleration > early period (improving)
    # 2. Recent price change is positive
    # 3. 5m price change > 1m price change (sustained trend)
    # 4. Volatility is decreasing (more directional)
    
    acceleration_improving = avg_late_accel > avg_early_accel
    price_positive = avg_price_change_1m > 0
    trend_sustained = avg_price_change_5m > avg_price_change_1m
    volatility_decreasing = late_vol < early_vol
    
    if not acceleration_improving:
        result["error"] = "acceleration_not_improving"
        return result
    
    # Calculate momentum volatility ratio trend
    mvr_values = [_to_float(x.get('momentum_volatility_ratio', 0), 0.0) for x in sorted_rows[-5:]]
    avg_mvr = np.mean(mvr_values)
    mvr_positive = avg_mvr > 0
    
    # Scoring
    accel_change = avg_late_accel - avg_early_accel
    accel_score = min(1.0, abs(accel_change) / 0.3)  # 0.3% acceleration = 1.0
    
    price_score = min(1.0, avg_price_change_1m / 0.3) if price_positive else 0
    trend_score = 0.3 if trend_sustained else 0
    vol_score = 0.2 if volatility_decreasing else 0
    mvr_score = 0.15 if mvr_positive else 0
    
    confidence = (
        accel_score * 0.40 +
        price_score * 0.25 +
        trend_score * 0.15 +
        vol_score * 0.10 +
        mvr_score * 0.10
    )
    confidence = min(1.0, max(0.0, confidence))
    
    if confidence >= 0.5:
        result["detected"] = True
        result["confidence"] = round(confidence, 3)
        result["momentum_acceleration"] = round(avg_late_accel, 4)
        result["price_change_1m"] = round(avg_price_change_1m, 4)
        result["price_change_5m"] = round(avg_price_change_5m, 4)
        result["interpretation"] = "Momentum accelerating - trend gaining strength"
    
    return result


def detect_microstructure_shift(
    order_book_rows: List[Dict[str, Any]],
    transaction_rows: List[Dict[str, Any]],
    whale_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Detect order flow shifting bullish across multiple indicators.
    
    Composite signal: order book + transactions + whale data all aligned bullishly.
    This is the most reliable signal for imminent 0.5% moves.
    
    Args:
        order_book_rows: Latest 15 minutes of order book data
        transaction_rows: Latest 15 minutes of transaction data
        whale_rows: Latest 15 minutes of whale activity data
    
    Returns:
        Detection result with confidence score and component scores
    """
    result = {
        "detected": False,
        "confidence": 0.0,
        "signal_type": "microstructure_shift",
        "components": {}
    }
    
    if not all([order_book_rows, transaction_rows, whale_rows]):
        result["error"] = "missing_data_sources"
        return result
    
    if len(order_book_rows) < 5 or len(transaction_rows) < 5 or len(whale_rows) < 3:
        result["error"] = "insufficient_data"
        return result
    
    # Sort all data sources
    ob_sorted = sorted(order_book_rows, key=lambda x: x.get('minute_number', 0))
    tx_sorted = sorted(transaction_rows, key=lambda x: x.get('minute_number', 0))
    whale_sorted = sorted(whale_rows, key=lambda x: x.get('minute_number', 0))
    
    # Get recent data (last 5 minutes)
    recent_ob = ob_sorted[-5:]
    recent_tx = tx_sorted[-5:]
    recent_whale = whale_sorted[-3:] if len(whale_sorted) >= 3 else whale_sorted
    
    # === ORDER BOOK COMPONENT ===
    volume_imbalances = [x.get('volume_imbalance', 0) for x in recent_ob]
    avg_vol_imbalance = np.mean(volume_imbalances)
    vol_imbalance_positive = avg_vol_imbalance > 0.1
    
    depth_ratios = [x.get('depth_imbalance_ratio', 1.0) for x in recent_ob]
    avg_depth_ratio = np.mean(depth_ratios)
    depth_ratio_bullish = avg_depth_ratio > 1.05  # Bids > asks by 5%
    
    microprice_devs = [x.get('microprice_deviation', 0) for x in recent_ob]
    avg_microprice_dev = np.mean(microprice_devs)
    microprice_bullish = avg_microprice_dev > 0
    
    aggression_ratios = [x.get('aggression_ratio', 1.0) for x in recent_ob]
    avg_aggression = np.mean(aggression_ratios)
    aggression_bullish = avg_aggression > 1.0  # Bids more aggressive
    
    ob_score = sum([
        0.30 if vol_imbalance_positive else 0,
        0.30 if depth_ratio_bullish else 0,
        0.25 if microprice_bullish else 0,
        0.15 if aggression_bullish else 0
    ])
    
    result["components"]["order_book"] = {
        "score": round(ob_score, 3),
        "volume_imbalance": round(avg_vol_imbalance, 3),
        "depth_ratio": round(avg_depth_ratio, 3),
        "microprice_dev": round(avg_microprice_dev, 3)
    }
    
    # === TRANSACTION COMPONENT ===
    buy_pressures = [x.get('buy_sell_pressure', 0) for x in recent_tx]
    avg_buy_pressure = np.mean(buy_pressures)
    buy_pressure_positive = avg_buy_pressure > 0.15
    
    volume_accel = [x.get('volume_acceleration_ratio', 1.0) for x in recent_tx]
    avg_vol_accel = np.mean(volume_accel)
    volume_accelerating = avg_vol_accel > 1.1  # 10% increase
    
    whale_vol_pcts = [x.get('whale_volume_pct', 0) for x in recent_tx]
    avg_whale_vol = np.mean(whale_vol_pcts)
    whale_participation = avg_whale_vol > 20  # Whales active
    
    buy_trade_pcts = [x.get('buy_trade_pct', 50) for x in recent_tx]
    avg_buy_trade_pct = np.mean(buy_trade_pcts)
    buy_trades_dominant = avg_buy_trade_pct > 55
    
    tx_score = sum([
        0.35 if buy_pressure_positive else 0,
        0.30 if volume_accelerating else 0,
        0.20 if whale_participation else 0,
        0.15 if buy_trades_dominant else 0
    ])
    
    result["components"]["transactions"] = {
        "score": round(tx_score, 3),
        "buy_pressure": round(avg_buy_pressure, 3),
        "volume_accel": round(avg_vol_accel, 3),
        "whale_volume_pct": round(avg_whale_vol, 2)
    }
    
    # === WHALE COMPONENT ===
    net_flow_ratios = [x.get('net_flow_ratio', 0) for x in recent_whale]
    avg_net_flow = np.mean(net_flow_ratios)
    net_flow_bullish = avg_net_flow > 0.2
    
    acc_ratios = [x.get('accumulation_ratio', 1.0) for x in recent_whale]
    avg_acc_ratio = np.mean(acc_ratios)
    accumulation_strong = avg_acc_ratio > 1.5
    
    strong_acc_pcts = [x.get('strong_accumulation_pct', 0) for x in recent_whale]
    avg_strong_acc = np.mean(strong_acc_pcts)
    strong_acc_present = avg_strong_acc > 5
    
    whale_score = sum([
        0.40 if net_flow_bullish else 0,
        0.35 if accumulation_strong else 0,
        0.25 if strong_acc_present else 0
    ])
    
    result["components"]["whale_activity"] = {
        "score": round(whale_score, 3),
        "net_flow_ratio": round(avg_net_flow, 3),
        "accumulation_ratio": round(avg_acc_ratio, 3),
        "strong_accumulation_pct": round(avg_strong_acc, 2)
    }
    
    # === AGGREGATE SCORE ===
    # Weight: order book 35%, transactions 40%, whale 25%
    aggregate_score = (ob_score * 0.35 + tx_score * 0.40 + whale_score * 0.25)
    
    # Additional bonus for all three aligned
    all_aligned = (ob_score > 0.6 and tx_score > 0.6 and whale_score > 0.6)
    if all_aligned:
        aggregate_score = min(1.0, aggregate_score * 1.15)  # 15% bonus
    
    confidence = min(1.0, max(0.0, aggregate_score))
    
    if confidence >= 0.5:
        result["detected"] = True
        result["confidence"] = round(confidence, 3)
        result["all_aligned"] = all_aligned
        result["interpretation"] = (
            "Strong microstructure shift - all signals aligned bullishly" if all_aligned
            else "Microstructure shift detected - majority of signals bullish"
        )
    
    return result


# =============================================================================
# VELOCITY AND ACCELERATION CALCULATIONS FOR MICRO-MOVEMENT DETECTION
# =============================================================================

def calculate_velocity_metrics(
    current_values: List[float],
    timestamps: List[datetime],
    lookback_periods: int = 5
) -> Dict[str, float]:
    """
    Calculate velocity (first derivative) and acceleration (second derivative).
    
    Args:
        current_values: List of values ordered oldest to newest
        timestamps: Corresponding timestamps
        lookback_periods: Number of periods to use for calculation
        
    Returns:
        Dictionary with velocity and acceleration metrics
    """
    if len(current_values) < 3:
        return {"velocity": 0.0, "acceleration": 0.0, "jerk": 0.0}
    
    # Calculate time deltas in seconds
    time_deltas = []
    for i in range(1, len(timestamps)):
        delta = (timestamps[i] - timestamps[i-1]).total_seconds()
        time_deltas.append(delta if delta > 0 else 1.0)
    
    # Calculate velocity (first derivative)
    velocities = []
    for i in range(1, len(current_values)):
        velocity = (current_values[i] - current_values[i-1]) / time_deltas[i-1]
        velocities.append(velocity)
    
    # Calculate acceleration (second derivative)
    accelerations = []
    for i in range(1, len(velocities)):
        if i < len(time_deltas):
            accel = (velocities[i] - velocities[i-1]) / time_deltas[i]
            accelerations.append(accel)
    
    # Calculate jerk (third derivative) for momentum quality
    jerks = []
    for i in range(1, len(accelerations)):
        if i + 1 < len(time_deltas):
            jerk = (accelerations[i] - accelerations[i-1]) / time_deltas[i+1]
            jerks.append(jerk)
    
    return {
        "velocity": velocities[-1] if velocities else 0.0,
        "velocity_avg": float(np.mean(velocities[-lookback_periods:])) if velocities else 0.0,
        "acceleration": accelerations[-1] if accelerations else 0.0,
        "acceleration_avg": float(np.mean(accelerations[-lookback_periods:])) if accelerations else 0.0,
        "jerk": jerks[-1] if jerks else 0.0,
        "momentum_persistence": _calculate_momentum_persistence(velocities),
    }


def _calculate_momentum_persistence(velocities: List[float], threshold: float = 0.0) -> float:
    """
    Calculate how consistently velocity stays in one direction.
    Returns ratio of periods velocity stayed positive (or negative).
    """
    if not velocities:
        return 0.0
    
    # Count consecutive same-sign periods
    if velocities[-1] > threshold:
        # Currently positive - count backwards
        count = 0
        for v in reversed(velocities):
            if v > threshold:
                count += 1
            else:
                break
        return count / len(velocities)
    else:
        # Currently negative
        count = 0
        for v in reversed(velocities):
            if v <= threshold:
                count += 1
            else:
                break
        return -count / len(velocities)


def calculate_order_book_velocities(
    order_book_rows: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate velocity metrics for order book data.
    
    Key insight: Order book tilting SPEED is more predictive than tilt level.
    """
    if len(order_book_rows) < 3:
        return {}
    
    # Sort by minute (ascending)
    sorted_rows = sorted(order_book_rows, key=lambda x: x.get('minute_number', 0))
    
    # Extract time series
    imbalances = [float(r.get('volume_imbalance', 0) or 0) for r in sorted_rows]
    depth_ratios = [float(r.get('depth_imbalance_ratio', 1.0) or 1.0) for r in sorted_rows]
    spreads = [float(r.get('spread_bps', 10) or 10) for r in sorted_rows]
    bid_depths = [float(r.get('bid_depth_10', 0) or 0) for r in sorted_rows]
    ask_depths = [float(r.get('ask_depth_10', 0) or 0) for r in sorted_rows]
    
    # Create dummy timestamps (1 minute apart)
    base_time = datetime.now()
    timestamps = [base_time + timedelta(minutes=i) for i in range(len(sorted_rows))]
    
    # Calculate velocities
    imbalance_vel = calculate_velocity_metrics(imbalances, timestamps)
    depth_ratio_vel = calculate_velocity_metrics(depth_ratios, timestamps)
    spread_vel = calculate_velocity_metrics(spreads, timestamps)
    
    # Bid/Ask depth velocities
    bid_changes = [bid_depths[i] - bid_depths[i-1] for i in range(1, len(bid_depths))]
    ask_changes = [ask_depths[i] - ask_depths[i-1] for i in range(1, len(ask_depths))]
    
    # Cumulative imbalance (5-minute sum)
    cumulative_imbalance = sum(imbalances[-5:]) if len(imbalances) >= 5 else sum(imbalances)
    
    # Imbalance consistency (% of periods with same sign)
    recent_imbalances = imbalances[-5:] if len(imbalances) >= 5 else imbalances
    if recent_imbalances:
        positive_count = sum(1 for x in recent_imbalances if x > 0)
        consistency = max(positive_count, len(recent_imbalances) - positive_count) / len(recent_imbalances)
    else:
        consistency = 0.5
    
    # Liquidity gap score
    latest = sorted_rows[-1]
    bid_liq = float(latest.get('bid_liquidity_share_pct', 50) or 50)
    ask_liq = float(latest.get('ask_liquidity_share_pct', 50) or 50)
    liquidity_gap = abs(bid_liq - ask_liq) / 100.0
    
    # Liquidity score (composite)
    total_liq = float(latest.get('total_liquidity', 0) or 0)
    liquidity_score = min(1.0, total_liq / 100000) if total_liq > 0 else 0.5  # Normalize to 100k
    
    return {
        "imbalance_velocity_1m": imbalance_vel["velocity"],
        "imbalance_velocity_30s": imbalance_vel["velocity"] / 2,  # Estimate 30s
        "imbalance_acceleration": imbalance_vel["acceleration"],
        "depth_ratio_velocity": depth_ratio_vel["velocity"],
        "spread_velocity": spread_vel["velocity"],
        "bid_depth_velocity": float(np.mean(bid_changes[-3:])) if bid_changes else 0,
        "ask_depth_velocity": float(np.mean(ask_changes[-3:])) if ask_changes else 0,
        "cumulative_imbalance_5m": cumulative_imbalance,
        "imbalance_consistency_5m": consistency,
        "liquidity_gap_score": liquidity_gap,
        "liquidity_score": liquidity_score,
        "liquidity_concentration": 0.5,  # Default - would need more detailed order book data
        "spread_percentile_1h": 0.5,  # Default - would need historical spread data
        "imbalance_momentum_persistence": imbalance_vel["momentum_persistence"],
    }


def calculate_transaction_velocities(
    transaction_rows: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate velocity metrics for transaction data.
    
    Key insight: Volume ACCELERATION predicts breakouts better than volume level.
    """
    if len(transaction_rows) < 3:
        return {}
    
    sorted_rows = sorted(transaction_rows, key=lambda x: x.get('minute_number', 0))
    
    # Extract time series
    volumes = [float(r.get('total_volume_usd', 0) or 0) for r in sorted_rows]
    buy_pressures = [float(r.get('buy_sell_pressure', 0) or 0) for r in sorted_rows]
    trade_counts = [int(r.get('trade_count', 0) or 0) for r in sorted_rows]
    buy_vol_pcts = [float(r.get('buy_volume_pct', 50) or 50) for r in sorted_rows]
    sell_vol_pcts = [float(r.get('sell_volume_pct', 50) or 50) for r in sorted_rows]
    
    buy_volumes = [buy_vol_pcts[i] / 100 * volumes[i] for i in range(len(volumes))]
    sell_volumes = [sell_vol_pcts[i] / 100 * volumes[i] for i in range(len(volumes))]
    
    base_time = datetime.now()
    timestamps = [base_time + timedelta(minutes=i) for i in range(len(sorted_rows))]
    
    volume_vel = calculate_velocity_metrics(volumes, timestamps)
    pressure_vel = calculate_velocity_metrics(buy_pressures, timestamps)
    
    # Cumulative delta (running sum of buy - sell)
    deltas = [b - s for b, s in zip(buy_volumes, sell_volumes)]
    cumulative_delta = sum(deltas)
    cumulative_delta_5m = sum(deltas[-5:]) if len(deltas) >= 5 else cumulative_delta
    
    # Trade intensity (trades per second, normalized)
    avg_trade_count = float(np.mean(trade_counts)) if trade_counts else 1
    trade_intensity = trade_counts[-1] / max(avg_trade_count, 1) if trade_counts else 1.0
    
    # Intensity velocity
    intensity_series = [tc / max(avg_trade_count, 1) for tc in trade_counts]
    intensity_vel = calculate_velocity_metrics(intensity_series, timestamps)
    
    # Large trade intensity
    large_trades = [int(r.get('large_trade_count', 0) or 0) for r in sorted_rows]
    large_trade_intensity = float(np.mean(large_trades[-3:])) if large_trades else 0
    
    # Delta divergence (cumulative delta vs price direction)
    price_changes = [float(r.get('price_change_1m', 0) or 0) for r in sorted_rows]
    cumulative_price = sum(price_changes)
    
    # Divergence: positive delta but negative price = bearish divergence
    if cumulative_delta > 0 and cumulative_price < 0:
        delta_divergence = -abs(cumulative_delta) * abs(cumulative_price)
    elif cumulative_delta < 0 and cumulative_price > 0:
        delta_divergence = -abs(cumulative_delta) * abs(cumulative_price)
    else:
        delta_divergence = abs(cumulative_delta) * abs(cumulative_price) * np.sign(cumulative_delta)
    
    # Volume percentile (simplified - would need historical data for real percentile)
    volume_percentile = min(1.0, volumes[-1] / max(float(np.mean(volumes)), 1)) if volumes else 0.5
    
    return {
        "volume_velocity": volume_vel["velocity"],
        "volume_acceleration": volume_vel["acceleration"],
        "volume_percentile_1h": volume_percentile,
        "cumulative_delta": cumulative_delta,
        "cumulative_delta_5m": cumulative_delta_5m,
        "delta_divergence": delta_divergence,
        "trade_intensity": trade_intensity,
        "trade_intensity_velocity": intensity_vel["velocity"],
        "large_trade_intensity": large_trade_intensity,
        "pressure_velocity": pressure_vel["velocity"],
        "pressure_acceleration": pressure_vel["acceleration"],
    }


def calculate_whale_velocities(
    whale_rows: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate velocity metrics for whale activity data.
    """
    if len(whale_rows) < 2:
        return {}
    
    sorted_rows = sorted(whale_rows, key=lambda x: x.get('minute_number', 0))
    
    # Extract time series
    net_flows = [float(r.get('net_flow_ratio', 0) or 0) for r in sorted_rows]
    total_moved = [float(r.get('total_sol_moved', 0) or 0) for r in sorted_rows]
    
    base_time = datetime.now()
    timestamps = [base_time + timedelta(minutes=i) for i in range(len(sorted_rows))]
    
    flow_vel = calculate_velocity_metrics(net_flows, timestamps)
    
    # Cumulative flow (10-minute sum)
    cumulative_flow = sum(net_flows[-10:]) if len(net_flows) >= 10 else sum(net_flows)
    
    # Stealth accumulation score (buying without price impact)
    # High net inflow but low price change = stealth accumulation
    latest = sorted_rows[-1]
    net_flow = float(latest.get('net_flow_ratio', 0) or 0)
    stealth_score = 0.0
    if net_flow > 0.2:  # Significant accumulation
        stealth_score = min(1.0, net_flow / 0.5)
    
    # Distribution urgency (how fast are whales selling)
    distribution_urgency = 0.0
    if net_flow < -0.1:
        distribution_urgency = min(1.0, abs(net_flow) / 0.4)
    
    # Activity regime (0=quiet, 1=normal, 2=active)
    total_sol = float(latest.get('total_sol_moved', 0) or 0)
    if total_sol < 1000:
        activity_regime = 0
    elif total_sol < 10000:
        activity_regime = 1
    else:
        activity_regime = 2
    
    # Time since last large move and frequency
    movement_count = int(latest.get('movement_count', 0) or 0)
    massive_count = int(latest.get('massive_move_count', 0) or 0)
    
    return {
        "flow_velocity": flow_vel["velocity"],
        "flow_acceleration": flow_vel["acceleration"],
        "cumulative_flow_10m": cumulative_flow,
        "stealth_accumulation_score": stealth_score,
        "distribution_urgency": distribution_urgency,
        "whale_activity_regime": activity_regime,
        "time_since_last_large_move": 60.0,  # Default - would need timestamp tracking
        "large_move_frequency_5m": massive_count / 5.0 if massive_count else 0,
    }


def calculate_price_velocities(
    price_rows: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate velocity metrics for price movements.
    """
    if len(price_rows) < 3:
        return {}
    
    sorted_rows = sorted(price_rows, key=lambda x: x.get('minute_number', 0))
    
    # Extract price changes
    price_changes = [float(r.get('price_change_1m', 0) or 0) for r in sorted_rows]
    volatilities = [float(r.get('volatility_pct', 0) or 0) for r in sorted_rows]
    close_prices = [float(r.get('close_price', 0) or 0) for r in sorted_rows]
    high_prices = [float(r.get('high_price', 0) or 0) for r in sorted_rows]
    low_prices = [float(r.get('low_price', 0) or 0) for r in sorted_rows]
    
    base_time = datetime.now()
    timestamps = [base_time + timedelta(minutes=i) for i in range(len(sorted_rows))]
    
    price_vel = calculate_velocity_metrics(price_changes, timestamps)
    vol_vel = calculate_velocity_metrics(volatilities, timestamps)
    
    # Realized volatility (std of returns)
    realized_vol = float(np.std(price_changes)) if len(price_changes) >= 2 else 0
    
    # Volatility of volatility
    vol_of_vol = float(np.std(volatilities)) if len(volatilities) >= 2 else 0
    
    # Volatility regime
    avg_vol = float(np.mean(volatilities)) if volatilities else 0
    if avg_vol < 0.1:
        vol_regime = 0  # Low
    elif avg_vol < 0.3:
        vol_regime = 1  # Normal
    else:
        vol_regime = 2  # High
    
    # Higher highs/lows count (5 minutes)
    recent_highs = high_prices[-5:] if len(high_prices) >= 5 else high_prices
    recent_lows = low_prices[-5:] if len(low_prices) >= 5 else low_prices
    
    higher_highs = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i] > recent_highs[i-1])
    higher_lows = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] > recent_lows[i-1])
    
    # Trend strength (EMA crossover approximation)
    if len(close_prices) >= 5:
        short_ma = float(np.mean(close_prices[-3:]))
        long_ma = float(np.mean(close_prices[-5:]))
        trend_strength = (short_ma - long_ma) / long_ma * 100 if long_ma != 0 else 0
    else:
        trend_strength = 0
    
    # Support/resistance distance (simplified)
    if close_prices and high_prices and low_prices:
        current_price = close_prices[-1]
        recent_high = max(high_prices[-10:]) if len(high_prices) >= 10 else max(high_prices)
        recent_low = min(low_prices[-10:]) if len(low_prices) >= 10 else min(low_prices)
        
        dist_resistance = (recent_high - current_price) / current_price * 100 if current_price else 0
        dist_support = (current_price - recent_low) / current_price * 100 if current_price else 0
        
        # Breakout imminence (closer to resistance with momentum = higher score)
        if dist_resistance < 0.5 and price_vel["velocity"] > 0:
            breakout_imminence = min(1.0, (0.5 - dist_resistance) / 0.5 + abs(price_vel["velocity"]) / 0.01)
        else:
            breakout_imminence = 0
    else:
        dist_resistance = 0
        dist_support = 0
        breakout_imminence = 0
    
    return {
        "price_velocity_1m": price_vel["velocity"],
        "price_velocity_30s": price_vel["velocity"] / 2,  # Estimate
        "velocity_acceleration": price_vel["acceleration"],
        "momentum_persistence": price_vel["momentum_persistence"],
        "realized_volatility_1m": realized_vol,
        "volatility_of_volatility": vol_of_vol,
        "volatility_regime": vol_regime,
        "trend_strength_ema": trend_strength,
        "price_vs_vwap_pct": 0,  # Would need VWAP calculation
        "price_vs_twap_pct": 0,  # Would need TWAP calculation
        "higher_highs_count_5m": higher_highs,
        "higher_lows_count_5m": higher_lows,
        "distance_to_resistance_pct": dist_resistance,
        "distance_to_support_pct": dist_support,
        "breakout_imminence_score": breakout_imminence,
    }


def calculate_vpin_estimate(
    transaction_rows: List[Dict[str, Any]],
    num_buckets: int = 10
) -> float:
    """
    Estimate Volume-Synchronized Probability of Informed Trading (VPIN).
    
    Higher VPIN = more informed trading = higher probability of directional move.
    
    Simplified VPIN calculation:
    VPIN = |sum(buy_volume) - sum(sell_volume)| / total_volume
    """
    if not transaction_rows:
        return 0.0
    
    total_buy = sum(
        float(r.get('buy_volume_pct', 50) or 50) / 100 * float(r.get('total_volume_usd', 0) or 0)
        for r in transaction_rows
    )
    total_sell = sum(
        float(r.get('sell_volume_pct', 50) or 50) / 100 * float(r.get('total_volume_usd', 0) or 0)
        for r in transaction_rows
    )
    total_volume = total_buy + total_sell
    
    if total_volume == 0:
        return 0.0
    
    vpin = abs(total_buy - total_sell) / total_volume
    return min(1.0, vpin)


def calculate_order_flow_toxicity(
    transaction_rows: List[Dict[str, Any]],
    order_book_rows: List[Dict[str, Any]]
) -> float:
    """
    Calculate order flow toxicity (adverse selection metric).
    
    High toxicity = orders tend to move price against you = institutional flow.
    """
    if not transaction_rows or not order_book_rows:
        return 0.0
    
    # Get latest data
    latest_tx = transaction_rows[0] if transaction_rows else {}
    latest_ob = order_book_rows[0] if order_book_rows else {}
    
    # Components of toxicity
    volume_imbalance = abs(float(latest_ob.get('volume_imbalance', 0) or 0))
    whale_volume_pct = float(latest_tx.get('whale_volume_pct', 0) or 0) / 100
    spread_bps = float(latest_ob.get('spread_bps', 10) or 10)
    
    # Higher toxicity when:
    # 1. Volume imbalance is high (directional flow)
    # 2. Whale participation is high (informed traders)
    # 3. Spread is wide (market makers protecting themselves)
    
    toxicity = (
        volume_imbalance * 0.4 +
        whale_volume_pct * 0.35 +
        min(1.0, spread_bps / 20) * 0.25  # Normalize spread contribution
    )
    
    return min(1.0, max(0.0, toxicity))


# =============================================================================
# CROSS-ASSET CORRELATION AND LEAD-LAG ANALYSIS
# =============================================================================

def calculate_cross_asset_metrics(
    sol_price_rows: List[Dict[str, Any]],
    btc_price_rows: List[Dict[str, Any]],
    eth_price_rows: List[Dict[str, Any]]
) -> Dict[str, float]:
    """
    Calculate cross-asset correlations and lead-lag relationships.
    
    Key insight: BTC often leads SOL by 1-2 minutes. ETH can diverge.
    """
    result = {
        "btc_sol_correlation_1m": 0.0,
        "btc_sol_correlation_5m": 0.0,
        "btc_leads_sol_lag1": 0.0,
        "btc_leads_sol_lag2": 0.0,
        "sol_beta_to_btc": 1.0,
        "eth_sol_correlation_1m": 0.0,
        "eth_leads_sol_lag1": 0.0,
        "sol_beta_to_eth": 1.0,
        "btc_sol_divergence": 0.0,
        "eth_sol_divergence": 0.0,
        "cross_asset_momentum_align": 0.0,
    }
    
    if not all([sol_price_rows, btc_price_rows, eth_price_rows]):
        return result
    
    # Extract price change series (sorted oldest to newest)
    def get_changes(rows, field='price_change_1m'):
        sorted_rows = sorted(rows, key=lambda x: x.get('minute_number', 0))
        return [float(r.get(field, 0) or 0) for r in sorted_rows]
    
    sol_changes = get_changes(sol_price_rows)
    btc_changes = get_changes(btc_price_rows)
    eth_changes = get_changes(eth_price_rows)
    
    min_len = min(len(sol_changes), len(btc_changes), len(eth_changes))
    if min_len < 5:
        return result
    
    # Trim to same length
    sol_changes = sol_changes[-min_len:]
    btc_changes = btc_changes[-min_len:]
    eth_changes = eth_changes[-min_len:]
    
    # Calculate correlations (handle edge cases)
    try:
        if len(btc_changes) > 2 and np.std(btc_changes) > 0 and np.std(sol_changes) > 0:
            result["btc_sol_correlation_1m"] = float(np.corrcoef(btc_changes, sol_changes)[0, 1])
        if len(eth_changes) > 2 and np.std(eth_changes) > 0 and np.std(sol_changes) > 0:
            result["eth_sol_correlation_1m"] = float(np.corrcoef(eth_changes, sol_changes)[0, 1])
    except (ValueError, FloatingPointError):
        pass
    
    # 5-minute correlation (using 5-minute changes if available)
    sol_5m = get_changes(sol_price_rows, 'price_change_5m')[-min_len:]
    btc_5m = get_changes(btc_price_rows, 'price_change_5m')[-min_len:]
    try:
        if len(sol_5m) > 2 and len(btc_5m) > 2 and np.std(sol_5m) > 0 and np.std(btc_5m) > 0:
            result["btc_sol_correlation_5m"] = float(np.corrcoef(btc_5m, sol_5m)[0, 1])
    except (ValueError, FloatingPointError):
        pass
    
    # Lead-lag: Does BTC[t-1] predict SOL[t]?
    try:
        if len(btc_changes) > 3 and len(sol_changes) > 3:
            btc_lagged_1 = btc_changes[:-1]  # BTC at t-1
            sol_current = sol_changes[1:]    # SOL at t
            if np.std(btc_lagged_1) > 0 and np.std(sol_current) > 0:
                result["btc_leads_sol_lag1"] = float(np.corrcoef(btc_lagged_1, sol_current)[0, 1])
            
            if len(btc_changes) > 4:
                btc_lagged_2 = btc_changes[:-2]  # BTC at t-2
                sol_current_2 = sol_changes[2:]  # SOL at t
                if np.std(btc_lagged_2) > 0 and np.std(sol_current_2) > 0:
                    result["btc_leads_sol_lag2"] = float(np.corrcoef(btc_lagged_2, sol_current_2)[0, 1])
    except (ValueError, FloatingPointError):
        pass
    
    try:
        if len(eth_changes) > 3 and len(sol_changes) > 3:
            eth_lagged_1 = eth_changes[:-1]
            sol_current = sol_changes[1:]
            if np.std(eth_lagged_1) > 0 and np.std(sol_current) > 0:
                result["eth_leads_sol_lag1"] = float(np.corrcoef(eth_lagged_1, sol_current)[0, 1])
    except (ValueError, FloatingPointError):
        pass
    
    # Beta calculation (sensitivity)
    # SOL_return = alpha + beta * BTC_return
    try:
        if len(btc_changes) > 2:
            btc_var = np.var(btc_changes)
            if btc_var > 0:
                covariance = np.cov(sol_changes, btc_changes)[0, 1]
                result["sol_beta_to_btc"] = float(covariance / btc_var)
    except (ValueError, FloatingPointError):
        pass
    
    try:
        if len(eth_changes) > 2:
            eth_var = np.var(eth_changes)
            if eth_var > 0:
                covariance = np.cov(sol_changes, eth_changes)[0, 1]
                result["sol_beta_to_eth"] = float(covariance / eth_var)
    except (ValueError, FloatingPointError):
        pass
    
    # Divergence: SOL outperforming/underperforming vs BTC/ETH
    recent_sol = sum(sol_changes[-3:]) if len(sol_changes) >= 3 else sol_changes[-1] if sol_changes else 0
    recent_btc = sum(btc_changes[-3:]) if len(btc_changes) >= 3 else btc_changes[-1] if btc_changes else 0
    recent_eth = sum(eth_changes[-3:]) if len(eth_changes) >= 3 else eth_changes[-1] if eth_changes else 0
    
    # Positive divergence = SOL outperforming
    result["btc_sol_divergence"] = recent_sol - recent_btc * result["sol_beta_to_btc"]
    result["eth_sol_divergence"] = recent_sol - recent_eth * result["sol_beta_to_eth"]
    
    # Momentum alignment: Are all three moving same direction?
    sol_direction = np.sign(recent_sol)
    btc_direction = np.sign(recent_btc)
    eth_direction = np.sign(recent_eth)
    
    if sol_direction == btc_direction == eth_direction and sol_direction != 0:
        # All aligned
        result["cross_asset_momentum_align"] = sol_direction * (abs(recent_sol) + abs(recent_btc) + abs(recent_eth)) / 3
    else:
        # Mixed signals
        result["cross_asset_momentum_align"] = (sol_direction + btc_direction + eth_direction) / 3 * 0.3
    
    # Handle NaN values
    for key in result:
        if isinstance(result[key], float) and (np.isnan(result[key]) or np.isinf(result[key])):
            result[key] = 0.0
    
    return result


# =============================================================================
# 30-SECOND INTERVAL CALCULATIONS
# =============================================================================

def fetch_30_second_data(
    start_time: datetime,
    end_time: datetime,
    token: str = "SOL"
) -> List[Dict[str, Any]]:
    """
    Fetch and aggregate data at 30-second intervals for the full 15-minute window.
    Returns 30 buckets (one per 30-second interval).
    """
    # Query raw second-level data and bucket into 30-second intervals
    query = """
        WITH thirty_sec_buckets AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) + 
                    INTERVAL '30 seconds' * FLOOR(EXTRACT(SECOND FROM timestamp) / 30) AS bucket_ts,
                price,
                timestamp
            FROM prices
            WHERE timestamp >= %s AND timestamp <= %s AND token = %s
        ),
        aggregated AS (
            SELECT 
                bucket_ts,
                (ARRAY_AGG(price ORDER BY timestamp ASC))[1] AS open_price,
                (ARRAY_AGG(price ORDER BY timestamp DESC))[1] AS close_price,
                MIN(price) AS low_price,
                MAX(price) AS high_price,
                AVG(price) AS avg_price,
                STDDEV(price) AS price_stddev,
                COUNT(*) AS tick_count
            FROM thirty_sec_buckets
            GROUP BY bucket_ts
        )
        SELECT 
            bucket_ts,
            open_price,
            close_price,
            high_price,
            low_price,
            avg_price,
            price_stddev,
            tick_count,
            ROUND(CAST((close_price - open_price) / NULLIF(open_price, 0) * 100 AS NUMERIC), 6) AS price_change_30s,
            ROUND(CAST((high_price - low_price) / NULLIF(avg_price, 0) * 100 AS NUMERIC), 6) AS volatility_30s
        FROM aggregated
        ORDER BY bucket_ts ASC
    """
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, [start_time, end_time, token])
                results = cursor.fetchall()
                return results if results else []
    except Exception as e:
        logger.error(f"Failed to fetch 30-second data: {e}")
        return []


def calculate_30_second_interval_metrics(
    thirty_sec_rows: List[Dict[str, Any]],
    interval_index: int
) -> Dict[str, Any]:
    """
    Calculate metrics for a specific 30-second interval.
    
    Args:
        thirty_sec_rows: All 30-second buckets (sorted oldest to newest)
        interval_index: Which interval (0-29) to calculate for
        
    Returns:
        Metrics specific to this 30-second interval
    """
    result = {
        "ts_price_change_30s": 0.0,
        "ts_volume_30s": 0.0,
        "ts_buy_sell_pressure_30s": 0.0,
        "ts_imbalance_30s": 0.0,
        "ts_trade_count_30s": 0,
        "ts_momentum_30s": 0.0,
        "ts_volatility_30s": 0.0,
        "ts_open_price": 0.0,
        "ts_close_price": 0.0,
        "ts_high_price": 0.0,
        "ts_low_price": 0.0,
    }
    
    if not thirty_sec_rows or interval_index >= len(thirty_sec_rows):
        return result
    
    current = thirty_sec_rows[interval_index]
    
    result["ts_price_change_30s"] = float(current.get("price_change_30s", 0) or 0)
    result["ts_volatility_30s"] = float(current.get("volatility_30s", 0) or 0)
    result["ts_open_price"] = float(current.get("open_price", 0) or 0)
    result["ts_close_price"] = float(current.get("close_price", 0) or 0)
    result["ts_high_price"] = float(current.get("high_price", 0) or 0)
    result["ts_low_price"] = float(current.get("low_price", 0) or 0)
    result["ts_trade_count_30s"] = int(current.get("tick_count", 0) or 0)
    
    # Calculate momentum (change in price change)
    if interval_index > 0:
        prev = thirty_sec_rows[interval_index - 1]
        prev_change = float(prev.get("price_change_30s", 0) or 0)
        curr_change = result["ts_price_change_30s"]
        result["ts_momentum_30s"] = curr_change - prev_change
    
    return result


def calculate_30_second_velocity_at_interval(
    thirty_sec_rows: List[Dict[str, Any]],
    interval_index: int,
    lookback: int = 5
) -> Dict[str, float]:
    """
    Calculate velocity metrics at a specific 30-second interval using rolling window.
    
    This gives per-interval velocity which is crucial for detecting momentum shifts.
    """
    result = {
        "ts_price_velocity": 0.0,
        "ts_price_acceleration": 0.0,
        "ts_momentum_persistence": 0.0,
        "ts_volatility_regime": 0,
    }
    
    if not thirty_sec_rows or interval_index < 2:
        return result
    
    # Get lookback window of price changes
    start_idx = max(0, interval_index - lookback + 1)
    window = thirty_sec_rows[start_idx:interval_index + 1]
    
    if len(window) < 3:
        return result
    
    # Extract price changes
    changes = [float(r.get("price_change_30s", 0) or 0) for r in window]
    
    # Velocity = rate of change of price changes (acceleration of price)
    velocities = [changes[i] - changes[i-1] for i in range(1, len(changes))]
    result["ts_price_velocity"] = velocities[-1] if velocities else 0.0
    
    # Acceleration = rate of change of velocity
    if len(velocities) >= 2:
        accelerations = [velocities[i] - velocities[i-1] for i in range(1, len(velocities))]
        result["ts_price_acceleration"] = accelerations[-1] if accelerations else 0.0
    
    # Momentum persistence = how many consecutive periods in same direction
    if velocities:
        current_sign = np.sign(velocities[-1])
        count = 0
        for v in reversed(velocities):
            if np.sign(v) == current_sign and current_sign != 0:
                count += 1
            else:
                break
        result["ts_momentum_persistence"] = count / len(velocities) * current_sign
    
    # Volatility regime (based on recent volatility)
    volatilities = [float(r.get("volatility_30s", 0) or 0) for r in window]
    avg_vol = np.mean(volatilities) if volatilities else 0
    if avg_vol < 0.05:
        result["ts_volatility_regime"] = 0  # Low
    elif avg_vol < 0.15:
        result["ts_volatility_regime"] = 1  # Normal
    else:
        result["ts_volatility_regime"] = 2  # High
    
    return result


def calculate_30_second_metrics(
    thirty_sec_rows: List[Dict[str, Any]],
    transaction_rows: List[Dict[str, Any]] = None,
    order_book_rows: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Calculate metrics at 30-second granularity.
    """
    result = {
        "price_change_30s": 0.0,
        "volume_30s": 0.0,
        "buy_sell_pressure_30s": 0.0,
        "imbalance_30s": 0.0,
        "trade_count_30s": 0,
        "momentum_30s": 0.0,
        "volatility_30s": 0.0,
    }
    
    if not thirty_sec_rows:
        return result
    
    # Sort newest first
    sorted_rows = sorted(thirty_sec_rows, key=lambda x: x.get('bucket_ts', datetime.min), reverse=True)
    
    # Price metrics
    if len(sorted_rows) >= 2:
        current = sorted_rows[0]
        previous = sorted_rows[1]
        
        result["price_change_30s"] = float(current.get("price_change_30s", 0) or 0)
        result["volatility_30s"] = float(current.get("volatility_30s", 0) or 0)
        
        # Momentum at 30s
        curr_change = float(current.get("price_change_30s", 0) or 0)
        prev_change = float(previous.get("price_change_30s", 0) or 0)
        result["momentum_30s"] = curr_change - prev_change
    elif len(sorted_rows) == 1:
        current = sorted_rows[0]
        result["price_change_30s"] = float(current.get("price_change_30s", 0) or 0)
        result["volatility_30s"] = float(current.get("volatility_30s", 0) or 0)
    
    # Aggregate volume for 30-second window (from transactions)
    if transaction_rows:
        # Estimate 30-second volume as half of 1-minute volume
        latest_tx = transaction_rows[0] if transaction_rows else {}
        result["volume_30s"] = float(latest_tx.get("total_volume_usd", 0) or 0) / 2
        result["buy_sell_pressure_30s"] = float(latest_tx.get("buy_sell_pressure", 0) or 0)
        result["trade_count_30s"] = int(latest_tx.get("trade_count", 0) or 0) // 2
    
    # Order book at 30s
    if order_book_rows:
        latest_ob = order_book_rows[0] if order_book_rows else {}
        result["imbalance_30s"] = float(latest_ob.get("volume_imbalance", 0) or 0)
    
    return result


# =============================================================================
# COMPOSITE MICRO-MOVE PROBABILITY SCORING
# =============================================================================

def calculate_micro_move_composite_score(
    order_book_rows: List[Dict[str, Any]],
    transaction_rows: List[Dict[str, Any]],
    whale_rows: List[Dict[str, Any]],
    price_rows: List[Dict[str, Any]],
    cross_asset_metrics: Dict[str, float],
    velocity_metrics: Dict[str, float],
    micro_patterns: Dict[str, Any],
    target_move_pct: float = 0.5
) -> Dict[str, Any]:
    """
    Calculate comprehensive probability score for 0.4-0.6% move.
    
    This is the PRIMARY signal your bot should use for entry decisions.
    
    Args:
        target_move_pct: Target move size (default 0.5%)
        
    Returns:
        Dictionary with probability, direction, confidence, and component scores
    """
    result = {
        "micro_move_probability": 0.0,
        "micro_move_direction": 0.0,
        "micro_move_confidence": 0.0,
        "micro_move_timeframe": 5.0,  # Minutes
        "order_flow_score": 0.0,
        "whale_alignment_score": 0.0,
        "momentum_quality_score": 0.0,
        "volatility_regime_score": 0.0,
        "cross_asset_score": 0.0,
        "false_signal_risk": 0.5,
        "adverse_selection_risk": 0.0,
        "slippage_estimate_bps": 5.0,  # bps
    }
    
    if not all([order_book_rows, transaction_rows, price_rows]):
        return result
    
    # Get latest data
    latest_ob = order_book_rows[0] if order_book_rows else {}
    latest_tx = transaction_rows[0] if transaction_rows else {}
    latest_whale = whale_rows[0] if whale_rows else {}
    latest_price = price_rows[0] if price_rows else {}
    
    def _safe_float(val, default=0.0):
        try:
            if val is None:
                return default
            return float(val)
        except (TypeError, ValueError):
            return default
    
    # =========================================================================
    # COMPONENT 1: ORDER FLOW SCORE (30% weight)
    # =========================================================================
    order_flow_components = []
    
    # 1a. Volume imbalance (10%)
    vol_imbalance = _safe_float(latest_ob.get("volume_imbalance", 0))
    vol_imbalance_score = min(1.0, abs(vol_imbalance) / 0.3)  # 0.3 = strong imbalance
    order_flow_components.append(vol_imbalance_score * 0.10)
    
    # 1b. Depth imbalance ratio (8%)
    depth_ratio = _safe_float(latest_ob.get("depth_imbalance_ratio", 1.0), 1.0)
    if depth_ratio > 1.0:
        depth_score = min(1.0, (depth_ratio - 1.0) / 0.2)  # 1.2 ratio = strong
    else:
        depth_score = min(1.0, (1.0 - depth_ratio) / 0.2)
    order_flow_components.append(depth_score * 0.08)
    
    # 1c. Imbalance velocity (7%)
    imb_velocity = _safe_float(velocity_metrics.get("imbalance_velocity_1m", 0))
    imb_vel_score = min(1.0, abs(imb_velocity) / 0.1)
    order_flow_components.append(imb_vel_score * 0.07)
    
    # 1d. Buy/sell pressure (5%)
    buy_pressure = _safe_float(latest_tx.get("buy_sell_pressure", 0))
    pressure_score = min(1.0, abs(buy_pressure) / 0.25)
    order_flow_components.append(pressure_score * 0.05)
    
    order_flow_total = sum(order_flow_components)
    result["order_flow_score"] = round(order_flow_total / 0.30, 3)  # Normalize to 0-1
    
    # =========================================================================
    # COMPONENT 2: MOMENTUM QUALITY (25% weight)
    # =========================================================================
    momentum_components = []
    
    # 2a. Price velocity (8%)
    price_vel = _safe_float(velocity_metrics.get("price_velocity_1m", 0))
    price_vel_score = min(1.0, abs(price_vel) / 0.005)  # 0.5%/min = strong
    momentum_components.append(price_vel_score * 0.08)
    
    # 2b. Momentum acceleration (7%)
    mom_accel = _safe_float(latest_price.get("momentum_acceleration_1m", 0))
    mom_accel_score = min(1.0, abs(mom_accel) / 0.2)
    momentum_components.append(mom_accel_score * 0.07)
    
    # 2c. Volume acceleration (6%)
    vol_accel = _safe_float(velocity_metrics.get("volume_acceleration", 0))
    vol_accel_score = min(1.0, abs(vol_accel) / 50000)  # $50k/min² = strong
    momentum_components.append(vol_accel_score * 0.06)
    
    # 2d. Momentum persistence (4%)
    persistence = _safe_float(velocity_metrics.get("momentum_persistence", 0))
    persistence_score = abs(persistence)
    momentum_components.append(persistence_score * 0.04)
    
    momentum_total = sum(momentum_components)
    result["momentum_quality_score"] = round(momentum_total / 0.25, 3)
    
    # =========================================================================
    # COMPONENT 3: WHALE ALIGNMENT (20% weight)
    # =========================================================================
    whale_components = []
    
    if whale_rows:
        # 3a. Net flow ratio (8%)
        net_flow = _safe_float(latest_whale.get("net_flow_ratio", 0))
        net_flow_score = min(1.0, abs(net_flow) / 0.6)
        whale_components.append(net_flow_score * 0.08)
        
        # 3b. Accumulation pattern (6%)
        acc_ratio = _safe_float(latest_whale.get("accumulation_ratio", 1.0), 1.0)
        if acc_ratio > 1.0:
            acc_score = min(1.0, (acc_ratio - 1.0) / 3.0)  # 4x = strong
        else:
            acc_score = min(1.0, (1.0 - acc_ratio) / 0.75)  # 0.25 = strong selling
        whale_components.append(acc_score * 0.06)
        
        # 3c. Stealth accumulation detection (4%)
        stealth_detected = micro_patterns.get("whale_stealth_accumulation", {}).get("detected", False)
        stealth_conf = _safe_float(micro_patterns.get("whale_stealth_accumulation", {}).get("confidence", 0))
        whale_components.append(stealth_conf * 0.04 if stealth_detected else 0)
        
        # 3d. Flow velocity (2%)
        flow_vel = _safe_float(velocity_metrics.get("flow_velocity", 0))
        flow_vel_score = min(1.0, abs(flow_vel) / 10000)  # 10k SOL/min = strong
        whale_components.append(flow_vel_score * 0.02)
    
    whale_total = sum(whale_components) if whale_components else 0
    result["whale_alignment_score"] = round(whale_total / 0.20, 3) if whale_components else 0
    
    # =========================================================================
    # COMPONENT 4: VOLATILITY REGIME (15% weight)
    # =========================================================================
    vol_components = []
    
    # 4a. Current volatility level (5%)
    volatility = _safe_float(latest_price.get("volatility_pct", 0))
    # Sweet spot: 0.1-0.3% volatility - high enough for moves, low enough for signal
    if 0.1 <= volatility <= 0.4:
        vol_level_score = 1.0
    elif volatility < 0.1:
        vol_level_score = volatility / 0.1  # Too quiet
    else:
        vol_level_score = max(0, 1.0 - (volatility - 0.4) / 0.6)  # Too noisy
    vol_components.append(vol_level_score * 0.05)
    
    # 4b. Volatility compression (5%)
    # Decreasing volatility often precedes breakouts
    vol_of_vol = _safe_float(velocity_metrics.get("volatility_of_volatility", 0))
    compression_score = max(0, 1.0 - vol_of_vol) if vol_of_vol < 1.0 else 0
    vol_components.append(compression_score * 0.05)
    
    # 4c. Spread tightness (5%)
    spread = _safe_float(latest_ob.get("spread_bps", 10), 10)
    spread_score = max(0, 1.0 - spread / 15)  # <15 bps = good
    vol_components.append(spread_score * 0.05)
    
    vol_total = sum(vol_components)
    result["volatility_regime_score"] = round(vol_total / 0.15, 3)
    
    # =========================================================================
    # COMPONENT 5: CROSS-ASSET ALIGNMENT (10% weight)
    # =========================================================================
    xa_components = []
    
    # 5a. BTC leading signal (4%)
    btc_lead = _safe_float(cross_asset_metrics.get("btc_leads_sol_lag1", 0))
    btc_lead_score = min(1.0, abs(btc_lead))
    xa_components.append(btc_lead_score * 0.04)
    
    # 5b. Momentum alignment (4%)
    momentum_align = _safe_float(cross_asset_metrics.get("cross_asset_momentum_align", 0))
    align_score = min(1.0, abs(momentum_align) / 0.3)
    xa_components.append(align_score * 0.04)
    
    # 5c. SOL outperformance (2%)
    divergence = _safe_float(cross_asset_metrics.get("btc_sol_divergence", 0))
    div_score = min(1.0, abs(divergence) / 0.3)
    xa_components.append(div_score * 0.02)
    
    xa_total = sum(xa_components)
    result["cross_asset_score"] = round(xa_total / 0.10, 3)
    
    # =========================================================================
    # AGGREGATE PROBABILITY
    # =========================================================================
    raw_probability = (
        order_flow_total +
        momentum_total +
        whale_total +
        vol_total +
        xa_total
    )
    
    # =========================================================================
    # DIRECTION DETERMINATION
    # =========================================================================
    direction_signals = []
    
    # Volume imbalance direction
    direction_signals.append(np.sign(vol_imbalance) * vol_imbalance_score)
    
    # Depth ratio direction
    if depth_ratio > 1.0:
        direction_signals.append(1 * depth_score)
    else:
        direction_signals.append(-1 * depth_score)
    
    # Buy/sell pressure direction
    direction_signals.append(np.sign(buy_pressure) * pressure_score)
    
    # Whale net flow direction
    if whale_rows:
        direction_signals.append(np.sign(net_flow) * net_flow_score)
    
    # Price momentum direction
    price_change = _safe_float(latest_price.get("price_change_1m", 0))
    direction_signals.append(np.sign(price_change) * price_vel_score)
    
    # Cross-asset alignment direction
    direction_signals.append(np.sign(momentum_align) * align_score)
    
    # Weighted direction
    direction = float(np.mean(direction_signals)) if direction_signals else 0
    result["micro_move_direction"] = round(np.clip(direction, -1, 1), 3)
    
    # =========================================================================
    # RISK ADJUSTMENTS
    # =========================================================================
    
    # False signal risk (reduces probability)
    false_signal_factors = []
    
    # Low volume = higher false signal risk
    total_volume = _safe_float(latest_tx.get("total_volume_usd", 0))
    if total_volume < 100000:  # $100k minimum
        false_signal_factors.append(0.3)
    
    # Wide spread = higher risk
    if spread > 12:
        false_signal_factors.append(0.2)
    
    # Conflicting signals = higher risk
    if len(set(np.sign(s) for s in direction_signals if s != 0)) > 1:
        false_signal_factors.append(0.15)
    
    # Low whale activity = lower confidence
    if not whale_rows or _safe_float(latest_whale.get("total_sol_moved", 0)) < 10000:
        false_signal_factors.append(0.1)
    
    false_signal_risk = min(0.8, sum(false_signal_factors))
    result["false_signal_risk"] = round(false_signal_risk, 3)
    
    # Adverse selection (from order flow toxicity)
    toxicity = calculate_order_flow_toxicity(transaction_rows, order_book_rows)
    result["adverse_selection_risk"] = round(toxicity, 3)
    
    # Slippage estimate (based on liquidity and spread)
    liquidity = _safe_float(latest_ob.get("total_liquidity", 0))
    base_slippage = spread / 2  # Half spread minimum
    liquidity_impact = 0 if liquidity > 50000 else (50000 - liquidity) / 50000 * 5
    result["slippage_estimate_bps"] = round(base_slippage + liquidity_impact, 2)
    
    # =========================================================================
    # FINAL PROBABILITY
    # =========================================================================
    # Apply risk deductions
    adjusted_probability = raw_probability * (1 - false_signal_risk * 0.5)
    
    # Boost if micro-patterns detected
    micro_pattern_boost = 0
    for pattern_name in ["volume_divergence", "order_book_squeeze", "microstructure_shift"]:
        pattern = micro_patterns.get(pattern_name, {})
        if pattern.get("detected"):
            micro_pattern_boost += _safe_float(pattern.get("confidence", 0)) * 0.05
    
    final_probability = min(0.95, adjusted_probability + micro_pattern_boost)
    result["micro_move_probability"] = round(final_probability, 3)
    
    # Confidence (how certain we are in the prediction)
    signal_strength = abs(direction)
    signal_consistency = 1 - false_signal_risk
    result["micro_move_confidence"] = round((signal_strength * 0.5 + signal_consistency * 0.5), 3)
    
    # Expected timeframe (minutes until move)
    # Faster if: high velocity, high volume, aligned signals
    base_timeframe = 5.0
    velocity_factor = min(1.0, abs(price_vel) / 0.003)  # Higher velocity = faster
    volume_factor = min(1.0, total_volume / 500000)  # Higher volume = faster
    result["micro_move_timeframe"] = round(base_timeframe * (1 - velocity_factor * 0.3 - volume_factor * 0.2), 1)
    
    return result


# =============================================================================
# ENHANCED SCORING SYSTEM FOR 0.5% CLIMBS
# =============================================================================

def calculate_breakout_probability(
    patterns: Dict[str, Any],
    order_book_rows: List[Dict[str, Any]],
    transaction_rows: List[Dict[str, Any]],
    whale_rows: List[Dict[str, Any]],
    price_rows: List[Dict[str, Any]],
    micro_patterns: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Multi-factor scoring optimized for 0.5% climb probability.
    
    This scoring system weighs micro-patterns more heavily than traditional
    chart patterns, as they are more predictive for small moves.
    
    Args:
        patterns: Traditional pattern detection results
        order_book_rows: Order book data
        transaction_rows: Transaction data
        whale_rows: Whale activity data
        price_rows: Price movement data
        micro_patterns: Results from micro-pattern detectors
    
    Returns:
        Dictionary with overall score and component breakdown
    """
    result = {
        "overall_score": 0.0,
        "component_scores": {},
        "risk_factors": [],
        "confidence_level": "low"
    }
    
    if not all([order_book_rows, transaction_rows, price_rows]):
        result["error"] = "insufficient_data"
        return result
    
    # Get latest data points
    latest_ob = order_book_rows[0] if order_book_rows else {}
    latest_tx = transaction_rows[0] if transaction_rows else {}
    latest_whale = whale_rows[0] if whale_rows else {}
    latest_price = price_rows[0] if price_rows else {}

    def _to_float(value: Any, default: float = 0.0) -> float:
        """Convert numeric/Decimal values to float for scoring math."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    
    # === COMPONENT 1: MICRO-PATTERNS (35% weight) ===
    # These are the most predictive for 0.5% moves
    micro_score = 0.0
    
    # Volume divergence (10%)
    vol_div = micro_patterns.get("volume_divergence", {})
    if vol_div.get("detected"):
        micro_score += vol_div.get("confidence", 0) * 0.10
    
    # Order book squeeze (10%)
    ob_squeeze = micro_patterns.get("order_book_squeeze", {})
    if ob_squeeze.get("detected"):
        micro_score += ob_squeeze.get("confidence", 0) * 0.10
    
    # Whale stealth accumulation (8%)
    whale_stealth = micro_patterns.get("whale_stealth_accumulation", {})
    if whale_stealth.get("detected"):
        micro_score += whale_stealth.get("confidence", 0) * 0.08
    
    # Momentum acceleration (5%)
    momentum_accel = micro_patterns.get("momentum_acceleration", {})
    if momentum_accel.get("detected"):
        micro_score += momentum_accel.get("confidence", 0) * 0.05
    
    # Microstructure shift (2% bonus - already factored into other components)
    micro_shift = micro_patterns.get("microstructure_shift", {})
    if micro_shift.get("detected") and micro_shift.get("all_aligned"):
        micro_score += micro_shift.get("confidence", 0) * 0.02
    
    micro_score = min(0.35, micro_score)  # Cap at 35%
    result["component_scores"]["micro_patterns"] = round(micro_score, 4)
    
    # === COMPONENT 2: ORDER BOOK PRESSURE (25% weight) ===
    ob_score = 0.0
    
    # Depth imbalance (8%)
    depth_ratio = _to_float(latest_ob.get("depth_imbalance_ratio", 1.0), 1.0)
    if depth_ratio > 1.0:
        depth_score = min(1.0, (depth_ratio - 1.0) / 0.15) * 0.08  # 1.15 ratio = max
        ob_score += depth_score
    
    # Spread tightness (6%)
    spread_bps = _to_float(latest_ob.get("spread_bps", 10), 10.0)
    spread_score = max(0, (1.0 - min(1.0, spread_bps / 10))) * 0.06
    ob_score += spread_score
    
    # Volume imbalance (6%)
    vol_imbalance = _to_float(latest_ob.get("volume_imbalance", 0), 0.0)
    if vol_imbalance > 0:
        vol_imb_score = min(1.0, vol_imbalance / 0.3) * 0.06
        ob_score += vol_imb_score
    
    # Microprice deviation (3%)
    microprice_dev = _to_float(latest_ob.get("microprice_deviation", 0), 0.0)
    if microprice_dev > 0:
        micro_dev_score = min(1.0, microprice_dev / 5.0) * 0.03
        ob_score += micro_dev_score
    
    # Aggression ratio (2%)
    aggression = _to_float(latest_ob.get("aggression_ratio", 1.0), 1.0)
    if aggression > 1.0:
        agg_score = min(1.0, (aggression - 1.0) / 0.3) * 0.02
        ob_score += agg_score
    
    ob_score = min(0.25, ob_score)
    result["component_scores"]["order_book"] = round(ob_score, 4)
    
    # === COMPONENT 3: TRANSACTION FLOW (25% weight) ===
    tx_score = 0.0
    
    # Buy/sell pressure (10%)
    buy_pressure = _to_float(latest_tx.get("buy_sell_pressure", 0), 0.0)
    if buy_pressure > 0:
        pressure_score = min(1.0, buy_pressure / 0.3) * 0.10
        tx_score += pressure_score
    
    # Volume acceleration (7%)
    vol_accel = _to_float(latest_tx.get("volume_acceleration_ratio", 1.0), 1.0)
    if vol_accel > 1.0:
        accel_score = min(1.0, (vol_accel - 1.0) / 0.5) * 0.07  # 1.5x = max
        tx_score += accel_score
    
    # Whale participation (5%)
    whale_vol_pct = _to_float(latest_tx.get("whale_volume_pct", 0), 0.0)
    whale_part_score = min(1.0, whale_vol_pct / 40) * 0.05  # 40% = max
    tx_score += whale_part_score
    
    # Pressure shift (3%)
    pressure_shift = _to_float(latest_tx.get("pressure_shift_1m", 0), 0.0)
    if pressure_shift > 0:
        shift_score = min(1.0, pressure_shift / 0.2) * 0.03
        tx_score += shift_score
    
    tx_score = min(0.25, tx_score)
    result["component_scores"]["transactions"] = round(tx_score, 4)
    
    # === COMPONENT 4: WHALE ALIGNMENT (10% weight) ===
    whale_score = 0.0
    
    if whale_rows:
        # Net flow ratio (5%)
        net_flow = _to_float(latest_whale.get("net_flow_ratio", 0), 0.0)
        if net_flow > 0:
            flow_score = min(1.0, net_flow / 0.5) * 0.05
            whale_score += flow_score
        
        # Strong accumulation (3%)
        strong_acc_pct = _to_float(latest_whale.get("strong_accumulation_pct", 0), 0.0)
        acc_score = min(1.0, strong_acc_pct / 25) * 0.03  # 25% = max
        whale_score += acc_score
        
        # Accumulation ratio (2%)
        acc_ratio = _to_float(latest_whale.get("accumulation_ratio", 1.0), 1.0)
        if acc_ratio > 1.0:
            ratio_score = min(1.0, (acc_ratio - 1.0) / 2.0) * 0.02  # 3.0 ratio = max
            whale_score += ratio_score
    
    whale_score = min(0.10, whale_score)
    result["component_scores"]["whale"] = round(whale_score, 4)
    
    # === COMPONENT 5: MOMENTUM (5% weight) ===
    momentum_score = 0.0
    
    # Price momentum (3%)
    price_change_1m = _to_float(latest_price.get("price_change_1m", 0), 0.0)
    if price_change_1m > 0:
        price_mom_score = min(1.0, price_change_1m / 0.3) * 0.03  # 0.3% = max
        momentum_score += price_mom_score
    
    # Momentum acceleration (2%)
    mom_accel = _to_float(latest_price.get("momentum_acceleration_1m", 0), 0.0)
    if mom_accel > 0:
        accel_score = min(1.0, mom_accel / 0.2) * 0.02
        momentum_score += accel_score
    
    momentum_score = min(0.05, momentum_score)
    result["component_scores"]["momentum"] = round(momentum_score, 4)
    
    # === TRADITIONAL PATTERNS (Reduced to 5% for micro-moves) ===
    pattern_score = patterns.get("breakout_score", 0.0) * 0.05
    result["component_scores"]["traditional_patterns"] = round(pattern_score, 4)
    
    # === CALCULATE OVERALL SCORE ===
    overall = micro_score + ob_score + tx_score + whale_score + momentum_score + pattern_score
    overall = min(1.0, max(0.0, overall))
    
    # === RISK FACTORS (reduce score) ===
    risk_deductions = 0.0
    
    # High volatility (indicates noise, not signal)
    volatility = _to_float(latest_price.get("volatility_pct", 0), 0.0)
    if volatility > 1.0:
        risk_deductions += 0.05
        result["risk_factors"].append(f"high_volatility_{volatility:.2f}%")
    
    # Spread widening (liquidity issues)
    spread = _to_float(latest_ob.get("spread_bps", 0), 0.0)
    if spread > 15:
        risk_deductions += 0.03
        result["risk_factors"].append(f"wide_spread_{spread:.2f}_bps")
    
    # Low volume (signal unreliable)
    volume = _to_float(latest_tx.get("total_volume_usd", 0), 0.0)
    if volume < 50000:  # Less than $50k/min
        risk_deductions += 0.04
        result["risk_factors"].append(f"low_volume_${volume:.0f}")
    
    # Whale distribution (selling pressure)
    if whale_rows:
        dist_pct = _to_float(latest_whale.get("distribution_pressure_pct", 0), 0.0)
        if dist_pct > 15:
            risk_deductions += 0.06
            result["risk_factors"].append(f"whale_distribution_{dist_pct:.2f}%")
    
    # Apply risk deductions
    overall = max(0.0, overall - risk_deductions)
    result["overall_score"] = round(overall, 4)
    result["risk_deduction"] = round(risk_deductions, 4)
    
    # === CONFIDENCE LEVEL ===
    if overall >= 0.75:
        result["confidence_level"] = "very_high"
    elif overall >= 0.60:
        result["confidence_level"] = "high"
    elif overall >= 0.45:
        result["confidence_level"] = "medium"
    elif overall >= 0.30:
        result["confidence_level"] = "low"
    else:
        result["confidence_level"] = "very_low"
    
    # === INTERPRETATION ===
    if overall >= 0.60:
        result["interpretation"] = "Strong probability of 0.5%+ climb in next 1-3 minutes"
        result["action_recommendation"] = "consider_entry"
    elif overall >= 0.45:
        result["interpretation"] = "Moderate probability of 0.5%+ climb - monitor closely"
        result["action_recommendation"] = "watch"
    elif overall >= 0.30:
        result["interpretation"] = "Low probability - mixed signals"
        result["action_recommendation"] = "wait"
    else:
        result["interpretation"] = "Very low probability - avoid entry"
        result["action_recommendation"] = "avoid"
    
    return result


# =============================================================================
# TRADITIONAL PATTERN DETECTION (Legacy - Reduced Weight for Micro-Moves)
# =============================================================================

def find_swings(df: pd.DataFrame, lookback: int = 10, lookforward: int = 10) -> pd.DataFrame:
    """Identify local swing highs and lows in price series."""
    if df.empty or "price" not in df.columns:
        df = df.copy()
        df["swing_high"] = False
        df["swing_low"] = False
        return df
    
    prices = df["price"].values
    n = len(prices)
    swing_high = np.zeros(n, dtype=bool)
    swing_low = np.zeros(n, dtype=bool)
    
    for i in range(lookback, n - lookforward):
        window = prices[i - lookback : i + lookforward + 1]
        p = prices[i]
        if p == window.max():
            swing_high[i] = True
        if p == window.min():
            swing_low[i] = True
    
    df = df.copy()
    df["swing_high"] = swing_high
    df["swing_low"] = swing_low
    return df


def detect_ascending_triangle(df: pd.DataFrame, min_touches: int = 2, high_tolerance_pct: float = 0.15) -> Dict[str, Any]:
    """Detect ascending triangle pattern."""
    result = {"detected": False, "confidence": 0.0}
    
    if df.empty or "swing_high" not in df.columns:
        return result
    
    swings_high = df[df["swing_high"]]
    swings_low = df[df["swing_low"]]
    
    if len(swings_high) < min_touches or len(swings_low) < min_touches:
        return result
    
    resistance = swings_high["price"].median()
    high_dev = np.abs(swings_high["price"] / resistance - 1.0) * 100
    avg_high_dev = high_dev.mean()
    
    if avg_high_dev > high_tolerance_pct:
        return result
    
    lows = swings_low["price"].values
    low_indices = np.arange(len(lows))
    
    if len(lows) >= 2:
        slope, intercept = np.polyfit(low_indices, lows, 1)
    else:
        return result
    
    if slope <= 0:
        return result
    
    first_half = df.iloc[:len(df)//2]["price"]
    second_half = df.iloc[len(df)//2:]["price"]
    
    first_range = first_half.max() - first_half.min()
    second_range = second_half.max() - second_half.min()
    
    compression_ratio = second_range / max(first_range, 0.0001)
    
    resistance_quality = max(0, 1.0 - (avg_high_dev / high_tolerance_pct))
    slope_score = min(1.0, slope / lows.mean() * 1000)
    compression_score = max(0, 1.0 - compression_ratio)
    
    confidence = (resistance_quality * 0.4 + slope_score * 0.35 + compression_score * 0.25)
    confidence = min(1.0, max(0.0, confidence))
    
    support = intercept + slope * (len(lows) - 1)
    
    return {
        "detected": True,
        "confidence": round(confidence, 3),
        "resistance_level": round(float(resistance), 4),
        "support_level": round(float(support), 4),
        "support_slope": round(float(slope), 6),
        "touches_resistance": len(swings_high),
        "touches_support": len(swings_low),
        "compression_ratio": round(float(compression_ratio), 3),
    }


def detect_bullish_flag(df: pd.DataFrame, pole_pct_threshold: float = 0.3) -> Dict[str, Any]:
    """Detect bullish flag pattern."""
    result = {"detected": False, "confidence": 0.0}
    
    if df.empty or len(df) < 60:
        return result
    
    prices = df["price"].values
    n = len(prices)
    
    pole_end_idx = int(n * 0.4)
    flag_start_idx = pole_end_idx
    
    pole_prices = prices[:pole_end_idx]
    flag_prices = prices[flag_start_idx:]
    
    if len(pole_prices) < 10 or len(flag_prices) < 10:
        return result
    
    pole_low = pole_prices.min()
    pole_high = pole_prices.max()
    pole_height = pole_high - pole_low
    pole_height_pct = (pole_height / pole_low) * 100 if pole_low > 0 else 0
    
    if pole_height_pct < pole_pct_threshold:
        return result
    
    pole_low_idx = np.argmin(pole_prices)
    pole_high_idx = np.argmax(pole_prices)
    
    if pole_high_idx <= pole_low_idx:
        return result
    
    flag_indices = np.arange(len(flag_prices))
    if len(flag_prices) >= 2:
        flag_slope, flag_intercept = np.polyfit(flag_indices, flag_prices, 1)
    else:
        return result
    
    flag_slope_normalized = flag_slope / np.mean(flag_prices) * 100
    
    if flag_slope_normalized > 0.05:
        return result
    
    flag_low = flag_prices.min()
    retracement = (pole_high - flag_low) / pole_height if pole_height > 0 else 1
    
    if retracement > 0.5:
        return result
    
    pole_strength = min(1.0, pole_height_pct / 1.0)
    retracement_quality = 1.0 - retracement
    flag_tightness = max(0, 1.0 - abs(flag_slope_normalized) / 0.1)
    
    confidence = (pole_strength * 0.4 + retracement_quality * 0.35 + flag_tightness * 0.25)
    confidence = min(1.0, max(0.0, confidence))
    
    return {
        "detected": True,
        "confidence": round(confidence, 3),
        "pole_height_pct": round(float(pole_height_pct), 4),
        "pole_low": round(float(pole_low), 4),
        "pole_high": round(float(pole_high), 4),
        "retracement_pct": round(float(retracement * 100), 2),
    }


def detect_bullish_pennant(df: pd.DataFrame, pole_pct_threshold: float = 0.3) -> Dict[str, Any]:
    """Detect bullish pennant pattern."""
    result = {"detected": False, "confidence": 0.0}
    
    if df.empty or "swing_high" not in df.columns or len(df) < 60:
        return result
    
    prices = df["price"].values
    n = len(prices)
    
    pole_end_idx = int(n * 0.35)
    pennant_start_idx = pole_end_idx
    
    pole_prices = prices[:pole_end_idx]
    pennant_df = df.iloc[pennant_start_idx:]
    
    if len(pole_prices) < 10 or len(pennant_df) < 20:
        return result
    
    pole_low = pole_prices.min()
    pole_high = pole_prices.max()
    pole_height = pole_high - pole_low
    pole_height_pct = (pole_height / pole_low) * 100 if pole_low > 0 else 0
    
    if pole_height_pct < pole_pct_threshold:
        return result
    
    pole_low_idx = np.argmin(pole_prices)
    pole_high_idx = np.argmax(pole_prices)
    
    if pole_high_idx <= pole_low_idx:
        return result
    
    pennant_highs = pennant_df[pennant_df["swing_high"]]
    pennant_lows = pennant_df[pennant_df["swing_low"]]
    
    if len(pennant_highs) < 2 or len(pennant_lows) < 2:
        return result
    
    high_values = pennant_highs["price"].values
    low_values = pennant_lows["price"].values
    high_indices = np.arange(len(high_values))
    low_indices = np.arange(len(low_values))
    
    high_slope, high_intercept = np.polyfit(high_indices, high_values, 1)
    low_slope, low_intercept = np.polyfit(low_indices, low_values, 1)
    
    avg_price = np.mean(prices)
    high_slope_norm = high_slope / avg_price * 100
    low_slope_norm = low_slope / avg_price * 100
    
    if high_slope_norm > 0 or low_slope_norm < 0:
        return result
    
    first_half_pennant = pennant_df.iloc[:len(pennant_df)//2]["price"]
    second_half_pennant = pennant_df.iloc[len(pennant_df)//2:]["price"]
    
    first_range = first_half_pennant.max() - first_half_pennant.min()
    second_range = second_half_pennant.max() - second_half_pennant.min()
    
    compression_ratio = second_range / max(first_range, 0.0001)
    
    if compression_ratio > 0.9:
        return result
    
    pole_strength = min(1.0, pole_height_pct / 1.0)
    convergence_quality = min(1.0, abs(high_slope_norm) + abs(low_slope_norm)) / 0.5
    compression_quality = 1.0 - compression_ratio
    
    confidence = (pole_strength * 0.35 + convergence_quality * 0.35 + compression_quality * 0.3)
    confidence = min(1.0, max(0.0, confidence))
    
    return {
        "detected": True,
        "confidence": round(confidence, 3),
        "pole_height_pct": round(float(pole_height_pct), 4),
        "compression_ratio": round(float(compression_ratio), 3),
    }


def detect_falling_wedge(df: pd.DataFrame, min_touches: int = 2) -> Dict[str, Any]:
    """Detect falling wedge pattern (bullish reversal)."""
    result = {"detected": False, "confidence": 0.0}
    
    if df.empty or "swing_high" not in df.columns:
        return result
    
    swings_high = df[df["swing_high"]]
    swings_low = df[df["swing_low"]]
    
    if len(swings_high) < min_touches or len(swings_low) < min_touches:
        return result
    
    high_values = swings_high["price"].values
    low_values = swings_low["price"].values
    high_indices = np.arange(len(high_values))
    low_indices = np.arange(len(low_values))
    
    high_slope, high_intercept = np.polyfit(high_indices, high_values, 1)
    low_slope, low_intercept = np.polyfit(low_indices, low_values, 1)
    
    avg_price = df["price"].mean()
    high_slope_norm = high_slope / avg_price * 100
    low_slope_norm = low_slope / avg_price * 100
    
    if high_slope_norm >= 0 or low_slope_norm >= 0:
        return result
    
    if low_slope_norm <= high_slope_norm:
        return result
    
    first_width = high_values[0] - low_values[0] if len(high_values) > 0 and len(low_values) > 0 else 0
    last_width = high_values[-1] - low_values[-1] if len(high_values) > 0 and len(low_values) > 0 else 0
    
    if first_width <= 0 or last_width <= 0:
        return result
    
    wedge_contraction = last_width / first_width
    
    if wedge_contraction > 0.95:
        return result
    
    convergence_rate = (low_slope_norm - high_slope_norm) / max(abs(high_slope_norm), 0.001)
    convergence_score = min(1.0, convergence_rate)
    contraction_score = 1.0 - wedge_contraction
    touch_score = min(1.0, (len(swings_high) + len(swings_low)) / 6)
    
    confidence = (convergence_score * 0.4 + contraction_score * 0.35 + touch_score * 0.25)
    confidence = min(1.0, max(0.0, confidence))
    
    return {
        "detected": True,
        "confidence": round(confidence, 3),
        "wedge_contraction": round(float(wedge_contraction), 3),
    }


def detect_all_patterns(df: pd.DataFrame, swing_lookback: int = 10, swing_lookforward: int = 10) -> Dict[str, Any]:
    """Run all bullish pattern detectors and compute aggregate scores."""
    result: Dict[str, Any] = {
        "detected": [],
        "breakout_score": 0.0,
        "swing_structure": {},
        "error": None
    }
    
    if df.empty or "price" not in df.columns:
        result["error"] = "no_price_data"
        return result
    
    if len(df) < 30:
        result["error"] = "insufficient_data"
        return result
    
    df_with_swings = find_swings(df, lookback=swing_lookback, lookforward=swing_lookforward)
    
    pattern_weights = {
        "ascending_triangle": 0.20,
        "bullish_flag": 0.20,
        "bullish_pennant": 0.18,
        "falling_wedge": 0.15,
    }
    
    patterns = {}
    
    asc_tri = detect_ascending_triangle(df_with_swings)
    patterns["ascending_triangle"] = asc_tri
    if asc_tri.get("detected"):
        result["detected"].append("ascending_triangle")
    
    bull_flag = detect_bullish_flag(df_with_swings)
    patterns["bullish_flag"] = bull_flag
    if bull_flag.get("detected"):
        result["detected"].append("bullish_flag")
    
    bull_pennant = detect_bullish_pennant(df_with_swings)
    patterns["bullish_pennant"] = bull_pennant
    if bull_pennant.get("detected"):
        result["detected"].append("bullish_pennant")
    
    fall_wedge = detect_falling_wedge(df_with_swings)
    patterns["falling_wedge"] = fall_wedge
    if fall_wedge.get("detected"):
        result["detected"].append("falling_wedge")
    
    result.update(patterns)
    
    weighted_sum = 0.0
    total_weight = 0.0
    
    for pattern_name, weight in pattern_weights.items():
        pattern_result = patterns.get(pattern_name, {})
        if pattern_result.get("detected"):
            confidence = pattern_result.get("confidence", 0.0)
            weighted_sum += confidence * weight
            total_weight += weight
    
    if total_weight > 0:
        result["breakout_score"] = round(weighted_sum / total_weight, 3)
    else:
        result["breakout_score"] = 0.0
    
    swings_high = df_with_swings[df_with_swings["swing_high"]]
    swings_low = df_with_swings[df_with_swings["swing_low"]]
    
    swing_structure = {
        "total_swing_highs": len(swings_high),
        "total_swing_lows": len(swings_low)
    }
    
    if len(swings_low) >= 2:
        lows = swings_low["price"].values
        swing_structure["higher_lows"] = bool(lows[-1] > lows[0])
    
    if len(swings_high) >= 2:
        highs = swings_high["price"].values
        swing_structure["lower_highs"] = bool(highs[-1] < highs[0])
    
    if swing_structure.get("higher_lows") and not swing_structure.get("lower_highs", True):
        swing_structure["trend"] = "bullish"
    elif swing_structure.get("lower_highs") and not swing_structure.get("higher_lows", True):
        swing_structure["trend"] = "bearish"
    else:
        swing_structure["trend"] = "neutral"
    
    swing_structure["price_range"] = {
        "high": round(float(df["price"].max()), 4),
        "low": round(float(df["price"].min()), 4),
        "current": round(float(df["price"].iloc[-1]), 4),
    }
    
    result["swing_structure"] = swing_structure
    
    if result["error"] is None:
        del result["error"]
    
    return result


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def annotate_minute_spans(rows: List[Dict[str, Any]], window_end: datetime) -> None:
    """Augment each row with interval bounds relative to window_end."""
    for row in rows:
        minute_timestamp = row.get("minute_timestamp")
        minute_dt = None
        
        if isinstance(minute_timestamp, datetime):
            minute_dt = minute_timestamp
        elif isinstance(minute_timestamp, str):
            # Try multiple date formats
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
                try:
                    minute_dt = datetime.strptime(minute_timestamp, fmt)
                    break
                except ValueError:
                    continue
            # Also try ISO format parsing
            if minute_dt is None:
                try:
                    minute_dt = datetime.fromisoformat(minute_timestamp.replace('Z', '+00:00').split('+')[0])
                except (ValueError, AttributeError):
                    continue
        
        if minute_dt is None:
            continue

        delta_minutes = (window_end - minute_dt).total_seconds() / 60.0
        span_from = max(0, math.floor(delta_minutes))
        span_to = span_from + 1
        row["minute_span_from"] = span_from
        row["minute_span_to"] = span_to


def build_minute_span_view(
    order_rows: List[Dict[str, Any]],
    transaction_rows: List[Dict[str, Any]],
    whale_rows: Optional[List[Dict[str, Any]]] = None,
    price_rows: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Create combined records grouped by minute span."""
    spans: Dict[int, Dict[str, Any]] = {}

    def add_to_span(row: Dict[str, Any], key: str) -> None:
        span_from = row.get("minute_span_from")
        span_to = row.get("minute_span_to")
        if span_from is None or span_to is None:
            return
        bucket = spans.setdefault(
            int(span_from),
            {"minute_span_from": span_from, "minute_span_to": span_to},
        )
        bucket[key] = row

    for record in order_rows:
        add_to_span(record, "order_book")

    for record in transaction_rows:
        add_to_span(record, "transactions")

    if whale_rows:
        for record in whale_rows:
            add_to_span(record, "whale_activity")

    if price_rows:
        for record in price_rows:
            add_to_span(record, "price_movements")

    return [spans[key] for key in sorted(spans.keys())]


def make_json_serializable(obj: Any) -> Any:
    """Recursively convert values so they can be JSON-encoded."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def persist_trail(buyin_id: int, payload: Dict[str, Any]) -> bool:
    """Persist the generated trail data to the buyin_trail_minutes table.
    
    This is the primary persistence method - stores one row per minute (15 rows total)
    in the buyin_trail_minutes table for efficient querying.
    
    Args:
        buyin_id: The ID of the buyin record
        payload: The trail payload from generate_trail_payload()
        
    Returns:
        True if persistence succeeded, False otherwise
    """
    # Insert into buyin_trail_minutes table (primary storage)
    success = insert_trail_data(buyin_id, payload)
    
    if success:
        logger.info(f"✓ Trail data persisted to table for buyin_id={buyin_id}")
    else:
        logger.warning(f"✗ Trail data persistence failed for buyin_id={buyin_id}")
    
    return success


def persist_trail_json_legacy(buyin_id: int, payload: Dict[str, Any]) -> None:
    """(Deprecated) Persist the generated trail JSON into the buy-in row.
    
    This is the legacy JSON persistence method. Use persist_trail() instead
    for the new table-based storage.
    """
    serializable_payload = make_json_serializable({
        key: value
        for key, value in payload.items()
        if key not in {"existing_trail", "persisted"}
    })
    
    trail_json = json.dumps(serializable_payload, ensure_ascii=True)
    
    # Write to PostgreSQL only
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE follow_the_goat_buyins
                    SET fifteen_min_trail = %s
                    WHERE id = %s
                """, [trail_json, buyin_id])
            conn.commit()
    except Exception as e:
        logger.warning(f"PostgreSQL trail persist failed: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def generate_trail_payload(
    buyin_id: int,
    symbol: Optional[str] = None,
    lookback_minutes: Optional[int] = None,
    persist: bool = True,
    include_30_second: bool = True,
) -> Dict[str, Any]:
    """Generate the 15-minute trail payload for a buy-in with enhanced micro-pattern detection.

    NEW: Now includes:
    - 30-second interval data
    - Velocity/acceleration metrics
    - Cross-asset lead-lag analysis
    - Composite micro-move probability scoring

    Args:
        buyin_id: Target identifier in follow_the_goat_buyins.
        symbol: Optional override for the order book symbol (default: SOLUSDT).
        lookback_minutes: Window size in minutes (default: 15).
        persist: If True (default), store data in buyin_trail_minutes table.
        include_30_second: If True (default), include 30-second interval data.

    Returns:
        JSON-serializable dictionary containing all analytics and pattern detection.
    """
    symbol_to_use = symbol or DEFAULT_SYMBOL
    minutes = lookback_minutes or DEFAULT_LOOKBACK_MINUTES
    if minutes <= 0:
        raise ValueError("lookback_minutes must be greater than zero")

    try:
        buyin = fetch_buyin(buyin_id)

        followed_at = buyin.get("followed_at")
        if not isinstance(followed_at, datetime):
            raise MissingFollowedAtError(
                f"Buy-in #{buyin_id} is missing a valid followed_at timestamp"
            )

        window_end = followed_at
        window_start = window_end - timedelta(minutes=minutes)

        logger.info(f"Generating trail for buyin_id={buyin_id}, window: {window_start} to {window_end}")

        # === FETCH ALL DATA SOURCES ===
        order_book_rows = fetch_order_book_signals(symbol_to_use, window_start, window_end)
        transaction_rows = fetch_transactions(window_start, window_end)
        whale_rows = fetch_whale_activity(window_start, window_end)
        
        # Fetch SOL price movements (primary)
        price_rows = fetch_price_movements(window_start, window_end, token="SOL", coin_id=5)
        
        # Fetch BTC and ETH price movements for cross-market analysis
        btc_price_rows = fetch_price_movements(window_start, window_end, token="BTC", coin_id=6)
        eth_price_rows = fetch_price_movements(window_start, window_end, token="ETH", coin_id=7)
        
        second_prices = fetch_second_prices(window_start, window_end)

        # === ADD FIELD TYPE METADATA ===
        order_book_rows = annotate_field_types(order_book_rows, "order_book_signals")
        transaction_rows = annotate_field_types(transaction_rows, "transactions")
        whale_rows = annotate_field_types(whale_rows, "whale_activity")
        price_rows = annotate_field_types(price_rows, "price_movements")
        btc_price_rows = annotate_field_types(btc_price_rows, "price_movements")
        eth_price_rows = annotate_field_types(eth_price_rows, "price_movements")

        # === RUN PATTERN DETECTION ===
        # Traditional patterns (legacy - reduced weight)
        if not second_prices.empty:
            traditional_patterns = detect_all_patterns(second_prices)
        else:
            traditional_patterns = {
                "detected": [],
                "breakout_score": 0.0,
                "swing_structure": {},
                "error": "no_second_price_data"
            }

        # === RUN MICRO-PATTERN DETECTION (Optimized for 0.5% moves) ===
        micro_patterns = {}
        
        # Volume divergence
        vol_div = detect_volume_divergence(transaction_rows, price_rows)
        micro_patterns["volume_divergence"] = vol_div
        if vol_div.get("detected"):
            logger.info(f"✓ Volume divergence detected (confidence: {vol_div.get('confidence'):.2f})")
        
        # Order book squeeze
        ob_squeeze = detect_order_book_squeeze(order_book_rows)
        micro_patterns["order_book_squeeze"] = ob_squeeze
        if ob_squeeze.get("detected"):
            logger.info(f"✓ Order book squeeze detected (confidence: {ob_squeeze.get('confidence'):.2f})")
        
        # Whale stealth accumulation
        whale_stealth = detect_whale_stealth_accumulation(whale_rows, price_rows)
        micro_patterns["whale_stealth_accumulation"] = whale_stealth
        if whale_stealth.get("detected"):
            logger.info(f"✓ Whale stealth accumulation detected (confidence: {whale_stealth.get('confidence'):.2f})")
        
        # Momentum acceleration
        momentum_accel = detect_momentum_acceleration(price_rows)
        micro_patterns["momentum_acceleration"] = momentum_accel
        if momentum_accel.get("detected"):
            logger.info(f"✓ Momentum acceleration detected (confidence: {momentum_accel.get('confidence'):.2f})")
        
        # Microstructure shift (composite signal)
        micro_shift = detect_microstructure_shift(order_book_rows, transaction_rows, whale_rows)
        micro_patterns["microstructure_shift"] = micro_shift
        if micro_shift.get("detected"):
            logger.info(f"✓ Microstructure shift detected (confidence: {micro_shift.get('confidence'):.2f})")

        # === NEW: FETCH 30-SECOND DATA ===
        thirty_second_rows = []
        thirty_second_metrics = {}
        if include_30_second:
            thirty_second_rows = fetch_30_second_data(window_start, window_end, "SOL")
            thirty_second_metrics = calculate_30_second_metrics(
                thirty_second_rows, transaction_rows, order_book_rows
            )
            if thirty_second_metrics:
                logger.debug(f"30-second data: {len(thirty_second_rows)} rows")

        # === NEW: CALCULATE VELOCITY METRICS ===
        ob_velocity_metrics = calculate_order_book_velocities(order_book_rows)
        tx_velocity_metrics = calculate_transaction_velocities(transaction_rows)
        whale_velocity_metrics = calculate_whale_velocities(whale_rows)
        price_velocity_metrics = calculate_price_velocities(price_rows)
        
        # Combine velocity metrics into single dict
        velocity_metrics = {
            **ob_velocity_metrics,
            **tx_velocity_metrics,
            **whale_velocity_metrics,
            **price_velocity_metrics,
        }
        
        # Add VPIN and toxicity estimates
        velocity_metrics["vpin_estimate"] = calculate_vpin_estimate(transaction_rows)
        velocity_metrics["order_flow_toxicity"] = calculate_order_flow_toxicity(transaction_rows, order_book_rows)
        
        logger.debug(f"Velocity metrics calculated: {len(velocity_metrics)} fields")

        # === NEW: CALCULATE CROSS-ASSET METRICS ===
        cross_asset_metrics = calculate_cross_asset_metrics(
            price_rows, btc_price_rows, eth_price_rows
        )
        if cross_asset_metrics.get("btc_leads_sol_lag1", 0) > 0.5:
            logger.info(f"✓ Strong BTC lead-lag correlation: {cross_asset_metrics['btc_leads_sol_lag1']:.2f}")

        # === NEW: CALCULATE COMPOSITE MICRO-MOVE SCORE ===
        micro_move_score = calculate_micro_move_composite_score(
            order_book_rows=order_book_rows,
            transaction_rows=transaction_rows,
            whale_rows=whale_rows,
            price_rows=price_rows,
            cross_asset_metrics=cross_asset_metrics,
            velocity_metrics=velocity_metrics,
            micro_patterns=micro_patterns,
            target_move_pct=0.5
        )
        
        logger.info(
            f"Micro-move probability: {micro_move_score.get('micro_move_probability', 0):.2f} "
            f"direction: {micro_move_score.get('micro_move_direction', 0):.2f} "
            f"confidence: {micro_move_score.get('micro_move_confidence', 0):.2f}"
        )

        # === CALCULATE OVERALL BREAKOUT PROBABILITY ===
        breakout_analysis = calculate_breakout_probability(
            traditional_patterns,
            order_book_rows,
            transaction_rows,
            whale_rows,
            price_rows,
            micro_patterns
        )
        
        logger.info(
            f"Overall breakout probability: {breakout_analysis.get('overall_score'):.2f} "
            f"({breakout_analysis.get('confidence_level')})"
        )

        # Annotate rows with minute spans
        annotate_minute_spans(order_book_rows, window_end)
        annotate_minute_spans(transaction_rows, window_end)
        annotate_minute_spans(whale_rows, window_end)
        annotate_minute_spans(price_rows, window_end)
        annotate_minute_spans(btc_price_rows, window_end)
        annotate_minute_spans(eth_price_rows, window_end)

        # === BUILD PAYLOAD ===
        payload: Dict[str, Any] = {
            "buyin_id": buyin["id"],
            "symbol": symbol_to_use,
            "followed_at": window_end,
            "window": {
                "start": window_start,
                "end": window_end,
                "minutes": minutes,
            },
            # Existing data sections
            "order_book_signals": order_book_rows,
            "transactions": transaction_rows,
            "whale_activity": whale_rows,
            "price_movements": price_rows,
            "btc_price_movements": btc_price_rows,
            "eth_price_movements": eth_price_rows,
            "traditional_patterns": traditional_patterns,
            "micro_patterns": micro_patterns,
            "breakout_analysis": breakout_analysis,
            "second_prices": (
                second_prices.reset_index().to_dict('records')
                if not second_prices.empty
                else []
            ),
            "minute_spans": build_minute_span_view(
                order_book_rows, transaction_rows, whale_rows, price_rows
            ),
            # NEW: Enhanced metrics for micro-movement detection
            "thirty_second_data": thirty_second_rows,
            "thirty_second_metrics": thirty_second_metrics,
            "velocity_metrics": velocity_metrics,
            "cross_asset_metrics": cross_asset_metrics,
            "micro_move_score": micro_move_score,  # PRIMARY SIGNAL
        }

        if buyin.get("existing_trail") is not None:
            payload["existing_trail"] = buyin["existing_trail"]

        if persist:
            success = persist_trail(buyin_id, payload)
            payload["persisted"] = success

        return make_json_serializable(payload)

    except TrailError:
        raise
    except Exception as e:
        logger.error("Trail generation error: %s", e, exc_info=True)
        raise TrailError(f"Failed to generate trail: {e}") from e


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate a 15-minute trail payload with micro-pattern detection")
    parser.add_argument("buyin_id", type=int, help="follow_the_goat_buyins ID")
    parser.add_argument("--symbol", help="Override symbol (default: SOLUSDT)")
    parser.add_argument("--minutes", type=int, help="Lookback window in minutes (default: 15)")
    parser.add_argument("--persist", action="store_true", help="Persist the generated data")

    args = parser.parse_args()

    try:
        payload = generate_trail_payload(
            buyin_id=args.buyin_id,
            symbol=args.symbol,
            lookback_minutes=args.minutes,
            persist=args.persist,
        )
    except TrailError as exc:
        logger.error("%s", exc)
        raise SystemExit(1)

    print(json.dumps(payload, indent=2))
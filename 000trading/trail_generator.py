"""
15-Minute Trail Generator
=========================
Generate analytics trail data for buy-in signals using DuckDB.

This module fetches order book, transactions, whale activity, and price data
from DuckDB tables and computes derived metrics for pattern validation.

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
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Add project root to path
import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb
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
    """Execute a query using master2's DuckDB, HTTP API, TradingDataEngine, or file-based DuckDB.
    
    Priority:
    1. master2's _local_duckdb (MAIN database with all data - when running in master2 process)
    2. master2's HTTP API (port 5052 - when running standalone)
    3. TradingDataEngine (master.py's in-memory DB)
    4. File-based DuckDB (fallback - usually empty)
    
    Args:
        query: SQL query (use ? for placeholders - DuckDB format)
        params: Query parameters
        as_dict: If True, return list of dicts; if False, return list of tuples
        graceful: If True, return empty list on table-not-found errors instead of raising
    
    Returns:
        List of dicts (if as_dict=True) or list of tuples
    """
    # First, try to get master2's local DuckDB (the MAIN database)
    try:
        from scheduler.master2 import get_local_duckdb, _local_duckdb_lock
        local_db = get_local_duckdb()
        if local_db is not None:
            with _local_duckdb_lock:
                result = local_db.execute(query, params or [])
                if as_dict:
                    columns = [desc[0] for desc in result.description]
                    rows = result.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                return result.fetchall()
    except (ImportError, AttributeError):
        # master2 not available or not initialized yet
        pass
    except Exception as e:
        error_msg = str(e).lower()
        if graceful and ("does not exist" in error_msg or "no such table" in error_msg):
            logger.debug(f"Table not found in master2 DB (graceful mode): {e}")
            return []
        # If it's a real error, log it but continue to fallback
        logger.warning(f"Error querying master2 DB, will try HTTP API: {e}")
    
    # Second, try master2's HTTP API (for standalone execution)
    try:
        import requests
        # Substitute params into query (simple replacement for ? placeholders)
        formatted_query = query
        if params:
            for param in params:
                if isinstance(param, str):
                    formatted_query = formatted_query.replace('?', f"'{param}'", 1)
                elif hasattr(param, 'isoformat'):  # datetime objects
                    formatted_query = formatted_query.replace('?', f"'{param.isoformat()}'", 1)
                else:
                    formatted_query = formatted_query.replace('?', str(param), 1)
        
        resp = requests.post(
            "http://127.0.0.1:5052/query",
            json={"sql": formatted_query},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success"):
                results = data.get("results", [])
                if as_dict:
                    return results
                return [tuple(r.values()) for r in results] if results else []
    except Exception as e:
        logger.debug(f"master2 HTTP API query failed: {e}")
    
    # Third, try TradingDataEngine (master.py's in-memory DB)
    engine = _get_engine_if_running()
    try:
        if engine is not None:
            results = engine.read(query, params or [])
            if as_dict:
                return results
            return [tuple(r.values()) for r in results] if results else []
    except Exception as e:
        error_msg = str(e).lower()
        if graceful and ("does not exist" in error_msg or "no such table" in error_msg):
            logger.debug(f"Table not found in engine (graceful mode): {e}")
            return []
        logger.warning(f"Error querying engine, will try file DB: {e}")
    
    # Finally, fallback to file-based DuckDB
    try:
        with get_duckdb("central") as conn:
            result = conn.execute(query, params or [])
            if as_dict:
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            return result.fetchall()
    except Exception as e:
        error_msg = str(e).lower()
        if graceful and ("does not exist" in error_msg or "no such table" in error_msg):
            logger.debug(f"Table not found (graceful mode): {e}")
            return []
        raise
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "SOLUSDT")
DEFAULT_LOOKBACK_MINUTES = int(os.getenv("TRAIL_LOOKBACK_MINUTES", "15"))
TRAIL_COLUMN_NAME = "fifteen_min_trail"  # DuckDB uses this name (no numeric prefix)


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
    """Return buy-in metadata needed for the trail from DuckDB (fast).
    
    Uses DuckDB for speed - MySQL is only for storage.
    DuckDB uses 'fifteen_min_trail' column name.
    """
    result = None
    
    # PRIORITY 1: Try master2's local DuckDB
    try:
        from scheduler.master2 import get_local_duckdb, _local_duckdb_lock
        local_db = get_local_duckdb()
        if local_db is not None:
            with _local_duckdb_lock:
                res = local_db.execute("""
                    SELECT id, followed_at, fifteen_min_trail as existing_trail
                    FROM follow_the_goat_buyins
                    WHERE id = ?
                    LIMIT 1
                """, [buyin_id]).fetchone()
                if res:
                    result = res
    except (ImportError, AttributeError):
        pass
    except Exception as e:
        logger.debug(f"master2 DB fetch_buyin failed: {e}")
    
    # PRIORITY 2: Try HTTP API
    if result is None:
        try:
            import requests
            resp = requests.post(
                "http://127.0.0.1:5052/query",
                json={"sql": f"SELECT id, followed_at, fifteen_min_trail as existing_trail FROM follow_the_goat_buyins WHERE id = {buyin_id} LIMIT 1"},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success") and data.get("results"):
                    row_data = data["results"][0]
                    result = (row_data.get("id"), row_data.get("followed_at"), row_data.get("existing_trail"))
        except Exception as e:
            logger.debug(f"HTTP API fetch_buyin failed: {e}")
    
    # PRIORITY 3: Fallback to file-based DuckDB
    if result is None:
        try:
            with get_duckdb("central") as conn:
                result = conn.execute("""
                    SELECT id, followed_at, fifteen_min_trail as existing_trail
                    FROM follow_the_goat_buyins
                    WHERE id = ?
                    LIMIT 1
                """, [buyin_id]).fetchone()
        except Exception as e:
            raise TrailError(f"Failed to fetch buyin #{buyin_id}: {e}")
    
    if not result:
        raise BuyinNotFoundError(f"Buy-in #{buyin_id} not found")
    
    # DuckDB returns tuple, convert to dict
    row = {
        'id': result[0],
        'followed_at': result[1],
        'existing_trail': result[2]
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
    """Fetch order book signals from DuckDB/TradingDataEngine with computed metrics."""
    # DuckDB query for order book data with minute aggregation
    query = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', ts) AS minute_timestamp,
                symbol,
                LAST(mid_price ORDER BY ts) AS mid_price,
                LAST(best_bid ORDER BY ts) AS best_bid,
                LAST(best_ask ORDER BY ts) AS best_ask,
                LAST(microprice ORDER BY ts) AS microprice,
                AVG(volume_imbalance) AS volume_imbalance,
                AVG(relative_spread_bps) AS relative_spread_bps,
                AVG(microprice_dev_bps) AS microprice_dev_bps,
                AVG(bid_depth_10) AS bid_depth_10,
                AVG(ask_depth_10) AS ask_depth_10,
                AVG(total_depth_10) AS total_depth_10,
                AVG(bid_depth_bps_10) AS bid_depth_bps_10,
                AVG(ask_depth_bps_10) AS ask_depth_bps_10,
                AVG(bid_slope) AS bid_slope,
                AVG(ask_slope) AS ask_slope,
                AVG(bid_vwap_10) AS bid_vwap_10,
                AVG(ask_vwap_10) AS ask_vwap_10,
                SUM(COALESCE(net_liquidity_change_1s, 0)) AS net_liquidity_change_sum,
                COUNT(*) AS sample_count,
                MIN(ts) AS period_start,
                MAX(ts) AS period_end
            FROM order_book_features
            WHERE symbol = ?
                AND ts >= ?
                AND ts <= ?
            GROUP BY DATE_TRUNC('minute', ts), symbol
        ),
        numbered_minutes AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY minute_timestamp) AS row_num
            FROM minute_aggregates
        )
        SELECT 
            m0.minute_timestamp,
            m0.symbol,
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
            ON m0.symbol = m1.symbol 
            AND m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        LEFT JOIN numbered_minutes m2
            ON m0.symbol = m2.symbol 
            AND m0.row_num > 2
            AND m2.row_num = m0.row_num - 2
        LEFT JOIN numbered_minutes m3
            ON m0.symbol = m3.symbol 
            AND m0.row_num > 3
            AND m3.row_num = m0.row_num - 3
        LEFT JOIN numbered_minutes m5
            ON m0.symbol = m5.symbol 
            AND m0.row_num > 5
            AND m5.row_num = m0.row_num - 5
        LEFT JOIN numbered_minutes m10
            ON m0.symbol = m10.symbol 
            AND m0.row_num > 10
            AND m10.row_num = m0.row_num - 10
        WHERE m0.symbol = ?
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
        """
    return _execute_query(query, [symbol, start_time, end_time, symbol])


def fetch_transactions(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch transaction data from local DuckDB and compute per-minute metrics.
    
    Data source: master2's local DuckDB (sol_stablecoin_trades table)
    Priority: master2 local DB > TradingDataEngine > file-based DuckDB
    
    Note: sol_stablecoin_trades timestamps are in local time (UTC+1),
    while buyin followed_at is in UTC. We adjust the query time range.
    """
    # Adjust for timezone: trades are stored in local time (UTC+1)
    # Add 1 hour to convert UTC to local time
    tz_offset = timedelta(hours=1)
    local_start = start_time + tz_offset
    local_end = end_time + tz_offset
    
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
    raw_trades = _execute_query(query, [local_start, local_end])
    
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
    
    # Convert timestamps back to UTC (subtract the timezone offset we added earlier)
    for result in final_results:
        if isinstance(result.get('minute_timestamp'), datetime):
            result['minute_timestamp'] = result['minute_timestamp'] - tz_offset
        elif isinstance(result.get('minute_timestamp'), pd.Timestamp):
            result['minute_timestamp'] = (result['minute_timestamp'] - tz_offset).to_pydatetime()
    
    logger.info("Aggregated transactions into %d minutes of data", len(final_results))
    return final_results


def fetch_whale_activity(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch whale movement data from local DuckDB and compute per-minute metrics.
    
    Data source: master2's local DuckDB or TradingDataEngine (whale_movements table)
    Priority: master2 local DB > TradingDataEngine > file-based DuckDB
    
    Note: whale_movements timestamps are in local time (UTC+1),
    while buyin followed_at is in UTC. We adjust the query time range.
    """
    # Adjust for timezone: whale data is stored in local time (UTC+1)
    # Add 1 hour to convert UTC to local time
    tz_offset = timedelta(hours=1)
    local_start = start_time + tz_offset
    local_end = end_time + tz_offset
    
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
    raw_whales = _execute_query(query, [local_start, local_end])
    
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
    
    # Convert timestamps back to UTC (subtract the timezone offset we added earlier)
    for result in final_results:
        if isinstance(result.get('minute_timestamp'), datetime):
            result['minute_timestamp'] = result['minute_timestamp'] - tz_offset
        elif isinstance(result.get('minute_timestamp'), pd.Timestamp):
            result['minute_timestamp'] = (result['minute_timestamp'] - tz_offset).to_pydatetime()
    
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
    # Query for prices table (modern format: ts, token, price)
    query_prices = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', ts) AS minute_timestamp,
                MIN(price) AS low_price,
                MAX(price) AS high_price,
                AVG(price) AS avg_price,
                FIRST(price ORDER BY ts ASC) AS true_open,
                LAST(price ORDER BY ts ASC) AS true_close,
                MAX(price) - MIN(price) AS price_range,
                STDDEV(price) AS price_stddev,
                COUNT(*) AS price_updates
            FROM prices
            WHERE ts >= ?
                AND ts <= ?
                AND token = ?
            GROUP BY DATE_TRUNC('minute', ts)
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
    
    # PRIORITY 1: Try master2's local DuckDB (has synced prices table with SOL, BTC, ETH)
    try:
        from scheduler.master2 import get_local_duckdb, _local_duckdb_lock
        local_db = get_local_duckdb()
        if local_db is not None:
            with _local_duckdb_lock:
                result = local_db.execute(query_prices, [start_time, end_time, token])
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                if rows:
                    logger.debug(f"Got {len(rows)} price movements from master2 local DB for {token}")
                    return [dict(zip(columns, row)) for row in rows]
    except (ImportError, AttributeError):
        pass  # master2 not available
    except Exception as e:
        logger.debug(f"master2 DB query failed for {token}: {e}")
    
    # PRIORITY 2: Try master2's HTTP API (for standalone execution)
    try:
        import requests
        # Format the query with parameters
        formatted_query = query_prices
        for param in [start_time, end_time, token]:
            if isinstance(param, str):
                formatted_query = formatted_query.replace('?', f"'{param}'", 1)
            elif hasattr(param, 'isoformat'):  # datetime
                formatted_query = formatted_query.replace('?', f"'{param.isoformat()}'", 1)
            else:
                formatted_query = formatted_query.replace('?', str(param), 1)
        
        resp = requests.post(
            "http://127.0.0.1:5052/query",
            json={"sql": formatted_query},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success") and data.get("results"):
                logger.debug(f"Got {len(data['results'])} price movements from master2 HTTP API for {token}")
                return data["results"]
    except Exception as e:
        logger.debug(f"master2 HTTP API query failed for {token}: {e}")
    
    # PRIORITY 3: Try TradingDataEngine (master.py's in-memory DB)
    engine = _get_engine_if_running()
    if engine is not None:
        results = _execute_query(query_prices, [start_time, end_time, token])
        if results:
            return results
    
    # Fallback: file-based DuckDB price_points (fast)
    # DuckDB uses strftime() for date formatting
    query_duckdb = """
        WITH minute_aggregates AS (
            SELECT 
                strftime(created_at, '%Y-%m-%d %H:%M:00') AS minute_timestamp,
                MIN(value) AS low_price,
                MAX(value) AS high_price,
                AVG(value) AS avg_price,
                FIRST(value ORDER BY created_at ASC) AS true_open,
                LAST(value ORDER BY created_at DESC) AS true_close,
                MAX(value) - MIN(value) AS price_range,
                STDDEV(value) AS price_stddev,
                COUNT(*) AS price_updates
            FROM price_points
            WHERE created_at >= ?
                AND created_at <= ?
                AND coin_id = ?
            GROUP BY strftime(created_at, '%Y-%m-%d %H:%M:00')
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
        with get_duckdb("central") as conn:
            result = conn.execute(query_duckdb, [start_time, end_time, coin_id])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error("DuckDB price_movements query failed for %s (coin_id=%s): %s", token, coin_id, e)
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
        SELECT ts AS ts, price AS price
        FROM prices
        WHERE ts >= ? AND ts <= ? AND token = ?
        ORDER BY ts ASC
    """
    results = _execute_query(query, [start_time, end_time, "SOL"])
    
    if results:
        df = pd.DataFrame(results)
        if not df.empty and "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
            df = df.set_index("ts")
            df["price"] = df["price"].astype(float)
            return df
    
    # Fallback to file-based DuckDB price_points if prices table returned no results
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT created_at AS ts, value AS price
                FROM price_points
                WHERE created_at >= ? AND created_at <= ?
                    AND coin_id = 5
                ORDER BY created_at ASC
            """, [start_time, end_time])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            fallback_results = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error("DuckDB second_prices query failed: %s", e)
        fallback_results = []
    
    if not fallback_results:
        return pd.DataFrame(columns=["price"])
    
    df = pd.DataFrame(fallback_results)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.set_index("ts")
    df["price"] = df["price"].astype(float)
    return df


# =============================================================================
# PATTERN DETECTION FUNCTIONS
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
    
    # Write to DuckDB only (no MySQL)
    try:
        with get_duckdb("central") as conn:
            conn.execute("""
                UPDATE follow_the_goat_buyins
                SET fifteen_min_trail = ?
                WHERE id = ?
            """, [trail_json, buyin_id])
    except Exception as e:
        logger.warning(f"DuckDB trail persist failed: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def generate_trail_payload(
    buyin_id: int,
    symbol: Optional[str] = None,
    lookback_minutes: Optional[int] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Generate the 15-minute trail payload for a buy-in.

    Args:
        buyin_id: Target identifier in follow_the_goat_buyins.
        symbol: Optional override for the order book symbol (default: SOLUSDT).
        lookback_minutes: Window size in minutes (default: 15).
        persist: If True (default), store data in buyin_trail_minutes table.

    Returns:
        JSON-serializable dictionary containing order book and transaction data.
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

        # Fetch data from all sources
        order_book_rows = fetch_order_book_signals(symbol_to_use, window_start, window_end)
        transaction_rows = fetch_transactions(window_start, window_end)
        whale_rows = fetch_whale_activity(window_start, window_end)
        
        # Fetch SOL price movements (primary)
        price_rows = fetch_price_movements(window_start, window_end, token="SOL", coin_id=5)
        
        # Fetch BTC and ETH price movements for cross-market analysis
        btc_price_rows = fetch_price_movements(window_start, window_end, token="BTC", coin_id=6)
        eth_price_rows = fetch_price_movements(window_start, window_end, token="ETH", coin_id=7)
        
        second_prices = fetch_second_prices(window_start, window_end)

        # Run pattern detection
        if not second_prices.empty:
            patterns = detect_all_patterns(second_prices)
        else:
            patterns = {
                "detected": [],
                "breakout_score": 0.0,
                "swing_structure": {},
                "error": "no_second_price_data"
            }

        # Annotate rows with minute spans
        annotate_minute_spans(order_book_rows, window_end)
        annotate_minute_spans(transaction_rows, window_end)
        annotate_minute_spans(whale_rows, window_end)
        annotate_minute_spans(price_rows, window_end)
        annotate_minute_spans(btc_price_rows, window_end)
        annotate_minute_spans(eth_price_rows, window_end)

        payload: Dict[str, Any] = {
            "buyin_id": buyin["id"],
            "symbol": symbol_to_use,
            "followed_at": window_end,
            "window": {
                "start": window_start,
                "end": window_end,
                "minutes": minutes,
            },
            "order_book_signals": order_book_rows,
            "transactions": transaction_rows,
            "whale_activity": whale_rows,
            "price_movements": price_rows,
            "btc_price_movements": btc_price_rows,
            "eth_price_movements": eth_price_rows,
            "patterns": patterns,
            "second_prices": (
                second_prices.reset_index().to_dict('records')
                if not second_prices.empty
                else []
            ),
            "minute_spans": build_minute_span_view(
                order_book_rows, transaction_rows, whale_rows, price_rows
            ),
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

    parser = argparse.ArgumentParser(description="Generate a 15-minute trail payload")
    parser.add_argument("buyin_id", type=int, help="follow_the_goat_buyins ID")
    parser.add_argument("--symbol", help="Override symbol (default: SOLUSDT)")
    parser.add_argument("--minutes", type=int, help="Lookback window in minutes (default: 15)")
    parser.add_argument("--persist", action="store_true", help="Persist the generated JSON")

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


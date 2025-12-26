"""
15-Minute Trail Generator
=========================
Generate analytics trail data for buy-in signals using DuckDB.

This module fetches order book, transactions, whale activity, and price data
from DuckDB tables and computes derived metrics for pattern validation.

Usage:
    from _000trading.trail_generator import generate_trail_payload
    
    payload = generate_trail_payload(buyin_id=123)
    # payload contains order_book_signals, transactions, whale_activity, 
    # price_movements, patterns, and minute_spans
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

from core.database import get_duckdb, get_mysql, dual_write_update

logger = logging.getLogger(__name__)
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
    """Return buy-in metadata needed for the trail from DuckDB."""
    with get_duckdb("central") as conn:
        result = conn.execute("""
            SELECT id, followed_at, fifteen_min_trail as existing_trail
            FROM follow_the_goat_buyins
            WHERE id = ?
            LIMIT 1
        """, [buyin_id]).fetchone()
        
        if not result:
            raise BuyinNotFoundError(f"Buy-in #{buyin_id} not found")
        
        columns = ['id', 'followed_at', 'existing_trail']
        row = dict(zip(columns, result))
        
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
    """Fetch order book signals from DuckDB with computed metrics."""
    with get_duckdb("central") as conn:
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
        result = conn.execute(query, [symbol, start_time, end_time, symbol])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def fetch_transactions(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch transaction data from DuckDB with computed metrics."""
    with get_duckdb("central") as conn:
        query = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', trade_timestamp) AS minute_timestamp,
                SUM(sol_amount) AS total_sol_volume,
                SUM(stablecoin_amount) AS total_usd_volume,
                COUNT(*) AS trade_count,
                SUM(stablecoin_amount) / NULLIF(SUM(sol_amount), 0) AS vwap,
                SUM(CASE WHEN direction = 'buy' THEN sol_amount ELSE 0 END) AS buy_volume,
                SUM(CASE WHEN direction = 'sell' THEN sol_amount ELSE 0 END) AS sell_volume,
                SUM(CASE WHEN direction = 'buy' THEN stablecoin_amount ELSE 0 END) AS buy_usd_volume,
                SUM(CASE WHEN direction = 'sell' THEN stablecoin_amount ELSE 0 END) AS sell_usd_volume,
                SUM(CASE WHEN direction = 'buy' THEN 1 ELSE 0 END) AS buy_count,
                SUM(CASE WHEN direction = 'sell' THEN 1 ELSE 0 END) AS sell_count,
                SUM(CASE WHEN perp_direction = 'long' THEN sol_amount ELSE 0 END) AS long_volume,
                SUM(CASE WHEN perp_direction = 'short' THEN sol_amount ELSE 0 END) AS short_volume,
                SUM(CASE WHEN perp_direction = 'long' THEN stablecoin_amount ELSE 0 END) AS long_usd_volume,
                SUM(CASE WHEN perp_direction = 'short' THEN stablecoin_amount ELSE 0 END) AS short_usd_volume,
                SUM(CASE WHEN perp_direction = 'long' THEN 1 ELSE 0 END) AS long_count,
                SUM(CASE WHEN perp_direction = 'short' THEN 1 ELSE 0 END) AS short_count,
                SUM(CASE WHEN stablecoin_amount > 10000 THEN stablecoin_amount ELSE 0 END) AS large_trade_volume,
                SUM(CASE WHEN stablecoin_amount > 10000 THEN 1 ELSE 0 END) AS large_trade_count,
                SUM(CASE WHEN stablecoin_amount BETWEEN 1000 AND 10000 THEN stablecoin_amount ELSE 0 END) AS medium_trade_volume,
                SUM(CASE WHEN stablecoin_amount < 1000 THEN stablecoin_amount ELSE 0 END) AS small_trade_volume,
                AVG(stablecoin_amount) AS avg_trade_size,
                MAX(stablecoin_amount) AS max_trade_size,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price) AS avg_price,
                FIRST(price ORDER BY trade_timestamp ASC) AS open_price,
                LAST(price ORDER BY trade_timestamp DESC) AS close_price
            FROM sol_stablecoin_trades
            WHERE trade_timestamp >= ?
                AND trade_timestamp <= ?
            GROUP BY DATE_TRUNC('minute', trade_timestamp)
        ),
        numbered_minutes AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY minute_timestamp) AS row_num
            FROM minute_aggregates
        )
        SELECT 
            m0.minute_timestamp,
            m0.row_num AS minute_number,
            ROUND(((m0.buy_volume - m0.sell_volume) / NULLIF((m0.buy_volume + m0.sell_volume), 0)), 6) AS buy_sell_pressure,
            ROUND((m0.buy_volume / NULLIF((m0.buy_volume + m0.sell_volume), 0) * 100), 6) AS buy_volume_pct,
            ROUND((m0.sell_volume / NULLIF((m0.buy_volume + m0.sell_volume), 0) * 100), 6) AS sell_volume_pct,
            ROUND((((m0.buy_volume - m0.sell_volume) / NULLIF((m0.buy_volume + m0.sell_volume), 0)) -
                ((COALESCE(m1.buy_volume, m0.buy_volume) - COALESCE(m1.sell_volume, m0.sell_volume)) / 
                 NULLIF((COALESCE(m1.buy_volume, m0.buy_volume) + COALESCE(m1.sell_volume, m0.sell_volume)), 0))), 6) AS pressure_shift_1m,
            ROUND((m0.long_volume / NULLIF(m0.short_volume, 0)), 6) AS long_short_ratio,
            ROUND((m0.long_volume / NULLIF((m0.long_volume + m0.short_volume), 0) * 100), 6) AS long_volume_pct,
            ROUND((m0.short_volume / NULLIF((m0.long_volume + m0.short_volume), 0) * 100), 6) AS short_volume_pct,
            ROUND(((m0.long_volume - m0.short_volume) / NULLIF((m0.long_volume + m0.short_volume), 0) * 100), 6) AS perp_position_skew_pct,
            ROUND(((m0.long_volume / NULLIF(m0.short_volume, 0)) - 
                (COALESCE(m1.long_volume, m0.long_volume) / NULLIF(COALESCE(m1.short_volume, m0.short_volume), 0))), 6) AS long_ratio_shift_1m,
            ROUND(((m0.long_volume + m0.short_volume) / NULLIF(m0.total_sol_volume, 0) * 100), 6) AS perp_dominance_pct,
            ROUND(m0.total_usd_volume, 2) AS total_volume_usd,
            ROUND((m0.total_usd_volume / NULLIF(COALESCE(m1.total_usd_volume, m0.total_usd_volume), 0)), 6) AS volume_acceleration_ratio,
            ROUND((m0.large_trade_volume / NULLIF(m0.total_usd_volume, 0) * 100), 6) AS whale_volume_pct,
            ROUND(m0.avg_trade_size, 2) AS avg_trade_size,
            ROUND(m0.trade_count / 60.0, 2) AS trades_per_second,
            ROUND((m0.buy_count / NULLIF((m0.buy_count + m0.sell_count), 0) * 100), 6) AS buy_trade_pct,
            ROUND(((m0.close_price - m0.open_price) / NULLIF(m0.open_price, 0) * 100), 6) AS price_change_1m,
            ROUND(((m0.max_price - m0.min_price) / NULLIF(m0.avg_price, 0) * 100), 6) AS price_volatility_pct,
            m0.trade_count,
            m0.large_trade_count,
            ROUND(m0.vwap, 2) AS vwap
        FROM numbered_minutes m0
        LEFT JOIN numbered_minutes m1 
            ON m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        LEFT JOIN numbered_minutes m2
            ON m0.row_num > 2
            AND m2.row_num = m0.row_num - 2
        LEFT JOIN numbered_minutes m5
            ON m0.row_num > 5
            AND m5.row_num = m0.row_num - 5
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
        """
        result = conn.execute(query, [start_time, end_time])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def fetch_whale_activity(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch whale movement data from DuckDB with computed metrics."""
    with get_duckdb("central") as conn:
        query = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                SUM(CASE WHEN direction = 'in' THEN ABS(sol_change) ELSE 0 END) AS inflow_sol,
                SUM(CASE WHEN direction = 'in' THEN 1 ELSE 0 END) AS inflow_count,
                SUM(CASE WHEN direction = 'out' THEN ABS(sol_change) ELSE 0 END) AS outflow_sol,
                SUM(CASE WHEN direction = 'out' THEN 1 ELSE 0 END) AS outflow_count,
                SUM(CASE 
                    WHEN direction = 'in' THEN ABS(sol_change)
                    WHEN direction = 'out' THEN -ABS(sol_change)
                    ELSE 0 
                END) AS net_flow_sol,
                SUM(ABS(sol_change)) AS total_sol_moved,
                COUNT(*) AS total_movements,
                SUM(CASE WHEN percentage_moved > 10 THEN ABS(sol_change) ELSE 0 END) AS massive_move_sol,
                SUM(CASE WHEN percentage_moved > 10 THEN 1 ELSE 0 END) AS massive_move_count,
                SUM(CASE WHEN percentage_moved BETWEEN 5 AND 10 THEN ABS(sol_change) ELSE 0 END) AS large_move_sol,
                SUM(CASE WHEN percentage_moved BETWEEN 5 AND 10 THEN 1 ELSE 0 END) AS large_move_count,
                SUM(CASE WHEN percentage_moved BETWEEN 2 AND 5 THEN ABS(sol_change) ELSE 0 END) AS medium_move_sol,
                SUM(CASE WHEN percentage_moved BETWEEN 2 AND 5 THEN 1 ELSE 0 END) AS medium_move_count,
                SUM(CASE 
                    WHEN direction = 'in' AND percentage_moved > 5 THEN ABS(sol_change)
                    ELSE 0 
                END) AS strong_accumulation_sol,
                SUM(CASE 
                    WHEN direction = 'out' AND percentage_moved > 5 THEN ABS(sol_change)
                    ELSE 0 
                END) AS strong_distribution_sol,
                AVG(ABS(sol_change)) AS avg_move_size,
                MAX(ABS(sol_change)) AS max_move_size,
                AVG(percentage_moved) AS avg_percentage_moved,
                MAX(percentage_moved) AS max_percentage_moved
            FROM whale_movements
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
            m0.row_num AS minute_number,
            ROUND(
                CASE 
                    WHEN (m0.inflow_sol + m0.outflow_sol) > 0 THEN
                        m0.net_flow_sol / (m0.inflow_sol + m0.outflow_sol)
                    WHEN m0.outflow_sol > 0 AND m0.inflow_sol = 0 THEN -1.0
                    WHEN m0.inflow_sol > 0 AND m0.outflow_sol = 0 THEN 1.0
                    ELSE 0
                END,
            6) AS net_flow_ratio,
            ROUND(
                CASE 
                    WHEN m1.minute_timestamp IS NOT NULL AND (m0.inflow_sol + m0.outflow_sol) > 0 AND (m1.inflow_sol + m1.outflow_sol) > 0 THEN
                        (m0.net_flow_sol / (m0.inflow_sol + m0.outflow_sol)) -
                        (m1.net_flow_sol / (m1.inflow_sol + m1.outflow_sol))
                    ELSE 0
                END,
            6) AS flow_shift_1m,
            ROUND(
                CASE
                    WHEN m0.outflow_sol > 0 AND m0.inflow_sol > 0 THEN m0.inflow_sol / m0.outflow_sol
                    WHEN m0.inflow_sol > 0 AND m0.outflow_sol = 0 THEN 999.0
                    WHEN m0.outflow_sol > 0 AND m0.inflow_sol = 0 THEN 0.0
                    ELSE 1.0
                END,
            6) AS accumulation_ratio,
            ROUND(m0.strong_accumulation_sol, 2) AS strong_accumulation,
            ROUND(m0.total_sol_moved, 2) AS total_sol_moved,
            ROUND(m0.inflow_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS inflow_share_pct,
            ROUND(m0.outflow_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS outflow_share_pct,
            ROUND(m0.net_flow_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS net_flow_strength_pct,
            ROUND(m0.strong_accumulation_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS strong_accumulation_pct,
            ROUND(m0.strong_distribution_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS strong_distribution_pct,
            m0.total_movements AS movement_count,
            ROUND(m0.massive_move_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS massive_move_pct,
            ROUND(m0.avg_percentage_moved, 6) AS avg_wallet_pct_moved,
            ROUND(m0.strong_distribution_sol / NULLIF(m0.total_sol_moved, 0) * 100, 6) AS distribution_pressure_pct,
            ROUND(
                CASE
                    WHEN m1.outflow_sol > 0 THEN (m0.outflow_sol - m1.outflow_sol) / m1.outflow_sol * 100
                    WHEN m0.outflow_sol > 0 THEN 100.0
                    ELSE 0
                END,
            6) AS outflow_surge_pct,
            ROUND(
                ABS(m0.inflow_count - m0.outflow_count) / 
                NULLIF((m0.inflow_count + m0.outflow_count), 0) * 100,
            6) AS movement_imbalance_pct,
            ROUND(m0.inflow_sol, 2) AS inflow_sol,
            ROUND(m0.outflow_sol, 2) AS outflow_sol,
            ROUND(m0.net_flow_sol, 2) AS net_flow_sol,
            m0.inflow_count,
            m0.outflow_count,
            m0.massive_move_count,
            ROUND(m0.max_move_size, 2) AS max_move_size,
            ROUND(m0.strong_distribution_sol, 2) AS strong_distribution
        FROM numbered_minutes m0
        LEFT JOIN numbered_minutes m1 
            ON m0.row_num > 1
            AND m1.row_num = m0.row_num - 1
        ORDER BY m0.minute_timestamp DESC
        LIMIT 15
        """
        result = conn.execute(query, [start_time, end_time])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def fetch_price_movements(
    start_time: datetime,
    end_time: datetime
) -> List[Dict[str, Any]]:
    """Fetch price movement data from DuckDB with computed metrics."""
    with get_duckdb("central") as conn:
        query = """
        WITH minute_aggregates AS (
            SELECT 
                DATE_TRUNC('minute', created_at) AS minute_timestamp,
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
                AND coin_id = 5
            GROUP BY DATE_TRUNC('minute', created_at)
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
            ROUND(m0.true_open, 2) AS open_price,
            ROUND(m0.high_price, 2) AS high_price,
            ROUND(m0.low_price, 2) AS low_price,
            ROUND(m0.true_close, 2) AS close_price,
            ROUND(m0.avg_price, 2) AS avg_price,
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
        result = conn.execute(query, [start_time, end_time])
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def fetch_second_prices(
    start_time: datetime,
    end_time: datetime
) -> pd.DataFrame:
    """Fetch 1-second price data for pattern detection."""
    with get_duckdb("central") as conn:
        query = """
            SELECT created_at AS ts, value AS price
            FROM price_points
            WHERE created_at >= ? AND created_at <= ?
                AND coin_id = 5
            ORDER BY created_at ASC
        """
        result = conn.execute(query, [start_time, end_time])
        rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame(columns=["price"])
        
        df = pd.DataFrame(rows, columns=["ts", "price"])
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
        if isinstance(minute_timestamp, datetime):
            minute_dt = minute_timestamp
        elif isinstance(minute_timestamp, str):
            try:
                minute_dt = datetime.strptime(minute_timestamp, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        else:
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


def persist_trail(buyin_id: int, payload: Dict[str, Any]) -> None:
    """Persist the generated trail JSON into the buy-in row."""
    serializable_payload = make_json_serializable({
        key: value
        for key, value in payload.items()
        if key not in {"existing_trail", "persisted"}
    })
    
    trail_json = json.dumps(serializable_payload, ensure_ascii=True)
    
    # Dual-write to both DuckDB and MySQL
    dual_write_update(
        table="follow_the_goat_buyins",
        data={"fifteen_min_trail": trail_json},
        where={"id": buyin_id}
    )


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def generate_trail_payload(
    buyin_id: int,
    symbol: Optional[str] = None,
    lookback_minutes: Optional[int] = None,
    persist: bool = False,
) -> Dict[str, Any]:
    """Generate the 15-minute trail payload for a buy-in.

    Args:
        buyin_id: Target identifier in follow_the_goat_buyins.
        symbol: Optional override for the order book symbol (default: SOLUSDT).
        lookback_minutes: Window size in minutes (default: 15).
        persist: If True, store the JSON in the database.

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
        price_rows = fetch_price_movements(window_start, window_end)
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
            persist_trail(buyin_id, payload)
            payload["persisted"] = True

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


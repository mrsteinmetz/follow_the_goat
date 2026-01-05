"""
Trail Data Storage
==================
Module for storing 15-minute trail data in a structured table format.

This module handles:
- Table creation for DuckDB and MySQL
- Flattening trail payloads into database rows
- Dual-write insertion (15 rows per buyin)
- Query functions to retrieve trail data

Usage:
    from trail_data import insert_trail_data, get_trail_for_buyin
    
    # Insert trail data for a buyin
    insert_trail_data(buyin_id=123, trail_payload=payload)
    
    # Retrieve trail data
    rows = get_trail_for_buyin(buyin_id=123)
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
PROJECT_ROOT = Path(__file__).parent.parent
MODULE_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(MODULE_DIR))

from core.database import get_duckdb

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# =============================================================================
# FIELD MAPPINGS (JSON field -> DB column)
# =============================================================================

PRICE_MOVEMENTS_FIELDS = {
    "price_change_1m": "pm_price_change_1m",
    "momentum_volatility_ratio": "pm_momentum_volatility_ratio",
    "momentum_acceleration_1m": "pm_momentum_acceleration_1m",
    "price_change_5m": "pm_price_change_5m",
    "price_change_10m": "pm_price_change_10m",
    "volatility_pct": "pm_volatility_pct",
    "body_range_ratio": "pm_body_range_ratio",
    "volatility_surge_ratio": "pm_volatility_surge_ratio",
    "price_stddev_pct": "pm_price_stddev_pct",
    "trend_consistency_3m": "pm_trend_consistency_3m",
    "cumulative_return_5m": "pm_cumulative_return_5m",
    "candle_body_pct": "pm_candle_body_pct",
    "upper_wick_pct": "pm_upper_wick_pct",
    "lower_wick_pct": "pm_lower_wick_pct",
    "wick_balance_ratio": "pm_wick_balance_ratio",
    "price_vs_ma5_pct": "pm_price_vs_ma5_pct",
    "breakout_strength_10m": "pm_breakout_strength_10m",
    "open_price": "pm_open_price",
    "high_price": "pm_high_price",
    "low_price": "pm_low_price",
    "close_price": "pm_close_price",
    "avg_price": "pm_avg_price",
}

# BTC Price Movements - for cross-market correlation analysis
BTC_PRICE_FIELDS = {
    "price_change_1m": "btc_price_change_1m",
    "price_change_5m": "btc_price_change_5m",
    "price_change_10m": "btc_price_change_10m",
    "volatility_pct": "btc_volatility_pct",
    "open_price": "btc_open_price",
    "close_price": "btc_close_price",
}

# ETH Price Movements - for cross-market correlation analysis
ETH_PRICE_FIELDS = {
    "price_change_1m": "eth_price_change_1m",
    "price_change_5m": "eth_price_change_5m",
    "price_change_10m": "eth_price_change_10m",
    "volatility_pct": "eth_volatility_pct",
    "open_price": "eth_open_price",
    "close_price": "eth_close_price",
}

ORDER_BOOK_FIELDS = {
    "mid_price": "ob_mid_price",
    "price_change_1m": "ob_price_change_1m",
    "price_change_5m": "ob_price_change_5m",
    "price_change_10m": "ob_price_change_10m",
    "volume_imbalance": "ob_volume_imbalance",
    "imbalance_shift_1m": "ob_imbalance_shift_1m",
    "imbalance_trend_3m": "ob_imbalance_trend_3m",
    "depth_imbalance_ratio": "ob_depth_imbalance_ratio",
    "bid_liquidity_share_pct": "ob_bid_liquidity_share_pct",
    "ask_liquidity_share_pct": "ob_ask_liquidity_share_pct",
    "depth_imbalance_pct": "ob_depth_imbalance_pct",
    "total_liquidity": "ob_total_liquidity",
    "liquidity_change_3m": "ob_liquidity_change_3m",
    "microprice_deviation": "ob_microprice_deviation",
    "microprice_acceleration_2m": "ob_microprice_acceleration_2m",
    "spread_bps": "ob_spread_bps",
    "aggression_ratio": "ob_aggression_ratio",
    "vwap_spread_bps": "ob_vwap_spread_bps",
    "net_flow_5m": "ob_net_flow_5m",
    "net_flow_to_liquidity_ratio": "ob_net_flow_to_liquidity_ratio",
    "sample_count": "ob_sample_count",
    "coverage_seconds": "ob_coverage_seconds",
}

TRANSACTIONS_FIELDS = {
    "buy_sell_pressure": "tx_buy_sell_pressure",
    "buy_volume_pct": "tx_buy_volume_pct",
    "sell_volume_pct": "tx_sell_volume_pct",
    "pressure_shift_1m": "tx_pressure_shift_1m",
    "pressure_trend_3m": "tx_pressure_trend_3m",
    "long_short_ratio": "tx_long_short_ratio",
    "long_volume_pct": "tx_long_volume_pct",
    "short_volume_pct": "tx_short_volume_pct",
    "perp_position_skew_pct": "tx_perp_position_skew_pct",
    "long_ratio_shift_1m": "tx_long_ratio_shift_1m",
    "perp_dominance_pct": "tx_perp_dominance_pct",
    "total_volume_usd": "tx_total_volume_usd",
    "volume_acceleration_ratio": "tx_volume_acceleration_ratio",
    "volume_surge_ratio": "tx_volume_surge_ratio",
    "whale_volume_pct": "tx_whale_volume_pct",
    "avg_trade_size": "tx_avg_trade_size",
    "trades_per_second": "tx_trades_per_second",
    "buy_trade_pct": "tx_buy_trade_pct",
    "price_change_1m": "tx_price_change_1m",
    "price_volatility_pct": "tx_price_volatility_pct",
    "cumulative_buy_flow_5m": "tx_cumulative_buy_flow_5m",
    "trade_count": "tx_trade_count",
    "large_trade_count": "tx_large_trade_count",
    "vwap": "tx_vwap",
}

WHALE_ACTIVITY_FIELDS = {
    "net_flow_ratio": "wh_net_flow_ratio",
    "flow_shift_1m": "wh_flow_shift_1m",
    "flow_trend_3m": "wh_flow_trend_3m",
    "accumulation_ratio": "wh_accumulation_ratio",
    "strong_accumulation": "wh_strong_accumulation",
    "cumulative_flow_5m": "wh_cumulative_flow_5m",
    "total_sol_moved": "wh_total_sol_moved",
    "inflow_share_pct": "wh_inflow_share_pct",
    "outflow_share_pct": "wh_outflow_share_pct",
    "net_flow_strength_pct": "wh_net_flow_strength_pct",
    "strong_accumulation_pct": "wh_strong_accumulation_pct",
    "strong_distribution_pct": "wh_strong_distribution_pct",
    "activity_surge_ratio": "wh_activity_surge_ratio",
    "movement_count": "wh_movement_count",
    "massive_move_pct": "wh_massive_move_pct",
    "avg_wallet_pct_moved": "wh_avg_wallet_pct_moved",
    "largest_move_dominance": "wh_largest_move_dominance",
    "distribution_pressure_pct": "wh_distribution_pressure_pct",
    "outflow_surge_pct": "wh_outflow_surge_pct",
    "movement_imbalance_pct": "wh_movement_imbalance_pct",
    "inflow_sol": "wh_inflow_sol",
    "outflow_sol": "wh_outflow_sol",
    "net_flow_sol": "wh_net_flow_sol",
    "inflow_count": "wh_inflow_count",
    "outflow_count": "wh_outflow_count",
    "massive_move_count": "wh_massive_move_count",
    "max_move_size": "wh_max_move_size",
    "strong_distribution": "wh_strong_distribution",
}


# =============================================================================
# TABLE MANAGEMENT
# =============================================================================

def ensure_trail_table_exists_duckdb() -> None:
    """Ensure the buyin_trail_minutes table exists in DuckDB."""
    from features.price_api.schema import SCHEMA_BUYIN_TRAIL_MINUTES
    
    with get_duckdb("central") as conn:
        conn.execute(SCHEMA_BUYIN_TRAIL_MINUTES)
        logger.debug("Ensured buyin_trail_minutes table exists in DuckDB")


def ensure_trail_table_exists_mysql() -> None:
    """Ensure the buyin_trail_minutes table exists in MySQL."""
    # MySQL version of the schema (slightly different syntax)
    create_sql = """
    CREATE TABLE IF NOT EXISTS buyin_trail_minutes (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        buyin_id BIGINT NOT NULL,
        minute TINYINT NOT NULL,
        
        -- Price Movements (pm_)
        pm_price_change_1m DOUBLE,
        pm_momentum_volatility_ratio DOUBLE,
        pm_momentum_acceleration_1m DOUBLE,
        pm_price_change_5m DOUBLE,
        pm_price_change_10m DOUBLE,
        pm_volatility_pct DOUBLE,
        pm_body_range_ratio DOUBLE,
        pm_volatility_surge_ratio DOUBLE,
        pm_price_stddev_pct DOUBLE,
        pm_trend_consistency_3m DOUBLE,
        pm_cumulative_return_5m DOUBLE,
        pm_candle_body_pct DOUBLE,
        pm_upper_wick_pct DOUBLE,
        pm_lower_wick_pct DOUBLE,
        pm_wick_balance_ratio DOUBLE,
        pm_price_vs_ma5_pct DOUBLE,
        pm_breakout_strength_10m DOUBLE,
        pm_open_price DOUBLE,
        pm_high_price DOUBLE,
        pm_low_price DOUBLE,
        pm_close_price DOUBLE,
        pm_avg_price DOUBLE,
        
        -- BTC Price Movements (btc_)
        btc_price_change_1m DOUBLE,
        btc_price_change_5m DOUBLE,
        btc_price_change_10m DOUBLE,
        btc_volatility_pct DOUBLE,
        btc_open_price DOUBLE,
        btc_close_price DOUBLE,
        
        -- ETH Price Movements (eth_)
        eth_price_change_1m DOUBLE,
        eth_price_change_5m DOUBLE,
        eth_price_change_10m DOUBLE,
        eth_volatility_pct DOUBLE,
        eth_open_price DOUBLE,
        eth_close_price DOUBLE,
        
        -- Order Book Signals (ob_)
        ob_mid_price DOUBLE,
        ob_price_change_1m DOUBLE,
        ob_price_change_5m DOUBLE,
        ob_price_change_10m DOUBLE,
        ob_volume_imbalance DOUBLE,
        ob_imbalance_shift_1m DOUBLE,
        ob_imbalance_trend_3m DOUBLE,
        ob_depth_imbalance_ratio DOUBLE,
        ob_bid_liquidity_share_pct DOUBLE,
        ob_ask_liquidity_share_pct DOUBLE,
        ob_depth_imbalance_pct DOUBLE,
        ob_total_liquidity DOUBLE,
        ob_liquidity_change_3m DOUBLE,
        ob_microprice_deviation DOUBLE,
        ob_microprice_acceleration_2m DOUBLE,
        ob_spread_bps DOUBLE,
        ob_aggression_ratio DOUBLE,
        ob_vwap_spread_bps DOUBLE,
        ob_net_flow_5m DOUBLE,
        ob_net_flow_to_liquidity_ratio DOUBLE,
        ob_sample_count INT,
        ob_coverage_seconds INT,
        
        -- Transactions (tx_)
        tx_buy_sell_pressure DOUBLE,
        tx_buy_volume_pct DOUBLE,
        tx_sell_volume_pct DOUBLE,
        tx_pressure_shift_1m DOUBLE,
        tx_pressure_trend_3m DOUBLE,
        tx_long_short_ratio DOUBLE,
        tx_long_volume_pct DOUBLE,
        tx_short_volume_pct DOUBLE,
        tx_perp_position_skew_pct DOUBLE,
        tx_long_ratio_shift_1m DOUBLE,
        tx_perp_dominance_pct DOUBLE,
        tx_total_volume_usd DOUBLE,
        tx_volume_acceleration_ratio DOUBLE,
        tx_volume_surge_ratio DOUBLE,
        tx_whale_volume_pct DOUBLE,
        tx_avg_trade_size DOUBLE,
        tx_trades_per_second DOUBLE,
        tx_buy_trade_pct DOUBLE,
        tx_price_change_1m DOUBLE,
        tx_price_volatility_pct DOUBLE,
        tx_cumulative_buy_flow_5m DOUBLE,
        tx_trade_count INT,
        tx_large_trade_count INT,
        tx_vwap DOUBLE,
        
        -- Whale Activity (wh_)
        wh_net_flow_ratio DOUBLE,
        wh_flow_shift_1m DOUBLE,
        wh_flow_trend_3m DOUBLE,
        wh_accumulation_ratio DOUBLE,
        wh_strong_accumulation DOUBLE,
        wh_cumulative_flow_5m DOUBLE,
        wh_total_sol_moved DOUBLE,
        wh_inflow_share_pct DOUBLE,
        wh_outflow_share_pct DOUBLE,
        wh_net_flow_strength_pct DOUBLE,
        wh_strong_accumulation_pct DOUBLE,
        wh_strong_distribution_pct DOUBLE,
        wh_activity_surge_ratio DOUBLE,
        wh_movement_count INT,
        wh_massive_move_pct DOUBLE,
        wh_avg_wallet_pct_moved DOUBLE,
        wh_largest_move_dominance DOUBLE,
        wh_distribution_pressure_pct DOUBLE,
        wh_outflow_surge_pct DOUBLE,
        wh_movement_imbalance_pct DOUBLE,
        wh_inflow_sol DOUBLE,
        wh_outflow_sol DOUBLE,
        wh_net_flow_sol DOUBLE,
        wh_inflow_count INT,
        wh_outflow_count INT,
        wh_massive_move_count INT,
        wh_max_move_size DOUBLE,
        wh_strong_distribution DOUBLE,
        
        -- Pattern Detection (pat_)
        pat_breakout_score DOUBLE,
        pat_detected_count INT,
        pat_detected_list VARCHAR(255),
        pat_asc_tri_detected TINYINT(1),
        pat_asc_tri_confidence DOUBLE,
        pat_asc_tri_resistance_level DOUBLE,
        pat_asc_tri_support_level DOUBLE,
        pat_asc_tri_compression_ratio DOUBLE,
        pat_bull_flag_detected TINYINT(1),
        pat_bull_flag_confidence DOUBLE,
        pat_bull_flag_pole_height_pct DOUBLE,
        pat_bull_flag_retracement_pct DOUBLE,
        pat_bull_pennant_detected TINYINT(1),
        pat_bull_pennant_confidence DOUBLE,
        pat_bull_pennant_compression_ratio DOUBLE,
        pat_fall_wedge_detected TINYINT(1),
        pat_fall_wedge_confidence DOUBLE,
        pat_fall_wedge_contraction DOUBLE,
        pat_cup_handle_detected TINYINT(1),
        pat_cup_handle_confidence DOUBLE,
        pat_cup_handle_depth_pct DOUBLE,
        pat_inv_hs_detected TINYINT(1),
        pat_inv_hs_confidence DOUBLE,
        pat_inv_hs_neckline DOUBLE,
        pat_swing_trend VARCHAR(20),
        pat_swing_higher_lows TINYINT(1),
        pat_swing_lower_highs TINYINT(1),
        
        -- Second Prices Summary (sp_)
        sp_price_count INT,
        sp_min_price DOUBLE,
        sp_max_price DOUBLE,
        sp_start_price DOUBLE,
        sp_end_price DOUBLE,
        sp_price_range_pct DOUBLE,
        sp_total_change_pct DOUBLE,
        sp_volatility_pct DOUBLE,
        sp_avg_price DOUBLE,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE KEY idx_trail_buyin_minute (buyin_id, minute),
        KEY idx_trail_buyin_id (buyin_id),
        KEY idx_trail_minute (minute),
        KEY idx_trail_created_at (created_at),
        KEY idx_trail_breakout_score (pat_breakout_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    
    # MySQL table creation removed - DuckDB is the primary database
    pass


def ensure_trail_tables_exist() -> None:
    """Ensure the buyin_trail_minutes table exists in DuckDB."""
    ensure_trail_table_exists_duckdb()
    # MySQL table creation removed


# =============================================================================
# DATA EXTRACTION
# =============================================================================

def _get_section_data_for_minute(
    trail_data: Dict[str, Any],
    section_key: str,
    minute: int
) -> Optional[Dict[str, Any]]:
    """Extract section data for a specific minute from trail data."""
    section_data = trail_data.get(section_key, [])
    
    if not isinstance(section_data, list):
        return None
    
    # Find record with matching minute_span_from
    for record in section_data:
        if isinstance(record, dict):
            span_from = record.get("minute_span_from")
            if span_from is not None and int(span_from) == minute:
                return record
    
    return None


def _extract_pattern_data(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract pattern detection data from trail payload."""
    patterns = trail_data.get("patterns", {})
    
    if not isinstance(patterns, dict):
        return {}
    
    result = {
        "pat_breakout_score": patterns.get("breakout_score"),
        "pat_detected_count": len(patterns.get("detected", [])),
        "pat_detected_list": ",".join(patterns.get("detected", [])) if patterns.get("detected") else None,
    }
    
    # Ascending Triangle
    asc_tri = patterns.get("ascending_triangle", {})
    result["pat_asc_tri_detected"] = bool(asc_tri.get("detected"))
    result["pat_asc_tri_confidence"] = asc_tri.get("confidence")
    result["pat_asc_tri_resistance_level"] = asc_tri.get("resistance_level")
    result["pat_asc_tri_support_level"] = asc_tri.get("support_level")
    result["pat_asc_tri_compression_ratio"] = asc_tri.get("compression_ratio")
    
    # Bullish Flag
    bull_flag = patterns.get("bullish_flag", {})
    result["pat_bull_flag_detected"] = bool(bull_flag.get("detected"))
    result["pat_bull_flag_confidence"] = bull_flag.get("confidence")
    result["pat_bull_flag_pole_height_pct"] = bull_flag.get("pole_height_pct")
    result["pat_bull_flag_retracement_pct"] = bull_flag.get("retracement_pct")
    
    # Bullish Pennant
    bull_pennant = patterns.get("bullish_pennant", {})
    result["pat_bull_pennant_detected"] = bool(bull_pennant.get("detected"))
    result["pat_bull_pennant_confidence"] = bull_pennant.get("confidence")
    result["pat_bull_pennant_compression_ratio"] = bull_pennant.get("compression_ratio")
    
    # Falling Wedge
    fall_wedge = patterns.get("falling_wedge", {})
    result["pat_fall_wedge_detected"] = bool(fall_wedge.get("detected"))
    result["pat_fall_wedge_confidence"] = fall_wedge.get("confidence")
    result["pat_fall_wedge_contraction"] = fall_wedge.get("wedge_contraction")
    
    # Cup and Handle
    cup_handle = patterns.get("cup_and_handle", {})
    result["pat_cup_handle_detected"] = bool(cup_handle.get("detected"))
    result["pat_cup_handle_confidence"] = cup_handle.get("confidence")
    result["pat_cup_handle_depth_pct"] = cup_handle.get("cup_depth_pct")
    
    # Inverse Head & Shoulders
    inv_hs = patterns.get("inverse_head_shoulders", {})
    result["pat_inv_hs_detected"] = bool(inv_hs.get("detected"))
    result["pat_inv_hs_confidence"] = inv_hs.get("confidence")
    result["pat_inv_hs_neckline"] = inv_hs.get("neckline")
    
    # Swing Structure
    swing = patterns.get("swing_structure", {})
    result["pat_swing_trend"] = swing.get("trend")
    result["pat_swing_higher_lows"] = bool(swing.get("higher_lows"))
    result["pat_swing_lower_highs"] = bool(swing.get("lower_highs"))
    
    return result


def _extract_second_prices_stats(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract summary statistics from second_prices data."""
    second_prices = trail_data.get("second_prices", [])
    
    if not second_prices or not isinstance(second_prices, list):
        return {
            "sp_price_count": 0,
            "sp_min_price": None,
            "sp_max_price": None,
            "sp_start_price": None,
            "sp_end_price": None,
            "sp_price_range_pct": None,
            "sp_total_change_pct": None,
            "sp_volatility_pct": None,
            "sp_avg_price": None,
        }
    
    # Extract prices from records
    prices = []
    for record in second_prices:
        if isinstance(record, dict):
            price = record.get("price")
            if price is not None:
                try:
                    prices.append(float(price))
                except (ValueError, TypeError):
                    continue
    
    if not prices:
        return {
            "sp_price_count": 0,
            "sp_min_price": None,
            "sp_max_price": None,
            "sp_start_price": None,
            "sp_end_price": None,
            "sp_price_range_pct": None,
            "sp_total_change_pct": None,
            "sp_volatility_pct": None,
            "sp_avg_price": None,
        }
    
    # Compute statistics
    price_count = len(prices)
    min_price = min(prices)
    max_price = max(prices)
    start_price = prices[0]
    end_price = prices[-1]
    avg_price = sum(prices) / price_count
    
    # Price range percentage
    price_range_pct = None
    if start_price and start_price > 0:
        price_range_pct = (max_price - min_price) / start_price * 100
    
    # Total change percentage
    total_change_pct = None
    if start_price and start_price > 0:
        total_change_pct = (end_price - start_price) / start_price * 100
    
    # Volatility (standard deviation / average * 100)
    volatility_pct = None
    if price_count >= 2 and avg_price > 0:
        variance = sum((p - avg_price) ** 2 for p in prices) / price_count
        std_dev = variance ** 0.5
        volatility_pct = (std_dev / avg_price) * 100
    
    return {
        "sp_price_count": price_count,
        "sp_min_price": round(min_price, 8),
        "sp_max_price": round(max_price, 8),
        "sp_start_price": round(start_price, 8),
        "sp_end_price": round(end_price, 8),
        "sp_price_range_pct": round(price_range_pct, 4) if price_range_pct is not None else None,
        "sp_total_change_pct": round(total_change_pct, 4) if total_change_pct is not None else None,
        "sp_volatility_pct": round(volatility_pct, 4) if volatility_pct is not None else None,
        "sp_avg_price": round(avg_price, 8),
    }


def _convert_value(value: Any) -> Any:
    """Convert Python values to database-compatible types."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, datetime):
        return value
    return str(value)


def _build_row_for_minute(
    buyin_id: int,
    trail_data: Dict[str, Any],
    minute: int,
    pattern_data: Dict[str, Any],
    second_prices_stats: Dict[str, Any]
) -> Dict[str, Any]:
    """Build a flattened row for a specific minute."""
    row = {
        "buyin_id": buyin_id,
        "minute": minute,
    }
    
    # Extract Price Movements data
    pm_data = _get_section_data_for_minute(trail_data, "price_movements", minute)
    if pm_data:
        for json_field, db_col in PRICE_MOVEMENTS_FIELDS.items():
            row[db_col] = _convert_value(pm_data.get(json_field))
    
    # Extract Order Book Signals data
    ob_data = _get_section_data_for_minute(trail_data, "order_book_signals", minute)
    if ob_data:
        for json_field, db_col in ORDER_BOOK_FIELDS.items():
            row[db_col] = _convert_value(ob_data.get(json_field))
    
    # Extract Transactions data
    tx_data = _get_section_data_for_minute(trail_data, "transactions", minute)
    if tx_data:
        for json_field, db_col in TRANSACTIONS_FIELDS.items():
            row[db_col] = _convert_value(tx_data.get(json_field))
    
    # Extract Whale Activity data
    wh_data = _get_section_data_for_minute(trail_data, "whale_activity", minute)
    if wh_data:
        for json_field, db_col in WHALE_ACTIVITY_FIELDS.items():
            row[db_col] = _convert_value(wh_data.get(json_field))
    
    # Extract BTC Price Movements data
    btc_data = _get_section_data_for_minute(trail_data, "btc_price_movements", minute)
    if btc_data:
        for json_field, db_col in BTC_PRICE_FIELDS.items():
            row[db_col] = _convert_value(btc_data.get(json_field))
    
    # Extract ETH Price Movements data
    eth_data = _get_section_data_for_minute(trail_data, "eth_price_movements", minute)
    if eth_data:
        for json_field, db_col in ETH_PRICE_FIELDS.items():
            row[db_col] = _convert_value(eth_data.get(json_field))
    
    # Add pattern data (same for all minutes of a trade)
    for key, value in pattern_data.items():
        row[key] = _convert_value(value)
    
    # Add second_prices summary statistics (same for all minutes)
    for key, value in second_prices_stats.items():
        row[key] = _convert_value(value)
    
    return row


# =============================================================================
# FLATTEN TRAIL TO ROWS
# =============================================================================

def flatten_trail_to_rows(buyin_id: int, trail_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert a trail payload into 15 database rows (one per minute).
    
    Args:
        buyin_id: The ID of the buyin record
        trail_payload: The trail data payload from generate_trail_payload()
        
    Returns:
        List of 15 dictionaries, each representing one minute of data
    """
    if not trail_payload:
        logger.warning(f"Empty trail payload for buyin_id={buyin_id}")
        return []
    
    # Extract pattern data once (same for all minutes)
    pattern_data = _extract_pattern_data(trail_payload)
    
    # Extract second_prices summary statistics once (same for all minutes)
    second_prices_stats = _extract_second_prices_stats(trail_payload)
    
    rows = []
    for minute in range(15):
        row = _build_row_for_minute(
            buyin_id=buyin_id,
            trail_data=trail_payload,
            minute=minute,
            pattern_data=pattern_data,
            second_prices_stats=second_prices_stats
        )
        rows.append(row)
    
    return rows


# =============================================================================
# INSERT FUNCTIONS
# =============================================================================

def _get_all_columns() -> List[str]:
    """Get all column names for the trail table (excluding id and created_at)."""
    columns = ["buyin_id", "minute"]
    
    # Add section columns
    columns.extend(PRICE_MOVEMENTS_FIELDS.values())
    columns.extend(BTC_PRICE_FIELDS.values())
    columns.extend(ETH_PRICE_FIELDS.values())
    columns.extend(ORDER_BOOK_FIELDS.values())
    columns.extend(TRANSACTIONS_FIELDS.values())
    columns.extend(WHALE_ACTIVITY_FIELDS.values())
    
    # Add pattern columns
    pattern_cols = [
        "pat_breakout_score", "pat_detected_count", "pat_detected_list",
        "pat_asc_tri_detected", "pat_asc_tri_confidence", "pat_asc_tri_resistance_level",
        "pat_asc_tri_support_level", "pat_asc_tri_compression_ratio",
        "pat_bull_flag_detected", "pat_bull_flag_confidence", "pat_bull_flag_pole_height_pct",
        "pat_bull_flag_retracement_pct",
        "pat_bull_pennant_detected", "pat_bull_pennant_confidence", "pat_bull_pennant_compression_ratio",
        "pat_fall_wedge_detected", "pat_fall_wedge_confidence", "pat_fall_wedge_contraction",
        "pat_cup_handle_detected", "pat_cup_handle_confidence", "pat_cup_handle_depth_pct",
        "pat_inv_hs_detected", "pat_inv_hs_confidence", "pat_inv_hs_neckline",
        "pat_swing_trend", "pat_swing_higher_lows", "pat_swing_lower_highs",
    ]
    columns.extend(pattern_cols)
    
    # Add second_prices summary columns
    sp_cols = [
        "sp_price_count", "sp_min_price", "sp_max_price", "sp_start_price",
        "sp_end_price", "sp_price_range_pct", "sp_total_change_pct",
        "sp_volatility_pct", "sp_avg_price",
    ]
    columns.extend(sp_cols)
    
    return columns


def insert_trail_rows_duckdb(buyin_id: int, rows: List[Dict[str, Any]]) -> bool:
    """Insert trail rows into DuckDB.
    
    CRITICAL: Uses master2's write queue for thread-safe writes.
    
    Args:
        buyin_id: The buyin ID (used for logging)
        rows: List of row dictionaries to insert
        
    Returns:
        True if successful, False otherwise
    """
    if not rows:
        return False
    
    columns = _get_all_columns()
    col_list = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])
    
    # Use the write queue registered by master2
    try:
        from core.database import duckdb_execute_write
        
        # Check if data already exists first (read operation)
        with get_duckdb("central", read_only=True) as cursor:
            existing = cursor.execute(
                "SELECT COUNT(*) FROM buyin_trail_minutes WHERE buyin_id = ?",
                [buyin_id]
            ).fetchone()
            existing_count = existing[0] if existing else 0
            
            if existing_count > 0:
                logger.debug(f"Trail data already exists for buyin_id={buyin_id}, skipping")
                return True
        
        # Insert rows via write queue (thread-safe, non-blocking)
        inserted_count = 0
        for row in rows:
            values = [row.get(col) for col in columns]
            duckdb_execute_write(
                "central",
                f"INSERT INTO buyin_trail_minutes ({col_list}) VALUES ({placeholders})",
                values,
                sync=False  # Non-blocking for speed
            )
            inserted_count += 1
        
        # Final sync write to ensure completion
        duckdb_execute_write(
            "central",
            "SELECT 1",  # Dummy query to ensure all previous writes complete
            [],
            sync=True
        )
        
        # Verify
        with get_duckdb("central", read_only=True) as cursor:
            verify = cursor.execute(
                "SELECT COUNT(*) FROM buyin_trail_minutes WHERE buyin_id = ?",
                [buyin_id]
            ).fetchone()
            verify_count = verify[0] if verify else 0
        
        logger.info(f"✅ PERSISTED via write queue: {inserted_count} trail rows for buyin_id={buyin_id}, verified: {verify_count}")
        return True
            
    except Exception as e:
        logger.error(f"Failed to write trail data via write queue for buyin_id={buyin_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def insert_trail_rows_mysql(buyin_id: int, rows: List[Dict[str, Any]]) -> bool:
    """Insert trail rows into MySQL.
    
    Args:
        buyin_id: The buyin ID (used for logging)
        rows: List of row dictionaries to insert
        
    Returns:
        True if successful, False otherwise
    """
    if not rows:
        return False
    
    # MySQL insert removed - DuckDB is the primary database
    return True


def insert_trail_data(buyin_id: int, trail_payload: Dict[str, Any]) -> bool:
    """Insert trail data for a buyin into DuckDB.
    
    This is the main entry point for persisting trail data.
    
    Args:
        buyin_id: The ID of the buyin record
        trail_payload: The trail data payload from generate_trail_payload()
        
    Returns:
        True if DuckDB insert succeeded, False otherwise
    """
    # Ensure tables exist
    ensure_trail_tables_exist()
    
    # Flatten the trail payload into rows
    rows = flatten_trail_to_rows(buyin_id, trail_payload)
    
    if not rows:
        logger.warning(f"No rows generated for buyin_id={buyin_id}")
        return False
    
    # Insert into DuckDB (primary and only database)
    duckdb_success = insert_trail_rows_duckdb(buyin_id, rows)
    
    if duckdb_success:
        logger.info(f"✓ Inserted {len(rows)} trail rows for buyin_id={buyin_id}")
    else:
        logger.error(f"✗ Failed to insert trail rows for buyin_id={buyin_id}")
    
    return duckdb_success


# =============================================================================
# QUERY FUNCTIONS
# =============================================================================

def get_trail_for_buyin(buyin_id: int) -> List[Dict[str, Any]]:
    """Retrieve all trail rows for a buyin from DuckDB.
    
    Args:
        buyin_id: The ID of the buyin record
        
    Returns:
        List of dictionaries, one per minute (0-14), ordered by minute
    """
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT * FROM buyin_trail_minutes
                WHERE buyin_id = ?
                ORDER BY minute ASC
            """, [buyin_id])
            
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
            
    except Exception as e:
        logger.error(f"Failed to get trail for buyin_id={buyin_id}: {e}")
        return []


def get_trail_minute(buyin_id: int, minute: int) -> Optional[Dict[str, Any]]:
    """Retrieve a specific minute's trail data for a buyin.
    
    Args:
        buyin_id: The ID of the buyin record
        minute: The minute index (0-14)
        
    Returns:
        Dictionary with the minute's data, or None if not found
    """
    if minute < 0 or minute > 14:
        logger.warning(f"Invalid minute {minute}, must be 0-14")
        return None
    
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT * FROM buyin_trail_minutes
                WHERE buyin_id = ? AND minute = ?
                LIMIT 1
            """, [buyin_id, minute])
            
            columns = [desc[0] for desc in result.description]
            row = result.fetchone()
            
            if row:
                return dict(zip(columns, row))
            return None
            
    except Exception as e:
        logger.error(f"Failed to get trail minute for buyin_id={buyin_id}, minute={minute}: {e}")
        return None


def delete_trail_for_buyin(buyin_id: int) -> bool:
    """Delete all trail rows for a buyin from both databases.
    
    Args:
        buyin_id: The ID of the buyin record
        
    Returns:
        True if deletion succeeded, False otherwise
    """
    duckdb_success = False
    mysql_success = False
    
    try:
        with get_duckdb("central") as conn:
            conn.execute("DELETE FROM buyin_trail_minutes WHERE buyin_id = ?", [buyin_id])
        duckdb_success = True
    except Exception as e:
        logger.warning(f"DuckDB delete failed for buyin_id={buyin_id}: {e}")
    
    return duckdb_success


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

from core.database import get_postgres

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
    """Ensure the buyin_trail_minutes table exists in PostgreSQL."""
    from features.price_api.schema import SCHEMA_BUYIN_TRAIL_MINUTES
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(SCHEMA_BUYIN_TRAIL_MINUTES)
        conn.commit()
        logger.debug("Ensured buyin_trail_minutes table exists in PostgreSQL")


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
    """Ensure the buyin_trail_minutes table exists in PostgreSQL."""
    ensure_trail_table_exists_duckdb()  # Name kept for compatibility, but uses PostgreSQL


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
    patterns = trail_data.get("patterns") or trail_data.get("traditional_patterns") or {}
    
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


def _extract_micro_pattern_data(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract micro-pattern detection data from trail payload."""
    micro = trail_data.get("micro_patterns", {})
    if not isinstance(micro, dict):
        return {}
    
    def _flag(key: str) -> bool:
        return bool(micro.get(key, {}).get("detected"))
    
    def _conf(key: str):
        return micro.get(key, {}).get("confidence")
    
    return {
        "mp_volume_divergence_detected": _flag("volume_divergence"),
        "mp_volume_divergence_confidence": _conf("volume_divergence"),
        "mp_order_book_squeeze_detected": _flag("order_book_squeeze"),
        "mp_order_book_squeeze_confidence": _conf("order_book_squeeze"),
        "mp_whale_stealth_accumulation_detected": _flag("whale_stealth_accumulation"),
        "mp_whale_stealth_accumulation_confidence": _conf("whale_stealth_accumulation"),
        "mp_momentum_acceleration_detected": _flag("momentum_acceleration"),
        "mp_momentum_acceleration_confidence": _conf("momentum_acceleration"),
        "mp_microstructure_shift_detected": _flag("microstructure_shift"),
        "mp_microstructure_shift_confidence": _conf("microstructure_shift"),
    }


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
    micro_pattern_data: Dict[str, Any],
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

    # Add micro-pattern data (same for all minutes of a trade)
    for key, value in micro_pattern_data.items():
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
    micro_pattern_data = _extract_micro_pattern_data(trail_payload)
    
    # Extract second_prices summary statistics once (same for all minutes)
    second_prices_stats = _extract_second_prices_stats(trail_payload)
    
    rows = []
    for minute in range(15):
        row = _build_row_for_minute(
            buyin_id=buyin_id,
            trail_data=trail_payload,
            minute=minute,
            pattern_data=pattern_data,
            micro_pattern_data=micro_pattern_data,
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

    # Add micro-pattern columns
    micro_pattern_cols = [
        "mp_volume_divergence_detected", "mp_volume_divergence_confidence",
        "mp_order_book_squeeze_detected", "mp_order_book_squeeze_confidence",
        "mp_whale_stealth_accumulation_detected", "mp_whale_stealth_accumulation_confidence",
        "mp_momentum_acceleration_detected", "mp_momentum_acceleration_confidence",
        "mp_microstructure_shift_detected", "mp_microstructure_shift_confidence",
    ]
    columns.extend(micro_pattern_cols)
    
    # Add second_prices summary columns
    sp_cols = [
        "sp_price_count", "sp_min_price", "sp_max_price", "sp_start_price",
        "sp_end_price", "sp_price_range_pct", "sp_total_change_pct",
        "sp_volatility_pct", "sp_avg_price",
    ]
    columns.extend(sp_cols)
    
    return columns


def insert_trail_rows_duckdb(buyin_id: int, rows: List[Dict[str, Any]]) -> bool:
    """Insert trail rows into PostgreSQL.
    
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
    placeholders = ", ".join(["%s" for _ in columns])
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Check if data already exists first
                cursor.execute(
                    "SELECT COUNT(*) as count FROM buyin_trail_minutes WHERE buyin_id = %s",
                    [buyin_id]
                )
                existing = cursor.fetchone()
                existing_count = existing['count'] if existing else 0
                
                if existing_count > 0:
                    logger.debug(f"Trail data already exists for buyin_id={buyin_id}, skipping")
                    return True
                
                # Insert all rows
                inserted_count = 0
                for row in rows:
                    # Convert numpy types to Python native types
                    values = []
                    for col in columns:
                        val = row.get(col)
                        # Convert numpy types to Python native types
                        if val is not None and hasattr(val, 'item'):
                            val = val.item()  # Convert numpy scalar to Python scalar
                        values.append(val)
                    
                    cursor.execute(
                        f"INSERT INTO buyin_trail_minutes ({col_list}) VALUES ({placeholders})",
                        values
                    )
                    inserted_count += 1
                
                conn.commit()
                
                # Verify
                cursor.execute(
                    "SELECT COUNT(*) as count FROM buyin_trail_minutes WHERE buyin_id = %s",
                    [buyin_id]
                )
                verify = cursor.fetchone()
                verify_count = verify['count'] if verify else 0
        
        logger.info(f"✅ PERSISTED to PostgreSQL: {inserted_count} trail rows for buyin_id={buyin_id}, verified: {verify_count}")
        return True
            
    except Exception as e:
        logger.error(f"Failed to write trail data to PostgreSQL for buyin_id={buyin_id}: {e}")
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
    """Insert trail data for a buyin into PostgreSQL.
    
    This is the main entry point for persisting trail data.
    
    Args:
        buyin_id: The ID of the buyin record
        trail_payload: The trail data payload from generate_trail_payload()
        
    Returns:
        True if PostgreSQL insert succeeded, False otherwise
    """
    # Ensure tables exist
    ensure_trail_tables_exist()
    
    # Flatten the trail payload into rows
    rows = flatten_trail_to_rows(buyin_id, trail_payload)
    
    if not rows:
        logger.warning(f"No rows generated for buyin_id={buyin_id}")
        return False
    
    # Insert into PostgreSQL (primary and only database)
    postgres_success = insert_trail_rows_duckdb(buyin_id, rows)
    
    if postgres_success:
        logger.info(f"✓ Inserted {len(rows)} trail rows for buyin_id={buyin_id}")
    else:
        logger.error(f"✗ Failed to insert trail rows for buyin_id={buyin_id}")
    
    # Also insert normalized filter values for filter analysis
    filter_success = insert_filter_values(buyin_id, rows, trail_payload)
    if filter_success:
        logger.debug(f"✓ Inserted filter values for buyin_id={buyin_id}")
    
    return postgres_success


# =============================================================================
# FILTER VALUES - Normalized filter storage for filter analysis
# =============================================================================

# Column to section mapping
COLUMN_SECTION_MAP = {
    "pm_": "price_movements",
    "ob_": "order_book",
    "tx_": "transactions",
    "wh_": "whale_activity",
    "btc_": "btc_correlation",
    "eth_": "eth_correlation",
    "pat_": "patterns",
    "mp_": "micro_patterns",
    "sp_": "second_prices",
}


def _get_section_for_column(column_name: str) -> str:
    """Determine section from column prefix."""
    for prefix, section in COLUMN_SECTION_MAP.items():
        if column_name.startswith(prefix):
            return section
    return "unknown"


def _get_filterable_columns() -> List[str]:
    """Get all filterable column names from the field mappings."""
    columns = []
    columns.extend(PRICE_MOVEMENTS_FIELDS.values())
    columns.extend(BTC_PRICE_FIELDS.values())
    columns.extend(ETH_PRICE_FIELDS.values())
    columns.extend(ORDER_BOOK_FIELDS.values())
    columns.extend(TRANSACTIONS_FIELDS.values())
    columns.extend(WHALE_ACTIVITY_FIELDS.values())
    
    # Add pattern columns (numeric ones only)
    pattern_cols = [
        "pat_breakout_score",
        "pat_asc_tri_confidence", "pat_asc_tri_resistance_level",
        "pat_asc_tri_support_level", "pat_asc_tri_compression_ratio",
        "pat_bull_flag_confidence", "pat_bull_flag_pole_height_pct",
        "pat_bull_flag_retracement_pct",
        "pat_bull_pennant_confidence", "pat_bull_pennant_compression_ratio",
        "pat_fall_wedge_confidence", "pat_fall_wedge_contraction",
        "pat_cup_handle_confidence", "pat_cup_handle_depth_pct",
        "pat_inv_hs_confidence", "pat_inv_hs_neckline",
    ]
    columns.extend(pattern_cols)

    # Add micro-pattern columns (confidence + detected flags)
    micro_pattern_cols = [
        "mp_volume_divergence_detected", "mp_volume_divergence_confidence",
        "mp_order_book_squeeze_detected", "mp_order_book_squeeze_confidence",
        "mp_whale_stealth_accumulation_detected", "mp_whale_stealth_accumulation_confidence",
        "mp_momentum_acceleration_detected", "mp_momentum_acceleration_confidence",
        "mp_microstructure_shift_detected", "mp_microstructure_shift_confidence",
    ]
    columns.extend(micro_pattern_cols)
    
    # Add second prices columns
    sp_cols = [
        "sp_price_count", "sp_min_price", "sp_max_price", "sp_start_price",
        "sp_end_price", "sp_price_range_pct", "sp_total_change_pct",
        "sp_volatility_pct", "sp_avg_price",
    ]
    columns.extend(sp_cols)
    
    return columns


def _is_ratio_by_name(column_name: str) -> int:
    """Heuristic to classify ratio-style fields."""
    name = column_name.lower()
    return 1 if any(token in name for token in ["pct", "ratio", "bps", "share", "acceleration"]) else 0


def _build_is_ratio_map(trail_payload: Optional[Dict[str, Any]]) -> Dict[str, int]:
    """Build a column -> is_ratio map using trail payload field_types metadata."""
    if not trail_payload:
        return {}
    
    section_maps = {
        "price_movements": PRICE_MOVEMENTS_FIELDS,
        "order_book_signals": ORDER_BOOK_FIELDS,
        "transactions": TRANSACTIONS_FIELDS,
        "whale_activity": WHALE_ACTIVITY_FIELDS,
        "btc_price_movements": BTC_PRICE_FIELDS,
        "eth_price_movements": ETH_PRICE_FIELDS,
    }
    
    is_ratio_map: Dict[str, int] = {}
    for section_key, field_map in section_maps.items():
        rows = trail_payload.get(section_key) or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            field_types = row.get("field_types")
            if not isinstance(field_types, dict):
                continue
            for json_field, is_ratio in field_types.items():
                column_name = field_map.get(json_field)
                if column_name:
                    is_ratio_map[column_name] = 1 if is_ratio else 0
    
    return is_ratio_map


def insert_filter_values(
    buyin_id: int,
    wide_rows: List[Dict[str, Any]],
    trail_payload: Optional[Dict[str, Any]] = None
) -> bool:
    """Insert normalized filter values into trade_filter_values table.
    
    Converts wide format rows (one row per minute with many columns)
    into long format rows (one row per filter-minute combination).
    
    Args:
        buyin_id: The ID of the buyin record
        wide_rows: List of wide-format rows from flatten_trail_to_rows()
        
    Returns:
        True if insertion succeeded, False otherwise
    """
    if not wide_rows:
        return False
    
    filterable_columns = _get_filterable_columns()
    is_ratio_map = _build_is_ratio_map(trail_payload)
    
    try:
        # Check if data already exists
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) as count FROM trade_filter_values WHERE buyin_id = %s",
                    [buyin_id]
                )
                existing = cursor.fetchone()
                existing_count = existing['count'] if existing else 0
                
                if existing_count > 0:
                    logger.debug(f"Filter values already exist for buyin_id={buyin_id}, skipping")
                    return True
        
        # Generate unique IDs using timestamp
        import time
        base_id = int(time.time() * 1000000)  # Microsecond precision
        id_counter = 0
        
        # Convert each wide row to multiple normalized rows
        inserted_count = 0
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for row in wide_rows:
                    minute = row.get("minute", 0)
                    
                    for col_name in filterable_columns:
                        value = row.get(col_name)
                        
                        # Skip null values to save space
                        if value is None:
                            continue
                        
                        # Skip non-numeric values
                        if not isinstance(value, (int, float)):
                            continue
                        
                        section = _get_section_for_column(col_name)
                        
                        # Insert the normalized row
                        is_ratio = is_ratio_map.get(col_name, _is_ratio_by_name(col_name))
                        
                        cursor.execute("""
                            INSERT INTO trade_filter_values 
                            (id, buyin_id, minute, filter_name, filter_value, is_ratio, section)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, [
                            base_id + id_counter,
                            buyin_id,
                            minute,
                            col_name,
                            float(value),
                            is_ratio,
                            section,
                        ])
                        id_counter += 1
                        inserted_count += 1
                
                conn.commit()
        
        logger.info(f"✓ Inserted {inserted_count} filter values for buyin_id={buyin_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to insert filter values for buyin_id={buyin_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


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
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM buyin_trail_minutes
                    WHERE buyin_id = %s
                    ORDER BY minute ASC
                """, [buyin_id])
                
                rows = cursor.fetchall()
                # RealDictCursor returns dicts directly
                return rows if rows else []
            
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
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM buyin_trail_minutes
                    WHERE buyin_id = %s AND minute = %s
                    LIMIT 1
                """, [buyin_id, minute])
                
                row = cursor.fetchone()
                # RealDictCursor returns dict directly
                return row if row else None
            
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
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM buyin_trail_minutes WHERE buyin_id = %s", [buyin_id])
            conn.commit()
        duckdb_success = True
    except Exception as e:
        logger.warning(f"PostgreSQL delete failed for buyin_id={buyin_id}: {e}")
    
    return duckdb_success


"""
Pattern Validator
=================
Validate buy-in signals against schema-based rules and project filters.

This module evaluates 15-minute trail data to determine if a buy-in signal
passes all required stages and risk filters.

Usage:
    from _000trading.pattern_validator import validate_buyin_signal
    
    result = validate_buyin_signal(buyin_id=123, play_id=46)
    print(result['decision'])  # 'GO' or 'NO_GO'
"""

from __future__ import annotations

import copy
import json
import logging
import os
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_duckdb, get_mysql

# Import trail generator
from _000trading.trail_generator import (
    generate_trail_payload,
    TrailError,
    make_json_serializable,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Default pattern schema - Staged evaluation approach
DEFAULT_PATTERN_SCHEMA = {
    "window": {"minutes": 15},
    
    "stages": [
        {
            "name": "setup",
            "description": "Absorption and coiling structure (first ~10 minutes).",
            "all": [
                {"metric": "transactions.buy_trade_pct", "agg": "avg", "op": ">=", "value": 55, "lookback_min": 10},
                {"metric": "transactions.whale_volume_pct", "agg": "avg", "op": ">=", "value": 40, "lookback_min": 10},
                {"metric": "transactions.perp_dominance_pct", "agg": "avg", "op": ">=", "value": 75, "lookback_min": 10},
                {"metric": "price_movements.price_vs_ma5_pct", "agg": "avg", "op": "<=", "value": 0.2, "lookback_min": 10}
            ]
        },
        {
            "name": "trigger",
            "description": "Flow shift and aggression increase (last 3 minutes).",
            "window_override": {"lookback_min": 3},
            "at_minute_from_end": 3,
            "all": [],
            "any": [
                {"metric": "order_book.net_flow_5m", "agg": "diff", "op": ">=", "value": 3000},
                {"metric": "transactions.buy_sell_pressure", "agg": "last", "op": ">=", "value": 0.25},
                {"metric": "order_book.aggression_ratio", "agg": "last", "op": ">=", "value": 2}
            ]
        },
        {
            "name": "confirm",
            "description": "Breakout validation in final minute.",
            "window_override": {"lookback_min": 1},
            "at_minute_from_end": 1,
            "all": [
                {"metric": "price_movements.close_price", "agg": "diff", "op": ">=", "value": 0.05},
                {"metric": "transactions.buy_sell_pressure", "agg": "last", "op": ">=", "value": 0.20}
            ]
        }
    ],
    
    "decision": {
        "logic": "ALL_STAGES_PASS",
        "on_pass": "GO",
        "on_fail": "NO_GO"
    }
}

DEFAULT_WINDOW_MINUTES = DEFAULT_PATTERN_SCHEMA.get("window", {}).get("minutes", 15)

# Cache for schemas
_SCHEMA_CACHE: Dict[int, Dict[str, Any]] = {}
_SCHEMA_SOURCE_CACHE: Dict[int, str] = {}

# Section prefix mapping for project filters
SECTION_PREFIX_MAP = {
    "pm_": "price_movements",
    "tx_": "transactions",
    "ob_": "order_book_signals",
    "wh_": "whale_activity",
    "sp_": "second_prices",
    "pat_": "patterns",
}


# =============================================================================
# SCHEMA LOADING
# =============================================================================

def _merge_schema_dicts(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge schema dictionaries (dict keys only, lists replaced)."""
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_schema_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def _fetch_pattern_schema_from_db(play_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve pattern schema JSON for a play from the database."""
    try:
        with get_duckdb("central") as conn:
            result = conn.execute("""
                SELECT pattern_validator, pattern_validator_enable
                FROM follow_the_goat_plays
                WHERE id = ?
            """, [play_id]).fetchone()
            
            if not result:
                logger.info("Play %s not found while loading pattern schema", play_id)
                return None
            
            raw_schema = result[0]
            enable_flag = result[1] if len(result) > 1 else 0
            
            if enable_flag != 1:
                logger.debug("Pattern schema disabled for play %s (enable flag=%s)", play_id, enable_flag)
                return None
            
            if raw_schema in (None, '', b'', bytearray()):
                logger.debug("Pattern schema empty for play %s; using default schema", play_id)
                return {}
            
            if isinstance(raw_schema, (bytes, bytearray)):
                raw_schema = raw_schema.decode('utf-8')
            
            schema = json.loads(raw_schema)
            if not isinstance(schema, dict):
                logger.warning("Pattern schema for play %s is not a JSON object", play_id)
                return None
            
            return schema
            
    except Exception as exc:
        logger.error("Error loading pattern schema for play %s: %s", play_id, exc)
        return None


def clear_schema_cache(play_id: Optional[int] = None) -> Dict[str, Any]:
    """Clear the schema cache.
    
    Args:
        play_id: If provided, clear only this play's cache. If None, clear all.
        
    Returns:
        Dict with cache clearing stats
    """
    global _SCHEMA_CACHE, _SCHEMA_SOURCE_CACHE
    
    if play_id is not None:
        if play_id in _SCHEMA_CACHE:
            del _SCHEMA_CACHE[play_id]
            logger.info("Cleared schema cache for play_id=%s", play_id)
        if play_id in _SCHEMA_SOURCE_CACHE:
            del _SCHEMA_SOURCE_CACHE[play_id]
        return {
            "status": "success",
            "action": "cleared_single",
            "play_id": play_id,
            "message": f"Cache cleared for play_id={play_id}"
        }
    else:
        count = len(_SCHEMA_CACHE)
        _SCHEMA_CACHE.clear()
        _SCHEMA_SOURCE_CACHE.clear()
        logger.info("Cleared entire schema cache (%d entries)", count)
        return {
            "status": "success",
            "action": "cleared_all",
            "entries_cleared": count,
            "message": f"Cleared all schema cache ({count} entries)"
        }


def load_pattern_schema(play_id: Optional[int]) -> Tuple[Dict[str, Any], str]:
    """Load pattern schema for a play, falling back to default schema.

    Returns:
        Tuple of (schema, source) where source is 'default' or 'custom'.
    """
    if not play_id or play_id <= 0:
        return copy.deepcopy(DEFAULT_PATTERN_SCHEMA), 'default'

    if play_id in _SCHEMA_CACHE:
        return copy.deepcopy(_SCHEMA_CACHE[play_id]), _SCHEMA_SOURCE_CACHE.get(play_id, 'custom')

    custom_schema = _fetch_pattern_schema_from_db(play_id)

    if custom_schema:
        merged_schema = _merge_schema_dicts(DEFAULT_PATTERN_SCHEMA, custom_schema)
        _SCHEMA_CACHE[play_id] = merged_schema
        _SCHEMA_SOURCE_CACHE[play_id] = 'custom'
        return copy.deepcopy(merged_schema), 'custom'

    # Either empty or failed to load - cache default fallback
    _SCHEMA_CACHE[play_id] = copy.deepcopy(DEFAULT_PATTERN_SCHEMA)
    _SCHEMA_SOURCE_CACHE[play_id] = 'default'
    return copy.deepcopy(DEFAULT_PATTERN_SCHEMA), 'default'


# =============================================================================
# METRIC EXTRACTION AND AGGREGATION
# =============================================================================

def extract_metric_values(
    trail_data: Dict[str, Any],
    source: str,
    metric_name: str,
    lookback_min: int,
    at_minute_from_end: Optional[int] = None
) -> Tuple[List[float], Dict[str, int]]:
    """Extract metric values from trail data for the lookback window."""
    source_key_map = {
        "transactions": "transactions",
        "order_book": "order_book_signals",
        "whale_activity": "whale_activity",
        "price_movements": "price_movements"
    }
    
    source_key = source_key_map.get(source)
    if not source_key or source_key not in trail_data:
        logger.warning(f"Source {source} not found in trail data")
        return [], {"startIndex": 0, "endIndex": 0, "minutes": 0}
    
    records = trail_data[source_key]
    if not records:
        return [], {"startIndex": 0, "endIndex": 0, "minutes": 0}
    
    # Records are ordered DESC (most recent first), so we reverse to get chronological order
    records = list(reversed(records))
    total_records = len(records)
    
    # Determine window based on at_minute_from_end
    if at_minute_from_end is not None:
        end_index = total_records - at_minute_from_end
        start_index = max(0, end_index - lookback_min)
    else:
        end_index = total_records
        start_index = max(0, total_records - lookback_min)
    
    window_records = records[start_index:end_index]
    window_info = {
        "startIndex": start_index,
        "endIndex": end_index,
        "minutes": len(window_records)
    }
    
    values = []
    for record in window_records:
        value = record.get(metric_name)
        if value is not None:
            try:
                values.append(float(value))
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {metric_name}={value} to float")
                continue
    
    return values, window_info


def apply_aggregation(values: List[float], agg: str) -> Optional[float]:
    """Apply aggregation function to metric values."""
    if not values:
        return None
    
    if agg == "last":
        return values[0]
    elif agg == "avg":
        return statistics.mean(values)
    elif agg == "min":
        return min(values)
    elif agg == "max":
        return max(values)
    elif agg == "sum":
        return sum(values)
    elif agg == "diff":
        if len(values) < 2:
            return 0.0
        return values[0] - values[-1]
    elif agg == "slope":
        if len(values) < 2:
            return 0.0
        n = len(values)
        x = list(range(n))
        x_mean = statistics.mean(x)
        y_mean = statistics.mean(values)
        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        return numerator / denominator if denominator != 0 else 0.0
    elif agg == "stdev":
        if len(values) < 2:
            return 0.0
        return statistics.stdev(values)
    else:
        logger.warning(f"Unknown aggregation function: {agg}")
        return None


def evaluate_operator(actual: float, op: str, expected: Any) -> bool:
    """Evaluate a condition operator."""
    if op == ">=":
        return actual >= expected
    elif op == "<=":
        return actual <= expected
    elif op == "==":
        return actual == expected
    elif op == "!=":
        return actual != expected
    elif op == ">":
        return actual > expected
    elif op == "<":
        return actual < expected
    elif op == "between":
        if isinstance(expected, (list, tuple)) and len(expected) == 2:
            return expected[0] <= actual <= expected[1]
        else:
            logger.warning(f"Between operator requires [min, max] range, got {expected}")
            return False
    else:
        logger.warning(f"Unknown operator: {op}")
        return False


def evaluate_condition(
    trail_data: Dict[str, Any],
    condition: Dict[str, Any],
    at_minute_from_end: Optional[int] = None
) -> Dict[str, Any]:
    """Evaluate a single condition against trail data."""
    metric = condition["metric"]
    agg = condition["agg"]
    op = condition["op"]
    threshold = condition["value"]
    lookback_min = condition.get("lookback_min", 3)
    
    if "." in metric:
        source, metric_name = metric.split(".", 1)
    else:
        source = "transactions"
        metric_name = metric
    
    values, window_info = extract_metric_values(
        trail_data, source, metric_name, lookback_min, at_minute_from_end
    )
    
    result = {
        "metric": metric,
        "agg": agg,
        "window": window_info,
        "op": op,
        "threshold": threshold,
        "value": None,
        "pass": False,
        "note": None,
        "weight": condition.get("weight"),
    }
    
    if not values:
        result["note"] = f"No data available for {lookback_min}m lookback"
        return result
    
    computed = apply_aggregation(values, agg)
    
    if computed is None:
        result["note"] = f"Failed to compute {agg}"
        return result
    
    result["value"] = round(computed, 6)
    passed = evaluate_operator(computed, op, threshold)
    result["pass"] = passed
    result["note"] = f"{agg}({metric})={computed:.4f} {op} {threshold} → {'PASS' if passed else 'FAIL'}"
    
    return result


def evaluate_stage(
    trail_data: Dict[str, Any],
    stage: Dict[str, Any]
) -> Dict[str, Any]:
    """Evaluate a single stage."""
    stage_name = stage.get("name", "unnamed")
    all_conditions = stage.get("all", [])
    any_conditions = stage.get("any", [])
    at_minute_from_end = stage.get("at_minute_from_end")
    
    checks = []
    any_checks_results: List[Dict[str, Any]] = []
    stage_window = None
    
    # Evaluate ALL conditions
    all_pass = True
    for condition in all_conditions:
        check_result = evaluate_condition(trail_data, condition, at_minute_from_end)
        checks.append(check_result)
        
        if stage_window is None:
            stage_window = check_result["window"]
        
        if not check_result["pass"]:
            all_pass = False
    
    # Evaluate ANY conditions
    any_pass = len(any_conditions) == 0
    for condition in any_conditions:
        check_result = evaluate_condition(trail_data, condition, at_minute_from_end)
        any_checks_results.append(check_result)
        
        if stage_window is None:
            stage_window = check_result["window"]
        
        if check_result["pass"]:
            any_pass = True
    
    result = {
        "name": stage_name,
        "window": stage_window or {"startIndex": 0, "endIndex": 0, "minutes": 0},
        "checks": checks,
        "any_checks": any_checks_results,
        "all_pass": all_pass,
        "any_pass": any_pass,
        "pass": all_pass and any_pass
    }
    
    return result


# =============================================================================
# PROJECT FILTER VALIDATION
# =============================================================================

def _fetch_project_filters(project_id: int) -> List[Dict[str, Any]]:
    """Fetch active filters for a pattern config project from the database."""
    try:
        with get_mysql() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, name, section, minute, field_name, field_column, 
                       from_value, to_value, is_active
                FROM pattern_config_filters
                WHERE project_id = %s AND is_active = 1
                ORDER BY id ASC
            """, (project_id,))
            filters = cursor.fetchall()
            cursor.close()
            return filters
    except Exception as e:
        logger.error("Failed to fetch project filters for project_id=%s: %s", project_id, e)
        return []


def _get_section_from_field_column(field_column: str) -> Optional[str]:
    """Parse the section from a field_column using the prefix mapping."""
    if not field_column:
        return None
    
    for prefix, section in SECTION_PREFIX_MAP.items():
        if field_column.startswith(prefix):
            return section
    
    return None


def _get_field_name_from_column(field_column: str) -> Optional[str]:
    """Extract the field name from a field_column by removing the section prefix."""
    if not field_column:
        return None
    
    for prefix in SECTION_PREFIX_MAP.keys():
        if field_column.startswith(prefix):
            return field_column[len(prefix):]
    
    return field_column


def _extract_pattern_data_flat(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract pattern detection data from trail and flatten to field names."""
    patterns = trail_data.get("patterns", {})
    
    if not isinstance(patterns, dict):
        return {}
    
    result = {
        "breakout_score": patterns.get("breakout_score"),
        "detected_count": len(patterns.get("detected", [])),
    }
    
    # Ascending Triangle
    asc_tri = patterns.get("ascending_triangle", {})
    result["asc_tri_detected"] = bool(asc_tri.get("detected"))
    result["asc_tri_confidence"] = asc_tri.get("confidence")
    
    # Bullish Flag
    bull_flag = patterns.get("bullish_flag", {})
    result["bull_flag_detected"] = bool(bull_flag.get("detected"))
    result["bull_flag_confidence"] = bull_flag.get("confidence")
    
    # Bullish Pennant
    bull_pennant = patterns.get("bullish_pennant", {})
    result["bull_pennant_detected"] = bool(bull_pennant.get("detected"))
    result["bull_pennant_confidence"] = bull_pennant.get("confidence")
    
    # Falling Wedge
    fall_wedge = patterns.get("falling_wedge", {})
    result["fall_wedge_detected"] = bool(fall_wedge.get("detected"))
    result["fall_wedge_confidence"] = fall_wedge.get("confidence")
    
    # Swing Structure
    swing = patterns.get("swing_structure", {})
    result["swing_trend"] = swing.get("trend")
    result["swing_higher_lows"] = bool(swing.get("higher_lows"))
    result["swing_lower_highs"] = bool(swing.get("lower_highs"))
    
    return result


def _calculate_second_prices_aggregates(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate aggregate statistics from second_prices data."""
    second_prices = trail_data.get("second_prices", [])
    
    if not second_prices or not isinstance(second_prices, list):
        return {}
    
    prices = []
    for row in second_prices:
        price = row.get("price")
        if price is not None:
            try:
                prices.append(float(price))
            except (ValueError, TypeError):
                continue
    
    if len(prices) < 2:
        return {}
    
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / len(prices)
    current_price = prices[-1]
    
    price_range = max_price - min_price
    price_range_pct = (price_range / avg_price * 100) if avg_price > 0 else 0
    
    variance = sum((p - avg_price) ** 2 for p in prices) / len(prices)
    std_dev = variance ** 0.5
    volatility_pct = (std_dev / avg_price * 100) if avg_price > 0 else 0
    
    price_trend_pct = ((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] > 0 else 0
    
    return {
        "min_price": min_price,
        "max_price": max_price,
        "avg_price": avg_price,
        "price_range_pct": price_range_pct,
        "volatility_pct": volatility_pct,
        "price_trend_pct": price_trend_pct,
    }


def _find_minute_data(
    trail_data: Dict[str, Any],
    section: str,
    minute: int
) -> Optional[Dict[str, Any]]:
    """Find the data row for a specific minute in a trail section."""
    if section == "second_prices":
        return _calculate_second_prices_aggregates(trail_data)
    
    if section == "patterns":
        return _extract_pattern_data_flat(trail_data)
    
    section_data = trail_data.get(section, [])
    if not isinstance(section_data, list):
        return None
    
    for row in section_data:
        if isinstance(row, dict) and row.get("minute_span_from") == minute:
            return row
    
    return None


def _evaluate_filter_condition(
    value: Optional[float],
    from_value: Optional[float],
    to_value: Optional[float]
) -> bool:
    """Check if a value is within the filter range."""
    if value is None:
        return False
    
    try:
        value = float(value)
    except (ValueError, TypeError):
        return False
    
    if from_value is not None:
        try:
            if value < float(from_value):
                return False
        except (ValueError, TypeError):
            pass
    
    if to_value is not None:
        try:
            if value > float(to_value):
                return False
        except (ValueError, TypeError):
            pass
    
    return True


def validate_with_project_filters(
    trail_data: Dict[str, Any],
    project_id: int,
    play_id: int
) -> Dict[str, Any]:
    """Validate trail data against project filters."""
    logger.info("Validating with project filters for project_id=%s, play_id=%s", project_id, play_id)
    
    filters = _fetch_project_filters(project_id)
    
    if not filters:
        logger.warning("No active filters found for project_id=%s, defaulting to NO_GO", project_id)
        return {
            "decision": "NO_GO",
            "reason": "no_active_filters",
            "filter_results": [],
            "filters_passed": 0,
            "filters_total": 0,
            "filters_failed": 0,
            "project_id": project_id,
            "play_id": play_id,
        }
    
    filter_results = []
    filters_passed = 0
    filters_failed = 0
    
    for filter_def in filters:
        filter_id = filter_def.get("id")
        filter_name = filter_def.get("name", "")
        field_column = filter_def.get("field_column", "")
        field_name = filter_def.get("field_name", "")
        from_value = filter_def.get("from_value")
        to_value = filter_def.get("to_value")
        minute = filter_def.get("minute")
        
        if minute is None:
            minute = 0
        else:
            minute = int(minute)
        
        section = _get_section_from_field_column(field_column)
        if not section:
            section = filter_def.get("section")
            if section:
                section_map = {
                    "price_movements": "price_movements",
                    "transactions": "transactions", 
                    "order_book_signals": "order_book_signals",
                    "whale_activity": "whale_activity",
                    "second_prices": "second_prices",
                    "patterns": "patterns",
                }
                section = section_map.get(section, section)
        
        if not section:
            logger.warning("Could not determine section for filter id=%s", filter_id)
            filter_results.append({
                "filter_id": filter_id,
                "filter_name": filter_name,
                "field": field_column or field_name,
                "minute": minute,
                "from_value": float(from_value) if from_value is not None else None,
                "to_value": float(to_value) if to_value is not None else None,
                "actual_value": None,
                "passed": False,
                "error": "unknown_section",
            })
            filters_failed += 1
            continue
        
        minute_data = _find_minute_data(trail_data, section, minute)
        
        if minute_data is None:
            logger.debug("No data found for section=%s, minute=%s", section, minute)
            filter_results.append({
                "filter_id": filter_id,
                "filter_name": filter_name,
                "field": field_column or field_name,
                "section": section,
                "minute": minute,
                "from_value": float(from_value) if from_value is not None else None,
                "to_value": float(to_value) if to_value is not None else None,
                "actual_value": None,
                "passed": False,
                "error": "no_minute_data",
            })
            filters_failed += 1
            continue
        
        lookup_field = _get_field_name_from_column(field_column) or field_name
        actual_value = minute_data.get(lookup_field)
        
        passed = _evaluate_filter_condition(actual_value, from_value, to_value)
        
        filter_result = {
            "filter_id": filter_id,
            "filter_name": filter_name,
            "field": field_column or field_name,
            "section": section,
            "minute": minute,
            "from_value": float(from_value) if from_value is not None else None,
            "to_value": float(to_value) if to_value is not None else None,
            "actual_value": float(actual_value) if actual_value is not None else None,
            "passed": passed,
        }
        
        filter_results.append(filter_result)
        
        if passed:
            filters_passed += 1
        else:
            filters_failed += 1
    
    all_pass = filters_failed == 0 and filters_passed > 0
    decision = "GO" if all_pass else "NO_GO"
    
    reason = "all_filters_passed" if all_pass else f"{filters_failed}_of_{len(filters)}_filters_failed"
    
    logger.info(
        "Project filter validation complete: decision=%s, passed=%s, failed=%s, total=%s",
        decision, filters_passed, filters_failed, len(filters)
    )
    
    return {
        "decision": decision,
        "reason": reason,
        "filter_results": filter_results,
        "filters_passed": filters_passed,
        "filters_failed": filters_failed,
        "filters_total": len(filters),
        "project_id": project_id,
        "play_id": play_id,
    }


def save_filter_results_to_db(
    buyin_id: int,
    play_id: int,
    project_results: List[Dict[str, Any]]
) -> bool:
    """Persist filter evaluation results to the trade_filter_results table."""
    try:
        with get_mysql() as conn:
            cursor = conn.cursor()
            
            insert_sql = """
                INSERT INTO trade_filter_results (
                    buyin_id, play_id, project_id, filter_id, filter_name,
                    field_column, section, minute, from_value, to_value,
                    actual_value, passed, error
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            rows_to_insert = []
            for project_result in project_results:
                project_id = project_result.get('project_id')
                filter_results = project_result.get('filter_results', [])
                
                for fr in filter_results:
                    rows_to_insert.append((
                        buyin_id,
                        play_id,
                        project_id,
                        fr.get('filter_id'),
                        fr.get('filter_name'),
                        fr.get('field'),
                        fr.get('section'),
                        fr.get('minute', 0),
                        fr.get('from_value'),
                        fr.get('to_value'),
                        fr.get('actual_value'),
                        1 if fr.get('passed') else 0,
                        fr.get('error'),
                    ))
            
            if rows_to_insert:
                cursor.executemany(insert_sql, rows_to_insert)
                conn.commit()
                logger.info("Saved %d filter results for buyin_id=%s", len(rows_to_insert), buyin_id)
            
            cursor.close()
            return True
            
    except Exception as exc:
        logger.error("Failed to save filter results for buyin_id=%s: %s", buyin_id, exc)
        return False


def validate_with_multiple_projects(
    trail_data: Dict[str, Any],
    project_ids: List[int],
    play_id: int,
    buyin_id: int,
    save_results: bool = True
) -> Dict[str, Any]:
    """Validate trail data against multiple projects.
    
    Returns GO if ANY project's filters ALL pass (OR logic between projects, AND within).
    """
    logger.info(
        "Validating with multiple projects for buyin_id=%s, play_id=%s, projects=%s",
        buyin_id, play_id, project_ids
    )
    
    if not project_ids:
        logger.warning("No project IDs provided for multi-project validation")
        return {
            "decision": "NO_GO",
            "reason": "no_projects_configured",
            "project_results": [],
            "projects_evaluated": 0,
            "projects_passed": 0,
            "winning_project_id": None,
            "play_id": play_id,
            "buyin_id": buyin_id,
        }
    
    all_project_results = []
    any_project_passed = False
    winning_project_id = None
    
    for project_id in project_ids:
        result = validate_with_project_filters(trail_data, project_id, play_id)
        result['project_id'] = project_id
        all_project_results.append(result)
        
        if result['decision'] == 'GO':
            if not any_project_passed:
                winning_project_id = project_id
            any_project_passed = True
            logger.info(
                "Project %s PASSED for buyin_id=%s (filters: %s/%s passed)",
                project_id, buyin_id, result['filters_passed'], result['filters_total']
            )
        else:
            logger.info(
                "Project %s FAILED for buyin_id=%s (filters: %s/%s passed, %s failed)",
                project_id, buyin_id, result['filters_passed'], 
                result['filters_total'], result['filters_failed']
            )
    
    decision = "GO" if any_project_passed else "NO_GO"
    projects_passed = sum(1 for r in all_project_results if r['decision'] == 'GO')
    
    if any_project_passed:
        reason = f"project_{winning_project_id}_passed"
    else:
        reason = "all_projects_failed"
    
    if save_results:
        save_filter_results_to_db(buyin_id, play_id, all_project_results)
    
    logger.info(
        "Multi-project validation complete: decision=%s, projects_passed=%s/%s, winning_project=%s",
        decision, projects_passed, len(project_ids), winning_project_id
    )
    
    return {
        "decision": decision,
        "reason": reason,
        "project_results": all_project_results,
        "projects_evaluated": len(project_ids),
        "projects_passed": projects_passed,
        "winning_project_id": winning_project_id,
        "play_id": play_id,
        "buyin_id": buyin_id,
    }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _get_current_price(trail_data: Dict[str, Any]) -> Optional[float]:
    """Extract the most recent close price from trail data."""
    price_movements = trail_data.get("price_movements") or []
    if not isinstance(price_movements, list) or not price_movements:
        return None

    latest_entry = price_movements[0]
    close_price = latest_entry.get("close_price")
    try:
        return float(close_price) if close_price is not None else None
    except (TypeError, ValueError):
        return None


def _extract_market_context(trail_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract lightweight market context features from trail data."""
    price_movements = trail_data.get("price_movements") or []
    transactions = trail_data.get("transactions") or []

    if not isinstance(price_movements, list) or not price_movements:
        return {}

    recent_prices: List[float] = []
    for entry in price_movements[:10]:
        value = entry.get("close_price")
        if value is None:
            continue
        try:
            recent_prices.append(float(value))
        except (TypeError, ValueError):
            continue

    trend_direction = "neutral"
    recent_price_change_pct = 0.0
    if len(recent_prices) >= 2:
        latest = recent_prices[0]
        oldest = recent_prices[-1]
        if oldest != 0:
            price_change_ratio = (latest - oldest) / oldest
            recent_price_change_pct = round(price_change_ratio * 100, 2)
            if price_change_ratio > 0.002:
                trend_direction = "uptrend"
            elif price_change_ratio < -0.002:
                trend_direction = "downtrend"

    volatilities: List[float] = []
    for entry in price_movements[:10]:
        volatility = entry.get("volatility_pct")
        if volatility is None:
            continue
        try:
            volatilities.append(float(volatility))
        except (TypeError, ValueError):
            continue

    avg_volatility = round(statistics.mean(volatilities), 4) if volatilities else 0.0

    volumes: List[float] = []
    for entry in transactions[:10]:
        value = entry.get("total_volume_usd")
        if value is None:
            continue
        try:
            volumes.append(float(value))
        except (TypeError, ValueError):
            continue
    avg_volume = round(statistics.mean(volumes), 2) if volumes else 0.0

    return {
        "trend_direction": trend_direction,
        "avg_volatility_10m": avg_volatility,
        "recent_price_change_pct": recent_price_change_pct,
        "avg_volume_10m": avg_volume,
        "time_of_day_utc": datetime.utcnow().hour
    }


def _analyze_decision_quality(stages: List[Dict[str, Any]], all_pass: bool) -> Dict[str, Any]:
    """Analyze aggregate quality metrics for stage evaluation results."""
    total_checks = 0
    passed_checks = 0
    failed_checks: List[Dict[str, Any]] = []

    for stage in stages:
        stage_name = stage.get("name")

        for check in stage.get("checks", []):
            total_checks += 1
            if check.get("pass"):
                passed_checks += 1
                continue

            value = check.get("value")
            threshold = check.get("threshold")
            gap: Optional[float] = None
            if isinstance(value, (int, float)) and isinstance(threshold, (int, float)):
                gap = abs(float(value) - float(threshold))

            failed_checks.append({
                "stage": stage_name,
                "metric": check.get("metric"),
                "value": value,
                "threshold": threshold,
                "gap": gap
            })

        any_checks = stage.get("any_checks", [])
        if any_checks:
            total_checks += 1
            passed_any = any(check.get("pass") for check in any_checks)
            if passed_any:
                passed_checks += 1

    confidence = passed_checks / total_checks if total_checks else 0.0

    return {
        "confidence_score": round(confidence, 3),
        "checks_passed": passed_checks,
        "checks_total": total_checks,
        "all_failed_checks": failed_checks,
    }


# =============================================================================
# MAIN VALIDATION FUNCTION
# =============================================================================

def validate_buyin_signal(
    buyin_id: int,
    symbol: Optional[str] = None,
    lookback_minutes: Optional[int] = None,
    play_id: Optional[int] = None,
    save_to_file: bool = False,
    project_id: Optional[int] = None,
    project_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Validate a buy-in signal using schema-based rules or project filters.
    
    If project_ids (list) is provided, uses multi-project validation where
    the trade passes if ANY project's filters ALL pass.
    If project_id (single) is provided, uses single project filter validation.
    Otherwise uses the schema-based validation from the database.
    
    Args:
        buyin_id: The buy-in ID to validate
        symbol: Optional symbol override
        lookback_minutes: Optional lookback override (default: 15)
        play_id: Optional play ID to load a custom pattern schema
        save_to_file: Persist decision JSON to disk when True
        project_id: Optional single pattern config project ID (legacy)
        project_ids: Optional list of project IDs for multi-project validation
        
    Returns:
        Dict with decision ('GO' or 'NO_GO') and detailed results
    """
    projects_to_validate = []
    if project_ids:
        projects_to_validate = [pid for pid in project_ids if pid]
    elif project_id:
        projects_to_validate = [project_id]
    
    logger.info(
        "Validating buy-in signal #%s%s%s",
        buyin_id,
        f" (play_id={play_id})" if play_id else "",
        f" (projects={projects_to_validate})" if projects_to_validate else ""
    )
    
    schema_source = 'default'
    validator_version = "v1_schema_based"

    # If project_ids provided, use multi-project validation
    if projects_to_validate:
        validator_version = "v3_multi_project_filters" if len(projects_to_validate) > 1 else "v2_project_filters"
        try:
            trail_data = generate_trail_payload(
                buyin_id=buyin_id,
                symbol=symbol,
                lookback_minutes=lookback_minutes or DEFAULT_WINDOW_MINUTES
            )
            
            multi_result = validate_with_multiple_projects(
                trail_data=trail_data,
                project_ids=projects_to_validate,
                play_id=play_id or 0,
                buyin_id=buyin_id,
                save_results=True
            )
            
            market_context = _extract_market_context(trail_data)
            price_at_decision = _get_current_price(trail_data)
            
            all_filter_results = []
            total_passed = 0
            total_failed = 0
            total_filters = 0
            for pr in multi_result.get('project_results', []):
                all_filter_results.extend(pr.get('filter_results', []))
                total_passed += pr.get('filters_passed', 0)
                total_failed += pr.get('filters_failed', 0)
                total_filters += pr.get('filters_total', 0)
            
            result = {
                "buyin_id": buyin_id,
                "timestamp": datetime.utcnow().isoformat(),
                "decision": multi_result["decision"],
                "reason": multi_result["reason"],
                "stages": [],
                "symbol": symbol,
                "schema_source": "multi_project_filters" if len(projects_to_validate) > 1 else "project_filters",
                "schema_play_id": play_id,
                "project_ids": projects_to_validate,
                "winning_project_id": multi_result.get("winning_project_id"),
                "projects_evaluated": multi_result.get("projects_evaluated"),
                "projects_passed": multi_result.get("projects_passed"),
                "market_context": market_context,
                "outcomes": {
                    "status": "pending",
                    "price_at_decision": price_at_decision,
                },
                "decision_quality": {
                    "filters_passed": total_passed,
                    "filters_failed": total_failed,
                    "filters_total": total_filters,
                    "projects_evaluated": multi_result.get("projects_evaluated"),
                    "projects_passed": multi_result.get("projects_passed"),
                },
                "validator_version": validator_version,
                "project_results": multi_result.get("project_results", []),
                "filter_results": all_filter_results,
            }
            
            logger.info(
                "Multi-project filter validation complete for buy-in #%s: %s (projects_passed=%s/%s)",
                buyin_id, result["decision"], 
                multi_result.get("projects_passed"), multi_result.get("projects_evaluated")
            )
            
            return result
            
        except TrailError as e:
            logger.error(f"Trail generation error for project validation: {e}")
            return {
                "buyin_id": buyin_id,
                "timestamp": datetime.utcnow().isoformat(),
                "decision": "ERROR",
                "error": str(e),
                "error_type": "TrailError",
                "schema_play_id": play_id,
                "project_ids": projects_to_validate,
                "validator_version": validator_version,
            }
        except Exception as e:
            logger.error(f"Project filter validation error: {e}", exc_info=True)
            return {
                "buyin_id": buyin_id,
                "timestamp": datetime.utcnow().isoformat(),
                "decision": "ERROR",
                "error": str(e),
                "error_type": type(e).__name__,
                "schema_play_id": play_id,
                "project_ids": projects_to_validate,
                "validator_version": validator_version,
            }

    # Standard schema-based validation
    try:
        pattern_schema, schema_source = load_pattern_schema(play_id)
        schema_minutes = pattern_schema.get("window", {}).get("minutes", DEFAULT_WINDOW_MINUTES)

        trail_data = generate_trail_payload(
            buyin_id=buyin_id,
            symbol=symbol,
            lookback_minutes=lookback_minutes or schema_minutes
        )

        market_context = _extract_market_context(trail_data)
        price_at_decision = _get_current_price(trail_data)
        
        result = {
            "buyin_id": buyin_id,
            "timestamp": datetime.utcnow().isoformat(),
            "decision": "NO_GO",
            "stages": [],
            "symbol": symbol,
            "schema_source": schema_source,
            "schema_play_id": play_id,
            "schema_minutes": schema_minutes,
            "market_context": market_context,
            "outcomes": {
                "status": "pending",
                "price_at_decision": price_at_decision,
            },
            "decision_quality": {},
            "validator_version": validator_version,
        }
        
        # Evaluate each stage
        all_stages_pass = True
        stages = pattern_schema.get("stages", [])
        if not isinstance(stages, list):
            logger.warning("Pattern schema stages malformed")
            stages = []

        for stage_def in stages:
            stage_result = evaluate_stage(trail_data, stage_def)
            result["stages"].append(stage_result)
            
            if not stage_result["pass"]:
                all_stages_pass = False
                logger.info(f"Stage '{stage_result['name']}' FAILED")
            else:
                logger.info(f"Stage '{stage_result['name']}' PASSED")
        
        # Make final decision
        decision_config = pattern_schema.get("decision", DEFAULT_PATTERN_SCHEMA["decision"])
        on_pass = decision_config.get("on_pass", "GO")
        on_fail = decision_config.get("on_fail", "NO_GO")

        result["decision"] = on_pass if all_stages_pass else on_fail
        result["decision_quality"] = _analyze_decision_quality(result["stages"], all_stages_pass)
        
        logger.info(f"Final decision for buy-in #{buyin_id}: {result['decision']}")
        
        return result
        
    except TrailError as e:
        logger.error(f"Trail generation error: {e}")
        return {
            "buyin_id": buyin_id,
            "timestamp": datetime.utcnow().isoformat(),
            "decision": "ERROR",
            "error": str(e),
            "error_type": "TrailError",
            "schema_play_id": play_id,
            "schema_source": schema_source,
            "validator_version": validator_version,
        }
    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        return {
            "buyin_id": buyin_id,
            "timestamp": datetime.utcnow().isoformat(),
            "decision": "ERROR",
            "error": str(e),
            "error_type": type(e).__name__,
            "schema_play_id": play_id,
            "schema_source": schema_source,
            "validator_version": validator_version,
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Validate a buy-in signal")
    parser.add_argument("buyin_id", type=int, help="Buy-in ID to validate")
    parser.add_argument("--symbol", help="Override symbol")
    parser.add_argument("--minutes", type=int, help="Override lookback minutes")
    parser.add_argument("--play-id", type=int, help="Use pattern schema from follow_the_goat_plays.id")
    parser.add_argument("--project-ids", type=str, help="Comma-separated project IDs for filter validation")
    
    args = parser.parse_args()
    
    project_ids = None
    if args.project_ids:
        project_ids = [int(x.strip()) for x in args.project_ids.split(",")]
    
    result = validate_buyin_signal(
        buyin_id=args.buyin_id,
        symbol=args.symbol,
        lookback_minutes=args.minutes,
        play_id=args.play_id,
        project_ids=project_ids,
    )
    
    print(f"\nDecision: {result['decision']}")
    print(json.dumps(result, indent=2, default=str))


"""
Pre-Entry Price Movement Analysis
==================================
Calculate price movement metrics BEFORE trade entry.

This module provides functions to:
1. Get price at specific time points before entry
2. Calculate percentage changes at 1m, 2m, 3m, 5m, 10m before entry
3. Determine trend direction (rising/falling/flat)
4. Filter out falling-price entries

Key Finding from Analysis (8,515 trades):
- 10-minute window: Not in top 25 combinations (too slow for SOL)
- 3-minute window: 80-100% win rate ⭐ OPTIMAL
- 2-minute window: 62.5% win rate

Recommended Filter (Based on Jan 28, 2026 Analysis):
- pre_entry_change_3m > 0.08% → 80-100% win rate
- Catches quick reversals EARLY (7 minutes before old 10m filter)
- Perfect for SOL's fast 5-60 minute cycles
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

logger = logging.getLogger(__name__)


def get_price_before_entry(entry_time: datetime, minutes_before: int, window_seconds: int = 30) -> Optional[float]:
    """
    Get the price N minutes before entry time.
    
    Args:
        entry_time: Timestamp of entry
        minutes_before: How many minutes before entry to look
        window_seconds: Search window in seconds (default 30s = ±15s from target)
    
    Returns:
        Price as float, or None if not found
    """
    target_time = entry_time - timedelta(minutes=minutes_before)
    start_time = target_time - timedelta(seconds=window_seconds // 2)
    end_time = target_time + timedelta(seconds=window_seconds // 2)
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT price, timestamp
                    FROM prices
                    WHERE token = 'SOL'
                      AND timestamp >= %s
                      AND timestamp <= %s
                    ORDER BY timestamp ASC
                    LIMIT 1
                """, [start_time, end_time])
                result = cursor.fetchone()
                
                if result:
                    return float(result['price'])
                return None
                
    except Exception as e:
        logger.error(f"Error getting price {minutes_before}m before entry: {e}")
        return None


def calculate_pre_entry_metrics(entry_time: datetime, entry_price: float) -> Dict[str, Any]:
    """
    Calculate comprehensive pre-entry price movement metrics.
    
    Args:
        entry_time: Timestamp when trade was entered
        entry_price: Price at entry
    
    Returns:
        Dict with price movement metrics:
        {
            'pre_entry_price_1m_before': float or None,
            'pre_entry_price_2m_before': float or None,
            'pre_entry_price_3m_before': float or None,
            'pre_entry_price_5m_before': float or None,
            'pre_entry_price_10m_before': float or None,
            'pre_entry_change_1m': float or None,  # % change from 1m ago to entry
            'pre_entry_change_2m': float or None,
            'pre_entry_change_3m': float or None,
            'pre_entry_change_5m': float or None,
            'pre_entry_change_10m': float or None,
            'pre_entry_trend': str,  # 'rising', 'falling', 'flat', 'unknown'
        }
    """
    result = {
        'pre_entry_price_1m_before': None,
        'pre_entry_price_2m_before': None,
        'pre_entry_price_3m_before': None,
        'pre_entry_price_5m_before': None,
        'pre_entry_price_10m_before': None,
        'pre_entry_change_1m': None,
        'pre_entry_change_2m': None,
        'pre_entry_change_3m': None,
        'pre_entry_change_5m': None,
        'pre_entry_change_10m': None,
        'pre_entry_trend': 'unknown'
    }
    
    # Get prices at key points before entry
    price_1m_before = get_price_before_entry(entry_time, 1)
    price_2m_before = get_price_before_entry(entry_time, 2)
    price_3m_before = get_price_before_entry(entry_time, 3)
    price_5m_before = get_price_before_entry(entry_time, 5)
    price_10m_before = get_price_before_entry(entry_time, 10)
    
    # Store raw prices
    result['pre_entry_price_1m_before'] = price_1m_before
    result['pre_entry_price_2m_before'] = price_2m_before
    result['pre_entry_price_3m_before'] = price_3m_before
    result['pre_entry_price_5m_before'] = price_5m_before
    result['pre_entry_price_10m_before'] = price_10m_before
    
    # Calculate percentage changes
    if price_1m_before:
        result['pre_entry_change_1m'] = ((entry_price - price_1m_before) / price_1m_before) * 100
        
    if price_2m_before:
        result['pre_entry_change_2m'] = ((entry_price - price_2m_before) / price_2m_before) * 100
    
    if price_3m_before:
        result['pre_entry_change_3m'] = ((entry_price - price_3m_before) / price_3m_before) * 100
    
    if price_5m_before:
        result['pre_entry_change_5m'] = ((entry_price - price_5m_before) / price_5m_before) * 100
    
    if price_10m_before:
        result['pre_entry_change_10m'] = ((entry_price - price_10m_before) / price_10m_before) * 100
    
    # Determine trend direction based on 1m and 5m changes
    change_1m = result['pre_entry_change_1m']
    change_5m = result['pre_entry_change_5m']
    
    if change_1m is not None and change_5m is not None:
        # Rising: Both positive with 1m > 0.05% and 5m > 0.1%
        if change_1m > 0.05 and change_5m > 0.1:
            result['pre_entry_trend'] = 'rising'
        # Falling: Both negative with 1m < -0.05% and 5m < -0.1%
        elif change_1m < -0.05 and change_5m < -0.1:
            result['pre_entry_trend'] = 'falling'
        # Flat: Neither rising nor falling
        else:
            result['pre_entry_trend'] = 'flat'
    else:
        result['pre_entry_trend'] = 'unknown'
    
    return result


def should_enter_based_on_price_movement(
    pre_entry_metrics: Dict[str, Any],
    min_change_3m: float = 0.20  # Increased from 0.08 to prevent weak entries
) -> tuple[bool, str]:
    """
    Determine if trade should be entered based on price movement.
    
    UPDATED: Uses 3-minute window (optimal for SOL's fast cycles).
    Threshold increased to 0.20% to prevent "buying the top" entries.
    
    Args:
        pre_entry_metrics: Dict returned by calculate_pre_entry_metrics()
        min_change_3m: Minimum 3m price change % required (default 0.20%)
    
    Returns:
        Tuple of (should_enter: bool, reason: str)
    """
    change_3m = pre_entry_metrics.get('pre_entry_change_3m')
    
    if change_3m is None:
        logger.warning("No price data 3m before entry - allowing trade (no filter)")
        return True, "NO_PRICE_DATA"
    
    # CRITICAL FILTER: Price must be rising (catches quick reversals early)
    if change_3m < min_change_3m:
        logger.info(f"Trade filtered: price change 3m = {change_3m:.3f}% (need >= {min_change_3m}%)")
        return False, f"FALLING_PRICE (change_3m={change_3m:.3f}%)"
    
    logger.debug(f"Trade passes price movement filter: change_3m = {change_3m:.3f}%")
    return True, "PASS"


def log_pre_entry_analysis(pre_entry_metrics: Dict[str, Any], logger_instance: logging.Logger = None) -> None:
    """
    Log pre-entry analysis results for debugging.
    
    Args:
        pre_entry_metrics: Dict returned by calculate_pre_entry_metrics()
        logger_instance: Optional logger instance (defaults to module logger)
    """
    log = logger_instance or logger
    
    change_1m = pre_entry_metrics.get('pre_entry_change_1m')
    change_2m = pre_entry_metrics.get('pre_entry_change_2m')
    change_3m = pre_entry_metrics.get('pre_entry_change_3m')
    change_5m = pre_entry_metrics.get('pre_entry_change_5m')
    change_10m = pre_entry_metrics.get('pre_entry_change_10m')
    trend = pre_entry_metrics.get('pre_entry_trend', 'unknown')
    
    log.info(f"Pre-entry analysis:")
    log.info(f"  Trend: {trend.upper()}")
    if change_1m is not None:
        log.info(f"  1m change: {change_1m:+.3f}%")
    if change_2m is not None:
        log.info(f"  2m change: {change_2m:+.3f}%")
    if change_3m is not None:
        log.info(f"  3m change: {change_3m:+.3f}% {'✓' if change_3m >= 0.08 else '✗'} (PRIMARY FILTER)")
    if change_5m is not None:
        log.info(f"  5m change: {change_5m:+.3f}%")
    if change_10m is not None:
        log.info(f"  10m change: {change_10m:+.3f}%")


# Example usage
if __name__ == "__main__":
    # Test with recent trade
    import sys
    
    if len(sys.argv) > 1:
        buyin_id = int(sys.argv[1])
        
        # Get buyin details
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT followed_at, our_entry_price
                    FROM follow_the_goat_buyins
                    WHERE id = %s
                """, [buyin_id])
                result = cursor.fetchone()
        
        if result:
            entry_time = result['followed_at']
            entry_price = float(result['our_entry_price'])
            
            print(f"\nAnalyzing buyin #{buyin_id}")
            print(f"Entry time: {entry_time}")
            print(f"Entry price: ${entry_price:.4f}\n")
            
            metrics = calculate_pre_entry_metrics(entry_time, entry_price)
            log_pre_entry_analysis(metrics)
            
            should_enter, reason = should_enter_based_on_price_movement(metrics)
            print(f"\nDecision: {'✓ ENTER' if should_enter else '✗ REJECT'}")
            print(f"Reason: {reason}")
        else:
            print(f"Buyin #{buyin_id} not found")
    else:
        print("Usage: python pre_entry_price_movement.py <buyin_id>")

"""
Generate Price Movement Visualization for Trades
=================================================
This script generates a visualization showing how price movement before entry
correlates with trade outcomes. Can be used to enhance the visual-trades page.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import json

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres


def analyze_specific_trade(buyin_id: int) -> Dict:
    """
    Analyze a specific trade and return detailed price movement analysis.
    """
    # Get trade details
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    followed_at,
                    our_entry_price,
                    potential_gains,
                    our_status,
                    price_cycle
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, [buyin_id])
            trade = cursor.fetchone()
    
    if not trade:
        return {"error": "Trade not found"}
    
    entry_time = trade['followed_at']
    entry_price = float(trade['our_entry_price'])
    
    # Get price data before entry (10 minutes before to 60 minutes after)
    start_time = entry_time - timedelta(minutes=10)
    end_time = entry_time + timedelta(minutes=60)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, price
                FROM prices
                WHERE token = 'SOL'
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY timestamp ASC
            """, [start_time, end_time])
            prices = cursor.fetchall()
    
    # Calculate price movement metrics
    prices_data = []
    for p in prices:
        minutes_from_entry = (p['timestamp'] - entry_time).total_seconds() / 60
        prices_data.append({
            'timestamp': p['timestamp'].isoformat(),
            'price': float(p['price']),
            'minutes_from_entry': round(minutes_from_entry, 2)
        })
    
    # Find price at key points before entry
    price_1m_before = None
    price_2m_before = None
    price_5m_before = None
    price_10m_before = None
    
    for p in prices:
        minutes_diff = (entry_time - p['timestamp']).total_seconds() / 60
        
        if 0.5 <= minutes_diff <= 1.5 and price_1m_before is None:
            price_1m_before = float(p['price'])
        if 1.5 <= minutes_diff <= 2.5 and price_2m_before is None:
            price_2m_before = float(p['price'])
        if 4.5 <= minutes_diff <= 5.5 and price_5m_before is None:
            price_5m_before = float(p['price'])
        if 9.5 <= minutes_diff <= 10.5 and price_10m_before is None:
            price_10m_before = float(p['price'])
    
    # Calculate changes
    changes = {}
    if price_1m_before:
        changes['change_1m'] = ((entry_price - price_1m_before) / price_1m_before) * 100
    if price_2m_before:
        changes['change_2m'] = ((entry_price - price_2m_before) / price_2m_before) * 100
    if price_5m_before:
        changes['change_5m'] = ((entry_price - price_5m_before) / price_5m_before) * 100
    if price_10m_before:
        changes['change_10m'] = ((entry_price - price_10m_before) / price_10m_before) * 100
    
    # Determine trend
    trend = 'unknown'
    if price_5m_before and price_1m_before:
        if changes.get('change_1m', 0) > 0.05 and changes.get('change_5m', 0) > 0.1:
            trend = 'rising'
        elif changes.get('change_1m', 0) < -0.05 and changes.get('change_5m', 0) < -0.1:
            trend = 'falling'
        else:
            trend = 'flat'
    
    # Get recommended action based on filters
    recommendation = "UNKNOWN"
    risk_level = "medium"
    
    change_10m = changes.get('change_10m', 0)
    
    if change_10m >= 0.15:
        recommendation = "STRONG BUY SIGNAL"
        risk_level = "low"
    elif change_10m >= 0.05:
        recommendation = "MODERATE BUY SIGNAL"
        risk_level = "medium"
    elif change_10m <= -0.10:
        recommendation = "AVOID - FALLING PRICE"
        risk_level = "high"
    else:
        recommendation = "NEUTRAL - FLAT PRICE"
        risk_level = "medium"
    
    return {
        'trade_id': buyin_id,
        'entry_time': entry_time.isoformat(),
        'entry_price': entry_price,
        'potential_gains': float(trade['potential_gains']) if trade['potential_gains'] else 0,
        'outcome': 'good' if float(trade['potential_gains'] or 0) >= 0.5 else 'bad',
        'prices': prices_data,
        'pre_entry_analysis': {
            'price_1m_before': price_1m_before,
            'price_2m_before': price_2m_before,
            'price_5m_before': price_5m_before,
            'price_10m_before': price_10m_before,
            'changes': changes,
            'trend': trend,
            'recommendation': recommendation,
            'risk_level': risk_level
        }
    }


def generate_summary_report(hours: int = 24) -> Dict:
    """
    Generate summary statistics for all trades in the time window.
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN potential_gains >= 0.5 THEN 1 END) as good,
                    COUNT(CASE WHEN potential_gains < 0.5 THEN 1 END) as bad,
                    AVG(potential_gains) as avg_gain
                FROM follow_the_goat_buyins
                WHERE followed_at >= NOW() - INTERVAL '%s hours'
                  AND potential_gains IS NOT NULL
                  AND play_id = 46
            """, [hours])
            stats = cursor.fetchone()
    
    return {
        'time_window': f'{hours} hours',
        'total_trades': stats['total'],
        'good_trades': stats['good'],
        'bad_trades': stats['bad'],
        'win_rate': (stats['good'] / stats['total'] * 100) if stats['total'] > 0 else 0,
        'avg_gain': float(stats['avg_gain']) if stats['avg_gain'] else 0
    }


def main():
    """
    Main function - can be called with trade ID or generate summary.
    """
    import sys
    
    if len(sys.argv) > 1:
        # Analyze specific trade
        try:
            trade_id = int(sys.argv[1])
            result = analyze_specific_trade(trade_id)
            print(json.dumps(result, indent=2))
        except ValueError:
            print("Error: Trade ID must be an integer")
            sys.exit(1)
    else:
        # Generate summary
        summary = generate_summary_report(24)
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

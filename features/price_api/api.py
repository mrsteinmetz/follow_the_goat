"""
Price API - Flask server providing DuckDB data access for PHP frontend.

Migrated from: 000old_code/solana_node/chart/build_pattern_config/DuckDBClient.php
This replaces the MySQL queries with direct DuckDB access.

Usage:
    python api.py          # Run on default port 5050
    python api.py --port 8080  # Run on custom port
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

app = Flask(__name__)
CORS(app)

# Database path - relative to project root
DB_PATH = Path(__file__).parent.parent.parent / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"


def get_connection():
    """Get a DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        with get_connection() as conn:
            result = conn.execute("SELECT COUNT(*) FROM price_points").fetchone()
            hot_count = result[0] if result else 0
        
        return jsonify({
            'status': 'ok',
            'database': str(DB_PATH),
            'hot_table_rows': hot_count,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/price_points', methods=['POST'])
def get_price_points():
    """
    Get price points for charting.
    
    Request JSON:
    {
        "token": "SOL",  # Token symbol (BTC, ETH, SOL)
        "start_datetime": "2024-01-01 00:00:00",  # Start of range
        "end_datetime": "2024-01-02 00:00:00"     # End of range
    }
    
    Response JSON:
    {
        "prices": [
            {"x": "2024-01-01 00:00:00", "y": 123.45},
            ...
        ],
        "count": 1000
    }
    """
    try:
        data = request.get_json() or {}
        
        token = data.get('token', 'SOL')
        end_datetime = data.get('end_datetime', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        # Default to 24 hours ago if not specified
        if 'start_datetime' in data:
            start_datetime = data['start_datetime']
        else:
            start_dt = datetime.now() - timedelta(hours=24)
            start_datetime = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        with get_connection() as conn:
            result = conn.execute("""
                SELECT ts, price 
                FROM price_points 
                WHERE token = ? 
                  AND ts BETWEEN ? AND ?
                ORDER BY ts ASC
            """, [token, start_datetime, end_datetime]).fetchall()
        
        prices = [
            {'x': row[0].strftime('%Y-%m-%d %H:%M:%S'), 'y': float(row[1])}
            for row in result
        ]
        
        return jsonify({
            'prices': prices,
            'count': len(prices)
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'prices': [],
            'count': 0
        }), 500


@app.route('/latest_prices', methods=['GET'])
def get_latest_prices():
    """
    Get the latest price for each token.
    
    Response JSON:
    {
        "prices": {
            "SOL": {"price": 123.45, "ts": "2024-01-01 00:00:00"},
            "BTC": {"price": 45000.00, "ts": "2024-01-01 00:00:00"},
            "ETH": {"price": 2500.00, "ts": "2024-01-01 00:00:00"}
        }
    }
    """
    try:
        with get_connection() as conn:
            result = conn.execute("""
                SELECT token, price, ts
                FROM price_points
                WHERE (token, ts) IN (
                    SELECT token, MAX(ts) FROM price_points GROUP BY token
                )
            """).fetchall()
        
        prices = {
            row[0]: {
                'price': float(row[1]),
                'ts': row[2].strftime('%Y-%m-%d %H:%M:%S')
            }
            for row in result
        }
        
        return jsonify({'prices': prices})
    
    except Exception as e:
        return jsonify({
            'error': str(e),
            'prices': {}
        }), 500


@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Get database statistics.
    
    Response JSON:
    {
        "hot_table_rows": 10000,
        "archive_table_rows": 500000,
        "tokens": ["BTC", "ETH", "SOL"],
        "oldest_hot": "2024-01-01 00:00:00",
        "newest_hot": "2024-01-02 00:00:00"
    }
    """
    try:
        with get_connection() as conn:
            # Hot table count
            hot_count = conn.execute("SELECT COUNT(*) FROM price_points").fetchone()[0]
            
            # Archive table count
            try:
                archive_count = conn.execute("SELECT COUNT(*) FROM price_points_archive").fetchone()[0]
            except:
                archive_count = 0
            
            # Distinct tokens
            tokens = [row[0] for row in conn.execute("SELECT DISTINCT token FROM price_points ORDER BY token").fetchall()]
            
            # Date range in hot table
            date_range = conn.execute("""
                SELECT MIN(ts), MAX(ts) FROM price_points
            """).fetchone()
            
            oldest = date_range[0].strftime('%Y-%m-%d %H:%M:%S') if date_range[0] else None
            newest = date_range[1].strftime('%Y-%m-%d %H:%M:%S') if date_range[1] else None
        
        return jsonify({
            'hot_table_rows': hot_count,
            'archive_table_rows': archive_count,
            'tokens': tokens,
            'oldest_hot': oldest,
            'newest_hot': newest
        })
    
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Price API Server')
    parser.add_argument('--port', type=int, default=5050, help='Port to run on (default: 5050)')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()
    
    print(f"Starting Price API on http://{args.host}:{args.port}")
    print(f"Database: {DB_PATH}")
    print(f"Database exists: {DB_PATH.exists()}")
    
    app.run(host=args.host, port=args.port, debug=args.debug)


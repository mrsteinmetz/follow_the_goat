"""
Website API Server - PostgreSQL Direct Access
==============================================
Provides all endpoints for the website by querying PostgreSQL directly.

Usage:
    python scheduler/website_api.py              # Default port 5051
    python scheduler/website_api.py --port 5051  # Explicit port

Architecture:
    master.py - Data ingestion (prices, trades, etc.)
    master2.py - Trading logic (buyins, positions, etc.)
    website_api.py (port 5051) - Website API (reads from PostgreSQL)
    
All services share the same PostgreSQL database.
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, request
from flask_cors import CORS
from core.database import get_postgres, postgres_query, verify_tables_exist

app = Flask(__name__)
CORS(app)

logger = logging.getLogger("website_api")
logging.basicConfig(level=logging.INFO)


# =============================================================================
# HEALTH & STATUS ENDPOINTS
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check - verify PostgreSQL connection."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) AS count FROM prices")
                prices_result = cursor.fetchone()
                prices_count = prices_result['count'] if prices_result else 0
                
                cursor.execute("SELECT COUNT(*) AS count FROM cycle_tracker")
                cycles_result = cursor.fetchone()
                cycles_count = cycles_result['count'] if cycles_result else 0
                
                cursor.execute("SELECT COUNT(*) AS count FROM follow_the_goat_buyins")
                buyins_result = cursor.fetchone()
                buyins_count = buyins_result['count'] if buyins_result else 0
        
        return jsonify({
            'status': 'healthy',
            'database': 'PostgreSQL',
            'tables': {
                'prices': prices_count,
                'cycles': cycles_count,
                'buyins': buyins_count
            },
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return jsonify({'status': 'error', 'error': str(e)}), 500


# =============================================================================
# PRICE & CYCLE ENDPOINTS
# =============================================================================

@app.route('/cycle_tracker', methods=['GET'])
def get_cycle_tracker():
    """Get price cycles."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        status_filter = request.args.get('status')
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                if status_filter == 'active':
                    cursor.execute("""
                        SELECT * FROM cycle_tracker
                        WHERE cycle_end_time IS NULL
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                elif status_filter == 'completed':
                    cursor.execute("""
                        SELECT * FROM cycle_tracker
                        WHERE cycle_end_time IS NOT NULL
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                else:
                    cursor.execute("""
                        SELECT * FROM cycle_tracker
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get cycles failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/latest_prices', methods=['GET'])
def get_latest_prices():
    """Get latest prices for all tokens."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT token, price, timestamp
                    FROM prices
                    WHERE (token, timestamp) IN (
                        SELECT token, MAX(timestamp) FROM prices GROUP BY token
                    )
                """)
                results = cursor.fetchall()
        
        return jsonify({'prices': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get prices failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/price_points', methods=['POST'])
def get_price_points():
    """Get price points for charting (used by website index.php)."""
    try:
        data = request.get_json()
        token = data.get('token', 'SOL')
        start_datetime = data.get('start_datetime')
        end_datetime = data.get('end_datetime')
        max_points = data.get('max_points', 5000)
        
        if not start_datetime or not end_datetime:
            return jsonify({'error': 'start_datetime and end_datetime required'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get all price points in the time range
                cursor.execute("""
                    SELECT timestamp, price
                    FROM prices
                    WHERE token = %s
                        AND timestamp >= %s
                        AND timestamp <= %s
                    ORDER BY timestamp ASC
                """, [token, start_datetime, end_datetime])
                
                results = cursor.fetchall()
                
                # Format for JavaScript charting
                prices = []
                for row in results:
                    prices.append({
                        'x': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(row['timestamp'], 'strftime') else str(row['timestamp']),
                        'y': float(row['price'])
                    })
                
                # Apply max_points limit if specified
                if max_points > 0 and len(prices) > max_points:
                    # Simple downsampling: take every Nth point
                    step = len(prices) // max_points
                    prices = prices[::step]
        
        return jsonify({
            'prices': prices,
            'count': len(prices),
            'total_available': len(results)
        })
    except Exception as e:
        logger.error(f"Get price points failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# =============================================================================
# TRADING ENDPOINTS
# =============================================================================

@app.route('/plays', methods=['GET'])
def get_plays():
    """Get active plays."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays
                    WHERE active = TRUE
                    ORDER BY id DESC LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get plays failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/buyins', methods=['GET'])
def get_buyins():
    """Get buyins."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        status_filter = request.args.get('status')
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                if status_filter:
                    cursor.execute("""
                        SELECT * FROM follow_the_goat_buyins
                        WHERE our_status = %s
                        ORDER BY id DESC LIMIT %s
                    """, [status_filter, limit])
                else:
                    cursor.execute("""
                        SELECT * FROM follow_the_goat_buyins
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get buyins failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/profiles', methods=['GET'])
def get_profiles():
    """Get wallet profiles."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM wallet_profiles
                    ORDER BY score DESC LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get profiles failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/profiles/stats', methods=['GET'])
def get_profile_stats():
    """Get wallet profile statistics."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_profiles,
                        AVG(score) as avg_score,
                        MAX(score) as max_score,
                        COUNT(CASE WHEN score > 70 THEN 1 END) as high_performers
                    FROM wallet_profiles
                """)
                result = cursor.fetchone()
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Get profile stats failed: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# PATTERN ENDPOINTS
# =============================================================================

@app.route('/patterns', methods=['GET'])
def get_patterns():
    """Get filter patterns."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM pattern_config_filters
                    ORDER BY id DESC LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get patterns failed: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# SCHEDULER STATUS (READ FROM FILE)
# =============================================================================

@app.route('/scheduler_status', methods=['GET'])
def get_scheduler_status():
    """Get scheduler job status (read from exported JSON file)."""
    try:
        import json
        status_file = PROJECT_ROOT / "logs" / "master2_job_status.json"
        
        if status_file.exists():
            with open(status_file, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        else:
            return jsonify({
                'status': 'unavailable',
                'message': 'Job status file not found. Is master2.py running?'
            })
    except Exception as e:
        logger.error(f"Get scheduler status failed: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# QUERY ENDPOINT (DEBUGGING)
# =============================================================================

@app.route('/query', methods=['POST'])
def query_sql():
    """Execute arbitrary SQL query (read-only for security)."""
    try:
        sql = request.json.get('sql', '').strip()
        
        # Security: only allow SELECT
        if not sql.upper().startswith('SELECT'):
            return jsonify({'error': 'Only SELECT queries allowed'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
        
        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Website API Server")
    parser.add_argument('--port', type=int, default=5051, help='Port to listen on')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    args = parser.parse_args()
    
    print("=" * 60)
    print("Follow The Goat - Website API Server")
    print("=" * 60)
    print(f"Database: PostgreSQL (shared)")
    print(f"Port: {args.port}")
    print(f"Host: {args.host}")
    print("=" * 60)
    
    # Verify PostgreSQL connection
    if not verify_tables_exist():
        print("\n[ERROR] PostgreSQL connection failed!")
        print("Make sure PostgreSQL is running and schema is initialized.")
        sys.exit(1)
    
    print("\n✓ PostgreSQL connection verified")
    print(f"\nStarting server on http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.\n")
    
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()

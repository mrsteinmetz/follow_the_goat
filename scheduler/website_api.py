"""
Website API Server - Proxy to master2.py's Local API
=====================================================
Runs independently from master.py and master2.py. Can be restarted anytime.

This API provides all endpoints for the website by proxying requests
to master2.py's Local API (port 5052). Gets computed data from master2's DuckDB!

Data Flow:
    Website (PHP) -> website_api.py (5051) -> master2.py (5052) -> Local DuckDB (computed data)

Usage:
    python scheduler/website_api.py              # Default port 5051
    python scheduler/website_api.py --port 5051  # Explicit port

Architecture:
    master.py (port 5050) - Data Engine (raw data ingestion)
    master2.py (port 5052) - Trading logic + Local API (computed data: cycles, profiles, etc.)
    website_api.py (port 5051) - Website API proxy (CAN restart freely)
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from functools import wraps
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, request
from flask_cors import CORS

# Import the engine client for HTTP calls to master.py
from core.engine_client import get_engine_client, is_engine_available

# Import scheduler status (shared with master.py when in same process,
# but we'll proxy to master.py for this too)
try:
    from scheduler.status import get_job_status, get_job_metrics
    HAS_LOCAL_STATUS = True
except ImportError:
    HAS_LOCAL_STATUS = False

app = Flask(__name__)
CORS(app)

logger = logging.getLogger("website_api")

# Master.py Data Engine API URL (port 5050)
DATA_ENGINE_URL = "http://127.0.0.1:5050"


# Master2.py Local API URL (port 5052)
MASTER2_LOCAL_API_URL = "http://127.0.0.1:5052"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def engine_required(f):
    """Decorator to check if master2.py's Local API is available."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not is_engine_available():
            return jsonify({
                "status": "error",
                "error": "Master2 Local API not available. Is master2.py running?",
                "hint": "Start master2.py first: python scheduler/master2.py"
            }), 503
        return f(*args, **kwargs)
    return decorated


def safe_int(value, default=0):
    """Safely convert value to int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    """Safely convert value to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# =============================================================================
# HEALTH & STATUS ENDPOINTS
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check - proxies to Data Engine."""
    client = get_engine_client()
    health = client.health_check()
    
    return jsonify({
        'status': health.get('status', 'unknown'),
        'proxy': 'website_api',
        'engine_status': health.get('status'),
        'engine_running': health.get('engine_running', False),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics from Data Engine."""
    client = get_engine_client()
    tables = client.get_tables()
    
    return jsonify({
        'status': 'ok',
        'tables': tables,
        'source': 'data_engine_proxy',
        'timestamp': datetime.now().isoformat()
    })


@app.route('/scheduler_status', methods=['GET'])
def get_scheduler_status():
    """Get scheduler job status from in-memory tracking."""
    try:
        # Import here to avoid circular imports
        from scheduler.status import _job_status, _job_status_lock, _scheduler_start_time
        
        with _job_status_lock:
            jobs = dict(_job_status)
        
        return jsonify({
            'status': 'ok',
            'jobs': jobs,
            'timestamp': datetime.now().isoformat(),
            'scheduler_started': _scheduler_start_time.isoformat() if _scheduler_start_time else None
        })
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/job_metrics', methods=['GET'])
def get_job_metrics_endpoint():
    """
    Get detailed job execution metrics from BOTH master.py and master2.py.
    
    Combines:
    - DuckDB metrics (master.py jobs + any master2 jobs that successfully write)
    - In-memory metrics (master2.py jobs via shared status module)
    """
    hours = safe_float(request.args.get('hours', 1))
    minutes = int(hours * 60)
    
    all_jobs = {}
    
    try:
        # SOURCE 1: Get DuckDB metrics (master.py jobs)
        import requests
        response = requests.post(
            "http://127.0.0.1:5050/query",
            json={"sql": f"""
                SELECT 
                    job_id,
                    COUNT(*) as execution_count,
                    AVG(duration_ms) as avg_duration_ms,
                    MAX(duration_ms) as max_duration_ms,
                    MIN(duration_ms) as min_duration_ms,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                    MAX(started_at) as last_execution
                FROM job_execution_metrics
                WHERE started_at >= NOW() - INTERVAL {minutes} MINUTE
                GROUP BY job_id
                ORDER BY job_id
            """},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Import expected intervals for determining if jobs are slow
            try:
                from features.price_api.schema import JOB_EXPECTED_INTERVALS_MS
            except ImportError:
                JOB_EXPECTED_INTERVALS_MS = {}
            
            for row in data.get('results', []):
                job_id = row.get('job_id', 'unknown')
                avg_ms = row.get('avg_duration_ms', 0) or 0
                expected_interval = JOB_EXPECTED_INTERVALS_MS.get(job_id, 60000)
                
                all_jobs[job_id] = {
                    'job_id': job_id,
                    'execution_count': row.get('execution_count', 0),
                    'avg_duration_ms': round(avg_ms, 2),
                    'max_duration_ms': round(row.get('max_duration_ms', 0) or 0, 2),
                    'min_duration_ms': round(row.get('min_duration_ms', 0) or 0, 2),
                    'error_count': row.get('error_count', 0),
                    'expected_interval_ms': expected_interval,
                    'is_slow': avg_ms > expected_interval * 0.8,
                    'last_execution': row.get('last_execution'),
                    'recent_executions': [],
                    'source': 'master.py (DuckDB)'
                }
    except Exception as e:
        logger.warning(f"Could not fetch DuckDB metrics: {e}")
    
    try:
        # SOURCE 2: Get in-memory status from master2.py jobs (via JSON file)
        import json
        status_file = Path(__file__).parent.parent / "logs" / "master2_job_status.json"
        
        if status_file.exists():
            try:
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
                
                # Import expected intervals
                try:
                    from features.price_api.schema import JOB_EXPECTED_INTERVALS_MS
                except ImportError:
                    JOB_EXPECTED_INTERVALS_MS = {}
                
                for job_id, job_info in status_data.get('jobs', {}).items():
                    # Skip if already in DuckDB results (prefer DuckDB metrics)
                    if job_id in all_jobs:
                        continue
                    
                    # Add master2.py in-memory status
                    last_duration_ms = job_info.get('last_duration_ms', 0) or 0
                    expected_interval = JOB_EXPECTED_INTERVALS_MS.get(job_id, 60000)
                    
                    all_jobs[job_id] = {
                        'job_id': job_id,
                        'execution_count': job_info.get('run_count', 0),
                        'avg_duration_ms': round(last_duration_ms, 2),  # Using last duration as proxy
                        'max_duration_ms': round(last_duration_ms, 2),
                        'min_duration_ms': round(last_duration_ms, 2),
                        'error_count': 1 if job_info.get('status') == 'error' else 0,
                        'expected_interval_ms': expected_interval,
                        'is_slow': last_duration_ms > expected_interval * 0.8,
                        'last_execution': job_info.get('last_end') or job_info.get('last_start'),
                        'recent_executions': [],
                        'source': 'master2.py (in-memory)',
                        'status': job_info.get('status', 'unknown'),
                        'description': job_info.get('description', job_id)
                    }
            except json.JSONDecodeError as e:
                logger.warning(f"Could not parse job status file: {e}")
        else:
            logger.debug(f"Job status file not found: {status_file}")
    except Exception as e:
        logger.warning(f"Could not fetch in-memory status: {e}")
    
    return jsonify({
        'status': 'ok',
        'hours': hours,
        'jobs': all_jobs,
        'timestamp': datetime.now().isoformat(),
        'sources': {
            'duckdb': len([j for j in all_jobs.values() if j.get('source') == 'master.py (DuckDB)']),
            'memory': len([j for j in all_jobs.values() if j.get('source') == 'master2.py (in-memory)'])
        }
    })


# =============================================================================
# PRICE DATA ENDPOINTS
# =============================================================================

@app.route('/price_points', methods=['POST'])
@engine_required
def get_price_points():
    """
    Get price points for charting.
    
    Request JSON:
    {
        "token": "SOL",
        "start_datetime": "2024-01-01 00:00:00",
        "end_datetime": "2024-01-02 00:00:00"
    }
    
    Note: This endpoint now queries the 'prices' table (not the legacy 'price_points' table)
    """
    data = request.get_json() or {}
    
    token = data.get('token', 'SOL').upper()
    end_datetime = data.get('end_datetime', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    start_datetime = data.get('start_datetime', (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'))
    
    client = get_engine_client()
    
    try:
        # Query the 'prices' table (where Jupiter price data is actually stored)
        results = client.query(f"""
            SELECT ts, price, token
            FROM prices
            WHERE token = '{token}'
              AND ts >= '{start_datetime}'
              AND ts <= '{end_datetime}'
            ORDER BY ts ASC
        """)
        
        prices = []
        for row in results:
            prices.append({
                'x': row.get('ts'),  # timestamp
                'y': row.get('price')  # price value
            })
        
        return jsonify({
            'status': 'ok',
            'token': token,
            'prices': prices,
            'count': len(prices),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'prices': []
        }), 500


@app.route('/latest_prices', methods=['GET'])
@engine_required
def get_latest_prices():
    """Get latest prices for all tokens."""
    client = get_engine_client()
    
    tokens = ['BTC', 'ETH', 'SOL']
    prices = {}
    
    for token in tokens:
        price = client.get_price(token)
        if price is not None:
            prices[token] = {
                'price': price,
                'timestamp': datetime.now().isoformat()
            }
    
    return jsonify({
        'status': 'ok',
        'prices': prices
    })


# =============================================================================
# CYCLE TRACKER ENDPOINTS
# =============================================================================

@app.route('/cycle_tracker', methods=['GET'])
@engine_required
def get_cycle_tracker():
    """Get cycle tracker data."""
    threshold = safe_float(request.args.get('threshold'), None)
    hours = request.args.get('hours', '24')
    limit = safe_int(request.args.get('limit', 100))
    
    client = get_engine_client()
    
    # Build query
    conditions = ["coin_id = 5"]  # SOL
    
    if threshold is not None:
        conditions.append(f"threshold = {threshold}")
    
    if hours != 'all':
        hours_int = safe_int(hours, 24)
        conditions.append(f"cycle_start_time >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = " AND ".join(conditions)
    
    try:
        results = client.query(f"""
            SELECT 
                id, coin_id, threshold, cycle_start_time, cycle_end_time,
                sequence_start_id, sequence_start_price, highest_price_reached,
                lowest_price_reached, max_percent_increase, max_percent_increase_from_lowest,
                total_data_points, created_at
            FROM cycle_tracker
            WHERE {where_clause}
            ORDER BY cycle_start_time DESC
            LIMIT {limit}
        """)
        
        return jsonify({
            'status': 'ok',
            'cycles': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'cycles': []
        }), 500


# =============================================================================
# PRICE ANALYSIS ENDPOINTS
# =============================================================================

@app.route('/price_analysis', methods=['GET'])
@engine_required
def get_price_analysis():
    """Get price analysis data."""
    coin_id = safe_int(request.args.get('coin_id', 5))
    hours = request.args.get('hours', '24')
    limit = safe_int(request.args.get('limit', 100))
    
    client = get_engine_client()
    
    conditions = [f"coin_id = {coin_id}"]
    
    if hours != 'all':
        hours_int = safe_int(hours, 24)
        conditions.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = " AND ".join(conditions)
    
    try:
        results = client.query(f"""
            SELECT *
            FROM price_analysis
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        
        return jsonify({
            'status': 'ok',
            'price_analysis': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'price_analysis': []
        }), 500


# =============================================================================
# PROFILES ENDPOINTS
# =============================================================================

@app.route('/profiles', methods=['GET'])
@engine_required
def get_profiles():
    """
    Get wallet profiles with aggregated data per wallet.
    
    This endpoint aggregates the wallet_profiles table to show:
    - One row per wallet (instead of one per trade)
    - Average potential gain across all trades
    - Total invested amount
    - Trade counts above/below threshold
    """
    threshold = safe_float(request.args.get('threshold'), None)
    hours = request.args.get('hours', '24')
    limit = safe_int(request.args.get('limit', 100))
    wallet = request.args.get('wallet')
    order_by = request.args.get('order_by', 'recent')
    
    client = get_engine_client()
    
    # Build WHERE conditions
    conditions = []
    
    if threshold is not None:
        conditions.append(f"threshold = {threshold}")
    
    if wallet:
        conditions.append(f"wallet_address = '{wallet}'")
    
    if hours != 'all':
        hours_int = safe_int(hours, 24)
        conditions.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Determine ORDER BY
    if order_by == 'avg_gain':
        order_clause = "ORDER BY avg_potential_gain DESC"
    elif order_by == 'trade_count':
        order_clause = "ORDER BY trade_count DESC"
    else:  # 'recent'
        order_clause = "ORDER BY latest_trade DESC"
    
    try:
        # Aggregated query: one row per wallet
        # Note: We use the row's 'threshold' column (not the filter parameter) for gain comparisons
        results = client.query(f"""
            SELECT 
                wallet_address,
                COUNT(*) as trade_count,
                AVG(
                    CASE 
                        WHEN short = 1 THEN 
                            ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100
                        ELSE 
                            ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100
                    END
                ) as avg_potential_gain,
                SUM(COALESCE(stablecoin_amount, 0)) as total_invested,
                SUM(
                    CASE 
                        WHEN short = 1 THEN 
                            CASE WHEN ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100 < threshold THEN 1 ELSE 0 END
                        ELSE 
                            CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 < threshold THEN 1 ELSE 0 END
                    END
                ) as trades_below_threshold,
                SUM(
                    CASE 
                        WHEN short = 1 THEN 
                            CASE WHEN ((trade_entry_price - lowest_price_reached) / trade_entry_price) * 100 >= threshold THEN 1 ELSE 0 END
                        ELSE 
                            CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price) * 100 >= threshold THEN 1 ELSE 0 END
                    END
                ) as trades_at_above_threshold,
                MAX(trade_timestamp) as latest_trade,
                ANY_VALUE(threshold) as threshold_value
            FROM wallet_profiles
            {where_clause}
            GROUP BY wallet_address
            {order_clause}
            LIMIT {limit}
        """)
        
        return jsonify({
            'status': 'ok',
            'profiles': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'profiles': []
        }), 500


@app.route('/profiles/stats', methods=['GET'])
@engine_required
def get_profiles_stats():
    """Get aggregated statistics for wallet profiles."""
    threshold = safe_float(request.args.get('threshold'), None)
    hours = request.args.get('hours', 'all')
    
    client = get_engine_client()
    
    # Build WHERE conditions
    conditions = []
    
    if threshold is not None:
        conditions.append(f"threshold = {threshold}")
    
    if hours != 'all':
        hours_int = safe_int(hours, 24)
        conditions.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    try:
        results = client.query(f"""
            SELECT 
                COUNT(*) as total_profiles,
                COUNT(DISTINCT wallet_address) as unique_wallets,
                COUNT(DISTINCT price_cycle) as unique_cycles,
                SUM(COALESCE(stablecoin_amount, 0)) as total_invested,
                AVG(trade_entry_price) as avg_entry_price
            FROM wallet_profiles
            {where_clause}
        """)
        
        stats = results[0] if results else {
            'total_profiles': 0,
            'unique_wallets': 0,
            'unique_cycles': 0,
            'total_invested': 0,
            'avg_entry_price': 0
        }
        
        return jsonify({
            'status': 'ok',
            'stats': stats,
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'stats': {
                'total_profiles': 0,
                'unique_wallets': 0,
                'unique_cycles': 0,
                'total_invested': 0,
                'avg_entry_price': 0
            }
        }), 500


# =============================================================================
# PLAYS ENDPOINTS
# =============================================================================

@app.route('/plays', methods=['GET'])
@engine_required
def get_plays():
    """Get all plays."""
    client = get_engine_client()
    
    try:
        results = client.query("""
            SELECT *
            FROM follow_the_goat_plays
            ORDER BY sorting ASC, id DESC
        """)
        
        return jsonify({
            'status': 'ok',
            'plays': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'plays': []
        }), 500


@app.route('/plays/<int:play_id>', methods=['GET'])
@engine_required
def get_play(play_id):
    """Get a single play by ID."""
    client = get_engine_client()
    
    try:
        results = client.query(f"""
            SELECT *
            FROM follow_the_goat_plays
            WHERE id = {play_id}
        """)
        
        if results:
            return jsonify({
                'status': 'ok',
                'play': results[0]
            })
        else:
            return jsonify({
                'status': 'error',
                'error': f'Play {play_id} not found'
            }), 404
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


# =============================================================================
# BUYINS ENDPOINTS
# =============================================================================

@app.route('/buyins', methods=['GET'])
@engine_required
def get_buyins():
    """Get buyins/trades."""
    play_id = safe_int(request.args.get('play_id'), None)
    status = request.args.get('status')
    hours = request.args.get('hours', '24')
    limit = safe_int(request.args.get('limit', 100))
    
    client = get_engine_client()
    
    conditions = []
    
    if play_id:
        conditions.append(f"play_id = {play_id}")
    
    if status:
        conditions.append(f"our_status = '{status}'")
    
    if hours != 'all':
        hours_int = safe_int(hours, 24)
        conditions.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    try:
        results = client.query(f"""
            SELECT *
            FROM follow_the_goat_buyins
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        
        return jsonify({
            'status': 'ok',
            'buyins': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'buyins': []
        }), 500


@app.route('/buyins/<int:buyin_id>', methods=['GET'])
def get_single_buyin(buyin_id):
    """
    Get a single buyin/trade by ID.
    
    Queries MySQL directly to avoid lock contention with master2's jobs.
    """
    try:
        from core.database import get_mysql
        
        with get_mysql() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT *
                FROM follow_the_goat_buyins
                WHERE id = %s
            """, (buyin_id,))
            
            buyin = cursor.fetchone()
            
            if buyin:
                # Convert datetime objects to strings for JSON
                for key, value in buyin.items():
                    if hasattr(value, 'isoformat'):
                        buyin[key] = value.isoformat()
                
                return jsonify({
                    'status': 'ok',
                    'buyin': buyin
                })
            else:
                return jsonify({
                    'status': 'error',
                    'error': f'Buyin {buyin_id} not found'
                }), 404
        
    except Exception as e:
        logger.error(f"Error fetching buyin {buyin_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


# =============================================================================
# TRAIL DATA ENDPOINTS
# =============================================================================

@app.route('/trail/buyin/<int:buyin_id>', methods=['GET'])
def get_trail_for_buyin(buyin_id):
    """
    Get 15-minute trail data for a specific buyin.
    
    Query params:
        - source: 'duckdb' (default) or 'mysql'
    
    Queries MySQL directly to avoid lock contention with master2's jobs.
    """
    source = request.args.get('source', 'mysql')
    
    try:
        from core.database import get_mysql
        
        with get_mysql() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT *
                FROM buyin_trail_minutes
                WHERE buyin_id = %s
                ORDER BY minute ASC
            """, (buyin_id,))
            
            trail_data = cursor.fetchall()
            
            # Convert datetime objects to strings for JSON
            for row in trail_data:
                for key, value in row.items():
                    if hasattr(value, 'isoformat'):
                        row[key] = value.isoformat()
            
            return jsonify({
                'status': 'ok',
                'trail_data': trail_data,
                'count': len(trail_data),
                'source': 'mysql'
            })
        
    except Exception as e:
        logger.error(f"Error fetching trail data for buyin {buyin_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'trail_data': []
        }), 500


# =============================================================================
# RECENT TRADES ENDPOINTS
# =============================================================================

@app.route('/recent_trades', methods=['GET'])
@engine_required
def get_recent_trades():
    """Get recent trades from sol_stablecoin_trades."""
    limit = safe_int(request.args.get('limit', 100))
    minutes = safe_int(request.args.get('minutes', 5))
    direction = request.args.get('direction', 'all')
    
    client = get_engine_client()
    
    conditions = [f"trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE"]
    
    if direction != 'all':
        conditions.append(f"direction = '{direction}'")
    
    where_clause = " AND ".join(conditions)
    
    try:
        results = client.query(f"""
            SELECT *
            FROM sol_stablecoin_trades
            WHERE {where_clause}
            ORDER BY trade_timestamp DESC
            LIMIT {limit}
        """)
        
        return jsonify({
            'status': 'ok',
            'trades': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'trades': []
        }), 500


# =============================================================================
# GENERIC QUERY ENDPOINT
# =============================================================================

@app.route('/query', methods=['POST'])
@engine_required
def execute_query():
    """
    Execute a generic query (SELECT only).
    
    This proxies directly to master.py's /query endpoint.
    """
    data = request.get_json() or {}
    
    table = data.get('table')
    columns = data.get('columns')
    where = data.get('where')
    order_by = data.get('order_by')
    limit = safe_int(data.get('limit', 100))
    
    if not table:
        return jsonify({
            'status': 'error',
            'error': 'table parameter is required'
        }), 400
    
    client = get_engine_client()
    
    # Build query
    cols = ", ".join(columns) if columns else "*"
    
    conditions = []
    if where:
        for key, value in where.items():
            if isinstance(value, str):
                conditions.append(f"{key} = '{value}'")
            else:
                conditions.append(f"{key} = {value}")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    
    sql = f"SELECT {cols} FROM {table} {where_clause} {order_clause} LIMIT {limit}"
    
    try:
        results = client.query(sql)
        
        return jsonify({
            'status': 'ok',
            'results': results,
            'count': len(results),
            'source': 'data_engine'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'results': []
        }), 500


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Website API Server (Proxy to Data Engine)')
    parser.add_argument('--port', type=int, default=5051, help='Port to run on (default: 5051)')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    print("=" * 60)
    print("Starting Website API Server (Proxy)")
    print("=" * 60)
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print(f"Master2 Local API: {DATA_ENGINE_URL}")
    print("")
    print("This API proxies requests to master2.py's Local API.")
    print("Gets computed data (cycles, profiles) from master2's DuckDB.")
    print("")
    print("Can be restarted anytime without affecting master2.py!")
    print("=" * 60)
    
    # Check if Master2 Local API is available
    if is_engine_available():
        print(f"✓ Master2 Local API is available at {DATA_ENGINE_URL}")
    else:
        print(f"✗ WARNING: Master2 Local API not available at {DATA_ENGINE_URL}")
        print("  Make sure master2.py is running first!")
    
    print("=" * 60)
    
    # Run Flask
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
        threaded=True,
        use_reloader=args.debug
    )


if __name__ == "__main__":
    main()

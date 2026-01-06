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
import threading
import time

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
# SIMPLE CACHE FOR FREQUENTLY ACCESSED ENDPOINTS
# =============================================================================

class SimpleCache:
    """Thread-safe simple cache with TTL."""
    
    def __init__(self, default_ttl=5):
        self._cache = {}
        self._lock = threading.Lock()
        self.default_ttl = default_ttl
    
    def get(self, key):
        """Get cached value if not expired."""
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    return value
                else:
                    del self._cache[key]
            return None
    
    def set(self, key, value, ttl=None):
        """Set cached value with TTL."""
        if ttl is None:
            ttl = self.default_ttl
        with self._lock:
            self._cache[key] = (value, time.time() + ttl)
    
    def clear(self):
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()


# Global cache instance
_cache = SimpleCache(default_ttl=5)  # 5 second TTL for most endpoints


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
@engine_required
def get_scheduler_status():
    """
    Get scheduler job status from master2.py's Local API.
    
    Proxies to master2.py's /scheduler/status endpoint which has access to
    the in-memory job status tracking.
    
    Cached for 3 seconds to reduce load on master2.py.
    """
    # Check cache first
    cache_key = 'scheduler_status'
    cached = _cache.get(cache_key)
    if cached is not None:
        return jsonify(cached)
    
    try:
        import requests
        
        # Proxy to master2.py's Local API
        url = f"{MASTER2_LOCAL_API_URL}/scheduler/status"
        logger.debug(f"Proxying scheduler_status request to: {url}")
        
        response = requests.get(url, timeout=10)
        
        logger.debug(f"Master2 API response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Received data keys: {list(data.keys())}")
            logger.debug(f"Jobs count: {len(data.get('jobs', {}))}")
            
            # Prepare response
            result = {
                'status': 'ok',
                'jobs': data.get('jobs', {}),
                'timestamp': data.get('timestamp', datetime.now().isoformat()),
                'scheduler_started': data.get('scheduler_started')
            }
            
            # Cache for 3 seconds
            _cache.set(cache_key, result, ttl=3)
            
            return jsonify(result)
        else:
            logger.error(f"Master2 API returned status {response.status_code}: {response.text}")
            return jsonify({
                'status': 'error',
                'error': f"Master2 API returned status {response.status_code}",
                'jobs': {},
                'timestamp': datetime.now().isoformat()
            }), response.status_code
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get scheduler status from master2: {e}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': f"Failed to connect to master2.py: {str(e)}",
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        }), 503
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}", exc_info=True)
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
        "end_datetime": "2024-01-02 00:00:00",
        "max_points": 5000  # Optional: limit number of points (default: 5000)
    }
    
    Note: This endpoint now queries the 'prices' table (not the legacy 'price_points' table)
    For large time ranges, data is sampled to reduce transfer size.
    """
    data = request.get_json() or {}
    
    token = data.get('token', 'SOL').upper()
    end_datetime = data.get('end_datetime', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    start_datetime = data.get('start_datetime', (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'))
    max_points = data.get('max_points', 5000)  # Default limit to 5000 points
    
    client = get_engine_client()
    
    try:
        # First, get count to determine if we need sampling
        count_sql = f"""
            SELECT COUNT(*) as cnt
            FROM prices
            WHERE token = '{token}'
              AND ts >= '{start_datetime}'
              AND ts <= '{end_datetime}'
        """
        logger.info(f"[PRICE_POINTS] Count query: {count_sql}")
        count_result = client.query(count_sql)
        logger.info(f"[PRICE_POINTS] Count result: {count_result}")
        
        total_count = count_result[0].get('cnt', 0) if count_result else 0
        logger.info(f"[PRICE_POINTS] Total count: {total_count}, max_points: {max_points}")
        
        # If we have more points than max_points, sample the data
        if total_count > max_points:
            # Use sampling: get every Nth point
            sample_interval = max(1, total_count // max_points)
            
            # BUG FIX: If sample_interval is 1, we don't need sampling at all
            # because rn % 1 is always 0, so rn % 1 = 1 will return NO results
            if sample_interval == 1:
                logger.info(f"[PRICE_POINTS] Sample interval is 1, getting all points instead")
                full_sql = f"""
                    SELECT ts, price, token
                    FROM prices
                    WHERE token = '{token}'
                      AND ts >= '{start_datetime}'
                      AND ts <= '{end_datetime}'
                    ORDER BY ts ASC
                """
                results = client.query(full_sql)
            else:
                sample_sql = f"""
                    SELECT ts, price, token
                    FROM (
                        SELECT ts, price, token,
                               ROW_NUMBER() OVER (ORDER BY ts ASC) as rn
                        FROM prices
                        WHERE token = '{token}'
                          AND ts >= '{start_datetime}'
                          AND ts <= '{end_datetime}'
                    ) ranked
                    WHERE rn % {sample_interval} = 1
                    ORDER BY ts ASC
                """
                logger.info(f"[PRICE_POINTS] Using sampling with interval {sample_interval}")
                logger.info(f"[PRICE_POINTS] Sample SQL: {sample_sql[:200]}...")
                results = client.query(sample_sql)
            logger.info(f"[PRICE_POINTS] Results count: {len(results) if results else 0}")
        else:
            # Get all points if under limit
            full_sql = f"""
                SELECT ts, price, token
                FROM prices
                WHERE token = '{token}'
                  AND ts >= '{start_datetime}'
                  AND ts <= '{end_datetime}'
                ORDER BY ts ASC
            """
            logger.info(f"[PRICE_POINTS] Getting all points (no sampling)")
            results = client.query(full_sql)
            logger.info(f"[PRICE_POINTS] Full results count: {len(results) if results else 0}")
        
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
            'total_available': total_count,
            'sampled': total_count > max_points,
            'source': 'data_engine'
        })
        
    except Exception as e:
        logger.error(f"Error fetching price points: {e}", exc_info=True)
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
    """
    Get cycle tracker data - proxies to master2's Local API.
    
    By default, only returns COMPLETED cycles (cycle_end_time IS NOT NULL).
    Set active_only=true to get active cycles.
    """
    import requests
    
    # Proxy to master2's Local API
    try:
        params = dict(request.args)
        # Force active_only=false to get completed cycles only (unless explicitly requested)
        if params.get('active_only', 'false').lower() != 'true':
            params['active_only'] = 'false'
        
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/cycle_tracker",
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error proxying cycle_tracker to master2: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'cycles': []
        }), 500
        
        # Get total count (all cycles matching filters, not just paginated)
        total_count_result = client.query(f"""
            SELECT COUNT(*) as total
            FROM cycle_tracker
            WHERE {where_clause}
        """)
        total_count = total_count_result[0]['total'] if total_count_result else 0
        
        # Calculate missing cycles: gaps in the ID sequence
        # Get min and max IDs for this filter
        minmax_result = client.query(f"""
            SELECT MIN(id) as min_id, MAX(id) as max_id
            FROM cycle_tracker
            WHERE {where_clause}
        """)
        
        missing_cycles = 0
        if minmax_result and minmax_result[0]['min_id'] is not None:
            min_id = minmax_result[0]['min_id']
            max_id = minmax_result[0]['max_id']
            expected_count = max_id - min_id + 1
            missing_cycles = expected_count - total_count
        
        return jsonify({
            'status': 'ok',
            'cycles': results,
            'count': len(results),  # Paginated count
            'total_count': total_count,  # Total matching filter
            'missing_cycles': missing_cycles,  # Gaps in ID sequence
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
def get_profiles():
    """
    Get wallet profiles with aggregated data per wallet.
    
    Proxies to master2's local DuckDB where wallet_profiles are stored.
    """
    import requests
    
    # Forward all query parameters to master2
    params = request.args.to_dict()
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/profiles",
            params=params,
            timeout=30
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error proxying profiles request to master2: {e}")
        return jsonify({
            'status': 'error',
            'error': f'Failed to connect to master2: {str(e)}',
            'profiles': []
        }), 500


@app.route('/profiles/stats', methods=['GET'])
def get_profiles_stats():
    """Get aggregated statistics for wallet profiles - proxies to master2."""
    import requests
    
    params = request.args.to_dict()
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/profiles/stats",
            params=params,
            timeout=30
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error proxying profiles/stats request to master2: {e}")
        return jsonify({
            'status': 'error',
            'error': f'Failed to connect to master2: {str(e)}'
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


@app.route('/plays/performance', methods=['GET'])
@engine_required
def get_all_plays_performance():
    """
    Get performance metrics for all plays (batch operation).
    
    Query params:
        hours: Time window (default: 'all', or number like 24, 12, 6, 2)
    """
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filters
        time_filter_pending = ""
        time_filter_no_go = ""
        time_filter_sold = ""
        
        if hours != 'all':
            try:
                hours_int = int(hours)
                time_filter_pending = f"AND followed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_int} HOUR"
                time_filter_no_go = f"AND followed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_int} HOUR"
                time_filter_sold = f"AND our_exit_timestamp >= CURRENT_TIMESTAMP - INTERVAL {hours_int} HOUR"
            except ValueError:
                pass
        
        client = get_engine_client()
        plays_data = {}
        
        # Get all play IDs
        plays_result = client.query("SELECT id FROM follow_the_goat_plays ORDER BY id")
        play_ids = [p['id'] for p in plays_result] if plays_result else []
        
        if not play_ids:
            return jsonify({
                'success': True,
                'plays': {}
            })
        
        # Get live trades stats (pending trades)
        live_query = f"""
            SELECT 
                play_id,
                COUNT(*) as active_trades,
                AVG(CASE 
                    WHEN our_entry_price > 0 AND current_price > 0 
                    THEN ((current_price - our_entry_price) / our_entry_price) * 100 
                    ELSE NULL 
                END) as active_avg_profit
            FROM follow_the_goat_buyins
            WHERE our_status = 'pending' {time_filter_pending}
            GROUP BY play_id
        """
        live_results = client.query(live_query)
        live_stats = {r['play_id']: r for r in live_results} if live_results else {}
        
        # Get no_go counts
        no_go_query = f"""
            SELECT 
                play_id,
                COUNT(*) as no_go_count
            FROM follow_the_goat_buyins
            WHERE our_status = 'no_go' {time_filter_no_go}
            GROUP BY play_id
        """
        no_go_results = client.query(no_go_query)
        no_go_stats = {r['play_id']: r['no_go_count'] for r in no_go_results} if no_go_results else {}
        
        # Get sold/completed trades stats
        sold_query = f"""
            SELECT 
                play_id,
                SUM(our_profit_loss) as total_profit_loss,
                COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades
            FROM follow_the_goat_buyins
            WHERE our_status IN ('sold', 'completed') {time_filter_sold}
            GROUP BY play_id
        """
        sold_results = client.query(sold_query)
        sold_stats = {r['play_id']: r for r in sold_results} if sold_results else {}
        
        # Combine stats for each play - use string keys for JavaScript compatibility
        for play_id in play_ids:
            live = live_stats.get(play_id, {})
            sold = sold_stats.get(play_id, {})
            
            plays_data[str(play_id)] = {
                'total_profit_loss': float(sold.get('total_profit_loss') or 0),
                'winning_trades': int(sold.get('winning_trades') or 0),
                'losing_trades': int(sold.get('losing_trades') or 0),
                'total_no_gos': no_go_stats.get(play_id, 0),
                'active_trades': int(live.get('active_trades') or 0),
                'active_avg_profit': float(live.get('active_avg_profit')) if live.get('active_avg_profit') is not None else None
            }
        
        return jsonify({
            'success': True,
            'plays': plays_data
        })
        
    except Exception as e:
        logger.error(f"Error in get_all_plays_performance: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
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
        conditions.append(f"followed_at >= NOW() - INTERVAL {hours_int} HOUR")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    try:
        # Use followed_at (correct column name) and leverage composite index
        results = client.query(f"""
            SELECT *
            FROM follow_the_goat_buyins
            {where_clause}
            ORDER BY followed_at DESC
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
@engine_required
def get_single_buyin(buyin_id):
    """
    Get a single buyin/trade by ID.
    
    Queries DuckDB via the data engine.
    """
    client = get_engine_client()
    
    try:
        results = client.query(f"""
            SELECT *
            FROM follow_the_goat_buyins
            WHERE id = {buyin_id}
        """)
        
        if results:
            return jsonify({
                'status': 'ok',
                'buyin': results[0]
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
@engine_required
def get_trail_for_buyin(buyin_id):
    """
    Get 15-minute trail data for a specific buyin.
    
    Query params:
        - source: 'duckdb' (default) or 'mysql'
    
    Queries DuckDB via the data engine.
    """
    source = request.args.get('source', 'duckdb')
    
    client = get_engine_client()
    
    try:
        results = client.query(f"""
            SELECT *
            FROM buyin_trail_minutes
            WHERE buyin_id = {buyin_id}
            ORDER BY minute ASC
        """)
        
        return jsonify({
            'status': 'ok',
            'trail_data': results,
            'count': len(results),
            'source': 'duckdb'
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
# PATTERN CONFIG ENDPOINTS
# =============================================================================

@app.route('/patterns/projects', methods=['GET'])
@engine_required
def get_pattern_projects():
    """Get all pattern config projects - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/patterns/projects",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error fetching pattern projects: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'projects': []
        }), 500


@app.route('/patterns/projects/<int:project_id>', methods=['GET'])
@engine_required
def get_pattern_project(project_id):
    """Get a single pattern project with its filters - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/patterns/projects/{project_id}",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error fetching pattern project {project_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/patterns/projects', methods=['POST'])
@engine_required
def create_pattern_project():
    """Create a new pattern project - proxies to master2's DuckDB."""
    import requests
    
    data = request.get_json() or {}
    
    try:
        response = requests.post(
            f"{MASTER2_LOCAL_API_URL}/patterns/projects",
            json=data,
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error creating pattern project: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/patterns/projects/<int:project_id>', methods=['DELETE'])
@engine_required
def delete_pattern_project(project_id):
    """Delete a pattern project and all its filters - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.delete(
            f"{MASTER2_LOCAL_API_URL}/patterns/projects/{project_id}",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error deleting pattern project {project_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/patterns/projects/<int:project_id>/filters', methods=['GET'])
@engine_required
def get_pattern_filters(project_id):
    """Get filters for a pattern project - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/patterns/projects/{project_id}/filters",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error fetching filters for project {project_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'filters': []
        }), 500


@app.route('/patterns/filters', methods=['POST'])
@engine_required
def create_pattern_filter():
    """Create a new pattern filter - proxies to master2's DuckDB."""
    import requests
    
    data = request.get_json() or {}
    
    try:
        response = requests.post(
            f"{MASTER2_LOCAL_API_URL}/patterns/filters",
            json=data,
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error creating pattern filter: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['PUT'])
@engine_required
def update_pattern_filter(filter_id):
    """Update a pattern filter - proxies to master2's DuckDB."""
    import requests
    
    data = request.get_json() or {}
    
    try:
        response = requests.put(
            f"{MASTER2_LOCAL_API_URL}/patterns/filters/{filter_id}",
            json=data,
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error updating pattern filter {filter_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['DELETE'])
@engine_required
def delete_pattern_filter(filter_id):
    """Delete a pattern filter - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.delete(
            f"{MASTER2_LOCAL_API_URL}/patterns/filters/{filter_id}",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error deleting pattern filter {filter_id}: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500


# =============================================================================
# FILTER ANALYSIS ENDPOINTS (proxy to master2)
# =============================================================================

@app.route('/filter-analysis/dashboard', methods=['GET'])
def get_filter_analysis_dashboard():
    """Get filter analysis dashboard - proxies to master2's DuckDB."""
    import requests
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/filter-analysis/dashboard",
            timeout=30
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error getting filter analysis dashboard: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/filter-analysis/settings', methods=['GET'])
def get_filter_settings():
    """Get auto filter settings - proxies to master2."""
    import requests
    
    try:
        response = requests.get(
            f"{MASTER2_LOCAL_API_URL}/filter-analysis/settings",
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error getting filter settings: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/filter-analysis/settings', methods=['POST'])
def save_filter_settings():
    """Save auto filter settings - proxies to master2."""
    import requests
    
    data = request.get_json() or {}
    
    try:
        response = requests.post(
            f"{MASTER2_LOCAL_API_URL}/filter-analysis/settings",
            json=data,
            timeout=10
        )
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logger.error(f"Error saving filter settings: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
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
        print(f"✓ Master2 Local API is available at {MASTER2_LOCAL_API_URL}")
    else:
        print(f"✗ WARNING: Master2 Local API not available at {MASTER2_LOCAL_API_URL}")
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

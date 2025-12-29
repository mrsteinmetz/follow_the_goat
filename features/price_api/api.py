"""
Central DuckDB API - Flask server with TradingDataEngine integration.

This API serves as the central gateway for:
- Reading from TradingDataEngine (in-memory 24hr hot data) with zero lock contention
- Falling back to MySQL for historical data
- Writing to TradingDataEngine (non-blocking, queue-based)
- Managing plays, trades, and price data

Migrated from: 000old_code/solana_node/chart/build_pattern_config/DuckDBClient.php

Usage:
    python api.py          # Run on default port 5050
    python api.py --port 8080  # Run on custom port
"""

import sys
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta
import json
import logging

from core.database import (
    get_duckdb, get_mysql, get_trading_engine,
    dual_write_insert, dual_write_update, dual_write_delete,
    smart_query, init_duckdb_tables, cleanup_all_hot_tables
)
from core.config import settings
from features.price_api.schema import HOT_TABLES, TIMESTAMP_COLUMNS

app = Flask(__name__)
CORS(app)

# Configure logger
logger = logging.getLogger("price_api")

# Database path for backward compatibility (WSL-aware)
from core.database import DATABASES
DB_PATH = DATABASES.get("prices", PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb")


# =============================================================================
# HEALTH & STATUS ENDPOINTS
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    # Check TradingDataEngine (in-memory DuckDB)
    try:
        engine = get_trading_engine()
        if engine._running:
            engine_status = engine.health_check()
            duckdb_status = engine_status.get('duckdb', 'unknown')
            mysql_status = engine_status.get('mysql', 'unknown')
            engine_running = True
        else:
            duckdb_status = "engine_not_running"
            mysql_status = "engine_not_running"
            engine_running = False
    except Exception as e:
        duckdb_status = f"error: {str(e)}"
        mysql_status = "unknown"
        engine_running = False
    
    # Fallback MySQL check if engine not running
    if not engine_running:
        try:
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    mysql_status = "ok"
        except Exception as e:
            mysql_status = f"error: {str(e)}"
    
    return jsonify({
        'status': 'ok' if duckdb_status == 'ok' and mysql_status == 'ok' else 'degraded',
        'duckdb': duckdb_status,
        'mysql': mysql_status,
        'engine_running': engine_running,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics."""
    try:
        stats = {}
        
        # Try TradingDataEngine first (in-memory)
        try:
            engine = get_trading_engine()
            if engine._running:
                engine_stats = engine.get_stats()
                for table, count in engine_stats.get('table_counts', {}).items():
                    stats[f"engine_{table}"] = count
                stats['engine_queue_size'] = engine_stats.get('queue_size', 0)
                stats['engine_writes_queued'] = engine_stats.get('writes_queued', 0)
                stats['engine_writes_committed'] = engine_stats.get('writes_committed', 0)
        except:
            pass
        
        # Fallback to file-based DuckDB
        if not stats:
            try:
                with get_duckdb("central") as conn:
                    for table in HOT_TABLES:
                        try:
                            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                            stats[f"duckdb_{table}"] = count
                        except:
                            stats[f"duckdb_{table}"] = 0
            except:
                pass
        
        # MySQL stats
        with get_mysql() as mysql_conn:
            with mysql_conn.cursor() as cursor:
                for table in HOT_TABLES + ['follow_the_goat_plays', 'price_points']:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                        result = cursor.fetchone()
                        stats[f"mysql_{table}"] = result['cnt']
                    except:
                        stats[f"mysql_{table}"] = 0
        
        return jsonify({
            'status': 'ok',
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/trades_diagnostic', methods=['GET'])
def trades_diagnostic():
    """
    Diagnostic endpoint to compare trade counts across all data sources.
    
    Sync Path (FAST): Webhook DuckDB → Python DuckDB (every 2s)
    Fallback Path: MySQL → Python DuckDB (only if webhook unavailable)
    
    Data sources checked:
    - .NET Webhook DuckDB (in-memory on quicknode.smz.dk) - SOURCE
    - Python DuckDB sol_stablecoin_trades - LOCAL CACHE
    - MySQL sol_stablecoin_trades - BACKUP
    
    Query params:
    - minutes: Time window in minutes (default: 5)
    """
    import requests
    import time as time_module
    
    minutes = int(request.args.get('minutes', 5))
    
    diagnostic = {
        'time_window_minutes': minutes,
        'timestamp': datetime.now().isoformat(),
        'sync_mode': 'DuckDB→DuckDB (ultra-fast, every 500ms)',
        'sources': {}
    }
    
    # 1. Check .NET Webhook DuckDB (in-memory) - THE SOURCE OF TRUTH
    try:
        start_time = time_module.time()
        webhook_response = requests.get(
            f'http://quicknode.smz.dk/api/trades?limit=1000',
            timeout=5
        )
        response_time_ms = (time_module.time() - start_time) * 1000
        
        if webhook_response.status_code == 200:
            data = webhook_response.json()
            if data.get('success'):
                results = data.get('results', [])
                # Filter by time window
                now = datetime.now()
                recent_count = 0
                buy_count = 0
                for trade in results:
                    ts_str = trade.get('trade_timestamp', '')
                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00').replace('+00:00', ''))
                            if (now - ts).total_seconds() <= minutes * 60:
                                recent_count += 1
                                if trade.get('direction', '').lower() == 'buy':
                                    buy_count += 1
                        except:
                            pass
                
                diagnostic['sources']['webhook_duckdb'] = {
                    'status': 'ok',
                    'role': 'SOURCE (real-time from QuickNode)',
                    'total_in_hot': len(results),
                    f'last_{minutes}m': recent_count,
                    f'last_{minutes}m_buys': buy_count,
                    'response_time_ms': round(response_time_ms, 1),
                    'source': data.get('source', 'duckdb_inmemory')
                }
            else:
                diagnostic['sources']['webhook_duckdb'] = {
                    'status': 'error',
                    'error': data.get('error', 'Unknown error')
                }
        else:
            diagnostic['sources']['webhook_duckdb'] = {
                'status': 'error',
                'http_code': webhook_response.status_code
            }
    except requests.exceptions.RequestException as e:
        diagnostic['sources']['webhook_duckdb'] = {
            'status': 'unreachable',
            'error': str(e),
            'note': 'Sync will fallback to MySQL (slower)'
        }
    
    # 2. Check MySQL sol_stablecoin_trades - BACKUP/ARCHIVE
    try:
        start_time = time_module.time()
        with get_mysql() as mysql_conn:
            with mysql_conn.cursor() as cursor:
                # Total in last X minutes
                cursor.execute(f"""
                    SELECT COUNT(*) as cnt
                    FROM sol_stablecoin_trades
                    WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                """)
                recent = cursor.fetchone()['cnt']
                
                # Buy count
                cursor.execute(f"""
                    SELECT COUNT(*) as cnt
                    FROM sol_stablecoin_trades
                    WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                    AND direction = 'buy'
                """)
                buy_count = cursor.fetchone()['cnt']
                
                # Total in table
                cursor.execute("SELECT COUNT(*) as cnt FROM sol_stablecoin_trades")
                total = cursor.fetchone()['cnt']
        
        query_time_ms = (time_module.time() - start_time) * 1000
        
        diagnostic['sources']['mysql'] = {
            'status': 'ok',
            'role': 'BACKUP (written by .NET webhook, used as fallback)',
            'total_in_table': total,
            f'last_{minutes}m': recent,
            f'last_{minutes}m_buys': buy_count,
            'query_time_ms': round(query_time_ms, 1)
        }
    except Exception as e:
        diagnostic['sources']['mysql'] = {
            'status': 'error',
            'error': str(e)
        }
    
    # 3. Check Python DuckDB sol_stablecoin_trades - LOCAL CACHE
    try:
        start_time = time_module.time()
        with get_duckdb("central") as conn:
            # Total in last X minutes
            result = conn.execute(f"""
                SELECT COUNT(*) as cnt
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
            """).fetchone()
            recent = result[0] if result else 0
            
            # Buy count
            result = conn.execute(f"""
                SELECT COUNT(*) as cnt
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                AND direction = 'buy'
            """).fetchone()
            buy_count = result[0] if result else 0
            
            # Total in table
            result = conn.execute("SELECT COUNT(*) FROM sol_stablecoin_trades").fetchone()
            total = result[0] if result else 0
        
        query_time_ms = (time_module.time() - start_time) * 1000
        
        diagnostic['sources']['python_duckdb'] = {
            'status': 'ok',
            'role': 'LOCAL CACHE (synced every 500ms from Webhook)',
            'total_in_table': total,
            f'last_{minutes}m': recent,
            f'last_{minutes}m_buys': buy_count,
            'query_time_ms': round(query_time_ms, 1)
        }
    except Exception as e:
        diagnostic['sources']['python_duckdb'] = {
            'status': 'error',
            'error': str(e)
        }
    
    # 4. Check TradingDataEngine (in-memory)
    try:
        engine = get_trading_engine()
        if engine._running:
            result = engine.read(f"""
                SELECT COUNT(*) as cnt
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
            """, [])
            recent = result[0]['cnt'] if result else 0
            
            result = engine.read(f"""
                SELECT COUNT(*) as cnt
                FROM sol_stablecoin_trades
                WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                AND direction = 'buy'
            """, [])
            buy_count = result[0]['cnt'] if result else 0
            
            result = engine.read("SELECT COUNT(*) as cnt FROM sol_stablecoin_trades", [])
            total = result[0]['cnt'] if result else 0
            
            diagnostic['sources']['trading_engine'] = {
                'status': 'ok',
                'total_in_table': total,
                f'last_{minutes}m': recent,
                f'last_{minutes}m_buys': buy_count
            }
        else:
            diagnostic['sources']['trading_engine'] = {
                'status': 'not_running'
            }
    except Exception as e:
        diagnostic['sources']['trading_engine'] = {
            'status': 'error',
            'error': str(e)
        }
    
    # Summary / Comparison - Focus on Webhook→DuckDB sync (fast path)
    webhook_recent = diagnostic['sources'].get('webhook_duckdb', {}).get(f'last_{minutes}m', 0)
    duckdb_recent = diagnostic['sources'].get('python_duckdb', {}).get(f'last_{minutes}m', 0)
    mysql_recent = diagnostic['sources'].get('mysql', {}).get(f'last_{minutes}m', 0)
    
    webhook_status = diagnostic['sources'].get('webhook_duckdb', {}).get('status', 'unknown')
    
    if webhook_status == 'ok' and webhook_recent is not None and duckdb_recent is not None:
        sync_lag = webhook_recent - duckdb_recent
        diagnostic['sync_status'] = {
            'sync_path': 'Webhook DuckDB → Python DuckDB (ULTRA-FAST 500ms)',
            'webhook_trades': webhook_recent,
            'local_duckdb_trades': duckdb_recent,
            'sync_lag': sync_lag,
            'synced': sync_lag <= 1,  # Tight tolerance for 500ms sync
            'message': 'Synced' if sync_lag <= 1 else f'Local DuckDB is {sync_lag} trades behind Webhook'
        }
    elif webhook_status != 'ok':
        # Fallback mode - compare MySQL to DuckDB
        if mysql_recent is not None and duckdb_recent is not None:
            sync_lag = mysql_recent - duckdb_recent
            diagnostic['sync_status'] = {
                'sync_path': 'MySQL → Python DuckDB (FALLBACK - webhook unavailable)',
                'mysql_trades': mysql_recent,
                'local_duckdb_trades': duckdb_recent,
                'sync_lag': sync_lag,
                'synced': sync_lag <= 5,
                'message': 'Synced (fallback mode)' if sync_lag <= 5 else f'Local DuckDB is {sync_lag} trades behind MySQL',
                'warning': 'Webhook is unavailable - using slower MySQL fallback'
            }
    
    return jsonify(diagnostic)


@app.route('/scheduler_status', methods=['GET'])
def get_scheduler_status():
    """
    Get scheduler job status - shows when each job last ran.
    This helps monitor if all scheduled tasks are running properly.
    """
    try:
        # Import from shared status module (avoids circular imports)
        from scheduler.status import get_job_status
        status = get_job_status()
        
        return jsonify({
            'status': 'ok',
            **status
        })
    except ImportError as e:
        # Scheduler module not running in same process
        return jsonify({
            'status': 'unavailable',
            'message': f'Scheduler status not available: {str(e)}',
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/job_metrics', methods=['GET'])
def get_job_metrics():
    """
    Get job execution metrics with execution time analysis.
    
    Query parameters:
    - hours: Number of hours of history to analyze (default: 1)
    
    Returns per-job statistics:
    - avg_duration_ms: Average execution duration
    - max_duration_ms: Maximum execution duration  
    - min_duration_ms: Minimum execution duration
    - execution_count: Number of executions in time window
    - error_count: Number of failed executions
    - expected_interval_ms: Expected job interval
    - is_slow: True if avg duration > 80% of expected interval
    - recent_executions: Last 50 executions with timestamps and durations
    
    Query parameters:
    - hours: Number of hours of history (default: 1, supports decimals like 0.083 for 5 min)
    - minutes: Alternative to hours - number of minutes (takes precedence if provided)
    """
    # Support both minutes and hours parameters
    minutes = request.args.get('minutes', type=int)
    if minutes is not None:
        # Convert minutes to hours (as decimal)
        hours = max(5, min(1440, minutes)) / 60.0
    else:
        hours = request.args.get('hours', 1, type=float)
        # Validate hours (0.083 = 5 min to 24 hours)
        hours = max(0.083, min(24, hours))
    
    try:
        from scheduler.status import get_job_metrics as fetch_metrics
        metrics = fetch_metrics(hours=hours)
        return jsonify(metrics)
        
    except ImportError as e:
        # Scheduler module not running in same process
        return jsonify({
            'status': 'unavailable',
            'message': f'Job metrics not available: {str(e)}',
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'jobs': {},
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/job_metrics_debug', methods=['GET'])
def get_job_metrics_debug():
    """Debug endpoint to check metrics table directly."""
    try:
        from core.database import get_duckdb
        from scheduler.status import _metrics_table_initialized, _metrics_writer_running
        
        result = {
            'metrics_table_initialized': _metrics_table_initialized,
            'metrics_writer_running': _metrics_writer_running,
        }
        
        try:
            with get_duckdb("central") as conn:
                # Check if table exists
                tables = conn.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_name = 'job_execution_metrics'
                """).fetchall()
                result['table_exists'] = len(tables) > 0
                
                if result['table_exists']:
                    count = conn.execute("SELECT COUNT(*) FROM job_execution_metrics").fetchone()
                    result['row_count'] = count[0]
                    
                    # Get sample rows
                    rows = conn.execute("""
                        SELECT job_id, status, duration_ms, started_at 
                        FROM job_execution_metrics 
                        ORDER BY started_at DESC LIMIT 5
                    """).fetchall()
                    result['sample_rows'] = [
                        {'job_id': r[0], 'status': r[1], 'duration_ms': r[2], 'started_at': str(r[3])}
                        for r in rows
                    ]
        except Exception as db_error:
            result['db_error'] = str(db_error)
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# PLAYS ENDPOINTS
# =============================================================================

@app.route('/plays', methods=['GET'])
def get_plays():
    """Get all plays (from MySQL - master data)."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, created_at, name, description, sorting, short_play, 
                           is_active, live_trades, max_buys_per_cycle
                    FROM follow_the_goat_plays 
                    ORDER BY sorting ASC, id DESC
                """)
                plays = cursor.fetchall()
        
        # Convert datetime objects to strings
        for play in plays:
            if play.get('created_at'):
                play['created_at'] = play['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({'plays': plays, 'count': len(plays)})
    except Exception as e:
        return jsonify({'error': str(e), 'plays': []}), 500


@app.route('/plays/<int:play_id>', methods=['GET'])
def get_play(play_id):
    """Get a single play by ID."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays WHERE id = %s
                """, [play_id])
                play = cursor.fetchone()
        
        if not play:
            return jsonify({'error': 'Play not found'}), 404
        
        # Convert datetime and parse JSON fields
        if play.get('created_at'):
            play['created_at'] = play['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({'play': play})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/plays/<int:play_id>/for_edit', methods=['GET'])
def get_play_for_edit(play_id):
    """Get a single play with all fields needed for editing."""
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays WHERE id = %s
                """, [play_id])
                play = cursor.fetchone()
        
        if not play:
            return jsonify({'success': False, 'error': 'Play not found'}), 404
        
        # Helper to safely parse JSON fields
        def safe_json_parse(value, default):
            if not value:
                return default
            try:
                return json.loads(value) if isinstance(value, str) else value
            except (json.JSONDecodeError, TypeError):
                return default
        
        # Parse JSON fields for frontend
        result = {
            'success': True,
            'id': play['id'],
            'name': play['name'],
            'description': play['description'],
            'find_wallets_sql': safe_json_parse(play.get('find_wallets_sql'), {'query': ''}),
            'sell_logic': safe_json_parse(play.get('sell_logic'), {'tolerance_rules': {'increases': [], 'decreases': []}}),
            'max_buys_per_cycle': play.get('max_buys_per_cycle', 5),
            'short_play': play.get('short_play', 0),
            'trigger_on_perp': safe_json_parse(play.get('tricker_on_perp'), {'mode': 'any'}),
            'timing_conditions': safe_json_parse(play.get('timing_conditions'), {'enabled': False}),
            'bundle_trades': safe_json_parse(play.get('bundle_trades'), {'enabled': False}),
            'cashe_wallets': safe_json_parse(play.get('cashe_wallets'), {'enabled': False}),
            'pattern_validator_enable': play.get('pattern_validator_enable', 0),
            'pattern_update_by_ai': play.get('pattern_update_by_ai', 0),
            'project_ids': safe_json_parse(play.get('project_ids'), []),
        }
        
        return jsonify(result)
    except Exception as e:
        import traceback
        logger.error(f"Error in get_play_for_edit: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays', methods=['POST'])
def create_play():
    """
    Create a new play (writes to MySQL, DuckDB sync happens via scheduler).
    
    Request JSON: Play fields
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Required fields
        required = ['name', 'description', 'find_wallets_sql']
        for field in required:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        # Build JSON fields
        find_wallets_sql = json.dumps({'query': data['find_wallets_sql']})
        sell_logic = json.dumps(data.get('sell_logic', {'tolerance_rules': {'increases': [], 'decreases': []}}))
        tricker_on_perp = json.dumps(data.get('trigger_on_perp', {'mode': 'any'}))
        timing_conditions = json.dumps(data.get('timing_conditions', {'enabled': False}))
        bundle_trades = json.dumps(data.get('bundle_trades', {'enabled': False}))
        cashe_wallets = json.dumps(data.get('cashe_wallets', {'enabled': False}))
        project_ids = json.dumps(data.get('project_ids', [])) if data.get('project_ids') else None
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO follow_the_goat_plays 
                    (name, description, find_wallets_sql, sell_logic, max_buys_per_cycle, 
                     short_play, tricker_on_perp, timing_conditions, bundle_trades, 
                     cashe_wallets, project_ids, is_active, sorting, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 10, NOW())
                """, [
                    data['name'],
                    data['description'],
                    find_wallets_sql,
                    sell_logic,
                    data.get('max_buys_per_cycle', 5),
                    data.get('short_play', 0),
                    tricker_on_perp,
                    timing_conditions,
                    bundle_trades,
                    cashe_wallets,
                    project_ids
                ])
                new_id = cursor.lastrowid
        
        return jsonify({'success': True, 'id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>', methods=['PUT'])
def update_play(play_id):
    """
    Update a play.
    
    Request JSON: Fields to update
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Build update query dynamically
        updates = []
        values = []
        
        # Simple fields
        simple_fields = ['name', 'description', 'max_buys_per_cycle', 'short_play', 
                        'sorting', 'is_active', 'pattern_validator_enable', 'pattern_update_by_ai']
        for field in simple_fields:
            if field in data:
                updates.append(f"{field} = %s")
                values.append(data[field])
        
        # JSON fields that need encoding
        json_fields = {
            'find_wallets_sql': lambda x: json.dumps({'query': x}) if isinstance(x, str) else json.dumps(x),
            'sell_logic': json.dumps,
            'trigger_on_perp': lambda x: json.dumps(x),  # Note: stored as tricker_on_perp
            'timing_conditions': json.dumps,
            'bundle_trades': json.dumps,
            'cashe_wallets': json.dumps,
            'project_ids': lambda x: json.dumps(x) if x else None,
            'pattern_validator': json.dumps,
        }
        
        # Map frontend field names to database column names
        field_mapping = {
            'trigger_on_perp': 'tricker_on_perp'
        }
        
        for field, encoder in json_fields.items():
            if field in data:
                db_field = field_mapping.get(field, field)
                updates.append(f"{db_field} = %s")
                values.append(encoder(data[field]))
        
        if not updates:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400
        
        values.append(play_id)
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE follow_the_goat_plays SET {', '.join(updates)} WHERE id = %s",
                    values
                )
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>', methods=['DELETE'])
def delete_play(play_id):
    """Delete a play."""
    try:
        # Prevent deletion of restricted plays (e.g., play 46)
        if play_id == 46:
            return jsonify({'success': False, 'error': 'This play cannot be deleted'}), 403
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # First delete related trades
                cursor.execute("DELETE FROM follow_the_goat_buyins WHERE play_id = %s", [play_id])
                cursor.execute("DELETE FROM follow_the_goat_buyins_archive WHERE play_id = %s", [play_id])
                
                # Then delete the play
                cursor.execute("DELETE FROM follow_the_goat_plays WHERE id = %s", [play_id])
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>/duplicate', methods=['POST'])
def duplicate_play(play_id):
    """Duplicate a play with a new name."""
    try:
        data = request.get_json() or {}
        new_name = data.get('new_name')
        
        if not new_name:
            return jsonify({'success': False, 'error': 'new_name is required'}), 400
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get original play
                cursor.execute("SELECT * FROM follow_the_goat_plays WHERE id = %s", [play_id])
                original = cursor.fetchone()
                
                if not original:
                    return jsonify({'success': False, 'error': 'Play not found'}), 404
                
                # Insert duplicate with new name
                cursor.execute("""
                    INSERT INTO follow_the_goat_plays 
                    (name, description, find_wallets_sql, sell_logic, max_buys_per_cycle,
                     short_play, tricker_on_perp, timing_conditions, bundle_trades,
                     cashe_wallets, pattern_validator, pattern_validator_enable,
                     pattern_update_by_ai, project_ids, is_active, sorting, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, [
                    new_name,
                    original.get('description'),
                    original.get('find_wallets_sql'),
                    original.get('sell_logic'),
                    original.get('max_buys_per_cycle', 5),
                    original.get('short_play', 0),
                    original.get('tricker_on_perp'),
                    original.get('timing_conditions'),
                    original.get('bundle_trades'),
                    original.get('cashe_wallets'),
                    original.get('pattern_validator'),
                    original.get('pattern_validator_enable', 0),
                    original.get('pattern_update_by_ai', 0),
                    original.get('project_ids'),
                    1,  # is_active
                    original.get('sorting', 10)
                ])
                new_id = cursor.lastrowid
        
        return jsonify({'success': True, 'new_id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>/performance', methods=['GET'])
def get_play_performance(play_id):
    """
    Get performance metrics for a single play.
    
    Query params:
        hours: Time window (default: 'all', or number like 24, 12, 6, 2)
    
    Note: All trades are now in the live buyins table (no archive in new solution).
    """
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filters
        time_filter_pending = ""
        time_filter_no_go = ""
        time_filter_sold = ""
        if hours != 'all':
            hours_int = int(hours)
            time_filter_pending = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
            time_filter_no_go = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
            time_filter_sold = f"AND our_exit_timestamp >= NOW() - INTERVAL {hours_int} HOUR"
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get active (pending) trades stats
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as active_trades,
                        AVG(CASE 
                            WHEN our_entry_price > 0 AND current_price > 0 
                            THEN ((current_price - our_entry_price) / our_entry_price) * 100 
                            ELSE NULL 
                        END) as active_avg_profit
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s AND our_status = 'pending' {time_filter_pending}
                """, [play_id])
                live_stats = cursor.fetchone()
                
                # Get no_go count from live table
                cursor.execute(f"""
                    SELECT COUNT(*) as no_go_count
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s AND our_status = 'no_go' {time_filter_no_go}
                """, [play_id])
                no_go_result = cursor.fetchone()
                
                # Get sold/completed trades stats from live table
                cursor.execute(f"""
                    SELECT 
                        SUM(our_profit_loss) as total_profit_loss,
                        COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s AND our_status IN ('sold', 'completed') {time_filter_sold}
                """, [play_id])
                sold_stats = cursor.fetchone()
        
        return jsonify({
            'success': True,
            'play_id': play_id,
            'total_profit_loss': float(sold_stats['total_profit_loss'] or 0),
            'winning_trades': sold_stats['winning_trades'] or 0,
            'losing_trades': sold_stats['losing_trades'] or 0,
            'total_no_gos': no_go_result['no_go_count'] or 0,
            'active_trades': live_stats['active_trades'] or 0,
            'active_avg_profit': float(live_stats['active_avg_profit']) if live_stats['active_avg_profit'] else None
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/performance', methods=['GET'])
def get_all_plays_performance():
    """
    Get performance metrics for all plays (batch operation).
    
    Query params:
        hours: Time window (default: 'all', or number like 24, 12, 6, 2)
    
    Note: no_go trades live in the buyins table until cleanup, so we count them
    from both the live buyins table AND the archive table for accurate totals.
    """
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filters for different tables
        time_filter_pending = ""  # For pending trades: use followed_at
        time_filter_no_go = ""    # For no_go trades: use followed_at (set on insert)
        time_filter_archive = ""  # For archived trades: use our_exit_timestamp
        
        if hours != 'all':
            try:
                hours_int = int(hours)
                time_filter_pending = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
                time_filter_no_go = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
                time_filter_archive = f"AND our_exit_timestamp >= NOW() - INTERVAL {hours_int} HOUR"
            except ValueError:
                pass  # Keep empty filters if hours is invalid
        
        plays_data = {}
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get all play IDs
                cursor.execute("SELECT id FROM follow_the_goat_plays ORDER BY id")
                play_ids = [row['id'] for row in cursor.fetchall()]
                
                # Get live trades stats for all plays (pending trades only)
                cursor.execute(f"""
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
                """)
                live_stats = {row['play_id']: row for row in cursor.fetchall()}
                
                # Get no_go counts from live buyins table
                cursor.execute(f"""
                    SELECT 
                        play_id,
                        COUNT(*) as no_go_count
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'no_go' {time_filter_no_go}
                    GROUP BY play_id
                """)
                no_go_stats = {row['play_id']: row['no_go_count'] for row in cursor.fetchall()}
                
                # Get sold/completed trades stats from live table (archive is deprecated)
                time_filter_sold = ""
                if hours != 'all':
                    try:
                        hours_int = int(hours)
                        time_filter_sold = f"AND our_exit_timestamp >= NOW() - INTERVAL {hours_int} HOUR"
                    except ValueError:
                        pass
                        
                cursor.execute(f"""
                    SELECT 
                        play_id,
                        SUM(our_profit_loss) as total_profit_loss,
                        COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades
                    FROM follow_the_goat_buyins
                    WHERE our_status IN ('sold', 'completed') {time_filter_sold}
                    GROUP BY play_id
                """)
                sold_stats = {row['play_id']: row for row in cursor.fetchall()}
        
        # Combine stats for each play - use string keys for JavaScript compatibility
        for play_id in play_ids:
            live = live_stats.get(play_id, {})
            sold = sold_stats.get(play_id, {})
            
            # Use string keys for consistent JavaScript access
            plays_data[str(play_id)] = {
                'total_profit_loss': float(sold.get('total_profit_loss') or 0),
                'winning_trades': int(sold.get('winning_trades') or 0),
                'losing_trades': int(sold.get('losing_trades') or 0),
                'total_no_gos': no_go_stats.get(play_id, 0),
                'active_trades': int(live.get('active_trades') or 0),
                'active_avg_profit': float(live.get('active_avg_profit')) if live.get('active_avg_profit') else None
            }
        
        return jsonify({
            'success': True,
            'plays': plays_data
        })
    except Exception as e:
        import traceback
        logger.error(f"Error in get_all_plays_performance: {traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# BUYINS (TRADES) ENDPOINTS
# =============================================================================

@app.route('/buyins', methods=['GET'])
def get_buyins():
    """
    Get buyins/trades.
    
    Query params:
        play_id: Filter by play ID
        status: Filter by status (pending, sold, no_go, etc.)
        hours: Limit to last N hours (default: 24, use 'all' for MySQL)
        limit: Max records (default: 100)
    """
    try:
        play_id = request.args.get('play_id', type=int)
        status = request.args.get('status')
        hours = request.args.get('hours', '24')
        limit = request.args.get('limit', 100, type=int)
        
        # Determine source
        use_duckdb = hours != 'all'
        hours_int = int(hours) if hours != 'all' else None
        
        where_clauses = []
        params = []
        
        if play_id:
            where_clauses.append("play_id = %s" if not use_duckdb else "play_id = ?")
            params.append(play_id)
        
        if status:
            where_clauses.append("our_status = %s" if not use_duckdb else "our_status = ?")
            params.append(status)
        
        if hours_int:
            # Both DuckDB and MySQL support INTERVAL N HOUR syntax
            # hours_int is validated as integer, so safe for string formatting
            where_clauses.append(f"followed_at >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
            SELECT id, play_id, wallet_address, tolerance, block_timestamp, price,
                   followed_at, our_entry_price, our_exit_price, our_exit_timestamp,
                   our_profit_loss, our_status, higest_price_reached, current_price
            FROM follow_the_goat_buyins
            WHERE {where_str}
            ORDER BY followed_at DESC
            LIMIT {limit}
        """
        
        if use_duckdb:
            with get_duckdb("central") as conn:
                result = conn.execute(query, params).fetchall()
                columns = [desc[0] for desc in conn.description]
                buyins = [dict(zip(columns, row)) for row in result]
        else:
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    buyins = cursor.fetchall()
        
        # Convert timestamps
        for buyin in buyins:
            for key in ['block_timestamp', 'followed_at', 'our_exit_timestamp']:
                if buyin.get(key) and hasattr(buyin[key], 'strftime'):
                    buyin[key] = buyin[key].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'buyins': buyins,
            'count': len(buyins),
            'source': 'duckdb' if use_duckdb else 'mysql'
        })
    except Exception as e:
        return jsonify({'error': str(e), 'buyins': []}), 500


@app.route('/buyins', methods=['POST'])
def create_buyin():
    """
    Create a new buyin/trade (dual-write).
    
    Request JSON: All buyin fields
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        # Set defaults
        if 'followed_at' not in data:
            data['followed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if 'our_status' not in data:
            data['our_status'] = 'pending'
        
        duckdb_ok, mysql_ok = dual_write_insert('follow_the_goat_buyins', data)
        
        return jsonify({
            'success': duckdb_ok and mysql_ok,
            'duckdb': duckdb_ok,
            'mysql': mysql_ok
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/buyins/<int:buyin_id>', methods=['PUT'])
def update_buyin(buyin_id):
    """
    Update a buyin/trade (dual-write).
    
    Request JSON: Fields to update
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        duckdb_ok, mysql_ok = dual_write_update(
            'follow_the_goat_buyins',
            data,
            {'id': buyin_id}
        )
        
        return jsonify({
            'success': duckdb_ok or mysql_ok,
            'duckdb': duckdb_ok,
            'mysql': mysql_ok
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/buyins/<int:buyin_id>', methods=['GET'])
def get_single_buyin(buyin_id):
    """Get a single buyin/trade by ID from the live table only."""
    try:
        # Always use live table (archive is deprecated)
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM follow_the_goat_buyins WHERE id = %s", [buyin_id])
                buyin = cursor.fetchone()
        
        if not buyin:
            return jsonify({'success': False, 'error': 'Trade not found'}), 404
        
        # Convert datetime fields
        for key in ['block_timestamp', 'followed_at', 'our_exit_timestamp', 'created_at']:
            if buyin.get(key) and hasattr(buyin[key], 'strftime'):
                buyin[key] = buyin[key].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({'success': True, 'buyin': buyin, 'source': 'live'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/buyins/cleanup_no_gos', methods=['DELETE'])
def cleanup_no_gos():
    """
    Delete all no_go trades older than 24 hours from the live table.
    These trades are already archived in MySQL so this is just cleanup.
    """
    try:
        deleted_count = 0
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Count before delete
                cursor.execute("""
                    SELECT COUNT(*) as cnt FROM follow_the_goat_buyins 
                    WHERE our_status = 'no_go' 
                    AND followed_at < NOW() - INTERVAL 24 HOUR
                """)
                count_result = cursor.fetchone()
                deleted_count = count_result['cnt'] if count_result else 0
                
                if deleted_count > 0:
                    # Delete from live table
                    cursor.execute("""
                        DELETE FROM follow_the_goat_buyins 
                        WHERE our_status = 'no_go' 
                        AND followed_at < NOW() - INTERVAL 24 HOUR
                    """)
        
        # Also clean up from DuckDB
        try:
            with get_duckdb("central") as conn:
                conn.execute("""
                    DELETE FROM follow_the_goat_buyins 
                    WHERE our_status = 'no_go' 
                    AND followed_at < NOW() - INTERVAL 24 HOUR
                """)
        except:
            pass  # DuckDB cleanup is optional
        
        return jsonify({
            'success': True,
            'deleted': deleted_count,
            'message': f'Deleted {deleted_count} no_go trades older than 24 hours'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# PRICE CHECKS ENDPOINTS
# =============================================================================

@app.route('/price_checks', methods=['GET'])
def get_price_checks():
    """Get price checks for a buyin."""
    try:
        buyin_id = request.args.get('buyin_id', type=int)
        hours = request.args.get('hours', '24')
        limit = request.args.get('limit', 100, type=int)
        
        if not buyin_id:
            return jsonify({'error': 'buyin_id required'}), 400
        
        use_duckdb = hours != 'all'
        hours_int = int(hours) if hours != 'all' else None
        
        where_clauses = ["buyin_id = ?" if use_duckdb else "buyin_id = %s"]
        params = [buyin_id]
        
        if use_duckdb and hours_int:
            where_clauses.append(f"checked_at >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_str = " AND ".join(where_clauses)
        
        query = f"""
            SELECT * FROM follow_the_goat_buyins_price_checks
            WHERE {where_str}
            ORDER BY checked_at DESC
            LIMIT {limit}
        """
        
        if use_duckdb:
            with get_duckdb("central") as conn:
                result = conn.execute(query, params).fetchall()
                columns = [desc[0] for desc in conn.description]
                checks = [dict(zip(columns, row)) for row in result]
        else:
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    checks = cursor.fetchall()
        
        return jsonify({
            'price_checks': checks,
            'count': len(checks),
            'source': 'duckdb' if use_duckdb else 'mysql'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/price_checks', methods=['POST'])
def create_price_check():
    """Create a new price check (dual-write)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        if 'created_at' not in data:
            data['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        duckdb_ok, mysql_ok = dual_write_insert('follow_the_goat_buyins_price_checks', data)
        
        return jsonify({
            'success': duckdb_ok and mysql_ok,
            'duckdb': duckdb_ok,
            'mysql': mysql_ok
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# PRICE POINTS ENDPOINTS (Legacy + New)
# =============================================================================

@app.route('/price_points', methods=['POST'])
def get_price_points():
    """
    Get price points for charting.
    
    Uses TradingDataEngine (in-memory DuckDB) for instant reads with zero lock contention.
    Falls back to file-based DuckDB if engine not running.
    
    Request JSON:
    {
        "token": "SOL",  # Token symbol (BTC, ETH, SOL)
        "start_datetime": "2024-01-01 00:00:00",
        "end_datetime": "2024-01-02 00:00:00"
    }
    """
    try:
        data = request.get_json() or {}
        
        token = data.get('token', 'SOL')
        end_datetime = data.get('end_datetime', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        if 'start_datetime' in data:
            start_datetime = data['start_datetime']
        else:
            start_dt = datetime.now() - timedelta(hours=24)
            start_datetime = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        prices = []
        source = 'none'
        
        # Try TradingDataEngine first (in-memory, zero locks)
        try:
            engine = get_trading_engine()
            if engine._running:
                # Use >= and <= for inclusive range (BETWEEN can be ambiguous with timezone)
                results = engine.read("""
                    SELECT ts, price 
                    FROM prices 
                    WHERE token = ? 
                      AND ts >= ? 
                      AND ts <= ?
                    ORDER BY ts ASC
                """, [token, start_datetime, end_datetime])
                
                prices = [
                    {'x': row['ts'].strftime('%Y-%m-%d %H:%M:%S'), 'y': float(row['price'])}
                    for row in results
                ]
                if prices:
                    source = 'engine'
        except:
            pass
        
        # Fallback to file-based DuckDB if engine not available or returned no data
        if not prices:
            import duckdb
            legacy_db = DATABASES.get("prices")
            
            try:
                with duckdb.connect(str(legacy_db), read_only=True) as conn:
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
                if prices:
                    source = 'duckdb'
            except:
                pass
        
        # Fallback to MySQL for historical data (older than 24 hours)
        if not prices:
            try:
                from core.database import get_mysql
                
                # Check if we're requesting historical data (more than 24h ago)
                end_dt = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M:%S')
                cutoff = datetime.now() - timedelta(hours=24)
                
                # Map token to coin_id (SOL=5, BTC=6, ETH=7)
                token_to_coin_id = {'SOL': 5, 'BTC': 6, 'ETH': 7}
                coin_id = token_to_coin_id.get(token, 5)
                
                if end_dt < cutoff:
                    # Historical data - fetch from MySQL
                    with get_mysql() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("""
                                SELECT created_at, value
                                FROM price_points
                                WHERE coin_id = %s
                                  AND created_at BETWEEN %s AND %s
                                ORDER BY created_at ASC
                            """, [coin_id, start_datetime, end_datetime])
                            
                            rows = cursor.fetchall()
                            prices = [
                                {'x': row['created_at'].strftime('%Y-%m-%d %H:%M:%S'), 'y': float(row['value'])}
                                for row in rows
                            ]
                            if prices:
                                source = 'mysql'
            except Exception as e:
                logger.debug(f"MySQL fallback failed: {e}")
                pass
        
        return jsonify({
            'prices': prices,
            'count': len(prices),
            'source': source
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'prices': [],
            'count': 0
        }), 500


@app.route('/latest_prices', methods=['GET'])
def get_latest_prices():
    """Get the latest price for each token.
    
    Uses TradingDataEngine (in-memory) for instant reads with zero lock contention.
    """
    try:
        prices = {}
        
        # Try TradingDataEngine first
        try:
            engine = get_trading_engine()
            if engine._running:
                results = engine.read("""
                    SELECT token, price, ts
                    FROM prices
                    WHERE (token, ts) IN (
                        SELECT token, MAX(ts) FROM prices GROUP BY token
                    )
                """)
                
                prices = {
                    row['token']: {
                        'price': float(row['price']),
                        'ts': row['ts'].strftime('%Y-%m-%d %H:%M:%S')
                    }
                    for row in results
                }
        except:
            pass
        
        # Fallback to file-based DuckDB
        if not prices:
            import duckdb
            legacy_db = DATABASES.get("prices")
            
            with duckdb.connect(str(legacy_db), read_only=True) as conn:
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


# =============================================================================
# PRICE ANALYSIS & CYCLE TRACKER ENDPOINTS
# =============================================================================

@app.route('/price_analysis', methods=['GET'])
def get_price_analysis():
    """Get price analysis data from TradingDataEngine (in-memory DuckDB).
    
    Uses in-memory DuckDB for instant reads with zero lock contention.
    """
    try:
        hours = request.args.get('hours', '24')
        coin_id = request.args.get('coin_id', 5, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        data = []
        source = 'engine'
        
        # Try TradingDataEngine first (in-memory, instant)
        try:
            engine = get_trading_engine()
            if engine._running:
                where_clauses = ["coin_id = ?"]
                params = [coin_id]
                
                if hours != 'all':
                    hours_int = int(hours)
                    where_clauses.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
                
                where_str = " AND ".join(where_clauses)
                
                query = f"""
                    SELECT * FROM price_analysis
                    WHERE {where_str}
                    ORDER BY created_at DESC
                    LIMIT {limit}
                """
                
                data = engine.read(query, params)
        except Exception as e:
            source = 'engine_error'
        
        # Fallback to MySQL if engine not available or no data
        if not data:
            source = 'mysql'
            where_clauses = ["coin_id = %s"]
            params = [coin_id]
            
            if hours != 'all':
                hours_int = int(hours)
                where_clauses.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
            
            where_str = " AND ".join(where_clauses)
            
            query = f"""
                SELECT * FROM price_analysis
                WHERE {where_str}
                ORDER BY created_at DESC
                LIMIT {limit}
            """
            
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    data = cursor.fetchall()
        
        return jsonify({
            'price_analysis': data,
            'count': len(data),
            'source': source
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/cycle_tracker', methods=['GET'])
def get_cycle_tracker():
    """Get cycle tracker data from TradingDataEngine (in-memory DuckDB).
    
    Uses in-memory DuckDB for instant reads with zero lock contention.
    
    Note: Time filter uses 'created_at' (when record was inserted) not 'cycle_start_time'
    (when price movement started). This is important when processing historical data.
    """
    try:
        hours = request.args.get('hours', 'all')  # Default to all for better UX
        threshold = request.args.get('threshold', type=float)
        limit = request.args.get('limit', 100, type=int)
        
        data = []
        source = 'engine'
        
        # Determine if we need historical data (beyond 24 hours)
        # DuckDB only holds 24 hours, so use MySQL for historical queries
        use_mysql_for_history = False
        if hours == 'all':
            # "All" means all historical data - must use MySQL
            use_mysql_for_history = True
        elif hours != 'all':
            hours_int = int(hours)
            if hours_int > 24:
                # Requesting more than 24 hours - must use MySQL
                use_mysql_for_history = True
        
        # Try TradingDataEngine first (in-memory, instant) - only for recent data (24h or less)
        if not use_mysql_for_history:
            try:
                engine = get_trading_engine()
                if engine._running:
                    where_clauses = []
                    params = []
                    
                    if threshold:
                        where_clauses.append("threshold = ?")
                        params.append(threshold)
                    
                    # DuckDB only holds 24 hours - always limit to last 24h even if hours='all'
                    # For DuckDB, "all" means "all available data" which is max 24 hours
                    if hours == 'all':
                        where_clauses.append("created_at >= NOW() - INTERVAL 24 HOUR")
                    else:
                        hours_int = int(hours)
                        where_clauses.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
                    
                    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
                    
                    query = f"""
                        SELECT * FROM cycle_tracker
                        WHERE {where_str}
                        ORDER BY id DESC
                        LIMIT {limit}
                    """
                    
                    data = engine.read(query, params)
                    if data:
                        source = 'engine'
            except Exception as e:
                source = 'engine_error'
        
        # Use MySQL for historical data (beyond 24h) or if engine not available/no data
        if use_mysql_for_history or not data:
            source = 'mysql'
            where_clauses = []
            params = []
            
            if threshold:
                where_clauses.append("threshold = %s")
                params.append(threshold)
            
            # MySQL can handle all historical data
            if hours != 'all':
                hours_int = int(hours)
                where_clauses.append(f"created_at >= NOW() - INTERVAL {hours_int} HOUR")
            # If hours='all', no time filter - MySQL returns all historical data
            
            where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query = f"""
                SELECT * FROM cycle_tracker
                WHERE {where_str}
                ORDER BY id DESC
                LIMIT {limit}
            """
            
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    data = cursor.fetchall()
        
        return jsonify({
            'cycles': data,
            'count': len(data),
            'source': source
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/profiles', methods=['GET'])
def get_profiles():
    """Get wallet profiles from TradingDataEngine (in-memory DuckDB).
    
    Uses in-memory DuckDB for instant reads with zero lock contention.
    
    Query Parameters:
        - threshold: Filter by threshold value (e.g., 0.3)
        - hours: Time window ('all', '1', '24', etc.) - default 'all'
        - limit: Max records to return (default 100)
        - order_by: Ordering ('recent', 'trade_count') - default 'recent'
        - wallet: Filter by specific wallet address
    """
    try:
        hours = request.args.get('hours', 'all')
        threshold = request.args.get('threshold', type=float)
        limit = request.args.get('limit', 100, type=int)
        order_by = request.args.get('order_by', 'recent')
        wallet = request.args.get('wallet')
        
        data = []
        source = 'engine'
        
        # Determine if we need historical data (beyond 24 hours)
        use_mysql_for_history = False
        if hours == 'all':
            use_mysql_for_history = True
        elif hours != 'all':
            hours_int = int(hours)
            if hours_int > 24:
                use_mysql_for_history = True
        
        # Build ORDER BY clause - always aggregate to get distinct wallets
        if order_by == 'trade_count':
            order_clause = "trade_count DESC, latest_trade DESC"
        elif order_by == 'recent':
            order_clause = "latest_trade DESC"
        else:
            # Default: order by avg gain descending
            order_clause = "avg_potential_gain DESC, trade_count DESC"
        use_aggregation = True
        
        # Try TradingDataEngine first (in-memory, instant) - only for recent data (24h or less)
        if not use_mysql_for_history:
            try:
                engine = get_trading_engine()
                if engine._running:
                    where_clauses = []
                    params = []
                    
                    if threshold:
                        where_clauses.append("threshold = ?")
                        params.append(threshold)
                    
                    if wallet:
                        where_clauses.append("wallet_address = ?")
                        params.append(wallet)
                    
                    # DuckDB only holds 24 hours
                    if hours == 'all':
                        where_clauses.append("trade_timestamp >= NOW() - INTERVAL 24 HOUR")
                    else:
                        hours_int = int(hours)
                        where_clauses.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
                    
                    where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
                    
                    if use_aggregation:
                        query = f"""
                            SELECT * FROM (
                                SELECT 
                                    wallet_address,
                                    threshold,
                                    COUNT(*) as trade_count,
                                    MAX(trade_timestamp) as latest_trade,
                                    MIN(trade_timestamp) as earliest_trade,
                                    AVG(trade_entry_price) as avg_entry_price,
                                    COALESCE(SUM(stablecoin_amount), 0) as total_invested,
                                    AVG(CASE WHEN trade_entry_price > 0 THEN (highest_price_reached - trade_entry_price) / trade_entry_price * 100 ELSE 0 END) as avg_potential_gain,
                                    SUM(CASE WHEN trade_entry_price > 0 AND (highest_price_reached - trade_entry_price) / trade_entry_price * 100 < threshold THEN 1 ELSE 0 END) as trades_below_threshold,
                                    SUM(CASE WHEN trade_entry_price > 0 AND (highest_price_reached - trade_entry_price) / trade_entry_price * 100 >= threshold THEN 1 ELSE 0 END) as trades_at_above_threshold
                                FROM wallet_profiles
                                WHERE {where_str}
                                GROUP BY wallet_address, threshold
                            ) AS subq
                            ORDER BY {order_clause}
                            LIMIT {limit}
                        """
                    else:
                        query = f"""
                            SELECT * FROM wallet_profiles
                            WHERE {where_str}
                            ORDER BY {order_clause}
                            LIMIT {limit}
                        """
                    
                    data = engine.read(query, params)
                    if data:
                        source = 'engine'
            except Exception as e:
                source = 'engine_error'
        
        # Use MySQL for historical data (beyond 24h) or if engine not available/no data
        if use_mysql_for_history or not data:
            source = 'mysql'
            where_clauses = []
            params = []
            
            if threshold:
                where_clauses.append("threshold = %s")
                params.append(threshold)
            
            if wallet:
                where_clauses.append("wallet_address = %s")
                params.append(wallet)
            
            # MySQL can handle all historical data
            if hours != 'all':
                hours_int = int(hours)
                where_clauses.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
            
            where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            if use_aggregation:
                query = f"""
                    SELECT * FROM (
                        SELECT 
                            wallet_address,
                            threshold,
                            COUNT(*) as trade_count,
                            MAX(trade_timestamp) as latest_trade,
                            MIN(trade_timestamp) as earliest_trade,
                            AVG(trade_entry_price) as avg_entry_price,
                            COALESCE(SUM(stablecoin_amount), 0) as total_invested,
                            AVG(CASE WHEN trade_entry_price > 0 THEN (highest_price_reached - trade_entry_price) / trade_entry_price * 100 ELSE 0 END) as avg_potential_gain,
                            SUM(CASE WHEN trade_entry_price > 0 AND (highest_price_reached - trade_entry_price) / trade_entry_price * 100 < threshold THEN 1 ELSE 0 END) as trades_below_threshold,
                            SUM(CASE WHEN trade_entry_price > 0 AND (highest_price_reached - trade_entry_price) / trade_entry_price * 100 >= threshold THEN 1 ELSE 0 END) as trades_at_above_threshold
                        FROM wallet_profiles
                        WHERE {where_str}
                        GROUP BY wallet_address, threshold
                    ) AS subq
                    ORDER BY {order_clause}
                    LIMIT {limit}
                """
            else:
                query = f"""
                    SELECT * FROM wallet_profiles
                    WHERE {where_str}
                    ORDER BY trade_timestamp DESC
                    LIMIT {limit}
                """
            
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    data = cursor.fetchall()
        
        return jsonify({
            'profiles': data,
            'count': len(data),
            'source': source,
            'aggregated': use_aggregation
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/profiles/stats', methods=['GET'])
def get_profiles_stats():
    """Get aggregate statistics for wallet profiles.
    
    Query Parameters:
        - threshold: Filter by threshold value
        - hours: Time window ('all', '1', '24', etc.)
    """
    try:
        hours = request.args.get('hours', 'all')
        threshold = request.args.get('threshold', type=float)
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        if threshold:
            where_clauses.append("threshold = %s")
            params.append(threshold)
        
        if hours != 'all':
            hours_int = int(hours)
            where_clauses.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
        
        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Try TradingDataEngine first
        try:
            engine = get_trading_engine()
            if engine._running:
                duck_params = []
                duck_where_clauses = []
                
                if threshold:
                    duck_where_clauses.append("threshold = ?")
                    duck_params.append(threshold)
                
                if hours != 'all':
                    hours_int = int(hours)
                    duck_where_clauses.append(f"trade_timestamp >= NOW() - INTERVAL {hours_int} HOUR")
                
                duck_where_str = " AND ".join(duck_where_clauses) if duck_where_clauses else "1=1"
                
                result = engine.read_one(f"""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(DISTINCT wallet_address) as unique_wallets,
                        COUNT(DISTINCT price_cycle) as unique_cycles,
                        COUNT(DISTINCT threshold) as thresholds_used,
                        MIN(trade_timestamp) as earliest_trade,
                        MAX(trade_timestamp) as latest_trade,
                        AVG(trade_entry_price) as avg_entry_price,
                        COALESCE(SUM(stablecoin_amount), 0) as total_invested
                    FROM wallet_profiles
                    WHERE {duck_where_str}
                """, duck_params)
                
                if result and result.get('total_profiles', 0) > 0:
                    return jsonify({
                        'stats': result,
                        'source': 'engine'
                    })
        except Exception:
            pass
        
        # Fall back to MySQL
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(DISTINCT wallet_address) as unique_wallets,
                        COUNT(DISTINCT price_cycle) as unique_cycles,
                        COUNT(DISTINCT threshold) as thresholds_used,
                        MIN(trade_timestamp) as earliest_trade,
                        MAX(trade_timestamp) as latest_trade,
                        AVG(trade_entry_price) as avg_entry_price,
                        COALESCE(SUM(stablecoin_amount), 0) as total_invested
                    FROM wallet_profiles
                    WHERE {where_str}
                """, params)
                result = cursor.fetchone()
        
        return jsonify({
            'stats': result,
            'source': 'mysql'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# ADMIN ENDPOINTS
# =============================================================================

@app.route('/admin/init_tables', methods=['POST'])
def admin_init_tables():
    """Initialize DuckDB tables."""
    try:
        init_duckdb_tables("central")
        return jsonify({'success': True, 'message': 'Tables initialized'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/cleanup', methods=['POST'])
def admin_cleanup():
    """Clean up old data from DuckDB hot tables."""
    try:
        hours = request.args.get('hours', 24, type=int)
        cleaned = cleanup_all_hot_tables("central", hours)
        return jsonify({
            'success': True,
            'records_cleaned': cleaned,
            'hours_threshold': hours
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/sync_from_mysql', methods=['POST'])
def admin_sync_from_mysql():
    """
    Sync last 24 hours of data from MySQL to DuckDB.
    Use this for initial population or recovery.
    """
    try:
        hours = request.args.get('hours', 24, type=int)
        tables = request.args.getlist('tables') or HOT_TABLES
        
        results = {}
        
        for table in tables:
            if table not in HOT_TABLES and table != 'follow_the_goat_plays':
                results[table] = {'error': 'Not a valid table'}
                continue
            
            ts_col = TIMESTAMP_COLUMNS.get(table, 'created_at')
            
            # Get data from MySQL
            with get_mysql() as mysql_conn:
                with mysql_conn.cursor() as cursor:
                    if table == 'follow_the_goat_plays':
                        cursor.execute(f"SELECT * FROM {table}")
                    else:
                        cursor.execute(f"""
                            SELECT * FROM {table}
                            WHERE {ts_col} >= NOW() - INTERVAL {hours} HOUR
                        """)
                    rows = cursor.fetchall()
            
            if not rows:
                results[table] = {'synced': 0}
                continue
            
            # Clear and insert into DuckDB
            with get_duckdb("central") as conn:
                if table != 'follow_the_goat_plays':
                    conn.execute(f"DELETE FROM {table}")
                else:
                    conn.execute(f"DELETE FROM {table}")
                
                # Build insert
                columns = list(rows[0].keys())
                # Handle 15_min_trail column rename
                columns_mapped = ['fifteen_min_trail' if c == '15_min_trail' else c for c in columns]
                placeholders = ", ".join(["?" for _ in columns])
                columns_str = ", ".join(columns_mapped)
                
                for row in rows:
                    values = []
                    for col in columns:
                        val = row[col]
                        # Convert datetime to string
                        if hasattr(val, 'strftime'):
                            val = val.strftime('%Y-%m-%d %H:%M:%S')
                        # Convert dict/list to JSON string
                        elif isinstance(val, (dict, list)):
                            val = json.dumps(val)
                        values.append(val)
                    
                    try:
                        conn.execute(f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})", values)
                    except Exception as e:
                        logger.error(f"Error inserting row into {table}: {e}")
                
                results[table] = {'synced': len(rows)}
        
        return jsonify({
            'success': True,
            'results': results
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# PATTERN CONFIG ENDPOINTS
# =============================================================================

def ensure_pattern_tables_mysql():
    """Ensure pattern config tables exist in MySQL."""
    with get_mysql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `pattern_config_projects` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `name` VARCHAR(255) NOT NULL,
                    `description` TEXT NULL,
                    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX `idx_name` (`name`),
                    INDEX `idx_created_at` (`created_at`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `pattern_config_filters` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `project_id` INT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `section` VARCHAR(100) NULL,
                    `minute` TINYINT NULL,
                    `field_name` VARCHAR(100) NOT NULL,
                    `field_column` VARCHAR(100) NULL,
                    `from_value` DECIMAL(20,8) NULL,
                    `to_value` DECIMAL(20,8) NULL,
                    `include_null` TINYINT(1) DEFAULT 0,
                    `play_id` INT NULL,
                    `is_active` TINYINT(1) DEFAULT 1,
                    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX `idx_project_id` (`project_id`),
                    INDEX `idx_section_minute` (`section`, `minute`),
                    INDEX `idx_is_active` (`is_active`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)


@app.route('/patterns/projects', methods=['GET'])
def get_pattern_projects():
    """Get all pattern config projects with filter counts."""
    try:
        ensure_pattern_tables_mysql()
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        p.*,
                        COUNT(f.id) AS filter_count,
                        SUM(CASE WHEN f.is_active = 1 THEN 1 ELSE 0 END) AS active_filter_count
                    FROM pattern_config_projects p
                    LEFT JOIN pattern_config_filters f ON f.project_id = p.id
                    GROUP BY p.id
                    ORDER BY p.updated_at DESC, p.created_at DESC
                """)
                projects = cursor.fetchall()
        
        # Convert datetime objects
        for project in projects:
            if project.get('created_at'):
                project['created_at'] = project['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if project.get('updated_at'):
                project['updated_at'] = project['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({
            'success': True,
            'projects': projects,
            'count': len(projects)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects', methods=['POST'])
def create_pattern_project():
    """Create a new pattern config project (dual-write to MySQL + DuckDB)."""
    try:
        ensure_pattern_tables_mysql()
        
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        name = data.get('name', '').strip()
        description = data.get('description', '').strip() or None
        
        if not name:
            return jsonify({'success': False, 'error': 'Project name is required'}), 400
        
        # Write to MySQL first to get the auto-generated ID
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pattern_config_projects (name, description)
                    VALUES (%s, %s)
                """, [name, description])
                new_id = cursor.lastrowid
        
        # Also write to DuckDB (for consistency)
        try:
            with get_duckdb("central") as duckdb_conn:
                duckdb_conn.execute("""
                    INSERT INTO pattern_config_projects (id, name, description, created_at, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, [new_id, name, description])
        except Exception as e:
            logger.error(f"DuckDB write failed (MySQL succeeded): {e}")
        
        return jsonify({
            'success': True,
            'id': new_id,
            'message': f"Project '{name}' created successfully"
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>', methods=['GET'])
def get_pattern_project(project_id):
    """Get a single pattern config project by ID with all its filters."""
    try:
        ensure_pattern_tables_mysql()
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get project
                cursor.execute("""
                    SELECT 
                        p.*,
                        COUNT(f.id) AS filter_count,
                        SUM(CASE WHEN f.is_active = 1 THEN 1 ELSE 0 END) AS active_filter_count
                    FROM pattern_config_projects p
                    LEFT JOIN pattern_config_filters f ON f.project_id = p.id
                    WHERE p.id = %s
                    GROUP BY p.id
                """, [project_id])
                project = cursor.fetchone()
                
                if not project:
                    return jsonify({'success': False, 'error': 'Project not found'}), 404
                
                # Get all filters for this project
                cursor.execute("""
                    SELECT * FROM pattern_config_filters 
                    WHERE project_id = %s
                    ORDER BY created_at DESC
                """, [project_id])
                filters = cursor.fetchall()
        
        # Convert datetime objects for project
        if project.get('created_at'):
            project['created_at'] = project['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if project.get('updated_at'):
            project['updated_at'] = project['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert datetime objects for filters
        for f in filters:
            if f.get('created_at'):
                f['created_at'] = f['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if f.get('updated_at'):
                f['updated_at'] = f['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
            # Convert Decimal to float for JSON serialization
            if f.get('from_value') is not None:
                f['from_value'] = float(f['from_value'])
            if f.get('to_value') is not None:
                f['to_value'] = float(f['to_value'])
        
        return jsonify({
            'success': True,
            'project': project,
            'filters': filters
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>', methods=['DELETE'])
def delete_pattern_project(project_id):
    """Delete a pattern config project and all its filters (dual-write)."""
    try:
        ensure_pattern_tables_mysql()
        
        # Delete from MySQL
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # First delete associated filters
                cursor.execute("DELETE FROM pattern_config_filters WHERE project_id = %s", [project_id])
                filters_deleted = cursor.rowcount
                
                # Then delete the project
                cursor.execute("DELETE FROM pattern_config_projects WHERE id = %s", [project_id])
                project_deleted = cursor.rowcount
        
        # Also delete from DuckDB
        try:
            with get_duckdb("central") as duckdb_conn:
                duckdb_conn.execute("DELETE FROM pattern_config_filters WHERE project_id = ?", [project_id])
                duckdb_conn.execute("DELETE FROM pattern_config_projects WHERE id = ?", [project_id])
        except Exception as e:
            logger.error(f"DuckDB delete failed (MySQL succeeded): {e}")
        
        if project_deleted == 0:
            return jsonify({'success': False, 'error': 'Project not found'}), 404
        
        return jsonify({
            'success': True,
            'filters_deleted': filters_deleted,
            'message': 'Project deleted successfully'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>/filters', methods=['GET'])
def get_pattern_filters(project_id):
    """Get all filters for a pattern config project."""
    try:
        ensure_pattern_tables_mysql()
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM pattern_config_filters
                    WHERE project_id = %s
                    ORDER BY section, minute, field_name
                """, [project_id])
                filters = cursor.fetchall()
        
        # Convert datetime objects and Decimal types
        for f in filters:
            if f.get('created_at'):
                f['created_at'] = f['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if f.get('updated_at'):
                f['updated_at'] = f['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
            if f.get('from_value') is not None:
                f['from_value'] = float(f['from_value'])
            if f.get('to_value') is not None:
                f['to_value'] = float(f['to_value'])
        
        return jsonify({
            'success': True,
            'filters': filters,
            'count': len(filters)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/filters', methods=['POST'])
def create_pattern_filter():
    """Create a new pattern config filter (dual-write to MySQL + DuckDB)."""
    try:
        ensure_pattern_tables_mysql()
        
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Required fields
        project_id = data.get('project_id')
        name = data.get('name', '').strip()
        field_name = data.get('field_name', '').strip()
        
        if not name or not field_name:
            return jsonify({'success': False, 'error': 'name and field_name are required'}), 400
        
        # Optional fields
        section = data.get('section')
        minute = data.get('minute')
        field_column = data.get('field_column')
        from_value = data.get('from_value')
        to_value = data.get('to_value')
        include_null = data.get('include_null', 0)
        play_id = data.get('play_id')
        is_active = data.get('is_active', 1)
        
        # Write to MySQL first
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pattern_config_filters 
                    (project_id, name, section, minute, field_name, field_column, 
                     from_value, to_value, include_null, play_id, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [project_id, name, section, minute, field_name, field_column,
                      from_value, to_value, include_null, play_id, is_active])
                new_id = cursor.lastrowid
        
        # Also write to DuckDB
        try:
            with get_duckdb("central") as duckdb_conn:
                duckdb_conn.execute("""
                    INSERT INTO pattern_config_filters 
                    (id, project_id, name, section, minute, field_name, field_column,
                     from_value, to_value, include_null, play_id, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, [new_id, project_id, name, section, minute, field_name, field_column,
                      from_value, to_value, include_null, play_id, is_active])
        except Exception as e:
            logger.error(f"DuckDB write failed (MySQL succeeded): {e}")
        
        return jsonify({
            'success': True,
            'id': new_id,
            'message': 'Filter created successfully'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['PUT'])
def update_pattern_filter(filter_id):
    """Update a pattern config filter (dual-write)."""
    try:
        ensure_pattern_tables_mysql()
        
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Build update query dynamically
        allowed_fields = ['name', 'section', 'minute', 'field_name', 'field_column',
                         'from_value', 'to_value', 'include_null', 'play_id', 'is_active']
        
        updates = []
        values = []
        for field in allowed_fields:
            if field in data:
                updates.append(f"{field} = %s")
                values.append(data[field])
        
        if not updates:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400
        
        values.append(filter_id)
        
        # Update MySQL
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE pattern_config_filters SET {', '.join(updates)} WHERE id = %s",
                    values
                )
                updated = cursor.rowcount
        
        # Also update DuckDB
        try:
            duck_updates = [f"{field} = ?" for field in allowed_fields if field in data]
            duck_values = [data[field] for field in allowed_fields if field in data]
            duck_values.append(filter_id)
            
            with get_duckdb("central") as duckdb_conn:
                duckdb_conn.execute(
                    f"UPDATE pattern_config_filters SET {', '.join(duck_updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    duck_values
                )
        except Exception as e:
            logger.error(f"DuckDB update failed (MySQL succeeded): {e}")
        
        if updated == 0:
            return jsonify({'success': False, 'error': 'Filter not found'}), 404
        
        return jsonify({'success': True, 'message': 'Filter updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['DELETE'])
def delete_pattern_filter(filter_id):
    """Delete a pattern config filter (dual-write)."""
    try:
        ensure_pattern_tables_mysql()
        
        # Delete from MySQL
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM pattern_config_filters WHERE id = %s", [filter_id])
                deleted = cursor.rowcount
        
        # Also delete from DuckDB
        try:
            with get_duckdb("central") as duckdb_conn:
                duckdb_conn.execute("DELETE FROM pattern_config_filters WHERE id = ?", [filter_id])
        except Exception as e:
            logger.error(f"DuckDB delete failed (MySQL succeeded): {e}")
        
        if deleted == 0:
            return jsonify({'success': False, 'error': 'Filter not found'}), 404
        
        return jsonify({'success': True, 'message': 'Filter deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# TRAIL DATA ENDPOINTS (for Pattern Builder analysis)
# =============================================================================

# Trail sections configuration
TRAIL_SECTIONS = {
    'price_movements': 'Price Movements',
    'order_book_signals': 'Order Book Signals',
    'transactions': 'Transactions',
    'whale_activity': 'Whale Activity',
    'patterns': 'Patterns'
}

# Section prefix mapping for flattened table columns
SECTION_PREFIXES = {
    'price_movements': 'pm_',
    'order_book_signals': 'ob_',
    'transactions': 'tx_',
    'whale_activity': 'wh_',
    'patterns': 'pat_'
}

# Gain range definitions
GAIN_RANGES = [
    {'id': 0, 'label': '< 0%', 'min': None, 'max': 0},
    {'id': 1, 'label': '0 - 0.2%', 'min': 0, 'max': 0.2},
    {'id': 2, 'label': '0.2 - 0.5%', 'min': 0.2, 'max': 0.5},
    {'id': 3, 'label': '0.5 - 1%', 'min': 0.5, 'max': 1.0},
    {'id': 4, 'label': '1 - 2%', 'min': 1.0, 'max': 2.0},
    {'id': 5, 'label': '2%+', 'min': 2.0, 'max': None},
]


@app.route('/trail/sections', methods=['GET'])
def get_trail_sections():
    """Get available trail data sections and their fields."""
    try:
        section = request.args.get('section')
        
        # If no section specified, return all sections
        if not section:
            return jsonify({
                'success': True,
                'sections': TRAIL_SECTIONS,
                'prefixes': SECTION_PREFIXES
            })
        
        # Get fields for specific section
        if section not in TRAIL_SECTIONS:
            return jsonify({'success': False, 'error': f'Invalid section: {section}'}), 400
        
        prefix = SECTION_PREFIXES.get(section, '')
        fields = []
        field_types = {}
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get columns from trail_data_flattened table
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                      AND TABLE_NAME = 'trail_data_flattened'
                      AND COLUMN_NAME LIKE %s
                    ORDER BY ORDINAL_POSITION
                """, [prefix + '%'])
                columns = cursor.fetchall()
                
                for col in columns:
                    field_name = col['COLUMN_NAME'][len(prefix):]  # Remove prefix
                    fields.append(field_name)
                    
                    # Determine if boolean based on data type or name
                    data_type = col['DATA_TYPE'].upper()
                    is_bool = data_type in ('TINYINT', 'BOOLEAN', 'BOOL') or field_name.startswith('is_') or field_name.startswith('has_')
                    field_types[field_name] = 'BOOLEAN' if is_bool else 'NUMERIC'
        
        return jsonify({
            'success': True,
            'section': section,
            'section_label': TRAIL_SECTIONS[section],
            'prefix': prefix,
            'fields': fields,
            'field_types': field_types,
            'count': len(fields)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/trail/field_stats', methods=['POST'])
def get_trail_field_stats():
    """
    Get field statistics for a section/minute, broken down by gain ranges.
    
    Request JSON:
    {
        "project_id": 1,
        "section": "price_movements",
        "minute": 0,
        "status": "all",  // "all", "sold", "no_go"
        "hours": 6,
        "analyse_mode": "all"  // "all" or "passed" (apply filters)
    }
    """
    try:
        data = request.get_json() or {}
        
        project_id = data.get('project_id')
        section = data.get('section', 'price_movements')
        minute = data.get('minute', 0)
        status = data.get('status', 'all')
        hours = data.get('hours', 6)
        analyse_mode = data.get('analyse_mode', 'all')
        
        if section not in TRAIL_SECTIONS:
            return jsonify({'success': False, 'error': f'Invalid section: {section}'}), 400
        
        prefix = SECTION_PREFIXES.get(section, '')
        
        # Get fields for this section
        fields = []
        field_types = {}
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get section fields
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                      AND TABLE_NAME = 'trail_data_flattened'
                      AND COLUMN_NAME LIKE %s
                    ORDER BY ORDINAL_POSITION
                """, [prefix + '%'])
                columns = cursor.fetchall()
                
                for col in columns:
                    field_name = col['COLUMN_NAME'][len(prefix):]
                    fields.append(field_name)
                    data_type = col['DATA_TYPE'].upper()
                    is_bool = data_type in ('TINYINT', 'BOOLEAN', 'BOOL') or field_name.startswith('is_') or field_name.startswith('has_')
                    field_types[field_name] = 'BOOLEAN' if is_bool else 'NUMERIC'
                
                # Build base WHERE clause
                where_clauses = ["minute = %s"]
                params = [minute]
                
                where_clauses.append("followed_at >= NOW() - INTERVAL %s HOUR")
                params.append(hours)
                
                if status != 'all':
                    where_clauses.append("our_status = %s")
                    params.append(status)
                
                # Get active filters if analyse_mode is 'passed'
                active_filters = []
                if analyse_mode == 'passed' and project_id:
                    cursor.execute("""
                        SELECT * FROM pattern_config_filters 
                        WHERE project_id = %s AND is_active = 1
                    """, [project_id])
                    active_filters = cursor.fetchall()
                    
                    # Apply filter conditions
                    for idx, f in enumerate(active_filters):
                        db_col = f['field_column'] or f['field_name']
                        from_val = f['from_value']
                        to_val = f['to_value']
                        include_null = f.get('include_null', 0)
                        exclude_mode = f.get('exclude_mode', 0)
                        
                        if db_col:
                            if exclude_mode:
                                # Exclude mode: keep values outside range
                                excl_parts = []
                                if from_val is not None:
                                    excl_parts.append(f"`{db_col}` < %s")
                                    params.append(float(from_val))
                                if to_val is not None:
                                    excl_parts.append(f"`{db_col}` > %s")
                                    params.append(float(to_val))
                                if include_null:
                                    excl_parts.append(f"`{db_col}` IS NULL")
                                if excl_parts:
                                    where_clauses.append("(" + " OR ".join(excl_parts) + ")")
                            else:
                                # Normal mode: keep values in range
                                range_parts = []
                                if from_val is not None:
                                    range_parts.append(f"`{db_col}` >= %s")
                                    params.append(float(from_val))
                                if to_val is not None:
                                    range_parts.append(f"`{db_col}` <= %s")
                                    params.append(float(to_val))
                                if range_parts:
                                    range_sql = " AND ".join(range_parts)
                                    if include_null:
                                        where_clauses.append(f"(({range_sql}) OR `{db_col}` IS NULL)")
                                    else:
                                        where_clauses.append(range_sql)
                
                where_sql = " AND ".join(where_clauses)
                
                # Build aggregation query for each gain range
                field_stats = {}
                for field in fields:
                    field_stats[field] = {
                        'type': field_types.get(field, 'NUMERIC'),
                        'ranges': {}
                    }
                
                # Get stats per gain range
                for gain_range in GAIN_RANGES:
                    range_where = where_sql
                    range_params = params.copy()
                    
                    # Add gain range condition
                    if gain_range['min'] is not None and gain_range['max'] is not None:
                        range_where += " AND potential_gains >= %s AND potential_gains < %s"
                        range_params.extend([gain_range['min'], gain_range['max']])
                    elif gain_range['min'] is not None:
                        range_where += " AND potential_gains >= %s"
                        range_params.append(gain_range['min'])
                    elif gain_range['max'] is not None:
                        range_where += " AND potential_gains < %s"
                        range_params.append(gain_range['max'])
                    
                    # Build SELECT for all fields
                    select_parts = ["COUNT(*) as trade_count"]
                    for field in fields:
                        db_col = f"{prefix}{field}"
                        if field_types.get(field) == 'BOOLEAN':
                            # For boolean: show percentage TRUE
                            select_parts.append(f"AVG(CASE WHEN `{db_col}` = 1 THEN 100 ELSE 0 END) as `{field}`")
                        else:
                            select_parts.append(f"AVG(`{db_col}`) as `{field}`")
                    
                    query = f"SELECT {', '.join(select_parts)} FROM trail_data_flattened WHERE {range_where}"
                    
                    cursor.execute(query, range_params)
                    result = cursor.fetchone()
                    
                    if result:
                        trade_count = result['trade_count'] or 0
                        for field in fields:
                            val = result.get(field)
                            field_stats[field]['ranges'][gain_range['id']] = {
                                'avg': float(val) if val is not None else None,
                                'count': trade_count
                            }
                
                # Get total trade count
                cursor.execute(f"SELECT COUNT(*) as cnt FROM trail_data_flattened WHERE {where_sql}", params)
                total_result = cursor.fetchone()
                total_trades = total_result['cnt'] if total_result else 0
        
        return jsonify({
            'success': True,
            'section': section,
            'minute': minute,
            'total_trades': total_trades,
            'fields': fields,
            'field_types': field_types,
            'field_stats': field_stats,
            'gain_ranges': GAIN_RANGES,
            'active_filters': len(active_filters) if analyse_mode == 'passed' else 0
        })
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/trail/gain_distribution', methods=['POST'])
def get_trail_gain_distribution():
    """
    Get trade count distribution across gain ranges.
    
    Request JSON:
    {
        "project_id": 1,
        "minute": 0,
        "status": "all",
        "hours": 6,
        "apply_filters": true
    }
    """
    try:
        data = request.get_json() or {}
        
        project_id = data.get('project_id')
        minute = data.get('minute', 0)
        status = data.get('status', 'all')
        hours = data.get('hours', 6)
        apply_filters = data.get('apply_filters', False)
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Build base WHERE clause
                where_clauses = ["minute = %s"]
                params = [minute]
                
                where_clauses.append("followed_at >= NOW() - INTERVAL %s HOUR")
                params.append(hours)
                
                if status != 'all':
                    where_clauses.append("our_status = %s")
                    params.append(status)
                
                base_where_sql = " AND ".join(where_clauses)
                base_params = params.copy()
                
                # Get active filters if requested
                active_filters = []
                if apply_filters and project_id:
                    cursor.execute("""
                        SELECT * FROM pattern_config_filters 
                        WHERE project_id = %s AND is_active = 1
                    """, [project_id])
                    active_filters = cursor.fetchall()
                    
                    # Apply filter conditions
                    for idx, f in enumerate(active_filters):
                        db_col = f['field_column'] or f['field_name']
                        from_val = f['from_value']
                        to_val = f['to_value']
                        include_null = f.get('include_null', 0)
                        exclude_mode = f.get('exclude_mode', 0)
                        
                        if db_col:
                            if exclude_mode:
                                excl_parts = []
                                if from_val is not None:
                                    excl_parts.append(f"`{db_col}` < %s")
                                    params.append(float(from_val))
                                if to_val is not None:
                                    excl_parts.append(f"`{db_col}` > %s")
                                    params.append(float(to_val))
                                if include_null:
                                    excl_parts.append(f"`{db_col}` IS NULL")
                                if excl_parts:
                                    where_clauses.append("(" + " OR ".join(excl_parts) + ")")
                            else:
                                range_parts = []
                                if from_val is not None:
                                    range_parts.append(f"`{db_col}` >= %s")
                                    params.append(float(from_val))
                                if to_val is not None:
                                    range_parts.append(f"`{db_col}` <= %s")
                                    params.append(float(to_val))
                                if range_parts:
                                    range_sql = " AND ".join(range_parts)
                                    if include_null:
                                        where_clauses.append(f"(({range_sql}) OR `{db_col}` IS NULL)")
                                    else:
                                        where_clauses.append(range_sql)
                
                filtered_where_sql = " AND ".join(where_clauses)
                
                # Get distribution for each gain range (both base and filtered)
                distribution = []
                total_base = 0
                total_filtered = 0
                sum_gains_base = 0
                sum_gains_filtered = 0
                
                for gain_range in GAIN_RANGES:
                    # Base count (without filters)
                    base_range_where = base_where_sql
                    base_range_params = base_params.copy()
                    
                    if gain_range['min'] is not None and gain_range['max'] is not None:
                        base_range_where += " AND potential_gains >= %s AND potential_gains < %s"
                        base_range_params.extend([gain_range['min'], gain_range['max']])
                    elif gain_range['min'] is not None:
                        base_range_where += " AND potential_gains >= %s"
                        base_range_params.append(gain_range['min'])
                    elif gain_range['max'] is not None:
                        base_range_where += " AND potential_gains < %s"
                        base_range_params.append(gain_range['max'])
                    
                    cursor.execute(f"""
                        SELECT COUNT(*) as cnt, COALESCE(SUM(potential_gains), 0) as sum_gains
                        FROM trail_data_flattened WHERE {base_range_where}
                    """, base_range_params)
                    base_result = cursor.fetchone()
                    base_count = base_result['cnt'] or 0
                    base_sum = float(base_result['sum_gains'] or 0)
                    total_base += base_count
                    sum_gains_base += base_sum
                    
                    # Filtered count
                    filtered_count = base_count
                    filtered_sum = base_sum
                    
                    if active_filters:
                        filtered_range_where = filtered_where_sql
                        filtered_range_params = params.copy()
                        
                        if gain_range['min'] is not None and gain_range['max'] is not None:
                            filtered_range_where += " AND potential_gains >= %s AND potential_gains < %s"
                            filtered_range_params.extend([gain_range['min'], gain_range['max']])
                        elif gain_range['min'] is not None:
                            filtered_range_where += " AND potential_gains >= %s"
                            filtered_range_params.append(gain_range['min'])
                        elif gain_range['max'] is not None:
                            filtered_range_where += " AND potential_gains < %s"
                            filtered_range_params.append(gain_range['max'])
                        
                        cursor.execute(f"""
                            SELECT COUNT(*) as cnt, COALESCE(SUM(potential_gains), 0) as sum_gains
                            FROM trail_data_flattened WHERE {filtered_range_where}
                        """, filtered_range_params)
                        filtered_result = cursor.fetchone()
                        filtered_count = filtered_result['cnt'] or 0
                        filtered_sum = float(filtered_result['sum_gains'] or 0)
                    
                    total_filtered += filtered_count
                    sum_gains_filtered += filtered_sum
                    
                    distribution.append({
                        'id': gain_range['id'],
                        'label': gain_range['label'],
                        'min': gain_range['min'],
                        'max': gain_range['max'],
                        'base_count': base_count,
                        'filtered_count': filtered_count,
                        'removed': base_count - filtered_count
                    })
        
        return jsonify({
            'success': True,
            'distribution': distribution,
            'totals': {
                'base': total_base,
                'filtered': total_filtered,
                'removed': total_base - total_filtered
            },
            'gains': {
                'base_sum': sum_gains_base,
                'filtered_sum': sum_gains_filtered,
                'base_avg': sum_gains_base / total_base if total_base > 0 else 0,
                'filtered_avg': sum_gains_filtered / total_filtered if total_filtered > 0 else 0
            },
            'active_filters': len(active_filters)
        })
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


# =============================================================================
# FILTER ANALYSIS ENDPOINTS (Auto-filter suggestions dashboard)
# =============================================================================

# Section display names for filter analysis
FILTER_SECTION_NAMES = {
    'price_movements': 'Price Movements',
    'order_book_signals': 'Order Book',
    'transactions': 'Transactions',
    'whale_activity': 'Whale Activity',
    'patterns': 'Patterns',
    'second_prices': 'Second Prices',
}


def ensure_filter_tables_mysql():
    """Ensure filter analysis tables exist in MySQL."""
    with get_mysql() as conn:
        with conn.cursor() as cursor:
            # Auto filter settings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS auto_filter_settings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    setting_key VARCHAR(50) UNIQUE NOT NULL,
                    setting_value VARCHAR(100) NOT NULL,
                    description VARCHAR(255),
                    setting_type ENUM('number', 'decimal', 'boolean') DEFAULT 'decimal',
                    min_value DECIMAL(10,4) DEFAULT NULL,
                    max_value DECIMAL(10,4) DEFAULT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_key (setting_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Insert defaults if not exist
            cursor.execute("""
                INSERT IGNORE INTO auto_filter_settings (setting_key, setting_value, description, setting_type, min_value, max_value) VALUES
                ('good_trade_threshold', '0.3', 'Minimum % gain for a trade to be considered good.', 'decimal', 0.1, 5.0),
                ('analysis_hours', '24', 'Hours of historical trade data to analyze.', 'number', 1, 168),
                ('min_filters_in_combo', '1', 'Minimum filters required in a combination.', 'number', 1, 10),
                ('max_filters_in_combo', '6', 'Maximum filters to combine.', 'number', 2, 15),
                ('min_good_trades_kept_pct', '50', 'Min % of good trades a single filter must keep.', 'number', 10, 100),
                ('min_bad_trades_removed_pct', '10', 'Min % of bad trades a single filter must remove.', 'number', 5, 100),
                ('combo_min_good_kept_pct', '25', 'Min % of good trades a combination must keep.', 'number', 5, 100),
                ('combo_min_improvement', '1.0', 'Min % improvement to add another filter.', 'decimal', 0.1, 10.0)
            """)
        conn.commit()


@app.route('/filter-analysis/dashboard', methods=['GET'])
def get_filter_analysis_dashboard():
    """Get all data for the filter analysis dashboard."""
    try:
        ensure_filter_tables_mysql()
        
        result = {
            'success': True,
            'summary': {},
            'suggestions': [],
            'combinations': [],
            'minute_distribution': [],
            'scheduler_runs': [],
            'filter_consistency': [],
            'trend_chart_data': [],
            'scheduler_stats': {'runs_today': 0, 'last_run': None, 'avg_filters': 0},
            'settings': [],
            'rolling_avgs': {}
        }
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get auto filter settings
                try:
                    cursor.execute("SELECT * FROM auto_filter_settings ORDER BY id")
                    result['settings'] = cursor.fetchall()
                except:
                    pass
                
                # Summary statistics
                try:
                    cursor.execute("""
                        SELECT COUNT(*) as total_filters, 
                            ROUND(AVG(good_trades_kept_pct), 1) as avg_good_kept,
                            ROUND(AVG(bad_trades_removed_pct), 1) as avg_bad_removed, 
                            ROUND(MAX(bad_trades_removed_pct), 1) as best_bad_removed,
                            MAX(good_trades_before) as total_good_trades, 
                            MAX(bad_trades_before) as total_bad_trades,
                            MAX(created_at) as last_updated, 
                            MAX(analysis_hours) as analysis_hours
                        FROM filter_reference_suggestions
                    """)
                    summary = cursor.fetchone()
                    if summary:
                        if summary.get('last_updated'):
                            summary['last_updated'] = summary['last_updated'].strftime('%Y-%m-%d %H:%M:%S')
                        result['summary'] = summary
                except:
                    pass
                
                # Minute distribution
                try:
                    cursor.execute("""
                        SELECT minute_analyzed, COUNT(*) as filter_count, 
                            ROUND(AVG(bad_trades_removed_pct), 1) as avg_bad_removed, 
                            ROUND(AVG(good_trades_kept_pct), 1) as avg_good_kept
                        FROM filter_reference_suggestions 
                        GROUP BY minute_analyzed 
                        ORDER BY avg_bad_removed DESC
                    """)
                    result['minute_distribution'] = cursor.fetchall()
                except:
                    pass
                
                # All suggestions with field info
                try:
                    cursor.execute("""
                        SELECT frs.*, ffc.section, ffc.field_name, ffc.value_type, ffc.description
                        FROM filter_reference_suggestions frs 
                        LEFT JOIN filter_fields_catalog ffc ON frs.filter_field_id = ffc.id
                        ORDER BY bad_trades_removed_pct DESC
                    """)
                    suggestions = cursor.fetchall()
                    for s in suggestions:
                        if s.get('created_at'):
                            s['created_at'] = s['created_at'].strftime('%Y-%m-%d %H:%M:%S')
                        # Convert Decimal to float
                        for key in ['from_value', 'to_value', 'good_trades_kept_pct', 'bad_trades_removed_pct']:
                            if s.get(key) is not None:
                                s[key] = float(s[key])
                    result['suggestions'] = suggestions
                except:
                    pass
                
                # Filter combinations
                try:
                    cursor.execute("""
                        SELECT *, COALESCE(minute_analyzed, 0) as minute_analyzed 
                        FROM filter_combinations 
                        ORDER BY bad_trades_removed_pct DESC
                    """)
                    combinations = cursor.fetchall()
                    for combo in combinations:
                        for key in ['good_trades_kept_pct', 'bad_trades_removed_pct']:
                            if combo.get(key) is not None:
                                combo[key] = float(combo[key])
                        # Get filter details
                        filter_ids = json.loads(combo.get('filter_ids', '[]') or '[]')
                        if filter_ids:
                            placeholders = ','.join(['%s'] * len(filter_ids))
                            cursor.execute(f"""
                                SELECT id, column_name, from_value, to_value 
                                FROM filter_reference_suggestions 
                                WHERE id IN ({placeholders})
                            """, filter_ids)
                            details = cursor.fetchall()
                            combo['filter_details'] = {d['id']: d for d in details}
                        else:
                            combo['filter_details'] = {}
                    result['combinations'] = combinations
                except:
                    pass
                
                # Recent scheduler runs
                try:
                    cursor.execute("""
                        SELECT * FROM filter_scheduler_runs 
                        ORDER BY run_timestamp DESC 
                        LIMIT 20
                    """)
                    runs = cursor.fetchall()
                    for run in runs:
                        if run.get('run_timestamp'):
                            run['run_timestamp'] = run['run_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                        if run.get('completed_at'):
                            run['completed_at'] = run['completed_at'].strftime('%Y-%m-%d %H:%M:%S')
                        for key in ['best_bad_removed_pct', 'best_good_kept_pct']:
                            if run.get(key) is not None:
                                run[key] = float(run[key])
                    result['scheduler_runs'] = runs
                except:
                    pass
                
                # Filter consistency (last 24 hours)
                try:
                    cursor.execute("""
                        SELECT 
                            fph.filter_column,
                            COUNT(*) as total_runs,
                            SUM(fph.is_in_best_combination) as times_in_best_combo,
                            ROUND(SUM(fph.is_in_best_combination) / COUNT(*) * 100, 1) as consistency_pct,
                            ROUND(AVG(fph.bad_trades_removed_pct), 2) as avg_bad_removed,
                            ROUND(AVG(fph.good_trades_kept_pct), 2) as avg_good_kept,
                            ROUND(AVG(fph.effectiveness_score), 2) as avg_effectiveness,
                            MAX(fph.run_timestamp) as last_seen,
                            (SELECT minute_analyzed FROM filter_performance_history 
                             WHERE filter_column = fph.filter_column 
                             ORDER BY run_timestamp DESC LIMIT 1) as latest_minute,
                            (SELECT from_value FROM filter_performance_history 
                             WHERE filter_column = fph.filter_column 
                             ORDER BY run_timestamp DESC LIMIT 1) as latest_from,
                            (SELECT to_value FROM filter_performance_history 
                             WHERE filter_column = fph.filter_column 
                             ORDER BY run_timestamp DESC LIMIT 1) as latest_to
                        FROM filter_performance_history fph
                        WHERE fph.run_timestamp >= NOW() - INTERVAL 24 HOUR
                        GROUP BY fph.filter_column
                        HAVING total_runs >= 2
                        ORDER BY consistency_pct DESC, avg_effectiveness DESC
                        LIMIT 30
                    """)
                    consistency = cursor.fetchall()
                    for c in consistency:
                        if c.get('last_seen'):
                            c['last_seen'] = c['last_seen'].strftime('%Y-%m-%d %H:%M:%S')
                        for key in ['consistency_pct', 'avg_bad_removed', 'avg_good_kept', 'avg_effectiveness', 'latest_from', 'latest_to']:
                            if c.get(key) is not None:
                                c[key] = float(c[key])
                    result['filter_consistency'] = consistency
                except:
                    pass
                
                # Trend chart data
                try:
                    cursor.execute("""
                        SELECT 
                            DATE_FORMAT(run_timestamp, '%Y-%m-%d %H:%i') as time_bucket,
                            ROUND(AVG(bad_trades_removed_pct), 2) as avg_bad_removed,
                            ROUND(AVG(good_trades_kept_pct), 2) as avg_good_kept,
                            ROUND(AVG(effectiveness_score), 2) as avg_effectiveness,
                            COUNT(DISTINCT filter_column) as filter_count
                        FROM filter_performance_history
                        WHERE run_timestamp >= NOW() - INTERVAL 24 HOUR
                        GROUP BY DATE_FORMAT(run_timestamp, '%Y-%m-%d %H:%i')
                        ORDER BY time_bucket ASC
                    """)
                    trend_data = cursor.fetchall()
                    for t in trend_data:
                        for key in ['avg_bad_removed', 'avg_good_kept', 'avg_effectiveness']:
                            if t.get(key) is not None:
                                t[key] = float(t[key])
                    result['trend_chart_data'] = trend_data
                except:
                    pass
                
                # Scheduler stats
                try:
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as runs_today,
                            MAX(run_timestamp) as last_run,
                            ROUND(AVG(total_filters_analyzed)) as avg_filters
                        FROM filter_scheduler_runs
                        WHERE DATE(run_timestamp) = CURDATE()
                    """)
                    stats = cursor.fetchone()
                    if stats:
                        if stats.get('last_run'):
                            stats['last_run'] = stats['last_run'].strftime('%Y-%m-%d %H:%M:%S')
                        result['scheduler_stats'] = stats
                except:
                    pass
                
                # Rolling averages
                try:
                    cursor.execute("""
                        SELECT 
                            filter_column,
                            ROUND(AVG(bad_trades_removed_pct), 2) as rolling_avg
                        FROM filter_performance_history
                        WHERE run_timestamp >= NOW() - INTERVAL 6 HOUR
                        GROUP BY filter_column
                    """)
                    for r in cursor.fetchall():
                        result['rolling_avgs'][r['filter_column']] = float(r['rolling_avg']) if r['rolling_avg'] else 0
                except:
                    pass
        
        return jsonify(result)
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/filter-analysis/settings', methods=['GET'])
def get_filter_settings():
    """Get auto filter settings."""
    try:
        ensure_filter_tables_mysql()
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM auto_filter_settings ORDER BY id")
                settings = cursor.fetchall()
        
        return jsonify({'success': True, 'settings': settings})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/filter-analysis/settings', methods=['POST'])
def save_filter_settings():
    """Save auto filter settings."""
    try:
        ensure_filter_tables_mysql()
        data = request.get_json() or {}
        settings = data.get('settings', {})
        
        errors = []
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                for key, value in settings.items():
                    try:
                        cursor.execute("""
                            UPDATE auto_filter_settings 
                            SET setting_value = %s 
                            WHERE setting_key = %s
                        """, [str(value), key])
                    except Exception as e:
                        errors.append(f"{key}: {str(e)}")
            conn.commit()
            
            # Get updated settings
            with conn.cursor() as cursor:
                cursor.execute("SELECT setting_key, setting_value FROM auto_filter_settings")
                current = {r['setting_key']: r['setting_value'] for r in cursor.fetchall()}
        
        return jsonify({
            'success': True,
            'message': 'Settings saved',
            'errors': errors,
            'current_settings': current
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# GENERIC QUERY ENDPOINT
# =============================================================================

@app.route('/query', methods=['POST'])
def generic_query():
    """
    Generic query endpoint for flexible data access.
    
    Request JSON:
    {
        "table": "follow_the_goat_buyins",
        "columns": ["id", "wallet_address"],  // optional, default *
        "where": {"play_id": 1},  // optional
        "order_by": "followed_at DESC",  // optional
        "limit": 100,  // optional
        "source": "auto"  // "duckdb", "mysql", or "auto" (default)
    }
    """
    try:
        data = request.get_json()
        if not data or 'table' not in data:
            return jsonify({'error': 'table required'}), 400
        
        table = data['table']
        columns = data.get('columns')
        where = data.get('where')
        order_by = data.get('order_by')
        limit = data.get('limit', 100)
        source = data.get('source', 'auto')
        
        # Tables that only exist in TradingDataEngine (in-memory, not file-based)
        engine_only_tables = ['order_book_features']
        
        # Validate table
        valid_tables = HOT_TABLES + engine_only_tables + ['follow_the_goat_plays', 'follow_the_goat_buyins_archive']
        if table not in valid_tables:
            return jsonify({'error': f'Invalid table. Valid: {valid_tables}'}), 400
        
        # Build query
        cols_str = ", ".join(columns) if columns else "*"
        query_parts = [f"SELECT {cols_str} FROM {table}"]
        params = []
        
        # Engine-only tables must query from TradingDataEngine
        use_engine_only = table in engine_only_tables
        use_duckdb = source != 'mysql' and (table in HOT_TABLES or use_engine_only)
        
        if where:
            where_clauses = []
            for col, val in where.items():
                where_clauses.append(f"{col} = ?" if use_duckdb else f"{col} = %s")
                params.append(val)
            query_parts.append("WHERE " + " AND ".join(where_clauses))
        
        if order_by:
            query_parts.append(f"ORDER BY {order_by}")
        
        if limit:
            query_parts.append(f"LIMIT {limit}")
        
        query = " ".join(query_parts)
        
        if use_duckdb:
            # Try TradingDataEngine first (in-memory, zero locks)
            rows = None
            actual_source = 'duckdb'
            try:
                engine = get_trading_engine()
                if engine._running:
                    results = engine.read(query, params)
                    rows = results
                    actual_source = 'engine'
            except Exception as engine_err:
                logger.debug(f"Engine query failed, falling back to file DuckDB: {engine_err}")
            
            # Fall back to file-based DuckDB if engine not available
            # (but NOT for engine-only tables like order_book_features)
            if rows is None and not use_engine_only:
                with get_duckdb("central") as conn:
                    result = conn.execute(query, params).fetchall()
                    col_names = [desc[0] for desc in conn.description]
                    rows = [dict(zip(col_names, row)) for row in result]
            
            # For engine-only tables with no data, return empty results
            if rows is None:
                rows = []
        else:
            actual_source = 'mysql'
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
        
        return jsonify({
            'results': rows,
            'count': len(rows),
            'source': actual_source
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# TRAIL DATA FOR BUYIN
# =============================================================================

@app.route('/trail/buyin/<int:buyin_id>', methods=['GET'])
def get_trail_for_buyin(buyin_id: int):
    """
    Get 15-minute trail data for a specific buyin.
    
    Returns 15 rows from buyin_trail_minutes table (one per minute).
    
    Query params:
    - source: 'duckdb' or 'mysql' (default: duckdb)
    """
    source = request.args.get('source', 'duckdb')
    
    try:
        if source == 'mysql':
            with get_mysql() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT *
                        FROM buyin_trail_minutes
                        WHERE buyin_id = %s
                        ORDER BY minute ASC
                    """, [buyin_id])
                    columns = [col[0] for col in cursor.description]
                    rows = []
                    for row in cursor.fetchall():
                        rows.append(dict(zip(columns, row)))
        else:
            with get_duckdb('central') as conn:
                result = conn.execute("""
                    SELECT *
                    FROM buyin_trail_minutes
                    WHERE buyin_id = ?
                    ORDER BY minute ASC
                """, [buyin_id]).fetchall()
                
                if result:
                    columns = [desc[0] for desc in conn.execute("DESCRIBE buyin_trail_minutes").fetchall()]
                    # Get column names from the executed query
                    desc_result = conn.execute("""
                        SELECT * FROM buyin_trail_minutes WHERE buyin_id = ? LIMIT 1
                    """, [buyin_id])
                    columns = [col[0] for col in desc_result.description]
                    rows = [dict(zip(columns, row)) for row in result]
                else:
                    rows = []
        
        return jsonify({
            'success': True,
            'buyin_id': buyin_id,
            'trail_data': rows,
            'row_count': len(rows),
            'source': source
        })
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'trace': traceback.format_exc()
        }), 500


# =============================================================================
# RECENT TRADES (Live Feed)
# =============================================================================

@app.route('/recent_trades', methods=['GET'])
def get_recent_trades():
    """
    Get recent buy trades from sol_stablecoin_trades.
    Used for the live trade feed page.
    
    Query params:
    - minutes: Time window in minutes (default: 5)
    - limit: Max records (default: 100)
    - direction: 'buy', 'sell', or 'all' (default: 'buy')
    
    Data sources (in order of preference):
    1. TradingDataEngine (in-memory, fastest)
    2. File-based DuckDB (fallback)
    3. Webhook API directly (fallback when local is empty)
    """
    import requests as req
    
    try:
        minutes = int(request.args.get('minutes', 5))
        limit = int(request.args.get('limit', 100))
        direction = request.args.get('direction', 'buy')
        
        # Build direction filter
        direction_filter = ""
        if direction in ('buy', 'sell'):
            direction_filter = f"AND direction = '{direction}'"
        
        # Try TradingDataEngine first for zero-lock reads
        trades = None
        total_count = 0
        source = 'duckdb'
        
        try:
            engine = get_trading_engine()
            if engine._running:
                # Get total count first
                count_query = f"""
                    SELECT COUNT(*) as cnt
                    FROM sol_stablecoin_trades
                    WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                    {direction_filter}
                """
                count_result = engine.read(count_query, [])
                if count_result:
                    total_count = count_result[0].get('cnt', 0)
                
                # Get limited trades
                query = f"""
                    SELECT id, wallet_address, signature, trade_timestamp,
                           stablecoin_amount, sol_amount, price, direction, perp_direction
                    FROM sol_stablecoin_trades
                    WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                    {direction_filter}
                    ORDER BY trade_timestamp DESC
                    LIMIT {limit}
                """
                trades = engine.read(query, [])
                source = 'engine'
                
                # If engine has trades, return them
                if trades and len(trades) > 0:
                    for trade in trades:
                        if trade.get('trade_timestamp') and hasattr(trade['trade_timestamp'], 'isoformat'):
                            trade['trade_timestamp'] = trade['trade_timestamp'].isoformat()
                    
                    return jsonify({
                        'trades': trades,
                        'count': len(trades),
                        'total_count': total_count,
                        'source': source,
                        'timestamp': datetime.now().isoformat()
                    })
        except Exception as e:
            logger.debug(f"Engine query failed: {e}")
        
        # Fallback to file-based DuckDB
        if trades is None or len(trades) == 0:
            try:
                with get_duckdb("central") as conn:
                    # Get total count
                    count_result = conn.execute(f"""
                        SELECT COUNT(*) as cnt
                        FROM sol_stablecoin_trades
                        WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                        {direction_filter}
                    """).fetchone()
                    total_count = count_result[0] if count_result else 0
                    
                    if total_count > 0:
                        # Get limited trades
                        result = conn.execute(f"""
                            SELECT id, wallet_address, signature, trade_timestamp,
                                   stablecoin_amount, sol_amount, price, direction, perp_direction
                            FROM sol_stablecoin_trades
                            WHERE trade_timestamp >= NOW() - INTERVAL {minutes} MINUTE
                            {direction_filter}
                            ORDER BY trade_timestamp DESC
                            LIMIT {limit}
                        """).fetchall()
                        
                        columns = ['id', 'wallet_address', 'signature', 'trade_timestamp',
                                  'stablecoin_amount', 'sol_amount', 'price', 'direction', 'perp_direction']
                        trades = [dict(zip(columns, row)) for row in result]
                        source = 'file_duckdb'
                        
                        for trade in trades:
                            if trade.get('trade_timestamp') and hasattr(trade['trade_timestamp'], 'isoformat'):
                                trade['trade_timestamp'] = trade['trade_timestamp'].isoformat()
                        
                        return jsonify({
                            'trades': trades,
                            'count': len(trades),
                            'total_count': total_count,
                            'source': source,
                            'timestamp': datetime.now().isoformat()
                        })
            except Exception as e:
                logger.debug(f"File DuckDB query failed: {e}")
        
        # FINAL FALLBACK: Query webhook directly (same source as Transaction Flow)
        # This ensures Trade Feed works even when local sync is behind
        try:
            logger.info("Local DuckDB empty, falling back to webhook API directly")
            webhook_response = req.get(
                'http://quicknode.smz.dk/api/trades',
                params={'limit': limit * 2},  # Get more to filter by direction
                timeout=5
            )
            
            if webhook_response.status_code == 200:
                data = webhook_response.json()
                if data.get('success'):
                    all_trades = data.get('results', [])
                    
                    # Filter by direction and time window
                    now = datetime.now()
                    filtered_trades = []
                    for trade in all_trades:
                        # Apply direction filter
                        if direction != 'all':
                            trade_direction = (trade.get('direction') or '').lower()
                            if trade_direction != direction:
                                continue
                        
                        # Apply time filter
                        ts_str = trade.get('trade_timestamp', '')
                        if ts_str:
                            try:
                                # Parse ISO timestamp
                                ts_str = ts_str.replace('Z', '').replace('T', ' ')[:19]
                                ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                                if (now - ts).total_seconds() > minutes * 60:
                                    continue
                            except:
                                pass
                        
                        filtered_trades.append({
                            'id': trade.get('id'),
                            'wallet_address': trade.get('wallet_address'),
                            'signature': trade.get('signature'),
                            'trade_timestamp': trade.get('trade_timestamp'),
                            'stablecoin_amount': trade.get('stablecoin_amount'),
                            'sol_amount': trade.get('sol_amount'),
                            'price': trade.get('price'),
                            'direction': trade.get('direction'),
                            'perp_direction': trade.get('perp_direction')
                        })
                        
                        if len(filtered_trades) >= limit:
                            break
                    
                    return jsonify({
                        'trades': filtered_trades,
                        'count': len(filtered_trades),
                        'total_count': len(filtered_trades),
                        'source': 'webhook_direct',
                        'timestamp': datetime.now().isoformat()
                    })
        except Exception as e:
            logger.error(f"Webhook fallback failed: {e}")
        
        # If all sources fail, return empty
        return jsonify({
            'trades': [],
            'count': 0,
            'total_count': 0,
            'source': 'none',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/tracked_wallets', methods=['GET'])
def get_tracked_wallets():
    """
    Get all wallets being tracked by active plays.
    Returns wallet addresses extracted from cashe_wallets_settings JSON field.
    Also includes perp_mode for each play to match follow_the_goat.py trigger logic.
    """
    try:
        wallets_by_play = {}
        all_wallets = set()
        
        # Get active plays with cached wallet settings AND perp config
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, cashe_wallets_settings, tricker_on_perp
                    FROM follow_the_goat_plays
                    WHERE is_active = 1
                """)
                plays = cursor.fetchall()
        
        for play in plays:
            play_id = play['id']
            play_name = play['name']
            cache_settings = play.get('cashe_wallets_settings')
            perp_config_raw = play.get('tricker_on_perp')
            
            # Parse perp_mode (matches follow_the_goat.py logic)
            perp_mode = 'any'
            if perp_config_raw:
                perp_config = perp_config_raw
                if isinstance(perp_config, str):
                    try:
                        perp_config = json.loads(perp_config)
                    except json.JSONDecodeError:
                        perp_config = None
                
                if isinstance(perp_config, dict):
                    mode = perp_config.get('mode', 'any')
                    if isinstance(mode, str) and mode.lower() in ('long_only', 'short_only', 'any'):
                        perp_mode = mode.lower()
            
            wallets = []
            if cache_settings:
                # Parse JSON if string
                if isinstance(cache_settings, str):
                    try:
                        cache_settings = json.loads(cache_settings)
                    except json.JSONDecodeError:
                        continue
                
                # Extract wallets from settings
                if isinstance(cache_settings, dict):
                    wallets = cache_settings.get('wallets', [])
            
            if wallets:
                wallets_by_play[play_id] = {
                    'play_id': play_id,
                    'play_name': play_name,
                    'wallets': wallets,
                    'count': len(wallets),
                    'perp_mode': perp_mode  # Include perp filter setting
                }
                all_wallets.update(wallets)
        
        return jsonify({
            'plays': wallets_by_play,
            'all_wallets': list(all_wallets),
            'total_wallet_count': len(all_wallets),
            'play_count': len(wallets_by_play),
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    import argparse
    import os
    
    # Disable Flask's auto-loading of .env files (avoids encoding issues)
    os.environ['FLASK_SKIP_DOTENV'] = '1'
    
    parser = argparse.ArgumentParser(description='Central DuckDB API Server')
    parser.add_argument('--port', type=int, default=5050, help='Port to run on (default: 5050)')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--init', action='store_true', help='Initialize DuckDB tables on startup')
    args = parser.parse_args()
    
    if args.init:
        print("Initializing DuckDB tables...")
        init_duckdb_tables("central")
        print("Tables initialized.")
    
    print(f"Starting Central DuckDB API on http://{args.host}:{args.port}")
    print(f"Central Database: {settings.central_db_path}")
    print(f"MySQL: {settings.mysql.host}/{settings.mysql.database}")
    
    app.run(host=args.host, port=args.port, debug=args.debug)

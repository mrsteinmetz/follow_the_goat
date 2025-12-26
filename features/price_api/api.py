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

from core.database import (
    get_duckdb, get_mysql, get_trading_engine,
    dual_write_insert, dual_write_update, dual_write_delete,
    smart_query, init_duckdb_tables, cleanup_all_hot_tables
)
from core.config import settings
from features.price_api.schema import HOT_TABLES, TIMESTAMP_COLUMNS

app = Flask(__name__)
CORS(app)

# Database path for backward compatibility
DB_PATH = PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"


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
    """
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filter
        time_filter = ""
        if hours != 'all':
            hours_int = int(hours)
            time_filter = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get live trades stats (from DuckDB hot data via live table)
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as active_trades,
                        AVG(CASE 
                            WHEN our_entry_price > 0 AND current_price > 0 
                            THEN ((current_price - our_entry_price) / our_entry_price) * 100 
                            ELSE NULL 
                        END) as active_avg_profit
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s AND our_status = 'pending' {time_filter}
                """, [play_id])
                live_stats = cursor.fetchone()
                
                # Get completed trades stats from archive
                cursor.execute(f"""
                    SELECT 
                        SUM(our_profit_loss) as total_profit_loss,
                        COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades,
                        COUNT(CASE WHEN our_status = 'no_go' THEN 1 END) as total_no_gos
                    FROM follow_the_goat_buyins_archive
                    WHERE play_id = %s {time_filter}
                """, [play_id])
                archive_stats = cursor.fetchone()
        
        return jsonify({
            'success': True,
            'play_id': play_id,
            'total_profit_loss': float(archive_stats['total_profit_loss'] or 0),
            'winning_trades': archive_stats['winning_trades'] or 0,
            'losing_trades': archive_stats['losing_trades'] or 0,
            'total_no_gos': archive_stats['total_no_gos'] or 0,
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
    """
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filter
        time_filter = ""
        time_filter_archive = ""
        if hours != 'all':
            try:
                hours_int = int(hours)
                time_filter = f"AND followed_at >= NOW() - INTERVAL {hours_int} HOUR"
                time_filter_archive = f"AND our_exit_timestamp >= NOW() - INTERVAL {hours_int} HOUR"
            except ValueError:
                pass  # Keep empty filters if hours is invalid
        
        plays_data = {}
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                # Get all play IDs
                cursor.execute("SELECT id FROM follow_the_goat_plays ORDER BY id")
                play_ids = [row['id'] for row in cursor.fetchall()]
                
                # Get live trades stats for all plays
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
                    WHERE our_status = 'pending' {time_filter}
                    GROUP BY play_id
                """)
                live_stats = {row['play_id']: row for row in cursor.fetchall()}
                
                # Get completed trades stats from archive for all plays
                cursor.execute(f"""
                    SELECT 
                        play_id,
                        SUM(our_profit_loss) as total_profit_loss,
                        COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades,
                        COUNT(CASE WHEN our_status = 'no_go' THEN 1 END) as total_no_gos
                    FROM follow_the_goat_buyins_archive
                    WHERE 1=1 {time_filter_archive}
                    GROUP BY play_id
                """)
                archive_stats = {row['play_id']: row for row in cursor.fetchall()}
        
        # Combine stats for each play - use string keys for JavaScript compatibility
        for play_id in play_ids:
            live = live_stats.get(play_id, {})
            archive = archive_stats.get(play_id, {})
            
            # Use string keys for consistent JavaScript access
            plays_data[str(play_id)] = {
                'total_profit_loss': float(archive.get('total_profit_loss') or 0),
                'winning_trades': int(archive.get('winning_trades') or 0),
                'losing_trades': int(archive.get('losing_trades') or 0),
                'total_no_gos': int(archive.get('total_no_gos') or 0),
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
        
        if use_duckdb and hours_int:
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
    """Get a single buyin/trade by ID."""
    try:
        source = request.args.get('source', 'live')  # 'live' or 'archive'
        
        table = 'follow_the_goat_buyins' if source == 'live' else 'follow_the_goat_buyins_archive'
        
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table} WHERE id = %s", [buyin_id])
                buyin = cursor.fetchone()
        
        if not buyin:
            return jsonify({'success': False, 'error': 'Trade not found'}), 404
        
        # Convert datetime fields
        for key in ['block_timestamp', 'followed_at', 'our_exit_timestamp', 'created_at']:
            if buyin.get(key) and hasattr(buyin[key], 'strftime'):
                buyin[key] = buyin[key].strftime('%Y-%m-%d %H:%M:%S')
        
        return jsonify({'success': True, 'buyin': buyin, 'source': source})
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
        
        # Try TradingDataEngine first (in-memory, zero locks)
        try:
            engine = get_trading_engine()
            if engine._running:
                results = engine.read("""
                    SELECT ts, price 
                    FROM prices 
                    WHERE token = ? 
                      AND ts BETWEEN ? AND ?
                    ORDER BY ts ASC
                """, [token, start_datetime, end_datetime])
                
                prices = [
                    {'x': row['ts'].strftime('%Y-%m-%d %H:%M:%S'), 'y': float(row['price'])}
                    for row in results
                ]
        except:
            pass
        
        # Fallback to file-based DuckDB if engine not available or returned no data
        if not prices:
            import duckdb
            legacy_db = PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"
            
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
            except:
                pass
        
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
            legacy_db = PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb"
            
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
                        print(f"Error inserting row into {table}: {e}")
                
                results[table] = {'synced': len(rows)}
        
        return jsonify({
            'success': True,
            'results': results
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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

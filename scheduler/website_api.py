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
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider
from core.database import get_postgres, postgres_query, verify_tables_exist

class CustomJSONProvider(DefaultJSONProvider):
    """Custom JSON provider that converts datetime objects to ISO format strings."""
    def default(self, obj):
        if isinstance(obj, datetime):
            # Always return ISO format with 'Z' suffix for UTC
            return obj.isoformat() + ('Z' if obj.tzinfo is None else '')
        return super().default(obj)

app = Flask(__name__)
app.json = CustomJSONProvider(app)
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
    """Get price cycles (with PHP-compatible response format)."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        threshold = request.args.get('threshold')
        hours = request.args.get('hours', '24')
        
        # Build WHERE clause conditions
        where_conditions = []
        params = []
        
        # Filter by threshold if specified
        if threshold:
            where_conditions.append("threshold = %s")
            params.append(float(threshold))
        
        # Filter by time if specified and not 'all'
        if hours and hours != 'all':
            try:
                hours_int = int(hours)
                where_conditions.append("created_at >= NOW() - INTERVAL '%s hours'")
                params.append(hours_int)
            except ValueError:
                pass
        
        # Build the WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get cycles
                query = f"""
                    SELECT * FROM cycle_tracker
                    {where_clause}
                    ORDER BY id DESC LIMIT %s
                """
                params.append(limit)
                cursor.execute(query, params)
                cycles = cursor.fetchall()
                
                # Get total count
                count_query = f"SELECT COUNT(*) as count FROM cycle_tracker {where_clause}"
                cursor.execute(count_query, params[:-1])  # Exclude limit from count query
                count_result = cursor.fetchone()
                total_count = count_result['count'] if count_result else 0
        
        # Format response for PHP compatibility
        return jsonify({
            'cycles': cycles,
            'count': len(cycles),
            'total_count': total_count,
            'missing_cycles': 0,  # Could calculate sequence gaps if needed
            'source': 'postgresql'
        })
    except Exception as e:
        logger.error(f"Get cycles failed: {e}", exc_info=True)
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
                # IMPORTANT: Return timestamps in ISO format with 'Z' to indicate UTC
                # JavaScript Date() will interpret plain datetime strings as local time!
                prices = []
                for row in results:
                    if hasattr(row['timestamp'], 'strftime'):
                        # Return ISO format with 'Z' suffix to explicitly indicate UTC
                        timestamp_str = row['timestamp'].strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
                    else:
                        timestamp_str = str(row['timestamp'])
                    
                    prices.append({
                        'x': timestamp_str,
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
    """Get all plays (both active and inactive)."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        active_only = request.args.get('active_only', 'false').lower() == 'true'
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                if active_only:
                    cursor.execute("""
                        SELECT * FROM follow_the_goat_plays
                        WHERE is_active = 1
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                else:
                    # Return all plays (active and inactive)
                    cursor.execute("""
                        SELECT * FROM follow_the_goat_plays
                        ORDER BY id DESC LIMIT %s
                    """, [limit])
                results = cursor.fetchall()
        
        return jsonify({'plays': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get plays failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/plays/<int:play_id>', methods=['GET'])
def get_single_play(play_id):
    """Get a single play by ID."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays
                    WHERE id = %s
                """, [play_id])
                
                result = cursor.fetchone()
        
        if result:
            return jsonify({'play': result})
        else:
            return jsonify({'error': 'Play not found'}), 404
    except Exception as e:
        logger.error(f"Get single play failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/plays/<int:play_id>/for_edit', methods=['GET'])
def get_play_for_edit(play_id):
    """Get a single play with all fields for editing."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays
                    WHERE id = %s
                """, [play_id])
                
                result = cursor.fetchone()
        
        if result:
            return jsonify({
                'success': True,
                'id': result['id'],
                'name': result['name'],
                'description': result['description'],
                'find_wallets_sql': result.get('find_wallets_sql'),
                'sell_logic': result.get('sell_logic'),
                'max_buys_per_cycle': result.get('max_buys_per_cycle', 5),
                'short_play': result.get('short_play', 0),
                'trigger_on_perp': result.get('tricker_on_perp'),
                'timing_conditions': result.get('timing_conditions'),
                'bundle_trades': result.get('bundle_trades'),
                'cashe_wallets': result.get('cashe_wallets'),
                'project_ids': result.get('project_ids'),
                'is_active': result.get('is_active', 1),
                'sorting': result.get('sorting', 10),
                'pattern_validator_enable': result.get('pattern_validator_enable', 0),
                'pattern_update_by_ai': result.get('pattern_update_by_ai', 0)
            })
        else:
            return jsonify({'success': False, 'error': 'Play not found'}), 404
    except Exception as e:
        logger.error(f"Get play for edit failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>', methods=['PUT'])
def update_play(play_id):
    """Update a play."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Build dynamic UPDATE query based on provided fields
        update_fields = []
        params = []
        
        # JSONB columns that need JSON string conversion
        jsonb_fields = {
            'find_wallets_sql', 'sell_logic', 'trigger_on_perp', 
            'timing_conditions', 'bundle_trades', 'cashe_wallets', 'project_ids'
        }
        
        field_mappings = {
            'name': 'name',
            'description': 'description',
            'find_wallets_sql': 'find_wallets_sql',
            'sell_logic': 'sell_logic',
            'max_buys_per_cycle': 'max_buys_per_cycle',
            'short_play': 'short_play',
            'trigger_on_perp': 'tricker_on_perp',
            'timing_conditions': 'timing_conditions',
            'bundle_trades': 'bundle_trades',
            'cashe_wallets': 'cashe_wallets',
            'project_ids': 'project_ids',
            'is_active': 'is_active',
            'sorting': 'sorting',
            'pattern_validator_enable': 'pattern_validator_enable',
            'pattern_update_by_ai': 'pattern_update_by_ai'
        }
        
        for key, db_field in field_mappings.items():
            if key in data:
                value = data[key]
                # Convert find_wallets_sql to JSON format if it's a string
                if key == 'find_wallets_sql' and isinstance(value, str):
                    value = {'query': value}
                
                # Convert dict/list values to JSON strings for JSONB columns
                if key in jsonb_fields and (isinstance(value, dict) or isinstance(value, list)):
                    value = json.dumps(value)
                
                update_fields.append(f"{db_field} = %s")
                params.append(value)
        
        if not update_fields:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400
        
        params.append(play_id)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    UPDATE follow_the_goat_plays
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                cursor.execute(query, params)
                rows_affected = cursor.rowcount
            conn.commit()
        
        if rows_affected > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Play not found'}), 404
    except Exception as e:
        logger.error(f"Update play failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>', methods=['DELETE'])
def delete_play(play_id):
    """Delete a play."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM follow_the_goat_plays
                    WHERE id = %s
                """, [play_id])
                rows_affected = cursor.rowcount
            conn.commit()
        
        if rows_affected > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Play not found'}), 404
    except Exception as e:
        logger.error(f"Delete play failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>/performance', methods=['GET'])
def get_play_performance(play_id):
    """Get performance metrics for a single play."""
    try:
        hours = request.args.get('hours', 'all')
        
        # Build time filter
        time_filter = ""
        params = [play_id]
        
        if hours != 'all':
            try:
                hours_int = int(hours)
                time_filter = "AND followed_at >= NOW() - INTERVAL '%s hours'"
                params.append(hours_int)
            except ValueError:
                pass
        
        with get_postgres() as conn:
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
                    WHERE our_status = 'pending' AND play_id = %s {time_filter}
                """, params)
                live_result = cursor.fetchone()
                
                # Get no_go counts
                cursor.execute(f"""
                    SELECT COUNT(*) as no_go_count
                    FROM follow_the_goat_buyins
                    WHERE our_status = 'no_go' AND play_id = %s {time_filter}
                """, params)
                no_go_result = cursor.fetchone()
                
                # Get sold/completed trades stats
                sold_filter = time_filter.replace('followed_at', 'our_exit_timestamp') if time_filter else ""
                cursor.execute(f"""
                    SELECT 
                        SUM(our_profit_loss) as total_profit_loss,
                        COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades
                    FROM follow_the_goat_buyins
                    WHERE our_status IN ('sold', 'completed') AND play_id = %s {sold_filter}
                """, params)
                sold_result = cursor.fetchone()
        
        return jsonify({
            'success': True,
            'total_profit_loss': float(sold_result.get('total_profit_loss') or 0) if sold_result else 0,
            'winning_trades': int(sold_result.get('winning_trades') or 0) if sold_result else 0,
            'losing_trades': int(sold_result.get('losing_trades') or 0) if sold_result else 0,
            'total_no_gos': int(no_go_result.get('no_go_count') or 0) if no_go_result else 0,
            'active_trades': int(live_result.get('active_trades') or 0) if live_result else 0,
            'active_avg_profit': float(live_result.get('active_avg_profit')) if live_result and live_result.get('active_avg_profit') else None
        })
    except Exception as e:
        logger.error(f"Get play performance failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays', methods=['POST'])
def create_play():
    """Create a new play in PostgreSQL."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        # Required fields
        required = ['name', 'description', 'find_wallets_sql']
        for field in required:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Prepare JSONB values - convert dicts/lists to JSON strings
                find_wallets_sql = {'query': data['find_wallets_sql']}
                sell_logic = data.get('sell_logic', {'tolerance_rules': {'increases': [], 'decreases': []}})
                trigger_on_perp = data.get('trigger_on_perp', {'mode': 'any'})
                timing_conditions = data.get('timing_conditions', {'enabled': False})
                bundle_trades = data.get('bundle_trades', {'enabled': False})
                cashe_wallets = data.get('cashe_wallets', {'enabled': False})
                project_ids = data.get('project_ids', []) if data.get('project_ids') else None
                
                cursor.execute("""
                    INSERT INTO follow_the_goat_plays (
                        name, description, find_wallets_sql, sell_logic, 
                        max_buys_per_cycle, short_play, tricker_on_perp, 
                        timing_conditions, bundle_trades, cashe_wallets, 
                        project_ids, is_active, sorting, 
                        pattern_validator_enable, pattern_update_by_ai
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                """, [
                    data['name'],
                    data['description'],
                    json.dumps(find_wallets_sql),
                    json.dumps(sell_logic),
                    data.get('max_buys_per_cycle', 5),
                    data.get('short_play', 0),
                    json.dumps(trigger_on_perp),
                    json.dumps(timing_conditions),
                    json.dumps(bundle_trades),
                    json.dumps(cashe_wallets) if cashe_wallets else None,
                    json.dumps(project_ids) if project_ids else None,
                    1,  # is_active = 1
                    10,
                    0,
                    0
                ])
                result = cursor.fetchone()
                new_id = result['id'] if result else None
            conn.commit()
        
        return jsonify({'success': True, 'id': new_id})
    except Exception as e:
        logger.error(f"Create play failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/<int:play_id>/duplicate', methods=['POST'])
def duplicate_play(play_id):
    """Duplicate a play with a new name."""
    try:
        data = request.get_json() or {}
        new_name = data.get('new_name')
        
        if not new_name:
            return jsonify({'success': False, 'error': 'new_name is required'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get original play
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays WHERE id = %s
                """, [play_id])
                original = cursor.fetchone()
                
                if not original:
                    return jsonify({'success': False, 'error': 'Play not found'}), 404
                
                # Create duplicate - convert JSONB values to JSON strings
                jsonb_fields_to_convert = [
                    'find_wallets_sql', 'sell_logic', 'tricker_on_perp',
                    'timing_conditions', 'bundle_trades', 'cashe_wallets',
                    'project_ids', 'pattern_validator', 'cashe_wallets_settings'
                ]
                
                def convert_jsonb_value(value):
                    """Convert dict/list to JSON string, or return None if value is None."""
                    if value is None:
                        return None
                    if isinstance(value, (dict, list)):
                        return json.dumps(value)
                    return value
                
                cursor.execute("""
                    INSERT INTO follow_the_goat_plays (
                        name, description, find_wallets_sql, sell_logic,
                        max_buys_per_cycle, short_play, tricker_on_perp,
                        timing_conditions, bundle_trades, cashe_wallets,
                        project_ids, is_active, sorting,
                        pattern_validator_enable, pattern_update_by_ai,
                        pattern_validator, cashe_wallets_settings
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                """, [
                    new_name,
                    original.get('description'),
                    convert_jsonb_value(original.get('find_wallets_sql')),
                    convert_jsonb_value(original.get('sell_logic')),
                    original.get('max_buys_per_cycle', 5),
                    original.get('short_play', 0),
                    convert_jsonb_value(original.get('tricker_on_perp')),
                    convert_jsonb_value(original.get('timing_conditions')),
                    convert_jsonb_value(original.get('bundle_trades')),
                    convert_jsonb_value(original.get('cashe_wallets')),
                    convert_jsonb_value(original.get('project_ids')),
                    1,  # is_active = 1
                    original.get('sorting', 10),
                    original.get('pattern_validator_enable', 0),
                    original.get('pattern_update_by_ai', 0),
                    convert_jsonb_value(original.get('pattern_validator')),
                    convert_jsonb_value(original.get('cashe_wallets_settings'))
                ])
                result = cursor.fetchone()
                new_id = result['id'] if result else None
            conn.commit()
        
        return jsonify({'success': True, 'new_id': new_id})
    except Exception as e:
        logger.error(f"Duplicate play failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/plays/performance', methods=['GET'])
def get_all_plays_performance():
    """Get performance metrics for all plays (both active and inactive)."""
    try:
        hours = request.args.get('hours', 'all')
        active_only = request.args.get('active_only', 'false').lower() == 'true'
        
        # Build time filters
        time_filter = ""
        params = []
        
        if hours != 'all':
            try:
                hours_int = int(hours)
                time_filter = "AND followed_at >= NOW() - INTERVAL '%s hours'"
                params.append(hours_int)
            except ValueError:
                pass
        
        plays_data = {}
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get all play IDs (or just active ones if requested)
                if active_only:
                    cursor.execute("SELECT id FROM follow_the_goat_plays WHERE is_active = 1")
                else:
                    cursor.execute("SELECT id FROM follow_the_goat_plays")
                play_ids = [row['id'] for row in cursor.fetchall()]
                
                for play_id in play_ids:
                    # Get active (pending) trades stats
                    query_params = [play_id] + params
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as active_trades,
                            AVG(CASE 
                                WHEN our_entry_price > 0 AND current_price > 0 
                                THEN ((current_price - our_entry_price) / our_entry_price) * 100 
                                ELSE NULL 
                            END) as active_avg_profit
                        FROM follow_the_goat_buyins
                        WHERE our_status = 'pending' AND play_id = %s {time_filter}
                    """, query_params)
                    live_result = cursor.fetchone()
                    
                    # Get no_go counts
                    cursor.execute(f"""
                        SELECT COUNT(*) as no_go_count
                        FROM follow_the_goat_buyins
                        WHERE our_status = 'no_go' AND play_id = %s {time_filter}
                    """, query_params)
                    no_go_result = cursor.fetchone()
                    
                    # Get sold/completed trades stats
                    sold_filter = time_filter.replace('followed_at', 'our_exit_timestamp') if time_filter else ""
                    cursor.execute(f"""
                        SELECT 
                            SUM(our_profit_loss) as total_profit_loss,
                            COUNT(CASE WHEN our_profit_loss > 0 THEN 1 END) as winning_trades,
                            COUNT(CASE WHEN our_profit_loss < 0 THEN 1 END) as losing_trades
                        FROM follow_the_goat_buyins
                        WHERE our_status IN ('sold', 'completed') AND play_id = %s {sold_filter}
                    """, query_params)
                    sold_result = cursor.fetchone()
                    
                    # Combine stats - use string keys for JavaScript compatibility
                    plays_data[str(play_id)] = {
                        'total_profit_loss': float(sold_result.get('total_profit_loss') or 0) if sold_result else 0,
                        'winning_trades': int(sold_result.get('winning_trades') or 0) if sold_result else 0,
                        'losing_trades': int(sold_result.get('losing_trades') or 0) if sold_result else 0,
                        'total_no_gos': int(no_go_result.get('no_go_count') or 0) if no_go_result else 0,
                        'active_trades': int(live_result.get('active_trades') or 0) if live_result else 0,
                        'active_avg_profit': float(live_result.get('active_avg_profit')) if live_result and live_result.get('active_avg_profit') else None
                    }
        
        return jsonify({
            'success': True,
            'plays': plays_data
        })
    except Exception as e:
        logger.error(f"Get plays performance failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/buyins', methods=['GET'])
def get_buyins():
    """Get buyins."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        status_filter = request.args.get('status')
        play_id = request.args.get('play_id')
        hours = request.args.get('hours')
        
        # Build WHERE clause conditions
        where_conditions = []
        params = []
        
        if status_filter:
            where_conditions.append("our_status = %s")
            params.append(status_filter)
        
        if play_id:
            where_conditions.append("play_id = %s")
            params.append(int(play_id))
        
        if hours and hours != 'all':
            try:
                hours_int = int(hours)
                where_conditions.append("followed_at >= NOW() - INTERVAL '%s hours'")
                params.append(hours_int)
            except ValueError:
                pass
        
        # Build the WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT * FROM follow_the_goat_buyins
                    {where_clause}
                    ORDER BY id DESC LIMIT %s
                """
                params.append(limit)
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                # Get total count
                count_query = f"SELECT COUNT(*) as count FROM follow_the_goat_buyins {where_clause}"
                cursor.execute(count_query, params[:-1])  # Exclude limit from count query
                count_result = cursor.fetchone()
                total_count = count_result['count'] if count_result else 0
        
        return jsonify({
            'buyins': results,
            'results': results,  # For backward compatibility
            'count': len(results),
            'total': total_count
        })
    except Exception as e:
        logger.error(f"Get buyins failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/buyins/<int:buyin_id>', methods=['GET'])
def get_single_buyin(buyin_id):
    """Get a single buyin by ID."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_buyins
                    WHERE id = %s
                """, [buyin_id])
                
                result = cursor.fetchone()
        
        if result:
            return jsonify({'buyin': result})
        else:
            return jsonify({'error': 'Buyin not found'}), 404
    except Exception as e:
        logger.error(f"Get single buyin failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/price_checks', methods=['GET'])
def get_price_checks():
    """Get price checks for a buyin."""
    try:
        buyin_id = request.args.get('buyin_id', type=int)
        hours = request.args.get('hours', default='24')
        limit = min(int(request.args.get('limit', 100)), 10000)
        
        if not buyin_id:
            return jsonify({'error': 'buyin_id parameter required'}), 400
        
        # Build WHERE clause for time filtering
        where_conditions = ["buyin_id = %s"]
        params = [buyin_id]
        
        if hours != 'all':
            try:
                hours_int = int(hours)
                where_conditions.append(f"checked_at >= NOW() - INTERVAL '{hours_int} hours'")
            except ValueError:
                pass
        
        where_clause = " AND ".join(where_conditions)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        id,
                        buyin_id,
                        checked_at,
                        current_price,
                        entry_price,
                        highest_price,
                        reference_price,
                        gain_from_entry,
                        drop_from_high,
                        drop_from_entry,
                        drop_from_reference,
                        tolerance,
                        basis,
                        bucket,
                        applied_rule,
                        should_sell,
                        is_backfill
                    FROM follow_the_goat_buyins_price_checks
                    WHERE {where_clause}
                    ORDER BY checked_at ASC
                    LIMIT %s
                """, params + [limit])
                
                results = cursor.fetchall()
        
        return jsonify({
            'success': True,
            'price_checks': [dict(row) for row in results],
            'count': len(results),
            'source': 'postgres'
        })
    except Exception as e:
        logger.error(f"Get price checks failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/buyins/cleanup_no_gos', methods=['DELETE'])
def cleanup_no_gos():
    """Delete all no_go trades older than 24 hours from PostgreSQL."""
    try:
        deleted_count = 0
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Count before delete
                cursor.execute("""
                    SELECT COUNT(*) as count FROM follow_the_goat_buyins 
                    WHERE our_status = 'no_go' 
                    AND followed_at < NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                deleted_count = result['count'] if result else 0
                
                if deleted_count > 0:
                    # Delete from PostgreSQL
                    cursor.execute("""
                        DELETE FROM follow_the_goat_buyins 
                        WHERE our_status = 'no_go' 
                        AND followed_at < NOW() - INTERVAL '24 hours'
                    """)
            conn.commit()
        
        return jsonify({
            'success': True,
            'deleted': deleted_count,
            'message': f'Deleted {deleted_count} no_go trades older than 24 hours'
        })
    except Exception as e:
        logger.error(f"Cleanup no-gos failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/profiles', methods=['GET'])
def get_profiles():
    """Get wallet profiles aggregated by wallet address."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        threshold = request.args.get('threshold')
        hours = request.args.get('hours', '24')
        order_by = request.args.get('order_by', 'recent')
        
        # Build WHERE clause conditions
        where_conditions = []
        params = []
        
        # Filter by threshold if specified
        if threshold:
            where_conditions.append("threshold = %s")
            params.append(float(threshold))
        
        # Filter by time if specified and not 'all'
        if hours and hours != 'all':
            try:
                hours_int = int(hours)
                where_conditions.append("trade_timestamp >= NOW() - INTERVAL '%s hours'")
                params.append(hours_int)
            except ValueError:
                pass
        
        # Build the WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Determine ORDER BY clause
        order_clause = "ORDER BY latest_trade DESC"  # Default: most recent
        if order_by == 'avg_gain':
            order_clause = "ORDER BY avg_potential_gain DESC"
        elif order_by == 'trade_count':
            order_clause = "ORDER BY trade_count DESC"
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Aggregate profiles by wallet address
                query = f"""
                    SELECT 
                        wallet_address,
                        COUNT(*) as trade_count,
                        AVG(
                            CASE 
                                WHEN highest_price_reached > 0 AND trade_entry_price > 0 
                                THEN ((highest_price_reached - trade_entry_price) / trade_entry_price * 100)
                                ELSE 0 
                            END
                        ) as avg_potential_gain,
                        SUM(stablecoin_amount) as total_invested,
                        SUM(CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price * 100) < COALESCE(%s, threshold) THEN 1 ELSE 0 END) as trades_below_threshold,
                        SUM(CASE WHEN ((highest_price_reached - trade_entry_price) / trade_entry_price * 100) >= COALESCE(%s, threshold) THEN 1 ELSE 0 END) as trades_at_above_threshold,
                        MAX(trade_timestamp) as latest_trade,
                        AVG(threshold) as threshold
                    FROM wallet_profiles
                    {where_clause}
                    GROUP BY wallet_address
                    {order_clause}
                    LIMIT %s
                """
                
                # Add threshold params for the COALESCE comparisons
                threshold_val = float(threshold) if threshold else 0.3
                query_params = [threshold_val, threshold_val] + params + [limit]
                
                cursor.execute(query, query_params)
                results = cursor.fetchall()
        
        return jsonify({
            'profiles': results,
            'count': len(results),
            'source': 'postgres',
            'aggregated': True
        })
    except Exception as e:
        logger.error(f"Get profiles failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/profiles/stats', methods=['GET'])
def get_profile_stats():
    """Get wallet profile statistics."""
    try:
        threshold = request.args.get('threshold')
        hours = request.args.get('hours', '24')
        
        # Build WHERE clause conditions
        where_conditions = []
        params = []
        
        # Filter by threshold if specified
        if threshold:
            where_conditions.append("threshold = %s")
            params.append(float(threshold))
        
        # Filter by time if specified and not 'all'
        if hours and hours != 'all':
            try:
                hours_int = int(hours)
                where_conditions.append("trade_timestamp >= NOW() - INTERVAL '%s hours'")
                params.append(hours_int)
            except ValueError:
                pass
        
        # Build the WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(DISTINCT wallet_address) as unique_wallets,
                        COUNT(DISTINCT price_cycle) as unique_cycles,
                        SUM(stablecoin_amount) as total_invested,
                        AVG(trade_entry_price) as avg_entry_price
                    FROM wallet_profiles
                    {where_clause}
                """
                cursor.execute(query, params)
                result = cursor.fetchone()
        
        return jsonify({
            'stats': result if result else {
                'total_profiles': 0,
                'unique_wallets': 0,
                'unique_cycles': 0,
                'total_invested': 0,
                'avg_entry_price': 0
            }
        })
    except Exception as e:
        logger.error(f"Get profile stats failed: {e}", exc_info=True)
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
# PATTERN PROJECT ENDPOINTS
# =============================================================================

@app.route('/patterns/projects', methods=['GET'])
def get_pattern_projects():
    """Get all pattern config projects with filter counts."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        p.id, p.name, p.description, p.created_at, p.updated_at,
                        COUNT(f.id) as filter_count,
                        COUNT(CASE WHEN f.is_active = 1 THEN 1 END) as active_filter_count
                    FROM pattern_config_projects p
                    LEFT JOIN pattern_config_filters f ON f.project_id = p.id
                    GROUP BY p.id, p.name, p.description, p.created_at, p.updated_at
                    ORDER BY p.updated_at DESC
                """)
                results = cursor.fetchall()
        
        return jsonify({'projects': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get pattern projects failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/patterns/projects', methods=['POST'])
def create_pattern_project():
    """Create a new pattern config project."""
    try:
        data = request.get_json() or {}
        name = data.get('name', '').strip()
        description = data.get('description', '').strip() or None
        
        if not name:
            return jsonify({'success': False, 'error': 'Project name is required'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pattern_config_projects (name, description, created_at, updated_at)
                    VALUES (%s, %s, NOW(), NOW())
                    RETURNING id
                """, [name, description])
                result = cursor.fetchone()
                new_id = result['id'] if result else None
            conn.commit()
        
        return jsonify({'success': True, 'id': new_id})
    except Exception as e:
        logger.error(f"Create pattern project failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>', methods=['GET'])
def get_pattern_project(project_id):
    """Get a single pattern config project by ID with its filters."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get project
                cursor.execute("""
                    SELECT id, name, description, created_at, updated_at
                    FROM pattern_config_projects
                    WHERE id = %s
                """, [project_id])
                project = cursor.fetchone()
                
                if not project:
                    return jsonify({'error': 'Project not found'}), 404
                
                # Get filters for this project
                cursor.execute("""
                    SELECT * FROM pattern_config_filters
                    WHERE project_id = %s
                    ORDER BY id DESC
                """, [project_id])
                filters = cursor.fetchall()
        
        return jsonify({
            'project': project,
            'filters': filters
        })
    except Exception as e:
        logger.error(f"Get pattern project failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>', methods=['DELETE'])
def delete_pattern_project(project_id):
    """Delete a pattern config project and all its filters."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Delete filters first
                cursor.execute("""
                    DELETE FROM pattern_config_filters
                    WHERE project_id = %s
                """, [project_id])
                
                # Delete project
                cursor.execute("""
                    DELETE FROM pattern_config_projects
                    WHERE id = %s
                """, [project_id])
                rows_deleted = cursor.rowcount
            conn.commit()
        
        if rows_deleted > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Project not found'}), 404
    except Exception as e:
        logger.error(f"Delete pattern project failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/projects/<int:project_id>/filters', methods=['GET'])
def get_pattern_project_filters(project_id):
    """Get all filters for a pattern config project."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM pattern_config_filters
                    WHERE project_id = %s
                    ORDER BY id DESC
                """, [project_id])
                results = cursor.fetchall()
        
        return jsonify({'filters': results, 'count': len(results)})
    except Exception as e:
        logger.error(f"Get pattern filters failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# =============================================================================
# PATTERN FILTER ENDPOINTS
# =============================================================================

@app.route('/patterns/filters', methods=['POST'])
def create_pattern_filter():
    """Create a new pattern config filter."""
    try:
        data = request.get_json() or {}
        
        project_id = data.get('project_id')
        name = data.get('name', '').strip()
        section = data.get('section')
        minute = data.get('minute')
        field_name = data.get('field_name')
        field_column = data.get('field_column')
        from_value = data.get('from_value')
        to_value = data.get('to_value')
        include_null = data.get('include_null', 0)
        exclude_mode = data.get('exclude_mode', 0)
        is_active = data.get('is_active', 1)
        
        if not project_id:
            return jsonify({'success': False, 'error': 'project_id is required'}), 400
        if not field_name:
            return jsonify({'success': False, 'error': 'field_name is required'}), 400
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO pattern_config_filters 
                    (project_id, name, section, minute, field_name, field_column, 
                     from_value, to_value, include_null, exclude_mode, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, [project_id, name or field_name, section, minute, field_name, 
                      field_column, from_value, to_value, include_null, exclude_mode, is_active])
                result = cursor.fetchone()
                new_id = result['id'] if result else None
                
                # Update project's updated_at timestamp
                cursor.execute("""
                    UPDATE pattern_config_projects 
                    SET updated_at = NOW()
                    WHERE id = %s
                """, [project_id])
            conn.commit()
        
        return jsonify({'success': True, 'id': new_id})
    except Exception as e:
        logger.error(f"Create pattern filter failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['PUT'])
def update_pattern_filter(filter_id):
    """Update a pattern config filter."""
    try:
        data = request.get_json() or {}
        
        # Build dynamic UPDATE query
        update_fields = []
        params = []
        
        allowed_fields = ['name', 'section', 'minute', 'field_name', 'field_column',
                          'from_value', 'to_value', 'include_null', 'exclude_mode', 'is_active']
        
        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                params.append(data[field])
        
        if not update_fields:
            return jsonify({'success': False, 'error': 'No fields to update'}), 400
        
        params.append(filter_id)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                query = f"""
                    UPDATE pattern_config_filters
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """
                cursor.execute(query, params)
                rows_affected = cursor.rowcount
                
                if rows_affected > 0:
                    # Update parent project's updated_at
                    cursor.execute("""
                        UPDATE pattern_config_projects 
                        SET updated_at = NOW()
                        WHERE id = (SELECT project_id FROM pattern_config_filters WHERE id = %s)
                    """, [filter_id])
            conn.commit()
        
        if rows_affected > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Filter not found'}), 404
    except Exception as e:
        logger.error(f"Update pattern filter failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/patterns/filters/<int:filter_id>', methods=['DELETE'])
def delete_pattern_filter(filter_id):
    """Delete a pattern config filter."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get project_id before deleting
                cursor.execute("""
                    SELECT project_id FROM pattern_config_filters WHERE id = %s
                """, [filter_id])
                filter_row = cursor.fetchone()
                project_id = filter_row['project_id'] if filter_row else None
                
                # Delete filter
                cursor.execute("""
                    DELETE FROM pattern_config_filters WHERE id = %s
                """, [filter_id])
                rows_deleted = cursor.rowcount
                
                # Update project's updated_at
                if project_id:
                    cursor.execute("""
                        UPDATE pattern_config_projects SET updated_at = NOW() WHERE id = %s
                    """, [project_id])
            conn.commit()
        
        if rows_deleted > 0:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Filter not found'}), 404
    except Exception as e:
        logger.error(f"Delete pattern filter failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


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
# ORDER BOOK ENDPOINTS
# =============================================================================

@app.route('/order_book_features', methods=['GET'])
def get_order_book_features():
    """Get order book features with PHP-compatible column names."""
    try:
        limit = min(int(request.args.get('limit', 100)), 5000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Calculate best_bid and best_ask from JSON arrays
                cursor.execute("""
                    SELECT 
                        id,
                        'SOLUSDT' AS symbol,
                        timestamp AS ts,
                        CAST((bids_json::json->0->>0) AS DOUBLE PRECISION) AS best_bid,
                        CAST((asks_json::json->0->>0) AS DOUBLE PRECISION) AS best_ask,
                        mid_price,
                        spread_bps AS relative_spread_bps,
                        bid_liquidity AS bid_depth_10,
                        ask_liquidity AS ask_depth_10,
                        total_depth_10,
                        volume_imbalance,
                        bid_slope,
                        ask_slope,
                        bid_depth_bps_5,
                        ask_depth_bps_5,
                        bid_depth_bps_10,
                        ask_depth_bps_10,
                        bid_depth_bps_25,
                        ask_depth_bps_25,
                        net_liquidity_change_1s,
                        microprice,
                        microprice_dev_bps,
                        source
                    FROM order_book_features
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({
            'results': results,
            'count': len(results),
            'source': 'postgres'
        })
    except Exception as e:
        logger.error(f"Get order book features failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# =============================================================================
# WHALE ACTIVITY & TRANSACTIONS ENDPOINTS
# =============================================================================

@app.route('/whale_movements', methods=['GET'])
def get_whale_movements():
    """Get whale-sized trades (transactions > $10,000) from PostgreSQL."""
    try:
        limit = min(int(request.args.get('limit', 100)), 5000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Get large trades with whale classification
                cursor.execute("""
                    SELECT 
                        signature,
                        wallet_address,
                        trade_timestamp AS timestamp,
                        direction,
                        stablecoin_amount,
                        price AS sol_price_at_trade,
                        sol_amount,
                        CASE 
                            WHEN stablecoin_amount >= 100000 THEN 'MEGA_WHALE'
                            WHEN stablecoin_amount >= 50000 THEN 'LARGE_WHALE'
                            WHEN stablecoin_amount >= 25000 THEN 'WHALE'
                            ELSE 'MODERATE_WHALE'
                        END AS whale_type,
                        stablecoin_amount AS abs_change,
                        0 AS fee_paid,
                        sol_amount AS current_balance,
                        sol_amount AS previous_balance,
                        0 AS sol_change
                    FROM sol_stablecoin_trades
                    WHERE stablecoin_amount > 10000
                    ORDER BY trade_timestamp DESC
                    LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'source': 'postgres'
        })
    except Exception as e:
        logger.error(f"Get whale movements failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/trades', methods=['GET'])
def get_trades():
    """Get all stablecoin trades from PostgreSQL."""
    try:
        limit = min(int(request.args.get('limit', 100)), 5000)
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        signature,
                        wallet_address,
                        trade_timestamp,
                        direction,
                        stablecoin_amount,
                        price AS sol_price_at_trade,
                        sol_amount
                    FROM sol_stablecoin_trades
                    ORDER BY trade_timestamp DESC
                    LIMIT %s
                """, [limit])
                results = cursor.fetchall()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'source': 'postgres'
        })
    except Exception as e:
        logger.error(f"Get trades failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# TRAIL DATA ENDPOINTS
# =============================================================================

@app.route('/trail/buyin/<int:buyin_id>', methods=['GET'])
def get_trail_for_buyin(buyin_id):
    """Get 15-minute trail data for a specific buyin."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM buyin_trail_minutes
                    WHERE buyin_id = %s
                    ORDER BY minute ASC
                """, [buyin_id])
                
                results = cursor.fetchall()
        
        return jsonify({
            'trail_data': results,
            'count': len(results),
            'source': 'postgres'
        })
    except Exception as e:
        logger.error(f"Get trail for buyin failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# Trail section field definitions
TRAIL_SECTIONS = {
    'price_movements': {
        'prefix': 'pm_',
        'fields': [
            'price_change_1m', 'momentum_volatility_ratio', 'momentum_acceleration_1m',
            'price_change_5m', 'price_change_10m', 'volatility_pct', 'body_range_ratio',
            'volatility_surge_ratio', 'price_stddev_pct', 'trend_consistency_3m',
            'cumulative_return_5m', 'candle_body_pct', 'upper_wick_pct', 'lower_wick_pct',
            'wick_balance_ratio', 'price_vs_ma5_pct', 'breakout_strength_10m',
            'open_price', 'high_price', 'low_price', 'close_price', 'avg_price'
        ]
    },
    'order_book_signals': {
        'prefix': 'ob_',
        'fields': [
            'mid_price', 'price_change_1m', 'price_change_5m', 'price_change_10m',
            'volume_imbalance', 'imbalance_shift_1m', 'imbalance_trend_3m',
            'depth_imbalance_ratio', 'bid_liquidity_share_pct', 'ask_liquidity_share_pct',
            'depth_imbalance_pct', 'total_liquidity', 'liquidity_change_3m',
            'microprice_deviation', 'microprice_acceleration_2m', 'spread_bps',
            'aggression_ratio', 'vwap_spread_bps', 'net_flow_5m', 'net_flow_to_liquidity_ratio',
            'sample_count', 'coverage_seconds'
        ]
    },
    'transactions': {
        'prefix': 'tx_',
        'fields': [
            'buy_sell_pressure', 'buy_volume_pct', 'sell_volume_pct', 'pressure_shift_1m',
            'pressure_trend_3m', 'long_short_ratio', 'long_volume_pct', 'short_volume_pct',
            'perp_position_skew_pct', 'long_ratio_shift_1m', 'perp_dominance_pct',
            'total_volume_usd', 'volume_acceleration_ratio', 'volume_surge_ratio',
            'whale_volume_pct', 'avg_trade_size', 'trades_per_second', 'buy_trade_pct',
            'price_change_1m', 'price_volatility_pct', 'cumulative_buy_flow_5m',
            'trade_count', 'large_trade_count', 'vwap'
        ]
    },
    'whale_activity': {
        'prefix': 'wh_',
        'fields': [
            'net_flow_ratio', 'flow_shift_1m', 'flow_trend_3m', 'accumulation_ratio',
            'strong_accumulation', 'cumulative_flow_5m', 'total_sol_moved',
            'inflow_share_pct', 'outflow_share_pct', 'net_flow_strength_pct',
            'strong_accumulation_pct', 'strong_distribution_pct', 'activity_surge_ratio',
            'movement_count', 'massive_move_pct', 'avg_wallet_pct_moved',
            'largest_move_dominance', 'distribution_pressure_pct', 'outflow_surge_pct',
            'movement_imbalance_pct', 'inflow_sol', 'outflow_sol', 'net_flow_sol',
            'inflow_count', 'outflow_count', 'massive_move_count', 'max_move_size',
            'strong_distribution'
        ]
    },
    'patterns': {
        'prefix': 'pat_',
        'fields': [
            'breakout_score', 'detected_count', 'detected_list',
            'asc_tri_detected', 'asc_tri_confidence', 'asc_tri_resistance_level',
            'asc_tri_support_level', 'asc_tri_compression_ratio',
            'bull_flag_detected', 'bull_flag_confidence', 'bull_flag_pole_height_pct',
            'bull_flag_retracement_pct'
        ]
    }
}

# Field types (boolean fields)
BOOLEAN_FIELDS = {
    'pat_asc_tri_detected', 'pat_bull_flag_detected', 'pat_cup_handle_detected',
    'pat_inv_head_shoulders_detected', 'pat_double_bottom_detected'
}


@app.route('/trail/sections', methods=['GET'])
def get_trail_sections():
    """Get available trail data sections and their fields."""
    try:
        section = request.args.get('section')
        
        if section and section in TRAIL_SECTIONS:
            # Return fields for specific section
            section_data = TRAIL_SECTIONS[section]
            prefix = section_data['prefix']
            fields = section_data['fields']
            
            # Determine field types
            field_types = {}
            for field in fields:
                full_col = prefix + field
                if full_col in BOOLEAN_FIELDS:
                    field_types[field] = 'BOOLEAN'
                else:
                    field_types[field] = 'NUMERIC'
            
            return jsonify({
                'success': True,
                'section': section,
                'prefix': prefix,
                'fields': fields,
                'field_types': field_types
            })
        else:
            # Return all sections
            sections_list = []
            for sec_name, sec_data in TRAIL_SECTIONS.items():
                sections_list.append({
                    'name': sec_name,
                    'prefix': sec_data['prefix'],
                    'field_count': len(sec_data['fields'])
                })
            
            return jsonify({
                'success': True,
                'sections': sections_list
            })
    except Exception as e:
        logger.error(f"Get trail sections failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/trail/field_stats', methods=['POST'])
def get_trail_field_stats():
    """Get field statistics for a section/minute, broken down by gain ranges."""
    try:
        data = request.get_json() or {}
        project_id = data.get('project_id')
        section = data.get('section', 'price_movements')
        minute = int(data.get('minute', 0))
        status = data.get('status', 'all')
        hours = int(data.get('hours', 24))
        analyse_mode = data.get('analyse_mode', 'all')  # 'all' or 'passed'
        
        if section not in TRAIL_SECTIONS:
            return jsonify({'success': False, 'error': 'Invalid section'}), 400
        
        section_data = TRAIL_SECTIONS[section]
        prefix = section_data['prefix']
        fields = section_data['fields']
        
        # Define gain ranges
        gain_ranges = [
            {'id': 'negative', 'label': '< 0%', 'min': None, 'max': 0},
            {'id': '0_to_0.1', 'label': '0-0.1%', 'min': 0, 'max': 0.1},
            {'id': '0.1_to_0.2', 'label': '0.1-0.2%', 'min': 0.1, 'max': 0.2},
            {'id': '0.2_to_0.3', 'label': '0.2-0.3%', 'min': 0.2, 'max': 0.3},
            {'id': '0.3_to_0.5', 'label': '0.3-0.5%', 'min': 0.3, 'max': 0.5},
            {'id': '0.5_to_1', 'label': '0.5-1%', 'min': 0.5, 'max': 1},
            {'id': '1_to_2', 'label': '1-2%', 'min': 1, 'max': 2},
            {'id': '2_plus', 'label': '2%+', 'min': 2, 'max': None},
        ]
        
        # Build WHERE clause
        where_conditions = ["t.minute = %s"]
        params = [minute]
        
        # Status filter
        if status and status != 'all':
            where_conditions.append("b.our_status = %s")
            params.append(status)
        
        # Time filter
        if hours:
            where_conditions.append("b.followed_at >= NOW() - INTERVAL '%s hours'")
            params.append(hours)
        
        # Get active filters if analyse_mode is 'passed'
        filter_conditions = []
        if project_id and analyse_mode == 'passed':
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT field_column, from_value, to_value, include_null, exclude_mode
                        FROM pattern_config_filters
                        WHERE project_id = %s AND is_active = 1
                    """, [project_id])
                    filters = cursor.fetchall()
                    
                    for f in filters:
                        col = f['field_column']
                        from_val = f['from_value']
                        to_val = f['to_value']
                        include_null = f['include_null']
                        exclude_mode = f['exclude_mode']
                        
                        if exclude_mode:
                            # Exclude mode: rows NOT in range
                            if from_val is not None and to_val is not None:
                                filter_conditions.append(f"(t.{col} < %s OR t.{col} > %s)")
                                params.extend([from_val, to_val])
                        else:
                            # Include mode: rows in range
                            cond_parts = []
                            if from_val is not None:
                                cond_parts.append(f"t.{col} >= %s")
                                params.append(from_val)
                            if to_val is not None:
                                cond_parts.append(f"t.{col} <= %s")
                                params.append(to_val)
                            
                            if include_null:
                                full_cond = f"({' AND '.join(cond_parts)} OR t.{col} IS NULL)"
                            else:
                                full_cond = f"({' AND '.join(cond_parts)})"
                            
                            if cond_parts:
                                filter_conditions.append(full_cond)
        
        where_clause = ' AND '.join(where_conditions + filter_conditions)
        
        # Build field statistics query
        field_stats = {}
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for field in fields:
                    col_name = prefix + field
                    full_col = f"t.{col_name}"
                    is_boolean = col_name in BOOLEAN_FIELDS
                    
                    # For boolean fields, calculate % TRUE
                    if is_boolean:
                        agg_expr = f"AVG(CASE WHEN {full_col} = true THEN 100.0 ELSE 0.0 END)"
                    else:
                        agg_expr = f"AVG({full_col})"
                    
                    field_stats[field] = {'type': 'BOOLEAN' if is_boolean else 'NUMERIC', 'ranges': {}}
                    
                    for gain_range in gain_ranges:
                        range_id = gain_range['id']
                        range_min = gain_range['min']
                        range_max = gain_range['max']
                        
                        # Build gain range condition
                        gain_cond = []
                        gain_params = list(params)
                        
                        if range_min is not None:
                            gain_cond.append("b.potential_gains >= %s")
                            gain_params.append(range_min)
                        if range_max is not None:
                            gain_cond.append("b.potential_gains < %s")
                            gain_params.append(range_max)
                        
                        gain_where = f" AND {' AND '.join(gain_cond)}" if gain_cond else ""
                        
                        query = f"""
                            SELECT {agg_expr} as avg_val, COUNT(*) as cnt
                            FROM buyin_trail_minutes t
                            JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                            WHERE {where_clause} {gain_where}
                        """
                        
                        cursor.execute(query, gain_params)
                        result = cursor.fetchone()
                        
                        field_stats[field]['ranges'][range_id] = {
                            'avg': float(result['avg_val']) if result and result['avg_val'] is not None else None,
                            'count': int(result['cnt']) if result else 0
                        }
                
                # Get total trades count
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT t.buyin_id) as total
                    FROM buyin_trail_minutes t
                    JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                    WHERE {where_clause}
                """, params)
                total_result = cursor.fetchone()
                total_trades = total_result['total'] if total_result else 0
        
        return jsonify({
            'success': True,
            'field_stats': field_stats,
            'gain_ranges': gain_ranges,
            'total_trades': total_trades
        })
    except Exception as e:
        logger.error(f"Get trail field stats failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/trail/gain_distribution', methods=['POST'])
def get_trail_gain_distribution():
    """Get trade count distribution across gain ranges, with and without filters applied."""
    try:
        data = request.get_json() or {}
        project_id = data.get('project_id')
        minute = int(data.get('minute', 0))
        status = data.get('status', 'all')
        hours = int(data.get('hours', 24))
        apply_filters = data.get('apply_filters', False)
        
        # Define gain ranges
        gain_ranges = [
            {'id': 'negative', 'label': '< 0%', 'min': None, 'max': 0},
            {'id': '0_to_0.1', 'label': '0-0.1%', 'min': 0, 'max': 0.1},
            {'id': '0.1_to_0.2', 'label': '0.1-0.2%', 'min': 0.1, 'max': 0.2},
            {'id': '0.2_to_0.3', 'label': '0.2-0.3%', 'min': 0.2, 'max': 0.3},
            {'id': '0.3_to_0.5', 'label': '0.3-0.5%', 'min': 0.3, 'max': 0.5},
            {'id': '0.5_to_1', 'label': '0.5-1%', 'min': 0.5, 'max': 1},
            {'id': '1_to_2', 'label': '1-2%', 'min': 1, 'max': 2},
            {'id': '2_plus', 'label': '2%+', 'min': 2, 'max': None},
        ]
        
        # Build base WHERE clause
        base_conditions = ["t.minute = %s"]
        base_params = [minute]
        
        if status and status != 'all':
            base_conditions.append("b.our_status = %s")
            base_params.append(status)
        
        if hours:
            base_conditions.append("b.followed_at >= NOW() - INTERVAL '%s hours'")
            base_params.append(hours)
        
        base_where = ' AND '.join(base_conditions)
        
        # Get active filters
        filter_conditions = []
        filter_params = []
        
        if project_id and apply_filters:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT field_column, from_value, to_value, include_null, exclude_mode
                        FROM pattern_config_filters
                        WHERE project_id = %s AND is_active = 1
                    """, [project_id])
                    filters = cursor.fetchall()
                    
                    for f in filters:
                        col = f['field_column']
                        from_val = f['from_value']
                        to_val = f['to_value']
                        include_null = f['include_null']
                        exclude_mode = f['exclude_mode']
                        
                        if exclude_mode:
                            if from_val is not None and to_val is not None:
                                filter_conditions.append(f"(t.{col} < %s OR t.{col} > %s)")
                                filter_params.extend([from_val, to_val])
                        else:
                            cond_parts = []
                            if from_val is not None:
                                cond_parts.append(f"t.{col} >= %s")
                                filter_params.append(from_val)
                            if to_val is not None:
                                cond_parts.append(f"t.{col} <= %s")
                                filter_params.append(to_val)
                            
                            if cond_parts:
                                if include_null:
                                    filter_conditions.append(f"({' AND '.join(cond_parts)} OR t.{col} IS NULL)")
                                else:
                                    filter_conditions.append(f"({' AND '.join(cond_parts)})")
        
        distribution = []
        total_base = 0
        total_filtered = 0
        total_avg_gain_base = 0
        total_avg_gain_filtered = 0
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                for gain_range in gain_ranges:
                    range_id = gain_range['id']
                    range_min = gain_range['min']
                    range_max = gain_range['max']
                    
                    # Build gain range condition
                    gain_cond = []
                    if range_min is not None:
                        gain_cond.append("b.potential_gains >= %s")
                    if range_max is not None:
                        gain_cond.append("b.potential_gains < %s")
                    
                    gain_where = f" AND {' AND '.join(gain_cond)}" if gain_cond else ""
                    
                    # Base count (without filters)
                    base_query_params = list(base_params)
                    if range_min is not None:
                        base_query_params.append(range_min)
                    if range_max is not None:
                        base_query_params.append(range_max)
                    
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT t.buyin_id) as cnt
                        FROM buyin_trail_minutes t
                        JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                        WHERE {base_where} {gain_where}
                    """, base_query_params)
                    base_result = cursor.fetchone()
                    base_count = int(base_result['cnt']) if base_result else 0
                    
                    # Filtered count (with filters)
                    if filter_conditions:
                        filter_where = ' AND '.join(filter_conditions)
                        filtered_query_params = base_query_params + filter_params
                        
                        cursor.execute(f"""
                            SELECT COUNT(DISTINCT t.buyin_id) as cnt
                            FROM buyin_trail_minutes t
                            JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                            WHERE {base_where} AND {filter_where} {gain_where}
                        """, filtered_query_params)
                        filtered_result = cursor.fetchone()
                        filtered_count = int(filtered_result['cnt']) if filtered_result else 0
                    else:
                        filtered_count = base_count
                    
                    distribution.append({
                        'id': range_id,
                        'label': gain_range['label'],
                        'base_count': base_count,
                        'filtered_count': filtered_count,
                        'removed': base_count - filtered_count
                    })
                    
                    total_base += base_count
                    total_filtered += filtered_count
                
                # Get average gains
                cursor.execute(f"""
                    SELECT AVG(b.potential_gains) as avg_gain
                    FROM buyin_trail_minutes t
                    JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                    WHERE {base_where}
                """, base_params)
                base_avg = cursor.fetchone()
                total_avg_gain_base = float(base_avg['avg_gain']) if base_avg and base_avg['avg_gain'] else 0
                
                if filter_conditions:
                    filter_where = ' AND '.join(filter_conditions)
                    cursor.execute(f"""
                        SELECT AVG(b.potential_gains) as avg_gain
                        FROM buyin_trail_minutes t
                        JOIN follow_the_goat_buyins b ON t.buyin_id = b.id
                        WHERE {base_where} AND {filter_where}
                    """, base_params + filter_params)
                    filtered_avg = cursor.fetchone()
                    total_avg_gain_filtered = float(filtered_avg['avg_gain']) if filtered_avg and filtered_avg['avg_gain'] else 0
                else:
                    total_avg_gain_filtered = total_avg_gain_base
        
        return jsonify({
            'success': True,
            'distribution': distribution,
            'totals': {
                'base': total_base,
                'filtered': total_filtered,
                'removed': total_base - total_filtered
            },
            'gains': {
                'base_avg': total_avg_gain_base,
                'filtered_avg': total_avg_gain_filtered
            }
        })
    except Exception as e:
        logger.error(f"Get trail gain distribution failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# FILTER ANALYSIS ENDPOINTS
# =============================================================================

@app.route('/filter-analysis/dashboard', methods=['GET'])
def get_filter_analysis_dashboard():
    """Get complete filter analysis dashboard data."""
    try:
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
            'rolling_avgs': {},
            'play_updates': []
        }
        
        # Load filter settings from config file
        import json
        from pathlib import Path
        config_path = Path(__file__).parent.parent / "000data_feeds" / "7_create_new_patterns" / "config.json"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            config = {
                'good_trade_threshold': 0.3,
                'analysis_hours': 24,
                'min_filters_in_combo': 1,
                'min_good_trades_kept_pct': 50,
                'min_bad_trades_removed_pct': 10
            }
        
        # Format settings for frontend (matching GET /filter-analysis/settings format)
        result['settings'] = [
            {
                'setting_key': 'good_trade_threshold',
                'setting_value': str(config.get('good_trade_threshold', 0.3)),
                'description': 'Good trade threshold percentage',
                'setting_type': 'decimal',
                'min_value': 0.1,
                'max_value': 5.0
            },
            {
                'setting_key': 'analysis_hours',
                'setting_value': str(config.get('analysis_hours', 24)),
                'description': 'Analysis window in hours',
                'setting_type': 'integer',
                'min_value': 1,
                'max_value': 168
            },
            {
                'setting_key': 'min_filters_in_combo',
                'setting_value': str(config.get('min_filters_in_combo', 1)),
                'description': 'Minimum filters in combination',
                'setting_type': 'integer',
                'min_value': 1,
                'max_value': 10
            },
            {
                'setting_key': 'min_good_trades_kept_pct',
                'setting_value': str(config.get('min_good_trades_kept_pct', 50)),
                'description': 'Minimum good trades kept percentage',
                'setting_type': 'integer',
                'min_value': 0,
                'max_value': 100
            },
            {
                'setting_key': 'min_bad_trades_removed_pct',
                'setting_value': str(config.get('min_bad_trades_removed_pct', 10)),
                'description': 'Minimum bad trades removed percentage',
                'setting_type': 'integer',
                'min_value': 0,
                'max_value': 100
            }
        ]
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Summary statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_filters, 
                        CAST(AVG(good_trades_kept_pct) AS NUMERIC(10,1)) as avg_good_kept,
                        CAST(AVG(bad_trades_removed_pct) AS NUMERIC(10,1)) as avg_bad_removed, 
                        CAST(MAX(bad_trades_removed_pct) AS NUMERIC(10,1)) as best_bad_removed,
                        MAX(good_trades_before) as total_good_trades, 
                        MAX(bad_trades_before) as total_bad_trades,
                        MAX(created_at) as last_updated, 
                        MAX(analysis_hours) as analysis_hours
                    FROM filter_reference_suggestions
                """)
                summary_row = cursor.fetchone()
                if summary_row:
                    result['summary'] = {
                        'total_filters': summary_row['total_filters'] or 0,
                        'avg_good_kept': summary_row['avg_good_kept'] or 0,
                        'avg_bad_removed': summary_row['avg_bad_removed'] or 0,
                        'best_bad_removed': summary_row['best_bad_removed'] or 0,
                        'total_good_trades': summary_row['total_good_trades'] or 0,
                        'total_bad_trades': summary_row['total_bad_trades'] or 0,
                        'last_updated': str(summary_row['last_updated'])[:19] if summary_row['last_updated'] else None,
                        'analysis_hours': summary_row['analysis_hours'] or 24
                    }
                
                # Minute distribution
                cursor.execute("""
                    SELECT 
                        minute_analyzed, 
                        COUNT(*) as filter_count, 
                        CAST(AVG(bad_trades_removed_pct) AS NUMERIC(10,1)) as avg_bad_removed, 
                        CAST(AVG(good_trades_kept_pct) AS NUMERIC(10,1)) as avg_good_kept
                    FROM filter_reference_suggestions 
                    GROUP BY minute_analyzed 
                    ORDER BY avg_bad_removed DESC
                """)
                result['minute_distribution'] = cursor.fetchall()
                
                # All suggestions with field info
                cursor.execute("""
                    SELECT 
                        frs.id, frs.filter_field_id, frs.column_name, frs.from_value, frs.to_value,
                        frs.total_trades, frs.good_trades_before, frs.bad_trades_before,
                        frs.good_trades_after, frs.bad_trades_after,
                        frs.good_trades_kept_pct, frs.bad_trades_removed_pct,
                        frs.bad_negative_count, frs.bad_0_to_01_count, frs.bad_01_to_02_count, frs.bad_02_to_03_count,
                        frs.analysis_hours, frs.minute_analyzed, frs.created_at,
                        COALESCE(frs.section, ffc.section, 'unknown') as section, 
                        COALESCE(ffc.field_name, frs.column_name) as field_name, 
                        'numeric' as value_type
                    FROM filter_reference_suggestions frs 
                    LEFT JOIN filter_fields_catalog ffc ON frs.filter_field_id = ffc.id
                    ORDER BY frs.bad_trades_removed_pct DESC
                """)
                suggestions = cursor.fetchall()
                for s in suggestions:
                    if s.get('created_at'):
                        s['created_at'] = str(s['created_at'])[:19]
                result['suggestions'] = suggestions
                
                # Filter combinations
                cursor.execute("""
                    SELECT 
                        id, combination_name, filter_count, filter_ids, filter_columns,
                        total_trades, good_trades_before, bad_trades_before,
                        good_trades_after, bad_trades_after,
                        good_trades_kept_pct, bad_trades_removed_pct,
                        best_single_bad_removed_pct, improvement_over_single,
                        bad_negative_count, bad_0_to_01_count, bad_01_to_02_count, bad_02_to_03_count,
                        COALESCE(minute_analyzed, 0) as minute_analyzed, analysis_hours
                    FROM filter_combinations 
                    ORDER BY bad_trades_removed_pct DESC
                    LIMIT 20
                """)
                result['combinations'] = cursor.fetchall()
                
                # Scheduler runs (get recent runs from filter_scheduler_runs)
                cursor.execute("""
                    SELECT 
                        run_timestamp,
                        completed_at,
                        status,
                        total_filters_analyzed,
                        filters_saved,
                        best_bad_removed_pct,
                        best_good_kept_pct,
                        analysis_hours,
                        EXTRACT(EPOCH FROM (completed_at - run_timestamp)) as duration_seconds
                    FROM filter_scheduler_runs
                    ORDER BY run_timestamp DESC
                    LIMIT 20
                """)
                scheduler_runs = cursor.fetchall()
                for run in scheduler_runs:
                    run['run_timestamp'] = str(run['run_timestamp'])[:19] if run.get('run_timestamp') else None
                    run['completed_at'] = str(run['completed_at'])[:19] if run.get('completed_at') else None
                result['scheduler_runs'] = scheduler_runs
                
                # Scheduler stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as runs_today,
                        MAX(run_timestamp) as last_run
                    FROM filter_scheduler_runs
                    WHERE run_timestamp >= CURRENT_DATE
                """)
                stats_row = cursor.fetchone()
                if stats_row:
                    result['scheduler_stats'] = {
                        'runs_today': stats_row['runs_today'] or 0,
                        'last_run': str(stats_row['last_run'])[:19] if stats_row.get('last_run') else None,
                        'avg_filters': result['summary'].get('total_filters', 0)
                    }
                
                # Filter consistency (filters that appear frequently in best combinations)
                cursor.execute("""
                    SELECT 
                        column_name as filter_column,
                        COUNT(*) as total_runs,
                        CAST(AVG(bad_trades_removed_pct) AS NUMERIC(10,1)) as avg_bad_removed,
                        CAST(AVG(good_trades_kept_pct) AS NUMERIC(10,1)) as avg_good_kept,
                        MAX(minute_analyzed) as latest_minute,
                        MAX(from_value) as latest_from,
                        MAX(to_value) as latest_to,
                        CAST((COUNT(*) * 100.0 / NULLIF((SELECT COUNT(DISTINCT created_at::date) 
                            FROM filter_reference_suggestions), 0)) AS NUMERIC(10,1)) as consistency_pct
                    FROM filter_reference_suggestions
                    WHERE bad_trades_removed_pct >= 30
                    GROUP BY column_name
                    HAVING COUNT(*) >= 2
                    ORDER BY consistency_pct DESC, avg_bad_removed DESC
                    LIMIT 50
                """)
                result['filter_consistency'] = cursor.fetchall()
                
                # AI Play Updates - shows which plays were updated by auto-filter system
                cursor.execute("""
                    SELECT 
                        apu.id, apu.play_id, apu.play_name, apu.project_id, apu.project_name,
                        apu.pattern_count, apu.filters_applied, apu.updated_at, apu.run_id, apu.status
                    FROM ai_play_updates apu
                    ORDER BY apu.updated_at DESC
                    LIMIT 50
                """)
                play_updates = cursor.fetchall()
                for pu in play_updates:
                    if pu.get('updated_at'):
                        pu['updated_at'] = str(pu['updated_at'])[:19]
                result['play_updates'] = play_updates
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Get filter analysis dashboard failed: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'summary': {},
            'suggestions': [],
            'combinations': [],
            'minute_distribution': [],
            'scheduler_runs': [],
            'filter_consistency': [],
            'trend_chart_data': [],
            'scheduler_stats': {'runs_today': 0, 'last_run': None, 'avg_filters': 0},
            'settings': [],
            'rolling_avgs': {},
            'play_updates': []
        })


@app.route('/filter-analysis/settings', methods=['GET'])
def get_filter_settings():
    """Get auto filter settings from config file."""
    import json
    from pathlib import Path
    
    try:
        # Path to config file
        config_path = Path(__file__).parent.parent / "000data_feeds" / "7_create_new_patterns" / "config.json"
        
        # Load config
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            # Default values
            config = {
                'good_trade_threshold': 0.3,
                'analysis_hours': 24,
                'min_filters_in_combo': 1,
                'min_good_trades_kept_pct': 50,
                'min_bad_trades_removed_pct': 10
            }
        
        return jsonify({
            'success': True,
            'settings': [
                {
                    'setting_key': 'good_trade_threshold',
                    'setting_value': str(config.get('good_trade_threshold', 0.3)),
                    'description': 'Good trade threshold percentage',
                    'setting_type': 'decimal',
                    'min_value': 0.1,
                    'max_value': 5.0
                },
                {
                    'setting_key': 'analysis_hours',
                    'setting_value': str(config.get('analysis_hours', 24)),
                    'description': 'Analysis window in hours',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 168
                },
                {
                    'setting_key': 'min_filters_in_combo',
                    'setting_value': str(config.get('min_filters_in_combo', 1)),
                    'description': 'Minimum filters in combination',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 10
                },
                {
                    'setting_key': 'min_good_trades_kept_pct',
                    'setting_value': str(config.get('min_good_trades_kept_pct', 50)),
                    'description': 'Minimum good trades kept percentage',
                    'setting_type': 'integer',
                    'min_value': 0,
                    'max_value': 100
                },
                {
                    'setting_key': 'min_bad_trades_removed_pct',
                    'setting_value': str(config.get('min_bad_trades_removed_pct', 10)),
                    'description': 'Minimum bad trades removed percentage',
                    'setting_type': 'integer',
                    'min_value': 0,
                    'max_value': 100
                }
            ]
        })
    except Exception as e:
        logger.error(f"Get filter settings failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/filter-analysis/settings', methods=['POST'])
def save_filter_settings():
    """Save auto filter settings to config file."""
    import json
    from pathlib import Path
    
    try:
        data = request.get_json() or {}
        settings = data.get('settings', {})
        
        # Path to config file
        config_path = Path(__file__).parent.parent / "000data_feeds" / "7_create_new_patterns" / "config.json"
        
        # Load existing config
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
        else:
            config = {}
        
        # Update with new settings (preserve all other config values)
        if 'good_trade_threshold' in settings:
            config['good_trade_threshold'] = float(settings['good_trade_threshold'])
        if 'analysis_hours' in settings:
            config['analysis_hours'] = int(settings['analysis_hours'])
        if 'min_filters_in_combo' in settings:
            config['min_filters_in_combo'] = int(settings['min_filters_in_combo'])
        if 'min_good_trades_kept_pct' in settings:
            config['min_good_trades_kept_pct'] = float(settings['min_good_trades_kept_pct'])
        if 'min_bad_trades_removed_pct' in settings:
            config['min_bad_trades_removed_pct'] = float(settings['min_bad_trades_removed_pct'])
        
        # Ensure directory exists
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to file (preserve all existing config values)
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Filter settings saved to {config_path}")
        
        return jsonify({
            'success': True,
            'message': 'Settings saved successfully to config file',
            'current_settings': {
                'good_trade_threshold': config.get('good_trade_threshold', 0.3),
                'analysis_hours': config.get('analysis_hours', 24),
                'min_filters_in_combo': config.get('min_filters_in_combo', 1),
                'min_good_trades_kept_pct': config.get('min_good_trades_kept_pct', 50),
                'min_bad_trades_removed_pct': config.get('min_bad_trades_removed_pct', 10)
            }
        })
    except Exception as e:
        logger.error(f"Save filter settings failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# QUERY ENDPOINT (STRUCTURED + RAW SQL)
# =============================================================================

@app.route('/query', methods=['POST'])
def query():
    """Execute query - supports both structured params and raw SQL."""
    try:
        data = request.json or {}
        
        # Check if this is a raw SQL query
        if 'sql' in data:
            sql = data.get('sql', '').strip()
            
            # Security: only allow SELECT
            if not sql.upper().startswith('SELECT'):
                return jsonify({'error': 'Only SELECT queries allowed'}), 400
            
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    results = cursor.fetchall()
            
            return jsonify({'results': results, 'count': len(results)})
        
        # Otherwise, handle structured query (for PHP DatabaseClient compatibility)
        table = data.get('table')
        if not table:
            return jsonify({'error': 'table parameter required'}), 400
        
        columns = data.get('columns', ['*'])
        where = data.get('where')
        order_by = data.get('order_by')
        limit = min(int(data.get('limit', 100)), 5000)
        
        # Special handling for order_book_features - map column names
        column_mappings = {}
        if table == 'order_book_features':
            column_mappings = {
                'ts': 'timestamp',
                'symbol': "'SOLUSDT'",  # Hardcoded since we only track SOLUSDT
                'relative_spread_bps': 'spread_bps',
                'bid_depth_10': 'bid_liquidity',
                'ask_depth_10': 'ask_liquidity',
                'best_bid': "CAST((bids_json::json->0->>0) AS DOUBLE PRECISION)",
                'best_ask': "CAST((asks_json::json->0->>0) AS DOUBLE PRECISION)"
            }
        
        # Build query
        if columns == ['*'] or not columns:
            columns_str = '*'
        else:
            # Sanitize and map column names
            safe_columns = []
            for col in columns:
                if isinstance(col, str) and col.replace('_', '').isalnum():
                    # Apply column mapping if exists
                    mapped_col = column_mappings.get(col, col)
                    # Add alias if it was mapped
                    if col in column_mappings and not mapped_col.startswith('CAST'):
                        safe_columns.append(f"{mapped_col} AS {col}")
                    elif col in column_mappings and mapped_col.startswith('CAST'):
                        safe_columns.append(f"{mapped_col} AS {col}")
                    else:
                        safe_columns.append(col)
            columns_str = ', '.join(safe_columns) if safe_columns else '*'
        
        query_parts = [f"SELECT {columns_str} FROM {table}"]
        params = []
        
        # Add WHERE clause if provided
        if where:
            where_conditions = []
            for key, value in where.items():
                # Sanitize key
                if isinstance(key, str) and key.replace('_', '').isalnum():
                    where_conditions.append(f"{key} = %s")
                    params.append(value)
            
            if where_conditions:
                query_parts.append("WHERE " + " AND ".join(where_conditions))
        
        # Add ORDER BY
        if order_by:
            # Basic sanitization - only allow alphanumeric, underscore, space, comma, ASC, DESC
            if isinstance(order_by, str):
                cleaned = order_by.replace('ASC', '').replace('DESC', '').replace(',', '').replace(' ', '').replace('_', '')
                if cleaned.isalnum():
                    # Apply column mapping for order_by field
                    order_by_mapped = order_by
                    for alias, real_col in column_mappings.items():
                        if order_by.startswith(alias):
                            # Replace only if it's not a complex expression
                            if not real_col.startswith('CAST'):
                                order_by_mapped = order_by.replace(alias, real_col, 1)
                            break
                    query_parts.append(f"ORDER BY {order_by_mapped}")
        
        # Add LIMIT
        query_parts.append(f"LIMIT %s")
        params.append(limit)
        
        sql = ' '.join(query_parts)
        logger.debug(f"Executing query: {sql} with params: {params}")
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                results = cursor.fetchall()
        
        return jsonify({
            'results': results,
            'count': len(results),
            'source': 'postgres'
        })
        
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
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

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
                    {'query': data['find_wallets_sql']},
                    data.get('sell_logic', {'tolerance_rules': {'increases': [], 'decreases': []}}),
                    data.get('max_buys_per_cycle', 5),
                    data.get('short_play', 0),
                    data.get('trigger_on_perp', {'mode': 'any'}),
                    data.get('timing_conditions', {'enabled': False}),
                    data.get('bundle_trades', {'enabled': False}),
                    data.get('cashe_wallets', {'enabled': False}),
                    data.get('project_ids', []) if data.get('project_ids') else None,
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
                
                # Create duplicate
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
                    original.get('find_wallets_sql'),
                    original.get('sell_logic'),
                    original.get('max_buys_per_cycle', 5),
                    original.get('short_play', 0),
                    original.get('tricker_on_perp'),
                    original.get('timing_conditions'),
                    original.get('bundle_trades'),
                    original.get('cashe_wallets'),
                    original.get('project_ids'),
                    1,  # is_active = 1
                    original.get('sorting', 10),
                    original.get('pattern_validator_enable', 0),
                    original.get('pattern_update_by_ai', 0),
                    original.get('pattern_validator'),
                    original.get('cashe_wallets_settings')
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

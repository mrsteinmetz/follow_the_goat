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
from datetime import datetime, timedelta, timezone
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
# GLOBAL ERROR HANDLING (prevent crashes from taking down the server)
# =============================================================================

@app.errorhandler(Exception)
def handle_uncaught(exc):
    """Catch uncaught exceptions and return 500 instead of crashing the process. Let HTTP errors (404, etc.) pass through."""
    from werkzeug.exceptions import HTTPException
    if isinstance(exc, HTTPException):
        return exc
    logger.error(f"Uncaught exception: {exc}", exc_info=True)
    return jsonify({"error": "Internal server error", "status": "error"}), 500


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
            'timestamp': datetime.now(timezone.utc).isoformat()
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


@app.route('/pump_training_entries', methods=['GET'])
def get_pump_training_entries():
    """Get clean_pump entry points (analytics) for the pumps chart.

    Returns rows from pump_training_labels: timestamps and prices where the
    pump model's path-aware labeling marked a minute-0 entry as 'clean_pump'
    (price rose >= 0.2% within 4 min without crashing). Used by pumps.php.
    """
    try:
        hours = request.args.get('hours', 48, type=int)
        hours = max(1, min(168, hours))
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT followed_at, buyin_id, entry_price, max_fwd_pct, time_to_peak_min, label
                    FROM pump_training_labels
                    WHERE followed_at >= NOW() - INTERVAL '1 hour' * %s
                    ORDER BY followed_at ASC
                """, [hours])
                rows = cursor.fetchall()
        # Serialize for JSON (timestamps to ISO string)
        entries = []
        for row in rows:
            followed = row.get('followed_at')
            if hasattr(followed, 'isoformat'):
                followed = followed.isoformat()
            elif followed is not None:
                followed = str(followed)
            entries.append({
                'followed_at': followed,
                'buyin_id': row.get('buyin_id'),
                'entry_price': float(row['entry_price']) if row.get('entry_price') is not None else None,
                'max_fwd_pct': float(row['max_fwd_pct']) if row.get('max_fwd_pct') is not None else None,
                'time_to_peak_min': float(row['time_to_peak_min']) if row.get('time_to_peak_min') is not None else None,
                'label': row.get('label', 'clean_pump'),
            })
        return jsonify({'entries': entries, 'count': len(entries)})
    except Exception as e:
        if 'pump_training_labels' in str(e) and ('does not exist' in str(e) or 'relation' in str(e).lower()):
            return jsonify({'entries': [], 'count': 0})  # Table not created yet (no refresh run)
        logger.error(f"Get pump training entries failed: {e}", exc_info=True)
        return jsonify({'error': str(e), 'entries': [], 'count': 0}), 500


@app.route('/pump/analytics', methods=['GET'])
def get_pump_analytics():
    """Pump Analytics dashboard data: signal outcomes, continuation history,
    fingerprint rules summary, and aggregate stats."""
    hours = request.args.get('hours', 24, type=int)
    hours = max(1, min(168, hours))
    limit = request.args.get('limit', 50, type=int)
    limit = max(1, min(200, limit))

    result = {
        'signal_summary': {},
        'continuation_summary': {},
        'fingerprint_rules': {},
        'recent_outcomes': [],
        'recent_continuation': [],
    }

    def _ts(val):
        if hasattr(val, 'isoformat'):
            return val.isoformat()
        return str(val) if val is not None else None

    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                # --- Signal outcomes summary ---
                cur.execute("""
                    SELECT
                        COUNT(*)                                      AS n_total,
                        COUNT(*) FILTER (WHERE hit_target = TRUE)     AS n_hits,
                        AVG(gain_pct)                                 AS avg_gain,
                        AVG(confidence)                               AS avg_confidence,
                        AVG(readiness_score)                          AS avg_readiness
                    FROM pump_signal_outcomes
                    WHERE created_at >= NOW() - INTERVAL '1 hour' * %s
                """, [hours])
                row = cur.fetchone()
                n_total = int(row['n_total'] or 0)
                n_hits = int(row['n_hits'] or 0)
                result['signal_summary'] = {
                    'n_total': n_total,
                    'n_hits': n_hits,
                    'win_rate': round(n_hits / n_total * 100, 1) if n_total > 0 else None,
                    'avg_gain': round(float(row['avg_gain']), 4) if row['avg_gain'] is not None else None,
                    'avg_confidence': round(float(row['avg_confidence']), 4) if row['avg_confidence'] is not None else None,
                    'avg_readiness': round(float(row['avg_readiness']), 4) if row['avg_readiness'] is not None else None,
                }

                # --- Circuit breaker (last 20 outcomes) ---
                cur.execute("""
                    SELECT hit_target, gain_pct
                    FROM pump_signal_outcomes
                    ORDER BY created_at DESC
                    LIMIT 20
                """)
                cb_rows = cur.fetchall()
                cb_n = len(cb_rows)
                cb_hits = sum(1 for r in cb_rows if r.get('hit_target'))
                cb_wr = round(cb_hits / cb_n * 100, 1) if cb_n > 0 else None
                result['signal_summary']['circuit_breaker'] = {
                    'n_recent': cb_n,
                    'hits_recent': cb_hits,
                    'win_rate_recent': cb_wr,
                    'tripped': cb_wr is not None and cb_wr < 35.0 and cb_n >= 10,
                }

                # --- Recent outcomes ---
                cur.execute("""
                    SELECT id, buyin_id, hit_target, gain_pct, confidence,
                           readiness_score, rule_id, pattern_id,
                           top_features_json, gates_passed_json, created_at
                    FROM pump_signal_outcomes
                    WHERE created_at >= NOW() - INTERVAL '1 hour' * %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, [hours, limit])
                for r in cur.fetchall():
                    result['recent_outcomes'].append({
                        'id': r['id'],
                        'buyin_id': r.get('buyin_id'),
                        'hit_target': r.get('hit_target'),
                        'gain_pct': round(float(r['gain_pct']), 4) if r.get('gain_pct') is not None else None,
                        'confidence': round(float(r['confidence']), 4) if r.get('confidence') is not None else None,
                        'readiness_score': round(float(r['readiness_score']), 4) if r.get('readiness_score') is not None else None,
                        'rule_id': r.get('rule_id'),
                        'pattern_id': r.get('pattern_id'),
                        'top_features': r.get('top_features_json'),
                        'gates_passed': r.get('gates_passed_json'),
                        'created_at': _ts(r.get('created_at')),
                    })

                # --- Continuation history summary ---
                cur.execute("""
                    SELECT
                        COUNT(*)                                  AS n_total,
                        COUNT(*) FILTER (WHERE passed = TRUE)     AS n_passed
                    FROM pump_continuation_history
                    WHERE created_at >= NOW() - INTERVAL '1 hour' * %s
                """, [hours])
                ch = cur.fetchone()
                ch_total = int(ch['n_total'] or 0)
                ch_passed = int(ch['n_passed'] or 0)
                result['continuation_summary'] = {
                    'n_total': ch_total,
                    'n_passed': ch_passed,
                    'pass_rate_pct': round(ch_passed / ch_total * 100, 1) if ch_total > 0 else None,
                }

                # --- Recent continuation checks ---
                cur.execute("""
                    SELECT id, buyin_id, passed, reason, rules_checked,
                           pre_entry_change_1m, rule_details, created_at
                    FROM pump_continuation_history
                    WHERE created_at >= NOW() - INTERVAL '1 hour' * %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, [hours, limit])
                for r in cur.fetchall():
                    result['recent_continuation'].append({
                        'id': r['id'],
                        'buyin_id': r.get('buyin_id'),
                        'passed': r.get('passed'),
                        'reason': r.get('reason'),
                        'rules_checked': r.get('rules_checked'),
                        'pre_entry_change_1m': round(float(r['pre_entry_change_1m']), 4) if r.get('pre_entry_change_1m') is not None else None,
                        'rule_details': r.get('rule_details'),
                        'created_at': _ts(r.get('created_at')),
                    })

        # --- Fingerprint rules (from JSON cache on disk) ---
        fp_path = PROJECT_ROOT / 'cache' / 'pump_fingerprint_report.json'
        if fp_path.exists():
            try:
                fp = json.loads(fp_path.read_text())
                ds = fp.get('data_summary', {})
                top_feats = fp.get('feature_rankings', [])[:15]
                patterns = fp.get('approved_patterns', [])
                combos = fp.get('top_combinations', [])
                result['fingerprint_rules'] = {
                    'generated_at': fp.get('generated_at'),
                    'lookback_hours': fp.get('lookback_hours'),
                    'n_entries': ds.get('total_entries'),
                    'n_pumps': ds.get('n_pumps'),
                    'n_independent_pumps': ds.get('n_independent_pumps'),
                    'pump_rate_pct': ds.get('pump_rate_pct'),
                    'n_approved_patterns': len(patterns),
                    'n_combinations': len(combos),
                    'top_features': [{
                        'feature': f.get('feature'),
                        'separation': round(float(f.get('abs_separation', 0)), 4),
                        'median_pump': f.get('median_pump'),
                        'median_non_pump': f.get('median_non_pump'),
                        'rule_eligible': f.get('rule_eligible'),
                    } for f in top_feats],
                    'approved_patterns': [{
                        'cluster_id': p.get('cluster_id'),
                        'precision': p.get('precision'),
                        'n_pumps': p.get('n_pumps'),
                        'features': list(p.get('feature_ranges', {}).keys()) if isinstance(p.get('feature_ranges'), dict) else [],
                    } for p in patterns[:20]],
                    'combinations': [{
                        'features': c.get('features'),
                        'precision': c.get('precision'),
                        'support': c.get('support'),
                    } for c in combos[:20]],
                }
            except Exception as e:
                logger.warning(f"Failed to read fingerprint report: {e}")
                result['fingerprint_rules'] = {'error': str(e)}
        else:
            result['fingerprint_rules'] = {'error': 'No fingerprint report found'}

        # --- Raw cache status (Parquet files written by data feeds) ---
        try:
            from core.raw_data_cache import OB_PARQUET, TRADE_PARQUET, WHALE_PARQUET, open_reader
            import time as _time

            def _parquet_info(p):
                if not p.exists():
                    return {'exists': False, 'rows': 0, 'age_seconds': None, 'size_kb': 0}
                age = _time.time() - p.stat().st_mtime
                return {
                    'exists': True,
                    'size_kb': round(p.stat().st_size / 1024),
                    'age_seconds': round(age, 1),
                }

            ob_info    = _parquet_info(OB_PARQUET)
            trade_info = _parquet_info(TRADE_PARQUET)
            whale_info = _parquet_info(WHALE_PARQUET)

            # Row counts via in-memory DuckDB (fast, no lock)
            try:
                con = open_reader()
                ob_info['rows']    = con.execute("SELECT COUNT(*) FROM ob_snapshots").fetchone()[0]
                trade_info['rows'] = con.execute("SELECT COUNT(*) FROM raw_trades").fetchone()[0]
                whale_info['rows'] = con.execute("SELECT COUNT(*) FROM whale_events").fetchone()[0]
                con.close()
            except Exception:
                pass

            # Live market features
            live_feats = None
            try:
                from core.raw_data_cache import get_live_features
                live_feats = get_live_features(window_min=5)
            except Exception:
                pass

            result['raw_cache'] = {
                'ob':     ob_info,
                'trades': trade_info,
                'whales': whale_info,
                'live_features': live_feats,
            }
        except Exception as rc_err:
            result['raw_cache'] = {'error': str(rc_err)}

        # --- Cycle tracker summary (ground-truth pump counts) ---
        try:
            with get_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            COUNT(*) FILTER (WHERE max_percent_increase >= 0.2) AS p02,
                            COUNT(*) FILTER (WHERE max_percent_increase >= 0.3) AS p03,
                            COUNT(*) FILTER (WHERE max_percent_increase >= 0.4) AS p04
                        FROM cycle_tracker
                        WHERE cycle_start_time >= NOW() - INTERVAL '1 hour' * %s
                    """, [hours])
                    ct = cur.fetchone()
                    result['cycle_tracker'] = {
                        'pumps_0_2pct': int(ct['p02'] or 0),
                        'pumps_0_3pct': int(ct['p03'] or 0),
                        'pumps_0_4pct': int(ct['p04'] or 0),
                        'hours': hours,
                    }
        except Exception as ct_err:
            result['cycle_tracker'] = {'error': str(ct_err)}

        return jsonify(result)

    except Exception as e:
        tbl = str(e)
        if 'does not exist' in tbl or 'relation' in tbl.lower():
            return jsonify(result)
        logger.error(f"Pump analytics failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/pump/opportunity_funnel', methods=['GET'])
def get_pump_opportunity_funnel():
    """Daily opportunity funnel for play 3 pump signals and historical trade scatter."""
    days = request.args.get('days', 7, type=int)
    days = max(1, min(30, days))

    PUMP_PLAY_ID = 3

    result = {
        'daily_funnel': [],
        'all_time_trades': [],
        'summary': {},
    }

    def _ts(val):
        if hasattr(val, 'isoformat'):
            return val.isoformat()
        return str(val) if val is not None else None

    try:
        with get_postgres() as conn:
            with conn.cursor() as cur:
                # --- Daily cycle counts (no_go + sold + error = total cycles) ---
                cur.execute("""
                    SELECT DATE(created_at) AS day,
                           COUNT(*) AS total_cycles,
                           COUNT(*) FILTER (WHERE our_status = 'sold') AS trades_made
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s
                      AND created_at >= NOW() - INTERVAL '1 day' * %s
                    GROUP BY 1 ORDER BY 1
                """, [PUMP_PLAY_ID, days])
                cycle_rows = {str(r['day']): r for r in cur.fetchall()}

                # --- Daily continuation breakdown by reason category ---
                # Note: passed=TRUE can still have "flat"/"error" reasons — categorize by text
                cur.execute("""
                    SELECT DATE(h.created_at) AS day,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (
                               WHERE h.reason NOT ILIKE '%%flat%%'
                                 AND h.reason NOT ILIKE '%%not rising%%'
                                 AND h.reason NOT ILIKE '%%error%%'
                           ) AS cont_passed,
                           COUNT(*) FILTER (
                               WHERE h.reason ILIKE '%%flat%%'
                                  OR h.reason ILIKE '%%not rising%%'
                           ) AS cont_flat,
                           COUNT(*) FILTER (
                               WHERE h.reason ILIKE '%%error%%'
                           ) AS cont_errors
                    FROM pump_continuation_history h
                    JOIN follow_the_goat_buyins b ON b.id = h.buyin_id AND b.play_id = %s
                    WHERE h.created_at >= NOW() - INTERVAL '1 day' * %s
                    GROUP BY 1 ORDER BY 1
                """, [PUMP_PLAY_ID, days])
                cont_rows = {str(r['day']): r for r in cur.fetchall()}

                all_days = sorted(set(list(cycle_rows.keys()) + list(cont_rows.keys())))
                total_cycles_sum = 0
                total_trades_sum = 0
                for day in all_days:
                    cy = cycle_rows.get(day, {})
                    co = cont_rows.get(day, {})
                    tc = int(cy.get('total_cycles') or 0)
                    tm = int(cy.get('trades_made') or 0)
                    total_cycles_sum += tc
                    total_trades_sum += tm
                    result['daily_funnel'].append({
                        'date': day,
                        'total_cycles': tc,
                        'trades_made': tm,
                        'cont_passed': int(co.get('cont_passed') or 0),
                        'cont_flat': int(co.get('cont_flat') or 0),
                        'cont_errors': int(co.get('cont_errors') or 0),
                    })

                n_days = len(all_days) or 1
                result['summary'] = {
                    'days': days,
                    'total_cycles': total_cycles_sum,
                    'total_trades': total_trades_sum,
                    'avg_trades_per_day': round(total_trades_sum / n_days, 1),
                }

                # --- All-time sold trades for play 3 ---
                cur.execute("""
                    SELECT id, followed_at, our_entry_price, our_exit_price,
                           our_profit_loss, our_status, entry_log, created_at
                    FROM follow_the_goat_buyins
                    WHERE play_id = %s AND our_status = 'sold'
                    ORDER BY created_at ASC
                """, [PUMP_PLAY_ID])
                trades = cur.fetchall()
                wins = 0
                for t in trades:
                    pnl = float(t['our_profit_loss']) if t.get('our_profit_loss') is not None else None
                    gain_pct = None
                    if t.get('our_exit_price') and t.get('our_entry_price') and float(t['our_entry_price']) > 0:
                        gain_pct = round((float(t['our_exit_price']) - float(t['our_entry_price'])) / float(t['our_entry_price']) * 100, 4)
                    if pnl is not None and pnl > 0:
                        wins += 1
                    el = t.get('entry_log')
                    if not isinstance(el, dict):
                        el = {}
                    result['all_time_trades'].append({
                        'id': t['id'],
                        'followed_at': _ts(t.get('followed_at')),
                        'created_at': _ts(t.get('created_at')),
                        'our_entry_price': float(t['our_entry_price']) if t.get('our_entry_price') is not None else None,
                        'our_exit_price': float(t['our_exit_price']) if t.get('our_exit_price') is not None else None,
                        'our_profit_loss': pnl,
                        'gain_pct': gain_pct,
                        'model_confidence': el.get('model_confidence'),
                    })
                n_trades = len(trades)
                result['summary']['win_rate_pct'] = round(wins / n_trades * 100, 1) if n_trades > 0 else None
                result['summary']['total_all_time_trades'] = n_trades

        return jsonify(result)

    except Exception as e:
        tbl = str(e)
        if 'does not exist' in tbl or 'relation' in tbl.lower():
            return jsonify(result)
        logger.error(f"Pump opportunity funnel failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/pump/feature_export', methods=['GET'])
def get_pump_feature_export():
    """Export the last N rows (default 100) of the 30-second feature matrix used by mega_simulator.

    Reads directly from the DuckDB Parquet caches (ob_snapshots, raw_trades, whale_events),
    buckets to 30-second intervals, computes 5-min rolling window features in DuckDB using
    window functions, and returns every row with all 26 feature columns + timestamp + mid_price.

    Also returns:
      - feature_meta: name/group/description for every feature
      - simulation_rules: up to 20 recently saved GA-approved rules from simulation_results
    """
    n_rows = request.args.get('rows', 100, type=int)
    n_rows = max(10, min(500, n_rows))

    # Feature metadata (name, group, description) — mirrors mega_simulator.FEATURES
    FEATURE_META = [
        # Order book: 5-min rolling
        {"name": "ob_avg_vol_imb",      "group": "Order Book (5-min)",      "description": "(bid_vol - ask_vol) / total — positive = buy pressure dominates"},
        {"name": "ob_avg_depth_ratio",  "group": "Order Book (5-min)",      "description": "bid depth / ask depth — >1 means more buy-side liquidity"},
        {"name": "ob_avg_spread_bps",   "group": "Order Book (5-min)",      "description": "bid-ask spread in basis points — lower = tighter, more liquid market"},
        {"name": "ob_net_liq_change",   "group": "Order Book (5-min)",      "description": "sum of net_liq_1s — net liquidity flow; positive = liquidity building on buy side"},
        {"name": "ob_bid_ask_ratio",    "group": "Order Book (5-min)",      "description": "bid_liq / ask_liq — overall liquidity skew across all price levels"},
        # Order book: acceleration
        {"name": "ob_imb_trend",        "group": "Order Book (Acceleration)", "description": "ob_imb_1m - ob_imb_5m — is volume imbalance accelerating vs baseline?"},
        {"name": "ob_depth_trend",      "group": "Order Book (Acceleration)", "description": "ob_depth_1m - ob_depth_5m — is buy-side depth building faster than average?"},
        {"name": "ob_liq_accel",        "group": "Order Book (Acceleration)", "description": "ob_bid_ask_1m - ob_bid_ask_5m — is liquidity skew rising in the last minute?"},
        # Order book: microstructure
        {"name": "ob_slope_ratio",      "group": "Order Book (Microstructure)", "description": "bid_slope / |ask_slope| — how steep is the buy side vs sell side of the book?"},
        {"name": "ob_depth_5bps_ratio", "group": "Order Book (Microstructure)", "description": "bid_dep_5bps / ask_dep_5bps — close-to-mid depth imbalance within 5bps of mid"},
        {"name": "ob_microprice_dev",   "group": "Order Book (Microstructure)", "description": "microprice - mid_price — directional pressure; positive = micro buying pressure"},
        # Trades
        {"name": "tr_buy_ratio",        "group": "Trades",   "description": "buy_vol / total_vol — fraction of volume on buy side; >0.55 = buying dominance"},
        {"name": "tr_large_ratio",      "group": "Trades",   "description": "vol from trades >50 SOL / total — institutional/whale trade activity proxy"},
        {"name": "tr_buy_accel",        "group": "Trades",   "description": "1-min buy ratio / 5-min buy ratio — is buy momentum building? >1 = acceleration"},
        {"name": "tr_avg_size",         "group": "Trades",   "description": "average trade size in SOL — larger = more conviction per execution"},
        {"name": "tr_n",                "group": "Trades",   "description": "total trade count in 5-min window — overall market activity level"},
        # Whale
        {"name": "wh_inflow_ratio",     "group": "Whale Activity", "description": "whale_in_sol / total_whale_sol — net accumulation fraction; >0.6 = whales accumulating"},
        {"name": "wh_net_flow",         "group": "Whale Activity", "description": "whale_in_sol - whale_out_sol — signed net SOL flow from tracked wallets"},
        {"name": "wh_large_count",      "group": "Whale Activity", "description": "events with significance >0.5 (MAJOR/SIGNIFICANT moves) in last 5 min"},
        {"name": "wh_n",                "group": "Whale Activity", "description": "total whale event count in window — how active are tracked wallets?"},
        {"name": "wh_avg_pct_moved",    "group": "Whale Activity", "description": "avg % of each whale's wallet moved — conviction; high = meaningful position change"},
        {"name": "wh_urgency_ratio",    "group": "Whale Activity", "description": "fraction of events moving >50% of wallet — urgent / panic moves"},
        # Price momentum
        {"name": "pm_price_change_30s", "group": "Price Momentum", "description": "price % change in last 30 seconds"},
        {"name": "pm_price_change_1m",  "group": "Price Momentum", "description": "price % change in last 1 minute"},
        {"name": "pm_price_change_5m",  "group": "Price Momentum", "description": "price % change in last 5 minutes"},
        {"name": "pm_velocity_30s",     "group": "Price Momentum", "description": "momentum acceleration: 1m_change - 5m_change; positive = price speeding up short-term"},
    ]

    try:
        from core.raw_data_cache import open_reader
        con = open_reader()

        try:
            # Build 30-second bucketed + 5-min rolling feature matrix entirely in DuckDB.
            # 5-min window = 10 buckets of 30s; 1-min window = 2 buckets of 30s.
            rows_raw = con.execute(f"""
                WITH
                -- ── 30-second OB buckets ─────────────────────────────────────
                ob_b AS (
                    SELECT
                        CAST(EPOCH(ts) / 30 AS BIGINT) * 30            AS bucket_epoch,
                        AVG(vol_imb)                                    AS _vi,
                        AVG(depth_ratio)                                AS _dr,
                        AVG(spread_bps)                                 AS _sp,
                        AVG(net_liq_1s)                                 AS _nl,
                        AVG(bid_liq / NULLIF(ask_liq, 0))               AS _ba,
                        AVG(bid_slope / NULLIF(ABS(ask_slope), 0))      AS _sr,
                        AVG(bid_dep_5bps / NULLIF(ask_dep_5bps, 0))     AS _d5,
                        AVG(microprice_dev)                             AS _mpd,
                        AVG(mid_price)                                  AS mid_price
                    FROM ob_snapshots
                    WHERE ts >= NOW() - INTERVAL '6 hours'
                    GROUP BY 1
                ),
                -- ── 30-second Trade buckets ──────────────────────────────────
                tr_b AS (
                    SELECT
                        CAST(EPOCH(ts) / 30 AS BIGINT) * 30            AS bucket_epoch,
                        COUNT(*)                                        AS tr_n_raw,
                        SUM(sol_amount)                                 AS tr_total,
                        SUM(CASE WHEN direction='buy' THEN sol_amount ELSE 0 END)
                          / NULLIF(SUM(sol_amount), 0)                  AS tr_buy_ratio_raw,
                        SUM(CASE WHEN sol_amount > 50 THEN sol_amount ELSE 0 END)
                          / NULLIF(SUM(sol_amount), 0)                  AS tr_large_ratio_raw,
                        AVG(sol_amount)                                 AS tr_avg_size_raw
                    FROM raw_trades
                    WHERE ts >= NOW() - INTERVAL '6 hours'
                    GROUP BY 1
                ),
                -- ── 30-second Whale buckets ──────────────────────────────────
                wh_b AS (
                    SELECT
                        CAST(EPOCH(ts) / 30 AS BIGINT) * 30            AS bucket_epoch,
                        COUNT(*)                                        AS wh_n_raw,
                        SUM(CASE WHEN direction IN ('in','receiving')  THEN ABS(sol_moved) ELSE 0 END)
                          - SUM(CASE WHEN direction IN ('out','sending') THEN ABS(sol_moved) ELSE 0 END)
                                                                        AS wh_net_flow_raw,
                        SUM(CASE WHEN direction IN ('in','receiving') THEN ABS(sol_moved) ELSE 0 END)
                          / NULLIF(SUM(ABS(sol_moved)), 0)              AS wh_inflow_ratio_raw,
                        COUNT(CASE WHEN significance > 0.5 THEN 1 END) AS wh_large_count_raw,
                        AVG(pct_moved)                                  AS wh_avg_pct_raw,
                        COUNT(CASE WHEN pct_moved > 50 THEN 1 END) * 1.0
                          / NULLIF(COUNT(*), 0)                         AS wh_urgency_ratio_raw
                    FROM whale_events
                    WHERE ts >= NOW() - INTERVAL '6 hours'
                    GROUP BY 1
                ),
                -- ── Join all sources on bucket_epoch ────────────────────────
                joined AS (
                    SELECT
                        TIMESTAMPTZ 'epoch' + ob._vi * 0 + ob.bucket_epoch * INTERVAL '1 second'
                                                                        AS bucket_ts,
                        ob.bucket_epoch,
                        ob.mid_price,
                        ob._vi, ob._dr, ob._sp, ob._nl, ob._ba,
                        ob._sr, ob._d5, ob._mpd,
                        COALESCE(tr.tr_n_raw, 0)                        AS tr_n_raw,
                        COALESCE(tr.tr_buy_ratio_raw, 0.5)              AS tr_buy_ratio_raw,
                        COALESCE(tr.tr_large_ratio_raw, 0)              AS tr_large_ratio_raw,
                        COALESCE(tr.tr_avg_size_raw, 0)                 AS tr_avg_size_raw,
                        COALESCE(wh.wh_n_raw, 0)                        AS wh_n_raw,
                        COALESCE(wh.wh_net_flow_raw, 0)                 AS wh_net_flow_raw,
                        COALESCE(wh.wh_inflow_ratio_raw, 0.5)           AS wh_inflow_ratio_raw,
                        COALESCE(wh.wh_large_count_raw, 0)              AS wh_large_count_raw,
                        COALESCE(wh.wh_avg_pct_raw, 0)                  AS wh_avg_pct_raw,
                        COALESCE(wh.wh_urgency_ratio_raw, 0)            AS wh_urgency_ratio_raw
                    FROM ob_b ob
                    LEFT JOIN tr_b tr USING (bucket_epoch)
                    LEFT JOIN wh_b wh USING (bucket_epoch)
                ),
                -- ── 5-min rolling (10 buckets) + 1-min rolling (2 buckets) ─
                rolled AS (
                    SELECT
                        bucket_ts,
                        mid_price,
                        -- OB 5-min rolling averages
                        AVG(_vi)  OVER w5  AS ob_avg_vol_imb,
                        AVG(_dr)  OVER w5  AS ob_avg_depth_ratio,
                        AVG(_sp)  OVER w5  AS ob_avg_spread_bps,
                        AVG(_nl)  OVER w5  AS ob_net_liq_change,
                        AVG(_ba)  OVER w5  AS ob_bid_ask_ratio,
                        -- OB 1-min rolling
                        AVG(_vi)  OVER w1  AS _ob_vi_1m,
                        AVG(_dr)  OVER w1  AS _ob_dr_1m,
                        AVG(_ba)  OVER w1  AS _ob_ba_1m,
                        -- OB microstructure 5-min
                        AVG(_sr)  OVER w5  AS ob_slope_ratio,
                        AVG(_d5)  OVER w5  AS ob_depth_5bps_ratio,
                        AVG(_mpd) OVER w5  AS ob_microprice_dev,
                        -- Trades 5-min rolling
                        AVG(tr_buy_ratio_raw)   OVER w5  AS tr_buy_ratio,
                        AVG(tr_large_ratio_raw) OVER w5  AS tr_large_ratio,
                        AVG(tr_avg_size_raw)    OVER w5  AS tr_avg_size,
                        SUM(tr_n_raw)           OVER w5  AS tr_n,
                        AVG(tr_buy_ratio_raw)   OVER w1  AS _tr_buy_1m,
                        -- Whale 5-min rolling
                        AVG(wh_inflow_ratio_raw) OVER w5  AS wh_inflow_ratio,
                        SUM(wh_net_flow_raw)     OVER w5  AS wh_net_flow,
                        SUM(wh_large_count_raw)  OVER w5  AS wh_large_count,
                        SUM(wh_n_raw)            OVER w5  AS wh_n,
                        AVG(wh_avg_pct_raw)      OVER w5  AS wh_avg_pct_moved,
                        AVG(wh_urgency_ratio_raw) OVER w5 AS wh_urgency_ratio,
                        -- Price momentum via LAG on mid_price
                        mid_price                                       AS p_now,
                        LAG(mid_price, 1)  OVER (ORDER BY bucket_epoch) AS p_30s_ago,
                        LAG(mid_price, 2)  OVER (ORDER BY bucket_epoch) AS p_1m_ago,
                        LAG(mid_price, 10) OVER (ORDER BY bucket_epoch) AS p_5m_ago
                    FROM joined
                    WINDOW
                        w5 AS (ORDER BY bucket_epoch ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
                        w1 AS (ORDER BY bucket_epoch ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
                )
                SELECT
                    bucket_ts                                            AS timestamp,
                    ROUND(mid_price::DOUBLE, 4)                          AS mid_price,
                    -- All 26 features in canonical order
                    ROUND(ob_avg_vol_imb::DOUBLE, 6)                     AS ob_avg_vol_imb,
                    ROUND(ob_avg_depth_ratio::DOUBLE, 6)                 AS ob_avg_depth_ratio,
                    ROUND(ob_avg_spread_bps::DOUBLE, 4)                  AS ob_avg_spread_bps,
                    ROUND(ob_net_liq_change::DOUBLE, 6)                  AS ob_net_liq_change,
                    ROUND(ob_bid_ask_ratio::DOUBLE, 6)                   AS ob_bid_ask_ratio,
                    ROUND((_ob_vi_1m - ob_avg_vol_imb)::DOUBLE, 6)      AS ob_imb_trend,
                    ROUND((_ob_dr_1m - ob_avg_depth_ratio)::DOUBLE, 6)  AS ob_depth_trend,
                    ROUND((_ob_ba_1m - ob_bid_ask_ratio)::DOUBLE, 6)    AS ob_liq_accel,
                    ROUND(ob_slope_ratio::DOUBLE, 6)                     AS ob_slope_ratio,
                    ROUND(ob_depth_5bps_ratio::DOUBLE, 6)                AS ob_depth_5bps_ratio,
                    ROUND(ob_microprice_dev::DOUBLE, 6)                  AS ob_microprice_dev,
                    ROUND(tr_buy_ratio::DOUBLE, 6)                       AS tr_buy_ratio,
                    ROUND(tr_large_ratio::DOUBLE, 6)                     AS tr_large_ratio,
                    ROUND(CASE WHEN AVG(tr_buy_ratio) OVER (ORDER BY bucket_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) > 0
                               THEN _tr_buy_1m / AVG(tr_buy_ratio) OVER (ORDER BY bucket_ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
                               ELSE 1.0 END::DOUBLE, 6)                  AS tr_buy_accel,
                    ROUND(tr_avg_size::DOUBLE, 4)                        AS tr_avg_size,
                    CAST(tr_n AS BIGINT)                                  AS tr_n,
                    ROUND(wh_inflow_ratio::DOUBLE, 6)                    AS wh_inflow_ratio,
                    ROUND(wh_net_flow::DOUBLE, 4)                        AS wh_net_flow,
                    CAST(wh_large_count AS BIGINT)                       AS wh_large_count,
                    CAST(wh_n AS BIGINT)                                  AS wh_n,
                    ROUND(wh_avg_pct_moved::DOUBLE, 4)                   AS wh_avg_pct_moved,
                    ROUND(wh_urgency_ratio::DOUBLE, 6)                   AS wh_urgency_ratio,
                    ROUND(CASE WHEN p_30s_ago > 0 THEN (p_now - p_30s_ago) / p_30s_ago * 100 ELSE 0 END::DOUBLE, 6) AS pm_price_change_30s,
                    ROUND(CASE WHEN p_1m_ago  > 0 THEN (p_now - p_1m_ago)  / p_1m_ago  * 100 ELSE 0 END::DOUBLE, 6) AS pm_price_change_1m,
                    ROUND(CASE WHEN p_5m_ago  > 0 THEN (p_now - p_5m_ago)  / p_5m_ago  * 100 ELSE 0 END::DOUBLE, 6) AS pm_price_change_5m,
                    ROUND(CASE WHEN p_1m_ago > 0 AND p_5m_ago > 0
                               THEN (p_now - p_1m_ago) / p_1m_ago * 100 - (p_now - p_5m_ago) / p_5m_ago * 100
                               ELSE 0 END::DOUBLE, 6)                    AS pm_velocity_30s
                FROM rolled
                WHERE p_5m_ago IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT {n_rows}
            """).fetchall()

            col_names = [
                'timestamp', 'mid_price',
                'ob_avg_vol_imb', 'ob_avg_depth_ratio', 'ob_avg_spread_bps',
                'ob_net_liq_change', 'ob_bid_ask_ratio',
                'ob_imb_trend', 'ob_depth_trend', 'ob_liq_accel',
                'ob_slope_ratio', 'ob_depth_5bps_ratio', 'ob_microprice_dev',
                'tr_buy_ratio', 'tr_large_ratio', 'tr_buy_accel', 'tr_avg_size', 'tr_n',
                'wh_inflow_ratio', 'wh_net_flow', 'wh_large_count', 'wh_n',
                'wh_avg_pct_moved', 'wh_urgency_ratio',
                'pm_price_change_30s', 'pm_price_change_1m',
                'pm_price_change_5m', 'pm_velocity_30s',
            ]
            rows_out = []
            for r in rows_raw:
                d = dict(zip(col_names, r))
                # Serialise timestamp
                ts_val = d.get('timestamp')
                if hasattr(ts_val, 'isoformat'):
                    d['timestamp'] = ts_val.isoformat()
                elif ts_val is not None:
                    d['timestamp'] = str(ts_val)
                rows_out.append(d)

        finally:
            con.close()

        # Pull up to 20 approved GA rules from simulation_results for context
        sim_rules = []
        try:
            with get_postgres() as pg:
                with pg.cursor() as cur:
                    cur.execute("""
                        SELECT run_id, rank, conditions_json, win_rate,
                               signals_per_day, oos_ev, oos_consistency, data_hours
                        FROM simulation_results
                        WHERE win_rate >= 0.55
                        ORDER BY created_at DESC
                        LIMIT 20
                    """)
                    for r in cur.fetchall():
                        sim_rules.append({
                            'run_id':           r['run_id'],
                            'rank':             r['rank'],
                            'conditions':       r['conditions_json'],
                            'win_rate_pct':     round(float(r['win_rate']) * 100, 1) if r['win_rate'] else None,
                            'signals_per_day':  round(float(r['signals_per_day']), 1) if r['signals_per_day'] else None,
                            'oos_precision_pct':round(float(r['oos_ev']) * 100, 1) if r.get('oos_ev') else None,
                            'oos_consistency_pct': round(float(r['oos_consistency']) * 100, 0) if r.get('oos_consistency') else None,
                            'data_hours':       round(float(r['data_hours']), 1) if r.get('data_hours') else None,
                        })
        except Exception as rule_err:
            logger.warning(f"feature_export: simulation_results query failed: {rule_err}")

        return jsonify({
            'rows':          rows_out,
            'n_rows':        len(rows_out),
            'feature_meta':  FEATURE_META,
            'simulation_rules': sim_rules,
            'bucket_seconds': 30,
            'window_seconds': 300,
            'note': (
                'Each row is one 30-second bucket. All features are computed as '
                '5-minute rolling windows over the preceding 10 buckets '
                '(acceleration features use 1-min = 2 buckets). '
                'Rows are ordered newest-first. '
                'simulation_rules are the most recent GA-approved rules '
                '(win_rate >= 55%) from the mega_simulator.'
            ),
        })

    except Exception as e:
        logger.error(f"feature_export failed: {e}", exc_info=True)
        return jsonify({'error': str(e), 'rows': [], 'n_rows': 0}), 500


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
    """Get a single play by ID with filter information."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM follow_the_goat_plays
                    WHERE id = %s
                """, [play_id])
                
                result = cursor.fetchone()
        
        if result:
            # Get filter information for projects used by this play
            play = dict(result)
            project_ids = []
            if play.get('project_ids'):
                try:
                    if isinstance(play['project_ids'], str):
                        project_ids = json.loads(play['project_ids'])
                    elif isinstance(play['project_ids'], list):
                        project_ids = play['project_ids']
                except:
                    project_ids = []
            
            # Get filters for each project with detailed filter information
            filters_info = []
            if project_ids:
                with get_postgres() as conn:
                    with conn.cursor() as cursor:
                        for project_id in project_ids:
                            # Get project info
                            cursor.execute("""
                                SELECT 
                                    p.id as project_id,
                                    p.name as project_name,
                                    p.updated_at as project_updated_at
                                FROM pattern_config_projects p
                                WHERE p.id = %s
                            """, [project_id])
                            project_info = cursor.fetchone()
                            
                            if project_info:
                                # Get all active filters for this project
                                cursor.execute("""
                                    SELECT 
                                        f.id,
                                        f.name,
                                        f.section,
                                        f.minute,
                                        f.field_name,
                                        f.field_column,
                                        f.from_value,
                                        f.to_value,
                                        f.include_null,
                                        f.is_active,
                                        f.updated_at
                                    FROM pattern_config_filters f
                                    WHERE f.project_id = %s AND f.is_active = 1
                                    ORDER BY f.minute, f.id
                                """, [project_id])
                                filters = cursor.fetchall()
                                
                                filters_info.append({
                                    'project_id': project_info['project_id'],
                                    'project_name': project_info['project_name'],
                                    'project_updated_at': project_info['project_updated_at'].isoformat() if project_info['project_updated_at'] else None,
                                    'filter_count': len(filters),
                                    'latest_filter_update': max([f['updated_at'].isoformat() for f in filters if f.get('updated_at')], default=None),
                                    'filters': [
                                        {
                                            'id': f['id'],
                                            'name': f['name'],
                                            'section': f['section'],
                                            'minute': f['minute'],
                                            'field_name': f['field_name'],
                                            'field_column': f['field_column'],
                                            'from_value': float(f['from_value']) if f.get('from_value') is not None else None,
                                            'to_value': float(f['to_value']) if f.get('to_value') is not None else None,
                                            'include_null': bool(f.get('include_null', 0)),
                                            'updated_at': f['updated_at'].isoformat() if f.get('updated_at') else None
                                        }
                                        for f in filters
                                    ]
                                })
            
            play['filters_info'] = filters_info
            return jsonify({'play': play})
        else:
            return jsonify({'error': 'Play not found'}), 404
    except Exception as e:
        logger.error(f"Get single play failed: {e}", exc_info=True)
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
        exclude_status = request.args.get('exclude_status')  # New parameter for excluding statuses
        play_id = request.args.get('play_id')
        hours = request.args.get('hours')
        
        # Build WHERE clause conditions
        where_conditions = []
        params = []
        
        if status_filter:
            where_conditions.append("our_status = %s")
            params.append(status_filter)
        
        # Support excluding multiple statuses (comma-separated)
        if exclude_status:
            excluded_statuses = [s.strip() for s in exclude_status.split(',') if s.strip()]
            if excluded_statuses:
                placeholders = ', '.join(['%s'] * len(excluded_statuses))
                where_conditions.append(f"our_status NOT IN ({placeholders})")
                params.extend(excluded_statuses)
        
        if play_id:
            where_conditions.append("play_id = %s")
            params.append(int(play_id))
        
        if hours and hours != 'all':
            try:
                hours_int = int(hours)
                # Use string formatting for INTERVAL, not parameterized query
                where_conditions.append(f"followed_at >= NOW() - INTERVAL '{hours_int} hours'")
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
    """Delete all no_go and error trades older than 24 hours from PostgreSQL."""
    try:
        deleted_count = 0
        
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Count before delete (both no_go and error status)
                cursor.execute("""
                    SELECT COUNT(*) as count FROM follow_the_goat_buyins 
                    WHERE our_status IN ('no_go', 'error')
                    AND followed_at < NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                deleted_count = result['count'] if result else 0
                
                if deleted_count > 0:
                    # Delete from PostgreSQL (both no_go and error status)
                    cursor.execute("""
                        DELETE FROM follow_the_goat_buyins 
                        WHERE our_status IN ('no_go', 'error')
                        AND followed_at < NOW() - INTERVAL '24 hours'
                    """)
            conn.commit()
        
        return jsonify({
            'success': True,
            'deleted': deleted_count,
            'message': f'Deleted {deleted_count} no_go/error trades older than 24 hours'
        })
    except Exception as e:
        logger.error(f"Cleanup no-gos failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/wallets', methods=['GET'])
def get_wallets():
    """Get all paper wallets with balance and P/L summary."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, balance, initial_balance, is_test, play_ids, fee_rate,
                           invest_pct, created_at, updated_at
                    FROM wallets
                    ORDER BY id
                """)
                wallets_raw = cursor.fetchall()

        wallets = []
        for w in wallets_raw:
            wallet_id = w['id']

            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            COALESCE(SUM(profit_loss_usdc), 0) AS total_pl_usdc,
                            COUNT(*) FILTER (WHERE status = 'closed') AS closed_trades,
                            COUNT(*) FILTER (WHERE status = 'open') AS open_trades,
                            COUNT(*) FILTER (WHERE status IN ('cancelled', 'missed')) AS cancelled_trades,
                            COUNT(*) FILTER (WHERE status = 'closed' AND profit_loss_usdc > 0) AS winning_trades,
                            COUNT(*) FILTER (WHERE status = 'closed' AND profit_loss_usdc <= 0) AS losing_trades
                        FROM wallet_trades
                        WHERE wallet_id = %s
                    """, [wallet_id])
                    stats = cursor.fetchone()

            play_ids = w['play_ids']
            if isinstance(play_ids, str):
                import json as _json
                try:
                    play_ids = _json.loads(play_ids)
                except Exception:
                    play_ids = []

            initial = float(w['initial_balance'])
            balance = float(w['balance'])
            total_pl = float(stats['total_pl_usdc']) if stats else 0.0
            total_pl_pct = round((total_pl / initial) * 100, 2) if initial > 0 else 0.0

            wallets.append({
                'id': wallet_id,
                'name': w['name'],
                'balance': balance,
                'initial_balance': initial,
                'is_test': bool(w['is_test']),
                'play_ids': play_ids,
                'fee_rate': float(w['fee_rate']),
                'invest_pct': float(w['invest_pct']) if w.get('invest_pct') is not None else 0.20,
                'total_pl_usdc': round(total_pl, 4),
                'total_pl_pct': total_pl_pct,
                'closed_trades': int(stats['closed_trades'] or 0) if stats else 0,
                'open_trades': int(stats['open_trades'] or 0) if stats else 0,
                'cancelled_trades': int(stats['cancelled_trades'] or 0) if stats else 0,
                'winning_trades': int(stats['winning_trades'] or 0) if stats else 0,
                'losing_trades': int(stats['losing_trades'] or 0) if stats else 0,
                'updated_at': w['updated_at'].isoformat() if w.get('updated_at') else None,
            })

        return jsonify({'success': True, 'wallets': wallets})
    except Exception as e:
        logger.error(f"Get wallets failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/wallets/<int:wallet_id>/trades', methods=['GET'])
def get_wallet_trades(wallet_id):
    """Get trade history for a specific wallet."""
    try:
        limit = min(int(request.args.get('limit', 100)), 1000)
        status_filter = request.args.get('status', None)

        query_params = [wallet_id]
        status_clause = ""
        if status_filter:
            status_clause = "AND wt.status = %s"
            query_params.append(status_filter)

        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT wt.id, wt.wallet_id, wt.buyin_id, wt.play_id, wt.status,
                           wt.entry_price, wt.position_usdc, wt.sol_amount, wt.buy_fee_usdc,
                           wt.exit_price, wt.sell_fee_usdc, wt.profit_loss_usdc,
                           wt.profit_loss_pct, wt.closed_at, wt.created_at
                    FROM wallet_trades wt
                    WHERE wt.wallet_id = %s {status_clause}
                    ORDER BY wt.id DESC
                    LIMIT %s
                """, query_params + [limit])
                rows = cursor.fetchall()

        trades = []
        for r in rows:
            trades.append({
                'id': r['id'],
                'wallet_id': r['wallet_id'],
                'buyin_id': r['buyin_id'],
                'play_id': r['play_id'],
                'status': r['status'],
                'entry_price': float(r['entry_price']) if r.get('entry_price') else None,
                'position_usdc': float(r['position_usdc']) if r.get('position_usdc') else None,
                'sol_amount': float(r['sol_amount']) if r.get('sol_amount') else None,
                'buy_fee_usdc': float(r['buy_fee_usdc']) if r.get('buy_fee_usdc') else None,
                'exit_price': float(r['exit_price']) if r.get('exit_price') else None,
                'sell_fee_usdc': float(r['sell_fee_usdc']) if r.get('sell_fee_usdc') else None,
                'profit_loss_usdc': float(r['profit_loss_usdc']) if r.get('profit_loss_usdc') else None,
                'profit_loss_pct': float(r['profit_loss_pct']) if r.get('profit_loss_pct') else None,
                'closed_at': r['closed_at'].isoformat() if r.get('closed_at') else None,
                'created_at': r['created_at'].isoformat() if r.get('created_at') else None,
            })

        return jsonify({'success': True, 'trades': trades, 'count': len(trades)})
    except Exception as e:
        logger.error(f"Get wallet trades failed: {e}", exc_info=True)
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
# SCHEDULER STATUS (POSTGRESQL)
# =============================================================================

@app.route('/scheduler_status', methods=['GET'])
def get_scheduler_status():
    """
    Backward-compatible scheduler status endpoint.
    
    Returns a shape similar to the historical JSON export, but the source of truth
    is PostgreSQL heartbeats (works across multiple independent services).
    """
    try:
        from scheduler.component_registry import ensure_default_components_registered
        ensure_default_components_registered()

        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    WITH latest_hb AS (
                        SELECT DISTINCT ON (component_id)
                            component_id, instance_id, host, pid, started_at,
                            last_heartbeat_at, status, last_error_at, last_error_message
                        FROM scheduler_component_heartbeats
                        ORDER BY component_id, last_heartbeat_at DESC
                    )
                    SELECT
                        c.component_id,
                        c.description,
                        c.kind,
                        c.group_name,
                        c.expected_interval_ms,
                        COALESCE(s.enabled, c.default_enabled) AS enabled,
                        hb.instance_id,
                        hb.host,
                        hb.pid,
                        hb.started_at,
                        hb.last_heartbeat_at,
                        hb.status,
                        hb.last_error_at,
                        hb.last_error_message
                    FROM scheduler_components c
                    LEFT JOIN scheduler_component_settings s ON s.component_id = c.component_id
                    LEFT JOIN latest_hb hb ON hb.component_id = c.component_id
                    ORDER BY c.group_name, c.kind, c.component_id
                    """
                )
                rows = cursor.fetchall()

        jobs = {}
        for r in rows or []:
            jobs[r["component_id"]] = {
                "job_id": r["component_id"],
                "description": r.get("description"),
                "kind": r.get("kind"),
                "group_name": r.get("group_name"),
                "enabled": bool(r.get("enabled")),
                "expected_interval_ms": r.get("expected_interval_ms"),
                "status": r.get("status") or "unavailable",
                "instance_id": r.get("instance_id"),
                "host": r.get("host"),
                "pid": r.get("pid"),
                "started_at": r["started_at"].isoformat() if r.get("started_at") else None,
                "last_heartbeat_at": r["last_heartbeat_at"].isoformat() if r.get("last_heartbeat_at") else None,
                "last_error_at": r["last_error_at"].isoformat() if r.get("last_error_at") else None,
                "last_error_message": r.get("last_error_message"),
            }

        return jsonify({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "jobs": jobs,
        })
    except Exception as e:
        logger.error(f"Get scheduler status failed: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/scheduler/components', methods=['GET'])
def scheduler_components():
    """
    Canonical scheduler components endpoint for the dashboard.
    Computes health using heartbeat freshness + enabled flag.
    """
    try:
        from scheduler.component_registry import ensure_default_components_registered
        ensure_default_components_registered()

        # scheduler_component_heartbeats uses TIMESTAMP WITHOUT TIME ZONE (naive UTC).
        # Strip tzinfo so subtraction works without TypeError.
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Latest heartbeat per component + last run per job_id (from job_execution_metrics)
                cursor.execute(
                    """
                    WITH latest_hb AS (
                        SELECT DISTINCT ON (component_id)
                            component_id, instance_id, host, pid, started_at,
                            last_heartbeat_at, status, last_error_at, last_error_message
                        FROM scheduler_component_heartbeats
                        ORDER BY component_id, last_heartbeat_at DESC
                    ),
                    last_run AS (
                        SELECT job_id AS component_id, MAX(started_at) AS last_run_at
                        FROM job_execution_metrics
                        GROUP BY job_id
                    )
                    SELECT
                        c.component_id,
                        c.kind,
                        c.group_name,
                        c.description,
                        c.expected_interval_ms,
                        c.default_enabled,
                        COALESCE(s.enabled, c.default_enabled) AS enabled,
                        hb.instance_id,
                        hb.host,
                        hb.pid,
                        hb.started_at,
                        hb.last_heartbeat_at,
                        hb.status AS hb_status,
                        hb.last_error_at,
                        hb.last_error_message,
                        lr.last_run_at
                    FROM scheduler_components c
                    LEFT JOIN scheduler_component_settings s ON s.component_id = c.component_id
                    LEFT JOIN latest_hb hb ON hb.component_id = c.component_id
                    LEFT JOIN last_run lr ON lr.component_id = c.component_id
                    ORDER BY c.group_name, c.kind, c.component_id
                    """
                )
                rows = cursor.fetchall()

        components = []
        for r in rows or []:
            enabled = bool(r.get("enabled"))
            hb_at = r.get("last_heartbeat_at")
            if hb_at is not None and getattr(hb_at, "tzinfo", None) is not None:
                hb_at = hb_at.replace(tzinfo=None)
            hb_status = r.get("hb_status") or "unavailable"

            timeout_s = 30.0
            expected_ms = r.get("expected_interval_ms")
            if expected_ms:
                timeout_s = max(timeout_s, (float(expected_ms) / 1000.0) * 3.0)

            healthy = False
            health_reason = "unavailable"
            if not enabled:
                healthy = False
                health_reason = "disabled"
            elif hb_at is None:
                healthy = False
                health_reason = "no_heartbeat"
            else:
                age_s = (now - hb_at).total_seconds()
                if age_s > timeout_s:
                    healthy = False
                    health_reason = f"stale_heartbeat_{age_s:.0f}s"
                elif hb_status in ("running", "idle"):
                    healthy = True
                    health_reason = "ok"
                else:
                    healthy = False
                    health_reason = hb_status

            components.append({
                "component_id": r["component_id"],
                "kind": r.get("kind"),
                "group_name": r.get("group_name"),
                "description": r.get("description"),
                "expected_interval_ms": r.get("expected_interval_ms"),
                "default_enabled": bool(r.get("default_enabled")),
                "enabled": enabled,
                "healthy": healthy,
                "health_reason": health_reason,
                "last_run_at": r["last_run_at"].isoformat() if r.get("last_run_at") else None,
                "heartbeat": {
                    "instance_id": r.get("instance_id"),
                    "host": r.get("host"),
                    "pid": r.get("pid"),
                    "started_at": r["started_at"].isoformat() if r.get("started_at") else None,
                    "last_heartbeat_at": r["last_heartbeat_at"].isoformat() if r.get("last_heartbeat_at") else None,
                    "status": hb_status,
                    "last_error_at": r["last_error_at"].isoformat() if r.get("last_error_at") else None,
                    "last_error_message": r.get("last_error_message"),
                }
            })

        return jsonify({
            "status": "ok",
            "timestamp": now.replace(tzinfo=timezone.utc).isoformat(),
            "components": components,
        })
    except Exception as e:
        # If tables aren't migrated yet, still return the full component list so the UI
        # can show everything as "off / not running" instead of showing nothing.
        logger.error(f"Get scheduler components failed: {e}", exc_info=True)
        try:
            from scheduler.component_registry import DEFAULT_COMPONENT_DEFS
            fallback = []
            for c in DEFAULT_COMPONENT_DEFS:
                fallback.append({
                    "component_id": c.component_id,
                    "kind": c.kind,
                    "group_name": c.group_name,
                    "description": c.description,
                    "expected_interval_ms": c.expected_interval_ms,
                    "default_enabled": c.default_enabled,
                    "enabled": c.default_enabled,
                    "healthy": False,
                    "health_reason": "db_unavailable",
                    "last_run_at": None,
                    "heartbeat": {
                        "instance_id": None,
                        "host": None,
                        "pid": None,
                        "started_at": None,
                        "last_heartbeat_at": None,
                        "status": "unavailable",
                        "last_error_at": None,
                        "last_error_message": None,
                    },
                })
            return jsonify({
                "status": "degraded",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "components": fallback,
                "warning": "scheduler tables not available; showing static component list",
                "error": str(e),
            }), 200
        except Exception:
            return jsonify({"status": "error", "error": str(e)}), 500


@app.route('/scheduler/components/<component_id>', methods=['PUT'])
def update_scheduler_component(component_id):
    """Enable/disable a scheduler component."""
    try:
        payload = request.get_json(silent=True) or {}
        enabled = payload.get("enabled")
        note = payload.get("note")

        if enabled is None or not isinstance(enabled, bool):
            return jsonify({"success": False, "error": "Missing boolean field: enabled"}), 400

        from scheduler.control import set_component_enabled
        ok = set_component_enabled(
            component_id=component_id,
            enabled=enabled,
            updated_by=request.remote_addr,
            note=note,
        )
        if not ok:
            return jsonify({"success": False, "error": "Unknown component_id"}), 404
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Update scheduler component failed: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/scheduler/errors', methods=['GET'])
def scheduler_errors():
    """Return recent scheduler error events (optionally filtered by component_id)."""
    try:
        component_id = request.args.get("component_id")
        hours = float(request.args.get("hours", 24.0))
        hours = max(0.01, min(168.0, hours))  # 1 minute to 7 days
        limit = min(int(request.args.get("limit", 200)), 1000)

        hours_str = str(hours)

        with get_postgres() as conn:
            with conn.cursor() as cursor:
                if component_id:
                    cursor.execute(
                        """
                        SELECT id, component_id, occurred_at, host, pid, instance_id, message, traceback, context
                        FROM scheduler_error_events
                        WHERE component_id = %s
                          AND occurred_at >= NOW() - (%s || ' hours')::interval
                        ORDER BY occurred_at DESC
                        LIMIT %s
                        """,
                        [component_id, hours_str, limit],
                    )
                else:
                    cursor.execute(
                        """
                        SELECT id, component_id, occurred_at, host, pid, instance_id, message, traceback, context
                        FROM scheduler_error_events
                        WHERE occurred_at >= NOW() - (%s || ' hours')::interval
                        ORDER BY occurred_at DESC
                        LIMIT %s
                        """,
                        [hours_str, limit],
                    )
                rows = cursor.fetchall()

        events = []
        for r in rows or []:
            events.append({
                "id": r.get("id"),
                "component_id": r.get("component_id"),
                "occurred_at": r["occurred_at"].isoformat() if r.get("occurred_at") else None,
                "host": r.get("host"),
                "pid": r.get("pid"),
                "instance_id": r.get("instance_id"),
                "message": r.get("message"),
                "traceback": r.get("traceback"),
                "context": r.get("context"),
            })

        return jsonify({
            "status": "ok",
            "hours": hours,
            "count": len(events),
            "events": events,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        logger.error(f"Get scheduler errors failed: {e}", exc_info=True)
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route('/job_metrics', methods=['GET'])
def get_job_metrics():
    """
    Get job execution metrics with execution time analysis.
    
    Query parameters:
    - hours: Number of hours of history to analyze (default: 1, max: 24)
    
    Returns per-job statistics including:
    - avg_duration_ms, max_duration_ms, min_duration_ms
    - execution_count, error_count
    - recent_executions (last 20)
    - is_slow flag (avg > 80% of expected interval)
    """
    try:
        hours = float(request.args.get('hours', 1.0))
        hours = max(0.01, min(24.0, hours))  # Clamp to 0.01-24 hours
        
        from scheduler.status import get_job_metrics as fetch_metrics
        metrics = fetch_metrics(hours=hours)
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Get job metrics failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e),
            'message': 'Failed to fetch job metrics'
        }), 500


@app.route('/job_metrics_debug', methods=['GET'])
def get_job_metrics_debug():
    """Debug endpoint to check metrics table status."""
    try:
        from scheduler.status import _metrics_table_initialized, _metrics_writer_running
        
        result = {
            'metrics_table_initialized': _metrics_table_initialized,
            'metrics_writer_running': _metrics_writer_running,
        }
        
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    # Check if table exists
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM information_schema.tables 
                        WHERE table_name = 'job_execution_metrics'
                    """)
                    count_result = cursor.fetchone()
                    result['table_exists'] = (count_result['count'] if count_result else 0) > 0
                    
                    if result['table_exists']:
                        cursor.execute("SELECT COUNT(*) as count FROM job_execution_metrics")
                        row_count_result = cursor.fetchone()
                        result['row_count'] = row_count_result['count'] if row_count_result else 0
                        
                        # Get sample rows
                        cursor.execute("""
                            SELECT job_id, status, duration_ms, started_at 
                            FROM job_execution_metrics 
                            ORDER BY started_at DESC LIMIT 5
                        """)
                        rows = cursor.fetchall()
                        result['sample_rows'] = [
                            {
                                'job_id': r['job_id'], 
                                'status': r['status'], 
                                'duration_ms': r['duration_ms'], 
                                'started_at': r['started_at'].isoformat() if r['started_at'] else None
                            }
                            for r in rows
                        ]
        except Exception as db_error:
            result['db_error'] = str(db_error)
            import traceback
            result['traceback'] = traceback.format_exc()
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Get job metrics debug failed: {e}")
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
    """Get 30-second interval trail data for a specific buyin (30 rows)."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM buyin_trail_minutes
                    WHERE buyin_id = %s
                    ORDER BY minute ASC, sub_minute ASC
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
        
        # Load filter settings from PostgreSQL (same source as POST endpoint)
        import json
        
        # Default values
        defaults = {
            'good_trade_threshold': '0.3',
            'analysis_hours': '24',
            'min_filters_in_combo': '2',
            'max_filters_in_combo': '6',
            'min_good_trades_kept_pct': '50',
            'min_bad_trades_removed_pct': '10',
            'combo_min_good_kept_pct': '25',
            'combo_min_improvement': '1.0',
            'auto_project_name': 'AutoFilters',
            'percentile_low': '10',
            'percentile_high': '90',
            'is_ratio': 'false',
            'skip_columns': '[]',
            'section_prefixes': '{}'
        }
        
        # Load from PostgreSQL
        config = defaults.copy()
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT setting_key, setting_value FROM auto_filter_settings")
                    rows = cursor.fetchall()
                    for row in rows:
                        config[row['setting_key']] = row['setting_value'] or defaults.get(row['setting_key'], '')
        except Exception as e:
            logger.warning(f"Could not load settings from PostgreSQL in dashboard endpoint, using defaults: {e}")
        
        # Format settings for frontend (matching GET /filter-analysis/settings format)
        result['settings'] = [
            {
                'setting_key': 'good_trade_threshold',
                'setting_value': config.get('good_trade_threshold', '0.3'),
                'description': 'Good trade threshold percentage',
                'setting_type': 'decimal',
                'min_value': 0.1,
                'max_value': 5.0
            },
            {
                'setting_key': 'analysis_hours',
                'setting_value': config.get('analysis_hours', '24'),
                'description': 'Analysis window in hours',
                'setting_type': 'integer',
                'min_value': 1,
                'max_value': 168
            },
            {
                'setting_key': 'min_filters_in_combo',
                'setting_value': config.get('min_filters_in_combo', '2'),
                'description': 'Minimum filters in combination',
                'setting_type': 'integer',
                'min_value': 1,
                'max_value': 10
            },
            {
                'setting_key': 'min_good_trades_kept_pct',
                'setting_value': config.get('min_good_trades_kept_pct', '50'),
                'description': 'Minimum good trades kept percentage',
                'setting_type': 'integer',
                'min_value': 0,
                'max_value': 100
            },
            {
                'setting_key': 'min_bad_trades_removed_pct',
                'setting_value': config.get('min_bad_trades_removed_pct', '10'),
                'description': 'Minimum bad trades removed percentage',
                'setting_type': 'integer',
                'min_value': 0,
                'max_value': 100
            },
            {
                'setting_key': 'is_ratio',
                'setting_value': config.get('is_ratio', 'false'),
                'description': 'Use ratio-only filters',
                'setting_type': 'boolean',
                'min_value': None,
                'max_value': None
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
                        apu.pattern_count, apu.filters_applied, apu.updated_at, apu.run_id, apu.status,
                        p.updated_at as project_updated_at,
                        MAX(f.updated_at) as filter_version
                    FROM ai_play_updates apu
                    LEFT JOIN pattern_config_projects p ON p.id = apu.project_id
                    LEFT JOIN pattern_config_filters f ON f.project_id = p.id AND f.is_active = 1
                    GROUP BY apu.id, apu.play_id, apu.play_name, apu.project_id, apu.project_name,
                             apu.pattern_count, apu.filters_applied, apu.updated_at, apu.run_id, apu.status,
                             p.updated_at
                    ORDER BY apu.updated_at DESC
                    LIMIT 50
                """)
                play_updates = cursor.fetchall()
                for pu in play_updates:
                    if pu.get('updated_at'):
                        pu['updated_at'] = str(pu['updated_at'])[:19]
                    if pu.get('project_updated_at'):
                        pu['project_updated_at'] = str(pu['project_updated_at'])[:19]
                    if pu.get('filter_version'):
                        pu['filter_version'] = str(pu['filter_version'])[:19]
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
    """Get auto filter settings from PostgreSQL table."""
    import json
    
    try:
        # Default values
        defaults = {
            'good_trade_threshold': '0.3',
            'analysis_hours': '24',
            'min_filters_in_combo': '2',
            'max_filters_in_combo': '6',
            'min_good_trades_kept_pct': '50',
            'min_bad_trades_removed_pct': '10',
            'combo_min_good_kept_pct': '25',
            'combo_min_improvement': '1.0',
            'auto_project_name': 'AutoFilters',
            'percentile_low': '10',
            'percentile_high': '90',
            'is_ratio': 'false',
            'skip_columns': '[]',
            'section_prefixes': '{}'
        }
        
        # Load from PostgreSQL
        config = defaults.copy()
        try:
            with get_postgres() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT setting_key, setting_value FROM auto_filter_settings")
                    rows = cursor.fetchall()
                    for row in rows:
                        config[row['setting_key']] = row['setting_value'] or defaults.get(row['setting_key'], '')
        except Exception as e:
            logger.warning(f"Could not load settings from PostgreSQL, using defaults: {e}")
        
        # Parse JSON fields
        try:
            skip_columns = json.loads(config.get('skip_columns', '[]'))
        except:
            skip_columns = []
        
        try:
            section_prefixes = json.loads(config.get('section_prefixes', '{}'))
        except:
            section_prefixes = {}
        
        return jsonify({
            'success': True,
            'settings': [
                {
                    'setting_key': 'good_trade_threshold',
                    'setting_value': config.get('good_trade_threshold', '0.3'),
                    'description': 'Good trade threshold percentage',
                    'setting_type': 'decimal',
                    'min_value': 0.1,
                    'max_value': 5.0
                },
                {
                    'setting_key': 'analysis_hours',
                    'setting_value': config.get('analysis_hours', '24'),
                    'description': 'Analysis window in hours',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 168
                },
                {
                    'setting_key': 'min_filters_in_combo',
                    'setting_value': config.get('min_filters_in_combo', '2'),
                    'description': 'Minimum filters in combination',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 10
                },
                {
                    'setting_key': 'max_filters_in_combo',
                    'setting_value': config.get('max_filters_in_combo', '6'),
                    'description': 'Maximum filters in combination',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 20
                },
                {
                    'setting_key': 'min_good_trades_kept_pct',
                    'setting_value': config.get('min_good_trades_kept_pct', '50'),
                    'description': 'Minimum good trades kept percentage',
                    'setting_type': 'integer',
                    'min_value': 0,
                    'max_value': 100
                },
                {
                    'setting_key': 'min_bad_trades_removed_pct',
                    'setting_value': config.get('min_bad_trades_removed_pct', '10'),
                    'description': 'Minimum bad trades removed percentage',
                    'setting_type': 'integer',
                    'min_value': 0,
                    'max_value': 100
                },
                {
                    'setting_key': 'combo_min_good_kept_pct',
                    'setting_value': config.get('combo_min_good_kept_pct', '25'),
                    'description': 'Minimum good trades kept for combinations',
                    'setting_type': 'integer',
                    'min_value': 0,
                    'max_value': 100
                },
                {
                    'setting_key': 'combo_min_improvement',
                    'setting_value': config.get('combo_min_improvement', '1.0'),
                    'description': 'Minimum improvement over single filter',
                    'setting_type': 'decimal',
                    'min_value': 0.1,
                    'max_value': 50.0
                },
                {
                    'setting_key': 'auto_project_name',
                    'setting_value': config.get('auto_project_name', 'AutoFilters'),
                    'description': 'Name of auto-generated project',
                    'setting_type': 'string',
                    'min_value': None,
                    'max_value': None
                },
                {
                    'setting_key': 'percentile_low',
                    'setting_value': config.get('percentile_low', '10'),
                    'description': 'Low percentile for threshold calculation',
                    'setting_type': 'integer',
                    'min_value': 1,
                    'max_value': 50
                },
                {
                    'setting_key': 'percentile_high',
                    'setting_value': config.get('percentile_high', '90'),
                    'description': 'High percentile for threshold calculation',
                    'setting_type': 'integer',
                    'min_value': 50,
                    'max_value': 99
                },
                {
                    'setting_key': 'is_ratio',
                    'setting_value': config.get('is_ratio', 'false'),
                    'description': 'Use ratio-only filters',
                    'setting_type': 'boolean',
                    'min_value': None,
                    'max_value': None
                }
            ]
        })
    except Exception as e:
        logger.error(f"Get filter settings failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/filter-analysis/settings', methods=['POST'])
def save_filter_settings():
    """Save auto filter settings to PostgreSQL table."""
    import json
    
    try:
        data = request.get_json() or {}
        settings = data.get('settings', {})
        
        # Ensure table exists
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS auto_filter_settings (
                        id SERIAL PRIMARY KEY,
                        setting_key VARCHAR(100) UNIQUE NOT NULL,
                        setting_value TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_auto_filter_settings_key 
                    ON auto_filter_settings(setting_key)
                """)
                conn.commit()
        
        # Save each setting to PostgreSQL
        saved_settings = {}
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Map of setting keys to their values
                setting_map = {
                    'good_trade_threshold': settings.get('good_trade_threshold'),
                    'analysis_hours': settings.get('analysis_hours'),
                    'min_filters_in_combo': settings.get('min_filters_in_combo'),
                    'max_filters_in_combo': settings.get('max_filters_in_combo'),
                    'min_good_trades_kept_pct': settings.get('min_good_trades_kept_pct'),
                    'min_bad_trades_removed_pct': settings.get('min_bad_trades_removed_pct'),
                    'combo_min_good_kept_pct': settings.get('combo_min_good_kept_pct'),
                    'combo_min_improvement': settings.get('combo_min_improvement'),
                    'auto_project_name': settings.get('auto_project_name'),
                    'percentile_low': settings.get('percentile_low'),
                    'percentile_high': settings.get('percentile_high'),
                    'is_ratio': settings.get('is_ratio'),
                    'skip_columns': settings.get('skip_columns'),
                    'section_prefixes': settings.get('section_prefixes'),
                }
                
                for key, value in setting_map.items():
                    if value is not None:
                        # Convert complex types to JSON strings
                        if isinstance(value, (dict, list)):
                            value_str = json.dumps(value)
                        else:
                            value_str = str(value)
                        
                        # Upsert setting
                        cursor.execute("""
                            INSERT INTO auto_filter_settings (setting_key, setting_value, updated_at)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (setting_key) 
                            DO UPDATE SET 
                                setting_value = EXCLUDED.setting_value,
                                updated_at = CURRENT_TIMESTAMP
                        """, [key, value_str])
                        
                        saved_settings[key] = value
                
                conn.commit()
        
        logger.info(f"Filter settings saved to PostgreSQL: {list(saved_settings.keys())}")
        
        return jsonify({
            'success': True,
            'message': 'Settings saved successfully to PostgreSQL',
            'current_settings': saved_settings
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
# EMAIL REPORT PREVIEW
# =============================================================================

@app.route('/email_report')
def email_report_preview():
    """Render the system health email report as a live HTML page."""
    try:
        from features.email_report.report import generate_html
        html = generate_html()
        return html, 200, {'Content-Type': 'text/html; charset=utf-8'}
    except Exception as e:
        logger.error(f"Email report generation failed: {e}", exc_info=True)
        return f"<pre>Error generating report:\n{e}</pre>", 500, {'Content-Type': 'text/html'}


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

    # Prefer Waitress (production WSGI) for stability; fallback to Flask dev server with threading
    try:
        import waitress
        # Threaded server: handles concurrent requests without blocking; no connection exhaustion
        waitress.serve(app, host=args.host, port=args.port, threads=8, connection_limit=100)
    except ImportError:
        app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == '__main__':
    main()

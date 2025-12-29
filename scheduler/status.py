"""
Scheduler Job Status Tracking
=============================
Shared module for tracking job execution status.
Separate from master.py to avoid circular imports with API.

Includes:
- In-memory job status tracking (for real-time dashboard)
- DuckDB persistence of execution metrics (for historical analysis)
"""

import time
import threading
import logging
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

logger = logging.getLogger(__name__)

# =============================================================================
# JOB STATUS TRACKING - Tracks when each job last ran (in-memory)
# =============================================================================
_job_status = {}
_job_status_lock = threading.Lock()
_scheduler_start_time = None

# Counter for generating unique metric IDs
_metric_id_counter = 0
_metric_id_lock = threading.Lock()

# Flag to track if metrics table is initialized
_metrics_table_initialized = False


def set_scheduler_start_time():
    """Record when the scheduler started (UTC)."""
    global _scheduler_start_time
    _scheduler_start_time = datetime.now(timezone.utc)
    
    # Initialize the metrics table on scheduler start
    _init_metrics_table()


def _get_next_metric_id() -> int:
    """Generate a unique metric ID using timestamp-based counter."""
    global _metric_id_counter
    with _metric_id_lock:
        _metric_id_counter += 1
        # Use timestamp + counter to ensure uniqueness even after restart
        base = int(time.time() * 1000) % 1000000000
        return base * 1000 + (_metric_id_counter % 1000)


def _init_metrics_table():
    """Initialize the job_execution_metrics table in DuckDB.
    
    Only initializes if TradingDataEngine is running (uses in-memory DuckDB).
    Skips if engine not running to avoid file-based DuckDB lock issues.
    """
    global _metrics_table_initialized
    
    if _metrics_table_initialized:
        return True
    
    try:
        # Check if TradingDataEngine is running first
        try:
            from core.trading_engine import _engine_instance
            if _engine_instance is None or not _engine_instance._running:
                logger.info("[METRICS] TradingDataEngine not running yet - skipping metrics init")
                return False
        except Exception:
            logger.info("[METRICS] TradingDataEngine not available - skipping metrics init")
            return False
        
        from core.database import get_duckdb
        from features.price_api.schema import SCHEMA_JOB_EXECUTION_METRICS
        
        logger.info("[METRICS] Initializing job_execution_metrics table...")
        
        with get_duckdb("central") as conn:
            # Create table if not exists
            conn.execute(SCHEMA_JOB_EXECUTION_METRICS)
            
            # Verify table was created
            result = conn.execute("SELECT COUNT(*) FROM job_execution_metrics").fetchone()
            logger.info(f"[METRICS] Table ready, current row count: {result[0]}")
        
        _metrics_table_initialized = True
        logger.info("[METRICS] job_execution_metrics table initialized successfully")
        return True
    except Exception as e:
        import traceback
        logger.error(f"[METRICS] Failed to initialize job_execution_metrics table: {e}\n{traceback.format_exc()}")
        return False


# Queue for async metrics recording (prevents blocking job execution)
_metrics_queue = []
_metrics_queue_lock = threading.Lock()
_metrics_writer_running = False
_metrics_writer_thread = None


def stop_metrics_writer():
    """Stop the background metrics writer (for clean shutdown)."""
    global _metrics_writer_running
    _metrics_writer_running = False
    if _metrics_writer_thread and _metrics_writer_thread.is_alive():
        _metrics_writer_thread.join(timeout=2)
    logger.info("[METRICS] Background metrics writer stopped")


def _start_metrics_writer():
    """Start the background metrics writer thread."""
    global _metrics_writer_running
    
    if _metrics_writer_running:
        return
    
    def _writer_loop():
        """Background thread that writes metrics to DuckDB."""
        global _metrics_writer_running
        _metrics_writer_running = True
        
        from core.database import get_duckdb
        
        while _metrics_writer_running:
            batch = []
            try:
                # Get pending metrics
                with _metrics_queue_lock:
                    if _metrics_queue:
                        batch = _metrics_queue.copy()
                        _metrics_queue.clear()
                
                if batch:
                    # Write batch to DuckDB using the connection pool (handles locking)
                    try:
                        with get_duckdb("central") as conn:
                            for metric in batch:
                                conn.execute("""
                                    INSERT INTO job_execution_metrics 
                                    (id, job_id, started_at, ended_at, duration_ms, status, error_message, created_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                """, metric)
                        logger.debug(f"[METRICS] Wrote {len(batch)} metrics to DuckDB")
                    except Exception as e:
                        logger.warning(f"[METRICS] Batch write failed: {e}")
                
                # Sleep briefly to batch up writes
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[METRICS] Writer loop error: {e}")
                time.sleep(1)
    
    global _metrics_writer_thread
    _metrics_writer_thread = threading.Thread(target=_writer_loop, name="MetricsWriter", daemon=True)
    _metrics_writer_thread.start()
    logger.info("[METRICS] Background metrics writer started")


def _record_execution(job_id: str, started_at: float, ended_at: float, 
                      duration_ms: float, status: str, error_message: str = None):
    """
    Record a job execution to the metrics queue (non-blocking).
    
    Queues metrics for async writing to avoid blocking job execution.
    A background thread handles the actual DuckDB writes.
    """
    global _metrics_table_initialized, _metrics_writer_running
    
    # Ensure table exists
    if not _metrics_table_initialized:
        if not _init_metrics_table():
            return
    
    # Ensure writer is running (may not be if table was initialized separately)
    if not _metrics_writer_running:
        _start_metrics_writer()
    
    try:
        metric_id = _get_next_metric_id()
        started_dt = datetime.fromtimestamp(started_at, tz=timezone.utc)
        ended_dt = datetime.fromtimestamp(ended_at, tz=timezone.utc)
        
        # Truncate error message to fit column
        err_msg = error_message
        if err_msg and len(err_msg) > 500:
            err_msg = err_msg[:497] + "..."
        
        # Queue the metric for async writing (non-blocking)
        metric_tuple = [
            metric_id,
            job_id,
            started_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
            ended_dt.strftime('%Y-%m-%d %H:%M:%S.%f'),
            duration_ms,
            status,
            err_msg
        ]
        
        with _metrics_queue_lock:
            _metrics_queue.append(metric_tuple)
        
        logger.debug(f"[METRICS] Queued {job_id}: {duration_ms:.1f}ms, {status}")
        
    except Exception as e:
        logger.warning(f"[METRICS] Failed to queue metric for {job_id}: {e}")


def track_job(job_id: str, description: str = None):
    """
    Decorator to track job execution status.
    
    Records:
    - In-memory status (start time, end time, status, error message, run count)
    - DuckDB metrics (execution duration for historical analysis)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _job_status
            
            # Record start time with high precision
            start_time = time.time()
            start_dt = datetime.now(timezone.utc)
            
            with _job_status_lock:
                _job_status[job_id] = {
                    'job_id': job_id,
                    'description': description or job_id,
                    'status': 'running',
                    'last_start': start_dt.isoformat(),
                    'last_end': None,
                    'last_success': _job_status.get(job_id, {}).get('last_success'),
                    'last_error': _job_status.get(job_id, {}).get('last_error'),
                    'error_message': None,
                    'run_count': _job_status.get(job_id, {}).get('run_count', 0) + 1,
                    'last_duration_ms': _job_status.get(job_id, {}).get('last_duration_ms'),
                }
            
            error_msg = None
            status = 'success'
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                
                with _job_status_lock:
                    _job_status[job_id]['status'] = 'success'
                    _job_status[job_id]['last_end'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['last_success'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['last_duration_ms'] = duration_ms
                
                # Record to DuckDB (async)
                _record_execution(job_id, start_time, end_time, duration_ms, 'success')
                
                return result
                
            except Exception as e:
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                error_msg = str(e)
                status = 'error'
                
                with _job_status_lock:
                    _job_status[job_id]['status'] = 'error'
                    _job_status[job_id]['last_end'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['last_error'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['error_message'] = error_msg
                    _job_status[job_id]['last_duration_ms'] = duration_ms
                
                # Record to DuckDB (async)
                _record_execution(job_id, start_time, end_time, duration_ms, 'error', error_msg)
                
                raise
                
        return wrapper
    return decorator


def update_job_status(job_id: str, status: str, description: str = None, 
                      error_message: str = None, is_service: bool = False, 
                      is_stream: bool = False):
    """
    Manually update job status (for services/streams that aren't decorated).
    """
    global _job_status
    
    with _job_status_lock:
        now = datetime.now(timezone.utc).isoformat()
        existing = _job_status.get(job_id, {})
        
        _job_status[job_id] = {
            'job_id': job_id,
            'description': description or existing.get('description', job_id),
            'status': status,
            'last_start': existing.get('last_start') or now,
            'last_end': now if status in ('success', 'error', 'stopped') else None,
            'last_success': now if status == 'success' else existing.get('last_success'),
            'last_error': now if status == 'error' else existing.get('last_error'),
            'error_message': error_message,
            'run_count': existing.get('run_count', 0) + (1 if status == 'running' else 0),
            'last_duration_ms': existing.get('last_duration_ms'),
            'is_service': is_service,
            'is_stream': is_stream,
        }


def get_job_status() -> dict:
    """Get current status of all tracked jobs."""
    with _job_status_lock:
        return {
            'jobs': dict(_job_status),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'scheduler_started': _scheduler_start_time.isoformat() if _scheduler_start_time else None,
        }


def get_job_metrics(hours: float = 1) -> dict:
    """
    Get job execution metrics from DuckDB.
    
    Returns per-job statistics:
    - avg_duration_ms
    - max_duration_ms  
    - min_duration_ms
    - execution_count
    - error_count
    - expected_interval_ms
    - is_slow (avg > expected interval)
    
    Args:
        hours: Number of hours of history to analyze (default: 1, supports decimals)
    
    Returns:
        Dictionary with 'jobs' containing metrics per job
    """
    try:
        from core.database import get_duckdb
        from features.price_api.schema import JOB_EXPECTED_INTERVALS_MS
        
        metrics = {}
        
        # Convert hours to minutes for more precise filtering
        minutes = int(hours * 60)
        
        with get_duckdb("central") as conn:
            # Single optimized query that gets both aggregate stats AND recent executions
            # Using a CTE to filter data once and reuse it
            result = conn.execute(f"""
                WITH filtered_metrics AS (
                    SELECT job_id, started_at, duration_ms, status
                    FROM job_execution_metrics
                    WHERE started_at >= NOW() - INTERVAL {minutes} MINUTE
                ),
                agg_stats AS (
                    SELECT 
                        job_id,
                        COUNT(*) as execution_count,
                        AVG(duration_ms) as avg_duration_ms,
                        MAX(duration_ms) as max_duration_ms,
                        MIN(duration_ms) as min_duration_ms,
                        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                        MAX(started_at) as last_execution
                    FROM filtered_metrics
                    GROUP BY job_id
                ),
                recent AS (
                    SELECT job_id, started_at, duration_ms, status,
                           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY started_at DESC) as rn
                    FROM filtered_metrics
                )
                SELECT 
                    a.job_id,
                    a.execution_count,
                    a.avg_duration_ms,
                    a.max_duration_ms,
                    a.min_duration_ms,
                    a.error_count,
                    a.last_execution,
                    r.started_at as recent_started_at,
                    r.duration_ms as recent_duration_ms,
                    r.status as recent_status,
                    r.rn
                FROM agg_stats a
                LEFT JOIN recent r ON a.job_id = r.job_id AND r.rn <= 20
                ORDER BY a.job_id, r.rn
            """).fetchall()
            
            # Process results - aggregate and recent are combined in one query
            current_job_id = None
            for row in result:
                job_id = row[0]
                
                # First time seeing this job - add aggregate data
                if job_id != current_job_id:
                    current_job_id = job_id
                    expected_interval = JOB_EXPECTED_INTERVALS_MS.get(job_id, 60000)
                    avg_ms = row[2] if row[2] else 0
                    
                    metrics[job_id] = {
                        'job_id': job_id,
                        'execution_count': row[1],
                        'avg_duration_ms': round(avg_ms, 2),
                        'max_duration_ms': round(row[3], 2) if row[3] else 0,
                        'min_duration_ms': round(row[4], 2) if row[4] else 0,
                        'error_count': row[5],
                        'expected_interval_ms': expected_interval,
                        'is_slow': avg_ms > expected_interval * 0.8,
                        'last_execution': row[6].isoformat() if row[6] else None,
                        'recent_executions': []
                    }
                
                # Add recent execution if present
                if row[7] is not None:  # recent_started_at
                    metrics[job_id]['recent_executions'].append({
                        'started_at': row[7].isoformat() if row[7] else None,
                        'duration_ms': round(row[8], 2) if row[8] else 0,
                        'status': row[9]
                    })
        
        return {
            'status': 'ok',
            'hours': hours,
            'jobs': metrics,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get job metrics: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'jobs': {},
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

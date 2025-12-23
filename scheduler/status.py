"""
Scheduler Job Status Tracking
=============================
Shared module for tracking job execution status.
Separate from master.py to avoid circular imports with API.
"""

import threading
from datetime import datetime, timezone
from functools import wraps

# =============================================================================
# JOB STATUS TRACKING - Tracks when each job last ran
# =============================================================================
_job_status = {}
_job_status_lock = threading.Lock()
_scheduler_start_time = None


def set_scheduler_start_time():
    """Record when the scheduler started (UTC)."""
    global _scheduler_start_time
    _scheduler_start_time = datetime.now(timezone.utc)


def track_job(job_id: str, description: str = None):
    """
    Decorator to track job execution status.
    Records start time, end time, status (success/error), and error message if any.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _job_status
            
            with _job_status_lock:
                _job_status[job_id] = {
                    'job_id': job_id,
                    'description': description or job_id,
                    'status': 'running',
                    'last_start': datetime.now(timezone.utc).isoformat(),
                    'last_end': None,
                    'last_success': _job_status.get(job_id, {}).get('last_success'),
                    'last_error': _job_status.get(job_id, {}).get('last_error'),
                    'error_message': None,
                    'run_count': _job_status.get(job_id, {}).get('run_count', 0) + 1,
                }
            
            try:
                result = func(*args, **kwargs)
                with _job_status_lock:
                    _job_status[job_id]['status'] = 'success'
                    _job_status[job_id]['last_end'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['last_success'] = datetime.now(timezone.utc).isoformat()
                return result
            except Exception as e:
                with _job_status_lock:
                    _job_status[job_id]['status'] = 'error'
                    _job_status[job_id]['last_end'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['last_error'] = datetime.now(timezone.utc).isoformat()
                    _job_status[job_id]['error_message'] = str(e)
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


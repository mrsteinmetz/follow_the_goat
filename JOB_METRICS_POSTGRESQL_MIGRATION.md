# Job Metrics PostgreSQL Migration - Complete

## Overview

Successfully migrated the job execution metrics system from DuckDB to PostgreSQL. The scheduler metrics dashboard at `http://195.201.84.5/pages/features/scheduler-metrics/` now displays real-time job execution data from PostgreSQL.

## What Was Changed

### 1. **scheduler/status.py** - Core Metrics Tracking Module
   - **Updated `_init_metrics_table()`**: Now verifies PostgreSQL table instead of DuckDB
   - **Updated `_start_metrics_writer()`**: Background writer now uses PostgreSQL connections
   - **Updated `_record_execution()`**: Metrics are queued and written to PostgreSQL asynchronously
   - **Updated `get_job_metrics()`**: Retrieves metrics from PostgreSQL with proper RealDictCursor handling
   - **Fixed cursor access**: Changed from tuple indexing `result[0]` to dict keys `result['count']` for RealDictCursor compatibility

### 2. **scheduler/master2.py** - Trading Logic API  
   - **Added `/job_metrics` endpoint**: Returns job execution statistics (avg/max/min duration, execution counts, slow job detection)
   - **Added `/job_metrics_debug` endpoint**: Debug endpoint to verify table status and recent metrics
   - Uses FastAPI on port 5052

### 3. **scheduler/website_api.py** - Website Backend API
   - **Added `/job_metrics` endpoint**: Same metrics endpoint accessible from website (Flask, port 5051)
   - **Added `/job_metrics_debug` endpoint**: Debug endpoint for troubleshooting
   - Both endpoints query PostgreSQL directly

### 4. **features/price_api/api.py** - Price API Module
   - **Updated `/job_metrics_debug` endpoint**: Fixed to use PostgreSQL instead of DuckDB
   - Updated cursor access patterns for RealDictCursor

### 5. **Database Schema**
   - **Recreated `job_execution_metrics` table** with correct schema:
     ```sql
     CREATE TABLE job_execution_metrics (
         id BIGSERIAL PRIMARY KEY,
         job_id VARCHAR(100) NOT NULL,
         started_at TIMESTAMP NOT NULL,
         ended_at TIMESTAMP NOT NULL,
         duration_ms DOUBLE PRECISION NOT NULL,
         status VARCHAR(20) NOT NULL,
         error_message VARCHAR(500),
         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     );
     ```
   - **Added indexes** for performance:
     - `idx_job_metrics_job_id` on `job_id`
     - `idx_job_metrics_started_at` on `started_at`
     - `idx_job_metrics_job_started` on `(job_id, started_at)`

## How It Works

### Metrics Collection Flow

1. **Job Execution**: When a scheduled job runs (wrapped with `@track_job` decorator):
   - Start time is recorded
   - Job executes
   - End time and duration are calculated
   - Metrics are queued for async writing

2. **Async Writing**: Background thread (`_metrics_writer_thread`):
   - Collects queued metrics every 0.5 seconds
   - Writes batch to PostgreSQL
   - Non-blocking to avoid slowing down job execution

3. **Retrieval**: Website queries `/job_metrics` endpoint:
   - API queries PostgreSQL for metrics within time window (default 1 hour)
   - Calculates aggregate statistics (avg, max, min, count)
   - Identifies slow jobs (avg duration > 80% of expected interval)
   - Returns recent execution history (last 20 per job)

### Endpoints Available

#### Website API (port 5051)
- `GET /job_metrics?hours=1` - Get job execution metrics for past N hours
- `GET /job_metrics_debug` - Debug endpoint showing table status

#### Master2 API (port 5052)  
- `GET /job_metrics?hours=1` - Get job execution metrics for past N hours
- `GET /job_metrics_debug` - Debug endpoint showing table status

### Response Format

```json
{
    "status": "ok",
    "hours": 1.0,
    "jobs": {
        "follow_the_goat": {
            "job_id": "follow_the_goat",
            "execution_count": 3600,
            "avg_duration_ms": 45.23,
            "max_duration_ms": 250.15,
            "min_duration_ms": 5.02,
            "error_count": 0,
            "expected_interval_ms": 1000,
            "is_slow": false,
            "last_execution": "2026-01-14T00:49:34.732589",
            "recent_executions": [
                {
                    "started_at": "2026-01-14T00:49:34.732589",
                    "duration_ms": 42.38,
                    "status": "success"
                }
            ]
        }
    },
    "timestamp": "2026-01-14T00:49:40.123456"
}
```

## Key Features

1. **Performance Metrics**: Track average, min, max execution times per job
2. **Slow Job Detection**: Automatically flags jobs running slower than expected
3. **Error Tracking**: Counts and logs job failures with error messages
4. **Recent History**: Shows last 20 executions for each job
5. **Flexible Time Windows**: Query metrics for any time period (0.01 to 24 hours)
6. **Non-blocking**: Async writing prevents job execution delays

## Data Retention

- **Hot Storage**: Last 24 hours kept in PostgreSQL `job_execution_metrics` table
- **Automatic Cleanup**: Old metrics are archived/deleted by `keep_24_hours_of_data.py`

## Monitoring Jobs Tracked

Jobs currently being tracked (from `scheduler/master2.py`):
- `train_validator` - Train validator (every 10s)
- `follow_the_goat` - Wallet tracker cycle (every 1s)
- `trailing_stop_seller` - Trailing stop seller (every 1s)
- `update_potential_gains` - Update potential gains (every 15s)
- `create_new_patterns` - Auto-generate filter patterns (every 5 min)
- `create_profiles` - Build wallet profiles (every 30s)
- `archive_old_data` - Archive data older than 24h (hourly)
- `export_job_status` - Export job status to file (every 5s)

## Testing

To verify the system is working:

1. **Check table status**:
   ```bash
   curl http://localhost:5051/job_metrics_debug | python3 -m json.tool
   ```

2. **Get recent metrics**:
   ```bash
   curl "http://localhost:5051/job_metrics?hours=1" | python3 -m json.tool
   ```

3. **View in browser**: 
   http://195.201.84.5/pages/features/scheduler-metrics/

## Troubleshooting

### No metrics appearing
- Check that master2.py is running: `ps aux | grep master2.py`
- Verify table exists: `curl http://localhost:5051/job_metrics_debug`
- Check writer status: Look for `metrics_writer_running: true` in debug output

### Slow queries
- Ensure indexes exist: Check `idx_job_metrics_*` indexes
- Reduce time window: Use `hours=0.5` instead of `hours=24`

### Connection errors
- Verify PostgreSQL is running and accessible
- Check connection pooling: `core/database.py` has pool settings

## Migration Complete ✓

The job metrics system is now fully operational with PostgreSQL, providing real-time performance monitoring for all scheduled jobs. The website dashboard displays execution times, error rates, and identifies slow-running jobs automatically.

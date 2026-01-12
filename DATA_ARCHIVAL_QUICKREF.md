# Data Archival Quick Reference

## Overview
Automatically archives PostgreSQL data older than 24 hours to compressed Parquet files.

## Files
- **Script:** `000data_feeds/8_keep_24_hours_of_data/keep_24_hours_of_data.py`
- **Scheduler:** `scheduler/master2.py` (runs hourly at :00)
- **Archive Location:** `/root/follow_the_goat/archived_data/`

## Quick Commands

### Manual Testing (Dry Run - No Deletion)
```bash
cd /root/follow_the_goat
ARCHIVE_DRY_RUN=1 python3 000data_feeds/8_keep_24_hours_of_data/keep_24_hours_of_data.py
```

### Manual Execution (Production)
```bash
cd /root/follow_the_goat
python3 000data_feeds/8_keep_24_hours_of_data/keep_24_hours_of_data.py
```

### Check Archived Files
```bash
# List all Parquet files
find /root/follow_the_goat/archived_data -name "*.parquet" -exec ls -lh {} \;

# Check disk usage by directory
du -sh /root/follow_the_goat/archived_data/*/
```

### Read Archived Data
```python
import pandas as pd

# Read archived prices
df = pd.read_parquet('archived_data/prices/prices_2026-01-12.parquet')
print(f"Rows: {len(df)}")
print(df.head())

# Filter while reading (efficient)
df_sol = pd.read_parquet(
    'archived_data/prices/prices_2026-01-12.parquet',
    filters=[('token', '==', 'SOL')]
)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_ARCHIVAL_ENABLED` | `1` | Enable/disable hourly archival (0=off, 1=on) |
| `ARCHIVE_DRY_RUN` | `0` | Test mode without deletion (0=production, 1=dry-run) |

## What Gets Archived

### Transactional Tables (Archived)
- `prices`, `sol_stablecoin_trades`, `order_book_features`
- `whale_movements`, `cycle_tracker`, `follow_the_goat_buyins`
- `follow_the_goat_buyins_price_checks`, `price_points`, `price_analysis`
- `wallet_profiles`, `buyin_trail_minutes`, `trade_filter_values`
- `job_execution_metrics`

### Configuration Tables (Never Archived)
- `follow_the_goat_plays`, `follow_the_goat_tracking`
- `pattern_config_projects`, `pattern_config_filters`
- `wallet_profiles_state`, `filter_fields_catalog`
- `filter_reference_suggestions`, `filter_combinations`
- `ai_play_updates`

## Monitoring

### Check Logs
```bash
# View archival job logs
tail -f logs/scheduler2_errors.log | grep "archive_old_data"

# View last archival run
tail -100 logs/scheduler2_errors.log | grep -A 20 "Starting data archival"
```

### Verify Job is Running
```python
# When master2.py is running, check registered jobs
import requests
response = requests.get('http://localhost:5052/health')
print(response.json())
```

## Troubleshooting

### Archival Not Running
1. Check `DATA_ARCHIVAL_ENABLED=1` is set
2. Verify `master2.py` is running
3. Check logs for errors

### Disk Space Issues
```bash
# Check archive directory size
du -sh /root/follow_the_goat/archived_data/

# Check PostgreSQL database size
sudo -u postgres psql -c "SELECT pg_size_pretty(pg_database_size('solcatcher'));"
```

### Restore Archived Data
```python
import pandas as pd
from core.database import get_postgres

# Read Parquet file
df = pd.read_parquet('archived_data/prices/prices_2026-01-11.parquet')

# Write back to PostgreSQL
with get_postgres() as conn:
    df.to_sql('prices', conn, if_exists='append', index=False)
```

## Performance Stats (Tested)

| Table | Rows | PostgreSQL Size | Parquet Size | Compression |
|-------|------|-----------------|--------------|-------------|
| prices | 1.3M | 164 MB | 13 MB | **12.7x** |
| sol_stablecoin_trades | 825K | 524 MB | 98 MB | **5.4x** |

## Schedule

- **Frequency:** Every hour at :00 (1:00, 2:00, 3:00, etc.)
- **Retention:** 24 hours in PostgreSQL
- **Archival:** All data older than 24 hours → Parquet
- **Deletion:** After successful archival (atomic operation)

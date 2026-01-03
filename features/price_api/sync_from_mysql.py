"""
MySQL Sync Script - DEPRECATED
==============================
This script was previously used to sync data from Windows MySQL to DuckDB.

As of the architecture change, MySQL is no longer used as a data SOURCE.
- DuckDB (in-memory) is the PRIMARY database for all live operations
- MySQL (local Ubuntu) is used ONLY for archiving expired data
- Plays are loaded from config/plays_cache.json at startup

The archive operation is handled automatically by the scheduler cleanup jobs.
See core/database.py: archive_old_data() for the archive-on-cleanup logic.

For manual data operations, use the scripts in scripts/ directory.
"""

print(__doc__)
print("\nThis script is deprecated. Use the scheduler cleanup jobs instead.")
print("See scheduler/master.py for automatic archiving.")

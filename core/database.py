"""
Database Connection Manager for DuckDB
======================================
Central database access for all features.
See duckdb/ARCHITECTURE.md for schema documentation.
"""

import duckdb
from pathlib import Path
from contextlib import contextmanager
from typing import Optional

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# Database paths
DATABASES = {
    "prices": PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb",
}


def get_db_path(name: str = "prices") -> Path:
    """Get the path to a database file."""
    if name not in DATABASES:
        raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
    return DATABASES[name]


@contextmanager
def get_db(name: str = "prices"):
    """
    Context manager for database connections.
    
    Usage:
        with get_db() as conn:
            result = conn.execute("SELECT * FROM price_points").fetchall()
    """
    conn = duckdb.connect(str(get_db_path(name)))
    try:
        yield conn
    finally:
        conn.close()


def register_database(name: str, path: Path):
    """
    Register a new database path.
    Call this when adding new feature databases.
    """
    DATABASES[name] = path


def archive_old_data(table_name: str, db_name: str = "prices", hours: int = 24):
    """
    Move data older than specified hours from hot to cold storage.
    
    Args:
        table_name: Base table name (without _archive suffix)
        db_name: Database to use
        hours: Age threshold in hours (default 24)
    """
    archive_table = f"{table_name}_archive"
    
    with get_db(db_name) as conn:
        # Move old data to archive
        conn.execute(f"""
            INSERT INTO {archive_table}
            SELECT * FROM {table_name}
            WHERE created_at < NOW() - INTERVAL {hours} HOUR
        """)
        
        # Delete from hot table
        deleted = conn.execute(f"""
            DELETE FROM {table_name}
            WHERE created_at < NOW() - INTERVAL {hours} HOUR
        """).fetchone()
        
        return deleted


def ensure_tables_exist(db_name: str, schema_sql: str):
    """
    Run schema SQL to ensure tables exist.
    Idempotent - safe to call multiple times.
    """
    with get_db(db_name) as conn:
        conn.execute(schema_sql)


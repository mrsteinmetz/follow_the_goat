"""
Database Connection Manager - PostgreSQL Only
==============================================
Central database access for PostgreSQL.
All data stored in a single PostgreSQL database.

Connection Strategy:
- PostgreSQL: Connection pooling for efficient concurrent access
- All reads and writes go directly to PostgreSQL
- No hot/cold storage distinction - all data in one place
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
import threading
import logging
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta

from core.config import settings

# Configure logger for this module
logger = logging.getLogger("database")

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# =============================================================================
# PostgreSQL Connection Pool
# =============================================================================

class PostgreSQLPool:
    """
    Singleton connection pool for PostgreSQL.
    
    Uses psycopg2.pool.SimpleConnectionPool for efficient connection management.
    Thread-safe with automatic connection recycling.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pool = None
                    cls._instance._initialized = False
        return cls._instance
    
    def _initialize_pool(self):
        """Initialize the connection pool."""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            try:
                # Debug: Print connection parameters
                logger.info(f"Attempting PostgreSQL connection: {settings.postgres.user}@{settings.postgres.host}:{settings.postgres.port}/{settings.postgres.database}")
                
                # Try simple connection first (without cursor_factory)
                test_conn = psycopg2.connect(
                    host=settings.postgres.host,
                    user=settings.postgres.user,
                    password=settings.postgres.password,
                    database=settings.postgres.database,
                    port=settings.postgres.port,
                    connect_timeout=10
                )
                test_conn.close()
                logger.info("✓ Test connection successful")
                
                # Create connection pool with 1-10 connections (reduced for stability)
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=settings.postgres.host,
                    user=settings.postgres.user,
                    password=settings.postgres.password,
                    database=settings.postgres.database,
                    port=settings.postgres.port,
                    cursor_factory=psycopg2.extras.RealDictCursor,
                    connect_timeout=10
                )
                self._initialized = True
                logger.info(f"PostgreSQL connection pool initialized: {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.database}")
            except psycopg2.OperationalError as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"PostgreSQL OperationalError: {str(e) or 'No error message'}\nDetails: {e.args}\n{error_details}")
                raise
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"Failed to initialize PostgreSQL connection pool: {type(e).__name__}: {e}\n{error_details}")
                raise
    
    def get_connection(self):
        """Get a connection from the pool."""
        if not self._initialized:
            self._initialize_pool()
        
        try:
            conn = self._pool.getconn()
            conn.autocommit = True  # Auto-commit for simplicity
            return conn
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL connection from pool: {e}")
            raise
    
    def return_connection(self, conn):
        """Return a connection to the pool."""
        if self._pool and conn:
            try:
                self._pool.putconn(conn)
            except Exception as e:
                logger.warning(f"Failed to return connection to pool: {e}")
    
    def close_all(self):
        """Close all connections in the pool."""
        if self._pool:
            try:
                self._pool.closeall()
                logger.info("All PostgreSQL connections closed")
            except Exception as e:
                logger.error(f"Error closing PostgreSQL pool: {e}")
            finally:
                self._initialized = False
                self._pool = None


# Global pool instance
_pool = PostgreSQLPool()


# =============================================================================
# PostgreSQL Connection Management
# =============================================================================

@contextmanager
def get_postgres():
    """
    Context manager for PostgreSQL connections.
    
    Gets a connection from the pool and automatically returns it when done.
    
    Usage:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM prices WHERE token = %s", ['SOL'])
                results = cursor.fetchall()
    
    Returns:
        psycopg2 connection with RealDictCursor factory (results as dicts)
    """
    conn = None
    try:
        conn = _pool.get_connection()
        yield conn
    except Exception as e:
        logger.error(f"PostgreSQL connection error: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise
    finally:
        if conn:
            _pool.return_connection(conn)


def get_postgres_connection():
    """
    Get a raw PostgreSQL connection (must be closed manually).
    
    Use get_postgres() context manager instead when possible.
    
    Returns:
        psycopg2 connection
    """
    return _pool.get_connection()


def close_all_postgres():
    """Close all PostgreSQL connections (for shutdown)."""
    _pool.close_all()


# =============================================================================
# Helper Functions for Common Operations
# =============================================================================

def postgres_execute(sql: str, params: List[Any] = None) -> int:
    """
    Execute a write query (INSERT/UPDATE/DELETE) on PostgreSQL.
    
    Args:
        sql: SQL query string
        params: Query parameters (optional)
    
    Returns:
        Number of rows affected
    
    Example:
        rows = postgres_execute(
            "INSERT INTO prices (timestamp, token, price) VALUES (%s, %s, %s)",
            [datetime.now(), 'SOL', 123.45]
        )
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            return cursor.rowcount


def postgres_query(sql: str, params: List[Any] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query on PostgreSQL.
    
    Args:
        sql: SQL query string
        params: Query parameters (optional)
    
    Returns:
        List of result dictionaries
    
    Example:
        prices = postgres_query(
            "SELECT * FROM prices WHERE token = %s ORDER BY timestamp DESC LIMIT 10",
            ['SOL']
        )
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            return cursor.fetchall()


def postgres_query_one(sql: str, params: List[Any] = None) -> Optional[Dict[str, Any]]:
    """
    Execute a SELECT query and return only the first result.
    
    Args:
        sql: SQL query string
        params: Query parameters (optional)
    
    Returns:
        First result as dictionary, or None if no results
    
    Example:
        price = postgres_query_one(
            "SELECT price FROM prices WHERE token = %s ORDER BY timestamp DESC LIMIT 1",
            ['SOL']
        )
    """
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            return cursor.fetchone()


def postgres_insert(table: str, data: Dict[str, Any]) -> int:
    """
    Insert a record into PostgreSQL.
    
    Args:
        table: Table name
        data: Dictionary of column -> value
    
    Returns:
        ID of inserted record (if table has SERIAL primary key)
    
    Example:
        record_id = postgres_insert("prices", {
            "timestamp": datetime.now(),
            "token": "SOL",
            "price": 123.45
        })
    """
    columns = list(data.keys())
    values = list(data.values())
    placeholders = ", ".join(["%s" for _ in columns])
    columns_str = ", ".join(columns)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Try to get the ID if table has a SERIAL primary key
            cursor.execute(
                f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) RETURNING id",
                values
            )
            result = cursor.fetchone()
            return result['id'] if result else cursor.rowcount


def postgres_insert_many(table: str, records: List[Dict[str, Any]]) -> int:
    """
    Insert multiple records into PostgreSQL (bulk insert).
    
    Args:
        table: Table name
        records: List of dictionaries (all must have same keys)
    
    Returns:
        Number of rows inserted
    
    Example:
        count = postgres_insert_many("prices", [
            {"timestamp": datetime.now(), "token": "SOL", "price": 123.45},
            {"timestamp": datetime.now(), "token": "BTC", "price": 50000.00},
        ])
    """
    if not records:
        return 0
    
    columns = list(records[0].keys())
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s" for _ in columns])
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Build VALUES list for all records
            values_list = []
            for record in records:
                values_list.append([record.get(col) for col in columns])
            
            # Use execute_values for efficient bulk insert
            from psycopg2.extras import execute_values
            execute_values(
                cursor,
                f"INSERT INTO {table} ({columns_str}) VALUES %s ON CONFLICT DO NOTHING",
                values_list
            )
            return cursor.rowcount


def postgres_update(table: str, data: Dict[str, Any], where: Dict[str, Any]) -> int:
    """
    Update records in PostgreSQL.
    
    Args:
        table: Table name
        data: Dictionary of column -> new value
        where: Dictionary of column -> value for WHERE clause
    
    Returns:
        Number of rows updated
    
    Example:
        rows = postgres_update(
            "follow_the_goat_buyins",
            {"our_status": "sold", "our_exit_price": 125.00},
            {"id": 123}
        )
    """
    set_cols = list(data.keys())
    set_values = list(data.values())
    where_cols = list(where.keys())
    where_values = list(where.values())
    
    set_str = ", ".join([f"{col} = %s" for col in set_cols])
    where_str = " AND ".join([f"{col} = %s" for col in where_cols])
    all_values = set_values + where_values
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE {table} SET {set_str} WHERE {where_str}",
                all_values
            )
            return cursor.rowcount


# =============================================================================
# Data Cleanup Functions
# =============================================================================

def cleanup_old_data(table_name: str, timestamp_column: str, hours: int = 24) -> int:
    """
    Delete old data from a table based on timestamp.
    
    Args:
        table_name: Table name to clean up
        timestamp_column: Column name containing timestamp
        hours: Age threshold in hours (default 24)
    
    Returns:
        Number of records deleted
    
    Example:
        deleted = cleanup_old_data("prices", "timestamp", hours=24)
    """
    sql = f"""
        DELETE FROM {table_name}
        WHERE {timestamp_column} < NOW() - INTERVAL '{hours} hours'
    """
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old records from {table_name} (older than {hours}h)")
            return deleted


def cleanup_all_hot_tables(hours: int = 24) -> int:
    """
    Clean up all time-series tables.
    
    Tables cleaned:
    - prices (24h)
    - sol_stablecoin_trades (24h)
    - order_book_features (24h)
    - whale_movements (24h)
    - cycle_tracker (72h for completed cycles)
    - follow_the_goat_buyins (72h)
    - follow_the_goat_buyins_price_checks (72h)
    - wallet_profiles (24h)
    - job_execution_metrics (24h)
    
    Args:
        hours: Default age threshold (can be overridden per table)
    
    Returns:
        Total number of records deleted
    """
    total_deleted = 0
    
    # Standard 24h cleanup tables
    tables_24h = [
        ("prices", "timestamp"),
        ("sol_stablecoin_trades", "trade_timestamp"),
        ("order_book_features", "timestamp"),
        ("whale_movements", "timestamp"),
        ("wallet_profiles", "trade_timestamp"),
        ("price_analysis", "created_at"),
        ("buyin_trail_minutes", "created_at"),
        ("job_execution_metrics", "started_at"),
    ]
    
    for table, ts_col in tables_24h:
        try:
            deleted = cleanup_old_data(table, ts_col, hours=24)
            total_deleted += deleted
        except Exception as e:
            logger.warning(f"Failed to cleanup {table}: {e}")
    
    # 72h cleanup for trades (must match cycle retention)
    tables_72h = [
        ("follow_the_goat_buyins", "followed_at"),
        ("follow_the_goat_buyins_price_checks", "checked_at"),
    ]
    
    for table, ts_col in tables_72h:
        try:
            deleted = cleanup_old_data(table, ts_col, hours=72)
            total_deleted += deleted
        except Exception as e:
            logger.warning(f"Failed to cleanup {table}: {e}")
    
    # Special case: cycle_tracker (only delete COMPLETED cycles older than 72h)
    try:
        sql = """
            DELETE FROM cycle_tracker
            WHERE cycle_end_time IS NOT NULL
            AND cycle_end_time < NOW() - INTERVAL '72 hours'
        """
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                deleted = cursor.rowcount
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} completed cycles from cycle_tracker (72h retention)")
                total_deleted += deleted
    except Exception as e:
        logger.warning(f"Failed to cleanup cycle_tracker: {e}")
    
    return total_deleted


# =============================================================================
# Database Initialization
# =============================================================================

def init_postgres_schema():
    """
    Initialize PostgreSQL schema by running the migration script.
    
    This should be run once to set up all tables.
    Safe to run multiple times (CREATE TABLE IF NOT EXISTS).
    """
    schema_file = PROJECT_ROOT / "scripts" / "postgres_schema.sql"
    
    if not schema_file.exists():
        logger.warning(f"Schema file not found: {schema_file}")
        return False
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                # Read and execute the schema file
                schema_sql = schema_file.read_text()
                cursor.execute(schema_sql)
                logger.info("PostgreSQL schema initialized successfully")
                return True
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL schema: {e}")
        return False


def verify_tables_exist() -> bool:
    """
    Verify that all required tables exist in PostgreSQL.
    
    Returns:
        True if all tables exist, False otherwise
    """
    required_tables = [
        'prices', 'sol_stablecoin_trades', 'order_book_features', 'whale_movements',
        'cycle_tracker', 'follow_the_goat_plays', 'follow_the_goat_buyins',
        'follow_the_goat_buyins_price_checks', 'follow_the_goat_tracking',
        'price_points', 'price_analysis', 'wallet_profiles', 'wallet_profiles_state',
        'pattern_config_projects', 'pattern_config_filters', 'buyin_trail_minutes',
        'trade_filter_values', 'job_execution_metrics'
    ]
    
    try:
        with get_postgres() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = ANY(%s)
                """, [required_tables])
                
                existing_tables = {row['table_name'] for row in cursor.fetchall()}
                missing_tables = set(required_tables) - existing_tables
                
                if missing_tables:
                    logger.error(f"Missing tables in PostgreSQL: {missing_tables}")
                    return False
                
                logger.info(f"All {len(required_tables)} required tables exist in PostgreSQL")
                return True
    except Exception as e:
        logger.error(f"Failed to verify tables: {e}")
        return False


# =============================================================================
# Legacy Aliases (for backward compatibility during migration)
# =============================================================================

# Alias get_postgres to get_mysql for legacy code
get_mysql = get_postgres
get_mysql_connection = get_postgres_connection

# Alias for database module imports
get_db = get_postgres

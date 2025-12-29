"""
Database Connection Manager
===========================
Central database access for DuckDB (hot storage) and MySQL (historical master).
See duckdb/ARCHITECTURE.md for schema documentation.

Architecture:
- DuckDB: 24-hour hot storage for fast reads (TRADING BOT uses this)
- MySQL: Full historical data (master storage / archive)
- Dual-write: All writes go to both databases

Connection Strategy:
- DuckDB: Pooled connection with locking (WSL/Windows mount compatible)
         Windows-mounted filesystems (/mnt/c/) don't support concurrent DuckDB access.
         We use a single connection per database with thread locks.
- MySQL: Connection per request (pooled by pymysql)
"""

import duckdb
import pymysql
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

# Check if we're running in WSL by looking for /mnt/c path
IS_WSL = str(PROJECT_ROOT).startswith('/mnt/')

# DuckDB database paths
# On WSL, use WSL-native filesystem for better DuckDB performance and locking
if IS_WSL:
    WSL_DATA_DIR = Path.home() / "follow_the_goat_data"
    WSL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATABASES = {
        "prices": WSL_DATA_DIR / "prices.duckdb",
        "central": WSL_DATA_DIR / "central.duckdb",
    }
    logger.info(f"WSL detected - using native filesystem for DuckDB: {WSL_DATA_DIR}")
else:
    DATABASES = {
        "prices": PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb",
        "central": PROJECT_ROOT / "000data_feeds" / "central.duckdb",
    }


# =============================================================================
# DuckDB Connection Pool (Linux-Optimized with WAL Mode)
# =============================================================================
# On Linux, DuckDB supports excellent concurrency with WAL (Write-Ahead Log) mode.
# We use a single master connection per database that's shared across threads.
# WAL mode allows concurrent reads while writes are serialized automatically.
#
# The "Unique file handle conflict" error happens when multiple threads
# simultaneously try to CREATE new connections. We prevent this by:
# 1. Creating one master connection per database at startup
# 2. Using cursor() to create thread-local cursors from the master connection
# 3. Enabling WAL mode for better concurrent access
# =============================================================================

class DuckDBPool:
    """
    Singleton connection pool for DuckDB databases (Linux-optimized).
    
    Uses a single master connection per database with WAL mode enabled.
    Thread safety is achieved through DuckDB's internal mechanisms on Linux,
    plus a creation lock to prevent race conditions during initial connection.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._connections = {}
                    cls._instance._creation_locks = {}  # Only for connection creation
        return cls._instance
    
    def get_connection(self, name: str) -> duckdb.DuckDBPyConnection:
        """Get or create a persistent connection to a DuckDB database.
        
        On Linux with WAL mode, the same connection can be used by multiple
        threads safely. DuckDB handles the internal locking.
        """
        if name not in DATABASES:
            raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
        
        # Fast path: return existing healthy connection
        conn = self._connections.get(name)
        if conn is not None:
            try:
                # Quick health check
                conn.execute("SELECT 1").fetchone()
                return conn
            except Exception as e:
                logger.warning(f"DuckDB connection '{name}' unhealthy: {e}")
                # Fall through to recreate
        
        # Slow path: create new connection (with lock to prevent race)
        if name not in self._creation_locks:
            with self._lock:
                if name not in self._creation_locks:
                    self._creation_locks[name] = threading.Lock()
        
        with self._creation_locks[name]:
            # Double-check after acquiring lock
            conn = self._connections.get(name)
            if conn is not None:
                try:
                    conn.execute("SELECT 1").fetchone()
                    return conn
                except:
                    pass  # Will recreate below
            
            # Close old connection if exists
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass
            
            # Create new connection with WAL mode for better concurrency
            db_path = DATABASES[name]
            db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Creating DuckDB connection to '{name}' at {db_path}")
            
            conn = duckdb.connect(str(db_path))
            
            # Enable WAL mode for better concurrent access on Linux
            try:
                conn.execute("PRAGMA enable_progress_bar=false")  # Disable progress bar for cleaner logs
                # Note: DuckDB uses WAL-like behavior by default on Linux
            except Exception as e:
                logger.debug(f"PRAGMA setting note: {e}")
            
            self._connections[name] = conn
            return conn
    
    def close_all(self):
        """Close all connections (for shutdown)."""
        with self._lock:
            for name, conn in list(self._connections.items()):
                if conn is not None:
                    try:
                        conn.close()
                        logger.debug(f"Closed DuckDB connection '{name}'")
                    except Exception as e:
                        logger.warning(f"Error closing DuckDB connection '{name}': {e}")
            self._connections.clear()
            logger.info("All DuckDB connections closed")
    
    def close(self, name: str):
        """Close a specific connection (will be recreated on next access)."""
        creation_lock = self._creation_locks.get(name)
        if creation_lock:
            with creation_lock:
                conn = self._connections.get(name)
                if conn is not None:
                    try:
                        conn.close()
                        logger.debug(f"Closed DuckDB connection '{name}'")
                    except Exception as e:
                        logger.warning(f"Error closing DuckDB connection '{name}': {e}")
                    self._connections[name] = None
    
    def get_lock(self, name: str) -> threading.Lock:
        """Get a lock for a database (for backward compatibility).
        
        On Linux, DuckDB handles concurrency internally, so this lock is
        only used for connection creation, not for every operation.
        """
        if name not in self._creation_locks:
            with self._lock:
                if name not in self._creation_locks:
                    self._creation_locks[name] = threading.Lock()
        return self._creation_locks[name]


# Global pool instance
_pool = DuckDBPool()


def _get_db_path(name: str) -> Path:
    """Get the path for a named database."""
    if name not in DATABASES:
        raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
    db_path = DATABASES[name]
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return db_path


# =============================================================================
# DuckDB Connection Management (Engine-First)
# =============================================================================

def _get_engine_if_running():
    """Get TradingDataEngine if it's running, otherwise return None."""
    try:
        from core.trading_engine import _engine_instance
        if _engine_instance is not None and _engine_instance._running:
            return _engine_instance
    except Exception:
        pass
    return None


class QueryResult:
    """Thread-safe query result that holds fetched data."""
    
    def __init__(self, rows, description):
        self._rows = rows
        self._index = 0
        self.description = description
    
    def fetchone(self):
        """Fetch one result."""
        if self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            return row
        return None
    
    def fetchall(self):
        """Fetch all remaining results."""
        remaining = self._rows[self._index:]
        self._index = len(self._rows)
        return remaining
    
    def fetchmany(self, size=None):
        """Fetch many results."""
        if size is None:
            size = 1
        end = min(self._index + size, len(self._rows))
        rows = self._rows[self._index:end]
        self._index = end
        return rows


class EngineConnectionWrapper:
    """
    Thread-safe wrapper that makes TradingDataEngine look like a DuckDB connection.
    
    This allows existing code using `conn.execute()` to work with the
    in-memory engine without modification.
    
    Thread safety: Each execute() fetches ALL results immediately while holding
    the lock, then returns a QueryResult with the data. This prevents race
    conditions between execute() and fetch() calls.
    """
    
    def __init__(self, engine):
        self._engine = engine
        self.description = None  # Set after execute()
    
    def execute(self, query: str, params=None):
        """Execute a query and return a thread-safe result object.
        
        Fetches all results immediately while holding the lock to prevent
        race conditions in multi-threaded environments.
        """
        if params is None:
            params = []
        
        # Fetch ALL results while holding lock - prevents race conditions
        with self._engine._conn_lock:
            result = self._engine._conn.execute(query, params)
            description = result.description
            rows = result.fetchall()
        
        # Return thread-safe result with pre-fetched data
        query_result = QueryResult(rows, description)
        self.description = description
        return query_result
    
    def executemany(self, query: str, params_list):
        """Execute a query with multiple parameter sets."""
        with self._engine._conn_lock:
            result = self._engine._conn.executemany(query, params_list)
            description = result.description if hasattr(result, 'description') else None
            try:
                rows = result.fetchall()
            except:
                rows = []
        
        query_result = QueryResult(rows, description)
        self.description = description
        return query_result
    
    def fetchone(self):
        """For backwards compatibility - returns None (use execute().fetchone())."""
        return None
    
    def fetchall(self):
        """For backwards compatibility - returns empty (use execute().fetchall())."""
        return []
    
    def fetchmany(self, size=None):
        """For backwards compatibility - returns empty (use execute().fetchmany())."""
        return []


def get_db_path(name: str = "central") -> Path:
    """Get the path to a DuckDB database file."""
    return _get_db_path(name)


@contextmanager
def get_duckdb(name: str = "central", read_only: bool = False):
    """
    Context manager for DuckDB connections.
    
    AUTOMATICALLY uses TradingDataEngine (in-memory) when running under scheduler.
    Falls back to file-based DuckDB only when engine is not available.
    
    This ensures zero lock contention for the trading bot - all modules
    automatically benefit from the in-memory engine without code changes.
    
    Usage:
        with get_duckdb() as conn:
            result = conn.execute("SELECT * FROM table").fetchall()
    
    Args:
        name: Database name ("central" or "prices")
        read_only: Ignored (kept for API compatibility)
    """
    # Try to use TradingDataEngine first (in-memory, zero locks)
    engine = _get_engine_if_running()
    if engine is not None and name == "central":
        # Use engine wrapper for central database
        yield EngineConnectionWrapper(engine)
        return
    
    # Fallback to file-based DuckDB (standalone mode or non-central DB)
    try:
        conn = _pool.get_connection(name)
        yield conn
    except Exception as e:
        error_str = str(e).lower()
        # Handle connection errors by reconnecting
        if "connection" in error_str or "closed" in error_str or "file handle" in error_str or "attach" in error_str:
            logger.warning(f"DuckDB connection error, reconnecting: {e}")
            _pool.close(name)
            conn = _pool.get_connection(name)
            yield conn
        else:
            raise


@contextmanager
def get_duckdb_fresh(name: str = "central"):
    """
    Get a FRESH DuckDB connection (not pooled).
    
    Use this when you need an isolated connection (e.g., for long-running
    operations that shouldn't block other threads).
    
    On Linux, this is generally safe for read operations.
    """
    db_path = _get_db_path(name)
    conn = duckdb.connect(str(db_path))
    
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_duckdb_pooled(name: str = "central"):
    """
    Alias for get_duckdb() - both use the connection pool.
    
    Kept for backward compatibility.
    """
    with get_duckdb(name) as conn:
        yield conn


# Legacy alias for backward compatibility
@contextmanager
def get_db(name: str = "central"):
    """Legacy alias for get_duckdb()."""
    with get_duckdb(name) as conn:
        yield conn


def register_database(name: str, path: Path):
    """Register a new database path."""
    DATABASES[name] = path


def close_all_duckdb():
    """Close all DuckDB connections (for shutdown)."""
    _pool.close_all()


# =============================================================================
# MySQL Connection Management
# =============================================================================

@contextmanager
def get_mysql():
    """
    Context manager for MySQL connections.
    
    Usage:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM table")
                result = cursor.fetchall()
    """
    conn = pymysql.connect(
        host=settings.mysql.host,
        user=settings.mysql.user,
        password=settings.mysql.password,
        database=settings.mysql.database,
        port=settings.mysql.port,
        charset=settings.mysql.charset,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


def get_mysql_connection():
    """Get a raw MySQL connection (for use without context manager)."""
    return pymysql.connect(
        host=settings.mysql.host,
        user=settings.mysql.user,
        password=settings.mysql.password,
        database=settings.mysql.database,
        port=settings.mysql.port,
        charset=settings.mysql.charset,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


# =============================================================================
# Dual-Write Operations
# =============================================================================

def dual_write_insert(
    table: str,
    data: Dict[str, Any],
    duckdb_name: str = "central"
) -> Tuple[bool, bool]:
    """
    Insert a record into both DuckDB and MySQL.
    
    Args:
        table: Table name
        data: Dictionary of column -> value
        duckdb_name: DuckDB database name
    
    Returns:
        Tuple of (duckdb_success, mysql_success)
    """
    columns = list(data.keys())
    values = list(data.values())
    placeholders_duckdb = ", ".join(["?" for _ in columns])
    placeholders_mysql = ", ".join(["%s" for _ in columns])
    columns_str = ", ".join(columns)
    
    duckdb_success = False
    mysql_success = False
    
    # Write to DuckDB
    try:
        with get_duckdb(duckdb_name) as conn:
            conn.execute(
                f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders_duckdb})",
                values
            )
            duckdb_success = True
    except Exception as e:
        logger.error(f"DuckDB insert error for {table}: {e}")
    
    # Write to MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders_mysql})",
                    values
                )
            mysql_success = True
    except Exception as e:
        logger.error(f"MySQL insert error for {table}: {e}")
    
    return duckdb_success, mysql_success


def dual_write_update(
    table: str,
    data: Dict[str, Any],
    where: Dict[str, Any],
    duckdb_name: str = "central"
) -> Tuple[bool, bool]:
    """
    Update records in both DuckDB and MySQL.
    
    Args:
        table: Table name
        data: Dictionary of column -> new value
        where: Dictionary of column -> value for WHERE clause
        duckdb_name: DuckDB database name
    
    Returns:
        Tuple of (duckdb_success, mysql_success)
    """
    set_cols = list(data.keys())
    set_values = list(data.values())
    where_cols = list(where.keys())
    where_values = list(where.values())
    
    set_str_duckdb = ", ".join([f"{col} = ?" for col in set_cols])
    where_str_duckdb = " AND ".join([f"{col} = ?" for col in where_cols])
    
    set_str_mysql = ", ".join([f"{col} = %s" for col in set_cols])
    where_str_mysql = " AND ".join([f"{col} = %s" for col in where_cols])
    
    all_values = set_values + where_values
    
    duckdb_success = False
    mysql_success = False
    
    # Update DuckDB
    try:
        with get_duckdb(duckdb_name) as conn:
            conn.execute(
                f"UPDATE {table} SET {set_str_duckdb} WHERE {where_str_duckdb}",
                all_values
            )
            duckdb_success = True
    except Exception as e:
        logger.error(f"DuckDB update error for {table}: {e}")
    
    # Update MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {table} SET {set_str_mysql} WHERE {where_str_mysql}",
                    all_values
                )
            mysql_success = True
    except Exception as e:
        logger.error(f"MySQL update error for {table}: {e}")
    
    return duckdb_success, mysql_success


def dual_write_delete(
    table: str,
    where: Dict[str, Any],
    duckdb_name: str = "central"
) -> Tuple[bool, bool]:
    """
    Delete records from both DuckDB and MySQL.
    
    Args:
        table: Table name
        where: Dictionary of column -> value for WHERE clause
        duckdb_name: DuckDB database name
    
    Returns:
        Tuple of (duckdb_success, mysql_success)
    """
    where_cols = list(where.keys())
    where_values = list(where.values())
    
    where_str_duckdb = " AND ".join([f"{col} = ?" for col in where_cols])
    where_str_mysql = " AND ".join([f"{col} = %s" for col in where_cols])
    
    duckdb_success = False
    mysql_success = False
    
    # Delete from DuckDB
    try:
        with get_duckdb(duckdb_name) as conn:
            conn.execute(f"DELETE FROM {table} WHERE {where_str_duckdb}", where_values)
            duckdb_success = True
    except Exception as e:
        logger.error(f"DuckDB delete error for {table}: {e}")
    
    # Delete from MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table} WHERE {where_str_mysql}", where_values)
            mysql_success = True
    except Exception as e:
        logger.error(f"MySQL delete error for {table}: {e}")
    
    return duckdb_success, mysql_success


# =============================================================================
# Smart Query Routing
# =============================================================================

def smart_query(
    table: str,
    columns: List[str] = None,
    where: Dict[str, Any] = None,
    order_by: str = None,
    limit: int = None,
    time_column: str = None,
    start_time: datetime = None,
    end_time: datetime = None,
    duckdb_name: str = "central"
) -> List[Dict[str, Any]]:
    """
    Smart query that routes to DuckDB for recent data (24hr) or MySQL for historical.
    
    Args:
        table: Table name
        columns: List of columns to select (None = all)
        where: Dictionary of column -> value for WHERE clause
        order_by: ORDER BY clause (e.g., "created_at DESC")
        limit: LIMIT clause
        time_column: Column name for time-based routing
        start_time: Query start time
        end_time: Query end time
        duckdb_name: DuckDB database name
    
    Returns:
        List of result dictionaries
    """
    # Determine which database to use
    use_duckdb = True
    cutoff_time = datetime.now() - timedelta(hours=settings.hot_storage_hours)
    
    if start_time and start_time < cutoff_time:
        # Query includes historical data - use MySQL
        use_duckdb = False
    
    # Build query
    cols_str = ", ".join(columns) if columns else "*"
    query_parts = [f"SELECT {cols_str} FROM {table}"]
    params = []
    
    where_clauses = []
    if where:
        for col, val in where.items():
            if use_duckdb:
                where_clauses.append(f"{col} = ?")
            else:
                where_clauses.append(f"{col} = %s")
            params.append(val)
    
    if time_column and start_time:
        if use_duckdb:
            where_clauses.append(f"{time_column} >= ?")
        else:
            where_clauses.append(f"{time_column} >= %s")
        params.append(start_time)
    
    if time_column and end_time:
        if use_duckdb:
            where_clauses.append(f"{time_column} <= ?")
        else:
            where_clauses.append(f"{time_column} <= %s")
        params.append(end_time)
    
    if where_clauses:
        query_parts.append("WHERE " + " AND ".join(where_clauses))
    
    if order_by:
        query_parts.append(f"ORDER BY {order_by}")
    
    if limit:
        query_parts.append(f"LIMIT {limit}")
    
    query = " ".join(query_parts)
    
    # Execute query
    if use_duckdb:
        with get_duckdb(duckdb_name) as conn:
            result = conn.execute(query, params).fetchall()
            columns_names = [desc[0] for desc in conn.description]
            return [dict(zip(columns_names, row)) for row in result]
    else:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()


# =============================================================================
# Archive Operations (Hot -> Cold)
# =============================================================================

def archive_old_data(table_name: str, db_name: str = "central", hours: int = 24):
    """
    Remove data older than specified hours from DuckDB hot storage.
    Data is kept in MySQL (master) so this is just cleanup.
    
    Args:
        table_name: Table name to clean up
        db_name: DuckDB database name
        hours: Age threshold in hours (default 24)
    """
    from features.price_api.schema import TIMESTAMP_COLUMNS, HOT_TABLES
    
    if table_name not in HOT_TABLES:
        print(f"Table {table_name} is not a hot table, skipping cleanup")
        return 0
    
    ts_col = TIMESTAMP_COLUMNS.get(table_name)
    if not ts_col:
        print(f"No timestamp column defined for {table_name}, skipping cleanup")
        return 0
    
    with get_duckdb(db_name) as conn:
        # Special handling for cycle_tracker: only delete completed cycles older than 24h
        if table_name == "cycle_tracker":
            # Count records to delete (only completed cycles)
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE cycle_end_time IS NOT NULL 
                AND cycle_end_time < NOW() - INTERVAL {hours} HOUR
            """).fetchone()
            count = count_result[0] if count_result else 0
            
            if count > 0:
                # Delete old completed cycles only
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE cycle_end_time IS NOT NULL 
                    AND cycle_end_time < NOW() - INTERVAL {hours} HOUR
                """)
                print(f"Cleaned up {count} old completed cycles from {table_name}")
        else:
            # Count records to delete
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR
            """).fetchone()
            count = count_result[0] if count_result else 0
            
            if count > 0:
                # Delete old records
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR
                """)
                print(f"Cleaned up {count} old records from {table_name}")
        
        return count


def cleanup_all_hot_tables(db_name: str = "central", hours: int = None):
    """
    Clean up all hot tables in DuckDB.
    
    Uses different retention periods:
    - Trades (follow_the_goat_buyins): 72 hours (settings.trades_hot_storage_hours)
    - Other tables: 24 hours (settings.hot_storage_hours)
    
    Args:
        db_name: DuckDB database name
        hours: Override hours for all tables (None = use per-table settings)
    """
    from features.price_api.schema import HOT_TABLES
    
    # Tables that use trades retention (72h)
    TRADES_TABLES = ['follow_the_goat_buyins', 'follow_the_goat_buyins_price_checks']
    
    total_cleaned = 0
    for table in HOT_TABLES:
        # Determine hours for this table
        if hours is not None:
            table_hours = hours
        elif table in TRADES_TABLES:
            table_hours = settings.trades_hot_storage_hours  # 72 hours for trades
        else:
            table_hours = settings.hot_storage_hours  # 24 hours for others
        
        cleaned = archive_old_data(table, db_name, table_hours)
        total_cleaned += cleaned
    
    return total_cleaned


# =============================================================================
# Database Initialization
# =============================================================================

def run_migrations(conn):
    """
    Run database migrations to handle schema updates.
    Safe to run multiple times.
    
    Two types of migrations:
    1. ADD_COLUMN: Add a single column if missing
    2. RECREATE: Drop and recreate table if schema is outdated (for in-memory DBs)
    """
    # Tables that should be dropped and recreated if schema is outdated
    # (safe for in-memory databases where data is repopulated on each start)
    tables_to_recreate_if_outdated = [
        ("buyin_trail_minutes", "minute"),  # Check for 'minute' column
    ]
    
    for table_name, required_column in tables_to_recreate_if_outdated:
        try:
            # Check if table exists and has the required column
            result = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM pragma_table_info('{table_name}')
                    WHERE name = '{required_column}'
                )
            """).fetchone()
            
            if result[0] == 0:
                # Table exists but missing required column - drop it
                # It will be recreated with correct schema by init_all_tables
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped outdated table {table_name} (missing {required_column} column)")
        except Exception as e:
            # Table doesn't exist yet, that's fine
            logger.debug(f"Migration check for {table_name}: {e}")
    
    # Individual column additions (for tables where we want to preserve data)
    column_migrations = [
        # Add fifteen_min_trail column if missing (added after initial schema)
        ("follow_the_goat_buyins", "fifteen_min_trail", "JSON"),
    ]
    
    for table_name, column_name, column_type in column_migrations:
        try:
            # Check if column exists using PRAGMA
            result = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT * FROM pragma_table_info('{table_name}')
                    WHERE name = '{column_name}'
                )
            """).fetchone()
            
            if result[0] == 0:
                # Column doesn't exist, add it
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                logger.info(f"Added missing column {column_name} to {table_name}")
        except Exception as e:
            # Table might not exist yet, skip migration
            logger.debug(f"Migration check for {table_name}.{column_name}: {e}")


def init_duckdb_tables(db_name: str = "central"):
    """Initialize all tables in DuckDB."""
    from features.price_api.schema import init_all_tables
    
    with get_duckdb(db_name) as conn:
        # Run migrations FIRST to drop outdated tables before creating new ones
        run_migrations(conn)
        # Then create all tables with correct schema
        init_all_tables(conn)


def ensure_tables_exist(db_name: str, schema_sql: str):
    """
    Run schema SQL to ensure tables exist.
    Idempotent - safe to call multiple times.
    """
    with get_duckdb(db_name) as conn:
        conn.execute(schema_sql)


# =============================================================================
# Trading Engine Access
# =============================================================================

def get_trading_engine():
    """
    Get the global TradingDataEngine singleton.
    
    The TradingDataEngine provides:
    - In-memory DuckDB for zero lock contention
    - Queue-based non-blocking writes
    - Instant reads
    - Background MySQL sync
    - Auto-cleanup of data older than 24h
    
    Usage:
        from core.database import get_trading_engine
        
        engine = get_trading_engine()
        engine.start()  # Start background threads
        
        # Non-blocking write
        engine.write('prices', {'ts': datetime.now(), 'token': 'SOL', 'price': 123.45})
        
        # Instant read
        results = engine.read("SELECT * FROM prices WHERE token = ?", ['SOL'])
        
        # Shutdown
        engine.stop()
    """
    from core.trading_engine import get_engine
    return get_engine()


def start_trading_engine():
    """Get and start the trading engine (convenience function)."""
    from core.trading_engine import start_engine
    return start_engine()


def stop_trading_engine():
    """Stop the trading engine."""
    from core.trading_engine import stop_engine
    stop_engine()

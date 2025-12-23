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
- DuckDB: Persistent connections (shared within process to avoid file locking)
- MySQL: Connection per request (pooled by pymysql)
"""

import duckdb
import pymysql
import threading
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta

from core.config import settings

# Project root
PROJECT_ROOT = Path(__file__).parent.parent

# DuckDB database paths
DATABASES = {
    "prices": PROJECT_ROOT / "000data_feeds" / "1_jupiter_get_prices" / "prices.duckdb",
    "central": PROJECT_ROOT / "000data_feeds" / "central.duckdb",
}


# =============================================================================
# DuckDB Connection Pool (Singleton per database)
# =============================================================================

class DuckDBPool:
    """
    Singleton connection pool for DuckDB databases.
    
    DuckDB only allows one connection per file at a time. By maintaining
    persistent connections, we avoid file locking issues when multiple
    components need to access the same database.
    
    Thread-safe: Uses locks to ensure only one thread accesses a connection at a time.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._connections = {}
                    cls._instance._conn_locks = {}
        return cls._instance
    
    def get_connection(self, name: str) -> duckdb.DuckDBPyConnection:
        """Get or create a persistent connection to a DuckDB database."""
        if name not in DATABASES:
            raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
        
        # Create lock for this database if it doesn't exist
        if name not in self._conn_locks:
            with self._lock:
                if name not in self._conn_locks:
                    self._conn_locks[name] = threading.Lock()
        
        # Get or create connection
        if name not in self._connections or self._connections[name] is None:
            with self._conn_locks[name]:
                if name not in self._connections or self._connections[name] is None:
                    db_path = DATABASES[name]
                    db_path.parent.mkdir(parents=True, exist_ok=True)
                    self._connections[name] = duckdb.connect(str(db_path))
        
        return self._connections[name]
    
    def get_lock(self, name: str) -> threading.Lock:
        """Get the lock for a specific database."""
        if name not in self._conn_locks:
            with self._lock:
                if name not in self._conn_locks:
                    self._conn_locks[name] = threading.Lock()
        return self._conn_locks[name]
    
    def close_all(self):
        """Close all connections (for shutdown)."""
        with self._lock:
            for name, conn in self._connections.items():
                if conn is not None:
                    try:
                        conn.close()
                    except:
                        pass
            self._connections.clear()
    
    def close(self, name: str):
        """Close a specific connection."""
        if name in self._connections:
            with self._conn_locks.get(name, self._lock):
                if name in self._connections and self._connections[name] is not None:
                    try:
                        self._connections[name].close()
                    except:
                        pass
                    self._connections[name] = None


# Global pool instance
_pool = DuckDBPool()


# =============================================================================
# DuckDB Connection Management
# =============================================================================

def get_db_path(name: str = "central") -> Path:
    """Get the path to a DuckDB database file."""
    if name not in DATABASES:
        raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
    return DATABASES[name]


@contextmanager
def get_duckdb(name: str = "central", read_only: bool = False):
    """
    Context manager for DuckDB connections using the shared pool.
    
    Uses persistent connections to avoid file locking issues.
    Thread-safe: acquires lock before yielding connection.
    
    Usage:
        with get_duckdb() as conn:
            result = conn.execute("SELECT * FROM table").fetchall()
    
    Args:
        name: Database name ("central" or "prices")
        read_only: Ignored (kept for backward compatibility)
    """
    conn = _pool.get_connection(name)
    lock = _pool.get_lock(name)
    
    # Acquire lock to ensure thread-safe access
    with lock:
        try:
            yield conn
        except Exception as e:
            # If there's a connection error, try to reconnect
            if "connection" in str(e).lower() or "closed" in str(e).lower():
                _pool.close(name)
                conn = _pool.get_connection(name)
                yield conn
            else:
                raise


@contextmanager
def get_duckdb_fresh(name: str = "central"):
    """
    Get a fresh (non-pooled) DuckDB connection.
    
    Use this only when you need a separate connection (e.g., for long-running operations).
    The connection will be closed when the context exits.
    """
    db_path = get_db_path(name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        yield conn
    finally:
        conn.close()


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
        print(f"DuckDB insert error for {table}: {e}")
    
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
        print(f"MySQL insert error for {table}: {e}")
    
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
        print(f"DuckDB update error for {table}: {e}")
    
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
        print(f"MySQL update error for {table}: {e}")
    
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
        print(f"DuckDB delete error for {table}: {e}")
    
    # Delete from MySQL
    try:
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {table} WHERE {where_str_mysql}", where_values)
            mysql_success = True
    except Exception as e:
        print(f"MySQL delete error for {table}: {e}")
    
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


def cleanup_all_hot_tables(db_name: str = "central", hours: int = 24):
    """Clean up all hot tables in DuckDB."""
    from features.price_api.schema import HOT_TABLES
    
    total_cleaned = 0
    for table in HOT_TABLES:
        cleaned = archive_old_data(table, db_name, hours)
        total_cleaned += cleaned
    
    return total_cleaned


# =============================================================================
# Database Initialization
# =============================================================================

def init_duckdb_tables(db_name: str = "central"):
    """Initialize all tables in DuckDB."""
    from features.price_api.schema import init_all_tables
    
    with get_duckdb(db_name) as conn:
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

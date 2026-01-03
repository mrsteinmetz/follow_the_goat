"""
Database Connection Manager
===========================
Central database access for DuckDB (hot storage) and PostgreSQL (archive).
See duckdb/ARCHITECTURE.md for schema documentation.

Architecture:
- DuckDB: In-memory hot storage for fast reads (PRIMARY - trading bot uses this)
- PostgreSQL: Archive database for expired data (local PostgreSQL)
- Archive-on-cleanup: Data is archived to PostgreSQL before being deleted from DuckDB

Connection Strategy:
- DuckDB: Pooled connection with WAL mode for concurrency
- PostgreSQL: Connection per request for archive operations only
"""

import duckdb
import psycopg2
import psycopg2.extras
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

# In-memory mode (requested): keep ALL DuckDB databases in RAM.
# Controlled by env DUCKDB_IN_MEMORY (default "1" = in-memory).
USE_IN_MEMORY = os.getenv("DUCKDB_IN_MEMORY", "1") == "1"

# DuckDB database targets
# - In-memory: we map names to the sentinel ":memory:" string
# - File-backed (legacy fallback): same paths as before
if USE_IN_MEMORY:
    DATABASES = {
        "prices": ":memory:",
        "central": ":memory:",
    }
    logger.info("DuckDB configured for in-memory databases (no file persistence).")
else:
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
                    cls._instance._external_registered = set()  # Track externally registered connections
        return cls._instance
    
    def get_connection(self, name: str) -> duckdb.DuckDBPyConnection:
        """Get or create a persistent connection to a DuckDB database."""
        if name not in DATABASES:
            raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
        
        # Fast path: return existing connection (skip health check for registered external connections)
        conn = self._connections.get(name)
        if conn is not None:
            # Check if this is an externally registered connection (from master2.py)
            # These connections are managed externally, so we trust them and skip health checks
            if name in self._external_registered:
                return conn
            
            # For pool-managed connections, do a health check
            try:
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
            
            # Create new connection (in-memory or file-backed)
            target = DATABASES[name]
            if USE_IN_MEMORY:
                conn = duckdb.connect(database=":memory:")
                logger.info(f"Created in-memory DuckDB connection '{name}'.")
            else:
                target_path = Path(target)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Creating DuckDB connection to '{name}' at {target_path}")
                conn = duckdb.connect(str(target_path))
                try:
                    conn.execute("PRAGMA enable_progress_bar=false")  # Disable progress bar for cleaner logs
                except Exception as e:
                    logger.debug(f"PRAGMA setting note: {e}")
            
            # Apply schema (idempotent per process)
            try:
                _apply_schema_if_needed(conn, name)
            except Exception:
                pass

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
_SCHEMA_INITIALIZED = {}

def _apply_schema_if_needed(conn, name: str):
    """Apply core schema once per process for a given DB."""
    if _SCHEMA_INITIALIZED.get(name):
        return
    try:
        from features.price_api import schema as schema_module
        schema_sqls = {
            k: v for k, v in schema_module.__dict__.items()
            if k.startswith("SCHEMA_") and isinstance(v, str)
        }
        for sql in schema_sqls.values():
            conn.execute(sql)
        _SCHEMA_INITIALIZED[name] = True
        logger.info(f"Applied schema to DuckDB '{name}' ({len(schema_sqls)} statements).")
    except Exception as e:
        logger.error(f"Failed to apply schema to DuckDB '{name}': {e}")
        raise


def _get_db_path(name: str):
    """Get the path for a named database (None when in-memory)."""
    if name not in DATABASES:
        raise ValueError(f"Unknown database: {name}. Available: {list(DATABASES.keys())}")
    if USE_IN_MEMORY:
        return None
    db_path = Path(DATABASES[name])
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


def get_db_path(name: str = "central"):
    """Get the path to a DuckDB database file (None when in-memory)."""
    return _get_db_path(name)


@contextmanager
def get_duckdb(name: str = "central", read_only: bool = False):
    """
    Context manager for DuckDB connections.
    
    AUTOMATICALLY uses TradingDataEngine (in-memory) when running under scheduler.
    Falls back to pooled DuckDB when engine is not available.
    
    IMPORTANT: For registered in-memory connections, uses locking to prevent
    memory corruption from concurrent access to the same connection object.
    
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
    
    # Check if this is a registered external connection with a lock
    if name in _pool._external_registered:
        conn = _pool.get_connection(name)
        lock = getattr(_pool, '_external_locks', {}).get(name)
        if lock:
            # Lock required for safe access to shared in-memory connection
            with lock:
                yield conn
            return
    
    # Fallback to pooled DuckDB (standalone mode or non-central DB)
    # Get connection OUTSIDE try/except to avoid "generator didn't stop after throw()"
    # The yield must be outside exception handlers for proper generator cleanup
    conn = None
    try:
        conn = _pool.get_connection(name)
    except Exception as e:
        error_str = str(e).lower()
        # Handle connection errors by reconnecting
        if "connection" in error_str or "closed" in error_str or "file handle" in error_str or "attach" in error_str:
            logger.warning(f"DuckDB connection error, reconnecting: {e}")
            _pool.close(name)
            conn = _pool.get_connection(name)
        else:
            raise
    
    # Yield OUTSIDE try/except to prevent generator cleanup issues
    yield conn


@contextmanager
def get_duckdb_fresh(name: str = "central"):
    """
    Get a FRESH DuckDB connection (not pooled).
    
    In in-memory mode, a truly fresh connection would be empty; to avoid
    surprising data loss, we fall back to the pooled connection when
    USE_IN_MEMORY is enabled.
    """
    if USE_IN_MEMORY:
        # Reuse pooled connection to preserve in-memory state
        conn = _pool.get_connection(name)
        try:
            yield conn
        finally:
            # Do not close pooled connection
            pass
    else:
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


def register_connection(name: str, conn, lock=None):
    """
    Register an external DuckDB connection into the pool.
    
    This allows master2.py to inject its local in-memory DuckDB so that
    all modules using get_duckdb("central") will use the same connection.
    
    IMPORTANT: In-memory DuckDB connections MUST be protected with a lock
    even on Linux, because the same connection object cannot handle truly
    concurrent operations without memory corruption.
    
    Args:
        name: Database name (e.g., "central")
        conn: DuckDB connection object
        lock: threading.Lock() for thread-safe access (REQUIRED for in-memory)
    """
    _pool._connections[name] = conn
    _pool._external_registered.add(name)  # Mark as externally managed (skip health checks)
    
    # Store the lock in a separate dict (can't attach to DuckDB connection object)
    if not hasattr(_pool, '_external_locks'):
        _pool._external_locks = {}
    _pool._external_locks[name] = lock
    
    logger.info(f"Registered external DuckDB connection as '{name}'")


def close_all_duckdb():
    """Close all DuckDB connections (for shutdown)."""
    _pool.close_all()


# =============================================================================
# PostgreSQL Connection Management (Archive Database Only)
# =============================================================================

# Flag to track if PostgreSQL archive is available
_postgres_available = None


def _check_postgres_available() -> bool:
    """Check if PostgreSQL archive database is available.
    
    On first call, actually tests the connection.
    Subsequent calls return cached value for performance.
    """
    global _postgres_available
    
    # Return cached value immediately (non-blocking)
    if _postgres_available is not None:
        return _postgres_available
    
    # Not configured = not available
    if not settings.postgres.password:
        _postgres_available = False
        return False
    
    # First call: actually test the connection
    try:
        conn = psycopg2.connect(
            host=settings.postgres.host,
            user=settings.postgres.user,
            password=settings.postgres.password,
            database=settings.postgres.database,
            port=settings.postgres.port,
            connect_timeout=3
        )
        conn.close()
        _postgres_available = True
        logger.info("PostgreSQL archive database connected successfully")
        return True
    except Exception as e:
        logger.warning(f"PostgreSQL archive not available: {e}")
        _postgres_available = False
        return False


@contextmanager
def get_postgres():
    """
    Context manager for PostgreSQL archive connections.
    
    Usage:
        with get_postgres() as conn:
            if conn:  # May be None if PostgreSQL not available
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO table_archive ...")
    
    Returns None if PostgreSQL is not configured or unavailable.
    """
    if not _check_postgres_available():
        yield None
        return
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=settings.postgres.host,
            user=settings.postgres.user,
            password=settings.postgres.password,
            database=settings.postgres.database,
            port=settings.postgres.port,
            cursor_factory=psycopg2.extras.RealDictCursor,
            connect_timeout=3,   # Short timeout - archive is not critical
        )
        conn.autocommit = True
        yield conn
    except Exception as e:
        logger.debug(f"PostgreSQL archive connection error (non-critical): {e}")
        yield None
    finally:
        if conn:
            conn.close()


def get_postgres_connection():
    """Get a raw PostgreSQL connection for archive operations.
    
    Returns None if PostgreSQL is not available.
    Short timeouts ensure this never blocks trading operations.
    """
    if not _check_postgres_available():
        return None
    
    try:
        conn = psycopg2.connect(
            host=settings.postgres.host,
            user=settings.postgres.user,
            password=settings.postgres.password,
            database=settings.postgres.database,
            port=settings.postgres.port,
            cursor_factory=psycopg2.extras.RealDictCursor,
            connect_timeout=3,   # Short timeout - archive is not critical
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.debug(f"PostgreSQL archive connection failed (non-critical): {e}")
        return None


# Legacy aliases for backward compatibility
@contextmanager
def get_mysql():
    """Legacy alias for get_postgres()."""
    with get_postgres() as conn:
        yield conn


def get_mysql_connection():
    """Legacy alias for get_postgres_connection()."""
    return get_postgres_connection()


# =============================================================================
# DuckDB-Only Write Operations (PostgreSQL is archive-only)
# =============================================================================

def duckdb_insert(
    table: str,
    data: Dict[str, Any],
    duckdb_name: str = "central"
) -> bool:
    """
    Insert a record into DuckDB.
    
    Args:
        table: Table name
        data: Dictionary of column -> value
        duckdb_name: DuckDB database name
    
    Returns:
        True if successful, False otherwise
    """
    columns = list(data.keys())
    values = list(data.values())
    placeholders = ", ".join(["?" for _ in columns])
    columns_str = ", ".join(columns)
    
    try:
        with get_duckdb(duckdb_name) as conn:
            conn.execute(
                f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})",
                values
            )
            return True
    except Exception as e:
        logger.error(f"DuckDB insert error for {table}: {e}")
        return False


def duckdb_update(
    table: str,
    data: Dict[str, Any],
    where: Dict[str, Any],
    duckdb_name: str = "central"
) -> bool:
    """
    Update records in DuckDB.
    
    Args:
        table: Table name
        data: Dictionary of column -> new value
        where: Dictionary of column -> value for WHERE clause
        duckdb_name: DuckDB database name
    
    Returns:
        True if successful, False otherwise
    """
    set_cols = list(data.keys())
    set_values = list(data.values())
    where_cols = list(where.keys())
    where_values = list(where.values())
    
    set_str = ", ".join([f"{col} = ?" for col in set_cols])
    where_str = " AND ".join([f"{col} = ?" for col in where_cols])
    all_values = set_values + where_values
    
    try:
        with get_duckdb(duckdb_name) as conn:
            conn.execute(
                f"UPDATE {table} SET {set_str} WHERE {where_str}",
                all_values
            )
            return True
    except Exception as e:
        logger.error(f"DuckDB update error for {table}: {e}")
        return False


def duckdb_query(
    table: str,
    columns: List[str] = None,
    where: Dict[str, Any] = None,
    order_by: str = None,
    limit: int = None,
    duckdb_name: str = "central"
) -> List[Dict[str, Any]]:
    """
    Query DuckDB (primary database for all reads).
    
    Args:
        table: Table name
        columns: List of columns to select (None = all)
        where: Dictionary of column -> value for WHERE clause
        order_by: ORDER BY clause (e.g., "created_at DESC")
        limit: LIMIT clause
        duckdb_name: DuckDB database name
    
    Returns:
        List of result dictionaries
    """
    cols_str = ", ".join(columns) if columns else "*"
    query_parts = [f"SELECT {cols_str} FROM {table}"]
    params = []
    
    if where:
        where_clauses = [f"{col} = ?" for col in where.keys()]
        query_parts.append("WHERE " + " AND ".join(where_clauses))
        params.extend(where.values())
    
    if order_by:
        query_parts.append(f"ORDER BY {order_by}")
    
    if limit:
        query_parts.append(f"LIMIT {limit}")
    
    query = " ".join(query_parts)
    
    with get_duckdb(duckdb_name) as conn:
        result = conn.execute(query, params).fetchall()
        columns_names = [desc[0] for desc in conn.description]
        return [dict(zip(columns_names, row)) for row in result]


# =============================================================================
# Archive Operations (Hot -> Cold with ASYNC PostgreSQL Archive)
# =============================================================================
# CRITICAL: PostgreSQL archive runs in background thread - NEVER blocks trading!

import threading
from queue import Queue
import copy

# Background archive queue and worker thread
_archive_queue = Queue()
_archive_worker_running = False
_archive_worker_thread = None


def _archive_worker():
    """Background worker that processes PostgreSQL archive operations.
    
    Runs in a separate thread so it NEVER blocks the main trading system.
    If PostgreSQL is slow or down, archives are simply dropped (they're not critical).
    """
    global _archive_worker_running, _postgres_available
    
    while _archive_worker_running:
        try:
            # Wait for archive job with timeout (allows clean shutdown)
            try:
                table_name, rows = _archive_queue.get(timeout=1.0)
            except:
                continue
            
            if not rows:
                continue
                
            # Use same table name in PostgreSQL (no _archive suffix in V2)
            archive_table = table_name
            
            # Try to archive - if it fails, just log and continue
            try:
                with get_postgres() as conn:
                    if not conn:
                        _postgres_available = False
                        logger.debug(f"PostgreSQL not available, dropping archive for {table_name} ({len(rows)} rows)")
                        continue
                    
                    columns = list(rows[0].keys())
                    placeholders = ", ".join(["%s" for _ in columns])
                    columns_str = ", ".join(columns)
                    
                    archived = 0
                    with conn.cursor() as cursor:
                        for row in rows:
                            try:
                                values = []
                                for col in columns:
                                    val = row.get(col)
                                    if hasattr(val, 'strftime'):
                                        val = val.strftime('%Y-%m-%d %H:%M:%S')
                                    values.append(val)
                                
                                # PostgreSQL uses ON CONFLICT DO NOTHING instead of INSERT IGNORE
                                cursor.execute(
                                    f"INSERT INTO {archive_table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                                    values
                                )
                                archived += 1
                            except:
                                pass  # Silently skip failed rows
                    
                    if archived > 0:
                        _postgres_available = True  # Mark PostgreSQL as working
                        logger.info(f"[ASYNC] Archived {archived} rows to {archive_table}")
                        
            except Exception as e:
                _postgres_available = False
                logger.debug(f"[ASYNC] Archive to PostgreSQL failed (non-critical): {e}")
                
        except Exception as e:
            logger.debug(f"Archive worker error: {e}")


def _start_archive_worker():
    """Start the background archive worker if not already running."""
    global _archive_worker_running, _archive_worker_thread
    
    if _archive_worker_running and _archive_worker_thread and _archive_worker_thread.is_alive():
        return
    
    _archive_worker_running = True
    _archive_worker_thread = threading.Thread(
        target=_archive_worker,
        name="PostgreSQL-Archive-Worker",
        daemon=True  # Dies with main process
    )
    _archive_worker_thread.start()
    logger.info("Started background PostgreSQL archive worker thread")


def _stop_archive_worker():
    """Stop the background archive worker."""
    global _archive_worker_running
    _archive_worker_running = False


def _archive_to_postgres_async(table_name: str, rows: List[Dict[str, Any]]):
    """
    Queue rows for async archive to PostgreSQL - NEVER BLOCKS.
    
    This is fire-and-forget. If PostgreSQL is slow/down, rows are dropped.
    Trading speed is more important than historical archives.
    
    Args:
        table_name: Table name (same name used in both DuckDB and PostgreSQL)
        rows: List of row dictionaries to archive
    """
    if not rows:
        return
    
    # Start worker if needed
    _start_archive_worker()
    
    # Deep copy rows to avoid any reference issues
    rows_copy = copy.deepcopy(rows)
    
    # Queue for background processing - non-blocking
    try:
        _archive_queue.put_nowait((table_name, rows_copy))
        logger.debug(f"Queued {len(rows)} rows for async archive to {table_name}")
    except:
        # Queue full - drop the archive (speed > archives)
        logger.debug(f"Archive queue full, dropping {len(rows)} rows for {table_name}")


# Legacy alias for backward compatibility
def _archive_to_mysql_async(table_name: str, rows: List[Dict[str, Any]]):
    """Legacy alias for _archive_to_postgres_async()."""
    _archive_to_postgres_async(table_name, rows)


def archive_old_data(table_name: str, db_name: str = "central", hours: int = 24):
    """
    Delete old data from DuckDB and queue for async PostgreSQL archive.
    
    CRITICAL: DuckDB cleanup happens IMMEDIATELY. PostgreSQL archive is fire-and-forget
    in a background thread. Trading speed is NEVER compromised.
    
    Process:
    1. SELECT data older than threshold from DuckDB
    2. DELETE from DuckDB IMMEDIATELY (speed first!)
    3. Queue data for async PostgreSQL archive (background thread)
    
    Args:
        table_name: Table name to clean up
        db_name: DuckDB database name
        hours: Age threshold in hours (default 24)
    
    Returns:
        Number of records cleaned up from DuckDB
    """
    from features.price_api.schema import TIMESTAMP_COLUMNS, HOT_TABLES
    
    if table_name not in HOT_TABLES:
        logger.debug(f"Table {table_name} is not a hot table, skipping cleanup")
        return 0
    
    ts_col = TIMESTAMP_COLUMNS.get(table_name)
    if not ts_col:
        logger.debug(f"No timestamp column defined for {table_name}, skipping cleanup")
        return 0
    
    with get_duckdb(db_name) as conn:
        # Special handling for cycle_tracker: only process completed cycles
        if table_name == "cycle_tracker":
            # Select old completed cycles for archiving
            rows = conn.execute(f"""
                SELECT * FROM {table_name}
                WHERE cycle_end_time IS NOT NULL 
                AND cycle_end_time < NOW() - INTERVAL {hours} HOUR
            """).fetchall()
            
            if rows:
                # Convert to list of dicts
                columns = [desc[0] for desc in conn.description]
                rows_dict = [dict(zip(columns, row)) for row in rows]
                
                # DELETE FROM DUCKDB FIRST (speed priority!)
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE cycle_end_time IS NOT NULL 
                    AND cycle_end_time < NOW() - INTERVAL {hours} HOUR
                """)
                logger.info(f"Cleaned up {len(rows)} old completed cycles from {table_name}")
                
                # Queue for async PostgreSQL archive (fire-and-forget, never blocks)
                _archive_to_postgres_async(table_name, rows_dict)
                
                return len(rows)
        else:
            # Select old records for archiving
            rows = conn.execute(f"""
                SELECT * FROM {table_name}
                WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR
            """).fetchall()
            
            if rows:
                # Convert to list of dicts
                columns = [desc[0] for desc in conn.description]
                rows_dict = [dict(zip(columns, row)) for row in rows]
                
                # DELETE FROM DUCKDB FIRST (speed priority!)
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE {ts_col} < NOW() - INTERVAL {hours} HOUR
                """)
                logger.info(f"Cleaned up {len(rows)} old records from {table_name}")
                
                # Queue for async PostgreSQL archive (fire-and-forget, never blocks)
                _archive_to_postgres_async(table_name, rows_dict)
                
                return len(rows)
        
        return 0


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
    - Background PostgreSQL sync for archiving
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

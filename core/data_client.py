"""
Data Engine Client - API Client for TradingDataEngine Access
============================================================
Helper functions for accessing the Data Engine API from master2.py.

This module provides simple functions for:
- insert(table, data) - Queue a write to DuckDB
- insert_sync(table, data) - Insert and get ID immediately
- query(sql, params) - Execute a SELECT query
- get_backfill(table, hours) - Get historical data for startup
- get_latest(table, limit) - Get latest records

Usage:
    from core.data_client import DataClient
    
    client = DataClient()  # Uses default http://localhost:5050
    
    # Insert data
    client.insert("prices", {"ts": datetime.now(), "token": "SOL", "price": 123.45})
    
    # Query data
    results = client.query("SELECT * FROM prices WHERE token = ?", ["SOL"])
    
    # Get backfill data on startup
    prices = client.get_backfill("prices", hours=2)
"""

import requests
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger("data_client")

# Default API URL
DEFAULT_API_URL = "http://localhost:5050"


class DataClient:
    """
    Client for the Data Engine API.
    
    Provides methods for interacting with the TradingDataEngine
    running in master.py via HTTP API.
    """
    
    def __init__(self, base_url: str = DEFAULT_API_URL, timeout: float = 5.0):
        """
        Initialize the data client.
        
        Args:
            base_url: Base URL of the Data Engine API (default: http://localhost:5050)
            timeout: Request timeout in seconds (default: 5.0)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check if the Data Engine is healthy.
        
        Returns:
            Health status dictionary with engine stats
        
        Raises:
            ConnectionError: If API is not reachable
        """
        try:
            response = self._session.get(
                f"{self.base_url}/health",
                timeout=self.timeout
            )
            return response.json()
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Data Engine API not reachable: {e}")
    
    def is_available(self) -> bool:
        """
        Check if the Data Engine API is available.
        
        Returns:
            True if API is reachable and engine is running
        """
        try:
            health = self.health_check()
            return health.get("engine_running", False)
        except:
            return False
    
    def insert(self, table: str, data: Dict[str, Any]) -> bool:
        """
        Queue a write operation to DuckDB (non-blocking).
        
        The write is queued and processed asynchronously.
        Returns immediately without waiting for completion.
        
        Args:
            table: Table name
            data: Dictionary of column -> value
        
        Returns:
            True if queued successfully
        
        Raises:
            ValueError: If table is unknown
            ConnectionError: If API is not reachable
        """
        # Serialize datetime objects
        serialized_data = self._serialize_data(data)
        
        try:
            response = self._session.post(
                f"{self.base_url}/insert",
                json={"table": table, "data": serialized_data},
                timeout=self.timeout
            )
            
            if response.status_code == 400:
                raise ValueError(response.json().get("detail", "Invalid request"))
            
            response.raise_for_status()
            result = response.json()
            return result.get("success", False)
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Insert failed: {e}")
    
    def insert_batch(self, table: str, records: List[Dict[str, Any]]) -> int:
        """
        Queue multiple write operations (non-blocking).
        
        Args:
            table: Table name
            records: List of dictionaries to insert
        
        Returns:
            Number of records queued
        """
        serialized_records = [self._serialize_data(r) for r in records]
        
        try:
            response = self._session.post(
                f"{self.base_url}/insert/batch",
                json={"table": table, "records": serialized_records},
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            return result.get("queued_count", 0)
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Batch insert failed: {e}")
    
    def insert_sync(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a record synchronously and return the generated ID.
        
        Use this when you need the ID immediately after insert.
        
        Args:
            table: Table name
            data: Dictionary of column -> value
        
        Returns:
            Generated record ID
        """
        serialized_data = self._serialize_data(data)
        
        try:
            response = self._session.post(
                f"{self.base_url}/insert/sync",
                json={"table": table, "data": serialized_data},
                timeout=self.timeout
            )
            
            if response.status_code == 400:
                raise ValueError(response.json().get("detail", "Invalid request"))
            
            response.raise_for_status()
            result = response.json()
            return result.get("id")
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Sync insert failed: {e}")
    
    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            sql: SQL query string (SELECT only)
            params: Optional query parameters
        
        Returns:
            List of result dictionaries
        
        Raises:
            ValueError: If query is not a SELECT
            ConnectionError: If API is not reachable
        """
        try:
            response = self._session.post(
                f"{self.base_url}/query",
                json={"sql": sql, "params": params or []},
                timeout=self.timeout * 2  # Allow more time for queries
            )
            
            if response.status_code == 400:
                raise ValueError(response.json().get("detail", "Invalid query"))
            
            response.raise_for_status()
            result = response.json()
            return result.get("results", [])
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Query failed: {e}")
    
    def get_backfill(
        self,
        table: str,
        hours: int = None,
        minutes: int = None,
        limit: int = None  # Changed: None means no limit
    ) -> List[Dict[str, Any]]:
        """
        Get historical data for backfill on startup.

        This is typically used by master2.py to load recent data when starting.
        Use hours for startup backfill, minutes for continuous sync.

        Args:
            table: Table name
            hours: Hours of data to retrieve (1-24, for startup backfill)
            minutes: Minutes of data to retrieve (1-60, for continuous sync)
            limit: Maximum records to return (None = no limit, gets ALL data in time window)

        Returns:
            List of records from the specified time range
        """
        try:
            # Build params - use minutes for short intervals, hours for longer
            params = {}
            if limit is not None:
                params["limit"] = limit
            if minutes is not None:
                params["minutes"] = minutes
            elif hours is not None:
                params["hours"] = hours
            else:
                params["hours"] = 2  # Default to 2 hours
            
            response = self._session.get(
                f"{self.base_url}/backfill/{table}",
                params=params,
                timeout=self.timeout * 3  # Allow more time for backfill
            )
            
            if response.status_code == 400:
                raise ValueError(response.json().get("detail", "Invalid table"))
            
            response.raise_for_status()
            result = response.json()
            # Support both 'results' (old) and 'records' (new) keys
            return result.get("records", result.get("results", []))
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Backfill failed: {e}")
    
    def get_latest(
        self, 
        table: str, 
        limit: int = 100, 
        token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the latest records from a table.
        
        Args:
            table: Table name
            limit: Number of records to return
            token: Optional token filter (for prices/trades)
        
        Returns:
            List of latest records
        """
        try:
            params = {"limit": limit}
            if token:
                params["token"] = token
            
            response = self._session.get(
                f"{self.base_url}/latest/{table}",
                params=params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            return result.get("results", [])
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Get latest failed: {e}")
    
    def get_new_since_id(
        self,
        table: str,
        since_id: int = 0,
        limit: int = 1000
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Get NEW records since a specific ID (for incremental sync).
        
        This is the most efficient way to sync - only fetches records
        that haven't been synced yet based on ID.
        
        Args:
            table: Table name
            since_id: Get records with ID greater than this value
            limit: Maximum records to return
        
        Returns:
            Tuple of (records, max_id) where max_id is the highest ID returned
        """
        try:
            response = self._session.get(
                f"{self.base_url}/sync/{table}",
                params={"since_id": since_id, "limit": limit},
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            records = result.get("records", [])
            max_id = result.get("max_id", since_id)
            return records, max_id
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Incremental sync failed: {e}")
    
    def get_price(self, token: str = "SOL") -> Optional[float]:
        """
        Get the current price of a token.
        
        Args:
            token: Token symbol (default: SOL)
        
        Returns:
            Current price or None if not available
        """
        try:
            response = self._session.get(
                f"{self.base_url}/price/{token}",
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            return result.get("price")
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Get price failed: {e}")
            return None
    
    def list_tables(self) -> Dict[str, int]:
        """
        List all tables and their row counts.
        
        Returns:
            Dictionary of table_name -> row_count
        """
        try:
            response = self._session.get(
                f"{self.base_url}/tables",
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            return result.get("tables", {})
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"List tables failed: {e}")
    
    def _serialize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize data for JSON (handle datetime, etc.)."""
        result = {}
        for key, value in data.items():
            if hasattr(value, 'isoformat'):
                result[key] = value.isoformat()
            elif isinstance(value, bytes):
                result[key] = value.decode('utf-8', errors='replace')
            else:
                result[key] = value
        return result
    
    def close(self):
        """Close the session."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()


# =============================================================================
# Module-Level Convenience Functions
# =============================================================================

# Default client instance
_default_client: Optional[DataClient] = None


def get_client(base_url: str = DEFAULT_API_URL) -> DataClient:
    """Get or create the default client instance."""
    global _default_client
    if _default_client is None:
        _default_client = DataClient(base_url)
    return _default_client


def insert(table: str, data: Dict[str, Any]) -> bool:
    """Queue a write to DuckDB (convenience function)."""
    return get_client().insert(table, data)


def insert_sync(table: str, data: Dict[str, Any]) -> int:
    """Insert and return ID (convenience function)."""
    return get_client().insert_sync(table, data)


def query(sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Execute a SELECT query (convenience function)."""
    return get_client().query(sql, params)


def get_backfill(table: str, hours: int = None, minutes: int = None) -> List[Dict[str, Any]]:
    """Get backfill data (convenience function)."""
    return get_client().get_backfill(table, hours=hours, minutes=minutes)


def get_latest(table: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get latest records (convenience function)."""
    return get_client().get_latest(table, limit)


def get_price(token: str = "SOL") -> Optional[float]:
    """Get current price (convenience function)."""
    return get_client().get_price(token)


# =============================================================================
# Test / Demo
# =============================================================================

if __name__ == "__main__":
    import sys
    
    print("Testing Data Engine Client")
    print("=" * 60)
    
    client = DataClient()
    
    # Test health check
    print("\n1. Health Check:")
    try:
        health = client.health_check()
        print(f"   Status: {health.get('status')}")
        print(f"   Engine Running: {health.get('engine_running')}")
    except ConnectionError as e:
        print(f"   ERROR: {e}")
        print("\n   Make sure master.py is running!")
        sys.exit(1)
    
    # Test list tables
    print("\n2. List Tables:")
    try:
        tables = client.list_tables()
        for table, count in tables.items():
            print(f"   {table}: {count} rows")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test get price
    print("\n3. Get SOL Price:")
    try:
        price = client.get_price("SOL")
        print(f"   SOL Price: ${price}")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    # Test query
    print("\n4. Sample Query (last 5 prices):")
    try:
        results = client.query("SELECT * FROM prices ORDER BY ts DESC LIMIT 5")
        for row in results:
            print(f"   {row}")
    except Exception as e:
        print(f"   ERROR: {e}")
    
    print("\n" + "=" * 60)
    print("Client test complete!")

"""
Engine Client - HTTP client for accessing master2.py's Local API
=================================================================
When running outside of master2.py (e.g., website_api.py), this client
provides access to the local DuckDB via HTTP calls to port 5052.

Architecture:
    master.py (port 5050) - Data Engine (raw data ingestion)
    master2.py (port 5052) - Trading logic + Local API (computed data)
    website_api.py uses this client to call master2.py's Local API

Usage:
    from core.engine_client import get_engine_client
    
    client = get_engine_client()
    results = client.query("SELECT * FROM prices LIMIT 10")
"""

import requests
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Singleton instance
_client_instance = None

# Master2 Local API URL (computed data: cycles, profiles, etc.)
DATA_ENGINE_URL = "http://127.0.0.1:5052"


class EngineClient:
    """HTTP client for master.py's Data Engine API."""
    
    def __init__(self, base_url: str = DATA_ENGINE_URL, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self._session = requests.Session()
    
    def is_available(self) -> bool:
        """Check if the Data Engine is available."""
        try:
            resp = self._session.get(f"{self.base_url}/health", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('status') in ('ok', 'degraded')
            return False
        except Exception:
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """Get health status from Data Engine."""
        try:
            resp = self._session.get(f"{self.base_url}/health", timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query via the Data Engine API.
        
        Args:
            sql: SQL SELECT query
            params: Optional query parameters
        
        Returns:
            List of result dictionaries
        """
        try:
            resp = self._session.post(
                f"{self.base_url}/query",
                json={"sql": sql, "params": params or []},
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("success"):
                return data.get("results", [])
            else:
                logger.error(f"Query failed: {data.get('error')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Data Engine request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
    
    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Optional[Dict[str, Any]]:
        """Execute a query and return the first result."""
        results = self.query(sql, params)
        return results[0] if results else None
    
    def get_tables(self) -> Dict[str, int]:
        """Get table names and row counts."""
        try:
            resp = self._session.get(f"{self.base_url}/tables", timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            return data.get("tables", {})
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return {}
    
    def get_latest(self, table: str, limit: int = 100, token: str = None) -> List[Dict[str, Any]]:
        """Get latest records from a table."""
        try:
            params = {"limit": limit}
            if token:
                params["token"] = token
            
            resp = self._session.get(
                f"{self.base_url}/latest/{table}",
                params=params,
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            logger.error(f"Failed to get latest from {table}: {e}")
            return []
    
    def get_price(self, token: str = "SOL") -> Optional[float]:
        """Get current price for a token."""
        try:
            resp = self._session.get(
                f"{self.base_url}/price/{token}",
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("price")
        except Exception as e:
            logger.error(f"Failed to get price for {token}: {e}")
            return None
    
    def get_backfill(self, table: str, hours: int = 2, limit: int = 10000) -> List[Dict[str, Any]]:
        """Get historical data for a table."""
        try:
            resp = self._session.get(
                f"{self.base_url}/backfill/{table}",
                params={"hours": hours, "limit": limit},
                timeout=self.timeout
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            logger.error(f"Failed to get backfill for {table}: {e}")
            return []
    
    def insert(self, table: str, data: Dict[str, Any]) -> bool:
        """Insert a record (queued, non-blocking)."""
        try:
            resp = self._session.post(
                f"{self.base_url}/insert",
                json={"table": table, "data": data},
                timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json().get("success", False)
        except Exception as e:
            logger.error(f"Failed to insert into {table}: {e}")
            return False
    
    def insert_sync(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """Insert a record synchronously and return the ID."""
        try:
            resp = self._session.post(
                f"{self.base_url}/insert/sync",
                json={"table": table, "data": data},
                timeout=self.timeout
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("success"):
                return result.get("id")
            return None
        except Exception as e:
            logger.error(f"Failed to sync insert into {table}: {e}")
            return None
    
    def close(self):
        """Close the HTTP session."""
        self._session.close()


def get_engine_client() -> EngineClient:
    """Get or create the singleton EngineClient instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = EngineClient()
    return _client_instance


def is_engine_available() -> bool:
    """Check if the Data Engine API is available."""
    return get_engine_client().is_available()

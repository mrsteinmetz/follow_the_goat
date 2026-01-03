"""
Webhook Client - Fetch live data from .NET Webhook DuckDB In-Memory API

This client reads from the .NET webhook's in-memory DuckDB (24hr hot storage).
Data source: http://195.201.84.5/api/

Usage:
    from core.webhook_client import WebhookClient
    
    client = WebhookClient()
    
    # Get whale movements (latest 100)
    whale_data = client.get_whale_movements(limit=100)
    
    # Get trades with time range (for 15-minute trail)
    from datetime import datetime, timedelta
    end = datetime.now()
    start = end - timedelta(minutes=15)
    trades = client.get_trades(start_time=start, end_time=end)
    
    # Check health
    health = client.get_health()
"""

import requests
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger("webhook_client")


class WebhookClient:
    """Client for reading live data from .NET Webhook DuckDB In-Memory API."""
    
    # Note: Webhook is on HTTP only (no SSL binding on server)
    DEFAULT_URL = "http://195.201.84.5"
    
    def __init__(self, base_url: str = None, timeout: int = 10):
        """
        Initialize the webhook client.
        
        Args:
            base_url: Webhook API base URL (default: http://195.201.84.5)
            timeout: Request timeout in seconds
        """
        self.base_url = (base_url or self.DEFAULT_URL).rstrip('/')
        self.timeout = timeout
    
    def is_available(self) -> bool:
        """Check if the webhook API is available."""
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=3,
                verify=False
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def get_health(self) -> Optional[Dict[str, Any]]:
        """
        Get webhook health status.
        
        Returns:
            Health data dict or None if unavailable
        """
        try:
            response = requests.get(
                f"{self.base_url}/health",
                timeout=self.timeout,
                verify=False
            )
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return None
    
    def get_whale_movements(
        self, 
        limit: Optional[int] = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get whale movements from DuckDB in-memory hot storage.
        
        Args:
            limit: Maximum number of records (None = no limit)
            start_time: Optional start of time window (inclusive)
            end_time: Optional end of time window (inclusive)
            
        Returns:
            List of whale movement records
        """
        try:
            # Build query params
            params = {}
            
            # Add time range if specified
            if start_time:
                params["start"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
            if end_time:
                params["end"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Only add limit if explicitly specified
            if limit is not None:
                params["limit"] = limit
            
            response = requests.get(
                f"{self.base_url}/api/whale-movements",
                params=params,
                timeout=self.timeout,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    results = data.get("results", [])
                    logger.debug(f"Got {len(results)} whale movements from webhook API")
                    return results
                else:
                    logger.warning(f"API error: {data.get('error')}")
            return []
        except Exception as e:
            logger.error(f"Failed to get whale movements: {e}")
            return []
    
    def get_trades(
        self, 
        limit: Optional[int] = 100,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get trades from DuckDB in-memory hot storage.
        
        Args:
            limit: Maximum number of records (None = no limit)
            start_time: Optional start of time window (inclusive)
            end_time: Optional end of time window (inclusive)
            
        Returns:
            List of trade records
        """
        try:
            # Build query params
            params = {}
            
            # Add time range if specified
            if start_time:
                params["start"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
            if end_time:
                params["end"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
            
            # Only add limit if explicitly specified
            if limit is not None:
                params["limit"] = limit
            
            response = requests.get(
                f"{self.base_url}/api/trades",
                params=params,
                timeout=self.timeout,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    results = data.get("results", [])
                    logger.debug(f"Got {len(results)} trades from webhook API")
                    return results
                else:
                    logger.warning(f"API error: {data.get('error')}")
            return []
        except Exception as e:
            logger.error(f"Failed to get trades: {e}")
            return []
    
    def get_duckdb_stats(self) -> Dict[str, int]:
        """
        Get DuckDB in-memory storage stats.
        
        Returns:
            Dict with trades_count and whale_count
        """
        health = self.get_health()
        if health and "duckdb" in health:
            return {
                "trades_count": health["duckdb"].get("trades_in_hot_storage", 0),
                "whale_count": health["duckdb"].get("whale_movements_in_hot_storage", 0),
                "retention": health["duckdb"].get("retention", "24 hours")
            }
        return {"trades_count": 0, "whale_count": 0, "retention": "unknown"}


# Convenience function for quick access
def get_client(base_url: str = None) -> WebhookClient:
    """Get a webhook client instance."""
    return WebhookClient(base_url)


if __name__ == "__main__":
    # Quick test
    import warnings
    from datetime import timedelta
    warnings.filterwarnings("ignore")  # Suppress SSL warnings for test
    
    client = WebhookClient()
    
    print("=== Webhook Client Test ===\n")
    
    if client.is_available():
        print("Webhook API is available\n")
        
        # Get stats
        stats = client.get_duckdb_stats()
        print(f"DuckDB Stats:")
        print(f"   Trades in hot storage: {stats['trades_count']}")
        print(f"   Whale movements in hot storage: {stats['whale_count']}")
        print(f"   Retention: {stats['retention']}\n")
        
        # Get latest whale movements
        whales = client.get_whale_movements(limit=5)
        print(f"Latest {len(whales)} Whale Movements:")
        for w in whales:
            print(f"   {w.get('whale_type', 'unknown')} | {w.get('direction', '?')} | {w.get('abs_change', 0):.2f} SOL")
        
        print()
        
        # Get latest trades
        trades = client.get_trades(limit=5)
        print(f"Latest {len(trades)} Trades:")
        for t in trades:
            print(f"   {t.get('direction', '?')} | {t.get('sol_amount', 0):.2f} SOL @ ${t.get('price', 0):.2f}")
        
        print()
        
        # Test time-range query (last 15 minutes)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=15)
        print(f"Time-Range Query (last 15 min): {start_time.isoformat()} to {end_time.isoformat()}")
        
        trades_15m = client.get_trades(start_time=start_time, end_time=end_time)
        print(f"   Trades in window: {len(trades_15m)}")
        
        whales_15m = client.get_whale_movements(start_time=start_time, end_time=end_time)
        print(f"   Whale movements in window: {len(whales_15m)}")
    else:
        print("Webhook API is not available")


"""
Webhook Client - Fetch live data from .NET Webhook DuckDB In-Memory API

This client reads from the .NET webhook's in-memory DuckDB (24hr hot storage).
Data source: https://quicknode.smz.dk/api/

Usage:
    from core.webhook_client import WebhookClient
    
    client = WebhookClient()
    
    # Get whale movements
    whale_data = client.get_whale_movements(limit=100)
    
    # Get trades
    trades = client.get_trades(limit=100)
    
    # Check health
    health = client.get_health()
"""

import requests
from typing import Optional, List, Dict, Any
from datetime import datetime


class WebhookClient:
    """Client for reading live data from .NET Webhook DuckDB In-Memory API."""
    
    # Note: Webhook is on HTTP only (no SSL binding on server)
    DEFAULT_URL = "http://quicknode.smz.dk"
    
    def __init__(self, base_url: str = None, timeout: int = 10):
        """
        Initialize the webhook client.
        
        Args:
            base_url: Webhook API base URL (default: https://quicknode.smz.dk)
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
            print(f"[WebhookClient] Health check failed: {e}")
            return None
    
    def get_whale_movements(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get whale movements from DuckDB in-memory hot storage.
        
        Args:
            limit: Maximum number of records (max 500)
            
        Returns:
            List of whale movement records
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/whale-movements",
                params={"limit": min(limit, 500)},
                timeout=self.timeout,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return data.get("results", [])
                else:
                    print(f"[WebhookClient] API error: {data.get('error')}")
            return []
        except Exception as e:
            print(f"[WebhookClient] Failed to get whale movements: {e}")
            return []
    
    def get_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get trades from DuckDB in-memory hot storage.
        
        Args:
            limit: Maximum number of records (max 500)
            
        Returns:
            List of trade records
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/trades",
                params={"limit": min(limit, 500)},
                timeout=self.timeout,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    return data.get("results", [])
                else:
                    print(f"[WebhookClient] API error: {data.get('error')}")
            return []
        except Exception as e:
            print(f"[WebhookClient] Failed to get trades: {e}")
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
    warnings.filterwarnings("ignore")  # Suppress SSL warnings for test
    
    client = WebhookClient()
    
    print("=== Webhook Client Test ===\n")
    
    if client.is_available():
        print("✅ Webhook API is available\n")
        
        # Get stats
        stats = client.get_duckdb_stats()
        print(f"📊 DuckDB Stats:")
        print(f"   Trades in hot storage: {stats['trades_count']}")
        print(f"   Whale movements in hot storage: {stats['whale_count']}")
        print(f"   Retention: {stats['retention']}\n")
        
        # Get latest whale movements
        whales = client.get_whale_movements(limit=5)
        print(f"🐋 Latest {len(whales)} Whale Movements:")
        for w in whales:
            print(f"   {w.get('whale_type', 'unknown')} | {w.get('direction', '?')} | {w.get('abs_change', 0):.2f} SOL")
        
        print()
        
        # Get latest trades
        trades = client.get_trades(limit=5)
        print(f"💱 Latest {len(trades)} Trades:")
        for t in trades:
            print(f"   {t.get('direction', '?')} | {t.get('sol_amount', 0):.2f} SOL @ ${t.get('price', 0):.2f}")
    else:
        print("❌ Webhook API is not available")


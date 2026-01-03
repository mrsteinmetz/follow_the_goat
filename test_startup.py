#!/usr/bin/env python3
"""Test if website_api.py can start properly"""
import sys
from pathlib import Path

# Add project root
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

print("Testing imports...")

try:
    import requests
    print("✓ requests")
except Exception as e:
    print(f"✗ requests: {e}")

try:
    from flask import Flask
    print("✓ flask")
except Exception as e:
    print(f"✗ flask: {e}")

try:
    from core.engine_client import get_engine_client
    print("✓ core.engine_client")
except Exception as e:
    print(f"✗ core.engine_client: {e}")

print("\nAll imports OK!")
print("\nTesting master2 API connection...")

try:
    import requests
    response = requests.get("http://127.0.0.1:5052/health", timeout=2)
    print(f"✓ Master2 API: {response.status_code}")
    data = response.json()
    print(f"  Buyins count: {data.get('tables', {}).get('follow_the_goat_buyins', 0)}")
except Exception as e:
    print(f"✗ Master2 API: {e}")

print("\nTests complete!")

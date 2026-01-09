#!/usr/bin/env python3
"""Test webhook health endpoint"""

import requests
import json

try:
    response = requests.get('http://127.0.0.1:8001/webhook/health', timeout=5)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print(f"Response:\n{json.dumps(response.json(), indent=2, default=str)}")
    else:
        print(f"Error: {response.text}")
except requests.exceptions.ConnectionError:
    print("ERROR: Connection refused - webhook API not running on port 8001")
except Exception as e:
    print(f"Error: {e}")

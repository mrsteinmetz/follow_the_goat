#!/usr/bin/env python3
"""Quick test script to verify the new endpoints work"""
import requests
import json

def test_endpoints():
    trade_id = 20260103074424711
    base_url = "http://127.0.0.1:5051"
    
    print("=" * 60)
    print("Testing Website API Endpoints")
    print("=" * 60)
    
    # Test 1: Single Buyin Endpoint
    print(f"\n1. Testing GET /buyins/{trade_id}")
    try:
        response = requests.get(f"{base_url}/buyins/{trade_id}", timeout=5)
        print(f"   Status: {response.status_code}")
        data = response.json()
        print(f"   Has 'status' key: {'status' in data}")
        print(f"   Has 'buyin' key: {'buyin' in data}")
        if 'buyin' in data:
            buyin = data['buyin']
            print(f"   Buyin ID: {buyin.get('id')}")
            print(f"   Status: {buyin.get('our_status')}")
            print(f"   Entry Price: {buyin.get('our_entry_price')}")
        print("   ✓ PASS: Buyin endpoint working")
    except Exception as e:
        print(f"   ✗ FAIL: {e}")
    
    # Test 2: Trail Data Endpoint
    print(f"\n2. Testing GET /trail/buyin/{trade_id}")
    try:
        response = requests.get(f"{base_url}/trail/buyin/{trade_id}?source=duckdb", timeout=5)
        print(f"   Status: {response.status_code}")
        data = response.json()
        print(f"   Has 'status' key: {'status' in data}")
        print(f"   Has 'trail_data' key: {'trail_data' in data}")
        if 'trail_data' in data:
            trail = data['trail_data']
            print(f"   Trail data rows: {len(trail)}")
            if trail:
                print(f"   First row minute: {trail[0].get('minute')}")
                print(f"   Last row minute: {trail[-1].get('minute')}")
        print("   ✓ PASS: Trail endpoint working")
    except Exception as e:
        print(f"   ✗ FAIL: {e}")
    
    print("\n" + "=" * 60)
    print("Tests Complete!")
    print("=" * 60)

if __name__ == "__main__":
    test_endpoints()

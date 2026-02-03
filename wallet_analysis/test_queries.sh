#!/bin/bash
# Quick test of wallet analysis queries

echo "==================================="
echo "Testing Wallet Analysis Queries"
echo "==================================="
echo ""

cd /root/follow_the_goat/wallet_analysis

echo "1. Testing quick query (top 5 wallets)..."
python3 quick_wallet_query.py 5
echo ""

echo "2. Testing comprehensive analysis..."
python3 find_high_potential_wallets.py
echo ""

echo "3. Testing advanced filter (example: min 5 trades, min 1% gain)..."
python3 advanced_wallet_filter.py --min-trades 5 --min-gain 1.0 --limit 10
echo ""

echo "==================================="
echo "Test complete!"
echo "==================================="

#!/bin/bash
# Setup script for USDC/SOL swap testing

echo "=================================================="
echo "USDC <-> SOL Swap Test - Setup"
echo "=================================================="
echo ""

# Check Python version
echo "Checking Python version..."
python3 --version

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Python 3 not found"
    exit 1
fi

echo "✅ Python found"
echo ""

# Install required packages
echo "Installing required packages..."
pip install solders base58 requests python-dotenv

if [ $? -ne 0 ]; then
    echo "❌ ERROR: Failed to install packages"
    exit 1
fi

echo ""
echo "✅ Packages installed successfully!"
echo ""

# Check if .env exists in project root
if [ ! -f "../.env" ]; then
    echo "⚠️  No .env file found in project root"
    echo ""
    echo "Please create /root/follow_the_goat/.env with:"
    echo ""
    cat env_example.txt
    echo ""
else
    echo "✅ Found .env file"
    
    # Check if Solana keys are configured
    if grep -q "SOLANA_PRIVATE_KEY=your_base58_private_key_here" ../.env 2>/dev/null; then
        echo "⚠️  SOLANA_PRIVATE_KEY not configured in .env"
    elif grep -q "SOLANA_PRIVATE_KEY=" ../.env 2>/dev/null; then
        echo "✅ SOLANA_PRIVATE_KEY found in .env"
    else
        echo "⚠️  SOLANA_PRIVATE_KEY not found in .env"
    fi
fi

echo ""
echo "=================================================="
echo "Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "1. Add your SOLANA_PRIVATE_KEY to .env file"
echo "2. Fund your wallet with at least \$6 USDC"
echo "3. Run: python testswap.py"
echo ""
echo "Need help? Read README.md for detailed instructions"
echo ""

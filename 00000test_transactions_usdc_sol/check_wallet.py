#!/usr/bin/env python3
"""
Quick wallet verification script
Tests that your Solana wallet is properly configured before running the swap test
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

def check_wallet():
    """Verify wallet configuration"""
    
    print("\n" + "="*60)
    print("Wallet Configuration Check")
    print("="*60 + "\n")
    
    # Check for private key
    private_key = os.getenv("SOLANA_PRIVATE_KEY", "")
    
    if not private_key:
        print("❌ SOLANA_PRIVATE_KEY not set")
        print("\nAdd to your .env file:")
        print("SOLANA_PRIVATE_KEY=your_base58_private_key_here")
        return False
    
    if private_key == "your_base58_private_key_here":
        print("❌ SOLANA_PRIVATE_KEY is placeholder value")
        print("\nReplace with your actual private key in .env file")
        return False
    
    print("✅ SOLANA_PRIVATE_KEY found")
    
    # Check if packages are installed
    try:
        import solders
        print("✅ solders package installed")
    except ImportError:
        print("❌ solders package not installed")
        print("   Run: pip install solders")
        return False
    
    try:
        import base58
        print("✅ base58 package installed")
    except ImportError:
        print("❌ base58 package not installed")
        print("   Run: pip install base58")
        return False
    
    # Try to load the wallet
    try:
        import base58
        from solders.keypair import Keypair
        
        secret_key = base58.b58decode(private_key)
        keypair = Keypair.from_bytes(secret_key)
        
        print("✅ Wallet loaded successfully")
        print(f"\n📍 Wallet Address: {keypair.pubkey()}")
        
        # Check balances
        print("\n⏳ Checking balances...")
        
        import requests
        
        rpc_url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        wallet_address = str(keypair.pubkey())
        
        # Check SOL balance
        response = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [wallet_address]
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "result" in data:
                sol_balance = data["result"]["value"] / 1e9
                print(f"   SOL:  {sol_balance:.9f}")
                
                if sol_balance < 0.001:
                    print("   ⚠️  Low SOL balance - add ~0.01 SOL for fees")
        
        # Check USDC balance
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        
        response = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    wallet_address,
                    {"mint": usdc_mint},
                    {"encoding": "jsonParsed"}
                ]
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "result" in data and data["result"]["value"]:
                usdc_amount = float(data["result"]["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"])
                print(f"   USDC: {usdc_amount:.6f}")
                
                if usdc_amount >= 5.0:
                    print("   ✅ Sufficient USDC for test")
                else:
                    print(f"   ⚠️  Need at least $5 USDC (have ${usdc_amount:.2f})")
            else:
                print("   USDC: 0.000000")
                print("   ⚠️  No USDC found - fund wallet before testing")
        
        print("\n" + "="*60)
        print("✅ Wallet configuration looks good!")
        print("="*60)
        print("\nReady to run: python testswap.py")
        print("\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading wallet: {e}")
        print("\nCheck that your private key is correct")
        return False

if __name__ == "__main__":
    try:
        success = check_wallet()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

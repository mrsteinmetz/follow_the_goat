#!/usr/bin/env python3
"""
USDC <-> SOL Swap Test Script with Fee Analysis
================================================
Tests round-trip swaps: USDC -> SOL -> USDC
Tracks all fees including:
- Jupiter platform fees
- Solana network fees
- Slippage costs
- Price impact

Requirements:
1. Install: pip install solders base58 requests
2. Create a Solana wallet and fund it with USDC
3. Add your private key to .env file
"""

import os
import sys
import json
import time
import base58
import requests
from datetime import datetime
from typing import Dict, Optional, Tuple

# Solana imports
try:
    from solders.keypair import Keypair  # type: ignore
    from solders.pubkey import Pubkey  # type: ignore
    from solders.transaction import VersionedTransaction  # type: ignore
    from solders.rpc.requests import SendVersionedTransaction  # type: ignore
    from solders.rpc.config import RpcSendTransactionConfig  # type: ignore
    from solders.commitment_config import CommitmentLevel  # type: ignore
except ImportError:
    print("ERROR: Missing required packages!")
    print("Install with: pip install solders base58 requests")
    sys.exit(1)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("WARNING: python-dotenv not installed, relying on existing environment variables")


# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Configuration for swap testing"""
    
    # Solana RPC endpoint
    RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    
    # Your wallet private key (base58 encoded)
    # KEEP THIS SECRET! Add to .env file as: SOLANA_PRIVATE_KEY=your_key_here
    PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY", "")
    
    # Jupiter API
    JUPITER_API_URL = "https://quote-api.jup.ag/v6"
    JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "")
    
    # Token addresses (Solana mainnet)
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
    SOL_MINT = "So11111111111111111111111111111111111111112"   # Wrapped SOL
    
    # Test amount (in USDC, with 6 decimals)
    TEST_AMOUNT_USDC = 5.0  # $5 USD
    USDC_DECIMALS = 6
    SOL_DECIMALS = 9
    
    # Slippage tolerance (basis points, 50 = 0.5%)
    # NOTE: This is MAXIMUM acceptable, actual slippage is usually much lower
    # For trading bot with 0.05% target: use 10-20 bps tolerance
    SLIPPAGE_BPS = 10  # 10 basis points = 0.1% (actual fees often < 0.05%)
    
    # Fee target for trading bot
    TARGET_FEE_BPS = 5  # 5 basis points = 0.05% total cost target


# ============================================================================
# Wallet Management
# ============================================================================

def load_wallet() -> Optional[Keypair]:
    """Load wallet from private key"""
    if not Config.PRIVATE_KEY:
        print("\n❌ ERROR: SOLANA_PRIVATE_KEY not set in environment!")
        print("\nTo set up your wallet:")
        print("1. Create a Solana wallet (or export from Phantom/Solflare)")
        print("2. Get your private key as base58 string")
        print("3. Add to .env file:")
        print("   SOLANA_PRIVATE_KEY=your_base58_private_key_here")
        print("\n⚠️  SECURITY: Never commit .env to git!")
        return None
    
    try:
        # Decode base58 private key
        secret_key = base58.b58decode(Config.PRIVATE_KEY)
        keypair = Keypair.from_bytes(secret_key)
        
        print(f"✅ Wallet loaded: {keypair.pubkey()}")
        return keypair
        
    except Exception as e:
        print(f"❌ Failed to load wallet: {e}")
        return None


def get_token_balance(wallet_address: str, token_mint: str) -> Optional[float]:
    """Get token balance for wallet"""
    try:
        # For SOL (native token)
        if token_mint == Config.SOL_MINT:
            response = requests.post(
                Config.RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getBalance",
                    "params": [wallet_address]
                }
            )
            data = response.json()
            if "result" in data:
                lamports = data["result"]["value"]
                return lamports / (10 ** Config.SOL_DECIMALS)
        
        # For SPL tokens (USDC)
        else:
            response = requests.post(
                Config.RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        wallet_address,
                        {"mint": token_mint},
                        {"encoding": "jsonParsed"}
                    ]
                }
            )
            data = response.json()
            
            if "result" in data and data["result"]["value"]:
                token_amount = data["result"]["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
                return float(token_amount["uiAmount"])
            
            return 0.0
            
    except Exception as e:
        print(f"❌ Failed to get balance: {e}")
        return None


# ============================================================================
# Jupiter Swap Functions
# ============================================================================

def get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 50
) -> Optional[Dict]:
    """Get swap quote from Jupiter"""
    try:
        headers = {}
        if Config.JUPITER_API_KEY:
            headers["x-api-key"] = Config.JUPITER_API_KEY
        
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
            "onlyDirectRoutes": "false",
            "asLegacyTransaction": "false"
        }
        
        response = requests.get(
            f"{Config.JUPITER_API_URL}/quote",
            params=params,
            headers=headers
        )
        response.raise_for_status()
        
        return response.json()
        
    except Exception as e:
        print(f"❌ Failed to get quote: {e}")
        return None


def get_low_fee_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 10,
    max_fee_bps: float = 5.0
) -> Optional[Dict]:
    """
    Get quote optimized for LOW FEES (target < 0.05%)
    
    Strategies:
    1. Try direct routes first (fewer hops = lower fees)
    2. Prefer high-liquidity pools (lower price impact)
    3. Filter by fee tier
    """
    try:
        headers = {}
        if Config.JUPITER_API_KEY:
            headers["x-api-key"] = Config.JUPITER_API_KEY
        
        # Try direct routes first (lowest fees)
        print("   Trying direct routes first...")
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
            "onlyDirectRoutes": "true",  # Direct only = fewer fees
            "asLegacyTransaction": "false"
        }
        
        response = requests.get(
            f"{Config.JUPITER_API_URL}/quote",
            params=params,
            headers=headers
        )
        
        if response.status_code == 200:
            quote = response.json()
            
            # Check if fee is acceptable
            in_amount = int(quote["inAmount"])
            out_amount = int(quote["outAmount"])
            price_impact = abs(float(quote.get("priceImpactPct", 0)))
            
            # Estimate total fee (price impact is main component)
            estimated_fee_bps = price_impact * 100
            
            print(f"   Direct route: {price_impact:.4f}% price impact")
            
            if estimated_fee_bps <= max_fee_bps:
                print(f"   ✅ Direct route meets fee target (<{max_fee_bps/100:.3f}%)")
                return quote
            else:
                print(f"   ⚠️  Direct route fee too high: {estimated_fee_bps/100:.3f}%")
        
        # If direct routes don't work, try all routes
        print("   Trying all routes...")
        params["onlyDirectRoutes"] = "false"
        
        response = requests.get(
            f"{Config.JUPITER_API_URL}/quote",
            params=params,
            headers=headers
        )
        response.raise_for_status()
        
        quote = response.json()
        price_impact = abs(float(quote.get("priceImpactPct", 0)))
        estimated_fee_bps = price_impact * 100
        
        print(f"   Best route: {price_impact:.4f}% price impact")
        
        if estimated_fee_bps > max_fee_bps:
            print(f"   ⚠️  Warning: Estimated fee {estimated_fee_bps/100:.3f}% exceeds target {max_fee_bps/100:.3f}%")
            print(f"   ⚠️  Consider: smaller trade size or wait for better liquidity")
        
        return quote
        
    except Exception as e:
        print(f"❌ Failed to get low-fee quote: {e}")
        return None


def execute_swap(
    wallet: Keypair,
    quote: Dict
) -> Optional[str]:
    """Execute swap using Jupiter"""
    try:
        # Get swap transaction
        headers = {"Content-Type": "application/json"}
        if Config.JUPITER_API_KEY:
            headers["x-api-key"] = Config.JUPITER_API_KEY
        
        swap_request = {
            "quoteResponse": quote,
            "userPublicKey": str(wallet.pubkey()),
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": "auto"
        }
        
        response = requests.post(
            f"{Config.JUPITER_API_URL}/swap",
            json=swap_request,
            headers=headers
        )
        response.raise_for_status()
        swap_data = response.json()
        
        # Deserialize transaction
        swap_transaction_buf = base58.b58decode(swap_data["swapTransaction"])
        transaction = VersionedTransaction.from_bytes(swap_transaction_buf)
        
        # Sign transaction
        signed_transaction = VersionedTransaction(
            transaction.message,
            [wallet]
        )
        
        # Send transaction
        rpc_config = RpcSendTransactionConfig(
            skip_preflight=False,
            preflight_commitment=CommitmentLevel.Confirmed,
            max_retries=3
        )
        
        serialized_tx = bytes(signed_transaction)
        
        response = requests.post(
            Config.RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    base58.b58encode(serialized_tx).decode('utf-8'),
                    {
                        "skipPreflight": False,
                        "preflightCommitment": "confirmed",
                        "maxRetries": 3
                    }
                ]
            }
        )
        
        result = response.json()
        
        if "error" in result:
            print(f"❌ Transaction error: {result['error']}")
            return None
        
        signature = result["result"]
        print(f"✅ Transaction sent: {signature}")
        
        return signature
        
    except Exception as e:
        print(f"❌ Failed to execute swap: {e}")
        import traceback
        traceback.print_exc()
        return None


def wait_for_confirmation(signature: str, max_wait: int = 60) -> bool:
    """Wait for transaction confirmation"""
    print(f"⏳ Waiting for confirmation (max {max_wait}s)...")
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.post(
                Config.RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignatureStatuses",
                    "params": [[signature]]
                }
            )
            
            result = response.json()
            
            if "result" in result and result["result"]["value"]:
                status = result["result"]["value"][0]
                
                if status:
                    if status.get("err"):
                        print(f"❌ Transaction failed: {status['err']}")
                        return False
                    
                    if status.get("confirmationStatus") in ["confirmed", "finalized"]:
                        print(f"✅ Transaction confirmed!")
                        return True
            
            time.sleep(2)
            
        except Exception as e:
            print(f"⚠️  Error checking status: {e}")
            time.sleep(2)
    
    print(f"❌ Timeout waiting for confirmation")
    return False


# ============================================================================
# Fee Analysis
# ============================================================================

def analyze_swap_result(
    quote: Dict,
    input_amount: float,
    output_amount: float,
    input_symbol: str,
    output_symbol: str
) -> Dict:
    """Analyze swap results and calculate fees"""
    
    # Extract quote info
    in_amount = int(quote["inAmount"])
    out_amount = int(quote["outAmount"])
    
    # Get price impact
    price_impact_pct = float(quote.get("priceImpactPct", 0))
    
    # Get platform fee info
    platform_fee = quote.get("platformFee", {})
    platform_fee_amount = float(platform_fee.get("amount", 0)) if platform_fee else 0
    
    # Calculate slippage
    expected_output = float(out_amount)
    actual_output = output_amount
    slippage = ((expected_output - actual_output) / expected_output * 100) if expected_output > 0 else 0
    
    # Get route info
    route_plan = quote.get("routePlan", [])
    num_hops = len(route_plan)
    
    dexes_used = []
    for hop in route_plan:
        swap_info = hop.get("swapInfo", {})
        label = swap_info.get("label", "Unknown")
        if label not in dexes_used:
            dexes_used.append(label)
    
    return {
        "input_amount": input_amount,
        "output_amount": output_amount,
        "input_symbol": input_symbol,
        "output_symbol": output_symbol,
        "price_impact_pct": price_impact_pct,
        "platform_fee": platform_fee_amount,
        "slippage_pct": slippage,
        "num_hops": num_hops,
        "dexes_used": dexes_used
    }


# ============================================================================
# Main Test Flow
# ============================================================================

def print_banner():
    """Print test banner"""
    print("\n" + "="*70)
    print("USDC <-> SOL Swap Fee Test")
    print("="*70)
    print(f"Test Amount: ${Config.TEST_AMOUNT_USDC} USDC")
    print(f"Slippage: {Config.SLIPPAGE_BPS / 100}%")
    print(f"RPC: {Config.RPC_URL[:50]}...")
    print("="*70 + "\n")


def test_round_trip_swap():
    """Test USDC -> SOL -> USDC round trip"""
    
    print_banner()
    
    # Load wallet
    wallet = load_wallet()
    if not wallet:
        return
    
    wallet_address = str(wallet.pubkey())
    
    # Check initial balances
    print("\n📊 Initial Balances:")
    print("-" * 50)
    
    initial_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
    initial_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    
    if initial_usdc is None or initial_sol is None:
        print("❌ Failed to fetch balances")
        return
    
    print(f"USDC: {initial_usdc:.6f}")
    print(f"SOL:  {initial_sol:.9f}")
    
    # Check if we have enough USDC
    if initial_usdc < Config.TEST_AMOUNT_USDC:
        print(f"\n❌ Insufficient USDC balance!")
        print(f"Required: {Config.TEST_AMOUNT_USDC} USDC")
        print(f"Available: {initial_usdc} USDC")
        return
    
    # Convert test amount to raw units (USDC has 6 decimals)
    test_amount_raw = int(Config.TEST_AMOUNT_USDC * (10 ** Config.USDC_DECIMALS))
    
    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "wallet": wallet_address,
        "initial_usdc": initial_usdc,
        "initial_sol": initial_sol,
        "swaps": []
    }
    
    # ========================================================================
    # SWAP 1: USDC -> SOL
    # ========================================================================
    
    print("\n\n🔄 SWAP 1: USDC -> SOL")
    print("="*70)
    
    print("⏳ Getting low-fee quote...")
    quote1 = get_low_fee_quote(
        input_mint=Config.USDC_MINT,
        output_mint=Config.SOL_MINT,
        amount=test_amount_raw,
        slippage_bps=Config.SLIPPAGE_BPS,
        max_fee_bps=Config.TARGET_FEE_BPS
    )
    
    if not quote1:
        print("❌ Failed to get quote for USDC -> SOL")
        return
    
    expected_sol = int(quote1["outAmount"]) / (10 ** Config.SOL_DECIMALS)
    print(f"Expected output: {expected_sol:.9f} SOL")
    
    input(f"\n⚠️  Press ENTER to execute swap 1 (USDC -> SOL)...")
    
    sig1 = execute_swap(wallet, quote1)
    if not sig1:
        print("❌ Failed to execute swap")
        return
    
    if not wait_for_confirmation(sig1):
        print("❌ Swap not confirmed")
        return
    
    # Wait a bit for balance update
    time.sleep(3)
    
    # Check balances after swap 1
    mid_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
    mid_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    
    print(f"\n📊 Balances after swap 1:")
    print(f"USDC: {mid_usdc:.6f} (change: {mid_usdc - initial_usdc:+.6f})")
    print(f"SOL:  {mid_sol:.9f} (change: {mid_sol - initial_sol:+.9f})")
    
    swap1_analysis = analyze_swap_result(
        quote1,
        Config.TEST_AMOUNT_USDC,
        mid_sol - initial_sol,
        "USDC",
        "SOL"
    )
    results["swaps"].append({
        "direction": "USDC->SOL",
        "signature": sig1,
        **swap1_analysis
    })
    
    # Calculate how much SOL we got
    sol_received = mid_sol - initial_sol
    
    if sol_received <= 0:
        print("❌ No SOL received, aborting test")
        return
    
    # ========================================================================
    # SWAP 2: SOL -> USDC
    # ========================================================================
    
    print("\n\n🔄 SWAP 2: SOL -> USDC")
    print("="*70)
    
    # Use the SOL we received (minus a small buffer for fees)
    sol_to_swap = int(sol_received * 0.99 * (10 ** Config.SOL_DECIMALS))
    
    print("⏳ Getting low-fee quote...")
    quote2 = get_low_fee_quote(
        input_mint=Config.SOL_MINT,
        output_mint=Config.USDC_MINT,
        amount=sol_to_swap,
        slippage_bps=Config.SLIPPAGE_BPS,
        max_fee_bps=Config.TARGET_FEE_BPS
    )
    
    if not quote2:
        print("❌ Failed to get quote for SOL -> USDC")
        return
    
    expected_usdc = int(quote2["outAmount"]) / (10 ** Config.USDC_DECIMALS)
    print(f"Expected output: {expected_usdc:.6f} USDC")
    
    input(f"\n⚠️  Press ENTER to execute swap 2 (SOL -> USDC)...")
    
    sig2 = execute_swap(wallet, quote2)
    if not sig2:
        print("❌ Failed to execute swap")
        return
    
    if not wait_for_confirmation(sig2):
        print("❌ Swap not confirmed")
        return
    
    # Wait a bit for balance update
    time.sleep(3)
    
    # Check final balances
    final_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
    final_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    
    print(f"\n📊 Final Balances:")
    print(f"USDC: {final_usdc:.6f} (change: {final_usdc - initial_usdc:+.6f})")
    print(f"SOL:  {final_sol:.9f} (change: {final_sol - initial_sol:+.9f})")
    
    swap2_analysis = analyze_swap_result(
        quote2,
        sol_received * 0.99,
        final_usdc - mid_usdc,
        "SOL",
        "USDC"
    )
    results["swaps"].append({
        "direction": "SOL->USDC",
        "signature": sig2,
        **swap2_analysis
    })
    
    # ========================================================================
    # FINAL ANALYSIS
    # ========================================================================
    
    print("\n\n" + "="*70)
    print("📊 ROUND-TRIP ANALYSIS")
    print("="*70)
    
    total_usdc_change = final_usdc - initial_usdc
    total_cost_usd = abs(total_usdc_change)
    total_cost_pct = (total_cost_usd / Config.TEST_AMOUNT_USDC) * 100
    
    print(f"\nStarting USDC: ${initial_usdc:.6f}")
    print(f"Ending USDC:   ${final_usdc:.6f}")
    print(f"Net Change:    ${total_usdc_change:+.6f}")
    print(f"\nTotal Cost:    ${total_cost_usd:.6f}")
    print(f"Cost %:        {total_cost_pct:.3f}%")
    
    print(f"\n{'Swap':<12} {'Route':<20} {'Price Impact':<15} {'DEXes Used':<30}")
    print("-" * 70)
    
    for swap in results["swaps"]:
        direction = swap["direction"]
        price_impact = f"{swap['price_impact_pct']:.3f}%"
        dexes = ", ".join(swap["dexes_used"][:2])  # Show first 2
        print(f"{direction:<12} {swap['num_hops']} hops{'':<14} {price_impact:<15} {dexes:<30}")
    
    results["final_usdc"] = final_usdc
    results["final_sol"] = final_sol
    results["total_cost_usdc"] = total_cost_usd
    results["total_cost_pct"] = total_cost_pct
    
    # Save results to file
    output_file = f"swap_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    
    print("\n🔗 View transactions:")
    for swap in results["swaps"]:
        print(f"  {swap['direction']}: https://solscan.io/tx/{swap['signature']}")
    
    print("\n" + "="*70)
    print("✅ Test Complete!")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        test_round_trip_swap()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test cancelled by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

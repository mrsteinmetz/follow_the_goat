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
import base64
import requests
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

# Solana imports
try:
    from solders.keypair import Keypair  # type: ignore
    from solders.transaction import VersionedTransaction  # type: ignore
    from solders.message import to_bytes_versioned  # type: ignore
    from solders.signature import Signature  # type: ignore
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

def _get_rpc_url() -> str:
    """Use SOLANA_RPC_URL, or build from helius_key if set."""
    url = os.getenv("SOLANA_RPC_URL", "")
    if url:
        return url
    key = os.getenv("helius_key", "")
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return "https://api.mainnet-beta.solana.com"


class Config:
    """Configuration for swap testing"""
    
    # Solana RPC endpoint
    RPC_URL = _get_rpc_url()
    
    # Wallet: prefer usdc_private_key (from .env), fallback to SOLANA_PRIVATE_KEY
    PRIVATE_KEY = os.getenv("usdc_private_key", "") or os.getenv("SOLANA_PRIVATE_KEY", "")
    
    # Jupiter Ultra API (recommended; lite-api.jup.ag deprecated Jan 31 2026)
    # https://dev.jup.ag/docs/ultra/get-started
    JUPITER_API_BASE = "https://api.jup.ag"
    JUPITER_ORDER_URL = f"{JUPITER_API_BASE}/ultra/v1/order"
    JUPITER_EXECUTE_URL = f"{JUPITER_API_BASE}/ultra/v1/execute"
    # Jupiter API key (from .env; same key used by trading bot)
    JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "") or os.getenv("jupiter_api_key", "")
    
    # Token addresses (Solana mainnet)
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
    SOL_MINT = "So11111111111111111111111111111111111111112"   # Wrapped SOL
    
    # Test amount (in USDC). Override with env TEST_AMOUNT_USDC (e.g. 5 or 10)
    TEST_AMOUNT_USDC = float(os.getenv("TEST_AMOUNT_USDC", "5.0"))
    USDC_DECIMALS = 6
    SOL_DECIMALS = 9
    
    # Slippage tolerance (basis points, 50 = 0.5%)
    # NOTE: This is MAXIMUM acceptable, actual slippage is usually much lower
    # For trading bot with 0.05% target: use 10-20 bps tolerance
    SLIPPAGE_BPS = 10  # 10 basis points = 0.1% (actual fees often < 0.05%)
    
    # Fee target for trading bot
    TARGET_FEE_BPS = 5  # 5 basis points = 0.05% total cost target

    # If set (e.g. AUTO_EXECUTE_SWAP=1), run both swaps without prompting
    AUTO_EXECUTE_SWAP = os.getenv("AUTO_EXECUTE_SWAP", "").strip().lower() in ("1", "true", "yes")

    # Keep a small SOL buffer for network fees (fixed amount, not %)
    SOL_FEE_BUFFER = float(os.getenv("SOL_FEE_BUFFER", "0.002"))


# ============================================================================
# Wallet Management
# ============================================================================

def load_wallet() -> Optional[Keypair]:
    """Load wallet from private key"""
    if not Config.PRIVATE_KEY:
        print("\n❌ ERROR: No wallet private key set!")
        print("\nAdd to .env in project root:")
        print("   usdc_private_key=your_base58_private_key_here")
        print("   (or SOLANA_PRIVATE_KEY=...)")
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


def get_sol_price_usdc() -> Optional[float]:
    """Get SOL price in USDC using Ultra order for 1 SOL."""
    one_sol = 1 * (10 ** Config.SOL_DECIMALS)
    quote = get_quote(
        input_mint=Config.SOL_MINT,
        output_mint=Config.USDC_MINT,
        amount=one_sol,
        slippage_bps=10,
        taker=None,
    )
    if not quote:
        return None
    out_amount = int(quote.get("outAmount", 0))
    return out_amount / (10 ** Config.USDC_DECIMALS)


# ============================================================================
# Jupiter Swap Functions
# ============================================================================

def get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 50,
    taker: Optional[str] = None,
) -> Optional[Dict]:
    """Get swap order from Jupiter Ultra API (GET /ultra/v1/order)."""
    try:
        headers = {}
        if Config.JUPITER_API_KEY:
            headers["x-api-key"] = Config.JUPITER_API_KEY
        
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
        }
        if taker:
            params["taker"] = taker
        if slippage_bps is not None:
            params["slippageBps"] = str(slippage_bps)
        
        response = requests.get(
            Config.JUPITER_ORDER_URL,
            params=params,
            headers=headers,
            timeout=15,
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
    max_fee_bps: float = 5.0,
    taker: Optional[str] = None,
) -> Optional[Dict]:
    """
    Get order from Jupiter Ultra API, optimized for low fees (target < 0.05%).
    Ultra API: GET /ultra/v1/order
    """
    try:
        order = get_quote(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=amount,
            slippage_bps=slippage_bps,
            taker=taker,
        )
        if not order:
            return None
        # Ultra response: inAmount, outAmount, priceImpactPct (string) or priceImpact (number), routePlan
        in_amount = int(order.get("inAmount", 0))
        out_amount = int(order.get("outAmount", 0))
        price_impact_raw = order.get("priceImpactPct") or order.get("priceImpact") or 0
        price_impact = abs(float(price_impact_raw))
        estimated_fee_bps = price_impact * 100
        print(f"   Ultra route: {price_impact:.4f}% price impact")
        if estimated_fee_bps > max_fee_bps:
            print(f"   ⚠️  Warning: Estimated fee {estimated_fee_bps/100:.3f}% exceeds target {max_fee_bps/100:.3f}%")
        return order
    except Exception as e:
        print(f"❌ Failed to get low-fee quote: {e}")
        return None


def _send_tx_via_rpc(serialized_tx: bytes) -> Optional[str]:
    """Send signed transaction via RPC sendTransaction (fallback when Jupiter execute rejects)."""
    try:
        tx_b58 = base58.b58encode(serialized_tx).decode("utf-8")
        response = requests.post(
            Config.RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    tx_b58,
                    {"skipPreflight": False, "preflightCommitment": "confirmed", "maxRetries": 3},
                ],
            },
            timeout=30,
        )
        data = response.json()
        if data.get("error"):
            print(f"❌ RPC sendTransaction error: {data['error']}")
            return None
        sig = data.get("result")
        if sig:
            print(f"✅ Transaction sent via RPC: {sig}")
        return sig
    except Exception as e:
        print(f"❌ RPC send failed: {e}")
        return None


def _sign_tx_bytes(transaction: VersionedTransaction, wallet: Keypair, mode: str) -> bytes:
    """Sign a VersionedTransaction and return serialized bytes.

    mode:
      - "raw": bytes(transaction.message)
      - "versioned": to_bytes_versioned(transaction.message)
    """
    if mode == "versioned":
        msg_bytes = to_bytes_versioned(transaction.message)
    else:
        msg_bytes = bytes(transaction.message)
    sig = wallet.sign_message(msg_bytes)

    header = transaction.message.header
    num_required = header.num_required_signatures
    account_keys = transaction.message.account_keys
    signer_index = None
    for i in range(num_required):
        if account_keys[i] == wallet.pubkey():
            signer_index = i
            break
    if signer_index is None:
        raise ValueError("Wallet pubkey not in required signer list")

    signatures: List[Signature] = [Signature.default()] * num_required
    signatures[signer_index] = sig
    signed_tx = VersionedTransaction.populate(transaction.message, signatures)
    return bytes(signed_tx)


def execute_swap(
    wallet: Keypair,
    order: Dict,
) -> Optional[str]:
    """Execute swap using Jupiter Ultra API (POST /ultra/v1/execute).
    Order must contain transaction (base64) and requestId from GET /ultra/v1/order.
    Jupiter executes on their side; no RPC sendTransaction needed.
    """
    try:
        transaction_raw = order.get("transaction")
        request_id = order.get("requestId")
        if not transaction_raw or not request_id:
            print("❌ Order missing transaction or requestId (Ultra API)")
            return None

        # Deserialize: Ultra returns base64; fallback to base58 if decode fails
        try:
            tx_bytes = base64.b64decode(transaction_raw)
        except Exception:
            tx_bytes = base58.b58decode(transaction_raw)
        transaction = VersionedTransaction.from_bytes(tx_bytes)

        def _execute_signed_bytes(tx_bytes_out: bytes) -> Tuple[str, Optional[str], Dict]:
            signed_b64 = base64.b64encode(tx_bytes_out).decode("utf-8")
            payload = {
                "signedTransaction": signed_b64,
                "requestId": str(request_id),
            }
            headers = {"Content-Type": "application/json"}
            if Config.JUPITER_API_KEY:
                headers["x-api-key"] = Config.JUPITER_API_KEY
            response = requests.post(
                Config.JUPITER_EXECUTE_URL,
                json=payload,
                headers=headers,
                timeout=60,
            )
            result = response.json() if response.content else {}
            if response.ok and not result.get("error"):
                return "success", result.get("signature"), result
            if response.status_code == 400 and result.get("code") == -2:
                return "decode_error", None, result
            return "error", None, result

        # First try: versioned message bytes (matches Solana v0 signing)
        tx_bytes_out = _sign_tx_bytes(transaction, wallet, "versioned")
        status, signature, result = _execute_signed_bytes(tx_bytes_out)
        if status == "success" and signature:
            print(f"✅ Transaction submitted: {signature} (status: {result.get('status', '')})")
            return signature

        # If Jupiter can't decode, try versioned message bytes
        if status == "decode_error":
            print("⚠️  Jupiter execute rejected versioned-signed tx; retrying with raw message bytes...")
            alt_bytes = _sign_tx_bytes(transaction, wallet, "raw")
            status2, signature2, result2 = _execute_signed_bytes(alt_bytes)
            if status2 == "success" and signature2:
                print(f"✅ Transaction submitted: {signature2} (status: {result2.get('status', '')})")
                return signature2

        # Fallback to RPC sendTransaction (try raw, then versioned if needed)
        print("⚠️  Jupiter execute failed; sending via RPC...")
        sig_rpc = _send_tx_via_rpc(tx_bytes_out)
        if sig_rpc:
            return sig_rpc
        alt_bytes = _sign_tx_bytes(transaction, wallet, "raw")
        return _send_tx_via_rpc(alt_bytes)
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
    
    # Ultra API: inAmount/outAmount can be str; priceImpactPct str or priceImpact number
    in_amount = int(quote.get("inAmount", 0))
    out_amount = int(quote.get("outAmount", 0))
    price_impact_pct = float(quote.get("priceImpactPct") or quote.get("priceImpact") or 0)
    
    # Platform fee (Ultra may not have platformFee)
    platform_fee = quote.get("platformFee") or {}
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
    print(f"Jupiter: Ultra API ({Config.JUPITER_API_BASE})")
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

    # Solana tx fees are paid in SOL; need a small amount for both swaps
    min_sol_for_fees = 0.005  # ~2 txs
    if initial_sol is not None and initial_sol < min_sol_for_fees:
        print(f"\n⚠️  Very low SOL balance ({initial_sol:.6f} SOL).")
        print("   You need a small amount of SOL to pay network fees (~0.01 SOL).")
        print("   Get free SOL from a faucet or transfer a tiny amount to this wallet.")
        if not Config.AUTO_EXECUTE_SWAP:
            reply = input("   Continue anyway? [y/N]: ").strip().lower()
            if reply != "y":
                return
        else:
            print("   Proceeding anyway (may fail on first transaction).")
    
    # Convert test amount to raw units (USDC has 6 decimals)
    test_amount_raw = int(Config.TEST_AMOUNT_USDC * (10 ** Config.USDC_DECIMALS))
    
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
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
        max_fee_bps=Config.TARGET_FEE_BPS,
        taker=wallet_address,
    )
    
    if not quote1:
        print("❌ Failed to get quote for USDC -> SOL")
        return
    
    expected_sol = int(quote1["outAmount"]) / (10 ** Config.SOL_DECIMALS)
    print(f"Expected output: {expected_sol:.9f} SOL")
    
    if not Config.AUTO_EXECUTE_SWAP:
        input(f"\n⚠️  Press ENTER to execute swap 1 (USDC -> SOL)...")
    else:
        print("\nExecuting swap 1 (USDC -> SOL)...")
    
    sig1 = execute_swap(wallet, quote1)
    if not sig1:
        print("❌ Failed to execute swap")
        return
    
    if not wait_for_confirmation(sig1):
        print("❌ Swap not confirmed")
        return
    
    # Wait for balance update (RPC/indexer can lag)
    time.sleep(8)
    
    mid_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
    mid_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    sol_received = mid_sol - initial_sol if mid_sol is not None and initial_sol is not None else 0
    if sol_received <= 0:
        time.sleep(5)
        mid_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
        mid_sol = get_token_balance(wallet_address, Config.SOL_MINT)
        sol_received = (mid_sol - initial_sol) if mid_sol is not None and initial_sol is not None else 0
    
    print(f"\n📊 Balances after swap 1:")
    print(f"USDC: {mid_usdc:.6f} (change: {mid_usdc - initial_usdc:+.6f})")
    print(f"SOL:  {mid_sol:.9f} (change: {mid_sol - initial_sol:+.9f})")
    
    swap1_analysis = analyze_swap_result(
        quote1,
        Config.TEST_AMOUNT_USDC,
        sol_received,
        "USDC",
        "SOL"
    )
    results["swaps"].append({
        "direction": "USDC->SOL",
        "signature": sig1,
        **swap1_analysis
    })
    
    if sol_received <= 0:
        print("❌ No SOL received (tx may have landed; check Solscan). Aborting test.")
        if sig1:
            print(f"   Tx: https://solscan.io/tx/{sig1}")
        return
    
    # ========================================================================
    # SWAP 2: SOL -> USDC
    # ========================================================================
    
    print("\n\n🔄 SWAP 2: SOL -> USDC")
    print("="*70)
    
    # Use SOL received minus a small fixed buffer for network fees
    sol_to_swap_amt = max(sol_received - Config.SOL_FEE_BUFFER, 0)
    sol_to_swap = int(sol_to_swap_amt * (10 ** Config.SOL_DECIMALS))
    if sol_to_swap <= 0:
        print("❌ SOL received is too small after fee buffer; aborting swap 2")
        return
    
    print("⏳ Getting low-fee quote...")
    quote2 = get_low_fee_quote(
        input_mint=Config.SOL_MINT,
        output_mint=Config.USDC_MINT,
        amount=sol_to_swap,
        slippage_bps=Config.SLIPPAGE_BPS,
        max_fee_bps=Config.TARGET_FEE_BPS,
        taker=wallet_address,
    )
    
    if not quote2:
        print("❌ Failed to get quote for SOL -> USDC")
        return
    
    expected_usdc = int(quote2["outAmount"]) / (10 ** Config.USDC_DECIMALS)
    print(f"Expected output: {expected_usdc:.6f} USDC")
    
    if not Config.AUTO_EXECUTE_SWAP:
        input(f"\n⚠️  Press ENTER to execute swap 2 (SOL -> USDC)...")
    else:
        print("\nExecuting swap 2 (SOL -> USDC)...")
    
    sig2 = execute_swap(wallet, quote2)
    if not sig2:
        print("❌ Failed to execute swap")
        return
    
    if not wait_for_confirmation(sig2):
        print("❌ Swap not confirmed")
        return
    
    # Wait for balance update (RPC/indexer can lag)
    time.sleep(8)
    
    final_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
    final_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    usdc_change = (final_usdc - mid_usdc) if final_usdc is not None and mid_usdc is not None else 0
    sol_change = (final_sol - mid_sol) if final_sol is not None and mid_sol is not None else 0
    if abs(usdc_change) < 1e-6 and abs(sol_change) < 1e-9:
        time.sleep(5)
        final_usdc = get_token_balance(wallet_address, Config.USDC_MINT)
        final_sol = get_token_balance(wallet_address, Config.SOL_MINT)
    
    print(f"\n📊 Final Balances:")
    print(f"USDC: {final_usdc:.6f} (change: {final_usdc - initial_usdc:+.6f})")
    print(f"SOL:  {final_sol:.9f} (change: {final_sol - initial_sol:+.9f})")
    if abs(final_usdc - mid_usdc) < 1e-6 and abs(final_sol - mid_sol) < 1e-9:
        print("⚠️  Balances unchanged after swap 2; check Solscan for tx status.")
        print(f"   Tx: https://solscan.io/tx/{sig2}")
    
    swap2_analysis = analyze_swap_result(
        quote2,
        sol_received * 0.99,
        (final_usdc - mid_usdc) if final_usdc is not None and mid_usdc is not None else 0,
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
    
    # Compute total portfolio value in USDC (includes leftover SOL)
    sol_price = get_sol_price_usdc()
    if sol_price:
        start_value = initial_usdc + (initial_sol * sol_price)
        end_value = final_usdc + (final_sol * sol_price)
        total_cost_usd = max(0.0, start_value - end_value)
        total_cost_pct = (total_cost_usd / Config.TEST_AMOUNT_USDC) * 100
    else:
        total_usdc_change = final_usdc - initial_usdc
        total_cost_usd = abs(total_usdc_change)
        total_cost_pct = (total_cost_usd / Config.TEST_AMOUNT_USDC) * 100
    
    print(f"\nStarting USDC: ${initial_usdc:.6f}")
    print(f"Ending USDC:   ${final_usdc:.6f}")
    if sol_price:
        print(f"SOL Price:     ${sol_price:.6f}")
        print(f"Start Value:   ${start_value:.6f}")
        print(f"End Value:     ${end_value:.6f}")
    else:
        total_usdc_change = final_usdc - initial_usdc
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
    output_file = f"swap_test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
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

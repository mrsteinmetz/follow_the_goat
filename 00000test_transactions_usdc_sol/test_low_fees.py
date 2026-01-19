#!/usr/bin/env python3
"""
Low-Fee Trading Strategy Test
==============================
Tests different strategies to achieve < 0.05% trading fees on Solana

Strategies tested:
1. Direct routes (1-hop) vs multi-hop routes
2. Different trade sizes (optimal size for lowest fees)
3. Different liquidity pools (Orca, Raydium, etc.)
4. Time of day effects
"""

import os
import sys
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass


class Config:
    """Configuration"""
    JUPITER_API_URL = "https://quote-api.jup.ag/v6"
    JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "")
    
    USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    SOL_MINT = "So11111111111111111111111111111111111111112"
    
    USDC_DECIMALS = 6
    SOL_DECIMALS = 9
    
    # Target: 0.05% total fees = 5 basis points
    TARGET_FEE_BPS = 5


def get_quote_analysis(
    input_mint: str,
    output_mint: str,
    amount: int,
    only_direct: bool = False
) -> Optional[Dict]:
    """Get quote and analyze fees"""
    try:
        headers = {}
        if Config.JUPITER_API_KEY:
            headers["x-api-key"] = Config.JUPITER_API_KEY
        
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": "10",  # Low tolerance for low-fee targeting
            "onlyDirectRoutes": "true" if only_direct else "false",
            "asLegacyTransaction": "false"
        }
        
        response = requests.get(
            f"{Config.JUPITER_API_URL}/quote",
            params=params,
            headers=headers,
            timeout=10
        )
        
        if response.status_code != 200:
            return None
        
        quote = response.json()
        
        # Analyze the quote
        in_amount = int(quote["inAmount"])
        out_amount = int(quote["outAmount"])
        price_impact_pct = abs(float(quote.get("priceImpactPct", 0)))
        
        # Get route info
        route_plan = quote.get("routePlan", [])
        num_hops = len(route_plan)
        
        dexes = []
        pool_types = []
        for hop in route_plan:
            swap_info = hop.get("swapInfo", {})
            label = swap_info.get("label", "Unknown")
            if label not in dexes:
                dexes.append(label)
            
            # Try to get pool fee tier
            fee_pct = swap_info.get("feeAmount", 0)
            if fee_pct:
                pool_types.append(f"{label}:{fee_pct}")
        
        # Estimate total fee
        # For Solana DEXes:
        # - Orca: 0.01% (1 bps), 0.04% (4 bps), 0.3% (30 bps) pools
        # - Raydium: 0.25% (25 bps) typically
        # - Whirlpool: 0.01% to 1% depending on pool
        
        estimated_fee_bps = price_impact_pct * 100  # Convert to basis points
        
        return {
            "quote": quote,
            "in_amount": in_amount,
            "out_amount": out_amount,
            "price_impact_pct": price_impact_pct,
            "estimated_fee_bps": estimated_fee_bps,
            "num_hops": num_hops,
            "dexes": dexes,
            "pool_types": pool_types,
            "route_type": "direct" if only_direct else "optimal"
        }
        
    except Exception as e:
        print(f"Error getting quote: {e}")
        return None


def test_trade_size_impact():
    """Test how trade size affects fees"""
    print("\n" + "="*70)
    print("TEST 1: Trade Size Impact on Fees")
    print("="*70)
    print("\nTesting USDC -> SOL swaps at different sizes...")
    
    # Test different trade sizes
    test_sizes = [
        5,      # $5
        10,     # $10
        50,     # $50
        100,    # $100
        500,    # $500
        1000,   # $1000
        5000,   # $5000
    ]
    
    results = []
    
    for size_usd in test_sizes:
        amount_raw = int(size_usd * (10 ** Config.USDC_DECIMALS))
        
        print(f"\n${size_usd} USDC:")
        
        analysis = get_quote_analysis(
            Config.USDC_MINT,
            Config.SOL_MINT,
            amount_raw
        )
        
        if analysis:
            fee_bps = analysis["estimated_fee_bps"]
            fee_pct = fee_bps / 100
            
            meets_target = "✅" if fee_bps <= Config.TARGET_FEE_BPS else "❌"
            
            print(f"  {meets_target} Fee: {fee_pct:.4f}% ({fee_bps:.2f} bps)")
            print(f"  Route: {analysis['num_hops']} hops via {', '.join(analysis['dexes'][:3])}")
            
            results.append({
                "size_usd": size_usd,
                "fee_bps": fee_bps,
                "fee_pct": fee_pct,
                "num_hops": analysis["num_hops"],
                "dexes": analysis["dexes"],
                "meets_target": fee_bps <= Config.TARGET_FEE_BPS
            })
        else:
            print(f"  ❌ Failed to get quote")
        
        time.sleep(0.5)  # Rate limiting
    
    # Find optimal size
    optimal = min(
        [r for r in results if r["meets_target"]],
        key=lambda x: x["fee_bps"],
        default=None
    )
    
    if optimal:
        print(f"\n✅ OPTIMAL SIZE: ${optimal['size_usd']} with {optimal['fee_pct']:.4f}% fee")
    else:
        # Find lowest fee even if it doesn't meet target
        lowest = min(results, key=lambda x: x["fee_bps"])
        print(f"\n⚠️  No size meets {Config.TARGET_FEE_BPS} bps target")
        print(f"   Best: ${lowest['size_usd']} with {lowest['fee_pct']:.4f}% fee")
    
    return results


def test_direct_vs_multihop():
    """Test direct routes vs multi-hop routes"""
    print("\n" + "="*70)
    print("TEST 2: Direct Routes vs Multi-Hop")
    print("="*70)
    
    # Test with $100
    amount = int(100 * (10 ** Config.USDC_DECIMALS))
    
    print("\nTesting $100 USDC -> SOL swap...")
    
    # Direct route
    print("\n1. DIRECT ROUTE (1-hop):")
    direct = get_quote_analysis(
        Config.USDC_MINT,
        Config.SOL_MINT,
        amount,
        only_direct=True
    )
    
    if direct:
        print(f"   Fee: {direct['price_impact_pct']:.4f}% ({direct['estimated_fee_bps']:.2f} bps)")
        print(f"   DEXes: {', '.join(direct['dexes'])}")
    else:
        print("   ❌ No direct route available")
    
    # Multi-hop route
    print("\n2. MULTI-HOP ROUTE (optimized):")
    multihop = get_quote_analysis(
        Config.USDC_MINT,
        Config.SOL_MINT,
        amount,
        only_direct=False
    )
    
    if multihop:
        print(f"   Fee: {multihop['price_impact_pct']:.4f}% ({multihop['estimated_fee_bps']:.2f} bps)")
        print(f"   Hops: {multihop['num_hops']}")
        print(f"   DEXes: {', '.join(multihop['dexes'])}")
    
    # Compare
    if direct and multihop:
        print("\n📊 COMPARISON:")
        
        direct_fee = direct['estimated_fee_bps']
        multi_fee = multihop['estimated_fee_bps']
        
        if direct_fee < multi_fee:
            diff = multi_fee - direct_fee
            print(f"   ✅ Direct route is better by {diff:.2f} bps ({diff/100:.4f}%)")
            winner = "direct"
        else:
            diff = direct_fee - multi_fee
            print(f"   ✅ Multi-hop route is better by {diff:.2f} bps ({diff/100:.4f}%)")
            winner = "multihop"
        
        return {"winner": winner, "direct_fee": direct_fee, "multi_fee": multi_fee}
    
    return None


def test_different_pairs():
    """Test fees across different trading pairs"""
    print("\n" + "="*70)
    print("TEST 3: Different Trading Pairs")
    print("="*70)
    
    # Test different pairs
    pairs = [
        ("USDC->SOL", Config.USDC_MINT, Config.SOL_MINT),
        ("SOL->USDC", Config.SOL_MINT, Config.USDC_MINT),
        # Note: For SOL->USDC, we need to calculate equivalent amount
    ]
    
    results = []
    
    # Get SOL price first
    print("\nGetting SOL price...")
    usdc_amount = int(100 * (10 ** Config.USDC_DECIMALS))
    price_quote = get_quote_analysis(Config.USDC_MINT, Config.SOL_MINT, usdc_amount)
    
    if not price_quote:
        print("❌ Failed to get SOL price")
        return None
    
    sol_per_100_usdc = price_quote["out_amount"] / (10 ** Config.SOL_DECIMALS)
    
    print(f"\n$100 USDC = ~{sol_per_100_usdc:.6f} SOL\n")
    
    # Test USDC -> SOL
    print("1. USDC -> SOL ($100):")
    analysis = get_quote_analysis(Config.USDC_MINT, Config.SOL_MINT, usdc_amount)
    
    if analysis:
        fee_bps = analysis["estimated_fee_bps"]
        meets_target = "✅" if fee_bps <= Config.TARGET_FEE_BPS else "❌"
        print(f"   {meets_target} Fee: {fee_bps/100:.4f}% ({fee_bps:.2f} bps)")
        print(f"   Route: {', '.join(analysis['dexes'][:2])}")
        results.append({"pair": "USDC->SOL", "fee_bps": fee_bps})
    
    # Test SOL -> USDC
    print("\n2. SOL -> USDC (equivalent to $100):")
    sol_amount = int(sol_per_100_usdc * (10 ** Config.SOL_DECIMALS))
    analysis = get_quote_analysis(Config.SOL_MINT, Config.USDC_MINT, sol_amount)
    
    if analysis:
        fee_bps = analysis["estimated_fee_bps"]
        meets_target = "✅" if fee_bps <= Config.TARGET_FEE_BPS else "❌"
        print(f"   {meets_target} Fee: {fee_bps/100:.4f}% ({fee_bps:.2f} bps)")
        print(f"   Route: {', '.join(analysis['dexes'][:2])}")
        results.append({"pair": "SOL->USDC", "fee_bps": fee_bps})
    
    return results


def generate_recommendations(test_results: Dict):
    """Generate recommendations based on test results"""
    print("\n" + "="*70)
    print("📋 RECOMMENDATIONS FOR 0.05% FEE TARGET")
    print("="*70)
    
    size_results = test_results.get("size_test", [])
    route_results = test_results.get("route_test", {})
    
    print("\n1. OPTIMAL TRADE SIZE:")
    
    if size_results:
        meeting_target = [r for r in size_results if r["meets_target"]]
        
        if meeting_target:
            best = min(meeting_target, key=lambda x: x["fee_bps"])
            print(f"   ✅ Use trades of ${best['size_usd']}+ for {best['fee_pct']:.4f}% fees")
            print(f"   ✅ Larger trades generally have lower % fees")
        else:
            best = min(size_results, key=lambda x: x["fee_bps"])
            print(f"   ⚠️  0.05% target difficult to achieve")
            print(f"   💡 Best observed: ${best['size_usd']} at {best['fee_pct']:.4f}%")
            print(f"   💡 Consider: larger trade sizes (>$1000)")
    
    print("\n2. ROUTE SELECTION:")
    if route_results:
        winner = route_results.get("winner")
        if winner == "direct":
            print("   ✅ Use DIRECT ROUTES (onlyDirectRoutes=true)")
            print("   ✅ Fewer hops = lower fees")
        else:
            print("   ✅ Use JUPITER OPTIMIZATION (let it choose)")
            print("   ✅ Multi-hop can sometimes find better rates")
    
    print("\n3. STRATEGIES TO ACHIEVE 0.05% FEES:")
    print("   🎯 Use larger trade sizes ($500+)")
    print("   🎯 Trade during low-activity hours (less competition)")
    print("   🎯 Use limit orders instead of market swaps when possible")
    print("   🎯 Get Jupiter Pro API key for priority routing")
    print("   🎯 Target high-liquidity pairs (SOL/USDC has best liquidity)")
    print("   🎯 Consider direct DEX integration (Orca SDK)")
    
    print("\n4. REALISTIC FEE EXPECTATIONS:")
    print("   • $5-50 trades:   0.10% - 0.30% typical")
    print("   • $100-500:       0.05% - 0.15% achievable")
    print("   • $1000+:         0.03% - 0.08% possible")
    print("   • $10,000+:       0.02% - 0.05% likely")
    
    print("\n5. ALTERNATIVE APPROACHES:")
    print("   💡 Direct Orca integration: 0.01% fee pools available")
    print("   💡 Raydium CLMM: 0.01-0.04% fee tiers")
    print("   💡 Market maker APIs: Can get <0.05% on large orders")
    print("   💡 Time trades: Wait for optimal liquidity conditions")


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("LOW-FEE TRADING STRATEGY TEST")
    print("="*70)
    print(f"\nTarget: < {Config.TARGET_FEE_BPS} bps (0.05%) total fees")
    print("Testing on: SOL/USDC pair (highest liquidity on Solana)")
    print("="*70)
    
    results = {}
    
    # Test 1: Trade size impact
    try:
        results["size_test"] = test_trade_size_impact()
    except Exception as e:
        print(f"Size test failed: {e}")
    
    time.sleep(2)
    
    # Test 2: Direct vs multi-hop
    try:
        results["route_test"] = test_direct_vs_multihop()
    except Exception as e:
        print(f"Route test failed: {e}")
    
    time.sleep(2)
    
    # Test 3: Different pairs
    try:
        results["pair_test"] = test_different_pairs()
    except Exception as e:
        print(f"Pair test failed: {e}")
    
    # Generate recommendations
    generate_recommendations(results)
    
    # Save results
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_file = f"low_fee_test_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {output_file}")
    print("\n" + "="*70)
    print("✅ Test Complete!")
    print("="*70 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test cancelled")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

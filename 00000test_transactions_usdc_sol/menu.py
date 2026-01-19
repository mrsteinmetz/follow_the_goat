#!/usr/bin/env python3
"""
Simple CLI helper for swap testing
"""

import sys

def show_menu():
    print("\n" + "="*60)
    print("USDC/SOL Swap Test - Helper Menu")
    print("="*60)
    print("\n1. Check wallet configuration")
    print("2. Run swap test ($5 USDC)")
    print("3. View last test results")
    print("4. Show setup instructions")
    print("5. Exit")
    
    choice = input("\nEnter choice (1-5): ").strip()
    
    if choice == "1":
        import subprocess
        subprocess.run([sys.executable, "check_wallet.py"])
    
    elif choice == "2":
        import subprocess
        print("\n⚠️  This will execute real transactions on Solana mainnet!")
        confirm = input("Type 'yes' to continue: ").strip().lower()
        if confirm == "yes":
            subprocess.run([sys.executable, "testswap.py"])
        else:
            print("Cancelled.")
    
    elif choice == "3":
        import os
        import glob
        
        # Find most recent result file
        results = glob.glob("swap_test_*.json")
        if results:
            latest = max(results, key=os.path.getmtime)
            print(f"\nLatest result: {latest}")
            
            import json
            with open(latest, 'r') as f:
                data = json.load(f)
            
            print(f"\nWallet: {data['wallet']}")
            print(f"Timestamp: {data['timestamp']}")
            print(f"Initial USDC: ${data['initial_usdc']:.6f}")
            print(f"Final USDC: ${data['final_usdc']:.6f}")
            print(f"\nTotal Cost: ${data['total_cost_usdc']:.6f} ({data['total_cost_pct']:.3f}%)")
            
            print("\nSwaps:")
            for swap in data['swaps']:
                print(f"  {swap['direction']}: {swap['num_hops']} hops via {', '.join(swap['dexes_used'][:2])}")
                print(f"    Price impact: {swap['price_impact_pct']:.3f}%")
                print(f"    Tx: https://solscan.io/tx/{swap['signature']}")
        else:
            print("\nNo test results found. Run a test first (option 2).")
    
    elif choice == "4":
        print("\n" + "="*60)
        print("SETUP INSTRUCTIONS")
        print("="*60)
        print("\n1. Install dependencies:")
        print("   ./setup.sh")
        print("\n2. Add to .env file:")
        print("   SOLANA_PRIVATE_KEY=your_key_here")
        print("\n3. Fund wallet with $6 USDC")
        print("\n4. Verify: python check_wallet.py")
        print("\n5. Run test: python testswap.py")
        print("\nSee QUICKSTART.md for detailed instructions.")
    
    elif choice == "5":
        print("\nGoodbye!")
        sys.exit(0)
    
    else:
        print("\nInvalid choice!")
    
    input("\nPress ENTER to continue...")
    show_menu()

if __name__ == "__main__":
    try:
        show_menu()
    except KeyboardInterrupt:
        print("\n\nGoodbye!")
        sys.exit(0)

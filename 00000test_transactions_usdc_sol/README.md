# USDC <-> SOL Swap Fee Testing

This script tests round-trip transaction fees for swapping between USDC and SOL using Jupiter aggregator.

## What It Does

1. **Swap 1:** Converts $5 USDC → SOL
2. **Swap 2:** Converts SOL back → USDC
3. **Analyzes:**
   - Total fees paid (network + platform + slippage)
   - Price impact on each swap
   - DEX routes used
   - Final cost as percentage of initial amount

## Setup Instructions

### 1. Install Required Packages

```bash
pip install solders base58 requests python-dotenv
```

### 2. Set Up Your Wallet

You need a Solana wallet with at least $6 USDC (for the $5 test + fees).

**Option A: Export from Phantom Wallet**
1. Open Phantom
2. Settings → Security & Privacy → Export Private Key
3. Copy the private key (base58 format)

**Option B: Export from Solflare**
1. Settings → Show Private Key
2. Copy the private key

**Option C: Use Solana CLI**
```bash
solana-keygen new --outfile ~/my-wallet.json
# Then get the base58 key:
# The private key is in the JSON file, you'll need to convert it
```

### 3. Configure Environment Variables

Create or edit `.env` file in the project root:

```bash
# Solana Configuration
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
SOLANA_PRIVATE_KEY=your_base58_private_key_here

# Optional: Jupiter API key for better rates (free at portal.jup.ag)
JUPITER_API_KEY=your_jupiter_api_key_here
```

**⚠️ SECURITY WARNING:**
- **NEVER** commit your `.env` file to git
- **NEVER** share your private key
- This key has full access to your wallet funds

### 4. Fund Your Wallet with USDC

You need USDC on Solana mainnet. You can:

1. **Buy USDC directly** on Phantom/Solflare
2. **Bridge** from another chain using:
   - [Portal Bridge](https://www.portalbridge.com/)
   - [Allbridge](https://app.allbridge.io/)
3. **Swap** from another token on Jupiter

Make sure you have at least **$6 USDC** to cover:
- $5 for the test swap
- ~$0.01-0.10 for Solana network fees

### 5. Run the Test

```bash
cd /root/follow_the_goat/00000test_transactions_usdc_sol
python testswap.py
```

The script will:
1. Show your initial balances
2. Ask for confirmation before each swap
3. Display real-time progress
4. Save detailed results to a JSON file

## Understanding the Results

### Output Example

```
📊 ROUND-TRIP ANALYSIS
======================================================================

Starting USDC: $5.000000
Ending USDC:   $4.975000
Net Change:    -$0.025000

Total Cost:    $0.025000
Cost %:        0.500%

Swap         Route                Price Impact    DEXes Used
----------------------------------------------------------------------
USDC->SOL    2 hops              0.025%          Orca, Raydium
SOL->USDC    1 hops              0.018%          Orca
```

### What the Numbers Mean

- **Total Cost**: How much you lost in the round trip (fees + slippage)
- **Cost %**: Percentage lost relative to initial amount
- **Price Impact**: How much the swap moved the market price
- **DEXes Used**: Which decentralized exchanges were used for best rates

### Typical Fee Ranges

- **Network fees**: $0.00001 - $0.001 per transaction (Solana is cheap!)
- **Jupiter platform fee**: 0% (Jupiter is free, but routes through DEXs with their own fees)
- **DEX fees**: 0.01% - 0.3% per swap (varies by DEX and pool)
- **Slippage**: 0.01% - 0.5% (depends on order size and liquidity)

**Total round-trip cost: typically 0.3% - 1.0%**

For a $5 test, expect to lose about **$0.015 - $0.05** total.

## Customization

Edit `testswap.py` to change:

```python
# Test amount (line 65)
TEST_AMOUNT_USDC = 5.0  # Change to any amount

# Slippage tolerance (line 71)
SLIPPAGE_BPS = 50  # 50 basis points = 0.5%

# Test different tokens (lines 60-61)
TOKEN_A = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
TOKEN_B = "So11111111111111111111111111111111111111112"   # SOL
```

## Output Files

Results are saved to `swap_test_YYYYMMDD_HHMMSS.json` with:
- Wallet address
- Initial/final balances
- Transaction signatures
- Detailed fee breakdown
- Route information

## Troubleshooting

### "Insufficient USDC balance"
- Transfer more USDC to your wallet
- Check you're using the correct wallet address

### "Failed to get quote"
- Check your internet connection
- Verify RPC URL is correct
- Try adding a Jupiter API key

### "Transaction failed"
- Increase slippage tolerance (try 100 bps = 1%)
- Wait a few seconds and try again
- Check you have enough SOL for network fees (~0.001 SOL)

### "Timeout waiting for confirmation"
- Solana network might be congested
- Check transaction on [Solscan](https://solscan.io/)
- Script will show transaction signature even if it times out

## Safety Features

- **Confirmation prompts** before each swap
- **Balance checks** before executing
- **Detailed logging** of every step
- **JSON export** of all results
- **Solscan links** to verify on blockchain

## Advanced Usage

### Test Different Routes

Compare different slippage settings:

```bash
# Test with 0.1% slippage
python testswap.py --slippage 10

# Test with 1% slippage  
python testswap.py --slippage 100
```

### Use Custom RPC

For faster/more reliable execution:

```bash
# Get a free RPC from:
# - QuickNode: https://www.quicknode.com/
# - Alchemy: https://www.alchemy.com/
# - Helius: https://www.helius.dev/

# Then in .env:
SOLANA_RPC_URL=https://your-custom-rpc-url.com
```

## Additional Resources

- [Jupiter Docs](https://station.jup.ag/docs)
- [Solana Docs](https://docs.solana.com/)
- [Solders (Python SDK)](https://github.com/kevinheavey/solders)
- [USDC on Solana](https://www.circle.com/en/usdc-multichain/solana)

## Questions?

- Check the output JSON file for detailed transaction data
- View transactions on [Solscan](https://solscan.io/) using the signatures
- Jupiter transaction details: https://jup.ag/

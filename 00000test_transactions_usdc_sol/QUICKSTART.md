# Quick Start Guide - USDC/SOL Swap Testing

## 🚀 Fast Setup (5 minutes)

### 1. Install Dependencies
```bash
cd /root/follow_the_goat/00000test_transactions_usdc_sol
./setup.sh
```

Or manually:
```bash
pip install solders base58 requests python-dotenv
```

### 2. Configure Wallet

Add to `/root/follow_the_goat/.env`:

```
SOLANA_PRIVATE_KEY=your_base58_key_here
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
```

**Get your private key from:**
- Phantom: Settings → Security & Privacy → Export Private Key
- Solflare: Settings → Show Private Key

### 3. Fund Wallet

Transfer **$6 USDC** to your Solana wallet:
- $5 for the test
- ~$1 buffer for fees

**USDC Contract:** `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`

### 4. Verify Setup

```bash
python check_wallet.py
```

Should show:
- ✅ Wallet loaded
- ✅ Sufficient USDC
- Your wallet address and balances

### 5. Run Test

```bash
python testswap.py
```

## 📊 What to Expect

### Timeline
- Setup: ~2 minutes
- Swap 1 (USDC→SOL): ~30 seconds
- Swap 2 (SOL→USDC): ~30 seconds
- Total: ~5 minutes

### Costs
For a $5 test swap:
- **Network fees:** ~$0.0001 (Solana is cheap!)
- **DEX fees:** ~$0.01 - $0.02 (0.2% - 0.4%)
- **Slippage:** ~$0.005 - $0.01 (0.1% - 0.2%)
- **Total cost:** ~$0.015 - $0.03 (0.3% - 0.6%)

You'll end with approximately **$4.97 - $4.985** after the round trip.

## 📁 Files Created

After running the test:

```
00000test_transactions_usdc_sol/
├── testswap.py              # Main test script
├── check_wallet.py          # Wallet verification
├── setup.sh                 # Setup script
├── README.md                # Full documentation
├── QUICKSTART.md            # This file
└── swap_test_20260115_123456.json  # Results (timestamped)
```

## 🔍 Reading Results

The JSON output contains:

```json
{
  "timestamp": "2026-01-15T12:34:56",
  "wallet": "Your_Wallet_Address",
  "initial_usdc": 5.0,
  "final_usdc": 4.975,
  "total_cost_usdc": 0.025,
  "total_cost_pct": 0.5,
  "swaps": [
    {
      "direction": "USDC->SOL",
      "signature": "transaction_hash_here",
      "price_impact_pct": 0.025,
      "dexes_used": ["Orca", "Raydium"]
    }
  ]
}
```

**Key metrics:**
- `total_cost_usdc`: Total fees paid
- `total_cost_pct`: Percentage loss
- `dexes_used`: Which DEXes gave best rates

## 🔗 View on Blockchain

After running, you'll get Solscan links:
```
https://solscan.io/tx/YOUR_TRANSACTION_SIGNATURE
```

## 🛠️ Troubleshooting

### "SOLANA_PRIVATE_KEY not set"
→ Add your private key to `.env` file

### "Insufficient USDC balance"
→ Transfer more USDC to your wallet

### "Failed to get quote"
→ Check internet connection
→ Try again (network might be busy)

### "Transaction timeout"
→ Check Solscan link - transaction may still be processing
→ Solana network might be congested

## ⚙️ Customize Test

Edit `testswap.py`:

```python
# Line 65 - Change test amount
TEST_AMOUNT_USDC = 10.0  # Test with $10

# Line 71 - Change slippage tolerance  
SLIPPAGE_BPS = 100  # 1% slippage (for larger amounts)

# Line 60-61 - Test different tokens
TOKEN_A = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC
TOKEN_B = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # USDT
```

## 🔒 Security Checklist

- [ ] `.env` file is NOT committed to git
- [ ] Private key is stored securely
- [ ] Only testing with small amounts ($5-10)
- [ ] Using mainnet (not devnet) for real fee data
- [ ] Verified wallet address before funding

## 📞 Support

If you encounter issues:

1. **Check wallet:** Run `python check_wallet.py`
2. **View logs:** Check console output for error messages
3. **Verify transaction:** Use Solscan link to see blockchain status
4. **Read full docs:** See `README.md` for detailed troubleshooting

## 🎯 What This Tests

✅ **Real transaction fees** on Solana mainnet
✅ **Jupiter aggregator** routing efficiency  
✅ **Slippage costs** for your order size
✅ **Price impact** on different liquidity pools
✅ **Round-trip costs** for USDC ↔ SOL trading

This data helps you understand:
- True cost of trading on Solana
- Whether Jupiter finds good routes
- If $5 trades are efficient (or if larger amounts are better)
- Expected losses from round-trip trading

## 💡 Pro Tips

1. **Get a custom RPC** (free from QuickNode/Helius) for faster execution
2. **Test at different times** - fees vary with network congestion
3. **Compare with CEX** - Coinbase/Binance fees are typically 0.1-0.5% 
4. **Try different amounts** - larger swaps often have better rates
5. **Check multiple DEXes** - Jupiter automatically finds best routes

---

Ready? Run `python testswap.py` to start testing! 🚀

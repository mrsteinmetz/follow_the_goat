#!/usr/bin/env python3
"""
Fee Monitor for Trading Bot
============================
Tracks actual fees paid and alerts if exceeding target

Use this in your production bot to monitor and optimize fees in real-time
"""

import json
import time
from datetime import datetime, timedelta
from typing import List, Dict
from collections import defaultdict


class FeeMonitor:
    """
    Monitor and analyze trading fees in real-time
    
    Usage:
        monitor = FeeMonitor(target_fee_bps=5)  # 0.05% target
        
        # After each trade:
        monitor.log_trade(
            amount_in=100,
            amount_out=99.95,
            token_in="USDC",
            token_out="SOL"
        )
        
        # Check performance:
        stats = monitor.get_stats()
        print(f"Average fee: {stats['avg_fee_pct']:.4f}%")
    """
    
    def __init__(self, target_fee_bps: float = 5.0):
        self.target_fee_bps = target_fee_bps
        self.target_fee_pct = target_fee_bps / 100
        self.trades = []
        
    def log_trade(
        self,
        amount_in: float,
        amount_out: float,
        token_in: str,
        token_out: str,
        signature: str = None,
        metadata: Dict = None
    ):
        """Log a completed trade"""
        
        # Calculate fee (as percentage of input)
        fee_pct = (1 - (amount_out / amount_in)) * 100
        fee_bps = fee_pct * 100
        fee_usd = amount_in * (fee_pct / 100)  # Approximate
        
        trade = {
            "timestamp": datetime.utcnow().isoformat(),
            "amount_in": amount_in,
            "amount_out": amount_out,
            "token_in": token_in,
            "token_out": token_out,
            "fee_pct": fee_pct,
            "fee_bps": fee_bps,
            "fee_usd": fee_usd,
            "meets_target": fee_bps <= self.target_fee_bps,
            "signature": signature,
            "metadata": metadata or {}
        }
        
        self.trades.append(trade)
        
        # Alert if exceeds target significantly
        if fee_bps > self.target_fee_bps * 2:
            print(f"⚠️  HIGH FEE ALERT: {fee_pct:.4f}% ({fee_bps:.1f} bps) on ${amount_in:.2f} trade")
            print(f"   Target: {self.target_fee_pct:.4f}% ({self.target_fee_bps:.1f} bps)")
        
        return trade
    
    def get_stats(self, last_n_trades: int = None) -> Dict:
        """Get statistics on fees"""
        
        if not self.trades:
            return {"error": "No trades logged"}
        
        trades = self.trades[-last_n_trades:] if last_n_trades else self.trades
        
        total_fees_bps = sum(t["fee_bps"] for t in trades)
        total_fees_usd = sum(t["fee_usd"] for t in trades)
        avg_fee_bps = total_fees_bps / len(trades)
        avg_fee_pct = avg_fee_bps / 100
        
        meeting_target = sum(1 for t in trades if t["meets_target"])
        target_rate = (meeting_target / len(trades)) * 100
        
        # Best and worst
        best = min(trades, key=lambda t: t["fee_bps"])
        worst = max(trades, key=lambda t: t["fee_bps"])
        
        # By size
        by_size = defaultdict(list)
        for t in trades:
            if t["amount_in"] < 50:
                by_size["small"].append(t["fee_bps"])
            elif t["amount_in"] < 500:
                by_size["medium"].append(t["fee_bps"])
            else:
                by_size["large"].append(t["fee_bps"])
        
        size_stats = {}
        for size, fees in by_size.items():
            if fees:
                size_stats[size] = sum(fees) / len(fees)
        
        return {
            "total_trades": len(trades),
            "avg_fee_pct": avg_fee_pct,
            "avg_fee_bps": avg_fee_bps,
            "total_fees_usd": total_fees_usd,
            "target_fee_bps": self.target_fee_bps,
            "meeting_target_pct": target_rate,
            "best_fee_pct": best["fee_pct"],
            "worst_fee_pct": worst["fee_pct"],
            "avg_by_size": size_stats
        }
    
    def print_report(self, last_hours: int = 24):
        """Print detailed fee report"""
        
        print("\n" + "="*70)
        print("FEE PERFORMANCE REPORT")
        print("="*70)
        
        # Filter by time
        cutoff = datetime.utcnow() - timedelta(hours=last_hours)
        recent_trades = [
            t for t in self.trades 
            if datetime.fromisoformat(t["timestamp"]) > cutoff
        ]
        
        if not recent_trades:
            print(f"\nNo trades in last {last_hours} hours")
            return
        
        print(f"\nPeriod: Last {last_hours} hours")
        print(f"Trades: {len(recent_trades)}")
        
        stats = self.get_stats(len(recent_trades))
        
        print("\n📊 OVERALL PERFORMANCE:")
        print(f"   Average Fee: {stats['avg_fee_pct']:.4f}% ({stats['avg_fee_bps']:.1f} bps)")
        print(f"   Target Fee:  {self.target_fee_pct:.4f}% ({self.target_fee_bps:.1f} bps)")
        
        if stats['avg_fee_bps'] <= self.target_fee_bps:
            print(f"   ✅ MEETING TARGET!")
        else:
            diff = stats['avg_fee_bps'] - self.target_fee_bps
            print(f"   ❌ Above target by {diff:.1f} bps ({diff/100:.4f}%)")
        
        print(f"\n   Meeting Target: {stats['meeting_target_pct']:.1f}% of trades")
        print(f"   Total Fees Paid: ${stats['total_fees_usd']:.2f}")
        
        print("\n📈 RANGE:")
        print(f"   Best:  {stats['best_fee_pct']:.4f}%")
        print(f"   Worst: {stats['worst_fee_pct']:.4f}%")
        
        if stats['avg_by_size']:
            print("\n📏 BY TRADE SIZE:")
            for size, avg_fee in stats['avg_by_size'].items():
                print(f"   {size.capitalize()}: {avg_fee/100:.4f}% ({avg_fee:.1f} bps)")
        
        print("\n💡 RECOMMENDATIONS:")
        
        if stats['avg_fee_bps'] > self.target_fee_bps * 1.5:
            print("   ⚠️  Fees significantly above target")
            if 'small' in stats['avg_by_size']:
                small_avg = stats['avg_by_size']['small']
                if small_avg > self.target_fee_bps * 2:
                    print("   💡 Small trades have high fees - consider batching")
            print("   💡 Try larger trade sizes")
            print("   💡 Use direct routes (onlyDirectRoutes=true)")
            print("   💡 Consider direct Orca pool integration")
        elif stats['avg_fee_bps'] > self.target_fee_bps:
            print("   ⚡ Close to target - minor optimization possible")
            print("   💡 Time trades during low-activity hours")
            print("   💡 Monitor liquidity depth before trading")
        else:
            print("   ✅ Excellent! Fees are within target")
            print("   💡 Current strategy is working well")
        
        print("\n" + "="*70 + "\n")
    
    def save_to_file(self, filename: str = None):
        """Save trade log to JSON file"""
        if not filename:
            filename = f"fee_log_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        data = {
            "target_fee_bps": self.target_fee_bps,
            "trades": self.trades,
            "stats": self.get_stats()
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Fee log saved to: {filename}")
        return filename
    
    def load_from_file(self, filename: str):
        """Load trade log from JSON file"""
        with open(filename, 'r') as f:
            data = json.load(f)
        
        self.target_fee_bps = data["target_fee_bps"]
        self.trades = data["trades"]
        
        print(f"📂 Loaded {len(self.trades)} trades from {filename}")


def example_usage():
    """Example of how to use FeeMonitor in your bot"""
    
    print("="*70)
    print("FEE MONITOR - Example Usage")
    print("="*70)
    
    # Initialize monitor with 0.05% target
    monitor = FeeMonitor(target_fee_bps=5.0)
    
    # Simulate some trades
    print("\n📝 Simulating trades...\n")
    
    trades = [
        # Small trades (higher fees)
        {"amount_in": 10, "amount_out": 9.97, "token_in": "USDC", "token_out": "SOL"},
        {"amount_in": 25, "amount_out": 24.94, "token_in": "USDC", "token_out": "SOL"},
        
        # Medium trades (better fees)
        {"amount_in": 100, "amount_out": 99.92, "token_in": "USDC", "token_out": "SOL"},
        {"amount_in": 250, "amount_out": 249.85, "token_in": "USDC", "token_out": "SOL"},
        
        # Large trades (best fees)
        {"amount_in": 1000, "amount_out": 999.5, "token_in": "USDC", "token_out": "SOL"},
        {"amount_in": 2500, "amount_out": 2498.75, "token_in": "USDC", "token_out": "SOL"},
    ]
    
    for trade in trades:
        result = monitor.log_trade(**trade)
        print(f"${trade['amount_in']:<6.0f} USDC → SOL: {result['fee_pct']:.4f}% fee "
              f"({'✅' if result['meets_target'] else '❌'})")
        time.sleep(0.5)
    
    # Print report
    monitor.print_report()
    
    # Save to file
    monitor.save_to_file()


if __name__ == "__main__":
    example_usage()

"""
Wallet Executor
===============
Paper trading wallet that mirrors pump signal trades (train_validator only)
with fictional USDC. Runs every 1 second.

Each cycle:
  1. Finds new pump signal buyins (wallet_address LIKE 'PUMP_V4_P%') for wallets
     linked to that play_id. If wallet has available balance, opens a position
     sized at invest_pct (default 20%) of the current available balance.
  2. Finds open wallet_trades whose underlying buyin is now sold/error/no_go.
     Closes the position, calculates P/L with fee_rate (0.05%) on each side,
     and returns net proceeds to the wallet balance.

Balance flow (20% example with $5,000 wallet):
  - New trade fires  → invest $1,000 (20% of $5,000), balance = $4,000
  - Another fires    → invest $800 (20% of $4,000), balance = $3,200
  - First trade sold → P/L calculated, proceeds returned to balance
  - Next trade uses  → 20% of whatever balance is available at that moment

Usage:
    python3 000trading/wallet_executor.py
    python3 scheduler/run_component.py --component wallet_executor
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List

import sys
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres, postgres_execute

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =============================================================================
# SCHEMA INIT
# =============================================================================

def ensure_schema() -> None:
    """Create tables, apply column migrations, and seed test wallet."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            # Main wallets table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallets (
                    id BIGSERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    balance DOUBLE PRECISION NOT NULL,
                    initial_balance DOUBLE PRECISION NOT NULL,
                    is_test BOOLEAN DEFAULT TRUE,
                    play_ids JSONB NOT NULL DEFAULT '[]',
                    fee_rate DOUBLE PRECISION DEFAULT 0.0005,
                    invest_pct DOUBLE PRECISION DEFAULT 0.20,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Safe migration: add invest_pct if it doesn't exist yet
            cur.execute("""
                ALTER TABLE wallets
                ADD COLUMN IF NOT EXISTS invest_pct DOUBLE PRECISION DEFAULT 0.20
            """)

            # Per-trade positions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS wallet_trades (
                    id BIGSERIAL PRIMARY KEY,
                    wallet_id BIGINT NOT NULL,
                    buyin_id BIGINT NOT NULL,
                    play_id INTEGER,
                    status VARCHAR(20) DEFAULT 'open',
                    entry_price DOUBLE PRECISION NOT NULL,
                    position_usdc DOUBLE PRECISION NOT NULL,
                    sol_amount DOUBLE PRECISION NOT NULL,
                    buy_fee_usdc DOUBLE PRECISION NOT NULL,
                    exit_price DOUBLE PRECISION,
                    sell_fee_usdc DOUBLE PRECISION,
                    profit_loss_usdc DOUBLE PRECISION,
                    profit_loss_pct DOUBLE PRECISION,
                    closed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wallet_trades_wallet_id
                    ON wallet_trades (wallet_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wallet_trades_buyin_id
                    ON wallet_trades (buyin_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_wallet_trades_status
                    ON wallet_trades (status)
            """)

            # Seed / upgrade test wallet
            cur.execute("SELECT id, initial_balance FROM wallets LIMIT 1")
            existing = cur.fetchone()
            if not existing:
                cur.execute("""
                    INSERT INTO wallets
                        (name, balance, initial_balance, is_test, play_ids, fee_rate, invest_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, ['Test Wallet', 5000.0, 5000.0, True,
                      json.dumps([3, 4, 5, 6]), 0.0005, 0.20])
                logger.info("Seeded test wallet: $5,000 USDC | 20% per trade | plays [3,4,5,6]")
            elif float(existing['initial_balance']) == 500.0:
                # Upgrade the old $500 seed → $5,000 (add the difference)
                cur.execute("""
                    UPDATE wallets
                    SET initial_balance = 5000.0,
                        balance         = balance + 4500.0,
                        invest_pct      = 0.20,
                        updated_at      = NOW()
                    WHERE id = %s
                """, [existing['id']])
                logger.info("Upgraded test wallet $500 → $5,000 (added $4,500)")

    logger.info("Wallet schema ready")


# =============================================================================
# WALLET LOADER
# =============================================================================

def load_wallets() -> List[Dict[str, Any]]:
    """Load all wallets from the database (fresh read each cycle)."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, balance, initial_balance, is_test,
                       play_ids, fee_rate, invest_pct
                FROM wallets
                ORDER BY id
            """)
            rows = cur.fetchall()

    wallets = []
    for row in rows:
        play_ids = row['play_ids']
        if isinstance(play_ids, str):
            try:
                play_ids = json.loads(play_ids)
            except Exception:
                play_ids = []
        wallets.append({
            'id':              row['id'],
            'name':            row['name'],
            'balance':         float(row['balance']),
            'initial_balance': float(row['initial_balance']),
            'is_test':         bool(row['is_test']),
            'play_ids':        [int(p) for p in (play_ids or [])],
            'fee_rate':        float(row['fee_rate']),
            'invest_pct':      float(row['invest_pct'] if row['invest_pct'] is not None else 0.20),
        })
    return wallets


# =============================================================================
# HELPERS
# =============================================================================

def _reload_balance(wallet: Dict[str, Any]) -> float:
    """Re-read the wallet balance from DB so we always invest against the live figure."""
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM wallets WHERE id = %s", [wallet['id']])
            row = cur.fetchone()
    if row:
        wallet['balance'] = float(row['balance'])
    return wallet['balance']


# =============================================================================
# OPEN POSITIONS
# =============================================================================

def open_new_positions(wallet: Dict[str, Any]) -> int:
    """Open wallet positions for any new pump signal buyins not yet tracked.

    Invests invest_pct (default 20%) of the current available balance per trade.
    Multiple concurrent positions are allowed — balance is simply decremented
    by the position size each time.

    Returns number of positions opened this cycle.
    """
    play_ids = wallet['play_ids']
    if not play_ids:
        return 0

    # Always read fresh balance from DB before calculating position size
    balance = _reload_balance(wallet)
    if balance <= 0:
        return 0

    # Find all untracked pump signal buyins for this wallet's plays, oldest first
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.id, b.play_id, b.our_entry_price, b.our_status
                FROM follow_the_goat_buyins b
                WHERE b.wallet_address LIKE 'PUMP_V4_P%%'
                  AND b.play_id = ANY(%s)
                  AND b.our_entry_price IS NOT NULL
                  AND b.our_entry_price > 0
                  AND NOT EXISTS (
                      SELECT 1 FROM wallet_trades wt
                      WHERE wt.buyin_id = b.id
                        AND wt.wallet_id = %s
                  )
                ORDER BY b.id ASC
                LIMIT 20
            """, [play_ids, wallet['id']])
            new_buyins = cur.fetchall()

    opened = 0
    for buyin in new_buyins:
        # Re-check balance each iteration — it decreases as we open positions
        if wallet['balance'] <= 0:
            break

        # Buyins that are already resolved (we missed while balance was tied up)
        # → mark as 'missed' so the NOT EXISTS never re-scans them
        if buyin['our_status'] in ('no_go', 'error', 'sold', 'completed'):
            postgres_execute("""
                INSERT INTO wallet_trades
                    (wallet_id, buyin_id, play_id, status, entry_price,
                     position_usdc, sol_amount, buy_fee_usdc)
                VALUES (%s, %s, %s, 'missed', %s, 0, 0, 0)
            """, [wallet['id'], buyin['id'], buyin['play_id'],
                  float(buyin['our_entry_price'])])
            logger.debug(
                f"[Wallet #{wallet['id']}] Marked buyin #{buyin['id']} as missed "
                f"(status={buyin['our_status']})"
            )
            continue

        # Still pending — open a position at invest_pct of current available balance
        entry_price  = float(buyin['our_entry_price'])
        position_usdc = round(wallet['balance'] * wallet['invest_pct'], 4)
        buy_fee       = round(position_usdc * wallet['fee_rate'], 8)
        sol_amount    = round((position_usdc - buy_fee) / entry_price, 8)

        postgres_execute("""
            INSERT INTO wallet_trades
                (wallet_id, buyin_id, play_id, status, entry_price,
                 position_usdc, sol_amount, buy_fee_usdc)
            VALUES (%s, %s, %s, 'open', %s, %s, %s, %s)
        """, [wallet['id'], buyin['id'], buyin['play_id'],
              entry_price, position_usdc, sol_amount, buy_fee])

        # Deduct position from available balance (not locked to 0 — partial investment)
        postgres_execute("""
            UPDATE wallets
            SET balance    = balance - %s,
                updated_at = NOW()
            WHERE id = %s
        """, [position_usdc, wallet['id']])
        wallet['balance'] = round(wallet['balance'] - position_usdc, 4)

        pct_label = f"{wallet['invest_pct'] * 100:.0f}%"
        logger.info(
            f"[Wallet #{wallet['id']} '{wallet['name']}'] Opened: "
            f"buyin #{buyin['id']} play={buyin['play_id']} "
            f"@ ${entry_price:.4f} | {pct_label} = ${position_usdc:.2f} USDC "
            f"({sol_amount:.6f} SOL, fee ${buy_fee:.4f}) | "
            f"remaining balance: ${wallet['balance']:.2f}"
        )
        opened += 1

    return opened


# =============================================================================
# CLOSE POSITIONS
# =============================================================================

def close_resolved_positions(wallet: Dict[str, Any]) -> int:
    """Close wallet_trades whose underlying buyin is now sold/error/no_go.

    Returns number of positions closed.
    """
    with get_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wt.id   AS wt_id,
                       wt.buyin_id,
                       wt.entry_price,
                       wt.position_usdc,
                       wt.sol_amount,
                       wt.buy_fee_usdc,
                       wt.play_id,
                       b.our_status,
                       b.our_exit_price
                FROM wallet_trades wt
                JOIN follow_the_goat_buyins b ON b.id = wt.buyin_id
                WHERE wt.wallet_id = %s
                  AND wt.status    = 'open'
                  AND b.our_status IN ('sold', 'completed', 'error', 'no_go')
            """, [wallet['id']])
            open_trades = cur.fetchall()

    closed = 0
    for wt in open_trades:
        buyin_status  = wt['our_status']
        position_usdc = float(wt['position_usdc'])
        sol_amount    = float(wt['sol_amount'])

        if buyin_status in ('error', 'no_go'):
            # Refund the original position — no trade executed
            postgres_execute("""
                UPDATE wallet_trades
                SET status = 'cancelled', closed_at = NOW()
                WHERE id = %s
            """, [wt['wt_id']])
            postgres_execute("""
                UPDATE wallets
                SET balance    = balance + %s,
                    updated_at = NOW()
                WHERE id = %s
            """, [position_usdc, wallet['id']])
            wallet['balance'] += position_usdc
            logger.info(
                f"[Wallet #{wallet['id']}] Refunded ${position_usdc:.2f} — "
                f"buyin #{wt['buyin_id']} {buyin_status} | "
                f"balance: ${wallet['balance']:.2f}"
            )
            closed += 1
            continue

        # Sold — calculate P/L
        exit_price = wt['our_exit_price']
        if exit_price is None or float(exit_price) <= 0:
            # Exit price not yet written — retry next cycle
            continue

        exit_price     = float(exit_price)
        gross_proceeds = round(sol_amount * exit_price, 8)
        sell_fee       = round(gross_proceeds * wallet['fee_rate'], 8)
        net_proceeds   = round(gross_proceeds - sell_fee, 8)
        pl_usdc        = round(net_proceeds - position_usdc, 8)
        pl_pct         = round((pl_usdc / position_usdc) * 100, 4) if position_usdc > 0 else 0.0

        postgres_execute("""
            UPDATE wallet_trades
            SET status           = 'closed',
                exit_price       = %s,
                sell_fee_usdc    = %s,
                profit_loss_usdc = %s,
                profit_loss_pct  = %s,
                closed_at        = NOW()
            WHERE id = %s
        """, [exit_price, sell_fee, pl_usdc, pl_pct, wt['wt_id']])

        postgres_execute("""
            UPDATE wallets
            SET balance    = balance + %s,
                updated_at = NOW()
            WHERE id = %s
        """, [net_proceeds, wallet['id']])
        wallet['balance'] = round(wallet['balance'] + net_proceeds, 4)

        sign = '+' if pl_usdc >= 0 else ''
        logger.info(
            f"[Wallet #{wallet['id']} '{wallet['name']}'] Closed: "
            f"buyin #{wt['buyin_id']} play={wt['play_id']} "
            f"entry=${float(wt['entry_price']):.4f} exit=${exit_price:.4f} | "
            f"P/L: {sign}${pl_usdc:.4f} ({sign}{pl_pct:.2f}%) | "
            f"balance: ${wallet['balance']:.2f}"
        )
        closed += 1

    return closed


# =============================================================================
# MAIN CYCLE
# =============================================================================

_schema_ready = False


def run_wallet_cycle() -> None:
    """Execute one wallet executor cycle: close resolved positions, then open new ones."""
    global _schema_ready
    if not _schema_ready:
        ensure_schema()
        _schema_ready = True

    wallets = load_wallets()
    for wallet in wallets:
        try:
            closed = close_resolved_positions(wallet)
            opened = open_new_positions(wallet)
            if opened or closed:
                logger.debug(
                    f"[Wallet #{wallet['id']}] cycle: opened={opened} closed={closed} "
                    f"balance=${wallet['balance']:.2f}"
                )
        except Exception as e:
            logger.error(
                f"Error processing wallet #{wallet['id']} '{wallet['name']}': {e}",
                exc_info=True,
            )


def run_continuous(interval_seconds: float = 1.0) -> None:
    """Run the wallet executor loop continuously."""
    logger.info("=" * 60)
    logger.info("WALLET EXECUTOR STARTED")
    logger.info(f"  Interval: {interval_seconds}s")
    logger.info("=" * 60)

    ensure_schema()

    while True:
        try:
            run_wallet_cycle()
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — shutting down")
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(interval_seconds)

    logger.info("Wallet executor stopped")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_continuous()

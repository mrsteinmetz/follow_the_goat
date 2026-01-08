"""
FastAPI webhook to receive QuickNode payloads and write directly to
PostgreSQL with 24h hot storage.

Designed to be tolerant of varying payload shapes. Any missing fields will
be skipped, but the endpoint will still return 200 to avoid QuickNode
retries failing the pipeline.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from core.database import get_postgres, postgres_insert
from features.webhook.parser import parse_timestamp
from features.webhook.models import TradePayload, WhalePayload

logger = logging.getLogger("webhook_api")

# File logging for webhook (placed in project-level logs/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
log_file = LOGS_DIR / "webhook.log"
handler = RotatingFileHandler(log_file, maxBytes=2 * 1024 * 1024, backupCount=3)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI(title="Follow The Goat - Webhook", version="1.0.0")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _check_db():
    """Check if PostgreSQL is available."""
    try:
        with get_postgres() as conn:
            return True
    except Exception as e:
        logger.error(f"PostgreSQL not available: {e}")
        raise HTTPException(status_code=503, detail="PostgreSQL not available")


def _next_id(engine, table: str) -> int:
    result = engine.read_one(f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {table}")
    return int(result["next_id"]) if result else 1


def _upsert_trade(payload: TradePayload) -> int:
    """Insert trade into DuckDB AND queue for PostgreSQL (dual-write)."""
    engine = _engine()
    trade_id = payload.id or _next_id(engine, "sol_stablecoin_trades")
    ts = parse_timestamp(payload.trade_timestamp) or datetime.utcnow()
    created_at = datetime.utcnow()
    
    # Write to DuckDB (fast, in-memory)
    engine.execute(
        """
        INSERT OR REPLACE INTO sol_stablecoin_trades
        (id, wallet_address, signature, trade_timestamp,
         stablecoin_amount, sol_amount, price, direction,
         perp_direction, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            trade_id,
            payload.wallet_address,
            payload.signature,
            ts,
            payload.stablecoin_amount,
            payload.sol_amount,
            payload.price,
            payload.direction,
            payload.perp_direction,
            created_at,
        ],
    )
    
    # DUAL-WRITE: Queue for async PostgreSQL (fire-and-forget, never blocks)
    try:
        from core.database import write_to_postgres_async
        write_to_postgres_async("sol_stablecoin_trades", {
            "id": trade_id,
            "wallet_address": payload.wallet_address,
            "signature": payload.signature,
            "trade_timestamp": ts,
            "stablecoin_amount": payload.stablecoin_amount,
            "sol_amount": payload.sol_amount,
            "price": payload.price,
            "direction": payload.direction,
            "perp_direction": payload.perp_direction,
            "created_at": created_at,
        })
    except Exception as e:
        # PostgreSQL write is optional - don't fail the trade if it errors
        logger.debug(f"PostgreSQL dual-write skipped for trade {trade_id}: {e}")
    
    return trade_id


def _upsert_whale(payload: WhalePayload) -> int:
    """Insert whale movement into DuckDB AND queue for PostgreSQL (dual-write)."""
    engine = _engine()
    whale_id = payload.id or _next_id(engine, "whale_movements")
    ts = parse_timestamp(payload.timestamp) or datetime.utcnow()
    received_at = parse_timestamp(payload.received_at) or datetime.utcnow()
    created_at = datetime.utcnow()
    
    # Write to DuckDB (fast, in-memory)
    engine.execute(
        """
        INSERT OR REPLACE INTO whale_movements
        (id, signature, wallet_address, whale_type, current_balance,
         sol_change, abs_change, percentage_moved, direction, action,
         movement_significance, previous_balance, fee_paid, block_time,
         timestamp, received_at, slot, has_perp_position, perp_platform,
         perp_direction, perp_size, perp_leverage, perp_entry_price,
         raw_data_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            whale_id,
            payload.signature,
            payload.wallet_address,
            payload.whale_type,
            payload.current_balance,
            payload.sol_change,
            payload.abs_change,
            payload.percentage_moved,
            payload.direction,
            payload.action,
            payload.movement_significance,
            payload.previous_balance,
            payload.fee_paid,
            payload.block_time,
            ts,
            received_at,
            payload.slot,
            payload.has_perp_position,
            payload.perp_platform,
            payload.perp_direction,
            payload.perp_size,
            payload.perp_leverage,
            payload.perp_entry_price,
            payload.raw_data_json,
            created_at,
        ],
    )
    
    # DUAL-WRITE: Queue for async PostgreSQL (fire-and-forget, never blocks)
    try:
        from core.database import write_to_postgres_async
        write_to_postgres_async("whale_movements", {
            "id": whale_id,
            "signature": payload.signature,
            "wallet_address": payload.wallet_address,
            "whale_type": payload.whale_type,
            "current_balance": payload.current_balance,
            "sol_change": payload.sol_change,
            "abs_change": payload.abs_change,
            "percentage_moved": payload.percentage_moved,
            "direction": payload.direction,
            "action": payload.action,
            "movement_significance": payload.movement_significance,
            "previous_balance": payload.previous_balance,
            "fee_paid": payload.fee_paid,
            "block_time": payload.block_time,
            "timestamp": ts,
            "received_at": received_at,
            "slot": payload.slot,
            "has_perp_position": payload.has_perp_position,
            "perp_platform": payload.perp_platform,
            "perp_direction": payload.perp_direction,
            "perp_size": payload.perp_size,
            "perp_leverage": payload.perp_leverage,
            "perp_entry_price": payload.perp_entry_price,
            "raw_data_json": payload.raw_data_json,
            "created_at": created_at,
        })
    except Exception as e:
        # PostgreSQL write is optional - don't fail the whale if it errors
        logger.debug(f"PostgreSQL dual-write skipped for whale {whale_id}: {e}")
    
    return whale_id


def _normalize_trade_dict(d: Dict[str, Any]) -> Optional[TradePayload]:
    wallet = d.get("wallet_address") or d.get("wallet") or d.get("owner") or d.get("walletAddress")
    if not wallet:
        return None
    return TradePayload(
        id=d.get("id"),
        signature=d.get("signature") or d.get("tx_signature") or d.get("transaction"),
        wallet_address=wallet,
        direction=d.get("direction") or d.get("side") or d.get("action"),
        sol_amount=d.get("sol_amount") or d.get("amount_sol") or d.get("sol"),
        stablecoin_amount=d.get("stablecoin_amount") or d.get("usdc_amount") or d.get("amount_usdc"),
        price=d.get("price") or d.get("mark_price") or d.get("avg_price"),
        perp_direction=d.get("perp_direction") or d.get("perp_side"),
        trade_timestamp=d.get("trade_timestamp") or d.get("timestamp") or d.get("block_time"),
    )


def _normalize_whale_dict(d: Dict[str, Any]) -> Optional[WhalePayload]:
    wallet = d.get("wallet_address") or d.get("wallet") or d.get("owner") or d.get("walletAddress")
    if not wallet:
        return None
    return WhalePayload(
        id=d.get("id"),
        signature=d.get("signature") or d.get("tx_signature") or d.get("transaction"),
        wallet_address=wallet,
        whale_type=d.get("whale_type") or d.get("type"),
        current_balance=d.get("current_balance") or d.get("balance"),
        sol_change=d.get("sol_change") or d.get("delta_sol"),
        abs_change=d.get("abs_change") or d.get("abs_delta"),
        percentage_moved=d.get("percentage_moved") or d.get("pct_moved"),
        direction=d.get("direction") or d.get("side") or d.get("action"),
        action=d.get("action"),
        movement_significance=d.get("movement_significance") or d.get("significance"),
        previous_balance=d.get("previous_balance"),
        fee_paid=d.get("fee_paid") or d.get("fee"),
        block_time=d.get("block_time") or d.get("slot"),
        timestamp=d.get("timestamp") or d.get("trade_timestamp") or d.get("block_time"),
        received_at=d.get("received_at"),
        slot=d.get("slot"),
        has_perp_position=d.get("has_perp_position"),
        perp_platform=d.get("perp_platform"),
        perp_direction=d.get("perp_direction"),
        perp_size=d.get("perp_size"),
        perp_leverage=d.get("perp_leverage"),
        perp_entry_price=d.get("perp_entry_price"),
        raw_data_json=d.get("raw_data_json") or d.get("raw"),
    )


def _ensure_list(payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #


@app.post("/")
@app.post("/webhook")
@app.post("/webhook/")
async def webhook_trades(payload: Union[Dict[str, Any], List[Dict[str, Any]]]):
    """
    Receive trade transactions from QuickNode webhook.
    
    Expected payload format:
    {
        "matchedTransactions": [...], 
        "totalMatched": N,
        "blockHeight": ...,
        ...metadata...
    }
    
    Note: Accepts /, /webhook, and /webhook/ to match .NET behavior
    """
    # DEBUG: Log the raw payload to see what QuickNode is actually sending
    try:
        import json
        payload_str = json.dumps(payload, default=str)
        logger.info(f"[DEBUG] Raw payload received: {payload_str[:2000]}")
    except Exception as e:
        logger.warning(f"[DEBUG] Could not serialize payload: {e}")
    
    raw_items = _ensure_list(payload)
    items: List[Dict[str, Any]] = []
    
    # Extract matchedTransactions from wrapper payload
    for item in raw_items:
        if isinstance(item, dict):
            # Log the keys to see what fields are present
            logger.info(f"[DEBUG] Payload keys: {list(item.keys())}")
            
            txs = item.get("matchedTransactions") or item.get("transactions") or []
            logger.info(f"[DEBUG] Found matchedTransactions: {len(txs) if isinstance(txs, list) else 'not a list'}")
            
            if isinstance(txs, list) and txs:
                items.extend(txs)
                logger.info(f"[DEBUG] Added {len(txs)} transactions to items")
            # If there are no matchedTransactions but the dict looks like an actual trade
            # (has wallet_address), then treat it as a trade
            elif item.get("wallet_address") or item.get("signature"):
                items.append(item)
                logger.info(f"[DEBUG] Added item as direct trade (has wallet_address/signature)")
    
    logger.info(f"[DEBUG] Total items to process: {len(items)}")
    
    # If no transactions found, return early (this is normal when QuickNode
    # sends metadata-only payloads with matchedTransactions: [])
    if not items:
        return {"success": True, "inserted": 0, "received": 0}
    
    # Process transactions
    inserted = 0
    for item in items:
        tp = _normalize_trade_dict(item)
        if not tp:
            logger.warning(f"Trade payload skipped (no wallet_address): {item}")
            continue
        try:
            _upsert_trade(tp)
            inserted += 1
        except Exception as e:
            logger.error(f"Trade upsert failed: {e}; payload={item}")
    
    logger.info(f"/webhook received={len(items)} inserted={inserted}")
    return {"success": True, "inserted": inserted, "received": len(items)}


@app.post("/webhooks/whale-activity")
@app.post("/webhook/whale-activity")
@app.post("/webhook/whale-activity/")
async def webhook_whale(payload: Union[Dict[str, Any], List[Dict[str, Any]]]):
    raw_items = _ensure_list(payload)
    items: List[Dict[str, Any]] = []
    for item in raw_items:
        if isinstance(item, dict):
            movements = item.get("whaleMovements") or item.get("movements") or []
            if isinstance(movements, list) and movements:
                items.extend(movements)
            else:
                items.append(item)
    if not items:
        items = raw_items
    inserted = 0
    for item in items:
        wp = _normalize_whale_dict(item)
        if not wp:
            logger.warning(f"Whale payload skipped (no wallet): {item}")
            continue
        try:
            _upsert_whale(wp)
            inserted += 1
        except Exception as e:
            logger.error(f"Whale upsert failed: {e}; payload={item}")
    logger.info(f"/webhook/whale-activity received={len(items)} inserted={inserted}")
    return {"success": True, "inserted": inserted, "received": len(items)}


@app.get("/webhook/health")
async def webhook_health():
    try:
        engine = _engine()
        trades = engine.read_one("SELECT COUNT(*) AS cnt FROM sol_stablecoin_trades")
        whales = engine.read_one("SELECT COUNT(*) AS cnt FROM whale_movements")
        
        # Get first (oldest) transaction timestamp
        first_trade = engine.read_one(
            "SELECT trade_timestamp FROM sol_stablecoin_trades ORDER BY trade_timestamp ASC LIMIT 1"
        )
        
        return {
            "status": "ok",
            "timestamp": datetime.utcnow().isoformat(),
            "duckdb": {
                "trades_in_hot_storage": trades["cnt"] if trades else 0,
                "whale_movements_in_hot_storage": whales["cnt"] if whales else 0,
                "first_trade_timestamp": first_trade["trade_timestamp"].isoformat() if first_trade and first_trade.get("trade_timestamp") else None,
                "retention": "24 hours",
            },
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.get("/webhook/api/trades")
async def api_trades(
    limit: Optional[int] = 100,
    after_id: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    engine = _engine()
    where_clauses: List[str] = []
    if after_id is not None:
        where_clauses.append(f"id > {after_id}")
    if start:
        ts = parse_timestamp(start)
        if ts:
            where_clauses.append(f"trade_timestamp >= '{ts}'")
    if end:
        ts = parse_timestamp(end)
        if ts:
            where_clauses.append(f"trade_timestamp <= '{ts}'")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    max_limit = 5000 if after_id is not None else 1000
    limit_val = min(limit or max_limit, max_limit)
    rows = engine.read(
        f"""
        SELECT id, wallet_address, signature, trade_timestamp,
               stablecoin_amount, sol_amount, price, direction,
               perp_direction, created_at
        FROM sol_stablecoin_trades
        {where_sql}
        ORDER BY trade_timestamp DESC, id DESC
        LIMIT ?
        """,
        [limit_val],
    )
    return {
        "success": True,
        "source": "trading_engine",
        "count": len(rows),
        "results": rows,
        "max_id": max((r["id"] for r in rows), default=after_id or 0),
    }


@app.get("/webhook/api/whale-movements")
async def api_whales(
    limit: Optional[int] = 100,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    engine = _engine()
    where_clauses: List[str] = []
    if start:
        ts = parse_timestamp(start)
        if ts:
            where_clauses.append(f"timestamp >= '{ts}'")
    if end:
        ts = parse_timestamp(end)
        if ts:
            where_clauses.append(f"timestamp <= '{ts}'")
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    limit_val = min(limit or 100, 1000)
    rows = engine.read(
        f"""
        SELECT id, signature, wallet_address, whale_type, current_balance,
               sol_change, abs_change, percentage_moved, direction, action,
               movement_significance, previous_balance, fee_paid, block_time,
               timestamp, received_at, slot, has_perp_position, perp_platform,
               perp_direction, perp_size, perp_leverage, perp_entry_price,
               raw_data_json, created_at
        FROM whale_movements
        {where_sql}
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        [limit_val],
    )
    return {
        "success": True,
        "source": "trading_engine",
        "count": len(rows),
        "results": rows,
    }

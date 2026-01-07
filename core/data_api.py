"""
Data Engine API - FastAPI Server for DuckDB Access
===================================================
Central API for accessing TradingDataEngine's in-memory DuckDB.

This API provides:
- POST /insert - Queue write to DuckDB (returns immediately)
- POST /query - Execute SELECT query, return results
- GET /health - Health check with stats
- GET /backfill - Get last N hours of data for startup

Usage:
    # Run standalone:
    python core/data_api.py
    
    # Or import and use with uvicorn:
    from core.data_api import app
    uvicorn.run(app, host="0.0.0.0", port=5050)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
import logging
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger("data_api")

app = FastAPI(
    title="Follow The Goat - Data Engine API",
    description="Central API for accessing trading data from in-memory DuckDB",
    version="1.0.0"
)

# CORS for web access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Request/Response Models
# =============================================================================

class InsertRequest(BaseModel):
    """Request model for /insert endpoint."""
    table: str
    data: Dict[str, Any]


class InsertBatchRequest(BaseModel):
    """Request model for /insert/batch endpoint."""
    table: str
    records: List[Dict[str, Any]]


class QueryRequest(BaseModel):
    """Request model for /query endpoint."""
    sql: str
    params: Optional[List[Any]] = None


class QueryResponse(BaseModel):
    """Response model for /query endpoint."""
    success: bool
    count: int
    results: List[Dict[str, Any]]
    error: Optional[str] = None


# =============================================================================
# Helper Functions
# =============================================================================

def _get_engine():
    """Get TradingDataEngine, raise HTTPException if not available."""
    try:
        from core.database import get_trading_engine
        engine = get_trading_engine()
        if not engine or not getattr(engine, "_running", False):
            raise HTTPException(
                status_code=503,
                detail="TradingDataEngine not running. Start master.py first."
            )
        return engine
    except ImportError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to import trading engine: {e}"
        )


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a row for JSON response (handle datetime, etc.)."""
    result = {}
    for key, value in row.items():
        if hasattr(value, 'isoformat'):
            result[key] = value.isoformat()
        elif isinstance(value, bytes):
            result[key] = value.decode('utf-8', errors='replace')
        else:
            result[key] = value
    return result


# =============================================================================
# API Endpoints
# =============================================================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint with engine statistics.
    
    Returns:
        - status: "ok" or "degraded"
        - engine_running: bool
        - stats: engine statistics
        - timestamp: current time
    """
    try:
        engine = _get_engine()
        stats = engine.get_stats()
        health = engine.health_check()
        
        return {
            "status": "ok" if health.get("duckdb") == "ok" else "degraded",
            "engine_running": True,
            "stats": stats,
            "health": health,
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        return {
            "status": "error",
            "engine_running": False,
            "message": "TradingDataEngine not running",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


@app.post("/insert")
async def insert_record(request: InsertRequest):
    """
    Queue a write operation to DuckDB (non-blocking).
    
    The write is queued and processed asynchronously by the batch writer.
    Returns immediately without waiting for the write to complete.
    
    Args:
        table: Table name (e.g., "prices", "trades", "orderbook")
        data: Dictionary of column -> value
    
    Returns:
        - success: bool
        - queued: bool (always True on success)
    """
    engine = _get_engine()
    
    try:
        engine.write(request.table, request.data)
        return {
            "success": True,
            "queued": True,
            "table": request.table
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.post("/insert/batch")
async def insert_batch(request: InsertBatchRequest):
    """
    Queue multiple write operations to DuckDB (non-blocking).
    
    Args:
        table: Table name
        records: List of dictionaries to insert
    
    Returns:
        - success: bool
        - queued_count: number of records queued
    """
    engine = _get_engine()
    
    try:
        engine.write_batch(request.table, request.records)
        return {
            "success": True,
            "queued_count": len(request.records),
            "table": request.table
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch insert failed: {e}")


@app.post("/insert/sync")
async def insert_sync(request: InsertRequest):
    """
    Insert a record synchronously and return the generated ID.
    
    Use this when you need the ID immediately after insert.
    
    Args:
        table: Table name
        data: Dictionary of column -> value
    
    Returns:
        - success: bool
        - id: generated record ID
    """
    engine = _get_engine()
    
    try:
        record_id = engine.write_sync(request.table, request.data)
        return {
            "success": True,
            "id": record_id,
            "table": request.table
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync insert failed: {e}")


@app.post("/query")
async def execute_query(request: QueryRequest):
    """
    Execute a SELECT query and return results.
    
    Args:
        sql: SQL query string (SELECT only)
        params: Optional query parameters
    
    Returns:
        - success: bool
        - count: number of rows returned
        - results: list of result dictionaries
    """
    engine = _get_engine()
    
    # Basic security: only allow SELECT queries
    sql_upper = request.sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT queries are allowed. Use /insert for writes."
        )
    
    try:
        results = engine.read(request.sql, request.params or [])
        serialized = [_serialize_row(row) for row in results]
        
        return {
            "success": True,
            "count": len(serialized),
            "results": serialized
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")


@app.get("/backfill/{table}")
async def get_backfill_data(
    table: str,
    hours: int = Query(default=None, ge=1, le=24, description="Hours of data to retrieve (use hours OR minutes)"),
    minutes: int = Query(default=None, ge=1, le=60, description="Minutes of data to retrieve (for short intervals)"),
    limit: int = Query(default=10000, ge=1, le=100000, description="Maximum records to return")
):
    """
    Get historical data for backfill on startup.
    
    This endpoint is used by master2.py to load recent data when it starts.
    Use 'hours' for initial backfill (1-24 hours) or 'minutes' for sync (1-60 minutes).
    
    Args:
        table: Table name to query
        hours: Hours of data to retrieve (default: 2, max: 24) - use for startup backfill
        minutes: Minutes of data to retrieve (max: 60) - use for continuous sync
        limit: Maximum records to return (default: 10000, max: 100000)
    
    Returns:
        - success: bool
        - count: number of rows
        - results: list of records
        - cutoff_time: timestamp used for filtering
    """
    engine = _get_engine()
    
    # Map table names to their timestamp columns
    timestamp_columns = {
        "prices": "ts",
        "price_points": "created_at",
        "transactions": "ts",
        "orderbook": "ts",
        "trades": "ts",
        "price_analysis": "created_at",
        "cycle_tracker": "cycle_start_time",
        "order_book_features": "ts",
        "wallet_profiles": "trade_timestamp",
        "follow_the_goat_buyins": "followed_at",
        "buyin_trail_minutes": "created_at",
        "sol_stablecoin_trades": "trade_timestamp",
        "follow_the_goat_plays": "created_at",
        "whale_movements": "timestamp",
    }
    
    ts_col = timestamp_columns.get(table)
    if not ts_col:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown table '{table}' or no timestamp column defined"
        )
    
    # Calculate cutoff time - use UTC since all timestamps in DB are UTC
    if minutes is not None:
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        time_desc = f"{minutes} minutes"
    elif hours is not None:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        time_desc = f"{hours} hours"
    else:
        # Default to 2 hours if neither specified
        cutoff = datetime.utcnow() - timedelta(hours=2)
        time_desc = "2 hours (default)"
    
    try:
        # Special handling for cycle_tracker: get ALL cycles (no time filter)
        # Cycles are cumulative - we need all cycles for website display
        # Active cycles have cycle_end_time IS NULL, completed cycles have end_time set
        if table == "cycle_tracker":
            results = engine.read(
                f"""SELECT * FROM {table} 
                    ORDER BY id DESC LIMIT ?""",
                [limit]
            )
        else:
            # Query with time filter
            results = engine.read(
                f"SELECT * FROM {table} WHERE {ts_col} >= ? ORDER BY {ts_col} DESC LIMIT ?",
                [cutoff, limit]
            )
        serialized = [_serialize_row(row) for row in results]
        
        return {
            "success": True,
            "table": table,
            "count": len(serialized),
            "records": serialized,  # Use 'records' for consistency with DataClient
            "cutoff_time": cutoff.isoformat(),
            "time_range": time_desc
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backfill query failed: {e}")


@app.get("/tables")
async def list_tables():
    """
    List all available tables and their row counts.
    
    Returns:
        - tables: dict of table_name -> row_count
    """
    engine = _get_engine()
    stats = engine.get_stats()
    
    return {
        "success": True,
        "tables": stats.get("table_counts", {}),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/latest/{table}")
async def get_latest(
    table: str,
    limit: int = Query(default=100, ge=1, le=1000, description="Number of records"),
    token: Optional[str] = Query(default=None, description="Token filter (for prices/trades)")
):
    """
    Get the latest records from a table.
    
    Args:
        table: Table name
        limit: Number of records to return
        token: Optional token filter (for prices, trades, orderbook)
    
    Returns:
        - success: bool
        - count: number of rows
        - results: list of records
    """
    engine = _get_engine()
    
    try:
        results = engine.get_latest(table, token=token, limit=limit)
        serialized = [_serialize_row(row) for row in results]
        
        return {
            "success": True,
            "table": table,
            "count": len(serialized),
            "results": serialized
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get latest: {e}")


@app.get("/sync/{table}")
async def get_new_records(
    table: str,
    since_id: int = Query(default=0, ge=0, description="Get records with id > since_id"),
    limit: int = Query(default=1000, ge=1, le=10000, description="Maximum records to return")
):
    """
    Get NEW records since a specific ID (for incremental sync).
    
    This is optimized for continuous sync - only fetches records that haven't been synced yet.
    Much more efficient than time-based backfill for real-time trading.
    
    Args:
        table: Table name
        since_id: Get records with ID greater than this value
        limit: Maximum records to return (default: 1000)
    
    Returns:
        - success: bool
        - count: number of new records
        - records: list of new records
        - max_id: highest ID in results (use this as since_id for next sync)
    """
    engine = _get_engine()
    
    # All tables have 'id' as primary key
    valid_tables = [
        "prices", "price_points", "order_book_features", 
        "sol_stablecoin_trades", "whale_movements", "wallet_profiles",
        "follow_the_goat_buyins", "buyin_trail_minutes", "cycle_tracker"
    ]
    
    if table not in valid_tables:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown table '{table}'. Valid tables: {valid_tables}"
        )
    
    try:
        # Special handling for cycle_tracker: ALWAYS include ALL active cycles
        # This ensures master2 sees cycle closures (when cycle_end_time is set)
        # Regular sync only returns NEW records, but cycle closures UPDATE existing records
        if table == "cycle_tracker":
            # Get ALL active cycles (cycle_end_time IS NULL) regardless of ID
            # Plus any new cycles (id > since_id)
            results = engine.read(
                f"""SELECT * FROM {table} 
                    WHERE cycle_end_time IS NULL OR id > ? 
                    ORDER BY id ASC 
                    LIMIT ?""",
                [since_id, limit]
            )
        else:
            # Regular sync: only get NEW records with ID > since_id
            results = engine.read(
                f"SELECT * FROM {table} WHERE id > ? ORDER BY id ASC LIMIT ?",
                [since_id, limit]
            )
        
        serialized = [_serialize_row(row) for row in results]
        
        # Get the max ID from results for next sync
        max_id = max((r.get('id', 0) for r in serialized), default=since_id)
        
        return {
            "success": True,
            "table": table,
            "count": len(serialized),
            "records": serialized,
            "since_id": since_id,
            "max_id": max_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync query failed: {e}")


@app.get("/price/{token}")
async def get_current_price(token: str = "SOL"):
    """
    Get the current price of a token.
    
    Args:
        token: Token symbol (default: SOL)
    
    Returns:
        - price: current price or null
        - token: token symbol
        - timestamp: time of price
    """
    engine = _get_engine()
    
    try:
        price = engine.get_price(token)
        
        return {
            "success": True,
            "token": token,
            "price": price,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get price: {e}")


# =============================================================================
# Standalone Runner
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    print("=" * 60)
    print("Starting Data Engine API")
    print("=" * 60)
    print("Endpoints:")
    print("  POST /insert       - Queue write to DuckDB")
    print("  POST /insert/batch - Queue batch writes")
    print("  POST /insert/sync  - Synchronous insert with ID")
    print("  POST /query        - Execute SELECT query")
    print("  GET  /backfill/{table} - Get historical data")
    print("  GET  /health       - Health check")
    print("  GET  /tables       - List tables")
    print("  GET  /latest/{table} - Get latest records")
    print("  GET  /price/{token} - Get current price")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=5050)

"""
Pydantic models for FastAPI webhook payloads.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TradePayload(BaseModel):
    id: Optional[int] = Field(None, description="Trade ID (optional, auto-generated if missing)")
    signature: Optional[str] = None
    wallet_address: str
    direction: Optional[str] = None
    sol_amount: Optional[float] = None
    stablecoin_amount: Optional[float] = None
    price: Optional[float] = None
    perp_direction: Optional[str] = None
    trade_timestamp: Optional[datetime] = None


class WhalePayload(BaseModel):
    id: Optional[int] = Field(None, description="Whale movement ID (optional, auto-generated if missing)")
    signature: Optional[str] = None
    wallet_address: str
    whale_type: Optional[str] = None
    current_balance: Optional[float] = None
    sol_change: Optional[float] = None
    abs_change: Optional[float] = None
    percentage_moved: Optional[float] = None
    direction: Optional[str] = None
    action: Optional[str] = None
    movement_significance: Optional[str] = None
    previous_balance: Optional[float] = None
    fee_paid: Optional[float] = None
    block_time: Optional[int] = None
    timestamp: Optional[datetime] = None
    received_at: Optional[datetime] = None
    slot: Optional[int] = None
    has_perp_position: Optional[bool] = None
    perp_platform: Optional[str] = None
    perp_direction: Optional[str] = None
    perp_size: Optional[float] = None
    perp_leverage: Optional[float] = None
    perp_entry_price: Optional[float] = None
    raw_data_json: Optional[str] = None

from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class MarketPayload(BaseModel):
    # Simplified for the SDK prototype
    price: float
    size: float
    side: str

class MarketEvent(BaseModel):
    exchange_time: datetime
    symbol: str
    payload: MarketPayload

class BarPayload(BaseModel):
    open: float
    high: float
    low: float
    close: float
    volume: float

class OrderStatus(BaseModel):
    order_id: UUID
    status: str
    filled_quantity: float
    avg_price: float

class KuberaStrategy(ABC):
    def __init__(self, name: str):
        self.name = name
        self.positions = {}

    @abstractmethod
    def on_tick(self, event: MarketEvent):
        pass

    @abstractmethod
    def on_bar(self, symbol: str, bar: BarPayload):
        pass

    def on_fill(self, status: OrderStatus):
        print(f"[{self.name}] Order {status.order_id} filled: {status.status} @ {status.avg_price}")

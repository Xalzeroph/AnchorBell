"""
Binance market data compatibility layer.
Only responsible for collecting reference data.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class BinanceSnapshot:
    symbol: str
    timestamp: datetime
    index_price: Decimal
    mark_price: Decimal
    bid_price: Decimal
    ask_price: Decimal

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal("2")

    @property
    def deviation(self) -> Decimal:
        return (self.mid_price - self.index_price) / self.index_price


class BinanceMarketClient:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def get_snapshot(self) -> BinanceSnapshot:
        raise NotImplementedError("WebSocket collector will be implemented next")

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    timestamp: datetime
    index_price: float
    mark_price: float
    bid_price: float
    ask_price: float

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2

    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price

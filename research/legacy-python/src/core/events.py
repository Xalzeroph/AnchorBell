"""Low latency internal event contracts.

Hot path objects. Independent from exchange SDKs.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class MarketTick:
    symbol: str
    timestamp_ns: int
    bid: float
    ask: float
    last: float
    index_price: float
    mark_price: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True)
class SignalIntent:
    symbol: str
    side: str
    price: float
    quantity: float
    reason: str

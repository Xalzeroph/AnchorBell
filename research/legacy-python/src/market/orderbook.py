"""In-memory order book state for hot path calculations."""
from dataclasses import dataclass, field


@dataclass
class OrderBookState:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def mid_price(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def imbalance(self, levels: int = 5) -> float:
        bid = sum(v for _, v in sorted(self.bids.items(), reverse=True)[:levels])
        ask = sum(v for _, v in sorted(self.asks.items())[:levels])
        total = bid + ask
        return 0.0 if total == 0 else (bid - ask) / total

"""Exchange websocket adapter boundary.

This module only translates exchange messages into internal events.
It must not contain strategy or execution logic.
"""

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class DepthUpdate:
    symbol: str
    timestamp_ns: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


class BinanceWebSocketAdapter:
    """Binance transport adapter.

    Real websocket implementation is intentionally isolated from
    market models and strategy code.
    """

    def __init__(self, on_depth: Callable[[DepthUpdate], None]):
        self._on_depth = on_depth

    async def connect(self, symbol: str) -> None:
        """Connect to Binance streams.

        Implementation will bind Binance depth/mark streams here.
        """
        raise NotImplementedError

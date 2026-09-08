"""Strategy to execution contract."""

from dataclasses import dataclass
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: Side
    price: float
    quantity: float
    post_only: bool = True


class OrderManager:
    """Execution boundary.

    Receives strategy intents and handles exchange-specific details.
    """

    async def submit(self, intent: OrderIntent) -> None:
        raise NotImplementedError

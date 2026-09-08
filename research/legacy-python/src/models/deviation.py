"""Static anchor deviation model for analytics."""

from decimal import Decimal


def calculate_deviation(mid_price: Decimal, index_price: Decimal) -> Decimal:
    """Return perp deviation from Binance index anchor."""
    if index_price <= 0:
        raise ValueError("index_price must be positive")
    return (mid_price - index_price) / index_price


def classify_signal(deviation: Decimal, threshold: Decimal = Decimal("0.008")) -> str:
    """Analytics signal, not execution logic."""
    if deviation <= -threshold:
        return "LONG_ANCHOR"
    if deviation >= threshold:
        return "SHORT_ANCHOR"
    return "NO_SIGNAL"

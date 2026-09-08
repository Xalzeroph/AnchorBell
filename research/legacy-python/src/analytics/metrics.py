from src.data.schema import MarketSnapshot

# Archival analytics only; this package is outside the Rust runtime.


def deviation(snapshot: MarketSnapshot) -> float:
    """Distance between traded market mid and Binance index anchor."""
    return (snapshot.mid_price - snapshot.index_price) / snapshot.index_price


def anchor_gap(snapshot: MarketSnapshot) -> float:
    """Absolute price gap from anchor."""
    return snapshot.mid_price - snapshot.index_price


def spread_ratio(snapshot: MarketSnapshot) -> float:
    """Relative bid ask spread."""
    return snapshot.spread / snapshot.mid_price

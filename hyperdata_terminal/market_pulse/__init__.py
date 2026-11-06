"""Market Pulse: durable BTC/XRP trade capture, windows and read-only facts.

The package is deliberately independent from the live screener.  It defines a
small, versioned data contract, writes normalized executions to PostgreSQL and
publishes closed minute windows through a read-only HTTP API.  Legacy screener
output can only consume that API; it never reads collector sockets directly.
"""
from __future__ import annotations

from .contracts import (
    SCHEMA_VERSION,
    WINDOW_SCHEMA_VERSION,
    AggregationKind,
    ContractError,
    DeliveryKind,
    ExecutionEvent,
    Gap,
    InstrumentSpec,
    QualityState,
    Side,
    Venue,
    WindowAggregate,
)

__all__ = [
    "SCHEMA_VERSION",
    "WINDOW_SCHEMA_VERSION",
    "AggregationKind",
    "ContractError",
    "DeliveryKind",
    "ExecutionEvent",
    "Gap",
    "InstrumentSpec",
    "QualityState",
    "Side",
    "Venue",
    "WindowAggregate",
]

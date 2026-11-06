"""Versioned contracts for Market Pulse.

Money and quantities are ``Decimal`` end to end.  An execution carries its
instrument-spec version so a replay uses the contract size that was valid when
the trade happened, not today's exchange metadata.

The canonical channel map is intentionally narrow.  OKX ``trades`` (aggregated)
and ``trades-all`` are not interchangeable: the pilot saw one aggregated record
represent four executions.  A canonical stream therefore has exactly one
aggregation kind, and the deduplication key includes it.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Mapping

SCHEMA_VERSION = "market-pulse.event.v1"
WINDOW_SCHEMA_VERSION = "market-pulse.window.v1"

WINDOW_MS = 60_000
DEFAULT_LATE_GRACE_MS = 5_000


class ContractError(ValueError):
    """An event or instrument violates the public contract."""


class Venue(StrEnum):
    HYPERLIQUID = "hyperliquid"
    OKX = "okx"


class MarketType(StrEnum):
    PERP = "perp"
    SPOT = "spot"


class ContractType(StrEnum):
    LINEAR = "linear"
    INVERSE = "inverse"
    UNKNOWN = "unknown"


class AggregationKind(StrEnum):
    """What one stored record represents.

    ``execution`` is a single exchange execution.  ``aggregated`` is an
    exchange-side aggregate that may stand for several executions and must
    never be counted as one execution or merged with an execution stream.
    """

    EXECUTION = "execution"
    AGGREGATED = "aggregated"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class DeliveryKind(StrEnum):
    LIVE = "live"
    RECOVERY = "recovery"


class QualityState(StrEnum):
    WARMING_UP = "warming_up"
    OBSERVED_NO_DETECTED_GAP = "observed_no_detected_gap"
    GAP_DETECTED = "gap_detected"
    RECOVERING = "recovering"
    RECONCILED = "reconciled"
    DECLARED_OUTAGE = "declared_outage"
    UNAVAILABLE = "unavailable"


class NotionalStatus(StrEnum):
    OK = "ok"
    BLOCKED_UNKNOWN_UNIT = "blocked_unknown_unit"
    BLOCKED_MISSING_SPEC = "blocked_missing_spec"


# (venue, channel) -> aggregation kind.  These are the only V1 streams allowed
# to feed analytics.  Other channels may be replayed for reconciliation, but
# their records stay in a separate aggregation-kind namespace.
CANONICAL_CHANNELS: dict[tuple[str, str], AggregationKind] = {
    (Venue.HYPERLIQUID, "trades"): AggregationKind.EXECUTION,
    (Venue.OKX, "trades-all"): AggregationKind.EXECUTION,
}

# Channels accepted for recovery/validation, with their semantics.
KNOWN_CHANNELS: dict[tuple[str, str], AggregationKind] = {
    **CANONICAL_CHANNELS,
    (Venue.OKX, "trades"): AggregationKind.AGGREGATED,
}


def canonical_channel(venue: str, channel: str) -> AggregationKind:
    try:
        return CANONICAL_CHANNELS[(Venue(venue), channel)]
    except (ValueError, KeyError) as exc:
        raise ContractError(f"{venue}/{channel} is not a canonical V1 channel") from exc


def known_channel(venue: str, channel: str) -> AggregationKind:
    try:
        return KNOWN_CHANNELS[(Venue(venue), channel)]
    except (ValueError, KeyError) as exc:
        raise ContractError(f"unknown channel {venue}/{channel}") from exc


def _require_text(value: Any, field: str, *, max_length: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    value = value.strip()
    if len(value) > max_length:
        raise ContractError(f"{field} is longer than {max_length} characters")
    return value


def _require_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{field} must be an integer with an explicit unit")
    if value < minimum:
        raise ContractError(f"{field} must be >= {minimum}")
    return value


def _decimal(value: Any, field: str, *, positive: bool = False, optional: bool = False) -> Decimal | None:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ContractError(f"{field} must be a decimal, not bool")
    if isinstance(value, float):
        raise ContractError(f"{field} must not be a float; pass a string or Decimal")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ContractError(f"{field} is not a decimal") from exc
    if not result.is_finite():
        raise ContractError(f"{field} must be finite")
    if positive and result <= 0:
        raise ContractError(f"{field} must be > 0")
    if not positive and result < 0:
        raise ContractError(f"{field} must be >= 0")
    return result


def normalize_epoch_ms(value: Any, unit: str) -> int:
    """Convert an exchange timestamp with an explicit unit to UTC milliseconds."""
    if unit not in {"ms", "s"}:
        raise ContractError("timestamp unit must be explicitly 'ms' or 's'")
    if isinstance(value, bool):
        raise ContractError("timestamp must not be bool")
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise ContractError("timestamp is not an integer") from exc
    if integer < 0:
        raise ContractError("timestamp must be >= 0")
    return integer if unit == "ms" else integer * 1_000


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """A point-in-time specification of one tradable instrument."""

    venue: str
    native_market_id: str
    base_asset: str
    market_type: str
    quote_currency: str
    settlement_currency: str
    contract_type: str
    native_amount_unit: str
    contract_size: Decimal | None
    contract_multiplier: Decimal | None
    spec_version: str
    valid_from_ms: int
    source: str
    valid_to_ms: int | None = None
    tick_size: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", Venue(self.venue).value)
        self._validate()

    def _validate(self) -> None:
        _require_text(self.native_market_id, "native_market_id")
        _require_text(self.base_asset, "base_asset", max_length=32)
        _require_text(self.quote_currency, "quote_currency", max_length=16)
        _require_text(self.settlement_currency, "settlement_currency", max_length=16)
        _require_text(self.source, "source")
        _require_text(self.spec_version, "spec_version", max_length=128)
        _require_int(self.valid_from_ms, "valid_from_ms")
        if self.valid_to_ms is not None:
            _require_int(self.valid_to_ms, "valid_to_ms")
            if self.valid_to_ms <= self.valid_from_ms:
                raise ContractError("valid_to_ms must be after valid_from_ms")
        if self.market_type not in {item.value for item in MarketType}:
            raise ContractError(f"unknown market_type {self.market_type!r}")
        if self.contract_type not in {item.value for item in ContractType}:
            raise ContractError(f"unknown contract_type {self.contract_type!r}")
        if self.native_amount_unit not in {"base", "contract", "unknown"}:
            raise ContractError("native_amount_unit must be 'base', 'contract' or 'unknown'")
        if self.contract_size is not None:
            _decimal(self.contract_size, "contract_size", positive=True)
        if self.contract_multiplier is not None:
            _decimal(self.contract_multiplier, "contract_multiplier", positive=True)
        if self.tick_size is not None:
            _decimal(self.tick_size, "tick_size", positive=True)
        if self.market_type == MarketType.PERP and self.contract_type == ContractType.INVERSE:
            # V1 does not mix inverse and linear contracts in one series.
            return

    @property
    def is_v1_eligible(self) -> bool:
        """True only for an explicitly verified linear perpetual."""
        return (
            self.market_type == MarketType.PERP.value
            and self.contract_type == ContractType.LINEAR.value
            and self.native_amount_unit in {"base", "contract"}
            and self.contract_size is not None
            and self.contract_size > 0
        )

    @property
    def notional_status(self) -> NotionalStatus:
        if self.native_amount_unit == "unknown" or self.contract_size is None:
            return NotionalStatus.BLOCKED_UNKNOWN_UNIT
        return NotionalStatus.OK

    def base_quantity(self, native_amount: Decimal | int | str) -> Decimal | None:
        """Convert the exchange amount to base-asset units.

        ``None`` means the unit is not known.  The caller must store the event
        and block notional rather than silently guessing a contract size.
        """
        amount = _decimal(native_amount, "native_amount", positive=True)
        assert amount is not None
        if self.native_amount_unit == "base":
            return amount
        if self.native_amount_unit == "contract" and self.contract_size is not None:
            multiplier = self.contract_multiplier or Decimal(1)
            return amount * self.contract_size * multiplier
        return None

    def quote_amount(self, price: Decimal | int | str, native_amount: Decimal | int | str) -> Decimal | None:
        base = self.base_quantity(native_amount)
        if base is None:
            return None
        return base * _decimal(price, "price", positive=True)  # type: ignore[operator]

    def to_payload(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "base_asset": self.base_asset,
            "market_type": self.market_type,
            "quote_currency": self.quote_currency,
            "settlement_currency": self.settlement_currency,
            "contract_type": self.contract_type,
            "native_amount_unit": self.native_amount_unit,
            "contract_size": _json_decimal(self.contract_size),
            "contract_multiplier": _json_decimal(self.contract_multiplier),
            "spec_version": self.spec_version,
            "valid_from_ms": self.valid_from_ms,
            "valid_to_ms": self.valid_to_ms,
            "tick_size": _json_decimal(self.tick_size),
            "source": self.source,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "InstrumentSpec":
        return cls(
            venue=payload["venue"],
            native_market_id=payload["native_market_id"],
            base_asset=payload["base_asset"],
            market_type=payload["market_type"],
            quote_currency=payload["quote_currency"],
            settlement_currency=payload["settlement_currency"],
            contract_type=payload["contract_type"],
            native_amount_unit=payload["native_amount_unit"],
            contract_size=_optional_decimal(payload.get("contract_size")),
            contract_multiplier=_optional_decimal(payload.get("contract_multiplier")),
            spec_version=payload["spec_version"],
            valid_from_ms=int(payload["valid_from_ms"]),
            valid_to_ms=None if payload.get("valid_to_ms") is None else int(payload["valid_to_ms"]),
            tick_size=_optional_decimal(payload.get("tick_size")),
            source=payload["source"],
        )


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """One normalized trade record, ready for durable ingestion."""

    venue: str
    native_market_id: str
    channel: str
    aggregation_kind: str
    trade_id: str
    event_time_ms: int
    receive_time_ms: int
    source_epoch: str
    side: str
    price: Decimal
    native_amount: Decimal
    base_amount: Decimal | None
    quote_amount: Decimal | None
    instrument_spec_version: str
    raw_ref: str | None = None
    execution_count: int = 1
    delivery_kind: str = DeliveryKind.LIVE.value
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", Venue(self.venue).value)
        object.__setattr__(self, "side", Side(self.side).value)
        object.__setattr__(self, "aggregation_kind", AggregationKind(self.aggregation_kind).value)
        object.__setattr__(self, "delivery_kind", DeliveryKind(self.delivery_kind).value)
        if self.schema_version != SCHEMA_VERSION:
            raise ContractError(
                f"unsupported schema_version {self.schema_version!r}; expected {SCHEMA_VERSION!r}"
            )
        _require_text(self.native_market_id, "native_market_id")
        _require_text(self.channel, "channel")
        _require_text(self.trade_id, "trade_id")
        _require_text(self.source_epoch, "source_epoch")
        _require_text(self.instrument_spec_version, "instrument_spec_version")
        _require_int(self.event_time_ms, "event_time_ms")
        _require_int(self.receive_time_ms, "receive_time_ms")
        _require_int(self.execution_count, "execution_count", minimum=1)
        object.__setattr__(self, "price", _decimal(self.price, "price", positive=True))
        object.__setattr__(self, "native_amount", _decimal(self.native_amount, "native_amount", positive=True))
        object.__setattr__(self, "base_amount", _decimal(self.base_amount, "base_amount", optional=True))
        object.__setattr__(self, "quote_amount", _decimal(self.quote_amount, "quote_amount", optional=True))
        if (self.base_amount is None) != (self.quote_amount is None):
            raise ContractError("base_amount and quote_amount must be known together or both absent")
        expected = known_channel(self.venue, self.channel)
        if expected != self.aggregation_kind:
            raise ContractError(
                f"channel {self.venue}/{self.channel} has aggregation_kind {expected.value!r}, "
                f"not {self.aggregation_kind!r}"
            )

    @property
    def dedup_key(self) -> tuple[str, str, str, str, str]:
        return (
            self.venue,
            self.native_market_id,
            self.channel,
            self.aggregation_kind,
            self.trade_id,
        )

    @property
    def window_start_ms(self) -> int:
        return self.event_time_ms // WINDOW_MS * WINDOW_MS

    @property
    def notional_status(self) -> NotionalStatus:
        if self.base_amount is None:
            return NotionalStatus.BLOCKED_UNKNOWN_UNIT
        return NotionalStatus.OK

    def content_fingerprint(self) -> str:
        """Stable hash of the fields that must agree for a duplicate.

        Receive time, source epoch, raw location and delivery kind intentionally
        do not participate: the same execution may arrive over a reconnect and
        from recovery with different envelope metadata.
        """
        payload = {
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "aggregation_kind": self.aggregation_kind,
            "trade_id": self.trade_id,
            "event_time_ms": self.event_time_ms,
            "side": self.side,
            "price": _json_decimal(self.price),
            "native_amount": _json_decimal(self.native_amount),
            "base_amount": _json_decimal(self.base_amount),
            "quote_amount": _json_decimal(self.quote_amount),
            "instrument_spec_version": self.instrument_spec_version,
            "execution_count": self.execution_count,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "aggregation_kind": self.aggregation_kind,
            "trade_id": self.trade_id,
            "event_time_ms": self.event_time_ms,
            "receive_time_ms": self.receive_time_ms,
            "source_epoch": self.source_epoch,
            "side": self.side,
            "price": _json_decimal(self.price),
            "native_amount": _json_decimal(self.native_amount),
            "base_amount": _json_decimal(self.base_amount),
            "quote_amount": _json_decimal(self.quote_amount),
            "instrument_spec_version": self.instrument_spec_version,
            "raw_ref": self.raw_ref,
            "execution_count": self.execution_count,
            "delivery_kind": self.delivery_kind,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ExecutionEvent":
        return cls(
            schema_version=payload.get("schema_version", SCHEMA_VERSION),
            venue=payload["venue"],
            native_market_id=payload["native_market_id"],
            channel=payload["channel"],
            aggregation_kind=payload["aggregation_kind"],
            trade_id=payload["trade_id"],
            event_time_ms=int(payload["event_time_ms"]),
            receive_time_ms=int(payload["receive_time_ms"]),
            source_epoch=payload["source_epoch"],
            side=payload["side"],
            price=_decimal(payload["price"], "price", positive=True),  # type: ignore[arg-type]
            native_amount=_decimal(payload["native_amount"], "native_amount", positive=True),  # type: ignore[arg-type]
            base_amount=_optional_decimal(payload.get("base_amount")),
            quote_amount=_optional_decimal(payload.get("quote_amount")),
            instrument_spec_version=payload["instrument_spec_version"],
            raw_ref=payload.get("raw_ref"),
            execution_count=int(payload.get("execution_count", 1)),
            delivery_kind=payload.get("delivery_kind", DeliveryKind.LIVE.value),
        )


@dataclass(frozen=True, slots=True)
class Gap:
    """A known or suspected missing interval for one channel and source epoch."""

    gap_id: int
    venue: str
    native_market_id: str
    channel: str
    source_epoch: str
    start_ms: int
    detected_at_ms: int
    state: str
    end_ms: int | None = None
    method: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        QualityState(self.state)
        if self.end_ms is not None and self.end_ms < self.start_ms:
            raise ContractError("gap end_ms must not be before start_ms")

    def overlaps(self, start_ms: int, end_ms: int) -> bool:
        return self.start_ms <= end_ms and (self.end_ms is None or self.end_ms >= start_ms)

    def to_payload(self) -> dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "source_epoch": self.source_epoch,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "detected_at_ms": self.detected_at_ms,
            "state": self.state,
            "method": self.method,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class WindowAggregate:
    """A reproducible UTC-minute aggregate and its quality provenance."""

    venue: str
    native_market_id: str
    channel: str
    aggregation_kind: str
    window_start_ms: int
    window_end_ms: int
    version: int
    quality: str
    event_count: int
    execution_count: int
    buy_base: Decimal | None
    sell_base: Decimal | None
    buy_quote: Decimal | None
    sell_quote: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    first_event_ms: int | None
    last_event_ms: int | None
    provisional: bool
    revised_at_ms: int | None = None
    baseline_version: str | None = None
    coverage: dict[str, Any] | None = None
    schema_version: str = WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        QualityState(self.quality)
        _require_int(self.window_start_ms, "window_start_ms")
        _require_int(self.window_end_ms, "window_end_ms")
        if self.window_end_ms != self.window_start_ms + WINDOW_MS:
            raise ContractError("V1 windows must be exactly 60_000 ms")
        _require_int(self.version, "version", minimum=1)
        _require_int(self.event_count, "event_count")
        _require_int(self.execution_count, "execution_count")

    @property
    def aggregate_key(self) -> tuple[str, str, str, str, int]:
        return (
            self.venue,
            self.native_market_id,
            self.channel,
            self.aggregation_kind,
            self.window_start_ms,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "aggregation_kind": self.aggregation_kind,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "version": self.version,
            "quality": self.quality,
            "event_count": self.event_count,
            "execution_count": self.execution_count,
            "buy_base": _json_decimal(self.buy_base),
            "sell_base": _json_decimal(self.sell_base),
            "buy_quote": _json_decimal(self.buy_quote),
            "sell_quote": _json_decimal(self.sell_quote),
            "open": _json_decimal(self.open),
            "high": _json_decimal(self.high),
            "low": _json_decimal(self.low),
            "close": _json_decimal(self.close),
            "first_event_ms": self.first_event_ms,
            "last_event_ms": self.last_event_ms,
            "provisional": self.provisional,
            "revised_at_ms": self.revised_at_ms,
            "baseline_version": self.baseline_version,
            "coverage": self.coverage or {},
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "WindowAggregate":
        return cls(
            schema_version=payload.get("schema_version", WINDOW_SCHEMA_VERSION),
            venue=payload["venue"],
            native_market_id=payload["native_market_id"],
            channel=payload["channel"],
            aggregation_kind=payload["aggregation_kind"],
            window_start_ms=int(payload["window_start_ms"]),
            window_end_ms=int(payload["window_end_ms"]),
            version=int(payload["version"]),
            quality=payload["quality"],
            event_count=int(payload["event_count"]),
            execution_count=int(payload["execution_count"]),
            buy_base=_optional_decimal(payload.get("buy_base")),
            sell_base=_optional_decimal(payload.get("sell_base")),
            buy_quote=_optional_decimal(payload.get("buy_quote")),
            sell_quote=_optional_decimal(payload.get("sell_quote")),
            open=_optional_decimal(payload.get("open")),
            high=_optional_decimal(payload.get("high")),
            low=_optional_decimal(payload.get("low")),
            close=_optional_decimal(payload.get("close")),
            first_event_ms=_optional_int(payload.get("first_event_ms")),
            last_event_ms=_optional_int(payload.get("last_event_ms")),
            provisional=bool(payload["provisional"]),
            revised_at_ms=_optional_int(payload.get("revised_at_ms")),
            baseline_version=payload.get("baseline_version"),
            coverage=payload.get("coverage") or {},
        )


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value, "decimal", optional=True)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _require_int(int(value), "integer")


def _json_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def affected_window(event: ExecutionEvent) -> tuple[str, str, str, str, int]:
    return (
        event.venue,
        event.native_market_id,
        event.channel,
        event.aggregation_kind,
        event.window_start_ms,
    )

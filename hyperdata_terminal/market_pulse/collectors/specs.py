"""Verified V1 instrument catalogue for BTC and XRP.

Contract sizes are inputs, not guesses.  OKX values come from the public
``/api/v5/public/instruments`` response; the collector can refresh them through
``OkxInstrumentCatalog`` before subscribing.  Hyperliquid linear perpetuals are
denominated in base units, so their contract size is exactly one base unit.
"""
from __future__ import annotations

from decimal import Decimal

from ..contracts import ContractType, InstrumentSpec, MarketType

V1_MARKETS = ("BTC", "XRP")

OKX_MARKET_IDS = {
    "BTC": "BTC-USDT-SWAP",
    "XRP": "XRP-USDT-SWAP",
}
# Public OKX contract values for the two V1 instruments.  The catalog fetch
# below is preferred and creates a new spec version when these change.
OKX_DEFAULT_CONTRACT_SIZE = {
    "BTC": Decimal("0.01"),
    "XRP": Decimal("100"),
}


def hyperliquid_spec(base_asset: str, *, valid_from_ms: int = 0) -> InstrumentSpec:
    return InstrumentSpec(
        venue="hyperliquid",
        native_market_id=base_asset,
        base_asset=base_asset,
        market_type=MarketType.PERP.value,
        quote_currency="USDC",
        settlement_currency="USDC",
        contract_type=ContractType.LINEAR.value,
        native_amount_unit="base",
        contract_size=Decimal(1),
        contract_multiplier=Decimal(1),
        spec_version=f"hyperliquid-{base_asset.lower()}-perp-base-v1",
        valid_from_ms=valid_from_ms,
        source="hyperliquid linear perpetual / base units",
    )


def okx_spec(
    base_asset: str,
    contract_size: Decimal,
    *,
    valid_from_ms: int = 0,
    contract_multiplier: Decimal = Decimal(1),
    source: str = "okx public instruments",
) -> InstrumentSpec:
    market_id = market_for_venue("okx", base_asset)
    value = format(contract_size.normalize(), "f")
    multiplier = format(contract_multiplier.normalize(), "f")
    return InstrumentSpec(
        venue="okx",
        native_market_id=market_id,
        base_asset=base_asset,
        market_type=MarketType.PERP.value,
        quote_currency="USDT",
        settlement_currency="USDT",
        contract_type=ContractType.LINEAR.value,
        native_amount_unit="contract",
        contract_size=contract_size,
        contract_multiplier=contract_multiplier,
        spec_version=f"okx-{base_asset.lower()}-usdt-swap-ctval-{value}-mult-{multiplier}-v1",
        valid_from_ms=valid_from_ms,
        source=source,
    )


def default_instruments(markets: tuple[str, ...] = V1_MARKETS) -> list[InstrumentSpec]:
    result: list[InstrumentSpec] = []
    for market in markets:
        base = market.upper()
        result.append(hyperliquid_spec(base))
        result.append(okx_spec(base, OKX_DEFAULT_CONTRACT_SIZE[base]))
    return result


def spec_index(specs: list[InstrumentSpec]) -> dict[tuple[str, str], InstrumentSpec]:
    """Latest valid spec per venue/market pair."""
    index: dict[tuple[str, str], InstrumentSpec] = {}
    for spec in specs:
        key = (spec.venue, spec.native_market_id)
        current = index.get(key)
        if current is None or (spec.valid_from_ms, spec.spec_version) > (
            current.valid_from_ms,
            current.spec_version,
        ):
            index[key] = spec
    return index


def market_for_venue(venue: str, base_asset: str) -> str:
    from hyperdata_terminal.instruments import identity, is_binance

    if is_binance(base_asset):
        if venue != "binance":
            raise ValueError("a Binance observation is not another venue's contract")
        return identity(base_asset)[1]
    if venue == "binance":
        raise ValueError("Binance observations require explicit venue identity")
    if venue == "okx":
        return OKX_MARKET_IDS.get(base_asset, base_asset.upper() + "-USDT-SWAP")
    return base_asset

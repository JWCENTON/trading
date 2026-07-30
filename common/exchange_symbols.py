# common/exchange_symbols.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass


_CANONICAL_ASSET_RE = re.compile(r"^[A-Z0-9]{2,20}$")


@dataclass(frozen=True)
class CanonicalInstrumentResolution:
    status: str
    source: str
    symbol: str | None
    base_asset: str | None
    quote_asset: str | None


def exchange_name() -> str:
    return os.environ.get("EXCHANGE", "BINANCE").strip().upper()


def quote_asset() -> str:
    return os.environ.get("QUOTE_ASSET", "USDC").strip().upper()


def resolve_canonical_instrument(
    symbol: str | None,
) -> CanonicalInstrumentResolution:
    """Resolve the internal spot symbol against the configured quote contract."""
    normalized = str(symbol).strip().upper() if symbol else ""
    quote = quote_asset()
    if (
        not normalized
        or not quote
        or not _CANONICAL_ASSET_RE.fullmatch(normalized)
        or not _CANONICAL_ASSET_RE.fullmatch(quote)
        or not normalized.endswith(quote)
    ):
        return CanonicalInstrumentResolution(
            "UNKNOWN", "UNKNOWN", normalized or None, None, None
        )
    base = normalized[: -len(quote)]
    if not base or not _CANONICAL_ASSET_RE.fullmatch(base):
        return CanonicalInstrumentResolution(
            "UNKNOWN", "UNKNOWN", normalized, None, None
        )
    return CanonicalInstrumentResolution(
        "RESOLVED", "CANONICAL_SYMBOL_RESOLUTION",
        normalized, base, quote,
    )


def to_okx_inst_id(symbol: str) -> str:
    """
    Internal: BTCUSDC
    OKX spot: BTC-USDC
    """
    s = str(symbol).strip().upper()
    q = quote_asset()
    if not s.endswith(q):
        raise ValueError(f"symbol={s} does not end with QUOTE_ASSET={q}")
    base = s[: -len(q)]
    return f"{base}-{q}"


def from_okx_inst_id(inst_id: str) -> str:
    return str(inst_id).strip().upper().replace("-", "")


def to_exchange_symbol(symbol: str, exchange: str | None = None) -> str:
    ex = (exchange or exchange_name()).strip().upper()
    if ex == "OKX":
        return to_okx_inst_id(symbol)
    return str(symbol).strip().upper()


def from_exchange_symbol(symbol: str, exchange: str | None = None) -> str:
    ex = (exchange or exchange_name()).strip().upper()
    if ex == "OKX":
        return from_okx_inst_id(symbol)
    return str(symbol).strip().upper()

# common/exchange_symbols.py
from __future__ import annotations

import os


def exchange_name() -> str:
    return os.environ.get("EXCHANGE", "BINANCE").strip().upper()


def quote_asset() -> str:
    return os.environ.get("QUOTE_ASSET", "USDC").strip().upper()


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

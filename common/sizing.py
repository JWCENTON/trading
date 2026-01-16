# common/sizing.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Tuple
import time


@dataclass(frozen=True)
class SymbolFilters:
    step: float
    min_qty: float
    min_notional: float
    ts: float  # cache timestamp


# in-process cache: (symbol) -> SymbolFilters
_FILTERS_CACHE: dict[str, SymbolFilters] = {}


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def floor_to_step(qty: float, step: float) -> float:
    """
    Floor qty to the nearest valid increment of step.
    If step <= 0, returns qty unchanged.
    """
    if step is None or step <= 0:
        return float(qty)

    q = Decimal(str(qty))
    s = Decimal(str(step))
    floored = (q / s).to_integral_value(rounding=ROUND_DOWN) * s
    return float(floored)


def get_symbol_filters(
    client,
    symbol: str,
    *,
    cache_ttl_seconds: int = 3600,
) -> SymbolFilters:
    """
    Fetch symbol filters from Binance and cache them in-process.

    Requires client.get_symbol_info(symbol) compatible with python-binance.
    """
    now = time.time()
    cached = _FILTERS_CACHE.get(symbol)
    if cached and (now - cached.ts) < cache_ttl_seconds:
        return cached

    info = client.get_symbol_info(symbol)
    if not info:
        raise RuntimeError(f"Cannot fetch symbol_info for {symbol}")

    lot = next((f for f in info.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
    # Binance can expose min notional under MIN_NOTIONAL (spot) or NOTIONAL in some cases
    mn = next((f for f in info.get("filters", []) if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL")), None)

    step = _to_float(lot.get("stepSize") if lot else 0.0, 0.0)
    min_qty = _to_float(lot.get("minQty") if lot else 0.0, 0.0)

    # MIN_NOTIONAL usually uses key "minNotional"
    min_notional = _to_float(mn.get("minNotional") if mn else 0.0, 0.0)

    sf = SymbolFilters(step=step, min_qty=min_qty, min_notional=min_notional, ts=now)
    _FILTERS_CACHE[symbol] = sf
    return sf


def compute_qty_from_notional(
    client,
    *,
    symbol: str,
    px: float,
    target_notional: float,
    min_notional_buffer_pct: float = 0.05,
    cache_ttl_seconds: int = 3600,
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (qty, info) where qty is floored to LOT_SIZE step and respects minQty.
    info is meant to be stored in strategy_events SIZING event.

    Behavior:
    - effective_min_notional = min_notional * (1 + buffer_pct)
    - notional_used = max(target_notional, effective_min_notional)
    - qty_raw = notional_used / px
    - qty = floor(qty_raw, step)
    - if qty < min_qty -> qty = min_qty (then floor again just in case)
    """
    if px is None or float(px) <= 0:
        return 0.0, {
            "px": _to_float(px),
            "qty": 0.0,
            "qty_raw": 0.0,
            "step": 0.0,
            "min_qty": 0.0,
            "notional": 0.0,
            "min_notional": 0.0,
            "target_notional": _to_float(target_notional),
            "min_notional_eff": 0.0,
            "min_notional_buffer_pct": _to_float(min_notional_buffer_pct),
            "error": "bad_px",
        }

    f = get_symbol_filters(client, symbol, cache_ttl_seconds=cache_ttl_seconds)
    step = float(f.step)
    min_qty = float(f.min_qty)
    min_notional = float(f.min_notional)

    buffer_pct = float(min_notional_buffer_pct)
    min_notional_eff = min_notional * (1.0 + buffer_pct) if min_notional > 0 else 0.0
    notional_used = max(float(target_notional), float(min_notional_eff), 0.0)

    qty_raw = notional_used / float(px)
    qty = floor_to_step(qty_raw, step)

    if min_qty > 0 and qty < min_qty:
        qty = min_qty
        qty = floor_to_step(qty, step)

    info = {
        "px": float(px),
        "qty": float(qty),
        "qty_raw": float(qty_raw),
        "step": float(step),
        "min_qty": float(min_qty),
        "notional": float(qty) * float(px),
        "min_notional": float(min_notional),
        "target_notional": float(target_notional),
        "min_notional_eff": float(min_notional_eff),
        "min_notional_buffer_pct": float(buffer_pct),
    }
    return float(qty), info
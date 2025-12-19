# common/execution.py
import os
import uuid
import logging
from decimal import Decimal, ROUND_DOWN

TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()
LIVE_ORDERS_ENABLED = os.environ.get("LIVE_ORDERS_ENABLED", "0") == "1"

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()
LIVE_MAX_NOTIONAL = float(os.environ.get("LIVE_MAX_NOTIONAL", "0"))  # 0 = no limit

PANIC_DISABLE_TRADING = os.environ.get("PANIC_DISABLE_TRADING", "0") == "1"

def _floor_to_step(qty: float, step: str) -> float:
    q = Decimal(str(qty))
    s = Decimal(str(step))
    if s == 0:
        return float(q)
    return float((q / s).to_integral_value(rounding=ROUND_DOWN) * s)

def _get_symbol_filters(client, symbol: str):
    info = client.get_symbol_info(symbol)
    if not info:
        raise RuntimeError(f"Symbol info not found for {symbol}")
    lot = next((f for f in info["filters"] if f["filterType"] == "LOT_SIZE"), None)
    min_notional = next((f for f in info["filters"] if f["filterType"] in ("MIN_NOTIONAL","NOTIONAL")), None)

    step_size = lot["stepSize"] if lot else "0.00000001"
    min_qty = float(lot["minQty"]) if lot else 0.0
    min_notional_val = float(min_notional.get("minNotional", 0.0)) if min_notional else 0.0
    return step_size, min_qty, min_notional_val

def place_live_order(client, symbol: str, side: str, qty: float, client_order_id: str = None):
    # --- HARD SAFETY: quote asset ---
    if not symbol.endswith(QUOTE_ASSET):
        raise RuntimeError(f"Symbol {symbol} does not match QUOTE_ASSET={QUOTE_ASSET}")

    # --- MODE GUARD ---
    if TRADING_MODE != "LIVE":
        return None

    side = side.strip().upper()
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side={side} (expected BUY/SELL)")

    # --- PANIC / KILL SWITCH ---
    if PANIC_DISABLE_TRADING:
        logging.warning(
            "LIVE ORDER BLOCKED (PANIC_DISABLE_TRADING=1) symbol=%s side=%s qty=%.8f",
            symbol, side, qty
        )
        return None

    # --- MASTER ENABLE ---
    if not LIVE_ORDERS_ENABLED:
        logging.warning(
            "LIVE ORDER BLOCKED (LIVE_ORDERS_ENABLED=0) symbol=%s side=%s qty=%.8f",
            symbol, side, qty
        )
        return None

    # --- idempotency ---
    if client_order_id is None:
        client_order_id = str(uuid.uuid4())

    # --- exchange filters ---
    step_size, min_qty, min_notional = _get_symbol_filters(client, symbol)

    qty_adj = _floor_to_step(qty, step_size)
    if qty_adj <= 0 or qty_adj < min_qty:
        raise RuntimeError(
            f"Qty too small after step rounding: qty={qty} -> {qty_adj}, min_qty={min_qty}"
        )

    # --- single price fetch ---
    px = float(client.get_symbol_ticker(symbol=symbol)["price"])
    notional = px * qty_adj

    # --- safety: max notional ---
    if LIVE_MAX_NOTIONAL > 0 and notional > LIVE_MAX_NOTIONAL:
        raise RuntimeError(
            f"Notional {notional:.2f} > LIVE_MAX_NOTIONAL {LIVE_MAX_NOTIONAL:.2f}"
        )

    # --- exchange MIN_NOTIONAL ---
    if min_notional and min_notional > 0 and notional < min_notional:
        raise RuntimeError(
            f"Notional {notional:.2f} < minNotional {min_notional:.2f} for {symbol}"
        )

    logging.warning(
        "LIVE ORDER SENDING symbol=%s side=%s type=MARKET qty=%.8f clientOrderId=%s",
        symbol, side, qty_adj, client_order_id
    )

    resp = client.create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty_adj,
        newClientOrderId=client_order_id,
    )

    logging.warning(
        "LIVE ORDER ACK symbol=%s side=%s orderId=%s status=%s",
        symbol, side, resp.get("orderId"), resp.get("status")
    )

    return resp
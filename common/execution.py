# common/execution.py
import uuid
import logging
from decimal import Decimal, ROUND_DOWN
from binance.exceptions import BinanceAPIException

def _base_asset_from_symbol(symbol: str, quote_asset: str) -> str:
    s = symbol.upper()
    q = quote_asset.upper()
    if not s.endswith(q):
        return s
    return s[:-len(q)]

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

def place_live_order(
    client,
    symbol: str,
    side: str,
    qty: float,
    *,
    trading_mode: str,
    live_orders_enabled: bool,
    quote_asset: str,
    panic_disable_trading: bool,
    live_max_notional: float,
    client_order_id: str = None,
    skip_balance_precheck: bool = False,
):
    # --- HARD SAFETY: quote asset ---
    if not symbol.endswith(quote_asset.upper()):
        raise RuntimeError(f"Symbol {symbol} does not match QUOTE_ASSET={quote_asset}")

    # --- MODE GUARD ---
    if trading_mode != "LIVE":
        return None

    side = side.strip().upper()
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side={side} (expected BUY/SELL)")

    # --- PANIC / KILL SWITCH ---
    if panic_disable_trading:
        logging.warning("LIVE ORDER BLOCKED (PANIC_DISABLE_TRADING=1) symbol=%s side=%s qty=%.8f", symbol, side, qty)
        return None

    # --- MASTER ENABLE ---
    if not live_orders_enabled:
        logging.warning("LIVE ORDER BLOCKED (LIVE_ORDERS_ENABLED=0) symbol=%s side=%s qty=%.8f", symbol, side, qty)
        return None

    # --- idempotency ---
    if client_order_id is None:
        client_order_id = str(uuid.uuid4())

    # --- exchange filters ---
    step_size, min_qty, min_notional = _get_symbol_filters(client, symbol)

    qty_adj = _floor_to_step(qty, step_size)
    if qty_adj <= 0 or qty_adj < min_qty:
        raise RuntimeError(f"Qty too small after step rounding: qty={qty} -> {qty_adj}, min_qty={min_qty}")

    # --- single price fetch ---
    px = float(client.get_symbol_ticker(symbol=symbol)["price"])
    notional = px * qty_adj

    # --- balance pre-check ---
    if not skip_balance_precheck:
        try:
            acct = client.get_account()
            balances = {b["asset"]: float(b["free"]) for b in acct.get("balances", [])}
        except Exception as e:
            logging.error("LIVE ORDER: failed to fetch account balances: %s", e)
            return None

        base_asset = _base_asset_from_symbol(symbol, quote_asset)
        free_base = balances.get(base_asset, 0.0)
        free_quote = balances.get(quote_asset.upper(), 0.0)

        if side == "SELL":
            if free_base + 1e-12 < qty_adj:
                logging.error(
                    "LIVE ORDER BLOCKED (insufficient balance): symbol=%s side=SELL need %s=%.8f free=%.8f",
                    symbol, base_asset, qty_adj, free_base
                )
                return None

        if side == "BUY":
            if free_quote + 1e-8 < notional:
                logging.error(
                    "LIVE ORDER BLOCKED (insufficient balance): symbol=%s side=BUY need %s≈%.4f free=%.4f (qty=%.8f px=%.2f)",
                    symbol, quote_asset.upper(), notional, free_quote, qty_adj, px
                )
                return None

    # --- safety: max notional ---
    if live_max_notional > 0 and notional > live_max_notional:
        raise RuntimeError(f"Notional {notional:.2f} > LIVE_MAX_NOTIONAL {live_max_notional:.2f}")

    # --- exchange MIN_NOTIONAL ---
    if min_notional and min_notional > 0 and notional < min_notional:
        raise RuntimeError(f"Notional {notional:.2f} < minNotional {min_notional:.2f} for {symbol}")

    logging.warning(
        "LIVE ORDER SENDING symbol=%s side=%s type=MARKET qty=%.8f clientOrderId=%s",
        symbol, side, qty_adj, client_order_id
    )

    try:
        resp = client.create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=qty_adj,
            newClientOrderId=client_order_id,
        )
    except BinanceAPIException as e:
        if getattr(e, "code", None) == -2010:
            logging.error("LIVE ORDER REJECTED (-2010 insufficient balance): %s", e)
            return None
        raise

    logging.warning(
        "LIVE ORDER ACK symbol=%s side=%s orderId=%s status=%s",
        symbol, side, resp.get("orderId"), resp.get("status")
    )
    return resp
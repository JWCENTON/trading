# common/execution.py
import uuid
import time
import logging
from binance.exceptions import BinanceAPIException
from decimal import Decimal, ROUND_DOWN, ROUND_UP


def build_live_client_order_id(symbol: str, pos_id: int, leg: str) -> str:
    """
    Binance SPOT limit: 36 chars.
    Canon: ORC|L|{symbol}|P{pos_id}|{E|X}
    """
    sym = str(symbol).upper()
    leg_u = str(leg).upper()[:1]  # "E" or "X"
    cid = f"ORC|L|{sym}|P{int(pos_id)}|{leg_u}"
    if len(cid) <= 36:
        return cid
    return f"ORC|L|P{int(pos_id)}|{leg_u}"[:36]


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def get_best_bid_ask(client, symbol: str):
    ob = client.get_order_book(symbol=symbol, limit=5)
    best_bid = _safe_float(ob["bids"][0][0]) if ob.get("bids") else None
    best_ask = _safe_float(ob["asks"][0][0]) if ob.get("asks") else None
    return best_bid, best_ask


def mk_child_client_order_id(base_id: str, suffix: str) -> str:
    # Binance max 36 chars for newClientOrderId
    s = str(suffix).upper()[:3]
    b = str(base_id)
    if len(b) > 32:
        b = b[:32]
    return f"{b}-{s}"[:36]


def place_live_exit_maker_then_market(
    client,
    symbol: str,
    side: str,
    qty: float,
    *,
    base_client_order_id: str,
    maker_offset_bps: float = 2.0,
    timeout_sec: int = 7,
    poll_sec: float = 1.0,
):
    """
    EXIT execution:
      1) LIMIT_MAKER (post-only)
      2) poll up to timeout_sec
      3) cancel + MARKET for remaining
    Returns dict {ok, live_ok, status, executed_qty, filled_as, resp, maker_price, best_bid, best_ask}
    """

    side_u = str(side).upper()
    qty_f = float(qty)

    best_bid, best_ask = get_best_bid_ask(client, symbol)
    if best_bid is None or best_ask is None:
        return {"ok": False, "live_ok": False, "status": "NO_BOOK", "executed_qty": 0.0, "filled_as": None, "resp": {"error": "order_book_empty"}}

    # SELL: price slightly ABOVE best_bid to remain maker
    # BUY:  price slightly BELOW best_ask to remain maker
    if side_u == "SELL":
        maker_price = best_bid * (1.0 + maker_offset_bps / 10_000.0)
    else:
        maker_price = best_ask * (1.0 - maker_offset_bps / 10_000.0)

    maker_cid = mk_child_client_order_id(base_client_order_id, "MKR")

    try:
        resp_maker = client.create_order(
            symbol=symbol,
            side=side_u,
            type="LIMIT_MAKER",
            quantity=f"{qty_f:.8f}",
            price=f"{maker_price:.2f}",
            newClientOrderId=maker_cid,
        )
    except Exception as e:
        return {"ok": False, "live_ok": False, "status": "MAKER_CREATE_FAILED", "executed_qty": 0.0, "filled_as": None, "resp": {"error": str(e)}, "maker_price": float(maker_price), "best_bid": best_bid, "best_ask": best_ask}

    order_id = resp_maker.get("orderId")
    executed_qty = 0.0

    deadline = time.time() + max(0, int(timeout_sec))
    status = None

    while time.time() < deadline:
        time.sleep(max(0.1, float(poll_sec)))
        try:
            o = client.get_order(symbol=symbol, orderId=order_id)
        except Exception:
            continue

        status = str(o.get("status", "")).upper()
        executed_qty = _safe_float(o.get("executedQty"), 0.0)

        if status == "FILLED" or executed_qty >= qty_f * 0.999:
            return {
                "ok": True,
                "live_ok": True,
                "status": status or "FILLED_BY_QTY",
                "executed_qty": float(executed_qty),
                "filled_as": "MAKER",
                "resp": {"maker_create": resp_maker, "maker_final": o},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

    # timeout -> cancel
    try:
        cancel_resp = client.cancel_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        cancel_resp = {"error": str(e)}

    remaining = max(0.0, qty_f - float(executed_qty))

    if remaining > 0.0:
        mkt_cid = mk_child_client_order_id(base_client_order_id, "MKT")
        try:
            resp_mkt = client.create_order(
                symbol=symbol,
                side=side_u,
                type="MARKET",
                quantity=f"{remaining:.8f}",
                newClientOrderId=mkt_cid,
            )
            mkt_status = str(resp_mkt.get("status", "")).upper()
            mkt_exec = _safe_float(resp_mkt.get("executedQty"), 0.0)
            live_ok = (mkt_status == "FILLED") or (mkt_exec > 0.0)
            return {
                "ok": True,
                "live_ok": bool(live_ok),
                "status": "FALLBACK_MARKET",
                "executed_qty": float(executed_qty) + float(mkt_exec),
                "filled_as": "MARKET_FALLBACK",
                "resp": {"maker_create": resp_maker, "cancel": cancel_resp, "market": resp_mkt},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }
        except Exception as e:
            return {
                "ok": False,
                "live_ok": False,
                "status": "FALLBACK_MARKET_FAILED",
                "executed_qty": float(executed_qty),
                "filled_as": None,
                "resp": {"maker_create": resp_maker, "cancel": cancel_resp, "error": str(e)},
                "maker_price": float(maker_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }

    return {
        "ok": True,
        "live_ok": float(executed_qty) > 0.0,
        "status": "MAKER_TIMEOUT_NO_REMAIN",
        "executed_qty": float(executed_qty),
        "filled_as": "MAKER_TIMEOUT",
        "resp": {"maker_create": resp_maker, "cancel": cancel_resp},
        "maker_price": float(maker_price),
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


def _blocked(reason: str, **meta):
    return {"ok": False, "blocked": True, "reason": reason, "meta": meta}


def compute_live_qty_from_notional(
    client,
    symbol: str,
    *,
    target_notional: float,
    quote_asset: str,
    min_notional_buffer_pct: float = 0.05,  # 5% bufor
):
    step_size, min_qty, min_notional = _get_symbol_filters(client, symbol)

    px = float(client.get_symbol_ticker(symbol=symbol)["price"])

    # docelowy notional: max z target i minNotional*(1+bufor)
    want_notional = float(target_notional)
    if min_notional and min_notional > 0:
        want_notional = max(want_notional, min_notional * (1.0 + min_notional_buffer_pct))

    qty_raw = want_notional / px

    # CEIL do kroku, żeby nie spaść poniżej minNotional przez rounding
    qty_adj = float(_ceil_to_step(qty_raw, step_size))

    # jeszcze minQty
    if min_qty and qty_adj < min_qty:
        qty_adj = float(_ceil_to_step(min_qty, step_size))

    notional = px * qty_adj
    return qty_adj, px, notional, step_size, min_qty, min_notional


def _qty_to_plain_str(qty) -> str:
    """
    Binance expects: ^([0-9]{1,20})(\.[0-9]{1,20})?$
    so: no exponent, no commas, no spaces.
    """
    d = Decimal(str(qty))  # str() to avoid numpy repr issues
    # normalize to plain decimal (no exponent)
    s = format(d, "f")
    # trim trailing zeros
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _ceil_to_step(qty: float, step: str) -> Decimal:
    q = Decimal(str(qty))
    s = Decimal(str(step))
    if s == 0:
        return q
    return (q / s).to_integral_value(rounding=ROUND_UP) * s


def _base_asset_from_symbol(symbol: str, quote_asset: str) -> str:
    s = symbol.upper()
    q = quote_asset.upper()
    if not s.endswith(q):
        return s
    return s[:-len(q)]


def _floor_to_step(qty: float, step: str) -> Decimal:
    q = Decimal(str(qty))
    s = Decimal(str(step))
    if s == 0:
        return q
    return (q / s).to_integral_value(rounding=ROUND_DOWN) * s


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
        return _blocked("NOT_LIVE_MODE", trading_mode=trading_mode)

    side = side.strip().upper()
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side={side} (expected BUY/SELL)")

    # --- PANIC / KILL SWITCH ---
    if panic_disable_trading:
        logging.warning("LIVE ORDER BLOCKED (PANIC_DISABLE_TRADING=1) symbol=%s side=%s qty=%.8f", symbol, side, qty)
        return _blocked("PANIC_DISABLE_TRADING", symbol=symbol, side=side, qty=float(qty))

    # --- MASTER ENABLE ---
    if not live_orders_enabled:
        logging.warning("LIVE ORDER BLOCKED (LIVE_ORDERS_ENABLED=0) symbol=%s side=%s qty=%.8f", symbol, side, qty)
        return _blocked("LIVE_ORDERS_DISABLED", symbol=symbol, side=side, qty=float(qty))

    # --- idempotency ---
    if client_order_id is None:
        client_order_id = str(uuid.uuid4())

    # --- exchange filters ---
    step_size, min_qty, min_notional = _get_symbol_filters(client, symbol)

    qty_adj = _floor_to_step(qty, step_size)
    if qty_adj <= 0 or qty_adj < Decimal(str(min_qty)):
        logging.warning(
            "LIVE ORDER BLOCKED (minQty/step): symbol=%s side=%s qty=%.8f -> qty_adj=%.8f step=%s min_qty=%.8f",
            symbol, side, qty, qty_adj, step_size, min_qty
        )
        return _blocked("MIN_QTY_OR_STEP", qty=float(qty), qty_adj=float(qty_adj), step=step_size, min_qty=float(min_qty))

    # --- single price fetch ---
    px = float(client.get_symbol_ticker(symbol=symbol)["price"])
    notional = Decimal(str(px)) * qty_adj

    # --- balance pre-check ---
    if not skip_balance_precheck:
        try:
            acct = client.get_account()
            balances = {b["asset"]: float(b["free"]) for b in acct.get("balances", [])}
        except BinanceAPIException as e:
            logging.error(
                "LIVE ORDER REJECTED symbol=%s side=%s qty=%.8f code=%s msg=%s clientOrderId=%s",
                symbol, side, float(qty_adj),
                getattr(e, "code", None),
                getattr(e, "message", str(e)),
                client_order_id,
            )
            return None
        except Exception:
            logging.exception(
                "LIVE ORDER ERROR symbol=%s side=%s qty=%.8f clientOrderId=%s",
                symbol, side, float(qty_adj), client_order_id
            )
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
                return _blocked("INSUFFICIENT_BASE", base_asset=base_asset, need=float(qty_adj), free=float(free_base))

        if side == "BUY":
            if free_quote + 1e-8 < notional:
                logging.error(
                    "LIVE ORDER BLOCKED (insufficient balance): symbol=%s side=BUY need %s≈%.4f free=%.4f (qty=%.8f px=%.2f)",
                    symbol, quote_asset.upper(), notional, free_quote, qty_adj, px
                )
                return _blocked("INSUFFICIENT_QUOTE", quote_asset=quote_asset.upper(), need=float(notional), free=float(free_quote), qty=float(qty_adj), px=float(px))

    # --- safety: max notional ---
    if live_max_notional > 0 and notional > live_max_notional:
        raise RuntimeError(f"Notional {notional:.2f} > LIVE_MAX_NOTIONAL {live_max_notional:.2f}")

    # --- exchange MIN_NOTIONAL ---
    if min_notional and min_notional > 0 and notional < min_notional:
        logging.warning(
            "LIVE ORDER BLOCKED (minNotional): symbol=%s side=%s notional=%.4f < min_notional=%.4f (qty=%.8f px=%.4f)",
            symbol, side, notional, min_notional, qty_adj, px
        )
        return _blocked("MIN_NOTIONAL", notional=float(notional), min_notional=float(min_notional), qty=float(qty_adj), px=float(px))

    logging.warning(
        "LIVE ORDER SENDING symbol=%s side=%s type=MARKET qty=%.8f clientOrderId=%s",
        symbol, side, float(qty_adj), client_order_id
    )

    try:
        resp = client.create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=_qty_to_plain_str(qty_adj),
            newClientOrderId=client_order_id,
        )
    except BinanceAPIException as e:
        if getattr(e, "code", None) == -2010:
            logging.error("LIVE ORDER REJECTED (-2010 insufficient balance): %s", e)
            return _blocked("BINANCE_REJECTED_2010", code=getattr(e, "code", None), msg=getattr(e, "message", str(e)))
        raise

    logging.warning(
        "LIVE ORDER ACK symbol=%s side=%s orderId=%s status=%s",
        symbol, side, resp.get("orderId"), resp.get("status")
    )
    status = (resp or {}).get("status")
    executed_qty = float((resp or {}).get("executedQty", 0) or 0)
    live_ok = (status in ("FILLED", "PARTIALLY_FILLED")) and executed_qty > 0

    return {
        "ok": True,               # backward compat = ACK
        "blocked": False,
        "resp": resp,
        "send_ok": True,
        "live_ok": live_ok,
        "status": status,
        "executed_qty": executed_qty,
        }
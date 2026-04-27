# common/execution.py
import uuid
import time
import logging
from binance.exceptions import BinanceAPIException
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import re

_CID_RE = re.compile(r"[^A-Za-z0-9\-_]")

def build_live_client_order_id(symbol: str, pos_id: int, leg: str) -> str:
    """
    Binance SPOT newClientOrderId: ^[a-zA-Z0-9-_]{1,36}$
    Deterministic + short.
    Format: ORC-L-{SYMBOL}-P{pos}-{E|X}-{h6}
    """
    sym = str(symbol).upper()
    leg_u = str(leg).upper()[:1]  # E / X

    base = f"ORC-L-{sym}-P{int(pos_id)}-{leg_u}"
    base = _CID_RE.sub("-", base)  # safety, should already be ok

    # ensure <= 36 with deterministic hash suffix
    if len(base) <= 36:
        return base

    h6 = hashlib.sha1(base.encode("utf-8")).hexdigest()[:6]
    # keep front, append -h6
    keep = 36 - (1 + len(h6))  # "-"+h6
    return f"{base[:keep]}-{h6}"


def build_live_entry_intent_client_order_id(
    symbol: str,
    strategy: str,
    interval: str,
    candle_open_time,
) -> str:
    """
    CID for LIVE ENTRY before position row exists.
    Must fit Binance <= 36 chars.
    """
    sym = str(symbol).upper()
    strat = str(strategy).upper()[:4]
    itv = str(interval).lower()

    ts_raw = str(candle_open_time)
    h8 = hashlib.sha1(f"{sym}|{strat}|{itv}|{ts_raw}".encode("utf-8")).hexdigest()[:8]

    base = f"ORC-L-{sym}-{strat}-{itv}-E-{h8}"
    base = _CID_RE.sub("-", base)

    if len(base) <= 36:
        return base

    h6 = hashlib.sha1(base.encode("utf-8")).hexdigest()[:6]
    keep = 36 - (1 + len(h6))
    return f"{base[:keep]}-{h6}"


def _attach_order_to_position(conn, *, position_id: int, leg: str, client_order_id: str, order_id: str):
    """
    Attach on ACK (deterministic SSOT binding).
    No commit here — caller owns transaction.
    """
    leg_u = str(leg).upper()
    if leg_u in ("ENTRY", "E"):
        sql = """
            UPDATE positions
            SET entry_client_order_id = COALESCE(entry_client_order_id, %s),
                entry_order_id        = COALESCE(entry_order_id, %s)
            WHERE id = %s;
        """
    elif leg_u in ("EXIT", "X"):
        sql = """
            UPDATE positions
            SET exit_client_order_id = COALESCE(exit_client_order_id, %s),
                exit_order_id         = COALESCE(exit_order_id, %s)
            WHERE id = %s;
        """
    else:
        raise ValueError(f"Invalid leg={leg} (expected ENTRY/EXIT)")

    with conn.cursor() as cur:
        cur.execute(sql, (str(client_order_id), str(order_id) if order_id is not None else None, int(position_id)))


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


def compute_sellable_asset_qty(
    client,
    symbol: str,
    *,
    requested_qty: float,
    quote_asset: str,
    reserve_step_count: int = 1,
):
    step_size, min_qty, _ = _get_symbol_filters(client, symbol)
    acct = client.get_account()
    bals = {b["asset"]: float(b["free"]) for b in acct.get("balances", [])}

    base_asset = _base_asset_from_symbol(symbol, quote_asset)
    free_qty = float(bals.get(base_asset.upper(), 0.0))

    reserve = float(Decimal(str(step_size)) * Decimal(str(max(0, reserve_step_count))))
    usable_free = max(0.0, free_qty - reserve)
    sellable_qty = float(_floor_to_step(min(float(requested_qty), usable_free), step_size))

    return {
        "requested_qty": float(requested_qty),
        "free_qty": float(free_qty),
        "sellable_qty": float(sellable_qty),
        "step_size": float(step_size),
        "min_qty": float(min_qty),
        "base_asset": str(base_asset).upper(),
    }


def preflight_live_order(
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
    skip_balance_precheck: bool = False,
):
    """
    Full business validation BEFORE create_order().
    Returns canonical result:
      {ok, blocked, reason, meta, qty_adj, px, notional}
    """
    try:
        symbol = str(symbol).upper()
        side = str(side).upper()

        if trading_mode != "LIVE":
            return _blocked("NOT_LIVE_MODE", trading_mode=trading_mode)

        if side not in ("BUY", "SELL"):
            return _blocked("INVALID_SIDE", side=side)

        if panic_disable_trading:
            return _blocked("PANIC_DISABLE_TRADING")

        if not live_orders_enabled:
            return _blocked("LIVE_ORDERS_DISABLED")

        if quote_asset and not symbol.endswith(str(quote_asset).upper()):
            return _blocked("QUOTE_ASSET_MISMATCH", symbol=symbol, quote_asset=quote_asset)

        step_size, min_qty, min_notional = _get_symbol_filters(client, symbol)

        qty_adj = float(_floor_to_step(qty, step_size))
        if min_qty and qty_adj < min_qty:
            return _blocked(
                "QTY_BELOW_MIN_QTY",
                qty=float(qty),
                qty_adj=float(qty_adj),
                min_qty=float(min_qty),
                step=float(step_size),
            )

        px = float(client.get_symbol_ticker(symbol=symbol)["price"])
        notional = float(px * qty_adj)

        if min_notional and notional < min_notional:
            return _blocked(
                "NOTIONAL_BELOW_MIN_NOTIONAL",
                qty=float(qty_adj),
                px=float(px),
                notional=float(notional),
                min_notional=float(min_notional),
            )

        if (not skip_balance_precheck) and side == "BUY":
            # zostaw obecną logikę balance check, tylko bez exceptionów
            try:
                acct = client.get_account()
                bals = {b["asset"]: float(b["free"]) for b in acct.get("balances", [])}
                free_quote = float(bals.get(str(quote_asset).upper(), 0.0))
                if free_quote + 1e-9 < notional:
                    return _blocked(
                        "INSUFFICIENT_QUOTE_BALANCE",
                        quote_asset=str(quote_asset).upper(),
                        free=float(free_quote),
                        needed=float(notional),
                    )
            except Exception as e:
                return _blocked("BALANCE_PRECHECK_FAILED", err=str(e))

        if side == "SELL":
            try:
                sell_meta = compute_sellable_asset_qty(
                    client,
                    symbol,
                    requested_qty=float(qty_adj),
                    quote_asset=quote_asset,
                )
                sellable_qty = float(sell_meta["sellable_qty"])

                if sellable_qty <= 0.0:
                    return _blocked("EXIT_BLOCKED_INSUFFICIENT_BALANCE", **sell_meta)

                if min_qty and sellable_qty < min_qty:
                    return _blocked("EXIT_BLOCKED_TOO_SMALL_BALANCE", **sell_meta)

                qty_adj = float(sellable_qty)
                notional = float(px * qty_adj)

                if min_notional and notional < min_notional:
                    return _blocked(
                        "EXIT_BLOCKED_NOTIONAL_TOO_SMALL",
                        **sell_meta,
                        px=float(px),
                        notional=float(notional),
                        min_notional=float(min_notional),
                    )
            except Exception as e:
                return _blocked("EXIT_BALANCE_PRECHECK_FAILED", err=str(e))


        if live_max_notional and float(live_max_notional) > 0 and notional > float(live_max_notional):
            return _blocked(
                "LIVE_MAX_NOTIONAL_EXCEEDED",
                qty=float(qty_adj),
                px=float(px),
                notional=float(notional),
                live_max_notional=float(live_max_notional),
            )

        return {
            "ok": True,
            "blocked": False,
            "reason": None,
            "meta": {
                "qty": float(qty),
                "qty_adj": float(qty_adj),
                "px": float(px),
                "notional": float(notional),
                "step": float(step_size) if step_size is not None else None,
                "min_qty": float(min_qty) if min_qty is not None else None,
                "min_notional": float(min_notional) if min_notional is not None else None,
            },
            "qty_adj": float(qty_adj),
            "px": float(px),
            "notional": float(notional),
        }

    except Exception as e:
        return _blocked("PREFLIGHT_EXCEPTION", err=str(e))


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
    # --- SSOT binding (optional but required for SSOT-managed LIVE) ---
    db_conn=None,
    position_id: int | None = None,
    leg: str | None = None,
):
    pre = preflight_live_order(
        client,
        symbol,
        side,
        qty,
        trading_mode=trading_mode,
        live_orders_enabled=live_orders_enabled,
        quote_asset=quote_asset,
        panic_disable_trading=panic_disable_trading,
        live_max_notional=live_max_notional,
        skip_balance_precheck=skip_balance_precheck,
    )

    if not pre.get("ok"):
        return {
            "ok": False,
            "live_ok": False,
            "blocked": True,
            "reason": pre.get("reason"),
            "meta": pre.get("meta"),
            "resp": None,
        }

    qty = float(pre["qty_adj"])

    if client_order_id is None:
        if trading_mode == "LIVE" and position_id is not None and leg is not None:
            leg_char = "E" if str(leg).upper().startswith("E") else "X"
            client_order_id = build_live_client_order_id(symbol, int(position_id), leg_char)
        else:
            client_order_id = str(uuid.uuid4())

    try:
        logging.info(
            "LIVE ORDER SEND symbol=%s side=%s qty=%.8f clientOrderId=%s",
            symbol, side, qty, client_order_id
        )

        resp = client.create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=_qty_to_plain_str(qty),
            newClientOrderId=client_order_id,
        )

        status = str(resp.get("status", "")).upper()
        executed_qty = _safe_float(resp.get("executedQty"), 0.0)
        live_ok = (status == "FILLED") or (executed_qty > 0.0)

        # --- SSOT attach (CRITICAL FIX) ---
        if live_ok and db_conn is not None and position_id is not None and leg is not None:
            try:
                order_id = resp.get("orderId")
                _attach_order_to_position(
                    db_conn,
                    position_id=int(position_id),
                    leg=str(leg),
                    client_order_id=str(client_order_id),
                    order_id=str(order_id) if order_id is not None else None,
                )
            except Exception as e:
                logging.exception(
                    "SSOT ATTACH FAILED position_id=%s leg=%s clientOrderId=%s err=%s",
                    position_id, leg, client_order_id, str(e)
                )

        return {
            "ok": True,
            "live_ok": bool(live_ok),
            "blocked": False,
            "reason": None,
            "resp": resp,
            "client_order_id": client_order_id,
        }

    except BinanceAPIException as e:
        logging.error(
            "LIVE ORDER REJECTED symbol=%s side=%s qty=%.8f code=%s msg=%s clientOrderId=%s",
            symbol, side, qty,
            getattr(e, "code", None),
            getattr(e, "message", str(e)),
            client_order_id,
        )
        return {
            "ok": False,
            "live_ok": False,
            "blocked": False,
            "reason": "BINANCE_API_EXCEPTION",
            "meta": {"code": getattr(e, "code", None), "msg": getattr(e, "message", str(e))},
            "resp": None,
        }

    except Exception as e:
        logging.exception(
            "LIVE ORDER ERROR symbol=%s side=%s qty=%.8f clientOrderId=%s",
            symbol, side, qty, client_order_id
        )
        return {
            "ok": False,
            "live_ok": False,
            "blocked": False,
            "reason": "LIVE_ORDER_EXCEPTION",
            "meta": {"err": str(e)},
            "resp": None,
        }
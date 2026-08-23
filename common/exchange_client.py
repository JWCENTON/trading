# common/exchange_client.py
from __future__ import annotations

import os
import time
import logging
import hmac
import hashlib
import base64
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json

from common.exchange_symbols import to_exchange_symbol
from common.financial_truth_identity import (
    AccountIdentityCache,
    ExchangeAccountIdentity,
    okx_account_identity,
)


_OKX_ACCOUNT_IDENTITY_CACHE = AccountIdentityCache()


class FillFetchResult(list):
    """List-compatible fill result with an explicit fetch-boundary contract."""

    def __init__(
        self,
        values,
        *,
        filter_applied: bool,
        filter_mode: str,
        requested_boundary: int | None,
        effective_boundary: int | None,
    ):
        super().__init__(values)
        self.filter_applied = bool(filter_applied)
        self.filter_mode = str(filter_mode)
        self.requested_boundary = requested_boundary
        self.effective_boundary = effective_boundary


class ExchangeAPIException(Exception):
    def __init__(self, message: str, *, code=None, raw=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.raw = raw


def _okx_client_order_id(client_order_id: str | None) -> str | None:
    if not client_order_id:
        return None
    clean = "".join(ch for ch in str(client_order_id) if ch.isalnum())
    return clean[:32] if clean else None


def _exact_nonnegative_decimal(value: object, *, field: str) -> str:
    """Render an exchange decimal without a binary-float round trip."""
    try:
        number = Decimal(str(value if value not in (None, "") else "0"))
    except (InvalidOperation, ValueError) as exc:
        raise ExchangeAPIException(f"OKX {field} is not a decimal") from exc
    if not number.is_finite():
        raise ExchangeAPIException(f"OKX {field} must be finite")
    number = abs(number)
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"



class BinanceMarketDataAdapter:
    def __init__(self):
        from binance.client import Client

        api_key = os.environ.get("BINANCE_API_KEY")
        api_secret = os.environ.get("BINANCE_API_SECRET")
        self.client = Client(api_key=api_key, api_secret=api_secret)

    def get_symbol_info(self, symbol: str):
        return self.client.get_symbol_info(symbol)

    def get_symbol_ticker(self, *, symbol: str):
        return self.client.get_symbol_ticker(symbol=symbol)

    def get_last_price(self, symbol: str) -> float:
        return float(self.get_symbol_ticker(symbol=symbol)["price"])

    def get_account(self):
        return self.client.get_account()

    def get_balances(self) -> dict:
        acct = self.get_account()
        return {b["asset"].upper(): float(b["free"]) for b in acct.get("balances", [])}

    def get_order_book(self, *, symbol: str, limit: int = 5):
        return self.client.get_order_book(symbol=symbol, limit=limit)

    def get_best_bid_ask(self, symbol: str):
        ob = self.get_order_book(symbol=symbol, limit=5)
        best_bid = float(ob["bids"][0][0]) if ob.get("bids") else None
        best_ask = float(ob["asks"][0][0]) if ob.get("asks") else None
        return best_bid, best_ask

    def create_order(self, **kwargs):
        try:
            return self.client.create_order(**kwargs)
        except Exception as e:
            raise ExchangeAPIException(
                str(getattr(e, "message", str(e))),
                code=getattr(e, "code", None),
                raw=e,
            ) from e

    def place_market_order(self, *, symbol: str, side: str, quantity: str, client_order_id: str | None = None):
        kwargs = {
            "symbol": symbol,
            "side": str(side).upper(),
            "type": "MARKET",
            "quantity": quantity,
        }
        if client_order_id:
            kwargs["newClientOrderId"] = client_order_id
        return self.create_order(**kwargs)

    def place_limit_maker_order(self, *, symbol: str, side: str, quantity: str, price: str, client_order_id: str | None = None):
        kwargs = {
            "symbol": symbol,
            "side": str(side).upper(),
            "type": "LIMIT_MAKER",
            "quantity": quantity,
            "price": price,
        }
        if client_order_id:
            kwargs["newClientOrderId"] = client_order_id
        return self.create_order(**kwargs)

    def get_order(self, **kwargs):
        try:
            return self.client.get_order(**kwargs)
        except Exception as e:
            raise ExchangeAPIException(
                str(getattr(e, "message", str(e))),
                code=getattr(e, "code", None),
                raw=e,
            ) from e

    def get_order_status(self, *, symbol: str, order_id):
        return self.get_order(symbol=symbol, orderId=order_id)

    def find_order_by_client_order_id(
        self, *, symbol: str, client_order_id: str
    ) -> dict:
        """Read-only exact order recovery by the exchange client order ID."""
        try:
            order = self.get_order(
                symbol=symbol,
                origClientOrderId=str(client_order_id),
            )
        except ExchangeAPIException as exc:
            # Binance Spot documents -2013 for an order identity that does not
            # exist. Other failures (including auth/transport) are not absence.
            if str(exc.code) == "-2013":
                return {
                    "outcome": "NOT_FOUND",
                    "order": None,
                    "error_code": exc.code,
                    "error_message": exc.message,
                }
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": exc.code,
                "error_message": exc.message,
            }
        except Exception as exc:
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": None,
                "error_message": str(exc),
            }

        if order is None:
            return {
                "outcome": "NOT_FOUND",
                "order": None,
                "error_code": None,
                "error_message": None,
            }
        if not isinstance(order, dict) or not order:
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exact client order lookup returned a malformed row",
            }
        returned_client_order_id = str(order.get("clientOrderId") or "")
        if returned_client_order_id != str(client_order_id) or not order.get(
            "orderId"
        ):
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exchange client or order identity mismatch",
            }
        return {
            "outcome": "FOUND",
            "order": order,
            "error_code": None,
            "error_message": None,
        }

    def cancel_order(self, **kwargs):
        try:
            return self.client.cancel_order(**kwargs)
        except Exception as e:
            raise ExchangeAPIException(
                str(getattr(e, "message", str(e))),
                code=getattr(e, "code", None),
                raw=e,
            ) from e

    def cancel_order_by_id(self, *, symbol: str, order_id):
        return self.cancel_order(symbol=symbol, orderId=order_id)

    def get_my_trades(self, **kwargs):
        return self.client.get_my_trades(**kwargs)

    def get_klines(self, *, symbol: str, interval: str, limit: int = 1000, start_ms: Optional[int] = None) -> List[list]:
        kwargs: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": int(limit),
        }
        if start_ms is not None:
            kwargs["startTime"] = int(start_ms)
        return self.client.get_klines(**kwargs)


class OkxMarketDataAdapter:
    def __init__(self):
        self.base_url = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
        self.timeout = float(os.environ.get("OKX_HTTP_TIMEOUT_SECONDS", "10"))

    @staticmethod
    def _okx_bar(interval: str) -> str:
        # OKX supports 1m/3m/5m/15m/30m/1H/2H/4H...
        s = str(interval).strip()
        if s.endswith("h"):
            return s[:-1] + "H"
        if s.endswith("d"):
            return s[:-1] + "D"
        return s

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        s = str(interval).strip()
        unit = s[-1]
        n = int(s[:-1])
        if unit == "m":
            return n * 60_000
        if unit in ("h", "H"):
            return n * 60 * 60_000
        if unit in ("d", "D"):
            return n * 24 * 60 * 60_000
        raise ValueError(f"Unsupported interval={interval}")

    @classmethod
    def _to_binance_like_kline(cls, k: list, interval: str) -> list:
        """
        OKX candle:
          [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        Binance-like internal:
          [open_time, open, high, low, close, volume, close_time, quote_volume, trades]
        """
        ts = int(k[0])
        open_px = k[1]
        high = k[2]
        low = k[3]
        close = k[4]
        volume = k[5]
        close_time = ts + cls._interval_to_ms(interval) - 1
        trades = 0
        return [ts, open_px, high, low, close, volume, close_time, None, trades]

    def _okx_credentials(self) -> tuple[str, str, str]:
        api_key = os.environ.get("OKX_API_KEY", "").strip()
        api_secret = os.environ.get("OKX_API_SECRET", "").strip()
        passphrase = os.environ.get("OKX_API_PASSPHRASE", "").strip()

        if not api_key or not api_secret or not passphrase:
            raise RuntimeError("Missing OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE")

        return api_key, api_secret, passphrase

    def _credential_cache_scope(self) -> str:
        api_key, _api_secret, _passphrase = self._okx_credentials()
        material = "|".join(
            (
                self.base_url,
                api_key,
                os.environ.get("OKX_TESTNET", "false").strip().lower(),
            )
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    @staticmethod
    def _okx_timestamp() -> str:
        # OKX accepts ISO-8601 UTC timestamp with milliseconds, e.g. 2020-12-08T09:08:57.715Z
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    @staticmethod
    def _json_body(payload: Optional[Dict[str, Any]]) -> str:
        if not payload:
            return ""
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _sign(secret: str, message: str) -> str:
        digest = hmac.new(
            secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _private_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        api_key, api_secret, passphrase = self._okx_credentials()

        method_u = str(method).upper()
        query = f"?{urlencode(params)}" if params else ""
        url_path = f"{path}{query}"
        body_s = self._json_body(body)

        ts = self._okx_timestamp()
        sign_payload = f"{ts}{method_u}{url_path}{body_s}"
        sign = self._sign(api_secret, sign_payload)

        headers = {
            "User-Agent": "waltrade-bot/okx-private",
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": passphrase,
        }

        if os.environ.get("OKX_TESTNET", "false").strip().lower() in ("1", "true", "yes", "on"):
            headers["x-simulated-trading"] = "1"

        data_bytes = body_s.encode("utf-8") if body_s else None
        url = f"{self.base_url}{url_path}"
        req = Request(url, data=data_bytes, headers=headers, method=method_u)

        safe_meta = {
            "method": method_u,
            "base_url": self.base_url,
            "request_path": url_path,
            "query_present": bool(query),
            "body_length": len(body_s),
            "timestamp": ts,
            "signature_length": len(sign),
            "header_names": tuple(sorted(headers)),
            "demo_header": "x-simulated-trading" in headers,
            "timeout": self.timeout,
            "http_client": "urllib.request",
            "signer": "OkxMarketDataAdapter._sign",
        }
        self._last_private_request_diagnostic = safe_meta
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                safe_meta.update({
                    "http_status": int(resp.status),
                    "server_date": resp.headers.get("Date"),
                })
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                error_payload = json.loads(raw)
            except Exception:
                error_payload = {}
            safe_meta.update({
                "http_status": int(exc.code),
                "server_date": exc.headers.get("Date") if exc.headers else None,
                "okx_code": error_payload.get("code"),
                "okx_message": error_payload.get("msg"),
            })
            raise ExchangeAPIException(
                "OKX private request rejected",
                code=error_payload.get("code") or f"HTTP_{exc.code}",
                raw={
                    "http_status": int(exc.code),
                    "code": error_payload.get("code"),
                    "msg": error_payload.get("msg"),
                },
            ) from exc

        data = json.loads(raw)
        safe_meta.update({
            "okx_code": data.get("code"),
            "okx_message": data.get("msg"),
        })
        if str(data.get("code")) != "0":
            raise ExchangeAPIException(
                f"OKX private request failed code={data.get('code')} msg={data.get('msg')}",
                code=data.get("code"),
                raw=data,
            )
        return data

    def _execution_enabled(self) -> bool:
        return os.environ.get("OKX_EXECUTION_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")

    def _execution_blocked(self, method: str):
        raise RuntimeError(f"OKX execution method blocked: {method}; set OKX_EXECUTION_ENABLED=1 only after explicit canary approval")

    def get_symbol_info(self, symbol: str):
        inst = get_okx_spot_instrument(symbol)
        lot_sz = inst.get("lotSz", "0.00000001")
        min_sz = inst.get("minSz", "0")
        return {
            "symbol": symbol,
            "exchange_symbol": to_exchange_symbol(symbol, "OKX"),
            "filters": [
                {
                    "filterType": "LOT_SIZE",
                    "stepSize": lot_sz,
                    "minQty": min_sz,
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": inst.get("minNotional", "0"),
                },
            ],
            "raw": inst,
        }

    def get_symbol_ticker(self, *, symbol: str):
        inst_id = to_exchange_symbol(symbol, "OKX")
        data = self._request("/api/v5/market/ticker", {"instId": inst_id})
        if str(data.get("code")) != "0":
            raise RuntimeError(f"OKX ticker failed code={data.get('code')} msg={data.get('msg')}")
        if not isinstance(data, dict):
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": None,
                "error_message": "exact client order lookup returned a malformed response",
            }
        rows = data.get("data") or []
        if not isinstance(rows, list):
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exact client order lookup returned malformed data",
            }
        if not rows:
            raise RuntimeError(f"OKX ticker returned no data for {inst_id}")
        return {"symbol": symbol, "price": rows[0].get("last"), "raw": rows[0]}

    def get_last_price(self, symbol: str) -> float:
        return float(self.get_symbol_ticker(symbol=symbol)["price"])

    def get_order_book(self, *, symbol: str, limit: int = 5):
        inst_id = to_exchange_symbol(symbol, "OKX")
        data = self._request("/api/v5/market/books", {"instId": inst_id, "sz": int(limit)})
        if str(data.get("code")) != "0":
            raise RuntimeError(f"OKX order book failed code={data.get('code')} msg={data.get('msg')}")
        rows = data.get("data") or []
        if not rows:
            return {"bids": [], "asks": []}
        book = rows[0]
        return {
            "bids": [[x[0], x[1]] for x in book.get("bids", [])],
            "asks": [[x[0], x[1]] for x in book.get("asks", [])],
            "raw": book,
        }

    def get_best_bid_ask(self, symbol: str):
        ob = self.get_order_book(symbol=symbol, limit=5)
        best_bid = float(ob["bids"][0][0]) if ob.get("bids") else None
        best_ask = float(ob["asks"][0][0]) if ob.get("asks") else None
        return best_bid, best_ask

    def get_account(self):
        data = self._private_request("GET", "/api/v5/account/balance")

        balances = []
        for account_row in data.get("data") or []:
            for detail in account_row.get("details") or []:
                asset = str(detail.get("ccy", "")).upper()
                if not asset:
                    continue

                raw_free = detail.get("availBal")
                if raw_free in (None, ""):
                    raw_free = detail.get("cashBal")

                raw_total = detail.get("cashBal")
                if raw_total in (None, ""):
                    raw_total = detail.get("eq")
                if raw_total in (None, ""):
                    raw_total = raw_free

                try:
                    free = float(raw_free or 0.0)
                except Exception:
                    free = 0.0

                try:
                    total = float(raw_total or 0.0)
                except Exception:
                    total = free

                locked = max(total - free, 0.0)

                balances.append({
                    "asset": asset,
                    "free": str(free),
                    "locked": str(locked),
                })

        return {
            "canTrade": True,
            "canWithdraw": False,
            "exchange": "OKX",
            "account_read_ok": True,
            "balances": balances,
            "raw": data,
        }

    def get_raw_account_balance(self):
        """Return unmodified OKX balance evidence for Decimal-safe authorities."""
        return self._private_request("GET", "/api/v5/account/balance")

    def get_trade_fee(self, *, symbol: str, instrument_type: str = "SPOT"):
        """Return raw private fee-tier evidence without numeric coercion."""
        return self._private_request(
            "GET", "/api/v5/account/trade-fee",
            params={
                "instType": str(instrument_type).upper(),
                "instId": to_exchange_symbol(symbol, "OKX"),
            },
        )

    def get_account_bills_page(
        self,
        *,
        archive: bool = False,
        after: str | None = None,
        begin_ms: int | None = None,
        end_ms: int | None = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Return one immutable-ID page of OKX Trading Account bills.

        The caller owns exhaustive pagination and completeness.  This method
        deliberately performs no timestamp/amount correlation and requests
        only OKX bill type ``1`` (Transfer).
        """
        page_size = int(limit)
        if page_size < 1 or page_size > 100:
            raise ValueError("OKX_ACCOUNT_BILLS_LIMIT_INVALID")
        params: Dict[str, Any] = {"type": "1", "limit": str(page_size)}
        if after not in (None, ""):
            params["after"] = str(after)
        if begin_ms is not None:
            params["begin"] = str(int(begin_ms))
        if end_ms is not None:
            params["end"] = str(int(end_ms))
        return self._private_request(
            "GET",
            (
                "/api/v5/account/bills-archive"
                if archive
                else "/api/v5/account/bills"
            ),
            params=params,
        )

    def get_account_identity(
        self,
        *,
        refresh: bool = False,
    ) -> tuple[ExchangeAccountIdentity, str]:
        """Read and cache exchange-proven account identity; never logs credentials."""

        def fetch() -> ExchangeAccountIdentity:
            try:
                response = self._private_request("GET", "/api/v5/account/config")
            except HTTPError as exc:
                classification = (
                    "ACCOUNT_IDENTITY_AUTH_ERROR"
                    if exc.code in {401, 403}
                    else "ACCOUNT_IDENTITY_UNAVAILABLE"
                )
                raise RuntimeError(classification) from exc
            except ExchangeAPIException as exc:
                code = str(exc.code or "")
                classification = (
                    "ACCOUNT_IDENTITY_AUTH_ERROR"
                    if code in {"50110", "50111", "50112", "50113", "50114"}
                    else "ACCOUNT_IDENTITY_UNAVAILABLE"
                )
                raise RuntimeError(classification) from exc
            try:
                return okx_account_identity(response)
            except ValueError as exc:
                raise RuntimeError("ACCOUNT_IDENTITY_INVALID_RESPONSE") from exc

        return _OKX_ACCOUNT_IDENTITY_CACHE.get(
            self._credential_cache_scope(),
            fetch,
            refresh=refresh,
        )

    def get_balances(self) -> dict:
        data = self._private_request("GET", "/api/v5/account/balance")
        balances: dict[str, float] = {}

        for account_row in data.get("data") or []:
            for detail in account_row.get("details") or []:
                ccy = str(detail.get("ccy", "")).upper()
                if not ccy:
                    continue

                # OKX fields:
                # availBal = available balance
                # cashBal  = cash balance fallback
                raw_free = detail.get("availBal")
                if raw_free in (None, ""):
                    raw_free = detail.get("cashBal")

                try:
                    free = float(raw_free or 0.0)
                except Exception:
                    free = 0.0

                balances[ccy] = balances.get(ccy, 0.0) + free

        return balances

    def create_order(self, **kwargs):
        self._execution_blocked("create_order")

    def place_market_order(self, *, symbol: str, side: str, quantity: str, client_order_id: str | None = None):
        if not self._execution_enabled():
            self._execution_blocked("place_market_order")

        inst_id = to_exchange_symbol(symbol, "OKX")
        side_l = str(side).lower()

        if side_l == "buy":
            # Internal execution passes base quantity. OKX market BUY is safest
            # as quote currency amount with tgtCcy=quote_ccy.
            px = self.get_last_price(symbol)
            quote_sz = float(quantity) * float(px)
            body = {
                "instId": inst_id,
                "tdMode": "cash",
                "side": side_l,
                "ordType": "market",
                "sz": f"{quote_sz:.8f}",
                "tgtCcy": "quote_ccy",
            }
        else:
            body = {
                "instId": inst_id,
                "tdMode": "cash",
                "side": side_l,
                "ordType": "market",
                "sz": str(quantity),
                "tgtCcy": "base_ccy",
            }

        okx_client_order_id = _okx_client_order_id(client_order_id)
        if okx_client_order_id:
            body["clOrdId"] = okx_client_order_id

        data = self._private_request("POST", "/api/v5/trade/order", body=body)
        rows = data.get("data") or []
        raw = rows[0] if rows else {}

        okx_ord_id = raw.get("ordId")
        okx_s_code = str(raw.get("sCode", "0"))
        okx_s_msg = raw.get("sMsg", "")

        if okx_s_code != "0":
            raise ExchangeAPIException(
                f"OKX market order rejected sCode={okx_s_code} sMsg={okx_s_msg}",
                code=okx_s_code,
                raw=raw,
            )

        return {
            "symbol": symbol,
            "exchange_symbol": inst_id,
            "orderId": okx_ord_id,
            "clientOrderId": raw.get("clOrdId") or _okx_client_order_id(client_order_id),
            "status": "NEW",
            "executedQty": "0",
            "raw": raw,
        }

    def place_limit_maker_order(self, *, symbol: str, side: str, quantity: str, price: str, client_order_id: str | None = None):
        self._execution_blocked("place_limit_maker_order")

    def get_order(self, **kwargs):
        self._execution_blocked("get_order")

    def get_order_status(self, *, symbol: str, order_id):
        if not self._execution_enabled():
            self._execution_blocked("get_order_status")

        inst_id = to_exchange_symbol(symbol, "OKX")
        data = self._private_request(
            "GET",
            "/api/v5/trade/order",
            params={
                "instId": inst_id,
                "ordId": str(order_id),
            },
        )

        rows = data.get("data") or []
        raw = rows[0] if rows else {}

        state = str(raw.get("state", "")).lower()
        acc_fill_sz = raw.get("accFillSz", "0")

        status_map = {
            "live": "NEW",
            "partially_filled": "PARTIALLY_FILLED",
            "filled": "FILLED",
            "canceled": "CANCELED",
        }

        return {
            "symbol": symbol,
            "exchange_symbol": inst_id,
            "orderId": raw.get("ordId") or str(order_id),
            "clientOrderId": raw.get("clOrdId"),
            "status": status_map.get(state, state.upper() if state else "UNKNOWN"),
            "executedQty": acc_fill_sz,
            "raw": raw,
        }

    def find_order_by_client_order_id(
        self, *, symbol: str, client_order_id: str
    ) -> dict:
        """Read-only exact OKX order recovery using ``clOrdId``.

        This deliberately does not consult ``OKX_EXECUTION_ENABLED``: order
        recovery is an admission-safety read, not an execution operation.
        Empty and explicitly missing identities are distinct from transport,
        authentication, and malformed-response errors.
        """
        exchange_client_order_id = _okx_client_order_id(client_order_id)
        if exchange_client_order_id is None:
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": "INVALID_CLIENT_ORDER_ID",
                "error_message": "client_order_id is empty after OKX normalization",
            }

        inst_id = to_exchange_symbol(symbol, "OKX")
        try:
            data = self._private_request(
                "GET",
                "/api/v5/trade/order",
                params={
                    "instId": inst_id,
                    "clOrdId": exchange_client_order_id,
                },
            )
        except ExchangeAPIException as exc:
            # OKX 51603 is the exact-order lookup "order does not exist"
            # response. Treating any broader API failure as NOT_FOUND could
            # admit a duplicate order, so every other code remains ERROR.
            if str(exc.code) == "51603":
                return {
                    "outcome": "NOT_FOUND",
                    "order": None,
                    "error_code": exc.code,
                    "error_message": exc.message,
                }
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": exc.code,
                "error_message": exc.message,
            }
        except Exception as exc:
            return {
                "outcome": "ERROR",
                "order": None,
                "error_code": None,
                "error_message": str(exc),
            }

        rows = data.get("data") or []
        if not rows:
            return {
                "outcome": "NOT_FOUND",
                "order": None,
                "error_code": None,
                "error_message": None,
            }
        if len(rows) != 1 or not isinstance(rows[0], dict):
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exact client order lookup returned multiple or malformed rows",
            }

        raw = rows[0]
        returned_client_order_id = str(raw.get("clOrdId") or "")
        if returned_client_order_id != exchange_client_order_id:
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exchange client order identity mismatch",
            }

        state = str(raw.get("state", "")).lower()
        status_map = {
            "live": "NEW",
            "partially_filled": "PARTIALLY_FILLED",
            "filled": "FILLED",
            "canceled": "CANCELED",
        }
        order = {
            "symbol": symbol,
            "exchange_symbol": inst_id,
            "orderId": raw.get("ordId"),
            "clientOrderId": returned_client_order_id,
            "status": status_map.get(
                state, state.upper() if state else "UNKNOWN"
            ),
            "executedQty": raw.get("accFillSz", "0"),
            "raw": raw,
        }
        if not order["orderId"]:
            return {
                "outcome": "AMBIGUOUS",
                "order": None,
                "error_code": None,
                "error_message": "exchange order identity missing",
            }
        return {
            "outcome": "FOUND",
            "order": order,
            "error_code": None,
            "error_message": None,
        }

    def cancel_order(self, **kwargs):
        self._execution_blocked("cancel_order")

    def cancel_order_by_id(self, *, symbol: str, order_id):
        if not self._execution_enabled():
            self._execution_blocked("cancel_order_by_id")

        inst_id = to_exchange_symbol(symbol, "OKX")
        data = self._private_request(
            "POST",
            "/api/v5/trade/cancel-order",
            body={
                "instId": inst_id,
                "ordId": str(order_id),
            },
        )

        rows = data.get("data") or []
        raw = rows[0] if rows else {}

        s_code = str(raw.get("sCode", "0"))
        s_msg = raw.get("sMsg", "")

        if s_code != "0":
            raise ExchangeAPIException(
                f"OKX cancel rejected sCode={s_code} sMsg={s_msg}",
                code=s_code,
                raw=raw,
            )

        return {
            "symbol": symbol,
            "exchange_symbol": inst_id,
            "orderId": raw.get("ordId") or str(order_id),
            "clientOrderId": raw.get("clOrdId"),
            "status": "CANCELED",
            "raw": raw,
        }

    def get_my_trades(self, **kwargs):
        """
        OKX fills adapter.

        Binance-compatible input usually passes:
          symbol=BTCUSDC

        Optional:
          orderId / ordId
          after
          before
          limit
        """
        symbol = kwargs.get("symbol")
        if not symbol:
            raise ValueError("get_my_trades requires symbol")

        inst_id = to_exchange_symbol(symbol, "OKX")

        try:
            limit = int(kwargs.get("limit", 100))
        except Exception:
            limit = 100
        limit = max(1, min(limit, 100))

        params = {
            "instType": "SPOT",
            "instId": inst_id,
            "limit": limit,
        }

        order_id = kwargs.get("orderId") or kwargs.get("ordId")
        if order_id:
            params["ordId"] = str(order_id)

        if kwargs.get("after") is not None:
            params["after"] = str(kwargs.get("after"))
        if kwargs.get("before") is not None:
            params["before"] = str(kwargs.get("before"))

        data = self._private_request(
            "GET",
            "/api/v5/trade/fills-history",
            params=params,
        )

        rows = data.get("data") or []
        requested_boundary = kwargs.get("startTime")
        effective_boundary = None
        if requested_boundary is not None:
            requested_boundary = int(requested_boundary)
            correction_lookback_ms = max(
                0, int(kwargs.get("correctionLookbackMs") or 0)
            )
            effective_boundary = requested_boundary - correction_lookback_ms
            rows = [
                row for row in rows
                if row.get("ts") is not None
                and int(row["ts"]) >= effective_boundary
            ]
        out = []

        for r in rows:
            fill_sz = r.get("fillSz", "0")
            fill_px = r.get("fillPx", "0")
            fee = r.get("fee", "0")
            fee_ccy = r.get("feeCcy")
            ts = r.get("ts")

            # Binance-like normalized shape plus raw OKX.
            out.append({
                "symbol": symbol,
                "id": r.get("tradeId"),
                "orderId": r.get("ordId"),
                "clientOrderId": r.get("clOrdId") or None,
                "price": fill_px,
                "qty": fill_sz,
                "quoteQty": None,
                "commission": _exact_nonnegative_decimal(
                    fee, field="fill fee"
                ),
                "commissionAsset": fee_ccy,
                "time": int(ts) if ts else None,
                "isBuyer": str(r.get("side", "")).lower() == "buy",
                "isMaker": str(r.get("execType", "")).upper() == "M",
                "raw": r,
            })

        return FillFetchResult(
            out,
            filter_applied=requested_boundary is not None,
            filter_mode=(
                "LOCAL_EVENT_TIME_GTE"
                if requested_boundary is not None
                else "OKX_CURSOR_ONLY"
            ),
            requested_boundary=requested_boundary,
            effective_boundary=effective_boundary,
        )

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        req = Request(url, headers={"User-Agent": "waltrade-bot/okx-md"})
        with urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read().decode("utf-8")
        return json.loads(raw)

    def get_klines(self, *, symbol: str, interval: str, limit: int = 1000, start_ms: Optional[int] = None) -> List[list]:
        inst_id = to_exchange_symbol(symbol, "OKX")
        bar = self._okx_bar(interval)

        params: Dict[str, Any] = {
            "instId": inst_id,
            "bar": bar,
            "limit": min(int(limit), 100),
        }

        # OKX uses before/after pagination. For first safe adapter step, we fetch latest window.
        # catch_up still works because DB insert is ON CONFLICT DO NOTHING.
        data = self._request("/api/v5/market/history-candles", params)

        if str(data.get("code")) != "0":
            raise RuntimeError(f"OKX history-candles failed code={data.get('code')} msg={data.get('msg')}")

        rows = data.get("data") or []
        out = [self._to_binance_like_kline(k, interval) for k in rows]
        out.sort(key=lambda x: int(x[0]))
        return out


def get_market_data_client():
    ex = os.environ.get("EXCHANGE", "BINANCE").strip().upper()
    if ex == "OKX":
        logging.info("exchange_client: using OKX market data adapter")
        return OkxMarketDataAdapter()
    logging.info("exchange_client: using Binance market data adapter")
    return BinanceMarketDataAdapter()

def get_okx_spot_instrument(symbol: str) -> dict:
    """
    Public OKX instrument metadata for spot sizing.
    Returns one instrument dict for internal symbol like BTCUSDC.
    """
    base_url = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
    timeout = float(os.environ.get("OKX_HTTP_TIMEOUT_SECONDS", "10"))
    inst_id = to_exchange_symbol(symbol, "OKX")

    params = {
        "instType": "SPOT",
        "instId": inst_id,
    }
    url = f"{base_url}/api/v5/public/instruments?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "waltrade-bot/okx-instruments"})

    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")

    data = json.loads(raw)
    if str(data.get("code")) != "0":
        raise RuntimeError(f"OKX instruments failed code={data.get('code')} msg={data.get('msg')}")

    rows = data.get("data") or []
    if not rows:
        raise RuntimeError(f"OKX instruments returned no data for {inst_id}")

    return rows[0]

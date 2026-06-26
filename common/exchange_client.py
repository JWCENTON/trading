# common/exchange_client.py
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

from common.exchange_symbols import to_exchange_symbol


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
        return self.client.create_order(**kwargs)

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
        return self.client.get_order(**kwargs)

    def cancel_order(self, **kwargs):
        return self.client.cancel_order(**kwargs)

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
        rows = data.get("data") or []
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
        self._execution_blocked("get_account")

    def get_balances(self) -> dict:
        self._execution_blocked("get_balances")

    def create_order(self, **kwargs):
        self._execution_blocked("create_order")

    def place_market_order(self, *, symbol: str, side: str, quantity: str, client_order_id: str | None = None):
        self._execution_blocked("place_market_order")

    def place_limit_maker_order(self, *, symbol: str, side: str, quantity: str, price: str, client_order_id: str | None = None):
        self._execution_blocked("place_limit_maker_order")

    def get_order(self, **kwargs):
        self._execution_blocked("get_order")

    def cancel_order(self, **kwargs):
        self._execution_blocked("cancel_order")

    def get_my_trades(self, **kwargs):
        self._execution_blocked("get_my_trades")

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


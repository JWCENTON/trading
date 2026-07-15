from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

from tests.bot_bbrange.fixtures import StatefulBbrangeHarness, StrictFakeExchange


@pytest.fixture
def stateful_bbrange(monkeypatch):
    safe_env = {
        "SYMBOL": "BTCUSDC", "QUOTE_ASSET": "USDC",
        "STRATEGY_NAME": "BBRANGE", "INTERVAL": "1m",
        "TRADING_MODE": "PAPER", "DB_HOST": "invalid.test",
        "DB_NAME": "unit_test", "DB_USER": "unit_test",
        "DB_PASS": "unit_test", "EXCHANGE_PROVIDER": "OKX",
        "DAILY_MAX_LOSS_PCT": "0", "DISABLE_HOURS": "",
    }
    for key, value in safe_env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    exchange_client = importlib.import_module("common.exchange_client")
    fake = StrictFakeExchange()
    monkeypatch.setattr(exchange_client, "get_market_data_client", lambda: fake)
    name = "waltrade_bot_bbrange_stateful"
    sys.modules.pop(name, None)
    source = Path(__file__).resolve().parents[2] / "bot_bbrange" / "main.py"
    spec = importlib.util.spec_from_file_location(name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return StatefulBbrangeHarness(module, monkeypatch)

from __future__ import annotations

import importlib
import importlib.util
import socket
import sys
import time
from pathlib import Path

import psycopg2
import pytest
import requests
import urllib3.connection


ROOT = Path(__file__).resolve().parents[1]
STRATEGIES = (
    ("RSI", ROOT / "bot" / "main.py", "RSI exchange client initialization failed"),
    ("TREND", ROOT / "bot_trend" / "main.py", "TREND exchange client initialization failed"),
    (
        "SUPERTREND",
        ROOT / "bot_supertrend" / "main.py",
        "SUPERTREND exchange client initialization failed",
    ),
)


def _fresh_import(monkeypatch, label, source, factory):
    module_name = f"waltrade_{label.lower()}_main_import_safety"
    sys.modules.pop(module_name, None)
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.delenv("OKX_API_KEY", raising=False)
    monkeypatch.delenv("OKX_API_SECRET", raising=False)
    monkeypatch.delenv("OKX_PASSPHRASE", raising=False)

    blocked = lambda *_args, **_kwargs: pytest.fail("I/O during import")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    monkeypatch.setattr(urllib3.connection.HTTPConnection, "connect", blocked)
    monkeypatch.setattr(urllib3.connection.HTTPSConnection, "connect", blocked)
    monkeypatch.setattr(psycopg2, "connect", blocked)
    monkeypatch.setattr(time, "sleep", blocked)

    exchange_client = importlib.import_module("common.exchange_client")
    monkeypatch.setattr(exchange_client, "get_market_data_client", factory)

    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(("label", "source", "_error_log"), STRATEGIES)
def test_strategy_import_performs_no_io_or_factory_call(
    monkeypatch, label, source, _error_log
):
    calls = []
    module = _fresh_import(
        monkeypatch,
        label,
        source,
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    assert calls == []
    assert module._exchange_client is None


@pytest.mark.parametrize(("label", "source", "_error_log"), STRATEGIES)
def test_strategy_client_is_cached_and_factory_receives_no_arguments(
    monkeypatch, label, source, _error_log
):
    fake = object()
    calls = []

    def factory(*args, **kwargs):
        calls.append((args, kwargs))
        return fake

    module = _fresh_import(monkeypatch, label, source, factory)

    assert module.get_exchange_client() is fake
    assert module.get_exchange_client() is fake
    assert calls == [((), {})]


@pytest.mark.parametrize(("label", "source", "error_log"), STRATEGIES)
def test_strategy_factory_failure_is_logged_propagated_and_retryable(
    monkeypatch, caplog, label, source, error_log
):
    fake = object()
    calls = []

    def factory():
        calls.append("factory")
        if len(calls) == 1:
            raise RuntimeError("factory unavailable")
        return fake

    module = _fresh_import(monkeypatch, label, source, factory)

    with pytest.raises(RuntimeError, match="factory unavailable"):
        module.get_exchange_client()
    assert module._exchange_client is None
    assert error_log in caplog.text
    assert module.get_exchange_client() is fake
    assert calls == ["factory", "factory"]


@pytest.mark.parametrize(("label", "source", "_error_log"), STRATEGIES)
def test_main_loop_initializes_client_before_runtime_database_setup(
    monkeypatch, label, source, _error_log
):
    fake = object()
    module = _fresh_import(monkeypatch, label, source, lambda: fake)
    operations = []

    class StartupStop(Exception):
        pass

    def runtime_client():
        operations.append("client")
        return fake

    def runtime_database(*_args, **_kwargs):
        operations.append("runtime_db")
        raise StartupStop

    monkeypatch.setattr(module, "get_exchange_client", runtime_client)
    monkeypatch.setattr(module, "upsert_defaults", runtime_database)

    with pytest.raises(StartupStop):
        module.main_loop()
    assert operations == ["client", "runtime_db"]

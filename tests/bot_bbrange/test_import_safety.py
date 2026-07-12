from __future__ import annotations

import importlib
import socket
import sys
import time
from pathlib import Path

import psycopg2
import pytest
import requests
import urllib3.connection


def _fresh_import(monkeypatch):
    module_name = "waltrade_bot_bbrange_main_import_safety"
    sys.modules.pop(module_name, None)
    blocked = lambda *_args, **_kwargs: pytest.fail("I/O during import")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(
        requests.sessions.Session,
        "request",
        blocked,
    )
    monkeypatch.setattr(urllib3.connection.HTTPConnection, "connect", blocked)
    monkeypatch.setattr(
        psycopg2,
        "connect",
        blocked,
    )
    monkeypatch.setattr(time, "sleep", blocked)
    source = Path(__file__).resolve().parents[2] / "bot_bbrange" / "main.py"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_import_performs_no_network_or_database_io(monkeypatch):
    module = _fresh_import(monkeypatch)

    assert module._exchange_client is None


def test_exchange_client_is_created_once_on_first_runtime_use(monkeypatch):
    module = _fresh_import(monkeypatch)
    fake = object()
    calls = []
    monkeypatch.setattr(
        module,
        "get_market_data_client",
        lambda: calls.append("factory") or fake,
    )

    assert calls == []
    assert module.get_exchange_client() is fake
    assert module.get_exchange_client() is fake
    assert calls == ["factory"]


def test_exchange_client_initialization_failure_is_deferred(monkeypatch, caplog):
    module = _fresh_import(monkeypatch)

    def fail_factory():
        raise RuntimeError("factory unavailable")

    monkeypatch.setattr(module, "get_market_data_client", fail_factory)

    with pytest.raises(RuntimeError, match="factory unavailable"):
        module.get_exchange_client()
    assert module._exchange_client is None
    assert "BBRANGE exchange client initialization failed" in caplog.text

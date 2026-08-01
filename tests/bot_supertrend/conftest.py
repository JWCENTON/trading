from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.bot_supertrend.fixtures import SupertrendHarness


@pytest.fixture
def supertrend(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    name = "waltrade_bot_supertrend_stateful"
    sys.modules.pop(name, None)
    source = Path(__file__).resolve().parents[2] / "bot_supertrend" / "main.py"
    spec = importlib.util.spec_from_file_location(name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    def allow_preflight(*_args, action, **_kwargs):
        row = module.get_open_position()
        return action(SimpleNamespace(position_id=int(row[0]) if row else 77))

    monkeypatch.setattr(
        module, "execute_paper_exit_after_preflight", allow_preflight,
    )
    monkeypatch.setattr(
        module, "paper_supertrend_entries_enabled", lambda *_a, **_k: (True, None)
    )
    monkeypatch.setattr(
        module, "expire_paper_supertrend_slot_canaries", lambda *_a, **_k: 0
    )
    return module


@pytest.fixture
def harness(supertrend, monkeypatch):
    monkeypatch.setattr(
        threading.Thread, "start",
        lambda *_a, **_k: pytest.fail("background thread started"),
    )
    return SupertrendHarness(supertrend, monkeypatch)

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.entry_authority import entry_authority, exit_only_active
from common.simulated_execution_evidence import record_forward_paper_entry_atomic


ROOT = Path(__file__).resolve().parents[1]
STRATEGY_FILES = (
    "bot/main.py",
    "bot_trend/main.py",
    "bot_supertrend/main.py",
    "bot_bbrange/main.py",
)


def test_exit_only_blocks_entry_and_preserves_exit():
    env = {"WALTRADE_EXIT_ONLY": "1"}
    assert exit_only_active(env) is True
    assert entry_authority(is_exit=False, environment=env) == (
        False, "EXIT_ONLY_ENTRY_BLOCKED"
    )
    assert entry_authority(is_exit=True, environment=env) == (
        True, "EXIT_AUTHORITY_PRESERVED"
    )


def test_atomic_paper_entry_fails_before_connection_or_writes(monkeypatch):
    monkeypatch.setenv("WALTRADE_EXIT_ONLY", "1")
    calls = []

    def forbidden_connection():
        calls.append("connection")
        raise AssertionError("entry transaction must not start")

    result = record_forward_paper_entry_atomic(
        forbidden_connection,
        client=None,
        symbol="BTCUSDC",
        interval="1m",
        strategy="RSI",
        side="BUY",
        price=Decimal("100"),
        quantity=Decimal("0.1"),
        reason="TEST",
        candle_open_time=datetime.now(timezone.utc),
        deployment_id="local-paper",
        market_regime="TREND_UP",
        regime_source_provenance={},
    )
    assert not result
    assert result.status == "EXIT_ONLY_ENTRY_BLOCKED"
    assert result.simulated_order_id is None
    assert result.position_id is None
    assert calls == []


@pytest.mark.parametrize("relative_path", STRATEGY_FILES)
def test_all_four_strategy_paper_entries_use_shared_atomic_fence(relative_path):
    source = (ROOT / relative_path).read_text()
    assert "record_forward_paper_entry_atomic(" in source
    assert "execute_paper_exit_after_preflight(" in source


def test_live_default_is_unchanged(monkeypatch):
    monkeypatch.delenv("WALTRADE_EXIT_ONLY", raising=False)
    assert exit_only_active() is False
    assert entry_authority(is_exit=False) == (
        True, "ENTRY_AUTHORITY_AVAILABLE"
    )

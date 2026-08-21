from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.bot_supertrend.fixtures import candle


def paper_config():
    return SimpleNamespace(
        symbol="BTCUSDC",
        interval="1m",
        trading_mode="PAPER",
        live_orders_enabled=False,
        quote_asset="USDC",
    )


def execute(supertrend, *, is_exit=False):
    return supertrend.execute_and_record(
        side="SELL" if is_exit else "BUY",
        price=101.25 if is_exit else 100.0,
        qty_btc=0.125,
        reason="test-exit" if is_exit else "test-entry",
        candle_open_time=candle()[0],
        is_exit=is_exit,
        cfg_used=paper_config(),
        allow_live_orders=False,
        allow_meta={},
    )


def install_paper_boundaries(
    supertrend,
    monkeypatch,
    *,
    inserted=501,
    position_id=77,
    evidence_error=None,
):
    operations = []
    evidence_calls = []
    open_calls = []
    monkeypatch.setattr(
        supertrend,
        "insert_simulated_order",
        lambda **_kwargs: operations.append("simulated-order") or inserted,
    )
    monkeypatch.setattr(
        supertrend,
        "emit_strategy_event",
        lambda **event: operations.append(f"event:{event['event_type']}"),
    )

    def open_position(*args, **kwargs):
        open_calls.append((args, kwargs))
        operations.append("position-open")
        return position_id

    monkeypatch.setattr(supertrend, "open_position", open_position)
    monkeypatch.setattr(
        supertrend,
        "get_open_position",
        lambda: (
            None if position_id is None
            else (position_id, "LONG", 0.125, 100.0, candle()[0])
        ),
    )
    monkeypatch.setattr(supertrend, "get_exchange_client", lambda: object())
    monkeypatch.setattr(
        supertrend, "paper_supertrend_entries_enabled", lambda *_a, **_k: (True, None)
    )
    monkeypatch.setattr(supertrend, "persist_exit_intent", lambda *_a, **_k: "fixture")
    monkeypatch.setattr(
        supertrend,
        "reconcile_terminal_compatibility_outcome",
        lambda *_a, **_k: SimpleNamespace(applied=True, reason="RECONCILED"),
    )

    def evidence(*args, **kwargs):
        evidence_calls.append((args, kwargs))
        operations.append("evidence")
        if evidence_error:
            raise evidence_error
        return True

    monkeypatch.setattr(supertrend, "record_simulated_fill_evidence", evidence)
    return operations, open_calls, evidence_calls


def test_paper_entry_creates_direct_position_and_evidence(
    supertrend, monkeypatch
):
    operations, open_calls, evidence_calls = install_paper_boundaries(
        supertrend, monkeypatch
    )

    result = execute(supertrend)

    assert result["ledger_ok"] is True
    assert result["paper_executed"] is True
    assert result["position_id"] == 77
    assert result["simulated_order_id"] == 501
    assert len(open_calls) == 1
    args, kwargs = open_calls[0]
    assert args == ("LONG", 0.125, 100.0, None)
    assert kwargs["entry_time"] == candle()[0]
    assert len(evidence_calls) == 1
    assert evidence_calls[0][1]["position_id"] == 77
    assert evidence_calls[0][1]["simulated_order_id"] == 501
    assert evidence_calls[0][1]["environment"] == "paper"
    assert operations.index("simulated-order") < operations.index("position-open")
    assert operations.index("position-open") < operations.index("evidence")


def test_paper_duplicate_order_never_mutates_or_records_evidence(
    supertrend, monkeypatch
):
    operations, open_calls, evidence_calls = install_paper_boundaries(
        supertrend, monkeypatch, inserted=None
    )

    result = execute(supertrend)

    assert result["ledger_ok"] is False
    assert result["blocked_reason"] == "DB_GUARD_DUPLICATE"
    assert open_calls == []
    assert evidence_calls == []
    assert operations == ["simulated-order", "event:BLOCKED"]


def test_persisted_open_position_blocks_fresh_paper_writer(
    supertrend, monkeypatch
):
    _, open_calls, evidence_calls = install_paper_boundaries(
        supertrend, monkeypatch, position_id=None
    )

    result = execute(supertrend)

    assert result["ledger_ok"] is False
    assert result["paper_executed"] is False
    assert result["blocked_reason"] == "PAPER_POSITION_OPEN_FAILED"
    assert len(open_calls) == 1
    assert evidence_calls == []


def test_paper_exit_evidence_uses_open_position_before_caller_close(
    supertrend, monkeypatch
):
    operations, open_calls, evidence_calls = install_paper_boundaries(
        supertrend, monkeypatch
    )

    result = execute(supertrend, is_exit=True)
    operations.append("caller-close")

    assert open_calls == []
    assert result["position_id"] == 77
    assert result["simulated_order_id"] == 501
    assert evidence_calls[0][1]["position_id"] == 77
    assert evidence_calls[0][1]["simulated_order_id"] == 501
    assert evidence_calls[0][1]["exit_reason"] == "test-exit"
    assert evidence_calls[0][1]["require_terminal_close"] is True
    assert operations.index("evidence") < operations.index("caller-close")


@pytest.mark.parametrize("is_exit", [False, True])
def test_paper_evidence_failure_is_fail_open(
    supertrend, monkeypatch, caplog, is_exit
):
    _, _, evidence_calls = install_paper_boundaries(
        supertrend,
        monkeypatch,
        evidence_error=RuntimeError("evidence unavailable"),
    )

    result = execute(supertrend, is_exit=is_exit)

    assert len(evidence_calls) == 1
    assert result["ledger_ok"] is True
    assert result["paper_executed"] is True
    assert result["position_id"] == 77
    assert "paper persistence unavailable" in caplog.text


def test_live_suppression_never_calls_paper_position_or_evidence(
    supertrend, monkeypatch
):
    _, open_calls, evidence_calls = install_paper_boundaries(
        supertrend, monkeypatch
    )
    config = paper_config()
    config.trading_mode = "LIVE"

    result = supertrend.execute_and_record(
        side="BUY",
        price=100.0,
        qty_btc=0.125,
        reason="live-suppressed",
        candle_open_time=candle()[0],
        is_exit=False,
        cfg_used=config,
        allow_live_orders=False,
        allow_meta={},
    )

    assert result["blocked_reason"] == "LIVE_ORDER_SUPPRESSED"
    assert open_calls == []
    assert evidence_calls == []


def test_exit_evidence_failure_still_reaches_existing_close_path(
    harness, monkeypatch, caplog
):
    harness.set_position(price=100.0)
    module = harness.module
    production = harness.production_execute_and_record
    monkeypatch.setattr(module, "execute_and_record", production)
    monkeypatch.setattr(module, "insert_simulated_order", lambda **_kwargs: 501)
    monkeypatch.setattr(
        module,
        "record_simulated_fill_evidence",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("evidence unavailable")
        ),
    )
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())

    observed = harness.strategy_cycle(
        candle(price=102.0), candle(minute=-1)
    )

    assert observed.position is None
    assert sum(item[0] == "CLOSE" for item in observed.mutations) == 1
    assert "paper persistence unavailable" in caplog.text


@pytest.mark.parametrize("close_behavior", [False, RuntimeError("db unavailable")])
def test_paper_exit_close_failure_is_not_success(
    harness, monkeypatch, close_behavior, caplog
):
    harness.set_position(price=100.0)
    module = harness.module
    monkeypatch.setattr(module, "execute_and_record", harness.production_execute_and_record)
    monkeypatch.setattr(module, "insert_simulated_order", lambda **_kwargs: 501)
    monkeypatch.setattr(
        module, "record_simulated_fill_evidence", lambda *_args, **_kwargs: True
    )
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())

    def close(*_args, **_kwargs):
        if isinstance(close_behavior, Exception):
            raise close_behavior
        return close_behavior

    monkeypatch.setattr(module, "close_position", close)
    observed = harness.strategy_cycle(candle(price=102.0), candle(minute=-1))

    assert observed.final_decision.action != "EXIT"
    assert observed.final_decision.reason_text == "POSITION_CLOSE_FAILED"
    assert harness.position is not None
    assert not any(event.get("event_type") == "POSITION_CLOSED" for event in observed.events)
    assert "POSITION_CLOSE_FAILED" in caplog.text


def test_two_exit_close_attempts_have_one_success_and_one_stale_failure(
    supertrend, monkeypatch
):
    state = {"open": True, "calls": 0}
    events = []

    def conditional_close(**kwargs):
        assert kwargs["expected_position_id"] == 77
        state["calls"] += 1
        if not state["open"]:
            return False
        state["open"] = False
        events.append("POSITION_CLOSED")
        return True

    monkeypatch.setattr(supertrend, "close_position", conditional_close)
    monkeypatch.setattr(
        supertrend, "emit_strategy_event",
        lambda **event: events.append(event["event_type"]),
    )
    base = {
        "ledger_ok": True,
        "position_id": 77,
        "simulated_order_id": 501,
    }

    first = supertrend._close_supertrend_exit(
        base, exit_price=101.0, reason="test", candle_open_time=candle()[0]
    )
    second = supertrend._close_supertrend_exit(
        {**base, "simulated_order_id": 502},
        exit_price=101.0, reason="test", candle_open_time=candle()[0],
    )

    assert first["position_close_succeeded"] is True
    assert second["position_close_succeeded"] is False
    assert second["blocked_reason"] == "POSITION_CLOSE_FAILED"
    assert state["calls"] == 2
    assert events.count("POSITION_CLOSED") == 1
    assert events.count("POSITION_CLOSE_FAILED") == 1


def test_unknown_mode_fails_before_any_execution_mutation(supertrend, monkeypatch):
    calls = []
    monkeypatch.setattr(
        supertrend, "insert_simulated_order",
        lambda **_kwargs: calls.append("simulated-order"),
    )
    config = paper_config()
    config.trading_mode = "PPAER"

    result = supertrend.execute_and_record(
        side="BUY", price=100.0, qty_btc=0.125, reason="invalid-mode",
        candle_open_time=candle()[0], is_exit=False, cfg_used=config,
        allow_live_orders=False, allow_meta={},
    )

    assert result["blocked_reason"] == "INVALID_TRADING_MODE"
    assert calls == []

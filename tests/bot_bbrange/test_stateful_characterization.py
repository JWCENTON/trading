from __future__ import annotations

import pytest

from tests.bot_bbrange.fixtures import band_frame, candle, runtime_snapshot


def reasons(cycle):
    return [event.payload.get("reason") for event in cycle.events]


def kinds(cycle):
    return [operation.kind for operation in cycle.operations]


def test_lifecycle_no_signal_and_invalid_data(stateful_bbrange):
    no_signal = stateful_bbrange.cycle(candle())
    assert [event.payload["event_type"] for event in no_signal.events][0] == "RUN_START"
    assert reasons(no_signal)[-2:] == ["NO_SIGNAL", "DONE"]
    invalid = stateful_bbrange.cycle(candle(ema=None))
    assert reasons(invalid) == ["ENTER", "INDICATORS_NOT_READY", "DONE"]
    assert not [x for x in invalid.operations if x.kind == "execution"]


@pytest.mark.parametrize("control, reason", [("halt", "BOT_MODE_HALT"),
                                               ("disabled", "BOT_DISABLED"),
                                               ("regime", "REGIME_BLOCK")])
def test_control_and_regime_blocks(stateful_bbrange, control, reason):
    h = stateful_bbrange
    if control == "halt":
        h.mode = "HALT"
    elif control == "disabled":
        h.enabled = False
    else:
        h.regime_allow = False
    observed = h.cycle(candle(close=90.0, low=89.0))
    assert reason in reasons(observed)
    assert "execution" not in kinds(observed)


def test_insufficient_candles_and_invalid_bands(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    monkeypatch.setattr(h.module.pd, "read_sql_query",
                        lambda *_a, **_k: band_frame().iloc[:2])
    assert "NOT_ENOUGH_CANDLES" in reasons(h.cycle(candle(close=90.0, low=89.0)))
    frame = band_frame().copy()
    frame["close"] = float("nan")
    monkeypatch.setattr(h.module.pd, "read_sql_query", lambda *_a, **_k: frame)
    assert "BB_NOT_READY" in reasons(h.cycle(candle(close=90.0, low=89.0)))


@pytest.mark.parametrize(
    "mode,result,opens",
    [
        ("PAPER", None, True),
        ("LIVE", None, True),
        ("LIVE", {"ledger_ok": True, "live_attempted": False,
                  "live_ok": False, "blocked_reason": "LIVE_ORDER_SUPPRESSED"}, False),
        ("LIVE", {"ledger_ok": True, "live_attempted": True,
                  "live_ok": False, "blocked_reason": "REJECTED"}, False),
        ("PAPER", {"ledger_ok": False, "live_attempted": False,
                   "live_ok": False, "blocked_reason": "DB_GUARD_DUPLICATE"}, False),
    ],
)
def test_entry_outcomes_and_exactly_once(stateful_bbrange, mode, result, opens):
    h = stateful_bbrange
    h.trading_mode = mode
    h.execution_result = result
    observed = h.cycle(candle(close=90.0, low=89.0))
    assert kinds(observed).count("execution") == 1
    assert kinds(observed).count("position_open") == (1 if opens else 0)
    assert (observed.position is not None) is opens


def test_entry_hold_take_profit_stateful_sequence(stateful_bbrange):
    h = stateful_bbrange
    entry = h.cycle(candle(close=90.0, low=89.0))
    hold = h.cycle(candle(close=90.2, high=90.4, low=90.0))
    exited = h.cycle(candle(close=91.5, high=92.0, low=91.0))
    assert entry.position is not None
    assert hold.position is not None and "POSITION_OPEN_NO_EXIT" in reasons(hold)
    assert kinds(hold).count("execution") == 0
    assert exited.position is None
    assert kinds(exited).count("execution") == 1
    assert kinds(exited).count("position_close") == 1


def test_stop_loss_confirmed_close_exactly_once(stateful_bbrange):
    h = stateful_bbrange
    h.set_position()
    observed = h.cycle(candle(close=99.0, high=100.0, low=98.0))
    assert observed.position is None
    assert kinds(observed).count("execution") == 1
    assert kinds(observed).count("position_close") == 1
    assert next(x for x in observed.operations if x.kind == "position_close").payload["reason"] == "STOP_LOSS"


def test_profit_lock_armed_state_persists_then_giveback_exits(stateful_bbrange):
    h = stateful_bbrange
    h.set_position()
    h.profit_lock_state = "ARMED"
    armed = h.cycle(candle(close=100.2, high=100.4, low=100.0))
    assert armed.position is not None
    assert next(x for x in armed.operations
                if x.kind == "profit_lock_event").payload["reason"] == "ARMED_WAITING"
    h.profit_lock_state = "TRIGGERED"
    exited = h.cycle(candle(close=100.1, high=100.3, low=99.9))
    assert exited.position is None
    assert kinds(exited).count("position_close") == 1


def test_block_unblock_signal_and_suppressed_retry(stateful_bbrange):
    h = stateful_bbrange
    h.regime_allow = False
    blocked = h.cycle(candle(close=90.0, low=89.0))
    h.regime_allow = True
    h.trading_mode = "LIVE"
    h.execution_result = {"ledger_ok": True, "live_attempted": False,
                          "live_ok": False, "blocked_reason": "LIVE_ORDER_SUPPRESSED"}
    suppressed = h.cycle(candle(close=90.0, low=89.0))
    h.execution_result = None
    entered = h.cycle(candle(close=90.0, low=89.0))
    assert "REGIME_BLOCK" in reasons(blocked)
    assert suppressed.position is None
    assert entered.position is not None


def test_panic_exit_and_error_then_recovery(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    h.set_position()
    h.mode = "PANIC"
    panic = h.cycle(candle())
    assert panic.position is None
    assert kinds(panic).count("execution") == 1
    h.mode = "NORMAL"
    original = h.module.get_runtime_snapshot
    monkeypatch.setattr(h.module, "get_runtime_snapshot",
                        lambda **_k: (_ for _ in ()).throw(RuntimeError("fixture")))
    with pytest.raises(RuntimeError, match="fixture"):
        h.cycle(candle())
    monkeypatch.setattr(h.module, "get_runtime_snapshot", original)
    recovered = h.cycle(candle())
    assert "NO_SIGNAL" in reasons(recovered)

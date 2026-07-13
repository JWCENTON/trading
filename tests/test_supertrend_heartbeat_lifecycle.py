from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def module():
    return importlib.import_module("bot_supertrend.main")


def _record_lifecycle(monkeypatch, module, times):
    effects = []
    ticks = iter(times)
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(
        module,
        "lifecycle_heartbeat",
        lambda status, **details: effects.append(("heartbeat", status, details)),
    )
    return effects


def test_cycle_orders_start_runtime_run_end_and_end_heartbeat(monkeypatch, module):
    effects = _record_lifecycle(monkeypatch, module, [10.0, 12.5])

    def iteration(_client, last_ingest_ts, **_kwargs):
        effects.extend([("runtime",), ("RUN_END",)])
        return last_ingest_ts + 1

    monkeypatch.setattr(module, "run_loop_iteration", iteration)

    assert module.run_loop_cycle(object(), 4.0) == 5.0
    assert [item[:2] for item in effects] == [
        ("heartbeat", "RUNNING"),
        ("runtime",),
        ("RUN_END",),
        ("heartbeat", "CYCLE_OK"),
    ]
    assert effects[-1][2]["duration_s"] == 2.5
    assert effects[-1][2]["error"] is None


@pytest.mark.parametrize("result", ["NO_SIGNAL", "BLOCKED"])
def test_non_trading_cycle_still_ends_with_fresh_heartbeat(
    monkeypatch, module, result
):
    effects = _record_lifecycle(monkeypatch, module, [20.0, 21.0])

    def iteration(_client, last_ingest_ts, **_kwargs):
        effects.append((result,))
        return last_ingest_ts

    monkeypatch.setattr(module, "run_loop_iteration", iteration)

    module.run_loop_cycle(object(), 7.0)
    assert effects[0][:2] == ("heartbeat", "RUNNING")
    assert effects[-1][:2] == ("heartbeat", "CYCLE_OK")


def test_exception_records_error_heartbeat_without_escaping(monkeypatch, module):
    effects = _record_lifecycle(monkeypatch, module, [30.0, 34.0])
    events = []

    def iteration(_client, _last_ingest_ts, **_kwargs):
        raise RuntimeError("indicator failure")

    monkeypatch.setattr(module, "run_loop_iteration", iteration)
    monkeypatch.setattr(module, "emit_strategy_event", lambda **event: events.append(event))

    assert module.run_loop_cycle(object(), 9.0) == 9.0
    assert effects[-1][0:2] == ("heartbeat", "ERROR")
    assert isinstance(effects[-1][2]["error"], RuntimeError)
    assert len(events) == 1
    assert events[0]["event_type"] == "ERROR"
    assert events[0]["reason"] == "EXCEPTION"


def test_363_second_completed_cycle_is_fresh_at_end(monkeypatch, module):
    heartbeat_times = []
    clock = {"now": 0.0}
    ticks = iter([0.0, 363.0])
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))

    def heartbeat(status, **_details):
        heartbeat_times.append((status, clock["now"]))

    def iteration(_client, last_ingest_ts, **_kwargs):
        clock["now"] = 363.0
        return last_ingest_ts

    monkeypatch.setattr(module, "lifecycle_heartbeat", heartbeat)
    monkeypatch.setattr(module, "run_loop_iteration", iteration)

    module.run_loop_cycle(object(), 0.0)
    last_tick = heartbeat_times[-1][1]
    assert heartbeat_times[-1][0] == "CYCLE_OK"
    assert 363.0 - last_tick < 360.0


def test_no_progress_past_threshold_is_stale_without_periodic_watchdog(
    monkeypatch, module
):
    heartbeat_times = []
    clock = {"now": 0.0}
    ticks = iter([0.0, 361.0])
    monkeypatch.setattr(module.time, "perf_counter", lambda: next(ticks))

    def heartbeat(status, **_details):
        heartbeat_times.append((status, clock["now"]))

    def blocked_iteration(_client, _last_ingest_ts, **_kwargs):
        clock["now"] = 361.0
        assert heartbeat_times == [("RUNNING", 0.0)]
        assert clock["now"] - heartbeat_times[-1][1] > 360.0
        raise RuntimeError("release simulated blocked call")

    monkeypatch.setattr(module, "lifecycle_heartbeat", heartbeat)
    monkeypatch.setattr(module, "run_loop_iteration", blocked_iteration)
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_event: None)

    module.run_loop_cycle(object(), 0.0)


def test_lifecycle_heartbeat_is_fail_open(monkeypatch, module, caplog):
    monkeypatch.setattr(
        module, "get_db_conn", lambda: (_ for _ in ()).throw(RuntimeError("db down"))
    )

    module.lifecycle_heartbeat("RUNNING")

    assert "SUPERTREND lifecycle heartbeat failed" in caplog.text

from __future__ import annotations

import importlib

import pandas as pd
import pytest


@pytest.fixture
def module(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    return importlib.import_module("bot_supertrend.main")


def _fixture_frame(rows=80):
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    data = []
    for index in range(rows):
        close = 100.0 + (index * 0.03) + ((index % 7) * 0.01)
        data.append(
            {
                "id": index + 1,
                "open_time": base + pd.Timedelta(minutes=index),
                "open": close - 0.02,
                "high": close + 0.08,
                "low": close - 0.09,
                "close": close,
            }
        )
    return pd.DataFrame(data)


class _Cursor:
    def __init__(self):
        self.rows = None
        self.sql = None

    def executemany(self, sql, rows):
        self.sql = sql
        self.rows = list(rows)

    def execute(self, sql, _params=None):
        self.state_sql = sql

    def fetchone(self):
        return None

    def close(self):
        pass


class _Connection:
    def __init__(self):
        self.cursor_obj = _Cursor()
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def set_session(self, *, readonly):
        assert readonly is True

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _run_update(monkeypatch, module, frame, callback=None):
    conn = _Connection()
    monkeypatch.setattr(module, "get_db_conn", lambda: conn)
    monkeypatch.setattr(
        module.pd,
        "read_sql_query",
        lambda *_args, **_kwargs: frame.copy(deep=True),
    )
    target = module.update_indicators(progress_callback=callback)
    return conn.cursor_obj.rows, target


def test_none_callback_is_not_required(monkeypatch, module):
    rows, _target = _run_update(monkeypatch, module, _fixture_frame(), callback=None)
    assert len(rows) == 50


def test_callback_receives_phases_and_is_not_called_per_row(monkeypatch, module):
    calls = []
    frame = _fixture_frame(120)
    rows, _target = _run_update(
        monkeypatch,
        module,
        frame,
        callback=lambda *progress: calls.append(progress),
    )

    phases = [phase for phase, _processed, _total in calls]
    assert phases[:4] == ["LOAD_HISTORY", "EMA", "RSI", "ATR"]
    assert "SUPERTREND_LOOP" in phases
    assert phases[-1] == "PERSIST_LATEST"
    assert len(calls) < len(frame) / 10
    assert len(rows) == 50


def test_indicator_outputs_are_identical_with_and_without_callback(
    monkeypatch, module
):
    frame = _fixture_frame(180)
    baseline, baseline_target = _run_update(
        monkeypatch, module, frame, callback=None,
    )
    calls = []
    with_progress, progress_target = _run_update(
        monkeypatch,
        module,
        frame,
        callback=lambda *progress: calls.append(progress),
    )

    assert with_progress == baseline
    assert [row[-1] for row in with_progress] == [row[-1] for row in baseline]
    assert len(with_progress) == len(baseline) == 50
    assert calls
    assert progress_target == baseline_target


def test_indicator_snapshot_freezes_second_last_candle_as_cycle_target(
    monkeypatch, module,
):
    frame = _fixture_frame(80)
    _rows, target = _run_update(monkeypatch, module, frame)
    assert target == frame.iloc[-2]["open_time"].to_pydatetime()


def test_progress_heartbeat_is_time_gated_and_tracks_real_growth(
    monkeypatch, module
):
    clock = {"now": 0.0}
    writes = []
    monkeypatch.setattr(
        module,
        "lifecycle_heartbeat",
        lambda status, **meta: writes.append((clock["now"], status, meta)),
    )
    reporter = module.IndicatorProgressHeartbeat(
        cycle_started_at="cycle-1",
        interval_s=90.0,
        monotonic=lambda: clock["now"],
    )

    reporter("LOAD_HISTORY", 1000, 20000)
    clock["now"] = 89.0
    reporter("SUPERTREND_LOOP", 5000, 20000)
    clock["now"] = 90.0
    reporter("SUPERTREND_LOOP", 10000, 20000)
    clock["now"] = 120.0
    reporter("SUPERTREND_LOOP", 15000, 20000)
    clock["now"] = 180.0
    reporter("SUPERTREND_LOOP", 20000, 20000)

    assert [item[0] for item in writes] == [90.0, 180.0]
    assert all(item[1] == "INDICATOR_PROGRESS" for item in writes)
    assert [item[2]["processed_rows"] for item in writes] == [10000, 20000]


def test_phase_transition_can_emit_after_time_gate(monkeypatch, module):
    clock = {"now": 0.0}
    writes = []
    monkeypatch.setattr(
        module,
        "lifecycle_heartbeat",
        lambda status, **meta: writes.append((status, meta)),
    )
    reporter = module.IndicatorProgressHeartbeat(
        cycle_started_at="cycle-2",
        interval_s=90.0,
        monotonic=lambda: clock["now"],
    )
    reporter("EMA", 100, 100)
    clock["now"] = 90.0
    reporter("RSI", 100, 100)

    assert writes[0][1]["phase"] == "RSI"


def test_no_growth_emits_no_new_progress_and_can_become_stale(
    monkeypatch, module
):
    clock = {"now": 0.0}
    writes = []
    monkeypatch.setattr(
        module,
        "lifecycle_heartbeat",
        lambda status, **meta: writes.append((status, meta)),
    )
    reporter = module.IndicatorProgressHeartbeat(
        cycle_started_at="cycle-3",
        interval_s=90.0,
        monotonic=lambda: clock["now"],
    )
    reporter("SUPERTREND_LOOP", 5000, 20000)
    for now in (90.0, 180.0, 270.0, 361.0):
        clock["now"] = now
        reporter("SUPERTREND_LOOP", 5000, 20000)

    assert writes == []
    assert clock["now"] > 360.0


def test_459_second_real_progress_never_exceeds_stale_threshold(
    monkeypatch, module
):
    clock = {"now": 0.0}
    ticks = [(0.0, "RUNNING")]
    monkeypatch.setattr(
        module,
        "lifecycle_heartbeat",
        lambda status, **_meta: ticks.append((clock["now"], status)),
    )
    reporter = module.IndicatorProgressHeartbeat(
        cycle_started_at="cycle-4",
        interval_s=90.0,
        monotonic=lambda: clock["now"],
    )

    for now, processed in (
        (30.0, 5000),
        (90.0, 10000),
        (180.0, 20000),
        (270.0, 30000),
        (360.0, 40000),
    ):
        clock["now"] = now
        reporter("SUPERTREND_LOOP", processed, 50000)
    clock["now"] = 459.0
    ticks.append((clock["now"], "CYCLE_OK"))

    assert [status for _when, status in ticks] == [
        "RUNNING",
        "INDICATOR_PROGRESS",
        "INDICATOR_PROGRESS",
        "INDICATOR_PROGRESS",
        "INDICATOR_PROGRESS",
        "CYCLE_OK",
    ]
    assert max(b - a for (a, _), (b, _) in zip(ticks, ticks[1:])) < 360.0


def test_callback_failure_is_logged_once_and_indicator_result_is_unchanged(
    monkeypatch, module, caplog
):
    frame = _fixture_frame(100)
    baseline = _run_update(monkeypatch, module, frame, callback=None)
    calls = []

    def broken_callback(*progress):
        calls.append(progress)
        raise RuntimeError("heartbeat DB unavailable")

    actual = _run_update(monkeypatch, module, frame, callback=broken_callback)

    assert actual == baseline
    assert len(calls) == 1
    assert caplog.text.count("SUPERTREND indicator progress callback failed") == 1

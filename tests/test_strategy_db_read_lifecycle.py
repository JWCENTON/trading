from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from common import db as db_module
from common.db import db_write_conn, read_only_db_conn


ROOT = Path(__file__).resolve().parents[1]


class RecordingCursor:
    def __init__(self, role, events, *, execute_error=None, close_error=None):
        self.role = role
        self.events = events
        self.execute_error = execute_error
        self.close_error = close_error
        self.rows = []
        self.sql = None

    def executemany(self, sql, rows):
        self.events.append(f"{self.role}_execute")
        self.sql = sql
        self.rows = list(rows)
        if self.execute_error:
            raise self.execute_error

    def close(self):
        self.events.append(f"{self.role}_cursor_close")
        if self.close_error:
            raise self.close_error


class RecordingConnection:
    def __init__(
        self,
        role="read",
        events=None,
        *,
        rollback_error=None,
        close_error=None,
        execute_error=None,
        cursor_close_error=None,
        set_session_error=None,
        commit_error=None,
    ):
        self.role = role
        self.events = events if events is not None else []
        self.rollback_error = rollback_error
        self.close_error = close_error
        self.set_session_error = set_session_error
        self.commit_error = commit_error
        self.readonly = False
        self.commits = 0
        self.cursor_obj = RecordingCursor(
            role,
            self.events,
            execute_error=execute_error,
            close_error=cursor_close_error,
        )

    def set_session(self, *, readonly):
        self.events.append(f"{self.role}_set_readonly")
        if self.set_session_error:
            raise self.set_session_error
        self.readonly = bool(readonly)

    def cursor(self):
        return self.cursor_obj

    def rollback(self):
        self.events.append(f"{self.role}_rollback")
        if self.rollback_error:
            raise self.rollback_error

    def commit(self):
        self.events.append(f"{self.role}_commit")
        if self.commit_error:
            raise self.commit_error
        self.commits += 1

    def close(self):
        self.events.append(f"{self.role}_close")
        if self.close_error:
            raise self.close_error


class TrackedFrame(pd.DataFrame):
    _metadata = ["events", "fail_on_calculate", "calculation_started"]

    @property
    def _constructor(self):
        return pd.DataFrame

    def __getitem__(self, key):
        if key == "close" and not self.calculation_started:
            self.calculation_started = True
            assert "read_close" in self.events
            self.events.append("calculate")
            if self.fail_on_calculate:
                raise RuntimeError("calculation failed")
        return super().__getitem__(key)


def _tracked_frame(strategy_dir, events, *, fail_on_calculate=False):
    rows = 80
    data = {
        "id": range(1, rows + 1),
        "open_time": pd.date_range(
            "2026-01-01", periods=rows, freq="min", tz="UTC"
        ),
        "close": [100.0 + index * 0.03 for index in range(rows)],
    }
    if strategy_dir in {"bot", "bot_supertrend"}:
        data.update(
            {
                "high": [value + 0.08 for value in data["close"]],
                "low": [value - 0.09 for value in data["close"]],
            }
        )
    if strategy_dir == "bot_supertrend":
        data["open"] = [value - 0.02 for value in data["close"]]
    frame = TrackedFrame(data)
    frame.events = events
    frame.fail_on_calculate = fail_on_calculate
    frame.calculation_started = False
    return frame


def _load_strategy(monkeypatch, strategy_dir):
    strategy_name = {
        "bot": "RSI",
        "bot_bbrange": "BBRANGE",
        "bot_trend": "TREND",
        "bot_supertrend": "SUPERTREND",
    }[strategy_dir]
    safe_env = {
        "SYMBOL": "BTCUSDC",
        "QUOTE_ASSET": "USDC",
        "STRATEGY_NAME": strategy_name,
        "INTERVAL": "1m",
        "TRADING_MODE": "PAPER",
        "LIVE_ORDERS_ENABLED": "0",
        "DB_HOST": "invalid.test",
        "DB_NAME": "unit_test",
        "DB_USER": "unit_test",
        "DB_PASS": "unit_test",
        "EXCHANGE_PROVIDER": "OKX",
        "DAILY_MAX_LOSS_PCT": "0",
        "DISABLE_HOURS": "",
        "ADAPTIVE_EARLY_CUT_SHADOW_ENABLED": "0",
    }
    for key, value in safe_env.items():
        monkeypatch.setenv(key, value)
    module_name = f"waltrade_db_lifecycle_{strategy_dir}"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(
        module_name, ROOT / strategy_dir / "main.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _cleanup_case(
    *,
    body_error=None,
    rollback_error=None,
    close_error=None,
    set_session_error=None,
):
    events = []
    conn = RecordingConnection(
        events=events,
        rollback_error=rollback_error,
        close_error=close_error,
        set_session_error=set_session_error,
    )

    def connect():
        events.append("read_open")
        return conn

    def run():
        with read_only_db_conn(connect) as opened:
            assert opened is conn
            events.append("read_query")
            if body_error:
                raise body_error

    return run, conn, events


def test_read_only_connection_normal_path():
    run, conn, events = _cleanup_case()

    run()

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    assert conn.readonly is True
    assert conn.commits == 0


@pytest.mark.parametrize(
    ("rollback_error", "close_error", "expected_type", "expected_message"),
    [
        (RuntimeError("rollback failed"), None, RuntimeError, "rollback failed"),
        (None, ValueError("close failed"), ValueError, "close failed"),
        (
            RuntimeError("rollback failed"),
            ValueError("close failed"),
            RuntimeError,
            "rollback failed",
        ),
    ],
)
def test_cleanup_failure_is_primary_when_body_succeeds(
    rollback_error, close_error, expected_type, expected_message
):
    run, conn, events = _cleanup_case(
        rollback_error=rollback_error, close_error=close_error
    )

    with pytest.raises(expected_type, match=expected_message) as caught:
        run()

    assert events[-2:] == ["read_rollback", "read_close"]
    expected_errors = int(rollback_error is not None) + int(close_error is not None)
    assert len(caught.value.cleanup_errors) == expected_errors
    assert conn.commits == 0


@pytest.mark.parametrize(
    ("rollback_error", "close_error", "cleanup_error_count"),
    [
        (None, None, 0),
        (RuntimeError("rollback failed"), None, 1),
        (None, ValueError("close failed"), 1),
        (RuntimeError("rollback failed"), ValueError("close failed"), 2),
    ],
)
def test_body_exception_remains_primary_across_cleanup_failures(
    rollback_error, close_error, cleanup_error_count
):
    body_error = LookupError("body failed")
    run, conn, events = _cleanup_case(
        body_error=body_error,
        rollback_error=rollback_error,
        close_error=close_error,
    )

    with pytest.raises(LookupError, match="body failed") as caught:
        run()

    assert caught.value is body_error
    assert events[-2:] == ["read_rollback", "read_close"]
    assert len(getattr(caught.value, "cleanup_errors", ())) == cleanup_error_count
    assert conn.commits == 0


@pytest.mark.parametrize(
    ("body_error", "rollback_error", "close_error", "expected_type"),
    [
        (LookupError("body failed"), RuntimeError("rollback failed"), None, LookupError),
        (LookupError("body failed"), None, ValueError("close failed"), LookupError),
        (
            LookupError("body failed"),
            RuntimeError("rollback failed"),
            ValueError("close failed"),
            LookupError,
        ),
        (None, RuntimeError("rollback failed"), None, RuntimeError),
        (None, None, ValueError("close failed"), ValueError),
    ],
)
def test_logger_failure_never_changes_cleanup_precedence_or_order(
    monkeypatch, body_error, rollback_error, close_error, expected_type
):
    def fail_logging(*_args, **_kwargs):
        raise OSError("logger failed")

    monkeypatch.setattr(db_module.logging, "exception", fail_logging)
    run, conn, events = _cleanup_case(
        body_error=body_error,
        rollback_error=rollback_error,
        close_error=close_error,
    )

    with pytest.raises(expected_type) as caught:
        run()

    if body_error is not None:
        assert caught.value is body_error
    elif rollback_error is not None:
        assert caught.value is rollback_error
    else:
        assert caught.value is close_error
    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    expected_cleanup = tuple(
        error for error in (rollback_error, close_error) if error is not None
    )
    assert caught.value.cleanup_errors == expected_cleanup
    assert conn.commits == 0


@pytest.mark.parametrize(
    ("rollback_error", "close_error", "logger_fails"),
    [
        (None, None, False),
        (RuntimeError("rollback failed"), None, False),
        (None, ValueError("close failed"), False),
        (
            RuntimeError("rollback failed"),
            ValueError("close failed"),
            True,
        ),
    ],
)
def test_readonly_setup_failure_is_primary_and_never_yields(
    monkeypatch, rollback_error, close_error, logger_fails
):
    setup_error = ConnectionError("set_session failed")
    if logger_fails:
        monkeypatch.setattr(
            db_module.logging,
            "exception",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("logger failed")
            ),
        )
    run, conn, events = _cleanup_case(
        set_session_error=setup_error,
        rollback_error=rollback_error,
        close_error=close_error,
    )

    with pytest.raises(ConnectionError, match="set_session failed") as caught:
        run()

    assert caught.value is setup_error
    assert events == [
        "read_open",
        "read_set_readonly",
        "read_rollback",
        "read_close",
    ]
    assert "read_query" not in events
    assert getattr(caught.value, "cleanup_errors", ()) == tuple(
        error for error in (rollback_error, close_error) if error is not None
    )
    assert conn.commits == 0


def test_connection_without_set_session_is_rejected_fail_closed():
    class DynamicConnection:
        def __init__(self):
            self.events = []

        def rollback(self):
            self.events.append("rollback")

        def close(self):
            self.events.append("close")

    conn = DynamicConnection()
    body_called = False

    with pytest.raises(TypeError, match="must support set_session"):
        with read_only_db_conn(lambda: conn):
            body_called = True

    assert body_called is False
    assert not hasattr(conn, "readonly")
    assert conn.events == ["rollback", "close"]


def test_cleanup_diagnostics_fall_back_to_context_without_masking_primary(
    monkeypatch,
):
    class NonAttachableError(LookupError):
        def __setattr__(self, name, value):
            if name == "cleanup_errors":
                raise AttributeError("diagnostic attributes disabled")
            super().__setattr__(name, value)

    monkeypatch.setattr(
        db_module.logging,
        "exception",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("logger failed")),
    )
    body_error = NonAttachableError("body failed")
    rollback_error = RuntimeError("rollback failed")
    run, _conn, events = _cleanup_case(
        body_error=body_error,
        rollback_error=rollback_error,
    )

    with pytest.raises(NonAttachableError) as caught:
        run()

    assert caught.value is body_error
    assert events[-2:] == ["read_rollback", "read_close"]
    diagnostics = caught.value.__context__
    assert diagnostics is not None
    assert diagnostics.cleanup_errors == (rollback_error,)


def test_repeated_read_cycles_leave_every_connection_closed():
    events = []
    connections = [RecordingConnection(events=events) for _ in range(4)]

    for conn in connections:
        with read_only_db_conn(lambda conn=conn: conn):
            pass

    assert events.count("read_set_readonly") == 4
    assert events.count("read_rollback") == 4
    assert events.count("read_close") == 4
    assert all(conn.commits == 0 for conn in connections)


def _writer_case(
    *,
    execute_error=None,
    commit_error=None,
    rollback_error=None,
    cursor_close_error=None,
    close_error=None,
):
    events = []
    conn = RecordingConnection(
        "writer",
        events,
        execute_error=execute_error,
        commit_error=commit_error,
        rollback_error=rollback_error,
        cursor_close_error=cursor_close_error,
        close_error=close_error,
    )

    def connect():
        events.append("writer_open")
        return conn

    def run():
        with db_write_conn(connect) as (opened_conn, cur):
            assert opened_conn is conn
            cur.executemany("UPDATE candles SET close=%s WHERE id=%s", [(1, 1)])
            opened_conn.commit()

    return run, conn, events


def test_writer_connection_normal_path_commits_and_closes_in_order():
    run, conn, events = _writer_case()

    run()

    assert events == [
        "writer_open",
        "writer_execute",
        "writer_commit",
        "writer_cursor_close",
        "writer_close",
    ]
    assert conn.commits == 1


@pytest.mark.parametrize(
    (
        "execute_error",
        "commit_error",
        "rollback_error",
        "cursor_close_error",
        "close_error",
        "expected_primary",
        "expected_cleanup",
        "logger_fails",
    ),
    [
        (RuntimeError("execute failed"), None, None, None, None, "execute failed", (), False),
        (
            RuntimeError("execute failed"),
            None,
            OSError("rollback failed"),
            None,
            None,
            "execute failed",
            ("rollback failed",),
            False,
        ),
        (None, RuntimeError("commit failed"), None, None, None, "commit failed", (), False),
        (
            None,
            RuntimeError("commit failed"),
            OSError("rollback failed"),
            None,
            None,
            "commit failed",
            ("rollback failed",),
            False,
        ),
        (None, None, None, OSError("cursor close failed"), None, "cursor close failed", ("cursor close failed",), False),
        (None, None, None, None, OSError("connection close failed"), "connection close failed", ("connection close failed",), False),
        (
            None,
            None,
            None,
            OSError("cursor close failed"),
            ValueError("connection close failed"),
            "cursor close failed",
            ("cursor close failed", "connection close failed"),
            False,
        ),
        (
            RuntimeError("execute failed"),
            None,
            OSError("rollback failed"),
            ValueError("cursor close failed"),
            LookupError("connection close failed"),
            "execute failed",
            ("rollback failed", "cursor close failed", "connection close failed"),
            True,
        ),
    ],
)
def test_writer_exception_precedence_and_cleanup_matrix(
    monkeypatch,
    execute_error,
    commit_error,
    rollback_error,
    cursor_close_error,
    close_error,
    expected_primary,
    expected_cleanup,
    logger_fails,
):
    if logger_fails:
        monkeypatch.setattr(
            db_module.logging,
            "exception",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("logger failed")
            ),
        )
    run, conn, events = _writer_case(
        execute_error=execute_error,
        commit_error=commit_error,
        rollback_error=rollback_error,
        cursor_close_error=cursor_close_error,
        close_error=close_error,
    )

    with pytest.raises(BaseException, match=expected_primary) as caught:
        run()

    body_error = execute_error or commit_error
    if body_error is not None:
        assert caught.value is body_error
        assert "writer_rollback" in events
    assert events[-2:] == ["writer_cursor_close", "writer_close"]
    assert tuple(
        str(error) for error in getattr(caught.value, "cleanup_errors", ())
    ) == expected_cleanup
    assert conn.commits == (0 if commit_error else int(execute_error is None))


def _install_indicator_boundaries(
    monkeypatch,
    module,
    strategy_dir,
    *,
    empty=False,
    fail_on_read=False,
    fail_on_calculate=False,
    fail_on_writer=False,
    fail_on_writer_commit=False,
    fail_on_writer_rollback=False,
    fail_on_writer_cursor_close=False,
    fail_on_writer_close=False,
):
    events = []
    read_conn = RecordingConnection("read", events)
    writer_conn = RecordingConnection(
        "writer",
        events,
        execute_error=RuntimeError("writer failed") if fail_on_writer else None,
        commit_error=(
            RuntimeError("writer commit failed")
            if fail_on_writer_commit
            else None
        ),
        rollback_error=(
            RuntimeError("writer rollback failed")
            if fail_on_writer_rollback
            else None
        ),
        cursor_close_error=(
            RuntimeError("writer cursor close failed")
            if fail_on_writer_cursor_close
            else None
        ),
        close_error=(
            RuntimeError("writer connection close failed")
            if fail_on_writer_close
            else None
        ),
    )
    opened = []

    def connection_factory():
        if not opened:
            events.append("read_open")
            opened.append(read_conn)
            return read_conn
        events.append("writer_open")
        opened.append(writer_conn)
        return writer_conn

    def read_sql(query, conn, params=None):
        assert conn is read_conn
        assert conn.readonly is True
        events.append("read_query")
        read_conn.query = query
        read_conn.params = params
        if fail_on_read:
            raise RuntimeError("read failed")
        if empty:
            return pd.DataFrame()
        return _tracked_frame(
            strategy_dir, events, fail_on_calculate=fail_on_calculate
        )

    monkeypatch.setattr(module, "get_db_conn", connection_factory)
    monkeypatch.setattr(module.pd, "read_sql_query", read_sql)
    return events, read_conn, writer_conn, opened


@pytest.mark.parametrize(
    "strategy_dir", ["bot", "bot_bbrange", "bot_trend", "bot_supertrend"]
)
def test_all_strategy_indicator_paths_close_read_before_calculate_and_write(
    monkeypatch, strategy_dir
):
    module = _load_strategy(monkeypatch, strategy_dir)
    events, read_conn, writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, strategy_dir
    )
    progress = []

    if strategy_dir == "bot_supertrend":
        module.update_indicators(
            progress_callback=lambda phase, done, total: progress.append(
                (phase, done, total)
            )
        )
    else:
        module.update_indicators()

    core = [
        event
        for event in events
        if not event.endswith("_cursor_close")
    ]
    assert core == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
        "calculate",
        "writer_open",
        "writer_execute",
        "writer_commit",
        "writer_close",
    ]
    assert opened == [read_conn, writer_conn]
    assert read_conn.commits == 0
    assert writer_conn.commits == 1
    assert writer_conn.cursor_obj.rows
    assert read_conn.params == ("BTCUSDC", "1m")
    if strategy_dir == "bot_supertrend":
        phases = [phase for phase, _done, _total in progress]
        assert phases[:4] == ["LOAD_HISTORY", "EMA", "RSI", "ATR"]
        assert "SUPERTREND_LOOP" in phases
        assert phases[-1] == "PERSIST_LATEST"


@pytest.mark.parametrize(
    "strategy_dir", ["bot", "bot_bbrange", "bot_trend", "bot_supertrend"]
)
@pytest.mark.parametrize(
    ("failure", "expected_primary", "expected_cleanup"),
    [
        ("execute_rollback", "writer failed", ("writer rollback failed",)),
        (
            "commit_rollback",
            "writer commit failed",
            ("writer rollback failed",),
        ),
        (
            "cursor_close",
            "writer cursor close failed",
            ("writer cursor close failed",),
        ),
        (
            "connection_close",
            "writer connection close failed",
            ("writer connection close failed",),
        ),
    ],
)
def test_each_strategy_writer_failures_preserve_primary_and_complete_cleanup(
    monkeypatch, strategy_dir, failure, expected_primary, expected_cleanup
):
    module = _load_strategy(monkeypatch, strategy_dir)
    options = {
        "fail_on_writer": failure == "execute_rollback",
        "fail_on_writer_commit": failure == "commit_rollback",
        "fail_on_writer_rollback": failure in {"execute_rollback", "commit_rollback"},
        "fail_on_writer_cursor_close": failure == "cursor_close",
        "fail_on_writer_close": failure == "connection_close",
    }
    events, read_conn, writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, strategy_dir, **options
    )
    progress = []

    with pytest.raises(RuntimeError, match=expected_primary) as caught:
        if strategy_dir == "bot_supertrend":
            module.update_indicators(
                progress_callback=lambda phase, done, total: progress.append(
                    (phase, done, total)
                )
            )
        else:
            module.update_indicators()

    assert opened == [read_conn, writer_conn]
    assert events.index("read_close") < events.index("writer_open")
    assert events[-2:] == ["writer_cursor_close", "writer_close"]
    assert tuple(
        str(error) for error in getattr(caught.value, "cleanup_errors", ())
    ) == expected_cleanup
    if failure in {"execute_rollback", "commit_rollback"}:
        assert events[-3:] == [
            "writer_rollback",
            "writer_cursor_close",
            "writer_close",
        ]
    if strategy_dir == "bot_supertrend" and failure in {
        "execute_rollback",
        "commit_rollback",
    }:
        persist = [item for item in progress if item[0] == "PERSIST_LATEST"]
        assert len(persist) == 1
        assert persist[0][1] == 0


def test_rsi_empty_frame_closes_read_without_writer(monkeypatch):
    module = _load_strategy(monkeypatch, "bot")
    events, read_conn, _writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot", empty=True
    )

    module.update_indicators()

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    assert opened == [read_conn]


def test_rsi_calculation_exception_happens_after_read_close_without_writer(
    monkeypatch,
):
    module = _load_strategy(monkeypatch, "bot")
    events, read_conn, _writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot", fail_on_calculate=True
    )

    with pytest.raises(RuntimeError, match="calculation failed"):
        module.update_indicators()

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
        "calculate",
    ]
    assert opened == [read_conn]


def test_rsi_read_exception_rolls_back_and_closes_without_writer(monkeypatch):
    module = _load_strategy(monkeypatch, "bot")
    events, read_conn, _writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot", fail_on_read=True
    )

    with pytest.raises(RuntimeError, match="read failed"):
        module.update_indicators()

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    assert "calculate" not in events
    assert opened == [read_conn]


def test_rsi_writer_exception_rolls_back_and_closes_separate_writer(monkeypatch):
    module = _load_strategy(monkeypatch, "bot")
    events, read_conn, writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot", fail_on_writer=True
    )

    with pytest.raises(RuntimeError, match="writer failed"):
        module.update_indicators()

    core = [event for event in events if not event.endswith("_cursor_close")]
    assert core == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
        "calculate",
        "writer_open",
        "writer_execute",
        "writer_rollback",
        "writer_close",
    ]
    assert opened == [read_conn, writer_conn]
    assert writer_conn.commits == 0


def test_rsi_writer_commit_failure_rolls_back_and_closes_separate_writer(
    monkeypatch,
):
    module = _load_strategy(monkeypatch, "bot")
    events, read_conn, writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot", fail_on_writer_commit=True
    )

    with pytest.raises(RuntimeError, match="writer commit failed"):
        module.update_indicators()

    core = [event for event in events if not event.endswith("_cursor_close")]
    assert core == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
        "calculate",
        "writer_open",
        "writer_execute",
        "writer_commit",
        "writer_rollback",
        "writer_close",
    ]
    assert opened == [read_conn, writer_conn]
    assert writer_conn.commits == 0


def test_trend_empty_frame_closes_read_without_calculation_or_writer(monkeypatch):
    module = _load_strategy(monkeypatch, "bot_trend")
    events, read_conn, _writer_conn, opened = _install_indicator_boundaries(
        monkeypatch, module, "bot_trend", empty=True
    )

    module.update_indicators()

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    assert "calculate" not in events
    assert opened == [read_conn]


def test_bbrange_run_strategy_uses_one_short_lived_candle_read(monkeypatch):
    module = _load_strategy(monkeypatch, "bot_bbrange")
    events = []
    read_conn = RecordingConnection("read", events)
    opens = []

    def connection_factory():
        events.append("read_open")
        opens.append(read_conn)
        return read_conn

    def read_sql(_query, conn, params=None):
        assert conn is read_conn
        assert conn.readonly is True
        events.append("read_query")
        read_conn.params = params
        return pd.DataFrame()

    cfg = SimpleNamespace(trading_mode="PAPER")
    control = SimpleNamespace(
        enabled=True,
        mode="NORMAL",
        regime_enabled=True,
        regime_mode="DRY_RUN",
    )
    monkeypatch.setattr(module, "get_db_conn", connection_factory)
    monkeypatch.setattr(module.pd, "read_sql_query", read_sql)
    monkeypatch.setattr(
        module,
        "get_runtime_snapshot",
        lambda **_kwargs: {
            "bc": control,
            "cfg_effective": cfg,
            "heartbeat": {},
            "allowed_orders_entry": False,
            "allowed_orders_exit": False,
            "allow_meta_entry": {},
            "allow_meta_exit": {},
        },
    )
    monkeypatch.setattr(module, "get_open_position", lambda: None)
    monkeypatch.setattr(module, "heartbeat", lambda _info: None)
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_event: None)
    monkeypatch.setattr(module, "emit_blocked", lambda **_event: None)
    monkeypatch.setattr(module, "emit_regime_gate_event", lambda **_event: None)
    monkeypatch.setattr(
        module,
        "decide_regime_gate",
        lambda **_kwargs: SimpleNamespace(allow=True, why="test", regime="FLAT", meta={}),
    )
    monkeypatch.setattr(module, "hard_time_exit_enabled", lambda: False)
    monkeypatch.setattr(module, "DAILY_MAX_LOSS_PCT", 0.0)
    monkeypatch.setattr(module, "DISABLE_HOURS_SET", set())
    open_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    module.run_strategy((open_time, 100.0, 101.0, 99.0, 100.0, 100.0, 50.0))

    assert events == [
        "read_open",
        "read_set_readonly",
        "read_query",
        "read_rollback",
        "read_close",
    ]
    assert opens == [read_conn]
    assert read_conn.params == (
        "BTCUSDC",
        "1m",
        open_time,
        max(module.BB_PERIOD + 30, 120),
    )


@contextmanager
def _managed_postgres_observer(connection_factory):
    observer = connection_factory()
    try:
        observer.set_session(readonly=True, autocommit=True)
        yield observer
    except BaseException as primary:
        cleanup_errors = db_module._cleanup_db_resources(
            (
                ("observer rollback", observer.rollback),
                ("observer close", observer.close),
            )
        )
        db_module._attach_cleanup_errors(primary, cleanup_errors)
        raise
    else:
        cleanup_errors = db_module._cleanup_db_resources(
            (
                ("observer rollback", observer.rollback),
                ("observer close", observer.close),
            )
        )
        if cleanup_errors:
            primary = cleanup_errors[0]
            db_module._attach_cleanup_errors(primary, cleanup_errors)
            raise primary


@pytest.mark.parametrize("close_fails", [False, True])
def test_postgres_observer_setup_failure_always_closes_without_masking_primary(
    monkeypatch, close_fails
):
    events = []
    setup_error = ConnectionError("observer setup failed")
    close_error = RuntimeError("observer close failed") if close_fails else None

    class Observer:
        def set_session(self, *, readonly, autocommit):
            events.append("observer_set_session")
            assert readonly is True
            assert autocommit is True
            raise setup_error

        def rollback(self):
            events.append("observer_rollback")

        def close(self):
            events.append("observer_close")
            if close_error:
                raise close_error

    if close_fails:
        monkeypatch.setattr(
            db_module.logging,
            "exception",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("logger failed")
            ),
        )

    with pytest.raises(ConnectionError, match="observer setup failed") as caught:
        with _managed_postgres_observer(Observer):
            raise AssertionError("observer body must not run")

    assert caught.value is setup_error
    assert events == [
        "observer_set_session",
        "observer_rollback",
        "observer_close",
    ]
    assert getattr(caught.value, "cleanup_errors", ()) == (
        (close_error,) if close_error else ()
    )


def test_real_postgres_read_only_lifecycle_when_disposable_dsn_is_configured():
    dsn = os.getenv("WALTRADE_TEST_PG_DSN", "").strip()
    if not dsn:
        pytest.skip(
            "WALTRADE_TEST_PG_DSN is not set; real disposable PostgreSQL required"
        )

    import psycopg2
    from psycopg2 import errors
    from psycopg2.extensions import parse_dsn

    try:
        database = str(parse_dsn(dsn).get("dbname") or "")
    except Exception as exc:
        raise RuntimeError("invalid WALTRADE_TEST_PG_DSN") from exc
    if not database.lower().endswith("_test"):
        raise RuntimeError(
            "refusing PostgreSQL lifecycle test: DSN database must end in _test"
        )

    application_name = f"waltrade_read_lifecycle_{uuid.uuid4().hex}"

    def guarded_connect(app_name=application_name):
        conn = psycopg2.connect(
            dsn,
            application_name=app_name,
            connect_timeout=5,
            options="-c statement_timeout=10000 -c lock_timeout=2000",
        )
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                actual_database = str(cur.fetchone()[0])
                if actual_database != database or not actual_database.lower().endswith(
                    "_test"
                ):
                    raise RuntimeError(
                        "refusing PostgreSQL lifecycle test: connected database "
                        "is not *_test"
                    )
                cur.execute(
                    "SELECT value FROM automation_kv "
                    "WHERE key='waltrade_disposable_test_db'"
                )
                marker = cur.fetchone()
                if marker is None or str(marker[0]).strip().lower() != "true":
                    raise RuntimeError(
                        "refusing PostgreSQL lifecycle test: "
                        "waltrade_disposable_test_db=true is required"
                    )
            conn.autocommit = False
            return conn
        except Exception:
            conn.close()
            raise

    with _managed_postgres_observer(
        lambda: guarded_connect(f"{application_name}_observer")
    ) as observer:
        with read_only_db_conn(guarded_connect) as read_conn:
            frame = pd.read_sql_query("SELECT 1 AS value", read_conn)
            assert frame.to_dict("records") == [{"value": 1}]
            with observer.cursor() as cur:
                cur.execute(
                    "SELECT state FROM pg_stat_activity WHERE application_name=%s",
                    (application_name,),
                )
                assert cur.fetchone() is not None
            with read_conn.cursor() as cur:
                with pytest.raises(errors.ReadOnlySqlTransaction):
                    cur.execute(
                        "UPDATE automation_kv SET value=value "
                        "WHERE key='waltrade_disposable_test_db'"
                    )

        with observer.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=%s",
                (application_name,),
            )
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT count(*) FROM pg_locks AS locks "
                "JOIN pg_stat_activity AS activity ON activity.pid=locks.pid "
                "WHERE activity.application_name=%s "
                "AND locks.locktype IN ('transactionid', 'virtualxid')",
                (application_name,),
            )
            assert cur.fetchone()[0] == 0

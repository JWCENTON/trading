from __future__ import annotations

import ast
import importlib
from pathlib import Path
import sys

import pytest

from common.schema_readiness import (
    PENDING_ENTRY_FUNCTION,
    PENDING_ENTRY_INDEX_CONTRACT,
    PENDING_ENTRY_REQUIRED_KV,
    PENDING_ENTRY_SCHEMA_MARKER,
    PENDING_ENTRY_TABLES,
    PENDING_ENTRY_TRIGGER,
    validate_pending_entry_reconciliation_schema,
    validate_strategy_runtime_schema,
)


ROOT = Path(__file__).resolve().parents[1]
STRATEGY_FILES = (
    ROOT / "bot" / "main.py",
    ROOT / "bot_bbrange" / "main.py",
    ROOT / "bot_trend" / "main.py",
    ROOT / "bot_supertrend" / "main.py",
)
DDL_PREFIXES = ("ALTER", "CREATE", "DROP", "TRUNCATE", "REINDEX", "VACUUM")


class RecordingCursor:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.sql = []

    def execute(self, sql, params=None):
        normalized = " ".join(str(sql).split())
        self.sql.append((normalized, params))
        assert not normalized.upper().startswith(DDL_PREFIXES)

    def fetchall(self):
        return next(self.responses)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class RecordingConnection:
    def __init__(self, responses):
        self.cursor_obj = RecordingCursor(responses)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def _pending_ready_responses():
    columns = [
        (table, column)
        for table, names in PENDING_ENTRY_TABLES.items()
        for column in names
    ]
    index_defs = [
        (name, "CREATE UNIQUE INDEX " + name + " ON public.x " + " ".join(parts))
        for name, parts in PENDING_ENTRY_INDEX_CONTRACT.items()
    ]
    return [
        columns,
        index_defs,
        [(PENDING_ENTRY_FUNCTION,)],
        [(PENDING_ENTRY_TRIGGER, "strategy_events", PENDING_ENTRY_FUNCTION)],
        [
            (key, "1" if key == PENDING_ENTRY_SCHEMA_MARKER else "configured")
            for key in PENDING_ENTRY_REQUIRED_KV
        ],
    ]


def test_strategy_modules_do_not_import_or_call_ensure_schema():
    for path in STRATEGY_FILES:
        tree = ast.parse(path.read_text())
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        calls = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        assert "ensure_schema" not in imported, path
        assert "ensure_schema" not in calls, path


def test_schema_readiness_uses_selects_only():
    from common.schema_readiness import REQUIRED_COLUMNS, REQUIRED_INDEXES

    columns = [
        (table, column)
        for table, names in REQUIRED_COLUMNS.items()
        for column in names
    ]
    conn = RecordingConnection([
        columns,
        [(name,) for name in REQUIRED_INDEXES],
        *_pending_ready_responses(),
    ])

    validate_strategy_runtime_schema(conn)

    assert len(conn.cursor_obj.sql) == 7
    assert all(sql.upper().startswith("SELECT") for sql, _ in conn.cursor_obj.sql)
    assert not any("PG_ADVISORY" in sql.upper() for sql, _ in conn.cursor_obj.sql)


def test_schema_readiness_fails_fast_when_schema_is_missing():
    conn = RecordingConnection([[], [], [], [], [], [], []])

    with pytest.raises(RuntimeError, match="apply migrations"):
        validate_strategy_runtime_schema(conn)


@pytest.mark.parametrize(
    ("response_index", "replacement", "expected"),
    [
        (0, [], "columns:"),
        (1, [], "index:"),
        (2, [], "function:"),
        (3, [], "trigger:"),
        (4, [], "marker:"),
    ],
)
def test_pending_entry_readiness_rejects_incomplete_contract(
    response_index, replacement, expected
):
    responses = _pending_ready_responses()
    responses[response_index] = replacement
    conn = RecordingConnection(responses)
    with pytest.raises(RuntimeError, match=expected):
        validate_pending_entry_reconciliation_schema(conn)


def test_pending_entry_readiness_rejects_wrong_index_definition():
    responses = _pending_ready_responses()
    responses[1][0] = (responses[1][0][0], "CREATE INDEX wrong ON public.x (id)")
    conn = RecordingConnection(responses)
    with pytest.raises(RuntimeError, match="index_definition"):
        validate_pending_entry_reconciliation_schema(conn)


@pytest.mark.parametrize("missing_name", sorted(PENDING_ENTRY_INDEX_CONTRACT))
def test_pending_entry_readiness_rejects_each_missing_critical_index(missing_name):
    responses = _pending_ready_responses()
    responses[1] = [row for row in responses[1] if row[0] != missing_name]
    conn = RecordingConnection(responses)
    with pytest.raises(RuntimeError, match=missing_name):
        validate_pending_entry_reconciliation_schema(conn)


def test_pending_entry_readiness_accepts_full_contract():
    validate_pending_entry_reconciliation_schema(
        RecordingConnection(_pending_ready_responses())
    )


def test_runner_validates_schema_before_fetching_or_starting_children(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    sys.modules.pop("services.bot_runner.main", None)
    runner = importlib.import_module("services.bot_runner.main")
    operations = []
    conn = RecordingConnection([])

    class StopAfterReadiness(Exception):
        pass

    monkeypatch.setattr(runner, "db_connect", lambda: operations.append("connect") or conn)

    def readiness(_conn):
        operations.append("readiness")
        raise StopAfterReadiness

    monkeypatch.setattr(runner, "validate_strategy_runtime_schema", readiness)
    monkeypatch.setattr(
        runner,
        "record_worker_heartbeat",
        lambda *_args, **_kwargs: operations.append("error_heartbeat"),
    )
    monkeypatch.setattr(
        runner,
        "fetch_desired_configs",
        lambda *_args: pytest.fail("children/config fetched before readiness"),
    )
    monkeypatch.setattr(
        runner,
        "start_bot",
        lambda *_args: pytest.fail("child started before readiness"),
    )

    with pytest.raises(StopAfterReadiness):
        runner.main()
    assert operations == ["connect", "readiness", "error_heartbeat"]
    assert conn.closed


def test_user_settings_snapshot_performs_no_ddl(monkeypatch):
    module = importlib.import_module("common.user_settings")

    class Cursor(RecordingCursor):
        def fetchone(self):
            return (None, 6, 0, 0, "AUTO", None)

        def close(self):
            pass

    class Connection:
        def __init__(self):
            self.cursor_obj = Cursor([])

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            pass

        def close(self):
            pass

    conn = Connection()
    monkeypatch.setattr(module, "get_db_conn", lambda: conn)

    module.get_user_settings_snapshot()

    assert len(conn.cursor_obj.sql) == 1
    assert conn.cursor_obj.sql[0][0].upper().startswith("SELECT")


def test_worker_heartbeat_runtime_path_contains_no_schema_ddl():
    source = (ROOT / "common" / "worker_heartbeat.py").read_text().upper()
    for prefix in DDL_PREFIXES:
        assert f"{prefix} TABLE" not in source
    assert "917263001" not in source

from __future__ import annotations

import json

import pytest
from psycopg2.extensions import parse_dsn

from tests.postgres import database_baseline_fixture as fixture_module


VALID_DSN = (
    "host=fixture.invalid port=5432 "
    "dbname=waltrade_baseline_test_external user=test password=secret"
)
MISSING = object()


class FakeCursor:
    def __init__(self, connection, marker):
        self.connection = connection
        self.marker = marker
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        rendered = str(query)
        self.connection.server.statements.append(
            (self.connection.database, rendered, params)
        )
        if "current_database" in rendered:
            self._row = (self.connection.database,)
        elif "to_regclass" in rendered:
            self._row = (
                "automation_kv" if self.marker is not MISSING else None,
            )
        elif "SELECT value" in rendered:
            self._row = None if self.marker is MISSING else (self.marker,)
        else:
            self._row = None

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self, server, database, marker):
        self.server = server
        self.database = database
        self.marker = marker
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self, self.marker)

    def close(self):
        self.closed = True


class FakePostgres:
    def __init__(self, marker="true"):
        self.marker = marker
        self.connect_calls = []
        self.statements = []

    def connect(self, dsn=None, **kwargs):
        self.connect_calls.append((dsn, kwargs))
        if dsn:
            database = parse_dsn(dsn).get("dbname")
        else:
            database = kwargs.get("dbname")
        return FakeConnection(self, str(database), self.marker)


def _install_fake(monkeypatch, *, marker="true"):
    server = FakePostgres(marker)
    monkeypatch.setattr(fixture_module.psycopg2, "connect", server.connect)
    return server


def test_external_dsn_accepts_canonical_true_marker(monkeypatch):
    server = _install_fake(monkeypatch)

    fixture = fixture_module._external_disposable_postgres(VALID_DSN, "a" * 12)

    assert fixture.database == "waltrade_baseline_test_external"
    assert fixture.external_dsn == VALID_DSN
    assert len(server.connect_calls) == 1


@pytest.mark.parametrize(
    "database",
    [
        "waltrade_external_test",
        "waltrade_baseline_test_",
        "waltrade_baseline_test_bad-name",
    ],
)
def test_external_dsn_rejects_invalid_database_name(monkeypatch, database):
    server = _install_fake(monkeypatch)

    with pytest.raises(RuntimeError, match="must match"):
        fixture_module._external_disposable_postgres(
            f"dbname={database} user=test", "b" * 12
        )

    assert server.connect_calls == []


@pytest.mark.parametrize(
    "database",
    [
        "postgres", "template0", "template1", "trading", "trading_live",
        "trading_paper", "waltrade_baseline_test_trading_live",
        "waltrade_baseline_test_trading_paper",
    ],
)
def test_external_dsn_rejects_known_production_databases(monkeypatch, database):
    server = _install_fake(monkeypatch)

    with pytest.raises(RuntimeError, match="known non-test|production-marked"):
        fixture_module._external_disposable_postgres(
            f"dbname={database} user=test", "c" * 12
        )

    assert server.connect_calls == []


def test_external_dsn_rejects_missing_marker(monkeypatch):
    _install_fake(monkeypatch, marker=MISSING)

    with pytest.raises(RuntimeError, match="marker is required"):
        fixture_module._external_disposable_postgres(VALID_DSN, "d" * 12)


def test_external_dsn_rejects_false_marker(monkeypatch):
    _install_fake(monkeypatch, marker="false")

    with pytest.raises(RuntimeError, match="marker must be true"):
        fixture_module._external_disposable_postgres(VALID_DSN, "e" * 12)


def test_external_fixture_executes_no_docker_commands(monkeypatch):
    _install_fake(monkeypatch)
    monkeypatch.setenv(fixture_module.EXTERNAL_DSN_ENV, VALID_DSN)
    monkeypatch.delenv("WALTRADE_RUN_DISPOSABLE_PG", raising=False)
    monkeypatch.setattr(
        fixture_module, "_docker",
        lambda *_args: pytest.fail("Docker command executed in external mode"),
    )
    generator = fixture_module.disposable_postgres_v16.__wrapped__()

    fixture = next(generator)
    assert fixture.external_dsn == VALID_DSN
    with pytest.raises(StopIteration):
        next(generator)


def test_external_child_create_connect_and_cleanup_are_owned(monkeypatch):
    server = _install_fake(monkeypatch)
    fixture = fixture_module._external_disposable_postgres(VALID_DSN, "f" * 12)
    logical = "waltrade_baseline_test_child"

    fixture.create_database(logical)
    physical = fixture._children[logical]
    child = fixture.connect(logical)
    assert child.database == physical
    child.close()
    fixture.cleanup()

    create_queries = [statement for statement in server.statements if "CREATE DATABASE" in statement[1]]
    drop_queries = [statement for statement in server.statements if "DROP DATABASE" in statement[1]]
    assert len(create_queries) == 1
    assert len(drop_queries) == 1
    assert physical in create_queries[0][1]
    assert physical in drop_queries[0][1]
    assert fixture.database not in drop_queries[0][1]
    assert fixture._children == {}


def test_external_connect_rejects_unowned_database(monkeypatch):
    _install_fake(monkeypatch)
    fixture = fixture_module._external_disposable_postgres(VALID_DSN, "1" * 12)

    with pytest.raises(RuntimeError, match="only to its root or a child"):
        fixture.connect("waltrade_baseline_test_not_owned")

    with pytest.raises(RuntimeError, match="root database cannot be a child"):
        fixture.create_database(fixture.database)

    with pytest.raises(RuntimeError, match="child database must match"):
        fixture.create_database("not_a_disposable_child")


def test_managed_docker_mode_lifecycle_is_unchanged(monkeypatch):
    server = _install_fake(monkeypatch)
    calls = []

    def fake_docker(*args):
        calls.append(args)
        if args[0] == "inspect":
            return json.dumps([{
                "NetworkSettings": {
                    "Ports": {"5432/tcp": [{"HostPort": "55432"}]}
                }
            }])
        return ""

    monkeypatch.delenv(fixture_module.EXTERNAL_DSN_ENV, raising=False)
    monkeypatch.setenv("WALTRADE_RUN_DISPOSABLE_PG", "1")
    monkeypatch.setenv("WALTRADE_DISPOSABLE_TOKEN", "2" * 12)
    monkeypatch.setattr(fixture_module, "_docker", fake_docker)
    generator = fixture_module.disposable_postgres_v16.__wrapped__()

    fixture = next(generator)
    assert fixture.external_dsn is None
    assert fixture.database == "waltrade_baseline_test_live"
    assert fixture.port == 55432
    with pytest.raises(StopIteration):
        next(generator)

    actions = [(call[0], call[1]) for call in calls if len(call) > 1]
    assert actions == [
        ("network", "create"),
        ("volume", "create"),
        ("run", "-d"),
        ("inspect", "waltrade-baseline-v1-test-222222222222-pg"),
        ("rm", "-f"),
        ("volume", "rm"),
        ("network", "rm"),
    ]
    assert server.connect_calls[0][0] is None

import json

import pytest

from common.bounded_horizon_label_automation import (
    BOUNDED_LIMIT,
    TELEMETRY_PREFIX,
    run_bounded_horizon_label_automation,
)


def _env(deployment_id, trading_mode="PAPER"):
    environment = "paper" if trading_mode == "PAPER" else "live"
    return {
        "TRADING_MODE": trading_mode,
        "ENVIRONMENT": environment,
        "DEPLOYMENT_ID": deployment_id,
    }


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        params = tuple(params or ())
        self.connection.queries.append((normalized, params))
        if "SELECT EXISTS(" in normalized:
            self.row = (self.connection.due.pop(0),)
        elif "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1" in normalized:
            self.connection.producer_calls.append(params)
            result = self.connection.producer_results.pop(0)
            if isinstance(result, Exception):
                raise result
            self.row = (result,)
        elif "INSERT INTO automation_kv" in normalized:
            self.connection.kv[params[0]] = params[1]
            self.row = None
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, *, due=(), producer_results=()):
        self.due = list(due)
        self.producer_results = list(producer_results)
        self.producer_calls = []
        self.queries = []
        self.kv = {}
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _key(suffix):
    return f"{TELEMETRY_PREFIX}_{suffix}"


def test_local_paper_mature_snapshot_calls_canonical_bounded_producer():
    conn = _Connection(due=[True], producer_results=[3])

    result = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )

    assert result.status == "OK"
    assert result.inserted == 3
    assert conn.producer_calls == [("trading_paper", "LOCAL", BOUNDED_LIMIT)]
    assert conn.kv[_key("last_status")] == "OK"
    assert conn.kv[_key("last_success_at")]
    assert json.loads(conn.kv[_key("last_stats_json")])["inserted"] == 3


def test_paper_without_mature_horizon_is_not_due_and_skips_producer():
    conn = _Connection(due=[False])

    result = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )

    assert result.status == "NOT_DUE"
    assert result.producer_called is False
    assert conn.producer_calls == []
    assert conn.kv[_key("last_status")] == "NOT_DUE"


def test_paper_retry_is_idempotent_at_automation_boundary():
    conn = _Connection(due=[True, True], producer_results=[3, 0])

    first = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )
    second = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )

    assert (first.inserted, second.inserted) == (3, 0)
    assert len(conn.producer_calls) == 2
    assert json.loads(conn.kv[_key("last_stats_json")])["inserted"] == 0


@pytest.mark.parametrize("deployment_id", ["local-live", "vps-live"])
def test_live_is_hard_fenced_without_db_access(deployment_id):
    conn = _Connection()

    result = run_bounded_horizon_label_automation(
        conn, environ=_env(deployment_id, "LIVE")
    )

    assert result is None
    assert conn.queries == []
    assert conn.producer_calls == []
    assert conn.kv == {}


def test_vps_paper_shared_code_resolves_canonical_producer_identity():
    conn = _Connection(due=[True], producer_results=[1])

    result = run_bounded_horizon_label_automation(
        conn, environ=_env("vps-paper")
    )

    assert result.status == "OK"
    assert conn.producer_calls == [("trading_paper", "VPS", BOUNDED_LIMIT)]


def test_producer_exception_is_error_telemetry_and_does_not_escape():
    conn = _Connection(
        due=[True], producer_results=[RuntimeError("producer unavailable")]
    )

    result = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )

    assert result.status == "ERROR"
    assert "producer unavailable" in result.error
    assert conn.rollbacks == 1
    assert conn.kv[_key("last_status")] == "ERROR"
    assert "producer unavailable" in conn.kv[_key("last_error")]


def test_backlog_drains_across_bounded_cycles_without_changing_limit():
    conn = _Connection(due=[True, True], producer_results=[500, 37])

    first = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )
    second = run_bounded_horizon_label_automation(
        conn, environ=_env("local-paper")
    )

    assert (first.inserted, second.inserted) == (500, 37)
    assert conn.producer_calls == [
        ("trading_paper", "LOCAL", 500),
        ("trading_paper", "LOCAL", 500),
    ]


def test_due_query_uses_horizon_maturity_without_fixed_business_cadence():
    conn = _Connection(due=[False])

    run_bounded_horizon_label_automation(conn, environ=_env("local-paper"))

    due_sql = next(sql for sql, _ in conn.queries if "SELECT EXISTS(" in sql)
    assert "(VALUES(15),(30),(60))" in due_sql
    assert "clock_timestamp() >=" in due_sql
    assert "make_interval(mins=>horizon.horizon_minutes)" in due_sql
    assert "NOT EXISTS" in due_sql


def test_runner_source_has_outer_paper_fence_and_failure_isolation():
    source = open("automation_runner/main.py", encoding="utf-8").read()
    assert 'if cfg.trading_mode == "PAPER":' in source
    assert "run_bounded_horizon_label_automation" in source
    assert '"bounded_horizon_label_automation failed"' in source

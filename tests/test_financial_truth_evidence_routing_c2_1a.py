from __future__ import annotations

from datetime import datetime, timezone

import pytest

from common.financial_truth_repository import (
    CanonicalFinancialTruthWriteRepository,
    EXCHANGE_SCHEMA_CONTRACT,
    SIMULATED_SCHEMA_CONTRACT,
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
    SourceReadinessIssue,
)
from common.financial_truth_writer import FinancialTruthReconciler


NOW = datetime(2026, 7, 28, tzinfo=timezone.utc)
POSITION = (7, "OPEN", None, None, None)


def evidence_row(
    prefix: str,
    *,
    environment: str | None = None,
    deployment: str = "deployment-1",
    exchange: str | None = None,
):
    simulated = prefix == "simulated"
    return (
        f"{prefix}:1", "order-1", 7, "ENTRY", "BUY", "BTCUSDC",
        "1", "10", "10", "0.01", "USDC", "0.01", None,
        "account-fingerprint", "metadata-fingerprint", "0.001",
        "BTC", "USDC", f"{prefix.upper()}_EXECUTION",
        exchange or ("SIMULATOR" if simulated else "OKX"),
        environment or ("paper" if simulated else "live"),
        deployment, "SOURCE_V1", NOW,
    )


def schema_rows(contract):
    return [
        (table_name, column_name)
        for table_name, columns in contract.items()
        for column_name in columns
    ]


class Cursor:
    def __init__(self, *, columns, simulated_rows=(), exchange_rows=()):
        self.columns = list(columns)
        self.simulated_rows = list(simulated_rows)
        self.exchange_rows = list(exchange_rows)
        self.queries = []
        self.kind = None
        self.params = None

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.queries.append((normalized, params))
        self.params = params
        if "FROM public.positions WHERE id=" in normalized:
            self.kind = "position"
        elif "FROM information_schema.columns" in normalized:
            self.kind = "capability"
        elif "FROM public.simulated_execution_fills_v1 sf" in normalized:
            self.kind = "simulated"
        elif "FROM public.binance_order_fills f" in normalized:
            self.kind = "exchange"
        else:
            raise AssertionError(f"unexpected query: {normalized}")

    def fetchone(self):
        assert self.kind == "position"
        return POSITION

    def fetchall(self):
        if self.kind == "capability":
            requested_tables = set(self.params[0])
            return [row for row in self.columns if row[0] in requested_tables]
        if self.kind == "simulated":
            position_id, environment, deployment = self.params
            return [
                row for row in self.simulated_rows
                if row[2] == position_id
                and row[20].lower() == environment
                and row[21] == deployment
            ]
        if self.kind == "exchange":
            position_id, exchange = self.params[3], self.params[4]
            return [
                row for row in self.exchange_rows
                if row[2] == position_id and row[19].lower() == exchange
            ]
        raise AssertionError(f"unexpected fetchall for {self.kind}")

    def close(self):
        pass


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def repository(cursor):
    return FinancialTruthSourceRepository(lambda: Connection(cursor))


@pytest.mark.parametrize(
    "exchange_shape",
    [
        [],  # LOCAL PAPER: binance_orders absent.
        [  # VPS PAPER: legacy subset without the required contract.
            ("binance_orders", "position_id"),
            ("binance_orders", "order_id"),
            ("binance_orders", "symbol"),
            ("binance_orders", "side"),
            ("binance_orders", "status"),
            ("binance_order_fills", "id"),
            ("binance_order_fills", "source"),
            ("binance_order_fills", "order_id"),
            ("binance_order_fills", "symbol"),
            ("binance_order_fills", "side"),
            ("binance_order_fills", "executed_qty"),
            ("binance_order_fills", "avg_price"),
            ("binance_order_fills", "commission_amount"),
            ("binance_order_fills", "commission_asset"),
            ("binance_order_fills", "commission_usdc"),
            ("binance_order_fills", "event_time"),
        ],
        schema_rows(EXCHANGE_SCHEMA_CONTRACT),  # Full exchange schema.
    ],
)
def test_paper_always_uses_simulated_path(exchange_shape):
    cursor = Cursor(
        columns=schema_rows(SIMULATED_SCHEMA_CONTRACT) + exchange_shape,
        simulated_rows=[evidence_row("simulated", deployment="local-paper")],
        exchange_rows=[evidence_row("exchange")],
    )

    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("paper", "OKX", "local-paper"),
    )

    assert issue is None
    assert [fill.fill_id for fill in fills] == ["simulated:1"]
    assert not any(
        "binance_order_fills" in str(params)
        for q, params in cursor.queries
        if "information_schema.columns" in q
    )
    assert not any(
        "FROM public.binance_order_fills f" in q for q, _ in cursor.queries
    )
    simulated_query = next(
        item for item in cursor.queries
        if "FROM public.simulated_execution_fills_v1 sf" in item[0]
    )
    assert simulated_query[1] == (7, "paper", "local-paper")
    assert "FROM simulated_execution_fills_v1 sf" not in simulated_query[0]


def test_paper_empty_simulated_table_is_controlled_no_evidence():
    cursor = Cursor(columns=schema_rows(SIMULATED_SCHEMA_CONTRACT))
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("paper", "OKX", "local-paper"),
    )
    assert fills == ()
    assert issue == "NO_EXECUTION_EVIDENCE"


def test_paper_missing_simulated_column_is_controlled_unsupported():
    columns = schema_rows(SIMULATED_SCHEMA_CONTRACT)
    columns.remove(("simulated_execution_fills_v1", "position_id"))
    cursor = Cursor(columns=columns)
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("paper", "OKX", "local-paper"),
    )
    assert fills == ()
    assert issue == "SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED"
    assert not any(
        "FROM public.simulated_execution_fills_v1 sf" in q
        for q, _ in cursor.queries
    )


def test_live_full_contract_uses_exchange_only():
    cursor = Cursor(
        columns=(
            schema_rows(EXCHANGE_SCHEMA_CONTRACT)
            + schema_rows(SIMULATED_SCHEMA_CONTRACT)
        ),
        simulated_rows=[evidence_row("simulated")],
        exchange_rows=[evidence_row("exchange")],
    )
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("live", "OKX", "local-live"),
    )
    assert issue is None
    assert [fill.fill_id for fill in fills] == ["exchange:1"]
    assert not any(
        "FROM public.simulated_execution_fills_v1 sf" in q
        for q, _ in cursor.queries
    )
    exchange_query = next(
        item for item in cursor.queries
        if "FROM public.binance_order_fills f" in item[0]
    )
    assert exchange_query[1][-1] == "okx"
    assert "FROM binance_order_fills f" not in exchange_query[0]
    assert "JOIN public.binance_orders bo" in exchange_query[0]
    assert "JOIN binance_orders bo" not in exchange_query[0]


@pytest.mark.parametrize(
    "missing",
    [
        ("binance_orders", "exchange_source"),
        ("binance_orders", "reconciled_position_id"),
    ],
)
def test_live_incomplete_exchange_contract_never_executes_exchange_query(missing):
    columns = schema_rows(EXCHANGE_SCHEMA_CONTRACT)
    columns.remove(missing)
    cursor = Cursor(columns=columns)
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("live", "OKX", "local-live"),
    )
    assert fills == ()
    assert issue == "EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED"
    assert not any(
        "FROM public.binance_order_fills f" in q for q, _ in cursor.queries
    )


def test_live_absent_exchange_tables_is_controlled_unsupported():
    cursor = Cursor(columns=schema_rows(SIMULATED_SCHEMA_CONTRACT))
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("live", "OKX", "local-live"),
    )
    assert fills == ()
    assert issue == "EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED"


def test_paper_deployment_filter_is_enforced_by_fixture():
    cursor = Cursor(
        columns=schema_rows(SIMULATED_SCHEMA_CONTRACT),
        simulated_rows=[
            evidence_row("simulated", deployment="local-paper"),
            evidence_row("simulated", deployment="vps-paper"),
        ],
    )
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("paper", "OKX", "local-paper"),
    )
    assert issue is None
    assert len(fills) == 1
    assert fills[0].source_deployment_id == "local-paper"


def test_live_exchange_filter_is_enforced_by_fixture():
    cursor = Cursor(
        columns=schema_rows(EXCHANGE_SCHEMA_CONTRACT),
        exchange_rows=[
            evidence_row("exchange", exchange="OKX"),
            evidence_row("exchange", exchange="OTHER_EXCHANGE"),
        ],
    )
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("live", "OKX", "local-live"),
    )
    assert issue is None
    assert len(fills) == 1
    assert fills[0].source_exchange == "OKX"


@pytest.mark.parametrize(
    ("environment", "exchange", "deployment", "reason"),
    [
        ("unknown", "OKX", "x", "INVALID_EXECUTION_EVIDENCE_ENVIRONMENT"),
        ("live", None, "x", "LIVE_EXECUTION_EVIDENCE_EXCHANGE_REQUIRED"),
        ("paper", "OKX", "", "EXECUTION_EVIDENCE_DEPLOYMENT_REQUIRED"),
    ],
)
def test_invalid_context_fails_before_connection(
    environment, exchange, deployment, reason
):
    called = False

    def connect():
        nonlocal called
        called = True
        raise AssertionError("connection must not be opened")

    with pytest.raises(ValueError, match=reason):
        context = ExecutionEvidenceContext(environment, exchange, deployment)
        FinancialTruthSourceRepository(connect).read_position_and_fills(
            7, context=context
        )
    assert called is False


@pytest.mark.parametrize(
    "issue",
    list(SourceReadinessIssue),
)
def test_reconciler_exposes_controlled_source_issue(monkeypatch, issue):
    reconciler = FinancialTruthReconciler(lambda: None)
    monkeypatch.setattr(
        reconciler.sources,
        "read_position_and_fills",
        lambda *_args, **_kwargs: (POSITION, (), issue),
    )
    context = ExecutionEvidenceContext(
        "live" if issue.value.startswith("EXCHANGE") else "paper",
        "OKX",
        "deployment-1",
    )
    outcome = reconciler.reconcile(
        7, requested_mode="shadow", evidence_context=context
    )
    calculation = outcome["calculation"]
    assert calculation.financial_truth_status == "UNKNOWN"
    assert calculation.failure_code == issue.value
    assert calculation.failure_detail == issue.value
    assert outcome["written"] is False


class ApplyCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class ApplyConnection:
    def __init__(self):
        self._cursor = ApplyCursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def enable_paper_apply(monkeypatch):
    monkeypatch.setenv("FINANCIAL_TRUTH_WRITER_ENABLED", "1")
    monkeypatch.setenv("FINANCIAL_TRUTH_WRITER_MODE", "apply")
    monkeypatch.setenv("FINANCIAL_TRUTH_WRITER_ENV_ALLOWLIST", "paper")


@pytest.mark.parametrize("issue", list(SourceReadinessIssue))
def test_apply_source_readiness_issue_never_calls_canonical_or_audit_writer(
    monkeypatch, issue
):
    enable_paper_apply(monkeypatch)
    reconciler = FinancialTruthReconciler(ApplyConnection)
    monkeypatch.setattr(
        reconciler.sources,
        "read_position_and_fills",
        lambda *_args, **_kwargs: (POSITION, (), issue),
    )
    lock_calls = []
    write_calls = []
    monkeypatch.setattr(
        CanonicalFinancialTruthWriteRepository,
        "lock_position",
        lambda *_args: lock_calls.append(_args),
    )
    monkeypatch.setattr(
        CanonicalFinancialTruthWriteRepository,
        "write",
        lambda *_args, **_kwargs: write_calls.append((_args, _kwargs)),
    )

    outcome = reconciler.reconcile(
        7,
        requested_mode="apply",
        evidence_context=ExecutionEvidenceContext(
            "paper", "OKX", "local-paper"
        ),
    )

    assert len(lock_calls) == 1
    assert write_calls == []
    assert outcome["written"] is False
    assert outcome["calculation"].financial_truth_status == "UNKNOWN"
    assert outcome["calculation"].failure_code == issue.value
    assert outcome["calculation"].failure_detail == issue.value


def test_apply_valid_canonical_outcome_still_calls_writer(monkeypatch):
    enable_paper_apply(monkeypatch)
    reconciler = FinancialTruthReconciler(ApplyConnection)
    valid_fill = FinancialTruthSourceRepository._fill(
        evidence_row("simulated", deployment="local-paper")
    )
    monkeypatch.setattr(
        reconciler.sources,
        "read_position_and_fills",
        lambda *_args, **_kwargs: (POSITION, (valid_fill,), None),
    )
    monkeypatch.setattr(
        CanonicalFinancialTruthWriteRepository,
        "lock_position",
        lambda *_args: None,
    )
    write_calls = []

    def write_spy(*args, **kwargs):
        write_calls.append((args, kwargs))
        return True

    monkeypatch.setattr(
        CanonicalFinancialTruthWriteRepository, "write", write_spy
    )

    outcome = reconciler.reconcile(
        7,
        requested_mode="apply",
        evidence_context=ExecutionEvidenceContext(
            "paper", "OKX", "local-paper"
        ),
    )

    assert len(write_calls) == 1
    assert outcome["written"] is True
    assert outcome["calculation"].financial_truth_status == "INCOMPLETE"

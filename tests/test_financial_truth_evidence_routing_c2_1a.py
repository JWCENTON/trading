from __future__ import annotations

from datetime import datetime, timezone

import pytest

from common.financial_truth_repository import (
    EXCHANGE_SCHEMA_CONTRACT,
    SIMULATED_SCHEMA_CONTRACT,
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
)
from common.financial_truth_writer import FinancialTruthReconciler


NOW = datetime(2026, 7, 28, tzinfo=timezone.utc)
POSITION = (7, "OPEN", None, None, None)


def evidence_row(prefix: str):
    return (
        f"{prefix}:1", "order-1", 7, "ENTRY", "BUY", "BTCUSDC",
        "1", "10", "10", "0.01", "USDC", "0.01", None,
        "account-fingerprint", "metadata-fingerprint", "0.001",
        "BTC", "USDC", f"{prefix.upper()}_EXECUTION",
        "SIMULATOR" if prefix == "simulated" else "okx",
        "paper" if prefix == "simulated" else "live",
        "deployment-1", "SOURCE_V1", NOW,
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

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.queries.append((normalized, params))
        if "FROM positions WHERE id=" in normalized:
            self.kind = "position"
        elif "FROM information_schema.columns" in normalized:
            self.kind = "capability"
        elif "FROM simulated_execution_fills_v1 sf" in normalized:
            self.kind = "simulated"
        elif "FROM binance_order_fills f" in normalized:
            self.kind = "exchange"
        else:
            raise AssertionError(f"unexpected query: {normalized}")

    def fetchone(self):
        assert self.kind == "position"
        return POSITION

    def fetchall(self):
        return {
            "capability": self.columns,
            "simulated": self.simulated_rows,
            "exchange": self.exchange_rows,
        }[self.kind]

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
            ("binance_orders", "order_id"),
            ("binance_orders", "symbol"),
        ],
        schema_rows(EXCHANGE_SCHEMA_CONTRACT),  # Full exchange schema.
    ],
)
def test_paper_always_uses_simulated_path(exchange_shape):
    cursor = Cursor(
        columns=schema_rows(SIMULATED_SCHEMA_CONTRACT) + exchange_shape,
        simulated_rows=[evidence_row("simulated")],
        exchange_rows=[evidence_row("exchange")],
    )

    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("paper", "OKX", "local-paper"),
    )

    assert issue is None
    assert [fill.fill_id for fill in fills] == ["simulated:1"]
    assert not any("FROM binance_order_fills f" in q for q, _ in cursor.queries)
    simulated_query = next(
        item for item in cursor.queries
        if "FROM simulated_execution_fills_v1 sf" in item[0]
    )
    assert simulated_query[1] == (7, "paper", "local-paper")


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
        "FROM simulated_execution_fills_v1 sf" in q for q, _ in cursor.queries
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
        "FROM simulated_execution_fills_v1 sf" in q for q, _ in cursor.queries
    )
    exchange_query = next(
        item for item in cursor.queries
        if "FROM binance_order_fills f" in item[0]
    )
    assert exchange_query[1][-1] == "okx"


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
    assert not any("FROM binance_order_fills f" in q for q, _ in cursor.queries)


def test_live_absent_exchange_tables_is_controlled_unsupported():
    cursor = Cursor(columns=schema_rows(SIMULATED_SCHEMA_CONTRACT))
    _, fills, issue = repository(cursor).read_position_and_fills(
        7,
        context=ExecutionEvidenceContext("live", "OKX", "local-live"),
    )
    assert fills == ()
    assert issue == "EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED"


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
    [
        "NO_EXECUTION_EVIDENCE",
        "SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED",
        "EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED",
    ],
)
def test_reconciler_exposes_controlled_source_issue(monkeypatch, issue):
    reconciler = FinancialTruthReconciler(lambda: None)
    monkeypatch.setattr(
        reconciler.sources,
        "read_position_and_fills",
        lambda *_args, **_kwargs: (POSITION, (), issue),
    )
    context = ExecutionEvidenceContext(
        "live" if issue.startswith("EXCHANGE") else "paper",
        "OKX",
        "deployment-1",
    )
    outcome = reconciler.reconcile(
        7, requested_mode="shadow", evidence_context=context
    )
    calculation = outcome["calculation"]
    assert calculation.financial_truth_status == "UNKNOWN"
    assert calculation.failure_code == issue
    assert calculation.failure_detail == issue
    assert outcome["written"] is False

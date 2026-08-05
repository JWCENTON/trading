from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INGEST = ROOT / "common" / "exchange_ingest_trades.py"


def source() -> str:
    return INGEST.read_text()


def test_live_exit_reconciliation_invokes_canonical_financial_truth():
    text = source()

    assert "reconcile_live_terminal_financial_truth" in text
    assert "CanonicalFinancialTruthWriteRepository.write" in text
    assert "calculate_financial_truth" in text
    assert 'invocation_type="RUNTIME_LIVE_EXIT"' in text


def test_live_ft_is_written_before_ingest_commit():
    text = source()

    reconcile_pos = text.index("reconcile_okx_exit_fills(")
    ft_pos = text.index(
        "reconcile_live_terminal_financial_truth(",
        reconcile_pos + 1,
    )
    commit_pos = text.index("conn.commit()", ft_pos)

    assert reconcile_pos < ft_pos < commit_pos


def test_live_terminal_ft_failure_is_not_silently_committed():
    text = source()

    assert "LIVE_FINANCIAL_TRUTH_RECONCILIATION_FAILED" in text
    assert "raise" in text[
        text.index("LIVE_FINANCIAL_TRUTH_RECONCILIATION_FAILED"):
        text.index("LIVE_FINANCIAL_TRUTH_RECONCILIATION_FAILED") + 500
    ]


def test_partial_or_open_positions_are_not_written_as_complete_ft():
    text = source()

    assert "p.status='CLOSED'" in text
    assert "financial_truth.financial_truth_status != \"COMPLETE\"" in text


class _Cursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return []


class _CursorContext:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self):
        self.cursor_instance = _Cursor()

    def cursor(self):
        return _CursorContext(self.cursor_instance)


def test_live_terminal_ft_no_changed_orders_is_noop():
    from common.exchange_ingest_trades import (
        reconcile_live_terminal_financial_truth,
    )

    conn = _Connection()

    result = reconcile_live_terminal_financial_truth(
        conn,
        changed_order_ids=(),
        environment="live",
        deployment_id="local-live",
        source="okx",
    )

    assert result == 0
    assert conn.cursor_instance.executed == []


def test_live_terminal_ft_rejects_missing_deployment():
    import pytest

    from common.exchange_ingest_trades import (
        reconcile_live_terminal_financial_truth,
    )

    with pytest.raises(
        RuntimeError,
        match="LIVE_FINANCIAL_TRUTH_DEPLOYMENT_REQUIRED",
    ):
        reconcile_live_terminal_financial_truth(
            _Connection(),
            changed_order_ids=("123",),
            environment="live",
            deployment_id="",
            source="okx",
        )


def test_live_terminal_ft_passes_inventory_classification_to_calculator():
    text = source()

    assert "inventory_classification=inventory_classification" in text
    assert "ExitInventoryStatus.FULLY_EXECUTED_CLOSE" in text
    assert "ExitInventoryStatus.TERMINAL_DUST_CLOSE" in text
    assert "LIVE_FINANCIAL_TRUTH_INVENTORY_EVIDENCE_INCOMPLETE" in text
    assert "LIVE_FINANCIAL_TRUTH_TERMINAL_DUST_CONFLICT" in text

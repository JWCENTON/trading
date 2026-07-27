from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from common.financial_truth import (
    FinancialTruthEvidence,
    FinancialTruthLifecycle,
    PositionLifecycle,
    financial_truth_api_values,
    validate_financial_truth,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260727_canonical_financial_truth_foundation_v1.sql"
)
API = ROOT / "api/main.py"


def complete_evidence() -> FinancialTruthEvidence:
    return FinancialTruthEvidence(
        executed_entry_qty=Decimal("1"),
        executed_exit_qty=Decimal("1"),
        remaining_qty=Decimal("0"),
        authoritative_entry_fees_usdc=Decimal("0.10"),
        authoritative_exit_fees_usdc=Decimal("0.11"),
        authoritative_gross_pnl=Decimal("2.00"),
        authoritative_net_pnl=Decimal("1.79"),
        authoritative_source="EXCHANGE_FILLS",
        authoritative_evidence={"entry_fill_ids": [1], "exit_fill_ids": [2]},
    )


@pytest.mark.parametrize("position_status", list(PositionLifecycle))
@pytest.mark.parametrize("truth_status", list(FinancialTruthLifecycle))
def test_position_and_financial_truth_lifecycles_are_independent(
    position_status,
    truth_status,
):
    assert position_status.value in {"OPEN", "CLOSED"}
    assert truth_status.value in {"UNKNOWN", "INCOMPLETE", "COMPLETE", "FAILED"}


def test_open_does_not_imply_unknown():
    pair = (PositionLifecycle.OPEN, FinancialTruthLifecycle.INCOMPLETE)
    assert pair != (PositionLifecycle.OPEN, FinancialTruthLifecycle.UNKNOWN)


def test_closed_does_not_imply_complete():
    pair = (PositionLifecycle.CLOSED, FinancialTruthLifecycle.INCOMPLETE)
    assert pair != (PositionLifecycle.CLOSED, FinancialTruthLifecycle.COMPLETE)


def test_complete_requires_all_authoritative_evidence():
    incomplete = FinancialTruthEvidence(
        executed_entry_qty=Decimal("1"),
        executed_exit_qty=Decimal("1"),
        remaining_qty=Decimal("0"),
        authoritative_gross_pnl=Decimal("2"),
    )
    with pytest.raises(ValueError, match="COMPLETE requires authoritative evidence"):
        validate_financial_truth(FinancialTruthLifecycle.COMPLETE, incomplete)

    validate_financial_truth(
        FinancialTruthLifecycle.COMPLETE,
        complete_evidence(),
    )


def test_estimates_never_replace_authoritative_values():
    values = financial_truth_api_values(
        authoritative_gross_pnl=None,
        authoritative_net_pnl=None,
        estimated_gross_pnl=Decimal("2.50"),
        estimated_net_pnl=Decimal("2.10"),
    )
    assert values["authoritative_gross_pnl"] is None
    assert values["authoritative_net_pnl"] is None
    assert values["estimated_gross_pnl"] == Decimal("2.50")
    assert values["estimated_net_pnl"] == Decimal("2.10")


def test_migration_has_no_backfill_or_runtime_writer():
    sql = MIGRATION.read_text()
    upper = sql.upper()
    assert "CREATE TABLE IF NOT EXISTS CANONICAL_FINANCIAL_TRUTH_V1" in upper
    assert "CREATE OR REPLACE VIEW V_CANONICAL_FINANCIAL_TRUTH_V1" in upper
    assert "INSERT INTO CANONICAL_FINANCIAL_TRUTH_V1" not in upper
    assert "UPDATE CANONICAL_FINANCIAL_TRUTH_V1" not in upper
    assert "DELETE FROM" not in upper
    assert "CREATE TRIGGER" not in upper


def test_complete_constraint_does_not_use_estimates():
    sql = " ".join(MIGRATION.read_text().split())
    constraint = sql.split(
        "CONSTRAINT ck_canonical_financial_truth_complete_v1", 1
    )[1].split(
        "CONSTRAINT ck_canonical_financial_truth_failed_v1", 1
    )[0]
    assert "authoritative_gross_pnl IS NOT NULL" in constraint
    assert "authoritative_net_pnl IS NOT NULL" in constraint
    assert "estimated_gross_pnl" not in constraint
    assert "estimated_net_pnl" not in constraint


def test_api_contract_exposes_authoritative_and_estimated_values_separately():
    source = API.read_text()
    endpoint = source.split(
        '"/financial-truth/positions/{position_id}"', 1
    )[1].split('@app.get("/ops/live-attempts")', 1)[0]
    for field in (
        "authoritative_gross_pnl",
        "authoritative_net_pnl",
        "estimated_gross_pnl",
        "estimated_net_pnl",
        "financial_truth_status",
        "position_status",
    ):
        assert field in endpoint
    assert "COALESCE(authoritative_gross_pnl" not in endpoint
    assert "COALESCE(authoritative_net_pnl" not in endpoint
    assert "None if value is None else float(value)" in endpoint


def test_unknown_view_does_not_fabricate_authoritative_numbers():
    sql = " ".join(MIGRATION.read_text().split())
    view = sql.split("CREATE OR REPLACE VIEW v_canonical_financial_truth_v1", 1)[1]
    assert "COALESCE(ft.financial_truth_status, 'UNKNOWN')" in view
    assert "ft.authoritative_gross_pnl" in view
    assert "ft.authoritative_net_pnl" in view
    assert "COALESCE(ft.authoritative_gross_pnl" not in view
    assert "COALESCE(ft.authoritative_net_pnl" not in view

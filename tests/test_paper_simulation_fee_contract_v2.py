from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import inspect
from pathlib import Path

import pytest

import common.simulated_execution_evidence as execution
from common.financial_truth_calculator import FillEvidence, calculate_financial_truth
from common.financial_truth_repository import CanonicalFinancialTruthWriteRepository
from common.paper_simulation_fee_config import (
    FEE_MODEL_V1,
    FEE_MODEL_V2,
    load_paper_simulation_fee_config,
)
from common.simulated_execution_evidence import (
    create_simulated_execution_fill_cursor,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260813_paper_simulation_fee_contract_v2.sql"
).read_text()
PAPER_EXAMPLE = (ROOT / ".env.paper.example").read_text()
OKX_PAPER_EXAMPLE = (ROOT / ".env.okx.paper.example").read_text()


class FillCursor:
    def __init__(self, entry_contract_rows=()):
        self.entry_contract_rows = list(entry_contract_rows)
        self.calls = []
        self._result = None

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.calls.append((normalized, params))
        if "FROM simulated_execution_fills_v1" in normalized:
            self._result = list(self.entry_contract_rows)
        elif "INSERT INTO simulated_execution_fills_v1" in normalized:
            self._result = [(901,)]
        else:
            self._result = []

    def fetchall(self):
        return list(self._result or [])

    def fetchone(self):
        rows = list(self._result or [])
        return rows[0] if rows else None


class TruthWriteCursor:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((" ".join(str(query).split()), params))

    def fetchone(self):
        return None


def _write(cur, purpose: str):
    return create_simulated_execution_fill_cursor(
        cur,
        simulated_order_id=101 if purpose == "ENTRY" else 102,
        position_id=77,
        order_purpose=purpose,
        side="BUY" if purpose == "ENTRY" else "SELL",
        symbol="BTCUSDC",
        quantity=Decimal("2"),
        price=Decimal("50") if purpose == "ENTRY" else Decimal("55"),
        account_identity_id=1,
        instrument_snapshot_id=2,
        environment="paper",
        deployment_id="local-paper",
        execution_at=datetime(2026, 8, 13, tzinfo=timezone.utc),
        interval="1m",
        strategy="RSI",
        account_identity_fingerprint="account",
        instrument_metadata_fingerprint="instrument",
    )


def _insert_params(cur):
    return next(
        params for query, params in cur.calls
        if "INSERT INTO simulated_execution_fills_v1" in query
    )


def test_configured_v2_entry_fee_and_stored_provenance(monkeypatch):
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    monkeypatch.setenv("PAPER_FEE_RATE", "0.0004")
    cur = FillCursor()

    assert _write(cur, "ENTRY") == 901
    params = _insert_params(cur)

    assert params[7] == Decimal("100")
    assert params[8] == Decimal("0.3500")
    assert params[10] == Decimal("0.3500")
    assert params[15] == FEE_MODEL_V2
    assert params[16] == Decimal("0.0035")
    assert params[17] == FEE_MODEL_V2
    assert params[18] == "ENV:PAPER_SIMULATION_FEE_RATE"


def test_entry_fill_defers_post_inventory_authorities(monkeypatch):
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    calls = []
    monkeypatch.setattr(
        execution, "deploy_paper_simulated_fill_cursor",
        lambda *args, **kwargs: calls.append("reservation_deployed"),
    )
    monkeypatch.setattr(
        execution, "activate_boundary_for_position_cursor",
        lambda *args, **kwargs: calls.append("boundary_activated") or "INSERTED",
    )
    monkeypatch.setattr(
        execution, "handoff_paper_fill_pre_entry_risk_cursor",
        lambda *args, **kwargs: calls.append("pre_entry_handoff") or "INSERTED",
    )
    monkeypatch.setattr(
        execution, "link_entry_opportunity_position_fail_open_cursor",
        lambda *args, **kwargs: calls.append("position_evidence_linked"),
    )

    assert _write(FillCursor(), "ENTRY") == 901
    assert calls == []


def test_record_flow_canonicalizes_inventory_before_pre_entry_handoff():
    source = inspect.getsource(execution.record_simulated_fill_evidence)

    inventory_update = source.index("apply_inventory_lifecycle_mutation(")
    reservation_deployment = source.index("deploy_paper_simulated_fill_cursor(")
    boundary_activation = source.index("activate_boundary_for_position_cursor(")
    cost_evidence = source.index(
        "link_entry_opportunity_position_fail_open_cursor("
    )
    handoff = source.index("handoff_paper_fill_pre_entry_risk_cursor(")
    duplicate_noop = source.index("if fill_id is None:")

    assert (
        duplicate_noop
        < inventory_update
        < reservation_deployment
        < boundary_activation
        < cost_evidence
        < handoff
    )
    assert "if not is_exit:" in source[inventory_update:reservation_deployment]


def test_exit_uses_frozen_entry_fee_contract(monkeypatch):
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0004")
    cur = FillCursor((
        (
            FEE_MODEL_V2, Decimal("0.0035"), FEE_MODEL_V2,
            "ENV:PAPER_SIMULATION_FEE_RATE", Decimal("100"),
            Decimal("0.3500"),
        ),
    ))

    _write(cur, "EXIT")
    params = _insert_params(cur)

    assert params[7] == Decimal("110")
    assert params[8] == Decimal("0.3850")
    assert params[10] == Decimal("0.3850")
    assert params[16] == Decimal("0.0035")
    assert params[17] == FEE_MODEL_V2
    assert params[18] == "ENV:PAPER_SIMULATION_FEE_RATE"


def _fill(purpose: str, price: str) -> FillEvidence:
    notional = Decimal(price)
    fee = notional * Decimal("0.0035")
    return FillEvidence(
        fill_id=purpose,
        order_id=f"order-{purpose}",
        position_id=77,
        purpose=purpose,
        side="BUY" if purpose == "ENTRY" else "SELL",
        symbol="BTCUSDC",
        quantity=Decimal("1"),
        price=Decimal(price),
        notional=notional,
        fee_quantity=fee,
        fee_asset="USDC",
        authoritative_fee_usdc=fee,
        estimated_fee_usdc=None,
        event_time=datetime(2026, 8, 13, tzinfo=timezone.utc),
        source_authority="SIMULATED_EXECUTION",
        source_exchange="SIMULATOR",
        source_environment="paper",
        source_deployment_id="local-paper",
        account_identity_fingerprint="account",
        instrument_metadata_fingerprint="instrument",
        step_size=Decimal("0.00000001"),
        base_asset="BTC",
        quote_asset="USDC",
        source_version=FEE_MODEL_V2,
        simulation_fee_rate=Decimal("0.0035"),
        fee_model_version=FEE_MODEL_V2,
        fee_config_source="ENV:PAPER_SIMULATION_FEE_RATE",
    )


def test_canonical_ft_gross_fees_net_and_provenance_are_exact():
    result = calculate_financial_truth(
        position_id=77,
        position_status="CLOSED",
        fills=(_fill("ENTRY", "100"), _fill("EXIT", "110")),
    )

    assert result.financial_truth_status == "COMPLETE"
    assert result.authoritative_gross_pnl == Decimal("10")
    assert result.authoritative_entry_fees_usdc == Decimal("0.3500")
    assert result.authoritative_exit_fees_usdc == Decimal("0.3850")
    assert result.authoritative_fees_usdc == Decimal("0.7350")
    assert result.authoritative_net_pnl == Decimal("9.2650")
    assert result.simulation_fee_rate == Decimal("0.0035")
    assert result.fee_model_version == FEE_MODEL_V2
    assert result.fee_config_source == "ENV:PAPER_SIMULATION_FEE_RATE"

    cur = TruthWriteCursor()
    assert CanonicalFinancialTruthWriteRepository.write(
        cur,
        result,
        invocation_type="RUNTIME_PAPER_EXIT",
        invocation_identity="test-v2",
    )
    params = next(
        params for query, params in cur.calls
        if "INSERT INTO canonical_financial_truth_v1" in query
    )
    evidence = params["evidence"].adapted
    assert evidence["simulation_fee_rate"] == "0.0035"
    assert evidence["fee_model_version"] == FEE_MODEL_V2
    assert evidence["fee_config_source"] == "ENV:PAPER_SIMULATION_FEE_RATE"


def test_deterministic_result_and_explicit_legacy_default(monkeypatch):
    first = calculate_financial_truth(
        position_id=77,
        position_status="CLOSED",
        fills=(_fill("ENTRY", "100"), _fill("EXIT", "110")),
    )
    second = calculate_financial_truth(
        position_id=77,
        position_status="CLOSED",
        fills=(_fill("ENTRY", "100"), _fill("EXIT", "110")),
    )
    assert first == second
    assert first.source_fingerprint == second.source_fingerprint

    monkeypatch.delenv("PAPER_SIMULATION_FEE_RATE", raising=False)
    monkeypatch.delenv("PAPER_FEE_RATE", raising=False)
    legacy = load_paper_simulation_fee_config()
    assert legacy.rate == Decimal("0.0004")
    assert legacy.model_version == FEE_MODEL_V1
    assert legacy.config_source == "LEGACY_DEFAULT:0.0004"


def test_invalid_explicit_config_fails_closed(monkeypatch):
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "not-a-rate")
    with pytest.raises(RuntimeError, match="INVALID_PAPER_SIMULATION_FEE_RATE"):
        load_paper_simulation_fee_config()


def test_paper_real_execution_remains_disabled_and_migration_is_forward_only():
    for content in (PAPER_EXAMPLE, OKX_PAPER_EXAMPLE):
        assert "LIVE_ORDERS_ENABLED=0" in content
        assert "OKX_EXECUTION_ENABLED=0" in content
        assert "PAPER_SIMULATION_FEE_RATE=0.0035" in content

    assert "UPDATE public.simulated_execution_fills_v1" not in MIGRATION
    assert "UPDATE public.canonical_financial_truth_v1" not in MIGRATION
    assert "PRE_FEE_MODEL_V2" in MIGRATION
    assert "FEE_MODEL_V2" in MIGRATION

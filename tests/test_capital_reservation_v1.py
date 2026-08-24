from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.capital_reservation import (
    CapitalReservationEvidence,
    accepted_commitment_event,
    deploy_live_entry_fill_cursor,
    paper_account_identity_fingerprint,
)
from common.live_managed_capital import LiveManagedCapitalEvidence
from common.portfolio_state import RealizedEvidence, build_portfolio_state


NOW = datetime(2026, 8, 21, 10, tzinfo=timezone.utc)


def evidence(*, reserved="12.25", internal="7.25", reflected="5"):
    return CapitalReservationEvidence(
        Decimal(reserved), Decimal(internal), Decimal(reflected), 2,
        "CANONICAL", "CANONICAL", NOW, "a" * 64, (),
    )


def live_capital(available="100.125", ord_frozen="5"):
    return LiveManagedCapitalEvidence(
        Decimal("205"), "CANONICAL", Decimal(available), None,
        "INCOMPLETE", None, "NOT_YET_CANONICAL", Decimal("205"),
        Decimal("0"), Decimal("0"), "CANONICAL", NOW, NOW, (),
        Decimal(ord_frozen), Decimal(ord_frozen),
    )


def live_state(reservation, capital=None):
    return build_portfolio_state(
        environment="LIVE", deployment_id="local-live", as_of=NOW,
        baseline=None, realized=RealizedEvidence(0, 0, None, None),
        open_marks=(), historical_peak_managed_equity=Decimal("205"),
        live_capital=capital or live_capital(),
        live_baseline_managed_equity=Decimal("205"), live_baseline_at=NOW,
        reservation_evidence=reservation,
    )


def test_decimal_event_has_exact_accounting_and_no_float_input():
    event = accepted_commitment_event(
        environment="PAPER", deployment_id="local-paper",
        account_identity_fingerprint=paper_account_identity_fingerprint(
            "local-paper"
        ), source_identity="SIMULATED_ORDER:1", symbol="BTCUSDC",
        strategy="RSI", interval="1m",
        requested_notional=Decimal("0.123456789123456789"),
        effective_at=NOW, source_authority="TEST", provenance={},
    )
    assert event.remaining_reserved_notional == Decimal("0.123456789123456789")
    assert event.deployed_notional == event.released_notional == Decimal("0")
    with pytest.raises(ValueError, match="INVALID_DECIMAL"):
        accepted_commitment_event(
            environment="PAPER", deployment_id="local-paper",
            account_identity_fingerprint="a" * 64, source_identity="FLOAT",
            symbol="BTCUSDC", strategy="RSI", interval="1m",
            requested_notional=0.1, effective_at=NOW,
            source_authority="TEST", provenance={},
        )


def test_entry_only_api_rejects_exit_purpose():
    with pytest.raises(TypeError):
        accepted_commitment_event(
            environment="PAPER", deployment_id="local-paper",
            account_identity_fingerprint="a" * 64, source_identity="EXIT:1",
            symbol="BTCUSDC", strategy="RSI", interval="1m",
            requested_notional=Decimal("1"), effective_at=NOW,
            source_authority="TEST", provenance={}, purpose="EXIT",
        )


def test_zero_live_fill_is_explicit_noop_without_deployment_write():
    class Cursor:
        def __init__(self):
            self.queries = []
            self.row = None

        def execute(self, query, params=None):
            self.queries.append(query)
            self.row = ("capital_reservation_event_v1", "v_capital_reservation_current_v1")

        def fetchone(self):
            return self.row

    cursor = Cursor()
    assert deploy_live_entry_fill_cursor(
        cursor, intent_id="11111111-1111-1111-1111-111111111111",
        fill_evidence_id="22222222-2222-2222-2222-222222222222",
        position_id=1, filled_quantity=Decimal("0"),
        cumulative_filled_quantity=Decimal("0"),
        requested_quantity=Decimal("1"), effective_at=NOW,
    ) == "ZERO_FILL_NOOP"
    assert len(cursor.queries) == 1


def test_live_available_subtracts_only_internal_unreflected_once():
    result = live_state(evidence())
    assert result.reserved_capital == Decimal("12.25")
    assert result.available_capital == Decimal("92.875")
    assert result.available_capital_status == "CANONICAL"
    assert result.reserved_capital_status == "CANONICAL"


def test_exchange_reflected_reservation_is_not_subtracted_again():
    result = live_state(evidence(reserved="5", internal="0", reflected="5"))
    assert result.available_capital == Decimal("100.125")


def test_exchange_lock_mismatch_is_explicit_and_fail_closed():
    result = live_state(evidence(), live_capital(ord_frozen="4.99"))
    assert result.reserved_capital is None
    assert result.available_capital is None
    assert result.reserved_capital_status == "RECONCILIATION_FAILED"
    assert "LIVE_EXCHANGE_ORDER_FROZEN_RESERVATION_MISMATCH" in result.incomplete_reasons


def test_reconciliation_failure_fails_closed_without_guessed_zero():
    failed = CapitalReservationEvidence(
        None, None, None, 1, "RECONCILIATION_FAILED",
        "RECONCILIATION_FAILED", NOW, "a" * 64,
        ("CAPITAL_RESERVATION_RECONCILIATION_FAILED",),
    )
    result = live_state(failed, live_capital(ord_frozen="0"))
    assert result.reserved_capital is None
    assert result.available_capital is None
    assert result.reserved_capital_status == "RECONCILIATION_FAILED"


def test_inventory_is_not_reservation_and_paper_available_remains_incomplete():
    result = build_portfolio_state(
        environment="PAPER", deployment_id="local-paper", as_of=NOW,
        baseline=None, realized=RealizedEvidence(0, 0, None, None),
        open_marks=(), historical_peak_managed_equity=None,
        reservation_evidence=evidence(reserved="4", internal="0", reflected="0"),
    )
    assert result.deployed_capital == Decimal("0")
    assert result.reserved_capital == Decimal("4")
    assert result.available_capital is None
    assert result.available_capital_status == "INCOMPLETE"

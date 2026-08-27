from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from common.simulated_execution_evidence import (
    execute_paper_exit_after_preflight,
    paper_exit_preflight_cursor,
)


ADOPTION_SHA = "a" * 40
RUNTIME_SHA = "b" * 40
ADOPTED = datetime(2026, 7, 1, tzinfo=timezone.utc)
ACTIVE = (
    41,
    "FEE_AWARE_INVENTORY_C2_2",
    "paper",
    "local-paper",
    7,
    "ACTIVE",
    ADOPTED,
    ADOPTION_SHA,
)


class Cursor:
    def __init__(
        self, *, position, active=ACTIVE, compatible=False,
        entry_contract_rows=None,
    ):
        self.position = position
        self.active = active
        self.compatible = compatible
        self.entry_contract_rows = (
            [(
                "PAPER_SIMULATOR_FINANCIAL_MODEL_V2", Decimal("0.0035"),
                "PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
                "ENV:PAPER_SIMULATION_FEE_RATE", Decimal("100"),
                Decimal("0.3500"),
            )]
            if entry_contract_rows is None else list(entry_contract_rows)
        )
        self.result = None

    def execute(self, sql, _params=None):
        normalized = " ".join(str(sql).split())
        if "pg_advisory_xact_lock" in normalized:
            self.result = (None,)
        elif "FROM positions" in normalized:
            self.result = self.position
        elif "FROM runtime_contract_adoption_v2" in normalized:
            self.result = self.active
        elif "is_existing_projected_c2_2_compatible" in normalized:
            self.result = (self.compatible,)
        elif "FROM simulated_execution_fills_v1" in normalized:
            self.result = list(self.entry_contract_rows)
        else:
            raise AssertionError(normalized)

    def fetchone(self):
        if isinstance(self.result, list):
            return self.result[0] if self.result else None
        return self.result

    def fetchall(self):
        return list(self.result) if isinstance(self.result, list) else []


def classify(monkeypatch, *, position, active=ACTIVE, compatible=False):
    monkeypatch.setenv("GIT_SHA", RUNTIME_SHA)
    return paper_exit_preflight_cursor(
        Cursor(position=position, active=active, compatible=compatible),
        deployment_id="local-paper", symbol="BTCUSDC", strategy="RSI",
        interval="1m",
    )


@pytest.mark.parametrize(
    ("position", "active", "compatible", "reason"),
    [
        (None, ACTIVE, False, "POSITION_NOT_FOUND"),
        ((77, "CLOSED", 41, 7, ADOPTED), ACTIVE, False,
         "POSITION_ALREADY_CLOSED"),
        ((77, "OPEN", None, None, ADOPTED - timedelta(seconds=1)),
         ACTIVE, False, "ENTRY_BEFORE_ACTIVE_ADOPTION"),
        ((77, "OPEN", None, 7, ADOPTED), ACTIVE, False,
         "MISSING_ADOPTION_ID"),
        ((77, "OPEN", 41, None, ADOPTED), ACTIVE, False,
         "MISSING_GENERATION"),
        ((77, "OPEN", 40, 6, ADOPTED), ACTIVE, False,
         "GENERATION_MISMATCH"),
        ((77, "OPEN", None, None, None), ACTIVE, False,
         "LEGACY_NOT_COMPATIBLE"),
        ((77, "OPEN", 41, 7, ADOPTED), None, False,
         "INVENTORY_CONTRACT_INCOMPLETE"),
    ],
)
def test_diagnostic_reason_matrix(
    monkeypatch, position, active, compatible, reason
):
    result = classify(
        monkeypatch, position=position, active=active, compatible=compatible
    )
    assert result.allowed is False
    assert result.reason_code == reason


@pytest.mark.parametrize(
    ("position", "compatible", "reason"),
    [
        ((77, "OPEN", 41, 7, ADOPTED), False, "ACTIVE_GENERATION_MATCH"),
        ((77, "OPEN", None, None, ADOPTED - timedelta(days=1)), True,
         "LEGACY_COMPATIBLE"),
        ((77, "OPEN", None, None, ADOPTED + timedelta(seconds=1)), False,
         "FORWARD_ENTRY_COMPATIBLE"),
    ],
)
def test_existing_guard_allow_semantics_are_preserved(
    monkeypatch, position, compatible, reason
):
    result = classify(monkeypatch, position=position, compatible=compatible)
    assert result.allowed is True
    assert result.reason_code == reason
    assert result.active_adoption_git_revision == ADOPTION_SHA
    assert result.runtime_git_revision == RUNTIME_SHA
    assert result.runtime_revision_matches_adoption_provenance is False


def test_forward_exit_without_frozen_entry_fee_contract_fails_closed(monkeypatch):
    monkeypatch.setenv("GIT_SHA", RUNTIME_SHA)
    result = paper_exit_preflight_cursor(
        Cursor(
            position=(77, "OPEN", None, None, ADOPTED + timedelta(seconds=1)),
            entry_contract_rows=(),
        ),
        deployment_id="local-paper", symbol="BTCUSDC", strategy="RSI",
        interval="1m",
    )

    assert result.allowed is False
    assert result.reason_code == "PAPER_EXIT_ENTRY_FEE_CONTRACT_MISSING"
    assert result.detail == "PAPER_EXIT_REQUIRES_FROZEN_ENTRY_FEE_CONTRACT"


def test_legacy_entry_contract_is_preserved_without_current_config_fallback(
    monkeypatch,
):
    monkeypatch.setenv("GIT_SHA", RUNTIME_SHA)
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    result = paper_exit_preflight_cursor(
        Cursor(
            position=(77, "OPEN", None, None, ADOPTED + timedelta(seconds=1)),
            entry_contract_rows=((
                "PAPER_SIMULATOR_FINANCIAL_MODEL_V1", None, None, None,
                Decimal("100"), Decimal("0.0400"),
            ),),
        ),
        deployment_id="local-paper", symbol="BTCUSDC", strategy="RSI",
        interval="1m",
    )

    assert result.allowed is True
    assert result.reason_code == "FORWARD_ENTRY_COMPATIBLE"


@pytest.mark.parametrize(
    "active",
    [
        ACTIVE[:2] + ("live",) + ACTIVE[3:],
        ACTIVE[:3] + ("wrong-paper",) + ACTIVE[4:],
        ACTIVE[:5] + ("SUPERSEDED",) + ACTIVE[6:],
    ],
)
def test_wrong_scope_or_inactive_adoption_fails_closed(monkeypatch, active):
    result = classify(
        monkeypatch,
        position=(77, "OPEN", 41, 7, ADOPTED),
        active=active,
    )
    assert result.allowed is False
    assert result.reason_code == "INVENTORY_CONTRACT_INCOMPLETE"


@pytest.mark.parametrize("position_id", [10326, 10333, 10340])
def test_known_legacy_targets_remain_denied_with_newer_runtime(
    monkeypatch, position_id
):
    result = classify(
        monkeypatch,
        position=(
            position_id,
            "OPEN",
            None,
            None,
            ADOPTED - timedelta(seconds=1),
        ),
        compatible=False,
    )
    assert result.allowed is False
    assert result.reason_code == "ENTRY_BEFORE_ACTIVE_ADOPTION"
    assert result.runtime_revision_matches_adoption_provenance is False


@pytest.mark.parametrize("position_id", [10491, 10493, 10495, 10496, 10498])
def test_known_generation_five_positions_allow_newer_runtime(
    monkeypatch, position_id
):
    active_generation_five = ACTIVE[:4] + (5,) + ACTIVE[5:]
    result = classify(
        monkeypatch,
        position=(position_id, "OPEN", 41, 5, ADOPTED),
        active=active_generation_five,
    )
    assert result.allowed is True
    assert result.reason_code == "ACTIVE_GENERATION_MATCH"
    assert result.runtime_revision_matches_adoption_provenance is False


def test_denial_emits_diagnostic_and_never_runs_execution_action(monkeypatch):
    events = []
    action_calls = []
    denied = SimpleNamespace(
        allowed=False, reason_code="GENERATION_MISMATCH", position_id=77,
        event_fields=lambda: {
            "position_id": 77, "position_status": "OPEN",
            "position_adoption_id": 40, "position_generation": 6,
            "active_adoption_id": 41, "active_generation": 7,
            "legacy_compatibility": False,
        },
    )

    class Guard:
        def __enter__(self):
            return denied

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        "common.simulated_execution_evidence.paper_exit_preflight_guard",
        lambda *_args, **_kwargs: Guard(),
    )
    result = execute_paper_exit_after_preflight(
        lambda: None, deployment_id="local-paper", symbol="BTCUSDC",
        strategy="RSI", interval="1m", exit_trigger="STOP_LOSS",
        decision="SELL", price=99.0, candle_open_time=ADOPTED,
        emit_event=lambda **event: events.append(event),
        action=lambda _result: action_calls.append(True),
    )
    assert action_calls == []
    assert result["ledger_ok"] is False
    assert result["preflight_reason_code"] == "GENERATION_MISMATCH"
    assert len(events) == 1
    assert events[0]["event_type"] == "PAPER_EXIT_PREFLIGHT_BLOCKED"
    assert events[0]["reason"] == "GENERATION_MISMATCH"


def test_allowed_preflight_returns_existing_execution_result_unchanged(monkeypatch):
    expected = {
        "gross_executed_qty": "0.10000000",
        "base_fee_qty": "0.00004000",
        "quote_fee": "0.00400000",
        "net_entry_inventory": "0.09996000",
        "exit_inventory_reduction": "0.09996000",
        "remaining_inventory": "0",
        "gross_pnl": "1.00000000",
        "fees": "0.00800000",
        "net_pnl": "0.99200000",
        "financial_truth_status": "COMPLETE",
    }
    allowed = SimpleNamespace(allowed=True, position_id=77)

    class Guard:
        def __enter__(self):
            return allowed

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        "common.simulated_execution_evidence.paper_exit_preflight_guard",
        lambda *_args, **_kwargs: Guard(),
    )
    calls = []
    result = execute_paper_exit_after_preflight(
        lambda: None, deployment_id="local-paper", symbol="BTCUSDC",
        strategy="RSI", interval="1m", exit_trigger="TAKE_PROFIT",
        decision="SELL", price=101.0, candle_open_time=ADOPTED,
        emit_event=lambda **_event: pytest.fail("unexpected blocked event"),
        action=lambda preflight: calls.append(preflight.position_id) or expected,
    )
    assert calls == [77]
    assert result is expected

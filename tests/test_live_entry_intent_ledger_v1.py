from __future__ import annotations

import uuid
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.entry_intent import (
    EntryIntentContractVersion,
    EntryIntentDeployment,
    EntryIntentEnvironment,
    EntryIntentInsertOutcome,
    EntryIntentOrderPurpose,
    EntryIntentSide,
    LiveEntryIntent,
    canonical_decimal,
    classify_insert_outcome,
)


GIT_REVISION = "7" * 40
DECISION_ID = uuid.UUID("91cc3845-137f-5d2d-9e4e-c18e8e973653")
PREPARED_AT = datetime(2026, 7, 30, 11, 7, 5, tzinfo=timezone.utc)


def intent(**changes) -> LiveEntryIntent:
    values = {
        "environment": EntryIntentEnvironment.LIVE,
        "deployment_id": EntryIntentDeployment.LOCAL_LIVE,
        "git_revision": GIT_REVISION,
        "adoption_id": 1,
        "generation": 1,
        "decision_id": DECISION_ID,
        "symbol": "BNBUSDC",
        "strategy": "TREND",
        "interval": "1m",
        "exchange_source": "okx",
        "client_order_id": "ORC-L-BNBUSDC-TREN-1m-E-regression",
        "requested_qty": Decimal("0.033895"),
        "prepared_at": PREPARED_AT,
        "producer_identity": "bot-trend",
    }
    values.update(changes)
    return LiveEntryIntent.build(**values)


@pytest.mark.parametrize("value", ["trading_paper", "LOCAL", "VPS", "LIVE"])
def test_environment_rejects_noncanonical_aliases(value):
    with pytest.raises(ValueError):
        intent(environment=value)


@pytest.mark.parametrize(
    "value", ["trading_paper", "local", "VPS", "LOCAL-LIVE"]
)
def test_deployment_rejects_noncanonical_aliases(value):
    with pytest.raises(ValueError):
        intent(deployment_id=value)


def test_environment_must_match_deployment():
    with pytest.raises(ValueError, match="does not match"):
        intent(
            environment=EntryIntentEnvironment.PAPER,
            deployment_id=EntryIntentDeployment.LOCAL_LIVE,
        )


def test_contract_is_entry_buy_only():
    row = intent()
    assert row.order_purpose is EntryIntentOrderPurpose.ENTRY
    assert row.side is EntryIntentSide.BUY
    assert row.contract_version is EntryIntentContractVersion.V1
    with pytest.raises(ValueError):
        replace(row, order_purpose="EXIT")
    with pytest.raises(ValueError):
        replace(row, side="SELL")


@pytest.mark.parametrize("quantity", ["0", "-0.1", "NaN", "Infinity"])
def test_quantity_must_be_positive_finite_decimal(quantity):
    with pytest.raises(ValueError):
        intent(requested_qty=quantity)


def test_decimal_serialization_is_canonical_and_float_free():
    assert canonical_decimal(Decimal("0.1000")) == "0.1"
    assert canonical_decimal("1.230000") == "1.23"
    assert canonical_decimal("0.00000100") == "0.000001"
    with pytest.raises(ValueError, match="binary float"):
        canonical_decimal(0.1)  # type: ignore[arg-type]


def test_deterministic_uuid_and_fingerprint_are_retry_stable():
    first = intent(requested_qty=Decimal("0.0338950"))
    second = intent(requested_qty=Decimal("0.03389500"))
    assert first.intent_id == second.intent_id
    assert first.content_fingerprint == second.content_fingerprint
    assert len(first.content_fingerprint) == 64


def test_natural_identity_change_changes_uuid():
    first = intent()
    second = intent(client_order_id=f"{first.client_order_id}-other")
    assert first.intent_id != second.intent_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation", 2),
        ("decision_id", uuid.UUID("bd68f573-fe57-570b-a49e-e520b43aa3e3")),
        ("requested_qty", Decimal("0.033896")),
        ("producer_identity", "bot-trend-other"),
    ],
)
def test_semantic_change_changes_fingerprint_not_natural_identity(field, value):
    first = intent()
    second = intent(**{field: value})
    assert first.intent_id == second.intent_id
    assert first.content_fingerprint != second.content_fingerprint


def test_prepared_at_is_metadata_not_semantic_retry_content():
    first = intent()
    second = intent(
        prepared_at=datetime(2026, 7, 30, 11, 7, 6, tzinfo=timezone.utc)
    )
    assert first.intent_id == second.intent_id
    assert first.content_fingerprint == second.content_fingerprint


def test_model_is_immutable():
    row = intent()
    with pytest.raises(FrozenInstanceError):
        row.requested_qty = Decimal("9")  # type: ignore[misc]


def test_natural_key_insert_outcomes_are_explicit():
    row = intent()
    assert (
        classify_insert_outcome(None, row)
        is EntryIntentInsertOutcome.CREATED
    )
    assert classify_insert_outcome(
        row.content_fingerprint, row
    ) is EntryIntentInsertOutcome.IDEMPOTENT_EXISTING
    assert classify_insert_outcome(
        "0" * 64, row
    ) is EntryIntentInsertOutcome.CONFLICT


def test_slot_identity_is_canonical_and_self_validating():
    row = intent()
    assert row.slot_identity == "BNBUSDC:TREND:1m"
    with pytest.raises(ValueError, match="slot_identity"):
        replace(row, slot_identity="BNBUSDC:TREND:5m")


def test_fingerprint_tampering_is_rejected():
    with pytest.raises(ValueError, match="content_fingerprint"):
        replace(intent(), content_fingerprint="0" * 64)

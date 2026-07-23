from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.orc_apply_ledger import (
    EXECUTION_MODE_APPLY,
    EXECUTION_MODE_OBSERVE_ONLY,
    OrcObserveOnlyGuardError,
    WRITER_VERSION,
    WriterIdentity,
    canonical_json,
    deterministic_picks_hash,
    make_slot_decision,
    parse_required_execution_guard,
    insert_slot_decision,
    LedgerSlotCountMismatch,
    stable_hash,
    validate_slot_counts,
    resolve_execution_mode,
)


ON_REASON = "ORC_INTEGRATION_V2: V7 readiness + MME context picked (entries ON, ENFORCE)"
OFF_REASON = "ORC_INTEGRATION_V2: not ready, late/exhausted, or not picked (entries OFF, DRY_RUN)"
TEST_GIT_SHA = "d6d9c6cc50cc4f7a066445dd5d2cd0ea92264dc3"


@pytest.fixture(autouse=True)
def immutable_writer_metadata(monkeypatch):
    monkeypatch.setenv("GIT_SHA", TEST_GIT_SHA)


def control(live: bool, **overrides):
    row = {
        "symbol": "SOLUSDC", "interval": "1m", "strategy": "TREND",
        "enabled": True, "live_orders_enabled": live,
        "regime_enabled": True,
        "regime_mode": "ENFORCE" if live else "DRY_RUN",
        "reason": ON_REASON if live else OFF_REASON,
        "control_mode": "AUTO", "control_source": "ORC",
        "manual_override_reason": None,
        "manual_override_updated_at": None,
        "live_since": None, "last_disabled_at": None,
        "updated_at": datetime(2026, 7, 20, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def source(**overrides):
    row = {
        "eligible_v63": True, "picked_v63_now": True,
        "v63_reason": "PICK_CORE_NET_AWARE", "v63_score": Decimal("0.35"),
        "n_trades_3d": Decimal("7"), "net_sum_3d": Decimal("0.16012334"),
        "profit_factor_3d": Decimal("999"), "orc_v7_ready": True,
        "readiness_reason": "READY", "v7_reason": "READY",
        "mme_orc_avoid": False, "mme_remaining_score": Decimal("45"),
        "mme_exhaustion_risk": Decimal("40"), "context_v2_ready_now": True,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "before,want,transition,touched",
    [
        (False, True, "ENABLED", True),
        (True, False, "DISABLED", True),
        (True, True, "RETAINED_ON", False),
        (False, False, "RETAINED_OFF", False),
    ],
)
def test_transition_matrix(before, want, transition, touched):
    row = make_slot_decision(
        control(before), source(), want_on=want,
        pick_source="ORC_V6_3" if want else None,
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    assert row["transition_type"] == transition
    assert row["touched"] is touched
    assert row["resulting_live"] is want


def test_two_desired_slots_only_one_touched():
    retained = make_slot_decision(
        control(True, symbol="ETHUSDC"), source(), want_on=True,
        pick_source="ORC_V6_3", on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    enabled = make_slot_decision(
        control(False), source(), want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    assert sum(row["want_on"] for row in (retained, enabled)) == 2
    assert sum(row["touched"] for row in (retained, enabled)) == 1


def test_empty_pick_set_has_empty_hash_and_retained_off():
    row = make_slot_decision(
        control(False), None, want_on=False, pick_source=None,
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    assert deterministic_picks_hash([row]) == ""
    assert row["transition_type"] == "RETAINED_OFF"


def test_snapshot_is_point_in_time_and_hash_does_not_follow_mutable_source():
    mutable = source(net_sum_3d=Decimal("1.25"))
    decision = make_slot_decision(
        control(False), mutable, want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    frozen = deepcopy(decision["snapshot"])
    original_hash = decision["snapshot_hash"]
    mutable["net_sum_3d"] = Decimal("-99")
    assert decision["snapshot"] == frozen
    assert decision["snapshot_hash"] == original_hash


def test_hashes_are_canonical_and_slot_order_independent():
    left = {"b": Decimal("2.00"), "a": [1, "ż"]}
    right = {"a": [1, "ż"], "b": Decimal("2.00")}
    assert canonical_json(left) == canonical_json(right)
    assert stable_hash(left) == stable_hash(right)
    one = make_slot_decision(
        control(False), source(), want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    two = dict(one, symbol="ETHUSDC", slot_key="ETHUSDC|1m|TREND")
    assert deterministic_picks_hash([one, two]) == deterministic_picks_hash([two, one])


def test_reason_only_change_is_a_touch_without_changing_transition():
    row = make_slot_decision(
        control(True, reason="legacy reason"), source(), want_on=True,
        pick_source="ORC_V6_3", on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    assert row["transition_type"] == "RETAINED_ON"
    assert row["touched"] is True


@pytest.mark.parametrize(
    "deployment,mode,environment",
    [
        ("local-live", "LIVE", "trading_live"),
        ("local-paper", "PAPER", "trading_paper"),
        ("vps-live", "LIVE", "trading_live"),
        ("vps-paper", "PAPER", "trading_paper"),
    ],
)
def test_environment_deployment_isolation(monkeypatch, deployment, mode, environment):
    monkeypatch.setenv("DEPLOYMENT_ID", deployment)
    identity = WriterIdentity.from_env(mode)
    assert identity.deployment_id == deployment
    assert identity.environment == environment


def test_deployment_identity_fails_closed(monkeypatch):
    monkeypatch.delenv("DEPLOYMENT_ID", raising=False)
    with pytest.raises(ValueError, match="DEPLOYMENT_ID"):
        WriterIdentity.from_env("LIVE")


def test_writer_metadata_is_required_and_immutable(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.delenv("GIT_SHA")
    monkeypatch.setenv("COMMIT_SHA", TEST_GIT_SHA)
    monkeypatch.setenv("ORC_WRITER_VERSION", "mutable-runtime-value")
    with pytest.raises(ValueError, match="GIT_SHA"):
        WriterIdentity.from_env("LIVE")

    monkeypatch.setenv("GIT_SHA", TEST_GIT_SHA.upper())
    identity = WriterIdentity.from_env("LIVE")
    assert identity.git_sha == TEST_GIT_SHA
    assert identity.version == WRITER_VERSION
    assert identity.version == "ORC_APPLY_WRITER_V1_3"


@pytest.mark.parametrize("invalid_sha", ["", "d6d9c6c", "g" * 40, "a" * 39, "a" * 41])
def test_writer_rejects_invalid_build_sha(monkeypatch, invalid_sha):
    monkeypatch.setenv("DEPLOYMENT_ID", "vps-live")
    monkeypatch.setenv("GIT_SHA", invalid_sha)
    with pytest.raises(ValueError, match="GIT_SHA"):
        WriterIdentity.from_env("LIVE")


def test_retry_identity_is_stable_across_mutable_writer_env(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "vps-live")
    monkeypatch.setenv("ORC_WRITER_VERSION", "first")
    first = WriterIdentity.from_env("LIVE")
    monkeypatch.setenv("ORC_WRITER_VERSION", "second")
    monkeypatch.setenv("COMMIT_SHA", "f" * 40)
    second = WriterIdentity.from_env("LIVE")
    assert first == second
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    with pytest.raises(ValueError, match="TRADING_MODE"):
        WriterIdentity.from_env("LIVE")


@pytest.mark.parametrize(
    "placeholder",
    [
        "REQUIRED_SET_LOCAL_LIVE_OR_VPS_LIVE",
        "REQUIRED_SET_LOCAL_PAPER_OR_VPS_PAPER",
    ],
)
def test_deployment_identity_rejects_example_placeholders(monkeypatch, placeholder):
    monkeypatch.setenv("DEPLOYMENT_ID", placeholder)
    with pytest.raises(ValueError, match="DEPLOYMENT_ID"):
        WriterIdentity.from_env("LIVE")


def test_learning_apply_contract_remains_outside_ledger():
    # The ledger records existing ORC decisions; it has no Learning action field.
    row = make_slot_decision(
        control(False), source(), want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )
    assert "learning" not in canonical_json(row["snapshot"]).lower()


def test_slot_insert_sql_parameter_cardinality(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    identity = WriterIdentity.from_env("LIVE")
    row = make_slot_decision(
        control(False), source(), want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
    )

    class Cursor:
        rowcount = 1
        def execute(self, sql, params):
            assert sql.count("%s") == len(params)
            assert len(params) == 57

    assert insert_slot_decision(
        Cursor(), "11111111-1111-4111-8111-111111111111", identity, row
    ) == 1


@pytest.mark.parametrize("source_count", [32, 28])
def test_valid_counter_semantics(source_count):
    assert validate_slot_counts(source_count, 28, 28, 28) == source_count - 28


def test_prepared_slot_mismatch_fails_closed():
    with pytest.raises(LedgerSlotCountMismatch, match="prepared_slot_count"):
        validate_slot_counts(28, 28, 27)


def test_inserted_slot_mismatch_fails_closed():
    with pytest.raises(LedgerSlotCountMismatch, match="inserted_slot_count") as exc:
        validate_slot_counts(28, 28, 28, 27)
    assert exc.value.error_classification == "LEDGER_SLOT_COUNT_MISMATCH"


def test_negative_source_excluded_count_fails_closed():
    with pytest.raises(LedgerSlotCountMismatch, match="source_candidate_count"):
        validate_slot_counts(27, 28, 28)


@pytest.mark.parametrize(
    "before,want,effect",
    [
        (False, True, "WOULD_ENABLE"),
        (True, False, "WOULD_DISABLE"),
        (True, True, "WOULD_RETAIN_ON"),
        (False, False, "WOULD_RETAIN_OFF"),
    ],
)
def test_observe_only_separates_desired_decision_from_actual_effect(
    before, want, effect
):
    row = make_slot_decision(
        control(before), source(), want_on=want,
        pick_source="ORC_V6_3" if want else None,
        on_reason=ON_REASON, off_reason=OFF_REASON,
        execution_mode=EXECUTION_MODE_OBSERVE_ONLY,
    )
    assert row["decision_effect"] == effect
    assert row["transition_type"] == effect
    assert row["resulting_live"] is before
    assert row["touched"] is False
    assert row["state_changed"] is False


def test_apply_decision_effect_preserves_live_semantics():
    row = make_slot_decision(
        control(False), source(), want_on=True, pick_source="ORC_V6_3",
        on_reason=ON_REASON, off_reason=OFF_REASON,
        execution_mode=EXECUTION_MODE_APPLY,
    )
    assert row["decision_effect"] == "APPLIED_ENABLE"
    assert row["transition_type"] == "ENABLED"
    assert row["resulting_live"] is True
    assert row["touched"] is True


def test_paper_observe_only_flag_is_default_off(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    identity = WriterIdentity.from_env("PAPER")
    assert resolve_execution_mode(
        identity, "PAPER", observe_only_enabled=False,
        live_orders_enabled=False, execution_enabled=False,
    ) is None


def test_paper_observe_only_requires_both_execution_guards(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
    identity = WriterIdentity.from_env("PAPER")
    assert resolve_execution_mode(
        identity, "PAPER", observe_only_enabled=True,
        live_orders_enabled=False, execution_enabled=False,
    ) == EXECUTION_MODE_OBSERVE_ONLY
    for live_orders, execution in ((True, False), (False, True), (True, True)):
        with pytest.raises(OrcObserveOnlyGuardError):
            resolve_execution_mode(
                identity, "PAPER", observe_only_enabled=True,
                live_orders_enabled=live_orders, execution_enabled=execution,
            )


def test_live_apply_does_not_depend_on_observe_only_flag(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    identity = WriterIdentity.from_env("LIVE")
    for flag in (False, True):
        assert resolve_execution_mode(
            identity, "LIVE", observe_only_enabled=flag,
            live_orders_enabled=True, execution_enabled=True,
        ) == EXECUTION_MODE_APPLY


@pytest.mark.parametrize("value", [None, "", "maybe", "required"])
def test_paper_execution_guards_reject_ambiguous_values(value):
    with pytest.raises(OrcObserveOnlyGuardError):
        parse_required_execution_guard("OKX_EXECUTION_ENABLED", value)


@pytest.mark.parametrize("value,expected", [("0", False), ("false", False), ("1", True), ("true", True)])
def test_paper_execution_guards_accept_only_explicit_booleans(value, expected):
    assert parse_required_execution_guard("guard", value) is expected

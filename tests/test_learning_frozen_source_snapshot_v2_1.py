from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/"
    "20260724_learning_frozen_source_snapshot_v2_1_payload_propagation.sql"
).read_text()
HARNESS = (
    ROOT
    / "tests/postgres/"
    "learning_frozen_source_snapshot_v2_1_payload_propagation.sql"
).read_text()
V2 = (
    ROOT
    / "db/migrations/20260724_learning_canonical_frozen_source_snapshot_v2.sql"
).read_text()


def test_corrective_migration_is_additive_and_idempotent():
    assert "BEGIN;" in MIGRATION
    assert "COMMIT;" in MIGRATION
    assert "CREATE OR REPLACE FUNCTION propagate_" not in MIGRATION
    assert "source_snapshot_token = COALESCE(" in MIGRATION
    assert "current_setting(" in MIGRATION
    assert "LEARNING_FROZEN_SOURCE_V2_1_PREREQUISITE_MISSING" in MIGRATION
    assert "LEARNING_FROZEN_SOURCE_V2_1_PROPAGATION_PATCH_MISSING" in MIGRATION


def test_only_current_state_upserts_receive_explicit_provenance():
    assert "learning_slot_statistics_v1.source_snapshot_token" in MIGRATION
    assert "learning_calibration_proposals_v1.source_snapshot_token" in MIGRATION
    for immutable_or_run_keyed in (
        "learning_proposal_observations_v1.source_snapshot_token",
        "learning_canonical_evidence_selection_v1.source_snapshot_token",
        "learning_evidence_manifests_v1.source_snapshot_token",
        "learning_canonical_source_snapshot_rows_v2.source_snapshot_token",
    ):
        assert immutable_or_run_keyed not in MIGRATION


def test_original_fail_closed_guard_is_preserved():
    assert "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT" in V2
    assert "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT" in MIGRATION
    assert "LEARNING_FROZEN_SOURCE_V2_1_FAIL_CLOSED_GUARD_CHANGED" in MIGRATION
    for forbidden in (
        "DROP TRIGGER",
        "DISABLE TRIGGER",
        "ON CONFLICT DO NOTHING",
        "DELETE FROM",
        "TRUNCATE",
        "CASCADE",
    ):
        assert forbidden not in MIGRATION


def test_postgres_harness_reproduces_exact_vps_error_before_fix():
    assert (
        "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT "
        "table=learning_slot_statistics_v1"
    ) in HARNESS
    assert "EXPECTED_VPS_PAYLOAD_CONFLICT_NOT_RAISED" in HARNESS
    assert "FAILED_CONFLICT_LEFT_PARTIAL_STATE" in HARNESS
    assert HARNESS.count(
        "20260724_learning_frozen_source_snapshot_v2_1_payload_propagation.sql"
    ) == 2


def test_postgres_harness_covers_retry_identity_and_true_conflict():
    for contract in (
        "CURRENT_STATE_TOKEN_NOT_ROLLED_FORWARD",
        "SAME_DECISION_IDENTITY_DUPLICATED",
        "DISTINCT_DECISION_IDENTITY_NOT_INSERTED",
        "COMPLETE_SNAPSHOT_CHANGED",
        "TRUE_PAYLOAD_CONFLICT_NOT_RAISED",
    ):
        assert contract in HARNESS


def test_patch_does_not_change_learning_or_trading_policy():
    combined = MIGRATION + HARNESS
    for forbidden in (
        "bot_control",
        "execution",
        "sizing",
        "risk",
        "ORC_",
        "TREATMENT",
        "PAPER_EXPERIMENT",
    ):
        assert forbidden not in combined

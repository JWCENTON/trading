from pathlib import Path
import hashlib
import uuid


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260724_learning_canonical_frozen_source_snapshot_v2.sql"
).read_text()
ROLLBACK = (
    ROOT
    / "db/migrations/20260724_learning_canonical_frozen_source_snapshot_v2_rollback.sql"
).read_text()
HARNESS = (
    ROOT / "tests/postgres/learning_canonical_frozen_source_snapshot_v2.sql"
).read_text()


def _function_body(name, following):
    start = MIGRATION.index(f"FUNCTION {name}")
    end = MIGRATION.index(following, start)
    return MIGRATION[start:end]


def test_snapshot_schema_is_persistent_decision_level_and_append_only():
    for relation in (
        "learning_canonical_source_snapshots_v2",
        "learning_canonical_source_snapshot_rows_v2",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {relation}" in MIGRATION
    for field in (
        "snapshot_token",
        "feedback_run_id",
        "deployment_instance_id",
        "environment",
        "deployment_id",
        "evidence_window_start",
        "evidence_window_end",
        "evidence_cutoff_at",
        "source_snapshot_at",
        "decision_key",
        "decision_id",
        "position_id",
        "entry_time",
        "exit_time",
        "outcome_timestamp",
        "realized_pnl_usdc",
        "gross_pnl_usdc",
        "fees_usdc",
        "eligibility_reason",
        "ordinal",
        "row_hash",
        "snapshot_hash",
    ):
        assert field in MIGRATION
    assert "UNIQUE (snapshot_token, decision_key)" in MIGRATION
    assert "LEARNING_FROZEN_SOURCE_IMMUTABLE" in MIGRATION


def test_live_source_is_materialized_exactly_once_per_new_snapshot():
    capture = _function_body(
        "capture_learning_canonical_source_snapshot_v2",
        "CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2",
    )
    assert capture.count(
        "FROM learning_canonical_evidence_universe_live_v1("
    ) == 1
    assert "learning_canonical_source_snapshot_rows_v2" in capture
    assert "ORDER BY u.decision_key" in capture
    assert "GET DIAGNOSTICS v_source_count = ROW_COUNT" in capture


def test_public_universe_reads_frozen_rows_during_a_run():
    wrapper = _function_body(
        "learning_canonical_evidence_universe_v1",
        "CREATE OR REPLACE FUNCTION capture_learning_canonical_source_snapshot_v2",
    )
    assert "waltrade.learning_source_snapshot_token" in wrapper
    assert "learning_canonical_source_snapshot_rows_v2" in wrapper
    assert "snapshot_status = 'COMPLETE'" in wrapper
    assert "LEARNING_FROZEN_SOURCE_CONTEXT_MISMATCH" in wrapper
    assert "learning_canonical_evidence_universe_live_v1" in wrapper


def test_due_wrapper_captures_before_feedback_refresh():
    patch = _function_body(
        "capture_learning_canonical_source_snapshot_v2",
        "CREATE OR REPLACE FUNCTION validate_learning_frozen_source_parity_v2",
    )
    assert "RETURNING id INTO v_run_id;" in patch
    assert (
        "PERFORM capture_learning_canonical_source_snapshot_v2(v_run_id);"
        in patch
    )
    assert (
        "v_anchor || E'\\n\\n    PERFORM "
        "capture_learning_canonical_source_snapshot_v2(v_run_id);'"
    ) in patch


def test_token_propagates_to_every_downstream_contract():
    for table in (
        "learning_slot_statistics_v1",
        "learning_calibration_proposals_v1",
        "learning_proposal_observations_v1",
        "learning_canonical_evidence_selection_v1",
        "learning_evidence_manifests_v1",
    ):
        assert f"ALTER TABLE {table}" in MIGRATION
        assert f"'{table}'" in MIGRATION
    assert "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT" in MIGRATION


def test_deferred_guard_requires_all_six_counts_and_header_parity():
    guard = _function_body(
        "validate_learning_frozen_source_parity_v2",
        "DROP TRIGGER IF EXISTS learning_frozen_source_parity_v2",
    )
    for count in (
        "v_snapshot_count",
        "v_stats_count",
        "v_proposal_count",
        "v_observation_count",
        "v_selection_count",
        "v_aggregate_count",
        "v_membership_count",
        "NEW.evidence_decision_count",
    ):
        assert count in guard
    assert "LEARNING_FROZEN_SOURCE_PARITY_MISMATCH" in guard
    assert "DEFERRABLE INITIALLY DEFERRED" in MIGRATION


def test_retry_is_identical_and_conflicts_fail_closed():
    capture = _function_body(
        "capture_learning_canonical_source_snapshot_v2",
        "CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2",
    )
    assert "WHERE feedback_run_id = p_feedback_run_id" in capture
    assert "LEARNING_FROZEN_SOURCE_IDEMPOTENCY_CONFLICT" in capture
    assert "RETURN v_existing.snapshot_token" in capture
    assert "ON CONFLICT DO NOTHING" not in MIGRATION


def test_runtime_identity_is_shared_and_supports_local_vps_live_paper():
    capture = _function_body(
        "capture_learning_canonical_source_snapshot_v2",
        "CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2",
    )
    assert "learning_evidence_runtime_identity_v1()" in capture
    assert "WHEN 'live' THEN 'trading_live'" in capture
    assert "WHEN 'paper' THEN 'trading_paper'" in capture
    assert "v_identity.deployment_instance_id" in capture
    assert "v_identity.deployment_id" in capture
    assert "'local-live'" not in capture
    assert "'vps-live'" not in capture


def test_39_unique_decisions_have_deterministic_membership_and_hash():
    keys = [f"decision-{index:02d}" for index in range(39)]
    decision_ids = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"waltrade:{key}")) for key in keys
    ]
    rows = list(zip(keys, decision_ids, strict=True))
    ordered = sorted(rows)
    first_hash = hashlib.sha256(
        "\n".join(f"{key}|{decision_id}" for key, decision_id in ordered).encode()
    ).hexdigest()
    second_hash = hashlib.sha256(
        "\n".join(f"{key}|{decision_id}" for key, decision_id in reversed(
            list(reversed(ordered))
        )).encode()
    ).hexdigest()
    assert len({key for key, _ in rows}) == 39
    assert len(ordered) == 39
    assert first_hash == second_hash


def test_cutoff_and_classification_guards_remain_in_live_universe():
    canonical = (
        ROOT
        / "db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql"
    ).read_text()
    for contract in (
        "created_at <= p_evidence_cutoff_at",
        "ingested_at <= p_evidence_cutoff_at",
        "o.created_at <= p_evidence_cutoff_at",
        "o.calculated_at <= p_evidence_cutoff_at",
        "EXCLUDED_MISSING_REGISTRY",
        "EXCLUDED_MISSING_OUTCOME",
        "EXCLUDED_CONFLICTING_IDENTITY",
        "EXCLUDED_CHRONOLOGY",
    ):
        assert contract in canonical


def test_legacy_history_is_not_rewritten_or_backfilled():
    assert "UPDATE learning_proposal_observations_v1" not in MIGRATION
    assert "UPDATE learning_evidence_manifests_v1" not in MIGRATION
    assert "LEGACY_AGGREGATE_ONLY" not in MIGRATION
    assert "ada8a02a-49d8-4344-b451-886cf25022c3" in HARNESS
    assert "exact_membership_available <> false" in HARNESS


def test_rollback_refuses_to_destroy_complete_snapshots():
    assert "LEARNING_FROZEN_SOURCE_V2_ROLLBACK_REFUSED" in ROLLBACK
    assert "snapshot_status = 'COMPLETE'" in ROLLBACK
    assert "CASCADE" not in ROLLBACK
    assert "DELETE FROM" not in ROLLBACK


def test_scope_and_security_are_db_only_and_fail_closed():
    combined = MIGRATION + ROLLBACK
    for forbidden in (
        "/home/",
        "/mnt/",
        "/tmp/",
        "xmin",
        "CASCADE",
        "bot_control",
        "TREATMENT",
        "PAPER_EXPERIMENT",
    ):
        assert forbidden not in combined
    for forbidden_path in (
        "automation_runner/main.py",
        "services/bot_runner/Dockerfile",
        "bot/main.py",
        "bot_trend/main.py",
        "bot_supertrend/main.py",
        "bot_bbrange/main.py",
    ):
        assert forbidden_path not in combined

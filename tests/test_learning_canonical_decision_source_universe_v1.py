from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql"
).read_text()
FEEDBACK = (
    ROOT / "db/migrations/20260710_learning_feedback_engine_v1.sql"
).read_text()
FEEDBACK_UPGRADE = (
    ROOT
    / "db/migrations/20260723_learning_feedback_canonical_source_upgrade_v1.sql"
).read_text()
REPAIR = (
    ROOT / "db/migrations/20260723_learning_decision_98b4_repair_v1.sql"
).read_text()
ROLLBACK = (
    ROOT / "db/migrations/20260723_learning_canonical_manifest_rollback_v1.sql"
).read_text()
MANIFEST = (
    ROOT / "db/migrations/20260721_learning_evidence_manifest_v1.sql"
).read_text()
RUNNER = (ROOT / "automation_runner/main.py").read_text()


def test_one_shared_run_scoped_source_is_used_by_feedback_and_manifest():
    signature = "learning_canonical_evidence_universe_v1("
    assert signature in MIGRATION
    assert signature in FEEDBACK_UPGRADE
    assert MANIFEST.count(signature) >= 3
    assert "FROM v_decision_intelligence_v1 d" not in MANIFEST


def test_warehouse_registry_and_outcome_are_required_for_eligibility():
    assert "JOIN warehouse w USING (decision_key)" in MIGRATION
    assert "JOIN registry r USING (decision_key)" in MIGRATION
    assert "JOIN outcomes o USING (decision_key)" in MIGRATION
    assert "ELSE 'ELIGIBLE'" in MIGRATION


def test_missing_registry_is_excluded_and_observable():
    assert "EXCLUDED_MISSING_REGISTRY" in MIGRATION
    assert "excluded_missing_registry" in MIGRATION


def test_missing_outcome_is_excluded_and_observable():
    assert "EXCLUDED_MISSING_OUTCOME" in MIGRATION
    assert "excluded_missing_outcome" in MIGRATION


def test_multiple_registry_ids_are_excluded():
    assert "r.registry_ids <> 1" in MIGRATION
    assert "EXCLUDED_CONFLICTING_IDENTITY" in MIGRATION


def test_multiple_outcomes_are_excluded():
    assert "o.outcome_ids <> 1" in MIGRATION
    assert "EXCLUDED_CONFLICTING_OUTCOME" in MIGRATION


def test_conflicting_pnl_is_excluded():
    assert "c.warehouse_pnl_variants <> 1" in MIGRATION
    assert "c.warehouse_pnl_usdc IS DISTINCT FROM o.net_pnl_usdc" in MIGRATION
    assert "EXCLUDED_CONFLICTING_PNL" in MIGRATION


def test_post_cutoff_sources_are_bounded():
    for marker in (
        "x.created_at <= p_evidence_cutoff_at",
        "r.ingested_at <= p_evidence_cutoff_at",
        "o.created_at <= p_evidence_cutoff_at",
        "o.calculated_at <= p_evidence_cutoff_at",
        "EXCLUDED_POST_CUTOFF",
    ):
        assert marker in MIGRATION


def test_chronology_is_fail_closed():
    assert "o.calculated_at < w.exit_time" in MIGRATION
    assert "EXCLUDED_CHRONOLOGY" in MIGRATION


def test_authoritative_outcome_owns_trade_metrics():
    for field in (
        "o.gross_pnl_usdc",
        "o.fees_usdc",
        "o.net_pnl_usdc",
        "o.mfe_pct",
        "o.mae_pct",
    ):
        assert field in MIGRATION


def test_closed_representative_is_selected_deterministically():
    assert (
        "(x.exit_time IS NOT NULL AND x.net_pnl_usdc IS NOT NULL) DESC"
        in MIGRATION
    )
    assert "x.id" in MIGRATION


def test_feedback_and_frozen_membership_share_the_same_rows():
    assert "u.eligibility_reason = 'ELIGIBLE'" in FEEDBACK_UPGRADE
    assert MANIFEST.count("u.eligibility_reason") >= 2
    assert "v_source_count <> v_observation.evidence_decisions" in MANIFEST


def test_selection_telemetry_is_complete_and_immutable():
    for field in (
        "source_candidate_count",
        "canonical_eligible_count",
        "excluded_missing_registry",
        "excluded_missing_outcome",
        "excluded_conflicting_identity",
        "excluded_conflicting_outcome",
        "excluded_post_cutoff",
        "excluded_chronology",
        "excluded_other_reason",
    ):
        assert field in MIGRATION
    assert "learning canonical evidence telemetry is immutable" in MIGRATION


def test_source_universe_retry_is_idempotent_but_mutation_conflicts():
    assert "source_universe_hash" in MIGRATION
    assert "ON CONFLICT DO NOTHING" in MANIFEST
    assert "LEARNING_CANONICAL_SOURCE_UNIVERSE_CONFLICT" in MANIFEST


def test_explicit_98b4_repair_is_deterministic_and_narrow():
    assert "98b4eb54128ca4800d8cc91499026e7f" in REPAIR
    assert "v_position_id CONSTANT BIGINT := 3078" in REPAIR
    assert "2cf22538-41ff-5be3-ab51-40cbb9f468e1" in REPAIR
    assert "46821b51-7075-593b-8166-3d39f923e391" in REPAIR
    assert "PRODUCER_PIPELINE_INTEGRITY_FAILURE" in REPAIR


def test_repair_has_conflict_detection_and_no_overwrite():
    assert "LEARNING_98B4_REPAIR_REGISTRY_CONFLICT" in REPAIR
    assert "LEARNING_98B4_REPAIR_OUTCOME_CONFLICT" in REPAIR
    assert "LEARNING_98B4_REPAIR_SOURCE_FINGERPRINT_MISMATCH" in REPAIR
    assert "DO UPDATE" not in REPAIR
    assert "DELETE FROM" not in REPAIR


def test_repair_audit_is_immutable_and_fingerprinted():
    for marker in (
        "learning_decision_identity_repairs_v1",
        "source_fingerprint",
        "registry_fingerprint",
        "outcome_fingerprint",
        "learning_decision_identity_repairs_immutable_v1",
    ):
        assert marker in REPAIR


def test_production_upgrade_does_not_replay_historical_view_ddl():
    assert "CREATE OR REPLACE VIEW" not in FEEDBACK_UPGRADE
    assert "DROP VIEW" not in FEEDBACK_UPGRADE
    assert "DROP" not in FEEDBACK_UPGRADE
    assert "20260710_learning_feedback_engine_v1.sql" not in FEEDBACK_UPGRADE


def test_production_upgrade_is_versioned_idempotent_and_preserves_rollback_source():
    assert "learning_feedback_engine_v1_pre_canonical_source_v1" in FEEDBACK_UPGRADE
    assert "pg_get_functiondef" in FEEDBACK_UPGRADE
    assert "LEARNING_CANONICAL_UPGRADE_PATCH_ANCHOR_MISSING" in FEEDBACK_UPGRADE
    assert "DO UPDATE" not in FEEDBACK_UPGRADE
    assert "DELETE FROM" not in FEEDBACK_UPGRADE


def test_exact_repair_requires_local_live_runtime_identity_without_bypass():
    assert "current_database() IS DISTINCT FROM 'trading_live'" in REPAIR
    assert REPAIR.count("IS DISTINCT FROM 'local'") == 1
    assert REPAIR.count("IS DISTINCT FROM 'live'") == 1
    assert "waltrade.deployment_instance_id" in REPAIR
    assert "waltrade.environment" in REPAIR
    assert "offline_validation" not in REPAIR
    assert "production_repair" not in REPAIR


def test_exact_repair_pins_source_identity_and_financials():
    for marker in (
        "SOLUSDC",
        "'5m'",
        "TREND",
        "LONG",
        "2026-07-12 16:10:26.711057+00",
        "2026-07-12 17:55:59.341506+00",
        "3736964691072163840",
        "3737177178304454656",
        "ORC-L-SOLUSDC-TREN-5m-E-4d06da4e",
        "ORC-L-SOLUSDC-P3078-X",
        "trade_id = 905390",
        "-0.07277221",
        "-0.05679520",
        "0.01597701",
    ):
        assert marker in REPAIR


def test_exact_repair_pins_all_three_fingerprints():
    for fingerprint in (
        "6f0f4eac62fa11101db0c4e461f80cc7241e79d81538ac339f02187d24e4ac5c",
        "0e2757c85002470fb88460f547e7bf0c52fba781e8d619d177e63f2c30606f17",
        "bcac6eefec889aa4603fe247d1045a7a28797fce72900872122a71e1469758d4",
    ):
        assert fingerprint in REPAIR


def test_exact_repair_has_single_key_no_orphan_scan():
    assert REPAIR.count("98b4eb54128ca4800d8cc91499026e7f") == 1
    assert " IN SELECT " not in REPAIR
    assert "\n    LOOP" not in REPAIR


def test_dependency_prerequisites_are_explicit():
    assert "CANONICAL_SOURCE_PREREQUISITE_MISSING" in MIGRATION
    assert "LEARNING_EVIDENCE_MANIFEST_PREREQUISITE_MISSING" in MANIFEST


def test_rollback_is_exact_and_has_no_cascade():
    for exact_identity in (
        "98b4eb54128ca4800d8cc91499026e7f",
        "2cf22538-41ff-5be3-ab51-40cbb9f468e1",
        "46821b51-7075-593b-8166-3d39f923e391",
        "72e73dc9-8b2d-572f-bef9-1fc18a877adf",
    ):
        assert exact_identity in ROLLBACK
    assert " CASCADE" not in ROLLBACK
    assert "learning_feedback_engine_v1_pre_canonical_source_v1" in ROLLBACK
    assert "DROP VIEW" not in ROLLBACK


def test_historical_feedback_is_not_rewritten():
    assert "UPDATE learning_slot_statistics_v1" not in MIGRATION
    assert "UPDATE learning_proposal_observations_v1" not in MIGRATION
    assert "UPDATE learning_feedback_refresh_runs_v1" not in MIGRATION


def test_future_producer_runs_before_feedback_cutoff():
    producer = RUNNER.index("refresh_decision_identity_outcome_v1(")
    feedback = RUNNER.index("refresh_learning_feedback_engine_v1_2_if_due(", producer)
    assert producer < feedback
    assert "LEARNING_CANONICAL_IDENTITY_PRODUCER_MISSING" in RUNNER
    assert "LEARNING_CANONICAL_IDENTITY_PRODUCER_FAILED" in RUNNER


def test_producer_identity_is_fail_closed_and_has_no_fallback():
    for deployment_id in (
        '"local-live": "LOCAL"',
        '"local-paper": "LOCAL"',
        '"vps-live": "VPS"',
        '"vps-paper": "VPS"',
    ):
        assert deployment_id in RUNNER
    assert "LEARNING_CANONICAL_IDENTITY_INVALID_DEPLOYMENT_ID" in RUNNER

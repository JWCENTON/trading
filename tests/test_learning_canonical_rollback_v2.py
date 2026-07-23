from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SHARED = (
    ROOT
    / "db/migrations/20260724_learning_canonical_shared_rollback_v2.sql"
).read_text()
EXACT = (
    ROOT
    / "db/migrations/20260724_learning_decision_98b4_rollback_v1.sql"
).read_text()
LEGACY_GENERAL = (
    ROOT
    / "db/migrations/20260723_learning_canonical_manifest_rollback_v1.sql"
).read_text()
PAPER = (
    ROOT
    / "db/migrations/20260723_learning_canonical_manifest_paper_rollback_v1.sql"
).read_text()
CONFLICT_AUDIT = (
    ROOT / "scripts/learning_canonical_identity_conflict_audit_v1.sql"
).read_text()
CLASSIFICATION_FIX = (
    ROOT
    / "db/migrations/20260724_learning_canonical_source_universe_v1_1_identity_classification.sql"
).read_text()


def test_shared_rollback_has_scalable_derived_identity_and_database_guard():
    assert "v_instance || '-' || v_environment" in SHARED
    assert "^[a-z0-9]+(?:-[a-z0-9]+)*$" in SHARED
    assert "trading_live" in SHARED and "trading_paper" in SHARED
    assert "local-live" not in SHARED
    assert "vps-live" not in SHARED
    assert "vps2" not in SHARED
    assert "DATABASE_IDENTITY_MISMATCH" in SHARED


def test_shared_rollback_is_repair_independent_and_history_safe():
    forbidden = (
        "98b4eb54128ca4800d8cc91499026e7f",
        "learning_decision_identity_repairs_v1",
        "decision_registry_v1",
        "decision_outcomes_v1",
        "CASCADE",
        "UPDATE learning_feedback",
        "DELETE FROM learning_feedback",
        "UPDATE learning_proposal",
        "DELETE FROM learning_proposal",
    )
    for value in forbidden:
        assert value not in SHARED
    assert "ALREADY_APPLIED" in SHARED
    assert "PARTIAL_STATE" in SHARED


def test_exact_repair_rollback_is_separate_and_local_live_only():
    assert "trading_live" in EXACT
    assert "'local'" in EXACT and "'live'" in EXACT
    assert "98b4eb54128ca4800d8cc91499026e7f" in EXACT
    assert "2cf22538-41ff-5be3-ab51-40cbb9f468e1" in EXACT
    assert "46821b51-7075-593b-8166-3d39f923e391" in EXACT
    assert "learning_evidence_manifests_v1" not in EXACT
    assert "learning_canonical_evidence_universe_v1" not in EXACT
    assert "CASCADE" not in EXACT


def test_legacy_rollbacks_remain_for_compatibility():
    assert "LEARNING_CANONICAL_ROLLBACK_RUNTIME_IDENTITY_MISMATCH" in LEGACY_GENERAL
    assert "LEARNING_CANONICAL_PAPER_ROLLBACK_RUNTIME_IDENTITY_MISMATCH" in PAPER


def test_conflict_audit_is_read_only_and_emits_required_lineage():
    for required in (
        "registry_decision_ids",
        "outcome_decision_ids",
        "deployment_instance_id",
        "registry_environments",
        "source_table",
        "strategy",
        "symbol",
        "timeframe",
        "position_id",
        "order_client_order_linkage",
        "exact_conflict_reason",
        "conflict_record_count",
        "complete_lifecycle_provenance",
        "legacy_identity_only",
        "present_in_feedback_source",
        "member_of_manifest_universe",
    ):
        assert required in CONFLICT_AUDIT
    for forbidden in ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE"):
        assert forbidden not in CONFLICT_AUDIT


def test_identity_classification_fix_is_additive_and_idempotent():
    assert "count(*) AS registry_rows" in CLASSIFICATION_FIX
    assert "count(r.*) AS registry_rows" in CLASSIFICATION_FIX
    assert "CREATE OR REPLACE FUNCTION" not in CLASSIFICATION_FIX
    assert "pg_get_functiondef" in CLASSIFICATION_FIX
    assert "ALREADY_APPLIED" in CLASSIFICATION_FIX
    assert "UNEXPECTED_FUNCTION_DEFINITION" in CLASSIFICATION_FIX
    assert "UPDATE " not in CLASSIFICATION_FIX
    assert "DELETE " not in CLASSIFICATION_FIX
    assert "CASCADE" not in CLASSIFICATION_FIX

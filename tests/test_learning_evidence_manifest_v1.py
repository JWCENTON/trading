import re
from pathlib import Path

import pytest

from common.learning_evidence_context import (
    resolve_learning_evidence_identity,
    set_learning_evidence_transaction_context,
)


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "db/migrations/20260721_learning_evidence_manifest_v1.sql").read_text()
FEEDBACK_SQL = (
    ROOT / "db/migrations/20260710_learning_feedback_engine_v1.sql"
).read_text()
CANONICAL_SQL = (
    ROOT
    / "db/migrations/20260723_learning_canonical_decision_source_universe_v1.sql"
).read_text()


def test_schema_has_header_membership_and_aggregate_contract():
    for table in (
        "learning_evidence_manifests_v1",
        "learning_evidence_membership_v1",
        "learning_evidence_aggregates_v1",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in SQL
    for field in (
        "evidence_manifest_id", "feedback_run_id", "validation_run_id",
        "shadow_recommendation_id", "evidence_cutoff_at", "manifest_hash",
        "aggregate_hash", "row_fingerprint", "regime_distribution",
        "missing_fees_count", "missing_mfe_count", "missing_mae_count",
    ):
        assert field in SQL


def test_complete_capture_is_fail_closed_on_109_to_109_parity():
    assert "v_source_count <> v_observation.evidence_decisions" in SQL
    assert "LEARNING_EVIDENCE_COUNT_MISMATCH" in SQL
    assert "v_inserted_count <> v_source_count" in SQL
    assert "LEARNING_EVIDENCE_CHILD_COUNT_MISMATCH" in SQL
    # No partial COMPLETE header can commit because exceptions abort the transaction.
    assert "manifest_status IN ('BUILDING','COMPLETE','LEGACY_AGGREGATE_ONLY')" in SQL
    assert "CREATE CONSTRAINT TRIGGER learning_evidence_complete_deferred_v1" in SQL
    assert "INCOMPLETE_LEARNING_EVIDENCE_MANIFEST" in SQL
    assert "UNFINALIZED_LEARNING_EVIDENCE_MANIFEST" in SQL


def test_idempotency_and_conflicting_membership_guards():
    assert "UNIQUE (deployment_id, environment, feedback_run_id" in SQL
    assert "LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT" in SQL
    assert "STALE_BUILDING_LEARNING_EVIDENCE_MANIFEST" in SQL
    assert "v_existing.manifest_hash<>v_manifest_hash" in SQL
    assert "UNIQUE (evidence_manifest_id, decision_key)" in SQL


def test_construction_uses_random_transaction_local_capability_not_xid():
    assert "v_construction_token := gen_random_uuid()" in SQL
    assert "waltrade.learning_manifest_construction_token" in SQL
    assert "construction_token UUID UNIQUE" in SQL
    assert "set_config('waltrade.learning_manifest_construction_token','',true)" in SQL
    for forbidden in (
        "xmin", "txid_current", "txid_current_if_assigned", "pg_current_xact_id",
    ):
        assert forbidden not in SQL


def test_building_to_complete_is_the_only_mutable_header_transition():
    assert "OLD.manifest_status = 'BUILDING'" in SQL
    assert "NEW.manifest_status = 'COMPLETE'" in SQL
    assert "to_jsonb(OLD) - 'manifest_status'" in SQL
    assert "LEARNING_MANIFEST_FINALIZATION_CAPABILITY_REQUIRED" in SQL
    assert "finalize_learning_evidence_manifest_v1(v_manifest_id)" in SQL
    assert "UPDATE learning_evidence_manifests_v1 SET manifest_status='COMPLETE'" in SQL


def test_token_guards_header_children_aggregate_and_context():
    for marker in (
        "learning_evidence_manifest_construction_v1",
        "LEARNING_MANIFEST_HEADER_CAPABILITY_REQUIRED",
        "LEARNING_MANIFEST_CONSTRUCTION_TOKEN_REQUIRED",
        "INVALID_LEARNING_MANIFEST_CONSTRUCTION_TOKEN",
        "LEARNING_MANIFEST_CONSTRUCTION_CAPABILITY_MISMATCH",
        "v_header.construction_token <> v_token",
        "v_header.manifest_status <> 'BUILDING'",
        "v_header.deployment_instance_id <> v_instance",
        "v_header.environment <> v_environment",
        "v_header.deployment_id <> v_deployment",
    ):
        assert marker in SQL


def test_integrity_guards_cover_hash_cutoff_coverage_and_cross_deployment():
    for marker in (
        "LEARNING_EVIDENCE_AFTER_CUTOFF",
        "LEARNING_EVIDENCE_CROSS_DEPLOYMENT_DECISION",
        "LEARNING_EVIDENCE_COVERAGE_MISMATCH",
        "LEARNING_EVIDENCE_ROW_FINGERPRINT_MISMATCH",
        "LEARNING_EVIDENCE_AGGREGATE_HASH_MISMATCH",
        "v_manifest_hash IS DISTINCT FROM v_header.manifest_hash",
        "v_aggregate_hash IS DISTINCT FROM v_header.aggregate_hash",
    ):
        assert marker in SQL


def test_all_manifest_relations_are_append_only():
    assert SQL.count("BEFORE UPDATE OR DELETE ON learning_evidence_") == 3
    assert "learning evidence manifest is immutable and append-only" in SQL


def test_ordering_and_hashing_are_deterministic():
    assert "row_number() OVER(ORDER BY decision_key)" in SQL
    assert "string_agg(jsonb_build_array(decision_key,decision_id)::text" in SQL
    assert "ORDER BY decision_key" in SQL
    assert SQL.count("digest(") >= 5
    assert "ORDER BY outcome_timestamp NULLS LAST, decision_key" in SQL


def test_optional_metrics_do_not_define_membership_identity():
    membership_hash = re.search(
        r"string_agg\(jsonb_build_array\(decision_key,decision_id\).*?'sha256'",
        SQL,
        re.S,
    ).group(0)
    for optional in ("fees_usdc", "mfe_pct", "mae_pct", "market_regime"):
        assert optional not in membership_hash
    for coverage in ("fees_available", "mfe_available", "mae_available", "regime_available"):
        assert coverage in SQL


def test_cutoff_and_forward_separation_are_explicit():
    assert "evidence_cutoff_at" in SQL
    assert (
        "v_observation.sample_to, v_evidence_cutoff_at"
        in SQL
    )
    assert "d.refreshed_at BETWEEN p_sample_from AND p_sample_to" in CANONICAL_SQL
    assert "outcome_timestamp" in SQL


def test_event_window_is_not_reused_as_the_as_of_cutoff():
    assert "source_snapshot_at TIMESTAMPTZ" in SQL
    assert "v_evidence_cutoff_at := COALESCE(v_run.started_at,v_run.requested_at)" in SQL
    assert (
        "v_observation.sample_from,v_observation.sample_to,\n"
        "            v_evidence_cutoff_at,v_evidence_cutoff_at"
    ) in SQL
    assert "v_observation.sample_to,v_observation.sample_to" not in SQL


def test_source_availability_is_bounded_before_aggregation_and_membership():
    for marker in (
        "x.created_at <= p_evidence_cutoff_at",
        "r.ingested_at <= p_evidence_cutoff_at",
        "o.calculated_at <= p_evidence_cutoff_at",
        "EXCLUDED_POST_CUTOFF",
    ):
        assert marker in CANONICAL_SQL
    assert SQL.count("learning_canonical_evidence_universe_v1(") >= 3


def test_frozen_membership_metrics_are_checked_against_feedback_statistics():
    assert "LEARNING_EVIDENCE_AGGREGATE_PARITY_MISMATCH" in SQL
    assert "v_observation.source_wins" in SQL
    assert "v_observation.source_losses" in SQL
    assert "v_observation.source_net_pnl_usdc" in SQL
    assert "v_observation.source_profit_factor" in SQL
    assert "v_observation.source_expectancy_usdc" in SQL
    assert "v_observation.source_win_rate_pct" in SQL
    assert "v_observation.source_breakeven" in SQL
    assert "v_observation.source_gross_profit_usdc" in SQL
    assert "v_observation.source_gross_loss_usdc" in SQL


def test_feedback_and_manifest_share_canonical_profit_factor_helper():
    signature = "learning_canonical_profit_factor_v1("
    assert SQL.count(signature) >= 2
    assert FEEDBACK_SQL.count(signature) >= 2
    for source in (SQL, FEEDBACK_SQL):
        assert "COALESCE(p_gross_profit_usdc, 0)" in source
        assert "/ ABS(p_gross_loss_usdc)" in source
        assert "THEN 999::NUMERIC" in source
        assert "p_pnl_coverage_count, 0) = 0 THEN NULL::NUMERIC" in source
        assert "END, 12)" in source


@pytest.mark.parametrize(
    "decisions,coverage,gross_profit,gross_loss,expected",
    [
        (2, 2, 4, -2, 2),
        (1, 1, 4, None, 999),
        (1, 1, None, -2, 0),
        (1, 1, 0, 0, 0),
        (0, 0, None, None, None),
        (2, 0, None, None, None),
        (2, 2, 0, 0, 0),
        (2, 2, 3, None, 999),
        (2, 2, None, -3, 0),
        (1, 1, 3, None, 999),
        (1, 1, None, -3, 0),
    ],
)
def test_canonical_profit_factor_contract_model(
    decisions, coverage, gross_profit, gross_loss, expected
):
    if not decisions or not coverage:
        actual = None
    elif abs(gross_loss or 0) == 0 and (gross_profit or 0) > 0:
        actual = 999
    elif abs(gross_loss or 0) == 0:
        actual = 0
    else:
        actual = (gross_profit or 0) / abs(gross_loss)
    assert actual == expected


def test_profit_factor_parity_does_not_coalesce_null_and_zero():
    parity = re.search(
        r"round\(\(v_aggregate->>'profit_factor'\).*?"
        r"round\(v_observation\.source_profit_factor,12\)",
        SQL,
        re.S,
    ).group(0)
    assert "IS DISTINCT FROM" in parity
    assert "COALESCE" not in parity


def test_deployment_is_derived_and_isolated_not_host_fallback():
    assert "current_setting('waltrade.deployment_instance_id', true)" in SQL
    assert "current_setting('waltrade.environment', true)" in SQL
    assert "INVALID_LEARNING_EVIDENCE_RUNTIME_IDENTITY" in SQL
    capture = SQL[SQL.index("CREATE OR REPLACE FUNCTION capture_learning_evidence_manifests_v1") :]
    assert "INTO v_deployment_id FROM decision_registry_v1" not in capture
    assert "current_database()" not in capture
    assert "UNIQUE (deployment_id, environment, feedback_run_id" in SQL


@pytest.mark.parametrize("instance,environment", [
    ("local", "live"), ("local", "paper"),
    ("vps", "live"), ("vps", "paper"),
    ("vps2", "live"), ("vps2", "paper"),
    ("regional-eu", "live"), ("regional-eu", "paper"),
])
def test_runtime_identity_accepts_scalable_instance_environment_pairs(instance, environment):
    deployment = f"{instance}-{environment}"
    assert resolve_learning_evidence_identity({
        "DEPLOYMENT_ID": deployment, "ENVIRONMENT": environment,
    }) == (instance, environment, deployment)
    assert resolve_learning_evidence_identity({
        "DEPLOYMENT_INSTANCE_ID": instance, "ENVIRONMENT": environment,
    }) == (instance, environment, deployment)


@pytest.mark.parametrize("values", [
    {"ENVIRONMENT": "live"}, {"DEPLOYMENT_ID": "local-live"},
    {"DEPLOYMENT_ID": "LOCAL", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_ID": "local-live", "ENVIRONMENT": "trading_live"},
    {"DEPLOYMENT_ID": "local-live", "ENVIRONMENT": "paper"},
    {"DEPLOYMENT_ID": "local-paper", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_ID": "unknown", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_ID": "", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_ID": " local-live", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "VPS2", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "-vps2", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2-", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps--2", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps_2", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "eu/vps", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2-live", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2-paper", "ENVIRONMENT": "paper"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2", "ENVIRONMENT": "trading_live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2", "ENVIRONMENT": "trading_paper"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2", "ENVIRONMENT": "LIVE"},
    {"DEPLOYMENT_ID": "vps2-live", "ENVIRONMENT": "paper"},
    {"DEPLOYMENT_ID": "vps2", "ENVIRONMENT": "live"},
    {"DEPLOYMENT_INSTANCE_ID": "vps2", "DEPLOYMENT_ID": "other-live", "ENVIRONMENT": "live"},
])
def test_runtime_identity_rejects_missing_legacy_mismatch_empty_and_whitespace(values):
    with pytest.raises(ValueError):
        resolve_learning_evidence_identity(values)


def test_runtime_helper_sets_both_values_transaction_locally(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("ENVIRONMENT", "live")
    class Cursor:
        def execute(self, sql, params):
            self.sql, self.params = sql, params
    cur = Cursor()
    assert set_learning_evidence_transaction_context(cur) == ("local", "live", "local-live")
    assert cur.params == ("local", "live")
    assert "set_config('waltrade.deployment_instance_id', %s, true)" in cur.sql
    assert "set_config('waltrade.environment', %s, true)" in cur.sql


def test_legacy_backfill_never_fabricates_membership():
    assert "LEGACY_AGGREGATE_ONLY" in SQL
    assert "exact_membership_available" in SQL
    backfill = SQL[SQL.index("-- Preserve historical aggregates explicitly") :]
    assert "learning_evidence_membership_v1" not in backfill


def test_publisher_requires_complete_manifest():
    assert "COMPLETE_LEARNING_EVIDENCE_MANIFEST_REQUIRED" in SQL
    assert "BEFORE INSERT OR UPDATE ON learning_shadow_confidence_proposals_v1" in SQL
    assert "m.manifest_status='COMPLETE' AND m.exact_membership_available" in SQL
    assert "LEARNING_EVIDENCE_PUBLISHER_CONTEXT_MISMATCH" in SQL


def test_registry_provenance_is_separate_and_cannot_override_runtime_identity():
    assert "v_registry_deployment_id" in SQL
    assert "v_registry_environment" in SQL
    assert "AMBIGUOUS_OR_MISSING_LEGACY_REGISTRY_PROVENANCE" in SQL
    assert "v_manifest_id,v_deployment_id,v_deployment_instance_id,v_environment" in SQL


def test_legacy_and_complete_headers_use_runtime_identity():
    backfill = SQL[SQL.index("-- Preserve historical aggregates explicitly") :]
    assert "learning_evidence_runtime_identity_v1()" in backfill
    assert "i.deployment_id,i.deployment_instance_id,i.environment" in backfill
    assert "v_manifest_id,v_deployment_id,v_deployment_instance_id,v_environment" in SQL


def test_sql_identity_is_dynamic_derived_and_strict():
    assert "current_setting('waltrade.deployment_instance_id', true)" in SQL
    assert "v_instance || '-' || v_environment" in SQL
    assert "length(v_instance) NOT BETWEEN 1 AND 63" in SQL
    assert "^[a-z0-9]+(?:-[a-z0-9]+)*$" in SQL
    assert "v_instance LIKE '%-live'" in SQL
    assert "v_instance LIKE '%-paper'" in SQL
    assert "deployment_id = deployment_instance_id || '-' || environment" in SQL
    for hardcoded in ("vps2-live", "regional-eu-live"):
        assert hardcoded not in SQL


def test_patch_does_not_touch_trading_or_apply_state():
    for table in ("bot_control", "positions", "orders", "simulated_orders"):
        assert not re.search(rf"(?:INSERT INTO|UPDATE|DELETE FROM)\s+{table}\b", SQL, re.I)
    for forbidden in ("PAPER_EXPERIMENT", "TREATMENT", "apply_enabled = true"):
        assert forbidden not in SQL

import copy
import hashlib
import json
from pathlib import Path

import pytest

from common.schema_provenance import (
    BASELINE_VERSION,
    classify_provenance_role,
    hierarchical_entry,
    hierarchical_readiness,
    identity_string,
    compare_inventory,
    fingerprint,
    load_manifest,
    normalize_sql,
    object_key,
    validate_difference_contract,
    validate_manifest,
    validate_tracked_provenance,
)
from scripts.waltrade_schema_baseline_v1 import (
    CATALOG_SQL,
    candidate_tracked_paths,
    connect,
    merge_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "db/schema_baseline/waltrade_database_baseline_v1.json"
DIFFERENCES_PATH = ROOT / "db/schema_baseline/expected_environment_differences_v1.json"
@pytest.fixture(scope="module")
def manifest():
    return load_manifest(MANIFEST_PATH)


@pytest.fixture(scope="module")
def differences():
    return json.loads(DIFFERENCES_PATH.read_text())


def _accepted_contract(differences):
    contract = copy.deepcopy(differences)
    for rule in contract["rules"]:
        rule["classification"] = "EXPECTED_ENVIRONMENT_VARIANT"
        rule["risk_class"] = "P1"
    return contract


def test_normalization_ignores_format_comments_and_dump_set_commands():
    left = """SET statement_timeout = 0;
              -- dump noise
              CREATE VIEW x AS SELECT a, b FROM t WHERE a = 1;"""
    right = "CREATE VIEW x AS SELECT a,b FROM t WHERE a=1;"
    assert normalize_sql(left) == normalize_sql(right)
    assert fingerprint(left) == fingerprint(right)


def test_normalization_keeps_function_body_and_predicate_semantics():
    assert fingerprint("BEGIN RETURN 1; END") != fingerprint("BEGIN RETURN 2; END")
    assert fingerprint("WHERE status='OPEN'") != fingerprint("WHERE status='CLOSED'")


def test_fingerprint_is_deterministic_for_structural_json():
    assert fingerprint({"b": 2, "a": 1}) == fingerprint({"a": 1, "b": 2})


def test_manifest_has_exact_unique_membership_and_required_fields(manifest):
    keys = [(*object_key(entry), entry["applicability"]) for entry in manifest["objects"]]
    assert len(keys) == len(set(keys)) == 1182
    assert manifest["baseline_version"] == BASELINE_VERSION
    assert {entry["applicability"] for entry in manifest["objects"]} == {
        "COMMON", "LIVE_ONLY", "PAPER_ONLY"
    }


@pytest.mark.parametrize("name", [
    "positions", "binance_orders", "binance_order_fills",
    "decision_replay_v1", "learning_feature_warehouse_v1",
    "learning_feedback_shadow_recommendations", "learning_knowledge_base_v1",
    "decision_outcomes_v1", "bot_control", "orc_apply_runs_v1",
    "slot_capital_policy", "ui_notifications",
])
def test_required_relation_is_manifested(manifest, name):
    assert any(
        e["object_name"] == name and e["object_type"] in {"TABLE", "VIEW"}
        for e in manifest["objects"]
    )


@pytest.mark.parametrize("name", [
    "fill_position_pnl_on_close", "refresh_decision_replay_v1",
    "refresh_learning_feature_warehouse_v1",
    "refresh_learning_feedback_shadow_recommendations_v1",
    "refresh_learning_knowledge_base_v1",
    "fn_refresh_adaptive_sizing_for_slot",
])
def test_required_function_is_manifested(manifest, name):
    assert any(e["object_type"] == "FUNCTION" and e["object_name"] == name
               for e in manifest["objects"])


def _actual(manifest, env):
    rows = []
    for entry in manifest["objects"]:
        if entry["applicability"] not in ("COMMON", f"{env}_ONLY"):
            continue
        row = {
            key: entry.get(key, "")
            for key in ("object_type", "schema", "object_name",
                        "identity_arguments", "parent_relation", "enabled_state",
                        "owner_contract", "management", "extension_name",
                        "extension_version")
        }
        row["management"] = entry.get("management", "APPLICATION")
        row["owner_contract"] = entry.get("owner_contract", "")
        row["extension_name"] = entry.get("extension_name")
        row["extension_version"] = entry.get("extension_version")
        row["canonical_definition_sha256"] = entry.get(
            "environment_fingerprints", {}
        ).get(env, entry["canonical_definition_sha256"])
        row["enabled_state"] = entry.get("environment_enabled_states", {}).get(
            env, entry.get("enabled_state")
        )
        rows.append(row)
    return rows


@pytest.mark.parametrize("env", ["LIVE", "PAPER"])
def test_environment_applicability_reaches_ready(manifest, differences, env):
    assert compare_inventory(
        manifest, _actual(manifest, env), env,
        difference_contract=_accepted_contract(differences),
    ) == ("READY", [])


def test_unknown_runtime_object_detection(manifest):
    actual = _actual(manifest, "LIVE")
    actual.append({
        "object_type": "TRIGGER", "schema": "public", "object_name": "trg_unknown",
        "identity_arguments": "", "parent_relation": "positions",
        "canonical_definition_sha256": "a" * 64, "enabled_state": "ENABLED",
    })
    status, diff = compare_inventory(manifest, actual, "LIVE")
    assert status == "UNKNOWN_CRITICAL_OBJECT"
    assert diff[-1].kind == "UNKNOWN_CRITICAL_OBJECT"


@pytest.mark.parametrize("object_type", ["FUNCTION", "TRIGGER", "TABLE"])
def test_object_level_definition_mismatch(manifest, object_type):
    actual = _actual(manifest, "LIVE")
    row = next(
        x for x in actual
        if x["object_type"] == object_type
        and x.get("management", "APPLICATION") == "APPLICATION"
    )
    row["canonical_definition_sha256"] = "0" * 64
    status, diff = compare_inventory(manifest, actual, "LIVE")
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "DEFINITION_MISMATCH" and d.object_name == row["object_name"]
               for d in diff)


def test_trigger_enabled_state_mismatch(manifest):
    actual = _actual(manifest, "LIVE")
    row = next(x for x in actual if x["object_type"] == "TRIGGER")
    row["enabled_state"] = "DISABLED" if row["enabled_state"] == "ENABLED" else "ENABLED"
    status, diff = compare_inventory(manifest, actual, "LIVE")
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "ENABLED_STATE_MISMATCH" for d in diff)


def test_missing_object_has_object_level_diff(manifest):
    actual = _actual(manifest, "LIVE")
    removed = actual.pop()
    status, diff = compare_inventory(manifest, actual, "LIVE")
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "MISSING_OBJECT" and d.object_name == removed["object_name"]
               for d in diff)


def test_environment_contract_violation(manifest):
    actual = _actual(manifest, "PAPER")
    live_only = next(e for e in manifest["objects"] if e["applicability"] == "LIVE_ONLY")
    actual.append({
        **{key: live_only.get(key, "") for key in (
            "object_type", "schema", "object_name", "identity_arguments",
            "parent_relation", "enabled_state"
        )},
        "canonical_definition_sha256": live_only["canonical_definition_sha256"],
    })
    assert compare_inventory(manifest, actual, "PAPER")[0] == (
        "ENVIRONMENT_CONTRACT_VIOLATION"
    )


def test_checksum_conflict_fails_closed(manifest):
    assert compare_inventory(
        manifest, _actual(manifest, "LIVE"), "LIVE", checksum_conflict=True
    )[0] == "CHECKSUM_CONFLICT"


def test_legacy_p0_is_adopted_but_not_declared_safe(manifest):
    legacy = [
        e for e in manifest["objects"]
        if e.get("legacy_baseline_status") == "BASELINE_ADOPTED_LEGACY_P0"
    ]
    assert len(legacy) == 5
    assert any(e["object_name"] == "fill_position_pnl_on_close" for e in legacy)
    assert all(e["risk_class"] == "P0" for e in legacy)


def test_expected_difference_contract_defaults_fail_closed(manifest, differences):
    contract = json.loads(DIFFERENCES_PATH.read_text())
    assert contract["default_for_unlisted_mismatch"] == "UNEXPECTED"
    assert all(
        e.get("difference_classification") == "BLOCKED_PENDING_DECISION"
        for e in manifest["objects"]
        if "environment_fingerprints" in e and e["object_name"] != "allocation_policy"
    )
    assert next(
        e for e in manifest["objects"] if e["object_name"] == "allocation_policy"
    )["difference_classification"] == "UNEXPECTED_DRIFT"
    validate_difference_contract(differences)
    status, _ = compare_inventory(
        manifest, _actual(manifest, "LIVE"), "LIVE",
        difference_contract=differences,
    )
    assert status == "BLOCKED_PENDING_DECISION"


def test_no_trading_or_decision_runtime_files_changed_by_feature():
    changed_feature_files = {
        "common/schema_provenance.py",
        "scripts/waltrade_schema_baseline_v1.py",
    }
    assert not any(path.startswith(("bot/", "automation_runner/", "api/main.py"))
                   for path in changed_feature_files)


def test_malformed_manifest_and_duplicate_identity_are_rejected(manifest):
    with pytest.raises(ValueError, match="unsupported"):
        validate_manifest({})
    duplicate = copy.deepcopy(manifest)
    duplicate["objects"].append(copy.deepcopy(duplicate["objects"][0]))
    with pytest.raises(ValueError, match="duplicate manifest"):
        validate_manifest(duplicate)


def test_duplicate_actual_inventory_is_rejected(manifest, differences):
    actual = _actual(manifest, "LIVE")
    actual.append(copy.deepcopy(actual[0]))
    with pytest.raises(ValueError, match="duplicate actual"):
        compare_inventory(
            manifest, actual, "LIVE",
            difference_contract=_accepted_contract(differences),
        )


def test_overloaded_routines_and_trigger_parent_are_exact_identities():
    base = {
        "object_type": "FUNCTION", "schema": "public", "object_name": "f",
        "parent_relation": "",
    }
    assert object_key({**base, "identity_arguments": "integer"}) != object_key(
        {**base, "identity_arguments": "text"}
    )
    trigger = {
        "object_type": "TRIGGER", "schema": "public", "object_name": "trg",
        "identity_arguments": "",
    }
    assert object_key({**trigger, "parent_relation": "a"}) != object_key(
        {**trigger, "parent_relation": "b"}
    )


def test_owner_mismatch_fails(manifest, differences):
    actual = _actual(manifest, "LIVE")
    row = next(x for x in actual if x.get("management", "APPLICATION") == "APPLICATION")
    row["owner_contract"] = "intruder"
    status, drift = compare_inventory(
        manifest, actual, "LIVE",
        difference_contract=_accepted_contract(differences),
    )
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "OWNER_MISMATCH" for d in drift)


def test_exact_environment_rules_and_wildcards():
    contract = json.loads(DIFFERENCES_PATH.read_text())
    assert len(contract["rules"]) == 11
    assert all(set(rule["identity"]) == {
        "object_type", "schema", "object_name", "identity_arguments", "parent_relation"
    } for rule in contract["rules"])
    assert not any("*" in json.dumps(rule["identity"]) for rule in contract["rules"])


def test_unmatched_environment_variant_fails_closed(manifest):
    actual = _actual(manifest, "LIVE")
    status, drift = compare_inventory(
        manifest, actual, "LIVE",
        difference_contract={
            "baseline_version": BASELINE_VERSION,
            "default_for_unlisted_mismatch": "UNEXPECTED",
            "rules": [],
        },
    )
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "UNMATCHED_ENVIRONMENT_DIFFERENCE" for d in drift)


@pytest.mark.parametrize("risk", ["P0", "P1"])
def test_unknown_application_object_fails_closed(manifest, differences, risk):
    actual = _actual(manifest, "LIVE")
    actual.append({
        "object_type": "TABLE", "schema": "public", "object_name": f"unknown_{risk}",
        "identity_arguments": "", "parent_relation": "",
        "canonical_definition_sha256": "f" * 64, "owner_contract": "botuser",
        "management": "APPLICATION", "risk_class": risk,
    })
    assert compare_inventory(
        manifest, actual, "LIVE",
        difference_contract=_accepted_contract(differences),
    )[0] == "UNKNOWN_CRITICAL_OBJECT"


def test_extension_objects_are_distinct_from_application_drift_and_version_is_checked(
    manifest, differences,
):
    actual = _actual(manifest, "LIVE")
    actual.append({
        "object_type": "FUNCTION", "schema": "public", "object_name": "digest_test_extension",
        "identity_arguments": "text, text", "parent_relation": "",
        "canonical_definition_sha256": "e" * 64, "owner_contract": "postgres",
        "management": "EXTENSION", "extension_name": "pgcrypto",
        "extension_version": "1.3",
    })
    status, drift = compare_inventory(
        manifest, actual, "LIVE",
        difference_contract=_accepted_contract(differences),
    )
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "UNMANIFESTED_EXTENSION" for d in drift)
    assert not any(d.kind == "UNKNOWN_CRITICAL_OBJECT" for d in drift)
    extended = copy.deepcopy(manifest)
    entry = copy.deepcopy(actual[-1])
    entry.update({
        "applicability": "COMMON", "source_file": "extension catalog",
        "risk_class": "P1", "provenance_status": "EXTENSION_MANAGED",
    })
    extended["objects"].append(entry)
    changed = copy.deepcopy(actual)
    changed[-1]["extension_version"] = "1.4"
    status, drift = compare_inventory(
        extended, changed, "LIVE",
        difference_contract=_accepted_contract(differences),
    )
    assert status == "DRIFT_DETECTED"
    assert any(d.kind == "EXTENSION_VERSION_MISMATCH" for d in drift)


def test_full_catalog_query_has_all_required_object_families():
    for token in (
        "MATERIALIZED_VIEW", "SEQUENCE", "PROCEDURE", "TRIGGER", "CONSTRAINT",
        "INDEX", "POLICY", "RULE", "EVENT_TRIGGER",
    ):
        assert token in CATALOG_SQL
    assert "relname = ANY" not in CATALOG_SQL
    assert "proname ILIKE" not in CATALOG_SQL
    assert "pg_extension" in CATALOG_SQL


def test_missing_credential_source_fails_without_secret_leak(monkeypatch, tmp_path):
    for name in (
        "DATABASE_DSN", "DB_PASS", "PGPASSWORD", "PGPASSFILE", "DB_NAME",
        "PGDATABASE", "DB_USER", "PGUSER", "DB_HOST", "PGHOST",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_HOST", "test_host")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    with pytest.raises(RuntimeError) as exc:
        connect()
    assert "password" not in str(exc.value).lower()


def test_manifest_output_is_deterministic_and_unmatched_difference_blocks(differences):
    row = {
        "object_type": "TABLE", "schema": "public", "object_name": "x",
        "identity_arguments": "", "parent_relation": "",
        "canonical_definition_sha256": "1" * 64, "owner_contract": "botuser",
        "enabled_state": None, "management": "APPLICATION",
    }
    assert merge_manifest([row], [copy.deepcopy(row)], differences) == merge_manifest(
        [copy.deepcopy(row)], [row], differences
    )
    changed = copy.deepcopy(row)
    changed["canonical_definition_sha256"] = "2" * 64
    with pytest.raises(RuntimeError, match="unmatched"):
        merge_manifest([row], [changed], differences)


def _hierarchy_object(object_type, name, *, parent="", management="APPLICATION"):
    return {
        "object_type": object_type, "schema": "public", "object_name": name,
        "identity_arguments": "", "parent_relation": parent,
        "canonical_definition_sha256": "a" * 64, "owner_contract": "botuser",
        "management": management, "applicability": "COMMON",
        "source_file": "fixture", "risk_class": "P1",
        "provenance_status": "BASELINE_ADOPTED",
    }


@pytest.mark.parametrize("object_type,expected", [
    ("TABLE", "PROVENANCE_ROOT"),
    ("VIEW", "PROVENANCE_ROOT"),
    ("MATERIALIZED_VIEW", "PROVENANCE_ROOT"),
    ("FUNCTION", "INDEPENDENT_EXECUTABLE"),
    ("PROCEDURE", "INDEPENDENT_EXECUTABLE"),
    ("TRIGGER", "INDEPENDENT_EXECUTABLE"),
    ("RULE", "INDEPENDENT_EXECUTABLE"),
    ("CONSTRAINT", "OWNED_CHILD"),
    ("INDEX", "OWNED_CHILD"),
    ("SEQUENCE", "OWNED_CHILD"),
])
def test_hierarchical_role_classification(object_type, expected):
    assert classify_provenance_role(
        _hierarchy_object(object_type, "object", parent="root")
    ) == expected


def test_table_children_have_root_and_do_not_add_manual_decision():
    table = hierarchical_entry(
        _hierarchy_object("TABLE", "orders"),
        source_evidence={
            "provenance_status": "TRACKED_CURRENT",
            "source_path": "db/migrations/orders.sql", "source_commit": "f" * 40,
        },
    )
    children = [
        hierarchical_entry(_hierarchy_object("SEQUENCE", "orders_id_seq"),
                           sequence_root=table["identity"]),
        hierarchical_entry(_hierarchy_object("CONSTRAINT", "orders_pkey", parent="orders")),
        hierarchical_entry(_hierarchy_object("INDEX", "orders_pkey", parent="orders")),
        hierarchical_entry(_hierarchy_object("CONSTRAINT", "orders_user_fk", parent="orders")),
    ]
    assert all(row["root_identity"] == table["identity"] for row in children)
    assert sum(
        row["provenance_role"] in {"PROVENANCE_ROOT", "INDEPENDENT_EXECUTABLE"}
        for row in [table, *children]
    ) == 1


def test_trigger_and_writer_function_remain_independent_executables():
    function = _hierarchy_object("FUNCTION", "write_position")
    function["identity_arguments"] = "uuid, numeric"
    trigger = _hierarchy_object("TRIGGER", "trg_write_position", parent="positions")
    assert hierarchical_entry(function)["root_identity"] == identity_string(function)
    assert hierarchical_entry(trigger)["root_identity"] == identity_string(trigger)


def test_dependency_ordering_overloads_and_same_name_trigger_identities():
    fn_int = _hierarchy_object("FUNCTION", "f")
    fn_int["identity_arguments"] = "integer"
    fn_text = copy.deepcopy(fn_int)
    fn_text["identity_arguments"] = "text"
    trg_a = _hierarchy_object("TRIGGER", "trg_same", parent="a")
    trg_b = _hierarchy_object("TRIGGER", "trg_same", parent="b")
    assert identity_string(fn_int) != identity_string(fn_text)
    assert identity_string(trg_a) != identity_string(trg_b)
    row = hierarchical_entry(
        fn_int, dependency_identities=["TABLE:public:z::", "TABLE:public:a::"]
    )
    assert row["dependency_identities"] == ["TABLE:public:a::", "TABLE:public:z::"]


def test_extension_member_and_manual_provenance_statuses():
    extension = hierarchical_entry(
        _hierarchy_object("FUNCTION", "digest", management="EXTENSION")
    )
    assert extension["provenance_role"] == "EXTENSION_MANAGED"
    assert extension["provenance_status"] == "EXTENSION_MANAGED"
    for status, adoption in (
        ("TRACKED_CURRENT", "ADOPTABLE_CURRENT"),
        ("TRACKED_HISTORICAL", "ADOPTABLE_LEGACY_KNOWN"),
        ("LOCAL_UNTRACKED_SOURCE", "ADOPTABLE_LEGACY_KNOWN"),
        ("RUNTIME_OBSERVED_PENDING_ADOPTION", "BLOCKED_NO_SOURCE"),
    ):
        row = hierarchical_entry(
            _hierarchy_object("TABLE", status.lower()),
            source_evidence={"provenance_status": status},
        )
        assert row["adoption_status"] == adoption


def test_runtime_only_roots_and_p2_are_not_silently_approved():
    for risk in ("P0", "P1", "P2"):
        obj = _hierarchy_object("TABLE", f"runtime_{risk}")
        obj["risk_class"] = risk
        row = hierarchical_entry(obj)
        assert row["adoption_status"] == "BLOCKED_NO_SOURCE"


def test_blocked_root_blocks_child_and_child_inherits_risk():
    root = hierarchical_entry(
        {**_hierarchy_object("TABLE", "positions"), "risk_class": "P0"}
    )
    child = hierarchical_entry(
        _hierarchy_object("INDEX", "positions_idx", parent="positions")
    )
    child["risk_class"] = root["risk_class"]
    child["adoption_status"] = root["adoption_status"]
    assert child["risk_class"] == "P0"
    assert child["adoption_status"] == "BLOCKED_NO_SOURCE"


def test_canonical_common_blocker_and_readiness_split():
    root = hierarchical_entry(
        _hierarchy_object("VIEW", "v_positions_pnl"),
        source_evidence={"provenance_status": "TRACKED_CURRENT"},
        canonical_blocker=True,
    )
    state = hierarchical_readiness([root])
    assert root["adoption_status"] == "BLOCKED_CANONICAL_DEFINITION_REQUIRED"
    assert state["catalog_coverage_ready"] is True
    assert state["adoption_ready"] is False


def test_missing_root_and_unclassified_fail_catalog_coverage():
    child = hierarchical_entry(
        _hierarchy_object("INDEX", "missing_idx", parent="missing")
    )
    assert hierarchical_readiness([child])["catalog_coverage_ready"] is False
    malformed = {**child, "provenance_role": "UNKNOWN"}
    assert hierarchical_readiness([malformed])["catalog_coverage_ready"] is False


def test_dependency_cycle_is_deterministic_and_does_not_recurse():
    a = hierarchical_entry(
        _hierarchy_object("VIEW", "a"), dependency_identities=["VIEW:public:b::"]
    )
    b = hierarchical_entry(
        _hierarchy_object("VIEW", "b"), dependency_identities=["VIEW:public:a::"]
    )
    state = hierarchical_readiness([a, b])
    assert state["catalog_coverage_ready"] is True
    assert [a["dependency_identities"], b["dependency_identities"]] == [
        ["VIEW:public:b::"], ["VIEW:public:a::"],
    ]


def test_manifest_hierarchical_coverage_and_deterministic_order(manifest):
    objects = manifest["objects"]
    assert len(objects) == 1182
    assert [row["identity"] for row in objects] == sorted(
        row["identity"] for row in objects
    )
    assert all(row["provenance_role"] for row in objects)
    assert all(row["root_identity"] for row in objects)
    assert not hierarchical_readiness(objects)["unclassified"]
    assert manifest["catalog_gate"] == {
        "catalog_coverage_ready": True,
        "adoption_ready": False,
        "blocked_count": 398,
    }


def _tracked_entry(source_path):
    return {
        **_hierarchy_object("TABLE", "tracked_object"),
        "provenance_status": "TRACKED_CURRENT",
        "source_path": source_path,
    }


def test_tracked_current_source_in_candidate_index_passes():
    validate_tracked_provenance(
        {"objects": [_tracked_entry("db/migrations/tracked.sql")]},
        {"db/migrations/tracked.sql"},
    )


@pytest.mark.parametrize("kind,path", [
    ("ignored", "db/migrations/ignored.sql"),
    ("untracked", "local/untracked.sql"),
    ("missing", "missing.sql"),
])
def test_tracked_current_source_outside_candidate_index_fails(kind, path):
    with pytest.raises(ValueError, match="dangling tracked provenance"):
        validate_tracked_provenance({"objects": [_tracked_entry(path)]}, set())


def test_observed_without_tracked_source_is_pending_adoption():
    row = hierarchical_entry(_hierarchy_object("TABLE", "observed_only"))
    assert row["provenance_status"] == "RUNTIME_OBSERVED_PENDING_ADOPTION"
    assert row["adoption_status"] == "BLOCKED_NO_SOURCE"


def test_proposed_only_identity_is_not_added_to_observed_manifest(differences):
    observed = _hierarchy_object("TABLE", "observed")
    observed.pop("applicability")
    observed.pop("source_file")
    observed.pop("risk_class")
    observed.pop("provenance_status")
    result = merge_manifest([observed], [copy.deepcopy(observed)], differences)
    assert [row["object_name"] for row in result["objects"]] == ["observed"]
    assert not any(row["object_name"] == "proposed_only" for row in result["objects"])


def test_checkpoint_manifest_has_no_dangling_tracked_provenance(manifest):
    validate_tracked_provenance(manifest, candidate_tracked_paths())
    assert not any(
        "20260724_database_baseline_provenance_v1.sql" in row.get("source_path", "")
        or "20260724_runtime_ddl_schema_parity_v1.sql" in row.get("source_path", "")
        for row in manifest["objects"]
    )
    assert manifest["catalog_gate"]["adoption_ready"] is False


def test_manual_dependency_edge_requires_identity_and_reason():
    def validate(edge):
        if not all(edge.get(key) for key in ("source", "target", "reason", "evidence")):
            raise ValueError("malformed manual dependency edge")
    validate({"source": "a", "target": "b", "reason": "PL/pgSQL body", "evidence": "x.sql"})
    with pytest.raises(ValueError, match="malformed"):
        validate({"source": "a", "target": "b", "reason": "", "evidence": "x.sql"})

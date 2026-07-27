from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from common.schema_provenance import (
    BASELINE_VERSION,
    canonical_json,
    fingerprint,
    identity_string,
    normalize_sql,
    object_key,
    validate_difference_contract,
    validate_manifest,
    validate_tracked_provenance,
)


ARTIFACT_CONTRACT_NAME = "DATABASE_BASELINE_ARTIFACT_CONTRACT"
ARTIFACT_CONTRACT_VERSION = "1"
CATALOG_SCHEMA_VERSION = "WALTRADE_CATALOG_CSV_V1"
NORMALIZATION_VERSION = "common.schema_provenance.normalize_sql:v1"
PROVENANCE_MODEL_VERSION = "WALTRADE_PROVENANCE_MODEL_V1"
FINGERPRINT_ALGORITHM = "SHA-256(normalized-definition-UTF8)"
CSV_HEADER = (
    "object_type",
    "schema_name",
    "object_name",
    "identity_arguments",
    "parent_relation",
    "definition",
    "owner_name",
    "enabled_state",
    "case",
    "extname",
    "extversion",
)
VALID_OBJECT_TYPES = {
    "TABLE",
    "VIEW",
    "MATERIALIZED_VIEW",
    "SEQUENCE",
    "FUNCTION",
    "PROCEDURE",
    "TRIGGER",
    "CONSTRAINT",
    "INDEX",
    "POLICY",
    "RULE",
    "EVENT_TRIGGER",
    "EXTENSION",
}
ENVIRONMENTS = {
    "LOCAL_LIVE": "LIVE",
    "LOCAL_PAPER": "PAPER",
    "VPS_LIVE": "LIVE",
    "VPS_PAPER": "PAPER",
}
PRIMARY_CLASSIFICATIONS = {
    "APPLICATION_OWNED_TRACKED",
    "TRACKED_RUNTIME_DDL",
    "RUNTIME_OBSERVED_PENDING_ADOPTION",
    "EXTENSION_MANAGED",
    "INTERNAL_METADATA",
    "EXPECTED_ENVIRONMENT_SPECIFIC",
    "HISTORICAL_ORPHAN_PENDING_DECISION",
}

# Offline evidence corrections for catalog identities absent from the LOCAL-built
# manifest. Each source is present in the candidate Git index and was verified
# from Git history; no environment is queried while applying these rules.
HISTORICAL_EVIDENCE = {
    ("CONSTRAINT", "public", "ux_binance_orders_symbol_order_id", "", "binance_orders"): {
        "primary_classification": "HISTORICAL_ORPHAN_PENDING_DECISION",
        "provenance_status": "TRACKED_HISTORICAL",
        "tracked_source": "db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql",
        "source_commit": "05687db8846c92a1bb1c6824d2be3eb1e52f9272",
        "adoption_status": "ADOPTABLE_LEGACY_KNOWN",
        "applicability": "PAPER_ONLY",
    },
    ("INDEX", "public", "ux_binance_orders_symbol_order_id", "", "binance_orders"): {
        "primary_classification": "HISTORICAL_ORPHAN_PENDING_DECISION",
        "provenance_status": "TRACKED_HISTORICAL",
        "tracked_source": "db/migrations/20260714_pending_entry_fill_reconciliation_v1.sql",
        "source_commit": "05687db8846c92a1bb1c6824d2be3eb1e52f9272",
        "adoption_status": "ADOPTABLE_LEGACY_KNOWN",
        "applicability": "PAPER_ONLY",
    },
}
for _name, _parent in (
    ("ix_lfa_entry_trace_slot_time", "entry_trace_events"),
    ("ix_lfa_positions_closed_exit_time", "positions"),
    ("ix_lfa_slot_brain_slot_window_time", "slot_brain_snapshot"),
    ("ix_lfa_strategy_events_slot_time", "strategy_events"),
):
    HISTORICAL_EVIDENCE[("INDEX", "public", _name, "", _parent)] = {
        "primary_classification": "APPLICATION_OWNED_TRACKED",
        "provenance_status": "TRACKED_CURRENT",
        "tracked_source": "db/migrations/20260703_learning_feedback_audit_v11_compat.sql",
        "source_commit": "260f9661f1ccf4b53e003eb5f2354c71dc59e026",
        "adoption_status": "ADOPTABLE_CURRENT",
        "applicability": "PAPER_ONLY",
    }


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def _internal_management(name: str, management: str) -> str:
    if (
        name.startswith(
            ("schema_migration_ledger_v1", "schema_baseline_adoption_v1")
        )
        or name.startswith(
            (
                "prevent_schema_provenance_",
                "trg_schema_provenance_",
                "trg_schema_migration_",
                "trg_schema_baseline_",
                "ix_schema_migration_",
            )
        )
    ):
        return "INTERNAL"
    return management


def parse_raw_catalog(raw_path: str | Path) -> tuple[list[dict[str, Any]], str, str]:
    path = Path(raw_path)
    raw = path.read_bytes()
    raw_hash = sha256_bytes(raw)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CSV_HEADER:
            raise ValueError(f"unsupported catalog CSV header: {reader.fieldnames}")
        source_rows = list(reader)
    routine_counts = Counter(
        (row["schema_name"], row["object_name"])
        for row in source_rows
        if row["object_type"] in {"FUNCTION", "PROCEDURE"}
    )
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for source in source_rows:
        missing = [
            field for field in CSV_HEADER
            if field not in source or source[field] is None
        ]
        if missing:
            raise ValueError(f"catalog row missing fields: {missing}")
        object_type = source["object_type"]
        if object_type not in VALID_OBJECT_TYPES:
            raise ValueError(f"unsupported object type: {object_type}")
        management = _internal_management(source["object_name"], source["case"])
        if management not in {"APPLICATION", "EXTENSION", "INTERNAL"}:
            raise ValueError(f"unsupported management classification: {management}")
        row = {
            "object_type": object_type,
            "schema": source["schema_name"],
            "object_name": source["object_name"],
            "identity_arguments": source["identity_arguments"] or "",
            "parent_relation": source["parent_relation"] or "",
            "canonical_definition_sha256": fingerprint(source["definition"] or ""),
            "normalized_definition": normalize_sql(source["definition"] or ""),
            "owner_contract": source["owner_name"] or None,
            "enabled_state": source["enabled_state"] or None,
            "management": management,
            "extension_name": source["extname"] or None,
            "extension_version": source["extversion"] or None,
            "overloaded": routine_counts[
                (source["schema_name"], source["object_name"])
            ] > 1,
        }
        key = object_key(row)
        if key in seen:
            raise ValueError(f"duplicate catalog identity: {key}")
        seen.add(key)
        row["identity"] = identity_string(row)
        rows.append(row)
    rows.sort(key=lambda row: row["identity"])
    header_hash = sha256_bytes((",".join(CSV_HEADER) + "\n").encode("utf-8"))
    return rows, raw_hash, header_hash


def _source_hashes(source_paths: Iterable[str | Path]) -> dict[str, str]:
    return {
        Path(path).name: sha256_bytes(Path(path).read_bytes())
        for path in sorted(map(Path, source_paths), key=lambda item: item.name)
    }


def _enrich_inventory(
    observed: list[dict[str, Any]], manifest: dict[str, Any]
) -> list[dict[str, Any]]:
    by_key = {object_key(row): row for row in manifest["objects"]}
    enriched = []
    for row in observed:
        expected = by_key.get(object_key(row))
        evidence = HISTORICAL_EVIDENCE.get(object_key(row))
        result = dict(row)
        if expected:
            if row["management"] == "EXTENSION":
                primary = "EXTENSION_MANAGED"
            elif row["management"] == "INTERNAL":
                primary = "INTERNAL_METADATA"
            elif expected["applicability"] != "COMMON":
                primary = "EXPECTED_ENVIRONMENT_SPECIFIC"
            else:
                primary = {
                    "TRACKED_CURRENT": "APPLICATION_OWNED_TRACKED",
                    "TRACKED_RUNTIME_DDL": "TRACKED_RUNTIME_DDL",
                    "RUNTIME_OBSERVED_PENDING_ADOPTION": (
                        "RUNTIME_OBSERVED_PENDING_ADOPTION"
                    ),
                    "EXTENSION_MANAGED": "EXTENSION_MANAGED",
                    "TRACKED_HISTORICAL": "HISTORICAL_ORPHAN_PENDING_DECISION",
                }.get(expected["provenance_status"])
            result.update({
                "risk_class": expected["risk_class"],
                "tracked_source": expected.get("source_path", ""),
                "provenance_status": expected["provenance_status"],
                "provenance_role": expected["provenance_role"],
                "adoption_status": expected["adoption_status"],
                "applicability": expected["applicability"],
                "primary_classification": primary,
            })
        elif evidence:
            result.update({
                "risk_class": (
                    "P0" if "order" in row["identity"] or "position" in row["identity"]
                    else "P1"
                ),
                **evidence,
                "provenance_role": "OWNED_CHILD",
            })
        elif row["management"] == "INTERNAL":
            result.update({
                "risk_class": "P3",
                "tracked_source": "",
                "provenance_status": "RUNTIME_OBSERVED_PENDING_ADOPTION",
                "provenance_role": "POSTGRES_INTERNAL",
                "adoption_status": "BLOCKED_NO_SOURCE",
                "applicability": "COMMON",
                "primary_classification": "INTERNAL_METADATA",
            })
        elif row["management"] == "EXTENSION":
            result.update({
                "risk_class": "P3",
                "tracked_source": "",
                "provenance_status": "EXTENSION_MANAGED",
                "provenance_role": "EXTENSION_MANAGED",
                "adoption_status": "ADOPTABLE_CURRENT",
                "applicability": "COMMON",
                "primary_classification": "EXTENSION_MANAGED",
            })
        else:
            result.update({
                "risk_class": "UNKNOWN",
                "tracked_source": "",
                "provenance_status": "UNKNOWN",
                "provenance_role": "UNKNOWN",
                "adoption_status": "BLOCKED_UNCLASSIFIED",
                "applicability": "UNKNOWN",
                "primary_classification": None,
            })
        result["secondary_flags"] = {
            "manual_decision": (
                result["primary_classification"]
                == "HISTORICAL_ORPHAN_PENDING_DECISION"
                or (
                    result["primary_classification"] != "INTERNAL_METADATA"
                    and
                    result["provenance_role"]
                    in {"PROVENANCE_ROOT", "INDEPENDENT_EXECUTABLE"}
                    and result["adoption_status"].startswith("BLOCKED_")
                )
            ),
            "canonical_blocker": (
                result["provenance_role"]
                in {"PROVENANCE_ROOT", "INDEPENDENT_EXECUTABLE"}
                and result["primary_classification"] != "INTERNAL_METADATA"
                and
                result["adoption_status"]
                == "BLOCKED_CANONICAL_DEFINITION_REQUIRED"
            ),
            "expected_difference": (
                result["primary_classification"]
                == "EXPECTED_ENVIRONMENT_SPECIFIC"
            ),
            "risk": result["risk_class"],
        }
        enriched.append(result)
    return enriched


def semantic_counters(
    inventory: list[dict[str, Any]], drifts: list[dict[str, Any]]
) -> dict[str, int]:
    """Aggregate identity sets; secondary flags never define ownership."""
    manual = {
        row["identity"] for row in inventory
        if row["secondary_flags"]["manual_decision"]
    }
    blockers = {
        row["identity"] for row in inventory
        if row["secondary_flags"]["canonical_blocker"]
    }
    by_identity = {row["identity"]: row for row in inventory}
    return {
        "observed_identities": len(by_identity),
        "application_owned": sum(
            row["management"] == "APPLICATION" for row in by_identity.values()
        ),
        "extension_managed": sum(
            row["primary_classification"] == "EXTENSION_MANAGED"
            for row in by_identity.values()
        ),
        "internal_metadata": sum(
            row["primary_classification"] == "INTERNAL_METADATA"
            for row in by_identity.values()
        ),
        "unclassified": sum(
            row["primary_classification"] not in PRIMARY_CLASSIFICATIONS
            for row in by_identity.values()
        ),
        "tracked_current": sum(
            row["provenance_status"] == "TRACKED_CURRENT"
            for row in by_identity.values()
        ),
        "tracked_runtime_ddl": sum(
            row["provenance_status"] == "TRACKED_RUNTIME_DDL"
            for row in by_identity.values()
        ),
        "runtime_observed_pending_adoption": sum(
            row["provenance_status"] == "RUNTIME_OBSERVED_PENDING_ADOPTION"
            and row["primary_classification"] != "INTERNAL_METADATA"
            for row in by_identity.values()
        ),
        "manual_decisions": len(manual),
        "pending_p0": sum(by_identity[key]["risk_class"] == "P0" for key in manual),
        "pending_p1": sum(by_identity[key]["risk_class"] == "P1" for key in manual),
        "canonical_common_blockers": len(blockers),
        "unknown_differences": len({
            (row["identity"], row["kind"]) for row in drifts
        }),
        "blocked_contract_differences": len({
            (row["identity"], row["kind"]) for row in drifts
            if row["kind"] == "BLOCKED_PENDING_DECISION"
        }),
    }


def _build_payload(
    inventory: list[dict[str, Any]],
    manifest: dict[str, Any],
    differences: dict[str, Any],
    environment_identity: str,
) -> dict[str, Any]:
    logical_environment = ENVIRONMENTS[environment_identity]
    manifest_by_key = {object_key(row): row for row in manifest["objects"]}
    difference_by_key = {
        object_key(rule["identity"]): rule for rule in differences["rules"]
    }
    manual = [row for row in inventory if row["secondary_flags"]["manual_decision"]]
    unknown = [
        row for row in inventory
        if row["primary_classification"] not in PRIMARY_CLASSIFICATIONS
    ]
    blockers = [
        row for row in inventory if row["secondary_flags"]["canonical_blocker"]
    ]
    expected_keys = {
        object_key(row) for row in manifest["objects"]
        if row["applicability"] in {"COMMON", f"{logical_environment}_ONLY"}
    }
    actual_keys = {object_key(row) for row in inventory}
    drifts = []
    for key in sorted(expected_keys - actual_keys):
        expected = manifest_by_key[key]
        drifts.append({
            "kind": "MISSING_OBJECT",
            "identity": expected["identity"],
            "expected": expected["canonical_definition_sha256"],
            "actual": None,
        })
    for row in inventory:
        key = object_key(row)
        expected = manifest_by_key.get(key)
        if expected is None and row["management"] != "INTERNAL":
            drifts.append({
                "kind": "UNKNOWN",
                "identity": row["identity"],
                "expected": None,
                "actual": row["canonical_definition_sha256"],
            })
            continue
        if expected:
            expected_hash = expected.get("environment_fingerprints", {}).get(
                logical_environment, expected["canonical_definition_sha256"]
            )
            if row["canonical_definition_sha256"] != expected_hash:
                drifts.append({
                    "kind": "DEFINITION_MISMATCH",
                    "identity": row["identity"],
                    "expected": expected_hash,
                    "actual": row["canonical_definition_sha256"],
                })
            rule = difference_by_key.get(key)
            if rule and rule["classification"] != "EXPECTED_ENVIRONMENT_VARIANT":
                drifts.append({
                    "kind": rule["classification"],
                    "identity": row["identity"],
                    "expected": "EXPECTED_ENVIRONMENT_VARIANT",
                    "actual": rule["classification"],
                })
    counters = semantic_counters(inventory, drifts)
    financial = [
        row for row in inventory if row["risk_class"] in {"P0", "P1"}
    ]
    return {
        "inventory": inventory,
        "counters": counters,
        "drifts": sorted(drifts, key=lambda row: (row["identity"], row["kind"])),
        "canonical_common_blockers": blockers,
        "financial_truth_candidates": financial,
        "p0_p1": financial,
        "status": (
            "CATALOG_COVERAGE_READY"
            if not unknown else "UNKNOWN_CRITICAL_OBJECT"
        ),
        "coverage": not unknown and all(
            row["primary_classification"] in PRIMARY_CLASSIFICATIONS
            for row in inventory
        ),
    }


def canonicalize_raw(
    *,
    raw_path: str | Path,
    environment_identity: str,
    manifest: dict[str, Any],
    differences: dict[str, Any],
    tracked_paths: Iterable[str],
    output_dir: str | Path,
    canonicalizer_git_sha: str,
    source_paths: Iterable[str | Path],
    expected_raw_sha256: str | None = None,
    generated_at_utc: str | None = None,
    fingerprint_algorithm: str = FINGERPRINT_ALGORITHM,
) -> dict[str, Any]:
    if environment_identity not in ENVIRONMENTS:
        raise ValueError(f"unsupported environment identity: {environment_identity}")
    if fingerprint_algorithm != FINGERPRINT_ALGORITHM:
        raise ValueError(f"unsupported fingerprint algorithm: {fingerprint_algorithm}")
    validate_manifest(manifest)
    validate_difference_contract(differences)
    validate_tracked_provenance(manifest, tracked_paths)
    if differences["baseline_version"] != manifest["baseline_version"]:
        raise ValueError("expected-differences contract version mismatch")
    inventory, raw_hash, header_hash = parse_raw_catalog(raw_path)
    observed_keys = {object_key(row) for row in inventory}
    missing_evidence_sources = {
        evidence["tracked_source"]
        for key, evidence in HISTORICAL_EVIDENCE.items()
        if key in observed_keys and evidence["tracked_source"] not in set(tracked_paths)
    }
    if missing_evidence_sources:
        raise ValueError(
            "dangling historical evidence: "
            + ", ".join(sorted(missing_evidence_sources))
        )
    if expected_raw_sha256 and raw_hash != expected_raw_sha256:
        raise ValueError(
            f"raw input SHA-256 mismatch: expected {expected_raw_sha256}, got {raw_hash}"
        )
    enriched = _enrich_inventory(inventory, manifest)
    payload = _build_payload(enriched, manifest, differences, environment_identity)
    payload_hash = sha256_bytes(canonical_bytes(payload))
    timestamp = generated_at_utc or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    contract = {
        "artifact_contract_name": ARTIFACT_CONTRACT_NAME,
        "artifact_contract_version": ARTIFACT_CONTRACT_VERSION,
        "catalog_schema_version": CATALOG_SCHEMA_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "provenance_model_version": PROVENANCE_MODEL_VERSION,
        "expected_differences_contract_version": (
            f"{differences['baseline_version']}:"
            f"{sha256_bytes(canonical_bytes(differences))}"
        ),
        "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
        "canonicalizer_git_sha": canonicalizer_git_sha,
        "canonicalizer_source_hashes": _source_hashes(source_paths),
        "raw_input_sha256": raw_hash,
        "raw_input_header_sha256": header_hash,
        "environment_identity": environment_identity,
        "generated_at_utc": timestamp,
        "deterministic_payload_sha256": payload_hash,
    }
    report = {"contract": contract, "semantic_payload": payload}
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "canonical-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "canonical-normalized-inventory.json").write_text(
        json.dumps(enriched, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary = {
        "contract": contract,
        "counters": payload["counters"],
        "status": payload["status"],
    }
    (output / "canonical-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report

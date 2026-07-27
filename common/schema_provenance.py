from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


BASELINE_VERSION = "WALTRADE_DATABASE_BASELINE_V1"
VALID_ENVIRONMENTS = {"LIVE", "PAPER"}
VALID_APPLICABILITY = {"COMMON", "LIVE_ONLY", "PAPER_ONLY"}
VALID_MANAGEMENT = {"APPLICATION", "EXTENSION", "INTERNAL"}
VALID_PROVENANCE_ROLES = {
    "PROVENANCE_ROOT",
    "OWNED_CHILD",
    "INDEPENDENT_EXECUTABLE",
    "EXTENSION_MANAGED",
    "POSTGRES_INTERNAL",
}
VALID_ADOPTION_STATUSES = {
    "ADOPTABLE_CURRENT",
    "ADOPTABLE_LEGACY_KNOWN",
    "BLOCKED_NO_SOURCE",
    "BLOCKED_CANONICAL_DEFINITION_REQUIRED",
    "BLOCKED_UNCLASSIFIED",
}
VALID_PROVENANCE_STATUSES = {
    "TRACKED_CURRENT",
    "TRACKED_RUNTIME_DDL",
    "TRACKED_HISTORICAL",
    "LOCAL_UNTRACKED_SOURCE",
    "RUNTIME_OBSERVED_PENDING_ADOPTION",
    "EXTENSION_MANAGED",
}
VALID_DIFFERENCE_CLASSIFICATIONS = {
    "EXPECTED_ENVIRONMENT_VARIANT",
    "UNEXPECTED_DRIFT",
    "BLOCKED_PENDING_DECISION",
}
GATE_RESULTS = {
    "READY",
    "DRIFT_DETECTED",
    "CHECKSUM_CONFLICT",
    "UNKNOWN_CRITICAL_OBJECT",
    "ENVIRONMENT_CONTRACT_VIOLATION",
    "BLOCKED_PENDING_DECISION",
}


def normalize_sql(value: str) -> str:
    """Canonicalize catalog SQL without hiding semantically relevant tokens."""
    value = re.sub(r"/\*.*?\*/", " ", value or "", flags=re.S)
    value = re.sub(r"(?m)^\s*--.*$", " ", value)
    value = re.sub(
        r"(?im)^\s*(?:SET\s+.+?|SELECT\s+pg_catalog\.set_config\(.+?\));?\s*$",
        " ",
        value,
    )
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\s*([(),;=])\s*", r"\1", value)
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(value: str | Any) -> str:
    canonical = normalize_sql(value) if isinstance(value, str) else canonical_json(value)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def object_key(entry: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        entry["object_type"],
        entry["schema"],
        entry["object_name"],
        entry.get("identity_arguments", ""),
        entry.get("parent_relation", ""),
    )


def normalize_owner(value: str | None) -> str:
    return (value or "").strip().lower()


def identity_string(entry: dict[str, Any]) -> str:
    typ, schema, name, args, parent = object_key(entry)
    return ":".join((typ, schema, name, args, parent))


def classify_provenance_role(entry: dict[str, Any]) -> str:
    management = entry.get("management", "APPLICATION")
    if management == "EXTENSION":
        return "EXTENSION_MANAGED"
    if management == "INTERNAL":
        return "POSTGRES_INTERNAL"
    typ = entry["object_type"]
    if typ in {"CONSTRAINT", "INDEX", "SEQUENCE"}:
        return "OWNED_CHILD"
    if typ in {"FUNCTION", "PROCEDURE", "TRIGGER", "RULE"}:
        return "INDEPENDENT_EXECUTABLE"
    return "PROVENANCE_ROOT"


P0_TERMS = (
    "position", "pnl", "order", "fill", "execution", "reconcil", "risk",
    "capital", "sizing", "orc_", "learning", "final_decision", "decision_outcome",
)
P1_TERMS = (
    "control", "auth", "user_", "api_", "audit", "notification", "heartbeat",
    "scheduler", "automation", "readiness", "migration",
)


def classify_root_risk(entry: dict[str, Any]) -> str:
    text = " ".join((
        entry.get("object_name", ""),
        entry.get("parent_relation", ""),
        entry.get("definition", ""),
    )).lower()
    if any(term in text for term in P0_TERMS):
        return "P0"
    if any(term in text for term in P1_TERMS):
        return "P1"
    if entry["object_type"] in {
        "TABLE", "VIEW", "MATERIALIZED_VIEW", "FUNCTION", "PROCEDURE",
        "TRIGGER", "POLICY", "RULE", "EVENT_TRIGGER",
    }:
        return "P2"
    return "P3"


def hierarchical_entry(
    entry: dict[str, Any],
    *,
    dependency_identities: Iterable[str] = (),
    sequence_root: str | None = None,
    source_evidence: dict[str, str] | None = None,
    canonical_blocker: bool = False,
) -> dict[str, Any]:
    result = dict(entry)
    prior_provenance_status = result.get("provenance_status", "")
    role = classify_provenance_role(result)
    identity = identity_string(result)
    parent_identity = ""
    if role == "OWNED_CHILD":
        if result["object_type"] == "SEQUENCE":
            parent_identity = sequence_root or ""
        elif result.get("parent_relation"):
            parent_identity = ":".join((
                "TABLE", result.get("schema", "public"),
                result["parent_relation"], "", "",
            ))
    root_identity = parent_identity if role == "OWNED_CHILD" else identity
    evidence = source_evidence or {}
    provenance = evidence.get(
        "provenance_status", "RUNTIME_OBSERVED_PENDING_ADOPTION"
    )
    source_path = evidence.get("source_path", "")
    source_commit = evidence.get("source_commit", "")
    risk = result.get("risk_class") or classify_root_risk(result)
    if role == "EXTENSION_MANAGED":
        provenance = "EXTENSION_MANAGED"
        adoption = "ADOPTABLE_CURRENT"
        risk = "P3"
    elif canonical_blocker:
        adoption = "BLOCKED_CANONICAL_DEFINITION_REQUIRED"
    elif provenance in {"TRACKED_CURRENT", "TRACKED_RUNTIME_DDL"}:
        adoption = "ADOPTABLE_CURRENT"
    elif provenance in {"TRACKED_HISTORICAL", "LOCAL_UNTRACKED_SOURCE"}:
        adoption = "ADOPTABLE_LEGACY_KNOWN"
    else:
        adoption = "BLOCKED_NO_SOURCE"
    result.update({
        "identity": identity,
        "provenance_role": role,
        "parent_identity": parent_identity,
        "root_identity": root_identity,
        "dependency_identities": sorted(set(dependency_identities)),
        "provenance_status": provenance,
        "source_path": source_path,
        "source_commit": source_commit,
        "risk_class": risk,
        "adoption_status": adoption,
        "environment_applicability": result.get("applicability", "COMMON"),
    })
    if prior_provenance_status == "BASELINE_ADOPTED_LEGACY_P0":
        result["legacy_baseline_status"] = prior_provenance_status
    return result


def hierarchical_readiness(objects: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(objects)
    identities = {row.get("identity") for row in rows}
    unclassified = [
        row for row in rows
        if row.get("provenance_role") not in VALID_PROVENANCE_ROLES
        or row.get("adoption_status") not in VALID_ADOPTION_STATUSES
    ]
    missing_roots = [
        row for row in rows
        if row.get("provenance_role") == "OWNED_CHILD"
        and (
            not row.get("root_identity")
            or row.get("root_identity") not in identities
        )
    ]
    coverage = not unclassified and not missing_roots and all(
        row.get("canonical_definition_sha256") for row in rows
    )
    blocked = [
        row for row in rows
        if row.get("adoption_status", "").startswith("BLOCKED_")
    ]
    return {
        "catalog_coverage_ready": coverage,
        "adoption_ready": coverage and not blocked,
        "unclassified": unclassified,
        "missing_roots": missing_roots,
        "blocked": blocked,
    }


def _require_unique_actual(
    actual: Iterable[dict[str, Any]],
) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    result: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for entry in actual:
        key = object_key(entry)
        if key in result:
            raise ValueError(f"duplicate actual inventory identity: {key}")
        if entry["object_type"] in {"FUNCTION", "PROCEDURE"} and not entry.get(
            "identity_arguments"
        ) and entry.get("overloaded"):
            raise ValueError(f"overloaded routine missing identity arguments: {key}")
        result[key] = entry
    return result


def applies_to(entry: dict[str, Any], environment: str) -> bool:
    environment = environment.upper()
    applicability = entry["applicability"]
    return applicability == "COMMON" or applicability == f"{environment}_ONLY"


def validate_manifest(manifest: dict[str, Any]) -> None:
    if manifest.get("baseline_version") != BASELINE_VERSION:
        raise ValueError("unsupported baseline_version")
    seen: set[tuple[str, str, str, str, str, str]] = set()
    for entry in manifest.get("objects", []):
        missing = {
            "object_type", "schema", "object_name", "applicability",
            "source_file", "canonical_definition_sha256", "risk_class",
            "provenance_status",
        } - set(entry)
        if missing:
            raise ValueError(f"manifest entry missing fields: {sorted(missing)}")
        if entry["applicability"] not in VALID_APPLICABILITY:
            raise ValueError(f"invalid applicability: {entry['applicability']}")
        if not re.fullmatch(r"[0-9a-f]{64}", entry["canonical_definition_sha256"]):
            raise ValueError(f"invalid fingerprint: {object_key(entry)}")
        if entry.get("management", "APPLICATION") not in VALID_MANAGEMENT:
            raise ValueError(f"invalid management classification: {object_key(entry)}")
        if entry.get("management", "APPLICATION") == "APPLICATION" and not normalize_owner(
            entry.get("owner_contract")
        ):
            raise ValueError(f"application object missing owner contract: {object_key(entry)}")
        if entry["provenance_status"] not in VALID_PROVENANCE_STATUSES:
            raise ValueError(
                f"invalid provenance status: {entry['provenance_status']}: "
                f"{object_key(entry)}"
            )
        variants = entry.get("environment_fingerprints", {})
        if entry["applicability"] == "COMMON" and set(variants) not in (
            set(), {"LIVE", "PAPER"}
        ):
            raise ValueError(f"COMMON environment variants incomplete: {object_key(entry)}")
        identity = (*object_key(entry), entry["applicability"])
        if identity in seen:
            raise ValueError(f"duplicate manifest entry: {identity}")
        seen.add(identity)


def validate_tracked_provenance(
    manifest: dict[str, Any], tracked_paths: Iterable[str]
) -> None:
    """Reject tracked provenance absent from the candidate Git index."""
    tracked = set(tracked_paths)
    for entry in manifest.get("objects", []):
        if entry.get("provenance_status") not in {
            "TRACKED_CURRENT", "TRACKED_RUNTIME_DDL"
        }:
            continue
        source_path = entry.get("source_path", "")
        if not source_path or source_path not in tracked:
            raise ValueError(
                f"dangling tracked provenance: {identity_string(entry)}: {source_path}"
            )


def validate_difference_contract(contract: dict[str, Any]) -> None:
    if contract.get("baseline_version") != BASELINE_VERSION:
        raise ValueError("difference contract baseline_version mismatch")
    seen: set[tuple[str, str, str, str, str]] = set()
    for rule in contract.get("rules", []):
        identity = rule.get("identity", {})
        missing = {
            "object_type", "schema", "object_name", "identity_arguments",
            "parent_relation",
        } - set(identity)
        if missing:
            raise ValueError(f"difference rule missing identity fields: {sorted(missing)}")
        key = object_key(identity)
        if key in seen:
            raise ValueError(f"duplicate difference rule: {key}")
        seen.add(key)
        if rule.get("expected_applicability") not in VALID_APPLICABILITY:
            raise ValueError(f"invalid expected applicability: {key}")
        if rule.get("classification") not in VALID_DIFFERENCE_CLASSIFICATIONS:
            raise ValueError(f"invalid difference classification: {key}")
        for field in ("reason", "risk_class", "source_evidence"):
            if not str(rule.get(field, "")).strip():
                raise ValueError(f"difference rule missing {field}: {key}")
        if (
            rule["risk_class"] == "P0"
            and rule["classification"] == "EXPECTED_ENVIRONMENT_VARIANT"
        ):
            raise ValueError(f"P0 difference cannot be automatically expected: {key}")


def difference_rule_map(
    contract: dict[str, Any],
) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    validate_difference_contract(contract)
    return {object_key(rule["identity"]): rule for rule in contract["rules"]}


@dataclass(frozen=True)
class Drift:
    kind: str
    object_type: str
    schema: str
    object_name: str
    expected: str | None
    actual: str | None
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def compare_inventory(
    manifest: dict[str, Any],
    actual: Iterable[dict[str, Any]],
    environment: str,
    *,
    checksum_conflict: bool = False,
    difference_contract: dict[str, Any] | None = None,
) -> tuple[str, list[Drift]]:
    environment = environment.upper()
    if environment not in VALID_ENVIRONMENTS:
        raise ValueError(f"invalid environment: {environment}")
    validate_manifest(manifest)
    expected = {
        object_key(e): e for e in manifest["objects"] if applies_to(e, environment)
    }
    approved_all = {object_key(e): e for e in manifest["objects"]}
    actual_map = _require_unique_actual(actual)
    rules = difference_rule_map(difference_contract) if difference_contract else {}
    drifts: list[Drift] = []
    for key, entry in sorted(expected.items()):
        observed = actual_map.get(key)
        expected_hash = entry.get("environment_fingerprints", {}).get(
            environment, entry["canonical_definition_sha256"]
        )
        if observed is None:
            drifts.append(Drift("MISSING_OBJECT", *key[:3], expected_hash, None, str(key)))
            continue
        actual_hash = observed["canonical_definition_sha256"]
        management = observed.get("management", "APPLICATION")
        expected_management = entry.get("management", "APPLICATION")
        if management != expected_management:
            drifts.append(Drift(
                "MANAGEMENT_MISMATCH", *key[:3], expected_management, management, str(key)
            ))
        if management == "EXTENSION":
            if (
                entry.get("extension_name") != observed.get("extension_name")
                or entry.get("extension_version") != observed.get("extension_version")
            ):
                drifts.append(Drift(
                    "EXTENSION_VERSION_MISMATCH", *key[:3],
                    f"{entry.get('extension_name')}:{entry.get('extension_version')}",
                    f"{observed.get('extension_name')}:{observed.get('extension_version')}",
                    str(key),
                ))
            continue
        if actual_hash != expected_hash:
            drifts.append(Drift(
                "DEFINITION_MISMATCH", *key[:3], expected_hash, actual_hash, str(key)
            ))
        expected_enabled = entry.get("environment_enabled_states", {}).get(
            environment, entry.get("enabled_state")
        )
        if expected_enabled != observed.get("enabled_state"):
            drifts.append(Drift(
                "ENABLED_STATE_MISMATCH", *key[:3],
                expected_enabled, observed.get("enabled_state"), str(key)
            ))
        expected_owner = normalize_owner(entry.get("owner_contract"))
        actual_owner = normalize_owner(observed.get("owner_contract"))
        if expected_owner != actual_owner:
            drifts.append(Drift(
                "OWNER_MISMATCH", *key[:3], expected_owner, actual_owner, str(key)
            ))
        if "environment_fingerprints" in entry:
            rule = rules.get(key)
            if rule is None:
                drifts.append(Drift(
                    "UNMATCHED_ENVIRONMENT_DIFFERENCE", *key[:3], None, environment, str(key)
                ))
            elif rule["classification"] != "EXPECTED_ENVIRONMENT_VARIANT":
                drifts.append(Drift(
                    rule["classification"], *key[:3],
                    "EXPECTED_ENVIRONMENT_VARIANT", rule["classification"], rule["reason"]
                ))
    for key, observed in sorted(actual_map.items()):
        approved = approved_all.get(key)
        if approved is None:
            if observed.get("management") == "INTERNAL":
                continue
            if observed.get("management") == "EXTENSION":
                drifts.append(Drift(
                    "UNMANIFESTED_EXTENSION", *key[:3], None,
                    f"{observed.get('extension_name')}:{observed.get('extension_version')}",
                    str(key),
                ))
                continue
            drifts.append(Drift(
                "UNKNOWN_CRITICAL_OBJECT", *key[:3], None,
                observed["canonical_definition_sha256"], str(key)
            ))
        elif not applies_to(approved, environment):
            drifts.append(Drift(
                "ENVIRONMENT_CONTRACT_VIOLATION", *key[:3],
                approved["applicability"], environment, str(key)
            ))
    if rules:
        manifested = {object_key(e) for e in manifest["objects"]}
        for key, rule in sorted(rules.items()):
            if key not in manifested:
                drifts.append(Drift(
                    "STALE_EXPECTED_DIFFERENCE", *key[:3],
                    rule["expected_applicability"], None, str(key),
                ))
    if checksum_conflict:
        return "CHECKSUM_CONFLICT", drifts
    kinds = {d.kind for d in drifts}
    if "UNKNOWN_CRITICAL_OBJECT" in kinds:
        return "UNKNOWN_CRITICAL_OBJECT", drifts
    if "ENVIRONMENT_CONTRACT_VIOLATION" in kinds:
        return "ENVIRONMENT_CONTRACT_VIOLATION", drifts
    if "BLOCKED_PENDING_DECISION" in kinds:
        return "BLOCKED_PENDING_DECISION", drifts
    if drifts:
        return "DRIFT_DETECTED", drifts
    return "READY", []


def load_manifest(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    validate_manifest(manifest)
    return manifest

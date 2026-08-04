#!/usr/bin/env python3
"""Compare LOCAL and VPS WALTRADE_PARAMETER_EXPORT_V1 artifacts offline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

try:
    from scripts.export_effective_parameters_v1 import (
        CONTRACT_VERSION,
        canonical_sha256,
        record_identity,
    )
except ModuleNotFoundError:  # Direct execution: python scripts/compare_....py
    from export_effective_parameters_v1 import (  # type: ignore[no-redef]
        CONTRACT_VERSION,
        canonical_sha256,
        record_identity,
    )


PASS_CLASSES = {"MATCH", "ALLOWED_DIFFERENCE"}


class ComparisonContractError(RuntimeError):
    pass


def _load(path: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as source:
        return json.load(source)


def _index(document: dict[str, Any]) -> dict[tuple[str, ...], dict[str, Any]]:
    if document.get("contract_version") != CONTRACT_VERSION:
        raise ComparisonContractError("unsupported parameter export contract")
    if document.get("record_count") != len(document.get("records", [])):
        raise ComparisonContractError("record_count mismatch")
    if document.get("canonical_sha256") != canonical_sha256(document):
        raise ComparisonContractError("canonical_sha256 mismatch")
    result = {}
    for record in document["records"]:
        identity = record_identity(record)
        if identity in result:
            raise ComparisonContractError(f"duplicate record identity: {identity}")
        result[identity] = record
    return result


def _allowed_index(manifest: dict[str, Any]) -> dict[tuple[str, ...], dict[str, Any]]:
    if manifest.get("contract_version") != "PARAMETER_PARITY_ALLOWED_DIFFERENCES_V1":
        raise ComparisonContractError("unsupported allowed-differences contract")
    result = {}
    for entry in manifest.get("entries", []):
        identity = tuple(str(entry[field]) for field in (
            "strategy", "symbol", "interval", "parameter_name",
        ))
        if identity in result:
            raise ComparisonContractError(f"duplicate allowed identity: {identity}")
        result[identity] = entry
    return result


def compare_documents(
    local: dict[str, Any],
    vps: dict[str, Any],
    allowed_manifest: dict[str, Any],
) -> dict[str, Any]:
    local_rows = _index(local)
    vps_rows = _index(vps)
    allowed = _allowed_index(allowed_manifest)
    findings: list[dict[str, Any]] = []

    if local.get("environment") != vps.get("environment") or local.get("mode") != vps.get("mode"):
        findings.append({
            "classification": "UNEXPECTED_DIFFERENCE",
            "identity": None,
            "detail": "environment/mode mismatch",
        })

    for identity in sorted(local_rows.keys() | vps_rows.keys()):
        left = local_rows.get(identity)
        right = vps_rows.get(identity)
        if left is None:
            classification = "EXTRA_RECORD"
            detail = "record exists only in VPS export"
        elif right is None:
            classification = "MISSING_RECORD"
            detail = "record is missing from VPS export"
        else:
            provenance_fields = (
                "value_type", "source_layer", "source_identity", "source_priority",
                "runtime_service", "runtime_child_identity",
            )
            changed_provenance = [
                field for field in provenance_fields if left.get(field) != right.get(field)
            ]
            if changed_provenance:
                classification = "SOURCE_PROVENANCE_DRIFT"
                detail = ",".join(changed_provenance)
            elif left.get("effective_value") == right.get("effective_value"):
                classification = "MATCH"
                detail = None
            else:
                exception = allowed.get(identity)
                is_paper = local.get("environment") == "PAPER" and vps.get("environment") == "PAPER"
                if (
                    exception
                    and is_paper
                    and exception.get("environment_scope") == "PAPER_ONLY"
                    and exception.get("LOCAL_value") == left.get("effective_value")
                    and exception.get("VPS_value") == right.get("effective_value")
                    and exception.get("decision") == "INSUFFICIENT_EVIDENCE_RETAIN_DRIFT"
                    and exception.get("blocking") is False
                    and exception.get("LIVE_impact") is False
                ):
                    classification = "ALLOWED_DIFFERENCE"
                    detail = exception["decision"]
                else:
                    classification = "UNEXPECTED_DIFFERENCE"
                    detail = "effective_value"
        findings.append({
            "classification": classification,
            "identity": list(identity),
            "local_value": None if left is None else left.get("effective_value"),
            "vps_value": None if right is None else right.get("effective_value"),
            "detail": detail,
        })

    counts: dict[str, int] = {}
    for finding in findings:
        name = finding["classification"]
        counts[name] = counts.get(name, 0) + 1
    passed = all(finding["classification"] in PASS_CLASSES for finding in findings)
    return {"status": "PASS" if passed else "FAIL", "counts": counts, "findings": findings}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--local", required=True)
    result.add_argument("--vps", required=True)
    result.add_argument("--allowed-differences", required=True)
    result.add_argument("--output")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = compare_documents(
            _load(args.local), _load(args.vps), _load(args.allowed_differences)
        )
    except (OSError, ValueError, ComparisonContractError) as exc:
        result = {"status": "FAIL", "error": str(exc), "counts": {}, "findings": []}
    serialized = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized, encoding="utf-8", newline="\n")
        print(json.dumps({
            "status": result["status"],
            "counts": result.get("counts", {}),
            "output": str(output),
        }, sort_keys=True))
    else:
        print(serialized, end="")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from common.database_baseline_artifact import (
    ARTIFACT_CONTRACT_VERSION,
    CSV_HEADER,
    FINGERPRINT_ALGORITHM,
    HISTORICAL_EVIDENCE,
    PRIMARY_CLASSIFICATIONS,
    canonicalize_raw,
    semantic_counters,
)
from common.schema_provenance import BASELINE_VERSION, fingerprint
from scripts import waltrade_schema_baseline_v1 as cli


def _manifest():
    return {
        "baseline_version": BASELINE_VERSION,
        "generated_from_git_sha": "a" * 40,
        "normalization_contract": "common.schema_provenance.normalize_sql:v1",
        "objects": [{
            "object_type": "TABLE",
            "schema": "public",
            "object_name": "sample",
            "identity_arguments": "",
            "parent_relation": "",
            "applicability": "COMMON",
            "source_file": "db/sample.sql",
            "source_path": "db/sample.sql",
            "source_commit": "a" * 40,
            "canonical_definition_sha256": fingerprint("id bigint"),
            "risk_class": "P0",
            "provenance_status": "TRACKED_CURRENT",
            "provenance_role": "PROVENANCE_ROOT",
            "adoption_status": "ADOPTABLE_CURRENT",
            "owner_contract": "botuser",
            "identity": "TABLE:public:sample::",
            "root_identity": "TABLE:public:sample::",
        }],
    }


def _differences():
    return {
        "baseline_version": BASELINE_VERSION,
        "default_for_unlisted_mismatch": "UNEXPECTED",
        "rules": [],
    }


def _row(name="sample", definition="id bigint"):
    return {
        "object_type": "TABLE",
        "schema_name": "public",
        "object_name": name,
        "identity_arguments": "",
        "parent_relation": "",
        "definition": definition,
        "owner_name": "botuser",
        "enabled_state": "",
        "case": "APPLICATION",
        "extname": "",
        "extversion": "",
    }


def _csv(path: Path, rows=None, header=CSV_HEADER):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows or [_row()])
    return path


def _run(tmp_path, raw, *, environment="LOCAL_LIVE", timestamp="2026-01-01T00:00:00Z",
         expected_hash=None, fingerprint_algorithm=FINGERPRINT_ALGORITHM):
    source = tmp_path / "source.py"
    source.write_text("canonicalizer source\n")
    return canonicalize_raw(
        raw_path=raw,
        environment_identity=environment,
        manifest=_manifest(),
        differences=_differences(),
        tracked_paths={"db/sample.sql"},
        output_dir=tmp_path / f"out-{environment}-{timestamp[-2:]}",
        canonicalizer_git_sha="a" * 40,
        source_paths=[source],
        expected_raw_sha256=expected_hash,
        generated_at_utc=timestamp,
        fingerprint_algorithm=fingerprint_algorithm,
    )


def test_deterministic_payload_and_timestamp_exclusion(tmp_path):
    raw = _csv(tmp_path / "raw.csv")
    first = _run(tmp_path, raw, timestamp="2026-01-01T00:00:00Z")
    second = _run(tmp_path, raw, timestamp="2026-01-02T00:00:00Z")
    assert first["semantic_payload"] == second["semantic_payload"]
    assert (
        first["contract"]["deterministic_payload_sha256"]
        == second["contract"]["deterministic_payload_sha256"]
    )
    assert first["contract"]["generated_at_utc"] != second["contract"]["generated_at_utc"]


def test_row_order_does_not_change_semantic_payload(tmp_path):
    rows = [_row(), _row("other", "x text")]
    manifest = _manifest()
    other = dict(manifest["objects"][0])
    other.update({
        "object_name": "other",
        "identity": "TABLE:public:other::",
        "root_identity": "TABLE:public:other::",
        "canonical_definition_sha256": fingerprint("x text"),
    })
    manifest["objects"].append(other)
    source = tmp_path / "source.py"
    source.write_text("source\n")
    reports = []
    for index, ordered in enumerate((rows, list(reversed(rows)))):
        reports.append(canonicalize_raw(
            raw_path=_csv(tmp_path / f"raw-{index}.csv", ordered),
            environment_identity="LOCAL_LIVE",
            manifest=manifest,
            differences=_differences(),
            tracked_paths={"db/sample.sql"},
            output_dir=tmp_path / f"out-{index}",
            canonicalizer_git_sha="a" * 40,
            source_paths=[source],
            generated_at_utc="2026-01-01T00:00:00Z",
        ))
    assert reports[0]["semantic_payload"] == reports[1]["semantic_payload"]


def test_json_key_order_and_sql_whitespace_fingerprints():
    assert fingerprint({"b": 2, "a": 1}) == fingerprint({"a": 1, "b": 2})
    assert fingerprint("SELECT a, b FROM t") == fingerprint(" SELECT a,b  FROM t ")


def test_null_normalization_and_contract_metadata(tmp_path):
    report = _run(tmp_path, _csv(tmp_path / "raw.csv"))
    row = report["semantic_payload"]["inventory"][0]
    assert row["enabled_state"] is None
    contract = report["contract"]
    assert contract["artifact_contract_version"] == ARTIFACT_CONTRACT_VERSION
    assert contract["fingerprint_algorithm"] == FINGERPRINT_ALGORITHM
    assert contract["raw_input_sha256"]
    assert contract["raw_input_header_sha256"]


def test_duplicate_identity_rejected(tmp_path):
    raw = _csv(tmp_path / "raw.csv", [_row(), _row()])
    with pytest.raises(ValueError, match="duplicate catalog identity"):
        _run(tmp_path, raw)


def test_unknown_csv_schema_rejected(tmp_path):
    raw = _csv(tmp_path / "raw.csv", header=CSV_HEADER[:-1])
    with pytest.raises(ValueError, match="unsupported catalog CSV header"):
        _run(tmp_path, raw)


def test_missing_required_csv_field_rejected(tmp_path):
    raw = tmp_path / "raw.csv"
    raw.write_text(",".join(CSV_HEADER) + "\nTABLE,public,sample\n")
    with pytest.raises(ValueError, match="catalog row missing fields"):
        _run(tmp_path, raw)


def test_raw_hash_mismatch_and_unknown_algorithm_rejected(tmp_path):
    raw = _csv(tmp_path / "raw.csv")
    with pytest.raises(ValueError, match="raw input SHA-256 mismatch"):
        _run(tmp_path, raw, expected_hash="0" * 64)
    with pytest.raises(ValueError, match="unsupported fingerprint algorithm"):
        _run(tmp_path, raw, fingerprint_algorithm="MD5")


def test_dangling_source_and_contract_version_rejected(tmp_path):
    raw = _csv(tmp_path / "raw.csv")
    source = tmp_path / "source.py"
    source.write_text("source\n")
    with pytest.raises(ValueError, match="dangling tracked provenance"):
        canonicalize_raw(
            raw_path=raw,
            environment_identity="LOCAL_LIVE",
            manifest=_manifest(),
            differences=_differences(),
            tracked_paths=set(),
            output_dir=tmp_path / "out",
            canonicalizer_git_sha="a" * 40,
            source_paths=[source],
        )
    differences = _differences()
    differences["baseline_version"] = "OTHER"
    with pytest.raises(ValueError, match="baseline_version mismatch"):
        canonicalize_raw(
            raw_path=raw,
            environment_identity="LOCAL_LIVE",
            manifest=_manifest(),
            differences=differences,
            tracked_paths={"db/sample.sql"},
            output_dir=tmp_path / "out2",
            canonicalizer_git_sha="a" * 40,
            source_paths=[source],
        )


def test_four_envelopes_have_identical_contract_schema(tmp_path):
    raw = _csv(tmp_path / "raw.csv")
    reports = [
        _run(tmp_path, raw, environment=environment)
        for environment in ("LOCAL_LIVE", "LOCAL_PAPER", "VPS_LIVE", "VPS_PAPER")
    ]
    stable_fields = {
        "artifact_contract_name", "artifact_contract_version",
        "catalog_schema_version", "normalization_version",
        "provenance_model_version", "expected_differences_contract_version",
        "fingerprint_algorithm", "canonicalizer_git_sha",
        "canonicalizer_source_hashes", "raw_input_header_sha256",
    }
    assert all(set(report["contract"]) == set(reports[0]["contract"]) for report in reports)
    for field in stable_fields:
        assert len({report["contract"][field] if not isinstance(
            report["contract"][field], dict
        ) else json.dumps(report["contract"][field], sort_keys=True)
                    for report in reports}) == 1
    assert all(set(report["semantic_payload"]) == set(reports[0]["semantic_payload"])
               for report in reports)
    assert all(report["semantic_payload"]["coverage"] for report in reports)
    assert all(
        report["semantic_payload"]["counters"]["unclassified"] == 0
        for report in reports
    )


def _semantic_row(
    identity: str,
    *,
    primary: str | None = "APPLICATION_OWNED_TRACKED",
    risk: str = "P2",
    manual: bool = False,
    blocker: bool = False,
    expected: bool = False,
):
    return {
        "identity": identity,
        "management": "APPLICATION",
        "primary_classification": primary,
        "provenance_status": "TRACKED_CURRENT",
        "risk_class": risk,
        "secondary_flags": {
            "manual_decision": manual,
            "canonical_blocker": blocker,
            "expected_difference": expected,
            "risk": risk,
        },
    }


def test_semantic_counters_use_identity_sets_and_independent_flags():
    rows = [
        _semantic_row("a", risk="P0", manual=True, blocker=True, expected=True),
        _semantic_row("b", risk="P1", manual=True),
        _semantic_row("c", risk="P2", manual=True),
        _semantic_row("d", risk="P0"),
    ]
    counters = semantic_counters(rows, [])
    assert counters["manual_decisions"] == 3
    assert counters["manual_decisions"] != (
        counters["pending_p0"] + counters["pending_p1"]
    )
    assert counters["pending_p0"] == 1
    assert counters["pending_p1"] == 1
    assert counters["canonical_common_blockers"] == 1
    assert counters["observed_identities"] == 4
    assert counters["application_owned"] == 4


def test_secondary_flags_do_not_change_primary_ownership_count():
    plain = _semantic_row("a")
    flagged = _semantic_row("a", risk="P0", manual=True, blocker=True, expected=True)
    assert semantic_counters([plain], [])["application_owned"] == 1
    assert semantic_counters([flagged], [])["application_owned"] == 1


def test_unclassified_primary_causes_coverage_failure(tmp_path):
    report = _run(tmp_path, _csv(tmp_path / "raw.csv", [_row("unknown")]))
    assert report["semantic_payload"]["coverage"] is False
    assert report["semantic_payload"]["counters"]["unclassified"] == 1


def test_primary_classification_is_exactly_one_scalar():
    for evidence in HISTORICAL_EVIDENCE.values():
        assert isinstance(evidence["primary_classification"], str)
        assert evidence["primary_classification"] in PRIMARY_CLASSIFICATIONS


def test_old_vps_aggregation_fixture_reproduces_bad_totals():
    old_financial = [{"risk": "P0"}] * 206 + [{"risk": "P1"}] * 148
    assert len(old_financial) == 354
    assert sum(row["risk"] == "P0" for row in old_financial) == 206
    assert sum(row["risk"] == "P1" for row in old_financial) == 148


def test_canonical_vps_aggregation_fixture_uses_decision_identities():
    rows = (
        [_semantic_row(f"p0-{i}", risk="P0", manual=True) for i in range(103)]
        + [_semantic_row(f"p1-{i}", risk="P1", manual=True) for i in range(13)]
        + [_semantic_row(f"p2-{i}", risk="P2", manual=True) for i in range(29)]
        + [_semantic_row(f"not-manual-{i}", risk="P0") for i in range(61)]
    )
    counters = semantic_counters(rows, [])
    assert counters["manual_decisions"] == 145
    assert counters["pending_p0"] == 103
    assert counters["pending_p1"] == 13


def test_six_vps_paper_evidence_identities_are_classified():
    assert len(HISTORICAL_EVIDENCE) == 6
    classifications = [
        evidence["primary_classification"]
        for evidence in HISTORICAL_EVIDENCE.values()
    ]
    assert classifications.count("APPLICATION_OWNED_TRACKED") == 4
    assert classifications.count("HISTORICAL_ORPHAN_PENDING_DECISION") == 2


def test_cli_canonicalize_branch_is_offline(monkeypatch, tmp_path):
    raw = _csv(tmp_path / "raw.csv")
    monkeypatch.setattr(cli, "connect", lambda: pytest.fail("DB connection attempted"))
    monkeypatch.setattr(cli, "candidate_tracked_paths", lambda: {"db/sample.sql"})
    monkeypatch.setattr(cli, "load_checkpoint_manifest", lambda _path: _manifest())
    monkeypatch.setattr(cli, "load_difference_contract", lambda _path: _differences())
    monkeypatch.setattr(
        "sys.argv",
        [
            "waltrade_schema_baseline_v1.py", "canonicalize-raw",
            "--environment-identity", "LOCAL_LIVE",
            "--raw-csv", str(raw),
            "--output-dir", str(tmp_path / "output"),
        ],
    )
    assert cli.main() == 0
    assert (tmp_path / "output/canonical-report.json").exists()


def test_canonicalizer_has_no_database_or_adoption_path():
    source = Path("common/database_baseline_artifact.py").read_text()
    for forbidden in (
        "psycopg", "docker", "psql", "def adopt", "INSERT INTO",
        "CREATE TABLE", "ALTER TABLE",
    ):
        assert forbidden not in source

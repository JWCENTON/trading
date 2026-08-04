from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.compare_effective_parameters_v1 import compare_documents
from scripts.export_effective_parameters_v1 import build_document, canonical_sha256


MANIFEST = json.loads(
    (ROOT / "contracts/parameter_parity_allowed_differences_v1.json").read_text()
)
GENERATED_A = "2026-08-04T12:00:00Z"
GENERATED_B = "2026-08-04T13:00:00Z"
SHA = "a" * 40


def _raw(strategy="RSI", symbol="BTCUSDC", interval="1m", name="RSI_MIN", value="30"):
    return {
        "row_id": 1,
        "strategy": strategy,
        "symbol": symbol,
        "interval": interval,
        "parameter_name": name,
        "effective_value": value,
        "source_updated_at": "2026-08-04T10:00:00.000000Z",
        "history_source": "TEST_CANONICAL",
    }


def _document(rows, environment="PAPER", deployment="local-paper", generated=GENERATED_A):
    return build_document(
        rows,
        deployment_id=deployment,
        environment=environment,
        mode=environment,
        git_sha=SHA,
        runtime_git_sha=SHA,
        oci_revision=SHA,
        generated_at=generated,
    )


def _rehash(document):
    document["canonical_sha256"] = canonical_sha256(document)
    return document


def _manifest_rows(side):
    value_field = f"{side}_value"
    return [
        _raw(
            entry["strategy"], entry["symbol"], entry["interval"],
            entry["parameter_name"], entry[value_field],
        )
        for entry in MANIFEST["entries"]
    ]


def test_01_identical_exports_pass():
    local = _document([_raw()])
    vps = _document([_raw()], deployment="vps-paper", generated=GENERATED_B)
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "PASS"
    assert result["counts"] == {"MATCH": 1}


def test_02_exactly_15_allowed_paper_drifts_pass():
    local = _document(_manifest_rows("LOCAL"))
    vps = _document(_manifest_rows("VPS"), deployment="vps-paper")
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "PASS"
    assert result["counts"] == {"ALLOWED_DIFFERENCE": 15}


def test_03_one_live_value_change_fails():
    entry = MANIFEST["entries"][0]
    local = _document([_raw(entry["strategy"], entry["symbol"], entry["interval"], entry["parameter_name"], entry["LOCAL_value"])], environment="LIVE", deployment="local-live")
    vps = _document([_raw(entry["strategy"], entry["symbol"], entry["interval"], entry["parameter_name"], entry["VPS_value"])], environment="LIVE", deployment="vps-live")
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"UNEXPECTED_DIFFERENCE": 1}


def test_04_unknown_paper_difference_fails():
    local = _document([_raw(value="30")])
    vps = _document([_raw(value="31")], deployment="vps-paper")
    assert compare_documents(local, vps, MANIFEST)["status"] == "FAIL"


def test_05_missing_record_fails():
    result = compare_documents(_document([_raw()]), _document([], deployment="vps-paper"), MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"MISSING_RECORD": 1}


def test_06_extra_record_fails():
    result = compare_documents(_document([]), _document([_raw()], deployment="vps-paper"), MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"EXTRA_RECORD": 1}


def test_07_source_layer_change_fails():
    local = _document([_raw()])
    vps = copy.deepcopy(local)
    vps["deployment_id"] = "vps-paper"
    vps["records"][0]["deployment_id"] = "vps-paper"
    vps["records"][0]["source_layer"] = "ENV"
    _rehash(vps)
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"SOURCE_PROVENANCE_DRIFT": 1}


def test_08_generated_at_does_not_change_hash():
    first = _document([_raw()], generated=GENERATED_A)
    second = _document([_raw()], generated=GENERATED_B)
    assert first["canonical_sha256"] == second["canonical_sha256"]


def test_09_record_order_does_not_change_hash():
    rows = [_raw(symbol="BTCUSDC"), _raw(symbol="ETHUSDC")]
    first = _document(rows)
    second = _document(list(reversed(rows)))
    second["records"].reverse()
    assert canonical_sha256(first) == canonical_sha256(second)


def test_10_secrets_and_host_identity_are_not_exported():
    document = _document([_raw()])
    serialized = json.dumps(document).lower()
    for forbidden in (
        "password", "secret", "api_key", "passphrase", "account_id",
        "hostname", "host_path", "container_id", "database_url",
    ):
        assert forbidden not in serialized


def test_11_manifest_has_exactly_15_unique_entries():
    assert len(MANIFEST["entries"]) == 15
    identities = {
        (e["strategy"], e["symbol"], e["interval"], e["parameter_name"])
        for e in MANIFEST["entries"]
    }
    assert len(identities) == 15
    assert sum(e["strategy"] == "SUPERTREND" for e in MANIFEST["entries"]) == 8
    assert sum(e["strategy"] == "RSI" for e in MANIFEST["entries"]) == 3
    assert sum(e["strategy"] == "BBRANGE" for e in MANIFEST["entries"]) == 4


def test_12_manifest_has_no_wildcards_and_exact_review_contract():
    for entry in MANIFEST["entries"]:
        assert entry["environment_scope"] == "PAPER_ONLY"
        assert entry["review_condition"] == (
            "minimum 30 canonical FT COMPLETE, non-excluded, "
            "candidate-sensitive outcomes"
        )
        assert entry["decision"] == "INSUFFICIENT_EVIDENCE_RETAIN_DRIFT"
        assert entry["blocking"] is False
        assert entry["LIVE_impact"] is False
        for value in entry.values():
            if isinstance(value, str):
                assert "*" not in value and "?" not in value


def test_13_comparator_cli_exit_code_is_gate(tmp_path):
    local = _document([_raw()])
    vps = _document([_raw()], deployment="vps-paper")
    paths = {
        "local": tmp_path / "local.json",
        "vps": tmp_path / "vps.json",
        "manifest": tmp_path / "manifest.json",
    }
    paths["local"].write_text(json.dumps(local))
    paths["vps"].write_text(json.dumps(vps))
    paths["manifest"].write_text(json.dumps(MANIFEST))
    command = [
        sys.executable,
        str(ROOT / "scripts/compare_effective_parameters_v1.py"),
        "--local", str(paths["local"]),
        "--vps", str(paths["vps"]),
        "--allowed-differences", str(paths["manifest"]),
    ]
    assert subprocess.run(command, capture_output=True).returncode == 0
    vps["records"][0]["effective_value"] = "31"
    _rehash(vps)
    paths["vps"].write_text(json.dumps(vps))
    assert subprocess.run(command, capture_output=True).returncode != 0

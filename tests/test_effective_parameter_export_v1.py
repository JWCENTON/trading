from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.compare_effective_parameters_v1 import compare_documents
from scripts.export_effective_parameters_v1 import (
    build_document,
    canonical_sha256,
    discover_runtime_defaults,
    lineage_sha256,
)


MANIFEST = json.loads(
    (ROOT / "contracts/parameter_parity_allowed_differences_v1.json").read_text()
)
GENERATED_A = "2026-08-04T12:00:00Z"
GENERATED_B = "2026-08-04T13:00:00Z"
SHA = "a" * 40


def _raw(
    strategy="RSI", symbol="BTCUSDC", interval="1m", name="RSI_MIN",
    value="30", history_source="MANUAL",
    updated_at="2026-08-04T10:00:00.000000Z",
):
    return {
        "row_id": 1,
        "strategy": strategy,
        "symbol": symbol,
        "interval": interval,
        "parameter_name": name,
        "effective_value": value,
        "source_updated_at": updated_at,
        "history_source": history_source,
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
    document["effective_canonical_sha256"] = canonical_sha256(document)
    document["lineage_sha256"] = lineage_sha256(document)
    return document


def _manifest_rows(side, history_source="MANUAL"):
    value_field = f"{side}_value"
    return [
        _raw(
            entry["strategy"], entry["symbol"], entry["interval"],
            entry["parameter_name"], entry[value_field], history_source,
        )
        for entry in MANIFEST["entries"]
    ]


def test_01_same_value_manual_vs_repair_is_informational():
    local = _document([_raw(history_source="MANUAL")])
    vps = _document(
        [_raw(history_source="PARAMETER_PARITY_REPAIR_V1")],
        deployment="vps-paper",
    )
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "PASS"
    assert result["counts"] == {"LINEAGE_DIFFERENCE_INFORMATIONAL": 1}


def test_02_current_row_vs_history_row_is_informational():
    local = _document([_raw(history_source=None)])
    vps = _document([_raw(history_source="MANUAL")], deployment="vps-paper")
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "PASS"
    assert result["counts"] == {"LINEAGE_DIFFERENCE_INFORMATIONAL": 1}


def test_03_different_source_updated_at_is_informational():
    local = _document([_raw(updated_at="2026-08-01T00:00:00Z")])
    vps = _document(
        [_raw(updated_at="2026-08-02T00:00:00Z")], deployment="vps-paper"
    )
    assert compare_documents(local, vps, MANIFEST)["counts"] == {
        "LINEAGE_DIFFERENCE_INFORMATIONAL": 1
    }


def test_04_live_effective_value_drift_fails():
    local = _document([_raw(value="30")], environment="LIVE", deployment="local-live")
    vps = _document([_raw(value="31")], environment="LIVE", deployment="vps-live")
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"EFFECTIVE_VALUE_DRIFT": 1}


def test_05_source_layer_drift_fails():
    local = _document([_raw()])
    vps = copy.deepcopy(local)
    vps["records"][0]["source_layer"] = "CODE_DEFAULT"
    _rehash(vps)
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"SOURCE_LAYER_DRIFT": 1}


def test_06_runtime_consumer_drift_fails():
    local = _document([_raw()])
    vps = copy.deepcopy(local)
    vps["records"][0]["runtime_child_identity"] = "RSI:BTCUSDC:5m"
    _rehash(vps)
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"] == {"RUNTIME_CONSUMER_DRIFT": 1}


def test_07_allowed_paper_value_drift_ignores_lineage_difference():
    local = _document(_manifest_rows("LOCAL", "MANUAL"))
    vps = _document(
        _manifest_rows("VPS", "PARAMETER_PARITY_REPAIR_V1"),
        deployment="vps-paper",
    )
    result = compare_documents(local, vps, MANIFEST)
    assert result["status"] == "PASS"
    assert result["counts"] == {"ALLOWED_VALUE_DIFFERENCE": 15}
    assert all(finding["lineage_difference"] for finding in result["findings"])


def test_08_min_notional_code_default_consumed_matches_both_sides():
    slot = [_raw(strategy="SUPERTREND", name="MIN_ATR_PCT", value="0.08")]
    discovered = discover_runtime_defaults(slot)
    default = next(
        row for row in discovered if row["parameter_name"] == "MIN_NOTIONAL_BUFFER_PCT"
    )
    assert default["effective_value"] == "0.05"
    assert default["source_layer"] == "CODE_DEFAULT"
    assert default["consumed"] is True
    local = _document(discovered)
    vps = _document(discovered, deployment="vps-paper")
    assert compare_documents(local, vps, MANIFEST)["status"] == "PASS"


def test_09_supertrend_order_notional_non_consumed_is_not_missing():
    row = _raw(
        strategy="SUPERTREND", name="ORDER_NOTIONAL_USDC", value="6"
    )
    local = _document([row])
    assert local["records"][0]["consumed"] is False
    result = compare_documents(local, _document([], deployment="vps-paper"), MANIFEST)
    assert result["status"] == "PASS"
    assert "MISSING_CONSUMED_PARAMETER" not in result["counts"]


def test_10_unknown_missing_consumed_parameter_fails():
    result = compare_documents(
        _document([_raw()]), _document([], deployment="vps-paper"), MANIFEST
    )
    assert result["status"] == "FAIL"
    assert result["counts"] == {"MISSING_CONSUMED_PARAMETER": 1}


def test_11_lineage_hash_can_differ_with_identical_effective_hash():
    local = _document([_raw(history_source="MANUAL")])
    vps = _document([_raw(history_source=None)], deployment="vps-paper")
    assert local["effective_canonical_sha256"] == vps["effective_canonical_sha256"]
    assert local["lineage_sha256"] != vps["lineage_sha256"]


def test_12_live_effective_hash_parity_survives_lineage_difference():
    local = _document(
        [_raw(history_source="MANUAL")], environment="LIVE", deployment="local-live"
    )
    vps = _document(
        [_raw(history_source="PARAMETER_PARITY_REPAIR_V1")],
        environment="LIVE", deployment="vps-live",
    )
    assert local["effective_canonical_sha256"] == vps["effective_canonical_sha256"]
    assert compare_documents(local, vps, MANIFEST)["status"] == "PASS"


def test_13_source_priority_drift_and_extra_consumed_parameter_fail():
    local = _document([_raw()])
    vps = copy.deepcopy(local)
    vps["records"][0]["source_priority"] = 99
    _rehash(vps)
    assert compare_documents(local, vps, MANIFEST)["counts"] == {
        "SOURCE_PRIORITY_DRIFT": 1
    }
    assert compare_documents(_document([]), local, MANIFEST)["counts"] == {
        "EXTRA_CONSUMED_PARAMETER": 1
    }


def test_14_generated_at_and_order_do_not_change_effective_hash():
    rows = [_raw(symbol="BTCUSDC"), _raw(symbol="ETHUSDC")]
    first = _document(rows, generated=GENERATED_A)
    second = _document(list(reversed(rows)), generated=GENERATED_B)
    second["records"].reverse()
    assert canonical_sha256(first) == canonical_sha256(second)


def test_15_manifest_and_secret_contracts_remain_closed():
    assert len(MANIFEST["entries"]) == 15
    identities = {
        (e["strategy"], e["symbol"], e["interval"], e["parameter_name"])
        for e in MANIFEST["entries"]
    }
    assert len(identities) == 15
    for entry in MANIFEST["entries"]:
        assert entry["environment_scope"] == "PAPER_ONLY"
        assert entry["blocking"] is False
        assert entry["LIVE_impact"] is False
        assert all("*" not in str(value) and "?" not in str(value) for value in entry.values())
    serialized = json.dumps(_document([_raw()])).lower()
    for forbidden in (
        "password", "secret", "api_key", "passphrase", "account_id",
        "hostname", "host_path", "container_id", "database_url",
    ):
        assert forbidden not in serialized


def test_16_comparator_cli_exit_code_is_gate(tmp_path):
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
        sys.executable, str(ROOT / "scripts/compare_effective_parameters_v1.py"),
        "--local", str(paths["local"]), "--vps", str(paths["vps"]),
        "--allowed-differences", str(paths["manifest"]),
    ]
    assert subprocess.run(command, capture_output=True).returncode == 0
    vps["records"][0]["effective_value"] = "31"
    _rehash(vps)
    paths["vps"].write_text(json.dumps(vps))
    assert subprocess.run(command, capture_output=True).returncode != 0


def test_17_value_type_and_environment_scope_mismatch_fail():
    local = _document([_raw()])
    vps = copy.deepcopy(local)
    vps["records"][0]["value_type"] = "INTEGER"
    _rehash(vps)
    assert compare_documents(local, vps, MANIFEST)["counts"] == {
        "UNEXPECTED_DIFFERENCE": 1
    }

    live = _document([_raw()], environment="LIVE", deployment="vps-live")
    result = compare_documents(local, live, MANIFEST)
    assert result["status"] == "FAIL"
    assert result["counts"]["UNEXPECTED_DIFFERENCE"] == 1

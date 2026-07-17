import importlib.util
import json
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/causal_learning_telemetry_fingerprint_v1.py"
SPEC = importlib.util.spec_from_file_location("causal_fingerprint", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


def record(kind="view", name="example", subobject=None, definition="SELECT 1"):
    return {
        "record_type": kind,
        "schema_name": "public",
        "object_name": name,
        "subobject_name": subobject,
        "canonical_definition": definition,
    }


def manifest(path, records):
    path.write_text(json.dumps({"records": records}), encoding="utf-8")


def test_order_and_whitespace_are_canonicalized():
    left = [record(name="b", definition=" SELECT\n  2 "), record(name="a")]
    right = [record(name="a"), record(name="b", definition="SELECT 2")]
    assert MODULE.canonicalize(left)[1] == MODULE.canonicalize(right)[1]


def test_null_is_not_empty_string():
    null_record = record(subobject=None)
    empty_record = record(subobject="")
    assert MODULE.canonicalize([null_record])[1] != MODULE.canonicalize([empty_record])[1]


def test_diff_reports_missing_extra_and_changed(tmp_path, capsys):
    left = [record(name="missing"), record(name="changed", definition="CHECK (x > 0)")]
    right = [record(name="extra"), record(name="changed", definition="CHECK (x >= 0)")]
    assert MODULE.diff(left, right)
    output = capsys.readouterr().out
    assert "missing:" in output
    assert "extra:" in output
    assert "changed:" in output
    assert "manifest_diff=different" in output


def test_diff_detects_changed_function_and_view(capsys):
    left = [record("function", "f", "", "BEGIN RETURN 1; END"), record("view", "v", definition="SELECT 1")]
    right = deepcopy(left)
    right[0]["canonical_definition"] = "BEGIN RETURN 2; END"
    right[1]["canonical_definition"] = "SELECT 2"
    assert MODULE.diff(left, right)
    assert capsys.readouterr().out.count("changed:") == 2


def test_empty_diff(tmp_path, capsys):
    left = tmp_path / "left.json"
    right = tmp_path / "right.json"
    records = [record()]
    manifest(left, records)
    manifest(right, records)
    assert MODULE.main(["--diff", str(left), str(right)]) == 0
    assert "manifest_diff=empty" in capsys.readouterr().out


def test_expected_manifest_count_is_172():
    assert sum(MODULE.EXPECTED_COUNTS.values()) == 172
    assert MODULE.EXPECTED_COUNTS == {
        "table": 4, "column": 117, "constraint": 30, "index": 2,
        "trigger": 5, "function": 7, "view": 4, "flag": 3,
    }


def test_main_rejects_invalid_record_count(monkeypatch, capsys):
    records = [record("table", f"table_{number}") for number in range(171)]
    monkeypatch.setattr(MODULE, "read_database", lambda *_: records)
    assert MODULE.main([]) == 2
    assert "manifest count contract violated" in capsys.readouterr().err


def test_sql_uses_explicit_names_not_namespace_patterns():
    sql = MODULE.SQL.read_text(encoding="utf-8")
    assert "learning_%" not in sql
    assert "decision_%" not in sql
    assert "v_learning_%" not in sql
    columns_section = sql.split("manifest_columns(table_name, column_name) AS (VALUES", 1)[1].split(
        "manifest_constraints(table_name, constraint_name) AS (VALUES", 1
    )[0]
    assert columns_section.count("('learning_recommendation_snapshots_v1','") == 23
    assert "pg_get_constraintdef" in sql
    assert "pg_get_functiondef" in sql
    assert "pg_get_viewdef" in sql

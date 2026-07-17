#!/usr/bin/env python3
"""Canonical Causal Learning Telemetry V1 schema fingerprint and diff tool."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

VERSION = "causal_learning_telemetry_v1"
EXPECTED_COUNTS = {"table": 4, "column": 117, "constraint": 30, "index": 2,
                   "trigger": 5, "function": 7, "view": 4, "flag": 3}
ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "tests/postgres/causal_learning_telemetry_fingerprint_v1.sql"
def normalize(value: str | None) -> str | None:
    """Normalize catalog text to the V1 whitespace contract."""
    if value is None:
        return None
    return re.sub(r"\s+", " ", value).strip()


def canonical_record(record: dict[str, str | None]) -> dict[str, str | None]:
    return {key: normalize(record.get(key)) for key in
            ("record_type", "schema_name", "object_name", "subobject_name", "canonical_definition")}


def key(record: dict[str, str | None]) -> tuple[str | None, ...]:
    return tuple(record[name] for name in ("record_type", "schema_name", "object_name", "subobject_name"))


def canonicalize(records: list[dict[str, str | None]]) -> tuple[list[dict[str, str | None]], bytes]:
    normalized = [canonical_record(record) for record in records]
    normalized.sort(key=lambda record: tuple("" if item is None else item for item in key(record)))
    payload = b"".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        for record in normalized
    )
    return normalized, payload


def read_database(psql_args: list[str], docker_container: str | None = None) -> list[dict[str, str | None]]:
    psql = ["psql", "-X", "--no-psqlrc", "-q", "-v", "ON_ERROR_STOP=1", "-A", "-t", *psql_args]
    if docker_container:
        command = ["docker", "exec", "-i", docker_container, *psql]
        result = subprocess.run(command, check=True, input=SQL.read_text(encoding="utf-8"),
                                stdout=subprocess.PIPE, text=True, encoding="utf-8")
    else:
        command = [*psql, "-f", str(SQL)]
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True, encoding="utf-8")
    records = []
    for line in result.stdout.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 5:
            raise ValueError(f"invalid SQL protocol row with {len(fields)} fields")
        values = [None if field == "-" else bytes.fromhex(field).decode("utf-8") for field in fields]
        records.append(dict(zip(("record_type", "schema_name", "object_name", "subobject_name", "canonical_definition"), values)))
    return records


def load(path: Path) -> list[dict[str, str | None]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    return document["records"] if isinstance(document, dict) else document


def write(path: Path, records: list[dict[str, str | None]]) -> None:
    normalized, payload = canonicalize(records)
    document = {"manifest_version": VERSION, "fingerprint": hashlib.sha256(payload).hexdigest(), "records": normalized}
    path.write_text(json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n")


def diff(left: list[dict[str, str | None]], right: list[dict[str, str | None]]) -> bool:
    left_map = {key(r): canonical_record(r) for r in left}
    right_map = {key(r): canonical_record(r) for r in right}
    missing, extra = sorted(left_map.keys() - right_map.keys()), sorted(right_map.keys() - left_map.keys())
    changed = sorted(k for k in left_map.keys() & right_map.keys() if left_map[k] != right_map[k])
    for label, keys in (("missing", missing), ("extra", extra), ("changed", changed)):
        for item in keys:
            print(f"{label}: " + " | ".join("<NULL>" if part is None else part for part in item))
            if label == "changed":
                print(f"  left={left_map[item]['canonical_definition']!r}")
                print(f"  right={right_map[item]['canonical_definition']!r}")
    print(f"manifest_diff={'different' if missing or extra or changed else 'empty'}")
    return bool(missing or extra or changed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--psql-arg", action="append", default=[], help="argument passed verbatim to psql; repeatable")
    parser.add_argument("--docker-container", help="run psql inside this local PostgreSQL container")
    parser.add_argument("--output", type=Path, help="write canonical JSON manifest")
    parser.add_argument("--diff", nargs=2, type=Path, metavar=("LEFT", "RIGHT"), help="diff two saved manifests")
    args = parser.parse_args(argv)
    if args.diff:
        return int(diff(load(args.diff[0]), load(args.diff[1])))
    records = read_database(args.psql_arg, args.docker_container)
    normalized, payload = canonicalize(records)
    counts = Counter(record["record_type"] for record in normalized)
    print(f"manifest_version={VERSION}")
    print(f"manifest_record_count={len(normalized)}")
    print(f"fingerprint={hashlib.sha256(payload).hexdigest()}")
    labels = {"index": "indexes"}
    for category, expected in EXPECTED_COUNTS.items():
        print(f"{labels.get(category, category + 's')}={counts[category]}")
    if args.output:
        write(args.output, normalized)
    valid = len(normalized) == sum(EXPECTED_COUNTS.values()) and all(counts[k] == v for k, v in EXPECTED_COUNTS.items())
    if not valid:
        print("error=manifest count contract violated", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

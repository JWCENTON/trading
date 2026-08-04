#!/usr/bin/env python3
"""Canonical PostgreSQL fingerprint for the Learning Quarantine V1 contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    ROOT / "contracts" / "learning_quarantine_contract_v1_manifest.json"
)
ALLOWED_OBJECT_TYPES = {
    "table", "column", "constraint", "index", "trigger", "function", "view",
}
IDENTITY_FIELDS = ("object_type", "schema", "object_name", "subidentity")
SQL_FIELDS = {"default_sql", "definition_sql"}


def canonical_json(value: Any) -> str:
    """Compact UTF-8/LF-independent JSON representation with explicit nulls."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _consume_quoted(value: str, start: int, quote: str) -> tuple[str, int]:
    result = [quote]
    index = start + 1
    while index < len(value):
        char = value[index]
        result.append(char)
        index += 1
        if char != quote:
            continue
        if index < len(value) and value[index] == quote:
            result.append(value[index])
            index += 1
            continue
        break
    return "".join(result), index


def normalize_sql(value: str | None) -> str | None:
    """Normalize catalog SQL without changing literals or operator order.

    The V1 contract converts CRLF/CR to LF, removes SQL comments, collapses
    whitespace outside quoted literals/identifiers, strips trailing whitespace,
    and canonicalizes dollar-quote tags. Dollar-quoted routine bodies are
    normalized recursively while quoted values inside them remain byte-exact.
    """
    if value is None:
        return None
    source = value.replace("\r\n", "\n").replace("\r", "\n")
    output: list[str] = []
    whitespace_pending = False
    index = 0
    while index < len(source):
        if source.startswith("--", index):
            end = source.find("\n", index + 2)
            index = len(source) if end < 0 else end + 1
            whitespace_pending = True
            continue
        if source.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            whitespace_pending = True
            continue
        char = source[index]
        if char.isspace():
            whitespace_pending = True
            index += 1
            continue
        if whitespace_pending and output and output[-1] != " ":
            output.append(" ")
        whitespace_pending = False
        if char in {"'", '"'}:
            quoted, index = _consume_quoted(source, index, char)
            output.append(quoted)
            continue
        if char == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$", source[index:])
            if match:
                delimiter = match.group(0)
                body_start = index + len(delimiter)
                body_end = source.find(delimiter, body_start)
                if body_end >= 0:
                    body = normalize_sql(source[body_start:body_end]) or ""
                    output.extend(("$body$", body, "$body$"))
                    index = body_end + len(delimiter)
                    continue
        output.append(char)
        index += 1
    return "".join(output).strip()


def _normalize_definition(value: Any, field: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            key: _normalize_definition(item, key)
            for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        return [_normalize_definition(item, field) for item in value]
    if isinstance(value, str) and field in SQL_FIELDS:
        return normalize_sql(value)
    return value


def identity(record: dict[str, Any]) -> tuple[str, str, str, str | None]:
    return tuple(record[field] for field in IDENTITY_FIELDS)  # type: ignore[return-value]


def identity_text(value: tuple[str, str, str, str | None]) -> str:
    return " | ".join("<NULL>" if item is None else item for item in value)


def validate_manifest(document: dict[str, Any]) -> None:
    for field in (
        "contract_version", "manifest_version", "normalization_version",
        "source_migrations", "objects",
    ):
        if field not in document:
            raise ValueError(f"manifest missing field: {field}")
    if not document["objects"]:
        raise ValueError("manifest object list is empty")
    seen: set[tuple[str, str, str, str | None]] = set()
    for entry in document["objects"]:
        if set(entry) != set(IDENTITY_FIELDS):
            raise ValueError(f"invalid manifest object fields: {entry}")
        if entry["object_type"] not in ALLOWED_OBJECT_TYPES:
            raise ValueError(f"invalid object type: {entry['object_type']}")
        for field in ("schema", "object_name"):
            value = entry[field]
            if not isinstance(value, str) or not value:
                raise ValueError(f"invalid {field}: {entry}")
            if any(token in value for token in ("*", "%", "?", "[", "]")):
                raise ValueError(f"wildcard forbidden in manifest: {entry}")
        subidentity = entry["subidentity"]
        if subidentity is not None and (
            not isinstance(subidentity, str)
            or any(token in subidentity for token in ("*", "%", "?", "[", "]"))
        ):
            raise ValueError(f"invalid subidentity: {entry}")
        key = identity(entry)
        if key in seen:
            raise ValueError(f"duplicate manifest object: {identity_text(key)}")
        seen.add(key)


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    validate_manifest(document)
    return document


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _base_record(entry: dict[str, Any]) -> str:
    subidentity = (
        "NULL" if entry["subidentity"] is None else _literal(entry["subidentity"])
    )
    return (
        "'object_type'," + _literal(entry["object_type"])
        + ",'schema'," + _literal(entry["schema"])
        + ",'object_name'," + _literal(entry["object_name"])
        + ",'subidentity'," + subidentity
    )


def catalog_query(entry: dict[str, Any]) -> str:
    """Return one catalog-only SELECT yielding a canonicalizable JSON record."""
    typ = entry["object_type"]
    schema = _literal(entry["schema"])
    name = _literal(entry["object_name"])
    sub = entry["subidentity"]
    base = _base_record(entry)
    if typ == "table":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'relkind',c.relkind,'persistence',c.relpersistence,
          'is_partition',c.relispartition,
          'reloptions',COALESCE((SELECT jsonb_agg(option ORDER BY option)
            FROM unnest(COALESCE(c.reloptions,ARRAY[]::text[])) option),'[]'::jsonb)
        ))::text
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname={schema} AND c.relname={name} AND c.relkind IN ('r','p')
        """
    if typ == "column":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'ordinal',a.attnum,'type',format_type(a.atttypid,a.atttypmod),
          'not_null',a.attnotnull,
          'default_sql',pg_get_expr(d.adbin,d.adrelid,true),
          'identity',NULLIF(a.attidentity,''),'generated',NULLIF(a.attgenerated,''),
          'collation_schema',cn.nspname,'collation_name',coll.collname
        ))::text
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        LEFT JOIN pg_collation coll ON coll.oid=NULLIF(a.attcollation,0)
        LEFT JOIN pg_namespace cn ON cn.oid=coll.collnamespace
        WHERE n.nspname={schema} AND c.relname={name}
          AND a.attname={_literal(str(sub))} AND a.attnum>0 AND NOT a.attisdropped
        """
    if typ == "constraint":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'constraint_type',con.contype,
          'definition_sql',pg_get_constraintdef(con.oid,true),
          'deferrable',con.condeferrable,'initially_deferred',con.condeferred,
          'validated',con.convalidated,'no_inherit',con.connoinherit
        ))::text
        FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname={schema} AND c.relname={_literal(str(sub))}
          AND con.conname={name}
        """
    if typ == "index":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'definition_sql',pg_get_indexdef(i.indexrelid,0,true),
          'unique',i.indisunique,'primary',i.indisprimary,
          'exclusion',i.indisexclusion,'immediate',i.indimmediate,
          'valid',i.indisvalid,'ready',i.indisready
        ))::text
        FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_class tbl ON tbl.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=tbl.relnamespace
        WHERE n.nspname={schema} AND tbl.relname={_literal(str(sub))}
          AND idx.relname={name}
        """
    if typ == "trigger":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'definition_sql',pg_get_triggerdef(t.oid,true),'enabled',t.tgenabled
        ))::text
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname={schema} AND c.relname={_literal(str(sub))}
          AND t.tgname={name} AND NOT t.tgisinternal
        """
    if typ == "function":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'definition_sql',pg_get_functiondef(p.oid),
          'identity_arguments',pg_get_function_identity_arguments(p.oid),
          'result',pg_get_function_result(p.oid),'language',lang.lanname,
          'kind',p.prokind,'volatility',p.provolatile,'strict',p.proisstrict,
          'security_definer',p.prosecdef,'leakproof',p.proleakproof,
          'parallel',p.proparallel,
          'configuration',COALESCE((SELECT jsonb_agg(setting ORDER BY setting)
            FROM unnest(COALESCE(p.proconfig,ARRAY[]::text[])) setting),'[]'::jsonb)
        ))::text
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        JOIN pg_language lang ON lang.oid=p.prolang
        WHERE n.nspname={schema} AND p.proname={name}
          AND oidvectortypes(p.proargtypes)={_literal(str(sub))}
        """
    if typ == "view":
        return f"""
        SELECT jsonb_build_object({base},'definition',jsonb_build_object(
          'relkind',c.relkind,'definition_sql',pg_get_viewdef(c.oid,true),
          'reloptions',COALESCE((SELECT jsonb_agg(option ORDER BY option)
            FROM unnest(COALESCE(c.reloptions,ARRAY[]::text[])) option),'[]'::jsonb)
        ))::text
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname={schema} AND c.relname={name} AND c.relkind IN ('v','m')
        """
    raise ValueError(f"unsupported object type: {typ}")


def _parse_record(value: str | dict[str, Any]) -> dict[str, Any]:
    record = json.loads(value) if isinstance(value, str) else value
    record["definition"] = _normalize_definition(record["definition"])
    return record


def collect_with_cursor(
    cursor: Any, manifest: dict[str, Any]
) -> tuple[int, list[dict[str, Any]], list[tuple[str, str, str, str | None]]]:
    """Collect the manifest inventory through an existing DB-API cursor."""
    validate_manifest(manifest)
    cursor.execute("SHOW search_path")
    prior_search_path = str(cursor.fetchone()[0])
    cursor.execute("SET LOCAL search_path=pg_catalog")
    cursor.execute("SELECT current_setting('server_version_num')::int / 10000")
    major = int(cursor.fetchone()[0])
    records: list[dict[str, Any]] = []
    missing: list[tuple[str, str, str, str | None]] = []
    for entry in manifest["objects"]:
        cursor.execute(catalog_query(entry))
        row = cursor.fetchone()
        if row is None:
            missing.append(identity(entry))
        else:
            records.append(_parse_record(row[0]))
    cursor.execute(
        "SELECT set_config('search_path',%s,true)", (prior_search_path,)
    )
    cursor.fetchone()
    return major, records, missing


def psql_script(manifest: dict[str, Any]) -> str:
    queries = [
        "BEGIN READ ONLY",
        "SET LOCAL search_path=pg_catalog",
        "SELECT jsonb_build_object('metadata','postgresql_major','value',"
        "current_setting('server_version_num')::int / 10000)::text",
    ]
    queries.extend(catalog_query(entry) for entry in manifest["objects"])
    queries.append("ROLLBACK")
    return ";\n".join(query.strip() for query in queries) + ";\n"


def collect_with_psql(
    manifest: dict[str, Any], psql_args: list[str], docker_container: str | None
) -> tuple[int, list[dict[str, Any]], list[tuple[str, str, str, str | None]]]:
    command = ["psql", "-X", "--no-psqlrc", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", *psql_args]
    if docker_container:
        command = ["docker", "exec", "-i", docker_container, *command]
    result = subprocess.run(
        command, check=True, input=psql_script(manifest), text=True,
        encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    major: int | None = None
    records: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if value.get("metadata") == "postgresql_major":
            major = int(value["value"])
        else:
            records.append(_parse_record(value))
    if major is None:
        raise RuntimeError("PostgreSQL major version missing from catalog protocol")
    actual = {identity(record) for record in records}
    missing = [
        identity(entry) for entry in manifest["objects"]
        if identity(entry) not in actual
    ]
    return major, records, missing


def build_artifact(
    manifest: dict[str, Any], postgresql_major: int,
    records: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    normalized = []
    for source in records:
        record = {
            field: source.get(field) for field in IDENTITY_FIELDS
        }
        record["definition"] = _normalize_definition(source["definition"])
        normalized.append(record)
    normalized.sort(key=lambda record: tuple(
        "" if record[field] is None else record[field] for field in IDENTITY_FIELDS
    ))
    canonical_objects = []
    artifact_objects = []
    for record in normalized:
        object_hash = sha256(canonical_json(record))
        canonical_objects.append(record)
        artifact_objects.append({**record, "sha256": object_hash})
    canonical_document = {
        "contract_version": manifest["contract_version"],
        "manifest_version": manifest["manifest_version"],
        "normalization_version": manifest["normalization_version"],
        "postgresql_major": postgresql_major,
        "objects": canonical_objects,
    }
    return {
        "contract_version": manifest["contract_version"],
        "manifest_version": manifest["manifest_version"],
        "normalization_version": manifest["normalization_version"],
        "postgresql_major": postgresql_major,
        "object_count": len(artifact_objects),
        "canonical_sha256": sha256(canonical_json(canonical_document)),
        "objects": artifact_objects,
    }


def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(artifact) + "\n", encoding="utf-8", newline="\n")


def load_artifact(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def diff_artifacts(
    left: dict[str, Any], right: dict[str, Any]
) -> tuple[bool, list[str]]:
    left_map = {identity(record): record for record in left["objects"]}
    right_map = {identity(record): record for record in right["objects"]}
    lines: list[str] = []
    for key in sorted(left_map.keys() - right_map.keys(), key=str):
        lines.append(f"missing object: {identity_text(key)}")
    for key in sorted(right_map.keys() - left_map.keys(), key=str):
        lines.append(f"extra object: {identity_text(key)}")
    for key in sorted(left_map.keys() & right_map.keys(), key=str):
        if left_map[key]["sha256"] == right_map[key]["sha256"]:
            continue
        typ = key[0]
        lines.append(f"changed {typ}: {identity_text(key)}")
        lines.append(f"  left_sha256={left_map[key]['sha256']}")
        lines.append(f"  right_sha256={right_map[key]['sha256']}")
        left_definition = left_map[key]["definition"]
        right_definition = right_map[key]["definition"]
        for field in sorted(set(left_definition) | set(right_definition)):
            if left_definition.get(field) != right_definition.get(field):
                lines.append(
                    f"  changed definition.{field}: "
                    f"left={left_definition.get(field)!r} "
                    f"right={right_definition.get(field)!r}"
                )
    for field in (
        "contract_version", "manifest_version", "normalization_version",
        "postgresql_major",
    ):
        if left.get(field) != right.get(field):
            lines.append(
                f"changed metadata.{field}: left={left.get(field)!r} "
                f"right={right.get(field)!r}"
            )
    different = bool(lines)
    lines.append(f"contract_diff={'different' if different else 'empty'}")
    return different, lines


def _print_artifact(artifact: dict[str, Any]) -> None:
    for field in (
        "contract_version", "manifest_version", "postgresql_major",
        "object_count", "canonical_sha256",
    ):
        print(f"{field}={artifact[field]}")
    for record in artifact["objects"]:
        print(f"object_sha256={record['sha256']} | {identity_text(identity(record))}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--docker-container")
    parser.add_argument(
        "--psql-arg", action="append", default=[],
        help="argument passed verbatim to psql; repeatable",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--diff", nargs=2, type=Path, metavar=("LEFT", "RIGHT"))
    args = parser.parse_args(argv)
    if args.diff:
        different, lines = diff_artifacts(
            load_artifact(args.diff[0]), load_artifact(args.diff[1])
        )
        print("\n".join(lines))
        return int(different)
    manifest = load_manifest(args.manifest)
    major, records, missing = collect_with_psql(
        manifest, args.psql_arg, args.docker_container
    )
    artifact = build_artifact(manifest, major, records)
    _print_artifact(artifact)
    for key in missing:
        print(f"missing object: {identity_text(key)}", file=sys.stderr)
    if args.output:
        write_artifact(args.output, artifact)
        print(f"canonical_json_artifact={args.output}")
    if missing:
        print(
            f"error=manifest object contract violated; missing={len(missing)}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

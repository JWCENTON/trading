#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from common.database_baseline_artifact import canonicalize_raw
from common.schema_provenance import (
    BASELINE_VERSION,
    compare_inventory,
    classify_root_risk,
    difference_rule_map,
    fingerprint,
    hierarchical_entry,
    hierarchical_readiness,
    identity_string,
    load_manifest,
    object_key,
    validate_difference_contract,
    validate_tracked_provenance,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "db" / "schema_baseline" / "waltrade_database_baseline_v1.json"
DEFAULT_DIFFERENCES = (
    ROOT / "db" / "schema_baseline" / "expected_environment_differences_v1.json"
)
APPROVED_SCHEMAS = ("public",)


def candidate_tracked_paths() -> set[str]:
    """Return paths in the candidate Git index, independent of working-tree files."""
    return set(subprocess.check_output(
        ["git", "ls-files", "--cached"], cwd=ROOT, text=True
    ).splitlines())


def load_checkpoint_manifest(path: str | Path) -> dict:
    manifest = load_manifest(path)
    validate_tracked_provenance(manifest, candidate_tracked_paths())
    return manifest


CATALOG_SQL = r"""
WITH selected_rel AS (
  SELECT c.oid, n.nspname AS schema_name, c.relname, c.relkind,
         pg_get_userbyid(c.relowner) AS owner_name,
         e.extname, e.extversion
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_class'::regclass
    AND dep.objid=c.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE n.nspname = ANY(%(schemas)s)
    AND c.relkind IN ('r','p','v','m','S')
), relation_objects AS (
  SELECT CASE relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED_VIEW'
                     WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END AS object_type,
         schema_name, relname AS object_name, '' AS identity_arguments,
         '' AS parent_relation,
         CASE WHEN relkind IN ('v','m') THEN pg_get_viewdef(oid, true)
              WHEN relkind='S' THEN jsonb_build_object(
                'data_type',format_type(s.seqtypid,NULL),'start',s.seqstart,
                'increment',s.seqincrement,'min',s.seqmin,'max',s.seqmax,
                'cache',s.seqcache,'cycle',s.seqcycle)::text
              ELSE (SELECT jsonb_agg(jsonb_build_object(
                       'name',a.attname,'type',format_type(a.atttypid,a.atttypmod),
                       'not_null',a.attnotnull,
                       'default',pg_get_expr(d.adbin,d.adrelid),
                       'identity',a.attidentity,'generated',a.attgenerated
                     ) ORDER BY a.attnum)::text
                    FROM pg_attribute a
                    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                   WHERE a.attrelid=selected_rel.oid AND a.attnum>0 AND NOT a.attisdropped)
         END AS definition,
         owner_name, NULL::text AS enabled_state,
         CASE WHEN extname IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         extname, extversion
  FROM selected_rel LEFT JOIN pg_sequence s ON s.seqrelid=selected_rel.oid
), function_objects AS (
  SELECT CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END,
         n.nspname, p.proname,
         pg_get_function_identity_arguments(p.oid), '',
         pg_get_functiondef(p.oid), pg_get_userbyid(p.proowner), NULL::text,
         CASE WHEN e.oid IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         e.extname,e.extversion
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_proc'::regclass
    AND dep.objid=p.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE n.nspname = ANY(%(schemas)s) AND p.prokind IN ('f','p')
), trigger_objects AS (
  SELECT 'TRIGGER', n.nspname, t.tgname, '', c.relname,
         pg_get_triggerdef(t.oid, true), pg_get_userbyid(c.relowner),
         CASE t.tgenabled WHEN 'O' THEN 'ENABLED' WHEN 'D' THEN 'DISABLED'
              WHEN 'R' THEN 'REPLICA' WHEN 'A' THEN 'ALWAYS' END,
         CASE WHEN e.oid IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         e.extname,e.extversion
  FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_trigger'::regclass
    AND dep.objid=t.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE NOT t.tgisinternal AND n.nspname = ANY(%(schemas)s)
), constraint_objects AS (
  SELECT 'CONSTRAINT', n.nspname, con.conname, '', c.relname,
         pg_get_constraintdef(con.oid, true), pg_get_userbyid(c.relowner), NULL::text,
         CASE WHEN e.oid IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         e.extname,e.extversion
  FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_constraint'::regclass
    AND dep.objid=con.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE n.nspname = ANY(%(schemas)s)
), index_objects AS (
  SELECT 'INDEX', n.nspname, i.relname, '', c.relname,
         pg_get_indexdef(i.oid), pg_get_userbyid(i.relowner), NULL::text,
         CASE WHEN e.oid IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         e.extname,e.extversion
  FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
  JOIN pg_class c ON c.oid=x.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_class'::regclass
    AND dep.objid=i.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE n.nspname = ANY(%(schemas)s)
), policy_objects AS (
  SELECT 'POLICY', n.nspname, pol.polname, '', c.relname,
         concat_ws('|',pol.polcmd,pol.polpermissive,pg_get_expr(pol.polqual,pol.polrelid),
                   pg_get_expr(pol.polwithcheck,pol.polrelid)),
         pg_get_userbyid(c.relowner),NULL::text,'APPLICATION',NULL::text,NULL::text
  FROM pg_policy pol JOIN pg_class c ON c.oid=pol.polrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname = ANY(%(schemas)s)
), rule_objects AS (
  SELECT 'RULE',n.nspname,r.rulename,'',c.relname,pg_get_ruledef(r.oid,true),
         pg_get_userbyid(c.relowner),NULL::text,
         CASE WHEN e.oid IS NULL THEN 'APPLICATION' ELSE 'EXTENSION' END,
         e.extname,e.extversion
  FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class
  JOIN pg_namespace n ON n.oid=c.relnamespace
  LEFT JOIN pg_depend dep ON dep.classid='pg_rewrite'::regclass
    AND dep.objid=r.oid AND dep.deptype='e'
  LEFT JOIN pg_extension e ON e.oid=dep.refobjid
  WHERE n.nspname = ANY(%(schemas)s) AND r.rulename <> '_RETURN'
), event_trigger_objects AS (
  SELECT 'EVENT_TRIGGER','',e.evtname,'','',
         concat_ws('|',e.evtevent,array_to_string(e.evttags,','),p.proname),
         pg_get_userbyid(e.evtowner),
         CASE e.evtenabled WHEN 'O' THEN 'ENABLED' WHEN 'D' THEN 'DISABLED'
              WHEN 'R' THEN 'REPLICA' WHEN 'A' THEN 'ALWAYS' END,
         'APPLICATION',NULL::text,NULL::text
  FROM pg_event_trigger e JOIN pg_proc p ON p.oid=e.evtfoid
), extension_objects AS (
  SELECT 'EXTENSION',n.nspname,e.extname,'','',e.extversion,
         pg_get_userbyid(e.extowner),NULL::text,'EXTENSION',e.extname,e.extversion
  FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
)
SELECT * FROM relation_objects UNION ALL SELECT * FROM function_objects
UNION ALL SELECT * FROM trigger_objects UNION ALL SELECT * FROM constraint_objects
UNION ALL SELECT * FROM index_objects UNION ALL SELECT * FROM policy_objects
UNION ALL SELECT * FROM rule_objects UNION ALL SELECT * FROM event_trigger_objects
UNION ALL SELECT * FROM extension_objects
ORDER BY 1,2,3,4,5
"""


def connect():
    import psycopg2
    dsn = os.getenv("DATABASE_DSN")
    password = os.getenv("DB_PASS") or os.getenv("PGPASSWORD")
    if dsn:
        if re.search(r"(?i)(?:password|passwd)\\s*=", dsn):
            raise RuntimeError("DATABASE_DSN must not embed a password")
        return psycopg2.connect(dsn, connect_timeout=5)
    dbname = os.getenv("DB_NAME") or os.getenv("PGDATABASE")
    user = os.getenv("DB_USER") or os.getenv("PGUSER")
    host = os.getenv("DB_HOST") or os.getenv("PGHOST")
    if not all((dbname, user, host)):
        raise RuntimeError(
            "explicit DB_NAME/PGDATABASE, DB_USER/PGUSER and DB_HOST/PGHOST are required"
        )
    kwargs = {
        "host": host, "port": int(os.getenv("DB_PORT") or os.getenv("PGPORT") or "5432"),
        "dbname": dbname, "user": user, "connect_timeout": 5,
    }
    if password:
        kwargs["password"] = password
    elif not os.getenv("PGPASSFILE") and not (Path.home() / ".pgpass").exists():
        raise RuntimeError("explicit credential source is required")
    return psycopg2.connect(**kwargs)


def environment(conn, explicit: str | None) -> str:
    env = (explicit or os.getenv("TRADING_MODE") or os.getenv("ENVIRONMENT") or "").upper()
    if env in {"LIVE", "PAPER"}:
        return env
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        name = cur.fetchone()[0].lower()
    if name.endswith("_live"):
        return "LIVE"
    if name.endswith("_paper"):
        return "PAPER"
    raise RuntimeError("environment must be explicitly LIVE or PAPER")


def inventory(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(CATALOG_SQL, {
            "schemas": list(APPROVED_SCHEMAS),
        })
        rows = cur.fetchall()
    result = []
    seen_routines: dict[tuple[str, str], int] = {}
    for row in rows:
        typ, schema, name = row[:3]
        if typ in {"FUNCTION", "PROCEDURE"}:
            seen_routines[(schema, name)] = seen_routines.get((schema, name), 0) + 1
    for (
        typ, schema, name, args, parent, definition, owner, enabled,
        management, extension_name, extension_version,
    ) in rows:
        if (
            name.startswith(("schema_migration_ledger_v1", "schema_baseline_adoption_v1"))
            or name.startswith((
                "prevent_schema_provenance_", "trg_schema_provenance_",
                "trg_schema_migration_", "trg_schema_baseline_",
                "ix_schema_migration_",
            ))
        ):
            management = "INTERNAL"
        result.append({
            "object_type": typ, "schema": schema, "object_name": name,
            "identity_arguments": args or "", "parent_relation": parent or "",
            "canonical_definition_sha256": fingerprint(definition or ""),
            "owner_contract": owner, "enabled_state": enabled,
            "management": management,
            "extension_name": extension_name,
            "extension_version": extension_version,
            "overloaded": seen_routines.get((schema, name), 0) > 1,
        })
    return result


def load_difference_contract(path: str | Path = DEFAULT_DIFFERENCES) -> dict:
    contract = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_difference_contract(contract)
    return contract


def gate(conn, manifest: dict, env: str, difference_contract: dict) -> tuple[str, list]:
    # A preceding identity/current_database probe may have opened a transaction.
    # The gate owns a fresh read-only transaction and never preserves caller work.
    conn.rollback()
    conn.set_session(readonly=True)
    status, drifts = compare_inventory(
        manifest, inventory(conn), env,
        difference_contract=difference_contract,
    )
    conn.rollback()
    return status, drifts


def manifest_entry(observed: dict, applicability: str, env: str) -> dict:
    entry = dict(observed)
    entry.update({
        "applicability": applicability,
        "source_file": "BASELINE_ADOPTION_LOCAL_CATALOG_20260724",
        "risk_class": (
            "P0" if observed["object_type"] in {"FUNCTION", "TRIGGER"}
            and any(p in observed["object_name"] for p in ("reconcil", "fill_position_pnl"))
            else "P1"
        ),
        "provenance_status": (
            "BASELINE_ADOPTED_LEGACY_P0"
            if observed["object_type"] in {"FUNCTION", "TRIGGER"}
            and any(p in observed["object_name"] for p in ("reconcil", "fill_position_pnl"))
            else "BASELINE_ADOPTED"
        ),
    })
    return entry


def unique_inventory_map(rows: list[dict]) -> dict:
    result = {}
    for row in rows:
        key = object_key(row)
        if key in result:
            raise ValueError(f"duplicate inventory identity: {key}")
        result[key] = row
    return result


def merge_manifest(
    live: list[dict], paper: list[dict], difference_contract: dict,
) -> dict:
    rules = difference_rule_map(difference_contract)
    live_map, paper_map = unique_inventory_map(live), unique_inventory_map(paper)
    objects = []
    for key in sorted(set(live_map) | set(paper_map)):
        left, right = live_map.get(key), paper_map.get(key)
        if left and right:
            entry = manifest_entry(left, "COMMON", "LIVE")
            if left["canonical_definition_sha256"] != right["canonical_definition_sha256"]:
                rule = rules.get(key)
                if rule is None:
                    raise RuntimeError(f"unmatched LIVE/PAPER definition difference: {key}")
                entry["environment_fingerprints"] = {
                    "LIVE": left["canonical_definition_sha256"],
                    "PAPER": right["canonical_definition_sha256"],
                }
                entry["difference_classification"] = rule["classification"]
            if left.get("enabled_state") != right.get("enabled_state"):
                entry["environment_enabled_states"] = {
                    "LIVE": left.get("enabled_state"), "PAPER": right.get("enabled_state")
                }
            objects.append(entry)
        else:
            env, observed = ("LIVE", left) if left else ("PAPER", right)
            objects.append(manifest_entry(observed, f"{env}_ONLY", env))
    return {
        "baseline_version": BASELINE_VERSION,
        "generated_from_git_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "normalization_contract": "common.schema_provenance.normalize_sql:v1",
        "objects": objects,
    }


def inventory_from_catalog_json(path: str) -> list[dict]:
    payload = Path(path).read_text(encoding="utf-8")
    rows = json.loads(payload[payload.find("["):])
    result = []
    for row in rows:
        result.append({
            "object_type": row["object_type"],
            "schema": row["schema"],
            "object_name": row["object_name"],
            "identity_arguments": row.get("identity_arguments") or "",
            "parent_relation": row.get("parent_relation") or "",
            "canonical_definition_sha256": fingerprint(row.get("definition") or ""),
            "definition": row.get("definition") or "",
            "owner_contract": row.get("owner_contract"),
            "enabled_state": row.get("enabled_state"),
            "management": row.get("management", "APPLICATION"),
            "extension_name": row.get("extension_name"),
            "extension_version": row.get("extension_version"),
        })
    return result


DECLARATION_RE = re.compile(
    r"(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:TABLE|VIEW|MATERIALIZED\s+VIEW|FUNCTION|PROCEDURE|TRIGGER|"
    r"POLICY|RULE|INDEX|SEQUENCE)\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:public\.)?[\"']?([a-zA-Z_][a-zA-Z0-9_]*)"
)


def repo_declaration_index() -> dict[str, list[dict[str, str]]]:
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    paths = subprocess.check_output(
        ["git", "ls-files"], cwd=ROOT, text=True
    ).splitlines()
    result: dict[str, list[dict[str, str]]] = {}
    for relative in sorted(paths):
        if not relative.startswith((
            "db/", "scripts/", "api/", "automation_runner/", "common/", "services/",
        )):
            continue
        path = ROOT / relative
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for match in DECLARATION_RE.finditer(text):
            name = match.group(1).lower()
            status = (
                "TRACKED_RUNTIME_DDL" if path.suffix == ".py"
                else "TRACKED_CURRENT"
            )
            result.setdefault(name, []).append({
                "provenance_status": status,
                "source_path": relative,
                "source_commit": head,
            })
    return result


def repo_historical_declaration_index() -> dict[str, list[dict[str, str]]]:
    """Index deleted/older declarations from one deterministic history walk."""
    output = subprocess.check_output(
        [
            "git", "log", "--all", "--format=@@COMMIT:%H", "--patch",
            "--", "db", "scripts", "api", "automation_runner", "common", "services",
        ],
        cwd=ROOT,
        text=True,
        errors="replace",
    )
    current_commit = ""
    current_path = ""
    result: dict[str, list[dict[str, str]]] = {}
    for line in output.splitlines():
        if line.startswith("@@COMMIT:"):
            current_commit = line.split(":", 1)[1]
            current_path = ""
            continue
        if line.startswith("+++ b/"):
            current_path = line[6:]
            continue
        if not current_commit or not current_path or not line.startswith(("+", " ")):
            continue
        match = DECLARATION_RE.search(line[1:])
        if not match:
            continue
        result.setdefault(match.group(1).lower(), []).append({
            "provenance_status": "TRACKED_HISTORICAL",
            "source_path": current_path,
            "source_commit": current_commit,
        })
    return result


def _dependency_model(
    live: list[dict], paper: list[dict], dependency_payloads: list[dict],
) -> tuple[dict[str, set[str]], dict[str, str]]:
    rows = live + paper
    by_relation = {
        (row["schema"], row["object_name"]): identity_string(row)
        for row in rows
        if row["object_type"] in {"TABLE", "VIEW", "MATERIALIZED_VIEW"}
    }
    by_routine = {
        (row["schema"], row["object_name"], row.get("identity_arguments", "")):
            identity_string(row)
        for row in rows if row["object_type"] in {"FUNCTION", "PROCEDURE"}
    }
    by_trigger = {
        (row["schema"], row["object_name"], row.get("parent_relation", "")):
            identity_string(row)
        for row in rows if row["object_type"] == "TRIGGER"
    }
    by_sequence = {
        (row["schema"], row["object_name"]): identity_string(row)
        for row in rows if row["object_type"] == "SEQUENCE"
    }
    edges: dict[str, set[str]] = {}
    sequence_roots: dict[str, str] = {}
    for payload in dependency_payloads:
        for seq_schema, seq_name, table_schema, table_name, _column in payload.get(
            "sequence_owner", []
        ):
            child = by_sequence.get((seq_schema, seq_name))
            root = by_relation.get((table_schema, table_name))
            if child and root:
                edges.setdefault(child, set()).add(root)
                sequence_roots[child] = root
        for view_schema, view_name, ref_schema, ref_name in payload.get(
            "view_relation", []
        ):
            source = by_relation.get((view_schema, view_name))
            target = by_relation.get((ref_schema, ref_name))
            if source and target and source != target:
                edges.setdefault(source, set()).add(target)
        for schema, trigger, parent, fn_schema, fn_name, args in payload.get(
            "trigger_function", []
        ):
            source = by_trigger.get((schema, trigger, parent))
            target = by_routine.get((fn_schema, fn_name, args))
            if source and target:
                edges.setdefault(source, set()).add(target)
    return edges, sequence_roots


def build_hierarchical_manifest(
    live: list[dict],
    paper: list[dict],
    difference_contract: dict,
    dependency_payloads: list[dict],
) -> dict:
    flat = merge_manifest(live, paper, difference_contract)
    edges, sequence_roots = _dependency_model(live, paper, dependency_payloads)
    declarations = repo_declaration_index()
    historical = repo_historical_declaration_index()
    canonical_names = {
        "fn_refresh_adaptive_sizing_for_slot", "bot_control", "orc_apply_runs_v1",
        "slot_capital_policy", "v_orc_candidates_v5c", "v_orc_picks_v5",
        "v_orc_v62_candidates", "v_positions_pnl_gross_real",
        "v_positions_pnl_net_real_ssot", "v_slot_adaptive_sizing_v1",
    }
    objects = []
    for entry in flat["objects"]:
        identity = identity_string(entry)
        evidence_options = declarations.get(entry["object_name"].lower(), [])
        if not evidence_options:
            evidence_options = historical.get(entry["object_name"].lower(), [])
        evidence = sorted(
            evidence_options,
            key=lambda item: (
                item["provenance_status"] == "TRACKED_CURRENT",
                item["source_path"],
            ),
        )[-1] if evidence_options else None
        entry["risk_class"] = classify_root_risk(entry)
        row = hierarchical_entry(
            entry,
            dependency_identities=edges.get(identity, ()),
            sequence_root=sequence_roots.get(identity),
            source_evidence=evidence,
            canonical_blocker=entry["object_name"] in canonical_names,
        )
        row.pop("definition", None)
        objects.append(row)
    objects.sort(key=lambda row: row["identity"])
    by_identity = {row["identity"]: row for row in objects}
    for row in objects:
        if row["provenance_role"] == "OWNED_CHILD":
            root = by_identity.get(row["root_identity"])
            if root:
                row["risk_class"] = root["risk_class"]
                if root["adoption_status"].startswith("BLOCKED_"):
                    row["adoption_status"] = root["adoption_status"]
                elif row["provenance_status"] == "RUNTIME_OBSERVED_PENDING_ADOPTION":
                    row["provenance_status"] = root["provenance_status"]
                    row["source_path"] = root["source_path"]
                    row["source_commit"] = root["source_commit"]
                    row["adoption_status"] = root["adoption_status"]
    flat["objects"] = objects
    flat["catalog_gate"] = hierarchical_readiness(objects)
    flat["catalog_gate"] = {
        "catalog_coverage_ready": flat["catalog_gate"]["catalog_coverage_ready"],
        "adoption_ready": flat["catalog_gate"]["adoption_ready"],
        "blocked_count": len(flat["catalog_gate"]["blocked"]),
    }
    return flat


def main() -> int:
    parser = argparse.ArgumentParser(description="WalTrade schema baseline/provenance V1")
    parser.add_argument(
        "command",
        choices=(
            "inventory", "build-manifest", "build-hierarchical-manifest", "gate",
            "canonicalize-raw",
        ),
    )
    parser.add_argument("--environment", choices=("LIVE", "PAPER"))
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--differences", default=str(DEFAULT_DIFFERENCES))
    parser.add_argument("--live-inventory")
    parser.add_argument("--paper-inventory")
    parser.add_argument("--live-dependencies")
    parser.add_argument("--paper-dependencies")
    parser.add_argument("--report-dir")
    parser.add_argument(
        "--environment-identity",
        choices=("LOCAL_LIVE", "LOCAL_PAPER", "VPS_LIVE", "VPS_PAPER"),
    )
    parser.add_argument("--raw-csv")
    parser.add_argument("--output-dir")
    parser.add_argument("--expected-raw-sha256")
    args = parser.parse_args()
    if args.command == "canonicalize-raw":
        if not args.environment_identity or not args.raw_csv or not args.output_dir:
            parser.error(
                "canonicalize-raw requires --environment-identity, --raw-csv "
                "and --output-dir"
            )
        manifest = load_checkpoint_manifest(args.manifest)
        differences = load_difference_contract(args.differences)
        artifact_module = ROOT / "common" / "database_baseline_artifact.py"
        canonicalize_raw(
            raw_path=args.raw_csv,
            environment_identity=args.environment_identity,
            manifest=manifest,
            differences=differences,
            tracked_paths=candidate_tracked_paths(),
            output_dir=args.output_dir,
            canonicalizer_git_sha=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            source_paths=(
                artifact_module,
                ROOT / "common" / "schema_provenance.py",
                Path(args.manifest),
                Path(args.differences),
            ),
            expected_raw_sha256=args.expected_raw_sha256,
        )
        return 0
    if args.command in {"build-manifest", "build-hierarchical-manifest"}:
        if not args.live_inventory or not args.paper_inventory:
            parser.error("build-manifest requires --live-inventory and --paper-inventory")
        live_inventory = inventory_from_catalog_json(args.live_inventory)
        paper_inventory = inventory_from_catalog_json(args.paper_inventory)
        differences = load_difference_contract(args.differences)
        if args.command == "build-hierarchical-manifest":
            if not args.live_dependencies or not args.paper_dependencies:
                parser.error(
                    "build-hierarchical-manifest requires both dependency payloads"
                )
            dependencies = [
                json.loads(Path(args.live_dependencies).read_text()),
                json.loads(Path(args.paper_dependencies).read_text()),
            ]
            manifest = build_hierarchical_manifest(
                live_inventory, paper_inventory, differences, dependencies,
            )
        else:
            manifest = merge_manifest(live_inventory, paper_inventory, differences)
        validate_tracked_provenance(manifest, candidate_tracked_paths())
        destination = Path(args.manifest)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps({"manifest": str(destination), "objects": len(manifest["objects"])}))
        return 0
    conn = connect()
    try:
        env = environment(conn, args.environment)
        if args.command == "inventory":
            print(json.dumps({"environment": env, "objects": inventory(conn)}, sort_keys=True))
            return 0
        manifest = load_checkpoint_manifest(args.manifest)
        difference_contract = load_difference_contract(args.differences)
        status, drifts = gate(conn, manifest, env, difference_contract)
        print(json.dumps({
            "environment": env, "status": status,
            "diff": [d.as_dict() for d in drifts],
        }, indent=2, sort_keys=True))
        return 0 if status == "READY" else 2
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

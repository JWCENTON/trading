#!/usr/bin/env python3
"""Read-only canonical exporter for effective strategy_params DB overrides."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence


CONTRACT_VERSION = "WALTRADE_PARAMETER_EXPORT_V1_1"
RECORD_SORT_FIELDS = (
    "environment", "strategy", "symbol", "interval", "parameter_name",
)
RECORD_ID_FIELDS = ("strategy", "symbol", "interval", "parameter_name")
FINGERPRINT_FIELDS = (
    "contract_version",
    "environment",
    "mode",
    "strategy",
    "symbol",
    "interval",
    "parameter_name",
    "effective_value",
    "value_type",
    "source_layer",
    "source_priority",
    "runtime_service",
    "runtime_child_identity",
    "consumed",
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")

PARAMETER_QUERY = r"""
BEGIN READ ONLY;
SELECT json_build_object(
    'row_id', sp.id,
    'strategy', sp.strategy,
    'symbol', sp.symbol,
    'interval', sp.interval,
    'parameter_name', sp.param_name,
    'effective_value', sp.param_value::TEXT,
    'source_updated_at', to_char(
        sp.updated_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ),
    'history_source', history.source
)::TEXT
FROM strategy_params sp
LEFT JOIN LATERAL (
    SELECT h.source
    FROM strategy_params_history h
    WHERE h.strategy = sp.strategy
      AND h.symbol = sp.symbol
      AND COALESCE(h.interval, '1m') = sp.interval
      AND h.param_name = sp.param_name
      AND h.new_value = sp.param_value
      AND h.changed_at <= sp.updated_at
    ORDER BY h.changed_at DESC, h.id DESC
    LIMIT 1
) history ON TRUE
ORDER BY sp.strategy, sp.symbol, sp.interval, sp.param_name;
ROLLBACK;
""".strip()


class ExportContractError(RuntimeError):
    """Raised when source rows cannot form a complete canonical export."""


def canonical_decimal(value: str) -> str:
    decimal = Decimal(value)
    if not decimal.is_finite():
        raise ExportContractError(f"non-finite parameter value: {value}")
    normalized = format(decimal, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return "0" if normalized in ("", "-0") else normalized


def canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def record_identity(record: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(record[field]) for field in RECORD_ID_FIELDS)


def canonical_payload(document: dict[str, Any]) -> dict[str, Any]:
    records = sorted(
        (
            {field: record[field] for field in FINGERPRINT_FIELDS}
            for record in document["records"]
        ),
        key=lambda record: tuple(str(record[field]) for field in RECORD_SORT_FIELDS),
    )
    return {
        "contract_version": document["contract_version"],
        "environment": document["environment"],
        "mode": document["mode"],
        "records": records,
    }


def canonical_sha256(document: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(canonical_payload(document))).hexdigest()


def lineage_payload(document: dict[str, Any]) -> dict[str, Any]:
    records = sorted(
        (
            {
                "strategy": record["strategy"],
                "symbol": record["symbol"],
                "interval": record["interval"],
                "parameter_name": record["parameter_name"],
                "lineage_metadata": record["lineage_metadata"],
            }
            for record in document["records"]
        ),
        key=lambda record: tuple(str(record[field]) for field in RECORD_ID_FIELDS),
    )
    return {"contract_version": document["contract_version"], "records": records}


def lineage_sha256(document: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(lineage_payload(document))).hexdigest()


def discover_runtime_defaults(
    raw_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Add active code defaults absent from DB without inventing DB provenance."""
    rows = [dict(row) for row in raw_rows]
    slots = {
        (str(row["strategy"]), str(row["symbol"]), str(row["interval"]))
        for row in rows
    }
    identities = {
        (
            str(row["strategy"]), str(row["symbol"]), str(row["interval"]),
            str(row["parameter_name"]),
        )
        for row in rows
    }
    for strategy, symbol, interval in sorted(slots):
        identity = (strategy, symbol, interval, "MIN_NOTIONAL_BUFFER_PCT")
        if strategy in ("RSI", "TREND", "SUPERTREND", "BBRANGE") and identity not in identities:
            rows.append({
                "row_id": None,
                "strategy": strategy,
                "symbol": symbol,
                "interval": interval,
                "parameter_name": "MIN_NOTIONAL_BUFFER_PCT",
                "effective_value": "0.05",
                "source_updated_at": None,
                "history_source": None,
                "source_layer": "CODE_DEFAULT",
                "source_priority": 10,
                "source_identity": "code_default:MIN_NOTIONAL_BUFFER_PCT",
                "consumed": True,
            })
    return rows


def build_document(
    raw_rows: Sequence[dict[str, Any]],
    *,
    deployment_id: str,
    environment: str,
    mode: str,
    git_sha: str | None,
    runtime_git_sha: str | None,
    oci_revision: str | None,
    generated_at: str,
) -> dict[str, Any]:
    environment = environment.upper()
    mode = mode.upper()
    if environment not in ("PAPER", "LIVE") or mode not in ("PAPER", "LIVE"):
        raise ExportContractError("environment and mode must be PAPER or LIVE")
    if environment != mode:
        raise ExportContractError("environment and mode must identify the same contract")
    if not deployment_id.strip():
        raise ExportContractError("deployment_id cannot be empty")
    for label, sha in (
        ("git_sha", git_sha),
        ("runtime_git_sha", runtime_git_sha),
        ("oci_revision", oci_revision),
    ):
        if sha is not None and not SHA_RE.fullmatch(sha):
            raise ExportContractError(f"{label} must be a lowercase 40-character SHA")

    records: list[dict[str, Any]] = []
    identities: set[tuple[str, ...]] = set()
    for raw in raw_rows:
        required = (
            "strategy", "symbol", "interval", "parameter_name",
            "effective_value",
        )
        if any(raw.get(field) is None for field in required):
            raise ExportContractError(f"incomplete source row: {raw!r}")
        history_source = raw.get("history_source")
        source_identity = raw.get("source_identity") or (
            f"strategy_params_history:{history_source}"
            if history_source else "strategy_params:current_row"
        )
        strategy = str(raw["strategy"])
        symbol = str(raw["symbol"])
        interval = str(raw["interval"])
        parameter_name = str(raw["parameter_name"])
        consumed = bool(raw.get("consumed", not (
            strategy == "SUPERTREND" and parameter_name == "ORDER_NOTIONAL_USDC"
        )))
        record = {
            "contract_version": CONTRACT_VERSION,
            "deployment_id": deployment_id,
            "environment": environment,
            "mode": mode,
            "strategy": strategy,
            "symbol": symbol,
            "interval": interval,
            "parameter_name": parameter_name,
            "effective_value": canonical_decimal(str(raw["effective_value"])),
            "value_type": "DECIMAL",
            "source_layer": str(raw.get("source_layer", "STRATEGY_PARAMS_DB")),
            "source_priority": int(raw.get("source_priority", 100)),
            "runtime_service": "bot-runner",
            "runtime_child_identity": f"{strategy}:{symbol}:{interval}",
            "consumed": consumed,
            "lineage_metadata": {
                "source_identity": str(source_identity),
                "source_updated_at": raw.get("source_updated_at"),
                "history_source": history_source,
                "history_row_present": history_source is not None,
            },
        }
        identity = record_identity(record)
        if identity in identities:
            raise ExportContractError(f"duplicate parameter identity: {identity}")
        identities.add(identity)
        records.append(record)

    records.sort(key=lambda record: tuple(record[field] for field in RECORD_SORT_FIELDS))
    document = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": generated_at,
        "git_sha": git_sha,
        "runtime_git_sha": runtime_git_sha,
        "oci_revision": oci_revision,
        "deployment_id": deployment_id,
        "environment": environment,
        "mode": mode,
        "record_count": len(records),
        "consumed_parameter_count": sum(record["consumed"] for record in records),
        "non_consumed_parameter_count": sum(not record["consumed"] for record in records),
        "effective_canonical_sha256": "",
        "lineage_sha256": "",
        "records": records,
    }
    document["effective_canonical_sha256"] = canonical_sha256(document)
    document["lineage_sha256"] = lineage_sha256(document)
    return document


def _psql_command(args: argparse.Namespace) -> list[str]:
    if args.dsn:
        return ["psql", "--dbname", args.dsn, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", PARAMETER_QUERY]
    inner = 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -qAt -v ON_ERROR_STOP=1 -c "$1"'
    if args.db_container:
        return ["docker", "exec", args.db_container, "sh", "-lc", inner, "sh", PARAMETER_QUERY]
    command = ["docker", "compose"]
    for compose_file in args.compose_file:
        command.extend(("-f", compose_file))
    if args.compose_project:
        command.extend(("-p", args.compose_project))
    command.extend(("exec", "-T", args.db_service, "sh", "-lc", inner, "sh", PARAMETER_QUERY))
    return command


def read_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    result = subprocess.run(
        _psql_command(args),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=os.environ.copy(),
    )
    if result.returncode:
        detail = result.stderr.strip().splitlines()[-1:] or ["unknown psql error"]
        raise ExportContractError(f"read-only parameter query failed: {detail[0]}")
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def _git_sha() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], check=False, capture_output=True, text=True
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and SHA_RE.fullmatch(value) else None


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    source = result.add_mutually_exclusive_group(required=True)
    source.add_argument("--dsn", help="PostgreSQL DSN; prefer PG* environment or .pgpass")
    source.add_argument("--db-container", help="Docker DB container execution context")
    source.add_argument("--compose-project", help="Docker Compose project execution context")
    result.add_argument("--compose-file", action="append", default=[])
    result.add_argument("--db-service", default="db")
    result.add_argument("--deployment-id", required=True)
    result.add_argument("--environment", required=True, choices=("PAPER", "LIVE"))
    result.add_argument("--mode", required=True, choices=("PAPER", "LIVE"))
    result.add_argument("--output", required=True)
    result.add_argument("--git-sha")
    result.add_argument("--runtime-git-sha")
    result.add_argument("--oci-revision")
    result.add_argument("--generated-at")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    generated_at = args.generated_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    document = build_document(
        discover_runtime_defaults(read_rows(args)),
        deployment_id=args.deployment_id,
        environment=args.environment,
        mode=args.mode,
        git_sha=args.git_sha or _git_sha(),
        runtime_git_sha=args.runtime_git_sha,
        oci_revision=args.oci_revision,
        generated_at=generated_at,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(canonical_json_bytes(document))
    print(json.dumps({
        "output": str(output),
        "record_count": document["record_count"],
        "consumed_parameter_count": document["consumed_parameter_count"],
        "non_consumed_parameter_count": document["non_consumed_parameter_count"],
        "effective_canonical_sha256": document["effective_canonical_sha256"],
        "lineage_sha256": document["lineage_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

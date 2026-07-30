from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
from enum import Enum
import json
import os
from pathlib import Path
import sys
from typing import Any

import psycopg2
from psycopg2.extensions import parse_dsn

from common.db import read_only_db_conn
from common.legacy_recovery import (
    FillApplicationProof,
    IngestionApplicationStatus,
    LegacyPositionRecomputationService,
    LegacyRecoveryPlanner,
    UnappliedFillRecoveryService,
)
from common.legacy_recovery_repository import (
    EvidenceStatus,
    ExternalExecutionEvidenceRepository,
    LegacyPositionEvidenceRepository,
    UnappliedFillEvidenceRepository,
)
from common.legacy_recovery_schema import (
    LegacyRecoverySchemaReadinessRepository,
    SchemaContractStatus,
)


PLANNER_VERSION = "LEGACY_RECOVERY_PLANNER_V2"


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: _json_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Read-only legacy recovery planner")
    result.add_argument(
        "--database-url-env", required=True,
        help="Name of environment variable containing the PostgreSQL DSN",
    )
    result.add_argument("--environment", choices=("LIVE", "PAPER"), required=True)
    result.add_argument("--expected-database", required=True)
    result.add_argument("--output-json")
    sub = result.add_subparsers(dest="command", required=True)
    sub.add_parser("check-schema")
    position = sub.add_parser("plan-position")
    position.add_argument("--position-id", required=True, type=int)
    fill = sub.add_parser("plan-fill")
    fill.add_argument("--source", required=True)
    fill.add_argument("--trade-id", required=True)
    fill.add_argument("--order-id", required=True)
    external = sub.add_parser("classify-external")
    external.add_argument("--source", required=True)
    external.add_argument("--trade-id", required=True)
    external.add_argument("--order-id", required=True)
    return result


def _connection_factory(args):
    dsn = os.environ.get(args.database_url_env)
    if not dsn:
        raise RuntimeError(
            f"CONFIGURATION_ERROR:{args.database_url_env}_IS_REQUIRED"
        )
    parsed = parse_dsn(dsn)
    configured = str(parsed.get("dbname") or "")
    if configured != args.expected_database:
        raise RuntimeError(
            f"DATABASE_IDENTITY_MISMATCH:{configured!r}"
        )
    expected_environment = (
        "PAPER" if "paper" in configured.lower() else "LIVE"
    )
    if args.environment != expected_environment:
        raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")

    def connect():
        return psycopg2.connect(dsn, connect_timeout=5)

    return connect


def _identity(connection, args) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='15s'")
        cur.execute(
            "SELECT current_database(),current_user,"
            "current_setting('transaction_read_only')"
        )
        database, user, readonly = cur.fetchone()
    if database != args.expected_database or readonly != "on":
        raise RuntimeError("READ_ONLY_IDENTITY_GUARD_FAILED")
    return {
        "environment": args.environment,
        "database": database,
        "database_user": user,
        "transaction_read_only": True,
    }


def _schema(connection) -> dict[str, Any]:
    readiness = LegacyRecoverySchemaReadinessRepository().check(connection)
    return _json_value(readiness)


def _position(connection, args, environment_identity, schema):
    if schema["status"] != SchemaContractStatus.PRESENT_VALID.value:
        raise RuntimeError("SCHEMA_NOT_READY")
    envelope = LegacyPositionEvidenceRepository().read(
        connection, position_id=args.position_id,
    )
    base = {
        "schema_status": schema["status"],
        "environment_identity": environment_identity,
        "incident_type": "LEGACY_POSITION",
        "incident_identity": str(args.position_id),
        "planner_version": PLANNER_VERSION,
        "evidence_status": envelope.evidence_status.value,
        "missing_evidence": envelope.missing_evidence,
        "conflicting_evidence": envelope.conflicting_evidence,
        "provenance_completeness": (
            "COMPLETE"
            if envelope.source_provenance.get("account_fingerprints")
            and envelope.source_provenance.get("instrument_fingerprints")
            else "INCOMPLETE"
        ),
    }
    if envelope.evidence is None:
        return {
            **base, "semantic_fingerprint": None,
            "gross_entry_qty": None, "base_asset_entry_fee_qty": None,
            "net_entry_inventory_qty": None, "gross_exit_qty": None,
            "base_asset_exit_fee_qty": None,
            "raw_remaining_inventory_qty": None,
            "normalized_remaining_inventory_qty": None,
            "precision_status": None, "precision_source": None,
            "fee_valuation_status": "UNKNOWN",
            "fee_valuation_source": None,
            "lifecycle_status": (
                (envelope.current_state or {}).get("position") or {}
            ).get("status"),
            "recommended_lifecycle_status": None,
            "financial_truth_current_status": "UNKNOWN",
            "financial_truth_eligibility": False,
            "eligible_actions": [],
            "blocked_actions": list(LegacyRecoveryPlanner.POSITION_ACTIONS),
            "blocking_reasons": list(
                envelope.missing_evidence + envelope.conflicting_evidence
            ),
            "expected_row_changes": [],
            "post_state_invariants": ["NO_WRITES"],
        }
    result = LegacyPositionRecomputationService().recompute(envelope.evidence)
    plan = LegacyRecoveryPlanner().position_plan(result)
    valuations = [
        fill.fee_valuation
        for fill in envelope.evidence.entry_fills + envelope.evidence.exit_fills
    ]
    statuses = sorted({item.status.value for item in valuations})
    sources = sorted({
        item.valuation_source for item in valuations if item.valuation_source
    })
    current_ft = (envelope.current_state or {}).get("financial_truth") or []
    current_position = (envelope.current_state or {}).get("position") or {}
    return {
        **base, "semantic_fingerprint": result.evidence_fingerprint,
        "gross_entry_qty": result.gross_entry_qty,
        "base_asset_entry_fee_qty": result.base_asset_entry_fee_qty,
        "net_entry_inventory_qty": result.net_entry_inventory_qty,
        "gross_exit_qty": result.gross_exit_qty,
        "base_asset_exit_fee_qty": result.base_asset_exit_fee_qty,
        "raw_remaining_inventory_qty": result.raw_remaining_qty,
        "normalized_remaining_inventory_qty": result.normalized_remaining_qty,
        "precision_status": (
            result.precision_status.value if result.precision_status else None
        ),
        "precision_source": result.precision_source,
        "fee_valuation_status": statuses,
        "fee_valuation_source": sources,
        "lifecycle_status": current_position.get("status"),
        "recommended_lifecycle_status": (
            "CLOSED" if result.lifecycle_should_be_closed else "OPEN"
        ),
        "financial_truth_current_status": (
            current_ft[0]["financial_truth_status"] if current_ft else "ABSENT"
        ),
        "financial_truth_eligibility": result.financial_truth_eligibility,
        "eligible_actions": plan.eligible_actions,
        "blocked_actions": plan.blocked_actions,
        "blocking_reasons": result.blocking_reasons,
        "expected_row_changes": plan.expected_row_changes,
        "post_state_invariants": plan.post_state_invariants,
    }


def _fill(connection, args, environment_identity, schema):
    if schema["status"] != SchemaContractStatus.PRESENT_VALID.value:
        raise RuntimeError("SCHEMA_NOT_READY")
    envelope = UnappliedFillEvidenceRepository().read(
        connection, source=args.source,
        trade_id=args.trade_id, order_id=args.order_id,
    )
    if envelope.evidence is None:
        raise RuntimeError(f"FILL_EVIDENCE_{envelope.evidence_status.value}")
    candidate = envelope.evidence
    state = envelope.current_state or {}
    ingestion = state["ingestion"]
    fills = state["local_fills"]
    proof = None
    if fills or ingestion.get("applied_fingerprint") is not None:
        local_id = f"fill:{fills[0]['id']}" if len(fills) == 1 else None
        proof = FillApplicationProof(
            candidate.source, candidate.trade_id,
            candidate.exchange_order_id, candidate.semantic_fingerprint,
            local_id, ingestion.get("applied_fingerprint"),
            ingestion.get("applied_at"),
        )
    decision = UnappliedFillRecoveryService().classify(candidate, proof)
    plan = LegacyRecoveryPlanner().fill_plan(
        decision, candidate.semantic_fingerprint,
    )
    return {
        "schema_status": schema["status"],
        "environment_identity": environment_identity,
        "incident_type": "UNAPPLIED_FILL",
        "incident_identity": (
            f"{candidate.source}:{candidate.trade_id}:"
            f"{candidate.exchange_order_id}"
        ),
        "planner_version": PLANNER_VERSION,
        "semantic_fingerprint": candidate.semantic_fingerprint,
        "source": candidate.source, "trade_id": candidate.trade_id,
        "exchange_order_id": candidate.exchange_order_id,
        "client_order_id": candidate.client_order_id,
        "bot_ownership_status": candidate.ownership.value,
        "local_fill_status": "PRESENT" if fills else "ABSENT",
        "ingestion_application_status": decision.status.value,
        "applied_fingerprint_status": (
            "PRESENT" if ingestion.get("applied_fingerprint") else "ABSENT"
        ),
        "candidate_position_linkage": candidate.position_id,
        "classification": decision.status.value,
        "eligible_actions": plan.eligible_actions,
        "blocked_actions": plan.blocked_actions,
        "blocking_reasons": decision.blocking_reasons,
        "expected_row_changes": plan.expected_row_changes,
        "post_state_invariants": plan.post_state_invariants,
    }


def _external(connection, args, environment_identity, schema):
    if schema["status"] != SchemaContractStatus.PRESENT_VALID.value:
        raise RuntimeError("SCHEMA_NOT_READY")
    envelope = ExternalExecutionEvidenceRepository().read(
        connection, source=args.source,
        trade_id=args.trade_id, order_id=args.order_id,
    )
    if envelope.evidence_status is not EvidenceStatus.COMPLETE:
        raise RuntimeError(
            f"EXTERNAL_EVIDENCE_{envelope.evidence_status.value}"
        )
    payload = envelope.evidence
    return {
        "schema_status": schema["status"],
        "environment_identity": environment_identity,
        "incident_type": "EXTERNAL_EXECUTION",
        "incident_identity": f"{args.source}:{args.trade_id}:{args.order_id}",
        "planner_version": PLANNER_VERSION,
        "semantic_fingerprint": payload.get("semantic_fingerprint"),
        "source": args.source, "trade_id": args.trade_id,
        "exchange_order_id": args.order_id,
        "client_order_id": payload.get("client_order_id"),
        "bot_ownership_status": "MANUAL_OR_EXTERNAL",
        "local_fill_status": "ABSENT",
        "ingestion_application_status": "EXTERNAL_OR_MANUAL_UNLINKED",
        "applied_fingerprint_status": "ABSENT",
        "candidate_position_linkage": None,
        "classification": "EXTERNAL_OR_MANUAL_UNLINKED",
        "eligible_actions": ["CLASSIFY_EXTERNAL_OR_MANUAL"],
        "blocked_actions": ["CREATE_POSITION", "WRITE_FINANCIAL_TRUTH"],
        "blocking_reasons": [],
        "expected_row_changes": ["repair_audit:plan_only"],
        "post_state_invariants": [
            "no_fabricated_position", "no_automatic_financial_truth",
        ],
    }


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        factory = _connection_factory(args)
        with read_only_db_conn(factory) as connection:
            identity = _identity(connection, args)
            schema = _schema(connection)
            if args.command == "check-schema":
                result = {
                    "schema_status": schema["status"],
                    "environment_identity": identity,
                    "schema": schema,
                }
            elif args.command == "plan-position":
                result = _position(connection, args, identity, schema)
            elif args.command == "plan-fill":
                result = _fill(connection, args, identity, schema)
            else:
                result = _external(connection, args, identity, schema)
        rendered = json.dumps(
            _json_value(result), sort_keys=True, separators=(",", ":"),
        )
        if args.output_json:
            Path(args.output_json).write_text(rendered + "\n", encoding="utf-8")
        else:
            print(rendered)
        return (
            0 if result["schema_status"] == "PRESENT_VALID" else 3
        )
    except Exception as exc:
        print(json.dumps({
            "error": str(exc), "command": getattr(args, "command", None),
        }, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

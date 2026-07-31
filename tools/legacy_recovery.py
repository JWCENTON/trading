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
    semantic_repair_fingerprint,
)
from common.legacy_recovery_repository import (
    EvidenceStatus,
    ExternalEvidenceFileAdapter,
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
    external.add_argument(
        "--evidence-json",
        help="Operator-provided immutable JSON evidence (read-only)",
    )
    open_cohort = sub.add_parser("audit-open-cohort")
    open_cohort.add_argument("--limit", type=int, default=100)
    closed_cohort = sub.add_parser("audit-unresolved-closed")
    closed_cohort.add_argument("--limit", type=int, default=100)
    return result


def _normalize_global_options(argv):
    """Allow documented global identity options before or after a subcommand."""
    if argv is None:
        return None
    values = list(argv)
    global_names = {
        "--database-url-env", "--environment", "--expected-database",
        "--output-json",
    }
    globals_: list[str] = []
    remainder: list[str] = []
    index = 0
    while index < len(values):
        item = values[index]
        if item in global_names and index + 1 < len(values):
            globals_.extend(values[index:index + 2])
            index += 2
        else:
            remainder.append(item)
            index += 1
    return globals_ + remainder


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
    orders = state.get("orders") or []
    lineage_order = orders[0] if len(orders) == 1 else {}
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
    missing_position = (
        candidate.ownership.value == "BOT_OWNED"
        and candidate.position_id is None
        and not fills
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
        "strategy": lineage_order.get("strategy"),
        "interval": lineage_order.get("interval"),
        "order_purpose": lineage_order.get("order_purpose"),
        "local_order_row_id": lineage_order.get("id"),
        "local_order_created_at": lineage_order.get("created_at"),
        "bot_ownership_status": candidate.ownership.value,
        "local_fill_status": "PRESENT" if fills else "ABSENT",
        "ingestion_application_status": decision.status.value,
        "applied_fingerprint_status": (
            "PRESENT" if ingestion.get("applied_fingerprint") else "ABSENT"
        ),
        "candidate_position_linkage": candidate.position_id,
        "linkage_classification": (
            "BOT_OWNED_LINKABLE" if missing_position
            else candidate.ownership.value
        ),
        "incident_model": (
            "MISSING_POSITION_AFTER_FILLED_ENTRY"
            if missing_position else None
        ),
        "classification": (
            "MISSING_POSITION_AFTER_FILLED_ENTRY"
            if missing_position else decision.status.value
        ),
        "eligible_actions": plan.eligible_actions,
        "blocked_actions": plan.blocked_actions,
        "blocking_reasons": decision.blocking_reasons,
        "expected_row_changes": plan.expected_row_changes,
        "post_state_invariants": plan.post_state_invariants,
    }


def _external(connection, args, environment_identity, schema):
    if schema["status"] != SchemaContractStatus.PRESENT_VALID.value:
        raise RuntimeError("SCHEMA_NOT_READY")
    if args.evidence_json:
        envelope = ExternalEvidenceFileAdapter().read(
            args.evidence_json, source=args.source,
            trade_id=args.trade_id, order_id=args.order_id,
        )
    else:
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
        "semantic_fingerprint": (
            payload.get("source_fingerprint")
            or payload.get("semantic_fingerprint")
        ),
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


def _raw_inventory(state):
    fills = (state or {}).get("fills") or []
    entries = [row for row in fills if str(row.get("side")).upper() == "BUY"]
    exits = [row for row in fills if str(row.get("side")).upper() == "SELL"]
    gross_entry = sum(
        (Decimal(str(row.get("executed_qty") or 0)) for row in entries),
        Decimal("0"),
    )
    entry_base_fee = sum(
        (
            Decimal(str(row.get("commission_amount") or 0))
            for row in entries
            if str(row.get("commission_asset") or "").upper()
            and str(row.get("commission_asset") or "").upper()
            == str(row.get("symbol") or "")[:-4].upper()
        ),
        Decimal("0"),
    )
    gross_exit = sum(
        (Decimal(str(row.get("executed_qty") or 0)) for row in exits),
        Decimal("0"),
    )
    exit_base_fee = sum(
        (
            Decimal(str(row.get("commission_amount") or 0))
            for row in exits
            if str(row.get("commission_asset") or "").upper()
            == str(row.get("symbol") or "")[:-4].upper()
        ),
        Decimal("0"),
    )
    net_entry = gross_entry - entry_base_fee
    return gross_entry, net_entry, gross_exit, net_entry - gross_exit - exit_base_fee


def _cohort_item(connection, position_id: int, *, closed: bool):
    envelope = LegacyPositionEvidenceRepository().read(
        connection, position_id=position_id,
    )
    state = envelope.current_state or {}
    position = state.get("position") or {}
    gross_entry, net_entry, gross_exit, raw_remaining = _raw_inventory(state)
    result = (
        LegacyPositionRecomputationService().recompute(envelope.evidence)
        if envelope.evidence is not None else None
    )
    normalized = result.normalized_remaining_qty if result else raw_remaining
    ft = state.get("financial_truth") or []
    ft_status = ft[0].get("financial_truth_status") if ft else "ABSENT"
    reasons = list(envelope.missing_evidence + envelope.conflicting_evidence)
    if closed:
        if envelope.conflicting_evidence:
            planner_status = "INVENTORY_CONFLICT"
        elif envelope.evidence is not None and result.financial_truth_eligibility:
            planner_status = "READY_FOR_FINANCIAL_TRUTH"
        elif "FEE_EVIDENCE" in reasons:
            planner_status = "MISSING_FEE"
        elif any(reason.endswith("FILLS") for reason in reasons):
            planner_status = "MISSING_FILL"
        elif "ACCOUNT_PROVENANCE" in reasons:
            planner_status = "MISSING_PROVENANCE"
        else:
            planner_status = "UNRESOLVED"
    else:
        if envelope.conflicting_evidence:
            planner_status = "CONFLICT"
        elif result and result.normalized_remaining_qty == 0:
            planner_status = (
                "DUST_WITHIN_PRECISION"
                if result.raw_remaining_qty != 0 else "PHANTOM_OPEN"
            )
        elif result and result.normalized_remaining_qty > 0:
            planner_status = "REAL_OPEN_POSITION"
        elif gross_exit > 0 and raw_remaining > 0:
            planner_status = "PARTIALLY_EXITED"
        elif "EXIT_FILLS" in reasons:
            planner_status = "MISSING_EXIT_EVIDENCE"
        else:
            planner_status = "CONFLICT"
    semantic = semantic_repair_fingerprint({
        "position_id": position_id,
        "status": position.get("status"),
        "gross_entry_qty": gross_entry,
        "net_entry_inventory": net_entry,
        "gross_exit_qty": gross_exit,
        "raw_remaining_qty": raw_remaining,
        "financial_truth_status": ft_status,
        "blocking_reasons": sorted(reasons),
    })
    return {
        "position_id": position_id,
        "symbol": position.get("symbol"),
        "interval": position.get("interval"),
        "strategy": position.get("strategy"),
        "lifecycle_status": position.get("status"),
        "financial_truth_status": ft_status,
        "gross_entry_qty": gross_entry,
        "net_entry_inventory": net_entry,
        "gross_exit_qty": gross_exit,
        "raw_remaining_qty": raw_remaining,
        "normalized_remaining_qty": normalized,
        "fee_completeness": "INCOMPLETE" if "FEE_EVIDENCE" in reasons else "COMPLETE",
        "provenance_completeness": (
            "INCOMPLETE" if "ACCOUNT_PROVENANCE" in reasons else "COMPLETE"
        ),
        "planner_status": planner_status,
        "blocking_reasons": reasons,
        "recommended_next_action": (
            "REVIEW_FOR_REPAIR" if reasons else "PLAN_ONLY"
        ),
        "semantic_fingerprint": semantic,
    }


def _cohort(connection, args, environment_identity, schema, *, closed: bool):
    if schema["status"] != SchemaContractStatus.PRESENT_VALID.value:
        raise RuntimeError("SCHEMA_NOT_READY")
    if not 1 <= args.limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    with connection.cursor() as cur:
        if closed:
            cur.execute(
                """
                SELECT p.id
                FROM positions p
                LEFT JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
                WHERE p.status='CLOSED'
                  AND COALESCE(ft.financial_truth_status,'ABSENT') <> 'COMPLETE'
                ORDER BY p.exit_time DESC NULLS LAST,p.id DESC
                LIMIT %s
                """,
                (args.limit,),
            )
        else:
            cur.execute(
                "SELECT id FROM positions WHERE status='OPEN' "
                "ORDER BY entry_time,id LIMIT %s",
                (args.limit,),
            )
        ids = [int(row[0]) for row in cur.fetchall()]
    items = [
        _cohort_item(connection, position_id, closed=closed)
        for position_id in ids
    ]
    statuses = [item["planner_status"] for item in items]
    summary = {
        "total": len(items),
        "ready": sum(status == "READY_FOR_FINANCIAL_TRUTH" for status in statuses),
        "incomplete": sum(status.startswith("MISSING_") or status == "UNRESOLVED" for status in statuses),
        "conflict": sum("CONFLICT" in status for status in statuses),
        "phantom_open": statuses.count("PHANTOM_OPEN"),
        "real_open": statuses.count("REAL_OPEN_POSITION"),
        "dust": statuses.count("DUST_WITHIN_PRECISION"),
        "missing_financial_truth": sum(
            item["financial_truth_status"] != "COMPLETE" for item in items
        ),
        "missing_fills": statuses.count("MISSING_FILL"),
        "missing_provenance": statuses.count("MISSING_PROVENANCE"),
    }
    return {
        "schema_status": schema["status"],
        "environment_identity": environment_identity,
        "command": (
            "audit-unresolved-closed" if closed else "audit-open-cohort"
        ),
        "limit": args.limit,
        "items": items,
        "summary": summary,
    }


def main(argv=None) -> int:
    args = parser().parse_args(
        _normalize_global_options(sys.argv[1:] if argv is None else argv)
    )
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
            elif args.command == "audit-open-cohort":
                result = _cohort(
                    connection, args, identity, schema, closed=False,
                )
            elif args.command == "audit-unresolved-closed":
                result = _cohort(
                    connection, args, identity, schema, closed=True,
                )
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

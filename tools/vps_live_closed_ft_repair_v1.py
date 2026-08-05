#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import psycopg2

from common.financial_truth_calculator import (
    FillEvidence,
    calculate_financial_truth,
)
from common.inventory_quantity import (
    ExitInventoryStatus,
    InstrumentExecutionLimits,
    InventoryEvidenceStatus,
    classify_exit_inventory,
    project_inventory_from_execution_evidence,
)
from common.legacy_recovery import (
    semantic_repair_fingerprint,
    value_fee,
)


CONTRACT_VERSION = "VPS_LIVE_CLOSED_FT_REPAIR_V1"
PLANNER_VERSION = CONTRACT_VERSION + "_PLANNER"
ENVIRONMENT = "LIVE"
DEPLOYMENT_ID = "vps-live"
DATABASE = "trading_live"
POSITION_IDS = (3053, 3054, 3056, 3057, 3058, 3070, 3071, 3072, 3073, 3079, 3080, 3081)

EXPECTED_EVIDENCE_PROVENANCE_EXCLUSION_IDS = frozenset((3053, 3054, 3056, 3058, 3070, 3071, 3072, 3073, 3079, 3080, 3081))
EXPECTED_INVENTORY_MISMATCH_EXCLUSION_IDS = frozenset((3057,))
EXPECTED_EXISTING_EXCLUSION_IDS = (
    EXPECTED_EVIDENCE_PROVENANCE_EXCLUSION_IDS
    | EXPECTED_INVENTORY_MISMATCH_EXCLUSION_IDS
)

EXPECTED_MUTATIONS = (
    "canonical_financial_truth_v1:INSERT",
    "canonical_financial_truth_audit_v1:INSERT",
    "legacy_repair_audit_v1:INSERT",
    "legacy_repair_provenance_v1:INSERT",
)

FORBIDDEN_MUTATIONS = (
    "positions",
    "binance_orders",
    "binance_order_fills",
    "position_lifecycle_events_c2_2",
    "decision_outcomes_v1",
    "decision_replay_v1",
    "learning_feature_warehouse_v1",
    "exit_trace_v1",
    "exit_trace_v2",
    "exit_trace_v3",
)


def decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return "0" if value == 0 else format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): json_safe(item)
            for key, item in sorted(
                value.items(),
                key=lambda pair: str(pair[0]),
            )
        }
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def rows(cur) -> list[dict[str, Any]]:
    names = [column[0] for column in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def json_rows(cur, table: str, position_id: int, order_by: str) -> list[Any]:
    cur.execute(
        f"""
        SELECT row_to_json(t)
        FROM {table} t
        WHERE t.position_id=%s
        ORDER BY {order_by}
        """,
        (position_id,),
    )
    return [row[0] for row in cur.fetchall()]


def read_identity(cur) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
          id,
          exchange,
          account_scope,
          identity_source,
          identity_version,
          identity_fingerprint,
          captured_at
        FROM financial_truth_account_identity_v1
        WHERE exchange='OKX'
          AND source_authority='EXCHANGE_EXECUTION'
        ORDER BY captured_at DESC, id DESC
        """
    )
    identities = rows(cur)
    if len(identities) != 1:
        raise RuntimeError(
            f"ACCOUNT_IDENTITY_NOT_EXACT:{len(identities)}"
        )
    return identities[0]


def read_instrument(cur, symbol: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
          id,
          exchange,
          symbol,
          base_asset,
          quote_asset,
          step_size,
          min_qty,
          min_notional,
          quantity_precision,
          price_precision,
          metadata_fingerprint,
          captured_at
        FROM financial_truth_instrument_snapshot_v1
        WHERE exchange='OKX'
          AND symbol=%s
          AND source_authority='EXCHANGE_EXECUTION'
        ORDER BY captured_at DESC, id DESC
        LIMIT 1
        """,
        (symbol,),
    )
    instrument = cur.fetchone()
    if instrument is None:
        raise RuntimeError(
            f"INSTRUMENT_SNAPSHOT_MISSING:{symbol}"
        )
    names = [column[0] for column in cur.description]
    return dict(zip(names, instrument))


def read_position(cur, position_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
          id,
          symbol,
          strategy,
          interval,
          status,
          side,
          qty,
          entry_price,
          exit_price,
          entry_time,
          exit_time,
          exit_reason,
          entry_order_id,
          exit_order_id,
          gross_pnl_usdc,
          fees_usdc,
          net_pnl_usdc,
          inventory_evidence_status,
          gross_entry_executed_qty,
          entry_base_fee_qty,
          net_entry_inventory_qty,
          cumulative_exit_executed_qty,
          exit_inventory_reduction_qty,
          remaining_inventory_qty,
          terminal_dust_qty,
          terminal_reason
        FROM positions
        WHERE id=%s
        """,
        (position_id,),
    )
    result = rows(cur)
    if len(result) != 1:
        raise RuntimeError(
            f"POSITION_NOT_EXACT:{position_id}:{len(result)}"
        )
    position = result[0]
    if position["status"] != "CLOSED":
        raise RuntimeError(
            f"POSITION_NOT_CLOSED:{position_id}"
        )
    if not position["entry_order_id"] or not position["exit_order_id"]:
        raise RuntimeError(
            f"ORDER_ID_MISSING:{position_id}"
        )
    return position


def read_orders(
    cur,
    entry_order_id: str,
    exit_order_id: str,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          id,
          created_at,
          symbol,
          side,
          order_type,
          client_order_id,
          order_id,
          status,
          position_id,
          is_exit,
          strategy,
          interval,
          order_purpose,
          requested_qty,
          order_accepted,
          exchange_source,
          reconciliation_status,
          reconciled_position_id,
          reconciled_at,
          reconciled_fill_count,
          reconciled_executed_qty,
          unreconciled_qty,
          last_reconciliation_action
        FROM binance_orders
        WHERE order_id=ANY(%s)
        ORDER BY order_id,id
        """,
        ([entry_order_id, exit_order_id],),
    )
    return rows(cur)


def read_fills(
    cur,
    entry_order_id: str,
    exit_order_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cur.execute(
        """
        SELECT
          id,
          source,
          order_id,
          symbol,
          side,
          role,
          executed_qty,
          avg_price,
          quote_notional_usdc,
          commission_amount,
          commission_asset,
          commission_usdc,
          event_time,
          trade_id
        FROM binance_order_fills
        WHERE order_id=ANY(%s)
        ORDER BY event_time,id
        """,
        ([entry_order_id, exit_order_id],),
    )
    all_fills = rows(cur)

    entry_fills = [
        row for row in all_fills
        if str(row["order_id"]) == entry_order_id
    ]
    exit_fills = [
        row for row in all_fills
        if str(row["order_id"]) == exit_order_id
    ]

    if not entry_fills:
        raise RuntimeError("MISSING_ENTRY_FILLS")
    if not exit_fills:
        raise RuntimeError("MISSING_EXIT_FILLS")

    for row in all_fills:
        required = (
            "executed_qty",
            "avg_price",
            "quote_notional_usdc",
            "commission_amount",
            "commission_asset",
            "commission_usdc",
            "event_time",
            "trade_id",
        )
        missing = [
            key for key in required
            if row.get(key) is None
        ]
        if missing:
            raise RuntimeError(
                "INCOMPLETE_FILL_ECONOMICS:"
                + str(row["id"])
                + ":"
                + ",".join(missing)
            )

        calculated_notional = (
            decimal(row["executed_qty"])
            * decimal(row["avg_price"])
        )
        if decimal(row["quote_notional_usdc"]) != calculated_notional:
            raise RuntimeError(
                f"FILL_NOTIONAL_CONFLICT:{row['id']}"
            )

    return entry_fills, exit_fills


def read_existing_state(cur, position_id: int) -> dict[str, Any]:
    counts = {}

    count_specs = {
        "ft_rows": (
            "canonical_financial_truth_v1",
            "position_id=%s",
            position_id,
        ),
        "ft_audit_rows": (
            "canonical_financial_truth_audit_v1",
            "position_id=%s",
            position_id,
        ),
        "repair_audit_rows": (
            "legacy_repair_audit_v1",
            "incident_type='LEGACY_POSITION' "
            "AND incident_identity=%s",
            str(position_id),
        ),
        "provenance_rows": (
            "legacy_repair_provenance_v1",
            "evidence_source='LEGACY_POSITION_REPAIR' "
            "AND source_identity=%s",
            (
                f"LIVE:{DEPLOYMENT_ID}:{DATABASE}:"
                f"position:{position_id}"
            ),
        ),
    }

    for name, (table, predicate, value) in count_specs.items():
        cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {predicate}",
            (value,),
        )
        counts[name] = int(cur.fetchone()[0])

    if any(counts.values()):
        raise RuntimeError(
            f"EXISTING_REPAIR_ARTIFACT_CONFLICT:"
            f"{position_id}:{counts}"
        )

    cur.execute(
        """
        SELECT row_to_json(x)
        FROM learning_outcome_exclusion_v1 x
        WHERE environment='LIVE'
          AND deployment_id='vps-live'
          AND position_id=%s
        """,
        (position_id,),
    )
    exclusions = [row[0] for row in cur.fetchall()]

    expected_existing = position_id in EXPECTED_EXISTING_EXCLUSION_IDS
    if expected_existing and len(exclusions) != 1:
        raise RuntimeError(
            f"EXPECTED_EXISTING_EXCLUSION_MISSING:{position_id}"
        )
    if not expected_existing and exclusions:
        raise RuntimeError(
            f"UNEXPECTED_EXISTING_EXCLUSION:{position_id}"
        )

    if exclusions:
        exclusion = exclusions[0]

        if position_id in EXPECTED_EVIDENCE_PROVENANCE_EXCLUSION_IDS:
            expected_contract = (
                "EVIDENCE_PROVENANCE_INCOMPLETE",
                "FINANCIAL_TRUTH_CONTAINMENT",
                "VPS_LIVE_RECENT_UNRESOLVED_FT_AUDIT_20260804",
            )
        elif position_id in EXPECTED_INVENTORY_MISMATCH_EXCLUSION_IDS:
            expected_contract = (
                "INVENTORY_ACCOUNT_MISMATCH",
                "INVENTORY_OWNERSHIP_AUDIT",
                "VPS_LIVE_RECENT_UNRESOLVED_FT_AUDIT_20260804",
            )
        else:
            raise RuntimeError(
                f"EXCLUSION_POSITION_CLASSIFICATION_MISSING:{position_id}"
            )

        actual_contract = (
            exclusion["exclusion_reason"],
            exclusion["source_type"],
            exclusion["source_reference"],
        )

        if actual_contract != expected_contract:
            raise RuntimeError(
                f"EXISTING_EXCLUSION_CONTRACT_CONFLICT:"
                f"{position_id}:{actual_contract}:{expected_contract}"
            )

        exclusion_action = "KEEP_EXISTING"
    else:
        exclusion = None
        exclusion_action = "INSERT"

    return {
        **counts,
        "exclusion": exclusion,
        "exclusion_action": exclusion_action,
    }


def read_learning_snapshot(cur, position_id: int) -> dict[str, Any]:
    snapshot = {
        "exit_trace_v1": json_rows(
            cur, "exit_trace_v1", position_id, "position_id"
        ),
        "exit_trace_v2": json_rows(
            cur, "exit_trace_v2", position_id, "position_id"
        ),
        "exit_trace_v3": json_rows(
            cur, "exit_trace_v3", position_id, "position_id"
        ),
        "decision_replay_v1": json_rows(
            cur, "decision_replay_v1", position_id, "decision_key"
        ),
        "learning_feature_warehouse_v1": json_rows(
            cur,
            "learning_feature_warehouse_v1",
            position_id,
            "to_jsonb(t)::text",
        ),
        "decision_registry_v1": json_rows(
            cur, "decision_registry_v1", position_id, "decision_id"
        ),
        "decision_outcomes_v1": json_rows(
            cur, "decision_outcomes_v1", position_id, "outcome_id"
        ),
    }

    expected_counts = {
        "exit_trace_v1": 1,
        "exit_trace_v2": 1,
        "exit_trace_v3": 1,
        "decision_replay_v1": 1,
        "decision_registry_v1": 1,
        "decision_outcomes_v1": 1,
    }

    for table, expected in expected_counts.items():
        actual = len(snapshot[table])
        if actual != expected:
            raise RuntimeError(
                f"LEARNING_ARTIFACT_COUNT_CONFLICT:"
                f"{position_id}:{table}:{actual}:{expected}"
            )

    if not snapshot["learning_feature_warehouse_v1"]:
        raise RuntimeError(
            f"FEATURE_WAREHOUSE_MISSING:{position_id}"
        )

    return snapshot


def assert_learning_readers_empty(cur, position_id: int) -> None:
    checks = (
        ("v_learning_eligible_closed_positions_v1", "id"),
        ("v_learning_eligible_exit_trace_v1", "position_id"),
        ("v_learning_eligible_exit_trace_v2", "position_id"),
        ("v_learning_eligible_exit_trace_v3", "position_id"),
        ("v_learning_eligible_feature_warehouse_v1", "position_id"),
        ("v_learning_eligible_decision_replay_v1", "position_id"),
        ("v_learning_eligible_decision_registry_v1", "position_id"),
        ("v_learning_eligible_decision_outcomes_v1", "position_id"),
    )

    for view, column in checks:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {view}
            WHERE {column}=%s
            """,
            (position_id,),
        )
        count = int(cur.fetchone()[0])
        if count != 0:
            raise RuntimeError(
                f"LEARNING_READER_NOT_EMPTY:"
                f"{position_id}:{view}:{count}"
            )


def calculate_plan(
    cur,
    *,
    position: Mapping[str, Any],
    identity: Mapping[str, Any],
    instrument: Mapping[str, Any],
    entry_fills: list[Mapping[str, Any]],
    exit_fills: list[Mapping[str, Any]],
):
    symbol = str(position["symbol"])
    base_asset = str(instrument["base_asset"]).upper()
    quote_asset = str(instrument["quote_asset"]).upper()
    lot_size = decimal(instrument["step_size"])
    min_size = decimal(instrument["min_qty"])
    min_notional = decimal(instrument["min_notional"] or 0)

    inventory = project_inventory_from_execution_evidence(
        symbol=symbol,
        entry_fills=entry_fills,
        exit_fills=exit_fills,
    )

    if inventory.evidence_status is not InventoryEvidenceStatus.COMPLETE:
        raise RuntimeError(
            "INVENTORY_EVIDENCE_INCOMPLETE:"
            + ",".join(inventory.incomplete_reasons)
        )

    limits = InstrumentExecutionLimits(
        lot_size,
        min_size,
        min_notional,
        None,
        min_notional == 0,
    )

    classification = classify_exit_inventory(
        previous_remaining_qty=inventory.net_entry_inventory_qty,
        cumulative_exit_inventory_reduction_qty=(
            inventory.exit_inventory_reduction_qty
        ),
        previous_cumulative_exit_inventory_reduction_qty=Decimal("0"),
        inventory=inventory,
        limits=limits,
        tolerance=lot_size,
    )

    if classification.status not in {
        ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
        ExitInventoryStatus.TERMINAL_DUST_CLOSE,
    }:
        raise RuntimeError(
            f"TERMINAL_CLASSIFICATION_REQUIRED:"
            f"{position['id']}:{classification.status.value}"
        )

    canonical_fills: list[FillEvidence] = []

    for purpose, fill_rows in (
        ("ENTRY", entry_fills),
        ("EXIT", exit_fills),
    ):
        for row in fill_rows:
            fee = value_fee(
                quantity=decimal(row["commission_amount"]),
                asset=str(row["commission_asset"]),
                base_asset=base_asset,
                quote_asset=quote_asset,
                fill_price=decimal(row["avg_price"]),
            )
            if fee.valued_fee_usdc is None:
                raise RuntimeError(
                    f"CANONICAL_FEE_VALUATION_INCOMPLETE:"
                    f"{row['id']}"
                )

            canonical_fills.append(
                FillEvidence(
                    fill_id=f"exchange:{row['id']}",
                    order_id=str(row["order_id"]),
                    position_id=int(position["id"]),
                    purpose=purpose,
                    side=str(row["side"]).upper(),
                    symbol=str(row["symbol"]),
                    quantity=decimal(row["executed_qty"]),
                    price=decimal(row["avg_price"]),
                    notional=decimal(row["quote_notional_usdc"]),
                    fee_quantity=decimal(row["commission_amount"]),
                    fee_asset=str(row["commission_asset"]).upper(),
                    authoritative_fee_usdc=fee.valued_fee_usdc,
                    estimated_fee_usdc=None,
                    event_time=row["event_time"],
                    source_authority="EXCHANGE_EXECUTION",
                    source_exchange="okx",
                    source_environment="live",
                    source_deployment_id=DEPLOYMENT_ID,
                    account_identity_fingerprint=str(
                        identity["identity_fingerprint"]
                    ),
                    instrument_metadata_fingerprint=str(
                        instrument["metadata_fingerprint"]
                    ),
                    step_size=lot_size,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    source_version=CONTRACT_VERSION,
                )
            )

    financial_truth = calculate_financial_truth(
        position_id=int(position["id"]),
        position_status="CLOSED",
        fills=tuple(canonical_fills),
        position_symbol=symbol,
        inventory_classification=classification,
    )

    if financial_truth.financial_truth_status != "COMPLETE":
        raise RuntimeError(
            f"FINANCIAL_TRUTH_NOT_COMPLETE:"
            f"{position['id']}:"
            f"{financial_truth.failure_detail or financial_truth.failure_code}"
        )

    return inventory, classification, financial_truth


def build_plan(connection, git_sha: str) -> dict[str, Any]:
    connection.rollback()
    connection.set_session(readonly=True, autocommit=False)

    with connection.cursor() as cur:
        cur.execute("SELECT current_database()")
        database = str(cur.fetchone()[0])
        if database != DATABASE:
            raise RuntimeError(
                f"DATABASE_IDENTITY_MISMATCH:{database}"
            )

        identity = read_identity(cur)
        plan_rows = []

        for position_id in POSITION_IDS:
            position = read_position(cur, position_id)
            instrument = read_instrument(
                cur, str(position["symbol"])
            )

            entry_order_id = str(position["entry_order_id"])
            exit_order_id = str(position["exit_order_id"])

            orders = read_orders(
                cur,
                entry_order_id,
                exit_order_id,
            )
            entry_fills, exit_fills = read_fills(
                cur,
                entry_order_id,
                exit_order_id,
            )

            existing = read_existing_state(cur, position_id)
            learning_snapshot = read_learning_snapshot(
                cur, position_id
            )
            assert_learning_readers_empty(cur, position_id)

            inventory, classification, financial_truth = (
                calculate_plan(
                    cur,
                    position=position,
                    identity=identity,
                    instrument=instrument,
                    entry_fills=entry_fills,
                    exit_fills=exit_fills,
                )
            )

            fingerprint_payload = json_safe({
                "contract_version": CONTRACT_VERSION,
                "planner_version": PLANNER_VERSION,
                "environment": ENVIRONMENT,
                "deployment_id": DEPLOYMENT_ID,
                "database": DATABASE,
                "git_sha": git_sha,
                "position": position,
                "orders": orders,
                "entry_fills": entry_fills,
                "exit_fills": exit_fills,
                "identity": identity,
                "instrument": instrument,
                "inventory": asdict(inventory),
                "classification": asdict(classification),
                "financial_truth": financial_truth.semantic_values(),
                "existing_state": existing,
                "learning_snapshot": learning_snapshot,
                "expected_mutations": EXPECTED_MUTATIONS,
                "forbidden_mutations": FORBIDDEN_MUTATIONS,
            })

            plan_fingerprint = semantic_repair_fingerprint(
                fingerprint_payload
            )

            plan_rows.append({
                "position_id": position_id,
                "symbol": position["symbol"],
                "strategy": position["strategy"],
                "interval": position["interval"],
                "entry_order_id": entry_order_id,
                "exit_order_id": exit_order_id,
                "entry_fill_count": len(entry_fills),
                "exit_fill_count": len(exit_fills),
                "gross_entry_qty": inventory.gross_entry_executed_qty,
                "entry_base_fee_qty": inventory.entry_base_fee_qty,
                "net_entry_inventory_qty": (
                    inventory.net_entry_inventory_qty
                ),
                "gross_exit_qty": (
                    inventory.cumulative_exit_executed_qty
                ),
                "remaining_inventory_qty": (
                    classification.remaining_inventory_qty
                ),
                "classification": classification.status.value,
                "authoritative_gross_pnl": (
                    financial_truth.authoritative_gross_pnl
                ),
                "authoritative_fees_usdc": (
                    financial_truth.authoritative_fees_usdc
                ),
                "authoritative_net_pnl": (
                    financial_truth.authoritative_net_pnl
                ),
                "source_fingerprint": (
                    financial_truth.source_fingerprint
                ),
                "plan_fingerprint": plan_fingerprint,
                "exclusion_action": existing["exclusion_action"],
                "existing_exclusion_id": (
                    existing["exclusion"]["exclusion_id"]
                    if existing["exclusion"] else None
                ),
                "learning_artifact_counts": {
                    key: len(value)
                    for key, value in learning_snapshot.items()
                },
                "expected_mutations": EXPECTED_MUTATIONS,
                "forbidden_mutations": FORBIDDEN_MUTATIONS,
                "fingerprint_payload": fingerprint_payload,
            })

        connection.rollback()

    summary = {
        "positions_planned": len(plan_rows),
        "ft_complete": sum(
            1 for row in plan_rows
            if row["authoritative_net_pnl"] is not None
        ),
        "fully_executed_close": sum(
            1 for row in plan_rows
            if row["classification"] == "FULLY_EXECUTED_CLOSE"
        ),
        "terminal_dust_close": sum(
            1 for row in plan_rows
            if row["classification"] == "TERMINAL_DUST_CLOSE"
        ),
        "exclusions_to_insert": sum(
            1 for row in plan_rows
            if row["exclusion_action"] == "INSERT"
        ),
        "existing_exclusions_to_keep": sum(
            1 for row in plan_rows
            if row["exclusion_action"] == "KEEP_EXISTING"
        ),
        "db_writes": 0,
        "okx_calls": 0,
    }

    if summary["positions_planned"] != 12:
        raise RuntimeError("PLAN_POSITION_COUNT_INVALID")
    if summary["ft_complete"] != 12:
        raise RuntimeError("PLAN_FT_COMPLETE_COUNT_INVALID")
    if summary["exclusions_to_insert"] != 0:
        raise RuntimeError("PLAN_EXCLUSION_INSERT_COUNT_INVALID")
    if summary["existing_exclusions_to_keep"] != 12:
        raise RuntimeError("PLAN_EXISTING_EXCLUSION_COUNT_INVALID")

    return json_safe({
        "contract_version": CONTRACT_VERSION,
        "mode": "PLAN",
        "generated_at": datetime.now(timezone.utc),
        "generated_from_git_revision": git_sha,
        "environment": ENVIRONMENT,
        "deployment_id": DEPLOYMENT_ID,
        "database": DATABASE,
        "summary": summary,
        "positions": plan_rows,
    })


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "PLAN-only VPS LIVE CLOSED Financial Truth repair V1"
        )
    )
    parser.add_argument(
        "--output",
        default="/tmp/vps_live_closed_ft_repair_v1_plan.json",
    )
    parser.add_argument(
        "--git-sha",
        required=True,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Not implemented by design",
    )
    args = parser.parse_args()

    if args.apply:
        raise RuntimeError(
            "APPLY_NOT_IMPLEMENTED_PLAN_ONLY"
        )

    password = (
        os.environ.get("DB_PASSWORD")
        or os.environ.get("POSTGRES_PASSWORD")
        or os.environ.get("PGPASSWORD")
    )
    if not password:
        raise RuntimeError("DB_PASSWORD_REQUIRED")

    connection = psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", DATABASE),
        user=os.environ.get("DB_USER", "botuser"),
        password=password,
    )

    try:
        plan = build_plan(connection, args.git_sha)
    finally:
        connection.close()

    output = Path(args.output)
    output.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n"
    )

    print(json.dumps({
        "contract_version": plan["contract_version"],
        "mode": plan["mode"],
        "generated_from_git_revision": (
            plan["generated_from_git_revision"]
        ),
        "summary": plan["summary"],
        "output": str(output),
    }, indent=2, sort_keys=True))

    for row in plan["positions"]:
        print(
            "PLAN_POSITION"
            f" id={row['position_id']}"
            f" symbol={row['symbol']}"
            f" slot={row['strategy']}:{row['interval']}"
            f" classification={row['classification']}"
            f" remaining={row['remaining_inventory_qty']}"
            f" gross={row['authoritative_gross_pnl']}"
            f" fees={row['authoritative_fees_usdc']}"
            f" net={row['authoritative_net_pnl']}"
            f" exclusion={row['exclusion_action']}"
            f" fingerprint={row['plan_fingerprint']}"
        )

    print("VPS_LIVE_CLOSED_FT_REPAIR_PLAN_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2

from common.financial_truth_repository import (
    CanonicalFinancialTruthWriteRepository,
)
from common.legacy_recovery_repository import (
    LegacyProvenanceRepository,
    LegacyRepairAuditRepository,
)
from common.legacy_repair_quarantine import (
    LearningOutcomeExclusionRepository,
)

from tools.local_live_closed_ft_repair_v1 import (
    CONTRACT_VERSION,
    DATABASE,
    DEPLOYMENT_ID,
    ENVIRONMENT,
    EXPECTED_MUTATIONS,
    FORBIDDEN_MUTATIONS,
    PLANNER_VERSION,
    POSITION_IDS,
    assert_learning_readers_empty,
    calculate_plan,
    json_safe,
    read_existing_state,
    read_fills,
    read_identity,
    read_instrument,
    read_learning_snapshot,
    read_orders,
    read_position,
    semantic_repair_fingerprint,
)


WRITER_VERSION = CONTRACT_VERSION + "_WRITER"
ROLLBACK_PROOF_VERSION = CONTRACT_VERSION + "_ROLLBACK_PROOF"

EXPECTED_EXISTING_EXCLUSION_ID = 3077


def stable_snapshot(
    cur,
    *,
    position: dict[str, Any],
    orders: list[dict[str, Any]],
    entry_fills: list[dict[str, Any]],
    exit_fills: list[dict[str, Any]],
    learning_snapshot: dict[str, Any],
) -> str:
    payload = json_safe({
        "position": position,
        "orders": orders,
        "entry_fills": entry_fills,
        "exit_fills": exit_fills,
        "learning_snapshot": learning_snapshot,
    })
    return semantic_repair_fingerprint(payload)


def current_exclusion(cur, position_id: int):
    return LearningOutcomeExclusionRepository.current(
        cur,
        environment=ENVIRONMENT,
        deployment_id=DEPLOYMENT_ID,
        position_id=position_id,
    )


def verify_saved_plan(
    saved_plan: dict[str, Any],
    *,
    git_sha: str,
) -> dict[int, dict[str, Any]]:
    if saved_plan.get("contract_version") != CONTRACT_VERSION:
        raise RuntimeError("PLAN_CONTRACT_VERSION_MISMATCH")

    if saved_plan.get("mode") != "PLAN":
        raise RuntimeError("PLAN_MODE_INVALID")

    if saved_plan.get("generated_from_git_revision") != git_sha:
        raise RuntimeError("PLAN_GIT_SHA_MISMATCH")

    summary = saved_plan.get("summary") or {}

    required_summary = {
        "positions_planned": 13,
        "ft_complete": 13,
        "exclusions_to_insert": 12,
        "existing_exclusions_to_keep": 1,
        "db_writes": 0,
        "okx_calls": 0,
    }

    for key, expected in required_summary.items():
        actual = summary.get(key)
        if actual != expected:
            raise RuntimeError(
                f"PLAN_SUMMARY_MISMATCH:{key}:{actual}:{expected}"
            )

    rows = saved_plan.get("positions") or []

    if len(rows) != 13:
        raise RuntimeError(
            f"PLAN_ROW_COUNT_INVALID:{len(rows)}"
        )

    by_id = {
        int(row["position_id"]): row
        for row in rows
    }

    if tuple(sorted(by_id)) != POSITION_IDS:
        raise RuntimeError("PLAN_POSITION_SET_INVALID")

    return by_id


def insert_or_keep_exclusion(
    cur,
    *,
    position_id: int,
    plan_fingerprint: str,
    git_sha: str,
) -> tuple[int, str]:
    existing = current_exclusion(cur, position_id)

    if position_id == EXPECTED_EXISTING_EXCLUSION_ID:
        if existing is None:
            raise RuntimeError(
                "EXPECTED_3077_EXCLUSION_MISSING"
            )

        exclusion_id = int(existing[0])

        cur.execute(
            """
            SELECT
              exclusion_reason,
              source_type
            FROM learning_outcome_exclusion_v1
            WHERE exclusion_id=%s
            """,
            (exclusion_id,),
        )
        row = cur.fetchone()

        if row != (
            "LEGACY_REPAIR",
            "LEGACY_POSITION_REPAIR",
        ):
            raise RuntimeError(
                "EXPECTED_3077_EXCLUSION_CONTRACT_INVALID"
            )

        return exclusion_id, "KEEP_EXISTING"

    if existing is not None:
        raise RuntimeError(
            f"UNEXPECTED_EXISTING_EXCLUSION:{position_id}"
        )

    exclusion_id = LearningOutcomeExclusionRepository.insert(
        cur,
        environment=ENVIRONMENT,
        deployment_id=DEPLOYMENT_ID,
        position_id=position_id,
        semantic_fingerprint_v2=plan_fingerprint,
        git_sha=git_sha,
    )

    return exclusion_id, "INSERT"


def verify_ft_row(cur, position_id: int, source_fingerprint: str):
    cur.execute(
        """
        SELECT
          financial_truth_status,
          source_fingerprint,
          writer_version,
          source_environment,
          source_deployment_id
        FROM canonical_financial_truth_v1
        WHERE position_id=%s
        """,
        (position_id,),
    )
    row = cur.fetchone()

    if row is None:
        raise RuntimeError(
            f"FT_ROW_MISSING:{position_id}"
        )

    if row[0] != "COMPLETE":
        raise RuntimeError(
            f"FT_NOT_COMPLETE:{position_id}:{row[0]}"
        )

    if row[1] != source_fingerprint:
        raise RuntimeError(
            f"FT_FINGERPRINT_MISMATCH:{position_id}"
        )

    if row[3] != "live":
        raise RuntimeError(
            f"FT_ENVIRONMENT_MISMATCH:{position_id}:{row[3]}"
        )

    if row[4] != DEPLOYMENT_ID:
        raise RuntimeError(
            f"FT_DEPLOYMENT_MISMATCH:{position_id}:{row[4]}"
        )

    return row


def verify_artifact_counts(cur):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM canonical_financial_truth_v1
        WHERE position_id BETWEEN 3066 AND 3078
        """
    )
    ft_rows = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM canonical_financial_truth_audit_v1
        WHERE position_id BETWEEN 3066 AND 3078
        """
    )
    ft_audit_rows = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM learning_outcome_exclusion_v1
        WHERE environment='LIVE'
          AND deployment_id='local-live'
          AND position_id BETWEEN 3066 AND 3078
        """
    )
    exclusion_rows = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM legacy_repair_audit_v1
        WHERE incident_type='LEGACY_POSITION'
          AND incident_identity::bigint
              BETWEEN 3066 AND 3078
        """
    )
    repair_audit_rows = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM legacy_repair_provenance_v1
        WHERE evidence_source='LEGACY_POSITION_REPAIR'
          AND source_identity ~
            '^LIVE:local-live:trading_live:position:(306[6-9]|307[0-8])$'
        """
    )
    provenance_rows = int(cur.fetchone()[0])

    actual = {
        "ft_rows": ft_rows,
        "ft_audit_rows": ft_audit_rows,
        "exclusion_rows": exclusion_rows,
        "repair_audit_rows": repair_audit_rows,
        "provenance_rows": provenance_rows,
    }

    expected = {
        "ft_rows": 13,
        "ft_audit_rows": 13,
        "exclusion_rows": 13,
        "repair_audit_rows": 13,
        "provenance_rows": 13,
    }

    if actual != expected:
        raise RuntimeError(
            f"IN_TRANSACTION_ARTIFACT_COUNTS_INVALID:"
            f"{actual}:{expected}"
        )

    return actual


def rollback_proof(
    connection,
    *,
    saved_plan: dict[str, Any],
    git_sha: str,
) -> dict[str, Any]:
    saved_by_id = verify_saved_plan(
        saved_plan,
        git_sha=git_sha,
    )

    connection.rollback()
    connection.set_session(
        isolation_level="SERIALIZABLE",
        readonly=False,
        autocommit=False,
    )

    proof_rows = []

    try:
        with connection.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='5s'")
            cur.execute("SET LOCAL statement_timeout='90s'")

            cur.execute("SELECT current_database()")
            database = str(cur.fetchone()[0])

            if database != DATABASE:
                raise RuntimeError(
                    f"DATABASE_IDENTITY_MISMATCH:{database}"
                )

            identity = read_identity(cur)

            for position_id in POSITION_IDS:
                saved = saved_by_id[position_id]

                cur.execute(
                    """
                    SELECT id
                    FROM positions
                    WHERE id=%s
                    FOR UPDATE
                    """,
                    (position_id,),
                )

                if cur.fetchone() != (position_id,):
                    raise RuntimeError(
                        f"POSITION_LOCK_FAILED:{position_id}"
                    )

                position = read_position(cur, position_id)
                instrument = read_instrument(
                    cur,
                    str(position["symbol"]),
                )

                entry_order_id = str(
                    position["entry_order_id"]
                )
                exit_order_id = str(
                    position["exit_order_id"]
                )

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

                existing = read_existing_state(
                    cur,
                    position_id,
                )

                learning_snapshot = read_learning_snapshot(
                    cur,
                    position_id,
                )

                assert_learning_readers_empty(
                    cur,
                    position_id,
                )

                forbidden_before = stable_snapshot(
                    cur,
                    position=position,
                    orders=orders,
                    entry_fills=entry_fills,
                    exit_fills=exit_fills,
                    learning_snapshot=learning_snapshot,
                )

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
                    "inventory": inventory.__dict__,
                    "classification": classification.__dict__,
                    "financial_truth": (
                        financial_truth.semantic_values()
                    ),
                    "existing_state": existing,
                    "learning_snapshot": learning_snapshot,
                    "expected_mutations": EXPECTED_MUTATIONS,
                    "forbidden_mutations": FORBIDDEN_MUTATIONS,
                })

                live_plan_fingerprint = (
                    semantic_repair_fingerprint(
                        fingerprint_payload
                    )
                )

                if (
                    live_plan_fingerprint
                    != saved["plan_fingerprint"]
                ):
                    raise RuntimeError(
                        f"PLAN_STALE:{position_id}:"
                        f"{live_plan_fingerprint}:"
                        f"{saved['plan_fingerprint']}"
                    )

                if (
                    financial_truth.source_fingerprint
                    != saved["source_fingerprint"]
                ):
                    raise RuntimeError(
                        f"FT_SOURCE_FINGERPRINT_STALE:"
                        f"{position_id}"
                    )

                exclusion_id, exclusion_action = (
                    insert_or_keep_exclusion(
                        cur,
                        position_id=position_id,
                        plan_fingerprint=(
                            live_plan_fingerprint
                        ),
                        git_sha=git_sha,
                    )
                )

                assert_learning_readers_empty(
                    cur,
                    position_id,
                )

                invocation = (
                    f"LIVE:{DEPLOYMENT_ID}:"
                    f"{position_id}:"
                    f"{live_plan_fingerprint}"
                )

                written = (
                    CanonicalFinancialTruthWriteRepository.write(
                        cur,
                        financial_truth,
                        invocation_type=CONTRACT_VERSION,
                        invocation_identity=invocation,
                    )
                )

                if not written:
                    raise RuntimeError(
                        f"FT_WRITE_NOT_PERFORMED:{position_id}"
                    )

                verify_ft_row(
                    cur,
                    position_id,
                    financial_truth.source_fingerprint,
                )

                assert_learning_readers_empty(
                    cur,
                    position_id,
                )

                now = datetime.now(timezone.utc)

                audit_written = (
                    LegacyRepairAuditRepository.append(
                        cur,
                        {
                            "incident_type": "LEGACY_POSITION",
                            "incident_identity": str(position_id),
                            "operation_type": (
                                "LOCAL_LIVE_CLOSED_FT_ONLY_REPAIR"
                            ),
                            "planner_version": PLANNER_VERSION,
                            "writer_version": WRITER_VERSION,
                            "semantic_fingerprint_before": (
                                live_plan_fingerprint
                            ),
                            "semantic_fingerprint_expected": (
                                live_plan_fingerprint
                            ),
                            "semantic_fingerprint_after": (
                                live_plan_fingerprint
                            ),
                            "plan_status": "ELIGIBLE",
                            "execution_status": "APPLIED",
                            "invocation_identity": invocation,
                            "requested_at": now,
                            "started_at": now,
                            "completed_at": now,
                            "actor_source": (
                                "BOUNDED_FT_ONLY_REPAIR_SERVICE"
                            ),
                            "blocking_reasons": [],
                            "eligible_actions": list(
                                EXPECTED_MUTATIONS
                            ),
                            "executed_actions": list(
                                EXPECTED_MUTATIONS
                            ),
                            "expected_changes": list(
                                EXPECTED_MUTATIONS
                            ),
                            "actual_changes": list(
                                EXPECTED_MUTATIONS
                            ),
                            "post_state_invariants": [
                                "POSITION_REMAINS_CLOSED",
                                "FINANCIAL_TRUTH_COMPLETE",
                                "LEARNING_EXCLUDED",
                                "LEGACY_LEARNING_ARTIFACTS_UNCHANGED",
                                "ORDERS_UNCHANGED",
                                "FILLS_UNCHANGED",
                                "NO_EXCHANGE_MUTATION",
                            ],
                            "error_code": None,
                            "error_detail": None,
                        },
                    )
                )

                if not audit_written:
                    raise RuntimeError(
                        f"REPAIR_AUDIT_NOT_WRITTEN:"
                        f"{position_id}"
                    )

                source_identity = (
                    f"LIVE:{DEPLOYMENT_ID}:"
                    f"{DATABASE}:position:{position_id}"
                )

                provenance_written = (
                    LegacyProvenanceRepository.record(
                        cur,
                        {
                            "evidence_source": (
                                "LEGACY_POSITION_REPAIR"
                            ),
                            "source_identity": source_identity,
                            "source_fingerprint": (
                                live_plan_fingerprint
                            ),
                            "instrument_identity": str(
                                position["symbol"]
                            ),
                            "account_provenance": {
                                "account_identity_id": (
                                    identity["id"]
                                ),
                                "account_identity_fingerprint": (
                                    identity[
                                        "identity_fingerprint"
                                    ]
                                ),
                                "identity_source": (
                                    identity["identity_source"]
                                ),
                                "entry_fill_ids": [
                                    str(row["id"])
                                    for row in entry_fills
                                ],
                                "exit_fill_ids": [
                                    str(row["id"])
                                    for row in exit_fills
                                ],
                            },
                            "deployment_provenance": {
                                "environment": ENVIRONMENT,
                                "deployment_id": DEPLOYMENT_ID,
                                "database": DATABASE,
                                "git_sha": git_sha,
                                "contract_version": (
                                    CONTRACT_VERSION
                                ),
                                "rollback_proof_version": (
                                    ROLLBACK_PROOF_VERSION
                                ),
                            },
                            "fee_evidence": {
                                "entry_base_fee_qty": str(
                                    inventory.entry_base_fee_qty
                                ),
                                "authoritative_entry_fees_usdc": str(
                                    financial_truth
                                    .authoritative_entry_fees_usdc
                                ),
                                "authoritative_exit_fees_usdc": str(
                                    financial_truth
                                    .authoritative_exit_fees_usdc
                                ),
                                "authoritative_fees_usdc": str(
                                    financial_truth
                                    .authoritative_fees_usdc
                                ),
                            },
                            "valuation_evidence": {
                                "financial_truth_status": (
                                    financial_truth
                                    .financial_truth_status
                                ),
                                "authoritative_gross_pnl": str(
                                    financial_truth
                                    .authoritative_gross_pnl
                                ),
                                "authoritative_net_pnl": str(
                                    financial_truth
                                    .authoritative_net_pnl
                                ),
                                "remaining_inventory_qty": str(
                                    classification
                                    .remaining_inventory_qty
                                ),
                                "classification": (
                                    classification.status.value
                                ),
                            },
                            "immutable_payload": (
                                fingerprint_payload
                            ),
                            "observed_at": now,
                        },
                    )
                )

                if not provenance_written:
                    raise RuntimeError(
                        f"PROVENANCE_NOT_WRITTEN:"
                        f"{position_id}"
                    )

                position_after = read_position(
                    cur,
                    position_id,
                )
                orders_after = read_orders(
                    cur,
                    entry_order_id,
                    exit_order_id,
                )
                entry_fills_after, exit_fills_after = (
                    read_fills(
                        cur,
                        entry_order_id,
                        exit_order_id,
                    )
                )
                learning_after = read_learning_snapshot(
                    cur,
                    position_id,
                )

                forbidden_after = stable_snapshot(
                    cur,
                    position=position_after,
                    orders=orders_after,
                    entry_fills=entry_fills_after,
                    exit_fills=exit_fills_after,
                    learning_snapshot=learning_after,
                )

                if forbidden_after != forbidden_before:
                    raise RuntimeError(
                        f"FORBIDDEN_STATE_CHANGED:"
                        f"{position_id}"
                    )

                assert_learning_readers_empty(
                    cur,
                    position_id,
                )

                proof_rows.append({
                    "position_id": position_id,
                    "classification": (
                        classification.status.value
                    ),
                    "remaining_inventory_qty": str(
                        classification
                        .remaining_inventory_qty
                    ),
                    "authoritative_net_pnl": str(
                        financial_truth
                        .authoritative_net_pnl
                    ),
                    "plan_fingerprint": (
                        live_plan_fingerprint
                    ),
                    "source_fingerprint": (
                        financial_truth.source_fingerprint
                    ),
                    "exclusion_id": exclusion_id,
                    "exclusion_action": exclusion_action,
                    "ft_written": True,
                    "audit_written": True,
                    "provenance_written": True,
                    "learning_excluded": True,
                    "forbidden_state_unchanged": True,
                })

            counts = verify_artifact_counts(cur)

            print(
                "ROLLBACK_PROOF_IN_TRANSACTION_COUNTS="
                + json.dumps(
                    counts,
                    sort_keys=True,
                )
            )

            for row in proof_rows:
                print(
                    "ROLLBACK_PROOF_POSITION"
                    f" id={row['position_id']}"
                    f" classification="
                    f"{row['classification']}"
                    f" remaining="
                    f"{row['remaining_inventory_qty']}"
                    f" net="
                    f"{row['authoritative_net_pnl']}"
                    f" exclusion="
                    f"{row['exclusion_action']}"
                    f" fingerprint="
                    f"{row['plan_fingerprint']}"
                )

            connection.rollback()

            return {
                "positions_proved": len(proof_rows),
                "in_transaction_counts": counts,
                "transaction_committed": False,
                "db_persistent_writes": 0,
                "okx_calls": 0,
            }

    except Exception:
        connection.rollback()
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--plan",
        required=True,
    )
    parser.add_argument(
        "--git-sha",
        required=True,
    )
    args = parser.parse_args()

    plan_path = Path(args.plan)

    if not plan_path.is_file():
        raise RuntimeError("PLAN_FILE_MISSING")

    saved_plan = json.loads(plan_path.read_text())

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
        dbname=os.environ.get(
            "DB_NAME",
            DATABASE,
        ),
        user=os.environ.get(
            "DB_USER",
            "botuser",
        ),
        password=password,
    )

    try:
        result = rollback_proof(
            connection,
            saved_plan=saved_plan,
            git_sha=args.git_sha,
        )
    finally:
        connection.close()

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
        )
    )

    if result["positions_proved"] != 13:
        raise RuntimeError(
            "ROLLBACK_PROOF_POSITION_COUNT_INVALID"
        )

    if result["transaction_committed"]:
        raise RuntimeError(
            "ROLLBACK_PROOF_COMMIT_DETECTED"
        )

    print(
        "LOCAL_LIVE_CLOSED_FT_REPAIR_"
        "ROLLBACK_PROOF_PASS"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

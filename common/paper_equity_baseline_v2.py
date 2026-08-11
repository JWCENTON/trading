from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping


BASELINE_VERSION = "PAPER_EQUITY_BASELINE_V2"
SOURCE_AUTHORITY = "CANONICAL_PAPER_ACCOUNT_READ_MODEL_V1"
PAPER_DEPLOYMENTS = frozenset({"local-paper", "vps-paper"})


def _decimal(value: object) -> Decimal:
    return Decimal(str(value or 0))


@dataclass(frozen=True)
class PaperEquityBaselineV2:
    baseline_id: int
    deployment_id: str
    baseline_timestamp: datetime
    baseline_account_total: Decimal
    baseline_managed_equity: Decimal
    baseline_external_manual: Decimal
    baseline_available: Decimal
    baseline_inventory_value: Decimal
    baseline_realized_net_pnl: Decimal
    baseline_unrealized_pnl: Decimal
    baseline_fees: Decimal
    baseline_open_positions: int
    frozen_pre_baseline_unresolved_count: int
    evidence_status: str
    source_authority: str
    approved_by: str
    approval_provenance: Mapping[str, Any]
    activation_fingerprint: str
    created_at: datetime


@dataclass(frozen=True)
class BaselineActivationResult:
    baseline: PaperEquityBaselineV2 | None
    created: bool
    status: str


@dataclass(frozen=True)
class PostBaselinePaperEquity:
    account_total: Decimal
    external_manual: Decimal
    managed_equity: Decimal | None
    available: Decimal
    inventory_value: Decimal
    realized_net_pnl: Decimal | None
    unrealized_pnl: Decimal
    fees: Decimal | None
    evidence_status: str


_BASELINE_SELECT = """
SELECT baseline_id,deployment_id,baseline_timestamp,
       baseline_account_total,baseline_managed_equity,
       baseline_external_manual,baseline_available,
       baseline_inventory_value,baseline_realized_net_pnl,
       baseline_unrealized_pnl,baseline_fees,baseline_open_positions,
       frozen_pre_baseline_unresolved_count,evidence_status,
       source_authority,approved_by,approval_provenance,
       activation_fingerprint,created_at
FROM paper_equity_baseline_v2
WHERE deployment_id=%s AND baseline_version=%s
"""


def _baseline_from_row(row: tuple[Any, ...]) -> PaperEquityBaselineV2:
    return PaperEquityBaselineV2(
        baseline_id=int(row[0]), deployment_id=str(row[1]),
        baseline_timestamp=row[2], baseline_account_total=_decimal(row[3]),
        baseline_managed_equity=_decimal(row[4]),
        baseline_external_manual=_decimal(row[5]),
        baseline_available=_decimal(row[6]),
        baseline_inventory_value=_decimal(row[7]),
        baseline_realized_net_pnl=_decimal(row[8]),
        baseline_unrealized_pnl=_decimal(row[9]),
        baseline_fees=_decimal(row[10]), baseline_open_positions=int(row[11]),
        frozen_pre_baseline_unresolved_count=int(row[12]),
        evidence_status=str(row[13]), source_authority=str(row[14]),
        approved_by=str(row[15]), approval_provenance=dict(row[16] or {}),
        activation_fingerprint=str(row[17]), created_at=row[18],
    )


def fetch_paper_equity_baseline_v2(
    cur: Any, *, deployment_id: str
) -> PaperEquityBaselineV2 | None:
    deployment = str(deployment_id).strip().lower()
    if deployment not in PAPER_DEPLOYMENTS:
        return None
    cur.execute(_BASELINE_SELECT, (deployment, BASELINE_VERSION))
    row = cur.fetchone()
    return _baseline_from_row(row) if row else None


def calculate_post_baseline_paper_equity(
    baseline: PaperEquityBaselineV2,
    *, closed_count: int, resolved_count: int,
    realized_net_pnl: Decimal | None, fees: Decimal | None,
    current_unrealized_pnl: Decimal,
    current_inventory_value: Decimal,
) -> PostBaselinePaperEquity:
    if min(closed_count, resolved_count) < 0 or resolved_count > closed_count:
        raise ValueError("invalid post-baseline outcome coverage")
    realized_known = realized_net_pnl is not None or resolved_count == 0
    economic_delta = (
        _decimal(realized_net_pnl) + current_unrealized_pnl
        - baseline.baseline_unrealized_pnl
    )
    total = baseline.baseline_account_total + economic_delta
    managed_candidate = baseline.baseline_managed_equity + economic_delta
    complete = resolved_count == closed_count and realized_known
    return PostBaselinePaperEquity(
        account_total=total,
        external_manual=baseline.baseline_external_manual,
        managed_equity=managed_candidate if complete else None,
        available=total-current_inventory_value,
        inventory_value=current_inventory_value,
        realized_net_pnl=realized_net_pnl,
        unrealized_pnl=current_unrealized_pnl,
        fees=fees,
        evidence_status="COMPLETE" if complete else "INCOMPLETE",
    )


def activate_paper_equity_baseline_v2(
    cur: Any, *, deployment_id: str, observation: Any,
    unresolved_outcomes: Mapping[int, Mapping[str, Any]],
    approved_by: str, approval_provenance: Mapping[str, Any],
    trading_mode: str = "PAPER",
) -> BaselineActivationResult:
    mode = str(trading_mode).strip().upper()
    deployment = str(deployment_id).strip().lower()
    if mode != "PAPER":
        return BaselineActivationResult(None, False, "NOT_APPLICABLE")
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("PAPER_EQUITY_BASELINE_V2_DEPLOYMENT_INVALID")
    approver = str(approved_by).strip()
    provenance = dict(approval_provenance or {})
    if not approver or not provenance:
        raise ValueError("PAPER_EQUITY_BASELINE_V2_APPROVAL_REQUIRED")
    if observation.external_manual_value_usdc is None:
        raise ValueError("PAPER_EQUITY_BASELINE_V2_EXTERNAL_VALUE_REQUIRED")
    if observation.source_timestamp.tzinfo is None:
        raise ValueError("PAPER_EQUITY_BASELINE_V2_TIMESTAMP_REQUIRED")

    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"{BASELINE_VERSION}:{deployment}",),
    )
    existing = fetch_paper_equity_baseline_v2(cur, deployment_id=deployment)
    if existing is not None:
        return BaselineActivationResult(existing, False, "ALREADY_ACTIVE")

    frozen_payload = [
        {
            "position_id": int(position_id),
            "outcome_status": str(outcome.get("outcome_status") or "UNRESOLVED"),
            "evidence_status": str(outcome.get("evidence_status") or "INCOMPLETE"),
            "blocking_reasons": list(outcome.get("blocking_reasons") or []),
        }
        for position_id, outcome in sorted(unresolved_outcomes.items())
        if not bool(outcome.get("evidence_complete"))
    ]
    baseline_managed = (
        observation.account_total_value_usdc
        - observation.external_manual_value_usdc
    )
    fingerprint_payload = {
        "baseline_version": BASELINE_VERSION,
        "deployment_id": deployment,
        "baseline_timestamp": observation.source_timestamp.isoformat(),
        "baseline_account_total": str(observation.account_total_value_usdc),
        "baseline_managed_equity": str(baseline_managed),
        "baseline_external_manual": str(observation.external_manual_value_usdc),
        "baseline_available": str(observation.available_usdc),
        "baseline_inventory_value": str(observation.bot_inventory_value_usdc),
        "baseline_unrealized_pnl": str(observation.unrealized_pnl_usdc or 0),
        "frozen_position_ids": [row["position_id"] for row in frozen_payload],
        "source_authority": SOURCE_AUTHORITY,
        "approved_by": approver,
        "approval_provenance": provenance,
    }
    fingerprint = hashlib.sha256(json.dumps(
        fingerprint_payload, sort_keys=True, separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()

    cur.execute(
        """
        INSERT INTO paper_equity_baseline_v2 (
          deployment_id,baseline_version,baseline_timestamp,
          cutover_boundary,baseline_account_total,baseline_managed_equity,
          baseline_external_manual,baseline_available,
          baseline_inventory_value,baseline_realized_net_pnl,
          baseline_unrealized_pnl,baseline_fees,baseline_open_positions,
          frozen_pre_baseline_unresolved_count,evidence_status,
          source_authority,approved_by,approval_provenance,
          activation_fingerprint
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
          'COMPLETE',%s,%s,%s::jsonb,%s
        ) RETURNING baseline_id
        """,
        (
            deployment, BASELINE_VERSION, observation.source_timestamp,
            observation.source_timestamp, observation.account_total_value_usdc,
            baseline_managed, observation.external_manual_value_usdc,
            observation.available_usdc, observation.bot_inventory_value_usdc,
            observation.realized_net_pnl_usdc or Decimal("0"),
            observation.unrealized_pnl_usdc or Decimal("0"),
            observation.fees_usdc or Decimal("0"),
            int(observation.open_positions), len(frozen_payload),
            SOURCE_AUTHORITY, approver,
            json.dumps(provenance, sort_keys=True, default=str), fingerprint,
        ),
    )
    baseline_id = int(cur.fetchone()[0])

    if frozen_payload:
        cur.execute(
            """
            INSERT INTO paper_equity_frozen_outcome_v2 (
              baseline_id,deployment_id,position_id,classification,
              original_outcome_status,original_evidence_status,
              original_blocking_reasons,immutable_economic_snapshot,
              original_financial_truth_rows
            )
            SELECT %s,%s,p.id,'PRE_BASELINE_FROZEN',
                   item->>'outcome_status',item->>'evidence_status',
                   COALESCE(item->'blocking_reasons','[]'::jsonb),
                   jsonb_build_object(
                     'gross_pnl_usdc',p.gross_pnl_usdc,
                     'fees_usdc',p.fees_usdc,
                     'net_pnl_usdc',p.net_pnl_usdc,
                     'qty',p.qty,'entry_price',p.entry_price,
                     'exit_price',p.exit_price,
                     'entry_order_id',p.entry_order_id,
                     'exit_order_id',p.exit_order_id,
                     'exit_time',p.exit_time
                   ),
                   (SELECT count(*) FROM canonical_financial_truth_v1 ft
                    WHERE ft.position_id=p.id)
            FROM jsonb_array_elements(%s::jsonb) item
            JOIN positions p ON p.id=(item->>'position_id')::bigint
            """,
            (baseline_id, deployment,
             json.dumps(frozen_payload, sort_keys=True, default=str)),
        )
        if cur.rowcount != len(frozen_payload):
            raise RuntimeError("PAPER_EQUITY_BASELINE_V2_FROZEN_COHORT_INCOMPLETE")

    baseline = fetch_paper_equity_baseline_v2(cur, deployment_id=deployment)
    if baseline is None:
        raise RuntimeError("PAPER_EQUITY_BASELINE_V2_POSTCONDITION_FAILED")
    return BaselineActivationResult(baseline, True, "CREATED")

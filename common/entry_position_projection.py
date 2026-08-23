"""LEI1D immutable entry-fill position projection.

The caller owns the transaction.  This module never commits and never performs
network I/O.  OFF is the default and returns before opening a database cursor.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping

from common.capital_reservation import deploy_live_entry_fill_cursor
from common.live_exit_cost import load_live_exit_cost_links_cursor
from common.pre_entry_risk import (
    load_current_pre_entry_risk_cursor,
    pre_entry_risk_schema_available_cursor,
    transition_pre_entry_risk_cursor,
)
from common.position_risk_boundary import activate_live_boundary_cursor
from common.position_risk_boundary import (
    RiskBoundaryProjection,
    evaluate_position_risk,
)


ENTRY_POSITION_PROJECTION_MODE_ENV = "LIVE_ENTRY_POSITION_PROJECTION_MODE"
PROJECTION_DIAGNOSTIC_NAMESPACE = uuid.UUID(
    "fcb026d2-93d2-5fce-9adb-a40dffb1f649"
)
LIVE_OPEN_RISK_MARK_FRESHNESS = timedelta(minutes=20)


class EntryPositionProjectionMode(str, Enum):
    OFF = "OFF"
    SHADOW = "SHADOW"
    ENFORCE = "ENFORCE"

    @classmethod
    def from_env(
        cls, environment: Mapping[str, str] | None = None
    ) -> "EntryPositionProjectionMode":
        source = os.environ if environment is None else environment
        raw = str(source.get(ENTRY_POSITION_PROJECTION_MODE_ENV, "OFF"))
        try:
            return cls(raw.strip().upper())
        except ValueError as exc:
            raise ValueError("LIVE_ENTRY_POSITION_PROJECTION_MODE_INVALID") from exc


class EntryProjectionOutcome(str, Enum):
    MODE_OFF = "MODE_OFF"
    SHADOW = "SHADOW"
    NO_ELIGIBLE_FILL = "NO_ELIGIBLE_FILL"
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_UPDATED = "POSITION_UPDATED"
    NO_OP = "NO_OP"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class EntryProjectionResult:
    outcome: EntryProjectionOutcome
    intent_id: uuid.UUID | None = None
    position_id: int | None = None
    newly_applied_entry_qty: Decimal = Decimal("0")
    cumulative_eligible_entry_qty: Decimal = Decimal("0")
    event_inserted: bool = False
    detail: str | None = None
    post_fill_risk_handoff: str | None = None


@dataclass(frozen=True)
class EntryProjectionRunStats:
    mode: EntryPositionProjectionMode
    scanned: int = 0
    opened: int = 0
    updated: int = 0
    no_op: int = 0
    blocked: int = 0


def entry_ack_requires_projection(
    environment: Mapping[str, str] | None = None,
) -> bool:
    return EntryPositionProjectionMode.from_env(environment) is (
        EntryPositionProjectionMode.ENFORCE
    )


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _assets(symbol: str) -> tuple[str, str]:
    value = str(symbol).upper()
    for quote in ("USDC", "USDT", "USD", "EUR"):
        if value.endswith(quote):
            return value[: -len(quote)], quote
    raise RuntimeError("LEI1D_SYMBOL_ASSET_IDENTITY_UNKNOWN")


def _fingerprint(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _diagnostic(cur, *, intent_id, fill_evidence_id, classification, detail, evidence):
    identity = json.dumps(
        {
            "classification": classification,
            "detail": detail,
            "fill_evidence_id": str(fill_evidence_id) if fill_evidence_id else None,
            "intent_id": str(intent_id) if intent_id else None,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    diagnostic_id = uuid.uuid5(PROJECTION_DIAGNOSTIC_NAMESPACE, identity)
    cur.execute(
        """
        INSERT INTO live_entry_position_projection_diagnostics_v1(
          diagnostic_id,intent_id,fill_evidence_id,classification,detail,
          evidence,contract_version
        ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,
          'LIVE_ENTRY_POSITION_PROJECTION_DIAGNOSTIC_V1')
        ON CONFLICT (diagnostic_id) DO NOTHING
        """,
        (
            str(diagnostic_id),
            str(intent_id) if intent_id else None,
            str(fill_evidence_id) if fill_evidence_id else None,
            classification,
            detail,
            json.dumps(evidence, sort_keys=True, separators=(",", ":")),
        ),
    )


def _live_post_fill_risk_handoff_cursor(
    cur, *, intent_id: uuid.UUID, position_id: int, eligible: list[tuple],
    cumulative_gross_quantity: Decimal, cumulative_net_quantity: Decimal,
    weighted_entry_price: Decimal, projection_fingerprint: str,
    as_of: datetime | None,
) -> str:
    """Converge LIVE fill ownership under the caller-owned transaction."""
    if not pre_entry_risk_schema_available_cursor(cur):
        return "SCHEMA_UNAVAILABLE"
    canonical_as_of = as_of
    if canonical_as_of is None:
        cur.execute("SELECT clock_timestamp()")
        canonical_as_of = cur.fetchone()[0]
    if canonical_as_of.tzinfo is None:
        canonical_as_of = canonical_as_of.replace(tzinfo=timezone.utc)

    cur.execute(
        "SELECT requested_qty FROM live_entry_intents_v1 "
        "WHERE intent_id=%s AND environment='live'",
        (str(intent_id),),
    )
    intent_row = cur.fetchone()
    if intent_row is None:
        return "FILL_ATTRIBUTION_INCOMPLETE"
    requested_quantity = _decimal(intent_row[0])
    if cumulative_gross_quantity > requested_quantity:
        return "QUANTITY_ACCOUNTING_INCOMPLETE"

    cumulative = Decimal("0")
    for row in eligible:
        fill_quantity = _decimal(row[11])
        cumulative += fill_quantity
        reservation_status = deploy_live_entry_fill_cursor(
            cur, intent_id=intent_id, fill_evidence_id=row[0],
            position_id=position_id, filled_quantity=fill_quantity,
            cumulative_filled_quantity=cumulative,
            requested_quantity=requested_quantity,
            effective_at=row[15],
        )
        if reservation_status not in {"INSERTED", "IDEMPOTENT"}:
            return f"RESERVATION_DEPLOYMENT_INCOMPLETE:{reservation_status}"

    boundary_status = activate_live_boundary_cursor(
        cur, intent_id=str(intent_id), position_id=position_id,
        canonical_entry_basis=weighted_entry_price,
        effective_at=canonical_as_of,
    )
    if boundary_status not in {"INSERTED", "IDEMPOTENT"}:
        return f"BOUNDARY_INCOMPLETE:{boundary_status}"

    cur.execute(
        "SELECT boundary_id,environment,deployment_id,"
        "account_identity_fingerprint,side,state,boundary_distance_pct,"
        "entry_basis_price,entry_basis_authority,boundary_price,boundary_type,"
        "execution_price_guarantee,policy_fingerprint,effective_at,"
        "event_fingerprint,reservation_id "
        "FROM v_position_risk_boundary_current_v1 "
        "WHERE intent_id=%s AND position_id=%s",
        (str(intent_id), int(position_id)),
    )
    boundary = cur.fetchone()
    if boundary is None or str(boundary[5]) not in {
        "BOUNDARY_ACTIVATED", "BOUNDARY_REVISED_ENTRY_BASIS"
    }:
        return "BOUNDARY_INCOMPLETE:MISSING_BOUNDARY"
    boundary_id = uuid.UUID(str(boundary[0]))
    deployment_id = str(boundary[2])
    account_identity = str(boundary[3])
    exit_costs = load_live_exit_cost_links_cursor(
        cur, deployment_id=deployment_id,
        account_identity_fingerprint=account_identity, as_of=canonical_as_of,
    )
    fee_rate, fee_status, fee_model = exit_costs.get(
        int(position_id), (None, "MISSING_EXIT_COST_AUTHORITY", None)
    )
    if fee_status != "CANONICAL" or fee_rate is None or fee_model is None:
        return f"EXIT_COST_INCOMPLETE:{fee_status}"
    cur.execute(
        "SELECT s.exit_cost_snapshot_id,s.snapshot_fingerprint "
        "FROM live_position_exit_cost_link_v1 l "
        "JOIN live_exit_cost_snapshot_v1 s USING(exit_cost_snapshot_id) "
        "WHERE l.position_id=%s AND l.boundary_id=%s",
        (int(position_id), str(boundary_id)),
    )
    exit_cost_evidence = cur.fetchone()
    if exit_cost_evidence is None:
        return "EXIT_COST_INCOMPLETE:MISSING_EXIT_COST_AUTHORITY"

    cur.execute(
        "SELECT status,side,remaining_inventory_qty,inventory_evidence_status,"
        "symbol,interval FROM positions WHERE id=%s",
        (int(position_id),),
    )
    position = cur.fetchone()
    if (
        position is None
        or str(position[0]) != "OPEN"
        or str(position[3]) != "COMPLETE"
        or position[2] is None
        or _decimal(position[2]) != cumulative_net_quantity
    ):
        return "POSITION_INVENTORY_INCOMPLETE"
    cur.execute(
        "SELECT close,open_time FROM candles WHERE symbol=%s AND interval=%s "
        "AND open_time<=%s ORDER BY open_time DESC LIMIT 1",
        (str(position[4]), str(position[5]), canonical_as_of),
    )
    mark = cur.fetchone()
    if mark is None or mark[0] is None or mark[1] is None:
        mark_price = None
        mark_timestamp = None
        mark_status = "PRICE_UNAVAILABLE"
    else:
        mark_price = _decimal(mark[0])
        mark_timestamp = mark[1]
        mark_status = (
            "PRICE_STALE"
            if mark_timestamp < canonical_as_of - LIVE_OPEN_RISK_MARK_FRESHNESS
            else "CANONICAL"
        )
    projection = RiskBoundaryProjection(
        boundary_id=boundary_id, position_id=int(position_id),
        environment=str(boundary[1]), deployment_id=deployment_id,
        account_identity_fingerprint=account_identity,
        side=str(boundary[4]), state=str(boundary[5]),
        boundary_distance_pct=_decimal(boundary[6]),
        entry_basis_price=_decimal(boundary[7]),
        entry_basis_authority=str(boundary[8]),
        boundary_price=_decimal(boundary[9]),
        boundary_type=str(boundary[10]),
        execution_price_guarantee=str(boundary[11]),
        policy_fingerprint=str(boundary[12]), effective_at=boundary[13],
        exit_fee_rate=fee_rate, exit_fee_model=fee_model,
        exit_fee_status=fee_status,
    )
    open_risk = evaluate_position_risk(
        position_id=int(position_id), side=str(position[1]),
        remaining_inventory_qty=_decimal(position[2]),
        mark_price=mark_price, mark_status=mark_status,
        projection=projection, require_exit_cost=True,
    )
    if open_risk.status != "CANONICAL":
        return f"OPEN_RISK_INCOMPLETE:{open_risk.status}"

    reservation_id = uuid.UUID(str(boundary[15]))
    current = load_current_pre_entry_risk_cursor(
        cur, reservation_id=reservation_id,
    )
    if current is None:
        return "PRE_ENTRY_RISK_INCOMPLETE"
    if current.transferred_quantity > cumulative_gross_quantity:
        return "QUANTITY_ACCOUNTING_INCOMPLETE"
    transfer_quantity = cumulative_gross_quantity - current.transferred_quantity
    if transfer_quantity == 0:
        return "IDEMPOTENT"
    if transfer_quantity > current.remaining_committed_quantity:
        return "QUANTITY_ACCOUNTING_INCOMPLETE"
    open_risk_fingerprint = _fingerprint({
        "authority": "POSITION_RISK_BOUNDARY_AUTHORITY_V1_PLUS_"
        "LIVE_EXIT_COST_AUTHORITY_V1",
        "boundary_event_fingerprint": str(boundary[14]),
        "boundary_id": str(boundary_id),
        "exit_cost_contract": fee_model,
        "exit_cost_snapshot_id": str(exit_cost_evidence[0]),
        "exit_cost_snapshot_fingerprint": str(exit_cost_evidence[1]),
        "fill_evidence_ids": [str(row[0]) for row in eligible],
        "mark_price": str(mark_price),
        "mark_timestamp": mark_timestamp.astimezone(timezone.utc).isoformat(),
        "open_risk_to_trigger": str(open_risk.open_risk_to_trigger),
        "position_id": int(position_id),
        "projection_fingerprint": projection_fingerprint,
        "cumulative_gross_quantity": str(cumulative_gross_quantity),
        "cumulative_net_quantity": str(cumulative_net_quantity),
    })
    status, _ = transition_pre_entry_risk_cursor(
        cur, reservation_id=reservation_id,
        source_event_identity=(
            f"LIVE_OPEN_RISK_HANDOFF:{projection_fingerprint}"
        ),
        effective_at=canonical_as_of, transfer_quantity=transfer_quantity,
        open_risk_status="CANONICAL", open_risk_position_id=int(position_id),
        open_risk_boundary_id=boundary_id,
        open_risk_evidence_fingerprint=open_risk_fingerprint,
        source_authority="LIVE_CANONICAL_OPEN_RISK_HANDOFF",
        provenance={
            "intent_id": str(intent_id),
            "position_id": int(position_id),
            "fill_evidence_ids": [str(row[0]) for row in eligible],
            "exit_cost_snapshot_id": str(exit_cost_evidence[0]),
            "exit_cost_snapshot_fingerprint": str(exit_cost_evidence[1]),
            "transferred_quantity": str(transfer_quantity),
            "cumulative_gross_quantity": str(cumulative_gross_quantity),
            "cumulative_net_inventory_quantity": str(cumulative_net_quantity),
            "mark_timestamp": mark_timestamp.astimezone(timezone.utc).isoformat(),
        },
    )
    return status


_LATEST_FILL_ROWS_SQL = """
WITH latest AS (
  SELECT DISTINCT ON (a.fill_evidence_id)
    a.fill_evidence_id,a.intent_id,a.submission_attempt_id,a.ack_id,
    a.client_order_id,a.strategy,a."interval",a.order_purpose,
    a.local_fill_id,a.linked_position_id,a.attribution_status,
    a.application_status,a.canonical_source_fingerprint,a.decided_at
  FROM live_entry_fill_applications_v1 a
  WHERE a.intent_id=%s
  ORDER BY a.fill_evidence_id,a.decided_at DESC,a.application_decision_id DESC
)
SELECT
  e.fill_evidence_id,e.environment,e.deployment_id,e.adoption_id,e.generation,
  e.git_revision,e.exchange_source,e.exchange_order_id,e.client_order_id,
  e.symbol,e.side,e.executed_qty,e.price,e.fee,e.fee_asset,e.executed_at,
  e.source_fingerprint,
  l.submission_attempt_id,l.ack_id,l.strategy,l."interval",l.order_purpose,
  l.local_fill_id,l.linked_position_id,l.attribution_status,
  l.application_status,l.canonical_source_fingerprint,
  f.commission_usdc
FROM latest l
JOIN live_entry_fill_evidence_v1 e ON e.fill_evidence_id=l.fill_evidence_id
LEFT JOIN binance_order_fills f ON f.id=l.local_fill_id
ORDER BY e.executed_at,e.fill_evidence_id
"""


def project_entry_intent(
    cur, intent_id: uuid.UUID | str, *, as_of: datetime | None = None,
) -> EntryProjectionResult:
    """Project one immutable intent under caller-owned transaction."""
    canonical_intent_id = uuid.UUID(str(intent_id))
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"LEI1D|{canonical_intent_id}",),
    )
    cur.execute(_LATEST_FILL_ROWS_SQL, (str(canonical_intent_id),))
    all_rows = list(cur.fetchall())
    if not all_rows:
        return EntryProjectionResult(
            EntryProjectionOutcome.NO_ELIGIBLE_FILL, canonical_intent_id
        )

    hard_blockers = {
        "IDEMPOTENCY_CONFLICT",
        "AMBIGUOUS",
        "CORRECTION_PENDING",
        "EXTERNAL_OR_MANUAL_UNLINKED",
    }
    for row in all_rows:
        if str(row[25]) in hard_blockers or str(row[24]) in {
            "CONFLICTED", "AMBIGUOUS", "EXTERNAL_OR_MANUAL_UNLINKED"
        }:
            detail = f"{row[24]}:{row[25]}"
            _diagnostic(
                cur,
                intent_id=canonical_intent_id,
                fill_evidence_id=row[0],
                classification="CORRECTION_OR_CONFLICT",
                detail=detail,
                evidence={"attribution_status": row[24], "application_status": row[25]},
            )
            return EntryProjectionResult(
                EntryProjectionOutcome.BLOCKED,
                canonical_intent_id,
                detail=detail,
            )

    eligible = [
        row for row in all_rows
        if str(row[25]) in {"APPLIED", "TRUE_DUPLICATE_APPLIED"}
        and str(row[24]) in {
            "BOT_OWNED_ATTRIBUTED", "BOT_OWNED_MISSING_POSITION"
        }
    ]
    if not eligible:
        return EntryProjectionResult(
            EntryProjectionOutcome.NO_ELIGIBLE_FILL, canonical_intent_id
        )

    identity_indexes = (1, 2, 3, 4, 5, 6, 7, 8, 9, 17, 18, 19, 20, 21)
    identity = tuple(eligible[0][index] for index in identity_indexes)
    if any(tuple(row[index] for index in identity_indexes) != identity for row in eligible):
        _diagnostic(
            cur,
            intent_id=canonical_intent_id,
            fill_evidence_id=eligible[0][0],
            classification="POSITION_IDENTITY_CONFLICT",
            detail="ELIGIBLE_FILL_LINEAGE_NOT_UNIQUE",
            evidence={"fill_count": len(eligible)},
        )
        return EntryProjectionResult(
            EntryProjectionOutcome.BLOCKED,
            canonical_intent_id,
            detail="ELIGIBLE_FILL_LINEAGE_NOT_UNIQUE",
        )

    first = eligible[0]
    symbol = str(first[9])
    if str(first[10]) != "BUY" or str(first[21]) != "ENTRY":
        return EntryProjectionResult(
            EntryProjectionOutcome.BLOCKED,
            canonical_intent_id,
            detail="ENTRY_BUY_LINEAGE_REQUIRED",
        )
    base_asset, quote_asset = _assets(symbol)
    gross = Decimal("0")
    base_fee = Decimal("0")
    notional = Decimal("0")
    normalized_fees = Decimal("0")
    fill_payload = []
    for row in eligible:
        quantity = _decimal(row[11])
        price = _decimal(row[12])
        fee = _decimal(row[13])
        fee_asset = str(row[14] or "").upper()
        if not fee_asset:
            _diagnostic(
                cur,
                intent_id=canonical_intent_id,
                fill_evidence_id=row[0],
                classification="INCOMPLETE_FEE_EVIDENCE",
                detail="FEE_ASSET_MISSING",
                evidence={"fee": str(fee)},
            )
            return EntryProjectionResult(
                EntryProjectionOutcome.BLOCKED,
                canonical_intent_id,
                detail="FEE_ASSET_MISSING",
            )
        gross += quantity
        notional += quantity * price
        if fee_asset == base_asset:
            base_fee += fee
        normalized = row[27]
        if normalized is None:
            if fee_asset in {quote_asset, "USDC", "USDT", "USD"}:
                normalized = fee
            elif fee_asset == base_asset:
                normalized = fee * price
            else:
                _diagnostic(
                    cur,
                    intent_id=canonical_intent_id,
                    fill_evidence_id=row[0],
                    classification="INCOMPLETE_FEE_EVIDENCE",
                    detail="NORMALIZED_FEE_MISSING",
                    evidence={"fee_asset": fee_asset, "fee": str(fee)},
                )
                return EntryProjectionResult(
                    EntryProjectionOutcome.BLOCKED,
                    canonical_intent_id,
                    detail="NORMALIZED_FEE_MISSING",
                )
        normalized_fees += _decimal(normalized)
        fill_payload.append(
            {
                "fill_evidence_id": str(row[0]),
                "source_fingerprint": str(row[16]),
                "executed_qty": str(quantity),
                "price": str(price),
                "fee": str(fee),
                "fee_asset": fee_asset,
            }
        )
    net = gross - base_fee
    if gross <= 0 or net <= 0:
        raise RuntimeError("LEI1D_NONPOSITIVE_NET_ENTRY_INVENTORY")
    weighted_price = notional / gross
    projection_fingerprint = _fingerprint(
        {
            "fills": fill_payload,
            "gross": str(gross),
            "base_fee": str(base_fee),
            "net": str(net),
            "notional": str(notional),
        }
    )

    cur.execute(
        """
        SELECT projection_id,position_id,projected_gross_entry_qty
        FROM live_entry_position_projections_v1
        WHERE intent_id=%s FOR UPDATE
        """,
        (str(canonical_intent_id),),
    )
    projection = cur.fetchone()
    linked_ids = {int(row[23]) for row in eligible if row[23] is not None}
    if len(linked_ids) > 1:
        return EntryProjectionResult(
            EntryProjectionOutcome.BLOCKED,
            canonical_intent_id,
            detail="MULTIPLE_LINKED_POSITIONS",
        )
    position_id = int(projection[1]) if projection else (
        next(iter(linked_ids)) if linked_ids else None
    )
    strategy = str(first[19])
    interval = str(first[20])
    order_id = str(first[7])
    client_order_id = str(first[8])
    if position_id is None:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"LEI1D_SLOT|{symbol}|{strategy}|{interval}",),
        )
        cur.execute(
            """
            SELECT id FROM positions
            WHERE symbol=%s AND strategy=%s AND "interval"=%s AND status='OPEN'
            ORDER BY id FOR UPDATE
            """,
            (symbol, strategy, interval),
        )
        open_positions = list(cur.fetchall())
        if open_positions:
            _diagnostic(
                cur,
                intent_id=canonical_intent_id,
                fill_evidence_id=first[0],
                classification="LEGACY_OPEN_POSITION_CONFLICT",
                detail="OPEN_POSITION_WITHOUT_IMMUTABLE_INTENT_LINK",
                evidence={"position_ids": [int(row[0]) for row in open_positions]},
            )
            return EntryProjectionResult(
                EntryProjectionOutcome.BLOCKED,
                canonical_intent_id,
                detail="OPEN_POSITION_WITHOUT_IMMUTABLE_INTENT_LINK",
            )
        cur.execute(
            """
            INSERT INTO positions(
              symbol,strategy,"interval",status,side,qty,entry_price,entry_time,
              entry_order_id,entry_client_order_id,fees_usdc,
              inventory_evidence_status,gross_entry_executed_qty,
              entry_base_fee_qty,net_entry_inventory_qty,
              cumulative_exit_executed_qty,exit_inventory_reduction_qty,
              remaining_inventory_qty,inventory_calculated_at,
              inventory_contract_adoption_id,inventory_contract_generation,
              entry_intent_id
            ) VALUES (
              %s,%s,%s,'OPEN','LONG',%s,%s,%s,%s,%s,%s,'COMPLETE',
              %s,%s,%s,0,0,%s,clock_timestamp(),%s,%s,%s
            ) RETURNING id
            """,
            (
                symbol, strategy, interval, net, weighted_price,
                min(row[15] for row in eligible), order_id, client_order_id,
                normalized_fees, gross, base_fee, net, net,
                int(first[3]), int(first[4]), str(canonical_intent_id),
            ),
        )
        position_id = int(cur.fetchone()[0])
    else:
        cur.execute(
            """
            SELECT symbol,strategy,"interval",status,entry_order_id,
                   entry_client_order_id,entry_intent_id,
                   COALESCE(exit_inventory_reduction_qty,0)
            FROM positions WHERE id=%s FOR UPDATE
            """,
            (position_id,),
        )
        position = cur.fetchone()
        valid = position is not None and all(
            (
                str(position[0]) == symbol,
                str(position[1]) == strategy,
                str(position[2]) == interval,
                str(position[3]) == "OPEN",
                position[4] is None or str(position[4]) == order_id,
                position[5] is None or str(position[5]) == client_order_id,
                position[6] is None or str(position[6]) == str(canonical_intent_id),
            )
        )
        if not valid:
            _diagnostic(
                cur,
                intent_id=canonical_intent_id,
                fill_evidence_id=first[0],
                classification="POSITION_IDENTITY_CONFLICT",
                detail="LINKED_POSITION_IDENTITY_MISMATCH",
                evidence={"position_id": position_id},
            )
            return EntryProjectionResult(
                EntryProjectionOutcome.BLOCKED,
                canonical_intent_id,
                position_id=position_id,
                detail="LINKED_POSITION_IDENTITY_MISMATCH",
            )
        if net < _decimal(position[7]):
            raise RuntimeError("LEI1D_ENTRY_INVENTORY_BELOW_EXIT_REDUCTION")
        cur.execute(
            "UPDATE positions SET entry_intent_id=COALESCE(entry_intent_id,%s) "
            "WHERE id=%s",
            (str(canonical_intent_id), position_id),
        )

    previous = _decimal(projection[2]) if projection else Decimal("0")
    delta = gross - previous
    if delta < 0:
        raise RuntimeError("LEI1D_PROJECTION_HIGH_WATER_REGRESSION")
    if projection is None:
        cur.execute(
            """
            INSERT INTO live_entry_position_projections_v1(
              intent_id,position_id,environment,deployment_id,adoption_id,
              generation,git_revision,exchange_source,exchange_order_id,
              client_order_id,submission_attempt_id,ack_id,symbol,strategy,
              "interval",projected_fill_count,projected_gross_entry_qty,
              projected_entry_base_fee_qty,projected_net_entry_qty,
              projected_entry_notional,projection_fingerprint,contract_version
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,'LIVE_ENTRY_POSITION_PROJECTION_V1'
            ) RETURNING projection_id
            """,
            (
                str(canonical_intent_id), position_id, first[1], first[2],
                first[3], first[4], first[5], first[6], order_id,
                client_order_id, str(first[17]), str(first[18]), symbol,
                strategy, interval, len(eligible), gross, base_fee, net,
                notional, projection_fingerprint,
            ),
        )
        projection_id = int(cur.fetchone()[0])
    else:
        projection_id = int(projection[0])
        if delta > 0:
            cur.execute(
                """
                UPDATE live_entry_position_projections_v1 SET
                  projected_fill_count=%s,projected_gross_entry_qty=%s,
                  projected_entry_base_fee_qty=%s,projected_net_entry_qty=%s,
                  projected_entry_notional=%s,projection_fingerprint=%s,
                  updated_at=clock_timestamp()
                WHERE projection_id=%s
                """,
                (len(eligible), gross, base_fee, net, notional,
                 projection_fingerprint, projection_id),
            )
    if delta == 0:
        handoff_status = _live_post_fill_risk_handoff_cursor(
            cur, intent_id=canonical_intent_id,
            position_id=int(projection[1]), eligible=eligible,
            cumulative_gross_quantity=gross,
            cumulative_net_quantity=net,
            weighted_entry_price=weighted_price,
            projection_fingerprint=projection_fingerprint, as_of=as_of,
        )
        return EntryProjectionResult(
            EntryProjectionOutcome.NO_OP, canonical_intent_id, position_id,
            delta, gross, False, post_fill_risk_handoff=handoff_status,
        )

    cur.execute(
        "SELECT COALESCE(exit_inventory_reduction_qty,0) FROM positions "
        "WHERE id=%s",
        (position_id,),
    )
    exit_reduction = _decimal(cur.fetchone()[0])
    remaining = net - exit_reduction
    if remaining < 0:
        raise RuntimeError("LEI1D_ENTRY_INVENTORY_BELOW_EXIT_REDUCTION")
    cur.execute(
        """
        UPDATE positions SET
          entry_intent_id=%s,entry_order_id=COALESCE(entry_order_id,%s),
          entry_client_order_id=COALESCE(entry_client_order_id,%s),
          entry_price=%s,entry_time=LEAST(entry_time,%s),fees_usdc=%s,
          inventory_evidence_status='COMPLETE',gross_entry_executed_qty=%s,
          entry_base_fee_qty=%s,net_entry_inventory_qty=%s,
          remaining_inventory_qty=%s,qty=%s,
          inventory_calculated_at=clock_timestamp(),
          inventory_contract_adoption_id=COALESCE(
            inventory_contract_adoption_id,%s),
          inventory_contract_generation=COALESCE(
            inventory_contract_generation,%s)
        WHERE id=%s AND status='OPEN'
        """,
        (
            str(canonical_intent_id), order_id, client_order_id,
            weighted_price, min(row[15] for row in eligible), normalized_fees,
            gross, base_fee, net, remaining, remaining,
            int(first[3]), int(first[4]), position_id,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError("LEI1D_POSITION_UPDATE_LOST_OPEN_RACE")
    handoff_status = _live_post_fill_risk_handoff_cursor(
        cur, intent_id=canonical_intent_id, position_id=position_id,
        eligible=eligible, cumulative_gross_quantity=gross,
        cumulative_net_quantity=net, weighted_entry_price=weighted_price,
        projection_fingerprint=projection_fingerprint, as_of=as_of,
    )
    for row in eligible:
        cur.execute(
            """
            INSERT INTO live_entry_position_projection_fills_v1(
              projection_id,intent_id,position_id,fill_evidence_id,
              local_fill_id,source_fingerprint,executed_qty,
              entry_base_fee_qty,contract_version
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
              'LIVE_ENTRY_POSITION_PROJECTION_FILL_V1')
            ON CONFLICT (fill_evidence_id) DO NOTHING
            """,
            (
                projection_id, str(canonical_intent_id), position_id,
                str(row[0]), int(row[22]), str(row[16]), row[11],
                row[13] if str(row[14]).upper() == base_asset else Decimal("0"),
            ),
        )
    event_inserted = False
    if previous == 0:
        cur.execute(
            """
            INSERT INTO position_lifecycle_events_c2_2(
              position_id,order_id,mutation_kind,mutation_high_water,payload
            ) VALUES (%s,%s,'POSITION_OPENED',%s,%s::jsonb)
            ON CONFLICT DO NOTHING RETURNING event_id
            """,
            (
                position_id, order_id, gross,
                json.dumps({
                    "intent_id": str(canonical_intent_id),
                    "ack_id": str(first[18]),
                    "submission_attempt_id": str(first[17]),
                    "position_id": position_id,
                    "fill_evidence_ids": [str(row[0]) for row in eligible],
                    "gross_entry_executed_qty": str(gross),
                    "entry_base_fee_qty": str(base_fee),
                    "net_entry_inventory_qty": str(net),
                    "entry_price": str(weighted_price),
                    "contract_version": "LIVE_ENTRY_POSITION_PROJECTION_V1",
                }, sort_keys=True, separators=(",", ":")),
            ),
        )
        event_inserted = cur.fetchone() is not None
    cur.execute(
        """
        UPDATE binance_orders SET
          reconciled_position_id=%s,reconciliation_status='LEI1D_PROJECTED',
          reconciled_at=clock_timestamp(),reconciled_fill_count=%s,
          reconciled_executed_qty=%s,unreconciled_qty=0,
          reconciliation_error=NULL,last_reconciliation_action='LEI1D_PROJECTED'
        WHERE lower(exchange_source)=%s AND symbol=%s AND order_id=%s
          AND order_purpose='ENTRY'
        """,
        (position_id, len(eligible), gross, str(first[6]), symbol, order_id),
    )
    return EntryProjectionResult(
        EntryProjectionOutcome.POSITION_OPENED if previous == 0
        else EntryProjectionOutcome.POSITION_UPDATED,
        canonical_intent_id, position_id, delta, gross, event_inserted,
        post_fill_risk_handoff=handoff_status,
    )


def run_entry_position_projection(
    conn,
    *,
    mode: EntryPositionProjectionMode | str | None = None,
    environment: Mapping[str, str] | None = None,
    batch_size: int = 100,
) -> EntryProjectionRunStats:
    selected = (
        EntryPositionProjectionMode.from_env(environment)
        if mode is None
        else mode if isinstance(mode, EntryPositionProjectionMode)
        else EntryPositionProjectionMode(str(mode).upper())
    )
    if selected is EntryPositionProjectionMode.OFF:
        return EntryProjectionRunStats(selected)
    source = os.environ if environment is None else environment
    if selected is EntryPositionProjectionMode.ENFORCE and (
        str(source.get("LIVE_ENTRY_SUBMISSION_MODE", "OFF")).upper() != "ENFORCE"
        or str(source.get("LIVE_ENTRY_FILL_ATTRIBUTION_MODE", "OFF")).upper()
        != "ENFORCE"
    ):
        raise RuntimeError("LEI1D_PREREQUISITE_FEATURES_NOT_ENFORCED")
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest AS (
              SELECT DISTINCT ON (fill_evidence_id)
                fill_evidence_id,intent_id,decided_at,application_decision_id
              FROM live_entry_fill_applications_v1
              WHERE intent_id IS NOT NULL
              ORDER BY fill_evidence_id,decided_at DESC,
                       application_decision_id DESC
            )
            SELECT DISTINCT intent_id FROM latest
            ORDER BY intent_id LIMIT %s
            """,
            (max(1, min(int(batch_size), 1000)),),
        )
        intents = [uuid.UUID(str(row[0])) for row in cur.fetchall()]
    if selected is EntryPositionProjectionMode.SHADOW:
        return EntryProjectionRunStats(selected, scanned=len(intents))
    counts = {"opened": 0, "updated": 0, "no_op": 0, "blocked": 0}
    for intent_id in intents:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT lei1d_intent_projection")
            try:
                result = project_entry_intent(cur, intent_id)
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT lei1d_intent_projection")
                cur.execute("RELEASE SAVEPOINT lei1d_intent_projection")
                raise
            cur.execute("RELEASE SAVEPOINT lei1d_intent_projection")
        if result.outcome is EntryProjectionOutcome.POSITION_OPENED:
            counts["opened"] += 1
        elif result.outcome is EntryProjectionOutcome.POSITION_UPDATED:
            counts["updated"] += 1
        elif result.outcome in {
            EntryProjectionOutcome.NO_OP, EntryProjectionOutcome.NO_ELIGIBLE_FILL
        }:
            counts["no_op"] += 1
        else:
            counts["blocked"] += 1
    return EntryProjectionRunStats(selected, scanned=len(intents), **counts)

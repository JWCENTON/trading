from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class FillMutationDecision(str, Enum):
    NEW_AUTHORITATIVE_EVIDENCE = "NEW_AUTHORITATIVE_EVIDENCE"
    AUTHORITATIVE_CORRECTION = "AUTHORITATIVE_CORRECTION"
    NO_CHANGE = "NO_CHANGE"
    LEGACY_RECONSTRUCTION_BLOCKED = "LEGACY_RECONSTRUCTION_BLOCKED"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"
    AMBIGUOUS_CORRECTION = "AMBIGUOUS_CORRECTION"
    EXISTING_PROJECTED_EVIDENCE = "EXISTING_PROJECTED_EVIDENCE"
    ADOPTION_GENERATION_MISMATCH = "ADOPTION_GENERATION_MISMATCH"
    ADOPTION_NOT_ACTIVE = "ADOPTION_NOT_ACTIVE"


class InventoryRowGeneration(str, Enum):
    FORWARD_C2_2 = "FORWARD_C2_2"
    EXISTING_PROJECTED_C2_2 = "EXISTING_PROJECTED_C2_2"
    LEGACY_UNPROJECTED = "LEGACY_UNPROJECTED"
    LEGACY_RECONSTRUCTION_APPROVED = "LEGACY_RECONSTRUCTION_APPROVED"
    ADOPTION_GENERATION_MISMATCH = "ADOPTION_GENERATION_MISMATCH"


@dataclass(frozen=True)
class RegisteredFillChange:
    ingestion_id: int
    decision: FillMutationDecision
    fingerprint: str
    correction_revision: int
    row_generation: InventoryRowGeneration | None = None
    adoption_id: int | None = None
    contract_generation: int | None = None

    @property
    def mutation_decision(self) -> FillMutationDecision:
        if self.row_generation is None:
            return FillMutationDecision.ADOPTION_NOT_ACTIVE
        if (
            self.row_generation
            is InventoryRowGeneration.ADOPTION_GENERATION_MISMATCH
        ):
            return FillMutationDecision.ADOPTION_GENERATION_MISMATCH
        if self.row_generation is InventoryRowGeneration.LEGACY_UNPROJECTED:
            return FillMutationDecision.LEGACY_RECONSTRUCTION_BLOCKED
        if (
            self.row_generation
            is InventoryRowGeneration.EXISTING_PROJECTED_C2_2
            and self.decision
            in {
                FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
                FillMutationDecision.AUTHORITATIVE_CORRECTION,
            }
        ):
            return FillMutationDecision.EXISTING_PROJECTED_EVIDENCE
        return self.decision

    @property
    def permits_mutation(self) -> bool:
        evidence_accepted = self.decision in {
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
        }
        return evidence_accepted and self.row_generation in {
            InventoryRowGeneration.FORWARD_C2_2,
            InventoryRowGeneration.EXISTING_PROJECTED_C2_2,
        }


def classify_inventory_row_generation(
    *,
    entry_time,
    active_adopted_at,
    active_adoption_id: int | None,
    active_generation: int | None,
    position_adoption_id: int | None,
    position_generation: int | None,
    existing_projected_compatible: bool,
    historical_reconstruction_approved: bool = False,
) -> InventoryRowGeneration:
    if historical_reconstruction_approved:
        return InventoryRowGeneration.LEGACY_RECONSTRUCTION_APPROVED
    if existing_projected_compatible:
        return InventoryRowGeneration.EXISTING_PROJECTED_C2_2
    attributed = (
        position_adoption_id is not None or position_generation is not None
    )
    if attributed:
        if (
            active_adoption_id is not None
            and active_generation is not None
            and position_adoption_id == active_adoption_id
            and position_generation == active_generation
        ):
            return InventoryRowGeneration.FORWARD_C2_2
        return InventoryRowGeneration.ADOPTION_GENERATION_MISMATCH
    if (
        active_adoption_id is not None
        and active_generation is not None
        and active_adopted_at is not None
        and entry_time is not None
        and entry_time >= active_adopted_at
    ):
        return InventoryRowGeneration.FORWARD_C2_2
    return InventoryRowGeneration.LEGACY_UNPROJECTED


def is_existing_projected_c2_2_compatible(
    position: Mapping[str, Any],
    *,
    has_authoritative_entry_evidence: bool,
    entry_evidence_gross_qty: Any = None,
    entry_evidence_base_fee_qty: Any = None,
    exit_evidence_gross_qty: Any = None,
    tolerance: Decimal = Decimal("0.000000000001"),
) -> bool:
    """Pure counterpart of the PostgreSQL compatibility predicate."""
    required = (
        "gross_entry_executed_qty",
        "entry_base_fee_qty",
        "net_entry_inventory_qty",
        "cumulative_exit_executed_qty",
        "exit_inventory_reduction_qty",
        "remaining_inventory_qty",
        "inventory_calculated_at",
        "qty",
    )
    if (
        position.get("inventory_evidence_status") != "COMPLETE"
        or not has_authoritative_entry_evidence
        or any(position.get(field) is None for field in required)
        or entry_evidence_gross_qty is None
        or entry_evidence_base_fee_qty is None
    ):
        return False
    try:
        qty = Decimal(str(position["qty"]))
        remaining = Decimal(str(position["remaining_inventory_qty"]))
        gross_entry = Decimal(str(position["gross_entry_executed_qty"]))
        entry_fee = Decimal(str(position["entry_base_fee_qty"]))
        net_entry = Decimal(str(position["net_entry_inventory_qty"]))
        exit_reduction = Decimal(str(position["exit_inventory_reduction_qty"]))
        cumulative_exit = Decimal(
            str(position["cumulative_exit_executed_qty"])
        )
        evidence_entry = Decimal(str(entry_evidence_gross_qty))
        evidence_fee = Decimal(str(entry_evidence_base_fee_qty))
        evidence_exit = (
            Decimal(str(exit_evidence_gross_qty))
            if exit_evidence_gross_qty is not None
            else Decimal("0")
        )
    except Exception:
        return False
    return all(
        (
            abs(qty - remaining) <= tolerance,
            abs(net_entry - (gross_entry - entry_fee)) <= tolerance,
            abs(remaining - (net_entry - exit_reduction)) <= tolerance,
            abs(evidence_entry - gross_entry) <= tolerance,
            abs(evidence_fee - entry_fee) <= tolerance,
            abs(evidence_exit - cumulative_exit) <= tolerance,
            gross_entry >= entry_fee,
            net_entry >= exit_reduction,
        )
    )


def authoritative_fill_payload(
    row: Mapping[str, Any], *, account_identity_key: str
) -> dict[str, Any]:
    return {
        "exchange": str(row.get("source") or "").lower(),
        "account_identity": str(account_identity_key),
        "instrument": str(row.get("symbol") or "").upper(),
        "trade_id": str(row.get("trade_id") or ""),
        "order_id": str(row.get("order_id") or ""),
        "side": str(row.get("side") or "").upper(),
        "executed_qty": str(row.get("executed_qty") or ""),
        "fill_price": str(row.get("avg_price") or ""),
        "fee_quantity": str(row.get("commission_amount") or ""),
        "fee_currency": str(row.get("commission_asset") or "").upper(),
        "event_time_ms": int(row.get("event_time_ms") or 0),
    }


def authoritative_fill_fingerprint(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def classify_authoritative_change(
    previous_payload: Mapping[str, Any],
    current_payload: Mapping[str, Any],
) -> FillMutationDecision:
    if (
        authoritative_fill_fingerprint(previous_payload)
        == authoritative_fill_fingerprint(current_payload)
    ):
        return FillMutationDecision.NO_CHANGE
    previous_qty = Decimal(str(previous_payload.get("executed_qty") or 0))
    current_qty = Decimal(str(current_payload.get("executed_qty") or 0))
    if current_qty < previous_qty:
        return FillMutationDecision.AMBIGUOUS_CORRECTION
    return FillMutationDecision.AUTHORITATIVE_CORRECTION


def _resolve_row_generation(cur, row: Mapping[str, Any]):
    cur.execute(
        """
        SELECT
          CASE
            WHEN adoption.adoption_id IS NULL THEN 'ADOPTION_NOT_ACTIVE'
            WHEN p.inventory_contract_adoption_id IS NOT NULL
             AND (
               p.inventory_contract_adoption_id <> adoption.adoption_id
               OR p.inventory_contract_generation <> adoption.generation
             ) THEN 'ADOPTION_GENERATION_MISMATCH'
            WHEN p.inventory_contract_adoption_id = adoption.adoption_id
             AND p.inventory_contract_generation = adoption.generation
              THEN 'FORWARD_C2_2'
            WHEN is_existing_projected_c2_2_compatible(
              p.id, adoption.environment
            ) THEN 'EXISTING_PROJECTED_C2_2'
            WHEN p.inventory_contract_adoption_id IS NULL
             AND p.inventory_contract_generation IS NULL
             AND p.entry_time >= adoption.adopted_at THEN 'FORWARD_C2_2'
            ELSE 'LEGACY_UNPROJECTED'
          END,
          adoption.adoption_id,
          adoption.generation
        FROM positions p
        LEFT JOIN binance_orders bo
          ON bo.position_id = p.id OR bo.order_id = p.entry_order_id
        LEFT JOIN runtime_contract_adoption_v2 adoption
          ON adoption.contract_name = 'FEE_AWARE_INVENTORY_C2_2'
         AND adoption.status = 'ACTIVE'
         AND adoption.environment = lower(%s)
         AND adoption.deployment_id = %s
        WHERE p.entry_order_id = %s OR p.exit_order_id = %s
           OR bo.order_id = %s
        ORDER BY p.id
        LIMIT 1
        """,
        (
            str(row.get("environment") or "").lower(),
            str(row.get("deployment_id") or ""),
            str(row.get("order_id") or ""),
            str(row.get("order_id") or ""),
            str(row.get("order_id") or ""),
        ),
    )
    result = cur.fetchone()
    if result is None:
        return None, None, None
    classification, adoption_id, generation = result
    if classification == "ADOPTION_NOT_ACTIVE":
        return classification, None, None
    return classification, adoption_id, generation


def register_fill_change(
    cur,
    row: Mapping[str, Any],
    *,
    account_identity_key: str,
) -> RegisteredFillChange:
    """Classify a source fill before it is allowed to alter authoritative state."""
    payload = authoritative_fill_payload(
        row, account_identity_key=account_identity_key
    )
    row_classification, adoption_id, contract_generation = (
        _resolve_row_generation(cur, row)
    )
    row_generation = (
        InventoryRowGeneration(row_classification)
        if row_classification in {item.value for item in InventoryRowGeneration}
        else None
    )
    fingerprint = authoritative_fill_fingerprint(payload)
    identity = (
        payload["exchange"],
        payload["account_identity"],
        payload["instrument"],
        payload["trade_id"],
    )
    cur.execute(
        """
        SELECT ingestion_id,source_fingerprint,authoritative_payload,
               correction_revision
        FROM exchange_fill_ingestion_state_v2
        WHERE source=%s AND account_identity_key=%s
          AND symbol=%s AND trade_id=%s
        FOR UPDATE
        """,
        identity,
    )
    existing = cur.fetchone()
    if existing is None:
        cur.execute(
            """
            SELECT order_id,symbol,side,executed_qty,avg_price,
                   commission_amount,commission_asset,
                   (extract(epoch FROM event_time)*1000)::bigint
            FROM binance_order_fills
            WHERE source=%s AND trade_id=%s
            """,
            (payload["exchange"], payload["trade_id"]),
        )
        previously_ingested = cur.fetchone()
        if previously_ingested is not None:
            (
                old_order_id, old_symbol, old_side, old_qty, old_price,
                old_fee, old_fee_asset, old_event_time_ms,
            ) = previously_ingested
            old_payload = authoritative_fill_payload(
                {
                    "source": payload["exchange"],
                    "symbol": old_symbol,
                    "trade_id": payload["trade_id"],
                    "order_id": old_order_id,
                    "side": old_side,
                    "executed_qty": old_qty,
                    "avg_price": old_price,
                    "commission_amount": old_fee,
                    "commission_asset": old_fee_asset,
                    "event_time_ms": old_event_time_ms,
                },
                account_identity_key=account_identity_key,
            )
            old_fingerprint = authoritative_fill_fingerprint(old_payload)
            decision = classify_authoritative_change(old_payload, payload)
            same = decision is FillMutationDecision.NO_CHANGE
            status = {
                FillMutationDecision.NO_CHANGE: "DUPLICATE",
                FillMutationDecision.AUTHORITATIVE_CORRECTION: (
                    "CORRECTION_PENDING"
                ),
                FillMutationDecision.AMBIGUOUS_CORRECTION: "AMBIGUOUS",
            }[decision]
            preserve_existing = (
                decision is FillMutationDecision.AMBIGUOUS_CORRECTION
            )
            stored_fingerprint = (
                old_fingerprint if preserve_existing else fingerprint
            )
            stored_payload = old_payload if preserve_existing else payload
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,account_identity_key,symbol,trade_id,order_id,side,
                  source_fingerprint,applied_fingerprint,applied_at,
                  application_status,authoritative_payload,last_decision,
                  correction_revision,adoption_id,contract_generation
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,
                  CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                  %s,%s::jsonb,%s,CASE WHEN %s THEN 0 ELSE 1 END,%s,%s
                )
                RETURNING ingestion_id
                """,
                (
                    *identity,
                    payload["order_id"],
                    payload["side"],
                    stored_fingerprint,
                    old_fingerprint if (same or preserve_existing) else None,
                    same or preserve_existing,
                    status,
                    json.dumps(stored_payload, sort_keys=True),
                    decision.value,
                    same,
                    adoption_id,
                    contract_generation,
                ),
            )
            return RegisteredFillChange(
                int(cur.fetchone()[0]), decision, fingerprint,
                0 if same else 1,
                row_generation, adoption_id, contract_generation,
            )
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,
              last_decision,adoption_id,contract_generation
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'NEW',%s::jsonb,%s,%s,%s)
            RETURNING ingestion_id
            """,
            (
                *identity,
                payload["order_id"],
                payload["side"],
                fingerprint,
                json.dumps(payload, sort_keys=True),
                FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE.value,
                adoption_id,
                contract_generation,
            ),
        )
        return RegisteredFillChange(
            int(cur.fetchone()[0]),
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            fingerprint,
            0,
            row_generation,
            adoption_id,
            contract_generation,
        )

    ingestion_id, previous_fingerprint, previous_payload, revision = existing
    if str(previous_fingerprint) == fingerprint:
        cur.execute(
            """
            UPDATE exchange_fill_ingestion_state_v2
            SET last_seen_at=clock_timestamp(),application_status='DUPLICATE',
                last_decision=%s
            WHERE ingestion_id=%s
            """,
            (FillMutationDecision.NO_CHANGE.value, int(ingestion_id)),
        )
        return RegisteredFillChange(
            int(ingestion_id), FillMutationDecision.NO_CHANGE,
            fingerprint, int(revision), row_generation,
            adoption_id, contract_generation,
        )

    next_revision = int(revision) + 1
    decision = classify_authoritative_change(previous_payload or {}, payload)
    if decision is FillMutationDecision.AMBIGUOUS_CORRECTION:
        status = "AMBIGUOUS"
        cur.execute(
            """
            UPDATE exchange_fill_ingestion_state_v2
            SET last_seen_at=clock_timestamp(),application_status=%s,
                correction_revision=%s,last_decision=%s
            WHERE ingestion_id=%s
            """,
            (
                status, next_revision, decision.value, int(ingestion_id),
            ),
        )
        return RegisteredFillChange(
            int(ingestion_id), decision, fingerprint, next_revision,
            row_generation, adoption_id, contract_generation,
        )
    else:
        status = "CORRECTION_PENDING"
    cur.execute(
        """
        UPDATE exchange_fill_ingestion_state_v2
        SET last_seen_at=clock_timestamp(),source_fingerprint=%s,
            application_status=%s,correction_revision=%s,
            authoritative_payload=%s::jsonb,last_decision=%s
        WHERE ingestion_id=%s
        """,
        (
            fingerprint, status, next_revision,
            json.dumps(payload, sort_keys=True), decision.value,
            int(ingestion_id),
        ),
    )
    return RegisteredFillChange(
        int(ingestion_id), decision, fingerprint, next_revision,
        row_generation, adoption_id, contract_generation,
    )


def mark_fill_change_applied(cur, change: RegisteredFillChange) -> None:
    if not change.permits_mutation:
        return
    status = (
        "CORRECTION_APPLIED"
        if change.decision is FillMutationDecision.AUTHORITATIVE_CORRECTION
        else "NEW"
    )
    cur.execute(
        """
        UPDATE exchange_fill_ingestion_state_v2
        SET applied_fingerprint=%s,applied_at=clock_timestamp(),
            application_status=%s
        WHERE ingestion_id=%s
        """,
        (change.fingerprint, status, change.ingestion_id),
    )


def attribute_fill_change_position(
    cur, row: Mapping[str, Any], change: RegisteredFillChange
) -> None:
    """Bind an accepted forward/compatible row before evidence is replaced."""
    if (
        not change.permits_mutation
        or change.adoption_id is None
        or change.contract_generation is None
    ):
        return
    cur.execute(
        """
        UPDATE positions p
        SET inventory_contract_adoption_id=%s,
            inventory_contract_generation=%s
        WHERE p.inventory_contract_adoption_id IS NULL
          AND p.inventory_contract_generation IS NULL
          AND (
            p.entry_order_id=%s OR p.exit_order_id=%s
            OR EXISTS (
              SELECT 1 FROM binance_orders bo
              WHERE bo.position_id=p.id AND bo.order_id=%s
            )
          )
        """,
        (
            change.adoption_id,
            change.contract_generation,
            str(row.get("order_id") or ""),
            str(row.get("order_id") or ""),
            str(row.get("order_id") or ""),
        ),
    )

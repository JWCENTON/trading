from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


DELAYED_ENTRY_BOOTSTRAP_DEPLOYMENTS = frozenset({"local-live", "vps-live"})


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
    OBSERVED_NOT_APPLIED = "OBSERVED_NOT_APPLIED"


class FillApplicationClassification(str, Enum):
    OBSERVED_NOT_APPLIED = "OBSERVED_NOT_APPLIED"
    TRUE_DUPLICATE_APPLIED = "TRUE_DUPLICATE_APPLIED"
    CORRECTION_PENDING = "CORRECTION_PENDING"
    AMBIGUOUS = "AMBIGUOUS"
    EXTERNAL_OR_MANUAL_UNLINKED = "EXTERNAL_OR_MANUAL_UNLINKED"
    BLOCKED_MISSING_CONTEXT = "BLOCKED_MISSING_CONTEXT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    REJECTED = "REJECTED"


class InventoryRowGeneration(str, Enum):
    FORWARD_C2_2 = "FORWARD_C2_2"
    FORWARD_C2_2_PENDING_ENTRY = "FORWARD_C2_2_PENDING_ENTRY"
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
    application_status: FillApplicationClassification = (
        FillApplicationClassification.OBSERVED_NOT_APPLIED
    )

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
        if self.application_status in {
            FillApplicationClassification.AMBIGUOUS,
            FillApplicationClassification.EXTERNAL_OR_MANUAL_UNLINKED,
            FillApplicationClassification.BLOCKED_MISSING_CONTEXT,
            FillApplicationClassification.IDEMPOTENCY_CONFLICT,
            FillApplicationClassification.REJECTED,
        }:
            return False
        evidence_accepted = self.decision in {
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
        }
        return evidence_accepted and self.row_generation in {
            InventoryRowGeneration.FORWARD_C2_2,
            InventoryRowGeneration.FORWARD_C2_2_PENDING_ENTRY,
            InventoryRowGeneration.EXISTING_PROJECTED_C2_2,
        }


def classify_fill_application_state(
    *,
    source_fingerprint: str,
    applied_fingerprint: str | None,
    applied_at: Any,
    local_fill_id: int | None,
    resolved_adoption_id: int | None,
    resolved_generation: int | None,
    applied_adoption_id: int | None,
    applied_generation: int | None,
    local_fill_matches: bool,
    current_status: str | None = None,
    attribution_status: str | None = None,
) -> FillApplicationClassification:
    """Classify replay separately from semantic source equality.

    A matching source fingerprint is not application proof.  The legacy
    mutable ingestion row may claim a true duplicate only when it points to a
    canonical local fill, carries a matching applied fingerprint/timestamp,
    and remains in the same adopted generation.  Pending corrections and
    ambiguous evidence are never collapsed into a duplicate on replay.
    """
    sticky = {
        item.value: item
        for item in (
            FillApplicationClassification.CORRECTION_PENDING,
            FillApplicationClassification.AMBIGUOUS,
            FillApplicationClassification.EXTERNAL_OR_MANUAL_UNLINKED,
            FillApplicationClassification.BLOCKED_MISSING_CONTEXT,
            FillApplicationClassification.IDEMPOTENCY_CONFLICT,
            FillApplicationClassification.REJECTED,
        )
    }
    if current_status in sticky:
        return sticky[current_status]
    attribution_classification = _application_classification_for_attribution(
        attribution_status
    )
    if attribution_classification is not None:
        return attribution_classification
    complete = all(
        value is not None
        for value in (
            local_fill_id,
            applied_fingerprint,
            applied_at,
            resolved_adoption_id,
            resolved_generation,
            applied_adoption_id,
            applied_generation,
        )
    )
    if (
        complete
        and local_fill_matches
        and str(applied_fingerprint) == str(source_fingerprint)
        and int(applied_adoption_id) == int(resolved_adoption_id)
        and int(applied_generation) == int(resolved_generation)
    ):
        return FillApplicationClassification.TRUE_DUPLICATE_APPLIED
    return FillApplicationClassification.OBSERVED_NOT_APPLIED


def _application_classification_for_attribution(
    attribution_status: str | None,
) -> FillApplicationClassification | None:
    return {
        "EXTERNAL_OR_MANUAL_UNLINKED": (
            FillApplicationClassification.EXTERNAL_OR_MANUAL_UNLINKED
        ),
        "AMBIGUOUS": FillApplicationClassification.AMBIGUOUS,
        "CONFLICTED": FillApplicationClassification.IDEMPOTENCY_CONFLICT,
        "UNKNOWN": FillApplicationClassification.OBSERVED_NOT_APPLIED,
    }.get(str(attribution_status or "").upper())


def _local_fill_application_proof_matches(
    cur,
    payload: Mapping[str, Any],
    *,
    account_identity_key: str,
    local_fill_id: int | None,
) -> bool:
    if local_fill_id is None:
        return False
    cur.execute(
        """
        SELECT id,order_id,symbol,side,executed_qty,avg_price,
               commission_amount,commission_asset,
               (extract(epoch FROM event_time)*1000)::bigint
        FROM binance_order_fills
        WHERE source=%s AND trade_id=%s AND id=%s
        """,
        (payload["exchange"], payload["trade_id"], local_fill_id),
    )
    local = cur.fetchone()
    if local is None or int(local[0]) != int(local_fill_id):
        return False
    local_payload = authoritative_fill_payload(
        {
            "source": payload["exchange"],
            "symbol": local[2],
            "trade_id": payload["trade_id"],
            "order_id": local[1],
            "side": local[3],
            "executed_qty": local[4],
            "avg_price": local[5],
            "commission_amount": local[6],
            "commission_asset": local[7],
            "event_time_ms": local[8],
        },
        account_identity_key=account_identity_key,
    )
    return (
        authoritative_fill_fingerprint(local_payload)
        == authoritative_fill_fingerprint(payload)
    )


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
    def rendered(field: str) -> str:
        value = row.get(field)
        return "" if value is None else str(value)

    return {
        "exchange": str(row.get("source") or "").lower(),
        "account_identity": str(account_identity_key),
        "instrument": str(row.get("symbol") or "").upper(),
        "trade_id": str(row.get("trade_id") or ""),
        "order_id": str(row.get("order_id") or ""),
        "side": str(row.get("side") or "").upper(),
        "executed_qty": rendered("executed_qty"),
        "fill_price": rendered("avg_price"),
        "fee_quantity": rendered("commission_amount"),
        "fee_currency": str(row.get("commission_asset") or "").upper(),
        "event_time_ms": int(row.get("event_time_ms") or 0),
    }


def authoritative_fill_fingerprint(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _legacy_zero_fee_payload_equivalent(
    previous_payload: Mapping[str, Any] | None,
    current_payload: Mapping[str, Any],
) -> bool:
    if not isinstance(previous_payload, Mapping):
        return False
    normalized = dict(current_payload)
    if previous_payload.get("fee_quantity") != "":
        return False
    try:
        if Decimal(str(current_payload.get("fee_quantity"))) != 0:
            return False
    except Exception:
        return False
    normalized["fee_quantity"] = ""
    return dict(previous_payload) == normalized


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


def _okx_wire_client_order_id(value: Any) -> str | None:
    if value is None:
        return None
    normalized = "".join(char for char in str(value) if char.isalnum())[:32]
    return normalized or None


def _resolve_pending_entry_generation(
    cur,
    row: Mapping[str, Any],
    *,
    account_identity_key: str | None,
):
    """Resolve the first delayed fill from an exact baseline pending order.

    This is deliberately narrower than the normal position-owned generation
    resolver.  It only bootstraps fresh allowlisted LIVE OKX BUY evidence whose
    deterministic wire CID proves ownership by one accepted pending entry
    order.  Previously observed evidence is excluded so deploying this code
    cannot replay an historical ``OBSERVED_NOT_APPLIED`` cohort.
    """
    source = str(row.get("source") or "").strip().lower()
    environment = str(row.get("environment") or "").strip().lower()
    deployment_id = str(row.get("deployment_id") or "").strip()
    side = str(row.get("side") or "").strip().upper()
    symbol = str(row.get("symbol") or "").strip().upper()
    order_id = str(row.get("order_id") or "").strip()
    trade_id = str(row.get("trade_id") or "").strip()
    wire_cid = _okx_wire_client_order_id(row.get("client_order_id"))
    account_identity_status = str(
        row.get("account_identity_status") or ""
    ).strip().upper()
    try:
        event_time_ms = int(row.get("event_time_ms") or 0)
    except (TypeError, ValueError):
        return None, None, None
    if not all((
        source == "okx",
        environment == "live",
        deployment_id in DELAYED_ENTRY_BOOTSTRAP_DEPLOYMENTS,
        side == "BUY",
        symbol,
        order_id,
        trade_id,
        wire_cid,
        account_identity_key,
        row.get("account_identity_id") is not None,
        account_identity_status == "VERIFIED",
        event_time_ms > 0,
    )):
        return None, None, None

    cur.execute(
        """
        /* fill-change:pending-entry-bootstrap-v1 */
        SELECT 'FORWARD_C2_2_PENDING_ENTRY',
               adoption.adoption_id,adoption.generation
        FROM binance_orders bo
        JOIN runtime_contract_adoption_v2 adoption
          ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
         AND adoption.status='ACTIVE'
         AND adoption.environment=%s
         AND adoption.deployment_id=%s
        WHERE lower(COALESCE(bo.exchange_source,''))='okx'
          AND bo.symbol=%s
          AND bo.order_id=%s
          AND upper(bo.side)='BUY'
          AND upper(COALESCE(bo.order_purpose,''))='ENTRY'
          AND COALESCE(bo.is_exit,false) IS FALSE
          AND bo.order_accepted IS TRUE
          AND upper(COALESCE(bo.status,'')) IN (
            'NEW','ACCEPTED','PARTIALLY_FILLED'
          )
          AND bo.position_id IS NULL
          AND bo.reconciled_position_id IS NULL
          AND bo.strategy IS NOT NULL AND btrim(bo.strategy)<>''
          AND bo."interval" IS NOT NULL AND btrim(bo."interval")<>''
          AND bo.requested_qty IS NOT NULL AND bo.requested_qty>0
          AND bo.client_order_id LIKE (
            'ORC-L-' || bo.symbol || '-' || upper(left(bo.strategy,4))
            || '-' || lower(bo."interval") || '-E-%%'
          )
          AND bo.client_order_id ~ '-E-[0-9a-f]{6,8}$'
          AND left(regexp_replace(
            bo.client_order_id,'[^A-Za-z0-9]','','g'
          ),32)=%s
          AND COALESCE(bo.raw->>'orderId','')=bo.order_id
          AND COALESCE(bo.raw->>'clientOrderId','')=%s
          AND upper(COALESCE(bo.raw->>'status','')) IN (
            'NEW','ACCEPTED','PARTIALLY_FILLED'
          )
          AND COALESCE(NULLIF(bo.raw->>'executedQty','')::numeric,0)=0
          AND EXISTS (
            SELECT 1 FROM strategy_events event
            WHERE event.event_type='LIVE_ORDER_SENT'
              AND event.symbol=bo.symbol
              AND event.strategy=bo.strategy
              AND event."interval"=bo."interval"
              AND event.info->'resp'->>'orderId'=bo.order_id
              AND event.info->>'client_order_id'=bo.client_order_id
              AND event.info->'resp'->>'clientOrderId'=%s
              AND lower(COALESCE(event.info->>'exchange_source',''))='okx'
              AND COALESCE(NULLIF(
                event.info->>'order_accepted',''
              )::boolean,false) IS TRUE
              AND COALESCE(NULLIF(
                event.info->>'is_exit',''
              )::boolean,false) IS FALSE
              AND upper(COALESCE(event.info->>'order_purpose',''))='ENTRY'
              AND upper(COALESCE(event.info->>'status','')) IN (
                'NEW','ACCEPTED','PARTIALLY_FILLED'
              )
              AND COALESCE(NULLIF(
                event.info->>'executed_qty',''
              )::numeric,0)=0
          )
          AND bo.created_at>=adoption.adopted_at
          AND bo.created_at<=to_timestamp(%s/1000.0)
          AND to_timestamp(%s/1000.0)<=bo.created_at+interval '7 days'
          AND to_timestamp(%s/1000.0)>=clock_timestamp()-interval '7 days'
          AND NOT EXISTS (
            SELECT 1 FROM positions p
            WHERE p.entry_order_id=bo.order_id
               OR p.exit_order_id=bo.order_id
               OR p.id=bo.position_id
               OR p.id=bo.reconciled_position_id
          )
          AND NOT EXISTS (
            SELECT 1 FROM binance_order_fills f
            WHERE lower(f.source)='okx' AND f.trade_id::text=%s
          )
          AND NOT EXISTS (
            SELECT 1 FROM exchange_fill_ingestion_state_v2 state
            WHERE state.source='okx'
              AND state.account_identity_key=%s
              AND state.symbol=%s AND state.trade_id=%s
          )
        ORDER BY bo.id
        LIMIT 1
        """,
        (
            environment,
            deployment_id,
            symbol,
            order_id,
            wire_cid,
            wire_cid,
            wire_cid,
            event_time_ms,
            event_time_ms,
            event_time_ms,
            trade_id,
            str(account_identity_key),
            symbol,
            trade_id,
        ),
    )
    result = cur.fetchone()
    if result is None:
        return None, None, None
    classification, adoption_id, generation = result
    return classification, int(adoption_id), int(generation)


def _resolve_row_generation(
    cur,
    row: Mapping[str, Any],
    *,
    account_identity_key: str | None = None,
):
    cur.execute(
        """
        SELECT
          CASE
            WHEN adoption.adoption_id IS NULL THEN 'ADOPTION_NOT_ACTIVE'
            WHEN p.inventory_contract_adoption_id = adoption.adoption_id
             AND p.inventory_contract_generation = adoption.generation
              THEN 'FORWARD_C2_2'
            WHEN is_existing_projected_c2_2_compatible(
              p.id, adoption.environment
            ) THEN 'EXISTING_PROJECTED_C2_2'
            WHEN p.inventory_contract_adoption_id IS NOT NULL
              THEN 'ADOPTION_GENERATION_MISMATCH'
            WHEN p.inventory_contract_adoption_id IS NULL
             AND p.inventory_contract_generation IS NULL
             AND p.entry_time >= adoption.adopted_at THEN 'FORWARD_C2_2'
            ELSE 'LEGACY_UNPROJECTED'
          END,
          CASE
            WHEN p.inventory_contract_adoption_id IS NOT NULL
              THEN p.inventory_contract_adoption_id
            ELSE adoption.adoption_id
          END,
          CASE
            WHEN p.inventory_contract_generation IS NOT NULL
              THEN p.inventory_contract_generation
            ELSE adoption.generation
          END
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
        return _resolve_pending_entry_generation(
            cur, row, account_identity_key=account_identity_key
        )
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
    lei1c_attribution_status = row.get("_lei1c_attribution_status")
    lei1c_application_classification = (
        _application_classification_for_attribution(
            str(lei1c_attribution_status)
            if lei1c_attribution_status is not None else None
        )
    )
    payload = authoritative_fill_payload(
        row, account_identity_key=account_identity_key
    )
    row_classification, adoption_id, contract_generation = (
        _resolve_row_generation(
            cur, row, account_identity_key=account_identity_key
        )
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
               correction_revision,adoption_id,contract_generation,
               applied_fingerprint,applied_at,local_fill_id,application_status
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
            SELECT id,order_id,symbol,side,executed_qty,avg_price,
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
                local_fill_id, old_order_id, old_symbol, old_side, old_qty,
                old_price, old_fee, old_fee_asset, old_event_time_ms,
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
                FillMutationDecision.NO_CHANGE: "OBSERVED_NOT_APPLIED",
                FillMutationDecision.AUTHORITATIVE_CORRECTION: (
                    "CORRECTION_PENDING"
                ),
                FillMutationDecision.AMBIGUOUS_CORRECTION: "AMBIGUOUS",
            }[decision]
            if lei1c_application_classification is not None:
                status = lei1c_application_classification.value
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
                  source_fingerprint,application_status,local_fill_id,
                  authoritative_payload,last_decision,correction_revision
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,
                  %s,%s,%s::jsonb,%s,CASE WHEN %s THEN 0 ELSE 1 END
                )
                RETURNING ingestion_id
                """,
                (
                    *identity,
                    payload["order_id"],
                    payload["side"],
                    stored_fingerprint,
                    status,
                    int(local_fill_id),
                    json.dumps(stored_payload, sort_keys=True),
                    (
                        FillMutationDecision.OBSERVED_NOT_APPLIED.value
                        if same else decision.value
                    ),
                    same,
                ),
            )
            returned_decision = (
                FillMutationDecision.OBSERVED_NOT_APPLIED
                if same else decision
            )
            return RegisteredFillChange(
                int(cur.fetchone()[0]), returned_decision, fingerprint,
                0 if same else 1,
                row_generation, adoption_id, contract_generation,
                FillApplicationClassification(status),
            )
        initial_application_status = (
            lei1c_application_classification
            or FillApplicationClassification.OBSERVED_NOT_APPLIED
        )
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,
              last_decision
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s
            )
            RETURNING ingestion_id
            """,
            (
                *identity,
                payload["order_id"],
                payload["side"],
                fingerprint,
                initial_application_status.value,
                json.dumps(payload, sort_keys=True),
                FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE.value,
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
            initial_application_status,
        )

    (
        ingestion_id,
        previous_fingerprint,
        previous_payload,
        revision,
        applied_adoption_id,
        applied_generation,
        applied_fingerprint,
        applied_at,
        local_fill_id,
        current_application_status,
    ) = existing
    legacy_zero_equivalent = _legacy_zero_fee_payload_equivalent(
        previous_payload, payload
    )
    if str(previous_fingerprint) == fingerprint or legacy_zero_equivalent:
        classification_fingerprint = (
            str(previous_fingerprint)
            if legacy_zero_equivalent else fingerprint
        )
        local_fill_matches = _local_fill_application_proof_matches(
            cur,
            payload,
            account_identity_key=account_identity_key,
            local_fill_id=(
                int(local_fill_id) if local_fill_id is not None else None
            ),
        )
        application_status = classify_fill_application_state(
            source_fingerprint=classification_fingerprint,
            applied_fingerprint=(
                str(applied_fingerprint)
                if applied_fingerprint is not None else None
            ),
            applied_at=applied_at,
            local_fill_id=(
                int(local_fill_id) if local_fill_id is not None else None
            ),
            resolved_adoption_id=adoption_id,
            resolved_generation=contract_generation,
            applied_adoption_id=(
                int(applied_adoption_id)
                if applied_adoption_id is not None else None
            ),
            applied_generation=(
                int(applied_generation)
                if applied_generation is not None else None
            ),
            local_fill_matches=local_fill_matches,
            current_status=str(current_application_status),
            attribution_status=(
                str(lei1c_attribution_status)
                if lei1c_attribution_status is not None else None
            ),
        )
        replay_decision = (
            FillMutationDecision.NO_CHANGE
            if application_status
            is not FillApplicationClassification.OBSERVED_NOT_APPLIED
            else FillMutationDecision.OBSERVED_NOT_APPLIED
        )
        cur.execute(
            """
            UPDATE exchange_fill_ingestion_state_v2
            SET last_seen_at=clock_timestamp(),application_status=%s,
                last_decision=%s
            WHERE ingestion_id=%s
            """,
            (
                application_status.value,
                replay_decision.value,
                int(ingestion_id),
            ),
        )
        return RegisteredFillChange(
            int(ingestion_id), replay_decision,
            classification_fingerprint, int(revision), row_generation,
            (
                int(applied_adoption_id)
                if applied_adoption_id is not None else None
            ),
            int(applied_generation) if applied_generation is not None else None,
            application_status,
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
            FillApplicationClassification.AMBIGUOUS,
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
        FillApplicationClassification.CORRECTION_PENDING,
    )


def mark_fill_change_applied(cur, change: RegisteredFillChange) -> None:
    if (
        not change.permits_mutation
        or change.adoption_id is None
        or change.contract_generation is None
    ):
        return
    status = (
        "CORRECTION_APPLIED"
        if change.decision is FillMutationDecision.AUTHORITATIVE_CORRECTION
        else "APPLIED"
    )
    cur.execute(
        """
        UPDATE exchange_fill_ingestion_state_v2 AS state
        SET applied_fingerprint=%s,applied_at=clock_timestamp(),
            application_status=%s,
            adoption_id=COALESCE(state.adoption_id,%s),
            contract_generation=COALESCE(state.contract_generation,%s),
            local_fill_id=COALESCE(state.local_fill_id,local_fill.id)
        FROM binance_order_fills AS local_fill
        WHERE state.ingestion_id=%s
          AND local_fill.source=state.source
          AND local_fill.trade_id::text=state.trade_id
          AND local_fill.order_id=state.order_id
          AND local_fill.symbol=state.symbol
          AND local_fill.side=state.side
          AND local_fill.executed_qty IS NOT DISTINCT FROM
              NULLIF(state.authoritative_payload->>'executed_qty','')::numeric
          AND local_fill.avg_price IS NOT DISTINCT FROM
              NULLIF(state.authoritative_payload->>'fill_price','')::numeric
          AND local_fill.commission_amount IS NOT DISTINCT FROM
              NULLIF(state.authoritative_payload->>'fee_quantity','')::numeric
          AND COALESCE(local_fill.commission_asset,'')=
              COALESCE(state.authoritative_payload->>'fee_currency','')
          AND (extract(epoch FROM local_fill.event_time)*1000)::bigint=
              (state.authoritative_payload->>'event_time_ms')::bigint
          AND (
            state.local_fill_id IS NULL
            OR state.local_fill_id=local_fill.id
          )
          AND (
            (state.adoption_id IS NULL
             AND state.contract_generation IS NULL)
            OR
            (state.adoption_id=%s AND state.contract_generation=%s)
          )
        """,
        (
            change.fingerprint,
            status,
            change.adoption_id,
            change.contract_generation,
            change.ingestion_id,
            change.adoption_id,
            change.contract_generation,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError("FILL_APPLIED_GENERATION_IMMUTABILITY_CONFLICT")


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

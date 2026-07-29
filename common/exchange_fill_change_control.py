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


class InventoryRowGeneration(str, Enum):
    FORWARD_C2_2 = "FORWARD_C2_2"
    LEGACY_UNPROJECTED = "LEGACY_UNPROJECTED"
    LEGACY_RECONSTRUCTION_APPROVED = "LEGACY_RECONSTRUCTION_APPROVED"


@dataclass(frozen=True)
class RegisteredFillChange:
    ingestion_id: int
    decision: FillMutationDecision
    fingerprint: str
    correction_revision: int

    @property
    def permits_mutation(self) -> bool:
        return self.decision in {
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            FillMutationDecision.AUTHORITATIVE_CORRECTION,
        }


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
                  correction_revision
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,
                  CASE WHEN %s THEN clock_timestamp() ELSE NULL END,
                  %s,%s::jsonb,%s,CASE WHEN %s THEN 0 ELSE 1 END
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
                ),
            )
            return RegisteredFillChange(
                int(cur.fetchone()[0]), decision, fingerprint,
                0 if same else 1,
            )
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,
              last_decision
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'NEW',%s::jsonb,%s)
            RETURNING ingestion_id
            """,
            (
                *identity,
                payload["order_id"],
                payload["side"],
                fingerprint,
                json.dumps(payload, sort_keys=True),
                FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE.value,
            ),
        )
        return RegisteredFillChange(
            int(cur.fetchone()[0]),
            FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE,
            fingerprint,
            0,
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
            fingerprint, int(revision),
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

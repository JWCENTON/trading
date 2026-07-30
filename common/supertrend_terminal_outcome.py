from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import hashlib
import json

PRODUCER_VERSION = "SUPERTREND_TERMINAL_COMPAT_V1"


def paper_supertrend_entries_enabled(
    connection_factory, *, deployment_id: str
) -> tuple[bool, str | None]:
    conn = connection_factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entries_enabled,operator_reason
                FROM paper_strategy_entry_gate_v1
                WHERE environment='paper' AND deployment_id=%s
                  AND strategy='SUPERTREND'
                """,
                (deployment_id,),
            )
            row = cur.fetchone()
            return (True, None) if row is None else (bool(row[0]), str(row[1]))
    finally:
        conn.close()


def _fingerprint(payload: dict) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def persist_exit_intent(
    connection_factory,
    *,
    position_id: int,
    simulated_order_id: int,
    deployment_id: str,
    symbol: str,
    interval: str,
    canonical_reason_code: str,
    raw_reason: str,
    exit_decision_at: datetime,
) -> str:
    payload = {
        "position_id": int(position_id),
        "simulated_order_id": int(simulated_order_id),
        "environment": "paper",
        "deployment_id": deployment_id,
        "strategy": "SUPERTREND",
        "symbol": symbol,
        "interval": interval,
        "canonical_reason_code": canonical_reason_code,
        "raw_reason": raw_reason,
        "exit_decision_at": exit_decision_at.isoformat(),
        "producer_version": PRODUCER_VERSION,
    }
    fingerprint = _fingerprint(payload)
    conn = connection_factory()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO supertrend_exit_intents_v1(
                      position_id,simulated_order_id,environment,deployment_id,
                      strategy,symbol,"interval",canonical_reason_code,raw_reason,
                      exit_decision_at,producer_version,content_fingerprint
                    ) VALUES (%s,%s,'paper',%s,'SUPERTREND',%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (position_id,simulated_order_id) DO NOTHING
                    """,
                    (
                        position_id, simulated_order_id, deployment_id, symbol,
                        interval, canonical_reason_code, raw_reason,
                        exit_decision_at, PRODUCER_VERSION, fingerprint,
                    ),
                )
                cur.execute(
                    """
                    SELECT content_fingerprint FROM supertrend_exit_intents_v1
                    WHERE position_id=%s AND simulated_order_id=%s
                    """,
                    (position_id, simulated_order_id),
                )
                stored = cur.fetchone()
                if stored is None or stored[0] != fingerprint:
                    raise RuntimeError("SUPERTREND_EXIT_INTENT_CONFLICT")
        return fingerprint
    finally:
        conn.close()


@dataclass(frozen=True)
class ReconcileResult:
    applied: bool
    reason: str
    gross: Decimal | None = None
    fees: Decimal | None = None
    net: Decimal | None = None


def reconcile_terminal_compatibility_outcome(
    connection_factory,
    *,
    position_id: int,
    simulated_order_id: int,
    deployment_id: str,
) -> ReconcileResult:
    conn = connection_factory()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.status,p.inventory_evidence_status,
                           p.remaining_inventory_qty,p.gross_pnl_usdc,
                           p.fees_usdc,p.net_pnl_usdc,p.exit_reason,
                           i.canonical_reason_code,i.content_fingerprint,
                           p.inventory_contract_adoption_id
                    FROM positions p
                    JOIN supertrend_exit_intents_v1 i
                      ON i.position_id=p.id AND i.simulated_order_id=%s
                     AND i.environment='paper' AND i.deployment_id=%s
                    JOIN runtime_contract_adoption_v2 a
                      ON a.adoption_id=p.inventory_contract_adoption_id
                     AND a.generation=p.inventory_contract_generation
                     AND a.environment='paper' AND a.deployment_id=%s
                     AND a.status='ACTIVE'
                    WHERE p.id=%s AND p.strategy='SUPERTREND'
                    FOR UPDATE OF p
                    """,
                    (simulated_order_id, deployment_id, deployment_id, position_id),
                )
                row = cur.fetchone()
                if row is None:
                    return ReconcileResult(False, "POSITION_OR_INTENT_NOT_ELIGIBLE")
                if row[0] != "CLOSED" or row[1] != "COMPLETE" or Decimal(str(row[2])) != 0:
                    return ReconcileResult(False, "INVENTORY_NOT_TERMINAL")
                cur.execute(
                    """
                    SELECT count(*) FROM position_lifecycle_events_c2_2
                    WHERE position_id=%s
                      AND mutation_kind IN ('POSITION_CLOSED','POSITION_CLOSED_TERMINAL_DUST')
                    """,
                    (position_id,),
                )
                if int(cur.fetchone()[0]) != 1:
                    return ReconcileResult(False, "TERMINAL_LIFECYCLE_COUNT_INVALID")
                cur.execute(
                    """
                    SELECT
                      count(*) FILTER (WHERE order_purpose='ENTRY'),
                      count(*) FILTER (WHERE order_purpose='EXIT'),
                      sum(fill_notional) FILTER (WHERE order_purpose='ENTRY'),
                      sum(fill_notional) FILTER (WHERE order_purpose='EXIT'),
                      sum(authoritative_fee_usdc),
                      bool_and(authoritative_fee_usdc IS NOT NULL)
                    FROM simulated_execution_fills_v1 WHERE position_id=%s
                    """,
                    (position_id,),
                )
                fills = cur.fetchone()
                if not fills or fills[0] < 1 or fills[1] < 1:
                    return ReconcileResult(False, "FILL_EVIDENCE_INCOMPLETE")
                if not fills[5] or fills[4] is None:
                    return ReconcileResult(False, "AUTHORITATIVE_FEE_MISSING")
                entry = Decimal(str(fills[2]))
                exit_ = Decimal(str(fills[3]))
                fees = Decimal(str(fills[4]))
                gross = exit_ - entry
                net = gross - fees
                existing = tuple(row[3:7])
                if any(v not in (None, "", Decimal("0"), 0) for v in existing):
                    if (
                        Decimal(str(row[3])) == gross
                        and Decimal(str(row[4])) == fees
                        and Decimal(str(row[5])) == net
                        and str(row[6]) == str(row[7])
                    ):
                        return ReconcileResult(False, "ALREADY_RECONCILED", gross, fees, net)
                    return ReconcileResult(False, "EXISTING_OUTCOME_CONFLICT")
                cur.execute(
                    """
                    UPDATE positions
                    SET gross_pnl_usdc=%s,fees_usdc=%s,net_pnl_usdc=%s,exit_reason=%s
                    WHERE id=%s AND strategy='SUPERTREND'
                      AND status='CLOSED'
                      AND inventory_evidence_status='COMPLETE'
                      AND remaining_inventory_qty=0
                      AND COALESCE(gross_pnl_usdc,0)=0
                      AND COALESCE(fees_usdc,0)=0
                      AND COALESCE(net_pnl_usdc,0)=0
                      AND NULLIF(exit_reason,'') IS NULL
                    """,
                    (gross, fees, net, row[7], position_id),
                )
                if cur.rowcount != 1:
                    return ReconcileResult(False, "CONDITIONAL_UPDATE_LOST")
                return ReconcileResult(True, "RECONCILED", gross, fees, net)
    finally:
        conn.close()

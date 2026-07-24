"""Shared fail-open epilogue for finalized causal decision observations."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from common.db import get_db_conn
from common.decision_contract import FinalDecision
from common.decision_observation_transport import (
    DurableDecisionObservationProducer,
    ProducerObservationStatus,
    TransportFlags,
    deterministic_decision_key,
)
from common.final_decision_producer_audit import (
    AUDIT_LEDGER_HEALTH,
    AuditDecisionContext,
    AuditLedgerFlags,
    FinalDecisionProducerAuditLedger,
)


def finalize_decision_observation(
    decision: FinalDecision | None,
    *,
    source_service: str,
    connection_factory: Callable[[], object] = get_db_conn,
) -> FinalDecision | None:
    """Observe a finalized decision and return the identical object.

    Configuration is evaluated before a producer is constructed so the
    default-off and kill-switch paths cannot open a database connection.
    ``None`` is retained for legacy cycles that have no canonical decision
    identity (for example, no market row at all).
    """
    if decision is None:
        return None

    ledger = None
    decision_key = None
    attempted_at = None
    if AuditLedgerFlags.from_env().enabled:
        try:
            decision_key = deterministic_decision_key(decision)
            ledger = FinalDecisionProducerAuditLedger(
                connection_factory,
                AuditDecisionContext.from_decision(
                    decision,
                    decision_key=decision_key,
                    source_service=source_service,
                ),
            )
            ledger.append("FINALIZED")
        except Exception as exc:
            AUDIT_LEDGER_HEALTH.degrade(type(exc).__name__)
            logging.exception(
                "final_decision_producer_audit_failure",
                extra={
                    "status": "DENOMINATOR_DEGRADED",
                    "event_type": "FINALIZED",
                    "error_class": type(exc).__name__,
                    "source_service": source_service,
                },
            )

    try:
        flags = TransportFlags.from_env()
    except Exception as exc:
        attempted_at = datetime.now(timezone.utc)
        if ledger is not None:
            ledger.append("PRODUCER_ATTEMPTED", attempted_at=attempted_at)
            ledger.append(
                "VALIDATION_REJECTED",
                attempted_at=attempted_at,
                error_class=type(exc).__name__,
            )
            ledger.close()
        logging.exception(
            "FinalDecision observation configuration rejected; "
            "trading result unchanged",
            extra={"source_service": source_service},
        )
        return decision

    if not flags.decision_observation_enabled:
        if ledger is not None:
            ledger.append(
                "SKIPPED_DISABLED",
                skip_reason=ProducerObservationStatus.SKIPPED_DISABLED.value,
            )
            ledger.close()
        return decision
    if flags.kill_switch:
        if ledger is not None:
            ledger.append(
                "SKIPPED_KILL_SWITCH",
                skip_reason=ProducerObservationStatus.SKIPPED_KILL_SWITCH.value,
            )
            ledger.close()
        return decision

    try:
        attempted_at = datetime.now(timezone.utc)
        if ledger is not None:
            ledger.append("PRODUCER_ATTEMPTED", attempted_at=attempted_at)
        producer = DurableDecisionObservationProducer(
            connection_factory,
            flags,
            source_service=source_service,
        )
        result = producer.observe_with_result(
            decision,
            decision_key=decision_key,
        )
        if ledger is not None:
            linked_outbox_event_id = (
                result.outbox_event_id
                if result.status in {
                    ProducerObservationStatus.ACCEPTED,
                    ProducerObservationStatus.IDEMPOTENT_EXISTING,
                    ProducerObservationStatus.IDEMPOTENCY_CONFLICT,
                }
                else None
            )
            ledger.append(
                result.status.value,
                attempted_at=attempted_at,
                skip_reason=result.skip_reason,
                error_class=result.error_class,
                outbox_event_id=linked_outbox_event_id,
            )
    except Exception as exc:
        # The producer is itself fail-open. This outer boundary also protects
        # trading from construction/configuration regressions in the epilogue.
        logging.exception(
            "FinalDecision causal observation sink failed; trading result unchanged",
            extra={"source_service": source_service},
        )
        if ledger is not None:
            ledger.append(
                "OUTBOX_WRITE_FAILED",
                attempted_at=attempted_at,
                error_class=type(exc).__name__,
            )
    finally:
        if ledger is not None:
            ledger.close()
    return decision

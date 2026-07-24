"""Append-only audit denominator for finalized decision producer lifecycles."""

from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from common.decision_contract import FinalDecision
from common.decision_observation import (
    decision_kind_from_final_decision,
    stable_hash,
)


PAYLOAD_VERSION = "FINAL_DECISION_PRODUCER_AUDIT_V1"
EVENT_TYPES = frozenset({
    "FINALIZED",
    "PRODUCER_ATTEMPTED",
    "ACCEPTED",
    "SKIPPED_DISABLED",
    "SKIPPED_KILL_SWITCH",
    "VALIDATION_REJECTED",
    "SERIALIZATION_FAILED",
    "OUTBOX_WRITE_FAILED",
    "IDEMPOTENT_EXISTING",
    "IDEMPOTENCY_CONFLICT",
})
TERMINAL_EVENT_TYPES = EVENT_TYPES - {"FINALIZED", "PRODUCER_ATTEMPTED"}
_AUDIT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "waltrade:final-decision-producer-audit:v1")
_HEALTH_LOCK = threading.Lock()
_DIAGNOSTIC_KEYS: set[tuple[str, str]] = set()


@dataclass(frozen=True)
class AuditLedgerFlags:
    enabled: bool = False

    @classmethod
    def from_env(cls) -> "AuditLedgerFlags":
        value = os.getenv("FINAL_DECISION_PRODUCER_AUDIT_LEDGER_ENABLED", "0")
        return cls(enabled=value.strip().lower() in {"1", "true", "yes", "on"})


@dataclass
class AuditLedgerHealth:
    status: str = "HEALTHY"
    failures: Counter[str] = field(default_factory=Counter)

    def degrade(self, error_class: str) -> None:
        with _HEALTH_LOCK:
            self.status = "DENOMINATOR_DEGRADED"
            self.failures[error_class] += 1


AUDIT_LEDGER_HEALTH = AuditLedgerHealth()


def reset_audit_ledger_health_for_tests() -> None:
    """Reset process-local diagnostics; intended only for isolated unit tests."""
    with _HEALTH_LOCK:
        AUDIT_LEDGER_HEALTH.status = "HEALTHY"
        AUDIT_LEDGER_HEALTH.failures.clear()
        _DIAGNOSTIC_KEYS.clear()


@dataclass(frozen=True)
class AuditIdentity:
    decision_key: str
    finalized_event_id: str
    attempt_id: str
    attempt_ordinal: int = 1

    @classmethod
    def build(cls, deployment_id: str, decision_key: str) -> "AuditIdentity":
        finalized = str(uuid.uuid5(
            _AUDIT_NAMESPACE,
            f"{deployment_id}:{decision_key}:FINALIZED:{PAYLOAD_VERSION}",
        ))
        attempt = str(uuid.uuid5(
            _AUDIT_NAMESPACE,
            f"{finalized}:attempt:1:{PAYLOAD_VERSION}",
        ))
        return cls(decision_key, finalized, attempt)

    def audit_event_id(self, event_type: str) -> str:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unsupported producer audit event type: {event_type}")
        branch = (
            f"attempt:{self.attempt_id}"
            if event_type not in {"FINALIZED", "SKIPPED_DISABLED", "SKIPPED_KILL_SWITCH"}
            else "finalized" if event_type == "FINALIZED" else f"skip:{event_type}"
        )
        return str(uuid.uuid5(
            _AUDIT_NAMESPACE,
            f"{self.finalized_event_id}:{branch}:{event_type}:{PAYLOAD_VERSION}",
        ))


@dataclass(frozen=True)
class AuditDecisionContext:
    identity: AuditIdentity
    deployment_id: str
    environment: str
    source_service: str
    source_instance: str
    strategy: str
    symbol: str
    interval: str
    original_decision_type: str
    decision_kind: str
    action: str
    direction: str | None
    decision_created_at: datetime
    finalized_at: datetime
    semantic_digest: str

    @classmethod
    def from_decision(
        cls,
        decision: FinalDecision,
        *,
        decision_key: str,
        source_service: str,
        source_instance: str | None = None,
    ) -> "AuditDecisionContext":
        kind = decision_kind_from_final_decision(decision)
        semantic = stable_hash({
            "strategy": decision.evaluation.strategy,
            "symbol": decision.evaluation.symbol,
            "interval": decision.evaluation.interval,
            "action": decision.action or "NO_ACTION",
            "direction": decision.side,
            "confidence": None,
            "quantity_intent": None,
            "entry_intent": decision.reference_price,
            "stop_loss_intent": None,
            "take_profit_intent": None,
            "exit_intent": None,
            "execution_eligible": decision.entry_attempted,
        })
        deployment_id = decision.evaluation.deployment_id
        return cls(
            identity=AuditIdentity.build(deployment_id, decision_key),
            deployment_id=deployment_id,
            environment=decision.evaluation.environment,
            source_service=source_service,
            source_instance=source_instance or socket.gethostname(),
            strategy=decision.evaluation.strategy,
            symbol=decision.evaluation.symbol,
            interval=decision.evaluation.interval,
            original_decision_type=decision.decision_type.value,
            decision_kind=kind,
            action=decision.action or "NO_ACTION",
            direction=decision.side,
            decision_created_at=decision.finished_at,
            finalized_at=decision.finished_at,
            semantic_digest=semantic,
        )


class FinalDecisionProducerAuditLedger:
    """One fail-open append session for one finalized decision lifecycle."""

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        context: AuditDecisionContext,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.connection_factory = connection_factory
        self.context = context
        self.logger = logger or logging.getLogger(__name__)
        self._conn: Any | None = None

    def append(
        self,
        event_type: str,
        *,
        attempted_at: datetime | None = None,
        skip_reason: str | None = None,
        error_class: str | None = None,
        outbox_event_id: str | None = None,
    ) -> bool:
        try:
            self._append(
                event_type,
                attempted_at=attempted_at,
                skip_reason=skip_reason,
                error_class=error_class,
                outbox_event_id=outbox_event_id,
            )
            return True
        except Exception as exc:
            self._degrade(type(exc).__name__, event_type)
            self._rollback_safely()
            return False

    def _append(
        self,
        event_type: str,
        *,
        attempted_at: datetime | None,
        skip_reason: str | None,
        error_class: str | None,
        outbox_event_id: str | None,
    ) -> None:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unsupported producer audit event type: {event_type}")
        ctx = self.context
        identity = ctx.identity
        is_attempt_event = event_type not in {
            "FINALIZED", "SKIPPED_DISABLED", "SKIPPED_KILL_SWITCH",
        }
        attempt_id = identity.attempt_id if is_attempt_event else None
        attempt_ordinal = identity.attempt_ordinal if is_attempt_event else None
        producer_status = None if event_type == "FINALIZED" else (
            "ATTEMPTED" if event_type == "PRODUCER_ATTEMPTED" else event_type
        )
        event_digest = stable_hash({
            "audit_event_id": identity.audit_event_id(event_type),
            "finalized_event_id": identity.finalized_event_id,
            "attempt_id": attempt_id,
            "attempt_ordinal": attempt_ordinal,
            "event_type": event_type,
            "decision_key": identity.decision_key,
            "deployment_id": ctx.deployment_id,
            "environment": ctx.environment,
            "source_service": ctx.source_service,
            "source_instance": ctx.source_instance,
            "strategy": ctx.strategy,
            "symbol": ctx.symbol,
            "interval": ctx.interval,
            "original_decision_type": ctx.original_decision_type,
            "decision_kind": ctx.decision_kind,
            "action": ctx.action,
            "direction": ctx.direction,
            "producer_status": producer_status,
            "skip_reason": skip_reason,
            "error_class": error_class,
            "semantic_digest": ctx.semantic_digest,
            "outbox_event_id": outbox_event_id,
            "payload_version": PAYLOAD_VERSION,
        })
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO final_decision_producer_audit_v1(
                  audit_event_id,finalized_event_id,attempt_id,attempt_ordinal,
                  event_type,decision_key,deployment_id,environment,
                  source_service,source_instance,strategy,symbol,interval,
                  original_decision_type,decision_kind,action,direction,
                  decision_created_at,finalized_at,producer_attempted_at,
                  producer_status,skip_reason,error_class,semantic_digest,
                  outbox_event_id,payload_version,event_digest
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (audit_event_id) DO NOTHING
                """,
                (
                    identity.audit_event_id(event_type),
                    identity.finalized_event_id,
                    attempt_id,
                    attempt_ordinal,
                    event_type,
                    identity.decision_key,
                    ctx.deployment_id,
                    ctx.environment,
                    ctx.source_service,
                    ctx.source_instance,
                    ctx.strategy,
                    ctx.symbol,
                    ctx.interval,
                    ctx.original_decision_type,
                    ctx.decision_kind,
                    ctx.action,
                    ctx.direction,
                    ctx.decision_created_at,
                    ctx.finalized_at,
                    attempted_at,
                    producer_status,
                    skip_reason,
                    error_class,
                    ctx.semantic_digest,
                    outbox_event_id,
                    PAYLOAD_VERSION,
                    event_digest,
                ),
            )
            cur.execute(
                "SELECT event_digest FROM final_decision_producer_audit_v1 "
                "WHERE audit_event_id=%s",
                (identity.audit_event_id(event_type),),
            )
            existing = cur.fetchone()
            if existing is None or existing[0] != event_digest:
                raise RuntimeError("AUDIT_EVENT_IDENTITY_CONFLICT")
        conn.commit()

    def _connection(self) -> Any:
        if self._conn is None:
            self._conn = self.connection_factory()
        return self._conn

    def _rollback_safely(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.rollback()
        except Exception:
            pass

    def _degrade(self, error_class: str, event_type: str) -> None:
        AUDIT_LEDGER_HEALTH.degrade(error_class)
        key = (self.context.source_service, error_class)
        with _HEALTH_LOCK:
            if key in _DIAGNOSTIC_KEYS:
                return
            _DIAGNOSTIC_KEYS.add(key)
        self.logger.error(
            "final_decision_producer_audit_failure",
            extra={
                "status": "DENOMINATOR_DEGRADED",
                "event_type": event_type,
                "error_class": error_class,
                "deployment_id": self.context.deployment_id,
                "decision_key": self.context.identity.decision_key,
                "source_service": self.context.source_service,
            },
        )

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        except Exception as exc:
            self._degrade(type(exc).__name__, "CLOSE")
        finally:
            self._conn = None

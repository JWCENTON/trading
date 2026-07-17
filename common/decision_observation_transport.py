"""Durable transport for causal decision observations (default-off foundation)."""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from common.decision_contract import FinalDecision
from common.decision_observation import (
    DecisionObservationEvent, FailureCode, IdempotencyConflict, VALID_DEPLOYMENTS,
    event_from_final_decision, stable_hash,
)

TRANSPORT_SCHEMA_VERSION = "CAUSAL_DECISION_OBSERVATION_TRANSPORT_V1"
STATUSES = frozenset({"PENDING", "PROCESSING", "RETRY", "PROCESSED",
                      "DEAD_LETTER", "IDEMPOTENCY_CONFLICT"})
DEPLOYMENT_ENVIRONMENTS = {
    "local-live": "trading_live",
    "local-paper": "trading_paper",
    "vps-live": "trading_live",
    "vps-paper": "trading_paper",
}
_DIAGNOSTIC_LOCK = threading.Lock()
_EMITTED_PRODUCER_DIAGNOSTICS: set[tuple[str, str | None, str]] = set()


@dataclass(frozen=True)
class TransportFlags:
    decision_observation_enabled: bool = False
    shadow_observation_enabled: bool = False
    auto_apply: bool = False
    kill_switch: bool = True
    deployment_id: str | None = None
    batch_size: int = 100
    retry_limit: int = 5
    retry_backoff_seconds: int = 60
    lease_timeout_seconds: int = 300

    @classmethod
    def from_env(cls) -> "TransportFlags":
        yes = lambda name, default: os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}
        return cls(
            decision_observation_enabled=yes("CAUSAL_DECISION_OBSERVATION_ENABLED", "0"),
            shadow_observation_enabled=yes("CAUSAL_SHADOW_OBSERVATION_ENABLED", "0"),
            auto_apply=yes("CAUSAL_LEARNING_AUTO_APPLY", "0"),
            kill_switch=yes("CAUSAL_LEARNING_KILL_SWITCH", "1"),
            deployment_id=os.getenv("DEPLOYMENT_ID") or None,
            batch_size=max(1, int(os.getenv("CAUSAL_OUTBOX_BATCH_SIZE", "100"))),
            retry_limit=max(1, int(os.getenv("CAUSAL_OUTBOX_RETRY_LIMIT", "5"))),
            retry_backoff_seconds=max(1, int(os.getenv("CAUSAL_OUTBOX_RETRY_BACKOFF_SECONDS", "60"))),
            lease_timeout_seconds=max(1, int(os.getenv("CAUSAL_OUTBOX_LEASE_TIMEOUT_SECONDS", "300"))),
        )


@dataclass
class TransportMetrics:
    counters: Counter[str] = field(default_factory=Counter)
    gauges: dict[str, float] = field(default_factory=dict)

    def increment(self, name: str) -> None:
        self.counters[name] += 1

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = value


def _payload(event: DecisionObservationEvent) -> dict[str, Any]:
    def value(item: Any) -> Any:
        if isinstance(item, datetime):
            return item.isoformat()
        return str(item) if item.__class__.__name__ == "Decimal" else item
    return {key: value(item) for key, item in asdict(event).items()}


def deterministic_decision_key(decision: FinalDecision) -> str:
    """Stable retry identity; never uses consumer time or position heuristics."""
    identity = (*decision.evaluation.identity_components(), decision.finished_at.isoformat(),
                decision.decision_type.value, decision.decision_subtype.value)
    return stable_hash({"identity": identity})


class DurableDecisionObservationProducer:
    """Fail-open producer. It returns the exact FinalDecision object supplied."""

    def __init__(self, connection_factory: Callable[[], Any], flags: TransportFlags,
                 *, source_service: str, source_instance: str | None = None,
                 metrics: TransportMetrics | None = None,
                 logger: logging.Logger | None = None) -> None:
        self.connection_factory = connection_factory
        self.flags = flags
        self.source_service = source_service
        self.source_instance = source_instance or socket.gethostname()
        self.metrics = metrics or TransportMetrics()
        self.logger = logger or logging.getLogger(__name__)
        self.last_error_code: str | None = None
        self.last_skip_reason: str | None = None

    def observe(self, decision: FinalDecision, *, decision_key: str | None = None,
                event_id: str | None = None) -> FinalDecision:
        if not self.flags.decision_observation_enabled:
            self.last_skip_reason = "OBSERVATION_DISABLED"
            return decision
        if self.flags.kill_switch:
            self.last_skip_reason = "KILL_SWITCH_ACTIVE"
            return decision
        deployment = self.flags.deployment_id
        if deployment is None:
            self._error("CONFIGURATION_INVALID", None, decision_key)
            return decision
        if deployment not in VALID_DEPLOYMENTS or decision.evaluation.deployment_id != deployment:
            self._error("DEPLOYMENT_MISMATCH", None, decision_key)
            return decision
        if decision.evaluation.environment != DEPLOYMENT_ENVIRONMENTS[deployment]:
            self._error("ENVIRONMENT_MISMATCH", None, decision_key)
            return decision
        key = decision_key or deterministic_decision_key(decision)
        try:
            stable_event_id = event_id or str(uuid.uuid5(
                uuid.NAMESPACE_URL, f"waltrade:{deployment}:{key}"
            ))
            event = event_from_final_decision(
                decision, event_id=stable_event_id, decision_key=key,
                source_service=self.source_service, source_instance=self.source_instance,
            )
            payload = _payload(event)
            digest = stable_hash(payload)
            conn = self.connection_factory()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO causal_decision_observation_outbox_v1
                        (event_id,deployment_id,decision_key,event_schema_version,event_payload,
                         event_payload_hash,semantic_digest,source_service,source_instance,
                         decision_created_at,inserted_at,processing_status)
                        VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,now(),'PENDING')
                        ON CONFLICT (deployment_id,decision_key) DO NOTHING
                        RETURNING event_id""",
                        (event.event_id, deployment, key, TRANSPORT_SCHEMA_VERSION,
                         json.dumps(payload, sort_keys=True), digest, event.semantic_digest,
                         self.source_service, self.source_instance, event.decision_created_at),
                    )
                    inserted = cur.fetchone()
                    if inserted is None:
                        cur.execute("""SELECT event_payload_hash FROM causal_decision_observation_outbox_v1
                                    WHERE deployment_id=%s AND decision_key=%s""", (deployment, key))
                        if cur.fetchone()[0] != digest:
                            cur.execute("""UPDATE causal_decision_observation_outbox_v1
                                        SET processing_status='IDEMPOTENCY_CONFLICT',
                                            last_error_code='IDEMPOTENCY_CONFLICT',last_error_at=now()
                                        WHERE deployment_id=%s AND decision_key=%s""", (deployment, key))
                            self.metrics.increment("outbox_idempotency_conflicts_total")
                            self.last_error_code = "IDEMPOTENCY_CONFLICT"
                    else:
                        self.metrics.increment("outbox_events_created_total")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        except Exception:
            self._error(FailureCode.DECISION_OBSERVATION_WRITE_FAILED.value, event_id, key)
        return decision

    def _error(self, code: str, event_id: str | None, decision_key: str | None) -> None:
        self.last_error_code = code
        self.metrics.increment("decision_observation_write_failures_total")
        diagnostic_key = (code, self.flags.deployment_id, self.source_service)
        with _DIAGNOSTIC_LOCK:
            if diagnostic_key in _EMITTED_PRODUCER_DIAGNOSTICS:
                return
            _EMITTED_PRODUCER_DIAGNOSTICS.add(diagnostic_key)
        self.logger.error("causal_outbox_producer_failure", extra={
            "event_id": event_id, "decision_key": decision_key,
            "deployment_id": self.flags.deployment_id, "source_service": self.source_service,
            "status": "WRITE_FAILED", "attempt_count": 0, "error_code": code,
        })


class DecisionObservationOutboxConsumer:
    """Automation-runner single writer for baseline observation telemetry."""

    def __init__(self, connection_factory: Callable[[], Any], flags: TransportFlags,
                 *, consumer_id: str, metrics: TransportMetrics | None = None,
                 logger: logging.Logger | None = None) -> None:
        self.connection_factory, self.flags, self.consumer_id = connection_factory, flags, consumer_id
        self.metrics = metrics or TransportMetrics()
        self.logger = logger or logging.getLogger(__name__)
        self.last_skip_reason: str | None = None

    def poll(self) -> int:
        started = time.monotonic()
        self.metrics.set("consumer_last_poll_timestamp", time.time())
        if not self.flags.decision_observation_enabled:
            self.last_skip_reason = "OBSERVATION_DISABLED"
            return 0
        if self.flags.kill_switch:
            self.last_skip_reason = "KILL_SWITCH_ACTIVE"
            return 0
        if self.flags.deployment_id not in VALID_DEPLOYMENTS:
            self.last_skip_reason = "CONFIGURATION_INVALID"
            return 0
        conn = self.connection_factory()
        processed = 0
        try:
            with conn.cursor() as cur:
                cur.execute("""UPDATE causal_decision_observation_outbox_v1 SET processing_status='RETRY',
                            claimed_at=NULL,claimed_by=NULL,last_error_code='STALE_CLAIM',last_error_at=now(),
                            next_attempt_at=now()
                            WHERE deployment_id=%s AND processing_status='PROCESSING'
                              AND claimed_at < now()-(%s * interval '1 second')""",
                            (self.flags.deployment_id, self.flags.lease_timeout_seconds))
                cur.execute("""SELECT event_id,event_payload,event_payload_hash,semantic_digest,attempt_count
                            FROM causal_decision_observation_outbox_v1
                            WHERE deployment_id=%s AND processing_status IN ('PENDING','RETRY')
                              AND (next_attempt_at IS NULL OR next_attempt_at<=now())
                            ORDER BY decision_created_at,inserted_at,event_id
                            FOR UPDATE SKIP LOCKED LIMIT %s""",
                            (self.flags.deployment_id, self.flags.batch_size))
                rows = cur.fetchall()
                self.metrics.set("current_batch_in_progress", float(len(rows)))
                for event_id, payload, payload_hash, semantic_digest, attempt_count in rows:
                    cur.execute("""UPDATE causal_decision_observation_outbox_v1 SET processing_status='PROCESSING',
                                claimed_at=now(),claimed_by=%s,attempt_count=attempt_count+1 WHERE event_id=%s""",
                                (self.consumer_id, event_id))
                    cur.execute("SAVEPOINT causal_event")
                    try:
                        if stable_hash(payload) != payload_hash:
                            raise ValueError("EVENT_PAYLOAD_HASH_MISMATCH")
                        self._persist(cur, payload, semantic_digest)
                        cur.execute("RELEASE SAVEPOINT causal_event")
                        cur.execute("""UPDATE causal_decision_observation_outbox_v1 SET processing_status='PROCESSED',
                                    processed_at=now(),claimed_at=NULL,claimed_by=NULL WHERE event_id=%s""", (event_id,))
                        processed += 1
                        self.metrics.increment("outbox_events_processed_total")
                    except Exception as exc:
                        cur.execute("ROLLBACK TO SAVEPOINT causal_event")
                        cur.execute("RELEASE SAVEPOINT causal_event")
                        attempts = attempt_count + 1
                        conflict = isinstance(exc, IdempotencyConflict)
                        dead = not conflict and attempts >= self.flags.retry_limit
                        terminal = conflict or dead
                        status = "IDEMPOTENCY_CONFLICT" if conflict else "DEAD_LETTER" if dead else "RETRY"
                        cur.execute("""UPDATE causal_decision_observation_outbox_v1 SET processing_status=%s,
                                    next_attempt_at=CASE WHEN %s THEN NULL ELSE now()+(%s * interval '1 second') END,
                                    claimed_at=NULL,claimed_by=NULL,last_error_code=%s,last_error_at=now()
                                    WHERE event_id=%s""",
                                    (status, terminal, self.flags.retry_backoff_seconds, type(exc).__name__, event_id))
                        self.metrics.increment("outbox_idempotency_conflicts_total" if conflict else
                                               "outbox_events_dead_letter_total" if dead else
                                               "outbox_events_retry_total")
                cur.execute("""SELECT COALESCE(EXTRACT(epoch FROM now()-min(inserted_at)),0)
                            FROM causal_decision_observation_outbox_v1 WHERE deployment_id=%s
                            AND processing_status IN ('PENDING','RETRY')""", (self.flags.deployment_id,))
                self.metrics.set("outbox_oldest_pending_age_seconds", float(cur.fetchone()[0]))
            conn.commit()
            self.metrics.set("consumer_last_success_timestamp", time.time())
            self.metrics.set("consumer_last_batch_success_timestamp", time.time())
            return processed
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
            self.metrics.set("current_batch_in_progress", 0)
            self.metrics.set("consumer_batch_duration_seconds", time.monotonic() - started)

    def _persist(self, cur: Any, payload: dict[str, Any], expected_semantic_digest: str) -> None:
        keys = ("event_id","decision_key","decision_created_at","environment","deployment_id",
                "strategy","symbol","interval","slot_key","regime","regime_confidence","action",
                "direction","confidence","quantity_intent","entry_intent","stop_loss_intent",
                "take_profit_intent","exit_intent","execution_eligible","decision_reason",
                "decision_payload_hash","source_service","source_instance","decision_kind","schema_version")
        semantic = stable_hash({name: payload.get(name) for name in (
            "strategy","symbol","interval","action","direction","confidence","quantity_intent",
            "entry_intent","stop_loss_intent","take_profit_intent","exit_intent","execution_eligible")})
        event_digest = stable_hash(payload)
        if semantic != expected_semantic_digest:
            raise ValueError("SEMANTIC_DIGEST_MISMATCH")
        cur.execute("""INSERT INTO causal_decision_observation_v1
                    (event_id,decision_key,decision_created_at,environment,deployment_id,strategy,symbol,interval,
                     slot_key,regime,regime_confidence,action,direction,confidence,quantity_intent,entry_intent,
                     stop_loss_intent,take_profit_intent,exit_intent,execution_eligible,decision_reason,
                     decision_payload_hash,semantic_digest,event_digest,source_service,source_instance,
                     decision_kind,schema_version,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (deployment_id,decision_key) DO NOTHING""",
                    tuple(payload.get(key) for key in keys[:22]) + (semantic, event_digest,) +
                    tuple(payload.get(key) for key in keys[22:]) + (datetime.now(timezone.utc),))
        cur.execute("""SELECT event_digest FROM causal_decision_observation_v1
                    WHERE deployment_id=%s AND decision_key=%s""",
                    (payload["deployment_id"], payload["decision_key"]))
        if cur.fetchone()[0] != event_digest:
            raise IdempotencyConflict("different observation payload for decision key")
        # Baseline-only projections. They carry no attribution or recommendation context.
        projection = json.dumps({"decision_kind": payload["decision_kind"], "action": payload["action"]})
        cur.execute("""INSERT INTO decision_replay_v1
                    (environment,decision_key,symbol,interval,strategy,replay_status,decision_vector,
                     deployment_id,observation_decision_key,causal_linkage_status)
                    VALUES (%s,%s,%s,%s,%s,'OBSERVATION_ONLY',%s::jsonb,%s,%s,'NO_ACTIVE_RECOMMENDATION')
                    ON CONFLICT DO NOTHING""", (payload["environment"], payload["decision_key"],
                    payload["symbol"], payload["interval"], payload["strategy"], projection,
                    payload["deployment_id"], payload["decision_key"]))
        cur.execute("""INSERT INTO learning_feature_warehouse_v1
                    (environment,decision_key,symbol,interval,strategy,evidence_status,raw_evidence,
                     deployment_id,observation_decision_key,causal_linkage_status)
                    VALUES (%s,%s,%s,%s,%s,'OBSERVATION_ONLY',%s::jsonb,%s,%s,'NO_ACTIVE_RECOMMENDATION')
                    ON CONFLICT DO NOTHING""", (payload["environment"], payload["decision_key"],
                    payload["symbol"], payload["interval"], payload["strategy"], projection,
                    payload["deployment_id"], payload["decision_key"]))

"""LEI1B committed entry submission and immutable ACK linkage.

The module deliberately keeps three boundaries separate:

* :class:`LiveEntryIntent` is immutable semantic intent evidence;
* :class:`EntrySubmissionAttempt` is an atomically claimed network attempt;
* :class:`EntryOrderAck` is immutable exchange acknowledgement evidence.

``EntrySubmissionRepository`` owns a fresh transaction for every write.  In
ENFORCE mode the high-level helper therefore cannot invoke the network callback
until both the intent and the single V1 attempt claim have returned from a
successful commit.  A retry which finds the attempt claim performs a client
order ID lookup and never blindly submits a second order.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

from common.contract_adoption import (
    contract_adoption_compatible,
    log_runtime_revision_provenance_diagnostic,
)
from common.entry_intent import (
    EntryIntentContractVersion,
    EntryIntentDeployment,
    EntryIntentEnvironment,
    EntryIntentInsertOutcome,
    EntryIntentOrderPurpose,
    EntryIntentSide,
    LiveEntryIntent,
    canonical_decimal,
)


ENTRY_SUBMISSION_MODE_ENV = "LIVE_ENTRY_SUBMISSION_MODE"
ENTRY_SUBMISSION_ATTEMPT_NAMESPACE = uuid.UUID(
    "ee694a91-0257-5949-a68f-50e58914c9c7"
)
ENTRY_ORDER_ACK_NAMESPACE = uuid.UUID(
    "8e0f4c10-e106-535c-9d5a-90f2babff229"
)


class EntrySubmissionMode(str, Enum):
    OFF = "OFF"
    SHADOW = "SHADOW"
    ENFORCE = "ENFORCE"

    @classmethod
    def from_env(
        cls, environment: Mapping[str, str] | None = None
    ) -> "EntrySubmissionMode":
        source = os.environ if environment is None else environment
        raw = str(source.get(ENTRY_SUBMISSION_MODE_ENV, "OFF")).strip().upper()
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError("LIVE_ENTRY_SUBMISSION_MODE_INVALID") from exc


class EntrySubmissionContractVersion(str, Enum):
    V1 = "LIVE_ENTRY_SUBMISSION_V1"


class EntryOrderAckContractVersion(str, Enum):
    V1 = "LIVE_ENTRY_ORDER_ACK_V1"


class SubmissionAttemptOutcome(str, Enum):
    CREATED = "CREATED"
    IDEMPOTENT_EXISTING = "IDEMPOTENT_EXISTING"
    CONFLICT = "CONFLICT"


class AckPersistOutcome(str, Enum):
    PERSISTED = "PERSISTED"
    IDEMPOTENT_EXISTING = "IDEMPOTENT_EXISTING"
    CONFLICT = "CONFLICT"


class ExchangeLookupOutcome(str, Enum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    AMBIGUOUS = "AMBIGUOUS"
    ERROR = "ERROR"


class EntrySubmissionEventType(str, Enum):
    ENTRY_INTENT_CREATED = "ENTRY_INTENT_CREATED"
    ENTRY_INTENT_IDEMPOTENT_EXISTING = "ENTRY_INTENT_IDEMPOTENT_EXISTING"
    ENTRY_INTENT_CONFLICT = "ENTRY_INTENT_CONFLICT"
    ENTRY_INTENT_COMMIT_FAILED = "ENTRY_INTENT_COMMIT_FAILED"
    ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT = (
        "ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT"
    )
    ENTRY_SUBMISSION_ATTEMPTED = "ENTRY_SUBMISSION_ATTEMPTED"
    ENTRY_ACK_PERSISTED = "ENTRY_ACK_PERSISTED"
    ENTRY_ACK_RECOVERED_BY_CLIENT_ORDER_ID = (
        "ENTRY_ACK_RECOVERED_BY_CLIENT_ORDER_ID"
    )
    ENTRY_ACK_CONFLICT = "ENTRY_ACK_CONFLICT"
    ENTRY_SUBMISSION_AMBIGUOUS = "ENTRY_SUBMISSION_AMBIGUOUS"


class EntrySubmissionExecutionOutcome(str, Enum):
    OFF_NETWORK_SUBMITTED = "OFF_NETWORK_SUBMITTED"
    SHADOW_NETWORK_SUBMITTED = "SHADOW_NETWORK_SUBMITTED"
    ACK_PERSISTED = "ACK_PERSISTED"
    ACK_ALREADY_PERSISTED = "ACK_ALREADY_PERSISTED"
    ACK_RECOVERED = "ACK_RECOVERED"
    BLOCKED_INTENT_CONFLICT = "BLOCKED_INTENT_CONFLICT"
    BLOCKED_INTENT_COMMIT_FAILED = "BLOCKED_INTENT_COMMIT_FAILED"
    BLOCKED_INTENT_COMMIT_UNKNOWN = "BLOCKED_INTENT_COMMIT_UNKNOWN"
    BLOCKED_ATTEMPT_CONFLICT = "BLOCKED_ATTEMPT_CONFLICT"
    BLOCKED_ATTEMPT_COMMIT_FAILED = "BLOCKED_ATTEMPT_COMMIT_FAILED"
    BLOCKED_ATTEMPT_COMMIT_UNKNOWN = "BLOCKED_ATTEMPT_COMMIT_UNKNOWN"
    BLOCKED_ACK_CONFLICT = "BLOCKED_ACK_CONFLICT"
    BLOCKED_ACK_PERSISTENCE_FAILED = "BLOCKED_ACK_PERSISTENCE_FAILED"
    BLOCKED_ACK_PERSISTENCE_UNKNOWN = "BLOCKED_ACK_PERSISTENCE_UNKNOWN"
    RECOVERY_NOT_FOUND = "RECOVERY_NOT_FOUND"
    RECOVERY_AMBIGUOUS = "RECOVERY_AMBIGUOUS"
    RECOVERY_ERROR = "RECOVERY_ERROR"


class EntrySubmissionRepositoryError(RuntimeError):
    """Base class for fail-closed LEI1B repository errors."""


class IntentCommitFailed(EntrySubmissionRepositoryError):
    pass


class IntentCommitOutcomeUnknown(EntrySubmissionRepositoryError):
    pass


class SubmissionAttemptCommitFailed(EntrySubmissionRepositoryError):
    pass


class SubmissionAttemptCommitOutcomeUnknown(EntrySubmissionRepositoryError):
    pass


class AckPersistenceFailed(EntrySubmissionRepositoryError):
    pass


class AckPersistenceOutcomeUnknown(EntrySubmissionRepositoryError):
    pass


class ActiveAdoptionResolutionError(EntrySubmissionRepositoryError):
    pass


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("ascii")).hexdigest()


def _nonempty(value: object, field: str) -> str:
    text = str(value)
    if not text or text != text.strip():
        raise ValueError(f"{field} must be nonempty and have no outer whitespace")
    return text


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _git_revision(value: str) -> str:
    revision = _nonempty(value, "git_revision")
    if len(revision) != 40 or any(
        char not in "0123456789abcdef" for char in revision
    ):
        raise ValueError("git_revision must be 40 lowercase hex characters")
    return revision


@dataclass(frozen=True, slots=True)
class EntrySubmissionAttempt:
    submission_attempt_id: uuid.UUID
    intent_id: uuid.UUID
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    adoption_id: int
    generation: int
    git_revision: str
    client_order_id: str
    exchange_source: str
    symbol: str
    strategy: str
    interval: str
    order_purpose: EntryIntentOrderPurpose
    side: EntryIntentSide
    requested_qty: Decimal
    attempt_ordinal: int
    submission_fingerprint: str
    submitted_at: datetime
    producer_identity: str
    contract_version: EntrySubmissionContractVersion

    @staticmethod
    def deterministic_id(intent_id: uuid.UUID, attempt_ordinal: int = 1) -> uuid.UUID:
        if not isinstance(intent_id, uuid.UUID):
            raise ValueError("intent_id must be UUID")
        if int(attempt_ordinal) <= 0:
            raise ValueError("attempt_ordinal must be positive")
        return uuid.uuid5(
            ENTRY_SUBMISSION_ATTEMPT_NAMESPACE,
            f"{intent_id}:{int(attempt_ordinal)}:{EntrySubmissionContractVersion.V1.value}",
        )

    @staticmethod
    def calculate_fingerprint(
        intent: LiveEntryIntent,
        *,
        attempt_ordinal: int,
        producer_identity: str,
    ) -> str:
        return _fingerprint(
            {
                "adoption_id": intent.adoption_id,
                "attempt_ordinal": int(attempt_ordinal),
                "client_order_id": intent.client_order_id,
                "contract_version": EntrySubmissionContractVersion.V1.value,
                "deployment_id": intent.deployment_id.value,
                "environment": intent.environment.value,
                "exchange_source": intent.exchange_source,
                "generation": intent.generation,
                "git_revision": intent.git_revision,
                "intent_content_fingerprint": intent.content_fingerprint,
                "intent_id": str(intent.intent_id),
                "interval": intent.interval,
                "order_purpose": intent.order_purpose.value,
                "producer_identity": producer_identity,
                "requested_qty": canonical_decimal(intent.requested_qty),
                "side": intent.side.value,
                "strategy": intent.strategy,
                "symbol": intent.symbol,
            }
        )

    @classmethod
    def build(
        cls,
        intent: LiveEntryIntent,
        *,
        submitted_at: datetime,
        producer_identity: str,
        attempt_ordinal: int = 1,
    ) -> "EntrySubmissionAttempt":
        ordinal = int(attempt_ordinal)
        producer = _nonempty(producer_identity, "producer_identity")
        return cls(
            submission_attempt_id=cls.deterministic_id(intent.intent_id, ordinal),
            intent_id=intent.intent_id,
            environment=intent.environment,
            deployment_id=intent.deployment_id,
            adoption_id=intent.adoption_id,
            generation=intent.generation,
            git_revision=intent.git_revision,
            client_order_id=intent.client_order_id,
            exchange_source=intent.exchange_source,
            symbol=intent.symbol,
            strategy=intent.strategy,
            interval=intent.interval,
            order_purpose=intent.order_purpose,
            side=intent.side,
            requested_qty=intent.requested_qty,
            attempt_ordinal=ordinal,
            submission_fingerprint=cls.calculate_fingerprint(
                intent,
                attempt_ordinal=ordinal,
                producer_identity=producer,
            ),
            submitted_at=_aware(submitted_at, "submitted_at"),
            producer_identity=producer,
            contract_version=EntrySubmissionContractVersion.V1,
        )

    def __post_init__(self) -> None:
        EntryIntentEnvironment(self.environment)
        EntryIntentDeployment(self.deployment_id)
        EntryIntentOrderPurpose(self.order_purpose)
        EntryIntentSide(self.side)
        EntrySubmissionContractVersion(self.contract_version)
        _git_revision(self.git_revision)
        _nonempty(self.client_order_id, "client_order_id")
        _nonempty(self.exchange_source, "exchange_source")
        _nonempty(self.symbol, "symbol")
        _nonempty(self.strategy, "strategy")
        _nonempty(self.interval, "interval")
        _nonempty(self.producer_identity, "producer_identity")
        _aware(self.submitted_at, "submitted_at")
        if int(self.adoption_id) <= 0 or int(self.generation) <= 0:
            raise ValueError("adoption_id and generation must be positive")
        if int(self.attempt_ordinal) <= 0:
            raise ValueError("attempt_ordinal must be positive")
        if Decimal(canonical_decimal(self.requested_qty)) <= 0:
            raise ValueError("requested_qty must be positive")
        if self.submission_attempt_id != self.deterministic_id(
            self.intent_id, self.attempt_ordinal
        ):
            raise ValueError("submission_attempt_id does not match identity")
        if len(self.submission_fingerprint) != 64 or any(
            char not in "0123456789abcdef"
            for char in self.submission_fingerprint
        ):
            raise ValueError("submission_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class EntryOrderAck:
    ack_id: uuid.UUID
    submission_attempt_id: uuid.UUID
    intent_id: uuid.UUID
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    adoption_id: int
    generation: int
    git_revision: str
    client_order_id: str
    exchange_source: str
    exchange_order_id: str
    exchange_order_status: str
    symbol: str
    strategy: str
    interval: str
    order_purpose: EntryIntentOrderPurpose
    side: EntryIntentSide
    requested_qty: Decimal
    ack_fingerprint: str
    acknowledged_at: datetime
    recovered_by_client_order_id: bool
    producer_identity: str
    contract_version: EntryOrderAckContractVersion

    @staticmethod
    def deterministic_id(intent_id: uuid.UUID) -> uuid.UUID:
        if not isinstance(intent_id, uuid.UUID):
            raise ValueError("intent_id must be UUID")
        return uuid.uuid5(
            ENTRY_ORDER_ACK_NAMESPACE,
            f"{intent_id}:{EntryOrderAckContractVersion.V1.value}",
        )

    @staticmethod
    def calculate_fingerprint(
        intent: LiveEntryIntent,
        attempt: EntrySubmissionAttempt,
        *,
        exchange_order_id: str,
        exchange_order_status: str,
    ) -> str:
        return _fingerprint(
            {
                "ack_contract_version": EntryOrderAckContractVersion.V1.value,
                "adoption_id": intent.adoption_id,
                "client_order_id": intent.client_order_id,
                "deployment_id": intent.deployment_id.value,
                "environment": intent.environment.value,
                "exchange_order_id": exchange_order_id,
                "exchange_order_status": exchange_order_status,
                "exchange_source": intent.exchange_source,
                "generation": intent.generation,
                "git_revision": intent.git_revision,
                "intent_content_fingerprint": intent.content_fingerprint,
                "intent_id": str(intent.intent_id),
                "interval": intent.interval,
                "order_purpose": intent.order_purpose.value,
                "requested_qty": canonical_decimal(intent.requested_qty),
                "side": intent.side.value,
                "strategy": intent.strategy,
                "submission_attempt_id": str(attempt.submission_attempt_id),
                "symbol": intent.symbol,
            }
        )

    @classmethod
    def build(
        cls,
        intent: LiveEntryIntent,
        attempt: EntrySubmissionAttempt,
        *,
        exchange_order_id: str,
        exchange_order_status: str,
        acknowledged_at: datetime,
        producer_identity: str,
        recovered_by_client_order_id: bool = False,
    ) -> "EntryOrderAck":
        attempt_matches_intent = (
            attempt.intent_id == intent.intent_id
            and attempt.environment == intent.environment
            and attempt.deployment_id == intent.deployment_id
            and attempt.adoption_id == intent.adoption_id
            and attempt.generation == intent.generation
            and attempt.git_revision == intent.git_revision
            and attempt.client_order_id == intent.client_order_id
            and attempt.exchange_source == intent.exchange_source
            and attempt.symbol == intent.symbol
            and attempt.strategy == intent.strategy
            and attempt.interval == intent.interval
            and attempt.order_purpose == intent.order_purpose
            and attempt.side == intent.side
            and canonical_decimal(attempt.requested_qty)
            == canonical_decimal(intent.requested_qty)
            and attempt.submission_attempt_id
            == EntrySubmissionAttempt.deterministic_id(
                intent.intent_id, attempt.attempt_ordinal
            )
            and attempt.submission_fingerprint
            == EntrySubmissionAttempt.calculate_fingerprint(
                intent,
                attempt_ordinal=attempt.attempt_ordinal,
                producer_identity=attempt.producer_identity,
            )
        )
        if not attempt_matches_intent:
            raise ValueError("submission attempt does not match intent")
        order_id = _nonempty(exchange_order_id, "exchange_order_id")
        order_status = _nonempty(
            exchange_order_status, "exchange_order_status"
        ).upper()
        producer = _nonempty(producer_identity, "producer_identity")
        return cls(
            ack_id=cls.deterministic_id(intent.intent_id),
            submission_attempt_id=attempt.submission_attempt_id,
            intent_id=intent.intent_id,
            environment=intent.environment,
            deployment_id=intent.deployment_id,
            adoption_id=intent.adoption_id,
            generation=intent.generation,
            git_revision=intent.git_revision,
            client_order_id=intent.client_order_id,
            exchange_source=intent.exchange_source,
            exchange_order_id=order_id,
            exchange_order_status=order_status,
            symbol=intent.symbol,
            strategy=intent.strategy,
            interval=intent.interval,
            order_purpose=intent.order_purpose,
            side=intent.side,
            requested_qty=intent.requested_qty,
            ack_fingerprint=cls.calculate_fingerprint(
                intent,
                attempt,
                exchange_order_id=order_id,
                exchange_order_status=order_status,
            ),
            acknowledged_at=_aware(acknowledged_at, "acknowledged_at"),
            recovered_by_client_order_id=bool(recovered_by_client_order_id),
            producer_identity=producer,
            contract_version=EntryOrderAckContractVersion.V1,
        )

    def __post_init__(self) -> None:
        EntryIntentEnvironment(self.environment)
        EntryIntentDeployment(self.deployment_id)
        EntryIntentOrderPurpose(self.order_purpose)
        EntryIntentSide(self.side)
        EntryOrderAckContractVersion(self.contract_version)
        _git_revision(self.git_revision)
        for field, value in (
            ("client_order_id", self.client_order_id),
            ("exchange_source", self.exchange_source),
            ("exchange_order_id", self.exchange_order_id),
            ("exchange_order_status", self.exchange_order_status),
            ("symbol", self.symbol),
            ("strategy", self.strategy),
            ("interval", self.interval),
            ("producer_identity", self.producer_identity),
        ):
            _nonempty(value, field)
        _aware(self.acknowledged_at, "acknowledged_at")
        if self.exchange_order_status != self.exchange_order_status.upper():
            raise ValueError("exchange_order_status must be canonical uppercase")
        if int(self.adoption_id) <= 0 or int(self.generation) <= 0:
            raise ValueError("adoption_id and generation must be positive")
        if Decimal(canonical_decimal(self.requested_qty)) <= 0:
            raise ValueError("requested_qty must be positive")
        if self.ack_id != self.deterministic_id(self.intent_id):
            raise ValueError("ack_id does not match identity")
        if len(self.ack_fingerprint) != 64 or any(
            char not in "0123456789abcdef" for char in self.ack_fingerprint
        ):
            raise ValueError("ack_fingerprint must be lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class ExchangeOrderObservation:
    exchange_order_id: str
    exchange_order_status: str
    acknowledged_at: datetime

    def __post_init__(self) -> None:
        _nonempty(self.exchange_order_id, "exchange_order_id")
        _nonempty(self.exchange_order_status, "exchange_order_status")
        _aware(self.acknowledged_at, "acknowledged_at")


@dataclass(frozen=True, slots=True)
class ExchangeOrderLookupResult:
    outcome: ExchangeLookupOutcome
    order: ExchangeOrderObservation | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        outcome = ExchangeLookupOutcome(self.outcome)
        if (outcome is ExchangeLookupOutcome.FOUND) != (self.order is not None):
            raise ValueError("FOUND lookup must contain exactly one order")


@dataclass(frozen=True, slots=True)
class EntrySubmissionEvent:
    event_type: EntrySubmissionEventType
    intent_id: uuid.UUID
    client_order_id: str
    occurred_at: datetime
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class EntrySubmissionResult:
    outcome: EntrySubmissionExecutionOutcome
    events: tuple[EntrySubmissionEvent, ...]
    network_called: bool
    recovery_lookup_performed: bool
    ack: EntryOrderAck | None = None
    raw_response: Any = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class ActiveEntrySubmissionAdoption:
    adoption_id: int
    generation: int
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    git_revision: str
    runtime_git_revision: str | None = None
    runtime_revision_matches_adoption_provenance: bool = True


class EntrySubmissionRepositoryProtocol(Protocol):
    def resolve_active_adoption(
        self,
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        runtime_git_revision: str,
    ) -> ActiveEntrySubmissionAdoption: ...

    def commit_intent(
        self, intent: LiveEntryIntent
    ) -> EntryIntentInsertOutcome: ...

    def record_submission_attempt(
        self, attempt: EntrySubmissionAttempt
    ) -> SubmissionAttemptOutcome: ...

    def persist_ack(self, ack: EntryOrderAck) -> AckPersistOutcome: ...

    def load_submission_attempt(
        self, intent_id: uuid.UUID, attempt_ordinal: int = 1
    ) -> EntrySubmissionAttempt | None: ...

    def load_ack(self, intent_id: uuid.UUID) -> EntryOrderAck | None: ...


class EntrySubmissionRepository:
    """PostgreSQL repository with one committed transaction per operation."""

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    @staticmethod
    def _cleanup(conn: Any, cur: Any, *, rollback: bool) -> None:
        if rollback:
            try:
                conn.rollback()
            except BaseException:
                pass
        try:
            cur.close()
        except BaseException:
            pass
        try:
            conn.close()
        except BaseException:
            pass

    def resolve_active_adoption(
        self,
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        runtime_git_revision: str,
    ) -> ActiveEntrySubmissionAdoption:
        environment_value = EntryIntentEnvironment(environment)
        deployment_value = EntryIntentDeployment(deployment_id)
        revision = _git_revision(runtime_git_revision)
        if deployment_value.value.split("-", 1)[1] != environment_value.value:
            raise ActiveAdoptionResolutionError(
                "ENTRY_SUBMISSION_ADOPTION_DEPLOYMENT_MISMATCH"
            )
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT adoption_id,contract_name,environment,deployment_id,
                       generation,status,git_revision
                FROM runtime_contract_adoption_v2
                WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
                  AND environment=%s AND deployment_id=%s AND status='ACTIVE'
                ORDER BY adoption_id
                """,
                (environment_value.value, deployment_value.value),
            )
            rows = list(cur.fetchall())
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise ActiveAdoptionResolutionError(
                "ENTRY_SUBMISSION_ADOPTION_LOOKUP_FAILED"
            ) from exc
        self._cleanup(conn, cur, rollback=True)
        if len(rows) != 1:
            raise ActiveAdoptionResolutionError(
                "ENTRY_SUBMISSION_ACTIVE_ADOPTION_NOT_UNIQUE"
            )
        (
            adoption_id,
            contract_name,
            adoption_environment,
            adoption_deployment_id,
            generation,
            status,
            adoption_git_revision,
        ) = rows[0]
        if not contract_adoption_compatible(
            contract_name=contract_name,
            environment=adoption_environment,
            deployment_id=adoption_deployment_id,
            status=status,
            generation=generation,
            expected_environment=environment_value.value,
            expected_deployment_id=deployment_value.value,
        ):
            raise ActiveAdoptionResolutionError(
                "ENTRY_SUBMISSION_ACTIVE_ADOPTION_INCOMPATIBLE"
            )
        revision_matches = log_runtime_revision_provenance_diagnostic(
            adoption_id=int(adoption_id),
            generation=int(generation),
            adoption_git_revision=str(adoption_git_revision),
            runtime_git_revision=revision,
        )
        return ActiveEntrySubmissionAdoption(
            adoption_id=int(adoption_id),
            generation=int(generation),
            environment=environment_value,
            deployment_id=deployment_value,
            git_revision=str(adoption_git_revision),
            runtime_git_revision=revision,
            runtime_revision_matches_adoption_provenance=revision_matches,
        )

    def commit_intent(
        self, intent: LiveEntryIntent
    ) -> EntryIntentInsertOutcome:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO live_entry_intents_v1(
                  intent_id,environment,deployment_id,git_revision,adoption_id,
                  generation,decision_id,symbol,strategy,"interval",slot_identity,
                  exchange_source,client_order_id,order_purpose,side,requested_qty,
                  content_fingerprint,prepared_at,producer_identity,contract_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT DO NOTHING
                RETURNING intent_id,content_fingerprint
                """,
                (
                    str(intent.intent_id),
                    intent.environment.value,
                    intent.deployment_id.value,
                    intent.git_revision,
                    intent.adoption_id,
                    intent.generation,
                    str(intent.decision_id),
                    intent.symbol,
                    intent.strategy,
                    intent.interval,
                    intent.slot_identity,
                    intent.exchange_source,
                    intent.client_order_id,
                    intent.order_purpose.value,
                    intent.side.value,
                    str(intent.requested_qty),
                    intent.content_fingerprint,
                    intent.prepared_at,
                    intent.producer_identity,
                    intent.contract_version.value,
                ),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                outcome = EntryIntentInsertOutcome.CREATED
            else:
                cur.execute(
                    """
                    SELECT intent_id,content_fingerprint
                    FROM live_entry_intents_v1
                    WHERE environment=%s AND deployment_id=%s
                      AND exchange_source=%s AND client_order_id=%s
                    """,
                    intent.natural_key,
                )
                existing = cur.fetchone()
                outcome = (
                    EntryIntentInsertOutcome.IDEMPOTENT_EXISTING
                    if existing is not None
                    and uuid.UUID(str(existing[0])) == intent.intent_id
                    and str(existing[1]) == intent.content_fingerprint
                    else EntryIntentInsertOutcome.CONFLICT
                )
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise IntentCommitFailed("ENTRY_INTENT_COMMIT_FAILED") from exc
        try:
            conn.commit()
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise IntentCommitOutcomeUnknown(
                "ENTRY_INTENT_COMMIT_OUTCOME_UNKNOWN"
            ) from exc
        self._cleanup(conn, cur, rollback=False)
        return outcome

    def record_submission_attempt(
        self, attempt: EntrySubmissionAttempt
    ) -> SubmissionAttemptOutcome:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO live_entry_submissions_v1(
                  submission_attempt_id,intent_id,environment,deployment_id,
                  adoption_id,generation,git_revision,client_order_id,
                  exchange_source,symbol,strategy,"interval",order_purpose,side,
                  requested_qty,attempt_ordinal,submission_fingerprint,
                  submitted_at,producer_identity,contract_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT DO NOTHING
                RETURNING submission_attempt_id,submission_fingerprint
                """,
                (
                    str(attempt.submission_attempt_id),
                    str(attempt.intent_id),
                    attempt.environment.value,
                    attempt.deployment_id.value,
                    attempt.adoption_id,
                    attempt.generation,
                    attempt.git_revision,
                    attempt.client_order_id,
                    attempt.exchange_source,
                    attempt.symbol,
                    attempt.strategy,
                    attempt.interval,
                    attempt.order_purpose.value,
                    attempt.side.value,
                    str(attempt.requested_qty),
                    attempt.attempt_ordinal,
                    attempt.submission_fingerprint,
                    attempt.submitted_at,
                    attempt.producer_identity,
                    attempt.contract_version.value,
                ),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                outcome = SubmissionAttemptOutcome.CREATED
            else:
                cur.execute(
                    """
                    SELECT submission_attempt_id,submission_fingerprint
                    FROM live_entry_submissions_v1
                    WHERE intent_id=%s AND attempt_ordinal=%s
                    """,
                    (str(attempt.intent_id), attempt.attempt_ordinal),
                )
                existing = cur.fetchone()
                outcome = (
                    SubmissionAttemptOutcome.IDEMPOTENT_EXISTING
                    if existing is not None
                    and uuid.UUID(str(existing[0]))
                    == attempt.submission_attempt_id
                    and str(existing[1]) == attempt.submission_fingerprint
                    else SubmissionAttemptOutcome.CONFLICT
                )
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise SubmissionAttemptCommitFailed(
                "ENTRY_SUBMISSION_ATTEMPT_COMMIT_FAILED"
            ) from exc
        try:
            conn.commit()
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise SubmissionAttemptCommitOutcomeUnknown(
                "ENTRY_SUBMISSION_ATTEMPT_COMMIT_OUTCOME_UNKNOWN"
            ) from exc
        self._cleanup(conn, cur, rollback=False)
        return outcome

    def persist_ack(self, ack: EntryOrderAck) -> AckPersistOutcome:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO live_entry_order_acks_v1(
                  ack_id,submission_attempt_id,intent_id,environment,deployment_id,
                  adoption_id,generation,git_revision,client_order_id,
                  exchange_source,exchange_order_id,exchange_order_status,symbol,
                  strategy,"interval",order_purpose,side,requested_qty,
                  ack_fingerprint,acknowledged_at,recovered_by_client_order_id,
                  producer_identity,contract_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  %s,%s,%s
                )
                ON CONFLICT DO NOTHING
                RETURNING ack_id,ack_fingerprint
                """,
                (
                    str(ack.ack_id),
                    str(ack.submission_attempt_id),
                    str(ack.intent_id),
                    ack.environment.value,
                    ack.deployment_id.value,
                    ack.adoption_id,
                    ack.generation,
                    ack.git_revision,
                    ack.client_order_id,
                    ack.exchange_source,
                    ack.exchange_order_id,
                    ack.exchange_order_status,
                    ack.symbol,
                    ack.strategy,
                    ack.interval,
                    ack.order_purpose.value,
                    ack.side.value,
                    str(ack.requested_qty),
                    ack.ack_fingerprint,
                    ack.acknowledged_at,
                    ack.recovered_by_client_order_id,
                    ack.producer_identity,
                    ack.contract_version.value,
                ),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                outcome = AckPersistOutcome.PERSISTED
            else:
                cur.execute(
                    """
                    SELECT ack_id,ack_fingerprint
                    FROM live_entry_order_acks_v1
                    WHERE intent_id=%s
                    """,
                    (str(ack.intent_id),),
                )
                existing = cur.fetchone()
                outcome = (
                    AckPersistOutcome.IDEMPOTENT_EXISTING
                    if existing is not None
                    and uuid.UUID(str(existing[0])) == ack.ack_id
                    and str(existing[1]) == ack.ack_fingerprint
                    else AckPersistOutcome.CONFLICT
                )
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise AckPersistenceFailed("ENTRY_ACK_PERSISTENCE_FAILED") from exc
        try:
            conn.commit()
        except BaseException as exc:
            self._cleanup(conn, cur, rollback=True)
            raise AckPersistenceOutcomeUnknown(
                "ENTRY_ACK_PERSISTENCE_OUTCOME_UNKNOWN"
            ) from exc
        self._cleanup(conn, cur, rollback=False)
        return outcome

    def load_submission_attempt(
        self, intent_id: uuid.UUID, attempt_ordinal: int = 1
    ) -> EntrySubmissionAttempt | None:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT submission_attempt_id,intent_id,environment,deployment_id,
                       adoption_id,generation,git_revision,client_order_id,
                       exchange_source,symbol,strategy,"interval",order_purpose,
                       side,requested_qty,attempt_ordinal,submission_fingerprint,
                       submitted_at,producer_identity,contract_version
                FROM live_entry_submissions_v1
                WHERE intent_id=%s AND attempt_ordinal=%s
                """,
                (str(intent_id), int(attempt_ordinal)),
            )
            row = cur.fetchone()
        finally:
            self._cleanup(conn, cur, rollback=True)
        if row is None:
            return None
        return EntrySubmissionAttempt(
            submission_attempt_id=uuid.UUID(str(row[0])),
            intent_id=uuid.UUID(str(row[1])),
            environment=EntryIntentEnvironment(row[2]),
            deployment_id=EntryIntentDeployment(row[3]),
            adoption_id=int(row[4]),
            generation=int(row[5]),
            git_revision=str(row[6]),
            client_order_id=str(row[7]),
            exchange_source=str(row[8]),
            symbol=str(row[9]),
            strategy=str(row[10]),
            interval=str(row[11]),
            order_purpose=EntryIntentOrderPurpose(row[12]),
            side=EntryIntentSide(row[13]),
            requested_qty=Decimal(str(row[14])),
            attempt_ordinal=int(row[15]),
            submission_fingerprint=str(row[16]),
            submitted_at=row[17],
            producer_identity=str(row[18]),
            contract_version=EntrySubmissionContractVersion(row[19]),
        )

    def load_ack(self, intent_id: uuid.UUID) -> EntryOrderAck | None:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT ack_id,submission_attempt_id,intent_id,environment,
                       deployment_id,adoption_id,generation,git_revision,
                       client_order_id,exchange_source,exchange_order_id,
                       exchange_order_status,symbol,strategy,"interval",
                       order_purpose,side,requested_qty,ack_fingerprint,
                       acknowledged_at,recovered_by_client_order_id,
                       producer_identity,contract_version
                FROM live_entry_order_acks_v1 WHERE intent_id=%s
                """,
                (str(intent_id),),
            )
            row = cur.fetchone()
        finally:
            self._cleanup(conn, cur, rollback=True)
        if row is None:
            return None
        return EntryOrderAck(
            ack_id=uuid.UUID(str(row[0])),
            submission_attempt_id=uuid.UUID(str(row[1])),
            intent_id=uuid.UUID(str(row[2])),
            environment=EntryIntentEnvironment(row[3]),
            deployment_id=EntryIntentDeployment(row[4]),
            adoption_id=int(row[5]),
            generation=int(row[6]),
            git_revision=str(row[7]),
            client_order_id=str(row[8]),
            exchange_source=str(row[9]),
            exchange_order_id=str(row[10]),
            exchange_order_status=str(row[11]),
            symbol=str(row[12]),
            strategy=str(row[13]),
            interval=str(row[14]),
            order_purpose=EntryIntentOrderPurpose(row[15]),
            side=EntryIntentSide(row[16]),
            requested_qty=Decimal(str(row[17])),
            ack_fingerprint=str(row[18]),
            acknowledged_at=row[19],
            recovered_by_client_order_id=bool(row[20]),
            producer_identity=str(row[21]),
            contract_version=EntryOrderAckContractVersion(row[22]),
        )


def normalize_exchange_order_observation(
    value: Any,
    *,
    observed_at: datetime | None = None,
) -> ExchangeOrderObservation:
    if isinstance(value, ExchangeOrderObservation):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("ENTRY_EXCHANGE_ACK_INVALID")
    candidate: Mapping[str, Any] = value
    for key in ("order", "resp", "response"):
        nested = candidate.get(key)
        if isinstance(nested, Mapping):
            candidate = nested
            break
    data = candidate.get("data")
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], Mapping):
        candidate = data[0]
    order_id = next(
        (
            candidate[key]
            for key in ("exchange_order_id", "order_id", "orderId", "ordId")
            if candidate.get(key) not in (None, "")
        ),
        None,
    )
    status = next(
        (
            candidate[key]
            for key in ("exchange_order_status", "status", "state", "ordStatus")
            if candidate.get(key) not in (None, "")
        ),
        "ACKNOWLEDGED",
    )
    if order_id is None:
        raise ValueError("ENTRY_EXCHANGE_ACK_ORDER_ID_MISSING")
    return ExchangeOrderObservation(
        exchange_order_id=str(order_id),
        exchange_order_status=str(status).upper(),
        acknowledged_at=_aware(
            observed_at or datetime.now(timezone.utc), "acknowledged_at"
        ),
    )


def normalize_exchange_lookup_result(
    value: Any,
    *,
    observed_at: datetime | None = None,
) -> ExchangeOrderLookupResult:
    if isinstance(value, ExchangeOrderLookupResult):
        return value
    if not isinstance(value, Mapping):
        return ExchangeOrderLookupResult(
            ExchangeLookupOutcome.ERROR, detail="ENTRY_EXCHANGE_LOOKUP_INVALID"
        )
    try:
        outcome = ExchangeLookupOutcome(str(value.get("outcome", "ERROR")).upper())
    except ValueError:
        outcome = ExchangeLookupOutcome.ERROR
    if outcome is not ExchangeLookupOutcome.FOUND:
        return ExchangeOrderLookupResult(
            outcome,
            detail=(
                str(value.get("detail") or value.get("error_message"))
                if value.get("detail") or value.get("error_message")
                else None
            ),
        )
    try:
        order = normalize_exchange_order_observation(
            value.get("order", value), observed_at=observed_at
        )
    except ValueError as exc:
        return ExchangeOrderLookupResult(
            ExchangeLookupOutcome.ERROR, detail=str(exc)
        )
    return ExchangeOrderLookupResult(ExchangeLookupOutcome.FOUND, order=order)


def execute_committed_entry_submission(
    *,
    mode: EntrySubmissionMode | str | None,
    intent: LiveEntryIntent,
    repository: EntrySubmissionRepositoryProtocol,
    network_submit: Callable[[], Any],
    lookup_by_client_order_id: Callable[..., Any],
    event_sink: Callable[[EntrySubmissionEvent], None] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> EntrySubmissionResult:
    """Execute one bounded LEI1B entry flow.

    ``OFF`` and ``SHADOW`` preserve legacy admission behavior.  Only ENFORCE
    claims the deterministic attempt and controls network admission.  V1 never
    retries a NOT_FOUND, AMBIGUOUS, or ERROR lookup automatically.
    """

    selected_mode = (
        EntrySubmissionMode.from_env()
        if mode is None
        else EntrySubmissionMode(str(mode).upper())
        if not isinstance(mode, EntrySubmissionMode)
        else mode
    )
    emitted: list[EntrySubmissionEvent] = []

    def emit(event_type: EntrySubmissionEventType, detail: str | None = None) -> None:
        event = EntrySubmissionEvent(
            event_type=event_type,
            intent_id=intent.intent_id,
            client_order_id=intent.client_order_id,
            occurred_at=_aware(clock(), "event occurred_at"),
            detail=detail,
        )
        emitted.append(event)
        if event_sink is not None:
            try:
                event_sink(event)
            except BaseException:
                # Event transport cannot move the commit/network boundary.  The
                # immutable ledgers remain the canonical admission evidence.
                pass

    def result(
        outcome: EntrySubmissionExecutionOutcome,
        *,
        network_called: bool,
        lookup: bool = False,
        ack: EntryOrderAck | None = None,
        raw: Any = None,
        error: str | None = None,
    ) -> EntrySubmissionResult:
        return EntrySubmissionResult(
            outcome=outcome,
            events=tuple(emitted),
            network_called=network_called,
            recovery_lookup_performed=lookup,
            ack=ack,
            raw_response=raw,
            error_code=error,
        )

    if selected_mode is EntrySubmissionMode.OFF:
        raw = network_submit()
        return result(
            EntrySubmissionExecutionOutcome.OFF_NETWORK_SUBMITTED,
            network_called=True,
            raw=raw,
        )

    if selected_mode is EntrySubmissionMode.SHADOW:
        try:
            shadow_outcome = repository.commit_intent(intent)
            emit(
                EntrySubmissionEventType.ENTRY_INTENT_CREATED
                if shadow_outcome is EntryIntentInsertOutcome.CREATED
                else EntrySubmissionEventType.ENTRY_INTENT_IDEMPOTENT_EXISTING
                if shadow_outcome is EntryIntentInsertOutcome.IDEMPOTENT_EXISTING
                else EntrySubmissionEventType.ENTRY_INTENT_CONFLICT
            )
        except BaseException as exc:
            emit(
                EntrySubmissionEventType.ENTRY_INTENT_COMMIT_FAILED,
                type(exc).__name__,
            )
        raw = network_submit()
        return result(
            EntrySubmissionExecutionOutcome.SHADOW_NETWORK_SUBMITTED,
            network_called=True,
            raw=raw,
        )

    try:
        intent_outcome = repository.commit_intent(intent)
    except IntentCommitOutcomeUnknown as exc:
        emit(EntrySubmissionEventType.ENTRY_INTENT_COMMIT_FAILED, str(exc))
        emit(
            EntrySubmissionEventType.ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT,
            "COMMIT_OUTCOME_UNKNOWN",
        )
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_INTENT_COMMIT_UNKNOWN,
            network_called=False,
            error=str(exc),
        )
    except BaseException as exc:
        emit(EntrySubmissionEventType.ENTRY_INTENT_COMMIT_FAILED, str(exc))
        emit(
            EntrySubmissionEventType.ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT,
            "COMMIT_FAILED",
        )
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_INTENT_COMMIT_FAILED,
            network_called=False,
            error=str(exc),
        )

    if intent_outcome is EntryIntentInsertOutcome.CONFLICT:
        emit(EntrySubmissionEventType.ENTRY_INTENT_CONFLICT)
        emit(
            EntrySubmissionEventType.ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT,
            "FINGERPRINT_CONFLICT",
        )
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_INTENT_CONFLICT,
            network_called=False,
            error="ENTRY_INTENT_CONFLICT",
        )
    emit(
        EntrySubmissionEventType.ENTRY_INTENT_CREATED
        if intent_outcome is EntryIntentInsertOutcome.CREATED
        else EntrySubmissionEventType.ENTRY_INTENT_IDEMPOTENT_EXISTING
    )

    try:
        existing_ack = repository.load_ack(intent.intent_id)
    except BaseException as exc:
        emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
        return result(
            EntrySubmissionExecutionOutcome.RECOVERY_ERROR,
            network_called=False,
            error=str(exc),
        )
    if existing_ack is not None:
        emit(EntrySubmissionEventType.ENTRY_ACK_PERSISTED, "IDEMPOTENT_EXISTING")
        return result(
            EntrySubmissionExecutionOutcome.ACK_ALREADY_PERSISTED,
            network_called=False,
            ack=existing_ack,
            raw={
                "orderId": existing_ack.exchange_order_id,
                "clientOrderId": existing_ack.client_order_id,
                "status": existing_ack.exchange_order_status,
            },
        )

    try:
        attempt = EntrySubmissionAttempt.build(
            intent,
            submitted_at=_aware(clock(), "submitted_at"),
            producer_identity=intent.producer_identity,
        )
    except BaseException as exc:
        emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_CONFLICT,
            network_called=False,
            error=str(exc),
        )
    if intent_outcome is EntryIntentInsertOutcome.IDEMPOTENT_EXISTING:
        # A committed intent with no ACK can represent a crash before send or
        # an uncertain send.  It must be reconciled by exact CID before any
        # process is allowed to claim/send.  This also prevents a concurrency
        # loser from racing the CREATED winner's attempt claim.
        attempt_outcome = SubmissionAttemptOutcome.IDEMPOTENT_EXISTING
    else:
        try:
            attempt_outcome = repository.record_submission_attempt(attempt)
        except SubmissionAttemptCommitOutcomeUnknown as exc:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_COMMIT_UNKNOWN,
                network_called=False,
                error=str(exc),
            )
        except BaseException as exc:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_COMMIT_FAILED,
                network_called=False,
                error=str(exc),
            )
    if attempt_outcome is SubmissionAttemptOutcome.CONFLICT:
        emit(
            EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS,
            "SUBMISSION_ATTEMPT_CONFLICT",
        )
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_CONFLICT,
            network_called=False,
            error="ENTRY_SUBMISSION_ATTEMPT_CONFLICT",
        )

    network_called = False

    def recover(
        error_detail: str | None = None,
        candidate_ack: EntryOrderAck | None = None,
    ) -> EntrySubmissionResult:
        try:
            raw_lookup = lookup_by_client_order_id(
                symbol=intent.symbol,
                client_order_id=intent.client_order_id,
            )
            lookup = normalize_exchange_lookup_result(
                raw_lookup, observed_at=_aware(clock(), "lookup observed_at")
            )
        except BaseException as exc:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
            return result(
                EntrySubmissionExecutionOutcome.RECOVERY_ERROR,
                network_called=network_called,
                lookup=True,
                error=str(exc),
            )
        if lookup.outcome is ExchangeLookupOutcome.NOT_FOUND:
            emit(
                EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS,
                "CID_NOT_FOUND_EXPLICIT_RETRY_REQUIRED",
            )
            return result(
                EntrySubmissionExecutionOutcome.RECOVERY_NOT_FOUND,
                network_called=network_called,
                lookup=True,
                error=error_detail or lookup.detail,
            )
        if lookup.outcome is ExchangeLookupOutcome.AMBIGUOUS:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, lookup.detail)
            return result(
                EntrySubmissionExecutionOutcome.RECOVERY_AMBIGUOUS,
                network_called=network_called,
                lookup=True,
                error=error_detail or lookup.detail,
            )
        if lookup.outcome is ExchangeLookupOutcome.ERROR or lookup.order is None:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, lookup.detail)
            return result(
                EntrySubmissionExecutionOutcome.RECOVERY_ERROR,
                network_called=network_called,
                lookup=True,
                error=error_detail or lookup.detail,
            )
        ack_attempt = attempt
        if intent_outcome is EntryIntentInsertOutcome.IDEMPOTENT_EXISTING:
            try:
                stored_attempt = repository.load_submission_attempt(
                    intent.intent_id, 1
                )
            except BaseException as exc:
                emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
                return result(
                    EntrySubmissionExecutionOutcome.RECOVERY_ERROR,
                    network_called=network_called,
                    lookup=True,
                    error=str(exc),
                )
            if stored_attempt is None:
                emit(
                    EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS,
                    "CID_FOUND_WITHOUT_COMMITTED_ATTEMPT",
                )
                return result(
                    EntrySubmissionExecutionOutcome.RECOVERY_ERROR,
                    network_called=network_called,
                    lookup=True,
                    error="CID_FOUND_WITHOUT_COMMITTED_ATTEMPT",
                )
            ack_attempt = stored_attempt
        if (
            candidate_ack is not None
            and candidate_ack.exchange_order_id
            != lookup.order.exchange_order_id
        ):
            emit(
                EntrySubmissionEventType.ENTRY_ACK_CONFLICT,
                "CID_LOOKUP_EXCHANGE_ORDER_ID_MISMATCH",
            )
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ACK_CONFLICT,
                network_called=network_called,
                lookup=True,
                error="ENTRY_ACK_CONFLICT",
            )
        try:
            recovered_ack = EntryOrderAck.build(
                intent,
                ack_attempt,
                exchange_order_id=lookup.order.exchange_order_id,
                exchange_order_status=(
                    candidate_ack.exchange_order_status
                    if candidate_ack is not None
                    else lookup.order.exchange_order_status
                ),
                acknowledged_at=lookup.order.acknowledged_at,
                producer_identity=intent.producer_identity,
                recovered_by_client_order_id=True,
            )
        except BaseException as exc:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_CONFLICT,
                network_called=network_called,
                lookup=True,
                error=str(exc),
            )
        try:
            ack_outcome = repository.persist_ack(recovered_ack)
        except AckPersistenceOutcomeUnknown as exc:
            emit(EntrySubmissionEventType.ENTRY_SUBMISSION_AMBIGUOUS, str(exc))
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ACK_PERSISTENCE_UNKNOWN,
                network_called=network_called,
                lookup=True,
                error=str(exc),
            )
        except BaseException as exc:
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ACK_PERSISTENCE_FAILED,
                network_called=network_called,
                lookup=True,
                error=str(exc),
            )
        if ack_outcome is AckPersistOutcome.CONFLICT:
            emit(EntrySubmissionEventType.ENTRY_ACK_CONFLICT)
            return result(
                EntrySubmissionExecutionOutcome.BLOCKED_ACK_CONFLICT,
                network_called=network_called,
                lookup=True,
                error="ENTRY_ACK_CONFLICT",
            )
        emit(EntrySubmissionEventType.ENTRY_ACK_RECOVERED_BY_CLIENT_ORDER_ID)
        emit(
            EntrySubmissionEventType.ENTRY_ACK_PERSISTED,
            ack_outcome.value,
        )
        return result(
            EntrySubmissionExecutionOutcome.ACK_RECOVERED,
            network_called=network_called,
            lookup=True,
            ack=recovered_ack,
            raw=raw_lookup,
        )

    if attempt_outcome is SubmissionAttemptOutcome.IDEMPOTENT_EXISTING:
        return recover("SUBMISSION_ATTEMPT_ALREADY_CLAIMED")

    emit(EntrySubmissionEventType.ENTRY_SUBMISSION_ATTEMPTED)
    network_called = True
    try:
        raw_response = network_submit()
        observation = normalize_exchange_order_observation(
            raw_response, observed_at=_aware(clock(), "acknowledged_at")
        )
    except BaseException as exc:
        return recover(str(exc))

    try:
        ack = EntryOrderAck.build(
            intent,
            attempt,
            exchange_order_id=observation.exchange_order_id,
            exchange_order_status=observation.exchange_order_status,
            acknowledged_at=observation.acknowledged_at,
            producer_identity=intent.producer_identity,
        )
    except BaseException as exc:
        emit(EntrySubmissionEventType.ENTRY_ACK_CONFLICT, str(exc))
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_ACK_CONFLICT,
            network_called=True,
            raw=raw_response,
            error=str(exc),
        )
    try:
        ack_outcome = repository.persist_ack(ack)
    except BaseException as exc:
        # A successful/uncertain send followed by missing persistence is always
        # reconciled by the original client order ID, never by a second send.
        # Preserve the originally observed ACK payload after the lookup proves
        # the same exchange order identity.  A later exchange status transition
        # must not turn an unknown commit result into a false ACK conflict.
        return recover(str(exc), candidate_ack=ack)
    if ack_outcome is AckPersistOutcome.CONFLICT:
        emit(EntrySubmissionEventType.ENTRY_ACK_CONFLICT)
        return result(
            EntrySubmissionExecutionOutcome.BLOCKED_ACK_CONFLICT,
            network_called=True,
            raw=raw_response,
            error="ENTRY_ACK_CONFLICT",
        )
    emit(EntrySubmissionEventType.ENTRY_ACK_PERSISTED, ack_outcome.value)
    return result(
        EntrySubmissionExecutionOutcome.ACK_PERSISTED,
        network_called=True,
        ack=ack,
        raw=raw_response,
    )

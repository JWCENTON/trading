"""LEI1C immutable exchange fill evidence and applied-state semantics.

This boundary deliberately stops before position projection.  It records an
authoritative source observation, attributes it only through exact order/CID
lineage, and appends an application decision separately.  Evidence commit and
application-decision commit use distinct transactions so a retry can recover
the intentional crash window between them.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Callable, Mapping, Protocol, Sequence

from common.entry_intent import (
    EntryIntentDeployment,
    EntryIntentEnvironment,
    canonical_decimal,
)


ENTRY_FILL_ATTRIBUTION_MODE_ENV = "LIVE_ENTRY_FILL_ATTRIBUTION_MODE"
ENTRY_FILL_EVIDENCE_NAMESPACE = uuid.UUID(
    "21d06534-b44a-51c1-9172-03060eebfce8"
)
ENTRY_FILL_APPLICATION_NAMESPACE = uuid.UUID(
    "ead9d1f6-26f7-5e31-bfd4-bd88834856af"
)


class EntryFillAttributionMode(str, Enum):
    OFF = "OFF"
    SHADOW = "SHADOW"
    ENFORCE = "ENFORCE"

    @classmethod
    def from_env(
        cls, environment: Mapping[str, str] | None = None
    ) -> "EntryFillAttributionMode":
        source = os.environ if environment is None else environment
        raw = str(
            source.get(ENTRY_FILL_ATTRIBUTION_MODE_ENV, "OFF")
        ).strip().upper()
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError("LIVE_ENTRY_FILL_ATTRIBUTION_MODE_INVALID") from exc


class FillEvidenceContractVersion(str, Enum):
    V1 = "LIVE_ENTRY_FILL_EVIDENCE_V1"


class FillApplicationContractVersion(str, Enum):
    V1 = "LIVE_ENTRY_FILL_APPLICATION_V1"


class FillEvidenceInsertOutcome(str, Enum):
    CREATED = "CREATED"
    IDEMPOTENT_EXISTING = "IDEMPOTENT_EXISTING"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


class FillApplicationInsertOutcome(str, Enum):
    CREATED = "CREATED"
    IDEMPOTENT_EXISTING = "IDEMPOTENT_EXISTING"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"


class FillAttributionStatus(str, Enum):
    BOT_OWNED_ATTRIBUTED = "BOT_OWNED_ATTRIBUTED"
    BOT_OWNED_MISSING_POSITION = "BOT_OWNED_MISSING_POSITION"
    BOT_OWNED_MISSING_LINEAGE = "BOT_OWNED_MISSING_LINEAGE"
    LEGACY_BOT_OWNED = "LEGACY_BOT_OWNED"
    EXTERNAL_OR_MANUAL_UNLINKED = "EXTERNAL_OR_MANUAL_UNLINKED"
    AMBIGUOUS = "AMBIGUOUS"
    CONFLICTED = "CONFLICTED"
    UNKNOWN = "UNKNOWN"


class FillApplicationStatus(str, Enum):
    OBSERVED_NOT_APPLIED = "OBSERVED_NOT_APPLIED"
    APPLIED = "APPLIED"
    TRUE_DUPLICATE_APPLIED = "TRUE_DUPLICATE_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    EXTERNAL_OR_MANUAL_UNLINKED = "EXTERNAL_OR_MANUAL_UNLINKED"
    AMBIGUOUS = "AMBIGUOUS"
    CORRECTION_PENDING = "CORRECTION_PENDING"


class EntryFillEventType(str, Enum):
    ENTRY_FILL_OBSERVED = "ENTRY_FILL_OBSERVED"
    ENTRY_FILL_EVIDENCE_CREATED = "ENTRY_FILL_EVIDENCE_CREATED"
    ENTRY_FILL_EVIDENCE_IDEMPOTENT = "ENTRY_FILL_EVIDENCE_IDEMPOTENT"
    ENTRY_FILL_ATTRIBUTED = "ENTRY_FILL_ATTRIBUTED"
    ENTRY_FILL_OBSERVED_NOT_APPLIED = "ENTRY_FILL_OBSERVED_NOT_APPLIED"
    ENTRY_FILL_TRUE_DUPLICATE_APPLIED = "ENTRY_FILL_TRUE_DUPLICATE_APPLIED"
    ENTRY_FILL_EXTERNAL_UNLINKED = "ENTRY_FILL_EXTERNAL_UNLINKED"
    ENTRY_FILL_AMBIGUOUS = "ENTRY_FILL_AMBIGUOUS"
    ENTRY_FILL_CONFLICT = "ENTRY_FILL_CONFLICT"
    ENTRY_FILL_CORRECTION_PENDING = "ENTRY_FILL_CORRECTION_PENDING"


class EntryFillProcessingOutcome(str, Enum):
    MODE_OFF = "MODE_OFF"
    EVIDENCE_RECORDED = "EVIDENCE_RECORDED"
    EVIDENCE_IDEMPOTENT = "EVIDENCE_IDEMPOTENT"
    TRUE_DUPLICATE_APPLIED = "TRUE_DUPLICATE_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    EXTERNAL_UNLINKED = "EXTERNAL_UNLINKED"
    AMBIGUOUS = "AMBIGUOUS"
    CORRECTION_PENDING = "CORRECTION_PENDING"
    REPOSITORY_ERROR = "REPOSITORY_ERROR"


class EntryFillRepositoryError(RuntimeError):
    pass


def _nonempty(value: object, field: str) -> str:
    if value is None:
        raise ValueError(f"{field} must be nonempty")
    rendered = str(value)
    if not rendered or rendered != rendered.strip():
        raise ValueError(f"{field} must be nonempty and have no outer whitespace")
    return rendered


def _optional_nonempty(value: object | None, field: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, field)


def _git_revision(value: object) -> str:
    revision = _nonempty(value, "git_revision")
    if len(revision) != 40 or any(
        char not in "0123456789abcdef" for char in revision
    ):
        raise ValueError("git_revision must be 40 lowercase hex characters")
    return revision


def _aware(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    if value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _canonical_timestamp(value: datetime) -> str:
    return (
        _aware(value, "timestamp")
        .astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _decimal(
    value: Decimal | str | int,
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field} must not use binary float")
    try:
        number = Decimal(canonical_decimal(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be Decimal-compatible") from exc
    if positive and number <= 0:
        raise ValueError(f"{field} must be positive")
    if nonnegative and number < 0:
        raise ValueError(f"{field} must be nonnegative")
    return number


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Decimal):
        return canonical_decimal(value)
    if isinstance(value, datetime):
        return _canonical_timestamp(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        raise ValueError("immutable source payload must not contain binary float")
    return str(value)


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        _json_ready(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("ascii")).hexdigest()


def exchange_wire_client_order_id(
    exchange_source: str, client_order_id: str | None
) -> str | None:
    """Return the exact deterministic wire identity used by the adapter."""
    if client_order_id is None:
        return None
    candidate = _nonempty(client_order_id, "client_order_id")
    if str(exchange_source).lower() == "okx":
        candidate = "".join(char for char in candidate if char.isalnum())[:32]
        return candidate or None
    return candidate


@dataclass(frozen=True, slots=True)
class EntryFillContractContext:
    """Contract generation that owns an exchange fill observation.

    New exchange activity normally uses the one active runtime adoption.  A
    fill may, however, arrive after the exact LEI1B ACK that caused it has been
    superseded.  In that case the immutable evidence must retain the ACK's
    historical adoption/generation/Git identity instead of being relabelled as
    activity from the currently active generation.
    """

    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    adoption_id: int
    generation: int
    git_revision: str

    def __post_init__(self) -> None:
        environment = EntryIntentEnvironment(self.environment)
        deployment = EntryIntentDeployment(self.deployment_id)
        if deployment.value.split("-", 1)[1] != environment.value:
            raise ValueError("deployment_id does not match environment")
        if int(self.adoption_id) <= 0 or int(self.generation) <= 0:
            raise ValueError("adoption_id and generation must be positive")
        _git_revision(self.git_revision)

    @classmethod
    def from_observation(
        cls, observation: "EntryFillObservation"
    ) -> "EntryFillContractContext":
        return cls(
            environment=observation.environment,
            deployment_id=observation.deployment_id,
            adoption_id=observation.adoption_id,
            generation=observation.generation,
            git_revision=observation.git_revision,
        )


@dataclass(frozen=True, slots=True)
class EntryFillObservation:
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    adoption_id: int
    generation: int
    git_revision: str
    exchange_source: str
    exchange_trade_id: str
    exchange_order_id: str
    wire_client_order_id: str | None
    symbol: str
    side: str
    executed_qty: Decimal
    price: Decimal
    notional: Decimal
    fee: Decimal
    fee_asset: str | None
    executed_at: datetime
    observed_at: datetime
    producer_identity: str
    source_payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        environment = EntryIntentEnvironment(self.environment)
        deployment = EntryIntentDeployment(self.deployment_id)
        if deployment.value.split("-", 1)[1] != environment.value:
            raise ValueError("deployment_id does not match environment")
        if int(self.adoption_id) <= 0 or int(self.generation) <= 0:
            raise ValueError("adoption_id and generation must be positive")
        _git_revision(self.git_revision)
        if self.exchange_source != _nonempty(
            self.exchange_source, "exchange_source"
        ).lower():
            raise ValueError("exchange_source must be canonical lowercase")
        _nonempty(self.exchange_trade_id, "exchange_trade_id")
        _nonempty(self.exchange_order_id, "exchange_order_id")
        _optional_nonempty(self.wire_client_order_id, "wire_client_order_id")
        if self.symbol != _nonempty(self.symbol, "symbol").upper():
            raise ValueError("symbol must be canonical uppercase")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        _decimal(self.executed_qty, "executed_qty", positive=True)
        _decimal(self.price, "price", positive=True)
        _decimal(self.notional, "notional", positive=True)
        _decimal(self.fee, "fee", nonnegative=True)
        if self.fee_asset is not None and self.fee_asset != _nonempty(
            self.fee_asset, "fee_asset"
        ).upper():
            raise ValueError("fee_asset must be canonical uppercase")
        _aware(self.executed_at, "executed_at")
        _aware(self.observed_at, "observed_at")
        _nonempty(self.producer_identity, "producer_identity")
        _canonical_json(dict(self.source_payload))

    @classmethod
    def build(
        cls,
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        adoption_id: int,
        generation: int,
        git_revision: str,
        exchange_source: str,
        exchange_trade_id: object,
        exchange_order_id: object,
        client_order_id: str | None,
        symbol: str,
        side: str,
        executed_qty: Decimal | str | int,
        price: Decimal | str | int,
        notional: Decimal | str | int | None = None,
        fee: Decimal | str | int = Decimal("0"),
        fee_asset: str | None = None,
        executed_at: datetime,
        observed_at: datetime,
        producer_identity: str,
        source_payload: Mapping[str, Any] | None = None,
    ) -> "EntryFillObservation":
        source = _nonempty(exchange_source, "exchange_source").lower()
        quantity = _decimal(executed_qty, "executed_qty", positive=True)
        fill_price = _decimal(price, "price", positive=True)
        fill_notional = (
            quantity * fill_price
            if notional is None
            else _decimal(notional, "notional", positive=True)
        )
        fee_number = abs(_decimal(fee, "fee"))
        wire_cid = exchange_wire_client_order_id(source, client_order_id)
        raw = dict(source_payload or {})
        return cls(
            environment=EntryIntentEnvironment(environment),
            deployment_id=EntryIntentDeployment(deployment_id),
            adoption_id=int(adoption_id),
            generation=int(generation),
            git_revision=_git_revision(git_revision),
            exchange_source=source,
            exchange_trade_id=_nonempty(
                exchange_trade_id, "exchange_trade_id"
            ),
            exchange_order_id=_nonempty(
                exchange_order_id, "exchange_order_id"
            ),
            wire_client_order_id=wire_cid,
            symbol=_nonempty(symbol, "symbol").upper(),
            side=_nonempty(side, "side").upper(),
            executed_qty=quantity,
            price=fill_price,
            notional=fill_notional,
            fee=fee_number,
            fee_asset=(
                _nonempty(fee_asset, "fee_asset").upper()
                if fee_asset is not None else None
            ),
            executed_at=_aware(executed_at, "executed_at"),
            observed_at=_aware(observed_at, "observed_at"),
            producer_identity=_nonempty(
                producer_identity, "producer_identity"
            ),
            source_payload=_json_ready(raw),
        )

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return (
            self.environment.value,
            self.deployment_id.value,
            self.exchange_source,
            self.exchange_trade_id,
        )

    @property
    def semantic_payload(self) -> Mapping[str, Any]:
        return {
            "client_order_id": self.wire_client_order_id,
            "deployment_id": self.deployment_id.value,
            "environment": self.environment.value,
            "exchange_order_id": self.exchange_order_id,
            "exchange_source": self.exchange_source,
            "exchange_trade_id": self.exchange_trade_id,
            "executed_at": _canonical_timestamp(self.executed_at),
            "executed_qty": canonical_decimal(self.executed_qty),
            "fee": canonical_decimal(self.fee),
            "fee_asset": self.fee_asset,
            "notional": canonical_decimal(self.notional),
            "price": canonical_decimal(self.price),
            "side": self.side,
            "symbol": self.symbol,
        }

    @property
    def source_fingerprint(self) -> str:
        return _fingerprint(self.semantic_payload)


@dataclass(frozen=True, slots=True)
class FillLineageResolution:
    status: FillAttributionStatus
    method: str
    intent_id: uuid.UUID | None = None
    submission_attempt_id: uuid.UUID | None = None
    ack_id: uuid.UUID | None = None
    client_order_id: str | None = None
    strategy: str | None = None
    interval: str | None = None
    order_purpose: str | None = None
    linked_position_id: int | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        status = FillAttributionStatus(self.status)
        _nonempty(self.method, "method")
        _optional_nonempty(self.client_order_id, "client_order_id")
        if self.strategy is not None and self.strategy != _nonempty(
            self.strategy, "strategy"
        ).upper():
            raise ValueError("strategy must be canonical uppercase")
        if self.interval is not None and self.interval != _nonempty(
            self.interval, "interval"
        ).lower():
            raise ValueError("interval must be canonical lowercase")
        if self.order_purpose is not None and self.order_purpose != "ENTRY":
            raise ValueError("order_purpose must be ENTRY when present")
        if (self.strategy is None) != (self.interval is None):
            raise ValueError("strategy and interval must be present together")
        complete = all(
            value is not None
            for value in (
                self.intent_id,
                self.submission_attempt_id,
                self.ack_id,
                self.client_order_id,
                self.strategy,
                self.interval,
                self.order_purpose,
            )
        )
        if status in {
            FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
            FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
        } and not complete:
            raise ValueError("complete bot-owned attribution requires LEI1B lineage")
        if (
            status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
            and self.linked_position_id is None
        ):
            raise ValueError("BOT_OWNED_ATTRIBUTED requires position proof")
        if (
            status is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
            and self.linked_position_id is not None
        ):
            raise ValueError("BOT_OWNED_MISSING_POSITION forbids position proof")

    @property
    def identity_payload(self) -> Mapping[str, Any]:
        return {
            "ack_id": str(self.ack_id) if self.ack_id else None,
            "client_order_id": self.client_order_id,
            "intent_id": str(self.intent_id) if self.intent_id else None,
            "interval": self.interval,
            "linked_position_id": self.linked_position_id,
            "order_purpose": self.order_purpose,
            "strategy": self.strategy,
            "submission_attempt_id": (
                str(self.submission_attempt_id)
                if self.submission_attempt_id else None
            ),
        }

    @property
    def attribution_fingerprint(self) -> str:
        return _fingerprint(self.identity_payload)


@dataclass(frozen=True, slots=True)
class EntryFillEvidence:
    fill_evidence_id: uuid.UUID
    observation: EntryFillObservation
    lineage: FillLineageResolution
    source_fingerprint: str
    attribution_fingerprint: str
    contract_version: FillEvidenceContractVersion

    @staticmethod
    def deterministic_id(observation: EntryFillObservation) -> uuid.UUID:
        return uuid.uuid5(
            ENTRY_FILL_EVIDENCE_NAMESPACE,
            _canonical_json(
                {
                    "deployment_id": observation.deployment_id.value,
                    "environment": observation.environment.value,
                    "exchange_source": observation.exchange_source,
                    "exchange_trade_id": observation.exchange_trade_id,
                    "identity_version": "LIVE_ENTRY_FILL_ID_V1",
                }
            ),
        )

    @classmethod
    def build(
        cls,
        observation: EntryFillObservation,
        lineage: FillLineageResolution,
    ) -> "EntryFillEvidence":
        return cls(
            fill_evidence_id=cls.deterministic_id(observation),
            observation=observation,
            lineage=lineage,
            source_fingerprint=observation.source_fingerprint,
            attribution_fingerprint=lineage.attribution_fingerprint,
            contract_version=FillEvidenceContractVersion.V1,
        )

    def __post_init__(self) -> None:
        FillEvidenceContractVersion(self.contract_version)
        if self.fill_evidence_id != self.deterministic_id(self.observation):
            raise ValueError("fill_evidence_id does not match natural identity")
        if self.source_fingerprint != self.observation.source_fingerprint:
            raise ValueError("source_fingerprint does not match source semantics")
        if self.attribution_fingerprint != self.lineage.attribution_fingerprint:
            raise ValueError("attribution_fingerprint does not match lineage")

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return self.observation.natural_key


@dataclass(frozen=True, slots=True)
class EntryFillApplicationDecision:
    application_decision_id: uuid.UUID
    fill_evidence_id: uuid.UUID
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    adoption_id: int
    generation: int
    git_revision: str
    exchange_source: str
    intent_id: uuid.UUID | None
    submission_attempt_id: uuid.UUID | None
    ack_id: uuid.UUID | None
    client_order_id: str | None
    strategy: str | None
    interval: str | None
    order_purpose: str | None
    local_fill_id: int | None
    linked_position_id: int | None
    attribution_status: FillAttributionStatus
    attribution_fingerprint: str
    application_status: FillApplicationStatus
    application_target_identity: str | None
    canonical_source_fingerprint: str
    observed_source_fingerprint: str
    applied_fingerprint: str | None
    applied_at: datetime | None
    decision_fingerprint: str
    decision_payload: Mapping[str, Any]
    decided_at: datetime
    producer_identity: str
    contract_version: FillApplicationContractVersion

    @classmethod
    def build(
        cls,
        evidence: EntryFillEvidence,
        *,
        lineage: FillLineageResolution | None = None,
        application_status: FillApplicationStatus | str,
        decided_at: datetime,
        producer_identity: str,
        observed_source_fingerprint: str | None = None,
        applied_fingerprint: str | None = None,
        applied_at: datetime | None = None,
        local_fill_id: int | None = None,
        linked_position_id: int | None = None,
        application_target_identity: str | None = None,
        decision_payload: Mapping[str, Any] | None = None,
    ) -> "EntryFillApplicationDecision":
        status = FillApplicationStatus(application_status)
        selected_lineage = lineage or evidence.lineage
        selected_linked_position_id = (
            linked_position_id
            if linked_position_id is not None
            else selected_lineage.linked_position_id
        )
        observed = observed_source_fingerprint or evidence.source_fingerprint
        payload = {
            "application_status": status.value,
            "application_target_identity": application_target_identity,
            "attribution_detail": selected_lineage.detail,
            "canonical_attribution_fingerprint": (
                evidence.attribution_fingerprint
            ),
            "attribution_fingerprint": (
                selected_lineage.attribution_fingerprint
            ),
            "attribution_identity": selected_lineage.identity_payload,
            "attribution_method": selected_lineage.method,
            "attribution_status": selected_lineage.status.value,
            "canonical_source_fingerprint": evidence.source_fingerprint,
            "fill_evidence_id": str(evidence.fill_evidence_id),
            "observed_source_fingerprint": observed,
            "proof": {
                "applied_at": (
                    _canonical_timestamp(applied_at) if applied_at else None
                ),
                "applied_fingerprint": applied_fingerprint,
                "linked_position_id": selected_linked_position_id,
                "local_fill_id": local_fill_id,
            },
            "reason": _json_ready(dict(decision_payload or {})),
        }
        decision_fingerprint = _fingerprint(payload)
        decision_id = uuid.uuid5(
            ENTRY_FILL_APPLICATION_NAMESPACE,
            f"{evidence.fill_evidence_id}:{decision_fingerprint}",
        )
        observation = evidence.observation
        return cls(
            application_decision_id=decision_id,
            fill_evidence_id=evidence.fill_evidence_id,
            environment=observation.environment,
            deployment_id=observation.deployment_id,
            adoption_id=observation.adoption_id,
            generation=observation.generation,
            git_revision=observation.git_revision,
            exchange_source=observation.exchange_source,
            intent_id=selected_lineage.intent_id,
            submission_attempt_id=selected_lineage.submission_attempt_id,
            ack_id=selected_lineage.ack_id,
            client_order_id=selected_lineage.client_order_id,
            strategy=selected_lineage.strategy,
            interval=selected_lineage.interval,
            order_purpose=selected_lineage.order_purpose,
            local_fill_id=local_fill_id,
            linked_position_id=selected_linked_position_id,
            attribution_status=selected_lineage.status,
            attribution_fingerprint=(
                selected_lineage.attribution_fingerprint
            ),
            application_status=status,
            application_target_identity=application_target_identity,
            canonical_source_fingerprint=evidence.source_fingerprint,
            observed_source_fingerprint=observed,
            applied_fingerprint=applied_fingerprint,
            applied_at=applied_at,
            decision_fingerprint=decision_fingerprint,
            decision_payload=payload,
            decided_at=_aware(decided_at, "decided_at"),
            producer_identity=_nonempty(
                producer_identity, "producer_identity"
            ),
            contract_version=FillApplicationContractVersion.V1,
        )

    def __post_init__(self) -> None:
        status = FillApplicationStatus(self.application_status)
        attribution_status = FillAttributionStatus(self.attribution_status)
        FillApplicationContractVersion(self.contract_version)
        environment = EntryIntentEnvironment(self.environment)
        deployment = EntryIntentDeployment(self.deployment_id)
        if deployment.value.split("-", 1)[1] != environment.value:
            raise ValueError("application deployment_id does not match environment")
        if int(self.adoption_id) <= 0 or int(self.generation) <= 0:
            raise ValueError("application adoption_id and generation must be positive")
        _git_revision(self.git_revision)
        if self.exchange_source != _nonempty(
            self.exchange_source, "exchange_source"
        ).lower():
            raise ValueError("application exchange_source must be lowercase")
        _optional_nonempty(self.client_order_id, "client_order_id")
        if self.strategy is not None and self.strategy != _nonempty(
            self.strategy, "strategy"
        ).upper():
            raise ValueError("application strategy must be canonical uppercase")
        if self.interval is not None and self.interval != _nonempty(
            self.interval, "interval"
        ).lower():
            raise ValueError("application interval must be canonical lowercase")
        if (self.strategy is None) != (self.interval is None):
            raise ValueError(
                "application strategy and interval must be present together"
            )
        if self.order_purpose is not None and self.order_purpose != "ENTRY":
            raise ValueError("application order_purpose must be ENTRY")
        _aware(self.decided_at, "decided_at")
        if self.applied_at is not None:
            _aware(self.applied_at, "applied_at")
        for field, value in (
            ("canonical_source_fingerprint", self.canonical_source_fingerprint),
            ("observed_source_fingerprint", self.observed_source_fingerprint),
            ("attribution_fingerprint", self.attribution_fingerprint),
            ("decision_fingerprint", self.decision_fingerprint),
        ):
            if len(str(value)) != 64 or not re.fullmatch(
                r"[0-9a-f]{64}", str(value)
            ):
                raise ValueError(f"{field} must be lowercase SHA-256")
        attribution_identity = self.decision_payload.get(
            "attribution_identity"
        )
        if not isinstance(attribution_identity, Mapping):
            raise ValueError("decision payload must bind attribution identity")
        if self.attribution_fingerprint != _fingerprint(
            attribution_identity
        ):
            raise ValueError(
                "attribution fingerprint does not match decision identity"
            )
        typed_identity = {
            "ack_id": str(self.ack_id) if self.ack_id else None,
            "client_order_id": self.client_order_id,
            "intent_id": str(self.intent_id) if self.intent_id else None,
            "interval": self.interval,
            "linked_position_id": self.linked_position_id,
            "order_purpose": self.order_purpose,
            "strategy": self.strategy,
            "submission_attempt_id": (
                str(self.submission_attempt_id)
                if self.submission_attempt_id else None
            ),
        }
        if any(
            attribution_identity.get(field) != value
            for field, value in typed_identity.items()
        ):
            raise ValueError(
                "typed application lineage differs from decision identity"
            )
        proof_status = status in {
            FillApplicationStatus.APPLIED,
            FillApplicationStatus.TRUE_DUPLICATE_APPLIED,
        }
        proof_complete = all(
            value is not None
            for value in (
                self.local_fill_id,
                self.applied_fingerprint,
                self.applied_at,
                self.application_target_identity,
            )
        )
        if proof_status != proof_complete:
            raise ValueError("applied status and complete application proof differ")
        if proof_status and attribution_status not in {
            FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
            FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
        }:
            raise ValueError("applied proof requires exact bot-owned attribution")
        if proof_status and self.application_target_identity != (
            f"binance_order_fills:{self.local_fill_id}"
        ):
            raise ValueError(
                "application target must identify the canonical local fill"
            )
        if (
            attribution_status
            is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
            and self.linked_position_id is not None
        ):
            raise ValueError(
                "BOT_OWNED_MISSING_POSITION forbids linked position proof"
            )
        if (
            attribution_status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
            and self.linked_position_id is None
        ):
            raise ValueError("BOT_OWNED_ATTRIBUTED requires linked position proof")
        complete_bot_lineage = all(
            value is not None
            for value in (
                self.intent_id,
                self.submission_attempt_id,
                self.ack_id,
                self.client_order_id,
                self.strategy,
                self.interval,
                self.order_purpose,
            )
        )
        if attribution_status in {
            FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
            FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
        } and not complete_bot_lineage:
            raise ValueError(
                "complete application attribution requires LEI1B lineage"
            )
        if proof_status and (
            self.applied_fingerprint != self.canonical_source_fingerprint
        ):
            raise ValueError("applied fingerprint must equal canonical source")
        changed_source = (
            self.observed_source_fingerprint
            != self.canonical_source_fingerprint
        )
        canonical_attribution = self.decision_payload.get(
            "canonical_attribution_fingerprint"
        )
        changed_attribution = (
            isinstance(canonical_attribution, str)
            and len(canonical_attribution) == 64
            and self.attribution_fingerprint != canonical_attribution
        )
        if status is FillApplicationStatus.CORRECTION_PENDING and not changed_source:
            raise ValueError("correction requires changed source payload")
        if (
            status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
            and attribution_status is not FillAttributionStatus.CONFLICTED
            and not changed_source
            and not changed_attribution
        ):
            raise ValueError(
                "conflict requires changed source or attribution payload"
            )
        if status in {
            FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED,
            FillApplicationStatus.AMBIGUOUS,
        } and (
            self.observed_source_fingerprint
            != self.canonical_source_fingerprint
        ):
            raise ValueError("non-conflict state requires canonical source payload")
        if (
            status is FillApplicationStatus.OBSERVED_NOT_APPLIED
            and proof_complete
        ):
            raise ValueError(
                "OBSERVED_NOT_APPLIED requires at least one missing proof field"
            )
        if self.decision_fingerprint != _fingerprint(
            dict(self.decision_payload)
        ):
            raise ValueError("decision_fingerprint does not match decision payload")
        expected_id = uuid.uuid5(
            ENTRY_FILL_APPLICATION_NAMESPACE,
            f"{self.fill_evidence_id}:{self.decision_fingerprint}",
        )
        if self.application_decision_id != expected_id:
            raise ValueError("application_decision_id does not match decision")


def classify_application_state(
    evidence: EntryFillEvidence,
    latest: EntryFillApplicationDecision | None,
    *,
    observed_source_fingerprint: str | None = None,
    observed_attribution_fingerprint: str | None = None,
) -> FillApplicationStatus:
    observed_source = observed_source_fingerprint or evidence.source_fingerprint
    observed_attribution = (
        observed_attribution_fingerprint or evidence.attribution_fingerprint
    )
    if observed_source != evidence.source_fingerprint:
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    if latest is None:
        if evidence.lineage.status is FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED:
            return FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
        if evidence.lineage.status is FillAttributionStatus.AMBIGUOUS:
            return FillApplicationStatus.AMBIGUOUS
        if evidence.lineage.status is FillAttributionStatus.CONFLICTED:
            return FillApplicationStatus.IDEMPOTENCY_CONFLICT
        return FillApplicationStatus.OBSERVED_NOT_APPLIED
    if latest.fill_evidence_id != evidence.fill_evidence_id:
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    if latest.application_status is FillApplicationStatus.CORRECTION_PENDING:
        return FillApplicationStatus.CORRECTION_PENDING
    if latest.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT:
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    if latest.application_status is FillApplicationStatus.AMBIGUOUS:
        return FillApplicationStatus.AMBIGUOUS
    if latest.application_status is FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED:
        return FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
    if latest.applied_fingerprint not in (None, evidence.source_fingerprint):
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    if latest.attribution_fingerprint != observed_attribution:
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    complete = all(
        value is not None
        for value in (
            latest.local_fill_id,
            latest.applied_fingerprint,
            latest.applied_at,
            latest.application_target_identity,
        )
    )
    if complete and latest.applied_fingerprint == evidence.source_fingerprint:
        return FillApplicationStatus.TRUE_DUPLICATE_APPLIED
    return FillApplicationStatus.OBSERVED_NOT_APPLIED


@dataclass(frozen=True, slots=True)
class EntryFillEvent:
    event_type: EntryFillEventType
    fill_evidence_id: uuid.UUID | None
    occurred_at: datetime
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class EntryFillProcessingResult:
    outcome: EntryFillProcessingOutcome
    application_status: FillApplicationStatus | None
    attribution_status: FillAttributionStatus | None
    evidence: EntryFillEvidence | None
    events: tuple[EntryFillEvent, ...]
    error_code: str | None = None


class EntryFillAttributionRepositoryProtocol(Protocol):
    def resolve_lineage(
        self, observation: EntryFillObservation
    ) -> FillLineageResolution: ...

    def commit_evidence(
        self, evidence: EntryFillEvidence
    ) -> FillEvidenceInsertOutcome: ...

    def load_evidence(
        self, natural_key: Sequence[str]
    ) -> EntryFillEvidence | None: ...

    def load_latest_application(
        self, fill_evidence_id: uuid.UUID
    ) -> EntryFillApplicationDecision | None: ...

    def application_proof_matches(
        self,
        evidence: EntryFillEvidence,
        decision: EntryFillApplicationDecision,
    ) -> bool: ...

    def load_existing_application_proof(
        self,
        evidence: EntryFillEvidence,
    ) -> tuple[int, datetime] | None: ...

    def append_application(
        self, decision: EntryFillApplicationDecision
    ) -> FillApplicationInsertOutcome: ...


class EntryFillAttributionRepository:
    """PostgreSQL LEI1C repository; every operation owns one transaction."""

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

    @staticmethod
    def _ack_candidate(row: Sequence[Any]) -> Mapping[str, Any]:
        return {
            "ack_id": uuid.UUID(str(row[0])),
            "submission_attempt_id": uuid.UUID(str(row[1])),
            "intent_id": uuid.UUID(str(row[2])),
            "environment": str(row[3]),
            "deployment_id": str(row[4]),
            "adoption_id": int(row[5]),
            "generation": int(row[6]),
            "git_revision": str(row[7]),
            "client_order_id": str(row[8]),
            "exchange_source": str(row[9]),
            "exchange_order_id": str(row[10]),
            "symbol": str(row[11]),
            "strategy": str(row[12]),
            "interval": str(row[13]),
            "order_purpose": str(row[14]),
            "side": str(row[15]),
        }

    @staticmethod
    def _context_matches(
        candidate: Mapping[str, Any],
        observation: EntryFillObservation,
        *,
        require_exchange_order_id: bool = False,
    ) -> bool:
        matches = EntryFillAttributionRepository._domain_matches(
            candidate, observation
        ) and all(
            (
                candidate["adoption_id"] == observation.adoption_id,
                candidate["generation"] == observation.generation,
                candidate["git_revision"] == observation.git_revision,
            )
        )
        return matches and (
            not require_exchange_order_id
            or candidate.get("exchange_order_id")
            == observation.exchange_order_id
        )

    @staticmethod
    def _domain_matches(
        candidate: Mapping[str, Any], observation: EntryFillObservation
    ) -> bool:
        """Partition lineage by the complete PAPER/LIVE runtime domain."""
        return all(
            (
                candidate["environment"] == observation.environment.value,
                candidate["deployment_id"] == observation.deployment_id.value,
                candidate["exchange_source"] == observation.exchange_source,
                candidate["symbol"] == observation.symbol,
                candidate["side"] == observation.side,
                candidate["order_purpose"] == "ENTRY",
            )
        )

    @staticmethod
    def _isolation_scope_matches(
        candidate: Mapping[str, Any], observation: EntryFillObservation
    ) -> bool:
        return (
            candidate["environment"] == observation.environment.value
            and candidate["deployment_id"]
            == observation.deployment_id.value
        )

    @staticmethod
    def _ambiguous(detail: str) -> FillLineageResolution:
        return FillLineageResolution(
            FillAttributionStatus.AMBIGUOUS,
            "FAIL_CLOSED",
            detail=detail,
        )

    @staticmethod
    def _conflicted(detail: str) -> FillLineageResolution:
        return FillLineageResolution(
            FillAttributionStatus.CONFLICTED,
            "FAIL_CLOSED",
            detail=detail,
        )

    @staticmethod
    def _fetch_ack_candidates(
        cur: Any,
        observation: EntryFillObservation,
        *,
        by_order: bool,
    ) -> list[Mapping[str, Any]]:
        base = """
            SELECT a.ack_id,a.submission_attempt_id,a.intent_id,
                   a.environment,a.deployment_id,a.adoption_id,a.generation,
                   a.git_revision,a.client_order_id,a.exchange_source,
                   a.exchange_order_id,a.symbol,a.strategy,a."interval",
                   a.order_purpose,a.side
            FROM live_entry_order_acks_v1 a
            JOIN live_entry_submissions_v1 s
              ON s.submission_attempt_id=a.submission_attempt_id
             AND s.intent_id=a.intent_id
            JOIN live_entry_intents_v1 i ON i.intent_id=a.intent_id
            WHERE a.exchange_source=%s
        """
        if by_order:
            cur.execute(
                base + " AND a.exchange_order_id=%s ORDER BY a.ack_id",
                (
                    observation.exchange_source,
                    observation.exchange_order_id,
                ),
            )
        elif observation.wire_client_order_id is None:
            return []
        elif observation.exchange_source == "okx":
            cur.execute(
                base
                + """
                  AND left(
                    regexp_replace(a.client_order_id,'[^A-Za-z0-9]','','g'),32
                  )=%s
                  ORDER BY a.ack_id
                """,
                (
                    observation.exchange_source,
                    observation.wire_client_order_id,
                ),
            )
        else:
            cur.execute(
                base + " AND a.client_order_id=%s ORDER BY a.ack_id",
                (
                    observation.exchange_source,
                    observation.wire_client_order_id,
                ),
            )
        return [
            EntryFillAttributionRepository._ack_candidate(row)
            for row in cur.fetchall()
        ]

    @staticmethod
    def _position_ids(
        cur: Any,
        observation: EntryFillObservation,
        client_order_id: str | None,
    ) -> tuple[list[int], bool]:
        wire_cid = exchange_wire_client_order_id(
            observation.exchange_source, client_order_id
        )
        cur.execute(
            """
            WITH observed(exchange_source,symbol,order_id,wire_cid) AS (
              VALUES (%s,%s,%s,%s)
            ),
            candidates AS (
              SELECT bo.*,
                     bo.order_id=o.order_id AS order_match,
                     CASE
                       WHEN o.wire_cid IS NULL THEN false
                       WHEN o.exchange_source='okx' THEN
                         left(regexp_replace(
                           COALESCE(bo.client_order_id,''),
                           '[^A-Za-z0-9]','','g'
                         ),32)=o.wire_cid
                       ELSE bo.client_order_id=o.wire_cid
                     END AS cid_match,
                     o.wire_cid
              FROM binance_orders bo CROSS JOIN observed o
              WHERE lower(COALESCE(bo.exchange_source,''))=o.exchange_source
                AND bo.symbol=o.symbol
                AND (
                  bo.order_id=o.order_id
                  OR CASE
                    WHEN o.wire_cid IS NULL THEN false
                    WHEN o.exchange_source='okx' THEN
                      left(regexp_replace(
                        COALESCE(bo.client_order_id,''),
                        '[^A-Za-z0-9]','','g'
                      ),32)=o.wire_cid
                    ELSE bo.client_order_id=o.wire_cid
                  END
                )
            ),
            exact_orders AS (
              SELECT *
              FROM candidates
              WHERE (order_id IS NULL OR order_match)
                AND (
                  wire_cid IS NULL
                  OR client_order_id IS NULL
                  OR cid_match
                )
            ),
            identity_state AS (
              SELECT COALESCE(bool_or(
                (order_id IS NOT NULL AND NOT order_match)
                OR (
                  wire_cid IS NOT NULL
                  AND client_order_id IS NOT NULL
                  AND NOT cid_match
                )
              ),false) AS conflicted
              FROM candidates
            ),
            linked(position_id) AS (
              SELECT bo.position_id
              FROM exact_orders bo
              WHERE bo.position_id IS NOT NULL
              UNION ALL
              SELECT bo.reconciled_position_id
              FROM exact_orders bo
              WHERE bo.reconciled_position_id IS NOT NULL
              UNION ALL
              SELECT p.id
              FROM positions p
              JOIN exact_orders bo
                ON p.id=bo.position_id
                OR p.id=bo.reconciled_position_id
                OR p.entry_order_id=bo.order_id
            ),
            unique_links AS (
              SELECT DISTINCT position_id
              FROM linked
              WHERE position_id IS NOT NULL
            )
            SELECT unique_links.position_id,identity_state.conflicted
            FROM identity_state
            LEFT JOIN unique_links ON true
            ORDER BY unique_links.position_id NULLS LAST
            """,
            (
                observation.exchange_source,
                observation.symbol,
                observation.exchange_order_id,
                wire_cid,
            ),
        )
        rows = list(cur.fetchall())
        positions = [int(row[0]) for row in rows if row[0] is not None]
        conflicted = any(len(row) > 1 and bool(row[1]) for row in rows)
        return positions, conflicted

    @staticmethod
    def _fetch_partial_lineage(
        cur: Any, observation: EntryFillObservation
    ) -> list[Mapping[str, Any]]:
        if observation.wire_client_order_id is None:
            return []
        cid_predicate = "s.client_order_id=%s"
        if observation.exchange_source == "okx":
            cid_predicate = (
                "left(regexp_replace(s.client_order_id,"
                "'[^A-Za-z0-9]','','g'),32)=%s"
            )
        cur.execute(
            f"""
            SELECT i.intent_id,s.submission_attempt_id,i.environment,
                   i.deployment_id,i.adoption_id,i.generation,i.git_revision,
                   i.client_order_id,i.exchange_source,i.symbol,i.strategy,
                   i."interval",i.order_purpose,i.side
            FROM live_entry_submissions_v1 s
            JOIN live_entry_intents_v1 i ON i.intent_id=s.intent_id
            WHERE s.exchange_source=%s AND {cid_predicate}
            ORDER BY i.intent_id,s.submission_attempt_id
            """,
            (
                observation.exchange_source,
                observation.wire_client_order_id,
            ),
        )
        rows = list(cur.fetchall())

        def candidate(row: Sequence[Any]) -> Mapping[str, Any]:
            return {
                "intent_id": uuid.UUID(str(row[0])),
                "submission_attempt_id": (
                    uuid.UUID(str(row[1])) if row[1] is not None else None
                ),
                "environment": str(row[2]),
                "deployment_id": str(row[3]),
                "adoption_id": int(row[4]),
                "generation": int(row[5]),
                "git_revision": str(row[6]),
                "client_order_id": str(row[7]),
                "exchange_source": str(row[8]),
                "symbol": str(row[9]),
                "strategy": str(row[10]),
                "interval": str(row[11]),
                "order_purpose": str(row[12]),
                "side": str(row[13]),
            }

        candidates = [candidate(row) for row in rows]
        scoped_submission_exists = any(
            EntryFillAttributionRepository._domain_matches(item, observation)
            for item in candidates
        )
        if not scoped_submission_exists:
            intent_predicate = "i.client_order_id=%s"
            if observation.exchange_source == "okx":
                intent_predicate = (
                    "left(regexp_replace(i.client_order_id,"
                    "'[^A-Za-z0-9]','','g'),32)=%s"
                )
            cur.execute(
                f"""
                SELECT i.intent_id,NULL::uuid,i.environment,i.deployment_id,
                       i.adoption_id,i.generation,i.git_revision,
                       i.client_order_id,i.exchange_source,i.symbol,i.strategy,
                       i."interval",i.order_purpose,i.side
                FROM live_entry_intents_v1 i
                WHERE i.exchange_source=%s AND {intent_predicate}
                ORDER BY i.intent_id
                """,
                (
                    observation.exchange_source,
                    observation.wire_client_order_id,
                ),
            )
            candidates.extend(candidate(row) for row in cur.fetchall())
        return candidates

    @staticmethod
    def _fetch_legacy_orders(
        cur: Any, observation: EntryFillObservation
    ) -> list[Mapping[str, Any]]:
        params: list[Any] = [
            observation.exchange_source,
            observation.symbol,
            observation.exchange_order_id,
        ]
        cid_clause = "FALSE"
        if observation.wire_client_order_id is not None:
            if observation.exchange_source == "okx":
                cid_clause = (
                    "left(regexp_replace(COALESCE(client_order_id,''),"
                    "'[^A-Za-z0-9]','','g'),32)=%s"
                )
            else:
                cid_clause = "client_order_id=%s"
            params.append(observation.wire_client_order_id)
        cur.execute(
            f"""
            SELECT id,exchange_source,order_id,client_order_id,symbol,side,
                   strategy,"interval",order_purpose,position_id,
                   reconciled_position_id,is_exit
            FROM binance_orders
            WHERE (
              exchange_source IS NULL OR lower(exchange_source)=%s
            )
              AND (symbol IS NULL OR symbol=%s)
              AND (order_id=%s OR {cid_clause})
            ORDER BY id
            """,
            tuple(params),
        )
        return [
            {
                "row_id": int(row[0]),
                "exchange_source": (
                    str(row[1]).lower() if row[1] is not None else None
                ),
                "order_id": str(row[2]) if row[2] is not None else None,
                "client_order_id": (
                    str(row[3]) if row[3] is not None else None
                ),
                "symbol": str(row[4]) if row[4] is not None else None,
                "side": str(row[5]) if row[5] is not None else None,
                "strategy": str(row[6]) if row[6] is not None else None,
                "interval": str(row[7]) if row[7] is not None else None,
                "order_purpose": (
                    str(row[8]) if row[8] is not None else None
                ),
                "position_id": row[9] or row[10],
                "is_exit": bool(row[11]) if row[11] is not None else None,
            }
            for row in cur.fetchall()
        ]

    def resolve_observation_context(
        self, observation: EntryFillObservation
    ) -> EntryFillContractContext:
        """Resolve an exact LEI1B owner before immutable evidence is built.

        Foreign PAPER/LIVE rows are ignored.  Conflicting or ambiguous exact
        identities inside the same runtime domain fail closed.  With no exact
        LEI1B lineage, the caller-provided active context remains authoritative.
        """
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            raw_order = self._fetch_ack_candidates(
                cur, observation, by_order=True
            )
            raw_cid = self._fetch_ack_candidates(
                cur, observation, by_order=False
            )
            domain_order = [
                item for item in raw_order
                if self._domain_matches(item, observation)
            ]
            domain_cid = [
                item for item in raw_cid
                if self._domain_matches(item, observation)
            ]
            if len(domain_order) > 1 or len(domain_cid) > 1:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CONTEXT_ACK_AMBIGUOUS"
                )
            order_candidate = domain_order[0] if domain_order else None
            cid_candidate = domain_cid[0] if domain_cid else None
            if (
                order_candidate is not None
                and cid_candidate is not None
                and order_candidate["ack_id"] != cid_candidate["ack_id"]
            ):
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CONTEXT_ORDER_CID_CONFLICT"
                )
            selected = order_candidate or cid_candidate
            if selected is not None:
                selected_wire_cid = exchange_wire_client_order_id(
                    observation.exchange_source,
                    selected["client_order_id"],
                )
                if (
                    selected["exchange_order_id"]
                    != observation.exchange_order_id
                    or (
                        observation.wire_client_order_id is not None
                        and selected_wire_cid
                        != observation.wire_client_order_id
                    )
                ):
                    raise EntryFillRepositoryError(
                        "ENTRY_FILL_CONTEXT_ACK_IDENTITY_CONFLICT"
                    )
                return EntryFillContractContext(
                    environment=observation.environment,
                    deployment_id=observation.deployment_id,
                    adoption_id=selected["adoption_id"],
                    generation=selected["generation"],
                    git_revision=selected["git_revision"],
                )

            same_isolation_ack = any(
                self._isolation_scope_matches(item, observation)
                for item in (*raw_order, *raw_cid)
            )
            if same_isolation_ack:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CONTEXT_ACK_DOMAIN_CONFLICT"
                )

            raw_partial = self._fetch_partial_lineage(cur, observation)
            domain_partial = [
                item for item in raw_partial
                if self._domain_matches(item, observation)
            ]
            if len(domain_partial) > 1:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CONTEXT_PARTIAL_AMBIGUOUS"
                )
            if domain_partial:
                selected = domain_partial[0]
                return EntryFillContractContext(
                    environment=observation.environment,
                    deployment_id=observation.deployment_id,
                    adoption_id=selected["adoption_id"],
                    generation=selected["generation"],
                    git_revision=selected["git_revision"],
                )
            if any(
                self._isolation_scope_matches(item, observation)
                for item in raw_partial
            ):
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CONTEXT_PARTIAL_DOMAIN_CONFLICT"
                )
            return EntryFillContractContext.from_observation(observation)
        except EntryFillRepositoryError:
            raise
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_CONTEXT_RESOLUTION_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)

    def resolve_lineage(
        self, observation: EntryFillObservation
    ) -> FillLineageResolution:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            raw_order_candidates = self._fetch_ack_candidates(
                cur, observation, by_order=True
            )
            raw_cid_candidates = self._fetch_ack_candidates(
                cur, observation, by_order=False
            )
            domain_order_candidates = [
                candidate
                for candidate in raw_order_candidates
                if self._domain_matches(candidate, observation)
            ]
            domain_cid_candidates = [
                candidate
                for candidate in raw_cid_candidates
                if self._domain_matches(candidate, observation)
            ]
            if (
                len(domain_order_candidates) > 1
                or len(domain_cid_candidates) > 1
            ):
                return self._ambiguous("MULTIPLE_EXACT_ACK_CANDIDATES")
            if (
                domain_order_candidates
                and domain_cid_candidates
                and domain_order_candidates[0]["ack_id"]
                != domain_cid_candidates[0]["ack_id"]
            ):
                return self._conflicted("ORDER_ID_AND_CID_LINK_DIFFER")
            order_candidates = [
                candidate
                for candidate in domain_order_candidates
                if self._context_matches(
                    candidate,
                    observation,
                    require_exchange_order_id=True,
                )
            ]
            cid_candidates = [
                candidate
                for candidate in domain_cid_candidates
                if self._context_matches(
                    candidate,
                    observation,
                    require_exchange_order_id=True,
                )
            ]
            order_candidate = order_candidates[0] if order_candidates else None
            cid_candidate = cid_candidates[0] if cid_candidates else None
            if (
                order_candidate is not None
                and cid_candidate is not None
                and order_candidate["ack_id"] != cid_candidate["ack_id"]
            ):
                return self._conflicted("ORDER_ID_AND_CID_LINK_DIFFER")
            candidate = order_candidate or cid_candidate
            if candidate is not None:
                candidate_wire_cid = exchange_wire_client_order_id(
                    observation.exchange_source,
                    candidate["client_order_id"],
                )
                if (
                    observation.wire_client_order_id is not None
                    and candidate_wire_cid
                    != observation.wire_client_order_id
                ):
                    return self._conflicted("ACK_CLIENT_ORDER_ID_MISMATCH")
                positions, position_identity_conflict = self._position_ids(
                    cur, observation, candidate["client_order_id"]
                )
                if position_identity_conflict:
                    return self._conflicted(
                        "POSITION_ORDER_ID_AND_CID_LINK_DIFFER"
                    )
                if len(positions) > 1:
                    return self._ambiguous("MULTIPLE_POSITION_LINKS")
                return FillLineageResolution(
                    (
                        FillAttributionStatus.BOT_OWNED_ATTRIBUTED
                        if positions
                        else FillAttributionStatus.BOT_OWNED_MISSING_POSITION
                    ),
                    (
                        "EXACT_EXCHANGE_ORDER_ID"
                        if order_candidate is not None
                        else "EXACT_CLIENT_ORDER_ID"
                    ),
                    intent_id=candidate["intent_id"],
                    submission_attempt_id=candidate["submission_attempt_id"],
                    ack_id=candidate["ack_id"],
                    client_order_id=candidate["client_order_id"],
                    strategy=candidate["strategy"],
                    interval=candidate["interval"],
                    order_purpose=candidate["order_purpose"],
                    linked_position_id=(positions[0] if positions else None),
                )

            if raw_order_candidates or raw_cid_candidates:
                return self._conflicted("ACK_RUNTIME_CONTEXT_MISMATCH")

            raw_partial = self._fetch_partial_lineage(cur, observation)
            domain_partial = [
                candidate
                for candidate in raw_partial
                if self._domain_matches(candidate, observation)
            ]
            if len(domain_partial) > 1:
                return self._ambiguous(
                    "MULTIPLE_PARTIAL_LINEAGE_CANDIDATES"
                )
            partial = [
                candidate
                for candidate in domain_partial
                if self._context_matches(candidate, observation)
            ]
            if partial:
                candidate = partial[0]
                return FillLineageResolution(
                    FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE,
                    "EXACT_CLIENT_ORDER_ID_PARTIAL_LINEAGE",
                    intent_id=candidate["intent_id"],
                    submission_attempt_id=candidate["submission_attempt_id"],
                    client_order_id=candidate["client_order_id"],
                    strategy=candidate["strategy"],
                    interval=candidate["interval"],
                    order_purpose=candidate["order_purpose"],
                    detail="ACK_MISSING",
                )
            if raw_partial:
                return self._conflicted("PARTIAL_LINEAGE_CONTEXT_MISMATCH")

            raw_legacy = self._fetch_legacy_orders(cur, observation)
            scoped_legacy = [
                order
                for order in raw_legacy
                if order["exchange_source"] in {
                    None,
                    observation.exchange_source,
                }
                and order["symbol"] in {None, observation.symbol}
            ]
            legacy = [
                order
                for order in scoped_legacy
                if order["side"] in {None, observation.side}
            ]
            if len(legacy) > 1:
                return self._ambiguous("MULTIPLE_LEGACY_ORDER_CANDIDATES")
            if legacy:
                order = legacy[0]
                order_id_matches = (
                    order["order_id"] == observation.exchange_order_id
                )
                legacy_wire_cid = exchange_wire_client_order_id(
                    observation.exchange_source,
                    order["client_order_id"],
                )
                cid_matches = (
                    observation.wire_client_order_id is not None
                    and legacy_wire_cid
                    == observation.wire_client_order_id
                )
                if not order_id_matches and not cid_matches:
                    return self._conflicted("LEGACY_ORDER_IDENTITY_MISMATCH")
                if (
                    order["order_id"] is not None
                    and not order_id_matches
                ):
                    return self._conflicted("LEGACY_ORDER_ID_MISMATCH")
                if (
                    observation.wire_client_order_id is not None
                    and legacy_wire_cid is not None
                    and not cid_matches
                ):
                    return self._conflicted("LEGACY_CLIENT_ORDER_ID_MISMATCH")
                if order["is_exit"] is True or order["order_purpose"] not in {
                    None,
                    "ENTRY",
                }:
                    return FillLineageResolution(
                        FillAttributionStatus.UNKNOWN,
                        "EXACT_NON_ENTRY_ORDER_OUT_OF_SCOPE",
                        detail="OUTSIDE_LEI1C_ENTRY_SCOPE",
                    )
                strategy = (
                    str(order["strategy"]).upper()
                    if order["strategy"] else None
                )
                interval = (
                    str(order["interval"]).lower()
                    if order["interval"] else None
                )
                lineage_detail = "LEI1B_LINEAGE_ABSENT"
                if (strategy is None) != (interval is None):
                    strategy = None
                    interval = None
                    lineage_detail += ":INCOMPLETE_STRATEGY_INTERVAL_OMITTED"
                return FillLineageResolution(
                    FillAttributionStatus.LEGACY_BOT_OWNED,
                    (
                        "EXACT_LEGACY_ORDER_EVIDENCE"
                        if order_id_matches
                        else "EXACT_LEGACY_CLIENT_ORDER_ID_EVIDENCE"
                    ),
                    client_order_id=order["client_order_id"],
                    strategy=strategy,
                    interval=interval,
                    order_purpose="ENTRY",
                    linked_position_id=(
                        int(order["position_id"])
                        if order["position_id"] is not None else None
                    ),
                    detail=lineage_detail,
                )
            if scoped_legacy:
                return self._conflicted("LEGACY_ORDER_CONTEXT_MISMATCH")
            return FillLineageResolution(
                FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED,
                "NO_EXACT_BOT_OWNED_EVIDENCE",
            )
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_LINEAGE_RESOLUTION_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)

    @staticmethod
    def _source_payload(evidence: EntryFillEvidence) -> Mapping[str, Any]:
        return {
            "attribution": {
                "detail": evidence.lineage.detail,
                "identity": evidence.lineage.identity_payload,
                "linked_position_id": evidence.lineage.linked_position_id,
                "method": evidence.lineage.method,
            },
            "raw": _json_ready(dict(evidence.observation.source_payload)),
            "semantic": evidence.observation.semantic_payload,
        }

    def commit_evidence(
        self, evidence: EntryFillEvidence
    ) -> FillEvidenceInsertOutcome:
        observation = evidence.observation
        lineage = evidence.lineage
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO live_entry_fill_evidence_v1(
                  fill_evidence_id,environment,deployment_id,adoption_id,
                  generation,git_revision,exchange_source,exchange_trade_id,
                  exchange_order_id,client_order_id,wire_client_order_id,
                  intent_id,submission_attempt_id,ack_id,linked_position_id,
                  attribution_status,attribution_fingerprint,symbol,strategy,"interval",
                  order_purpose,side,executed_qty,price,notional,fee,fee_asset,
                  executed_at,source_fingerprint,source_payload,observed_at,
                  producer_identity,contract_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s
                )
                ON CONFLICT DO NOTHING
                RETURNING fill_evidence_id,source_fingerprint
                """,
                (
                    str(evidence.fill_evidence_id),
                    observation.environment.value,
                    observation.deployment_id.value,
                    observation.adoption_id,
                    observation.generation,
                    observation.git_revision,
                    observation.exchange_source,
                    observation.exchange_trade_id,
                    observation.exchange_order_id,
                    lineage.client_order_id,
                    observation.wire_client_order_id,
                    str(lineage.intent_id) if lineage.intent_id else None,
                    (
                        str(lineage.submission_attempt_id)
                        if lineage.submission_attempt_id else None
                    ),
                    str(lineage.ack_id) if lineage.ack_id else None,
                    lineage.linked_position_id,
                    lineage.status.value,
                    evidence.attribution_fingerprint,
                    observation.symbol,
                    lineage.strategy,
                    lineage.interval,
                    lineage.order_purpose,
                    observation.side,
                    str(observation.executed_qty),
                    str(observation.price),
                    str(observation.notional),
                    str(observation.fee),
                    observation.fee_asset,
                    observation.executed_at,
                    evidence.source_fingerprint,
                    _canonical_json(self._source_payload(evidence)),
                    observation.observed_at,
                    observation.producer_identity,
                    evidence.contract_version.value,
                ),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                outcome = FillEvidenceInsertOutcome.CREATED
            else:
                cur.execute(
                    """
                    SELECT fill_evidence_id,source_fingerprint
                    FROM live_entry_fill_evidence_v1
                    WHERE environment=%s AND deployment_id=%s
                      AND exchange_source=%s AND exchange_trade_id=%s
                    """,
                    evidence.natural_key,
                )
                existing = cur.fetchone()
                outcome = (
                    FillEvidenceInsertOutcome.IDEMPOTENT_EXISTING
                    if existing is not None
                    and uuid.UUID(str(existing[0])) == evidence.fill_evidence_id
                    and str(existing[1]) == evidence.source_fingerprint
                    else FillEvidenceInsertOutcome.IDEMPOTENCY_CONFLICT
                )
        except Exception as exc:
            self._cleanup(conn, cur, rollback=True)
            raise EntryFillRepositoryError(
                "ENTRY_FILL_EVIDENCE_COMMIT_FAILED"
            ) from exc
        try:
            conn.commit()
        except Exception as exc:
            self._cleanup(conn, cur, rollback=True)
            raise EntryFillRepositoryError(
                "ENTRY_FILL_EVIDENCE_COMMIT_OUTCOME_UNKNOWN"
            ) from exc
        self._cleanup(conn, cur, rollback=False)
        return outcome

    @staticmethod
    def _evidence_from_row(row: Sequence[Any]) -> EntryFillEvidence:
        payload = row[29] if isinstance(row[29], Mapping) else {}
        raw = payload.get("raw", {}) if isinstance(payload, Mapping) else {}
        observation = EntryFillObservation.build(
            environment=row[1],
            deployment_id=row[2],
            adoption_id=row[3],
            generation=row[4],
            git_revision=row[5],
            exchange_source=row[6],
            exchange_trade_id=row[7],
            exchange_order_id=row[8],
            client_order_id=row[10],
            symbol=row[17],
            side=row[21],
            executed_qty=row[22],
            price=row[23],
            notional=row[24],
            fee=row[25],
            fee_asset=row[26],
            executed_at=row[27],
            observed_at=row[30],
            producer_identity=row[31],
            source_payload=raw,
        )
        attribution_payload = (
            payload.get("attribution", {})
            if isinstance(payload, Mapping) else {}
        )
        lineage = FillLineageResolution(
            status=FillAttributionStatus(row[15]),
            method=str(attribution_payload.get("method") or "IMMUTABLE_EVIDENCE"),
            intent_id=uuid.UUID(str(row[11])) if row[11] else None,
            submission_attempt_id=(
                uuid.UUID(str(row[12])) if row[12] else None
            ),
            ack_id=uuid.UUID(str(row[13])) if row[13] else None,
            client_order_id=str(row[9]) if row[9] else None,
            strategy=str(row[18]) if row[18] else None,
            interval=str(row[19]) if row[19] else None,
            order_purpose=str(row[20]) if row[20] else None,
            linked_position_id=(
                int(row[14]) if row[14] is not None else None
            ),
            detail=(
                str(attribution_payload.get("detail"))
                if attribution_payload.get("detail") is not None else None
            ),
        )
        return EntryFillEvidence(
            fill_evidence_id=uuid.UUID(str(row[0])),
            observation=observation,
            lineage=lineage,
            source_fingerprint=str(row[28]),
            attribution_fingerprint=str(row[16]),
            contract_version=FillEvidenceContractVersion(row[32]),
        )

    def load_evidence(
        self, natural_key: Sequence[str]
    ) -> EntryFillEvidence | None:
        if len(tuple(natural_key)) != 4:
            raise ValueError("fill evidence natural key must have four fields")
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT fill_evidence_id,environment,deployment_id,adoption_id,
                       generation,git_revision,exchange_source,exchange_trade_id,
                       exchange_order_id,client_order_id,wire_client_order_id,
                       intent_id,submission_attempt_id,ack_id,linked_position_id,
                       attribution_status,attribution_fingerprint,symbol,strategy,"interval",
                       order_purpose,side,executed_qty,price,notional,fee,
                       fee_asset,executed_at,source_fingerprint,source_payload,
                       observed_at,producer_identity,contract_version
                FROM live_entry_fill_evidence_v1
                WHERE environment=%s AND deployment_id=%s
                  AND exchange_source=%s AND exchange_trade_id=%s
                """,
                tuple(natural_key),
            )
            row = cur.fetchone()
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_EVIDENCE_LOAD_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)
        return self._evidence_from_row(row) if row is not None else None

    @staticmethod
    def _application_from_row(
        row: Sequence[Any],
    ) -> EntryFillApplicationDecision:
        return EntryFillApplicationDecision(
            application_decision_id=uuid.UUID(str(row[0])),
            fill_evidence_id=uuid.UUID(str(row[1])),
            environment=EntryIntentEnvironment(row[2]),
            deployment_id=EntryIntentDeployment(row[3]),
            adoption_id=int(row[4]),
            generation=int(row[5]),
            git_revision=str(row[6]),
            exchange_source=str(row[7]),
            client_order_id=str(row[8]) if row[8] else None,
            intent_id=uuid.UUID(str(row[9])) if row[9] else None,
            submission_attempt_id=(uuid.UUID(str(row[10])) if row[10] else None),
            ack_id=uuid.UUID(str(row[11])) if row[11] else None,
            strategy=str(row[12]) if row[12] else None,
            interval=str(row[13]) if row[13] else None,
            order_purpose=str(row[14]) if row[14] else None,
            local_fill_id=int(row[15]) if row[15] is not None else None,
            linked_position_id=int(row[16]) if row[16] is not None else None,
            attribution_status=FillAttributionStatus(row[17]),
            attribution_fingerprint=str(row[18]),
            application_status=FillApplicationStatus(row[19]),
            application_target_identity=str(row[20]) if row[20] else None,
            canonical_source_fingerprint=str(row[21]),
            observed_source_fingerprint=str(row[22]),
            applied_fingerprint=str(row[23]) if row[23] else None,
            applied_at=row[24],
            decision_fingerprint=str(row[25]),
            decision_payload=row[26],
            decided_at=row[27],
            producer_identity=str(row[28]),
            contract_version=FillApplicationContractVersion(row[29]),
        )

    def load_latest_application(
        self, fill_evidence_id: uuid.UUID
    ) -> EntryFillApplicationDecision | None:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT application_decision_id,fill_evidence_id,environment,
                       deployment_id,adoption_id,generation,git_revision,
                       exchange_source,client_order_id,intent_id,
                       submission_attempt_id,ack_id,strategy,"interval",
                       order_purpose,
                       local_fill_id,linked_position_id,attribution_status,
                       attribution_fingerprint,application_status,
                       application_target_identity,canonical_source_fingerprint,
                       observed_source_fingerprint,applied_fingerprint,applied_at,
                       decision_fingerprint,decision_payload,decided_at,
                       producer_identity,contract_version
                FROM live_entry_fill_applications_v1
                WHERE fill_evidence_id=%s
                ORDER BY
                  CASE application_status
                    WHEN 'IDEMPOTENCY_CONFLICT' THEN 300
                    WHEN 'CORRECTION_PENDING' THEN 290
                    WHEN 'AMBIGUOUS' THEN 280
                    WHEN 'APPLIED' THEN 200
                    WHEN 'TRUE_DUPLICATE_APPLIED' THEN 200
                    ELSE 100
                  END DESC,
                  CASE attribution_status
                    WHEN 'BOT_OWNED_ATTRIBUTED' THEN 60
                    WHEN 'BOT_OWNED_MISSING_POSITION' THEN 50
                    WHEN 'BOT_OWNED_MISSING_LINEAGE' THEN 40
                    WHEN 'LEGACY_BOT_OWNED' THEN 30
                    WHEN 'EXTERNAL_OR_MANUAL_UNLINKED' THEN 20
                    WHEN 'UNKNOWN' THEN 10
                    ELSE 0
                  END DESC,
                  decided_at DESC,application_decision_id DESC
                LIMIT 1
                """,
                (str(fill_evidence_id),),
            )
            row = cur.fetchone()
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_APPLICATION_LOAD_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)
        return self._application_from_row(row) if row is not None else None

    def application_proof_matches(
        self,
        evidence: EntryFillEvidence,
        decision: EntryFillApplicationDecision,
    ) -> bool:
        """Revalidate every mutable source behind an APPLIED decision."""
        if decision.local_fill_id is None:
            return False
        observation = evidence.observation
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM binance_order_fills f
                  WHERE f.id=%s
                    AND lower(f.source)=%s
                    AND f.trade_id::text=%s
                    AND f.order_id=%s
                    AND f.symbol=%s
                    AND upper(f.side)=%s
                    AND f.executed_qty=%s
                    AND f.avg_price=%s
                    AND f.quote_notional_usdc=%s
                    AND f.commission_amount=%s
                    AND f.commission_asset IS NOT DISTINCT FROM %s
                    AND f.event_time=%s
                )
                """,
                (
                    decision.local_fill_id,
                    observation.exchange_source,
                    observation.exchange_trade_id,
                    observation.exchange_order_id,
                    observation.symbol,
                    observation.side,
                    observation.executed_qty,
                    observation.price,
                    observation.notional,
                    observation.fee,
                    observation.fee_asset,
                    observation.executed_at,
                ),
            )
            row = cur.fetchone()
            local_fill_matches = bool(row and row[0])
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_APPLICATION_PROOF_LOAD_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)
        if not local_fill_matches:
            return False
        reason = decision.decision_payload.get("reason")
        bridge_decision = (
            isinstance(reason, Mapping)
            and reason.get("decision_kind")
            == "EXISTING_LOCAL_APPLICATION_PROOF"
        )
        if not bridge_decision:
            return True
        discovered = self.load_existing_application_proof(evidence)
        return (
            discovered is not None
            and int(discovered[0]) == int(decision.local_fill_id)
            and discovered[1] == decision.applied_at
        )

    def load_existing_application_proof(
        self,
        evidence: EntryFillEvidence,
    ) -> tuple[int, datetime] | None:
        """Read an already-committed legacy/local-fill application proof."""
        observation = evidence.observation
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT f.id,state.applied_at
                FROM binance_order_fills f
                JOIN exchange_fill_ingestion_state_v2 state
                  ON state.source=f.source
                 AND state.symbol=f.symbol
                 AND state.trade_id=f.trade_id::text
                 AND state.local_fill_id=f.id
                WHERE lower(f.source)=%s
                  AND f.trade_id::text=%s
                  AND f.order_id=%s
                  AND f.symbol=%s
                  AND upper(f.side)=%s
                  AND f.executed_qty=%s
                  AND f.avg_price=%s
                  AND f.quote_notional_usdc=%s
                  AND f.commission_amount=%s
                  AND f.commission_asset IS NOT DISTINCT FROM %s
                  AND f.event_time=%s
                  AND state.authoritative_payload->>'exchange'=%s
                  AND state.authoritative_payload->>'instrument'=%s
                  AND state.authoritative_payload->>'trade_id'=%s
                  AND state.authoritative_payload->>'order_id'=%s
                  AND state.authoritative_payload->>'side'=%s
                  AND NULLIF(
                    state.authoritative_payload->>'executed_qty',''
                  )::numeric=%s
                  AND NULLIF(
                    state.authoritative_payload->>'fill_price',''
                  )::numeric=%s
                  AND COALESCE(NULLIF(
                    state.authoritative_payload->>'fee_quantity',''
                  )::numeric,0)=%s
                  AND COALESCE(
                    state.authoritative_payload->>'fee_currency',''
                  )=COALESCE(%s,'')
                  AND (
                    state.authoritative_payload->>'event_time_ms'
                  )::bigint=(
                    extract(epoch FROM f.event_time)*1000
                  )::bigint
                  AND state.adoption_id=%s
                  AND state.contract_generation=%s
                  AND state.applied_fingerprint=state.source_fingerprint
                  AND state.applied_at IS NOT NULL
                  AND state.application_status IN (
                    'APPLIED','CORRECTION_APPLIED','TRUE_DUPLICATE_APPLIED'
                  )
                ORDER BY f.id,state.ingestion_id
                """,
                (
                    observation.exchange_source,
                    observation.exchange_trade_id,
                    observation.exchange_order_id,
                    observation.symbol,
                    observation.side,
                    observation.executed_qty,
                    observation.price,
                    observation.notional,
                    observation.fee,
                    observation.fee_asset,
                    observation.executed_at,
                    observation.exchange_source,
                    observation.symbol,
                    observation.exchange_trade_id,
                    observation.exchange_order_id,
                    observation.side,
                    observation.executed_qty,
                    observation.price,
                    observation.fee,
                    observation.fee_asset,
                    observation.adoption_id,
                    observation.generation,
                ),
            )
            rows = list(cur.fetchall())
            if len(rows) > 1:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_APPLICATION_PROOF_AMBIGUOUS"
                )
            if not rows:
                return None
            return int(rows[0][0]), _aware(rows[0][1], "applied_at")
        except EntryFillRepositoryError:
            raise
        except Exception as exc:
            raise EntryFillRepositoryError(
                "ENTRY_FILL_APPLICATION_PROOF_DISCOVERY_FAILED"
            ) from exc
        finally:
            self._cleanup(conn, cur, rollback=True)

    def append_application(
        self, decision: EntryFillApplicationDecision
    ) -> FillApplicationInsertOutcome:
        conn = self._connection_factory()
        cur = conn.cursor()
        try:
            # Serialize append decisions for one immutable evidence identity.
            # The lock and the hard-state check are separate statements so a
            # waiter gets a fresh READ COMMITTED snapshot after the lock.
            cur.execute(
                """
                SELECT fill_evidence_id
                FROM live_entry_fill_evidence_v1
                WHERE fill_evidence_id=%s
                FOR UPDATE
                """,
                (str(decision.fill_evidence_id),),
            )
            if cur.fetchone() is None:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_APPLICATION_EVIDENCE_MISSING"
                )
            hard_statuses = {
                FillApplicationStatus.IDEMPOTENCY_CONFLICT,
                FillApplicationStatus.CORRECTION_PENDING,
                FillApplicationStatus.AMBIGUOUS,
            }
            if decision.application_status not in hard_statuses:
                cur.execute(
                    """
                    SELECT application_decision_id
                    FROM live_entry_fill_applications_v1
                    WHERE fill_evidence_id=%s
                      AND application_status IN (
                        'IDEMPOTENCY_CONFLICT',
                        'CORRECTION_PENDING',
                        'AMBIGUOUS'
                      )
                    LIMIT 1
                    """,
                    (str(decision.fill_evidence_id),),
                )
                if cur.fetchone() is not None:
                    self._cleanup(conn, cur, rollback=True)
                    return FillApplicationInsertOutcome.IDEMPOTENCY_CONFLICT
            cur.execute(
                """
                INSERT INTO live_entry_fill_applications_v1(
                  application_decision_id,fill_evidence_id,environment,
                  deployment_id,adoption_id,generation,git_revision,
                  exchange_source,client_order_id,intent_id,
                  submission_attempt_id,ack_id,strategy,"interval",order_purpose,
                  local_fill_id,linked_position_id,attribution_status,
                  attribution_fingerprint,application_status,
                  application_target_identity,canonical_source_fingerprint,
                  observed_source_fingerprint,applied_fingerprint,applied_at,
                  decision_fingerprint,decision_payload,decided_at,
                  producer_identity,contract_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s
                )
                ON CONFLICT DO NOTHING
                RETURNING application_decision_id,decision_fingerprint
                """,
                (
                    str(decision.application_decision_id),
                    str(decision.fill_evidence_id),
                    decision.environment.value,
                    decision.deployment_id.value,
                    decision.adoption_id,
                    decision.generation,
                    decision.git_revision,
                    decision.exchange_source,
                    decision.client_order_id,
                    str(decision.intent_id) if decision.intent_id else None,
                    (
                        str(decision.submission_attempt_id)
                        if decision.submission_attempt_id else None
                    ),
                    str(decision.ack_id) if decision.ack_id else None,
                    decision.strategy,
                    decision.interval,
                    decision.order_purpose,
                    decision.local_fill_id,
                    decision.linked_position_id,
                    decision.attribution_status.value,
                    decision.attribution_fingerprint,
                    decision.application_status.value,
                    decision.application_target_identity,
                    decision.canonical_source_fingerprint,
                    decision.observed_source_fingerprint,
                    decision.applied_fingerprint,
                    decision.applied_at,
                    decision.decision_fingerprint,
                    _canonical_json(decision.decision_payload),
                    decision.decided_at,
                    decision.producer_identity,
                    decision.contract_version.value,
                ),
            )
            inserted = cur.fetchone()
            if inserted is not None:
                outcome = FillApplicationInsertOutcome.CREATED
            else:
                cur.execute(
                    """
                    SELECT application_decision_id,decision_fingerprint
                    FROM live_entry_fill_applications_v1
                    WHERE fill_evidence_id=%s AND decision_fingerprint=%s
                    """,
                    (
                        str(decision.fill_evidence_id),
                        decision.decision_fingerprint,
                    ),
                )
                existing = cur.fetchone()
                outcome = (
                    FillApplicationInsertOutcome.IDEMPOTENT_EXISTING
                    if existing is not None
                    and uuid.UUID(str(existing[0]))
                    == decision.application_decision_id
                    and str(existing[1]) == decision.decision_fingerprint
                    else FillApplicationInsertOutcome.IDEMPOTENCY_CONFLICT
                )
        except Exception as exc:
            self._cleanup(conn, cur, rollback=True)
            raise EntryFillRepositoryError(
                "ENTRY_FILL_APPLICATION_COMMIT_FAILED"
            ) from exc
        try:
            conn.commit()
        except Exception as exc:
            self._cleanup(conn, cur, rollback=True)
            raise EntryFillRepositoryError(
                "ENTRY_FILL_APPLICATION_COMMIT_OUTCOME_UNKNOWN"
            ) from exc
        self._cleanup(conn, cur, rollback=False)
        return outcome


def _event_for_application(
    status: FillApplicationStatus,
) -> EntryFillEventType:
    return {
        FillApplicationStatus.OBSERVED_NOT_APPLIED: (
            EntryFillEventType.ENTRY_FILL_OBSERVED_NOT_APPLIED
        ),
        FillApplicationStatus.APPLIED: (
            EntryFillEventType.ENTRY_FILL_OBSERVED_NOT_APPLIED
        ),
        FillApplicationStatus.TRUE_DUPLICATE_APPLIED: (
            EntryFillEventType.ENTRY_FILL_TRUE_DUPLICATE_APPLIED
        ),
        FillApplicationStatus.IDEMPOTENCY_CONFLICT: (
            EntryFillEventType.ENTRY_FILL_CONFLICT
        ),
        FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED: (
            EntryFillEventType.ENTRY_FILL_EXTERNAL_UNLINKED
        ),
        FillApplicationStatus.AMBIGUOUS: (
            EntryFillEventType.ENTRY_FILL_AMBIGUOUS
        ),
        FillApplicationStatus.CORRECTION_PENDING: (
            EntryFillEventType.ENTRY_FILL_CORRECTION_PENDING
        ),
    }[status]


def _processing_outcome(
    evidence_outcome: FillEvidenceInsertOutcome,
    status: FillApplicationStatus,
) -> EntryFillProcessingOutcome:
    if status is FillApplicationStatus.TRUE_DUPLICATE_APPLIED:
        return EntryFillProcessingOutcome.TRUE_DUPLICATE_APPLIED
    if status is FillApplicationStatus.IDEMPOTENCY_CONFLICT:
        return EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    if status is FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED:
        return EntryFillProcessingOutcome.EXTERNAL_UNLINKED
    if status is FillApplicationStatus.AMBIGUOUS:
        return EntryFillProcessingOutcome.AMBIGUOUS
    if status is FillApplicationStatus.CORRECTION_PENDING:
        return EntryFillProcessingOutcome.CORRECTION_PENDING
    return (
        EntryFillProcessingOutcome.EVIDENCE_RECORDED
        if evidence_outcome is FillEvidenceInsertOutcome.CREATED
        else EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    )


_ATTRIBUTION_STRENGTH = {
    FillAttributionStatus.BOT_OWNED_ATTRIBUTED: 60,
    FillAttributionStatus.BOT_OWNED_MISSING_POSITION: 50,
    FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE: 40,
    FillAttributionStatus.LEGACY_BOT_OWNED: 30,
    FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED: 20,
    FillAttributionStatus.UNKNOWN: 10,
    FillAttributionStatus.AMBIGUOUS: 0,
    FillAttributionStatus.CONFLICTED: 0,
}

_HARD_APPLICATION_STATES = {
    FillApplicationStatus.IDEMPOTENCY_CONFLICT,
    FillApplicationStatus.CORRECTION_PENDING,
    FillApplicationStatus.AMBIGUOUS,
}


def _initial_application_status(
    lineage: FillLineageResolution,
) -> FillApplicationStatus:
    if lineage.status is FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED:
        return FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
    if lineage.status is FillAttributionStatus.AMBIGUOUS:
        return FillApplicationStatus.AMBIGUOUS
    if lineage.status is FillAttributionStatus.CONFLICTED:
        return FillApplicationStatus.IDEMPOTENCY_CONFLICT
    return FillApplicationStatus.OBSERVED_NOT_APPLIED


def _canonical_hard_lineage(
    canonical: EntryFillEvidence,
    observed: FillLineageResolution,
    latest: EntryFillApplicationDecision | None = None,
) -> FillLineageResolution:
    """Retain accepted lineage while recording a new fail-closed verdict."""
    accepted = canonical.lineage
    return FillLineageResolution(
        status=observed.status,
        method=observed.method,
        intent_id=(
            latest.intent_id
            if latest is not None and latest.intent_id is not None
            else accepted.intent_id
        ),
        submission_attempt_id=(
            latest.submission_attempt_id
            if latest is not None
            and latest.submission_attempt_id is not None
            else accepted.submission_attempt_id
        ),
        ack_id=(
            latest.ack_id
            if latest is not None and latest.ack_id is not None
            else accepted.ack_id
        ),
        client_order_id=(
            latest.client_order_id
            if latest is not None and latest.client_order_id is not None
            else accepted.client_order_id
        ),
        strategy=(
            latest.strategy
            if latest is not None and latest.strategy is not None
            else accepted.strategy
        ),
        interval=(
            latest.interval
            if latest is not None and latest.interval is not None
            else accepted.interval
        ),
        order_purpose=(
            latest.order_purpose
            if latest is not None and latest.order_purpose is not None
            else accepted.order_purpose
        ),
        linked_position_id=(
            latest.linked_position_id
            if latest is not None and latest.linked_position_id is not None
            else accepted.linked_position_id
        ),
        detail=observed.detail,
    )


def _application_lineage(
    decision: EntryFillApplicationDecision,
) -> FillLineageResolution:
    return FillLineageResolution(
        status=decision.attribution_status,
        method=str(
            decision.decision_payload.get("attribution_method")
            or "APPLICATION_DECISION"
        ),
        intent_id=decision.intent_id,
        submission_attempt_id=decision.submission_attempt_id,
        ack_id=decision.ack_id,
        client_order_id=decision.client_order_id,
        strategy=decision.strategy,
        interval=decision.interval,
        order_purpose=decision.order_purpose,
        linked_position_id=decision.linked_position_id,
        detail=(
            str(decision.decision_payload.get("attribution_detail"))
            if decision.decision_payload.get("attribution_detail") is not None
            else None
        ),
    )


def _lineage_identity_values(
    lineage: FillLineageResolution,
) -> Mapping[str, object | None]:
    return {
        "intent_id": lineage.intent_id,
        "submission_attempt_id": lineage.submission_attempt_id,
        "ack_id": lineage.ack_id,
        "client_order_id": lineage.client_order_id,
        "strategy": lineage.strategy,
        "interval": lineage.interval,
        "linked_position_id": lineage.linked_position_id,
        "order_purpose": lineage.order_purpose,
    }


def _lineage_compatibility(
    canonical: EntryFillEvidence,
    latest: EntryFillApplicationDecision | None,
    candidate: FillLineageResolution,
    *,
    require_refinement: bool,
) -> bool:
    """Require all previously known lineage identities to remain stable."""
    existing = dict(_lineage_identity_values(canonical.lineage))
    if latest is not None:
        for field in existing:
            value = getattr(latest, field)
            if value is not None:
                existing[field] = value
    observed = _lineage_identity_values(candidate)
    for field, prior in existing.items():
        current = observed[field]
        if prior is not None and current is not None and prior != current:
            return False
        if require_refinement and prior is not None and current is None:
            return False
    return True


def _reported_application_status(
    evidence: EntryFillEvidence,
    current: EntryFillApplicationDecision,
) -> FillApplicationStatus:
    if current.application_status in {
        FillApplicationStatus.APPLIED,
        FillApplicationStatus.TRUE_DUPLICATE_APPLIED,
    }:
        return classify_application_state(
            evidence,
            current,
            observed_source_fingerprint=evidence.source_fingerprint,
            observed_attribution_fingerprint=current.attribution_fingerprint,
        )
    return current.application_status


def process_entry_fill_attribution(
    *,
    mode: EntryFillAttributionMode | str,
    observation: EntryFillObservation,
    repository: EntryFillAttributionRepositoryProtocol | None,
    clock: Callable[[], datetime] | None = None,
) -> EntryFillProcessingResult:
    """Observe one fill without creating/updating a position or inventory."""
    selected_mode = EntryFillAttributionMode(mode)
    now = clock or (lambda: datetime.now(timezone.utc))
    events: list[EntryFillEvent] = [
        EntryFillEvent(
            EntryFillEventType.ENTRY_FILL_OBSERVED,
            None,
            _aware(now(), "event time"),
        )
    ]
    if selected_mode is EntryFillAttributionMode.OFF:
        return EntryFillProcessingResult(
            EntryFillProcessingOutcome.MODE_OFF,
            None,
            None,
            None,
            tuple(events),
        )
    if repository is None:
        return EntryFillProcessingResult(
            EntryFillProcessingOutcome.REPOSITORY_ERROR,
            None,
            None,
            None,
            tuple(events),
            "ENTRY_FILL_REPOSITORY_REQUIRED",
        )

    try:
        lineage = repository.resolve_lineage(observation)
        candidate = EntryFillEvidence.build(observation, lineage)
        evidence_outcome = repository.commit_evidence(candidate)
        canonical = candidate
        if evidence_outcome is not FillEvidenceInsertOutcome.CREATED:
            loaded = repository.load_evidence(candidate.natural_key)
            if loaded is None:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_CANONICAL_EVIDENCE_MISSING"
                )
            canonical = loaded
        if evidence_outcome in {
            FillEvidenceInsertOutcome.CREATED,
            FillEvidenceInsertOutcome.IDEMPOTENT_EXISTING,
        }:
            events.append(
                EntryFillEvent(
                    (
                        EntryFillEventType.ENTRY_FILL_EVIDENCE_CREATED
                        if evidence_outcome
                        is FillEvidenceInsertOutcome.CREATED
                        else EntryFillEventType.ENTRY_FILL_EVIDENCE_IDEMPOTENT
                    ),
                    canonical.fill_evidence_id,
                    _aware(now(), "event time"),
                )
            )
        latest = repository.load_latest_application(canonical.fill_evidence_id)
        effective_status = (
            latest.attribution_status
            if latest is not None else canonical.lineage.status
        )
        effective_fingerprint = (
            latest.attribution_fingerprint
            if latest is not None else canonical.attribution_fingerprint
        )
        status = (
            latest.application_status
            if latest is not None
            else _initial_application_status(canonical.lineage)
        )
        decision_lineage: FillLineageResolution | None = None
        decision_kind: str | None = None
        application_proof: tuple[int, datetime] | None = None

        if evidence_outcome is FillEvidenceInsertOutcome.IDEMPOTENCY_CONFLICT:
            status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
            # Preserve a currently accepted lineage for economics-only
            # corrections.  A changed order/CID attribution is untrusted and
            # gets an explicit CONFLICTED verdict without replacing evidence.
            if candidate.attribution_fingerprint == effective_fingerprint:
                decision_lineage = lineage
            else:
                decision_lineage = _canonical_hard_lineage(
                    canonical,
                    FillLineageResolution(
                        FillAttributionStatus.CONFLICTED,
                        "FAIL_CLOSED",
                        detail="SOURCE_PAYLOAD_LINEAGE_CONFLICT",
                    ),
                    latest,
                )
            decision_kind = "SOURCE_PAYLOAD_CONFLICT"
        elif lineage.status is FillAttributionStatus.CONFLICTED:
            status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
            decision_lineage = _canonical_hard_lineage(
                canonical, lineage, latest
            )
            decision_kind = "LINEAGE_CONFLICT"
        elif lineage.status is FillAttributionStatus.AMBIGUOUS:
            status = FillApplicationStatus.AMBIGUOUS
            decision_lineage = _canonical_hard_lineage(
                canonical, lineage, latest
            )
            decision_kind = "LINEAGE_AMBIGUOUS"
        elif latest is not None and latest.application_status in (
            _HARD_APPLICATION_STATES
        ):
            # Hard states are append-only and require an explicit future
            # resolution contract; ordinary replay cannot erase them.
            status = latest.application_status
        elif latest is not None and latest.application_status in {
            FillApplicationStatus.APPLIED,
            FillApplicationStatus.TRUE_DUPLICATE_APPLIED,
        }:
            if not repository.application_proof_matches(canonical, latest):
                status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
                decision_lineage = _canonical_hard_lineage(
                    canonical,
                    FillLineageResolution(
                        FillAttributionStatus.CONFLICTED,
                        "FAIL_CLOSED",
                        detail="LOCAL_APPLICATION_PROOF_DRIFT",
                    ),
                    latest,
                )
                decision_kind = "LOCAL_APPLICATION_PROOF_DRIFT"
            elif candidate.attribution_fingerprint != effective_fingerprint:
                status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
                decision_lineage = _canonical_hard_lineage(
                    canonical,
                    FillLineageResolution(
                        FillAttributionStatus.CONFLICTED,
                        "FAIL_CLOSED",
                        detail="APPLIED_ATTRIBUTION_IDENTITY_CHANGED",
                    ),
                    latest,
                )
                decision_kind = "APPLIED_ATTRIBUTION_CONFLICT"
            else:
                status = _reported_application_status(canonical, latest)
        else:
            candidate_strength = _ATTRIBUTION_STRENGTH[lineage.status]
            effective_strength = _ATTRIBUTION_STRENGTH[effective_status]
            compatible = _lineage_compatibility(
                canonical,
                latest,
                lineage,
                require_refinement=candidate_strength > effective_strength,
            )
            if not compatible:
                status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
                decision_lineage = _canonical_hard_lineage(
                    canonical,
                    FillLineageResolution(
                        FillAttributionStatus.CONFLICTED,
                        "FAIL_CLOSED",
                        detail="LINEAGE_IDENTITY_CHANGED",
                    ),
                    latest,
                )
                decision_kind = "LINEAGE_IDENTITY_CONFLICT"
            elif candidate_strength > effective_strength:
                status = _initial_application_status(lineage)
                decision_lineage = lineage
                decision_kind = "ATTRIBUTION_UPGRADE"
                effective_status = lineage.status
                effective_fingerprint = candidate.attribution_fingerprint
            elif (
                candidate_strength == effective_strength
                and candidate.attribution_fingerprint
                != effective_fingerprint
            ):
                status = FillApplicationStatus.IDEMPOTENCY_CONFLICT
                decision_lineage = _canonical_hard_lineage(
                    canonical,
                    FillLineageResolution(
                        FillAttributionStatus.CONFLICTED,
                        "FAIL_CLOSED",
                        detail="LINEAGE_FINGERPRINT_CHANGED",
                    ),
                    latest,
                )
                decision_kind = "LINEAGE_IDENTITY_CONFLICT"
            elif latest is None:
                status = _initial_application_status(canonical.lineage)
                decision_lineage = canonical.lineage
                decision_kind = "INITIAL_CLASSIFICATION"

        if status is FillApplicationStatus.OBSERVED_NOT_APPLIED:
            proof_lineage = (
                decision_lineage
                if decision_lineage is not None
                else (
                    _application_lineage(latest)
                    if latest is not None else canonical.lineage
                )
            )
            if proof_lineage.status in {
                FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
                FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
            }:
                application_proof = (
                    repository.load_existing_application_proof(canonical)
                )
                if application_proof is not None:
                    status = FillApplicationStatus.APPLIED
                    decision_lineage = proof_lineage
                    decision_kind = "EXISTING_LOCAL_APPLICATION_PROOF"

        if decision_lineage is not None:
            decision = EntryFillApplicationDecision.build(
                canonical,
                lineage=decision_lineage,
                application_status=status,
                decided_at=_aware(now(), "decision time"),
                producer_identity=observation.producer_identity,
                observed_source_fingerprint=candidate.source_fingerprint,
                local_fill_id=(
                    application_proof[0]
                    if application_proof is not None else None
                ),
                applied_fingerprint=(
                    canonical.source_fingerprint
                    if application_proof is not None else None
                ),
                applied_at=(
                    application_proof[1]
                    if application_proof is not None else None
                ),
                application_target_identity=(
                    f"binance_order_fills:{application_proof[0]}"
                    if application_proof is not None else None
                ),
                decision_payload={
                    "decision_kind": decision_kind,
                    "lineage_detail": decision_lineage.detail,
                    "lineage_method": decision_lineage.method,
                    "observed_lineage": {
                        "attribution_fingerprint": (
                            lineage.attribution_fingerprint
                        ),
                        "detail": lineage.detail,
                        "identity": lineage.identity_payload,
                        "method": lineage.method,
                        "status": lineage.status.value,
                    },
                    "observed_semantic_payload": observation.semantic_payload,
                },
            )
            repository.append_application(decision)
            current = repository.load_latest_application(
                canonical.fill_evidence_id
            )
            if current is None:
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_APPLICATION_DECISION_MISSING"
                )
            if current.application_status in {
                FillApplicationStatus.APPLIED,
                FillApplicationStatus.TRUE_DUPLICATE_APPLIED,
            } and not repository.application_proof_matches(
                canonical, current
            ):
                raise EntryFillRepositoryError(
                    "ENTRY_FILL_APPLICATION_PROOF_CHANGED_DURING_DECISION"
                )
            status = _reported_application_status(canonical, current)
            effective_status = current.attribution_status

        if effective_status in {
            FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
            FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
        }:
            events.append(
                EntryFillEvent(
                    EntryFillEventType.ENTRY_FILL_ATTRIBUTED,
                    canonical.fill_evidence_id,
                    _aware(now(), "event time"),
                    effective_status.value,
                )
            )
        events.append(
            EntryFillEvent(
                _event_for_application(status),
                canonical.fill_evidence_id,
                _aware(now(), "event time"),
                status.value,
            )
        )
        return EntryFillProcessingResult(
            _processing_outcome(evidence_outcome, status),
            status,
            effective_status,
            canonical,
            tuple(events),
        )
    except Exception as exc:
        return EntryFillProcessingResult(
            EntryFillProcessingOutcome.REPOSITORY_ERROR,
            None,
            None,
            None,
            tuple(events),
            str(exc),
        )


def recover_entry_fill_attribution(
    *,
    observation: EntryFillObservation,
    repository: EntryFillAttributionRepositoryProtocol,
    clock: Callable[[], datetime] | None = None,
) -> EntryFillProcessingResult:
    """Explicit, single-observation recovery entry point; never scans history."""
    return process_entry_fill_attribution(
        mode=EntryFillAttributionMode.ENFORCE,
        observation=observation,
        repository=repository,
        clock=clock,
    )

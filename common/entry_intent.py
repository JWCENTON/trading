from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Mapping


ENTRY_INTENT_UUID_NAMESPACE = uuid.UUID(
    "a2df4cc7-67de-5fb6-9e41-fac24d12d758"
)


class EntryIntentEnvironment(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class EntryIntentDeployment(str, Enum):
    LOCAL_PAPER = "local-paper"
    LOCAL_LIVE = "local-live"
    VPS_PAPER = "vps-paper"
    VPS_LIVE = "vps-live"


class EntryIntentOrderPurpose(str, Enum):
    ENTRY = "ENTRY"


class EntryIntentSide(str, Enum):
    BUY = "BUY"


class EntryIntentContractVersion(str, Enum):
    V1 = "LIVE_ENTRY_INTENT_V1"


class EntryIntentInsertOutcome(str, Enum):
    CREATED = "CREATED"
    IDEMPOTENT_EXISTING = "IDEMPOTENT_EXISTING"
    CONFLICT = "CONFLICT"


def canonical_decimal(value: Decimal | str | int) -> str:
    if isinstance(value, float):
        raise ValueError("requested_qty must not use binary float")
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("requested_qty must be a Decimal-compatible value") from exc
    if not quantity.is_finite():
        raise ValueError("requested_qty must be finite")
    rendered = format(quantity, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"", "-0"} else rendered


def canonical_slot_identity(symbol: str, strategy: str, interval: str) -> str:
    return f"{symbol}:{strategy}:{interval}"


def _require_nonempty(value: str, field: str) -> str:
    text = str(value)
    if not text or text != text.strip():
        raise ValueError(f"{field} must be nonempty and have no outer whitespace")
    return text


def _canonical_json(payload: Mapping[str, str | int]) -> str:
    return json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


@dataclass(frozen=True, slots=True)
class LiveEntryIntent:
    intent_id: uuid.UUID
    environment: EntryIntentEnvironment
    deployment_id: EntryIntentDeployment
    git_revision: str
    adoption_id: int
    generation: int
    decision_id: uuid.UUID
    symbol: str
    strategy: str
    interval: str
    slot_identity: str
    exchange_source: str
    client_order_id: str
    order_purpose: EntryIntentOrderPurpose
    side: EntryIntentSide
    requested_qty: Decimal
    content_fingerprint: str
    prepared_at: datetime
    producer_identity: str
    contract_version: EntryIntentContractVersion

    def __post_init__(self) -> None:
        try:
            environment = EntryIntentEnvironment(self.environment)
            deployment = EntryIntentDeployment(self.deployment_id)
            purpose = EntryIntentOrderPurpose(self.order_purpose)
            side = EntryIntentSide(self.side)
            version = EntryIntentContractVersion(self.contract_version)
        except ValueError as exc:
            raise ValueError("entry intent contains a noncanonical enum value") from exc

        if deployment.value.split("-", 1)[1] != environment.value:
            raise ValueError("deployment_id does not match environment")
        git_revision = _require_nonempty(self.git_revision, "git_revision")
        if len(git_revision) != 40 or any(
            char not in "0123456789abcdef" for char in git_revision
        ):
            raise ValueError("git_revision must be 40 lowercase hex characters")
        if int(self.adoption_id) <= 0:
            raise ValueError("adoption_id must be positive")
        if int(self.generation) <= 0:
            raise ValueError("generation must be positive")
        if not isinstance(self.decision_id, uuid.UUID):
            raise ValueError("decision_id must be UUID")
        if self.symbol != _require_nonempty(self.symbol, "symbol").upper():
            raise ValueError("symbol must be canonical uppercase")
        if self.strategy != _require_nonempty(self.strategy, "strategy").upper():
            raise ValueError("strategy must be canonical uppercase")
        if self.interval != _require_nonempty(self.interval, "interval").lower():
            raise ValueError("interval must be canonical lowercase")
        expected_slot = canonical_slot_identity(
            self.symbol, self.strategy, self.interval
        )
        if self.slot_identity != expected_slot:
            raise ValueError("slot_identity does not match symbol/strategy/interval")
        if self.exchange_source != _require_nonempty(
            self.exchange_source, "exchange_source"
        ).lower():
            raise ValueError("exchange_source must be canonical lowercase")
        _require_nonempty(self.client_order_id, "client_order_id")
        _require_nonempty(self.producer_identity, "producer_identity")
        quantity = Decimal(canonical_decimal(self.requested_qty))
        if quantity <= 0:
            raise ValueError("requested_qty must be positive")
        if self.prepared_at.tzinfo is None or self.prepared_at.utcoffset() is None:
            raise ValueError("prepared_at must be timezone-aware")
        if self.intent_id != self.deterministic_intent_id(
            environment=environment,
            deployment_id=deployment,
            exchange_source=self.exchange_source,
            client_order_id=self.client_order_id,
        ):
            raise ValueError("intent_id does not match the natural identity")
        if self.content_fingerprint != self.calculate_fingerprint(
            environment=environment,
            deployment_id=deployment,
            git_revision=git_revision,
            adoption_id=self.adoption_id,
            generation=self.generation,
            decision_id=self.decision_id,
            symbol=self.symbol,
            strategy=self.strategy,
            interval=self.interval,
            slot_identity=self.slot_identity,
            exchange_source=self.exchange_source,
            client_order_id=self.client_order_id,
            order_purpose=purpose,
            side=side,
            requested_qty=self.requested_qty,
            producer_identity=self.producer_identity,
            contract_version=version,
        ):
            raise ValueError("content_fingerprint does not match intent content")

    @staticmethod
    def deterministic_intent_id(
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        exchange_source: str,
        client_order_id: str,
    ) -> uuid.UUID:
        identity = _canonical_json(
            {
                "client_order_id": _require_nonempty(
                    client_order_id, "client_order_id"
                ),
                "deployment_id": EntryIntentDeployment(deployment_id).value,
                "environment": EntryIntentEnvironment(environment).value,
                "exchange_source": _require_nonempty(
                    exchange_source, "exchange_source"
                ),
                "identity_version": "LIVE_ENTRY_INTENT_ID_V1",
            }
        )
        return uuid.uuid5(ENTRY_INTENT_UUID_NAMESPACE, identity)

    @staticmethod
    def calculate_fingerprint(
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        git_revision: str,
        adoption_id: int,
        generation: int,
        decision_id: uuid.UUID,
        symbol: str,
        strategy: str,
        interval: str,
        slot_identity: str,
        exchange_source: str,
        client_order_id: str,
        order_purpose: EntryIntentOrderPurpose | str,
        side: EntryIntentSide | str,
        requested_qty: Decimal | str | int,
        producer_identity: str,
        contract_version: EntryIntentContractVersion | str,
    ) -> str:
        payload = {
            "adoption_id": int(adoption_id),
            "client_order_id": str(client_order_id),
            "contract_version": EntryIntentContractVersion(
                contract_version
            ).value,
            "decision_id": str(decision_id),
            "deployment_id": EntryIntentDeployment(deployment_id).value,
            "environment": EntryIntentEnvironment(environment).value,
            "exchange_source": str(exchange_source),
            "generation": int(generation),
            "git_revision": str(git_revision),
            "interval": str(interval),
            "order_purpose": EntryIntentOrderPurpose(order_purpose).value,
            "producer_identity": str(producer_identity),
            "requested_qty": canonical_decimal(requested_qty),
            "side": EntryIntentSide(side).value,
            "slot_identity": str(slot_identity),
            "strategy": str(strategy),
            "symbol": str(symbol),
        }
        return hashlib.sha256(_canonical_json(payload).encode("ascii")).hexdigest()

    @classmethod
    def build(
        cls,
        *,
        environment: EntryIntentEnvironment | str,
        deployment_id: EntryIntentDeployment | str,
        git_revision: str,
        adoption_id: int,
        generation: int,
        decision_id: uuid.UUID,
        symbol: str,
        strategy: str,
        interval: str,
        exchange_source: str,
        client_order_id: str,
        requested_qty: Decimal | str | int,
        prepared_at: datetime,
        producer_identity: str,
    ) -> "LiveEntryIntent":
        environment_value = EntryIntentEnvironment(environment)
        deployment_value = EntryIntentDeployment(deployment_id)
        purpose = EntryIntentOrderPurpose.ENTRY
        side = EntryIntentSide.BUY
        version = EntryIntentContractVersion.V1
        quantity = Decimal(canonical_decimal(requested_qty))
        slot_identity = canonical_slot_identity(symbol, strategy, interval)
        intent_id = cls.deterministic_intent_id(
            environment=environment_value,
            deployment_id=deployment_value,
            exchange_source=exchange_source,
            client_order_id=client_order_id,
        )
        fingerprint = cls.calculate_fingerprint(
            environment=environment_value,
            deployment_id=deployment_value,
            git_revision=git_revision,
            adoption_id=adoption_id,
            generation=generation,
            decision_id=decision_id,
            symbol=symbol,
            strategy=strategy,
            interval=interval,
            slot_identity=slot_identity,
            exchange_source=exchange_source,
            client_order_id=client_order_id,
            order_purpose=purpose,
            side=side,
            requested_qty=quantity,
            producer_identity=producer_identity,
            contract_version=version,
        )
        return cls(
            intent_id=intent_id,
            environment=environment_value,
            deployment_id=deployment_value,
            git_revision=git_revision,
            adoption_id=int(adoption_id),
            generation=int(generation),
            decision_id=decision_id,
            symbol=symbol,
            strategy=strategy,
            interval=interval,
            slot_identity=slot_identity,
            exchange_source=exchange_source,
            client_order_id=client_order_id,
            order_purpose=purpose,
            side=side,
            requested_qty=quantity,
            content_fingerprint=fingerprint,
            prepared_at=prepared_at,
            producer_identity=producer_identity,
            contract_version=version,
        )

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return (
            self.environment.value,
            self.deployment_id.value,
            self.exchange_source,
            self.client_order_id,
        )


def classify_insert_outcome(
    existing_fingerprint: str | None,
    candidate: LiveEntryIntent,
) -> EntryIntentInsertOutcome:
    if existing_fingerprint is None:
        return EntryIntentInsertOutcome.CREATED
    if str(existing_fingerprint) == candidate.content_fingerprint:
        return EntryIntentInsertOutcome.IDEMPOTENT_EXISTING
    return EntryIntentInsertOutcome.CONFLICT

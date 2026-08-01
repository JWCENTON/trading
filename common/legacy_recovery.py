from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Sequence


ZERO = Decimal("0")
SERIALIZER_VERSION = "LEGACY_RECOVERY_SEMANTIC_V1"


def _current_database(cur) -> str:
    cur.execute("SELECT current_database()")
    return str(cur.fetchone()[0])


class PrecisionStatus(str, Enum):
    EXACT_ZERO = "EXACT_ZERO"
    DUST_WITHIN_INSTRUMENT_PRECISION = "DUST_WITHIN_INSTRUMENT_PRECISION"
    REAL_REMAINING_INVENTORY = "REAL_REMAINING_INVENTORY"
    OVER_EXIT_WITHIN_PRECISION = "OVER_EXIT_WITHIN_PRECISION"
    OVER_EXIT_CONFLICT = "OVER_EXIT_CONFLICT"


class RecomputationStatus(str, Enum):
    COMPLETE_CLOSED = "COMPLETE_CLOSED"
    COMPLETE_OPEN = "COMPLETE_OPEN"
    INCOMPLETE = "INCOMPLETE"
    CONFLICT = "CONFLICT"


class FeeValuationStatus(str, Enum):
    AUTHORITATIVE_QUOTE_FEE = "AUTHORITATIVE_QUOTE_FEE"
    AUTHORITATIVE_BASE_FEE_WITH_FILL_PRICE = (
        "AUTHORITATIVE_BASE_FEE_WITH_FILL_PRICE"
    )
    AUTHORITATIVE_BASE_FEE_WITH_EXTERNAL_VALUATION = (
        "AUTHORITATIVE_BASE_FEE_WITH_EXTERNAL_VALUATION"
    )
    ESTIMATED = "ESTIMATED"
    UNKNOWN = "UNKNOWN"


class IngestionApplicationStatus(str, Enum):
    OBSERVED_NOT_APPLIED = "OBSERVED_NOT_APPLIED"
    APPLIED = "APPLIED"
    TRUE_DUPLICATE_APPLIED = "TRUE_DUPLICATE_APPLIED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    EXTERNAL_OR_MANUAL_UNLINKED = "EXTERNAL_OR_MANUAL_UNLINKED"
    BLOCKED_MISSING_CONTEXT = "BLOCKED_MISSING_CONTEXT"


class OrderOwnership(str, Enum):
    BOT_OWNED = "BOT_OWNED"
    MANUAL_OR_EXTERNAL = "MANUAL_OR_EXTERNAL"
    AMBIGUOUS = "AMBIGUOUS"


class ProvenanceSource(str, Enum):
    STORED_IMMUTABLE_SNAPSHOT = "STORED_IMMUTABLE_SNAPSHOT"
    EXCHANGE_PAYLOAD = "EXCHANGE_PAYLOAD"
    CANONICAL_SYMBOL_RESOLVER = "CANONICAL_SYMBOL_RESOLVER"
    DEPLOYMENT_ACCOUNT_SNAPSHOT = "DEPLOYMENT_ACCOUNT_SNAPSHOT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class PrecisionPolicy:
    lot_size: Decimal
    exchange_base_precision: int
    known_fill_precision: int
    decimal_quantum: Decimal
    source: str

    def __post_init__(self) -> None:
        if self.lot_size <= ZERO or self.decimal_quantum <= ZERO:
            raise ValueError("precision quantities must be positive")
        if self.exchange_base_precision < 0 or self.known_fill_precision < 0:
            raise ValueError("precision digits must be non-negative")

    @property
    def normalization_boundary(self) -> Decimal:
        return max(
            self.lot_size,
            Decimal(1).scaleb(-self.exchange_base_precision),
            Decimal(1).scaleb(-self.known_fill_precision),
            self.decimal_quantum,
        )

    def classify(self, raw: Decimal) -> tuple[PrecisionStatus, Decimal, str]:
        if raw == ZERO:
            return PrecisionStatus.EXACT_ZERO, ZERO, "EXACT_ARITHMETIC_ZERO"
        if abs(raw) <= self.normalization_boundary:
            status = (
                PrecisionStatus.DUST_WITHIN_INSTRUMENT_PRECISION
                if raw > ZERO
                else PrecisionStatus.OVER_EXIT_WITHIN_PRECISION
            )
            return status, ZERO, "WITHIN_FORMAL_INSTRUMENT_PRECISION"
        if raw > ZERO:
            return (
                PrecisionStatus.REAL_REMAINING_INVENTORY,
                raw,
                "ABOVE_FORMAL_INSTRUMENT_PRECISION",
            )
        return (
            PrecisionStatus.OVER_EXIT_CONFLICT,
            raw,
            "OVER_EXIT_ABOVE_FORMAL_INSTRUMENT_PRECISION",
        )


@dataclass(frozen=True)
class FeeValuation:
    original_fee_quantity: Decimal
    original_fee_asset: str
    valuation_price: Decimal | None
    valuation_source: str | None
    valued_fee_usdc: Decimal | None
    status: FeeValuationStatus


def value_fee(
    *,
    quantity: Decimal,
    asset: str,
    base_asset: str,
    quote_asset: str,
    fill_price: Decimal | None,
    external_price: Decimal | None = None,
    estimated_price: Decimal | None = None,
) -> FeeValuation:
    asset = asset.upper()
    if asset == quote_asset.upper():
        return FeeValuation(
            quantity, asset, Decimal("1"), "IMMUTABLE_FILL_QUOTE_FEE",
            quantity, FeeValuationStatus.AUTHORITATIVE_QUOTE_FEE,
        )
    if asset == base_asset.upper() and fill_price is not None:
        return FeeValuation(
            quantity, asset, fill_price, "SAME_FILL_EXECUTION_PRICE",
            quantity * fill_price,
            FeeValuationStatus.AUTHORITATIVE_BASE_FEE_WITH_FILL_PRICE,
        )
    if asset == base_asset.upper() and external_price is not None:
        return FeeValuation(
            quantity, asset, external_price, "EXTERNAL_VALUATION_SNAPSHOT",
            quantity * external_price,
            FeeValuationStatus.AUTHORITATIVE_BASE_FEE_WITH_EXTERNAL_VALUATION,
        )
    if estimated_price is not None:
        return FeeValuation(
            quantity, asset, estimated_price, "ESTIMATED_VALUATION",
            quantity * estimated_price, FeeValuationStatus.ESTIMATED,
        )
    return FeeValuation(
        quantity, asset, None, None, None, FeeValuationStatus.UNKNOWN,
    )


@dataclass(frozen=True)
class LegacyFillEvidence:
    fill_id: str
    order_id: str
    trade_id: str
    side: str
    quantity: Decimal
    price: Decimal
    fee_quantity: Decimal
    fee_asset: str
    fee_valuation: FeeValuation


@dataclass(frozen=True)
class LegacyPositionEvidence:
    position_id: int
    symbol: str
    base_asset: str
    quote_asset: str
    entry_fills: tuple[LegacyFillEvidence, ...]
    exit_fills: tuple[LegacyFillEvidence, ...]
    precision_policy: PrecisionPolicy | None
    one_unambiguous_position: bool = True
    complete_entry_orders: bool = True
    complete_exit_orders: bool = True
    unambiguous_position_linkage: bool = True
    instrument_identity_resolved: bool = True
    no_unapplied_position_fills: bool = True
    no_pending_corrections: bool = True


@dataclass(frozen=True)
class LegacyPositionRecomputation:
    position_id: int
    recomputation_status: RecomputationStatus
    gross_entry_qty: Decimal
    base_asset_entry_fee_qty: Decimal
    net_entry_inventory_qty: Decimal
    gross_exit_qty: Decimal
    base_asset_exit_fee_qty: Decimal
    raw_remaining_qty: Decimal
    normalized_remaining_qty: Decimal
    precision_status: PrecisionStatus | None
    normalization_reason: str | None
    precision_source: str | None
    lifecycle_should_be_open: bool
    lifecycle_should_be_closed: bool
    financial_truth_eligibility: bool
    blocking_reasons: tuple[str, ...]
    evidence_fingerprint: str


def _canonical(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == ZERO:
            return "0"
        rendered = format(value.normalize(), "f")
        return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return _canonical(asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): _canonical(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, set):
        return sorted((_canonical(item) for item in value), key=str)
    return value


def canonical_semantic_bytes(value: Any) -> bytes:
    document = {
        "serializer_version": SERIALIZER_VERSION,
        "value": _canonical(value),
    }
    return (
        json.dumps(
            document, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def semantic_repair_fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_semantic_bytes(value)).hexdigest()


def semantic_repair_state(state: Mapping[str, Any]) -> dict[str, Any]:
    excluded = {
        "last_seen_at", "last_polled_at", "attempt_count", "heartbeat_at",
        "observed_at", "updated_at",
    }
    def filter_value(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                key: filter_value(item)
                for key, item in value.items() if key not in excluded
            }
        if isinstance(value, (list, tuple)):
            return [filter_value(item) for item in value]
        return value

    return filter_value(state)


def resolve_provenance(
    candidates: Mapping[ProvenanceSource, Any],
) -> tuple[ProvenanceSource, Any | None]:
    priority = (
        ProvenanceSource.STORED_IMMUTABLE_SNAPSHOT,
        ProvenanceSource.EXCHANGE_PAYLOAD,
        ProvenanceSource.CANONICAL_SYMBOL_RESOLVER,
        ProvenanceSource.DEPLOYMENT_ACCOUNT_SNAPSHOT,
    )
    for source in priority:
        if source in candidates and candidates[source] is not None:
            return source, candidates[source]
    return ProvenanceSource.UNKNOWN, None


class LegacyPositionRecomputationService:
    def recompute(
        self, evidence: LegacyPositionEvidence
    ) -> LegacyPositionRecomputation:
        blocking: list[str] = []
        requirements = (
            ("AMBIGUOUS_POSITION", evidence.one_unambiguous_position),
            ("INCOMPLETE_ENTRY_ORDERS", evidence.complete_entry_orders),
            ("INCOMPLETE_EXIT_ORDERS", evidence.complete_exit_orders),
            ("AMBIGUOUS_POSITION_LINKAGE", evidence.unambiguous_position_linkage),
            ("INSTRUMENT_IDENTITY_UNRESOLVED", evidence.instrument_identity_resolved),
            ("UNAPPLIED_POSITION_FILL", evidence.no_unapplied_position_fills),
            ("PENDING_CORRECTION", evidence.no_pending_corrections),
        )
        blocking.extend(reason for reason, ready in requirements if not ready)
        if not evidence.entry_fills:
            blocking.append("MISSING_ENTRY_FILLS")
        if not evidence.exit_fills:
            blocking.append("MISSING_EXIT_FILLS")
        if evidence.precision_policy is None:
            blocking.append("PRECISION_POLICY_UNRESOLVED")

        all_fills = evidence.entry_fills + evidence.exit_fills
        if any(
            fill.fee_valuation.status
            in {FeeValuationStatus.UNKNOWN, FeeValuationStatus.ESTIMATED}
            for fill in all_fills
        ):
            blocking.append("MISSING_AUTHORITATIVE_FEE_VALUATION")

        gross_entry = sum((f.quantity for f in evidence.entry_fills), ZERO)
        entry_base_fee = sum(
            (
                f.fee_quantity for f in evidence.entry_fills
                if f.fee_asset.upper() == evidence.base_asset.upper()
            ),
            ZERO,
        )
        net_entry = gross_entry - entry_base_fee
        gross_exit = sum((f.quantity for f in evidence.exit_fills), ZERO)
        exit_base_fee = sum(
            (
                f.fee_quantity for f in evidence.exit_fills
                if f.fee_asset.upper() == evidence.base_asset.upper()
            ),
            ZERO,
        )
        raw = net_entry - gross_exit - exit_base_fee
        precision_status = None
        normalized = raw
        reason = None
        if evidence.precision_policy is not None:
            precision_status, normalized, reason = (
                evidence.precision_policy.classify(raw)
            )
        conflict = (
            precision_status is PrecisionStatus.OVER_EXIT_CONFLICT
            or gross_entry < entry_base_fee
        )
        if conflict:
            blocking.append("INVENTORY_CONFLICT")
            status = RecomputationStatus.CONFLICT
        elif blocking:
            status = RecomputationStatus.INCOMPLETE
        elif normalized == ZERO:
            status = RecomputationStatus.COMPLETE_CLOSED
        else:
            status = RecomputationStatus.COMPLETE_OPEN

        semantic_evidence = {
            "position_id": evidence.position_id,
            "symbol": evidence.symbol,
            "entry_fills": evidence.entry_fills,
            "exit_fills": evidence.exit_fills,
            "precision_policy": evidence.precision_policy,
            "requirements": requirements,
        }
        return LegacyPositionRecomputation(
            evidence.position_id, status, gross_entry, entry_base_fee,
            net_entry, gross_exit, exit_base_fee, raw, normalized,
            precision_status, reason,
            evidence.precision_policy.source if evidence.precision_policy else None,
            status is RecomputationStatus.COMPLETE_OPEN,
            status is RecomputationStatus.COMPLETE_CLOSED,
            status in {
                RecomputationStatus.COMPLETE_OPEN,
                RecomputationStatus.COMPLETE_CLOSED,
            },
            tuple(dict.fromkeys(blocking)),
            semantic_repair_fingerprint(semantic_evidence),
        )


@dataclass(frozen=True)
class FillApplicationProof:
    source: str
    trade_id: str
    exchange_order_id: str
    semantic_fingerprint: str
    local_fill_identity: str | None
    applied_fingerprint: str | None
    applied_at: datetime | None


def classify_fill_application(
    *,
    observed_fingerprint: str,
    proof: FillApplicationProof | None,
) -> IngestionApplicationStatus:
    if proof is None or (
        proof.local_fill_identity is None
        and proof.applied_fingerprint is None
        and proof.applied_at is None
    ):
        return IngestionApplicationStatus.OBSERVED_NOT_APPLIED
    if proof.applied_fingerprint not in (None, observed_fingerprint):
        return IngestionApplicationStatus.IDEMPOTENCY_CONFLICT
    complete = all(
        (
            proof.source, proof.trade_id, proof.exchange_order_id,
            proof.local_fill_identity, proof.applied_fingerprint,
            proof.applied_at,
        )
    )
    if complete and proof.semantic_fingerprint == observed_fingerprint:
        return IngestionApplicationStatus.TRUE_DUPLICATE_APPLIED
    return IngestionApplicationStatus.IDEMPOTENCY_CONFLICT


@dataclass(frozen=True)
class RecoveryCandidate:
    ingestion_id: int
    source: str
    symbol: str
    trade_id: str
    exchange_order_id: str
    semantic_fingerprint: str
    immutable_payload: Mapping[str, Any]
    ownership: OrderOwnership
    position_id: int | None
    linkage_unambiguous: bool
    client_order_id: str | None


@dataclass(frozen=True)
class FillRecoveryDecision:
    status: IngestionApplicationStatus
    may_write_local_fill: bool
    position_id: int | None
    provenance_facts: Mapping[str, Any]
    blocking_reasons: tuple[str, ...]


class UnappliedFillRecoveryService:
    def classify(
        self,
        candidate: RecoveryCandidate,
        proof: FillApplicationProof | None = None,
    ) -> FillRecoveryDecision:
        existing = classify_fill_application(
            observed_fingerprint=candidate.semantic_fingerprint, proof=proof,
        )
        facts = {
            "ownership": candidate.ownership.value,
            "client_order_id_present": candidate.client_order_id is not None,
            "source": candidate.source,
            "trade_id": candidate.trade_id,
            "exchange_order_id": candidate.exchange_order_id,
        }
        if existing in {
            IngestionApplicationStatus.TRUE_DUPLICATE_APPLIED,
            IngestionApplicationStatus.IDEMPOTENCY_CONFLICT,
        }:
            return FillRecoveryDecision(
                existing, False, candidate.position_id, facts, (),
            )
        if candidate.ownership is OrderOwnership.MANUAL_OR_EXTERNAL:
            return FillRecoveryDecision(
                IngestionApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED,
                False, None, facts, (),
            )
        if (
            candidate.ownership is not OrderOwnership.BOT_OWNED
            or candidate.position_id is None
            or not candidate.linkage_unambiguous
        ):
            return FillRecoveryDecision(
                IngestionApplicationStatus.BLOCKED_MISSING_CONTEXT,
                False, None, facts, ("UNAMBIGUOUS_BOT_POSITION_LINK_REQUIRED",),
            )
        return FillRecoveryDecision(
            IngestionApplicationStatus.OBSERVED_NOT_APPLIED,
            True, candidate.position_id, facts, (),
        )


@dataclass(frozen=True)
class RepairPlan:
    incident_type: str
    eligible_actions: tuple[str, ...]
    blocked_actions: tuple[str, ...]
    required_preconditions: tuple[str, ...]
    expected_row_changes: tuple[str, ...]
    semantic_fingerprint: str
    post_state_invariants: tuple[str, ...]


class LegacyRecoveryPlanner:
    POSITION_ACTIONS = (
        "NORMALIZE_REMAINING_QTY_TO_ZERO",
        "CLOSE_POSITION_LIFECYCLE",
        "WRITE_TERMINAL_LIFECYCLE_EVENT",
        "UPDATE_EXPLICIT_STALE_ORDER_STATES",
        "WRITE_CANONICAL_FINANCIAL_TRUTH",
        "WRITE_REPAIR_AUDIT",
    )

    def position_plan(
        self, result: LegacyPositionRecomputation
    ) -> RepairPlan:
        eligible = (
            self.POSITION_ACTIONS
            if result.recomputation_status is RecomputationStatus.COMPLETE_CLOSED
            else ()
        )
        return RepairPlan(
            "LEGACY_POSITION",
            eligible,
            () if eligible else self.POSITION_ACTIONS,
            result.blocking_reasons,
            (
                "positions:1", "position_lifecycle_events_c2_2:0..1",
                "binance_orders:explicit_ids_only",
                "canonical_financial_truth_v1:1",
                "legacy_repair_audit_v1:1",
            ) if eligible else (),
            result.evidence_fingerprint,
            (
                "remaining_inventory_qty=0",
                "position.status=CLOSED",
                "financial_truth_status=COMPLETE",
                "second_execution=NO_OP",
            ) if eligible else ("NO_WRITES",),
        )

    def fill_plan(self, decision: FillRecoveryDecision, fingerprint: str) -> RepairPlan:
        action = (
            ("WRITE_CANONICAL_LOCAL_FILL", "MARK_INGESTION_APPLIED", "WRITE_REPAIR_AUDIT")
            if decision.may_write_local_fill
            else (
                ("CLASSIFY_EXTERNAL_OR_MANUAL", "WRITE_REPAIR_AUDIT")
                if decision.status
                is IngestionApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
                else ()
            )
        )
        return RepairPlan(
            "UNAPPLIED_FILL", action,
            () if action else ("WRITE_CANONICAL_LOCAL_FILL",),
            decision.blocking_reasons,
            ("binance_order_fills:0..1", "exchange_fill_ingestion_state_v2:1"),
            fingerprint,
            (
                "no_fabricated_position",
                "applied_requires_local_fill_proof",
                "second_execution=TRUE_DUPLICATE_APPLIED",
            ),
        )


def _append_execution_audit(
    cur,
    *,
    incident_type: str,
    incident_identity: str,
    operation_type: str,
    fingerprint: str,
    invocation_identity: str,
    executed_actions: Sequence[str],
    actual_changes: Sequence[str],
) -> None:
    from datetime import datetime, timezone
    from common.legacy_recovery_repository import LegacyRepairAuditRepository

    now = datetime.now(timezone.utc)
    LegacyRepairAuditRepository.append(cur, {
        "incident_type": incident_type,
        "incident_identity": incident_identity,
        "operation_type": operation_type,
        "planner_version": "LEGACY_RECOVERY_PLANNER_V2",
        "writer_version": SERIALIZER_VERSION,
        "semantic_fingerprint_before": fingerprint,
        "semantic_fingerprint_expected": fingerprint,
        "semantic_fingerprint_after": fingerprint,
        "plan_status": "ELIGIBLE",
        "execution_status": "APPLIED",
        "invocation_identity": invocation_identity,
        "requested_at": now,
        "started_at": now,
        "completed_at": now,
        "actor_source": "BOUNDED_REPAIR_SERVICE",
        "blocking_reasons": [],
        "eligible_actions": list(executed_actions),
        "executed_actions": list(executed_actions),
        "expected_changes": list(actual_changes),
        "actual_changes": list(actual_changes),
        "post_state_invariants": ["SEMANTIC_CAS_MATCHED"],
        "error_code": None,
        "error_detail": None,
    })


class CanonicalFillLedgerRepository:
    @staticmethod
    def apply(
        cur,
        *,
        candidate: RecoveryCandidate,
        position_id: int,
        local_fill_values: Mapping[str, Any],
        expected_semantic_fingerprint: str,
    ) -> bool:
        cur.execute(
            """
            SELECT source_fingerprint,applied_fingerprint,applied_at
            FROM exchange_fill_ingestion_state_v2
            WHERE ingestion_id=%s FOR UPDATE
            """,
            (candidate.ingestion_id,),
        )
        row = cur.fetchone()
        if row is None or str(row[0]) != expected_semantic_fingerprint:
            raise RuntimeError("SEMANTIC_CAS_CONFLICT")
        if row[1] is not None:
            if str(row[1]) == expected_semantic_fingerprint and row[2] is not None:
                return False
            raise RuntimeError("IDEMPOTENCY_CONFLICT")
        cur.execute(
            """
            INSERT INTO binance_order_fills(
              source,trade_id,order_id,symbol,side,executed_qty,avg_price,
              commission_amount,commission_asset,event_time,raw
            ) VALUES (
              %(source)s,%(trade_id)s,%(order_id)s,%(symbol)s,%(side)s,
              %(executed_qty)s,%(avg_price)s,%(commission_amount)s,
              %(commission_asset)s,%(event_time)s,%(raw)s
            )
            ON CONFLICT (source,trade_id) DO NOTHING
            RETURNING id
            """,
            dict(local_fill_values),
        )
        inserted = cur.fetchone()
        if inserted is None:
            raise RuntimeError("LOCAL_FILL_IDEMPOTENCY_CONFLICT")
        local_fill_id = int(inserted[0])
        cur.execute(
            """
            UPDATE exchange_fill_ingestion_state_v2
            SET applied_fingerprint=%s,applied_at=clock_timestamp(),
                application_status='APPLIED',local_fill_id=%s,
                linked_position_id=%s
            WHERE ingestion_id=%s AND source_fingerprint=%s
              AND applied_fingerprint IS NULL
            """,
            (
                expected_semantic_fingerprint, local_fill_id, position_id,
                candidate.ingestion_id, expected_semantic_fingerprint,
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError("SEMANTIC_CAS_CONFLICT")
        _append_execution_audit(
            cur, incident_type="UNAPPLIED_FILL",
            incident_identity=(
                f"{candidate.source}:{candidate.symbol}:{candidate.trade_id}"
            ),
            operation_type="RECOVER_FILL",
            fingerprint=expected_semantic_fingerprint,
            invocation_identity=(
                f"recover-fill:{candidate.ingestion_id}:"
                f"{expected_semantic_fingerprint}"
            ),
            executed_actions=("WRITE_CANONICAL_LOCAL_FILL", "MARK_APPLIED"),
            actual_changes=(
                f"local_fill_id:{local_fill_id}",
                f"linked_position_id:{position_id}",
            ),
        )
        return True

    @staticmethod
    def classify_external(
        cur, *, candidate: RecoveryCandidate, expected_semantic_fingerprint: str
    ) -> bool:
        cur.execute(
            """
            UPDATE exchange_fill_ingestion_state_v2
            SET application_status='EXTERNAL_OR_MANUAL_UNLINKED',
                ownership_classification='MANUAL_OR_EXTERNAL',
                classification_payload=%s
            WHERE ingestion_id=%s AND source_fingerprint=%s
              AND applied_fingerprint IS NULL
              AND application_status<>'EXTERNAL_OR_MANUAL_UNLINKED'
            """,
            (
                json.dumps({
                    "client_order_id": candidate.client_order_id,
                    "client_order_id_present": candidate.client_order_id is not None,
                    "exchange_order_id": candidate.exchange_order_id,
                }),
                candidate.ingestion_id, expected_semantic_fingerprint,
            ),
        )
        changed = cur.rowcount == 1
        if changed:
            _append_execution_audit(
                cur, incident_type="UNAPPLIED_FILL",
                incident_identity=(
                    f"{candidate.source}:{candidate.symbol}:{candidate.trade_id}"
                ),
                operation_type="CLASSIFY_EXTERNAL",
                fingerprint=expected_semantic_fingerprint,
                invocation_identity=(
                    f"classify-external:{candidate.ingestion_id}:"
                    f"{expected_semantic_fingerprint}"
                ),
                executed_actions=("CLASSIFY_EXTERNAL_OR_MANUAL",),
                actual_changes=("ingestion_classification",),
            )
        return changed


class CanonicalPositionLifecycleRepairRepository:
    @staticmethod
    def apply(
        cur,
        *,
        result: LegacyPositionRecomputation,
        expected_semantic_fingerprint: str,
        exit_order_ids: Sequence[str],
        invocation_identity: str | None = None,
        audit_fingerprint: str | None = None,
        stage_hook: Callable[[str], None] | None = None,
    ) -> bool:
        if result.recomputation_status is not RecomputationStatus.COMPLETE_CLOSED:
            raise RuntimeError("RECOMPUTATION_NOT_COMPLETE_CLOSED")
        if result.evidence_fingerprint != expected_semantic_fingerprint:
            raise RuntimeError("SEMANTIC_CAS_CONFLICT")
        cur.execute("SELECT status FROM positions WHERE id=%s FOR UPDATE", (result.position_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("POSITION_NOT_FOUND")
        cur.execute(
            """
            SELECT semantic_fingerprint_expected FROM legacy_repair_audit_v1
            WHERE incident_type='LEGACY_POSITION' AND incident_identity=%s
              AND execution_status IN ('APPLIED','IDEMPOTENT_NO_OP')
            ORDER BY recorded_at DESC,audit_id DESC LIMIT 1
            """,
            (str(result.position_id),),
        )
        existing = cur.fetchone()
        if existing is not None:
            if str(existing[0]) == expected_semantic_fingerprint:
                return False
            raise RuntimeError("SEMANTIC_CAS_CONFLICT")
        cur.execute(
            """
            UPDATE positions SET status='CLOSED',qty=0,
              inventory_evidence_status='COMPLETE',
              gross_entry_executed_qty=%s,entry_base_fee_qty=%s,
              net_entry_inventory_qty=%s,cumulative_exit_executed_qty=%s,
              exit_inventory_reduction_qty=%s,remaining_inventory_qty=0,
              terminal_dust_qty=0,terminal_reason='PRECISION_NORMALIZED_ZERO',
              inventory_calculated_at=clock_timestamp()
            WHERE id=%s
            """,
            (
                result.gross_entry_qty, result.base_asset_entry_fee_qty,
                result.net_entry_inventory_qty, result.gross_exit_qty,
                result.gross_exit_qty + result.base_asset_exit_fee_qty,
                result.position_id,
            ),
        )
        if stage_hook is not None:
            stage_hook("position_update")
        cur.execute(
            """
            INSERT INTO position_lifecycle_events_c2_2(
              position_id,order_id,mutation_kind,mutation_high_water,payload
            ) VALUES (%s,%s,'POSITION_CLOSED',%s,%s::jsonb)
            ON CONFLICT DO NOTHING
            """,
            (
                result.position_id,
                ",".join(str(item) for item in sorted(exit_order_ids)),
                result.gross_exit_qty,
                json.dumps({
                    "repair_version": SERIALIZER_VERSION,
                    "raw_inventory_delta": str(result.raw_remaining_qty),
                    "normalized_remaining_qty": str(result.normalized_remaining_qty),
                    "normalization_reason": result.normalization_reason,
                    "precision_source": result.precision_source,
                    "precision_status": (
                        result.precision_status.value
                        if result.precision_status else None
                    ),
                    "semantic_fingerprint": expected_semantic_fingerprint,
                }),
            ),
        )
        if stage_hook is not None:
            stage_hook("lifecycle")
        if exit_order_ids:
            cur.execute(
                """
                UPDATE binance_orders SET status='FILLED'
                WHERE order_id = ANY(%s) AND status<>'FILLED'
                """,
                (list(exit_order_ids),),
            )
        if stage_hook is not None:
            stage_hook("order_states")
        _append_execution_audit(
            cur, incident_type="LEGACY_POSITION",
            incident_identity=str(result.position_id),
            operation_type="REPAIR_POSITION",
            fingerprint=audit_fingerprint or expected_semantic_fingerprint,
            invocation_identity=invocation_identity or (
                f"repair-position:{result.position_id}:"
                f"{expected_semantic_fingerprint}"
            ),
            executed_actions=LegacyRecoveryPlanner.POSITION_ACTIONS,
            actual_changes=(
                "position:CLOSED", "remaining_inventory_qty:0",
                f"raw_inventory_delta:{result.raw_remaining_qty}",
            ),
        )
        if stage_hook is not None:
            stage_hook("audit")
        return True


class LegacyRecoveryTransactionService:
    """Transaction boundary; callers supply an uncommitted connection."""

    @staticmethod
    def repair_position(
        connection,
        *,
        position_id: int,
        environment: str,
        deployment_id: str,
        expected_semantic_fingerprint_v2: str,
        git_sha: str,
        invocation_identity: str,
        stage_hook: Callable[[str], None] | None = None,
    ) -> Mapping[str, Any]:
        from common.financial_truth_repository import (
            CanonicalFinancialTruthWriteRepository,
        )
        from common.legacy_repair_quarantine import (
            EXCLUSION_REASON,
            SOURCE_TYPE,
            LearningArtifactRepository,
            LearningOutcomeExclusionRepository,
            LegacyPositionRepairPlanRepository,
            LegacyRepairQuarantineSchemaReadinessRepository,
            call_stage_hook,
        )
        from common.legacy_recovery_repository import LegacyProvenanceRepository

        try:
            if str(environment).upper() != "PAPER":
                raise RuntimeError("LIVE_APPLY_NOT_AUTHORIZED")
            if not str(deployment_id).strip():
                raise RuntimeError("DEPLOYMENT_ID_REQUIRED")
            if not re.fullmatch(r"[0-9a-f]{64}", expected_semantic_fingerprint_v2):
                raise RuntimeError("EXPECTED_FINGERPRINT_V2_INVALID")
            if not re.fullmatch(r"[0-9a-f]{40}", git_sha):
                raise RuntimeError("GIT_SHA_INVALID")
            # The writer owns the complete transaction boundary. Discard any
            # preceding planner/read transaction on a reused connection.
            connection.rollback()
            connection.set_session(
                isolation_level="SERIALIZABLE", readonly=False, autocommit=False
            )
            with connection.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout='5s'")
                cur.execute("SET LOCAL statement_timeout='60s'")
                readiness = (
                    LegacyRepairQuarantineSchemaReadinessRepository().check(
                        connection
                    )
                )
                if readiness.status != "PRESENT_VALID":
                    raise RuntimeError(
                        "SCHEMA_NOT_READY:" + ",".join(readiness.issues)
                    )
                existing_exclusion = LearningOutcomeExclusionRepository.current(
                    cur, environment="PAPER", deployment_id=deployment_id,
                    position_id=position_id,
                )
                if existing_exclusion is not None:
                    if str(existing_exclusion[1]) != expected_semantic_fingerprint_v2:
                        raise RuntimeError("LEARNING_EXCLUSION_IDEMPOTENCY_CONFLICT")
                    cur.execute(
                        """
                        SELECT audit_id FROM legacy_repair_audit_v1
                        WHERE incident_type='LEGACY_POSITION'
                          AND incident_identity=%s
                          AND invocation_identity=%s
                          AND execution_status='APPLIED'
                        """,
                        (str(position_id), invocation_identity),
                    )
                    audit = cur.fetchone()
                    provenance_identity = (
                        f"PAPER:{deployment_id}:"
                        f"{_current_database(cur)}:position:{position_id}"
                    )
                    cur.execute(
                        """
                        SELECT provenance_id
                        FROM legacy_repair_provenance_v1
                        WHERE evidence_source=%s AND source_identity=%s
                        """,
                        (SOURCE_TYPE, provenance_identity),
                    )
                    provenance = cur.fetchone()
                    cur.execute(
                        "SELECT status,remaining_inventory_qty FROM positions "
                        "WHERE id=%s",
                        (position_id,),
                    )
                    position = cur.fetchone()
                    cur.execute(
                        "SELECT financial_truth_status "
                        "FROM canonical_financial_truth_v1 WHERE position_id=%s",
                        (position_id,),
                    )
                    financial_truth = cur.fetchone()
                    LearningArtifactRepository.assert_absent(cur, position_id)
                    if (
                        audit is None or provenance is None or position is None
                        or position[0] != "CLOSED"
                        or Decimal(str(position[1])) != ZERO
                        or financial_truth != ("COMPLETE",)
                    ):
                        raise RuntimeError("IDEMPOTENCY_STATE_CONFLICT")
                    connection.rollback()
                    return {
                        "status": "ALREADY_APPLIED",
                        "position_id": int(position_id),
                        "semantic_fingerprint_v2": expected_semantic_fingerprint_v2,
                        "learning_excluded": True,
                        "transaction_committed": False,
                        "writes": 0,
                        "audit_id": int(audit[0]),
                        "provenance_id": int(provenance[0]),
                        "exclusion_id": int(existing_exclusion[0]),
                    }

                initial = LegacyPositionRepairPlanRepository.build(
                    connection, position_id=position_id, environment="PAPER",
                    deployment_id=deployment_id,
                )
                if initial.semantic_fingerprint_v2 != expected_semantic_fingerprint_v2:
                    raise RuntimeError("PLAN_STALE")
                LegacyPositionRepairPlanRepository.lock_evidence(cur, initial)
                locked = LegacyPositionRepairPlanRepository.build(
                    connection, position_id=position_id, environment="PAPER",
                    deployment_id=deployment_id,
                )
                if locked.semantic_fingerprint_v2 != expected_semantic_fingerprint_v2:
                    raise RuntimeError("PLAN_STALE")
                if not locked.eligible:
                    raise RuntimeError(
                        "REPAIR_NOT_ELIGIBLE:" + ",".join(locked.blocking_reasons)
                    )
                LearningArtifactRepository.assert_absent(cur, position_id)
                exclusion_id = LearningOutcomeExclusionRepository.insert(
                    cur, environment="PAPER", deployment_id=deployment_id,
                    position_id=position_id,
                    semantic_fingerprint_v2=locked.semantic_fingerprint_v2,
                    git_sha=git_sha,
                )
                call_stage_hook(stage_hook, "exclusion")
                changed = CanonicalPositionLifecycleRepairRepository.apply(
                    cur, result=locked.recomputation,
                    expected_semantic_fingerprint=(
                        locked.semantic_fingerprint_v1
                    ),
                    exit_order_ids=locked.exit_order_ids,
                    invocation_identity=invocation_identity,
                    audit_fingerprint=locked.semantic_fingerprint_v2,
                    stage_hook=stage_hook,
                )
                if not changed:
                    raise RuntimeError("IDEMPOTENCY_STATE_CONFLICT")
                if locked.financial_truth.financial_truth_status != "COMPLETE":
                    raise RuntimeError("FINANCIAL_TRUTH_NOT_COMPLETE")
                CanonicalFinancialTruthWriteRepository.write(
                    cur, locked.financial_truth,
                    invocation_type="LEGACY_POSITION_REPAIR",
                    invocation_identity=invocation_identity,
                )
                call_stage_hook(stage_hook, "financial_truth")
                provenance_payload = dict(locked.evidence_payload)
                provenance_payload.update({
                    "learning_exclusion_id": exclusion_id,
                    "learning_eligible": False,
                    "git_sha": git_sha,
                    "invocation_identity": invocation_identity,
                })
                LegacyProvenanceRepository.record(cur, {
                    "evidence_source": SOURCE_TYPE,
                    "source_identity": locked.provenance_identity,
                    "source_fingerprint": locked.semantic_fingerprint_v2,
                    "instrument_identity": (
                        (locked.evidence_payload.get("position_state") or {}).get(
                            "symbol"
                        )
                    ),
                    "account_provenance": {
                        "fill_ids": list(
                            locked.entry_fill_ids + locked.exit_fill_ids
                        )
                    },
                    "deployment_provenance": {
                        "environment": "PAPER",
                        "deployment_id": deployment_id,
                        "database_identity": locked.database_name,
                        "git_sha": git_sha,
                    },
                    "fee_evidence": {
                        "base_asset_entry_fee_qty": str(
                            locked.recomputation.base_asset_entry_fee_qty
                        ),
                        "base_asset_exit_fee_qty": str(
                            locked.recomputation.base_asset_exit_fee_qty
                        ),
                    },
                    "valuation_evidence": {
                        "financial_truth_status": (
                            locked.financial_truth.financial_truth_status
                        ),
                        "authoritative_net_pnl": (
                            str(locked.financial_truth.authoritative_net_pnl)
                            if locked.financial_truth.authoritative_net_pnl
                            is not None else None
                        ),
                    },
                    "immutable_payload": provenance_payload,
                    "observed_at": datetime.now(timezone.utc),
                })
                call_stage_hook(stage_hook, "provenance")
                cur.execute(
                    "SELECT audit_id FROM legacy_repair_audit_v1 "
                    "WHERE invocation_identity=%s",
                    (invocation_identity,),
                )
                audit_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT provenance_id FROM legacy_repair_provenance_v1 "
                    "WHERE evidence_source=%s AND source_identity=%s",
                    (SOURCE_TYPE, locked.provenance_identity),
                )
                provenance_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT status,remaining_inventory_qty FROM positions "
                    "WHERE id=%s",
                    (position_id,),
                )
                position = cur.fetchone()
                cur.execute(
                    "SELECT financial_truth_status "
                    "FROM canonical_financial_truth_v1 WHERE position_id=%s",
                    (position_id,),
                )
                financial_truth = cur.fetchone()
                LearningArtifactRepository.assert_absent(cur, position_id)
                if (
                    position is None or position[0] != "CLOSED"
                    or Decimal(str(position[1])) != ZERO
                    or financial_truth != ("COMPLETE",)
                ):
                    raise RuntimeError("POSTCONDITION_FAILED")
                call_stage_hook(stage_hook, "postconditions")
            connection.commit()
            return {
                "status": "APPLIED",
                "position_id": int(position_id),
                "semantic_fingerprint_v2": locked.semantic_fingerprint_v2,
                "learning_excluded": True,
                "transaction_committed": True,
                "writes": None,
                "audit_id": audit_id,
                "provenance_id": provenance_id,
                "exclusion_id": exclusion_id,
            }
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def recover_fill(
        connection,
        *,
        candidate: RecoveryCandidate,
        decision: FillRecoveryDecision,
        local_fill_values: Mapping[str, Any] | None = None,
    ) -> bool:
        try:
            with connection.cursor() as cur:
                if decision.may_write_local_fill:
                    if local_fill_values is None or decision.position_id is None:
                        raise RuntimeError("LOCAL_FILL_VALUES_REQUIRED")
                    changed = CanonicalFillLedgerRepository.apply(
                        cur, candidate=candidate,
                        position_id=decision.position_id,
                        local_fill_values=local_fill_values,
                        expected_semantic_fingerprint=(
                            candidate.semantic_fingerprint
                        ),
                    )
                elif (
                    decision.status
                    is IngestionApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
                ):
                    changed = CanonicalFillLedgerRepository.classify_external(
                        cur, candidate=candidate,
                        expected_semantic_fingerprint=(
                            candidate.semantic_fingerprint
                        ),
                    )
                else:
                    raise RuntimeError("RECOVERY_DECISION_NOT_WRITABLE")
            connection.commit()
            return changed
        except Exception:
            connection.rollback()
            raise

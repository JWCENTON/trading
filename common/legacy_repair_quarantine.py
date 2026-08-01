from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping

from common.financial_truth_calculator import (
    FillEvidence,
    FinancialTruthCalculation,
    calculate_financial_truth,
)
from common.legacy_recovery import (
    FeeValuationStatus,
    LegacyPositionRecomputation,
    LegacyPositionRecomputationService,
    LegacyRecoveryPlanner,
    semantic_repair_fingerprint,
    semantic_repair_state,
    value_fee,
)
from common.legacy_recovery_repository import LegacyPositionEvidenceRepository
from common.legacy_recovery_schema import (
    LegacyRecoverySchemaReadinessRepository,
    SchemaContractStatus,
)


MIGRATION_ID = "20260801_legacy_repair_learning_quarantine_v1.sql"
SCHEMA_VERSION = "LEGACY_REPAIR_LEARNING_QUARANTINE_V1"
MANIFEST_CHECKSUM = (
    "fab46cd6eb55ac7353732834e7144fc26ce2937a442aa244e8a205f3f724d4d0"
)
FINGERPRINT_VERSION = "LEGACY_REPAIR_PLAN_FINGERPRINT_V2"
EXCLUSION_REASON = "LEGACY_REPAIR"
SOURCE_TYPE = "LEGACY_POSITION_REPAIR"
CREATED_BY = "LEGACY_RECOVERY_APPLY_V1"

LEARNING_ARTIFACT_TABLES = (
    "exit_trace_v1",
    "exit_trace_v2",
    "exit_trace_v3",
    "learning_feedback_shadow_recommendations",
    "learning_feature_warehouse_v1",
    "decision_replay_v1",
    "decision_registry_v1",
    "decision_outcomes_v1",
)

GUARDED_ARTIFACT_TABLES = LEARNING_ARTIFACT_TABLES


@dataclass(frozen=True)
class QuarantineSchemaReadiness:
    status: str
    issues: tuple[str, ...]


@dataclass(frozen=True)
class LegacyPositionRepairPlanV2:
    position_id: int
    environment: str
    deployment_id: str
    database_name: str
    eligible: bool
    blocking_reasons: tuple[str, ...]
    semantic_fingerprint_v1: str
    semantic_fingerprint_v2: str
    recomputation: LegacyPositionRecomputation
    financial_truth: FinancialTruthCalculation
    entry_order_ids: tuple[str, ...]
    exit_order_ids: tuple[str, ...]
    entry_fill_ids: tuple[int, ...]
    exit_fill_ids: tuple[int, ...]
    provenance_identity: str
    invocation_identity: str
    evidence_payload: Mapping[str, Any]


def _database_name(connection) -> str:
    with connection.cursor() as cur:
        cur.execute("SELECT current_database()")
        return str(cur.fetchone()[0])


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _fill_evidence(
    row: Mapping[str, Any],
    *,
    position_id: int,
    environment: str,
    deployment_id: str,
) -> FillEvidence:
    quantity = Decimal(str(row["executed_qty"]))
    price = Decimal(str(row["avg_price"]))
    fee_quantity = (
        Decimal(str(row["commission_amount"]))
        if row.get("commission_amount") is not None else None
    )
    base_asset = row.get("base_asset")
    quote_asset = row.get("quote_asset")
    valuation = value_fee(
        quantity=fee_quantity or Decimal("0"),
        asset=str(row.get("commission_asset") or ""),
        base_asset=str(base_asset or ""),
        quote_asset=str(quote_asset or ""),
        fill_price=price,
    )
    authoritative_statuses = {
        FeeValuationStatus.AUTHORITATIVE_QUOTE_FEE,
        FeeValuationStatus.AUTHORITATIVE_BASE_FEE_WITH_FILL_PRICE,
        FeeValuationStatus.AUTHORITATIVE_BASE_FEE_WITH_EXTERNAL_VALUATION,
    }
    event_time = row.get("event_time")
    if not isinstance(event_time, datetime):
        raise RuntimeError("FILL_EVENT_TIME_REQUIRED")
    side = str(row.get("side") or "").upper()
    purpose = "ENTRY" if side == "BUY" else "EXIT" if side == "SELL" else ""
    return FillEvidence(
        fill_id=str(row["id"]),
        order_id=str(row["order_id"]),
        position_id=position_id,
        purpose=purpose,
        side=side,
        symbol=str(row["symbol"]),
        quantity=quantity,
        price=price,
        notional=quantity * price,
        fee_quantity=fee_quantity,
        fee_asset=row.get("commission_asset"),
        authoritative_fee_usdc=(
            valuation.valued_fee_usdc
            if valuation.status in authoritative_statuses else None
        ),
        estimated_fee_usdc=(
            valuation.valued_fee_usdc
            if valuation.status is FeeValuationStatus.ESTIMATED else None
        ),
        event_time=event_time,
        source_authority="LEGACY_RECONSTRUCTED_EXECUTION",
        source_exchange=str(row.get("source") or "unknown").lower(),
        source_environment=environment.lower(),
        source_deployment_id=deployment_id,
        account_identity_fingerprint=row.get("identity_fingerprint"),
        instrument_metadata_fingerprint=row.get("metadata_fingerprint"),
        step_size=(
            Decimal(str(row["step_size"]))
            if row.get("step_size") is not None else None
        ),
        base_asset=base_asset,
        quote_asset=quote_asset,
        source_version="LEGACY_RECOVERY_EVIDENCE_V2",
    )


class LegacyRepairQuarantineSchemaReadinessRepository:
    REQUIRED_COLUMNS = {
        "exclusion_id": ("bigint", "NO"),
        "environment": ("text", "NO"),
        "deployment_id": ("text", "NO"),
        "position_id": ("bigint", "NO"),
        "exclusion_reason": ("text", "NO"),
        "source_type": ("text", "NO"),
        "semantic_fingerprint_v2": ("text", "NO"),
        "created_at": ("timestamp with time zone", "NO"),
        "created_by": ("text", "NO"),
        "git_sha": ("text", "NO"),
    }

    def check(self, connection) -> QuarantineSchemaReadiness:
        issues: list[str] = []
        legacy = LegacyRecoverySchemaReadinessRepository().check(connection)
        if legacy.status is not SchemaContractStatus.PRESENT_VALID:
            issues.append(f"LEGACY_RECOVERY_SCHEMA:{legacy.status.value}")
            issues.extend(legacy.issues)
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT column_name,data_type,is_nullable
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='learning_outcome_exclusion_v1'
                """
            )
            actual = {
                str(name): (str(data_type), str(nullable))
                for name, data_type, nullable in cur.fetchall()
            }
            for name, expected in self.REQUIRED_COLUMNS.items():
                if actual.get(name) != expected:
                    issues.append(f"QUARANTINE_COLUMN:{name}")
            cur.execute(
                """
                SELECT conname FROM pg_constraint
                WHERE conrelid=to_regclass(
                  'public.learning_outcome_exclusion_v1'
                )
                """
            )
            constraints = {str(row[0]) for row in cur.fetchall()}
            for name in (
                "learning_outcome_exclusion_v1_pkey",
                "ux_learning_outcome_exclusion_v1_identity",
                "fk_learning_outcome_exclusion_v1_position",
                "ck_learning_outcome_exclusion_v1_contract",
            ):
                if name not in constraints:
                    issues.append(f"QUARANTINE_CONSTRAINT:{name}")
            cur.execute(
                """
                SELECT to_regclass(
                  'public.v_learning_eligible_closed_positions_v1'
                ),to_regprocedure(
                  'public.learning_outcome_is_excluded_v1(bigint)'
                )
                """
            )
            view, predicate = cur.fetchone()
            if view is None:
                issues.append("QUARANTINE_ELIGIBLE_VIEW")
            if predicate is None:
                issues.append("QUARANTINE_PREDICATE")
            cur.execute(
                """
                SELECT c.relname,t.tgname
                FROM pg_trigger t
                JOIN pg_class c ON c.oid=t.tgrelid
                WHERE NOT t.tgisinternal
                  AND t.tgname LIKE 'trg_lq_%'
                """
            )
            guarded = {str(table) for table, _name in cur.fetchall()}
            for table in GUARDED_ARTIFACT_TABLES:
                if table not in guarded:
                    issues.append(f"QUARANTINE_GUARD:{table}")
            cur.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname='public'
                  AND indexname='ix_learning_outcome_exclusion_v1_position'
                """
            )
            if cur.fetchone() is None:
                issues.append("QUARANTINE_INDEX:position")
            cur.execute(
                """
                SELECT 1 FROM pg_trigger
                WHERE tgrelid=to_regclass(
                  'public.learning_outcome_exclusion_v1'
                )
                  AND tgname='trg_learning_outcome_exclusion_v1_append_only'
                  AND NOT tgisinternal
                """
            )
            if cur.fetchone() is None:
                issues.append("QUARANTINE_APPEND_ONLY_TRIGGER")
            cur.execute(
                """
                SELECT checksum_sha256,schema_baseline_version,success
                FROM schema_migration_ledger_v1
                WHERE migration_id=%s
                ORDER BY applied_at DESC LIMIT 1
                """,
                (MIGRATION_ID,),
            )
            ledger = cur.fetchone()
            if (
                ledger is None
                or str(ledger[0]) != MANIFEST_CHECKSUM
                or str(ledger[1]) != SCHEMA_VERSION
                or not ledger[2]
            ):
                issues.append("QUARANTINE_MIGRATION_LEDGER")
        return QuarantineSchemaReadiness(
            "PRESENT_VALID" if not issues else "NOT_READY",
            tuple(dict.fromkeys(issues)),
        )


class LearningArtifactRepository:
    @staticmethod
    def counts(cur, position_id: int) -> dict[str, int]:
        result: dict[str, int] = {}
        for table in LEARNING_ARTIFACT_TABLES:
            cur.execute(
                f"SELECT count(*) FROM public.{table} WHERE position_id=%s",
                (int(position_id),),
            )
            result[table] = int(cur.fetchone()[0])
        return result

    @classmethod
    def assert_absent(cls, cur, position_id: int) -> None:
        found = {
            table: count
            for table, count in cls.counts(cur, position_id).items()
            if count
        }
        if found:
            raise RuntimeError(
                "LEARNING_ARTIFACT_ALREADY_EXISTS:"
                + ",".join(f"{table}={count}" for table, count in found.items())
            )


class LearningOutcomeExclusionRepository:
    @staticmethod
    def current(cur, *, environment: str, deployment_id: str, position_id: int):
        cur.execute(
            """
            SELECT exclusion_id,semantic_fingerprint_v2,exclusion_reason,
                   source_type,git_sha
            FROM learning_outcome_exclusion_v1
            WHERE environment=%s AND deployment_id=%s AND position_id=%s
            FOR UPDATE
            """,
            (environment, deployment_id, int(position_id)),
        )
        return cur.fetchone()

    @staticmethod
    def insert(
        cur,
        *,
        environment: str,
        deployment_id: str,
        position_id: int,
        semantic_fingerprint_v2: str,
        git_sha: str,
    ) -> int:
        cur.execute(
            """
            INSERT INTO learning_outcome_exclusion_v1(
              environment,deployment_id,position_id,exclusion_reason,
              source_type,semantic_fingerprint_v2,created_by,git_sha
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(environment,deployment_id,position_id) DO NOTHING
            RETURNING exclusion_id
            """,
            (
                environment, deployment_id, int(position_id), EXCLUSION_REASON,
                SOURCE_TYPE, semantic_fingerprint_v2, CREATED_BY, git_sha,
            ),
        )
        row = cur.fetchone()
        if row is not None:
            return int(row[0])
        current = LearningOutcomeExclusionRepository.current(
            cur, environment=environment, deployment_id=deployment_id,
            position_id=position_id,
        )
        if current is None or str(current[1]) != semantic_fingerprint_v2:
            raise RuntimeError("LEARNING_EXCLUSION_IDEMPOTENCY_CONFLICT")
        return int(current[0])


class LegacyPositionRepairPlanRepository:
    @staticmethod
    def build(
        connection,
        *,
        position_id: int,
        environment: str,
        deployment_id: str,
    ) -> LegacyPositionRepairPlanV2:
        environment = str(environment).upper()
        if environment not in {"PAPER", "LIVE"}:
            raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
        if not str(deployment_id).strip():
            raise RuntimeError("DEPLOYMENT_ID_REQUIRED")
        database_name = _database_name(connection)
        envelope = LegacyPositionEvidenceRepository().read(
            connection, position_id=int(position_id),
        )
        if envelope.evidence is None:
            raise RuntimeError(
                "POSITION_EVIDENCE_" + envelope.evidence_status.value + ":"
                + ",".join(
                    envelope.missing_evidence + envelope.conflicting_evidence
                )
            )
        state = envelope.current_state or {}
        position = state.get("position") or {}
        fills = tuple(state.get("fills") or ())
        recomputation = LegacyPositionRecomputationService().recompute(
            envelope.evidence
        )
        repair_plan = LegacyRecoveryPlanner().position_plan(recomputation)
        canonical_fills = tuple(
            _fill_evidence(
                row, position_id=int(position_id), environment=environment,
                deployment_id=deployment_id,
            )
            for row in fills
        )
        financial_truth = calculate_financial_truth(
            position_id=int(position_id), position_status="CLOSED",
            fills=canonical_fills, position_symbol=position.get("symbol"),
        )
        entry_rows = tuple(
            row for row in fills if str(row.get("side") or "").upper() == "BUY"
        )
        exit_rows = tuple(
            row for row in fills if str(row.get("side") or "").upper() == "SELL"
        )
        entry_order_ids = tuple(sorted({
            str(row["order_id"]) for row in entry_rows
        }))
        exit_order_ids = tuple(sorted({
            str(row["order_id"]) for row in exit_rows
        }))
        entry_fill_ids = tuple(sorted(int(row["id"]) for row in entry_rows))
        exit_fill_ids = tuple(sorted(int(row["id"]) for row in exit_rows))
        provenance_identity = (
            f"{environment}:{deployment_id}:{database_name}:position:{position_id}"
        )
        invocation_identity = f"repair-position-v2:{provenance_identity}"
        blocking = list(
            envelope.missing_evidence + envelope.conflicting_evidence
            + recomputation.blocking_reasons
        )
        if str(position.get("status") or "").upper() != "OPEN":
            blocking.append("POSITION_NOT_OPEN")
        if not repair_plan.eligible_actions:
            blocking.append("REPAIR_PLAN_NOT_ELIGIBLE")
        if financial_truth.financial_truth_status != "COMPLETE":
            blocking.append(
                "FINANCIAL_TRUTH_" + financial_truth.financial_truth_status
            )
            if financial_truth.failure_detail:
                blocking.append(financial_truth.failure_detail)
        evidence_payload = {
            "fingerprint_version": FINGERPRINT_VERSION,
            "environment": environment,
            "deployment_id": deployment_id,
            "database_identity": database_name,
            "position_id": int(position_id),
            "position_state": semantic_repair_state(position),
            "orders": semantic_repair_state(state.get("orders") or []),
            "fills": semantic_repair_state(fills),
            "entry_order_ids": entry_order_ids,
            "exit_order_ids": exit_order_ids,
            "entry_fill_ids": entry_fill_ids,
            "exit_fill_ids": exit_fill_ids,
            "gross_entry_qty": recomputation.gross_entry_qty,
            "base_asset_entry_fee_qty": recomputation.base_asset_entry_fee_qty,
            "net_entry_inventory_qty": recomputation.net_entry_inventory_qty,
            "gross_exit_qty": recomputation.gross_exit_qty,
            "base_asset_exit_fee_qty": recomputation.base_asset_exit_fee_qty,
            "planned_mutations": repair_plan.expected_row_changes,
            "planned_lifecycle": repair_plan.post_state_invariants,
            "financial_truth": financial_truth.semantic_values(),
            "learning_exclusion_reason": EXCLUSION_REASON,
            "repair_classification": SOURCE_TYPE,
            "provenance_identity": provenance_identity,
            "idempotency_identity": invocation_identity,
        }
        evidence_payload = _json_safe(evidence_payload)
        fingerprint_v2 = semantic_repair_fingerprint(evidence_payload)
        return LegacyPositionRepairPlanV2(
            int(position_id), environment, deployment_id, database_name,
            not blocking, tuple(dict.fromkeys(blocking)),
            recomputation.evidence_fingerprint, fingerprint_v2,
            recomputation, financial_truth, entry_order_ids, exit_order_ids,
            entry_fill_ids, exit_fill_ids, provenance_identity,
            invocation_identity, evidence_payload,
        )

    @staticmethod
    def lock_evidence(cur, plan: LegacyPositionRepairPlanV2) -> None:
        cur.execute(
            "SELECT id FROM positions WHERE id=%s FOR UPDATE",
            (plan.position_id,),
        )
        if cur.fetchone() is None:
            raise RuntimeError("POSITION_NOT_FOUND")
        order_ids = list(plan.entry_order_ids + plan.exit_order_ids)
        cur.execute(
            "SELECT id FROM binance_orders WHERE order_id=ANY(%s) "
            "ORDER BY id FOR UPDATE",
            (order_ids or [""],),
        )
        cur.fetchall()
        fill_ids = list(plan.entry_fill_ids + plan.exit_fill_ids)
        cur.execute(
            "SELECT id FROM binance_order_fills WHERE id=ANY(%s) ORDER BY id FOR UPDATE",
            (fill_ids or [-1],),
        )
        cur.fetchall()
        cur.execute(
            "SELECT audit_id FROM legacy_repair_audit_v1 "
            "WHERE incident_type='LEGACY_POSITION' AND incident_identity=%s "
            "ORDER BY audit_id FOR UPDATE",
            (str(plan.position_id),),
        )
        cur.fetchall()
        cur.execute(
            "SELECT provenance_id FROM legacy_repair_provenance_v1 "
            "WHERE evidence_source=%s AND source_identity=%s FOR UPDATE",
            (SOURCE_TYPE, plan.provenance_identity),
        )
        cur.fetchall()


def call_stage_hook(
    hook: Callable[[str], None] | None, stage: str
) -> None:
    if hook is not None:
        hook(stage)

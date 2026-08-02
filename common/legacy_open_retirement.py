from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Any, Callable, Mapping

from common.financial_truth_calculator import calculate_financial_truth
from common.financial_truth_repository import (
    CanonicalFinancialTruthWriteRepository,
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
)
from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    InstrumentExecutionLimits,
    floor_to_lot,
    project_inventory_from_execution_evidence,
)
from common.legacy_exit_intent_gate import (
    HistoricalExitIntentGateRepository,
)
from common.legacy_recovery import semantic_repair_fingerprint
from common.legacy_recovery_order_evidence import (
    LegacyRecoveryOrderEvidenceRepository,
    OrderEvidenceSourceType,
)
from common.legacy_recovery_repository import (
    LegacyProvenanceRepository,
    LegacyRepairAuditRepository,
)
from common.legacy_repair_quarantine import (
    LearningArtifactRepository,
    LearningOutcomeExclusionRepository,
    LegacyRepairQuarantineSchemaReadinessRepository,
    call_stage_hook,
)
from common.simulated_execution_evidence import (
    SIMULATION_FEE_RATE,
    SIMULATION_MODEL_VERSION,
    create_simulated_execution_fill_cursor,
    create_simulated_order_cursor,
    lock_simulated_exit_slot_cursor,
    simulated_order_write_status,
)
from common.simulated_order_namespace import (
    ADMINISTRATIVE_ORDER_CLASS,
    NAMESPACE_SCHEMA_VERSION,
    detect_simulated_order_namespace,
    require_simulated_order_namespace_v1,
)


RETIREMENT_TYPE = "LEGACY_ADMINISTRATIVE_CLOSE"
RETIREMENT_INCIDENT = "LEGACY_OPEN_RETIREMENT"
RETIREMENT_SOURCE = "LEGACY_OPEN_POSITION_RETIREMENT"
RETIREMENT_PLANNER_VERSION = "PAPER_OPEN_RETIREMENT_PLANNER_V1"
RETIREMENT_WRITER_VERSION = "PAPER_OPEN_RETIREMENT_WRITER_V1"
LEARNING_TRUST = "LEGACY_RECONSTRUCTED_NOT_TRUSTED_FORWARD"
MARKET_EVIDENCE_MAX_AGE_SECONDS = 20 * 60
ZERO = Decimal("0")


def _safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _safe(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return value


def _row(cur) -> Mapping[str, Any] | None:
    value = cur.fetchone()
    if value is None:
        return None
    return dict(zip((item[0] for item in cur.description), value))


def _rows(cur) -> tuple[Mapping[str, Any], ...]:
    names = tuple(item[0] for item in cur.description)
    return tuple(dict(zip(names, value)) for value in cur.fetchall())


@dataclass(frozen=True)
class CurrentMarketEvidence:
    source_type: str
    source_table: str
    source_primary_key: int
    symbol: str
    interval: str
    price: Decimal
    market_timestamp: datetime
    observed_at: datetime
    freshness: str
    max_age_seconds: int


@dataclass(frozen=True)
class LegacyOpenRetirementPlan:
    position_id: int
    environment: str
    deployment_id: str
    database_name: str
    status: str
    blocking_reasons: tuple[str, ...]
    semantic_fingerprint_v2: str
    retirement_type: str
    position: Mapping[str, Any]
    order_evidence: Mapping[str, Any]
    entry_order_id: int | None
    entry_fill_id: int | None
    account_identity_id: int | None
    instrument_snapshot_id: int | None
    instrument: Mapping[str, Any]
    remaining_inventory: Decimal
    planned_exit_qty: Decimal
    market: CurrentMarketEvidence | None
    historical_exit_intent_gate: Any
    artifact_gate: Any
    provenance_identity: str
    invocation_identity: str
    evidence_payload: Mapping[str, Any]

    @property
    def eligible(self) -> bool:
        return self.status == "READY"

    def public_payload(self) -> Mapping[str, Any]:
        market = asdict(self.market) if self.market is not None else None
        return _safe({
            "status": self.status,
            "reason": self.blocking_reasons[0] if self.blocking_reasons else None,
            "blocking_reasons": self.blocking_reasons,
            "position_id": self.position_id,
            "retirement_type": self.retirement_type,
            "semantic_fingerprint_v2": self.semantic_fingerprint_v2,
            "current_price": self.market.price if self.market else None,
            "price_source": self.market.source_type if self.market else None,
            "market_evidence": market,
            "planned_exit_qty": self.planned_exit_qty,
            "historical_exit_intent_gate": (
                self.historical_exit_intent_gate.public_payload()
            ),
            "financial_truth_status": "COMPLETE" if self.eligible else "BLOCKED",
            "learning_eligible": False,
            "learning_trust": LEARNING_TRUST,
            "reporting_eligible": False,
            "learning_artifact_gate": self.artifact_gate.public_payload(),
            "idempotency_identity": self.invocation_identity,
        })


class LegacyOpenRetirementPlanRepository:
    @staticmethod
    def _schema_ready(connection) -> None:
        readiness = LegacyRepairQuarantineSchemaReadinessRepository().check(
            connection
        )
        if readiness.status != "PRESENT_VALID":
            raise RuntimeError("SCHEMA_NOT_READY:" + ",".join(readiness.issues))
        capabilities = LegacyRecoveryOrderEvidenceRepository.detect_capabilities(
            connection, environment="PAPER", deployment_id="readiness-only"
        )
        if (
            capabilities.source_type
            is not OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
            or not capabilities.simulated_execution_fills
        ):
            raise RuntimeError("PAPER_SIMULATED_WRITER_NOT_READY")
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name,column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name=ANY(%s)
                """,
                ([
                    "candles", "positions", "simulated_orders",
                    "simulated_execution_fills_v1",
                    "financial_truth_instrument_snapshot_v1",
                    "financial_truth_account_identity_v1",
                ],),
            )
            available: dict[str, set[str]] = {}
            for table, column in cur.fetchall():
                available.setdefault(str(table), set()).add(str(column))
        required = {
            "candles": {
                "id", "symbol", "interval", "open_time", "close_time", "close",
            },
            "positions": {
                "id", "symbol", "strategy", "interval", "status", "side", "qty",
                "entry_price", "entry_time", "exit_price", "exit_time",
                "exit_reason", "entry_order_id", "exit_order_id",
                "entry_client_order_id", "exit_client_order_id",
                "inventory_evidence_status", "gross_entry_executed_qty",
                "entry_base_fee_qty", "net_entry_inventory_qty",
                "cumulative_exit_executed_qty", "exit_inventory_reduction_qty",
                "remaining_inventory_qty", "inventory_contract_adoption_id",
                "inventory_contract_generation", "exit_context_json",
            },
            "simulated_orders": {
                "id", "created_at", "symbol", "interval", "strategy", "side",
                "price", "quantity_btc", "reason", "rsi_14", "ema_21",
                "candle_open_time", "is_exit",
            },
            "simulated_execution_fills_v1": {
                "id", "simulated_order_id", "position_id", "fill_index",
                "order_purpose", "side", "symbol", "fill_qty", "fill_price",
                "fill_notional", "fee_qty", "fee_asset",
                "authoritative_fee_usdc", "account_identity_id",
                "instrument_snapshot_id", "environment", "deployment_id",
                "simulation_model_version", "execution_at", "source_fingerprint",
            },
            "financial_truth_instrument_snapshot_v1": {
                "id", "metadata_fingerprint", "step_size", "min_qty",
                "min_notional", "quantity_precision", "price_precision",
                "base_asset", "quote_asset",
            },
            "financial_truth_account_identity_v1": {
                "id", "identity_fingerprint",
            },
        }
        issues = [
            f"{table}:{column}"
            for table, columns in required.items()
            for column in sorted(columns - available.get(table, set()))
        ]
        if issues:
            raise RuntimeError(
                "OPEN_RETIREMENT_SCHEMA_NOT_READY:" + ",".join(issues)
            )

    @classmethod
    def build(
        cls,
        connection,
        *,
        position_id: int,
        environment: str,
        deployment_id: str,
        git_sha: str,
    ) -> LegacyOpenRetirementPlan:
        environment = str(environment).upper()
        if environment != "PAPER":
            raise RuntimeError("LIVE_RETIREMENT_NOT_AUTHORIZED")
        if not str(deployment_id).strip():
            raise RuntimeError("DEPLOYMENT_ID_REQUIRED")
        if not re.fullmatch(r"[0-9a-f]{40}", str(git_sha).lower()):
            raise RuntimeError("GIT_SHA_INVALID")
        cls._schema_ready(connection)
        namespace = detect_simulated_order_namespace(connection)
        with connection.cursor() as cur:
            cur.execute("SELECT current_database(),clock_timestamp()")
            database_name, database_now = cur.fetchone()
            cur.execute(
                """
                SELECT p.id,p.symbol,p.strategy,p."interval" AS interval,
                       p.status,p.side,p.qty,p.entry_price,p.entry_time,
                       p.exit_price,p.exit_time,p.exit_reason,p.entry_order_id,
                       p.exit_order_id,p.entry_client_order_id,
                       p.exit_client_order_id,p.inventory_evidence_status,
                       p.gross_entry_executed_qty,p.entry_base_fee_qty,
                       p.net_entry_inventory_qty,
                       COALESCE(p.cumulative_exit_executed_qty,0)
                         AS cumulative_exit_executed_qty,
                       COALESCE(p.exit_inventory_reduction_qty,0)
                         AS exit_inventory_reduction_qty,
                       p.remaining_inventory_qty,
                       p.inventory_contract_adoption_id,
                       p.inventory_contract_generation
                FROM positions p WHERE p.id=%s
                """,
                (int(position_id),),
            )
            position = _row(cur)
            if position is None:
                raise RuntimeError("POSITION_NOT_FOUND")
            resolution = LegacyRecoveryOrderEvidenceRepository.resolve(
                connection, position=position, environment=environment,
                deployment_id=deployment_id,
            )
            order_evidence = resolution.fingerprint_payload()
            cur.execute(
                """
                SELECT sf.id,sf.simulated_order_id,sf.position_id,
                       sf.environment,sf.deployment_id,
                       sf.order_purpose,sf.side,sf.symbol,
                       sf.fill_qty AS executed_qty,sf.fill_price AS avg_price,
                       sf.fill_notional,sf.fee_qty AS commission_amount,
                       sf.fee_asset AS commission_asset,
                       sf.authoritative_fee_usdc,sf.execution_at,
                       sf.account_identity_id,sf.instrument_snapshot_id,
                       sf.source_fingerprint,ai.identity_fingerprint,
                       im.metadata_fingerprint,im.step_size,im.min_qty,
                       im.min_notional,im.quantity_precision,
                       im.price_precision,im.base_asset,im.quote_asset
                FROM simulated_execution_fills_v1 sf
                LEFT JOIN financial_truth_account_identity_v1 ai
                  ON ai.id=sf.account_identity_id
                LEFT JOIN financial_truth_instrument_snapshot_v1 im
                  ON im.id=sf.instrument_snapshot_id
                WHERE sf.position_id=%s AND lower(sf.environment)='paper'
                  AND sf.deployment_id=%s
                ORDER BY sf.execution_at,sf.id
                """,
                (int(position_id), str(deployment_id)),
            )
            fills = _rows(cur)
            entry_fills = tuple(
                row for row in fills if str(row["order_purpose"]).upper() == "ENTRY"
            )
            exit_fills = tuple(
                row for row in fills if str(row["order_purpose"]).upper() == "EXIT"
            )
            cur.execute(
                """
                SELECT id,symbol,"interval" AS interval,close,open_time,close_time
                FROM candles WHERE upper(symbol)=upper(%s) AND "interval"=%s
                ORDER BY open_time DESC,id DESC LIMIT 1
                """,
                (position["symbol"], position["interval"]),
            )
            market_row = _row(cur)
            cur.execute(
                """
                SELECT adoption_id,generation,adopted_at
                FROM runtime_contract_adoption_v2
                WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
                  AND environment='paper' AND deployment_id=%s
                  AND status='ACTIVE'
                """,
                (str(deployment_id),),
            )
            active_adoption = _row(cur)
            cur.execute(
                """
                SELECT count(*) FROM position_lifecycle_events_c2_2
                WHERE position_id=%s AND mutation_kind IN (
                  'POSITION_CLOSED','POSITION_CLOSED_TERMINAL_DUST'
                )
                """,
                (int(position_id),),
            )
            terminal_lifecycle_count = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*) FROM canonical_financial_truth_v1
                WHERE position_id=%s AND financial_truth_status='COMPLETE'
                """,
                (int(position_id),),
            )
            terminal_ft_count = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*) FROM legacy_repair_audit_v1
                WHERE incident_identity=%s AND incident_type IN (
                  'LEGACY_POSITION','LEGACY_OPEN_RETIREMENT'
                )
                """,
                (str(position_id),),
            )
            audit_count = int(cur.fetchone()[0])
            provenance_identity = (
                f"PAPER:{deployment_id}:{database_name}:position:"
                f"{int(position_id)}:open-retirement"
            )
            invocation_identity = f"open-retirement-v1:{provenance_identity}"
            cur.execute(
                """
                SELECT count(*) FROM legacy_repair_provenance_v1
                WHERE source_identity LIKE %s OR source_identity LIKE %s
                """,
                (
                    f"%:position:{int(position_id)}",
                    f"%:position:{int(position_id)}:%",
                ),
            )
            provenance_count = int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT count(*) FROM learning_outcome_exclusion_v1
                WHERE position_id=%s
                """,
                (int(position_id),),
            )
            exclusion_count = int(cur.fetchone()[0])
            historical_exit_intent_gate = (
                HistoricalExitIntentGateRepository.classify(
                    cur,
                    position=position,
                    resolved_exit_order_count=len(resolution.exit_orders),
                    source_conflict_count=len(resolution.conflicting_evidence),
                    position_exit_fill_count=len(exit_fills),
                    terminal_lifecycle_count=terminal_lifecycle_count,
                    terminal_financial_truth_count=terminal_ft_count,
                )
            )
            artifact_gate = LearningArtifactRepository.classify(
                cur, position_id=int(position_id), environment="PAPER",
                deployment_id=str(deployment_id),
            )

        blocking: list[str] = []
        if str(position["status"]).upper() != "OPEN":
            blocking.append("POSITION_NOT_OPEN")
        if str(position.get("side") or "LONG").upper() not in {"LONG", "BUY"}:
            blocking.append("RETIREMENT_REQUIRES_LONG_SELL_POSITION")
        if resolution.capabilities.source_type is not (
            OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
        ):
            blocking.append("PAPER_SIMULATED_ORDER_SOURCE_REQUIRED")
        if resolution.conflicting_evidence:
            blocking.extend(resolution.conflicting_evidence)
        if len(resolution.entry_orders) != 1:
            blocking.append(
                "ENTRY_ORDER_EVIDENCE_AMBIGUOUS"
                if len(resolution.entry_orders) > 1
                else "ENTRY_ORDER_EVIDENCE_NOT_FOUND"
            )
        if not historical_exit_intent_gate.retirement_allowed:
            blocking.append(
                "EXIT_EVIDENCE_EXECUTED_OR_AMBIGUOUS:"
                + str(historical_exit_intent_gate.reason or "UNKNOWN")
            )
        if len(entry_fills) != 1:
            blocking.append(
                "ENTRY_FILL_EVIDENCE_AMBIGUOUS"
                if len(entry_fills) > 1 else "ENTRY_FILL_EVIDENCE_NOT_FOUND"
            )
        if terminal_lifecycle_count:
            blocking.append("TERMINAL_LIFECYCLE_EXISTS")
        if terminal_ft_count:
            blocking.append("TERMINAL_FINANCIAL_TRUTH_EXISTS")
        if audit_count or provenance_count or exclusion_count:
            blocking.append("EXISTING_PARTIAL_REPAIR_OR_RETIREMENT")
        if not artifact_gate.repair_allowed:
            blocking.append(
                "TERMINAL_OR_AMBIGUOUS_ARTIFACTS:"
                + str(artifact_gate.reason or "UNKNOWN")
            )

        entry = entry_fills[0] if len(entry_fills) == 1 else None
        administrative_environment = (
            str(entry["environment"]) if entry is not None
            else environment.lower()
        )
        administrative_deployment_id = (
            str(entry["deployment_id"]) if entry is not None
            else str(deployment_id)
        )
        instrument: Mapping[str, Any] = {}
        remaining = ZERO
        planned_qty = ZERO
        if entry is not None:
            required = (
                "account_identity_id", "instrument_snapshot_id",
                "identity_fingerprint", "metadata_fingerprint", "step_size",
                "quantity_precision", "base_asset", "quote_asset",
                "environment", "deployment_id",
            )
            if any(entry.get(name) is None for name in required):
                blocking.append("INSTRUMENT_OR_ACCOUNT_EVIDENCE_INCOMPLETE")
            instrument = {
                name: entry.get(name) for name in (
                    "instrument_snapshot_id", "metadata_fingerprint", "step_size",
                    "min_qty", "min_notional", "quantity_precision",
                    "price_precision", "base_asset", "quote_asset",
                )
            }
            inventory = project_inventory_from_execution_evidence(
                symbol=str(position["symbol"]),
                entry_fills=entry_fills, exit_fills=(),
                quote_asset=str(entry.get("quote_asset") or "USDC"),
            )
            remaining = inventory.remaining_inventory_qty
            planned_qty = remaining
            if remaining <= ZERO:
                blocking.append("REMAINING_INVENTORY_NOT_POSITIVE")
            step = (
                Decimal(str(entry["step_size"]))
                if entry.get("step_size") is not None else None
            )
            if step is None or floor_to_lot(remaining, step) != remaining:
                blocking.append("INVENTORY_NOT_FULLY_EXECUTABLE")
            min_qty = (
                Decimal(str(entry["min_qty"]))
                if entry.get("min_qty") is not None else None
            )
            if min_qty is not None and remaining < min_qty:
                blocking.append("INVENTORY_BELOW_MINIMUM_QUANTITY")
            stored_remaining = position.get("remaining_inventory_qty")
            if stored_remaining is not None and Decimal(str(stored_remaining)) != remaining:
                blocking.append("INVENTORY_MISMATCH")
            if position.get("qty") is None or Decimal(str(position["qty"])) != remaining:
                blocking.append("INVENTORY_MISMATCH")
            if str(entry.get("side") or "").upper() != "BUY":
                blocking.append("ENTRY_SIDE_CONFLICT")

        if active_adoption is None:
            blocking.append("LEGACY_CLASSIFICATION_UNRESOLVED")
        else:
            active_match = (
                position.get("inventory_contract_adoption_id")
                == active_adoption["adoption_id"]
                and position.get("inventory_contract_generation")
                == active_adoption["generation"]
            )
            pre_adoption = (
                position.get("entry_time") is not None
                and active_adoption.get("adopted_at") is not None
                and position["entry_time"] < active_adoption["adopted_at"]
            )
            if active_match or not pre_adoption:
                blocking.append("POSITION_NOT_LEGACY_PRE_ADOPTION")

        market = None
        if market_row is None or market_row.get("close") is None:
            blocking.append("CURRENT_MARKET_EVIDENCE_NOT_FOUND")
        else:
            open_time = market_row["open_time"].astimezone(timezone.utc)
            close_time = market_row["close_time"].astimezone(timezone.utc)
            age = (database_now.astimezone(timezone.utc) - open_time).total_seconds()
            fresh = 0 <= age <= MARKET_EVIDENCE_MAX_AGE_SECONDS
            market = CurrentMarketEvidence(
                "CANONICAL_LATEST_CANDLE_CLOSE", "candles",
                int(market_row["id"]), str(position["symbol"]),
                str(position["interval"]), Decimal(str(market_row["close"])),
                open_time, close_time, "FRESH" if fresh else "STALE",
                MARKET_EVIDENCE_MAX_AGE_SECONDS,
            )
            if market.price <= ZERO:
                blocking.append("CURRENT_MARKET_EVIDENCE_INVALID")
            if not fresh:
                blocking.append("CURRENT_MARKET_EVIDENCE_STALE")
            if entry is not None and entry.get("min_notional") is not None:
                if planned_qty * market.price < Decimal(str(entry["min_notional"])):
                    blocking.append("INVENTORY_BELOW_MINIMUM_NOTIONAL")

        if market is not None:
            with connection.cursor() as cur:
                if namespace.is_namespace_v1:
                    cur.execute(
                        """
                        SELECT count(*) FROM simulated_orders
                        WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE'
                          AND environment=%s AND deployment_id=%s
                          AND position_id=%s
                        """,
                        (
                            administrative_environment,
                            administrative_deployment_id,
                            int(position_id),
                        ),
                    )
                else:
                    cur.execute(
                        """
                        SELECT count(*) FROM simulated_orders
                        WHERE upper(symbol)=upper(%s) AND "interval"=%s
                          AND strategy=%s AND candle_open_time=%s AND is_exit
                        """,
                        (
                            position["symbol"], position["interval"],
                            position["strategy"], market.market_timestamp,
                        ),
                    )
                if int(cur.fetchone()[0]):
                    blocking.append("PARALLEL_EXIT_OR_RETIREMENT_INTENT")

        payload = _safe({
            "fingerprint_version": "PAPER_OPEN_RETIREMENT_FINGERPRINT_V2",
            "environment": environment,
            "deployment_id": deployment_id,
            "database_identity": str(database_name),
            "position": position,
            "order_evidence": order_evidence,
            "entry_fill": entry,
            "remaining_inventory": remaining,
            "instrument_precision": instrument,
            "current_market_evidence": asdict(market) if market else None,
            "planned_exit_quantity": planned_qty,
            "planned_simulated_order": {
                "side": "SELL", "reason": RETIREMENT_TYPE,
                "order_class": ADMINISTRATIVE_ORDER_CLASS,
                "position_id": int(position_id),
                "environment": administrative_environment,
                "deployment_id": administrative_deployment_id,
                "administrative_idempotency_identity": (
                    f"{administrative_environment}:"
                    f"{administrative_deployment_id}:"
                    f"{int(position_id)}"
                ),
                "namespace_schema_version": NAMESPACE_SCHEMA_VERSION,
                "candle_open_time": (
                    market.market_timestamp if market else None
                ),
            },
            "planned_simulated_fill": {
                "purpose": "EXIT", "side": "SELL",
                "quantity": planned_qty,
                "price": market.price if market else None,
                "fee_rate": SIMULATION_FEE_RATE,
                "fee_asset": instrument.get("quote_asset"),
                "simulation_model_version": SIMULATION_MODEL_VERSION,
                "execution_time_contract": "DATABASE_CLOCK_AT_APPLY",
            },
            "planned_lifecycle": {
                "mutation_kind": "POSITION_CLOSED",
                "exit_reason": RETIREMENT_TYPE,
                "outcome_origin": RETIREMENT_TYPE,
                "remaining_inventory": "0",
            },
            "planned_financial_truth": "COMPLETE",
            "historical_exit_intent_gate": (
                historical_exit_intent_gate.fingerprint_payload()
            ),
            "learning_artifact_gate": artifact_gate.fingerprint_payload(),
            "existing_terminal_evidence": {
                "terminal_lifecycle_count": terminal_lifecycle_count,
                "terminal_financial_truth_count": terminal_ft_count,
                "repair_or_retirement_audit_count": audit_count,
                "repair_or_retirement_provenance_count": provenance_count,
                "learning_exclusion_count": exclusion_count,
                "historical_exit_intent_count": (
                    historical_exit_intent_gate.intent_count
                ),
            },
            "learning_exclusion": {
                "learning_eligible": False, "learning_trust": LEARNING_TRUST,
            },
            "reporting_eligible": False,
            "retirement_classification": RETIREMENT_TYPE,
            "audit_provenance_identity": provenance_identity,
            "git_sha": str(git_sha).lower(),
            "idempotency_identity": invocation_identity,
        })
        fingerprint = semantic_repair_fingerprint(payload)
        return LegacyOpenRetirementPlan(
            int(position_id), environment, str(deployment_id), str(database_name),
            "READY" if not blocking else "BLOCKED",
            tuple(dict.fromkeys(blocking)), fingerprint, RETIREMENT_TYPE,
            _safe(position), order_evidence,
            (
                int(resolution.entry_orders[0].source_primary_key)
                if len(resolution.entry_orders) == 1 else None
            ),
            int(entry["id"]) if entry is not None else None,
            int(entry["account_identity_id"]) if entry is not None and entry.get("account_identity_id") is not None else None,
            int(entry["instrument_snapshot_id"]) if entry is not None and entry.get("instrument_snapshot_id") is not None else None,
            _safe(instrument), remaining, planned_qty, market,
            historical_exit_intent_gate, artifact_gate,
            provenance_identity, invocation_identity, payload,
        )

    @staticmethod
    def lock_evidence(cur, plan: LegacyOpenRetirementPlan) -> None:
        lock_simulated_exit_slot_cursor(
            cur,
            symbol=str(plan.position["symbol"]),
            interval=str(plan.position["interval"]),
            strategy=str(plan.position["strategy"]),
        )
        cur.execute("SELECT id FROM positions WHERE id=%s FOR UPDATE", (plan.position_id,))
        if cur.fetchone() is None:
            raise RuntimeError("POSITION_NOT_FOUND")
        for table, row_id in (
            ("simulated_orders", plan.entry_order_id),
            ("simulated_execution_fills_v1", plan.entry_fill_id),
            ("financial_truth_instrument_snapshot_v1", plan.instrument_snapshot_id),
        ):
            if row_id is None:
                raise RuntimeError("PLAN_STALE")
            cur.execute(f"SELECT id FROM {table} WHERE id=%s FOR UPDATE", (row_id,))
            if cur.fetchone() is None:
                raise RuntimeError("PLAN_STALE")
        if plan.market is None:
            raise RuntimeError("PLAN_STALE")
        cur.execute(
            "SELECT id FROM candles WHERE id=%s FOR SHARE",
            (plan.market.source_primary_key,),
        )
        if cur.fetchone() is None:
            raise RuntimeError("PLAN_STALE")
        cur.execute(
            "SELECT id FROM simulated_orders WHERE id=ANY(%s) ORDER BY id FOR UPDATE",
            ([plan.entry_order_id],),
        )
        cur.fetchall()
        cur.execute(
            "SELECT id FROM simulated_execution_fills_v1 WHERE position_id=%s "
            "ORDER BY id FOR UPDATE",
            (plan.position_id,),
        )
        cur.fetchall()
        historical_ids = list(plan.historical_exit_intent_gate.intent_ids)
        if historical_ids:
            cur.execute(
                "SELECT id FROM simulated_orders WHERE id=ANY(%s) "
                "ORDER BY id FOR UPDATE",
                (historical_ids,),
            )
            if tuple(int(row[0]) for row in cur.fetchall()) != tuple(
                historical_ids
            ):
                raise RuntimeError("PLAN_STALE")
            cur.execute(
                "SELECT id FROM simulated_execution_fills_v1 "
                "WHERE simulated_order_id=ANY(%s) ORDER BY id FOR UPDATE",
                (historical_ids,),
            )
            locked_fill_ids = tuple(int(row[0]) for row in cur.fetchall())
            if locked_fill_ids != tuple(
                plan.historical_exit_intent_gate.fill_ids
            ):
                raise RuntimeError("PLAN_STALE")
        LearningArtifactRepository.lock(cur, plan.position_id)
        for sql, params in (
            (
                "SELECT exclusion_id FROM learning_outcome_exclusion_v1 "
                "WHERE environment='PAPER' AND deployment_id=%s AND position_id=%s "
                "FOR UPDATE",
                (plan.deployment_id, plan.position_id),
            ),
            (
                "SELECT audit_id FROM legacy_repair_audit_v1 "
                "WHERE incident_type=%s AND incident_identity=%s FOR UPDATE",
                (RETIREMENT_INCIDENT, str(plan.position_id)),
            ),
            (
                "SELECT provenance_id FROM legacy_repair_provenance_v1 "
                "WHERE evidence_source=%s AND source_identity=%s FOR UPDATE",
                (RETIREMENT_SOURCE, plan.provenance_identity),
            ),
        ):
            cur.execute(sql, params)
            cur.fetchall()


class LegacyOpenRetirementTransactionService:
    @staticmethod
    def _already_retired(cur, *, plan_identity: str, expected: str):
        cur.execute(
            """
            SELECT a.audit_id,p.provenance_id,e.exclusion_id,
                   pos.status,pos.remaining_inventory_qty,
                   ft.financial_truth_status,so.id,sf.id,l.event_id
            FROM legacy_repair_audit_v1 a
            JOIN legacy_repair_provenance_v1 p
              ON p.evidence_source=%s AND p.source_identity=%s
             AND p.source_fingerprint=%s
            JOIN learning_outcome_exclusion_v1 e
              ON e.position_id=a.incident_identity::bigint
             AND e.semantic_fingerprint_v2=%s
            JOIN positions pos ON pos.id=a.incident_identity::bigint
            JOIN canonical_financial_truth_v1 ft ON ft.position_id=pos.id
            JOIN simulated_execution_fills_v1 sf ON sf.position_id=pos.id
             AND sf.order_purpose='EXIT'
            JOIN simulated_orders so ON so.id=sf.simulated_order_id
            JOIN position_lifecycle_events_c2_2 l ON l.position_id=pos.id
             AND l.mutation_kind='POSITION_CLOSED'
            WHERE a.incident_type=%s AND a.invocation_identity=%s
              AND a.semantic_fingerprint_expected=%s
              AND a.execution_status='APPLIED'
            """,
            (
                RETIREMENT_SOURCE, plan_identity, expected, expected,
                RETIREMENT_INCIDENT, f"open-retirement-v1:{plan_identity}",
                expected,
            ),
        )
        rows = cur.fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("IDEMPOTENCY_STATE_CONFLICT")
        row = rows[0]
        if row[3] != "CLOSED" or Decimal(str(row[4])) != ZERO or row[5] != "COMPLETE":
            raise RuntimeError("IDEMPOTENCY_STATE_CONFLICT")
        return {
            "status": "ALREADY_RETIRED", "writes": 0,
            "audit_id": int(row[0]), "provenance_id": int(row[1]),
            "exclusion_id": int(row[2]), "exit_order_id": int(row[6]),
            "exit_fill_id": int(row[7]), "lifecycle_event_id": int(row[8]),
            "transaction_committed": False,
        }

    @classmethod
    def apply(
        cls,
        connection,
        *,
        position_id: int,
        environment: str,
        deployment_id: str,
        expected_semantic_fingerprint_v2: str,
        git_sha: str,
        stage_hook: Callable[[str], None] | None = None,
    ) -> Mapping[str, Any]:
        if str(environment).upper() != "PAPER":
            raise RuntimeError("LIVE_RETIREMENT_NOT_AUTHORIZED")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_semantic_fingerprint_v2):
            raise RuntimeError("EXPECTED_FINGERPRINT_V2_INVALID")
        try:
            connection.rollback()
            connection.set_session(
                isolation_level="SERIALIZABLE", readonly=False, autocommit=False
            )
            with connection.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout='5s'")
                cur.execute("SET LOCAL statement_timeout='60s'")
                LegacyOpenRetirementPlanRepository._schema_ready(connection)
                require_simulated_order_namespace_v1(connection)
                cur.execute("SELECT current_database()")
                database_name = str(cur.fetchone()[0])
                identity = (
                    f"PAPER:{deployment_id}:{database_name}:position:"
                    f"{int(position_id)}:open-retirement"
                )
                already = cls._already_retired(
                    cur, plan_identity=identity,
                    expected=expected_semantic_fingerprint_v2,
                )
                if already is not None:
                    connection.rollback()
                    return {
                        **already, "position_id": int(position_id),
                        "retirement_type": RETIREMENT_TYPE,
                        "semantic_fingerprint_v2": expected_semantic_fingerprint_v2,
                    }
                initial = LegacyOpenRetirementPlanRepository.build(
                    connection, position_id=position_id, environment="PAPER",
                    deployment_id=deployment_id, git_sha=git_sha,
                )
                if initial.semantic_fingerprint_v2 != expected_semantic_fingerprint_v2:
                    raise RuntimeError("PLAN_STALE")
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s,%s)",
                    (0x4C52, int(position_id)),
                )
                LegacyOpenRetirementPlanRepository.lock_evidence(cur, initial)
                require_simulated_order_namespace_v1(connection)
                locked = LegacyOpenRetirementPlanRepository.build(
                    connection, position_id=position_id, environment="PAPER",
                    deployment_id=deployment_id, git_sha=git_sha,
                )
                if locked.semantic_fingerprint_v2 != expected_semantic_fingerprint_v2:
                    raise RuntimeError("PLAN_STALE")
                if not locked.eligible:
                    raise RuntimeError(
                        "RETIREMENT_NOT_ELIGIBLE:" + ",".join(locked.blocking_reasons)
                    )
                exclusion_id = LearningOutcomeExclusionRepository.insert(
                    cur, environment="PAPER", deployment_id=deployment_id,
                    position_id=position_id,
                    semantic_fingerprint_v2=locked.semantic_fingerprint_v2,
                    git_sha=git_sha,
                )
                call_stage_hook(stage_hook, "exclusion")
                LearningArtifactRepository.assert_excluded_from_readers(
                    cur, position_id
                )
                cur.execute("SELECT clock_timestamp()")
                execution_at = cur.fetchone()[0]
                exit_order_id = create_simulated_order_cursor(
                    cur, symbol=str(locked.position["symbol"]),
                    interval=str(locked.position["interval"]),
                    strategy=str(locked.position["strategy"]), side="SELL",
                    price=locked.market.price, quantity=locked.planned_exit_qty,
                    reason=RETIREMENT_TYPE,
                    candle_open_time=locked.market.market_timestamp,
                    is_exit=True,
                    order_class=ADMINISTRATIVE_ORDER_CLASS,
                    position_id=int(position_id),
                    environment=str(
                        locked.evidence_payload["planned_simulated_order"]
                        ["environment"]
                    ),
                    deployment_id=str(
                        locked.evidence_payload["planned_simulated_order"]
                        ["deployment_id"]
                    ),
                )
                if not exit_order_id:
                    status = simulated_order_write_status(exit_order_id)
                    if status == "IDEMPOTENT_EXISTING_ADMINISTRATIVE_ORDER":
                        raise RuntimeError("ALREADY_RETIRED")
                    raise RuntimeError(status)
                call_stage_hook(stage_hook, "exit_order")
                exit_fill_id = create_simulated_execution_fill_cursor(
                    cur, simulated_order_id=exit_order_id,
                    position_id=position_id, order_purpose="EXIT", side="SELL",
                    symbol=str(locked.position["symbol"]),
                    quantity=locked.planned_exit_qty, price=locked.market.price,
                    account_identity_id=locked.account_identity_id,
                    instrument_snapshot_id=locked.instrument_snapshot_id,
                    environment="paper", deployment_id=deployment_id,
                    execution_at=execution_at,
                    interval=str(locked.position["interval"]),
                    strategy=str(locked.position["strategy"]),
                    account_identity_fingerprint=str(
                        locked.evidence_payload["entry_fill"][
                            "identity_fingerprint"
                        ]
                    ),
                    instrument_metadata_fingerprint=str(
                        locked.instrument["metadata_fingerprint"]
                    ),
                )
                if exit_fill_id is None:
                    raise RuntimeError("SIMULATED_EXIT_FILL_CONFLICT")
                call_stage_hook(stage_hook, "exit_fill")
                cur.execute(
                    """
                    UPDATE positions SET exit_order_id=%s,
                      exit_client_order_id=%s,
                      exit_context_json=COALESCE(exit_context_json,'{}'::jsonb)
                        || jsonb_build_object(
                          'outcome_origin',%s::text,
                          'learning_eligible',false,
                          'learning_trust',%s::text,
                          'reporting_eligible',false
                        )
                    WHERE id=%s AND status='OPEN' AND exit_order_id IS NULL
                    """,
                    (
                        str(exit_order_id),
                        f"legacy-retirement-{position_id}-{exit_order_id}",
                        RETIREMENT_TYPE, LEARNING_TRUST,
                        int(position_id),
                    ),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("POSITION_EXIT_INTENT_CONFLICT")
                call_stage_hook(stage_hook, "position_update")
                cur.execute(
                    """
                    SELECT order_purpose,fill_qty,fee_qty,fee_asset
                    FROM simulated_execution_fills_v1
                    WHERE position_id=%s ORDER BY execution_at,id
                    """,
                    (int(position_id),),
                )
                fill_rows = cur.fetchall()
                entry_fills = [
                    {"executed_qty": row[1], "commission_amount": row[2],
                     "commission_asset": row[3]}
                    for row in fill_rows if row[0] == "ENTRY"
                ]
                exit_fills = [
                    {"executed_qty": row[1], "commission_amount": row[2],
                     "commission_asset": row[3]}
                    for row in fill_rows if row[0] == "EXIT"
                ]
                inventory = project_inventory_from_execution_evidence(
                    symbol=str(locked.position["symbol"]),
                    entry_fills=entry_fills, exit_fills=exit_fills,
                    quote_asset=str(locked.instrument["quote_asset"]),
                )
                limits = InstrumentExecutionLimits(
                    Decimal(str(locked.instrument["step_size"])),
                    (
                        Decimal(str(locked.instrument["min_qty"]))
                        if locked.instrument.get("min_qty") is not None else None
                    ),
                    (
                        Decimal(str(locked.instrument["min_notional"]))
                        if locked.instrument.get("min_notional") is not None else None
                    ),
                    locked.market.price, True,
                )
                mutation = apply_inventory_lifecycle_mutation(
                    cur, position_id=position_id, order_id=str(exit_order_id),
                    inventory=inventory, limits=limits,
                    previous_remaining_qty=locked.remaining_inventory,
                    previous_exit_high_water=Decimal(str(
                        locked.position.get("cumulative_exit_executed_qty") or 0
                    )),
                    has_exit_evidence=True, exit_price=locked.market.price,
                    exit_time=execution_at, execution_source="PAPER_SIMULATED",
                    exit_reason=RETIREMENT_TYPE,
                    event_payload={
                        "outcome_origin": RETIREMENT_TYPE,
                        "learning_eligible": False,
                        "learning_trust": LEARNING_TRUST,
                        "reporting_eligible": False,
                    },
                )
                if mutation.position_status != "CLOSED" or not mutation.event_inserted:
                    raise RuntimeError("RETIREMENT_LIFECYCLE_NOT_TERMINAL")
                call_stage_hook(stage_hook, "lifecycle")
                source = FinancialTruthSourceRepository(lambda: connection)
                position_row, evidence_fills, issue = source.read_position_and_fills(
                    position_id,
                    context=ExecutionEvidenceContext(
                        "paper", None, deployment_id
                    ),
                    connection=connection,
                )
                if issue is not None:
                    raise RuntimeError("FINANCIAL_TRUTH_SOURCE_NOT_READY:" + str(issue))
                calculation = calculate_financial_truth(
                    position_id=position_id, position_status=str(position_row[1]),
                    fills=evidence_fills, position_symbol=str(position_row[5]),
                )
                if calculation.financial_truth_status != "COMPLETE":
                    raise RuntimeError(
                        "FINANCIAL_TRUTH_NOT_COMPLETE:"
                        + str(calculation.failure_code or calculation.failure_detail)
                    )
                CanonicalFinancialTruthWriteRepository.write(
                    cur, calculation,
                    invocation_type="LEGACY_OPEN_RETIREMENT",
                    invocation_identity=locked.invocation_identity,
                )
                call_stage_hook(stage_hook, "financial_truth")
                now = datetime.now(timezone.utc)
                inserted_audit = LegacyRepairAuditRepository.append(cur, {
                    "incident_type": RETIREMENT_INCIDENT,
                    "incident_identity": str(position_id),
                    "operation_type": "APPLY_OPEN_RETIREMENT",
                    "planner_version": RETIREMENT_PLANNER_VERSION,
                    "writer_version": RETIREMENT_WRITER_VERSION,
                    "semantic_fingerprint_before": locked.semantic_fingerprint_v2,
                    "semantic_fingerprint_expected": locked.semantic_fingerprint_v2,
                    "semantic_fingerprint_after": locked.semantic_fingerprint_v2,
                    "plan_status": "ELIGIBLE", "execution_status": "APPLIED",
                    "invocation_identity": locked.invocation_identity,
                    "requested_at": now, "started_at": now, "completed_at": now,
                    "actor_source": "BOUNDED_PAPER_RETIREMENT_SERVICE",
                    "blocking_reasons": [],
                    "eligible_actions": ["LEGACY_ADMINISTRATIVE_CLOSE"],
                    "executed_actions": [
                        "LEARNING_EXCLUSION", "SIMULATED_SELL_ORDER",
                        "SIMULATED_EXIT_FILL", "POSITION_CLOSED",
                        "CANONICAL_FINANCIAL_TRUTH",
                    ],
                    "expected_changes": ["ONE_POSITION_ONLY"],
                    "actual_changes": [
                        f"exit_order_id:{exit_order_id}",
                        f"exit_fill_id:{exit_fill_id}",
                    ],
                    "post_state_invariants": [
                        "remaining_inventory_qty=0", "learning_eligible=false",
                        "reporting_eligible=false",
                    ],
                    "error_code": None, "error_detail": None,
                })
                if not inserted_audit:
                    raise RuntimeError("RETIREMENT_AUDIT_CONFLICT")
                call_stage_hook(stage_hook, "audit")
                provenance_payload = dict(locked.evidence_payload)
                provenance_payload.update({
                    "exit_order_id": exit_order_id,
                    "exit_fill_id": exit_fill_id,
                    "learning_exclusion_id": exclusion_id,
                    "outcome_origin": RETIREMENT_TYPE,
                })
                if not LegacyProvenanceRepository.record(cur, {
                    "evidence_source": RETIREMENT_SOURCE,
                    "source_identity": locked.provenance_identity,
                    "source_fingerprint": locked.semantic_fingerprint_v2,
                    "instrument_identity": locked.position["symbol"],
                    "account_provenance": {
                        "account_identity_id": locked.account_identity_id,
                        "entry_fill_id": locked.entry_fill_id,
                        "exit_fill_id": exit_fill_id,
                    },
                    "deployment_provenance": {
                        "environment": "PAPER", "deployment_id": deployment_id,
                        "database_identity": locked.database_name,
                        "git_sha": git_sha,
                    },
                    "fee_evidence": {
                        "fee_policy": str(SIMULATION_FEE_RATE),
                        "authoritative_fees_usdc": str(
                            calculation.authoritative_fees_usdc
                        ),
                    },
                    "valuation_evidence": {
                        "financial_truth_status": "COMPLETE",
                        "authoritative_net_pnl": str(
                            calculation.authoritative_net_pnl
                        ),
                    },
                    "immutable_payload": provenance_payload,
                    "observed_at": now,
                }):
                    raise RuntimeError("RETIREMENT_PROVENANCE_CONFLICT")
                call_stage_hook(stage_hook, "provenance")
                cur.execute(
                    "SELECT event_id FROM position_lifecycle_events_c2_2 "
                    "WHERE position_id=%s AND order_id=%s "
                    "AND mutation_kind='POSITION_CLOSED'",
                    (int(position_id), str(exit_order_id)),
                )
                lifecycle_event_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT audit_id FROM legacy_repair_audit_v1 "
                    "WHERE invocation_identity=%s",
                    (locked.invocation_identity,),
                )
                audit_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT provenance_id FROM legacy_repair_provenance_v1 "
                    "WHERE evidence_source=%s AND source_identity=%s",
                    (RETIREMENT_SOURCE, locked.provenance_identity),
                )
                provenance_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT status,remaining_inventory_qty,exit_reason "
                    "FROM positions WHERE id=%s",
                    (int(position_id),),
                )
                post_position = cur.fetchone()
                LearningArtifactRepository.assert_snapshot(
                    cur, locked.artifact_gate, position_id
                )
                LearningArtifactRepository.assert_excluded_from_readers(
                    cur, position_id
                )
                if (
                    post_position is None or post_position[0] != "CLOSED"
                    or Decimal(str(post_position[1])) != ZERO
                    or post_position[2] != RETIREMENT_TYPE
                ):
                    raise RuntimeError("POSTCONDITION_FAILED")
                call_stage_hook(stage_hook, "postconditions")
            connection.commit()
            return {
                "status": "APPLIED", "position_id": int(position_id),
                "retirement_type": RETIREMENT_TYPE,
                "semantic_fingerprint_v2": locked.semantic_fingerprint_v2,
                "exit_order_id": exit_order_id, "exit_fill_id": exit_fill_id,
                "lifecycle_event_id": lifecycle_event_id,
                "audit_id": audit_id, "provenance_id": provenance_id,
                "exclusion_id": exclusion_id, "learning_eligible": False,
                "reporting_eligible": False, "transaction_committed": True,
            }
        except Exception:
            connection.rollback()
            raise

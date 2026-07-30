from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from common.legacy_recovery import (
    FeeValuationStatus,
    LegacyFillEvidence,
    LegacyPositionEvidence,
    OrderOwnership,
    PrecisionPolicy,
    RecoveryCandidate,
    semantic_repair_fingerprint,
    value_fee,
)


class EvidenceStatus(str, Enum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"
    CONFLICT = "CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    SCHEMA_NOT_READY = "SCHEMA_NOT_READY"


@dataclass(frozen=True)
class EvidenceEnvelope:
    evidence_status: EvidenceStatus
    missing_evidence: tuple[str, ...]
    conflicting_evidence: tuple[str, ...]
    source_provenance: Mapping[str, Any]
    evidence: Any = None
    current_state: Mapping[str, Any] | None = None


def _rows(cur) -> list[dict[str, Any]]:
    columns = [item[0] for item in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


class LegacyPositionEvidenceRepository:
    def read(self, connection, *, position_id: int) -> EvidenceEnvelope:
        if int(position_id) <= 0:
            raise ValueError("explicit positive position_id required")
        missing: list[str] = []
        conflicts: list[str] = []
        with connection.cursor() as cur:
            cur.execute("SELECT * FROM positions WHERE id=%s", (int(position_id),))
            positions = _rows(cur)
            if not positions:
                return EvidenceEnvelope(
                    EvidenceStatus.NOT_FOUND, ("POSITION_NOT_FOUND",), (), {},
                )
            if len(positions) != 1:
                return EvidenceEnvelope(
                    EvidenceStatus.CONFLICT, (), ("MULTIPLE_POSITIONS",), {},
                )
            position = positions[0]
            position_symbol = str(position.get("symbol") or "").upper()
            cur.execute(
                """
                SELECT * FROM binance_orders
                WHERE position_id=%s OR reconciled_position_id=%s
                   OR order_id IN (%s,%s)
                ORDER BY id
                """,
                (
                    int(position_id), int(position_id),
                    position.get("entry_order_id"),
                    position.get("exit_order_id"),
                ),
            )
            orders = _rows(cur)
            order_ids = [str(row["order_id"]) for row in orders]
            if not order_ids:
                missing.append("LINKED_ORDERS")
            cur.execute(
                """
                SELECT f.*,im.step_size,im.quantity_precision,
                       im.base_asset,im.quote_asset,im.metadata_fingerprint,
                       ai.identity_fingerprint
                FROM binance_order_fills f
                LEFT JOIN financial_truth_instrument_snapshot_v1 im
                  ON im.id=f.instrument_snapshot_id
                LEFT JOIN financial_truth_account_identity_v1 ai
                  ON ai.id=f.account_identity_id
                WHERE f.order_id=ANY(%s)
                ORDER BY f.event_time,f.id
                """,
                (order_ids or [""],),
            )
            fills = _rows(cur)
            cur.execute(
                "SELECT * FROM exchange_fill_ingestion_state_v2 "
                "WHERE order_id=ANY(%s) ORDER BY ingestion_id",
                (order_ids or [""],),
            )
            ingestion = _rows(cur)
            if not ingestion:
                missing.append("INGESTION_EVIDENCE")
            cur.execute(
                "SELECT * FROM position_lifecycle_events_c2_2 "
                "WHERE position_id=%s ORDER BY event_id",
                (int(position_id),),
            )
            lifecycle = _rows(cur)
            cur.execute(
                "SELECT * FROM canonical_financial_truth_v1 WHERE position_id=%s",
                (int(position_id),),
            )
            financial_truth = _rows(cur)
            cur.execute(
                "SELECT * FROM strategy_events WHERE symbol=%s "
                "ORDER BY created_at,id",
                (position_symbol,),
            )
            strategy_events = _rows(cur)
            cur.execute(
                "SELECT * FROM legacy_repair_audit_v1 "
                "WHERE incident_type='LEGACY_POSITION' AND incident_identity=%s "
                "ORDER BY recorded_at,audit_id",
                (str(position_id),),
            )
            audit = _rows(cur)

        symbols = {
            str(row.get("symbol") or "").upper()
            for row in fills + orders if row.get("symbol")
        }
        if any(symbol != position_symbol for symbol in symbols):
            conflicts.append("ORDER_FILL_SYMBOL_CONFLICT")
        entries = [row for row in fills if str(row.get("side")).upper() == "BUY"]
        exits = [row for row in fills if str(row.get("side")).upper() == "SELL"]
        if not entries:
            missing.append("ENTRY_FILLS")
        if not exits:
            missing.append("EXIT_FILLS")
        if any(
            row.get("commission_amount") is None
            or not row.get("commission_asset")
            for row in fills
        ):
            missing.append("FEE_EVIDENCE")
        snapshots = [
            row for row in fills
            if row.get("step_size") is not None
            and row.get("quantity_precision") is not None
            and row.get("base_asset") and row.get("quote_asset")
        ]
        precision = None
        if not snapshots:
            missing.append("INSTRUMENT_PRECISION")
            base_asset = None
            quote_asset = None
        else:
            contracts = {
                (
                    Decimal(str(row["step_size"])),
                    int(row["quantity_precision"]),
                    str(row["base_asset"]).upper(),
                    str(row["quote_asset"]).upper(),
                )
                for row in snapshots
            }
            if len(contracts) != 1:
                conflicts.append("INSTRUMENT_SNAPSHOT_CONFLICT")
                base_asset = quote_asset = None
            else:
                step, quantity_precision, base_asset, quote_asset = contracts.pop()
                known_precision = max(
                    max(0, -Decimal(str(row["executed_qty"])).as_tuple().exponent)
                    for row in fills
                )
                precision = PrecisionPolicy(
                    step, quantity_precision, known_precision,
                    Decimal(1).scaleb(-known_precision),
                    f"INSTRUMENT_SNAPSHOT:{snapshots[0]['metadata_fingerprint']}",
                )
        if any(row.get("identity_fingerprint") is None for row in fills):
            missing.append("ACCOUNT_PROVENANCE")
        if any(
            row.get("applied_fingerprint") is None
            for row in ingestion
            if str(row.get("trade_id")) not in {
                str(fill.get("trade_id")) for fill in fills
            }
        ):
            missing.append("UNAPPLIED_POSITION_FILL")

        def adapt(row: Mapping[str, Any]) -> LegacyFillEvidence:
            fee_asset = str(row.get("commission_asset") or "")
            valuation = value_fee(
                quantity=Decimal(str(row["commission_amount"])),
                asset=fee_asset,
                base_asset=base_asset or "",
                quote_asset=quote_asset or "",
                fill_price=Decimal(str(row["avg_price"])),
            )
            return LegacyFillEvidence(
                str(row["id"]), str(row["order_id"]), str(row["trade_id"]),
                str(row["side"]), Decimal(str(row["executed_qty"])),
                Decimal(str(row["avg_price"])),
                Decimal(str(row["commission_amount"])), fee_asset, valuation,
            )

        evidence = None
        if base_asset and quote_asset and "FEE_EVIDENCE" not in missing:
            evidence = LegacyPositionEvidence(
                int(position_id), position_symbol, base_asset, quote_asset,
                tuple(adapt(row) for row in entries),
                tuple(adapt(row) for row in exits), precision,
                complete_entry_orders=bool(entries),
                complete_exit_orders=bool(exits),
                instrument_identity_resolved=precision is not None,
                no_unapplied_position_fills=(
                    "UNAPPLIED_POSITION_FILL" not in missing
                ),
            )
        status = (
            EvidenceStatus.CONFLICT if conflicts
            else EvidenceStatus.INCOMPLETE if missing
            else EvidenceStatus.COMPLETE
        )
        state = {
            "position": position, "orders": orders, "fills": fills,
            "ingestion": ingestion, "lifecycle": lifecycle,
            "financial_truth": financial_truth, "audit": audit,
            "strategy_events": strategy_events,
        }
        return EvidenceEnvelope(
            status, tuple(dict.fromkeys(missing)),
            tuple(dict.fromkeys(conflicts)),
            {
                "account_fingerprints": sorted({
                    str(row["identity_fingerprint"]) for row in fills
                    if row.get("identity_fingerprint")
                }),
                "instrument_fingerprints": sorted({
                    str(row["metadata_fingerprint"]) for row in fills
                    if row.get("metadata_fingerprint")
                }),
            },
            evidence, state,
        )


class UnappliedFillEvidenceRepository:
    def read(
        self, connection, *, source: str, trade_id: str, order_id: str
    ) -> EvidenceEnvelope:
        if not all(str(item).strip() for item in (source, trade_id, order_id)):
            raise ValueError("explicit source, trade_id and order_id required")
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM exchange_fill_ingestion_state_v2
                WHERE lower(source)=lower(%s) AND trade_id=%s AND order_id=%s
                """,
                (source, trade_id, order_id),
            )
            ingestion = _rows(cur)
            cur.execute(
                "SELECT * FROM binance_orders WHERE order_id=%s ORDER BY id",
                (order_id,),
            )
            orders = _rows(cur)
            cur.execute(
                "SELECT * FROM binance_order_fills "
                "WHERE lower(source)=lower(%s) AND trade_id=%s",
                (source, trade_id),
            )
            fills = _rows(cur)
        if not ingestion:
            return EvidenceEnvelope(
                EvidenceStatus.NOT_FOUND, ("INGESTION_NOT_FOUND",), (), {},
            )
        if len(ingestion) != 1:
            return EvidenceEnvelope(
                EvidenceStatus.CONFLICT, (), ("MULTIPLE_INGESTION_ROWS",), {},
            )
        row = ingestion[0]
        conflicts = []
        if fills and any(str(fill["order_id"]) != order_id for fill in fills):
            conflicts.append("LOCAL_FILL_ORDER_CONFLICT")
        order = orders[0] if len(orders) == 1 else None
        ownership = (
            OrderOwnership.BOT_OWNED if order and order.get("client_order_id")
            else OrderOwnership.AMBIGUOUS
        )
        linked = None
        if order:
            linked = order.get("reconciled_position_id") or order.get("position_id")
        candidate = RecoveryCandidate(
            int(row["ingestion_id"]), str(row["source"]), str(row["symbol"]),
            str(row["trade_id"]), str(row["order_id"]),
            str(row["source_fingerprint"]),
            row.get("authoritative_payload") or {}, ownership,
            int(linked) if linked is not None else None,
            bool(linked), order.get("client_order_id") if order else None,
        )
        status = EvidenceStatus.CONFLICT if conflicts else EvidenceStatus.COMPLETE
        return EvidenceEnvelope(
            status, (), tuple(conflicts),
            {"ingestion_id": row["ingestion_id"]}, candidate,
            {"ingestion": row, "orders": orders, "local_fills": fills},
        )


class ExternalExecutionEvidenceRepository:
    def read(
        self, connection, *, source: str, trade_id: str, order_id: str
    ) -> EvidenceEnvelope:
        identity = f"{source.lower()}:{trade_id}:{order_id}"
        with connection.cursor() as cur:
            cur.execute(
                "SELECT * FROM legacy_repair_provenance_v1 "
                "WHERE evidence_source=%s AND source_identity=%s",
                ("EXTERNAL_EXECUTION", identity),
            )
            rows = _rows(cur)
        if not rows:
            return EvidenceEnvelope(
                EvidenceStatus.NOT_FOUND, ("EXTERNAL_EVIDENCE_NOT_FOUND",), (),
                {"source_identity": identity},
            )
        row = rows[0]
        payload = row["immutable_payload"]
        conflicts = []
        if payload.get("client_order_id") not in (None, ""):
            conflicts.append("EXTERNAL_CLIENT_ORDER_ID_PRESENT")
        return EvidenceEnvelope(
            EvidenceStatus.CONFLICT if conflicts else EvidenceStatus.COMPLETE,
            (), tuple(conflicts),
            {"provenance_id": row["provenance_id"]}, payload,
            {"provenance": row},
        )


class LegacyRepairAuditRepository:
    @staticmethod
    def append(cur, record: Mapping[str, Any]) -> bool:
        from psycopg2.extras import Json
        values = dict(record)
        values.setdefault("requested_at", datetime.now(timezone.utc))
        for key in (
            "blocking_reasons", "eligible_actions", "executed_actions",
            "expected_changes", "actual_changes", "post_state_invariants",
        ):
            values[key] = Json(values.get(key, []))
        cur.execute(
            """
            INSERT INTO legacy_repair_audit_v1(
              incident_type,incident_identity,operation_type,planner_version,
              writer_version,semantic_fingerprint_before,
              semantic_fingerprint_expected,semantic_fingerprint_after,
              plan_status,execution_status,invocation_identity,requested_at,
              started_at,completed_at,actor_source,blocking_reasons,
              eligible_actions,executed_actions,expected_changes,actual_changes,
              post_state_invariants,error_code,error_detail
            ) VALUES (
              %(incident_type)s,%(incident_identity)s,%(operation_type)s,
              %(planner_version)s,%(writer_version)s,
              %(semantic_fingerprint_before)s,%(semantic_fingerprint_expected)s,
              %(semantic_fingerprint_after)s,%(plan_status)s,
              %(execution_status)s,%(invocation_identity)s,%(requested_at)s,
              %(started_at)s,%(completed_at)s,%(actor_source)s,
              %(blocking_reasons)s,%(eligible_actions)s,%(executed_actions)s,
              %(expected_changes)s,%(actual_changes)s,
              %(post_state_invariants)s,%(error_code)s,%(error_detail)s
            )
            ON CONFLICT(invocation_identity) DO NOTHING RETURNING audit_id
            """,
            values,
        )
        return cur.fetchone() is not None


class LegacyProvenanceRepository:
    @staticmethod
    def record(cur, record: Mapping[str, Any]) -> bool:
        from psycopg2.extras import Json
        cur.execute(
            "SELECT source_fingerprint FROM legacy_repair_provenance_v1 "
            "WHERE evidence_source=%s AND source_identity=%s FOR UPDATE",
            (record["evidence_source"], record["source_identity"]),
        )
        existing = cur.fetchone()
        if existing:
            if str(existing[0]) == record["source_fingerprint"]:
                return False
            raise RuntimeError("PROVENANCE_IDENTITY_CONFLICT")
        values = dict(record)
        for key in (
            "account_provenance", "deployment_provenance", "fee_evidence",
            "valuation_evidence", "immutable_payload",
        ):
            values[key] = Json(values.get(key, {}))
        cur.execute(
            """
            INSERT INTO legacy_repair_provenance_v1(
              evidence_source,source_identity,source_fingerprint,
              instrument_identity,account_provenance,deployment_provenance,
              fee_evidence,valuation_evidence,immutable_payload,observed_at
            ) VALUES (
              %(evidence_source)s,%(source_identity)s,%(source_fingerprint)s,
              %(instrument_identity)s,%(account_provenance)s,
              %(deployment_provenance)s,%(fee_evidence)s,
              %(valuation_evidence)s,%(immutable_payload)s,%(observed_at)s
            )
            """,
            values,
        )
        return True

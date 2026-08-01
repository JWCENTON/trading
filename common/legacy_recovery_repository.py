from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from pathlib import Path
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
from common.legacy_recovery_order_evidence import (
    LegacyRecoveryOrderEvidenceRepository,
    OrderEvidenceSourceType,
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
    def read(
        self,
        connection,
        *,
        position_id: int,
        environment: str,
        deployment_id: str,
    ) -> EvidenceEnvelope:
        if int(position_id) <= 0:
            raise ValueError("explicit positive position_id required")
        missing: list[str] = []
        conflicts: list[str] = []
        position = LegacyRecoveryOrderEvidenceRepository.read_position(
            connection, position_id=int(position_id),
        )
        if position is None:
            return EvidenceEnvelope(
                EvidenceStatus.NOT_FOUND, ("POSITION_NOT_FOUND",), (), {},
            )
        resolution = LegacyRecoveryOrderEvidenceRepository.resolve(
            connection, position=position, environment=environment,
            deployment_id=deployment_id,
        )
        missing.extend(resolution.missing_evidence)
        conflicts.extend(resolution.conflicting_evidence)
        order_records = resolution.entry_orders + resolution.exit_orders
        orders = [
            {
                "id": item.source_primary_key,
                "order_id": item.order_identity,
                "client_order_id": item.client_order_identity,
                "symbol": position.get("symbol"),
                "side": item.side,
                "status": item.status,
                "position_id": int(position_id),
                "reconciled_position_id": None,
                "strategy": position.get("strategy"),
                "interval": position.get("interval"),
                "order_purpose": item.order_purpose,
                "requested_qty": item.quantity,
                "price": item.price,
                "source_table": item.source_table,
                "source_primary_key": item.source_primary_key,
                "linkage_type": item.linkage_type,
                "matching_criteria": item.matching_criteria,
                "timestamp_delta_ms": item.timestamp_delta_ms,
                "source_type": item.source_type.value,
            }
            for item in order_records
        ]
        if not orders:
            missing.append("LINKED_ORDERS")
        order_ids = [item.order_identity for item in order_records]
        with connection.cursor() as cur:
            position_symbol = str(position.get("symbol") or "").upper()
            if (
                resolution.capabilities.source_type
                is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
                and resolution.capabilities.simulated_execution_fills
            ):
                cur.execute(
                    """
                    SELECT sf.id,'simulator'::text AS source,
                           ('simulated:' || sf.id::text) AS trade_id,
                           sf.simulated_order_id::text AS order_id,sf.symbol,
                           sf.side,sf.fill_qty AS executed_qty,
                           sf.fill_price AS avg_price,
                           sf.fee_qty AS commission_amount,
                           sf.fee_asset AS commission_asset,
                           sf.execution_at AS event_time,
                           sf.order_purpose,im.step_size,
                           im.quantity_precision,im.base_asset,im.quote_asset,
                           im.metadata_fingerprint,ai.identity_fingerprint
                    FROM public.simulated_execution_fills_v1 sf
                    LEFT JOIN public.financial_truth_instrument_snapshot_v1 im
                      ON im.id=sf.instrument_snapshot_id
                    LEFT JOIN public.financial_truth_account_identity_v1 ai
                      ON ai.id=sf.account_identity_id
                    WHERE sf.position_id=%s
                      AND sf.simulated_order_id=ANY(%s)
                      AND lower(sf.environment)=%s AND sf.deployment_id=%s
                    ORDER BY sf.execution_at,sf.id
                    """,
                    (
                        int(position_id),
                        [int(value) for value in order_ids] or [-1],
                        str(environment).lower(), str(deployment_id),
                    ),
                )
                fills = _rows(cur)
                ingestion = []
            elif (
                resolution.capabilities.source_type
                in {
                    OrderEvidenceSourceType.LEGACY_ORDER_SOURCE,
                    OrderEvidenceSourceType.LIVE_EXCHANGE_ORDER_SOURCE,
                }
                and resolution.capabilities.binance_order_fills
            ):
                cur.execute(
                    """
                    SELECT f.id,f.source,f.trade_id,f.order_id,f.symbol,f.side,
                           f.executed_qty,f.avg_price,f.commission_amount,
                           f.commission_asset,f.event_time,
                           im.step_size,im.quantity_precision,
                           im.base_asset,im.quote_asset,im.metadata_fingerprint,
                           ai.identity_fingerprint
                    FROM public.binance_order_fills f
                    LEFT JOIN public.financial_truth_instrument_snapshot_v1 im
                      ON im.id=f.instrument_snapshot_id
                    LEFT JOIN public.financial_truth_account_identity_v1 ai
                      ON ai.id=f.account_identity_id
                    WHERE f.order_id=ANY(%s)
                    ORDER BY f.event_time,f.id
                    """,
                    (order_ids or [""],),
                )
                fills = _rows(cur)
                cur.execute(
                    "SELECT to_regclass('public.exchange_fill_ingestion_state_v2')"
                )
                if cur.fetchone()[0] is None:
                    ingestion = []
                else:
                    cur.execute(
                        "SELECT to_jsonb(i) AS row FROM "
                        "public.exchange_fill_ingestion_state_v2 i "
                        "WHERE order_id=ANY(%s) ORDER BY ingestion_id",
                        (order_ids or [""],),
                    )
                    ingestion = [row[0] for row in cur.fetchall()]
                if not ingestion:
                    missing.append("INGESTION_EVIDENCE")
            else:
                fills = []
                ingestion = []

            def json_rows(table: str, key: str, value: Any, order: str):
                cur.execute(f"SELECT to_regclass('public.{table}')")
                if cur.fetchone()[0] is None:
                    return []
                cur.execute(
                    f"SELECT to_jsonb(t) FROM public.{table} t "
                    f"WHERE {key}=%s ORDER BY {order}",
                    (value,),
                )
                return [row[0] for row in cur.fetchall()]

            lifecycle = json_rows(
                "position_lifecycle_events_c2_2", "position_id",
                int(position_id), "event_id",
            )
            financial_truth = json_rows(
                "canonical_financial_truth_v1", "position_id",
                int(position_id), "position_id",
            )
            cur.execute("SELECT to_regclass('public.legacy_repair_audit_v1')")
            if cur.fetchone()[0] is None:
                audit = []
            else:
                cur.execute(
                    "SELECT to_jsonb(a) FROM public.legacy_repair_audit_v1 a "
                    "WHERE incident_type='LEGACY_POSITION' "
                    "AND incident_identity=%s ORDER BY recorded_at,audit_id",
                    (str(position_id),),
                )
                audit = [row[0] for row in cur.fetchall()]

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
            "order_evidence": resolution.fingerprint_payload(),
            # Strategy telemetry is deliberately excluded. Economic evidence is
            # linked by position/order IDs above; a symbol-wide telemetry scan
            # is neither authoritative nor bounded.
            "strategy_events": [],
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
                "order_evidence": resolution.fingerprint_payload(),
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
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name='binance_orders'
                """
            )
            order_columns = {str(row[0]) for row in cur.fetchall()}
            required_order_columns = {
                "id", "order_id", "client_order_id", "position_id",
            }
            if required_order_columns.issubset(order_columns):
                reconciled_projection = (
                    "reconciled_position_id"
                    if "reconciled_position_id" in order_columns
                    else "NULL::BIGINT AS reconciled_position_id"
                )
                optional = [
                    (
                        f'"{name}" AS {name}' if name == "interval" else name
                    ) if name in order_columns else f"NULL::TEXT AS {name}"
                    for name in ("strategy", "interval", "order_purpose")
                ]
                cur.execute(
                    "SELECT id,order_id,client_order_id,position_id,"
                    + reconciled_projection + "," + ",".join(optional)
                    + " FROM public.binance_orders WHERE order_id=%s ORDER BY id",
                    (order_id,),
                )
                orders = _rows(cur)
            else:
                orders = []
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='binance_order_fills'
                """
            )
            fill_columns = {str(row[0]) for row in cur.fetchall()}
            required_fill_columns = {
                "id", "source", "trade_id", "order_id", "symbol", "side",
                "executed_qty", "avg_price", "commission_amount",
                "commission_asset", "event_time",
            }
            if required_fill_columns.issubset(fill_columns):
                cur.execute(
                    "SELECT id,source,trade_id,order_id,symbol,side,"
                    "executed_qty,avg_price,commission_amount,commission_asset,"
                    "event_time FROM public.binance_order_fills "
                    "WHERE lower(source)=lower(%s) AND trade_id=%s",
                    (source, trade_id),
                )
                fills = _rows(cur)
            else:
                fills = []
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


class ExternalEvidenceFileAdapter:
    """Validate operator-supplied immutable exchange evidence without DB writes."""

    REQUIRED_FIELDS = (
        "source", "exchange_order_id", "trade_id", "symbol", "side", "qty",
        "price", "fee", "fee_asset", "timestamp", "client_order_id",
        "account_identity",
    )

    def read(
        self, path: str | Path, *, source: str, trade_id: str, order_id: str
    ) -> EvidenceEnvelope:
        raw = Path(path).read_bytes()
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("INVALID_EXTERNAL_EVIDENCE_JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("EXTERNAL_EVIDENCE_MUST_BE_OBJECT")
        missing = tuple(
            f"EXTERNAL_EVIDENCE_FIELD:{field}"
            for field in self.REQUIRED_FIELDS
            if field not in payload or payload[field] is None
        )
        if missing:
            return EvidenceEnvelope(EvidenceStatus.INCOMPLETE, missing, (), {})
        conflicts = []
        identities = (
            ("source", str(payload["source"]).lower(), str(source).lower()),
            ("trade_id", str(payload["trade_id"]), str(trade_id)),
            ("exchange_order_id", str(payload["exchange_order_id"]), str(order_id)),
        )
        conflicts.extend(
            f"EXTERNAL_EVIDENCE_IDENTITY:{name}"
            for name, actual, expected in identities if actual != expected
        )
        if str(payload["side"]).upper() not in {"BUY", "SELL"}:
            conflicts.append("EXTERNAL_EVIDENCE_SIDE")
        canonical = dict(payload)
        supplied_fingerprint = canonical.pop("source_fingerprint", None)
        fingerprint = semantic_repair_fingerprint(canonical)
        if supplied_fingerprint not in (None, fingerprint):
            conflicts.append("EXTERNAL_EVIDENCE_FINGERPRINT")
        canonical["source_fingerprint"] = fingerprint
        status = EvidenceStatus.CONFLICT if conflicts else EvidenceStatus.COMPLETE
        return EvidenceEnvelope(
            status, (), tuple(conflicts),
            {"adapter": "OPERATOR_IMMUTABLE_JSON", "path": str(path)},
            canonical, {"raw_sha256": hashlib.sha256(raw).hexdigest()},
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

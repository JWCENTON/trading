from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class OrderEvidenceSourceType(str, Enum):
    PAPER_SIMULATED_ORDER_SOURCE = "PAPER_SIMULATED_ORDER_SOURCE"
    LEGACY_ORDER_SOURCE = "LEGACY_ORDER_SOURCE"
    LIVE_EXCHANGE_ORDER_SOURCE = "LIVE_EXCHANGE_ORDER_SOURCE"
    UNSUPPORTED_ORDER_SOURCE = "UNSUPPORTED_ORDER_SOURCE"


SIMULATED_ORDER_COLUMNS = {
    "id", "created_at", "symbol", "interval", "strategy", "side",
    "price", "quantity_btc", "is_exit",
}
BINANCE_ORDER_COLUMNS = {
    "id", "created_at", "symbol", "side", "order_id", "client_order_id",
    "position_id",
}
BINANCE_FILL_COLUMNS = {
    "id", "source", "trade_id", "order_id", "symbol", "side",
    "executed_qty", "avg_price", "commission_amount", "commission_asset",
    "event_time", "instrument_snapshot_id", "account_identity_id",
}
SIMULATED_FILL_COLUMNS = {
    "id", "simulated_order_id", "position_id", "order_purpose", "side",
    "symbol", "fill_qty", "fill_price", "fill_notional", "fee_qty",
    "fee_asset", "account_identity_id", "instrument_snapshot_id",
    "source_authority", "environment", "deployment_id",
    "simulation_model_version", "execution_at",
}
INSTRUMENT_COLUMNS = {
    "id", "step_size", "quantity_precision", "base_asset", "quote_asset",
    "metadata_fingerprint",
}
ACCOUNT_COLUMNS = {"id", "identity_fingerprint"}


@dataclass(frozen=True)
class OrderEvidenceCapabilities:
    environment: str
    deployment_id: str
    database_identity: str
    source_type: OrderEvidenceSourceType
    source_table: str | None
    simulated_orders: bool
    simulated_execution_fills: bool
    binance_orders: bool
    binance_order_fills: bool
    reconciled_position_id: bool
    issues: tuple[str, ...]
    writer_issues: tuple[str, ...]

    @property
    def supported(self) -> bool:
        return self.source_type is not OrderEvidenceSourceType.UNSUPPORTED_ORDER_SOURCE

    @property
    def planner_ready(self) -> bool:
        return self.supported and not self.issues

    @property
    def writer_ready(self) -> bool:
        return self.planner_ready and not self.writer_issues

    def public_payload(self) -> Mapping[str, Any]:
        return {
            "status": "PRESENT_VALID" if self.planner_ready else "UNSUPPORTED",
            "writer_status": (
                "PRESENT_VALID" if self.writer_ready else "NOT_READY"
            ),
            "order_evidence_source": self.source_type.value,
            "source_table": self.source_table,
            "database_identity": self.database_identity,
            "deployment_id": self.deployment_id,
            "capabilities": {
                "simulated_orders": self.simulated_orders,
                "simulated_execution_fills_v1": self.simulated_execution_fills,
                "binance_orders": self.binance_orders,
                "binance_order_fills": self.binance_order_fills,
                "reconciled_position_id": self.reconciled_position_id,
                "order_status_writable": (
                    self.source_type
                    is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
                    or self.source_type
                    in {
                        OrderEvidenceSourceType.LEGACY_ORDER_SOURCE,
                        OrderEvidenceSourceType.LIVE_EXCHANGE_ORDER_SOURCE,
                    }
                    and not self.writer_issues
                ),
            },
            "issues": list(self.issues),
            "writer_issues": list(self.writer_issues),
        }


@dataclass(frozen=True)
class ResolvedOrderEvidence:
    source_type: OrderEvidenceSourceType
    source_table: str
    source_primary_key: int
    order_identity: str
    client_order_identity: str | None
    linkage_type: str
    matching_criteria: Mapping[str, Any]
    timestamp_delta_ms: int | None
    quantity: Decimal | None
    price: Decimal | None
    status: str | None
    side: str
    order_purpose: str

    def fingerprint_payload(self) -> Mapping[str, Any]:
        return {
            "source_type": self.source_type.value,
            "source_table": self.source_table,
            "source_primary_key": self.source_primary_key,
            "order_identity": self.order_identity,
            "client_order_identity": self.client_order_identity,
            "linkage_type": self.linkage_type,
            "matching_criteria": dict(self.matching_criteria),
            "timestamp_delta_ms": self.timestamp_delta_ms,
            "quantity": self.quantity,
            "price": self.price,
            "status": self.status,
            "side": self.side,
            "order_purpose": self.order_purpose,
        }


@dataclass(frozen=True)
class OrderEvidenceResolution:
    capabilities: OrderEvidenceCapabilities
    entry_orders: tuple[ResolvedOrderEvidence, ...]
    exit_orders: tuple[ResolvedOrderEvidence, ...]
    missing_evidence: tuple[str, ...]
    conflicting_evidence: tuple[str, ...]

    def fingerprint_payload(self) -> Mapping[str, Any]:
        return {
            "capability_contract_version": "LEGACY_ORDER_EVIDENCE_V1",
            "environment": self.capabilities.environment,
            "deployment_id": self.capabilities.deployment_id,
            "database_identity": self.capabilities.database_identity,
            "source_type": self.capabilities.source_type.value,
            "source_table": self.capabilities.source_table,
            "reconciled_position_id_available": (
                self.capabilities.reconciled_position_id
            ),
            "entry_orders": [item.fingerprint_payload() for item in self.entry_orders],
            "exit_orders": [item.fingerprint_payload() for item in self.exit_orders],
            "missing_evidence": list(self.missing_evidence),
            "conflicting_evidence": list(self.conflicting_evidence),
        }


def _columns(cur, tables: tuple[str, ...]) -> dict[str, set[str]]:
    cur.execute(
        """
        SELECT table_name,column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=ANY(%s)
        """,
        (list(tables),),
    )
    result: dict[str, set[str]] = {}
    for table, column in cur.fetchall():
        result.setdefault(str(table), set()).add(str(column))
    return result


def _rows(cur) -> list[dict[str, Any]]:
    names = [item[0] for item in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def _optional(columns: set[str], name: str, postgres_type: str) -> str:
    if name in columns:
        return f'"{name}" AS {name}' if name == "interval" else name
    return f"NULL::{postgres_type} AS {name}"


class LegacyRecoveryOrderEvidenceRepository:
    """Capability-aware, environment-scoped legacy order evidence V1."""

    MATCH_WINDOW_SECONDS = 5
    TABLES = (
        "positions", "simulated_orders", "simulated_execution_fills_v1",
        "binance_orders", "binance_order_fills",
        "financial_truth_instrument_snapshot_v1",
        "financial_truth_account_identity_v1",
    )

    @classmethod
    def detect_capabilities(
        cls, connection, *, environment: str, deployment_id: str,
    ) -> OrderEvidenceCapabilities:
        environment = str(environment).strip().upper()
        deployment_id = str(deployment_id).strip()
        if environment not in {"PAPER", "LIVE"}:
            raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
        if not deployment_id:
            raise RuntimeError("DEPLOYMENT_ID_REQUIRED")
        with connection.cursor() as cur:
            cur.execute("SELECT current_database()")
            database = str(cur.fetchone()[0])
            available = _columns(cur, cls.TABLES)
        simulated_orders = SIMULATED_ORDER_COLUMNS.issubset(
            available.get("simulated_orders", set())
        )
        evidence_joins = (
            INSTRUMENT_COLUMNS.issubset(available.get(
                "financial_truth_instrument_snapshot_v1", set()
            ))
            and ACCOUNT_COLUMNS.issubset(available.get(
                "financial_truth_account_identity_v1", set()
            ))
        )
        simulated_fills = (
            SIMULATED_FILL_COLUMNS.issubset(
                available.get("simulated_execution_fills_v1", set())
            ) and evidence_joins
        )
        binance_orders = BINANCE_ORDER_COLUMNS.issubset(
            available.get("binance_orders", set())
        )
        binance_fills = (
            BINANCE_FILL_COLUMNS.issubset(
                available.get("binance_order_fills", set())
            ) and evidence_joins
        )
        reconciled = "reconciled_position_id" in available.get(
            "binance_orders", set()
        )
        if environment == "PAPER" and simulated_orders:
            source = OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
            table = "simulated_orders"
        elif environment == "PAPER" and binance_orders:
            source = OrderEvidenceSourceType.LEGACY_ORDER_SOURCE
            table = "binance_orders"
        elif environment == "LIVE" and binance_orders:
            source = OrderEvidenceSourceType.LIVE_EXCHANGE_ORDER_SOURCE
            table = "binance_orders"
        else:
            source = OrderEvidenceSourceType.UNSUPPORTED_ORDER_SOURCE
            table = None
        issues: tuple[str, ...]
        if table is None:
            issues = ("ORDER_EVIDENCE_SOURCE_UNSUPPORTED",)
        elif (
            source is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
            and not simulated_fills
        ):
            issues = ("SIMULATED_FILL_EVIDENCE_SCHEMA_UNSUPPORTED",)
        elif source in {
            OrderEvidenceSourceType.LEGACY_ORDER_SOURCE,
            OrderEvidenceSourceType.LIVE_EXCHANGE_ORDER_SOURCE,
        } and not binance_fills:
            issues = ("EXCHANGE_FILL_EVIDENCE_SCHEMA_UNSUPPORTED",)
        else:
            issues = ()
        writer_issues = (
            ("ORDER_STATUS_WRITE_CAPABILITY_UNSUPPORTED",)
            if source in {
                OrderEvidenceSourceType.LEGACY_ORDER_SOURCE,
                OrderEvidenceSourceType.LIVE_EXCHANGE_ORDER_SOURCE,
            } and "status" not in available.get("binance_orders", set())
            else ()
        )
        return OrderEvidenceCapabilities(
            environment, deployment_id, database, source, table,
            simulated_orders, simulated_fills, binance_orders, binance_fills,
            reconciled, issues, writer_issues,
        )

    @classmethod
    def read_position(cls, connection, *, position_id: int) -> Mapping[str, Any] | None:
        with connection.cursor() as cur:
            available = _columns(cur, ("positions",)).get("positions", set())
            required = {
                "id", "symbol", "strategy", "interval", "status", "qty",
                "entry_order_id", "exit_order_id", "entry_time", "exit_time",
            }
            if not required.issubset(available):
                raise RuntimeError("POSITION_SCHEMA_UNSUPPORTED")
            projection = [
                "id", "symbol", "strategy", '"interval" AS interval', "status",
                "qty", "entry_order_id", "exit_order_id", "entry_time", "exit_time",
                _optional(available, "entry_client_order_id", "text"),
                _optional(available, "exit_client_order_id", "text"),
                _optional(available, "entry_price", "numeric"),
                _optional(available, "exit_price", "numeric"),
            ]
            cur.execute(
                "SELECT " + ",".join(projection)
                + " FROM public.positions WHERE id=%s",
                (int(position_id),),
            )
            rows = _rows(cur)
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("MULTIPLE_POSITIONS")
        return rows[0]

    @staticmethod
    def _delta_ms(order_time: datetime | None, position_time: datetime | None) -> int | None:
        if order_time is None or position_time is None:
            return None
        return int(
            (position_time.astimezone(timezone.utc) - order_time.astimezone(timezone.utc))
            .total_seconds() * 1000
        )

    @classmethod
    def _simulated_candidates(
        cls, connection, *, position: Mapping[str, Any], purpose: str,
        capabilities: OrderEvidenceCapabilities,
    ) -> list[ResolvedOrderEvidence]:
        is_exit = purpose == "EXIT"
        explicit = position.get("exit_order_id" if is_exit else "entry_order_id")
        position_time = position.get("exit_time" if is_exit else "entry_time")
        position_price = position.get("exit_price" if is_exit else "entry_price")
        side = "SELL" if is_exit else "BUY"
        with connection.cursor() as cur:
            # A canonical simulated fill is the strongest position/order linkage.
            if capabilities.simulated_execution_fills:
                cur.execute(
                    """
                    SELECT so.id,so.created_at,so.symbol,so.strategy,
                           so."interval" AS interval,so.side,so.price,
                           so.quantity_btc,so.is_exit,sf.order_purpose
                    FROM public.simulated_orders so
                    JOIN public.simulated_execution_fills_v1 sf
                      ON sf.simulated_order_id=so.id
                    WHERE sf.position_id=%s AND upper(sf.order_purpose)=%s
                      AND lower(sf.environment)=%s AND sf.deployment_id=%s
                    GROUP BY so.id,so.created_at,so.symbol,so.strategy,
                             so."interval",so.side,so.price,so.quantity_btc,
                             so.is_exit,sf.order_purpose
                    ORDER BY so.id
                    """,
                    (
                        int(position["id"]), purpose,
                        capabilities.environment.lower(),
                        capabilities.deployment_id,
                    ),
                )
                linked = _rows(cur)
                if linked:
                    return [
                        cls._simulated_record(
                            row, purpose=purpose, position_time=position_time,
                            linkage_type="SIMULATED_FILL_POSITION_LINKAGE",
                            matching={
                                "position_id": int(position["id"]),
                                "environment": capabilities.environment,
                                "deployment_id": capabilities.deployment_id,
                                "order_purpose": purpose,
                            },
                        )
                        for row in linked
                    ]
            if explicit is not None and str(explicit).isdigit():
                cur.execute(
                    """
                    SELECT id,created_at,symbol,strategy,"interval" AS interval,
                           side,price,quantity_btc,is_exit
                    FROM public.simulated_orders WHERE id=%s AND is_exit=%s
                    ORDER BY id
                    """,
                    (int(explicit), is_exit),
                )
                rows = _rows(cur)
                return [
                    cls._simulated_record(
                        row, purpose=purpose, position_time=position_time,
                        linkage_type="EXPLICIT_POSITION_ORDER_ID",
                        matching={"position_order_id": str(explicit)},
                    )
                    for row in rows
                ]
            # No timestamp-only fallback: every field below is an exact predicate.
            if position_time is None or position_price is None or position.get("qty") is None:
                return []
            cur.execute(
                """
                SELECT id,created_at,symbol,strategy,"interval" AS interval,
                       side,price,quantity_btc,is_exit
                FROM public.simulated_orders
                WHERE upper(symbol)=upper(%s) AND strategy=%s
                  AND "interval"=%s AND upper(side)=%s AND is_exit=%s
                  AND price=%s AND quantity_btc=%s
                  AND created_at<=%s
                  AND created_at>=%s-(%s * interval '1 second')
                ORDER BY id
                """,
                (
                    position["symbol"], position["strategy"],
                    position["interval"], side, is_exit,
                    position_price, position["qty"], position_time,
                    position_time, cls.MATCH_WINDOW_SECONDS,
                ),
            )
            rows = _rows(cur)
        matching = {
            "environment": capabilities.environment,
            "symbol": str(position["symbol"]).upper(),
            "strategy": position["strategy"],
            "interval": position["interval"],
            "side": side,
            "order_purpose": purpose,
            "price": position_price,
            "quantity": position["qty"],
            "created_at_lte_position_time": True,
            "max_timestamp_delta_seconds": cls.MATCH_WINDOW_SECONDS,
        }
        return [
            cls._simulated_record(
                row, purpose=purpose, position_time=position_time,
                linkage_type="DETERMINISTIC_EXACT_RECONSTRUCTION",
                matching=matching,
            )
            for row in rows
        ]

    @classmethod
    def _simulated_record(
        cls, row: Mapping[str, Any], *, purpose: str,
        position_time: datetime | None, linkage_type: str,
        matching: Mapping[str, Any],
    ) -> ResolvedOrderEvidence:
        return ResolvedOrderEvidence(
            OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE,
            "simulated_orders", int(row["id"]), str(row["id"]), None,
            linkage_type, matching,
            cls._delta_ms(row.get("created_at"), position_time),
            Decimal(str(row["quantity_btc"])), Decimal(str(row["price"])),
            "FILLED", str(row["side"]).upper(), purpose,
        )

    @classmethod
    def _binance_candidates(
        cls, connection, *, position: Mapping[str, Any], purpose: str,
        capabilities: OrderEvidenceCapabilities,
    ) -> list[ResolvedOrderEvidence]:
        is_exit = purpose == "EXIT"
        explicit_order = position.get(
            "exit_order_id" if is_exit else "entry_order_id"
        )
        explicit_client = position.get(
            "exit_client_order_id" if is_exit else "entry_client_order_id"
        )
        side = "SELL" if is_exit else "BUY"
        with connection.cursor() as cur:
            cols = _columns(cur, ("binance_orders",)).get("binance_orders", set())
            projection = [
                "id", "created_at", "symbol", "side", "order_id",
                "client_order_id", "position_id",
                _optional(cols, "reconciled_position_id", "bigint"),
                _optional(cols, "strategy", "text"),
                _optional(cols, "interval", "text"),
                _optional(cols, "order_purpose", "text"),
                _optional(cols, "status", "text"),
                _optional(cols, "requested_qty", "numeric"),
                _optional(cols, "price", "numeric"),
            ]
            clauses = ["upper(side)=%s"]
            params: list[Any] = [side]
            identity_clauses = ["position_id=%s"]
            identity_params: list[Any] = [int(position["id"])]
            if capabilities.reconciled_position_id:
                identity_clauses.append("reconciled_position_id=%s")
                identity_params.append(int(position["id"]))
            if explicit_order is not None:
                identity_clauses.append("order_id=%s")
                identity_params.append(str(explicit_order))
            if explicit_client is not None:
                identity_clauses.append("client_order_id=%s")
                identity_params.append(str(explicit_client))
            if "order_purpose" in cols:
                clauses.append("(order_purpose IS NULL OR upper(order_purpose)=%s)")
                params.append(purpose)
            cur.execute(
                "SELECT " + ",".join(projection)
                + " FROM public.binance_orders WHERE "
                + " AND ".join(clauses)
                + " AND (" + " OR ".join(identity_clauses) + ") ORDER BY id",
                tuple(params + identity_params),
            )
            rows = _rows(cur)
        result = []
        for row in rows:
            if explicit_order is not None and str(row.get("order_id")) == str(explicit_order):
                linkage = "EXPLICIT_POSITION_ORDER_ID"
            elif explicit_client is not None and str(row.get("client_order_id")) == str(explicit_client):
                linkage = "EXPLICIT_POSITION_CLIENT_ORDER_ID"
            elif row.get("position_id") == int(position["id"]):
                linkage = "AUTHORITATIVE_POSITION_ID"
            else:
                linkage = "OPTIONAL_RECONCILED_POSITION_HINT"
            result.append(ResolvedOrderEvidence(
                capabilities.source_type, "binance_orders", int(row["id"]),
                str(row["order_id"]),
                None if row.get("client_order_id") is None else str(row["client_order_id"]),
                linkage,
                {"position_id": int(position["id"]), "side": side,
                 "order_purpose": purpose},
                cls._delta_ms(
                    row.get("created_at"),
                    position.get("exit_time" if is_exit else "entry_time"),
                ),
                None if row.get("requested_qty") is None else Decimal(str(row["requested_qty"])),
                None if row.get("price") is None else Decimal(str(row["price"])),
                None if row.get("status") is None else str(row["status"]),
                str(row["side"]).upper(), purpose,
            ))
        return result

    @classmethod
    def resolve(
        cls, connection, *, position: Mapping[str, Any], environment: str,
        deployment_id: str,
    ) -> OrderEvidenceResolution:
        caps = cls.detect_capabilities(
            connection, environment=environment, deployment_id=deployment_id,
        )
        if not caps.supported:
            return OrderEvidenceResolution(
                caps, (), (), ("ORDER_EVIDENCE_SOURCE_UNSUPPORTED",), (),
            )
        resolver = (
            cls._simulated_candidates
            if caps.source_type is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
            else cls._binance_candidates
        )
        entries = resolver(
            connection, position=position, purpose="ENTRY", capabilities=caps,
        )
        exits = resolver(
            connection, position=position, purpose="EXIT", capabilities=caps,
        )
        missing: list[str] = []
        conflicts: list[str] = []
        if not entries:
            missing.append("ENTRY_ORDER_EVIDENCE_NOT_FOUND")
        elif len(entries) > 1:
            conflicts.append("ENTRY_ORDER_EVIDENCE_AMBIGUOUS")
        if not exits:
            missing.append("EXIT_ORDER_EVIDENCE_NOT_FOUND")
        elif len(exits) > 1:
            conflicts.append("EXIT_ORDER_EVIDENCE_AMBIGUOUS")

        # PAPER precedence is simulated. A second authoritative legacy linkage
        # is nevertheless a conflict, never an arbitrary alternative choice.
        if caps.source_type is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE and caps.binance_orders:
            legacy_caps = OrderEvidenceCapabilities(
                caps.environment, caps.deployment_id, caps.database_identity,
                OrderEvidenceSourceType.LEGACY_ORDER_SOURCE, "binance_orders",
                caps.simulated_orders, caps.simulated_execution_fills,
                caps.binance_orders, caps.binance_order_fills,
                caps.reconciled_position_id, (), caps.writer_issues,
            )
            legacy_entries = cls._binance_candidates(
                connection, position=position, purpose="ENTRY",
                capabilities=legacy_caps,
            )
            legacy_exits = cls._binance_candidates(
                connection, position=position, purpose="EXIT",
                capabilities=legacy_caps,
            )
            if legacy_entries or legacy_exits:
                conflicts.append("ORDER_EVIDENCE_SOURCE_CONFLICT")
        return OrderEvidenceResolution(
            caps, tuple(entries), tuple(exits),
            tuple(dict.fromkeys(missing)), tuple(dict.fromkeys(conflicts)),
        )

    @classmethod
    def lock_order_evidence(cls, cur, plan) -> None:
        caps = cls.detect_capabilities(
            cur.connection, environment=plan.environment,
            deployment_id=plan.deployment_id,
        )
        if not caps.writer_ready:
            raise RuntimeError("WRITER_ORDER_EVIDENCE_SOURCE_UNSUPPORTED")
        expected = str(plan.order_evidence["source_type"])
        if caps.source_type.value != expected or caps.database_identity != plan.database_name:
            raise RuntimeError("PLAN_STALE")
        order_rows = list(plan.order_evidence.get("entry_orders") or []) + list(
            plan.order_evidence.get("exit_orders") or []
        )
        ids = sorted(int(item["source_primary_key"]) for item in order_rows)
        table = str(plan.order_evidence.get("source_table") or "")
        if table not in {"simulated_orders", "binance_orders"} or table != caps.source_table:
            raise RuntimeError("PLAN_STALE")
        cur.execute(
            f"SELECT id FROM public.{table} WHERE id=ANY(%s) ORDER BY id FOR UPDATE",
            (ids or [-1],),
        )
        if [int(row[0]) for row in cur.fetchall()] != ids:
            raise RuntimeError("PLAN_STALE")
        fill_ids = sorted(int(item) for item in plan.entry_fill_ids + plan.exit_fill_ids)
        if fill_ids:
            fill_table = (
                "simulated_execution_fills_v1"
                if caps.source_type is OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
                else "binance_order_fills"
            )
            cur.execute(
                f"SELECT id FROM public.{fill_table} WHERE id=ANY(%s) "
                "ORDER BY id FOR UPDATE",
                (fill_ids,),
            )
            if [int(row[0]) for row in cur.fetchall()] != fill_ids:
                raise RuntimeError("PLAN_STALE")
        # Locking an exact prior row is insufficient when a new exact candidate
        # appeared. Re-resolve the complete candidate set under the writer
        # transaction and compare the same semantic contract used by plan V2.
        position = cls.read_position(
            cur.connection, position_id=int(plan.position_id),
        )
        if position is None:
            raise RuntimeError("PLAN_STALE")
        current = cls.resolve(
            cur.connection, position=position, environment=plan.environment,
            deployment_id=plan.deployment_id,
        ).fingerprint_payload()
        from common.legacy_recovery import semantic_repair_fingerprint
        if semantic_repair_fingerprint(current) != semantic_repair_fingerprint(
            plan.order_evidence
        ):
            raise RuntimeError("PLAN_STALE")

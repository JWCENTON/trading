from __future__ import annotations

from dataclasses import dataclass
from typing import Any


LEGACY_SCHEMA_VERSION = "LEGACY_SIMULATED_ORDER_SCHEMA"
NAMESPACE_SCHEMA_VERSION = "SIMULATED_ORDER_NAMESPACE_V1"
FORWARD_ORDER_CLASS = "FORWARD"
ADMINISTRATIVE_ORDER_CLASS = "LEGACY_ADMINISTRATIVE_CLOSE"
MIGRATION_REQUIRED = "SIMULATED_ORDER_NAMESPACE_MIGRATION_REQUIRED"
MIGRATION_ID = "20260802_simulated_order_namespace_v1.sql"
CONTRACT_CHECKSUM = (
    "c130ae635082d1c57b81c2cb6ca072436650a081b877584c31db0fb006da4746"
)

NAMESPACE_COLUMNS = {
    "order_class": ("text", "NO"),
    "position_id": ("bigint", "YES"),
    "environment": ("text", "YES"),
    "deployment_id": ("text", "YES"),
}
LEGACY_UNIQUENESS_OBJECTS = {
    "sim_orders_uniq_candle_exit",
    "ux_sim_orders_one_per_candle",
    "ux_sim_orders_one_per_candle_isexit",
}
FORWARD_INDEX = "ux_sim_orders_forward_one_per_candle"
ADMINISTRATIVE_INDEX = "ux_sim_orders_admin_position"
CLASS_CONSTRAINT = "ck_sim_orders_order_class"
IDENTITY_CONSTRAINT = "ck_sim_orders_order_identity"
POSITION_FK = "fk_sim_orders_position"


def _normalized(value: Any) -> str:
    return "".join(str(value or "").lower().split())


@dataclass(frozen=True)
class SimulatedOrderNamespaceReadiness:
    schema_version: str
    status: str
    columns: dict[str, bool]
    forward_slot_constraint: str
    administrative_position_idempotency: str
    legacy_global_constraints_absent: bool
    forward_writer_readiness: str
    retirement_writer_readiness: str
    issues: tuple[str, ...]

    @property
    def is_legacy(self) -> bool:
        return (
            self.schema_version == LEGACY_SCHEMA_VERSION
            and self.status == "PRESENT_VALID"
        )

    @property
    def is_namespace_v1(self) -> bool:
        return (
            self.schema_version == NAMESPACE_SCHEMA_VERSION
            and self.status == "PRESENT_VALID"
        )

    def public_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "columns": dict(self.columns),
            "forward_slot_constraint": self.forward_slot_constraint,
            "administrative_position_idempotency": (
                self.administrative_position_idempotency
            ),
            "legacy_global_constraints_absent": (
                self.legacy_global_constraints_absent
            ),
            "forward_writer_readiness": self.forward_writer_readiness,
            "retirement_writer_readiness": self.retirement_writer_readiness,
            "issues": list(self.issues),
        }


def detect_simulated_order_namespace(connection) -> SimulatedOrderNamespaceReadiness:
    """Read PostgreSQL catalogs only and classify the exact writer contract."""
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT column_name,data_type,is_nullable,column_default
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='simulated_orders'
            """
        )
        columns = {
            str(name): (str(dtype), str(nullable), default)
            for name, dtype, nullable, default in cur.fetchall()
        }
        if not columns:
            return SimulatedOrderNamespaceReadiness(
                schema_version="MISSING_SIMULATED_ORDER_SCHEMA",
                status="PRESENT_INVALID",
                columns={name: False for name in NAMESPACE_COLUMNS},
                forward_slot_constraint="MISSING",
                administrative_position_idempotency="MISSING",
                legacy_global_constraints_absent=True,
                forward_writer_readiness="NOT_READY",
                retirement_writer_readiness="NOT_READY",
                issues=("MISSING_TABLE:simulated_orders",),
            )
        cur.execute(
            """
            SELECT conname,lower(pg_get_constraintdef(oid)),contype,convalidated
            FROM pg_constraint
            WHERE conrelid='public.simulated_orders'::regclass
            """
        )
        constraints = {
            str(name): (str(definition), str(kind), bool(validated))
            for name, definition, kind, validated in cur.fetchall()
        }
        cur.execute(
            """
            SELECT index_relation.relname,pg_get_indexdef(index_row.indexrelid),
                   pg_get_expr(index_row.indpred,index_row.indrelid),
                   index_row.indisunique,index_row.indisvalid,index_row.indisready
            FROM pg_index index_row
            JOIN pg_class index_relation
              ON index_relation.oid=index_row.indexrelid
            JOIN pg_namespace namespace
              ON namespace.oid=index_relation.relnamespace
            WHERE namespace.nspname='public'
              AND index_row.indrelid='public.simulated_orders'::regclass
            """
        )
        indexes = {
            str(name): {
                "definition": str(definition),
                "predicate": predicate,
                "unique": bool(unique),
                "valid": bool(valid),
                "ready": bool(ready),
            }
            for name, definition, predicate, unique, valid, ready
            in cur.fetchall()
        }

    column_presence = {name: name in columns for name in NAMESPACE_COLUMNS}
    present_namespace_columns = {name for name, present in column_presence.items() if present}
    legacy_objects_present = LEGACY_UNIQUENESS_OBJECTS.intersection(
        set(indexes) | set(constraints)
    )

    if not present_namespace_columns:
        issues = []
        expected_legacy = {
            "ux_sim_orders_one_per_candle": (
                "(symbol,\"interval\",strategy,candle_open_time)",
                None,
            ),
            "ux_sim_orders_one_per_candle_isexit": (
                "(symbol,\"interval\",strategy,candle_open_time,is_exit)",
                None,
            ),
            "sim_orders_uniq_candle_exit": (
                "(symbol,\"interval\",strategy,candle_open_time,is_exit)",
                None,
            ),
        }
        for name, (fragment, predicate) in expected_legacy.items():
            index = indexes.get(name)
            if index is None:
                issues.append(f"MISSING_LEGACY_INDEX:{name}")
            elif (
                not index["unique"]
                or not index["valid"]
                or not index["ready"]
                or _normalized(fragment) not in _normalized(index["definition"])
                or index["predicate"] is not predicate
            ):
                issues.append(f"LEGACY_INDEX_MISMATCH:{name}")
        valid = not issues
        return SimulatedOrderNamespaceReadiness(
            schema_version=LEGACY_SCHEMA_VERSION,
            status="PRESENT_VALID" if valid else "PRESENT_INVALID",
            columns=column_presence,
            forward_slot_constraint=(
                "PRESENT_VALID_LEGACY" if valid else "PRESENT_INVALID"
            ),
            administrative_position_idempotency="MISSING",
            legacy_global_constraints_absent=False,
            forward_writer_readiness=(
                "PRESENT_VALID_LEGACY_COMPAT" if valid else "NOT_READY"
            ),
            retirement_writer_readiness=MIGRATION_REQUIRED,
            issues=tuple(issues),
        )

    issues = []
    if present_namespace_columns != set(NAMESPACE_COLUMNS):
        missing = sorted(set(NAMESPACE_COLUMNS) - present_namespace_columns)
        issues.append("PARTIAL_NAMESPACE_COLUMNS:" + ",".join(missing))
    for name, expected in NAMESPACE_COLUMNS.items():
        actual = columns.get(name)
        if actual is not None and actual[:2] != expected:
            issues.append(f"COLUMN_MISMATCH:{name}:{actual[:2]!r}")
    order_class = columns.get("order_class")
    if order_class is not None and _normalized(order_class[2]) not in {
        "'forward'::text", "'forward'"
    }:
        issues.append("COLUMN_DEFAULT_MISMATCH:order_class")

    constraint_fragments = {
        CLASS_CONSTRAINT: (
            "order_class", "forward", "legacy_administrative_close"
        ),
        IDENTITY_CONSTRAINT: (
            "position_id", "environment", "deployment_id", "is_exit",
            "side", "reason", "legacy_administrative_close", "forward",
        ),
        POSITION_FK: (
            "foreign key (position_id)", "references positions(id)",
            "on delete restrict",
        ),
    }
    for name, fragments in constraint_fragments.items():
        constraint = constraints.get(name)
        definition = _normalized(constraint[0]) if constraint else ""
        if (
            constraint is None
            or not constraint[2]
            or any(_normalized(fragment) not in definition for fragment in fragments)
        ):
            issues.append(f"CONSTRAINT_MISMATCH:{name}")

    expected_indexes = {
        FORWARD_INDEX: (
            "(symbol,\"interval\",strategy,candle_open_time)",
            "order_class='forward'::text",
        ),
        ADMINISTRATIVE_INDEX: (
            "(environment,deployment_id,position_id)",
            "order_class='legacy_administrative_close'::text",
        ),
    }
    index_validity = {}
    for name, (keys, predicate) in expected_indexes.items():
        index = indexes.get(name)
        valid = bool(
            index
            and index["unique"]
            and index["valid"]
            and index["ready"]
            and _normalized(keys) in _normalized(index["definition"])
            and _normalized(predicate) in _normalized(index["predicate"])
        )
        index_validity[name] = valid
        if not valid:
            issues.append(f"INDEX_MISMATCH:{name}")

    legacy_absent = not legacy_objects_present
    if not legacy_absent:
        issues.append(
            "LEGACY_GLOBAL_UNIQUENESS_PRESENT:"
            + ",".join(sorted(legacy_objects_present))
        )
    valid = not issues
    return SimulatedOrderNamespaceReadiness(
        schema_version=(
            NAMESPACE_SCHEMA_VERSION if valid else "UNKNOWN_SIMULATED_ORDER_SCHEMA"
        ),
        status="PRESENT_VALID" if valid else "PRESENT_INVALID",
        columns=column_presence,
        forward_slot_constraint=(
            "PRESENT_VALID" if index_validity.get(FORWARD_INDEX) else "PRESENT_INVALID"
        ),
        administrative_position_idempotency=(
            "PRESENT_VALID"
            if index_validity.get(ADMINISTRATIVE_INDEX)
            else "PRESENT_INVALID"
        ),
        legacy_global_constraints_absent=legacy_absent,
        forward_writer_readiness="PRESENT_VALID" if valid else "NOT_READY",
        retirement_writer_readiness="PRESENT_VALID" if valid else "NOT_READY",
        issues=tuple(issues),
    )


def require_simulated_order_namespace_v1(connection) -> SimulatedOrderNamespaceReadiness:
    readiness = detect_simulated_order_namespace(connection)
    if not readiness.is_namespace_v1:
        if readiness.is_legacy:
            raise RuntimeError(MIGRATION_REQUIRED)
        raise RuntimeError(
            "SIMULATED_ORDER_NAMESPACE_SCHEMA_INVALID:"
            + ",".join(readiness.issues)
        )
    return readiness

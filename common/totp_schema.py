from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


TOTP_TABLES = ("user_totp", "user_recovery_codes")

TOTP_COLUMNS = {
    "user_totp": (
        ("user_id", "integer", False, None),
        ("totp_secret", "text", False, None),
        ("enabled", "boolean", False, "false"),
        ("created_at", "timestamp with time zone", False, "now"),
        ("enabled_at", "timestamp with time zone", True, None),
        ("disabled_at", "timestamp with time zone", True, None),
        ("last_used_at", "timestamp with time zone", True, None),
    ),
    "user_recovery_codes": (
        ("id", "bigint", False, "sequence"),
        ("user_id", "integer", False, None),
        ("code_hash", "text", False, None),
        ("created_at", "timestamp with time zone", False, "now"),
        ("used_at", "timestamp with time zone", True, None),
    ),
}

TOTP_INDEXES = {
    "ix_user_recovery_codes_user_active": (
        "user_recovery_codes",
        "(user_id, used_at)",
    ),
}


@dataclass(frozen=True)
class TotpSchemaReport:
    ready: bool
    missing_objects: tuple[str, ...]
    mismatched_objects: tuple[str, ...]

    def failure_message(self) -> str:
        missing = ",".join(self.missing_objects) or "none"
        mismatched = ",".join(self.mismatched_objects) or "none"
        return (
            "TOTP_SCHEMA_NOT_READY: "
            f"missing_objects={missing}; mismatched_objects={mismatched}"
        )

    def preflight_output(self) -> str:
        missing = ",".join(self.missing_objects) or "none"
        mismatched = ",".join(self.mismatched_objects) or "none"
        ready = "YES" if self.ready else "NO"
        return "\n".join(
            (
                f"TOTP_SCHEMA_READY={ready}",
                f"missing_objects={missing}",
                f"mismatched_objects={mismatched}",
            )
        )


def _normalize_definition(value: str) -> str:
    normalized = value.lower().replace('"', "").replace("public.", "")
    return re.sub(r"\s+", " ", normalized).strip()


def _default_matches(actual: str | None, expected: str | None) -> bool:
    if expected is None:
        return actual is None
    normalized = _normalize_definition(actual or "").replace(" ", "")
    if expected == "false":
        return normalized in {"false", "false::boolean"}
    if expected == "now":
        return normalized == "now()"
    if expected == "sequence":
        return normalized.startswith("nextval(")
    raise ValueError(f"unsupported expected default: {expected}")


def inspect_totp_schema(cur: Any) -> TotpSchemaReport:
    """Inspect the canonical TOTP shape using SELECT-only catalog queries."""
    missing: set[str] = set()
    mismatched: set[str] = set()

    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = ANY(%s)
        ORDER BY table_name
        """,
        (list(TOTP_TABLES),),
    )
    existing_tables = {str(row[0]) for row in (cur.fetchall() or [])}
    for table in TOTP_TABLES:
        if table not in existing_tables:
            missing.add(f"table:public.{table}")

    cur.execute(
        """
        SELECT table_name,column_name,data_type,is_nullable,column_default,
               ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ANY(%s)
        ORDER BY table_name,ordinal_position
        """,
        (list(TOTP_TABLES),),
    )
    actual_columns: dict[str, list[tuple[str, str, bool, str | None]]] = {
        table: [] for table in TOTP_TABLES
    }
    for table, column, data_type, nullable, default, _ordinal in (
        cur.fetchall() or []
    ):
        actual_columns[str(table)].append(
            (str(column), str(data_type), str(nullable) == "YES", default)
        )

    for table, expected_columns in TOTP_COLUMNS.items():
        actual_by_name = {
            column[0]: column for column in actual_columns.get(table, [])
        }
        expected_names = [column[0] for column in expected_columns]
        actual_names = [column[0] for column in actual_columns.get(table, [])]
        for name, data_type, nullable, default in expected_columns:
            object_name = f"column:public.{table}.{name}"
            actual = actual_by_name.get(name)
            if table not in existing_tables:
                continue
            if actual is None:
                missing.add(object_name)
                continue
            if (
                actual[1] != data_type
                or actual[2] != nullable
                or not _default_matches(actual[3], default)
            ):
                mismatched.add(object_name)
        if table in existing_tables and actual_names != expected_names:
            mismatched.add(f"table:public.{table}:column_order_or_extras")

    cur.execute(
        """
        SELECT indexname,indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = ANY(%s)
        ORDER BY indexname
        """,
        (list(TOTP_INDEXES),),
    )
    actual_indexes = {
        str(name): _normalize_definition(str(definition))
        for name, definition in (cur.fetchall() or [])
    }
    for name, (table, columns) in TOTP_INDEXES.items():
        object_name = f"index:public.{name}"
        definition = actual_indexes.get(name)
        if definition is None:
            missing.add(object_name)
        elif (
            f" on {table} " not in definition
            or columns not in definition
            or definition.startswith("create unique index")
        ):
            mismatched.add(object_name)

    cur.execute(
        """
        SELECT rel.relname,con.contype,pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = 'public' AND rel.relname = ANY(%s)
          AND con.contype IN ('p','f')
        ORDER BY rel.relname,con.contype,con.conname
        """,
        (list(TOTP_TABLES),),
    )
    constraints: dict[str, list[tuple[str, str]]] = {
        table: [] for table in TOTP_TABLES
    }
    for table, constraint_type, definition in (cur.fetchall() or []):
        constraints[str(table)].append(
            (str(constraint_type), _normalize_definition(str(definition)))
        )

    expected_constraints = {
        "user_totp": (
            ("p", "primary key (user_id)"),
            (
                "f",
                "foreign key (user_id) references users(id) on delete cascade",
            ),
        ),
        "user_recovery_codes": (
            ("p", "primary key (id)"),
            (
                "f",
                "foreign key (user_id) references users(id) on delete cascade",
            ),
        ),
    }
    for table, expected_items in expected_constraints.items():
        if table not in existing_tables:
            continue
        for constraint_type, expected_definition in expected_items:
            kind = "primary_key" if constraint_type == "p" else "users_fk"
            object_name = f"constraint:public.{table}.{kind}"
            same_kind = [
                definition
                for actual_type, definition in constraints.get(table, [])
                if actual_type == constraint_type
            ]
            if expected_definition in same_kind:
                continue
            if same_kind:
                mismatched.add(object_name)
            else:
                missing.add(object_name)

    missing_items = tuple(sorted(missing))
    mismatched_items = tuple(sorted(mismatched))
    return TotpSchemaReport(
        ready=not missing_items and not mismatched_items,
        missing_objects=missing_items,
        mismatched_objects=mismatched_items,
    )


def require_totp_schema(cur: Any) -> TotpSchemaReport:
    report = inspect_totp_schema(cur)
    if not report.ready:
        raise RuntimeError(report.failure_message())
    return report

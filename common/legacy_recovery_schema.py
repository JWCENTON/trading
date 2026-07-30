from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json


MIGRATION_ID = "20260730_legacy_position_fill_recovery_v1.sql"
SCHEMA_VERSION = "LEGACY_RECOVERY_SCHEMA_V2"


class SchemaContractStatus(str, Enum):
    MISSING = "MISSING"
    PRESENT_VALID = "PRESENT_VALID"
    PRESENT_INVALID = "PRESENT_INVALID"
    CONTRACT_MISMATCH = "CONTRACT_MISMATCH"
    PARTIAL_INSTALLATION = "PARTIAL_INSTALLATION"


TABLE_COLUMNS = {
    "legacy_repair_audit_v1": {
        "audit_id": ("bigint", "NO"),
        "incident_type": ("text", "NO"),
        "incident_identity": ("text", "NO"),
        "operation_type": ("text", "NO"),
        "planner_version": ("text", "NO"),
        "semantic_fingerprint_expected": ("text", "YES"),
        "plan_status": ("text", "NO"),
        "execution_status": ("text", "NO"),
        "invocation_identity": ("text", "NO"),
        "blocking_reasons": ("jsonb", "NO"),
        "recorded_at": ("timestamp with time zone", "NO"),
    },
    "legacy_repair_provenance_v1": {
        "provenance_id": ("bigint", "NO"),
        "evidence_source": ("text", "NO"),
        "source_identity": ("text", "NO"),
        "source_fingerprint": ("text", "NO"),
        "instrument_identity": ("text", "YES"),
        "account_provenance": ("jsonb", "NO"),
        "deployment_provenance": ("jsonb", "NO"),
        "fee_evidence": ("jsonb", "NO"),
        "valuation_evidence": ("jsonb", "NO"),
        "immutable_payload": ("jsonb", "NO"),
        "observed_at": ("timestamp with time zone", "NO"),
        "recorded_at": ("timestamp with time zone", "NO"),
    },
    "exchange_fill_ingestion_state_v2": {
        "local_fill_id": ("bigint", "YES"),
        "linked_position_id": ("bigint", "YES"),
        "ownership_classification": ("text", "YES"),
        "classification_payload": ("jsonb", "NO"),
    },
}

INDEX_CONTRACT = {
    "ix_legacy_repair_audit_incident_history": (
        "legacy_repair_audit_v1",
        "(incident_type, incident_identity, recorded_at desc, audit_id desc)",
        False,
    ),
    "ix_legacy_repair_audit_semantic_expected": (
        "legacy_repair_audit_v1", "(semantic_fingerprint_expected)", False,
    ),
    "ix_legacy_repair_provenance_fingerprint": (
        "legacy_repair_provenance_v1", "(source_fingerprint)", False,
    ),
    "ix_legacy_repair_provenance_instrument_observed": (
        "legacy_repair_provenance_v1",
        "(instrument_identity, observed_at desc)", False,
    ),
    "ix_exchange_fill_ingestion_recovery_lookup": (
        "exchange_fill_ingestion_state_v2",
        "(source, symbol, order_id, trade_id)", False,
    ),
    "ix_exchange_fill_ingestion_application": (
        "exchange_fill_ingestion_state_v2",
        "(application_status, applied_fingerprint)", False,
    ),
}

CONSTRAINT_FRAGMENTS = {
    "ux_legacy_repair_audit_invocation": "unique (invocation_identity)",
    "ux_legacy_repair_provenance_source_identity": (
        "unique (evidence_source, source_identity)"
    ),
    "exchange_fill_ingestion_state_v2_application_status_check": (
        "observed_not_applied"
    ),
    "fk_exchange_fill_ingestion_local_fill": (
        "foreign key (local_fill_id) references binance_order_fills(id) "
        "on delete restrict"
    ),
    "fk_exchange_fill_ingestion_position": (
        "foreign key (linked_position_id) references positions(id) "
        "on delete restrict"
    ),
}

FUNCTION_CONTRACT = {
    "prevent_legacy_recovery_history_mutation_v1": "raise exception",
}

TRIGGER_CONTRACT = {
    "trg_legacy_repair_audit_append_only": (
        "legacy_repair_audit_v1",
        "prevent_legacy_recovery_history_mutation_v1",
    ),
    "trg_legacy_repair_provenance_immutable": (
        "legacy_repair_provenance_v1",
        "prevent_legacy_recovery_history_mutation_v1",
    ),
}


def canonical_manifest_checksum() -> str:
    payload = {
        "migration_id": MIGRATION_ID,
        "schema_version": SCHEMA_VERSION,
        "tables": TABLE_COLUMNS,
        "indexes": INDEX_CONTRACT,
        "constraints": CONSTRAINT_FRAGMENTS,
        "functions": FUNCTION_CONTRACT,
        "triggers": TRIGGER_CONTRACT,
    }
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


MANIFEST_CHECKSUM = canonical_manifest_checksum()


@dataclass(frozen=True)
class SchemaReadiness:
    status: SchemaContractStatus
    issues: tuple[str, ...]
    migration_id: str = MIGRATION_ID
    schema_version: str = SCHEMA_VERSION


class LegacyRecoverySchemaReadinessRepository:
    def check(self, connection) -> SchemaReadiness:
        issues = []
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name,column_name,data_type,is_nullable
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=ANY(%s)
                """,
                (list(TABLE_COLUMNS),),
            )
            actual = {
                (str(t), str(c)): (str(dtype), str(nullable))
                for t, c, dtype, nullable in cur.fetchall()
            }
            present_tables = {table for table, _column in actual}
            expected_new = {
                "legacy_repair_audit_v1", "legacy_repair_provenance_v1"
            }
            for table, columns in TABLE_COLUMNS.items():
                for column, contract in columns.items():
                    found = actual.get((table, column))
                    if found is None:
                        issues.append(f"MISSING_COLUMN:{table}.{column}")
                    elif found != contract:
                        issues.append(
                            f"COLUMN_MISMATCH:{table}.{column}:{found!r}"
                        )
            cur.execute(
                "SELECT indexname,lower(indexdef) FROM pg_indexes "
                "WHERE schemaname='public' AND indexname=ANY(%s)",
                (list(INDEX_CONTRACT),),
            )
            indexes = {str(name): str(defn) for name, defn in cur.fetchall()}
            for name, (table, columns, unique) in INDEX_CONTRACT.items():
                definition = indexes.get(name)
                if definition is None:
                    issues.append(f"MISSING_INDEX:{name}")
                elif (
                    f" on public.{table} " not in definition
                    or columns not in definition
                    or ("create unique index" in definition) != unique
                ):
                    issues.append(f"INDEX_MISMATCH:{name}")
            cur.execute(
                """
                SELECT conname,lower(pg_get_constraintdef(oid))
                FROM pg_constraint WHERE conname=ANY(%s)
                """,
                (list(CONSTRAINT_FRAGMENTS),),
            )
            constraints = {str(name): str(defn) for name, defn in cur.fetchall()}
            for name, fragment in CONSTRAINT_FRAGMENTS.items():
                if fragment not in constraints.get(name, ""):
                    issues.append(f"CONSTRAINT_MISMATCH:{name}")
            cur.execute(
                """
                SELECT p.proname,lower(pg_get_functiondef(p.oid))
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='public' AND p.proname=ANY(%s)
                """,
                (list(FUNCTION_CONTRACT),),
            )
            functions = {str(name): str(defn) for name, defn in cur.fetchall()}
            for name, fragment in FUNCTION_CONTRACT.items():
                if fragment not in functions.get(name, ""):
                    issues.append(f"FUNCTION_MISMATCH:{name}")
            cur.execute(
                """
                SELECT t.tgname,c.relname,p.proname
                FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                JOIN pg_proc p ON p.oid=t.tgfoid
                WHERE NOT t.tgisinternal AND t.tgname=ANY(%s)
                """,
                (list(TRIGGER_CONTRACT),),
            )
            triggers = {
                str(name): (str(table), str(function))
                for name, table, function in cur.fetchall()
            }
            for name, contract in TRIGGER_CONTRACT.items():
                if triggers.get(name) != contract:
                    issues.append(f"TRIGGER_MISMATCH:{name}")
            cur.execute(
                "SELECT checksum_sha256,schema_baseline_version,success "
                "FROM schema_migration_ledger_v1 WHERE migration_id=%s "
                "ORDER BY applied_at DESC LIMIT 1",
                (MIGRATION_ID,),
            )
            ledger = cur.fetchone()
            if ledger is None:
                issues.append("MISSING_MIGRATION_LEDGER")
            elif (
                str(ledger[0]) != MANIFEST_CHECKSUM
                or str(ledger[1]) != SCHEMA_VERSION
                or not ledger[2]
            ):
                issues.append("MIGRATION_LEDGER_CONTRACT_MISMATCH")
        if not issues:
            status = SchemaContractStatus.PRESENT_VALID
        elif present_tables & expected_new and present_tables & expected_new != expected_new:
            status = SchemaContractStatus.PARTIAL_INSTALLATION
        elif not present_tables & expected_new:
            status = SchemaContractStatus.MISSING
        elif any("MISMATCH" in issue for issue in issues):
            status = SchemaContractStatus.CONTRACT_MISMATCH
        else:
            status = SchemaContractStatus.PRESENT_INVALID
        return SchemaReadiness(status, tuple(issues))

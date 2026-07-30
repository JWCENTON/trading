from pathlib import Path
from types import SimpleNamespace

import pytest

from common.legacy_recovery_schema import (
    CONSTRAINT_FRAGMENTS,
    FUNCTION_CONTRACT,
    INDEX_CONTRACT,
    MANIFEST_CHECKSUM,
    MIGRATION_ID,
    SCHEMA_VERSION,
    TABLE_COLUMNS,
    TRIGGER_CONTRACT,
)
from tools.legacy_recovery import _connection_factory, parser


ROOT = Path(__file__).resolve().parents[1]
FORWARD = (
    ROOT / "db/migrations/20260730_legacy_position_fill_recovery_v1.sql"
).read_text()
ROLLBACK = (
    ROOT / "db/migrations/"
    "20260730_legacy_position_fill_recovery_v1_rollback.sql"
).read_text()


def test_manifest_lists_every_forward_object_and_contract_identity():
    assert MIGRATION_ID in FORWARD
    assert MANIFEST_CHECKSUM in FORWARD
    assert SCHEMA_VERSION in FORWARD
    for table, columns in TABLE_COLUMNS.items():
        assert table in FORWARD
        for column in columns:
            assert column in FORWARD
    for index in INDEX_CONTRACT:
        assert index in FORWARD
    for constraint in CONSTRAINT_FRAGMENTS:
        assert constraint in FORWARD
    for function in FUNCTION_CONTRACT:
        assert function in FORWARD
    for trigger in TRIGGER_CONTRACT:
        assert trigger in FORWARD


def test_forward_is_transactional_idempotent_and_checksum_conflict_safe():
    upper = FORWARD.upper()
    assert upper.startswith("-- WALTRADE")
    assert "BEGIN;" in upper and upper.rstrip().endswith("COMMIT;")
    assert "MIGRATION_CHECKSUM_CONFLICT" in upper
    assert "WHERE NOT EXISTS" in upper
    assert "UPDATE POSITIONS" not in upper
    assert "INSERT INTO BINANCE_ORDER_FILLS" not in upper


def test_audit_and_provenance_are_append_only_with_bounded_identities():
    assert "UNIQUE(INVOCATION_IDENTITY)" in FORWARD.upper()
    assert "UNIQUE(INCIDENT_TYPE,INCIDENT_IDENTITY)" not in FORWARD.upper()
    assert "UNIQUE(EVIDENCE_SOURCE,SOURCE_IDENTITY)" in FORWARD.upper()
    assert "BEFORE UPDATE OR DELETE ON LEGACY_REPAIR_AUDIT_V1" in FORWARD.upper()
    assert (
        "BEFORE UPDATE OR DELETE ON LEGACY_REPAIR_PROVENANCE_V1"
        in FORWARD.upper()
    )


def test_rollback_is_transactional_fail_closed_and_never_touches_trading_data():
    upper = ROLLBACK.upper()
    assert "BEGIN;" in upper and upper.rstrip().endswith("COMMIT;")
    assert "ROLLBACK_BLOCKED_HISTORY_EXISTS" in upper
    assert "DROP TABLE IF EXISTS LEGACY_REPAIR_AUDIT_V1" in upper
    assert "DROP TABLE IF EXISTS LEGACY_REPAIR_PROVENANCE_V1" in upper
    assert "CASCADE" not in upper
    assert "UPDATE POSITIONS" not in upper
    assert "UPDATE BINANCE_ORDERS" not in upper
    assert "UPDATE BINANCE_ORDER_FILLS" not in upper
    assert "UPDATE CANONICAL_FINANCIAL_TRUTH" not in upper


def test_cli_has_only_explicit_read_only_operations():
    help_text = parser().format_help()
    assert "--database-url-env" in help_text
    assert "--expected-database" in help_text
    subcommands = parser()._subparsers._group_actions[0].choices
    assert set(subcommands) == {
        "check-schema", "plan-position", "plan-fill", "classify-external",
    }
    assert "apply" not in subcommands


def test_api_image_owns_tooling_import_path():
    dockerfile = (ROOT / "api/Dockerfile").read_text()
    assert "COPY tools /app/tools" in dockerfile
    assert "ENV PYTHONPATH=/app" in dockerfile


def test_cli_requires_explicit_dsn_and_never_embeds_credentials(monkeypatch):
    monkeypatch.delenv("EXPLICIT_TEST_DSN", raising=False)
    with pytest.raises(RuntimeError, match="IS_REQUIRED"):
        _connection_factory(SimpleNamespace(
            database_url_env="EXPLICIT_TEST_DSN",
            expected_database="trading_live",
            environment="LIVE",
        ))
    source = (ROOT / "tools/legacy_recovery.py").read_text()
    assert '"password"' not in source
    assert "transaction_read_only" in source
    assert "read_only_db_conn" in source

from __future__ import annotations

from datetime import datetime, timezone
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.legacy_recovery_repository import (
    EvidenceStatus,
    ExternalExecutionEvidenceRepository,
    LegacyPositionEvidenceRepository,
    LegacyProvenanceRepository,
    LegacyRepairAuditRepository,
    UnappliedFillEvidenceRepository,
)
from common.legacy_recovery_schema import (
    LegacyRecoverySchemaReadinessRepository,
    MANIFEST_CHECKSUM,
    MIGRATION_ID,
    SCHEMA_VERSION,
    SchemaContractStatus,
)
from tools.legacy_recovery import _fill, _position, main as cli_main


ROOT = Path(__file__).resolve().parents[2]
FORWARD = (
    ROOT / "db/migrations/20260730_legacy_position_fill_recovery_v1.sql"
).read_text()
ROLLBACK = (
    ROOT / "db/migrations/"
    "20260730_legacy_position_fill_recovery_v1_rollback.sql"
).read_text()
BASELINE = (
    ROOT / "db/migrations/20260724_database_baseline_provenance_v1.sql"
).read_text()


@pytest.fixture()
def contract_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_legacy_contract"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    conn = disposable_postgres_v16.connect(name)
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP SCHEMA public CASCADE; CREATE SCHEMA public;
            CREATE TABLE positions(
              id BIGINT PRIMARY KEY,symbol TEXT,strategy TEXT,"interval" TEXT,
              status TEXT,qty NUMERIC,entry_order_id TEXT,exit_order_id TEXT,
              entry_time TIMESTAMPTZ DEFAULT now(),exit_time TIMESTAMPTZ
            );
            CREATE TABLE binance_orders(
              id BIGSERIAL PRIMARY KEY,order_id TEXT,symbol TEXT,side TEXT,
              client_order_id TEXT,position_id BIGINT,reconciled_position_id BIGINT,
              strategy TEXT,"interval" TEXT,order_purpose TEXT,
              created_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE TABLE binance_order_fills(
              id BIGSERIAL PRIMARY KEY,source TEXT,trade_id TEXT,order_id TEXT,
              symbol TEXT,side TEXT,executed_qty NUMERIC,avg_price NUMERIC,
              commission_amount NUMERIC,commission_asset TEXT,
              event_time TIMESTAMPTZ,instrument_snapshot_id BIGINT,
              account_identity_id BIGINT,raw JSONB DEFAULT '{}'::jsonb,
              UNIQUE(source,trade_id)
            );
            CREATE TABLE exchange_fill_ingestion_state_v2(
              ingestion_id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,
              account_identity_key TEXT NOT NULL,symbol TEXT NOT NULL,
              trade_id TEXT NOT NULL,order_id TEXT NOT NULL,side TEXT NOT NULL,
              source_fingerprint TEXT NOT NULL,applied_fingerprint TEXT,
              applied_at TIMESTAMPTZ,application_status TEXT NOT NULL,
              correction_revision INTEGER DEFAULT 0,
              authoritative_payload JSONB NOT NULL,last_decision TEXT NOT NULL,
              CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
                CHECK(application_status IN (
                  'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED',
                  'AMBIGUOUS','REJECTED'
                ))
            );
            CREATE TABLE financial_truth_instrument_snapshot_v1(
              id BIGINT PRIMARY KEY,step_size NUMERIC,quantity_precision INTEGER,
              base_asset TEXT,quote_asset TEXT,metadata_fingerprint TEXT
            );
            CREATE TABLE financial_truth_account_identity_v1(
              id BIGINT PRIMARY KEY,identity_fingerprint TEXT
            );
            CREATE TABLE position_lifecycle_events_c2_2(
              event_id BIGSERIAL PRIMARY KEY,position_id BIGINT
            );
            CREATE TABLE canonical_financial_truth_v1(
              position_id BIGINT PRIMARY KEY,financial_truth_status TEXT
            );
            CREATE TABLE strategy_events(
              id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ DEFAULT now(),
              symbol TEXT
            );
            """
        )
        cur.execute(BASELINE)
    conn.commit()
    yield conn
    conn.close()


def _apply(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _audit(invocation, fingerprint):
    now = datetime.now(timezone.utc)
    return {
        "incident_type": "LEGACY_POSITION", "incident_identity": "3080",
        "operation_type": "PLAN", "planner_version": "V2",
        "writer_version": None, "semantic_fingerprint_before": fingerprint,
        "semantic_fingerprint_expected": fingerprint,
        "semantic_fingerprint_after": None, "plan_status": "ELIGIBLE",
        "execution_status": "PLAN_ONLY", "invocation_identity": invocation,
        "requested_at": now, "started_at": now, "completed_at": now,
        "actor_source": "TEST", "blocking_reasons": [],
        "eligible_actions": ["PLAN"], "executed_actions": [],
        "expected_changes": [], "actual_changes": [],
        "post_state_invariants": ["NO_WRITES"],
        "error_code": None, "error_detail": None,
    }


def test_gate_a_forward_twice_ledger_and_manifest(contract_db):
    _apply(contract_db, FORWARD)
    _apply(contract_db, FORWARD)
    readiness = LegacyRecoverySchemaReadinessRepository().check(contract_db)
    assert readiness.status is SchemaContractStatus.PRESENT_VALID, readiness.issues
    with contract_db.cursor() as cur:
        cur.execute(
            "SELECT count(*),min(checksum_sha256),min(schema_baseline_version) "
            "FROM schema_migration_ledger_v1 WHERE migration_id=%s",
            (MIGRATION_ID,),
        )
        assert cur.fetchone() == (1, MANIFEST_CHECKSUM, SCHEMA_VERSION)


def test_gate_b_append_only_audit_and_provenance_conflict(contract_db):
    _apply(contract_db, FORWARD)
    first = "a" * 64
    second = "b" * 64
    with contract_db.cursor() as cur:
        assert LegacyRepairAuditRepository.append(cur, _audit("invoke-1", first))
        assert LegacyRepairAuditRepository.append(cur, _audit("invoke-2", second))
        assert not LegacyRepairAuditRepository.append(cur, _audit("invoke-1", first))
        provenance = {
            "evidence_source": "EXCHANGE_PAYLOAD",
            "source_identity": "okx:trade:1",
            "source_fingerprint": first,
            "instrument_identity": "BNB-USDC",
            "account_provenance": {}, "deployment_provenance": {},
            "fee_evidence": {}, "valuation_evidence": {},
            "immutable_payload": {"trade_id": "1"},
            "observed_at": datetime.now(timezone.utc),
        }
        assert LegacyProvenanceRepository.record(cur, provenance)
        assert not LegacyProvenanceRepository.record(cur, provenance)
        with pytest.raises(RuntimeError, match="PROVENANCE_IDENTITY_CONFLICT"):
            LegacyProvenanceRepository.record(
                cur, {**provenance, "source_fingerprint": second},
            )
    contract_db.commit()
    with contract_db.cursor() as cur:
        cur.execute(
            "SELECT semantic_fingerprint_expected FROM legacy_repair_audit_v1 "
            "ORDER BY recorded_at,audit_id"
        )
        assert [row[0] for row in cur.fetchall()] == [first, second]
        with pytest.raises(Exception, match="append-only"):
            cur.execute("UPDATE legacy_repair_audit_v1 SET plan_status='X'")
    contract_db.rollback()


def test_gate_c_rollback_before_use_restores_pre_schema(contract_db):
    _apply(contract_db, FORWARD)
    _apply(contract_db, ROLLBACK)
    _apply(contract_db, ROLLBACK)
    with contract_db.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.legacy_repair_audit_v1'),"
            "to_regclass('public.legacy_repair_provenance_v1')"
        )
        assert cur.fetchone() == (None, None)
        cur.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname='exchange_fill_ingestion_state_v2_application_status_check'"
        )
        assert "OBSERVED_NOT_APPLIED" not in cur.fetchone()[0]


def test_gate_d_rollback_after_history_fails_closed(contract_db):
    _apply(contract_db, FORWARD)
    with contract_db.cursor() as cur:
        LegacyRepairAuditRepository.append(cur, _audit("used", "c" * 64))
    contract_db.commit()
    with pytest.raises(Exception, match="ROLLBACK_BLOCKED_HISTORY_EXISTS"):
        with contract_db.cursor() as cur:
            cur.execute(ROLLBACK)
    contract_db.rollback()
    with contract_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM legacy_repair_audit_v1")
        assert cur.fetchone()[0] == 1


def _seed_complete_position(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO positions("
            "id,symbol,strategy,\"interval\",status,qty,entry_order_id,exit_order_id"
            ") VALUES "
            "(3080,'BNBUSDC','TREND','5m','OPEN',0.000123,'entry','exit')"
        )
        cur.execute(
            "INSERT INTO financial_truth_instrument_snapshot_v1 VALUES "
            "(1,0.000001,8,'BNB','USDC','instrument-fp')"
        )
        cur.execute(
            "INSERT INTO financial_truth_account_identity_v1 "
            "VALUES (1,'account-fp')"
        )
        cur.execute(
            "INSERT INTO binance_orders(order_id,symbol,side,client_order_id,"
            "reconciled_position_id) VALUES "
            "('entry','BNBUSDC','BUY','bot-entry',3080),"
            "('exit','BNBUSDC','SELL','bot-exit',3080)"
        )
        cur.execute(
            """
            INSERT INTO binance_order_fills(
              source,trade_id,order_id,symbol,side,executed_qty,avg_price,
              commission_amount,commission_asset,event_time,
              instrument_snapshot_id,account_identity_id
            ) VALUES
              ('okx','entry-trade','entry','BNBUSDC','BUY',0.035152,566.1,
               0.000123032,'BNB',now(),1,1),
              ('okx','exit-trade','exit','BNBUSDC','SELL',0.035029,567.1,
               0.06952731065,'USDC',now(),1,1)
            """
        )
        for trade_id, order_id, side in (
            ("entry-trade", "entry", "BUY"),
            ("exit-trade", "exit", "SELL"),
        ):
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,account_identity_key,symbol,trade_id,order_id,side,
                  source_fingerprint,application_status,authoritative_payload,
                  last_decision
                ) VALUES ('okx','acct','BNBUSDC',%s,%s,%s,%s,
                  'TRUE_DUPLICATE_APPLIED','{}','NO_CHANGE')
                """,
                (trade_id, order_id, side, ("1" if side == "BUY" else "2") * 64),
            )
    conn.commit()


def test_gate_e_position_reader_is_complete_and_decimal_safe(contract_db):
    _apply(contract_db, FORWARD)
    _seed_complete_position(contract_db)
    envelope = LegacyPositionEvidenceRepository().read(
        contract_db, position_id=3080,
    )
    assert envelope.evidence_status is EvidenceStatus.COMPLETE
    assert str(envelope.evidence.entry_fills[0].quantity) == "0.035152"
    assert envelope.source_provenance["account_fingerprints"] == ["account-fp"]
    plan = _position(
        contract_db, SimpleNamespace(position_id=3080),
        {"environment": "LIVE", "database": "fixture"},
        {"status": "PRESENT_VALID"},
    )
    assert plan["evidence_status"] == "COMPLETE"
    assert plan["normalized_remaining_inventory_qty"] == 0
    assert plan["recommended_lifecycle_status"] == "CLOSED"
    assert plan["financial_truth_eligibility"]
    assert plan["fee_valuation_status"]
    assert plan["provenance_completeness"] == "COMPLETE"


def test_gate_e2_large_unrelated_strategy_history_is_not_read(contract_db):
    _apply(contract_db, FORWARD)
    _seed_complete_position(contract_db)
    with contract_db.cursor() as cur:
        cur.execute(
            "INSERT INTO strategy_events(symbol) "
            "SELECT 'BNBUSDC' FROM generate_series(1,100000)"
        )
    contract_db.commit()
    started = time.monotonic()
    envelope = LegacyPositionEvidenceRepository().read(
        contract_db, position_id=3080,
    )
    elapsed = time.monotonic() - started
    assert envelope.current_state["strategy_events"] == []
    assert elapsed < 2


def test_gate_f_unapplied_reader_and_gate_g_external(contract_db):
    _apply(contract_db, FORWARD)
    with contract_db.cursor() as cur:
        cur.execute(
            "INSERT INTO exchange_fill_ingestion_state_v2("
            "source,account_identity_key,symbol,trade_id,order_id,side,"
            "source_fingerprint,application_status,authoritative_payload,"
            "last_decision) VALUES "
            "('okx','acct','BNBUSDC','341287','buy-order','BUY',%s,"
            "'OBSERVED_NOT_APPLIED','{}','NEW')",
            ("d" * 64,),
        )
        cur.execute(
            "INSERT INTO legacy_repair_provenance_v1("
            "evidence_source,source_identity,source_fingerprint,"
            "instrument_identity,immutable_payload,observed_at) VALUES "
            "('EXTERNAL_EXECUTION','okx:341617:external-order',%s,"
            "'BNB-USDC',%s,now())",
            (
                "e" * 64,
                '{"client_order_id":null,"position_id":null,'
                '"semantic_fingerprint":"' + "e" * 64 + '"}',
            ),
        )
    contract_db.commit()
    fill = UnappliedFillEvidenceRepository().read(
        contract_db, source="okx", trade_id="341287", order_id="buy-order",
    )
    assert fill.evidence_status is EvidenceStatus.COMPLETE
    assert fill.current_state["local_fills"] == []
    assert fill.current_state["ingestion"]["applied_fingerprint"] is None
    external = ExternalExecutionEvidenceRepository().read(
        contract_db, source="okx", trade_id="341617",
        order_id="external-order",
    )
    assert external.evidence_status is EvidenceStatus.COMPLETE
    assert external.evidence["client_order_id"] is None
    assert external.evidence["position_id"] is None


def test_gate_g2_bot_fill_without_position_has_explicit_incident_model(contract_db):
    _apply(contract_db, FORWARD)
    with contract_db.cursor() as cur:
        cur.execute(
            "INSERT INTO binance_orders("
            "order_id,symbol,side,client_order_id,strategy,\"interval\","
            "order_purpose) VALUES "
            "('missing-position','BNBUSDC','BUY','bot-entry','TREND','1m','ENTRY')"
        )
        cur.execute(
            "INSERT INTO exchange_fill_ingestion_state_v2("
            "source,account_identity_key,symbol,trade_id,order_id,side,"
            "source_fingerprint,application_status,authoritative_payload,"
            "last_decision) VALUES "
            "('okx','acct','BNBUSDC','trade-missing','missing-position','BUY',"
            "%s,'OBSERVED_NOT_APPLIED','{}','NEW')",
            ("f" * 64,),
        )
    contract_db.commit()
    result = _fill(
        contract_db,
        SimpleNamespace(
            source="okx", trade_id="trade-missing",
            order_id="missing-position",
        ),
        {"environment": "LIVE", "database": "fixture"},
        {"status": "PRESENT_VALID"},
    )
    assert result["incident_model"] == "MISSING_POSITION_AFTER_FILLED_ENTRY"
    assert result["linkage_classification"] == "BOT_OWNED_LINKABLE"


def test_gate_h_read_only_transaction_rejects_writer(contract_db):
    _apply(contract_db, FORWARD)
    contract_db.set_session(readonly=True)
    with pytest.raises(Exception, match="read-only transaction"):
        with contract_db.cursor() as cur:
            cur.execute("INSERT INTO legacy_repair_audit_v1 DEFAULT VALUES")
    contract_db.rollback()


def test_cli_check_schema_uses_explicit_identity_without_credential_output(
    contract_db, disposable_postgres_v16, monkeypatch, capsys,
):
    _apply(contract_db, FORWARD)
    dsn = (
        f"host=127.0.0.1 port={disposable_postgres_v16.port} "
        "dbname=waltrade_baseline_test_legacy_contract "
        f"user={disposable_postgres_v16.user} "
        f"password={disposable_postgres_v16.password}"
    )
    monkeypatch.setenv("LEGACY_TEST_DATABASE_URL", dsn)
    code = cli_main([
        "--database-url-env", "LEGACY_TEST_DATABASE_URL",
        "--environment", "LIVE",
        "--expected-database", "waltrade_baseline_test_legacy_contract",
        "check-schema",
    ])
    output = capsys.readouterr().out
    assert code == 0
    assert '"transaction_read_only":true' in output
    assert disposable_postgres_v16.password not in output


def test_manifest_detects_index_and_constraint_mismatch(contract_db):
    _apply(contract_db, FORWARD)
    with contract_db.cursor() as cur:
        cur.execute("DROP INDEX ix_legacy_repair_audit_semantic_expected")
        cur.execute(
            "ALTER TABLE exchange_fill_ingestion_state_v2 DROP CONSTRAINT "
            "exchange_fill_ingestion_state_v2_application_status_check"
        )
        cur.execute(
            "ALTER TABLE exchange_fill_ingestion_state_v2 ADD CONSTRAINT "
            "exchange_fill_ingestion_state_v2_application_status_check "
            "CHECK(application_status IN ('NEW'))"
        )
    contract_db.commit()
    result = LegacyRecoverySchemaReadinessRepository().check(contract_db)
    assert result.status is SchemaContractStatus.CONTRACT_MISMATCH
    assert "MISSING_INDEX:ix_legacy_repair_audit_semantic_expected" in result.issues
    assert any(
        item.startswith("CONSTRAINT_MISMATCH:") for item in result.issues
    )


def test_manifest_detects_partial_installation(contract_db):
    with contract_db.cursor() as cur:
        cur.execute(
            "CREATE TABLE legacy_repair_audit_v1(audit_id BIGINT)"
        )
    contract_db.commit()
    result = LegacyRecoverySchemaReadinessRepository().check(contract_db)
    assert result.status is SchemaContractStatus.PARTIAL_INSTALLATION


def test_forward_rejects_wrong_checksum_for_same_migration_id(contract_db):
    with contract_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO schema_migration_ledger_v1(
              migration_id,checksum_sha256,environment,deployment_id,
              database_name,applied_by,status,success,execution_duration_ms,
              git_sha,schema_baseline_version
            ) VALUES (%s,%s,'LIVE','TEST',current_database(),'test','APPLIED',
              true,0,'test','WRONG')
            """,
            (MIGRATION_ID, "f" * 64),
        )
    contract_db.commit()
    with pytest.raises(Exception, match="MIGRATION_CHECKSUM_CONFLICT"):
        with contract_db.cursor() as cur:
            cur.execute(FORWARD)
    contract_db.rollback()

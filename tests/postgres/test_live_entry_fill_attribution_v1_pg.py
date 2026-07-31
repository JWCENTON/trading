"""Disposable PostgreSQL 16 schema gates for the LEI1C fill ledgers."""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.entry_fill_attribution import (
    EntryFillAttributionMode,
    EntryFillAttributionRepository,
    EntryFillObservation,
    EntryFillProcessingOutcome,
    FillApplicationStatus,
    FillAttributionStatus,
    process_entry_fill_attribution,
)
from common.exchange_ingest_trades import (
    LEI1C_LEDGER_CHECKSUM,
    LEI1C_MIGRATION_ID,
)


ROOT = Path(__file__).resolve().parents[2]
LEI1A = (
    ROOT / "db/migrations/20260730_live_entry_intent_ledger_v1.sql"
).read_text()
LEI1B = (
    ROOT / "db/migrations/20260731_live_entry_submission_ack_v1.sql"
).read_text()
FORWARD_PATH = (
    ROOT / "db/migrations/20260731_live_entry_fill_attribution_v1.sql"
)
ROLLBACK_PATH = (
    ROOT
    / "db/migrations/20260731_live_entry_fill_attribution_v1_rollback.sql"
)
MANIFEST_PATH = (
    ROOT
    / "db/migrations/20260731_live_entry_fill_attribution_v1_manifest.json"
)
FORWARD = FORWARD_PATH.read_text()
ROLLBACK = ROLLBACK_PATH.read_text()
MANIFEST = json.loads(MANIFEST_PATH.read_text())

GIT_REVISION = "f" * 40
SOURCE_FINGERPRINT = "a" * 64
ATTRIBUTION_FINGERPRINT = "b" * 64
INTENT_ID = "11111111-1111-5111-8111-111111111111"
SUBMISSION_ID = "22222222-2222-5222-8222-222222222222"
ACK_ID = "33333333-3333-5333-8333-333333333333"
CLIENT_ORDER_ID = "ORC-L-BNBUSDC-TREN-1m-E-lei1c-pg"
WIRE_CLIENT_ORDER_ID = "ORCLBNBUSDCTREN1mElei1cpg"
TRADE_ID = "7001"
EXECUTED_AT = datetime(2026, 7, 31, 19, 0, 3, tzinfo=timezone.utc)
OBSERVED_AT = datetime(2026, 7, 31, 19, 0, 4, tzinfo=timezone.utc)
DECIDED_AT = datetime(2026, 7, 31, 19, 0, 5, tzinfo=timezone.utc)

BASE_SCHEMA = f"""
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGINT PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  status TEXT NOT NULL,
  git_revision TEXT NOT NULL,
  adopted_at TIMESTAMPTZ,
  deactivated_at TIMESTAMPTZ
);
CREATE TABLE schema_migration_ledger_v1 (
  ledger_id BIGSERIAL PRIMARY KEY,
  migration_id TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  database_name TEXT NOT NULL,
  applied_by TEXT NOT NULL,
  status TEXT NOT NULL,
  success BOOLEAN NOT NULL,
  execution_duration_ms BIGINT NOT NULL,
  git_sha TEXT NOT NULL,
  error_summary TEXT,
  schema_baseline_version TEXT NOT NULL
);
INSERT INTO runtime_contract_adoption_v2(
  adoption_id,contract_name,environment,deployment_id,generation,status,
  git_revision,adopted_at
) VALUES (
  1,'FEE_AWARE_INVENTORY_C2_2','live','local-live',1,'ACTIVE',
  '{GIT_REVISION}','2026-07-31T18:00:00Z'
);
CREATE TABLE binance_order_fills(
  id BIGINT PRIMARY KEY,
  source TEXT NOT NULL,
  order_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  executed_qty NUMERIC NOT NULL,
  avg_price NUMERIC NOT NULL,
  quote_notional_usdc NUMERIC NOT NULL,
  commission_amount NUMERIC NOT NULL,
  commission_asset TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  trade_id BIGINT
);
CREATE TABLE exchange_fill_ingestion_state_v2(
  ingestion_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  symbol TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  local_fill_id BIGINT,
  adoption_id BIGINT,
  contract_generation BIGINT,
  source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT,
  applied_at TIMESTAMPTZ,
  application_status TEXT NOT NULL,
  authoritative_payload JSONB NOT NULL
);
CREATE TABLE positions(
  id BIGINT PRIMARY KEY,
  entry_order_id TEXT
);
CREATE TABLE binance_orders(
  id BIGINT PRIMARY KEY,
  exchange_source TEXT,
  order_id TEXT,
  client_order_id TEXT,
  symbol TEXT,
  side TEXT,
  strategy TEXT,
  "interval" TEXT,
  order_purpose TEXT,
  position_id BIGINT,
  reconciled_position_id BIGINT,
  is_exit BOOLEAN
);
"""

INTENT_AND_SUBMISSION = f"""
INSERT INTO live_entry_intents_v1(
  intent_id,environment,deployment_id,git_revision,adoption_id,generation,
  decision_id,symbol,strategy,"interval",slot_identity,exchange_source,
  client_order_id,order_purpose,side,requested_qty,content_fingerprint,
  prepared_at,producer_identity,contract_version
) VALUES (
  '{INTENT_ID}','live','local-live','{GIT_REVISION}',1,1,
  '44444444-4444-5444-8444-444444444444','BNBUSDC','TREND','1m',
  'BNBUSDC:TREND:1m','okx','{CLIENT_ORDER_ID}','ENTRY','BUY',0.02,
  '{'c' * 64}','2026-07-31T19:00:00Z','postgres-contract-test',
  'LIVE_ENTRY_INTENT_V1'
);
INSERT INTO live_entry_submissions_v1(
  submission_attempt_id,intent_id,environment,deployment_id,adoption_id,
  generation,git_revision,client_order_id,exchange_source,symbol,strategy,
  "interval",order_purpose,side,requested_qty,attempt_ordinal,
  submission_fingerprint,submitted_at,producer_identity,contract_version
) VALUES (
  '{SUBMISSION_ID}','{INTENT_ID}','live','local-live',1,1,
  '{GIT_REVISION}','{CLIENT_ORDER_ID}','okx','BNBUSDC','TREND','1m',
  'ENTRY','BUY',0.02,1,'{'d' * 64}','2026-07-31T19:00:01Z',
  'postgres-contract-test','LIVE_ENTRY_SUBMISSION_V1'
);
"""

ACK_LINEAGE = f"""
INSERT INTO live_entry_order_acks_v1(
  ack_id,submission_attempt_id,intent_id,environment,deployment_id,
  adoption_id,generation,git_revision,client_order_id,exchange_source,
  exchange_order_id,exchange_order_status,symbol,strategy,"interval",
  order_purpose,side,requested_qty,ack_fingerprint,acknowledged_at,
  recovered_by_client_order_id,producer_identity,contract_version
) VALUES (
  '{ACK_ID}','{SUBMISSION_ID}','{INTENT_ID}','live','local-live',1,1,
  '{GIT_REVISION}','{CLIENT_ORDER_ID}','okx','okx-order-lei1c','LIVE',
  'BNBUSDC','TREND','1m','ENTRY','BUY',0.02,'{'e' * 64}',
  '2026-07-31T19:00:02Z',false,'postgres-contract-test',
  'LIVE_ENTRY_ORDER_ACK_V1'
);
"""

LINEAGE = INTENT_AND_SUBMISSION + ACK_LINEAGE


def _create_database(disposable_postgres_v16, purpose):
    name = f"waltrade_baseline_test_lei1c_{purpose}_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(name)
    return name


def _apply(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _install(disposable_postgres_v16, purpose, *, lineage=True, ack=True):
    name = _create_database(disposable_postgres_v16, purpose)
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn, BASE_SCHEMA)
        _apply(conn, LEI1A)
        _apply(conn, LEI1B)
        if lineage:
            _apply(conn, INTENT_AND_SUBMISSION)
            if ack:
                _apply(conn, ACK_LINEAGE)
        _apply(conn, FORWARD)
    finally:
        conn.close()
    return name, lambda: disposable_postgres_v16.connect(name)


def _observation(**changes):
    values = {
        "environment": "live",
        "deployment_id": "local-live",
        "adoption_id": 1,
        "generation": 1,
        "git_revision": GIT_REVISION,
        "exchange_source": "okx",
        "exchange_trade_id": TRADE_ID,
        "exchange_order_id": "okx-order-lei1c",
        "client_order_id": CLIENT_ORDER_ID,
        "symbol": "BNBUSDC",
        "side": "BUY",
        "executed_qty": Decimal("0.01"),
        "price": Decimal("300"),
        "notional": None,
        "fee": Decimal("0.001"),
        "fee_asset": "USDC",
        "executed_at": EXECUTED_AT,
        "observed_at": OBSERVED_AT,
        "producer_identity": "postgres-contract-test",
        "source_payload": {"tradeId": TRADE_ID},
    }
    values.update(changes)
    return EntryFillObservation.build(**values)


def _process(repository, observation=None, *, clock_at=DECIDED_AT):
    return process_entry_fill_attribution(
        mode=EntryFillAttributionMode.SHADOW,
        observation=observation or _observation(),
        repository=repository,
        clock=lambda: clock_at,
    )


def _counts(factory):
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM live_entry_fill_evidence_v1),"
                "(SELECT count(*) FROM live_entry_fill_applications_v1),"
                "(SELECT count(*) FROM positions)"
            )
            return cur.fetchone()
    finally:
        conn.rollback()
        conn.close()

def _seed_canonical_local_fill(
    factory, *, source_fingerprint, fill_id=10
):
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO binance_order_fills(
                  id,source,order_id,symbol,side,executed_qty,avg_price,
                  quote_notional_usdc,commission_amount,commission_asset,
                  event_time,trade_id
                ) VALUES (%s,'okx','okx-order-lei1c','BNBUSDC','BUY',
                          0.01,300,3,0.001,'USDC',%s,%s)
                """,
                (fill_id, EXECUTED_AT, int(TRADE_ID)),
            )
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,symbol,trade_id,order_id,local_fill_id,adoption_id,
                  contract_generation,source_fingerprint,applied_fingerprint,
                  applied_at,application_status,authoritative_payload
                ) VALUES (
                  'okx','BNBUSDC',%s,'okx-order-lei1c',%s,1,1,%s,%s,
                  '2026-07-31T19:00:04Z','APPLIED',%s::jsonb
                )
                """,
                (
                    TRADE_ID,
                    fill_id,
                    source_fingerprint,
                    source_fingerprint,
                    json.dumps(
                        {
                            "exchange": "okx",
                            "account_identity": "okx:postgres-gate",
                            "instrument": "BNBUSDC",
                            "trade_id": TRADE_ID,
                            "order_id": "okx-order-lei1c",
                            "side": "BUY",
                            "executed_qty": "0.01",
                            "fill_price": "300",
                            "fee_quantity": "0.001",
                            "fee_currency": "USDC",
                            "event_time_ms": int(
                                EXECUTED_AT.timestamp() * 1000
                            ),
                        },
                        sort_keys=True,
                    ),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _evidence_values(**changes):
    values = {
        "fill_evidence_id": "55555555-5555-5555-8555-555555555555",
        "exchange_trade_id": TRADE_ID,
        "exchange_order_id": "okx-order-lei1c",
        "client_order_id": CLIENT_ORDER_ID,
        "wire_client_order_id": WIRE_CLIENT_ORDER_ID,
        "intent_id": INTENT_ID,
        "submission_attempt_id": SUBMISSION_ID,
        "ack_id": ACK_ID,
        "linked_position_id": None,
        "attribution_status": "BOT_OWNED_MISSING_POSITION",
        "attribution_fingerprint": ATTRIBUTION_FINGERPRINT,
        "symbol": "BNBUSDC",
        "strategy": "TREND",
        "interval": "1m",
        "order_purpose": "ENTRY",
        "side": "BUY",
        "executed_qty": "0.01",
        "price": "300",
        "notional": "3",
        "fee": "0.001",
        "fee_asset": "USDC",
        "source_fingerprint": SOURCE_FINGERPRINT,
        "source_payload": json.dumps({"tradeId": TRADE_ID}),
    }
    values.update(changes)
    return values


def _insert_evidence(conn, **changes):
    values = _evidence_values(**changes)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO live_entry_fill_evidence_v1(
              fill_evidence_id,environment,deployment_id,adoption_id,
              generation,git_revision,exchange_source,exchange_trade_id,
              exchange_order_id,client_order_id,wire_client_order_id,intent_id,
              submission_attempt_id,ack_id,linked_position_id,
              attribution_status,
              attribution_fingerprint,symbol,strategy,"interval",order_purpose,
              side,executed_qty,price,notional,fee,fee_asset,executed_at,
              source_fingerprint,source_payload,observed_at,producer_identity,
              contract_version
            ) VALUES (
              %(fill_evidence_id)s,'live','local-live',1,1,%(git_revision)s,
              'okx',%(exchange_trade_id)s,%(exchange_order_id)s,
              %(client_order_id)s,%(wire_client_order_id)s,%(intent_id)s,
              %(submission_attempt_id)s,%(ack_id)s,%(linked_position_id)s,
              %(attribution_status)s,
              %(attribution_fingerprint)s,%(symbol)s,%(strategy)s,%(interval)s,
              %(order_purpose)s,%(side)s,%(executed_qty)s,%(price)s,%(notional)s,
              %(fee)s,%(fee_asset)s,'2026-07-31T19:00:03Z',
              %(source_fingerprint)s,%(source_payload)s::jsonb,
              '2026-07-31T19:00:04Z','postgres-contract-test',
              'LIVE_ENTRY_FILL_EVIDENCE_V1'
            )
            """,
            {**values, "git_revision": GIT_REVISION},
        )
    conn.commit()
    return values


def _insert_application(conn, **changes):
    values = {
        "application_decision_id": "66666666-6666-5666-8666-666666666666",
        "fill_evidence_id": "55555555-5555-5555-8555-555555555555",
        "client_order_id": CLIENT_ORDER_ID,
        "intent_id": INTENT_ID,
        "submission_attempt_id": SUBMISSION_ID,
        "ack_id": ACK_ID,
        "strategy": "TREND",
        "interval": "1m",
        "order_purpose": "ENTRY",
        "local_fill_id": None,
        "linked_position_id": None,
        "attribution_status": "BOT_OWNED_MISSING_POSITION",
        "attribution_fingerprint": ATTRIBUTION_FINGERPRINT,
        "application_status": "OBSERVED_NOT_APPLIED",
        "application_target_identity": None,
        "canonical_source_fingerprint": SOURCE_FINGERPRINT,
        "observed_source_fingerprint": SOURCE_FINGERPRINT,
        "applied_fingerprint": None,
        "applied_at": None,
        "decision_fingerprint": "1" * 64,
        "decision_payload": json.dumps({"outcome": "OBSERVED_NOT_APPLIED"}),
    }
    values.update(changes)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO live_entry_fill_applications_v1(
              application_decision_id,fill_evidence_id,environment,
              deployment_id,adoption_id,generation,git_revision,
              exchange_source,client_order_id,intent_id,
              submission_attempt_id,ack_id,strategy,"interval",order_purpose,
              local_fill_id,linked_position_id,attribution_status,
              attribution_fingerprint,application_status,
              application_target_identity,canonical_source_fingerprint,
              observed_source_fingerprint,applied_fingerprint,applied_at,
              decision_fingerprint,decision_payload,decided_at,
              producer_identity,contract_version
            ) VALUES (
              %(application_decision_id)s,%(fill_evidence_id)s,'live',
              'local-live',1,1,%(git_revision)s,'okx',%(client_order_id)s,
              %(intent_id)s,%(submission_attempt_id)s,%(ack_id)s,%(strategy)s,
              %(interval)s,%(order_purpose)s,%(local_fill_id)s,
              %(linked_position_id)s,%(attribution_status)s,
              %(attribution_fingerprint)s,%(application_status)s,
              %(application_target_identity)s,%(canonical_source_fingerprint)s,
              %(observed_source_fingerprint)s,%(applied_fingerprint)s,
              %(applied_at)s,%(decision_fingerprint)s,
              %(decision_payload)s::jsonb,'2026-07-31T19:00:05Z',
              'postgres-contract-test','LIVE_ENTRY_FILL_APPLICATION_V1'
            )
            """,
            {**values, "git_revision": GIT_REVISION},
        )
    conn.commit()
    return values


def test_gate_a_migration_twice_manifest_and_zero_backfill(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "migration")
    conn = factory()
    try:
        _apply(conn, FORWARD)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM live_entry_fill_evidence_v1),"
                "(SELECT count(*) FROM live_entry_fill_applications_v1)"
            )
            assert cur.fetchone() == (0, 0)
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
                "AND tablename IN ('live_entry_fill_evidence_v1',"
                "'live_entry_fill_applications_v1')"
            )
            assert set(MANIFEST["indexes"]).issubset(
                {row[0] for row in cur.fetchall()}
            )
            cur.execute(
                "SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND "
                "tgrelid IN ('live_entry_fill_evidence_v1'::regclass,"
                "'live_entry_fill_applications_v1'::regclass)"
            )
            assert set(MANIFEST["triggers"]) == {
                row[0] for row in cur.fetchall()
            }
            cur.execute(
                "SELECT count(*),min(checksum_sha256),"
                "min(schema_baseline_version) "
                "FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (MANIFEST["migration_id"],),
            )
            assert cur.fetchone() == (
                1,
                MANIFEST["ledger_checksum_sha256"],
                MANIFEST["contract_version"],
            )
    finally:
        conn.rollback()
        conn.close()

    assert MANIFEST["data_policy"] == {
        "backfill": False,
        "runtime_writer_activation": False,
        "append_only": True,
        "position_projection": False,
        "rollback_blocked_after_evidence": True,
    }
    assert MANIFEST["runtime_scope"] == {
        "current_readiness": "LIVE_ONLY",
        "paper_mode": "OFF",
        "paper_schema_apply": False,
        "schema_apply_guard": (
            "reject-paper-database-or-paper-only-active-adoption"
        ),
        "paper_shadow_prerequisite": "simulated-fill-and-lineage-adapter",
    }
    assert (
        MANIFEST["prerequisites"]
        == MANIFEST["ledger_contract"]["runtime_dependencies"]
    )
    assert LEI1C_MIGRATION_ID == MANIFEST["migration_id"]
    assert LEI1C_LEDGER_CHECKSUM == MANIFEST["ledger_checksum_sha256"]
    assert MANIFEST["migration_sha256"] == hashlib.sha256(
        FORWARD_PATH.read_bytes()
    ).hexdigest()
    assert MANIFEST["rollback_sha256"] == hashlib.sha256(
        ROLLBACK_PATH.read_bytes()
    ).hexdigest()
    contract_bytes = json.dumps(
        MANIFEST["ledger_contract"],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    assert (
        MANIFEST["ledger_checksum_algorithm"]
        == "sha256(canonical-json:ledger_contract)"
    )
    assert MANIFEST["ledger_checksum_sha256"] == hashlib.sha256(
        contract_bytes
    ).hexdigest()

    conflict_name = _create_database(
        disposable_postgres_v16, "checksum_conflict"
    )
    conflict_conn = disposable_postgres_v16.connect(conflict_name)
    try:
        _apply(conflict_conn, BASE_SCHEMA)
        _apply(conflict_conn, LEI1A)
        _apply(conflict_conn, LEI1B)
        with conflict_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO schema_migration_ledger_v1(
                  migration_id,checksum_sha256,environment,deployment_id,
                  database_name,applied_by,status,success,
                  execution_duration_ms,git_sha,schema_baseline_version
                ) VALUES (%s,%s,'LIVE','checksum-conflict',current_database(),
                          'postgres-contract-test','APPLIED',true,0,%s,%s)
                """,
                (
                    MANIFEST["migration_id"],
                    "0" * 64,
                    GIT_REVISION,
                    MANIFEST["contract_version"],
                ),
            )
        conflict_conn.commit()
        with pytest.raises(Exception, match="MIGRATION_CHECKSUM_CONFLICT"):
            with conflict_conn.cursor() as cur:
                cur.execute(FORWARD)
        conflict_conn.rollback()
        with conflict_conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.live_entry_fill_evidence_v1')"
            )
            assert cur.fetchone()[0] is None
    finally:
        conflict_conn.rollback()
        conflict_conn.close()


def test_gate_a_live_only_migration_rejects_paper_scope(
    disposable_postgres_v16,
):
    name = _create_database(disposable_postgres_v16, "paper_scope_rejected")
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn, BASE_SCHEMA)
        _apply(conn, LEI1A)
        _apply(conn, LEI1B)
        with pytest.raises(Exception, match="LEI1C_RUNTIME_SCOPE_LIVE_ONLY"):
            with conn.cursor() as cur:
                cur.execute(FORWARD)
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.live_entry_fill_evidence_v1'),"
                "to_regclass('public.live_entry_fill_applications_v1')"
            )
            assert cur.fetchone() == (None, None)
            cur.execute(
                "SELECT count(*) FROM schema_migration_ledger_v1 "
                "WHERE migration_id=%s",
                (MANIFEST["migration_id"],),
            )
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()


def test_gate_b_exact_ack_attribution_with_existing_position(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "exact_attribution")
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(20,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  21,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',20,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    repository = EntryFillAttributionRepository(factory)
    resolution = repository.resolve_lineage(_observation())
    assert resolution.status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
    assert resolution.method == "EXACT_EXCHANGE_ORDER_ID"
    assert resolution.linked_position_id == 20

    result = _process(repository)
    assert result.attribution_status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
    assert result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert _counts(factory) == (1, 1, 1)


def test_gate_b_late_fill_keeps_exact_superseded_ack_generation(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "historical_ack_context")
    next_git = "1" * 40
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE runtime_contract_adoption_v2 "
                "SET status='SUPERSEDED',deactivated_at=clock_timestamp() "
                "WHERE adoption_id=1"
            )
            cur.execute(
                """
                INSERT INTO runtime_contract_adoption_v2(
                  adoption_id,contract_name,environment,deployment_id,
                  generation,status,git_revision,adopted_at
                ) VALUES (
                  2,'FEE_AWARE_INVENTORY_C2_2','live','local-live',2,
                  'ACTIVE',%s,clock_timestamp()
                )
                """,
                (next_git,),
            )
        conn.commit()
    finally:
        conn.close()

    repository = EntryFillAttributionRepository(factory)
    preliminary = _observation(
        adoption_id=2,
        generation=2,
        git_revision=next_git,
    )
    context = repository.resolve_observation_context(preliminary)

    assert (context.adoption_id, context.generation, context.git_revision) == (
        1,
        1,
        GIT_REVISION,
    )
    historical = _observation(
        adoption_id=context.adoption_id,
        generation=context.generation,
        git_revision=context.git_revision,
    )
    result = _process(repository, historical)

    assert result.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert (
        result.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert result.evidence is not None
    assert result.evidence.observation.adoption_id == 1
    assert result.evidence.observation.generation == 1
    assert result.evidence.observation.git_revision == GIT_REVISION


def test_gate_b_ambiguous_replay_retains_canonical_lineage_fail_closed(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "canonical_then_ambiguous")
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(20,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  21,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',20,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    repository = EntryFillAttributionRepository(factory)
    first = _process(repository)
    assert first.attribution_status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(30,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  31,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',30,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    replay = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 8, tzinfo=timezone.utc),
    )

    assert replay.outcome is EntryFillProcessingOutcome.AMBIGUOUS
    assert replay.application_status is FillApplicationStatus.AMBIGUOUS
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT intent_id,submission_attempt_id,ack_id,"
                "linked_position_id,attribution_status,decision_payload "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='AMBIGUOUS'"
            )
            intent_id, submission_id, ack_id, position_id, status, payload = (
                cur.fetchone()
            )
            assert str(intent_id) == INTENT_ID
            assert str(submission_id) == SUBMISSION_ID
            assert str(ack_id) == ACK_ID
            assert position_id == 20
            assert status == "AMBIGUOUS"
            assert payload["reason"]["observed_lineage"]["status"] == "AMBIGUOUS"
            assert payload["reason"]["observed_lineage"]["identity"] == {
                "ack_id": None,
                "client_order_id": None,
                "intent_id": None,
                "interval": None,
                "linked_position_id": None,
                "order_purpose": None,
                "strategy": None,
                "submission_attempt_id": None,
            }
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (1, 2, 2)


def test_gate_b_late_position_upgrade_then_ambiguity_keeps_upgrade(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "upgrade_then_ambiguous")
    repository = EntryFillAttributionRepository(factory)

    first = _process(repository)
    assert (
        first.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(20,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  21,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',20,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    upgraded = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 7, tzinfo=timezone.utc),
    )
    assert upgraded.attribution_status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(30,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  31,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',30,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    ambiguous = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 8, tzinfo=timezone.utc),
    )
    assert ambiguous.outcome is EntryFillProcessingOutcome.AMBIGUOUS

    evidence = repository.load_evidence(_observation().natural_key)
    assert evidence is not None
    assert evidence.lineage.linked_position_id is None
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT attribution_status,linked_position_id "
                "FROM live_entry_fill_applications_v1 "
                "ORDER BY decided_at,application_decision_id"
            )
            assert cur.fetchall() == [
                ("BOT_OWNED_MISSING_POSITION", None),
                ("BOT_OWNED_ATTRIBUTED", 20),
                ("AMBIGUOUS", 20),
            ]
    finally:
        conn.rollback()
        conn.close()

    invalid_conn = factory()
    try:
        with pytest.raises(Exception, match="APPLICATION_LINEAGE_REGRESSION"):
            _insert_application(
                invalid_conn,
                application_decision_id=(
                    "78787878-7878-5878-8878-787878787878"
                ),
                fill_evidence_id=str(evidence.fill_evidence_id),
                linked_position_id=None,
                attribution_status="AMBIGUOUS",
                application_status="AMBIGUOUS",
                canonical_source_fingerprint=evidence.source_fingerprint,
                observed_source_fingerprint=evidence.source_fingerprint,
                decision_fingerprint="8" * 64,
                decision_payload=json.dumps({"invalid": "drops-position"}),
            )
        invalid_conn.rollback()
    finally:
        invalid_conn.close()
    assert _counts(factory) == (1, 3, 2)


def test_gate_b_direct_evidence_rejects_position_identity_or_uniqueness_gap(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "position_guard")
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(20,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  21,'okx','okx-order-lei1c','mismatched-cid','BNBUSDC',
                  'BUY','TREND','1m','ENTRY',20,NULL,false
                )
                """
            )
        conn.commit()

        with pytest.raises(
            Exception, match="FILL_POSITION_ATTRIBUTION_MISMATCH"
        ):
            _insert_evidence(
                conn,
                linked_position_id=20,
                attribution_status="BOT_OWNED_ATTRIBUTED",
            )
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE binance_orders SET client_order_id=%s WHERE id=21",
                (CLIENT_ORDER_ID,),
            )
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(30,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  31,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                  '1m','ENTRY',30,NULL,false
                )
                """,
                (CLIENT_ORDER_ID,),
            )
        conn.commit()

        with pytest.raises(
            Exception, match="FILL_POSITION_ATTRIBUTION_MISMATCH"
        ):
            _insert_evidence(
                conn,
                linked_position_id=20,
                attribution_status="BOT_OWNED_ATTRIBUTED",
            )
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM live_entry_fill_evidence_v1")
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.parametrize(
    ("local_order_id", "local_client_order_id"),
    (
        ("okx-order-lei1c", None),
        (None, CLIENT_ORDER_ID),
    ),
)
def test_gate_b_position_link_allows_one_exact_identity_with_other_unknown(
    disposable_postgres_v16,
    local_order_id,
    local_client_order_id,
):
    _, factory = _install(disposable_postgres_v16, "position_null_tolerant")
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO positions(id) VALUES (20)")
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES (
                  21,'okx',%s,%s,'BNBUSDC','BUY','TREND','1m','ENTRY',
                  20,NULL,false
                )
                """,
                (local_order_id, local_client_order_id),
            )
        conn.commit()

        _insert_evidence(
            conn,
            linked_position_id=20,
            attribution_status="BOT_OWNED_ATTRIBUTED",
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT linked_position_id FROM live_entry_fill_evidence_v1"
            )
            assert cur.fetchone()[0] == 20
    finally:
        conn.rollback()
        conn.close()


def test_gate_c_missing_position_is_valid_observed_not_applied(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "missing_position")
    repository = EntryFillAttributionRepository(factory)

    result = _process(repository)

    assert result.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert (
        result.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert _counts(factory) == (1, 1, 0)


def test_gate_c_late_recovered_ack_appends_stronger_attribution_decision(
    disposable_postgres_v16,
):
    _, factory = _install(
        disposable_postgres_v16, "late_recovered_ack", ack=False
    )
    repository = EntryFillAttributionRepository(factory)

    first = _process(repository)
    assert (
        first.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE
    )
    assert first.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED

    conn = factory()
    try:
        recovered_ack = ACK_LINEAGE.replace(
            "'2026-07-31T19:00:02Z',false,",
            "'2026-07-31T19:00:02Z',true,",
        )
        assert recovered_ack != ACK_LINEAGE
        _apply(conn, recovered_ack)
    finally:
        conn.close()

    replay = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 7, tzinfo=timezone.utc),
    )

    assert (
        replay.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert replay.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT attribution_status FROM live_entry_fill_evidence_v1"
            )
            assert cur.fetchone()[0] == "BOT_OWNED_MISSING_LINEAGE"
            cur.execute(
                "SELECT attribution_status,application_status "
                "FROM live_entry_fill_applications_v1"
            )
            assert set(cur.fetchall()) == {
                ("BOT_OWNED_MISSING_LINEAGE", "OBSERVED_NOT_APPLIED"),
                ("BOT_OWNED_MISSING_POSITION", "OBSERVED_NOT_APPLIED"),
            }
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (1, 2, 0)


def test_gate_c_external_then_late_ack_hard_replay_keeps_typed_identity(
    disposable_postgres_v16,
):
    _, factory = _install(
        disposable_postgres_v16, "external_late_ack_ambiguous", lineage=False
    )
    repository = EntryFillAttributionRepository(factory)

    first = _process(repository)
    assert (
        first.attribution_status
        is FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED
    )

    conn = factory()
    try:
        _apply(conn, LINEAGE)
    finally:
        conn.close()

    upgraded = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 7, tzinfo=timezone.utc),
    )
    assert (
        upgraded.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(id,entry_order_id) VALUES "
                "(20,'okx-order-lei1c'),(30,'okx-order-lei1c')"
            )
            cur.execute(
                """
                INSERT INTO binance_orders(
                  id,exchange_source,order_id,client_order_id,symbol,side,
                  strategy,"interval",order_purpose,position_id,
                  reconciled_position_id,is_exit
                ) VALUES
                  (21,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                   '1m','ENTRY',20,NULL,false),
                  (31,'okx','okx-order-lei1c',%s,'BNBUSDC','BUY','TREND',
                   '1m','ENTRY',30,NULL,false)
                """,
                (CLIENT_ORDER_ID, CLIENT_ORDER_ID),
            )
        conn.commit()
    finally:
        conn.close()

    hard = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 8, tzinfo=timezone.utc),
    )
    assert hard.outcome is EntryFillProcessingOutcome.AMBIGUOUS

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT client_order_id,strategy,\"interval\",order_purpose "
                "FROM live_entry_fill_evidence_v1"
            )
            assert cur.fetchone() == (None, None, None, None)
            cur.execute(
                "SELECT client_order_id,strategy,\"interval\",order_purpose,"
                "intent_id,submission_attempt_id,ack_id "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='AMBIGUOUS'"
            )
            row = cur.fetchone()
            assert row[:4] == (CLIENT_ORDER_ID, "TREND", "1m", "ENTRY")
            assert tuple(str(value) for value in row[4:]) == (
                INTENT_ID,
                SUBMISSION_ID,
                ACK_ID,
            )
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (1, 3, 2)


def test_gate_d_duplicate_without_application_proof_stays_not_applied(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "unapplied_duplicate")
    repository = EntryFillAttributionRepository(factory)

    first = _process(repository)
    second = _process(repository)

    assert first.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert second.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert first.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert second.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert _counts(factory) == (1, 1, 0)


def test_gate_e_complete_canonical_proof_is_true_duplicate_applied(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "true_duplicate")
    repository = EntryFillAttributionRepository(factory)
    first = _process(repository)
    assert first.evidence is not None
    _seed_canonical_local_fill(
        # C2.2 and LEI1C intentionally use different canonical payloads and
        # therefore need not share a fingerprint value.
        factory, source_fingerprint="9" * 64
    )

    invalid_conn = factory()
    try:
        with pytest.raises(Exception):
            _insert_application(
                invalid_conn,
                application_decision_id=(
                    "12121212-1212-5212-8212-121212121212"
                ),
                fill_evidence_id=str(first.evidence.fill_evidence_id),
                local_fill_id=10,
                attribution_fingerprint=(
                    first.evidence.attribution_fingerprint
                ),
                application_status="APPLIED",
                application_target_identity="not-the-canonical-fill:10",
                canonical_source_fingerprint=first.evidence.source_fingerprint,
                observed_source_fingerprint=first.evidence.source_fingerprint,
                applied_fingerprint=first.evidence.source_fingerprint,
                applied_at="2026-07-31T19:00:06Z",
                decision_fingerprint="2" * 64,
                decision_payload=json.dumps({"invalid": "target-identity"}),
            )
        invalid_conn.rollback()
        with pytest.raises(Exception):
            _insert_application(
                invalid_conn,
                application_decision_id=(
                    "13131313-1313-5313-8313-131313131313"
                ),
                fill_evidence_id=str(first.evidence.fill_evidence_id),
                local_fill_id=10,
                attribution_fingerprint=(
                    first.evidence.attribution_fingerprint
                ),
                application_status="APPLIED",
                application_target_identity="binance_order_fills:10",
                canonical_source_fingerprint=first.evidence.source_fingerprint,
                observed_source_fingerprint=first.evidence.source_fingerprint,
                applied_fingerprint=None,
                applied_at="2026-07-31T19:00:06Z",
                decision_fingerprint="3" * 64,
                decision_payload=json.dumps(
                    {"invalid": "null-applied-fingerprint"}
                ),
            )
        invalid_conn.rollback()
    finally:
        invalid_conn.close()

    replay = _process(repository)

    assert replay.outcome is EntryFillProcessingOutcome.TRUE_DUPLICATE_APPLIED
    assert (
        replay.application_status
        is FillApplicationStatus.TRUE_DUPLICATE_APPLIED
    )
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT application_status,local_fill_id,"
                "application_target_identity,applied_fingerprint,"
                "applied_at IS NOT NULL,decision_payload "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='APPLIED'"
            )
            status, fill_id, target, fingerprint, has_time, payload = (
                cur.fetchone()
            )
            assert (status, fill_id, target, fingerprint, has_time) == (
                "APPLIED",
                10,
                "binance_order_fills:10",
                first.evidence.source_fingerprint,
                True,
            )
            assert (
                payload["reason"]["decision_kind"]
                == "EXISTING_LOCAL_APPLICATION_PROOF"
            )
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (1, 2, 0)


def test_gate_e_bridge_revalidates_mutable_ingestion_application_proof(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "bridge_proof_drift")
    repository = EntryFillAttributionRepository(factory)
    first = _process(repository)
    assert first.evidence is not None
    _seed_canonical_local_fill(factory, source_fingerprint="9" * 64)

    bridged = _process(repository)
    assert (
        bridged.application_status
        is FillApplicationStatus.TRUE_DUPLICATE_APPLIED
    )

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE exchange_fill_ingestion_state_v2
                SET application_status='CORRECTION_PENDING',
                    applied_fingerprint=NULL,applied_at=NULL
                WHERE trade_id=%s
                """,
                (TRADE_ID,),
            )
        conn.commit()
    finally:
        conn.close()

    replay = _process(
        repository,
        clock_at=datetime(2026, 7, 31, 19, 0, 9, tzinfo=timezone.utc),
    )

    assert replay.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert replay.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert replay.attribution_status is FillAttributionStatus.CONFLICTED


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("executed_qty", Decimal("0.011")),
        ("price", Decimal("301")),
        ("fee", Decimal("0.002")),
        ("exchange_order_id", "different-order-id"),
        ("client_order_id", "ORC-L-BNBUSDC-TREN-1m-E-other"),
    ),
)
def test_gate_f_changed_source_semantics_preserve_canonical_and_record_conflict(
    disposable_postgres_v16,
    field,
    value,
):
    _, factory = _install(
        disposable_postgres_v16, f"conflict_{field}"
    )
    repository = EntryFillAttributionRepository(factory)
    assert _process(repository).outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED

    changed = _observation(
        **{
            field: value,
            "source_payload": {"tradeId": TRADE_ID, "changed": field},
        }
    )
    conflict = _process(repository, changed)

    assert conflict.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert conflict.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT executed_qty::text,price::text,fee::text,"
                "exchange_order_id,wire_client_order_id "
                "FROM live_entry_fill_evidence_v1"
            )
            assert cur.fetchone() == (
                "0.01",
                "300",
                "0.001",
                "okx-order-lei1c",
                WIRE_CLIENT_ORDER_ID,
            )
            cur.execute(
                "SELECT canonical_source_fingerprint,"
                "observed_source_fingerprint,decision_payload "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='IDEMPOTENCY_CONFLICT'"
            )
            canonical, observed, payload = cur.fetchone()
            assert observed != canonical
            assert (
                payload["reason"]["observed_semantic_payload"][field]
                == changed.semantic_payload[field]
            )
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (1, 2, 0)


@pytest.mark.parametrize(("side", "trade_id"), (("BUY", "8001"), ("SELL", "8002")))
def test_gate_g_external_manual_buy_and_sell_remain_unlinked(
    disposable_postgres_v16,
    side,
    trade_id,
):
    _, factory = _install(
        disposable_postgres_v16, f"external_{side.lower()}"
    )
    repository = EntryFillAttributionRepository(factory)
    observation = _observation(
        exchange_trade_id=trade_id,
        exchange_order_id=f"manual-order-{trade_id}",
        client_order_id=None,
        side=side,
        source_payload={"tradeId": trade_id, "origin": "manual"},
    )

    result = _process(repository, observation)

    assert result.outcome is EntryFillProcessingOutcome.EXTERNAL_UNLINKED
    assert (
        result.attribution_status
        is FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED
    )
    assert (
        result.application_status
        is FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
    )
    assert _counts(factory) == (1, 1, 0)


def test_gate_h_two_sessions_same_fill_converge_on_one_evidence_identity(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "concurrency")
    repository = EntryFillAttributionRepository(factory)
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait(timeout=5)
        return _process(repository)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result(timeout=10) for future in (
            pool.submit(worker),
            pool.submit(worker),
        )]

    assert {result.outcome for result in results} == {
        EntryFillProcessingOutcome.EVIDENCE_RECORDED,
        EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT,
    }
    assert {
        result.application_status for result in results
    } == {FillApplicationStatus.OBSERVED_NOT_APPLIED}
    assert _counts(factory) == (1, 1, 0)


def test_gate_h_concurrent_conflicting_payload_is_preserved_fail_closed(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "concurrent_conflict")
    repository = EntryFillAttributionRepository(factory)
    canonical_candidate = _observation()
    changed_candidate = _observation(
        executed_qty=Decimal("0.011"),
        source_payload={"tradeId": TRADE_ID, "changed": "executed_qty"},
    )
    barrier = threading.Barrier(2)

    def worker(observation):
        barrier.wait(timeout=5)
        return _process(repository, observation)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result(timeout=10) for future in (
            pool.submit(worker, canonical_candidate),
            pool.submit(worker, changed_candidate),
        )]

    assert all(result.error_code is None for result in results)
    assert any(
        result.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
        for result in results
    )
    stored = repository.load_evidence(canonical_candidate.natural_key)
    assert stored is not None
    conflicting = (
        changed_candidate
        if stored.source_fingerprint == canonical_candidate.source_fingerprint
        else canonical_candidate
    )
    replay = _process(
        repository,
        conflicting,
        clock_at=datetime(2026, 7, 31, 19, 0, 8, tzinfo=timezone.utc),
    )
    assert replay.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM live_entry_fill_evidence_v1")
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*),count(DISTINCT observed_source_fingerprint) "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='IDEMPOTENCY_CONFLICT'"
            )
            assert cur.fetchone() == (1, 1)
    finally:
        conn.rollback()
        conn.close()


def test_gate_h_each_distinct_conflict_has_one_deterministic_decision(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "distinct_conflicts")
    repository = EntryFillAttributionRepository(factory)
    canonical = _observation()
    changed_qty = _observation(
        executed_qty=Decimal("0.011"),
        source_payload={"tradeId": TRADE_ID, "changed": "executed_qty"},
    )
    changed_price = _observation(
        price=Decimal("301"),
        source_payload={"tradeId": TRADE_ID, "changed": "price"},
    )

    assert _process(repository, canonical).error_code is None
    for offset, observation in enumerate(
        (changed_qty, changed_price, changed_qty), start=6
    ):
        result = _process(
            repository,
            observation,
            clock_at=datetime(
                2026, 7, 31, 19, 0, offset, tzinfo=timezone.utc
            ),
        )
        assert (
            result.application_status
            is FillApplicationStatus.IDEMPOTENCY_CONFLICT
        )

    stored = repository.load_evidence(canonical.natural_key)
    assert stored is not None
    blocked_conn = factory()
    try:
        with pytest.raises(Exception, match="UNRESOLVED_HARD_STATE"):
            _insert_application(
                blocked_conn,
                application_decision_id=(
                    "abababab-abab-5bab-8bab-abababababab"
                ),
                fill_evidence_id=str(stored.fill_evidence_id),
                attribution_fingerprint=stored.attribution_fingerprint,
                canonical_source_fingerprint=stored.source_fingerprint,
                observed_source_fingerprint=stored.source_fingerprint,
                decision_fingerprint="f" * 64,
                decision_payload=json.dumps({"forbidden": "benign-after-hard"}),
            )
        blocked_conn.rollback()
    finally:
        blocked_conn.close()

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*),count(DISTINCT decision_fingerprint),"
                "count(DISTINCT observed_source_fingerprint) "
                "FROM live_entry_fill_applications_v1 "
                "WHERE application_status='IDEMPOTENCY_CONFLICT'"
            )
            assert cur.fetchone() == (2, 2, 2)
            cur.execute("SELECT count(*) FROM live_entry_fill_evidence_v1")
            assert cur.fetchone()[0] == 1
    finally:
        conn.rollback()
        conn.close()


def test_gate_i_partial_fills_share_ack_but_keep_distinct_evidence(
    disposable_postgres_v16,
):
    _, factory = _install(disposable_postgres_v16, "partial_fills")
    repository = EntryFillAttributionRepository(factory)
    first = _observation(
        exchange_trade_id="7001",
        executed_qty=Decimal("0.004"),
        fee=Decimal("0.0004"),
        source_payload={"tradeId": "7001", "part": 1},
    )
    second = _observation(
        exchange_trade_id="7002",
        executed_qty=Decimal("0.006"),
        fee=Decimal("0.0006"),
        source_payload={"tradeId": "7002", "part": 2},
    )

    results = (_process(repository, first), _process(repository, second))

    assert all(
        result.attribution_status
        is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
        for result in results
    )
    assert all(
        result.application_status
        is FillApplicationStatus.OBSERVED_NOT_APPLIED
        for result in results
    )
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*),sum(executed_qty),sum(fee),"
                "count(DISTINCT ack_id) FROM live_entry_fill_evidence_v1"
            )
            assert cur.fetchone() == (
                2,
                Decimal("0.010"),
                Decimal("0.0010"),
                1,
            )
    finally:
        conn.rollback()
        conn.close()
    assert _counts(factory) == (2, 2, 0)


def test_gate_j_empty_rollback_passes_and_evidence_rollback_fails_closed(
    disposable_postgres_v16,
):
    name, factory = _install(disposable_postgres_v16, "rollback")
    conn = factory()
    try:
        with pytest.raises(Exception, match="ACK_ATTRIBUTION_MISMATCH"):
            _insert_evidence(conn, exchange_order_id="wrong-order")
        conn.rollback()
        _apply(conn, ROLLBACK)
        _apply(conn, ROLLBACK)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.live_entry_fill_evidence_v1'),"
                "to_regclass('public.live_entry_fill_applications_v1')"
            )
            assert cur.fetchone() == (None, None)
        _apply(conn, FORWARD)
        _insert_evidence(conn)
        with pytest.raises(Exception, match="immutable and append-only"):
            with conn.cursor() as cur:
                cur.execute("UPDATE live_entry_fill_evidence_v1 SET fee=0")
        conn.rollback()
        with pytest.raises(
            Exception,
            match="ROLLBACK_BLOCKED_IMMUTABLE_EVIDENCE_EXISTS",
        ):
            with conn.cursor() as cur:
                cur.execute(ROLLBACK)
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM live_entry_fill_evidence_v1")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()

    # The failed rollback leaves all evidence in the same disposable database.
    verify = disposable_postgres_v16.connect(name)
    try:
        with verify.cursor() as cur:
            cur.execute("SELECT count(*) FROM live_entry_fill_evidence_v1")
            assert cur.fetchone()[0] == 1
    finally:
        verify.rollback()
        verify.close()

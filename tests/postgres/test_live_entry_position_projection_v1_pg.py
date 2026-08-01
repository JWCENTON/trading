from __future__ import annotations

import json
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from common.entry_fill_attribution import (
    EntryFillAttributionMode,
    EntryFillAttributionRepository,
    EntryFillObservation,
    FillApplicationStatus,
    FillAttributionStatus,
    process_entry_fill_attribution,
)
from common.entry_position_projection import (
    EntryProjectionOutcome,
    project_entry_intent,
)


ROOT = Path(__file__).resolve().parents[2]
LEI1A = (ROOT / "db/migrations/20260730_live_entry_intent_ledger_v1.sql").read_text()
LEI1B = (ROOT / "db/migrations/20260731_live_entry_submission_ack_v1.sql").read_text()
LEI1C = (ROOT / "db/migrations/20260731_live_entry_fill_attribution_v1.sql").read_text()
LEI1D_PATH = ROOT / "db/migrations/20260801_live_entry_position_projection_v1.sql"
LEI1D = LEI1D_PATH.read_text()
ROLLBACK = (ROOT / "db/migrations/20260801_live_entry_position_projection_v1_rollback.sql").read_text()
MANIFEST = json.loads((ROOT / "db/migrations/20260801_live_entry_position_projection_v1_manifest.json").read_text())

GIT = "a" * 40
INTENT = uuid.UUID("11111111-1111-5111-8111-111111111111")
SUBMISSION = uuid.UUID("22222222-2222-5222-8222-222222222222")
ACK = uuid.UUID("33333333-3333-5333-8333-333333333333")
CID = "ORC-L-BNBUSDC-TREN-1m-E-lei1d"
ORDER = "okx-order-lei1d"
START = datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc)


BASE = f"""
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGINT PRIMARY KEY,contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,deployment_id TEXT NOT NULL,generation BIGINT NOT NULL,
  status TEXT NOT NULL,git_revision TEXT NOT NULL,adopted_at TIMESTAMPTZ,
  deactivated_at TIMESTAMPTZ
);
CREATE TABLE schema_migration_ledger_v1 (
  ledger_id BIGSERIAL PRIMARY KEY,migration_id TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  environment TEXT NOT NULL,deployment_id TEXT NOT NULL,database_name TEXT NOT NULL,
  applied_by TEXT NOT NULL,status TEXT NOT NULL,success BOOLEAN NOT NULL,
  execution_duration_ms BIGINT NOT NULL,git_sha TEXT NOT NULL,error_summary TEXT,
  schema_baseline_version TEXT NOT NULL
);
INSERT INTO runtime_contract_adoption_v2 VALUES (
  1,'FEE_AWARE_INVENTORY_C2_2','live','local-live',1,'ACTIVE','{GIT}',
  '2026-08-01T07:00:00Z',NULL
);
CREATE TABLE positions (
  id BIGSERIAL PRIMARY KEY,symbol TEXT,strategy TEXT,"interval" TEXT,status TEXT,
  side TEXT,qty NUMERIC,entry_price NUMERIC,entry_time TIMESTAMPTZ,
  entry_order_id TEXT,entry_client_order_id TEXT,fees_usdc NUMERIC,
  inventory_evidence_status TEXT,gross_entry_executed_qty NUMERIC,
  entry_base_fee_qty NUMERIC,net_entry_inventory_qty NUMERIC,
  cumulative_exit_executed_qty NUMERIC,exit_inventory_reduction_qty NUMERIC,
  remaining_inventory_qty NUMERIC,inventory_calculated_at TIMESTAMPTZ,
  inventory_contract_adoption_id BIGINT REFERENCES runtime_contract_adoption_v2,
  inventory_contract_generation BIGINT
);
CREATE UNIQUE INDEX ux_positions_open_slot_test
  ON positions(symbol,strategy,"interval") WHERE status='OPEN';
CREATE TABLE binance_order_fills (
  id BIGINT PRIMARY KEY,source TEXT NOT NULL,order_id TEXT NOT NULL,symbol TEXT NOT NULL,
  side TEXT NOT NULL,executed_qty NUMERIC NOT NULL,avg_price NUMERIC NOT NULL,
  quote_notional_usdc NUMERIC NOT NULL,commission_amount NUMERIC NOT NULL,
  commission_asset TEXT,event_time TIMESTAMPTZ NOT NULL,trade_id BIGINT,
  commission_usdc NUMERIC
);
CREATE TABLE exchange_fill_ingestion_state_v2 (
  ingestion_id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,symbol TEXT NOT NULL,
  trade_id TEXT NOT NULL,order_id TEXT NOT NULL,local_fill_id BIGINT,adoption_id BIGINT,
  contract_generation BIGINT,source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT,applied_at TIMESTAMPTZ,application_status TEXT NOT NULL,
  authoritative_payload JSONB NOT NULL
);
CREATE TABLE binance_orders (
  id BIGSERIAL PRIMARY KEY,exchange_source TEXT,order_id TEXT,client_order_id TEXT,
  symbol TEXT,side TEXT,strategy TEXT,"interval" TEXT,order_purpose TEXT,
  position_id BIGINT,reconciled_position_id BIGINT,is_exit BOOLEAN,
  reconciliation_status TEXT,reconciled_at TIMESTAMPTZ,reconciled_fill_count INTEGER,
  reconciled_executed_qty NUMERIC,unreconciled_qty NUMERIC,
  reconciliation_error TEXT,last_reconciliation_action TEXT
);
CREATE TABLE position_lifecycle_events_c2_2 (
  event_id BIGSERIAL PRIMARY KEY,position_id BIGINT NOT NULL REFERENCES positions(id),
  order_id TEXT NOT NULL,mutation_kind TEXT NOT NULL CHECK (mutation_kind IN (
    'POSITION_REDUCED','POSITION_CLOSED','POSITION_CLOSED_TERMINAL_DUST')),
  mutation_high_water NUMERIC NOT NULL,payload JSONB NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),emitted_at TIMESTAMPTZ,
  UNIQUE(position_id,order_id,mutation_kind,mutation_high_water)
);
"""

LINEAGE = f"""
INSERT INTO live_entry_intents_v1(
  intent_id,environment,deployment_id,git_revision,adoption_id,generation,
  decision_id,symbol,strategy,"interval",slot_identity,exchange_source,
  client_order_id,order_purpose,side,requested_qty,content_fingerprint,
  prepared_at,producer_identity,contract_version
) VALUES (
  '{INTENT}','live','local-live','{GIT}',1,1,
  '44444444-4444-5444-8444-444444444444','BNBUSDC','TREND','1m',
  'BNBUSDC:TREND:1m','okx','{CID}','ENTRY','BUY',1,'{'c' * 64}',
  '{START.isoformat()}','lei1d-pg','LIVE_ENTRY_INTENT_V1'
);
INSERT INTO live_entry_submissions_v1(
  submission_attempt_id,intent_id,environment,deployment_id,adoption_id,generation,
  git_revision,client_order_id,exchange_source,symbol,strategy,"interval",
  order_purpose,side,requested_qty,attempt_ordinal,submission_fingerprint,
  submitted_at,producer_identity,contract_version
) VALUES (
  '{SUBMISSION}','{INTENT}','live','local-live',1,1,'{GIT}','{CID}','okx',
  'BNBUSDC','TREND','1m','ENTRY','BUY',1,1,'{'d' * 64}',
  '{(START + timedelta(seconds=1)).isoformat()}','lei1d-pg',
  'LIVE_ENTRY_SUBMISSION_V1'
);
INSERT INTO live_entry_order_acks_v1(
  ack_id,submission_attempt_id,intent_id,environment,deployment_id,adoption_id,
  generation,git_revision,client_order_id,exchange_source,exchange_order_id,
  exchange_order_status,symbol,strategy,"interval",order_purpose,side,
  requested_qty,ack_fingerprint,acknowledged_at,recovered_by_client_order_id,
  producer_identity,contract_version
) VALUES (
  '{ACK}','{SUBMISSION}','{INTENT}','live','local-live',1,1,'{GIT}','{CID}',
  'okx','{ORDER}','LIVE','BNBUSDC','TREND','1m','ENTRY','BUY',1,
  '{'e' * 64}','{(START + timedelta(seconds=2)).isoformat()}',false,
  'lei1d-pg','LIVE_ENTRY_ORDER_ACK_V1'
);
INSERT INTO binance_orders(
  exchange_source,order_id,client_order_id,symbol,side,strategy,"interval",
  order_purpose,is_exit,reconciled_executed_qty
) VALUES ('okx','{ORDER}','{CID}','BNBUSDC','BUY','TREND','1m','ENTRY',false,0);
"""


def _apply(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _install(disposable_postgres_v16, purpose):
    name = f"waltrade_baseline_test_lei1d_{purpose}_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(name)
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn, BASE)
        _apply(conn, LEI1A)
        _apply(conn, LEI1B)
        _apply(conn, LINEAGE)
        _apply(conn, LEI1C)
        _apply(conn, LEI1D)
    finally:
        conn.close()
    return name, lambda: disposable_postgres_v16.connect(name)


def _seed_fill(factory, *, fill_id, trade_id, qty, price, fee, fee_asset, at):
    observation = EntryFillObservation.build(
        environment="live",deployment_id="local-live",adoption_id=1,generation=1,
        git_revision=GIT,exchange_source="okx",exchange_trade_id=str(trade_id),
        exchange_order_id=ORDER,client_order_id=CID,symbol="BNBUSDC",side="BUY",
        executed_qty=qty,price=price,fee=fee,fee_asset=fee_asset,
        executed_at=at,observed_at=at + timedelta(milliseconds=1),
        producer_identity="lei1d-pg",source_payload={"trade_id": str(trade_id)},
    )
    fee_usdc = (
        Decimal(str(fee)) * Decimal(str(price))
        if fee_asset == "BNB" else Decimal(str(fee))
    )
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO binance_order_fills(
                  id,source,order_id,symbol,side,executed_qty,avg_price,
                  quote_notional_usdc,commission_amount,commission_asset,
                  event_time,trade_id,commission_usdc
                ) VALUES (%s,'okx',%s,'BNBUSDC','BUY',%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (fill_id, ORDER, qty, price, Decimal(str(qty))*Decimal(str(price)),
                 fee, fee_asset, at, trade_id, fee_usdc),
            )
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,symbol,trade_id,order_id,local_fill_id,adoption_id,
                  contract_generation,source_fingerprint,applied_fingerprint,
                  applied_at,application_status,authoritative_payload
                ) VALUES ('okx','BNBUSDC',%s,%s,%s,1,1,%s,%s,%s,'APPLIED',%s::jsonb)
                """,
                (str(trade_id), ORDER, fill_id, observation.source_fingerprint,
                 observation.source_fingerprint, at,
                 json.dumps({
                     "exchange": "okx",
                     "account_identity": "okx:lei1d-pg",
                     "instrument": "BNBUSDC",
                     "trade_id": str(trade_id),
                     "order_id": ORDER,
                     "side": "BUY",
                     "executed_qty": str(qty),
                     "fill_price": str(price),
                     "fee_quantity": str(fee),
                     "fee_currency": fee_asset,
                     "event_time_ms": int(at.timestamp() * 1000),
                 }, sort_keys=True)),
            )
        conn.commit()
    finally:
        conn.close()
    repository = EntryFillAttributionRepository(factory)
    first_result = process_entry_fill_attribution(
        mode=EntryFillAttributionMode.ENFORCE,
        observation=observation,
        repository=repository,
    )
    result = first_result
    if result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED:
        result = process_entry_fill_attribution(
            mode=EntryFillAttributionMode.ENFORCE,
            observation=observation,
            repository=repository,
        )
    assert result.attribution_status in {
        FillAttributionStatus.BOT_OWNED_MISSING_POSITION,
        FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
    }
    assert result.application_status is FillApplicationStatus.TRUE_DUPLICATE_APPLIED


def _project(factory, *, commit=True):
    conn = factory()
    try:
        with conn.cursor() as cur:
            result = project_entry_intent(cur, INTENT)
        if commit:
            conn.commit()
        else:
            conn.rollback()
        return result
    finally:
        conn.close()


def _state(factory):
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id,p.qty,p.entry_price,p.gross_entry_executed_qty,
                       p.entry_base_fee_qty,p.net_entry_inventory_qty,
                       p.remaining_inventory_qty,p.entry_intent_id,
                       (SELECT count(*) FROM positions),
                       (SELECT count(*) FROM position_lifecycle_events_c2_2
                         WHERE mutation_kind='POSITION_OPENED'),
                       (SELECT count(*) FROM live_entry_position_projection_fills_v1)
                FROM positions p WHERE p.entry_intent_id=%s
                """,
                (str(INTENT),),
            )
            return cur.fetchone()
    finally:
        conn.rollback()
        conn.close()


def test_schema_manifest_idempotency_and_empty_rollback(disposable_postgres_v16):
    assert hashlib.sha256(LEI1D_PATH.read_bytes()).hexdigest() == MANIFEST["migration_sha256"]
    _name, factory = _install(disposable_postgres_v16, "schema_idempotent")
    conn = factory()
    try:
        _apply(conn, LEI1D)
        _apply(conn, ROLLBACK)
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.live_entry_position_projections_v1')")
            assert cur.fetchone()[0] is None
    finally:
        conn.close()


def test_intent_and_ack_without_eligible_fill_create_no_position(disposable_postgres_v16):
    _name, factory = _install(disposable_postgres_v16, "zero_fill")
    result = _project(factory)
    assert result.outcome is EntryProjectionOutcome.NO_ELIGIBLE_FILL
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM positions")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM position_lifecycle_events_c2_2")
            assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()


def test_partial_fills_weighted_price_fees_and_exactly_once_event(disposable_postgres_v16):
    _name, factory = _install(disposable_postgres_v16, "partial")
    fills = (
        (10, 7001, "0.3", "100", "0.003", "BNB"),
        (11, 7002, "0.2", "110", "0.1", "USDC"),
        (12, 7003, "0.5", "90", "0.005", "BNB"),
    )
    position_id = None
    for index, values in enumerate(fills):
        _seed_fill(factory, fill_id=values[0], trade_id=values[1], qty=values[2],
                   price=values[3], fee=values[4], fee_asset=values[5],
                   at=START + timedelta(minutes=index))
        result = _project(factory)
        position_id = position_id or result.position_id
        assert result.position_id == position_id
    state = _state(factory)
    assert state[1:7] == (
        Decimal("0.992"), Decimal("97"), Decimal("1.0"), Decimal("0.008"),
        Decimal("0.992"), Decimal("0.992"),
    )
    assert state[8:] == (1, 1, 3)
    replay = _project(factory)
    assert replay.outcome is EntryProjectionOutcome.NO_OP
    assert replay.newly_applied_entry_qty == 0


def test_crash_rollback_retry_and_concurrency_are_idempotent(disposable_postgres_v16):
    _name, factory = _install(disposable_postgres_v16, "retry_concurrency")
    _seed_fill(factory, fill_id=20, trade_id=7101, qty="0.4", price="100",
               fee="0.001", fee_asset="BNB", at=START)
    rolled_back = _project(factory, commit=False)
    assert rolled_back.outcome is EntryProjectionOutcome.POSITION_OPENED
    assert _state(factory) is None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _x: _project(factory), range(2)))
    assert sorted(result.outcome.value for result in results) == ["NO_OP", "POSITION_OPENED"]
    assert _state(factory)[8:] == (1, 1, 1)


def test_out_of_order_final_state_is_deterministic(disposable_postgres_v16):
    final_states = []
    fills = {
        1: (31, 7201, "0.3", "100", "0.003", "BNB"),
        2: (32, 7202, "0.2", "110", "0.1", "USDC"),
        3: (33, 7203, "0.5", "90", "0.005", "BNB"),
    }
    for label, order in (("ordered", (1, 2, 3)), ("reversed", (3, 1, 2))):
        _name, factory = _install(disposable_postgres_v16, label)
        for index in order:
            values = fills[index]
            _seed_fill(factory, fill_id=values[0], trade_id=values[1], qty=values[2],
                       price=values[3], fee=values[4], fee_asset=values[5],
                       at=START + timedelta(minutes=index))
            _project(factory)
        final_states.append(_state(factory)[1:7])
    assert final_states[0] == final_states[1]


def test_legacy_open_position_fails_closed_and_rollback_blocks_evidence(disposable_postgres_v16):
    _name, factory = _install(disposable_postgres_v16, "legacy_conflict")
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(symbol,strategy,\"interval\",status,side,qty,"
                "entry_price,entry_time) VALUES ('BNBUSDC','TREND','1m','OPEN',"
                "'LONG',1,100,now())"
            )
        conn.commit()
    finally:
        conn.close()
    _seed_fill(factory, fill_id=40, trade_id=7301, qty="0.2", price="100",
               fee="0.001", fee_asset="BNB", at=START)
    result = _project(factory)
    assert result.outcome is EntryProjectionOutcome.BLOCKED
    assert result.detail == "OPEN_POSITION_WITHOUT_IMMUTABLE_INTENT_LINK"
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM live_entry_position_projections_v1")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM live_entry_position_projection_diagnostics_v1")
            assert cur.fetchone()[0] == 1
        conn.commit()
        try:
            _apply(conn, ROLLBACK)
        except Exception as exc:
            assert "LEI1D_ROLLBACK_BLOCKED_PROJECTION_EVIDENCE_EXISTS" in str(exc)
            conn.rollback()
        else:  # pragma: no cover
            raise AssertionError("rollback must fail closed when diagnostics exist")
    finally:
        conn.close()


def test_correction_pending_blocks_further_trusted_projection(disposable_postgres_v16):
    _name, factory = _install(disposable_postgres_v16, "correction")
    _seed_fill(factory, fill_id=50, trade_id=7401, qty="0.2", price="100",
               fee="0.001", fee_asset="BNB", at=START)
    _project(factory)
    before = _state(factory)[1:7]
    correction = EntryFillObservation.build(
        environment="live",deployment_id="local-live",adoption_id=1,generation=1,
        git_revision=GIT,exchange_source="okx",exchange_trade_id="7401",
        exchange_order_id=ORDER,client_order_id=CID,symbol="BNBUSDC",side="BUY",
        executed_qty="0.25",price="100",fee="0.001",fee_asset="BNB",
        executed_at=START,observed_at=START + timedelta(minutes=10),
        producer_identity="lei1d-pg",source_payload={"correction": True},
    )
    correction_result = process_entry_fill_attribution(
        mode=EntryFillAttributionMode.ENFORCE,observation=correction,
        repository=EntryFillAttributionRepository(factory),
    )
    assert correction_result.application_status in {
        FillApplicationStatus.CORRECTION_PENDING,
        FillApplicationStatus.IDEMPOTENCY_CONFLICT,
    }
    blocked = _project(factory)
    assert blocked.outcome is EntryProjectionOutcome.BLOCKED
    assert _state(factory)[1:7] == before

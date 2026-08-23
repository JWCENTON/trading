from __future__ import annotations

import json
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.capital_reservation import (
    accept_live_entry_intent_cursor,
    prepare_live_submission_cursor,
    reconcile_live_submission_cursor,
)
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
from common.live_exit_cost import capture_okx_exit_cost_snapshot_cursor
from common.position_risk_boundary import accept_boundary_policy_cursor
from common.pre_entry_risk import freeze_live_pre_entry_risk_cursor


ROOT = Path(__file__).resolve().parents[2]
LEI1A = (ROOT / "db/migrations/20260730_live_entry_intent_ledger_v1.sql").read_text()
LEI1B = (ROOT / "db/migrations/20260731_live_entry_submission_ack_v1.sql").read_text()
LEI1C = (ROOT / "db/migrations/20260731_live_entry_fill_attribution_v1.sql").read_text()
LEI1D_PATH = ROOT / "db/migrations/20260801_live_entry_position_projection_v1.sql"
LEI1D = LEI1D_PATH.read_text()
ROLLBACK = (ROOT / "db/migrations/20260801_live_entry_position_projection_v1_rollback.sql").read_text()
MANIFEST = json.loads((ROOT / "db/migrations/20260801_live_entry_position_projection_v1_manifest.json").read_text())
CAPITAL = (ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql").read_text()
BOUNDARY = (ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql").read_text()
EXIT_COST = (ROOT / "db/migrations/20260821_z_live_exit_cost_authority_v1.sql").read_text()
PRE_ENTRY_RISK = (ROOT / "db/migrations/20260822_pre_entry_risk_authority_v1.sql").read_text()

GIT = "a" * 40
INTENT = uuid.UUID("11111111-1111-5111-8111-111111111111")
SUBMISSION = uuid.UUID("22222222-2222-5222-8222-222222222222")
ACK = uuid.UUID("33333333-3333-5333-8333-333333333333")
CID = "ORC-L-BNBUSDC-TREN-1m-E-lei1d"
ORDER = "okx-order-lei1d"
START = datetime(2026, 8, 1, 8, 0, tzinfo=timezone.utc)
IDENTITY = "a" * 64


class _FeeClient:
    def get_trade_fee(self, *, symbol, instrument_type):
        return {"code": "0", "data": [{
            "instType": instrument_type, "taker": "-0.001",
            "maker": "-0.0005", "level": "Lv1", "ruleType": "normal",
            "ts": str(int(START.timestamp() * 1000)),
        }]}


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


def _install_live_risk_authorities(
    factory, *, include_exit_cost=True, include_boundary=True,
):
    intent = SimpleNamespace(
        intent_id=INTENT,
        deployment_id=SimpleNamespace(value="local-live"),
        decision_id=uuid.UUID("44444444-4444-5444-8444-444444444444"),
        symbol="BNBUSDC", strategy="TREND", interval="1m",
        client_order_id=CID, content_fingerprint="c" * 64,
        requested_qty=Decimal("1"), git_revision=GIT,
    )
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(CAPITAL)
            cur.execute(BOUNDARY)
            cur.execute(EXIT_COST)
            cur.execute(PRE_ENTRY_RISK)
            cur.execute(
                "CREATE TABLE candles(symbol text,interval text,"
                "open_time timestamptz,close numeric)"
            )
            cur.execute(
                "INSERT INTO candles VALUES ('BNBUSDC','1m',%s,100)",
                (START,),
            )
            _, reservation_id = accept_live_entry_intent_cursor(
                cur, intent=intent, account_identity_fingerprint=IDENTITY,
                requested_notional=Decimal("100"), effective_at=START,
            )
            assert reservation_id is not None
            if include_exit_cost:
                capture_okx_exit_cost_snapshot_cursor(
                    cur, exchange_client=_FeeClient(),
                    deployment_id="local-live",
                    account_identity_fingerprint=IDENTITY,
                    symbol="BNBUSDC", observed_at=START,
                )
            if include_boundary:
                assert accept_boundary_policy_cursor(
                    cur, environment="LIVE", deployment_id="local-live",
                    account_identity_fingerprint=IDENTITY,
                    reservation_id=reservation_id,
                    decision_id=str(intent.decision_id),
                    intent_id=str(intent.intent_id),
                    order_identity=str(intent.client_order_id),
                    symbol=intent.symbol, strategy=intent.strategy,
                    interval=intent.interval, effective_at=START,
                    source_authority="TEST_LIVE_COMMITMENT",
                    provenance={"test": True},
                    boundary_distance_pct=Decimal("0.8"),
                )[0] == "INSERTED"
                assert freeze_live_pre_entry_risk_cursor(
                    cur, intent=intent, reservation_id=reservation_id,
                    account_identity_fingerprint=IDENTITY,
                    reference_price_timestamp=START,
                    effective_at=START,
                )[0] == "INSERTED"
            prepare_live_submission_cursor(
                cur, reservation_id=reservation_id,
                intent_identity=str(INTENT), effective_at=START,
            )
            reconcile_live_submission_cursor(
                cur, reservation_id=reservation_id,
                source_event_identity=f"ACK:{ACK}", accepted=True,
                effective_at=START, order_identity=ORDER,
            )
        conn.commit()
    finally:
        conn.close()
    return intent


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


def _project(factory, *, commit=True, as_of=None):
    conn = factory()
    try:
        with conn.cursor() as cur:
            result = project_entry_intent(cur, INTENT, as_of=as_of)
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


def _risk_state(factory):
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state,original_quantity,"
                "transferred_quantity,remaining_committed_quantity "
                "FROM v_pre_entry_risk_current_v1 WHERE intent_id=%s",
                (str(INTENT),),
            )
            pre = cur.fetchone()
            cur.execute(
                "SELECT state,remaining_reserved_notional,deployed_notional,"
                "position_id FROM v_capital_reservation_current_v1 "
                "WHERE intent_identity=%s",
                (str(INTENT),),
            )
            reservation = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM pre_entry_risk_event_v1 "
                "WHERE source_authority='LIVE_CANONICAL_OPEN_RISK_HANDOFF'"
            )
            handoffs = cur.fetchone()[0]
            return pre, reservation, handoffs
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


def test_live_partial_then_full_post_fill_risk_handoff_is_exact_and_idempotent(
    disposable_postgres_v16,
):
    _name, factory = _install(disposable_postgres_v16, "live_risk_handoff")
    _install_live_risk_authorities(factory)
    _seed_fill(
        factory, fill_id=60, trade_id=7501, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )
    first = _project(factory, as_of=START + timedelta(minutes=2))
    assert first.post_fill_risk_handoff == "INSERTED"
    assert _risk_state(factory) == (
        (
            "PARTIALLY_TRANSFERRED", Decimal("1.000000000000000000"),
            Decimal("0.400000000000000000"), Decimal("0.600000000000000000"),
        ),
        (
            "PARTIALLY_DEPLOYED", Decimal("60.000000000000000000"),
            Decimal("40.000000000000000000"), first.position_id,
        ),
        1,
    )
    _seed_fill(
        factory, fill_id=61, trade_id=7502, qty="0.6", price="101",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=3),
    )
    second = _project(factory, as_of=START + timedelta(minutes=4))
    assert second.post_fill_risk_handoff == "INSERTED"
    assert _risk_state(factory) == (
        (
            "REPLACED_BY_OPEN_RISK", Decimal("1.000000000000000000"),
            Decimal("1.000000000000000000"), Decimal("0E-18"),
        ),
        (
            "DEPLOYED", Decimal("0E-18"),
            Decimal("100.000000000000000000"), first.position_id,
        ),
        2,
    )
    replay = _project(factory, as_of=START + timedelta(minutes=4))
    assert replay.outcome is EntryProjectionOutcome.NO_OP
    assert replay.post_fill_risk_handoff == "IDEMPOTENT"
    assert _risk_state(factory)[2] == 2


@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        ("reservation", "RESERVATION_DEPLOYMENT_INCOMPLETE:TEST_FAILURE"),
        ("boundary", "BOUNDARY_INCOMPLETE:MISSING_BOUNDARY"),
    ),
)
def test_live_post_fill_authority_failure_keeps_pre_entry_owner(
    disposable_postgres_v16, monkeypatch, failure, expected,
):
    _name, factory = _install(disposable_postgres_v16, f"fail_{failure}")
    _install_live_risk_authorities(factory)
    if failure == "reservation":
        monkeypatch.setattr(
            "common.entry_position_projection.deploy_live_entry_fill_cursor",
            lambda *args, **kwargs: "TEST_FAILURE",
        )
    else:
        monkeypatch.setattr(
            "common.entry_position_projection.activate_live_boundary_cursor",
            lambda *args, **kwargs: "MISSING_BOUNDARY",
        )
    _seed_fill(
        factory, fill_id=70, trade_id=7601, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )
    result = _project(factory, as_of=START + timedelta(minutes=2))
    assert result.post_fill_risk_handoff == expected
    assert _risk_state(factory)[0][0:4] == (
        "ACTIVE_COMMITTED", Decimal("1.000000000000000000"),
        Decimal("0E-18"), Decimal("1.000000000000000000"),
    )
    assert _risk_state(factory)[2] == 0


def test_live_stale_exit_cost_and_incomplete_open_risk_fail_closed(
    disposable_postgres_v16,
):
    _name, factory = _install(disposable_postgres_v16, "stale_exit")
    _install_live_risk_authorities(factory)
    _seed_fill(
        factory, fill_id=80, trade_id=7701, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )
    stale = _project(factory, as_of=START + timedelta(hours=25))
    assert stale.post_fill_risk_handoff == (
        "EXIT_COST_INCOMPLETE:MISSING_EXIT_COST_AUTHORITY"
    )
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"

    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO candles VALUES ('BNBUSDC','1m',%s,99)",
                (START + timedelta(minutes=5),),
            )
        conn.commit()
    finally:
        conn.close()
    breached = _project(factory, as_of=START + timedelta(minutes=6))
    assert breached.post_fill_risk_handoff == (
        "OPEN_RISK_INCOMPLETE:BOUNDARY_BREACHED_UNRESOLVED"
    )
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"
    assert _risk_state(factory)[2] == 0


def test_live_missing_mark_and_inventory_incomplete_converge_without_risk_gap(
    disposable_postgres_v16,
):
    _name, factory = _install(disposable_postgres_v16, "mark_inventory_gate")
    _install_live_risk_authorities(factory)
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM candles")
        conn.commit()
    finally:
        conn.close()
    _seed_fill(
        factory, fill_id=85, trade_id=7751, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )
    missing_mark = _project(factory, as_of=START + timedelta(minutes=2))
    assert missing_mark.post_fill_risk_handoff == (
        "OPEN_RISK_INCOMPLETE:MISSING_MARK"
    )
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE positions SET inventory_evidence_status='INCOMPLETE' "
                "WHERE id=%s", (missing_mark.position_id,),
            )
            cur.execute(
                "INSERT INTO candles VALUES ('BNBUSDC','1m',%s,100)",
                (START + timedelta(minutes=2),),
            )
        conn.commit()
    finally:
        conn.close()
    inventory = _project(factory, as_of=START + timedelta(minutes=3))
    assert inventory.post_fill_risk_handoff == "POSITION_INVENTORY_INCOMPLETE"
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"


def test_live_projection_and_authority_writes_roll_back_atomically_on_exception(
    disposable_postgres_v16, monkeypatch,
):
    _name, factory = _install(disposable_postgres_v16, "atomic_rollback")
    _install_live_risk_authorities(factory)
    _seed_fill(
        factory, fill_id=86, trade_id=7752, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )

    def _deployment_failure(*args, **kwargs):
        raise RuntimeError("TEST_RESERVATION_DEPLOYMENT_FAILURE")

    monkeypatch.setattr(
        "common.entry_position_projection.deploy_live_entry_fill_cursor",
        _deployment_failure,
    )
    with pytest.raises(RuntimeError, match="TEST_RESERVATION_DEPLOYMENT_FAILURE"):
        _project(factory, as_of=START + timedelta(minutes=2))
    assert _state(factory) is None
    pre, reservation, handoffs = _risk_state(factory)
    assert pre[0] == "ACTIVE_COMMITTED"
    assert reservation[0] == "EXCHANGE_ACK"
    assert handoffs == 0


def test_missing_fill_attribution_and_projection_conflict_never_release_pre_risk(
    disposable_postgres_v16,
):
    _name, factory = _install(disposable_postgres_v16, "lineage_fail_closed")
    _install_live_risk_authorities(factory)
    assert _project(factory).outcome is EntryProjectionOutcome.NO_ELIGIBLE_FILL
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO positions(symbol,strategy,\"interval\",status,side,qty,"
                "entry_price,entry_time) VALUES "
                "('BNBUSDC','TREND','1m','OPEN','LONG',1,100,now())"
            )
        conn.commit()
    finally:
        conn.close()
    _seed_fill(
        factory, fill_id=90, trade_id=7801, qty="0.4", price="100",
        fee="0.1", fee_asset="USDC", at=START + timedelta(minutes=1),
    )
    blocked = _project(factory, as_of=START + timedelta(minutes=2))
    assert blocked.outcome is EntryProjectionOutcome.BLOCKED
    assert blocked.detail == "OPEN_POSITION_WITHOUT_IMMUTABLE_INTENT_LINK"
    assert _risk_state(factory)[0][0] == "ACTIVE_COMMITTED"
    assert _risk_state(factory)[2] == 0

from __future__ import annotations

from types import SimpleNamespace

import pytest

from common.legacy_recovery import (
    LegacyPositionRecomputationService,
    LegacyRecoveryPlanner,
    semantic_repair_fingerprint,
)
from common.legacy_recovery_order_evidence import (
    LegacyRecoveryOrderEvidenceRepository,
    OrderEvidenceSourceType,
)
from common.legacy_recovery_repository import (
    EvidenceStatus,
    LegacyPositionEvidenceRepository,
)


@pytest.fixture()
def evidence_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_legacy_order_evidence_v1"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute(
            """
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            CREATE TABLE positions(
              id BIGINT PRIMARY KEY,symbol TEXT NOT NULL,strategy TEXT NOT NULL,
              "interval" TEXT NOT NULL,status TEXT NOT NULL,qty NUMERIC NOT NULL,
              entry_order_id TEXT,exit_order_id TEXT,
              entry_client_order_id TEXT,exit_client_order_id TEXT,
              entry_time TIMESTAMPTZ,exit_time TIMESTAMPTZ,
              entry_price NUMERIC,exit_price NUMERIC
            );
            CREATE TABLE financial_truth_instrument_snapshot_v1(
              id BIGINT PRIMARY KEY,step_size NUMERIC,
              quantity_precision INTEGER,base_asset TEXT,quote_asset TEXT,
              metadata_fingerprint TEXT
            );
            CREATE TABLE financial_truth_account_identity_v1(
              id BIGINT PRIMARY KEY,identity_fingerprint TEXT
            );
            INSERT INTO financial_truth_instrument_snapshot_v1 VALUES
              (1,0.000001,8,'BNB','USDC','instrument-v1');
            INSERT INTO financial_truth_account_identity_v1 VALUES
              (1,'account-v1');
            """
        )
    connection.commit()
    yield connection
    connection.close()


def _position(connection, *, entry_order_id=None, exit_order_id=None):
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO positions(
              id,symbol,strategy,"interval",status,qty,
              entry_order_id,exit_order_id,entry_time,exit_time,
              entry_price,exit_price
            ) VALUES (
              10326,'BNBUSDC','BBRANGE','1m','OPEN',0.035075,
              %s,%s,'2026-08-01 12:00:00+00','2026-08-01 12:05:00+00',
              570.2,571.2
            )
            """,
            (entry_order_id, exit_order_id),
        )
    connection.commit()


def _simulated_schema(connection, *, with_fills=False):
    with connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE simulated_orders(
              id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL,
              symbol TEXT NOT NULL,"interval" TEXT NOT NULL,strategy TEXT NOT NULL,
              side TEXT NOT NULL,price NUMERIC NOT NULL,
              quantity_btc NUMERIC NOT NULL,reason TEXT,rsi_14 NUMERIC,
              ema_21 NUMERIC,candle_open_time TIMESTAMPTZ,is_exit BOOLEAN NOT NULL
            )
            """
        )
        if with_fills:
            cur.execute(
                """
                CREATE TABLE simulated_execution_fills_v1(
                  id BIGSERIAL PRIMARY KEY,simulated_order_id BIGINT NOT NULL,
                  position_id BIGINT NOT NULL,order_purpose TEXT NOT NULL,
                  side TEXT NOT NULL,symbol TEXT NOT NULL,
                  fill_qty NUMERIC NOT NULL,fill_price NUMERIC NOT NULL,
                  fill_notional NUMERIC NOT NULL,fee_qty NUMERIC,
                  fee_asset TEXT,account_identity_id BIGINT,
                  instrument_snapshot_id BIGINT,source_authority TEXT NOT NULL,
                  environment TEXT NOT NULL,deployment_id TEXT NOT NULL,
                  simulation_model_version TEXT NOT NULL,
                  execution_at TIMESTAMPTZ NOT NULL
                )
                """
            )
    connection.commit()


def _binance_schema(connection, *, reconciled: bool):
    optional = ",reconciled_position_id BIGINT" if reconciled else ""
    with connection.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE binance_orders(
              id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL,
              symbol TEXT NOT NULL,side TEXT NOT NULL,order_id TEXT NOT NULL,
              client_order_id TEXT,position_id BIGINT,
              strategy TEXT,"interval" TEXT,order_purpose TEXT,status TEXT,
              requested_qty NUMERIC,price NUMERIC
            """ + optional + ")"
        )
        cur.execute(
            """
            CREATE TABLE binance_order_fills(
              id BIGSERIAL PRIMARY KEY,source TEXT,trade_id TEXT,order_id TEXT,
              symbol TEXT,side TEXT,executed_qty NUMERIC,avg_price NUMERIC,
              commission_amount NUMERIC,commission_asset TEXT,
              event_time TIMESTAMPTZ,instrument_snapshot_id BIGINT,
              account_identity_id BIGINT
            )
            """
        )
    connection.commit()


def _exact_simulated_entry(connection):
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO simulated_orders(
              created_at,symbol,"interval",strategy,side,price,
              quantity_btc,is_exit
            ) VALUES (
              '2026-08-01 11:59:59+00','BNBUSDC','1m','BBRANGE','BUY',
              570.2,0.035075,false
            ) RETURNING id
            """
        )
        value = int(cur.fetchone()[0])
    connection.commit()
    return value


def _resolve(connection, *, environment="PAPER", deployment_id="local-paper"):
    position = LegacyRecoveryOrderEvidenceRepository.read_position(
        connection, position_id=10326,
    )
    return LegacyRecoveryOrderEvidenceRepository.resolve(
        connection, position=position, environment=environment,
        deployment_id=deployment_id,
    )


def test_variant_a_local_paper_uses_simulated_source_without_binance_table(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db, with_fills=True)
    order_id = _exact_simulated_entry(evidence_db)
    result = _resolve(evidence_db)
    assert result.capabilities.public_payload()["status"] == "PRESENT_VALID"
    assert result.capabilities.source_type is (
        OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
    )
    assert not result.capabilities.binance_orders
    assert [item.source_primary_key for item in result.entry_orders] == [order_id]


def test_variant_b_vps_paper_legacy_source_projects_missing_reconciled_hint(evidence_db):
    _position(evidence_db, entry_order_id="entry-1")
    _binance_schema(evidence_db, reconciled=False)
    with evidence_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO binance_orders(
              created_at,symbol,side,order_id,client_order_id,position_id,
              order_purpose,status
            ) VALUES (
              '2026-08-01 11:59:59+00','BNBUSDC','BUY','entry-1',
              'bot-entry',10326,'ENTRY','FILLED'
            )
            """
        )
    evidence_db.commit()
    result = _resolve(evidence_db)
    assert result.capabilities.source_type is OrderEvidenceSourceType.LEGACY_ORDER_SOURCE
    assert not result.capabilities.reconciled_position_id
    assert result.entry_orders[0].linkage_type == "EXPLICIT_POSITION_ORDER_ID"


def test_variant_c_full_legacy_schema_uses_reconciled_linkage_hint(evidence_db):
    _position(evidence_db)
    _binance_schema(evidence_db, reconciled=True)
    with evidence_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO binance_orders(
              created_at,symbol,side,order_id,reconciled_position_id,
              order_purpose,status
            ) VALUES (
              '2026-08-01 11:59:59+00','BNBUSDC','BUY','entry-r',10326,
              'ENTRY','FILLED'
            )
            """
        )
    evidence_db.commit()
    result = _resolve(evidence_db)
    assert result.capabilities.reconciled_position_id
    assert result.entry_orders[0].linkage_type == "OPTIONAL_RECONCILED_POSITION_HINT"


def test_variant_d_paper_precedence_is_simulated_but_conflicting_source_blocks(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db)
    _binance_schema(evidence_db, reconciled=False)
    _exact_simulated_entry(evidence_db)
    with evidence_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO binance_orders(
              created_at,symbol,side,order_id,position_id,order_purpose
            ) VALUES (
              '2026-08-01 11:59:59+00','BNBUSDC','BUY','different',10326,'ENTRY'
            )
            """
        )
    evidence_db.commit()
    result = _resolve(evidence_db)
    assert result.capabilities.source_type is (
        OrderEvidenceSourceType.PAPER_SIMULATED_ORDER_SOURCE
    )
    assert "ORDER_EVIDENCE_SOURCE_CONFLICT" in result.conflicting_evidence


def test_variant_e_no_source_is_controlled_unsupported(evidence_db):
    _position(evidence_db)
    result = _resolve(evidence_db)
    assert result.capabilities.source_type is (
        OrderEvidenceSourceType.UNSUPPORTED_ORDER_SOURCE
    )
    assert result.missing_evidence == ("ORDER_EVIDENCE_SOURCE_UNSUPPORTED",)


def test_variant_f_zero_candidate_is_controlled_not_found(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db)
    result = _resolve(evidence_db)
    assert "ENTRY_ORDER_EVIDENCE_NOT_FOUND" in result.missing_evidence


def test_variant_g_one_candidate_generates_plan_with_source_identity(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db, with_fills=True)
    with evidence_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO simulated_orders(
              created_at,symbol,"interval",strategy,side,price,
              quantity_btc,is_exit
            ) VALUES
              ('2026-08-01 11:59:59+00','BNBUSDC','1m','BBRANGE','BUY',
               570.2,0.035075,false),
              ('2026-08-01 12:04:59+00','BNBUSDC','1m','BBRANGE','SELL',
               571.2,0.035075,true)
            RETURNING id
            """
        )
        entry_id, exit_id = [int(row[0]) for row in cur.fetchall()]
        cur.execute(
            """
            INSERT INTO simulated_execution_fills_v1(
              simulated_order_id,position_id,order_purpose,side,symbol,
              fill_qty,fill_price,fill_notional,fee_qty,fee_asset,
              account_identity_id,instrument_snapshot_id,source_authority,
              environment,deployment_id,simulation_model_version,execution_at
            ) VALUES
              (%s,10326,'ENTRY','BUY','BNBUSDC',0.035075,570.2,
               20,0.01,'USDC',1,1,'SIMULATED_EXECUTION','paper',
               'local-paper','V1','2026-08-01 12:00:00+00'),
              (%s,10326,'EXIT','SELL','BNBUSDC',0.035075,571.2,
               20,0.01,'USDC',1,1,'SIMULATED_EXECUTION','paper',
               'local-paper','V1','2026-08-01 12:05:00+00')
            """,
            (entry_id, exit_id),
        )
    evidence_db.commit()
    envelope = LegacyPositionEvidenceRepository().read(
        evidence_db, position_id=10326, environment="PAPER",
        deployment_id="local-paper",
    )
    assert envelope.evidence_status is EvidenceStatus.COMPLETE
    recomputation = LegacyPositionRecomputationService().recompute(envelope.evidence)
    plan = LegacyRecoveryPlanner().position_plan(recomputation)
    assert plan is not None
    payload = envelope.source_provenance["order_evidence"]
    assert payload["source_type"] == "PAPER_SIMULATED_ORDER_SOURCE"
    assert payload["entry_orders"][0]["source_primary_key"] == entry_id
    assert semantic_repair_fingerprint(payload)


def test_variant_h_multiple_exact_candidates_are_ambiguous(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db)
    _exact_simulated_entry(evidence_db)
    _exact_simulated_entry(evidence_db)
    result = _resolve(evidence_db)
    assert len(result.entry_orders) == 2
    assert "ENTRY_ORDER_EVIDENCE_AMBIGUOUS" in result.conflicting_evidence


def test_variant_i_changed_candidate_set_is_plan_stale_before_writes(evidence_db):
    _position(evidence_db)
    _simulated_schema(evidence_db, with_fills=True)
    _exact_simulated_entry(evidence_db)
    planned = _resolve(evidence_db)
    plan = SimpleNamespace(
        environment="PAPER", deployment_id="local-paper",
        database_name=planned.capabilities.database_identity,
        position_id=10326, entry_fill_ids=(), exit_fill_ids=(),
        order_evidence=planned.fingerprint_payload(),
    )
    _exact_simulated_entry(evidence_db)
    with evidence_db.cursor() as cur:
        with pytest.raises(RuntimeError, match="PLAN_STALE"):
            LegacyRecoveryOrderEvidenceRepository.lock_order_evidence(cur, plan)
        cur.execute("SELECT count(*) FROM positions WHERE id=10326")
        assert cur.fetchone()[0] == 1
    evidence_db.rollback()

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from common.simulated_execution_evidence import (
    SimulatedOrderWriteBlocked,
    create_simulated_order_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260805_forward_decision_registry_continuity_v1.sql"
).read_text()
NAMESPACE = (
    ROOT / "db/migrations/20260802_simulated_order_namespace_v1.sql"
).read_text()
LEDGER = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()


BOOTSTRAP = r"""
CREATE EXTENSION pgcrypto;

CREATE FUNCTION waltrade_uuid_v5_v1(p_namespace UUID,p_name TEXT)
RETURNS UUID LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE v_bytes BYTEA;v_hex TEXT;
BEGIN
 v_bytes:=substring(digest(uuid_send(p_namespace)||convert_to(p_name,'UTF8'),'sha1') FROM 1 FOR 16);
 v_bytes:=set_byte(v_bytes,6,(get_byte(v_bytes,6)&15)|80);
 v_bytes:=set_byte(v_bytes,8,(get_byte(v_bytes,8)&63)|128);
 v_hex:=encode(v_bytes,'hex');
 RETURN (substring(v_hex,1,8)||'-'||substring(v_hex,9,4)||'-'||
         substring(v_hex,13,4)||'-'||substring(v_hex,17,4)||'-'||
         substring(v_hex,21,12))::UUID;
END $$;

CREATE TABLE positions(
 id BIGSERIAL PRIMARY KEY,symbol TEXT NOT NULL,strategy TEXT NOT NULL,
 interval TEXT NOT NULL,status TEXT NOT NULL,side TEXT,qty NUMERIC,
 entry_price NUMERIC,entry_time TIMESTAMPTZ NOT NULL,exit_time TIMESTAMPTZ,
 exit_reason TEXT,gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,net_pnl_usdc NUMERIC,
 market_regime TEXT
);
CREATE TABLE simulated_orders(
 id SERIAL PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 symbol TEXT NOT NULL,"interval" TEXT NOT NULL,strategy TEXT NOT NULL,
 side TEXT NOT NULL,price NUMERIC NOT NULL,quantity_btc NUMERIC NOT NULL,
 reason TEXT,rsi_14 NUMERIC,ema_21 NUMERIC,candle_open_time TIMESTAMPTZ NOT NULL,
 is_exit BOOLEAN NOT NULL DEFAULT false,
 CONSTRAINT sim_orders_uniq_candle_exit
   UNIQUE(symbol,"interval",strategy,candle_open_time,is_exit)
);
CREATE UNIQUE INDEX ux_sim_orders_one_per_candle
 ON simulated_orders(symbol,"interval",strategy,candle_open_time);
CREATE UNIQUE INDEX ux_sim_orders_one_per_candle_isexit
 ON simulated_orders(symbol,"interval",strategy,candle_open_time,is_exit);

CREATE TABLE simulated_execution_fills_v1(
 id BIGSERIAL PRIMARY KEY,
 simulated_order_id BIGINT NOT NULL REFERENCES simulated_orders(id),
 position_id BIGINT NOT NULL REFERENCES positions(id),fill_index INTEGER NOT NULL DEFAULT 1,
 order_purpose TEXT NOT NULL,side TEXT NOT NULL,symbol TEXT NOT NULL,
 environment TEXT NOT NULL,deployment_id TEXT NOT NULL,
 UNIQUE(simulated_order_id,fill_index)
);

CREATE TABLE decision_registry_v1(
 decision_id UUID PRIMARY KEY,legacy_decision_key TEXT,deployment_id TEXT NOT NULL,
 environment TEXT NOT NULL,decision_type TEXT NOT NULL,decision_source TEXT NOT NULL,
 symbol TEXT,interval TEXT,strategy TEXT,market_regime TEXT,
 decision_timestamp TIMESTAMPTZ NOT NULL,source_table TEXT NOT NULL,
 source_record_id TEXT NOT NULL,source_natural_key TEXT NOT NULL,
 source_created_at TIMESTAMPTZ,observed_at TIMESTAMPTZ NOT NULL,
 ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),engine_name TEXT,
 engine_version TEXT,schema_version TEXT NOT NULL,decision_action TEXT,
 decision_reason TEXT,decision_payload JSONB NOT NULL DEFAULT '{}',position_id BIGINT,
 recommendation_id TEXT,run_id UUID,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 causal_linkage_status TEXT NOT NULL DEFAULT 'NO_ACTIVE_RECOMMENDATION',
 UNIQUE(deployment_id,environment,source_table,source_record_id,decision_type),
 CONSTRAINT ck_decision_registry_type CHECK(decision_type IN(
   'TRADE_EXECUTED','NO_TRADE','SIGNAL_REJECTED','ENTRY_BLOCKED',
   'ENTRY_SUPPRESSED','PAPER_SIMULATION')),
 CONSTRAINT ck_decision_registry_trade_position CHECK(
   decision_type<>'TRADE_EXECUTED' OR position_id IS NOT NULL)
);
CREATE UNIQUE INDEX ux_decision_registry_trade_position_v1
 ON decision_registry_v1(deployment_id,environment,position_id)
 WHERE decision_type='TRADE_EXECUTED';

CREATE TABLE decision_outcomes_v1(
 outcome_id UUID PRIMARY KEY,decision_id UUID NOT NULL REFERENCES decision_registry_v1,
 deployment_id TEXT NOT NULL,environment TEXT NOT NULL,outcome_type TEXT NOT NULL,
 horizon_minutes INTEGER,actual_trade BOOLEAN NOT NULL,position_id BIGINT,
 gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,net_pnl_usdc NUMERIC,mfe_pct NUMERIC,
 mae_pct NUMERIC,giveback_pct NUMERIC,outcome_status TEXT NOT NULL,
 outcome_reason TEXT,source_table TEXT NOT NULL,source_record_id TEXT NOT NULL,
 engine_name TEXT NOT NULL,engine_version TEXT NOT NULL,schema_version TEXT NOT NULL,
 evidence JSONB NOT NULL DEFAULT '{}',calculated_at TIMESTAMPTZ NOT NULL,
 run_id UUID,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE NULLS NOT DISTINCT(decision_id,outcome_type,horizon_minutes)
);
CREATE TABLE exit_trace_v1(position_id BIGINT PRIMARY KEY,mfe_pct NUMERIC,mae_pct NUMERIC,giveback_pct NUMERIC);
CREATE TABLE canonical_financial_truth_v1(position_id BIGINT PRIMARY KEY,financial_truth_status TEXT NOT NULL);
CREATE TABLE runtime_contract_adoption_v2(
 contract_name TEXT,environment TEXT,deployment_id TEXT,git_revision TEXT,
 generation INTEGER,status TEXT
);
INSERT INTO runtime_contract_adoption_v2 VALUES(
 'FEE_AWARE_INVENTORY_C2_2','paper','local-paper',repeat('a',40),1,'ACTIVE'
);
CREATE TABLE learning_outcome_exclusion_v1(position_id BIGINT PRIMARY KEY);

CREATE OR REPLACE FUNCTION refresh_decision_identity_outcome_v1(
 p_lookback_hours INTEGER,p_environment TEXT,p_deployment_id TEXT,
 p_run_id UUID DEFAULT gen_random_uuid()
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE v_namespace UUID:='c966214a-6a82-50e9-913b-5144237cdf44';
        v_outcome_namespace UUID:='f955f1a5-e3ea-51cf-9178-b7c838b609a4';
        v_count INTEGER;
BEGIN
    WITH source_rows AS (
        SELECT p.*
        FROM positions p
        WHERE p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)
    ), upserted AS (
        INSERT INTO decision_registry_v1(
          decision_id,deployment_id,environment,decision_type,decision_source,
          symbol,interval,strategy,decision_timestamp,source_table,source_record_id,
          source_natural_key,observed_at,engine_version,schema_version,decision_payload,
          position_id
        ) SELECT waltrade_uuid_v5_v1(v_namespace,p.id::TEXT),p_deployment_id,
          p_environment,'TRADE_EXECUTED','POSITION',p.symbol,p.interval,p.strategy,
          p.entry_time,'positions',p.id::TEXT,p.id::TEXT,now(),'LEGACY','V1','{}',p.id
          FROM source_rows p ON CONFLICT DO NOTHING RETURNING 1
    ) SELECT count(*) INTO v_count FROM upserted;

    WITH source_outcomes AS (
        SELECT d.decision_id,d.deployment_id,d.environment,d.position_id,
               p.gross_pnl_usdc,p.fees_usdc,p.net_pnl_usdc,
               e.mfe_pct,e.mae_pct,e.giveback_pct,p.exit_time,p.exit_reason
        FROM decision_registry_v1 d
        JOIN positions p ON p.id = d.position_id
        LEFT JOIN exit_trace_v1 e ON e.position_id = p.id
        WHERE d.deployment_id = p_deployment_id
          AND d.environment = p_environment
          AND d.decision_type = 'TRADE_EXECUTED'
          AND p.exit_time IS NOT NULL
    ), upserted AS (
        INSERT INTO decision_outcomes_v1(
          outcome_id,decision_id,deployment_id,environment,outcome_type,
          actual_trade,position_id,gross_pnl_usdc,fees_usdc,net_pnl_usdc,
          mfe_pct,mae_pct,giveback_pct,outcome_status,outcome_reason,source_table,
          source_record_id,engine_name,engine_version,schema_version,evidence,
          calculated_at
        ) SELECT waltrade_uuid_v5_v1(v_outcome_namespace,s.decision_id::TEXT),
          s.decision_id,s.deployment_id,s.environment,'ACTUAL_TRADE',true,s.position_id,
          s.gross_pnl_usdc,s.fees_usdc,s.net_pnl_usdc,s.mfe_pct,s.mae_pct,s.giveback_pct,
          'COMPLETE',s.exit_reason,'positions',s.position_id::TEXT,'OUTCOME','V1','V1',
          '{}',now() FROM source_outcomes s ON CONFLICT DO NOTHING RETURNING 1
    ) SELECT count(*) INTO v_count FROM upserted;
    RETURN jsonb_build_object('status','OK');
END $$;

CREATE VIEW v_learning_eligible_forward_decision_v1 AS
SELECT r.decision_id,r.position_id
FROM decision_registry_v1 r
JOIN decision_outcomes_v1 o USING(decision_id)
JOIN canonical_financial_truth_v1 ft ON ft.position_id=r.position_id
LEFT JOIN learning_outcome_exclusion_v1 x ON x.position_id=r.position_id
WHERE r.decision_type='ENTRY_DECISION'
  AND o.outcome_status='COMPLETE'
  AND ft.financial_truth_status='COMPLETE'
  AND x.position_id IS NULL;
"""


@pytest.fixture()
def continuity_db(disposable_postgres_v16):
    temporary_name = "waltrade_baseline_test_forward_registry_paper_v1"
    try:
        disposable_postgres_v16.create_database(temporary_name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    admin = disposable_postgres_v16.connect()
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname='trading_paper'"
        )
        if cur.fetchone() is None:
            cur.execute(
                f'ALTER DATABASE "{temporary_name}" RENAME TO trading_paper'
            )
    admin.close()
    name = "trading_paper"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=disposable_postgres_v16.port,
        dbname=name,
        user=disposable_postgres_v16.user,
        password=disposable_postgres_v16.password,
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(BOOTSTRAP)
        cur.execute(LEDGER)
        cur.execute(NAMESPACE)
        cur.execute(MIGRATION)
        cur.execute(MIGRATION)
    conn.commit()
    yield conn
    conn.close()


def _entry(cur, candle):
    return create_simulated_order_cursor(
        cur,symbol="BTCUSDC",interval="1m",strategy="BBRANGE",side="BUY",
        price=Decimal("100"),quantity=Decimal("0.01"),reason="FINAL_ALLOW",
        candle_open_time=candle,is_exit=False,
    )


def test_forward_entry_retry_failure_and_full_outcome_continuity(continuity_db):
    candle = datetime(2026,8,5,7,0,tzinfo=timezone.utc)
    with continuity_db.cursor() as cur:
        order_id = _entry(cur,candle)
        assert isinstance(order_id,int) and not isinstance(order_id,bool)
        cur.execute(
            "INSERT INTO positions(symbol,strategy,interval,status,side,qty,"
            "entry_price,entry_time) VALUES "
            "('BTCUSDC','BBRANGE','1m','OPEN','LONG',0.01,100,%s) RETURNING id",
            (candle,),
        )
        position_id = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO simulated_execution_fills_v1("
            "simulated_order_id,position_id,order_purpose,side,symbol,environment,deployment_id) "
            "VALUES(%s,%s,'ENTRY','BUY','BTCUSDC','paper','local-paper') RETURNING decision_id",
            (order_id,position_id),
        )
        decision_id = cur.fetchone()[0]
    continuity_db.commit()

    with continuity_db.cursor() as cur:
        retry = _entry(cur,candle)
        assert isinstance(retry,SimulatedOrderWriteBlocked)
        assert retry.status == "IDEMPOTENT_EXISTING_FORWARD_ORDER"
    continuity_db.rollback()

    with continuity_db.cursor() as cur:
        cur.execute(
            "SELECT count(*),(SELECT count(*) FROM simulated_orders),"
            "(SELECT count(*) FROM positions),position_id,decision_action,"
            "decision_payload->>'final_action',decision_payload->>'execution_side' "
            "FROM decision_registry_v1 WHERE decision_type='ENTRY_DECISION' "
            "GROUP BY position_id,decision_action,decision_payload"
        )
        assert cur.fetchone() == (1,1,1,position_id,"EXECUTE","EXECUTE","BUY")
        cur.execute("SELECT decision_id FROM simulated_orders WHERE id=%s",(order_id,))
        assert cur.fetchone()[0] == decision_id

        cur.execute(
            "UPDATE positions SET status='CLOSED',exit_time=now(),exit_reason='TEST_EXIT',"
            "gross_pnl_usdc=1,fees_usdc=0.1,net_pnl_usdc=0.9 WHERE id=%s",
            (position_id,),
        )
        cur.execute(
            "SELECT refresh_decision_identity_outcome_v1(24,'trading_paper','LOCAL')"
        )
        cur.execute("SELECT count(*) FROM decision_outcomes_v1")
        assert cur.fetchone()[0] == 0
        cur.execute(
            "INSERT INTO canonical_financial_truth_v1 VALUES(%s,'COMPLETE')",
            (position_id,),
        )
        cur.execute(
            "SELECT refresh_decision_identity_outcome_v1(24,'trading_paper','LOCAL')"
        )
        cur.execute("SELECT decision_id,position_id FROM decision_outcomes_v1")
        assert cur.fetchall() == [(decision_id,position_id)]
        cur.execute("SELECT count(*) FROM v_learning_eligible_forward_decision_v1")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM decision_registry_v1 WHERE position_id=%s",
            (position_id,),
        )
        assert cur.fetchone()[0] == 1
    continuity_db.commit()

    with continuity_db.cursor() as cur:
        cur.execute(
            """CREATE FUNCTION fail_registry_write_v1() RETURNS trigger
               LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'REGISTRY_WRITE_FAILED';END$$;
               CREATE TRIGGER aaa_fail_registry_write_v1 BEFORE INSERT
               ON decision_registry_v1 FOR EACH ROW EXECUTE FUNCTION fail_registry_write_v1()"""
        )
    continuity_db.commit()
    failure_candle = datetime(2026,8,5,7,1,tzinfo=timezone.utc)
    with pytest.raises(psycopg2.errors.RaiseException,match="REGISTRY_WRITE_FAILED"):
        with continuity_db.cursor() as cur:
            _entry(cur,failure_candle)
    continuity_db.rollback()
    with continuity_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM simulated_orders")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM simulated_execution_fills_v1")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT count(*) FROM positions")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 WHERE migration_id=%s",
            ("20260805_forward_decision_registry_continuity_v1.sql",),
        )
        assert cur.fetchone()[0] == 1

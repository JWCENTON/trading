from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
import pytest

import common.local_live_legacy_residual_repair as repair_module
from common.local_live_legacy_residual_repair import (
    ALLOWED_POSITION_IDS,
    APPLY_ENABLE_ENV,
    BoundedResidualRepairService,
    ManifestPosition,
    RepairManifest,
    RuntimeIdentity,
)
from tests.postgres.database_baseline_fixture import disposable_postgres_v16


GIT_SHA = "7" * 40
CASES = {
    3079: ("BTCUSDC", "0.00031545", "0.000001104075", "0.00031435", "0.00000001", "0.0001"),
    3080: ("BNBUSDC", "0.035152", "0.000123032", "0.035029", "0.000001", "0.001"),
    3081: ("ETHUSDC", "0.010623", "0.0000371805", "0.010585", "0.000001", "0.001"),
    3082: ("ETHUSDC", "0.010584", "0.000037044", "0.010547", "0.000001", "0.001"),
    3083: ("SOLUSDC", "0.25921", "0.000907235", "0.25831", "0.00001", "0.01"),
    3084: ("BTCUSDC", "0.00030191", "0.000001056685", "0.00030085", "0.00000001", "0.0001"),
    3085: ("SOLUSDC", "0.26924", "0.000942340", "0.26829", "0.00001", "0.01"),
}


SCHEMA = r"""
CREATE TABLE positions(
  id BIGINT PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
  interval TEXT NOT NULL, status TEXT NOT NULL, side TEXT NOT NULL,
  qty NUMERIC NOT NULL, entry_price NUMERIC NOT NULL, entry_time TIMESTAMPTZ NOT NULL,
  exit_price NUMERIC, exit_time TIMESTAMPTZ, exit_reason TEXT,
  entry_order_id TEXT NOT NULL, exit_order_id TEXT NOT NULL,
  inventory_evidence_status TEXT, gross_entry_executed_qty NUMERIC,
  entry_base_fee_qty NUMERIC, net_entry_inventory_qty NUMERIC,
  cumulative_exit_executed_qty NUMERIC, exit_inventory_reduction_qty NUMERIC,
  remaining_inventory_qty NUMERIC, terminal_dust_qty NUMERIC,
  terminal_reason TEXT, inventory_calculated_at TIMESTAMPTZ
);
CREATE TABLE bot_control(id INTEGER PRIMARY KEY, live_orders_enabled BOOLEAN NOT NULL);
CREATE TABLE panic_state(id BOOLEAN PRIMARY KEY, panic_enabled BOOLEAN NOT NULL);
CREATE TABLE automation_kv(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE worker_heartbeats(
  service_name TEXT, environment TEXT, status TEXT, meta JSONB,
  PRIMARY KEY(service_name,environment)
);
CREATE TABLE binance_orders(
  id BIGINT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, symbol TEXT NOT NULL,
  side TEXT NOT NULL, client_order_id TEXT NOT NULL, order_id TEXT UNIQUE NOT NULL,
  status TEXT NOT NULL, position_id BIGINT, is_exit BOOLEAN NOT NULL,
  strategy TEXT, interval TEXT, order_purpose TEXT, requested_qty NUMERIC,
  order_accepted BOOLEAN, exchange_source TEXT, reconciliation_status TEXT,
  reconciled_position_id BIGINT, reconciled_at TIMESTAMPTZ,
  reconciled_fill_count INTEGER, reconciled_executed_qty NUMERIC,
  unreconciled_qty NUMERIC, last_reconciliation_action TEXT
);
CREATE TABLE binance_order_fills(
  id BIGINT PRIMARY KEY, source TEXT NOT NULL, order_id TEXT NOT NULL,
  symbol TEXT NOT NULL, side TEXT NOT NULL, role TEXT NOT NULL,
  executed_qty NUMERIC NOT NULL, avg_price NUMERIC NOT NULL,
  quote_notional_usdc NUMERIC NOT NULL, commission_amount NUMERIC,
  commission_asset TEXT, commission_usdc NUMERIC, event_time TIMESTAMPTZ NOT NULL,
  trade_id TEXT NOT NULL
);
CREATE TABLE exchange_fill_ingestion_state_v2(
  ingestion_id BIGINT PRIMARY KEY, source TEXT NOT NULL DEFAULT 'okx',
  account_identity_key TEXT NOT NULL DEFAULT '1', symbol TEXT NOT NULL,
  order_id TEXT NOT NULL, trade_id TEXT NOT NULL,
  correction_revision INTEGER, source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT, applied_at TIMESTAMPTZ,
  application_status TEXT, adoption_id BIGINT,
  contract_generation BIGINT, local_fill_id BIGINT
);
CREATE TABLE position_lifecycle_events_c2_2(
  event_id BIGSERIAL PRIMARY KEY, position_id BIGINT NOT NULL REFERENCES positions(id),
  order_id TEXT NOT NULL, mutation_kind TEXT NOT NULL,
  mutation_high_water NUMERIC NOT NULL, payload JSONB NOT NULL,
  committed_at TIMESTAMPTZ DEFAULT clock_timestamp(), emitted_at TIMESTAMPTZ,
  UNIQUE(position_id,order_id,mutation_kind,mutation_high_water)
);
CREATE TABLE canonical_financial_truth_v1(
  position_id BIGINT PRIMARY KEY REFERENCES positions(id),
  financial_truth_status TEXT NOT NULL, executed_entry_qty NUMERIC,
  executed_exit_qty NUMERIC, remaining_qty NUMERIC, gross_entry_qty NUMERIC,
  gross_exit_qty NUMERIC, base_asset_entry_fee_qty NUMERIC,
  base_asset_exit_fee_qty NUMERIC, net_entry_inventory_qty NUMERIC,
  net_exit_inventory_reduction_qty NUMERIC, gross_remaining_execution_qty NUMERIC,
  remaining_inventory_qty NUMERIC, authoritative_entry_notional NUMERIC,
  authoritative_exit_notional NUMERIC, authoritative_entry_fees_usdc NUMERIC,
  authoritative_exit_fees_usdc NUMERIC, authoritative_fees_usdc NUMERIC,
  authoritative_gross_pnl NUMERIC, authoritative_net_pnl NUMERIC,
  estimated_gross_pnl NUMERIC, estimated_fees_usdc NUMERIC,
  estimated_net_pnl NUMERIC, entry_fill_count INTEGER, exit_fill_count INTEGER,
  first_entry_fill_at TIMESTAMPTZ, last_entry_fill_at TIMESTAMPTZ,
  first_exit_fill_at TIMESTAMPTZ, last_exit_fill_at TIMESTAMPTZ,
  source_authority TEXT, source_exchange TEXT, source_environment TEXT,
  source_deployment_id TEXT, source_account_identity_fingerprint TEXT,
  source_order_ids JSONB NOT NULL, source_fill_ids JSONB NOT NULL,
  source_fingerprint TEXT, calculation_version TEXT, writer_version TEXT,
  calculated_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, failure_code TEXT,
  failure_detail TEXT, failure_reason TEXT, authoritative_source TEXT,
  authoritative_evidence JSONB NOT NULL, evidence_observed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);
CREATE TABLE canonical_financial_truth_audit_v1(
  id BIGSERIAL PRIMARY KEY, position_id BIGINT NOT NULL REFERENCES positions(id),
  previous_status TEXT, new_status TEXT NOT NULL, previous_fingerprint TEXT,
  new_fingerprint TEXT NOT NULL, previous_values JSONB NOT NULL,
  new_values JSONB NOT NULL, reason TEXT NOT NULL, writer_version TEXT NOT NULL,
  invocation_type TEXT NOT NULL, invocation_identity TEXT, created_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  UNIQUE(position_id,new_fingerprint)
);
CREATE TABLE legacy_repair_audit_v1(
  audit_id BIGSERIAL PRIMARY KEY, incident_type TEXT NOT NULL,
  incident_identity TEXT NOT NULL, operation_type TEXT NOT NULL,
  planner_version TEXT NOT NULL, writer_version TEXT,
  semantic_fingerprint_before TEXT, semantic_fingerprint_expected TEXT,
  semantic_fingerprint_after TEXT, plan_status TEXT NOT NULL,
  execution_status TEXT NOT NULL, invocation_identity TEXT UNIQUE NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL, started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ, actor_source TEXT NOT NULL,
  blocking_reasons JSONB NOT NULL, eligible_actions JSONB NOT NULL,
  executed_actions JSONB NOT NULL, expected_changes JSONB NOT NULL,
  actual_changes JSONB NOT NULL, post_state_invariants JSONB NOT NULL,
  error_code TEXT, error_detail TEXT, recorded_at TIMESTAMPTZ DEFAULT clock_timestamp()
);
CREATE TABLE legacy_repair_provenance_v1(
  provenance_id BIGSERIAL PRIMARY KEY, evidence_source TEXT NOT NULL,
  source_identity TEXT NOT NULL, source_fingerprint TEXT NOT NULL,
  instrument_identity TEXT, account_provenance JSONB NOT NULL,
  deployment_provenance JSONB NOT NULL, fee_evidence JSONB NOT NULL,
  valuation_evidence JSONB NOT NULL, immutable_payload JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL, recorded_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  UNIQUE(evidence_source,source_identity)
);
CREATE TABLE learning_outcome_exclusion_v1(
  exclusion_id BIGSERIAL PRIMARY KEY, environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL, position_id BIGINT NOT NULL REFERENCES positions(id),
  exclusion_reason TEXT NOT NULL, source_type TEXT NOT NULL,
  semantic_fingerprint_v2 TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  created_by TEXT NOT NULL, git_sha TEXT NOT NULL,
  UNIQUE(environment,deployment_id,position_id)
);
CREATE TABLE learning_feature_warehouse_v1(
  decision_key TEXT PRIMARY KEY, position_id BIGINT, evidence_status TEXT
);
CREATE TABLE decision_replay_v1(
  decision_key TEXT PRIMARY KEY, position_id BIGINT, replay_status TEXT
);
CREATE TABLE exit_trace_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE exit_trace_v2(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE exit_trace_v3(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE decision_outcomes_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE learning_feedback_shadow_recommendations(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE TABLE decision_registry_v1(id BIGSERIAL PRIMARY KEY,position_id BIGINT);
CREATE VIEW v_learning_eligible_closed_positions_v1 AS
 SELECT id AS position_id FROM positions p WHERE status='CLOSED' AND exit_time IS NOT NULL
 AND NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=p.id);
CREATE VIEW v_learning_eligible_exit_trace_v1 AS SELECT position_id FROM exit_trace_v1 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_exit_trace_v2 AS SELECT position_id FROM exit_trace_v2 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_exit_trace_v3 AS SELECT position_id FROM exit_trace_v3 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_shadow_recommendations_v1 AS SELECT position_id FROM learning_feedback_shadow_recommendations s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_feature_warehouse_v1 AS SELECT position_id FROM learning_feature_warehouse_v1 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_decision_replay_v1 AS SELECT position_id FROM decision_replay_v1 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_decision_registry_v1 AS SELECT position_id FROM decision_registry_v1 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
CREATE VIEW v_learning_eligible_decision_outcomes_v1 AS SELECT position_id FROM decision_outcomes_v1 s
 WHERE NOT EXISTS(SELECT 1 FROM learning_outcome_exclusion_v1 x WHERE x.position_id=s.position_id);
"""


class FakeExchange:
    place_order_calls = 0
    cancel_order_calls = 0

    def __init__(self, evidence):
        self.evidence = evidence
        self.pending = ()
        self.drop_trade_for = None

    def pending_spot_orders(self):
        return tuple(self.pending)

    def account_fingerprint(self):
        return "account-identity-v1"

    def order(self, symbol, order_id):
        rows = self.evidence[order_id]
        qty = sum((Decimal(row["quantity"]) for row in rows), Decimal("0"))
        return {"order_id": order_id, "state": "filled", "executed_qty": str(qty)}

    def fills(self, symbol, order_id):
        rows = self.evidence[order_id]
        if self.drop_trade_for == order_id:
            return tuple(rows[:-1])
        return tuple(rows)

    def instrument(self, symbol):
        _symbol, _gross, _fee, _exit, lot, minimum = next(
            row for row in CASES.values() if row[0] == symbol
        )
        return {
            "symbol": symbol, "instrument_id": symbol[:-4] + "-USDC",
            "base_asset": symbol[:-4], "quote_asset": "USDC",
            "lot_size": lot, "min_size": minimum, "min_notional": "0",
            "metadata_fingerprint": "instrument-" + symbol,
        }


def _seed(connection):
    now = datetime(2026, 8, 1, tzinfo=timezone.utc)
    exchange_evidence = {}
    with connection.cursor() as cur:
        cur.execute(SCHEMA)
        cur.executemany("INSERT INTO bot_control VALUES(%s,false)", [(i,) for i in range(1, 33)])
        cur.execute("INSERT INTO panic_state VALUES(true,false)")
        cur.execute("INSERT INTO automation_kv VALUES('orc_v5_apply_mode','automation_runner')")
        cur.execute("INSERT INTO worker_heartbeats VALUES('bot-runner-orchestrator','LIVE','healthy','{}')")
        next_order_row = 5000
        next_fill = 6000
        next_ingestion = 7000
        for offset, position_id in enumerate(sorted(CASES)):
            symbol, gross, base_fee, exit_qty, _lot, _minimum = CASES[position_id]
            entry_order = f"entry-{position_id}"
            exit_order = f"exit-{position_id}"
            entry_price = Decimal("100") + offset
            exit_price = entry_price + Decimal("2")
            entry_time = now + timedelta(minutes=offset * 10)
            exit_time = entry_time + timedelta(minutes=5)
            cur.execute(
                "INSERT INTO positions(id,symbol,strategy,interval,status,side,qty,entry_price,entry_time,entry_order_id,exit_order_id) "
                "VALUES(%s,%s,'Supertrend','1m','OPEN','LONG',%s,%s,%s,%s,%s)",
                (position_id, symbol, gross, entry_price, entry_time, entry_order, exit_order),
            )
            for is_exit, order_id, side, qty, price in (
                (False, entry_order, "BUY", Decimal(gross), entry_price),
                (True, exit_order, "SELL", Decimal(exit_qty), exit_price),
            ):
                next_order_row += 1
                next_fill += 1
                next_ingestion += 1
                trade_id = f"trade-{next_fill}"
                fee_qty = Decimal("0.01") if is_exit else Decimal(base_fee)
                fee_asset = "USDC" if is_exit else symbol[:-4]
                fee_usdc = fee_qty if is_exit else fee_qty * price
                notional = qty * price
                cur.execute(
                    "INSERT INTO binance_orders VALUES(%s,%s,%s,%s,%s,%s,'FILLED',%s,%s,'Supertrend','1m',%s,%s,true,'okx','APPLIED',%s,%s,1,%s,0,'APPLIED')",
                    (next_order_row, entry_time, symbol, side, f"client-{order_id}", order_id,
                     position_id, is_exit, "EXIT" if is_exit else "ENTRY", qty,
                     position_id, exit_time, qty),
                )
                cur.execute(
                    "INSERT INTO binance_order_fills VALUES(%s,'okx',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (next_fill, order_id, symbol, side, "EXIT" if is_exit else "ENTRY",
                     qty, price, notional, fee_qty, fee_asset, fee_usdc,
                     exit_time if is_exit else entry_time, trade_id),
                )
                cur.execute(
                    "INSERT INTO exchange_fill_ingestion_state_v2("
                    "ingestion_id,symbol,order_id,trade_id,correction_revision,"
                    "source_fingerprint,applied_fingerprint,applied_at,"
                    "application_status,adoption_id,contract_generation,local_fill_id) "
                    "VALUES(%s,%s,%s,%s,0,%s,%s,now(),'APPLIED',1,1,%s)",
                    (next_ingestion, symbol, order_id, trade_id,
                     "f" * 64, "f" * 64, next_fill),
                )
                exchange_evidence[order_id] = ({
                    "trade_id": trade_id, "order_id": order_id, "side": side,
                    "quantity": str(qty), "price": str(price),
                    "fee_quantity": str(fee_qty), "fee_asset": fee_asset,
                    "event_time_ms": str(int((exit_time if is_exit else entry_time).timestamp() * 1000)),
                },)
            cur.execute(
                "INSERT INTO learning_feature_warehouse_v1 VALUES(%s,%s,'OPEN_OR_INCOMPLETE')",
                (f"decision-{position_id}", position_id),
            )
            cur.execute(
                "INSERT INTO decision_replay_v1 VALUES(%s,%s,'REPLAY_OPEN_OR_INCOMPLETE')",
                (f"decision-{position_id}", position_id),
            )
        for row_id in (3758, 3760, 3762):
            cur.execute(
                "INSERT INTO binance_orders VALUES(%s,%s,'BNBUSDC','SELL',%s,%s,'FILLED',NULL,true,NULL,NULL,'ADMINISTRATIVE',1,true,'okx','QUARANTINED',NULL,NULL,0,0,0,'NO_LINKAGE')",
                (row_id, now, f"forbidden-client-{row_id}",
                 "3789163681263689728" if row_id == 3762 else f"forbidden-{row_id}"),
            )
        for ingestion_id in (22, 23, 24, 25):
            cur.execute(
                "INSERT INTO exchange_fill_ingestion_state_v2("
                "ingestion_id,symbol,order_id,trade_id,correction_revision,"
                "source_fingerprint,application_status) "
                "VALUES(%s,'BNBUSDC',%s,%s,0,%s,'EXTERNAL_OR_MANUAL_UNLINKED')",
                (ingestion_id, "3789163681263689728",
                 f"forbidden-trade-{ingestion_id}", "e" * 64),
            )
    connection.commit()
    return exchange_evidence


def _manifest(fingerprints=None):
    fingerprints = fingerprints or {position_id: "0" * 64 for position_id in CASES}
    return RepairManifest("LIVE", "local-live", tuple(
        ManifestPosition(
            position_id, f"entry-{position_id}", f"exit-{position_id}",
            fingerprints[position_id],
        ) for position_id in sorted(CASES)
    ))


def _service(pg, exchange, manifest):
    return BoundedResidualRepairService(
        pg.connect, exchange,
        RuntimeIdentity("OKX", "LIVE", "local-live", GIT_SHA, "PROCESS_SUPERVISOR"),
        manifest, expected_git_sha=GIT_SHA, expected_database=pg.database,
    )


def _execute(pg, sql, params=()):
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
        connection.commit()
    finally:
        connection.close()


def _scalar(pg, sql, params=()):
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    finally:
        connection.close()


def test_disposable_pg_bounded_writer_contract(disposable_postgres_v16, monkeypatch):
    database = "waltrade_baseline_test_residual_writer_v1"
    disposable_postgres_v16.create_database(database)
    pg = replace(disposable_postgres_v16, database=database)
    monkeypatch.setattr(repair_module, "EXPECTED_DATABASE", pg.database)
    connection = pg.connect()
    try:
        evidence = _seed(connection)
    finally:
        connection.close()
    exchange = FakeExchange(evidence)
    provisional = _service(pg, exchange, _manifest()).plan(enforce_fingerprints=False)
    assert provisional.summary() == {
        "positions_planned": 7, "phantom_closes": 4,
        "terminal_dust_closes": 3, "expected_db_mutations": 49,
        "okx_mutations": 0, "blocked_rows": 0, "already_repaired_rows": 0,
    }
    fingerprints = {row.position_id: row.semantic_fingerprint for row in provisional.positions}
    manifest = _manifest(fingerprints)
    service = _service(pg, exchange, manifest)
    assert service.plan().summary()["positions_planned"] == 7

    # Safety gates use committed disposable state and are restored immediately.
    _execute(pg, "UPDATE bot_control SET live_orders_enabled=true WHERE id=1")
    with pytest.raises(RuntimeError, match="LIVE_ORDERS_NOT_CONTAINED"):
        service.plan()
    _execute(pg, "UPDATE bot_control SET live_orders_enabled=false WHERE id=1")
    exchange.pending = ({"ordId": "pending"},)
    with pytest.raises(RuntimeError, match="OKX_PENDING_SPOT_ORDERS"):
        service.plan()
    exchange.pending = ()
    _execute(pg, "INSERT INTO positions(id,symbol,strategy,interval,status,side,qty,entry_price,entry_time,entry_order_id,exit_order_id) VALUES(9999,'BTCUSDC','x','1m','OPEN','LONG',1,1,now(),'x','y')")
    with pytest.raises(RuntimeError, match="UNEXPECTED_OPEN_COHORT"):
        service.plan()
    _execute(pg, "DELETE FROM positions WHERE id=9999")
    _execute(pg, "UPDATE binance_order_fills SET order_id='hidden' WHERE order_id='entry-3079'")
    with pytest.raises(RuntimeError, match="MISSING_ENTRY_FILL"):
        service.plan()
    _execute(pg, "UPDATE binance_order_fills SET order_id='entry-3079' WHERE order_id='hidden'")
    _execute(pg, "UPDATE binance_order_fills SET order_id='hidden-exit' WHERE order_id='exit-3079'")
    with pytest.raises(RuntimeError, match="MISSING_EXIT_FILL"):
        service.plan()
    _execute(pg, "UPDATE binance_order_fills SET order_id='exit-3079' WHERE order_id='hidden-exit'")
    _execute(pg, "UPDATE binance_order_fills SET commission_usdc=NULL WHERE order_id='exit-3079'")
    with pytest.raises(RuntimeError, match="MISSING_FEE_EVIDENCE"):
        service.plan()
    _execute(pg, "UPDATE binance_order_fills SET commission_usdc=0.01 WHERE order_id='exit-3079'")
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET correction_revision=2,"
        "applied_fingerprint=NULL,applied_at=NULL,adoption_id=NULL,"
        "contract_generation=NULL,local_fill_id=NULL,"
        "application_status='CORRECTION_PENDING' WHERE order_id='entry-3079'",
    )
    with pytest.raises(RuntimeError, match="BLOCKED_BY_MISSING_EQUIVALENCE_PROOF"):
        service.plan()
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET correction_revision=0,"
        "applied_fingerprint=source_fingerprint,applied_at=now(),adoption_id=1,"
        "contract_generation=1,local_fill_id=(SELECT id FROM binance_order_fills "
        "WHERE order_id='entry-3079'),application_status='APPLIED' "
        "WHERE order_id='entry-3079'",
    )
    exchange.drop_trade_for = "entry-3079"
    with pytest.raises(RuntimeError, match="OKX_FILL_HIGH_WATER_MISMATCH"):
        service.plan()
    exchange.drop_trade_for = None
    _execute(pg, "UPDATE positions SET status='CLOSED' WHERE id=3079")
    with pytest.raises(RuntimeError, match="PARTIAL_REPAIR_STATE"):
        service.plan()
    _execute(pg, "UPDATE positions SET status='OPEN' WHERE id=3079")
    _execute(pg, "INSERT INTO canonical_financial_truth_v1(position_id,financial_truth_status,source_order_ids,source_fill_ids,authoritative_evidence) VALUES(3079,'UNKNOWN','[]','[]','{}')")
    with pytest.raises(RuntimeError, match="PARTIAL_REPAIR_STATE"):
        service.plan()
    _execute(pg, "DELETE FROM canonical_financial_truth_v1 WHERE position_id=3079")
    _execute(pg, "INSERT INTO position_lifecycle_events_c2_2(position_id,order_id,mutation_kind,mutation_high_water,payload) VALUES(3079,'exit-3079','POSITION_CLOSED',1,'{}')")
    with pytest.raises(RuntimeError, match="PARTIAL_REPAIR_STATE"):
        service.plan()
    _execute(pg, "DELETE FROM position_lifecycle_events_c2_2 WHERE position_id=3079")
    _execute(pg, "INSERT INTO learning_outcome_exclusion_v1(environment,deployment_id,position_id,exclusion_reason,source_type,semantic_fingerprint_v2,created_by,git_sha) VALUES('LIVE','local-live',3079,'LEGACY_REPAIR','LEGACY_POSITION_REPAIR',%s,'test',%s)", (fingerprints[3079], GIT_SHA))
    with pytest.raises(RuntimeError, match="PARTIAL_REPAIR_STATE"):
        service.plan()
    _execute(pg, "DELETE FROM learning_outcome_exclusion_v1 WHERE position_id=3079")
    wrong_order_manifest = replace(manifest, positions=(
        replace(manifest.positions[0], entry_order_id="unexpected"),
        *manifest.positions[1:],
    ))
    with pytest.raises(RuntimeError, match="ENTRY_ORDER_IDENTITY_MISMATCH"):
        _service(pg, exchange, wrong_order_manifest).plan()

    learning_before = (
        _scalar(pg, "SELECT count(*) FROM learning_feature_warehouse_v1"),
        _scalar(pg, "SELECT count(*) FROM decision_replay_v1"),
    )
    forbidden_before = (
        _scalar(pg, "SELECT count(*) FROM binance_orders WHERE id=ANY(%s)", ([3758, 3760, 3762],)),
        _scalar(pg, "SELECT count(*) FROM exchange_fill_ingestion_state_v2 WHERE ingestion_id=ANY(%s)", ([22, 23, 24, 25],)),
    )
    monkeypatch.setenv(APPLY_ENABLE_ENV, "1")
    stages = []

    def fail_after_exclusion(position_id, stage):
        stages.append((position_id, stage))
        if position_id == 3079 and stage == "exclusion":
            raise RuntimeError("INJECTED_FAILURE")

    with pytest.raises(RuntimeError, match="INJECTED_FAILURE"):
        service.apply(
            apply_requested=True, environment="LIVE", deployment_id="local-live",
            manifest_path="canonical-test-manifest.json", stage_hook=fail_after_exclusion,
        )
    assert _scalar(pg, "SELECT count(*) FROM learning_outcome_exclusion_v1") == 0
    assert _scalar(pg, "SELECT count(*) FROM canonical_financial_truth_v1") == 0
    assert _scalar(pg, "SELECT count(*) FROM positions WHERE status='CLOSED'") == 0
    assert not any(position_id != 3079 for position_id, _stage in stages)

    results = service.apply(
        apply_requested=True, environment="LIVE", deployment_id="local-live",
        manifest_path="canonical-test-manifest.json",
    )
    assert [row["status"] for row in results] == ["APPLIED"] * 7
    assert _scalar(pg, "SELECT count(*) FROM positions WHERE status='CLOSED'") == 7
    assert _scalar(pg, "SELECT count(*) FROM canonical_financial_truth_v1 WHERE financial_truth_status='COMPLETE'") == 7
    assert _scalar(pg, "SELECT count(*) FROM canonical_financial_truth_audit_v1") == 7
    assert _scalar(pg, "SELECT count(*) FROM learning_outcome_exclusion_v1") == 7
    assert _scalar(pg, "SELECT count(*) FROM position_lifecycle_events_c2_2 WHERE mutation_kind='POSITION_CLOSED'") == 4
    assert _scalar(pg, "SELECT count(*) FROM position_lifecycle_events_c2_2 WHERE mutation_kind='POSITION_CLOSED_TERMINAL_DUST'") == 3
    assert _scalar(pg, "SELECT count(*) FROM v_learning_eligible_closed_positions_v1") == 0
    assert learning_before == (
        _scalar(pg, "SELECT count(*) FROM learning_feature_warehouse_v1"),
        _scalar(pg, "SELECT count(*) FROM decision_replay_v1"),
    )
    assert forbidden_before == (
        _scalar(pg, "SELECT count(*) FROM binance_orders WHERE id=ANY(%s)", ([3758, 3760, 3762],)),
        _scalar(pg, "SELECT count(*) FROM exchange_fill_ingestion_state_v2 WHERE ingestion_id=ANY(%s)", ([22, 23, 24, 25],)),
    )
    assert _scalar(pg, "SELECT count(*) FROM binance_orders WHERE order_id='3789163681263689728' AND position_id IS NULL") == 1
    assert exchange.place_order_calls == exchange.cancel_order_calls == 0

    second = service.apply(
        apply_requested=True, environment="LIVE", deployment_id="local-live",
        manifest_path="canonical-test-manifest.json",
    )
    assert [row["status"] for row in second] == ["ALREADY_REPAIRED"] * 7
    assert all(row["writes"] == 0 for row in second)
    conflicting = dict(fingerprints)
    conflicting[3079] = "a" * 64
    with pytest.raises(RuntimeError, match="IDEMPOTENCY_CONFLICT"):
        _service(pg, exchange, _manifest(conflicting)).plan()

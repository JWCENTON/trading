from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import common.legacy_fill_equivalence_proof as proof_module
from common.exchange_fill_change_control import (
    authoritative_fill_fingerprint,
    authoritative_fill_payload,
)
from common.legacy_fill_equivalence_proof import (
    APPLY_ENABLE_ENV,
    EXPECTED_INGESTION_IDS,
    LegacyFillEquivalenceProofService,
    ManifestProof,
    ProofManifest,
    RuntimeIdentity,
)
from tests.postgres.database_baseline_fixture import disposable_postgres_v16


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT / "db/migrations/20260802_legacy_fill_equivalence_proof_v1.sql"
).read_text(encoding="utf-8")
GIT_SHA = "7" * 40
MAPPING = {
    8: 3084, 10: 3079, 12: 3082, 14: 3081,
    16: 3085, 18: 3085, 19: 3085, 20: 3085,
}


BASE_SCHEMA = """
CREATE TABLE schema_migration_ledger_v1(
  ledger_id BIGSERIAL PRIMARY KEY, migration_id TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  environment TEXT NOT NULL, deployment_id TEXT NOT NULL,
  database_name TEXT NOT NULL, applied_by TEXT NOT NULL, status TEXT NOT NULL,
  success BOOLEAN NOT NULL, execution_duration_ms BIGINT NOT NULL,
  git_sha TEXT NOT NULL, error_summary TEXT,
  schema_baseline_version TEXT NOT NULL
);
CREATE TABLE runtime_contract_adoption_v2(
  adoption_id BIGINT PRIMARY KEY, contract_name TEXT, environment TEXT,
  deployment_id TEXT, generation BIGINT, status TEXT, adopted_at TIMESTAMPTZ
);
INSERT INTO runtime_contract_adoption_v2 VALUES(
  1,'FEE_AWARE_INVENTORY_C2_2','live','local-live',1,'ACTIVE',
  '2026-07-29T22:42:28Z'
);
CREATE TABLE positions(
  id BIGINT PRIMARY KEY, entry_order_id TEXT, exit_order_id TEXT,
  entry_time TIMESTAMPTZ, inventory_contract_adoption_id BIGINT,
  inventory_contract_generation BIGINT
);
CREATE OR REPLACE FUNCTION is_existing_projected_c2_2_compatible(BIGINT,TEXT)
RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$ SELECT FALSE $$;
CREATE TABLE bot_control(id INTEGER PRIMARY KEY,live_orders_enabled BOOLEAN NOT NULL);
CREATE TABLE binance_orders(
  id BIGINT PRIMARY KEY,order_id TEXT,position_id BIGINT
);
CREATE TABLE binance_order_fills(
  id BIGINT PRIMARY KEY,source TEXT NOT NULL,order_id TEXT NOT NULL,
  symbol TEXT NOT NULL,side TEXT NOT NULL,executed_qty NUMERIC NOT NULL,
  avg_price NUMERIC NOT NULL,quote_notional_usdc NUMERIC NOT NULL,
  commission_amount NUMERIC NOT NULL,commission_asset TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,trade_id BIGINT NOT NULL,
  account_identity_id BIGINT NOT NULL
);
CREATE TABLE exchange_fill_ingestion_state_v2(
  ingestion_id BIGINT PRIMARY KEY,source TEXT NOT NULL,
  account_identity_key TEXT NOT NULL,symbol TEXT NOT NULL,trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,source_fingerprint TEXT NOT NULL,
  applied_fingerprint TEXT,applied_at TIMESTAMPTZ,
  application_status TEXT NOT NULL,correction_revision INTEGER NOT NULL,
  authoritative_payload JSONB NOT NULL,adoption_id BIGINT,
  contract_generation BIGINT,local_fill_id BIGINT
);
"""


class FakeExchange:
    place_order_calls = 0
    cancel_order_calls = 0

    def __init__(self, evidence):
        self.evidence = evidence
        self.pending = ()
        self.order_state = "filled"

    def pending_spot_orders(self):
        return tuple(self.pending)

    def order(self, symbol, order_id):
        rows = self.evidence[order_id]
        total = sum((Decimal(row["quantity"]) for row in rows), Decimal("0"))
        return {
            "order_id": order_id, "state": self.order_state, "side": "BUY",
            "executed_qty": str(total),
        }

    def fills(self, symbol, order_id):
        return tuple(self.evidence[order_id])


def _execute(pg, sql, params=()):
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
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


def _seed(pg):
    connection = pg.connect()
    evidence = {}
    proofs = []
    try:
        with connection.cursor() as cur:
            cur.execute(BASE_SCHEMA)
            cur.executemany(
                "INSERT INTO bot_control VALUES(%s,FALSE)",
                [(index,) for index in range(1, 33)],
            )
            orders_by_position = {}
            for position_id in sorted(set(MAPPING.values())):
                order_id = f"order-{position_id}"
                orders_by_position[position_id] = order_id
                cur.execute(
                    "INSERT INTO positions VALUES(%s,%s,%s,%s,NULL,NULL)",
                    (position_id, order_id, f"exit-{position_id}",
                     datetime(2026, 7, 1, tzinfo=timezone.utc)),
                )
                cur.execute(
                    "INSERT INTO binance_orders VALUES(%s,%s,%s)",
                    (5000 + position_id, order_id, position_id),
                )
                evidence[order_id] = []
            for ingestion_id, position_id in sorted(MAPPING.items()):
                order_id = orders_by_position[position_id]
                trade_id = str(900000 + ingestion_id)
                fill_id = 1000 + ingestion_id
                quantity = Decimal("0.01") + Decimal(ingestion_id) / Decimal("100000")
                price = Decimal("100") + Decimal(ingestion_id)
                fee = quantity * Decimal("0.0035")
                event = datetime(2026, 7, 2, 12, ingestion_id, tzinfo=timezone.utc)
                source = {
                    "source": "okx", "symbol": "BTCUSDC", "trade_id": trade_id,
                    "order_id": order_id, "side": "BUY", "executed_qty": quantity,
                    "avg_price": price, "commission_amount": fee,
                    "commission_asset": "BTC",
                    "event_time_ms": int(event.timestamp() * 1000),
                }
                payload = authoritative_fill_payload(source, account_identity_key="1")
                fingerprint = authoritative_fill_fingerprint(payload)
                cur.execute(
                    "INSERT INTO binance_order_fills VALUES("
                    "%s,'okx',%s,'BTCUSDC','BUY',%s,%s,%s,%s,'BTC',%s,%s,1)",
                    (fill_id, order_id, quantity, price, quantity * price, fee,
                     event, int(trade_id)),
                )
                cur.execute(
                    "INSERT INTO exchange_fill_ingestion_state_v2 VALUES("
                    "%s,'okx','1','BTCUSDC',%s,%s,%s,NULL,NULL,"
                    "'CORRECTION_PENDING',2,%s::jsonb,NULL,NULL,NULL)",
                    (ingestion_id, trade_id, order_id, fingerprint,
                     proof_module.canonical_json(payload)),
                )
                evidence[order_id].append({
                    "trade_id": trade_id, "order_id": order_id, "side": "BUY",
                    "quantity": str(quantity), "price": str(price),
                    "fee_quantity": str(fee), "fee_currency": "BTC",
                    "event_time_ms": str(int(event.timestamp() * 1000)),
                })
                proofs.append(ManifestProof(
                    ingestion_id, position_id, order_id, trade_id, fill_id, 2,
                    fingerprint,
                ))
            for order_row in (3758, 3760, 3762):
                cur.execute(
                    "INSERT INTO binance_orders VALUES(%s,%s,NULL)",
                    (order_row, f"forbidden-{order_row}"),
                )
            for ingestion_id in (22, 23, 24, 25):
                cur.execute(
                    "INSERT INTO exchange_fill_ingestion_state_v2 VALUES("
                    "%s,'okx','1','BNBUSDC',%s,'3789163681263689728',%s,NULL,"
                    "NULL,'OBSERVED_NOT_APPLIED',0,'{}',NULL,NULL,NULL)",
                    (ingestion_id, f"forbidden-{ingestion_id}", "f" * 64),
                )
        connection.commit()
    finally:
        connection.close()
    return evidence, ProofManifest(
        "LIVE", "local-live", pg.database,
        tuple(sorted(proofs, key=lambda item: item.ingestion_id)),
    )


def _service(pg, exchange, manifest):
    return LegacyFillEquivalenceProofService(
        pg.connect, exchange,
        RuntimeIdentity("OKX", "LIVE", "local-live", GIT_SHA, "PROCESS_SUPERVISOR"),
        manifest, expected_git_sha=GIT_SHA, expected_database=pg.database,
    )


def test_postgres_atomic_append_only_equivalence_contract(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_fill_equivalence_v1"
    disposable_postgres_v16.create_database(database)
    pg = replace(disposable_postgres_v16, database=database)
    monkeypatch.setattr(proof_module, "EXPECTED_DATABASE", database)
    evidence, manifest = _seed(pg)
    exchange = FakeExchange(evidence)
    service = _service(pg, exchange, manifest)

    missing_schema_plan = service.plan()
    assert missing_schema_plan.schema_status == "MISSING"
    assert missing_schema_plan.summary() == {
        "proof_candidates": 8, "equivalence_exact": 8,
        "repair_impact_NONE": 8, "blocked": 0, "unexpected": 0,
        "OKX_mutations": 0, "DB_mutations": 0,
    }

    _execute(pg, MIGRATION)
    assert service.plan().schema_status == "PRESENT"
    monkeypatch.setenv(APPLY_ENABLE_ENV, "1")
    immutable_before = (
        _scalar(pg, "SELECT json_agg(row_to_json(state) ORDER BY ingestion_id)::text "
                "FROM exchange_fill_ingestion_state_v2 state"),
        _scalar(pg, "SELECT json_agg(row_to_json(fill) ORDER BY id)::text "
                "FROM binance_order_fills fill"),
        _scalar(pg, "SELECT json_agg(row_to_json(position) ORDER BY id)::text "
                "FROM positions position"),
    )

    def fail_on_last(ingestion_id, stage):
        if ingestion_id == 20 and stage == "inserted":
            raise RuntimeError("INJECTED_FINAL_RECORD_FAILURE")

    with pytest.raises(RuntimeError, match="INJECTED_FINAL_RECORD_FAILURE"):
        service.apply(
            apply_requested=True, environment="LIVE", deployment_id="local-live",
            database=database, manifest_path="proof-manifest.json",
            stage_hook=fail_on_last,
        )
    assert _scalar(pg, "SELECT count(*) FROM legacy_fill_equivalence_proof_v1") == 0

    first = service.apply(
        apply_requested=True, environment="LIVE", deployment_id="local-live",
        database=database, manifest_path="proof-manifest.json",
    )
    assert first == {
        "proofs": 8, "inserted": 8, "idempotent_noop": 0, "status": "VALID",
    }
    assert immutable_before == (
        _scalar(pg, "SELECT json_agg(row_to_json(state) ORDER BY ingestion_id)::text "
                "FROM exchange_fill_ingestion_state_v2 state"),
        _scalar(pg, "SELECT json_agg(row_to_json(fill) ORDER BY id)::text "
                "FROM binance_order_fills fill"),
        _scalar(pg, "SELECT json_agg(row_to_json(position) ORDER BY id)::text "
                "FROM positions position"),
    )
    assert _scalar(
        pg, "SELECT count(*) FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE proof_status='VALID'",
    ) == 8
    assert _scalar(
        pg, "SELECT count(*) FROM legacy_fill_equivalence_proof_v1 "
        "WHERE ingestion_id=ANY(%s)", ([22, 23, 24, 25],),
    ) == 0

    second = service.apply(
        apply_requested=True, environment="LIVE", deployment_id="local-live",
        database=database, manifest_path="proof-manifest.json",
    )
    assert second == {
        "proofs": 8, "inserted": 0, "idempotent_noop": 8, "status": "VALID",
    }

    for statement in (
        "UPDATE legacy_fill_equivalence_proof_v1 SET repair_impact='NONE'",
        "DELETE FROM legacy_fill_equivalence_proof_v1",
        "TRUNCATE legacy_fill_equivalence_proof_v1",
    ):
        connection = pg.connect()
        try:
            with connection.cursor() as cur:
                with pytest.raises(Exception, match="append-only"):
                    cur.execute(statement)
            connection.rollback()
        finally:
            connection.close()

    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET correction_revision=3 "
        "WHERE ingestion_id=8",
    )
    assert _scalar(
        pg, "SELECT proof_status FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE ingestion_id=8",
    ) == "STALE_INGESTION_REVISION"
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET correction_revision=2 "
        "WHERE ingestion_id=8",
    )
    original_fingerprint = next(
        row.latest_observed_fingerprint for row in manifest.proofs
        if row.ingestion_id == 8
    )
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET source_fingerprint=%s "
        "WHERE ingestion_id=8", ("a" * 64,),
    )
    assert _scalar(
        pg, "SELECT proof_status FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE ingestion_id=8",
    ) == "STALE_OBSERVED_FINGERPRINT"
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET source_fingerprint=%s "
        "WHERE ingestion_id=8", (original_fingerprint,),
    )
    _execute(pg, "UPDATE binance_order_fills SET avg_price=avg_price+1 WHERE id=1008")
    assert _scalar(
        pg, "SELECT proof_status FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE ingestion_id=8",
    ) == "STALE_CANONICAL_FILL"
    _execute(pg, "UPDATE binance_order_fills SET avg_price=108 WHERE id=1008")
    _execute(pg, "UPDATE binance_order_fills SET order_id='identity-drift' WHERE id=1008")
    assert _scalar(
        pg, "SELECT proof_status FROM v_legacy_fill_equivalence_proof_status_v1 "
        "WHERE ingestion_id=8",
    ) == "IDENTITY_CONFLICT"
    _execute(pg, "UPDATE binance_order_fills SET order_id='order-3084' WHERE id=1008")

    exchange.evidence["order-3084"][0]["fee_quantity"] = "999"
    with pytest.raises(RuntimeError, match="EQUIVALENCE_PROOF_CONFLICT"):
        service.plan()
    assert exchange.place_order_calls == exchange.cancel_order_calls == 0


def test_postgres_fail_closed_preconditions(disposable_postgres_v16, monkeypatch):
    database = "waltrade_baseline_test_fill_equivalence_gates_v1"
    disposable_postgres_v16.create_database(database)
    pg = replace(disposable_postgres_v16, database=database)
    monkeypatch.setattr(proof_module, "EXPECTED_DATABASE", database)
    evidence, manifest = _seed(pg)
    exchange = FakeExchange(evidence)
    service = _service(pg, exchange, manifest)

    _execute(pg, "UPDATE bot_control SET live_orders_enabled=TRUE WHERE id=1")
    with pytest.raises(RuntimeError, match="LIVE_ORDERS_NOT_CONTAINED"):
        service.plan()
    _execute(pg, "UPDATE bot_control SET live_orders_enabled=FALSE WHERE id=1")

    exchange.pending = ({"ordId": "pending"},)
    with pytest.raises(RuntimeError, match="OKX_PENDING_SPOT_ORDERS"):
        service.plan()
    exchange.pending = ()

    _execute(
        pg, "INSERT INTO exchange_fill_ingestion_state_v2 VALUES("
        "99,'okx','1','BTCUSDC','unexpected','order-3084',%s,NULL,NULL,"
        "'CORRECTION_PENDING',1,'{}',NULL,NULL,NULL)", ("d" * 64,),
    )
    with pytest.raises(RuntimeError, match="UNEXPECTED_PROOF_COHORT"):
        service.plan()
    _execute(pg, "DELETE FROM exchange_fill_ingestion_state_v2 WHERE ingestion_id=99")

    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET application_status='APPLIED' "
        "WHERE ingestion_id=8",
    )
    with pytest.raises(RuntimeError, match="CORRECTION_STATUS_INVALID"):
        service.plan()
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 "
        "SET application_status='CORRECTION_PENDING' WHERE ingestion_id=8",
    )

    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET applied_fingerprint=%s "
        "WHERE ingestion_id=8", ("a" * 64,),
    )
    with pytest.raises(RuntimeError, match="NATIVE_APPLICATION_PROOF_PRESENT"):
        service.plan()
    _execute(
        pg, "UPDATE exchange_fill_ingestion_state_v2 SET applied_fingerprint=NULL "
        "WHERE ingestion_id=8",
    )

    _execute(
        pg, "UPDATE positions SET entry_time='2026-08-01T00:00:00Z' WHERE id=3084",
    )
    with pytest.raises(RuntimeError, match="ROW_GENERATION_NOT_LEGACY_UNPROJECTED"):
        service.plan()
    _execute(
        pg, "UPDATE positions SET entry_time='2026-07-01T00:00:00Z' WHERE id=3084",
    )

    _execute(
        pg, "INSERT INTO binance_order_fills SELECT 2008,source,order_id,symbol,"
        "side,executed_qty,avg_price,quote_notional_usdc,commission_amount,"
        "commission_asset,event_time,trade_id,account_identity_id "
        "FROM binance_order_fills WHERE id=1008",
    )
    with pytest.raises(RuntimeError, match="CANONICAL_FILL_IDENTITY_NOT_EXACT"):
        service.plan()
    _execute(pg, "DELETE FROM binance_order_fills WHERE id=2008")

    target = evidence["order-3084"][0]
    for field, changed in (
        ("quantity", "9"),
        ("price", "9"),
        ("fee_quantity", "9"),
        ("fee_currency", "USDC"),
        ("event_time_ms", "1"),
    ):
        original = target[field]
        target[field] = changed
        with pytest.raises(RuntimeError, match="EQUIVALENCE_PROOF_CONFLICT"):
            service.plan()
        target[field] = original

    exchange.order_state = "live"
    with pytest.raises(RuntimeError, match="OKX_ORDER_NOT_FILLED"):
        service.plan()
    exchange.order_state = "filled"

    _execute(pg, "DELETE FROM binance_order_fills WHERE id=1008")
    with pytest.raises(RuntimeError, match="CANONICAL_FILL_MISSING"):
        service.plan()

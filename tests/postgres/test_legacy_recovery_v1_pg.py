from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from psycopg2.extras import Json

from common.financial_truth_calculator import FinancialTruthCalculation
from common.legacy_recovery import (
    FeeValuationStatus,
    FillApplicationProof,
    IngestionApplicationStatus,
    LegacyFillEvidence,
    LegacyPositionEvidence,
    LegacyPositionRecomputationService,
    LegacyRecoveryTransactionService,
    OrderOwnership,
    PrecisionPolicy,
    RecoveryCandidate,
    UnappliedFillRecoveryService,
    classify_fill_application,
    semantic_repair_fingerprint,
)


ROOT = Path(__file__).resolve().parents[2]
D = Decimal


@pytest.fixture()
def recovery_db(disposable_postgres_v16):
    db = disposable_postgres_v16
    name = "waltrade_baseline_test_recovery"
    try:
        db.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    conn = db.connect(name)
    with conn.cursor() as cur:
        cur.execute(
            """
            DROP SCHEMA public CASCADE; CREATE SCHEMA public;
            CREATE TABLE positions(
              id BIGINT PRIMARY KEY,status TEXT NOT NULL,qty NUMERIC,
              inventory_evidence_status TEXT,gross_entry_executed_qty NUMERIC,
              entry_base_fee_qty NUMERIC,net_entry_inventory_qty NUMERIC,
              cumulative_exit_executed_qty NUMERIC,
              exit_inventory_reduction_qty NUMERIC,remaining_inventory_qty NUMERIC,
              terminal_dust_qty NUMERIC,terminal_reason TEXT,
              inventory_calculated_at TIMESTAMPTZ
            );
            CREATE TABLE binance_orders(
              id BIGSERIAL PRIMARY KEY,order_id TEXT UNIQUE,status TEXT,
              account_identity_id BIGINT,instrument_snapshot_id BIGINT,
              account_identity_status TEXT,account_identity_failure_code TEXT
            );
            CREATE TABLE binance_order_fills(
              id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,trade_id TEXT NOT NULL,
              order_id TEXT NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,
              executed_qty NUMERIC NOT NULL,avg_price NUMERIC NOT NULL,
              commission_amount NUMERIC,commission_asset TEXT,
              event_time TIMESTAMPTZ NOT NULL,raw JSONB NOT NULL,
              account_identity_id BIGINT,instrument_snapshot_id BIGINT,
              account_identity_status TEXT,account_identity_failure_code TEXT,
              UNIQUE(source,trade_id)
            );
            CREATE TABLE simulated_orders(id BIGINT PRIMARY KEY);
            CREATE TABLE position_lifecycle_events_c2_2(
              event_id BIGSERIAL PRIMARY KEY,
              position_id BIGINT NOT NULL REFERENCES positions(id),
              order_id TEXT NOT NULL,mutation_kind TEXT NOT NULL,
              mutation_high_water NUMERIC NOT NULL,payload JSONB NOT NULL,
              UNIQUE(position_id,order_id,mutation_kind,mutation_high_water)
            );
            CREATE TABLE exchange_fill_ingestion_state_v2(
              ingestion_id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,
              account_identity_key TEXT NOT NULL,symbol TEXT NOT NULL,
              trade_id TEXT NOT NULL,order_id TEXT NOT NULL,side TEXT NOT NULL,
              first_seen_at TIMESTAMPTZ DEFAULT clock_timestamp(),
              last_seen_at TIMESTAMPTZ DEFAULT clock_timestamp(),
              source_fingerprint TEXT NOT NULL,applied_fingerprint TEXT,
              applied_at TIMESTAMPTZ,application_status TEXT NOT NULL,
              correction_revision INTEGER DEFAULT 0,
              authoritative_payload JSONB NOT NULL,last_decision TEXT NOT NULL,
              UNIQUE(source,account_identity_key,symbol,trade_id),
              CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
                CHECK(application_status IN (
                  'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED',
                  'AMBIGUOUS','REJECTED'
                ))
            );
            """
        )
        cur.execute(
            (
                ROOT
                / "db/migrations/"
                "20260801_schema_migration_ledger_v1_baseline.sql"
            ).read_text()
        )
        cur.execute(
            (ROOT / "db/migrations/20260727_canonical_financial_truth_foundation_v1.sql").read_text()
        )
        cur.execute(
            (ROOT / "db/migrations/20260728_canonical_financial_truth_writer_v1.sql").read_text()
        )
        cur.execute(
            (ROOT / "db/migrations/20260730_legacy_position_fill_recovery_v1.sql").read_text()
        )
    conn.commit()
    yield conn
    conn.close()


def _fee(qty, asset, price):
    from common.legacy_recovery import value_fee
    return value_fee(
        quantity=D(qty), asset=asset, base_asset="BNB",
        quote_asset="USDC", fill_price=D(price),
    )


def _result():
    entry = LegacyFillEvidence(
        "entry", "entry-order", "entry-trade", "BUY", D("0.035152"),
        D("566.1"), D("0.000123032"), "BNB",
        _fee("0.000123032", "BNB", "566.1"),
    )
    exit_ = LegacyFillEvidence(
        "exit", "exit-order", "exit-trade", "SELL", D("0.035029"),
        D("567.1"), D("0.06952731065"), "USDC",
        _fee("0.06952731065", "USDC", "567.1"),
    )
    return LegacyPositionRecomputationService().recompute(
        LegacyPositionEvidence(
            3080, "BNBUSDC", "BNB", "USDC", (entry,), (exit_,),
            PrecisionPolicy(D("0.000001"), 8, 9, D("0.000000001"), "FIXTURE_LOT_SZ"),
        )
    )


def _calculation(result):
    now = datetime.now(timezone.utc)
    entry_notional = D("19.8995472")
    exit_notional = D("19.8649459")
    entry_fee = D("0.0696484152")
    exit_fee = D("0.06952731065")
    gross = exit_notional - entry_notional
    return FinancialTruthCalculation(
        3080, "CLOSED", "COMPLETE", result.gross_entry_qty,
        result.gross_exit_qty, result.base_asset_entry_fee_qty,
        result.base_asset_exit_fee_qty, result.net_entry_inventory_qty,
        result.gross_exit_qty + result.base_asset_exit_fee_qty,
        result.gross_entry_qty - result.gross_exit_qty, D("0"),
        entry_notional, exit_notional, entry_fee, exit_fee,
        entry_fee + exit_fee, gross, gross - entry_fee - exit_fee,
        None, None, None, 1, 1, now, now, now, now,
        "EXCHANGE_EXECUTION", "okx", "live", "fixture",
        "account-fixture", ("entry-order", "exit-order"),
        ("entry", "exit"), result.evidence_fingerprint,
        "FINANCIAL_TRUTH_CALCULATION_V2", None, None,
    )


def test_fixture_a_legacy_position_repair_api_requires_quarantine_context(
    recovery_db,
):
    with recovery_db.cursor() as cur:
        cur.execute("INSERT INTO positions VALUES (3080,'OPEN',0.000123)")
        cur.execute(
            "INSERT INTO binance_orders(order_id,status) VALUES "
            "('exit-order','PARTIALLY_FILLED')"
        )
    recovery_db.commit()
    result = _result()
    with pytest.raises(TypeError):
        LegacyRecoveryTransactionService.repair_position(
            recovery_db, result=result,
            expected_semantic_fingerprint=result.evidence_fingerprint,
            exit_order_ids=("exit-order",),
            financial_truth_calculation=_calculation(result),
            invocation_identity="fixture-a",
        )
    with recovery_db.cursor() as cur:
        cur.execute(
            "SELECT status,qty,remaining_inventory_qty FROM positions WHERE id=3080"
        )
        assert cur.fetchone() == ("OPEN", D("0.000123"), None)
        cur.execute(
            "SELECT financial_truth_status FROM canonical_financial_truth_v1 "
            "WHERE position_id=3080"
        )
        assert cur.fetchone() is None
        cur.execute("SELECT count(*) FROM legacy_repair_audit_v1")
        assert cur.fetchone()[0] == 0


def _candidate(ingestion_id, fingerprint, *, ownership=OrderOwnership.BOT_OWNED,
               position_id=4000, client="bot-client"):
    return RecoveryCandidate(
        ingestion_id, "okx", "BNBUSDC", f"trade-{ingestion_id}",
        f"order-{ingestion_id}", fingerprint, {"immutable": True}, ownership,
        position_id, True, client,
    )


def test_fixture_b_unapplied_fill_is_recovered_then_true_duplicate(recovery_db):
    fingerprint = semantic_repair_fingerprint({"qty": D("0.1"), "fee": D("0.001")})
    with recovery_db.cursor() as cur:
        cur.execute("INSERT INTO positions VALUES (4000,'OPEN',0.1)")
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,last_decision
            ) VALUES ('okx','acct','BNBUSDC','trade-1','order-1','BUY',%s,
              'OBSERVED_NOT_APPLIED','{}','NEW') RETURNING ingestion_id
            """,
            (fingerprint,),
        )
        ingestion_id = cur.fetchone()[0]
    recovery_db.commit()
    candidate = _candidate(ingestion_id, fingerprint)
    decision = UnappliedFillRecoveryService().classify(candidate)
    assert decision.status is IngestionApplicationStatus.OBSERVED_NOT_APPLIED
    assert LegacyRecoveryTransactionService.recover_fill(
        recovery_db, candidate=candidate, decision=decision,
        local_fill_values={
            "source": "okx", "trade_id": "trade-1", "order_id": "order-1",
            "symbol": "BNBUSDC", "side": "BUY", "executed_qty": D("0.1"),
            "avg_price": D("590"), "commission_amount": D("0.001"),
            "commission_asset": "BNB", "event_time": datetime.now(timezone.utc),
            "raw": Json({"immutable": True}),
        },
    )
    with recovery_db.cursor() as cur:
        cur.execute(
            "SELECT local_fill_id,applied_fingerprint,applied_at "
            "FROM exchange_fill_ingestion_state_v2 WHERE ingestion_id=%s",
            (ingestion_id,),
        )
        local_id, applied, applied_at = cur.fetchone()
        cur.execute(
            "SELECT execution_status FROM legacy_repair_audit_v1 "
            "WHERE incident_type='UNAPPLIED_FILL'"
        )
        assert cur.fetchone() == ("APPLIED",)
    proof = FillApplicationProof(
        "okx", "trade-1", "order-1", fingerprint,
        f"fill:{local_id}", applied, applied_at,
    )
    assert classify_fill_application(
        observed_fingerprint=fingerprint, proof=proof,
    ) is IngestionApplicationStatus.TRUE_DUPLICATE_APPLIED


def test_fixture_c_external_fill_is_classified_without_fabricated_position(recovery_db):
    fingerprint = semantic_repair_fingerprint({"qty": D("0.101163")})
    with recovery_db.cursor() as cur:
        for index, ownership in enumerate(("BOT_OWNED", "AMBIGUOUS", "BOT_OWNED")):
            buy_fingerprint = semantic_repair_fingerprint(
                {"qty": D("0.01") + D(index) / 1000, "ownership": ownership}
            )
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,account_identity_key,symbol,trade_id,order_id,side,
                  source_fingerprint,application_status,authoritative_payload,
                  last_decision,ownership_classification
                ) VALUES (
                  'okx','acct','BNBUSDC',%s,%s,'BUY',%s,
                  'BLOCKED_MISSING_CONTEXT','{}','NEW',%s
                )
                """,
                (f"ambiguous-buy-{index}", f"buy-order-{index}",
                 buy_fingerprint, ownership),
            )
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,last_decision
            ) VALUES ('okx','acct','BNBUSDC','external-trade',
              '3789163681263689728','SELL',%s,'OBSERVED_NOT_APPLIED','{}','NEW')
            RETURNING ingestion_id
            """,
            (fingerprint,),
        )
        ingestion_id = cur.fetchone()[0]
    recovery_db.commit()
    candidate = _candidate(
        ingestion_id, fingerprint, ownership=OrderOwnership.MANUAL_OR_EXTERNAL,
        position_id=None, client=None,
    )
    decision = UnappliedFillRecoveryService().classify(candidate)
    assert LegacyRecoveryTransactionService.recover_fill(
        recovery_db, candidate=candidate, decision=decision,
    )
    with recovery_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM positions")
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT application_status,classification_payload "
            "FROM exchange_fill_ingestion_state_v2 WHERE ingestion_id=%s",
            (ingestion_id,),
        )
        status, payload = cur.fetchone()
    assert status == "EXTERNAL_OR_MANUAL_UNLINKED"
    assert payload["client_order_id_present"] is False
    with recovery_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM exchange_fill_ingestion_state_v2 "
            "WHERE side='BUY' AND application_status='BLOCKED_MISSING_CONTEXT'"
        )
        assert cur.fetchone()[0] == 3


def test_fixture_d_operational_change_preserves_cas_economic_change_rolls_back(
    recovery_db, disposable_postgres_v16,
):
    fingerprint = semantic_repair_fingerprint({"qty": D("1"), "applied": None})
    with recovery_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchange_fill_ingestion_state_v2(
              source,account_identity_key,symbol,trade_id,order_id,side,
              source_fingerprint,application_status,authoritative_payload,last_decision
            ) VALUES ('okx','acct','BNBUSDC','trade-cas','order-cas','BUY',%s,
              'OBSERVED_NOT_APPLIED','{"qty":"1"}','NEW') RETURNING ingestion_id
            """,
            (fingerprint,),
        )
        ingestion_id = cur.fetchone()[0]
    recovery_db.commit()
    concurrent = disposable_postgres_v16.connect(
        "waltrade_baseline_test_recovery"
    )
    try:
        with concurrent.cursor() as cur:
            cur.execute(
                "UPDATE exchange_fill_ingestion_state_v2 "
                "SET last_seen_at=clock_timestamp() WHERE ingestion_id=%s",
                (ingestion_id,),
            )
        concurrent.commit()
        with recovery_db.cursor() as cur:
            cur.execute(
                "SELECT source_fingerprint FROM exchange_fill_ingestion_state_v2 "
                "WHERE ingestion_id=%s",
                (ingestion_id,),
            )
            assert cur.fetchone()[0] == fingerprint
        recovery_db.commit()
        with concurrent.cursor() as cur:
            cur.execute(
                "UPDATE exchange_fill_ingestion_state_v2 "
                "SET source_fingerprint=%s WHERE ingestion_id=%s",
                (
                    semantic_repair_fingerprint(
                        {"qty": D("2"), "applied": None}
                    ),
                    ingestion_id,
                ),
            )
        concurrent.commit()
    finally:
        concurrent.close()
    candidate = _candidate(ingestion_id, fingerprint)
    decision = UnappliedFillRecoveryService().classify(candidate)
    with pytest.raises(RuntimeError, match="SEMANTIC_CAS_CONFLICT"):
        LegacyRecoveryTransactionService.recover_fill(
            recovery_db, candidate=candidate, decision=decision,
            local_fill_values={
                "source": "okx", "trade_id": "trade-cas", "order_id": "order-cas",
                "symbol": "BNBUSDC", "side": "BUY", "executed_qty": D("1"),
                "avg_price": D("1"), "commission_amount": D("0"),
                "commission_asset": "USDC", "event_time": datetime.now(timezone.utc),
                "raw": Json({}),
            },
        )
    with recovery_db.cursor() as cur:
        cur.execute("SELECT count(*) FROM binance_order_fills WHERE trade_id='trade-cas'")
        assert cur.fetchone()[0] == 0

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.legacy_recovery import LegacyRecoveryTransactionService
from common.legacy_repair_quarantine import (
    ArtifactGateClassification,
    LearningArtifactRepository,
    LegacyPositionRepairPlanRepository,
    LegacyRepairQuarantineSchemaReadinessRepository,
)
from tools.legacy_recovery import main as cli_main


ROOT = Path(__file__).resolve().parents[2]
BASELINE = (
    ROOT / "db/migrations/20260801_schema_migration_ledger_v1_baseline.sql"
).read_text()
FINANCIAL_FOUNDATION = (
    ROOT / "db/migrations/20260727_canonical_financial_truth_foundation_v1.sql"
).read_text()
FINANCIAL_WRITER = (
    ROOT / "db/migrations/20260728_canonical_financial_truth_writer_v1.sql"
).read_text()
LEGACY = (
    ROOT / "db/migrations/20260730_legacy_position_fill_recovery_v1.sql"
).read_text()
QUARANTINE = (
    ROOT / "db/migrations/20260801_legacy_repair_learning_quarantine_v1.sql"
).read_text()
ARTIFACT_POLICY = (
    ROOT / "db/migrations/20260801_legacy_repair_existing_artifact_policy_v1.sql"
).read_text()

GIT_SHA = "2fc6efae2bf2a342ac4ea73968d47432d1a964b5"
DEPLOYMENT = "local-paper"


SCHEMA = """
CREATE TABLE positions(
  id BIGINT PRIMARY KEY,symbol TEXT NOT NULL,strategy TEXT NOT NULL,
  "interval" TEXT NOT NULL,status TEXT NOT NULL,side TEXT NOT NULL DEFAULT 'LONG',
  qty NUMERIC,entry_price NUMERIC,entry_time TIMESTAMPTZ DEFAULT now(),
  exit_price NUMERIC,exit_time TIMESTAMPTZ,exit_reason TEXT,
  entry_order_id TEXT,exit_order_id TEXT,
  inventory_evidence_status TEXT,gross_entry_executed_qty NUMERIC,
  entry_base_fee_qty NUMERIC,net_entry_inventory_qty NUMERIC,
  cumulative_exit_executed_qty NUMERIC,exit_inventory_reduction_qty NUMERIC,
  remaining_inventory_qty NUMERIC,terminal_dust_qty NUMERIC,
  terminal_reason TEXT,inventory_calculated_at TIMESTAMPTZ
);
CREATE TABLE binance_orders(
  id BIGSERIAL PRIMARY KEY,order_id TEXT UNIQUE,status TEXT,symbol TEXT,side TEXT,
  client_order_id TEXT,position_id BIGINT,reconciled_position_id BIGINT,
  strategy TEXT,"interval" TEXT,order_purpose TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),account_identity_id BIGINT,
  instrument_snapshot_id BIGINT,account_identity_status TEXT,
  account_identity_failure_code TEXT
);
CREATE TABLE financial_truth_instrument_snapshot_v1(
  id BIGINT PRIMARY KEY,source TEXT NOT NULL DEFAULT 'okx',
  symbol TEXT NOT NULL DEFAULT 'BNBUSDC',base_asset TEXT NOT NULL,
  quote_asset TEXT NOT NULL,tick_size NUMERIC NOT NULL DEFAULT 0.01,
  step_size NUMERIC NOT NULL,min_qty NUMERIC NOT NULL DEFAULT 0.000001,
  min_notional NUMERIC,price_precision INTEGER NOT NULL DEFAULT 2,
  quantity_precision INTEGER NOT NULL,metadata_fingerprint TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),raw JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE financial_truth_account_identity_v1(
  id BIGINT PRIMARY KEY,source TEXT NOT NULL DEFAULT 'okx',
  account_uid TEXT NOT NULL DEFAULT 'uid',account_main_uid TEXT,
  account_type TEXT,environment TEXT NOT NULL DEFAULT 'paper',
  identity_fingerprint TEXT NOT NULL,observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE binance_order_fills(
  id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,
  executed_qty NUMERIC NOT NULL,avg_price NUMERIC NOT NULL,
  quote_notional_usdc NUMERIC,commission_amount NUMERIC,
  commission_asset TEXT,commission_usdc NUMERIC,event_time TIMESTAMPTZ NOT NULL,
  raw JSONB NOT NULL DEFAULT '{}',account_identity_id BIGINT,
  instrument_snapshot_id BIGINT,account_identity_status TEXT,
  account_identity_failure_code TEXT,UNIQUE(source,trade_id)
);
CREATE TABLE simulated_orders(id BIGINT PRIMARY KEY);
CREATE TABLE position_lifecycle_events_c2_2(
  event_id BIGSERIAL PRIMARY KEY,position_id BIGINT NOT NULL REFERENCES positions(id),
  order_id TEXT NOT NULL,mutation_kind TEXT NOT NULL,
  mutation_high_water NUMERIC NOT NULL,payload JSONB NOT NULL,
  UNIQUE(position_id,order_id,mutation_kind,mutation_high_water)
);
CREATE TABLE exchange_fill_ingestion_state_v2(
  ingestion_id BIGSERIAL PRIMARY KEY,source TEXT NOT NULL,
  account_identity_key TEXT NOT NULL,symbol TEXT NOT NULL,trade_id TEXT NOT NULL,
  order_id TEXT NOT NULL,side TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  last_seen_at TIMESTAMPTZ DEFAULT clock_timestamp(),
  source_fingerprint TEXT NOT NULL,applied_fingerprint TEXT,applied_at TIMESTAMPTZ,
  application_status TEXT NOT NULL,correction_revision INTEGER DEFAULT 0,
  authoritative_payload JSONB NOT NULL,last_decision TEXT NOT NULL,
  UNIQUE(source,account_identity_key,symbol,trade_id),
  CONSTRAINT exchange_fill_ingestion_state_v2_application_status_check
    CHECK(application_status IN(
      'NEW','DUPLICATE','CORRECTION_PENDING','CORRECTION_APPLIED',
      'AMBIGUOUS','REJECTED'
    ))
);
CREATE TABLE exit_trace_v1(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT UNIQUE
);
CREATE TABLE exit_trace_v2(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT UNIQUE
);
CREATE TABLE exit_trace_v3(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT UNIQUE
);
CREATE TABLE learning_feedback_shadow_recommendations(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT,environment TEXT DEFAULT 'paper',
  decision_key TEXT,recommendation_type TEXT DEFAULT 'UNKNOWN',
  recommendation_action TEXT DEFAULT 'SHADOW_OBSERVE_ONLY',
  evidence JSONB DEFAULT '{}',created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE learning_feature_warehouse_v1(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT,environment TEXT DEFAULT 'paper',
  deployment_id TEXT DEFAULT 'legacy-unknown',decision_key TEXT,
  evidence_status TEXT DEFAULT 'UNKNOWN',net_pnl_usdc NUMERIC,
  exit_time TIMESTAMPTZ,causal_linkage_status TEXT DEFAULT 'LEGACY_NOT_ATTRIBUTABLE',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE decision_replay_v1(
  id BIGSERIAL PRIMARY KEY,position_id BIGINT,environment TEXT DEFAULT 'paper',
  deployment_id TEXT DEFAULT 'legacy-unknown',decision_key TEXT,
  replay_status TEXT DEFAULT 'UNKNOWN',exit_time TIMESTAMPTZ,
  causal_linkage_status TEXT DEFAULT 'LEGACY_NOT_ATTRIBUTABLE',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE decision_registry_v1(
  decision_id BIGSERIAL PRIMARY KEY,position_id BIGINT,
  environment TEXT DEFAULT 'paper',deployment_id TEXT DEFAULT 'LOCAL',
  legacy_decision_key TEXT,decision_type TEXT DEFAULT 'TRADE_EXECUTED',
  source_table TEXT DEFAULT 'positions',source_record_id TEXT,
  source_natural_key TEXT,decision_payload JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE decision_outcomes_v1(
  outcome_id BIGSERIAL PRIMARY KEY,position_id BIGINT,
  environment TEXT DEFAULT 'paper',deployment_id TEXT DEFAULT 'LOCAL',
  outcome_status TEXT DEFAULT 'UNKNOWN',learning_eligible BOOLEAN,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE OR REPLACE FUNCTION trg_capture_exit_trace_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status='CLOSED' THEN
    INSERT INTO exit_trace_v1(position_id) VALUES(NEW.id)
    ON CONFLICT(position_id) DO NOTHING;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER trg_capture_exit_trace_v1
AFTER UPDATE OF status ON positions FOR EACH ROW
EXECUTE FUNCTION trg_capture_exit_trace_v1();
"""


@pytest.fixture()
def quarantine_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_paper_legacy_quarantine"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(SCHEMA)
        cur.execute(BASELINE)
        cur.execute(FINANCIAL_FOUNDATION)
        cur.execute(FINANCIAL_WRITER)
        cur.execute(LEGACY)
        cur.execute(QUARANTINE)
        cur.execute(ARTIFACT_POLICY)
    connection.commit()
    yield connection
    connection.close()


def _seed_position(connection, position_id: int = 3080) -> None:
    entry_order = f"entry-{position_id}"
    exit_order = f"exit-{position_id}"
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_truth_instrument_snapshot_v1(
              id,base_asset,quote_asset,step_size,quantity_precision,
              metadata_fingerprint
            ) VALUES (%s,'BNB','USDC',0.000001,9,%s)
            """,
            (position_id, "a" * 64),
        )
        cur.execute(
            "INSERT INTO financial_truth_account_identity_v1("
            "id,identity_fingerprint) VALUES (%s,%s)",
            (position_id, "b" * 64),
        )
        cur.execute(
            """
            INSERT INTO positions(
              id,symbol,strategy,"interval",status,qty,entry_price,
              entry_order_id,exit_order_id
            ) VALUES (%s,'BNBUSDC','BBRANGE','1m','OPEN',0.035028968,
                      566.1,%s,%s)
            """,
            (position_id, entry_order, exit_order),
        )
        cur.execute(
            """
            INSERT INTO binance_orders(
              order_id,status,symbol,side,client_order_id,position_id,
              strategy,"interval",order_purpose
            ) VALUES
              (%s,'FILLED','BNBUSDC','BUY','bot-entry',%s,'BBRANGE','1m','ENTRY'),
              (%s,'PARTIALLY_FILLED','BNBUSDC','SELL','bot-exit',%s,'BBRANGE','1m','EXIT')
            """,
            (entry_order, position_id, exit_order, position_id),
        )
        cur.execute(
            """
            INSERT INTO binance_order_fills(
              source,trade_id,order_id,symbol,side,executed_qty,avg_price,
              commission_amount,commission_asset,event_time,
              account_identity_id,instrument_snapshot_id
            ) VALUES
              ('okx',%s,%s,'BNBUSDC','BUY',0.035152,566.1,
               0.000123032,'BNB',now()-interval '2 minutes',%s,%s),
              ('okx',%s,%s,'BNBUSDC','SELL',0.035028968,567.1,
               0.06952731065,'USDC',now()-interval '1 minute',%s,%s)
            """,
            (
                f"entry-trade-{position_id}", entry_order, position_id,
                position_id, f"exit-trade-{position_id}", exit_order,
                position_id, position_id,
            ),
        )
        for trade, order, side in (
            (f"entry-trade-{position_id}", entry_order, "BUY"),
            (f"exit-trade-{position_id}", exit_order, "SELL"),
        ):
            cur.execute(
                """
                INSERT INTO exchange_fill_ingestion_state_v2(
                  source,account_identity_key,symbol,trade_id,order_id,side,
                  source_fingerprint,applied_fingerprint,applied_at,
                  application_status,authoritative_payload,last_decision
                ) VALUES ('okx','account','BNBUSDC',%s,%s,%s,%s,%s,now(),
                          'APPLIED','{}','APPLIED')
                """,
                (trade, order, side, "c" * 64, "c" * 64),
            )
    connection.commit()


def _counts(connection, position_id: int) -> dict[str, int]:
    tables = (
        "learning_outcome_exclusion_v1", "position_lifecycle_events_c2_2",
        "canonical_financial_truth_v1", "legacy_repair_audit_v1",
        "legacy_repair_provenance_v1", "exit_trace_v1",
    )
    with connection.cursor() as cur:
        result = {}
        for table in tables:
            cur.execute(
                f"SELECT count(*) FROM {table} WHERE position_id=%s"
                if table not in {
                    "legacy_repair_audit_v1", "legacy_repair_provenance_v1"
                } else (
                    "SELECT count(*) FROM legacy_repair_audit_v1 "
                    "WHERE incident_identity=%s"
                    if table == "legacy_repair_audit_v1" else
                    "SELECT count(*) FROM legacy_repair_provenance_v1 "
                    "WHERE source_identity LIKE %s"
                ),
                (
                    (str(position_id) if table == "legacy_repair_audit_v1"
                     else f"%:position:{position_id}")
                    if table in {
                        "legacy_repair_audit_v1", "legacy_repair_provenance_v1"
                    } else position_id,
                ),
            )
            result[table] = int(cur.fetchone()[0])
        return result


def _seed_benign_artifacts(connection, position_id: int = 3080) -> None:
    decision_key = f"legacy-open-{position_id}"
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO learning_feedback_shadow_recommendations(
              position_id,environment,decision_key,recommendation_type,
              recommendation_action,evidence
            ) VALUES (%s,'trading_paper',%s,'OBSERVE_INCOMPLETE_PNL',
                      'SHADOW_OBSERVE_ONLY',%s)
            """,
            (position_id, decision_key, json.dumps({"position_id": position_id})),
        )
        cur.execute(
            """
            INSERT INTO learning_feature_warehouse_v1(
              position_id,environment,deployment_id,decision_key,
              evidence_status,causal_linkage_status
            ) VALUES (%s,'trading_paper','legacy-unknown',%s,
                      'OPEN_OR_INCOMPLETE','LEGACY_NOT_ATTRIBUTABLE')
            """,
            (position_id, decision_key),
        )
        cur.execute(
            """
            INSERT INTO decision_replay_v1(
              position_id,environment,deployment_id,decision_key,
              replay_status,causal_linkage_status
            ) VALUES (%s,'trading_paper','legacy-unknown',%s,
                      'REPLAY_OPEN_OR_INCOMPLETE','LEGACY_NOT_ATTRIBUTABLE')
            """,
            (position_id, decision_key),
        )
        cur.execute(
            """
            INSERT INTO decision_registry_v1(
              position_id,environment,deployment_id,legacy_decision_key,
              source_record_id,source_natural_key,decision_payload
            ) VALUES (%s,'trading_paper','LOCAL',%s,%s,%s,%s)
            """,
            (
                position_id, decision_key, str(position_id),
                f"LOCAL|trading_paper|positions|{position_id}|TRADE_EXECUTED",
                json.dumps({"position_status": "OPEN", "exit_time": None}),
            ),
        )
    connection.commit()


def test_migration_is_idempotent_and_schema_ready(quarantine_db):
    with quarantine_db.cursor() as cur:
        cur.execute(QUARANTINE)
        cur.execute(ARTIFACT_POLICY)
    quarantine_db.commit()
    readiness = LegacyRepairQuarantineSchemaReadinessRepository().check(
        quarantine_db
    )
    assert readiness.status == "PRESENT_VALID", readiness.issues


def test_artifact_gate_no_artifacts_allows_repair(quarantine_db):
    _seed_position(quarantine_db)
    with quarantine_db.cursor() as cur:
        gate = LearningArtifactRepository.classify(
            cur, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
        )
    assert gate.classification is ArtifactGateClassification.NO_ARTIFACTS
    assert gate.repair_allowed is True
    assert gate.artifacts == ()


def test_artifact_gate_explicit_benign_cohort_allows_repair(quarantine_db):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        gate = LearningArtifactRepository.classify(
            cur, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
        )
    assert gate.classification is (
        ArtifactGateClassification.BENIGN_OPEN_INCOMPLETE_ARTIFACTS
    )
    assert gate.repair_allowed is True
    assert [artifact["status"] for artifact in gate.artifacts] == [
        "OBSERVE_INCOMPLETE_PNL", "OPEN_OR_INCOMPLETE",
        "REPLAY_OPEN_OR_INCOMPLETE", "OPEN",
    ]


def test_exclusion_is_visible_to_close_trigger_and_normal_close_is_preserved(
    quarantine_db,
):
    with quarantine_db.cursor() as cur:
        cur.execute(
            "INSERT INTO positions(id,symbol,strategy,\"interval\",status,qty) "
            "VALUES (1,'BNBUSDC','BBRANGE','1m','OPEN',1),"
            "(2,'BNBUSDC','BBRANGE','1m','OPEN',1)"
        )
        cur.execute(
            """
            INSERT INTO learning_outcome_exclusion_v1(
              environment,deployment_id,position_id,exclusion_reason,
              source_type,semantic_fingerprint_v2,created_by,git_sha
            ) VALUES ('PAPER','local-paper',1,'LEGACY_REPAIR',
              'LEGACY_POSITION_REPAIR',%s,'TEST',%s)
            """,
            ("d" * 64, GIT_SHA),
        )
        cur.execute(
            "UPDATE positions SET status='CLOSED',exit_time=clock_timestamp() "
            "WHERE id IN (1,2)"
        )
        cur.execute("SELECT position_id FROM exit_trace_v1 ORDER BY position_id")
        assert cur.fetchall() == [(2,)]
        cur.execute(
            "SELECT id FROM v_learning_eligible_closed_positions_v1 ORDER BY id"
        )
        assert cur.fetchall() == [(2,)]
    quarantine_db.rollback()


def test_all_ingress_guards_exclude_shadow_warehouse_replay_and_decisions(
    quarantine_db,
):
    with quarantine_db.cursor() as cur:
        cur.execute(
            "INSERT INTO positions(id,symbol,strategy,\"interval\",status,qty) "
            "VALUES (11,'BNBUSDC','BBRANGE','1m','CLOSED',0),"
            "(12,'BNBUSDC','BBRANGE','1m','CLOSED',0)"
        )
    quarantine_db.commit()
    _seed_benign_artifacts(quarantine_db, 11)
    with quarantine_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO learning_outcome_exclusion_v1(
              environment,deployment_id,position_id,exclusion_reason,
              source_type,semantic_fingerprint_v2,created_by,git_sha
            ) VALUES ('PAPER','local-paper',11,'LEGACY_REPAIR',
              'LEGACY_POSITION_REPAIR',%s,'TEST',%s)
            """,
            ("e" * 64, GIT_SHA),
        )
        for table in ("exit_trace_v1", "exit_trace_v2", "exit_trace_v3"):
            cur.execute(
                f"INSERT INTO {table}(position_id) "
                "SELECT id FROM positions WHERE status='CLOSED'"
            )
        cur.execute(
            "INSERT INTO learning_feedback_shadow_recommendations(position_id) "
            "SELECT id FROM positions WHERE status='CLOSED'"
        )
        cur.execute(
            "INSERT INTO learning_feature_warehouse_v1(position_id) "
            "SELECT position_id "
            "FROM learning_feedback_shadow_recommendations"
        )
        cur.execute(
            "INSERT INTO decision_replay_v1(position_id) "
            "SELECT position_id FROM learning_feature_warehouse_v1"
        )
        for table in ("decision_registry_v1", "decision_outcomes_v1"):
            cur.execute(
                f"INSERT INTO {table}(position_id) "
                "SELECT id FROM positions WHERE status='CLOSED'"
            )
        direct_terminal = (
            "exit_trace_v1", "exit_trace_v2", "exit_trace_v3",
            "decision_outcomes_v1",
        )
        historical_benign = (
            "learning_feedback_shadow_recommendations",
            "learning_feature_warehouse_v1", "decision_replay_v1",
            "decision_registry_v1",
        )
        for table in direct_terminal:
            cur.execute(f"SELECT position_id FROM {table} ORDER BY position_id")
            assert cur.fetchall() == [(12,)], table
        for table in historical_benign:
            cur.execute(f"SELECT position_id FROM {table} ORDER BY position_id")
            assert cur.fetchall() == [(11,), (12,)], table
        for _table, view in (
            ("exit_trace_v1", "v_learning_eligible_exit_trace_v1"),
            ("exit_trace_v2", "v_learning_eligible_exit_trace_v2"),
            ("exit_trace_v3", "v_learning_eligible_exit_trace_v3"),
            (
                "learning_feedback_shadow_recommendations",
                "v_learning_eligible_shadow_recommendations_v1",
            ),
            (
                "learning_feature_warehouse_v1",
                "v_learning_eligible_feature_warehouse_v1",
            ),
            ("decision_replay_v1", "v_learning_eligible_decision_replay_v1"),
            (
                "decision_registry_v1",
                "v_learning_eligible_decision_registry_v1",
            ),
            (
                "decision_outcomes_v1",
                "v_learning_eligible_decision_outcomes_v1",
            ),
        ):
            cur.execute(f"SELECT position_id FROM {view} ORDER BY position_id")
            assert cur.fetchall() == [(12,)], view
        for table in (
            "exit_trace_v1", "exit_trace_v2", "exit_trace_v3",
            "learning_feedback_shadow_recommendations",
            "learning_feature_warehouse_v1", "decision_replay_v1",
            "decision_registry_v1", "decision_outcomes_v1",
        ):
            cur.execute(
                f"SELECT count(*) FROM {table} WHERE position_id=11"
            )
            expected = 0 if table in direct_terminal else 1
            assert cur.fetchone()[0] == expected, table
    quarantine_db.rollback()


def test_successful_repair_is_atomic_quarantined_and_idempotent(quarantine_db):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        artifacts_before = LearningArtifactRepository.snapshot(cur, 3080)
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert plan.eligible, plan.blocking_reasons
    assert plan.artifact_gate.classification is (
        ArtifactGateClassification.BENIGN_OPEN_INCOMPLETE_ARTIFACTS
    )
    first = LegacyRecoveryTransactionService.repair_position(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
        expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
        git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
    )
    assert first["status"] == "APPLIED"
    assert first["learning_excluded"] is True
    with quarantine_db.cursor() as cur:
        cur.execute(
            "SELECT status,remaining_inventory_qty FROM positions WHERE id=3080"
        )
        assert cur.fetchone() == ("CLOSED", 0)
        cur.execute(
            "SELECT financial_truth_status FROM canonical_financial_truth_v1 "
            "WHERE position_id=3080"
        )
        assert cur.fetchone() == ("COMPLETE",)
        assert LearningArtifactRepository.snapshot(cur, 3080) == artifacts_before
        LearningArtifactRepository.assert_excluded_from_readers(cur, 3080)
        cur.execute(
            "SELECT immutable_payload FROM legacy_repair_provenance_v1 "
            "WHERE source_identity LIKE %s",
            ("%:position:3080",),
        )
        provenance = cur.fetchone()[0]
        assert provenance["learning_artifact_gate"]["classification"] == (
            "BENIGN_OPEN_INCOMPLETE_ARTIFACTS"
        )
        assert len(provenance["learning_artifact_gate"]["artifacts"]) == 4
    assert _counts(quarantine_db, 3080) == {
        "learning_outcome_exclusion_v1": 1,
        "position_lifecycle_events_c2_2": 1,
        "canonical_financial_truth_v1": 1,
        "legacy_repair_audit_v1": 1,
        "legacy_repair_provenance_v1": 1,
        "exit_trace_v1": 0,
    }
    before = _counts(quarantine_db, 3080)
    second = LegacyRecoveryTransactionService.repair_position(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
        expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
        git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
    )
    assert second["status"] == "ALREADY_APPLIED"
    assert second["writes"] == 0
    assert _counts(quarantine_db, 3080) == before
    with quarantine_db.cursor() as cur:
        assert LearningArtifactRepository.snapshot(cur, 3080) == artifacts_before


def test_plan_v2_fingerprint_is_deterministic(quarantine_db):
    _seed_position(quarantine_db)
    first = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    quarantine_db.rollback()
    second = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert second.semantic_fingerprint_v2 == first.semantic_fingerprint_v2
    assert second.invocation_identity == first.invocation_identity


@pytest.mark.parametrize(
    "stage",
    ["exclusion", "position_update", "lifecycle", "financial_truth", "audit", "provenance"],
)
def test_failpoints_roll_back_every_partial_write(quarantine_db, stage):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        artifacts_before = LearningArtifactRepository.snapshot(cur, 3080)
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )

    def fail(current):
        if current == stage:
            raise RuntimeError(f"TEST_FAILPOINT:{stage}")

    with pytest.raises(RuntimeError, match="TEST_FAILPOINT"):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
            stage_hook=fail,
        )
    assert _counts(quarantine_db, 3080) == {
        "learning_outcome_exclusion_v1": 0,
        "position_lifecycle_events_c2_2": 0,
        "canonical_financial_truth_v1": 0,
        "legacy_repair_audit_v1": 0,
        "legacy_repair_provenance_v1": 0,
        "exit_trace_v1": 0,
    }
    with quarantine_db.cursor() as cur:
        cur.execute("SELECT status FROM positions WHERE id=3080")
        assert cur.fetchone() == ("OPEN",)
        assert LearningArtifactRepository.snapshot(cur, 3080) == artifacts_before


@pytest.mark.parametrize("artifact", ["exit_trace_v1", "decision_outcomes_v1"])
def test_terminal_learning_artifact_blocks_with_zero_writes(quarantine_db, artifact):
    _seed_position(quarantine_db)
    with quarantine_db.cursor() as cur:
        cur.execute(f"INSERT INTO {artifact}(position_id) VALUES (3080)")
    quarantine_db.commit()
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    with pytest.raises(
        RuntimeError, match="LEARNING_TERMINAL_OR_AMBIGUOUS_ARTIFACT"
    ):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    with quarantine_db.cursor() as cur:
        cur.execute("SELECT status FROM positions WHERE id=3080")
        assert cur.fetchone() == ("OPEN",)
        cur.execute("SELECT count(*) FROM learning_outcome_exclusion_v1")
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize(
    "artifact",
    [
        "learning_feedback_shadow_recommendations",
        "learning_feature_warehouse_v1", "decision_replay_v1",
        "decision_registry_v1",
    ],
)
def test_unknown_or_terminal_status_is_blocked(quarantine_db, artifact):
    _seed_position(quarantine_db)
    with quarantine_db.cursor() as cur:
        cur.execute(f"INSERT INTO {artifact}(position_id) VALUES (3080)")
    quarantine_db.commit()
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert plan.artifact_gate.classification is (
        ArtifactGateClassification.TERMINAL_OR_AMBIGUOUS_ARTIFACTS
    )
    assert plan.artifact_gate.repair_allowed is False
    with pytest.raises(
        RuntimeError, match="LEARNING_TERMINAL_OR_AMBIGUOUS_ARTIFACT"
    ):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    assert _counts(quarantine_db, 3080)["learning_outcome_exclusion_v1"] == 0


def test_duplicate_benign_artifact_is_ambiguous_and_blocked(quarantine_db):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO learning_feedback_shadow_recommendations(
              position_id,environment,decision_key,recommendation_type
            ) VALUES (3080,'trading_paper','legacy-open-3080',
                      'OBSERVE_INCOMPLETE_PNL')
            """
        )
    quarantine_db.commit()
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert plan.artifact_gate.classification is (
        ArtifactGateClassification.TERMINAL_OR_AMBIGUOUS_ARTIFACTS
    )
    assert plan.artifact_gate.reason == "DUPLICATE_ARTIFACT:shadow_recommendation"


def test_benign_status_with_terminal_shadow_evidence_is_blocked(quarantine_db):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        cur.execute(
            "UPDATE learning_feedback_shadow_recommendations "
            "SET evidence=evidence || '{\"net_pnl_usdc\":\"1.0\"}'::jsonb "
            "WHERE position_id=3080"
        )
    quarantine_db.commit()
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert plan.artifact_gate.repair_allowed is False
    assert plan.artifact_gate.reason == "SHADOW_TERMINAL_EVIDENCE"


@pytest.mark.parametrize(
    "column,value",
    [("environment", "trading_live"), ("deployment_id", "VPS")],
)
def test_artifact_identity_mismatch_is_blocked(quarantine_db, column, value):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    with quarantine_db.cursor() as cur:
        cur.execute(
            f"UPDATE learning_feature_warehouse_v1 SET {column}=%s "
            "WHERE position_id=3080",
            (value,),
        )
    quarantine_db.commit()
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    assert plan.artifact_gate.repair_allowed is False
    assert "MISMATCH" in str(plan.artifact_gate.reason)


def test_terminal_artifact_appearing_after_plan_makes_plan_stale(quarantine_db):
    _seed_position(quarantine_db)
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    with quarantine_db.cursor() as cur:
        cur.execute("INSERT INTO decision_outcomes_v1(position_id) VALUES (3080)")
    quarantine_db.commit()
    with pytest.raises(RuntimeError, match="PLAN_STALE"):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    assert _counts(quarantine_db, 3080)["learning_outcome_exclusion_v1"] == 0


@pytest.mark.parametrize("changed_evidence", ["fill", "position"])
def test_plan_stale_and_live_are_rejected_without_writes(
    quarantine_db, changed_evidence,
):
    _seed_position(quarantine_db)
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    with quarantine_db.cursor() as cur:
        if changed_evidence == "fill":
            cur.execute("UPDATE binance_order_fills SET avg_price=568 WHERE id=("
                        "SELECT max(id) FROM binance_order_fills)")
        else:
            cur.execute("UPDATE positions SET qty=qty-0.000001 WHERE id=3080")
    quarantine_db.commit()
    with pytest.raises(RuntimeError, match="PLAN_STALE"):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    with pytest.raises(RuntimeError, match="LIVE_APPLY_NOT_AUTHORIZED"):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="LIVE",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    assert _counts(quarantine_db, 3080)["learning_outcome_exclusion_v1"] == 0


def test_missing_quarantine_schema_blocks_with_zero_business_writes(
    quarantine_db,
):
    _seed_position(quarantine_db)
    plan = LegacyPositionRepairPlanRepository.build(
        quarantine_db, position_id=3080, environment="PAPER",
        deployment_id=DEPLOYMENT,
    )
    with quarantine_db.cursor() as cur:
        cur.execute("DROP TABLE learning_outcome_exclusion_v1 CASCADE")
    quarantine_db.commit()
    with pytest.raises(RuntimeError, match="SCHEMA_NOT_READY:QUARANTINE_COLUMN"):
        LegacyRecoveryTransactionService.repair_position(
            quarantine_db, position_id=3080, environment="PAPER",
            deployment_id=DEPLOYMENT,
            expected_semantic_fingerprint_v2=plan.semantic_fingerprint_v2,
            git_sha=GIT_SHA, invocation_identity=plan.invocation_identity,
        )
    with quarantine_db.cursor() as cur:
        cur.execute("SELECT status FROM positions WHERE id=3080")
        assert cur.fetchone() == ("OPEN",)
        for table in (
            "position_lifecycle_events_c2_2", "canonical_financial_truth_v1",
            "legacy_repair_audit_v1", "legacy_repair_provenance_v1",
        ):
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == 0, table


def test_cli_confirmation_live_and_plan_hash_contract(
    quarantine_db, disposable_postgres_v16, monkeypatch, capsys,
):
    _seed_position(quarantine_db)
    _seed_benign_artifacts(quarantine_db)
    dsn = (
        f"host=127.0.0.1 port={disposable_postgres_v16.port} "
        "dbname=waltrade_baseline_test_paper_legacy_quarantine "
        f"user={disposable_postgres_v16.user} "
        f"password={disposable_postgres_v16.password}"
    )
    monkeypatch.setenv("LEGACY_QUARANTINE_TEST_DSN", dsn)
    base = [
        "--database-url-env", "LEGACY_QUARANTINE_TEST_DSN",
        "--expected-database", "waltrade_baseline_test_paper_legacy_quarantine",
        "--environment", "PAPER", "--deployment-id", DEPLOYMENT,
    ]
    assert cli_main(base + ["plan-position", "--position-id", "3080"]) == 0
    planned = json.loads(capsys.readouterr().out)
    assert planned["eligible"] is True
    assert len(planned["semantic_fingerprint_v2"]) == 64
    assert planned["learning_eligible"] is False
    assert planned["learning_artifact_gate"]["classification"] == (
        "BENIGN_OPEN_INCOMPLETE_ARTIFACTS"
    )
    assert planned["learning_artifact_gate"]["repair_allowed"] is True
    assert len(planned["learning_artifact_gate"]["artifacts"]) == 4
    apply = base + [
        "--git-sha", GIT_SHA, "apply-position", "--position-id", "3080",
        "--expected-fingerprint-v2", planned["semantic_fingerprint_v2"],
    ]
    assert cli_main(apply) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == "CONFIRM_APPLY_REQUIRED"
    live = [item if item != "PAPER" else "LIVE" for item in apply]
    assert cli_main(live + ["--confirm-apply"]) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == "LIVE_APPLY_NOT_AUTHORIZED"
    assert cli_main(apply + ["--confirm-apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["status"] == "APPLIED"
    assert applied["learning_excluded"] is True
    assert cli_main(apply + ["--confirm-apply"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "ALREADY_APPLIED"
    assert _counts(quarantine_db, 3080)["learning_outcome_exclusion_v1"] == 1

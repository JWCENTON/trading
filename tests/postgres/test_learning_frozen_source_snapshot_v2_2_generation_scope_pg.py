from __future__ import annotations

from pathlib import Path

import pytest
from psycopg2 import errors


ROOT = Path(__file__).resolve().parents[2]
V1 = (ROOT / "db/migrations/20260710_learning_feedback_engine_v1.sql").read_text()
V1_1 = (ROOT / "db/migrations/20260710_learning_feedback_engine_v1_1.sql").read_text()
V1_2 = (
    ROOT / "db/migrations/20260710_learning_feedback_engine_v1_2_automation.sql"
).read_text()
V1_3 = (
    ROOT / "db/migrations/20260710_learning_feedback_engine_v1_3_validation.sql"
).read_text()
V2_1 = (
    ROOT
    / "db/migrations/20260724_learning_frozen_source_snapshot_v2_1_payload_propagation.sql"
).read_text()
V2_2 = (
    ROOT
    / "db/migrations/20260804_learning_frozen_source_snapshot_v2_2_generation_scoped_projection.sql"
).read_text()

TOKENS = {
    "A": "11111111-1111-4111-8111-111111111111",
    "B": "22222222-2222-4222-8222-222222222222",
    "C": "33333333-3333-4333-8333-333333333333",
    "D": "44444444-4444-4444-8444-444444444444",
}

BOOTSTRAP = r"""
CREATE TABLE automation_kv(
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE v_decision_intelligence_v1(
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    decision_key TEXT NOT NULL,
    net_pnl_usdc NUMERIC,
    recommendation_type TEXT,
    recommendation_action TEXT,
    missing_context_count INTEGER,
    refreshed_at TIMESTAMPTZ NOT NULL,
    decision_lifecycle_status TEXT NOT NULL,
    has_pnl BOOLEAN NOT NULL
);
"""

FROZEN_SOURCE = r"""
CREATE TABLE learning_canonical_source_snapshots_v2(
    snapshot_token UUID PRIMARY KEY,
    snapshot_status TEXT NOT NULL,
    feedback_run_id BIGINT NOT NULL UNIQUE
);

ALTER TABLE learning_slot_statistics_v1
    ADD COLUMN source_snapshot_token UUID
        REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
ALTER TABLE learning_calibration_proposals_v1
    ADD COLUMN source_snapshot_token UUID
        REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);
ALTER TABLE learning_proposal_observations_v1
    ADD COLUMN source_snapshot_token UUID
        REFERENCES learning_canonical_source_snapshots_v2(snapshot_token);

CREATE OR REPLACE FUNCTION propagate_learning_source_snapshot_token_v2()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_token_text TEXT := current_setting(
        'waltrade.learning_source_snapshot_token', true
    );
    v_token UUID;
BEGIN
    IF v_token_text IS NULL OR v_token_text = '' THEN
        RETURN NEW;
    END IF;
    v_token := v_token_text::UUID;
    PERFORM 1 FROM learning_canonical_source_snapshots_v2
     WHERE snapshot_token = v_token AND snapshot_status = 'COMPLETE';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'LEARNING_FROZEN_SOURCE_CONTEXT_MISSING';
    END IF;
    IF NEW.source_snapshot_token IS NOT NULL
       AND NEW.source_snapshot_token <> v_token THEN
        RAISE EXCEPTION
            'LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT table=%', TG_TABLE_NAME;
    END IF;
    NEW.source_snapshot_token := v_token;
    RETURN NEW;
END;
$$;

CREATE TRIGGER propagate_learning_source_snapshot_v2
BEFORE INSERT OR UPDATE ON learning_slot_statistics_v1
FOR EACH ROW EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2();
CREATE TRIGGER propagate_learning_source_snapshot_v2
BEFORE INSERT OR UPDATE ON learning_calibration_proposals_v1
FOR EACH ROW EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2();
CREATE TRIGGER propagate_learning_source_snapshot_v2
BEFORE INSERT OR UPDATE ON learning_proposal_observations_v1
FOR EACH ROW EXECUTE FUNCTION propagate_learning_source_snapshot_token_v2();

CREATE TABLE schema_migration_ledger_v1(
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

CREATE TABLE learning_canonical_evidence_selection_v1(
    feedback_run_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    source_snapshot_token UUID NOT NULL,
    decisions INTEGER NOT NULL,
    PRIMARY KEY(feedback_run_id,symbol)
);
CREATE TABLE learning_evidence_manifests_v1(
    manifest_id BIGSERIAL PRIMARY KEY,
    feedback_run_id BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    source_snapshot_token UUID NOT NULL,
    decisions INTEGER NOT NULL,
    UNIQUE(feedback_run_id,symbol)
);
CREATE TABLE learning_evidence_membership_v1(
    manifest_id BIGINT NOT NULL REFERENCES learning_evidence_manifests_v1,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY(manifest_id,ordinal)
);
CREATE TABLE learning_evidence_aggregates_v1(
    manifest_id BIGINT PRIMARY KEY REFERENCES learning_evidence_manifests_v1,
    decisions INTEGER NOT NULL
);

CREATE OR REPLACE FUNCTION capture_learning_evidence_manifests_v1(
    p_feedback_run_id BIGINT
) RETURNS JSONB LANGUAGE plpgsql AS $$
DECLARE
    v_observation RECORD;
    v_manifest_id BIGINT;
BEGIN
    FOR v_observation IN
        SELECT o.*, s.sample_from, s.decisions AS statistics_decisions
          FROM learning_proposal_observations_v1 o
          JOIN learning_slot_statistics_v1 s USING (environment, symbol, interval, strategy, window_days)
         WHERE o.refresh_run_id = p_feedback_run_id
    LOOP
        INSERT INTO learning_canonical_evidence_selection_v1(
            feedback_run_id,symbol,source_snapshot_token,decisions
        ) VALUES (
            p_feedback_run_id,v_observation.symbol,
            v_observation.source_snapshot_token,v_observation.statistics_decisions
        ) ON CONFLICT DO NOTHING;
        INSERT INTO learning_evidence_manifests_v1(
            feedback_run_id,symbol,source_snapshot_token,decisions
        ) VALUES (
            p_feedback_run_id,v_observation.symbol,
            v_observation.source_snapshot_token,v_observation.statistics_decisions
        ) ON CONFLICT DO NOTHING RETURNING manifest_id INTO v_manifest_id;
        IF v_manifest_id IS NOT NULL THEN
            INSERT INTO learning_evidence_membership_v1(manifest_id,ordinal)
            SELECT v_manifest_id, ordinal
              FROM generate_series(1,v_observation.statistics_decisions) ordinal;
            INSERT INTO learning_evidence_aggregates_v1(manifest_id,decisions)
            VALUES (v_manifest_id,v_observation.statistics_decisions);
        END IF;
    END LOOP;
    RETURN jsonb_build_object('status', 'ok');
END;
$$;
"""


@pytest.fixture(scope="module")
def generation_db(disposable_postgres_v16):
    name = "waltrade_baseline_test_learning_frozen_v2_2"
    try:
        disposable_postgres_v16.create_database(name)
    except Exception as exc:
        if "already exists" not in str(exc):
            raise
    connection = disposable_postgres_v16.connect(name)
    with connection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
        cur.execute(BOOTSTRAP)
        for migration in (V1, V1_1, V1_2, V1_3):
            cur.execute(migration)
        cur.execute(FROZEN_SOURCE)
        for index, token in enumerate(TOKENS.values(), start=1):
            cur.execute(
                "INSERT INTO learning_canonical_source_snapshots_v2 "
                "(snapshot_token,snapshot_status,feedback_run_id) "
                "VALUES (%s,'COMPLETE',%s)",
                (token, index),
            )
        cur.execute(V2_1)
        cur.execute(V2_2)
        cur.execute(V2_2)
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def rollback_case(generation_db):
    yield
    generation_db.rollback()


def _set_source(cur, database: str, symbols: tuple[str, ...]) -> None:
    cur.execute("TRUNCATE v_decision_intelligence_v1")
    for symbol in symbols:
        for ordinal in range(12):
            cur.execute(
                """
                INSERT INTO v_decision_intelligence_v1(
                    environment,symbol,interval,strategy,decision_key,
                    net_pnl_usdc,recommendation_type,recommendation_action,
                    missing_context_count,refreshed_at,
                    decision_lifecycle_status,has_pnl
                ) VALUES (%s,%s,'1m','RSI',%s,%s,'ENTRY','CONFIRM',0,
                          now(),'CLOSED',true)
                """,
                (database, symbol, f"{symbol}-{ordinal}", 1 if ordinal % 2 else -0.4),
            )


def _run_generation(cur, database: str, name: str, symbols: tuple[str, ...]) -> int:
    _set_source(cur, database, symbols)
    cur.execute(
        "SELECT set_config('waltrade.learning_source_snapshot_token',%s,false)",
        (TOKENS[name],),
    )
    cur.execute("SELECT refresh_learning_feedback_engine_v1_1(30,10,30)")
    cur.execute("SELECT refresh_learning_feedback_engine_v1_1(30,10,30)")
    cur.execute(
        """
        INSERT INTO learning_feedback_refresh_runs_v1(
          environment,engine_version,trigger_source,status,window_days,
          min_observe_sample,min_action_sample,interval_hours,started_at
        ) VALUES (%s,'LEARNING_FEEDBACK_ENGINE_V1_2','TEST','RUNNING',
                  30,10,30,12,now()) RETURNING id
        """,
        (database,),
    )
    run_id = int(cur.fetchone()[0])
    cur.execute(
        "UPDATE learning_feedback_refresh_runs_v1 "
        "SET status='OK',finished_at=now() WHERE id=%s",
        (run_id,),
    )
    cur.execute("SELECT capture_learning_evidence_manifests_v1(%s)", (run_id,))
    cur.execute("SELECT capture_learning_evidence_manifests_v1(%s)", (run_id,))
    return run_id


def test_full_production_v1_1_is_installed_and_generation_scoped(generation_db):
    with generation_db.cursor() as cur:
        cur.execute(
            "SELECT pg_get_functiondef("
            "'refresh_learning_feedback_engine_v1_1(integer,integer,integer)'::regprocedure)"
        )
        definition = cur.fetchone()[0]
    assert "LEARNING_FEEDBACK_SAMPLE_POLICY_V1_1" in definition
    assert "v_base_result := refresh_learning_feedback_engine_v1(" in definition
    assert "source_snapshot_token = NULLIF(" in definition
    assert len(definition) > 14000


def test_shrink_same_grow_and_reappearing_slot_preserve_generation(generation_db):
    database = generation_db.info.dbname
    with generation_db.cursor() as cur:
        run_a = _run_generation(cur, database, "A", ("BTCUSDC", "ETHUSDC", "SOLUSDC"))
        cur.execute(
            "SELECT count(*) FROM learning_evidence_manifests_v1 "
            "WHERE feedback_run_id=%s AND source_snapshot_token=%s",
            (run_a, TOKENS["A"]),
        )
        assert cur.fetchone()[0] == 3
        run_b = _run_generation(cur, database, "B", ("BTCUSDC", "SOLUSDC"))

        cur.execute(
            "SELECT source_snapshot_token FROM learning_slot_statistics_v1 "
            "WHERE symbol='ETHUSDC'"
        )
        assert str(cur.fetchone()[0]) == TOKENS["A"]
        cur.execute(
            "SELECT source_snapshot_token FROM learning_calibration_proposals_v1 "
            "WHERE symbol='ETHUSDC'"
        )
        assert str(cur.fetchone()[0]) == TOKENS["A"]
        cur.execute(
            "SELECT count(*) FROM learning_proposal_observations_v1 "
            "WHERE refresh_run_id=%s AND symbol='ETHUSDC'",
            (run_b,),
        )
        assert cur.fetchone()[0] == 0
        for table in (
            "learning_canonical_evidence_selection_v1",
            "learning_evidence_manifests_v1",
        ):
            cur.execute(
                f"SELECT count(*) FROM {table} "
                "WHERE feedback_run_id=%s AND symbol='ETHUSDC'",
                (run_b,),
            )
            assert cur.fetchone()[0] == 0
        cur.execute(
            """
            SELECT count(*),sum(m.decisions),sum(a.decisions),count(mm.ordinal)
              FROM learning_evidence_manifests_v1 m
              JOIN learning_evidence_aggregates_v1 a USING(manifest_id)
              JOIN learning_evidence_membership_v1 mm USING(manifest_id)
             WHERE m.feedback_run_id=%s
            """,
            (run_b,),
        )
        manifest_count, header_sum, aggregate_sum_repeated, membership_count = cur.fetchone()
        assert manifest_count == 24
        assert header_sum == 288
        assert aggregate_sum_repeated == 288
        assert membership_count == 24
        cur.execute(
            "SELECT count(*),sum(decisions) FROM learning_evidence_manifests_v1 "
            "WHERE feedback_run_id=%s",
            (run_b,),
        )
        assert cur.fetchone() == (2, 24)

        run_c = _run_generation(cur, database, "C", ("BTCUSDC", "SOLUSDC"))
        cur.execute(
            "SELECT count(*) FROM learning_proposal_observations_v1 "
            "WHERE refresh_run_id=%s",
            (run_c,),
        )
        assert cur.fetchone()[0] == 2

        run_d = _run_generation(
            cur, database, "D", ("BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC")
        )
        cur.execute(
            "SELECT symbol,source_snapshot_token::text "
            "FROM learning_slot_statistics_v1 ORDER BY symbol"
        )
        assert cur.fetchall() == [
            ("BNBUSDC", TOKENS["D"]),
            ("BTCUSDC", TOKENS["D"]),
            ("ETHUSDC", TOKENS["D"]),
            ("SOLUSDC", TOKENS["D"]),
        ]
        cur.execute(
            "SELECT count(*) FROM learning_proposal_observations_v1 "
            "WHERE refresh_run_id=%s",
            (run_d,),
        )
        assert cur.fetchone()[0] == 4
        assert run_a < run_b < run_c < run_d


def test_wrong_explicit_token_remains_fail_closed(generation_db):
    with generation_db.cursor() as cur:
        cur.execute(
            "SELECT set_config('waltrade.learning_source_snapshot_token',%s,false)",
            (TOKENS["B"],),
        )
        with pytest.raises(errors.RaiseException) as caught:
            cur.execute(
                """
                INSERT INTO learning_slot_statistics_v1(
                  environment,symbol,interval,strategy,window_days,decisions,
                  learning_status,learning_reason,source_snapshot_token
                ) VALUES ('test','X','1m','RSI',30,0,
                          'INSUFFICIENT_SAMPLE','test',%s)
                """,
                (TOKENS["A"],),
            )
        assert "LEARNING_FROZEN_SOURCE_PAYLOAD_CONFLICT" in str(caught.value)


def test_migration_ledger_is_exactly_once(generation_db):
    with generation_db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM schema_migration_ledger_v1 WHERE migration_id=%s",
            (
                "20260804_learning_frozen_source_snapshot_v2_2_"
                "generation_scoped_projection.sql",
            ),
        )
        assert cur.fetchone()[0] == 1

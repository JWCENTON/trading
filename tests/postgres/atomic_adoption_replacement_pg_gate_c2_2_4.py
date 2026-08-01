from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
import threading
import time

import psycopg2
import pytest

from common.contract_adoption import (
    activate_contract_adoption,
    replace_active_contract_adoption,
    rollback_active_to_prepared_contract_adoption,
    rollback_contract_adoption,
)
from common.exchange_fill_change_control import _resolve_row_generation
from common.simulated_execution_evidence import (
    paper_position_mutation_allowed_cursor,
)


DSN = os.environ["WALTRADE_C224_PG_DSN"]
CONTRACT = "FEE_AWARE_INVENTORY_C2_2"
ENVIRONMENT = "paper"
DEPLOYMENT = "local-paper"
OLD_SHA = "1" * 40
CANDIDATE_SHA = "2" * 40
RECOVERY_SHA = "3" * 40


SCHEMA = """
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGSERIAL PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL CHECK (environment IN ('live', 'paper')),
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  status TEXT NOT NULL CHECK (
    status IN ('PREPARED','ACTIVE','DEACTIVATED','ROLLED_BACK','SUPERSEDED')
  ),
  adopted_at TIMESTAMPTZ,
  deactivated_at TIMESTAMPTZ,
  git_revision TEXT NOT NULL,
  migration_version TEXT NOT NULL,
  container_revision TEXT,
  activation_reason TEXT NOT NULL,
  deactivation_reason TEXT,
  supersedes_adoption_id BIGINT REFERENCES runtime_contract_adoption_v2(adoption_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (contract_name,environment,deployment_id,generation),
  CHECK (
    (status='ACTIVE' AND adopted_at IS NOT NULL AND deactivated_at IS NULL)
    OR status<>'ACTIVE'
  ),
  CHECK (
    status NOT IN ('DEACTIVATED','ROLLED_BACK','SUPERSEDED')
    OR (deactivated_at IS NOT NULL AND deactivation_reason IS NOT NULL)
  )
);
CREATE UNIQUE INDEX ux_runtime_contract_adoption_v2_active
  ON runtime_contract_adoption_v2(contract_name,environment,deployment_id)
  WHERE status='ACTIVE';
CREATE INDEX ix_runtime_contract_adoption_v2_history
  ON runtime_contract_adoption_v2(
    contract_name,environment,deployment_id,generation DESC
  );

CREATE TABLE positions (
  id BIGINT PRIMARY KEY,
  entry_order_id TEXT,
  exit_order_id TEXT,
  entry_time TIMESTAMPTZ,
  inventory_contract_adoption_id BIGINT
    REFERENCES runtime_contract_adoption_v2(adoption_id),
  inventory_contract_generation BIGINT,
  inventory_evidence_status TEXT,
  CHECK (
    (inventory_contract_adoption_id IS NULL
      AND inventory_contract_generation IS NULL)
    OR
    (inventory_contract_adoption_id IS NOT NULL
      AND inventory_contract_generation IS NOT NULL
      AND inventory_contract_generation > 0)
  )
);
CREATE TABLE binance_orders (
  order_id TEXT PRIMARY KEY,
  position_id BIGINT REFERENCES positions(id)
);
CREATE FUNCTION is_existing_projected_c2_2_compatible(
  p_position_id BIGINT,p_environment TEXT
) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
    SELECT 1 FROM positions
    WHERE id=p_position_id AND inventory_evidence_status='COMPLETE'
      AND p_environment IN ('paper','live')
  )
$$;

CREATE FUNCTION activate_contract_adoption(
  p_adoption_id BIGINT,p_expected_git_revision TEXT,
  p_expected_environment TEXT,p_expected_deployment_id TEXT
) RETURNS runtime_contract_adoption_v2 LANGUAGE plpgsql AS $$
DECLARE result runtime_contract_adoption_v2;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'FEE_AWARE_INVENTORY_C2_2|' || p_expected_environment || '|' ||
    p_expected_deployment_id,0
  ));
  SELECT * INTO result FROM runtime_contract_adoption_v2
  WHERE adoption_id=p_adoption_id FOR UPDATE;
  IF result.adoption_id IS NULL OR result.status<>'PREPARED'
     OR result.git_revision<>p_expected_git_revision
     OR result.environment<>p_expected_environment
     OR result.deployment_id<>p_expected_deployment_id THEN
    RAISE EXCEPTION 'ADOPTION_ACTIVATION_MISMATCH';
  END IF;
  IF EXISTS (
    SELECT 1 FROM runtime_contract_adoption_v2
    WHERE contract_name=result.contract_name
      AND environment=result.environment
      AND deployment_id=result.deployment_id
      AND status='ACTIVE' AND adoption_id<>result.adoption_id
  ) THEN
    RAISE EXCEPTION 'ADOPTION_ACTIVE_CONFLICT';
  END IF;
  UPDATE runtime_contract_adoption_v2
  SET status='ACTIVE',adopted_at=clock_timestamp()
  WHERE adoption_id=result.adoption_id RETURNING * INTO result;
  RETURN result;
END $$;

CREATE FUNCTION rollback_contract_adoption(
  p_adoption_id BIGINT,p_reason TEXT
) RETURNS runtime_contract_adoption_v2 LANGUAGE plpgsql AS $$
DECLARE result runtime_contract_adoption_v2;
BEGIN
  UPDATE runtime_contract_adoption_v2
  SET status='ROLLED_BACK',deactivated_at=clock_timestamp(),
      deactivation_reason=p_reason
  WHERE adoption_id=p_adoption_id AND status IN ('PREPARED','ACTIVE')
  RETURNING * INTO result;
  IF result.adoption_id IS NULL THEN
    RAISE EXCEPTION 'ADOPTION_ROLLBACK_INVALID_STATE';
  END IF;
  RETURN result;
END $$;
"""


def connection():
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout='4s'")
        cur.execute("SET statement_timeout='8s'")
    conn.commit()
    return conn


@pytest.fixture(scope="session", autouse=True)
def database_contract():
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
                cur.execute(SCHEMA)
        yield
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def clean_state(database_contract, monkeypatch):
    conn = connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE binance_orders,positions,"
                "runtime_contract_adoption_v2 RESTART IDENTITY CASCADE"
            )
    conn.close()
    monkeypatch.setenv("GIT_SHA", CANDIDATE_SHA)


def seed(
    generation,
    status,
    sha,
    *,
    supersedes=None,
    scope=DEPLOYMENT,
    environment=ENVIRONMENT,
    contract=CONTRACT,
):
    conn = connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runtime_contract_adoption_v2(
                  contract_name,environment,deployment_id,generation,status,
                  adopted_at,git_revision,migration_version,container_revision,
                  activation_reason,supersedes_adoption_id
                ) VALUES (
                  %s,%s,%s,%s,%s,
                  CASE WHEN %s='ACTIVE' THEN clock_timestamp() END,
                  %s,'C2.2.2',%s,'runtime gate',%s
                ) RETURNING adoption_id
                """,
                (
                    contract, environment, scope, generation, status, status,
                    sha, f"image-{generation}", supersedes,
                ),
            )
            return int(cur.fetchone()[0])


def replace(conn, old_id, new_id, old_gen, old_sha, new_sha, deployment=DEPLOYMENT):
    with conn:
        with conn.cursor() as cur:
            return replace_active_contract_adoption(
                cur,
                prepared_adoption_id=new_id,
                expected_current_active_adoption_id=old_id,
                expected_current_active_generation=old_gen,
                expected_current_active_git_revision=old_sha,
                expected_new_git_revision=new_sha,
                expected_environment=ENVIRONMENT,
                expected_deployment_id=deployment,
                supersession_reason="two-session runtime gate",
            )


def rows():
    conn = connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT adoption_id,generation,status,git_revision,adopted_at,
                       deactivated_at,deactivation_reason,supersedes_adoption_id
                FROM runtime_contract_adoption_v2 ORDER BY generation
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def assert_one_active():
    snapshot = rows()
    assert sum(row[2] == "ACTIVE" for row in snapshot) == 1
    assert all(row[4] is not None for row in snapshot if row[2] == "ACTIVE")
    assert all(
        row[5] is not None and row[6]
        for row in snapshot if row[2] in {"SUPERSEDED", "ROLLED_BACK"}
    )
    return snapshot


def snapshot_hash(snapshot):
    payload = json.dumps(snapshot, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def test_normal_replacement_and_idempotent_retry():
    old_id = seed(1, "ACTIVE", OLD_SHA)
    new_id = seed(2, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    conn = connection()
    first = replace(conn, old_id, new_id, 1, OLD_SHA, CANDIDATE_SHA)
    before = rows()
    before_hash = snapshot_hash(before)
    second = replace(conn, old_id, new_id, 1, OLD_SHA, CANDIDATE_SHA)
    after = rows()
    conn.close()

    assert first.outcome == "REPLACED"
    assert second.outcome == "ALREADY_REPLACED"
    assert before_hash == snapshot_hash(after)
    assert before == after
    assert after[0][2] == "SUPERSEDED"
    assert after[1][2] == "ACTIVE"
    assert after[0][5] == after[1][4]
    assert after[1][7] == old_id
    assert_one_active()


def parallel(*operations):
    barrier = threading.Barrier(len(operations))

    def run(operation):
        conn = connection()
        started = datetime.now(timezone.utc)
        barrier.wait()
        try:
            result = operation(conn)
            return started, datetime.now(timezone.utc), "OK", result
        except Exception as exc:
            return started, datetime.now(timezone.utc), "ERROR", str(exc)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=len(operations)) as executor:
        return list(executor.map(run, operations))


def test_real_two_session_same_candidate_race():
    old_id = seed(10, "ACTIVE", OLD_SHA)
    new_id = seed(11, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    results = parallel(
        lambda conn: replace(conn, old_id, new_id, 10, OLD_SHA, CANDIDATE_SHA),
        lambda conn: replace(conn, old_id, new_id, 10, OLD_SHA, CANDIDATE_SHA),
    )

    outcomes = sorted(
        item[3].outcome for item in results if item[2] == "OK"
    )
    assert outcomes == ["ALREADY_REPLACED", "REPLACED"]
    assert all(item[1] >= item[0] for item in results)
    assert_one_active()


def test_competing_candidates_race():
    old_id = seed(20, "ACTIVE", OLD_SHA)
    first_id = seed(21, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    second_id = seed(22, "PREPARED", RECOVERY_SHA, supersedes=old_id)
    results = parallel(
        lambda conn: replace(
            conn, old_id, first_id, 20, OLD_SHA, CANDIDATE_SHA
        ),
        lambda conn: replace(
            conn, old_id, second_id, 20, OLD_SHA, RECOVERY_SHA
        ),
    )

    assert sum(item[2] == "OK" for item in results) == 1
    assert any(
        "ADOPTION_REPLACEMENT_ACTIVE_MISMATCH" in str(item[3])
        for item in results if item[2] == "ERROR"
    )
    snapshot = assert_one_active()
    assert sum(row[2] == "PREPARED" for row in snapshot) == 1


def test_replacement_versus_normal_activation_race():
    old_id = seed(30, "ACTIVE", OLD_SHA)
    replacement_id = seed(
        31, "PREPARED", CANDIDATE_SHA, supersedes=old_id
    )
    activation_id = seed(32, "PREPARED", RECOVERY_SHA, supersedes=old_id)

    def activate(conn):
        with conn:
            with conn.cursor() as cur:
                return activate_contract_adoption(
                    cur,
                    adoption_id=activation_id,
                    expected_git_revision=RECOVERY_SHA,
                    expected_environment=ENVIRONMENT,
                    expected_deployment_id=DEPLOYMENT,
                )

    results = parallel(
        lambda conn: replace(
            conn, old_id, replacement_id, 30, OLD_SHA, CANDIDATE_SHA
        ),
        activate,
    )
    assert sum(item[2] == "OK" for item in results) == 1
    assert_one_active()


def test_replacement_versus_rollback_race():
    old_id = seed(40, "ACTIVE", OLD_SHA)
    new_id = seed(41, "PREPARED", CANDIDATE_SHA, supersedes=old_id)

    def rollback(conn):
        with conn:
            with conn.cursor() as cur:
                return rollback_contract_adoption(
                    cur, adoption_id=old_id, reason="competing rollback"
                )

    results = parallel(
        lambda conn: replace(
            conn, old_id, new_id, 40, OLD_SHA, CANDIDATE_SHA
        ),
        rollback,
    )
    assert sum(item[2] == "OK" for item in results) == 1
    assert any(
        "ADOPTION_ROLLBACK_REPLACEMENT_CONFLICT" in str(item[3])
        or "ADOPTION_ROLLBACK_INVALID_STATE" in str(item[3])
        for item in results if item[2] == "ERROR"
    )
    assert_one_active()


def test_forced_second_update_failure_rolls_back_savepoint():
    old_id = seed(50, "ACTIVE", OLD_SHA)
    new_id = seed(51, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    conn = connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE FUNCTION reject_gate_activation() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.adoption_id=%s AND NEW.status='ACTIVE' THEN
                    RAISE EXCEPTION 'FORCED_SECOND_UPDATE_FAILURE';
                  END IF;
                  RETURN NEW;
                END $$;
                CREATE TRIGGER reject_gate_activation
                BEFORE UPDATE ON runtime_contract_adoption_v2
                FOR EACH ROW EXECUTE FUNCTION reject_gate_activation();
                """,
                (new_id,),
            )
    with pytest.raises(RuntimeError, match="ADOPTION_REPLACEMENT_CONFLICT"):
        replace(conn, old_id, new_id, 50, OLD_SHA, CANDIDATE_SHA)
    snapshot = rows()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DROP TRIGGER reject_gate_activation
                  ON runtime_contract_adoption_v2;
                DROP FUNCTION reject_gate_activation();
                """
            )
    conn.close()

    assert snapshot[0][2] == "ACTIVE"
    assert snapshot[0][5] is None
    assert snapshot[1][2] == "PREPARED"
    assert snapshot[1][4] is None
    assert_one_active()


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"old_sha": "9" * 40}, "ADOPTION_REPLACEMENT_ACTIVE_MISMATCH"),
        ({"new_sha": "9" * 40}, "ADOPTION_REPLACEMENT_SHA_MISMATCH"),
        ({"deployment": "wrong"}, "ADOPTION_REPLACEMENT_SCOPE_MISMATCH"),
        ({"old_id": 9999}, "ADOPTION_REPLACEMENT_ACTIVE_MISMATCH"),
        ({"new_id": 9999}, "ADOPTION_REPLACEMENT_PREPARED_MISMATCH"),
    ],
)
def test_mismatch_matrix(change, error):
    old_id = seed(60, "ACTIVE", OLD_SHA)
    new_id = seed(61, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    values = {
        "old_id": old_id, "new_id": new_id, "old_sha": OLD_SHA,
        "new_sha": CANDIDATE_SHA, "deployment": DEPLOYMENT,
    }
    values.update(change)
    conn = connection()
    with pytest.raises(RuntimeError, match=error):
        replace(
            conn, values["old_id"], values["new_id"], 60,
            values["old_sha"], values["new_sha"], values["deployment"],
        )
    conn.close()
    assert [row[2] for row in rows()] == ["ACTIVE", "PREPARED"]
    assert_one_active()


def test_scope_contract_and_generation_order_mismatches():
    old_id = seed(70, "ACTIVE", OLD_SHA)
    wrong_scope_id = seed(
        71, "PREPARED", CANDIDATE_SHA, supersedes=old_id, scope="other"
    )
    conn = connection()
    with pytest.raises(RuntimeError, match="ADOPTION_REPLACEMENT_SCOPE_MISMATCH"):
        replace(conn, old_id, wrong_scope_id, 70, OLD_SHA, CANDIDATE_SHA)
    conn.close()

    conn = connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM runtime_contract_adoption_v2 WHERE adoption_id=%s",
                (wrong_scope_id,),
            )
            cur.execute(
                """
                INSERT INTO runtime_contract_adoption_v2(
                  contract_name,environment,deployment_id,generation,status,
                  git_revision,migration_version,activation_reason,
                  supersedes_adoption_id
                ) VALUES (%s,%s,%s,69,'PREPARED',%s,'C2.2.2','gate',%s)
                RETURNING adoption_id
                """,
                (CONTRACT, ENVIRONMENT, DEPLOYMENT, CANDIDATE_SHA, old_id),
            )
            lower_id = int(cur.fetchone()[0])
    with pytest.raises(
        RuntimeError, match="ADOPTION_REPLACEMENT_GENERATION_ORDER_INVALID"
    ):
        replace(conn, old_id, lower_id, 70, OLD_SHA, CANDIDATE_SHA)
    conn.close()
    assert_one_active()


@pytest.mark.parametrize(
    ("environment", "contract"),
    [
        ("live", CONTRACT),
        (ENVIRONMENT, "OTHER_INVENTORY_CONTRACT"),
    ],
)
def test_wrong_environment_or_contract_name_is_scope_mismatch(
    environment, contract
):
    old_id = seed(
        75, "ACTIVE", OLD_SHA, environment=environment, contract=contract
    )
    new_id = seed(
        76, "PREPARED", CANDIDATE_SHA, supersedes=old_id,
        environment=environment, contract=contract,
    )
    conn = connection()

    with pytest.raises(RuntimeError, match="ADOPTION_REPLACEMENT_SCOPE_MISMATCH"):
        replace(conn, old_id, new_id, 75, OLD_SHA, CANDIDATE_SHA)

    conn.close()
    assert [row[2] for row in rows()] == ["ACTIVE", "PREPARED"]
    assert_one_active()


def test_runtime_revision_does_not_gate_generation_owned_position(monkeypatch):
    old_id = seed(1, "ACTIVE", OLD_SHA)
    new_id = seed(2, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    conn = connection()
    replace(conn, old_id, new_id, 1, OLD_SHA, CANDIDATE_SHA)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions VALUES
                  (100,'old-entry',NULL,clock_timestamp(),%s,1,'COMPLETE'),
                  (101,'new-entry',NULL,clock_timestamp(),NULL,NULL,'COMPLETE');
                INSERT INTO binance_orders VALUES
                  ('old-entry',100),('new-entry',101);
                """,
                (old_id,),
            )

    monkeypatch.setenv("GIT_SHA", CANDIDATE_SHA)
    with conn.cursor() as cur:
        assert paper_position_mutation_allowed_cursor(
            cur, position_id=100, deployment_id=DEPLOYMENT
        )
        classification = _resolve_row_generation(
            cur,
            {
                "environment": ENVIRONMENT,
                "deployment_id": DEPLOYMENT,
                "order_id": "old-entry",
            },
        )
        assert classification == ("EXISTING_PROJECTED_C2_2", old_id, 1)
        new_classification = _resolve_row_generation(
            cur,
            {
                "environment": ENVIRONMENT,
                "deployment_id": DEPLOYMENT,
                "order_id": "new-entry",
            },
        )
        assert new_classification == ("EXISTING_PROJECTED_C2_2", new_id, 2)
    conn.rollback()

    monkeypatch.setenv("GIT_SHA", OLD_SHA)
    with conn.cursor() as cur:
        assert paper_position_mutation_allowed_cursor(
            cur, position_id=100, deployment_id=DEPLOYMENT
        )
    conn.rollback()
    for bad in (None, "abc", "z" * 40):
        if bad is None:
            monkeypatch.delenv("GIT_SHA", raising=False)
        else:
            monkeypatch.setenv("GIT_SHA", bad)
        with conn.cursor() as cur:
            assert paper_position_mutation_allowed_cursor(
                cur, position_id=100, deployment_id=DEPLOYMENT
            )
        conn.rollback()
    conn.close()
    assert_one_active()


def test_failed_candidate_recovery_generation_three():
    first_id = seed(1, "ACTIVE", OLD_SHA)
    failed_id = seed(2, "PREPARED", CANDIDATE_SHA, supersedes=first_id)
    conn = connection()
    replace(conn, first_id, failed_id, 1, OLD_SHA, CANDIDATE_SHA)
    recovery_id = seed(3, "PREPARED", OLD_SHA, supersedes=failed_id)
    with conn:
        with conn.cursor() as cur:
            result = rollback_active_to_prepared_contract_adoption(
                cur,
                prepared_recovery_adoption_id=recovery_id,
                expected_failed_active_adoption_id=failed_id,
                expected_failed_active_generation=2,
                expected_failed_active_git_revision=CANDIDATE_SHA,
                expected_recovery_git_revision=OLD_SHA,
                expected_environment=ENVIRONMENT,
                expected_deployment_id=DEPLOYMENT,
                rollback_reason="candidate failed to start",
            )
    conn.close()
    snapshot = rows()

    assert result.outcome == "REPLACED"
    assert [row[2] for row in snapshot] == [
        "SUPERSEDED", "ROLLED_BACK", "ACTIVE"
    ]
    assert snapshot[2][7] == failed_id
    assert_one_active()


def test_long_transaction_waits_then_returns_deterministically():
    old_id = seed(80, "ACTIVE", OLD_SHA)
    new_id = seed(81, "PREPARED", CANDIDATE_SHA, supersedes=old_id)
    acquired = threading.Event()

    def slow(conn):
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"{CONTRACT}|{ENVIRONMENT}|{DEPLOYMENT}",),
                )
                acquired.set()
                time.sleep(0.6)
                return replace_active_contract_adoption(
                    cur,
                    prepared_adoption_id=new_id,
                    expected_current_active_adoption_id=old_id,
                    expected_current_active_generation=80,
                    expected_current_active_git_revision=OLD_SHA,
                    expected_new_git_revision=CANDIDATE_SHA,
                    expected_environment=ENVIRONMENT,
                    expected_deployment_id=DEPLOYMENT,
                    supersession_reason="slow holder",
                )

    def waiting(conn):
        acquired.wait(timeout=2)
        started = time.monotonic()
        result = replace(conn, old_id, new_id, 80, OLD_SHA, CANDIDATE_SHA)
        return time.monotonic() - started, result

    results = parallel(slow, waiting)
    waiting_result = next(
        item[3] for item in results
        if item[2] == "OK" and isinstance(item[3], tuple)
    )
    assert waiting_result[0] >= 0.5
    assert waiting_result[1].outcome == "ALREADY_REPLACED"
    assert_one_active()

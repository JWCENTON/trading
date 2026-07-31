"""Disposable PostgreSQL 16 gates for the LEI1B submission/ACK boundary."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.entry_intent import EntryIntentInsertOutcome, LiveEntryIntent
from common.entry_submission import (
    AckPersistOutcome,
    ActiveAdoptionResolutionError,
    EntryOrderAck,
    EntrySubmissionAttempt,
    EntrySubmissionExecutionOutcome,
    EntrySubmissionMode,
    EntrySubmissionRepository,
    SubmissionAttemptCommitFailed,
    SubmissionAttemptOutcome,
    execute_committed_entry_submission,
)


ROOT = Path(__file__).resolve().parents[2]
LEI1A = (
    ROOT / "db/migrations/20260730_live_entry_intent_ledger_v1.sql"
).read_text()
FORWARD_PATH = (
    ROOT / "db/migrations/20260731_live_entry_submission_ack_v1.sql"
)
ROLLBACK_PATH = (
    ROOT
    / "db/migrations/20260731_live_entry_submission_ack_v1_rollback.sql"
)
MANIFEST_PATH = (
    ROOT
    / "db/migrations/20260731_live_entry_submission_ack_v1_manifest.json"
)
FORWARD = FORWARD_PATH.read_text()
ROLLBACK = ROLLBACK_PATH.read_text()
MANIFEST = json.loads(MANIFEST_PATH.read_text())

GIT_REVISION = "f" * 40
PREPARED_AT = datetime(2026, 7, 31, 18, 45, tzinfo=timezone.utc)
ACKNOWLEDGED_AT = PREPARED_AT + timedelta(seconds=2)

ADOPTION_SCHEMA = f"""
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGINT PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  status TEXT NOT NULL,
  git_revision TEXT NOT NULL
);
INSERT INTO runtime_contract_adoption_v2(
  adoption_id,contract_name,environment,deployment_id,generation,status,
  git_revision
) VALUES
  (1,'FEE_AWARE_INVENTORY_C2_2','live','local-live',1,'ACTIVE',
   '{GIT_REVISION}'),
  (2,'OTHER_CONTRACT','live','local-live',1,'ACTIVE','{'e' * 40}'),
  (3,'FEE_AWARE_INVENTORY_C2_2','paper','local-paper',1,'ACTIVE',
   '{GIT_REVISION}');
"""


def _intent(**changes) -> LiveEntryIntent:
    values = {
        "environment": "live",
        "deployment_id": "local-live",
        "git_revision": GIT_REVISION,
        "adoption_id": 1,
        "generation": 1,
        "decision_id": uuid.UUID("29a20a95-9555-522d-aedb-6b923c283ca1"),
        "symbol": "BNBUSDC",
        "strategy": "TREND",
        "interval": "1m",
        "exchange_source": "okx",
        "client_order_id": "ORC-L-BNBUSDC-TREN-1m-E-lei1b-pg",
        "requested_qty": Decimal("0.033895"),
        "prepared_at": PREPARED_AT,
        "producer_identity": "postgres-contract-test",
    }
    values.update(changes)
    return LiveEntryIntent.build(**values)


def _attempt(intent=None) -> EntrySubmissionAttempt:
    selected = intent or _intent()
    return EntrySubmissionAttempt.build(
        selected,
        submitted_at=PREPARED_AT + timedelta(seconds=1),
        producer_identity=selected.producer_identity,
    )


def _ack(intent=None, attempt=None, **changes) -> EntryOrderAck:
    selected_intent = intent or _intent()
    selected_attempt = attempt or _attempt(selected_intent)
    values = {
        "exchange_order_id": "okx-order-1",
        "exchange_order_status": "LIVE",
        "acknowledged_at": ACKNOWLEDGED_AT,
        "producer_identity": selected_intent.producer_identity,
    }
    values.update(changes)
    return EntryOrderAck.build(selected_intent, selected_attempt, **values)


def _create_database(disposable_postgres_v16, purpose):
    name = f"waltrade_baseline_test_lei1b_{purpose}_{uuid.uuid4().hex[:8]}"
    disposable_postgres_v16.create_database(name)
    return name


def _apply(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _install(disposable_postgres_v16, purpose):
    name = _create_database(disposable_postgres_v16, purpose)
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn, ADOPTION_SCHEMA)
        _apply(conn, LEI1A)
        _apply(conn, FORWARD)
    finally:
        conn.close()
    factory = lambda: disposable_postgres_v16.connect(name)
    return name, factory, EntrySubmissionRepository(factory)


def _counts(factory):
    conn = factory()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT "
                "(SELECT count(*) FROM live_entry_intents_v1),"
                "(SELECT count(*) FROM live_entry_submissions_v1),"
                "(SELECT count(*) FROM live_entry_order_acks_v1)"
            )
            return tuple(int(item) for item in cur.fetchone())
    finally:
        conn.rollback()
        conn.close()


def test_gate_a_migration_twice_manifest_and_no_backfill(
    disposable_postgres_v16,
):
    name = _create_database(disposable_postgres_v16, "migration")
    conn = disposable_postgres_v16.connect(name)
    try:
        _apply(conn, ADOPTION_SCHEMA)
        _apply(conn, LEI1A)
        _apply(conn, FORWARD)
        _apply(conn, FORWARD)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM live_entry_submissions_v1"
            )
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM live_entry_order_acks_v1")
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
                "AND tablename IN ("
                "'live_entry_submissions_v1','live_entry_order_acks_v1')"
            )
            indexes = {row[0] for row in cur.fetchall()}
            assert set(MANIFEST["indexes"]).issubset(indexes)
            cur.execute(
                "SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND "
                "tgrelid IN ('live_entry_submissions_v1'::regclass,"
                "'live_entry_order_acks_v1'::regclass)"
            )
            triggers = {row[0] for row in cur.fetchall()}
            assert set(MANIFEST["triggers"]) == triggers
    finally:
        conn.close()
    assert MANIFEST["data_policy"] == {
        "backfill": False,
        "runtime_writer_activation": False,
        "append_only": True,
        "rollback_blocked_after_evidence": True,
    }
    assert MANIFEST["migration_sha256"] == hashlib.sha256(
        FORWARD_PATH.read_bytes()
    ).hexdigest()
    assert MANIFEST["rollback_sha256"] == hashlib.sha256(
        ROLLBACK_PATH.read_bytes()
    ).hexdigest()


def test_gate_b_commit_visible_before_network_and_failures_submit_zero(
    disposable_postgres_v16,
):
    _, factory, repository = _install(disposable_postgres_v16, "commit")
    observed_before_send = []

    def network_submit():
        # A separate PostgreSQL session proves both claims committed before the
        # callback crossed the network boundary.
        observed_before_send.append(_counts(factory))
        return {"orderId": "okx-order-1", "status": "live"}

    result = execute_committed_entry_submission(
        mode=EntrySubmissionMode.ENFORCE,
        intent=_intent(),
        repository=repository,
        network_submit=network_submit,
        lookup_by_client_order_id=lambda **_: {
            "outcome": "NOT_FOUND", "order": None,
        },
        clock=lambda: ACKNOWLEDGED_AT,
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_PERSISTED
    assert observed_before_send == [(1, 1, 0)]
    assert _counts(factory) == (1, 1, 1)

    network = []
    invalid_generation = _intent(
        client_order_id="ORC-L-BNBUSDC-TREN-1m-E-wrong-generation",
        generation=2,
    )
    blocked = execute_committed_entry_submission(
        mode=EntrySubmissionMode.ENFORCE,
        intent=invalid_generation,
        repository=repository,
        network_submit=lambda: network.append("send"),
        lookup_by_client_order_id=lambda **_: {
            "outcome": "NOT_FOUND", "order": None,
        },
        clock=lambda: ACKNOWLEDGED_AT,
    )
    assert blocked.outcome is EntrySubmissionExecutionOutcome.BLOCKED_INTENT_COMMIT_FAILED
    assert network == []
    assert _counts(factory) == (1, 1, 1)


def test_active_adoption_resolution_is_exact_and_sha_mismatch_fails_closed(
    disposable_postgres_v16,
):
    _, _, repository = _install(disposable_postgres_v16, "adoption")
    active = repository.resolve_active_adoption(
        environment="live",
        deployment_id="local-live",
        runtime_git_revision=GIT_REVISION,
    )
    assert (
        active.adoption_id,
        active.generation,
        active.environment.value,
        active.deployment_id.value,
        active.git_revision,
    ) == (1, 1, "live", "local-live", GIT_REVISION)
    with pytest.raises(
        ActiveAdoptionResolutionError, match="DEPLOYMENT_MISMATCH"
    ):
        repository.resolve_active_adoption(
            environment="paper",
            deployment_id="local-live",
            runtime_git_revision=GIT_REVISION,
        )
    with pytest.raises(ActiveAdoptionResolutionError, match="SHA_MISMATCH"):
        repository.resolve_active_adoption(
            environment="live",
            deployment_id="local-live",
            runtime_git_revision="e" * 40,
        )


def test_gate_c_two_identical_sessions_claim_at_most_one_network_submission(
    disposable_postgres_v16,
):
    _, factory, _ = _install(disposable_postgres_v16, "concurrency")
    intent = _intent()
    barrier = threading.Barrier(2)
    calls = []
    calls_lock = threading.Lock()

    def submit():
        with calls_lock:
            calls.append("send")
        # Give the losing transaction time to exercise committed-intent
        # recovery while this single claimed callback remains in flight.
        time.sleep(0.05)
        return {"orderId": "okx-order-concurrent", "status": "live"}

    def lookup(**_):
        with calls_lock:
            sent = bool(calls)
        return (
            {
                "outcome": "FOUND",
                "order": {
                    "orderId": "okx-order-concurrent",
                    "status": "live",
                },
            }
            if sent
            else {"outcome": "NOT_FOUND", "order": None}
        )

    def worker():
        barrier.wait(timeout=5)
        return execute_committed_entry_submission(
            mode=EntrySubmissionMode.ENFORCE,
            intent=intent,
            repository=EntrySubmissionRepository(factory),
            network_submit=submit,
            lookup_by_client_order_id=lookup,
            clock=lambda: ACKNOWLEDGED_AT,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result(timeout=10) for future in (
            pool.submit(worker), pool.submit(worker),
        )]

    assert calls == ["send"]
    assert sum(result.network_called for result in results) == 1
    assert {result.outcome for result in results}.issubset({
        EntrySubmissionExecutionOutcome.ACK_PERSISTED,
        EntrySubmissionExecutionOutcome.ACK_RECOVERED,
        EntrySubmissionExecutionOutcome.RECOVERY_NOT_FOUND,
        EntrySubmissionExecutionOutcome.ACK_ALREADY_PERSISTED,
    })
    assert _counts(factory) == (1, 1, 1)


def test_gate_d_semantic_conflict_has_no_second_submission(
    disposable_postgres_v16,
):
    _, factory, repository = _install(disposable_postgres_v16, "conflict")
    assert repository.commit_intent(_intent()) is EntryIntentInsertOutcome.CREATED
    network = []
    result = execute_committed_entry_submission(
        mode=EntrySubmissionMode.ENFORCE,
        intent=_intent(requested_qty=Decimal("0.033896")),
        repository=repository,
        network_submit=lambda: network.append("send"),
        lookup_by_client_order_id=lambda **_: {
            "outcome": "NOT_FOUND", "order": None,
        },
        clock=lambda: ACKNOWLEDGED_AT,
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.BLOCKED_INTENT_CONFLICT
    assert network == []
    assert _counts(factory) == (1, 0, 0)


def test_gates_e_f_ack_idempotency_conflict_and_append_only_history(
    disposable_postgres_v16,
):
    _, factory, repository = _install(disposable_postgres_v16, "acks")
    intent = _intent()
    attempt = _attempt(intent)
    ack = _ack(intent, attempt)
    assert repository.commit_intent(intent) is EntryIntentInsertOutcome.CREATED
    assert (
        repository.record_submission_attempt(attempt)
        is SubmissionAttemptOutcome.CREATED
    )
    assert repository.persist_ack(ack) is AckPersistOutcome.PERSISTED
    assert (
        repository.persist_ack(ack) is AckPersistOutcome.IDEMPOTENT_EXISTING
    )
    conflicting = _ack(
        intent,
        attempt,
        exchange_order_id="okx-order-conflict",
    )
    assert repository.persist_ack(conflicting) is AckPersistOutcome.CONFLICT
    assert _counts(factory) == (1, 1, 1)

    conn = factory()
    try:
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="immutable and append-only"):
                cur.execute(
                    "UPDATE live_entry_order_acks_v1 "
                    "SET exchange_order_status='FILLED'"
                )
        conn.rollback()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="immutable and append-only"):
                cur.execute("DELETE FROM live_entry_submissions_v1")
        conn.rollback()
    finally:
        conn.close()

    mismatched = replace(attempt, generation=2)
    with pytest.raises(SubmissionAttemptCommitFailed):
        repository.record_submission_attempt(mismatched)
    assert _counts(factory) == (1, 1, 1)


def test_gate_g_crash_recovery_found_persists_ack_without_second_order(
    disposable_postgres_v16,
):
    _, factory, repository = _install(disposable_postgres_v16, "recovery")
    intent = _intent()
    attempt = _attempt(intent)
    assert repository.commit_intent(intent) is EntryIntentInsertOutcome.CREATED
    assert (
        repository.record_submission_attempt(attempt)
        is SubmissionAttemptOutcome.CREATED
    )
    network = []
    lookups = []
    result = execute_committed_entry_submission(
        mode=EntrySubmissionMode.ENFORCE,
        intent=intent,
        repository=repository,
        network_submit=lambda: network.append("send"),
        lookup_by_client_order_id=lambda **kwargs: lookups.append(kwargs) or {
            "outcome": "FOUND",
            "order": {"orderId": "okx-order-recovered", "status": "filled"},
        },
        clock=lambda: ACKNOWLEDGED_AT,
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_RECOVERED
    assert network == []
    assert lookups == [{
        "symbol": intent.symbol,
        "client_order_id": intent.client_order_id,
    }]
    assert _counts(factory) == (1, 1, 1)
    loaded = repository.load_ack(intent.intent_id)
    assert loaded is not None
    assert loaded.exchange_order_id == "okx-order-recovered"
    assert loaded.recovered_by_client_order_id


def test_gate_h_rollback_empty_passes_and_evidence_fails_closed(
    disposable_postgres_v16,
):
    name, factory, repository = _install(disposable_postgres_v16, "rollback")
    conn = factory()
    try:
        _apply(conn, ROLLBACK)
        _apply(conn, ROLLBACK)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.live_entry_submissions_v1'),"
                "to_regclass('public.live_entry_order_acks_v1')"
            )
            assert cur.fetchone() == (None, None)
        _apply(conn, FORWARD)
    finally:
        conn.close()

    intent = _intent()
    attempt = _attempt(intent)
    assert repository.commit_intent(intent) is EntryIntentInsertOutcome.CREATED
    assert (
        repository.record_submission_attempt(attempt)
        is SubmissionAttemptOutcome.CREATED
    )
    conn = disposable_postgres_v16.connect(name)
    try:
        with pytest.raises(
            Exception, match="ROLLBACK_BLOCKED_IMMUTABLE_EVIDENCE_EXISTS"
        ):
            with conn.cursor() as cur:
                cur.execute(ROLLBACK)
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.live_entry_submissions_v1'),"
                "count(*) FROM live_entry_submissions_v1 GROUP BY 1"
            )
            assert cur.fetchone() == ("live_entry_submissions_v1", 1)
    finally:
        conn.close()

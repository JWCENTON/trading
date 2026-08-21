from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import replace
from pathlib import Path
import uuid

import pytest

from common.capital_reservation import (
    accepted_commitment_event,
    accept_paper_simulated_order_cursor,
    append_event_cursor,
    deploy_paper_simulated_fill_cursor,
    load_capital_reservation_evidence,
    transition_reservation_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql").read_text()
NOW = datetime(2026, 8, 21, 10, tzinfo=timezone.utc)


def _database(disposable_postgres_v16):
    name = "waltrade_baseline_test_reservation_" + uuid.uuid4().hex[:12]
    disposable_postgres_v16.create_database(name)
    return disposable_postgres_v16.connect(name)


def _apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def _accept(cur, *, environment="LIVE", deployment="local-live", source="intent-1", amount="10.125"):
    event = accepted_commitment_event(
        environment=environment, deployment_id=deployment,
        account_identity_fingerprint="a" * 64, source_identity=source,
        symbol="BTCUSDC", strategy="RSI", interval="1m",
        requested_notional=Decimal(amount), effective_at=NOW,
        source_authority="TEST_ACCEPTED_COMMITMENT", provenance={"source": source},
    )
    return event, append_event_cursor(cur, event)


def test_migration_idempotent_empty_and_append_only(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM capital_reservation_event_v1")
            assert cur.fetchone()[0] == 0
            event, _ = _accept(cur)
        conn.commit()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="CAPITAL_RESERVATION_APPEND_ONLY"):
                cur.execute(
                    "UPDATE capital_reservation_event_v1 SET source_authority='X' WHERE event_id=%s",
                    (str(event.event_id),),
                )
        conn.rollback()
    finally:
        conn.close()


def test_single_reservation_submission_ack_and_no_double_count(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            event, status = _accept(cur, amount="12.345678901234567890")
            assert status == "INSERTED"
            status, _ = transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity="SUBMITTED:intent-1", state="SUBMITTED",
                effective_at=NOW, source_authority="TEST_SUBMISSION", provenance={},
                reflection_state="INTERNAL_UNREFLECTED",
                reconciliation_status="PENDING_EXCHANGE_REFLECTION",
            )
            assert status == "INSERTED"
            status, _ = transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity="ACK:order-1", state="EXCHANGE_ACK",
                effective_at=NOW, source_authority="TEST_ACK", provenance={},
                reflection_state="INTERNAL_UNREFLECTED",
                reconciliation_status="PENDING_EXCHANGE_REFLECTION",
                order_identity="order-1",
            )
            assert status == "INSERTED"
            status, _ = transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity="LOCKED:order-1", state="EXCHANGE_LOCKED",
                effective_at=NOW, source_authority="TEST_LOCK", provenance={},
                reflection_state="EXCHANGE_REFLECTED",
                reconciliation_status="CANONICAL",
            )
            assert status == "INSERTED"
            current = load_capital_reservation_evidence(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
            )
            assert current.reserved_capital == Decimal("12.345678901234567890")
            assert current.internal_unreflected_reserved == Decimal("0")
            assert current.exchange_reflected_reserved == current.reserved_capital
            cur.execute(
                "SELECT count(*),count(DISTINCT reservation_id) FROM capital_reservation_event_v1"
            )
            assert cur.fetchone() == (4, 1)
        conn.rollback()
    finally:
        conn.close()


def test_duplicate_event_is_idempotent_and_commitment_identity_is_unique(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            event, status = _accept(cur, source="duplicate")
            assert status == "INSERTED"
            assert append_event_cursor(cur, event) == "IDEMPOTENT"
            conflicting = replace(
                event, event_id=uuid.uuid4(), reservation_id=uuid.uuid4(),
                event_fingerprint="f" * 64,
            )
            with pytest.raises(Exception, match="LOGICAL_COMMITMENT_CONFLICT"):
                append_event_cursor(cur, conflicting)
        conn.rollback()
    finally:
        conn.close()


def test_partial_fill_then_full_deployment_and_terminal_no_reactivation(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            event, _ = _accept(cur, amount="10")
            transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity="FILL:1", state="PARTIALLY_DEPLOYED",
                deployed_notional_delta=Decimal("3.25"), effective_at=NOW,
                source_authority="TEST_FILL", provenance={},
            )
            transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity="FILL:2", state="DEPLOYED",
                deployed_notional_delta=Decimal("6.75"), effective_at=NOW,
                source_authority="TEST_FILL", provenance={},
            )
            current = load_capital_reservation_evidence(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
            )
            assert current.reserved_capital == Decimal("0")
            with pytest.raises(ValueError, match="TERMINAL_REACTIVATION"):
                transition_reservation_cursor(
                    cur, reservation_id=event.reservation_id,
                    source_event_identity="BAD", state="SUBMITTED",
                    effective_at=NOW, source_authority="TEST", provenance={},
                )
        conn.rollback()
    finally:
        conn.close()


@pytest.mark.parametrize("terminal", ["CANCELLED", "EXPIRED", "REJECTED", "RELEASED"])
def test_terminal_release_returns_unused_capital(disposable_postgres_v16, terminal):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            event, _ = _accept(cur, source=terminal)
            transition_reservation_cursor(
                cur, reservation_id=event.reservation_id,
                source_event_identity=f"END:{terminal}", state=terminal,
                release_remaining=True, release_reason=terminal,
                effective_at=NOW, source_authority="TEST_TERMINAL", provenance={},
            )
            evidence = load_capital_reservation_evidence(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
            )
            assert evidence.reserved_capital == Decimal("0")
        conn.rollback()
    finally:
        conn.close()


def test_environment_and_account_scope_are_isolated(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            _accept(cur, environment="LIVE", deployment="local-live", source="live")
            _accept(cur, environment="PAPER", deployment="local-paper", source="paper")
            live = load_capital_reservation_evidence(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
            )
            paper = load_capital_reservation_evidence(
                cur, environment="PAPER", deployment_id="local-paper",
                account_identity_fingerprint="a" * 64,
            )
            wrong = load_capital_reservation_evidence(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="b" * 64,
            )
            assert live.reserved_capital == paper.reserved_capital == Decimal("10.125")
            assert wrong.reserved_capital == Decimal("0")
        conn.rollback()
    finally:
        conn.close()


def test_paper_order_then_fill_uses_same_reservation(disposable_postgres_v16):
    conn = _database(disposable_postgres_v16)
    try:
        _apply(conn)
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE simulated_orders (id bigint PRIMARY KEY, deployment_id text)"
            )
            cur.execute(
                "INSERT INTO simulated_orders VALUES (101,'local-paper')"
            )
            status, reservation_id = accept_paper_simulated_order_cursor(
                cur, simulated_order_id=101, deployment_id="local-paper",
                symbol="ETHUSDC", strategy="TREND", interval="5m",
                requested_notional=Decimal("50.000000000000000001"),
                effective_at=NOW, decision_identity="decision-101",
            )
            assert status == "INSERTED"
            assert deploy_paper_simulated_fill_cursor(
                cur, simulated_order_id=101, fill_id=201, position_id=301,
                deployed_notional=Decimal("50.000000000000000001"),
                effective_at=NOW,
            ) == "INSERTED"
            cur.execute(
                "SELECT count(*),count(DISTINCT reservation_id),max(state) "
                "FROM capital_reservation_event_v1 WHERE reservation_id=%s",
                (str(reservation_id),),
            )
            assert cur.fetchone() == (2, 1, "DEPLOYED")
        conn.rollback()
    finally:
        conn.close()

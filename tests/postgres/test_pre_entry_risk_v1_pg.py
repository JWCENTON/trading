from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Thread
import uuid

import pytest

from common.pre_entry_risk import (
    CONTRACT_VERSION,
    _make_event,
    append_pre_entry_risk_event_cursor,
    deterministic_pre_entry_risk_id,
    freeze_paper_pre_entry_risk_cursor,
    load_committed_pre_entry_risk_evidence_cursor,
    release_pre_entry_risk_cursor,
    transition_pre_entry_risk_cursor,
)
from common.capital_reservation import accept_paper_simulated_order_cursor
from common.position_risk_boundary import (
    accept_paper_boundary_cursor,
    activate_boundary_for_position_cursor,
)


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "db/migrations/20260822_pre_entry_risk_authority_v1.sql").read_text()
CAPITAL_MIGRATION = (ROOT / "db/migrations/20260821_capital_reservation_authority_v1.sql").read_text()
BOUNDARY_MIGRATION = (ROOT / "db/migrations/20260821_position_risk_boundary_authority_v1.sql").read_text()
NOW = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)
RESERVATION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
BOUNDARY_ID = uuid.UUID("33333333-3333-4333-8333-333333333333")


def database(server):
    name = "waltrade_baseline_test_pre_entry_" + uuid.uuid4().hex[:10]
    server.create_database(name)
    return name, server.connect(name)


def apply(conn):
    with conn.cursor() as cur:
        cur.execute(MIGRATION)
    conn.commit()


def frozen_event(*, environment="PAPER", fee_rate=Decimal("0.0035")):
    qty = Decimal("2")
    boundary = Decimal("99.2")
    core = Decimal("1.6")
    fee = boundary * qty * fee_rate
    risk_id = deterministic_pre_entry_risk_id(RESERVATION_ID)
    return _make_event(
        pre_entry_risk_id=risk_id, event_sequence=1,
        source_event_identity=f"FROZEN:{RESERVATION_ID}",
        environment=environment,
        deployment_id="local-paper" if environment == "PAPER" else "local-live",
        account_identity_fingerprint="a" * 64, decision_id="decision-1",
        commitment_id="commitment-1", reservation_id=RESERVATION_ID,
        intent_id=None if environment == "PAPER" else "intent-1",
        order_identity="101", symbol="BTCUSDC", strategy="RSI", interval="1m",
        side="LONG", boundary_id=BOUNDARY_ID, boundary_policy_id="policy-1",
        boundary_policy_version="POSITION_RISK_BOUNDARY_AUTHORITY_V1",
        boundary_policy_fingerprint="b" * 64,
        boundary_distance_pct=Decimal("0.8"), proposed_boundary_price=boundary,
        reference_price=Decimal("100"), reference_price_timestamp=NOW,
        reference_price_source="candles.close/FRESH_20_MINUTES",
        reference_price_row_identity="candles:BTCUSDC:1m:2026-08-22T12:00:00+00:00",
        reference_price_fingerprint="c" * 64, proposed_quantity=qty,
        quantity_source=("simulated_orders.quantity_btc" if environment == "PAPER" else "LIVE_ENTRY_INTENT_V1.requested_qty"),
        quantity_evidence_fingerprint="d" * 64,
        exit_cost_snapshot_or_model_id=("PAPER_SIMULATION_FEE_MODEL_V2" if environment == "PAPER" else "snapshot-1"),
        exit_cost_evidence_fingerprint="e" * 64,
        canonical_exit_fee_rate=fee_rate, pre_entry_core_price_risk=core,
        pre_entry_exit_fee_estimate=fee, total_pre_entry_risk=core + fee,
        original_quantity=qty, transferred_quantity=Decimal("0"),
        remaining_committed_quantity=qty, released_quantity=Decimal("0"),
        evidence_status="CANONICAL", lifecycle_state="ACTIVE_COMMITTED",
        open_risk_position_id=None, open_risk_boundary_id=None,
        open_risk_evidence_fingerprint=None, runtime_revision="f" * 40,
        effective_at=NOW, source_authority=f"{environment}_ACCEPTED_ENTRY_COMMITMENT",
        provenance={"test": True}, contract_version=CONTRACT_VERSION,
    )


def setup_transition_authorities(cur, *, reservation_state="DEPLOYED"):
    cur.execute(
        "CREATE TABLE v_position_risk_boundary_current_v1("
        "reservation_id uuid,state text,position_id bigint,boundary_id uuid)"
    )
    cur.execute(
        "INSERT INTO v_position_risk_boundary_current_v1 VALUES (%s,'BOUNDARY_ACTIVATED',501,%s)",
        (str(RESERVATION_ID), str(BOUNDARY_ID)),
    )
    cur.execute(
        "CREATE TABLE v_capital_reservation_current_v1(reservation_id uuid,state text)"
    )
    cur.execute(
        "INSERT INTO v_capital_reservation_current_v1 VALUES (%s,%s)",
        (str(RESERVATION_ID), reservation_state),
    )


def test_migration_idempotent_empty_append_only_and_no_backfill(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        apply(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pre_entry_risk_event_v1")
            assert cur.fetchone()[0] == 0
            event = frozen_event()
            assert append_pre_entry_risk_event_cursor(cur, event) == "INSERTED"
            assert append_pre_entry_risk_event_cursor(cur, event) == "IDEMPOTENT"
        conn.commit()
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="PRE_ENTRY_RISK_APPEND_ONLY"):
                cur.execute("UPDATE pre_entry_risk_event_v1 SET source_authority='X'")
        conn.rollback()
    finally:
        conn.close()


def test_partial_then_full_fill_transfers_exactly_once(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_transition_authorities(cur)
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            first_status, first = transition_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID, source_event_identity="FILL:1",
                effective_at=NOW, transfer_quantity=Decimal("0.5"),
                open_risk_status="CANONICAL", open_risk_position_id=501,
                open_risk_boundary_id=BOUNDARY_ID,
                open_risk_evidence_fingerprint="1" * 64,
            )
            assert first_status == "INSERTED"
            assert first.lifecycle_state == "PARTIALLY_TRANSFERRED"
            assert first.remaining_committed_quantity == Decimal("1.5")
            assert first.total_pre_entry_risk == Decimal("1.720800")
            assert transition_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID, source_event_identity="FILL:1",
                effective_at=NOW, transfer_quantity=Decimal("0.5"),
                open_risk_status="CANONICAL", open_risk_position_id=501,
                open_risk_boundary_id=BOUNDARY_ID,
                open_risk_evidence_fingerprint="1" * 64,
            )[0] == "IDEMPOTENT"
            _, final = transition_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID, source_event_identity="FILL:2",
                effective_at=NOW, transfer_quantity=Decimal("1.5"),
                open_risk_status="CANONICAL", open_risk_position_id=501,
                open_risk_boundary_id=BOUNDARY_ID,
                open_risk_evidence_fingerprint="2" * 64,
            )
            assert final.lifecycle_state == "REPLACED_BY_OPEN_RISK"
            assert final.remaining_committed_quantity == Decimal("0")
            assert final.total_pre_entry_risk == Decimal("0")
        conn.rollback()
    finally:
        conn.close()


def test_open_risk_incomplete_keeps_pre_entry_active(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_transition_authorities(cur)
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            with pytest.raises(Exception, match="EVIDENCE_INCOMPLETE"):
                transition_pre_entry_risk_cursor(
                    cur, reservation_id=RESERVATION_ID,
                    source_event_identity="FILL:INCOMPLETE", effective_at=NOW,
                    transfer_quantity=Decimal("2"), open_risk_status="INCOMPLETE",
                )
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pre_entry_risk_event_v1")
            assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_zero_fill_and_terminal_release_need_canonical_no_fill(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_transition_authorities(cur, reservation_state="REJECTED")
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            with pytest.raises(Exception, match="EVIDENCE_INCOMPLETE"):
                release_pre_entry_risk_cursor(
                    cur, reservation_id=RESERVATION_ID,
                    source_event_identity="REJECT:1", effective_at=NOW,
                    no_unattributed_fill_status="UNKNOWN",
                )
        conn.rollback()
        with conn.cursor() as cur:
            setup_transition_authorities(cur, reservation_state="REJECTED")
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            _, released = release_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID,
                source_event_identity="REJECT:1", effective_at=NOW,
                no_unattributed_fill_status="CANONICAL_NONE",
            )
            assert released.lifecycle_state == "RELEASED"
            assert released.released_quantity == Decimal("2")
            assert released.total_pre_entry_risk == Decimal("0")
        conn.rollback()
    finally:
        conn.close()


def test_partial_fill_cancel_keeps_open_slice_and_releases_only_remainder(disposable_postgres_v16):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            setup_transition_authorities(cur, reservation_state="REJECTED")
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            transition_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID, source_event_identity="FILL:PARTIAL",
                effective_at=NOW, transfer_quantity=Decimal("0.8"),
                open_risk_status="CANONICAL", open_risk_position_id=501,
                open_risk_boundary_id=BOUNDARY_ID,
                open_risk_evidence_fingerprint="1" * 64,
            )
            _, released = release_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID,
                source_event_identity="CANCEL:REMAINDER", effective_at=NOW,
                no_unattributed_fill_status="CANONICAL_NONE",
            )
            assert released.lifecycle_state == "RELEASED"
            assert released.transferred_quantity == Decimal("0.8")
            assert released.released_quantity == Decimal("1.2")
            assert released.remaining_committed_quantity == Decimal("0")
        conn.rollback()
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("environment", "fee_rate", "expected"),
    [("PAPER", Decimal("0.0035"), Decimal("2.29440")),
     ("LIVE", Decimal("0.0010"), Decimal("1.79840"))],
)
def test_paper_and_live_fee_authorities_are_preserved(disposable_postgres_v16, environment, fee_rate, expected):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        with conn.cursor() as cur:
            append_pre_entry_risk_event_cursor(cur, frozen_event(environment=environment, fee_rate=fee_rate))
            evidence = load_committed_pre_entry_risk_evidence_cursor(
                cur, environment=environment,
                deployment_id="local-paper" if environment == "PAPER" else "local-live",
                account_identity_fingerprint="a" * 64,
            )
            assert evidence.evidence_status == "CANONICAL"
            assert evidence.total_pre_entry_risk == expected
        conn.rollback()
    finally:
        conn.close()


def test_concurrent_duplicate_freeze_is_single_event(disposable_postgres_v16):
    name, conn = database(disposable_postgres_v16)
    apply(conn)
    conn.close()
    barrier = Barrier(2)
    results = []

    def write():
        worker = disposable_postgres_v16.connect(name)
        try:
            barrier.wait()
            with worker.cursor() as cur:
                results.append(append_pre_entry_risk_event_cursor(cur, frozen_event()))
            worker.commit()
        finally:
            worker.close()

    threads = [Thread(target=write), Thread(target=write)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    check = disposable_postgres_v16.connect(name)
    try:
        with check.cursor() as cur:
            cur.execute("SELECT count(*) FROM pre_entry_risk_event_v1")
            assert cur.fetchone()[0] == 1
        assert sorted(results) == ["IDEMPOTENT", "INSERTED"]
    finally:
        check.close()


def test_paper_replay_freezes_before_fill_and_transfers_after_boundary_activation(disposable_postgres_v16, monkeypatch):
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    _, conn = database(disposable_postgres_v16)
    try:
        with conn.cursor() as cur:
            cur.execute(CAPITAL_MIGRATION)
            cur.execute(BOUNDARY_MIGRATION)
            cur.execute(MIGRATION)
            cur.execute(
                "CREATE TABLE strategy_params(symbol text,strategy text,interval text,"
                "param_name text,param_value numeric)"
            )
            cur.execute("INSERT INTO strategy_params VALUES ('SOLUSDC','BBRANGE','1m','STOP_LOSS_PCT',0.8)")
            cur.execute(
                "CREATE TABLE simulated_orders(id bigint PRIMARY KEY,symbol text,strategy text,"
                "interval text,price numeric,quantity_btc numeric,candle_open_time timestamptz,"
                "decision_id text,deployment_id text)"
            )
            cur.execute(
                "INSERT INTO simulated_orders VALUES (101,'SOLUSDC','BBRANGE','1m',93.36,0.214,%s,'decision-1','local-paper')",
                (NOW,),
            )
            cur.execute("CREATE TABLE candles(symbol text,interval text,open_time timestamptz,close numeric)")
            cur.execute("INSERT INTO candles VALUES ('SOLUSDC','1m',%s,93.36)", (NOW,))
            cur.execute(
                "CREATE TABLE simulated_execution_fills_v1(id bigint PRIMARY KEY,"
                "simulated_order_id bigint,position_id bigint,order_purpose text,"
                "fill_qty numeric,fill_price numeric,execution_at timestamptz)"
            )
            _, reservation_id = accept_paper_simulated_order_cursor(
                cur, simulated_order_id=101, deployment_id="local-paper",
                symbol="SOLUSDC", strategy="BBRANGE", interval="1m",
                requested_notional=Decimal("19.97904"), effective_at=NOW,
                decision_identity="decision-1",
            )
            assert accept_paper_boundary_cursor(
                cur, simulated_order_id=101, deployment_id="local-paper",
                decision_id="decision-1", symbol="SOLUSDC", strategy="BBRANGE",
                interval="1m", effective_at=NOW,
            ) == "INSERTED"
            status, risk_id = freeze_paper_pre_entry_risk_cursor(
                cur, simulated_order_id=101, deployment_id="local-paper",
                effective_at=NOW, runtime_revision_value="f" * 40,
            )
            assert status == "INSERTED"
            assert risk_id == deterministic_pre_entry_risk_id(reservation_id)
            cur.execute("SELECT lifecycle_state,total_pre_entry_risk FROM v_pre_entry_risk_current_v1")
            assert cur.fetchone() == ("ACTIVE_COMMITTED", Decimal("0.229199546880000000"))
            cur.execute("INSERT INTO simulated_execution_fills_v1 VALUES (1,101,501,'ENTRY',0.214,93.36,%s)", (NOW,))
            assert activate_boundary_for_position_cursor(
                cur, position_id=501, environment="PAPER", deployment_id="local-paper",
                effective_at=NOW, source_authority="TEST_PAPER_FILL",
            ) == "INSERTED"
            cur.execute(
                "SELECT boundary_id FROM v_position_risk_boundary_current_v1 "
                "WHERE reservation_id=%s", (str(reservation_id),),
            )
            paper_boundary_id = uuid.UUID(str(cur.fetchone()[0]))
            _, transferred = transition_pre_entry_risk_cursor(
                cur, reservation_id=reservation_id, source_event_identity="PAPER_FILL:1",
                effective_at=NOW, transfer_quantity=Decimal("0.214"),
                open_risk_status="CANONICAL", open_risk_position_id=501,
                open_risk_boundary_id=paper_boundary_id,
                open_risk_evidence_fingerprint="9" * 64,
            )
            assert transferred.lifecycle_state == "REPLACED_BY_OPEN_RISK"
            assert transferred.remaining_committed_quantity == 0
        conn.rollback()
    finally:
        conn.close()

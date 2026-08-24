from dataclasses import replace
from datetime import datetime, timedelta, timezone
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
from common.capital_reservation import (
    accept_paper_simulated_order_cursor,
    deploy_paper_simulated_fill_cursor,
)
from common.position_risk_boundary import (
    accept_paper_boundary_cursor,
    activate_boundary_for_position_cursor,
)
from common.simulated_execution_evidence import (
    handoff_paper_fill_pre_entry_risk_cursor,
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


def frozen_event(
    *, environment="PAPER", fee_rate=Decimal("0.0035"),
    reservation_id=RESERVATION_ID, boundary_id=BOUNDARY_ID,
    decision_id="decision-1", order_identity="101",
):
    qty = Decimal("2")
    boundary = Decimal("99.2")
    core = Decimal("1.6")
    fee = boundary * qty * fee_rate
    risk_id = deterministic_pre_entry_risk_id(reservation_id)
    return _make_event(
        pre_entry_risk_id=risk_id, event_sequence=1,
        source_event_identity=f"FROZEN:{reservation_id}",
        environment=environment,
        deployment_id="local-paper" if environment == "PAPER" else "local-live",
        account_identity_fingerprint="a" * 64, decision_id=decision_id,
        commitment_id=f"commitment-{decision_id}", reservation_id=reservation_id,
        intent_id=None if environment == "PAPER" else "intent-1",
        order_identity=order_identity, symbol="BTCUSDC", strategy="RSI", interval="1m",
        side="LONG", boundary_id=boundary_id, boundary_policy_id="policy-1",
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


def test_committed_risk_as_of_does_not_forward_fill_later_release(
    disposable_postgres_v16,
):
    _, conn = database(disposable_postgres_v16)
    try:
        apply(conn)
        frozen = frozen_event(environment="LIVE")
        with conn.cursor() as cur:
            setup_transition_authorities(cur)
            append_pre_entry_risk_event_cursor(cur, frozen)
            release_pre_entry_risk_cursor(
                cur, reservation_id=RESERVATION_ID,
                source_event_identity="CANCEL:AFTER_BOUNDARY",
                effective_at=NOW + timedelta(minutes=10),
                no_unattributed_fill_status="CANONICAL_NONE",
            )
            at_boundary = load_committed_pre_entry_risk_evidence_cursor(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
                as_of=NOW + timedelta(minutes=5),
            )
            current = load_committed_pre_entry_risk_evidence_cursor(
                cur, environment="LIVE", deployment_id="local-live",
                account_identity_fingerprint="a" * 64,
            )
            assert at_boundary.total_pre_entry_risk == frozen.total_pre_entry_risk
            assert at_boundary.active_commitment_count == 1
            assert current.total_pre_entry_risk == Decimal("0")
            assert current.active_commitment_count == 0
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


def test_concurrent_independent_commitments_do_not_cross_transfer(
    disposable_postgres_v16,
):
    name, conn = database(disposable_postgres_v16)
    apply(conn)
    second_reservation = uuid.UUID("55555555-5555-4555-8555-555555555555")
    second_boundary = uuid.UUID("66666666-6666-4666-8666-666666666666")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE v_position_risk_boundary_current_v1("
                "reservation_id uuid,state text,position_id bigint,boundary_id uuid)"
            )
            cur.execute(
                "INSERT INTO v_position_risk_boundary_current_v1 VALUES "
                "(%s,'BOUNDARY_ACTIVATED',501,%s),"
                "(%s,'BOUNDARY_ACTIVATED',502,%s)",
                (
                    str(RESERVATION_ID), str(BOUNDARY_ID),
                    str(second_reservation), str(second_boundary),
                ),
            )
            cur.execute(
                "CREATE TABLE v_capital_reservation_current_v1("
                "reservation_id uuid,state text)"
            )
            cur.execute(
                "INSERT INTO v_capital_reservation_current_v1 VALUES "
                "(%s,'DEPLOYED'),(%s,'DEPLOYED')",
                (str(RESERVATION_ID), str(second_reservation)),
            )
            append_pre_entry_risk_event_cursor(cur, frozen_event())
            append_pre_entry_risk_event_cursor(
                cur, frozen_event(
                    reservation_id=second_reservation,
                    boundary_id=second_boundary, decision_id="decision-2",
                    order_identity="102",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    barrier = Barrier(2)
    errors = []

    def transfer(reservation_id, boundary_id, position_id, fingerprint):
        worker = disposable_postgres_v16.connect(name)
        try:
            barrier.wait()
            with worker.cursor() as cur:
                transition_pre_entry_risk_cursor(
                    cur, reservation_id=reservation_id,
                    source_event_identity=f"FILL:{position_id}",
                    effective_at=NOW, transfer_quantity=Decimal("2"),
                    open_risk_status="CANONICAL",
                    open_risk_position_id=position_id,
                    open_risk_boundary_id=boundary_id,
                    open_risk_evidence_fingerprint=fingerprint * 64,
                )
            worker.commit()
        except Exception as exc:
            errors.append(exc)
        finally:
            worker.close()

    threads = [
        Thread(
            target=transfer,
            args=(RESERVATION_ID, BOUNDARY_ID, 501, "7"),
        ),
        Thread(
            target=transfer,
            args=(second_reservation, second_boundary, 502, "8"),
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []

    check = disposable_postgres_v16.connect(name)
    try:
        with check.cursor() as cur:
            cur.execute(
                "SELECT reservation_id,lifecycle_state,transferred_quantity,"
                "remaining_committed_quantity FROM v_pre_entry_risk_current_v1 "
                "ORDER BY reservation_id"
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        assert all(row[1:] == (
            "REPLACED_BY_OPEN_RISK", Decimal("2.000000000000000000"),
            Decimal("0E-18"),
        ) for row in rows)
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
                "fill_qty numeric,fill_price numeric,execution_at timestamptz,"
                "environment text,deployment_id text,simulation_model_version text,"
                "simulation_fee_rate numeric,fee_model_version text)"
            )
            cur.execute(
                "CREATE TABLE positions(id bigint PRIMARY KEY,status text,side text,"
                "remaining_inventory_qty numeric,inventory_evidence_status text,"
                "symbol text,interval text,entry_opportunity_snapshot_id uuid)"
            )
            cur.execute(
                "CREATE TABLE entry_opportunity_evidence_v1("
                "snapshot_id uuid PRIMARY KEY,fee_rate_exit_assumption numeric,"
                "fee_model_version text)"
            )
            cur.execute(
                "INSERT INTO entry_opportunity_evidence_v1 VALUES "
                "('44444444-4444-4444-8444-444444444444',0.0035,"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')"
            )
            cur.execute(
                "INSERT INTO positions VALUES "
                "(501,'OPEN','BUY',0.214,'COMPLETE','SOLUSDC','1m',"
                "'44444444-4444-4444-8444-444444444444')"
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
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=None, environment="PAPER",
                deployment_id="local-paper",
            ) == "ZERO_FILL_NOOP"
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(1,101,501,'ENTRY',0.214,93.36,%s,'PAPER','local-paper',"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',0.0035,"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')",
                (NOW,),
            )
            assert deploy_paper_simulated_fill_cursor(
                cur, simulated_order_id=101, fill_id=1, position_id=501,
                deployed_notional=Decimal("19.97904"), effective_at=NOW,
            ) == "INSERTED"
            assert activate_boundary_for_position_cursor(
                cur, position_id=501, environment="PAPER", deployment_id="local-paper",
                effective_at=NOW, source_authority="TEST_PAPER_FILL",
            ) == "INSERTED"
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=1, environment="PAPER",
                deployment_id="local-paper",
            ) == "INSERTED"
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=1, environment="PAPER",
                deployment_id="local-paper",
            ) == "IDEMPOTENT"
            cur.execute(
                "SELECT lifecycle_state,original_quantity,transferred_quantity,"
                "remaining_committed_quantity,released_quantity "
                "FROM v_pre_entry_risk_current_v1"
            )
            assert cur.fetchone() == (
                "REPLACED_BY_OPEN_RISK", Decimal("0.214000000000000000"),
                Decimal("0.214000000000000000"), Decimal("0"), Decimal("0"),
            )

            cur.execute(
                "INSERT INTO strategy_params VALUES "
                "('BTCUSDC','RSI','1m','STOP_LOSS_PCT',0.8)"
            )
            cur.execute(
                "INSERT INTO simulated_orders VALUES "
                "(102,'BTCUSDC','RSI','1m',100,2,%s,'decision-2','local-paper')",
                (NOW,),
            )
            cur.execute(
                "INSERT INTO candles VALUES ('BTCUSDC','1m',%s,100)", (NOW,),
            )
            cur.execute(
                "INSERT INTO positions VALUES "
                "(502,'OPEN','BUY',0.5,'COMPLETE','BTCUSDC','1m',"
                "'44444444-4444-4444-8444-444444444444')"
            )
            _, second_reservation_id = accept_paper_simulated_order_cursor(
                cur, simulated_order_id=102, deployment_id="local-paper",
                symbol="BTCUSDC", strategy="RSI", interval="1m",
                requested_notional=Decimal("200"), effective_at=NOW,
                decision_identity="decision-2",
            )
            assert accept_paper_boundary_cursor(
                cur, simulated_order_id=102, deployment_id="local-paper",
                decision_id="decision-2", symbol="BTCUSDC", strategy="RSI",
                interval="1m", effective_at=NOW,
            ) == "INSERTED"
            assert freeze_paper_pre_entry_risk_cursor(
                cur, simulated_order_id=102, deployment_id="local-paper",
                effective_at=NOW, runtime_revision_value="f" * 40,
            )[0] == "INSERTED"
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(2,102,502,'ENTRY',0.5,100,%s,'PAPER','local-paper',"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',0.0035,"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')",
                (NOW,),
            )
            assert deploy_paper_simulated_fill_cursor(
                cur, simulated_order_id=102, fill_id=2, position_id=502,
                deployed_notional=Decimal("50"), effective_at=NOW,
            ) == "INSERTED"
            assert activate_boundary_for_position_cursor(
                cur, position_id=502, environment="PAPER",
                deployment_id="local-paper", effective_at=NOW,
                source_authority="TEST_PAPER_PARTIAL_FILL",
            ) == "INSERTED"
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=2, environment="PAPER",
                deployment_id="local-paper",
            ) == "INSERTED"
            cur.execute(
                "SELECT lifecycle_state,transferred_quantity,"
                "remaining_committed_quantity FROM v_pre_entry_risk_current_v1 "
                "WHERE reservation_id=%s", (str(second_reservation_id),),
            )
            assert cur.fetchone() == (
                "PARTIALLY_TRANSFERRED", Decimal("0.500000000000000000"),
                Decimal("1.500000000000000000"),
            )

            cur.execute(
                "UPDATE positions SET remaining_inventory_qty=2 WHERE id=502"
            )
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES "
                "(3,102,502,'ENTRY',1.5,100,%s,'PAPER','local-paper',"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2',0.0035,"
                "'PAPER_SIMULATOR_FINANCIAL_MODEL_V2')",
                (NOW,),
            )
            assert deploy_paper_simulated_fill_cursor(
                cur, simulated_order_id=102, fill_id=3, position_id=502,
                deployed_notional=Decimal("150"), effective_at=NOW,
            ) == "INSERTED"
            assert activate_boundary_for_position_cursor(
                cur, position_id=502, environment="PAPER",
                deployment_id="local-paper", effective_at=NOW,
                source_authority="TEST_PAPER_FULL_FILL",
            ) == "IDEMPOTENT"
            cur.execute("DELETE FROM candles WHERE symbol='BTCUSDC'")
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=3, environment="PAPER",
                deployment_id="local-paper",
            ) == "OPEN_RISK_INCOMPLETE:MISSING_MARK"
            cur.execute(
                "SELECT transferred_quantity,remaining_committed_quantity "
                "FROM v_pre_entry_risk_current_v1 WHERE reservation_id=%s",
                (str(second_reservation_id),),
            )
            assert cur.fetchone() == (
                Decimal("0.500000000000000000"),
                Decimal("1.500000000000000000"),
            )
            cur.execute(
                "INSERT INTO candles VALUES ('BTCUSDC','1m',%s,100)", (NOW,),
            )
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=3, environment="PAPER",
                deployment_id="local-paper",
            ) == "INSERTED"
            assert handoff_paper_fill_pre_entry_risk_cursor(
                cur, fill_id=3, environment="PAPER",
                deployment_id="local-paper",
            ) == "IDEMPOTENT"
            cur.execute(
                "SELECT lifecycle_state,original_quantity,transferred_quantity,"
                "remaining_committed_quantity FROM v_pre_entry_risk_current_v1 "
                "WHERE reservation_id=%s", (str(second_reservation_id),),
            )
            assert cur.fetchone() == (
                "REPLACED_BY_OPEN_RISK", Decimal("2.000000000000000000"),
                Decimal("2.000000000000000000"), Decimal("0"),
            )
            cur.execute(
                "SELECT lifecycle_state FROM v_pre_entry_risk_current_v1 "
                "WHERE reservation_id=%s", (str(reservation_id),),
            )
            assert cur.fetchone()[0] == "REPLACED_BY_OPEN_RISK"
        conn.rollback()
    finally:
        conn.close()

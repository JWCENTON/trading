from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
import uuid

import common.long_horizon_l3 as long_horizon_l3
from common.exit_guards.economic_floor_v2 import CanonicalOneMinuteMark
from common.long_horizon_l3 import (
    canonical_opportunity_identity,
    finalize_admission_cursor,
    guard_exit_cursor,
    prepare_admission_cursor,
    sampling_selected,
)
from common.simulated_execution_evidence import PaperRealizableNetEvidence

V2_CONTRACT_VERSION = "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2"


ROOT = Path(__file__).resolve().parents[2]
V1_MIGRATION_PATH = ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v1.sql"
V1_ROLLBACK_PATH = ROOT / "db/rollback/20260907_long_horizon_l3_direct_local_paper_v1_rollback.sql"
V2_MIGRATION_PATH = ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v2.sql"
V2_ROLLBACK_PATH = ROOT / "db/rollback/20260907_long_horizon_l3_direct_local_paper_v2_rollback.sql"
V3_MIGRATION_PATH = ROOT / "db/migrations/20260908_long_horizon_l3_direct_local_paper_v3.sql"
V3_ROLLBACK_PATH = ROOT / "db/rollback/20260908_long_horizon_l3_direct_local_paper_v3_rollback.sql"
V4_MIGRATION_PATH = ROOT / "db/migrations/20260908_long_horizon_l3_direct_local_paper_v4.sql"
V4_ROLLBACK_PATH = ROOT / "db/rollback/20260908_long_horizon_l3_direct_local_paper_v4_rollback.sql"
V1_MIGRATION = V1_MIGRATION_PATH.read_text()
V1_ROLLBACK = V1_ROLLBACK_PATH.read_text()
V2_MIGRATION = V2_MIGRATION_PATH.read_text()
V2_ROLLBACK = V2_ROLLBACK_PATH.read_text()
V3_MIGRATION = V3_MIGRATION_PATH.read_text()
V3_ROLLBACK = V3_ROLLBACK_PATH.read_text()
V4_MIGRATION = V4_MIGRATION_PATH.read_text()
V4_ROLLBACK = V4_ROLLBACK_PATH.read_text()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bootstrap(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION pgcrypto")
        cur.execute(
            """
            CREATE TABLE regime_gate_events(
              id bigserial PRIMARY KEY,created_at timestamptz NOT NULL DEFAULT now(),
              regime text,mode text,would_block boolean,why text,meta jsonb);
            CREATE TABLE simulated_orders(
              id bigserial PRIMARY KEY,decision_id uuid,entry_opportunity_snapshot_id uuid,
              created_at timestamptz NOT NULL DEFAULT now(),position_id bigint,
              is_exit boolean NOT NULL DEFAULT false);
            CREATE TABLE positions(
              id bigint PRIMARY KEY,status text,symbol text,interval text,strategy text,
              side text,entry_time timestamptz,entry_opportunity_snapshot_id uuid);
            CREATE TABLE paper_managed_equity_observation_v1(
              deployment_id text,managed_equity_status text,managed_equity numeric,
              observed_at timestamptz);
            CREATE TABLE simulated_execution_fills_v1(
              position_id bigint,order_purpose text,fill_notional numeric,
              environment text,deployment_id text);
            CREATE TABLE bot_control(
              symbol text,interval text,strategy text,enabled boolean,
              reason text,control_mode text,regime_enabled boolean,
              regime_mode text,updated_at timestamptz,
              PRIMARY KEY(symbol,interval,strategy));
            CREATE TABLE schema_migration_ledger_v1(
              ledger_id bigserial PRIMARY KEY,migration_id text,checksum_sha256 text,
              applied_at timestamptz default now(),environment text,deployment_id text,
              database_name text,applied_by text,status text,success boolean,
              execution_duration_ms bigint,git_sha text,error_summary text,
              schema_baseline_version text);
            """
        )
        ordinal = 0
        for strategy in ("RSI", "TREND", "SUPERTREND", "BBRANGE"):
            for interval in ("1m", "5m"):
                for symbol in ("BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"):
                    ordinal += 1
                    cur.execute(
                        """INSERT INTO bot_control(
                             symbol,interval,strategy,enabled,reason,control_mode,
                             regime_enabled,regime_mode,updated_at)
                           VALUES(%s,%s,%s,true,'BASELINE','MANUAL',true,'ENFORCE',%s)""",
                        (symbol, interval, strategy,
                         datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
                         + timedelta(seconds=ordinal)),
                    )
    conn.commit()


def _settings(cur, deployment="local-paper", checksum="b" * 64) -> None:
    cur.execute("SET waltrade.test_database='on'")
    cur.execute("SET waltrade.target_deployment_id=%s", (deployment,))
    cur.execute("SET waltrade.migration_git_sha=%s", ("a" * 40,))
    cur.execute("SET waltrade.migration_checksum=%s", (checksum,))


def _prepare_v1_history(conn) -> None:
    with conn.cursor() as cur:
        _settings(cur, checksum=_sha256(V1_MIGRATION_PATH))
        cur.execute(V1_MIGRATION)
        # Reproduce the accepted historical rollback state.  The immutable V1
        # rollback intentionally accepts only the literal trading_paper name,
        # so an isolated disposable database prepares the same state directly.
        cur.execute(
            "UPDATE long_horizon_l3_contract_v1 SET status='TERMINATED' "
            "WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1'"
        )
        cur.execute(
            "UPDATE bot_control SET regime_enabled=true,regime_mode='ENFORCE' "
            "WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')"
        )
    conn.commit()


def _prepare_v3_history(conn) -> None:
    _prepare_v1_history(conn)
    with conn.cursor() as cur:
        _settings(cur, checksum=_sha256(V3_MIGRATION_PATH))
        cur.execute(V3_MIGRATION)
    conn.commit()


def _snapshot(conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT jsonb_build_object(
              'bot_control',(SELECT jsonb_agg(to_jsonb(b) ORDER BY strategy,interval,symbol)
                               FROM bot_control b),
              'contracts',(SELECT jsonb_agg(to_jsonb(c) ORDER BY contract_id)
                             FROM long_horizon_l3_contract_v1 c),
              'ledger',(SELECT jsonb_agg(to_jsonb(l) ORDER BY ledger_id)
                          FROM schema_migration_ledger_v1 l),
              'contract_seq',(SELECT last_value FROM long_horizon_l3_contract_v1_contract_id_seq),
              'ledger_seq',(SELECT last_value FROM schema_migration_ledger_v1_ledger_id_seq))"""
        )
        return cur.fetchone()[0]


def test_historical_v1_artifacts_are_byte_for_byte_unchanged():
    assert _sha256(V1_MIGRATION_PATH) == "fef79cc2f3283744abb62ebb9c3233c9c70feb79b6424ef99f125eab871890fa"
    assert _sha256(V1_ROLLBACK_PATH) == "bbb58b2e2aeafeacf0bd401a1e57549e8bd7295e957c2cb5ecb68db2d1a03e97"


def test_v2_first_apply_second_apply_zero_state_change_and_exact_bot_restore(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_l3_v2_idem_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        before = _snapshot(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V2_MIGRATION_PATH))
            cur.execute(V2_MIGRATION)
        conn.commit()
        after_first = _snapshot(conn)
        assert after_first != before
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bot_control WHERE regime_enabled AND regime_mode='DRY_RUN'")
            assert cur.fetchone()[0] == 32
            cur.execute(
                """SELECT count(*),min(treatment_fingerprint),max(treatment_fingerprint),
                          count(DISTINCT start_cutoff),count(DISTINCT created_at)
                     FROM long_horizon_l3_contract_v1 WHERE contract_version=%s AND status='ACTIVE'""",
                (V2_CONTRACT_VERSION,),
            )
            count, minimum, maximum, cutoff_count, created_count = cur.fetchone()
            assert count == cutoff_count == created_count == 1
            assert minimum == maximum == "5ac39665922d9fc5fa4b5d5a56482b760f72dfca61c601104ca1caa7ef1c5b15"
            cur.execute(
                """SELECT count(*) FROM schema_migration_ledger_v1
                    WHERE migration_id='20260907_long_horizon_l3_direct_local_paper_v2'
                      AND environment='PAPER' AND deployment_id='local-paper' AND success"""
            )
            assert cur.fetchone()[0] == 1
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V2_MIGRATION_PATH))
            cur.execute(V2_MIGRATION)
        conn.commit()
        after_second = _snapshot(conn)
        assert after_second == after_first

        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V2_ROLLBACK_PATH))
            cur.execute(V2_ROLLBACK)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT jsonb_agg(to_jsonb(b) ORDER BY strategy,interval,symbol) FROM bot_control b")
            assert cur.fetchone()[0] == before["bot_control"]
            cur.execute(
                "SELECT status FROM long_horizon_l3_contract_v1 WHERE contract_version=%s",
                (V2_CONTRACT_VERSION,),
            )
            assert cur.fetchone()[0] == "TERMINATED"
    finally:
        conn.close()


def test_v2_rejects_other_deployments_without_writes(disposable_postgres_v16):
    database = "waltrade_baseline_test_l3_v2_guard_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        before = _snapshot(conn)
        for deployment in ("vps-paper", "local-live", "vps-live"):
            try:
                with conn.cursor() as cur:
                    _settings(cur, deployment=deployment, checksum=_sha256(V2_MIGRATION_PATH))
                    cur.execute(V2_MIGRATION)
            except Exception as exc:
                assert "LONG_HORIZON_L3_V2_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in str(exc)
                conn.rollback()
            else:
                raise AssertionError(f"{deployment} migration unexpectedly succeeded")
            assert _snapshot(conn) == before
    finally:
        conn.close()


def test_v2_transitional_positions_fail_preflight(disposable_postgres_v16):
    database = "waltrade_baseline_test_l3_v2_transition_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO positions(id,status) VALUES(13546,'OPEN')")
        conn.commit()
        before = _snapshot(conn)
        try:
            with conn.cursor() as cur:
                _settings(cur, checksum=_sha256(V2_MIGRATION_PATH))
                cur.execute(V2_MIGRATION)
        except Exception as exc:
            assert "L3_V2_TRANSITIONAL_POSITIONS_STILL_OPEN" in str(exc)
            conn.rollback()
        else:
            raise AssertionError("V2 activated with an open transitional position")
        assert _snapshot(conn) == before
    finally:
        conn.close()


def test_v2_cutoff_excludes_pre_cutoff_gate_and_accepts_post_cutoff_gate(
    disposable_postgres_v16, monkeypatch,
):
    monkeypatch.setattr(long_horizon_l3, "CONTRACT_VERSION", V2_CONTRACT_VERSION)
    database = "waltrade_baseline_test_l3_v2_cutoff_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V2_MIGRATION_PATH))
            cur.execute(V2_MIGRATION)
            cur.execute(
                "ALTER TABLE long_horizon_l3_admission_v1 "
                "ADD COLUMN contract_version text"
            )
            cur.execute(
                "ALTER TABLE long_horizon_l3_admission_v1 "
                "ADD COLUMN l0_comparator_version text"
            )
        conn.commit()
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        at = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
        with conn.cursor() as cur:
            cur.execute("SELECT start_cutoff FROM long_horizon_l3_contract_v1 WHERE contract_version=%s", (V2_CONTRACT_VERSION,))
            cutoff = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO regime_gate_events(created_at,regime,mode,would_block,why,meta)
                   VALUES(%s,'TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id""",
                (cutoff - timedelta(microseconds=1),),
            )
            old_gate = cur.fetchone()[0]
            excluded = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND", side="BUY",
                candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": old_gate}, entry_price=Decimal("70000"),
                instrument_step=Decimal("0.00000001"), instrument_min_qty=Decimal("0.0001"),
                instrument_min_notional=Decimal("0"),
            )
            assert not excluded.accepted and excluded.status == "PRE_L3_EXCLUDED"

            selected_gate = None
            for offset in range(1, 300):
                cur.execute(
                    """INSERT INTO regime_gate_events(created_at,regime,mode,would_block,why,meta)
                       VALUES(%s,'TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id""",
                    (cutoff + timedelta(seconds=offset),),
                )
                candidate = int(cur.fetchone()[0])
                identity = canonical_opportunity_identity(
                    gate_event_id=candidate, symbol="BTCUSDC", interval="1m",
                    strategy="TREND", side="BUY", candle_open_time=at,
                )
                if sampling_selected(identity, "L3_REGIME_WOULD_ALLOW"):
                    selected_gate = candidate
                    break
            assert selected_gate is not None
            cur.execute(
                "INSERT INTO paper_managed_equity_observation_v1 VALUES"
                "('local-paper','CANONICAL',635.430007829136,%s)", (at,),
            )
            accepted = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND", side="BUY",
                candle_open_time=at, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": selected_gate}, entry_price=Decimal("70000"),
                instrument_step=Decimal("0.00000001"), instrument_min_qty=Decimal("0.0001"),
                instrument_min_notional=Decimal("0"),
            )
            assert accepted.accepted
        conn.rollback()
    finally:
        conn.close()


def test_v3_first_apply_second_apply_zero_change_and_open_inventory_snapshot(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_l3_v3_idem_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        with conn.cursor() as cur:
            for position_id, symbol in ((13546, "SOLUSDC"), (13547, "ETHUSDC"), (13549, "BNBUSDC")):
                cur.execute(
                    """INSERT INTO positions(
                         id,status,symbol,interval,strategy,side,entry_time,
                         entry_opportunity_snapshot_id)
                       VALUES(%s,'OPEN',%s,'1m','BBRANGE','LONG',%s,%s)""",
                    (position_id, symbol,
                     datetime(2026, 9, 7, 20, 0, tzinfo=timezone.utc),
                     uuid.uuid4()),
                )
        conn.commit()
        before = _snapshot(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V3_MIGRATION_PATH))
            cur.execute(V3_MIGRATION)
        conn.commit()
        after_first = _snapshot(conn)
        assert after_first != before
        with conn.cursor() as cur:
            cur.execute(
                """SELECT treatment_fingerprint,start_cutoff,contract_payload
                     FROM long_horizon_l3_contract_v1
                    WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
                      AND status='ACTIVE'"""
            )
            fingerprint, cutoff, payload = cur.fetchone()
            assert fingerprint == "d8266af32cb05ebb1e7554213cdd9d97b38f4410c621581a7754f91223cc561b"
            assert payload["start_cutoff"] == cutoff.isoformat()
            assert [row["position_id"] for row in payload["pre_cutoff_open_positions"]] == [
                13546, 13547, 13549
            ]
            assert {row["classification"] for row in payload["pre_cutoff_open_positions"]} == {
                "PRE_L3_EXCLUDED"
            }
            cur.execute(
                """SELECT count(*) FROM bot_control
                    WHERE enabled AND regime_enabled AND regime_mode='DRY_RUN'"""
            )
            assert cur.fetchone()[0] == 32
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V3_MIGRATION_PATH))
            cur.execute(V3_MIGRATION)
        conn.commit()
        assert _snapshot(conn) == after_first

        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V3_ROLLBACK_PATH))
            cur.execute(V3_ROLLBACK)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT jsonb_agg(to_jsonb(b) ORDER BY strategy,interval,symbol) FROM bot_control b"
            )
            assert cur.fetchone()[0] == before["bot_control"]
            cur.execute(
                """SELECT status FROM long_horizon_l3_contract_v1
                    WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'"""
            )
            assert cur.fetchone()[0] == "TERMINATED"
    finally:
        conn.close()


def test_v3_rejects_non_local_paper_deployments_without_writes(disposable_postgres_v16):
    database = "waltrade_baseline_test_l3_v3_guard_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        before = _snapshot(conn)
        for deployment in ("vps-paper", "local-live", "vps-live"):
            try:
                with conn.cursor() as cur:
                    _settings(cur, deployment=deployment, checksum=_sha256(V3_MIGRATION_PATH))
                    cur.execute(V3_MIGRATION)
            except Exception as exc:
                assert "LONG_HORIZON_L3_V3_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in str(exc)
                conn.rollback()
            else:
                raise AssertionError(f"{deployment} migration unexpectedly succeeded")
            assert _snapshot(conn) == before
    finally:
        conn.close()


def test_v3_cutoff_excludes_old_lifecycle_and_links_first_post_cutoff_assignment(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_l3_v3_cutoff_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO positions(id,status,symbol,interval,strategy,side,entry_time)
                   VALUES(13546,'OPEN','SOLUSDC','1m','BBRANGE','LONG',%s)""",
                (datetime(2026, 9, 7, 20, 0, tzinfo=timezone.utc),),
            )
            _settings(cur, checksum=_sha256(V3_MIGRATION_PATH))
            cur.execute(V3_MIGRATION)
        conn.commit()
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        monkeypatch.setattr(
            long_horizon_l3,
            "CONTRACT_VERSION",
            "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3",
        )
        with conn.cursor() as cur:
            cur.execute(
                """SELECT start_cutoff FROM long_horizon_l3_contract_v1
                    WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'"""
            )
            cutoff = cur.fetchone()[0]
            entry_candle = cutoff + timedelta(seconds=30)
            cur.execute(
                """INSERT INTO regime_gate_events(created_at,regime,mode,would_block,why,meta)
                   VALUES(%s,'TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id""",
                (cutoff - timedelta(microseconds=1),),
            )
            old_gate = cur.fetchone()[0]
            excluded = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND", side="BUY",
                candle_open_time=entry_candle, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": old_gate}, entry_price=Decimal("70000"),
                instrument_step=Decimal("0.00000001"), instrument_min_qty=Decimal("0.0001"),
                instrument_min_notional=Decimal("0"),
            )
            assert not excluded.accepted and excluded.status == "PRE_L3_EXCLUDED"

            selected_gate = None
            for offset in range(1, 500):
                created_at = cutoff + timedelta(seconds=offset)
                cur.execute(
                    """INSERT INTO regime_gate_events(created_at,regime,mode,would_block,why,meta)
                       VALUES(%s,'TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id""",
                    (created_at,),
                )
                candidate = int(cur.fetchone()[0])
                identity = canonical_opportunity_identity(
                    gate_event_id=candidate, symbol="BTCUSDC", interval="1m",
                    strategy="TREND", side="BUY", candle_open_time=entry_candle,
                )
                if sampling_selected(identity, "L3_REGIME_WOULD_ALLOW"):
                    selected_gate = candidate
                    break
            assert selected_gate is not None
            cur.execute(
                "INSERT INTO paper_managed_equity_observation_v1 VALUES"
                "('local-paper','CANONICAL',635.430007829136,%s)", (entry_candle,),
            )
            accepted = prepare_admission_cursor(
                cur, symbol="BTCUSDC", interval="1m", strategy="TREND", side="BUY",
                candle_open_time=entry_candle, requested_notional=Decimal("9"),
                provenance={"regime_gate_event_id": selected_gate}, entry_price=Decimal("70000"),
                instrument_step=Decimal("0.00000001"), instrument_min_qty=Decimal("0.0001"),
                instrument_min_notional=Decimal("0"),
            )
            assert accepted.accepted and accepted.status == "L3_ACCEPTED"
            decision_id, snapshot_id = uuid.uuid4(), uuid.uuid4()
            cur.execute(
                """INSERT INTO positions(id,status,symbol,interval,strategy,side,entry_time)
                   VALUES(20001,'OPEN','BTCUSDC','1m','TREND','LONG',%s)""",
                (entry_candle,),
            )
            cur.execute(
                """INSERT INTO simulated_orders(
                     decision_id,entry_opportunity_snapshot_id,created_at,position_id,is_exit)
                   VALUES(%s,%s,%s,20001,false) RETURNING id""",
                (decision_id, snapshot_id, cutoff + timedelta(seconds=1)),
            )
            order_id = cur.fetchone()[0]
            finalize_admission_cursor(
                cur, admission_id=accepted.admission_id,
                simulated_order_id=order_id, position_id=20001,
            )
            cur.execute(
                """SELECT contract_version,l0_comparator_version,decision_id,snapshot_id,position_id
                     FROM long_horizon_l3_admission_v1 WHERE admission_id=%s""",
                (accepted.admission_id,),
            )
            assert cur.fetchone() == (
                "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3",
                "LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1",
                decision_id, snapshot_id, 20001,
            )
            cur.execute("UPDATE positions SET status='CLOSED' WHERE id=13546")
            allowed, status = guard_exit_cursor(
                cur, symbol="SOLUSDC", interval="1m", strategy="BBRANGE",
                reason="BBRANGE_PROFIT_LOCK", candle_open_time=entry_candle,
                price=Decimal("150"),
            )
            assert allowed and status == "NOT_L3_POSITION"
            cur.execute(
                "SELECT count(*) FROM long_horizon_l3_admission_v1 WHERE position_id=13546"
            )
            assert cur.fetchone()[0] == 0
        conn.rollback()
    finally:
        conn.close()


def test_v4_first_second_apply_zero_change_and_invalidates_v3(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_l3_v4_idem_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v3_history(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO positions(id,status,symbol,interval,strategy,side,entry_time)
                   VALUES(13551,'OPEN','BTCUSDC','1m','BBRANGE','LONG',%s)""",
                (datetime(2026, 9, 8, 7, 0, tzinfo=timezone.utc),),
            )
            cur.execute(
                """INSERT INTO regime_gate_events(regime,mode,would_block,why,meta)
                   VALUES('TREND_DOWN','DRY_RUN',true,'POLICY_WOULD_BLOCK','{}')
                   RETURNING id"""
            )
            gate_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO long_horizon_l3_admission_v1(
                   gate_event_id,cohort,sampling_identity,sampling_digest,
                   same_thesis_identity,symbol,interval,strategy,side,
                   entry_candle_open_time,entry_notional,entry_price,
                   instrument_min_qty,instrument_min_notional,effective_min_notional,
                   status,position_id,contract_version,l0_comparator_version)
                   VALUES(%s,'L3_REGIME_WOULD_BLOCK_SAMPLE','v3-id',%s,'thesis',
                   'BTCUSDC','1m','BBRANGE','BUY',%s,9,78000,0.0001,0,0,
                   'ACCEPTED',13551,'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3',
                   'LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1')""",
                (gate_id, "a" * 64, datetime(2026, 9, 8, 6, 54, tzinfo=timezone.utc)),
            )
        conn.commit()
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V4_MIGRATION_PATH))
            cur.execute(V4_MIGRATION)
        conn.commit()
        after_first = _snapshot(conn)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT status,contract_payload->>'invalidation_reason'
                   FROM long_horizon_l3_contract_v1
                   WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'"""
            )
            assert cur.fetchone() == (
                "INVALID",
                "MISSING_L3_LEDGER_PLUS_HARD_RISK_SUPPRESSION_PLUS_MISSING_PAIRED_L0",
            )
            cur.execute(
                """SELECT status,contract_payload->'pre_v4_excluded_positions'
                   FROM long_horizon_l3_contract_v1
                   WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'"""
            )
            status, excluded = cur.fetchone()
            assert status == "ACTIVE"
            assert [row["position_id"] for row in excluded] == [13551]
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V4_MIGRATION_PATH))
            cur.execute(V4_MIGRATION)
        conn.commit()
        assert _snapshot(conn) == after_first
    finally:
        conn.close()


def test_v4_rejects_non_local_paper_without_writes(disposable_postgres_v16):
    database = "waltrade_baseline_test_l3_v4_guard_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v3_history(conn)
        before = _snapshot(conn)
        for deployment in ("vps-paper", "local-live", "vps-live"):
            try:
                with conn.cursor() as cur:
                    _settings(cur, deployment=deployment, checksum=_sha256(V4_MIGRATION_PATH))
                    cur.execute(V4_MIGRATION)
            except Exception as exc:
                assert "LONG_HORIZON_L3_V4_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in str(exc)
                conn.rollback()
            else:
                raise AssertionError(f"{deployment} migration unexpectedly succeeded")
            assert _snapshot(conn) == before
    finally:
        conn.close()


def test_lowercase_paper_fill_creates_v4_mark_event(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_l3_v4_env_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v3_history(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V4_MIGRATION_PATH))
            cur.execute(V4_MIGRATION)
            cur.execute("ALTER TABLE positions ADD COLUMN exit_order_id text")
            cur.execute(
                """INSERT INTO positions(id,status,symbol,interval,strategy,side,entry_time)
                   VALUES(20001,'OPEN','BTCUSDC','1m','TREND','LONG',%s)""",
                (datetime.now(timezone.utc) - timedelta(minutes=5),),
            )
            cur.execute(
                """INSERT INTO regime_gate_events(regime,mode,would_block,why,meta)
                   VALUES('TREND_UP','DRY_RUN',false,'POLICY_ALLOW','{}') RETURNING id"""
            )
            gate_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO long_horizon_l3_admission_v1(
                   gate_event_id,cohort,sampling_identity,sampling_digest,
                   same_thesis_identity,symbol,interval,strategy,side,
                   entry_candle_open_time,entry_notional,entry_price,
                   instrument_min_qty,instrument_min_notional,effective_min_notional,
                   status,position_id,contract_version,l0_comparator_version)
                   VALUES(%s,'L3_REGIME_WOULD_ALLOW','v4-id',%s,'thesis-v4',
                   'BTCUSDC','1m','TREND','BUY',%s,9,78000,0.0001,0,0,
                   'ACCEPTED',20001,'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',
                   'LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1')""",
                (gate_id, "b" * 64, datetime.now(timezone.utc)),
            )
            cur.execute(
                "INSERT INTO simulated_execution_fills_v1 VALUES(20001,'ENTRY',9,'paper','local-paper')"
            )
        conn.commit()
        now = datetime.now(timezone.utc)
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        monkeypatch.setattr(
            long_horizon_l3,
            "load_latest_finalized_canonical_one_minute_mark",
            lambda *_args, **_kwargs: CanonicalOneMinuteMark(
                "AUTHORITATIVE", "BTCUSDC", now, candle_id=1,
                close_time=now, price=Decimal("78100"), source_id="candle:1",
            ),
        )
        monkeypatch.setattr(
            long_horizon_l3,
            "load_paper_realizable_net_evidence",
            lambda *_args, **_kwargs: PaperRealizableNetEvidence(
                status="AUTHORITATIVE", position_id=20001, symbol="BTCUSDC",
                interval="1m", strategy="TREND", observed_at=now,
                mark_price=Decimal("78100"), source_candle_id="candle:1",
                realizable_net_after_all_costs=Decimal("0.01"),
            ),
        )
        result = long_horizon_l3.evaluate_target_owner_cycle(
            trading_mode="paper", symbol="BTCUSDC", interval="1m",
            strategy="TREND", connection_factory=lambda: disposable_postgres_v16.connect(database),
        )
        assert result.status == "TARGET_NOT_REACHED"
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM long_horizon_l3_event_v1 WHERE position_id=20001")
            assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_v4_hard_risk_bypasses_suppression_and_paired_l0_is_idempotent(
    disposable_postgres_v16, monkeypatch,
):
    database = "waltrade_baseline_test_l3_v4_l0_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v3_history(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V4_MIGRATION_PATH))
            cur.execute(V4_MIGRATION)
            cur.execute(
                """INSERT INTO positions(id,status,symbol,interval,strategy,side,entry_time)
                   VALUES(20002,'OPEN','SOLUSDC','1m','BBRANGE','LONG',%s)""",
                (datetime.now(timezone.utc) - timedelta(minutes=10),),
            )
            cur.execute(
                """INSERT INTO regime_gate_events(regime,mode,would_block,why,meta)
                   VALUES('TREND_DOWN','DRY_RUN',true,'POLICY_WOULD_BLOCK','{}') RETURNING id"""
            )
            gate_id = cur.fetchone()[0]
            cur.execute(
                """INSERT INTO long_horizon_l3_admission_v1(
                   gate_event_id,cohort,sampling_identity,sampling_digest,
                   same_thesis_identity,symbol,interval,strategy,side,
                   entry_candle_open_time,entry_notional,entry_price,
                   instrument_min_qty,instrument_min_notional,effective_min_notional,
                   status,position_id,contract_version,l0_comparator_version)
                   VALUES(%s,'L3_REGIME_WOULD_BLOCK_SAMPLE','v4-l0',%s,'thesis-l0',
                   'SOLUSDC','1m','BBRANGE','BUY',%s,9,100,0.001,0,0,
                   'ACCEPTED',20002,'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',
                   'LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1')""",
                (gate_id, "c" * 64, datetime.now(timezone.utc)),
            )
        conn.commit()
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        evidence = PaperRealizableNetEvidence(
            status="AUTHORITATIVE", position_id=20002, symbol="SOLUSDC",
            interval="1m", strategy="BBRANGE", observed_at=datetime.now(timezone.utc),
            mark_price=Decimal("101"), source_candle_id="candle:2",
            fee_contract_fingerprint="fee", hypothetical_exit_notional=Decimal("9.09"),
            hypothetical_exit_fee=Decimal("0.031815"), entry_notional=Decimal("9"),
            entry_fees=Decimal("0.0315"), realizable_net_after_all_costs=Decimal("0.026685"),
        )
        monkeypatch.setattr(
            long_horizon_l3, "load_paper_realizable_net_evidence",
            lambda *_args, **_kwargs: evidence,
        )
        with conn.cursor() as cur:
            allowed, status = guard_exit_cursor(
                cur, symbol="SOLUSDC", interval="1m", strategy="BBRANGE",
                reason="BBRANGE STOP LOSS LONG", candle_open_time=datetime.now(timezone.utc),
                price=Decimal("99"),
            )
            assert allowed and status == "L3_NOT_APPLICABLE_OR_PRESERVED"
        conn.rollback()
        for _ in range(2):
            with conn.cursor() as cur:
                allowed, status = guard_exit_cursor(
                    cur, symbol="SOLUSDC", interval="1m", strategy="BBRANGE",
                    reason="BBRANGE PROFIT LOCK", candle_open_time=datetime.now(timezone.utc),
                    price=Decimal("101"),
                )
                assert not allowed and status == "L3_LEGACY_EXIT_SUPPRESSED"
            conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*),gross_pnl_at_l0_exit,entry_fee_at_l0_exit,
                          exit_fee_at_l0_exit,realizable_net_at_l0_exit
                   FROM long_horizon_l3_l0_comparator_v1
                   WHERE position_id=20002
                   GROUP BY gross_pnl_at_l0_exit,entry_fee_at_l0_exit,
                            exit_fee_at_l0_exit,realizable_net_at_l0_exit"""
            )
            assert cur.fetchone() == (
                1, Decimal("0.09"), Decimal("0.0315"),
                Decimal("0.031815"), Decimal("0.026685"),
            )
    finally:
        conn.close()

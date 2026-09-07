from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
import uuid

from common.long_horizon_l3 import (
    CONTRACT_VERSION,
    canonical_opportunity_identity,
    prepare_admission_cursor,
    sampling_selected,
)


ROOT = Path(__file__).resolve().parents[2]
V1_MIGRATION_PATH = ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v1.sql"
V1_ROLLBACK_PATH = ROOT / "db/rollback/20260907_long_horizon_l3_direct_local_paper_v1_rollback.sql"
V2_MIGRATION_PATH = ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v2.sql"
V2_ROLLBACK_PATH = ROOT / "db/rollback/20260907_long_horizon_l3_direct_local_paper_v2_rollback.sql"
V1_MIGRATION = V1_MIGRATION_PATH.read_text()
V1_ROLLBACK = V1_ROLLBACK_PATH.read_text()
V2_MIGRATION = V2_MIGRATION_PATH.read_text()
V2_ROLLBACK = V2_ROLLBACK_PATH.read_text()


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
              id bigserial PRIMARY KEY,decision_id uuid,entry_opportunity_snapshot_id uuid);
            CREATE TABLE positions(id bigint PRIMARY KEY,status text);
            CREATE TABLE paper_managed_equity_observation_v1(
              deployment_id text,managed_equity_status text,managed_equity numeric,
              observed_at timestamptz);
            CREATE TABLE simulated_execution_fills_v1(
              position_id bigint,order_purpose text,fill_notional numeric,
              environment text,deployment_id text);
            CREATE TABLE bot_control(
              symbol text,interval text,strategy text,regime_enabled boolean,
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
                        "INSERT INTO bot_control VALUES(%s,%s,%s,true,'ENFORCE',%s)",
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
                (CONTRACT_VERSION,),
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
                (CONTRACT_VERSION,),
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
            cur.execute("INSERT INTO positions VALUES(13546,'OPEN')")
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
    database = "waltrade_baseline_test_l3_v2_cutoff_" + uuid.uuid4().hex[:10]
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    try:
        _bootstrap(conn)
        _prepare_v1_history(conn)
        with conn.cursor() as cur:
            _settings(cur, checksum=_sha256(V2_MIGRATION_PATH))
            cur.execute(V2_MIGRATION)
        conn.commit()
        monkeypatch.setenv("TRADING_MODE", "PAPER")
        monkeypatch.setenv("DEPLOYMENT_ID", "local-paper")
        monkeypatch.setenv("LONG_HORIZON_L3_MODE", "TREATMENT")
        at = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
        with conn.cursor() as cur:
            cur.execute("SELECT start_cutoff FROM long_horizon_l3_contract_v1 WHERE contract_version=%s", (CONTRACT_VERSION,))
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
            assert not excluded.accepted and excluded.status == "PRE_L3_TRANSITIONAL_EXCLUDED"

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

from datetime import datetime, timedelta, timezone
import json
from unittest.mock import patch

import pytest

from scripts import l3_market_context_shadow_v1 as s


def opportunity():
    return dict(deployment_id="local-paper", environment="trading_paper", raw_signal_state="PRESENT",
                base_decision="BUY", outcome_eligible=True, observation_type="GATE_BLOCKED",
                symbol="BTCUSDC", interval="1m", strategy="BBRANGE", candle_open_time="2026-09-09T10:00:00Z",
                evaluation_started_at="2026-09-09T10:01:02Z", reference_price="100", fee_rate_entry="0.0035",
                fee_rate_exit="0.0035")


def gate():
    return dict(id=1, decision="ENTRY_CHECK", why="POLICY_WOULD_BLOCK", would_block=True)


@pytest.mark.parametrize("field,value", [("raw_signal_state","ABSENT"),("base_decision","NO_TRADE"),
    ("observation_type","ALREADY_OPEN_BLOCK"),("observation_type","POSITION_HOLD"),
    ("deployment_id","vps-paper"),("environment","trading_live")])
def test_excluded(field, value):
    o = opportunity()
    o[field] = value
    assert not s.eligible(o, gate())


def test_actual_rejected_opportunity_included():
    assert s.eligible(opportunity(), gate())


def test_sampling_is_frozen_and_has_no_authority():
    a = s.assignment(opportunity(), gate())
    assert a == s.assignment(opportunity(), gate())
    assert a["authority"].endswith("NOT_EXECUTION_DECISION")
    assert a["selected"] == (int(a["digest"],16) < s.THRESHOLDS[a["cohort"]])


def bars():
    start = datetime(2026,9,9,10,tzinfo=timezone.utc)
    return [dict(id=i,open_time=(start+timedelta(minutes=i)).isoformat(),
                 close_time=(start+timedelta(minutes=i+1,microseconds=-1)).isoformat(),
                 open=100,close=101,high=102,low=99,volume=10) for i in range(15)]


def test_features_never_use_future_candles_and_require_complete_path():
    b = bars()
    at = "2026-09-09T10:15:00Z"
    baseline = s.context(b,at,15)
    assert baseline["status"] == "AVAILABLE"
    future = dict(b[-1],id=99,open_time=at,close_time="2026-09-09T10:16:00Z",close=999999)
    assert s.context(b+[future],at,15) == baseline
    assert s.context(b,"2026-09-09T10:15:12Z",15) == baseline
    assert s.context(b[:-1],at,15)["status"] == s.NA


def test_fee_aware_targets():
    t = s.targets("100","0.0035","0.0035")
    for n,p in t.items():
        net = s.Decimal(p)/100 * s.Decimal("0.9965") - s.Decimal("1.0035")
        assert abs(net-s.Decimal(n)/100) < s.Decimal("1e-25")


def test_high_is_not_fill_and_outcomes_cannot_change_features():
    o = opportunity()
    snapshot = {"opportunity":o}
    original = s.encode(snapshot)
    b = bars()
    for c in b:
        c["close"] = 100
        c["high"] = 100000
    out = s.path_outcome(snapshot,b)
    assert out["touches"]["3"] == "PENDING_RIGHT_CENSORED"
    assert s.encode(snapshot) == original
    assert out["right_censored"]


def test_active_database_transport_readonly_and_explicit_paper():
    with patch.object(s.subprocess,"run") as run:
        run.return_value.stdout = '{"ok":true}\n'
        assert s.query("SELECT jsonb_build_object('ok',true)") == [{"ok":True}]
        args, kwargs = run.call_args
        assert ".env.okx.paper" in args[0]
        assert "default_transaction_read_only=on" in args[0][-1]
        assert kwargs["input"].startswith("BEGIN READ ONLY;")
        assert kwargs["input"].endswith("ROLLBACK;\n")
    for sql in ("UPDATE positions SET status='CLOSED'", "SELECT 1; DELETE FROM positions"):
        with pytest.raises(ValueError):
            s.query(sql)


def test_external_store_immutable_restart(tmp_path):
    db = s.open_store(tmp_path)
    with patch.object(s,"check_contract",return_value={"start_cutoff":"2026-09-08T20:04:40Z"}):
        first = s.initialize(db)
        assert first == s.initialize(db)
    db.close()
    db = s.open_store(tmp_path)
    assert s.initialize(db) == first
    assert db.execute("SELECT count(*) FROM contract").fetchone()[0] == 1


def test_contract_has_no_external_or_trading_authority(tmp_path):
    db = s.open_store(tmp_path)
    with patch.object(s,"check_contract",return_value={"start_cutoff":"2026-09-08T20:04:40Z"}):
        c = s.initialize(db)
    assert c["trading_authority"] == "NONE"
    assert c["external_sources"] == s.NA
    assert c["portfolio_reuse"] == "NOT_IDENTIFIABLE_IN_V1"


def test_collection_restart_no_duplicate_and_outcomes_separate(tmp_path):
    db = s.open_store(tmp_path)
    o = opportunity()
    o["observation_key"] = "immutable-test-key"
    o["evaluation_started_at"] = "2026-09-09T10:15:00Z"
    o["candle_open_time"] = "2026-09-09T10:14:00Z"
    item = {"opportunity":o,"gate":gate(),"decision":{"event_id":"test"}}
    candles = [dict(c,symbol="BTCUSDC") for c in bars()]
    def read(sql):
        if "SELECT jsonb_build_object('opportunity'" in sql:
            return [item]
        return []
    with patch.object(s,"check_contract",return_value={"start_cutoff":"2026-09-08T20:04:40Z"}), \
         patch.object(s,"now",return_value="2026-09-09T10:00:00+00:00"):
        contract = s.initialize(db)
    with patch.object(s,"check_contract"),patch.object(s,"query",side_effect=read), \
         patch.object(s,"candles_for",return_value=candles), \
         patch.object(s,"now",return_value="2026-09-09T10:16:00+00:00"):
        assert s.collect(db,contract)["added"] == 1
        frozen = db.execute("SELECT payload,hash FROM snapshots").fetchone()
        with patch.object(s,"now",return_value="2026-09-09T10:17:00+00:00"):
            assert s.collect(db,contract)["added"] == 0
        assert db.execute("SELECT payload,hash FROM snapshots").fetchone() == frozen
    assert db.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 1


@pytest.mark.parametrize("fraction", ["", ".4", ".41", ".419", ".4195", ".41953", ".419530", ".419530000"])
@pytest.mark.parametrize("zone", ["Z", "+00:00", "+0200", "-05:30"])
def test_timestamp_variable_precision_and_offset(fraction, zone):
    value = "2026-09-09T14:12:32" + fraction + zone
    parsed = s.dt(value)
    assert parsed.tzinfo is not None
    expected = int((fraction.lstrip(".") + "000000")[:6])
    assert parsed.microsecond == expected


def test_exact_production_failure():
    assert s.dt("2026-09-09T14:12:32.41953+00:00") == datetime(2026,9,9,14,12,32,419530,tzinfo=timezone.utc)


@pytest.mark.parametrize("value", ["broken", "2026-09-09T14:12:32", "2026-09-09T14:12:32.123456789Z",
                                  "2026-19-09T14:12:32Z", "2026-09-09T14:12:32+25:00"])
def test_timestamp_rejects_ambiguity(value):
    with pytest.raises(ValueError):
        s.dt(value)


def test_bad_record_is_explicit_other_records_continue_and_retry_resolves(tmp_path):
    db = s.open_store(tmp_path)
    good = dict(opportunity(),observation_key="good",evaluation_started_at="2026-09-09T10:15:00Z",
                candle_open_time="2026-09-09T10:14:00Z")
    bad = dict(good,observation_key="bad",evaluation_started_at="INVALID-TIMESTAMP")
    rows = [{"opportunity":o,"gate":gate(),"decision":{"event_id":o["observation_key"]}} for o in (bad,good)]
    def read(sql):
        return rows if "paper_opportunity_observation_v1 o JOIN" in sql and "SELECT jsonb_build_object('opportunity'" in sql else []
    contract = {"start_utc":"2026-09-09T10:00:00Z","fingerprint":"test"}
    with patch.object(s,"query",side_effect=read),patch.object(s,"check_contract"), \
         patch.object(s,"candles_for",return_value=[dict(c,symbol="BTCUSDC") for c in bars()]), \
         patch.object(s,"now",return_value="2026-09-09T10:16:00+00:00"):
        result = s.run_cycle(db,contract)
        assert result["snapshots"] == 1 and result["explicit_errors"] == 1
        err = db.execute("SELECT identity,payload FROM processing_errors WHERE resolved_at IS NULL").fetchone()
        assert err[0] == "bad" and "INVALID-TIMESTAMP" in err[1]
        # Reprocess the same record after its source becomes valid, never create duplicates.
        bad["evaluation_started_at"] = good["evaluation_started_at"]
        with patch.object(s,"now",return_value="2026-09-09T10:17:00+00:00"):
            result = s.run_cycle(db,contract)
        assert result["snapshots"] == 2 and result["explicit_errors"] == 0
        assert db.execute("SELECT resolved_at FROM processing_errors WHERE identity='bad'").fetchone()[0]


def test_cycle_source_failure_persisted_not_raised(tmp_path):
    db = s.open_store(tmp_path)
    with patch.object(s,"collect",side_effect=TimeoutError("source unavailable")):
        assert s.run_cycle(db,{"fingerprint":"test"})["collector_status"] == "DEGRADED_EXPLICIT_CYCLE_ERROR"
    assert db.execute("SELECT identity FROM processing_errors").fetchone()[0] == "__cycle__"


def test_post_event_source_reads_cannot_be_promoted(tmp_path):
    db = s.open_store(tmp_path)
    c = dict(bars()[0],symbol="BTCUSDC")
    at = "2026-09-09T10:16:00+00:00"
    snap = {"opportunity":dict(opportunity(),evaluation_started_at="2026-09-09T10:15:00Z"),
            "recorded_at":at,"symbol_context":{"status":"AVAILABLE"},"btc_context":{}}
    s.save_source_reads(db,"candle",[c],at)
    s.assess_snapshot(db,"late",snap,[("candle",c)])
    a = json.loads(db.execute("SELECT payload FROM snapshot_assessments").fetchone()[0])
    assert not a["pre_entry_filter_eligible"]
    assert a["source_availability"] == "UNKNOWN"
    assert a["snapshot_kind"] == "POST_EVENT_RECONSTRUCTION"
    assert not a["pre_entry_prediction"]


def test_proof_requires_same_values_observed_before_decision(tmp_path):
    db = s.open_store(tmp_path)
    c = dict(bars()[0],symbol="BTCUSDC")
    s.save_source_reads(db,"candle",[c],"2026-09-09T10:02:00Z")
    snap = {"opportunity":dict(opportunity(),evaluation_started_at="2026-09-09T10:15:00Z"),
            "recorded_at":"2026-09-09T10:16:00Z","symbol_context":{"status":"AVAILABLE"},"btc_context":{}}
    s.assess_snapshot(db,"proven",snap,[("candle",c)])
    s.assess_snapshot(db,"changed",snap,[("candle",dict(c,close=999))])
    rows = {i:json.loads(p) for i,p in db.execute("SELECT identity,payload FROM snapshot_assessments")}
    assert rows["proven"]["pre_entry_filter_eligible"]
    assert not rows["changed"]["pre_entry_filter_eligible"]


def test_old_snapshot_is_preserved_and_downgraded_additively(tmp_path):
    db = s.open_store(tmp_path)
    snap={"opportunity":opportunity(),"recorded_at":"2026-09-09T10:15:00Z",
          "symbol_context":{"status":"AVAILABLE"},"btc_context":{},"lookahead_status":"OLD_PASS"}
    original = s.encode(snap)
    db.execute("INSERT INTO snapshots VALUES(?,?,?,?)",("old",opportunity()["evaluation_started_at"],original,s.fingerprint(snap)))
    s.initialize_release(db)
    assert db.execute("SELECT payload FROM snapshots").fetchone()[0] == original
    a = json.loads(db.execute("SELECT availability_assessment FROM effective_snapshots").fetchone()[0])
    assert a["full_no_lookahead_proof"] == "UNKNOWN"
    assert a["legacy_evidence"]

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
        if "paper_opportunity_observation_v1 o" in sql:
            return [item]
        return []
    with patch.object(s,"check_contract",return_value={"start_cutoff":"2026-09-08T20:04:40Z"}), \
         patch.object(s,"now",return_value="2026-09-09T10:00:00+00:00"):
        contract = s.initialize(db)
    with patch.object(s,"check_contract"),patch.object(s,"query",side_effect=read), \
         patch.object(s,"candles_for",return_value=candles), \
         patch.object(s,"now",side_effect=["2026-09-09T10:16:00+00:00","2026-09-09T10:17:00+00:00"]):
        assert s.collect(db,contract)["added"] == 1
        frozen = db.execute("SELECT payload,hash FROM snapshots").fetchone()
        assert s.collect(db,contract)["added"] == 0
        assert db.execute("SELECT payload,hash FROM snapshots").fetchone() == frozen
    assert db.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM outcomes").fetchone()[0] == 1

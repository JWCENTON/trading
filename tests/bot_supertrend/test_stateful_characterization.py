from __future__ import annotations

import pytest
import pandas as pd
from types import SimpleNamespace

from tests.bot_supertrend.fixtures import RAW_EXECUTION_SCENARIOS, candle


def run_real_outer_cycle(
    module, monkeypatch, *, latest, prev, harness=None,
    calculation_failure=False, writer_failure=False, commit_failure=False,
    callback_failure=False, strategy_marker=False,
):
    operations = harness.operation_log if harness is not None else []
    start = len(operations)
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    frame = pd.DataFrame([
        {"id": i + 1, "open_time": base + pd.Timedelta(minutes=i),
         "open": 100.0 + i * 0.01, "high": 100.1 + i * 0.01,
         "low": 99.9 + i * 0.01, "close": 100.0 + i * 0.01}
        for i in range(30)
    ])
    if calculation_failure:
        frame = frame.drop(columns=["close"])

    class Cursor:
        def __init__(self, role):
            self.role = role

        def executemany(self, _sql, _rows):
            operations.append("writer_execute")
            if writer_failure:
                raise RuntimeError("writer failed")

        def close(self):
            operations.append(f"{self.role}_cursor_close")

    class Conn:
        def __init__(self, role):
            self.role = role
            self.cursor_obj = Cursor(role)

        def cursor(self):
            return self.cursor_obj

        def set_session(self, *, readonly):
            operations.append("read_set_readonly")

        def rollback(self):
            operations.append(f"{self.role}_rollback")

        def commit(self):
            operations.append(f"{self.role}_commit")
            if self.role == "writer" and commit_failure:
                raise RuntimeError("commit failed")

        def close(self):
            operations.append(f"{self.role}_close")

    connections = [Conn("read"), Conn("writer")]

    def get_conn():
        conn = connections.pop(0)
        operations.append(f"{conn.role}_open")
        return conn

    monkeypatch.setattr(module, "get_db_conn", get_conn)
    monkeypatch.setattr(
        module.pd, "read_sql_query",
        lambda *_a, **_k: operations.append("read_query") or frame.copy(),
    )
    monkeypatch.setattr(module, "load_runtime_params", lambda: operations.append("runtime_params"))
    monkeypatch.setattr(module, "fetch_klines", lambda: operations.append("fetch_klines") or [])
    monkeypatch.setattr(module, "save_klines", lambda rows: operations.append("save_klines"))
    monkeypatch.setattr(module, "get_last_closed_candle", lambda: operations.append("read_latest") or latest)
    monkeypatch.setattr(module, "get_prev_closed_candle", lambda: operations.append("read_prev") or prev)
    if strategy_marker:
        monkeypatch.setattr(module, "run_strategy", lambda *_a: operations.append("run_strategy"))
    if harness is None:
        monkeypatch.setattr(
            module, "emit_strategy_event",
            lambda **event: operations.append(f"event:{event['event_type']}"),
        )
    monkeypatch.setattr(
        module, "lifecycle_heartbeat",
        lambda status, **_meta: operations.append(f"lifecycle:{status}"),
    )
    original_progress = module.IndicatorProgressHeartbeat
    ticks = iter(range(1, 100))

    def progress_factory(**kwargs):
        reporter = original_progress(
            cycle_started_at=kwargs["cycle_started_at"], interval_s=0,
            monotonic=lambda: float(next(ticks)),
        )

        def record(phase, processed, total):
            operations.append("indicator_callback:entered")
            operations.append(f"indicator:{phase}:{processed}/{total}")
            if callback_failure:
                operations.append("indicator_callback:raised")
                raise RuntimeError("progress failed")
            reporter(phase, processed, total)

        return record

    monkeypatch.setattr(module, "IndicatorProgressHeartbeat", progress_factory)
    module.run_loop_cycle(object(), 0.0)
    return operations[start:]


def reasons(observed):
    return [event.get("reason") for event in observed.events]


def use_execution(harness, scenario_name):
    harness.apply_execution_scenario(RAW_EXECUTION_SCENARIOS[scenario_name])


def test_final_decision_no_signal_and_position_hold(harness):
    no_signal = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=1))
    assert no_signal.final_decision.decision_type.value == "NO_TRADE"
    assert no_signal.final_decision.reason_code.value == "NO_SIGNAL"
    harness.set_position()
    hold = harness.strategy_cycle(candle(minute=1, price=100.1), candle(direction=1))
    assert hold.final_decision.action == "HOLD"
    assert hold.final_decision.reason_text == "POSITION_HOLD"


def test_final_decision_regime_block_preserves_metadata(harness):
    harness.regime_allow = False
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    decision = observed.final_decision
    assert decision.decision_type.value == "ENTRY_BLOCKED"
    assert decision.reason_text == "REGIME_BLOCK"
    assert decision.details["why"] == "fixture"
    assert not observed.attempts


@pytest.mark.parametrize(
    "scenario, decision_type, subtype",
    [
        ("PAPER", "PAPER_SIMULATION", "PAPER_ONLY"),
        ("LIVE_SUPPRESSION", "ACTION_SUPPRESSED", "EXECUTION_NOT_ATTEMPTED"),
        ("ENTRY_FULL", "TRADE_EXECUTED", "EXECUTED"),
        ("ENTRY_PARTIAL", "TECHNICAL_FAILURE", "PARTIAL_EXECUTION"),
        ("ENTRY_ACK_ONLY", "TECHNICAL_FAILURE", "ORDER_ACCEPTED_NOT_FILLED"),
        ("ENTRY_REJECTION", "TECHNICAL_FAILURE", "ORDER_REJECTED"),
        ("DB_GUARD", "TECHNICAL_FAILURE", "LEDGER_FAILURE"),
    ],
)
def test_final_decision_entry_execution_mapping(
    harness, scenario, decision_type, subtype,
):
    use_execution(harness, scenario)
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    decision = observed.final_decision
    assert decision.decision_type.value == decision_type
    assert decision.decision_subtype.value == subtype
    assert decision.details["legacy_reason"].startswith("SUPERTREND flip DOWN->UP")
    assert decision.details["execution_stage"]
    assert len(observed.attempts) == 1


@pytest.mark.parametrize(
    "price, reason",
    [(102.0, "TAKE_PROFIT"), (99.0, "STOP_LOSS")],
)
def test_final_decision_exit_reason_and_exactly_once(harness, price, reason):
    use_execution(harness, "EXIT_FULL")
    harness.set_position()
    observed = harness.strategy_cycle(candle(price=price), candle(minute=-1))
    decision = observed.final_decision
    assert decision.action == "EXIT"
    assert decision.reason_code.value == reason
    assert decision.details["legacy_reason"] == decision.reason_text
    assert len(observed.attempts) == 1
    assert sum(mutation[0] == "CLOSE" for mutation in observed.mutations) == 1


def test_final_decision_profit_lock_and_flip(harness):
    use_execution(harness, "PAPER")
    harness.set_position()
    harness.profit_lock_state = "TRIGGERED"
    profit_lock = harness.strategy_cycle(candle(price=100.4), candle(minute=-1))
    assert profit_lock.final_decision.reason_code.value == "PROFIT_LOCK"
    harness.set_position()
    harness.profit_lock_state = "DISABLED"
    flip = harness.strategy_cycle(candle(minute=1, direction=-1), candle(direction=1))
    assert flip.final_decision.reason_code.value == "STRATEGY_EXIT"


def test_entry_hold_hold_flip_exit_sequence(harness):
    use_execution(harness, "ENTRY_FULL")
    entry = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    hold1 = harness.strategy_cycle(candle(minute=1, price=100.2, direction=1), candle(direction=1))
    hold2 = harness.strategy_cycle(candle(minute=2, price=100.3, direction=1), candle(minute=1, direction=1))
    exited = harness.strategy_cycle(candle(minute=3, price=100.3, direction=-1), candle(minute=2, direction=1))
    assert entry.position is not None
    assert hold1.position is not None and hold2.position is not None
    assert exited.position is None
    assert exited.attempts[0].reason.startswith("SUPERTREND EXIT ON FLIP DOWN")


def test_no_action_entry_duplicate_hold_sequence(harness, monkeypatch):
    use_execution(harness, "ENTRY_FULL")
    harness.module.LAST_PROCESSED_OPEN_TIME = None
    t1 = candle(direction=1)
    t2 = candle(minute=1, direction=1)
    t3 = candle(minute=2, price=100.2, direction=1)
    no_action = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness, latest=t1,
        prev=candle(minute=-1, direction=1),
    )
    entry = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness, latest=t2,
        prev=candle(direction=-1),
    )
    duplicate = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness, latest=t2,
        prev=candle(direction=-1),
    )
    hold = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness, latest=t3, prev=t2,
    )
    assert "event:RUN_START" in no_action and "event:BLOCKED" in no_action
    assert "mutation:open" in entry and harness.position is not None
    assert not any(op.startswith("event:") for op in duplicate)
    assert "heartbeat:strategy" in hold and "execution:entry" not in hold


def test_strategy_cycle_always_calls_strategy_for_same_candle(harness):
    same = candle(direction=1)
    harness.strategy_cycle(same, candle(minute=-1, direction=1))
    harness.strategy_cycle(same, candle(minute=-1, direction=1))
    assert harness.strategy_cycle_invocations == 2
    assert sum(event["event_type"] == "RUN_START" for event in harness.events) == 2


def test_profit_lock_armed_hold_then_exit(harness):
    use_execution(harness, "PAPER")
    harness.set_position(price=100.0)
    harness.profit_lock_state = "ARMED"
    armed = harness.strategy_cycle(candle(minute=1, price=100.3), candle(direction=1))
    harness.profit_lock_state = "TRIGGERED"
    exited = harness.strategy_cycle(candle(minute=2, price=100.4), candle(minute=1, direction=1))
    assert "ARMED_WAITING" in reasons(armed)
    assert exited.position is None
    assert "TRAIL_DROP" in reasons(exited)


def test_real_outer_success_no_signal_runs_real_strategy(harness, monkeypatch):
    harness.module.LAST_PROCESSED_OPEN_TIME = None
    operations = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness, latest=candle(direction=1),
        prev=candle(minute=-1, direction=1),
    )
    assert operations[0] == "lifecycle:RUNNING"
    assert operations.index("runtime_params") < operations.index("fetch_klines")
    assert operations.index("fetch_klines") < operations.index("save_klines")
    assert operations.index("read_close") < operations.index("indicator:LOAD_HISTORY:30/30")
    phases = ["LOAD_HISTORY", "EMA", "RSI", "ATR", "SUPERTREND_LOOP", "PERSIST_LATEST"]
    phase_indexes = [next(i for i, item in enumerate(operations)
                          if item.startswith(f"indicator:{phase}:")) for phase in phases]
    assert phase_indexes == sorted(phase_indexes)
    assert operations.index("writer_open") < operations.index("writer_execute")
    assert operations.index("writer_execute") < operations.index("writer_commit")
    assert operations.index("writer_commit") < operations.index("writer_close")
    assert operations.index("writer_close") < operations.index("read_latest")
    assert operations.index("read_latest") < operations.index("read_prev")
    assert operations.index("event:RUN_START") < operations.index("heartbeat:strategy")
    assert "event:RUN_END" in operations
    assert "NO_SIGNAL" in [event.get("reason") for event in harness.events]
    assert operations[-1] == "lifecycle:CYCLE_OK"


def test_real_outer_live_confirmed_fill_entry(harness, monkeypatch):
    harness.module.LAST_PROCESSED_OPEN_TIME = None
    use_execution(harness, "ENTRY_PARTIAL_CYCLE")
    operations = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness,
        latest=candle(direction=1), prev=candle(minute=-1, direction=-1),
    )
    assert len(harness.attempts) == 1
    result = harness.last_execution_result
    assert result["requested_qty"] == 0.1
    assert result["executed_qty"] == 0.04
    assert result["requested_qty"] != result["executed_qty"]
    assert harness.position[2] == 0.04
    assert result["order_id"] == "oid-entry-partial-404"
    assert result["exchange_status"] == "PARTIALLY_FILLED"
    assert result["resp"]["executedQty"] == "0.04"
    assert result["resp"]["origQty"] == "0.10"
    assert "full" not in result["resp"]["scenario_marker"]
    assert "FILLED" not in result["resp"]["status"].replace("PARTIALLY_FILLED", "")
    assert sum(mutation[0] == "OPEN" for mutation in harness.mutations) == 1
    assert operations.count("execution:entry") == 1
    assert operations.index("execution:entry") < operations.index("mutation:open")
    assert operations.index("mutation:open") < operations.index("event:RUN_END")
    assert operations[-1] == "lifecycle:CYCLE_OK"
    assert any(item.startswith("indicator:PERSIST_LATEST") for item in operations)


def test_real_outer_panic_full_exit_preserves_panic(harness, monkeypatch):
    harness.module.LAST_PROCESSED_OPEN_TIME = None
    harness.mode = "PANIC"
    use_execution(harness, "PANIC_FULL")
    harness.set_position()
    operations = run_real_outer_cycle(
        harness.module, monkeypatch, harness=harness,
        latest=candle(), prev=candle(minute=-1),
    )
    assert harness.position is None and harness.mode == "PANIC"
    assert len(harness.attempts) == 1 and harness.attempts[0].side == "SELL"
    sent = next(event for event in harness.events
                if event.get("event_type") == "LIVE_ORDER_SENT")
    assert sent["reason"] == "OK"
    assert harness.last_execution_result["live_ok"] is True
    assert sum(mutation[0] == "CLOSE" for mutation in harness.mutations) == 1
    assert not any(mutation[0] == "MODE" for mutation in harness.mutations)
    assert operations.index("execution:exit") < operations.index("mutation:close")
    assert operations.index("mutation:close") < operations.index("event:RUN_END")
    assert operations[-1] == "lifecycle:CYCLE_OK"


def test_natural_duplicate_duplicate_new_candle_sequence(harness, monkeypatch):
    module = harness.module
    module.LAST_PROCESSED_OPEN_TIME = None
    t1 = candle(direction=1)
    t2 = candle(minute=1, direction=1)
    cycle1 = run_real_outer_cycle(module, monkeypatch, harness=harness, latest=t1,
                                  prev=candle(minute=-1, direction=1))
    duplicate1 = run_real_outer_cycle(module, monkeypatch, harness=harness, latest=t1,
                                      prev=candle(minute=-1, direction=1))
    duplicate2 = run_real_outer_cycle(module, monkeypatch, harness=harness, latest=t1,
                                      prev=candle(minute=-1, direction=1))
    cycle4 = run_real_outer_cycle(module, monkeypatch, harness=harness, latest=t2, prev=t1)
    assert "event:RUN_START" in cycle1 and module.LAST_PROCESSED_OPEN_TIME == t2[0]
    for duplicate in (duplicate1, duplicate2):
        assert duplicate[0] == "lifecycle:RUNNING"
        assert "runtime_params" in duplicate and "fetch_klines" in duplicate
        assert any(item.startswith("indicator:PERSIST_LATEST") for item in duplicate)
        assert "read_latest" in duplicate and "read_prev" in duplicate
        assert not any(item.startswith("event:") for item in duplicate)
        assert not any(item in duplicate for item in (
            "event:RUN_START", "event:RUN_END", "heartbeat:strategy",
            "execution:entry", "execution:exit", "mutation:open", "mutation:close",
            "mutation:mode",
        ))
        assert duplicate[-1] == "lifecycle:CYCLE_OK"
    assert "event:RUN_START" in cycle4


def test_real_outer_no_row_runs_indicators_and_finishes_cycle(supertrend, monkeypatch):
    supertrend.LAST_PROCESSED_OPEN_TIME = None
    operations = run_real_outer_cycle(
        supertrend, monkeypatch, latest=None, prev=None, strategy_marker=True,
    )
    assert "read_latest" in operations and "read_prev" in operations
    assert operations.index("read_latest") < operations.index("read_prev")
    assert any(item.startswith("indicator:LOAD_HISTORY") for item in operations)
    assert any(item.startswith("indicator:PERSIST_LATEST") for item in operations)
    assert "run_strategy" not in operations
    assert supertrend.LAST_PROCESSED_OPEN_TIME is None
    assert operations[-1] == "lifecycle:CYCLE_OK"


@pytest.mark.parametrize(
    "failure, expected_marker",
    [("calculation", "read_close"), ("writer", "writer_execute"),
     ("commit", "writer_commit"),
     ("callback", "indicator:LOAD_HISTORY:30/30")],
)
def test_real_outer_indicator_failure_chronology(
    supertrend, harness, monkeypatch, failure, expected_marker,
):
    target = harness.module if failure == "callback" else supertrend
    target.LAST_PROCESSED_OPEN_TIME = None
    operations = run_real_outer_cycle(
        target, monkeypatch, harness=harness if failure == "callback" else None,
        latest=candle(), prev=candle(minute=-1),
        strategy_marker=failure != "callback",
        calculation_failure=failure == "calculation",
        writer_failure=failure == "writer",
        commit_failure=failure == "commit",
        callback_failure=failure == "callback",
    )
    assert expected_marker in operations
    if failure == "callback":
        assert "indicator_callback:raised" in operations
        assert operations.index("indicator_callback:raised") < operations.index("writer_execute")
        assert "event:RUN_START" in operations and "event:RUN_END" in operations
        assert "writer_execute" in operations and "writer_commit" in operations
        assert operations[-1] == "lifecycle:CYCLE_OK"
    else:
        assert "run_strategy" not in operations
        assert [item for item in operations if item.startswith("event:")] == ["event:ERROR"]
        assert "event:RUN_END" not in operations
        assert "event:ERROR" in operations
        assert "lifecycle:CYCLE_OK" not in operations
        assert operations[-1] == "lifecycle:ERROR"
        assert "read_rollback" in operations and "read_close" in operations
        assert not any(item.startswith(("execution:", "mutation:")) for item in operations)
        if failure == "calculation":
            read_order = [operations.index(item) for item in (
                "read_open", "read_query", "read_rollback", "read_close",
            )]
            assert read_order == sorted(read_order)
            assert operations.index("read_close") < operations.index("lifecycle:ERROR")
            assert "writer_open" not in operations
        else:
            expected = ["writer_open", "writer_execute"]
            if failure == "commit":
                expected.append("writer_commit")
            expected.extend(["writer_rollback", "writer_cursor_close", "writer_close",
                             "event:ERROR", "lifecycle:ERROR"])
            indexes = [operations.index(item) for item in expected]
            assert indexes == sorted(indexes)
            assert "writer_cursor_close" in operations and "writer_close" in operations


@pytest.mark.parametrize(
    "setup, latest, prev, expected",
    [
        (lambda h: setattr(h, "mode", "HALT"), candle(), candle(minute=-1), "BOT_MODE_HALT"),
        (lambda h: setattr(h, "enabled", False), candle(direction=1), candle(minute=-1, direction=-1), "BOT_DISABLED"),
        (lambda h: setattr(h, "regime_allow", False), candle(direction=1), candle(minute=-1, direction=-1), "REGIME_BLOCK"),
        (lambda h: setattr(h.module, "DISABLE_HOURS_SET", {12}), candle(direction=1), candle(minute=-1, direction=-1), "DISABLE_HOURS"),
        (lambda h: None, candle(direction=1, atr=0.01), candle(minute=-1, direction=-1), "ATR_TOO_LOW"),
        (lambda h: None, candle(direction=1), candle(minute=-1, direction=1), "NO_SIGNAL"),
    ],
)
def test_non_execution_entry_paths(harness, setup, latest, prev, expected):
    setup(harness)
    observed = harness.strategy_cycle(latest, prev)
    assert expected in reasons(observed)
    assert observed.attempts == ()
    assert observed.operation_log[-1] == "event:RUN_END"


def test_indicators_not_ready_precedes_strategy_heartbeat(harness):
    latest = list(candle())
    latest[6] = None
    observed = harness.strategy_cycle(tuple(latest), candle(minute=-1))
    assert "INDICATORS_NOT_READY" in reasons(observed)
    assert "heartbeat:strategy" not in observed.operation_log


def test_zero_sizing_is_forwarded_to_existing_execution_boundary(harness):
    use_execution(harness, "PAPER_ZERO")
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert len(observed.attempts) == 1
    assert observed.attempts[0].qty == 0.0


def test_live_daily_loss_control_blocks_before_execution(harness, monkeypatch):
    class Conn:
        def close(self):
            pass

    use_execution(harness, "ENTRY_FULL")
    monkeypatch.setattr(harness.module, "DAILY_MAX_LOSS_PCT", 1.0)
    monkeypatch.setattr(
        harness.module, "compute_daily_loss_pct_positions",
        lambda *_a, **_k: {"daily_pct": -2.0},
    )
    monkeypatch.setattr(harness.module, "should_emit_daily_loss_shadow", lambda **_k: False)
    monkeypatch.setattr(harness.module, "should_block_daily_loss_positions", lambda **_k: True)
    monkeypatch.setattr(harness.module, "get_db_conn", lambda: Conn())
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert "DAILY_MAX_LOSS_POSITIONS" in reasons(observed)
    assert observed.attempts == ()


@pytest.mark.parametrize(
    "scenario_name, closes",
    [("PAPER", False), ("LIVE_SUPPRESSION", False),
     ("ENTRY_REJECTION", False), ("ENTRY_FULL", True)],
)
def test_paper_live_entry_boundaries(harness, scenario_name, closes):
    use_execution(harness, scenario_name)
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert len(observed.attempts) == 1
    assert (observed.position is not None) is closes


def test_paper_entry_emits_position_opened_without_position_mutation(harness):
    use_execution(harness, "PAPER")
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert len(observed.attempts) == 1
    assert "LEDGER_OK" in reasons(observed)
    assert "SSOT_EXECUTE_AND_RECORD" in reasons(observed)
    assert observed.position is None
    assert observed.mutations == ()


def run_production_execute_and_record(module, monkeypatch, production_execute, scenario):
    class Conn:
        def commit(self):
            pass

        def close(self):
            pass

    exchange_double = {
        "attempted": True,
        "requested_qty": scenario["requested_qty"],
        "client_order_id": scenario["client_order_id"],
        "ok": True,
        "live_ok": bool(scenario["executed"]),
        "order_accepted": scenario["order_accepted"],
        "executed": scenario["executed"],
        "fully_executed": scenario["fully_executed"],
        "executed_qty": scenario["executed_qty"],
        "order_id": scenario["order_id"],
        "exchange_status": scenario["exchange_status"],
        "resp": dict(scenario["resp"]) if scenario["resp"] is not None else None,
    }
    monkeypatch.setattr(module, "insert_simulated_order",
                        lambda **_k: scenario["ledger_ok"])
    monkeypatch.setattr(module, "emit_strategy_event", lambda **_k: None)
    monkeypatch.setattr(module, "get_open_position", lambda: None)
    monkeypatch.setattr(module, "get_exchange_client", lambda: object())
    monkeypatch.setattr(module, "get_db_conn", Conn)
    monkeypatch.setattr(module, "place_live_order", lambda *_a, **_k: exchange_double)
    monkeypatch.setattr(module, "build_live_entry_intent_client_order_id",
                        lambda *_a, **_k: scenario["client_order_id"])
    monkeypatch.setattr(module, "open_position_from_live_ack", lambda **_k: 77)

    cfg = SimpleNamespace(
        symbol="BTCUSDC", interval="1m", trading_mode=scenario["trading_mode"],
        live_orders_enabled=True, quote_asset="USDC",
    )
    return production_execute(
        side="BUY", price=100.0, qty_btc=scenario["requested_qty"], reason="fixture",
        candle_open_time=candle()[0], is_exit=False, cfg_used=cfg,
        allow_live_orders=scenario["allow_execution"], allow_meta={},
    )


ENTRY_PARITY_SCENARIOS = [
    "PAPER", "DB_GUARD", "LIVE_SUPPRESSION", "ENTRY_REJECTION",
    "ENTRY_ACK_ONLY", "ENTRY_PARTIAL", "ENTRY_FULL",
]


@pytest.mark.parametrize("scenario_name", ENTRY_PARITY_SCENARIOS)
def test_fixture_raw_execution_result_matches_production_return_shape(
    harness, monkeypatch, scenario_name,
):
    scenario = RAW_EXECUTION_SCENARIOS[scenario_name]
    harness.apply_execution_scenario(scenario)

    harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    fixture_result = harness.last_execution_result
    production = run_production_execute_and_record(
        harness.module, monkeypatch, harness.production_execute_and_record, scenario,
    )
    assert fixture_result == production
    if scenario["resp"] is not None:
        assert fixture_result["resp"]["scenario_marker"] == scenario["resp"]["scenario_marker"]
        assert production["resp"]["scenario_marker"] == scenario["resp"]["scenario_marker"]


@pytest.mark.parametrize("field", ["client_order_id", "order_id", "status", "resp"])
def test_fixture_execution_metadata_is_scenario_sensitive(harness, field):
    scenario = dict(RAW_EXECUTION_SCENARIOS["ENTRY_PARTIAL"])
    scenario["resp"] = dict(scenario["resp"])
    if field == "client_order_id":
        scenario[field] = "cid-mutated-901"
        scenario["resp"]["clientOrderId"] = scenario[field]
    elif field == "order_id":
        scenario[field] = "oid-mutated-902"
        scenario["resp"]["orderId"] = scenario[field]
    elif field == "status":
        scenario["status"] = "TEST_PARTIAL_STATUS"
        scenario["exchange_status"] = scenario["status"]
        scenario["resp"]["status"] = scenario["status"]
    else:
        scenario["resp"]["scenario_marker"] = "mutated-marker-903"
    harness.apply_execution_scenario(scenario)
    harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    if field == "status":
        assert harness.last_execution_result["exchange_status"] == scenario["status"]
        assert harness.last_execution_result["resp"]["status"] == scenario["status"]
    else:
        assert harness.last_execution_result[field] == scenario[field]


def test_apply_execution_scenario_deep_snapshot_isolates_mutable_source(harness):
    source = dict(RAW_EXECUTION_SCENARIOS["ENTRY_PARTIAL"])
    source["resp"] = dict(source["resp"])
    expected = dict(source)
    expected["resp"] = dict(source["resp"])
    harness.apply_execution_scenario(source)

    source["client_order_id"] = "cid-changed-after-install"
    source["order_id"] = "oid-changed-after-install"
    source["status"] = "CHANGED"
    source["exchange_status"] = "CHANGED"
    source["requested_qty"] = 9.9
    source["executed_qty"] = 8.8
    source["blocked_reason"] = "CHANGED"
    source["resp"]["status"] = "CHANGED"
    source["resp"]["scenario_marker"] = "changed-after-install"

    active = harness.execution_scenario
    for field in (
        "client_order_id", "order_id", "status", "exchange_status",
        "requested_qty", "executed_qty", "blocked_reason",
    ):
        assert active[field] == expected[field]
    assert active["resp"]["scenario_marker"] == expected["resp"]["scenario_marker"]
    with pytest.raises(TypeError):
        active["order_id"] = "cannot-mutate"
    with pytest.raises(TypeError):
        active["resp"]["status"] = "cannot-mutate"

    harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    result = harness.last_execution_result
    assert result["client_order_id"] == expected["client_order_id"]
    assert result["order_id"] == expected["order_id"]
    assert result["requested_qty"] == expected["requested_qty"]
    assert result["executed_qty"] == expected["executed_qty"]
    assert result["resp"] == expected["resp"]


def test_apply_execution_scenario_rejects_incomplete_contract(harness):
    incomplete = dict(RAW_EXECUTION_SCENARIOS["ENTRY_FULL"])
    incomplete.pop("resp")
    with pytest.raises(ValueError, match="missing=\\['resp'\\]"):
        harness.apply_execution_scenario(incomplete)


def test_harness_has_no_legacy_execution_flags(harness):
    forbidden = {
        "execution_ledger_ok", "execution_order_accepted", "execution_executed",
        "execution_executed_qty", "execution_fully_executed",
        "execution_live_ok", "execution_requested_qty",
    }
    assert forbidden.isdisjoint(vars(harness))


@pytest.mark.parametrize(
    "scenario_name",
    ["PAPER", "DB_GUARD", "LIVE_SUPPRESSION",
     "ENTRY_REJECTION", "ENTRY_ACK_ONLY", "ENTRY_PARTIAL", "ENTRY_FULL",
     "EXIT_REJECTION", "EXIT_ACK_ONLY", "EXIT_PARTIAL", "EXIT_FULL",
     "PANIC_DB_GUARD", "PANIC_REJECTION", "PANIC_ACK_ONLY",
     "PANIC_PARTIAL", "PANIC_FULL"],
)
def test_active_scenario_cannot_form_hybrid_outcome(harness, scenario_name):
    scenario = RAW_EXECUTION_SCENARIOS[scenario_name]
    use_execution(harness, scenario_name)
    if scenario_name.startswith("PANIC_"):
        harness.mode = "PANIC"
        harness.set_position()
        harness.strategy_cycle(candle(), candle(minute=-1))
    elif scenario_name.startswith("EXIT_"):
        harness.set_position()
        harness.strategy_cycle(candle(price=102.0), candle(minute=-1))
    else:
        harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    result = harness.last_execution_result
    assert result["blocked_reason"] == scenario["blocked_reason"]
    assert result["client_order_id"] == scenario["client_order_id"]
    if scenario["resp"] is None:
        assert result["resp"] is None
        assert "order_id" not in result and "executed_qty" not in result
    else:
        assert result["resp"] == dict(scenario["resp"])
        assert result["resp"]["status"] == scenario["status"]
        assert result["resp"]["scenario_marker"] == scenario["resp"]["scenario_marker"]
    if scenario_name.startswith("ENTRY_"):
        assert result["order_id"] == scenario["order_id"]
        assert result["executed_qty"] == scenario["executed_qty"]


@pytest.mark.parametrize("scenario_name", ["PAPER", "DB_GUARD", "LIVE_SUPPRESSION"])
def test_non_live_result_preserves_missing_vs_none(harness, scenario_name):
    harness.apply_execution_scenario(RAW_EXECUTION_SCENARIOS[scenario_name])
    harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    result = harness.last_execution_result
    assert result["client_order_id"] is None and result["resp"] is None
    assert "order_id" not in result and "executed_qty" not in result


@pytest.mark.parametrize(
    "scenario_name, expected_qty, event_reason",
    [
        ("ENTRY_REJECTION", None, "ORDER_REJECTED"),
        ("ENTRY_ACK_ONLY", None, "ORDER_ACCEPTED_PENDING_FILL"),
        ("ENTRY_PARTIAL", 0.06, "OK"),
        ("ENTRY_FULL", 0.13, "OK"),
    ],
)
def test_live_entry_ack_fill_matrix_uses_confirmed_quantity(
    harness, scenario_name, expected_qty, event_reason,
):
    use_execution(harness, scenario_name)
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    sent = next(event for event in observed.events if event.get("event_type") == "LIVE_ORDER_SENT")
    assert sent["reason"] == event_reason
    assert len(observed.attempts) == 1
    assert (observed.position[2] if observed.position else None) == expected_qty
    if expected_qty is not None and not RAW_EXECUTION_SCENARIOS[scenario_name]["fully_executed"]:
        assert expected_qty != observed.attempts[0].qty


def test_entry_db_guard_failure_does_not_open_position(harness):
    use_execution(harness, "DB_GUARD")
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert "DB_GUARD_DUPLICATE" in reasons(observed)
    assert observed.position is None
    assert len(observed.attempts) == 1
    assert observed.mutations == ()
    assert harness.last_execution_result == {
        "ledger_ok": False, "live_attempted": False, "live_ok": False,
        "blocked_reason": "DB_GUARD_DUPLICATE", "client_order_id": None, "resp": None,
    }


@pytest.mark.parametrize(
    "price, age, flip, expected_reason",
    [(102.0, 5, False, "TAKE_PROFIT_LONG"),
     (98.0, 5, False, "STOP_LOSS_LONG"),
     (100.0, 100, False, "TIME_EXIT_LONG"),
     (100.0, 5, True, "FLIP_DOWN_EXIT")],
)
def test_exit_matrix_and_chronology(harness, price, age, flip, expected_reason):
    use_execution(harness, "PAPER")
    harness.set_position(price=100.0, age=age)
    harness.time_exit_enabled = age >= 90
    latest = candle(price=price, direction=-1 if flip else 1)
    prev = candle(minute=-1, price=100.0, direction=1)
    observed = harness.strategy_cycle(latest, prev)
    assert observed.position is None
    assert observed.mutations[-1][3] == expected_reason
    log = observed.operation_log
    assert log.index("execution:exit") < log.index("mutation:close")
    assert log.index("mutation:close") < log.index("event:RUN_END")
    assert log[-1] == "event:RUN_END"


def test_exit_db_guard_does_not_mutate(harness):
    use_execution(harness, "DB_GUARD")
    harness.set_position(price=100.0)
    observed = harness.strategy_cycle(candle(price=102.0), candle(minute=-1))
    assert "DB_GUARD_DUPLICATE" in reasons(observed)
    assert "EXIT_BLOCKED" in reasons(observed)
    assert observed.position is not None
    assert observed.mutations == ()


def test_live_exit_suppression_keeps_position(harness):
    use_execution(harness, "LIVE_SUPPRESSION")
    harness.set_position(price=100.0)
    observed = harness.strategy_cycle(candle(price=102.0), candle(minute=-1))
    assert len(observed.attempts) == 1
    assert observed.position is not None
    assert observed.mutations == ()
    assert "EXIT_BLOCKED" in reasons(observed)
    assert harness.last_execution_result["blocked_reason"] == "LIVE_ORDER_SUPPRESSED"
    assert harness.last_execution_result["resp"] is None


@pytest.mark.parametrize(
    "scenario_name, closes, event_reason",
    [
        ("EXIT_REJECTION", False, "ACK_NO_FILL"),
        ("EXIT_ACK_ONLY", False, "ACK_NO_FILL"),
        ("EXIT_PARTIAL", True, "OK"),
        ("EXIT_FULL", True, "OK"),
    ],
)
def test_live_exit_legacy_ack_fill_matrix(
    harness, scenario_name, closes, event_reason,
):
    use_execution(harness, scenario_name)
    harness.set_position(price=100.0)
    observed = harness.strategy_cycle(candle(price=102.0), candle(minute=-1))
    sent = next(event for event in observed.events if event.get("event_type") == "LIVE_ORDER_SENT")
    assert sent["reason"] == event_reason
    assert (observed.position is None) is closes
    assert len(observed.attempts) == 1


def test_panic_with_position_closes_and_preserves_historical_early_return(harness):
    use_execution(harness, "PAPER")
    harness.mode = "PANIC"
    harness.set_position()
    observed = harness.strategy_cycle(candle(), candle(minute=-1))
    assert observed.position is None
    assert harness.mode == "PANIC"
    assert "mutation:mode" not in observed.operation_log


def test_panic_without_position_transitions_to_halt(harness):
    harness.mode = "PANIC"
    observed = harness.strategy_cycle(candle(), candle(minute=-1))
    assert harness.mode == "HALT"
    assert observed.operation_log.index("mutation:mode") < observed.operation_log.index("event:RUN_END")


@pytest.mark.parametrize("accepted", [False, True])
def test_panic_live_rejection_and_ack_only_preserve_position_and_mode(
    harness, accepted,
):
    harness.mode = "PANIC"
    use_execution(harness, "PANIC_ACK_ONLY" if accepted else "PANIC_REJECTION")
    harness.set_position()
    observed = harness.strategy_cycle(candle(), candle(minute=-1))
    assert observed.position is not None
    assert harness.mode == "PANIC"
    assert "ACK_NO_FILL" in reasons(observed)
    assert "EXIT_BLOCKED" in reasons(observed)
    assert observed.operation_log[-1] == "event:RUN_END"


def test_panic_db_guard_preserves_position_and_panic_mode(harness):
    harness.mode = "PANIC"
    use_execution(harness, "PANIC_DB_GUARD")
    harness.set_position()
    observed = harness.strategy_cycle(candle(), candle(minute=-1))
    assert len(observed.attempts) == 1
    assert "DB_GUARD_DUPLICATE" in reasons(observed)
    assert "EXIT_BLOCKED" in reasons(observed)
    assert observed.position is not None
    assert harness.mode == "PANIC"
    assert observed.mutations == ()
    assert observed.operation_log[-1] == "event:RUN_END"
    assert harness.last_execution_result["blocked_reason"] == "DB_GUARD_DUPLICATE"
    assert harness.last_execution_result["resp"] is None


@pytest.mark.parametrize("scenario_name", ["PANIC_PARTIAL", "PANIC_FULL"])
def test_panic_live_partial_and_full_close_preserve_panic_mode(
    harness, scenario_name,
):
    harness.mode = "PANIC"
    use_execution(harness, scenario_name)
    harness.set_position()
    observed = harness.strategy_cycle(candle(), candle(minute=-1))
    assert len(observed.attempts) == 1
    assert observed.attempts[0].side == "SELL"
    assert observed.position is None
    assert harness.mode == "PANIC"
    assert "mutation:mode" not in observed.operation_log
    assert "OK" in reasons(observed)
    assert observed.operation_log[-1] == "event:RUN_END"


def test_historical_short_is_rejected_without_exit_attempt(harness):
    harness.position = (1, "SHORT", 0.1, 100.0, harness.now)
    observed = harness.strategy_cycle(candle(price=99.0), candle(minute=-1))
    assert "UNSUPPORTED_POSITION_SIDE" in reasons(observed)
    assert observed.attempts == ()
    assert observed.position[1] == "SHORT"


def test_run_end_is_last_strategy_boundary(harness):
    use_execution(harness, "ENTRY_FULL")
    observed = harness.strategy_cycle(candle(direction=1), candle(minute=-1, direction=-1))
    assert observed.operation_log.index("execution:entry") < observed.operation_log.index("mutation:open")
    assert observed.operation_log[-1] == "event:RUN_END"


def test_strict_exchange_boundary_rejects_any_private_access(harness):
    boundary = harness.module.get_exchange_client()
    with pytest.raises(AssertionError, match="unexpected real exchange call"):
        boundary.get_my_trades()

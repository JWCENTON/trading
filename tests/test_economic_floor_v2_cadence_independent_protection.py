from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.exit_guards.economic_floor_v2 import (
    ACTIVE_VERSION_ENV,
    CONTRACT_VERSION,
    MODE_ENV,
    V2_ACTIVE_EXIT_INTENT_EVENT,
    V2_ACTIVE_EXIT_REASON,
    V2_ARM_EVENT,
    V2_FINAL_EVENT,
    V2_OBSERVATION_EVENT,
    V2State,
    classify_canonical_one_minute_mark,
    economic_floor_v2_active,
    evaluate_v2_transition,
)
from common.simulated_execution_evidence import PaperRealizableNetEvidence


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 2, 12, 2, tzinfo=timezone.utc)


def evidence(
    net: str = "0.01", *, source: str = "candles:1:close:source",
    interval: str = "1m", strategy: str = "RSI", symbol: str = "BTCUSDC",
    status: str = "AUTHORITATIVE",
) -> PaperRealizableNetEvidence:
    return PaperRealizableNetEvidence(
        status=status, position_id=77, symbol=symbol, interval=interval,
        strategy=strategy, observed_at=NOW, mark_price=Decimal("100"),
        source_candle_id=source, entry_fill_ids=(10,),
        fee_contract_fingerprint="fee-v2", exit_fee_rate=Decimal("0.0035"),
        quantity=Decimal("1"), hypothetical_exit_notional=Decimal("100"),
        hypothetical_exit_fee=Decimal("0.35"),
        realizable_net_after_all_costs=(
            Decimal(net) if status == "AUTHORITATIVE" else None
        ),
        market_data_complete=status == "AUTHORITATIVE",
    )


def armed(*, source: str = "arm") -> V2State:
    return V2State(
        True, NOW - timedelta(minutes=1), Decimal("0.01"), source,
        Decimal("0.10"),
    )


def test_finalized_fresh_one_minute_source_is_authoritative():
    mark = classify_canonical_one_minute_mark(
        symbol="BTCUSDC", evaluated_at=NOW,
        row=(123, Decimal("99.5"), NOW - timedelta(seconds=60)),
    )
    assert mark.authoritative
    assert mark.source_id == f"candles:123:close:{mark.close_time.isoformat()}"


def test_stale_one_minute_source_fails_closed():
    mark = classify_canonical_one_minute_mark(
        symbol="BTCUSDC", evaluated_at=NOW,
        row=(123, Decimal("99.5"), NOW - timedelta(seconds=121)),
    )
    assert mark.status == "STALE_1M_SOURCE"
    assert not mark.authoritative


def test_unclosed_one_minute_source_has_no_authority():
    mark = classify_canonical_one_minute_mark(
        symbol="BTCUSDC", evaluated_at=NOW,
        row=(123, Decimal("99.5"), NOW + timedelta(milliseconds=1)),
    )
    assert mark.status == "UNCLOSED_1M_SOURCE"
    assert not mark.authoritative


def test_missing_or_invalid_one_minute_source_has_no_authority():
    assert classify_canonical_one_minute_mark(
        symbol="BTCUSDC", evaluated_at=NOW, row=None,
    ).status == "MISSING_FINALIZED_1M"
    assert classify_canonical_one_minute_mark(
        symbol="BTCUSDC", evaluated_at=NOW,
        row=(1, Decimal("0"), NOW - timedelta(seconds=1)),
    ).status == "INVALID_1M_PRICE"


def test_v2_mode_is_paper_only_and_requires_explicit_version():
    values = {ACTIVE_VERSION_ENV: "V2", MODE_ENV: "TREATMENT"}
    assert economic_floor_v2_active("PAPER", values)
    assert not economic_floor_v2_active("LIVE", values)
    assert not economic_floor_v2_active("PAPER", {MODE_ENV: "TREATMENT"})


def test_no_arm_before_authoritative_full_cost_cover():
    decision = evaluate_v2_transition(V2State(False), evidence=evidence("-0.0001"))
    assert decision.status == "NOT_COST_COVERED"
    assert decision.event_type == V2_OBSERVATION_EVENT
    assert not decision.state.armed


def test_first_qualifying_source_arms_exactly_once_without_exit():
    decision = evaluate_v2_transition(V2State(False), evidence=evidence("0"))
    assert decision.status == "ARMED_NOW_NO_SAME_SOURCE_EXIT"
    assert decision.event_type == V2_ARM_EVENT
    assert decision.state.armed
    assert not decision.exit_requested


def test_same_source_retry_is_idempotent():
    decision = evaluate_v2_transition(
        armed(), evidence=evidence("-1"), source_already_evaluated=True,
    )
    assert decision.status == "IDEMPOTENT_SOURCE_ALREADY_EVALUATED"
    assert decision.event_type is None
    assert not decision.exit_requested


def test_same_source_as_first_arm_never_exits():
    decision = evaluate_v2_transition(
        armed(source="same"), evidence=evidence("0", source="same"),
    )
    assert decision.status == "FIRST_ARM_SOURCE_NO_EXIT"
    assert not decision.exit_requested


def test_later_positive_net_preserves_upside():
    decision = evaluate_v2_transition(armed(), evidence=evidence("0.000001"))
    assert decision.status == "ARMED_UPSIDE_OPEN"
    assert decision.event_type == V2_OBSERVATION_EVENT
    assert not decision.exit_requested


@pytest.mark.parametrize("net", ["0", "-0.01"])
def test_later_zero_or_negative_net_claims_v2_exit(net):
    decision = evaluate_v2_transition(armed(), evidence=evidence(net))
    assert decision.status == "V2_EXIT_CLAIMED"
    assert decision.event_type == V2_ACTIVE_EXIT_INTENT_EVENT
    assert decision.exit_reason == V2_ACTIVE_EXIT_REASON
    assert decision.exit_requested


def test_existing_committed_strategy_exit_wins():
    decision = evaluate_v2_transition(
        armed(), evidence=evidence("-1"), existing_exit_committed=True,
    )
    assert decision.status == "EXISTING_EXIT_PRECEDENCE"
    assert not decision.exit_requested


def test_duplicate_exit_intent_is_impossible():
    decision = evaluate_v2_transition(
        armed(), evidence=evidence("-1"), intent_exists=True,
    )
    assert decision.status == "IDEMPOTENT_EXIT_INTENT_EXISTS"
    assert not decision.exit_requested


@pytest.mark.parametrize(
    "status",
    ["INCOMPLETE:INVENTORY", "INCOMPLETE:COST_AUTHORITY", "INCOMPLETE:EXECUTION_SCOPE"],
)
def test_missing_authoritative_evidence_fails_closed(status):
    decision = evaluate_v2_transition(
        armed(), evidence=evidence("-1", status=status),
    )
    assert decision.status == "FAIL_CLOSED_MISSING_AUTHORITY"
    assert not decision.exit_requested


@pytest.mark.parametrize("interval", ["1m", "5m"])
def test_both_originating_intervals_use_same_v2_reducer(interval):
    decision = evaluate_v2_transition(
        V2State(False), evidence=evidence("0", interval=interval),
    )
    assert decision.event_type == V2_ARM_EVENT


@pytest.mark.parametrize("strategy", ["RSI", "BBRANGE", "TREND", "SUPERTREND"])
def test_all_four_strategies_are_generic_inputs(strategy):
    decision = evaluate_v2_transition(
        V2State(False), evidence=evidence("0", strategy=strategy),
    )
    assert decision.event_type == V2_ARM_EVENT


@pytest.mark.parametrize("symbol", ["BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"])
def test_all_current_symbols_are_generic_inputs(symbol):
    decision = evaluate_v2_transition(
        V2State(False), evidence=evidence("0", symbol=symbol),
    )
    assert decision.event_type == V2_ARM_EVENT


def test_v2_namespace_is_distinct_from_v1():
    v1 = (ROOT / "common/exit_guards/economic_floor_shadow.py").read_text()
    for event in (
        V2_ARM_EVENT, V2_OBSERVATION_EVENT,
        V2_ACTIVE_EXIT_INTENT_EVENT, V2_FINAL_EVENT,
    ):
        assert event.startswith("ECONOMIC_FLOOR_V2_")
        assert event not in v1
    assert CONTRACT_VERSION == V2_ACTIVE_EXIT_REASON


def test_v1_authority_is_disabled_when_v2_is_active():
    from common.exit_guards.economic_floor_shadow import economic_floor_mode

    values = {
        ACTIVE_VERSION_ENV: "V2",
        MODE_ENV: "TREATMENT",
        "ECONOMIC_FLOOR_AFTER_COST_COVER_V1_MODE": "TREATMENT",
    }
    assert economic_floor_mode("PAPER", values) == "SHADOW_ONLY"
    assert economic_floor_v2_active("PAPER", values)


def test_owner_cycle_uses_finalized_1m_and_existing_locks():
    source = (ROOT / "common/exit_guards/economic_floor_v2.py").read_text()
    assert "interval='1m' AND close_time<=%s" in source
    assert "lock_simulated_exit_slot_cursor" in source
    assert "pg_advisory_xact_lock" in source
    assert "load_paper_realizable_net_evidence" in source


def test_owner_worker_wiring_is_after_normal_strategy_cycle_and_on_idle_cycles():
    files = (
        "bot/main.py", "bot_bbrange/main.py", "bot_trend/main.py",
        "bot_supertrend/main.py",
    )
    for relative in files:
        source = (ROOT / relative).read_text()
        assert "def run_economic_floor_v2_owner_cycle" in source
        assert source.count("run_economic_floor_v2_owner_cycle()") >= 2
        assert "V2_ACTIVE_EXIT_REASON" in source
        assert "allow_live_orders=False" in source


def test_non_originating_cycle_does_not_invoke_strategy_logic_early():
    for relative in ("bot/main.py", "bot_bbrange/main.py", "bot_trend/main.py"):
        source = (ROOT / relative).read_text()
        no_new = source.index("NO_NEW_CANDLE", source.index("def main_loop"))
        v2_call = source.index("run_economic_floor_v2_owner_cycle()", no_new)
        assert v2_call > no_new
    supertrend = (ROOT / "bot_supertrend/main.py").read_text()
    assert supertrend.index("run_economic_floor_v2_owner_cycle()", supertrend.index("def run_loop_cycle")) > supertrend.index("run_loop_iteration(", supertrend.index("def run_loop_cycle"))


def test_v2_final_event_links_only_complete_financial_truth():
    source = (ROOT / "common/exit_guards/economic_floor_v2.py").read_text()
    assert "financial_truth_status='COMPLETE'" in source
    assert "authoritative_net_pnl" in source
    assert "final_net_pnl_after_fees" in source


def test_paper_override_activates_only_v2_and_live_files_remain_untouched():
    paper = (ROOT / "docker-compose.paper.override.yaml").read_text()
    assert 'ECONOMIC_FLOOR_MODE: "V2_TREATMENT"' in paper
    assert 'ACTIVE_ECONOMIC_FLOOR_VERSION: "V2"' in paper
    assert 'ECONOMIC_FLOOR_AFTER_COST_COVER_V1_MODE: "SHADOW_ONLY"' in paper
    assert 'ECONOMIC_FLOOR_V2_MODE: "TREATMENT"' in paper
    for path in ROOT.glob("docker-compose*live*.yaml"):
        live = path.read_text()
        assert "ECONOMIC_FLOOR_V2_MODE" not in live
        assert "ACTIVE_ECONOMIC_FLOOR_VERSION" not in live

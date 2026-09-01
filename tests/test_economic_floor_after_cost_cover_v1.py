from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from common.exit_guards.economic_floor_shadow import (
    ACTIVE_EXIT_REASON,
    ARM_EVENT,
    FINAL_EVENT,
    ShadowState,
    economic_floor_mode,
    evaluate_active_exit_transition,
    evaluate_shadow_transition,
    observe_economic_floor_shadow,
)
from common.simulated_execution_evidence import PaperRealizableNetEvidence


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)


def evidence(net="0.01", *, status="AUTHORITATIVE", candle="c1"):
    return PaperRealizableNetEvidence(
        status=status, position_id=77, symbol="BTCUSDC", interval="1m",
        strategy="RSI", observed_at=NOW, mark_price=Decimal("100"),
        source_candle_id=candle, entry_fill_ids=(10,),
        fee_contract_fingerprint="fee-v2", exit_fee_rate=Decimal("0.0035"),
        quantity=Decimal("1"), hypothetical_exit_notional=Decimal("100"),
        hypothetical_exit_fee=Decimal("0.35"),
        realizable_net_after_all_costs=Decimal(net), market_data_complete=True,
    )


def test_no_arm_before_full_economic_cost_cover():
    result = evaluate_shadow_transition(ShadowState(False), evidence=evidence("-0.0001"))
    assert result.status == "NOT_COST_COVERED"
    assert not result.state.armed


def test_arm_exactly_once_at_first_qualifying_evaluation_without_exit():
    result = evaluate_shadow_transition(ShadowState(False), evidence=evidence("0"))
    assert result.event_type == ARM_EVENT
    assert result.decision == "ARMED_NO_EXIT"
    assert result.state.first_armed_at == NOW


def test_armed_state_survives_and_peak_is_monotonic():
    armed = evaluate_shadow_transition(ShadowState(False), evidence=evidence("0.10")).state
    later = evidence("0.08", candle="c2")
    later = PaperRealizableNetEvidence(**{**later.__dict__, "observed_at": NOW + timedelta(minutes=1)})
    result = evaluate_shadow_transition(armed, evidence=later)
    assert result.state.armed
    assert result.state.peak_realizable_net_after_arming == Decimal("0.10")
    higher = evidence("0.20", candle="c3")
    assert evaluate_shadow_transition(result.state, evidence=higher).state.peak_realizable_net_after_arming == Decimal("0.20")


def test_return_to_zero_or_negative_is_sticky_shadow_only():
    armed = evaluate_shadow_transition(ShadowState(False), evidence=evidence("0.10")).state
    returned = evaluate_shadow_transition(armed, evidence=evidence("0", candle="c2"))
    assert returned.state.returned_to_zero_or_negative
    assert returned.decision == "WOULD_EXIT_AT_ECONOMIC_FLOOR"
    recovered = evaluate_shadow_transition(returned.state, evidence=evidence("0.30", candle="c3"))
    assert recovered.state.returned_to_zero_or_negative
    assert recovered.decision == "HOLD_UPSIDE_OPEN"


def test_missing_fee_v2_or_incomplete_inventory_never_arms():
    for status in ("INCOMPLETE:COST_AUTHORITY", "INCOMPLETE:INVENTORY"):
        result = evaluate_shadow_transition(ShadowState(False), evidence=evidence(status=status))
        assert result.status == "NO_ARM_MISSING_AUTHORITY"
        assert not result.state.armed


def test_live_path_is_a_noop(monkeypatch):
    monkeypatch.setattr(
        "common.exit_guards.economic_floor_shadow.load_paper_realizable_net_evidence",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not read LIVE")),
    )
    result = observe_economic_floor_shadow(
        trading_mode="LIVE", position_id=1, symbol="BTCUSDC", interval="1m",
        strategy="RSI", current_price=Decimal("1"), observed_at=NOW,
        source_candle_id="live-candle",
    )
    assert result.status == "NOT_APPLICABLE_NON_PAPER"


def test_idempotency_and_complete_ft_linkage_contract_are_persisted():
    source = (ROOT / "common/exit_guards/economic_floor_shadow.py").read_text()
    assert "pg_advisory_xact_lock" in source
    assert "IDEMPOTENT_RETRY" in source
    assert "financial_truth_status='COMPLETE'" in source
    assert FINAL_EVENT in source
    assert "authoritative_net_pnl" in source


def test_all_four_paper_strategies_have_shadow_hook_and_ignore_its_result():
    files = ["bot/main.py", "bot_bbrange/main.py", "bot_trend/main.py", "bot_supertrend/main.py"]
    for relative in files:
        source = (ROOT / relative).read_text()
        assert "observe_economic_floor_shadow(" in source
        assert "claim_active_economic_floor_exit(" in source
        assert "reason=ACTIVE_EXIT_REASON" in source or "reason_text=ACTIVE_EXIT_REASON" in source
        assert "reconcile_economic_floor_shadow_closures(" in source
        assert "trading_mode=cfg_effective.trading_mode" in source


def test_reader_requires_open_complete_inventory_fee_v2_and_no_prior_exit():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    for contract in (
        'str(position[0]).upper() != "OPEN"',
        'str(inventory_status).upper() != "COMPLETE"',
        "str(fee_model) != FEE_MODEL_V2",
        "prior_exits != 0",
        "hypothetical_exit_fee=exit_fee",
    ):
        assert contract in source


def test_common_gate_never_executes_and_shadow_mode_remains_off():
    source = (ROOT / "common/exit_guards/economic_floor_shadow.py").read_text()
    assert '"ON_LOCAL_PAPER_ONLY" if mode == "TREATMENT" else "OFF"' in source
    assert '"ARMED_NO_EXIT"' in source
    assert "execute_and_record" not in source
    assert "close_position" not in source


def test_treatment_mode_is_paper_only_and_live_is_unchanged():
    values = {"ECONOMIC_FLOOR_AFTER_COST_COVER_V1_MODE": "TREATMENT"}
    assert economic_floor_mode("PAPER", values) == "TREATMENT"
    assert economic_floor_mode("LIVE", values) == "SHADOW_ONLY"


def active(net, *, source="later", state=None, mode="TREATMENT",
           committed=False, intent=False, status="AUTHORITATIVE"):
    state = state or ShadowState(
        True, NOW, Decimal("0.01"), Decimal("0.20"), False,
    )
    return evaluate_active_exit_transition(
        mode=mode, trading_mode="PAPER", state=state,
        evidence=evidence(net, status=status, candle=source),
        first_arm_source_candle_id="arm",
        existing_exit_committed=committed,
        intent_exists=intent,
    )


def test_active_first_arm_evaluation_never_exits():
    result = active("0", source="arm")
    assert not result.exit_requested
    assert result.status == "FIRST_ARM_EVALUATION_NO_EXIT"


def test_active_armed_positive_keeps_upside_open():
    result = active("0.00000001")
    assert not result.exit_requested
    assert result.status == "ARMED_UPSIDE_OPEN"


def test_active_armed_zero_or_negative_claims_exact_frozen_exit():
    for net in ("0", "-0.01"):
        result = active(net)
        assert result.exit_requested
        assert result.reason == ACTIVE_EXIT_REASON
        assert result.realizable_net_at_floor_exit == Decimal(net)


def test_active_duplicate_intent_and_existing_exit_preserve_idempotency():
    assert active("-0.01", intent=True).status == "IDEMPOTENT_ACTIVE_INTENT_EXISTS"
    assert active("-0.01", committed=True).status == "EXISTING_EXIT_PRECEDENCE"
    assert not active("-0.01", intent=True).exit_requested
    assert not active("-0.01", committed=True).exit_requested


def test_active_missing_fee_or_inventory_fails_closed():
    for status in ("INCOMPLETE:COST_AUTHORITY", "INCOMPLETE:INVENTORY"):
        result = active("-0.01", status=status)
        assert not result.exit_requested
        assert result.status == "FAIL_CLOSED_MISSING_AUTHORITY"


def test_paper_override_activates_treatment_without_live_config_change():
    paper = (ROOT / "docker-compose.paper.override.yaml").read_text()
    assert 'ECONOMIC_FLOOR_AFTER_COST_COVER_V1_MODE: "TREATMENT"' in paper
    live_files = list(ROOT.glob("docker-compose*live*.yaml"))
    assert all("ECONOMIC_FLOOR_AFTER_COST_COVER_V1_MODE" not in path.read_text() for path in live_files)


def test_final_linkage_carries_active_floor_evidence_to_complete_ft():
    source = (ROOT / "common/exit_guards/economic_floor_shadow.py").read_text()
    for field in (
        "realizable_net_at_floor_exit", "economic_floor_exit_at",
        "existing_exit_decision_at_same_evaluation",
        "final_financial_truth_status", "final_net_pnl_after_fees",
    ):
        assert field in source


def test_existing_exit_precedence_is_before_active_floor_in_all_four_bots():
    for relative in (
        "bot/main.py", "bot_bbrange/main.py",
        "bot_trend/main.py", "bot_supertrend/main.py",
    ):
        source = (ROOT / relative).read_text()
        active = source.index("economic_floor = claim_active_economic_floor_exit(")
        assert source.rfind("DecisionReason.STOP_LOSS", 0, active) >= 0
        assert source.find("PROFIT LOCK", active) > active

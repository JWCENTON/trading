from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from common.exit_guards.economic_floor_boundary_evidence import (
    CONTRACT_VERSION,
    EVIDENCE_EVENT,
    FINAL_EVENT,
    build_evidence_payload,
    persist_boundary_evidence_cursor,
)
from common.exit_guards.economic_floor_v2 import CanonicalOneMinuteMark
from common.simulated_execution_evidence import PaperRealizableNetEvidence


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)


def test_forward_payload_contains_complete_causal_evidence_without_authority():
    evidence = PaperRealizableNetEvidence(
        status="AUTHORITATIVE", position_id=77, symbol="BTCUSDC",
        interval="5m", strategy="TREND", observed_at=NOW,
        mark_price=Decimal("101"), source_candle_id="candles:2",
        entry_fill_ids=(10,), fee_contract_fingerprint="fee-v2",
        exit_fee_rate=Decimal("0.0035"), quantity=Decimal("1"),
        hypothetical_exit_notional=Decimal("101"),
        hypothetical_exit_fee=Decimal("0.3535"),
        realizable_net_after_all_costs=Decimal("0.05"),
        market_data_complete=True,
    )
    mark = CanonicalOneMinuteMark(
        "AUTHORITATIVE", "BTCUSDC", NOW, candle_id=2, close_time=NOW,
        price=Decimal("101"), source_id="candles:2", high=Decimal("102"),
        low=Decimal("100"), atr_pct=Decimal("0.25"),
    )
    previous = {
        "source_1m_close_time": (NOW - timedelta(minutes=1)).isoformat(),
        "realizable_net": "0.08", "peak_realizable_net": "0.10",
        "distinct_evaluations_since_arm": 1,
    }
    payload = build_evidence_payload(
        evidence=evidence, mark=mark, armed_at=NOW - timedelta(minutes=2),
        previous=previous, existing_exit_decision="HOLD",
        existing_exit_reason="ARMED_UPSIDE_OPEN",
        existing_exit_committed=False,
        recent_realized_volatility=Decimal("0.12"),
        regime="TREND_UP", regime_at=NOW - timedelta(seconds=1),
    )
    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["active_boundary_influence"] == "OFF"
    assert payload["realizable_net_pct_of_notional"] == Decimal("0.05") / Decimal("101") * 100
    assert payload["previous_realizable_net"] == Decimal("0.08")
    assert payload["realizable_net_change"] == Decimal("-0.03")
    assert payload["realizable_net_slope_per_minute"] == Decimal("-0.03")
    assert payload["peak_realizable_net"] == Decimal("0.10")
    assert payload["distance_from_peak_usdc"] == Decimal("0.05")
    assert payload["one_minute_candle_range"] == Decimal("2")
    assert payload["atr_pct"] == Decimal("0.25")
    assert payload["distinct_evaluations_since_arm"] == 2
    assert not payload["existing_exit_committed"]


def test_boundary_contract_is_observation_only_and_separate_from_v2():
    source = (ROOT / "common/exit_guards/economic_floor_boundary_evidence.py").read_text()
    assert EVIDENCE_EVENT == "ECONOMIC_FLOOR_BOUNDARY_V1_EVIDENCE"
    assert FINAL_EVENT == "ECONOMIC_FLOOR_BOUNDARY_V1_FINAL"
    assert "active_boundary_influence\": \"OFF" in source
    assert "'OBSERVE','FORWARD_EVIDENCE_ONLY'" in source
    assert "exit_requested" not in source
    assert "positive_buffer" not in source
    assert "ratchet" not in source.lower()


def test_live_cannot_produce_boundary_evidence_through_v2_owner():
    from common.exit_guards.economic_floor_v2 import economic_floor_v2_active

    values = {
        "ACTIVE_ECONOMIC_FLOOR_VERSION": "V2",
        "ECONOMIC_FLOOR_V2_MODE": "TREATMENT",
    }
    assert economic_floor_v2_active("PAPER", values)
    assert not economic_floor_v2_active("LIVE", values)


def test_predeployment_arm_is_excluded_from_forward_cohort():
    class Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return None

    result = persist_boundary_evidence_cursor(
        Cursor(), evidence=SimpleNamespace(position_id=77),
        mark=SimpleNamespace(source_id="candles:1"), armed_at=NOW,
        is_forward_arm=False, existing_exit_decision="HOLD",
        existing_exit_reason="ARMED_UPSIDE_OPEN",
        existing_exit_committed=False,
    )
    assert result == "PREEXISTING_ARM_EXCLUDED"

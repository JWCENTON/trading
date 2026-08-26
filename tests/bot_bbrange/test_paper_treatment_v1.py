from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

from common.bbrange_paper_treatment import EntryEvidence
from common.decision_contract import DecisionReason
from tests.bot_bbrange.fixtures import candle


def entry_evidence(module, *, driver=None, hint=None, missing=False):
    return EntryEvidence(
        observed_at=candle()[0], realtime_status="AVAILABLE",
        primary_driver=driver, realtime_score=Decimal("30"),
        mme_status=("MISSING_AT_ENTRY:NO_ACTIVE_MME_SEQUENCE" if missing else "AVAILABLE"),
        orc_hint=hint, mme_refreshed_at=None,
    )


def enable(module, monkeypatch):
    monkeypatch.setattr(
        module, "BBRANGE_TREATMENT_CONFIG",
        replace(module.BBRANGE_TREATMENT_CONFIG, requested=True, effective=True,
                runtime_status="ENABLED_PAPER"),
    )


def test_real_final_decision_path_blocks_1m_volume(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    enable(h.module, monkeypatch)
    monkeypatch.setattr(
        h.module, "load_entry_treatment_evidence",
        lambda *_args, **_kwargs: entry_evidence(h.module, driver="VOLUME"),
    )
    result = h.cycle(candle(close=90.0, low=89.0))
    assert result.returned_value.reason_code is DecisionReason.POLICY_BLOCK
    assert result.returned_value.details["treatment_reason"] == "VOLUME_PRIMARY_DRIVER"
    assert not [x for x in result.operations if x.kind == "execution"]


def test_real_final_decision_path_preserves_1m_non_candidate(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    enable(h.module, monkeypatch)
    monkeypatch.setattr(
        h.module, "load_entry_treatment_evidence",
        lambda *_args, **_kwargs: entry_evidence(h.module, driver="MOMENTUM"),
    )
    result = h.cycle(candle(close=90.0, low=89.0))
    assert result.position is not None
    assert len([x for x in result.operations if x.kind == "execution"]) == 1


def test_real_final_decision_path_blocks_5m_exact_hint(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    monkeypatch.setattr(h.module, "INTERVAL", "5m")
    enable(h.module, monkeypatch)
    monkeypatch.setattr(
        h.module, "load_entry_treatment_evidence",
        lambda *_args, **_kwargs: entry_evidence(
            h.module, driver="MOMENTUM", hint="ORC_AVOID_LATE_ENTRY"),
    )
    result = h.cycle(candle(close=90.0, low=89.0))
    assert result.returned_value.reason_code is DecisionReason.POLICY_BLOCK
    assert result.returned_value.details["interval"] == "5m"


def test_real_final_decision_path_keeps_missing_mme_neutral(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    monkeypatch.setattr(h.module, "INTERVAL", "5m")
    enable(h.module, monkeypatch)
    monkeypatch.setattr(
        h.module, "load_entry_treatment_evidence",
        lambda *_args, **_kwargs: entry_evidence(
            h.module, driver="MOMENTUM", missing=True),
    )
    result = h.cycle(candle(close=90.0, low=89.0))
    assert result.position is not None


def test_take_profit_precedes_treatment_profit_lock_state(stateful_bbrange, monkeypatch):
    h = stateful_bbrange
    enable(h.module, monkeypatch)
    h.set_position(price=100.0)
    monkeypatch.setattr(
        h.module, "load_profit_lock_economic_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("TAKE_PROFIT must not enter treatment Profit Lock path")
        ),
    )
    result = h.cycle(candle(close=101.0, high=102.0, low=100.0))
    exits = [x for x in result.operations if x.kind == "execution"]
    assert len(exits) == 1 and "TAKE PROFIT" in exits[0].payload["reason"]


def test_profit_lock_economic_state_is_observational_and_base_hold_survives(
    stateful_bbrange, monkeypatch,
):
    h = stateful_bbrange
    enable(h.module, monkeypatch)
    h.set_position(price=100.0)
    monkeypatch.setattr(
        h.module, "load_profit_lock_economic_state",
        lambda *_args, **_kwargs: SimpleNamespace(
            status="CANONICAL",
            event_fields=lambda: {
                "economic_edge_observed": True,
                "peak_realizable_net": "0.05",
                "current_realizable_net": "0.02",
                "treatment_behavior": "EVIDENCE_ONLY_NO_EXECUTION_CHANGE",
            },
        ),
    )
    result = h.cycle(candle(close=100.2, high=100.2, low=100.0))
    economic = [
        x for x in result.operations
        if x.kind == "profit_lock_event"
        and x.payload["event_type"] == "PROFIT_LOCK_ECONOMIC_STATE"
    ]
    assert len(economic) == 1
    assert not [x for x in result.operations if x.kind == "execution"]
    assert result.position is not None

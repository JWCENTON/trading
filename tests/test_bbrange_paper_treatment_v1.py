from datetime import datetime, timezone
from decimal import Decimal

from common.bbrange_paper_treatment import (
    CONTRACT_VERSION,
    EntryEvidence,
    TreatmentConfig,
    evaluate_entry_treatment,
    load_profit_lock_economic_state,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def evidence(*, driver=None, hint=None, mme_status="AVAILABLE"):
    return EntryEvidence(
        observed_at=NOW, realtime_status="AVAILABLE",
        primary_driver=driver, realtime_score=Decimal("34.25"),
        mme_status=mme_status, orc_hint=hint,
        mme_refreshed_at=NOW if mme_status == "AVAILABLE" else None,
    )


def config(mode="PAPER", enabled="1"):
    return TreatmentConfig.from_env({
        "TRADING_MODE": mode,
        "BBRANGE_PAPER_TREATMENT_V1_ENABLED": enabled,
        "BBRANGE_PAPER_TREATMENT_V1_STARTED_AT": NOW.isoformat(),
    })


def test_treatment_off_preserves_base_decision():
    result = evaluate_entry_treatment(
        config=config(enabled="0"), strategy="BBRANGE", interval="1m",
        evidence=evidence(driver="VOLUME"),
    )
    assert not result.applies and not result.blocked
    assert result.treatment_decision == "BASE_DECISION"


def test_1m_volume_primary_driver_blocks_with_provenance():
    result = evaluate_entry_treatment(
        config=config(), strategy="BBRANGE", interval="1m",
        evidence=evidence(driver="VOLUME"),
    )
    assert result.applies and result.blocked
    assert result.reason == "VOLUME_PRIMARY_DRIVER"
    assert result.details()["contract_version"] == CONTRACT_VERSION
    assert result.details()["base_decision"] == "BUY"


def test_1m_non_candidate_preserves_base_decision():
    result = evaluate_entry_treatment(
        config=config(), strategy="BBRANGE", interval="1m",
        evidence=evidence(driver="MOMENTUM"),
    )
    assert result.applies and not result.blocked


def test_5m_exact_orc_avoid_late_entry_blocks():
    result = evaluate_entry_treatment(
        config=config(), strategy="BBRANGE", interval="5m",
        evidence=evidence(hint="ORC_AVOID_LATE_ENTRY"),
    )
    assert result.blocked and result.reason == "ORC_AVOID_LATE_ENTRY"


def test_5m_missing_mme_is_neutral():
    result = evaluate_entry_treatment(
        config=config(), strategy="BBRANGE", interval="5m",
        evidence=evidence(mme_status="MISSING_AT_ENTRY:NO_ACTIVE_MME_SEQUENCE"),
    )
    assert not result.blocked
    assert result.reason == "MISSING_MME_NEUTRAL"


def test_other_strategies_are_unaffected():
    for strategy in ("RSI", "TREND", "SUPERTREND"):
        result = evaluate_entry_treatment(
            config=config(), strategy=strategy, interval="1m",
            evidence=evidence(driver="VOLUME"),
        )
        assert not result.applies and not result.blocked


def test_live_refuses_activation():
    live = config(mode="LIVE")
    assert live.requested and not live.effective
    assert live.runtime_status == "REFUSED_NON_PAPER"
    result = evaluate_entry_treatment(
        config=live, strategy="BBRANGE", interval="1m",
        evidence=evidence(driver="VOLUME"),
    )
    assert not result.blocked


class Cursor:
    def __init__(self):
        self.rows = iter([
            ("OPEN", "LONG", Decimal("2"), NOW, Decimal("0.0035"),
             "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"),
            (Decimal("2"), Decimal("200"), Decimal("0.7"), 1, 0),
            (0,),
            (Decimal("102"),),
        ])

    def execute(self, _query, _params=None):
        return None

    def fetchone(self):
        return next(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class Connection:
    def __init__(self):
        self.cursor_value = Cursor()
        self.readonly = False
        self.closed = False

    def set_session(self, *, readonly):
        self.readonly = readonly

    def cursor(self):
        return self.cursor_value

    def rollback(self):
        return None

    def close(self):
        self.closed = True


def test_profit_lock_decimal_complete_cost_state_is_read_only():
    conn = Connection()
    result = load_profit_lock_economic_state(
        lambda: conn, position_id=7, symbol="BTCUSDC", interval="1m",
        current_price=Decimal("101"), observed_at=NOW,
    )
    # current = 2*101 - 200 - 0.7 - 2*101*0.0035
    assert result.current_realizable_net == Decimal("0.5930")
    assert result.peak_realizable_net == Decimal("2.5860")
    assert result.economic_edge_observed is True
    assert result.treatment_behavior == "EVIDENCE_ONLY_NO_EXECUTION_CHANGE"
    assert conn.readonly and conn.closed

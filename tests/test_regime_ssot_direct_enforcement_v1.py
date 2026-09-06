from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from common.regime_gate import (
    REGIME_POLICY_FINGERPRINT,
    RegimeSourceRecord,
    canonical_strategy,
    decide_regime_gate,
)
from common.permissions import can_trade, get_panic_state
from common.runtime import RuntimeConfig


NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "db/migrations/20260906_regime_ssot_direct_paper_enforcement_v1.sql").read_text()
STRATEGIES = {
    "RSI": ROOT / "bot/main.py",
    "TREND": ROOT / "bot_trend/main.py",
    "SUPERTREND": ROOT / "bot_supertrend/main.py",
    "BBRANGE": ROOT / "bot_bbrange/main.py",
}


def record(regime="TREND_UP", *, source_age=0, pipeline_age=0):
    return RegimeSourceRecord(
        "BTCUSDC", "1m", NOW - timedelta(seconds=source_age), regime,
        NOW - timedelta(seconds=pipeline_age),
    )


def gate(*, source=record(), policy=(True, "fixture"), mode="ENFORCE",
         configured="ENFORCE", enabled=True):
    with patch("common.regime_gate.get_current_regime_record", return_value=source), patch(
        "common.regime_gate.get_policy", return_value=policy
    ):
        return decide_regime_gate(
            symbol="BTCUSDC", interval="1m", strategy="RSI", decision="ENTRY_CHECK",
            regime_enabled=enabled, regime_mode=mode, configured_regime_mode=configured,
            decision_candle_timestamp=NOW, evaluated_at=NOW,
        )


def test_allow_and_policy_block_are_direct_in_enforce():
    assert gate(policy=(True, None)).why == "POLICY_ALLOW"
    blocked = gate(policy=(False, None))
    assert blocked.allow is False and blocked.why == "POLICY_BLOCK"


@pytest.mark.parametrize(
    "source,policy,why",
    [
        (None, (True, None), "NO_REGIME_STATE"),
        (record(None), (True, None), "UNKNOWN_REGIME"),
        (record("UNKNOWN"), (True, None), "UNKNOWN_REGIME"),
        (record(source_age=421), (True, None), "STALE_REGIME_STATE"),
        (record(), None, "MISSING_POLICY"),
    ],
)
def test_missing_stale_unknown_and_missing_policy_fail_closed(source, policy, why):
    decision = gate(source=source, policy=policy)
    assert decision.allow is False and decision.why == why


def test_dry_run_is_observational_but_unambiguous():
    decision = gate(policy=(False, None), mode="DRY_RUN", configured="DRY_RUN")
    assert decision.allow is True and decision.would_block is True
    assert decision.why == "POLICY_WOULD_BLOCK"


def test_mode_conflict_fails_closed():
    decision = gate(mode="DRY_RUN", configured="ENFORCE")
    assert decision.allow is False and decision.why == "REGIME_CONFIG_CONFLICT"
    assert decision.mode == "ENFORCE"


def test_source_and_policy_errors_fail_closed():
    with patch("common.regime_gate.get_current_regime_record", side_effect=RuntimeError):
        decision = decide_regime_gate(
            symbol="BTCUSDC", interval="1m", strategy="RSI", decision="ENTRY_CHECK",
            regime_enabled=True, regime_mode="ENFORCE", configured_regime_mode="ENFORCE",
            decision_candle_timestamp=NOW, evaluated_at=NOW,
        )
    assert decision.allow is False and decision.why == "REGIME_SOURCE_ERROR"


def test_canonical_supertrend_policy_identity():
    assert canonical_strategy("SUPER_TREND") == "SUPERTREND"
    assert "('SUPERTREND','RANGE_LOWVOL', false" in MIGRATION
    assert "WHERE strategy='SUPER_TREND'" in MIGRATION


def test_policy_coverage_and_fingerprint_are_frozen():
    assert MIGRATION.count("REGIME_POLICY_20260906_V1',now())") == 20
    assert REGIME_POLICY_FINGERPRINT in MIGRATION
    assert "v_rows<>20" in MIGRATION


def test_all_four_entry_paths_supply_causal_source_and_runtime_mode():
    for name, path in STRATEGIES.items():
        source = path.read_text()
        start = source.index("gate_entry = decide_regime_gate(")
        body = source[start:source.index("\n        )", start) + 10]
        assert "configured_regime_mode=cfg_effective.regime_mode" in body, name
        assert "decision_candle_timestamp=open_time" in body, name
        assert "evaluated_at=datetime.now(timezone.utc)" in body, name


def test_watchdog_and_gate_share_source_and_freshness_contract():
    source = (ROOT / "automation_runner/main.py").read_text()
    assert "FROM market_regime" in source
    assert "regime_record_is_fresh(" in source
    assert "REGIME_GATE_CONTRACT_VERSION" in source
    assert "FROM regime_state" not in (ROOT / "common/regime_gate.py").read_text()


def test_direct_enforcement_is_paper_only_and_32_slots():
    assert "current_database() <> 'trading_paper'" in MIGRATION
    assert "regime_mode='ENFORCE'" in MIGRATION
    assert "<> 32" in MIGRATION


def test_gate_event_linkage_fields_are_carried_forward():
    source = (ROOT / "common/regime_gate.py").read_text()
    for field in (
        "regime_gate_event_id", "regime_gate_contract_version",
        "regime_gate_policy_fingerprint", "regime_gate_source_ts",
    ):
        assert field in source


def test_live_entry_authority_is_not_enabled_by_migration():
    assert "live_orders_enabled=true" not in MIGRATION.lower()
    assert "trading_live" not in MIGRATION


def test_panic_lookup_failure_is_fail_closed():
    source = (ROOT / "common/permissions.py").read_text()
    assert 'return True, "panic_state_check_failed_fail_closed"' in source


def test_panic_blocks_paper_entry_but_preserves_exit(monkeypatch):
    cfg = RuntimeConfig("BTCUSDC", "1m", "RSI", "PAPER", False, True,
                        "USDC", True, "ENFORCE")
    monkeypatch.setattr("common.permissions.get_panic_state", lambda: (True, "fixture"))
    assert can_trade(cfg, regime_allows_trade=True, is_exit=False)[0] is False
    assert can_trade(cfg, regime_allows_trade=True, is_exit=True)[0] is True


def test_panic_db_failure_returns_blocking_state(monkeypatch):
    monkeypatch.setattr("common.permissions.get_db_conn", lambda: (_ for _ in ()).throw(RuntimeError()))
    assert get_panic_state() == (True, "panic_state_check_failed_fail_closed")

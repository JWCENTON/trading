from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import pytest

from common.canonical_regime import (
    CANONICAL_REGIME_ATTRIBUTION_VERSION,
    evaluation_regime_fields,
    load_canonical_regime_at_decision,
)
from common.decision_contract import DecisionReason, EvaluationContext, FinalDecision
from common.decision_observation import event_from_final_decision


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "db/migrations/20260812_canonical_regime_attribution_v1.sql").read_text()
LEARNING_MIGRATION = (ROOT / "db/migrations/20260808_learning_canonical_registry_resolution_v1.sql").read_text()
TRANSPORT = (ROOT / "common/decision_observation_transport.py").read_text()
STRATEGY_FILES = {
    "RSI": ROOT / "bot/main.py",
    "TREND": ROOT / "bot_trend/main.py",
    "SUPERTREND": ROOT / "bot_supertrend/main.py",
    "BBRANGE": ROOT / "bot_bbrange/main.py",
}
NOW = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.sql = None
        self.params = None
        self.closed = False

    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, row):
        self.cursor_value = FakeCursor(row)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self):
        self.closed = True


def source_row(*, symbol="BTCUSDC", interval="1m", ts=None, regime="RANGE"):
    ts = ts or NOW - timedelta(minutes=1)
    return (symbol, interval, ts, regime, ts + timedelta(seconds=2), Decimal("0.812345"))


def load(row, *, symbol="BTCUSDC", interval="1m"):
    conn = FakeConnection(row)
    result = load_canonical_regime_at_decision(
        lambda: conn, symbol=symbol, interval=interval,
        decision_candle_timestamp=NOW,
    )
    return result, conn


def frozen_context(regime="RANGE", source_ts=None):
    source_ts = source_ts or NOW - timedelta(minutes=1)
    return EvaluationContext(
        deployment_id="local-paper", environment="trading_paper",
        symbol="BTCUSDC", interval="1m", strategy="RSI",
        candle_open_time=NOW, evaluation_started_at=NOW,
        engine_name="RSI", paper_mode=True, market_regime=regime,
        regime_confidence=Decimal("0.812345"),
        context={
            "regime_attribution_version": CANONICAL_REGIME_ATTRIBUTION_VERSION,
            "regime_source": "market_regime",
            "regime_source_symbol": "BTCUSDC",
            "regime_source_interval": "1m",
            "regime_source_ts": source_ts,
            "regime_source_created_at": source_ts + timedelta(seconds=2),
            "regime_source_confidence": Decimal("0.812345"),
        },
    )


def observation(ctx):
    decision = FinalDecision.no_trade(
        ctx, DecisionReason.NO_SIGNAL, finished_at=NOW,
        reference_price=Decimal("100"),
    )
    return event_from_final_decision(
        decision, event_id="00000000-0000-0000-0000-000000000001",
        decision_key="regime-test", source_service="bot-runner",
        source_instance="test",
    )


def test_01_market_regime_before_candle_is_frozen():
    item, _ = load(source_row())
    assert item.regime == "RANGE" and item.confidence == Decimal("0.812345")


def test_02_future_market_regime_is_excluded_by_query():
    _, conn = load(None)
    assert "ts<=%s" in conn.cursor_value.sql
    assert conn.cursor_value.params[2] == NOW


def test_03_latest_bounded_row_is_selected_deterministically():
    _, conn = load(source_row())
    assert "ORDER BY ts DESC" in conn.cursor_value.sql
    assert "LIMIT 1" in conn.cursor_value.sql


def test_04_wrong_symbol_has_no_source():
    item, conn = load(None, symbol="ETHUSDC")
    assert item is None and conn.cursor_value.params[0] == "ETHUSDC"


def test_05_wrong_interval_has_no_source():
    item, conn = load(None, interval="5m")
    assert item is None and conn.cursor_value.params[1] == "5m"


def test_06_no_source_is_explicit_without_invented_regime():
    regime, confidence, provenance = evaluation_regime_fields(
        lambda: FakeConnection(None), symbol="BTCUSDC", interval="1m",
        decision_candle_timestamp=NOW, paper_mode=True,
    )
    assert regime is None and confidence is None
    assert provenance["regime_source"] == "market_regime"
    assert provenance["regime_source_ts"] is None


def test_07_live_context_does_not_read_or_change_regime():
    def forbidden():
        raise AssertionError("LIVE must not read canonical PAPER attribution")
    assert evaluation_regime_fields(
        forbidden, symbol="BTCUSDC", interval="1m",
        decision_candle_timestamp=NOW, paper_mode=False,
    ) == (None, None, {})


@pytest.mark.parametrize("name,builder", [
    ("RSI", "_rsi_evaluation_context"),
    ("TREND", "_trend_evaluation_context"),
    ("SUPERTREND", "_supertrend_evaluation_context"),
    ("BBRANGE", "_bbrange_evaluation_context"),
])
def test_08_to_11_all_strategy_contexts_propagate_frozen_regime(
    name, builder,
):
    source = STRATEGY_FILES[name].read_text()
    start = source.index(f"def {builder}(")
    body = source[start:source.index("\n\ndef ", start + 5)]
    assert "evaluation_regime_fields(" in body
    assert "market_regime=market_regime" in body
    assert "regime_confidence=regime_confidence" in body
    assert "**regime_context" in body


def test_12_observation_regime_is_populated_from_context():
    event = observation(frozen_context())
    assert event.regime == "RANGE" and event.regime_confidence == Decimal("0.812345")


def test_13_observation_event_digest_covers_frozen_regime():
    assert observation(frozen_context("RANGE")).event_digest != observation(
        frozen_context("TREND_UP")
    ).event_digest


def test_14_observation_event_digest_covers_source_timestamp():
    assert observation(frozen_context()).event_digest != observation(
        frozen_context(source_ts=NOW - timedelta(minutes=2))
    ).event_digest


def test_15_final_decision_keeps_exact_frozen_context():
    ctx = frozen_context()
    decision = FinalDecision.no_trade(
        ctx, DecisionReason.NO_SIGNAL, finished_at=NOW,
    )
    assert decision.evaluation is ctx


def test_16_replay_projection_contains_frozen_source_not_market_read():
    for field in (
        "market_regime", "regime_source", "regime_source_ts",
        "regime_source_created_at", "regime_source_confidence",
        "regime_attribution_version",
    ):
        assert f'"{field}": payload.get' in TRANSPORT
    assert "FROM market_regime" not in TRANSPORT


def test_17_registry_function_accepts_regime_and_provenance():
    signature = "p_market_regime TEXT,\n    p_regime_source JSONB"
    assert signature in MIGRATION


def test_18_registry_row_persists_market_regime():
    assert "strategy,market_regime" in MIGRATION
    assert "p_strategy,p_market_regime" in MIGRATION


def test_19_observation_and_registry_use_the_same_frozen_value():
    for path in STRATEGY_FILES.values():
        source = path.read_text()
        assert "evaluation.market_regime" in source
        assert "frozen_regime_provenance(evaluation)" in source


def test_20_positions_are_created_with_frozen_market_regime():
    for path in STRATEGY_FILES.values():
        source = path.read_text()
        assert "entry_client_order_id, market_regime" in source


def test_21_historical_null_rows_are_not_backfilled():
    upper = MIGRATION.upper()
    assert "UPDATE PUBLIC.CAUSAL_DECISION_OBSERVATION_V1" not in upper
    assert "SET MARKET_REGIME=" not in upper


def test_22_strategy_identity_is_exact_and_has_no_super_trend_alias():
    assert "('RSI','TREND','SUPERTREND','BBRANGE')" in MIGRATION
    assert "SUPER_TREND" not in MIGRATION


def test_23_learning_canonical_universe_uses_registry_regime():
    assert "r.market_regime" in LEARNING_MIGRATION
    assert "min(r.market_regime) AS market_regime" in LEARNING_MIGRATION
    assert "positions.market_regime" not in LEARNING_MIGRATION


def test_24_migration_is_paper_only():
    assert "current_database()<>'trading_paper'" in MIGRATION
    assert "CANONICAL_REGIME_ATTRIBUTION_PAPER_ONLY" in MIGRATION


def test_25_missing_regime_entry_fails_closed():
    assert "CANONICAL_REGIME_ATTRIBUTION_REQUIRED" in MIGRATION
    assert "v_registry.market_regime IS NULL" in MIGRATION


def test_26_position_and_registry_regime_must_match_before_fill():
    assert "FORWARD_POSITION_REGIME_CONFLICT" in MIGRATION
    assert "market_regime IS NOT DISTINCT FROM v_registry_regime" in MIGRATION


def test_27_forward_cutover_marker_is_append_only_ledger_row():
    assert "schema_migration_ledger_v1" in MIGRATION
    assert "'CANONICAL_REGIME_ATTRIBUTION_V1'" in MIGRATION

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.long_horizon_l3 import (
    TARGET_EXIT_REASON,
    active,
    canonical_sample_identity,
    is_preserved_risk_exit,
    sample_bucket,
    target_reached,
)


ROOT = Path(__file__).resolve().parents[1]


def test_local_paper_only_authority():
    base = {"TRADING_MODE": "PAPER", "DEPLOYMENT_ID": "local-paper",
            "LONG_HORIZON_L3_MODE": "TREATMENT"}
    assert active(base)
    assert not active({**base, "TRADING_MODE": "LIVE"})
    assert not active({**base, "DEPLOYMENT_ID": "vps-paper"})
    assert not active({**base, "LONG_HORIZON_L3_MODE": "OFF"})


def test_sampling_is_deterministic_and_exactly_mod_ten():
    at = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    buckets = []
    for gate_id in range(1, 1001):
        identity = canonical_sample_identity(
            gate_event_id=gate_id, symbol="BTCUSDC", interval="1m",
            strategy="RSI", side="BUY", candle_open_time=at,
        )
        assert identity == canonical_sample_identity(
            gate_event_id=gate_id, symbol="BTCUSDC", interval="1m",
            strategy="RSI", side="BUY", candle_open_time=at,
        )
        buckets.append(sample_bucket(identity))
    assert set(buckets) == set(range(10))
    assert all(0 <= value <= 9 for value in buckets)


def test_three_percent_target_uses_realizable_net_over_entry_capital():
    assert not target_reached(Decimal("0.599999"), Decimal("20"))
    assert target_reached(Decimal("0.600000"), Decimal("20"))
    assert target_reached(Decimal("0.61"), Decimal("20"))
    assert not target_reached(Decimal("1"), Decimal("0"))


def test_only_hard_risk_authorities_bypass_l3_exit_suppression():
    for reason in ("STOP_LOSS", "PANIC", "MANUAL_EMERGENCY_EXIT",
                   "POSITION_INTEGRITY_EMERGENCY", "RISK_BUDGET_FORCED_EXIT"):
        assert is_preserved_risk_exit(reason)
    for reason in ("TAKE_PROFIT", "TIME_EXIT", "PROFIT_LOCK_TRAIL_DROP",
                   "SOFT_EXIT", "EARLY_CUT", "GUARDED_PROFIT_EXIT"):
        assert not is_preserved_risk_exit(reason)
    assert not is_preserved_risk_exit(TARGET_EXIT_REASON)


def test_all_four_paper_strategies_run_common_l3_owner_and_atomic_entry():
    for relative in ("bot/main.py", "bot_trend/main.py", "bot_supertrend/main.py",
                     "bot_bbrange/main.py"):
        source = (ROOT / relative).read_text()
        assert "run_long_horizon_l3_owner_cycle" in source
        assert "record_forward_paper_entry_atomic(" in source
        assert "LOCAL_PAPER_L3" in source


def test_migration_is_local_paper_only_and_contract_is_frozen():
    migration = (ROOT / "db/migrations/20260907_long_horizon_l3_direct_local_paper_v1.sql").read_text()
    assert "LONG_HORIZON_L3_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in migration
    assert "current_database()<>'trading_paper'" in migration
    assert "L3_SHA256_GATE_EVENT_ID_MOD10_EQ0_V1" in migration
    assert "LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1" in migration
    assert "'target_realizable_net_rate','0.03'" in migration
    assert "regime_mode='DRY_RUN'" in migration


def test_atomic_writer_owns_l3_admission_and_exact_linkage():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    prepare = source.index("prepare_admission_cursor")
    create = source.index("written = create_simulated_order_cursor", prepare)
    fill = source.index("result = record_simulated_fill_evidence", create)
    finalize = source.index("finalize_admission_cursor", fill)
    assert prepare < create < fill < finalize


def test_existing_exit_is_intercepted_before_order_insert():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    guard = source.index("guard_exit_cursor")
    insert = source.index("INSERT INTO simulated_orders", guard)
    assert guard < insert

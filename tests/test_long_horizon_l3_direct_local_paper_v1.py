from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.long_horizon_l3 import (
    ALLOW_SAMPLING_PROBABILITY,
    ALLOW_SAMPLING_THRESHOLD,
    BLOCK_SAMPLING_PROBABILITY,
    BLOCK_SAMPLING_THRESHOLD,
    EXPECTED_NOTIONAL,
    GLOBAL_HEAT_RATE,
    MINIMUM_EQUITY_FOR_BLOCK_CAPACITY,
    PRIMARY_SLEEVE_RATE,
    SAMPLING_FINGERPRINT,
    SAMPLING_SALT,
    SECONDARY_SLEEVE_RATE,
    TARGET_EXIT_REASON,
    CONTRACT_VERSION,
    active,
    available_slots,
    canonical_opportunity_identity,
    is_preserved_risk_exit,
    normalize_per_allocated_usdc,
    quantity_for_l3_notional,
    sample_digest,
    selected_from_digest,
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


def test_sampling_is_salted_deterministic_and_cohort_specific():
    at = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)
    identity = canonical_opportunity_identity(
        gate_event_id=17, symbol="BTCUSDC", interval="1m",
        strategy="RSI", side="BUY", candle_open_time=at,
    )
    assert identity == canonical_opportunity_identity(
        gate_event_id=17, symbol="BTCUSDC", interval="1m",
        strategy="RSI", side="BUY", candle_open_time=at,
    )
    allow = sample_digest(identity, "L3_REGIME_WOULD_ALLOW")
    block = sample_digest(identity, "L3_REGIME_WOULD_BLOCK_SAMPLE")
    assert allow == sample_digest(identity, "L3_REGIME_WOULD_ALLOW")
    assert allow != block
    assert SAMPLING_SALT == "0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057"
    assert ALLOW_SAMPLING_THRESHOLD == int(Decimal("0.13194281540") * (2 ** 256))
    assert BLOCK_SAMPLING_THRESHOLD == int(Decimal("0.07920637611") * (2 ** 256))


def test_exact_allow_and_block_uint256_thresholds_are_strict():
    assert selected_from_digest("L3_REGIME_WOULD_ALLOW", ALLOW_SAMPLING_THRESHOLD - 1)
    assert not selected_from_digest("L3_REGIME_WOULD_ALLOW", ALLOW_SAMPLING_THRESHOLD)
    assert selected_from_digest("L3_REGIME_WOULD_BLOCK_SAMPLE", BLOCK_SAMPLING_THRESHOLD - 1)
    assert not selected_from_digest("L3_REGIME_WOULD_BLOCK_SAMPLE", BLOCK_SAMPLING_THRESHOLD)


def test_nine_usdc_quantity_capacity_pause_and_normalization_contract():
    assert EXPECTED_NOTIONAL == Decimal("9")
    assert quantity_for_l3_notional(
        price=Decimal("80000"), step=Decimal("0.00000001"),
        min_qty=Decimal("0.0001"), min_notional=Decimal("0"),
    ) == Decimal("0.00011250")
    equity = Decimal("635.430007829136")
    assert available_slots(equity, PRIMARY_SLEEVE_RATE) == 28
    assert available_slots(equity, SECONDARY_SLEEVE_RATE) == 14
    assert available_slots(equity, GLOBAL_HEAT_RATE) == 42
    assert MINIMUM_EQUITY_FOR_BLOCK_CAPACITY == Decimal("585")
    assert normalize_per_allocated_usdc(Decimal("0.45")) == Decimal("0.05")


def test_current_instrument_minimum_guard_never_increases_notional():
    try:
        quantity_for_l3_notional(
            price=Decimal("100000"), step=Decimal("0.00000001"),
            min_qty=Decimal("0.0001"), min_notional=Decimal("0"),
        )
    except ValueError as exc:
        assert str(exc) == "MIN_NOTIONAL_NOT_MET"
    else:
        raise AssertionError("9 USDC was automatically increased to meet minimum")


def test_sampling_contract_is_unchanged_and_versioned_for_v3():
    assert ALLOW_SAMPLING_PROBABILITY == Decimal("0.13194281540")
    assert BLOCK_SAMPLING_PROBABILITY == Decimal("0.07920637611")
    assert SAMPLING_SALT == "0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057"
    assert CONTRACT_VERSION == "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3"
    assert SAMPLING_FINGERPRINT == "118b9099d707df87cd531376cf2718e683cb67239e80a4232e0f65767007bbbc"


def test_three_percent_target_uses_realizable_net_over_entry_capital():
    assert not target_reached(Decimal("0.269999"), Decimal("9"))
    assert target_reached(Decimal("0.270000"), Decimal("9"))
    assert target_reached(Decimal("0.28"), Decimal("9"))
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


def test_v3_migration_is_local_paper_only_idempotent_and_contract_is_frozen():
    migration = (ROOT / "db/migrations/20260908_long_horizon_l3_direct_local_paper_v3.sql").read_text()
    assert "LONG_HORIZON_L3_V3_LOCAL_PAPER_DEPLOYMENT_REQUIRED" in migration
    assert "current_database()<>'trading_paper'" in migration
    assert "L3_POWER_CALIBRATED_SALTED_SHA256_THRESHOLD_V1" in migration
    assert "'entry_notional_usdc','9'" in migration
    assert "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3" in migration
    assert "LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1" in migration
    assert "'target_realizable_net_rate','0.03'" in migration
    assert "regime_mode='DRY_RUN'" in migration
    assert "regime_mode IS DISTINCT FROM 'DRY_RUN'" in migration
    assert "PRE_L3_EXCLUDED" in migration
    assert "pre_cutoff_open_positions" in migration
    assert "activation_requires_zero_open_positions',false" in migration


def test_l3_assignment_and_paired_l0_use_separate_ledgers():
    migration = (ROOT / "db/migrations/20260908_long_horizon_l3_direct_local_paper_v3.sql").read_text()
    source = (ROOT / "common/long_horizon_l3.py").read_text()
    assert "long_horizon_l3_admission_v1" in source
    assert "long_horizon_l3_l0_comparator_v1" in source
    assert "contract_version,l0_comparator_version" in source
    assert "LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1" in migration


def test_atomic_writer_owns_l3_admission_and_exact_linkage():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    prepare = source.index("prepare_admission_cursor")
    create = source.index("written = create_simulated_order_cursor", prepare)
    fill = source.index("result = record_simulated_fill_evidence", create)
    finalize = source.index("finalize_admission_cursor", fill)
    assert prepare < create < fill < finalize


def test_minimum_notional_rejection_commits_evidence_before_any_order():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    prepare = source.index("l3_admission = prepare_admission_cursor")
    evidence_commit = source.index('l3_admission.status == "MIN_NOTIONAL_NOT_MET"', prepare)
    blocked_return = source.index('False, "MIN_NOTIONAL_NOT_MET", None, None', evidence_commit)
    create = source.index("written = create_simulated_order_cursor", blocked_return)
    assert prepare < evidence_commit < blocked_return < create


def test_existing_exit_is_intercepted_before_order_insert():
    source = (ROOT / "common/simulated_execution_evidence.py").read_text()
    guard = source.index("guard_exit_cursor")
    insert = source.index("INSERT INTO simulated_orders", guard)
    assert guard < insert

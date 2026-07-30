from decimal import Decimal
from pathlib import Path

from common.supertrend_terminal_outcome import ReconcileResult


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "common/supertrend_terminal_outcome.py").read_text()
BOT = (ROOT / "bot_supertrend/main.py").read_text()
GATE_SQL = (
    ROOT / "db/migrations/20260730_paper_strategy_entry_gate_v1.sql"
).read_text()
INTENT_SQL = (
    ROOT / "db/migrations/20260730_supertrend_exit_intent_v1.sql"
).read_text()


def test_entry_gate_is_strategy_scoped_entry_only_and_operator_owned():
    assert "PRIMARY KEY (environment,deployment_id,strategy)" in GATE_SQL
    assert "entries_enabled" in GATE_SQL
    assert "operator_reason" in GATE_SQL
    assert "paper_supertrend_entries_enabled" in BOT
    gate = BOT.index("paper_supertrend_entries_enabled(", BOT.index("def execute_and_record"))
    order = BOT.index("insert_simulated_order(", gate)
    assert gate < order
    assert "and not is_exit" in BOT[gate - 800:gate + 800]
    assert "symbol=cfg_used.symbol" in BOT
    assert "interval=cfg_used.interval" in BOT
    assert "SLOT_CANARY_CONSUMED" in SOURCE


def test_exit_intent_is_immutable_and_contains_no_outcome_state():
    assert "BEFORE UPDATE OR DELETE" in INTENT_SQL
    assert "content_fingerprint" in INTENT_SQL
    for forbidden in ("gross_pnl", "net_pnl", "fees_usdc", "remaining_inventory"):
        assert forbidden not in INTENT_SQL


def test_reconciler_is_position_scoped_decimal_and_post_terminal():
    assert "WHERE position_id=%s" in SOURCE
    assert "Decimal(str(" in SOURCE
    assert "inventory_evidence_status" in SOURCE
    assert "remaining_inventory_qty" in SOURCE
    assert "count(*) FROM position_lifecycle_events_c2_2" in SOURCE
    assert "status='CLOSED'" in SOURCE
    assert "WHERE id=%s AND strategy='SUPERTREND'" in SOURCE
    update = SOURCE[SOURCE.index("UPDATE positions"):]
    assert "status='OPEN'" not in update


def test_reconciler_fail_closed_contract_for_missing_evidence():
    for reason in (
        "POSITION_OR_INTENT_NOT_ELIGIBLE",
        "INVENTORY_NOT_TERMINAL",
        "TERMINAL_LIFECYCLE_COUNT_INVALID",
        "FILL_EVIDENCE_INCOMPLETE",
        "AUTHORITATIVE_FEE_MISSING",
        "EXISTING_OUTCOME_CONFLICT",
    ):
        assert reason in SOURCE
    assert "SUPERTREND_OUTCOME_UNRESOLVED" in BOT


def test_result_supports_win_loss_and_genuine_flat_without_float():
    win = ReconcileResult(True, "RECONCILED", Decimal("1"), Decimal(".2"), Decimal(".8"))
    loss = ReconcileResult(True, "RECONCILED", Decimal("-1"), Decimal(".2"), Decimal("-1.2"))
    flat = ReconcileResult(True, "RECONCILED", Decimal(".2"), Decimal(".2"), Decimal("0"))
    assert win.net > 0
    assert loss.net < 0
    assert flat.net == 0 and flat.gross != 0 and flat.fees != 0


def test_runtime_wiring_skips_legacy_close_only_after_reconciliation():
    assert 'result.get("terminal_outcome_reconciled")' in BOT
    assert '"position_close_succeeded": True' in BOT
    assert "record_simulated_fill_evidence" in BOT
    assert "reconcile_terminal_compatibility_outcome" in BOT

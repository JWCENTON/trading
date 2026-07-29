from pathlib import Path

from common.decision_contract import DecisionSubtype
from common.exchange_ingest_trades import RECONCILE_OKX_EXIT_FILLS_C2_2_SQL


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "db/migrations/20260729_fee_aware_inventory_terminal_dust_c2_2.sql"
).read_text()


def test_reconciler_has_no_magic_gross_close_threshold():
    assert "* 0.999" not in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "entry_base_fee" in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "TERMINAL_DUST_CLOSE" in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "POSITION_CLOSED_TERMINAL_DUST" in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL


def test_lifecycle_event_is_in_same_transaction_and_idempotent():
    assert "position_lifecycle_events_c2_2" in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "ON CONFLICT DO NOTHING" in RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
    assert "UNIQUE (position_id, order_id, mutation_kind, mutation_high_water)" in MIGRATION


def test_migration_is_forward_only_and_does_not_backfill_positions():
    upper = MIGRATION.upper()
    assert "UPDATE POSITIONS" not in upper
    assert "ADD COLUMN IF NOT EXISTS" in upper
    assert upper.startswith("BEGIN;")
    assert upper.rstrip().endswith("COMMIT;")


def test_final_decision_taxonomy_exposes_c2_2_outcomes():
    assert DecisionSubtype.FULL_EXECUTION.value == "FULL_EXECUTION"
    assert (
        DecisionSubtype.TERMINAL_DUST_EXECUTION_COMPLETE.value
        == "TERMINAL_DUST_EXECUTION_COMPLETE"
    )
    assert (
        DecisionSubtype.INCOMPLETE_EXECUTION_EVIDENCE.value
        == "INCOMPLETE_EXECUTION_EVIDENCE"
    )

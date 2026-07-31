from pathlib import Path

import pytest

from common.closed_outcome_read_model import (
    build_closed_outcome_rows_sql,
    build_closed_outcome_summary_sql,
)


ROOT = Path(__file__).resolve().parents[1]
API = (ROOT / "api/main.py").read_text()
MODEL = (ROOT / "common/closed_outcome_read_model.py").read_text()


def test_all_paper_consumers_use_shared_outcome_summary():
    assert API.count("fetch_closed_outcome_summary(") == 3
    assert API.count("fetch_closed_outcomes(") == 1
    assert "def ui_trading_24h" in API
    assert "def ui_account_summary" in API
    assert "def fetch_realized_pnl_stats" in API
    assert "def ui_recent_closed" in API


def test_model_is_bounded_and_never_uses_terminal_qty_for_pnl():
    assert "p.exit_time >= %(window_start)s" in MODEL
    assert "p.exit_time <= %(window_end)s" in MODEL
    assert "p.qty" not in MODEL
    assert "FROM simulated_execution_fills_v1 f" in MODEL
    assert "JOIN binance_order_fills f ON f.order_id = p.order_id" in MODEL
    assert "LEGACY_EXECUTION_PROVEN" in MODEL


def test_environment_uses_physically_isolated_query_shapes():
    paper = build_closed_outcome_summary_sql("PAPER")
    live = build_closed_outcome_rows_sql("LIVE")
    assert "binance_order_fills" not in paper
    assert "exchange_fill_ingestion_state_v2" not in paper
    assert "simulated_execution_fills_v1" not in live
    assert " OR f.order_id" not in live
    with pytest.raises(ValueError, match="unsupported closed-outcome environment"):
        build_closed_outcome_summary_sql("UNKNOWN")


def test_recent_rows_can_be_bounded_to_the_preselected_position_cohort():
    sql = build_closed_outcome_rows_sql("PAPER", bounded_position_ids=True)
    assert "p.id = ANY(%(position_ids)s)" in sql
    assert "ORDER BY position_id" in sql


def test_unresolved_and_flat_are_distinct():
    assert "WHEN NOT evidence_complete THEN 'UNRESOLVED'" in MODEL
    assert "WHEN net_pnl_usdc < 0 THEN 'LOSS'" in MODEL
    assert "ELSE 'FLAT'" in MODEL
    assert "trades - wins" not in MODEL


def test_api_exposes_compatibility_and_coverage_fields():
    for field in (
        "closed_pnl_24h",
        "trades_24h",
        "wins_24h",
        "losses_24h",
        "window_start",
        "window_end",
        "resolved_trades",
        "unresolved_trades",
        "coverage_ratio",
        "outcome_source_counts",
    ):
        assert field in API
    for field in (
        "account_value_status", "realized_coverage_count",
        "closed_positions_count", "realized_coverage_pct",
        "realized_source_breakdown", "unrealized_pnl",
        "calculation_method",
    ):
        assert field in API


def test_paper_account_frontend_labels_partial_bridge_honestly():
    panel = (
        ROOT / "frontend/src/components/live/AccountSnapshotPanel.tsx"
    ).read_text()
    assert "Partial reconstructed estimate" in panel
    assert "Realized coverage:" in panel
    assert 'isExchangeTruth ? "Exchange truth"' in panel

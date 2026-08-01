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
    assert "p.qty" in MODEL  # evidence eligibility only, never outcome arithmetic
    assert "p.qty *" not in MODEL
    assert "FROM simulated_execution_fills_v1 f" in MODEL
    assert "JOIN binance_order_fills f ON f.order_id = p.order_id" in MODEL
    assert "LEGACY_EXECUTION_PROVEN" in MODEL


def test_environment_uses_physically_isolated_query_shapes():
    paper = build_closed_outcome_summary_sql("PAPER")
    live = build_closed_outcome_rows_sql("LIVE")
    assert "binance_order_fills" not in paper
    assert "binance_order_fills" not in paper
    assert "simulated_execution_fills_v1" not in live
    assert " OR f.order_id" not in live
    with pytest.raises(ValueError, match="unsupported closed-outcome environment"):
        build_closed_outcome_summary_sql("UNKNOWN")


def test_summary_uses_grouped_counters_without_per_row_windows_or_json():
    summary = build_closed_outcome_summary_sql("PAPER")
    suffix = summary[summary.index(", summary_outcomes AS MATERIALIZED"):]
    assert " OVER (PARTITION BY " not in suffix
    assert "jsonb_build_object" not in suffix
    assert "jsonb_agg" not in suffix
    assert "GROUP BY outcome_source" in suffix
    assert "GROUP BY quality_class" in suffix
    assert "GROUP BY normalization_status" in suffix
    assert "GROUP BY rollout_impact" in suffix
    assert "strategy_events" not in summary
    assert "binance_order_fills" not in summary


def test_recent_rows_can_be_bounded_to_the_preselected_position_cohort():
    sql = build_closed_outcome_rows_sql("PAPER", bounded_position_ids=True)
    assert "p.id = ANY(%(position_ids)s)" in sql
    assert "ORDER BY position_id" in sql


def test_administrative_retirement_is_excluded_from_reporting_not_accounting():
    performance = build_closed_outcome_summary_sql("PAPER")
    account = build_closed_outcome_summary_sql(
        "PAPER", include_administrative_retirements=True
    )
    assert "LEGACY_ADMINISTRATIVE_CLOSE" in performance
    assert "LEGACY_ADMINISTRATIVE_CLOSE" not in account
    assert "include_administrative_retirements=True" in API


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
    assert "Resolved:" in panel
    assert "High assurance:" in panel
    assert "Legacy compatible:" in panel
    assert 'isExchangeTruth ? "Exchange truth"' in panel


def test_component_rounding_diagnostics_are_exposed_without_changing_source():
    api = (ROOT / "api/main.py").read_text()
    assert '"gross_rounding_bound"' in api
    assert '"fee_rounding_bound"' in api
    assert '"maximum_explainable_net_delta"' in api
    assert "component_rounding_accumulation_count" in api
    assert "material_conflict_count" in api
    paper = build_closed_outcome_summary_sql("PAPER")
    assert "COMPONENT_ROUNDING_ACCUMULATION" in paper
    assert "PAPER_SIMULATED_FILLS" in paper


def test_rollout_impact_and_source_confidence_contract_is_exposed():
    for field in (
        "selected_source_confidence",
        "rollout_impact",
        "comparison_source",
        "comparison_source_confidence",
        "source_superseded_reason",
        "position_order_linkage_status",
        "derived_entry_order_id",
        "derived_exit_order_id",
        "blocking_conflict_count",
        "superseded_conflict_count",
        "authoritative_conflict_count",
        "evidence_inconsistent_count",
        "not_evaluable_count",
        "rollout_gate_status",
        "rollout_impact_counts",
    ):
        assert field in API

    paper = build_closed_outcome_summary_sql("PAPER")
    assert "NON_BLOCKING_SOURCE_SUPERSEDED" in paper
    assert "BLOCKING_AUTHORITATIVE_CONFLICT" in paper
    assert "BLOCKING_EVIDENCE_INCONSISTENT" in paper
    assert (
        "HIGH_ASSURANCE_SIMULATED_FILLS_SUPERSEDE_"
        "UNTRUSTED_STORED_ZERO_PLACEHOLDER"
    ) in paper
    assert (
        "HIGH_ASSURANCE_SIMULATED_FILLS_SUPERSEDE_UNTRUSTED_"
        "STORED_ZERO_PLACEHOLDER_WITH_DERIVED_ORDER_LINKAGE"
    ) in paper
    for linkage_status in (
        "EXPLICIT_POSITION_ORDER_LINKAGE",
        "DERIVED_UNIQUE_FILL_LIFECYCLE_LINKAGE",
        "MISSING_ORDER_LINKAGE",
        "AMBIGUOUS_ORDER_LINKAGE",
        "CONFLICTING_ORDER_LINKAGE",
    ):
        assert linkage_status in paper
    for strict_evidence in (
        "simulated_identity_complete",
        "inventory_evidence_status = 'COMPLETE'",
        "remaining_inventory_qty = 0",
        "terminal_close_count = 1",
        "no_pending_correction",
        "source_authority_consistent",
        "simulation_model_consistent",
        "order_position_assignment_consistent",
        "fee_evidence_consistent",
        "fill_economics_consistent",
        "order_identity_consistent",
        "matching_terminal_close_count = 1",
    ):
        assert strict_evidence in paper

    live = build_closed_outcome_rows_sql("LIVE")
    assert "NON_BLOCKING_SOURCE_SUPERSEDED" not in live
    assert "BLOCKING_AUTHORITATIVE_CONFLICT" not in live

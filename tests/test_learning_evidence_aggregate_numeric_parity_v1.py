from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "db/migrations/20260805_learning_evidence_aggregate_numeric_parity_v1.sql"
).read_text()


def test_one_versioned_financial_normalization_contract_is_shared():
    assert "learning_financial_normalize_v1" in MIGRATION
    assert "SELECT round(value, 12)" in MIGRATION
    assert "RETURNS NULL ON NULL INPUT" in MIGRATION
    assert "IMMUTABLE" in MIGRATION
    assert "LEARNING_EVIDENCE_AGGREGATE_NUMERIC_V1" in MIGRATION


def test_source_projection_scale_12_is_an_explicit_prerequisite():
    for column in (
        "gross_profit_usdc",
        "gross_loss_usdc",
        "net_pnl_usdc",
        "profit_factor",
        "expectancy_usdc",
    ):
        assert column in MIGRATION
    assert "numeric_scale IS DISTINCT FROM 12" in MIGRATION
    assert "SOURCE_SCALE_MISMATCH" in MIGRATION


def test_future_payload_persistence_parity_and_hash_share_normalized_values():
    for expression in (
        "sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc>0)",
        "sum(realized_pnl_usdc) FILTER (WHERE realized_pnl_usdc<0)",
        "sum(realized_pnl_usdc)) net_pnl",
        "sum(fees_usdc)) fees",
        "max(drawdown)) max_drawdown",
    ):
        assert f"learning_financial_normalize_v1({expression}" in MIGRATION

    for source in (
        "source_gross_profit_usdc",
        "source_gross_loss_usdc",
        "source_net_pnl_usdc",
        "source_profit_factor",
        "source_expectancy_usdc",
    ):
        assert f"learning_financial_normalize_v1(v_observation.{source})" in MIGRATION

    assert "IS DISTINCT FROM learning_financial_normalize_v1" in MIGRATION
    assert "v_aggregate_hash := encode(digest(v_aggregate::text,''sha256''),''hex'')" in MIGRATION
    assert "UPDATE learning_evidence_aggregates_v1" not in MIGRATION
    assert "DELETE FROM learning_evidence_aggregates_v1" not in MIGRATION


def test_migration_is_additive_idempotent_and_does_not_touch_trading_state():
    assert "CREATE OR REPLACE FUNCTION public.learning_financial_normalize_v1" in MIGRATION
    assert "WHERE NOT EXISTS (" in MIGRATION
    for table in (
        "positions",
        "orders",
        "fills",
        "strategy_params",
        "bot_control",
        "learning_exclusions",
    ):
        assert f"UPDATE {table}" not in MIGRATION
        assert f"DELETE FROM {table}" not in MIGRATION

from pathlib import Path


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

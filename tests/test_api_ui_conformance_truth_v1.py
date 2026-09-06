from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API = (ROOT / "api/main.py").read_text()
TYPES = (ROOT / "frontend/src/api.ts").read_text()
PANEL = (ROOT / "frontend/src/components/health/HealthPanel.tsx").read_text()


def test_api_exposes_four_independent_health_dimensions_and_and_gate():
    for field in (
        "process_health", "data_health", "semantic_health", "authority_health",
        "overall_readiness",
    ):
        assert f'"{field}"' in API
    assert "process_health and data_health and semantic_health and authority_health" in API


def test_api_exposes_actual_regime_source_mode_policy_and_authority():
    for field in (
        "runtime_revision", "regime_source", "regime_source_fresh",
        "effective_regime_mode", "policy_coverage", "actual_entry_authority",
    ):
        assert f'"{field}"' in API


def test_ui_contract_and_panel_do_not_call_process_health_overall_health():
    assert "overall_readiness: boolean" in TYPES
    assert "Semantic / authority" in PANEL
    assert "NOT ACCEPTED" in PANEL
    assert "Runtime revision" in PANEL

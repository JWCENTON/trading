from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_CONTRACTS = {
    "live_managed_capital_authority_v1_contract.json",
    "portfolio_state_v1_contract.json",
    "portfolio_state_v1_contract.sha256",
}


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_affected_runtime_images_copy_the_canonical_contract_directory() -> None:
    for dockerfile in ("automation_runner/Dockerfile", "api/Dockerfile"):
        source = _source(dockerfile)
        assert "COPY contracts /app/contracts" in source
        assert not any(f"COPY contracts/{name}" in source for name in REQUIRED_CONTRACTS)


def test_required_contract_files_are_repo_owned_and_manifested() -> None:
    contract_dir = ROOT / "contracts"
    assert contract_dir.is_dir()
    assert REQUIRED_CONTRACTS <= {path.name for path in contract_dir.iterdir()}


def test_other_bounded_runtime_images_are_already_packaged_or_do_not_import_contract_consumers() -> None:
    for dockerfile in (
        "services/bot_runner/Dockerfile",
        "services/bot_runner_orchestrator/Dockerfile",
    ):
        assert "COPY . /app" in _source(dockerfile)

    for entrypoint in ("market_data_worker/main.py", "regime_worker/main.py"):
        source = _source(entrypoint)
        assert "common.portfolio_state" not in source
        assert "common.live_managed_capital" not in source

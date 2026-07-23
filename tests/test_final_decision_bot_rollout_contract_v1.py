from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = (ROOT / "docs/final-decision-bot-rolling-rollout-v1.md").read_text()


def test_profiled_rollout_v1_is_explicitly_superseded():
    assert "superseded" in CONTRACT
    assert "MUST NOT be used" in CONTRACT
    assert "final-decision-consolidated-bot-runner-rollout-v2.md" in CONTRACT
    assert "atomic version boundary" in CONTRACT

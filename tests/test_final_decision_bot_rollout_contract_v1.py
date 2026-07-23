from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.yaml").read_text()
CONTRACT = (
    ROOT / "docs/final-decision-bot-rolling-rollout-v1.md"
).read_text()

SERVICES = {
    "bot-rsi-btc": ("bot/Dockerfile", "bot/main.py", 'source_service="bot-rsi"'),
    "bot-trend-btc": (
        "bot_trend/Dockerfile",
        "bot_trend/main.py",
        'source_service="bot-trend"',
    ),
    "bot-supertrend-btc": (
        "bot_supertrend/Dockerfile",
        "bot_supertrend/main.py",
        'source_service="bot-supertrend"',
    ),
    "bot-bbrange-btc": (
        "bot_bbrange/Dockerfile",
        "bot_bbrange/main.py",
        'source_service="bot-bbrange"',
    ),
}


def test_all_and_only_four_strategy_workers_have_shared_producer_wiring():
    wired = []
    for path in ("bot/main.py", "bot_trend/main.py", "bot_supertrend/main.py",
                 "bot_bbrange/main.py"):
        source = (ROOT / path).read_text()
        assert "finalize_decision_observation" in source
        wired.append(path)
    assert set(wired) == {value[1] for value in SERVICES.values()}


def test_each_bot_image_requires_one_immutable_target_sha():
    for service, (dockerfile_path, source_path, source_service) in SERVICES.items():
        dockerfile = (ROOT / dockerfile_path).read_text()
        source = (ROOT / source_path).read_text()
        assert "ARG GIT_SHA" in dockerfile
        assert "grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'" in dockerfile
        assert 'LABEL org.opencontainers.image.revision="${GIT_SHA}"' in dockerfile
        assert 'ENV GIT_SHA="${GIT_SHA}"' in dockerfile
        assert "COPY common /app/common" in dockerfile
        assert source_service in source
        section = COMPOSE[COMPOSE.index(f"  {service}:"):]
        assert "GIT_SHA: ${GIT_SHA}" in section[:500]


def test_contract_is_strictly_rolling_and_has_per_worker_stop_gates():
    for service in SERVICES:
        assert f"`{service}`" in CONTRACT
    for gate in (
        "preserve the old image",
        "restart count zero",
        "fresh healthy heartbeat",
        "RETRY",
        "DEAD_LETTER",
        "IDEMPOTENCY_CONFLICT",
        "duplicate decision keys",
        "Rollback only the current worker",
    ):
        assert gate in CONTRACT
    assert "recreate a Compose group" in CONTRACT
    assert "performs none" in CONTRACT

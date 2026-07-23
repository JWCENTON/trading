from pathlib import Path
import ast
import re


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "services/bot_runner/Dockerfile").read_text()
LAUNCHER = (ROOT / "services/bot_runner/main.py").read_text()
LIVE_OVERRIDE = (ROOT / "docker-compose.live.override.yaml").read_text()
PAPER_OVERRIDE = (ROOT / "docker-compose.paper.override.yaml").read_text()
BASE_COMPOSE = (ROOT / "docker-compose.yaml").read_text()
CONTRACT = (
    ROOT / "docs/final-decision-consolidated-bot-runner-rollout-v2.md"
).read_text()

STRATEGY_PATHS = {
    "RSI": ("bot/main.py", 'source_service="bot-rsi"'),
    "TREND": ("bot_trend/main.py", 'source_service="bot-trend"'),
    "SUPERTREND": (
        "bot_supertrend/main.py",
        'source_service="bot-supertrend"',
    ),
    "BBRANGE": ("bot_bbrange/main.py", 'source_service="bot-bbrange"'),
}

EXPECTED_LIVE_IDENTITIES = {
    ("RSI", symbol, interval)
    for symbol in ("BNBUSDC", "BTCUSDC", "ETHUSDC", "SOLUSDC")
    for interval in ("1m", "5m")
} | {
    (strategy, symbol, interval)
    for strategy in ("BBRANGE", "TREND", "SUPERTREND")
    for symbol, interval in (
        ("BNBUSDC", "1m"),
        ("BNBUSDC", "5m"),
        ("BTCUSDC", "5m"),
        ("ETHUSDC", "1m"),
        ("ETHUSDC", "5m"),
        ("SOLUSDC", "1m"),
        ("SOLUSDC", "5m"),
    )
} - {("BBRANGE", "ETHUSDC", "1m")}


def _assignment(name):
    tree = ast.parse(LAUNCHER)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name
                   for target in node.targets):
                return ast.literal_eval(node.value)
    raise AssertionError(f"{name} assignment missing")


def _service_section(compose, name):
    start = compose.index(f"  {name}:")
    following = re.search(r"(?m)^  [A-Za-z0-9_-]+:$", compose[start + 3:])
    return (
        compose[start:]
        if following is None
        else compose[start:start + 3 + following.start()]
    )


def test_consolidated_image_has_fail_closed_immutable_sha_contract():
    assert "ARG GIT_SHA" in DOCKERFILE
    assert "grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'" in DOCKERFILE
    assert 'LABEL org.opencontainers.image.revision="${GIT_SHA}"' in DOCKERFILE
    assert 'ENV GIT_SHA="${GIT_SHA}"' in DOCKERFILE
    assert "COMMIT_SHA" not in DOCKERFILE
    assert "git rev-parse" not in DOCKERFILE
    assert "COPY . /app" in DOCKERFILE


def test_live_and_paper_compose_pass_exact_sha_without_fallback():
    for override in (LIVE_OVERRIDE, PAPER_OVERRIDE):
        section = _service_section(override, "bot-runner")
        assert "dockerfile: services/bot_runner/Dockerfile" in section
        assert "GIT_SHA: ${GIT_SHA}" in section
        assert "GIT_SHA:-" not in section


def test_launcher_uses_all_and_only_four_existing_strategy_modules():
    commands = _assignment("STRATEGY_CMD")
    expected = {
        name: ["python", "-u", f"/app/{path}"]
        for name, (path, _) in STRATEGY_PATHS.items()
    }
    assert commands == expected
    assert len(set(tuple(command) for command in commands.values())) == 4
    assert 'env["SYMBOL"] = row["symbol"]' in LAUNCHER
    assert 'env["INTERVAL"] = row["interval"]' in LAUNCHER
    assert 'env["STRATEGY_NAME"] = row["strategy"]' in LAUNCHER
    assert "return subprocess.Popen(cmd, env=env)" in LAUNCHER


def test_each_child_module_has_one_public_sink_wrapper_and_stable_identity():
    for strategy, (path, source_service) in STRATEGY_PATHS.items():
        source = (ROOT / path).read_text()
        assert "from common.final_decision_observation_sink import " \
               "finalize_decision_observation" in source
        assert source.count("def run_strategy(") == 1 or (
            strategy == "TREND" and source.count("def run_trend_strategy(") == 1
        )
        assert source_service in source
        assert "finalize_decision_observation(" in source
    assert "finalize_decision_observation" not in LAUNCHER


def test_reviewed_live_inventory_is_28_unique_four_family_identities():
    assert len(EXPECTED_LIVE_IDENTITIES) == 28
    assert len(set(EXPECTED_LIVE_IDENTITIES)) == 28
    assert {item[0] for item in EXPECTED_LIVE_IDENTITIES} == set(STRATEGY_PATHS)
    counts = {
        strategy: sum(item[0] == strategy for item in EXPECTED_LIVE_IDENTITIES)
        for strategy in STRATEGY_PATHS
    }
    assert counts == {"RSI": 8, "TREND": 7, "SUPERTREND": 7, "BBRANGE": 6}
    assert "BotKey" in LAUNCHER
    assert '"running_bots": len(running)' in LAUNCHER


def test_profiled_services_remain_dormant_and_are_never_rollout_targets():
    for service in (
        "bot-rsi-btc",
        "bot-trend-btc",
        "bot-supertrend-btc",
        "bot-bbrange-btc",
    ):
        section = _service_section(BASE_COMPOSE, service)
        assert 'profiles: ["bots"]' in section
        assert f"`{service}`" in CONTRACT
    assert "must not be rebuilt or recreated" in CONTRACT


def test_v2_contract_is_atomic_and_excludes_paper_and_four_worker_rollout():
    for gate in (
        "28 unique child identities",
        "28/28 cadence-aware fresh child heartbeats",
        "Exactly one consolidated `bot-runner` container",
        "RETRY",
        "DEAD_LETTER",
        "IDEMPOTENCY_CONFLICT",
        "manual overrides = 0",
        "OOMKilled=false",
        "Recreate only `bot-runner`",
        "mixed-version child population",
        "PAPER and VPS operations",
    ):
        assert gate in CONTRACT
    assert "one-worker-at-a-time" not in CONTRACT.lower()
    assert "Rollback only the current worker" not in CONTRACT

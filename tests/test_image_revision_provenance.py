from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_DOCKERFILE = (ROOT / "api/Dockerfile").read_text()
FRONTEND_DOCKERFILE = (ROOT / "frontend/Dockerfile").read_text()
COMPOSE = (ROOT / "docker-compose.yaml").read_text()
LIVE_OVERRIDE = (ROOT / "docker-compose.live.override.yaml").read_text()
PAPER_OVERRIDE = (ROOT / "docker-compose.paper.override.yaml").read_text()
CANONICAL_REVISION = (
    "${REVISION:?set REVISION to the canonical full git SHA}"
)


def _service_section(service, next_service):
    return COMPOSE[
        COMPOSE.index(f"  {service}:"):COMPOSE.index(f"  {next_service}:")
    ]


def test_api_image_revision_and_runtime_sha_contract_is_preserved():
    api = _service_section("api", "frontend")
    assert "ARG GIT_SHA" in API_DOCKERFILE
    assert "^([0-9a-f]{40}|[0-9a-f]{64})$" in API_DOCKERFILE
    assert 'org.opencontainers.image.revision="${GIT_SHA}"' in API_DOCKERFILE
    assert 'ENV GIT_SHA="${GIT_SHA}"' in API_DOCKERFILE
    assert f"GIT_SHA: {CANONICAL_REVISION}" in api
    assert "GIT_SHA: ${GIT_SHA}" not in api


def test_frontend_candidate_requires_sha_and_has_oci_revision_labels():
    frontend = _service_section("frontend", "regime-worker")
    assert "ARG BUILD_MODE=development" in FRONTEND_DOCKERFILE
    assert "ARG GIT_SHA=DEV-UNKNOWN" in FRONTEND_DOCKERFILE
    assert 'if [ "$BUILD_MODE" = "candidate" ]' in FRONTEND_DOCKERFILE
    assert "^([0-9a-f]{40}|[0-9a-f]{64})$" in FRONTEND_DOCKERFILE
    assert (
        'LABEL org.opencontainers.image.revision="${GIT_SHA}"'
        in FRONTEND_DOCKERFILE
    )
    assert (
        'LABEL org.opencontainers.image.created="${BUILD_TIMESTAMP}"'
        in FRONTEND_DOCKERFILE
    )
    assert "BUILD_MODE: ${IMAGE_BUILD_MODE:-development}" in frontend
    assert f"GIT_SHA: {CANONICAL_REVISION}" in frontend


def test_active_services_use_one_fail_closed_revision_for_build_and_runtime():
    for service, next_service in (
        ("api", "frontend"),
        ("frontend", "regime-worker"),
        ("regime-worker", "market-data-worker"),
        ("market-data-worker", "automation-runner"),
        ("automation-runner", "bot-rsi-btc"),
    ):
        section = _service_section(service, next_service)
        assert f"GIT_SHA: {CANONICAL_REVISION}" in section

    for override in (LIVE_OVERRIDE, PAPER_OVERRIDE):
        assert override.count(f"GIT_SHA: {CANONICAL_REVISION}") == 4


def test_worker_images_carry_immutable_oci_and_runtime_revision():
    for path in (
        "services/bot_runner_orchestrator/Dockerfile",
        "market_data_worker/Dockerfile",
        "regime_worker/Dockerfile",
    ):
        dockerfile = (ROOT / path).read_text()
        assert "ARG GIT_SHA" in dockerfile
        assert "^([0-9a-f]{40}|[0-9a-f]{64})$" in dockerfile
        assert 'LABEL org.opencontainers.image.revision="${GIT_SHA}"' in dockerfile
        assert 'ENV GIT_SHA="${GIT_SHA}"' in dockerfile


def test_frontend_build_info_is_generated_without_environment_or_secrets():
    assert "> public/build-info.json" in FRONTEND_DOCKERFILE
    assert '"git_sha"' in FRONTEND_DOCKERFILE
    assert '"build_timestamp"' in FRONTEND_DOCKERFILE
    for forbidden in (
        "DB_PASS", "API_KEY", "API_SECRET", "PASSPHRASE", "OPENAI_API_KEY",
    ):
        assert forbidden not in FRONTEND_DOCKERFILE


def test_development_build_is_explicitly_unverified():
    assert "GIT_SHA=DEV-UNKNOWN" in FRONTEND_DOCKERFILE
    assert "BUILD_TIMESTAMP=DEV-UNKNOWN" in FRONTEND_DOCKERFILE
    assert 'test "$GIT_SHA" = "DEV-UNKNOWN"' in FRONTEND_DOCKERFILE

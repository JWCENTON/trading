from scripts.verify_runtime_release_truth import inspect_service, verify


def _inspect(*, revision="a" * 40, config=True):
    labels = {
        "org.opencontainers.image.revision": revision,
        "com.docker.compose.service": "bot-runner",
    }
    if config:
        labels.update(
            {
                "com.docker.compose.project.config_files": "docker-compose.yaml,docker-compose.paper.override.yaml",
                "com.docker.compose.project.environment_file": ".env.okx.paper",
                "com.docker.compose.config-hash": "abc123",
            }
        )
    return [
        {
            "Image": "sha256:image",
            "Config": {
                "Labels": labels,
                "Env": [
                    f"GIT_SHA={revision}",
                    "TRADING_MODE=PAPER",
                    "REGIME_MODE=ENFORCE",
                    "DB_PASS=must-not-be-reported",
                ],
            },
        }
    ]


def test_matching_image_runtime_and_compose_truth_pass(monkeypatch):
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth._run",
        lambda *args: __import__("json").dumps(_inspect()),
    )
    result = inspect_service("paper-bot", "a" * 40)
    assert result.parity is True
    assert result.effective_config == {
        "TRADING_MODE": "PAPER",
        "REGIME_MODE": "ENFORCE",
    }


def test_revision_mismatch_is_not_hidden(monkeypatch):
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth._run",
        lambda *args: __import__("json").dumps(_inspect(revision="b" * 40)),
    )
    result = inspect_service("paper-bot", "a" * 40)
    assert result.parity is False
    assert "IMAGE_REVISION_MISMATCH" in result.errors
    assert "RUNTIME_REVISION_MISMATCH" in result.errors


def test_missing_compose_provenance_fails(monkeypatch):
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth._run",
        lambda *args: __import__("json").dumps(_inspect(config=False)),
    )
    assert "COMPOSE_PROVENANCE_INCOMPLETE" in inspect_service(
        "paper-bot", "a" * 40
    ).errors


def test_verify_detects_non_uniform_services(monkeypatch):
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth.repository_revision",
        lambda repo: ("a" * 40, "a" * 40),
    )
    revisions = iter(("a" * 40, "b" * 40))
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth.inspect_service",
        lambda name, expected: type(
            "Truth",
            (),
            {
                "runtime_revision": next(revisions),
                "parity": True,
                "__dict__": {},
            },
        )(),
    )
    # The real dataclass serialization is covered by the service tests; use a
    # minimal serializable stand-in for aggregate revision detection.
    monkeypatch.setattr(
        "scripts.verify_runtime_release_truth.asdict", lambda item: {}
    )
    result = verify(__import__("pathlib").Path("."), ["one", "two"], None)
    assert "SERVICE_RUNTIME_REVISIONS_NON_UNIFORM" in result["errors"]

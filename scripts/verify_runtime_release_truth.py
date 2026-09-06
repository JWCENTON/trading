#!/usr/bin/env python3
"""Read-only release-truth verifier for WalTrade Compose services.

The verifier intentionally uses only existing OCI/Compose metadata and the
container's explicit ``GIT_SHA``.  It never treats the repository HEAD alone as
proof of deployed code and never prints the full container environment.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


SAFE_CONFIG_KEYS = (
    "TRADING_MODE",
    "REGIME_MODE",
    "REGIME_ENABLED",
    "STRATEGY_NAME",
    "SYMBOL",
    "INTERVAL",
)


@dataclass(frozen=True)
class ServiceTruth:
    container: str
    service: str | None
    image_id: str
    image_revision: str | None
    runtime_revision: str | None
    compose_files: str | None
    compose_environment_file: str | None
    compose_config_hash: str | None
    effective_config: dict[str, str]
    parity: bool
    errors: tuple[str, ...]


def _run(*args: str) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True
    ).stdout.strip()


def repository_revision(repo: Path) -> tuple[str, str]:
    return (
        _run("git", "-C", str(repo), "rev-parse", "HEAD"),
        _run("git", "-C", str(repo), "rev-parse", "origin/main"),
    )


def inspect_service(container: str, expected_revision: str) -> ServiceTruth:
    raw = json.loads(_run("docker", "inspect", container))[0]
    labels = raw.get("Config", {}).get("Labels") or {}
    environment = {}
    for item in raw.get("Config", {}).get("Env") or []:
        key, separator, value = item.partition("=")
        if separator and (key == "GIT_SHA" or key in SAFE_CONFIG_KEYS):
            environment[key] = value

    image_revision = labels.get("org.opencontainers.image.revision")
    runtime_revision = environment.get("GIT_SHA")
    config_files = labels.get("com.docker.compose.project.config_files")
    environment_file = labels.get("com.docker.compose.project.environment_file")
    config_hash = labels.get("com.docker.compose.config-hash")
    errors = []
    if image_revision != expected_revision:
        errors.append("IMAGE_REVISION_MISMATCH")
    if runtime_revision != expected_revision:
        errors.append("RUNTIME_REVISION_MISMATCH")
    if image_revision != runtime_revision:
        errors.append("IMAGE_RUNTIME_SPLIT")
    if not config_files or not environment_file or not config_hash:
        errors.append("COMPOSE_PROVENANCE_INCOMPLETE")

    return ServiceTruth(
        container=container,
        service=labels.get("com.docker.compose.service"),
        image_id=str(raw.get("Image") or ""),
        image_revision=image_revision,
        runtime_revision=runtime_revision,
        compose_files=config_files,
        compose_environment_file=environment_file,
        compose_config_hash=config_hash,
        effective_config={key: environment[key] for key in SAFE_CONFIG_KEYS if key in environment},
        parity=not errors,
        errors=tuple(errors),
    )


def verify(repo: Path, containers: list[str], expected_revision: str | None) -> dict:
    head, origin_main = repository_revision(repo)
    expected = expected_revision or head
    services = [inspect_service(name, expected) for name in containers]
    revisions = {
        item.runtime_revision for item in services if item.runtime_revision is not None
    }
    errors = []
    if head != origin_main:
        errors.append("HEAD_ORIGIN_MAIN_MISMATCH")
    if expected != head:
        errors.append("EXPECTED_REVISION_NOT_REPOSITORY_HEAD")
    if len(revisions) > 1:
        errors.append("SERVICE_RUNTIME_REVISIONS_NON_UNIFORM")
    if any(not item.parity for item in services):
        errors.append("SERVICE_PARITY_FAILED")
    return {
        "contract": "WALTRADE_MINIMAL_RELEASE_TRUTH_V1",
        "repository_head": head,
        "origin_main": origin_main,
        "expected_revision": expected,
        "services": [asdict(item) for item in services],
        "pass": not errors,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--expected-revision")
    parser.add_argument("container", nargs="+")
    args = parser.parse_args()
    result = verify(args.repo.resolve(), args.container, args.expected_revision)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

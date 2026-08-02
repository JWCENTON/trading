#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.db import get_db_conn
from common.legacy_fill_equivalence_proof import (
    DockerRuntimeIdentityProbe,
    EXPECTED_DATABASE,
    EXPECTED_DEPLOYMENT,
    EXPECTED_ENVIRONMENT,
    LegacyFillEquivalenceProofService,
    OkxReadOnlyEvidenceClient,
    ProofManifest,
    render_plan,
)

def load_env_file(path: Path) -> None:
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def database_container_ip(name: str) -> str:
    result = subprocess.run(
        [
            "docker", "inspect", "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name,
        ],
        check=True, text=True, capture_output=True,
    )
    value = result.stdout.strip()
    if not value:
        raise RuntimeError("DATABASE_CONTAINER_IP_UNAVAILABLE")
    return value


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Append-only LOCAL LIVE legacy fill equivalence proof V1",
    )
    result.add_argument("--apply", action="store_true")
    result.add_argument("--environment")
    result.add_argument("--deployment-id")
    result.add_argument("--database", required=True)
    result.add_argument("--manifest", required=True)
    result.add_argument("--expected-git-sha", required=True)
    result.add_argument("--env-file", default=str(ROOT / ".env.okx.live"))
    result.add_argument("--db-container", default="trading-live-db-1")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.database != EXPECTED_DATABASE:
        raise RuntimeError("EXPECTED_DATABASE_IDENTITY_MISMATCH")
    if args.apply and (args.environment is None or args.deployment_id is None):
        raise RuntimeError("EXPLICIT_APPLY_IDENTITY_GATES_REQUIRED")
    if args.environment not in (None, EXPECTED_ENVIRONMENT):
        raise RuntimeError("ENVIRONMENT_IDENTITY_MISMATCH")
    if args.deployment_id not in (None, EXPECTED_DEPLOYMENT):
        raise RuntimeError("DEPLOYMENT_IDENTITY_MISMATCH")
    load_env_file(Path(args.env_file).resolve())
    os.environ["DB_HOST"] = database_container_ip(args.db_container)
    os.environ["DB_PORT"] = "5432"
    os.environ["DB_NAME"] = args.database
    manifest_path = Path(args.manifest).resolve()
    manifest = ProofManifest.load(manifest_path)
    runtime = DockerRuntimeIdentityProbe().read(repository=ROOT)
    service = LegacyFillEquivalenceProofService(
        get_db_conn, OkxReadOnlyEvidenceClient(), runtime, manifest,
        expected_git_sha=args.expected_git_sha,
        expected_database=args.database,
    )
    if not args.apply:
        print(render_plan(service.plan()), end="")
        return 0
    result = service.apply(
        apply_requested=True,
        environment=args.environment,
        deployment_id=args.deployment_id,
        database=args.database,
        manifest_path=manifest_path,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

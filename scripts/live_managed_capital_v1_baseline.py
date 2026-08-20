#!/usr/bin/env python3
"""Create or explicitly apply an immutable LIVE managed-capital plan artifact."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from common.db import db_write_conn, read_only_db_conn
from common.exchange_client import OkxMarketDataAdapter
from common.live_managed_capital import (
    activate_live_managed_capital_baseline,
    build_live_baseline_plan,
    canonical_json,
    load_live_managed_capital_evidence,
)


def _args():
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--plan-output", type=Path)
    action.add_argument("--apply-plan", type=Path)
    parser.add_argument("--accepted-at")
    parser.add_argument("--expected-fingerprint")
    parser.add_argument("--approved-by")
    parser.add_argument("--approval-reference-json")
    return parser.parse_args()


def _timestamp(raw: str | None, *, default: datetime) -> datetime:
    if not raw:
        return default
    value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("LIVE_BASELINE_ACCEPTED_AT_MUST_BE_UTC")
    return value.astimezone(timezone.utc)


def _fresh_runtime_context() -> tuple[str, str, str]:
    mode = os.environ.get("TRADING_MODE", "").strip().upper()
    deployment = os.environ.get("DEPLOYMENT_ID", "").strip().lower()
    revision = os.environ.get("GIT_SHA", "").strip()
    if mode != "LIVE" or deployment not in {"local-live", "vps-live"}:
        raise ValueError("LIVE_BASELINE_ENVIRONMENT_FENCE")
    if len(revision) != 40:
        raise ValueError("LIVE_BASELINE_RUNTIME_REVISION_REQUIRED")
    return mode, deployment, revision


def _build_plan(cur, *, plan_created_at: datetime, accepted_at: datetime):
    _mode, deployment, revision = _fresh_runtime_context()
    _evidence, existing, _peak, context = load_live_managed_capital_evidence(
        cur, exchange_client=OkxMarketDataAdapter(), deployment_id=deployment,
        as_of=plan_created_at, fully_closed_marks=True,
    )
    if existing is not None:
        raise ValueError("LIVE_BASELINE_ALREADY_ACCEPTED")
    return build_live_baseline_plan(
        context, deployment_id=deployment, plan_created_at=plan_created_at,
        accepted_at_candidate=accepted_at, runtime_revision=revision,
    )


def _write_artifact(path: Path, artifact: dict) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(canonical_json(artifact) + "\n", encoding="utf-8")
    temporary.replace(destination)


def main():
    args = _args()
    if args.plan_output:
        plan_created_at = datetime.now(timezone.utc)
        accepted_at = _timestamp(args.accepted_at, default=plan_created_at)
        with read_only_db_conn() as conn:
            with conn.cursor() as cur:
                artifact = _build_plan(
                    cur, plan_created_at=plan_created_at, accepted_at=accepted_at,
                )
        _write_artifact(args.plan_output, artifact)
        print(json.dumps({
            "artifact_fingerprint": artifact["artifact_fingerprint"],
            "path": str(args.plan_output.expanduser().resolve()),
            "status": "PLAN_CREATED",
        }, sort_keys=True))
        return

    if not all((args.expected_fingerprint, args.approved_by,
                args.approval_reference_json)):
        raise ValueError("LIVE_BASELINE_EXPLICIT_APPLY_ARGUMENTS_REQUIRED")
    if args.accepted_at:
        raise ValueError("LIVE_BASELINE_APPLY_ACCEPTED_AT_OVERRIDE_FORBIDDEN")
    artifact = json.loads(args.apply_plan.read_text(encoding="utf-8"))
    approval = json.loads(args.approval_reference_json)
    mode, deployment, revision = _fresh_runtime_context()
    identity, _diagnostic = OkxMarketDataAdapter().get_account_identity(refresh=True)
    with db_write_conn() as (conn, cur):
        baseline_id = activate_live_managed_capital_baseline(
            cur, artifact=artifact, expected_fingerprint=args.expected_fingerprint,
            approved_by=args.approved_by, approval_reference=approval,
            fresh_environment=mode, fresh_deployment_id=deployment,
            fresh_account_identity_fingerprint=identity.fingerprint,
            fresh_runtime_revision=revision,
        )
        conn.commit()
    print(json.dumps({"baseline_id": baseline_id, "status": "CREATED"}, sort_keys=True))


if __name__ == "__main__":
    main()

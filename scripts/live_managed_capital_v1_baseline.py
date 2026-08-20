#!/usr/bin/env python3
"""Plan or explicitly apply a LIVE managed-capital baseline.

Default operation is read-only plan.  Apply requires an exact previously
reviewed timestamp/fingerprint and explicit Product Owner approval metadata.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from common.db import db_write_conn, read_only_db_conn
from common.exchange_client import OkxMarketDataAdapter
from common.live_managed_capital import (
    activate_live_managed_capital_baseline,
    build_live_baseline_plan,
    load_live_managed_capital_evidence,
)


def _args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--accepted-at")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-fingerprint")
    parser.add_argument("--approved-by")
    parser.add_argument("--approval-reference-json")
    return parser.parse_args()


def _timestamp(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("LIVE_BASELINE_ACCEPTED_AT_MUST_BE_UTC")
    return value.astimezone(timezone.utc)


def _plan(cur, *, accepted_at):
    mode = os.environ.get("TRADING_MODE", "").strip().upper()
    deployment = os.environ.get("DEPLOYMENT_ID", "").strip().lower()
    revision = os.environ.get("GIT_SHA", "").strip()
    if mode != "LIVE" or deployment not in {"local-live", "vps-live"}:
        raise ValueError("LIVE_BASELINE_ENVIRONMENT_FENCE")
    if len(revision) != 40:
        raise ValueError("LIVE_BASELINE_RUNTIME_REVISION_REQUIRED")
    _evidence, existing, _peak, context = load_live_managed_capital_evidence(
        cur, exchange_client=OkxMarketDataAdapter(),
        deployment_id=deployment, as_of=accepted_at,
    )
    if existing is not None:
        raise ValueError("LIVE_BASELINE_ALREADY_ACCEPTED")
    return build_live_baseline_plan(
        context, deployment_id=deployment, accepted_at=accepted_at,
        runtime_revision=revision,
    )


def main():
    args = _args()
    accepted_at = _timestamp(args.accepted_at)
    if not args.apply:
        with read_only_db_conn() as conn:
            with conn.cursor() as cur:
                plan = _plan(cur, accepted_at=accepted_at)
        print(json.dumps(plan, sort_keys=True, separators=(",", ":")))
        return
    if not all((args.accepted_at, args.expected_fingerprint,
                args.approved_by, args.approval_reference_json)):
        raise ValueError("LIVE_BASELINE_EXPLICIT_APPLY_ARGUMENTS_REQUIRED")
    approval = json.loads(args.approval_reference_json)
    with db_write_conn() as (conn, cur):
        plan = _plan(cur, accepted_at=accepted_at)
        baseline_id = activate_live_managed_capital_baseline(
            cur, plan=plan, expected_fingerprint=args.expected_fingerprint,
            approved_by=args.approved_by, approval_reference=approval,
        )
        conn.commit()
    print(json.dumps({"baseline_id": baseline_id, "status": "CREATED"}, sort_keys=True))


if __name__ == "__main__":
    main()

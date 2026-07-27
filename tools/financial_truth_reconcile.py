from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import sys

import psycopg2

from common.financial_truth_writer import FinancialTruthReconciler


def _connection_factory(expected_environment: str):
    database = os.environ.get("POSTGRES_DB") or os.environ.get("DB_NAME")
    expected_database = f"trading_{expected_environment}"
    if database != expected_database:
        raise RuntimeError(
            f"CONFIGURATION_ERROR: expected {expected_database}, got {database!r}"
        )

    def connect():
        return psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST") or os.environ.get("DB_HOST", "db"),
            port=int(os.environ.get("POSTGRES_PORT") or os.environ.get("DB_PORT", "5432")),
            dbname=database,
            user=os.environ.get("POSTGRES_USER") or os.environ.get("DB_USER"),
            password=os.environ.get("POSTGRES_PASSWORD") or os.environ.get("DB_PASS"),
        )

    return connect


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Bounded canonical Financial Truth reconciliation"
    )
    result.add_argument("--environment", choices=("paper", "live"), required=True)
    result.add_argument("--position-id", type=int, action="append", required=True)
    result.add_argument(
        "--mode", choices=("disabled", "dry-run", "shadow", "apply"), required=True
    )
    result.add_argument("--limit", type=int, default=1)
    result.add_argument("--json", action="store_true")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.limit < 1 or args.limit > 100:
        raise SystemExit("--limit must be between 1 and 100")
    position_ids = tuple(dict.fromkeys(args.position_id))
    if len(position_ids) > args.limit:
        raise SystemExit("explicit position IDs exceed --limit")
    reconciler = FinancialTruthReconciler(_connection_factory(args.environment))
    results = []
    for position_id in position_ids:
        outcome = reconciler.reconcile(
            position_id,
            requested_mode=args.mode,
            environment=args.environment,
            invocation_identity="LOCAL_CLI",
        )
        if outcome.get("calculation") is not None:
            outcome = dict(outcome)
            outcome["calculation"] = asdict(outcome["calculation"])
        results.append(outcome)
    if args.json:
        print(json.dumps(results, default=str, sort_keys=True))
    else:
        for outcome in results:
            print(outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main())

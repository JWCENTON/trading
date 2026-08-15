"""PAPER-only automation for canonical bounded-horizon opportunity labels."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any, Mapping

from common.entry_opportunity_evidence import canonical_runtime_paper_provenance


LOGGER = logging.getLogger(__name__)

TARGET_VERSION = "NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1"
BOUNDED_LIMIT = 500
TELEMETRY_PREFIX = "bounded_horizon_label_producer_v1"


@dataclass(frozen=True)
class BoundedHorizonAutomationResult:
    status: str
    due: bool
    producer_called: bool
    inserted: int
    environment: str | None = None
    deployment_id: str | None = None
    error: str | None = None


def _runtime_identity(
    environ: Mapping[str, str],
) -> tuple[str, str] | None:
    """Return producer provenance for PAPER; hard-disable every LIVE identity."""
    if str(environ.get("TRADING_MODE") or "").strip().upper() != "PAPER":
        return None
    return canonical_runtime_paper_provenance(dict(environ))


def _upsert_kv(cur: Any, key: str, value: str) -> None:
    cur.execute(
        """
        INSERT INTO automation_kv(key,value,updated_at)
        VALUES(%s,%s,now())
        ON CONFLICT(key) DO UPDATE
        SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at
        """,
        (key, value),
    )


def _record_telemetry(conn: Any, values: Mapping[str, str]) -> None:
    with conn.cursor() as cur:
        for suffix, value in values.items():
            _upsert_kv(cur, f"{TELEMETRY_PREFIX}_{suffix}", value)
    conn.commit()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _due(cur: Any, environment: str, deployment_id: str) -> bool:
    """Check deterministic horizon maturity; the producer owns data readiness."""
    cur.execute(
        """
        SELECT EXISTS(
          SELECT 1
          FROM entry_opportunity_evidence_v1 snapshot
          CROSS JOIN (VALUES(15),(30),(60)) horizon(horizon_minutes)
          WHERE snapshot.environment=%s
            AND snapshot.deployment_id=%s
            AND clock_timestamp() >=
                CASE
                  WHEN snapshot.captured_at=date_trunc('minute',snapshot.captured_at)
                    THEN snapshot.captured_at
                  ELSE date_trunc('minute',snapshot.captured_at)+interval '1 minute'
                END + make_interval(mins=>horizon.horizon_minutes)
            AND NOT EXISTS(
              SELECT 1
              FROM entry_opportunity_bounded_horizon_labels_v1 label
              WHERE label.snapshot_id=snapshot.snapshot_id
                AND label.target_version=%s
                AND label.horizon_minutes=horizon.horizon_minutes
            )
          LIMIT 1
        )
        """,
        (environment, deployment_id, TARGET_VERSION),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def run_bounded_horizon_label_automation(
    conn: Any,
    *,
    environ: Mapping[str, str] | None = None,
    bounded_limit: int = BOUNDED_LIMIT,
) -> BoundedHorizonAutomationResult | None:
    """Finalize mature PAPER labels, failing open for trading on every error."""
    source = os.environ if environ is None else environ
    identity = _runtime_identity(source)
    if identity is None:
        return None

    environment, deployment_id = identity
    attempt_at = _utc_now()
    due = False
    producer_called = False

    try:
        with conn.cursor() as cur:
            due = _due(cur, environment, deployment_id)
        if not due:
            stats = {
                "bounded_limit": bounded_limit,
                "deployment_id": deployment_id,
                "due": False,
                "environment": environment,
                "inserted": 0,
                "producer_called": False,
                "target_version": TARGET_VERSION,
            }
            _record_telemetry(
                conn,
                {
                    "last_attempt_at": attempt_at,
                    "last_status": "NOT_DUE",
                    "last_error": "",
                    "last_stats_json": json.dumps(stats, sort_keys=True),
                },
            )
            return BoundedHorizonAutomationResult(
                status="NOT_DUE",
                due=False,
                producer_called=False,
                inserted=0,
                environment=environment,
                deployment_id=deployment_id,
            )

        # Persist the attempt independently so a producer rollback cannot erase it.
        _record_telemetry(conn, {"last_attempt_at": attempt_at})

        with conn.cursor() as cur:
            producer_called = True
            cur.execute(
                "SELECT refresh_entry_opportunity_bounded_horizon_labels_v1("
                "%s,%s,%s)",
                (environment, deployment_id, bounded_limit),
            )
            row = cur.fetchone()
            inserted = int(row[0] if row else 0)

        success_at = _utc_now()
        stats = {
            "bounded_limit": bounded_limit,
            "deployment_id": deployment_id,
            "due": due,
            "environment": environment,
            "inserted": inserted,
            "producer_called": producer_called,
            "target_version": TARGET_VERSION,
        }
        _record_telemetry(
            conn,
            {
                "last_success_at": success_at,
                "last_status": "OK",
                "last_error": "",
                "last_stats_json": json.dumps(stats, sort_keys=True),
            },
        )
        return BoundedHorizonAutomationResult(
            status="OK",
            due=True,
            producer_called=True,
            inserted=inserted,
            environment=environment,
            deployment_id=deployment_id,
        )
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            LOGGER.exception("bounded_horizon_label_automation rollback failed")
        error = f"{type(exc).__name__}: {exc}"[:1000]
        stats = {
            "bounded_limit": bounded_limit,
            "deployment_id": deployment_id,
            "due": due,
            "environment": environment,
            "inserted": 0,
            "producer_called": producer_called,
            "target_version": TARGET_VERSION,
        }
        try:
            _record_telemetry(
                conn,
                {
                    "last_attempt_at": attempt_at,
                    "last_status": "ERROR",
                    "last_error": error,
                    "last_stats_json": json.dumps(stats, sort_keys=True),
                },
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            LOGGER.exception("bounded_horizon_label_automation error telemetry failed")
        LOGGER.exception("bounded_horizon_label_automation producer failed")
        return BoundedHorizonAutomationResult(
            status="ERROR",
            due=due,
            producer_called=producer_called,
            inserted=0,
            environment=environment,
            deployment_id=deployment_id,
            error=error,
        )

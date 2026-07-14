# common/worker_heartbeat.py
import json
import logging
import os
import random
import time
from typing import Any, Optional

from common.db import get_db_conn
from common.runtime import trading_mode_from_env

log = logging.getLogger(__name__)

HEARTBEAT_MAX_RETRIES = 3
HEARTBEAT_RETRYABLE_ERRORS = ("deadlock detected", "could not serialize access")
def current_environment() -> str:
    return trading_mode_from_env()


def record_worker_heartbeat(
    service_name: str,
    *,
    status: str = "healthy",
    error: Optional[Any] = None,
    loop_duration_s: Optional[float] = None,
    meta: Optional[dict[str, Any]] = None,
    conn=None,
    _attempt: int = 1,
) -> None:
    """
    Best-effort heartbeat writer after mandatory trading-mode validation.
    Configuration errors propagate; DB write failures remain fail-open.
    Can reuse an existing connection or open its own short connection.
    """
    environment = current_environment()
    deployment = os.environ.get("ENVIRONMENT") or os.environ.get("APP_ENV")
    payload_meta = dict(meta or {})
    if deployment:
        payload_meta.setdefault("deployment", deployment)

    own_conn = conn is None
    hb_conn = conn
    try:
        if hb_conn is None:
            hb_conn = get_db_conn()
            hb_conn.autocommit = False

        loop_duration_ms = None
        if loop_duration_s is not None:
            loop_duration_ms = max(0, int(float(loop_duration_s) * 1000))

        error_text = None if error is None else str(error)[:2000]
        payload = json.dumps(payload_meta)

        with hb_conn.cursor() as cur:
            # Serialize heartbeat writes to avoid PostgreSQL deadlocks during concurrent
            # INSERT ... ON CONFLICT updates from multiple workers.
            cur.execute("SELECT pg_advisory_xact_lock(917263002)")

            cur.execute(
                """
                INSERT INTO worker_heartbeats (
                  service_name, environment, status, last_tick, last_ok,
                  last_error, loop_duration_ms, meta, updated_at
                )
                VALUES (
                  %s, %s, %s, now(),
                  CASE WHEN %s IS NULL THEN now() ELSE NULL END,
                  %s, %s, %s::jsonb, now()
                )
                ON CONFLICT (service_name, environment) DO UPDATE SET
                  status = EXCLUDED.status,
                  last_tick = EXCLUDED.last_tick,
                  last_ok = CASE
                    WHEN EXCLUDED.last_error IS NULL THEN EXCLUDED.last_tick
                    ELSE worker_heartbeats.last_ok
                  END,
                  last_error = EXCLUDED.last_error,
                  loop_duration_ms = EXCLUDED.loop_duration_ms,
                  meta = EXCLUDED.meta,
                  updated_at = now();
                """,
                (service_name, environment, status, error_text, error_text, loop_duration_ms, payload),
            )
        if own_conn:
            hb_conn.commit()
    except Exception as exc:
        try:
            if hb_conn is not None and not getattr(hb_conn, "closed", True):
                hb_conn.rollback()
        except Exception:
            pass

        msg = str(exc).lower()
        retryable = any(token in msg for token in HEARTBEAT_RETRYABLE_ERRORS)

        if retryable and _attempt < HEARTBEAT_MAX_RETRIES:
            sleep_s = round((0.05 * _attempt) + random.uniform(0.01, 0.08), 3)
            log.warning(
                "worker heartbeat retry for %s attempt=%s sleep=%.3fs error=%s",
                service_name,
                _attempt,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)
            try:
                if own_conn and hb_conn is not None and not getattr(hb_conn, "closed", True):
                    hb_conn.close()
            except Exception:
                pass

            return record_worker_heartbeat(
                service_name,
                status=status,
                error=error,
                loop_duration_s=loop_duration_s,
                meta=meta,
                conn=None if own_conn else conn,
                _attempt=_attempt + 1,
            )

        log.warning("worker heartbeat write failed for %s: %s", service_name, exc)
    finally:
        try:
            if own_conn and hb_conn is not None and not getattr(hb_conn, "closed", True):
                hb_conn.close()
        except Exception:
            pass

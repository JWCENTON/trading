# common/worker_heartbeat.py
import json
import logging
import os
from typing import Any, Optional

from common.db import get_db_conn

log = logging.getLogger(__name__)


def current_environment() -> str:
    return (
        os.environ.get("ENVIRONMENT")
        or os.environ.get("TRADING_MODE")
        or os.environ.get("APP_ENV")
        or "UNKNOWN"
    ).upper()


def ensure_worker_heartbeats_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS worker_heartbeats (
          service_name TEXT NOT NULL,
          environment TEXT NOT NULL DEFAULT 'UNKNOWN',
          status TEXT NOT NULL DEFAULT 'unknown',
          last_tick TIMESTAMPTZ NOT NULL DEFAULT now(),
          last_ok TIMESTAMPTZ,
          last_error TEXT,
          loop_duration_ms INTEGER,
          meta JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (service_name, environment)
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_status_updated
          ON worker_heartbeats(status, updated_at DESC);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_worker_heartbeats_last_tick
          ON worker_heartbeats(last_tick DESC);
        """
    )


def record_worker_heartbeat(
    service_name: str,
    *,
    status: str = "healthy",
    error: Optional[Any] = None,
    loop_duration_s: Optional[float] = None,
    meta: Optional[dict[str, Any]] = None,
    conn=None,
) -> None:
    """
    Best-effort heartbeat writer. Never raises to the caller.
    Can reuse an existing connection or open its own short connection.
    """
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
        environment = current_environment()
        payload = json.dumps(meta or {})

        with hb_conn.cursor() as cur:
            ensure_worker_heartbeats_table(cur)
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
            if own_conn and hb_conn is not None and not getattr(hb_conn, "closed", True):
                hb_conn.rollback()
        except Exception:
            pass
        log.warning("worker heartbeat write failed for %s: %s", service_name, exc)
    finally:
        try:
            if own_conn and hb_conn is not None and not getattr(hb_conn, "closed", True):
                hb_conn.close()
        except Exception:
            pass

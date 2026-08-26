"""PAPER-only producer for immutable full-opportunity path outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import os
from typing import Any, Mapping

from common.paper_opportunity_observation import FEATURE_FLAG


LOGGER = logging.getLogger(__name__)


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class PaperOpportunityOutcomeResult:
    status: str
    inserted: int
    deployment_id: str | None = None
    error: str | None = None


def _upsert(cur: Any, key: str, value: str) -> None:
    cur.execute(
        """INSERT INTO automation_kv(key,value,updated_at) VALUES(%s,%s,now())
           ON CONFLICT(key) DO UPDATE
           SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at""",
        (key, value),
    )


def run_paper_opportunity_outcome_automation(
    conn: Any,
    *,
    environ: Mapping[str, str] | None = None,
    bounded_limit: int = 500,
) -> PaperOpportunityOutcomeResult | None:
    values = os.environ if environ is None else environ
    if not _enabled(values.get(FEATURE_FLAG, "0")):
        return None
    if str(values.get("TRADING_MODE") or "").upper() != "PAPER":
        return None
    deployment = str(values.get("DEPLOYMENT_ID") or "").lower()
    if deployment not in {"local-paper", "vps-paper"}:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT refresh_paper_opportunity_outcomes_v1(%s,%s)",
                (deployment, bounded_limit),
            )
            row = cur.fetchone()
            inserted = int(row[0] if row else 0)
            _upsert(cur, "full_paper_opportunity_observation_v1_last_status", "OK")
            _upsert(
                cur,
                "full_paper_opportunity_observation_v1_last_success_at",
                datetime.now(timezone.utc).isoformat(),
            )
            _upsert(
                cur,
                "full_paper_opportunity_observation_v1_last_inserted",
                str(inserted),
            )
        conn.commit()
        return PaperOpportunityOutcomeResult("OK", inserted, deployment)
    except Exception as exc:
        conn.rollback()
        error = f"{type(exc).__name__}: {exc}"[:1000]
        try:
            with conn.cursor() as cur:
                _upsert(
                    cur,
                    "full_paper_opportunity_observation_v1_last_status",
                    "ERROR",
                )
            conn.commit()
        except Exception:
            conn.rollback()
        LOGGER.exception("full_paper_opportunity_outcome_v1 producer failed")
        return PaperOpportunityOutcomeResult("ERROR", 0, deployment, error)

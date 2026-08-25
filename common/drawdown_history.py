"""Environment-neutral immutable drawdown history mathematics."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping


ZERO = Decimal("0")
HUNDRED = Decimal("100")


def decimal_value(value: object) -> Decimal:
    """Require exact financial input; binary floats are never canonical."""
    if value is None or isinstance(value, float):
        raise ValueError("DRAWDOWN_DECIMAL_REQUIRED")
    return Decimal(str(value))


def decimal_text(value: object) -> str:
    exact = decimal_value(value)
    return "0" if exact == ZERO else format(exact.normalize(), "f")


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("DRAWDOWN_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, timedelta):
        return decimal_text(Decimal(str(value.total_seconds())))
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, float):
        raise ValueError("DRAWDOWN_DECIMAL_REQUIRED")
    return value


def canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        _json_value(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True,
    )


def fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def cadence_bucket(at: datetime, cadence: timedelta) -> datetime:
    if at.tzinfo is None or cadence.total_seconds() <= 0:
        raise ValueError("DRAWDOWN_TIMESTAMP_REQUIRED")
    utc = at.astimezone(timezone.utc)
    seconds = int(utc.timestamp())
    width = int(cadence.total_seconds())
    return datetime.fromtimestamp(seconds - seconds % width, tz=timezone.utc)


@dataclass(frozen=True)
class DrawdownObservation:
    observed_at: datetime
    observation_bucket_at: datetime
    observation_trigger: str
    managed_equity: Decimal
    flow_adjusted_equity: Decimal
    history_status: str = "CANONICAL"


@dataclass(frozen=True)
class DrawdownHistory:
    current_managed_equity: Decimal | None
    current_flow_adjusted_equity: Decimal | None
    peak_flow_adjusted_equity: Decimal | None
    current_drawdown_abs: Decimal | None
    current_drawdown_pct: Decimal | None
    max_drawdown_abs: Decimal | None
    max_drawdown_pct: Decimal | None
    recovery_status: str
    peak_timestamp: datetime | None
    drawdown_start: datetime | None
    recovery_timestamp: datetime | None
    drawdown_duration: timedelta | None
    history_status: str
    latest_observation_at: datetime | None


def _empty(
    status: str, *, latest: datetime | None = None,
    peak: Decimal | None = None, peak_at: datetime | None = None,
) -> DrawdownHistory:
    return DrawdownHistory(
        None, None, peak, None, None, None, None, "NO_HISTORY",
        peak_at, None, None, None, status, latest,
    )


def calculate_drawdown_history(
    *, baseline_managed_equity: Decimal | None,
    baseline_at: datetime | None,
    observations: Iterable[DrawdownObservation],
    as_of: datetime,
    cadence: timedelta,
    stale_after: timedelta,
    failure_priority: Iterable[str],
    cadence_anchor_at: datetime | None = None,
    timestamp_error: str = "DRAWDOWN_AS_OF_REQUIRED",
    decimal_error: str = "DRAWDOWN_DECIMAL_REQUIRED",
) -> DrawdownHistory:
    """Calculate signed drawdown and recovery from canonical timestamps only."""
    if as_of.tzinfo is None:
        raise ValueError(timestamp_error)
    if baseline_managed_equity is None or baseline_at is None:
        return _empty("NO_BASELINE")
    if baseline_at.tzinfo is None:
        raise ValueError(timestamp_error)
    def exact(value: object) -> Decimal:
        try:
            return decimal_value(value)
        except ValueError as exc:
            if str(exc) == "DRAWDOWN_DECIMAL_REQUIRED":
                raise ValueError(decimal_error) from exc
            raise

    baseline = exact(baseline_managed_equity)
    rows = sorted(
        tuple(observations),
        key=lambda row: (row.observed_at, row.observation_bucket_at),
    )
    for row in rows:
        if row.observed_at.tzinfo is None or row.observation_bucket_at.tzinfo is None:
            raise ValueError(timestamp_error)
    for failure in failure_priority:
        if any(row.history_status == failure for row in rows):
            return _empty(
                failure, latest=max((row.observed_at for row in rows), default=None)
            )
    canonical = [row for row in rows if row.history_status == "CANONICAL"]
    if not canonical:
        return _empty("NO_HISTORY", peak=baseline, peak_at=baseline_at)
    cadence_rows = sorted(
        (row for row in canonical if row.observation_trigger == "CADENCE_15M"),
        key=lambda row: row.observation_bucket_at,
    )
    gap = any(
        current.observation_bucket_at - previous.observation_bucket_at > cadence
        for previous, current in zip(cadence_rows, cadence_rows[1:])
    )
    if cadence_anchor_at is not None:
        if cadence_anchor_at.tzinfo is None:
            raise ValueError(timestamp_error)
        anchor = cadence_bucket(cadence_anchor_at, cadence)
        first_required = anchor + cadence
        latest_required = cadence_bucket(as_of, cadence)
        if latest_required >= first_required:
            if not cadence_rows or cadence_rows[0].observation_bucket_at > first_required:
                gap = True
            elif cadence_rows[-1].observation_bucket_at < latest_required:
                gap = True
    if gap:
        return _empty("OBSERVATION_GAP", latest=canonical[-1].observed_at)
    if as_of - canonical[-1].observed_at > stale_after:
        return _empty("STALE_HISTORY", latest=canonical[-1].observed_at)

    peak = baseline
    peak_at = baseline_at
    max_abs = ZERO
    max_pct: Decimal | None = ZERO if peak != ZERO else None
    episode_peak = peak
    episode_peak_at = peak_at
    active_start: datetime | None = None
    last_episode: tuple[datetime, datetime, datetime, timedelta] | None = None

    for row in canonical:
        adjusted = exact(row.flow_adjusted_equity)
        if adjusted > peak:
            peak = adjusted
            peak_at = row.observed_at
        drawdown_abs = adjusted - peak
        if drawdown_abs < max_abs:
            max_abs = drawdown_abs
        if peak != ZERO:
            drawdown_pct = drawdown_abs / peak * HUNDRED
            if max_pct is None or drawdown_pct < max_pct:
                max_pct = drawdown_pct
        elif drawdown_abs != ZERO:
            max_pct = None

        if active_start is None and adjusted < episode_peak:
            active_start = row.observed_at
        elif active_start is not None and adjusted >= episode_peak:
            recovered_at = row.observed_at
            last_episode = (
                episode_peak_at, active_start, recovered_at,
                recovered_at - active_start,
            )
            active_start = None
            episode_peak = adjusted
            episode_peak_at = row.observed_at
        elif active_start is None and adjusted >= episode_peak:
            episode_peak = adjusted
            episode_peak_at = row.observed_at

    current = canonical[-1]
    current_adjusted = exact(current.flow_adjusted_equity)
    current_abs = current_adjusted - peak
    current_pct = None if peak == ZERO else current_abs / peak * HUNDRED
    history_status = "ZERO_PEAK_PERCENT_UNAVAILABLE" if peak == ZERO else "CANONICAL"
    if active_start is not None:
        recovery_status = "IN_DRAWDOWN"
        drawdown_start = active_start
        recovery_at = None
        duration = current.observed_at - active_start
        applicable_peak_at = episode_peak_at
    elif last_episode is not None:
        recovery_status = "RECOVERED"
        applicable_peak_at, drawdown_start, recovery_at, duration = last_episode
    else:
        recovery_status = "NO_DRAWDOWN"
        applicable_peak_at = peak_at
        drawdown_start = recovery_at = duration = None
    return DrawdownHistory(
        exact(current.managed_equity), current_adjusted, peak,
        current_abs, current_pct, max_abs, max_pct, recovery_status,
        applicable_peak_at, drawdown_start, recovery_at, duration,
        history_status, current.observed_at,
    )

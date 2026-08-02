from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Protocol, Sequence, TypeVar


class FreshnessState(str, Enum):
    READY = "READY"
    CATCHING_UP = "CATCHING_UP"
    STALLED = "STALLED"
    UNKNOWN = "UNKNOWN"


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def interval_duration(value: str) -> timedelta:
    raw = str(value).strip().lower()
    if len(raw) < 2 or not raw[:-1].isdigit():
        raise ValueError(f"UNSUPPORTED_CANDLE_INTERVAL:{value}")
    amount = int(raw[:-1])
    if amount <= 0:
        raise ValueError(f"UNSUPPORTED_CANDLE_INTERVAL:{value}")
    unit = raw[-1]
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    raise ValueError(f"UNSUPPORTED_CANDLE_INTERVAL:{value}")


@dataclass(frozen=True)
class CheckpointIdentity:
    environment: str
    deployment_id: str
    symbol: str
    interval: str
    strategy: str = "SUPERTREND"

    def __post_init__(self):
        environment = str(self.environment).strip().lower()
        deployment_id = str(self.deployment_id).strip()
        symbol = str(self.symbol).strip().upper()
        interval = str(self.interval).strip().lower()
        strategy = str(self.strategy).strip().upper()
        if environment not in {"paper", "live"}:
            raise ValueError(f"INVALID_CHECKPOINT_ENVIRONMENT:{environment}")
        if not deployment_id:
            raise ValueError("MISSING_CHECKPOINT_DEPLOYMENT")
        if not symbol:
            raise ValueError("MISSING_CHECKPOINT_SYMBOL")
        if strategy != "SUPERTREND":
            raise ValueError(f"INVALID_CHECKPOINT_STRATEGY:{strategy}")
        interval_duration(interval)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "deployment_id", deployment_id)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "strategy", strategy)


@dataclass(frozen=True)
class CheckpointSnapshot:
    identity: CheckpointIdentity
    last_processed_candle_open_time: datetime
    updated_at: datetime
    state: FreshnessState
    latest_closed_candle_open_time: datetime | None
    backlog_size: int
    reason: str
    resume_source: str


@dataclass(frozen=True)
class FreshnessAssessment:
    state: FreshnessState
    checkpoint_before: datetime | None
    latest_closed_candle_open_time: datetime | None
    backlog_size: int
    reason: str
    resume_source: str


@dataclass(frozen=True)
class CandleProcessingContext:
    state: FreshnessState
    checkpoint_before: datetime | None
    latest_closed_candle_open_time: datetime
    backlog_size: int
    reason: str
    resume_source: str


@dataclass(frozen=True)
class ResumeResult:
    assessment: FreshnessAssessment
    processed_open_times: tuple[datetime, ...]
    checkpoint_after: datetime | None


def assess_freshness(
    *,
    interval: str,
    checkpoint_before: datetime | None,
    latest_closed_candle_open_time: datetime | None,
    candidate_open_times: Sequence[datetime],
) -> FreshnessAssessment:
    checkpoint = _utc(checkpoint_before)
    latest = _utc(latest_closed_candle_open_time)
    candidates = tuple(_utc(value) for value in candidate_open_times)
    resume_source = "PERSISTED_CHECKPOINT" if checkpoint is not None else "BOOTSTRAP_LATEST_CLOSED"

    if latest is None:
        return FreshnessAssessment(
            FreshnessState.STALLED, checkpoint, None, 0,
            "LATEST_CLOSED_CANDLE_UNAVAILABLE", resume_source,
        )
    if any(value is None for value in candidates):
        return FreshnessAssessment(
            FreshnessState.STALLED, checkpoint, latest, 0,
            "CANDLE_EVIDENCE_INCOMPLETE", resume_source,
        )
    if tuple(sorted(candidates)) != candidates or len(set(candidates)) != len(candidates):
        return FreshnessAssessment(
            FreshnessState.STALLED, checkpoint, latest, len(candidates),
            "CANDLE_SEQUENCE_AMBIGUOUS", resume_source,
        )
    if checkpoint is None:
        if candidates == (latest,):
            return FreshnessAssessment(
                FreshnessState.CATCHING_UP, None, latest, 1,
                "CHECKPOINT_BOOTSTRAP_REQUIRED", resume_source,
            )
        return FreshnessAssessment(
            FreshnessState.STALLED, None, latest, len(candidates),
            "BOOTSTRAP_CANDLE_EVIDENCE_MISMATCH", resume_source,
        )
    if checkpoint > latest:
        return FreshnessAssessment(
            FreshnessState.STALLED, checkpoint, latest, 0,
            "CHECKPOINT_AHEAD_OF_LATEST_CLOSED", resume_source,
        )
    if checkpoint == latest:
        if candidates:
            return FreshnessAssessment(
                FreshnessState.STALLED, checkpoint, latest, len(candidates),
                "PROCESSED_CANDLE_REAPPEARED", resume_source,
            )
        return FreshnessAssessment(
            FreshnessState.READY, checkpoint, latest, 0,
            "CHECKPOINT_MATCHES_LATEST_CLOSED", resume_source,
        )

    step = interval_duration(interval)
    distance = latest - checkpoint
    expected_count, remainder = divmod(distance, step)
    sequence_matches = (
        remainder == timedelta(0)
        and expected_count > 0
        and len(candidates) == expected_count
        and all(
            candidate == checkpoint + step * index
            for index, candidate in enumerate(candidates, start=1)
        )
    )
    if not sequence_matches:
        return FreshnessAssessment(
            FreshnessState.STALLED, checkpoint, latest,
            max(0, int(expected_count)),
            "CANDLE_SEQUENCE_GAP_OR_DRIFT", resume_source,
        )
    return FreshnessAssessment(
        FreshnessState.CATCHING_UP, checkpoint, latest, int(expected_count),
        "CANDLE_BACKLOG_PRESENT", resume_source,
    )


class CheckpointStore(Protocol):
    def load(self) -> CheckpointSnapshot | None: ...
    def observe(self, assessment: FreshnessAssessment) -> None: ...
    def advance(
        self, *, expected_before: datetime | None, processed_open_time: datetime,
        latest_closed_candle_open_time: datetime, backlog_size: int,
        resume_source: str,
    ) -> CheckpointSnapshot: ...
    def mark_stalled(
        self, *, expected_before: datetime | None,
        latest_closed_candle_open_time: datetime | None,
        backlog_size: int, reason: str, resume_source: str,
    ) -> None: ...


WorkItem = TypeVar("WorkItem")


def process_resume_workset(
    *,
    store: CheckpointStore,
    interval: str,
    latest_closed_candle_open_time: datetime | None,
    work_items: Sequence[WorkItem],
    open_time_of: Callable[[WorkItem], datetime],
    processor: Callable[[WorkItem, CandleProcessingContext], None],
) -> ResumeResult:
    snapshot = store.load()
    checkpoint = (
        snapshot.last_processed_candle_open_time if snapshot is not None else None
    )
    open_times = tuple(_utc(open_time_of(item)) for item in work_items)
    assessment = assess_freshness(
        interval=interval,
        checkpoint_before=checkpoint,
        latest_closed_candle_open_time=latest_closed_candle_open_time,
        candidate_open_times=open_times,
    )
    store.observe(assessment)
    if assessment.state in {FreshnessState.READY, FreshnessState.STALLED}:
        return ResumeResult(assessment, (), checkpoint)

    processed: list[datetime] = []
    current_checkpoint = checkpoint
    for index, item in enumerate(work_items):
        open_time = _utc(open_time_of(item))
        remaining = len(work_items) - index - 1
        # Older backlog is entry-blocked. The final contiguous latest candle is
        # entry-ready, while its durable checkpoint is still written only after
        # the complete processor returns successfully.
        entry_state = (
            FreshnessState.READY
            if remaining == 0 and open_time == assessment.latest_closed_candle_open_time
            else FreshnessState.CATCHING_UP
        )
        context = CandleProcessingContext(
            state=entry_state,
            checkpoint_before=current_checkpoint,
            latest_closed_candle_open_time=assessment.latest_closed_candle_open_time,
            backlog_size=remaining + 1,
            reason=(
                "LATEST_CLOSED_CANDLE_CONTIGUOUS"
                if entry_state is FreshnessState.READY
                else "CANDLE_BACKLOG_PRESENT"
            ),
            resume_source=assessment.resume_source,
        )
        try:
            processor(item, context)
            stored = store.advance(
                expected_before=current_checkpoint,
                processed_open_time=open_time,
                latest_closed_candle_open_time=assessment.latest_closed_candle_open_time,
                backlog_size=remaining,
                resume_source=assessment.resume_source,
            )
        except Exception as primary:
            try:
                store.mark_stalled(
                    expected_before=current_checkpoint,
                    latest_closed_candle_open_time=assessment.latest_closed_candle_open_time,
                    backlog_size=remaining + 1,
                    reason="CANDLE_PROCESSING_DID_NOT_COMPLETE",
                    resume_source=assessment.resume_source,
                )
            except Exception as stalled_error:
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note(f"checkpoint STALLED write failed: {stalled_error!r}")
            raise
        current_checkpoint = stored.last_processed_candle_open_time
        processed.append(open_time)
    return ResumeResult(assessment, tuple(processed), current_checkpoint)


class PostgresCheckpointStore:
    def __init__(self, connection_factory, identity: CheckpointIdentity):
        self.connection_factory = connection_factory
        self.identity = identity

    @property
    def _identity_params(self) -> tuple[str, str, str, str, str]:
        item = self.identity
        return (
            item.environment, item.deployment_id, item.symbol,
            item.interval, item.strategy,
        )

    @staticmethod
    def _snapshot(identity: CheckpointIdentity, row) -> CheckpointSnapshot | None:
        if row is None:
            return None
        return CheckpointSnapshot(
            identity=identity,
            last_processed_candle_open_time=_utc(row[0]),
            updated_at=_utc(row[1]),
            state=FreshnessState(str(row[2])),
            latest_closed_candle_open_time=_utc(row[3]),
            backlog_size=int(row[4]),
            reason=str(row[5]),
            resume_source=str(row[6]),
        )

    def _select(self, cur, *, for_update: bool = False):
        cur.execute(
            """
            SELECT last_processed_candle_open_time,updated_at,state,
                   latest_closed_candle_open_time,backlog_size,reason,resume_source
            FROM public.supertrend_candle_checkpoint_v1
            WHERE environment=%s AND deployment_id=%s AND symbol=%s
              AND "interval"=%s AND strategy=%s
            """ + (" FOR UPDATE" if for_update else ""),
            self._identity_params,
        )
        return self._snapshot(self.identity, cur.fetchone())

    def load(self) -> CheckpointSnapshot | None:
        conn = self.connection_factory()
        try:
            with conn.cursor() as cur:
                return self._select(cur)
        finally:
            try:
                conn.rollback()
            finally:
                conn.close()

    def _event(
        self, cur, *, event_type: str, before: datetime | None,
        after: datetime | None, latest: datetime | None,
        state: FreshnessState, backlog_size: int, reason: str,
        resume_source: str,
    ) -> None:
        cur.execute(
            """
            INSERT INTO public.supertrend_candle_checkpoint_event_v1(
              environment,deployment_id,symbol,"interval",strategy,event_type,
              checkpoint_before,checkpoint_after,latest_closed_candle_open_time,
              state,backlog_size,reason,resume_source
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (*self._identity_params, event_type, before, after, latest,
             state.value, int(backlog_size), reason, resume_source),
        )

    @staticmethod
    def _assert_expected(actual: CheckpointSnapshot | None, expected: datetime | None):
        actual_time = (
            actual.last_processed_candle_open_time if actual is not None else None
        )
        if _utc(actual_time) != _utc(expected):
            raise RuntimeError(
                f"SUPERTREND_CHECKPOINT_CONCURRENT_CHANGE:{actual_time}:{expected}"
            )

    def observe(self, assessment: FreshnessAssessment) -> None:
        conn = self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    current = self._select(cur, for_update=True)
                    self._assert_expected(current, assessment.checkpoint_before)
                    if current is not None:
                        cur.execute(
                            """
                            UPDATE public.supertrend_candle_checkpoint_v1
                            SET updated_at=clock_timestamp(),state=%s,
                                latest_closed_candle_open_time=%s,backlog_size=%s,
                                reason=%s,resume_source=%s
                            WHERE environment=%s AND deployment_id=%s AND symbol=%s
                              AND "interval"=%s AND strategy=%s
                            """,
                            (
                                assessment.state.value,
                                assessment.latest_closed_candle_open_time,
                                assessment.backlog_size, assessment.reason,
                                assessment.resume_source, *self._identity_params,
                            ),
                        )
                    self._event(
                        cur, event_type="OBSERVED",
                        before=assessment.checkpoint_before,
                        after=assessment.checkpoint_before,
                        latest=assessment.latest_closed_candle_open_time,
                        state=assessment.state,
                        backlog_size=assessment.backlog_size,
                        reason=assessment.reason,
                        resume_source=assessment.resume_source,
                    )
        finally:
            conn.close()

    def advance(
        self, *, expected_before: datetime | None, processed_open_time: datetime,
        latest_closed_candle_open_time: datetime, backlog_size: int,
        resume_source: str,
    ) -> CheckpointSnapshot:
        processed = _utc(processed_open_time)
        expected = _utc(expected_before)
        latest = _utc(latest_closed_candle_open_time)
        if expected is not None and processed <= expected:
            raise RuntimeError("SUPERTREND_CHECKPOINT_NON_MONOTONIC_ADVANCE")
        if processed > latest:
            raise RuntimeError("SUPERTREND_CHECKPOINT_AFTER_LATEST_CLOSED")
        state = FreshnessState.READY if int(backlog_size) == 0 else FreshnessState.CATCHING_UP
        reason = "CHECKPOINT_CAUGHT_UP" if state is FreshnessState.READY else "CHECKPOINT_ADVANCED"
        conn = self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    current = self._select(cur, for_update=True)
                    self._assert_expected(current, expected)
                    if current is None:
                        cur.execute(
                            """
                            INSERT INTO public.supertrend_candle_checkpoint_v1(
                              environment,deployment_id,symbol,"interval",strategy,
                              last_processed_candle_open_time,state,
                              latest_closed_candle_open_time,backlog_size,reason,resume_source
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (*self._identity_params, processed, state.value, latest,
                             int(backlog_size), reason, resume_source),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE public.supertrend_candle_checkpoint_v1
                            SET last_processed_candle_open_time=%s,
                                updated_at=clock_timestamp(),state=%s,
                                latest_closed_candle_open_time=%s,backlog_size=%s,
                                reason=%s,resume_source=%s
                            WHERE environment=%s AND deployment_id=%s AND symbol=%s
                              AND "interval"=%s AND strategy=%s
                            """,
                            (processed, state.value, latest, int(backlog_size),
                             reason, resume_source, *self._identity_params),
                        )
                    self._event(
                        cur, event_type="ADVANCED", before=expected, after=processed,
                        latest=latest, state=state, backlog_size=int(backlog_size),
                        reason=reason, resume_source=resume_source,
                    )
                    stored = self._select(cur)
                    if stored is None:
                        raise RuntimeError("SUPERTREND_CHECKPOINT_WRITE_MISSING")
                    return stored
        finally:
            conn.close()

    def mark_stalled(
        self, *, expected_before: datetime | None,
        latest_closed_candle_open_time: datetime | None,
        backlog_size: int, reason: str, resume_source: str,
    ) -> None:
        expected = _utc(expected_before)
        latest = _utc(latest_closed_candle_open_time)
        conn = self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    current = self._select(cur, for_update=True)
                    self._assert_expected(current, expected)
                    if current is not None:
                        cur.execute(
                            """
                            UPDATE public.supertrend_candle_checkpoint_v1
                            SET updated_at=clock_timestamp(),state='STALLED',
                                latest_closed_candle_open_time=%s,backlog_size=%s,
                                reason=%s,resume_source=%s
                            WHERE environment=%s AND deployment_id=%s AND symbol=%s
                              AND "interval"=%s AND strategy=%s
                            """,
                            (latest, int(backlog_size), reason, resume_source,
                             *self._identity_params),
                        )
                    self._event(
                        cur, event_type="STALLED", before=expected, after=expected,
                        latest=latest, state=FreshnessState.STALLED,
                        backlog_size=int(backlog_size), reason=reason,
                        resume_source=resume_source,
                    )
        finally:
            conn.close()

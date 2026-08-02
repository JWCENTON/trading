from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from common.supertrend_candle_checkpoint import (
    CheckpointIdentity,
    CheckpointSnapshot,
    FreshnessState,
    process_resume_workset,
)
from tests.bot_supertrend.fixtures import RAW_EXECUTION_SCENARIOS, candle


T0 = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
IDENTITY = CheckpointIdentity("paper", "local-paper", "BTCUSDC", "1m")


class MemoryStore:
    def __init__(self, checkpoint=None):
        self.snapshot = None
        if checkpoint is not None:
            self.snapshot = CheckpointSnapshot(
                IDENTITY, checkpoint, T0, FreshnessState.READY, checkpoint, 0,
                "TEST_SEED", "PERSISTED_CHECKPOINT",
            )
        self.events = []

    def load(self):
        return self.snapshot

    def observe(self, assessment):
        self.events.append(("OBSERVED", assessment))
        if self.snapshot is not None:
            self.snapshot = replace(
                self.snapshot,
                state=assessment.state,
                latest_closed_candle_open_time=assessment.latest_closed_candle_open_time,
                backlog_size=assessment.backlog_size,
                reason=assessment.reason,
                resume_source=assessment.resume_source,
            )

    def advance(
        self, *, expected_before, processed_open_time,
        latest_closed_candle_open_time, backlog_size, resume_source,
    ):
        actual = (
            self.snapshot.last_processed_candle_open_time
            if self.snapshot is not None else None
        )
        assert actual == expected_before
        state = FreshnessState.READY if backlog_size == 0 else FreshnessState.CATCHING_UP
        self.snapshot = CheckpointSnapshot(
            IDENTITY, processed_open_time, T0, state,
            latest_closed_candle_open_time, backlog_size,
            "CHECKPOINT_CAUGHT_UP" if backlog_size == 0 else "CHECKPOINT_ADVANCED",
            resume_source,
        )
        self.events.append(("ADVANCED", expected_before, processed_open_time))
        return self.snapshot

    def mark_stalled(
        self, *, expected_before, latest_closed_candle_open_time,
        backlog_size, reason, resume_source,
    ):
        actual = (
            self.snapshot.last_processed_candle_open_time
            if self.snapshot is not None else None
        )
        assert actual == expected_before
        if self.snapshot is not None:
            self.snapshot = replace(
                self.snapshot, state=FreshnessState.STALLED,
                latest_closed_candle_open_time=latest_closed_candle_open_time,
                backlog_size=backlog_size, reason=reason,
                resume_source=resume_source,
            )
        self.events.append(("STALLED", expected_before, reason))


def _run(store, *, latest, work, processor):
    return process_resume_workset(
        store=store,
        interval="1m",
        latest_closed_candle_open_time=latest,
        work_items=work,
        open_time_of=lambda value: value,
        processor=processor,
    )


def test_checkpoint_created_only_after_success_and_duplicate_is_skipped():
    store = MemoryStore()
    side_effects = []
    result = _run(
        store, latest=T0, work=[T0],
        processor=lambda item, context: side_effects.append((item, context.state)),
    )
    assert result.checkpoint_after == T0
    assert store.snapshot.last_processed_candle_open_time == T0
    assert store.snapshot.state is FreshnessState.READY
    assert side_effects == [(T0, FreshnessState.READY)]

    restarted_effects = []
    restarted = _run(
        store, latest=T0, work=[],
        processor=lambda item, context: restarted_effects.append(item),
    )
    assert restarted.assessment.state is FreshnessState.READY
    assert restarted_effects == []
    assert [event[0] for event in store.events].count("ADVANCED") == 1


def test_processing_error_does_not_advance_and_marks_stalled():
    store = MemoryStore(T0)
    t1 = T0 + timedelta(minutes=1)

    def fail(_item, _context):
        raise RuntimeError("strategy failed")

    with pytest.raises(RuntimeError, match="strategy failed"):
        _run(store, latest=t1, work=[t1], processor=fail)
    assert store.snapshot.last_processed_candle_open_time == T0
    assert store.snapshot.state is FreshnessState.STALLED
    assert store.events[-1][0] == "STALLED"


def test_restart_resumes_next_candle_and_catchup_transitions_to_ready():
    t1 = T0 + timedelta(minutes=1)
    t2 = T0 + timedelta(minutes=2)
    store = MemoryStore(T0)
    contexts = []

    def fail_second(item, context):
        contexts.append((item, context.state))
        if item == t2:
            raise RuntimeError("stop at second")

    with pytest.raises(RuntimeError, match="stop at second"):
        _run(store, latest=t2, work=[t1, t2], processor=fail_second)
    assert contexts == [
        (t1, FreshnessState.CATCHING_UP),
        (t2, FreshnessState.READY),
    ]
    assert store.snapshot.last_processed_candle_open_time == t1
    assert store.snapshot.state is FreshnessState.STALLED

    resumed = []
    result = _run(
        store, latest=t2, work=[t2],
        processor=lambda item, context: resumed.append((item, context.state)),
    )
    assert resumed == [(t2, FreshnessState.READY)]
    assert result.checkpoint_after == t2
    assert store.snapshot.state is FreshnessState.READY


def test_missing_candle_evidence_is_stalled_without_processing():
    t2 = T0 + timedelta(minutes=2)
    store = MemoryStore(T0)
    processed = []
    result = _run(
        store, latest=t2, work=[t2],
        processor=lambda item, context: processed.append(item),
    )
    assert result.assessment.state is FreshnessState.STALLED
    assert result.assessment.reason == "CANDLE_SEQUENCE_GAP_OR_DRIFT"
    assert processed == []
    assert store.snapshot.last_processed_candle_open_time == T0
    assert store.snapshot.state is FreshnessState.STALLED


def _freshness(module, state):
    return module.CandleProcessingContext(
        state=state,
        checkpoint_before=T0,
        latest_closed_candle_open_time=T0 + timedelta(minutes=1),
        backlog_size=2,
        reason=f"TEST_{state.value}",
        resume_source="TEST",
    )


@pytest.mark.parametrize(
    "state",
    [FreshnessState.CATCHING_UP, FreshnessState.STALLED, FreshnessState.UNKNOWN],
)
def test_non_ready_freshness_blocks_entry(harness, state):
    decision = harness.module.run_strategy(
        candle(direction=1), candle(minute=-1, direction=-1),
        freshness_context=_freshness(harness.module, state),
    )
    assert decision.decision_type.value == "ENTRY_SUPPRESSED"
    assert decision.decision_subtype.value == "READINESS_BLOCKED"
    assert decision.reason_text == f"SUPERTREND_FRESHNESS_{state.value}"
    assert harness.attempts == []


def test_stalled_entry_gate_does_not_block_existing_position_exit(harness):
    harness.apply_execution_scenario(RAW_EXECUTION_SCENARIOS["EXIT_FULL"])
    harness.set_position(price=100.0)
    decision = harness.module.run_strategy(
        candle(price=102.0, direction=1), candle(minute=-1, direction=1),
        freshness_context=_freshness(harness.module, FreshnessState.STALLED),
    )
    assert decision.action == "EXIT"
    assert decision.reason_code.value == "TAKE_PROFIT"
    assert len(harness.attempts) == 1
    assert harness.attempts[0].is_exit is True
    assert harness.position is None

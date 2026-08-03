from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from common.supertrend_candle_checkpoint import (
    CandleEvidencePending,
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

    def last_event_evidence(self):
        if not self.events:
            return None
        event = self.events[-1]
        if event[0] == "OBSERVED":
            return (
                event[1].reason,
                event[1].latest_closed_candle_open_time,
            )
        if event[0] == "STALLED":
            return event[2], event[3]
        return None

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
        self.events.append(
            ("STALLED", expected_before, reason, latest_closed_candle_open_time)
        )


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


def test_pending_dependent_evidence_catches_up_without_advancing_then_becomes_ready():
    store = MemoryStore(T0)
    t1 = T0 + timedelta(minutes=1)
    attempts = []

    def pending(_item, context):
        attempts.append(context.state)
        raise CandleEvidencePending("indicator evidence pending")

    result = _run(store, latest=t1, work=[t1], processor=pending)
    assert result.assessment.state is FreshnessState.CATCHING_UP
    assert result.assessment.reason == "CANDLE_DEPENDENT_EVIDENCE_PENDING"
    assert result.checkpoint_after == T0
    assert store.snapshot.last_processed_candle_open_time == T0
    assert store.snapshot.state is FreshnessState.CATCHING_UP
    assert [event[0] for event in store.events].count("ADVANCED") == 0
    assert attempts == [FreshnessState.READY]

    processed = []
    caught_up = _run(
        store, latest=t1, work=[t1],
        processor=lambda item, context: processed.append((item, context.state)),
    )
    assert processed == [(t1, FreshnessState.READY)]
    assert caught_up.checkpoint_after == t1
    assert store.snapshot.state is FreshnessState.READY


def test_new_frozen_target_does_not_inherit_pending_state_from_older_target():
    store = MemoryStore(T0)
    t1 = T0 + timedelta(minutes=1)
    t2 = T0 + timedelta(minutes=2)

    def pending(*_args):
        raise CandleEvidencePending("indicator evidence pending")

    first = _run(store, latest=t1, work=[t1], processor=pending)
    assert first.assessment.state is FreshnessState.CATCHING_UP

    next_cycle = _run(store, latest=t2, work=[t1, t2], processor=pending)
    assert next_cycle.assessment.state is FreshnessState.CATCHING_UP
    assert next_cycle.assessment.reason == "CANDLE_DEPENDENT_EVIDENCE_PENDING"
    assert next_cycle.checkpoint_after == T0


def test_next_cycle_processes_new_frozen_backlog_and_catches_up():
    store = MemoryStore(T0)
    t1 = T0 + timedelta(minutes=1)
    t2 = T0 + timedelta(minutes=2)
    processed = []

    first = _run(
        store, latest=t1, work=[t1],
        processor=lambda item, _context: processed.append(item),
    )
    assert first.checkpoint_after == t1

    second = _run(
        store, latest=t2, work=[t2],
        processor=lambda item, _context: processed.append(item),
    )
    assert second.checkpoint_after == t2
    assert store.snapshot.state is FreshnessState.READY
    assert processed == [t1, t2]


def test_repeated_pending_evidence_without_progress_becomes_stalled():
    store = MemoryStore(T0)
    t1 = T0 + timedelta(minutes=1)

    def pending(*_args):
        raise CandleEvidencePending("indicator evidence pending")

    first = _run(store, latest=t1, work=[t1], processor=pending)
    assert first.assessment.state is FreshnessState.CATCHING_UP
    second = _run(store, latest=t1, work=[t1], processor=pending)
    assert second.assessment.state is FreshnessState.STALLED
    assert second.assessment.reason == "CANDLE_DEPENDENT_EVIDENCE_NO_PROGRESS"
    assert second.checkpoint_after == T0
    assert store.snapshot.last_processed_candle_open_time == T0
    assert store.snapshot.state is FreshnessState.STALLED
    assert store.events[-1] == (
        "STALLED", T0, "CANDLE_DEPENDENT_EVIDENCE_NO_PROGRESS", t1,
    )


def test_dependent_evidence_is_pending_but_sequence_or_core_evidence_is_stalled(
    supertrend,
):
    latest = candle(minute=1)
    previous = candle()
    pending_latest = (*latest[:2], None, None, None, None, None)
    with pytest.raises(CandleEvidencePending, match="latest_missing=ema_21,rsi_14"):
        supertrend._validate_resume_candle_pair(pending_latest, previous)

    missing_core = (latest[0], None, *latest[2:])
    with pytest.raises(
        RuntimeError, match="SUPERTREND_CANDLE_SEQUENCE_EVIDENCE_INCOMPLETE",
    ):
        supertrend._validate_resume_candle_pair(missing_core, previous)

    with pytest.raises(
        RuntimeError, match="SUPERTREND_PREVIOUS_CANDLE_SEQUENCE_MISMATCH",
    ):
        supertrend._validate_resume_candle_pair(candle(minute=2), previous)


def test_pending_evidence_blocks_strategy_and_preserves_checkpoint(
    supertrend, monkeypatch,
):
    store = MemoryStore(T0)
    latest = (T0 + timedelta(minutes=1), 100.0, 99.0, 50.0, 1.0, 99.5, 1)
    pending_latest = (*latest[:2], None, None, None, None, None)
    previous = (T0, 100.0, 99.0, 50.0, 1.0, 99.5, 1)
    strategy_calls = []
    monkeypatch.setattr(supertrend, "PostgresCheckpointStore", lambda *_a: store)
    monkeypatch.setattr(
        supertrend, "get_last_closed_candle",
        lambda target: pending_latest if target == latest[0] else None,
    )
    monkeypatch.setattr(
        supertrend, "get_resume_candle_pairs",
        lambda *_a: [(pending_latest, previous)],
    )
    monkeypatch.setattr(
        supertrend, "run_strategy", lambda *_a, **_k: strategy_calls.append(True),
    )

    result = supertrend.process_supertrend_candle_resume(latest[0])
    assert result.assessment.state is FreshnessState.CATCHING_UP
    assert result.assessment.reason == "CANDLE_DEPENDENT_EVIDENCE_PENDING"
    assert result.checkpoint_after == T0
    assert store.snapshot.last_processed_candle_open_time == T0
    assert strategy_calls == []


def test_loop_passes_snapshot_frozen_target_to_resume(supertrend, monkeypatch):
    frozen_target = T0 + timedelta(minutes=1)
    newer_candle = T0 + timedelta(minutes=2)
    observed_market = {"latest": frozen_target}
    resume_targets = []

    monkeypatch.setattr(supertrend, "exchange_mytrades_enabled", lambda: False)
    monkeypatch.setattr(supertrend, "load_runtime_params", lambda: None)
    monkeypatch.setattr(supertrend, "fetch_klines", lambda: [frozen_target])
    monkeypatch.setattr(
        supertrend, "save_klines",
        lambda _rows: None,
    )

    def finish_snapshot_indicators(*, progress_callback=None):
        assert progress_callback is None
        assert observed_market["latest"] == frozen_target
        observed_market["latest"] = newer_candle
        return frozen_target

    monkeypatch.setattr(supertrend, "update_indicators", finish_snapshot_indicators)
    monkeypatch.setattr(
        supertrend, "process_supertrend_candle_resume", resume_targets.append,
    )

    supertrend.run_loop_iteration(object(), 0.0)
    assert resume_targets == [frozen_target]


def test_five_minute_ready_regression():
    store = MemoryStore(T0)
    t5 = T0 + timedelta(minutes=5)
    processed = []
    result = process_resume_workset(
        store=store,
        interval="5m",
        latest_closed_candle_open_time=t5,
        work_items=[t5],
        open_time_of=lambda value: value,
        processor=lambda item, context: processed.append((item, context.state)),
    )
    assert result.checkpoint_after == t5
    assert store.snapshot.state is FreshnessState.READY
    assert processed == [(t5, FreshnessState.READY)]


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

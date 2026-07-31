from __future__ import annotations

import inspect
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.entry_fill_attribution import (
    EntryFillApplicationDecision,
    EntryFillAttributionMode,
    EntryFillAttributionRepository,
    EntryFillRepositoryError,
    EntryFillEventType,
    EntryFillEvidence,
    EntryFillObservation,
    EntryFillProcessingOutcome,
    FillApplicationInsertOutcome,
    FillApplicationStatus,
    FillAttributionStatus,
    FillEvidenceInsertOutcome,
    FillLineageResolution,
    classify_application_state,
    exchange_wire_client_order_id,
    process_entry_fill_attribution,
    recover_entry_fill_attribution,
)


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)
EXECUTED_AT = NOW - timedelta(seconds=3)
GIT_REVISION = "a" * 40
RAW_OKX_CID = "ORC-L-BNBUSDC-TREND-1m-ENTRY-000001"
INTENT_ID = uuid.UUID("10000000-0000-4000-8000-000000000001")
SUBMISSION_ID = uuid.UUID("20000000-0000-4000-8000-000000000002")
ACK_ID = uuid.UUID("30000000-0000-4000-8000-000000000003")


def observation(**changes) -> EntryFillObservation:
    values = {
        "environment": "live",
        "deployment_id": "local-live",
        "adoption_id": 17,
        "generation": 3,
        "git_revision": GIT_REVISION,
        "exchange_source": "okx",
        "exchange_trade_id": "trade-1001",
        "exchange_order_id": "order-701",
        "client_order_id": RAW_OKX_CID,
        "symbol": "BNBUSDC",
        "side": "BUY",
        "executed_qty": "0.033895",
        "price": "590.125",
        "notional": "20.003786875",
        "fee": "0.00001000",
        "fee_asset": "BNB",
        "executed_at": EXECUTED_AT,
        "observed_at": NOW,
        "producer_identity": "local-live:exchange-ingest",
        "source_payload": {
            "tradeId": "trade-1001",
            "fee": "-0.00001000",
        },
    }
    values.update(changes)
    return EntryFillObservation.build(**values)


def bot_lineage(
    *,
    status: FillAttributionStatus = (
        FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    ),
    method: str = "EXACT_EXCHANGE_ORDER_ID",
    linked_position_id: int | None = None,
) -> FillLineageResolution:
    return FillLineageResolution(
        status=status,
        method=method,
        intent_id=INTENT_ID,
        submission_attempt_id=SUBMISSION_ID,
        ack_id=ACK_ID,
        client_order_id=RAW_OKX_CID,
        strategy="TREND",
        interval="1m",
        order_purpose="ENTRY",
        linked_position_id=linked_position_id,
    )


def partial_bot_lineage() -> FillLineageResolution:
    return FillLineageResolution(
        status=FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE,
        method="EXACT_CLIENT_ORDER_ID_PARTIAL_LINEAGE",
        intent_id=INTENT_ID,
        submission_attempt_id=SUBMISSION_ID,
        client_order_id=RAW_OKX_CID,
        strategy="TREND",
        interval="1m",
        order_purpose="ENTRY",
        detail="ACK_MISSING",
    )


def evidence(
    fill: EntryFillObservation | None = None,
    lineage: FillLineageResolution | None = None,
) -> EntryFillEvidence:
    return EntryFillEvidence.build(
        fill or observation(), lineage or bot_lineage()
    )


class InMemoryFillRepository:
    """Protocol fake with separate evidence and application commit points."""

    def __init__(self, lineage: FillLineageResolution | None = None):
        self.lineage = lineage or bot_lineage()
        self.evidence_rows: dict[
            tuple[str, str, str, str], EntryFillEvidence
        ] = {}
        self.application_rows: dict[
            uuid.UUID, list[EntryFillApplicationDecision]
        ] = {}
        self.calls: list[str] = []
        self.fail_evidence_commits = 0
        self.fail_application_commits = 0
        self.application_proof_valid = True
        self.discovered_application_proof = None

    def resolve_lineage(
        self, fill: EntryFillObservation
    ) -> FillLineageResolution:
        self.calls.append("resolve_lineage")
        return self.lineage

    def commit_evidence(
        self, candidate: EntryFillEvidence
    ) -> FillEvidenceInsertOutcome:
        self.calls.append("commit_evidence")
        if self.fail_evidence_commits:
            self.fail_evidence_commits -= 1
            raise RuntimeError("synthetic crash before evidence commit")
        canonical = self.evidence_rows.get(candidate.natural_key)
        if canonical is None:
            self.evidence_rows[candidate.natural_key] = candidate
            return FillEvidenceInsertOutcome.CREATED
        if (
            canonical.fill_evidence_id == candidate.fill_evidence_id
            and canonical.source_fingerprint == candidate.source_fingerprint
        ):
            return FillEvidenceInsertOutcome.IDEMPOTENT_EXISTING
        return FillEvidenceInsertOutcome.IDEMPOTENCY_CONFLICT

    def load_evidence(
        self, natural_key: tuple[str, str, str, str]
    ) -> EntryFillEvidence | None:
        self.calls.append("load_evidence")
        return self.evidence_rows.get(tuple(natural_key))

    def load_latest_application(
        self, fill_evidence_id: uuid.UUID
    ) -> EntryFillApplicationDecision | None:
        self.calls.append("load_latest_application")
        rows = self.application_rows.get(fill_evidence_id, [])
        return rows[-1] if rows else None

    def application_proof_matches(self, _evidence, _decision) -> bool:
        self.calls.append("application_proof_matches")
        return self.application_proof_valid

    def load_existing_application_proof(self, _evidence):
        self.calls.append("load_existing_application_proof")
        return self.discovered_application_proof

    def append_application(
        self, decision: EntryFillApplicationDecision
    ) -> FillApplicationInsertOutcome:
        self.calls.append("append_application")
        if self.fail_application_commits:
            self.fail_application_commits -= 1
            raise RuntimeError("synthetic crash after evidence commit")
        rows = self.application_rows.setdefault(decision.fill_evidence_id, [])
        for existing in rows:
            if existing.application_decision_id == decision.application_decision_id:
                return (
                    FillApplicationInsertOutcome.IDEMPOTENT_EXISTING
                    if existing.decision_fingerprint
                    == decision.decision_fingerprint
                    else FillApplicationInsertOutcome.IDEMPOTENCY_CONFLICT
                )
        rows.append(decision)
        return FillApplicationInsertOutcome.CREATED

    @property
    def applications(self) -> list[EntryFillApplicationDecision]:
        return [
            decision
            for rows in self.application_rows.values()
            for decision in rows
        ]


class LineageCursor:
    """Small SQL-script fake for the production attribution hierarchy."""

    def __init__(
        self,
        *,
        order_candidates=(),
        cid_candidates=(),
        positions=(),
        partial_candidates=(),
        intent_candidates=(),
        legacy_candidates=(),
    ):
        self.order_candidates = list(order_candidates)
        self.cid_candidates = list(cid_candidates)
        self.positions = [(value,) for value in positions]
        self.partial_candidates = list(partial_candidates)
        self.intent_candidates = list(intent_candidates)
        self.legacy_candidates = list(legacy_candidates)
        self.result = []
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query, _params=()):
        sql = " ".join(str(query).split())
        self.queries.append(sql)
        if "FROM live_entry_order_acks_v1 a" in sql:
            self.result = (
                self.order_candidates
                if "a.exchange_order_id=%s" in sql
                else self.cid_candidates
            )
        elif "SELECT DISTINCT position_id" in sql:
            self.result = self.positions
        elif "FROM live_entry_submissions_v1 s" in sql:
            self.result = self.partial_candidates
        elif "FROM live_entry_intents_v1 i" in sql:
            self.result = self.intent_candidates
        elif "FROM binance_orders" in sql:
            self.result = self.legacy_candidates
        else:
            raise AssertionError(f"unexpected lineage SQL: {sql}")

    def fetchall(self):
        return list(self.result)

    def close(self):
        self.closed = True


class LineageConnection:
    def __init__(self, cursor: LineageCursor):
        self._cursor = cursor
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def ack_candidate_row(
    fill: EntryFillObservation, **changes
) -> tuple[object, ...]:
    values = {
        "ack_id": ACK_ID,
        "submission_attempt_id": SUBMISSION_ID,
        "intent_id": INTENT_ID,
        "environment": fill.environment.value,
        "deployment_id": fill.deployment_id.value,
        "adoption_id": fill.adoption_id,
        "generation": fill.generation,
        "git_revision": fill.git_revision,
        "client_order_id": RAW_OKX_CID,
        "exchange_source": fill.exchange_source,
        "exchange_order_id": fill.exchange_order_id,
        "symbol": fill.symbol,
        "strategy": "TREND",
        "interval": "1m",
        "order_purpose": "ENTRY",
        "side": fill.side,
    }
    values.update(changes)
    return tuple(values.values())


def legacy_order_row(
    fill: EntryFillObservation, **changes
) -> tuple[object, ...]:
    values = {
        "row_id": 701,
        "exchange_source": fill.exchange_source,
        "order_id": fill.exchange_order_id,
        "client_order_id": None,
        "symbol": fill.symbol,
        "side": fill.side,
        "strategy": "TREND",
        "interval": "1m",
        "order_purpose": "ENTRY",
        "position_id": None,
        "reconciled_position_id": None,
        "is_exit": False,
    }
    values.update(changes)
    return tuple(values.values())


def partial_candidate_row(
    fill: EntryFillObservation, **changes
) -> tuple[object, ...]:
    values = {
        "intent_id": INTENT_ID,
        "submission_attempt_id": SUBMISSION_ID,
        "environment": fill.environment.value,
        "deployment_id": fill.deployment_id.value,
        "adoption_id": fill.adoption_id,
        "generation": fill.generation,
        "git_revision": fill.git_revision,
        "client_order_id": RAW_OKX_CID,
        "exchange_source": fill.exchange_source,
        "symbol": fill.symbol,
        "strategy": "TREND",
        "interval": "1m",
        "order_purpose": "ENTRY",
        "side": fill.side,
    }
    values.update(changes)
    return tuple(values.values())


def resolve_with_cursor(
    fill: EntryFillObservation, cursor: LineageCursor
) -> FillLineageResolution:
    connection = LineageConnection(cursor)
    repository = EntryFillAttributionRepository(lambda: connection)
    result = repository.resolve_lineage(fill)
    assert connection.rolled_back is True
    assert connection.closed is True
    assert cursor.closed is True
    return result


def resolve_context_with_cursor(
    fill: EntryFillObservation, cursor: LineageCursor
):
    connection = LineageConnection(cursor)
    repository = EntryFillAttributionRepository(lambda: connection)
    result = repository.resolve_observation_context(fill)
    assert connection.rolled_back is True
    assert connection.closed is True
    assert cursor.closed is True
    return result


def assert_lineage_queries_are_read_only(cursor: LineageCursor) -> None:
    forbidden = (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "MERGE ",
        "TRUNCATE ",
        " FOR UPDATE ",
    )
    for query in cursor.queries:
        leading_verb = query.lstrip().split(maxsplit=1)[0].upper()
        assert leading_verb in {"SELECT", "WITH"}
        upper = f" {query.upper()} "
        assert not any(token in upper for token in forbidden)


def run(
    repository: InMemoryFillRepository,
    fill: EntryFillObservation | None = None,
    *,
    mode: EntryFillAttributionMode = EntryFillAttributionMode.SHADOW,
):
    return process_entry_fill_attribution(
        mode=mode,
        observation=fill or observation(),
        repository=repository,
        clock=lambda: NOW,
    )


def test_mode_defaults_off_and_rejects_unknown_value(monkeypatch):
    monkeypatch.delenv("LIVE_ENTRY_FILL_ATTRIBUTION_MODE", raising=False)
    assert EntryFillAttributionMode.from_env() is EntryFillAttributionMode.OFF
    assert EntryFillAttributionMode.from_env(
        {"LIVE_ENTRY_FILL_ATTRIBUTION_MODE": " shadow "}
    ) is EntryFillAttributionMode.SHADOW
    with pytest.raises(
        ValueError, match="LIVE_ENTRY_FILL_ATTRIBUTION_MODE_INVALID"
    ):
        EntryFillAttributionMode.from_env(
            {"LIVE_ENTRY_FILL_ATTRIBUTION_MODE": "ACTIVE"}
        )


def test_mode_off_does_not_dereference_repository():
    class RepositoryTrap:
        touched = False

        def __getattribute__(self, name):
            if name == "touched":
                return object.__getattribute__(self, name)
            object.__setattr__(self, "touched", True)
            raise AssertionError(f"OFF mode touched repository attribute {name}")

    repository = RepositoryTrap()
    result = process_entry_fill_attribution(
        mode=EntryFillAttributionMode.OFF,
        observation=observation(),
        repository=repository,
        clock=lambda: NOW,
    )

    assert result.outcome is EntryFillProcessingOutcome.MODE_OFF
    assert result.evidence is None
    assert result.application_status is None
    assert repository.touched is False
    assert [event.event_type for event in result.events] == [
        EntryFillEventType.ENTRY_FILL_OBSERVED
    ]


def test_explicit_recovery_forces_enforce_for_exactly_one_observation(
    monkeypatch,
):
    import common.entry_fill_attribution as attribution

    fill = observation()
    repository = InMemoryFillRepository()
    sentinel = object()
    calls = []

    def clock():
        return NOW

    def capture(**kwargs):
        calls.append(kwargs)
        return sentinel

    monkeypatch.setattr(attribution, "process_entry_fill_attribution", capture)

    result = attribution.recover_entry_fill_attribution(
        observation=fill,
        repository=repository,
        clock=clock,
    )

    assert result is sentinel
    assert calls == [
        {
            "mode": EntryFillAttributionMode.ENFORCE,
            "observation": fill,
            "repository": repository,
            "clock": clock,
        }
    ]


def test_explicit_recovery_has_single_fill_idempotency_and_no_backfill_scan():
    repository = InMemoryFillRepository()
    fill = observation()

    first = recover_entry_fill_attribution(
        observation=fill,
        repository=repository,
        clock=lambda: NOW,
    )
    second = recover_entry_fill_attribution(
        observation=fill,
        repository=repository,
        clock=lambda: NOW,
    )

    assert first.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert second.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert len(repository.evidence_rows) == 1
    assert list(repository.evidence_rows) == [fill.natural_key]
    assert len(repository.applications) == 1
    assert repository.calls.count("resolve_lineage") == 2
    assert set(repository.calls) <= {
        "resolve_lineage",
        "commit_evidence",
            "load_evidence",
            "load_existing_application_proof",
            "load_latest_application",
        "append_application",
    }


def test_exchange_trade_identity_is_stable_but_semantic_payload_is_guarded():
    first = observation()
    redelivery = observation(
        observed_at=NOW + timedelta(minutes=5),
        producer_identity="local-live:exchange-retry",
        source_payload={"delivery": "retry", "fee": "-0.00001000"},
    )
    correction = observation(executed_qty="0.033896")
    another_trade = observation(exchange_trade_id="trade-1002")

    first_evidence = evidence(first)
    retry_evidence = evidence(redelivery)
    correction_evidence = evidence(correction)
    another_evidence = evidence(another_trade)

    assert first.natural_key == (
        "live",
        "local-live",
        "okx",
        "trade-1001",
    )
    assert first.source_fingerprint == redelivery.source_fingerprint
    assert first_evidence.fill_evidence_id == retry_evidence.fill_evidence_id
    assert correction.natural_key == first.natural_key
    assert correction.source_fingerprint != first.source_fingerprint
    assert correction_evidence.fill_evidence_id == first_evidence.fill_evidence_id
    assert another_evidence.fill_evidence_id != first_evidence.fill_evidence_id


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("exchange_trade_id", None),
        ("exchange_trade_id", ""),
        ("exchange_order_id", None),
        ("exchange_order_id", ""),
    ],
)
def test_observation_rejects_missing_authoritative_trade_identity(
    field,
    invalid_value,
):
    with pytest.raises(ValueError, match=field):
        observation(**{field: invalid_value})


def test_okx_wire_cid_normalization_and_decimal_fee_are_exact():
    expected_wire_cid = "".join(
        character for character in RAW_OKX_CID if character.isalnum()
    )[:32]
    fill = observation(fee="-0.0000000100")

    assert exchange_wire_client_order_id("okx", RAW_OKX_CID) == expected_wire_cid
    assert exchange_wire_client_order_id("binance", RAW_OKX_CID) == RAW_OKX_CID
    assert fill.wire_client_order_id == expected_wire_cid
    assert fill.fee == Decimal("0.0000000100")
    with pytest.raises(ValueError, match="binary float"):
        observation(fee=0.0001)


def test_exact_exchange_order_id_links_ack_and_existing_position():
    fill = observation()
    ack = ack_candidate_row(fill)
    cursor = LineageCursor(
        order_candidates=[ack], cid_candidates=[ack], positions=[912]
    )

    lineage = resolve_with_cursor(fill, cursor)

    assert lineage.status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
    assert lineage.method == "EXACT_EXCHANGE_ORDER_ID"
    assert lineage.intent_id == INTENT_ID
    assert lineage.submission_attempt_id == SUBMISSION_ID
    assert lineage.ack_id == ACK_ID
    assert lineage.linked_position_id == 912
    assert_lineage_queries_are_read_only(cursor)


def test_exact_normalized_cid_links_ack_without_requiring_position():
    fill = observation()
    ack = ack_candidate_row(fill)
    cursor = LineageCursor(
        order_candidates=[], cid_candidates=[ack], positions=[]
    )

    lineage = resolve_with_cursor(fill, cursor)

    assert lineage.status is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    assert lineage.method == "EXACT_CLIENT_ORDER_ID"
    assert lineage.client_order_id == RAW_OKX_CID
    assert lineage.linked_position_id is None


def test_cid_candidate_with_different_exchange_order_id_fails_closed():
    fill = observation(exchange_order_id="fill-order-701")
    ack = ack_candidate_row(fill, exchange_order_id="ack-order-701")
    cursor = LineageCursor(order_candidates=[], cid_candidates=[ack])

    lineage = resolve_with_cursor(fill, cursor)

    assert lineage.status is FillAttributionStatus.CONFLICTED
    assert lineage.method == "FAIL_CLOSED"
    assert lineage.detail == "ACK_RUNTIME_CONTEXT_MISMATCH"
    assert not any("SELECT DISTINCT position_id" in query for query in cursor.queries)


def test_legacy_exact_order_and_exact_cid_have_distinct_methods():
    fill = observation()
    exact_order = resolve_with_cursor(
        fill,
        LineageCursor(
            legacy_candidates=[
                legacy_order_row(fill, client_order_id=None)
            ]
        ),
    )
    normalized_cid = exchange_wire_client_order_id("okx", RAW_OKX_CID)
    exact_cid = resolve_with_cursor(
        fill,
        LineageCursor(
            legacy_candidates=[
                legacy_order_row(
                    fill,
                    order_id=None,
                    client_order_id=normalized_cid,
                )
            ]
        ),
    )

    assert exact_order.status is FillAttributionStatus.LEGACY_BOT_OWNED
    assert exact_order.method == "EXACT_LEGACY_ORDER_EVIDENCE"
    assert exact_cid.status is FillAttributionStatus.LEGACY_BOT_OWNED
    assert exact_cid.method == "EXACT_LEGACY_CLIENT_ORDER_ID_EVIDENCE"


@pytest.mark.parametrize(
    "legacy_changes",
    [
        {"order_id": "different-order", "client_order_id": RAW_OKX_CID},
        {"client_order_id": "ORC-L-DIFFERENT-CID"},
    ],
    ids=["explicit-order-mismatch", "normalized-cid-mismatch"],
)
def test_legacy_candidate_with_explicit_identity_mismatch_fails_closed(
    legacy_changes,
):
    fill = observation()
    lineage = resolve_with_cursor(
        fill,
        LineageCursor(
            legacy_candidates=[legacy_order_row(fill, **legacy_changes)]
        ),
    )

    assert lineage.status is FillAttributionStatus.CONFLICTED
    assert lineage.method == "FAIL_CLOSED"


@pytest.mark.parametrize(
    ("strategy", "interval"),
    [("TREND", None), (None, "1m")],
)
def test_partial_legacy_strategy_interval_pair_normalizes_both_to_none(
    strategy,
    interval,
):
    fill = observation(client_order_id=None)
    lineage = resolve_with_cursor(
        fill,
        LineageCursor(
            legacy_candidates=[
                legacy_order_row(
                    fill,
                    strategy=strategy,
                    interval=interval,
                )
            ]
        ),
    )

    assert lineage.status is FillAttributionStatus.LEGACY_BOT_OWNED
    assert lineage.method == "EXACT_LEGACY_ORDER_EVIDENCE"
    assert lineage.strategy is None
    assert lineage.interval is None


@pytest.mark.parametrize(
    ("field", "mismatched"),
    [
        ("environment", "paper"),
        ("deployment_id", "local-paper"),
        ("generation", 4),
        ("git_revision", "b" * 40),
    ],
)
def test_ack_runtime_context_mismatch_fails_closed(field, mismatched):
    fill = observation()
    ack = ack_candidate_row(fill, **{field: mismatched})
    cursor = LineageCursor(order_candidates=[ack], cid_candidates=[ack])

    lineage = resolve_with_cursor(fill, cursor)

    assert lineage.status is FillAttributionStatus.CONFLICTED
    assert lineage.method == "FAIL_CLOSED"
    assert lineage.detail == "ACK_RUNTIME_CONTEXT_MISMATCH"
    assert not any("SELECT DISTINCT position_id" in query for query in cursor.queries)


def test_exact_ack_context_preserves_historical_generation_after_rollover():
    preliminary = observation(adoption_id=18, generation=4, git_revision="b" * 40)
    historical = ack_candidate_row(
        preliminary,
        adoption_id=17,
        generation=3,
        git_revision=GIT_REVISION,
    )

    context = resolve_context_with_cursor(
        preliminary,
        LineageCursor(
            order_candidates=[historical],
            cid_candidates=[historical],
        ),
    )

    assert context.environment.value == "live"
    assert context.deployment_id.value == "local-live"
    assert context.adoption_id == 17
    assert context.generation == 3
    assert context.git_revision == GIT_REVISION


def test_exact_partial_lineage_preserves_historical_generation_after_rollover():
    preliminary = observation(adoption_id=18, generation=4, git_revision="b" * 40)
    historical = partial_candidate_row(
        preliminary,
        adoption_id=17,
        generation=3,
        git_revision=GIT_REVISION,
    )

    context = resolve_context_with_cursor(
        preliminary,
        LineageCursor(partial_candidates=[historical]),
    )

    assert context.adoption_id == 17
    assert context.generation == 3
    assert context.git_revision == GIT_REVISION


def test_ack_collision_in_foreign_paper_domain_does_not_make_live_ambiguous():
    fill = observation()
    live = ack_candidate_row(fill)
    paper = ack_candidate_row(
        fill,
        ack_id=uuid.UUID("30000000-0000-4000-8000-000000000099"),
        environment="paper",
        deployment_id="local-paper",
    )

    lineage = resolve_with_cursor(
        fill,
        LineageCursor(
            order_candidates=[paper, live],
            cid_candidates=[live, paper],
        ),
    )

    assert lineage.status is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    assert lineage.ack_id == ACK_ID


def test_same_domain_multiple_exact_ack_contexts_fail_closed():
    fill = observation()
    first = ack_candidate_row(fill)
    second = ack_candidate_row(
        fill,
        ack_id=uuid.UUID("30000000-0000-4000-8000-000000000099"),
        adoption_id=18,
        generation=4,
        git_revision="b" * 40,
    )
    connection = LineageConnection(
        LineageCursor(
            order_candidates=[first, second],
            cid_candidates=[first, second],
        )
    )
    repository = EntryFillAttributionRepository(lambda: connection)

    with pytest.raises(
        EntryFillRepositoryError,
        match="ENTRY_FILL_CONTEXT_ACK_AMBIGUOUS",
    ):
        repository.resolve_observation_context(fill)

    assert connection.rolled_back is True
    assert connection.closed is True

    lineage = resolve_with_cursor(
        fill,
        LineageCursor(
            order_candidates=[first, second],
            cid_candidates=[first, second],
        ),
    )
    assert lineage.status is FillAttributionStatus.AMBIGUOUS
    assert lineage.detail == "MULTIPLE_EXACT_ACK_CANDIDATES"


def test_observation_rejects_environment_deployment_mismatch():
    with pytest.raises(ValueError, match="deployment_id does not match environment"):
        observation(environment="paper")
    with pytest.raises(ValueError, match="deployment_id does not match environment"):
        observation(deployment_id="local-paper")


def test_shadow_mode_accepts_missing_position_as_observed_not_applied():
    repository = InMemoryFillRepository(bot_lineage())

    result = run(repository)

    assert result.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert result.attribution_status is FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    assert result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert result.evidence is not None
    assert result.evidence.lineage.linked_position_id is None
    assert [event.event_type for event in result.events] == [
        EntryFillEventType.ENTRY_FILL_OBSERVED,
        EntryFillEventType.ENTRY_FILL_EVIDENCE_CREATED,
        EntryFillEventType.ENTRY_FILL_ATTRIBUTED,
        EntryFillEventType.ENTRY_FILL_OBSERVED_NOT_APPLIED,
    ]
    assert repository.calls == [
        "resolve_lineage",
        "commit_evidence",
        "load_latest_application",
        "load_existing_application_proof",
        "append_application",
        "load_latest_application",
    ]


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_external_or_manual_fill_is_preserved_without_synthetic_lineage(side):
    fill = observation(side=side, client_order_id=None)
    cursor = LineageCursor()
    lineage = resolve_with_cursor(fill, cursor)
    assert lineage.status is FillAttributionStatus.EXTERNAL_OR_MANUAL_UNLINKED

    repository = InMemoryFillRepository(lineage)
    result = run(repository, fill, mode=EntryFillAttributionMode.ENFORCE)

    assert result.outcome is EntryFillProcessingOutcome.EXTERNAL_UNLINKED
    assert result.application_status is (
        FillApplicationStatus.EXTERNAL_OR_MANUAL_UNLINKED
    )
    assert result.evidence is not None
    assert result.evidence.lineage.intent_id is None
    assert result.evidence.lineage.ack_id is None
    assert repository.applications[0].local_fill_id is None
    assert repository.calls[:3] == [
        "resolve_lineage",
        "commit_evidence",
        "load_latest_application",
    ]


def test_exact_legacy_exit_sell_is_outside_lei1c_and_nonblocking_in_enforce():
    fill = observation(side="SELL", client_order_id=None)
    lineage = resolve_with_cursor(
        fill,
        LineageCursor(
            legacy_candidates=[
                legacy_order_row(
                    fill,
                    order_purpose="EXIT",
                    position_id=912,
                    is_exit=True,
                )
            ]
        ),
    )

    assert lineage.status is FillAttributionStatus.UNKNOWN
    assert lineage.method == "EXACT_NON_ENTRY_ORDER_OUT_OF_SCOPE"
    assert lineage.detail == "OUTSIDE_LEI1C_ENTRY_SCOPE"

    repository = InMemoryFillRepository(lineage)
    result = run(
        repository,
        fill,
        mode=EntryFillAttributionMode.ENFORCE,
    )

    assert result.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert result.attribution_status is FillAttributionStatus.UNKNOWN
    assert result.error_code is None
    assert result.evidence is not None
    assert result.evidence.lineage.method == "EXACT_NON_ENTRY_ORDER_OUT_OF_SCOPE"
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 1


def test_multiple_exact_ack_candidates_are_ambiguous_and_fail_closed():
    fill = observation()
    candidates = [
        ack_candidate_row(fill),
        ack_candidate_row(
            fill,
            ack_id=uuid.UUID("30000000-0000-4000-8000-000000000004"),
        ),
    ]
    lineage = resolve_with_cursor(
        fill,
        LineageCursor(order_candidates=candidates, cid_candidates=candidates),
    )
    assert lineage.status is FillAttributionStatus.AMBIGUOUS

    repository = InMemoryFillRepository(lineage)
    result = run(repository, fill)

    assert result.outcome is EntryFillProcessingOutcome.AMBIGUOUS
    assert result.application_status is FillApplicationStatus.AMBIGUOUS
    assert repository.applications[0].local_fill_id is None


def test_partial_fills_have_distinct_evidence_under_one_ack_and_order():
    repository = InMemoryFillRepository()
    first = observation(
        exchange_trade_id="partial-1",
        executed_qty="0.010000",
        notional="5.901250",
    )
    second = observation(
        exchange_trade_id="partial-2",
        executed_qty="0.023895",
        notional="14.102536875",
        observed_at=NOW + timedelta(seconds=1),
    )

    first_result = run(repository, first)
    second_result = run(repository, second)

    assert first_result.evidence is not None
    assert second_result.evidence is not None
    assert first_result.evidence.fill_evidence_id != second_result.evidence.fill_evidence_id
    assert first_result.evidence.lineage.ack_id == second_result.evidence.lineage.ack_id
    assert len(repository.evidence_rows) == 2
    assert len(repository.applications) == 2
    assert all(
        decision.application_status
        is FillApplicationStatus.OBSERVED_NOT_APPLIED
        for decision in repository.applications
    )


def test_duplicate_delivery_is_idempotent_but_not_true_duplicate_applied():
    repository = InMemoryFillRepository()
    first_result = run(repository)
    retry_result = run(
        repository,
        observation(
            observed_at=NOW + timedelta(minutes=1),
            source_payload={"delivery": "retry"},
        ),
    )

    assert first_result.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert retry_result.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert retry_result.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 1
    assert EntryFillEventType.ENTRY_FILL_EVIDENCE_IDEMPOTENT in {
        event.event_type for event in retry_result.events
    }
    assert EntryFillEventType.ENTRY_FILL_TRUE_DUPLICATE_APPLIED not in {
        event.event_type for event in retry_result.events
    }


def test_late_recovered_ack_appends_attribution_without_mutating_evidence():
    repository = InMemoryFillRepository(partial_bot_lineage())

    first = run(repository)
    assert first.evidence is not None
    immutable_evidence_id = first.evidence.fill_evidence_id
    immutable_fingerprint = first.evidence.attribution_fingerprint
    assert first.evidence.lineage.status is (
        FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE
    )
    assert repository.applications[0].attribution_status is (
        FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE
    )

    repository.lineage = bot_lineage()
    recovered = run(repository)

    assert recovered.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert recovered.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert recovered.attribution_status is (
        FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert recovered.evidence is not None
    assert recovered.evidence.fill_evidence_id == immutable_evidence_id
    assert recovered.evidence.attribution_fingerprint == immutable_fingerprint
    assert recovered.evidence.lineage.status is (
        FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE
    )
    assert len(repository.applications) == 2
    assert repository.applications[-1].attribution_status is (
        FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert repository.applications[-1].application_status is (
        FillApplicationStatus.OBSERVED_NOT_APPLIED
    )
    first_decision, recovered_decision = repository.applications
    assert recovered_decision.intent_id == first_decision.intent_id == INTENT_ID
    assert recovered_decision.submission_attempt_id == (
        first_decision.submission_attempt_id
    ) == SUBMISSION_ID
    assert first_decision.ack_id is None
    assert recovered_decision.ack_id == ACK_ID
    assert recovered_decision.attribution_fingerprint == (
        bot_lineage().attribution_fingerprint
    )
    assert recovered_decision.decision_payload["reason"]["observed_lineage"][
        "identity"
    ] == bot_lineage().identity_payload
    assert EntryFillEventType.ENTRY_FILL_ATTRIBUTED in {
        event.event_type for event in recovered.events
    }

    third = run(repository)

    assert third.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert third.attribution_status is (
        FillAttributionStatus.BOT_OWNED_MISSING_POSITION
    )
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 2


def test_created_and_concurrent_idempotent_paths_build_same_decision():
    lineage = bot_lineage()
    fill = observation()
    canonical = EntryFillEvidence.build(fill, lineage)
    creator = InMemoryFillRepository(lineage)
    concurrent_retry = InMemoryFillRepository(lineage)
    concurrent_retry.evidence_rows[canonical.natural_key] = canonical

    created = run(creator, fill)
    idempotent = run(concurrent_retry, fill)

    assert created.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert idempotent.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert len(creator.applications) == 1
    assert len(concurrent_retry.applications) == 1
    created_decision = creator.applications[0]
    idempotent_decision = concurrent_retry.applications[0]
    assert created_decision.decision_payload == idempotent_decision.decision_payload
    assert created_decision.decision_fingerprint == (
        idempotent_decision.decision_fingerprint
    )
    assert created_decision.application_decision_id == (
        idempotent_decision.application_decision_id
    )


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("exchange_order_id", "order-702"),
        ("client_order_id", "different-client-order-id"),
        ("symbol", "BTCUSDC"),
        ("side", "SELL"),
        ("executed_qty", "0.033896"),
        ("price", "590.126"),
        ("notional", "20.003786876"),
        ("fee", "0.00001001"),
        ("fee_asset", "USDC"),
        ("executed_at", EXECUTED_AT + timedelta(microseconds=1)),
    ],
)
def test_each_source_fingerprint_field_conflicts_without_mutating_evidence(
    field,
    changed_value,
):
    repository = InMemoryFillRepository()
    recorded = run(repository)
    assert recorded.evidence is not None
    canonical = recorded.evidence
    changed = observation(**{field: changed_value})
    assert changed.source_fingerprint != canonical.source_fingerprint

    result = run(repository, changed)

    assert result.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert result.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert result.error_code is None
    assert result.evidence is canonical
    assert result.evidence.source_fingerprint == canonical.source_fingerprint
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 2
    audit_decision = repository.applications[-1]
    assert audit_decision.application_status is (
        FillApplicationStatus.IDEMPOTENCY_CONFLICT
    )
    assert audit_decision.canonical_source_fingerprint == (
        canonical.source_fingerprint
    )
    assert audit_decision.observed_source_fingerprint == (
        changed.source_fingerprint
    )
    assert audit_decision.decision_payload["reason"]["decision_kind"] == (
        "SOURCE_PAYLOAD_CONFLICT"
    )
    assert audit_decision.decision_payload["reason"][
        "observed_semantic_payload"
    ] == changed.semantic_payload


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        (
            "intent_id",
            uuid.UUID("10000000-0000-4000-8000-000000000009"),
        ),
        (
            "submission_attempt_id",
            uuid.UUID("20000000-0000-4000-8000-000000000009"),
        ),
        ("ack_id", uuid.UUID("30000000-0000-4000-8000-000000000009")),
        ("client_order_id", "different-client-order-id"),
        ("strategy", "SCALPING"),
        ("interval", "5m"),
        ("order_purpose", "EXIT"),
    ],
)
def test_attribution_upgrade_rejects_changed_known_lineage_identity(
    field,
    changed_value,
):
    known_lineage = replace(
        bot_lineage(),
        status=FillAttributionStatus.BOT_OWNED_MISSING_LINEAGE,
        detail="ACK_NOT_YET_TRUSTED",
    )
    repository = InMemoryFillRepository(known_lineage)
    recorded = run(repository)
    assert recorded.evidence is not None

    candidate = bot_lineage()
    if field == "order_purpose":
        # A repository must fail closed even if it returns a malformed lineage
        # object that bypassed the constructor's ENTRY-only validation.
        object.__setattr__(candidate, field, changed_value)
    else:
        candidate = replace(candidate, **{field: changed_value})
    repository.lineage = candidate

    result = run(repository)

    assert result.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert result.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert result.error_code is None
    assert result.evidence is recorded.evidence
    assert result.evidence.lineage == known_lineage
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 2
    audit_decision = repository.applications[-1]
    assert audit_decision.application_status is (
        FillApplicationStatus.IDEMPOTENCY_CONFLICT
    )
    assert audit_decision.decision_payload["reason"]["decision_kind"] == (
        "LINEAGE_IDENTITY_CONFLICT"
    )


def test_attributed_position_reassignment_is_lineage_conflict():
    initial = bot_lineage(
        status=FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
        linked_position_id=1,
    )
    repository = InMemoryFillRepository(initial)
    recorded = run(repository)
    assert recorded.evidence is not None

    repository.lineage = bot_lineage(
        status=FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
        linked_position_id=2,
    )
    result = run(repository)

    assert result.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert result.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert result.error_code is None
    assert result.evidence is recorded.evidence
    assert result.evidence.lineage.linked_position_id == 1
    assert len(repository.applications) == 2
    assert repository.applications[-1].decision_payload["reason"][
        "decision_kind"
    ] == "LINEAGE_IDENTITY_CONFLICT"


def test_missing_position_to_exact_position_is_compatible_upgrade():
    repository = InMemoryFillRepository(bot_lineage())
    recorded = run(repository)
    assert recorded.evidence is not None

    repository.lineage = bot_lineage(
        status=FillAttributionStatus.BOT_OWNED_ATTRIBUTED,
        linked_position_id=1,
    )
    upgraded = run(repository)

    assert upgraded.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert upgraded.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert upgraded.attribution_status is FillAttributionStatus.BOT_OWNED_ATTRIBUTED
    assert upgraded.error_code is None
    assert upgraded.evidence is recorded.evidence
    assert upgraded.evidence.lineage.linked_position_id is None
    assert len(repository.applications) == 2
    assert repository.applications[-1].linked_position_id == 1
    assert repository.applications[-1].decision_payload["reason"][
        "decision_kind"
    ] == "ATTRIBUTION_UPGRADE"

    run(repository)
    assert len(repository.applications) == 2


def test_changed_economic_payload_for_same_trade_is_conflict_and_preserves_evidence():
    repository = InMemoryFillRepository()
    first_result = run(repository)
    canonical_fingerprint = first_result.evidence.source_fingerprint

    conflict_result = run(
        repository,
        observation(executed_qty="0.033896", notional="20.004376000"),
    )

    assert conflict_result.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert conflict_result.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert len(repository.evidence_rows) == 1
    assert conflict_result.evidence.source_fingerprint == canonical_fingerprint
    assert repository.applications[-1].observed_source_fingerprint != canonical_fingerprint
    assert repository.applications[-1].local_fill_id is None


@pytest.mark.parametrize(
    "missing",
    ["local_fill_id", "applied_fingerprint", "applied_at", "target"],
)
def test_incomplete_application_proof_remains_observed_not_applied(missing):
    canonical = evidence()
    proof = {
        "local_fill_id": 77,
        "applied_fingerprint": canonical.source_fingerprint,
        "applied_at": NOW,
        "application_target_identity": "binance_order_fills:77",
    }
    key = "application_target_identity" if missing == "target" else missing
    proof[key] = None
    latest = EntryFillApplicationDecision.build(
        canonical,
        application_status=FillApplicationStatus.OBSERVED_NOT_APPLIED,
        decided_at=NOW,
        producer_identity="lei1d-test-double",
        **proof,
    )

    assert classify_application_state(canonical, latest) is (
        FillApplicationStatus.OBSERVED_NOT_APPLIED
    )


def test_complete_matching_application_proof_is_true_duplicate_applied():
    repository = InMemoryFillRepository()
    recorded = run(repository)
    canonical = recorded.evidence
    assert canonical is not None
    applied = EntryFillApplicationDecision.build(
        canonical,
        application_status=FillApplicationStatus.APPLIED,
        decided_at=NOW + timedelta(seconds=1),
        producer_identity="future-lei1d-projector",
        local_fill_id=77,
        applied_fingerprint=canonical.source_fingerprint,
        applied_at=NOW + timedelta(seconds=1),
        application_target_identity="binance_order_fills:77",
    )
    repository.append_application(applied)

    retry = run(repository)

    assert retry.outcome is EntryFillProcessingOutcome.TRUE_DUPLICATE_APPLIED
    assert retry.application_status is FillApplicationStatus.TRUE_DUPLICATE_APPLIED
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 2
    assert retry.events[-1].event_type is (
        EntryFillEventType.ENTRY_FILL_TRUE_DUPLICATE_APPLIED
    )


def test_existing_exact_local_application_proof_is_bridged_then_trusted():
    repository = InMemoryFillRepository()
    repository.discovered_application_proof = (77, NOW)

    first = run(repository)
    replay = run(repository)

    assert first.outcome is EntryFillProcessingOutcome.TRUE_DUPLICATE_APPLIED
    assert first.application_status is (
        FillApplicationStatus.TRUE_DUPLICATE_APPLIED
    )
    assert len(repository.applications) == 1
    applied = repository.applications[0]
    assert applied.application_status is FillApplicationStatus.APPLIED
    assert applied.local_fill_id == 77
    assert applied.application_target_identity == "binance_order_fills:77"
    assert applied.applied_fingerprint == first.evidence.source_fingerprint
    assert replay.outcome is EntryFillProcessingOutcome.TRUE_DUPLICATE_APPLIED
    assert len(repository.applications) == 1


def test_invalidated_applied_proof_appends_sticky_hard_conflict():
    repository = InMemoryFillRepository()
    recorded = run(repository)
    canonical = recorded.evidence
    assert canonical is not None
    applied = EntryFillApplicationDecision.build(
        canonical,
        application_status=FillApplicationStatus.APPLIED,
        decided_at=NOW + timedelta(seconds=1),
        producer_identity="future-lei1d-projector",
        local_fill_id=77,
        applied_fingerprint=canonical.source_fingerprint,
        applied_at=NOW + timedelta(seconds=1),
        application_target_identity="binance_order_fills:77",
    )
    repository.append_application(applied)
    repository.application_proof_valid = False

    replay = run(repository)

    assert replay.outcome is EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    assert replay.application_status is FillApplicationStatus.IDEMPOTENCY_CONFLICT
    assert replay.attribution_status is FillAttributionStatus.CONFLICTED
    assert replay.error_code is None
    assert len(repository.applications) == 3
    hard_conflict = repository.applications[-1]
    assert hard_conflict.application_status is (
        FillApplicationStatus.IDEMPOTENCY_CONFLICT
    )
    assert hard_conflict.attribution_status is FillAttributionStatus.CONFLICTED
    assert hard_conflict.decision_payload["reason"]["decision_kind"] == (
        "LOCAL_APPLICATION_PROOF_DRIFT"
    )
    assert hard_conflict.decision_payload["reason"]["lineage_detail"] == (
        "LOCAL_APPLICATION_PROOF_DRIFT"
    )
    assert EntryFillEventType.ENTRY_FILL_CONFLICT in {
        event.event_type for event in replay.events
    }
    assert EntryFillEventType.ENTRY_FILL_TRUE_DUPLICATE_APPLIED not in {
        event.event_type for event in replay.events
    }

    sticky_replay = run(repository)

    assert sticky_replay.outcome is (
        EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT
    )
    assert sticky_replay.attribution_status is FillAttributionStatus.CONFLICTED
    assert len(repository.applications) == 3
    assert EntryFillEventType.ENTRY_FILL_TRUE_DUPLICATE_APPLIED not in {
        event.event_type for event in sticky_replay.events
    }


def test_application_target_must_name_the_canonical_local_fill():
    canonical = evidence()

    with pytest.raises(ValueError, match="application target"):
        EntryFillApplicationDecision.build(
            canonical,
            application_status=FillApplicationStatus.APPLIED,
            decided_at=NOW,
            producer_identity="future-lei1d-projector",
            local_fill_id=77,
            applied_fingerprint=canonical.source_fingerprint,
            applied_at=NOW,
            application_target_identity="binance_order_fills:78",
        )


def test_correction_pending_is_sticky_and_never_a_true_duplicate():
    repository = InMemoryFillRepository()
    recorded = run(repository)
    canonical = recorded.evidence
    assert canonical is not None
    changed = observation(executed_qty="0.033896").source_fingerprint
    pending = EntryFillApplicationDecision.build(
        canonical,
        application_status=FillApplicationStatus.CORRECTION_PENDING,
        decided_at=NOW + timedelta(seconds=1),
        producer_identity="recovery-review",
        observed_source_fingerprint=changed,
        decision_payload={"reason": "authoritative source changed"},
    )
    repository.append_application(pending)

    retry = run(repository)

    assert retry.outcome is EntryFillProcessingOutcome.CORRECTION_PENDING
    assert retry.application_status is FillApplicationStatus.CORRECTION_PENDING
    assert retry.events[-1].event_type is EntryFillEventType.ENTRY_FILL_CORRECTION_PENDING
    assert len(repository.applications) == 2


def test_crash_before_evidence_commit_retries_to_exactly_one_evidence():
    repository = InMemoryFillRepository()
    repository.fail_evidence_commits = 1

    crashed = run(repository)
    retried = run(repository)

    assert crashed.outcome is EntryFillProcessingOutcome.REPOSITORY_ERROR
    assert "synthetic crash before evidence commit" in crashed.error_code
    assert retried.outcome is EntryFillProcessingOutcome.EVIDENCE_RECORDED
    assert retried.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 1


def test_crash_after_evidence_commit_retries_as_observed_not_applied():
    repository = InMemoryFillRepository()
    repository.fail_application_commits = 1

    crashed = run(repository)
    assert crashed.outcome is EntryFillProcessingOutcome.REPOSITORY_ERROR
    assert len(repository.evidence_rows) == 1
    assert repository.applications == []

    retried = run(repository)

    assert retried.outcome is EntryFillProcessingOutcome.EVIDENCE_IDEMPOTENT
    assert retried.application_status is FillApplicationStatus.OBSERVED_NOT_APPLIED
    assert len(repository.evidence_rows) == 1
    assert len(repository.applications) == 1


def test_lei1c_contains_no_position_projection_or_lifecycle_open_write():
    source = inspect.getsource(EntryFillAttributionRepository).upper()
    boundary_source = inspect.getsource(process_entry_fill_attribution).upper()

    for forbidden in (
        "INSERT INTO POSITIONS",
        "UPDATE POSITIONS",
        "DELETE FROM POSITIONS",
        "POSITION_OPENED",
    ):
        assert forbidden not in source
        assert forbidden not in boundary_source

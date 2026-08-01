from __future__ import annotations

import uuid
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common import execution
from common.entry_intent import LiveEntryIntent
from common.exchange_client import ExchangeAPIException, OkxMarketDataAdapter
from common.entry_intent import EntryIntentInsertOutcome
from common.entry_submission import (
    AckPersistOutcome,
    AckPersistenceFailed,
    ActiveAdoptionResolutionError,
    ActiveEntrySubmissionAdoption,
    EntryOrderAck,
    EntrySubmissionAttempt,
    EntrySubmissionEventType,
    EntrySubmissionExecutionOutcome,
    EntrySubmissionMode,
    IntentCommitFailed,
    IntentCommitOutcomeUnknown,
    SubmissionAttemptOutcome,
    execute_committed_entry_submission,
)


GIT_REVISION = "f" * 40
PREPARED_AT = datetime(2026, 7, 31, 18, 45, tzinfo=timezone.utc)
ACKNOWLEDGED_AT = PREPARED_AT + timedelta(seconds=2)


def _intent(**changes) -> LiveEntryIntent:
    values = {
        "environment": "live",
        "deployment_id": "local-live",
        "git_revision": GIT_REVISION,
        "adoption_id": 1,
        "generation": 1,
        "decision_id": uuid.UUID("29a20a95-9555-522d-aedb-6b923c283ca1"),
        "symbol": "BNBUSDC",
        "strategy": "TREND",
        "interval": "1m",
        "exchange_source": "okx",
        "client_order_id": "ORC-L-BNBUSDC-TREN-1m-E-lei1b",
        "requested_qty": Decimal("0.033895"),
        "prepared_at": PREPARED_AT,
        "producer_identity": "bot-trend",
    }
    values.update(changes)
    return LiveEntryIntent.build(**values)


class StubRepository:
    def __init__(
        self,
        *,
        intent_outcome=EntryIntentInsertOutcome.CREATED,
        attempt_outcome=SubmissionAttemptOutcome.CREATED,
        ack_outcome=AckPersistOutcome.PERSISTED,
        existing_ack=None,
        existing_attempt=None,
    ):
        self.intent_outcome = intent_outcome
        self.attempt_outcome = attempt_outcome
        self.ack_outcome = ack_outcome
        self.existing_ack = existing_ack
        self.existing_attempt = existing_attempt
        self.intent_error = None
        self.attempt_error = None
        self.ack_error = None
        self.calls = []
        self.attempts = []
        self.acks = []

    def commit_intent(self, intent):
        self.calls.append("commit_intent")
        if self.intent_error is not None:
            raise self.intent_error
        return self.intent_outcome

    def load_ack(self, intent_id):
        self.calls.append("load_ack")
        return self.existing_ack

    def load_attempt(self, intent_id):
        self.calls.append("load_attempt")
        return self.existing_attempt

    def load_submission_attempt(self, intent_id, attempt_ordinal=1):
        self.calls.append("load_submission_attempt")
        return self.existing_attempt

    def record_submission_attempt(self, attempt):
        self.calls.append("record_submission_attempt")
        self.attempts.append(attempt)
        if self.attempt_error is not None:
            raise self.attempt_error
        return self.attempt_outcome

    def persist_ack(self, ack):
        self.calls.append("persist_ack")
        self.acks.append(ack)
        if self.ack_error is not None:
            error = self.ack_error
            self.ack_error = None
            raise error
        return self.ack_outcome


def _network_response(order_id="okx-order-1", status="live"):
    return {"orderId": order_id, "status": status}


def _execute(
    repository,
    *,
    intent=None,
    mode=EntrySubmissionMode.ENFORCE,
    network_submit=None,
    lookup=None,
    event_sink=None,
):
    return execute_committed_entry_submission(
        mode=mode,
        intent=intent or _intent(),
        repository=repository,
        network_submit=network_submit or (lambda: _network_response()),
        lookup_by_client_order_id=lookup or (
            lambda **_: {"outcome": "NOT_FOUND", "order": None}
        ),
        event_sink=event_sink,
        clock=lambda: ACKNOWLEDGED_AT,
    )


def test_mode_defaults_off_and_invalid_configuration_fails_closed(monkeypatch):
    monkeypatch.delenv("LIVE_ENTRY_SUBMISSION_MODE", raising=False)
    assert EntrySubmissionMode.from_env() is EntrySubmissionMode.OFF
    assert EntrySubmissionMode.from_env(
        {"LIVE_ENTRY_SUBMISSION_MODE": " shadow "}
    ) is EntrySubmissionMode.SHADOW
    with pytest.raises(ValueError, match="MODE_INVALID"):
        EntrySubmissionMode.from_env({"LIVE_ENTRY_SUBMISSION_MODE": "AUTO"})


def test_attempt_and_ack_identities_are_deterministic_and_immutable():
    intent = _intent()
    first = EntrySubmissionAttempt.build(
        intent,
        submitted_at=PREPARED_AT,
        producer_identity="entry-submitter",
    )
    retry = EntrySubmissionAttempt.build(
        intent,
        submitted_at=PREPARED_AT + timedelta(seconds=30),
        producer_identity="entry-submitter",
    )
    assert first.submission_attempt_id == retry.submission_attempt_id
    assert first.submission_fingerprint == retry.submission_fingerprint
    with pytest.raises(FrozenInstanceError):
        first.requested_qty = Decimal("9")  # type: ignore[misc]

    ack = EntryOrderAck.build(
        intent,
        first,
        exchange_order_id="okx-order-1",
        exchange_order_status="live",
        acknowledged_at=ACKNOWLEDGED_AT,
        producer_identity="entry-submitter",
    )
    recovered = EntryOrderAck.build(
        intent,
        retry,
        exchange_order_id="okx-order-1",
        exchange_order_status="LIVE",
        acknowledged_at=ACKNOWLEDGED_AT + timedelta(seconds=30),
        producer_identity="entry-submitter",
        recovered_by_client_order_id=True,
    )
    assert ack.ack_id == recovered.ack_id
    assert ack.ack_fingerprint == recovered.ack_fingerprint
    assert ack.exchange_order_status == "LIVE"
    assert not ack.recovered_by_client_order_id
    assert recovered.recovered_by_client_order_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("environment", "paper"),
        ("deployment_id", "vps-live"),
        ("adoption_id", 2),
        ("generation", 2),
        ("git_revision", "e" * 40),
        ("client_order_id", "different-cid"),
        ("requested_qty", Decimal("0.033896")),
    ],
)
def test_ack_rejects_attempt_runtime_or_semantic_mismatch(field, value):
    intent = _intent()
    attempt = EntrySubmissionAttempt.build(
        intent,
        submitted_at=PREPARED_AT,
        producer_identity="entry-submitter",
    )
    mismatched = replace(attempt, **{field: value})
    with pytest.raises(ValueError, match="does not match intent"):
        EntryOrderAck.build(
            intent,
            mismatched,
            exchange_order_id="okx-order-1",
            exchange_order_status="LIVE",
            acknowledged_at=ACKNOWLEDGED_AT,
            producer_identity="entry-submitter",
        )


def test_off_does_not_construct_ledger_work_and_shadow_never_controls_admission():
    off_repository = StubRepository()
    off_network = []
    off = _execute(
        off_repository,
        mode=EntrySubmissionMode.OFF,
        network_submit=lambda: off_network.append("send") or {"legacy": True},
    )
    assert off.outcome is EntrySubmissionExecutionOutcome.OFF_NETWORK_SUBMITTED
    assert off_repository.calls == []
    assert off_network == ["send"]

    shadow_repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.CONFLICT
    )
    shadow_network = []
    shadow = _execute(
        shadow_repository,
        mode=EntrySubmissionMode.SHADOW,
        network_submit=lambda: shadow_network.append("send") or {"legacy": True},
    )
    assert shadow.outcome is EntrySubmissionExecutionOutcome.SHADOW_NETWORK_SUBMITTED
    assert shadow_repository.calls == ["commit_intent"]
    assert shadow_network == ["send"]
    assert [event.event_type for event in shadow.events] == [
        EntrySubmissionEventType.ENTRY_INTENT_CONFLICT
    ]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            IntentCommitFailed("insert failed"),
            EntrySubmissionExecutionOutcome.BLOCKED_INTENT_COMMIT_FAILED,
        ),
        (
            IntentCommitOutcomeUnknown("commit unknown"),
            EntrySubmissionExecutionOutcome.BLOCKED_INTENT_COMMIT_UNKNOWN,
        ),
    ],
)
def test_enforce_blocks_network_on_intent_transaction_failure(error, expected):
    repository = StubRepository()
    repository.intent_error = error
    network = []
    result = _execute(
        repository,
        network_submit=lambda: network.append("send") or _network_response(),
    )
    assert result.outcome is expected
    assert not result.network_called
    assert network == []
    assert [event.event_type for event in result.events] == [
        EntrySubmissionEventType.ENTRY_INTENT_COMMIT_FAILED,
        EntrySubmissionEventType.ENTRY_NETWORK_BLOCKED_NO_COMMITTED_INTENT,
    ]


def test_enforce_blocks_network_on_semantic_intent_conflict():
    repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.CONFLICT
    )
    network = []
    result = _execute(
        repository,
        network_submit=lambda: network.append("send") or _network_response(),
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.BLOCKED_INTENT_CONFLICT
    assert network == []
    assert repository.calls == ["commit_intent"]


def test_commit_and_attempt_claim_complete_before_network_and_ack_persistence():
    repository = StubRepository()

    def network_submit():
        assert repository.calls == [
            "commit_intent",
            "load_ack",
            "record_submission_attempt",
        ]
        repository.calls.append("network_submit")
        return _network_response()

    result = _execute(repository, network_submit=network_submit)
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_PERSISTED
    assert result.network_called
    assert repository.calls == [
        "commit_intent",
        "load_ack",
        "record_submission_attempt",
        "network_submit",
        "persist_ack",
    ]
    assert result.ack is repository.acks[0]
    assert [event.event_type for event in result.events] == [
        EntrySubmissionEventType.ENTRY_INTENT_CREATED,
        EntrySubmissionEventType.ENTRY_SUBMISSION_ATTEMPTED,
        EntrySubmissionEventType.ENTRY_ACK_PERSISTED,
    ]


@pytest.mark.parametrize("strategy", ["RSI", "TREND", "BBRANGE", "SUPERTREND"])
def test_all_four_strategies_use_the_same_committed_submission_contract(strategy):
    repository = StubRepository()
    intent = _intent(
        strategy=strategy,
        producer_identity=f"bot-{strategy.lower()}",
    )
    result = _execute(repository, intent=intent)
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_PERSISTED
    assert repository.attempts[0].strategy == strategy
    assert repository.acks[0].strategy == strategy


def test_existing_ack_is_an_idempotent_no_network_no_lookup_result():
    intent = _intent()
    attempt = EntrySubmissionAttempt.build(
        intent,
        submitted_at=PREPARED_AT,
        producer_identity=intent.producer_identity,
    )
    ack = EntryOrderAck.build(
        intent,
        attempt,
        exchange_order_id="okx-order-1",
        exchange_order_status="LIVE",
        acknowledged_at=ACKNOWLEDGED_AT,
        producer_identity=intent.producer_identity,
    )
    repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.IDEMPOTENT_EXISTING,
        existing_ack=ack,
    )
    network = []
    lookup = []
    result = _execute(
        repository,
        intent=intent,
        network_submit=lambda: network.append("send"),
        lookup=lambda **kwargs: lookup.append(kwargs),
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_ALREADY_PERSISTED
    assert result.ack is ack
    assert not result.network_called
    assert not result.recovery_lookup_performed
    assert network == [] and lookup == []


def test_claimed_attempt_recovers_ack_by_exact_cid_without_second_send():
    intent = _intent()
    attempt = EntrySubmissionAttempt.build(
        intent,
        submitted_at=PREPARED_AT,
        producer_identity=intent.producer_identity,
    )
    repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.IDEMPOTENT_EXISTING,
        attempt_outcome=SubmissionAttemptOutcome.IDEMPOTENT_EXISTING,
        existing_attempt=attempt,
    )
    network = []
    lookups = []

    def lookup(**kwargs):
        lookups.append(kwargs)
        return {
            "outcome": "FOUND",
            "order": {"orderId": "okx-order-1", "status": "live"},
        }

    result = _execute(
        repository,
        intent=intent,
        network_submit=lambda: network.append("send"),
        lookup=lookup,
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_RECOVERED
    assert not result.network_called
    assert result.recovery_lookup_performed
    assert network == []
    assert lookups == [{
        "symbol": "BNBUSDC",
        "client_order_id": _intent().client_order_id,
    }]
    assert repository.acks[0].recovered_by_client_order_id
    assert [event.event_type for event in result.events][-2:] == [
        EntrySubmissionEventType.ENTRY_ACK_RECOVERED_BY_CLIENT_ORDER_ID,
        EntrySubmissionEventType.ENTRY_ACK_PERSISTED,
    ]


def test_existing_committed_intent_without_attempt_recovers_before_any_send():
    """A restart in the intent-commit/send gap may not create a blind order."""
    repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.IDEMPOTENT_EXISTING,
        attempt_outcome=SubmissionAttemptOutcome.CREATED,
        existing_attempt=None,
    )
    network = []
    lookups = []
    result = _execute(
        repository,
        network_submit=lambda: network.append("send") or _network_response(),
        lookup=lambda **kwargs: lookups.append(kwargs) or {
            "outcome": "NOT_FOUND",
            "order": None,
        },
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.RECOVERY_NOT_FOUND
    assert not result.network_called
    assert result.recovery_lookup_performed
    assert network == []
    assert lookups == [{
        "symbol": "BNBUSDC",
        "client_order_id": _intent().client_order_id,
    }]


def test_recovery_rejects_mismatched_committed_attempt_without_send():
    intent = _intent()
    mismatched_attempt = replace(
        EntrySubmissionAttempt.build(
            intent,
            submitted_at=PREPARED_AT,
            producer_identity=intent.producer_identity,
        ),
        generation=2,
    )
    repository = StubRepository(
        intent_outcome=EntryIntentInsertOutcome.IDEMPOTENT_EXISTING,
        existing_attempt=mismatched_attempt,
    )
    network = []
    result = _execute(
        repository,
        intent=intent,
        network_submit=lambda: network.append("send"),
        lookup=lambda **_: {
            "outcome": "FOUND",
            "order": {"orderId": "okx-order-1", "status": "live"},
        },
    )
    assert (
        result.outcome
        is EntrySubmissionExecutionOutcome.BLOCKED_ATTEMPT_CONFLICT
    )
    assert not result.network_called
    assert network == []


@pytest.mark.parametrize(
    ("lookup_outcome", "expected"),
    [
        ("NOT_FOUND", EntrySubmissionExecutionOutcome.RECOVERY_NOT_FOUND),
        ("AMBIGUOUS", EntrySubmissionExecutionOutcome.RECOVERY_AMBIGUOUS),
        ("ERROR", EntrySubmissionExecutionOutcome.RECOVERY_ERROR),
    ],
)
def test_uncertain_or_existing_attempt_never_blindly_retries(
    lookup_outcome, expected,
):
    repository = StubRepository(
        attempt_outcome=SubmissionAttemptOutcome.IDEMPOTENT_EXISTING
    )
    network = []
    result = _execute(
        repository,
        network_submit=lambda: network.append("send"),
        lookup=lambda **_: {
            "outcome": lookup_outcome,
            "order": None,
            "detail": "bounded recovery result",
        },
    )
    assert result.outcome is expected
    assert network == []
    assert not result.network_called
    assert result.recovery_lookup_performed


def test_send_success_ack_persistence_failure_recovers_without_second_send():
    repository = StubRepository()
    repository.ack_error = AckPersistenceFailed("write failed after ACK")
    network = []
    lookup = []

    def submit():
        network.append("send")
        return _network_response()

    def recover(**kwargs):
        lookup.append(kwargs)
        return {
            "outcome": "FOUND",
            # Exchange state may advance after the original ACK while its DB
            # commit result is unknown.  Recovery must retry the original ACK
            # fingerprint after CID proves the same exchange order identity.
            "order": {"orderId": "okx-order-1", "status": "filled"},
        }

    result = _execute(
        repository,
        network_submit=submit,
        lookup=recover,
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_RECOVERED
    assert network == ["send"]
    assert len(lookup) == 1
    assert len(repository.acks) == 2
    assert repository.acks[-1].recovered_by_client_order_id
    assert repository.acks[-1].exchange_order_status == "LIVE"
    assert (
        repository.acks[-1].ack_fingerprint
        == repository.acks[0].ack_fingerprint
    )


def test_ack_conflict_fails_closed_and_preserves_audit_event():
    repository = StubRepository(ack_outcome=AckPersistOutcome.CONFLICT)
    result = _execute(repository)
    assert result.outcome is EntrySubmissionExecutionOutcome.BLOCKED_ACK_CONFLICT
    assert result.network_called
    assert EntrySubmissionEventType.ENTRY_ACK_CONFLICT in {
        event.event_type for event in result.events
    }


def test_event_sink_failure_cannot_move_commit_before_network_boundary():
    repository = StubRepository()
    result = _execute(
        repository,
        event_sink=lambda event: (_ for _ in ()).throw(RuntimeError("sink down")),
    )
    assert result.outcome is EntrySubmissionExecutionOutcome.ACK_PERSISTED
    assert result.network_called


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"data": []}, "NOT_FOUND"),
        (
            {"data": [
                {"ordId": "one", "clOrdId": "wire", "state": "live"},
                {"ordId": "two", "clOrdId": "wire", "state": "live"},
            ]},
            "AMBIGUOUS",
        ),
        (
            {"data": [{
                "ordId": "one", "clOrdId": "different", "state": "live",
            }]},
            "AMBIGUOUS",
        ),
        (
            {"data": [{
                "ordId": "", "clOrdId": "wire", "state": "live",
            }]},
            "AMBIGUOUS",
        ),
    ],
)
def test_okx_recovery_classifies_absence_and_ambiguous_identity(
    monkeypatch, response, expected,
):
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)
    monkeypatch.setattr(
        client, "_private_request", lambda *_args, **_kwargs: response
    )
    # The raw CID normalizes to the literal exchange wire identity "wire".
    result = client.find_order_by_client_order_id(
        symbol="BNBUSDC", client_order_id="w-i-r-e"
    )
    assert result["outcome"] == expected
    assert result["order"] is None


@pytest.mark.parametrize(
    ("code", "expected"),
    [("51603", "NOT_FOUND"), ("50111", "ERROR")],
)
def test_okx_recovery_does_not_treat_auth_or_transport_error_as_absence(
    monkeypatch, code, expected,
):
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)

    def fail(*_args, **_kwargs):
        raise ExchangeAPIException("lookup failed", code=code)

    monkeypatch.setattr(client, "_private_request", fail)
    result = client.find_order_by_client_order_id(
        symbol="BNBUSDC", client_order_id="w-i-r-e"
    )
    assert result["outcome"] == expected
    assert result["order"] is None


def test_okx_recovery_uses_exact_wire_cid_without_execution_admission(
    monkeypatch,
):
    """The recovery read is exact and remains available under containment."""
    monkeypatch.setenv("OKX_EXECUTION_ENABLED", "0")
    wire_cid = "ORCLBNBUSDCTREN1mElei1b"
    observed = []
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)

    def private_request(method, path, **kwargs):
        observed.append((method, path, kwargs))
        return {
            "data": [{
                "instId": "BNB-USDC",
                "ordId": "3751751866456252416",
                "clOrdId": wire_cid,
                "state": "filled",
                "accFillSz": "0.033895",
            }]
        }

    monkeypatch.setattr(client, "_private_request", private_request)
    result = client.find_order_by_client_order_id(
        symbol="BNBUSDC",
        client_order_id="ORC-L-BNBUSDC-TREN-1m-E-lei1b",
    )

    assert observed == [(
        "GET",
        "/api/v5/trade/order",
        {"params": {
            "instId": "BNB-USDC",
            "clOrdId": wire_cid,
        }},
    )]
    assert result["outcome"] == "FOUND"
    assert result["order"]["orderId"] == "3751751866456252416"
    assert result["order"]["clientOrderId"] == wire_cid
    assert _intent().client_order_id == "ORC-L-BNBUSDC-TREN-1m-E-lei1b"
    assert result["order"]["status"] == "FILLED"


def test_common_entry_gate_commits_before_wire_and_returns_linked_ack(
    monkeypatch,
):
    repository = StubRepository()
    repository.resolve_active_adoption = lambda **_: (
        ActiveEntrySubmissionAdoption(
            adoption_id=1,
            generation=1,
            environment="live",
            deployment_id="local-live",
            git_revision=GIT_REVISION,
        )
    )

    class Client:
        def place_market_order(self, **_kwargs):
            assert repository.calls == [
                "commit_intent",
                "load_ack",
                "record_submission_attempt",
            ]
            repository.calls.append("network_wire")
            return {
                "orderId": "okx-order-common",
                "status": "NEW",
                "executedQty": "0",
            }

        def find_order_by_client_order_id(self, **_kwargs):
            return {"outcome": "NOT_FOUND", "order": None}

    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("GIT_SHA", GIT_REVISION)
    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.033895},
    )
    result = execution.place_live_order(
        Client(),
        "BNBUSDC",
        "BUY",
        0.033895,
        trading_mode="LIVE",
        live_orders_enabled=True,
        quote_asset="USDC",
        panic_disable_trading=False,
        live_max_notional=0.0,
        client_order_id=_intent().client_order_id,
        strategy="TREND",
        interval="1m",
        exchange_source="okx",
        order_purpose="ENTRY",
        entry_submission_mode=EntrySubmissionMode.ENFORCE,
        entry_submission_repository=repository,
        entry_submission_clock=lambda: ACKNOWLEDGED_AT,
    )
    assert result["ok"] is True
    assert result["entry_submission_outcome"] == "ACK_PERSISTED"
    assert result["order_id"] == "okx-order-common"
    assert repository.calls == [
        "commit_intent",
        "load_ack",
        "record_submission_attempt",
        "network_wire",
        "persist_ack",
    ]


def test_lei1d_enforce_never_projects_position_from_ack_execution_fields(
    monkeypatch,
):
    repository = StubRepository()
    repository.resolve_active_adoption = lambda **_: (
        ActiveEntrySubmissionAdoption(
            adoption_id=1,
            generation=1,
            environment="live",
            deployment_id="local-live",
            git_revision=GIT_REVISION,
        )
    )

    class Client:
        def place_market_order(self, **_kwargs):
            return {
                "orderId": "okx-order-lei1d",
                "status": "FILLED",
                "executedQty": "0.033895",
            }

        def find_order_by_client_order_id(self, **_kwargs):
            return {"outcome": "NOT_FOUND", "order": None}

    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("GIT_SHA", GIT_REVISION)
    monkeypatch.setenv("LIVE_ENTRY_POSITION_PROJECTION_MODE", "ENFORCE")
    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.033895},
    )
    result = execution.place_live_order(
        Client(), "BNBUSDC", "BUY", 0.033895,
        trading_mode="LIVE", live_orders_enabled=True, quote_asset="USDC",
        panic_disable_trading=False, live_max_notional=0.0,
        client_order_id=_intent().client_order_id, strategy="TREND",
        interval="1m", exchange_source="okx", order_purpose="ENTRY",
        entry_submission_mode=EntrySubmissionMode.ENFORCE,
        entry_submission_repository=repository,
        entry_submission_clock=lambda: ACKNOWLEDGED_AT,
    )
    assert result["order_accepted"] is True
    assert result["executed"] is False
    assert result["live_ok"] is False
    assert result["executed_qty"] == 0
    assert result["fill_evidence"] == ()
    assert result["status"] == "ENTRY_FILL_AWAITING_LEI1D_PROJECTION"


def test_common_enforce_identity_failure_blocks_wire(monkeypatch):
    repository = StubRepository()

    def reject_identity(**_kwargs):
        raise ActiveAdoptionResolutionError("SHA_MISMATCH")

    repository.resolve_active_adoption = reject_identity
    network = []

    class Client:
        def place_market_order(self, **_kwargs):
            network.append("send")

    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("GIT_SHA", GIT_REVISION)
    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.033895},
    )
    result = execution.place_live_order(
        Client(),
        "BNBUSDC",
        "BUY",
        0.033895,
        trading_mode="LIVE",
        live_orders_enabled=True,
        quote_asset="USDC",
        panic_disable_trading=False,
        live_max_notional=0.0,
        client_order_id=_intent().client_order_id,
        strategy="TREND",
        interval="1m",
        exchange_source="okx",
        order_purpose="ENTRY",
        entry_submission_mode=EntrySubmissionMode.ENFORCE,
        entry_submission_repository=repository,
    )
    assert result["ok"] is False
    assert result["reason"] == "ENTRY_RUNTIME_IDENTITY_MISMATCH"
    assert result["attempted"] is False
    assert network == []


def test_common_exit_bypasses_entry_gate_even_if_entry_mode_is_enforce(
    monkeypatch,
):
    class NeverRepository:
        def __getattr__(self, name):
            raise AssertionError(f"ENTRY repository called for EXIT: {name}")

    network = []

    class Client:
        def place_market_order(self, **_kwargs):
            network.append("send")
            return {
                "orderId": "exit-order",
                "status": "NEW",
                "executedQty": "0",
            }

    monkeypatch.setattr(
        execution,
        "preflight_live_order",
        lambda *_args, **_kwargs: {"ok": True, "qty_adj": 0.01},
    )
    result = execution.place_live_order(
        Client(),
        "BNBUSDC",
        "SELL",
        0.01,
        trading_mode="LIVE",
        live_orders_enabled=True,
        quote_asset="USDC",
        panic_disable_trading=False,
        live_max_notional=0.0,
        client_order_id="exit-cid",
        strategy="TREND",
        interval="1m",
        exchange_source="okx",
        order_purpose="EXIT",
        entry_submission_mode=EntrySubmissionMode.ENFORCE,
        entry_submission_repository=NeverRepository(),
    )
    assert result["ok"] is True
    assert result["order_id"] == "exit-order"
    assert network == ["send"]

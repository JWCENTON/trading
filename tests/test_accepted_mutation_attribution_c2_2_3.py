import copy
import json
from decimal import Decimal

import pytest

from common.exchange_fill_change_control import (
    FillApplicationClassification,
    FillMutationDecision,
    authoritative_fill_fingerprint,
    classify_fill_application_state,
    mark_fill_change_applied,
    register_fill_change,
)


@pytest.fixture(autouse=True)
def immutable_runtime_revision(monkeypatch):
    monkeypatch.setenv("GIT_SHA", "2" * 40)


class LedgerCursor:
    def __init__(self):
        self.resolutions = {}
        self.exchange_fills = {}
        self.ledger = {}
        self.next_id = 1
        self.result = None
        self.rowcount = 0
        self.positions = {
            position_id: {"status": "OPEN", "qty": qty}
            for position_id, qty in {
                3079: "0.00000110",
                3080: "0.000123",
                3081: "0.000038",
                3082: "0.000037",
                3083: "0.00090",
                3084: "0.00000106",
                3085: "0.00095",
            }.items()
        }
        self.lifecycle_events = []
        self.financial_truth = []

    def execute(self, query, params=()):
        sql = " ".join(query.split())
        self.result = None
        self.rowcount = 0
        if "SELECT CASE" in sql and "FROM positions p" in sql:
            order_id = str(params[3])
            self.result = self.resolutions[order_id]
            return
        if (
            "SELECT ingestion_id,source_fingerprint" in sql
            and "FROM exchange_fill_ingestion_state_v2" in sql
        ):
            key = tuple(str(value) for value in params)
            row = self.ledger.get(key)
            if row is not None:
                self.result = (
                    row["ingestion_id"],
                    row["source_fingerprint"],
                    copy.deepcopy(row["authoritative_payload"]),
                    row["correction_revision"],
                    row["adoption_id"],
                    row["contract_generation"],
                    row["applied_fingerprint"],
                    row["applied_at"],
                    row["local_fill_id"],
                    row["application_status"],
                )
            return
        if sql.startswith("SELECT") and "FROM binance_order_fills" in sql:
            self.result = self.exchange_fills.get(
                (str(params[0]), str(params[1]))
            )
            return
        if sql.startswith("INSERT INTO exchange_fill_ingestion_state_v2"):
            identity = tuple(str(value) for value in params[:4])
            is_existing_fill = len(params) == 12
            if is_existing_fill:
                (
                    order_id,
                    side,
                    fingerprint,
                    status,
                    local_fill_id,
                    payload_json,
                    decision,
                    same,
                ) = params[4:]
                revision = 0 if same else 1
            else:
                (
                    order_id,
                    side,
                    fingerprint,
                    status,
                    payload_json,
                    decision,
                ) = params[4:]
                local_fill_id = None
                revision = 0
            ingestion_id = self.next_id
            self.next_id += 1
            self.ledger[identity] = {
                "ingestion_id": ingestion_id,
                "source": identity[0],
                "trade_id": identity[3],
                "order_id": str(order_id),
                "side": str(side),
                "source_fingerprint": str(fingerprint),
                "applied_fingerprint": None,
                "applied_at": None,
                "local_fill_id": local_fill_id,
                "application_status": str(status),
                "authoritative_payload": json.loads(payload_json),
                "last_decision": str(decision),
                "correction_revision": revision,
                "adoption_id": None,
                "contract_generation": None,
            }
            self.result = (ingestion_id,)
            self.rowcount = 1
            return
        if (
            sql.startswith("UPDATE exchange_fill_ingestion_state_v2")
            and "SET last_seen_at=clock_timestamp(),application_status=%s"
            in sql
        ):
            status, decision, ingestion_id = params
            row = self._by_id(ingestion_id)
            row["application_status"] = str(status)
            row["last_decision"] = str(decision)
            self.rowcount = 1
            return
        if (
            sql.startswith("UPDATE exchange_fill_ingestion_state_v2")
            and "source_fingerprint=%s" in sql
        ):
            fingerprint, status, revision, payload_json, decision, ingestion_id = (
                params
            )
            row = self._by_id(ingestion_id)
            row.update(
                source_fingerprint=str(fingerprint),
                application_status=str(status),
                correction_revision=int(revision),
                authoritative_payload=json.loads(payload_json),
                last_decision=str(decision),
            )
            self.rowcount = 1
            return
        if (
            sql.startswith("UPDATE exchange_fill_ingestion_state_v2")
            and "applied_fingerprint=%s" in sql
        ):
            (
                fingerprint,
                status,
                adoption_id,
                generation,
                ingestion_id,
                expected_adoption,
                expected_generation,
            ) = params
            row = self._by_id(ingestion_id)
            existing = (row["adoption_id"], row["contract_generation"])
            expected = (expected_adoption, expected_generation)
            if existing not in {(None, None), expected}:
                return
            local_fill = self.exchange_fills.get(
                (row["source"], row["trade_id"])
            )
            if local_fill is None or str(local_fill[1]) != row["order_id"]:
                return
            row.update(
                applied_fingerprint=str(fingerprint),
                applied_at="clock_timestamp",
                local_fill_id=int(local_fill[0]),
                application_status=str(status),
                adoption_id=int(adoption_id),
                contract_generation=int(generation),
            )
            self.rowcount = 1
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.result

    def _by_id(self, ingestion_id):
        return next(
            row for row in self.ledger.values()
            if row["ingestion_id"] == int(ingestion_id)
        )


def fill(
    trade_id,
    order_id,
    *,
    symbol="SOLUSDC",
    qty="1",
    price="10",
    fee="0.01",
    fee_asset="USDC",
):
    return {
        "source": "okx",
        "symbol": symbol,
        "trade_id": str(trade_id),
        "order_id": str(order_id),
        "side": "SELL",
        "executed_qty": qty,
        "avg_price": price,
        "commission_amount": fee,
        "commission_asset": fee_asset,
        "event_time_ms": 1785313867843,
        "environment": "live",
        "deployment_id": "local-live",
    }


def old_fill_tuple(row):
    return (
        sum((index + 1) * ord(char) for index, char in enumerate(row["trade_id"])),
        row["order_id"],
        row["symbol"],
        row["side"],
        row["executed_qty"],
        row["avg_price"],
        row["commission_amount"],
        row["commission_asset"],
        row["event_time_ms"],
    )


def test_runtime_equivalent_residual_replay_never_claims_applied_generation():
    cur = LedgerCursor()
    before = copy.deepcopy(cur.positions)
    residual_fill_counts = {3079: 2, 3080: 2, 3081: 2, 3082: 2, 3083: 2, 3084: 2, 3085: 5}
    rows = []
    for position_id, count in residual_fill_counts.items():
        for index in range(count):
            row = fill(
                f"{position_id}{index}",
                f"order-{position_id}-{index}",
            )
            rows.append(row)
            cur.resolutions[row["order_id"]] = (
                "LEGACY_UNPROJECTED", 1, 1
            )
            cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)

    # The failed rollout produced 21 observational rows in total. Four unrelated
    # rows are permitted, but only the 17 residual identities are asserted here.
    for index in range(4):
        row = fill(f"other-{index}", f"other-order-{index}")
        cur.resolutions[row["order_id"]] = ("LEGACY_UNPROJECTED", 1, 1)
        cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)
        register_fill_change(cur, row, account_identity_key="account-1")

    for _ in range(5):
        for row in rows:
            change = register_fill_change(
                cur, row, account_identity_key="account-1"
            )
            assert change.mutation_decision in {
                FillMutationDecision.NO_CHANGE,
                FillMutationDecision.LEGACY_RECONSTRUCTION_BLOCKED,
            }
            mark_fill_change_applied(cur, change)

    assert len(cur.ledger) == 21
    residual_trade_ids = {row["trade_id"] for row in rows}
    residual_ledger = [
        row for key, row in cur.ledger.items() if key[3] in residual_trade_ids
    ]
    assert len(residual_ledger) == 17
    assert all(row["adoption_id"] is None for row in residual_ledger)
    assert all(row["contract_generation"] is None for row in residual_ledger)
    assert all(row["applied_fingerprint"] is None for row in residual_ledger)
    assert all(row["applied_at"] is None for row in residual_ledger)
    assert all(
        row["application_status"] == "OBSERVED_NOT_APPLIED"
        for row in residual_ledger
    )
    assert cur.positions == before
    assert cur.lifecycle_events == []
    assert cur.financial_truth == []


def test_first_accepted_application_owns_immutable_generation():
    cur = LedgerCursor()
    row = fill("forward-1", "forward-order", qty="1")
    cur.resolutions[row["order_id"]] = ("FORWARD_C2_2", 1, 1)

    accepted = register_fill_change(cur, row, account_identity_key="account-1")
    assert accepted.decision is FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE
    ledger = next(iter(cur.ledger.values()))
    assert ledger["adoption_id"] is None
    assert ledger["applied_fingerprint"] is None

    # The production ingest UPSERT creates the canonical local fill before the
    # application-proof update joins it by exact source/trade/order identity.
    cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)
    mark_fill_change_applied(cur, accepted)
    assert ledger["adoption_id"] == 1
    assert ledger["contract_generation"] == 1
    first_fingerprint = ledger["applied_fingerprint"]

    duplicate = register_fill_change(cur, row, account_identity_key="account-1")
    mark_fill_change_applied(cur, duplicate)
    assert duplicate.decision is FillMutationDecision.NO_CHANGE
    assert duplicate.application_status is (
        FillApplicationClassification.TRUE_DUPLICATE_APPLIED
    )
    assert ledger["adoption_id"] == 1
    assert ledger["contract_generation"] == 1
    assert ledger["applied_fingerprint"] == first_fingerprint

    cur.resolutions[row["order_id"]] = (
        "ADOPTION_GENERATION_MISMATCH", 2, 2
    )
    duplicate_after_generation_2 = register_fill_change(
        cur, row, account_identity_key="account-1"
    )
    mark_fill_change_applied(cur, duplicate_after_generation_2)
    assert ledger["adoption_id"] == 1
    assert ledger["contract_generation"] == 1
    assert ledger["applied_fingerprint"] == first_fingerprint


def test_same_fingerprint_without_complete_application_proof_is_observed_only():
    cur = LedgerCursor()
    row = fill("forward-unapplied", "forward-unapplied-order")
    cur.resolutions[row["order_id"]] = ("FORWARD_C2_2", 1, 1)

    first = register_fill_change(cur, row, account_identity_key="account-1")
    assert first.decision is FillMutationDecision.NEW_AUTHORITATIVE_EVIDENCE

    replay = register_fill_change(cur, row, account_identity_key="account-1")

    assert replay.decision is FillMutationDecision.OBSERVED_NOT_APPLIED
    assert replay.application_status is (
        FillApplicationClassification.OBSERVED_NOT_APPLIED
    )
    ledger = next(iter(cur.ledger.values()))
    assert ledger["local_fill_id"] is None
    assert ledger["applied_fingerprint"] is None
    assert ledger["applied_at"] is None
    mark_fill_change_applied(cur, replay)
    assert ledger["applied_fingerprint"] is None


def test_hard_application_state_precedes_new_attribution_classification():
    status = classify_fill_application_state(
        source_fingerprint="a" * 64,
        applied_fingerprint=None,
        applied_at=None,
        local_fill_id=None,
        resolved_adoption_id=1,
        resolved_generation=1,
        applied_adoption_id=None,
        applied_generation=None,
        local_fill_matches=False,
        current_status="CORRECTION_PENDING",
        attribution_status="EXTERNAL_OR_MANUAL_UNLINKED",
    )

    assert status is FillApplicationClassification.CORRECTION_PENDING


def test_external_lei1c_attribution_cannot_replay_as_true_duplicate():
    cur = LedgerCursor()
    row = fill("manual-external", "manual-external-order")
    cur.resolutions[row["order_id"]] = ("FORWARD_C2_2", 1, 1)

    first = register_fill_change(cur, row, account_identity_key="account-1")
    cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)
    mark_fill_change_applied(cur, first)

    attributed = dict(row)
    attributed["_lei1c_attribution_status"] = (
        "EXTERNAL_OR_MANUAL_UNLINKED"
    )
    replay = register_fill_change(
        cur, attributed, account_identity_key="account-1"
    )

    assert replay.application_status is (
        FillApplicationClassification.EXTERNAL_OR_MANUAL_UNLINKED
    )
    assert replay.application_status is not (
        FillApplicationClassification.TRUE_DUPLICATE_APPLIED
    )
    assert replay.permits_mutation is False
    ledger = next(iter(cur.ledger.values()))
    assert ledger["application_status"] == "EXTERNAL_OR_MANUAL_UNLINKED"


def test_mutated_local_fill_invalidates_legacy_duplicate_proof():
    cur = LedgerCursor()
    row = fill("local-drift", "local-drift-order")
    cur.resolutions[row["order_id"]] = ("FORWARD_C2_2", 1, 1)
    first = register_fill_change(cur, row, account_identity_key="account-1")
    cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)
    mark_fill_change_applied(cur, first)

    drifted = list(old_fill_tuple(row))
    drifted[4] = Decimal("9.999")
    cur.exchange_fills[("okx", row["trade_id"])] = tuple(drifted)

    replay = register_fill_change(cur, row, account_identity_key="account-1")

    assert replay.decision is FillMutationDecision.OBSERVED_NOT_APPLIED
    assert replay.application_status is (
        FillApplicationClassification.OBSERVED_NOT_APPLIED
    )
    assert replay.application_status is not (
        FillApplicationClassification.TRUE_DUPLICATE_APPLIED
    )


def test_legacy_empty_zero_fee_fingerprint_replays_without_correction():
    cur = LedgerCursor()
    row = fill("legacy-zero-fee", "legacy-zero-fee-order", fee="0")
    cur.resolutions[row["order_id"]] = ("FORWARD_C2_2", 1, 1)
    first = register_fill_change(cur, row, account_identity_key="account-1")
    cur.exchange_fills[("okx", row["trade_id"])] = old_fill_tuple(row)
    mark_fill_change_applied(cur, first)
    ledger = next(iter(cur.ledger.values()))

    legacy_payload = dict(ledger["authoritative_payload"])
    legacy_payload["fee_quantity"] = ""
    legacy_fingerprint = authoritative_fill_fingerprint(legacy_payload)
    ledger["authoritative_payload"] = legacy_payload
    ledger["source_fingerprint"] = legacy_fingerprint
    ledger["applied_fingerprint"] = legacy_fingerprint

    replay = register_fill_change(cur, row, account_identity_key="account-1")

    assert replay.decision is FillMutationDecision.NO_CHANGE
    assert replay.application_status is (
        FillApplicationClassification.TRUE_DUPLICATE_APPLIED
    )
    assert ledger["correction_revision"] == 0
    assert ledger["source_fingerprint"] == legacy_fingerprint


def test_legacy_new_evidence_is_observational_only():
    cur = LedgerCursor()
    row = fill("legacy-new", "legacy-new-order")
    cur.resolutions[row["order_id"]] = ("LEGACY_UNPROJECTED", 1, 1)

    change = register_fill_change(cur, row, account_identity_key="account-1")
    assert (
        change.mutation_decision
        is FillMutationDecision.LEGACY_RECONSTRUCTION_BLOCKED
    )
    mark_fill_change_applied(cur, change)

    ledger = next(iter(cur.ledger.values()))
    assert ledger["adoption_id"] is None
    assert ledger["contract_generation"] is None
    assert ledger["applied_fingerprint"] is None
    assert ledger["applied_at"] is None


def test_nonaccepted_decisions_never_execute_applied_update():
    class NoAppliedWrite:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("nonaccepted decision attempted applied write")

    for decision in (
        FillMutationDecision.NO_CHANGE,
        FillMutationDecision.OBSERVED_NOT_APPLIED,
        FillMutationDecision.INCOMPLETE_EVIDENCE,
        FillMutationDecision.AMBIGUOUS_CORRECTION,
        FillMutationDecision.ADOPTION_GENERATION_MISMATCH,
        FillMutationDecision.ADOPTION_NOT_ACTIVE,
        FillMutationDecision.LEGACY_RECONSTRUCTION_BLOCKED,
    ):
        mark_fill_change_applied(
            NoAppliedWrite(),
            type(
                "Change",
                (),
                {
                    "permits_mutation": False,
                    "decision": decision,
                    "adoption_id": 1,
                    "contract_generation": 1,
                },
            )(),
        )


def test_correction_preserves_compatible_generation_and_mismatch_is_noop():
    cur = LedgerCursor()
    original = fill("correction-1", "correction-order", qty="1")
    cur.resolutions[original["order_id"]] = ("FORWARD_C2_2", 1, 1)
    first = register_fill_change(
        cur, original, account_identity_key="account-1"
    )
    cur.exchange_fills[("okx", original["trade_id"])] = old_fill_tuple(original)
    mark_fill_change_applied(cur, first)
    ledger = next(iter(cur.ledger.values()))

    corrected = fill("correction-1", "correction-order", qty="1.1")
    correction = register_fill_change(
        cur, corrected, account_identity_key="account-1"
    )
    assert correction.decision is FillMutationDecision.AUTHORITATIVE_CORRECTION
    cur.exchange_fills[("okx", corrected["trade_id"])] = old_fill_tuple(corrected)
    mark_fill_change_applied(cur, correction)
    assert ledger["adoption_id"] == 1
    assert ledger["contract_generation"] == 1
    applied_correction = ledger["applied_fingerprint"]

    cur.resolutions[original["order_id"]] = (
        "ADOPTION_GENERATION_MISMATCH", 2, 2
    )
    mismatched = fill("correction-1", "correction-order", qty="1.2")
    mismatch = register_fill_change(
        cur, mismatched, account_identity_key="account-1"
    )
    assert (
        mismatch.mutation_decision
        is FillMutationDecision.ADOPTION_GENERATION_MISMATCH
    )
    mark_fill_change_applied(cur, mismatch)
    assert ledger["adoption_id"] == 1
    assert ledger["contract_generation"] == 1
    assert ledger["applied_fingerprint"] == applied_correction

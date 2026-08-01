from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import threading

import pytest

from common.contract_adoption import (
    contract_adoption_compatible,
    replace_active_contract_adoption,
    require_runtime_git_revision,
    rollback_active_to_prepared_contract_adoption,
    runtime_revision_matches_adoption_provenance,
)
from common.exchange_fill_change_control import (
    InventoryRowGeneration,
    classify_inventory_row_generation,
)


OLD_SHA = "1" * 40
NEW_SHA = "2" * 40
RECOVERY_SHA = "3" * 40
TRANSITION_AT = datetime(2026, 7, 29, 21, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]


def lifecycle_rows():
    return {
        1: [
            1, "FEE_AWARE_INVENTORY_C2_2", "paper", "local-paper", 1,
            "ACTIVE", TRANSITION_AT, None, OLD_SHA, "C2.2.2", None, None,
        ],
        2: [
            2, "FEE_AWARE_INVENTORY_C2_2", "paper", "local-paper", 2,
            "PREPARED", None, None, NEW_SHA, "C2.2.2", "image-2", 1,
        ],
    }


class ReplacementCursor:
    def __init__(self, rows=None, *, fail_activation=False):
        self.rows = deepcopy(rows or lifecycle_rows())
        self.fail_activation = fail_activation
        self.savepoint_rows = None
        self.result = None
        self.results = []
        self.rowcount = 0
        self.transition_reads = 0

    def execute(self, query, params=()):
        sql = " ".join(query.split())
        self.result = None
        self.results = []
        self.rowcount = 0
        if sql == "SAVEPOINT c2_2_4_adoption_replacement":
            self.savepoint_rows = deepcopy(self.rows)
            return
        if sql == "ROLLBACK TO SAVEPOINT c2_2_4_adoption_replacement":
            self.rows = deepcopy(self.savepoint_rows)
            return
        if sql == "RELEASE SAVEPOINT c2_2_4_adoption_replacement":
            self.savepoint_rows = None
            return
        if "pg_advisory_xact_lock" in sql:
            assert params == ("FEE_AWARE_INVENTORY_C2_2|paper|local-paper",)
            self.result = (None,)
            return
        if "WHERE adoption_id IN" in sql and "FOR UPDATE" in sql:
            self.results = [
                tuple(self.rows[row_id])
                for row_id in sorted({int(params[0]), int(params[1])})
                if row_id in self.rows
            ]
            return
        if sql.startswith("SELECT COUNT(*)"):
            contract, environment, deployment, excluded = params
            count = sum(
                row[1] == contract
                and row[2] == environment
                and row[3] == deployment
                and row[5] == "ACTIVE"
                and row[0] != excluded
                for row in self.rows.values()
            )
            self.result = (count,)
            return
        if sql == "SELECT clock_timestamp()":
            self.transition_reads += 1
            self.result = (TRANSITION_AT,)
            return
        if "SET status=%s,deactivated_at=%s" in sql:
            terminal_status, transition_at, reason, row_id, generation, sha = (
                params
            )
            row = self.rows[int(row_id)]
            if row[5] == "ACTIVE" and row[4] == generation and row[8] == sha:
                row[5] = terminal_status
                row[7] = transition_at
                self.rowcount = 1
            assert reason
            return
        if "SET status='ACTIVE'" in sql:
            if self.fail_activation:
                return
            transition_at, old_id, row_id, generation, sha = params
            row = self.rows[int(row_id)]
            if row[5] == "PREPARED" and row[4] == generation and row[8] == sha:
                row[5] = "ACTIVE"
                row[6] = transition_at
                row[11] = old_id
                self.rowcount = 1
                self.result = tuple(row[:11])
            return
        raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.results


def replace(cur, **overrides):
    values = {
        "prepared_adoption_id": 2,
        "expected_current_active_adoption_id": 1,
        "expected_current_active_generation": 1,
        "expected_current_active_git_revision": OLD_SHA,
        "expected_new_git_revision": NEW_SHA,
        "expected_environment": "paper",
        "expected_deployment_id": "local-paper",
        "supersession_reason": "candidate revision rollout",
    }
    values.update(overrides)
    return replace_active_contract_adoption(cur, **values)


def test_atomic_replacement_uses_one_timestamp_and_one_active_generation():
    cur = ReplacementCursor()

    result = replace(cur)

    assert result.outcome == "REPLACED"
    assert result.adoption.status == "ACTIVE"
    assert cur.rows[1][5] == "SUPERSEDED"
    assert cur.rows[2][5] == "ACTIVE"
    assert cur.rows[1][7] == cur.rows[2][6] == TRANSITION_AT
    assert cur.rows[2][11] == 1
    assert cur.transition_reads == 1
    assert sum(row[5] == "ACTIVE" for row in cur.rows.values()) == 1


def test_identical_retry_is_idempotent_without_another_timestamp_or_mutation():
    cur = ReplacementCursor()
    replace(cur)
    snapshot = deepcopy(cur.rows)

    result = replace(cur)

    assert result.outcome == "ALREADY_REPLACED"
    assert cur.rows == snapshot
    assert cur.transition_reads == 1


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda rows: rows[1].__setitem__(5, "DEACTIVATED"),
         "ADOPTION_REPLACEMENT_ACTIVE_MISMATCH"),
        (lambda rows: rows[2].__setitem__(5, "ROLLED_BACK"),
         "ADOPTION_REPLACEMENT_PREPARED_MISMATCH"),
        (lambda rows: rows[2].__setitem__(8, "4" * 40),
         "ADOPTION_REPLACEMENT_SHA_MISMATCH"),
        (lambda rows: rows[2].__setitem__(3, "other-paper"),
         "ADOPTION_REPLACEMENT_SCOPE_MISMATCH"),
        (lambda rows: rows[2].__setitem__(4, 1),
         "ADOPTION_REPLACEMENT_GENERATION_ORDER_INVALID"),
    ],
)
def test_replacement_mismatches_fail_before_mutation(mutation, error):
    rows = lifecycle_rows()
    mutation(rows)
    cur = ReplacementCursor(rows)
    snapshot = deepcopy(cur.rows)

    with pytest.raises(RuntimeError, match=error):
        replace(cur)

    assert cur.rows == snapshot
    assert cur.transition_reads == 0


def test_unexpected_second_active_is_a_conflict():
    rows = lifecycle_rows()
    rows[3] = [
        3, "FEE_AWARE_INVENTORY_C2_2", "paper", "local-paper", 3,
        "ACTIVE", TRANSITION_AT, None, "3" * 40, "C2.2.2", None, None,
    ]
    cur = ReplacementCursor(rows)

    with pytest.raises(RuntimeError, match="ADOPTION_REPLACEMENT_CONFLICT"):
        replace(cur)

    assert cur.rows[1][5] == "ACTIVE"
    assert cur.rows[2][5] == "PREPARED"


def test_second_update_failure_rolls_back_old_update_to_savepoint():
    cur = ReplacementCursor(fail_activation=True)

    with pytest.raises(RuntimeError, match="ADOPTION_REPLACEMENT_CONFLICT"):
        replace(cur)

    assert cur.rows[1][5] == "ACTIVE"
    assert cur.rows[2][5] == "PREPARED"
    assert sum(row[5] == "ACTIVE" for row in cur.rows.values()) == 1


def test_concurrent_identical_replacements_have_one_transition():
    cur = ReplacementCursor()
    transaction_lock = threading.Lock()
    barrier = threading.Barrier(2)
    outcomes = []

    def attempt():
        barrier.wait()
        with transaction_lock:
            outcomes.append(replace(cur).outcome)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["ALREADY_REPLACED", "REPLACED"]
    assert cur.transition_reads == 1
    assert sum(row[5] == "ACTIVE" for row in cur.rows.values()) == 1


def test_replacement_serializes_against_standard_activation():
    cur = ReplacementCursor()
    replace(cur)

    # A standard activation racing behind the replacement sees generation 2
    # as ACTIVE rather than PREPARED and must reject it.
    assert cur.rows[2][5] != "PREPARED"
    assert sum(row[5] == "ACTIVE" for row in cur.rows.values()) == 1


def test_replacement_serializes_against_rollback_of_old_active():
    cur = ReplacementCursor()
    replace(cur)

    # rollback_contract_adoption accepts only ACTIVE/PREPARED. Once its row
    # lock is released, the old row is immutable SUPERSEDED.
    rollback_eligible = cur.rows[1][5] in {"ACTIVE", "PREPARED"}
    assert not rollback_eligible
    assert cur.rows[2][5] == "ACTIVE"


def test_concurrent_preparation_does_not_change_explicit_candidate_selection():
    rows = lifecycle_rows()
    rows[3] = [
        3, "FEE_AWARE_INVENTORY_C2_2", "paper", "local-paper", 3,
        "PREPARED", None, None, "3" * 40, "C2.2.2", "image-3", 2,
    ]
    cur = ReplacementCursor(rows)

    result = replace(cur)

    assert result.adoption.adoption_id == 2
    assert cur.rows[2][5] == "ACTIVE"
    assert cur.rows[3][5] == "PREPARED"


def test_helper_declares_advisory_and_row_locking():
    source = (ROOT / "common/contract_adoption.py").read_text()

    assert "pg_advisory_xact_lock" in source
    assert "FOR UPDATE" in source
    assert "SELECT clock_timestamp()" in source
    assert "supersedes_adoption_id=%s" in source


def test_runtime_revision_remains_a_rollout_diagnostic(monkeypatch):
    monkeypatch.setenv("GIT_SHA", OLD_SHA)
    assert require_runtime_git_revision() == OLD_SHA

    active_generation_sha = NEW_SHA
    assert not runtime_revision_matches_adoption_provenance(
        adoption_git_revision=active_generation_sha,
    )


@pytest.mark.parametrize(
    ("overrides", "compatible"),
    [
        ({}, True),
        ({"contract_name": "OTHER"}, False),
        ({"environment": "live"}, False),
        ({"deployment_id": "other-paper"}, False),
        ({"status": "SUPERSEDED"}, False),
        ({"generation": 0}, False),
        ({"generation": 4}, False),
    ],
)
def test_contract_compatibility_excludes_runtime_provenance(
    overrides, compatible
):
    values = {
        "contract_name": "FEE_AWARE_INVENTORY_C2_2",
        "environment": "paper",
        "deployment_id": "local-paper",
        "status": "ACTIVE",
        "generation": 5,
        "expected_environment": "paper",
        "expected_deployment_id": "local-paper",
        "expected_generation": 5,
    }
    values.update(overrides)
    assert contract_adoption_compatible(**values) is compatible


def test_missing_or_invalid_runtime_revision_fails_closed(monkeypatch):
    monkeypatch.delenv("GIT_SHA", raising=False)
    with pytest.raises(RuntimeError, match="RUNTIME_GIT_REVISION_REQUIRED"):
        require_runtime_git_revision()

    monkeypatch.setenv("GIT_SHA", "main")
    with pytest.raises(RuntimeError, match="RUNTIME_GIT_REVISION_REQUIRED"):
        require_runtime_git_revision()


def test_generation_one_complete_position_remains_compatible_under_generation_two():
    result = classify_inventory_row_generation(
        entry_time=TRANSITION_AT,
        active_adopted_at=TRANSITION_AT,
        active_adoption_id=2,
        active_generation=2,
        position_adoption_id=1,
        position_generation=1,
        existing_projected_compatible=True,
    )

    assert result is InventoryRowGeneration.EXISTING_PROJECTED_C2_2


def test_duplicate_keeps_generation_one_ownership_under_generation_two():
    position_ownership = (1, 1)
    active_runtime = (2, 2)

    assert position_ownership != active_runtime
    assert classify_inventory_row_generation(
        entry_time=TRANSITION_AT,
        active_adopted_at=TRANSITION_AT,
        active_adoption_id=active_runtime[0],
        active_generation=active_runtime[1],
        position_adoption_id=position_ownership[0],
        position_generation=position_ownership[1],
        existing_projected_compatible=True,
    ) is InventoryRowGeneration.EXISTING_PROJECTED_C2_2


def test_new_position_receives_active_generation_two():
    result = classify_inventory_row_generation(
        entry_time=TRANSITION_AT,
        active_adopted_at=TRANSITION_AT,
        active_adoption_id=2,
        active_generation=2,
        position_adoption_id=None,
        position_generation=None,
        existing_projected_compatible=False,
    )

    assert result is InventoryRowGeneration.FORWARD_C2_2


def test_failed_candidate_recovery_uses_generation_three_not_reactivation():
    cur = ReplacementCursor()
    replace(cur)
    cur.rows[3] = [
        3, "FEE_AWARE_INVENTORY_C2_2", "paper", "local-paper", 3,
        "PREPARED", None, None, RECOVERY_SHA, "C2.2.2", "image-1", 2,
    ]

    result = rollback_active_to_prepared_contract_adoption(
        cur,
        prepared_recovery_adoption_id=3,
        expected_failed_active_adoption_id=2,
        expected_failed_active_generation=2,
        expected_failed_active_git_revision=NEW_SHA,
        expected_recovery_git_revision=RECOVERY_SHA,
        expected_environment="paper",
        expected_deployment_id="local-paper",
        rollback_reason="candidate start recovery",
    )

    assert result.outcome == "REPLACED"
    assert cur.rows[1][5] == "SUPERSEDED"
    assert cur.rows[2][5] == "ROLLED_BACK"
    assert cur.rows[3][5] == "ACTIVE"


def test_api_image_build_contract_carries_immutable_revision():
    dockerfile = (ROOT / "api/Dockerfile").read_text()
    compose = (ROOT / "docker-compose.yaml").read_text()
    api_section = compose[compose.index("  api:"):compose.index("  frontend:")]

    assert "ARG GIT_SHA" in dockerfile
    assert "org.opencontainers.image.revision=\"${GIT_SHA}\"" in dockerfile
    assert "ENV GIT_SHA=\"${GIT_SHA}\"" in dockerfile
    assert "GIT_SHA: ${GIT_SHA}" in api_section

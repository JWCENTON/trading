from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import re


GIT_REVISION_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


@dataclass(frozen=True)
class ContractAdoption:
    adoption_id: int
    contract_name: str
    environment: str
    deployment_id: str
    generation: int
    status: str
    adopted_at: datetime | None
    deactivated_at: datetime | None
    git_revision: str
    migration_version: str
    container_revision: str | None


@dataclass(frozen=True)
class ContractAdoptionReplacement:
    adoption: ContractAdoption
    outcome: str


def _row(value) -> ContractAdoption:
    return ContractAdoption(*value[:11])


def require_runtime_git_revision() -> str:
    revision = str(os.getenv("GIT_SHA") or "").strip().lower()
    if GIT_REVISION_PATTERN.fullmatch(revision) is None:
        raise RuntimeError("RUNTIME_GIT_REVISION_REQUIRED")
    return revision


def prepare_contract_adoption(
    cur,
    *,
    contract_name: str,
    environment: str,
    deployment_id: str,
    generation: int,
    git_revision: str,
    migration_version: str,
    container_revision: str | None,
    activation_reason: str,
    supersedes_adoption_id: int | None = None,
) -> ContractAdoption:
    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,deactivated_at,git_revision,migration_version,
               container_revision
        FROM prepare_contract_adoption(%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            contract_name,
            environment,
            deployment_id,
            generation,
            git_revision,
            migration_version,
            container_revision,
            activation_reason,
            supersedes_adoption_id,
        ),
    )
    return _row(cur.fetchone())


def activate_contract_adoption(
    cur,
    *,
    adoption_id: int,
    expected_git_revision: str,
    expected_environment: str,
    expected_deployment_id: str,
) -> ContractAdoption:
    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,deactivated_at,git_revision,migration_version,
               container_revision
        FROM activate_contract_adoption(%s,%s,%s,%s)
        """,
        (
            adoption_id,
            expected_git_revision,
            expected_environment,
            expected_deployment_id,
        ),
    )
    return _row(cur.fetchone())


def rollback_contract_adoption(
    cur, *, adoption_id: int, reason: str
) -> ContractAdoption:
    cur.execute(
        """
        SELECT contract_name,environment,deployment_id,status
        FROM runtime_contract_adoption_v2
        WHERE adoption_id=%s
        """,
        (adoption_id,),
    )
    target = cur.fetchone()
    if target is None:
        raise RuntimeError("ADOPTION_ROLLBACK_INVALID_STATE")
    contract_name, environment, deployment_id, _status = target
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"{contract_name}|{environment}|{deployment_id}",),
    )
    cur.execute(
        """
        SELECT status
        FROM runtime_contract_adoption_v2
        WHERE adoption_id=%s
        FOR UPDATE
        """,
        (adoption_id,),
    )
    locked = cur.fetchone()
    if locked is None or locked[0] not in {"PREPARED", "ACTIVE"}:
        raise RuntimeError("ADOPTION_ROLLBACK_INVALID_STATE")
    if locked[0] == "ACTIVE":
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM runtime_contract_adoption_v2
              WHERE supersedes_adoption_id=%s AND status='PREPARED'
            )
            """,
            (adoption_id,),
        )
        if bool(cur.fetchone()[0]):
            raise RuntimeError("ADOPTION_ROLLBACK_REPLACEMENT_CONFLICT")
    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,deactivated_at,git_revision,migration_version,
               container_revision
        FROM rollback_contract_adoption(%s,%s)
        """,
        (adoption_id, reason),
    )
    return _row(cur.fetchone())


def supersede_contract_adoption(
    cur,
    *,
    adoption_id: int,
    superseding_adoption_id: int,
    reason: str,
) -> ContractAdoption:
    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,deactivated_at,git_revision,migration_version,
               container_revision
        FROM supersede_contract_adoption(%s,%s,%s)
        """,
        (adoption_id, superseding_adoption_id, reason),
    )
    return _row(cur.fetchone())


def _transition_active_contract_adoption(
    cur,
    *,
    prepared_adoption_id: int,
    expected_current_active_adoption_id: int,
    expected_current_active_generation: int,
    expected_current_active_git_revision: str,
    expected_new_git_revision: str,
    expected_environment: str,
    expected_deployment_id: str,
    transition_reason: str,
    old_terminal_status: str,
) -> ContractAdoptionReplacement:
    """Atomically replace an explicitly identified ACTIVE generation.

    The caller owns the surrounding transaction. All validation happens while
    both lifecycle rows and the scope advisory lock are held. No mutation is
    issued before every expected old/new invariant has passed.
    """
    if (
        not str(transition_reason).strip()
        or old_terminal_status not in {"SUPERSEDED", "ROLLED_BACK"}
    ):
        raise RuntimeError("ADOPTION_REPLACEMENT_CONFLICT")

    lock_identity = (
        f"FEE_AWARE_INVENTORY_C2_2|{expected_environment}|"
        f"{expected_deployment_id}"
    )
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (lock_identity,),
    )
    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,deactivated_at,git_revision,migration_version,
               container_revision,supersedes_adoption_id
        FROM runtime_contract_adoption_v2
        WHERE adoption_id IN (%s,%s)
        ORDER BY adoption_id
        FOR UPDATE
        """,
        (
            int(expected_current_active_adoption_id),
            int(prepared_adoption_id),
        ),
    )
    rows = {int(row[0]): row for row in cur.fetchall()}
    old = rows.get(int(expected_current_active_adoption_id))
    new = rows.get(int(prepared_adoption_id))

    if (
        old is not None
        and new is not None
        and old[5] == old_terminal_status
        and new[5] == "ACTIVE"
        and new[11] == old[0]
        and old[2] == new[2] == expected_environment
        and old[3] == new[3] == expected_deployment_id
        and old[4] == expected_current_active_generation
        and old[8] == expected_current_active_git_revision
        and new[8] == expected_new_git_revision
    ):
        return ContractAdoptionReplacement(_row(new), "ALREADY_REPLACED")

    if (
        old is None
        or old[5] != "ACTIVE"
        or old[4] != expected_current_active_generation
        or old[8] != expected_current_active_git_revision
    ):
        raise RuntimeError("ADOPTION_REPLACEMENT_ACTIVE_MISMATCH")
    if new is None or new[5] != "PREPARED":
        raise RuntimeError("ADOPTION_REPLACEMENT_PREPARED_MISMATCH")
    if new[8] != expected_new_git_revision:
        raise RuntimeError("ADOPTION_REPLACEMENT_SHA_MISMATCH")
    if (
        old[1] != new[1]
        or old[1] != "FEE_AWARE_INVENTORY_C2_2"
        or old[2] != new[2]
        or old[3] != new[3]
        or old[2] != expected_environment
        or old[3] != expected_deployment_id
    ):
        raise RuntimeError("ADOPTION_REPLACEMENT_SCOPE_MISMATCH")
    if int(new[4]) <= int(old[4]):
        raise RuntimeError("ADOPTION_REPLACEMENT_GENERATION_ORDER_INVALID")

    cur.execute(
        """
        SELECT COUNT(*)
        FROM runtime_contract_adoption_v2
        WHERE contract_name=%s AND environment=%s AND deployment_id=%s
          AND status='ACTIVE' AND adoption_id<>%s
        """,
        (old[1], old[2], old[3], old[0]),
    )
    if int(cur.fetchone()[0]) != 0:
        raise RuntimeError("ADOPTION_REPLACEMENT_CONFLICT")

    cur.execute("SAVEPOINT c2_2_4_adoption_replacement")
    try:
        cur.execute("SELECT clock_timestamp()")
        transition_at = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE runtime_contract_adoption_v2
            SET status=%s,deactivated_at=%s,deactivation_reason=%s
            WHERE adoption_id=%s AND status='ACTIVE'
              AND generation=%s AND git_revision=%s
            """,
            (
                old_terminal_status,
                transition_at,
                str(transition_reason),
                old[0],
                old[4],
                old[8],
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError("ADOPTION_REPLACEMENT_CONFLICT")
        cur.execute(
            """
            UPDATE runtime_contract_adoption_v2
            SET status='ACTIVE',adopted_at=%s,supersedes_adoption_id=%s
            WHERE adoption_id=%s AND status='PREPARED'
              AND generation=%s AND git_revision=%s
            RETURNING adoption_id,contract_name,environment,deployment_id,
                      generation,status,adopted_at,deactivated_at,git_revision,
                      migration_version,container_revision
            """,
            (
                transition_at,
                old[0],
                new[0],
                new[4],
                new[8],
            ),
        )
        activated = cur.fetchone()
        if cur.rowcount != 1 or activated is None:
            raise RuntimeError("ADOPTION_REPLACEMENT_CONFLICT")
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT c2_2_4_adoption_replacement")
        cur.execute("RELEASE SAVEPOINT c2_2_4_adoption_replacement")
        if (
            isinstance(exc, RuntimeError)
            and str(exc) == "ADOPTION_REPLACEMENT_CONFLICT"
        ):
            raise
        raise RuntimeError("ADOPTION_REPLACEMENT_CONFLICT") from exc
    cur.execute("RELEASE SAVEPOINT c2_2_4_adoption_replacement")
    return ContractAdoptionReplacement(_row(activated), "REPLACED")


def replace_active_contract_adoption(
    cur,
    *,
    prepared_adoption_id: int,
    expected_current_active_adoption_id: int,
    expected_current_active_generation: int,
    expected_current_active_git_revision: str,
    expected_new_git_revision: str,
    expected_environment: str,
    expected_deployment_id: str,
    supersession_reason: str,
) -> ContractAdoptionReplacement:
    return _transition_active_contract_adoption(
        cur,
        prepared_adoption_id=prepared_adoption_id,
        expected_current_active_adoption_id=(
            expected_current_active_adoption_id
        ),
        expected_current_active_generation=(
            expected_current_active_generation
        ),
        expected_current_active_git_revision=(
            expected_current_active_git_revision
        ),
        expected_new_git_revision=expected_new_git_revision,
        expected_environment=expected_environment,
        expected_deployment_id=expected_deployment_id,
        transition_reason=supersession_reason,
        old_terminal_status="SUPERSEDED",
    )


def rollback_active_to_prepared_contract_adoption(
    cur,
    *,
    prepared_recovery_adoption_id: int,
    expected_failed_active_adoption_id: int,
    expected_failed_active_generation: int,
    expected_failed_active_git_revision: str,
    expected_recovery_git_revision: str,
    expected_environment: str,
    expected_deployment_id: str,
    rollback_reason: str,
) -> ContractAdoptionReplacement:
    """Atomically roll back an ACTIVE candidate into a new recovery generation.

    Historical SUPERSEDED generations are never reactivated.
    """
    return _transition_active_contract_adoption(
        cur,
        prepared_adoption_id=prepared_recovery_adoption_id,
        expected_current_active_adoption_id=(
            expected_failed_active_adoption_id
        ),
        expected_current_active_generation=(
            expected_failed_active_generation
        ),
        expected_current_active_git_revision=(
            expected_failed_active_git_revision
        ),
        expected_new_git_revision=expected_recovery_git_revision,
        expected_environment=expected_environment,
        expected_deployment_id=expected_deployment_id,
        transition_reason=rollback_reason,
        old_terminal_status="ROLLED_BACK",
    )

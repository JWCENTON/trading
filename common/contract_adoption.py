from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


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


def _row(value) -> ContractAdoption:
    return ContractAdoption(*value[:11])


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

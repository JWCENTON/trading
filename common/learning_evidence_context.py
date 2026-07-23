"""Strict transaction-local identity for Learning evidence persistence."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping


INSTANCE_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
MAX_INSTANCE_LENGTH = 63


def resolve_learning_evidence_identity(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str, str]:
    """Return instance, environment and derived deployment without normalization."""
    source = os.environ if environ is None else environ
    environment = source.get("ENVIRONMENT")
    if environment not in {"live", "paper"}:
        raise ValueError(
            "Learning evidence ENVIRONMENT must be exactly live or paper"
        )
    supplied_deployment = source.get("DEPLOYMENT_ID")
    instance_id = source.get("DEPLOYMENT_INSTANCE_ID")
    suffix = f"-{environment}"
    if instance_id is None:
        if not supplied_deployment or not supplied_deployment.endswith(suffix):
            raise ValueError("DEPLOYMENT_ID must end with the exact ENVIRONMENT suffix")
        instance_id = supplied_deployment[: -len(suffix)]
    if (
        not 1 <= len(instance_id) <= MAX_INSTANCE_LENGTH
        or INSTANCE_PATTERN.fullmatch(instance_id) is None
        or instance_id.endswith("-live")
        or instance_id.endswith("-paper")
    ):
        raise ValueError("DEPLOYMENT_INSTANCE_ID has invalid strict canonical syntax")
    deployment_id = f"{instance_id}-{environment}"
    if supplied_deployment is not None and supplied_deployment != deployment_id:
        raise ValueError("DEPLOYMENT_ID does not equal instance-environment identity")
    return instance_id, environment, deployment_id


def set_learning_evidence_transaction_context(cur) -> tuple[str, str, str]:
    """Set PostgreSQL GUCs locally; COMMIT/ROLLBACK clears both values."""
    instance_id, environment, deployment_id = resolve_learning_evidence_identity()
    cur.execute(
        """
        SELECT
            set_config('waltrade.deployment_instance_id', %s, true),
            set_config('waltrade.environment', %s, true)
        """,
        (instance_id, environment),
    )
    return instance_id, environment, deployment_id

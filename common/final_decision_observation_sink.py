"""Shared fail-open epilogue for finalized causal decision observations."""

from __future__ import annotations

import logging
from typing import Callable

from common.db import get_db_conn
from common.decision_contract import FinalDecision
from common.decision_observation_transport import (
    DurableDecisionObservationProducer,
    TransportFlags,
)


def finalize_decision_observation(
    decision: FinalDecision | None,
    *,
    source_service: str,
    connection_factory: Callable[[], object] = get_db_conn,
) -> FinalDecision | None:
    """Observe a finalized decision and return the identical object.

    Configuration is evaluated before a producer is constructed so the
    default-off and kill-switch paths cannot open a database connection.
    ``None`` is retained for legacy cycles that have no canonical decision
    identity (for example, no market row at all).
    """
    if decision is None:
        return None

    flags = TransportFlags.from_env()
    if not flags.decision_observation_enabled or flags.kill_switch:
        return decision

    try:
        producer = DurableDecisionObservationProducer(
            connection_factory,
            flags,
            source_service=source_service,
        )
        return producer.observe(decision)
    except Exception:
        # The producer is itself fail-open. This outer boundary also protects
        # trading from construction/configuration regressions in the epilogue.
        logging.exception(
            "FinalDecision causal observation sink failed; trading result unchanged",
            extra={"source_service": source_service},
        )
        return decision

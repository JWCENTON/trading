from __future__ import annotations

from dataclasses import dataclass, replace
import os
from typing import Callable

from common.financial_truth_calculator import (
    calculate_financial_truth,
    is_noncanonical_financial_truth_issue,
)
from common.financial_truth_repository import (
    CanonicalFinancialTruthWriteRepository,
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
    is_source_readiness_issue,
)


@dataclass(frozen=True)
class WriterActivation:
    enabled: bool
    mode: str
    environment: str
    allowlist: tuple[str, ...]

    @classmethod
    def from_environment(cls, environment: str):
        enabled = os.getenv("FINANCIAL_TRUTH_WRITER_ENABLED", "0") == "1"
        mode = os.getenv("FINANCIAL_TRUTH_WRITER_MODE", "disabled").lower()
        allowlist = tuple(
            item.strip().lower()
            for item in os.getenv(
                "FINANCIAL_TRUTH_WRITER_ENV_ALLOWLIST", "paper"
            ).split(",")
            if item.strip()
        )
        return cls(enabled, mode, environment.lower(), allowlist)

    def authorize(self, requested_mode: str) -> str:
        mode = requested_mode.lower()
        if mode in {"dry-run", "shadow"}:
            return mode
        if mode != "apply":
            raise RuntimeError("CONFIGURATION_ERROR: invalid writer mode")
        if not self.enabled or self.mode != "apply":
            raise RuntimeError("CONFIGURATION_ERROR: apply feature flag disabled")
        if self.environment not in self.allowlist:
            raise RuntimeError("CONFIGURATION_ERROR: environment not allowlisted")
        if self.environment != "paper":
            raise RuntimeError("CONFIGURATION_ERROR: LIVE apply denied by C2")
        return mode


class FinancialTruthReconciler:
    def __init__(self, connection_factory: Callable):
        self.connection_factory = connection_factory
        self.sources = FinancialTruthSourceRepository(connection_factory)

    def reconcile(
        self,
        position_id: int,
        *,
        requested_mode: str,
        evidence_context: ExecutionEvidenceContext,
        invocation_identity: str | None = None,
    ):
        if not isinstance(evidence_context, ExecutionEvidenceContext):
            raise TypeError("EXECUTION_EVIDENCE_CONTEXT_REQUIRED")
        activation = WriterActivation.from_environment(
            evidence_context.environment
        )
        if requested_mode == "disabled":
            return {"mode": "disabled", "calculated": False, "written": False}
        mode = activation.authorize(requested_mode)
        if mode in {"dry-run", "shadow"}:
            position, fills, source_issue = self.sources.read_position_and_fills(
                position_id, context=evidence_context
            )
            calculation = calculate_financial_truth(
                position_id=position[0], position_status=position[1], fills=fills,
                estimated_gross_pnl=position[2],
                estimated_fees_usdc=position[3],
                estimated_net_pnl=position[4],
            )
            if is_source_readiness_issue(source_issue):
                calculation = replace(
                    calculation,
                    failure_code=source_issue.value,
                    failure_detail=source_issue.value,
                )
            return {
                "mode": mode, "calculated": True, "written": False,
                "calculation": calculation,
            }
        conn = self.connection_factory()
        try:
            with conn:
                with conn.cursor() as cur:
                    CanonicalFinancialTruthWriteRepository.lock_position(
                        cur, position_id
                    )
                    position, fills, source_issue = (
                        self.sources.read_position_and_fills(
                            position_id, context=evidence_context, connection=conn
                        )
                    )
                    calculation = calculate_financial_truth(
                        position_id=position[0], position_status=position[1],
                        fills=fills, estimated_gross_pnl=position[2],
                        estimated_fees_usdc=position[3],
                        estimated_net_pnl=position[4],
                    )
                    if is_source_readiness_issue(source_issue):
                        calculation = replace(
                            calculation,
                            failure_code=source_issue.value,
                            failure_detail=source_issue.value,
                        )
                        return {
                            "mode": mode,
                            "calculated": True,
                            "written": False,
                            "calculation": calculation,
                        }
                    if is_noncanonical_financial_truth_issue(
                        calculation.failure_code
                    ):
                        return {
                            "mode": mode,
                            "calculated": True,
                            "written": False,
                            "calculation": calculation,
                        }
                    written = CanonicalFinancialTruthWriteRepository.write(
                        cur, calculation, invocation_type="CLI",
                        invocation_identity=invocation_identity,
                    )
            return {
                "mode": mode, "calculated": True, "written": written,
                "calculation": calculation,
            }
        finally:
            conn.close()

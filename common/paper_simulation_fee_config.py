from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import os
from typing import Mapping


PAPER_SIMULATION_FEE_RATE_ENV = "PAPER_SIMULATION_FEE_RATE"
LEGACY_PAPER_FEE_RATE_ENV = "PAPER_FEE_RATE"
LEGACY_DEFAULT_RATE = Decimal("0.0004")
FEE_MODEL_V1 = "PAPER_SIMULATOR_FINANCIAL_MODEL_V1"
FEE_MODEL_V2 = "PAPER_SIMULATOR_FINANCIAL_MODEL_V2"


@dataclass(frozen=True)
class PaperSimulationFeeConfig:
    rate: Decimal
    model_version: str
    config_source: str


def _rate(value: str, source: str) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"INVALID_PAPER_SIMULATION_FEE_RATE:{source}") from exc
    if not parsed.is_finite() or parsed < 0 or parsed > Decimal("0.10"):
        raise RuntimeError(f"INVALID_PAPER_SIMULATION_FEE_RATE:{source}")
    return parsed


def load_paper_simulation_fee_config(
    environ: Mapping[str, str] | None = None,
) -> PaperSimulationFeeConfig:
    values = os.environ if environ is None else environ
    configured = values.get(PAPER_SIMULATION_FEE_RATE_ENV)
    if configured is not None:
        if not str(configured).strip():
            raise RuntimeError("EMPTY_PAPER_SIMULATION_FEE_RATE")
        return PaperSimulationFeeConfig(
            rate=_rate(configured, PAPER_SIMULATION_FEE_RATE_ENV),
            model_version=FEE_MODEL_V2,
            config_source=f"ENV:{PAPER_SIMULATION_FEE_RATE_ENV}",
        )

    legacy = values.get(LEGACY_PAPER_FEE_RATE_ENV)
    if legacy is not None:
        if not str(legacy).strip():
            raise RuntimeError("EMPTY_LEGACY_PAPER_FEE_RATE")
        return PaperSimulationFeeConfig(
            rate=_rate(legacy, LEGACY_PAPER_FEE_RATE_ENV),
            model_version=FEE_MODEL_V1,
            config_source=f"LEGACY_ENV:{LEGACY_PAPER_FEE_RATE_ENV}",
        )

    return PaperSimulationFeeConfig(
        rate=LEGACY_DEFAULT_RATE,
        model_version=FEE_MODEL_V1,
        config_source="LEGACY_DEFAULT:0.0004",
    )

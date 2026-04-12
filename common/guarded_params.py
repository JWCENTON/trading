from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class GuardedProfitConfig:
    enabled: bool
    min_age_minutes: int
    mfe_arm_090_abs: float
    floor_090_abs: float
    mfe_arm_110_abs: float
    floor_110_abs: float


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def guarded_profit_defaults_map() -> Dict[str, float]:
    return {
        "GUARDED_PROFIT_ENABLED": 0.0,
        "GUARDED_MIN_AGE_MINUTES": 10.0,
        "GUARDED_MFE_ARM_090_ABS": 0.90,
        "GUARDED_FLOOR_090_ABS": 0.70,
        "GUARDED_MFE_ARM_110_ABS": 1.10,
        "GUARDED_FLOOR_110_ABS": 1.00,
    }


def parse_guarded_profit_config(params: Dict[str, float]) -> GuardedProfitConfig:
    defaults = guarded_profit_defaults_map()

    def getf(name: str) -> float:
        raw = params.get(name, defaults[name])
        return float(raw)

    enabled = getf("GUARDED_PROFIT_ENABLED") >= 1.0
    min_age_minutes = int(round(_clamp(getf("GUARDED_MIN_AGE_MINUTES"), 0.0, 1440.0)))

    mfe_arm_090_abs = _clamp(getf("GUARDED_MFE_ARM_090_ABS"), 0.0, 1_000_000.0)
    floor_090_abs = _clamp(getf("GUARDED_FLOOR_090_ABS"), 0.0, 1_000_000.0)
    mfe_arm_110_abs = _clamp(getf("GUARDED_MFE_ARM_110_ABS"), 0.0, 1_000_000.0)
    floor_110_abs = _clamp(getf("GUARDED_FLOOR_110_ABS"), 0.0, 1_000_000.0)

    # bezpieczeństwo relacji bucketów
    floor_090_abs = min(floor_090_abs, mfe_arm_090_abs)
    floor_110_abs = min(floor_110_abs, mfe_arm_110_abs)

    if mfe_arm_110_abs < mfe_arm_090_abs:
        mfe_arm_110_abs = mfe_arm_090_abs
    if floor_110_abs < floor_090_abs:
        floor_110_abs = floor_090_abs

    return GuardedProfitConfig(
        enabled=enabled,
        min_age_minutes=min_age_minutes,
        mfe_arm_090_abs=mfe_arm_090_abs,
        floor_090_abs=floor_090_abs,
        mfe_arm_110_abs=mfe_arm_110_abs,
        floor_110_abs=floor_110_abs,
    )

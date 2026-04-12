from dataclasses import dataclass
from typing import Optional

from common.guarded_params import GuardedProfitConfig
from common.position_path import PositionPathSnapshot


@dataclass(frozen=True)
class GuardedProfitDecision:
    triggered: bool
    reason_code: Optional[str]
    guard_bucket: Optional[str]
    mfe_abs: float
    floor_abs: Optional[float]
    current_move_abs: float
    age_minutes: float


def evaluate_guarded_profit(
    side: str,
    age_minutes: float,
    entry_price: float,
    path: PositionPathSnapshot,
    config: GuardedProfitConfig,
) -> GuardedProfitDecision:
    if not config.enabled:
        return GuardedProfitDecision(
            triggered=False,
            reason_code=None,
            guard_bucket=None,
            mfe_abs=float(path.mfe_abs),
            floor_abs=None,
            current_move_abs=float(path.current_close - entry_price),
            age_minutes=float(age_minutes),
        )

    if (side or "").upper() != "LONG":
        return GuardedProfitDecision(
            triggered=False,
            reason_code="SIDE_NOT_SUPPORTED",
            guard_bucket=None,
            mfe_abs=float(path.mfe_abs),
            floor_abs=None,
            current_move_abs=float(path.current_close - entry_price),
            age_minutes=float(age_minutes),
        )

    current_move_abs = float(path.current_close - entry_price)
    mfe_abs = float(path.mfe_abs)

    if age_minutes < float(config.min_age_minutes):
        return GuardedProfitDecision(
            triggered=False,
            reason_code="MIN_AGE_NOT_MET",
            guard_bucket=None,
            mfe_abs=mfe_abs,
            floor_abs=None,
            current_move_abs=current_move_abs,
            age_minutes=float(age_minutes),
        )

    guard_bucket = None
    floor_abs = None

    if mfe_abs >= float(config.mfe_arm_110_abs):
        guard_bucket = "GUARD_110"
        floor_abs = float(config.floor_110_abs)
    elif mfe_abs >= float(config.mfe_arm_090_abs):
        guard_bucket = "GUARD_090"
        floor_abs = float(config.floor_090_abs)

    if guard_bucket is None or floor_abs is None:
        return GuardedProfitDecision(
            triggered=False,
            reason_code="NOT_ARMED",
            guard_bucket=None,
            mfe_abs=mfe_abs,
            floor_abs=None,
            current_move_abs=current_move_abs,
            age_minutes=float(age_minutes),
        )

    triggered = current_move_abs <= floor_abs

    return GuardedProfitDecision(
        triggered=triggered,
        reason_code="GUARDED_PROFIT_LONG" if triggered else "ARMED_WAITING",
        guard_bucket=guard_bucket,
        mfe_abs=mfe_abs,
        floor_abs=float(floor_abs),
        current_move_abs=current_move_abs,
        age_minutes=float(age_minutes),
    )

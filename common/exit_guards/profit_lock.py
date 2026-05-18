import os
from dataclasses import dataclass
from typing import Optional, Set

from common.position_path import PositionPathSnapshot


DEFAULT_PROFIT_LOCK_STRATEGIES = "RSI,TREND,SUPERTREND,BBRANGE"
LEGACY_STRATEGY_ALIASES = {
    "SUPER_TREND": "SUPERTREND",
}


def normalize_strategy_name(strategy: str) -> str:
    """
    Normalize legacy strategy names for runtime reads only.

    TECH-DEBT: SUPER_TREND is an old spelling. Do not add new config/runtime
    rows under SUPER_TREND; migrate runtime/cache rows to SUPERTREND separately.
    Historical audit rows can remain unchanged.
    """
    s = (strategy or "").strip().upper()
    return LEGACY_STRATEGY_ALIASES.get(s, s)


def parse_strategy_set(value: str) -> Set[str]:
    return {
        normalize_strategy_name(part)
        for part in (value or "").split(",")
        if part.strip()
    }


@dataclass(frozen=True)
class ProfitLockConfig:
    enabled: bool
    strategies: Set[str]
    arm_pct: float
    floor_pct: float
    trail_drop_pct: float
    min_age_minutes: float

    @staticmethod
    def from_env() -> "ProfitLockConfig":
        return ProfitLockConfig(
            enabled=os.environ.get("PROFIT_LOCK_ENABLED", "1") == "1",
            strategies=parse_strategy_set(os.environ.get("PROFIT_LOCK_STRATEGIES", DEFAULT_PROFIT_LOCK_STRATEGIES)),
            arm_pct=float(os.environ.get("PROFIT_LOCK_ARM_PCT", "0.30")),
            floor_pct=float(os.environ.get("PROFIT_LOCK_FLOOR_PCT", "0.08")),
            trail_drop_pct=float(os.environ.get("PROFIT_LOCK_TRAIL_DROP_PCT", "0.20")),
            min_age_minutes=float(os.environ.get("PROFIT_LOCK_MIN_AGE_MINUTES", "2")),
        )


@dataclass(frozen=True)
class ProfitLockDecision:
    triggered: bool
    reason_code: Optional[str]
    trigger_type: Optional[str]
    peak_move_pct: float
    current_move_pct: float
    floor_pct: float
    trail_drop_pct: float
    age_minutes: float


def evaluate_profit_lock(
    *,
    strategy: str,
    side: str,
    age_minutes: float,
    entry_price: float,
    current_price: float,
    path: PositionPathSnapshot,
    config: ProfitLockConfig,
) -> ProfitLockDecision:
    side_u = (side or "").upper()
    strategy_u = normalize_strategy_name(strategy)

    if not config.enabled:
        return ProfitLockDecision(False, "DISABLED", None, 0.0, 0.0, config.floor_pct, config.trail_drop_pct, float(age_minutes))
    if strategy_u not in config.strategies:
        return ProfitLockDecision(False, "STRATEGY_NOT_ENABLED", None, 0.0, 0.0, config.floor_pct, config.trail_drop_pct, float(age_minutes))
    if age_minutes < float(config.min_age_minutes):
        return ProfitLockDecision(False, "MIN_AGE_NOT_MET", None, 0.0, 0.0, config.floor_pct, config.trail_drop_pct, float(age_minutes))
    if entry_price <= 0:
        return ProfitLockDecision(False, "BAD_ENTRY_PRICE", None, 0.0, 0.0, config.floor_pct, config.trail_drop_pct, float(age_minutes))

    if side_u == "LONG":
        peak_move_pct = (float(path.max_high) - float(entry_price)) / float(entry_price) * 100.0
        current_move_pct = (float(current_price) - float(entry_price)) / float(entry_price) * 100.0
        reason_code = "PROFIT_LOCK_LONG"
    else:
        peak_move_pct = (float(entry_price) - float(path.min_low)) / float(entry_price) * 100.0
        current_move_pct = (float(entry_price) - float(current_price)) / float(entry_price) * 100.0
        reason_code = "PROFIT_LOCK_SHORT"

    if peak_move_pct < float(config.arm_pct):
        return ProfitLockDecision(False, "NOT_ARMED", None, peak_move_pct, current_move_pct, config.floor_pct, config.trail_drop_pct, float(age_minutes))

    if current_move_pct <= float(config.floor_pct):
        return ProfitLockDecision(True, reason_code, "FLOOR", peak_move_pct, current_move_pct, config.floor_pct, config.trail_drop_pct, float(age_minutes))

    if (peak_move_pct - current_move_pct) >= float(config.trail_drop_pct):
        return ProfitLockDecision(True, reason_code, "TRAIL_DROP", peak_move_pct, current_move_pct, config.floor_pct, config.trail_drop_pct, float(age_minutes))

    return ProfitLockDecision(False, "ARMED_WAITING", None, peak_move_pct, current_move_pct, config.floor_pct, config.trail_drop_pct, float(age_minutes))

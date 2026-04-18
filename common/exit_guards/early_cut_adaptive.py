from dataclasses import dataclass


@dataclass(frozen=True)
class AdaptiveEarlyCutConfig:
    enabled: bool = True
    min_age_minutes: int = 45
    atr_mult: float = 1.0
    weak_mfe_atr_mult: float = 0.5


@dataclass(frozen=True)
class AdaptiveEarlyCutDecision:
    enabled: bool
    supported: bool
    would_cut: bool
    weak_trade: bool
    loss_breach: bool
    age_minutes: float
    min_age_minutes: int
    mfe_abs: float
    mae_abs: float
    current_move_abs: float
    current_move_pct: float
    atr_abs: float
    atr_pct: float
    adaptive_loss_abs: float
    adaptive_loss_pct: float
    weak_mfe_threshold_abs: float
    reason_code: str | None = None


def evaluate_adaptive_early_cut_long(
    *,
    age_minutes: float,
    entry_price: float,
    current_price: float,
    mfe_abs: float,
    mae_abs: float,
    atr_abs: float | None,
    config: AdaptiveEarlyCutConfig,
) -> AdaptiveEarlyCutDecision:
    current_move_abs = float(current_price - entry_price)
    current_move_pct = (current_move_abs / entry_price) * 100.0 if entry_price else 0.0
    atr_abs_value = float(atr_abs or 0.0)
    atr_pct = (atr_abs_value / entry_price) * 100.0 if entry_price else 0.0

    if not config.enabled:
        return AdaptiveEarlyCutDecision(
            enabled=False,
            supported=True,
            would_cut=False,
            weak_trade=False,
            loss_breach=False,
            age_minutes=float(age_minutes),
            min_age_minutes=int(config.min_age_minutes),
            mfe_abs=float(mfe_abs),
            mae_abs=float(mae_abs),
            current_move_abs=float(current_move_abs),
            current_move_pct=float(current_move_pct),
            atr_abs=atr_abs_value,
            atr_pct=float(atr_pct),
            adaptive_loss_abs=0.0,
            adaptive_loss_pct=0.0,
            weak_mfe_threshold_abs=0.0,
            reason_code="DISABLED",
        )

    if entry_price <= 0 or atr_abs is None or atr_abs_value <= 0:
        return AdaptiveEarlyCutDecision(
            enabled=True,
            supported=False,
            would_cut=False,
            weak_trade=False,
            loss_breach=False,
            age_minutes=float(age_minutes),
            min_age_minutes=int(config.min_age_minutes),
            mfe_abs=float(mfe_abs),
            mae_abs=float(mae_abs),
            current_move_abs=float(current_move_abs),
            current_move_pct=float(current_move_pct),
            atr_abs=atr_abs_value,
            atr_pct=float(atr_pct),
            adaptive_loss_abs=0.0,
            adaptive_loss_pct=0.0,
            weak_mfe_threshold_abs=0.0,
            reason_code="ATR_UNAVAILABLE",
        )

    adaptive_loss_abs = float(config.atr_mult) * atr_abs_value
    adaptive_loss_pct = (adaptive_loss_abs / entry_price) * 100.0

    weak_mfe_threshold_abs = float(config.weak_mfe_atr_mult) * atr_abs_value
    weak_trade = float(mfe_abs) < weak_mfe_threshold_abs
    loss_breach = current_move_abs <= -adaptive_loss_abs
    age_ok = float(age_minutes) >= float(config.min_age_minutes)

    would_cut = age_ok and weak_trade and loss_breach

    reason_code = None
    if not age_ok:
        reason_code = "AGE_NOT_REACHED"
    elif not weak_trade:
        reason_code = "TRADE_NOT_WEAK"
    elif not loss_breach:
        reason_code = "LOSS_NOT_BREACHED"
    elif would_cut:
        reason_code = "CUT"

    return AdaptiveEarlyCutDecision(
        enabled=True,
        supported=True,
        would_cut=bool(would_cut),
        weak_trade=bool(weak_trade),
        loss_breach=bool(loss_breach),
        age_minutes=float(age_minutes),
        min_age_minutes=int(config.min_age_minutes),
        mfe_abs=float(mfe_abs),
        mae_abs=float(mae_abs),
        current_move_abs=float(current_move_abs),
        current_move_pct=float(current_move_pct),
        atr_abs=float(atr_abs_value),
        atr_pct=float(atr_pct),
        adaptive_loss_abs=float(adaptive_loss_abs),
        adaptive_loss_pct=float(adaptive_loss_pct),
        weak_mfe_threshold_abs=float(weak_mfe_threshold_abs),
        reason_code=reason_code,
    )

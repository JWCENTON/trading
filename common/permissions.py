# common/permissions.py
from typing import Tuple, Dict
from .runtime import RuntimeConfig

def can_trade(cfg: RuntimeConfig, *, regime_allows_trade: bool) -> Tuple[bool, Dict]:
    """
    Jedno miejsce prawdy:
    PAPER -> zawsze True (symulacja)
    LIVE  -> True tylko gdy LIVE_ORDERS_ENABLED=1 i regime_allows_trade=True
    """
    if cfg.trading_mode == "PAPER":
        return True, {"why": "paper_mode"}

    if cfg.trading_mode == "LIVE":
        if not cfg.live_orders_enabled:
            return False, {"why": "live_orders_disabled"}
        if not regime_allows_trade:
            return False, {"why": "regime_block"}
        return True, {"why": "live_allowed"}

    return False, {"why": f"unknown_trading_mode:{cfg.trading_mode}"}

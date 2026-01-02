# common/permissions.py
from typing import Tuple, Dict
from .runtime import RuntimeConfig

def can_trade(cfg: RuntimeConfig, *, regime_allows_trade: bool, panic_disable_trading: bool = False):
    if cfg.trading_mode == "PAPER":
        return True, {"why": "paper_mode"}

    if cfg.trading_mode == "LIVE":
        if panic_disable_trading:
            return False, {"why": "panic_disable_trading"}
        if not cfg.live_orders_enabled:
            return False, {"why": "live_orders_disabled"}
        if not regime_allows_trade:
            return False, {"why": "regime_block"}
        return True, {"why": "live_allowed"}

    return False, {"why": f"unknown_trading_mode:{cfg.trading_mode}"}
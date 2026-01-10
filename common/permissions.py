# common/permissions.py
from .runtime import RuntimeConfig

def can_trade(
    cfg: RuntimeConfig,
    *,
    regime_allows_trade: bool,
    is_exit: bool = False,
    panic_disable_trading: bool = False,
):
    """
    Policy:
    - PAPER: always allow (orders are simulated anyway)
    - LIVE:
        - PANIC: block everything (entry and exit)
        - live_orders_enabled:
            - blocks ENTRY
            - does NOT block EXIT (so we can always flatten risk)
        - regime_allows_trade: blocks ENTRY only (exit should not be blocked by regime)
    """

    # PAPER
    if cfg.trading_mode == "PAPER":
        return True, {"why": "paper_mode"}

    # LIVE
    if cfg.trading_mode == "LIVE":
        if panic_disable_trading:
            return False, {"why": "panic_disable_trading"}

        # Allow exits even if live_orders are disabled
        if not cfg.live_orders_enabled:
            if is_exit:
                return True, {"why": "exit_allowed_while_live_disabled"}
            return False, {"why": "live_orders_disabled"}

        # Regime gate should never block exits
        if not regime_allows_trade and not is_exit:
            return False, {"why": "regime_block"}

        return True, {"why": "live_allowed"}

    return False, {"why": f"unknown_trading_mode:{cfg.trading_mode}"}
import os

def env_flag(name: str, default: str = "0") -> bool:
    """
    Returns True if env var is one of: 1, true, yes, on (case-insensitive).
    """
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def trading_mode() -> str:
    return (os.getenv("TRADING_MODE", "PAPER") or "PAPER").strip().upper()

def is_live_mode() -> bool:
    return trading_mode() == "LIVE"

def binance_mytrades_enabled() -> bool:
    # Safety: only allow in LIVE, AND only if flag explicitly enabled.
    return is_live_mode() and env_flag("BINANCE_MYTRADES_ENABLED", "0")

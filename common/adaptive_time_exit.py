import os


def _flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def hard_time_exit_enabled() -> bool:
    """
    Hard time exit is OFF by default.

    Time may be used as diagnostics/watchdog, but normal exits should come from:
    - profit lock
    - guarded profit
    - trailing/floor
    - regime flip
    - momentum fade
    - adaptive SL/ATR
    - entry thesis invalidation

    Set TIME_EXIT_FORCE_ENABLED=1 only for emergency/testing.
    """
    return _flag("TIME_EXIT_FORCE_ENABLED", "0")


def time_exit_policy_name() -> str:
    return os.getenv("TIME_EXIT_POLICY", "DIAGNOSTIC_ONLY").strip().upper() or "DIAGNOSTIC_ONLY"

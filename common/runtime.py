# common/runtime.py
from dataclasses import dataclass
import os


class TradingModeConfigurationError(ValueError):
    """Raised when the required TRADING_MODE is missing or invalid."""


def normalize_trading_mode(value) -> str:
    """Return the canonical LIVE/PAPER mode or fail closed."""
    mode = str(value or "").strip().upper()
    if mode not in {"LIVE", "PAPER"}:
        raise TradingModeConfigurationError(
            "TRADING_MODE must be explicitly set to LIVE or PAPER"
        )
    return mode


def trading_mode_from_env() -> str:
    return normalize_trading_mode(os.environ.get("TRADING_MODE"))


@dataclass(frozen=True)
class RuntimeConfig:
    symbol: str
    interval: str
    strategy: str
    trading_mode: str          # PAPER | LIVE
    live_orders_enabled: bool  # 0/1
    spot_mode: bool
    quote_asset: str
    regime_enabled: bool
    regime_mode: str           # DRY_RUN | ENFORCE

    @staticmethod
    def from_env() -> "RuntimeConfig":
        quote = os.environ.get("QUOTE_ASSET", "USDC").upper()
        symbol = os.environ.get("SYMBOL", "BTCUSDT").upper()

        return RuntimeConfig(
            symbol=symbol,
            interval=os.environ.get("INTERVAL", "1m"),
            strategy=os.environ.get("STRATEGY_NAME", "RSI").upper(),
            trading_mode=trading_mode_from_env(),
            live_orders_enabled=(os.environ.get("LIVE_ORDERS_ENABLED", "0") == "1"),
            spot_mode=(os.environ.get("SPOT_MODE", "1") == "1"),
            quote_asset=quote,
            regime_enabled=(os.environ.get("REGIME_ENABLED", "0") == "1"),
            regime_mode=os.environ.get("REGIME_MODE", "DRY_RUN").strip().upper(),
        )

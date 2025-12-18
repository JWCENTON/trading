import os
import logging

TRADING_MODE = os.environ.get("TRADING_MODE", "PAPER").upper()
LIVE_ORDERS_ENABLED = os.environ.get("LIVE_ORDERS_ENABLED", "0") == "1"

def place_live_order(client, symbol: str, side: str, qty: float):
    if TRADING_MODE != "LIVE":
        return None

    if not LIVE_ORDERS_ENABLED:
        logging.warning("LIVE ORDER BLOCKED (LIVE_ORDERS_ENABLED=0) symbol=%s side=%s qty=%.8f", symbol, side, qty)
        return None

    # docelowo:
    # return client.create_order(symbol=symbol, side=side, type="MARKET", quantity=qty)
    raise NotImplementedError("LIVE_ORDERS_ENABLED=1 but live order path not implemented yet")
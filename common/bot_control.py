# common/bot_control.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import logging

from common.db import get_db_conn

@dataclass(frozen=True)
class BotControl:
    enabled: bool
    mode: str
    live_orders_enabled: bool
    regime_enabled: bool
    regime_mode: str
    updated_at: Optional[datetime]

DEFAULT = BotControl(
    enabled=True,
    mode="NORMAL",
    live_orders_enabled=False,
    regime_enabled=False,
    regime_mode="DRY_RUN",
    updated_at=None,
)

def upsert_defaults(symbol: str, strategy: str, interval: str, *, cfg) -> None:
    """
    Gwarantuje, że rekord istnieje. Ustawia sensowne wartości startowe,
    ale nie nadpisuje ręcznych ustawień użytkownika.
    """
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_control(symbol, strategy, interval, mode, enabled, live_orders_enabled, regime_enabled, regime_mode, updated_at)
        VALUES (%s, %s, %s, 'NORMAL', TRUE, %s, %s, %s, now())
        ON CONFLICT (symbol, strategy, interval) DO NOTHING;
        """,
        (
            symbol, strategy, interval,
            bool(getattr(cfg, "live_orders_enabled", False)),
            bool(getattr(cfg, "regime_enabled", False)),
            str(getattr(cfg, "regime_mode", "DRY_RUN")).upper(),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

def read(symbol: str, strategy: str, interval: str) -> BotControl:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT enabled, mode, live_orders_enabled, regime_enabled, regime_mode, updated_at
        FROM bot_control
        WHERE symbol=%s AND strategy=%s AND interval=%s
        """,
        (symbol, strategy, interval),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return DEFAULT

    enabled, mode, loe, re, rm, updated_at = row
    return BotControl(
        enabled=bool(enabled),
        mode=str(mode or "NORMAL").upper(),
        live_orders_enabled=bool(loe),
        regime_enabled=bool(re),
        regime_mode=str(rm or "DRY_RUN").upper(),
        updated_at=updated_at,
    )
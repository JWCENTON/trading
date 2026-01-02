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
    reason: Optional[str]
    live_orders_enabled: bool
    regime_enabled: bool
    regime_mode: str
    updated_at: Optional[datetime]

DEFAULT = BotControl(
    enabled=True,
    mode="NORMAL",
    reason=None,
    live_orders_enabled=False,
    regime_enabled=False,
    regime_mode="DRY_RUN",
    updated_at=None,
)

def upsert_defaults(symbol: str, strategy: str, interval: str) -> None:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_control(symbol, strategy, interval, mode, enabled, reason, live_orders_enabled, regime_enabled, regime_mode, updated_at)
        VALUES (%s, %s, %s, 'NORMAL', TRUE, NULL, FALSE, FALSE, 'DRY_RUN', now())
        ON CONFLICT (symbol, strategy, interval) DO NOTHING;
        """,
        (symbol, strategy, interval),
    )
    conn.commit()
    cur.close()
    conn.close()

def read(symbol: str, strategy: str, interval: str) -> BotControl:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT enabled, mode, reason, live_orders_enabled, regime_enabled, regime_mode, updated_at
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

    enabled, mode, reason, loe, re, rm, updated_at = row[:7]

    return BotControl(
        enabled=bool(enabled),
        mode=str(mode or "NORMAL").upper(),
        reason=str(reason) if reason is not None else None,
        live_orders_enabled=bool(loe),
        regime_enabled=bool(re),
        regime_mode=str(rm or "DRY_RUN").upper(),
        updated_at=updated_at,
    )
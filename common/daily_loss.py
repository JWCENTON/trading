# common/daily_loss.py
import os
import psycopg2
from datetime import datetime, timezone

def _get_db_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "db"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "trading"),
        user=os.environ.get("DB_USER", "botuser"),
        password=os.environ.get("DB_PASS", "botpass"),
    )

def compute_daily_realized_pnl_quote_positions(symbol: str, interval: str, strategy: str) -> float:
    """
    Realized PnL today (UTC) computed from positions table for CLOSED trades only.
    Returns quote currency PnL (e.g., USDC).
    """
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        WITH day AS (SELECT date_trunc('day', now() AT TIME ZONE 'UTC') AS d0)
        SELECT
          COALESCE(SUM(
            CASE
              WHEN side IN ('LONG','BUY') THEN (exit_price-entry_price)*qty
              WHEN side IN ('SHORT','SELL') THEN (entry_price-exit_price)*qty
              ELSE 0
            END
          ), 0)
        FROM positions, day
        WHERE symbol=%s AND interval=%s AND strategy=%s
          AND status='CLOSED'
          AND exit_time >= day.d0;
        """,
        (symbol, interval, strategy),
    )
    v = cur.fetchone()[0]
    cur.close()
    conn.close()
    return float(v or 0.0)

def compute_daily_loss_pct_positions(symbol: str, interval: str, strategy: str, *, base_usdc: float) -> dict:
    """
    Returns dict payload suitable for strategy_events.info:
      {
        daily_pct: float,
        realized_today_quote: float,
        base_usdc: float,
        source: "positions_realized_only_v1",
      }
    """
    realized = compute_daily_realized_pnl_quote_positions(symbol, interval, strategy)
    b = float(base_usdc or 0.0)
    daily_pct = 0.0 if b <= 0 else (realized / b) * 100.0
    return {
        "daily_pct": float(daily_pct),
        "realized_today_quote": float(realized),
        "base_usdc": float(b),
        "source": "positions_realized_only_v1",
    }

def should_block_daily_loss_positions(*, daily_pct: float, limit_pct: float) -> bool:
    """
    limit_pct is positive number, e.g. 0.5 means -0.5% is block threshold.
    """
    if limit_pct <= 0:
        return False
    return float(daily_pct) <= -float(limit_pct)

import os

def should_emit_daily_loss_shadow(*, strategy: str) -> bool:
    """
    Opcja A: tylko jeden sentinel emituje DAILY_MAX_LOSS_POSITIONS_SHADOW.
    Sterowanie env:
      - DAILY_LOSS_SHADOW_SENTINEL=1  -> ten proces emituje
      - DAILY_LOSS_SHADOW_STRATEGY_ALLOWLIST="TREND,REGIME_WORKER" (opcjonalnie)
    Default: False (czyli strategie nie spamują).
    """
    if os.environ.get("DAILY_LOSS_SHADOW_SENTINEL", "0") == "1":
        return True

    allow = os.environ.get("DAILY_LOSS_SHADOW_STRATEGY_ALLOWLIST", "").strip()
    if not allow:
        return False

    allowed = {x.strip().upper() for x in allow.split(",") if x.strip()}
    return str(strategy or "").upper() in allowed
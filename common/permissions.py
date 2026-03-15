# common/permissions.py
from .runtime import RuntimeConfig
from common.db import get_db_conn
from dataclasses import dataclass
from typing import Optional
import psycopg2.extras


@dataclass
class RuntimePerms:
    panic_enabled: bool
    live_orders_enabled: bool
    regime_enabled: bool
    regime_mode: str
    reason: Optional[str] = None

def load_runtime_perms(conn, *, symbol: str, interval: str, strategy: str) -> RuntimePerms:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT panic_enabled FROM panic_state WHERE id=true;")
        p = cur.fetchone()
        panic_enabled = bool(p["panic_enabled"]) if p else False

        cur.execute("""
            SELECT live_orders_enabled, regime_enabled, regime_mode, reason
            FROM bot_control
            WHERE symbol=%s AND interval=%s AND strategy=%s
        """, (symbol, interval, strategy))
        r = cur.fetchone()
        if not r:
            # safest default: no live
            return RuntimePerms(
                panic_enabled=panic_enabled,
                live_orders_enabled=False,
                regime_enabled=False,
                regime_mode="DRY_RUN",
                reason="missing bot_control row",
            )

        return RuntimePerms(
            panic_enabled=panic_enabled,
            live_orders_enabled=bool(r["live_orders_enabled"]),
            regime_enabled=bool(r["regime_enabled"]),
            regime_mode=(r["regime_mode"] or "DRY_RUN"),
            reason=r.get("reason"),
        )
    

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
    # PANIC (DB) blocks ENTRY, allows EXIT
    panic_enabled, panic_reason = get_panic_state()
    if panic_enabled and not is_exit:
        return False, {"why": "PANIC_ENABLED", "panic_reason": panic_reason}

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


def is_panic_enabled() -> tuple[bool, str]:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT panic_enabled, COALESCE(reason,'') FROM panic_state WHERE id=true;")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return False, ""
    return bool(row[0]), str(row[1])


def get_panic_state() -> tuple[bool, str]:
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT panic_enabled, COALESCE(reason,'') FROM panic_state WHERE id=true;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return False, ""
        return bool(row[0]), str(row[1])
    except Exception:
        # fail-open: jeśli DB padła, nie zabijamy bota samym checkiem
        return False, "panic_state_check_failed"
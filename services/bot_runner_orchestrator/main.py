import os
import time
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Tuple, Any, Optional, List

import psycopg2
import psycopg2.extras


# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [orchestrator_v1] %(message)s",
)


# -------------------------
# Types
# -------------------------
@dataclass(frozen=True)
class BotKey:
    symbol: str
    interval: str
    strategy: str


# -------------------------
# DB
# -------------------------
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")


def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def fetch_all_dict(cur) -> List[dict]:
    rows = cur.fetchall()
    return [dict(r) for r in rows]


# -------------------------
# Config from automation_kv
# -------------------------
def load_orc_cfg(conn) -> dict:
    """
    Reads config from automation_kv.
    Provides defaults if keys are missing.
    """
    defaults = {
        "hb_max_lag_s": 420,
        "candles_max_lag_s": 420,
        "unrealized_in_risk": True,
        "exposure_max_quote": Decimal("25"),
        "daily_max_loss_quote": None,  # Decimal("2.5") recommended for LIVE micro; None disables this check
        "policy_version": os.getenv("BOT_VERSION", "orc_v1"),
        "actions_enabled": False,
    }

    key_map = {
        "orc_v1_hb_max_lag_s": ("hb_max_lag_s", int),
        "orc_v1_candles_max_lag_s": ("candles_max_lag_s", int),
        "orc_v1_unrealized_in_risk": ("unrealized_in_risk", lambda v: str(v).lower() in ("1", "true", "yes", "y", "on")),
        "orc_v1_exposure_max_quote": ("exposure_max_quote", Decimal),
        "orc_v1_daily_max_loss_quote": ("daily_max_loss_quote", Decimal),
        "orc_v1_policy_version": ("policy_version", str),
        "orc_v1_actions_enabled": ("actions_enabled", lambda v: str(v).lower() in ("1","true","yes","y","on")),
    }

    cfg = dict(defaults)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT key, value FROM automation_kv WHERE key LIKE 'orc_v1_%'")
        for r in fetch_all_dict(cur):
            k = r["key"]
            v = r["value"]
            if k in key_map:
                out_key, cast_fn = key_map[k]
                try:
                    cfg[out_key] = cast_fn(v)
                except Exception:
                    logging.warning("Bad cfg value for %s=%r; keeping default=%r", k, v, cfg[out_key])

    return cfg


# -------------------------
# Data loaders
# -------------------------
def get_panic(conn) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT panic_enabled, reason, updated_at FROM panic_state WHERE id = true")
        row = cur.fetchone()
        if not row:
            return {"panic_enabled": False, "reason": None, "updated_at": None}
        return dict(row)


def load_bot_control(conn) -> Dict[BotKey, dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy,
                   enabled, live_orders_enabled, regime_enabled, regime_mode,
                   reason AS control_reason, updated_at AS control_updated_at
            FROM bot_control
        """)
        out: Dict[BotKey, dict] = {}
        for r in fetch_all_dict(cur):
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out


def load_heartbeat(conn) -> Dict[BotKey, dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy,
                   last_seen AS hb_last_seen,
                   EXTRACT(EPOCH FROM (now() - last_seen))::int AS hb_lag_s,
                   info AS hb_info
            FROM bot_heartbeat
        """)
        out: Dict[BotKey, dict] = {}
        for r in fetch_all_dict(cur):
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out


def load_candles_latest(conn) -> Dict[Tuple[str, str], dict]:
    """
    Latest candle per symbol+interval, with last_close.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT c.symbol, c.interval,
                   MAX(c.open_time) AS candles_last_open_time,
                   EXTRACT(EPOCH FROM (now() - MAX(c.open_time)))::int AS candles_lag_s,
                   (ARRAY_AGG(c.close ORDER BY c.open_time DESC))[1] AS last_close
            FROM candles c
            GROUP BY 1,2
        """)
        out: Dict[Tuple[str, str], dict] = {}
        for r in fetch_all_dict(cur):
            out[(r["symbol"], r["interval"])] = dict(r)
        return out


def load_open_positions(conn) -> Dict[BotKey, dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy,
                   side, qty, entry_price, entry_time,
                   EXTRACT(EPOCH FROM (now() - entry_time))::int AS open_age_s
            FROM positions
            WHERE status='OPEN'
        """)
        out: Dict[BotKey, dict] = {}
        for r in fetch_all_dict(cur):
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out


def load_realized_pnl_today(conn) -> Dict[BotKey, dict]:
    """
    Realized PnL since UTC day start from CLOSED positions.
    LONG: (exit - entry) * qty
    SHORT: (entry - exit) * qty
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy,
                   COUNT(*) AS n_closed_today,
                   COALESCE(SUM(
                     CASE
                       WHEN upper(side) IN ('LONG','BUY')  THEN (exit_price - entry_price) * qty
                       WHEN upper(side) IN ('SHORT','SELL') THEN (entry_price - exit_price) * qty
                       ELSE 0
                     END
                   ),0) AS realized_pnl_quote_today
            FROM positions
            WHERE status='CLOSED'
              AND exit_time >= date_trunc('day', now() AT TIME ZONE 'UTC')
            GROUP BY 1,2,3
        """)
        out: Dict[BotKey, dict] = {}
        for r in fetch_all_dict(cur):
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out


# -------------------------
# Decision logic
# -------------------------
def decide_state(
    *,
    panic_enabled: bool,
    hb_lag_s: Optional[int],
    candles_lag_s: Optional[int],
    enabled: Optional[bool],
    live_orders_enabled: Optional[bool],
    pnl_total: Decimal,
    exposure: Decimal,
    cfg: dict,
) -> Tuple[str, Optional[str], Optional[str]]:
    if panic_enabled:
        return ("PANIC", "PANIC", "panic_state.panic_enabled=true")

    if hb_lag_s is None or hb_lag_s > cfg["hb_max_lag_s"]:
        return ("HEARTBEAT_STALE", "HEARTBEAT_STALE", f"hb_lag_s={hb_lag_s} > {cfg['hb_max_lag_s']}")

    if candles_lag_s is None or candles_lag_s > cfg["candles_max_lag_s"]:
        return ("CANDLES_STALE", "CANDLES_STALE", f"candles_lag_s={candles_lag_s} > {cfg['candles_max_lag_s']}")

    # Prefer QUOTE daily loss in v1
    if cfg.get("daily_max_loss_quote") is not None:
        if pnl_total <= -cfg["daily_max_loss_quote"]:
            return ("RISK_DAILYLOSS", "RISK_DAILYLOSS", f"pnl_total={pnl_total} <= -{cfg['daily_max_loss_quote']}")

    if exposure >= cfg["exposure_max_quote"]:
        return ("RISK_EXPOSURE", "RISK_EXPOSURE", f"exposure={exposure} >= {cfg['exposure_max_quote']}")

    if enabled is False:
        return ("HALTED", "HALTED", "bot_control.enabled=false")

    # Permission is tracked separately in bot_state.live_orders_enabled.
    # State should represent health/risk, not whether we allow entries.
    # Keeping state=OK improves dashboards and avoids treating "disabled" as an incident.
    if live_orders_enabled is False:
        return ("OK", "LIVE_ORDERS_DISABLED", "bot_control.live_orders_enabled=false")

    return ("OK", None, None)


# -------------------------
# Writers
# -------------------------
SQL_INSERT_RISK_METRICS = """
INSERT INTO orchestrator.risk_metrics(
  symbol, interval, strategy,
  realized_pnl_quote_today, unrealized_pnl_quote, pnl_total_quote_today,
  exposure_quote, last_price,
  n_closed_today, has_open_position,
  source, meta
) VALUES (
  %(symbol)s, %(interval)s, %(strategy)s,
  %(realized_pnl)s, %(unrealized_pnl)s, %(pnl_total)s,
  %(exposure)s, %(last_price)s,
  %(n_closed_today)s, %(has_open)s,
  'positions', %(meta)s::jsonb
);
"""

SQL_UPSERT_BOT_STATE = """
INSERT INTO orchestrator.bot_state (
  symbol, interval, strategy,
  state, blocker_primary, blocker_detail,
  enabled, live_orders_enabled, regime_enabled, regime_mode, control_reason, control_updated_at,
  hb_last_seen, hb_lag_s,
  candles_last_open_time, candles_lag_s,
  open_position, open_side, open_qty, open_entry_price, open_entry_time, open_age_s,
  realized_pnl_quote_today, unrealized_pnl_quote, pnl_total_quote_today, exposure_quote, last_price,
  policy_version, computed_at, meta
) VALUES (
  %(symbol)s, %(interval)s, %(strategy)s,
  %(state)s, %(blocker_primary)s, %(blocker_detail)s,
  %(enabled)s, %(live_orders_enabled)s, %(regime_enabled)s, %(regime_mode)s, %(control_reason)s, %(control_updated_at)s,
  %(hb_last_seen)s, %(hb_lag_s)s,
  %(candles_last_open_time)s, %(candles_lag_s)s,
  %(open_position)s, %(open_side)s, %(open_qty)s, %(open_entry_price)s, %(open_entry_time)s, %(open_age_s)s,
  %(realized_pnl)s, %(unrealized_pnl)s, %(pnl_total)s, %(exposure)s, %(last_price)s,
  %(policy_version)s, now(), %(meta)s::jsonb
)
ON CONFLICT (symbol, interval, strategy)
DO UPDATE SET
  state = EXCLUDED.state,
  blocker_primary = EXCLUDED.blocker_primary,
  blocker_detail = EXCLUDED.blocker_detail,
  enabled = EXCLUDED.enabled,
  live_orders_enabled = EXCLUDED.live_orders_enabled,
  regime_enabled = EXCLUDED.regime_enabled,
  regime_mode = EXCLUDED.regime_mode,
  control_reason = EXCLUDED.control_reason,
  control_updated_at = EXCLUDED.control_updated_at,
  hb_last_seen = EXCLUDED.hb_last_seen,
  hb_lag_s = EXCLUDED.hb_lag_s,
  candles_last_open_time = EXCLUDED.candles_last_open_time,
  candles_lag_s = EXCLUDED.candles_lag_s,
  open_position = EXCLUDED.open_position,
  open_side = EXCLUDED.open_side,
  open_qty = EXCLUDED.open_qty,
  open_entry_price = EXCLUDED.open_entry_price,
  open_entry_time = EXCLUDED.open_entry_time,
  open_age_s = EXCLUDED.open_age_s,
  realized_pnl_quote_today = EXCLUDED.realized_pnl_quote_today,
  unrealized_pnl_quote = EXCLUDED.unrealized_pnl_quote,
  pnl_total_quote_today = EXCLUDED.pnl_total_quote_today,
  exposure_quote = EXCLUDED.exposure_quote,
  last_price = EXCLUDED.last_price,
  policy_version = EXCLUDED.policy_version,
  computed_at = now(),
  meta = EXCLUDED.meta;
"""

SQL_INSERT_DECISION_LOG = """
INSERT INTO orchestrator.decision_log(
  symbol, interval, strategy,
  state_before, state_after,
  action, executed,
  reason_code, reason_detail,
  meta
) VALUES (
  %(symbol)s, %(interval)s, %(strategy)s,
  %(state_before)s, %(state_after)s,
  %(action)s, %(executed)s,
  %(reason_code)s, %(reason_detail)s,
  %(meta)s::jsonb
);
"""

SQL_DISABLE_LIVE_ORDERS = """
UPDATE bot_control
SET live_orders_enabled = false,
    reason = %(reason)s,
    updated_at = now()
WHERE symbol=%(symbol)s AND interval=%(interval)s AND strategy=%(strategy)s
  AND live_orders_enabled = true;
"""


def insert_risk_metrics(conn, bk: BotKey, realized_pnl: Decimal, unrealized: Decimal, pnl_total: Decimal,
                        exposure: Decimal, last_price: Optional[Decimal], n_closed_today: int, has_open: bool,
                        cfg: dict, effective_actions_enabled: bool):
    meta = {
        "note": "orc_v1",
        "source": "positions",
        "policy_version": cfg.get("policy_version"),
        "actions_enabled": bool(effective_actions_enabled),
        "hb_max_lag_s": cfg.get("hb_max_lag_s"),
        "candles_max_lag_s": cfg.get("candles_max_lag_s"),
    }
    params = {
        "symbol": bk.symbol,
        "interval": bk.interval,
        "strategy": bk.strategy,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized,
        "pnl_total": pnl_total,
        "exposure": exposure,
        "last_price": last_price,
        "n_closed_today": n_closed_today,
        "has_open": has_open,
        "meta": json.dumps(meta),
    }
    with conn.cursor() as cur:
        cur.execute(SQL_INSERT_RISK_METRICS, params)


def upsert_bot_state(conn, bk: BotKey, bc: dict, hb_row: Optional[dict], c_row: Optional[dict], op_row: Optional[dict],
                     state: str, blocker_primary: Optional[str], blocker_detail: Optional[str],
                     realized_pnl: Decimal, unrealized: Decimal, pnl_total: Decimal, exposure: Decimal,
                     last_price: Optional[Decimal], policy_version: str):
    meta = {
        "note": "orc_v1",
        "hb_info": hb_row.get("hb_info") if hb_row else None,
    }

    params = {
        "symbol": bk.symbol,
        "interval": bk.interval,
        "strategy": bk.strategy,
        "state": state,
        "blocker_primary": blocker_primary,
        "blocker_detail": blocker_detail,

        "enabled": bc.get("enabled"),
        "live_orders_enabled": bc.get("live_orders_enabled"),
        "regime_enabled": bc.get("regime_enabled"),
        "regime_mode": bc.get("regime_mode"),
        "control_reason": bc.get("control_reason"),
        "control_updated_at": bc.get("control_updated_at"),

        "hb_last_seen": hb_row.get("hb_last_seen") if hb_row else None,
        "hb_lag_s": hb_row.get("hb_lag_s") if hb_row else None,

        "candles_last_open_time": c_row.get("candles_last_open_time") if c_row else None,
        "candles_lag_s": c_row.get("candles_lag_s") if c_row else None,

        "open_position": op_row is not None,
        "open_side": op_row.get("side") if op_row else None,
        "open_qty": op_row.get("qty") if op_row else None,
        "open_entry_price": op_row.get("entry_price") if op_row else None,
        "open_entry_time": op_row.get("entry_time") if op_row else None,
        "open_age_s": op_row.get("open_age_s") if op_row else None,

        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized,
        "pnl_total": pnl_total,
        "exposure": exposure,
        "last_price": last_price,

        "policy_version": policy_version,
        "meta": json.dumps(meta),
    }

    with conn.cursor() as cur:
        cur.execute(SQL_UPSERT_BOT_STATE, params)


def insert_decision_log(conn, bk: BotKey, state_before: Optional[str], state_after: str,
                        action: str, executed: bool, reason_code: Optional[str], reason_detail: Optional[str],
                        meta: Optional[dict] = None):
    meta = meta or {"note": "orc_v1"}
    params = {
        "symbol": bk.symbol,
        "interval": bk.interval,
        "strategy": bk.strategy,
        "state_before": state_before,
        "state_after": state_after,
        "action": action,
        "executed": executed,
        "reason_code": reason_code,
        "reason_detail": reason_detail,
        "meta": json.dumps(meta),
    }
    with conn.cursor() as cur:
        cur.execute(SQL_INSERT_DECISION_LOG, params)


def disable_live_orders(conn, bk: BotKey, reason: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(SQL_DISABLE_LIVE_ORDERS, {
            "symbol": bk.symbol,
            "interval": bk.interval,
            "strategy": bk.strategy,
            "reason": reason,
        })
        return cur.rowcount > 0


# -------------------------
# Main orchestrator loop
# -------------------------
def run_orchestrator_v1(conn, actions_enabled: bool):
    cfg = load_orc_cfg(conn)
    effective_actions_enabled = bool(actions_enabled) and bool(cfg.get("actions_enabled", False))
    panic = get_panic(conn)

    bot_control = load_bot_control(conn)
    hb = load_heartbeat(conn)
    candles = load_candles_latest(conn)
    open_pos = load_open_positions(conn)
    realized = load_realized_pnl_today(conn)

    logging.info(
        "tick: bots=%d panic=%s actions_enabled=%s policy_version=%s",
        len(bot_control), panic.get("panic_enabled"), effective_actions_enabled, cfg.get("policy_version")
    )

    tick_ts = time.time()
    tick_id = f"{int(tick_ts)}"

    for bk, bc in bot_control.items():
        hb_row = hb.get(bk)
        c_row = candles.get((bk.symbol, bk.interval))
        op_row = open_pos.get(bk)
        r_row = realized.get(bk)

        hb_lag_s = int(hb_row["hb_lag_s"]) if hb_row and hb_row.get("hb_lag_s") is not None else None
        candles_lag_s = int(c_row["candles_lag_s"]) if c_row and c_row.get("candles_lag_s") is not None else None

        last_price: Optional[Decimal] = None
        if c_row and c_row.get("last_close") is not None:
            last_price = Decimal(str(c_row["last_close"]))

        realized_pnl = Decimal(str(r_row["realized_pnl_quote_today"])) if r_row else Decimal("0")
        n_closed_today = int(r_row["n_closed_today"]) if r_row else 0

        has_open = op_row is not None
        unrealized = Decimal("0")
        exposure = Decimal("0")

        if has_open and last_price is not None and op_row.get("qty") is not None and op_row.get("entry_price") is not None:
            qty = Decimal(str(op_row["qty"]))
            entry = Decimal(str(op_row["entry_price"]))
            side = str(op_row.get("side") or "").upper()

            if side in ("LONG", "BUY"):
                unrealized = (last_price - entry) * qty
            elif side in ("SHORT", "SELL"):
                unrealized = (entry - last_price) * qty

            exposure = abs(qty * last_price)

        pnl_total = realized_pnl + (unrealized if cfg["unrealized_in_risk"] else Decimal("0"))

        state, blocker_primary, blocker_detail = decide_state(
            panic_enabled=bool(panic.get("panic_enabled")),
            hb_lag_s=hb_lag_s,
            candles_lag_s=candles_lag_s,
            enabled=bc.get("enabled"),
            live_orders_enabled=bc.get("live_orders_enabled"),
            pnl_total=pnl_total,
            exposure=exposure,
            cfg=cfg,
        )

        # Always write metrics + state
        insert_risk_metrics(conn, bk, realized_pnl, unrealized, pnl_total, exposure, last_price, n_closed_today, has_open,
                    cfg=cfg, effective_actions_enabled=effective_actions_enabled)
        upsert_bot_state(conn, bk, bc, hb_row, c_row, op_row,
                         state, blocker_primary, blocker_detail,
                         realized_pnl, unrealized, pnl_total, exposure, last_price,
                         str(cfg.get("policy_version", "orc_v1")))

        # Decide action (DISABLE_ONLY)
        ACTIONS_ALLOWED_STATES = {"PANIC", "HEARTBEAT_STALE", "CANDLES_STALE"}

        action = "NOOP"
        executed = False

        if effective_actions_enabled and state in ACTIONS_ALLOWED_STATES:
            if bc["live_orders_enabled"] is True:
                executed = disable_live_orders(conn, bk, reason=f"ORC:{state}")
                action = "DISABLE_LIVE_ORDERS"

        insert_decision_log(
            conn, bk,
            state_before=None,  # optionally read from bot_state before upsert
            state_after=state,
            action=action,
            executed=executed,
            reason_code=blocker_primary,
            reason_detail=blocker_detail,
            meta={"policy_version": cfg.get("policy_version"), "actions_enabled": effective_actions_enabled, "tick_id": tick_id},
        )


def main():
    poll_s = int(os.getenv("ORC_POLL_SECONDS", "10"))
    actions_enabled = str(os.getenv("ORC_ACTIONS_ENABLED", "false")).lower() in ("1", "true", "yes", "y", "on")

    # Hard safety: default REPORT_ONLY
    if not actions_enabled:
        logging.info("Starting in REPORT_ONLY mode (ORC_ACTIONS_ENABLED=false)")

    while True:
        try:
            conn = get_db_conn()
            conn.autocommit = False
            try:
                run_orchestrator_v1(conn, actions_enabled=actions_enabled)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.exception("tick failed: %s", e)
            finally:
                conn.close()
        except Exception as e:
            logging.exception("db connection failed: %s", e)

        time.sleep(poll_s)


if __name__ == "__main__":
    main()
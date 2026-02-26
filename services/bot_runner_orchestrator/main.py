import os
import time
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from collections import defaultdict
from common.db import get_db_conn
from typing import Dict, Tuple, Any, Optional, List, Set
from datetime import datetime, timezone, timedelta
import psycopg2
import psycopg2.extras

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [orchestrator_v1] %(message)s",
)

log = logging.getLogger(__name__)

UNIVERSE_SYMBOLS = {"BTCUSDC", "ETHUSDC", "SOLUSDC", "BNBUSDC"}
UNIVERSE_STRATS = {"RSI", "SUPERTREND", "TREND", "BBRANGE"}
UNIVERSE_INTERVALS = tuple(
    x.strip() for x in os.getenv("ORC_ALLOC_INTERVALS", "1m").split(",") if x.strip()
)

TRADING_MODE = os.getenv("TRADING_MODE", "").upper()

MAX_PICKS = int(os.getenv("ORC_ALLOC_MAX_PICKS", "4"))
MAX_PER_SYMBOL = int(os.getenv("ORC_ALLOC_MAX_PER_SYMBOL", "1"))
HYSTERESIS_MIN = int(os.getenv("ORC_ALLOC_HYSTERESIS_MIN", "10"))
REGIME_LOOKBACK_MIN = int(os.getenv("ORC_ALLOC_REGIME_LOOKBACK_MIN", "30"))

CANDLES_MAX_LAG_S = int(os.getenv("ORC_ALLOC_CANDLES_MAX_LAG_S", "120"))  # 2x 1m default
HB_MAX_LAG_S = int(os.getenv("ORC_ALLOC_HB_MAX_LAG_S", "60"))             # reasonable default
SYMBOL_EXPOSURE_CAP = Decimal(os.getenv("ORC_ALLOC_SYMBOL_EXPOSURE_CAP", "0"))
COOLDOWN_HAS_OPEN_MIN = int(os.getenv("ORC_ALLOC_COOLDOWN_HAS_OPEN_MIN", "0"))
EXPOSURE_PRICE_SOURCE = (os.getenv("ORC_ALLOC_EXPOSURE_PRICE_SOURCE", "LAST_CLOSE") or "LAST_CLOSE").upper()

# -------------------------
# Types
# -------------------------
@dataclass(frozen=True)
class BotKey:
    symbol: str
    interval: str
    strategy: str


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
        "policy_version": "orc_v1",
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



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def kv_get(conn, key: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM automation_kv WHERE key=%s", (key,))
        row = cur.fetchone()
        return row[0] if row else None


def kv_set(conn, key: str, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO automation_kv(key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key) DO UPDATE
            SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
        """, (key, value))

# -------------------------
# Data loaders
# -------------------------
def load_open_symbols(conn) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT symbol
            FROM positions
            WHERE status='OPEN'
        """)
        return {r[0] for r in cur.fetchall()}


def load_promoted_candidates_latest(conn) -> Dict[BotKey, dict]:
    """
    Latest promoted_candidates row per (symbol, interval, strategy).
    Provides eligible_live + elig_reason for allocator observability / gating.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT ON (symbol, interval, strategy)
                symbol, interval, strategy,
                eligible_live, elig_reason,
                policy_version, published_at, source_ts,
                meta
            FROM promoted_candidates
            ORDER BY symbol, interval, strategy, published_at DESC
        """)
        out: Dict[BotKey, dict] = {}
        for r in cur.fetchall():
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out
    

def get_panic(conn) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT panic_enabled, reason, updated_at FROM panic_state WHERE id = true")
        row = cur.fetchone()
        if not row:
            return {"panic_enabled": False, "reason": None, "updated_at": None}
        return dict(row)


def had_recent_has_open(conn, bk: BotKey, minutes: int) -> bool:
    if minutes <= 0:
        return False
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM bot_control_audit
            WHERE symbol=%s AND interval=%s AND strategy=%s
              AND changed_at >= now() - (%s || ' minutes')::interval
              AND (new_row->>'reason') ILIKE 'ORC_ALLOC_A: has OPEN%%'
            LIMIT 1
        """, (bk.symbol, bk.interval, bk.strategy, str(int(minutes))))
        return cur.fetchone() is not None


def insert_orc_decision(conn, bk: BotKey, action: str, new_value: Optional[str], reason: str, meta: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO orchestrator_decisions(symbol, interval, strategy, action, new_value, reason, meta)
            VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)
        """, (bk.symbol, bk.interval, bk.strategy, action, new_value, reason, json.dumps(meta)))


def load_v2_best(conn) -> List[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
              symbol, interval, strategy,
              is_fresh, eligible_by_regime, n_open_symbol_interval,
              COALESCE(is_profitable_3d,false) AS is_profitable_3d,
              n_trades_3d, net_sum_3d, profit_factor_3d, win_rate_3d
            FROM v_orc_best_per_symbol_interval_v2
        """)
        return cur.fetchall()
    

def load_orc_v2_cfg(conn) -> dict:
    defaults = {
        "mode": "DRY_RUN",            # DRY_RUN | ENFORCE
        "max_picks": 8,
        "min_live_hours": 48,
        "min_off_hours": 24,
        "require_profitable": True,   # na start
        "step_days": 3,               # ladder: 3 dni na start
        "step_usdc": Decimal("5"),
        "min_trades_3d": 5,
    }
    key_map = {
        "orc_v2_mode": ("mode", str),
        "orc_v2_max_picks": ("max_picks", int),
        "orc_v2_min_live_hours": ("min_live_hours", int),
        "orc_v2_min_off_hours": ("min_off_hours", int),
        "orc_v2_min_trades_3d": ("min_trades_3d", int),
        "orc_v2_step_days": ("step_days", int),
        "orc_v2_step_usdc": ("step_usdc", Decimal),
        "orc_v2_require_profitable": ("require_profitable", lambda v: str(v).lower() in ("1","true","yes","y","on")),
    }

    cfg = dict(defaults)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT key, value
            FROM automation_kv
            WHERE key IN (
              'orc_v2_mode','orc_v2_max_picks','orc_v2_min_live_hours','orc_v2_min_off_hours',
              'orc_v2_min_trades_3d','orc_v2_step_days','orc_v2_step_usdc','orc_v2_require_profitable'
            )
        """)
        for r in cur.fetchall():
            k, v = r["key"], r["value"]
            if k in key_map:
                out_key, cast = key_map[k]
                try:
                    cfg[out_key] = cast(v)
                except Exception:
                    log.warning("Bad v2 cfg %s=%r; keeping %r", k, v, cfg[out_key])
    cfg["mode"] = (cfg["mode"] or "DRY_RUN").upper()
    return cfg


def load_open_symbol_exposure(conn) -> Dict[str, Decimal]:
    """
    Sums exposure per symbol across ALL OPEN positions.
    Exposure is abs(qty * price) in quote (USDC).
    Price source:
      - LAST_CLOSE: uses candles last_close for that (symbol, interval) if present, else entry_price
      - ENTRY_PRICE: always entry_price
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, qty, entry_price
            FROM positions
            WHERE status='OPEN'
        """)
        rows = cur.fetchall()

    if not rows:
        return {}

    # build (symbol, interval) -> last_close
    last_close: Dict[Tuple[str, str], Decimal] = {}
    if EXPOSURE_PRICE_SOURCE == "LAST_CLOSE":
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT c.symbol, c.interval,
                       (ARRAY_AGG(c.close ORDER BY c.open_time DESC))[1] AS last_close
                FROM candles c
                GROUP BY 1,2
            """)
            for r in cur.fetchall():
                if r["last_close"] is None:
                    continue
                last_close[(r["symbol"], r["interval"])] = Decimal(str(r["last_close"]))

    out: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    for r in rows:
        sym = r["symbol"]
        itv = r["interval"]
        qty = Decimal(str(r["qty"])) if r["qty"] is not None else Decimal("0")
        entry = Decimal(str(r["entry_price"])) if r["entry_price"] is not None else Decimal("0")

        px = entry
        if EXPOSURE_PRICE_SOURCE == "LAST_CLOSE":
            px = last_close.get((sym, itv), entry)

        out[sym] += abs(qty * px)

    return dict(out)


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


def v2_decision_fingerprint(action: str, reason: str, meta: dict) -> str:
    # stabilny fingerprint bez pól, które zmieniają się często
    stable = dict(meta or {})
    # nic tu nie usuwam, bo u Ciebie meta jest stabilne per tick; jak dodasz timestamp, usuń go tutaj
    payload = {"action": action, "reason": reason, "meta": stable}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def v2_should_write_decision(conn, bk: BotKey, fp: str) -> bool:
    key = f"orc_v2_last_fp:{bk.symbol}:{bk.interval}:{bk.strategy}"
    prev = kv_get(conn, key)
    if prev == fp:
        return False
    kv_set(conn, key, fp)
    return True


def v2_meets_min_trades(row: dict, min_trades: int) -> bool:
    n = row.get("n_trades_3d")
    if n is None:
        return False
    try:
        return int(n) >= int(min_trades)
    except Exception:
        return False
    

def v2_has_profit_metrics(row: dict) -> bool:
    """
    True only when 3d profitability metrics are actually present (not NULL).
    This prevents bootstrap slots (NULL metrics) from being treated as profitable picks.
    """
    return (
        row.get("n_trades_3d") is not None
        and row.get("net_sum_3d") is not None
        and row.get("profit_factor_3d") is not None
        and row.get("win_rate_3d") is not None
    )


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
    

def load_open_pos_flags(conn) -> Set[BotKey]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy
            FROM positions
            WHERE status='OPEN'
        """)
        out: Set[BotKey] = set()
        for r in cur.fetchall():
            out.add(BotKey(r["symbol"], r["interval"], r["strategy"]))
        return out


def load_v5_picks(conn) -> Set[Tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, interval, strategy
            FROM v_orc_picks_v5
        """)
        return {(r[0], r[1], r[2]) for r in cur.fetchall()}


def load_last_regime_gate(conn, window_minutes: int) -> Dict[BotKey, dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT DISTINCT ON (symbol, interval, strategy)
                symbol, interval, strategy,
                created_at, allow, would_block, why, regime
            FROM regime_gate_events
            WHERE created_at >= now() - interval '{int(window_minutes)} minutes'
            ORDER BY symbol, interval, strategy, created_at DESC
        """)
        out: Dict[BotKey, dict] = {}
        for r in cur.fetchall():
            bk = BotKey(r["symbol"], r["interval"], r["strategy"])
            out[bk] = dict(r)
        return out

def parse_last_pick(conn) -> Tuple[Optional[datetime], List[BotKey]]:
    ts_s = kv_get(conn, "orc_alloc_last_pick_ts")
    js = kv_get(conn, "orc_alloc_last_pick_json")

    last_ts: Optional[datetime] = None
    if ts_s:
        try:
            last_ts = datetime.fromisoformat(ts_s.replace("Z", "+00:00"))
        except Exception:
            last_ts = None

    picks: List[BotKey] = []
    if js:
        try:
            arr = json.loads(js)
            for x in arr:
                picks.append(BotKey(x["symbol"], x["interval"], x["strategy"]))
        except Exception:
            picks = []

    return last_ts, picks

def save_last_pick(conn, picks: List[BotKey]) -> None:
    kv_set(conn, "orc_alloc_last_pick_ts", utc_now().isoformat().replace("+00:00", "Z"))
    kv_set(conn, "orc_alloc_last_pick_json", json.dumps([
        {"symbol": p.symbol, "interval": p.interval, "strategy": p.strategy}
        for p in picks
    ]))

def universe_filter(bk: BotKey) -> bool:
    return (
        bk.interval in UNIVERSE_INTERVALS and
        bk.symbol in UNIVERSE_SYMBOLS and
        bk.strategy in UNIVERSE_STRATS
    )


def load_open_symbol_intervals(conn) -> Set[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT symbol, interval
            FROM positions
            WHERE status='OPEN'
        """)
        return {(r[0], r[1]) for r in cur.fetchall()}


def rank_key(bk: BotKey):
    # deterministic: SUPERTREND first, then RSI; BTC>ETH>SOL>BNB
    strat_rank = 0 if bk.strategy == "SUPERTREND" else 1
    sym_rank = {"BTCUSDC": 0, "ETHUSDC": 1, "SOLUSDC": 2, "BNBUSDC": 3}.get(bk.symbol, 9)
    return (strat_rank, sym_rank)


def preferred_strategy_for_regime(regime: Optional[str]) -> str:
    r = (regime or "").upper()
    if r.startswith("TREND"):
        # trend regime: prefer trend-following
        return "TREND" if "TREND" in UNIVERSE_STRATS else "SUPERTREND"
    if r.startswith("RANGE"):
        # range regime: prefer mean reversion / bands
        return "BBRANGE" if "BBRANGE" in UNIVERSE_STRATS else "RSI"
    # fallback
    return "SUPERTREND"


def preferred_strategy_by_symbol(last_rg: Dict[BotKey, dict]) -> Dict[Tuple[str, str], str]:
    """
    (symbol, interval) -> preferred strategy based on latest regime.
    TREND* => SUPERTREND
    RANGE* => RSI
    If missing, fallback to SUPERTREND.
    """
    out: Dict[Tuple[str, str], str] = {}

    # pick first seen regime per (symbol, interval)
    for bk, rg in last_rg.items():
        if not universe_filter(bk):
            continue
        key = (bk.symbol, bk.interval)
        if key in out:
            continue
        out[key] = preferred_strategy_for_regime(rg.get("regime"))

    # ensure fallback for all universe symbols x intervals
    for sym in UNIVERSE_SYMBOLS:
        for itv in UNIVERSE_INTERVALS:
            out.setdefault((sym, itv), "SUPERTREND")

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

def set_live_orders_enabled(conn, bk: BotKey, enabled: bool, reason: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bot_control
            SET live_orders_enabled=%s,
                updated_at=now(),
                reason=%s
            WHERE symbol=%s AND interval=%s AND strategy=%s
              AND enabled=true
              AND live_orders_enabled IS DISTINCT FROM %s
        """, (enabled, reason, bk.symbol, bk.interval, bk.strategy, enabled))
        return cur.rowcount > 0


def set_entries_and_regime(
    conn,
    bk: BotKey,
    *,
    entries_enabled: bool,
    regime_mode: str,
    reason: str
) -> bool:
    """
    Variant A SSOT:
      entries ON  => regime_enabled=true, regime_mode=ENFORCE
      entries OFF => optionally regime_mode=DRY_RUN (still regime_enabled=true for telemetry)
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bot_control
            SET live_orders_enabled=%s,
                regime_enabled=true,
                regime_mode=%s,
                updated_at=now(),
                reason=%s
            WHERE symbol=%s AND interval=%s AND strategy=%s
              AND enabled=true
              AND (
                  live_orders_enabled IS DISTINCT FROM %s
                  OR regime_mode IS DISTINCT FROM %s
                  OR regime_enabled IS DISTINCT FROM true
                  OR reason IS DISTINCT FROM %s
              )
        """, (
            entries_enabled,
            regime_mode,
            reason,
            bk.symbol, bk.interval, bk.strategy,
            entries_enabled,
            regime_mode,
            reason,
        ))
        return cur.rowcount > 0


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


def allocator_pick(
    *,
    conn,
    bot_control: Dict[BotKey, dict],
    hb: Dict[BotKey, dict],
    candles: Dict[Tuple[str, str], dict],
    last_rg: Dict[BotKey, dict],
    promoted: Dict[BotKey, dict],
    open_pos: Set[BotKey],
    open_sym_itv: Set[Tuple[str, str]],
    sym_exposure: Dict[str, Decimal],
    max_picks: int = MAX_PICKS,
    max_per_symbol: int = MAX_PER_SYMBOL,
) -> Tuple[List[BotKey], Dict[BotKey, str]]:
    """
    Returns:
      - desired picks (len <= max_picks)
      - reject_reason map for observability (why not picked)
    """
    pref = preferred_strategy_by_symbol(last_rg)
    candidates: List[BotKey] = []
    reject_reason: Dict[BotKey, str] = {}

    for bk, bc in bot_control.items():
        if not universe_filter(bk):
            continue
        if not bc.get("enabled", False):
            continue
        
        # HARD: promotion eligibility gate (paper_rank_v2 / promoted_candidates)
        pr = promoted.get(bk)
        if pr is not None:
            if not bool(pr.get("eligible_live", False)):
                # keep elig_reason for observability
                elig_reason = (pr.get("elig_reason") or "").strip()
                reject_reason[bk] = f"not_eligible:{elig_reason}" if elig_reason else "not_eligible"
                continue
        else:
            # Optional: if you require promo for everyone, you can block here.
            # For now we don't block when missing.
            pass

        # HARD: per-symbol exposure cap (across ALL OPEN positions) — eligibility gate
        if SYMBOL_EXPOSURE_CAP > 0:
            cur_exp = sym_exposure.get(bk.symbol, Decimal("0"))
            if cur_exp >= SYMBOL_EXPOSURE_CAP:
                reject_reason[bk] = "symbol_exposure_cap"
                continue

        # HARD: block ONLY same (symbol, interval) that already has OPEN
        if (bk.symbol, bk.interval) in open_sym_itv:
            # HARD: cooldown after "has OPEN" to reduce flapping
            if COOLDOWN_HAS_OPEN_MIN > 0 and had_recent_has_open(conn, bk, COOLDOWN_HAS_OPEN_MIN):
                reject_reason[bk] = "cooldown_after_has_open"
                continue

            reject_reason[bk] = "symbol_interval_has_open"
            continue

        if bk in open_pos:
            reject_reason[bk] = "bot_has_open"
            continue

        hb_row = hb.get(bk)
        if not hb_row:
            reject_reason[bk] = "no_heartbeat"
            continue
        hb_lag = hb_row.get("hb_lag_s")
        if hb_lag is None or hb_lag > HB_MAX_LAG_S:
            reject_reason[bk] = "hb_stale"
            continue

        c_row = candles.get((bk.symbol, bk.interval))
        if not c_row:
            reject_reason[bk] = "no_candles"
            continue
        c_lag = c_row.get("candles_lag_s")
        if c_lag is None or c_lag > CANDLES_MAX_LAG_S:
            reject_reason[bk] = "candles_stale"
            continue

        rg = last_rg.get(bk)
        if not rg:
            reject_reason[bk] = "no_regime_gate"
            continue
        if not bool(rg.get("allow", False)):
            reject_reason[bk] = "regime_disallow"
            continue
        if bool(rg.get("would_block", False)):
            reject_reason[bk] = "regime_would_block"
            continue

        candidates.append(bk)

    # ranking: prefer per (symbol, interval) by regime preference, then stable tiebreak
    itv_rank = {"1m": 0, "5m": 1}
    sym_rank = {"BTCUSDC": 0, "ETHUSDC": 1, "SOLUSDC": 2, "BNBUSDC": 3}

    # Strategy tie-break (when NOT preferred for regime)
    strat_rank_fallback = {
        "TREND": 0,
        "SUPERTREND": 1,
        "BBRANGE": 2,
        "RSI": 3,
    }

    def candidate_rank(bk: BotKey):
        preferred = pref.get((bk.symbol, bk.interval), "SUPERTREND")
        is_preferred = 0 if bk.strategy == preferred else 1
        return (
            itv_rank.get(bk.interval, 9),
            sym_rank.get(bk.symbol, 9),
            is_preferred,
            strat_rank_fallback.get(bk.strategy, 9),
            bk.strategy,
        )

    candidates.sort(key=candidate_rank)

    # NEW: cap per (symbol, interval) (entry-enabled)
    out: List[BotKey] = []
    per_key: Dict[Tuple[str, str], int] = {}

    for bk in candidates:
        if len(out) >= max_picks:
            break

        key = (bk.symbol, bk.interval)
        cnt = per_key.get(key, 0)

        if cnt >= max_per_symbol:
            reject_reason[bk] = "per_symbol_interval_cap"
            continue

        out.append(bk)
        per_key[key] = cnt + 1

    return out, reject_reason


def run_allocator_phase_a(conn, effective_actions_enabled: bool) -> None:
    # HARD GUARD: Phase A must never run when v2 policy active
    if (kv_get(conn, "orc_policy_version") or "").strip() == "v2_profit_first":
        return
    if not effective_actions_enabled:
        return

    now = utc_now()
    last_ts, sticky = parse_last_pick(conn)

    bc_all = load_bot_control(conn)
    open_pos = load_open_pos_flags(conn)
    open_sym_itv = load_open_symbol_intervals(conn)      # NOWE dla allocator-a
    sym_exposure = load_open_symbol_exposure(conn)

    # zawsze przygotuj dane do pickowania (tanie i proste)
    hb = load_heartbeat(conn)
    candles = load_candles_latest(conn)
    last_rg = load_last_regime_gate(conn, window_minutes=REGIME_LOOKBACK_MIN)
    promoted = load_promoted_candidates_latest(conn)

    pref = preferred_strategy_by_symbol(last_rg)

    # NEW: cap per symbol for "entry-enabled"
    max_per_symbol = MAX_PER_SYMBOL
    per_sym_itv: Dict[Tuple[str, str], int] = {}

    desired: List[BotKey] = []
    reject_reason: Dict[BotKey, str] = {}

    if last_ts and (now - last_ts) < timedelta(minutes=HYSTERESIS_MIN):
        keep: List[BotKey] = []
        for bk in sticky:
            row = bc_all.get(bk)
            if not row or not row.get("enabled", False):
                reject_reason[bk] = "sticky_disabled"
                continue
            if not universe_filter(bk):
                reject_reason[bk] = "sticky_out_of_universe"
                continue
            if bk in open_pos:
                reject_reason[bk] = "sticky_bot_has_open"
                continue
            if (bk.symbol, bk.interval) in open_sym_itv:
                reject_reason[bk] = "sticky_symbol_interval_has_open"
                continue

            k = (bk.symbol, bk.interval)
            cnt = per_sym_itv.get(k, 0)
            if cnt >= max_per_symbol:
                reject_reason[bk] = "sticky_per_symbol_interval_cap"
                continue

            keep.append(bk)
            per_sym_itv[k] = cnt + 1

        desired = keep[:MAX_PICKS]

        if not desired:
            desired, rr = allocator_pick(
                conn=conn,
                bot_control=bc_all, hb=hb, candles=candles, last_rg=last_rg,
                promoted=promoted,
                open_pos=open_pos, open_sym_itv=open_sym_itv,
                sym_exposure=sym_exposure,
                max_picks=MAX_PICKS, max_per_symbol=max_per_symbol
            )
            reject_reason.update(rr)
            save_last_pick(conn, desired)
            log.info("[alloc] sticky empty -> repick: %s", desired)
        else:
            # TOP-UP: jeśli sticky ma mniej niż MAX_PICKS, dobierz resztę z aktualnych kandydatów
            if len(desired) < MAX_PICKS:
                extra, rr = allocator_pick(
                    conn=conn,
                    bot_control=bc_all, hb=hb, candles=candles, last_rg=last_rg,
                    promoted=promoted,
                    open_pos=open_pos, open_sym_itv=open_sym_itv,
                    sym_exposure=sym_exposure,
                    max_picks=MAX_PICKS, max_per_symbol=max_per_symbol
                )
                reject_reason.update(rr)

                desired_set = set(desired)

                # rebuild per_sym from current desired
                per_sym_itv = {}
                for x in desired:
                    k = (x.symbol, x.interval)
                    per_sym_itv[k] = per_sym_itv.get(k, 0) + 1

                for bk in extra:
                    if bk in desired_set:
                        continue
                    
                    k = (bk.symbol, bk.interval)
                    cnt = per_sym_itv.get(k, 0)
                    if cnt >= max_per_symbol:
                        reject_reason[bk] = "topup_per_symbol_cap"
                        continue

                    desired.append(bk)
                    desired_set.add(bk)
                    per_sym_itv[k] = cnt + 1

                    if len(desired) >= MAX_PICKS:
                        break

                save_last_pick(conn, desired)
                log.info("[alloc] sticky keep + topup (%sm): %s", HYSTERESIS_MIN, desired)
            else:
                log.info("[alloc] sticky keep (%sm): %s", HYSTERESIS_MIN, desired)
    else:
        desired, rr = allocator_pick(
            conn=conn,
            bot_control=bc_all, hb=hb, candles=candles, last_rg=last_rg,
            promoted=promoted,
            open_pos=open_pos, open_sym_itv=open_sym_itv,
            sym_exposure=sym_exposure,
            max_picks=MAX_PICKS, max_per_symbol=max_per_symbol
        )
        reject_reason.update(rr)
        save_last_pick(conn, desired)
        log.info("[alloc] new pick: %s", desired)

    desired_set = set(desired)

    n_on = 0
    n_off = 0
    for bk, row in bc_all.items():
        if not universe_filter(bk):
            continue
        if not row.get("enabled", False):
            continue

        pr = promoted.get(bk)
        if pr is not None and not bool(pr.get("eligible_live", False)):
            detail = (pr.get("elig_reason") or "").strip()
            reason = f"ORC_ALLOC_A: not eligible ({detail}) (entries OFF, DRY_RUN)" if detail else \
                    "ORC_ALLOC_A: not eligible (entries OFF, DRY_RUN)"
            set_entries_and_regime(conn, bk, entries_enabled=False, regime_mode="DRY_RUN", reason=reason)
            n_off += 1
            continue

        # HARD: per-symbol exposure cap (across ALL OPEN positions) — enforced at apply stage
        if SYMBOL_EXPOSURE_CAP > 0:
            cur_exp = sym_exposure.get(bk.symbol, Decimal("0"))
            if cur_exp >= SYMBOL_EXPOSURE_CAP:
                set_entries_and_regime(
                    conn, bk,
                    entries_enabled=False,
                    regime_mode="DRY_RUN",
                    reason="ORC_ALLOC_A: symbol exposure cap (entries OFF, DRY_RUN)"
                )
                n_off += 1
                continue

        if bk in open_pos:
            set_entries_and_regime(
                conn, bk,
                entries_enabled=False,
                regime_mode="DRY_RUN",
                reason="ORC_ALLOC_A: has OPEN (entries OFF, DRY_RUN)"
            )
            n_off += 1
            continue

        if (bk.symbol, bk.interval) in open_sym_itv:
            set_entries_and_regime(
                conn, bk,
                entries_enabled=False,
                regime_mode="DRY_RUN",
                reason="ORC_ALLOC_A: symbol+interval has OPEN (entries OFF, DRY_RUN)"
            )
            n_off += 1
            continue

        if bk in desired_set:
            changed = set_entries_and_regime(
                conn, bk,
                entries_enabled=True,
                regime_mode="ENFORCE",
                reason="ORC_ALLOC_A: picked (entries ON, ENFORCE)"
            )
            n_on += 1
            if changed:
                log.info("[alloc] entries ON + ENFORCE %s", bk)
        else:
            rr = reject_reason.get(bk)

            if rr and rr.startswith("not_eligible:"):
                detail = rr.split(":", 1)[1].strip()
                reason = f"ORC_ALLOC_A: not eligible ({detail}) (entries OFF, DRY_RUN)"
            elif rr == "not_eligible":
                reason = "ORC_ALLOC_A: not eligible (entries OFF, DRY_RUN)"
            elif rr == "per_symbol_interval_cap" or rr == "sticky_per_symbol_interval_cap" or rr == "topup_per_symbol_cap":
                reason = "ORC_ALLOC_A: per_symbol_interval_cap (entries OFF, DRY_RUN)"
            elif rr == "max_picks_reached":
                reason = "ORC_ALLOC_A: max_picks_reached (entries OFF, DRY_RUN)"
            elif rr == "symbol_interval_has_open" or rr == "sticky_symbol_interval_has_open":
                reason = "ORC_ALLOC_A: symbol+interval has OPEN (entries OFF, DRY_RUN)"
            elif rr == "symbol_exposure_cap":
                reason = "ORC_ALLOC_A: symbol exposure cap (entries OFF, DRY_RUN)"
            elif rr == "cooldown_after_has_open":
                reason = "ORC_ALLOC_A: cooldown after has OPEN (entries OFF, DRY_RUN)"
            else:
                reason = "ORC_ALLOC_A: not picked (entries OFF, DRY_RUN)"

            changed = set_entries_and_regime(
                conn, bk,
                entries_enabled=False,
                regime_mode="DRY_RUN",
                reason=reason
            )
            n_off += 1
            if changed:
                log.info("[alloc] entries OFF + DRY_RUN %s", bk)

    n_enabled_universe = sum(1 for bk, row in bc_all.items() if universe_filter(bk) and row.get("enabled", False))
    n_promo = sum(1 for bk in promoted.keys() if universe_filter(bk))
    n_elig = sum(1 for bk, pr in promoted.items() if universe_filter(bk) and bool(pr.get("eligible_live", False)))

    log.info("[alloc] universe_enabled=%d promoted=%d eligible=%d desired=%d",
             n_enabled_universe, n_promo, n_elig, len(desired))
    
    log.info("[alloc] applied desired=%d universe_seen_on=%d universe_seen_off=%d", len(desired), n_on, n_off)

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
    # ---- ALLOCATOR / POLICY ----
    panic_enabled = bool(panic.get("panic_enabled"))

    # policy switch (from automation_kv)
    policy = (kv_get(conn, "orc_policy_version") or "").strip() or str(cfg.get("policy_version") or "orc_v1")
    policy = policy.strip()

    if (not panic_enabled) and TRADING_MODE == "LIVE":
        # v2 runs even in REPORT_ONLY (writes decisions only). ENFORCE still needs effective_actions_enabled.
        if policy == "v2_profit_first":
            v2cfg = load_orc_v2_cfg(conn)

            # safety: ENFORCE requires actions_enabled
            if v2cfg.get("mode") == "ENFORCE" and not effective_actions_enabled:
                log.warning("[v2] mode=ENFORCE but actions disabled -> forcing DRY_RUN")
                v2cfg["mode"] = "DRY_RUN"

            run_orc_v2_profit_first(conn, v2cfg, effective_actions_enabled=effective_actions_enabled, tick_id=tick_id)
        else:
            # legacy Phase A allocator (only when actions enabled)
            if effective_actions_enabled and os.getenv("ORC_ALLOC_A_ENABLED", "0") == "1":
                run_allocator_phase_a(conn, effective_actions_enabled)


def v2_block_reason(row: dict) -> str:
    if not bool(row.get("is_fresh")):
        return "STALE_GATE"
    if not bool(row.get("eligible_by_regime")):
        return "REGIME_BLOCK"
    if int(row.get("n_open_symbol_interval") or 0) > 0:
        return "OPEN_LOCK"

    # kluczowe: bootstrap (NULL metryki) => NO_METRICS_3D
    if not v2_has_profit_metrics(row):
        return "NO_METRICS_3D"

    # NEW: min trades gate (threshold from cfg will be injected into row meta in runner)
    min_trades = int(row.get("_min_trades_3d") or 0)
    if min_trades > 0 and not v2_meets_min_trades(row, min_trades):
        return "MIN_TRADES_3D"

    if not bool(row.get("is_profitable_3d")):
        return "NOT_PROFITABLE"

    return "OK"


def _v2_apply_bot_control(
    conn,
    bk: BotKey,
    *,
    entries_enabled: bool,
    regime_mode: str,
    reason: str,
) -> bool:
    """
    Updates bot_control AND timestamps:
      - enabling entries: set live_since=now() if was previously disabled
      - disabling entries: set last_disabled_at=now() if was previously enabled
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bot_control
            SET
              live_orders_enabled = %(loe)s,
              regime_enabled = true,
              regime_mode = %(rmode)s,
              updated_at = now(),
              reason = %(reason)s,

              live_since = CASE
                WHEN %(loe)s = true AND COALESCE(live_orders_enabled,false) = false THEN now()
                ELSE live_since
              END,

              last_disabled_at = CASE
                WHEN %(loe)s = false AND COALESCE(live_orders_enabled,false) = true THEN now()
                ELSE last_disabled_at
              END
            WHERE symbol=%(symbol)s AND interval=%(interval)s AND strategy=%(strategy)s
              AND enabled = true
              AND (
                   live_orders_enabled IS DISTINCT FROM %(loe)s
                OR regime_mode IS DISTINCT FROM %(rmode)s
                OR regime_enabled IS DISTINCT FROM true
                OR reason IS DISTINCT FROM %(reason)s
              )
        """, {
            "symbol": bk.symbol,
            "interval": bk.interval,
            "strategy": bk.strategy,
            "loe": bool(entries_enabled),
            "rmode": regime_mode,
            "reason": reason,
        })
        return cur.rowcount > 0


def apply_v2_enforce(conn, pick_set: Set[Tuple[str, str, str]], *, v2cfg: dict) -> None:
    """
    ENFORCE:
      - picked slots => entries ON + regime ENFORCE
      - non-picked slots => entries OFF + regime DRY_RUN

    Cooldowns:
      - min_live_hours: do not disable if current live period shorter than this
      - min_off_hours:  do not enable if bot was disabled too recently
    """
    min_live_h = int(v2cfg.get("min_live_hours") or 0)
    min_off_h = int(v2cfg.get("min_off_hours") or 0)

    # Load current state for universe
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT symbol, interval, strategy,
                   enabled, live_orders_enabled, regime_mode,
                   live_since, last_disabled_at
            FROM bot_control
        """)
        rows = cur.fetchall()

    now = utc_now()

    n_on = 0
    n_off = 0
    n_skip = 0

    for r in rows:
        bk = BotKey(r["symbol"], r["interval"], r["strategy"])
        if not universe_filter(bk):
            continue
        if not bool(r.get("enabled", False)):
            continue

        currently_on = bool(r.get("live_orders_enabled", False))
        live_since = r.get("live_since")
        last_disabled_at = r.get("last_disabled_at")

        want_on = (bk.symbol, bk.interval, bk.strategy) in pick_set

        # cooldown: prevent flapping
        if want_on:
            if min_off_h > 0 and last_disabled_at is not None:
                off_age_h = (now - last_disabled_at).total_seconds() / 3600.0
                if off_age_h < float(min_off_h):
                    n_skip += 1
                    continue
        else:
            if min_live_h > 0 and currently_on and live_since is not None:
                live_age_h = (now - live_since).total_seconds() / 3600.0
                if live_age_h < float(min_live_h):
                    n_skip += 1
                    continue

        if want_on:
            changed = _v2_apply_bot_control(
                conn, bk,
                entries_enabled=True,
                regime_mode="ENFORCE",
                reason="ORC_V2: picked (entries ON, ENFORCE)"
            )
            if changed:
                n_on += 1
        else:
            changed = _v2_apply_bot_control(
                conn, bk,
                entries_enabled=False,
                regime_mode="DRY_RUN",
                reason="ORC_V2: not picked (entries OFF, DRY_RUN)"
            )
            if changed:
                n_off += 1

    log.info("[v2] ENFORCE applied: on=%d off=%d skipped_by_cooldown=%d", n_on, n_off, n_skip)
        

def run_orc_v2_profit_first(conn, v2cfg: dict, *, effective_actions_enabled: bool, tick_id: Optional[str] = None) -> None:
    if kv_get(conn, "orc_v2_mode") is None:
        return

    mode = (v2cfg.get("mode") or "DRY_RUN").upper()  # DRY_RUN | ENFORCE
    max_picks = int(v2cfg.get("max_picks") or 8)

    # HARD GUARD: never ENFORCE when actions are disabled
    if mode == "ENFORCE" and not effective_actions_enabled:
        log.warning("[v2] ENFORCE requested but actions disabled -> forcing DRY_RUN")
        mode = "DRY_RUN"

    rows = load_v2_best(conn)
    require_profitable = bool(v2cfg.get("require_profitable", True))
    min_trades_3d = int(v2cfg.get("min_trades_3d") or 0)

    # 1) pick set (DB is SSOT)
    pick_set = load_v5_picks(conn)

    # optional: enforce max_picks safety here too (even though view already limits)
    if len(pick_set) > max_picks:
        pick_set = set(list(sorted(pick_set))[:max_picks])

    # 2) decisions log for all best_per rows
    for r in rows:
        bk = BotKey(r["symbol"], r["interval"], r["strategy"])
        base_meta = {
            "policy": "v2_profit_first",
            "mode": mode,
            "tick_id": tick_id,

            "is_fresh": bool(r["is_fresh"]),
            "eligible_by_regime": bool(r["eligible_by_regime"]),
            "n_open_symbol_interval": int(r["n_open_symbol_interval"] or 0),

            "is_profitable_3d": bool(r["is_profitable_3d"]),
            "n_trades_3d": int(r["n_trades_3d"] or 0) if r["n_trades_3d"] is not None else None,
            "net_sum_3d": str(r["net_sum_3d"]) if r["net_sum_3d"] is not None else None,
            "profit_factor_3d": str(r["profit_factor_3d"]) if r["profit_factor_3d"] is not None else None,
            "win_rate_3d": str(r["win_rate_3d"]) if r["win_rate_3d"] is not None else None,

            "min_trades_3d": min_trades_3d,
        }

        has_metrics = v2_has_profit_metrics(r)
        base_meta["has_profit_metrics_3d"] = has_metrics
        meets_trades = (min_trades_3d <= 0) or v2_meets_min_trades(r, min_trades_3d)
        base_meta["meets_min_trades_3d"] = bool(meets_trades)

        # inject for v2_block_reason()
        r["_min_trades_3d"] = min_trades_3d

        is_picked = (bk.symbol, bk.interval, bk.strategy) in pick_set

        if is_picked:
            if require_profitable:
                if not has_metrics:
                    fp = v2_decision_fingerprint("V2_BLOCK", "NO_METRICS_3D", base_meta)
                    if v2_should_write_decision(conn, bk, fp):
                        insert_orc_decision(conn, bk, "V2_BLOCK", None, "NO_METRICS_3D", base_meta)
                    continue

                if not meets_trades:
                    fp = v2_decision_fingerprint("V2_BLOCK", "MIN_TRADES_3D", base_meta)
                    if v2_should_write_decision(conn, bk, fp):
                        insert_orc_decision(conn, bk, "V2_BLOCK", None, "MIN_TRADES_3D", base_meta)
                    continue

                if not bool(r.get("is_profitable_3d")):
                    fp = v2_decision_fingerprint("V2_BLOCK", "NOT_PROFITABLE", base_meta)
                    if v2_should_write_decision(conn, bk, fp):
                        insert_orc_decision(conn, bk, "V2_BLOCK", None, "NOT_PROFITABLE", base_meta)
                    continue

            fp = v2_decision_fingerprint("V2_PROPOSE_ENABLE_LIVE", "PROFITABLE_3D_PICK", base_meta)
            if v2_should_write_decision(conn, bk, fp):
                insert_orc_decision(conn, bk, "V2_PROPOSE_ENABLE_LIVE", "true", "PROFITABLE_3D_PICK", base_meta)
        else:
            reason = v2_block_reason(r)
            fp = v2_decision_fingerprint("V2_BLOCK", reason, base_meta)
            if v2_should_write_decision(conn, bk, fp):
                insert_orc_decision(conn, bk, "V2_BLOCK", None, reason, base_meta)

    # tighten pick_set in ENFORCE when require_profitable
    if mode == "ENFORCE" and require_profitable:
        allowed: Set[Tuple[str, str, str]] = set()
        for r in rows:
            key = (r["symbol"], r["interval"], r["strategy"])
            if key not in pick_set:
                continue
            if not v2_has_profit_metrics(r):
                continue
            if min_trades_3d > 0 and not v2_meets_min_trades(r, min_trades_3d):
                continue
            if not bool(r.get("is_profitable_3d")):
                continue
            allowed.add(key)
        pick_set = allowed

    # 3) ENFORCE apply
    if mode == "ENFORCE":
        apply_v2_enforce(conn, pick_set, v2cfg=v2cfg)


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
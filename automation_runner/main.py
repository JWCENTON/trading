import os
import time
import json
import logging
import requests
from datetime import datetime, timezone, date
from pathlib import Path
from common.reconcile_positions import reconcile_positions
from common.db import get_db_conn
from binance.client import Client
from common.runtime import RuntimeConfig

cfg = RuntimeConfig.from_env()
API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] automation-runner: %(message)s")
client = Client(api_key=API_KEY, api_secret=API_SECRET) if cfg.trading_mode == "LIVE" else Client()
last_reconcile_ts = 0.0
last_ssot_watchdog_ts = 0.0

def _json_default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def q1(cur, sql, params=None):
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def _is_primary_writer_v5(cur) -> bool:
    """
    Single-writer lock to prevent bot_control flapping.
    Require: automation_kv.orc_writer_primary == 'V5'
    """
    v = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_writer_primary';")
    return (str(v or "").strip().upper() == "V5")


def exec_sql(cur, sql, params=None):
    cur.execute(sql, params or ())


def upsert_kv(cur, key, value):
    exec_sql(cur, """
        INSERT INTO automation_kv(key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key) DO UPDATE
        SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at;
    """, (key, value))


def set_panic(cur, enabled: bool, reason: str):
    exec_sql(cur, """
        UPDATE panic_state
        SET panic_enabled=%s, reason=%s, updated_at=now()
        WHERE id=true;
    """, (enabled, reason))


def disable_live_orders(cur, reason: str):
    # DISABLE-ONLY: tylko wyłączamy (nigdy nie włączamy)
    exec_sql(cur, """
        UPDATE bot_control
        SET live_orders_enabled=false, reason=%s, updated_at=now()
        WHERE live_orders_enabled=true;
    """, (reason,))


def _is_regime_panic_reason(reason: str) -> bool:
    return bool(reason) and reason.startswith("FAILSAFE: stale market_regime")


def _get_int_kv(cur, key: str, default: int = 0) -> int:
    v = q1(cur, "SELECT value FROM automation_kv WHERE key=%s;", (key,))
    try:
        return int(v)
    except Exception:
        return default
    
    
def run_orc_v5_apply(conn):
    """
    LIVE: apply v_orc_picks_v5 -> bot_control (DB-only), idempotent + throttled.
    Single writer of bot_control under V5 to avoid flapping and debug hell.
    """

    # Hard enable (env) + soft enable (kv) to avoid accidental activation
    if os.getenv("ORC_V5_APPLY_ENABLED", "0") != "1":
        return

    interval_s_env = int(os.getenv("ORC_V5_APPLY_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        # Optional KV overrides (if present)
        kv_enabled = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_v5_apply_enabled';")
        if kv_enabled is not None and str(kv_enabled).strip() not in ("1", "true", "TRUE", "yes", "on"):
            return
        
        # HARD SAFETY: single writer lock
        if not _is_primary_writer_v5(cur):
            logging.warning("orc_v5_apply: skip (orc_writer_primary != V5)")
            return

        kv_interval = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_v5_apply_interval_s';")
        interval_s = interval_s_env
        if kv_interval is not None:
            try:
                interval_s = int(kv_interval)
            except Exception:
                interval_s = interval_s_env

        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_v5_apply_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        # Safety: only LIVE + ACTIVE automation
        # (caller already checks, but keep it here to prevent accidental misuse)
        if cfg.trading_mode != "LIVE":
            return

        sql = """
        WITH universe AS (
          SELECT symbol, interval, strategy
          FROM bot_control
          WHERE enabled = true
            AND COALESCE(control_mode, 'AUTO') = 'AUTO'
            AND symbol IN ('BTCUSDC','ETHUSDC','SOLUSDC','BNBUSDC')
            AND interval IN ('1m','5m')
            AND strategy IN ('RSI','SUPERTREND','TREND','BBRANGE')
        ),
        picks AS (
          SELECT symbol, interval, strategy
          FROM v_orc_picks_v5
        ),
        desired AS (
          SELECT u.*,
                 (p.symbol IS NOT NULL) AS want_on
          FROM universe u
          LEFT JOIN picks p
            ON p.symbol=u.symbol AND p.interval=u.interval AND p.strategy=u.strategy
        ),
        applied AS (
          UPDATE bot_control bc
          SET
            live_orders_enabled = d.want_on,
            control_mode = 'AUTO',
            control_source = 'ORC',
            manual_override_reason = NULL,
            manual_override_updated_at = NULL,
            regime_enabled = true,
            regime_mode = CASE WHEN d.want_on THEN 'ENFORCE' ELSE 'DRY_RUN' END,
            updated_at = now(),
            reason = CASE WHEN d.want_on
                          THEN 'ORC_V5: picked (entries ON, ENFORCE)'
                          ELSE 'ORC_V5: not picked (entries OFF, DRY_RUN)'
                     END,
            live_since = CASE
              WHEN d.want_on = true AND COALESCE(bc.live_orders_enabled,false) = false THEN now()
              ELSE bc.live_since
            END,
            last_disabled_at = CASE
              WHEN d.want_on = false AND COALESCE(bc.live_orders_enabled,false) = true THEN now()
              ELSE bc.last_disabled_at
            END
          FROM desired d
          WHERE bc.symbol=d.symbol AND bc.interval=d.interval AND bc.strategy=d.strategy
            AND COALESCE(bc.control_mode, 'AUTO') = 'AUTO'
            AND (
              bc.live_orders_enabled IS DISTINCT FROM d.want_on
              OR COALESCE(bc.control_mode, 'AUTO') IS DISTINCT FROM 'AUTO'
              OR COALESCE(bc.control_source, 'ORC') IS DISTINCT FROM 'ORC'
              OR bc.manual_override_reason IS NOT NULL
              OR bc.manual_override_updated_at IS NOT NULL
              OR bc.regime_mode IS DISTINCT FROM (CASE WHEN d.want_on THEN 'ENFORCE' ELSE 'DRY_RUN' END)
              OR bc.regime_enabled IS DISTINCT FROM true
              OR bc.reason IS DISTINCT FROM (CASE WHEN d.want_on
                                THEN 'ORC_V5: picked (entries ON, ENFORCE)'
                                ELSE 'ORC_V5: not picked (entries OFF, DRY_RUN)'
                           END)
            )
          RETURNING d.want_on
        )
        SELECT
          (SELECT COUNT(*) FROM picks)                         AS desired_on,
          (SELECT COUNT(*) FROM universe)                      AS universe_n,
          (SELECT COUNT(*) FROM applied)                       AS touched,
          (SELECT COUNT(*) FROM applied WHERE want_on)         AS touched_on,
          (SELECT COUNT(*) FROM applied WHERE NOT want_on)     AS touched_off,
          (SELECT md5(string_agg(symbol||'|'||interval||'|'||strategy, ',' ORDER BY symbol, interval, strategy)) FROM picks) AS picks_hash;
        """

        cur.execute(sql)
        row = cur.fetchone()
        desired_on, universe_n, touched, touched_on, touched_off, picks_hash = row

        stats = {
            "desired_on": int(desired_on or 0),
            "universe_n": int(universe_n or 0),
            "touched": int(touched or 0),
            "touched_on": int(touched_on or 0),
            "touched_off": int(touched_off or 0),
            "picks_hash": str(picks_hash or ""),
            "applied_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
        }

        upsert_kv(cur, "orc_v5_apply_mode", "automation_runner")
        upsert_kv(cur, "orc_v5_apply_last_ts_s", str(now_ts))
        upsert_kv(cur, "orc_v5_apply_last_stats_json", json.dumps(stats, sort_keys=True))

    conn.commit()
    logging.info("orc_v5_apply: %s", stats)


def run_ssot_watchdog(conn):
    """
    Definition of DONE enforcement:
      - no OPEN position without entry_order_id older than 60s
      - no CLOSED position without exit_order_id (ONLY within window)
    """
    window_h = int(os.getenv("SSOT_WATCHDOG_WINDOW_HOURS", "48"))
    logging.info("ssot_watchdog: window_h=%s", window_h)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, strategy, interval, entry_time, entry_client_order_id
            FROM positions
            WHERE status='OPEN'
              AND entry_order_id IS NULL
              AND entry_time < now() - interval '60 seconds'
            LIMIT 50;
        """)
        bad_open = cur.fetchall()

        cur.execute("""
            SELECT id, symbol, strategy, interval, exit_time, exit_client_order_id
            FROM positions
            WHERE status='CLOSED'
              AND exit_time IS NOT NULL
              AND exit_order_id IS NULL
              AND exit_time >= now() - (%s || ' hours')::interval
            LIMIT 50;
        """, (str(window_h),))
        bad_closed = cur.fetchall()

        if not bad_open and not bad_closed:
            return

        details = {"bad_open": bad_open, "bad_closed": bad_closed}
        cur.execute("""
            INSERT INTO watchdog_events(symbol, interval, strategy, severity, event, details)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb)
        """, ("*", None, None, "CRIT", "SSOT_MISSING_ORDER_ID", json.dumps(details, default=_json_default)))

        reason = "FAILSAFE: SSOT missing order_id (positions not fully attached)"
        set_panic(cur, True, reason)
        disable_live_orders(cur, reason)

    conn.commit()


def now_utc_hhmm():
    return datetime.now(timezone.utc).strftime("%H%M")


def run_daily_report(conn):
    if os.getenv("DAILY_REPORT_ENABLED", "0") != "1":
        return

    target = os.getenv("DAILY_REPORT_HHMM_UTC", "0020")
    cur_hhmm = now_utc_hhmm()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logging.info("daily_report: check cur=%s target=%s", cur_hhmm, target)

    with conn.cursor() as cur:
        last = q1(cur, "SELECT value FROM automation_kv WHERE key='daily_report_last_run';")

        # Idempotencja: już wykonane dziś
        if last == today:
            return

        # Jeszcze za wcześnie (nie minęliśmy targetu)
        if cur_hhmm < target:
            return

        sql1 = "/app/scripts/010_daily_report_upsert.sql"
        sql2 = "/app/scripts/011_daily_report_retention.sql"

        logging.info(
            "daily_report: running 010/011 for %s (sql1=%s sql2=%s)",
            os.getenv("DB_NAME"), sql1, sql2
        )

        for path in (sql1, sql2):
            with open(path, "r", encoding="utf-8") as f:
                cur.execute(f.read())

        upsert_kv(cur, "daily_report_last_run", today)

    conn.commit()
    logging.info("daily_report: done")


def run_ip_panic_watch(conn):
    if os.getenv("IP_PANIC_WATCH_ENABLED", "0") != "1":
        return

    url = os.getenv("IPIFY_URL", "https://api.ipify.org")
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        new_ip = r.text.strip()
    except Exception as e:
        logging.warning("ip_panic_watch: could not fetch IP: %s", e)
        return

    with conn.cursor() as cur:
        old_ip = q1(cur, "SELECT value FROM automation_kv WHERE key='last_public_ip';")
        if old_ip == new_ip:
            return

        upsert_kv(cur, "last_public_ip", new_ip)
        reason = f"PUBLIC_IP_CHANGED old='{old_ip}' new='{new_ip}'"
        logging.warning("ip_panic_watch: %s", reason)

        set_panic(cur, True, reason)
        disable_live_orders(cur, "FAILSAFE: panic engaged (IP changed)")

    conn.commit()


def run_regime_watchdog(conn):
    if os.getenv("REGIME_WATCHDOG_ENABLED", "0") != "1":
        return

    # Trigger thresholds
    max_1m = int(os.getenv("REGIME_LAG_MAX_1M_SECONDS", "420"))
    max_5m = int(os.getenv("REGIME_LAG_MAX_5M_SECONDS", "1200"))

    # Clear behaviour (histereza)
    clear_good_ticks = int(os.getenv("REGIME_PANIC_CLEAR_GOOD_TICKS", "3"))
    clear_1m = int(os.getenv("REGIME_LAG_CLEAR_1M_SECONDS", str(max(60, max_1m // 2))))
    clear_5m = int(os.getenv("REGIME_LAG_CLEAR_5M_SECONDS", str(max(180, max_5m // 2))))

    with conn.cursor() as cur:
        # IMPORTANT: staleness = freshness of pipeline, so use created_at not ts
        cur.execute("""
            SELECT interval, EXTRACT(EPOCH FROM (now() - MAX(created_at)))::int AS lag_s
            FROM market_regime
            GROUP BY interval;
        """)
        rows = cur.fetchall()

        bad = []
        good_for_clear = True  # becomes false if any interval violates clear thresholds

        for interval, lag_s in rows:
            if interval == "1m":
                if lag_s > max_1m:
                    bad.append((interval, lag_s, max_1m))
                if lag_s > clear_1m:
                    good_for_clear = False
            elif interval == "5m":
                if lag_s > max_5m:
                    bad.append((interval, lag_s, max_5m))
                if lag_s > clear_5m:
                    good_for_clear = False

        # If stale -> engage panic (latch)
        if bad:
            msg = "FAILSAFE: stale market_regime " + ", ".join([f"{i} lag={ls}s>{mx}s" for i, ls, mx in bad])
            logging.error("regime_watchdog: %s", msg)

            set_panic(cur, True, msg)
            disable_live_orders(cur, msg)

            # reset clear counter whenever we are bad
            upsert_kv(cur, "regime_panic_good_ticks", "0")
            conn.commit()
            return

        # Not bad: maybe clear panic if the reason matches regime stale
        panic_enabled = q1(cur, "SELECT panic_enabled FROM panic_state WHERE id=true;")
        reason = q1(cur, "SELECT reason FROM panic_state WHERE id=true;")

        if panic_enabled and _is_regime_panic_reason(str(reason or "")):
            if good_for_clear:
                ticks = _get_int_kv(cur, "regime_panic_good_ticks", 0) + 1
            else:
                ticks = 0

            upsert_kv(cur, "regime_panic_good_ticks", str(ticks))

            if ticks >= clear_good_ticks:
                logging.warning("regime_watchdog: auto-clearing panic after %s good ticks", ticks)
                set_panic(cur, False, "AUTO-CLEARED: regime freshness OK")
                upsert_kv(cur, "regime_panic_good_ticks", "0")

        conn.commit()


def run_promo_allocator(conn):
    """
    LIVE: apply promotions->allocator (DB-only), idempotent by interval.
    Requires:
      - v_promoted_latest view exists
      - scripts/020_promo_allocator_apply.sql exists in image at /app/scripts/
    """
    if os.getenv("PROMO_ALLOC_ENABLED", "0") != "1":
        return

    interval_s = int(os.getenv("PROMO_ALLOC_INTERVAL_SECONDS", "60"))
    policy_name = os.getenv("PROMO_ALLOC_POLICY_NAME", "default")
    min_trades = int(os.getenv("PROMO_ALLOC_MIN_TRADES", "5"))

    now = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='promo_alloc_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now - last_ts < interval_s:
            return

        path = "/app/scripts/020_promo_allocator_apply.sql"
        apply_bot_control = 0 if cfg.trading_mode == "LIVE" else 1

        logging.info("promo_alloc: applying policy=%s min_trades=%s sql=%s apply_bot_control=%s",
                    policy_name, min_trades, path, apply_bot_control)

        sql = Path(path).read_text(encoding="utf-8")

        sql = sql.replace(":'policy_name'", "'" + policy_name.replace("'", "''") + "'")
        sql = sql.replace("(:'min_trades')::int", str(int(min_trades)))
        sql = sql.replace(":'apply_bot_control'", str(int(apply_bot_control)))

        cur.execute(sql)

        upsert_kv(cur, "promo_alloc_last_ts_s", str(now))

    conn.commit()
    logging.info("promo_alloc: done")



# --- PROMOTIONS: publish from PAPER to LIVE (v1) ---
import json
import hashlib

def get_kv(cur, key: str):
    cur.execute("SELECT value FROM automation_kv WHERE key=%s", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_kv(cur, key: str, value: str):
    cur.execute("""
        INSERT INTO automation_kv(key, value, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (key) DO UPDATE
        SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at;
    """, (key, value))

def _sha256_canon(payload: dict) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",",":")).encode("utf-8")
    return hashlib.sha256(canon).hexdigest()

def publish_promotions(conn):
    """
    PAPER -> LIVE publisher
    Primary: v_ranking_v1 status='CANDIDATE'
    Fallback: v_bot_scoreboard_sim_10d TOP by net_sum (min trades)
    """
    if os.getenv("PROMOTIONS_ENABLED", "0") != "1":
        return False

    live_api_base = (os.getenv("LIVE_API_BASE", "") or "").strip()
    if not live_api_base:
        logging.error("promotions: LIVE_API_BASE not set; skipping")
        return False

    interval_s = int(os.getenv("PROMOTIONS_INTERVAL_SECONDS", "300"))
    top_k = int(os.getenv("PROMOTIONS_TOP_K", "20"))
    min_trades = int(os.getenv("PROMOTIONS_MIN_TRADES", "20"))
    elig_min_trades = int(os.getenv("PROMOTIONS_ELIG_MIN_TRADES", "50"))
    elig_min_pf = float(os.getenv("PROMOTIONS_ELIG_MIN_PF", "1.05"))
    elig_require_pos_net = os.getenv("PROMOTIONS_ELIG_REQUIRE_POS_NET", "1") == "1"

    window_name = os.getenv("PROMOTIONS_WINDOW_NAME", "10d")
    policy_version = os.getenv("PROMOTIONS_POLICY_VERSION", "paper_rank_v1")

    now = time.time()

    with conn.cursor() as cur:
        last_ts_s = get_kv(cur, "promotions_last_ts_s")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now - last_ts < interval_s:
            return False

        # Primary: CANDIDATE from ranking
        cur.execute("""
            WITH r AS (
              SELECT symbol, interval, strategy
              FROM v_ranking_v1
              WHERE status='CANDIDATE'
            ),
            m AS (
              SELECT symbol, interval, strategy, n, net_sum, win_rate, profit_factor
              FROM v_bot_scoreboard_sim_10d
            )
            SELECT
              r.symbol, r.interval, r.strategy,
              COALESCE(m.net_sum, 0::numeric) AS paper_score,
              COALESCE(m.n, 0::bigint)        AS n_trades,
              m.win_rate,
              m.net_sum,
              m.profit_factor
            FROM r
              LEFT JOIN m USING (symbol, interval, strategy)
              WHERE COALESCE(m.n,0) >= %s
                AND COALESCE(m.net_sum,0::numeric) > 0::numeric
                AND COALESCE(m.profit_factor,0::numeric) >= 1.10
              ORDER BY COALESCE(m.net_sum, 0::numeric) DESC
            LIMIT %s;
        """, (min_trades, top_k))
        rows = cur.fetchall()

        mode = "CANDIDATE"
        if not rows:
            # Fallback: TOP scoreboard by net_sum (even if RED), to keep pipeline moving
            cur.execute("""
                SELECT
                  symbol, interval, strategy,
                  net_sum AS paper_score,
                  n       AS n_trades,
                  win_rate,
                  net_sum,
                  profit_factor
                FROM v_bot_scoreboard_sim_10d
                WHERE n >= %s
                ORDER BY net_sum DESC
                LIMIT %s;
            """, (min_trades, top_k))
            rows = cur.fetchall()
            mode = "FALLBACK_TOP_NETSUM"

        if not rows:
            logging.info("promotions: no rows to publish (mode=%s)", mode)
            set_kv(cur, "promotions_last_ts_s", str(now))
            conn.commit()
            return False

        rows_payload = []
        for (symbol, interval, strategy, paper_score, n_trades, win_rate, net_sum, profit_factor) in rows:
            pf = float(profit_factor) if profit_factor is not None else None
            n_int = int(n_trades) if n_trades is not None else 0
            net = float(net_sum) if net_sum is not None else 0.0

            eligible = True
            reasons = []
            if n_int < elig_min_trades:
                eligible = False
                reasons.append(f"n<{elig_min_trades}")
            if pf is None or pf < elig_min_pf:
                eligible = False
                reasons.append(f"pf<{elig_min_pf}")
            if elig_require_pos_net and net <= 0:
                eligible = False
                reasons.append("net_sum<=0")

            elig_reason = "OK" if eligible else ";".join(reasons)
            rows_payload.append({
                "symbol": symbol,
                "interval": interval,
                "strategy": strategy,
                "paper_score": float(paper_score) if paper_score is not None else 0.0,
                "n_trades": int(n_trades) if n_trades is not None else 0,
                "win_rate": float(win_rate) if win_rate is not None else None,
                "net_sum": float(net_sum) if net_sum is not None else None,
                "eligible_live": eligible,
                "elig_reason": elig_reason,
                "meta": {
                    "publisher": "paper_automation_runner",
                    "mode": mode,
                    "profit_factor": pf,
                    "elig_gate": {
                        "min_trades": elig_min_trades,
                        "min_pf": elig_min_pf,
                        "require_pos_net": elig_require_pos_net,
                }}
            })

        payload = {
            "policy_version": policy_version,
            "window_name": window_name,
            "source_ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
            "rows": rows_payload,
        }
        payload_hash = _sha256_canon(payload)
        payload["hash"] = payload_hash

        last_hash = get_kv(cur, "promotions_last_hash")
        if last_hash == payload_hash:
            logging.info("promotions: idempotent (same hash), skipping POST (mode=%s hash=%s)", mode, payload_hash[:12])
            set_kv(cur, "promotions_last_ts_s", str(now))
            conn.commit()
            return False

        url = live_api_base.rstrip("/") + "/internal/promotions/upsert"
        internal_token = (os.getenv("INTERNAL_API_TOKEN", "") or "").strip()

        headers = {
            "Content-Type": "application/json",
        }
        if internal_token:
            headers["X-Internal-Token"] = internal_token

        logging.info(
            "promotions: POST url=%s token_present=%s token_len=%s",
            url,
            bool(internal_token),
            len(internal_token),
        )

        try:
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            j = r.json()
            logging.info("promotions: POST ok (mode=%s) inserted=%s hash=%s",
                         mode, j.get("inserted"), payload_hash[:12])
        except Exception as e:
            logging.exception("promotions: POST failed url=%s err=%r", url, e)
            conn.rollback()
            return False

        set_kv(cur, "promotions_last_ts_s", str(now))
        set_kv(cur, "promotions_last_hash", payload_hash)
        conn.commit()
        return True

# --- /PROMOTIONS ---

def main():
    global last_reconcile_ts, last_ssot_watchdog_ts
    if os.getenv("AUTOMATION_ENABLED", "0") != "1":
        logging.info("AUTOMATION_ENABLED!=1; exiting")
        return

    mode = os.getenv("AUTOMATION_MODE", "DISABLE_ONLY").strip().upper()
    if mode not in ("DISABLE_ONLY", "ACTIVE"):
        logging.error("Refusing to start: unsupported AUTOMATION_MODE=%s", mode)
        return
    
    tick_s = int(os.environ.get("AUTOMATION_TICK_SECONDS", "60"))
    dbname = os.environ.get("DB_NAME", "")

    logging.info("automation-runner: started (mode=%s, db=%s, tick_s=%s)", mode, dbname, tick_s)

    ip_int = int(os.getenv("IP_CHECK_INTERVAL_SECONDS", "60"))
    rg_int = int(os.getenv("REGIME_WATCH_INTERVAL_SECONDS", "60"))

    last_ip = 0.0
    last_rg = 0.0

    while True:
        conn = None
        try:
            conn = get_db_conn()
            conn.autocommit = False

            now = time.time()

            run_daily_report(conn)

            # PAPER only: publish promotions to LIVE
            if cfg.trading_mode != "LIVE":
                publish_promotions(conn)

            # LIVE: apply allocator (ONLY when automation is not DISABLE_ONLY)
            if cfg.trading_mode == "LIVE" and mode != "DISABLE_ONLY":
                run_promo_allocator(conn)

            if cfg.trading_mode == "LIVE" and mode != "DISABLE_ONLY":
                run_orc_v5_apply(conn)

            logging.info("tick ok")

            if now - last_ip >= ip_int:
                run_ip_panic_watch(conn)
                last_ip = now

            if now - last_rg >= rg_int:
                run_regime_watchdog(conn)
                last_rg = now
                now = time.time()

            # Reconcile (co 60s) — only meaningful in LIVE
            if cfg.trading_mode == "LIVE" and (now - last_reconcile_ts >= 60):
                try:
                    reconcile_positions(conn, client, min_age_s=60)
                except Exception:
                    logging.exception("reconcile_positions failed")
                last_reconcile_ts = now

            # SSOT watchdog (co 30s)
            if cfg.trading_mode == "LIVE" and (now - last_ssot_watchdog_ts >= 30):
                try:
                    run_ssot_watchdog(conn)
                except Exception:
                    logging.exception("run_ssot_watchdog failed")
                last_ssot_watchdog_ts = now

        except Exception as e:
            logging.exception("tick failed: %s", str(e))
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

        time.sleep(tick_s)


if __name__ == "__main__":
    main()
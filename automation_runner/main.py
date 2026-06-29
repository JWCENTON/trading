import os
import time
import json
import logging
import requests
import shutil
from datetime import datetime, timezone, date
from pathlib import Path
from common.reconcile_positions import reconcile_positions
from common.db import get_db_conn
from common.runtime import RuntimeConfig
from common.exchange_client import get_market_data_client
from common.worker_heartbeat import record_worker_heartbeat

cfg = RuntimeConfig.from_env()
API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] automation-runner: %(message)s")
client = get_market_data_client()
last_reconcile_ts = 0.0
last_ssot_watchdog_ts = 0.0
last_disk_usage_check_ts = 0.0

ORC_APPLY_VERSION = os.getenv("ORC_APPLY_VERSION", "ORC_V6_3")
ORC_APPLY_MODE = os.getenv("ORC_APPLY_MODE", "COOLDOWN_PROMOTE_HYSTERESIS")
ORC_PICKS_VIEW = os.getenv("ORC_PICKS_VIEW", "v_orc_picks_v5")



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
    Backward-compatible: older DBs use V5; new ORC V6.2/V6.3 may use ORC_V6_2/ORC_V6_3.
    """
    v = str(q1(cur, "SELECT value FROM automation_kv WHERE key='orc_writer_primary';") or "").strip().upper()
    allowed = {"V5", "ORC_V6_2", "V6_2", "ORC_V6_3", "V6_3", ORC_APPLY_VERSION.upper()}
    return v in allowed


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
    LIVE: apply ORC picks view -> bot_control (DB-only), idempotent + throttled.
    Backward-compatible function name; active policy is logged through ORC_APPLY_VERSION.
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
            logging.warning("orc_apply: version=%s mode=%s skip (orc_writer_primary is not active ORC writer)", ORC_APPLY_VERSION, ORC_APPLY_MODE)
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
        WITH picks_base AS (
          SELECT symbol, interval, strategy
          FROM v_orc_v7_shadow_picks
          WHERE eligible_v7_shadow = true
        ),
        universe AS (
          SELECT bc.symbol, bc.interval, bc.strategy
          FROM bot_control bc
          LEFT JOIN picks_base pb
            ON pb.symbol = bc.symbol
           AND pb.interval = bc.interval
           AND pb.strategy = bc.strategy
          WHERE (
              bc.enabled = true
              OR pb.symbol IS NOT NULL
            )
            AND COALESCE(bc.control_mode, 'AUTO') = 'AUTO'
            AND bc.symbol IN ('BTCUSDC','ETHUSDC','SOLUSDC','BNBUSDC')
            AND bc.interval IN ('1m','5m')
            AND bc.strategy IN ('RSI','SUPERTREND','TREND','BBRANGE')
        ),
        explore_enabled AS (
        SELECT COALESCE((
            SELECT value IN ('1','true','TRUE','yes','on')
            FROM automation_kv
            WHERE key = 'orc_v62_explore_enabled'
        ), false) AS enabled
        ),
        picks AS (
        SELECT
            symbol,
            interval,
            strategy,
            'ORC_V6_3' AS pick_source
        FROM v_orc_v7_shadow_picks
        WHERE eligible_v7_shadow = true

        UNION ALL

        SELECT
            e.symbol,
            e.interval,
            e.strategy,
            'ORC_EXPLORE_V1' AS pick_source
        FROM v_orc_exploration_picks_v1 e
        CROSS JOIN explore_enabled x
        WHERE x.enabled = true
        ),
        desired AS (
          SELECT
            u.*,
            (p.symbol IS NOT NULL) AS want_on,
            p.pick_source
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
            reason = CASE
                WHEN d.want_on AND d.pick_source = 'ORC_EXPLORE_V1'
                    THEN 'ORC_EXPLORE_V1: controlled exploration (entries ON, ENFORCE)'
                WHEN d.want_on
                    THEN 'ORC_V7_READY: V6.3 edge + runtime readiness picked (entries ON, ENFORCE)'
                ELSE 'ORC_V7_READY: not ready or not picked (entries OFF, DRY_RUN)'
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
              OR bc.reason IS DISTINCT FROM (CASE
                            WHEN d.want_on AND d.pick_source = 'ORC_EXPLORE_V1'
                                THEN 'ORC_EXPLORE_V1: controlled exploration (entries ON, ENFORCE)'
                            WHEN d.want_on
                                THEN 'ORC_V7_READY: V6.3 edge + runtime readiness picked (entries ON, ENFORCE)'
                            ELSE 'ORC_V7_READY: not ready or not picked (entries OFF, DRY_RUN)'
                            END)
            )
          RETURNING d.want_on
        )
        SELECT
          (SELECT COUNT(*) FROM picks WHERE pick_source='ORC_V6_3')       AS core_picks_n,
          (SELECT COUNT(*) FROM picks WHERE pick_source='ORC_EXPLORE_V1') AS explore_picks_n,
          (SELECT COUNT(*) FROM picks)                                   AS want_on_n,
          (SELECT COUNT(*) FROM universe)                                AS universe_n,
          (SELECT COUNT(*) FROM applied)                                 AS touched,
          (SELECT COUNT(*) FROM applied WHERE want_on)                   AS touched_on,
          (SELECT COUNT(*) FROM applied WHERE NOT want_on)               AS touched_off,
          (SELECT md5(string_agg(symbol||'|'||interval||'|'||strategy||'|'||pick_source, ',' ORDER BY symbol, interval, strategy, pick_source)) FROM picks) AS picks_hash;
        """

        cur.execute(sql)
        row = cur.fetchone()
        core_picks_n, explore_picks_n, want_on_n, universe_n, touched, touched_on, touched_off, picks_hash = row

        stats = {
            "core_picks_n": int(core_picks_n or 0),
            "explore_picks_n": int(explore_picks_n or 0),
            "want_on_n": int(want_on_n or 0),
            "universe_n": int(universe_n or 0),
            "touched": int(touched or 0),
            "touched_on": int(touched_on or 0),
            "touched_off": int(touched_off or 0),
            "picks_hash": str(picks_hash or ""),
            "applied_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
            "orc_version": ORC_APPLY_VERSION,
            "orc_mode": ORC_APPLY_MODE,
            "picks_view": "v_orc_v7_shadow_picks",
        }

        upsert_kv(cur, "orc_v5_apply_mode", "automation_runner")
        upsert_kv(cur, "orc_active_version", ORC_APPLY_VERSION)
        upsert_kv(cur, "orc_active_mode", ORC_APPLY_MODE)
        upsert_kv(cur, "orc_v62_explore_enabled", "0")
        upsert_kv(cur, "orc_v63_explore_enabled", "0")
        upsert_kv(cur, "orc_v5_apply_last_ts_s", str(now_ts))
        upsert_kv(cur, "orc_v5_apply_last_stats_json", json.dumps(stats, sort_keys=True))

    conn.commit()
    logging.info("orc_apply: version=%s mode=%s stats=%s", ORC_APPLY_VERSION, ORC_APPLY_MODE, stats)








def run_market_memory_timeline_refresh(conn):
    """
    Refresh Market Memory event timeline / early reversal chain.
    Analytics-only: writes market_memory_timeline; does not change trading state.
    """
    if os.getenv("MARKET_MEMORY_TIMELINE_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_TIMELINE_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_timeline_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_timeline_v1'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_timeline: refresh function missing; skip")
            upsert_kv(cur, "market_memory_timeline_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_timeline_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_timeline_v1();")

        cur.execute("""
            SELECT
              COUNT(*) AS active_timelines,
              COUNT(*) FILTER (WHERE timeline_type='EARLY_REVERSAL_UP') AS early_reversal_up,
              COUNT(*) FILTER (WHERE timeline_type='FULL_IMPULSE_UP_CHAIN') AS full_impulse_up,
              COUNT(*) FILTER (WHERE chain_importance='EXTREME') AS extreme_chains,
              COUNT(*) FILTER (WHERE chain_importance='HIGH') AS high_chains,
              COUNT(*) FILTER (WHERE chain_importance='MEDIUM') AS medium_chains,
              ROUND(MAX(chain_score), 4) AS max_chain_score
            FROM v_market_memory_timeline_active;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              timeline_type,
              direction,
              chain_importance,
              ROUND(chain_score, 4) AS chain_score,
              chain_length,
              long_context,
              short_context,
              has_volume_spike,
              has_atr_expansion,
              has_breakout_up,
              has_momentum_up,
              first_event_at,
              last_event_at,
              chain_age_minutes,
              expires_at,
              reason
            FROM v_market_memory_timeline_active
            ORDER BY chain_score DESC NULLS LAST, last_event_at DESC
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_timelines": int(r[0] or 0),
            "early_reversal_up": int(r[1] or 0),
            "full_impulse_up": int(r[2] or 0),
            "extreme_chains": int(r[3] or 0),
            "high_chains": int(r[4] or 0),
            "medium_chains": int(r[5] or 0),
            "max_chain_score": float(r[6] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "timeline_type": x[2],
                    "direction": x[3],
                    "chain_importance": x[4],
                    "chain_score": float(x[5] or 0),
                    "chain_length": int(x[6] or 0),
                    "long_context": x[7],
                    "short_context": x[8],
                    "has_volume_spike": bool(x[9]),
                    "has_atr_expansion": bool(x[10]),
                    "has_breakout_up": bool(x[11]),
                    "has_momentum_up": bool(x[12]),
                    "first_event_at": x[13].isoformat() if x[13] else None,
                    "last_event_at": x[14].isoformat() if x[14] else None,
                    "chain_age_minutes": float(x[15] or 0),
                    "expires_at": x[16].isoformat() if x[16] else None,
                    "reason": x[17],
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_timeline_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_timeline_last_status", "ok")
        upsert_kv(cur, "market_memory_timeline_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("market_memory_timeline: active=%s max_score=%s early_reversal=%s",
                 stats["active_timelines"], stats["max_chain_score"], stats["early_reversal_up"])


def run_market_memory_clusters_refresh(conn):
    """
    Refresh Market Memory event clusters.
    Analytics-only: writes market_memory_event_clusters; does not change trading state.
    """
    if os.getenv("MARKET_MEMORY_CLUSTERS_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_CLUSTERS_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_clusters_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_event_clusters_v1'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_clusters: refresh function missing; skip")
            upsert_kv(cur, "market_memory_clusters_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_clusters_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_event_clusters_v1();")

        cur.execute("""
            SELECT
              COUNT(*) AS active_clusters,
              COUNT(*) FILTER (WHERE cluster_importance='EXTREME') AS extreme_clusters,
              COUNT(*) FILTER (WHERE cluster_importance='HIGH') AS high_clusters,
              COUNT(*) FILTER (WHERE cluster_importance='MEDIUM') AS medium_clusters,
              COUNT(*) FILTER (WHERE cluster_type='IMPULSE_UP_CLUSTER') AS impulse_up,
              COUNT(*) FILTER (WHERE cluster_type='MOMENTUM_BREAKOUT_CLUSTER') AS momentum_breakout,
              COUNT(*) FILTER (WHERE cluster_type='REVERSAL_CLUSTER') AS reversal_cluster,
              ROUND(MAX(cluster_score), 4) AS max_cluster_score
            FROM v_market_memory_event_clusters_active;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              cluster_type,
              direction,
              cluster_importance,
              ROUND(cluster_score, 4) AS cluster_score,
              event_count,
              volume_spike,
              atr_expansion,
              breakout_up,
              momentum_up,
              reversal_up_candidate,
              first_observed_at,
              last_observed_at,
              expires_at,
              reason
            FROM v_market_memory_event_clusters_active
            ORDER BY cluster_score DESC NULLS LAST, last_observed_at DESC
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_clusters": int(r[0] or 0),
            "extreme_clusters": int(r[1] or 0),
            "high_clusters": int(r[2] or 0),
            "medium_clusters": int(r[3] or 0),
            "impulse_up": int(r[4] or 0),
            "momentum_breakout": int(r[5] or 0),
            "reversal_cluster": int(r[6] or 0),
            "max_cluster_score": float(r[7] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "cluster_type": x[2],
                    "direction": x[3],
                    "cluster_importance": x[4],
                    "cluster_score": float(x[5] or 0),
                    "event_count": int(x[6] or 0),
                    "volume_spike": int(x[7] or 0),
                    "atr_expansion": int(x[8] or 0),
                    "breakout_up": int(x[9] or 0),
                    "momentum_up": int(x[10] or 0),
                    "reversal_up_candidate": int(x[11] or 0),
                    "first_observed_at": x[12].isoformat() if x[12] else None,
                    "last_observed_at": x[13].isoformat() if x[13] else None,
                    "expires_at": x[14].isoformat() if x[14] else None,
                    "reason": x[15],
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_clusters_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_clusters_last_status", "ok")
        upsert_kv(cur, "market_memory_clusters_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("market_memory_clusters: active=%s max_score=%s", stats["active_clusters"], stats["max_cluster_score"])


def run_market_memory_events_refresh(conn):
    """
    Refresh short-term Market Memory events.
    Analytics-only: writes market_memory_events; does not change trading state.
    """
    if os.getenv("MARKET_MEMORY_EVENTS_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_EVENTS_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_events_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_events_v1'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_events: refresh function missing; skip")
            upsert_kv(cur, "market_memory_events_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_events_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_events_v1();")

        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE expires_at > now()) AS active_events,
              COUNT(*) FILTER (WHERE event_type='REVERSAL_UP_CANDIDATE' AND expires_at > now()) AS reversal_up,
              COUNT(*) FILTER (WHERE event_type='BREAKOUT_UP' AND expires_at > now()) AS breakout_up,
              COUNT(*) FILTER (WHERE event_type='MOMENTUM_UP' AND expires_at > now()) AS momentum_up,
              COUNT(*) FILTER (WHERE event_type='VOLUME_SPIKE' AND expires_at > now()) AS volume_spike,
              COUNT(*) FILTER (WHERE event_type='ATR_EXPANSION' AND expires_at > now()) AS atr_expansion,
              ROUND(MAX(score) FILTER (WHERE expires_at > now()), 4) AS max_score
            FROM market_memory_events;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              event_type,
              ROUND(score, 4) AS score,
              direction,
              regime,
              ROUND(confidence, 4) AS confidence,
              window_label,
              observed_at,
              expires_at,
              reason
            FROM v_market_memory_events_active
            ORDER BY score DESC NULLS LAST, observed_at DESC
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_events": int(r[0] or 0),
            "reversal_up": int(r[1] or 0),
            "breakout_up": int(r[2] or 0),
            "momentum_up": int(r[3] or 0),
            "volume_spike": int(r[4] or 0),
            "atr_expansion": int(r[5] or 0),
            "max_score": float(r[6] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "event_type": x[2],
                    "score": float(x[3] or 0),
                    "direction": x[4],
                    "regime": x[5],
                    "confidence": float(x[6] or 0),
                    "window_label": x[7],
                    "observed_at": x[8].isoformat() if x[8] else None,
                    "expires_at": x[9].isoformat() if x[9] else None,
                    "reason": x[10],
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_events_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_events_last_status", "ok")
        upsert_kv(cur, "market_memory_events_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("market_memory_events: active=%s max_score=%s", stats["active_events"], stats["max_score"])


def run_market_memory_snapshot_refresh(conn):
    """
    Refresh Market Memory snapshots for multiple windows.
    Analytics-only: does not change bot_control, orders, positions, ORC picks or risk.
    """
    if os.getenv("MARKET_MEMORY_REFRESH_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_REFRESH_INTERVAL_SECONDS", "300"))
    now_ts = time.time()

    windows = [
        ("15m", 15),
        ("1h", 60),
        ("6h", 360),
        ("24h", 1440),
        ("7d", 10080),
        ("30d", 43200),
        ("90d", 129600),
    ]

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_refresh_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_snapshot'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_refresh: refresh function missing; skip")
            upsert_kv(cur, "market_memory_refresh_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_refresh_last_status", "missing_function")
            conn.commit()
            return

        refreshed = []
        for label, minutes in windows:
            cur.execute("SELECT refresh_market_memory_snapshot(%s, %s);", (label, minutes))
            refreshed.append(label)

        cur.execute("""
            SELECT
              window_label,
              COUNT(*) AS rows,
              COUNT(*) FILTER (WHERE status='HOT') AS hot,
              COUNT(*) FILTER (WHERE status='ACTIVE') AS active,
              COUNT(*) FILTER (WHERE status='OBSERVE') AS observe,
              COUNT(*) FILTER (WHERE status='NO_DATA') AS no_data,
              ROUND(MAX(realtime_score), 4) AS max_realtime_score
            FROM market_memory_snapshot
            GROUP BY window_label
            ORDER BY window_label;
        """)
        rows = cur.fetchall()

        summary_by_window = {
            r[0]: {
                "window_label": r[0],
                "rows": int(r[1] or 0),
                "hot": int(r[2] or 0),
                "active": int(r[3] or 0),
                "observe": int(r[4] or 0),
                "no_data": int(r[5] or 0),
                "max_realtime_score": float(r[6] or 0),
            }
            for r in rows
        }

        stats = {
            "windows": refreshed,
            "summary": [
                summary_by_window.get(label, {
                    "window_label": label,
                    "rows": 0,
                    "hot": 0,
                    "active": 0,
                    "observe": 0,
                    "no_data": 0,
                    "max_realtime_score": 0,
                })
                for label, _minutes in windows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_refresh_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_refresh_last_status", "ok")
        upsert_kv(cur, "market_memory_refresh_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("market_memory_refresh: refreshed windows=%s", ",".join(refreshed))


def run_slot_brain_snapshot_refresh(conn):
    """
    Refresh Slot Brain snapshots for multiple windows.
    Analytics-only: does not change bot_control, orders, positions, ORC picks or risk.
    """
    if os.getenv("SLOT_BRAIN_REFRESH_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("SLOT_BRAIN_REFRESH_INTERVAL_SECONDS", "300"))
    now_ts = time.time()

    windows = [
        ("15m", 15),
        ("1h", 60),
        ("6h", 360),
        ("24h", 1440),
        ("7d", 10080),
        ("30d", 43200),
        ("90d", 129600),
    ]

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='slot_brain_refresh_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_slot_brain_snapshot'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("slot_brain_refresh: refresh function missing; skip")
            upsert_kv(cur, "slot_brain_refresh_last_ts_s", str(now_ts))
            upsert_kv(cur, "slot_brain_refresh_last_status", "missing_function")
            conn.commit()
            return

        refreshed = []
        for label, minutes in windows:
            cur.execute("SELECT refresh_slot_brain_snapshot(%s, %s);", (label, minutes))
            refreshed.append(label)

        cur.execute("""
            SELECT
              window_label,
              COUNT(*) AS rows,
              COUNT(*) FILTER (WHERE edge_status='ALLOW_LIVE') AS allow_live,
              COUNT(*) FILTER (WHERE edge_status='OBSERVE') AS observe,
              COUNT(*) FILTER (WHERE edge_status='BLOCK_LIVE') AS block_live
            FROM slot_brain_snapshot
            GROUP BY window_label
            ORDER BY window_label;
        """)
        rows = cur.fetchall()

        summary_by_window = {
            r[0]: {
                "window_label": r[0],
                "rows": int(r[1] or 0),
                "allow_live": int(r[2] or 0),
                "observe": int(r[3] or 0),
                "block_live": int(r[4] or 0),
            }
            for r in rows
        }

        stats = {
            "windows": refreshed,
            "summary": [
                summary_by_window.get(label, {
                    "window_label": label,
                    "rows": 0,
                    "allow_live": 0,
                    "observe": 0,
                    "block_live": 0,
                })
                for label, _minutes in windows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "slot_brain_refresh_last_ts_s", str(now_ts))
        upsert_kv(cur, "slot_brain_refresh_last_status", "ok")
        upsert_kv(cur, "slot_brain_refresh_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("slot_brain_refresh: refreshed windows=%s", ",".join(refreshed))


def run_mfe_mae_snapshot_refresh(conn):
    """
    Periodically refreshes read-optimized MFE/MAE/Profit Giveback snapshot.
    This is analytics-only: it does not change trading, ORC picks, bot_control,
    positions, orders, or risk state.
    """
    if os.getenv("MFE_MAE_SNAPSHOT_REFRESH_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MFE_MAE_SNAPSHOT_REFRESH_INTERVAL_SECONDS", "300"))
    days_back = int(os.getenv("MFE_MAE_SNAPSHOT_DAYS_BACK", "30"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='mfe_mae_snapshot_refresh_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_trade_mfe_mae_snapshot'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("mfe_mae_snapshot: refresh function missing; skip")
            upsert_kv(cur, "mfe_mae_snapshot_refresh_last_ts_s", str(now_ts))
            upsert_kv(cur, "mfe_mae_snapshot_refresh_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT * FROM refresh_trade_mfe_mae_snapshot(%s);", (days_back,))
        row = cur.fetchone()
        refreshed_rows = int(row[0] or 0) if row else 0
        min_exit_time = row[1] if row else None
        max_exit_time = row[2] if row else None

        stats = {
            "days_back": days_back,
            "refreshed_rows": refreshed_rows,
            "min_exit_time": min_exit_time,
            "max_exit_time": max_exit_time,
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "mfe_mae_snapshot_refresh_last_ts_s", str(now_ts))
        upsert_kv(cur, "mfe_mae_snapshot_refresh_last_status", "ok")
        upsert_kv(cur, "mfe_mae_snapshot_refresh_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "mfe_mae_snapshot: refreshed rows=%s days_back=%s min_exit_time=%s max_exit_time=%s",
        refreshed_rows,
        days_back,
        min_exit_time,
        max_exit_time,
    )


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




def run_market_regime_confidence_refresh(conn):
    if os.getenv("MARKET_REGIME_CONFIDENCE_REFRESH_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_REGIME_CONFIDENCE_REFRESH_INTERVAL_SECONDS", "300"))
    days_back = int(os.getenv("MARKET_REGIME_CONFIDENCE_REFRESH_DAYS_BACK", "2"))
    now_ts = int(time.time())

    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_proc
                WHERE proname = 'refresh_market_regime_confidence'
            );
        """)
        exists = bool(cur.fetchone()[0])
        if not exists:
            logging.info("market_regime_confidence_refresh: function missing, skip")
            return

        last = q1(cur, "SELECT value FROM automation_kv WHERE key='market_regime_confidence_refresh_last_ts_s';")
        if last:
            try:
                if now_ts - int(last) < interval_s:
                    return
            except Exception:
                pass

        logging.info("market_regime_confidence_refresh: running")
        cur.execute("SELECT refresh_market_regime_confidence(%s);", (days_back,))
        updated = cur.fetchone()[0]
        logging.info("market_regime_confidence_refresh: updated=%s days_back=%s", updated, days_back)
        upsert_kv(cur, "market_regime_confidence_refresh_last_ts_s", str(now_ts))

    conn.commit()
    logging.info("market_regime_confidence_refresh: done")

def run_strategy_regime_stats_refresh(conn):
    if os.getenv("STRATEGY_REGIME_STATS_REFRESH_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("STRATEGY_REGIME_STATS_REFRESH_INTERVAL_SECONDS", "3600"))
    now_ts = int(time.time())

    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_proc
                WHERE proname = 'refresh_strategy_regime_stats'
            );
        """)
        exists = bool(cur.fetchone()[0])
        if not exists:
            logging.info("strategy_regime_stats_refresh: function missing, skip")
            return

        last = q1(cur, "SELECT value FROM automation_kv WHERE key='strategy_regime_stats_refresh_last_ts_s';")
        if last:
            try:
                if now_ts - int(last) < interval_s:
                    return
            except Exception:
                pass

        logging.info("strategy_regime_stats_refresh: running")
        cur.execute("SELECT refresh_strategy_regime_stats();")
        upsert_kv(cur, "strategy_regime_stats_refresh_last_ts_s", str(now_ts))

    conn.commit()
    logging.info("strategy_regime_stats_refresh: done")

def ensure_ui_notifications_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ui_notifications (
          id BIGSERIAL PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          event_type TEXT NOT NULL,
          category TEXT NOT NULL DEFAULT 'CRITICAL',
          severity TEXT NOT NULL DEFAULT 'info',
          title TEXT NOT NULL,
          message TEXT NOT NULL,
          source TEXT,
          read_at TIMESTAMPTZ,
          meta JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        """
    )
    cur.execute("ALTER TABLE ui_notifications ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'CRITICAL';")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ui_notifications_created_at
          ON ui_notifications(created_at DESC);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ui_notifications_read_at
          ON ui_notifications(read_at);
        """
    )


def create_ui_notification_dedup(cur, *, event_type: str, severity: str, title: str, message: str, source: str, meta: dict, dedupe_minutes: int = 1440):
    ensure_ui_notifications_table(cur)
    cur.execute(
        """
        SELECT id
        FROM ui_notifications
        WHERE event_type = %s
          AND created_at >= now() - (%s || ' minutes')::interval
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (event_type, str(dedupe_minutes)),
    )
    if cur.fetchone():
        return None

    cur.execute(
        """
        INSERT INTO ui_notifications (event_type, category, severity, title, message, source, meta)
        VALUES (%s, 'CRITICAL', %s, %s, %s, %s, %s::jsonb)
        RETURNING id;
        """,
        (event_type, severity, title, message, source, json.dumps(meta, default=_json_default)),
    )
    return cur.fetchone()[0]


def create_ui_recovery_notification_if_needed(cur, *, failure_event_types: tuple[str, ...], recovered_event_type: str, title: str, message: str, source: str, meta: dict, dedupe_minutes: int = 360):
    ensure_ui_notifications_table(cur)
    cur.execute(
        """
        SELECT id
        FROM ui_notifications
        WHERE event_type = ANY(%s)
          AND read_at IS NULL
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (list(failure_event_types),),
    )
    if not cur.fetchone():
        return None

    cur.execute(
        """
        UPDATE ui_notifications
        SET read_at = COALESCE(read_at, now())
        WHERE event_type = ANY(%s)
          AND read_at IS NULL;
        """,
        (list(failure_event_types),),
    )
    return create_ui_notification_dedup(
        cur,
        event_type=recovered_event_type,
        severity="success",
        title=title,
        message=message,
        source=source,
        meta=meta,
        dedupe_minutes=dedupe_minutes,
    )


def run_disk_usage_notification_check(conn):
    if os.getenv("DISK_USAGE_ALERT_ENABLED", "1") != "1":
        return

    path = os.getenv("DISK_USAGE_PATH", "/")
    warning_pct = float(os.getenv("DISK_USAGE_WARNING_PCT", "80"))
    danger_pct = float(os.getenv("DISK_USAGE_DANGER_PCT", "90"))
    dedupe_minutes = int(os.getenv("DISK_USAGE_ALERT_DEDUPE_MINUTES", "1440"))

    usage = shutil.disk_usage(path)
    total_gb = usage.total / (1024 ** 3)
    used_gb = usage.used / (1024 ** 3)
    free_gb = usage.free / (1024 ** 3)
    used_pct = (usage.used / usage.total * 100.0) if usage.total else 0.0

    meta = {
        "environment": os.getenv("ENVIRONMENT"),
        "trading_mode": cfg.trading_mode,
        "path": path,
        "used_pct": round(used_pct, 2),
        "warning_pct": warning_pct,
        "danger_pct": danger_pct,
        "total_gb": round(total_gb, 2),
        "used_gb": round(used_gb, 2),
        "free_gb": round(free_gb, 2),
    }

    with conn.cursor() as cur:
        if used_pct >= danger_pct:
            create_ui_notification_dedup(
                cur,
                event_type="disk_usage_danger",
                severity="danger",
                title="Disk usage danger",
                message=f"Disk usage on {path} is {used_pct:.1f}% ({free_gb:.1f} GB free).",
                source="disk_usage",
                meta=meta,
                dedupe_minutes=dedupe_minutes,
            )
        elif used_pct >= warning_pct:
            create_ui_notification_dedup(
                cur,
                event_type="disk_usage_warning",
                severity="warning",
                title="Disk usage warning",
                message=f"Disk usage on {path} is {used_pct:.1f}% ({free_gb:.1f} GB free).",
                source="disk_usage",
                meta=meta,
                dedupe_minutes=dedupe_minutes,
            )
        else:
            create_ui_recovery_notification_if_needed(
                cur,
                failure_event_types=("disk_usage_warning", "disk_usage_danger"),
                recovered_event_type="disk_usage_recovered",
                title="Disk usage recovered",
                message=f"Disk usage on {path} recovered to {used_pct:.1f}% ({free_gb:.1f} GB free).",
                source="disk_usage",
                meta=meta,
                dedupe_minutes=360,
            )

    conn.commit()


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

        # Regime-aware promotions v1: publish positive slot+regime edges separately.
        # This does not replace legacy promoted_candidates; it feeds promoted_regime_candidates.
        try:
            cur.execute("""
                SELECT
                  symbol,
                  interval,
                  strategy,
                  market_regime,
                  net_pnl AS paper_score,
                  trades AS n_trades,
                  win_rate_pct / 100.0 AS win_rate,
                  net_pnl AS net_sum,
                  profit_factor_net AS profit_factor,
                  fee_pressure_pct
                FROM v_slot_profile_v1_14d
                WHERE edge_status = 'ALLOW_LIVE_CANDIDATE'
                ORDER BY net_pnl DESC
                LIMIT %s
            """, (top_k,))
            regime_rows = cur.fetchall()
        except Exception:
            logging.exception("regime-promotions: failed to query v_slot_profile_v1_14d")
            regime_rows = []

        if regime_rows:
            regime_rows_payload = []
            for (
                symbol,
                interval,
                strategy,
                market_regime,
                paper_score,
                n_trades,
                win_rate,
                net_sum,
                profit_factor,
                fee_pressure_pct,
            ) in regime_rows:
                regime_rows_payload.append({
                    "symbol": symbol,
                    "interval": interval,
                    "strategy": strategy,
                    "market_regime": market_regime,
                    "paper_score": float(paper_score) if paper_score is not None else 0.0,
                    "n_trades": int(n_trades) if n_trades is not None else 0,
                    "win_rate": float(win_rate) if win_rate is not None else None,
                    "net_sum": float(net_sum) if net_sum is not None else None,
                    "profit_factor": float(profit_factor) if profit_factor is not None else None,
                    "fee_pressure_pct": float(fee_pressure_pct) if fee_pressure_pct is not None else None,
                    "eligible_live": True,
                    "elig_reason": "OK",
                    "meta": {
                        "publisher": "paper_automation_runner",
                        "mode": "REGIME_AWARE_SLOT_PROFILE_V1",
                        "source_view": "v_slot_profile_v1_14d",
                        "edge_status": "ALLOW_LIVE_CANDIDATE",
                    },
                })

            regime_payload = {
                "policy_version": f"{policy_version}_regime_v1",
                "window_name": "14d_regime",
                "source_ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
                "rows": regime_rows_payload,
            }
            regime_payload_hash = _sha256_canon(regime_payload)
            regime_payload["hash"] = regime_payload_hash

            last_regime_hash = get_kv(cur, "regime_promotions_last_hash")
            if last_regime_hash == regime_payload_hash:
                logging.info(
                    "regime-promotions: idempotent (same hash), skipping POST (hash=%s)",
                    regime_payload_hash[:12],
                )
            else:
                try:
                    regime_promotions_api_url = os.environ.get(
                        "PROMOTIONS_API_BASE",
                        os.environ.get("INTERNAL_API_BASE", "http://api:8000"),
                    )
                    # PAPER publishes promotions to LIVE API. Existing legacy publisher resolves this to live-api;
                    # keep the same behavior here when INTERNAL_API_BASE points at paper-api.
                    if "paper-api" in regime_promotions_api_url:
                        regime_promotions_api_url = regime_promotions_api_url.replace("paper-api", "live-api")

                    regime_promotions_token = os.environ.get("INTERNAL_API_TOKEN", "")
                    regime_promotions_headers = {}
                    if regime_promotions_token:
                        regime_promotions_headers["X-Internal-Token"] = regime_promotions_token

                    resp = requests.post(
                        f"{regime_promotions_api_url.rstrip('/')}/internal/regime-promotions/upsert",
                        json=regime_payload,
                        headers=regime_promotions_headers,
                        timeout=10,
                    )
                    if resp.status_code >= 300:
                        logging.warning(
                            "regime-promotions: POST failed status=%s body=%s",
                            resp.status_code,
                            resp.text[:500],
                        )
                    else:
                        logging.info(
                            "regime-promotions: published rows=%s hash=%s",
                            len(regime_rows_payload),
                            regime_payload_hash[:12],
                        )
                        set_kv(cur, "regime_promotions_last_hash", regime_payload_hash)
                except Exception:
                    logging.exception("regime-promotions: POST failed")

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
    global last_reconcile_ts, last_ssot_watchdog_ts, last_disk_usage_check_ts
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
    disk_int = int(os.getenv("DISK_USAGE_CHECK_INTERVAL_SECONDS", "300"))

    last_ip = 0.0
    last_rg = 0.0

    while True:
        tick_start = time.perf_counter()
        tick_error = None
        conn = None
        try:
            conn = get_db_conn()
            conn.autocommit = False

            now = time.time()

            run_daily_report(conn)

            try:
                run_strategy_regime_stats_refresh(conn)
            except Exception:
                logging.exception("strategy_regime_stats_refresh failed")

            try:
                run_market_regime_confidence_refresh(conn)
            except Exception:
                logging.exception("market_regime_confidence_refresh failed")

            try:
                run_market_memory_events_refresh(conn)
            except Exception:
                logging.exception("market_memory_events_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_clusters_refresh(conn)
            except Exception:
                logging.exception("market_memory_clusters_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_timeline_refresh(conn)
            except Exception:
                logging.exception("market_memory_timeline_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_snapshot_refresh(conn)
            except Exception:
                logging.exception("market_memory_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_slot_brain_snapshot_refresh(conn)
            except Exception:
                logging.exception("slot_brain_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_mfe_mae_snapshot_refresh(conn)
            except Exception:
                logging.exception("mfe_mae_snapshot: refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            if now - last_disk_usage_check_ts >= disk_int:
                try:
                    run_disk_usage_notification_check(conn)
                except Exception:
                    logging.exception("disk usage notification check failed")
                last_disk_usage_check_ts = now

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
            tick_error = e
            try:
                if conn is not None and not getattr(conn, "closed", True):
                    conn.rollback()
            except Exception as rollback_error:
                logging.warning("rollback skipped/failed after tick error: %s", rollback_error)
            logging.exception("tick failed: %s", str(e))
        finally:
            elapsed = time.perf_counter() - tick_start
            try:
                record_worker_heartbeat(
                    "automation-runner",
                    status="degraded" if tick_error else "healthy",
                    error=tick_error,
                    loop_duration_s=elapsed,
                    meta={"mode": mode, "tick_seconds": tick_s, "trading_mode": cfg.trading_mode},
                    conn=conn if conn is not None and not getattr(conn, "closed", True) else None,
                )
                if conn is not None and not getattr(conn, "closed", True):
                    conn.commit()
            except Exception:
                logging.exception("automation heartbeat failed")
            try:
                if conn is not None and not getattr(conn, "closed", True):
                    conn.close()
            except Exception:
                pass

        time.sleep(tick_s)


if __name__ == "__main__":
    main()
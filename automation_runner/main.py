import os
import time
import json
import logging
import requests
import shutil
import hashlib
import uuid
from functools import wraps
from datetime import datetime, timezone, date
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from common.reconcile_positions import reconcile_positions
from common.db import get_db_conn
from common.runtime import RuntimeConfig
from common.exchange_client import get_market_data_client
from common.worker_heartbeat import record_worker_heartbeat
from common.entry_fill_reconciliation import run_pending_entry_reconciliation_if_due
from common.decision_observation_transport import (
    DecisionObservationOutboxConsumer,
    TransportFlags,
    TransportMetrics,
)
from common.control_plane_authority import (
    CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID,
    try_acquire_control_plane_apply_lock,
)
from common.orc_apply_ledger import (
    EXECUTION_MODE_APPLY,
    EXECUTION_MODE_OBSERVE_ONLY,
    SCHEMA_VERSION as ORC_LEDGER_SCHEMA_VERSION,
    WriterIdentity,
    deterministic_picks_hash,
    insert_slot_decision,
    validate_slot_counts,
    make_slot_decision,
    parse_required_execution_guard,
    resolve_execution_mode,
    rows_as_dicts,
    utc_now,
)
from common.learning_evidence_context import (
    set_learning_evidence_transaction_context,
)
from common.bounded_horizon_label_automation import (
    run_bounded_horizon_label_automation,
)
from common.thesis_evidence_bundle import (
    canonical_evidence_cutoff,
    capture_thesis_evidence_bundle_cycle,
)
from common.equity_curve import (
    collect_current_equity,
    ensure_paper_equity_baseline_v2,
    upsert_daily_snapshot,
)
from common.live_managed_capital import (
    load_live_managed_capital_evidence,
    record_live_managed_equity_observation,
)
from common.owner_capital_flow_sync import run_owner_capital_flow_sync_if_due
from common.live_drawdown_history import (
    capture_observation_candidate,
    persist_observation_candidate,
    reemit_late_event_history,
    select_observation_trigger,
)
from common.paper_drawdown_history import (
    run_paper_drawdown_history_cycle as produce_paper_drawdown_history_cycle,
)
from common.portfolio_state import read_portfolio_state
from common.risk_budget_runtime import (
    run_risk_budget_state_evaluation_cycle,
)

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

LEARNING_FEEDBACK_SCHEDULER_VERSION = "LEARNING_FEEDBACK_SCHEDULER_V1_2"
LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION = "LEARNING_FEEDBACK_ENGINE_V1_2"
LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION = "LEARNING_ENGINE_V1_4"
LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE = "SHADOW"
LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED = False

def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}

ORC_INTEGRATION_V2_APPLY_ENABLED = _env_bool("ORC_INTEGRATION_V2_APPLY_ENABLED", "0")
ORC_LEDGER_OBSERVE_ONLY_ENABLED = _env_bool(
    "ORC_LEDGER_OBSERVE_ONLY_ENABLED", "0"
)

CAUSAL_TRANSPORT_METRICS = TransportMetrics()
_last_thesis_evidence_cutoff = None
_live_drawdown_pending = {}


def run_paper_drawdown_history_cycle():
    """PAPER Portfolio State observation -> immutable drawdown history."""
    if cfg.trading_mode.upper() != "PAPER":
        return {"status": "ENVIRONMENT_FENCE"}
    deployment_id = os.getenv("DEPLOYMENT_ID", "").strip().lower()
    if deployment_id not in {"local-paper", "vps-paper"}:
        raise RuntimeError("PAPER_DRAWDOWN_DEPLOYMENT_INVALID")
    return produce_paper_drawdown_history_cycle(
        connection_factory=get_db_conn,
        deployment_id=deployment_id,
        git_revision=os.getenv("GIT_SHA", "").strip().lower(),
    )


def run_live_drawdown_history_cycle():
    """Capture now, then append only after the lagged flow watermark covers now."""
    if not _env_bool("LIVE_DRAWDOWN_HISTORY_V1_ENABLED", "0"):
        return {"status": "DISABLED"}
    if cfg.trading_mode.upper() != "LIVE":
        return {"status": "ENVIRONMENT_FENCE"}
    deployment_id = os.getenv("DEPLOYMENT_ID", "").strip().lower()
    if deployment_id not in {"local-live", "vps-live"}:
        raise RuntimeError("LIVE_DRAWDOWN_DEPLOYMENT_INVALID")
    connection = get_db_conn()
    connection.autocommit = False
    persisted = []
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.v_live_drawdown_history_observation_v1')"
            )
            if cur.fetchone()[0] is None:
                connection.rollback()
                return {"status": "SCHEMA_UNAVAILABLE"}
            cur.execute(
                """SELECT account_identity_fingerprint
                   FROM live_managed_capital_baseline_v1
                   WHERE environment='LIVE' AND deployment_id=%s
                   ORDER BY accepted_at DESC LIMIT 1""",
                (deployment_id,),
            )
            identity_row = cur.fetchone()
            reemitted = 0
            if identity_row:
                reemitted = reemit_late_event_history(
                    cur, deployment_id=deployment_id,
                    account_identity_fingerprint=str(identity_row[0]),
                )
                if reemitted:
                    connection.commit()
            for identity, candidate in tuple(_live_drawdown_pending.items()):
                result = persist_observation_candidate(cur, candidate)
                if result.status == "CANONICAL":
                    connection.commit()
                    persisted.append(result.observation_id)
                    _live_drawdown_pending.pop(identity, None)
                elif result.status != "INCOMPLETE_CAPITAL_FLOW":
                    connection.rollback()
                    return {"status": result.status}
            trigger = select_observation_trigger(
                cur, now=datetime.now(timezone.utc),
                pending_keys=(
                    (candidate.observation_trigger, candidate.trigger_reference)
                    for candidate in _live_drawdown_pending.values()
                ),
            )
            if trigger is None:
                connection.rollback()
                return {"status": "CANONICAL", "persisted": persisted}
            observed_at = datetime.now(timezone.utc)
            bundle = load_live_managed_capital_evidence(
                cur, exchange_client=client, deployment_id=deployment_id,
                as_of=observed_at, fully_closed_marks=True,
            )
            live_capital, baseline, _peak, context = bundle
            state = read_portfolio_state(
                cur, environment="LIVE", deployment_id=deployment_id,
                as_of=observed_at, runtime_revision=os.getenv("GIT_SHA", ""),
                live_managed_bundle=bundle,
            )
            baseline_id = None
            if baseline is not None:
                cur.execute(
                    """SELECT baseline_id FROM live_managed_capital_baseline_v1
                       WHERE environment='LIVE' AND deployment_id=%s
                         AND activation_fingerprint=%s""",
                    (deployment_id, baseline.activation_fingerprint),
                )
                baseline_row = cur.fetchone()
                baseline_id = None if not baseline_row else int(baseline_row[0])
            captured = capture_observation_candidate(
                state=state, live_capital=live_capital, context=context,
                baseline_id=baseline_id, baseline=baseline,
                observed_at=observed_at, observation_trigger=trigger[0],
                trigger_reference=trigger[1],
                producer_identity=f"automation-runner:{os.getenv('HOSTNAME', 'unknown')}",
                git_revision=os.getenv("GIT_SHA", ""),
            )
            connection.rollback()
            if captured.candidate is not None:
                _live_drawdown_pending[
                    captured.candidate.observation_identity
                ] = captured.candidate
            return {
                "status": captured.status,
                "persisted": persisted,
                "pending": len(_live_drawdown_pending),
                "reconciliations_reemitted": reemitted,
            }
    finally:
        connection.close()


def run_thesis_evidence_bundle_v1() -> dict:
    """Run the independent evidence-only producer at most once per 5m cutoff."""
    global _last_thesis_evidence_cutoff
    if not _env_bool("THESIS_EVIDENCE_BUNDLE_V1_ENABLED", "0"):
        return {"status": "DISABLED"}
    cutoff = canonical_evidence_cutoff(datetime.now(timezone.utc))
    if cutoff == _last_thesis_evidence_cutoff:
        return {"status": "ALREADY_ATTEMPTED_FOR_CUTOFF", "evidence_cutoff": cutoff}
    result = capture_thesis_evidence_bundle_cycle()
    _last_thesis_evidence_cutoff = cutoff
    logging.info(
        "thesis_evidence_bundle_v1: status=%s cutoff=%s evidence=%s "
        "symbols=%s bundles=%s structural=%s mme=%s transitions=%s tactical_sets=%s "
        "candidate_freezes=%s candidate_evaluations=%s",
        result.get("status"), result.get("evidence_cutoff"),
        result.get("evidence_status"), result.get("symbols"),
        result.get("bundles"), result.get("structural"),
        result.get("mme_observations"), result.get("mme_transitions"),
        result.get("tactical_sets"),
        result.get("candidate_freezes"), result.get("candidate_evaluations"),
    )
    return result


def run_causal_decision_observation_consumer() -> int:
    """Poll independently of the long-loop heartbeat; defaults are fully off."""
    flags = TransportFlags.from_env()
    consumer = DecisionObservationOutboxConsumer(
        get_db_conn,
        flags,
        consumer_id=f"automation-runner:{os.getenv('HOSTNAME', 'unknown')}",
        metrics=CAUSAL_TRANSPORT_METRICS,
    )
    return consumer.poll()


def _sql_literal(value: str) -> str:
    """Return a safely quoted SQL string literal for f-string SQL fragments."""
    return "'" + str(value).replace("'", "''") + "'"


def get_active_orc_apply_view() -> tuple[str, str, str, str]:
    """
    Returns:
      view_name, eligible_sql, on_reason, off_reason
    """
    forced_view = str(os.getenv("ORC_PICKS_VIEW", "") or "").strip()

    if forced_view == "v_orc_integration_v2_picks" or ORC_INTEGRATION_V2_APPLY_ENABLED:
        return (
            "v_orc_integration_v2_picks",
            "context_v2_ready_now = true",
            "ORC_INTEGRATION_V2: V7 readiness + MME context picked (entries ON, ENFORCE)",
            "ORC_INTEGRATION_V2: not ready, late/exhausted, or not picked (entries OFF, DRY_RUN)",
        )

    return (
        "v_orc_v7_shadow_picks",
        "eligible_v7_shadow = true",
        "ORC_INTEGRATION_V2: picked by V2 context scoring (entries ON, ENFORCE)",
        "ORC_INTEGRATION_V2: not picked by V2 context scoring (entries OFF, DRY_RUN)",
    )

ORC_INTEGRATION_V2_APPLY_ENABLED = os.getenv("ORC_INTEGRATION_V2_APPLY_ENABLED", "0") == "1"



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


def _with_orc_apply_failure_ledger(function):
    """Rollback partial work, then best-effort append a failed run header."""
    @wraps(function)
    def wrapped(conn, run_id=None):
        fixed_run_id = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()
        failed_started_at = utc_now()
        try:
            return function(conn, run_id=fixed_run_id)
        except Exception as exc:
            conn.rollback()
            error_classification = getattr(
                exc, "error_classification", type(exc).__name__
            )[:120]
            try:
                identity = WriterIdentity.from_env(cfg.trading_mode)
                failed_completed_at = utc_now()
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO orc_apply_runs_v1 (
                             run_id,deployment_id,environment,deployment_identity,
                             writer_service,writer_instance,writer_version,git_sha,
                             started_at,completed_at,apply_mode,integration_version,
                             source_view,source_candidate_count,
                             candidate_universe_count,slot_decision_count,
                             source_excluded_count,desired_on_count,
                             previous_live_on_count,resulting_live_on_count,
                             touched_on_count,touched_off_count,unchanged_on_count,
                             unchanged_off_count,picks_hash,transaction_outcome,
                             error_classification,duration_ms,schema_version,
                             execution_mode
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                     0,0,0,0,0,0,0,0,0,0,0,'','ROLLED_BACK',%s,%s,%s,%s)
                           ON CONFLICT (deployment_id,environment,run_id) DO NOTHING""",
                        (
                            str(fixed_run_id),identity.deployment_id,
                            identity.environment,identity.deployment_id,
                            identity.service,identity.instance,identity.version,
                            identity.git_sha,failed_started_at,failed_completed_at,
                            ORC_APPLY_MODE,ORC_APPLY_VERSION,
                            get_active_orc_apply_view()[0],error_classification,
                            max(0, int((failed_completed_at-failed_started_at).total_seconds()*1000)),
                            ORC_LEDGER_SCHEMA_VERSION,
                            (EXECUTION_MODE_OBSERVE_ONLY
                             if identity.deployment_id.endswith("-paper")
                             else EXECUTION_MODE_APPLY),
                        ),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                logging.exception(
                    "orc_apply: run_id=%s outcome=ROLLBACK_EVIDENCE_FAILED error_class=%s",
                    fixed_run_id, error_classification,
                )
            logging.exception(
                "orc_apply: run_id=%s outcome=ROLLED_BACK error_class=%s",
                fixed_run_id, error_classification,
            )
            raise
    return wrapped


def _learning_feedback_runner_stats(status: str, **extra):
    stats = {
        "status": status,
        "scheduler_version": LEARNING_FEEDBACK_SCHEDULER_VERSION,
        "source_refresh_engine_version": LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION,
        "engine_version": LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
        "engine_mode": LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
        "apply_enabled": LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
    }
    stats.update(extra)
    return stats


def _upsert_learning_feedback_runner_observability(
    cur,
    status: str,
    now_ts: int,
    stats,
):
    upsert_kv(cur, "learning_feedback_engine_runner_last_status", status)
    upsert_kv(cur, "learning_feedback_engine_runner_last_ts_s", str(now_ts))
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_last_stats_json",
        json.dumps(stats, default=_json_default, sort_keys=True),
    )
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_scheduler_version",
        LEARNING_FEEDBACK_SCHEDULER_VERSION,
    )
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_source_refresh_engine_version",
        LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION,
    )
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_engine_version",
        LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
    )
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_engine_mode",
        LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
    )
    upsert_kv(
        cur,
        "learning_feedback_engine_runner_apply_enabled",
        "1" if LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED else "0",
    )

    if stats.get("last_success_at") is not None:
        upsert_kv(
            cur,
            "learning_feedback_engine_runner_last_success_at",
            _json_default(stats["last_success_at"]),
        )
    if stats.get("next_due_at") is not None:
        upsert_kv(
            cur,
            "learning_feedback_engine_runner_next_due_at",
            _json_default(stats["next_due_at"]),
        )


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
    
    
def evaluate_orc_control_universe(
    controls, source_by_key, picks_by_key, on_reason, off_reason, execution_mode
):
    decisions = []
    for control in controls:
        key = (control["symbol"], control["interval"], control["strategy"])
        pick_source = picks_by_key.get(key)
        decisions.append(
            make_slot_decision(
                control,
                source_by_key.get(key),
                want_on=pick_source is not None,
                pick_source=pick_source,
                on_reason=on_reason,
                off_reason=off_reason,
                execution_mode=execution_mode,
            )
        )
    return decisions


def persist_orc_slot_ledger(cur, run_id, identity, slot_decisions):
    return sum(
        insert_slot_decision(cur, run_id, identity, decision)
        for decision in slot_decisions
    )


def apply_orc_control_transitions(cur, decisions, execution_mode):
    attempted_writes = 0
    if execution_mode == EXECUTION_MODE_OBSERVE_ONLY:
        if any(decision["touched"] for decision in decisions):
            raise AssertionError("OBSERVE_ONLY prepared a bot_control mutation")
        assert attempted_writes == 0, "OBSERVE_ONLY bot_control writes attempted"
        return attempted_writes
    for decision in decisions:
        if not decision["touched"]:
            continue
        attempted_writes += 1
        cur.execute(
            """UPDATE bot_control bc
                  SET live_orders_enabled=%s,
                      control_mode = 'AUTO',control_source = 'ORC',
                      manual_override_reason=NULL,
                      manual_override_updated_at=NULL,
                      regime_enabled=true,regime_mode=%s,updated_at=now(),
                      reason=%s,
                      live_since=CASE WHEN %s=true AND
                        COALESCE(bc.live_orders_enabled,false)=false
                        THEN now() ELSE bc.live_since END,
                      last_disabled_at=CASE WHEN %s=false AND
                        COALESCE(bc.live_orders_enabled,false)=true
                        THEN now() ELSE bc.last_disabled_at END
                WHERE bc.symbol=%s AND bc.interval=%s AND bc.strategy=%s
                  AND COALESCE(bc.control_mode,'AUTO')='AUTO'""",
            (
                decision["want_on"],
                "ENFORCE" if decision["want_on"] else "DRY_RUN",
                decision["reason"], decision["want_on"],
                decision["want_on"], decision["symbol"],
                decision["interval"], decision["strategy"],
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                "ORC ledger/control-plane cardinality mismatch for "
                + decision["slot_key"]
            )
    return attempted_writes


@_with_orc_apply_failure_ledger
def run_orc_cycle(conn, run_id=None):
    """
    Evaluate ORC once, persist immutable evidence, and optionally apply transitions.
    """

    now_ts = time.time()
    started_at = utc_now()
    run_id = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()
    identity = WriterIdentity.from_env(cfg.trading_mode)
    paper_observation_requested = (
        identity.deployment_id.endswith("-paper")
        and ORC_LEDGER_OBSERVE_ONLY_ENABLED
    )
    execution_mode = resolve_execution_mode(
        identity,
        cfg.trading_mode,
        observe_only_enabled=ORC_LEDGER_OBSERVE_ONLY_ENABLED,
        live_orders_enabled=(
            parse_required_execution_guard(
                "LIVE_ORDERS_ENABLED", os.getenv("LIVE_ORDERS_ENABLED")
            ) if paper_observation_requested else _env_bool("LIVE_ORDERS_ENABLED", "0")
        ),
        execution_enabled=(
            parse_required_execution_guard(
                "OKX_EXECUTION_ENABLED", os.getenv("OKX_EXECUTION_ENABLED")
            ) if paper_observation_requested else _env_bool("OKX_EXECUTION_ENABLED", "0")
        ),
    )
    if execution_mode is None:
        return
    if (execution_mode == EXECUTION_MODE_APPLY
            and os.getenv("ORC_V5_APPLY_ENABLED", "0") != "1"):
        return
    interval_s_env = int(os.getenv(
        "ORC_V5_APPLY_INTERVAL_SECONDS" if execution_mode == EXECUTION_MODE_APPLY
        else "ORC_LEDGER_OBSERVE_ONLY_INTERVAL_SECONDS",
        "60",
    ))

    with conn.cursor() as cur:
        if not try_acquire_control_plane_apply_lock(cur):
            logging.info(
                "orc_apply: skip concurrent authoritative apply "
                "(advisory_lock=%s)",
                CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID,
            )
            return

        # Optional KV overrides (if present)
        if execution_mode == EXECUTION_MODE_APPLY:
            kv_enabled = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_v5_apply_enabled';")
            if kv_enabled is not None and str(kv_enabled).strip() not in ("1", "true", "TRUE", "yes", "on"):
                return
        
        # HARD SAFETY: single writer lock
        if not _is_primary_writer_v5(cur):
            logging.warning("orc_apply: version=%s mode=%s skip (orc_writer_primary is not active ORC writer)", ORC_APPLY_VERSION, ORC_APPLY_MODE)
            return

        last_ts_key = (
            "orc_v5_apply_last_ts_s" if execution_mode == EXECUTION_MODE_APPLY
            else "orc_ledger_observe_only_last_ts_s"
        )
        kv_interval = q1(cur, "SELECT value FROM automation_kv WHERE key=%s;", (
            "orc_v5_apply_interval_s" if execution_mode == EXECUTION_MODE_APPLY
            else "orc_ledger_observe_only_interval_s",
        ))
        interval_s = interval_s_env
        if kv_interval is not None:
            try:
                interval_s = int(kv_interval)
            except Exception:
                interval_s = interval_s_env

        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key=%s;", (last_ts_key,))
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        active_picks_view = "v_orc_integration_v2_picks" if ORC_INTEGRATION_V2_APPLY_ENABLED else "v_orc_v7_shadow_picks"
        active_picks_eligible_sql = "context_v2_ready_now = true" if ORC_INTEGRATION_V2_APPLY_ENABLED else "eligible_v7_shadow = true"
        active_pick_reason = (
            "ORC_INTEGRATION_V2: V7 readiness + MME context picked (entries ON, ENFORCE)"
            if ORC_INTEGRATION_V2_APPLY_ENABLED
            else "ORC_INTEGRATION_V2: picked by V2 context scoring (entries ON, ENFORCE)"
        )
        active_off_reason = (
            "ORC_INTEGRATION_V2: not ready, late/exhausted, or not picked (entries OFF, DRY_RUN)"
            if ORC_INTEGRATION_V2_APPLY_ENABLED
            else "ORC_INTEGRATION_V2: not picked by V2 context scoring (entries OFF, DRY_RUN)"
        )

        cur.execute("SELECT to_regclass(%s);", (active_picks_view,))
        if cur.fetchone()[0] is None:
            logging.warning("orc_apply: requested picks view %s missing; fallback to v_orc_v7_shadow_picks", active_picks_view)
            active_picks_view = "v_orc_v7_shadow_picks"
            active_picks_eligible_sql = "eligible_v7_shadow = true"
            active_pick_reason = "ORC_INTEGRATION_V2: picked by V2 context scoring (entries ON, ENFORCE)"
            active_off_reason = "ORC_INTEGRATION_V2: not picked by V2 context scoring (entries OFF, DRY_RUN)"

        active_picks_view, active_picks_eligible_sql, active_pick_reason, active_off_reason = get_active_orc_apply_view()

        cur.execute("SELECT to_regclass(%s);", (active_picks_view,))
        if cur.fetchone()[0] is None:
            logging.warning("orc_apply: picks view %s missing; fallback to v_orc_v7_shadow_picks", active_picks_view)
            active_picks_view = "v_orc_v7_shadow_picks"
            active_picks_eligible_sql = "eligible_v7_shadow = true"
            active_pick_reason = "ORC_INTEGRATION_V2: picked by V2 context scoring (entries ON, ENFORCE)"
            active_off_reason = "ORC_INTEGRATION_V2: not picked by V2 context scoring (entries OFF, DRY_RUN)"

        cur.execute(
            """SELECT candidate_universe_count,desired_on_count,
                      previous_live_on_count,resulting_live_on_count,
                      touched_on_count,touched_off_count,unchanged_on_count,
                      unchanged_off_count,picks_hash
                 FROM orc_apply_runs_v1
                WHERE deployment_id=%s AND environment=%s AND run_id=%s""",
            (identity.deployment_id, identity.environment, str(run_id)),
        )
        existing_run = cur.fetchone()
        if existing_run is not None:
            logging.info("orc_apply: run_id=%s idempotent replay skipped", run_id)
            return

        cur.execute(f"SELECT * FROM {active_picks_view}")
        source_rows = rows_as_dicts(cur)
        candidate_source_n = len(source_rows)
        cur.execute(
            """SELECT key,updated_at
                 FROM automation_kv
                WHERE key IN ('orc_candidate_context_v1_last_ts_s',
                              'market_memory_orc_context_v17_last_ts_s')
                ORDER BY key"""
        )
        source_refresh_times = dict(cur.fetchall())
        latest_source_refresh = max(source_refresh_times.values(), default=None)
        for row in source_rows:
            row["source_refresh_timestamps"] = source_refresh_times
            row["refreshed_at"] = latest_source_refresh
        source_by_key = {
            (row["symbol"], row["interval"], row["strategy"]): row
            for row in source_rows
        }
        eligible_field = active_picks_eligible_sql.split("=", 1)[0].strip()
        picks_by_key = {
            key: "ORC_V6_3"
            for key, row in source_by_key.items()
            if row.get(eligible_field) is True
        }

        explore_enabled = str(
            q1(cur, "SELECT value FROM automation_kv WHERE key='orc_v62_explore_enabled';")
            or "0"
        ).strip().lower() in {"1", "true", "yes", "on"}
        explore_keys = set()
        if explore_enabled:
            cur.execute(
                """SELECT symbol,interval,strategy
                     FROM v_orc_exploration_picks_v1
                    WHERE eligible_exploration_v1=true"""
            )
            explore_keys = {tuple(row) for row in cur.fetchall()}
            for key in explore_keys:
                picks_by_key.setdefault(key, "ORC_EXPLORE_V1")

        cur.execute(
            """SELECT symbol,interval,strategy,enabled,live_orders_enabled,
                      regime_enabled,regime_mode,reason,control_mode,control_source,
                      manual_override_reason,manual_override_updated_at,
                      live_since,last_disabled_at,updated_at
                 FROM bot_control
                WHERE COALESCE(control_mode,'AUTO')='AUTO'
                  AND symbol IN ('BTCUSDC','ETHUSDC','SOLUSDC','BNBUSDC')
                  AND interval IN ('1m','5m')
                  AND strategy IN ('RSI','SUPERTREND','TREND','BBRANGE')
                ORDER BY symbol,interval,strategy"""
        )
        controls = rows_as_dicts(cur)
        controls = [
            row for row in controls
            if row["enabled"] is True
            or (row["symbol"], row["interval"], row["strategy"]) in picks_by_key
        ]
        decisions = evaluate_orc_control_universe(
            controls, source_by_key, picks_by_key, active_pick_reason,
            active_off_reason, execution_mode,
        )

        slot_decisions = list(decisions)
        source_excluded_count = validate_slot_counts(
            candidate_source_n, len(decisions), len(slot_decisions)
        )
        inserted_slot_count = persist_orc_slot_ledger(
            cur, run_id, identity, slot_decisions
        )
        validate_slot_counts(
            candidate_source_n,
            len(decisions),
            len(slot_decisions),
            inserted_slot_count,
        )

        bot_control_writes_attempted = apply_orc_control_transitions(
            cur, decisions, execution_mode
        )
        if execution_mode == EXECUTION_MODE_OBSERVE_ONLY:
            assert bot_control_writes_attempted == 0

        core_picks_n = sum(d["pick_source"] == "ORC_V6_3" for d in decisions)
        explore_picks_n = sum(d["pick_source"] == "ORC_EXPLORE_V1" for d in decisions)
        want_on_n = sum(d["want_on"] for d in decisions)
        universe_n = len(decisions)
        touched_on = sum(d["state_changed"] and d["want_on"] for d in decisions)
        touched_off = sum(d["state_changed"] and not d["want_on"] for d in decisions)
        touched = touched_on + touched_off
        ledger_picks_hash = deterministic_picks_hash(decisions)
        legacy_pick_items = sorted(
            f'{d["symbol"]}|{d["interval"]}|{d["strategy"]}|{d["pick_source"]}'
            for d in decisions if d["want_on"]
        )
        picks_hash = (
            hashlib.md5(",".join(legacy_pick_items).encode("utf-8")).hexdigest()
            if legacy_pick_items else ""
        )
        completed_at = utc_now()
        duration_ms = max(0, int((completed_at - started_at).total_seconds() * 1000))
        previous_live_on = sum(d["previous_live"] for d in decisions)
        resulting_live_on = sum(d["resulting_live"] for d in decisions)
        unchanged_on = sum(d["previous_live"] and d["want_on"] for d in decisions)
        unchanged_off = sum(not d["previous_live"] and not d["want_on"] for d in decisions)
        cur.execute(
            """INSERT INTO orc_apply_runs_v1 (
                 run_id,deployment_id,environment,deployment_identity,
                 writer_service,writer_instance,writer_version,git_sha,
                 started_at,completed_at,apply_mode,integration_version,
                 source_view,source_candidate_count,candidate_universe_count,
                 slot_decision_count,source_excluded_count,desired_on_count,
                 previous_live_on_count,resulting_live_on_count,touched_on_count,
                 touched_off_count,unchanged_on_count,unchanged_off_count,
                 picks_hash,transaction_outcome,error_classification,duration_ms,
                 schema_version,execution_mode
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                         %s,%s,%s,%s,%s,%s,%s,%s,%s,'COMMITTED',NULL,%s,%s,%s)""",
            (
                str(run_id),identity.deployment_id,identity.environment,
                identity.deployment_id,identity.service,identity.instance,
                identity.version,identity.git_sha,started_at,completed_at,
                ORC_APPLY_MODE,ORC_APPLY_VERSION,active_picks_view,
                candidate_source_n,universe_n,inserted_slot_count,
                source_excluded_count,want_on_n,previous_live_on,resulting_live_on,touched_on,touched_off,
                unchanged_on,unchanged_off,ledger_picks_hash,duration_ms,
                ORC_LEDGER_SCHEMA_VERSION,execution_mode,
            ),
        )

        stats = {
            "core_picks_n": int(core_picks_n or 0),
            "explore_picks_n": int(explore_picks_n or 0),
            "want_on_n": int(want_on_n or 0),
            "universe_n": int(universe_n or 0),
            "source_candidate_count": candidate_source_n,
            "candidate_universe_count": universe_n,
            "slot_decision_count": inserted_slot_count,
            "source_excluded_count": source_excluded_count,
            "execution_mode": execution_mode,
            "bot_control_writes_attempted": bot_control_writes_attempted,
            "touched": int(touched or 0),
            "touched_on": int(touched_on or 0),
            "touched_off": int(touched_off or 0),
            "picks_hash": str(picks_hash or ""),
            "ledger_picks_hash": ledger_picks_hash,
            "run_id": str(run_id),
            "applied_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z"),
            "orc_version": ORC_APPLY_VERSION,
            "orc_mode": ORC_APPLY_MODE,
            "picks_view": active_picks_view,
            "orc_integration_v2_apply_enabled": bool(ORC_INTEGRATION_V2_APPLY_ENABLED),
        }

        if execution_mode == EXECUTION_MODE_APPLY:
            upsert_kv(cur, "orc_v5_apply_mode", "automation_runner")
            upsert_kv(cur, "orc_active_version", ORC_APPLY_VERSION)
            upsert_kv(cur, "orc_active_mode", ORC_APPLY_MODE)
            upsert_kv(cur, "orc_v62_explore_enabled", "0")
            upsert_kv(cur, "orc_v63_explore_enabled", "0")
            upsert_kv(cur, "orc_v5_apply_last_ts_s", str(now_ts))
            upsert_kv(cur, "orc_v5_apply_last_stats_json", json.dumps(stats, sort_keys=True))
        else:
            upsert_kv(cur, "orc_ledger_observe_only_last_ts_s", str(now_ts))
            upsert_kv(cur, "orc_ledger_observe_only_last_stats_json", json.dumps(stats, sort_keys=True))

    conn.commit()
    logging.info(
        "orc_cycle: run_id=%s outcome=COMMITTED execution_mode=%s duration_ms=%s version=%s mode=%s stats=%s",
        run_id, execution_mode, duration_ms, ORC_APPLY_VERSION, ORC_APPLY_MODE, stats,
    )













def run_orc_candidate_context_refresh(conn):
    """
    ORC Candidate Context V1.
    Shadow-only: reads v_orc_candidate_context_v1 and writes automation_kv.
    Does not touch bot_control/orders.
    """
    if os.getenv("ORC_CANDIDATE_CONTEXT_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("ORC_CANDIDATE_CONTEXT_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='orc_candidate_context_v1_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = current_schema()
              AND table_name = 'v_orc_candidate_context_v1';
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            upsert_kv(cur, "orc_candidate_context_v1_last_ts_s", str(now_ts))
            upsert_kv(cur, "orc_candidate_context_v1_last_status", "missing_view")
            conn.commit()
            return

        cur.execute("""
            SELECT
              COUNT(*) AS candidates,
              COUNT(*) FILTER (WHERE context_ready_now) AS context_ready,
              COUNT(*) FILTER (WHERE mme_context_status='MME_PRIORITY_CONTEXT') AS mme_priority,
              COUNT(*) FILTER (WHERE mme_context_status='MME_WATCH_CONTEXT') AS mme_watch,
              COUNT(*) FILTER (WHERE mme_context_status='MME_AVOID_CONTEXT') AS mme_avoid,
              ROUND(MAX(orc_context_score), 4) AS max_context_score
            FROM v_orc_candidate_context_v1;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              strategy,
              context_ready_now,
              ROUND(orc_context_score, 4),
              picked_v63_now,
              orc_v7_ready,
              readiness_reason,
              v7_reason,
              mme_context_status,
              mme_orc_status,
              mme_orc_hint,
              ROUND(mme_orc_readiness_score, 4),
              mme_sequence_type,
              mme_sequence_stage,
              mme_long_context,
              mme_short_context,
              ROUND(mme_score_bonus, 4)
            FROM v_orc_candidate_context_v1
            ORDER BY
              context_ready_now DESC,
              orc_context_score DESC NULLS LAST,
              mme_orc_readiness_score DESC NULLS LAST
            LIMIT 20;
        """)
        rows = cur.fetchall()

        stats = {
            "candidates": int(r[0] or 0),
            "context_ready": int(r[1] or 0),
            "mme_priority": int(r[2] or 0),
            "mme_watch": int(r[3] or 0),
            "mme_avoid": int(r[4] or 0),
            "max_context_score": float(r[5] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "strategy": x[2],
                    "context_ready_now": bool(x[3]),
                    "orc_context_score": float(x[4] or 0),
                    "picked_v63_now": bool(x[5]),
                    "orc_v7_ready": bool(x[6]),
                    "readiness_reason": x[7],
                    "v7_reason": x[8],
                    "mme_context_status": x[9],
                    "mme_orc_status": x[10],
                    "mme_orc_hint": x[11],
                    "mme_orc_readiness_score": float(x[12] or 0),
                    "mme_sequence_type": x[13],
                    "mme_sequence_stage": x[14],
                    "mme_long_context": x[15],
                    "mme_short_context": x[16],
                    "mme_score_bonus": float(x[17] or 0),
                }
                for x in rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "orc_candidate_context_v1_last_ts_s", str(now_ts))
        upsert_kv(cur, "orc_candidate_context_v1_last_status", "ok")
        upsert_kv(cur, "orc_candidate_context_v1_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "orc_candidate_context_v1: candidates=%s ready=%s max_score=%s",
        stats["candidates"],
        stats["context_ready"],
        stats["max_context_score"],
    )


def run_missed_opportunity_replay_refresh(conn):
    """
    Missed Opportunity Replay V1.
    Shadow-only: refreshes missed_opportunity_replay from entry_trace_events/candles.
    Does not touch bot_control/orders/positions/ORC picks.
    """
    if os.getenv("MISSED_OPPORTUNITY_REPLAY_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MISSED_OPPORTUNITY_REPLAY_INTERVAL_SECONDS", "900"))
    lookback_hours = int(os.getenv("MISSED_OPPORTUNITY_REPLAY_LOOKBACK_HOURS", "6"))
    min_realtime = float(os.getenv("MISSED_OPPORTUNITY_REPLAY_MIN_REALTIME", "50.0"))
    min_move_pct = float(os.getenv("MISSED_OPPORTUNITY_REPLAY_MIN_MOVE_PCT", "0.35"))

    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='missed_opportunity_replay_v1_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.routines
            WHERE specific_schema = current_schema()
              AND routine_name = 'refresh_missed_opportunity_replay_v1';
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            upsert_kv(cur, "missed_opportunity_replay_v1_last_ts_s", str(now_ts))
            upsert_kv(cur, "missed_opportunity_replay_v1_last_status", "missing_function")
            conn.commit()
            return

        cur.execute(
            """
            SELECT *
            FROM refresh_missed_opportunity_replay_v1(
              (%s::text || ' hours')::interval,
              %s,
              %s
            );
            """,
            (str(lookback_hours), min_realtime, min_move_pct),
        )
        refresh_row = cur.fetchone()

        cur.execute("""
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (WHERE replay_status='OK') AS ok_n,
              COUNT(*) FILTER (WHERE replay_status='WAITING_FOR_60M_CANDLE') AS waiting_n,
              COUNT(*) FILTER (WHERE missed_opportunity) AS missed_n,
              ROUND(MAX(missed_move_pct), 4) AS max_missed_move_pct,
              ROUND(MAX(realtime_score), 4) AS max_realtime_score
            FROM missed_opportunity_replay
            WHERE event_time >= now() - (%s::text || ' hours')::interval;
        """, (str(lookback_hours),))
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              strategy,
              reason,
              event_time,
              ROUND(realtime_score, 4),
              replay_status,
              missed_opportunity,
              ROUND(missed_move_pct, 4),
              ROUND(adverse_move_pct, 4)
            FROM v_missed_opportunity_top
            ORDER BY missed_opportunity DESC, missed_move_pct DESC NULLS LAST
            LIMIT 20;
        """)
        rows = cur.fetchall()

        stats = {
            "processed": int(refresh_row[0] or 0) if refresh_row else 0,
            "inserted_or_updated": int(refresh_row[1] or 0) if refresh_row else 0,
            "lookback_hours": lookback_hours,
            "min_realtime": min_realtime,
            "min_move_pct": min_move_pct,
            "total": int(r[0] or 0),
            "ok_n": int(r[1] or 0),
            "waiting_n": int(r[2] or 0),
            "missed_n": int(r[3] or 0),
            "max_missed_move_pct": float(r[4] or 0),
            "max_realtime_score": float(r[5] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "strategy": x[2],
                    "reason": x[3],
                    "event_time": x[4].isoformat() if x[4] else None,
                    "realtime_score": float(x[5] or 0),
                    "replay_status": x[6],
                    "missed_opportunity": bool(x[7]),
                    "missed_move_pct": float(x[8] or 0),
                    "adverse_move_pct": float(x[9] or 0),
                }
                for x in rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "missed_opportunity_replay_v1_last_ts_s", str(now_ts))
        upsert_kv(cur, "missed_opportunity_replay_v1_last_status", "ok")
        upsert_kv(cur, "missed_opportunity_replay_v1_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "missed_opportunity_replay_v1: processed=%s updated=%s missed=%s max_move=%s",
        stats["processed"],
        stats["inserted_or_updated"],
        stats["missed_n"],
        stats["max_missed_move_pct"],
    )


def run_market_memory_orc_context_refresh(conn):
    """
    Refresh/log MME V1.7 ORC context view.
    Shadow-only: reads v_market_memory_orc_context_v17 and writes automation_kv.
    """
    if os.getenv("MARKET_MEMORY_ORC_CONTEXT_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_ORC_CONTEXT_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_orc_context_v17_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = current_schema()
              AND table_name = 'v_market_memory_orc_context_v17';
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_orc_context_v17: view missing; skip")
            upsert_kv(cur, "market_memory_orc_context_v17_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_orc_context_v17_last_status", "missing_view")
            conn.commit()
            return

        cur.execute("""
            SELECT
              COUNT(*) AS active_contexts,
              COUNT(*) FILTER (WHERE mme_orc_priority) AS priority,
              COUNT(*) FILTER (WHERE mme_orc_watch) AS watch,
              COUNT(*) FILTER (WHERE mme_orc_avoid) AS avoid,
              ROUND(MAX(orc_readiness_score), 4) AS max_orc_readiness,
              ROUND(MAX(sequence_quality), 4) AS max_sequence_quality
            FROM v_market_memory_orc_context_v17;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              mme_orc_status,
              orc_hint,
              ROUND(orc_readiness_score, 4),
              ROUND(sequence_quality, 4),
              ROUND(continuation_score, 4),
              ROUND(reversal_score, 4),
              ROUND(late_entry_risk, 4),
              ranking_status,
              global_rank,
              sequence_type,
              sequence_stage,
              long_context,
              short_context,
              direction,
              orc_context_reason,
              expires_at
            FROM v_market_memory_orc_context_v17
            ORDER BY
              mme_orc_priority DESC,
              orc_readiness_score DESC NULLS LAST,
              sequence_quality DESC NULLS LAST
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_contexts": int(r[0] or 0),
            "priority": int(r[1] or 0),
            "watch": int(r[2] or 0),
            "avoid": int(r[3] or 0),
            "max_orc_readiness": float(r[4] or 0),
            "max_sequence_quality": float(r[5] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "mme_orc_status": x[2],
                    "orc_hint": x[3],
                    "orc_readiness_score": float(x[4] or 0),
                    "sequence_quality": float(x[5] or 0),
                    "continuation_score": float(x[6] or 0),
                    "reversal_score": float(x[7] or 0),
                    "late_entry_risk": float(x[8] or 0),
                    "ranking_status": x[9],
                    "global_rank": int(x[10] or 0),
                    "sequence_type": x[11],
                    "sequence_stage": x[12],
                    "long_context": x[13],
                    "short_context": x[14],
                    "direction": x[15],
                    "reason": x[16],
                    "expires_at": x[17].isoformat() if x[17] else None,
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_orc_context_v17_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_orc_context_v17_last_status", "ok")
        upsert_kv(cur, "market_memory_orc_context_v17_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "market_memory_orc_context_v17: active=%s priority=%s max_orc_ready=%s",
        stats["active_contexts"],
        stats["priority"],
        stats["max_orc_readiness"],
    )


def run_market_memory_sequence_refresh(conn):
    """
    Refresh MME V1.6 sequence engine.
    Shadow-only: writes market_memory_sequence; does not touch bot_control/ORC/orders.
    """
    if os.getenv("MARKET_MEMORY_SEQUENCE_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_SEQUENCE_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_sequence_v16_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_sequence_v16'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_sequence_v16: refresh function missing; skip")
            upsert_kv(cur, "market_memory_sequence_v16_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_sequence_v16_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_sequence_v16();")

        cur.execute("""
            SELECT
              COUNT(*) AS active_sequences,
              COUNT(*) FILTER (WHERE orc_hint='ORC_PRIORITY_CANDIDATE') AS priority,
              COUNT(*) FILTER (WHERE orc_hint='ORC_WATCH_CANDIDATE') AS watch,
              COUNT(*) FILTER (WHERE orc_hint='ORC_AVOID_LATE_ENTRY') AS avoid_late,
              ROUND(MAX(orc_readiness_score), 4) AS max_orc_readiness,
              ROUND(MAX(sequence_quality), 4) AS max_sequence_quality
            FROM v_market_memory_sequence_current;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              sequence_type,
              sequence_stage,
              direction,
              ROUND(sequence_quality, 4),
              ROUND(continuation_score, 4),
              ROUND(reversal_score, 4),
              ROUND(late_entry_risk, 4),
              ROUND(orc_readiness_score, 4),
              orc_hint,
              reason,
              ranking_status,
              global_rank,
              chain_age_minutes,
              expires_at
            FROM v_market_memory_sequence_current
            ORDER BY orc_readiness_score DESC NULLS LAST,
                     sequence_quality DESC NULLS LAST
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_sequences": int(r[0] or 0),
            "priority": int(r[1] or 0),
            "watch": int(r[2] or 0),
            "avoid_late": int(r[3] or 0),
            "max_orc_readiness": float(r[4] or 0),
            "max_sequence_quality": float(r[5] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "sequence_type": x[2],
                    "sequence_stage": x[3],
                    "direction": x[4],
                    "sequence_quality": float(x[5] or 0),
                    "continuation_score": float(x[6] or 0),
                    "reversal_score": float(x[7] or 0),
                    "late_entry_risk": float(x[8] or 0),
                    "orc_readiness_score": float(x[9] or 0),
                    "orc_hint": x[10],
                    "reason": x[11],
                    "ranking_status": x[12],
                    "global_rank": int(x[13] or 0),
                    "chain_age_minutes": float(x[14] or 0),
                    "expires_at": x[15].isoformat() if x[15] else None,
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_sequence_v16_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_sequence_v16_last_status", "ok")
        upsert_kv(cur, "market_memory_sequence_v16_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "market_memory_sequence_v16: active=%s priority=%s max_orc_ready=%s",
        stats["active_sequences"],
        stats["priority"],
        stats["max_orc_readiness"],
    )


def run_market_memory_ranking_refresh(conn):
    """
    Refresh MME V1.5 opportunity ranking.
    Shadow-only: writes market_memory_ranking; does not touch bot_control/ORC/orders.
    """
    if os.getenv("MARKET_MEMORY_RANKING_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_RANKING_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_ranking_v15_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_ranking_v15'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_ranking_v15: refresh function missing; skip")
            upsert_kv(cur, "market_memory_ranking_v15_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_ranking_v15_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_ranking_v15();")

        cur.execute("""
            SELECT
              COUNT(*) AS active_ranked,
              COUNT(*) FILTER (WHERE ranking_status='PRIORITY') AS priority,
              COUNT(*) FILTER (WHERE ranking_status='WATCH') AS watch,
              COUNT(*) FILTER (WHERE ranking_status='LATE_OR_EXHAUSTED') AS late_or_exhausted,
              ROUND(MAX(rank_score), 4) AS max_rank_score,
              ROUND(MAX(remaining_score), 4) AS max_remaining_score
            FROM v_market_memory_ranking_current;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              global_rank,
              symbol,
              interval,
              ranking_status,
              ROUND(rank_score, 4),
              ROUND(remaining_score, 4),
              ROUND(timing_score, 4),
              ROUND(opportunity_score, 4),
              ROUND(confidence_score, 4),
              ROUND(urgency_score, 4),
              ROUND(exhaustion_risk, 4),
              stage,
              direction,
              opportunity_type,
              reason,
              expires_at
            FROM v_market_memory_ranking_current
            ORDER BY global_rank ASC NULLS LAST
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_ranked": int(r[0] or 0),
            "priority": int(r[1] or 0),
            "watch": int(r[2] or 0),
            "late_or_exhausted": int(r[3] or 0),
            "max_rank_score": float(r[4] or 0),
            "max_remaining_score": float(r[5] or 0),
            "top": [
                {
                    "global_rank": int(x[0] or 0),
                    "symbol": x[1],
                    "interval": x[2],
                    "ranking_status": x[3],
                    "rank_score": float(x[4] or 0),
                    "remaining_score": float(x[5] or 0),
                    "timing_score": float(x[6] or 0),
                    "opportunity_score": float(x[7] or 0),
                    "confidence_score": float(x[8] or 0),
                    "urgency_score": float(x[9] or 0),
                    "exhaustion_risk": float(x[10] or 0),
                    "stage": x[11],
                    "direction": x[12],
                    "opportunity_type": x[13],
                    "reason": x[14],
                    "expires_at": x[15].isoformat() if x[15] else None,
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_ranking_v15_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_ranking_v15_last_status", "ok")
        upsert_kv(cur, "market_memory_ranking_v15_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info(
        "market_memory_ranking_v15: active=%s priority=%s max_rank=%s",
        stats["active_ranked"],
        stats["priority"],
        stats["max_rank_score"],
    )


def run_market_memory_opportunity_refresh(conn):
    """
    Refresh Market Memory opportunity / stage engine.
    Analytics-only: writes market_memory_opportunity; does not change trading state.
    """
    if os.getenv("MARKET_MEMORY_OPPORTUNITY_ENABLED", "1") != "1":
        return

    interval_s = int(os.getenv("MARKET_MEMORY_OPPORTUNITY_INTERVAL_SECONDS", "60"))
    now_ts = time.time()

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='market_memory_opportunity_last_ts_s';")
        last_ts = float(last_ts_s) if last_ts_s else 0.0
        if now_ts - last_ts < float(interval_s):
            return

        cur.execute("""
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'refresh_market_memory_opportunity_v1'
              AND n.nspname = current_schema();
        """)
        if int(cur.fetchone()[0] or 0) == 0:
            logging.warning("market_memory_opportunity: refresh function missing; skip")
            upsert_kv(cur, "market_memory_opportunity_last_ts_s", str(now_ts))
            upsert_kv(cur, "market_memory_opportunity_last_status", "missing_function")
            conn.commit()
            return

        cur.execute("SELECT refresh_market_memory_opportunity_v1();")

        cur.execute("""
            SELECT
              COUNT(*) AS active_opportunities,
              COUNT(*) FILTER (WHERE action_hint='PRIORITY_WATCH') AS priority_watch,
              COUNT(*) FILTER (WHERE action_hint='WATCH') AS watch,
              COUNT(*) FILTER (WHERE action_hint='LATE_OR_RISKY') AS late_or_risky,
              ROUND(MAX(opportunity_score), 4) AS max_opportunity_score,
              ROUND(MAX(exhaustion_risk), 4) AS max_exhaustion_risk
            FROM v_market_memory_opportunity_active;
        """)
        r = cur.fetchone()

        cur.execute("""
            SELECT
              symbol,
              interval,
              opportunity_type,
              stage,
              direction,
              ROUND(opportunity_score, 4),
              ROUND(confidence_score, 4),
              ROUND(urgency_score, 4),
              ROUND(exhaustion_risk, 4),
              action_hint,
              timeline_type,
              chain_length,
              chain_age_minutes,
              long_context,
              short_context,
              expires_at,
              reason
            FROM v_market_memory_opportunity_active
            ORDER BY opportunity_score DESC NULLS LAST, urgency_score DESC NULLS LAST
            LIMIT 20;
        """)
        top_rows = cur.fetchall()

        stats = {
            "active_opportunities": int(r[0] or 0),
            "priority_watch": int(r[1] or 0),
            "watch": int(r[2] or 0),
            "late_or_risky": int(r[3] or 0),
            "max_opportunity_score": float(r[4] or 0),
            "max_exhaustion_risk": float(r[5] or 0),
            "top": [
                {
                    "symbol": x[0],
                    "interval": x[1],
                    "opportunity_type": x[2],
                    "stage": x[3],
                    "direction": x[4],
                    "opportunity_score": float(x[5] or 0),
                    "confidence_score": float(x[6] or 0),
                    "urgency_score": float(x[7] or 0),
                    "exhaustion_risk": float(x[8] or 0),
                    "action_hint": x[9],
                    "timeline_type": x[10],
                    "chain_length": int(x[11] or 0),
                    "chain_age_minutes": float(x[12] or 0),
                    "long_context": x[13],
                    "short_context": x[14],
                    "expires_at": x[15].isoformat() if x[15] else None,
                    "reason": x[16],
                }
                for x in top_rows
            ],
            "refreshed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

        upsert_kv(cur, "market_memory_opportunity_last_ts_s", str(now_ts))
        upsert_kv(cur, "market_memory_opportunity_last_status", "ok")
        upsert_kv(cur, "market_memory_opportunity_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))

    conn.commit()
    logging.info("market_memory_opportunity: active=%s priority=%s max_score=%s",
                 stats["active_opportunities"], stats["priority_watch"], stats["max_opportunity_score"])


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


def run_daily_equity_snapshot(conn):
    """Capture one forward-only equity observation at the existing UTC cadence."""
    if os.getenv("EQUITY_SNAPSHOT_ENABLED", "1") != "1":
        return
    deployment_id = os.getenv("DEPLOYMENT_ID", "").strip().lower()
    if deployment_id not in {
        "local-paper", "local-live", "vps-paper", "vps-live"
    }:
        raise RuntimeError("EQUITY_SNAPSHOT_DEPLOYMENT_ID_INVALID")

    baseline_created = False
    if (
        cfg.trading_mode == "PAPER"
        and _env_bool("PAPER_EQUITY_BASELINE_V2_ENABLED", "0")
    ):
        approved_by = os.getenv(
            "PAPER_EQUITY_BASELINE_V2_APPROVED_BY", ""
        ).strip()
        configured_provenance = os.getenv(
            "PAPER_EQUITY_BASELINE_V2_APPROVAL_PROVENANCE", ""
        ).strip()
        if not approved_by or not configured_provenance:
            raise RuntimeError("PAPER_EQUITY_BASELINE_V2_APPROVAL_REQUIRED")
        with conn.cursor() as cur:
            activation = ensure_paper_equity_baseline_v2(
                cur,
                trading_mode=cfg.trading_mode,
                deployment_id=deployment_id,
                exchange_client=client,
                quote_asset=os.getenv("QUOTE_ASSET", "USDC").upper(),
                paper_start_usdc=Decimal(
                    os.getenv("PAPER_START_USDT", "1000")
                ),
                approved_by=approved_by,
                approval_provenance={
                    "approval_type": "PRODUCT_OWNER_APPROVED",
                    "configured_provenance": configured_provenance,
                    "runtime_git_sha": os.getenv("GIT_SHA", "UNKNOWN"),
                },
            )
            baseline_created = activation.created

    target = os.getenv(
        "EQUITY_SNAPSHOT_HHMM_UTC",
        os.getenv("DAILY_REPORT_HHMM_UTC", "0020"),
    )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if now_utc_hhmm() < target and not baseline_created:
        return

    with conn.cursor() as cur:
        last = q1(
            cur,
            "SELECT value FROM automation_kv WHERE key='equity_snapshot_last_run';",
        )
        if last == today and not baseline_created:
            return
        observation = collect_current_equity(
            cur,
            trading_mode=cfg.trading_mode,
            exchange_client=client,
            quote_asset=os.getenv("QUOTE_ASSET", "USDC").upper(),
            deployment_id=deployment_id,
            paper_start_usdc=Decimal(
                os.getenv("PAPER_START_USDT", "1000")
            ),
        )
        snapshot_id = upsert_daily_snapshot(
            cur,
            deployment_id=deployment_id,
            trading_mode=cfg.trading_mode,
            observation=observation,
        )
        live_observation_id = None
        if cfg.trading_mode.upper() == "LIVE":
            observed_at = datetime.now(timezone.utc)
            live_evidence, live_baseline, _peak, _context = (
                load_live_managed_capital_evidence(
                    cur, exchange_client=client, deployment_id=deployment_id,
                    as_of=observed_at,
                )
            )
            if live_baseline is not None:
                cur.execute(
                    "SELECT baseline_id FROM live_managed_capital_baseline_v1 "
                    "WHERE deployment_id=%s AND activation_fingerprint=%s",
                    (deployment_id, live_baseline.activation_fingerprint),
                )
                live_observation_id = record_live_managed_equity_observation(
                    cur, baseline_id=int(cur.fetchone()[0]),
                    deployment_id=deployment_id, observed_at=observed_at,
                    evidence=live_evidence,
                )
        upsert_kv(cur, "equity_snapshot_last_run", today)
    conn.commit()
    logging.info(
        "equity_snapshot: captured id=%s deployment=%s evidence=%s",
        snapshot_id, deployment_id, observation.evidence_status,
    )
    if live_observation_id is not None:
        logging.info(
            "live_managed_equity_observation: captured id=%s deployment=%s",
            live_observation_id, deployment_id,
        )





def run_entry_context_snapshot_refresh(conn):
    enabled = os.getenv("ENTRY_CONTEXT_SNAPSHOT_REFRESH_ENABLED", "0") == "1"
    lookback_hours = int(os.getenv("ENTRY_CONTEXT_SNAPSHOT_LOOKBACK_HOURS", "6"))

    now_ts = int(time.time())

    with conn.cursor() as cur:
        if not enabled:
            cur.execute("""
                INSERT INTO automation_kv(key, value, updated_at)
                VALUES
                  ('entry_context_snapshot_refresh_last_status', 'disabled', now()),
                  ('entry_context_snapshot_refresh_disabled_reason', 'env_disabled', now()),
                  ('entry_context_snapshot_refresh_last_ts_s', %s, now())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
            """, (str(now_ts),))
            conn.commit()
            return

        interval_text = f"{lookback_hours} hours"

        cur.execute("SELECT refresh_entry_context_snapshot_v1(now() - %s::interval);", (interval_text,))
        refreshed = cur.fetchone()[0]

        stats = {
            "refreshed": refreshed,
            "lookback_hours": lookback_hours,
        }

        cur.execute("""
            INSERT INTO automation_kv(key, value, updated_at)
            VALUES
              ('entry_context_snapshot_refresh_last_status', 'ok', now()),
              ('entry_context_snapshot_refresh_last_stats_json', %s, now()),
              ('entry_context_snapshot_refresh_last_ts_s', %s, now())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """, (json.dumps(stats, sort_keys=True), str(now_ts)))

        conn.commit()

    logging.info(
        "entry_context_snapshot_refresh: lookback_hours=%s refreshed=%s",
        lookback_hours,
        refreshed,
    )


def run_learning_telemetry_refresh(conn):
    """
    Refreshes Exit Trace / Exit Learning telemetry.
    Analytics only: does not touch bot_control, orders, positions or ORC picks.
    """
    now_ts = int(time.time())

    interval_s = int(os.getenv("LEARNING_TELEMETRY_REFRESH_INTERVAL_SECONDS", "300"))
    timeout_ms = int(os.getenv("LEARNING_TELEMETRY_REFRESH_TIMEOUT_MS", "300000"))

    env_name = cfg.trading_mode.lower()

    default_enabled = "0" if env_name == "paper" else "1"
    enabled = (os.getenv("LEARNING_TELEMETRY_REFRESH_ENABLED", default_enabled) or default_enabled).strip().lower()

    if enabled not in ("1", "true", "yes", "on"):
        with conn.cursor() as cur:
            upsert_kv(cur, "learning_telemetry_refresh_last_status", "disabled")
            upsert_kv(cur, "learning_telemetry_refresh_last_ts_s", str(now_ts))
            upsert_kv(cur, "learning_telemetry_refresh_disabled_reason", f"env={env_name or 'unknown'}")
            conn.commit()
        return

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='learning_telemetry_refresh_last_ts_s';")
        try:
            last_ts = int(last_ts_s) if last_ts_s else 0
        except Exception:
            last_ts = 0

        if now_ts - last_ts < interval_s:
            return

        funcs = [
            "refresh_exit_trace_v1",
            "refresh_exit_trace_v2",
            "refresh_exit_trace_v3",
            "refresh_exit_learning_v1",
        ]

        cur.execute(
            """
            SELECT proname
            FROM pg_proc
            WHERE proname = ANY(%s)
            """,
            (funcs,),
        )
        existing = {r[0] for r in cur.fetchall()}
        missing = [f for f in funcs if f not in existing]

        if missing:
            logging.warning("learning_telemetry_refresh: missing functions=%s; skip", ",".join(missing))
            upsert_kv(cur, "learning_telemetry_refresh_last_ts_s", str(now_ts))
            upsert_kv(cur, "learning_telemetry_refresh_last_status", "missing_function")
            upsert_kv(cur, "learning_telemetry_refresh_missing_functions", ",".join(missing))
            conn.commit()
            return

        cur.execute("SET LOCAL statement_timeout = %s;", (timeout_ms,))

        stats = {}

        logging.info("learning_telemetry_refresh: running")

        lookback_hours = int(os.getenv("LEARNING_TELEMETRY_LOOKBACK_HOURS", "24"))
        lookback_interval = f"{lookback_hours} hours"

        cur.execute("SELECT refresh_exit_trace_v1(now() - %s::interval);", (lookback_interval,))
        stats["exit_trace_v1"] = cur.fetchone()[0]

        cur.execute("SELECT refresh_exit_trace_v2(now() - %s::interval);", (lookback_interval,))
        stats["exit_trace_v2"] = cur.fetchone()[0]

        cur.execute("SELECT refresh_exit_learning_v1(now() - %s::interval);", (lookback_interval,))
        stats["exit_learning_v1"] = cur.fetchone()[0]

        cur.execute("SELECT refresh_exit_trace_v3(now() - %s::interval);", (lookback_interval,))
        stats["exit_trace_v3"] = cur.fetchone()[0]

        stats["lookback_hours"] = lookback_hours

        upsert_kv(cur, "learning_telemetry_refresh_last_ts_s", str(now_ts))
        upsert_kv(cur, "learning_telemetry_refresh_last_status", "ok")
        upsert_kv(cur, "learning_telemetry_refresh_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))
        conn.commit()

    logging.info(
        "learning_telemetry_refresh: lookback_hours=%s exit_trace_v1=%s exit_trace_v2=%s exit_trace_v3=%s exit_learning_v1=%s",
        stats.get("lookback_hours"),
        stats.get("exit_trace_v1"),
        stats.get("exit_trace_v2"),
        stats.get("exit_trace_v3"),
        stats.get("exit_learning_v1"),
    )


def run_shadow_learning_pipeline_refresh(conn):
    """
    Refreshes shadow learning pipeline.
    Shadow-only: does not touch bot_control, orders, positions, ORC picks or execution.
    """
    now_ts = int(time.time())

    interval_s = int(os.getenv("SHADOW_LEARNING_REFRESH_INTERVAL_SECONDS", "300"))
    timeout_ms = int(os.getenv("SHADOW_LEARNING_REFRESH_TIMEOUT_MS", "300000"))
    enabled = (os.getenv("SHADOW_LEARNING_REFRESH_ENABLED", "1") or "1").strip().lower()

    if enabled not in ("1", "true", "yes", "on"):
        with conn.cursor() as cur:
            upsert_kv(cur, "shadow_learning_pipeline_last_status", "disabled")
            upsert_kv(cur, "shadow_learning_pipeline_last_ts_s", str(now_ts))
            conn.commit()
        return

    with conn.cursor() as cur:
        last_ts_s = q1(cur, "SELECT value FROM automation_kv WHERE key='shadow_learning_pipeline_last_ts_s';")
        try:
            last_ts = int(last_ts_s) if last_ts_s else 0
        except Exception:
            last_ts = 0

        if now_ts - last_ts < interval_s:
            return

        funcs = [
            "refresh_learning_feedback_shadow_recommendations_v1",
            "refresh_learning_feature_warehouse_v1",
            "refresh_decision_replay_v1",
            "reconcile_forward_causal_artifacts_v1_3",
        ]

        cur.execute(
            """
            SELECT proname
            FROM pg_proc
            WHERE proname = ANY(%s)
            """,
            (funcs,),
        )
        existing = {r[0] for r in cur.fetchall()}
        missing = [f for f in funcs if f not in existing]

        if missing:
            logging.warning("shadow_learning_pipeline_refresh: missing functions=%s; skip", ",".join(missing))
            upsert_kv(cur, "shadow_learning_pipeline_last_ts_s", str(now_ts))
            upsert_kv(cur, "shadow_learning_pipeline_last_status", "missing_function")
            upsert_kv(cur, "shadow_learning_pipeline_missing_functions", ",".join(missing))
            conn.commit()
            return

        cur.execute("SET LOCAL statement_timeout = %s;", (timeout_ms,))

        lookback_hours = int(os.getenv("SHADOW_LEARNING_LOOKBACK_HOURS", "24"))

        logging.info("shadow_learning_pipeline_refresh: running")

        stats = {}
        cur.execute("SELECT refresh_learning_feedback_shadow_recommendations_v1(%s);", (lookback_hours,))
        stats["shadow_recommendations"] = cur.fetchone()[0]

        cur.execute("SELECT refresh_learning_feature_warehouse_v1(%s);", (lookback_hours,))
        stats["feature_warehouse"] = cur.fetchone()[0]

        cur.execute("SELECT refresh_decision_replay_v1(%s);", (lookback_hours,))
        stats["decision_replay"] = cur.fetchone()[0]

        cur.execute(
            "SELECT reconcile_forward_causal_artifacts_v1_3("
            "now() - make_interval(hours => %s));",
            (lookback_hours,),
        )
        stats["causal_reconciliation"] = cur.fetchone()[0]

        stats["lookback_hours"] = lookback_hours

        upsert_kv(cur, "shadow_learning_pipeline_last_ts_s", str(now_ts))
        upsert_kv(cur, "shadow_learning_pipeline_last_status", "ok")
        upsert_kv(cur, "shadow_learning_pipeline_last_stats_json", json.dumps(stats, default=_json_default, sort_keys=True))
        upsert_kv(cur, "shadow_learning_pipeline_lookback_hours", str(lookback_hours))
        conn.commit()

    logging.info(
        "shadow_learning_pipeline_refresh: lookback_hours=%s shadow_recommendations=%s feature_warehouse=%s decision_replay=%s",
        stats.get("lookback_hours"),
        stats.get("shadow_recommendations"),
        stats.get("feature_warehouse"),
        stats.get("decision_replay"),
    )


def run_learning_shadow_confidence_v14(conn, source_refresh_run_id):
    """Run V1.4 after V1.2/V1.3 commit, isolated from the main refresh."""
    if not source_refresh_run_id:
        return

    try:
        with conn.cursor() as cur:
            # V1.4 owns a separate transaction, so transaction-local identity
            # must be established again after the V1.2/V1.3 commit.
            set_learning_evidence_transaction_context(cur)
            cur.execute(
                """
                SELECT to_regprocedure(
                    'refresh_learning_shadow_confidence_proposals_v1_4(bigint,text)'
                ) IS NOT NULL;
                """
            )
            if not bool(cur.fetchone()[0]):
                logging.info(
                    "learning_engine_v1_4: engine_version=%s "
                    "engine_mode=%s apply_enabled=%s function missing; skip",
                    LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
                    LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
                    LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
                )
                conn.rollback()
                return

            cur.execute(
                """
                SELECT refresh_learning_shadow_confidence_proposals_v1_4(
                    %s,
                    'AUTOMATION_RUNNER'
                );
                """,
                (source_refresh_run_id,),
            )
            row = cur.fetchone()
            result = row[0] if row else None

        conn.commit()
        logging.info(
            "learning_engine_v1_4: engine_version=%s engine_mode=%s "
            "apply_enabled=%s isolated refresh completed "
            "source_refresh_run_id=%s result=%r",
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
            source_refresh_run_id,
            result,
        )
    except Exception:
        logging.exception(
            "learning_engine_v1_4: engine_version=%s engine_mode=%s "
            "apply_enabled=%s isolated refresh failed source_refresh_run_id=%s",
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
            source_refresh_run_id,
        )
        try:
            conn.rollback()
        except Exception:
            pass


def run_learning_feedback_engine_refresh(conn):
    """
    Runs Learning Feedback Engine V1.2 when its 12-hour window is due.

    The runner performs a lightweight due check first. PostgreSQL still owns
    the final advisory lock and safety checks.

    Shadow-only:
    - no bot_control writes,
    - no ORC weight application,
    - no confidence application,
    - no promotion application,
    - no capital allocation,
    - no order placement.
    """
    enabled = (
        os.getenv("LEARNING_FEEDBACK_AUTOMATION_ENABLED", "1")
        or "1"
    ).strip().lower()

    now_ts = int(time.time())

    if enabled not in ("1", "true", "yes", "on"):
        with conn.cursor() as cur:
            runner_stats = _learning_feedback_runner_stats(
                "disabled",
                reason="disabled_by_environment",
            )
            _upsert_learning_feedback_runner_observability(
                cur,
                "disabled",
                now_ts,
                runner_stats,
            )
        conn.commit()

        logging.info(
            "learning_feedback_scheduler_v1_2: "
            "engine_version=%s engine_mode=%s apply_enabled=%s "
            "status=disabled reason=environment",
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
        )
        return

    check_interval_s = max(
        60,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_CHECK_INTERVAL_SECONDS",
                "300",
            )
        ),
    )

    interval_hours = max(
        1,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_INTERVAL_HOURS",
                "12",
            )
        ),
    )

    window_days = max(
        1,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_WINDOW_DAYS",
                "30",
            )
        ),
    )

    min_observe_sample = max(
        1,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_MIN_OBSERVE_SAMPLE",
                "10",
            )
        ),
    )

    min_action_sample = max(
        min_observe_sample,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_MIN_ACTION_SAMPLE",
                "30",
            )
        ),
    )

    timeout_ms = max(
        1000,
        int(
            os.getenv(
                "LEARNING_FEEDBACK_TIMEOUT_MS",
                "300000",
            )
        ),
    )

    function_signature = (
        "refresh_learning_feedback_engine_v1_2_if_due("
        "integer,integer,integer,integer,boolean,text)"
    )

    with conn.cursor() as cur:
        # Applies only to this feedback transaction. V1.3 and evidence capture
        # run from triggers before this transaction commits.
        set_learning_evidence_transaction_context(cur)
        last_check_s = q1(
            cur,
            """
            SELECT value
            FROM automation_kv
            WHERE key =
                'learning_feedback_engine_runner_last_ts_s';
            """,
        )

        try:
            last_check = int(last_check_s) if last_check_s else 0
        except Exception:
            last_check = 0

        if now_ts - last_check < check_interval_s:
            return

        cur.execute(
            """
            SELECT to_regprocedure(%s) IS NOT NULL;
            """,
            (function_signature,),
        )

        function_exists = bool(cur.fetchone()[0])

        if not function_exists:
            runner_stats = _learning_feedback_runner_stats(
                "missing_function",
                missing_function=function_signature,
            )
            _upsert_learning_feedback_runner_observability(
                cur,
                "missing_function",
                now_ts,
                runner_stats,
            )
            upsert_kv(
                cur,
                "learning_feedback_engine_runner_missing_function",
                function_signature,
            )
            conn.commit()

            logging.warning(
                "learning_feedback_scheduler_v1_2: "
                "engine_version=%s engine_mode=%s apply_enabled=%s "
                "status=missing_function signature=%s",
                LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
                LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
                LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
                function_signature,
            )
            return

        # Lightweight pre-check. This prevents a SKIPPED_NOT_DUE history row
        # from being written on every automation-runner tick.
        cur.execute(
            """
            SELECT
                MAX(finished_at) AS last_success_at,
                CASE
                    WHEN MAX(finished_at) IS NULL THEN true
                    ELSE now() >= (
                        MAX(finished_at)
                        + make_interval(hours => %s)
                    )
                END AS is_due,
                CASE
                    WHEN MAX(finished_at) IS NULL THEN now()
                    ELSE (
                        MAX(finished_at)
                        + make_interval(hours => %s)
                    )
                END AS next_due_at
            FROM learning_feedback_refresh_runs_v1
            WHERE environment = current_database()
              AND status = 'OK';
            """,
            (
                interval_hours,
                interval_hours,
            ),
        )

        due_row = cur.fetchone()
        last_success_at = due_row[0] if due_row else None
        is_due = bool(due_row[1]) if due_row else True
        next_due_at = due_row[2] if due_row else None

        if not is_due:
            runner_stats = _learning_feedback_runner_stats(
                "not_due",
                interval_hours=interval_hours,
                last_success_at=last_success_at,
                next_due_at=next_due_at,
            )
            _upsert_learning_feedback_runner_observability(
                cur,
                "not_due",
                now_ts,
                runner_stats,
            )
            conn.commit()

            logging.info(
                "learning_feedback_scheduler_v1_2: "
                "engine_version=%s engine_mode=%s apply_enabled=%s "
                "status=not_due last_success_at=%s next_due_at=%s",
                LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
                LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
                LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
                last_success_at,
                next_due_at,
            )
            return

        cur.execute(
            "SET LOCAL statement_timeout = %s;",
            (timeout_ms,),
        )

        logging.info(
            "learning_feedback_scheduler_v1_2: "
            "source_refresh_engine_version=%s engine_version=%s "
            "engine_mode=%s apply_enabled=%s status=due running "
            "interval_hours=%s window_days=%s "
            "min_observe_sample=%s min_action_sample=%s",
            LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
            interval_hours,
            window_days,
            min_observe_sample,
            min_action_sample,
        )

        # Identity/outcome publication is the canonical producer boundary for
        # Learning evidence. Run it before the feedback header establishes its
        # point-in-time cutoff; warehouse PnL alone must never confer identity.
        runtime_deployment_id = os.getenv("DEPLOYMENT_ID", "").strip()
        deployment_scope = {
            "local-live": "LOCAL",
            "local-paper": "LOCAL",
            "vps-live": "VPS",
            "vps-paper": "VPS",
        }.get(runtime_deployment_id)
        if deployment_scope is None:
            raise RuntimeError(
                "LEARNING_CANONICAL_IDENTITY_INVALID_DEPLOYMENT_ID"
            )
        cur.execute(
            """
            SELECT to_regprocedure(
                'refresh_decision_identity_outcome_v1'
                '(integer,text,text,uuid)'
            ) IS NOT NULL;
            """
        )
        if not bool(cur.fetchone()[0]):
            raise RuntimeError(
                "LEARNING_CANONICAL_IDENTITY_PRODUCER_MISSING"
            )
        cur.execute(
            """
            SELECT refresh_decision_identity_outcome_v1(
                %s,
                current_database(),
                %s
            );
            """,
            (
                max(24, window_days * 24),
                deployment_scope,
            ),
        )
        identity_result = cur.fetchone()
        identity_result = identity_result[0] if identity_result else None
        if not isinstance(identity_result, dict):
            raise RuntimeError(
                "LEARNING_CANONICAL_IDENTITY_PRODUCER_INVALID_RESULT"
            )
        if identity_result.get("status") != "OK":
            raise RuntimeError(
                "LEARNING_CANONICAL_IDENTITY_PRODUCER_FAILED: "
                f"{identity_result}"
            )

        cur.execute(
            """
            SELECT refresh_learning_feedback_engine_v1_2_if_due(
                %s,
                %s,
                %s,
                %s,
                false,
                'AUTOMATION_RUNNER'
            );
            """,
            (
                interval_hours,
                window_days,
                min_observe_sample,
                min_action_sample,
            ),
        )

        row = cur.fetchone()
        result = row[0] if row else None

        if isinstance(result, str):
            try:
                result = json.loads(result)
            except (TypeError, ValueError):
                pass

        status = (
            result.get("status")
            if isinstance(result, dict)
            else "unknown"
        )

        cur.execute(
            """
            SELECT
                MAX(finished_at) AS last_success_at,
                CASE
                    WHEN MAX(finished_at) IS NULL THEN NULL
                    ELSE (
                        MAX(finished_at)
                        + make_interval(hours => %s)
                    )
                END AS next_due_at
            FROM learning_feedback_refresh_runs_v1
            WHERE environment = current_database()
              AND status = 'OK';
            """,
            (interval_hours,),
        )
        due_row = cur.fetchone()
        last_success_at = due_row[0] if due_row else None
        next_due_at = due_row[1] if due_row else None

        runner_stats = _learning_feedback_runner_stats(
            str(status),
            interval_hours=interval_hours,
            window_days=window_days,
            min_observe_sample=min_observe_sample,
            min_action_sample=min_action_sample,
            last_success_at=last_success_at,
            next_due_at=next_due_at,
            result=result,
        )

        _upsert_learning_feedback_runner_observability(
            cur,
            str(status),
            now_ts,
            runner_stats,
        )

    conn.commit()

    # V1.2 and its V1.3 trigger are durable before V1.4 starts. V1.4 owns a
    # separate transaction and catches both SQL-level and connection errors.
    if isinstance(result, dict) and result.get("status") == "ok":
        run_learning_shadow_confidence_v14(
            conn,
            result.get("run_id"),
        )

    if isinstance(result, dict):
        logging.info(
            "learning_feedback_scheduler_v1_2: "
            "source_refresh_engine_version=%s engine_version=%s "
            "engine_mode=%s apply_enabled=%s completed "
            "status=%s run_id=%s refreshed_at=%s",
            LEARNING_FEEDBACK_SOURCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
            result.get("status"),
            result.get("run_id"),
            result.get("refreshed_at"),
        )
    else:
        logging.info(
            "learning_feedback_scheduler_v1_2: "
            "engine_version=%s engine_mode=%s apply_enabled=%s result=%r",
            LEARNING_SHADOW_CONFIDENCE_ENGINE_VERSION,
            LEARNING_SHADOW_CONFIDENCE_ENGINE_MODE,
            LEARNING_SHADOW_CONFIDENCE_APPLY_ENABLED,
            result,
        )

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


def run_independent_refresh_job(conn, job, failure_message: str):
    """Run one refresh without leaking its failed transaction to the next."""
    try:
        job(conn)
    except Exception:
        logging.exception(failure_message)
        try:
            conn.rollback()
        except Exception:
            logging.exception("%s; rollback failed", failure_message)
            raise

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



# --- PROMOTIONS: environment-scoped publisher (v1) ---
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

def _resolve_promotions_api_base(environ=None):
    source = os.environ if environ is None else environ
    deployment_id = (source.get("DEPLOYMENT_ID", "") or "").strip()
    api_base = (source.get("INTERNAL_API_BASE", "") or "").strip()
    expected_host = {
        "local-live": "live-api",
        "vps-live": "live-api",
        "local-paper": "paper-api",
        "vps-paper": "paper-api",
    }.get(deployment_id)
    parsed = urlparse(api_base)
    if (
        expected_host is None
        or parsed.scheme not in {"http", "https"}
        or parsed.hostname != expected_host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        logging.error(
            "promotions: invalid environment-scoped INTERNAL_API_BASE "
            "deployment_id=%s host=%s",
            deployment_id or "<missing>",
            parsed.hostname or "<missing>",
        )
        return None
    return api_base.rstrip("/")

def publish_promotions(conn):
    """
    PAPER -> LIVE publisher
    Primary: v_ranking_v1 status='CANDIDATE'
    Fallback: v_bot_scoreboard_sim_10d TOP by net_sum (min trades)
    """
    if os.getenv("PROMOTIONS_ENABLED", "0") != "1":
        return False

    promotions_api_base = _resolve_promotions_api_base()
    if promotions_api_base is None:
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
                    regime_promotions_token = os.environ.get("INTERNAL_API_TOKEN", "")
                    regime_promotions_headers = {}
                    if regime_promotions_token:
                        regime_promotions_headers["X-Internal-Token"] = regime_promotions_token

                    resp = requests.post(
                        f"{promotions_api_base}/internal/regime-promotions/upsert",
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

        url = promotions_api_base + "/internal/promotions/upsert"
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

            if cfg.trading_mode == "LIVE":
                try:
                    owner_flow_sync = run_owner_capital_flow_sync_if_due(
                        conn,
                        exchange_client=client,
                        trading_mode=cfg.trading_mode,
                        deployment_id=os.getenv("DEPLOYMENT_ID", "").strip().lower(),
                    )
                    logging.info(
                        "owner_capital_flow_sync_v1 status=%s sync_through=%s",
                        (
                            owner_flow_sync.get("status")
                            if isinstance(owner_flow_sync, dict)
                            else owner_flow_sync.status
                        ),
                        (
                            None if isinstance(owner_flow_sync, dict)
                            else owner_flow_sync.sync_through
                        ),
                    )
                except Exception:
                    logging.exception("owner_capital_flow_sync_v1 failed")
                    conn.rollback()

                try:
                    drawdown_history = run_live_drawdown_history_cycle()
                    logging.info(
                        "live_drawdown_history_v1 status=%s pending=%s persisted=%s",
                        drawdown_history.get("status"),
                        drawdown_history.get("pending"),
                        drawdown_history.get("persisted"),
                    )
                except Exception:
                    logging.exception("live_drawdown_history_v1 failed")

            try:
                causal_processed = run_causal_decision_observation_consumer()
                logging.info("causal_observation_consumer processed=%s", causal_processed)
            except Exception:
                logging.exception("causal_observation_consumer failed")

            run_daily_report(conn)

            try:
                run_daily_equity_snapshot(conn)
            except Exception:
                logging.exception("equity_snapshot failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            run_independent_refresh_job(
                conn,
                run_strategy_regime_stats_refresh,
                "strategy_regime_stats_refresh failed",
            )
            run_independent_refresh_job(
                conn,
                run_market_regime_confidence_refresh,
                "market_regime_confidence_refresh failed",
            )

            # Freeze only evidence that existed at the closed 5m cutoff.  This
            # intentionally runs before the next mutable MME refresh cycle.
            try:
                run_thesis_evidence_bundle_v1()
            except Exception:
                logging.exception("thesis_evidence_bundle_v1 failed")

            if cfg.trading_mode == "PAPER":
                try:
                    paper_drawdown = run_paper_drawdown_history_cycle()
                    logging.info(
                        "paper_drawdown_history_v1 status=%s persisted=%s",
                        paper_drawdown.get("status"),
                        paper_drawdown.get("persisted"),
                    )
                except Exception:
                    logging.exception("paper_drawdown_history_v1 failed")

            try:
                risk_budget = run_risk_budget_state_evaluation_cycle(
                    exchange_client=(
                        client if cfg.trading_mode.upper() == "LIVE" else None
                    )
                )
                logging.info(
                    "risk_budget_state_evaluation_v1 status=%s boundary=%s "
                    "authority_status=%s execution_effect=NONE",
                    risk_budget.status, risk_budget.boundary,
                    risk_budget.authority_status,
                )
            except Exception:
                logging.exception("risk_budget_state_evaluation_v1 failed")

            run_independent_refresh_job(
                conn,
                run_market_memory_events_refresh,
                "market_memory_events_refresh failed",
            )

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
                run_market_memory_opportunity_refresh(conn)
            except Exception:
                logging.exception("market_memory_opportunity_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_ranking_refresh(conn)
            except Exception:
                logging.exception("market_memory_ranking_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_sequence_refresh(conn)
            except Exception:
                logging.exception("market_memory_sequence_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_market_memory_orc_context_refresh(conn)
            except Exception:
                logging.exception("market_memory_orc_context_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            try:
                run_orc_candidate_context_refresh(conn)
            except Exception:
                logging.exception("orc_candidate_context_refresh failed")

            try:
                run_missed_opportunity_replay_refresh(conn)
            except Exception:
                logging.exception("missed_opportunity_replay_refresh failed")
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

            try:
                run_entry_context_snapshot_refresh(conn)
                run_learning_telemetry_refresh(conn)
                run_shadow_learning_pipeline_refresh(conn)
            except Exception:
                logging.exception("entry_context_snapshot_refresh / learning_telemetry_refresh / shadow_learning_pipeline_refresh failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

            # PAPER observation only. The job has a second strict runtime-
            # deployment fence before it can call the canonical SQL producer.
            if cfg.trading_mode == "PAPER":
                run_independent_refresh_job(
                    conn,
                    run_bounded_horizon_label_automation,
                    "bounded_horizon_label_automation failed",
                )

            try:
                run_learning_feedback_engine_refresh(conn)
            except Exception:
                logging.exception(
                    "learning_feedback_scheduler_v1_2 refresh failed"
                )
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

            if (cfg.trading_mode == "LIVE" and mode != "DISABLE_ONLY") or (
                cfg.trading_mode == "PAPER" and ORC_LEDGER_OBSERVE_ONLY_ENABLED
            ):
                run_orc_cycle(conn)

            logging.info("tick ok")

            try:
                pending_entry_run = run_pending_entry_reconciliation_if_due(
                    conn,
                    batch_size=100,
                    trading_mode=cfg.trading_mode,
                )
                logging.info(
                    "pending_entry_reconciliation status=%s ran=%s "
                    "scanned=%s has_more=%s",
                    pending_entry_run.status,
                    pending_entry_run.ran,
                    pending_entry_run.stats.scanned,
                    pending_entry_run.stats.has_more,
                )
            except Exception:
                logging.exception("pending_entry_reconciliation failed")
                try:
                    conn.rollback()
                except Exception:
                    pass

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

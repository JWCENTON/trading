"""LOCAL PAPER-only direct Long Horizon L3 admission and exit authority.

The module deliberately reuses the canonical regime gate, atomic PAPER entry,
Fee V2 realizable-net evidence, and the finalized 1m owner cadence.  It has no
LIVE or VPS authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os

from common.db import get_db_conn
from common.exit_guards.economic_floor_v2 import (
    load_latest_finalized_canonical_one_minute_mark,
)
from common.simulated_execution_evidence import load_paper_realizable_net_evidence


CONTRACT_VERSION = "LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1"
L0_COMPARATOR_VERSION = "LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1"
TARGET_EXIT_REASON = "LONG_HORIZON_L3_REALIZABLE_NET_TARGET_V1"
TARGET_NET_RATE = Decimal("0.03")
PRIMARY_SLEEVE_RATE = Decimal("0.40")
SECONDARY_SLEEVE_RATE = Decimal("0.20")
GLOBAL_HEAT_RATE = Decimal("0.60")
MIN_FREE_CASH_RATE = Decimal("0.20")
EXPECTED_NOTIONAL = Decimal("20")
SAMPLING_VERSION = "L3_SHA256_GATE_EVENT_ID_MOD10_EQ0_V1"
SAME_THESIS_VERSION = "P4_15M_SYMBOL_SIDE_REGIME_V1"
ALLOWED_MODES = {"TREATMENT"}


@dataclass(frozen=True)
class AdmissionResult:
    accepted: bool
    status: str
    admission_id: int | None = None
    cohort: str | None = None


@dataclass(frozen=True)
class TargetDecision:
    status: str
    position_id: int | None = None
    exit_requested: bool = False
    mark_price: Decimal | None = None
    observed_at: datetime | None = None
    realizable_net: Decimal | None = None


def active(environ=None) -> bool:
    values = os.environ if environ is None else environ
    return (
        str(values.get("TRADING_MODE", "")).upper() == "PAPER"
        and str(values.get("DEPLOYMENT_ID", values.get("WALTRADE_DEPLOYMENT_ID", ""))).lower()
        == "local-paper"
        and str(values.get("LONG_HORIZON_L3_MODE", "OFF")).upper() in ALLOWED_MODES
    )


def canonical_sample_identity(*, gate_event_id: int, symbol: str, interval: str,
                              strategy: str, side: str, candle_open_time) -> str:
    ts = candle_open_time.astimezone(timezone.utc).isoformat()
    return "|".join((SAMPLING_VERSION, "local-paper", str(int(gate_event_id)),
                     symbol.upper(), interval.lower(), strategy.upper(), side.upper(), ts))


def sample_bucket(identity: str) -> int:
    return int(hashlib.sha256(identity.encode("utf-8")).hexdigest(), 16) % 10


def treatment_fingerprint(payload: dict) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def target_reached(realizable_net: Decimal, entry_capital: Decimal) -> bool:
    capital = Decimal(str(entry_capital))
    if capital <= 0:
        return False
    return Decimal(str(realizable_net)) / capital >= TARGET_NET_RATE


def admission_context_from_provenance(provenance: dict | None) -> dict | None:
    if not provenance or provenance.get("regime_gate_event_id") is None:
        return None
    return {
        "gate_event_id": int(provenance["regime_gate_event_id"]),
        "why": provenance.get("regime_gate_why"),
        "would_block": provenance.get("regime_gate_would_block"),
        "mode": provenance.get("regime_gate_mode"),
    }


def _same_thesis(symbol: str, side: str, regime: str, candle_open_time) -> str:
    epoch = int(candle_open_time.timestamp()) // 900
    return f"{SAME_THESIS_VERSION}|{symbol.upper()}|{side.upper()}|{regime}|{epoch}"


def prepare_admission_cursor(cur, *, symbol: str, interval: str, strategy: str,
                             side: str, candle_open_time, requested_notional: Decimal,
                             provenance: dict | None) -> AdmissionResult:
    """Qualify an L3 entry inside the canonical atomic-entry transaction."""
    if not active():
        return AdmissionResult(True, "L3_INACTIVE")
    ctx = admission_context_from_provenance(provenance)
    if ctx is None:
        return AdmissionResult(False, "L3_EXACT_GATE_LINK_REQUIRED")
    cur.execute(
        """SELECT id,regime,mode,would_block,why,meta FROM regime_gate_events
             WHERE id=%s FOR SHARE""", (ctx["gate_event_id"],),
    )
    gate = cur.fetchone()
    if gate is None:
        return AdmissionResult(False, "L3_GATE_EVENT_NOT_FOUND")
    gate_id, regime, mode, would_block, why, meta = gate
    if str(mode).upper() != "DRY_RUN" or str(why) not in {"POLICY_ALLOW", "POLICY_WOULD_BLOCK"}:
        return AdmissionResult(False, "L3_GATE_NOT_QUALIFIED")
    identity = canonical_sample_identity(
        gate_event_id=gate_id, symbol=symbol, interval=interval, strategy=strategy,
        side=side, candle_open_time=candle_open_time,
    )
    bucket = sample_bucket(identity)
    if str(why) == "POLICY_ALLOW" and not bool(would_block):
        cohort, sleeve_rate = "L3_REGIME_WOULD_ALLOW", PRIMARY_SLEEVE_RATE
    elif str(why) == "POLICY_WOULD_BLOCK" and bool(would_block) and bucket == 0:
        cohort, sleeve_rate = "L3_REGIME_WOULD_BLOCK_SAMPLE", SECONDARY_SLEEVE_RATE
    else:
        return AdmissionResult(False, "L3_REGIME_WOULD_BLOCK_OBSERVATION_ONLY")
    same_thesis = _same_thesis(symbol, side, str(regime), candle_open_time)
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("L3|" + same_thesis,))
    cur.fetchone()
    cur.execute(
        """SELECT admission_id,status,cohort FROM long_horizon_l3_admission_v1
             WHERE gate_event_id=%s""", (gate_id,),
    )
    existing = cur.fetchone()
    if existing:
        return AdmissionResult(str(existing[1]) == "ACCEPTED", "L3_IDEMPOTENT", int(existing[0]), str(existing[2]))
    cur.execute(
        """SELECT 1 FROM long_horizon_l3_admission_v1 a JOIN positions p ON p.id=a.position_id
             WHERE a.same_thesis_identity=%s AND a.status='ACCEPTED' AND p.status='OPEN' LIMIT 1""",
        (same_thesis,),
    )
    if cur.fetchone():
        return AdmissionResult(False, "L3_SAME_THESIS_ACTIVE")
    cur.execute(
        """SELECT managed_equity FROM paper_managed_equity_observation_v1
             WHERE deployment_id='local-paper' AND managed_equity_status='CANONICAL'
             ORDER BY observed_at DESC LIMIT 1"""
    )
    equity_row = cur.fetchone()
    if not equity_row or equity_row[0] is None:
        return AdmissionResult(False, "L3_CANONICAL_EQUITY_REQUIRED")
    equity = Decimal(str(equity_row[0]))
    cur.execute(
        """SELECT COALESCE(sum(a.entry_notional),0) FROM long_horizon_l3_admission_v1 a
             JOIN positions p ON p.id=a.position_id WHERE a.status='ACCEPTED' AND p.status='OPEN' AND a.cohort=%s""",
        (cohort,),
    )
    sleeve_used = Decimal(str(cur.fetchone()[0]))
    cur.execute(
        """SELECT COALESCE(sum(f.fill_notional),0) FROM positions p
             JOIN simulated_execution_fills_v1 f ON f.position_id=p.id AND f.order_purpose='ENTRY'
             WHERE p.status='OPEN' AND f.environment='PAPER' AND f.deployment_id='local-paper'"""
    )
    global_used = Decimal(str(cur.fetchone()[0]))
    requested = Decimal(str(requested_notional))
    if requested <= 0 or abs(requested - EXPECTED_NOTIONAL) > Decimal("0.75"):
        return AdmissionResult(False, "L3_NOTIONAL_CONTRACT_VIOLATION")
    if sleeve_used + requested > equity * sleeve_rate:
        return AdmissionResult(False, "L3_SLEEVE_SATURATED")
    if global_used + requested > equity * GLOBAL_HEAT_RATE or equity - global_used - requested < equity * MIN_FREE_CASH_RATE:
        return AdmissionResult(False, "L3_GLOBAL_CAPITAL_LIMIT")
    cur.execute(
        """INSERT INTO long_horizon_l3_admission_v1(
             gate_event_id,cohort,sampling_identity,sampling_bucket,same_thesis_identity,
             symbol,interval,strategy,side,entry_candle_open_time,entry_notional,status)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACCEPTED') RETURNING admission_id""",
        (gate_id, cohort, identity, bucket, same_thesis, symbol.upper(), interval.lower(),
         strategy.upper(), side.upper(), candle_open_time, requested),
    )
    return AdmissionResult(True, "L3_ACCEPTED", int(cur.fetchone()[0]), cohort)


def finalize_admission_cursor(cur, *, admission_id: int, simulated_order_id: int,
                              position_id: int) -> None:
    cur.execute(
        """SELECT decision_id,entry_opportunity_snapshot_id FROM simulated_orders WHERE id=%s""",
        (simulated_order_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        raise RuntimeError("L3_EXACT_DECISION_SNAPSHOT_LINK_REQUIRED")
    cur.execute(
        """UPDATE long_horizon_l3_admission_v1 SET decision_id=%s,snapshot_id=%s,
             simulated_order_id=%s,position_id=%s,linked_at=clock_timestamp()
             WHERE admission_id=%s AND (position_id IS NULL OR position_id=%s)""",
        (row[0], row[1], simulated_order_id, position_id, admission_id, position_id),
    )
    if cur.rowcount != 1:
        raise RuntimeError("L3_LINKAGE_CONFLICT")


def is_preserved_risk_exit(reason: str) -> bool:
    value = str(reason or "").upper()
    return any(token in value for token in ("STOP_LOSS", "PANIC", "MANUAL", "EMERGENCY", "INTEGRITY", "RISK_BUDGET"))


def guard_exit_cursor(cur, *, symbol: str, interval: str, strategy: str,
                      reason: str, candle_open_time, price: Decimal) -> tuple[bool, str]:
    if not active() or reason == TARGET_EXIT_REASON or is_preserved_risk_exit(reason):
        return True, "L3_NOT_APPLICABLE_OR_PRESERVED"
    cur.execute(
        """SELECT a.admission_id,a.position_id,p.side FROM long_horizon_l3_admission_v1 a
             JOIN positions p ON p.id=a.position_id
             WHERE a.status='ACCEPTED' AND p.status='OPEN' AND p.symbol=%s
               AND p.interval=%s AND p.strategy=%s ORDER BY a.admission_id DESC LIMIT 1""",
        (symbol.upper(), interval.lower(), strategy.upper()),
    )
    row = cur.fetchone()
    if not row:
        return True, "NOT_L3_POSITION"
    evidence = load_paper_realizable_net_evidence(
        lambda: cur.connection, trading_mode="PAPER", position_id=int(row[1]),
        symbol=symbol, interval=interval, strategy=strategy,
        current_price=Decimal(str(price)), observed_at=candle_open_time,
        source_candle_id=f"L0:{candle_open_time.isoformat()}", connection=cur.connection,
    )
    if not evidence.authoritative:
        return True, "L3_L0_COMPARATOR_INCOMPLETE_EXISTING_EXIT_PRESERVED"
    cur.execute(
        """INSERT INTO long_horizon_l3_l0_comparator_v1(
             admission_id,position_id,first_exit_at,exit_reason,exit_price,
             source_candle_open_time,status,realizable_net_at_l0_exit,fee_contract_fingerprint)
             VALUES (%s,%s,%s,%s,%s,%s,'FIRST_CAUSAL_L0_EXIT',%s,%s)
             ON CONFLICT(position_id) DO NOTHING""",
        (row[0], row[1], datetime.now(timezone.utc), str(reason), Decimal(str(price)),
         candle_open_time, evidence.realizable_net_after_all_costs,
         evidence.fee_contract_fingerprint),
    )
    return False, "L3_LEGACY_EXIT_SUPPRESSED"


def evaluate_target_owner_cycle(*, trading_mode: str, symbol: str, interval: str,
                                strategy: str, connection_factory=get_db_conn) -> TargetDecision:
    if str(trading_mode).upper() != "PAPER" or not active():
        return TargetDecision("INACTIVE")
    conn = connection_factory()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT a.admission_id,p.id FROM long_horizon_l3_admission_v1 a
                         JOIN positions p ON p.id=a.position_id WHERE a.status='ACCEPTED'
                         AND p.status='OPEN' AND p.symbol=%s AND p.interval=%s AND p.strategy=%s
                         ORDER BY a.admission_id DESC LIMIT 1 FOR UPDATE OF a""",
                    (symbol.upper(), interval.lower(), strategy.upper()),
                )
                row = cur.fetchone()
                if not row:
                    return TargetDecision("NO_OPEN_L3_POSITION")
                admission_id, position_id = int(row[0]), int(row[1])
                now = datetime.now(timezone.utc)
                mark = load_latest_finalized_canonical_one_minute_mark(cur, symbol=symbol, evaluated_at=now)
                if not mark.authoritative:
                    return TargetDecision(mark.status, position_id)
                cur.execute(
                    """SELECT e.target_reached,e.mark_price,e.source_close_time,e.realizable_net,
                              p.exit_order_id
                         FROM long_horizon_l3_event_v1 e JOIN positions p ON p.id=e.position_id
                        WHERE e.position_id=%s AND e.source_candle_id=%s""",
                    (position_id, mark.source_id),
                )
                prior = cur.fetchone()
                if prior:
                    retry = bool(prior[0]) and prior[4] is None
                    return TargetDecision(
                        "TARGET_RETRY" if retry else "SOURCE_ALREADY_EVALUATED",
                        position_id, retry, Decimal(str(prior[1])), prior[2],
                        Decimal(str(prior[3])),
                    )
                evidence = load_paper_realizable_net_evidence(
                    connection_factory, trading_mode="PAPER", position_id=position_id,
                    symbol=symbol, interval=interval, strategy=strategy,
                    current_price=mark.price, observed_at=mark.close_time,
                    source_candle_id=mark.source_id, connection=conn,
                )
                if not evidence.authoritative:
                    return TargetDecision(evidence.status, position_id)
                cur.execute(
                    """SELECT sum(fill_notional) FROM simulated_execution_fills_v1
                         WHERE position_id=%s AND order_purpose='ENTRY' AND environment='PAPER'
                         AND deployment_id='local-paper'""", (position_id,),
                )
                entry_capital = cur.fetchone()[0]
                if entry_capital is None or Decimal(str(entry_capital)) <= 0:
                    return TargetDecision("ENTRY_CAPITAL_AUTHORITY_REQUIRED", position_id)
                reached = target_reached(
                    Decimal(str(evidence.realizable_net_after_all_costs)),
                    Decimal(str(entry_capital)),
                )
                cur.execute(
                    """INSERT INTO long_horizon_l3_event_v1(
                         admission_id,position_id,event_type,source_candle_id,source_close_time,
                         mark_price,realizable_net,entry_capital,target_rate,target_reached)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (admission_id, position_id, "TARGET_EXIT_INTENT" if reached else "MARK_TO_MARKET",
                     mark.source_id, mark.close_time, mark.price, evidence.realizable_net_after_all_costs,
                     entry_capital, TARGET_NET_RATE, reached),
                )
                return TargetDecision(
                    "TARGET_REACHED" if reached else "TARGET_NOT_REACHED", position_id,
                    reached, mark.price, mark.close_time, evidence.realizable_net_after_all_costs,
                )
    finally:
        conn.close()

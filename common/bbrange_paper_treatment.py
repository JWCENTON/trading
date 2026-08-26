from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import os
from typing import Any, Mapping

from common.paper_simulation_fee_config import FEE_MODEL_V2
from common.realtime_engine import compute_realtime_snapshot


CONTRACT_VERSION = "BBRANGE_PAPER_TREATMENT_V1"
FEATURE_FLAG = "BBRANGE_PAPER_TREATMENT_V1_ENABLED"
STARTED_AT_ENV = "BBRANGE_PAPER_TREATMENT_V1_STARTED_AT"
ZERO = Decimal("0")


def _enabled(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"INVALID_{name.upper()}") from exc
    if not result.is_finite():
        raise ValueError(f"INVALID_{name.upper()}")
    return result


@dataclass(frozen=True)
class TreatmentConfig:
    requested: bool
    effective: bool
    runtime_status: str
    trading_mode: str
    started_at: str | None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None):
        values = os.environ if environ is None else environ
        requested = _enabled(values.get(FEATURE_FLAG, "0"))
        trading_mode = str(values.get("TRADING_MODE", "")).strip().upper()
        if not requested:
            status = "DISABLED"
        elif trading_mode != "PAPER":
            status = "REFUSED_NON_PAPER"
        else:
            status = "ENABLED_PAPER"
        return cls(
            requested=requested,
            effective=requested and trading_mode == "PAPER",
            runtime_status=status,
            trading_mode=trading_mode,
            started_at=(values.get(STARTED_AT_ENV) or None),
        )


@dataclass(frozen=True)
class EntryEvidence:
    observed_at: datetime
    realtime_status: str
    primary_driver: str | None
    realtime_score: Decimal | None
    mme_status: str
    orc_hint: str | None
    mme_refreshed_at: datetime | None

    def provenance(self) -> dict[str, Any]:
        return {
            "observed_at": self.observed_at,
            "realtime_status": self.realtime_status,
            "primary_driver": self.primary_driver,
            "realtime_score": self.realtime_score,
            "mme_status": self.mme_status,
            "orc_hint": self.orc_hint,
            "mme_refreshed_at": self.mme_refreshed_at,
        }


@dataclass(frozen=True)
class EntryTreatmentDecision:
    applies: bool
    blocked: bool
    reason: str
    base_decision: str
    treatment_decision: str
    interval: str
    feature_provenance: Mapping[str, Any]

    def details(self) -> dict[str, Any]:
        return {
            "treatment_name": CONTRACT_VERSION,
            "treatment_component": "ENTRY",
            "strategy": "BBRANGE",
            "interval": self.interval,
            "base_decision": self.base_decision,
            "treatment_decision": self.treatment_decision,
            "treatment_reason": self.reason,
            "feature_provenance": dict(self.feature_provenance),
            "contract_version": CONTRACT_VERSION,
        }


def evaluate_entry_treatment(
    *, config: TreatmentConfig, strategy: str, interval: str,
    evidence: EntryEvidence,
) -> EntryTreatmentDecision:
    strategy_u = str(strategy).upper()
    interval_s = str(interval)
    applies = bool(
        config.effective and strategy_u == "BBRANGE"
        and interval_s in {"1m", "5m"}
    )
    blocked = False
    reason = "TREATMENT_NOT_APPLICABLE"
    if applies and interval_s == "1m":
        blocked = str(evidence.primary_driver or "").upper() == "VOLUME"
        reason = "VOLUME_PRIMARY_DRIVER" if blocked else "NO_1M_CANDIDATE"
    elif applies and interval_s == "5m":
        # Missing MME is deliberately neutral. Only the explicit canonical hint
        # can block an entry.
        blocked = str(evidence.orc_hint or "").upper() == "ORC_AVOID_LATE_ENTRY"
        reason = "ORC_AVOID_LATE_ENTRY" if blocked else (
            "MISSING_MME_NEUTRAL" if evidence.mme_status != "AVAILABLE"
            else "NO_5M_CANDIDATE"
        )
    return EntryTreatmentDecision(
        applies=applies,
        blocked=blocked,
        reason=reason,
        base_decision="BUY",
        treatment_decision="NO_TRADE" if blocked else "BASE_DECISION",
        interval=interval_s,
        feature_provenance=evidence.provenance(),
    )


def load_entry_treatment_evidence(
    connection_factory, *, symbol: str, interval: str,
    decision_candle_timestamp: datetime,
) -> EntryEvidence:
    observed_at = decision_candle_timestamp
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    realtime = dict(compute_realtime_snapshot(
        str(symbol), str(interval), decision_candle_timestamp,
    ) or {})
    score = realtime.get("realtime_score")
    conn = connection_factory()
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload,refreshed_at
                FROM market_memory_sequence
                WHERE symbol=%s AND interval=%s
                  AND refreshed_at<=%s
                  AND (expires_at IS NULL OR expires_at>=%s)
                ORDER BY orc_readiness_score DESC NULLS LAST,
                         refreshed_at DESC
                LIMIT 1
                """,
                (str(symbol), str(interval), observed_at, observed_at),
            )
            row = cur.fetchone()
        conn.rollback()
    finally:
        conn.close()
    payload = dict(row[0] or {}) if row else {}
    return EntryEvidence(
        observed_at=observed_at,
        realtime_status=(
            "AVAILABLE" if realtime.get("ok") else
            "MISSING_AT_ENTRY:" + str(realtime.get("reason") or "REALTIME_NO_DATA")
        ),
        primary_driver=(
            str(realtime["primary_driver"]).upper()
            if realtime.get("primary_driver") is not None else None
        ),
        realtime_score=(_decimal(score, "realtime_score") if score is not None else None),
        mme_status="AVAILABLE" if row else "MISSING_AT_ENTRY:NO_ACTIVE_MME_SEQUENCE",
        orc_hint=(str(payload["orc_hint"]).upper() if payload.get("orc_hint") else None),
        mme_refreshed_at=(row[1] if row else None),
    )


@dataclass(frozen=True)
class ProfitLockEconomicState:
    status: str
    position_id: int
    interval: str
    observed_at: datetime
    economic_edge_observed: bool | None
    peak_realizable_net: Decimal | None
    current_realizable_net: Decimal | None
    quantity: Decimal | None
    fee_rate: Decimal | None
    source_authority: str
    treatment_behavior: str = "EVIDENCE_ONLY_NO_EXECUTION_CHANGE"

    def event_fields(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "contract_version": CONTRACT_VERSION,
            "treatment_name": CONTRACT_VERSION,
            "treatment_component": "PROFIT_LOCK",
        }


def _incomplete(position_id: int, interval: str, observed_at: datetime,
                status: str) -> ProfitLockEconomicState:
    return ProfitLockEconomicState(
        status=status, position_id=int(position_id), interval=str(interval),
        observed_at=observed_at, economic_edge_observed=None,
        peak_realizable_net=None, current_realizable_net=None,
        quantity=None, fee_rate=None,
        source_authority="PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
    )


def load_profit_lock_economic_state(
    connection_factory, *, position_id: int, symbol: str, interval: str,
    current_price: Decimal, observed_at: datetime,
) -> ProfitLockEconomicState:
    """Read-only, point-in-time PAPER economics; never reconstructs history."""
    price = _decimal(current_price, "current_price")
    conn = connection_factory()
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.status,p.side,p.remaining_inventory_qty,p.entry_time,
                       e.fee_rate_exit_assumption,e.fee_model_version
                FROM positions p
                LEFT JOIN entry_opportunity_evidence_v1 e
                  ON e.snapshot_id=p.entry_opportunity_snapshot_id
                WHERE p.id=%s AND p.symbol=%s AND p.strategy='BBRANGE'
                  AND p.interval=%s
                """,
                (int(position_id), str(symbol), str(interval)),
            )
            position = cur.fetchone()
            if position is None or str(position[0]) != "OPEN" or str(position[1]).upper() != "LONG":
                return _incomplete(position_id, interval, observed_at, "INCOMPLETE:POSITION")
            qty, entry_time, fee_rate, fee_model = position[2:]
            if qty is None or fee_rate is None or str(fee_model) != FEE_MODEL_V2:
                return _incomplete(position_id, interval, observed_at, "INCOMPLETE:COST_AUTHORITY")
            qty = _decimal(qty, "quantity")
            fee_rate = _decimal(fee_rate, "fee_rate")
            cur.execute(
                """
                SELECT COALESCE(sum(fill_qty),0),COALESCE(sum(fill_notional),0),
                       COALESCE(sum(authoritative_fee_usdc),0),
                       count(DISTINCT simulation_fee_rate),
                       count(*) FILTER (WHERE fee_model_version<>%s)
                FROM simulated_execution_fills_v1
                WHERE position_id=%s AND order_purpose='ENTRY'
                """,
                (FEE_MODEL_V2, int(position_id)),
            )
            entry_qty, entry_notional, entry_fees, rates, bad_models = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM simulated_execution_fills_v1 "
                "WHERE position_id=%s AND order_purpose='EXIT'",
                (int(position_id),),
            )
            prior_exits = int(cur.fetchone()[0])
            if (
                _decimal(entry_qty, "entry_qty") != qty or int(rates) != 1
                or int(bad_models) != 0 or prior_exits != 0
            ):
                return _incomplete(position_id, interval, observed_at, "INCOMPLETE:EXECUTION_SCOPE")
            entry_notional = _decimal(entry_notional, "entry_notional")
            entry_fees = _decimal(entry_fees, "entry_fees")
            cur.execute(
                """
                SELECT max(close)
                FROM candles
                WHERE symbol=%s AND interval=%s
                  AND open_time>=%s AND open_time<=%s
                """,
                (str(symbol), str(interval), entry_time, observed_at),
            )
            peak_price = cur.fetchone()[0]
        conn.rollback()
    finally:
        conn.close()
    if peak_price is None:
        return _incomplete(position_id, interval, observed_at, "INCOMPLETE:PRICE_PATH")

    def realizable(mark: Decimal) -> Decimal:
        exit_notional = qty * mark
        return exit_notional - entry_notional - entry_fees - exit_notional * fee_rate

    current_net = realizable(price)
    peak_net = realizable(_decimal(peak_price, "peak_price"))
    return ProfitLockEconomicState(
        status="CANONICAL", position_id=int(position_id), interval=str(interval),
        observed_at=observed_at, economic_edge_observed=peak_net > ZERO,
        peak_realizable_net=peak_net, current_realizable_net=current_net,
        quantity=qty, fee_rate=fee_rate,
        source_authority="PAPER_SIMULATOR_FINANCIAL_MODEL_V2",
    )

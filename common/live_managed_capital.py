"""Pure LIVE managed-capital authority semantics.

Network and persistence are deliberately outside this module.  Callers supply
raw OKX evidence, canonical marks, inventory and an explicitly accepted
baseline; all financial arithmetic remains Decimal-safe and fail-closed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping


_CONTRACT_PATH = Path(__file__).resolve().parents[1] / "contracts/live_managed_capital_authority_v1_contract.json"
_CONTRACT = json.loads(_CONTRACT_PATH.read_text())
CONTRACT_VERSION = str(_CONTRACT["contract_version"])
ACCOUNT_SCOPE = str(_CONTRACT["account_scope"])
MANAGED_ASSET_SCOPE = tuple(str(asset) for asset in _CONTRACT["managed_asset_scope"])
PRICE_FRESHNESS = timedelta(minutes=int(_CONTRACT["valuation"]["freshness_minutes"]))
ZERO = Decimal("0")


def decimal_exact(value: object, *, field: str) -> Decimal:
    if value in (None, "") or isinstance(value, float):
        raise ValueError(f"LIVE_CAPITAL_INVALID_DECIMAL:{field}")
    return Decimal(str(value))


@dataclass(frozen=True)
class RawOkxBalance:
    asset: str
    total_balance: Decimal
    available_balance: Decimal
    frozen_balance: Decimal
    order_frozen: Decimal
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class RawOkxAccountSnapshot:
    account_identity_fingerprint: str
    observed_at: datetime
    balances: tuple[RawOkxBalance, ...]
    source: str = "OKX_API_V5_ACCOUNT_BALANCE_RAW"


@dataclass(frozen=True)
class LiveManagedCapitalBaseline:
    accepted_at: datetime
    account_identity_fingerprint: str
    baseline_managed_equity: Decimal
    activation_fingerprint: str


@dataclass(frozen=True)
class InventoryLimit:
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal


@dataclass(frozen=True)
class LiveManagedCapitalEvidence:
    managed_equity: Decimal | None
    managed_equity_status: str
    raw_usdc_avail_bal: Decimal | None
    available_capital: Decimal | None
    available_capital_status: str
    reserved_capital: Decimal | None
    reserved_capital_status: str
    flow_adjusted_equity: Decimal | None
    cumulative_flow_in_usdc: Decimal
    cumulative_flow_out_usdc: Decimal
    inventory_reconciliation_status: str
    balance_observed_at: datetime | None
    mark_oldest_at: datetime | None
    incomplete_reasons: tuple[str, ...]


@dataclass(frozen=True)
class LiveManagedCapitalReadContext:
    snapshot: RawOkxAccountSnapshot
    marks: Mapping[str, tuple[Decimal | None, datetime | None]]
    inventory_quantities: Mapping[str, Decimal]
    inventory_limits: Mapping[str, InventoryLimit]


def parse_okx_balance_response(
    payload: Mapping[str, Any], *, account_identity_fingerprint: str,
    observed_at: datetime,
) -> RawOkxAccountSnapshot:
    if observed_at.tzinfo is None:
        raise ValueError("LIVE_CAPITAL_TIMESTAMP_REQUIRED")
    rows = payload.get("data") or []
    if len(rows) != 1:
        raise ValueError("LIVE_CAPITAL_OKX_ACCOUNT_ROW_NOT_EXACT")
    balances = []
    for detail in rows[0].get("details") or []:
        asset = str(detail.get("ccy") or "").upper()
        if not asset:
            raise ValueError("LIVE_CAPITAL_ASSET_REQUIRED")
        # No availBal->cashBal fallback and no inferred frozen balance.
        balances.append(RawOkxBalance(
            asset=asset,
            total_balance=decimal_exact(detail.get("cashBal"), field=f"{asset}.cashBal"),
            available_balance=decimal_exact(detail.get("availBal"), field=f"{asset}.availBal"),
            frozen_balance=decimal_exact(detail.get("frozenBal"), field=f"{asset}.frozenBal"),
            order_frozen=decimal_exact(detail.get("ordFrozen"), field=f"{asset}.ordFrozen"),
            raw={key: detail.get(key) for key in (
                "ccy", "cashBal", "availBal", "frozenBal", "ordFrozen", "eq", "eqUsd", "uTime"
            )},
        ))
    return RawOkxAccountSnapshot(
        account_identity_fingerprint=str(account_identity_fingerprint),
        observed_at=observed_at.astimezone(timezone.utc), balances=tuple(balances),
    )


def evaluate_live_managed_capital(
    snapshot: RawOkxAccountSnapshot,
    *, marks: Mapping[str, tuple[Decimal | None, datetime | None]],
    inventory_quantities: Mapping[str, Decimal],
    inventory_limits: Mapping[str, InventoryLimit],
    baseline: LiveManagedCapitalBaseline | None,
    cumulative_deposits_and_transfer_in: Decimal = ZERO,
    cumulative_withdrawals_and_transfer_out: Decimal = ZERO,
    as_of: datetime,
) -> LiveManagedCapitalEvidence:
    reasons: list[str] = []
    if as_of.tzinfo is None:
        raise ValueError("LIVE_CAPITAL_AS_OF_REQUIRED")
    if baseline is None:
        reasons.append("ACCEPTED_LIVE_BASELINE_UNAVAILABLE")
    elif baseline.account_identity_fingerprint != snapshot.account_identity_fingerprint:
        reasons.append("LIVE_ACCOUNT_IDENTITY_MISMATCH")

    by_asset = {row.asset: row for row in snapshot.balances}
    unknown = sorted(
        row.asset for row in snapshot.balances
        if row.asset not in MANAGED_ASSET_SCOPE and row.total_balance != ZERO
    )
    if unknown:
        reasons.append("UNCLASSIFIED_ACCOUNT_ASSET:" + ",".join(unknown))

    total = ZERO
    mark_times: list[datetime] = []
    valuation_complete = True
    for asset in MANAGED_ASSET_SCOPE:
        quantity = by_asset.get(asset).total_balance if asset in by_asset else ZERO
        if quantity == ZERO:
            continue
        if asset == "USDC":
            total += quantity
            continue
        price, timestamp = marks.get(asset, (None, None))
        if price is None or timestamp is None:
            valuation_complete = False
            reasons.append(f"PRICE_UNAVAILABLE:{asset}")
            continue
        if timestamp < as_of - PRICE_FRESHNESS:
            valuation_complete = False
            reasons.append(f"PRICE_STALE:{asset}")
            continue
        total += quantity * price
        mark_times.append(timestamp)

    reconciliation_complete = True
    for asset in MANAGED_ASSET_SCOPE:
        if asset == "USDC":
            continue
        exchange_qty = by_asset.get(asset).total_balance if asset in by_asset else ZERO
        inventory_qty = Decimal(str(inventory_quantities.get(asset, ZERO)))
        residual = exchange_qty - inventory_qty
        limit = inventory_limits.get(
            asset, InventoryLimit(Decimal("0.000000000001"), ZERO, ZERO)
        )
        if residual < -limit.step_size:
            reconciliation_complete = False
            reasons.append(f"INVENTORY_RECONCILIATION_DEFICIT:{asset}")
            continue
        positive = max(residual, ZERO)
        executable = (
            (positive / limit.step_size).to_integral_value(rounding=ROUND_DOWN)
            * limit.step_size if limit.step_size > ZERO else positive
        )
        price = marks.get(asset, (ZERO, None))[0] or ZERO
        material = executable > ZERO
        if limit.min_qty > ZERO and executable < limit.min_qty:
            material = False
        if limit.min_notional > ZERO and executable * price < limit.min_notional:
            material = False
        if material:
            reconciliation_complete = False
            reasons.append(f"INVENTORY_RECONCILIATION_MATERIAL_RESIDUAL:{asset}")

    complete = (
        baseline is not None
        and not unknown
        and valuation_complete
        and reconciliation_complete
        and baseline.account_identity_fingerprint == snapshot.account_identity_fingerprint
    )
    managed = total if complete else None
    flow_adjusted = None
    if managed is not None:
        flow_adjusted = (
            managed - Decimal(str(cumulative_deposits_and_transfer_in))
            + Decimal(str(cumulative_withdrawals_and_transfer_out))
        )
    usdc = by_asset.get("USDC")
    return LiveManagedCapitalEvidence(
        managed_equity=managed,
        managed_equity_status="CANONICAL" if complete else "INCOMPLETE",
        raw_usdc_avail_bal=usdc.available_balance if usdc else ZERO,
        available_capital=None,
        available_capital_status="INCOMPLETE",
        reserved_capital=None,
        reserved_capital_status="NOT_YET_CANONICAL",
        flow_adjusted_equity=flow_adjusted,
        cumulative_flow_in_usdc=Decimal(str(cumulative_deposits_and_transfer_in)),
        cumulative_flow_out_usdc=Decimal(str(cumulative_withdrawals_and_transfer_out)),
        inventory_reconciliation_status=("CANONICAL" if reconciliation_complete else "INCOMPLETE"),
        balance_observed_at=snapshot.observed_at,
        mark_oldest_at=min(mark_times) if mark_times else None,
        incomplete_reasons=tuple(dict.fromkeys(reasons + [
            "CANONICAL_INTERNAL_RESERVATION_AUTHORITY_UNAVAILABLE"
        ])),
    )


def baseline_fingerprint(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_live_baseline_plan(
    context: LiveManagedCapitalReadContext, *, deployment_id: str,
    accepted_at: datetime, runtime_revision: str,
) -> dict[str, Any]:
    provisional = LiveManagedCapitalBaseline(
        accepted_at, context.snapshot.account_identity_fingerprint, ZERO, "0" * 64
    )
    evidence = evaluate_live_managed_capital(
        context.snapshot, marks=context.marks,
        inventory_quantities=context.inventory_quantities,
        inventory_limits=context.inventory_limits, baseline=provisional,
        as_of=accepted_at,
    )
    payload = {
        "environment": "LIVE",
        "deployment_id": deployment_id,
        "contract_version": CONTRACT_VERSION,
        "account_identity_fingerprint": context.snapshot.account_identity_fingerprint,
        "account_scope": ACCOUNT_SCOPE,
        "accepted_at": accepted_at.astimezone(timezone.utc).isoformat(),
        "managed_asset_scope": list(MANAGED_ASSET_SCOPE),
        "raw_balance_snapshot": {
            row.asset: {
                "total_balance": str(row.total_balance),
                "available_balance": str(row.available_balance),
                "frozen_balance": str(row.frozen_balance),
                "order_frozen": str(row.order_frozen),
                "raw": dict(row.raw),
            } for row in context.snapshot.balances
        },
        "valuation_snapshot": {
            asset: {
                "mark": "1" if asset == "USDC" else (
                    None if context.marks.get(asset, (None, None))[0] is None
                    else str(context.marks[asset][0])
                ),
                "timestamp": accepted_at.isoformat() if asset == "USDC" else (
                    None if context.marks.get(asset, (None, None))[1] is None
                    else context.marks[asset][1].isoformat()
                ),
                "authority": "USDC_PAR" if asset == "USDC" else "candles.close/1m",
            } for asset in MANAGED_ASSET_SCOPE
        },
        "baseline_managed_equity": (
            None if evidence.managed_equity is None else str(evidence.managed_equity)
        ),
        "raw_okx_usdc_avail_bal": (
            None if evidence.raw_usdc_avail_bal is None else str(evidence.raw_usdc_avail_bal)
        ),
        "available_capital": None,
        "available_capital_status": "INCOMPLETE",
        "reserved_capital": None,
        "reserved_capital_status": "NOT_YET_CANONICAL",
        "ownership_reconciliation_status": evidence.inventory_reconciliation_status,
        "runtime_revision": runtime_revision,
        "plan_status": evidence.managed_equity_status,
        "incomplete_reasons": list(evidence.incomplete_reasons),
    }
    payload["activation_fingerprint"] = baseline_fingerprint(payload)
    return payload


def activate_live_managed_capital_baseline(
    cur: Any, *, plan: Mapping[str, Any], expected_fingerprint: str,
    approved_by: str, approval_reference: Mapping[str, Any],
) -> int:
    """Explicitly gated writer; never called by migrations or ordinary reads."""
    supplied = dict(plan)
    fingerprint = str(supplied.pop("activation_fingerprint", ""))
    if fingerprint != expected_fingerprint or baseline_fingerprint(supplied) != fingerprint:
        raise ValueError("LIVE_BASELINE_FINGERPRINT_MISMATCH")
    if supplied.get("plan_status") != "CANONICAL":
        raise ValueError("LIVE_BASELINE_PLAN_INCOMPLETE")
    approver = str(approved_by).strip()
    approval = dict(approval_reference or {})
    if not approver or not approval:
        raise ValueError("LIVE_BASELINE_PRODUCT_OWNER_APPROVAL_REQUIRED")
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"{CONTRACT_VERSION}:{supplied['deployment_id']}",),
    )
    cur.execute(
        """INSERT INTO live_managed_capital_baseline_v1(
             environment,deployment_id,contract_version,
             account_identity_fingerprint,account_scope,accepted_at,
             managed_asset_scope,raw_balance_snapshot,valuation_snapshot,
             baseline_managed_equity,raw_okx_usdc_avail_bal,
             available_capital,available_capital_status,reserved_capital,
             reserved_capital_status,ownership_reconciliation_status,
             runtime_revision,approved_by,approval_reference,
             activation_fingerprint
           ) VALUES (
             %s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s,
             %s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s
           ) RETURNING baseline_id""",
        (
            supplied["environment"], supplied["deployment_id"],
            supplied["contract_version"], supplied["account_identity_fingerprint"],
            supplied["account_scope"], supplied["accepted_at"],
            json.dumps(supplied["managed_asset_scope"]),
            json.dumps(supplied["raw_balance_snapshot"]),
            json.dumps(supplied["valuation_snapshot"]),
            supplied["baseline_managed_equity"], supplied["raw_okx_usdc_avail_bal"],
            supplied["available_capital"], supplied["available_capital_status"],
            supplied["reserved_capital"], supplied["reserved_capital_status"],
            supplied["ownership_reconciliation_status"], supplied["runtime_revision"],
            approver, json.dumps(approval), fingerprint,
        ),
    )
    return int(cur.fetchone()[0])


def record_owner_capital_flow(
    cur: Any, *, environment: str, deployment_id: str,
    account_identity_fingerprint: str, source_event_identity: str,
    asset: str, quantity: Decimal, value_usdc: Decimal,
    event_at: datetime, event_type: str, source: str,
    raw_provenance_reference: Mapping[str, Any],
    valuation_provenance: Mapping[str, Any],
) -> int:
    """Append one idempotent, account-fenced owner flow; never trading PnL."""
    mode = str(environment).upper()
    deployment = str(deployment_id).lower()
    kind = str(event_type).upper()
    if mode != "LIVE" or deployment not in {"local-live", "vps-live"}:
        raise ValueError("OWNER_CAPITAL_FLOW_ENVIRONMENT_FENCE")
    if kind not in {"DEPOSIT", "WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT"}:
        raise ValueError("OWNER_CAPITAL_FLOW_EVENT_TYPE_INVALID")
    if len(str(account_identity_fingerprint)) != 64:
        raise ValueError("OWNER_CAPITAL_FLOW_ACCOUNT_IDENTITY_REQUIRED")
    if event_at.tzinfo is None:
        raise ValueError("OWNER_CAPITAL_FLOW_TIMESTAMP_REQUIRED")
    qty = decimal_exact(quantity, field="owner_flow.quantity")
    value = decimal_exact(value_usdc, field="owner_flow.value_usdc")
    if qty <= ZERO or value <= ZERO:
        raise ValueError("OWNER_CAPITAL_FLOW_POSITIVE_VALUE_REQUIRED")
    provenance = dict(raw_provenance_reference or {})
    valuation = dict(valuation_provenance or {})
    if not provenance or not valuation:
        raise ValueError("OWNER_CAPITAL_FLOW_PROVENANCE_REQUIRED")
    identity = (
        mode, deployment, str(account_identity_fingerprint), str(source),
        str(source_event_identity),
    )
    values = identity[:3] + (
        identity[4], str(asset).upper(), qty, value, event_at, kind,
        identity[3], json.dumps(provenance), json.dumps(valuation),
    )
    cur.execute(
        """INSERT INTO owner_capital_flow_v1(
             environment,deployment_id,account_identity_fingerprint,
             source_event_identity,asset,quantity,value_usdc,event_at,
             event_type,source,evidence_status,raw_provenance_reference,
             valuation_provenance
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE',%s::jsonb,%s::jsonb)
           ON CONFLICT(environment,deployment_id,account_identity_fingerprint,source,source_event_identity)
           DO NOTHING RETURNING flow_id""",
        values,
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute(
        """SELECT flow_id,asset,quantity,value_usdc,event_at,event_type,
                  raw_provenance_reference,valuation_provenance
           FROM owner_capital_flow_v1
           WHERE environment=%s AND deployment_id=%s
             AND account_identity_fingerprint=%s AND source=%s
             AND source_event_identity=%s""",
        identity,
    )
    existing = cur.fetchone()
    expected = (
        str(asset).upper(), qty, value, event_at, kind, provenance, valuation
    )
    if not existing or tuple(existing[1:]) != expected:
        raise ValueError("OWNER_CAPITAL_FLOW_IDEMPOTENCY_CONFLICT")
    return int(existing[0])


def record_live_managed_equity_observation(
    cur: Any, *, baseline_id: int, deployment_id: str,
    observed_at: datetime, evidence: LiveManagedCapitalEvidence,
) -> int:
    if (
        evidence.managed_equity_status != "CANONICAL"
        or evidence.managed_equity is None
        or evidence.flow_adjusted_equity is None
    ):
        raise ValueError("LIVE_MANAGED_EQUITY_OBSERVATION_INCOMPLETE")
    payload = {
        "baseline_id": int(baseline_id), "deployment_id": deployment_id,
        "observed_at": observed_at.isoformat(),
        "raw_managed_equity": str(evidence.managed_equity),
        "cumulative_flow_in_usdc": str(evidence.cumulative_flow_in_usdc),
        "cumulative_flow_out_usdc": str(evidence.cumulative_flow_out_usdc),
        "flow_adjusted_equity": str(evidence.flow_adjusted_equity),
    }
    fingerprint = baseline_fingerprint(payload)
    cur.execute(
        """INSERT INTO live_managed_equity_observation_v1(
             baseline_id,deployment_id,observed_at,raw_managed_equity,
             cumulative_flow_in_usdc,cumulative_flow_out_usdc,
             flow_adjusted_equity,evidence_fingerprint,evidence_status
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE')
           ON CONFLICT(evidence_fingerprint) DO NOTHING
           RETURNING observation_id""",
        (baseline_id, deployment_id, observed_at, evidence.managed_equity,
         evidence.cumulative_flow_in_usdc, evidence.cumulative_flow_out_usdc,
         evidence.flow_adjusted_equity, fingerprint),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute(
        "SELECT observation_id FROM live_managed_equity_observation_v1 "
        "WHERE evidence_fingerprint=%s", (fingerprint,),
    )
    return int(cur.fetchone()[0])


def load_live_managed_capital_evidence(
    cur: Any, *, exchange_client: Any, deployment_id: str, as_of: datetime,
) -> tuple[
    LiveManagedCapitalEvidence, LiveManagedCapitalBaseline | None,
    Decimal | None, LiveManagedCapitalReadContext,
]:
    """Load current LIVE evidence without performing any database mutation."""
    cur.execute("SELECT to_regclass('public.live_managed_capital_baseline_v1')")
    schema_ready = cur.fetchone()[0] is not None
    baseline = None
    baseline_id = None
    if schema_ready:
        cur.execute(
            """SELECT baseline_id,accepted_at,account_identity_fingerprint,
                      baseline_managed_equity,activation_fingerprint
               FROM live_managed_capital_baseline_v1
               WHERE deployment_id=%s AND contract_version=%s""",
            (deployment_id, CONTRACT_VERSION),
        )
        row = cur.fetchone()
        if row:
            baseline_id = int(row[0])
            baseline = LiveManagedCapitalBaseline(
                accepted_at=row[1], account_identity_fingerprint=str(row[2]),
                baseline_managed_equity=Decimal(str(row[3])),
                activation_fingerprint=str(row[4]),
            )

    identity, _diagnostic = exchange_client.get_account_identity(refresh=True)
    snapshot = parse_okx_balance_response(
        exchange_client.get_raw_account_balance(),
        account_identity_fingerprint=identity.fingerprint, observed_at=as_of,
    )
    marks: dict[str, tuple[Decimal | None, datetime | None]] = {}
    for asset in MANAGED_ASSET_SCOPE:
        if asset == "USDC":
            continue
        cur.execute(
            """SELECT close,open_time FROM candles
               WHERE symbol=%s AND interval='1m' AND open_time<=%s
               ORDER BY open_time DESC LIMIT 1""",
            (f"{asset}USDC", as_of),
        )
        row = cur.fetchone()
        marks[asset] = (
            (None, None) if not row
            else (Decimal(str(row[0])), row[1])
        )
    cur.execute(
        """SELECT regexp_replace(upper(symbol),'USDC$',''),
                  sum(remaining_inventory_qty)
           FROM positions WHERE status='OPEN'
             AND remaining_inventory_qty IS NOT NULL
             AND inventory_evidence_status='COMPLETE'
           GROUP BY 1"""
    )
    inventory = {str(asset): Decimal(str(qty)) for asset, qty in cur.fetchall()}
    cur.execute(
        """SELECT DISTINCT ON (symbol)
                  regexp_replace(upper(symbol),'USDC$',''),step_size,min_qty,min_notional
           FROM financial_truth_instrument_snapshot_v1
           ORDER BY symbol,captured_at DESC,id DESC"""
    )
    limits = {
        str(asset): InventoryLimit(
            Decimal(str(step or 0)), Decimal(str(min_qty or 0)),
            Decimal(str(min_notional or 0)),
        ) for asset, step, min_qty, min_notional in cur.fetchall()
    }
    flow_in = flow_out = ZERO
    peak = None
    if baseline is not None:
        cur.execute(
            """SELECT
                 COALESCE(sum(value_usdc) FILTER (WHERE event_type IN ('DEPOSIT','TRANSFER_IN')),0),
                 COALESCE(sum(value_usdc) FILTER (WHERE event_type IN ('WITHDRAWAL','TRANSFER_OUT')),0)
               FROM owner_capital_flow_v1
               WHERE deployment_id=%s AND account_identity_fingerprint=%s
                 AND evidence_status='COMPLETE' AND event_at>%s AND event_at<=%s""",
            (deployment_id, baseline.account_identity_fingerprint,
             baseline.accepted_at, as_of),
        )
        flow_in, flow_out = (Decimal(str(value)) for value in cur.fetchone())
        cur.execute(
            """SELECT max(flow_adjusted_equity)
               FROM live_managed_equity_observation_v1
               WHERE baseline_id=%s AND evidence_status='COMPLETE'
                 AND observed_at<=%s""",
            (baseline_id, as_of),
        )
        peak_row = cur.fetchone()
        peak = None if not peak_row or peak_row[0] is None else Decimal(str(peak_row[0]))
    evidence = evaluate_live_managed_capital(
        snapshot, marks=marks, inventory_quantities=inventory,
        inventory_limits=limits, baseline=baseline,
        cumulative_deposits_and_transfer_in=flow_in,
        cumulative_withdrawals_and_transfer_out=flow_out, as_of=as_of,
    )
    if not schema_ready:
        evidence = LiveManagedCapitalEvidence(
            **{**evidence.__dict__, "incomplete_reasons": tuple(dict.fromkeys(
                ("LIVE_MANAGED_CAPITAL_SCHEMA_UNAVAILABLE",) + evidence.incomplete_reasons
            ))}
        )
    return evidence, baseline, peak, LiveManagedCapitalReadContext(
        snapshot=snapshot, marks=marks, inventory_quantities=inventory,
        inventory_limits=limits,
    )

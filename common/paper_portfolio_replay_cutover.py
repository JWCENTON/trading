"""Immutable forward-only PAPER Portfolio State replay cutover authority."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any, Callable, Mapping

from psycopg2.extras import Json

from common.capital_reservation import paper_account_identity_fingerprint
from common.position_risk_boundary import load_boundary_projections_cursor


CONTRACT_VERSION = "PAPER_PORTFOLIO_REPLAY_CUTOVER_V1"
PAPER_DEPLOYMENTS = frozenset({"local-paper", "vps-paper"})


class PaperPortfolioReplayUnavailable(RuntimeError):
    """Historical replay is outside the accepted forward-only interval."""


def _decimal(value: object, field: str) -> Decimal:
    if value is None or isinstance(value, float):
        raise ValueError(f"PAPER_REPLAY_CUTOVER_INVALID_DECIMAL:{field}")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"PAPER_REPLAY_CUTOVER_INVALID_DECIMAL:{field}")
    return result


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("PAPER_REPLAY_CUTOVER_TIMESTAMP_REQUIRED")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize(item) for item in value]
    if isinstance(value, float):
        raise ValueError("PAPER_REPLAY_CUTOVER_FLOAT_FORBIDDEN")
    return value


def fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PaperPortfolioReplayCutover:
    cutover_id: int
    deployment_id: str
    cutover_at: datetime
    git_revision: str
    portfolio_state_fingerprint: str
    cutover_fingerprint: str
    inventory_position_count: int


def replay_cutover_schema_available_cursor(cur: Any) -> bool:
    cur.execute("SELECT to_regclass('public.paper_portfolio_replay_cutover_v1')")
    row = cur.fetchone()
    return bool(row and row[0] is not None)


def load_replay_cutover_cursor(
    cur: Any, *, deployment_id: str,
) -> PaperPortfolioReplayCutover | None:
    deployment = str(deployment_id).strip().lower()
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("PAPER_REPLAY_CUTOVER_DEPLOYMENT_INVALID")
    if not replay_cutover_schema_available_cursor(cur):
        return None
    cur.execute(
        """SELECT cutover_id,deployment_id,cutover_at,git_revision,
                  portfolio_state_fingerprint,cutover_fingerprint,
                  inventory_position_count
           FROM paper_portfolio_replay_cutover_v1
           WHERE deployment_id=%s AND contract_version=%s""",
        (deployment, CONTRACT_VERSION),
    )
    row = cur.fetchone()
    return None if row is None else PaperPortfolioReplayCutover(
        int(row[0]), str(row[1]), row[2], str(row[3]), str(row[4]),
        str(row[5]), int(row[6]),
    )


def require_replay_cutover_cursor(
    cur: Any, *, deployment_id: str, as_of: datetime,
) -> PaperPortfolioReplayCutover:
    cutover = load_replay_cutover_cursor(cur, deployment_id=deployment_id)
    if cutover is None:
        raise PaperPortfolioReplayUnavailable(
            "PAPER_PORTFOLIO_REPLAY_CUTOVER_UNAVAILABLE"
        )
    if as_of < cutover.cutover_at:
        raise PaperPortfolioReplayUnavailable("UNSUPPORTED_PRE_REPLAY_CUTOVER")
    return cutover


def calibration_replay_eligibility_cursor(
    cur: Any, *, deployment_id: str, evaluation_as_of: datetime,
) -> tuple[bool, str]:
    cutover = load_replay_cutover_cursor(cur, deployment_id=deployment_id)
    if cutover is None or evaluation_as_of < cutover.cutover_at:
        return False, "PRE_CUTOVER_NON_CAUSAL_CALIBRATION_EVIDENCE"
    return True, "CANONICAL_REPLAY_INTERVAL_REQUIRED"


def create_replay_cutover_cursor(
    cur: Any, *, deployment_id: str, git_revision: str,
    portfolio_state_reader: Callable[..., Any] | None = None,
) -> tuple[PaperPortfolioReplayCutover, bool]:
    """Atomically freeze complete current PAPER inventory; caller owns commit."""
    deployment = str(deployment_id).strip().lower()
    revision = str(git_revision).strip().lower()
    if deployment not in PAPER_DEPLOYMENTS:
        raise ValueError("PAPER_REPLAY_CUTOVER_DEPLOYMENT_INVALID")
    if len(revision) != 40 or any(ch not in "0123456789abcdef" for ch in revision):
        raise ValueError("PAPER_REPLAY_CUTOVER_GIT_REVISION_INVALID")
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (f"{CONTRACT_VERSION}:{deployment}",),
    )
    existing = load_replay_cutover_cursor(cur, deployment_id=deployment)
    if existing is not None:
        return existing, False
    # Freeze the lifecycle writers before taking the canonical current read.
    # The lock is transaction-scoped and used only by the explicit activation.
    cur.execute(
        "LOCK TABLE positions,simulated_execution_fills_v1,"
        "position_risk_boundary_event_v1 IN SHARE MODE"
    )
    if portfolio_state_reader is None:
        from common.portfolio_state import read_portfolio_state
        portfolio_state_reader = read_portfolio_state
    state = portfolio_state_reader(
        cur, environment="PAPER", deployment_id=deployment, as_of=None,
        runtime_revision=revision,
    )
    required = {
        "total_capital_status": state.total_capital_status,
        "open_positions_status": state.open_positions_status,
        "deployed_capital_status": state.deployed_capital_status,
        "unrealized_pnl_status": state.unrealized_pnl_status,
        "open_risk_status": state.open_risk_status,
    }
    if (
        any(required[key] != "CANONICAL" for key in required if key != "open_risk_status")
        or required["open_risk_status"] not in {"CANONICAL", "CANONICAL_EMPTY"}
    ):
        raise RuntimeError("PAPER_REPLAY_CUTOVER_CURRENT_PORTFOLIO_INCOMPLETE")
    state_payload = state.serializable()
    state_fingerprint = fingerprint(state_payload)
    account = paper_account_identity_fingerprint(deployment)
    boundaries, boundary_status = load_boundary_projections_cursor(
        cur, environment="PAPER", deployment_id=deployment,
        account_identity_fingerprint=account, as_of=None,
    )
    if state.open_positions_count and boundary_status != "CANONICAL":
        raise RuntimeError("PAPER_REPLAY_CUTOVER_BOUNDARY_AUTHORITY_INCOMPLETE")
    cur.execute(
        """SELECT id,symbol,strategy,interval,side,remaining_inventory_qty,
                  entry_price,inventory_evidence_status,entry_order_id,
                  entry_opportunity_snapshot_id,inventory_contract_adoption_id,
                  inventory_contract_generation
           FROM positions WHERE status='OPEN' ORDER BY id"""
    )
    inventory_rows = cur.fetchall()
    if len(inventory_rows) != state.open_positions_count:
        raise RuntimeError("PAPER_REPLAY_CUTOVER_MEMBERSHIP_MISMATCH")
    risk_by_id = {item.position_id: item for item in state.position_risk}
    frozen: list[dict[str, Any]] = []
    for row in inventory_rows:
        position_id = int(row[0])
        qty = _decimal(row[5], "remaining_inventory_qty")
        basis = _decimal(row[6], "entry_basis_price")
        projection = boundaries.get(position_id)
        risk = risk_by_id.get(position_id)
        lineage = {
            "entry_order_id": None if row[8] is None else str(row[8]),
            "entry_opportunity_snapshot_id": None if row[9] is None else str(row[9]),
            "inventory_contract_adoption_id": row[10],
            "inventory_contract_generation": row[11],
        }
        if (
            qty <= 0 or basis <= 0 or str(row[7]) != "COMPLETE"
            or not any(value is not None for value in lineage.values())
            or projection is None or projection.position_id != position_id
            or projection.state not in {"BOUNDARY_ACTIVATED", "BOUNDARY_REVISED_ENTRY_BASIS"}
            or risk is None or risk.status != "CANONICAL"
        ):
            raise RuntimeError(
                f"PAPER_REPLAY_CUTOVER_POSITION_INCOMPLETE:{position_id}"
            )
        risk_payload = asdict(risk)
        item = {
            "position_id": position_id, "symbol": str(row[1]),
            "strategy": str(row[2]), "interval": str(row[3]),
            "side": str(row[4]), "remaining_inventory_qty": qty,
            "entry_basis_price": basis, "inventory_evidence_status": str(row[7]),
            "entry_lineage": lineage, "boundary_id": str(projection.boundary_id),
            "boundary_policy_fingerprint": projection.policy_fingerprint,
            "boundary_effective_at": projection.effective_at,
            "risk_owner": "POSITION_OPEN_RISK",
            "open_risk_evidence_fingerprint": fingerprint(risk_payload),
        }
        item["position_evidence_fingerprint"] = fingerprint(item)
        frozen.append(item)
    cutover_at = state.as_of.astimezone(timezone.utc)
    source_evidence = {
        "current_portfolio_state_status": "CANONICAL",
        "portfolio_state_source_authorities": state.source_authorities,
        "one_risk_owner_per_quantity_slice": True,
    }
    cutover_payload = {
        "contract_version": CONTRACT_VERSION, "deployment_id": deployment,
        "cutover_at": cutover_at, "git_revision": revision,
        "portfolio_state_fingerprint": state_fingerprint,
        "positions": frozen,
    }
    cutover_fingerprint = fingerprint(cutover_payload)
    cur.execute(
        """INSERT INTO paper_portfolio_replay_cutover_v1(
             deployment_id,cutover_at,git_revision,contract_version,
             portfolio_state_fingerprint,cutover_fingerprint,
             inventory_position_count,source_evidence)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING cutover_id""",
        (deployment, cutover_at, revision, CONTRACT_VERSION, state_fingerprint,
         cutover_fingerprint, len(frozen), Json(_normalize(source_evidence))),
    )
    cutover_id = int(cur.fetchone()[0])
    for item in frozen:
        cur.execute(
            """INSERT INTO paper_portfolio_replay_cutover_position_v1(
                 cutover_id,position_id,symbol,strategy,interval,side,
                 remaining_inventory_qty,entry_basis_price,
                 inventory_evidence_status,entry_lineage,boundary_id,
                 boundary_policy_fingerprint,boundary_effective_at,risk_owner,
                 open_risk_evidence_fingerprint,position_evidence_fingerprint)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (cutover_id, item["position_id"], item["symbol"], item["strategy"],
             item["interval"], item["side"], item["remaining_inventory_qty"],
             item["entry_basis_price"], item["inventory_evidence_status"],
             Json(_normalize(item["entry_lineage"])), item["boundary_id"],
             item["boundary_policy_fingerprint"], item["boundary_effective_at"],
             item["risk_owner"], item["open_risk_evidence_fingerprint"],
             item["position_evidence_fingerprint"]),
        )
    return PaperPortfolioReplayCutover(
        cutover_id, deployment, cutover_at, revision, state_fingerprint,
        cutover_fingerprint, len(frozen),
    ), True

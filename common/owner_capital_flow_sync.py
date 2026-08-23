"""Canonical OKX Trading Account owner-flow synchronization authority V1."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from common.live_managed_capital import record_owner_capital_flow


_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts/owner_capital_flow_canonical_sync_authority_v1_contract.json"
)
_CONTRACT = json.loads(_CONTRACT_PATH.read_text())
CONTRACT_VERSION = str(_CONTRACT["contract_version"])
SOURCE = str(_CONTRACT["required_source"])
SOURCE_SAFETY_LAG = timedelta(
    seconds=int(_CONTRACT["source_safety_lag_seconds"])
)
OVERLAP_RESCAN = timedelta(seconds=int(_CONTRACT["overlap_rescan_seconds"]))
PAGE_SIZE = int(_CONTRACT["page_size"])
BASELINE_BOOTSTRAP = datetime.fromisoformat(str(_CONTRACT["baseline_bootstrap"]))
CURRENT_ENDPOINT = "/api/v5/account/bills"
ARCHIVE_ENDPOINT = "/api/v5/account/bills-archive"
ZERO = Decimal("0")


class OwnerFlowSyncError(RuntimeError):
    def __init__(self, status: str, code: str):
        super().__init__(code)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class CanonicalBoundaryFlow:
    source_event_identity: str
    bill_id: str
    event_at: datetime
    event_type: str
    asset: str
    amount: Decimal
    raw_provenance_reference: Mapping[str, Any]


@dataclass(frozen=True)
class OwnerFlowSyncResult:
    status: str
    run_id: str | None
    source_cutoff: datetime | None
    sync_through: datetime | None
    page_count: int
    source_event_count: int
    canonical_event_count: int
    late_event_count: int
    error_code: str | None = None


@dataclass(frozen=True)
class OwnerFlowHistoryAuthority:
    cumulative_flow_in: Decimal | None
    cumulative_flow_out: Decimal | None
    sync_through: datetime | None
    flow_history_status: str
    run_id: str | None


def _canonical_json(value: Any) -> str:
    if isinstance(value, float):
        raise ValueError("OWNER_FLOW_CANONICAL_JSON_FLOAT_FORBIDDEN")
    if isinstance(value, Mapping):
        for item in value.values():
            _canonical_json(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _canonical_json(item)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False, default=str,
    )


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _exact_decimal(value: object, *, field: str) -> Decimal:
    if value in (None, "") or isinstance(value, float):
        raise OwnerFlowSyncError(
            "PARTIAL_SYNC", f"SOURCE_DECIMAL_INVALID:{field}"
        )
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise OwnerFlowSyncError(
            "PARTIAL_SYNC", f"SOURCE_DECIMAL_INVALID:{field}"
        ) from exc
    if not number.is_finite():
        raise OwnerFlowSyncError(
            "PARTIAL_SYNC", f"SOURCE_DECIMAL_INVALID:{field}"
        )
    return number


def _event_at(value: object) -> datetime:
    try:
        milliseconds = int(str(value))
    except (TypeError, ValueError) as exc:
        raise OwnerFlowSyncError(
            "PARTIAL_SYNC", "SOURCE_TIMESTAMP_INVALID"
        ) from exc
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def classify_trading_account_bill(
    row: Mapping[str, Any],
) -> CanonicalBoundaryFlow | None:
    """Map only exact Product-Owner-approved Trading Account crossings."""
    if str(row.get("type") or "") != "1":
        return None
    subtype = str(row.get("subType") or "")
    if subtype not in {"11", "12"}:
        return None

    bill_id = str(row.get("billId") or "").strip()
    if not bill_id:
        raise OwnerFlowSyncError("PARTIAL_SYNC", "SOURCE_BILL_ID_REQUIRED")
    source_identity = f"OKX:TRADING_BILL:{bill_id}"
    asset = str(row.get("ccy") or "").upper().strip()
    if asset != "USDC":
        raise OwnerFlowSyncError(
            "UNSUPPORTED_ASSET", f"UNSUPPORTED_ASSET:{asset or 'EMPTY'}"
        )
    source_from = str(row.get("from") or "")
    source_to = str(row.get("to") or "")
    balance_change = _exact_decimal(row.get("balChg"), field="balChg")

    if subtype == "11":
        if source_from != "6" or source_to != "18" or balance_change <= ZERO:
            raise OwnerFlowSyncError(
                "PARTIAL_SYNC", "TRANSFER_IN_SOURCE_CONTRACT_MISMATCH"
            )
        event_type = "TRANSFER_IN"
    else:
        if source_from != "18" or source_to != "6" or balance_change >= ZERO:
            raise OwnerFlowSyncError(
                "PARTIAL_SYNC", "TRANSFER_OUT_SOURCE_CONTRACT_MISMATCH"
            )
        event_type = "TRANSFER_OUT"

    raw = {
        "endpoint_authority": "OKX_TRADING_ACCOUNT_BILLS",
        "billId": bill_id,
        "type": "1",
        "subType": subtype,
        "from": source_from,
        "to": source_to,
        "ccy": asset,
        "balChg": str(row.get("balChg")),
        "ts": str(row.get("ts")),
    }
    return CanonicalBoundaryFlow(
        source_event_identity=source_identity,
        bill_id=bill_id,
        event_at=_event_at(row.get("ts")),
        event_type=event_type,
        asset=asset,
        amount=abs(balance_change),
        raw_provenance_reference=raw,
    )


def fetch_exhaustive_trading_account_bills(
    exchange_client: Any,
    *,
    range_from: datetime,
    source_cutoff: datetime,
    archive: bool,
) -> tuple[list[Mapping[str, Any]], int, str | None]:
    """Exhaust a fixed OKX bill range using immutable billId pagination."""
    if range_from.tzinfo is None or source_cutoff.tzinfo is None:
        raise ValueError("OWNER_FLOW_SYNC_TIMESTAMP_REQUIRED")
    after = None
    pages = 0
    rows_by_id: dict[str, Mapping[str, Any]] = {}
    terminal_cursor = None
    previous_cursor = None
    begin_ms = int(range_from.timestamp() * 1000)
    end_ms = int(source_cutoff.timestamp() * 1000)

    while True:
        try:
            payload = exchange_client.get_account_bills_page(
                archive=archive,
                after=after,
                begin_ms=begin_ms,
                end_ms=end_ms,
                limit=PAGE_SIZE,
            )
        except OwnerFlowSyncError:
            raise
        except Exception as exc:
            raise OwnerFlowSyncError(
                "SOURCE_UNAVAILABLE", "TRADING_ACCOUNT_BILLS_UNAVAILABLE"
            ) from exc
        if str(payload.get("code")) != "0":
            raise OwnerFlowSyncError(
                "SOURCE_UNAVAILABLE", "TRADING_ACCOUNT_BILLS_REJECTED"
            )
        page = payload.get("data")
        if not isinstance(page, list):
            raise OwnerFlowSyncError(
                "PAGINATION_INCOMPLETE", "TRADING_ACCOUNT_BILLS_PAGE_INVALID"
            )
        pages += 1
        if pages > 10000:
            raise OwnerFlowSyncError(
                "PAGINATION_INCOMPLETE", "TRADING_ACCOUNT_BILLS_PAGE_LIMIT"
            )

        page_timestamps: list[int] = []
        for raw_row in page:
            if not isinstance(raw_row, Mapping):
                raise OwnerFlowSyncError(
                    "PAGINATION_INCOMPLETE", "TRADING_ACCOUNT_BILL_ROW_INVALID"
                )
            bill_id = str(raw_row.get("billId") or "").strip()
            if not bill_id:
                raise OwnerFlowSyncError(
                    "PAGINATION_INCOMPLETE", "SOURCE_BILL_ID_REQUIRED"
                )
            timestamp = int(_event_at(raw_row.get("ts")).timestamp() * 1000)
            page_timestamps.append(timestamp)
            existing = rows_by_id.get(bill_id)
            if existing is not None and _canonical_json(existing) != _canonical_json(raw_row):
                raise OwnerFlowSyncError(
                    "PARTIAL_SYNC", "SOURCE_BILL_ID_PAYLOAD_CONFLICT"
                )
            rows_by_id[bill_id] = dict(raw_row)
        if page_timestamps != sorted(page_timestamps, reverse=True):
            raise OwnerFlowSyncError(
                "PAGINATION_INCOMPLETE", "SOURCE_ORDERING_INVALID"
            )

        if not page or len(page) < PAGE_SIZE:
            break
        terminal_cursor = str(page[-1].get("billId") or "").strip()
        if not terminal_cursor or terminal_cursor == previous_cursor:
            raise OwnerFlowSyncError(
                "PAGINATION_INCOMPLETE", "SOURCE_CURSOR_NOT_ADVANCING"
            )
        previous_cursor = terminal_cursor
        after = terminal_cursor

    ordered = sorted(
        rows_by_id.values(),
        key=lambda item: (_event_at(item.get("ts")), str(item.get("billId"))),
    )
    return ordered, pages, terminal_cursor


def _insert_sync_run(
    cur: Any,
    *,
    run_id: str,
    deployment_id: str,
    account_identity_fingerprint: str,
    range_from: datetime,
    source_cutoff: datetime,
    endpoint: str,
    terminal_cursor: str | None,
    last_source_event_id: str | None,
    page_count: int,
    source_event_count: int,
    canonical_event_count: int,
    late_event_count: int,
    started_at: datetime,
    completed_at: datetime,
    producer_identity: str,
    git_revision: str,
    source_fingerprint: str,
    status: str,
    error_code: str | None,
    evidence: Mapping[str, Any],
) -> None:
    cur.execute(
        """INSERT INTO owner_capital_flow_sync_run_v1(
             run_id,environment,deployment_id,account_identity_fingerprint,
             source,contract_version,range_from,source_cutoff,overlap_from,
             sync_through,source_endpoint,terminal_cursor,last_source_event_id,
             page_count,source_event_count,canonical_event_count,late_event_count,
             started_at,completed_at,producer_identity,git_revision,
             source_fingerprint,status,error_code,evidence
           ) VALUES (
             %s,'LIVE',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
             %s,%s,%s,%s,%s,%s,%s,%s::jsonb
           )""",
        (
            run_id, deployment_id, account_identity_fingerprint, SOURCE,
            CONTRACT_VERSION, range_from, source_cutoff, range_from,
            source_cutoff if status == "CANONICAL" else None, endpoint,
            terminal_cursor, last_source_event_id, page_count,
            source_event_count, canonical_event_count, late_event_count,
            started_at, completed_at, producer_identity, git_revision,
            source_fingerprint, status, error_code,
            _canonical_json(dict(evidence)),
        ),
    )


def _persist_failed_sync_run(
    conn: Any,
    *,
    run_id: str,
    deployment_id: str,
    account_identity_fingerprint: str,
    range_from: datetime,
    source_cutoff: datetime,
    endpoint: str,
    started_at: datetime,
    producer_identity: str,
    git_revision: str,
    status: str,
    error_code: str,
) -> OwnerFlowSyncResult:
    conn.rollback()
    completed_at = max(datetime.now(timezone.utc), started_at)
    failure_fingerprint = _fingerprint({
        "source": SOURCE,
        "range_from": range_from.isoformat(),
        "source_cutoff": source_cutoff.isoformat(),
        "endpoint": endpoint,
        "status": status,
        "error_code": error_code,
    })
    with conn.cursor() as cur:
        _insert_sync_run(
            cur,
            run_id=run_id,
            deployment_id=deployment_id,
            account_identity_fingerprint=account_identity_fingerprint,
            range_from=range_from,
            source_cutoff=source_cutoff,
            endpoint=endpoint,
            terminal_cursor=None,
            last_source_event_id=None,
            page_count=0,
            source_event_count=0,
            canonical_event_count=0,
            late_event_count=0,
            started_at=started_at,
            completed_at=completed_at,
            producer_identity=producer_identity,
            git_revision=git_revision,
            source_fingerprint=failure_fingerprint,
            status=status,
            error_code=error_code,
            evidence={
                "pages_exhausted": False,
                "failure": error_code,
                "source_safety_lag_seconds": int(
                    SOURCE_SAFETY_LAG.total_seconds()
                ),
            },
        )
    conn.commit()
    return OwnerFlowSyncResult(
        status, run_id, source_cutoff, None, 0, 0, 0, 0, error_code
    )


def _latest_canonical_sync(cur: Any, deployment_id: str, identity: str):
    cur.execute(
        """SELECT sync_through
           FROM owner_capital_flow_sync_run_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s AND source=%s
             AND status='CANONICAL'
           ORDER BY completed_at DESC,created_at DESC LIMIT 1""",
        (deployment_id, identity, SOURCE),
    )
    row = cur.fetchone()
    return None if not row else row[0]


def _unresolved_reconciliation_exists(
    cur: Any, deployment_id: str, identity: str,
) -> bool:
    cur.execute(
        """SELECT EXISTS(
             SELECT 1 FROM v_owner_capital_flow_reconciliation_current_v1
             WHERE environment='LIVE' AND deployment_id=%s
               AND account_identity_fingerprint=%s AND source=%s
               AND state='REQUIRED'
           )""",
        (deployment_id, identity, SOURCE),
    )
    return bool(cur.fetchone()[0])


def synchronize_owner_capital_flows(
    conn: Any,
    *,
    exchange_client: Any,
    deployment_id: str,
    observed_at: datetime | None = None,
    producer_identity: str = "automation-runner",
    git_revision: str | None = None,
) -> OwnerFlowSyncResult:
    """Synchronize one fixed, overlap-rescanned Trading Account bill range."""
    deployment = str(deployment_id).lower()
    if deployment not in {"local-live", "vps-live"}:
        raise ValueError("OWNER_FLOW_SYNC_DEPLOYMENT_FENCE")
    now = observed_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("OWNER_FLOW_SYNC_TIMESTAMP_REQUIRED")
    now = now.astimezone(timezone.utc)
    revision = str(git_revision or os.getenv("GIT_SHA", ""))
    if len(revision) != 40 or any(ch not in "0123456789abcdef" for ch in revision):
        raise ValueError("OWNER_FLOW_SYNC_GIT_REVISION_REQUIRED")
    started_at = now
    source_cutoff = now - SOURCE_SAFETY_LAG
    run_id = str(uuid.uuid4())

    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.owner_capital_flow_sync_run_v1')"
        )
        if cur.fetchone()[0] is None:
            return OwnerFlowSyncResult(
                "NO_SYNC", None, source_cutoff, None, 0, 0, 0, 0,
                "OWNER_FLOW_SYNC_SCHEMA_UNAVAILABLE",
            )
        cur.execute(
            """SELECT accepted_at,account_identity_fingerprint
               FROM live_managed_capital_baseline_v1
               WHERE environment='LIVE' AND deployment_id=%s
               ORDER BY accepted_at DESC LIMIT 1""",
            (deployment,),
        )
        baseline_row = cur.fetchone()
        if not baseline_row:
            return OwnerFlowSyncResult(
                "NO_SYNC", None, source_cutoff, None, 0, 0, 0, 0,
                "ACCEPTED_LIVE_BASELINE_UNAVAILABLE",
            )
        baseline_at, expected_identity = baseline_row
        if baseline_at < BASELINE_BOOTSTRAP:
            raise ValueError("OWNER_FLOW_SYNC_BASELINE_BEFORE_APPROVED_BOOTSTRAP")
        if source_cutoff <= baseline_at:
            return OwnerFlowSyncResult(
                "NO_SYNC", None, source_cutoff, None, 0, 0, 0, 0,
                "SOURCE_CUTOFF_NOT_AFTER_BASELINE",
            )
        cur.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))",
            (f"{CONTRACT_VERSION}:{deployment}:{expected_identity}",),
        )
        if not cur.fetchone()[0]:
            return OwnerFlowSyncResult(
                "PARTIAL_SYNC", None, source_cutoff, None, 0, 0, 0, 0,
                "SYNC_ALREADY_RUNNING",
            )
        previous_sync = _latest_canonical_sync(cur, deployment, expected_identity)

    try:
        identity, _diagnostic = exchange_client.get_account_identity(refresh=True)
        if identity.fingerprint != expected_identity:
            raise OwnerFlowSyncError(
                "ACCOUNT_IDENTITY_MISMATCH", "LIVE_ACCOUNT_IDENTITY_MISMATCH"
            )
        range_from = baseline_at
        if previous_sync is not None:
            range_from = max(baseline_at, previous_sync - OVERLAP_RESCAN)
        archive = range_from < now - timedelta(days=7)
        endpoint = ARCHIVE_ENDPOINT if archive else CURRENT_ENDPOINT
        if range_from < now - timedelta(days=90):
            raise OwnerFlowSyncError(
                "SOURCE_UNAVAILABLE", "TRADING_ACCOUNT_BILLS_HISTORY_LIMIT"
            )
        rows, page_count, terminal_cursor = fetch_exhaustive_trading_account_bills(
            exchange_client,
            range_from=range_from,
            source_cutoff=source_cutoff,
            archive=archive,
        )
        relevant_rows = [
            row for row in rows
            if range_from <= _event_at(row.get("ts")) <= source_cutoff
        ]
        source_evidence = [
            {
                key: str(row.get(key) or "")
                for key in ("billId", "type", "subType", "from", "to", "ccy", "balChg", "ts")
            }
            for row in relevant_rows
        ]
        source_fingerprint = _fingerprint({
            "source": SOURCE,
            "range_from": range_from.isoformat(),
            "source_cutoff": source_cutoff.isoformat(),
            "endpoint": endpoint,
            "events": sorted(source_evidence, key=lambda item: item["billId"]),
        })

        flows: list[CanonicalBoundaryFlow] = []
        for row in relevant_rows:
            mapped = classify_trading_account_bill(row)
            if mapped is not None:
                flows.append(mapped)

        late_flows: list[CanonicalBoundaryFlow] = []
        with conn.cursor() as cur:
            for flow in flows:
                cur.execute(
                    """SELECT flow_id FROM owner_capital_flow_v1
                       WHERE environment='LIVE' AND deployment_id=%s
                         AND account_identity_fingerprint=%s AND source=%s
                         AND source_event_identity=%s""",
                    (
                        deployment, expected_identity, SOURCE,
                        flow.source_event_identity,
                    ),
                )
                existed = cur.fetchone() is not None
                if (
                    not existed and previous_sync is not None
                    and flow.event_at < previous_sync
                ):
                    late_flows.append(flow)
                record_owner_capital_flow(
                    cur,
                    environment="LIVE",
                    deployment_id=deployment,
                    account_identity_fingerprint=expected_identity,
                    source_event_identity=flow.source_event_identity,
                    asset=flow.asset,
                    quantity=flow.amount,
                    value_usdc=flow.amount,
                    event_at=flow.event_at,
                    event_type=flow.event_type,
                    source=SOURCE,
                    raw_provenance_reference=flow.raw_provenance_reference,
                    valuation_provenance={
                        "authority": "OKX_TRADING_ACCOUNT_BILL_BALCHG",
                        "arithmetic": "Decimal(abs(balChg))",
                        "asset_scope": "USDC_ONLY",
                    },
                )

            status = (
                "LATE_EVENT_RECONCILIATION_REQUIRED"
                if late_flows or _unresolved_reconciliation_exists(
                    cur, deployment, expected_identity
                )
                else "CANONICAL"
            )
            completed_at = max(datetime.now(timezone.utc), started_at)
            _insert_sync_run(
                cur,
                run_id=run_id,
                deployment_id=deployment,
                account_identity_fingerprint=expected_identity,
                range_from=range_from,
                source_cutoff=source_cutoff,
                endpoint=endpoint,
                terminal_cursor=terminal_cursor,
                last_source_event_id=(
                    None if not relevant_rows
                    else f"OKX:TRADING_BILL:{relevant_rows[-1]['billId']}"
                ),
                page_count=page_count,
                source_event_count=len(relevant_rows),
                canonical_event_count=len(flows),
                late_event_count=len(late_flows),
                started_at=started_at,
                completed_at=completed_at,
                producer_identity=producer_identity,
                git_revision=revision,
                source_fingerprint=source_fingerprint,
                status=status,
                error_code=(
                    "UNRESOLVED_LATE_EVENT" if status != "CANONICAL" else None
                ),
                evidence={
                    "account_identity_match": True,
                    "fixed_source_cutoff": source_cutoff.isoformat(),
                    "overlap_seconds": int(OVERLAP_RESCAN.total_seconds()),
                    "pages_exhausted": True,
                    "deduplication": "billId",
                    "source_safety_lag_seconds": int(
                        SOURCE_SAFETY_LAG.total_seconds()
                    ),
                },
            )
            for flow in late_flows:
                reconciliation_key = _fingerprint({
                    "environment": "LIVE",
                    "deployment_id": deployment,
                    "account_identity_fingerprint": expected_identity,
                    "source": SOURCE,
                    "source_event_identity": flow.source_event_identity,
                })
                cur.execute(
                    """INSERT INTO owner_capital_flow_reconciliation_v1(
                         reconciliation_key,environment,deployment_id,
                         account_identity_fingerprint,source,
                         source_event_identity,event_at,prior_sync_through,
                         affected_from,state,source_run_id,evidence
                       ) VALUES (
                         %s,'LIVE',%s,%s,%s,%s,%s,%s,%s,'REQUIRED',%s,%s::jsonb
                       ) ON CONFLICT(reconciliation_key,state) DO NOTHING""",
                    (
                        reconciliation_key, deployment, expected_identity,
                        SOURCE, flow.source_event_identity, flow.event_at,
                        previous_sync, flow.event_at, run_id,
                        _canonical_json({
                            "reason": "PREVIOUSLY_UNSEEN_EVENT_BEFORE_SYNC_THROUGH",
                            "drawdown_observations_affected_from": flow.event_at.isoformat(),
                        }),
                    ),
                )
        conn.commit()
        return OwnerFlowSyncResult(
            status, run_id, source_cutoff,
            source_cutoff if status == "CANONICAL" else None,
            page_count, len(relevant_rows), len(flows), len(late_flows),
            None if status == "CANONICAL" else "UNRESOLVED_LATE_EVENT",
        )
    except OwnerFlowSyncError as exc:
        range_from = locals().get("range_from", baseline_at)
        endpoint = locals().get("endpoint", CURRENT_ENDPOINT)
        return _persist_failed_sync_run(
            conn,
            run_id=run_id,
            deployment_id=deployment,
            account_identity_fingerprint=expected_identity,
            range_from=range_from,
            source_cutoff=source_cutoff,
            endpoint=endpoint,
            started_at=started_at,
            producer_identity=producer_identity,
            git_revision=revision,
            status=exc.status,
            error_code=exc.code,
        )
    except ValueError as exc:
        if "OWNER_CAPITAL_FLOW_IDEMPOTENCY_CONFLICT" not in str(exc):
            raise
        return _persist_failed_sync_run(
            conn,
            run_id=run_id,
            deployment_id=deployment,
            account_identity_fingerprint=expected_identity,
            range_from=locals().get("range_from", baseline_at),
            source_cutoff=source_cutoff,
            endpoint=locals().get("endpoint", CURRENT_ENDPOINT),
            started_at=started_at,
            producer_identity=producer_identity,
            git_revision=revision,
            status="PARTIAL_SYNC",
            error_code="OWNER_CAPITAL_FLOW_IDEMPOTENCY_CONFLICT",
        )


def record_reconciliation_resolution(
    cur: Any,
    *,
    reconciliation_key: str,
    source_run_id: str,
    evidence: Mapping[str, Any],
) -> int:
    """Append resolution only after the future drawdown consumer re-emits evidence."""
    resolution = dict(evidence or {})
    if not resolution:
        raise ValueError("OWNER_FLOW_RECONCILIATION_EVIDENCE_REQUIRED")
    cur.execute(
        """SELECT environment,deployment_id,account_identity_fingerprint,
                  source,source_event_identity,event_at,prior_sync_through,
                  affected_from
           FROM owner_capital_flow_reconciliation_v1
           WHERE reconciliation_key=%s AND state='REQUIRED'""",
        (reconciliation_key,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("OWNER_FLOW_RECONCILIATION_REQUIRED_NOT_FOUND")
    cur.execute(
        """INSERT INTO owner_capital_flow_reconciliation_v1(
             reconciliation_key,environment,deployment_id,
             account_identity_fingerprint,source,source_event_identity,event_at,
             prior_sync_through,affected_from,state,source_run_id,evidence
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'RESOLVED',%s,%s::jsonb)
           ON CONFLICT(reconciliation_key,state) DO NOTHING
           RETURNING reconciliation_evidence_id""",
        (
            reconciliation_key, *row, source_run_id,
            _canonical_json(resolution),
        ),
    )
    inserted = cur.fetchone()
    if inserted:
        return int(inserted[0])
    cur.execute(
        """SELECT reconciliation_evidence_id,evidence
           FROM owner_capital_flow_reconciliation_v1
           WHERE reconciliation_key=%s AND state='RESOLVED'""",
        (reconciliation_key,),
    )
    existing = cur.fetchone()
    if not existing or existing[1] != resolution:
        raise ValueError("OWNER_FLOW_RECONCILIATION_IDEMPOTENCY_CONFLICT")
    return int(existing[0])


def load_owner_flow_history_authority(
    cur: Any,
    *,
    deployment_id: str,
    account_identity_fingerprint: str,
    as_of: datetime,
) -> OwnerFlowHistoryAuthority:
    """Return drawdown inputs only when the required source covers ``as_of``."""
    cur.execute("SELECT to_regclass('public.owner_capital_flow_sync_run_v1')")
    if cur.fetchone()[0] is None:
        return OwnerFlowHistoryAuthority(None, None, None, "NO_SYNC", None)
    cur.execute(
        """SELECT run_id,sync_through,status
           FROM v_owner_capital_flow_sync_authority_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s AND source=%s""",
        (deployment_id, account_identity_fingerprint, SOURCE),
    )
    row = cur.fetchone()
    if not row:
        return OwnerFlowHistoryAuthority(None, None, None, "NO_SYNC", None)
    run_id, sync_through, status = row
    if status != "CANONICAL":
        return OwnerFlowHistoryAuthority(
            None, None, sync_through, str(status), str(run_id)
        )
    if sync_through is None or sync_through < as_of:
        return OwnerFlowHistoryAuthority(
            None, None, sync_through, "STALE_SYNC", str(run_id)
        )
    if _unresolved_reconciliation_exists(
        cur, deployment_id, account_identity_fingerprint
    ):
        return OwnerFlowHistoryAuthority(
            None, None, sync_through,
            "LATE_EVENT_RECONCILIATION_REQUIRED", str(run_id),
        )
    cur.execute(
        """SELECT accepted_at FROM live_managed_capital_baseline_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s
           ORDER BY accepted_at DESC LIMIT 1""",
        (deployment_id, account_identity_fingerprint),
    )
    baseline = cur.fetchone()
    if not baseline:
        return OwnerFlowHistoryAuthority(
            None, None, sync_through, "NO_SYNC", str(run_id)
        )
    cur.execute(
        """SELECT
             COALESCE(sum(value_usdc) FILTER (
               WHERE event_type='TRANSFER_IN'),0),
             COALESCE(sum(value_usdc) FILTER (
               WHERE event_type='TRANSFER_OUT'),0)
           FROM owner_capital_flow_v1
           WHERE environment='LIVE' AND deployment_id=%s
             AND account_identity_fingerprint=%s AND source=%s
             AND evidence_status='COMPLETE' AND event_at>%s AND event_at<=%s""",
        (
            deployment_id, account_identity_fingerprint, SOURCE,
            baseline[0], as_of,
        ),
    )
    flow_in, flow_out = cur.fetchone()
    return OwnerFlowHistoryAuthority(
        Decimal(str(flow_in)), Decimal(str(flow_out)), sync_through,
        "CANONICAL", str(run_id),
    )


def run_owner_capital_flow_sync_if_due(
    conn: Any,
    *,
    exchange_client: Any,
    trading_mode: str,
    deployment_id: str,
) -> OwnerFlowSyncResult | Mapping[str, Any]:
    """Existing automation-runner hook; explicitly disabled by default."""
    if str(os.getenv("OWNER_CAPITAL_FLOW_SYNC_V1_ENABLED", "0")).lower() not in {
        "1", "true", "yes", "on",
    }:
        return {"status": "DISABLED"}
    if str(trading_mode).upper() != "LIVE":
        return {"status": "ENVIRONMENT_FENCE"}
    interval = int(os.getenv("OWNER_CAPITAL_FLOW_SYNC_V1_INTERVAL_SECONDS", "300"))
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.owner_capital_flow_sync_run_v1')")
        if cur.fetchone()[0] is None:
            return {"status": "SCHEMA_UNAVAILABLE"}
        cur.execute(
            """SELECT completed_at FROM owner_capital_flow_sync_run_v1
               WHERE environment='LIVE' AND deployment_id=%s AND source=%s
               ORDER BY completed_at DESC,created_at DESC LIMIT 1""",
            (deployment_id, SOURCE),
        )
        latest = cur.fetchone()
    now = datetime.now(timezone.utc)
    if latest and (now - latest[0]).total_seconds() < interval:
        return {"status": "NOT_DUE"}
    return synchronize_owner_capital_flows(
        conn,
        exchange_client=exchange_client,
        deployment_id=deployment_id,
        observed_at=now,
        producer_identity=f"automation-runner:{os.getenv('HOSTNAME', 'unknown')}",
        git_revision=os.getenv("GIT_SHA", ""),
    )

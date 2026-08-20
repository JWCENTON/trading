"""Canonical, deterministic and trading-read-only Portfolio State V1."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

from common.live_managed_capital import (
    LiveManagedCapitalEvidence,
    load_live_managed_capital_evidence,
)


PORTFOLIO_STATE_VERSION = "PORTFOLIO_STATE_V1"
PRICE_FRESHNESS = timedelta(minutes=20)
ZERO = Decimal("0")
DEPLOYMENT_MODES = {
    "local-paper": "PAPER",
    "local-live": "LIVE",
    "vps-paper": "PAPER",
    "vps-live": "LIVE",
}


def _decimal(value: object) -> Decimal:
    if value is None:
        raise ValueError("canonical financial value is missing")
    return Decimal(str(value))


@dataclass(frozen=True)
class PortfolioBaseline:
    timestamp: datetime
    managed_equity: Decimal
    unrealized_pnl: Decimal
    source_authority: str
    activation_fingerprint: str


@dataclass(frozen=True)
class RealizedEvidence:
    closed_count: int
    complete_count: int
    net_pnl: Decimal | None
    latest_evidence_at: datetime | None


@dataclass(frozen=True)
class OpenInventoryMark:
    position_id: int
    symbol: str
    strategy: str
    interval: str
    side: str
    entry_price: Decimal | None
    remaining_inventory_qty: Decimal | None
    inventory_evidence_status: str
    mark_price: Decimal | None
    mark_timestamp: datetime | None
    regime: str | None
    regime_timestamp: datetime | None


@dataclass(frozen=True)
class ExposureBucket:
    key: str
    quantity: Decimal | None
    market_value_usdc: Decimal | None
    evidence_status: str


@dataclass(frozen=True)
class PortfolioStateV1:
    portfolio_state_version: str
    environment: str
    deployment_id: str
    as_of: datetime
    runtime_revision: str | None
    capital_scope: str
    total_capital: Decimal | None
    total_capital_status: str
    available_capital: Decimal | None
    available_capital_status: str
    reserved_capital: Decimal | None
    reserved_capital_status: str
    deployed_capital: Decimal | None
    deployed_capital_status: str
    realized_pnl: Decimal | None
    realized_pnl_status: str
    unrealized_pnl: Decimal | None
    unrealized_pnl_status: str
    open_positions_count: int
    open_positions_status: str
    open_exposure_notional: Decimal | None
    open_exposure_status: str
    exposure_by_symbol: tuple[ExposureBucket, ...]
    exposure_by_strategy: tuple[ExposureBucket, ...]
    exposure_by_regime: tuple[ExposureBucket, ...]
    open_risk: Decimal | None
    open_risk_status: str
    portfolio_heat: Decimal | None
    portfolio_heat_status: str
    drawdown: Decimal | None
    drawdown_status: str
    source_timestamps: Mapping[str, datetime | None]
    source_freshness: Mapping[str, str]
    source_authorities: Mapping[str, str]
    incomplete_reasons: tuple[str, ...]

    def serializable(self) -> dict[str, Any]:
        def convert(value: Any) -> Any:
            if isinstance(value, Decimal):
                return format(value, "f")
            if isinstance(value, datetime):
                return value.astimezone(timezone.utc).isoformat()
            if isinstance(value, dict):
                return {key: convert(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [convert(item) for item in value]
            return value

        return convert(asdict(self))


def validate_identity(environment: str, deployment_id: str) -> tuple[str, str]:
    mode = str(environment or "").strip().upper()
    deployment = str(deployment_id or "").strip().lower()
    if deployment not in DEPLOYMENT_MODES or DEPLOYMENT_MODES[deployment] != mode:
        raise ValueError("PORTFOLIO_STATE_ENVIRONMENT_DEPLOYMENT_MISMATCH")
    return mode, deployment


def _mark_status(mark: OpenInventoryMark, *, as_of: datetime) -> str:
    if (
        mark.remaining_inventory_qty is None
        or mark.inventory_evidence_status != "COMPLETE"
    ):
        return "INCOMPLETE"
    if mark.mark_price is None or mark.mark_timestamp is None:
        return "PRICE_UNAVAILABLE"
    if mark.mark_timestamp < as_of - PRICE_FRESHNESS:
        return "PRICE_STALE"
    if mark.entry_price is None:
        return "INCOMPLETE"
    return "CANONICAL"


def _aggregate_exposure(
    marks: Iterable[OpenInventoryMark], *, dimension: str, as_of: datetime
) -> tuple[ExposureBucket, ...]:
    grouped: dict[str, list[OpenInventoryMark]] = {}
    for mark in marks:
        key = str(getattr(mark, dimension) or "UNKNOWN")
        grouped.setdefault(key, []).append(mark)
    result = []
    for key in sorted(grouped):
        rows = grouped[key]
        statuses = [_mark_status(row, as_of=as_of) for row in rows]
        status = next(
            (candidate for candidate in ("INCOMPLETE", "PRICE_UNAVAILABLE", "PRICE_STALE")
             if candidate in statuses),
            "CANONICAL",
        )
        quantities = [row.remaining_inventory_qty for row in rows]
        # Base-asset quantities are additive only inside a symbol bucket.
        # Strategy/regime buckets may contain heterogeneous assets and therefore
        # expose only their common USDC market-value measure.
        quantity = None
        if dimension == "symbol" and not any(value is None for value in quantities):
            quantity = sum((_decimal(value) for value in quantities), ZERO)
        market_value = None
        if status == "CANONICAL":
            market_value = sum(
                (abs(_decimal(row.remaining_inventory_qty) * _decimal(row.mark_price)) for row in rows),
                ZERO,
            )
        result.append(ExposureBucket(key, quantity, market_value, status))
    return tuple(result)


def build_portfolio_state(
    *, environment: str, deployment_id: str, as_of: datetime,
    baseline: PortfolioBaseline | None, realized: RealizedEvidence,
    open_marks: Iterable[OpenInventoryMark],
    historical_peak_managed_equity: Decimal | None,
    runtime_revision: str | None = None,
    live_capital: LiveManagedCapitalEvidence | None = None,
    live_baseline_managed_equity: Decimal | None = None,
    live_baseline_at: datetime | None = None,
) -> PortfolioStateV1:
    mode, deployment = validate_identity(environment, deployment_id)
    if as_of.tzinfo is None:
        raise ValueError("PORTFOLIO_STATE_AS_OF_MUST_BE_TIMEZONE_AWARE")
    marks = tuple(open_marks)
    mark_statuses = tuple(_mark_status(mark, as_of=as_of) for mark in marks)
    aggregate_mark_status = next(
        (candidate for candidate in ("INCOMPLETE", "PRICE_UNAVAILABLE", "PRICE_STALE")
         if candidate in mark_statuses),
        "CANONICAL",
    )

    deployed = None
    unrealized = None
    if aggregate_mark_status == "CANONICAL":
        deployed = sum(
            (abs(_decimal(mark.remaining_inventory_qty) * _decimal(mark.mark_price)) for mark in marks),
            ZERO,
        )
        unrealized = sum(
            (
                (_decimal(mark.entry_price) - _decimal(mark.mark_price))
                * _decimal(mark.remaining_inventory_qty)
                if mark.side.upper() in {"SELL", "SHORT"}
                else (_decimal(mark.mark_price) - _decimal(mark.entry_price))
                * _decimal(mark.remaining_inventory_qty)
                for mark in marks
            ),
            ZERO,
        )

    realized_complete = (
        realized.closed_count == realized.complete_count
        and (realized.net_pnl is not None or realized.closed_count == 0)
    )
    realized_pnl = (
        realized.net_pnl if realized.closed_count else ZERO
    ) if realized_complete else None
    realized_status = "CANONICAL" if realized_complete else "INCOMPLETE"
    if mode == "LIVE" and live_baseline_at is None:
        realized_pnl = None
        realized_status = "INCOMPLETE"

    total = None
    total_status = "NOT_YET_CANONICAL" if mode == "LIVE" else "INCOMPLETE"
    reasons: list[str] = []
    if mode == "PAPER" and baseline is None:
        reasons.append("ACCEPTED_PAPER_BASELINE_UNAVAILABLE")
    elif mode == "PAPER" and realized_status != "CANONICAL":
        reasons.append("POST_BASELINE_FINANCIAL_TRUTH_INCOMPLETE")
    elif mode == "PAPER" and aggregate_mark_status != "CANONICAL":
        total_status = aggregate_mark_status
        reasons.append(f"OPEN_MARK_{aggregate_mark_status}")
    elif mode == "PAPER":
        total = (
            baseline.managed_equity
            + _decimal(realized_pnl)
            + _decimal(unrealized)
            - baseline.unrealized_pnl
        )
        total_status = "CANONICAL"
    elif live_capital is None:
        reasons.append("LIVE_MANAGED_CAPITAL_EVIDENCE_UNAVAILABLE")
    elif live_capital.managed_equity_status == "CANONICAL":
        total = live_capital.managed_equity
        total_status = "CANONICAL"
        reasons.extend(live_capital.incomplete_reasons)
    else:
        total_status = "INCOMPLETE"
        reasons.extend(live_capital.incomplete_reasons)

    drawdown = None
    drawdown_status = "INCOMPLETE"
    if total_status == "CANONICAL":
        drawdown_equity = (
            live_capital.flow_adjusted_equity
            if mode == "LIVE" and live_capital is not None
            else total
        )
        peak = max(
            value for value in (
                (baseline.managed_equity if baseline else live_baseline_managed_equity),
                historical_peak_managed_equity,
                drawdown_equity,
            ) if value is not None
        )
        drawdown = None if peak == ZERO else (drawdown_equity - peak) / peak * Decimal("100")
        drawdown_status = "CANONICAL"

    if mode == "PAPER":
        reasons.append("AVAILABLE_BASELINE_DEPENDS_ON_FORBIDDEN_PRICE_FALLBACK")
    reasons.append("CANONICAL_CAPITAL_RESERVATION_AUTHORITY_UNAVAILABLE")

    available = None
    available_status = "INCOMPLETE"
    reserved = None
    reserved_status = "NOT_YET_CANONICAL"
    if mode == "LIVE" and live_capital is not None:
        available = live_capital.available_capital
        available_status = live_capital.available_capital_status
        reserved = live_capital.reserved_capital
        reserved_status = live_capital.reserved_capital_status

    source_marks = [mark.mark_timestamp for mark in marks if mark.mark_timestamp]
    source_regimes = [mark.regime_timestamp for mark in marks if mark.regime_timestamp]
    by_symbol = _aggregate_exposure(marks, dimension="symbol", as_of=as_of)
    by_strategy = _aggregate_exposure(marks, dimension="strategy", as_of=as_of)
    by_regime = _aggregate_exposure(marks, dimension="regime", as_of=as_of)
    return PortfolioStateV1(
        PORTFOLIO_STATE_VERSION, mode, deployment, as_of, runtime_revision or None,
        "MANAGED_PORTFOLIO_EQUITY", total, total_status,
        available, available_status, reserved, reserved_status,
        deployed, aggregate_mark_status, realized_pnl, realized_status,
        unrealized, aggregate_mark_status, len(marks), "CANONICAL",
        deployed, aggregate_mark_status, by_symbol, by_strategy, by_regime,
        None, "NOT_YET_CANONICAL", None, "NOT_YET_CANONICAL",
        drawdown, drawdown_status,
        {
            "accepted_baseline_at": (
                baseline.timestamp if baseline else live_baseline_at
            ),
            "live_balance_observed_at": (
                live_capital.balance_observed_at if live_capital else None
            ),
            "live_account_mark_oldest_at": (
                live_capital.mark_oldest_at if live_capital else None
            ),
            "financial_truth_latest_evidence_at": realized.latest_evidence_at,
            "mark_price_oldest_at": min(source_marks) if source_marks else None,
            "mark_price_latest_at": max(source_marks) if source_marks else None,
            "regime_latest_at": max(source_regimes) if source_regimes else None,
        },
        {
            "mark_price": aggregate_mark_status,
            "financial_truth": realized_status,
            "accepted_baseline": "CANONICAL" if (
                baseline or live_baseline_managed_equity is not None
            ) else "INCOMPLETE",
        },
        {
            "total_capital": (
                "LIVE_MANAGED_CAPITAL_AUTHORITY_V1_RAW_OKX_BALANCES_MARKED_TO_USDC"
                if mode == "LIVE" else
                "PAPER_EQUITY_BASELINE_V2_PLUS_POST_BASELINE_CANONICAL_FINANCIAL_TRUTH_COMPLETE_PLUS_FRESH_OPEN_MARK"
            ),
            "realized_pnl": "canonical_financial_truth_v1.COMPLETE",
            "inventory_quantity": "positions.remaining_inventory_qty",
            "mark_price": "candles.close/FRESH_20_MINUTES",
            "regime": "market_regime/CANONICAL_REGIME_ATTRIBUTION_V1",
            "drawdown": (
                "LIVE_MANAGED_CAPITAL_AUTHORITY_V1_FLOW_ADJUSTED_EQUITY"
                if mode == "LIVE" else
                "PAPER_EQUITY_BASELINE_V2_ALIGNED_MANAGED_EQUITY"
            ),
            "account_reporting_excluded": "RECONSTRUCTED_PARTIAL_MIXED",
        },
        tuple(dict.fromkeys(reasons)),
    )


def read_portfolio_state(
    cur: Any, *, environment: str, deployment_id: str, as_of: datetime,
    runtime_revision: str | None = None,
    exchange_client: Any | None = None,
) -> PortfolioStateV1:
    mode, deployment = validate_identity(environment, deployment_id)
    baseline = None
    if mode == "PAPER":
        cur.execute(
            """
            SELECT baseline_timestamp,baseline_managed_equity,
                   baseline_unrealized_pnl,source_authority,
                   activation_fingerprint
            FROM paper_equity_baseline_v2
            WHERE deployment_id=%s AND baseline_version='PAPER_EQUITY_BASELINE_V2'
              AND evidence_status='COMPLETE' AND baseline_timestamp<=%s
            """,
            (deployment, as_of),
        )
        row = cur.fetchone()
        if row:
            baseline = PortfolioBaseline(
                row[0], _decimal(row[1]), _decimal(row[2]), str(row[3]), str(row[4])
            )

    live_capital = None
    live_baseline = None
    live_peak = None
    if mode == "LIVE" and exchange_client is not None:
        live_capital, live_baseline, live_peak, _live_context = load_live_managed_capital_evidence(
            cur, exchange_client=exchange_client, deployment_id=deployment,
            as_of=as_of,
        )
    boundary = (
        baseline.timestamp if baseline else
        live_baseline.accepted_at if live_baseline else as_of
    )
    cur.execute(
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE ft.financial_truth_status='COMPLETE'
                 AND ft.authoritative_net_pnl IS NOT NULL),
               SUM(ft.authoritative_net_pnl) FILTER (
                 WHERE ft.financial_truth_status='COMPLETE'),
               MAX(ft.evidence_observed_at) FILTER (
                 WHERE ft.financial_truth_status='COMPLETE')
        FROM positions p
        LEFT JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id
        WHERE p.status='CLOSED' AND p.exit_time>%s AND p.exit_time<=%s
        """,
        (boundary, as_of),
    )
    row = cur.fetchone()
    realized = RealizedEvidence(int(row[0]), int(row[1]), row[2], row[3])

    cur.execute(
        """
        SELECT p.id,p.symbol,p.strategy,p.interval,p.side,p.entry_price,
               p.remaining_inventory_qty,p.inventory_evidence_status,
               mark.close,mark.open_time,regime.regime,regime.ts
        FROM positions p
        LEFT JOIN LATERAL (
          SELECT c.close,c.open_time FROM candles c
          WHERE c.symbol=p.symbol AND c.interval=p.interval
            AND c.open_time<=%s
          ORDER BY c.open_time DESC LIMIT 1
        ) mark ON TRUE
        LEFT JOIN LATERAL (
          SELECT r.regime,r.ts FROM market_regime r
          WHERE r.symbol=p.symbol AND r.interval=p.interval AND r.ts<=%s
            AND r.regime IS NOT NULL
          ORDER BY r.ts DESC LIMIT 1
        ) regime ON TRUE
        WHERE p.status='OPEN'
        ORDER BY p.id
        """,
        (as_of, as_of),
    )
    marks = tuple(
        OpenInventoryMark(
            int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
            None if row[5] is None else _decimal(row[5]),
            None if row[6] is None else _decimal(row[6]), str(row[7] or "INCOMPLETE"),
            None if row[8] is None else _decimal(row[8]), row[9],
            None if row[10] is None else str(row[10]), row[11],
        )
        for row in cur.fetchall()
    )

    peak = live_peak
    if baseline is not None:
        cur.execute(
            """
            SELECT MAX(waltrade_managed_equity_usdc)
            FROM equity_daily_snapshot_v1
            WHERE deployment_id=%s AND evidence_status='COMPLETE'
              AND source_timestamp>=%s AND source_timestamp<=%s
            """,
            (deployment, baseline.timestamp, as_of),
        )
        peak_row = cur.fetchone()
        peak = None if not peak_row or peak_row[0] is None else _decimal(peak_row[0])
    return build_portfolio_state(
        environment=mode, deployment_id=deployment, as_of=as_of,
        baseline=baseline, realized=realized, open_marks=marks,
        historical_peak_managed_equity=peak,
        runtime_revision=runtime_revision,
        live_capital=live_capital,
        live_baseline_managed_equity=(
            live_baseline.baseline_managed_equity if live_baseline else None
        ),
        live_baseline_at=(live_baseline.accepted_at if live_baseline else None),
    )

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Iterable, Mapping

from common.closed_outcome_read_model import fetch_closed_outcome_summary
from common.paper_account_read_model import reconstruct_paper_account


ZERO = Decimal("0")
TRACKED_BASE_ASSETS = ("BTC", "ETH", "BNB", "SOL")


def _decimal(value: object) -> Decimal:
    return Decimal(str(value or 0))


@dataclass(frozen=True)
class EquityObservation:
    account_total_value_usdc: Decimal
    external_manual_value_usdc: Decimal | None
    waltrade_managed_equity_usdc: Decimal | None
    available_usdc: Decimal
    bot_inventory_value_usdc: Decimal
    realized_net_pnl_usdc: Decimal | None
    unrealized_pnl_usdc: Decimal | None
    fees_usdc: Decimal | None
    open_positions: int
    evidence_status: str
    source_timestamp: datetime


@dataclass(frozen=True)
class EquityPoint:
    snapshot_date: date
    account_total_value_usdc: Decimal
    external_manual_value_usdc: Decimal | None
    waltrade_managed_equity_usdc: Decimal | None
    evidence_status: str


def subtract_external_manual(
    account_total: Decimal,
    external_manual: Decimal | None,
    *,
    ownership_complete: bool,
) -> tuple[Decimal | None, str]:
    if not ownership_complete or external_manual is None:
        return None, "INCOMPLETE"
    return account_total - external_manual, "COMPLETE"


def _change(current: Decimal | None, baseline: Decimal | None) -> dict[str, Decimal | None]:
    if current is None or baseline is None:
        return {"abs": None, "pct": None}
    delta = current - baseline
    pct = None if baseline == ZERO else delta / baseline * Decimal("100")
    return {"abs": delta, "pct": pct}


def calculate_equity_metrics(points: Iterable[EquityPoint]) -> dict[str, Any]:
    ordered = sorted(points, key=lambda point: point.snapshot_date)
    if not ordered:
        return {
            "current_waltrade_equity": None,
            "current_account_total": None,
            "change_7d_abs": None,
            "change_7d_pct": None,
            "change_30d_abs": None,
            "change_30d_pct": None,
            "month_open_equity": None,
            "month_change_abs": None,
            "month_change_pct": None,
            "peak_equity": None,
            "drawdown_from_peak_pct": None,
        }

    latest = ordered[-1]
    current = latest.waltrade_managed_equity_usdc

    def baseline(days: int) -> Decimal | None:
        cutoff = latest.snapshot_date - timedelta(days=days)
        candidates = [
            point.waltrade_managed_equity_usdc
            for point in ordered
            if point.snapshot_date == cutoff
            and point.waltrade_managed_equity_usdc is not None
        ]
        return candidates[-1] if candidates else None

    month_values = [
        point.waltrade_managed_equity_usdc
        for point in ordered
        if point.snapshot_date.year == latest.snapshot_date.year
        and point.snapshot_date.month == latest.snapshot_date.month
        and point.waltrade_managed_equity_usdc is not None
    ]
    month_open = month_values[0] if month_values else None
    seven = _change(current, baseline(7))
    thirty = _change(current, baseline(30))
    month = _change(current, month_open)
    complete_values = [
        point.waltrade_managed_equity_usdc
        for point in ordered
        if point.waltrade_managed_equity_usdc is not None
    ]
    peak = max(complete_values) if complete_values else None
    drawdown = (
        None if current is None or peak in (None, ZERO)
        else (current - peak) / peak * Decimal("100")
    )
    return {
        "current_waltrade_equity": current,
        "current_account_total": latest.account_total_value_usdc,
        "change_7d_abs": seven["abs"],
        "change_7d_pct": seven["pct"],
        "change_30d_abs": thirty["abs"],
        "change_30d_pct": thirty["pct"],
        "month_open_equity": month_open,
        "month_change_abs": month["abs"],
        "month_change_pct": month["pct"],
        "peak_equity": peak,
        "drawdown_from_peak_pct": drawdown,
    }


def _exchange_balances(client: Any, quote_asset: str) -> tuple[dict[str, Decimal], dict[str, Decimal], datetime]:
    observed_at = datetime.now(timezone.utc)
    account = client.get_account()
    tracked = set(TRACKED_BASE_ASSETS) | {quote_asset}
    quantities = {asset: ZERO for asset in tracked}
    for row in account.get("balances", []):
        asset = str(row.get("asset") or "").upper()
        if asset in quantities:
            quantities[asset] = _decimal(row.get("free")) + _decimal(row.get("locked"))
    prices = {quote_asset: Decimal("1")}
    for asset in TRACKED_BASE_ASSETS:
        if quantities[asset] > ZERO:
            ticker = client.get_symbol_ticker(symbol=f"{asset}{quote_asset}")
            prices[asset] = _decimal(ticker["price"])
        else:
            prices[asset] = ZERO
    return quantities, prices, observed_at


def _ownership_projection(cur: Any, quantities: Mapping[str, Decimal], prices: Mapping[str, Decimal], quote_asset: str) -> tuple[Decimal | None, Decimal, bool]:
    cur.execute(
        """
        SELECT symbol,
               COALESCE(SUM(remaining_inventory_qty), 0),
               bool_and(inventory_evidence_status='COMPLETE')
        FROM positions
        WHERE COALESCE(remaining_inventory_qty, 0) > 0
        GROUP BY symbol
        """
    )
    bot_qty: dict[str, Decimal] = {}
    inventory_complete = True
    for symbol, qty, complete in cur.fetchall():
        asset = str(symbol).upper().removesuffix(quote_asset)
        bot_qty[asset] = bot_qty.get(asset, ZERO) + _decimal(qty)
        inventory_complete = inventory_complete and bool(complete)

    cur.execute(
        """
        SELECT symbol, side, authoritative_payload
        FROM exchange_fill_ingestion_state_v2
        WHERE ownership_classification='MANUAL_OR_EXTERNAL'
          AND application_status='EXTERNAL_OR_MANUAL_UNLINKED'
        """
    )
    external_qty: dict[str, Decimal] = {}
    external_complete = True
    for symbol, side, payload in cur.fetchall():
        payload = payload or {}
        required = ("executed_qty", "fee_quantity", "fee_currency")
        if any(payload.get(key) in (None, "") for key in required):
            external_complete = False
            continue
        asset = str(symbol).upper().removesuffix(quote_asset)
        qty = _decimal(payload["executed_qty"])
        fee = _decimal(payload["fee_quantity"])
        if str(payload["fee_currency"]).upper() != asset:
            fee = ZERO
        delta = qty - fee if str(side).upper() == "BUY" else -(qty + fee)
        external_qty[asset] = external_qty.get(asset, ZERO) + delta

    cur.execute(
        """
        SELECT DISTINCT ON (symbol) symbol, step_size, min_qty, min_notional
        FROM financial_truth_instrument_snapshot_v1
        ORDER BY symbol, captured_at DESC, id DESC
        """
    )
    limits = {
        str(symbol).upper().removesuffix(quote_asset): (
            _decimal(step), _decimal(min_qty), _decimal(min_notional)
        )
        for symbol, step, min_qty, min_notional in cur.fetchall()
    }
    external_value = ZERO
    bot_value = ZERO
    complete = inventory_complete and external_complete
    for asset in TRACKED_BASE_ASSETS:
        bot = bot_qty.get(asset, ZERO)
        external = external_qty.get(asset, ZERO)
        step, min_qty, min_notional = limits.get(
            asset,
            (Decimal("0.000000000001"), ZERO, ZERO),
        )
        if external < -step:
            complete = False
        unexplained = quantities.get(asset, ZERO) - bot - max(external, ZERO)
        if unexplained < -step:
            complete = False
        price = prices.get(asset, ZERO)
        positive_unexplained = max(unexplained, ZERO)
        executable = (
            (positive_unexplained / step).to_integral_value(
                rounding=ROUND_DOWN
            ) * step
            if step > ZERO else positive_unexplained
        )
        is_dust = (
            executable <= ZERO
            or (min_qty > ZERO and executable < min_qty)
            or (
                min_notional > ZERO
                and price > ZERO
                and executable * price < min_notional
            )
        )
        if positive_unexplained > step and not is_dust:
            complete = False
        bot_value += bot * price
        external_value += max(external, ZERO) * price
    return (external_value if complete else None), bot_value, complete


def collect_current_equity(
    cur: Any,
    *,
    trading_mode: str,
    exchange_client: Any,
    quote_asset: str = "USDC",
    paper_start_usdc: Decimal = Decimal("1000"),
) -> EquityObservation:
    mode = trading_mode.upper()
    window_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
    observed_at = datetime.now(timezone.utc)
    stats = fetch_closed_outcome_summary(
        cur,
        environment=mode,
        window_start=window_start,
        window_end=observed_at,
        include_administrative_retirements=True,
    )
    cur.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(
          CASE WHEN UPPER(COALESCE(p.side,'LONG')) IN ('SELL','SHORT')
            THEN (p.entry_price-COALESCE(c.close,p.entry_price))*p.qty
            ELSE (COALESCE(c.close,p.entry_price)-p.entry_price)*p.qty END
        ),0), COALESCE(SUM(COALESCE(p.remaining_inventory_qty,p.qty)*COALESCE(c.close,p.entry_price)),0)
        FROM positions p
        LEFT JOIN LATERAL (
          SELECT close FROM candles c WHERE c.symbol=p.symbol AND c.interval=p.interval
          ORDER BY c.open_time DESC LIMIT 1
        ) c ON TRUE
        WHERE p.status='OPEN'
        """
    )
    open_positions, unrealized, paper_inventory_value = cur.fetchone()
    unrealized_decimal = _decimal(unrealized)

    if mode == "PAPER":
        bridge = reconstruct_paper_account(
            initial_equity=paper_start_usdc,
            realized_net_pnl=stats["net_pnl"],
            unrealized_pnl=unrealized_decimal,
            resolved_count=stats["resolved_trades"],
            closed_count=stats["trades"],
            source_breakdown=stats["outcome_source_counts"],
            high_assurance_count=stats["high_assurance_count"],
            legacy_compatible_count=stats["legacy_compatible_count"],
            quality_breakdown=stats["quality_breakdown"],
            external_adjustments=ZERO,
        )
        total = bridge.account_value
        complete = (
            total is not None
            and bridge.resolved_outcome_count == bridge.closed_positions_count
        )
        managed, evidence = subtract_external_manual(
            total or ZERO, ZERO, ownership_complete=complete
        )
        inventory_value = _decimal(paper_inventory_value)
        return EquityObservation(
            total or ZERO, ZERO if complete else None, managed,
            (total or ZERO) - inventory_value, inventory_value,
            stats["net_pnl"], unrealized_decimal, stats["fees"],
            int(open_positions), evidence, observed_at,
        )

    quantities, prices, observed_at = _exchange_balances(exchange_client, quote_asset)
    total = sum((quantities[asset] * prices[asset] for asset in quantities), ZERO)
    external, bot_inventory, ownership_complete = _ownership_projection(
        cur, quantities, prices, quote_asset
    )
    managed, evidence = subtract_external_manual(
        total, external, ownership_complete=ownership_complete
    )
    return EquityObservation(
        total, external, managed, quantities.get(quote_asset, ZERO),
        bot_inventory, stats["net_pnl"], unrealized_decimal, stats["fees"],
        int(open_positions), evidence, observed_at,
    )


def upsert_daily_snapshot(cur: Any, *, deployment_id: str, trading_mode: str, observation: EquityObservation, snapshot_date: date | None = None) -> int:
    day = snapshot_date or observation.source_timestamp.date()
    cur.execute(
        """
        INSERT INTO equity_daily_snapshot_v1 (
          snapshot_date,deployment_id,trading_mode,account_total_value_usdc,
          external_manual_value_usdc,waltrade_managed_equity_usdc,
          available_usdc,bot_inventory_value_usdc,realized_net_pnl_usdc,
          unrealized_pnl_usdc,fees_usdc,open_positions,evidence_status,
          source_timestamp
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (deployment_id,snapshot_date) DO UPDATE SET
          trading_mode=EXCLUDED.trading_mode,
          account_total_value_usdc=EXCLUDED.account_total_value_usdc,
          external_manual_value_usdc=EXCLUDED.external_manual_value_usdc,
          waltrade_managed_equity_usdc=EXCLUDED.waltrade_managed_equity_usdc,
          available_usdc=EXCLUDED.available_usdc,
          bot_inventory_value_usdc=EXCLUDED.bot_inventory_value_usdc,
          realized_net_pnl_usdc=EXCLUDED.realized_net_pnl_usdc,
          unrealized_pnl_usdc=EXCLUDED.unrealized_pnl_usdc,
          fees_usdc=EXCLUDED.fees_usdc,open_positions=EXCLUDED.open_positions,
          evidence_status=EXCLUDED.evidence_status,
          source_timestamp=EXCLUDED.source_timestamp
        RETURNING id
        """,
        (
            day, deployment_id, trading_mode.upper(),
            observation.account_total_value_usdc,
            observation.external_manual_value_usdc,
            observation.waltrade_managed_equity_usdc,
            observation.available_usdc, observation.bot_inventory_value_usdc,
            observation.realized_net_pnl_usdc, observation.unrealized_pnl_usdc,
            observation.fees_usdc, observation.open_positions,
            observation.evidence_status, observation.source_timestamp,
        ),
    )
    return int(cur.fetchone()[0])


def fetch_equity_history(cur: Any, *, deployment_id: str, days: int | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: list[Any] = [deployment_id]
    day_filter = ""
    if days is not None:
        day_filter = " AND snapshot_date >= (CURRENT_DATE - %s::integer)"
        params.append(days)
    cur.execute(
        """
        SELECT snapshot_date,account_total_value_usdc,
               external_manual_value_usdc,waltrade_managed_equity_usdc,
               available_usdc,bot_inventory_value_usdc,realized_net_pnl_usdc,
               unrealized_pnl_usdc,fees_usdc,open_positions,evidence_status,
               source_timestamp
        FROM equity_daily_snapshot_v1
        WHERE deployment_id=%s
        """ + day_filter + " ORDER BY snapshot_date",
        tuple(params),
    )
    keys = (
        "snapshot_date", "account_total_value_usdc", "external_manual_value_usdc",
        "waltrade_managed_equity_usdc", "available_usdc",
        "bot_inventory_value_usdc", "realized_net_pnl_usdc",
        "unrealized_pnl_usdc", "fees_usdc", "open_positions",
        "evidence_status", "source_timestamp",
    )
    items = [dict(zip(keys, row)) for row in cur.fetchall()]
    points = [
        EquityPoint(
            row["snapshot_date"], row["account_total_value_usdc"],
            row["external_manual_value_usdc"],
            row["waltrade_managed_equity_usdc"], row["evidence_status"],
        )
        for row in items
    ]
    return items, calculate_equity_metrics(points)

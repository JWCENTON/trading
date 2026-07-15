from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Iterable, Mapping, Any


class ExitQuantityStatus(str, Enum):
    NO_FILL = "NO_FILL"
    PARTIALLY_REDUCED = "PARTIALLY_REDUCED"
    FULLY_CLOSED = "FULLY_CLOSED"
    ALREADY_APPLIED = "ALREADY_APPLIED"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class ExitQuantityMutation:
    status: ExitQuantityStatus
    position_id: int
    applied_qty: Decimal
    remaining_qty: Decimal


def _decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def apply_confirmed_exit_quantity(
    connection_factory,
    *,
    position_id: int,
    exchange_source: str,
    symbol: str,
    strategy: str,
    interval: str,
    side: str,
    requested_qty,
    evidences: Iterable[Mapping[str, Any]],
    exit_price,
    exit_reason: str,
    close_tolerance: Decimal = Decimal("0.001"),
) -> ExitQuantityMutation:
    """Apply cumulative per-order fill evidence exactly once.

    Each evidence item is ``{order_id, client_order_id, cumulative_qty}``.
    ``binance_orders.reconciled_executed_qty`` is the durable high-water mark.
    Order rows and the position row are locked and updated in one transaction.
    Locks are always acquired order-first, matching the fill reconciler.
    """
    normalized = []
    for evidence in evidences:
        order_id = str(evidence.get("order_id") or "").strip()
        cumulative = _decimal(evidence.get("cumulative_qty"))
        if not order_id or cumulative <= 0:
            continue
        normalized.append((order_id, evidence.get("client_order_id"), cumulative))
    if not normalized:
        return ExitQuantityMutation(
            ExitQuantityStatus.NO_FILL, int(position_id), Decimal("0"), Decimal("0")
        )
    normalized.sort(key=lambda item: item[0])

    conn = connection_factory()
    cur = conn.cursor()
    try:
        total_delta = Decimal("0")

        for order_id, client_order_id, cumulative in normalized:
            cur.execute(
                """
                INSERT INTO binance_orders (
                    symbol, side, order_type, client_order_id, order_id,
                    status, raw, position_id, is_exit, strategy, interval,
                    order_purpose, requested_qty, order_accepted, exchange_source
                ) VALUES (%s,%s,'MARKET',%s,%s,'PARTIALLY_FILLED','{}'::jsonb,
                          %s,true,%s,%s,'EXIT',%s,true,%s)
                ON CONFLICT (exchange_source, symbol, order_id) DO NOTHING
                """,
                (
                    str(symbol), str(side).upper(), client_order_id, order_id,
                    int(position_id), str(strategy).upper(), str(interval),
                    _decimal(requested_qty), str(exchange_source).lower(),
                ),
            )
            cur.execute(
                """
                SELECT id, position_id, order_purpose,
                       COALESCE(reconciled_executed_qty, 0)
                FROM binance_orders
                WHERE exchange_source=%s AND symbol=%s AND order_id=%s
                FOR UPDATE
                """,
                (str(exchange_source).lower(), str(symbol), order_id),
            )
            row = cur.fetchone()
            if not row or int(row[1] or 0) != int(position_id) or row[2] != "EXIT":
                raise RuntimeError("exit order evidence conflicts with position linkage")
            order_row_id, _, _, already = row
            delta = max(Decimal("0"), cumulative - _decimal(already))
            total_delta += delta
            cur.execute(
                """
                UPDATE binance_orders
                SET reconciled_executed_qty=%s, reconciled_position_id=%s,
                    reconciled_at=now(), reconciliation_status='RECONCILED',
                    last_reconciliation_action=%s, unreconciled_qty=0,
                    reconciliation_error=NULL
                WHERE id=%s
                """,
                (
                    max(cumulative, _decimal(already)), int(position_id),
                    "EXIT_FILL_APPLIED" if delta > 0 else "EXIT_DUPLICATE_NOOP",
                    int(order_row_id),
                ),
            )

        cur.execute(
            "SELECT qty FROM positions WHERE id=%s AND status='OPEN' FOR UPDATE",
            (int(position_id),),
        )
        position_row = cur.fetchone()
        if not position_row:
            conn.rollback()
            return ExitQuantityMutation(
                ExitQuantityStatus.CONFLICT, int(position_id), Decimal("0"), Decimal("0")
            )
        current_qty = _decimal(position_row[0])

        applied = min(total_delta, current_qty)
        remaining = max(Decimal("0"), current_qty - applied)
        tolerance = max(Decimal("1e-12"), current_qty * close_tolerance)
        closed = remaining <= tolerance
        stored_qty = Decimal("0") if closed else remaining
        if applied > 0:
            cur.execute(
                """
                UPDATE positions
                SET qty=%s,
                    status=CASE WHEN %s THEN 'CLOSED' ELSE 'OPEN' END,
                    exit_price=CASE WHEN %s THEN %s ELSE exit_price END,
                    exit_time=CASE WHEN %s THEN now() ELSE exit_time END,
                    exit_reason=CASE WHEN %s THEN %s ELSE exit_reason END
                WHERE id=%s AND status='OPEN'
                """,
                (
                    stored_qty, closed, closed, float(exit_price), closed,
                    closed, str(exit_reason), int(position_id),
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("position changed during exit quantity mutation")
        conn.commit()
        status = (
            ExitQuantityStatus.FULLY_CLOSED if closed and applied > 0
            else ExitQuantityStatus.PARTIALLY_REDUCED if applied > 0
            else ExitQuantityStatus.ALREADY_APPLIED
        )
        return ExitQuantityMutation(status, int(position_id), applied, stored_qty)
    except BaseException:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def exit_fill_evidences(result: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    explicit = result.get("fill_evidence")
    nested = result.get("resp")
    if not explicit and isinstance(nested, Mapping):
        explicit = nested.get("fill_evidence")
    if explicit:
        return tuple(dict(item) for item in explicit)
    order_id = result.get("order_id")
    qty = _decimal(result.get("executed_qty"))
    if not order_id or qty <= 0:
        return ()
    return ({
        "order_id": str(order_id),
        "client_order_id": result.get("client_order_id"),
        "cumulative_qty": qty,
    },)


def apply_partial_exit_result(
    connection_factory,
    *,
    result: dict[str, Any],
    position_id: int,
    exchange_source: str,
    symbol: str,
    strategy: str,
    interval: str,
    side: str,
    exit_price,
    exit_reason: str,
) -> ExitQuantityMutation | None:
    if not result.get("executed") or result.get("fully_executed"):
        return None
    evidences = exit_fill_evidences(result)
    if not evidences:
        raise RuntimeError("confirmed partial exit lacks durable order identity")
    mutation = apply_confirmed_exit_quantity(
        connection_factory,
        position_id=position_id,
        exchange_source=exchange_source,
        symbol=symbol,
        strategy=strategy,
        interval=interval,
        side=side,
        requested_qty=result.get("requested_qty"),
        evidences=evidences,
        exit_price=exit_price,
        exit_reason=exit_reason,
    )
    result["position_quantity_status"] = mutation.status.value
    result["position_qty_applied"] = float(mutation.applied_qty)
    result["position_remaining_qty"] = float(mutation.remaining_qty)
    # Legacy callers must not interpret a partial fill as permission to close.
    result["live_ok"] = False
    return mutation

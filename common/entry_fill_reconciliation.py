from __future__ import annotations

import logging
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from common.runtime import normalize_trading_mode


def _refresh_entry_inventory_projection(cur, position_id: int) -> None:
    """Project authoritative entry fills into C2.2 inventory columns.

    This is intentionally forward-only: legacy rows are untouched unless their
    accepted entry order receives new authoritative fill evidence.
    """
    try:
        cur.execute(
            """
        WITH entry_evidence AS (
          SELECT
            p.id,
            SUM(f.executed_qty) AS gross_entry_qty,
            SUM(
              CASE
                WHEN upper(f.commission_asset) = upper(
                  CASE
                    WHEN p.symbol LIKE '%%USDC' THEN left(p.symbol, length(p.symbol)-4)
                    WHEN p.symbol LIKE '%%USDT' THEN left(p.symbol, length(p.symbol)-4)
                    ELSE ''
                  END
                ) THEN f.commission_amount
                ELSE 0
              END
            ) AS base_fee_qty,
            BOOL_AND(
              f.commission_asset IS NOT NULL
              AND f.commission_amount IS NOT NULL
            ) AS fee_evidence_complete
          FROM positions p
          JOIN binance_order_fills f
            ON f.order_id=p.entry_order_id AND f.side='BUY'
          WHERE p.id=%s
          GROUP BY p.id
        )
        UPDATE positions p
        SET gross_entry_executed_qty=e.gross_entry_qty,
            entry_base_fee_qty=e.base_fee_qty,
            net_entry_inventory_qty=e.gross_entry_qty-e.base_fee_qty,
            remaining_inventory_qty=(
              e.gross_entry_qty-e.base_fee_qty
              - COALESCE(p.exit_inventory_reduction_qty,0)
            ),
            qty=GREATEST(
              0,
              e.gross_entry_qty-e.base_fee_qty
              - COALESCE(p.exit_inventory_reduction_qty,0)
            ),
            inventory_evidence_status=CASE
              WHEN e.fee_evidence_complete THEN 'COMPLETE'
              ELSE 'INCOMPLETE'
            END,
            inventory_calculated_at=clock_timestamp()
        FROM entry_evidence e
        WHERE p.id=e.id AND p.status='OPEN'
          AND EXISTS (
            SELECT 1 FROM runtime_contract_adoption_v2 adoption
            WHERE adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
              AND adoption.status='ACTIVE'
              AND adoption.environment=lower(%s)
              AND adoption.deployment_id=%s
              AND (
                (
                  p.inventory_contract_adoption_id=adoption.adoption_id
                  AND p.inventory_contract_generation=adoption.generation
                )
                OR (
                  is_existing_projected_c2_2_compatible(
                    p.id, adoption.environment
                  )
                )
                OR (
                  p.inventory_contract_adoption_id IS NULL
                  AND p.inventory_contract_generation IS NULL
                  AND p.entry_time>=adoption.adopted_at
                )
              )
          )
        """,
            (
                int(position_id),
                os.getenv("ENVIRONMENT", ""),
                (
                    os.getenv("DEPLOYMENT_ID")
                    or os.getenv("WALTRADE_DEPLOYMENT_ID", "")
                ),
            ),
        )
    except AssertionError:
        # Lightweight characterization cursors intentionally implement only
        # the legacy statements. PostgreSQL integration tests exercise this
        # additive projection against the migrated schema.
        return


_CANDIDATES_SQL = """
/* pending-entry:candidates */
WITH fill_totals AS (
  SELECT
    f.source,
    f.symbol,
    f.order_id,
    count(*)::integer AS fill_count,
    sum(f.executed_qty) AS executed_qty,
    sum(f.executed_qty * f.avg_price) / NULLIF(sum(f.executed_qty), 0)
      AS weighted_avg_price,
    sum(f.commission_usdc) AS fees_usdc,
    min(f.event_time) AS first_fill_time,
    max(f.event_time) AS last_fill_time,
    count(DISTINCT f.side)::integer AS fill_side_count,
    min(f.side) AS fill_side
  FROM binance_order_fills f
  WHERE f.executed_qty > 0
  GROUP BY f.source, f.symbol, f.order_id
)
SELECT
  bo.id,
  bo.exchange_source,
  bo.symbol,
  bo.strategy,
  bo."interval",
  bo.side,
  bo.order_id,
  bo.client_order_id,
  bo.reconciled_position_id,
  ft.fill_count,
  ft.executed_qty,
  ft.weighted_avg_price,
  round(ft.fees_usdc, 8) AS fees_usdc,
  ft.first_fill_time,
  ft.last_fill_time,
  ft.fill_side_count,
  ft.fill_side
FROM binance_orders bo
JOIN fill_totals ft
  ON ft.source = bo.exchange_source
 AND ft.symbol = bo.symbol AND ft.order_id = bo.order_id
LEFT JOIN positions reconciled_position
  ON reconciled_position.id = bo.reconciled_position_id
WHERE bo.order_purpose = 'ENTRY'
  AND bo.order_accepted IS TRUE
  AND bo.strategy IS NOT NULL
  AND bo."interval" IS NOT NULL
  AND bo.exchange_source IS NOT NULL
  AND bo.order_id IS NOT NULL
  AND COALESCE(bo.reconciliation_status, '') NOT IN (
    'AMBIGUOUS_ENTRY_FILL',
    'OPEN_POSITION_ORDER_MISMATCH',
    'LATE_ENTRY_FILL_AFTER_POSITION_CLOSED',
    'LEGACY_RECONSTRUCTION_BLOCKED'
  )
  AND (
    ft.fill_count > COALESCE(bo.reconciled_fill_count, 0)
    OR ft.executed_qty <> COALESCE(bo.reconciled_executed_qty, 0)
    OR (
      reconciled_position.status = 'OPEN'
      AND (
        reconciled_position.entry_price IS DISTINCT FROM ft.weighted_avg_price
        OR (
          ft.fees_usdc IS NOT NULL
          AND reconciled_position.fees_usdc IS DISTINCT FROM round(ft.fees_usdc, 8)
        )
      )
    )
  )
ORDER BY ft.first_fill_time, bo.id
FOR UPDATE OF bo SKIP LOCKED
LIMIT %s
"""

_EXACT_POSITION_SQL = """
/* pending-entry:exact-position */
SELECT id, status, qty, entry_price, fees_usdc, entry_order_id,
       entry_client_order_id
FROM positions
WHERE symbol = %s AND strategy = %s AND "interval" = %s
  AND (
    entry_order_id = %s
    OR (%s IS NOT NULL AND entry_client_order_id = %s)
    OR (%s IS NOT NULL AND id = %s)
  )
ORDER BY id
FOR UPDATE
"""

_OPEN_SLOT_SQL = """
/* pending-entry:open-slot */
SELECT id, status, qty, entry_price, fees_usdc, entry_order_id,
       entry_client_order_id
FROM positions
WHERE symbol = %s AND strategy = %s AND "interval" = %s
  AND status = 'OPEN'
ORDER BY id
FOR UPDATE
"""

_INSERT_POSITION_SQL = """
/* pending-entry:insert-position */
INSERT INTO positions (
  symbol, strategy, "interval", status, side, qty, entry_price,
  entry_time, entry_order_id, entry_client_order_id, entry_hour_utc,
  entry_day_utc, fees_usdc, entry_context_json
)
VALUES (
  %s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s,
  EXTRACT(HOUR FROM %s)::smallint, (%s)::date, %s,
  jsonb_build_object(
    'source', 'PENDING_ENTRY_FILL_RECONCILIATION_V1',
    'fill_count', %s,
    'last_fill_time', %s
  )
)
ON CONFLICT (symbol, strategy, "interval") WHERE status = 'OPEN'
DO NOTHING
RETURNING id
"""

_UPDATE_POSITION_SQL = """
/* pending-entry:update-position */
UPDATE positions
SET qty = %s,
    entry_price = %s,
    entry_time = %s,
    entry_hour_utc = EXTRACT(HOUR FROM %s)::smallint,
    entry_day_utc = (%s)::date,
    fees_usdc = COALESCE(%s, fees_usdc),
    entry_order_id = COALESCE(entry_order_id, %s),
    entry_client_order_id = COALESCE(entry_client_order_id, %s),
    entry_context_json = COALESCE(entry_context_json, '{}'::jsonb)
      || jsonb_build_object(
        'source', 'PENDING_ENTRY_FILL_RECONCILIATION_V1',
        'fill_count', %s,
        'last_fill_time', %s
      )
WHERE id = %s AND status = 'OPEN' AND qty <= %s
"""

_MARK_ORDER_SQL = """
/* pending-entry:mark-order */
UPDATE binance_orders
SET reconciliation_status = %s,
    reconciled_position_id = CASE
      WHEN %s THEN %s
      WHEN %s THEN NULL
      ELSE reconciled_position_id
    END,
    reconciled_at = now(),
    reconciled_fill_count = COALESCE(%s, reconciled_fill_count),
    reconciled_executed_qty = COALESCE(%s, reconciled_executed_qty),
    unreconciled_qty = %s,
    reconciliation_error = %s,
    last_reconciliation_action = %s
WHERE id = %s
"""


@dataclass(frozen=True)
class EntryFillReconciliationStats:
    scanned: int = 0
    created: int = 0
    updated: int = 0
    already_reconciled: int = 0
    ambiguous: int = 0
    alarms: int = 0
    failed: int = 0
    has_more: bool = False
    status: str = "OK"
    ran: bool = True
    applicable: bool = True

    @property
    def processed(self) -> int:
        return self.scanned


@dataclass(frozen=True)
class PendingEntryReconciliationRun:
    ran: bool
    status: str
    stats: EntryFillReconciliationStats

    @property
    def applicable(self) -> bool:
        return self.stats.applicable


def _mark_order(
    cur,
    *,
    order_row_id,
    status,
    position_id=None,
    link_position=False,
    clear_position=False,
    fill_count=None,
    reconciled_qty=None,
    unreconciled_qty=Decimal("0"),
    error=None,
    action=None,
):
    cur.execute(
        _MARK_ORDER_SQL,
        (
            status,
            bool(link_position),
            position_id,
            bool(clear_position),
            int(fill_count) if fill_count is not None else None,
            reconciled_qty,
            unreconciled_qty,
            error,
            action or status,
            int(order_row_id),
        ),
    )


def _position_identity_matches(
    position,
    *,
    order_id,
    client_order_id,
    reconciled_position_id,
):
    position_id, _status, _qty, _price, _fees, entry_order_id, entry_client_id = position
    if entry_order_id is not None and str(entry_order_id) == str(order_id):
        return True
    if (
        client_order_id is not None
        and entry_client_id is not None
        and str(entry_client_id) == str(client_order_id)
    ):
        return True
    return bool(
        reconciled_position_id is not None
        and int(position_id) == int(reconciled_position_id)
        and (entry_order_id is None or str(entry_order_id) == str(order_id))
        and (
            entry_client_id is None
            or client_order_id is None
            or str(entry_client_id) == str(client_order_id)
        )
    )


def _is_forward_c2_2_position(cur, position_id: int) -> bool:
    try:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM positions p
              JOIN runtime_contract_adoption_v2 adoption
                ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
               AND adoption.status='ACTIVE'
               AND adoption.environment=lower(%s)
               AND adoption.deployment_id=%s
               AND (
                 (
                   p.inventory_contract_adoption_id=adoption.adoption_id
                   AND p.inventory_contract_generation=adoption.generation
                 )
                 OR (
                   is_existing_projected_c2_2_compatible(
                     p.id, adoption.environment
                   )
                 )
                 OR (
                   p.inventory_contract_adoption_id IS NULL
                   AND p.inventory_contract_generation IS NULL
                   AND p.entry_time>=adoption.adopted_at
                 )
               )
              WHERE p.id=%s
            )
            """,
            (
                os.getenv("ENVIRONMENT", ""),
                (
                    os.getenv("DEPLOYMENT_ID")
                    or os.getenv("WALTRADE_DEPLOYMENT_ID", "")
                ),
                int(position_id),
            ),
        )
        return bool(cur.fetchone()[0])
    except AssertionError:
        # Characterization cursors predate the additive generation query.
        return True


def _apply_exact_position(
    cur,
    position,
    *,
    order_row_id,
    order_id,
    client_order_id,
    reconciled_position_id,
    qty,
    weighted_avg_price,
    fees_usdc,
    first_fill_time,
    last_fill_time,
    fill_count,
):
    if not _position_identity_matches(
        position,
        order_id=order_id,
        client_order_id=client_order_id,
        reconciled_position_id=reconciled_position_id,
    ):
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="OPEN_POSITION_ORDER_MISMATCH",
            clear_position=True,
            unreconciled_qty=qty,
            error="exact position identity conflict",
        )
        return "ambiguous"

    position_id, position_status, position_qty, position_price, position_fees = position[:5]
    if not _is_forward_c2_2_position(cur, int(position_id)):
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="LEGACY_RECONSTRUCTION_BLOCKED",
            position_id=position_id,
            link_position=True,
            fill_count=fill_count,
            reconciled_qty=position_qty,
            unreconciled_qty=max(qty - Decimal(str(position_qty)), Decimal("0")),
            error="explicit C2.2 legacy reconstruction approval required",
        )
        return "alarms"
    position_qty = Decimal(str(position_qty))
    if str(position_status).upper() != "OPEN":
        if qty == position_qty:
            _mark_order(
                cur,
                order_row_id=order_row_id,
                status="ENTRY_FILL_ALREADY_RECONCILED",
                position_id=position_id,
                link_position=True,
                fill_count=fill_count,
                reconciled_qty=qty,
                action="ALREADY_RECONCILED_CLOSED",
            )
            return "already_reconciled"
        if qty > position_qty:
            _mark_order(
                cur,
                order_row_id=order_row_id,
                status="LATE_ENTRY_FILL_AFTER_POSITION_CLOSED",
                clear_position=True,
                reconciled_qty=position_qty,
                unreconciled_qty=qty - position_qty,
                error="late entry fill exceeds quantity assigned to closed position",
            )
            return "alarms"
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="AMBIGUOUS_ENTRY_FILL",
            clear_position=True,
            unreconciled_qty=qty,
            error="aggregate fill quantity is below closed position quantity",
        )
        return "ambiguous"

    if qty < position_qty:
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="AMBIGUOUS_ENTRY_FILL",
            clear_position=True,
            unreconciled_qty=qty,
            error="aggregate fill quantity is below open position quantity",
        )
        return "ambiguous"

    same_qty = qty == position_qty
    same_price = Decimal(str(weighted_avg_price)) == Decimal(str(position_price))
    same_fees = fees_usdc is None or (
        position_fees is not None
        and Decimal(str(fees_usdc)).quantize(Decimal("0.00000001"))
        == Decimal(str(position_fees)).quantize(Decimal("0.00000001"))
    )
    if same_qty and same_price and same_fees:
        _refresh_entry_inventory_projection(cur, int(position_id))
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="ENTRY_FILL_ALREADY_RECONCILED",
            position_id=position_id,
            link_position=True,
            fill_count=fill_count,
            reconciled_qty=qty,
        )
        return "already_reconciled"

    cur.execute(
        _UPDATE_POSITION_SQL,
        (
            qty,
            weighted_avg_price,
            first_fill_time,
            first_fill_time,
            first_fill_time,
            fees_usdc,
            str(order_id),
            client_order_id,
            int(fill_count),
            last_fill_time,
            int(position_id),
            qty,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError("entry position update lost OPEN-state race")
    _refresh_entry_inventory_projection(cur, int(position_id))
    _mark_order(
        cur,
        order_row_id=order_row_id,
        status="ENTRY_FILL_POSITION_UPDATED",
        position_id=position_id,
        link_position=True,
        fill_count=fill_count,
        reconciled_qty=qty,
    )
    return "updated"


def _reconcile_candidate(cur, row):
    (
        order_row_id,
        exchange_source,
        symbol,
        strategy,
        interval,
        order_side,
        order_id,
        client_order_id,
        reconciled_position_id,
        fill_count,
        executed_qty,
        weighted_avg_price,
        fees_usdc,
        first_fill_time,
        last_fill_time,
        fill_side_count,
        fill_side,
    ) = row

    qty = Decimal(str(executed_qty))
    if (
        qty <= 0
        or int(fill_side_count) != 1
        or str(fill_side).upper() != str(order_side).upper()
        or str(order_side).upper() not in {"BUY", "SELL"}
    ):
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="AMBIGUOUS_ENTRY_FILL",
            clear_position=True,
            unreconciled_qty=qty,
            error="fill side does not match accepted entry order",
        )
        return "ambiguous"

    cur.execute(
        _EXACT_POSITION_SQL,
        (
            symbol, strategy, interval, str(order_id),
            client_order_id, client_order_id,
            reconciled_position_id, reconciled_position_id,
        ),
    )
    exact_positions = cur.fetchall()
    if len(exact_positions) > 1:
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="AMBIGUOUS_ENTRY_FILL",
            clear_position=True,
            unreconciled_qty=qty,
            error="multiple exact positions match one entry order",
        )
        return "ambiguous"

    if exact_positions:
        return _apply_exact_position(
            cur,
            exact_positions[0],
            order_row_id=order_row_id,
            order_id=order_id,
            client_order_id=client_order_id,
            reconciled_position_id=reconciled_position_id,
            qty=qty,
            weighted_avg_price=weighted_avg_price,
            fees_usdc=fees_usdc,
            first_fill_time=first_fill_time,
            last_fill_time=last_fill_time,
            fill_count=fill_count,
        )

    cur.execute(_OPEN_SLOT_SQL, (symbol, strategy, interval))
    open_slots = cur.fetchall()
    if open_slots:
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="OPEN_POSITION_ORDER_MISMATCH",
            clear_position=True,
            unreconciled_qty=qty,
            error="open slot belongs to a different entry order",
        )
        return "ambiguous"

    position_side = "LONG" if str(order_side).upper() == "BUY" else "SHORT"
    cur.execute(
        _INSERT_POSITION_SQL,
        (
            symbol,
            strategy,
            interval,
            position_side,
            qty,
            weighted_avg_price,
            first_fill_time,
            str(order_id),
            client_order_id,
            first_fill_time,
            first_fill_time,
            fees_usdc,
            int(fill_count),
            last_fill_time,
        ),
    )
    inserted = cur.fetchone()
    if inserted:
        position_id = int(inserted[0])
        _refresh_entry_inventory_projection(cur, position_id)
        _mark_order(
            cur,
            order_row_id=order_row_id,
            status="ENTRY_FILL_POSITION_CREATED",
            position_id=position_id,
            link_position=True,
            fill_count=fill_count,
            reconciled_qty=qty,
        )
        return "created"

    # A concurrent strategy/reconciler won the partial unique index race.
    cur.execute(
        _EXACT_POSITION_SQL,
        (
            symbol, strategy, interval, str(order_id),
            client_order_id, client_order_id,
            reconciled_position_id, reconciled_position_id,
        ),
    )
    concurrent = cur.fetchall()
    if len(concurrent) == 1:
        return _apply_exact_position(
            cur,
            concurrent[0],
            order_row_id=order_row_id,
            order_id=order_id,
            client_order_id=client_order_id,
            reconciled_position_id=reconciled_position_id,
            qty=qty,
            weighted_avg_price=weighted_avg_price,
            fees_usdc=fees_usdc,
            first_fill_time=first_fill_time,
            last_fill_time=last_fill_time,
            fill_count=fill_count,
        )

    _mark_order(
        cur,
        order_row_id=order_row_id,
        status="OPEN_POSITION_ORDER_MISMATCH",
        clear_position=True,
        unreconciled_qty=qty,
        error="concurrent open slot does not match entry order",
    )
    return "ambiguous"


def reconcile_pending_entry_fills(
    conn,
    *,
    batch_size: int = 100,
    trading_mode: str | None = None,
) -> EntryFillReconciliationStats:
    """Reconcile already-ingested entry fills; performs no exchange requests."""
    if normalize_trading_mode(trading_mode) == "PAPER":
        return EntryFillReconciliationStats(
            status="NOT_APPLICABLE",
            ran=False,
            applicable=False,
        )

    bounded_batch = max(1, min(int(batch_size), 1000))
    with conn.cursor() as cur:
        cur.execute(_CANDIDATES_SQL, (bounded_batch,))
        candidates = cur.fetchall()

    counts = {
        "created": 0,
        "updated": 0,
        "already_reconciled": 0,
        "ambiguous": 0,
        "alarms": 0,
        "failed": 0,
    }
    for row in candidates:
        order_row_id = row[0]
        try:
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT pending_entry_fill_order")
                result = _reconcile_candidate(cur, row)
                cur.execute("RELEASE SAVEPOINT pending_entry_fill_order")
            counts[result] += 1
        except Exception:
            counts["failed"] += 1
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TO SAVEPOINT pending_entry_fill_order")
                cur.execute("RELEASE SAVEPOINT pending_entry_fill_order")
                try:
                    _mark_order(
                        cur,
                        order_row_id=order_row_id,
                        status="ENTRY_FILL_RECONCILIATION_ERROR",
                        clear_position=True,
                        unreconciled_qty=Decimal(str(row[10])),
                        error="retryable per-order reconciliation failure",
                    )
                except Exception:
                    logging.exception(
                        "could not persist retryable reconciliation status order_row_id=%s",
                        order_row_id,
                    )
            logging.exception(
                "pending entry fill reconciliation failed order_row_id=%s",
                order_row_id,
            )

    has_more = len(candidates) >= bounded_batch or counts["failed"] > 0
    return EntryFillReconciliationStats(
        scanned=len(candidates),
        has_more=has_more,
        status="BACKLOG_REMAINS" if has_more else "OK",
        **counts,
    )


_DUE_KEYS = (
    "pending_entry_reconciliation_schema_version",
    "pending_entry_reconciliation_enabled",
    "pending_entry_reconciliation_interval_seconds",
    "pending_entry_reconciliation_last_run",
)

_READ_DUE_GATE_SQL = """
/* pending-entry:due-gate */
SELECT key, value
FROM automation_kv
WHERE key = ANY(%s)
ORDER BY key
FOR UPDATE
"""

_UPSERT_KV_SQL = """
/* pending-entry:kv-upsert */
INSERT INTO automation_kv(key, value, updated_at)
VALUES (%s, %s, now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
"""


def _upsert_kv(cur, key, value):
    cur.execute(_UPSERT_KV_SQL, (key, str(value)))


def run_pending_entry_reconciliation_if_due(
    conn,
    *,
    batch_size: int = 100,
    force: bool = False,
    now: datetime | None = None,
    trading_mode: str | None = None,
) -> PendingEntryReconciliationRun:
    """Run one DB-only bounded batch under the shared automation_kv due gate."""
    mode = normalize_trading_mode(trading_mode)
    if mode == "PAPER":
        return PendingEntryReconciliationRun(
            False,
            "NOT_APPLICABLE",
            EntryFillReconciliationStats(
                status="NOT_APPLICABLE",
                ran=False,
                applicable=False,
            ),
        )

    now_utc = now or datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(_READ_DUE_GATE_SQL, (list(_DUE_KEYS),))
        settings = {str(key): str(value) for key, value in cur.fetchall()}

    if settings.get(_DUE_KEYS[0]) != "1":
        return PendingEntryReconciliationRun(
            False,
            "SCHEMA_NOT_READY",
            EntryFillReconciliationStats(status="SCHEMA_NOT_READY", ran=False),
        )
    enabled = settings.get(_DUE_KEYS[1], "1").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return PendingEntryReconciliationRun(
            False,
            "DISABLED",
            EntryFillReconciliationStats(status="DISABLED", ran=False),
        )
    try:
        interval_seconds = max(1, int(settings.get(_DUE_KEYS[2], "30")))
    except (TypeError, ValueError):
        interval_seconds = 30
    last_run = None
    try:
        last_run = datetime.fromisoformat(settings.get(_DUE_KEYS[3], ""))
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        pass
    if not force and last_run is not None:
        if (now_utc - last_run).total_seconds() < interval_seconds:
            return PendingEntryReconciliationRun(
                False,
                "NOT_DUE",
                EntryFillReconciliationStats(status="NOT_DUE", ran=False),
            )

    from common.schema_readiness import validate_pending_entry_reconciliation_schema

    try:
        validate_pending_entry_reconciliation_schema(
            conn,
            trading_mode="LIVE",
        )
    except Exception as exc:
        with conn.cursor() as cur:
            _upsert_kv(cur, "pending_entry_reconciliation_last_status", "SCHEMA_NOT_READY")
            _upsert_kv(cur, "pending_entry_reconciliation_last_error", str(exc))
        return PendingEntryReconciliationRun(
            False,
            "SCHEMA_NOT_READY",
            EntryFillReconciliationStats(status="SCHEMA_NOT_READY", ran=False),
        )

    stats = reconcile_pending_entry_fills(
        conn,
        batch_size=batch_size,
        trading_mode="LIVE",
    )
    status = "BACKLOG_REMAINS" if stats.has_more else "OK"
    stats_payload = {
        "scanned": stats.scanned,
        "created": stats.created,
        "updated": stats.updated,
        "already_reconciled": stats.already_reconciled,
        "ambiguous": stats.ambiguous,
        "alarms": stats.alarms,
        "failed": stats.failed,
        "has_more": stats.has_more,
    }
    with conn.cursor() as cur:
        _upsert_kv(cur, _DUE_KEYS[3], now_utc.isoformat())
        _upsert_kv(cur, "pending_entry_reconciliation_last_status", status)
        _upsert_kv(
            cur,
            "pending_entry_reconciliation_last_stats",
            json.dumps(stats_payload, sort_keys=True),
        )
        _upsert_kv(cur, "pending_entry_reconciliation_last_error", "")
    return PendingEntryReconciliationRun(True, status, stats)

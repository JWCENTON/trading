from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
import os
import uuid

import psycopg2

from common.contract_adoption import (
    contract_adoption_compatible,
    log_runtime_revision_provenance_diagnostic,
    require_runtime_git_revision,
)
from common.financial_truth_identity import IDENTITY_VERSION
from common.inventory_lifecycle import apply_inventory_lifecycle_mutation
from common.inventory_quantity import (
    InstrumentExecutionLimits,
    project_inventory_from_execution_evidence,
)
from common.simulated_order_namespace import (
    ADMINISTRATIVE_ORDER_CLASS,
    FORWARD_ORDER_CLASS,
    detect_simulated_order_namespace,
)


SIMULATION_MODEL_VERSION = "PAPER_SIMULATOR_FINANCIAL_MODEL_V1"
SIMULATION_FEE_RATE = Decimal("0.0004")
SIMULATED_IDENTITY_VERSION = "SIMULATED_ACCOUNT_IDENTITY_V1"
INSTRUMENT_METADATA_VERSION = "EXECUTION_INSTRUMENT_SNAPSHOT_V1"


@dataclass(frozen=True)
class SimulatedOrderWriteBlocked:
    status: str
    existing_order_id: int | None = None

    def __bool__(self) -> bool:
        return False


def simulated_order_write_status(value) -> str:
    if isinstance(value, SimulatedOrderWriteBlocked):
        return value.status
    if not value:
        return "DB_GUARD_DUPLICATE"
    return "INSERTED"


@dataclass(frozen=True)
class PaperExitPreflightResult:
    """Read-only decision for a PAPER exit before execution evidence exists."""

    allowed: bool
    reason_code: str
    position_id: int | None
    position_status: str | None
    position_adoption_id: int | None
    position_generation: int | None
    active_adoption_id: int | None
    active_generation: int | None
    legacy_compatibility: bool
    active_adoption_git_revision: str | None = None
    runtime_git_revision: str | None = None
    runtime_revision_matches_adoption_provenance: bool | None = None
    detail: str | None = None

    def event_fields(self) -> dict:
        return asdict(self)


def _paper_exit_result(
    *,
    allowed: bool,
    reason_code: str,
    position=None,
    active=None,
    legacy_compatibility: bool = False,
    runtime_git_revision: str | None = None,
    runtime_revision_matches_adoption_provenance: bool | None = None,
    detail: str | None = None,
) -> PaperExitPreflightResult:
    position = position or (None, None, None, None, None)
    active = active or (None, None, None, None)
    return PaperExitPreflightResult(
        allowed=bool(allowed),
        reason_code=str(reason_code),
        position_id=int(position[0]) if position[0] is not None else None,
        position_status=str(position[1]) if position[1] is not None else None,
        position_adoption_id=(
            int(position[2]) if position[2] is not None else None
        ),
        position_generation=(
            int(position[3]) if position[3] is not None else None
        ),
        active_adoption_id=int(active[0]) if active[0] is not None else None,
        active_generation=int(active[1]) if active[1] is not None else None,
        legacy_compatibility=bool(legacy_compatibility),
        active_adoption_git_revision=(
            str(active[3]) if active[3] is not None else None
        ),
        runtime_git_revision=runtime_git_revision,
        runtime_revision_matches_adoption_provenance=(
            runtime_revision_matches_adoption_provenance
        ),
        detail=detail,
    )


def paper_exit_preflight_cursor(
    cur,
    *,
    deployment_id: str,
    symbol: str,
    strategy: str,
    interval: str,
    position_id: int | None = None,
) -> PaperExitPreflightResult:
    """Classify one PAPER position without creating durable state."""
    revision = require_runtime_git_revision()
    scope = f"paper-exit|{deployment_id}|{symbol}|{strategy}|{interval}"
    # The transaction-scoped lock serializes patched workers for one strategy slot.
    # It disappears on rollback and does not mutate application data.
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (scope,))
    cur.fetchone()

    if position_id is None:
        cur.execute(
            """
            SELECT id,status,inventory_contract_adoption_id,
                   inventory_contract_generation,entry_time
            FROM positions
            WHERE symbol=%s AND strategy=%s AND interval=%s
            ORDER BY CASE WHEN status='OPEN' THEN 0 ELSE 1 END,
                     entry_time DESC NULLS LAST,id DESC
            LIMIT 1
            """,
            (str(symbol), str(strategy), str(interval)),
        )
    else:
        cur.execute(
            """
            SELECT id,status,inventory_contract_adoption_id,
                   inventory_contract_generation,entry_time
            FROM positions
            WHERE id=%s AND symbol=%s AND strategy=%s AND interval=%s
            """,
            (int(position_id), str(symbol), str(strategy), str(interval)),
        )
    position = cur.fetchone()
    if position is None:
        return _paper_exit_result(
            allowed=False, reason_code="POSITION_NOT_FOUND"
        )
    if str(position[1]).upper() != "OPEN":
        return _paper_exit_result(
            allowed=False,
            reason_code="POSITION_ALREADY_CLOSED",
            position=position,
        )

    cur.execute(
        """
        SELECT adoption_id,contract_name,environment,deployment_id,generation,
               status,adopted_at,git_revision
        FROM runtime_contract_adoption_v2
        WHERE contract_name='FEE_AWARE_INVENTORY_C2_2'
          AND environment='paper'
          AND deployment_id=%s
          AND status='ACTIVE'
        """,
        (str(deployment_id),),
    )
    active_row = cur.fetchone()
    active = None
    if active_row is not None and contract_adoption_compatible(
        contract_name=active_row[1],
        environment=active_row[2],
        deployment_id=active_row[3],
        status=active_row[5],
        generation=active_row[4],
        expected_environment="paper",
        expected_deployment_id=str(deployment_id),
    ):
        active = (
            active_row[0], active_row[4], active_row[6], active_row[7]
        )
    if (
        active is None
        or active[0] is None
        or active[1] is None
        or active[2] is None
    ):
        return _paper_exit_result(
            allowed=False,
            reason_code="INVENTORY_CONTRACT_INCOMPLETE",
            position=position,
            active=active,
            runtime_git_revision=revision,
        )

    revision_matches = log_runtime_revision_provenance_diagnostic(
        adoption_id=int(active[0]),
        generation=int(active[1]),
        adoption_git_revision=str(active[3]),
        runtime_git_revision=revision,
    )

    cur.execute(
        "SELECT is_existing_projected_c2_2_compatible(%s,'paper')",
        (int(position[0]),),
    )
    compatibility_row = cur.fetchone()
    compatible = bool(compatibility_row and compatibility_row[0])
    adoption_id, generation, entry_time = position[2], position[3], position[4]
    active_adoption_id, active_generation, adopted_at = active[:3]

    if (
        adoption_id == active_adoption_id
        and generation == active_generation
    ):
        return _paper_exit_result(
            allowed=True, reason_code="ACTIVE_GENERATION_MATCH",
            position=position, active=active,
            runtime_git_revision=revision,
            runtime_revision_matches_adoption_provenance=revision_matches,
        )
    if compatible:
        return _paper_exit_result(
            allowed=True, reason_code="LEGACY_COMPATIBLE",
            position=position, active=active, legacy_compatibility=True,
            runtime_git_revision=revision,
            runtime_revision_matches_adoption_provenance=revision_matches,
        )
    if adoption_id is None and generation is not None:
        reason = "MISSING_ADOPTION_ID"
    elif adoption_id is not None and generation is None:
        reason = "MISSING_GENERATION"
    elif adoption_id is None and generation is None:
        if entry_time is None:
            reason = "LEGACY_NOT_COMPATIBLE"
        elif entry_time < adopted_at:
            reason = "ENTRY_BEFORE_ACTIVE_ADOPTION"
        else:
            # Preserve the existing guard's forward-position compatibility rule.
            return _paper_exit_result(
                allowed=True, reason_code="FORWARD_ENTRY_COMPATIBLE",
                position=position, active=active,
                runtime_git_revision=revision,
                runtime_revision_matches_adoption_provenance=revision_matches,
            )
    elif (
        adoption_id != active_adoption_id
        or generation != active_generation
    ):
        reason = "GENERATION_MISMATCH"
    else:
        reason = "MUTATION_NOT_ALLOWED_OTHER"
    return _paper_exit_result(
        allowed=False, reason_code=reason, position=position, active=active,
        runtime_git_revision=revision,
        runtime_revision_matches_adoption_provenance=revision_matches,
    )


class PaperExitPreflightGuard:
    """Hold the slot lock until the existing PAPER execution path completes."""

    def __init__(self, connection_factory, **kwargs):
        self._connection_factory = connection_factory
        self._kwargs = kwargs
        self._conn = None
        self._cur = None
        self.result = _paper_exit_result(
            allowed=False, reason_code="INVENTORY_CONTRACT_INCOMPLETE"
        )

    def __enter__(self) -> PaperExitPreflightResult:
        try:
            self._conn = self._connection_factory()
            self._cur = self._conn.cursor()
            self.result = paper_exit_preflight_cursor(
                self._cur, **self._kwargs
            )
        except Exception as exc:
            self.result = _paper_exit_result(
                allowed=False,
                reason_code="INVENTORY_CONTRACT_INCOMPLETE",
                detail=f"{type(exc).__name__}:{exc}",
            )
        return self.result

    def __exit__(self, _exc_type, _exc, _tb):
        if self._conn is not None:
            try:
                self._conn.rollback()
            finally:
                if self._cur is not None:
                    self._cur.close()
                self._conn.close()
        return False


def paper_exit_preflight_guard(connection_factory, **kwargs):
    return PaperExitPreflightGuard(connection_factory, **kwargs)


def execute_paper_exit_after_preflight(
    connection_factory,
    *,
    deployment_id: str,
    symbol: str,
    strategy: str,
    interval: str,
    exit_trigger: str,
    decision: str,
    price: float,
    candle_open_time,
    emit_event,
    action,
):
    """Run an existing PAPER exit path only while its preflight lease is held."""
    with paper_exit_preflight_guard(
        connection_factory,
        deployment_id=deployment_id,
        symbol=symbol,
        strategy=strategy,
        interval=interval,
    ) as result:
        if result.allowed:
            return action(result)
        info = {
            **result.event_fields(),
            "symbol": str(symbol),
            "interval": str(interval),
            "strategy": str(strategy),
            "exit_trigger": str(exit_trigger),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        emit_event(
            event_type="PAPER_EXIT_PREFLIGHT_BLOCKED",
            decision=decision,
            reason=result.reason_code,
            price=float(price),
            candle_open_time=candle_open_time,
            info=info,
        )
        return {
            "ledger_ok": False,
            "live_attempted": False,
            "order_accepted": False,
            "live_ok": False,
            "paper_executed": False,
            "blocked_reason": "PAPER_EXIT_PREFLIGHT_BLOCKED",
            "preflight_reason_code": result.reason_code,
            "position_id": result.position_id,
            "client_order_id": None,
            "resp": {"paper_exit_preflight": info},
        }


def paper_position_mutation_allowed_cursor(
    cur, *, position_id: int, deployment_id: str
) -> bool:
    try:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM positions p
              JOIN runtime_contract_adoption_v2 adoption
                ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
               AND adoption.environment='paper'
               AND adoption.deployment_id=%s
               AND adoption.status='ACTIVE'
              WHERE p.id=%s
                AND (
                  (
                    p.inventory_contract_adoption_id=adoption.adoption_id
                    AND p.inventory_contract_generation=adoption.generation
                  )
                  OR (
                    is_existing_projected_c2_2_compatible(p.id,'paper')
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
                str(deployment_id),
                int(position_id),
            ),
        )
        return bool(cur.fetchone()[0])
    except AssertionError:
        # Existing characterization cursors do not model the additive schema.
        return True


def _hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _assets(symbol: str) -> tuple[str, str]:
    symbol = str(symbol).upper()
    for quote in ("USDC", "USDT", "USD", "EUR"):
        if symbol.endswith(quote):
            return symbol[:-len(quote)], quote
    raise ValueError("unsupported quote asset")


def lock_simulated_exit_slot_cursor(
    cur, *, symbol: str, interval: str, strategy: str
) -> None:
    """Serialize canonical PAPER exit intent/fill activity for one slot.

    The lock is transaction-scoped.  It is shared by normal forward PAPER
    writers and the bounded legacy-retirement writer, avoiding a broad table
    lock while making an exact-slot snapshot stable during re-plan and CAS.
    """
    identity = (
        "PAPER_SIMULATED_EXIT_SLOT_V1:"
        f"{str(symbol).upper()}:{str(interval)}:{str(strategy)}"
    )
    cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
        (identity,),
    )


def create_simulated_order_cursor(
    cur,
    *,
    symbol: str,
    interval: str,
    strategy: str,
    side: str,
    price: Decimal,
    quantity: Decimal,
    reason: str,
    candle_open_time,
    is_exit: bool,
    rsi_14: Decimal | None = None,
    ema_21: Decimal | None = None,
    order_class: str = FORWARD_ORDER_CLASS,
    position_id: int | None = None,
    environment: str | None = None,
    deployment_id: str | None = None,
) -> int | SimulatedOrderWriteBlocked:
    """Canonical transaction-bound simulated-order writer.

    The caller owns the transaction.  Expected unique conflicts are contained
    by a savepoint and classified by reading the exact namespace identity.
    """
    order_class = str(order_class).upper()
    if order_class not in {FORWARD_ORDER_CLASS, ADMINISTRATIVE_ORDER_CLASS}:
        raise ValueError(f"unsupported simulated order class: {order_class}")
    if order_class == FORWARD_ORDER_CLASS:
        if any(value is not None for value in (position_id, environment, deployment_id)):
            raise ValueError("FORWARD_ORDER_IDENTITY_MUST_BE_NULL")
    else:
        if (
            position_id is None
            or not str(environment or "").strip()
            or not str(deployment_id or "").strip()
            or not is_exit
            or str(side).upper() != "SELL"
            or str(reason) != ADMINISTRATIVE_ORDER_CLASS
        ):
            raise ValueError("ADMINISTRATIVE_ORDER_IDENTITY_INVALID")

    namespace = detect_simulated_order_namespace(cur.connection)
    if not (namespace.is_legacy or namespace.is_namespace_v1):
        raise RuntimeError(
            "SIMULATED_ORDER_NAMESPACE_SCHEMA_INVALID:"
            + ",".join(namespace.issues)
        )
    if order_class == ADMINISTRATIVE_ORDER_CLASS and not namespace.is_namespace_v1:
        raise RuntimeError("SIMULATED_ORDER_NAMESPACE_MIGRATION_REQUIRED")

    if is_exit:
        lock_simulated_exit_slot_cursor(
            cur, symbol=symbol, interval=interval, strategy=strategy,
        )
    params = (
        str(symbol), str(interval), str(strategy), str(side).upper(),
        Decimal(str(price)), Decimal(str(quantity)), str(reason),
        None if rsi_14 is None else Decimal(str(rsi_14)),
        None if ema_21 is None else Decimal(str(ema_21)),
        candle_open_time, bool(is_exit),
    )
    if namespace.is_namespace_v1:
        insert_sql = """
            INSERT INTO simulated_orders (
              symbol,interval,strategy,side,price,quantity_btc,reason,
              rsi_14,ema_21,candle_open_time,is_exit,
              order_class,position_id,environment,deployment_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """
        insert_params = params + (
            order_class,
            None if position_id is None else int(position_id),
            None if environment is None else str(environment),
            None if deployment_id is None else str(deployment_id),
        )
    else:
        insert_sql = """
            INSERT INTO simulated_orders (
              symbol,interval,strategy,side,price,quantity_btc,reason,
              rsi_14,ema_21,candle_open_time,is_exit
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """
        insert_params = params

    savepoint = "simulated_order_namespace_v1_insert"
    cur.execute(f"SAVEPOINT {savepoint}")
    try:
        cur.execute(insert_sql, insert_params)
        inserted = cur.fetchone()
    except psycopg2.errors.UniqueViolation as exc:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        if namespace.is_legacy:
            expected_constraints = {
                "sim_orders_uniq_candle_exit",
                "ux_sim_orders_one_per_candle",
                "ux_sim_orders_one_per_candle_isexit",
            }
        elif order_class == FORWARD_ORDER_CLASS:
            expected_constraints = {"ux_sim_orders_forward_one_per_candle"}
        else:
            expected_constraints = {"ux_sim_orders_admin_position"}
        if getattr(exc.diag, "constraint_name", None) not in expected_constraints:
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            raise
        if order_class == FORWARD_ORDER_CLASS:
            class_filter = (
                " AND order_class='FORWARD'" if namespace.is_namespace_v1 else ""
            )
            cur.execute(
                """
                SELECT id,side,price,quantity_btc,reason,rsi_14,ema_21,is_exit
                FROM simulated_orders
                WHERE symbol=%s AND interval=%s AND strategy=%s
                  AND candle_open_time=%s
                """ + class_filter + " ORDER BY id LIMIT 2",
                (str(symbol), str(interval), str(strategy), candle_open_time),
            )
            existing = cur.fetchall()
            conflict_status = "PAPER_ORDER_SLOT_ALREADY_OCCUPIED"
            identical_status = "IDEMPOTENT_EXISTING_FORWARD_ORDER"
        else:
            cur.execute(
                """
                SELECT id,side,price,quantity_btc,reason,rsi_14,ema_21,is_exit
                FROM simulated_orders
                WHERE order_class='LEGACY_ADMINISTRATIVE_CLOSE'
                  AND environment=%s AND deployment_id=%s AND position_id=%s
                ORDER BY id LIMIT 2
                """,
                (str(environment), str(deployment_id), int(position_id)),
            )
            existing = cur.fetchall()
            conflict_status = "ADMINISTRATIVE_ORDER_IDENTITY_CONFLICT"
            identical_status = "IDEMPOTENT_EXISTING_ADMINISTRATIVE_ORDER"
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        if len(existing) != 1:
            raise
        row = existing[0]
        expected_identity = (
            str(side).upper(), Decimal(str(price)), Decimal(str(quantity)),
            str(reason),
            None if rsi_14 is None else Decimal(str(rsi_14)),
            None if ema_21 is None else Decimal(str(ema_21)),
            bool(is_exit),
        )
        actual_identity = (
            str(row[1]).upper(), Decimal(str(row[2])), Decimal(str(row[3])),
            str(row[4]),
            None if row[5] is None else Decimal(str(row[5])),
            None if row[6] is None else Decimal(str(row[6])),
            bool(row[7]),
        )
        return SimulatedOrderWriteBlocked(
            identical_status if actual_identity == expected_identity else conflict_status,
            int(row[0]),
        )
    else:
        cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        if inserted is None:
            raise RuntimeError("SIMULATED_ORDER_INSERT_RETURNING_MISSING")
        return int(inserted[0])


def create_simulated_execution_fill_cursor(
    cur,
    *,
    simulated_order_id: int,
    position_id: int,
    order_purpose: str,
    side: str,
    symbol: str,
    quantity: Decimal,
    price: Decimal,
    account_identity_id: int | None,
    instrument_snapshot_id: int | None,
    environment: str,
    deployment_id: str,
    execution_at,
    interval: str | None = None,
    strategy: str | None = None,
    account_identity_fingerprint: str | None = None,
    instrument_metadata_fingerprint: str | None = None,
) -> int | None:
    """Canonical transaction-bound PAPER fill writer and fee policy."""
    purpose = str(order_purpose).upper()
    if purpose not in {"ENTRY", "EXIT"}:
        raise ValueError("INVALID_SIMULATED_ORDER_PURPOSE")
    if purpose == "EXIT":
        if interval is None or strategy is None:
            raise ValueError("EXIT_SLOT_IDENTITY_REQUIRED")
        lock_simulated_exit_slot_cursor(
            cur, symbol=symbol, interval=interval, strategy=strategy,
        )
    quantity = Decimal(str(quantity))
    price = Decimal(str(price))
    if quantity <= 0 or price < 0:
        raise ValueError("INVALID_SIMULATED_EXECUTION_VALUE")
    _base_asset, quote_asset = _assets(symbol)
    notional = quantity * price
    fee_usdc = notional * SIMULATION_FEE_RATE
    source_payload = {
        "simulated_order_id": int(simulated_order_id),
        "position_id": int(position_id),
        "purpose": purpose,
        "quantity": str(quantity),
        "price": str(price),
        "fee_usdc": str(fee_usdc),
        "identity": account_identity_fingerprint,
        "instrument": instrument_metadata_fingerprint,
        "environment": str(environment),
        "deployment_id": str(deployment_id),
        "model": SIMULATION_MODEL_VERSION,
    }
    cur.execute(
        """
        INSERT INTO simulated_execution_fills_v1 (
          simulated_order_id,position_id,order_purpose,side,symbol,
          fill_qty,fill_price,fill_notional,fee_qty,fee_asset,
          authoritative_fee_usdc,account_identity_id,instrument_snapshot_id,
          environment,deployment_id,simulation_model_version,execution_at,
          source_fingerprint
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (simulated_order_id,fill_index) DO NOTHING
        RETURNING id
        """,
        (
            int(simulated_order_id), int(position_id), purpose,
            str(side).upper(), str(symbol), quantity, price, notional,
            fee_usdc, quote_asset, fee_usdc, account_identity_id,
            instrument_snapshot_id, str(environment), str(deployment_id),
            SIMULATION_MODEL_VERSION, execution_at, _hash(source_payload),
        ),
    )
    row = cur.fetchone()
    return int(row[0]) if row is not None else None


def _instrument_values(client, symbol: str, *, allow_remote: bool = True):
    try:
        from common.sizing import _FILTERS_CACHE
        exchange = os.getenv("EXCHANGE", "BINANCE").strip().upper()
        cached = _FILTERS_CACHE.get(f"{exchange}:{symbol}")
        if cached is not None:
            step = Decimal(str(cached.step))
            return (
                step,
                Decimal(str(cached.min_qty)),
                Decimal(str(cached.min_notional)),
                abs(step.normalize().as_tuple().exponent) if step else None,
                None,
            )
        if not allow_remote:
            return None
        info = client.get_symbol_info(symbol)
        filters = {item.get("filterType"): item for item in info.get("filters", [])}
        lot = filters.get("LOT_SIZE") or {}
        notional = filters.get("MIN_NOTIONAL") or {}
        step = Decimal(str(lot.get("stepSize")))
        min_qty = Decimal(str(lot.get("minQty") or 0))
        min_notional = Decimal(str(notional.get("minNotional") or 0))
        raw = info.get("raw") or {}
        quantity_precision = (
            abs(step.normalize().as_tuple().exponent) if step else None
        )
        price_tick = raw.get("tickSz")
        price_precision = (
            abs(Decimal(str(price_tick)).normalize().as_tuple().exponent)
            if price_tick else None
        )
        return step, min_qty, min_notional, quantity_precision, price_precision
    except Exception:
        return None


def record_simulated_fill_evidence(
    connection_factory,
    *,
    client,
    simulated_order_id: int,
    position_id: int,
    environment: str,
    deployment_id: str,
) -> bool:
    """Persist additive evidence after an existing PAPER lifecycle action."""
    if str(environment).lower() != "paper":
        return False
    conn = connection_factory()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol,side,price,quantity_btc,is_exit,created_at,
                           "interval",strategy
                    FROM simulated_orders WHERE id=%s
                    """,
                    (int(simulated_order_id),),
                )
                order = cur.fetchone()
                if order is None:
                    return False
                (
                    symbol, side, price, qty, is_exit, execution_at,
                    interval, strategy,
                ) = order
                cur.execute(
                    """
                    SELECT
                      COALESCE(
                        p.inventory_contract_adoption_id,
                        adoption.adoption_id
                      ),
                      COALESCE(
                        p.inventory_contract_generation,
                        adoption.generation
                      ),
                      adoption.git_revision,
                      CASE
                        WHEN p.inventory_contract_adoption_id=adoption.adoption_id
                         AND p.inventory_contract_generation=adoption.generation
                          THEN 'FORWARD_C2_2'
                        WHEN is_existing_projected_c2_2_compatible(
                          p.id,'paper'
                        ) THEN 'EXISTING_PROJECTED_C2_2'
                        WHEN p.inventory_contract_adoption_id IS NOT NULL
                          THEN 'ADOPTION_GENERATION_MISMATCH'
                        WHEN p.inventory_contract_adoption_id IS NULL
                         AND p.inventory_contract_generation IS NULL
                         AND p.entry_time>=adoption.adopted_at
                          THEN 'FORWARD_C2_2'
                        ELSE 'LEGACY_UNPROJECTED'
                      END
                    FROM positions p
                    JOIN runtime_contract_adoption_v2 adoption
                      ON adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
                     AND adoption.environment='paper'
                     AND adoption.deployment_id=%s
                     AND adoption.status='ACTIVE'
                    WHERE p.id=%s
                    FOR UPDATE OF p
                    """,
                    (
                        str(deployment_id),
                        int(position_id),
                    ),
                )
                generation_gate = cur.fetchone()
                if generation_gate is None:
                    return False
                (
                    adoption_id,
                    contract_generation,
                    adoption_git_revision,
                    classification,
                ) = generation_gate
                if classification not in (
                    "FORWARD_C2_2", "EXISTING_PROJECTED_C2_2"
                ):
                    return False
                log_runtime_revision_provenance_diagnostic(
                    adoption_id=int(adoption_id),
                    generation=int(contract_generation),
                    adoption_git_revision=str(adoption_git_revision),
                    runtime_git_revision=require_runtime_git_revision(),
                )
                cur.execute(
                    """
                    UPDATE positions
                    SET inventory_contract_adoption_id=%s,
                        inventory_contract_generation=%s
                    WHERE id=%s
                      AND inventory_contract_adoption_id IS NULL
                      AND inventory_contract_generation IS NULL
                    """,
                    (
                        int(adoption_id), int(contract_generation),
                        int(position_id),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO financial_truth_simulated_account_v1 (
                      deployment_id,simulated_account_uid,identity_version
                    ) VALUES (%s,%s,%s)
                    ON CONFLICT (deployment_id) DO UPDATE
                      SET deployment_id=EXCLUDED.deployment_id
                    RETURNING simulated_account_uid
                    """,
                    (
                        str(deployment_id), str(uuid.uuid4()),
                        SIMULATED_IDENTITY_VERSION,
                    ),
                )
                simulated_uid = str(cur.fetchone()[0])
                identity_payload = {
                    "exchange": "SIMULATOR", "uid": simulated_uid,
                    "main_uid": simulated_uid, "scope": "SIMULATED",
                    "source": "SIMULATED_ACCOUNT_LEDGER",
                    "version": SIMULATED_IDENTITY_VERSION,
                }
                identity_fingerprint = _hash(identity_payload)
                cur.execute(
                    """
                    INSERT INTO financial_truth_account_identity_v1 (
                      source_authority,exchange,account_uid,main_account_uid,
                      account_scope,identity_source,identity_version,
                      identity_fingerprint,captured_at
                    ) VALUES (
                      'SIMULATED_EXECUTION','SIMULATOR',%s,%s,'SIMULATED',
                      'SIMULATED_ACCOUNT_LEDGER',%s,%s,clock_timestamp()
                    )
                    ON CONFLICT (identity_fingerprint) DO UPDATE
                      SET identity_fingerprint=EXCLUDED.identity_fingerprint
                    RETURNING id
                    """,
                    (
                        simulated_uid, simulated_uid,
                        SIMULATED_IDENTITY_VERSION, identity_fingerprint,
                    ),
                )
                identity_id = cur.fetchone()[0]
                metadata = _instrument_values(client, symbol, allow_remote=False)
                instrument_id = None
                metadata_fingerprint = None
                base_asset, quote_asset = _assets(symbol)
                if metadata is not None:
                    step, min_qty, min_notional, qty_precision, price_precision = metadata
                    metadata_payload = {
                        "source_authority": "SIMULATED_EXECUTION",
                        "exchange": os.getenv("EXCHANGE", "OKX").upper(),
                        "symbol": symbol, "base_asset": base_asset,
                        "quote_asset": quote_asset, "step_size": str(step),
                        "min_qty": str(min_qty), "min_notional": str(min_notional),
                        "quantity_precision": qty_precision,
                        "price_precision": price_precision,
                        "source": "EXCHANGE_PUBLIC_AT_EXECUTION",
                        "version": INSTRUMENT_METADATA_VERSION,
                    }
                    metadata_fingerprint = _hash(metadata_payload)
                    cur.execute(
                        """
                        INSERT INTO financial_truth_instrument_snapshot_v1 (
                          source_authority,exchange,symbol,base_asset,quote_asset,
                          step_size,min_qty,quantity_precision,price_precision,
                          min_notional,metadata_source,metadata_version,
                          metadata_fingerprint,captured_at
                        ) VALUES (
                          'SIMULATED_EXECUTION',%s,%s,%s,%s,%s,%s,%s,%s,%s,
                          'EXCHANGE_PUBLIC_AT_EXECUTION',%s,%s,clock_timestamp()
                        )
                        ON CONFLICT (metadata_fingerprint) DO UPDATE
                          SET metadata_fingerprint=EXCLUDED.metadata_fingerprint
                        RETURNING id
                        """,
                        (
                            os.getenv("EXCHANGE", "OKX").upper(), symbol,
                            base_asset, quote_asset, step, min_qty, qty_precision,
                            price_precision, min_notional,
                            INSTRUMENT_METADATA_VERSION, metadata_fingerprint,
                        ),
                    )
                    instrument_id = cur.fetchone()[0]
                quantity = Decimal(str(qty))
                fill_price = Decimal(str(price))
                fill_inserted = create_simulated_execution_fill_cursor(
                    cur,
                    simulated_order_id=int(simulated_order_id),
                    position_id=int(position_id),
                    order_purpose="EXIT" if is_exit else "ENTRY",
                    side=str(side), symbol=str(symbol), quantity=quantity,
                    price=fill_price, account_identity_id=int(identity_id),
                    instrument_snapshot_id=(
                        int(instrument_id) if instrument_id is not None else None
                    ),
                    environment=str(environment), deployment_id=str(deployment_id),
                    execution_at=execution_at,
                    interval=str(interval), strategy=str(strategy),
                    account_identity_fingerprint=identity_fingerprint,
                    instrument_metadata_fingerprint=metadata_fingerprint,
                ) is not None

                cur.execute(
                    """
                    SELECT order_purpose,fill_qty,fee_qty,fee_asset,
                           fill_price,execution_at,simulated_order_id
                    FROM simulated_execution_fills_v1
                    WHERE position_id=%s
                    ORDER BY execution_at,id
                    """,
                    (int(position_id),),
                )
                fill_rows = cur.fetchall()
                entry_fills = []
                exit_fills = []
                latest_exit_price = None
                latest_exit_time = None
                latest_exit_order_id = None
                for (
                    purpose, fill_qty, fee_qty, fee_asset, evidence_price,
                    evidence_time, evidence_order_id,
                ) in fill_rows:
                    item = {
                        "executed_qty": fill_qty,
                        "commission_amount": fee_qty,
                        "commission_asset": fee_asset,
                    }
                    if str(purpose).upper() == "ENTRY":
                        entry_fills.append(item)
                    else:
                        exit_fills.append(item)
                        latest_exit_price = Decimal(str(evidence_price))
                        latest_exit_time = evidence_time
                        latest_exit_order_id = str(evidence_order_id)

                inventory = project_inventory_from_execution_evidence(
                    symbol=str(symbol),
                    entry_fills=entry_fills,
                    exit_fills=exit_fills,
                    quote_asset=quote_asset,
                )
                cur.execute(
                    """
                    SELECT qty,COALESCE(cumulative_exit_executed_qty,0)
                    FROM positions WHERE id=%s FOR UPDATE
                    """,
                    (int(position_id),),
                )
                position = cur.fetchone()
                if position is None:
                    raise RuntimeError("simulated fill position missing")
                previous_qty, previous_high_water = position
                if metadata is None:
                    limits = InstrumentExecutionLimits(
                        None, None, None, None, False
                    )
                else:
                    step, min_qty, min_notional, _, _ = metadata
                    limits = InstrumentExecutionLimits(
                        Decimal(str(step)), Decimal(str(min_qty)),
                        Decimal(str(min_notional)), fill_price, True,
                    )
                apply_inventory_lifecycle_mutation(
                    cur,
                    position_id=int(position_id),
                    order_id=(
                        latest_exit_order_id
                        or f"simulated-entry-{int(simulated_order_id)}"
                    ),
                    inventory=inventory,
                    limits=limits,
                    previous_remaining_qty=Decimal(str(previous_qty)),
                    previous_exit_high_water=Decimal(str(previous_high_water)),
                    has_exit_evidence=bool(exit_fills),
                    exit_price=latest_exit_price,
                    exit_time=latest_exit_time,
                    execution_source="PAPER_SIMULATED",
                )
                return fill_inserted
    finally:
        conn.close()

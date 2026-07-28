from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from decimal import Decimal
from enum import Enum
from typing import Callable

from common.financial_truth_calculator import FillEvidence


@dataclass(frozen=True)
class ExecutionEvidenceContext:
    environment: str
    exchange: str | None
    deployment_id: str

    def __post_init__(self):
        environment = str(self.environment).strip().lower()
        exchange = str(self.exchange).strip().upper() if self.exchange else None
        deployment_id = str(self.deployment_id).strip()
        if environment not in {"paper", "live"}:
            raise ValueError("INVALID_EXECUTION_EVIDENCE_ENVIRONMENT")
        if environment == "live" and not exchange:
            raise ValueError("LIVE_EXECUTION_EVIDENCE_EXCHANGE_REQUIRED")
        if not deployment_id:
            raise ValueError("EXECUTION_EVIDENCE_DEPLOYMENT_REQUIRED")
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "exchange", exchange)
        object.__setattr__(self, "deployment_id", deployment_id)


class SourceReadinessIssue(str, Enum):
    NO_EXECUTION_EVIDENCE = "NO_EXECUTION_EVIDENCE"
    SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED = (
        "SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED"
    )
    EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED = (
        "EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED"
    )


def is_source_readiness_issue(value: object) -> bool:
    """Return whether a source outcome is categorically non-writable."""
    return isinstance(value, SourceReadinessIssue)


SIMULATED_SCHEMA_CONTRACT = {
    "simulated_execution_fills_v1": {
        "id", "simulated_order_id", "position_id", "order_purpose", "side",
        "symbol", "fill_qty", "fill_price", "fill_notional", "fee_qty",
        "fee_asset", "authoritative_fee_usdc", "estimated_fee_usdc",
        "account_identity_id", "instrument_snapshot_id", "source_authority",
        "environment", "deployment_id", "simulation_model_version",
        "execution_at",
    },
    "financial_truth_account_identity_v1": {"id", "identity_fingerprint"},
    "financial_truth_instrument_snapshot_v1": {
        "id", "metadata_fingerprint", "step_size", "base_asset", "quote_asset",
    },
}

EXCHANGE_SCHEMA_CONTRACT = {
    "binance_orders": {
        "exchange_source", "reconciled_position_id", "position_id",
        "order_purpose", "order_id", "symbol",
    },
    "binance_order_fills": {
        "id", "source", "order_id", "symbol", "side", "executed_qty",
        "avg_price", "quote_notional_usdc", "commission_amount",
        "commission_asset", "commission_usdc", "event_time",
        "account_identity_id", "instrument_snapshot_id",
    },
    "financial_truth_account_identity_v1": {"id", "identity_fingerprint"},
    "financial_truth_instrument_snapshot_v1": {
        "id", "metadata_fingerprint", "step_size", "base_asset", "quote_asset",
    },
}


def normalize_optional_asset(value) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if not normalized or normalized in {"NONE", "NULL", "UNKNOWN", "N/A"}:
        return None
    return normalized


class FinancialTruthSourceRepository:
    def __init__(self, connection_factory: Callable):
        self.connection_factory = connection_factory

    @staticmethod
    def _fill(row) -> FillEvidence:
        return FillEvidence(
            fill_id=str(row[0]), order_id=str(row[1]), position_id=int(row[2]),
            purpose=str(row[3]), side=str(row[4]), symbol=str(row[5]),
            quantity=Decimal(str(row[6])), price=Decimal(str(row[7])),
            notional=Decimal(str(row[8])),
            fee_quantity=None if row[9] is None else Decimal(str(row[9])),
            fee_asset=normalize_optional_asset(row[10]),
            authoritative_fee_usdc=(
                None if row[11] is None else Decimal(str(row[11]))
            ),
            estimated_fee_usdc=(
                None if row[12] is None else Decimal(str(row[12]))
            ),
            account_identity_fingerprint=row[13],
            instrument_metadata_fingerprint=row[14],
            step_size=None if row[15] is None else Decimal(str(row[15])),
            base_asset=normalize_optional_asset(row[16]),
            quote_asset=normalize_optional_asset(row[17]),
            source_authority=str(row[18]), source_exchange=str(row[19]),
            source_environment=str(row[20]), source_deployment_id=str(row[21]),
            source_version=str(row[22]),
            event_time=row[23].astimezone(timezone.utc),
        )

    @staticmethod
    def _supports(cur, contract: dict[str, set[str]]) -> bool:
        cur.execute(
            """
            SELECT table_name,column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=ANY(%s)
            """,
            (list(contract),),
        )
        available: dict[str, set[str]] = {}
        for table_name, column_name in cur.fetchall():
            available.setdefault(str(table_name), set()).add(str(column_name))
        return all(
            required.issubset(available.get(table_name, set()))
            for table_name, required in contract.items()
        )

    def read_position_and_fills(
        self,
        position_id: int,
        *,
        context: ExecutionEvidenceContext,
        connection=None,
    ):
        if not isinstance(context, ExecutionEvidenceContext):
            raise TypeError("EXECUTION_EVIDENCE_CONTEXT_REQUIRED")
        owned = connection is None
        conn = connection or self.connection_factory()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id,status,gross_pnl_usdc,fees_usdc,net_pnl_usdc
                FROM public.positions WHERE id=%s
                """,
                (int(position_id),),
            )
            position = cur.fetchone()
            if position is None:
                raise LookupError("POSITION_NOT_FOUND")
            if context.environment == "paper":
                if not self._supports(cur, SIMULATED_SCHEMA_CONTRACT):
                    return (
                        position,
                        (),
                        SourceReadinessIssue.SIMULATED_EXECUTION_SCHEMA_UNSUPPORTED,
                    )
                cur.execute(
                    """
                SELECT
                  'simulated:' || sf.id::text, sf.simulated_order_id::text,
                  sf.position_id, sf.order_purpose, sf.side, sf.symbol,
                  sf.fill_qty, sf.fill_price, sf.fill_notional,
                  sf.fee_qty, sf.fee_asset, sf.authoritative_fee_usdc,
                  sf.estimated_fee_usdc, ai.identity_fingerprint,
                  im.metadata_fingerprint, im.step_size, im.base_asset,
                  im.quote_asset, sf.source_authority, 'SIMULATOR',
                  sf.environment, sf.deployment_id,
                  sf.simulation_model_version, sf.execution_at
                FROM public.simulated_execution_fills_v1 sf
                LEFT JOIN public.financial_truth_account_identity_v1 ai
                  ON ai.id=sf.account_identity_id
                LEFT JOIN public.financial_truth_instrument_snapshot_v1 im
                  ON im.id=sf.instrument_snapshot_id
                WHERE sf.position_id=%s
                  AND lower(sf.environment)=%s
                  AND sf.deployment_id=%s
                ORDER BY sf.execution_at,sf.id
                    """,
                    (
                        int(position_id), context.environment,
                        context.deployment_id,
                    ),
                )
                rows = list(cur.fetchall())
            else:
                if not self._supports(cur, EXCHANGE_SCHEMA_CONTRACT):
                    return (
                        position,
                        (),
                        SourceReadinessIssue.EXCHANGE_EXECUTION_SCHEMA_UNSUPPORTED,
                    )
                cur.execute(
                    """
                    SELECT
                      'exchange:' || f.id::text, f.order_id, %s,
                      bo.order_purpose, f.side, f.symbol,
                      f.executed_qty, f.avg_price, f.quote_notional_usdc,
                      f.commission_amount, f.commission_asset,
                      CASE
                        WHEN im.base_asset IS NOT NULL
                         AND im.quote_asset IS NOT NULL
                         AND f.commission_asset IN (
                          im.base_asset, im.quote_asset
                        ) THEN f.commission_usdc
                        ELSE NULL
                      END,
                      CASE
                        WHEN im.base_asset IS NULL
                          OR im.quote_asset IS NULL
                          OR f.commission_asset IS NULL
                          OR f.commission_asset NOT IN (
                            im.base_asset, im.quote_asset
                          )
                        THEN f.commission_usdc
                        ELSE NULL
                      END,
                      ai.identity_fingerprint, im.metadata_fingerprint,
                      im.step_size, im.base_asset, im.quote_asset,
                      'EXCHANGE_EXECUTION', f.source,
                      %s, %s,
                      'EXCHANGE_FILL_V1', f.event_time
                    FROM public.binance_order_fills f
                    JOIN public.binance_orders bo
                      ON bo.exchange_source=f.source
                     AND bo.symbol=f.symbol AND bo.order_id=f.order_id
                    LEFT JOIN public.financial_truth_account_identity_v1 ai
                      ON ai.id=f.account_identity_id
                    LEFT JOIN public.financial_truth_instrument_snapshot_v1 im
                      ON im.id=f.instrument_snapshot_id
                    WHERE COALESCE(bo.reconciled_position_id,bo.position_id)=%s
                      AND lower(f.source)=%s
                    ORDER BY f.event_time,f.id
                    """,
                    (
                        int(position_id), context.environment,
                        context.deployment_id, int(position_id),
                        context.exchange.lower(),
                    ),
                )
                rows = list(cur.fetchall())
            fills = tuple(self._fill(row) for row in rows)
            return position, fills, (
                None if fills else SourceReadinessIssue.NO_EXECUTION_EVIDENCE
            )
        finally:
            cur.close()
            if owned:
                conn.close()


class CanonicalFinancialTruthWriteRepository:
    WRITER_VERSION = "FINANCIAL_TRUTH_RECONCILER_V1"

    @staticmethod
    def lock_position(cur, position_id: int) -> None:
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s,%s)",
            (0x4654, int(position_id)),
        )

    @staticmethod
    def current(cur, position_id: int):
        cur.execute(
            """
            SELECT financial_truth_status,source_fingerprint,
                   authoritative_gross_pnl,authoritative_net_pnl,
                   authoritative_fees_usdc,remaining_inventory_qty
            FROM canonical_financial_truth_v1 WHERE position_id=%s FOR UPDATE
            """,
            (int(position_id),),
        )
        return cur.fetchone()

    @classmethod
    def write(cls, cur, calculation, *, invocation_type: str, invocation_identity: str | None):
        from psycopg2.extras import Json

        def json_number(value):
            return None if value is None else str(value)

        previous = cls.current(cur, calculation.position_id)
        if previous is not None and previous[1] == calculation.source_fingerprint:
            return False
        values = calculation.semantic_values()
        evidence = {
            "source_order_ids": calculation.source_order_ids,
            "source_fill_ids": calculation.source_fill_ids,
            "source_fingerprint": calculation.source_fingerprint,
            "calculation_version": calculation.calculation_version,
        }
        now_complete = calculation.financial_truth_status == "COMPLETE"
        cur.execute(
            """
            INSERT INTO canonical_financial_truth_v1 (
              position_id,financial_truth_status,executed_entry_qty,
              executed_exit_qty,remaining_qty,gross_entry_qty,gross_exit_qty,
              base_asset_entry_fee_qty,base_asset_exit_fee_qty,
              net_entry_inventory_qty,net_exit_inventory_reduction_qty,
              gross_remaining_execution_qty,remaining_inventory_qty,
              authoritative_entry_notional,authoritative_exit_notional,
              authoritative_entry_fees_usdc,authoritative_exit_fees_usdc,
              authoritative_fees_usdc,authoritative_gross_pnl,
              authoritative_net_pnl,estimated_gross_pnl,estimated_fees_usdc,
              estimated_net_pnl,entry_fill_count,exit_fill_count,
              first_entry_fill_at,last_entry_fill_at,first_exit_fill_at,
              last_exit_fill_at,source_authority,source_exchange,
              source_environment,source_deployment_id,
              source_account_identity_fingerprint,source_order_ids,
              source_fill_ids,source_fingerprint,calculation_version,
              writer_version,calculated_at,completed_at,failure_code,
              failure_detail,failure_reason,authoritative_source,
              authoritative_evidence,evidence_observed_at,updated_at
            ) VALUES (
              %(position_id)s,%(status)s,%(entry_qty)s,%(exit_qty)s,
              %(remaining)s,%(entry_qty)s,%(exit_qty)s,%(entry_base_fee)s,
              %(exit_base_fee)s,%(net_entry)s,%(net_exit)s,%(gross_remaining)s,
              %(remaining)s,%(entry_notional)s,%(exit_notional)s,
              %(entry_fees)s,%(exit_fees)s,%(fees)s,%(gross_pnl)s,%(net_pnl)s,
              %(estimated_gross)s,%(estimated_fees)s,%(estimated_net)s,
              %(entry_count)s,%(exit_count)s,%(first_entry)s,%(last_entry)s,
              %(first_exit)s,%(last_exit)s,%(authority)s,%(exchange)s,
              %(environment)s,%(deployment)s,%(account_fp)s,%(order_ids)s,
              %(fill_ids)s,%(fingerprint)s,%(calculation_version)s,
              %(writer_version)s,clock_timestamp(),
              CASE WHEN %(complete)s THEN clock_timestamp() ELSE NULL END,
              %(failure_code)s,%(failure_detail)s,%(failure_reason)s,
              %(authoritative_source)s,%(evidence)s,
              CASE WHEN %(has_evidence)s THEN clock_timestamp() ELSE NULL END,
              clock_timestamp()
            )
            ON CONFLICT (position_id) DO UPDATE SET
              financial_truth_status=EXCLUDED.financial_truth_status,
              executed_entry_qty=EXCLUDED.executed_entry_qty,
              executed_exit_qty=EXCLUDED.executed_exit_qty,
              remaining_qty=EXCLUDED.remaining_qty,
              gross_entry_qty=EXCLUDED.gross_entry_qty,
              gross_exit_qty=EXCLUDED.gross_exit_qty,
              base_asset_entry_fee_qty=EXCLUDED.base_asset_entry_fee_qty,
              base_asset_exit_fee_qty=EXCLUDED.base_asset_exit_fee_qty,
              net_entry_inventory_qty=EXCLUDED.net_entry_inventory_qty,
              net_exit_inventory_reduction_qty=EXCLUDED.net_exit_inventory_reduction_qty,
              gross_remaining_execution_qty=EXCLUDED.gross_remaining_execution_qty,
              remaining_inventory_qty=EXCLUDED.remaining_inventory_qty,
              authoritative_entry_notional=EXCLUDED.authoritative_entry_notional,
              authoritative_exit_notional=EXCLUDED.authoritative_exit_notional,
              authoritative_entry_fees_usdc=EXCLUDED.authoritative_entry_fees_usdc,
              authoritative_exit_fees_usdc=EXCLUDED.authoritative_exit_fees_usdc,
              authoritative_fees_usdc=EXCLUDED.authoritative_fees_usdc,
              authoritative_gross_pnl=EXCLUDED.authoritative_gross_pnl,
              authoritative_net_pnl=EXCLUDED.authoritative_net_pnl,
              estimated_gross_pnl=EXCLUDED.estimated_gross_pnl,
              estimated_fees_usdc=EXCLUDED.estimated_fees_usdc,
              estimated_net_pnl=EXCLUDED.estimated_net_pnl,
              entry_fill_count=EXCLUDED.entry_fill_count,
              exit_fill_count=EXCLUDED.exit_fill_count,
              first_entry_fill_at=EXCLUDED.first_entry_fill_at,
              last_entry_fill_at=EXCLUDED.last_entry_fill_at,
              first_exit_fill_at=EXCLUDED.first_exit_fill_at,
              last_exit_fill_at=EXCLUDED.last_exit_fill_at,
              source_authority=EXCLUDED.source_authority,
              source_exchange=EXCLUDED.source_exchange,
              source_environment=EXCLUDED.source_environment,
              source_deployment_id=EXCLUDED.source_deployment_id,
              source_account_identity_fingerprint=EXCLUDED.source_account_identity_fingerprint,
              source_order_ids=EXCLUDED.source_order_ids,
              source_fill_ids=EXCLUDED.source_fill_ids,
              source_fingerprint=EXCLUDED.source_fingerprint,
              calculation_version=EXCLUDED.calculation_version,
              writer_version=EXCLUDED.writer_version,
              calculated_at=EXCLUDED.calculated_at,
              completed_at=EXCLUDED.completed_at,
              failure_code=EXCLUDED.failure_code,
              failure_detail=EXCLUDED.failure_detail,
              failure_reason=EXCLUDED.failure_reason,
              authoritative_source=EXCLUDED.authoritative_source,
              authoritative_evidence=EXCLUDED.authoritative_evidence,
              evidence_observed_at=EXCLUDED.evidence_observed_at,
              updated_at=EXCLUDED.updated_at
            """,
            {
                "position_id": calculation.position_id,
                "status": calculation.financial_truth_status,
                "entry_qty": calculation.gross_entry_qty,
                "exit_qty": calculation.gross_exit_qty,
                "remaining": calculation.remaining_inventory_qty,
                "entry_base_fee": calculation.base_asset_entry_fee_qty,
                "exit_base_fee": calculation.base_asset_exit_fee_qty,
                "net_entry": calculation.net_entry_inventory_qty,
                "net_exit": calculation.net_exit_inventory_reduction_qty,
                "gross_remaining": calculation.gross_remaining_execution_qty,
                "entry_notional": calculation.authoritative_entry_notional,
                "exit_notional": calculation.authoritative_exit_notional,
                "entry_fees": calculation.authoritative_entry_fees_usdc,
                "exit_fees": calculation.authoritative_exit_fees_usdc,
                "fees": calculation.authoritative_fees_usdc,
                "gross_pnl": calculation.authoritative_gross_pnl,
                "net_pnl": calculation.authoritative_net_pnl,
                "estimated_gross": calculation.estimated_gross_pnl,
                "estimated_fees": calculation.estimated_fees_usdc,
                "estimated_net": calculation.estimated_net_pnl,
                "entry_count": calculation.entry_fill_count,
                "exit_count": calculation.exit_fill_count,
                "first_entry": calculation.first_entry_fill_at,
                "last_entry": calculation.last_entry_fill_at,
                "first_exit": calculation.first_exit_fill_at,
                "last_exit": calculation.last_exit_fill_at,
                "authority": calculation.source_authority,
                "exchange": calculation.source_exchange,
                "environment": calculation.source_environment,
                "deployment": calculation.source_deployment_id,
                "account_fp": calculation.source_account_identity_fingerprint,
                "order_ids": Json(calculation.source_order_ids),
                "fill_ids": Json(calculation.source_fill_ids),
                "fingerprint": calculation.source_fingerprint,
                "calculation_version": calculation.calculation_version,
                "writer_version": cls.WRITER_VERSION,
                "complete": now_complete,
                "failure_code": calculation.failure_code,
                "failure_detail": calculation.failure_detail,
                "failure_reason": (
                    calculation.failure_detail
                    if calculation.financial_truth_status == "FAILED" else None
                ),
                "authoritative_source": calculation.source_authority,
                "evidence": Json(evidence),
                "has_evidence": bool(calculation.source_fill_ids),
            },
        )
        cur.execute(
            """
            INSERT INTO canonical_financial_truth_audit_v1 (
              position_id,previous_status,new_status,previous_fingerprint,
              new_fingerprint,previous_values,new_values,reason,writer_version,
              invocation_type,invocation_identity
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (position_id,new_fingerprint) DO NOTHING
            """,
            (
                calculation.position_id, previous[0] if previous else None,
                calculation.financial_truth_status,
                previous[1] if previous else None, calculation.source_fingerprint,
                Json({
                    "authoritative_gross_pnl": json_number(previous[2]) if previous else None,
                    "authoritative_net_pnl": json_number(previous[3]) if previous else None,
                    "authoritative_fees_usdc": json_number(previous[4]) if previous else None,
                    "remaining_inventory_qty": json_number(previous[5]) if previous else None,
                }),
                Json(values), "SOURCE_FINGERPRINT_CHANGED",
                cls.WRITER_VERSION, invocation_type, invocation_identity,
            ),
        )
        return True

# common/exchange_ingest_trades.py
import json
import time
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from common.exchange_client import get_market_data_client
from typing import Iterable, Dict, Any, Optional

import psycopg2
from psycopg2.extras import execute_batch

from common.contract_adoption import (
    log_runtime_revision_provenance_diagnostic,
    require_runtime_git_revision,
)
from common.entry_fill_reconciliation import (
    converge_partial_applied_recovery_link,
    run_pending_entry_reconciliation_if_due,
)
from common.entry_position_projection import (
    EntryPositionProjectionMode,
    run_entry_position_projection,
)
from common.entry_fill_attribution import (
    EntryFillAttributionMode,
    EntryFillAttributionRepository,
    EntryFillObservation,
    EntryFillProcessingOutcome,
    process_entry_fill_attribution,
)
from common.exchange_fill_change_control import (
    FillApplicationClassification,
    FillMutationDecision,
    attribute_fill_change_position,
    mark_fill_change_applied,
    register_fill_change,
)
from common.exchange_identity import normalize_exchange_source
from common.flags import trading_mode
from common.financial_truth_calculator import calculate_financial_truth
from common.financial_truth_repository import (
    CanonicalFinancialTruthWriteRepository,
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
)
from common.inventory_quantity import (
    ExitInventoryClassification,
    ExitInventoryStatus,
)


LEI1C_MIGRATION_ID = "20260731_live_entry_fill_attribution_v1.sql"
LEI1C_LEDGER_CHECKSUM = (
    "ad72d70d21d440de1d65c3499a1de1e95b6a27af0721c4c3c9c71f150168541d"
)


class FillIngestResult(tuple):
    """Two-item legacy result with additive applicability metadata."""

    def __new__(
        cls,
        fetched: int,
        fee_rows_priced: int,
        *,
        status: str = "OK",
        ran: bool = True,
        applicable: bool = True,
    ):
        result = super().__new__(cls, (int(fetched), int(fee_rows_priced)))
        result.status = status
        result.ran = bool(ran)
        result.applicable = bool(applicable)
        return result

    @property
    def fetched(self) -> int:
        return self[0]

    @property
    def fee_rows_priced(self) -> int:
        return self[1]


UPSERT_TRADE_SQL = """
INSERT INTO binance_order_fills (
  source,
  trade_id,
  order_id,
  symbol,
  side,
  role,
  executed_qty,
  avg_price,
  quote_notional_usdc,
  commission_amount,
  commission_asset,
  commission_usdc,
  event_time,
  fill_idx,
  raw,
  account_identity_id,
  instrument_snapshot_id,
  account_identity_status,
  account_identity_failure_code
)
VALUES (
  %(source)s,
  %(trade_id)s,
  %(order_id)s,
  %(symbol)s,
  %(side)s,
  %(role)s,
  %(executed_qty)s,
  %(avg_price)s,
  %(quote_notional_usdc)s,
  %(commission_amount)s,
  %(commission_asset)s,
  %(commission_usdc)s,
  to_timestamp(%(event_time_ms)s / 1000.0),
  %(fill_idx)s,
  %(raw)s::jsonb,
  %(account_identity_id)s,
  %(instrument_snapshot_id)s,
  %(account_identity_status)s,
  %(account_identity_failure_code)s
)
ON CONFLICT (source, trade_id) DO UPDATE SET
  order_id=EXCLUDED.order_id,
  symbol=EXCLUDED.symbol,
  side=EXCLUDED.side,
  role=EXCLUDED.role,
  executed_qty=EXCLUDED.executed_qty,
  avg_price=EXCLUDED.avg_price,
  quote_notional_usdc=EXCLUDED.quote_notional_usdc,
  commission_amount=EXCLUDED.commission_amount,
  commission_asset=EXCLUDED.commission_asset,
  commission_usdc=EXCLUDED.commission_usdc,
  event_time=EXCLUDED.event_time,
  raw=EXCLUDED.raw,
  account_identity_id=COALESCE(
    EXCLUDED.account_identity_id,binance_order_fills.account_identity_id
  ),
  instrument_snapshot_id=COALESCE(
    EXCLUDED.instrument_snapshot_id,binance_order_fills.instrument_snapshot_id
  ),
  account_identity_status=EXCLUDED.account_identity_status,
  account_identity_failure_code=EXCLUDED.account_identity_failure_code
WHERE (
  binance_order_fills.order_id,
  binance_order_fills.symbol,
  binance_order_fills.side,
  binance_order_fills.role,
  binance_order_fills.executed_qty,
  binance_order_fills.avg_price,
  binance_order_fills.quote_notional_usdc,
  binance_order_fills.commission_amount,
  binance_order_fills.commission_asset,
  binance_order_fills.commission_usdc,
  binance_order_fills.event_time,
  binance_order_fills.raw
) IS DISTINCT FROM (
  EXCLUDED.order_id,
  EXCLUDED.symbol,
  EXCLUDED.side,
  EXCLUDED.role,
  EXCLUDED.executed_qty,
  EXCLUDED.avg_price,
  EXCLUDED.quote_notional_usdc,
  EXCLUDED.commission_amount,
  EXCLUDED.commission_asset,
  EXCLUDED.commission_usdc,
  EXCLUDED.event_time,
  EXCLUDED.raw
);
"""

READ_STATE_SQL = """
SELECT last_trade_time_ms
FROM binance_ingest_state
WHERE symbol = %s;
"""

UPSERT_STATE_SQL = """
INSERT INTO binance_ingest_state(symbol, last_trade_time_ms)
VALUES (%s, %s)
ON CONFLICT (symbol)
DO UPDATE SET last_trade_time_ms = EXCLUDED.last_trade_time_ms,
              updated_at = now();
"""

PRICE_FEES_SQL = """
WITH priced AS (
  SELECT
    f.id,
    COALESCE(c_exact.close, c_prev.close) AS bnbusdc_close
  FROM binance_order_fills f
  LEFT JOIN candles c_exact
    ON c_exact.symbol='BNBUSDC'
   AND c_exact.interval='1m'
   AND c_exact.open_time = date_trunc('minute', f.event_time)
  LEFT JOIN LATERAL (
    SELECT close
    FROM candles c
    WHERE c.symbol='BNBUSDC'
      AND c.interval='1m'
      AND c.open_time <= date_trunc('minute', f.event_time)
    ORDER BY c.open_time DESC
    LIMIT 1
  ) c_prev ON true
  WHERE f.commission_asset='BNB'
    AND f.commission_usdc IS NULL
    AND f.event_time >= to_timestamp(%s / 1000.0)
)
UPDATE binance_order_fills f
SET
  bnbusdc_price = p.bnbusdc_close,
  commission_usdc = f.commission_amount * p.bnbusdc_close
FROM priced p
WHERE f.id = p.id
  AND p.bnbusdc_close IS NOT NULL;
"""



RECONCILE_OKX_EXIT_FILLS_SQL = """
WITH sell_orders AS (
  SELECT
    f.source,
    f.symbol,
    f.order_id,
    NULLIF(f.raw->'raw'->>'clOrdId', '') AS clordid,
    MIN(f.event_time) AS exit_time,
    SUM(f.executed_qty) AS executed_qty,
    CASE
      WHEN SUM(f.executed_qty) > 0
        THEN SUM(f.executed_qty * f.avg_price) / SUM(f.executed_qty)
      ELSE MAX(f.avg_price)
    END AS avg_exit_price
  FROM binance_order_fills f
  WHERE f.source = %s
    AND f.side = 'SELL'
    AND f.event_time >= to_timestamp(%s / 1000.0)
  GROUP BY f.source, f.symbol, f.order_id, NULLIF(f.raw->'raw'->>'clOrdId', '')
), pending_orders AS (
  SELECT bo.id, bo.position_id,
         s.executed_qty,
         s.executed_qty - COALESCE(bo.reconciled_executed_qty, 0) AS delta_qty,
         s.avg_exit_price, s.exit_time, s.order_id, s.clordid
  FROM binance_orders bo
  JOIN sell_orders s
    ON bo.exchange_source = s.source
   AND bo.symbol = s.symbol
   AND bo.order_id = s.order_id
  WHERE bo.order_purpose = 'EXIT'
    AND bo.position_id IS NOT NULL
    AND s.executed_qty > COALESCE(bo.reconciled_executed_qty, 0)
  FOR UPDATE OF bo
), applied_orders AS (
  UPDATE binance_orders bo
  SET reconciled_executed_qty = po.executed_qty,
      reconciled_position_id = bo.position_id,
      reconciled_at = now(), reconciliation_status = 'RECONCILED',
      last_reconciliation_action = 'EXIT_FILL_DELTA_APPLIED',
      unreconciled_qty = 0, reconciliation_error = NULL
  FROM pending_orders po
  WHERE bo.id = po.id
  RETURNING bo.position_id, po.delta_qty, po.avg_exit_price,
            po.exit_time, po.order_id, po.clordid
), candidates AS (
  SELECT
    p.id AS position_id, SUM(a.delta_qty) AS delta_qty,
    MAX(a.exit_time) AS exit_time,
    CASE WHEN SUM(a.delta_qty) > 0
      THEN SUM(a.delta_qty * a.avg_exit_price) / SUM(a.delta_qty)
      ELSE MAX(a.avg_exit_price) END AS avg_exit_price,
    MAX(a.order_id) AS order_id, MAX(a.clordid) AS clordid
  FROM applied_orders a
  JOIN positions p
    ON p.id = a.position_id AND p.status = 'OPEN'
  GROUP BY p.id
)
UPDATE positions p
SET
  qty = CASE WHEN c.delta_qty >= (p.qty * 0.999) THEN 0
             ELSE GREATEST(0, p.qty - c.delta_qty) END,
  status = CASE WHEN c.delta_qty >= (p.qty * 0.999) THEN 'CLOSED' ELSE 'OPEN' END,
  exit_price = CASE WHEN c.delta_qty >= (p.qty * 0.999) THEN c.avg_exit_price ELSE p.exit_price END,
  exit_time = CASE WHEN c.delta_qty >= (p.qty * 0.999) THEN c.exit_time ELSE p.exit_time END,
  exit_reason = CASE WHEN c.delta_qty >= (p.qty * 0.999)
    THEN COALESCE(NULLIF(p.exit_reason, ''), 'RECONCILED_OKX_EXIT_FILL') ELSE p.exit_reason END,
  exit_order_id = COALESCE(p.exit_order_id, c.order_id),
  exit_client_order_id = COALESCE(NULLIF(p.exit_client_order_id, ''), c.clordid),
  exit_hour_utc = EXTRACT(HOUR FROM c.exit_time)::smallint,
  exit_day_utc = c.exit_time::date,
  hold_minutes = COALESCE(p.hold_minutes, EXTRACT(EPOCH FROM (c.exit_time - p.entry_time)) / 60.0)
FROM candidates c
WHERE p.id = c.position_id
  AND p.status = 'OPEN';
"""

RECONCILE_OKX_EXIT_FILLS_C2_2_SQL = """
WITH sell_orders AS (
  SELECT f.source,f.symbol,f.order_id,
         NULLIF(f.raw->'raw'->>'clOrdId','') AS clordid,
         MIN(f.event_time) AS exit_time,
         MAX(f.event_time) AS evidence_updated_at,
         SUM(f.executed_qty) AS executed_qty,
         CASE WHEN SUM(f.executed_qty)>0
           THEN SUM(f.executed_qty*f.avg_price)/SUM(f.executed_qty)
           ELSE MAX(f.avg_price) END AS avg_exit_price
  FROM binance_order_fills f
  WHERE f.source=%s AND f.side='SELL'
    AND f.event_time>=to_timestamp(%s/1000.0)
  GROUP BY f.source,f.symbol,f.order_id,NULLIF(f.raw->'raw'->>'clOrdId','')
), pending_orders AS (
  SELECT bo.id,bo.position_id,s.executed_qty,
         s.executed_qty-COALESCE(bo.reconciled_executed_qty,0) delta_qty,
         s.avg_exit_price,s.exit_time,s.order_id,s.clordid,
         COALESCE(
           p.inventory_contract_adoption_id,adoption.adoption_id
         ) adoption_id,
         COALESCE(
           p.inventory_contract_generation,adoption.generation
         ) generation
  FROM binance_orders bo JOIN sell_orders s
    ON bo.exchange_source=s.source AND bo.symbol=s.symbol
   AND bo.order_id=s.order_id
  JOIN positions p ON p.id=bo.position_id
  JOIN LATERAL (
    SELECT a.adoption_id,a.generation,a.environment,a.adopted_at
    FROM runtime_contract_adoption_v2 a
    WHERE a.contract_name='FEE_AWARE_INVENTORY_C2_2'
      AND a.status='ACTIVE'
      AND a.environment=lower(%s)
      AND a.deployment_id=%s
    LIMIT 1
  ) adoption ON true
  WHERE bo.order_purpose='EXIT' AND bo.position_id IS NOT NULL
    AND s.order_id=ANY(%s)
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
  FOR UPDATE OF bo
), applied_orders AS (
  UPDATE binance_orders bo
  SET reconciled_executed_qty=po.executed_qty,
      reconciled_position_id=bo.position_id,reconciled_at=now(),
      reconciliation_status='RECONCILED',
      last_reconciliation_action='EXIT_FILL_DELTA_APPLIED',
      unreconciled_qty=0,reconciliation_error=NULL
  FROM pending_orders po WHERE bo.id=po.id
  RETURNING bo.position_id,po.avg_exit_price,po.exit_time,po.order_id,po.clordid,
    po.adoption_id,po.generation
), affected AS (
  SELECT position_id,MAX(avg_exit_price) avg_exit_price,
         MAX(exit_time) exit_time,MAX(order_id) order_id,MAX(clordid) clordid,
         MAX(adoption_id) adoption_id,MAX(generation) generation
  FROM applied_orders GROUP BY position_id
), evidence AS (
  SELECT p.id position_id,
    SUM(f.executed_qty) FILTER (WHERE bo.order_purpose='ENTRY') gross_entry,
    SUM(CASE WHEN bo.order_purpose='ENTRY'
              AND upper(f.commission_asset)=upper(
                CASE WHEN p.symbol LIKE '%%USDC'
                  THEN left(p.symbol,length(p.symbol)-4)
                  WHEN p.symbol LIKE '%%USDT'
                  THEN left(p.symbol,length(p.symbol)-4) ELSE '' END)
             THEN f.commission_amount ELSE 0 END) entry_base_fee,
    SUM(f.executed_qty) FILTER (WHERE bo.order_purpose='EXIT') gross_exit,
    SUM(CASE WHEN bo.order_purpose='EXIT'
              AND upper(f.commission_asset)=upper(
                CASE WHEN p.symbol LIKE '%%USDC'
                  THEN left(p.symbol,length(p.symbol)-4)
                  WHEN p.symbol LIKE '%%USDT'
                  THEN left(p.symbol,length(p.symbol)-4) ELSE '' END)
             THEN f.commission_amount ELSE 0 END) exit_base_fee,
    BOOL_AND(f.commission_asset IS NOT NULL
             AND f.commission_amount IS NOT NULL) fee_complete
  FROM affected a JOIN positions p ON p.id=a.position_id
  JOIN binance_orders bo
    ON bo.position_id=p.id OR bo.order_id=p.entry_order_id
  JOIN binance_order_fills f ON f.order_id=bo.order_id
  GROUP BY p.id
), instrument AS (
  SELECT DISTINCT ON (e.position_id) e.position_id,
         s.step_size lot_size,s.min_qty min_size,s.min_notional,
         c.close price,
         c.open_time>=clock_timestamp()-interval '20 minutes' price_fresh
  FROM evidence e JOIN positions p ON p.id=e.position_id
  LEFT JOIN binance_order_fills f ON f.order_id=p.entry_order_id
  LEFT JOIN financial_truth_instrument_snapshot_v1 s
    ON s.id=f.instrument_snapshot_id
  LEFT JOIN LATERAL (
    SELECT close,open_time FROM candles
    WHERE symbol=p.symbol ORDER BY open_time DESC LIMIT 1
  ) c ON true
  ORDER BY e.position_id,s.captured_at DESC NULLS LAST
), classified AS (
  SELECT a.*,e.gross_entry,e.entry_base_fee,e.gross_exit,e.exit_base_fee,
    e.gross_entry-e.entry_base_fee AS net_entry,
    e.gross_exit+e.exit_base_fee AS exit_reduction,
    GREATEST(0,e.gross_entry-e.entry_base_fee-e.gross_exit-e.exit_base_fee)
      AS remaining,
    i.lot_size,i.min_size,i.min_notional,i.price,i.price_fresh,e.fee_complete,
    CASE
      WHEN NOT e.fee_complete THEN 'INCOMPLETE_EVIDENCE'
      WHEN e.gross_entry-e.entry_base_fee-e.gross_exit-e.exit_base_fee
             <= 0.000000000001 THEN 'FULLY_EXECUTED_CLOSE'
      WHEN i.lot_size IS NULL OR i.min_size IS NULL
        OR (COALESCE(i.min_notional,0)>0 AND NOT COALESCE(i.price_fresh,false))
        THEN 'INCOMPLETE_EVIDENCE'
      WHEN floor(
             GREATEST(0,e.gross_entry-e.entry_base_fee-e.gross_exit-e.exit_base_fee)
             / i.lot_size
           )*i.lot_size < i.min_size
        OR (
          COALESCE(i.min_notional,0)>0
          AND floor(
            GREATEST(0,e.gross_entry-e.entry_base_fee-e.gross_exit-e.exit_base_fee)
            / i.lot_size
          )*i.lot_size*i.price < i.min_notional
        ) THEN 'TERMINAL_DUST_CLOSE'
      ELSE 'PARTIAL_REDUCTION'
    END classification
  FROM affected a JOIN evidence e ON e.position_id=a.position_id
  LEFT JOIN instrument i ON i.position_id=a.position_id
), mutated AS (
  UPDATE positions p SET
    gross_entry_executed_qty=c.gross_entry,
    entry_base_fee_qty=c.entry_base_fee,
    net_entry_inventory_qty=c.net_entry,
    cumulative_exit_executed_qty=c.gross_exit,
    exit_inventory_reduction_qty=c.exit_reduction,
    remaining_inventory_qty=c.remaining,
    qty=c.remaining,
    inventory_evidence_status=CASE
      WHEN c.classification='INCOMPLETE_EVIDENCE' THEN 'INCOMPLETE'
      ELSE 'COMPLETE' END,
    status=CASE WHEN c.classification IN
      ('FULLY_EXECUTED_CLOSE','TERMINAL_DUST_CLOSE') THEN 'CLOSED' ELSE 'OPEN' END,
    terminal_dust_qty=CASE WHEN c.classification='TERMINAL_DUST_CLOSE'
      THEN c.remaining ELSE NULL END,
    terminal_reason=CASE WHEN c.classification='TERMINAL_DUST_CLOSE'
      THEN 'TERMINAL_DUST' ELSE NULL END,
    exit_price=CASE WHEN c.classification IN
      ('FULLY_EXECUTED_CLOSE','TERMINAL_DUST_CLOSE') THEN c.avg_exit_price
      ELSE p.exit_price END,
    exit_time=CASE WHEN c.classification IN
      ('FULLY_EXECUTED_CLOSE','TERMINAL_DUST_CLOSE') THEN c.exit_time
      ELSE p.exit_time END,
    exit_reason=CASE WHEN c.classification='TERMINAL_DUST_CLOSE'
      THEN 'TERMINAL_DUST'
      WHEN c.classification='FULLY_EXECUTED_CLOSE'
      THEN COALESCE(NULLIF(p.exit_reason,''),'RECONCILED_OKX_EXIT_FILL')
      ELSE p.exit_reason END,
    exit_order_id=COALESCE(p.exit_order_id,c.order_id),
    exit_client_order_id=COALESCE(NULLIF(p.exit_client_order_id,''),c.clordid),
    inventory_contract_adoption_id=COALESCE(
      p.inventory_contract_adoption_id,c.adoption_id
    ),
    inventory_contract_generation=COALESCE(
      p.inventory_contract_generation,c.generation
    ),
    inventory_calculated_at=clock_timestamp()
  FROM classified c
  WHERE p.id=c.position_id AND p.status='OPEN'
    AND c.classification<>'INCOMPLETE_EVIDENCE'
  RETURNING p.id,c.order_id,c.gross_entry,c.entry_base_fee,c.net_entry,
    c.gross_exit,c.remaining,c.lot_size,c.min_size,c.min_notional,
    c.classification
)
INSERT INTO position_lifecycle_events_c2_2(
  position_id,order_id,mutation_kind,mutation_high_water,payload
)
SELECT id,order_id,
  CASE classification
    WHEN 'TERMINAL_DUST_CLOSE' THEN 'POSITION_CLOSED_TERMINAL_DUST'
    WHEN 'FULLY_EXECUTED_CLOSE' THEN 'POSITION_CLOSED'
    ELSE 'POSITION_REDUCED' END,
  gross_exit,
  jsonb_build_object(
    'position_id',id,'order_id',order_id,
    'gross_entry_executed_qty',gross_entry,
    'entry_base_fee_qty',entry_base_fee,
    'net_entry_inventory_qty',net_entry,
    'cumulative_exit_executed_qty',gross_exit,
    'remaining_inventory_qty',remaining,
    'terminal_dust_qty',CASE WHEN classification='TERMINAL_DUST_CLOSE'
      THEN remaining ELSE 0 END,
    'dust_qty',CASE WHEN classification='TERMINAL_DUST_CLOSE'
      THEN remaining ELSE 0 END,
    'lotSz',lot_size,'minSz',min_size,'min_notional',min_notional,
    'terminal_reason',CASE WHEN classification='TERMINAL_DUST_CLOSE'
      THEN 'TERMINAL_DUST' ELSE NULL END,
    'financial_truth_status','UNKNOWN'
  )
FROM mutated
WHERE classification<>'INCOMPLETE_EVIDENCE'
ON CONFLICT DO NOTHING
"""


def _mk_dsn(*, host: str, port: int, dbname: str, user: str, password: str) -> str:
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"



def _to_decimal_or_none(v):
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None

def _base_asset_from_symbol(symbol: str) -> str:
    s = str(symbol or "").upper()
    for q in ("USDC", "USDT", "USD", "EUR"):
        if s.endswith(q):
            return s[:-len(q)]
    return s[:3]

def _quote_notional_usdc(symbol: str, qty, price, quote_qty=None):
    q = _to_decimal_or_none(quote_qty)
    if q is not None:
        return q
    q_qty = _to_decimal_or_none(qty)
    q_price = _to_decimal_or_none(price)
    if q_qty is None or q_price is None:
        return Decimal("0")
    return abs(q_qty * q_price)

def _commission_usdc(symbol: str, commission, commission_asset, price):
    c = _to_decimal_or_none(commission)
    if c is None:
        return None

    asset = str(commission_asset or "").upper()
    if asset in ("USDC", "USDT", "USD"):
        return abs(c)

    base = _base_asset_from_symbol(symbol)
    px = _to_decimal_or_none(price)
    if asset == base and px is not None:
        return abs(c * px)

    return None



def reconcile_okx_exit_fills(
    conn,
    *,
    source: str,
    since_ms: int,
    changed_order_ids: Iterable[str] = (),
    environment: str | None = None,
    deployment_id: str | None = None,
) -> int:
    """
    Reconcile accepted OKX SELL fills back into positions SSOT.
    """
    if str(source).lower() != "okx":
        return 0
    changed_orders = [str(value) for value in changed_order_ids]
    if not changed_orders:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.position_lifecycle_events_c2_2')"
        )
        c2_2_ready = cur.fetchone()[0] is not None
        cur.execute(
            (
                RECONCILE_OKX_EXIT_FILLS_C2_2_SQL
                if c2_2_ready else RECONCILE_OKX_EXIT_FILLS_SQL
            ),
            (
                (
                    str(source).lower(),
                    int(since_ms),
                    str(environment or os.getenv("ENVIRONMENT", "")).lower(),
                    str(
                        deployment_id
                        or os.getenv("DEPLOYMENT_ID")
                        or os.getenv("WALTRADE_DEPLOYMENT_ID", "")
                    ),
                    changed_orders,
                )
                if c2_2_ready
                else (str(source).lower(), int(since_ms))
            ),
        )
        return int(cur.rowcount or 0)


def reconcile_live_terminal_financial_truth(
    conn,
    *,
    changed_order_ids: Iterable[str],
    environment: str,
    deployment_id: str,
    source: str,
) -> int:
    """
    Write canonical Financial Truth for terminal LIVE positions whose exit fills
    were reconciled in the caller-owned transaction.

    OPEN/PARTIAL positions are ignored. A terminal position without COMPLETE
    canonical evidence fails the caller transaction closed.
    """
    normalized_environment = str(environment or "").strip().lower()
    normalized_deployment = str(deployment_id or "").strip()
    normalized_source = str(source or "").strip().lower()
    order_ids = sorted({str(value) for value in changed_order_ids if str(value)})

    if normalized_source != "okx" or not order_ids:
        return 0
    if normalized_environment != "live":
        raise RuntimeError("LIVE_FINANCIAL_TRUTH_ENVIRONMENT_INVALID")
    if not normalized_deployment:
        raise RuntimeError("LIVE_FINANCIAL_TRUTH_DEPLOYMENT_REQUIRED")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              p.id,
              p.remaining_inventory_qty,
              p.terminal_dust_qty,
              p.terminal_reason,
              p.inventory_evidence_status
            FROM positions p
            WHERE p.status='CLOSED'
              AND EXISTS (
                SELECT 1
                FROM binance_orders bo
                WHERE (
                  bo.position_id=p.id
                  OR bo.reconciled_position_id=p.id
                )
                  AND bo.order_id=ANY(%s)
                  AND bo.order_purpose='EXIT'
                  AND lower(COALESCE(bo.exchange_source,''))=%s
              )
            ORDER BY p.id
            FOR UPDATE OF p
            """,
            (order_ids, normalized_source),
        )
        terminal_rows = list(cur.fetchall())

    if not terminal_rows:
        return 0

    context = ExecutionEvidenceContext(
        environment=normalized_environment,
        exchange=normalized_source.upper(),
        deployment_id=normalized_deployment,
    )
    source_repository = FinancialTruthSourceRepository(lambda: conn)
    written_count = 0

    for (
        position_id,
        remaining_inventory_qty,
        terminal_dust_qty,
        terminal_reason,
        inventory_evidence_status,
    ) in terminal_rows:
        position_id = int(position_id)

        if str(inventory_evidence_status or "") != "COMPLETE":
            raise RuntimeError(
                "LIVE_FINANCIAL_TRUTH_INVENTORY_EVIDENCE_INCOMPLETE:"
                + str(position_id)
            )

        remaining = Decimal(str(remaining_inventory_qty or 0))
        dust = Decimal(str(terminal_dust_qty or 0))

        if remaining < 0:
            raise RuntimeError(
                "LIVE_FINANCIAL_TRUTH_NEGATIVE_CANONICAL_REMAINING:"
                + str(position_id)
            )

        if remaining == 0:
            inventory_classification = ExitInventoryClassification(
                ExitInventoryStatus.FULLY_EXECUTED_CLOSE,
                Decimal("0"),
                Decimal("0"),
                Decimal("0"),
                None,
            )
        else:
            if dust != remaining:
                raise RuntimeError(
                    "LIVE_FINANCIAL_TRUTH_TERMINAL_DUST_CONFLICT:"
                    + str(position_id)
                )
            inventory_classification = ExitInventoryClassification(
                ExitInventoryStatus.TERMINAL_DUST_CLOSE,
                remaining,
                Decimal("0"),
                remaining,
                str(terminal_reason or "TERMINAL_DUST"),
            )

        with conn.cursor() as cur:
            CanonicalFinancialTruthWriteRepository.lock_position(
                cur, position_id
            )
            position, fills, source_issue = (
                source_repository.read_position_and_fills(
                    position_id,
                    context=context,
                    connection=conn,
                )
            )
            if source_issue is not None:
                raise RuntimeError(
                    "LIVE_FINANCIAL_TRUTH_SOURCE_"
                    + str(source_issue.value)
                )

            financial_truth = calculate_financial_truth(
                position_id=position[0],
                position_status=position[1],
                fills=fills,
                estimated_gross_pnl=position[2],
                estimated_fees_usdc=position[3],
                estimated_net_pnl=position[4],
                position_symbol=position[5],
                inventory_classification=inventory_classification,
            )
            if financial_truth.financial_truth_status != "COMPLETE":
                raise RuntimeError(
                    "LIVE_FINANCIAL_TRUTH_NOT_COMPLETE:"
                    + str(
                        financial_truth.failure_detail
                        or financial_truth.failure_code
                        or financial_truth.financial_truth_status
                    )
                )

            written = CanonicalFinancialTruthWriteRepository.write(
                cur,
                financial_truth,
                invocation_type="RUNTIME_LIVE_EXIT",
                invocation_identity=(
                    f"LIVE:{normalized_deployment}:"
                    f"{position_id}:{financial_truth.source_fingerprint}"
                ),
            )
            written_count += int(bool(written))

            cur.execute(
                """
                SELECT financial_truth_status
                FROM canonical_financial_truth_v1
                WHERE position_id=%s
                """,
                (position_id,),
            )
            if cur.fetchone() != ("COMPLETE",):
                raise RuntimeError(
                    "LIVE_FINANCIAL_TRUTH_POSTCONDITION_FAILED:"
                    + str(position_id)
                )

    return written_count



def _trade_to_row(symbol: str, t: Dict[str, Any], fill_idx: int = 0, source: str = "binance") -> Dict[str, Any]:
    # myTrades: id, orderId, price, qty, quoteQty, commission, commissionAsset, time, isBuyer, isMaker
    trade_id = t.get("id")
    order_id = t.get("orderId")
    if trade_id is None or not str(trade_id).strip():
        raise ValueError("exchange trade id is required")
    if order_id is None or not str(order_id).strip():
        raise ValueError("exchange order id is required")
    side = "BUY" if t.get("isBuyer") else "SELL"
    role = "MAKER" if t.get("isMaker") else "TAKER"
    raw = t.get("raw") if isinstance(t.get("raw"), dict) else {}
    client_order_id = (
        t.get("clientOrderId")
        or t.get("client_order_id")
        or raw.get("clOrdId")
        or None
    )
    return {
        "source": normalize_exchange_source(source),
        "trade_id": str(trade_id),
        "order_id": str(order_id),
        "client_order_id": (
            str(client_order_id) if client_order_id is not None else None
        ),
        "symbol": symbol,
        "side": side,
        "role": role,
        "executed_qty": t.get("qty"),
        "avg_price": t.get("price"),
        "quote_notional_usdc": _quote_notional_usdc(symbol, t.get("qty"), t.get("price"), t.get("quoteQty")),
        "commission_amount": t.get("commission"),
        "commission_asset": t.get("commissionAsset"),
        "commission_usdc": _commission_usdc(symbol, t.get("commission"), t.get("commissionAsset"), t.get("price")),
        "event_time_ms": int(t.get("time")),
        "fill_idx": int(fill_idx),
        "raw": json.dumps(t),
        "account_identity_id": None,
        "instrument_snapshot_id": None,
        "account_identity_status": "MISSING",
        "account_identity_failure_code": "ACCOUNT_IDENTITY_UNAVAILABLE",
    }


def _record_lei1c_observations(
    conn,
    *,
    dsn: str,
    rows: Iterable[Dict[str, Any]],
    forward_boundary_ms: int | None = None,
) -> None:
    """Dormant-by-default forward producer for immutable LEI1C evidence."""
    mode = EntryFillAttributionMode.from_env()
    if mode is EntryFillAttributionMode.OFF:
        return
    try:
        environment = str(os.getenv("ENVIRONMENT") or "").strip().lower()
        deployment_id = str(
            os.getenv("DEPLOYMENT_ID")
            or os.getenv("WALTRADE_DEPLOYMENT_ID")
            or ""
        ).strip().lower()
        git_revision = require_runtime_git_revision()
        # Keep optional SHADOW schema/read failures off the legacy ingest
        # transaction.  A PostgreSQL error on ``conn`` would otherwise leave
        # that transaction aborted even after a Python-level catch.
        with psycopg2.connect(dsn) as attribution_conn:
            with attribution_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT adoption.adoption_id,adoption.generation,
                           (
                             SELECT min(ledger.applied_at)
                             FROM schema_migration_ledger_v1 ledger
                             WHERE ledger.migration_id=%s
                               AND ledger.checksum_sha256=%s
                               AND ledger.status='APPLIED'
                               AND ledger.success=true
                           ) AS contract_activated_at,
                           adoption.git_revision
                    FROM runtime_contract_adoption_v2 adoption
                    WHERE adoption.contract_name='FEE_AWARE_INVENTORY_C2_2'
                      AND adoption.status='ACTIVE'
                      AND adoption.environment=%s
                      AND adoption.deployment_id=%s
                    ORDER BY adoption.adoption_id
                    """,
                    (
                        LEI1C_MIGRATION_ID,
                        LEI1C_LEDGER_CHECKSUM,
                        environment,
                        deployment_id,
                    ),
                )
                adoptions = list(cur.fetchall())
    except Exception:
        if mode is EntryFillAttributionMode.ENFORCE:
            raise
        logging.exception(
            "ENTRY_FILL_ATTRIBUTION|mode=%s error=SETUP_FAILED",
            mode.value,
        )
        return
    if len(adoptions) != 1:
        message = "LEI1C_ACTIVE_ADOPTION_NOT_UNIQUE"
        if mode is EntryFillAttributionMode.ENFORCE:
            raise RuntimeError(message)
        logging.error("ENTRY_FILL_ATTRIBUTION|mode=%s error=%s", mode.value, message)
        return
    (
        adoption_id,
        generation,
        contract_activated_at,
        adoption_git_revision,
    ) = adoptions[0]
    log_runtime_revision_provenance_diagnostic(
        adoption_id=int(adoption_id),
        generation=int(generation),
        adoption_git_revision=str(adoption_git_revision),
        runtime_git_revision=git_revision,
    )
    if (
        not isinstance(contract_activated_at, datetime)
        or contract_activated_at.tzinfo is None
        or contract_activated_at.utcoffset() is None
    ):
        message = "LEI1C_ACTIVATION_BOUNDARY_MISSING"
        if mode is EntryFillAttributionMode.ENFORCE:
            raise RuntimeError(message)
        logging.error("ENTRY_FILL_ATTRIBUTION|mode=%s error=%s", mode.value, message)
        return
    activated_utc = contract_activated_at.astimezone(timezone.utc)
    since_epoch = activated_utc - datetime(1970, 1, 1, tzinfo=timezone.utc)
    contract_boundary_ms = (
        since_epoch.days * 86_400_000
        + since_epoch.seconds * 1_000
        + (since_epoch.microseconds + 999) // 1_000
    )
    effective_boundary_ms = max(
        contract_boundary_ms,
        int(forward_boundary_ms)
        if forward_boundary_ms is not None else contract_boundary_ms,
    )
    try:
        repository = EntryFillAttributionRepository(
            lambda: psycopg2.connect(dsn)
        )
    except Exception:
        if mode is EntryFillAttributionMode.ENFORCE:
            raise
        logging.exception(
            "ENTRY_FILL_ATTRIBUTION|mode=%s error=REPOSITORY_INIT_FAILED",
            mode.value,
        )
        return
    for row in rows:
        try:
            raw = row.get("raw")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    raw = {"unparsed": raw}
            exchange_source = str(row.get("source") or "").lower()
            exchange_trade_id = row.get("trade_id")
            event_time_ms = int(row.get("event_time_ms"))
            existing_evidence = repository.load_evidence(
                (
                    environment,
                    deployment_id,
                    exchange_source,
                    str(exchange_trade_id),
                )
            )
            if (
                event_time_ms < effective_boundary_ms
                and existing_evidence is None
            ):
                logging.info(
                    "ENTRY_FILL_ATTRIBUTION|mode=%s outcome=PRE_BOUNDARY_SKIPPED "
                    "trade_id=%s boundary_ms=%s",
                    mode.value,
                    exchange_trade_id,
                    effective_boundary_ms,
                )
                continue
            observed_at = datetime.now(timezone.utc)

            def build_observation(
                *,
                context_environment: str,
                context_deployment_id: str,
                context_adoption_id: int,
                context_generation: int,
                context_git_revision: str,
            ) -> EntryFillObservation:
                return EntryFillObservation.build(
                    environment=context_environment,
                    deployment_id=context_deployment_id,
                    adoption_id=context_adoption_id,
                    generation=context_generation,
                    git_revision=context_git_revision,
                    exchange_source=exchange_source,
                    exchange_trade_id=exchange_trade_id,
                    exchange_order_id=row.get("order_id"),
                    client_order_id=row.get("client_order_id"),
                    symbol=str(row.get("symbol") or ""),
                    side=str(row.get("side") or ""),
                    executed_qty=row.get("executed_qty"),
                    price=row.get("avg_price"),
                    notional=row.get("quote_notional_usdc"),
                    fee=(
                        row.get("commission_amount")
                        if row.get("commission_amount") is not None else "0"
                    ),
                    fee_asset=row.get("commission_asset"),
                    executed_at=(
                        datetime(1970, 1, 1, tzinfo=timezone.utc)
                        + timedelta(microseconds=event_time_ms * 1_000)
                    ),
                    observed_at=observed_at,
                    producer_identity="exchange-fill-ingest-v1",
                    source_payload=(raw if isinstance(raw, dict) else {}),
                )

            if existing_evidence is not None:
                context = existing_evidence.observation
                observation = build_observation(
                    context_environment=context.environment.value,
                    context_deployment_id=context.deployment_id.value,
                    context_adoption_id=context.adoption_id,
                    context_generation=context.generation,
                    context_git_revision=context.git_revision,
                )
            else:
                preliminary = build_observation(
                    context_environment=environment,
                    context_deployment_id=deployment_id,
                    context_adoption_id=int(adoption_id),
                    context_generation=int(generation),
                    context_git_revision=git_revision,
                )
                context = repository.resolve_observation_context(preliminary)
                observation = build_observation(
                    context_environment=context.environment.value,
                    context_deployment_id=context.deployment_id.value,
                    context_adoption_id=context.adoption_id,
                    context_generation=context.generation,
                    context_git_revision=context.git_revision,
                )
            result = process_entry_fill_attribution(
                mode=mode,
                observation=observation,
                repository=repository,
            )
            if (
                mode is EntryFillAttributionMode.ENFORCE
                and result.attribution_status is not None
            ):
                row["_lei1c_attribution_status"] = (
                    result.attribution_status.value
                )
            logging.info(
                "ENTRY_FILL_ATTRIBUTION|mode=%s outcome=%s trade_id=%s "
                "attribution=%s application=%s",
                mode.value,
                result.outcome.value,
                observation.exchange_trade_id,
                (
                    result.attribution_status.value
                    if result.attribution_status else None
                ),
                (
                    result.application_status.value
                    if result.application_status else None
                ),
            )
            if mode is EntryFillAttributionMode.ENFORCE and result.outcome in {
                EntryFillProcessingOutcome.REPOSITORY_ERROR,
                EntryFillProcessingOutcome.IDEMPOTENCY_CONFLICT,
                EntryFillProcessingOutcome.AMBIGUOUS,
                EntryFillProcessingOutcome.CORRECTION_PENDING,
            }:
                raise RuntimeError(
                    result.error_code or f"LEI1C_{result.outcome.value}"
                )
        except Exception:
            if mode is EntryFillAttributionMode.ENFORCE:
                raise
            logging.exception(
                "ENTRY_FILL_ATTRIBUTION|mode=%s error=ROW_FAILED trade_id=%s",
                mode.value,
                row.get("trade_id"),
            )


def _persist_okx_identity_snapshot(conn, client):
    try:
        identity, diagnostic = client.get_account_identity()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO financial_truth_account_identity_v1 (
                  source_authority,exchange,account_uid,main_account_uid,
                  account_scope,identity_source,identity_version,
                  identity_fingerprint,captured_at
                ) VALUES (
                  'EXCHANGE_EXECUTION',%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (identity_fingerprint) DO UPDATE
                  SET identity_fingerprint=EXCLUDED.identity_fingerprint
                RETURNING id
                """,
                (
                    identity.exchange, identity.uid, identity.main_uid,
                    identity.scope, identity.source, identity.version,
                    identity.fingerprint, identity.captured_at,
                ),
            )
            identity_id = int(cur.fetchone()[0])
        logging.info(
            "FINANCIAL_TRUTH_IDENTITY|status=%s scope=%s uid=***%s",
            diagnostic, identity.scope, identity.uid[-4:],
        )
        return identity_id, "VERIFIED", None
    except Exception as exc:
        code = str(exc)
        if "ACCOUNT_IDENTITY_" not in code:
            code = "ACCOUNT_IDENTITY_UNAVAILABLE"
        logging.warning("FINANCIAL_TRUTH_IDENTITY|status=%s", code)
        return None, "MISSING", code


def _persist_instrument_snapshot(conn, client, symbol: str, source: str):
    try:
        from common.simulated_execution_evidence import (
            INSTRUMENT_METADATA_VERSION,
            _assets,
            _hash,
            _instrument_values,
        )
        values = _instrument_values(client, symbol)
        if values is None:
            return None
        step, min_qty, min_notional, qty_precision, price_precision = values
        base_asset, quote_asset = _assets(symbol)
        payload = {
            "source_authority": "EXCHANGE_EXECUTION", "exchange": source.upper(),
            "symbol": symbol, "base_asset": base_asset,
            "quote_asset": quote_asset, "step_size": str(step),
            "min_qty": str(min_qty), "min_notional": str(min_notional),
            "quantity_precision": qty_precision, "price_precision": price_precision,
            "source": "EXCHANGE_PUBLIC_AT_EXECUTION",
            "version": INSTRUMENT_METADATA_VERSION,
        }
        fingerprint = _hash(payload)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO financial_truth_instrument_snapshot_v1 (
                  source_authority,exchange,symbol,base_asset,quote_asset,
                  step_size,min_qty,quantity_precision,price_precision,
                  min_notional,metadata_source,metadata_version,
                  metadata_fingerprint,captured_at
                ) VALUES (
                  'EXCHANGE_EXECUTION',%s,%s,%s,%s,%s,%s,%s,%s,%s,
                  'EXCHANGE_PUBLIC_AT_EXECUTION',%s,%s,clock_timestamp()
                )
                ON CONFLICT (metadata_fingerprint) DO UPDATE
                  SET metadata_fingerprint=EXCLUDED.metadata_fingerprint
                RETURNING id
                """,
                (
                    source.upper(), symbol, base_asset, quote_asset, step,
                    min_qty, qty_precision, price_precision, min_notional,
                    INSTRUMENT_METADATA_VERSION, fingerprint,
                ),
            )
            return int(cur.fetchone()[0])
    except Exception:
        logging.warning(
            "FINANCIAL_TRUTH_INSTRUMENT|status=SNAPSHOT_UNAVAILABLE symbol=%s",
            symbol,
        )
        return None


def ingest_my_trades(
    *,
    client: Any,
    symbols: Iterable[str],
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_pass: str,
    lookback_ms_default: int = 7 * 24 * 3600 * 1000,
    api_limit: int = 1000,
) -> FillIngestResult:
    """
    Ingestuje fill-level z configured exchange (client.get_my_trades) do binance_order_fills.
    Historyczna nazwa tabeli zostaje, ale source musi być exchange-neutral:
    binance / okx / ...
    Idempotencja: UNIQUE(source, trade_id).

    Returns: (n_trades_fetched, n_fee_rows_priced)
    """
    mode = trading_mode()
    if mode != "LIVE":
        logging.info(
            "EXCHANGE_INGEST|status=NOT_APPLICABLE trading_mode=%s",
            mode,
        )
        return FillIngestResult(
            0,
            0,
            status="NOT_APPLICABLE",
            ran=False,
            applicable=False,
        )

    dsn = _mk_dsn(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass)
    now_ms = int(time.time() * 1000)
    source = normalize_exchange_source(os.getenv("EXCHANGE", "BINANCE") or "BINANCE")

    total_fetched = 0
    min_event_time_ms_seen: Optional[int] = None
    accepted_changes = []
    converged_partial_links = 0
    changed_exit_order_ids: set[str] = set()

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False
        identity_id, identity_status, identity_failure = (
            _persist_okx_identity_snapshot(conn, client)
            if source == "okx"
            else (None, "MISSING", "UNSUPPORTED_EXCHANGE_IDENTITY")
        )

        for symbol in symbols:
            instrument_snapshot_id = _persist_instrument_snapshot(
                conn, client, symbol, source
            )
            with conn.cursor() as cur:
                state_symbol = f"{source}:{symbol}"
                cur.execute(READ_STATE_SQL, (state_symbol,))
                row = cur.fetchone()
                if row is None:
                    start_ms = now_ms - lookback_ms_default
                    cur.execute(UPSERT_STATE_SQL, (state_symbol, start_ms))
                else:
                    start_ms = int(row[0])

            fetch_start = start_ms + 1  # anti-dup

            try:
                trades = client.get_my_trades(
                    symbol=symbol,
                    startTime=fetch_start,
                    correctionLookbackMs=lookback_ms_default,
                    limit=api_limit,
                )
            except Exception as e:
                logging.exception("EXCHANGE_INGEST|get_my_trades failed symbol=%s err=%s", symbol, str(e))
                continue

            if not trades:
                continue

            logging.info(
                "EXCHANGE_INGEST|fetch_boundary symbol=%s applied=%s mode=%s "
                "requested=%s effective=%s",
                symbol,
                getattr(trades, "filter_applied", False),
                getattr(trades, "filter_mode", "UNDECLARED"),
                getattr(trades, "requested_boundary", fetch_start),
                getattr(trades, "effective_boundary", None),
            )

            rows = [_trade_to_row(symbol, t, fill_idx=0, source=source) for t in trades]
            for item in rows:
                item.update({
                    "account_identity_id": identity_id,
                    "instrument_snapshot_id": instrument_snapshot_id,
                    "account_identity_status": identity_status,
                    "account_identity_failure_code": identity_failure,
                    "environment": os.getenv("ENVIRONMENT", ""),
                    "deployment_id": (
                        os.getenv("DEPLOYMENT_ID")
                        or os.getenv("WALTRADE_DEPLOYMENT_ID", "")
                    ),
                })
            _record_lei1c_observations(
                conn,
                dsn=dsn,
                rows=rows,
                forward_boundary_ms=fetch_start,
            )
            total_fetched += len(rows)

            accepted_rows = []
            with conn.cursor() as cur:
                for item in rows:
                    change = register_fill_change(
                        cur,
                        item,
                        account_identity_key=(
                            str(identity_id)
                            if identity_id is not None
                            else f"{source}:ACCOUNT_IDENTITY_MISSING"
                        ),
                    )
                    if (
                        change.application_status
                        is FillApplicationClassification.TRUE_DUPLICATE_APPLIED
                        and converge_partial_applied_recovery_link(
                            cur, ingestion_id=change.ingestion_id
                        )
                    ):
                        converged_partial_links += 1
                    if change.permits_mutation:
                        attribute_fill_change_position(cur, item, change)
                        accepted_rows.append(item)
                        accepted_changes.append(change)
                        if str(item["side"]).upper() == "SELL":
                            changed_exit_order_ids.add(str(item["order_id"]))
                    elif (
                        change.decision
                        is FillMutationDecision.AMBIGUOUS_CORRECTION
                    ):
                        logging.error(
                            "EXCHANGE_INGEST|decision=%s symbol=%s trade_id=%s",
                            change.decision.value, symbol, item["trade_id"],
                        )

            if not accepted_rows:
                continue

            min_t = min(r["event_time_ms"] for r in accepted_rows)
            min_event_time_ms_seen = min_t if min_event_time_ms_seen is None else min(min_event_time_ms_seen, min_t)

            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_TRADE_SQL, accepted_rows, page_size=500)
                execute_batch(
                    cur,
                    """
                    UPDATE binance_orders
                    SET account_identity_id=%(account_identity_id)s,
                        instrument_snapshot_id=%(instrument_snapshot_id)s,
                        account_identity_status=%(account_identity_status)s,
                        account_identity_failure_code=%(account_identity_failure_code)s
                    WHERE exchange_source=%(source)s
                      AND symbol=%(symbol)s AND order_id=%(order_id)s
                      AND account_identity_id IS NULL
                    """,
                    accepted_rows,
                    page_size=500,
                )

            max_time = max(r["event_time_ms"] for r in accepted_rows)
            with conn.cursor() as cur:
                cur.execute(UPSERT_STATE_SQL, (state_symbol, max_time))

        priced_updated = 0
        reconciled_entries = 0
        reconciled_exits = 0
        if min_event_time_ms_seen is not None:
            with conn.cursor() as cur:
                cur.execute(PRICE_FEES_SQL, (min_event_time_ms_seen,))
                priced_updated = cur.rowcount

        # LEI1D ENFORCE owns the fill-to-position boundary.  OFF/SHADOW retain
        # the legacy reconciler exactly; there are never two active projectors.
        lei1d_mode = EntryPositionProjectionMode.from_env()
        if lei1d_mode is EntryPositionProjectionMode.ENFORCE:
            projection_stats = run_entry_position_projection(
                conn, mode=lei1d_mode, batch_size=100,
            )
            reconciled_entries = projection_stats.opened + projection_stats.updated
            logging.info(
                "EXCHANGE_INGEST|LEI1D mode=%s scanned=%s opened=%s "
                "updated=%s no_op=%s blocked=%s",
                lei1d_mode.value, projection_stats.scanned,
                projection_stats.opened, projection_stats.updated,
                projection_stats.no_op, projection_stats.blocked,
            )
        else:
            # The due gate runs even when this ingest fetched no new fills, so
            # bounded backlog cannot depend on future exchange activity.
            with conn.cursor() as cur:
                cur.execute("SAVEPOINT pending_entry_fill_batch")
            try:
                entry_run = run_pending_entry_reconciliation_if_due(
                    conn,
                    batch_size=100,
                    trading_mode=mode,
                )
                with conn.cursor() as cur:
                    cur.execute("RELEASE SAVEPOINT pending_entry_fill_batch")
                entry_stats = entry_run.stats
                reconciled_entries = entry_stats.created + entry_stats.updated
                logging.info(
                    "EXCHANGE_INGEST|entry fill reconciliation "
                    "status=%s ran=%s scanned=%s created=%s updated=%s already=%s "
                    "recovered=%s ambiguous=%s alarms=%s failed=%s has_more=%s",
                    entry_run.status,
                    entry_run.ran,
                    entry_stats.scanned,
                    entry_stats.created,
                    entry_stats.updated,
                    entry_stats.already_reconciled,
                    entry_stats.recovered,
                    entry_stats.ambiguous,
                    entry_stats.alarms,
                    entry_stats.failed,
                    entry_stats.has_more,
                )
            except Exception:
                with conn.cursor() as cur:
                    cur.execute("ROLLBACK TO SAVEPOINT pending_entry_fill_batch")
                    cur.execute("RELEASE SAVEPOINT pending_entry_fill_batch")
                logging.exception(
                    "EXCHANGE_INGEST|entry fill reconciliation failed source=%s",
                    source,
                )

        live_ft_written = 0
        if min_event_time_ms_seen is not None:
            try:
                runtime_environment = os.getenv("ENVIRONMENT") or os.getenv(
                    "TRADING_MODE", ""
                )
                runtime_deployment = (
                    os.getenv("DEPLOYMENT_ID")
                    or os.getenv("WALTRADE_DEPLOYMENT_ID")
                    or ""
                )
                reconciled_exits = reconcile_okx_exit_fills(
                    conn,
                    source=source,
                    since_ms=min_event_time_ms_seen,
                    changed_order_ids=changed_exit_order_ids,
                    environment=runtime_environment,
                    deployment_id=runtime_deployment,
                )
                live_ft_written = reconcile_live_terminal_financial_truth(
                    conn,
                    changed_order_ids=changed_exit_order_ids,
                    environment=runtime_environment,
                    deployment_id=runtime_deployment,
                    source=source,
                )
            except Exception:
                logging.exception(
                    "LIVE_FINANCIAL_TRUTH_RECONCILIATION_FAILED|source=%s",
                    source,
                )
                raise

        with conn.cursor() as cur:
            for change in accepted_changes:
                mark_fill_change_applied(cur, change)
        conn.commit()

    if converged_partial_links:
        logging.warning(
            "EXCHANGE_INGEST|partial recovery linkage converged=%s",
            converged_partial_links,
        )

    if reconciled_exits:
        logging.warning(
            "EXCHANGE_INGEST|reconciled %s OKX exit fill(s) into positions SSOT",
            reconciled_exits,
        )
    if live_ft_written:
        logging.warning(
            "EXCHANGE_INGEST|canonical LIVE Financial Truth written=%s",
            live_ft_written,
        )
    if reconciled_entries:
        logging.warning(
            "EXCHANGE_INGEST|reconciled %s entry fill aggregate(s) into positions SSOT",
            reconciled_entries,
        )

    return FillIngestResult(total_fetched, priced_updated)

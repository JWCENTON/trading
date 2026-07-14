# common/exchange_ingest_trades.py
import json
import time
import logging
import os
from decimal import Decimal
from common.exchange_client import get_market_data_client
from typing import Iterable, Dict, Any, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_batch

from common.entry_fill_reconciliation import run_pending_entry_reconciliation_if_due
from common.exchange_identity import normalize_exchange_source


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
  raw
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
  %(raw)s::jsonb
)
ON CONFLICT (source, trade_id) DO NOTHING;
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
), candidates AS (
  SELECT
    p.id AS position_id,
    s.order_id,
    s.clordid,
    s.exit_time,
    s.executed_qty,
    s.avg_exit_price,
    ROW_NUMBER() OVER (
      PARTITION BY p.id
      ORDER BY s.exit_time ASC, s.order_id ASC
    ) AS rn
  FROM sell_orders s
  JOIN positions p
    ON p.symbol = s.symbol
   AND p.status = 'OPEN'
   AND p.side = 'LONG'
   AND s.executed_qty >= (p.qty * 0.98)
   AND (
        p.exit_order_id = s.order_id
        OR regexp_replace(COALESCE(p.exit_client_order_id, ''), '[^A-Za-z0-9]', '', 'g') = COALESCE(s.clordid, '')
        OR COALESCE(s.clordid, '') ILIKE ('%%P' || p.id::text || 'X%%')
   )
)
UPDATE positions p
SET
  status = 'CLOSED',
  exit_price = c.avg_exit_price,
  exit_time = c.exit_time,
  exit_reason = COALESCE(NULLIF(p.exit_reason, ''), 'RECONCILED_OKX_EXIT_FILL'),
  exit_order_id = COALESCE(p.exit_order_id, c.order_id),
  exit_client_order_id = COALESCE(NULLIF(p.exit_client_order_id, ''), c.clordid),
  exit_hour_utc = EXTRACT(HOUR FROM c.exit_time)::smallint,
  exit_day_utc = c.exit_time::date,
  hold_minutes = COALESCE(p.hold_minutes, EXTRACT(EPOCH FROM (c.exit_time - p.entry_time)) / 60.0)
FROM candidates c
WHERE p.id = c.position_id
  AND c.rn = 1
  AND p.status = 'OPEN';
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



def reconcile_okx_exit_fills(conn, *, source: str, since_ms: int) -> int:
    """
    Reconcile accepted OKX SELL fills back into positions SSOT.
    """
    if str(source).lower() != "okx":
        return 0
    with conn.cursor() as cur:
        cur.execute(RECONCILE_OKX_EXIT_FILLS_SQL, (str(source).lower(), int(since_ms)))
        return int(cur.rowcount or 0)


def _trade_to_row(symbol: str, t: Dict[str, Any], fill_idx: int = 0, source: str = "binance") -> Dict[str, Any]:
    # myTrades: id, orderId, price, qty, quoteQty, commission, commissionAsset, time, isBuyer, isMaker
    side = "BUY" if t.get("isBuyer") else "SELL"
    role = "MAKER" if t.get("isMaker") else "TAKER"
    return {
        "source": normalize_exchange_source(source),
        "trade_id": int(t["id"]),
        "order_id": str(t["orderId"]),
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
    }


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
) -> Tuple[int, int]:
    """
    Ingestuje fill-level z configured exchange (client.get_my_trades) do binance_order_fills.
    Historyczna nazwa tabeli zostaje, ale source musi być exchange-neutral:
    binance / okx / ...
    Idempotencja: UNIQUE(source, trade_id).

    Returns: (n_trades_fetched, n_fee_rows_priced)
    """
    dsn = _mk_dsn(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass)
    now_ms = int(time.time() * 1000)
    source = normalize_exchange_source(os.getenv("EXCHANGE", "BINANCE") or "BINANCE")

    total_fetched = 0
    min_event_time_ms_seen: Optional[int] = None

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False

        for symbol in symbols:
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
                trades = client.get_my_trades(symbol=symbol, startTime=fetch_start, limit=api_limit)
            except Exception as e:
                logging.exception("EXCHANGE_INGEST|get_my_trades failed symbol=%s err=%s", symbol, str(e))
                continue

            if not trades:
                continue

            rows = [_trade_to_row(symbol, t, fill_idx=0, source=source) for t in trades]
            total_fetched += len(rows)

            min_t = min(r["event_time_ms"] for r in rows)
            min_event_time_ms_seen = min_t if min_event_time_ms_seen is None else min(min_event_time_ms_seen, min_t)

            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_TRADE_SQL, rows, page_size=500)

            max_time = max(r["event_time_ms"] for r in rows)
            with conn.cursor() as cur:
                cur.execute(UPSERT_STATE_SQL, (state_symbol, max_time))

        priced_updated = 0
        reconciled_entries = 0
        reconciled_exits = 0
        if min_event_time_ms_seen is not None:
            with conn.cursor() as cur:
                cur.execute(PRICE_FEES_SQL, (min_event_time_ms_seen,))
                priced_updated = cur.rowcount

        # The due gate runs even when this ingest fetched no new fills, so bounded
        # backlog and retryable rows cannot depend on future exchange activity.
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT pending_entry_fill_batch")
        try:
            entry_run = run_pending_entry_reconciliation_if_due(
                conn, batch_size=100
            )
            with conn.cursor() as cur:
                cur.execute("RELEASE SAVEPOINT pending_entry_fill_batch")
            entry_stats = entry_run.stats
            reconciled_entries = entry_stats.created + entry_stats.updated
            logging.info(
                "EXCHANGE_INGEST|entry fill reconciliation "
                "status=%s ran=%s scanned=%s created=%s updated=%s already=%s "
                "ambiguous=%s alarms=%s failed=%s has_more=%s",
                entry_run.status,
                entry_run.ran,
                entry_stats.scanned,
                entry_stats.created,
                entry_stats.updated,
                entry_stats.already_reconciled,
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

        if min_event_time_ms_seen is not None:
            try:
                reconciled_exits = reconcile_okx_exit_fills(conn, source=source, since_ms=min_event_time_ms_seen)
            except Exception:
                logging.exception("EXCHANGE_INGEST|exit fill reconciliation failed source=%s", source)

        conn.commit()

    if reconciled_exits:
        logging.warning("EXCHANGE_INGEST|reconciled %s OKX exit fill(s) into positions SSOT", reconciled_exits)
    if reconciled_entries:
        logging.warning(
            "EXCHANGE_INGEST|reconciled %s entry fill aggregate(s) into positions SSOT",
            reconciled_entries,
        )

    return total_fetched, priced_updated

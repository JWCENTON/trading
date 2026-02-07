# common/binance_ingest_trades.py
import json
import time
import logging
from typing import Iterable, Dict, Any, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_batch
from binance.client import Client


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
  event_time,
  fill_idx,
  raw
)
VALUES (
  'binance',
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


def _mk_dsn(*, host: str, port: int, dbname: str, user: str, password: str) -> str:
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _trade_to_row(symbol: str, t: Dict[str, Any], fill_idx: int = 0) -> Dict[str, Any]:
    # myTrades: id, orderId, price, qty, quoteQty, commission, commissionAsset, time, isBuyer, isMaker
    side = "BUY" if t.get("isBuyer") else "SELL"
    role = "MAKER" if t.get("isMaker") else "TAKER"
    return {
        "trade_id": int(t["id"]),
        "order_id": str(t["orderId"]),
        "symbol": symbol,
        "side": side,
        "role": role,
        "executed_qty": t.get("qty"),
        "avg_price": t.get("price"),
        "quote_notional_usdc": t.get("quoteQty"),
        "commission_amount": t.get("commission"),
        "commission_asset": t.get("commissionAsset"),
        "event_time_ms": int(t.get("time")),
        "fill_idx": int(fill_idx),
        "raw": json.dumps(t),
    }


def ingest_my_trades(
    *,
    client: Client,
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
    Ingestuje fill-level z Binance (client.get_my_trades) do binance_order_fills.
    Idempotencja: UNIQUE(source, trade_id).

    Returns: (n_trades_fetched, n_fee_rows_priced)
    """
    dsn = _mk_dsn(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass)
    now_ms = int(time.time() * 1000)

    total_fetched = 0
    min_event_time_ms_seen: Optional[int] = None

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = False

        for symbol in symbols:
            with conn.cursor() as cur:
                cur.execute(READ_STATE_SQL, (symbol,))
                row = cur.fetchone()
                if row is None:
                    start_ms = now_ms - lookback_ms_default
                    cur.execute(UPSERT_STATE_SQL, (symbol, start_ms))
                else:
                    start_ms = int(row[0])

            fetch_start = start_ms + 1  # anti-dup

            try:
                trades = client.get_my_trades(symbol=symbol, startTime=fetch_start, limit=api_limit)
            except Exception as e:
                logging.exception("BINANCE_INGEST|get_my_trades failed symbol=%s err=%s", symbol, str(e))
                continue

            if not trades:
                continue

            rows = [_trade_to_row(symbol, t, fill_idx=0) for t in trades]
            total_fetched += len(rows)

            min_t = min(r["event_time_ms"] for r in rows)
            min_event_time_ms_seen = min_t if min_event_time_ms_seen is None else min(min_event_time_ms_seen, min_t)

            with conn.cursor() as cur:
                execute_batch(cur, UPSERT_TRADE_SQL, rows, page_size=500)

            max_time = max(r["event_time_ms"] for r in rows)
            with conn.cursor() as cur:
                cur.execute(UPSERT_STATE_SQL, (symbol, max_time))

        priced_updated = 0
        if min_event_time_ms_seen is not None:
            with conn.cursor() as cur:
                cur.execute(PRICE_FEES_SQL, (min_event_time_ms_seen,))
                priced_updated = cur.rowcount

        conn.commit()

    return total_fetched, priced_updated

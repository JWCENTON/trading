import os
import time
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_batch
from binance.client import Client

from common.schema import ensure_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "trading")
DB_USER = os.environ.get("DB_USER", "botuser")
DB_PASS = os.environ.get("DB_PASS", "botpass")

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

QUOTE_ASSET = os.environ.get("QUOTE_ASSET", "USDC").upper()

REGIME_SYMBOLS = os.environ.get("REGIME_SYMBOLS", "BTCUSDC").strip()
MD_INTERVALS = os.environ.get("MD_INTERVALS", os.environ.get("REGIME_INTERVAL", "1m")).strip()

MD_SLEEP_SECONDS = int(os.environ.get("MD_SLEEP_SECONDS", "10"))
MD_MAX_LIMIT = int(os.environ.get("MD_MAX_LIMIT", "1000"))  # Binance max is 1000 for most endpoints
MD_BACKFILL_CANDLES = int(os.environ.get("MD_BACKFILL_CANDLES", "500"))  # per symbol/interval on cold start
MD_LAG_TOLERANCE_SECONDS = int(os.environ.get("MD_LAG_TOLERANCE_SECONDS", "120"))

client = Client(api_key=API_KEY, api_secret=API_SECRET)

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def parse_list(s: str):
    return [x.strip().upper() for x in s.split(",") if x.strip()]

def interval_to_ms(interval: str) -> int:
    # Binance intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d...
    unit = interval[-1]
    n = int(interval[:-1])
    if unit == "m":
        return n * 60_000
    if unit == "h":
        return n * 60 * 60_000
    if unit == "d":
        return n * 24 * 60 * 60_000
    if unit == "w":
        return n * 7 * 24 * 60 * 60_000
    raise ValueError(f"Unsupported interval={interval}")

def last_open_time(symbol: str, interval: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(open_time)
        FROM candles
        WHERE symbol=%s AND interval=%s;
        """,
        (symbol, interval),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

def insert_candles(rows):
    if not rows:
        return 0
    conn = get_db_conn()
    cur = conn.cursor()
    execute_batch(
        cur,
        """
        INSERT INTO candles (
            symbol, interval, open_time,
            open, high, low, close,
            volume, close_time, trades
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
        """,
        rows,
        page_size=200,
    )
    inserted = cur.rowcount  # may be -1 for some drivers; not strictly reliable, but ok for logs
    conn.commit()
    cur.close()
    conn.close()
    return inserted

def fetch_klines(symbol: str, interval: str, *, start_ms: int = None, limit: int = 1000):
    kwargs = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        kwargs["startTime"] = int(start_ms)
    klines = client.get_klines(**kwargs)

    rows = []
    for k in klines:
        rows.append((
            symbol,
            interval,
            datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            k[1], k[2], k[3], k[4],
            k[5],
            datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
            k[8],
        ))
    return rows, klines

def backfill_if_empty(symbol: str, interval: str):
    lot = last_open_time(symbol, interval)
    if lot is not None:
        return

    logging.info("MD|backfill start (empty)|%s|%s|candles=%d", symbol, interval, MD_BACKFILL_CANDLES)
    rows, _ = fetch_klines(symbol, interval, start_ms=None, limit=min(MD_BACKFILL_CANDLES, MD_MAX_LIMIT))
    insert_candles(rows)
    logging.info("MD|backfill done|%s|%s|fetched=%d", symbol, interval, len(rows))

def catch_up(symbol: str, interval: str):
    ms = interval_to_ms(interval)
    lot = last_open_time(symbol, interval)

    # If still empty (should be handled by backfill), do minimal fetch.
    if lot is None:
        backfill_if_empty(symbol, interval)
        lot = last_open_time(symbol, interval)
        if lot is None:
            return

    start_ms = int(lot.timestamp() * 1000) + ms  # next candle open_time
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # If we are basically up to date, skip.
    if now_ms - start_ms < max(MD_LAG_TOLERANCE_SECONDS * 1000, ms):
        return

    total = 0
    # loop until we catch up; guard with a max cycles per tick
    for _ in range(10):
        rows, klines = fetch_klines(symbol, interval, start_ms=start_ms, limit=MD_MAX_LIMIT)
        if not rows:
            break

        insert_candles(rows)
        total += len(rows)

        # Advance start_ms to after the last returned kline open_time
        last_k_open_ms = klines[-1][0]
        next_start = last_k_open_ms + ms

        # If Binance returned only 1 candle and it's the same start, avoid infinite loop
        if next_start <= start_ms:
            break

        start_ms = next_start

        if int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms < ms:
            break

    if total > 0:
        logging.info("MD|catchup|%s|%s|inserted~=%d", symbol, interval, total)

def validate_symbol(symbol: str):
    if not symbol.endswith(QUOTE_ASSET):
        raise RuntimeError(f"MD worker: symbol={symbol} does not match QUOTE_ASSET={QUOTE_ASSET}")

def main():
    ensure_schema()

    symbols = parse_list(REGIME_SYMBOLS)
    intervals = [x.strip() for x in MD_INTERVALS.split(",") if x.strip()]

    for s in symbols:
        validate_symbol(s)

    logging.info("MD|start|symbols=%s|intervals=%s|sleep=%ss", symbols, intervals, MD_SLEEP_SECONDS)

    # cold-start backfill
    for s in symbols:
        for itv in intervals:
            try:
                backfill_if_empty(s, itv)
            except Exception:
                logging.exception("MD|backfill failed|%s|%s", s, itv)

    while True:
        tick_start = time.perf_counter()
        for s in symbols:
            for itv in intervals:
                try:
                    catch_up(s, itv)
                except Exception:
                    logging.exception("MD|catchup failed|%s|%s", s, itv)

        elapsed = time.perf_counter() - tick_start
        if elapsed < MD_SLEEP_SECONDS:
            time.sleep(MD_SLEEP_SECONDS - elapsed)

if __name__ == "__main__":
    main()
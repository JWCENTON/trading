import os
import time
import json
import logging
import pandas as pd

from common.schema import ensure_schema
from common.db import get_db_conn
from common.regime import detect_regime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = os.environ.get("REGIME_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")

raw = os.environ.get("REGIME_INTERVALS", "1m")
INTERVALS = [x.strip() for x in raw.split(",") if x.strip()]

LOOKBACK = int(os.environ.get("REGIME_LOOKBACK", "200"))
EMA_FAST = int(os.environ.get("REGIME_TREND_EMA_FAST", "21"))
EMA_SLOW = int(os.environ.get("REGIME_TREND_EMA_SLOW", "100"))
ATR_PERIOD = int(os.environ.get("REGIME_VOL_ATR_PERIOD", "14"))
VOL_THRESHOLD_PCT = float(os.environ.get("REGIME_VOL_THRESHOLD_PCT", "0.08"))
SHOCK_Z = float(os.environ.get("REGIME_SHOCK_Z", "3.0"))
TREND_MIN_STRENGTH_PCT = float(os.environ.get("REGIME_TREND_MIN_STRENGTH_PCT", "0.05"))

SLEEP_SECONDS = int(os.environ.get("REGIME_SLEEP_SECONDS", "60"))
REGIME_MAX_AGE_SECONDS = int(os.environ.get("REGIME_MAX_AGE_SECONDS", "180"))


def _interval_to_seconds(interval: str) -> int:
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    raise ValueError(f"Unsupported interval: {interval}")


def _interval_to_pandas_floor(interval: str) -> str:
    if interval.endswith("m"):
        n = int(interval[:-1])
        return f"{n}min"
    raise ValueError(f"Unsupported interval: {interval}")


def _last_closed_open_time(now_utc: pd.Timestamp, interval: str) -> pd.Timestamp:
    """
    Zwraca open_time ostatniej DOMKNIĘTEJ świecy na siatce interwału.
    """
    interval_sec = _interval_to_seconds(interval)
    floor_str = _interval_to_pandas_floor(interval)
    cutoff = now_utc - pd.Timedelta(seconds=interval_sec)
    return cutoff.floor(floor_str)


def fetch_candles(symbol: str, interval: str, lookback: int, end_ts: pd.Timestamp) -> pd.DataFrame:
    conn = get_db_conn()
    q = """
    SELECT open_time, open, high, low, close, volume
    FROM candles
    WHERE symbol=%s AND interval=%s
      AND open_time <= %s
    ORDER BY open_time DESC
    LIMIT %s
    """
    df = pd.read_sql_query(q, conn, params=(symbol, interval, end_ts, lookback))
    conn.close()
    if df is None or df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df.sort_values("open_time")


def candle_exists(symbol: str, interval: str, ts: pd.Timestamp) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM candles WHERE symbol=%s AND interval=%s AND open_time=%s LIMIT 1",
        (symbol, interval, ts.to_pydatetime()),
    )
    ok = cur.fetchone() is not None
    cur.close()
    conn.close()
    return ok


def already_have_regime(symbol: str, interval: str, ts: pd.Timestamp) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM market_regime WHERE symbol=%s AND interval=%s AND ts=%s LIMIT 1",
        (symbol, interval, ts.to_pydatetime()),
    )
    ok = cur.fetchone() is not None
    cur.close()
    conn.close()
    return ok


def get_last_regime_ts(symbol: str, interval: str):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM market_regime WHERE symbol=%s AND interval=%s",
        (symbol, interval),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def write_regime(symbol: str, interval: str, ts, payload: dict, lookback: int):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO market_regime(
          symbol, interval, ts,
          regime, vol_regime,
          score_trend, score_vol, score_shock,
          trend_dir,
          ema_fast, ema_slow,
          trend_strength_pct,
          atr_pct, shock_z,
          lookback, meta
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (symbol, interval, ts) DO UPDATE
        SET
          regime=EXCLUDED.regime,
          vol_regime=EXCLUDED.vol_regime,
          score_trend=EXCLUDED.score_trend,
          score_vol=EXCLUDED.score_vol,
          score_shock=EXCLUDED.score_shock,
          trend_dir=EXCLUDED.trend_dir,
          ema_fast=EXCLUDED.ema_fast,
          ema_slow=EXCLUDED.ema_slow,
          trend_strength_pct=EXCLUDED.trend_strength_pct,
          atr_pct=EXCLUDED.atr_pct,
          shock_z=EXCLUDED.shock_z,
          lookback=EXCLUDED.lookback,
          meta=EXCLUDED.meta,
          created_at=now();
        """,
        (
            symbol, interval, ts,
            payload.get("regime"),
            payload.get("vol_regime"),
            payload.get("score_trend"),
            payload.get("score_vol"),
            payload.get("score_shock"),
            payload.get("trend_dir"),
            payload.get("ema_fast"),
            payload.get("ema_slow"),
            payload.get("trend_strength_pct"),
            payload.get("atr_pct"),
            payload.get("shock_z"),
            lookback,
            json.dumps(
                {
                    "ema_fast": EMA_FAST,
                    "ema_slow": EMA_SLOW,
                    "atr_period": ATR_PERIOD,
                    "vol_threshold_pct": VOL_THRESHOLD_PCT,
                    "shock_z": SHOCK_Z,
                    "trend_min_strength_pct": TREND_MIN_STRENGTH_PCT,
                    "regime_max_age_seconds": REGIME_MAX_AGE_SECONDS,
                }
            ),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def main():
    ensure_schema()
    symbols = [s.strip() for s in SYMBOLS.split(",") if s.strip()]

    logging.info(
        "Regime worker starting: symbols=%s intervals=%s lookback=%d sleep=%ds max_age=%ds",
        symbols, INTERVALS, LOOKBACK, SLEEP_SECONDS, REGIME_MAX_AGE_SECONDS
    )

    while True:
        t0 = time.perf_counter()
        now_utc = pd.Timestamp.now(tz="UTC")

        for sym in symbols:
            for interval in INTERVALS:
                try:
                    step = pd.Timedelta(seconds=_interval_to_seconds(interval))
                    ts_target = _last_closed_open_time(now_utc, interval)

                    # stale guard: jeżeli target jest za stary -> skip (to zwykle znaczy, że candle ingestion stoi)
                    if (now_utc - ts_target).total_seconds() > REGIME_MAX_AGE_SECONDS:
                        logging.warning(
                            "Regime skip (stale target): %s %s ts_target=%s age=%.1fs > max_age=%ds",
                            sym, interval, ts_target.isoformat(),
                            (now_utc - ts_target).total_seconds(), REGIME_MAX_AGE_SECONDS
                        )
                        continue

                    last_ts = get_last_regime_ts(sym, interval)
                    if last_ts is None:
                        # start: policz od najbliższej dostępnej domkniętej świecy (target)
                        ts_next = ts_target
                    else:
                        last_ts = pd.Timestamp(last_ts, tz="UTC") if getattr(last_ts, "tzinfo", None) is None else pd.Timestamp(last_ts)
                        ts_next = last_ts + step

                    # catch-up loop: policz wszystkie brakujące ts aż do target
                    catchup_count = 0
                    while ts_next <= ts_target:
                        # jeżeli brak świecy dla tego ts, to ingestion jeszcze nie domknął tej minuty -> przerwij catchup
                        if not candle_exists(sym, interval, ts_next):
                            logging.info(
                                "Catch-up stop (missing candle): %s %s ts=%s",
                                sym, interval, ts_next.isoformat()
                            )
                            break

                        if already_have_regime(sym, interval, ts_next):
                            ts_next = ts_next + step
                            continue

                        df = fetch_candles(sym, interval, LOOKBACK, end_ts=ts_next)
                        if df is None or df.empty:
                            logging.warning("No candles yet for %s %s (end_ts=%s)", sym, interval, ts_next.isoformat())
                            break

                        payload = detect_regime(
                            df,
                            ema_fast=EMA_FAST,
                            ema_slow=EMA_SLOW,
                            atr_period=ATR_PERIOD,
                            vol_threshold_pct=VOL_THRESHOLD_PCT,
                            shock_z=SHOCK_Z,
                            trend_min_strength_pct=TREND_MIN_STRENGTH_PCT,
                        )

                        write_regime(sym, interval, ts_next.to_pydatetime(), payload, LOOKBACK)
                        catchup_count += 1

                        logging.info(
                            "REGIME %s %s ts=%s => %s (trend_dir=%s atr_pct=%.4f strength=%.4f shock_z=%s)",
                            sym, interval, ts_next.isoformat(),
                            payload.get("regime"), payload.get("trend_dir"),
                            payload.get("atr_pct") if payload.get("atr_pct") is not None else -1.0,
                            payload.get("trend_strength_pct") if payload.get("trend_strength_pct") is not None else -1.0,
                            payload.get("shock_z"),
                        )

                        ts_next = ts_next + step

                    if catchup_count > 0:
                        logging.info("Catch-up done: %s %s wrote=%d up_to=%s",
                                     sym, interval, catchup_count, ts_target.isoformat())

                except Exception:
                    logging.exception("Regime calc failed for %s %s", sym, interval)

        dt = time.perf_counter() - t0
        logging.info("Regime loop finished in %.3f s", dt)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
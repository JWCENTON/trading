import os
import time
import json
import logging
import pandas as pd

from common.db import get_db_conn
from common.regime import detect_regime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SYMBOLS = os.environ.get("REGIME_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")
INTERVAL = os.environ.get("REGIME_INTERVAL", "1m")

LOOKBACK = int(os.environ.get("REGIME_LOOKBACK", "200"))
EMA_FAST = int(os.environ.get("REGIME_TREND_EMA_FAST", "21"))
EMA_SLOW = int(os.environ.get("REGIME_TREND_EMA_SLOW", "100"))
ATR_PERIOD = int(os.environ.get("REGIME_VOL_ATR_PERIOD", "14"))
VOL_THRESHOLD_PCT = float(os.environ.get("REGIME_VOL_THRESHOLD_PCT", "0.08"))
SHOCK_Z = float(os.environ.get("REGIME_SHOCK_Z", "3.0"))
TREND_MIN_STRENGTH_PCT = float(os.environ.get("REGIME_TREND_MIN_STRENGTH_PCT", "0.05"))

SLEEP_SECONDS = int(os.environ.get("REGIME_SLEEP_SECONDS", "60"))


def fetch_candles(symbol: str, interval: str, lookback: int) -> pd.DataFrame:
    conn = get_db_conn()
    q = """
    SELECT open_time, open, high, low, close, volume
    FROM candles
    WHERE symbol=%s AND interval=%s
    ORDER BY open_time DESC
    LIMIT %s
    """
    df = pd.read_sql_query(q, conn, params=(symbol, interval, lookback))
    conn.close()
    if df is None or df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df.sort_values("open_time")


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
                }
            ),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def main():
    symbols = [s.strip() for s in SYMBOLS.split(",") if s.strip()]
    logging.info("Regime worker starting: symbols=%s interval=%s lookback=%d", symbols, INTERVAL, LOOKBACK)

    while True:
        t0 = time.perf_counter()
        for sym in symbols:
            try:
                df = fetch_candles(sym, INTERVAL, LOOKBACK)
                if df is None or df.empty:
                    logging.warning("No candles yet for %s %s", sym, INTERVAL)
                    continue

                # last candle timestamp = open_time (to align with your schema)
                ts = df["open_time"].iloc[-1]

                payload = detect_regime(
                    df,
                    ema_fast=EMA_FAST,
                    ema_slow=EMA_SLOW,
                    atr_period=ATR_PERIOD,
                    vol_threshold_pct=VOL_THRESHOLD_PCT,
                    shock_z=SHOCK_Z,
                    trend_min_strength_pct=TREND_MIN_STRENGTH_PCT,
                )

                write_regime(sym, INTERVAL, ts, payload, LOOKBACK)
                logging.info(
                    "REGIME %s %s ts=%s => %s (trend_dir=%s atr_pct=%.4f strength=%.4f shock_z=%s)",
                    sym, INTERVAL, ts.isoformat(),
                    payload["regime"], payload["trend_dir"],
                    payload["atr_pct"] if payload["atr_pct"] is not None else -1.0,
                    payload["trend_strength_pct"] if payload["trend_strength_pct"] is not None else -1.0,
                    payload["shock_z"],
                )

            except Exception:
                logging.exception("Regime calc failed for %s", sym)

        dt = time.perf_counter() - t0
        logging.info("Regime loop finished in %.3f s", dt)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()

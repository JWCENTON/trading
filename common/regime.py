from __future__ import annotations

import math
import numpy as np
import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def atr(df: pd.DataFrame, period: int) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(span=period, adjust=False).mean()


def zscore(x: pd.Series) -> pd.Series:
    x = x.astype(float)
    mu = x.rolling(window=len(x), min_periods=5).mean()
    sd = x.rolling(window=len(x), min_periods=5).std(ddof=0)
    return (x - mu) / sd.replace(0.0, np.nan)


def detect_regime(
    df: pd.DataFrame,
    *,
    ema_fast: int = 21,
    ema_slow: int = 100,
    atr_period: int = 14,
    vol_threshold_pct: float = 0.08,
    shock_z: float = 3.0,
    trend_min_strength_pct: float = 0.05,
) -> dict:
    """
    df: kolumny wymagane: open_time, open, high, low, close
    Zwraca dict gotowy do zapisu w DB.
    """
    if df is None or len(df) < max(ema_slow, atr_period) + 5:
        return {
            "regime": "UNKNOWN",
            "trend_dir": 0,
            "ema_fast": None,
            "ema_slow": None,
            "trend_strength_pct": None,
            "atr_pct": None,
            "shock_z": None,
        }

    df = df.sort_values("open_time").copy()
    close = df["close"].astype(float)

    ef = ema(close, ema_fast)
    es = ema(close, ema_slow)

    last_close = float(close.iloc[-1])
    last_ef = float(ef.iloc[-1])
    last_es = float(es.iloc[-1])

    # trend strength as pct distance between EMAs relative to price
    strength_pct = abs(last_ef - last_es) / (last_close if last_close else 1.0) * 100.0

    # trend_dir: +1 / -1 / 0
    if strength_pct >= trend_min_strength_pct:
        trend_dir = 1 if last_ef > last_es else -1
    else:
        trend_dir = 0

    # ATR%
    a = atr(df, atr_period)
    last_atr = float(a.iloc[-1])
    atr_pct = (last_atr / (last_close if last_close else 1.0)) * 100.0

    # shock via z-score of log returns (last point)
    rets = np.log(close / close.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
    shock_val = None
    is_shock = False
    if len(rets) >= 10:
        z = (rets - rets.mean()) / (rets.std(ddof=0) if rets.std(ddof=0) else np.nan)
        shock_val = float(z.iloc[-1])
        is_shock = (abs(shock_val) >= shock_z) if shock_val is not None and not math.isnan(shock_val) else False

    is_high_vol = atr_pct >= vol_threshold_pct
    vol_regime = "HIGH_VOL" if is_high_vol else "LOW_VOL"

    score_trend = strength_pct
    score_vol = atr_pct
    score_shock = abs(shock_val) if shock_val is not None and not math.isnan(shock_val) else None

    if is_shock:
        regime = "SHOCK"
    elif trend_dir == 1:
        regime = "TREND_UP"
    elif trend_dir == -1:
        regime = "TREND_DOWN"
    else:
        regime = "RANGE_HIGHVOL" if is_high_vol else "RANGE_LOWVOL"

    return {
        "regime": regime,
        "vol_regime": vol_regime,
        "score_trend": score_trend,
        "score_vol": score_vol,
        "score_shock": score_shock,
        "trend_dir": trend_dir,
        "ema_fast": last_ef,
        "ema_slow": last_es,
        "trend_strength_pct": strength_pct,
        "atr_pct": atr_pct,
        "shock_z": shock_val, 
    }

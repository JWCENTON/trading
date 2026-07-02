import math
from typing import Any, Dict, Optional

from common.db import get_db_conn

_CACHE: dict[tuple[str, str, Optional[str]], Dict[str, Any]] = {}


def _safe_float(v, default=None):
    try:
        if v is None:
            return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _score_atr(atr_pct: Optional[float]) -> float:
    if atr_pct is None:
        return 0.0
    return _clamp((atr_pct / 0.60) * 100.0)


def _score_volume(volume_ratio: Optional[float]) -> float:
    if volume_ratio is None:
        return 0.0
    return _clamp((volume_ratio / 2.0) * 100.0)


def _score_abs_pct(v: Optional[float], full_at: float) -> float:
    if v is None:
        return 0.0
    return _clamp((abs(v) / full_at) * 100.0)


def compute_realtime_snapshot(symbol: str, interval: str, candle_open_time=None, lookback: int = 40) -> Dict[str, Any]:
    """
    Shadow-only realtime snapshot.
    Nie podejmuje decyzji i nie wpływa na trading.
    Czyta candles i zwraca mierzalne metryki bieżącego rynku.
    """
    cache_key = (str(symbol), str(interval), str(candle_open_time) if candle_open_time is not None else None)
    if cache_key in _CACHE:
        return dict(_CACHE[cache_key])

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open_time, open, high, low, close, volume, ema_21, atr_14
                FROM candles
                WHERE symbol=%s
                  AND interval=%s
                  AND (%s::timestamptz IS NULL OR open_time <= %s::timestamptz)
                ORDER BY open_time DESC
                LIMIT %s
                """,
                (symbol, interval, candle_open_time, candle_open_time, int(lookback)),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    rows = list(reversed(rows))
    if len(rows) < 6:
        out = {
            "ok": False,
            "reason": "NOT_ENOUGH_CANDLES",
            "rows": len(rows),
            "realtime_score": 0.0,
            "realtime_status": "NO_DATA",
        }
        _CACHE[cache_key] = out
        return dict(out)

    latest = rows[-1]
    prev = rows[-2]

    _, _open, high, low, close, volume, ema_21, atr_14 = latest
    _, _, prev_high, prev_low, prev_close, prev_volume, prev_ema_21, prev_atr_14 = prev

    price = _safe_float(close)
    prev_price = _safe_float(prev_close)
    high_f = _safe_float(high)
    low_f = _safe_float(low)
    volume_f = _safe_float(volume)
    ema_f = _safe_float(ema_21)
    prev_ema_f = _safe_float(prev_ema_21)
    atr_f = _safe_float(atr_14)

    volumes = [_safe_float(r[5]) for r in rows[-21:-1]]
    volumes = [v for v in volumes if v is not None]
    avg_volume_20 = sum(volumes) / len(volumes) if volumes else None

    highs_prev = [_safe_float(r[2]) for r in rows[-21:-1]]
    lows_prev = [_safe_float(r[3]) for r in rows[-21:-1]]
    highs_prev = [v for v in highs_prev if v is not None]
    lows_prev = [v for v in lows_prev if v is not None]

    prev_20_high = max(highs_prev) if highs_prev else None
    prev_20_low = min(lows_prev) if lows_prev else None

    close_3 = _safe_float(rows[-4][4]) if len(rows) >= 4 else None
    close_5 = _safe_float(rows[-6][4]) if len(rows) >= 6 else None

    atr_pct = (atr_f / price * 100.0) if atr_f and price else None
    ema_slope_pct = ((ema_f - prev_ema_f) / prev_ema_f * 100.0) if ema_f and prev_ema_f else None
    volume_ratio = (volume_f / avg_volume_20) if volume_f is not None and avg_volume_20 else None
    momentum_3_pct = ((price - close_3) / close_3 * 100.0) if price and close_3 else None
    momentum_5_pct = ((price - close_5) / close_5 * 100.0) if price and close_5 else None
    range_pct = ((high_f - low_f) / price * 100.0) if high_f and low_f and price else None

    breakout_up = bool(price and prev_20_high and price > prev_20_high)
    breakout_down = bool(price and prev_20_low and price < prev_20_low)

    atr_score = _score_atr(atr_pct)
    volume_score = _score_volume(volume_ratio)
    slope_score = _score_abs_pct(ema_slope_pct, 0.20)
    momentum_score = max(_score_abs_pct(momentum_3_pct, 0.40), _score_abs_pct(momentum_5_pct, 0.70))
    breakout_score = 100.0 if (breakout_up or breakout_down) else 0.0

    realtime_score = (
        0.25 * atr_score
        + 0.20 * volume_score
        + 0.20 * slope_score
        + 0.25 * momentum_score
        + 0.10 * breakout_score
    )
    realtime_score = round(_clamp(realtime_score), 4)

    drivers = {
        "ATR": atr_score,
        "VOLUME": volume_score,
        "EMA_SLOPE": slope_score,
        "MOMENTUM": momentum_score,
        "BREAKOUT": breakout_score,
    }
    primary_driver = max(drivers, key=drivers.get)

    if realtime_score >= 70:
        status = "REALTIME_READY"
    elif realtime_score >= 50:
        status = "REALTIME_WATCH"
    elif realtime_score >= 30:
        status = "REALTIME_OBSERVE"
    else:
        status = "REALTIME_WEAK"

    out = {
        "ok": True,
        "realtime_score": realtime_score,
        "realtime_status": status,
        "primary_driver": primary_driver,
        "price": price,
        "prev_price": prev_price,
        "atr_pct": atr_pct,
        "ema_slope_pct": ema_slope_pct,
        "volume_ratio": volume_ratio,
        "momentum_3_pct": momentum_3_pct,
        "momentum_5_pct": momentum_5_pct,
        "range_pct": range_pct,
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
        "scores": drivers,
        "prev_20_high": prev_20_high,
        "prev_20_low": prev_20_low,
        "rows": len(rows),
    }
    _CACHE[cache_key] = out
    return dict(out)

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Callable

import pandas as pd


INDICATOR_STATE_CONTRACT = "SUPERTREND_INCREMENTAL_INDICATOR_STATE_V1"
INDICATOR_COLUMNS = (
    "ema_21",
    "rsi_14",
    "atr_14",
    "supertrend",
    "supertrend_direction",
)


@dataclass(frozen=True)
class IndicatorParameters:
    ema_period: int
    rsi_period: int
    atr_period: int
    supertrend_multiplier: float

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            {
                "atr_period": int(self.atr_period),
                "contract": INDICATOR_STATE_CONTRACT,
                "ema_period": int(self.ema_period),
                "rsi_period": int(self.rsi_period),
                "supertrend_multiplier": float(self.supertrend_multiplier),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IndicatorState:
    last_calculated_candle_open_time: object
    last_close: float
    ema_value: float
    atr_value: float
    final_upper_band: float
    final_lower_band: float
    supertrend_direction: int
    parameter_fingerprint: str

    def validate(self, parameters: IndicatorParameters) -> None:
        if self.parameter_fingerprint != parameters.fingerprint:
            raise ValueError("SUPERTREND_INDICATOR_PARAMETER_FINGERPRINT_MISMATCH")
        numeric = (
            self.last_close,
            self.ema_value,
            self.atr_value,
            self.final_upper_band,
            self.final_lower_band,
        )
        if not all(math.isfinite(float(value)) for value in numeric):
            raise ValueError("SUPERTREND_INDICATOR_STATE_NON_FINITE")
        if int(self.supertrend_direction) not in (-1, 1):
            raise ValueError("SUPERTREND_INDICATOR_DIRECTION_INVALID")


@dataclass(frozen=True)
class IndicatorCalculation:
    rows: pd.DataFrame
    state: IndicatorState
    mode: str
    history_rows_processed: int
    warm_rows_loaded: int
    incremental_rows_processed: int


def _state_from_last_row(
    frame: pd.DataFrame,
    parameters: IndicatorParameters,
) -> IndicatorState:
    row = frame.iloc[-1]
    return IndicatorState(
        last_calculated_candle_open_time=row["open_time"],
        last_close=float(row["close"]),
        ema_value=float(row["ema_21"]),
        atr_value=float(row["atr_14"]),
        final_upper_band=float(row["_final_upper_band"]),
        final_lower_band=float(row["_final_lower_band"]),
        supertrend_direction=int(row["supertrend_direction"]),
        parameter_fingerprint=parameters.fingerprint,
    )


def calculate_full_history(
    source: pd.DataFrame,
    parameters: IndicatorParameters,
    progress_callback: Callable[[str, int, int], None] | None = None,
    progress_row_step: int = 5000,
) -> IndicatorCalculation:
    """Reference calculation matching the original full-history implementation."""
    if source.empty:
        raise ValueError("SUPERTREND_INDICATOR_HISTORY_EMPTY")
    source_close = source["close"]
    frame = source.copy(deep=True)
    close = source_close.astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)

    frame["ema_21"] = close.ewm(
        span=parameters.ema_period, adjust=False,
    ).mean()
    if progress_callback is not None:
        progress_callback("EMA", len(frame), len(frame))

    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    roll_up = gain.rolling(window=parameters.rsi_period).mean()
    roll_down = loss.rolling(window=parameters.rsi_period).mean()
    rs = roll_up / roll_down
    frame["rsi_14"] = 100.0 - (100.0 / (1.0 + rs))
    if progress_callback is not None:
        progress_callback("RSI", len(frame), len(frame))

    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    frame["atr_14"] = tr.ewm(
        span=parameters.atr_period, adjust=False,
    ).mean()
    if progress_callback is not None:
        progress_callback("ATR", len(frame), len(frame))

    hl2 = (high + low) / 2.0
    basic_upper = hl2 + parameters.supertrend_multiplier * frame["atr_14"]
    basic_lower = hl2 - parameters.supertrend_multiplier * frame["atr_14"]
    final_upper = pd.Series(index=frame.index, dtype=float)
    final_lower = pd.Series(index=frame.index, dtype=float)
    direction = pd.Series(index=frame.index, dtype=int)
    supertrend = pd.Series(index=frame.index, dtype=float)

    final_upper.iloc[0] = float(basic_upper.iloc[0])
    final_lower.iloc[0] = float(basic_lower.iloc[0])
    direction.iloc[0] = 1
    supertrend.iloc[0] = float(final_lower.iloc[0])

    for position in range(1, len(frame)):
        previous_close = float(close.iloc[position - 1])
        upper = float(basic_upper.iloc[position])
        lower = float(basic_lower.iloc[position])
        previous_upper = float(final_upper.iloc[position - 1])
        previous_lower = float(final_lower.iloc[position - 1])
        final_upper.iloc[position] = (
            upper
            if upper < previous_upper or previous_close > previous_upper
            else previous_upper
        )
        final_lower.iloc[position] = (
            lower
            if lower > previous_lower or previous_close < previous_lower
            else previous_lower
        )
        current_close = float(close.iloc[position])
        if current_close > previous_upper:
            direction.iloc[position] = 1
        elif current_close < previous_lower:
            direction.iloc[position] = -1
        else:
            direction.iloc[position] = int(direction.iloc[position - 1])
        supertrend.iloc[position] = (
            float(final_lower.iloc[position])
            if int(direction.iloc[position]) == 1
            else float(final_upper.iloc[position])
        )
        if (
            progress_callback is not None
            and position % max(1, int(progress_row_step)) == 0
        ):
            progress_callback("SUPERTREND_LOOP", position, len(frame))

    if progress_callback is not None:
        progress_callback("SUPERTREND_LOOP", len(frame), len(frame))

    frame["_final_upper_band"] = final_upper
    frame["_final_lower_band"] = final_lower
    frame["supertrend_direction"] = direction
    frame["supertrend"] = supertrend
    state = _state_from_last_row(frame, parameters)
    return IndicatorCalculation(
        rows=frame,
        state=state,
        mode="FULL_BOOTSTRAP",
        history_rows_processed=len(frame),
        warm_rows_loaded=0,
        incremental_rows_processed=0,
    )


def calculate_incremental(
    new_rows: pd.DataFrame,
    warm_closes: pd.Series,
    state: IndicatorState,
    parameters: IndicatorParameters,
    progress_callback: Callable[[str, int, int], None] | None = None,
    progress_row_step: int = 5000,
) -> IndicatorCalculation:
    """Advance the recursive state using only new candles and RSI warm closes."""
    state.validate(parameters)
    if new_rows.empty:
        return IndicatorCalculation(
            rows=new_rows.copy(deep=True),
            state=state,
            mode="INCREMENTAL_NOOP",
            history_rows_processed=0,
            warm_rows_loaded=len(warm_closes),
            incremental_rows_processed=0,
        )
    if len(warm_closes) < parameters.rsi_period:
        raise ValueError("SUPERTREND_INCREMENTAL_RSI_WARM_STATE_INCOMPLETE")

    frame = new_rows.copy(deep=True)
    close = frame["close"].astype(float).reset_index(drop=True)
    high = frame["high"].astype(float).reset_index(drop=True)
    low = frame["low"].astype(float).reset_index(drop=True)

    seeded_ema = pd.Series(
        [float(state.ema_value), *close.tolist()], dtype=float,
    ).ewm(span=parameters.ema_period, adjust=False).mean()
    frame["ema_21"] = seeded_ema.iloc[1:].to_numpy()
    if progress_callback is not None:
        progress_callback("EMA", len(frame), len(frame))

    rsi_closes = pd.Series(
        [*warm_closes.astype(float).tolist(), *close.tolist()], dtype=float,
    )
    delta = rsi_closes.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    roll_up = gain.rolling(window=parameters.rsi_period).mean()
    roll_down = loss.rolling(window=parameters.rsi_period).mean()
    rs = roll_up / roll_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    frame["rsi_14"] = rsi.iloc[-len(frame):].to_numpy()
    if progress_callback is not None:
        progress_callback("RSI", len(frame), len(frame))

    previous_closes = pd.Series(
        [float(state.last_close), *close.iloc[:-1].tolist()], dtype=float,
    )
    true_range = pd.concat(
        [
            high - low,
            (high - previous_closes).abs(),
            (low - previous_closes).abs(),
        ],
        axis=1,
    ).max(axis=1)
    seeded_atr = pd.Series(
        [float(state.atr_value), *true_range.tolist()], dtype=float,
    ).ewm(span=parameters.atr_period, adjust=False).mean()
    frame["atr_14"] = seeded_atr.iloc[1:].to_numpy()
    if progress_callback is not None:
        progress_callback("ATR", len(frame), len(frame))

    previous_close = float(state.last_close)
    previous_upper = float(state.final_upper_band)
    previous_lower = float(state.final_lower_band)
    previous_direction = int(state.supertrend_direction)
    final_upper_values = []
    final_lower_values = []
    direction_values = []
    supertrend_values = []
    for position in range(len(frame)):
        current_close = float(close.iloc[position])
        midpoint = (float(high.iloc[position]) + float(low.iloc[position])) / 2.0
        atr = float(frame.iloc[position]["atr_14"])
        basic_upper = midpoint + parameters.supertrend_multiplier * atr
        basic_lower = midpoint - parameters.supertrend_multiplier * atr
        final_upper = (
            basic_upper
            if basic_upper < previous_upper or previous_close > previous_upper
            else previous_upper
        )
        final_lower = (
            basic_lower
            if basic_lower > previous_lower or previous_close < previous_lower
            else previous_lower
        )
        if current_close > previous_upper:
            direction = 1
        elif current_close < previous_lower:
            direction = -1
        else:
            direction = previous_direction
        supertrend = final_lower if direction == 1 else final_upper
        final_upper_values.append(final_upper)
        final_lower_values.append(final_lower)
        direction_values.append(direction)
        supertrend_values.append(supertrend)
        previous_close = current_close
        previous_upper = final_upper
        previous_lower = final_lower
        previous_direction = direction
        if (
            progress_callback is not None
            and position % max(1, int(progress_row_step)) == 0
        ):
            progress_callback("SUPERTREND_LOOP", position, len(frame))

    if progress_callback is not None:
        progress_callback("SUPERTREND_LOOP", len(frame), len(frame))

    frame["_final_upper_band"] = final_upper_values
    frame["_final_lower_band"] = final_lower_values
    frame["supertrend_direction"] = direction_values
    frame["supertrend"] = supertrend_values
    state_after = _state_from_last_row(frame, parameters)
    return IndicatorCalculation(
        rows=frame,
        state=state_after,
        mode="INCREMENTAL",
        history_rows_processed=0,
        warm_rows_loaded=len(warm_closes),
        incremental_rows_processed=len(frame),
    )

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from common.supertrend_incremental_indicators import (
    INDICATOR_COLUMNS,
    IndicatorParameters,
    calculate_full_history,
    calculate_incremental,
)


PARAMETERS = IndicatorParameters(21, 14, 14, 3.0)


def _candles(*, rows=260, interval="1min"):
    index = np.arange(rows, dtype=float)
    close = (
        100.0
        + np.sin(index / 7.0) * 2.1
        + np.cos(index / 19.0) * 0.8
        + index * 0.003
    )
    return pd.DataFrame(
        {
            "id": np.arange(1, rows + 1),
            "open_time": pd.date_range(
                "2026-01-01T00:00:00Z", periods=rows, freq=interval,
            ),
            "open": close - 0.03,
            "high": close + 0.11 + (index % 5) * 0.002,
            "low": close - 0.13 - (index % 3) * 0.003,
            "close": close,
        }
    )


def _assert_equivalent(actual, expected):
    assert actual["open_time"].tolist() == expected["open_time"].tolist()
    for column in (*INDICATOR_COLUMNS, "_final_upper_band", "_final_lower_band"):
        np.testing.assert_allclose(
            actual[column].to_numpy(dtype=float),
            expected[column].to_numpy(dtype=float),
            rtol=0.0,
            atol=1e-12,
            equal_nan=True,
        )


@pytest.mark.parametrize("interval", ["1min", "5min"])
def test_incremental_calculation_matches_full_history(interval):
    source = _candles(interval=interval)
    split = 173
    reference = calculate_full_history(source, PARAMETERS)
    bootstrap = calculate_full_history(source.iloc[:split], PARAMETERS)
    incremental = calculate_incremental(
        source.iloc[split:],
        source.iloc[split - PARAMETERS.rsi_period:split]["close"],
        bootstrap.state,
        PARAMETERS,
    )

    _assert_equivalent(
        incremental.rows.reset_index(drop=True),
        reference.rows.iloc[split:].reset_index(drop=True),
    )
    assert incremental.history_rows_processed == 0
    assert incremental.incremental_rows_processed == len(source) - split


def test_restart_resume_and_backlog_batches_are_equivalent_and_monotonic():
    source = _candles(rows=320)
    full = calculate_full_history(source, PARAMETERS)
    first_end = 151
    second_end = 244
    first = calculate_full_history(source.iloc[:first_end], PARAMETERS)
    second = calculate_incremental(
        source.iloc[first_end:second_end],
        source.iloc[first_end - PARAMETERS.rsi_period:first_end]["close"],
        first.state,
        PARAMETERS,
    )
    restarted = calculate_incremental(
        source.iloc[second_end:],
        source.iloc[second_end - PARAMETERS.rsi_period:second_end]["close"],
        second.state,
        PARAMETERS,
    )

    _assert_equivalent(
        second.rows.reset_index(drop=True),
        full.rows.iloc[first_end:second_end].reset_index(drop=True),
    )
    _assert_equivalent(
        restarted.rows.reset_index(drop=True),
        full.rows.iloc[second_end:].reset_index(drop=True),
    )
    assert first.state.last_calculated_candle_open_time < second.state.last_calculated_candle_open_time
    assert second.state.last_calculated_candle_open_time < restarted.state.last_calculated_candle_open_time


def test_no_new_candle_is_idempotent_noop():
    source = _candles(rows=100)
    full = calculate_full_history(source, PARAMETERS)
    result = calculate_incremental(
        source.iloc[0:0],
        source.iloc[-PARAMETERS.rsi_period:]["close"],
        full.state,
        PARAMETERS,
    )
    assert result.mode == "INCREMENTAL_NOOP"
    assert result.incremental_rows_processed == 0
    assert result.state == full.state
    assert result.rows.empty


def test_parameter_change_rejects_persisted_warm_state():
    source = _candles(rows=100)
    state = calculate_full_history(source, PARAMETERS).state
    changed = IndicatorParameters(34, 14, 14, 3.0)
    with pytest.raises(
        ValueError,
        match="SUPERTREND_INDICATOR_PARAMETER_FINGERPRINT_MISMATCH",
    ):
        calculate_incremental(
            source.iloc[0:0], source.iloc[-14:]["close"], state, changed,
        )

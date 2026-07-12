from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType, SimpleNamespace
from typing import Any, Mapping

import pandas as pd


OPEN_TIME = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class RecordedOperation:
    kind: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class BbrangeObservedBehavior:
    returned_value: Any
    terminal_reason: str | None
    event_types: tuple[str, ...]
    event_reasons: tuple[str | None, ...]
    operations: tuple[RecordedOperation, ...]


class Recorder:
    def __init__(self) -> None:
        self.items: list[RecordedOperation] = []

    def add(self, kind: str, **payload: Any) -> None:
        self.items.append(RecordedOperation(kind, MappingProxyType(dict(payload))))

    def observe(self, returned_value: Any) -> BbrangeObservedBehavior:
        events = [x for x in self.items if x.kind == "strategy_event"]
        terminal = next(
            (x.payload.get("reason") for x in reversed(events)
             if x.payload.get("event_type") not in {"RUN_END", "RUN_START"}),
            None,
        )
        return BbrangeObservedBehavior(
            returned_value=returned_value,
            terminal_reason=terminal,
            event_types=tuple(str(x.payload.get("event_type")) for x in events),
            event_reasons=tuple(x.payload.get("reason") for x in events),
            operations=tuple(self.items),
        )


class StrictFakeExchange:
    """Any unconfigured exchange access fails the test immediately."""

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected exchange call: {name}")


class FakeCursor:
    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder

    def execute(self, sql, params=None):
        self.recorder.add("sql", sql=" ".join(str(sql).split()), params=params)

    def fetchone(self):
        return None

    def close(self):
        self.recorder.add("cursor_close")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class FakeConnection:
    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder

    def cursor(self):
        self.recorder.add("cursor_open")
        return FakeCursor(self.recorder)

    def commit(self):
        self.recorder.add("db_commit")

    def rollback(self):
        self.recorder.add("db_rollback")

    def close(self):
        self.recorder.add("db_close")


def candle(*, close=100.0, high=100.5, low=99.5, ema=100.0, rsi=40.0):
    return (OPEN_TIME, 100.0, high, low, close, ema, rsi)


def band_frame() -> pd.DataFrame:
    closes = [99.0, 101.0] * 15
    return pd.DataFrame({
        "open_time": [OPEN_TIME for _ in closes],
        "close": closes,
    })


def runtime_snapshot(*, mode="NORMAL", enabled=True, trading_mode="PAPER",
                     allow_entry=True):
    bc = SimpleNamespace(
        mode=mode,
        enabled=enabled,
        regime_enabled=True,
        regime_mode="ENFORCE",
    )
    cfg = SimpleNamespace(
        trading_mode=trading_mode,
        symbol="BTCUSDC",
        interval="1m",
        spot_mode=True,
        live_orders_enabled=allow_entry,
        quote_asset="USDC",
    )
    return {
        "bc": bc,
        "cfg_effective": cfg,
        "heartbeat": {"fixture": True},
        "allowed_orders_entry": allow_entry,
        "allowed_orders_exit": True,
        "allow_meta_entry": {"why": "fixture"},
        "allow_meta_exit": {"why": "fixture"},
    }

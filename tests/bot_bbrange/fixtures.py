from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

    def set_session(self, *, readonly):
        assert readonly is True
        self.recorder.add("set_session_readonly")

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


class StatefulBbrangeHarness:
    """Mutable state model around the real BBRANGE run_strategy function."""

    def __init__(self, module, monkeypatch):
        self.module = module
        self.monkeypatch = monkeypatch
        self.recorder = Recorder()
        self.position = None
        self.next_position_id = 40
        self.trading_mode = "PAPER"
        self.allow_entry = True
        self.regime_allow = True
        self.enabled = True
        self.mode = "NORMAL"
        self.execution_result = None
        self.profit_lock_state = "DISABLED"
        self._install()

    def _install(self):
        m = self.module
        rec = self.recorder
        mp = self.monkeypatch
        mp.setattr(m, "emit_strategy_event",
                   lambda **kw: rec.add("strategy_event", **kw))
        mp.setattr(m, "emit_regime_gate_event",
                   lambda **kw: rec.add("regime_gate", **kw))
        mp.setattr(m, "heartbeat", lambda info: rec.add("heartbeat", info=info))
        mp.setattr(m, "get_runtime_snapshot", lambda **_kw: runtime_snapshot(
            mode=self.mode, enabled=self.enabled, trading_mode=self.trading_mode,
            allow_entry=self.allow_entry,
        ))
        mp.setattr(m, "get_open_position",
                   lambda: rec.add("position_lookup") or self.position)
        mp.setattr(m, "decide_regime_gate", lambda **_kw: SimpleNamespace(
            allow=self.regime_allow, why="fixture", regime="FLAT", meta={}))
        mp.setattr(m, "get_trend", lambda *_a: "FLAT")
        mp.setattr(m.pd, "read_sql_query", lambda *_a, **_kw: band_frame())
        mp.setattr(m, "get_db_conn", lambda: FakeConnection(rec))
        mp.setattr(m, "compute_qty_from_notional",
                   lambda *_a, **_kw: (0.1, {"fixture": True}))
        mp.setattr(m, "get_user_settings_snapshot", lambda: {})
        mp.setattr(m, "get_recent_win_streak", lambda **_kw: SimpleNamespace(
            eligible=False, checked=3, required=3, streak=0, source="fixture",
            error=None, boost_candidate=False, boost_allowed=False,
            boost_block_reason=None, prev_net_1=None, prev_net_2=None,
            prev_net_3=None, last_exit_reason=None, last_boost_exit_reason=None,
            last_trade_gross_pct=None, rolling_5_gross_pct_avg=None))
        mp.setattr(m, "DAILY_MAX_LOSS_PCT", 0.0)
        mp.setattr(m, "DISABLE_HOURS_SET", set())
        mp.setattr(m, "MIN_BB_WIDTH_PCT", 0.0001)
        mp.setattr(m, "BBRANGE_EXPLORE_ENABLED", False)
        mp.setattr(m, "hard_time_exit_enabled", lambda: False)
        mp.setattr(m, "load_position_path_snapshot", lambda **_kw: SimpleNamespace(
            bars_seen=4, max_high=102.0, min_low=99.0))
        mp.setattr(m, "evaluate_profit_lock", self._profit_lock)
        mp.setattr(m, "emit_profit_lock_event_once",
                   lambda **kw: rec.add("profit_lock_event", **kw))
        mp.setattr(m, "execute_and_record", self._execute)
        mp.setattr(m, "close_position", self._close)
        mp.setattr(m, "set_mode",
                   lambda mode, reason=None: rec.add("set_mode", mode=mode, reason=reason))

    def _profit_lock(self, **_kwargs):
        triggered = self.profit_lock_state == "TRIGGERED"
        armed = self.profit_lock_state == "ARMED"
        return SimpleNamespace(
            triggered=triggered,
            reason_code="TRAIL_DROP" if triggered else
                        ("ARMED_WAITING" if armed else "DISABLED"),
            trigger_type="TRAIL" if triggered else None,
            peak_move_pct=2.0 if (triggered or armed) else 0.0,
            current_move_pct=1.0, floor_pct=0.8, trail_drop_pct=1.0,
            age_minutes=10.0,
        )

    def _execute(self, **kwargs):
        self.recorder.add("execution", **kwargs)
        result = self.execution_result
        if result is None:
            if self.trading_mode == "LIVE":
                result = {"ledger_ok": True, "live_attempted": True,
                          "live_ok": True, "executed_qty": kwargs["qty_btc"]}
            else:
                result = {"ledger_ok": True, "live_attempted": False,
                          "live_ok": True}
        result = dict(result)
        if (kwargs["is_exit"] and self.trading_mode == "LIVE"
                and float(result.get("executed_qty") or 0.0) > 0
                and not result.get("fully_executed", False)
                and self.position is not None):
            before = self.position
            remaining = max(0.0, float(before[2]) - float(result["executed_qty"]))
            self.position = (before[0], before[1], remaining, before[3], before[4])
            self.recorder.add("position_reduced", executed_qty=result["executed_qty"],
                              remaining_qty=remaining)
            result["live_ok"] = False
        if (not kwargs["is_exit"] and result["ledger_ok"] and
                (self.trading_mode != "LIVE" or result["live_ok"])):
            self.next_position_id += 1
            self.position = (
                self.next_position_id, "LONG", kwargs["qty_btc"], kwargs["price"],
                OPEN_TIME - timedelta(minutes=5),
            )
            self.recorder.add("position_open", position_id=self.next_position_id)
        return result

    def _close(self, **kwargs):
        self.recorder.add("position_close", **kwargs)
        self.position = None
        return True

    def set_position(self, *, price=100.0, age_minutes=5):
        self.position = (17, "LONG", 0.1, price,
                         OPEN_TIME - timedelta(minutes=age_minutes))

    def cycle(self, row):
        start = len(self.recorder.items)
        returned = self.module.run_strategy(row)
        operations = tuple(self.recorder.items[start:])
        return SimpleNamespace(
            returned_value=returned,
            operations=operations,
            events=tuple(x for x in operations if x.kind == "strategy_event"),
            position=self.position,
        )

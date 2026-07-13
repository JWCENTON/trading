from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType, SimpleNamespace
from typing import Any, Mapping


OPEN_TIME = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class CapturedEvent:
    channel: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class OrderAttempt:
    side: str
    intent: str
    price: float
    quantity: float
    reason: str
    exit_kind: str | None


@dataclass(frozen=True)
class StateChange:
    name: str
    before: Any
    after: Any


@dataclass(frozen=True)
class RsiObservation:
    action: str
    reason: str | None
    signal: str | None
    candle_state: str
    position_before: Any
    position_after: Any
    order_attempts: tuple[OrderAttempt, ...]
    captured_events: tuple[CapturedEvent, ...]
    state_changes: tuple[StateChange, ...]


class StrictFakeExchange:
    """Any exchange method call is an isolation failure."""

    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected exchange call: {name}")


def candle(
    *,
    minute: int = 0,
    close: float = 100.0,
    high: float = 100.2,
    low: float = 99.8,
    ema: float = 100.0,
    rsi: float = 40.0,
    atr: float = 0.2,
):
    open_time = OPEN_TIME + timedelta(minutes=minute)
    return (open_time, 100.0, high, low, close, ema, rsi, atr)


def entry_candle(*, minute: int = 0):
    return candle(minute=minute, close=99.8, high=100.0, low=99.6, rsi=35.0)


def entry_previous_candle(*, minute: int = -1):
    return candle(minute=minute, close=99.7, high=99.9, low=99.5, rsi=25.0)


def runtime_snapshot(*, mode: str = "NORMAL", enabled: bool = True):
    return {
        "bc": SimpleNamespace(
            mode=mode,
            enabled=enabled,
            regime_enabled=True,
            regime_mode="ENFORCE",
        ),
        "cfg_effective": SimpleNamespace(
            trading_mode="PAPER",
            spot_mode=True,
            live_orders_enabled=False,
        ),
        "heartbeat": {"fixture": True},
        "allowed_orders_entry": False,
        "allowed_orders_exit": True,
        "allow_meta_entry": {"source": "rsi-characterization"},
        "allow_meta_exit": {"source": "rsi-characterization"},
    }


class RsiStatefulHarness:
    """Test-only stateful driver around the current RSI runtime function."""

    def __init__(self, module, monkeypatch) -> None:
        self.module = module
        self.monkeypatch = monkeypatch
        self.exchange = StrictFakeExchange()
        self.now = OPEN_TIME + timedelta(minutes=10)
        self.last_processed_open_time = None
        self.position = None
        self.next_position_id = 1
        self.runtime_mode = "NORMAL"
        self.runtime_enabled = True
        self.regime_allow = True
        self.time_exit_enabled = False
        self.profit_lock_triggered = False
        self.events: list[CapturedEvent] = []
        self.orders: list[OrderAttempt] = []
        self.changes: list[StateChange] = []
        self.observations: list[RsiObservation] = []
        self._install()

    def _install(self) -> None:
        harness = self

        class FakeDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = harness.now
                return value if tz is None else value.astimezone(tz)

        self.monkeypatch.setattr(self.module, "datetime", FakeDateTime)
        self.monkeypatch.setattr(self.module, "get_exchange_client", lambda: self.exchange)
        self.monkeypatch.setattr(self.module, "get_db_conn", self._unexpected_db)
        self.monkeypatch.setattr(self.module, "emit_strategy_event", self._strategy_event)
        self.monkeypatch.setattr(
            self.module,
            "emit_regime_gate_event",
            lambda **payload: self._event("regime_gate", **payload),
        )
        self.monkeypatch.setattr(
            self.module,
            "heartbeat",
            lambda info: self._event("heartbeat", info=info),
        )
        self.monkeypatch.setattr(
            self.module,
            "emit_profit_lock_event_once",
            lambda **payload: self._event("profit_lock", **payload),
        )
        self.monkeypatch.setattr(
            self.module,
            "get_runtime_snapshot",
            lambda **_kwargs: runtime_snapshot(
                mode=self.runtime_mode, enabled=self.runtime_enabled
            ),
        )
        self.monkeypatch.setattr(self.module, "get_open_position", lambda: self.position)
        self.monkeypatch.setattr(self.module, "execute_and_record", self._execute)
        self.monkeypatch.setattr(self.module, "execute_exit_safe", self._execute_exit)
        self.monkeypatch.setattr(
            self.module,
            "decide_regime_gate",
            lambda **_kwargs: SimpleNamespace(
                allow=self.regime_allow,
                why="fixture-allow" if self.regime_allow else "fixture-block",
                regime="FLAT",
                meta={},
            ),
        )
        self.monkeypatch.setattr(
            self.module, "hard_time_exit_enabled", lambda: self.time_exit_enabled
        )
        self.monkeypatch.setattr(
            self.module,
            "load_position_path_snapshot",
            lambda **_kwargs: SimpleNamespace(
                bars_seen=4, max_high=100.6, min_low=99.8
            ),
        )
        self.monkeypatch.setattr(
            self.module, "evaluate_profit_lock", self._profit_lock
        )
        self.monkeypatch.setattr(self.module, "get_user_settings_snapshot", lambda: {})
        self.monkeypatch.setattr(
            self.module,
            "get_recent_win_streak",
            lambda **_kwargs: SimpleNamespace(
                eligible=False,
                checked=3,
                required=3,
                streak=0,
                source="fixture",
                error=None,
                boost_candidate=False,
                boost_allowed=False,
                boost_block_reason=None,
                prev_net_1=None,
                prev_net_2=None,
                prev_net_3=None,
                last_exit_reason=None,
                last_boost_exit_reason=None,
                last_trade_gross_pct=None,
                rolling_5_gross_pct_avg=None,
            ),
        )
        self.monkeypatch.setattr(self.module, "DAILY_MAX_LOSS_PCT", 0.0)
        self.monkeypatch.setattr(self.module, "DISABLE_HOURS_SET", set())
        self.monkeypatch.setattr(self.module, "MAX_DIST_FROM_EMA_PCT", 0.5)
        self.monkeypatch.setattr(self.module, "ATR_MIN_PCT", 0.1)
        self.monkeypatch.setattr(self.module, "EMA_SLOPE_BLOCK", 1)
        self.monkeypatch.setattr(self.module, "MIN_EDGE_PCT", 0.12)
        self.monkeypatch.setattr(self.module, "ENTRY_BUFFER_PCT", 0.002)
        self.monkeypatch.setattr(self.module, "ORDER_QTY_BTC", 0.1)
        self.monkeypatch.setattr(self.module, "MAX_POSITION_MINUTES", 30)

    @staticmethod
    def _unexpected_db(*_args, **_kwargs):
        raise AssertionError("unexpected real database access")

    def _event(self, channel: str, **payload: Any) -> None:
        self.events.append(
            CapturedEvent(channel, MappingProxyType(dict(payload)))
        )

    def _strategy_event(self, **payload: Any) -> None:
        self._event("strategy", **payload)

    def _set_position(self, value) -> None:
        before = self.position
        self.position = value
        if before != value:
            self.changes.append(StateChange("position", before, value))

    def _record_order(
        self,
        *,
        side: str,
        price: float,
        quantity: float,
        reason: str,
        is_exit: bool,
        exit_kind: str | None = None,
        candle_open_time=None,
    ) -> dict[str, Any]:
        self.orders.append(
            OrderAttempt(
                side=str(side),
                intent="EXIT" if is_exit else "ENTRY",
                price=float(price),
                quantity=float(quantity),
                reason=str(reason),
                exit_kind=exit_kind,
            )
        )
        if is_exit:
            self._set_position(None)
        else:
            position = (
                self.next_position_id,
                "LONG" if str(side).upper() == "BUY" else "SHORT",
                float(quantity),
                float(price),
                candle_open_time or self.now,
            )
            self.next_position_id += 1
            self._set_position(position)
        return {"ledger_ok": True, "live_ok": True, "live_attempted": False}

    def _execute(self, *args, **kwargs):
        names = ("side", "price", "qty_btc", "reason", "candle_open_time")
        values = dict(zip(names, args))
        values.update(kwargs)
        return self._record_order(
            side=values["side"],
            price=values["price"],
            quantity=values["qty_btc"],
            reason=values["reason"],
            is_exit=bool(values.get("is_exit", False)),
            candle_open_time=values.get("candle_open_time"),
        )

    def _execute_exit(self, **kwargs):
        return self._record_order(
            side=kwargs["exit_side"],
            price=kwargs["price"],
            quantity=kwargs["qty_btc"],
            reason=kwargs["reason_text"],
            is_exit=True,
            exit_kind=kwargs["exit_kind"],
            candle_open_time=kwargs.get("candle_open_time"),
        )

    def _profit_lock(self, **_kwargs):
        if self.profit_lock_triggered:
            return SimpleNamespace(
                triggered=True,
                reason_code="PROFIT_LOCK_LONG",
                trigger_type="TRAIL_DROP",
                peak_move_pct=0.6,
                current_move_pct=0.2,
                floor_pct=0.08,
                trail_drop_pct=0.2,
                age_minutes=10.0,
            )
        return SimpleNamespace(
            triggered=False,
            reason_code="NOT_ARMED",
            trigger_type=None,
            peak_move_pct=0.0,
            current_move_pct=0.0,
            floor_pct=0.08,
            trail_drop_pct=0.2,
            age_minutes=10.0,
        )

    def open_long(
        self,
        *,
        entry_price: float = 100.0,
        quantity: float = 0.1,
        age_minutes: int = 10,
    ) -> None:
        self.position = (
            self.next_position_id,
            "LONG",
            quantity,
            entry_price,
            self.now - timedelta(minutes=age_minutes),
        )
        self.next_position_id += 1

    def cycle(self, row, prev_row=None) -> RsiObservation:
        self.events = []
        self.orders = []
        self.changes = []
        position_before = self.position

        if row is None:
            candle_state = "NO_CLOSED_CANDLE"
        else:
            open_time = row[0]
            self._strategy_event(
                event_type="TICK",
                decision=None,
                reason="LOOP",
                price=float(row[4]) if row[4] is not None else None,
                candle_open_time=open_time,
                info={
                    "last_processed": str(self.last_processed_open_time),
                    "open_time": str(open_time),
                },
            )
            if self.last_processed_open_time == open_time:
                candle_state = "ALREADY_PROCESSED"
                self._strategy_event(
                    event_type="IDLE",
                    decision=None,
                    reason="NO_NEW_CANDLE",
                    price=float(row[4]) if row[4] is not None else None,
                    candle_open_time=open_time,
                    info={
                        "open_time": str(open_time),
                        "last_processed": str(self.last_processed_open_time),
                    },
                )
            else:
                candle_state = "NEW_CANDLE"
                self.last_processed_open_time = open_time
                self.module.run_strategy(row, prev_row=prev_row)

        strategy_events = [
            event for event in self.events if event.channel == "strategy"
        ]
        signal = next(
            (
                event.payload.get("decision")
                for event in strategy_events
                if event.payload.get("event_type") in {"SIGNAL", "EXIT_SIGNAL", "EXIT_TIME"}
            ),
            None,
        )
        if signal is None:
            signal = next(
                (
                    event.payload.get("decision")
                    for event in reversed(strategy_events)
                    if event.payload.get("decision") is not None
                ),
                None,
            )
        reason = (
            self.orders[-1].reason
            if self.orders
            else next(
                (
                    event.payload.get("reason")
                    for event in reversed(strategy_events)
                    if event.payload.get("event_type") not in {"RUN_START", "RUN_END", "TICK"}
                ),
                None,
            )
        )
        action = (
            f"{self.orders[-1].intent}_{self.orders[-1].side}"
            if self.orders
            else "NO_ACTION"
        )
        observation = RsiObservation(
            action=action,
            reason=reason,
            signal=signal,
            candle_state=candle_state,
            position_before=position_before,
            position_after=self.position,
            order_attempts=tuple(self.orders),
            captured_events=tuple(self.events),
            state_changes=tuple(self.changes),
        )
        self.observations.append(observation)
        return observation

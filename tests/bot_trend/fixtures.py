from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any


OPEN_TIME = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ExecutionAttempt:
    side: str
    is_exit: bool
    price: float
    quantity: float
    reason: str


@dataclass(frozen=True)
class PositionMutation:
    operation: str
    before: Any
    after: Any
    reason: str | None


@dataclass(frozen=True)
class TrendObservation:
    cycle: int
    input_open_time: datetime | None
    candle_state: str
    position_before: Any
    position_after: Any
    strategy_events: tuple[dict[str, Any], ...]
    execution_attempts: tuple[ExecutionAttempt, ...]
    position_mutations: tuple[PositionMutation, ...]
    heartbeat_calls: tuple[dict[str, Any], ...]
    state_changes: tuple[tuple[str, Any, Any], ...]
    operation_log: tuple[str, ...]
    observed_action: str
    observed_reason: str | None
    final_decision: Any


class StrictFakeExchange:
    def __getattr__(self, name: str):
        raise AssertionError(f"unexpected real exchange boundary call: {name}")


def trend_rows(*, minute: int = 0, previous: float = 101.5, current: float = 102.0):
    """100 descending DB-shaped rows with a stable UP trend and configurable edge."""
    closes = [100.0] * 70 + [102.0] * 28 + [float(previous), float(current)]
    start = OPEN_TIME + timedelta(minutes=minute - len(closes) + 1)
    rows = [
        ("BTCUSDC", "1m", start + timedelta(minutes=i), close, close, 50.0)
        for i, close in enumerate(closes)
    ]
    return list(reversed(rows))


def flat_rows(*, minute: int = 0, count: int = 100, price: float = 100.0):
    start = OPEN_TIME + timedelta(minutes=minute - count + 1)
    rows = [
        ("BTCUSDC", "1m", start + timedelta(minutes=i), price, price, 50.0)
        for i in range(count)
    ]
    return list(reversed(rows))


def downtrend_rows(*, minute: int = 0):
    closes = [100.0] * 70 + [98.0] * 28 + [98.5, 98.0]
    start = OPEN_TIME + timedelta(minutes=minute - len(closes) + 1)
    return list(reversed([
        ("BTCUSDC", "1m", start + timedelta(minutes=i), close, close, 50.0)
        for i, close in enumerate(closes)
    ]))


class TrendStatefulHarness:
    """Deterministic, test-only driver around the current TREND runtime oracle."""

    def __init__(self, module, monkeypatch) -> None:
        self.module = module
        self.monkeypatch = monkeypatch
        self.production_execute_and_record = module.execute_and_record
        self.production_safe_close_if_open = module.safe_close_if_open
        self.exchange = StrictFakeExchange()
        self.now = OPEN_TIME + timedelta(minutes=10)
        self.rows = trend_rows()
        self.last_processed_open_time = None
        self.position = None
        self.next_position_id = 1
        self.runtime_mode = "NORMAL"
        self.runtime_enabled = True
        self.trading_mode = "PAPER"
        self.regime_allow = True
        self.allow_entry = True
        self.allow_exit = True
        self.execution_ledger_ok = True
        self.execution_live_attempted = True
        self.execution_live_ok = True
        self.execution_order_accepted: bool | None = None
        self.execution_executed_qty: float | None = None
        self.execution_fully_executed: bool | None = None
        self.time_exit_enabled = False
        self.max_position_minutes = 90
        self.profit_lock_state = "NONE"
        self.guarded_profit_triggered = False
        self.sizing_qty = 0.1
        self.events: list[dict[str, Any]] = []
        self.attempts: list[ExecutionAttempt] = []
        self.mutations: list[PositionMutation] = []
        self.heartbeats: list[dict[str, Any]] = []
        self.operation_log: list[str] = []
        self.exit_evidence: dict[str, float] = {}
        self.observations: list[TrendObservation] = []
        self.module.LAST_TREND_STATE = None
        self._install()

    def _install(self) -> None:
        harness = self

        class FakeDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return harness.now if tz is None else harness.now.astimezone(tz)

        self.monkeypatch.setattr(self.module, "datetime", FakeDateTime)
        self.monkeypatch.setattr(self.module, "get_latest_candles", lambda limit: self.rows)
        self.monkeypatch.setattr(self.module, "get_exchange_client", lambda: self.exchange)
        self.monkeypatch.setattr(self.module, "get_db_conn", self._unexpected_db)
        self.monkeypatch.setattr(self.module, "get_runtime_snapshot", self._runtime_snapshot)
        self.monkeypatch.setattr(self.module, "emit_strategy_event", self._strategy_event)
        self.monkeypatch.setattr(self.module, "emit_regime_gate_event", self._regime_event)
        self.monkeypatch.setattr(self.module, "heartbeat", self._heartbeat)
        self.monkeypatch.setattr(self.module, "get_open_position", lambda: self.position)
        self.monkeypatch.setattr(self.module, "execute_and_record", self._execute)
        self.monkeypatch.setattr(self.module, "close_position", self._close)
        self.monkeypatch.setattr(self.module, "safe_close_if_open", self._panic_close)
        self.monkeypatch.setattr(self.module, "set_mode", self._set_mode)
        self.monkeypatch.setattr(self.module, "decide_regime_gate", self._regime_gate)
        self.monkeypatch.setattr(self.module, "compute_qty_from_notional", self._size)
        self.monkeypatch.setattr(self.module, "get_user_settings_snapshot", lambda: {})
        self.monkeypatch.setattr(self.module, "get_recent_win_streak", self._win_streak)
        self.monkeypatch.setattr(self.module, "hard_time_exit_enabled", lambda: self.time_exit_enabled)
        self.monkeypatch.setattr(self.module, "load_position_path_snapshot", self._position_path)
        self.monkeypatch.setattr(self.module, "evaluate_profit_lock", self._profit_lock)
        self.monkeypatch.setattr(self.module, "evaluate_guarded_profit", self._guarded_profit)
        self.monkeypatch.setattr(self.module, "emit_profit_lock_event_once", self._profit_lock_event)
        self.monkeypatch.setattr(self.module, "ADAPTIVE_EARLY_CUT_SHADOW_ENABLED", False)
        self.monkeypatch.setattr(self.module, "GUARDED_PROFIT_ENABLED", False)
        self.monkeypatch.setattr(self.module, "DAILY_MAX_LOSS_PCT", 0.0)
        self.monkeypatch.setattr(self.module, "DISABLE_HOURS_SET", set())
        self.monkeypatch.setattr(self.module, "MAX_DIST_FROM_EMA_FAST_PCT", 0.6)
        self.monkeypatch.setattr(self.module, "EMA_SLOPE_MIN_PCT", 0.0)

    def _unexpected_db(self, *_args, **_kwargs):
        raise AssertionError("unexpected real database boundary call")

    def _runtime_snapshot(self, **_kwargs):
        return {
            "bc": SimpleNamespace(
                mode=self.runtime_mode,
                enabled=self.runtime_enabled,
                regime_enabled=True,
                regime_mode="ENFORCE",
            ),
            "cfg_effective": SimpleNamespace(
                trading_mode=self.trading_mode,
                time_exit_enabled=self.time_exit_enabled,
                max_position_minutes=self.max_position_minutes,
                symbol="BTCUSDC",
                interval="1m",
                quote_asset="USDC",
                live_orders_enabled=self.trading_mode == "LIVE",
            ),
            "heartbeat": {"fixture": True},
            "allowed_orders_entry": self.allow_entry,
            "allowed_orders_exit": self.allow_exit,
            "allow_meta_entry": {"why": "fixture-entry"},
            "allow_meta_exit": {"why": "fixture-exit"},
        }

    def _strategy_event(self, **payload):
        self.events.append(dict(payload))
        self.operation_log.append(f"strategy_event:{payload.get('event_type')}")

    def _regime_event(self, **payload):
        event = {"event_type": "REGIME_GATE", **payload}
        self.events.append(event)
        self.operation_log.append("strategy_event:REGIME_GATE")

    def _heartbeat(self, info):
        self.heartbeats.append(dict(info))
        self.operation_log.append("heartbeat:update")

    def _regime_gate(self, **kwargs):
        allow = self.regime_allow if kwargs.get("decision") == "ENTRY_CHECK" else True
        return SimpleNamespace(allow=allow, why="fixture", regime="TREND", meta={})

    def _execute(self, **kwargs):
        attempt = ExecutionAttempt(
            side=str(kwargs["side"]),
            is_exit=bool(kwargs["is_exit"]),
            price=float(kwargs["price"]),
            quantity=float(kwargs["qty_btc"]),
            reason=str(kwargs["reason"]),
        )
        self.attempts.append(attempt)
        self.operation_log.append("execution:exit" if attempt.is_exit else "execution:entry")
        allowed = self.allow_exit if attempt.is_exit else self.allow_entry
        live_attempted = self.trading_mode == "LIVE" and allowed and self.execution_live_attempted
        live_ok = self.execution_live_ok if self.trading_mode == "LIVE" else True
        if not self.execution_ledger_ok:
            self._strategy_event(
                event_type="BLOCKED", decision=attempt.side,
                reason="DB_GUARD_DUPLICATE", price=attempt.price,
                candle_open_time=kwargs["candle_open_time"],
                info={
                    "is_exit": attempt.is_exit,
                    "qty_btc": attempt.quantity,
                    "reason_text": attempt.reason,
                },
            )
            return {"ledger_ok": False, "live_attempted": False, "live_ok": False,
                    "blocked_reason": "DB_GUARD_DUPLICATE"}
        if self.trading_mode == "LIVE" and not allowed:
            return {"ledger_ok": True, "live_attempted": False, "live_ok": False,
                    "blocked_reason": "LIVE_ORDER_SUPPRESSED"}
        executed_qty = (
            self.execution_executed_qty
            if self.execution_executed_qty is not None
            else attempt.quantity if live_attempted and live_ok else 0.0
        )
        executed = bool(live_attempted and executed_qty > 0.0)
        order_accepted = (
            self.execution_order_accepted
            if self.execution_order_accepted is not None else executed
        )
        fully_executed = (
            self.execution_fully_executed
            if self.execution_fully_executed is not None else executed and live_ok
        )
        if not attempt.is_exit and (self.trading_mode != "LIVE" or executed):
            before = self.position
            self.position = (
                self.next_position_id,
                "LONG" if attempt.side == "BUY" else "SHORT",
                executed_qty if self.trading_mode == "LIVE" else attempt.quantity,
                attempt.price,
                self.now,
            )
            self.next_position_id += 1
            self._mutation("OPEN", before, self.position, attempt.reason)
        caller_live_ok = bool(fully_executed)
        result = {"ledger_ok": True,
                  "live_attempted": live_attempted,
                  "order_accepted": order_accepted, "executed": executed,
                  "fully_executed": fully_executed,
                  "executed_qty": executed_qty,
                  "requested_qty": attempt.quantity,
                  "live_ok": caller_live_ok, "blocked_reason": None}
        reduce_partial = (attempt.is_exit and self.trading_mode == "LIVE" and executed
                          and not fully_executed and self.position is not None)
        if reduce_partial:
            result["live_ok"] = False
        if self.trading_mode == "LIVE" and live_attempted:
            event_live_ok = bool(live_ok)
            self._strategy_event(
                event_type="LIVE_ORDER_SENT", decision=attempt.side,
                reason=(
                    "OK" if event_live_ok else
                    "ORDER_ACCEPTED_PENDING_FILL" if order_accepted else
                    "ORDER_REJECTED" if not attempt.is_exit else
                    "ACK_NO_FILL"
                ),
                price=attempt.price, candle_open_time=kwargs["candle_open_time"],
                info={"is_exit": attempt.is_exit, "result": result},
            )
        if reduce_partial:
            evidence_id = "fixture-exit-order"
            previous = self.exit_evidence.get(evidence_id, 0.0)
            delta = max(0.0, executed_qty - previous)
            self.exit_evidence[evidence_id] = max(previous, executed_qty)
            if delta > 0:
                before = self.position
                remaining = max(0.0, float(before[2]) - delta)
                self.position = (before[0], before[1], remaining, before[3], before[4])
                self._mutation("REDUCE", before, self.position, attempt.reason)
        return result

    def _close(self, *, exit_price, reason, open_time):
        before = self.position
        self.position = None
        self._mutation("CLOSE", before, None, reason)
        return True

    def _panic_close(self, *, current_price, candle_open_time, bc, cfg_effective):
        if self.position is None:
            return {"position": None, "result": None, "closed": False}
        position = self.position
        side = "SELL" if self.position[1] == "LONG" else "BUY"
        result = self._execute(
            side=side, price=current_price, qty_btc=self.position[2],
            reason="PANIC", candle_open_time=candle_open_time,
            cfg_used=cfg_effective, allow_live_orders=True,
            allow_meta={}, is_exit=True, pos_id=self.position[0],
        )
        closed = False
        if result["ledger_ok"] and (
            self.trading_mode != "LIVE" or result["live_ok"]
        ):
            self._close(exit_price=current_price, reason="PANIC", open_time=candle_open_time)
            closed = True
        return {"position": position, "result": result, "closed": closed}

    def _mutation(self, operation, before, after, reason):
        self.mutations.append(PositionMutation(operation, before, after, reason))
        self.operation_log.append(f"state_change:{operation.lower()}")

    def _set_mode(self, mode, reason=None):
        before = self.runtime_mode
        self.runtime_mode = str(mode)
        self.operation_log.append("state_change:mode")
        self.mutations.append(PositionMutation("MODE", before, self.runtime_mode, reason))

    def _size(self, *_args, **_kwargs):
        return self.sizing_qty, {"source": "trend-characterization"}

    def _win_streak(self, **_kwargs):
        return SimpleNamespace(
            eligible=False, checked=3, required=3, streak=0, source="fixture",
            error=None, boost_candidate=False, boost_allowed=False,
            boost_block_reason=None, prev_net_1=None, prev_net_2=None,
            prev_net_3=None, last_exit_reason=None, last_boost_exit_reason=None,
            last_trade_gross_pct=None, rolling_5_gross_pct_avg=None,
        )

    def _position_path(self, **_kwargs):
        entry = float(self.position[3]) if self.position else 100.0
        return SimpleNamespace(
            bars_seen=4, max_high=entry * 1.01, min_low=entry * 0.995,
            mfe_abs=entry * 0.01, mae_abs=entry * 0.005,
        )

    def _profit_lock(self, **kwargs):
        triggered = self.profit_lock_state == "TRIGGERED"
        armed = self.profit_lock_state == "ARMED"
        return SimpleNamespace(
            triggered=triggered,
            reason_code="TRAIL_DROP" if triggered else ("ARMED_WAITING" if armed else "DISABLED"),
            trigger_type="TRAIL" if triggered else None,
            peak_move_pct=1.0 if (triggered or armed) else 0.0,
            current_move_pct=0.7 if triggered else 0.9,
            age_minutes=10.0,
            floor_pct=0.7,
            trail_drop_pct=0.3,
        )

    def _guarded_profit(self, **kwargs):
        return SimpleNamespace(
            triggered=self.guarded_profit_triggered,
            guard_bucket="090",
            mfe_abs=1.0,
            current_move_abs=0.7,
            floor_abs=0.7,
            age_minutes=10.0,
        )

    def _profit_lock_event(self, **payload):
        self.events.append({"event_type": payload.get("event_type"), **payload})
        self.operation_log.append(f"strategy_event:{payload.get('event_type')}")

    def set_position(self, *, side="LONG", entry_price=102.0, age_minutes=5, qty=0.1):
        self.position = (1, side, qty, entry_price, self.now - timedelta(minutes=age_minutes))
        self.next_position_id = 2

    def advance(self, minutes: int):
        self.now += timedelta(minutes=minutes)

    def cycle(self, rows=None) -> TrendObservation:
        if rows is not None:
            self.rows = rows
        before = self.position
        trend_before = self.module.LAST_TREND_STATE
        start_e, start_a, start_m, start_h, start_l = (
            len(self.events), len(self.attempts), len(self.mutations),
            len(self.heartbeats), len(self.operation_log),
        )
        open_time = self.rows[0][2] if self.rows else None
        if open_time is not None and self.last_processed_open_time == open_time:
            candle_state = "ALREADY_PROCESSED"
            self._strategy_event(
                event_type="IDLE", decision=None, reason="NO_NEW_CANDLE",
                price=float(self.rows[0][3]), candle_open_time=open_time,
                info={"open_time": str(open_time), "last_processed": str(open_time)},
            )
            final_decision = self.module.build_no_new_candle_decision((open_time,))
        else:
            candle_state = "NEW_CANDLE" if open_time is not None else "NO_CLOSED_CANDLE"
            if open_time is not None:
                self.last_processed_open_time = open_time
            final_decision = self.module.run_trend_strategy()

        events = tuple(self.events[start_e:])
        attempts = tuple(self.attempts[start_a:])
        mutations = tuple(self.mutations[start_m:])
        heartbeats = tuple(self.heartbeats[start_h:])
        log = tuple(self.operation_log[start_l:])
        meaningful = [e for e in events if e.get("event_type") not in {"RUN_START", "RUN_END", "REGIME_GATE"}]
        reason = str(meaningful[-1].get("reason")) if meaningful and meaningful[-1].get("reason") is not None else None
        if candle_state == "ALREADY_PROCESSED":
            action = "IDLE"
        elif attempts:
            if attempts[-1].is_exit:
                action = "EXIT" if before is not None and self.position is None else "EXIT_ATTEMPT"
            else:
                action = "ENTRY_ATTEMPT"
        elif before is not None and self.position is not None:
            action = "HOLD"
        elif any(e.get("event_type") == "BLOCKED" for e in events):
            action = "BLOCKED"
        else:
            action = "NO_ACTION"
        changes = () if trend_before == self.module.LAST_TREND_STATE else (
            ("LAST_TREND_STATE", trend_before, self.module.LAST_TREND_STATE),
        )
        observation = TrendObservation(
            cycle=len(self.observations) + 1,
            input_open_time=open_time,
            candle_state=candle_state,
            position_before=before,
            position_after=self.position,
            strategy_events=events,
            execution_attempts=attempts,
            position_mutations=mutations,
            heartbeat_calls=heartbeats,
            state_changes=changes,
            operation_log=log,
            observed_action=action,
            observed_reason=reason,
            final_decision=final_decision,
        )
        self.observations.append(observation)
        return observation

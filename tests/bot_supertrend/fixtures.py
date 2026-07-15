from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from types import MappingProxyType
from typing import Any


OPEN_TIME = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


SCENARIO_FIELDS = frozenset({
    "name", "trading_mode", "allow_execution", "ledger_ok", "requested_qty",
    "executed_qty", "order_accepted", "executed", "fully_executed",
    "order_id", "client_order_id", "status", "exchange_status",
    "blocked_reason", "resp",
})


def freeze_scenario(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze_scenario(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze_scenario(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze_scenario(item) for item in value)
    return value


def _scenario(**values):
    return freeze_scenario(values)


RAW_EXECUTION_SCENARIOS = MappingProxyType({
    "PAPER": _scenario(
        name="PAPER", trading_mode="PAPER", allow_execution=True, ledger_ok=True,
        requested_qty=0.11, executed_qty=None, order_accepted=None, executed=None,
        fully_executed=None, order_id=None, client_order_id=None, status=None,
        exchange_status=None, blocked_reason=None, resp=None,
    ),
    "PAPER_ZERO": _scenario(
        name="PAPER_ZERO", trading_mode="PAPER", allow_execution=True, ledger_ok=True,
        requested_qty=0.0, executed_qty=None, order_accepted=None, executed=None,
        fully_executed=None, order_id=None, client_order_id=None, status=None,
        exchange_status=None, blocked_reason=None, resp=None,
    ),
    "DB_GUARD": _scenario(
        name="DB_GUARD", trading_mode="LIVE", allow_execution=True, ledger_ok=False,
        requested_qty=0.12, executed_qty=None, order_accepted=None, executed=None,
        fully_executed=None, order_id=None, client_order_id=None, status=None,
        exchange_status=None, blocked_reason="DB_GUARD_DUPLICATE", resp=None,
    ),
    "LIVE_SUPPRESSION": _scenario(
        name="LIVE_SUPPRESSION", trading_mode="LIVE", allow_execution=False,
        ledger_ok=True, requested_qty=0.14, executed_qty=None, order_accepted=None,
        executed=None, fully_executed=None, order_id=None, client_order_id=None,
        status=None, exchange_status=None, blocked_reason="LIVE_ORDER_SUPPRESSED",
        resp=None,
    ),
    "ENTRY_REJECTION": _scenario(
        name="ENTRY_REJECTION", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.16, executed_qty=0.0,
        order_accepted=False, executed=False, fully_executed=False,
        order_id=None, client_order_id="cid-rejected-731", status="REJECTED",
        exchange_status="REJECTED", blocked_reason="ORDER_REJECTED",
        resp={"clientOrderId": "cid-rejected-731", "status": "REJECTED",
              "executedQty": "0", "origQty": "0.16",
              "scenario_marker": "rejected-731"},
    ),
    "ENTRY_ACK_ONLY": _scenario(
        name="ENTRY_ACK_ONLY", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.15, executed_qty=0.0,
        order_accepted=True, executed=False, fully_executed=False,
        order_id="oid-ack-663", client_order_id="cid-ack-442", status="NEW",
        exchange_status="NEW", blocked_reason="ORDER_ACCEPTED_PENDING_FILL",
        resp={"orderId": "oid-ack-663", "clientOrderId": "cid-ack-442",
              "status": "NEW", "executedQty": "0", "origQty": "0.15",
              "scenario_marker": "ack-442"},
    ),
    "ENTRY_PARTIAL": _scenario(
        name="ENTRY_PARTIAL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.17, executed_qty=0.06,
        order_accepted=True, executed=True, fully_executed=False,
        order_id="oid-partial-815", client_order_id="cid-partial-208",
        status="PARTIALLY_FILLED", exchange_status="PARTIALLY_FILLED",
        blocked_reason=None,
        resp={"orderId": "oid-partial-815", "clientOrderId": "cid-partial-208",
              "status": "PARTIALLY_FILLED", "executedQty": "0.06",
              "origQty": "0.17", "scenario_marker": "partial-208"},
    ),
    "ENTRY_FULL": _scenario(
        name="ENTRY_FULL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.13, executed_qty=0.13,
        order_accepted=True, executed=True, fully_executed=True,
        order_id="oid-full-229", client_order_id="cid-full-504", status="FILLED",
        exchange_status="FILLED", blocked_reason=None,
        resp={"orderId": "oid-full-229", "clientOrderId": "cid-full-504",
              "status": "FILLED", "executedQty": "0.13", "origQty": "0.13",
              "scenario_marker": "full-504"},
    ),
    "ENTRY_PARTIAL_CYCLE": _scenario(
        name="ENTRY_PARTIAL_CYCLE", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.04,
        order_accepted=True, executed=True, fully_executed=False,
        order_id="oid-entry-partial-404", client_order_id="cid-entry-partial-104",
        status="PARTIALLY_FILLED", exchange_status="PARTIALLY_FILLED",
        blocked_reason=None,
        resp={"orderId": "oid-entry-partial-404",
              "clientOrderId": "cid-entry-partial-104", "status": "PARTIALLY_FILLED",
              "executedQty": "0.04", "origQty": "0.10",
              "scenario_marker": "entry-partial-cycle-104"},
    ),
    "EXIT_REJECTION": _scenario(
        name="EXIT_REJECTION", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.0,
        order_accepted=False, executed=False, fully_executed=False,
        order_id=None, client_order_id="cid-exit-rejected-701", status="REJECTED",
        exchange_status="REJECTED", blocked_reason="ACK_NO_FILL",
        resp={"clientOrderId": "cid-exit-rejected-701", "status": "REJECTED",
              "executedQty": "0", "origQty": "0.10",
              "scenario_marker": "exit-rejected-701"},
    ),
    "EXIT_ACK_ONLY": _scenario(
        name="EXIT_ACK_ONLY", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.0,
        order_accepted=True, executed=False, fully_executed=False,
        order_id="oid-exit-ack-702", client_order_id="cid-exit-ack-702", status="NEW",
        exchange_status="NEW", blocked_reason="ACK_NO_FILL",
        resp={"orderId": "oid-exit-ack-702", "clientOrderId": "cid-exit-ack-702",
              "status": "NEW", "executedQty": "0", "origQty": "0.10",
              "scenario_marker": "exit-ack-702"},
    ),
    "EXIT_PARTIAL": _scenario(
        name="EXIT_PARTIAL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.04,
        order_accepted=True, executed=True, fully_executed=False,
        order_id="oid-exit-partial-703", client_order_id="cid-exit-partial-703",
        status="PARTIALLY_FILLED", exchange_status="PARTIALLY_FILLED",
        blocked_reason=None,
        resp={"orderId": "oid-exit-partial-703", "clientOrderId": "cid-exit-partial-703",
              "status": "PARTIALLY_FILLED", "executedQty": "0.04", "origQty": "0.10",
              "scenario_marker": "exit-partial-703"},
    ),
    "EXIT_FULL": _scenario(
        name="EXIT_FULL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.10,
        order_accepted=True, executed=True, fully_executed=True,
        order_id="oid-exit-full-704", client_order_id="cid-exit-full-704", status="FILLED",
        exchange_status="FILLED", blocked_reason=None,
        resp={"orderId": "oid-exit-full-704", "clientOrderId": "cid-exit-full-704",
              "status": "FILLED", "executedQty": "0.10", "origQty": "0.10",
              "scenario_marker": "exit-full-704"},
    ),
    "PANIC_REJECTION": _scenario(
        name="PANIC_REJECTION", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.0,
        order_accepted=False, executed=False, fully_executed=False,
        order_id=None, client_order_id="cid-panic-rejected-801", status="REJECTED",
        exchange_status="REJECTED", blocked_reason="ACK_NO_FILL",
        resp={"clientOrderId": "cid-panic-rejected-801", "status": "REJECTED",
              "executedQty": "0", "origQty": "0.10",
              "scenario_marker": "panic-rejected-801"},
    ),
    "PANIC_DB_GUARD": _scenario(
        name="PANIC_DB_GUARD", trading_mode="LIVE", allow_execution=True,
        ledger_ok=False, requested_qty=0.10, executed_qty=None,
        order_accepted=None, executed=None, fully_executed=None,
        order_id=None, client_order_id=None, status=None, exchange_status=None,
        blocked_reason="DB_GUARD_DUPLICATE", resp=None,
    ),
    "PANIC_ACK_ONLY": _scenario(
        name="PANIC_ACK_ONLY", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.0,
        order_accepted=True, executed=False, fully_executed=False,
        order_id="oid-panic-ack-802", client_order_id="cid-panic-ack-802", status="NEW",
        exchange_status="NEW", blocked_reason="ACK_NO_FILL",
        resp={"orderId": "oid-panic-ack-802", "clientOrderId": "cid-panic-ack-802",
              "status": "NEW", "executedQty": "0", "origQty": "0.10",
              "scenario_marker": "panic-ack-802"},
    ),
    "PANIC_PARTIAL": _scenario(
        name="PANIC_PARTIAL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.04,
        order_accepted=True, executed=True, fully_executed=False,
        order_id="oid-panic-partial-803", client_order_id="cid-panic-partial-803",
        status="PARTIALLY_FILLED", exchange_status="PARTIALLY_FILLED", blocked_reason=None,
        resp={"orderId": "oid-panic-partial-803", "clientOrderId": "cid-panic-partial-803",
              "status": "PARTIALLY_FILLED", "executedQty": "0.04", "origQty": "0.10",
              "scenario_marker": "panic-partial-803"},
    ),
    "PANIC_FULL": _scenario(
        name="PANIC_FULL", trading_mode="LIVE", allow_execution=True,
        ledger_ok=True, requested_qty=0.10, executed_qty=0.10,
        order_accepted=True, executed=True, fully_executed=True,
        order_id="oid-panic-full-804", client_order_id="cid-panic-full-804", status="FILLED",
        exchange_status="FILLED", blocked_reason=None,
        resp={"orderId": "oid-panic-full-804", "clientOrderId": "cid-panic-full-804",
              "status": "FILLED", "executedQty": "0.10", "origQty": "0.10",
              "scenario_marker": "panic-full-804"},
    ),
})


def candle(*, minute=0, price=100.0, direction=1, atr=1.0):
    return (OPEN_TIME + timedelta(minutes=minute), price, 99.0, 50.0,
            atr, price - 0.5, direction)


@dataclass(frozen=True)
class Attempt:
    side: str
    is_exit: bool
    qty: float
    reason: str


class SupertrendHarness:
    def __init__(self, module, monkeypatch):
        self.module = module
        self.monkeypatch = monkeypatch
        self.now = OPEN_TIME + timedelta(minutes=10)
        self.position = None
        self.mode = "NORMAL"
        self.enabled = True
        self.trading_mode = "PAPER"
        self.allow_entry = True
        self.allow_exit = True
        self.regime_allow = True
        self.qty = 0.1
        self.profit_lock_state = "NONE"
        self.time_exit_enabled = False
        self.max_position_minutes = 90
        self.strategy_cycle_invocations = 0
        self.execution_scenario = RAW_EXECUTION_SCENARIOS["ENTRY_FULL"]
        self.events: list[dict[str, Any]] = []
        self.attempts: list[Attempt] = []
        self.mutations: list[tuple] = []
        self.operation_log: list[str] = []
        self.last_execution_result = None
        self._install()

    def _install(self):
        harness = self

        class FakeDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return harness.now if tz is None else harness.now.astimezone(tz)

        m = self.module
        self.production_execute_and_record = m.execute_and_record
        self.monkeypatch.setattr(m, "datetime", FakeDateTime)
        self.monkeypatch.setattr(m, "emit_strategy_event", self._event)
        self.monkeypatch.setattr(m, "emit_regime_gate_event", self._regime_event)
        self.monkeypatch.setattr(m, "heartbeat", self._heartbeat)
        self.monkeypatch.setattr(m, "get_runtime_snapshot", self._snapshot)
        self.monkeypatch.setattr(m, "get_open_position", lambda: self.position)
        self.monkeypatch.setattr(m, "execute_and_record", self._execute)
        self.monkeypatch.setattr(m, "close_position", self._close)
        self.monkeypatch.setattr(m, "set_mode", self._set_mode)
        self.monkeypatch.setattr(m, "decide_regime_gate", self._regime)
        self.monkeypatch.setattr(m, "compute_qty_from_notional_safe", self._size)
        self.monkeypatch.setattr(m, "get_exchange_client", lambda: StrictExchangeBoundary())
        self.monkeypatch.setattr(m, "get_db_conn", self._unexpected_db)
        self.monkeypatch.setattr(m.time, "sleep", self._unexpected_sleep)
        self.monkeypatch.setattr(m, "get_user_settings_snapshot", lambda: {})
        self.monkeypatch.setattr(m, "get_recent_win_streak", self._wins)
        self.monkeypatch.setattr(m, "hard_time_exit_enabled", lambda: self.time_exit_enabled)
        self.monkeypatch.setattr(m, "load_position_path_snapshot", self._path)
        self.monkeypatch.setattr(m, "evaluate_profit_lock", self._profit_lock)
        self.monkeypatch.setattr(m, "emit_profit_lock_event_once", self._profit_event)
        self.monkeypatch.setattr(m, "DAILY_MAX_LOSS_PCT", 0.0)
        self.monkeypatch.setattr(m, "DISABLE_HOURS_SET", set())

    def _event(self, **payload):
        self.events.append(dict(payload))
        self.operation_log.append(f"event:{payload['event_type']}")

    def _regime_event(self, **payload):
        self.events.append({"event_type": "REGIME_GATE", **payload})
        self.operation_log.append("event:REGIME_GATE")

    def _heartbeat(self, info):
        self.operation_log.append("heartbeat:strategy")

    def _snapshot(self, **_kwargs):
        cfg = SimpleNamespace(
            trading_mode=self.trading_mode,
            time_exit_enabled=self.time_exit_enabled,
            max_position_minutes=self.max_position_minutes,
            symbol="BTCUSDC", interval="1m", quote_asset="USDC",
            live_orders_enabled=self.trading_mode == "LIVE",
        )
        return {
            "bc": SimpleNamespace(mode=self.mode, enabled=self.enabled,
                                  regime_enabled=True, regime_mode="ENFORCE"),
            "cfg_effective": cfg, "heartbeat": {"fixture": True},
            "allowed_orders_entry": self.allow_entry,
            "allowed_orders_exit": self.allow_exit,
            "allow_meta_entry": {}, "allow_meta_exit": {},
        }

    def _execute(self, **kwargs):
        attempt = Attempt(str(kwargs["side"]), bool(kwargs["is_exit"]),
                          float(kwargs["qty_btc"]), str(kwargs["reason"]))
        self.attempts.append(attempt)
        self.operation_log.append("execution:exit" if attempt.is_exit else "execution:entry")
        scenario = self.execution_scenario
        allowed = scenario["allow_execution"]
        if not scenario["ledger_ok"]:
            self._event(event_type="BLOCKED", decision=attempt.side,
                        reason="DB_GUARD_DUPLICATE", price=kwargs["price"],
                        candle_open_time=kwargs["candle_open_time"], info={})
            result = {"ledger_ok": False, "live_attempted": False, "live_ok": False,
                      "blocked_reason": scenario["blocked_reason"],
                      "client_order_id": None, "resp": None}
            self.last_execution_result = result
            return result
        self._event(
            event_type="SIM_ORDER_CREATED", decision=attempt.side,
            reason="LEDGER_OK", price=kwargs["price"],
            candle_open_time=kwargs["candle_open_time"],
            info={"is_exit": attempt.is_exit, "qty_btc": attempt.qty,
                  "reason_text": attempt.reason},
        )
        if scenario["trading_mode"] == "LIVE" and not allowed:
            self._event(
                event_type="BLOCKED", decision=attempt.side,
                reason="LIVE_EXIT_NOT_ATTEMPTED" if attempt.is_exit else "LIVE_ENTRY_NOT_ATTEMPTED",
                price=kwargs["price"], candle_open_time=kwargs["candle_open_time"],
                info={"is_exit": attempt.is_exit, "reason_text": attempt.reason},
            )
            result = {"ledger_ok": True, "live_attempted": False, "live_ok": False,
                      "blocked_reason": scenario["blocked_reason"],
                      "client_order_id": None, "resp": None}
            self.last_execution_result = result
            return result
        if scenario["trading_mode"] != "LIVE":
            result = {"ledger_ok": True, "live_attempted": False, "live_ok": True,
                      "blocked_reason": None, "client_order_id": None, "resp": None}
            self.last_execution_result = result
            return result
        executed_qty = scenario["executed_qty"]
        executed = bool(scenario["executed"])
        accepted = bool(scenario["order_accepted"])
        live_ok = executed
        client_order_id = scenario["client_order_id"]
        order_id = scenario["order_id"]
        exchange_status = scenario["exchange_status"]
        raw_resp = dict(scenario["resp"])
        self._event(
            event_type="LIVE_ORDER_SENT", decision=attempt.side,
            reason=(
                "OK" if live_ok else
                "ORDER_ACCEPTED_PENDING_FILL" if not attempt.is_exit and accepted else
                "ORDER_REJECTED" if not attempt.is_exit else "ACK_NO_FILL"
            ),
            price=kwargs["price"], candle_open_time=kwargs["candle_open_time"],
            info={"is_exit": attempt.is_exit, "executed_qty": executed_qty},
        )
        if not attempt.is_exit and scenario["trading_mode"] == "LIVE" and live_ok:
            before = self.position
            self.position = (1, "LONG", float(executed_qty), float(kwargs["price"]), self.now)
            self.mutations.append(("OPEN", before, self.position))
            self.operation_log.append("mutation:open")
        if attempt.is_exit:
            result = {"ledger_ok": True, "live_attempted": True,
                      "live_ok": live_ok,
                      "blocked_reason": scenario["blocked_reason"],
                      "client_order_id": client_order_id, "resp": raw_resp}
        else:
            result = {
                "ledger_ok": True, "live_attempted": True,
                "order_accepted": accepted, "executed": executed,
                "fully_executed": scenario["fully_executed"],
                "executed_qty": float(executed_qty),
                "requested_qty": scenario["requested_qty"],
                "order_id": order_id, "exchange_status": exchange_status,
                "live_ok": live_ok,
                "blocked_reason": scenario["blocked_reason"],
                "client_order_id": client_order_id, "resp": raw_resp,
            }
        self.last_execution_result = result
        return result

    def _unexpected_db(self, *_args, **_kwargs):
        raise AssertionError("unexpected real DB boundary")

    def _unexpected_sleep(self, *_args, **_kwargs):
        raise AssertionError("unexpected sleep/background work")

    def _close(self, *, exit_price, reason, candle_open_time):
        before = self.position
        self.position = None
        self.mutations.append(("CLOSE", before, None, reason))
        self.operation_log.append("mutation:close")
        return True

    def _set_mode(self, mode, reason=None):
        before = self.mode
        self.mode = mode
        self.mutations.append(("MODE", before, mode, reason))
        self.operation_log.append("mutation:mode")

    def _regime(self, **kwargs):
        allow = self.regime_allow if kwargs.get("decision") == "ENTRY_CHECK" else True
        return SimpleNamespace(allow=allow, why="fixture", regime="TREND", meta={})

    def _size(self, *_args, **_kwargs):
        return self.qty, {"source": "fixture"}

    def _wins(self, **_kwargs):
        return SimpleNamespace(
            eligible=False, checked=3, required=3, streak=0, source="fixture",
            error=None, boost_candidate=False, boost_allowed=False,
            boost_block_reason=None, prev_net_1=None, prev_net_2=None,
            prev_net_3=None, last_exit_reason=None, last_boost_exit_reason=None,
            last_trade_gross_pct=None, rolling_5_gross_pct_avg=None,
        )

    def _path(self, **_kwargs):
        entry = float(self.position[3]) if self.position else 100.0
        return SimpleNamespace(bars_seen=4, max_high=entry * 1.02,
                               min_low=entry * 0.99)

    def _profit_lock(self, **_kwargs):
        triggered = self.profit_lock_state == "TRIGGERED"
        armed = self.profit_lock_state == "ARMED"
        return SimpleNamespace(
            triggered=triggered,
            reason_code="TRAIL_DROP" if triggered else ("ARMED_WAITING" if armed else "DISABLED"),
            trigger_type="TRAIL" if triggered else None,
            peak_move_pct=1.0 if (triggered or armed) else 0.0,
            current_move_pct=0.7, age_minutes=10.0, floor_pct=0.7,
            trail_drop_pct=0.3,
        )

    def _profit_event(self, **payload):
        self.events.append({"event_type": payload["event_type"], **payload})
        self.operation_log.append(f"event:{payload['event_type']}")

    def set_position(self, *, price=100.0, age=5):
        self.position = (1, "LONG", 0.1, price, self.now - timedelta(minutes=age))

    def apply_execution_scenario(self, scenario):
        if not isinstance(scenario, Mapping):
            raise TypeError("execution scenario must be a mapping")
        actual_fields = frozenset(scenario)
        if actual_fields != SCENARIO_FIELDS:
            missing = sorted(SCENARIO_FIELDS - actual_fields)
            extra = sorted(actual_fields - SCENARIO_FIELDS)
            raise ValueError(f"invalid execution scenario fields: missing={missing} extra={extra}")
        frozen = freeze_scenario(scenario)
        self.execution_scenario = frozen
        self.trading_mode = frozen["trading_mode"]
        self.allow_entry = frozen["allow_execution"]
        self.allow_exit = frozen["allow_execution"]
        self.qty = frozen["requested_qty"]

    def strategy_cycle(self, latest, prev):
        start = (len(self.events), len(self.attempts), len(self.mutations), len(self.operation_log))
        self.strategy_cycle_invocations += 1
        self.module.run_strategy(latest, prev)
        e, a, mu, log = start
        return SimpleNamespace(
            events=tuple(self.events[e:]), attempts=tuple(self.attempts[a:]),
            mutations=tuple(self.mutations[mu:]),
            operation_log=tuple(self.operation_log[log:]), position=self.position,
        )

class StrictExchangeBoundary:
    def __getattr__(self, name):
        raise AssertionError(f"unexpected real exchange call: {name}")

from __future__ import annotations

import importlib
from collections import Counter

import pytest


@pytest.fixture
def module(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    return importlib.import_module("services.bot_runner.main")


class FakeProcess:
    pid = 123

    def poll(self):
        return None


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.waits = []

    def time(self):
        return self.now

    def wait(self, seconds, shutdown_requested):
        self.waits.append(seconds)
        if shutdown_requested():
            return False
        self.now += seconds
        return True


def row(strategy, symbol, interval, *, enabled=True):
    return {
        "strategy": strategy,
        "symbol": symbol,
        "interval": interval,
        "enabled": enabled,
        "live_orders_enabled": False,
        "regime_enabled": True,
        "regime_mode": "DRY_RUN",
    }


def desired(module, rows):
    return {
        module.BotKey(item["symbol"], item["interval"], item["strategy"]): item
        for item in rows
    }


def run_batch(module, rows, *, stagger=1.5, shutdown=lambda: False):
    clock = FakeClock()
    starts = []
    running = {}
    attempts = {}

    def start_fn(item):
        starts.append((clock.now, item.copy()))
        return FakeProcess()

    candidates = module.ordered_start_candidates(desired(module, rows), running)
    count = module.start_worker_batch(
        candidates,
        running,
        attempts,
        stagger_seconds=stagger,
        start_fn=start_fn,
        now_fn=clock.time,
        wait_fn=clock.wait,
        shutdown_requested=shutdown,
    )
    return clock, starts, running, attempts, count


def test_order_is_deterministic_and_separates_strategies(module):
    rows = [
        row("SUPERTREND", "ETHUSDC", "1m"),
        row("RSI", "SOLUSDC", "5m"),
        row("TREND", "BNBUSDC", "1m"),
        row("RSI", "BNBUSDC", "1m"),
        row("BBRANGE", "ETHUSDC", "1m"),
    ]
    first = module.ordered_start_candidates(desired(module, rows), {})
    second = module.ordered_start_candidates(desired(module, reversed(rows)), {})
    assert [key for key, _ in first] == [key for key, _ in second]
    assert [key.strategy for key, _ in first] == [
        "RSI", "RSI", "BBRANGE", "TREND", "SUPERTREND"
    ]


def test_stagger_occurs_only_between_workers(module):
    clock, starts, _running, _attempts, count = run_batch(
        module,
        [row("RSI", "A", "1m"), row("TREND", "B", "1m"), row("SUPERTREND", "C", "1m")],
    )
    assert count == 3
    assert [when for when, _ in starts] == [0.0, 1.5, 3.0]
    assert clock.waits == [1.5, 1.5]


def test_zero_stagger_starts_same_schedule_without_extra_wait(module):
    clock, starts, _running, _attempts, count = run_batch(
        module,
        [row("RSI", "A", "1m"), row("TREND", "B", "1m")],
        stagger=0,
    )
    assert count == 2
    assert [when for when, _ in starts] == [0.0, 0.0]
    assert clock.waits == [0]


def test_interrupt_during_wait_prevents_later_spawns(module):
    state = {"shutdown": False}
    clock = FakeClock()
    starts = []
    running = {}

    def wait_fn(seconds, _shutdown_requested):
        clock.waits.append(seconds)
        state["shutdown"] = True
        return False

    candidates = module.ordered_start_candidates(
        desired(module, [row("RSI", "A", "1m"), row("TREND", "B", "1m")]),
        running,
    )
    count = module.start_worker_batch(
        candidates,
        running,
        {},
        start_fn=lambda item: starts.append(item) or FakeProcess(),
        now_fn=clock.time,
        wait_fn=wait_fn,
        shutdown_requested=lambda: state["shutdown"],
    )
    assert count == 1
    assert len(starts) == 1


def test_interruptible_wait_checks_shutdown_during_sleep(module):
    clock = {"now": 0.0, "shutdown": False}

    def sleep(seconds):
        clock["now"] += seconds
        clock["shutdown"] = True

    result = module.interruptible_wait(
        1.5,
        lambda: clock["shutdown"],
        monotonic=lambda: clock["now"],
        sleep=sleep,
    )
    assert result is False
    assert clock["now"] <= 0.1


def test_disabled_and_running_slots_are_not_candidates(module):
    enabled = row("RSI", "A", "1m")
    disabled = row("TREND", "B", "1m", enabled=False)
    already_running = module.BotKey("A", "1m", "RSI")
    candidates = module.ordered_start_candidates(
        desired(module, [enabled, disabled]),
        {already_running: object()},
    )
    assert candidates == []


def test_single_worker_restart_has_no_batch_delay(module):
    clock, starts, _running, _attempts, count = run_batch(
        module,
        [row("SUPERTREND", "ETHUSDC", "1m")],
    )
    assert count == 1
    assert starts[0][0] == 0.0
    assert clock.waits == []


def test_start_preserves_worker_command_and_environment(monkeypatch, module):
    captured = {}

    def popen(cmd, env):
        captured["cmd"] = cmd
        captured["env"] = env
        return FakeProcess()

    monkeypatch.setattr(module.subprocess, "Popen", popen)
    item = row("TREND", "ETHUSDC", "5m")
    module.start_bot(item)
    assert captured["cmd"] == module.STRATEGY_CMD["TREND"]
    assert captured["env"]["SYMBOL"] == "ETHUSDC"
    assert captured["env"]["INTERVAL"] == "5m"
    assert captured["env"]["STRATEGY_NAME"] == "TREND"
    assert captured["env"]["TRADING_MODE"] == "PAPER"


def test_28_worker_burst_model_limits_spawns_per_second(module):
    strategies = ["RSI", "BBRANGE", "TREND", "SUPERTREND"]
    symbols = ["BNBUSDC", "BTCUSDC", "ETHUSDC", "SOLUSDC"]
    slots = [
        row(strategy, symbol, interval)
        for strategy in strategies
        for symbol in symbols
        for interval in ("1m", "5m")
    ][:28]
    _clock, starts, _running, _attempts, count = run_batch(
        module, slots, stagger=1.5
    )
    baseline_per_second = Counter(0 for _item in slots)
    per_second = Counter(int(when) for when, _item in starts)
    assert max(baseline_per_second.values()) == 28
    assert count == 28
    assert max(per_second.values()) == 1
    assert starts[-1][0] == pytest.approx(40.5)

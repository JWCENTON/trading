from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.equity_curve import fetch_equity_history


ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc


class CanonicalCursor:
    def __init__(self, baseline, observations):
        self.baseline = baseline
        self.observations = observations
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def fetchone(self):
        return self.baseline

    def fetchall(self):
        return self.observations


def at(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def observation(
    observed_at: datetime, raw: str, adjusted: str, *, peak: bool = False,
):
    return (
        observed_at,
        Decimal(raw),
        Decimal(adjusted),
        Decimal("90"),
        Decimal("10"),
        Decimal("4"),
        Decimal("1"),
        {"open_positions_count": 2 if peak else 1},
    )


def canonical_history(*, short: bool = False):
    baseline_at = at("2026-08-20T08:00:00") if short else at("2026-07-01T08:00:00")
    baseline = (7, baseline_at, Decimal("100"), "f" * 64)
    if short:
        rows = [
            observation(at("2026-08-20T09:00:00"), "101", "101"),
            observation(at("2026-08-25T09:00:00"), "120", "120", peak=True),
            observation(at("2026-08-28T08:00:00"), "115", "112"),
        ]
    else:
        rows = [
            observation(at("2026-07-01T09:00:00"), "101", "101"),
            observation(at("2026-07-29T08:00:00"), "105", "105"),
            observation(at("2026-08-01T00:00:00"), "106", "106"),
            observation(at("2026-08-21T08:00:00"), "110", "110"),
            observation(at("2026-08-25T09:00:00"), "120", "120", peak=True),
            observation(at("2026-08-28T08:00:00"), "115", "112"),
        ]
    cursor = CanonicalCursor(baseline, rows)
    items, metrics = fetch_equity_history(
        cursor, deployment_id="local-live", trading_mode="LIVE",
    )
    return cursor, items, metrics


def test_live_uses_canonical_baseline_current_external_drawdown_and_chart() -> None:
    cursor, items, metrics = canonical_history()

    assert metrics["baseline_date"].isoformat() == "2026-07-01"
    assert metrics["baseline_equity"] == Decimal("100")
    assert metrics["current_waltrade_equity"] == Decimal("115")
    assert metrics["current_account_total"] == Decimal("115")
    assert items[-1]["external_manual_value_usdc"] == Decimal("0")
    assert [item["waltrade_managed_equity_usdc"] for item in items] == [
        Decimal("101"), Decimal("105"), Decimal("106"), Decimal("110"),
        Decimal("120"), Decimal("115"),
    ]
    assert metrics["peak_equity"] == Decimal("120")
    assert metrics["drawdown_from_peak_pct"] == Decimal("-6.666666666666666666666666667")
    assert metrics["since_baseline_abs"] == Decimal("12")
    assert metrics["since_baseline_pct"] == Decimal("12.00")
    sql = " ".join(cursor.statements)
    assert "live_managed_capital_baseline_v1" in sql
    assert "v_live_drawdown_history_observation_v1" in sql
    assert "equity_daily_snapshot_v1" not in sql


def test_live_period_metrics_use_only_available_canonical_history() -> None:
    _cursor, _items, metrics = canonical_history()
    assert metrics["change_7d_abs"] == Decimal("2")
    assert metrics["change_30d_abs"] == Decimal("7")
    assert metrics["month_change_abs"] == Decimal("6")


def test_short_history_does_not_fabricate_30d_or_month_history() -> None:
    _cursor, items, metrics = canonical_history(short=True)
    assert metrics["change_7d_abs"] is None
    assert metrics["change_7d_pct"] is None
    assert metrics["change_30d_abs"] is None
    assert metrics["change_30d_pct"] is None
    assert metrics["month_change_abs"] is None
    assert metrics["month_change_pct"] is None
    assert metrics["current_waltrade_equity"] == Decimal("115")
    assert metrics["drawdown_from_peak_pct"] is not None
    assert len(items) == 3


def test_missing_baseline_fails_closed_without_legacy_fallback() -> None:
    cursor = CanonicalCursor(None, [])
    items, metrics = fetch_equity_history(
        cursor, deployment_id="local-live", trading_mode="LIVE",
    )
    assert items == []
    assert metrics["current_waltrade_equity"] is None
    assert len(cursor.statements) == 1
    assert "equity_daily_snapshot_v1" not in cursor.statements[0]


def test_legacy_incomplete_snapshot_cannot_poison_canonical_current() -> None:
    cursor, _items, metrics = canonical_history(short=True)
    assert metrics["current_waltrade_equity"] == Decimal("115")
    assert all("equity_daily_snapshot_v1" not in sql for sql in cursor.statements)


def test_live_read_is_read_only_and_legacy_history_is_untouched() -> None:
    cursor, _items, _metrics = canonical_history()
    forbidden = ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "ALTER ", "DROP ")
    assert all(not statement.lstrip().upper().startswith(forbidden) for statement in cursor.statements)


def test_existing_frontend_response_contract_remains_compatible() -> None:
    _cursor, items, metrics = canonical_history(short=True)
    assert set(items[-1]) == {
        "snapshot_date", "account_total_value_usdc",
        "external_manual_value_usdc", "waltrade_managed_equity_usdc",
        "available_usdc", "bot_inventory_value_usdc",
        "realized_net_pnl_usdc", "unrealized_pnl_usdc", "fees_usdc",
        "open_positions", "evidence_status", "source_timestamp",
    }
    assert set(metrics) == {
        "current_waltrade_equity", "current_account_total", "change_7d_abs",
        "change_7d_pct", "change_30d_abs", "change_30d_pct",
        "month_open_equity", "month_change_abs", "month_change_pct",
        "peak_equity", "drawdown_from_peak_pct", "baseline_date",
        "baseline_equity", "since_baseline_abs", "since_baseline_pct",
    }
    frontend = (ROOT / "frontend/src/api.ts").read_text()
    assert "export interface UiEquityHistoryResponse" in frontend
    assert "external_manual_value_usdc: number | null" in frontend


def test_live_legacy_writer_is_disabled_while_paper_writer_is_preserved() -> None:
    scheduler = (ROOT / "automation_runner/main.py").read_text()
    function = scheduler.split("def run_daily_equity_snapshot(conn):", 1)[1].split(
        "def run_entry_context_snapshot_refresh", 1,
    )[0]
    assert 'if cfg.trading_mode.upper() == "LIVE":' in function
    assert "upsert_daily_snapshot(" in function
    assert "run_live_drawdown_history_cycle" in scheduler

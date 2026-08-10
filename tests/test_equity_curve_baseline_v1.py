from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from common.equity_curve import (
    EquityObservation,
    EquityPoint,
    _ownership_projection,
    calculate_equity_metrics,
    subtract_external_manual,
    upsert_daily_snapshot,
)


ROOT = Path(__file__).resolve().parents[1]


def point(day: str, managed: str | None, total: str = "100") -> EquityPoint:
    return EquityPoint(
        date.fromisoformat(day),
        Decimal(total),
        Decimal("0") if managed is not None else None,
        Decimal(managed) if managed is not None else None,
        "COMPLETE" if managed is not None else "INCOMPLETE",
    )


def test_decimal_external_subtraction_and_fail_closed() -> None:
    managed, status = subtract_external_manual(
        Decimal("301.123456789123456789"),
        Decimal("175.000000000000000001"),
        ownership_complete=True,
    )
    assert managed == Decimal("126.123456789123456788")
    assert status == "COMPLETE"
    assert subtract_external_manual(
        Decimal("301"), None, ownership_complete=False
    ) == (None, "INCOMPLETE")


def test_7d_30d_month_peak_and_drawdown_metrics() -> None:
    metrics = calculate_equity_metrics([
        point("2026-07-10", "80"),
        point("2026-08-01", "100"),
        point("2026-08-02", "120"),
        point("2026-08-09", "110", "130"),
    ])
    assert metrics["current_waltrade_equity"] == Decimal("110")
    assert metrics["change_7d_abs"] == Decimal("-10")
    assert metrics["change_30d_abs"] == Decimal("30")
    assert metrics["month_open_equity"] == Decimal("100")
    assert metrics["month_change_pct"] == Decimal("10")
    assert metrics["peak_equity"] == Decimal("120")
    assert metrics["drawdown_from_peak_pct"] == Decimal("-8.333333333333333333333333333")
    assert metrics["baseline_date"] == date(2026, 7, 10)
    assert metrics["baseline_equity"] == Decimal("80")
    assert metrics["since_baseline_abs"] == Decimal("30")
    assert metrics["since_baseline_pct"] == Decimal("37.500")


def test_empty_and_short_history_are_insufficient() -> None:
    assert calculate_equity_metrics([])["current_waltrade_equity"] is None
    metrics = calculate_equity_metrics([point("2026-08-09", "100")])
    assert metrics["change_7d_abs"] is None
    assert metrics["change_30d_pct"] is None
    assert metrics["month_change_abs"] == Decimal("0")


def test_baseline_ignores_incomplete_snapshots() -> None:
    metrics = calculate_equity_metrics([
        point("2026-08-01", None),
        point("2026-08-02", "90"),
        point("2026-08-03", "99"),
    ])
    assert metrics["baseline_date"] == date(2026, 8, 2)
    assert metrics["baseline_equity"] == Decimal("90")
    assert metrics["since_baseline_abs"] == Decimal("9")


class SequencedCursor:
    def __init__(self, results):
        self.results = list(results)
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))

    def fetchall(self):
        return self.results.pop(0)


def test_manual_btc_price_moves_total_but_is_subtracted_from_managed() -> None:
    cur = SequencedCursor([
        [],
        [("BTCUSDC", Decimal("0.1"), True)],
        [("BTCUSDC", "BUY", {
            "executed_qty": "0.5", "fee_quantity": "0",
            "fee_currency": "BTC",
        })],
        [("BTCUSDC", Decimal("0.00001"), Decimal("0.00001"), Decimal("1"))],
    ])
    external, bot_value, complete = _ownership_projection(
        cur,
        {"BTC": Decimal("0.6"), "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")},
        {"BTC": Decimal("200"), "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")},
        "USDC",
    )
    assert complete is True
    assert external == Decimal("100")
    assert bot_value == Decimal("20")
    managed, _ = subtract_external_manual(
        Decimal("170"), external, ownership_complete=complete
    )
    assert managed == Decimal("70")


def test_unattributed_below_minimum_inventory_is_canonical_dust() -> None:
    cur = SequencedCursor([
        [],
        [],
        [],
        [("BTCUSDC", Decimal("0.00000001"), Decimal("0.00001"), Decimal("1"))],
    ])
    external, bot_value, complete = _ownership_projection(
        cur,
        {"BTC": Decimal("0.00000002"), "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")},
        {"BTC": Decimal("100000"), "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")},
        "USDC",
    )
    assert complete is True
    assert external == Decimal("0")
    assert bot_value == Decimal("0")


def test_account_level_external_btc_is_valued_at_current_price() -> None:
    quantity = Decimal("0.00271641596")
    cur = SequencedCursor([
        [({
            "contract_version": "ACCOUNT_INVENTORY_OWNERSHIP_V1",
            "asset": "BTC",
            "ownership": "EXTERNAL_OR_MANUAL",
            "quantity": str(quantity),
            "quantity_basis": "AUTHORITATIVE_EVIDENCE",
            "evidence_status": "COMPLETE",
            "unresolved_quantity": "0",
        }, {"deployment_id": "vps-live"})],
        [],
        [],
        [("BTCUSDC", Decimal("0.00000001"), Decimal("0.00001"), Decimal("1"))],
    ])
    prices = {"BTC": Decimal("65000"), "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")}
    external, _, complete = _ownership_projection(
        cur,
        {"BTC": quantity, "ETH": Decimal("0"), "BNB": Decimal("0"), "SOL": Decimal("0")},
        prices,
        "USDC",
        deployment_id="vps-live",
    )
    assert complete is True
    assert external == quantity * prices["BTC"]
    assert "175." not in (ROOT / "common/equity_curve.py").read_text()


def test_recovered_bot_inventory_is_managed_while_external_stays_excluded() -> None:
    external_qty = Decimal("0.002716415960")
    recovered_qty = Decimal("0.000307798920")
    terminal_dust = Decimal("0.000000001405")
    account_qty = Decimal("0.003024216285")
    price = Decimal("120000")
    cur = SequencedCursor([
        [({
            "contract_version": "ACCOUNT_INVENTORY_OWNERSHIP_V1",
            "asset": "BTC",
            "ownership": "EXTERNAL_OR_MANUAL",
            "quantity": str(external_qty),
            "quantity_basis": "AUTHORITATIVE_EVIDENCE",
            "evidence_status": "COMPLETE",
            "unresolved_quantity": "0",
        }, {"deployment_id": "vps-live"})],
        [("BTCUSDC", recovered_qty + terminal_dust, True)],
        [],
        [("BTCUSDC", Decimal("0.00000001"), Decimal("0.00001"), Decimal("1"))],
    ])

    external_value, bot_value, complete = _ownership_projection(
        cur,
        {
            "BTC": account_qty,
            "ETH": Decimal("0"),
            "BNB": Decimal("0"),
            "SOL": Decimal("0"),
        },
        {
            "BTC": price,
            "ETH": Decimal("0"),
            "BNB": Decimal("0"),
            "SOL": Decimal("0"),
        },
        "USDC",
        deployment_id="vps-live",
    )

    assert external_qty + recovered_qty + terminal_dust == account_qty
    assert complete is True
    assert external_value == external_qty * price
    assert bot_value == (recovered_qty + terminal_dust) * price


class UpsertCursor:
    def __init__(self):
        self.params = None

    def execute(self, sql, params):
        assert "ON CONFLICT (deployment_id,snapshot_date) DO UPDATE" in sql
        self.params = params

    def fetchone(self):
        return (7,)


def test_daily_snapshot_upsert_is_idempotent_by_contract() -> None:
    cur = UpsertCursor()
    observation = EquityObservation(
        Decimal("100"), Decimal("0"), Decimal("100"), Decimal("80"),
        Decimal("20"), Decimal("1"), Decimal("2"), Decimal("0.1"), 1,
        "COMPLETE", datetime(2026, 8, 9, 0, 20, tzinfo=timezone.utc),
    )
    first = upsert_daily_snapshot(
        cur, deployment_id="local-live", trading_mode="LIVE",
        observation=observation,
    )
    second = upsert_daily_snapshot(
        cur, deployment_id="local-live", trading_mode="LIVE",
        observation=observation,
    )
    assert first == second == 7


def test_incomplete_snapshot_may_retain_proven_external_value() -> None:
    observation = EquityObservation(
        Decimal("100"), Decimal("0"), None, Decimal("100"), Decimal("0"),
        None, None, None, 0, "INCOMPLETE",
        datetime(2026, 8, 9, tzinfo=timezone.utc),
    )
    assert observation.external_manual_value_usdc == Decimal("0")
    assert observation.waltrade_managed_equity_usdc is None


def test_api_scheduler_and_schema_contracts_are_wired() -> None:
    api = (ROOT / "api/main.py").read_text()
    scheduler = (ROOT / "automation_runner/main.py").read_text()
    migration = (
        ROOT / "db/migrations/20260809_equity_curve_baseline_v1.sql"
    ).read_text()
    assert '@app.get("/ui/equity"' in api
    assert "run_daily_equity_snapshot(conn)" in scheduler
    assert "DAILY_REPORT_HHMM_UTC" in scheduler
    assert "UNIQUE (deployment_id, snapshot_date)" in migration
    assert "waltrade_managed_equity_usdc IS NULL" in migration

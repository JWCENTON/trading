from __future__ import annotations

from decimal import Decimal

import pytest

from common.recent_closed_read_model import (
    calculate_net_pnl_pct,
    resolve_entry_notional_usdc,
)


@pytest.mark.parametrize(
    ("position_id", "symbol", "strategy", "entry_notional", "net_pnl", "expected"),
    [
        (
            10350,
            "ETHUSDC",
            "TREND",
            Decimal("19.99941606"),
            Decimal("-0.21074883"),
            Decimal("-1.053774917066253583405874701"),
        ),
        (
            10348,
            "BTCUSDC",
            "BBRANGE",
            Decimal("19.999871628"),
            Decimal("0.25981595"),
            Decimal("1.299088088326803734422449764"),
        ),
        (
            10364,
            "BTCUSDC",
            "RSI",
            Decimal("20.036970486"),
            Decimal("-0.12329035"),
            Decimal("-0.6153143265152983367028551903"),
        ),
    ],
)
def test_paper_closed_positions_use_decimal_simulated_entry_notional(
    position_id, symbol, strategy, entry_notional, net_pnl, expected
):
    denominator = resolve_entry_notional_usdc(
        real_execution_notional=None,
        simulated_execution_notional=entry_notional,
        estimated_notional=None,
        legacy_price_qty_notional=Decimal("0"),
    )
    percentage = calculate_net_pnl_pct(net_pnl, denominator)

    assert position_id > 0
    assert symbol
    assert strategy
    assert denominator == entry_notional
    assert percentage == expected


def test_paper_loss_and_win_signs_follow_net_pnl_numerator():
    denominator = Decimal("20")

    assert calculate_net_pnl_pct(Decimal("-0.21"), denominator) < 0
    assert calculate_net_pnl_pct(Decimal("0.26"), denominator) > 0


def test_real_execution_notional_wins_over_paper_simulated_evidence():
    assert resolve_entry_notional_usdc(
        real_execution_notional=Decimal("20.03"),
        simulated_execution_notional=Decimal("19.99"),
        estimated_notional=Decimal("19.98"),
        legacy_price_qty_notional=Decimal("19.97"),
    ) == Decimal("20.03")


def test_legacy_nonzero_position_notional_remains_available():
    assert resolve_entry_notional_usdc(
        legacy_price_qty_notional=Decimal("18.25"),
    ) == Decimal("18.25")


def test_missing_or_zero_evidence_does_not_invent_zero_percent():
    denominator = resolve_entry_notional_usdc(
        real_execution_notional=None,
        simulated_execution_notional=None,
        estimated_notional=None,
        legacy_price_qty_notional=Decimal("0"),
    )

    assert denominator is None
    assert calculate_net_pnl_pct(Decimal("1.25"), denominator) is None


def test_fee_aware_contract_uses_gross_entry_and_net_pnl_once():
    gross_entry_notional = Decimal("20")
    net_pnl_after_costs = Decimal("-0.20")

    assert calculate_net_pnl_pct(
        net_pnl_after_costs, gross_entry_notional
    ) == Decimal("-1.00")

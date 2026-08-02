from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext

from common.financial_truth_calculator import (
    ARITHMETIC_CONTRACT_VERSION,
    PRECISION_CONTRACT_VERSION,
    FillEvidence,
    calculate_financial_truth,
)
from common.inventory_quantity import ExitInventoryClassification, ExitInventoryStatus


CASES = {
    3079: {
        "symbol": "BTCUSDC", "entry_qty": "0.00031545",
        "entry_base_fee": "0.000001104075", "exit_qty": "0.00031435",
        "entry_notional": "19.999561545", "exit_notional": "19.972573035",
        "entry_fee": "0.0699984654075", "exit_fee": "0.0699040056225",
        "remaining": "0", "classification": "FULLY_EXECUTED_CLOSE",
        "gross_pnl": "0.04275159999999999990172984500",
        "fees": "0.1399024710300",
        "net_pnl": "-0.09715087103000000009827015500",
    },
    3080: {
        "symbol": "BNBUSDC", "entry_qty": "0.035152",
        "entry_base_fee": "0.000123032", "exit_qty": "0.035029",
        "entry_notional": "19.8995472", "exit_notional": "19.8649459",
        "entry_fee": "0.0696484152", "exit_fee": "0.06952731065",
        "remaining": "0", "classification": "FULLY_EXECUTED_CLOSE",
        "gross_pnl": "0.035028999999999999937140256",
        "fees": "0.13917572585",
        "net_pnl": "-0.104146725850000000062859744",
    },
    3081: {
        "symbol": "ETHUSDC", "entry_qty": "0.010623",
        "entry_base_fee": "0.0000371805", "exit_qty": "0.010585",
        "entry_notional": "19.97963217", "exit_notional": "19.87153805",
        "entry_fee": "0.069928712595", "exit_fee": "0.069550383175",
        "remaining": "0.0000008195", "classification": "TERMINAL_DUST_CLOSE",
        "gross_pnl": "-0.0366241000000000000007147002",
        "fees": "0.139479095770",
        "net_pnl": "-0.17609778224675112895170722050280",
    },
    3082: {
        "symbol": "ETHUSDC", "entry_qty": "0.010584",
        "entry_base_fee": "0.000037044", "exit_qty": "0.010547",
        "entry_notional": "19.99931472", "exit_notional": "19.8083207",
        "entry_fee": "0.06999760152", "exit_fee": "0.06932912245",
        "remaining": "0", "classification": "FULLY_EXECUTED_CLOSE",
        "gross_pnl": "-0.1210795600000000000270587856",
        "fees": "0.13932672397",
        "net_pnl": "-0.2604062839700000000270587856",
    },
    3083: {
        "symbol": "SOLUSDC", "entry_qty": "0.25921",
        "entry_base_fee": "0.000907235", "exit_qty": "0.25831",
        "entry_notional": "19.9695384", "exit_notional": "19.8175432",
        "entry_fee": "0.06989338440", "exit_fee": "0.0693614012",
        "remaining": "0", "classification": "FULLY_EXECUTED_CLOSE",
        "gross_pnl": "-0.082659199999999999982943344",
        "fees": "0.13925478560",
        "net_pnl": "-0.221913985599999999982943344",
    },
    3084: {
        "symbol": "BTCUSDC", "entry_qty": "0.00030191",
        "entry_base_fee": "0.000001056685", "exit_qty": "0.00030085",
        "entry_notional": "19.899612684", "exit_notional": "19.901197415",
        "entry_fee": "0.0696486443940", "exit_fee": "0.0696541909525",
        "remaining": "0.000000003315", "classification": "TERMINAL_DUST_CLOSE",
        "gross_pnl": "0.07145187500000000007828415748",
        "fees": "0.1393028353465",
        "net_pnl": "-0.067850192911857752054289969687740",
    },
    3085: {
        "symbol": "SOLUSDC", "entry_qty": "0.26924",
        "entry_base_fee": "0.000942340", "exit_qty": "0.26829",
        "entry_notional": "19.9899605", "exit_notional": "19.7756559",
        "entry_fee": "0.06996486175", "exit_fee": "0.06921479565",
        "remaining": "0.000007660", "classification": "TERMINAL_DUST_CLOSE",
        "gross_pnl": "-0.143771014815777744742693325",
        "fees": "0.13917965740",
        "net_pnl": "-0.2829486746924262365709399902250",
    },
}


MULTI_ENTRY_3085 = (
    ("0.00383", "0.2843775", "0.000013405", "0.00099532125"),
    ("0.0977", "7.254225", "0.00034195", "0.0253897875"),
    ("0.01015", "0.7536375", "0.000035525", "0.00263773125"),
    ("0.02712", "2.0136600", "0.00009492", "0.0070478100"),
    ("0.01949", "1.4471325", "0.000068215", "0.00506496375"),
    ("0.11095", "8.2369280", "0.000388325", "0.02882924800"),
)


def _fill(position_id, case, purpose, index, qty, notional, fee_qty, fee_usdc):
    symbol = case["symbol"]
    base = symbol.removesuffix("USDC")
    return FillEvidence(
        fill_id=f"fixture:{position_id}:{purpose}:{index}",
        order_id=f"fixture:{position_id}:{purpose}", position_id=position_id,
        purpose=purpose, side="BUY" if purpose == "ENTRY" else "SELL",
        symbol=symbol, quantity=Decimal(qty), price=Decimal("1"),
        notional=Decimal(notional), fee_quantity=Decimal(fee_qty),
        fee_asset=base if purpose == "ENTRY" else "USDC",
        authoritative_fee_usdc=Decimal(fee_usdc), estimated_fee_usdc=None,
        event_time=datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(seconds=index),
        source_authority="EXCHANGE_EXECUTION", source_exchange="okx",
        source_environment="live", source_deployment_id="local-live",
        account_identity_fingerprint="fixture-account",
        instrument_metadata_fingerprint=f"fixture-{symbol}",
        step_size=Decimal("0.00000001"), base_asset=base, quote_asset="USDC",
        source_version="ARITHMETIC_FIXTURE_V1",
    )


def calculation_for(position_id: int, *, reverse: bool = False):
    case = CASES[position_id]
    if position_id == 3085:
        entries = tuple(
            _fill(position_id, case, "ENTRY", index, *row)
            for index, row in enumerate(MULTI_ENTRY_3085, start=1)
        )
    else:
        entries = (_fill(
            position_id, case, "ENTRY", 1, case["entry_qty"],
            case["entry_notional"], case["entry_base_fee"], case["entry_fee"],
        ),)
    exit_fill = _fill(
        position_id, case, "EXIT", 100, case["exit_qty"],
        case["exit_notional"], case["exit_fee"], case["exit_fee"],
    )
    fills = entries + (exit_fill,)
    if reverse:
        fills = tuple(reversed(fills))
    classification = ExitInventoryClassification(
        ExitInventoryStatus(case["classification"]), Decimal(case["remaining"]),
        Decimal("0"), Decimal(case["remaining"]),
        "BELOW_LOT_SIZE" if Decimal(case["remaining"]) > 0 else None,
    )
    return calculate_financial_truth(
        position_id=position_id, position_status="CLOSED", fills=fills,
        position_symbol=case["symbol"], inventory_classification=classification,
    )


def test_exact_local_live_arithmetic_vectors_all_seven():
    for position_id, case in CASES.items():
        result = calculation_for(position_id)
        assert str(result.authoritative_gross_pnl) == case["gross_pnl"]
        assert str(result.authoritative_fees_usdc) == case["fees"]
        assert str(result.authoritative_net_pnl) == case["net_pnl"]
        assert format(result.remaining_inventory_qty, "f") == case["remaining"]
        assert result.arithmetic_contract_version == ARITHMETIC_CONTRACT_VERSION
        assert result.precision_contract_version == PRECISION_CONTRACT_VERSION


def test_canonical_result_is_independent_of_ambient_decimal_context():
    original = getcontext().copy()
    try:
        for precision in (6, 12, 28, 50, 100):
            getcontext().prec = precision
            for position_id, case in CASES.items():
                result = calculation_for(position_id)
                assert str(result.authoritative_gross_pnl) == case["gross_pnl"]
                assert str(result.authoritative_net_pnl) == case["net_pnl"]
    finally:
        getcontext().prec = original.prec
        getcontext().rounding = original.rounding


def test_multi_fill_and_reordered_query_results_are_identical():
    ordered = calculation_for(3085)
    reversed_result = calculation_for(3085, reverse=True)
    assert ordered.authoritative_gross_pnl == reversed_result.authoritative_gross_pnl
    assert ordered.authoritative_net_pnl == reversed_result.authoritative_net_pnl
    assert ordered.source_fingerprint == reversed_result.source_fingerprint

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.exchange_client import OkxMarketDataAdapter
from common.live_exit_cost import (
    CANONICAL_SIGN_SEMANTICS,
    CONTRACT_VERSION,
    FRESHNESS,
    RAW_SIGN_SEMANTICS,
    parse_okx_trade_fee_response,
)
from common.position_risk_boundary import evaluate_position_risk
from common.portfolio_state import build_portfolio_state
from tests.test_position_risk_boundary_v1 import evaluate, projection
from tests.test_position_risk_boundary_v1 import mark
from common.portfolio_state import RealizedEvidence


NOW = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
IDENTITY = "a" * 64


def payload(taker="-0.000800000000000001", *, inst_type="SPOT"):
    return {
        "code": "0",
        "data": [{
            "instType": inst_type, "taker": taker, "maker": "-0.0005",
            "level": "Lv1", "ruleType": "normal", "ts": "1787313600000",
        }],
    }


def parse(**changes):
    values = dict(
        payload=payload(), deployment_id="local-live",
        account_identity_fingerprint=IDENTITY, symbol="BTCUSDC",
        observed_at=NOW,
    )
    values.update(changes)
    return parse_okx_trade_fee_response(**values)


def test_okx_negative_rate_normalizes_to_exact_positive_decimal():
    snapshot = parse()
    assert snapshot.contract_version == CONTRACT_VERSION
    assert snapshot.raw_fee_rate == Decimal("-0.000800000000000001")
    assert snapshot.canonical_fee_rate == Decimal("0.000800000000000001")
    assert snapshot.raw_sign_semantics == RAW_SIGN_SEMANTICS
    assert CANONICAL_SIGN_SEMANTICS == "POSITIVE_DECIMAL_COST_RATE"
    assert snapshot.expires_at == NOW + FRESHNESS
    assert snapshot.snapshot_fingerprint == parse().snapshot_fingerprint
    assert snapshot.exit_cost_snapshot_id == parse().exit_cost_snapshot_id


def test_float_and_instrument_mismatch_fail_closed():
    with pytest.raises(ValueError, match="INVALID_DECIMAL"):
        parse(payload=payload(taker=-0.0008))
    with pytest.raises(ValueError, match="INSTRUMENT_MISMATCH"):
        parse(payload=payload(inst_type="SWAP"))
    with pytest.raises(ValueError, match="DEPLOYMENT_MISMATCH"):
        parse(deployment_id="local-paper")
    with pytest.raises(ValueError, match="ACCOUNT_IDENTITY_INVALID"):
        parse(account_identity_fingerprint="wrong")


def test_existing_okx_adapter_reuses_private_get_path(monkeypatch):
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)
    calls = []
    monkeypatch.setattr(
        client, "_private_request",
        lambda method, path, **kwargs: calls.append((method, path, kwargs)) or payload(),
    )
    assert client.get_trade_fee(symbol="BTCUSDC") == payload()
    assert calls == [("GET", "/api/v5/account/trade-fee", {
        "params": {"instType": "SPOT", "instId": "BTC-USDC"},
    })]


@pytest.mark.parametrize("quantity", [None, Decimal("-0.0001")])
def test_null_or_negative_inventory_is_data_quality_failure(quantity):
    result = evaluate(remaining_inventory_qty=quantity)
    assert result.status == "INVENTORY_DATA_QUALITY_ERROR"
    assert result.open_risk_to_trigger is None


def test_zero_inventory_is_non_contributing_without_other_authorities():
    result = evaluate_position_risk(
        position_id=1, side="LONG", remaining_inventory_qty=Decimal("0"),
        mark_price=None, mark_status="PRICE_UNAVAILABLE", projection=None,
    )
    assert result.status == "CANONICAL"
    assert result.open_risk_to_trigger == Decimal("0")


def test_missing_and_stale_fee_authority_are_typed_and_never_zero():
    missing = evaluate(projection=projection(
        environment="LIVE", deployment_id="local-live",
        exit_fee_rate=None, exit_fee_model=None,
        exit_fee_status="MISSING_EXIT_COST_AUTHORITY",
    ))
    stale = evaluate(projection=projection(
        environment="LIVE", deployment_id="local-live",
        exit_fee_rate=None, exit_fee_model=CONTRACT_VERSION,
        exit_fee_status="STALE_FEE_EVIDENCE",
    ))
    assert missing.status == "MISSING_EXIT_COST_AUTHORITY"
    assert stale.status == "STALE_FEE_EVIDENCE"
    assert missing.open_risk_to_trigger is None
    assert stale.open_risk_to_trigger is None


def test_boundary_mark_qty_and_fee_produce_canonical_open_risk():
    result = evaluate(projection=projection(
        environment="LIVE", deployment_id="local-live",
        exit_fee_rate=Decimal("0.0008"),
        exit_fee_model=CONTRACT_VERSION, exit_fee_status="CANONICAL",
    ))
    assert result.core_price_risk == Decimal("3.6")
    assert result.exit_fee_estimate == Decimal("0.15872")
    assert result.open_risk_to_trigger == Decimal("3.75872")
    assert result.status == "CANONICAL"


def test_live_empty_and_mixed_portfolio_aggregation():
    common = dict(
        environment="LIVE", deployment_id="local-live", as_of=NOW,
        baseline=None, realized=RealizedEvidence(0, 0, None, None),
        historical_peak_managed_equity=None,
    )
    empty = build_portfolio_state(open_marks=(), risk_boundaries={}, **common)
    assert empty.open_risk == Decimal("0")
    assert empty.open_risk_status == "CANONICAL_EMPTY"

    canonical = projection(
        environment="LIVE", deployment_id="local-live",
        exit_fee_rate=Decimal("0.0008"),
        exit_fee_model=CONTRACT_VERSION, exit_fee_status="CANONICAL",
    )
    incomplete = projection(
        position_id=2, environment="LIVE", deployment_id="local-live",
        exit_fee_rate=None, exit_fee_model=None,
        exit_fee_status="MISSING_EXIT_COST_AUTHORITY",
    )
    mixed = build_portfolio_state(
        open_marks=(mark(1), mark(2)),
        risk_boundaries={1: canonical, 2: incomplete}, **common,
    )
    assert mixed.open_risk is None
    assert mixed.open_risk_status == "INCOMPLETE"
    assert mixed.partial_risk_sum == Decimal("3.75872")

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from common.exchange_client import OkxMarketDataAdapter
from common.owner_capital_flow_sync import (
    OVERLAP_RESCAN,
    SOURCE_SAFETY_LAG,
    OwnerFlowSyncError,
    classify_trading_account_bill,
    fetch_exhaustive_trading_account_bills,
    run_owner_capital_flow_sync_if_due,
)


NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def bill(
    bill_id="100", *, subtype="11", source_from="6", source_to="18",
    change="10.000000000000000001", asset="USDC", at=NOW,
):
    return {
        "billId": bill_id,
        "type": "1",
        "subType": subtype,
        "from": source_from,
        "to": source_to,
        "ccy": asset,
        "balChg": change,
        "ts": str(int(at.timestamp() * 1000)),
    }


def test_exact_transfer_in_and_out_mapping_is_decimal_and_exchange_identified():
    inbound = classify_trading_account_bill(bill())
    outbound = classify_trading_account_bill(
        bill(
            "101", subtype="12", source_from="18", source_to="6",
            change="-3.000000000000000009",
        )
    )
    assert inbound.event_type == "TRANSFER_IN"
    assert inbound.amount == Decimal("10.000000000000000001")
    assert inbound.source_event_identity == "OKX:TRADING_BILL:100"
    assert outbound.event_type == "TRANSFER_OUT"
    assert outbound.amount == Decimal("3.000000000000000009")
    assert outbound.source_event_identity == "OKX:TRADING_BILL:101"


def test_deposit_withdrawal_and_unapproved_transfer_subtypes_are_context_only():
    assert classify_trading_account_bill({**bill(), "type": "2"}) is None
    assert classify_trading_account_bill({**bill(), "subType": "285"}) is None


@pytest.mark.parametrize(
    "row,error",
    [
        (bill(source_from="18"), "TRANSFER_IN_SOURCE_CONTRACT_MISMATCH"),
        (bill(change="-1"), "TRANSFER_IN_SOURCE_CONTRACT_MISMATCH"),
        (
            bill("2", subtype="12", source_from="18", source_to="6", change="1"),
            "TRANSFER_OUT_SOURCE_CONTRACT_MISMATCH",
        ),
    ],
)
def test_boundary_direction_mismatch_fails_closed(row, error):
    with pytest.raises(OwnerFlowSyncError, match=error):
        classify_trading_account_bill(row)


def test_non_usdc_and_binary_float_fail_closed():
    with pytest.raises(OwnerFlowSyncError) as unsupported:
        classify_trading_account_bill(bill(asset="BTC"))
    assert unsupported.value.status == "UNSUPPORTED_ASSET"
    with pytest.raises(OwnerFlowSyncError, match="SOURCE_DECIMAL_INVALID"):
        classify_trading_account_bill(bill(change=0.1))


class PagesClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get_account_bills_page(self, **kwargs):
        self.calls.append(kwargs)
        return {"code": "0", "data": self.pages.pop(0)}


def test_pagination_exhausts_fixed_range_and_resumes_by_bill_id():
    first = [
        bill(
            str(1000 - index),
            at=NOW - timedelta(seconds=index),
        )
        for index in range(100)
    ]
    second = [bill("900", at=NOW - timedelta(seconds=100))]
    client = PagesClient([first, second])
    rows, pages, cursor = fetch_exhaustive_trading_account_bills(
        client,
        range_from=NOW - timedelta(days=1),
        source_cutoff=NOW,
        archive=False,
    )
    assert pages == 2
    assert len(rows) == 101
    assert cursor == "901"
    assert client.calls[0]["after"] is None
    assert client.calls[1]["after"] == "901"
    assert client.calls[0]["begin_ms"] == int((NOW - timedelta(days=1)).timestamp() * 1000)
    assert client.calls[0]["end_ms"] == int(NOW.timestamp() * 1000)


def test_duplicate_bill_payload_conflict_and_nonadvancing_cursor_fail_closed():
    first = [
        bill(str(1000 - index), at=NOW - timedelta(seconds=index))
        for index in range(100)
    ]
    conflict = [{**first[-1], "balChg": "999"}]
    with pytest.raises(OwnerFlowSyncError, match="PAYLOAD_CONFLICT"):
        fetch_exhaustive_trading_account_bills(
            PagesClient([first, conflict]),
            range_from=NOW - timedelta(days=1), source_cutoff=NOW, archive=False,
        )

    repeated = [
        bill(str(1000 - index), at=NOW - timedelta(seconds=index))
        for index in range(100)
    ]
    with pytest.raises(OwnerFlowSyncError, match="CURSOR_NOT_ADVANCING"):
        fetch_exhaustive_trading_account_bills(
            PagesClient([repeated, repeated]),
            range_from=NOW - timedelta(days=1), source_cutoff=NOW, archive=False,
        )


def test_exchange_client_requests_only_transfer_bills_with_bounded_page(monkeypatch):
    adapter = OkxMarketDataAdapter()
    captured = {}

    def request(method, path, *, params=None, body=None):
        captured.update(method=method, path=path, params=params, body=body)
        return {"code": "0", "data": []}

    monkeypatch.setattr(adapter, "_private_request", request)
    adapter.get_account_bills_page(
        archive=True, after="77", begin_ms=10, end_ms=20, limit=100,
    )
    assert captured == {
        "method": "GET",
        "path": "/api/v5/account/bills-archive",
        "params": {
            "type": "1", "limit": "100", "after": "77",
            "begin": "10", "end": "20",
        },
        "body": None,
    }
    with pytest.raises(ValueError, match="LIMIT_INVALID"):
        adapter.get_account_bills_page(limit=101)


def test_safety_contract_and_disabled_automation_hook_are_explicit(monkeypatch):
    assert SOURCE_SAFETY_LAG == timedelta(minutes=5)
    assert OVERLAP_RESCAN == timedelta(hours=24)
    monkeypatch.delenv("OWNER_CAPITAL_FLOW_SYNC_V1_ENABLED", raising=False)
    result = run_owner_capital_flow_sync_if_due(
        None,
        exchange_client=SimpleNamespace(),
        trading_mode="LIVE",
        deployment_id="local-live",
    )
    assert result == {"status": "DISABLED"}

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from common.entry_fill_reconciliation import (
    EntryFillReconciliationStats,
    PendingEntryReconciliationRun,
)
from common.exchange_client import OkxMarketDataAdapter
from common.exchange_identity import normalize_exchange_source
from common.exchange_ingest_trades import _trade_to_row


def test_exchange_source_normalization_is_canonical():
    assert normalize_exchange_source(" OKX ") == "okx"
    assert normalize_exchange_source("Binance") == "binance"
    assert normalize_exchange_source(None) == ""


def test_okx_fill_preserves_exact_fee_trade_id_and_client_order_id(monkeypatch):
    client = OkxMarketDataAdapter.__new__(OkxMarketDataAdapter)
    client_order_id = "ORC-L-BNBUSDC-TREND-1m-ENTRY-000001"
    monkeypatch.setattr(
        client,
        "_private_request",
        lambda *_args, **_kwargs: {
            "data": [
                {
                    "tradeId": "18446744073709551617",
                    "ordId": "order-701",
                    "clOrdId": client_order_id,
                    "fillSz": "0.033895",
                    "fillPx": "590.125",
                    "fee": "-0.0000000100",
                    "feeCcy": "BNB",
                    "ts": "1785499200000",
                    "side": "buy",
                    "execType": "T",
                }
            ]
        },
    )

    trade = client.get_my_trades(symbol="BNBUSDC")[0]
    row = _trade_to_row("BNBUSDC", trade, source="okx")

    assert trade["commission"] == "0.00000001"
    assert trade["clientOrderId"] == client_order_id
    assert row["trade_id"] == "18446744073709551617"
    assert row["client_order_id"] == client_order_id
    assert row["commission_amount"] == "0.00000001"


@pytest.mark.parametrize(
    ("field", "invalid_value", "error_field"),
    [
        ("id", None, "trade id"),
        ("id", "", "trade id"),
        ("orderId", None, "order id"),
        ("orderId", "", "order id"),
    ],
)
def test_trade_to_row_rejects_missing_authoritative_identity(
    field,
    invalid_value,
    error_field,
):
    trade = {
        "id": "trade-1",
        "orderId": "order-1",
        "price": "10",
        "qty": "1",
        "quoteQty": "10",
        "commission": "0.01",
        "commissionAsset": "USDC",
        "time": 1785499200000,
        "isBuyer": True,
        "isMaker": False,
    }
    trade[field] = invalid_value

    with pytest.raises(ValueError, match=error_field):
        _trade_to_row("BTCUSDC", trade, source="okx")


def test_fill_ingest_recovery_has_no_lei1c_schema_access_without_new_fills(
    monkeypatch,
):
    import common.exchange_ingest_trades as ingest

    calls = []

    class Cursor:
        def __init__(self):
            self.one = None
            self.rowcount = 0

        def execute(self, sql, _params=None):
            normalized = " ".join(str(sql).split())
            self.one = (123,) if "FROM binance_ingest_state" in normalized else None

        def fetchone(self):
            return self.one

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        autocommit = False

        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append("commit")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Client:
        def get_my_trades(self, **_kwargs):
            return []

    monkeypatch.setenv("TRADING_MODE", "LIVE")
    monkeypatch.setenv("EXCHANGE", " OKX ")
    monkeypatch.setattr(ingest.psycopg2, "connect", lambda _dsn: Connection())

    def due(_conn, *, batch_size, trading_mode):
        calls.append(("due", batch_size, trading_mode))
        return PendingEntryReconciliationRun(
            True,
            "COMPLETE",
            EntryFillReconciliationStats(created=1, recovered=1),
        )

    monkeypatch.setattr(ingest, "run_pending_entry_reconciliation_if_due", due)
    result = ingest.ingest_my_trades(
        client=Client(),
        symbols=["BTCUSDC"],
        db_host="local",
        db_port=5432,
        db_name="test",
        db_user="test",
        db_pass="test",
    )
    assert result == (0, 0)
    assert result.status == "OK"
    assert result.ran is True
    assert result.applicable is True
    assert calls == [("due", 100, "LIVE"), "commit"]


@pytest.mark.parametrize("lei1c_mode", ["SHADOW", "ENFORCE"])
@pytest.mark.parametrize(
    "failure_phase",
    ["setup", "row", "repository"],
)
def test_lei1c_failure_mode_preserves_shadow_legacy_ingest_and_enforces_gate(
    monkeypatch,
    lei1c_mode,
    failure_phase,
):
    import common.exchange_ingest_trades as ingest

    legacy_rows = []

    class Cursor:
        def __init__(self):
            self.one = None
            self.many = []
            self.rowcount = 0

        def execute(self, sql, _params=None):
            normalized = " ".join(str(sql).split())
            self.one = None
            self.many = []
            if "FROM binance_ingest_state" in normalized:
                self.one = (0,)
            elif "FROM runtime_contract_adoption_v2" in normalized:
                self.many = [
                    (
                        17,
                        3,
                        datetime(
                            2026, 7, 31, 11, 59, tzinfo=timezone.utc
                        ),
                        "b" * 40,
                    )
                ]

        def fetchone(self):
            return self.one

        def fetchall(self):
            return list(self.many)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        autocommit = False

        def cursor(self):
            return Cursor()

        def commit(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Client:
        def get_my_trades(self, **_kwargs):
            trade = {
                "id": "trade-shadow-1",
                "orderId": "order-shadow-1",
                "clientOrderId": "cid-shadow-1",
                "price": "10",
                "qty": "1",
                "quoteQty": "10",
                "commission": "0.01",
                "commissionAsset": "USDC",
                "time": 1785499200000,
                "isBuyer": True,
                "isMaker": False,
            }
            if failure_phase == "row":
                # Legacy JSON ingestion accepts this payload, while LEI1C's
                # immutable evidence contract correctly rejects binary floats.
                trade["syntheticBinaryFloat"] = 0.1
            return [trade]

    monkeypatch.setenv("TRADING_MODE", "LIVE")
    monkeypatch.setenv("EXCHANGE", "BINANCE")
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("LIVE_ENTRY_FILL_ATTRIBUTION_MODE", lei1c_mode)
    monkeypatch.setattr(ingest.psycopg2, "connect", lambda _dsn: Connection())
    monkeypatch.setattr(
        ingest, "_persist_instrument_snapshot", lambda *_args: None
    )
    monkeypatch.setattr(
        ingest,
        "run_pending_entry_reconciliation_if_due",
        lambda *_args, **_kwargs: PendingEntryReconciliationRun(
            False, "NOT_DUE", EntryFillReconciliationStats()
        ),
    )

    def register_legacy(_cur, row, *, account_identity_key):
        legacy_rows.append((row["trade_id"], account_identity_key))
        return type(
            "ObservedOnlyChange",
            (),
            {
                "ingestion_id": 1,
                "permits_mutation": False,
                "application_status": None,
                "decision": ingest.FillMutationDecision.OBSERVED_NOT_APPLIED,
            },
        )()

    monkeypatch.setattr(ingest, "register_fill_change", register_legacy)
    if failure_phase == "setup":
        def fail_setup():
            raise RuntimeError("synthetic LEI1C setup failure")

        monkeypatch.setattr(
            ingest,
            "require_runtime_git_revision",
            fail_setup,
        )
    else:
        monkeypatch.setattr(
            ingest, "require_runtime_git_revision", lambda: "a" * 40
        )
    if failure_phase == "repository":
        def fail_repository(*_args, **_kwargs):
            raise RuntimeError("synthetic LEI1C repository failure")

        monkeypatch.setattr(
            ingest,
            "EntryFillAttributionRepository",
            fail_repository,
        )

    def invoke():
        return ingest.ingest_my_trades(
            client=Client(),
            symbols=["BTCUSDC"],
            db_host="local",
            db_port=5432,
            db_name="test",
            db_user="test",
            db_pass="test",
        )
    if lei1c_mode == "ENFORCE":
        with pytest.raises(Exception):
            invoke()
        assert legacy_rows == []
    else:
        result = invoke()
        assert result == (1, 0)
        assert result.status == "OK"
        assert legacy_rows == [
            ("trade-shadow-1", "binance:ACCOUNT_IDENTITY_MISSING")
        ]


def test_lei1c_forward_producer_uses_migration_activation_boundary(
    monkeypatch,
):
    import common.exchange_ingest_trades as ingest

    activated_at = datetime(
        2026, 7, 31, 12, 0, 0, 500, tzinfo=timezone.utc
    )
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    boundary_floor_ms = int(
        (activated_at.replace(microsecond=0) - epoch).total_seconds() * 1000
    )

    class Cursor:
        def execute(self, _sql, _params=None):
            return None

        def fetchall(self):
            return [(17, 3, activated_at, "b" * 40)]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Repository:
        def __init__(self, _factory):
            pass

        def load_evidence(self, _natural_key):
            return None

        def resolve_observation_context(self, preliminary):
            return preliminary

    processed = []

    def process(*, mode, observation, repository):
        processed.append((mode, observation, repository))
        return SimpleNamespace(
            outcome=ingest.EntryFillProcessingOutcome.EVIDENCE_RECORDED,
            attribution_status=None,
            application_status=None,
            error_code=None,
        )

    def row(trade_id, event_time_ms):
        return {
            "source": "okx",
            "trade_id": trade_id,
            "order_id": f"order-{trade_id}",
            "client_order_id": None,
            "symbol": "BNBUSDC",
            "side": "BUY",
            "executed_qty": "0.01",
            "avg_price": "300",
            "quote_notional_usdc": "3",
            "commission_amount": "0.001",
            "commission_asset": "USDC",
            "event_time_ms": event_time_ms,
            "raw": "{}",
        }

    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("DEPLOYMENT_ID", "local-live")
    monkeypatch.setenv("LIVE_ENTRY_FILL_ATTRIBUTION_MODE", "SHADOW")
    monkeypatch.setattr(ingest, "require_runtime_git_revision", lambda: "a" * 40)
    monkeypatch.setattr(ingest.psycopg2, "connect", lambda _dsn: Connection())
    monkeypatch.setattr(ingest, "EntryFillAttributionRepository", Repository)
    monkeypatch.setattr(ingest, "process_entry_fill_attribution", process)

    ingest._record_lei1c_observations(
        None,
        dsn="synthetic",
        rows=[
            row("pre-activation", boundary_floor_ms),
            row("post-activation", boundary_floor_ms + 1),
        ],
        forward_boundary_ms=0,
    )

    assert [item[1].exchange_trade_id for item in processed] == [
        "post-activation"
    ]


def test_paper_fill_ingest_is_no_db_no_exchange_no_op(monkeypatch):
    import common.exchange_ingest_trades as ingest

    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setattr(
        ingest.psycopg2,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("PAPER ingest opened DB"),
    )
    monkeypatch.setattr(
        ingest,
        "run_pending_entry_reconciliation_if_due",
        lambda *_args, **_kwargs: pytest.fail("PAPER ingest ran reconciliation"),
    )

    class TrapExchangeClient:
        def __getattr__(self, name):
            raise AssertionError(f"PAPER ingest accessed exchange method {name}")

    result = ingest.ingest_my_trades(
        client=TrapExchangeClient(),
        symbols=["BTCUSDC"],
        db_host="local",
        db_port=5432,
        db_name="test",
        db_user="test",
        db_pass="test",
    )

    assert result == (0, 0)
    assert result.status == "NOT_APPLICABLE"
    assert result.ran is False
    assert result.applicable is False

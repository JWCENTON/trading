from __future__ import annotations

import pytest

from common.entry_fill_reconciliation import (
    EntryFillReconciliationStats,
    PendingEntryReconciliationRun,
)
from common.exchange_identity import normalize_exchange_source


def test_exchange_source_normalization_is_canonical():
    assert normalize_exchange_source(" OKX ") == "okx"
    assert normalize_exchange_source("Binance") == "binance"
    assert normalize_exchange_source(None) == ""


def test_fill_ingest_runs_due_reconciliation_without_new_fills(monkeypatch):
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
            False, "NOT_DUE", EntryFillReconciliationStats()
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

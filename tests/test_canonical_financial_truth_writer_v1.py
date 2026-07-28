from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError

import pytest

from common.financial_truth_calculator import (
    FillEvidence,
    calculate_financial_truth,
    classify_fee_asset_role,
    source_fingerprint,
)
from common.financial_truth_repository import normalize_optional_asset
from common.financial_truth_identity import (
    AccountIdentityCache,
    okx_account_identity,
)
from common.financial_truth_writer import WriterActivation
from common.exchange_client import OkxMarketDataAdapter


NOW = datetime(2026, 7, 28, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db/migrations/20260728_canonical_financial_truth_writer_v1.sql"


def fill(
    fill_id: str,
    purpose: str,
    qty: str,
    price: str,
    *,
    fee_qty: str | None = "0.10",
    fee_asset: str | None = "USDC",
    authoritative_fee: str | None = "0.10",
    estimated_fee: str | None = None,
    account: str | None = "account-fingerprint",
    metadata: str | None = "metadata-fingerprint",
    step: str | None = "0.001",
    position_id: int = 1,
    deployment: str = "paper-deployment-uuid",
) -> FillEvidence:
    quantity = Decimal(qty)
    value = Decimal(price)
    return FillEvidence(
        fill_id=fill_id, order_id=f"order-{purpose}-{fill_id}",
        position_id=position_id, purpose=purpose,
        side="BUY" if purpose == "ENTRY" else "SELL", symbol="BTCUSDC",
        quantity=quantity, price=value, notional=quantity * value,
        fee_quantity=None if fee_qty is None else Decimal(fee_qty),
        fee_asset=fee_asset,
        authoritative_fee_usdc=(
            None if authoritative_fee is None else Decimal(authoritative_fee)
        ),
        estimated_fee_usdc=(
            None if estimated_fee is None else Decimal(estimated_fee)
        ),
        event_time=NOW + timedelta(seconds=int(fill_id)),
        source_authority="SIMULATED_EXECUTION", source_exchange="SIMULATOR",
        source_environment="paper", source_deployment_id=deployment,
        account_identity_fingerprint=account,
        instrument_metadata_fingerprint=metadata,
        step_size=None if step is None else Decimal(step),
        base_asset="BTC", quote_asset="USDC",
        source_version="PAPER_SIMULATOR_V1",
    )


def calculate(*fills):
    return calculate_financial_truth(
        position_id=1, position_status="CLOSED", fills=fills
    )


def test_unknown_remains_unknown_without_evidence():
    result = calculate()
    assert result.financial_truth_status == "UNKNOWN"
    assert result.authoritative_net_pnl is None


def test_partial_entry_is_incomplete():
    result = calculate(fill("1", "ENTRY", "10", "2"))
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "MISSING_EXIT_FILLS"


def test_partial_exit_and_remaining_are_incomplete():
    result = calculate(
        fill("1", "ENTRY", "10", "2"),
        fill("2", "EXIT", "4", "3"),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.remaining_inventory_qty == Decimal("6")


def test_complete_execution_evidence_is_complete():
    result = calculate(
        fill("1", "ENTRY", "10", "2"),
        fill("2", "EXIT", "10", "3"),
    )
    assert result.financial_truth_status == "COMPLETE"
    assert result.authoritative_gross_pnl == Decimal("10")
    assert result.authoritative_net_pnl == Decimal("9.80")


def test_open_position_with_complete_execution_is_lifecycle_conflict():
    result = calculate_financial_truth(
        position_id=1,
        position_status="OPEN",
        fills=(
            fill("1", "ENTRY", "10", "2"),
            fill("2", "EXIT", "10", "3"),
        ),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "POSITION_LIFECYCLE_NOT_CLOSED"
    assert "POSITION_LIFECYCLE_NOT_CLOSED" in result.failure_detail


def test_open_entry_only_remains_missing_exit_incomplete():
    result = calculate_financial_truth(
        position_id=1, position_status="OPEN",
        fills=(fill("1", "ENTRY", "10", "2"),),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "MISSING_EXIT_FILLS"


def test_closed_position_missing_exit_remains_incomplete():
    result = calculate(fill("1", "ENTRY", "10", "2"))
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "MISSING_EXIT_FILLS"


def test_open_position_with_two_exit_fills_never_complete():
    result = calculate_financial_truth(
        position_id=1,
        position_status="OPEN",
        fills=(
            fill("1", "ENTRY", "10", "2"),
            fill("2", "EXIT", "6", "3"),
            fill("3", "EXIT", "6", "3"),
        ),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "POSITION_LIFECYCLE_NOT_CLOSED"
    assert "EXIT_QUANTITY_EXCEEDS_ENTRY" in result.failure_detail


def test_multiple_fills_and_proportional_entry_cost():
    result = calculate(
        fill("1", "ENTRY", "4", "2", authoritative_fee="0.04"),
        fill("2", "ENTRY", "6", "3", authoritative_fee="0.06"),
        fill("3", "EXIT", "5", "4", authoritative_fee="0.05"),
    )
    assert result.gross_entry_qty == Decimal("10")
    assert result.authoritative_entry_notional == Decimal("26")
    assert result.authoritative_gross_pnl == Decimal("7.0")
    assert result.authoritative_net_pnl == Decimal("6.900")


def test_base_entry_fee_preserves_gross_and_reduces_net_inventory():
    result = calculate(
        fill(
            "1", "ENTRY", "10", "2", fee_qty="1", fee_asset="BTC",
            authoritative_fee="2",
        ),
        fill("2", "EXIT", "9", "3", authoritative_fee="0"),
    )
    assert result.gross_entry_qty == Decimal("10")
    assert result.net_entry_inventory_qty == Decimal("9")
    assert result.gross_remaining_execution_qty == Decimal("1")
    assert result.remaining_inventory_qty == Decimal("0")
    assert result.financial_truth_status == "COMPLETE"
    assert result.authoritative_net_pnl == Decimal("7.0")


def test_third_asset_conversion_is_estimated_and_never_complete():
    result = calculate(
        fill(
            "1", "ENTRY", "10", "2", fee_asset="BNB",
            authoritative_fee=None, estimated_fee="0.12",
        ),
        fill("2", "EXIT", "10", "3"),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.authoritative_entry_fees_usdc is None
    assert result.authoritative_net_pnl is None
    assert result.estimated_fees_usdc == Decimal("0.12")
    assert "THIRD_ASSET_FEE_ESTIMATED" in result.failure_detail


def test_historical_usdc_fee_without_metadata_is_unclassified_estimate():
    entry = fill(
        "1", "ENTRY", "10", "2", fee_asset="USDC",
        authoritative_fee=None, estimated_fee="0.12", metadata=None, step=None,
    )
    exit_fill = fill(
        "2", "EXIT", "10", "3", fee_asset="USDC",
        authoritative_fee=None, estimated_fee="0.13", metadata=None, step=None,
    )
    entry = FillEvidence(**{
        **entry.__dict__, "base_asset": None, "quote_asset": None,
    })
    exit_fill = FillEvidence(**{
        **exit_fill.__dict__, "base_asset": None, "quote_asset": None,
    })

    result = calculate(entry, exit_fill)

    assert result.financial_truth_status == "INCOMPLETE"
    assert result.authoritative_fees_usdc is None
    assert result.authoritative_net_pnl is None
    assert result.estimated_fees_usdc == Decimal("0.25")
    assert "FEE_ASSET_ROLE_UNKNOWN" in result.failure_detail
    assert "MISSING_INSTRUMENT_METADATA" in result.failure_detail
    assert "FEE_VALUATION_ESTIMATED" in result.failure_detail
    assert "THIRD_ASSET_FEE_ESTIMATED" not in result.failure_detail


@pytest.mark.parametrize(
    ("fee_asset", "base_asset", "quote_asset", "expected"),
    [
        ("USDC", None, None, "UNKNOWN"),
        ("USDC", "BTC", None, "UNKNOWN"),
        ("BTC", None, "USDC", "UNKNOWN"),
        ("USDC", "BTC", "USDC", "QUOTE"),
        ("BTC", "BTC", "USDC", "BASE"),
        ("BNB", "BTC", "USDC", "THIRD"),
        (None, "BTC", "USDC", "UNKNOWN"),
    ],
)
def test_fee_asset_role_requires_sufficient_metadata(
    fee_asset, base_asset, quote_asset, expected
):
    assert classify_fee_asset_role(
        fee_asset, base_asset, quote_asset
    ) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("  ", None),
        ("usdc", "USDC"),
        (" None ", None),
        ("NULL", None),
        ("unknown", None),
        ("N/A", None),
    ],
)
def test_repository_optional_asset_normalization(value, expected):
    assert normalize_optional_asset(value) == expected


def test_exchange_reader_preserves_unclassified_fee_as_estimate():
    source = (
        ROOT / "common/financial_truth_repository.py"
    ).read_text(encoding="utf-8")
    assert "WHEN im.base_asset IS NULL" in source
    assert "OR im.quote_asset IS NULL" in source
    assert "THEN f.commission_usdc" in source


def test_missing_fee_is_not_zero():
    result = calculate(
        fill("1", "ENTRY", "10", "2", fee_qty=None, authoritative_fee=None),
        fill("2", "EXIT", "10", "3"),
    )
    assert result.authoritative_entry_fees_usdc is None
    assert result.authoritative_net_pnl is None


def test_missing_account_is_incomplete_not_failed():
    result = calculate(
        fill("1", "ENTRY", "10", "2", account=None),
        fill("2", "EXIT", "10", "3", account=None),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert result.failure_code == "MISSING_ACCOUNT_PROVENANCE"


def test_conflicting_account_is_failed():
    result = calculate(
        fill("1", "ENTRY", "10", "2", account="A"),
        fill("2", "EXIT", "10", "3", account="B"),
    )
    assert result.financial_truth_status == "FAILED"
    assert result.failure_code == "ACCOUNT_IDENTITY_CONFLICT"


def test_environment_or_deployment_cannot_replace_uid():
    result = calculate(
        fill("1", "ENTRY", "10", "2", account=None, deployment="same"),
        fill("2", "EXIT", "10", "3", account=None, deployment="same"),
    )
    assert result.financial_truth_status == "INCOMPLETE"


def test_missing_snapshot_with_nonzero_residual_is_incomplete():
    result = calculate(
        fill("1", "ENTRY", "10", "2", metadata=None, step=None),
        fill("2", "EXIT", "9", "3", metadata=None, step=None),
    )
    assert result.financial_truth_status == "INCOMPLETE"
    assert "MISSING_INSTRUMENT_SNAPSHOT" in result.failure_detail


def test_persisted_snapshot_drives_dust_tolerance():
    result = calculate(
        fill("1", "ENTRY", "10", "2", step="0.01"),
        fill("2", "EXIT", "9.995", "3", step="0.01"),
    )
    assert result.financial_truth_status == "COMPLETE"
    assert result.remaining_inventory_qty == Decimal("0.005")


def test_exit_beyond_entry_is_failed():
    result = calculate(
        fill("1", "ENTRY", "10", "2"),
        fill("2", "EXIT", "11", "3"),
    )
    assert result.financial_truth_status == "FAILED"
    assert result.failure_code == "EXIT_QUANTITY_EXCEEDS_ENTRY"


def test_fingerprint_is_deterministic_and_source_sensitive():
    first = fill("1", "ENTRY", "10", "2")
    assert source_fingerprint([first]) == source_fingerprint([first])
    changed = fill("1", "ENTRY", "10", "2.01")
    assert source_fingerprint([first]) != source_fingerprint([changed])


def test_okx_main_and_sub_account_identity():
    main = okx_account_identity({"data": [{"uid": "7", "mainUid": "7"}]}, captured_at=NOW)
    sub = okx_account_identity({"data": [{"uid": "8", "mainUid": "7"}]}, captured_at=NOW)
    assert main.scope == "MAIN"
    assert sub.scope == "SUB_ACCOUNT"
    assert main.fingerprint != sub.fingerprint


@pytest.mark.parametrize(
    "payload",
    ({}, {"data": []}, {"data": [{}]}, {"data": [{"uid": "1"}]}),
)
def test_okx_identity_rejects_incomplete_response(payload):
    with pytest.raises(ValueError, match="ACCOUNT_IDENTITY_INVALID_RESPONSE"):
        okx_account_identity(payload)


def test_identity_cache_hit_ttl_refresh_and_isolation():
    cache = AccountIdentityCache(ttl=timedelta(hours=24))
    calls = []

    def fetch(uid="1"):
        calls.append(uid)
        return okx_account_identity(
            {"data": [{"uid": uid, "mainUid": uid}]},
            captured_at=NOW + timedelta(days=len(calls) - 1),
        )

    one, status = cache.get("credential-hash-a", fetch, now=NOW)
    two, status_two = cache.get(
        "credential-hash-a", fetch, now=NOW + timedelta(hours=1)
    )
    three, status_three = cache.get(
        "credential-hash-a", fetch, now=NOW + timedelta(days=2)
    )
    other, _ = cache.get("credential-hash-b", lambda: fetch("2"), now=NOW)
    assert one == two
    assert status == "ACCOUNT_IDENTITY_FETCH_OK"
    assert status_two == "ACCOUNT_IDENTITY_CACHE_HIT"
    assert status_three == "ACCOUNT_IDENTITY_CACHE_REFRESH"
    assert other.uid == "2"
    assert len(calls) == 3


def test_identity_cache_error_invalidates():
    cache = AccountIdentityCache()
    cache.get(
        "scope",
        lambda: okx_account_identity(
            {"data": [{"uid": "1", "mainUid": "1"}]}, captured_at=NOW
        ),
        now=NOW,
    )
    with pytest.raises(RuntimeError):
        cache.get(
            "scope", lambda: (_ for _ in ()).throw(RuntimeError("auth")),
            now=NOW, refresh=True,
        )
    value, status = cache.get(
        "scope",
        lambda: okx_account_identity(
            {"data": [{"uid": "2", "mainUid": "2"}]}, captured_at=NOW
        ),
        now=NOW,
    )
    assert value.uid == "2"
    assert status == "ACCOUNT_IDENTITY_FETCH_OK"


def test_okx_identity_http_auth_error_is_structured(monkeypatch):
    monkeypatch.setenv("OKX_API_KEY", "key-must-not-appear")
    monkeypatch.setenv("OKX_API_SECRET", "secret-must-not-appear")
    monkeypatch.setenv("OKX_API_PASSPHRASE", "pass-must-not-appear")
    client = OkxMarketDataAdapter()
    monkeypatch.setattr(
        client,
        "_private_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            HTTPError("https://www.okx.com/api/v5/account/config", 401, "x", {}, None)
        ),
    )
    with pytest.raises(RuntimeError, match="ACCOUNT_IDENTITY_AUTH_ERROR") as exc:
        client.get_account_identity(refresh=True)
    rendered = str(exc.value)
    assert "key-must-not-appear" not in rendered
    assert "secret-must-not-appear" not in rendered
    assert "pass-must-not-appear" not in rendered


def test_activation_disabled_shadow_and_apply_gates(monkeypatch):
    activation = WriterActivation(False, "disabled", "paper", ("paper",))
    assert activation.authorize("shadow") == "shadow"
    assert activation.authorize("dry-run") == "dry-run"
    with pytest.raises(RuntimeError, match="feature flag disabled"):
        activation.authorize("apply")
    with pytest.raises(RuntimeError, match="LIVE apply denied"):
        WriterActivation(True, "apply", "live", ("live",)).authorize("apply")
    assert WriterActivation(True, "apply", "paper", ("paper",)).authorize("apply") == "apply"


def test_migration_is_schema_only_and_has_append_only_audit():
    sql = MIGRATION.read_text()
    upper = sql.upper()
    assert upper.startswith("BEGIN;")
    assert upper.rstrip().endswith("COMMIT;")
    assert "CREATE TABLE IF NOT EXISTS CANONICAL_FINANCIAL_TRUTH_AUDIT_V1" in upper
    assert "INSERT INTO CANONICAL_FINANCIAL_TRUTH" not in upper
    assert "UPDATE POSITIONS" not in upper
    assert "DELETE FROM" not in upper
    assert "CREATE TRIGGER" not in upper


def test_single_canonical_writer_path():
    matches = []
    for path in (ROOT / "common").glob("*.py"):
        text = path.read_text()
        if "INSERT INTO canonical_financial_truth_v1" in text:
            matches.append(path.name)
    assert matches == ["financial_truth_repository.py"]

from __future__ import annotations

import json
from types import SimpleNamespace

from common.legacy_recovery_repository import (
    EvidenceStatus,
    ExternalEvidenceFileAdapter,
)
from tools.legacy_recovery import _normalize_global_options, _raw_inventory, parser


def test_position_planner_never_reads_unbounded_strategy_telemetry():
    source = __import__("pathlib").Path(
        "common/legacy_recovery_repository.py"
    ).read_text()
    position_reader = source[
        source.index("class LegacyPositionEvidenceRepository"):
        source.index("class UnappliedFillEvidenceRepository")
    ]
    assert "FROM strategy_events" not in position_reader
    assert '"strategy_events": []' in position_reader


def test_cohort_commands_are_explicit_bounded_read_only_operations():
    choices = parser()._subparsers._group_actions[0].choices
    assert choices["audit-open-cohort"].get_default("limit") == 100
    assert "--limit" in choices["audit-open-cohort"].format_help()
    source = __import__("pathlib").Path("tools/legacy_recovery.py").read_text()
    assert "read_only_db_conn" in source
    assert "LIMIT %s" in source
    assert "apply" not in choices
    parsed = parser().parse_args(_normalize_global_options([
        "audit-open-cohort", "--environment", "LIVE",
        "--database-url-env", "DSN", "--expected-database", "trading_live",
    ]))
    assert parsed.command == "audit-open-cohort"


def test_external_operator_json_is_validated_and_fingerprinted(tmp_path):
    payload = {
        "source": "okx", "exchange_order_id": "order-1",
        "trade_id": "trade-1", "symbol": "BNB-USDC", "side": "SELL",
        "qty": "0.1", "price": "592.8", "fee": "-0.1",
        "fee_asset": "USDC", "timestamp": "2026-07-30T00:00:00Z",
        "client_order_id": "", "account_identity": "live-account-fp",
    }
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps(payload))
    first = ExternalEvidenceFileAdapter().read(
        path, source="okx", trade_id="trade-1", order_id="order-1",
    )
    second = ExternalEvidenceFileAdapter().read(
        path, source="okx", trade_id="trade-1", order_id="order-1",
    )
    assert first.evidence_status is EvidenceStatus.COMPLETE
    assert first.evidence["source_fingerprint"] == second.evidence["source_fingerprint"]


def test_raw_inventory_accounts_for_base_asset_fees():
    state = {"fills": [
        {
            "side": "BUY", "executed_qty": "1", "commission_amount": "0.01",
            "commission_asset": "BNB", "symbol": "BNBUSDC",
        },
        {
            "side": "SELL", "executed_qty": "0.5", "commission_amount": "0",
            "commission_asset": "USDC", "symbol": "BNBUSDC",
        },
    ]}
    assert tuple(map(str, _raw_inventory(state))) == ("1", "0.99", "0.5", "0.49")

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from common.recent_closed_read_model import (
    calculate_net_pnl_pct,
    resolve_entry_notional_usdc,
)


ROOT = Path(__file__).resolve().parents[1]
API_SOURCE = (ROOT / "api/main.py").read_text()
FRONTEND_API_SOURCE = (ROOT / "frontend/src/api.ts").read_text()
HANDLER = API_SOURCE[
    API_SOURCE.index('@app.get("/ui/recent-closed")'):
    API_SOURCE.index('@app.post("/ui/control/panic"')
]


@pytest.mark.parametrize(
    ("real", "simulated", "estimated", "historical", "expected"),
    [
        (20.0, 19.5, 19.0, 18.0, 20.0),
        (20.0, None, None, 0.0, 20.0),
        (None, 19.5, 19.0, 0.0, 19.5),
        (None, None, 19.0, 0.0, 19.0),
        (None, None, None, 18.0, 18.0),
        (None, None, None, 0.0, None),
        (0.0, None, None, 0.0, None),
    ],
)
def test_canonical_entry_notional_precedence(
    real, simulated, estimated, historical, expected
):
    assert resolve_entry_notional_usdc(
        real_execution_notional=real,
        simulated_execution_notional=simulated,
        estimated_notional=estimated,
        legacy_price_qty_notional=historical,
    ) == expected


def test_vps_live_fixture_uses_real_ssot_with_zero_qty():
    fixture = {
        "status": "CLOSED",
        "qty": 0.0,
        "entry_price": 1834.25,
        "entry_exec_notional_est": 20.03,
        "estimated_notional": None,
        "net_pnl_usdc": 0.41,
    }

    denominator = resolve_entry_notional_usdc(
        real_execution_notional=fixture["entry_exec_notional_est"],
        simulated_execution_notional=19.99,
        estimated_notional=fixture["estimated_notional"],
        legacy_price_qty_notional=fixture["entry_price"] * fixture["qty"],
    )

    assert denominator == Decimal("20.03")
    assert float(Decimal(str(fixture["net_pnl_usdc"])) / denominator * 100) == pytest.approx(
        2.0469296055916124
    )


def test_missing_denominator_keeps_record_and_percentage_unknown():
    record = {"id": 7, "status": "CLOSED"}
    denominator = resolve_entry_notional_usdc(
        real_execution_notional=None,
        simulated_execution_notional=None,
        estimated_notional=None,
        legacy_price_qty_notional=0,
    )
    pnl_pct = calculate_net_pnl_pct(1, denominator)

    assert record["id"] == 7
    assert pnl_pct is None


def test_recent_closed_sql_has_one_canonical_safe_denominator():
    precedence = [
        "p.entry_exec_notional_est",
        "p.simulated_entry_notional_usdc",
        "p.estimated_entry_notional_usdc",
        "p.entry_price * p.qty",
    ]
    resolved = HANDLER[
        HANDLER.index("resolved AS ("):HANDLER.index("FROM execution_evidence p")
    ]

    assert [resolved.index(source) for source in precedence] == sorted(
        resolved.index(source) for source in precedence
    )
    assert "NULLIF(" in resolved
    assert HANDLER.count("AS entry_notional_safe") == 1
    assert (
        "p.entry_notional_safe::double precision\n"
        "                  AS entry_notional_usdc"
        in HANDLER
    )
    assert "/ p.entry_notional_safe * 100.0" in HANDLER
    assert "/ COALESCE" not in HANDLER


def test_recent_closed_limits_positions_before_simulated_fill_aggregate():
    limit_at = HANDLER.index("LIMIT %s")
    lateral_at = HANDLER.index("LEFT JOIN LATERAL")
    fill_table_at = HANDLER.index("FROM simulated_execution_fills_v1 f")

    assert limit_at < lateral_at < fill_table_at
    assert "WHERE f.position_id = p.id" in HANDLER
    assert "FILTER (WHERE f.order_purpose = 'ENTRY')" in HANDLER
    assert "FILTER (WHERE f.order_purpose = 'EXIT')" in HANDLER
    assert "simulated_entry_fill_count" in HANDLER
    assert "simulated_exit_fill_count" in HANDLER


def test_recent_closed_payload_field_names_remain_compatible():
    assert '"entry_notional_usdc": _safe_float(r[10])' in HANDLER
    assert '"pnl_usdc": pnl_usdc' in HANDLER
    assert '"pnl_pct": _safe_float(r[13])' in HANDLER


def test_recent_closed_backend_uses_controlled_http_500():
    assert "logging.exception(\"ui/recent-closed failed\")" in HANDLER
    assert "status_code=500" in HANDLER
    assert "error\": str(" not in HANDLER
    assert "error_type" not in HANDLER


def test_recent_closed_client_rejects_legacy_error_payload():
    function = FRONTEND_API_SOURCE[
        FRONTEND_API_SOURCE.index("export async function getUiRecentClosed"):
        FRONTEND_API_SOURCE.index("export async function updatePanicState")
    ]
    assert "response.error || response.error_type" in function
    assert "throw new Error" in function
    assert "return response;" in function
    assert "division by zero" not in function.lower()

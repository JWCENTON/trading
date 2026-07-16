from __future__ import annotations

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
API_SOURCE = (ROOT / "api/main.py").read_text()
FRONTEND_API_SOURCE = (ROOT / "frontend/src/api.ts").read_text()
HANDLER = API_SOURCE[
    API_SOURCE.index('@app.get("/ui/recent-closed")'):
    API_SOURCE.index('@app.post("/ui/control/panic"')
]


def canonical_entry_notional(real, estimated, historical):
    for value in (real, estimated, historical):
        if value is not None:
            return None if value == 0 else value
    return None


@pytest.mark.parametrize(
    ("real", "estimated", "historical", "expected"),
    [
        (20.0, 19.0, 18.0, 20.0),
        (20.0, None, 0.0, 20.0),
        (None, 19.0, 0.0, 19.0),
        (None, None, 18.0, 18.0),
        (None, None, 0.0, None),
        (0.0, None, 0.0, None),
    ],
)
def test_canonical_entry_notional_precedence(
    real, estimated, historical, expected
):
    assert canonical_entry_notional(real, estimated, historical) == expected


def test_vps_live_fixture_uses_real_ssot_with_zero_qty():
    fixture = {
        "status": "CLOSED",
        "qty": 0.0,
        "entry_price": 1834.25,
        "entry_exec_notional_est": 20.03,
        "estimated_notional": None,
        "net_pnl_usdc": 0.41,
    }

    denominator = canonical_entry_notional(
        fixture["entry_exec_notional_est"],
        fixture["estimated_notional"],
        fixture["entry_price"] * fixture["qty"],
    )

    assert denominator == pytest.approx(20.03)
    assert fixture["net_pnl_usdc"] / denominator * 100 == pytest.approx(
        2.0469296055916124
    )


def test_missing_denominator_keeps_record_and_percentage_unknown():
    record = {"id": 7, "status": "CLOSED"}
    denominator = canonical_entry_notional(None, None, 0.0)
    pnl_pct = None if denominator is None else 1.0 / denominator * 100

    assert record["id"] == 7
    assert pnl_pct is None


def test_recent_closed_sql_has_one_canonical_safe_denominator():
    expected = """
                SELECT NULLIF(
                  COALESCE(
                    real.entry_exec_notional_est,
                    est.entry_notional_usdc,
                    p.entry_price * p.qty
                  ),
                  0
                ) AS entry_notional_safe
    """
    assert expected in HANDLER
    assert HANDLER.count("AS entry_notional_safe") == 1
    assert (
        "denom.entry_notional_safe::double precision AS entry_notional_usdc"
        in HANDLER
    )
    assert "/ denom.entry_notional_safe * 100.0" in HANDLER
    assert "/ COALESCE" not in HANDLER


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

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from common.equity_curve import EquityObservation, upsert_daily_snapshot
from common.paper_equity_baseline_v2 import (
    PaperEquityBaselineV2,
    activate_paper_equity_baseline_v2,
    calculate_post_baseline_paper_equity,
)


ROOT = Path(__file__).resolve().parents[2]
EQUITY_V1 = (
    ROOT / "db/migrations/20260809_equity_curve_baseline_v1.sql"
).read_text()
BASELINE_V2 = (
    ROOT / "db/migrations/20260811_paper_equity_baseline_v2.sql"
).read_text()
T0 = datetime(2026, 8, 11, 22, 30, tzinfo=timezone.utc)


def _baseline(**overrides) -> PaperEquityBaselineV2:
    values = {
        "baseline_id": 1,
        "deployment_id": "local-paper",
        "baseline_timestamp": T0,
        "baseline_account_total": Decimal("925"),
        "baseline_managed_equity": Decimal("925"),
        "baseline_external_manual": Decimal("0"),
        "baseline_available": Decimal("845"),
        "baseline_inventory_value": Decimal("80"),
        "baseline_realized_net_pnl": Decimal("-75"),
        "baseline_unrealized_pnl": Decimal("2"),
        "baseline_fees": Decimal("10"),
        "baseline_open_positions": 4,
        "frozen_pre_baseline_unresolved_count": 39,
        "evidence_status": "COMPLETE",
        "source_authority": "CANONICAL_PAPER_ACCOUNT_READ_MODEL_V1",
        "approved_by": "Jacek/Product Owner",
        "approval_provenance": {"approval_type": "PRODUCT_OWNER_APPROVED"},
        "activation_fingerprint": "a" * 64,
        "created_at": T0,
    }
    values.update(overrides)
    return PaperEquityBaselineV2(**values)


def _observation(total: str, timestamp: datetime = T0) -> EquityObservation:
    amount = Decimal(total)
    return EquityObservation(
        amount, Decimal("0"), None, amount-Decimal("80"), Decimal("80"),
        Decimal("-75"), Decimal("2"), Decimal("10"), 4, "INCOMPLETE",
        timestamp,
    )


def _unresolved(position_id: int) -> dict[int, dict[str, object]]:
    return {
        position_id: {
            "outcome_status": "UNRESOLVED",
            "evidence_status": "INCOMPLETE",
            "evidence_complete": False,
            "blocking_reasons": ["NO_AUTHORITATIVE_EVIDENCE"],
        }
    }


def test_pre_baseline_unresolved_is_outside_forward_denominator() -> None:
    result = calculate_post_baseline_paper_equity(
        _baseline(), closed_count=0, resolved_count=0,
        realized_net_pnl=None, fees=None,
        current_unrealized_pnl=Decimal("2"),
        current_inventory_value=Decimal("80"),
    )
    assert result.evidence_status == "COMPLETE"
    assert result.managed_equity == Decimal("925")


def test_new_unresolved_post_baseline_outcome_fails_closed() -> None:
    result = calculate_post_baseline_paper_equity(
        _baseline(), closed_count=1, resolved_count=0,
        realized_net_pnl=None, fees=None,
        current_unrealized_pnl=Decimal("2"),
        current_inventory_value=Decimal("80"),
    )
    assert result.evidence_status == "INCOMPLETE"
    assert result.managed_equity is None


def test_resolved_post_baseline_outcome_preserves_complete() -> None:
    result = calculate_post_baseline_paper_equity(
        _baseline(), closed_count=1, resolved_count=1,
        realized_net_pnl=Decimal("5"), fees=Decimal("0.2"),
        current_unrealized_pnl=Decimal("3"),
        current_inventory_value=Decimal("81"),
    )
    assert result.evidence_status == "COMPLETE"
    assert result.account_total == Decimal("931")
    assert result.managed_equity == Decimal("931")


class _NoSqlCursor:
    def execute(self, *_args, **_kwargs):
        pytest.fail("LIVE baseline path touched SQL")


def test_live_baseline_v2_is_not_applicable() -> None:
    result = activate_paper_equity_baseline_v2(
        _NoSqlCursor(), deployment_id="local-live",
        observation=_observation("100"), unresolved_outcomes={},
        approved_by="Jacek/Product Owner",
        approval_provenance={"approval_type": "PRODUCT_OWNER_APPROVED"},
        trading_mode="LIVE",
    )
    assert result.status == "NOT_APPLICABLE"
    assert result.baseline is None


def test_approval_provenance_is_required_before_sql() -> None:
    with pytest.raises(
        ValueError, match="PAPER_EQUITY_BASELINE_V2_APPROVAL_REQUIRED"
    ):
        activate_paper_equity_baseline_v2(
            _NoSqlCursor(), deployment_id="local-paper",
            observation=_observation("100"), unresolved_outcomes={},
            approved_by="", approval_provenance={}, trading_mode="PAPER",
        )


def test_append_only_activation_isolated_and_idempotent(
    disposable_postgres_v16,
) -> None:
    database = "waltrade_baseline_test_paper_equity_v2"
    disposable_postgres_v16.create_database(database)
    conn = disposable_postgres_v16.connect(database)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(EQUITY_V1)
        cur.execute(BASELINE_V2)
        cur.execute(BASELINE_V2)
        cur.execute(
            """
            CREATE TABLE positions(
              id BIGINT PRIMARY KEY,gross_pnl_usdc NUMERIC,fees_usdc NUMERIC,
              net_pnl_usdc NUMERIC,qty NUMERIC,entry_price NUMERIC,
              exit_price NUMERIC,entry_order_id TEXT,exit_order_id TEXT,
              exit_time TIMESTAMPTZ
            );
            CREATE TABLE canonical_financial_truth_v1(
              id BIGSERIAL PRIMARY KEY,position_id BIGINT
            );
            INSERT INTO positions VALUES
              (1,11,1,10,2,100,105,'entry-1','exit-1',%s),
              (2,-4,1,-5,3,50,48,'entry-2','exit-2',%s);
            INSERT INTO canonical_financial_truth_v1(position_id) VALUES (2);
            """,
            (T0, T0),
        )
        cur.execute(
            """
            SELECT id,gross_pnl_usdc,fees_usdc,net_pnl_usdc,qty,
                   entry_price,exit_price,entry_order_id,exit_order_id,exit_time
            FROM positions ORDER BY id
            """
        )
        economics_before = cur.fetchall()

    conn.autocommit = False
    with conn.cursor() as cur:
        local = activate_paper_equity_baseline_v2(
            cur, deployment_id="local-paper", observation=_observation("925"),
            unresolved_outcomes=_unresolved(1),
            approved_by="Jacek/Product Owner",
            approval_provenance={"approval_type": "PRODUCT_OWNER_APPROVED"},
        )
        assert local.created is True
        again = activate_paper_equity_baseline_v2(
            cur, deployment_id="local-paper", observation=_observation("999"),
            unresolved_outcomes=_unresolved(1),
            approved_by="Jacek/Product Owner",
            approval_provenance={"approval_type": "PRODUCT_OWNER_APPROVED"},
        )
        assert again.created is False
        assert again.baseline.baseline_id == local.baseline.baseline_id

        vps = activate_paper_equity_baseline_v2(
            cur, deployment_id="vps-paper",
            observation=_observation("1200", T0.replace(minute=31)),
            unresolved_outcomes=_unresolved(2),
            approved_by="Jacek/Product Owner",
            approval_provenance={"approval_type": "PRODUCT_OWNER_APPROVED"},
        )
        assert vps.baseline.baseline_account_total == Decimal("1200")

        complete = EquityObservation(
            Decimal("925"), Decimal("0"), Decimal("925"), Decimal("845"),
            Decimal("80"), None, Decimal("2"), None, 4, "COMPLETE", T0,
        )
        first = upsert_daily_snapshot(
            cur, deployment_id="local-paper", trading_mode="PAPER",
            observation=complete, snapshot_date=date(2026, 8, 11),
        )
        second = upsert_daily_snapshot(
            cur, deployment_id="local-paper", trading_mode="PAPER",
            observation=complete, snapshot_date=date(2026, 8, 11),
        )
        assert first == second
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_id,baseline_account_total FROM "
            "paper_equity_baseline_v2 ORDER BY deployment_id"
        )
        assert cur.fetchall() == [
            ("local-paper", Decimal("925")),
            ("vps-paper", Decimal("1200")),
        ]
        cur.execute(
            "SELECT deployment_id,position_id,classification,"
            "original_financial_truth_rows FROM "
            "paper_equity_frozen_outcome_v2 ORDER BY deployment_id"
        )
        assert cur.fetchall() == [
            ("local-paper", 1, "PRE_BASELINE_FROZEN", 0),
            ("vps-paper", 2, "PRE_BASELINE_FROZEN", 1),
        ]
        cur.execute(
            "SELECT count(*) FROM equity_daily_snapshot_v1 "
            "WHERE deployment_id='local-paper'"
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            """
            SELECT id,gross_pnl_usdc,fees_usdc,net_pnl_usdc,qty,
                   entry_price,exit_price,entry_order_id,exit_order_id,exit_time
            FROM positions ORDER BY id
            """
        )
        assert cur.fetchall() == economics_before
        cur.execute("SELECT position_id FROM canonical_financial_truth_v1")
        assert cur.fetchall() == [(2,)]
        with pytest.raises(Exception, match="APPEND_ONLY"):
            cur.execute(
                "UPDATE paper_equity_baseline_v2 SET approved_by='other' "
                "WHERE deployment_id='local-paper'"
            )
    conn.rollback()
    conn.close()

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import uuid

import psycopg2
import pytest

from common.financial_truth_calculator import calculate_financial_truth
from common.financial_truth_repository import (
    ExecutionEvidenceContext,
    FinancialTruthSourceRepository,
)
from common.financial_truth_writer import FinancialTruthReconciler
from common.simulated_execution_evidence import record_simulated_fill_evidence


SCHEMA = """
CREATE TABLE runtime_contract_adoption_v2 (
  adoption_id BIGINT PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  status TEXT NOT NULL,
  git_revision TEXT NOT NULL,
  adopted_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE positions (
  id BIGINT PRIMARY KEY,
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  interval TEXT NOT NULL,
  status TEXT NOT NULL,
  side TEXT NOT NULL,
  qty NUMERIC NOT NULL,
  entry_price NUMERIC,
  entry_time TIMESTAMPTZ,
  exit_price NUMERIC,
  exit_time TIMESTAMPTZ,
  exit_reason TEXT,
  gross_pnl_usdc NUMERIC,
  fees_usdc NUMERIC,
  net_pnl_usdc NUMERIC,
  inventory_evidence_status TEXT,
  gross_entry_executed_qty NUMERIC,
  entry_base_fee_qty NUMERIC,
  net_entry_inventory_qty NUMERIC,
  cumulative_exit_executed_qty NUMERIC DEFAULT 0,
  exit_inventory_reduction_qty NUMERIC,
  remaining_inventory_qty NUMERIC,
  terminal_dust_qty NUMERIC,
  terminal_reason TEXT,
  inventory_calculated_at TIMESTAMPTZ,
  inventory_contract_adoption_id BIGINT,
  inventory_contract_generation BIGINT
);
CREATE FUNCTION is_existing_projected_c2_2_compatible(BIGINT, TEXT)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$ SELECT FALSE $$;
CREATE TABLE simulated_orders (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,
  side TEXT NOT NULL,
  price NUMERIC NOT NULL,
  quantity_btc NUMERIC NOT NULL,
  reason TEXT NOT NULL,
  is_exit BOOLEAN NOT NULL
);
CREATE TABLE position_lifecycle_events_c2_2 (
  event_id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL,
  order_id TEXT NOT NULL,
  mutation_kind TEXT NOT NULL,
  mutation_high_water NUMERIC NOT NULL,
  payload JSONB NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  emitted_at TIMESTAMPTZ
);
"""


def _insert_order(
    conn, *, position_id: int, is_exit: bool, strategy: str = "BBRANGE",
    exit_reason: str | None = None,
) -> int:
    price = Decimal("99") if is_exit else Decimal("100")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO simulated_orders(
              symbol,interval,strategy,side,price,quantity_btc,reason,is_exit
            ) VALUES ('BTCUSDC','1m',%s,%s,%s,0.1,%s,%s)
            RETURNING id
            """,
            (
                strategy,
                "SELL" if is_exit else "BUY",
                price,
                exit_reason if is_exit else f"{strategy} ENTRY",
                is_exit,
            ),
        )
        order_id = int(cur.fetchone()[0])
    conn.commit()
    assert position_id > 0
    return order_id


def _record(
    connect, *, order_id: int, position_id: int, is_exit: bool,
    strategy: str = "BBRANGE", exit_reason: str | None = None,
):
    return record_simulated_fill_evidence(
        connect,
        client=object(),
        simulated_order_id=order_id,
        position_id=position_id,
        environment="paper",
        deployment_id="local-paper",
        exit_reason=exit_reason if is_exit else None,
        require_terminal_close=is_exit,
    )


@pytest.mark.parametrize(
    ("strategy", "exit_reason"),
    [
        pytest.param("BBRANGE", "BBRANGE PROFIT_LOCK", id="bbrange-profit-lock"),
        pytest.param("TREND", "TREND PAPER_EXIT", id="trend-paper-exit"),
        pytest.param("RSI", "RSI SOFT_EXIT", id="rsi-soft-exit"),
        pytest.param(
            "SUPERTREND", "SUPERTREND FLIP DOWN", id="supertrend-flip-down"
        ),
    ],
)
def test_paper_exit_fill_lifecycle_ft_and_failure_rollback(
    disposable_postgres_v16, monkeypatch, strategy, exit_reason
):
    database = f"waltrade_baseline_test_bbrange_{uuid.uuid4().hex[:10]}"
    disposable_postgres_v16.create_database(database)

    def connect():
        return disposable_postgres_v16.connect(database)

    monkeypatch.setenv("GIT_SHA", "b" * 40)
    monkeypatch.setenv("EXCHANGE", "OKX")
    monkeypatch.setenv("PAPER_SIMULATION_FEE_RATE", "0.0035")
    import common.sizing as sizing
    monkeypatch.setattr(sizing, "_FILTERS_CACHE", {})

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
        conn.commit()
        migration_root = Path(__file__).parents[2] / "db" / "migrations"
        with conn.cursor() as cur:
            cur.execute(
                (migration_root / (
                    "20260727_canonical_financial_truth_foundation_v1.sql"
                )).read_text()
            )
            cur.execute(
                (migration_root / (
                    "20260728_canonical_financial_truth_writer_v1.sql"
                )).read_text()
            )
            cur.execute(
                """
                ALTER TABLE simulated_execution_fills_v1
                  ADD COLUMN simulation_fee_rate NUMERIC,
                  ADD COLUMN fee_model_version TEXT,
                  ADD COLUMN fee_config_source TEXT
                """
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runtime_contract_adoption_v2 VALUES (
                  41,'FEE_AWARE_INVENTORY_C2_2','paper','local-paper',7,
                  'ACTIVE',%s,%s
                )
                """,
                ("a" * 40, datetime(2026, 8, 1, tzinfo=timezone.utc)),
            )
            cur.executemany(
                """
                INSERT INTO positions(
                  id,symbol,strategy,interval,status,side,qty,entry_price,
                  entry_time,inventory_contract_adoption_id,
                  inventory_contract_generation
                ) VALUES (%s,'BTCUSDC',%s,'1m','OPEN','LONG',0.1,100,
                          %s,41,7)
                """,
                [
                    (77, strategy, datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)),
                    (78, strategy, datetime(2026, 8, 2, 12, 1, tzinfo=timezone.utc)),
                ],
            )
        conn.commit()

        entry_order = _insert_order(
            conn, position_id=77, is_exit=False, strategy=strategy
        )
        assert _record(
            connect, order_id=entry_order, position_id=77, is_exit=False,
            strategy=strategy,
        ) is True
        exit_order = _insert_order(
            conn, position_id=77, is_exit=True, strategy=strategy,
            exit_reason=exit_reason,
        )
        assert _record(
            connect, order_id=exit_order, position_id=77, is_exit=True,
            strategy=strategy, exit_reason=exit_reason,
        ) is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status,exit_price,exit_reason FROM positions WHERE id=77"
            )
            assert cur.fetchone() == (
                "CLOSED", Decimal("99"), exit_reason,
            )
            cur.execute(
                """
                SELECT order_purpose FROM simulated_execution_fills_v1
                WHERE position_id=77 ORDER BY id
                """
            )
            assert cur.fetchall() == [("ENTRY",), ("EXIT",)]
            cur.execute(
                """
                SELECT count(*) FROM simulated_orders
                WHERE strategy=%s AND is_exit=true
                """,
                (strategy,),
            )
            assert cur.fetchone() == (1,)
            cur.execute(
                """
                SELECT mutation_kind FROM position_lifecycle_events_c2_2
                WHERE position_id=77
                """
            )
            assert cur.fetchall() == [("POSITION_CLOSED",)]

        repository = FinancialTruthSourceRepository(connect)
        position, fills, issue = repository.read_position_and_fills(
            77,
            context=ExecutionEvidenceContext(
                environment="paper", exchange=None,
                deployment_id="local-paper",
            ),
        )
        assert issue is None
        outcome = calculate_financial_truth(
            position_id=77,
            position_status=position[1],
            fills=fills,
            position_symbol=position[5],
        )
        assert outcome.financial_truth_status == "COMPLETE"
        assert outcome.calculation_version == "FINANCIAL_TRUTH_CALCULATION_V3"
        assert outcome.entry_fill_count == 1
        assert outcome.exit_fill_count == 1
        assert outcome.authoritative_fees_usdc is not None
        assert outcome.authoritative_fees_usdc > 0
        assert outcome.authoritative_net_pnl is not None
        assert (
            "WIN" if outcome.authoritative_net_pnl > 0
            else "LOSS" if outcome.authoritative_net_pnl < 0
            else "FLAT"
        ) in {"WIN", "LOSS"}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT financial_truth_status,calculation_version,
                       executed_entry_qty,executed_exit_qty,
                       authoritative_entry_fees_usdc,
                       authoritative_exit_fees_usdc,
                       authoritative_gross_pnl,authoritative_net_pnl,
                       entry_fill_count,exit_fill_count
                FROM canonical_financial_truth_v1 WHERE position_id=77
                """
            )
            canonical = cur.fetchone()
            status, calculation_version = canonical[:2]
            assert status == "COMPLETE"
            assert calculation_version == "FINANCIAL_TRUTH_CALCULATION_V3"
            assert canonical[2:] == (
                outcome.gross_entry_qty,
                outcome.gross_exit_qty,
                outcome.authoritative_entry_fees_usdc,
                outcome.authoritative_exit_fees_usdc,
                outcome.authoritative_gross_pnl,
                outcome.authoritative_net_pnl,
                outcome.entry_fill_count,
                outcome.exit_fill_count,
            )

        with conn:
            with conn.cursor() as cur:
                repeated = FinancialTruthReconciler(
                    connect
                ).reconcile_in_transaction(
                    77,
                    connection=conn,
                    cursor=cur,
                    evidence_context=ExecutionEvidenceContext(
                        environment="paper", exchange=None,
                        deployment_id="local-paper",
                    ),
                    invocation_identity=f"PAPER_SIMULATED_EXIT:{exit_order}",
                )
                assert repeated["written"] is False
                cur.execute(
                    "SELECT count(*) FROM canonical_financial_truth_v1 "
                    "WHERE position_id=77"
                )
                assert cur.fetchone() == (1,)

        rollback_entry = _insert_order(
            conn, position_id=78, is_exit=False, strategy=strategy
        )
        assert _record(
            connect, order_id=rollback_entry, position_id=78, is_exit=False,
            strategy=strategy,
        ) is True
        rollback_exit = _insert_order(
            conn, position_id=78, is_exit=True, strategy=strategy,
            exit_reason=exit_reason,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE FUNCTION reject_position_78_lifecycle()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.position_id=78 THEN
                    RAISE EXCEPTION 'synthetic lifecycle failure';
                  END IF;
                  RETURN NEW;
                END $$;
                CREATE TRIGGER reject_position_78_lifecycle
                BEFORE INSERT ON position_lifecycle_events_c2_2
                FOR EACH ROW EXECUTE FUNCTION reject_position_78_lifecycle();
                """
            )
        conn.commit()

        with pytest.raises(psycopg2.Error, match="synthetic lifecycle failure"):
            _record(
                connect, order_id=rollback_exit,
                position_id=78, is_exit=True, strategy=strategy,
                exit_reason=exit_reason,
            )

        with conn.cursor() as cur:
            cur.execute("SELECT status FROM positions WHERE id=78")
            assert cur.fetchone() == ("OPEN",)
            cur.execute(
                """
                SELECT order_purpose FROM simulated_execution_fills_v1
                WHERE position_id=78 ORDER BY id
                """
            )
            assert cur.fetchall() == [("ENTRY",)]
            cur.execute(
                """
                SELECT count(*) FROM position_lifecycle_events_c2_2
                WHERE position_id=78
                """
            )
            assert cur.fetchone() == (0,)
            cur.execute(
                """
                SELECT count(*) FROM canonical_financial_truth_v1
                WHERE position_id=78
                """
            )
            assert cur.fetchone() == (0,)
    finally:
        conn.close()

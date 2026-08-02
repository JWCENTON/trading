from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, getcontext

from tests.postgres.database_baseline_fixture import disposable_postgres_v16
from tests.test_financial_truth_arithmetic_contract_v1 import CASES, calculation_for


def test_postgresql_numeric_python_reference_and_canonical_exact_parity(
    disposable_postgres_v16,
):
    database = "waltrade_baseline_test_ft_arithmetic_v1"
    disposable_postgres_v16.create_database(database)
    pg = replace(disposable_postgres_v16, database=database)
    connection = pg.connect()
    try:
        with connection.cursor() as cur:
            cur.execute(
                "CREATE TABLE oracle_inputs("
                "position_id BIGINT PRIMARY KEY,gross_entry NUMERIC,"
                "entry_base_fee NUMERIC,gross_exit NUMERIC,"
                "entry_notional NUMERIC,exit_notional NUMERIC,"
                "entry_fee NUMERIC,exit_fee NUMERIC)"
            )
            cur.executemany(
                "INSERT INTO oracle_inputs VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                [(
                    position_id, case["entry_qty"], case["entry_base_fee"],
                    case["exit_qty"], case["entry_notional"],
                    case["exit_notional"], case["entry_fee"], case["exit_fee"],
                ) for position_id, case in CASES.items()],
            )
        connection.commit()

        query = """
            WITH ratios AS (
              SELECT *,
                LEAST(ROUND(gross_exit/gross_entry,20),1) AS gross_ratio,
                LEAST(ROUND(gross_exit/(gross_entry-entry_base_fee),20),1)
                  AS inventory_ratio
              FROM oracle_inputs
            )
            SELECT position_id,
              exit_notional-entry_notional*gross_ratio AS gross_pnl,
              entry_fee+exit_fee AS fees,
              exit_notional-entry_notional*gross_ratio
                -entry_fee*inventory_ratio-exit_fee AS net_pnl
            FROM ratios ORDER BY position_id
        """
        original = getcontext().copy()
        try:
            for precision in (6, 12, 28, 50, 100):
                getcontext().prec = precision
                with connection.cursor() as cur:
                    cur.execute(query)
                    postgres = {int(row[0]): row[1:] for row in cur.fetchall()}
                for position_id, case in CASES.items():
                    canonical = calculation_for(position_id)
                    expected = (
                        Decimal(case["gross_pnl"]), Decimal(case["fees"]),
                        Decimal(case["net_pnl"]),
                    )
                    assert postgres[position_id] == expected
                    assert (
                        canonical.authoritative_gross_pnl,
                        canonical.authoritative_fees_usdc,
                        canonical.authoritative_net_pnl,
                    ) == expected
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding

        with connection.cursor() as cur:
            cur.execute(query)
            first = cur.fetchall()
            cur.execute(query)
            second = cur.fetchall()
        assert first == second
    finally:
        connection.close()

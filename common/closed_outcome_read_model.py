from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


CLOSED_OUTCOME_CTE = """
WITH bounded_positions AS MATERIALIZED (
  SELECT p.*
  FROM positions p
  WHERE p.status = 'CLOSED'
    AND p.exit_time IS NOT NULL
    AND p.exit_time >= %(window_start)s
    AND p.exit_time <= %(window_end)s
),
bounded_financial_truth AS MATERIALIZED (
  SELECT ft.*
  FROM canonical_financial_truth_v1 ft
  JOIN bounded_positions p ON p.id = ft.position_id
  WHERE ft.financial_truth_status = 'COMPLETE'
),
bounded_simulated_fills AS MATERIALIZED (
  SELECT
    f.position_id,
    COUNT(*) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_fill_count,
    COUNT(*) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_fill_count,
    SUM(f.fill_qty) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_qty,
    SUM(f.fill_qty) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_qty,
    SUM(f.fill_notional) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_notional,
    SUM(f.fill_notional) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_notional,
    SUM(COALESCE(f.authoritative_fee_usdc, f.estimated_fee_usdc))
      AS total_fees,
    BOOL_AND(
      COALESCE(f.authoritative_fee_usdc, f.estimated_fee_usdc) IS NOT NULL
    ) AS fees_complete
  FROM simulated_execution_fills_v1 f
  JOIN bounded_positions p ON p.id = f.position_id
  WHERE f.order_purpose IN ('ENTRY', 'EXIT')
  GROUP BY f.position_id
),
evidence AS (
  SELECT
    p.id AS position_id,
    p.side,
    p.gross_pnl_usdc AS stored_gross,
    p.fees_usdc AS stored_fees,
    p.net_pnl_usdc AS stored_net,
    p.inventory_evidence_status,
    p.inventory_contract_generation,
    ft.authoritative_gross_pnl AS ft_gross,
    COALESCE(
      ft.authoritative_fees_usdc,
      ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc
    ) AS ft_fees,
    ft.authoritative_net_pnl AS ft_net,
    fills.entry_fill_count,
    fills.exit_fill_count,
    fills.entry_qty,
    fills.exit_qty,
    fills.entry_notional,
    fills.exit_notional,
    fills.total_fees,
    fills.fees_complete,
    (
      ft.position_id IS NOT NULL
      AND ft.authoritative_gross_pnl IS NOT NULL
      AND ft.authoritative_net_pnl IS NOT NULL
      AND COALESCE(
        ft.authoritative_fees_usdc,
        ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc
      ) IS NOT NULL
    ) AS financial_truth_complete,
    (
      p.gross_pnl_usdc IS NOT NULL
      AND p.fees_usdc IS NOT NULL
      AND p.net_pnl_usdc IS NOT NULL
      AND p.inventory_evidence_status = 'COMPLETE'
      AND p.inventory_contract_generation IS NOT NULL
      AND p.exit_context_json->>'outcome_provenance' = 'CLOSED_OUTCOME_V1'
      AND abs(p.net_pnl_usdc - (p.gross_pnl_usdc - p.fees_usdc)) <= 0.00000001
      AND NOT (
        p.gross_pnl_usdc = 0
        AND p.fees_usdc = 0
        AND p.net_pnl_usdc = 0
      )
    ) AS stored_proven,
    (
      COALESCE(fills.entry_fill_count, 0) > 0
      AND COALESCE(fills.exit_fill_count, 0) > 0
      AND fills.entry_qty IS NOT NULL
      AND fills.exit_qty IS NOT NULL
      AND fills.entry_qty = fills.exit_qty
      AND fills.entry_notional IS NOT NULL
      AND fills.exit_notional IS NOT NULL
      AND fills.fees_complete IS TRUE
      AND fills.total_fees IS NOT NULL
    ) AS fills_complete
  FROM bounded_positions p
  LEFT JOIN bounded_financial_truth ft ON ft.position_id = p.id
  LEFT JOIN bounded_simulated_fills fills ON fills.position_id = p.id
),
closed_outcomes AS (
  SELECT
    position_id,
    CASE
      WHEN financial_truth_complete THEN 'RESOLVED'
      WHEN stored_proven THEN 'RESOLVED'
      WHEN fills_complete THEN 'RESOLVED'
      ELSE 'UNRESOLVED'
    END AS outcome_status,
    CASE
      WHEN financial_truth_complete THEN 'FINANCIAL_TRUTH'
      WHEN stored_proven THEN 'STORED_PROVEN'
      WHEN fills_complete THEN 'SIMULATED_FILLS'
      ELSE 'UNRESOLVED'
    END AS outcome_source,
    CASE
      WHEN financial_truth_complete THEN ft_gross
      WHEN stored_proven THEN stored_gross
      WHEN fills_complete AND UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
        THEN entry_notional - exit_notional
      WHEN fills_complete THEN exit_notional - entry_notional
      ELSE NULL
    END::numeric AS gross_pnl_usdc,
    CASE
      WHEN financial_truth_complete THEN ft_fees
      WHEN stored_proven THEN stored_fees
      WHEN fills_complete THEN total_fees
      ELSE NULL
    END::numeric AS fees_usdc,
    CASE
      WHEN financial_truth_complete THEN ft_net
      WHEN stored_proven THEN stored_net
      WHEN fills_complete AND UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
        THEN entry_notional - exit_notional - total_fees
      WHEN fills_complete THEN exit_notional - entry_notional - total_fees
      ELSE NULL
    END::numeric AS net_pnl_usdc,
    (financial_truth_complete OR stored_proven OR fills_complete)
      AS evidence_complete
  FROM evidence
),
classified_outcomes AS (
  SELECT
    *,
    CASE
      WHEN NOT evidence_complete THEN 'UNRESOLVED'
      WHEN net_pnl_usdc > 0 THEN 'WIN'
      WHEN net_pnl_usdc < 0 THEN 'LOSS'
      ELSE 'FLAT'
    END AS result_class
  FROM closed_outcomes
)
"""


CLOSED_OUTCOME_ROWS_SQL = CLOSED_OUTCOME_CTE + """
SELECT
  position_id,
  outcome_status,
  outcome_source,
  gross_pnl_usdc,
  fees_usdc,
  net_pnl_usdc,
  result_class,
  evidence_complete
FROM classified_outcomes
ORDER BY position_id
"""


CLOSED_OUTCOME_SUMMARY_SQL = CLOSED_OUTCOME_CTE + """
SELECT
  COUNT(*)::int AS trades,
  COUNT(*) FILTER (WHERE evidence_complete)::int AS resolved_trades,
  COUNT(*) FILTER (WHERE NOT evidence_complete)::int AS unresolved_trades,
  COUNT(*) FILTER (WHERE result_class = 'WIN')::int AS wins,
  COUNT(*) FILTER (WHERE result_class = 'LOSS')::int AS losses,
  COUNT(*) FILTER (WHERE result_class = 'FLAT')::int AS flats,
  SUM(net_pnl_usdc) FILTER (WHERE evidence_complete) AS net_pnl,
  SUM(gross_pnl_usdc) FILTER (WHERE evidence_complete) AS gross_pnl,
  SUM(fees_usdc) FILTER (WHERE evidence_complete) AS fees,
  MAX(net_pnl_usdc) FILTER (WHERE evidence_complete) AS best_trade,
  MIN(net_pnl_usdc) FILTER (WHERE evidence_complete) AS worst_trade,
  COALESCE(
    jsonb_object_agg(outcome_source, source_count)
      FILTER (WHERE outcome_source IS NOT NULL),
    '{}'::jsonb
  ) AS outcome_source_counts
FROM (
  SELECT
    c.*,
    COUNT(*) OVER (PARTITION BY outcome_source)::int AS source_count
  FROM classified_outcomes c
) outcomes
"""


def fetch_closed_outcome_summary(
    cur: Any, *, window_start: datetime, window_end: datetime
) -> dict[str, Any]:
    if window_start > window_end:
        raise ValueError("window_start must not be after window_end")
    cur.execute(
        CLOSED_OUTCOME_SUMMARY_SQL,
        {"window_start": window_start, "window_end": window_end},
    )
    row = cur.fetchone()
    trades = int(row[0] or 0)
    resolved = int(row[1] or 0)
    wins = int(row[3] or 0)
    losses = int(row[4] or 0)
    flats = int(row[5] or 0)
    return {
        "trades": trades,
        "resolved_trades": resolved,
        "unresolved_trades": int(row[2] or 0),
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "net_pnl": row[6],
        "gross_pnl": row[7],
        "fees": row[8],
        "best_trade": row[9],
        "worst_trade": row[10],
        "coverage_ratio": (
            Decimal(resolved) / Decimal(trades) if trades else Decimal("1")
        ),
        "win_rate": (
            Decimal(wins) / Decimal(resolved) * Decimal("100")
            if resolved
            else Decimal("0")
        ),
        "outcome_source_counts": dict(row[11] or {}),
    }


def fetch_closed_outcomes(
    cur: Any, *, window_start: datetime, window_end: datetime
) -> dict[int, dict[str, Any]]:
    if window_start > window_end:
        raise ValueError("window_start must not be after window_end")
    cur.execute(
        CLOSED_OUTCOME_ROWS_SQL,
        {"window_start": window_start, "window_end": window_end},
    )
    return {
        int(row[0]): {
            "position_id": int(row[0]),
            "outcome_status": row[1],
            "outcome_source": row[2],
            "gross_pnl_usdc": row[3],
            "fees_usdc": row[4],
            "net_pnl_usdc": row[5],
            "result_class": row[6],
            "evidence_complete": bool(row[7]),
        }
        for row in cur.fetchall()
    }

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


OUTCOME_SOURCE_PRECEDENCE = (
    "FINANCIAL_TRUTH",
    "LEGACY_EXECUTION_PROVEN",
    "PAPER_SIMULATED_FILLS",
    "VERIFIED_LEGACY_STORED",
    "UNRESOLVED",
)

PAPER_OUTCOME_NORMALIZATION_VERSION = "PAPER_OUTCOME_NORMALIZATION_V1"
PAPER_OUTCOME_CALCULATION_VERSION = "CLOSED_OUTCOME_PAPER_V2"


LIVE_CLOSED_OUTCOME_CTE = """
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
bounded_legacy_order_identities AS MATERIALIZED (
  SELECT p.id AS position_id, p.symbol, p.side, p.entry_order_id AS order_id,
         'ENTRY'::text AS purpose
  FROM bounded_positions p
  WHERE p.entry_order_id IS NOT NULL
  UNION ALL
  SELECT p.id, p.symbol, p.side, p.exit_order_id, 'EXIT'::text
  FROM bounded_positions p
  WHERE p.exit_order_id IS NOT NULL
),
bounded_legacy_fills AS MATERIALIZED (
  SELECT
    p.position_id,
    f.source,
    f.trade_id,
    f.order_id,
    f.symbol AS fill_symbol,
    f.side AS fill_side,
    f.executed_qty,
    f.quote_notional_usdc,
    f.commission_amount,
    f.commission_asset,
    f.commission_usdc,
    p.purpose
  FROM bounded_legacy_order_identities p
  JOIN binance_order_fills f ON f.order_id = p.order_id
),
bounded_legacy_execution AS MATERIALIZED (
  SELECT
    p.id AS position_id,
    COUNT(*) FILTER (WHERE f.purpose = 'ENTRY') AS entry_fill_count,
    COUNT(*) FILTER (WHERE f.purpose = 'EXIT') AS exit_fill_count,
    SUM(f.executed_qty) FILTER (WHERE f.purpose = 'ENTRY') AS entry_qty,
    SUM(f.executed_qty) FILTER (WHERE f.purpose = 'EXIT') AS exit_qty,
    SUM(f.quote_notional_usdc) FILTER (WHERE f.purpose = 'ENTRY')
      AS entry_notional,
    SUM(f.quote_notional_usdc) FILTER (WHERE f.purpose = 'EXIT')
      AS exit_notional,
    SUM(f.commission_usdc) FILTER (WHERE f.purpose = 'ENTRY') AS entry_fees,
    SUM(f.commission_usdc) FILTER (WHERE f.purpose = 'EXIT') AS exit_fees,
    SUM(f.commission_amount) FILTER (
      WHERE f.purpose = 'ENTRY'
        AND replace(upper(f.commission_asset), '-', '') =
            replace(replace(upper(p.symbol), '-', ''), 'USDC', '')
    ) AS entry_base_fee,
    SUM(f.commission_amount) FILTER (
      WHERE f.purpose = 'EXIT'
        AND replace(upper(f.commission_asset), '-', '') =
            replace(replace(upper(p.symbol), '-', ''), 'USDC', '')
    ) AS exit_base_fee,
    COUNT(f.order_id) AS fill_count,
    COUNT(DISTINCT (lower(f.source), f.trade_id)) AS trade_identity_count,
    BOOL_AND(upper(replace(f.fill_symbol, '-', '')) = upper(replace(p.symbol, '-', '')))
      AS symbols_consistent,
    BOOL_AND(
      (f.purpose = 'ENTRY' AND upper(f.fill_side) = 'BUY') OR
      (f.purpose = 'EXIT' AND upper(f.fill_side) = 'SELL')
    ) AS sides_consistent,
    BOOL_AND(
      f.commission_amount IS NOT NULL
      AND f.commission_asset IS NOT NULL
      AND f.commission_usdc IS NOT NULL
      AND (
        upper(replace(f.commission_asset, '-', '')) = 'USDC'
        OR replace(upper(f.commission_asset), '-', '') =
           replace(replace(upper(p.symbol), '-', ''), 'USDC', '')
      )
    ) AS fees_complete,
    GREATEST(
      COALESCE(MAX(scale(f.executed_qty)) FILTER (WHERE f.purpose='ENTRY'), 0),
      COALESCE(MAX(scale(f.executed_qty)) FILTER (WHERE f.purpose='EXIT'), 0)
    ) AS quantity_scale
  FROM bounded_positions p
  LEFT JOIN bounded_legacy_fills f ON f.position_id = p.id
  GROUP BY p.id, p.symbol
),
legacy_calculated AS MATERIALIZED (
  SELECT
    l.*,
    LEAST(l.exit_qty / NULLIF(l.entry_qty, 0), 1) AS exited_ratio,
    (
      l.entry_qty - COALESCE(l.entry_base_fee, 0)
      - l.exit_qty - COALESCE(l.exit_base_fee, 0)
    ) AS remaining_qty,
    power(10::numeric, -l.quantity_scale) AS quantity_tolerance,
    NOT EXISTS (
      SELECT 1 FROM exchange_fill_ingestion_state_v2 i
      WHERE i.order_id IN (p.entry_order_id, p.exit_order_id)
        AND i.application_status IN ('CORRECTION_PENDING', 'AMBIGUOUS')
    ) AS no_pending_correction
  FROM bounded_positions p
  JOIN bounded_legacy_execution l ON l.position_id = p.id
),
bounded_legacy_outcomes AS MATERIALIZED (
  SELECT
    p.id AS position_id,
    CASE
      WHEN upper(coalesce(p.side, 'LONG')) IN ('SELL', 'SHORT')
        THEN l.entry_notional * l.exited_ratio - l.exit_notional
      ELSE l.exit_notional - l.entry_notional * l.exited_ratio
    END AS gross_pnl,
    l.entry_fees * l.exited_ratio + l.exit_fees AS fees,
    l.entry_notional,
    (
      p.entry_order_id <> p.exit_order_id
      AND l.entry_fill_count > 0 AND l.exit_fill_count > 0
      AND l.entry_qty > 0 AND l.exit_qty > 0
      AND l.entry_notional IS NOT NULL AND l.exit_notional IS NOT NULL
      AND l.symbols_consistent IS TRUE AND l.sides_consistent IS TRUE
      AND l.fees_complete IS TRUE
      AND l.fill_count = l.trade_identity_count
      AND abs(l.remaining_qty) <= l.quantity_tolerance
      AND l.no_pending_correction
    ) AS evidence_complete,
    ARRAY_REMOVE(ARRAY[
      CASE WHEN p.entry_order_id = p.exit_order_id THEN 'ORDER_IDENTITY_CONFLICT' END,
      CASE WHEN l.entry_fill_count = 0 THEN 'MISSING_ENTRY_FILLS' END,
      CASE WHEN l.exit_fill_count = 0 THEN 'MISSING_EXIT_FILLS' END,
      CASE WHEN l.symbols_consistent IS NOT TRUE THEN 'SYMBOL_CONFLICT' END,
      CASE WHEN l.sides_consistent IS NOT TRUE THEN 'SIDE_CONFLICT' END,
      CASE WHEN l.fees_complete IS NOT TRUE THEN 'FEE_EVIDENCE_INCOMPLETE' END,
      CASE WHEN l.fill_count <> l.trade_identity_count THEN 'DUPLICATE_TRADE_IDENTITY' END,
      CASE WHEN abs(l.remaining_qty) > l.quantity_tolerance THEN 'INVENTORY_CONFLICT' END,
      CASE WHEN NOT l.no_pending_correction THEN 'PENDING_CORRECTION' END
    ], NULL) AS blocking_reasons
  FROM bounded_positions p
  JOIN legacy_calculated l ON l.position_id = p.id
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
    legacy.gross_pnl AS legacy_gross,
    legacy.fees AS legacy_fees,
    legacy.gross_pnl - legacy.fees AS legacy_net,
    legacy.evidence_complete AS legacy_complete,
    legacy.blocking_reasons AS legacy_blocking_reasons,
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
    ) AS stored_proven
  FROM bounded_positions p
  LEFT JOIN bounded_financial_truth ft ON ft.position_id = p.id
  LEFT JOIN bounded_legacy_outcomes legacy ON legacy.position_id = p.id
),
closed_outcomes AS (
  SELECT
    position_id,
    CASE
      WHEN financial_truth_complete THEN 'RESOLVED'
      WHEN legacy_complete THEN 'RESOLVED'
      WHEN stored_proven THEN 'RESOLVED'
      ELSE 'UNRESOLVED'
    END AS outcome_status,
    CASE
      WHEN financial_truth_complete THEN 'FINANCIAL_TRUTH'
      WHEN legacy_complete THEN 'LEGACY_EXECUTION_PROVEN'
      WHEN stored_proven THEN 'STORED_PROVEN'
      ELSE 'UNRESOLVED'
    END AS outcome_source,
    CASE
      WHEN financial_truth_complete THEN ft_gross
      WHEN legacy_complete THEN legacy_gross
      WHEN stored_proven THEN stored_gross
      ELSE NULL
    END::numeric AS gross_pnl_usdc,
    CASE
      WHEN financial_truth_complete THEN ft_fees
      WHEN legacy_complete THEN legacy_fees
      WHEN stored_proven THEN stored_fees
      ELSE NULL
    END::numeric AS fees_usdc,
    CASE
      WHEN financial_truth_complete THEN ft_net
      WHEN legacy_complete THEN legacy_net
      WHEN stored_proven THEN stored_net
      ELSE NULL
    END::numeric AS net_pnl_usdc,
    (financial_truth_complete OR legacy_complete OR stored_proven)
      AS evidence_complete,
    CASE
      WHEN financial_truth_complete OR legacy_complete OR stored_proven
        THEN 'COMPLETE'
      ELSE 'INCOMPLETE'
    END AS evidence_status,
    CASE
      WHEN legacy_blocking_reasons IS NOT NULL THEN legacy_blocking_reasons
      ELSE ARRAY[]::text[]
    END AS blocking_reasons,
    CASE WHEN financial_truth_complete THEN 'HIGH_ASSURANCE'
      WHEN legacy_complete THEN 'LIVE_ONLY'
      WHEN stored_proven THEN 'LEGACY_COMPATIBLE'
      ELSE 'UNRESOLVED' END AS quality_class,
    CASE WHEN stored_proven THEN 'VERIFIED_LEGACY_STORED'
      ELSE 'LEGACY_STORED_INCOMPLETE' END AS legacy_stored_status,
    'SOURCE_NOT_COMPARABLE'::text AS normalization_status,
    stored_net AS normalization_stored_value,
    CASE WHEN financial_truth_complete THEN ft_net
      WHEN legacy_complete THEN legacy_net
      WHEN stored_proven THEN stored_net END AS normalization_resolved_value,
    NULL::numeric AS normalization_delta,
    NULL::text AS normalization_version,
    'CLOSED_OUTCOME_LIVE_V1'::text AS calculation_version,
    CASE WHEN stored_net IS NOT NULL THEN scale(stored_net) END AS stored_scale,
    NULL::integer AS fill_scale,
    NULL::text AS legacy_stored_provenance,
    NULL::text AS legacy_fee_model,
    NULL::numeric AS gross_delta,
    NULL::numeric AS fee_delta,
    NULL::numeric AS net_delta,
    NULL::numeric AS gross_rounding_bound,
    NULL::numeric AS fee_rounding_bound,
    NULL::numeric AS net_serialization_bound,
    NULL::numeric AS maximum_explainable_net_delta,
    NULL::numeric AS reconstructed_net_delta,
    CASE WHEN financial_truth_complete THEN 'AUTHORITATIVE'
      WHEN legacy_complete THEN 'HIGH_ASSURANCE'
      WHEN stored_proven THEN 'LEGACY_COMPATIBLE'
      ELSE 'UNRESOLVED' END AS selected_source_confidence,
    'NOT_EVALUABLE'::text AS rollout_impact,
    'NONE'::text AS comparison_source,
    'UNRESOLVED'::text AS comparison_source_confidence,
    NULL::text AS source_superseded_reason
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


PAPER_CLOSED_OUTCOME_CTE = """
WITH bounded_positions AS MATERIALIZED (
  SELECT p.id, p.side, p.entry_price, p.exit_price, p.qty,
    p.exit_context_json, p.gross_pnl_usdc, p.fees_usdc, p.net_pnl_usdc,
    p.symbol, p.entry_order_id, p.exit_order_id
  FROM positions p
  WHERE p.status = 'CLOSED' AND p.exit_time IS NOT NULL
    AND p.exit_time >= %(window_start)s AND p.exit_time <= %(window_end)s
),
bounded_financial_truth AS MATERIALIZED (
  SELECT ft.* FROM canonical_financial_truth_v1 ft
  JOIN bounded_positions p ON p.id = ft.position_id
  WHERE ft.financial_truth_status = 'COMPLETE'
),
bounded_ft_authority AS MATERIALIZED (
  SELECT ft.*, ft_position.exit_context_json AS ft_exit_context_json,
    (ft.financial_truth_status = 'COMPLETE'
      AND COALESCE(ft.entry_fill_count, 0) > 0
      AND COALESCE(ft.exit_fill_count, 0) > 0
      AND ft.executed_entry_qty IS NOT NULL
      AND ft.executed_exit_qty IS NOT NULL
      AND ft.executed_entry_qty = ft.executed_exit_qty
      AND COALESCE(ft.remaining_inventory_qty, ft.remaining_qty, 0) = 0
      AND ft.authoritative_gross_pnl IS NOT NULL
      AND COALESCE(ft.authoritative_fees_usdc,
        ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc) IS NOT NULL
      AND ft.authoritative_net_pnl IS NOT NULL
      AND ft.authoritative_gross_pnl - COALESCE(ft.authoritative_fees_usdc,
        ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc)
        = ft.authoritative_net_pnl
      AND ft.source_fingerprint IS NOT NULL
      AND ft.source_order_ids IS NOT NULL
      AND jsonb_array_length(ft.source_order_ids) >= 2
      AND ft.source_fill_ids IS NOT NULL
      AND jsonb_array_length(ft.source_fill_ids) =
        COALESCE(ft.entry_fill_count, 0) + COALESCE(ft.exit_fill_count, 0)
      AND (SELECT COUNT(DISTINCT value)
        FROM jsonb_array_elements_text(ft.source_fill_ids)) =
        jsonb_array_length(ft.source_fill_ids)
      AND ft.calculation_version IS NOT NULL
      AND ft.failure_reason IS NULL
      AND ft.failure_code IS NULL
      AND ft.failure_detail IS NULL
    ) AS authoritative_evidence_valid
  FROM bounded_financial_truth ft
  JOIN positions ft_position ON ft_position.id = ft.position_id
),
bounded_simulated_fills AS MATERIALIZED (
  SELECT f.position_id,
    COUNT(*) AS fill_count,
    COUNT(*) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_fill_count,
    COUNT(*) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_fill_count,
    SUM(f.fill_qty) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_qty,
    SUM(f.fill_qty) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_qty,
    SUM(f.fill_notional) FILTER (WHERE f.order_purpose = 'ENTRY') AS entry_notional,
    SUM(f.fill_notional) FILTER (WHERE f.order_purpose = 'EXIT') AS exit_notional,
    SUM(COALESCE(f.authoritative_fee_usdc, f.estimated_fee_usdc)) AS total_fees,
    BOOL_AND(COALESCE(f.authoritative_fee_usdc, f.estimated_fee_usdc) IS NOT NULL)
      AS fees_complete,
    BOOL_AND(upper(replace(f.symbol, '-', '')) = upper(replace(p.symbol, '-', '')))
      AS symbols_consistent,
    BOOL_AND(
      (f.order_purpose = 'ENTRY' AND upper(f.side) =
        CASE WHEN upper(coalesce(p.side, 'LONG')) IN ('SELL', 'SHORT') THEN 'SELL' ELSE 'BUY' END)
      OR
      (f.order_purpose = 'EXIT' AND upper(f.side) =
        CASE WHEN upper(coalesce(p.side, 'LONG')) IN ('SELL', 'SHORT') THEN 'BUY' ELSE 'SELL' END)
    ) AS sides_consistent,
    GREATEST(COALESCE(MAX(scale(f.fill_qty)), 0),
      COALESCE(MAX(scale(f.fill_notional)), 0),
      COALESCE(MAX(scale(COALESCE(f.authoritative_fee_usdc, f.estimated_fee_usdc))), 0))
      AS fill_scale
  FROM simulated_execution_fills_v1 f
  JOIN bounded_positions p ON p.id = f.position_id
  WHERE f.order_purpose IN ('ENTRY', 'EXIT')
  GROUP BY f.position_id
),
bounded_correction_orders AS MATERIALIZED (
  SELECT DISTINCT i.order_id
  FROM exchange_fill_ingestion_state_v2 i
  WHERE i.application_status IN ('CORRECTION_PENDING', 'AMBIGUOUS')
),
evidence AS (
  SELECT p.id AS position_id, p.side,
    p.entry_price, p.exit_price, p.qty,
    COALESCE(p.exit_context_json, ft.ft_exit_context_json) AS exit_context_json,
    p.gross_pnl_usdc AS stored_gross, p.fees_usdc AS stored_fees,
    p.net_pnl_usdc AS stored_net,
    ft.authoritative_gross_pnl AS ft_gross,
    COALESCE(ft.authoritative_fees_usdc,
      ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc) AS ft_fees,
    ft.authoritative_net_pnl AS ft_net,
    ft.financial_truth_status AS ft_status,
    ft.entry_fill_count AS ft_entry_fill_count,
    ft.exit_fill_count AS ft_exit_fill_count,
    ft.executed_entry_qty AS ft_entry_qty,
    ft.executed_exit_qty AS ft_exit_qty,
    COALESCE(ft.remaining_inventory_qty, ft.remaining_qty) AS ft_remaining_qty,
    ft.source_fingerprint AS ft_source_fingerprint,
    ft.source_order_ids AS ft_source_order_ids,
    ft.source_fill_ids AS ft_source_fill_ids,
    ft.calculation_version AS ft_calculation_version,
    ft.failure_reason AS ft_failure_reason,
    ft.failure_code AS ft_failure_code,
    ft.failure_detail AS ft_failure_detail,
    fills.fill_count, fills.entry_notional, fills.exit_notional, fills.total_fees,
    fills.symbols_consistent, fills.sides_consistent, fills.fill_scale,
    (ft.position_id IS NOT NULL AND ft.authoritative_gross_pnl IS NOT NULL
      AND ft.authoritative_net_pnl IS NOT NULL
      AND COALESCE(ft.authoritative_fees_usdc,
        ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc) IS NOT NULL
    ) AS financial_truth_complete,
    (ft.authoritative_evidence_valid
      AND entry_correction.order_id IS NULL
      AND exit_correction.order_id IS NULL
    ) AS financial_truth_authoritative_valid,
    (ft.position_id IS NOT NULL AND COALESCE((COALESCE(
      p.exit_context_json, ft.ft_exit_context_json)->>'outcome_provenance' IN (
        'FINANCIAL_TRUTH', 'AUTHORITATIVE_OUTCOME_V1'
      )
      AND COALESCE(COALESCE(p.exit_context_json, ft.ft_exit_context_json)
          ->>'calculation_version',
        COALESCE(p.exit_context_json, ft.ft_exit_context_json)
          ->>'outcome_calculation_version') IS NOT NULL
      AND COALESCE(COALESCE(p.exit_context_json, ft.ft_exit_context_json)
          ->>'source_fingerprint',
        COALESCE(p.exit_context_json, ft.ft_exit_context_json)
          ->>'evidence_identity') IS NOT NULL
    ), FALSE)) AS stored_authoritative_trusted,
    (p.gross_pnl_usdc IS NOT NULL AND p.fees_usdc IS NOT NULL
      AND p.net_pnl_usdc IS NOT NULL
      AND p.entry_price IS NOT NULL AND p.exit_price IS NOT NULL
      AND p.entry_price <> 0
      AND (COALESCE(p.qty, 0) > 0 OR p.entry_price <> p.exit_price)
      AND abs(p.net_pnl_usdc - (p.gross_pnl_usdc - p.fees_usdc)) <= 0.00000001
      AND entry_correction.order_id IS NULL
      AND exit_correction.order_id IS NULL
    ) AS legacy_stored_structurally_valid,
    (COALESCE(fills.entry_fill_count, 0) > 0
      AND COALESCE(fills.exit_fill_count, 0) > 0
      AND fills.entry_qty IS NOT NULL AND fills.exit_qty IS NOT NULL
      AND fills.entry_qty = fills.exit_qty
      AND fills.entry_notional IS NOT NULL AND fills.exit_notional IS NOT NULL
      AND fills.fees_complete IS TRUE AND fills.total_fees IS NOT NULL
      AND fills.symbols_consistent IS TRUE AND fills.sides_consistent IS TRUE
      AND entry_correction.order_id IS NULL
      AND exit_correction.order_id IS NULL
    ) AS fills_complete
  FROM bounded_positions p
  LEFT JOIN bounded_ft_authority ft ON ft.position_id = p.id
  LEFT JOIN bounded_simulated_fills fills ON fills.position_id = p.id
  LEFT JOIN bounded_correction_orders entry_correction
    ON entry_correction.order_id = p.entry_order_id
  LEFT JOIN bounded_correction_orders exit_correction
    ON exit_correction.order_id = p.exit_order_id
),
resolved_evidence AS (
  SELECT *,
    (legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
      AND NOT financial_truth_complete) AS verified_legacy_stored,
    CASE WHEN financial_truth_complete THEN 'FINANCIAL_TRUTH'
      WHEN fills_complete THEN 'PAPER_SIMULATED_FILLS'
      WHEN legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
        THEN 'VERIFIED_LEGACY_STORED'
      ELSE 'UNRESOLVED' END AS selected_source,
    CASE WHEN financial_truth_complete THEN ft_gross
      WHEN fills_complete AND UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
        THEN entry_notional - exit_notional
      WHEN fills_complete THEN exit_notional - entry_notional
      WHEN legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
        THEN stored_gross END::numeric AS resolved_gross,
    CASE WHEN financial_truth_complete THEN ft_fees
      WHEN fills_complete THEN total_fees
      WHEN legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
        THEN stored_fees END::numeric AS resolved_fees,
    CASE WHEN financial_truth_complete THEN ft_net
      WHEN fills_complete AND UPPER(COALESCE(side, 'LONG')) IN ('SELL', 'SHORT')
        THEN entry_notional - exit_notional - total_fees
      WHEN fills_complete THEN exit_notional - entry_notional - total_fees
      WHEN legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
        THEN stored_net END::numeric AS resolved_net
  FROM evidence
),
normalization_evidence AS (
  SELECT *,
    CASE WHEN stored_gross IS NOT NULL AND resolved_gross IS NOT NULL
      THEN stored_gross - resolved_gross END AS gross_delta,
    CASE WHEN stored_fees IS NOT NULL AND resolved_fees IS NOT NULL
      THEN stored_fees - resolved_fees END AS fee_delta,
    CASE WHEN stored_net IS NOT NULL AND resolved_net IS NOT NULL
      THEN stored_net - resolved_net END AS net_delta,
    CASE WHEN stored_gross IS NOT NULL
      THEN 0.5 * power(10::numeric, -scale(stored_gross)) END
      AS gross_rounding_bound,
    CASE WHEN stored_fees IS NOT NULL
      THEN 0.5 * power(10::numeric, -scale(stored_fees)) END
      AS fee_rounding_bound,
    CASE WHEN stored_net IS NOT NULL
      THEN 0.5 * power(10::numeric, -scale(stored_net)) END
      AS net_serialization_bound
  FROM resolved_evidence
),
closed_outcomes AS (
  SELECT position_id,
    CASE WHEN selected_source <> 'UNRESOLVED' THEN 'RESOLVED'
      ELSE 'UNRESOLVED' END AS outcome_status,
    selected_source AS outcome_source,
    resolved_gross AS gross_pnl_usdc,
    resolved_fees AS fees_usdc,
    resolved_net AS net_pnl_usdc,
    (selected_source <> 'UNRESOLVED') AS evidence_complete,
    CASE WHEN selected_source <> 'UNRESOLVED' THEN 'COMPLETE'
      ELSE 'INCOMPLETE' END AS evidence_status,
    ARRAY_REMOVE(ARRAY[
      CASE WHEN COALESCE(fill_count, 0) > 0 AND NOT fills_complete
        THEN 'SIMULATED_EVIDENCE_INCOMPLETE' END,
      CASE WHEN stored_net IS NOT NULL AND NOT legacy_stored_structurally_valid
        THEN 'LEGACY_STORED_INCOMPLETE' END
    ], NULL) AS blocking_reasons,
    CASE WHEN selected_source IN ('FINANCIAL_TRUTH', 'PAPER_SIMULATED_FILLS')
      THEN 'HIGH_ASSURANCE'
      WHEN selected_source = 'VERIFIED_LEGACY_STORED' THEN 'LEGACY_COMPATIBLE'
      ELSE 'UNRESOLVED' END AS quality_class,
    CASE
      WHEN legacy_stored_structurally_valid AND COALESCE(fill_count, 0) = 0
        THEN 'VERIFIED_LEGACY_STORED'
      WHEN legacy_stored_structurally_valid AND fills_complete
        AND stored_gross - stored_fees = stored_net
        AND resolved_gross - resolved_fees = resolved_net
        AND abs(gross_delta) <= gross_rounding_bound
        AND abs(fee_delta) <= fee_rounding_bound
        AND abs(net_delta - (gross_delta - fee_delta)) <= net_serialization_bound
        THEN 'VERIFIED_LEGACY_STORED'
      WHEN stored_net IS NOT NULL AND (COALESCE(fill_count, 0) > 0 OR
        NOT legacy_stored_structurally_valid) THEN 'LEGACY_STORED_CONFLICT'
      ELSE 'LEGACY_STORED_INCOMPLETE'
    END AS legacy_stored_status,
    CASE
      WHEN stored_gross IS NULL OR stored_fees IS NULL OR stored_net IS NULL
        OR resolved_gross IS NULL OR resolved_fees IS NULL OR resolved_net IS NULL
        THEN 'SOURCE_NOT_COMPARABLE'
      WHEN stored_gross - stored_fees <> stored_net
        OR resolved_gross - resolved_fees <> resolved_net
        OR abs(gross_delta) > gross_rounding_bound
        OR abs(fee_delta) > fee_rounding_bound
        OR abs(net_delta - (gross_delta - fee_delta)) > net_serialization_bound
        THEN 'MATERIAL_CONFLICT'
      WHEN gross_delta = 0 AND fee_delta = 0 AND net_delta = 0 THEN 'EXACT_MATCH'
      WHEN abs(net_delta) <= net_serialization_bound THEN 'ROUNDING_ONLY'
      WHEN abs(net_delta) <=
        gross_rounding_bound + fee_rounding_bound + net_serialization_bound
        THEN 'COMPONENT_ROUNDING_ACCUMULATION'
      ELSE 'MATERIAL_CONFLICT'
    END AS normalization_status,
    stored_net AS normalization_stored_value,
    resolved_net AS normalization_resolved_value,
    CASE WHEN stored_net IS NOT NULL AND resolved_net IS NOT NULL
      THEN resolved_net - stored_net END AS normalization_delta,
    'PAPER_OUTCOME_NORMALIZATION_V1'::text AS normalization_version,
    'CLOSED_OUTCOME_PAPER_V2'::text AS calculation_version,
    CASE WHEN stored_net IS NOT NULL THEN scale(stored_net) END AS stored_scale,
    fill_scale,
    exit_context_json->>'outcome_provenance' AS legacy_stored_provenance,
    COALESCE(exit_context_json->>'fee_model',
      exit_context_json->>'fee_model_version') AS legacy_fee_model,
    gross_delta,
    fee_delta,
    net_delta,
    gross_rounding_bound,
    fee_rounding_bound,
    net_serialization_bound,
    gross_rounding_bound + fee_rounding_bound + net_serialization_bound
      AS maximum_explainable_net_delta,
    gross_delta - fee_delta AS reconstructed_net_delta,
    CASE WHEN selected_source = 'FINANCIAL_TRUTH' THEN 'AUTHORITATIVE'
      WHEN selected_source = 'PAPER_SIMULATED_FILLS' THEN 'HIGH_ASSURANCE'
      WHEN selected_source = 'VERIFIED_LEGACY_STORED' THEN 'LEGACY_COMPATIBLE'
      ELSE 'UNRESOLVED' END AS selected_source_confidence,
    CASE WHEN stored_gross IS NOT NULL OR stored_fees IS NOT NULL
      OR stored_net IS NOT NULL THEN 'HISTORICAL_STORED'
      ELSE 'NONE' END AS comparison_source,
    CASE WHEN stored_authoritative_trusted THEN 'AUTHORITATIVE'
      WHEN legacy_stored_structurally_valid THEN 'LEGACY_COMPATIBLE'
      ELSE 'UNRESOLVED' END AS comparison_source_confidence,
    ft_status,
    financial_truth_authoritative_valid,
    stored_authoritative_trusted,
    ft_entry_qty,
    ft_exit_qty,
    ft_gross,
    ft_fees,
    ft_net,
    stored_gross,
    stored_fees,
    stored_net,
    exit_context_json
  FROM normalization_evidence
),
rollout_classified AS (
  SELECT *,
    CASE
      WHEN ft_status = 'COMPLETE' AND NOT financial_truth_authoritative_valid
        THEN 'BLOCKING_EVIDENCE_INCONSISTENT'
      WHEN normalization_status = 'SOURCE_NOT_COMPARABLE'
        OR outcome_source = 'UNRESOLVED' THEN 'NOT_EVALUABLE'
      WHEN normalization_status = 'EXACT_MATCH' THEN 'NON_BLOCKING_EXACT'
      WHEN normalization_status = 'ROUNDING_ONLY' THEN 'NON_BLOCKING_ROUNDING'
      WHEN normalization_status = 'COMPONENT_ROUNDING_ACCUMULATION'
        THEN 'NON_BLOCKING_COMPONENT_ROUNDING'
      WHEN normalization_status = 'MATERIAL_CONFLICT'
        AND outcome_source = 'FINANCIAL_TRUTH'
        AND financial_truth_authoritative_valid
        AND stored_gross = 0 AND stored_fees = 0 AND stored_net = 0
        AND NOT stored_authoritative_trusted
        AND COALESCE(exit_context_json->>'outcome_provenance', '') NOT IN (
          'FINANCIAL_TRUTH', 'AUTHORITATIVE_OUTCOME_V1'
        )
        AND COALESCE(exit_context_json->>'calculation_version',
          exit_context_json->>'outcome_calculation_version') IS NULL
        AND COALESCE(exit_context_json->>'source_fingerprint',
          exit_context_json->>'evidence_identity') IS NULL
        AND COALESCE(ft_entry_qty, 0) > 0
        AND COALESCE(ft_exit_qty, 0) > 0
        AND (ft_gross <> 0 OR ft_fees <> 0 OR ft_net <> 0)
        THEN 'NON_BLOCKING_SOURCE_SUPERSEDED'
      WHEN normalization_status = 'MATERIAL_CONFLICT'
        AND selected_source_confidence IN ('AUTHORITATIVE', 'HIGH_ASSURANCE')
        THEN 'BLOCKING_AUTHORITATIVE_CONFLICT'
      WHEN normalization_status = 'MATERIAL_CONFLICT'
        THEN 'BLOCKING_EVIDENCE_INCONSISTENT'
      ELSE 'NOT_EVALUABLE'
    END AS rollout_impact,
    CASE
      WHEN normalization_status = 'MATERIAL_CONFLICT'
        AND outcome_source = 'FINANCIAL_TRUTH'
        AND financial_truth_authoritative_valid
        AND stored_gross = 0 AND stored_fees = 0 AND stored_net = 0
        AND NOT stored_authoritative_trusted
        AND COALESCE(exit_context_json->>'calculation_version',
          exit_context_json->>'outcome_calculation_version') IS NULL
        AND COALESCE(exit_context_json->>'source_fingerprint',
          exit_context_json->>'evidence_identity') IS NULL
        AND COALESCE(ft_entry_qty, 0) > 0
        AND COALESCE(ft_exit_qty, 0) > 0
        AND (ft_gross <> 0 OR ft_fees <> 0 OR ft_net <> 0)
        THEN 'AUTHORITATIVE_FT_SUPERSEDES_UNTRUSTED_STORED_ZERO_PLACEHOLDER'
      ELSE NULL
    END AS source_superseded_reason
  FROM closed_outcomes
),
classified_outcomes AS (
  SELECT *, CASE WHEN NOT evidence_complete THEN 'UNRESOLVED'
    WHEN net_pnl_usdc > 0 THEN 'WIN' WHEN net_pnl_usdc < 0 THEN 'LOSS'
    ELSE 'FLAT' END AS result_class
  FROM rollout_classified
)
"""


ROWS_SQL_SUFFIX = """
SELECT
  position_id, outcome_status, outcome_source, gross_pnl_usdc, fees_usdc,
  net_pnl_usdc, result_class, evidence_complete, evidence_status, blocking_reasons,
  quality_class, legacy_stored_status, normalization_status,
  normalization_stored_value, normalization_resolved_value, normalization_delta,
  normalization_version, calculation_version, stored_scale, fill_scale,
  legacy_stored_provenance, legacy_fee_model, gross_delta, fee_delta, net_delta,
  gross_rounding_bound, fee_rounding_bound, net_serialization_bound,
  maximum_explainable_net_delta, reconstructed_net_delta,
  selected_source_confidence, rollout_impact, comparison_source,
  comparison_source_confidence, source_superseded_reason
FROM classified_outcomes
ORDER BY position_id
"""


SUMMARY_SQL_SUFFIX = """
, summary_outcomes AS MATERIALIZED (
  SELECT evidence_complete, result_class, net_pnl_usdc, gross_pnl_usdc,
    fees_usdc, outcome_source, quality_class, normalization_stored_value,
    normalization_resolved_value, normalization_delta, normalization_status,
    rollout_impact
  FROM classified_outcomes
)
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
  COALESCE(jsonb_object_agg(outcome_source, source_count)
    FILTER (WHERE outcome_source IS NOT NULL), '{}'::jsonb) AS outcome_source_counts,
  COUNT(*) FILTER (WHERE quality_class = 'HIGH_ASSURANCE')::int AS high_assurance_count,
  COUNT(*) FILTER (WHERE quality_class = 'LEGACY_COMPATIBLE')::int AS legacy_compatible_count,
  COALESCE(jsonb_object_agg(quality_class, quality_count)
    FILTER (WHERE quality_class IS NOT NULL), '{}'::jsonb) AS quality_breakdown,
  SUM(normalization_stored_value) FILTER (
    WHERE normalization_stored_value IS NOT NULL) AS stored_net_comparable,
  SUM(normalization_resolved_value) FILTER (
    WHERE normalization_stored_value IS NOT NULL
      AND normalization_resolved_value IS NOT NULL) AS resolved_net_comparable,
  SUM(normalization_delta) FILTER (WHERE normalization_delta IS NOT NULL)
    AS normalization_delta,
  CASE
    WHEN BOOL_OR(normalization_status = 'MATERIAL_CONFLICT') THEN 'MATERIAL_CONFLICT'
    WHEN BOOL_OR(normalization_status = 'COMPONENT_ROUNDING_ACCUMULATION')
      THEN 'NON_MATERIAL_NORMALIZATION'
    WHEN BOOL_OR(normalization_status = 'ROUNDING_ONLY') THEN 'ROUNDING_ONLY'
    WHEN BOOL_OR(normalization_status = 'EXACT_MATCH') THEN 'EXACT_MATCH'
    ELSE 'SOURCE_NOT_COMPARABLE'
  END AS aggregate_normalization_status,
  COUNT(*) FILTER (
    WHERE normalization_status = 'COMPONENT_ROUNDING_ACCUMULATION')::int
    AS component_rounding_accumulation_count,
  COUNT(*) FILTER (WHERE normalization_status = 'MATERIAL_CONFLICT')::int
    AS material_conflict_count,
  COALESCE(jsonb_object_agg(normalization_status, normalization_count)
    FILTER (WHERE normalization_status IS NOT NULL), '{}'::jsonb)
    AS normalization_status_counts,
  COUNT(*) FILTER (WHERE rollout_impact IN (
    'BLOCKING_AUTHORITATIVE_CONFLICT', 'BLOCKING_EVIDENCE_INCONSISTENT'))::int
    AS blocking_conflict_count,
  COUNT(*) FILTER (WHERE rollout_impact = 'NON_BLOCKING_SOURCE_SUPERSEDED')::int
    AS superseded_conflict_count,
  COUNT(*) FILTER (WHERE rollout_impact = 'BLOCKING_AUTHORITATIVE_CONFLICT')::int
    AS authoritative_conflict_count,
  COUNT(*) FILTER (WHERE rollout_impact = 'BLOCKING_EVIDENCE_INCONSISTENT')::int
    AS evidence_inconsistent_count,
  COUNT(*) FILTER (WHERE rollout_impact = 'NOT_EVALUABLE')::int
    AS not_evaluable_count,
  CASE
    WHEN BOOL_OR(rollout_impact IN (
      'BLOCKING_AUTHORITATIVE_CONFLICT', 'BLOCKING_EVIDENCE_INCONSISTENT'))
      THEN 'BLOCKED'
    WHEN BOOL_OR(rollout_impact = 'NOT_EVALUABLE') THEN 'INCOMPLETE'
    ELSE 'PASS'
  END AS rollout_gate_status,
  COALESCE(jsonb_object_agg(rollout_impact, rollout_impact_count)
    FILTER (WHERE rollout_impact IS NOT NULL), '{}'::jsonb)
    AS rollout_impact_counts
FROM (
  SELECT evidence_complete, result_class, net_pnl_usdc, gross_pnl_usdc,
    fees_usdc, outcome_source, quality_class, normalization_stored_value,
    normalization_resolved_value, normalization_delta, normalization_status,
    rollout_impact,
    COUNT(*) OVER (PARTITION BY outcome_source)::int AS source_count,
    COUNT(*) OVER (PARTITION BY quality_class)::int AS quality_count,
    COUNT(*) OVER (PARTITION BY normalization_status)::int AS normalization_count,
    COUNT(*) OVER (PARTITION BY rollout_impact)::int AS rollout_impact_count
  FROM summary_outcomes c
) outcomes
"""


def _closed_outcome_cte(environment: str) -> str:
    normalized = str(environment).strip().upper()
    if normalized == "LIVE":
        return LIVE_CLOSED_OUTCOME_CTE
    if normalized == "PAPER":
        return PAPER_CLOSED_OUTCOME_CTE
    raise ValueError(f"unsupported closed-outcome environment: {environment!r}")


def build_closed_outcome_rows_sql(
    environment: str, *, bounded_position_ids: bool = False
) -> str:
    cte = _closed_outcome_cte(environment)
    if bounded_position_ids:
        cte = cte.replace(
            "AND p.exit_time <= %(window_end)s",
            "AND p.exit_time <= %(window_end)s\n"
            "    AND p.id = ANY(%(position_ids)s)",
            1,
        )
    return cte + ROWS_SQL_SUFFIX


def build_closed_outcome_summary_sql(environment: str) -> str:
    cte = _closed_outcome_cte(environment)
    if str(environment).strip().upper() == "PAPER":
        # Account aggregation only needs stored provenance when FT is selected.
        # Avoid detoasting historical exit contexts for the entire account.
        cte = cte.replace(
            "p.exit_context_json, p.gross_pnl_usdc",
            "NULL::jsonb AS exit_context_json, "
            "p.gross_pnl_usdc",
            1,
        )
    return cte + SUMMARY_SQL_SUFFIX


def fetch_closed_outcome_summary(
    cur: Any, *, environment: str, window_start: datetime, window_end: datetime
) -> dict[str, Any]:
    if window_start > window_end:
        raise ValueError("window_start must not be after window_end")
    # This expression-heavy bounded query is faster without PostgreSQL JIT's
    # per-request compilation cost; LOCAL keeps the setting transaction-scoped.
    cur.execute("SET LOCAL jit = off")
    cur.execute(
        build_closed_outcome_summary_sql(environment),
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
        "high_assurance_count": int(row[12] or 0),
        "legacy_compatible_count": int(row[13] or 0),
        "quality_breakdown": dict(row[14] or {}),
        "stored_net_comparable": row[15],
        "resolved_net_comparable": row[16],
        "normalization_delta": row[17],
        "aggregate_normalization_status": row[18],
        "component_rounding_accumulation_count": int(row[19] or 0),
        "material_conflict_count": int(row[20] or 0),
        "normalization_status_counts": dict(row[21] or {}),
        "blocking_conflict_count": int(row[22] or 0),
        "superseded_conflict_count": int(row[23] or 0),
        "authoritative_conflict_count": int(row[24] or 0),
        "evidence_inconsistent_count": int(row[25] or 0),
        "not_evaluable_count": int(row[26] or 0),
        "rollout_gate_status": row[27],
        "rollout_impact_counts": dict(row[28] or {}),
        "normalization_version": (
            PAPER_OUTCOME_NORMALIZATION_VERSION
            if str(environment).strip().upper() == "PAPER" else None
        ),
    }


def fetch_closed_outcomes(
    cur: Any, *, environment: str, window_start: datetime, window_end: datetime,
    position_ids: list[int] | None = None,
) -> dict[int, dict[str, Any]]:
    if window_start > window_end:
        raise ValueError("window_start must not be after window_end")
    cur.execute("SET LOCAL jit = off")
    cur.execute(
        build_closed_outcome_rows_sql(
            environment, bounded_position_ids=position_ids is not None
        ),
        {
            "window_start": window_start,
            "window_end": window_end,
            "position_ids": position_ids,
        },
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
            "evidence_status": row[8],
            "blocking_reasons": list(row[9] or []),
            "quality_class": row[10],
            "legacy_stored_status": row[11],
            "normalization_status": row[12],
            "normalization_stored_value": row[13],
            "normalization_resolved_value": row[14],
            "normalization_delta": row[15],
            "normalization_version": row[16],
            "calculation_version": row[17],
            "stored_scale": row[18],
            "fill_scale": row[19],
            "legacy_stored_provenance": row[20],
            "legacy_fee_model": row[21],
            "gross_delta": row[22],
            "fee_delta": row[23],
            "net_delta": row[24],
            "gross_rounding_bound": row[25],
            "fee_rounding_bound": row[26],
            "net_serialization_bound": row[27],
            "maximum_explainable_net_delta": row[28],
            "reconstructed_net_delta": row[29],
            "selected_source_confidence": row[30],
            "rollout_impact": row[31],
            "comparison_source": row[32],
            "comparison_source_confidence": row[33],
            "source_superseded_reason": row[34],
        }
        for row in cur.fetchall()
    }

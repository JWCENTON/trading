-- WALTRADE STRATEGY REGIME FEE PRESSURE GUARD V1
-- A near-zero aggregate gross PnL must not be rendered as an out-of-contract
-- percentage. Such a cohort has no meaningful fee-pressure ratio and is NULL.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    conflicting_checksum TEXT;
BEGIN
    IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION
            'STRATEGY_REGIME_FEE_PRESSURE_REQUIRED_RELATION_MISSING:schema_migration_ledger_v1';
    END IF;
    IF to_regclass('public.positions') IS NULL THEN
        RAISE EXCEPTION
            'STRATEGY_REGIME_FEE_PRESSURE_REQUIRED_RELATION_MISSING:positions';
    END IF;
    IF to_regclass('public.strategy_regime_stats') IS NULL THEN
        RAISE EXCEPTION
            'STRATEGY_REGIME_FEE_PRESSURE_REQUIRED_RELATION_MISSING:strategy_regime_stats';
    END IF;

    SELECT checksum_sha256 INTO conflicting_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260803_strategy_regime_fee_pressure_guard_v1.sql'
      AND checksum_sha256<>
          '64750738d6f13869764cfff3263e0cb4ae081a896812a69a0038b167abdb023e'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflicting_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'STRATEGY_REGIME_FEE_PRESSURE_LEDGER_CHECKSUM_CONFLICT:%',
            conflicting_checksum;
    END IF;
END;
$dependencies$;

CREATE OR REPLACE VIEW public.v_strategy_regime_14d AS
SELECT
    strategy,
    symbol,
    "interval",
    market_regime,
    count(*) AS trades,
    count(*) FILTER (WHERE net_pnl_usdc > 0) AS wins,
    count(*) FILTER (WHERE net_pnl_usdc <= 0) AS losses,
    round(sum(net_pnl_usdc), 8) AS net_pnl_usdc,
    round(sum(gross_pnl_usdc), 8) AS gross_pnl_usdc,
    round(sum(fees_usdc), 8) AS fees_usdc,
    round(avg(net_pnl_usdc), 8) AS avg_net_usdc,
    round(
        100.0 * count(*) FILTER (WHERE net_pnl_usdc > 0)
        / NULLIF(count(*), 0),
        4
    ) AS win_rate_pct,
    round(
        sum(CASE WHEN net_pnl_usdc > 0 THEN net_pnl_usdc ELSE 0 END)
        / NULLIF(abs(sum(
            CASE WHEN net_pnl_usdc < 0 THEN net_pnl_usdc ELSE 0 END
        )), 0),
        6
    ) AS profit_factor,
    CASE
        WHEN abs(sum(gross_pnl_usdc)) = 0 THEN NULL
        WHEN abs(100.0 * sum(fees_usdc) / abs(sum(gross_pnl_usdc)))
             > 999999.9999
        THEN NULL
        ELSE round(
            100.0 * sum(fees_usdc) / abs(sum(gross_pnl_usdc)),
            4
        )
    END AS fee_pressure_pct
FROM public.positions
WHERE status='CLOSED'
  AND exit_time >= now() - interval '14 days'
  AND market_regime IS NOT NULL
  AND net_pnl_usdc IS NOT NULL
GROUP BY strategy,symbol,"interval",market_regime;

CREATE OR REPLACE VIEW public.v_strategy_regime_30d AS
SELECT
    strategy,
    symbol,
    "interval",
    market_regime,
    count(*) AS trades,
    count(*) FILTER (WHERE net_pnl_usdc > 0) AS wins,
    count(*) FILTER (WHERE net_pnl_usdc <= 0) AS losses,
    round(sum(net_pnl_usdc), 8) AS net_pnl_usdc,
    round(sum(gross_pnl_usdc), 8) AS gross_pnl_usdc,
    round(sum(fees_usdc), 8) AS fees_usdc,
    round(avg(net_pnl_usdc), 8) AS avg_net_usdc,
    round(
        100.0 * count(*) FILTER (WHERE net_pnl_usdc > 0)
        / NULLIF(count(*), 0),
        4
    ) AS win_rate_pct,
    round(
        sum(CASE WHEN net_pnl_usdc > 0 THEN net_pnl_usdc ELSE 0 END)
        / NULLIF(abs(sum(
            CASE WHEN net_pnl_usdc < 0 THEN net_pnl_usdc ELSE 0 END
        )), 0),
        6
    ) AS profit_factor,
    CASE
        WHEN abs(sum(gross_pnl_usdc)) = 0 THEN NULL
        WHEN abs(100.0 * sum(fees_usdc) / abs(sum(gross_pnl_usdc)))
             > 999999.9999
        THEN NULL
        ELSE round(
            100.0 * sum(fees_usdc) / abs(sum(gross_pnl_usdc)),
            4
        )
    END AS fee_pressure_pct
FROM public.positions
WHERE status='CLOSED'
  AND exit_time >= now() - interval '30 days'
  AND market_regime IS NOT NULL
  AND net_pnl_usdc IS NOT NULL
GROUP BY strategy,symbol,"interval",market_regime;

COMMENT ON VIEW public.v_strategy_regime_14d IS
  '14d strategy/regime cohort; fee pressure is NULL when the NUMERIC(10,4) ratio is not meaningful/representable.';
COMMENT ON VIEW public.v_strategy_regime_30d IS
  '30d strategy/regime cohort; fee pressure is NULL when the NUMERIC(10,4) ratio is not meaningful/representable.';

DO $postcondition$
DECLARE
    view_definition TEXT;
BEGIN
    SELECT pg_get_viewdef('public.v_strategy_regime_14d'::regclass, true)
    INTO view_definition;
    IF view_definition NOT LIKE '%999999.9999%' THEN
        RAISE EXCEPTION 'STRATEGY_REGIME_FEE_PRESSURE_14D_GUARD_MISSING';
    END IF;
    SELECT pg_get_viewdef('public.v_strategy_regime_30d'::regclass, true)
    INTO view_definition;
    IF view_definition NOT LIKE '%999999.9999%' THEN
        RAISE EXCEPTION 'STRATEGY_REGIME_FEE_PRESSURE_30D_GUARD_MISSING';
    END IF;
END;
$postcondition$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260803_strategy_regime_fee_pressure_guard_v1.sql',
    '64750738d6f13869764cfff3263e0cb4ae081a896812a69a0038b167abdb023e',
    CASE WHEN current_database() LIKE '%paper%' THEN 'PAPER' ELSE 'LIVE' END,
    'COMMON',current_database(),
    'operator-migration','APPLIED',TRUE,0,
    'bbbf63c1c557fe4316dccc7437120fc608cd38bc',
    'STRATEGY_REGIME_FEE_PRESSURE_GUARD_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id=
          '20260803_strategy_regime_fee_pressure_guard_v1.sql'
);

COMMIT;

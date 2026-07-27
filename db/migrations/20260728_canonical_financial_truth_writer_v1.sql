BEGIN;

ALTER TABLE canonical_financial_truth_v1
    ADD COLUMN IF NOT EXISTS gross_entry_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS gross_exit_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS base_asset_entry_fee_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS base_asset_exit_fee_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS net_entry_inventory_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS net_exit_inventory_reduction_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS gross_remaining_execution_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS remaining_inventory_qty NUMERIC,
    ADD COLUMN IF NOT EXISTS authoritative_entry_notional NUMERIC,
    ADD COLUMN IF NOT EXISTS authoritative_exit_notional NUMERIC,
    ADD COLUMN IF NOT EXISTS authoritative_fees_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS estimated_fees_usdc NUMERIC,
    ADD COLUMN IF NOT EXISTS entry_fill_count INTEGER,
    ADD COLUMN IF NOT EXISTS exit_fill_count INTEGER,
    ADD COLUMN IF NOT EXISTS first_entry_fill_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_entry_fill_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS first_exit_fill_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_exit_fill_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_authority TEXT,
    ADD COLUMN IF NOT EXISTS source_exchange TEXT,
    ADD COLUMN IF NOT EXISTS source_environment TEXT,
    ADD COLUMN IF NOT EXISTS source_deployment_id TEXT,
    ADD COLUMN IF NOT EXISTS source_account_identity_fingerprint TEXT,
    ADD COLUMN IF NOT EXISTS source_order_ids JSONB NOT NULL DEFAULT '[]'::JSONB,
    ADD COLUMN IF NOT EXISTS source_fill_ids JSONB NOT NULL DEFAULT '[]'::JSONB,
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT,
    ADD COLUMN IF NOT EXISTS calculation_version TEXT,
    ADD COLUMN IF NOT EXISTS writer_version TEXT,
    ADD COLUMN IF NOT EXISTS calculated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS failure_code TEXT,
    ADD COLUMN IF NOT EXISTS failure_detail TEXT;

CREATE TABLE IF NOT EXISTS financial_truth_account_identity_v1 (
    id BIGSERIAL PRIMARY KEY,
    source_authority TEXT NOT NULL,
    exchange TEXT NOT NULL,
    account_uid TEXT NOT NULL,
    main_account_uid TEXT NOT NULL,
    account_scope TEXT NOT NULL,
    identity_source TEXT NOT NULL,
    identity_version TEXT NOT NULL,
    identity_fingerprint TEXT NOT NULL UNIQUE,
    captured_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_ft_account_scope_v1
        CHECK (account_scope IN ('MAIN', 'SUB_ACCOUNT', 'SIMULATED')),
    CONSTRAINT ck_ft_account_authority_v1
        CHECK (source_authority IN ('EXCHANGE_EXECUTION', 'SIMULATED_EXECUTION'))
);

CREATE TABLE IF NOT EXISTS financial_truth_instrument_snapshot_v1 (
    id BIGSERIAL PRIMARY KEY,
    source_authority TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    step_size NUMERIC NOT NULL,
    min_qty NUMERIC,
    quantity_precision INTEGER,
    price_precision INTEGER,
    min_notional NUMERIC,
    metadata_source TEXT NOT NULL,
    metadata_version TEXT NOT NULL,
    metadata_fingerprint TEXT NOT NULL UNIQUE,
    captured_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_ft_instrument_step_v1 CHECK (step_size > 0)
);

CREATE TABLE IF NOT EXISTS financial_truth_simulated_account_v1 (
    deployment_id TEXT PRIMARY KEY,
    simulated_account_uid UUID NOT NULL UNIQUE,
    identity_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS simulated_execution_fills_v1 (
    id BIGSERIAL PRIMARY KEY,
    simulated_order_id BIGINT NOT NULL REFERENCES simulated_orders(id) ON DELETE RESTRICT,
    position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE RESTRICT,
    fill_index INTEGER NOT NULL DEFAULT 0,
    order_purpose TEXT NOT NULL,
    side TEXT NOT NULL,
    symbol TEXT NOT NULL,
    fill_qty NUMERIC NOT NULL,
    fill_price NUMERIC NOT NULL,
    fill_notional NUMERIC NOT NULL,
    fee_qty NUMERIC,
    fee_asset TEXT,
    authoritative_fee_usdc NUMERIC,
    estimated_fee_usdc NUMERIC,
    account_identity_id BIGINT
        REFERENCES financial_truth_account_identity_v1(id) ON DELETE RESTRICT,
    instrument_snapshot_id BIGINT
        REFERENCES financial_truth_instrument_snapshot_v1(id) ON DELETE RESTRICT,
    source_authority TEXT NOT NULL DEFAULT 'SIMULATED_EXECUTION',
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    simulation_model_version TEXT NOT NULL,
    execution_at TIMESTAMPTZ NOT NULL,
    source_fingerprint TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ux_simulated_execution_fill_order_idx_v1
        UNIQUE (simulated_order_id, fill_index),
    CONSTRAINT ck_simulated_execution_fill_purpose_v1
        CHECK (order_purpose IN ('ENTRY', 'EXIT')),
    CONSTRAINT ck_simulated_execution_fill_values_v1
        CHECK (fill_qty > 0 AND fill_price >= 0 AND fill_notional >= 0)
);

CREATE INDEX IF NOT EXISTS ix_simulated_execution_fills_position_v1
    ON simulated_execution_fills_v1(position_id, order_purpose, execution_at, id);

CREATE TABLE IF NOT EXISTS canonical_financial_truth_audit_v1 (
    id BIGSERIAL PRIMARY KEY,
    position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE RESTRICT,
    previous_status TEXT,
    new_status TEXT NOT NULL,
    previous_fingerprint TEXT,
    new_fingerprint TEXT NOT NULL,
    previous_values JSONB NOT NULL DEFAULT '{}'::JSONB,
    new_values JSONB NOT NULL,
    reason TEXT NOT NULL,
    writer_version TEXT NOT NULL,
    invocation_type TEXT NOT NULL,
    invocation_identity TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ux_canonical_ft_audit_transition_v1
        UNIQUE (position_id, new_fingerprint)
);

DO $$
BEGIN
    IF to_regclass('public.binance_order_fills') IS NOT NULL THEN
        ALTER TABLE binance_order_fills
            ADD COLUMN IF NOT EXISTS account_identity_id BIGINT
                REFERENCES financial_truth_account_identity_v1(id) ON DELETE RESTRICT,
            ADD COLUMN IF NOT EXISTS instrument_snapshot_id BIGINT
                REFERENCES financial_truth_instrument_snapshot_v1(id) ON DELETE RESTRICT,
            ADD COLUMN IF NOT EXISTS account_identity_status TEXT,
            ADD COLUMN IF NOT EXISTS account_identity_failure_code TEXT;
    END IF;
    IF to_regclass('public.binance_orders') IS NOT NULL THEN
        ALTER TABLE binance_orders
            ADD COLUMN IF NOT EXISTS account_identity_id BIGINT
                REFERENCES financial_truth_account_identity_v1(id) ON DELETE RESTRICT,
            ADD COLUMN IF NOT EXISTS instrument_snapshot_id BIGINT
                REFERENCES financial_truth_instrument_snapshot_v1(id) ON DELETE RESTRICT,
            ADD COLUMN IF NOT EXISTS account_identity_status TEXT,
            ADD COLUMN IF NOT EXISTS account_identity_failure_code TEXT;
    END IF;
END $$;

CREATE OR REPLACE VIEW v_canonical_financial_truth_v1 AS
SELECT
    p.id AS position_id,
    p.status AS position_status,
    COALESCE(ft.financial_truth_status, 'UNKNOWN') AS financial_truth_status,
    ft.executed_entry_qty,
    ft.executed_exit_qty,
    ft.remaining_qty,
    ft.authoritative_entry_fees_usdc,
    ft.authoritative_exit_fees_usdc,
    CASE
        WHEN ft.authoritative_entry_fees_usdc IS NULL
          OR ft.authoritative_exit_fees_usdc IS NULL
        THEN NULL
        ELSE ft.authoritative_entry_fees_usdc + ft.authoritative_exit_fees_usdc
    END AS authoritative_total_fees_usdc,
    ft.authoritative_gross_pnl,
    ft.authoritative_net_pnl,
    ft.estimated_gross_pnl,
    ft.estimated_net_pnl,
    ft.authoritative_source,
    COALESCE(ft.authoritative_evidence, '{}'::JSONB) AS authoritative_evidence,
    ft.failure_reason,
    COALESCE(ft.schema_version, 'FINANCIAL_TRUTH_V1') AS schema_version,
    ft.evidence_observed_at,
    ft.created_at,
    ft.updated_at,
    ft.gross_entry_qty,
    ft.gross_exit_qty,
    ft.net_entry_inventory_qty,
    ft.net_exit_inventory_reduction_qty,
    ft.gross_remaining_execution_qty,
    ft.remaining_inventory_qty,
    ft.authoritative_entry_notional,
    ft.authoritative_exit_notional,
    ft.authoritative_fees_usdc,
    ft.estimated_fees_usdc,
    ft.source_authority,
    ft.source_exchange,
    ft.source_environment,
    ft.source_deployment_id,
    ft.source_account_identity_fingerprint,
    ft.source_fingerprint,
    ft.calculation_version,
    ft.writer_version,
    ft.calculated_at,
    ft.completed_at,
    ft.failure_code,
    ft.failure_detail
FROM positions p
LEFT JOIN canonical_financial_truth_v1 ft ON ft.position_id=p.id;

COMMENT ON TABLE canonical_financial_truth_audit_v1 IS
'Append-only semantic transition audit owned only by FINANCIAL_TRUTH_RECONCILER_V1.';
COMMENT ON TABLE simulated_execution_fills_v1 IS
'Fill-level authoritative evidence for new deterministic PAPER executions only; no backfill.';
COMMENT ON COLUMN canonical_financial_truth_v1.executed_entry_qty IS
'Backward-compatible gross authoritative entry fill quantity.';
COMMENT ON COLUMN canonical_financial_truth_v1.executed_exit_qty IS
'Backward-compatible gross authoritative exit fill quantity.';
COMMENT ON COLUMN canonical_financial_truth_v1.remaining_qty IS
'Backward-compatible canonical economic remaining inventory; mirrors remaining_inventory_qty.';

COMMIT;

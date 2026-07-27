BEGIN;

CREATE TABLE IF NOT EXISTS canonical_financial_truth_v1 (
    position_id BIGINT PRIMARY KEY
        REFERENCES positions(id) ON DELETE RESTRICT,
    financial_truth_status TEXT NOT NULL DEFAULT 'UNKNOWN',

    executed_entry_qty NUMERIC,
    executed_exit_qty NUMERIC,
    remaining_qty NUMERIC,

    authoritative_entry_fees_usdc NUMERIC,
    authoritative_exit_fees_usdc NUMERIC,
    authoritative_gross_pnl NUMERIC,
    authoritative_net_pnl NUMERIC,

    estimated_gross_pnl NUMERIC,
    estimated_net_pnl NUMERIC,

    authoritative_source TEXT,
    authoritative_evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
    failure_reason TEXT,
    schema_version TEXT NOT NULL DEFAULT 'FINANCIAL_TRUTH_V1',
    evidence_observed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT ck_canonical_financial_truth_status_v1
        CHECK (financial_truth_status IN (
            'UNKNOWN', 'INCOMPLETE', 'COMPLETE', 'FAILED'
        )),
    CONSTRAINT ck_canonical_financial_truth_quantities_v1
        CHECK (
            (executed_entry_qty IS NULL OR executed_entry_qty >= 0)
            AND (executed_exit_qty IS NULL OR executed_exit_qty >= 0)
            AND (remaining_qty IS NULL OR remaining_qty >= 0)
        ),
    CONSTRAINT ck_canonical_financial_truth_complete_v1
        CHECK (
            financial_truth_status <> 'COMPLETE'
            OR (
                executed_entry_qty IS NOT NULL
                AND executed_exit_qty IS NOT NULL
                AND remaining_qty IS NOT NULL
                AND authoritative_entry_fees_usdc IS NOT NULL
                AND authoritative_exit_fees_usdc IS NOT NULL
                AND authoritative_gross_pnl IS NOT NULL
                AND authoritative_net_pnl IS NOT NULL
                AND NULLIF(btrim(authoritative_source), '') IS NOT NULL
                AND authoritative_evidence <> '{}'::JSONB
                AND evidence_observed_at IS NOT NULL
            )
        ),
    CONSTRAINT ck_canonical_financial_truth_failed_v1
        CHECK (
            financial_truth_status <> 'FAILED'
            OR NULLIF(btrim(failure_reason), '') IS NOT NULL
        )
);

CREATE INDEX IF NOT EXISTS ix_canonical_financial_truth_status_v1
ON canonical_financial_truth_v1(financial_truth_status, updated_at DESC);

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
        ELSE ft.authoritative_entry_fees_usdc
           + ft.authoritative_exit_fees_usdc
    END AS authoritative_total_fees_usdc,
    ft.authoritative_gross_pnl,
    ft.authoritative_net_pnl,
    ft.estimated_gross_pnl,
    ft.estimated_net_pnl,
    ft.authoritative_source,
    COALESCE(ft.authoritative_evidence, '{}'::JSONB)
        AS authoritative_evidence,
    ft.failure_reason,
    COALESCE(ft.schema_version, 'FINANCIAL_TRUTH_V1') AS schema_version,
    ft.evidence_observed_at,
    ft.created_at,
    ft.updated_at
FROM positions p
LEFT JOIN canonical_financial_truth_v1 ft
  ON ft.position_id = p.id;

COMMENT ON TABLE canonical_financial_truth_v1 IS
'Patch C1 foundation. Sole future writer: FINANCIAL_TRUTH_RECONCILER. '
'No C1 runtime writer or historical backfill exists.';

COMMENT ON COLUMN canonical_financial_truth_v1.authoritative_gross_pnl IS
'Authoritative fill-derived value. Must never be replaced by an estimate.';

COMMENT ON COLUMN canonical_financial_truth_v1.authoritative_net_pnl IS
'Authoritative fill-and-fee-derived value. Must never be replaced by an estimate.';

COMMENT ON COLUMN canonical_financial_truth_v1.estimated_gross_pnl IS
'Non-authoritative estimate, intentionally separate from authoritative_gross_pnl.';

COMMENT ON COLUMN canonical_financial_truth_v1.estimated_net_pnl IS
'Non-authoritative estimate, intentionally separate from authoritative_net_pnl.';

COMMENT ON VIEW v_canonical_financial_truth_v1 IS
'Read-only separation of Position Lifecycle and Financial Truth Lifecycle. '
'A missing canonical row is explicitly UNKNOWN with null authoritative values.';

COMMIT;

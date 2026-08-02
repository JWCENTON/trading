-- WALTRADE LEGACY FILL EQUIVALENCE PROOF V1
-- Additive, append-only evidence that a legacy pending correction, its
-- canonical local fill, and freshly fetched OKX truth are semantically equal.
-- This migration performs no backfill and does not activate a writer.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $dependencies$
DECLARE
    required_relation TEXT;
    conflicting_checksum TEXT;
BEGIN
    FOREACH required_relation IN ARRAY ARRAY[
        'schema_migration_ledger_v1',
        'exchange_fill_ingestion_state_v2',
        'binance_order_fills',
        'positions'
    ] LOOP
        IF to_regclass('public.' || required_relation) IS NULL THEN
            RAISE EXCEPTION
                'LEGACY_FILL_EQUIVALENCE_REQUIRED_RELATION_MISSING:%',
                required_relation;
        END IF;
    END LOOP;

    SELECT checksum_sha256 INTO conflicting_checksum
    FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260802_legacy_fill_equivalence_proof_v1.sql'
      AND checksum_sha256<>
          'a5b9ed1882338a4e90db41703e13b1709510e2f7be62210e4cb89099c7636135'
    ORDER BY applied_at DESC LIMIT 1;
    IF conflicting_checksum IS NOT NULL THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_LEDGER_CHECKSUM_CONFLICT:%',
            conflicting_checksum;
    END IF;

    IF EXISTS (
        SELECT required.column_name
        FROM (VALUES
          ('ingestion_id'),('source'),('account_identity_key'),('symbol'),
          ('trade_id'),('order_id'),('source_fingerprint'),
          ('applied_fingerprint'),('applied_at'),('application_status'),
          ('correction_revision'),('authoritative_payload'),('adoption_id'),
          ('contract_generation'),('local_fill_id')
        ) AS required(column_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns actual
            WHERE actual.table_schema='public'
              AND actual.table_name='exchange_fill_ingestion_state_v2'
              AND actual.column_name=required.column_name
        )
    ) THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_INGESTION_CONTRACT_INCOMPATIBLE';
    END IF;

    IF EXISTS (
        SELECT required.column_name
        FROM (VALUES
          ('id'),('source'),('order_id'),('symbol'),('side'),('executed_qty'),
          ('avg_price'),('quote_notional_usdc'),('commission_amount'),
          ('commission_asset'),('event_time'),('trade_id'),
          ('account_identity_id')
        ) AS required(column_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns actual
            WHERE actual.table_schema='public'
              AND actual.table_name='binance_order_fills'
              AND actual.column_name=required.column_name
        )
    ) THEN
        RAISE EXCEPTION
            'LEGACY_FILL_EQUIVALENCE_CANONICAL_FILL_CONTRACT_INCOMPATIBLE';
    END IF;
END;
$dependencies$;

CREATE TABLE IF NOT EXISTS public.legacy_fill_equivalence_proof_v1 (
    proof_id BIGSERIAL PRIMARY KEY,
    proof_version TEXT NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    source TEXT NOT NULL,
    account_identity_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    ingestion_id BIGINT NOT NULL,
    correction_revision INTEGER NOT NULL,
    exchange_order_id TEXT NOT NULL,
    exchange_trade_id TEXT NOT NULL,
    canonical_local_fill_id BIGINT NOT NULL,
    latest_observed_fingerprint TEXT NOT NULL,
    canonical_fill_fingerprint TEXT NOT NULL,
    okx_truth_fingerprint TEXT NOT NULL,
    proof_type TEXT NOT NULL,
    equivalence_state TEXT NOT NULL,
    fill_mutation_required BOOLEAN NOT NULL,
    repair_impact TEXT NOT NULL,
    position_id BIGINT NOT NULL,
    entry_or_exit TEXT NOT NULL,
    evidence_payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_by TEXT NOT NULL,
    git_revision TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    CONSTRAINT fk_legacy_fill_equivalence_ingestion_v1
        FOREIGN KEY(ingestion_id)
        REFERENCES public.exchange_fill_ingestion_state_v2(ingestion_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_legacy_fill_equivalence_canonical_fill_v1
        FOREIGN KEY(canonical_local_fill_id)
        REFERENCES public.binance_order_fills(id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_legacy_fill_equivalence_position_v1
        FOREIGN KEY(position_id) REFERENCES public.positions(id)
        ON DELETE RESTRICT,
    CONSTRAINT ck_legacy_fill_equivalence_contract_v1 CHECK (
        proof_version='LEGACY_FILL_EQUIVALENCE_PROOF_V1'
        AND environment='LIVE'
        AND deployment_id='local-live'
        AND source='okx'
        AND btrim(account_identity_key)<>''
        AND symbol=upper(symbol) AND btrim(symbol)<>''
        AND btrim(trade_id)<>''
        AND correction_revision>0
        AND exchange_trade_id=trade_id
        AND proof_type='LEGACY_CANONICAL_OKX_EQUIVALENCE'
        AND equivalence_state='PROVEN'
        AND fill_mutation_required=FALSE
        AND repair_impact='NONE'
        AND entry_or_exit IN ('ENTRY','EXIT')
        AND jsonb_typeof(evidence_payload_json)='object'
        AND btrim(created_by)<>''
        AND git_revision~'^[0-9a-f]{40}$'
        AND latest_observed_fingerprint~'^[0-9a-f]{64}$'
        AND canonical_fill_fingerprint~'^[0-9a-f]{64}$'
        AND okx_truth_fingerprint~'^[0-9a-f]{64}$'
        AND idempotency_key~'^[0-9a-f]{64}$'
    ),
    CONSTRAINT ux_legacy_fill_equivalence_idempotency_v1 UNIQUE (
        environment,deployment_id,source,account_identity_key,symbol,trade_id,
        correction_revision,latest_observed_fingerprint,
        canonical_fill_fingerprint,okx_truth_fingerprint,proof_version
    ),
    CONSTRAINT ux_legacy_fill_equivalence_identity_v1 UNIQUE (
        environment,deployment_id,source,account_identity_key,symbol,trade_id,
        correction_revision,proof_version
    ),
    CONSTRAINT ux_legacy_fill_equivalence_idempotency_key_v1
        UNIQUE(idempotency_key)
);

CREATE INDEX IF NOT EXISTS ix_legacy_fill_equivalence_position_v1
    ON public.legacy_fill_equivalence_proof_v1(position_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_legacy_fill_equivalence_ingestion_v1
    ON public.legacy_fill_equivalence_proof_v1(ingestion_id,created_at DESC);

CREATE OR REPLACE FUNCTION public.prevent_legacy_fill_equivalence_mutation_v1()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$function$;

DO $append_only_triggers$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname='trg_legacy_fill_equivalence_row_append_only_v1'
          AND tgrelid='public.legacy_fill_equivalence_proof_v1'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_legacy_fill_equivalence_row_append_only_v1
        BEFORE UPDATE OR DELETE
        ON public.legacy_fill_equivalence_proof_v1
        FOR EACH ROW
        EXECUTE FUNCTION public.prevent_legacy_fill_equivalence_mutation_v1();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname='trg_legacy_fill_equivalence_truncate_v1'
          AND tgrelid='public.legacy_fill_equivalence_proof_v1'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_legacy_fill_equivalence_truncate_v1
        BEFORE TRUNCATE
        ON public.legacy_fill_equivalence_proof_v1
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.prevent_legacy_fill_equivalence_mutation_v1();
    END IF;
END;
$append_only_triggers$;

CREATE OR REPLACE VIEW public.v_legacy_fill_equivalence_proof_status_v1 AS
SELECT
    proof.*,
    ingestion.correction_revision AS current_correction_revision,
    ingestion.source_fingerprint AS current_observed_fingerprint,
    ingestion.application_status AS current_application_status,
    fill.id AS current_canonical_fill_id,
    CASE
      WHEN ingestion.ingestion_id IS NULL THEN 'IDENTITY_CONFLICT'
      WHEN fill.id IS NULL THEN 'MISSING_CANONICAL_FILL'
      WHEN matching_fill.match_count<>1
        OR ingestion.source IS DISTINCT FROM proof.source
        OR ingestion.account_identity_key IS DISTINCT FROM proof.account_identity_key
        OR ingestion.symbol IS DISTINCT FROM proof.symbol
        OR ingestion.trade_id IS DISTINCT FROM proof.trade_id
        OR ingestion.order_id IS DISTINCT FROM proof.exchange_order_id
        OR fill.source IS DISTINCT FROM proof.source
        OR fill.symbol IS DISTINCT FROM proof.symbol
        OR fill.trade_id::TEXT IS DISTINCT FROM proof.exchange_trade_id
        OR fill.order_id IS DISTINCT FROM proof.exchange_order_id
        OR (
          fill.account_identity_id IS NOT NULL
          AND fill.account_identity_id::TEXT<>proof.account_identity_key
        )
        OR (
          proof.entry_or_exit='ENTRY'
          AND position.entry_order_id IS DISTINCT FROM proof.exchange_order_id
        )
        OR (
          proof.entry_or_exit='EXIT'
          AND position.exit_order_id IS DISTINCT FROM proof.exchange_order_id
        ) THEN 'IDENTITY_CONFLICT'
      WHEN ingestion.correction_revision
             IS DISTINCT FROM proof.correction_revision
        THEN 'STALE_INGESTION_REVISION'
      WHEN ingestion.source_fingerprint
             IS DISTINCT FROM proof.latest_observed_fingerprint
        THEN 'STALE_OBSERVED_FINGERPRINT'
      WHEN fill.side IS DISTINCT FROM
             proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,side}'
        OR fill.executed_qty IS DISTINCT FROM
             (proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,quantity}')::NUMERIC
        OR fill.avg_price IS DISTINCT FROM
             (proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,price}')::NUMERIC
        OR fill.quote_notional_usdc IS DISTINCT FROM
             (proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,quote_quantity}')::NUMERIC
        OR fill.commission_amount IS DISTINCT FROM
             (proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,fee_quantity}')::NUMERIC
        OR fill.commission_asset IS DISTINCT FROM
             proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,fee_currency}'
        OR (extract(epoch FROM fill.event_time)*1000)::BIGINT IS DISTINCT FROM
             (proof.evidence_payload_json#>>'{canonical_fill,semantic_payload,event_time_ms}')::BIGINT
        THEN 'STALE_CANONICAL_FILL'
      ELSE 'VALID'
    END AS proof_status
FROM public.legacy_fill_equivalence_proof_v1 proof
LEFT JOIN public.exchange_fill_ingestion_state_v2 ingestion
  ON ingestion.ingestion_id=proof.ingestion_id
LEFT JOIN public.binance_order_fills fill
  ON fill.id=proof.canonical_local_fill_id
LEFT JOIN public.positions position
  ON position.id=proof.position_id
LEFT JOIN LATERAL (
  SELECT count(*)::INTEGER AS match_count
  FROM public.binance_order_fills candidate
  WHERE candidate.source=proof.source
    AND candidate.order_id=proof.exchange_order_id
    AND candidate.trade_id::TEXT=proof.exchange_trade_id
) matching_fill ON TRUE;

COMMENT ON TABLE public.legacy_fill_equivalence_proof_v1 IS
    'Append-only proof of legacy correction/canonical fill/current OKX semantic equivalence; never application proof.';
COMMENT ON VIEW public.v_legacy_fill_equivalence_proof_status_v1 IS
    'Current DB-side validity of immutable equivalence proofs; OKX freshness must be rechecked externally.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260802_legacy_fill_equivalence_proof_v1.sql',
    'a5b9ed1882338a4e90db41703e13b1709510e2f7be62210e4cb89099c7636135',
    'LIVE','local-live',current_database(),'operator-migration','APPLIED',TRUE,0,
    '747bcbe803db44b6b99c4a04b6cdf6f0854e909f',
    'LEGACY_FILL_EQUIVALENCE_PROOF_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260802_legacy_fill_equivalence_proof_v1.sql'
);

COMMIT;

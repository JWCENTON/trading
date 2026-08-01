-- WALTRADE LEI1D: idempotent immutable-fill to position projection.
-- Additive only. No backfill, activation, historical repair, or runtime DDL.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
  IF to_regclass('public.live_entry_intents_v1') IS NULL
     OR to_regclass('public.live_entry_fill_evidence_v1') IS NULL
     OR to_regclass('public.live_entry_fill_applications_v1') IS NULL
     OR to_regclass('public.position_lifecycle_events_c2_2') IS NULL
     OR to_regclass('public.positions') IS NULL THEN
    RAISE EXCEPTION 'LEI1D_PREREQUISITE_SCHEMA_MISSING';
  END IF;
  IF EXISTS (
    SELECT required.column_name
    FROM (VALUES
      ('gross_entry_executed_qty'),('entry_base_fee_qty'),
      ('net_entry_inventory_qty'),('cumulative_exit_executed_qty'),
      ('exit_inventory_reduction_qty'),('remaining_inventory_qty'),
      ('inventory_evidence_status'),('inventory_contract_adoption_id'),
      ('inventory_contract_generation')
    ) required(column_name)
    WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns existing
      WHERE existing.table_schema='public'
        AND existing.table_name='positions'
        AND existing.column_name=required.column_name
    )
  ) THEN
    RAISE EXCEPTION 'LEI1D_PREREQUISITE_C2_2_POSITION_SCHEMA_MISSING';
  END IF;
END $$;

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS entry_intent_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='positions_entry_intent_id_fkey'
      AND conrelid='positions'::regclass
  ) THEN
    ALTER TABLE positions
      ADD CONSTRAINT positions_entry_intent_id_fkey
      FOREIGN KEY (entry_intent_id)
      REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_entry_intent_lei1d
  ON positions(entry_intent_id) WHERE entry_intent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS live_entry_position_projections_v1 (
  projection_id BIGSERIAL PRIMARY KEY,
  intent_id UUID NOT NULL UNIQUE
    REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  position_id BIGINT NOT NULL UNIQUE
    REFERENCES positions(id) ON DELETE RESTRICT,
  environment TEXT NOT NULL CHECK (environment='live'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','vps-live')),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  exchange_source TEXT NOT NULL CHECK (exchange_source=lower(exchange_source)),
  exchange_order_id TEXT NOT NULL CHECK (btrim(exchange_order_id) <> ''),
  client_order_id TEXT NOT NULL CHECK (btrim(client_order_id) <> ''),
  submission_attempt_id UUID NOT NULL
    REFERENCES live_entry_submissions_v1(submission_attempt_id) ON DELETE RESTRICT,
  ack_id UUID NOT NULL
    REFERENCES live_entry_order_acks_v1(ack_id) ON DELETE RESTRICT,
  symbol TEXT NOT NULL CHECK (symbol=upper(symbol)),
  strategy TEXT NOT NULL CHECK (strategy=upper(strategy)),
  "interval" TEXT NOT NULL CHECK ("interval"=lower("interval")),
  projected_fill_count BIGINT NOT NULL DEFAULT 0 CHECK (projected_fill_count >= 0),
  projected_gross_entry_qty NUMERIC NOT NULL DEFAULT 0
    CHECK (projected_gross_entry_qty >= 0),
  projected_entry_base_fee_qty NUMERIC NOT NULL DEFAULT 0
    CHECK (projected_entry_base_fee_qty >= 0),
  projected_net_entry_qty NUMERIC NOT NULL DEFAULT 0
    CHECK (projected_net_entry_qty >= 0),
  projected_entry_notional NUMERIC NOT NULL DEFAULT 0
    CHECK (projected_entry_notional >= 0),
  projection_fingerprint TEXT NOT NULL CHECK (
    projection_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  first_projected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  contract_version TEXT NOT NULL CHECK (
    contract_version='LIVE_ENTRY_POSITION_PROJECTION_V1'
  ),
  CONSTRAINT ux_live_entry_projection_exchange_order_v1 UNIQUE (
    environment,deployment_id,exchange_source,exchange_order_id
  ),
  CONSTRAINT ux_live_entry_projection_client_order_v1 UNIQUE (
    environment,deployment_id,exchange_source,client_order_id
  ),
  CONSTRAINT ck_live_entry_projection_inventory_v1 CHECK (
    projected_net_entry_qty =
      projected_gross_entry_qty - projected_entry_base_fee_qty
  )
);

CREATE TABLE IF NOT EXISTS live_entry_position_projection_fills_v1 (
  projection_fill_id BIGSERIAL PRIMARY KEY,
  projection_id BIGINT NOT NULL
    REFERENCES live_entry_position_projections_v1(projection_id) ON DELETE RESTRICT,
  intent_id UUID NOT NULL
    REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE RESTRICT,
  fill_evidence_id UUID NOT NULL UNIQUE
    REFERENCES live_entry_fill_evidence_v1(fill_evidence_id) ON DELETE RESTRICT,
  local_fill_id BIGINT NOT NULL UNIQUE
    REFERENCES binance_order_fills(id) ON DELETE RESTRICT,
  source_fingerprint TEXT NOT NULL CHECK (source_fingerprint ~ '^[0-9a-f]{64}$'),
  executed_qty NUMERIC NOT NULL CHECK (executed_qty > 0),
  entry_base_fee_qty NUMERIC NOT NULL CHECK (entry_base_fee_qty >= 0),
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  contract_version TEXT NOT NULL CHECK (
    contract_version='LIVE_ENTRY_POSITION_PROJECTION_FILL_V1'
  ),
  UNIQUE (intent_id,fill_evidence_id),
  UNIQUE (position_id,fill_evidence_id)
);

CREATE TABLE IF NOT EXISTS live_entry_position_projection_diagnostics_v1 (
  diagnostic_id UUID PRIMARY KEY,
  intent_id UUID REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  fill_evidence_id UUID
    REFERENCES live_entry_fill_evidence_v1(fill_evidence_id) ON DELETE RESTRICT,
  classification TEXT NOT NULL CHECK (classification IN (
    'OBSERVATION_ONLY','EXTERNAL_OR_MANUAL','CORRECTION_OR_CONFLICT',
    'INCOMPLETE_ATTRIBUTION','LEGACY_OPEN_POSITION_CONFLICT',
    'POSITION_IDENTITY_CONFLICT','INCOMPLETE_FEE_EVIDENCE'
  )),
  detail TEXT NOT NULL CHECK (btrim(detail) <> ''),
  evidence JSONB NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  contract_version TEXT NOT NULL CHECK (
    contract_version='LIVE_ENTRY_POSITION_PROJECTION_DIAGNOSTIC_V1'
  )
);

CREATE INDEX IF NOT EXISTS ix_live_entry_projection_pending_v1
  ON live_entry_position_projections_v1(updated_at,intent_id);
CREATE INDEX IF NOT EXISTS ix_live_entry_projection_fill_intent_v1
  ON live_entry_position_projection_fills_v1(intent_id,applied_at);
CREATE INDEX IF NOT EXISTS ix_live_entry_projection_diagnostic_intent_v1
  ON live_entry_position_projection_diagnostics_v1(intent_id,recorded_at DESC);

DO $$
DECLARE
  constraint_definition TEXT;
BEGIN
  SELECT pg_get_constraintdef(oid) INTO constraint_definition
  FROM pg_constraint
  WHERE conrelid='position_lifecycle_events_c2_2'::regclass
    AND conname='position_lifecycle_events_c2_2_mutation_kind_check';
  IF constraint_definition IS NOT NULL
     AND position('POSITION_OPENED' IN constraint_definition)=0 THEN
    ALTER TABLE position_lifecycle_events_c2_2
      DROP CONSTRAINT position_lifecycle_events_c2_2_mutation_kind_check;
    ALTER TABLE position_lifecycle_events_c2_2
      ADD CONSTRAINT position_lifecycle_events_c2_2_mutation_kind_check CHECK (
        mutation_kind IN (
          'POSITION_OPENED','POSITION_REDUCED','POSITION_CLOSED',
          'POSITION_CLOSED_TERMINAL_DUST'
        )
      );
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_position_opened_once_lei1d
  ON position_lifecycle_events_c2_2(position_id,mutation_kind)
  WHERE mutation_kind='POSITION_OPENED';

COMMENT ON COLUMN positions.entry_intent_id IS
  'LEI1D immutable position identity; never inferred from slot or time proximity.';
COMMENT ON TABLE live_entry_position_projections_v1 IS
  'LEI1D durable per-intent projection high-water and immutable position linkage.';
COMMENT ON TABLE live_entry_position_projection_fills_v1 IS
  'LEI1D exact immutable fill-to-position projection provenance.';
COMMENT ON TABLE live_entry_position_projection_diagnostics_v1 IS
  'LEI1D fail-closed diagnostic evidence; never authorizes position mutation.';

COMMIT;

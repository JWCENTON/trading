-- WALTRADE LEI1C: immutable authoritative ENTRY fill evidence and
-- append-only application decisions. Additive only: no backfill, no writer
-- activation, and no mutation of orders, fills, positions, or earlier ledgers.
-- Current readiness is LIVE-only. PAPER remains OFF/schema-unchanged until a
-- simulated-fill and lineage adapter has its own reviewed contract.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
DECLARE
  expected_checksum CONSTANT text :=
    'ad72d70d21d440de1d65c3499a1de1e95b6a27af0721c4c3c9c71f150168541d';
  found_checksum text;
BEGIN
  IF to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_SCHEMA_MIGRATION_LEDGER_V1_MISSING';
  END IF;
  SELECT checksum_sha256 INTO found_checksum
  FROM schema_migration_ledger_v1
  WHERE migration_id='20260731_live_entry_fill_attribution_v1.sql'
  ORDER BY applied_at DESC
  LIMIT 1;
  IF found_checksum IS NOT NULL AND found_checksum <> expected_checksum THEN
    RAISE EXCEPTION 'LEI1C_MIGRATION_CHECKSUM_CONFLICT';
  END IF;
  IF to_regclass('public.live_entry_intents_v1') IS NULL
     OR to_regclass('public.live_entry_submissions_v1') IS NULL
     OR to_regclass('public.live_entry_order_acks_v1') IS NULL THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_LEI1A_LEI1B_MISSING';
  END IF;
  IF to_regclass('public.runtime_contract_adoption_v2') IS NULL THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_ADOPTION_V2_MISSING';
  END IF;
  IF lower(current_database()) LIKE '%paper%'
     OR (
       EXISTS (
         SELECT 1 FROM runtime_contract_adoption_v2
         WHERE status='ACTIVE' AND environment='paper'
       )
       AND NOT EXISTS (
         SELECT 1 FROM runtime_contract_adoption_v2
         WHERE status='ACTIVE' AND environment='live'
       )
     ) THEN
    RAISE EXCEPTION 'LEI1C_RUNTIME_SCOPE_LIVE_ONLY';
  END IF;
  IF to_regclass('public.binance_orders') IS NULL
     OR to_regclass('public.binance_order_fills') IS NULL
     OR to_regclass('public.positions') IS NULL THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_LOCAL_PROJECTION_TABLES_MISSING';
  END IF;
  IF to_regclass('public.exchange_fill_ingestion_state_v2') IS NULL THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_INGESTION_STATE_V2_MISSING';
  END IF;
  IF EXISTS (
    SELECT required.column_name
    FROM (VALUES
      ('source'),('symbol'),('trade_id'),('order_id'),('local_fill_id'),
      ('adoption_id'),('contract_generation'),('source_fingerprint'),
      ('applied_fingerprint'),('applied_at'),('application_status'),
      ('authoritative_payload')
    ) AS required(column_name)
    WHERE NOT EXISTS (
      SELECT 1
      FROM information_schema.columns existing
      WHERE existing.table_schema='public'
        AND existing.table_name='exchange_fill_ingestion_state_v2'
        AND existing.column_name=required.column_name
    )
  ) THEN
    RAISE EXCEPTION 'LEI1C_PREREQUISITE_INGESTION_STATE_V2_INCOMPATIBLE';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS live_entry_fill_evidence_v1 (
  fill_evidence_id UUID PRIMARY KEY,
  environment TEXT NOT NULL CHECK (environment IN ('paper','live')),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','local-live','vps-paper','vps-live'
  )),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  exchange_source TEXT NOT NULL CHECK (
    btrim(exchange_source) <> ''
    AND exchange_source = btrim(exchange_source)
    AND exchange_source = lower(exchange_source)
  ),
  exchange_trade_id TEXT NOT NULL CHECK (
    btrim(exchange_trade_id) <> ''
    AND exchange_trade_id = btrim(exchange_trade_id)
  ),
  exchange_order_id TEXT NOT NULL CHECK (
    btrim(exchange_order_id) <> ''
    AND exchange_order_id = btrim(exchange_order_id)
  ),
  client_order_id TEXT CHECK (
    client_order_id IS NULL
    OR (btrim(client_order_id) <> '' AND client_order_id = btrim(client_order_id))
  ),
  wire_client_order_id TEXT CHECK (
    wire_client_order_id IS NULL
    OR (
      btrim(wire_client_order_id) <> ''
      AND wire_client_order_id = btrim(wire_client_order_id)
    )
  ),
  intent_id UUID REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  submission_attempt_id UUID
    REFERENCES live_entry_submissions_v1(submission_attempt_id)
    ON DELETE RESTRICT,
  ack_id UUID REFERENCES live_entry_order_acks_v1(ack_id) ON DELETE RESTRICT,
  linked_position_id BIGINT REFERENCES positions(id) ON DELETE RESTRICT,
  attribution_status TEXT NOT NULL CHECK (attribution_status IN (
    'BOT_OWNED_ATTRIBUTED',
    'BOT_OWNED_MISSING_POSITION',
    'BOT_OWNED_MISSING_LINEAGE',
    'LEGACY_BOT_OWNED',
    'EXTERNAL_OR_MANUAL_UNLINKED',
    'AMBIGUOUS',
    'CONFLICTED',
    'UNKNOWN'
  )),
  attribution_fingerprint TEXT NOT NULL CHECK (
    attribution_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  symbol TEXT NOT NULL CHECK (
    btrim(symbol) <> '' AND symbol = btrim(symbol) AND symbol = upper(symbol)
  ),
  strategy TEXT CHECK (
    strategy IS NULL
    OR (btrim(strategy) <> '' AND strategy = btrim(strategy)
        AND strategy = upper(strategy))
  ),
  "interval" TEXT CHECK (
    "interval" IS NULL
    OR (btrim("interval") <> '' AND "interval" = btrim("interval")
        AND "interval" = lower("interval"))
  ),
  order_purpose TEXT CHECK (
    order_purpose IS NULL OR order_purpose = 'ENTRY'
  ),
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  executed_qty NUMERIC NOT NULL CHECK (executed_qty > 0),
  price NUMERIC NOT NULL CHECK (price > 0),
  notional NUMERIC NOT NULL CHECK (notional > 0),
  fee NUMERIC NOT NULL CHECK (fee >= 0),
  fee_asset TEXT CHECK (
    fee_asset IS NULL
    OR (btrim(fee_asset) <> '' AND fee_asset = btrim(fee_asset)
        AND fee_asset = upper(fee_asset))
  ),
  executed_at TIMESTAMPTZ NOT NULL,
  source_fingerprint TEXT NOT NULL CHECK (
    source_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  source_payload JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  producer_identity TEXT NOT NULL CHECK (
    btrim(producer_identity) <> ''
    AND producer_identity = btrim(producer_identity)
  ),
  contract_version TEXT NOT NULL CHECK (
    contract_version = 'LIVE_ENTRY_FILL_EVIDENCE_V1'
  ),
  CONSTRAINT ck_live_entry_fill_environment_deployment_v1 CHECK (
    (environment='paper' AND deployment_id IN ('local-paper','vps-paper'))
    OR
    (environment='live' AND deployment_id IN ('local-live','vps-live'))
  ),
  CONSTRAINT ck_live_entry_fill_strategy_interval_pair_v1 CHECK (
    (strategy IS NULL) = ("interval" IS NULL)
  ),
  CONSTRAINT ck_live_entry_fill_lineage_shape_v1 CHECK (
    (submission_attempt_id IS NULL OR intent_id IS NOT NULL)
    AND (ack_id IS NULL OR submission_attempt_id IS NOT NULL)
  ),
  CONSTRAINT ck_live_entry_fill_initial_attribution_v1 CHECK (
    CASE attribution_status
      WHEN 'BOT_OWNED_ATTRIBUTED' THEN
        intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND linked_position_id IS NOT NULL
      WHEN 'BOT_OWNED_MISSING_POSITION' THEN
        intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND linked_position_id IS NULL
      WHEN 'EXTERNAL_OR_MANUAL_UNLINKED' THEN
        intent_id IS NULL
        AND submission_attempt_id IS NULL
        AND ack_id IS NULL
        AND linked_position_id IS NULL
      ELSE true
    END
  ),
  CONSTRAINT ux_live_entry_fill_source_identity_v1 UNIQUE (
    environment,deployment_id,exchange_source,exchange_trade_id
  )
);

CREATE TABLE IF NOT EXISTS live_entry_fill_applications_v1 (
  application_decision_id UUID PRIMARY KEY,
  fill_evidence_id UUID NOT NULL
    REFERENCES live_entry_fill_evidence_v1(fill_evidence_id)
    ON DELETE RESTRICT,
  environment TEXT NOT NULL CHECK (environment IN ('paper','live')),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','local-live','vps-paper','vps-live'
  )),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  exchange_source TEXT NOT NULL CHECK (
    btrim(exchange_source) <> ''
    AND exchange_source = btrim(exchange_source)
    AND exchange_source = lower(exchange_source)
  ),
  client_order_id TEXT CHECK (
    client_order_id IS NULL
    OR (btrim(client_order_id) <> '' AND client_order_id = btrim(client_order_id))
  ),
  intent_id UUID REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  submission_attempt_id UUID
    REFERENCES live_entry_submissions_v1(submission_attempt_id)
    ON DELETE RESTRICT,
  ack_id UUID REFERENCES live_entry_order_acks_v1(ack_id) ON DELETE RESTRICT,
  strategy TEXT CHECK (
    strategy IS NULL
    OR (btrim(strategy) <> '' AND strategy = btrim(strategy)
        AND strategy = upper(strategy))
  ),
  "interval" TEXT CHECK (
    "interval" IS NULL
    OR (btrim("interval") <> '' AND "interval" = btrim("interval")
        AND "interval" = lower("interval"))
  ),
  order_purpose TEXT CHECK (
    order_purpose IS NULL OR order_purpose = 'ENTRY'
  ),
  local_fill_id BIGINT REFERENCES binance_order_fills(id) ON DELETE RESTRICT,
  linked_position_id BIGINT REFERENCES positions(id) ON DELETE RESTRICT,
  attribution_status TEXT NOT NULL CHECK (attribution_status IN (
    'BOT_OWNED_ATTRIBUTED',
    'BOT_OWNED_MISSING_POSITION',
    'BOT_OWNED_MISSING_LINEAGE',
    'LEGACY_BOT_OWNED',
    'EXTERNAL_OR_MANUAL_UNLINKED',
    'AMBIGUOUS',
    'CONFLICTED',
    'UNKNOWN'
  )),
  attribution_fingerprint TEXT NOT NULL CHECK (
    attribution_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  application_status TEXT NOT NULL CHECK (application_status IN (
    'OBSERVED_NOT_APPLIED',
    'APPLIED',
    'TRUE_DUPLICATE_APPLIED',
    'IDEMPOTENCY_CONFLICT',
    'EXTERNAL_OR_MANUAL_UNLINKED',
    'AMBIGUOUS',
    'CORRECTION_PENDING'
  )),
  application_target_identity TEXT CHECK (
    application_target_identity IS NULL
    OR (
      btrim(application_target_identity) <> ''
      AND application_target_identity = btrim(application_target_identity)
    )
  ),
  canonical_source_fingerprint TEXT NOT NULL CHECK (
    canonical_source_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  observed_source_fingerprint TEXT NOT NULL CHECK (
    observed_source_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  applied_fingerprint TEXT CHECK (
    applied_fingerprint IS NULL
    OR applied_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  applied_at TIMESTAMPTZ,
  decision_fingerprint TEXT NOT NULL CHECK (
    decision_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  decision_payload JSONB NOT NULL,
  decided_at TIMESTAMPTZ NOT NULL,
  producer_identity TEXT NOT NULL CHECK (
    btrim(producer_identity) <> ''
    AND producer_identity = btrim(producer_identity)
  ),
  contract_version TEXT NOT NULL CHECK (
    contract_version = 'LIVE_ENTRY_FILL_APPLICATION_V1'
  ),
  CONSTRAINT ck_live_entry_fill_application_environment_deployment_v1 CHECK (
    (environment='paper' AND deployment_id IN ('local-paper','vps-paper'))
    OR
    (environment='live' AND deployment_id IN ('local-live','vps-live'))
  ),
  CONSTRAINT ck_live_entry_fill_application_lineage_shape_v1 CHECK (
    (submission_attempt_id IS NULL OR intent_id IS NOT NULL)
    AND (ack_id IS NULL OR submission_attempt_id IS NOT NULL)
  ),
  CONSTRAINT ck_live_entry_fill_application_strategy_interval_pair_v1 CHECK (
    (strategy IS NULL) = ("interval" IS NULL)
  ),
  CONSTRAINT ck_live_entry_fill_application_attribution_v1 CHECK (
    CASE attribution_status
      WHEN 'BOT_OWNED_ATTRIBUTED' THEN
        intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND client_order_id IS NOT NULL
        AND strategy IS NOT NULL
        AND "interval" IS NOT NULL
        AND order_purpose IS NOT NULL
        AND order_purpose = 'ENTRY'
        AND linked_position_id IS NOT NULL
      WHEN 'BOT_OWNED_MISSING_POSITION' THEN
        intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND client_order_id IS NOT NULL
        AND strategy IS NOT NULL
        AND "interval" IS NOT NULL
        AND order_purpose IS NOT NULL
        AND order_purpose = 'ENTRY'
        AND linked_position_id IS NULL
      WHEN 'EXTERNAL_OR_MANUAL_UNLINKED' THEN
        intent_id IS NULL
        AND submission_attempt_id IS NULL
        AND ack_id IS NULL
        AND client_order_id IS NULL
        AND strategy IS NULL
        AND "interval" IS NULL
        AND order_purpose IS NULL
        AND local_fill_id IS NULL
        AND linked_position_id IS NULL
      ELSE true
    END
  ),
  CONSTRAINT ck_live_entry_fill_application_proof_v1 CHECK (
    CASE application_status
      WHEN 'OBSERVED_NOT_APPLIED' THEN
        attribution_status NOT IN (
          'EXTERNAL_OR_MANUAL_UNLINKED','AMBIGUOUS','CONFLICTED'
        )
        AND observed_source_fingerprint = canonical_source_fingerprint
        AND (
          local_fill_id IS NULL
          OR application_target_identity IS NULL
          OR applied_fingerprint IS NULL
          OR applied_at IS NULL
        )
      WHEN 'APPLIED' THEN
        attribution_status IN (
          'BOT_OWNED_ATTRIBUTED','BOT_OWNED_MISSING_POSITION'
        )
        AND intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND local_fill_id IS NOT NULL
        AND application_target_identity IS NOT NULL
        AND application_target_identity =
            'binance_order_fills:' || local_fill_id::text
        AND observed_source_fingerprint = canonical_source_fingerprint
        AND applied_fingerprint IS NOT NULL
        AND applied_fingerprint = canonical_source_fingerprint
        AND applied_at IS NOT NULL
      WHEN 'TRUE_DUPLICATE_APPLIED' THEN
        attribution_status IN (
          'BOT_OWNED_ATTRIBUTED','BOT_OWNED_MISSING_POSITION'
        )
        AND intent_id IS NOT NULL
        AND submission_attempt_id IS NOT NULL
        AND ack_id IS NOT NULL
        AND local_fill_id IS NOT NULL
        AND application_target_identity IS NOT NULL
        AND application_target_identity =
            'binance_order_fills:' || local_fill_id::text
        AND observed_source_fingerprint = canonical_source_fingerprint
        AND applied_fingerprint IS NOT NULL
        AND applied_fingerprint = canonical_source_fingerprint
        AND applied_at IS NOT NULL
      WHEN 'IDEMPOTENCY_CONFLICT' THEN
        application_target_identity IS NULL
        AND local_fill_id IS NULL
        AND applied_fingerprint IS NULL
        AND applied_at IS NULL
      WHEN 'EXTERNAL_OR_MANUAL_UNLINKED' THEN
        attribution_status = 'EXTERNAL_OR_MANUAL_UNLINKED'
        AND observed_source_fingerprint = canonical_source_fingerprint
        AND application_target_identity IS NULL
        AND applied_fingerprint IS NULL
        AND applied_at IS NULL
      WHEN 'AMBIGUOUS' THEN
        attribution_status = 'AMBIGUOUS'
        AND observed_source_fingerprint = canonical_source_fingerprint
        AND application_target_identity IS NULL
        AND local_fill_id IS NULL
        AND applied_fingerprint IS NULL
        AND applied_at IS NULL
      WHEN 'CORRECTION_PENDING' THEN
        observed_source_fingerprint <> canonical_source_fingerprint
        AND application_target_identity IS NULL
        AND local_fill_id IS NULL
        AND applied_fingerprint IS NULL
        AND applied_at IS NULL
      ELSE false
    END
  ),
  CONSTRAINT ux_live_entry_fill_application_decision_v1 UNIQUE (
    fill_evidence_id,decision_fingerprint
  )
);

CREATE INDEX IF NOT EXISTS ix_live_entry_fill_exchange_order_v1
  ON live_entry_fill_evidence_v1(
    environment,deployment_id,exchange_source,exchange_order_id
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_client_order_v1
  ON live_entry_fill_evidence_v1(
    environment,deployment_id,exchange_source,client_order_id
  ) WHERE client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_wire_client_order_v1
  ON live_entry_fill_evidence_v1(
    environment,deployment_id,exchange_source,wire_client_order_id
  ) WHERE wire_client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_intent_v1
  ON live_entry_fill_evidence_v1(intent_id,executed_at)
  WHERE intent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_submission_v1
  ON live_entry_fill_evidence_v1(submission_attempt_id,executed_at)
  WHERE submission_attempt_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_ack_v1
  ON live_entry_fill_evidence_v1(ack_id,executed_at)
  WHERE ack_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_position_v1
  ON live_entry_fill_evidence_v1(linked_position_id,executed_at)
  WHERE linked_position_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_adoption_generation_v1
  ON live_entry_fill_evidence_v1(
    adoption_id,generation,observed_at DESC
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_status_v1
  ON live_entry_fill_applications_v1(
    environment,deployment_id,application_status,decided_at DESC
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_history_v1
  ON live_entry_fill_applications_v1(
    fill_evidence_id,application_status,decided_at DESC
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_intent_v1
  ON live_entry_fill_applications_v1(intent_id,decided_at DESC)
  WHERE intent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_client_order_v1
  ON live_entry_fill_applications_v1(
    environment,deployment_id,exchange_source,client_order_id,decided_at DESC
  ) WHERE client_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_local_fill_v1
  ON live_entry_fill_applications_v1(local_fill_id,decided_at DESC)
  WHERE local_fill_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_live_entry_fill_application_adoption_v1
  ON live_entry_fill_applications_v1(
    adoption_id,generation,decided_at DESC
  );

CREATE OR REPLACE FUNCTION live_entry_fill_position_link_matches_v1(
  p_exchange_source TEXT,
  p_symbol TEXT,
  p_exchange_order_id TEXT,
  p_wire_client_order_id TEXT,
  p_linked_position_id BIGINT
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
  WITH normalized_orders AS (
    SELECT bo.*,
      CASE WHEN p_exchange_source='okx'
        THEN left(
          regexp_replace(
            COALESCE(bo.client_order_id,''),'[^A-Za-z0-9]','','g'
          ),32
        )
        ELSE bo.client_order_id
      END AS normalized_client_order_id
    FROM binance_orders bo
    WHERE lower(COALESCE(bo.exchange_source,''))=p_exchange_source
      AND bo.symbol=p_symbol
  ),
  candidates AS (
    SELECT bo.*
    FROM normalized_orders bo
    WHERE (
      bo.order_id=p_exchange_order_id
      OR (
        p_wire_client_order_id IS NOT NULL
        AND bo.normalized_client_order_id=p_wire_client_order_id
      )
    )
  ),
  exact_orders AS (
    SELECT bo.*
    FROM candidates bo
    WHERE (bo.order_id IS NULL OR bo.order_id=p_exchange_order_id)
      AND (
        p_wire_client_order_id IS NULL
        OR bo.client_order_id IS NULL
        OR bo.normalized_client_order_id=p_wire_client_order_id
      )
  ),
  identity_state AS (
    SELECT COALESCE(bool_or(
      (bo.order_id IS NOT NULL AND bo.order_id<>p_exchange_order_id)
      OR (
        p_wire_client_order_id IS NOT NULL
        AND bo.client_order_id IS NOT NULL
        AND bo.normalized_client_order_id<>p_wire_client_order_id
      )
    ),false) AS conflicted
    FROM candidates bo
  ),
  exact_positions(position_id) AS (
    SELECT bo.position_id
    FROM exact_orders bo
    WHERE bo.position_id IS NOT NULL
    UNION
    SELECT bo.reconciled_position_id
    FROM exact_orders bo
    WHERE bo.reconciled_position_id IS NOT NULL
    UNION
    SELECT p.id
    FROM positions p
    JOIN exact_orders bo ON p.entry_order_id=bo.order_id
  ),
  position_state AS (
    SELECT count(*) AS position_count,min(position_id) AS only_position_id
    FROM exact_positions
  )
  SELECT p_linked_position_id IS NOT NULL
    AND NOT identity_state.conflicted
    AND position_state.position_count=1
    AND position_state.only_position_id=p_linked_position_id
  FROM identity_state CROSS JOIN position_state;
$$;

CREATE OR REPLACE FUNCTION validate_live_entry_fill_evidence_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  adoption runtime_contract_adoption_v2%ROWTYPE;
  intent live_entry_intents_v1%ROWTYPE;
  submission live_entry_submissions_v1%ROWTYPE;
  ack live_entry_order_acks_v1%ROWTYPE;
BEGIN
  SELECT * INTO adoption
  FROM runtime_contract_adoption_v2
  WHERE adoption_id=NEW.adoption_id;
  IF NOT FOUND
     OR adoption.contract_name <> 'FEE_AWARE_INVENTORY_C2_2'
     OR adoption.environment IS DISTINCT FROM NEW.environment
     OR adoption.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR adoption.generation IS DISTINCT FROM NEW.generation
     OR adoption.git_revision IS DISTINCT FROM NEW.git_revision
     OR (
       adoption.status <> 'ACTIVE'
       AND (
         adoption.adopted_at IS NULL
         OR (
           NEW.intent_id IS NULL
           AND NEW.submission_attempt_id IS NULL
           AND NEW.ack_id IS NULL
         )
       )
     ) THEN
    RAISE EXCEPTION 'LEI1C_FILL_ADOPTION_ATTRIBUTION_MISMATCH';
  END IF;

  IF NEW.client_order_id IS NOT NULL
     AND NEW.wire_client_order_id IS NOT NULL
     AND (
       CASE WHEN NEW.exchange_source='okx'
         THEN left(
           regexp_replace(NEW.client_order_id,'[^A-Za-z0-9]','','g'),32
         )
         ELSE NEW.client_order_id
       END
     ) IS DISTINCT FROM NEW.wire_client_order_id THEN
    RAISE EXCEPTION 'LEI1C_FILL_WIRE_CLIENT_ORDER_ID_MISMATCH';
  END IF;

  IF NEW.intent_id IS NOT NULL THEN
    SELECT * INTO intent
    FROM live_entry_intents_v1
    WHERE intent_id=NEW.intent_id;
    IF NOT FOUND
       OR intent.environment IS DISTINCT FROM NEW.environment
       OR intent.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR intent.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR intent.generation IS DISTINCT FROM NEW.generation
       OR intent.git_revision IS DISTINCT FROM NEW.git_revision
       OR intent.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR intent.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR intent.symbol IS DISTINCT FROM NEW.symbol
       OR intent.strategy IS DISTINCT FROM NEW.strategy
       OR intent."interval" IS DISTINCT FROM NEW."interval"
       OR intent.order_purpose IS DISTINCT FROM NEW.order_purpose
       OR intent.side IS DISTINCT FROM NEW.side THEN
      RAISE EXCEPTION 'LEI1C_FILL_INTENT_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF NEW.submission_attempt_id IS NOT NULL THEN
    SELECT * INTO submission
    FROM live_entry_submissions_v1
    WHERE submission_attempt_id=NEW.submission_attempt_id;
    IF NOT FOUND
       OR submission.intent_id IS DISTINCT FROM NEW.intent_id
       OR submission.environment IS DISTINCT FROM NEW.environment
       OR submission.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR submission.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR submission.generation IS DISTINCT FROM NEW.generation
       OR submission.git_revision IS DISTINCT FROM NEW.git_revision
       OR submission.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR submission.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR submission.symbol IS DISTINCT FROM NEW.symbol
       OR submission.strategy IS DISTINCT FROM NEW.strategy
       OR submission."interval" IS DISTINCT FROM NEW."interval"
       OR submission.order_purpose IS DISTINCT FROM NEW.order_purpose
       OR submission.side IS DISTINCT FROM NEW.side THEN
      RAISE EXCEPTION 'LEI1C_FILL_SUBMISSION_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF NEW.ack_id IS NOT NULL THEN
    SELECT * INTO ack
    FROM live_entry_order_acks_v1
    WHERE ack_id=NEW.ack_id;
    IF NOT FOUND
       OR ack.intent_id IS DISTINCT FROM NEW.intent_id
       OR ack.submission_attempt_id IS DISTINCT FROM NEW.submission_attempt_id
       OR ack.environment IS DISTINCT FROM NEW.environment
       OR ack.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR ack.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR ack.generation IS DISTINCT FROM NEW.generation
       OR ack.git_revision IS DISTINCT FROM NEW.git_revision
       OR ack.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR ack.exchange_order_id IS DISTINCT FROM NEW.exchange_order_id
       OR ack.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR ack.symbol IS DISTINCT FROM NEW.symbol
       OR ack.strategy IS DISTINCT FROM NEW.strategy
       OR ack."interval" IS DISTINCT FROM NEW."interval"
       OR ack.order_purpose IS DISTINCT FROM NEW.order_purpose
       OR ack.side IS DISTINCT FROM NEW.side THEN
      RAISE EXCEPTION 'LEI1C_FILL_ACK_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF NEW.linked_position_id IS NOT NULL
     AND NOT live_entry_fill_position_link_matches_v1(
       NEW.exchange_source,
       NEW.symbol,
       NEW.exchange_order_id,
       NEW.wire_client_order_id,
       NEW.linked_position_id
     ) THEN
    RAISE EXCEPTION 'LEI1C_FILL_POSITION_ATTRIBUTION_MISMATCH';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION validate_live_entry_fill_application_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  evidence live_entry_fill_evidence_v1%ROWTYPE;
  intent live_entry_intents_v1%ROWTYPE;
  submission live_entry_submissions_v1%ROWTYPE;
  ack live_entry_order_acks_v1%ROWTYPE;
  local_fill binance_order_fills%ROWTYPE;
BEGIN
  SELECT * INTO evidence
  FROM live_entry_fill_evidence_v1
  WHERE fill_evidence_id=NEW.fill_evidence_id
  FOR UPDATE;
  IF NOT FOUND
     OR evidence.environment IS DISTINCT FROM NEW.environment
     OR evidence.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR evidence.adoption_id IS DISTINCT FROM NEW.adoption_id
     OR evidence.generation IS DISTINCT FROM NEW.generation
     OR evidence.git_revision IS DISTINCT FROM NEW.git_revision
     OR evidence.exchange_source IS DISTINCT FROM NEW.exchange_source
     OR (
       evidence.client_order_id IS NOT NULL
       AND NEW.client_order_id IS DISTINCT FROM evidence.client_order_id
     )
     OR (
       evidence.intent_id IS NOT NULL
       AND NEW.intent_id IS DISTINCT FROM evidence.intent_id
     )
     OR (
       evidence.submission_attempt_id IS NOT NULL
       AND NEW.submission_attempt_id IS DISTINCT FROM
           evidence.submission_attempt_id
     )
     OR (
       evidence.ack_id IS NOT NULL
       AND NEW.ack_id IS DISTINCT FROM evidence.ack_id
     )
     OR (
       evidence.linked_position_id IS NOT NULL
       AND NEW.linked_position_id IS DISTINCT FROM
           evidence.linked_position_id
     )
     OR (
       evidence.strategy IS NOT NULL
       AND NEW.strategy IS DISTINCT FROM evidence.strategy
     )
     OR (
       evidence."interval" IS NOT NULL
       AND NEW."interval" IS DISTINCT FROM evidence."interval"
     )
     OR (
       evidence.order_purpose IS NOT NULL
       AND NEW.order_purpose IS DISTINCT FROM evidence.order_purpose
     )
     OR evidence.source_fingerprint IS DISTINCT FROM
        NEW.canonical_source_fingerprint THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_EVIDENCE_ATTRIBUTION_MISMATCH';
  END IF;

  IF NEW.client_order_id IS NOT NULL
     AND evidence.wire_client_order_id IS NOT NULL
     AND (
       CASE WHEN NEW.exchange_source='okx'
         THEN left(
           regexp_replace(NEW.client_order_id,'[^A-Za-z0-9]','','g'),32
         )
         ELSE NEW.client_order_id
       END
     ) IS DISTINCT FROM evidence.wire_client_order_id THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_WIRE_CLIENT_ORDER_ID_MISMATCH';
  END IF;

  IF EXISTS (
       SELECT 1
       FROM live_entry_fill_applications_v1 prior
       WHERE prior.fill_evidence_id=NEW.fill_evidence_id
         AND (
           (prior.intent_id IS NOT NULL
            AND NEW.intent_id IS DISTINCT FROM prior.intent_id)
           OR (prior.client_order_id IS NOT NULL
               AND NEW.client_order_id IS DISTINCT FROM
                   prior.client_order_id)
           OR (prior.submission_attempt_id IS NOT NULL
               AND NEW.submission_attempt_id IS DISTINCT FROM
                   prior.submission_attempt_id)
           OR (prior.ack_id IS NOT NULL
               AND NEW.ack_id IS DISTINCT FROM prior.ack_id)
           OR (prior.linked_position_id IS NOT NULL
               AND NEW.linked_position_id IS DISTINCT FROM
                   prior.linked_position_id)
           OR (prior.strategy IS NOT NULL
               AND NEW.strategy IS DISTINCT FROM prior.strategy)
           OR (prior."interval" IS NOT NULL
               AND NEW."interval" IS DISTINCT FROM prior."interval")
           OR (prior.order_purpose IS NOT NULL
               AND NEW.order_purpose IS DISTINCT FROM prior.order_purpose)
         )
     ) THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_LINEAGE_REGRESSION';
  END IF;

  IF NEW.application_status='IDEMPOTENCY_CONFLICT'
     AND NEW.observed_source_fingerprint =
         NEW.canonical_source_fingerprint
     AND NEW.attribution_fingerprint = evidence.attribution_fingerprint
     AND NEW.attribution_status <> 'CONFLICTED' THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_CONFLICT_PROOF_MISSING';
  END IF;

  IF NEW.application_status NOT IN (
       'IDEMPOTENCY_CONFLICT','CORRECTION_PENDING','AMBIGUOUS'
     )
     AND EXISTS (
       SELECT 1
       FROM live_entry_fill_applications_v1 prior
       WHERE prior.fill_evidence_id=NEW.fill_evidence_id
         AND prior.application_status IN (
           'IDEMPOTENCY_CONFLICT','CORRECTION_PENDING','AMBIGUOUS'
         )
     ) THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_UNRESOLVED_HARD_STATE';
  END IF;

  IF NEW.intent_id IS NOT NULL THEN
    SELECT * INTO intent
    FROM live_entry_intents_v1
    WHERE intent_id=NEW.intent_id;
    IF NOT FOUND
       OR intent.environment IS DISTINCT FROM NEW.environment
       OR intent.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR intent.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR intent.generation IS DISTINCT FROM NEW.generation
       OR intent.git_revision IS DISTINCT FROM NEW.git_revision
       OR intent.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR intent.symbol IS DISTINCT FROM evidence.symbol
       OR intent.side IS DISTINCT FROM evidence.side
       OR intent.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR intent.strategy IS DISTINCT FROM NEW.strategy
       OR intent."interval" IS DISTINCT FROM NEW."interval"
       OR intent.order_purpose IS DISTINCT FROM NEW.order_purpose THEN
      RAISE EXCEPTION 'LEI1C_APPLICATION_INTENT_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF NEW.submission_attempt_id IS NOT NULL THEN
    SELECT * INTO submission
    FROM live_entry_submissions_v1
    WHERE submission_attempt_id=NEW.submission_attempt_id;
    IF NOT FOUND
       OR submission.intent_id IS DISTINCT FROM NEW.intent_id
       OR submission.environment IS DISTINCT FROM NEW.environment
       OR submission.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR submission.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR submission.generation IS DISTINCT FROM NEW.generation
       OR submission.git_revision IS DISTINCT FROM NEW.git_revision
       OR submission.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR submission.symbol IS DISTINCT FROM evidence.symbol
       OR submission.side IS DISTINCT FROM evidence.side
       OR submission.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR submission.strategy IS DISTINCT FROM NEW.strategy
       OR submission."interval" IS DISTINCT FROM NEW."interval"
       OR submission.order_purpose IS DISTINCT FROM NEW.order_purpose THEN
      RAISE EXCEPTION 'LEI1C_APPLICATION_SUBMISSION_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF NEW.ack_id IS NOT NULL THEN
    SELECT * INTO ack
    FROM live_entry_order_acks_v1
    WHERE ack_id=NEW.ack_id;
    IF NOT FOUND
       OR ack.intent_id IS DISTINCT FROM NEW.intent_id
       OR ack.submission_attempt_id IS DISTINCT FROM NEW.submission_attempt_id
       OR ack.environment IS DISTINCT FROM NEW.environment
       OR ack.deployment_id IS DISTINCT FROM NEW.deployment_id
       OR ack.adoption_id IS DISTINCT FROM NEW.adoption_id
       OR ack.generation IS DISTINCT FROM NEW.generation
       OR ack.git_revision IS DISTINCT FROM NEW.git_revision
       OR ack.exchange_source IS DISTINCT FROM NEW.exchange_source
       OR ack.exchange_order_id IS DISTINCT FROM evidence.exchange_order_id
       OR ack.symbol IS DISTINCT FROM evidence.symbol
       OR ack.side IS DISTINCT FROM evidence.side
       OR ack.client_order_id IS DISTINCT FROM NEW.client_order_id
       OR ack.strategy IS DISTINCT FROM NEW.strategy
       OR ack."interval" IS DISTINCT FROM NEW."interval"
       OR ack.order_purpose IS DISTINCT FROM NEW.order_purpose THEN
      RAISE EXCEPTION 'LEI1C_APPLICATION_ACK_ATTRIBUTION_MISMATCH';
    END IF;
  END IF;

  IF evidence.linked_position_id IS NULL
     AND NEW.linked_position_id IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM live_entry_fill_applications_v1 prior
       WHERE prior.fill_evidence_id=NEW.fill_evidence_id
         AND prior.linked_position_id=NEW.linked_position_id
     )
     AND NOT live_entry_fill_position_link_matches_v1(
       evidence.exchange_source,
       evidence.symbol,
       evidence.exchange_order_id,
       evidence.wire_client_order_id,
       NEW.linked_position_id
     ) THEN
    RAISE EXCEPTION 'LEI1C_APPLICATION_POSITION_ATTRIBUTION_MISMATCH';
  END IF;

  IF NEW.local_fill_id IS NOT NULL THEN
    SELECT * INTO local_fill
    FROM binance_order_fills
    WHERE id=NEW.local_fill_id;
    IF NOT FOUND
       OR lower(local_fill.source) IS DISTINCT FROM evidence.exchange_source
       OR local_fill.trade_id::text IS DISTINCT FROM
          evidence.exchange_trade_id
       OR local_fill.order_id IS DISTINCT FROM evidence.exchange_order_id
       OR local_fill.symbol IS DISTINCT FROM evidence.symbol
       OR upper(local_fill.side) IS DISTINCT FROM evidence.side
       OR local_fill.executed_qty IS DISTINCT FROM evidence.executed_qty
       OR local_fill.avg_price IS DISTINCT FROM evidence.price
       OR local_fill.quote_notional_usdc IS DISTINCT FROM evidence.notional
       OR local_fill.commission_amount IS DISTINCT FROM evidence.fee
       OR local_fill.commission_asset IS DISTINCT FROM evidence.fee_asset
       OR local_fill.event_time IS DISTINCT FROM evidence.executed_at THEN
      RAISE EXCEPTION 'LEI1C_APPLICATION_LOCAL_FILL_PROOF_MISMATCH';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION prevent_live_entry_fill_history_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is immutable and append-only', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS live_entry_fill_validate_attribution_v1
  ON live_entry_fill_evidence_v1;
CREATE TRIGGER live_entry_fill_validate_attribution_v1
BEFORE INSERT ON live_entry_fill_evidence_v1
FOR EACH ROW EXECUTE FUNCTION validate_live_entry_fill_evidence_v1();

DROP TRIGGER IF EXISTS live_entry_fill_immutable_v1
  ON live_entry_fill_evidence_v1;
CREATE TRIGGER live_entry_fill_immutable_v1
BEFORE UPDATE OR DELETE ON live_entry_fill_evidence_v1
FOR EACH ROW EXECUTE FUNCTION prevent_live_entry_fill_history_mutation_v1();

DROP TRIGGER IF EXISTS live_entry_fill_application_validate_v1
  ON live_entry_fill_applications_v1;
CREATE TRIGGER live_entry_fill_application_validate_v1
BEFORE INSERT ON live_entry_fill_applications_v1
FOR EACH ROW EXECUTE FUNCTION validate_live_entry_fill_application_v1();

DROP TRIGGER IF EXISTS live_entry_fill_application_immutable_v1
  ON live_entry_fill_applications_v1;
CREATE TRIGGER live_entry_fill_application_immutable_v1
BEFORE UPDATE OR DELETE ON live_entry_fill_applications_v1
FOR EACH ROW EXECUTE FUNCTION prevent_live_entry_fill_history_mutation_v1();

COMMENT ON TABLE live_entry_fill_evidence_v1 IS
  'LEI1C immutable authoritative exchange fill evidence with observation-time attribution. One row per environment/deployment/exchange/trade identity; no position projection.';
COMMENT ON COLUMN live_entry_fill_evidence_v1.source_fingerprint IS
  'Canonical semantic fingerprint over exchange order/client identity, symbol, side, quantity, price, notional, fee, fee asset, and execution timestamp.';
COMMENT ON COLUMN live_entry_fill_evidence_v1.client_order_id IS
  'Original deterministic producer CID when resolved through LEI1B; never replaced by exchange wire normalization.';
COMMENT ON COLUMN live_entry_fill_evidence_v1.wire_client_order_id IS
  'Exact exchange-observed client order ID, including OKX alphanumeric/truncation normalization.';
COMMENT ON COLUMN live_entry_fill_evidence_v1.linked_position_id IS
  'Typed observation-time lineage evidence. BOT_OWNED_ATTRIBUTED requires exactly one null-tolerant, identity-consistent order/CID-to-position link; this is not application proof.';
COMMENT ON TABLE live_entry_fill_applications_v1 IS
  'LEI1C append-only attribution/application decisions, including stronger exact lineage recovered after evidence commit. APPLIED requires exact local proof from the existing ingestion writer bridge or a future LEI1D projector.';
COMMENT ON COLUMN live_entry_fill_applications_v1.decision_payload IS
  'Immutable decision evidence; conflict/correction rows preserve the changed incoming semantic payload instead of overwriting canonical fill evidence.';

INSERT INTO schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,
  schema_baseline_version
)
SELECT
  '20260731_live_entry_fill_attribution_v1.sql',
  'ad72d70d21d440de1d65c3499a1de1e95b6a27af0721c4c3c9c71f150168541d',
  'LIVE','LEI1C_SCHEMA_LIVE',
  current_database(),'operator-migration','APPLIED',true,0,
  COALESCE(
    (
      SELECT git_revision
      FROM runtime_contract_adoption_v2
      WHERE status='ACTIVE'
        AND environment='live'
      ORDER BY generation DESC,adoption_id DESC
      LIMIT 1
    ),
    repeat('0',40)
  ),
  'LEI1C_ENTRY_FILL_ATTRIBUTION_V1'
WHERE NOT EXISTS (
  SELECT 1 FROM schema_migration_ledger_v1
  WHERE migration_id='20260731_live_entry_fill_attribution_v1.sql'
);

COMMIT;

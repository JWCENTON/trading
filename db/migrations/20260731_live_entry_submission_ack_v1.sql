-- WALTRADE LEI1B: committed entry submission claim and immutable ACK linkage.
-- Additive only: no backfill and no writer activation.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
  IF to_regclass('public.live_entry_intents_v1') IS NULL THEN
    RAISE EXCEPTION 'LEI1B_PREREQUISITE_LIVE_ENTRY_INTENTS_V1_MISSING';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS live_entry_submissions_v1 (
  submission_attempt_id UUID PRIMARY KEY,
  intent_id UUID NOT NULL
    REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  environment TEXT NOT NULL CHECK (environment IN ('paper','live')),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','local-live','vps-paper','vps-live'
  )),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  client_order_id TEXT NOT NULL CHECK (
    btrim(client_order_id) <> '' AND client_order_id = btrim(client_order_id)
  ),
  exchange_source TEXT NOT NULL CHECK (
    btrim(exchange_source) <> ''
    AND exchange_source = btrim(exchange_source)
    AND exchange_source = lower(exchange_source)
  ),
  symbol TEXT NOT NULL CHECK (
    btrim(symbol) <> '' AND symbol = btrim(symbol) AND symbol = upper(symbol)
  ),
  strategy TEXT NOT NULL CHECK (
    btrim(strategy) <> ''
    AND strategy = btrim(strategy)
    AND strategy = upper(strategy)
  ),
  "interval" TEXT NOT NULL CHECK (
    btrim("interval") <> ''
    AND "interval" = btrim("interval")
    AND "interval" = lower("interval")
  ),
  order_purpose TEXT NOT NULL CHECK (order_purpose = 'ENTRY'),
  side TEXT NOT NULL CHECK (side = 'BUY'),
  requested_qty NUMERIC NOT NULL CHECK (requested_qty > 0),
  attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal = 1),
  submission_fingerprint TEXT NOT NULL CHECK (
    submission_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  submitted_at TIMESTAMPTZ NOT NULL,
  producer_identity TEXT NOT NULL CHECK (
    btrim(producer_identity) <> ''
    AND producer_identity = btrim(producer_identity)
  ),
  contract_version TEXT NOT NULL CHECK (
    contract_version = 'LIVE_ENTRY_SUBMISSION_V1'
  ),
  CONSTRAINT ck_live_entry_submission_environment_deployment_v1 CHECK (
    (environment='paper' AND deployment_id IN ('local-paper','vps-paper'))
    OR
    (environment='live' AND deployment_id IN ('local-live','vps-live'))
  ),
  CONSTRAINT ux_live_entry_submission_intent_ordinal_v1
    UNIQUE(intent_id,attempt_ordinal),
  CONSTRAINT ux_live_entry_submission_client_ordinal_v1
    UNIQUE(
      environment,deployment_id,exchange_source,client_order_id,attempt_ordinal
    )
);

CREATE TABLE IF NOT EXISTS live_entry_order_acks_v1 (
  ack_id UUID PRIMARY KEY,
  submission_attempt_id UUID NOT NULL
    REFERENCES live_entry_submissions_v1(submission_attempt_id)
    ON DELETE RESTRICT,
  intent_id UUID NOT NULL
    REFERENCES live_entry_intents_v1(intent_id) ON DELETE RESTRICT,
  environment TEXT NOT NULL CHECK (environment IN ('paper','live')),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN (
    'local-paper','local-live','vps-paper','vps-live'
  )),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  git_revision TEXT NOT NULL CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  client_order_id TEXT NOT NULL CHECK (
    btrim(client_order_id) <> '' AND client_order_id = btrim(client_order_id)
  ),
  exchange_source TEXT NOT NULL CHECK (
    btrim(exchange_source) <> ''
    AND exchange_source = btrim(exchange_source)
    AND exchange_source = lower(exchange_source)
  ),
  exchange_order_id TEXT NOT NULL CHECK (
    btrim(exchange_order_id) <> ''
    AND exchange_order_id = btrim(exchange_order_id)
  ),
  exchange_order_status TEXT NOT NULL CHECK (
    btrim(exchange_order_status) <> ''
    AND exchange_order_status = btrim(exchange_order_status)
    AND exchange_order_status = upper(exchange_order_status)
  ),
  symbol TEXT NOT NULL CHECK (
    btrim(symbol) <> '' AND symbol = btrim(symbol) AND symbol = upper(symbol)
  ),
  strategy TEXT NOT NULL CHECK (
    btrim(strategy) <> ''
    AND strategy = btrim(strategy)
    AND strategy = upper(strategy)
  ),
  "interval" TEXT NOT NULL CHECK (
    btrim("interval") <> ''
    AND "interval" = btrim("interval")
    AND "interval" = lower("interval")
  ),
  order_purpose TEXT NOT NULL CHECK (order_purpose = 'ENTRY'),
  side TEXT NOT NULL CHECK (side = 'BUY'),
  requested_qty NUMERIC NOT NULL CHECK (requested_qty > 0),
  ack_fingerprint TEXT NOT NULL CHECK (
    ack_fingerprint ~ '^[0-9a-f]{64}$'
  ),
  acknowledged_at TIMESTAMPTZ NOT NULL,
  recovered_by_client_order_id BOOLEAN NOT NULL DEFAULT false,
  producer_identity TEXT NOT NULL CHECK (
    btrim(producer_identity) <> ''
    AND producer_identity = btrim(producer_identity)
  ),
  contract_version TEXT NOT NULL CHECK (
    contract_version = 'LIVE_ENTRY_ORDER_ACK_V1'
  ),
  CONSTRAINT ck_live_entry_ack_environment_deployment_v1 CHECK (
    (environment='paper' AND deployment_id IN ('local-paper','vps-paper'))
    OR
    (environment='live' AND deployment_id IN ('local-live','vps-live'))
  ),
  CONSTRAINT ux_live_entry_ack_intent_v1 UNIQUE(intent_id),
  CONSTRAINT ux_live_entry_ack_client_v1 UNIQUE(
    environment,deployment_id,exchange_source,client_order_id
  ),
  CONSTRAINT ux_live_entry_ack_exchange_order_v1 UNIQUE(
    environment,deployment_id,exchange_source,exchange_order_id
  )
);

CREATE INDEX IF NOT EXISTS ix_live_entry_submission_client_v1
  ON live_entry_submissions_v1(
    environment,deployment_id,exchange_source,client_order_id
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_submission_intent_time_v1
  ON live_entry_submissions_v1(intent_id,submitted_at DESC);
CREATE INDEX IF NOT EXISTS ix_live_entry_submission_adoption_generation_v1
  ON live_entry_submissions_v1(
    adoption_id,generation,submitted_at DESC
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_ack_client_v1
  ON live_entry_order_acks_v1(
    environment,deployment_id,exchange_source,client_order_id
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_ack_exchange_order_v1
  ON live_entry_order_acks_v1(
    environment,deployment_id,exchange_source,exchange_order_id
  );
CREATE INDEX IF NOT EXISTS ix_live_entry_ack_adoption_generation_v1
  ON live_entry_order_acks_v1(
    adoption_id,generation,acknowledged_at DESC
  );

CREATE OR REPLACE FUNCTION validate_live_entry_submission_attribution_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  intent live_entry_intents_v1%ROWTYPE;
  adoption runtime_contract_adoption_v2%ROWTYPE;
BEGIN
  SELECT * INTO intent
  FROM live_entry_intents_v1
  WHERE intent_id=NEW.intent_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'LEI1B_SUBMISSION_INTENT_MISSING';
  END IF;
  SELECT * INTO adoption
  FROM runtime_contract_adoption_v2
  WHERE adoption_id=NEW.adoption_id;
  IF NOT FOUND
     OR adoption.contract_name <> 'FEE_AWARE_INVENTORY_C2_2'
     OR adoption.status <> 'ACTIVE'
     OR adoption.environment IS DISTINCT FROM NEW.environment
     OR adoption.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR adoption.generation IS DISTINCT FROM NEW.generation
     OR adoption.git_revision IS DISTINCT FROM NEW.git_revision THEN
    RAISE EXCEPTION 'LEI1B_SUBMISSION_ADOPTION_ATTRIBUTION_MISMATCH';
  END IF;
  IF intent.environment IS DISTINCT FROM NEW.environment
     OR intent.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR intent.adoption_id IS DISTINCT FROM NEW.adoption_id
     OR intent.generation IS DISTINCT FROM NEW.generation
     OR intent.git_revision IS DISTINCT FROM NEW.git_revision
     OR intent.client_order_id IS DISTINCT FROM NEW.client_order_id
     OR intent.exchange_source IS DISTINCT FROM NEW.exchange_source
     OR intent.symbol IS DISTINCT FROM NEW.symbol
     OR intent.strategy IS DISTINCT FROM NEW.strategy
     OR intent."interval" IS DISTINCT FROM NEW."interval"
     OR intent.order_purpose IS DISTINCT FROM NEW.order_purpose
     OR intent.side IS DISTINCT FROM NEW.side
     OR intent.requested_qty IS DISTINCT FROM NEW.requested_qty THEN
    RAISE EXCEPTION 'LEI1B_SUBMISSION_INTENT_ATTRIBUTION_MISMATCH';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION validate_live_entry_ack_attribution_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  intent live_entry_intents_v1%ROWTYPE;
  submission live_entry_submissions_v1%ROWTYPE;
BEGIN
  SELECT * INTO intent
  FROM live_entry_intents_v1
  WHERE intent_id=NEW.intent_id;
  SELECT * INTO submission
  FROM live_entry_submissions_v1
  WHERE submission_attempt_id=NEW.submission_attempt_id;
  IF intent.intent_id IS NULL OR submission.submission_attempt_id IS NULL THEN
    RAISE EXCEPTION 'LEI1B_ACK_PARENT_EVIDENCE_MISSING';
  END IF;
  IF submission.intent_id IS DISTINCT FROM NEW.intent_id
     OR intent.environment IS DISTINCT FROM NEW.environment
     OR intent.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR intent.adoption_id IS DISTINCT FROM NEW.adoption_id
     OR intent.generation IS DISTINCT FROM NEW.generation
     OR intent.git_revision IS DISTINCT FROM NEW.git_revision
     OR intent.client_order_id IS DISTINCT FROM NEW.client_order_id
     OR intent.exchange_source IS DISTINCT FROM NEW.exchange_source
     OR intent.symbol IS DISTINCT FROM NEW.symbol
     OR intent.strategy IS DISTINCT FROM NEW.strategy
     OR intent."interval" IS DISTINCT FROM NEW."interval"
     OR intent.order_purpose IS DISTINCT FROM NEW.order_purpose
     OR intent.side IS DISTINCT FROM NEW.side
     OR intent.requested_qty IS DISTINCT FROM NEW.requested_qty
     OR submission.environment IS DISTINCT FROM NEW.environment
     OR submission.deployment_id IS DISTINCT FROM NEW.deployment_id
     OR submission.adoption_id IS DISTINCT FROM NEW.adoption_id
     OR submission.generation IS DISTINCT FROM NEW.generation
     OR submission.git_revision IS DISTINCT FROM NEW.git_revision
     OR submission.client_order_id IS DISTINCT FROM NEW.client_order_id
     OR submission.exchange_source IS DISTINCT FROM NEW.exchange_source
     OR submission.symbol IS DISTINCT FROM NEW.symbol
     OR submission.strategy IS DISTINCT FROM NEW.strategy
     OR submission."interval" IS DISTINCT FROM NEW."interval"
     OR submission.order_purpose IS DISTINCT FROM NEW.order_purpose
     OR submission.side IS DISTINCT FROM NEW.side
     OR submission.requested_qty IS DISTINCT FROM NEW.requested_qty
     OR NEW.acknowledged_at < submission.submitted_at THEN
    RAISE EXCEPTION 'LEI1B_ACK_ATTRIBUTION_MISMATCH';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION prevent_live_entry_submission_history_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '% is immutable and append-only', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS live_entry_submission_validate_attribution_v1
  ON live_entry_submissions_v1;
CREATE TRIGGER live_entry_submission_validate_attribution_v1
BEFORE INSERT ON live_entry_submissions_v1
FOR EACH ROW EXECUTE FUNCTION validate_live_entry_submission_attribution_v1();

DROP TRIGGER IF EXISTS live_entry_submission_immutable_v1
  ON live_entry_submissions_v1;
CREATE TRIGGER live_entry_submission_immutable_v1
BEFORE UPDATE OR DELETE ON live_entry_submissions_v1
FOR EACH ROW EXECUTE FUNCTION prevent_live_entry_submission_history_mutation_v1();

DROP TRIGGER IF EXISTS live_entry_ack_validate_attribution_v1
  ON live_entry_order_acks_v1;
CREATE TRIGGER live_entry_ack_validate_attribution_v1
BEFORE INSERT ON live_entry_order_acks_v1
FOR EACH ROW EXECUTE FUNCTION validate_live_entry_ack_attribution_v1();

DROP TRIGGER IF EXISTS live_entry_ack_immutable_v1
  ON live_entry_order_acks_v1;
CREATE TRIGGER live_entry_ack_immutable_v1
BEFORE UPDATE OR DELETE ON live_entry_order_acks_v1
FOR EACH ROW EXECUTE FUNCTION prevent_live_entry_submission_history_mutation_v1();

COMMENT ON TABLE live_entry_submissions_v1 IS
  'LEI1B immutable claim of one admitted ENTRY network submission attempt; not mutable order, fill, position, or outcome state.';
COMMENT ON COLUMN live_entry_submissions_v1.submitted_at IS
  'Time the committed V1 attempt claim was prepared immediately before network admission; presence requires CID recovery before any retry.';
COMMENT ON TABLE live_entry_order_acks_v1 IS
  'LEI1B immutable exchange ACK linkage from committed intent and attempt to exchange order identity.';
COMMENT ON COLUMN live_entry_order_acks_v1.client_order_id IS
  'Original deterministic producer client order ID; exchange wire normalization never replaces this identity.';
COMMENT ON COLUMN live_entry_order_acks_v1.recovered_by_client_order_id IS
  'True only when the ACK evidence was reconstructed by exact client-order-ID lookup after an uncertain boundary.';

COMMIT;

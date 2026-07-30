BEGIN;

CREATE TABLE IF NOT EXISTS live_entry_intents_v1 (
  intent_id UUID PRIMARY KEY,
  environment TEXT NOT NULL
    CHECK (environment IN ('paper', 'live')),
  deployment_id TEXT NOT NULL
    CHECK (deployment_id IN (
      'local-paper', 'local-live', 'vps-paper', 'vps-live'
    )),
  git_revision TEXT NOT NULL
    CHECK (git_revision ~ '^[0-9a-f]{40}$'),
  adoption_id BIGINT NOT NULL
    REFERENCES runtime_contract_adoption_v2(adoption_id),
  generation BIGINT NOT NULL CHECK (generation > 0),
  decision_id UUID NOT NULL,
  symbol TEXT NOT NULL
    CHECK (btrim(symbol) <> '' AND symbol = upper(symbol)),
  strategy TEXT NOT NULL
    CHECK (btrim(strategy) <> '' AND strategy = upper(strategy)),
  "interval" TEXT NOT NULL
    CHECK (btrim("interval") <> '' AND "interval" = lower("interval")),
  slot_identity TEXT NOT NULL CHECK (btrim(slot_identity) <> ''),
  exchange_source TEXT NOT NULL
    CHECK (
      btrim(exchange_source) <> ''
      AND exchange_source = lower(exchange_source)
    ),
  client_order_id TEXT NOT NULL CHECK (btrim(client_order_id) <> ''),
  order_purpose TEXT NOT NULL CHECK (order_purpose = 'ENTRY'),
  side TEXT NOT NULL CHECK (side = 'BUY'),
  requested_qty NUMERIC NOT NULL CHECK (requested_qty > 0),
  content_fingerprint TEXT NOT NULL
    CHECK (content_fingerprint ~ '^[0-9a-f]{64}$'),
  prepared_at TIMESTAMPTZ NOT NULL,
  producer_identity TEXT NOT NULL CHECK (btrim(producer_identity) <> ''),
  contract_version TEXT NOT NULL
    CHECK (contract_version = 'LIVE_ENTRY_INTENT_V1'),
  CONSTRAINT ck_live_entry_intent_environment_deployment_v1 CHECK (
    (environment = 'paper'
      AND deployment_id IN ('local-paper', 'vps-paper'))
    OR
    (environment = 'live'
      AND deployment_id IN ('local-live', 'vps-live'))
  ),
  CONSTRAINT ck_live_entry_intent_slot_identity_v1 CHECK (
    slot_identity = symbol || ':' || strategy || ':' || "interval"
  ),
  CONSTRAINT ux_live_entry_intent_natural_key_v1 UNIQUE (
    environment, deployment_id, exchange_source, client_order_id
  )
);

CREATE INDEX IF NOT EXISTS ix_live_entry_intent_slot_v1
  ON live_entry_intents_v1(
    environment, deployment_id, slot_identity, prepared_at DESC
  );

CREATE INDEX IF NOT EXISTS ix_live_entry_intent_client_order_v1
  ON live_entry_intents_v1(exchange_source, client_order_id);

CREATE INDEX IF NOT EXISTS ix_live_entry_intent_adoption_generation_v1
  ON live_entry_intents_v1(adoption_id, generation, prepared_at DESC);

CREATE OR REPLACE FUNCTION validate_live_entry_intent_adoption_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM runtime_contract_adoption_v2 adoption
    WHERE adoption.adoption_id = NEW.adoption_id
      AND adoption.generation = NEW.generation
      AND adoption.environment = NEW.environment
      AND adoption.deployment_id = NEW.deployment_id
      AND adoption.contract_name = 'FEE_AWARE_INVENTORY_C2_2'
  ) THEN
    RAISE EXCEPTION 'LIVE_ENTRY_INTENT_ADOPTION_ATTRIBUTION_MISMATCH';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS live_entry_intent_validate_adoption_v1
  ON live_entry_intents_v1;
CREATE TRIGGER live_entry_intent_validate_adoption_v1
BEFORE INSERT ON live_entry_intents_v1
FOR EACH ROW EXECUTE FUNCTION validate_live_entry_intent_adoption_v1();

CREATE OR REPLACE FUNCTION prevent_live_entry_intent_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION
    'Live entry intent ledger is immutable and append-only';
END;
$$;

DROP TRIGGER IF EXISTS live_entry_intent_immutable_v1
  ON live_entry_intents_v1;
CREATE TRIGGER live_entry_intent_immutable_v1
BEFORE UPDATE OR DELETE ON live_entry_intents_v1
FOR EACH ROW EXECUTE FUNCTION prevent_live_entry_intent_mutation_v1();

COMMENT ON TABLE live_entry_intents_v1 IS
  'Append-only immutable evidence of a prepared LIVE/PAPER entry order intent; not order, fill, position, inventory, or outcome state.';
COMMENT ON COLUMN live_entry_intents_v1.content_fingerprint IS
  'SHA-256 of canonical LIVE_ENTRY_INTENT_V1 semantic content; prepared_at is intentionally excluded.';
COMMENT ON COLUMN live_entry_intents_v1.intent_id IS
  'Deterministic UUIDv5 over environment, deployment, exchange, and client order identity.';

COMMIT;

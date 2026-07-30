BEGIN;

CREATE TABLE IF NOT EXISTS supertrend_exit_intents_v1 (
  position_id BIGINT NOT NULL REFERENCES positions(id),
  simulated_order_id BIGINT NOT NULL REFERENCES simulated_orders(id),
  environment TEXT NOT NULL CHECK (environment = 'paper'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  strategy TEXT NOT NULL CHECK (strategy = 'SUPERTREND'),
  symbol TEXT NOT NULL CHECK (symbol = upper(symbol)),
  "interval" TEXT NOT NULL CHECK ("interval" = lower("interval")),
  canonical_reason_code TEXT NOT NULL CHECK (btrim(canonical_reason_code) <> ''),
  raw_reason TEXT NOT NULL CHECK (btrim(raw_reason) <> ''),
  exit_decision_at TIMESTAMPTZ NOT NULL,
  producer_version TEXT NOT NULL CHECK (btrim(producer_version) <> ''),
  content_fingerprint TEXT NOT NULL CHECK (content_fingerprint ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (position_id,simulated_order_id),
  UNIQUE (content_fingerprint)
);

CREATE OR REPLACE FUNCTION prevent_supertrend_exit_intent_mutation_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Supertrend exit intent ledger is immutable and append-only';
END;
$$;

DROP TRIGGER IF EXISTS supertrend_exit_intent_immutable_v1
  ON supertrend_exit_intents_v1;
CREATE TRIGGER supertrend_exit_intent_immutable_v1
BEFORE UPDATE OR DELETE ON supertrend_exit_intents_v1
FOR EACH ROW EXECUTE FUNCTION prevent_supertrend_exit_intent_mutation_v1();

COMMENT ON TABLE supertrend_exit_intents_v1 IS
  'Immutable SUPERTREND PAPER exit decision evidence; contains no PnL, inventory, or terminal state.';

COMMIT;

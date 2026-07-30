BEGIN;

CREATE TABLE IF NOT EXISTS paper_strategy_slot_canary_v1 (
  environment TEXT NOT NULL CHECK (environment = 'paper'),
  deployment_id TEXT NOT NULL
    CHECK (deployment_id IN ('local-paper','vps-paper')),
  strategy TEXT NOT NULL CHECK (strategy = 'SUPERTREND'),
  symbol TEXT NOT NULL CHECK (symbol = upper(symbol) AND btrim(symbol) <> ''),
  "interval" TEXT NOT NULL
    CHECK ("interval" = lower("interval") AND btrim("interval") <> ''),
  enabled BOOLEAN NOT NULL DEFAULT false,
  maximum_entries INTEGER NOT NULL DEFAULT 1 CHECK (maximum_entries = 1),
  accepted_entries_count INTEGER NOT NULL DEFAULT 0
    CHECK (accepted_entries_count BETWEEN 0 AND maximum_entries),
  expires_at TIMESTAMPTZ NOT NULL,
  operator_reason TEXT NOT NULL CHECK (btrim(operator_reason) <> ''),
  changed_by TEXT NOT NULL CHECK (btrim(changed_by) <> ''),
  changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(environment,deployment_id,strategy,symbol,"interval")
);

CREATE TABLE IF NOT EXISTS paper_strategy_slot_canary_audit_v1 (
  audit_id BIGSERIAL PRIMARY KEY,
  environment TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('INSERT','UPDATE')),
  old_row JSONB,
  new_row JSONB NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION audit_paper_strategy_slot_canary_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO paper_strategy_slot_canary_audit_v1(
    environment,deployment_id,strategy,symbol,"interval",
    action,old_row,new_row
  ) VALUES (
    NEW.environment,NEW.deployment_id,NEW.strategy,NEW.symbol,NEW."interval",
    TG_OP,CASE WHEN TG_OP='UPDATE' THEN to_jsonb(OLD) ELSE NULL END,to_jsonb(NEW)
  );
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS paper_strategy_slot_canary_audit_trg_v1
  ON paper_strategy_slot_canary_v1;
CREATE TRIGGER paper_strategy_slot_canary_audit_trg_v1
AFTER INSERT OR UPDATE ON paper_strategy_slot_canary_v1
FOR EACH ROW EXECUTE FUNCTION audit_paper_strategy_slot_canary_v1();

COMMENT ON TABLE paper_strategy_slot_canary_v1 IS
  'Operator-owned, PAPER-only SUPERTREND single-slot canary. Atomic consumption precedes order creation; exits never consult this object.';

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS paper_strategy_entry_gate_v1 (
  environment TEXT NOT NULL CHECK (environment = 'paper'),
  deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
  strategy TEXT NOT NULL CHECK (strategy = upper(strategy)),
  entries_enabled BOOLEAN NOT NULL,
  operator_reason TEXT NOT NULL CHECK (btrim(operator_reason) <> ''),
  changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  changed_by TEXT NOT NULL CHECK (btrim(changed_by) <> ''),
  PRIMARY KEY (environment,deployment_id,strategy)
);

COMMENT ON TABLE paper_strategy_entry_gate_v1 IS
  'Operator-owned PAPER entry-only gate. ORC and bot_control do not own this object; exits are never gated.';

COMMIT;

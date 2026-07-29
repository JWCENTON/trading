BEGIN;

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS inventory_evidence_status TEXT,
  ADD COLUMN IF NOT EXISTS gross_entry_executed_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS entry_base_fee_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS net_entry_inventory_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS cumulative_exit_executed_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS exit_inventory_reduction_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS remaining_inventory_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS terminal_dust_qty NUMERIC,
  ADD COLUMN IF NOT EXISTS terminal_reason TEXT,
  ADD COLUMN IF NOT EXISTS inventory_calculated_at TIMESTAMPTZ;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_positions_inventory_evidence_status_c2_2'
  ) THEN
    ALTER TABLE positions ADD CONSTRAINT ck_positions_inventory_evidence_status_c2_2
      CHECK (
        inventory_evidence_status IS NULL
        OR inventory_evidence_status IN ('COMPLETE', 'INCOMPLETE')
      );
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_positions_inventory_quantities_nonnegative_c2_2'
  ) THEN
    ALTER TABLE positions ADD CONSTRAINT ck_positions_inventory_quantities_nonnegative_c2_2
      CHECK (
        (gross_entry_executed_qty IS NULL OR gross_entry_executed_qty >= 0)
        AND (entry_base_fee_qty IS NULL OR entry_base_fee_qty >= 0)
        AND (net_entry_inventory_qty IS NULL OR net_entry_inventory_qty >= 0)
        AND (cumulative_exit_executed_qty IS NULL OR cumulative_exit_executed_qty >= 0)
        AND (exit_inventory_reduction_qty IS NULL OR exit_inventory_reduction_qty >= 0)
        AND (remaining_inventory_qty IS NULL OR remaining_inventory_qty >= 0)
        AND (terminal_dust_qty IS NULL OR terminal_dust_qty >= 0)
      );
  END IF;
END
$$;

COMMENT ON COLUMN positions.qty IS
  'Compatibility/runtime projection of remaining_inventory_qty for C2.2+ rows.';
COMMENT ON COLUMN positions.terminal_dust_qty IS
  'Authoritative unsold inventory retained when lifecycle closes as TERMINAL_DUST.';

CREATE INDEX IF NOT EXISTS ix_positions_terminal_dust_c2_2
  ON positions (terminal_reason, exit_time DESC)
  WHERE terminal_reason = 'TERMINAL_DUST';

CREATE TABLE IF NOT EXISTS position_lifecycle_events_c2_2 (
  event_id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL REFERENCES positions(id),
  order_id TEXT NOT NULL,
  mutation_kind TEXT NOT NULL CHECK (
    mutation_kind IN (
      'POSITION_REDUCED',
      'POSITION_CLOSED',
      'POSITION_CLOSED_TERMINAL_DUST'
    )
  ),
  mutation_high_water NUMERIC NOT NULL,
  payload JSONB NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  emitted_at TIMESTAMPTZ,
  UNIQUE (position_id, order_id, mutation_kind, mutation_high_water)
);

COMMENT ON TABLE position_lifecycle_events_c2_2 IS
  'Transactional exactly-once lifecycle telemetry outbox for C2.2 mutations.';

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS runtime_contract_adoption_v2 (
  adoption_id BIGSERIAL PRIMARY KEY,
  contract_name TEXT NOT NULL,
  environment TEXT NOT NULL CHECK (environment IN ('live', 'paper')),
  deployment_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  status TEXT NOT NULL CHECK (
    status IN ('PREPARED', 'ACTIVE', 'DEACTIVATED', 'ROLLED_BACK', 'SUPERSEDED')
  ),
  adopted_at TIMESTAMPTZ,
  deactivated_at TIMESTAMPTZ,
  git_revision TEXT NOT NULL,
  migration_version TEXT NOT NULL,
  container_revision TEXT,
  activation_reason TEXT NOT NULL,
  deactivation_reason TEXT,
  supersedes_adoption_id BIGINT REFERENCES runtime_contract_adoption_v2(adoption_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (contract_name, environment, deployment_id, generation),
  CHECK (
    (status = 'ACTIVE' AND adopted_at IS NOT NULL AND deactivated_at IS NULL)
    OR status <> 'ACTIVE'
  ),
  CHECK (
    status NOT IN ('DEACTIVATED', 'ROLLED_BACK', 'SUPERSEDED')
    OR (deactivated_at IS NOT NULL AND deactivation_reason IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_runtime_contract_adoption_v2_active
  ON runtime_contract_adoption_v2(contract_name, environment, deployment_id)
  WHERE status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS ix_runtime_contract_adoption_v2_history
  ON runtime_contract_adoption_v2(
    contract_name, environment, deployment_id, generation DESC
  );

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS inventory_contract_adoption_id BIGINT,
  ADD COLUMN IF NOT EXISTS inventory_contract_generation BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'positions_inventory_contract_adoption_id_fkey'
      AND conrelid = 'positions'::regclass
  ) THEN
    ALTER TABLE positions
      ADD CONSTRAINT positions_inventory_contract_adoption_id_fkey
      FOREIGN KEY (inventory_contract_adoption_id)
      REFERENCES runtime_contract_adoption_v2(adoption_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_positions_inventory_contract_attribution_c2_2_2'
      AND conrelid = 'positions'::regclass
  ) THEN
    ALTER TABLE positions
      ADD CONSTRAINT ck_positions_inventory_contract_attribution_c2_2_2
      CHECK (
        (inventory_contract_adoption_id IS NULL
          AND inventory_contract_generation IS NULL)
        OR
        (inventory_contract_adoption_id IS NOT NULL
          AND inventory_contract_generation IS NOT NULL
          AND inventory_contract_generation > 0)
      );
  END IF;
END $$;

ALTER TABLE exchange_fill_ingestion_state_v2
  ADD COLUMN IF NOT EXISTS adoption_id BIGINT,
  ADD COLUMN IF NOT EXISTS contract_generation BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'exchange_fill_ingestion_state_v2_adoption_id_fkey'
      AND conrelid = 'exchange_fill_ingestion_state_v2'::regclass
  ) THEN
    ALTER TABLE exchange_fill_ingestion_state_v2
      ADD CONSTRAINT exchange_fill_ingestion_state_v2_adoption_id_fkey
      FOREIGN KEY (adoption_id)
      REFERENCES runtime_contract_adoption_v2(adoption_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ck_exchange_fill_generation_attribution_c2_2_2'
      AND conrelid = 'exchange_fill_ingestion_state_v2'::regclass
  ) THEN
    ALTER TABLE exchange_fill_ingestion_state_v2
      ADD CONSTRAINT ck_exchange_fill_generation_attribution_c2_2_2
      CHECK (
        (adoption_id IS NULL AND contract_generation IS NULL)
        OR
        (adoption_id IS NOT NULL AND contract_generation IS NOT NULL
          AND contract_generation > 0)
      );
  END IF;
END $$;

CREATE OR REPLACE FUNCTION prepare_contract_adoption(
  p_contract_name TEXT,
  p_environment TEXT,
  p_deployment_id TEXT,
  p_generation BIGINT,
  p_git_revision TEXT,
  p_migration_version TEXT,
  p_container_revision TEXT,
  p_activation_reason TEXT,
  p_supersedes_adoption_id BIGINT DEFAULT NULL
) RETURNS runtime_contract_adoption_v2
LANGUAGE plpgsql AS $$
DECLARE
  result runtime_contract_adoption_v2;
BEGIN
  IF p_environment NOT IN ('live', 'paper')
     OR NULLIF(btrim(p_deployment_id), '') IS NULL
     OR NULLIF(btrim(p_git_revision), '') IS NULL
     OR p_generation <= 0 THEN
    RAISE EXCEPTION 'ADOPTION_PREPARE_INVALID_TARGET';
  END IF;

  INSERT INTO runtime_contract_adoption_v2(
    contract_name, environment, deployment_id, generation, status,
    git_revision, migration_version, container_revision, activation_reason,
    supersedes_adoption_id
  ) VALUES (
    p_contract_name, p_environment, p_deployment_id, p_generation, 'PREPARED',
    p_git_revision, p_migration_version, p_container_revision,
    p_activation_reason, p_supersedes_adoption_id
  )
  ON CONFLICT (contract_name, environment, deployment_id, generation)
  DO UPDATE SET generation = EXCLUDED.generation
  WHERE runtime_contract_adoption_v2.status = 'PREPARED'
    AND runtime_contract_adoption_v2.git_revision = EXCLUDED.git_revision
    AND runtime_contract_adoption_v2.migration_version =
        EXCLUDED.migration_version
    AND runtime_contract_adoption_v2.container_revision IS NOT DISTINCT FROM
        EXCLUDED.container_revision
  RETURNING * INTO result;

  IF result.adoption_id IS NULL THEN
    RAISE EXCEPTION 'ADOPTION_PREPARE_CONFLICT';
  END IF;
  RETURN result;
END $$;

CREATE OR REPLACE FUNCTION activate_contract_adoption(
  p_adoption_id BIGINT,
  p_expected_git_revision TEXT,
  p_expected_environment TEXT,
  p_expected_deployment_id TEXT
) RETURNS runtime_contract_adoption_v2
LANGUAGE plpgsql AS $$
DECLARE
  result runtime_contract_adoption_v2;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'FEE_AWARE_INVENTORY_C2_2|' || p_expected_environment || '|' ||
    p_expected_deployment_id, 0
  ));

  SELECT * INTO result FROM runtime_contract_adoption_v2
  WHERE adoption_id = p_adoption_id FOR UPDATE;

  IF result.adoption_id IS NULL
     OR result.status <> 'PREPARED'
     OR result.git_revision <> p_expected_git_revision
     OR result.environment <> p_expected_environment
     OR result.deployment_id <> p_expected_deployment_id THEN
    RAISE EXCEPTION 'ADOPTION_ACTIVATION_MISMATCH';
  END IF;
  IF EXISTS (
    SELECT 1 FROM runtime_contract_adoption_v2
    WHERE contract_name = result.contract_name
      AND environment = result.environment
      AND deployment_id = result.deployment_id
      AND status = 'ACTIVE'
      AND adoption_id <> result.adoption_id
  ) THEN
    RAISE EXCEPTION 'ADOPTION_ACTIVE_CONFLICT';
  END IF;

  UPDATE runtime_contract_adoption_v2
  SET status = 'ACTIVE', adopted_at = clock_timestamp()
  WHERE adoption_id = result.adoption_id
  RETURNING * INTO result;
  RETURN result;
END $$;

CREATE OR REPLACE FUNCTION rollback_contract_adoption(
  p_adoption_id BIGINT,
  p_reason TEXT
) RETURNS runtime_contract_adoption_v2
LANGUAGE plpgsql AS $$
DECLARE
  result runtime_contract_adoption_v2;
BEGIN
  IF NULLIF(btrim(p_reason), '') IS NULL THEN
    RAISE EXCEPTION 'ADOPTION_ROLLBACK_REASON_REQUIRED';
  END IF;
  UPDATE runtime_contract_adoption_v2
  SET status = 'ROLLED_BACK', deactivated_at = clock_timestamp(),
      deactivation_reason = p_reason
  WHERE adoption_id = p_adoption_id AND status IN ('PREPARED', 'ACTIVE')
  RETURNING * INTO result;
  IF result.adoption_id IS NULL THEN
    RAISE EXCEPTION 'ADOPTION_ROLLBACK_INVALID_STATE';
  END IF;
  RETURN result;
END $$;

CREATE OR REPLACE FUNCTION supersede_contract_adoption(
  p_adoption_id BIGINT,
  p_superseding_adoption_id BIGINT,
  p_reason TEXT
) RETURNS runtime_contract_adoption_v2
LANGUAGE plpgsql AS $$
DECLARE
  result runtime_contract_adoption_v2;
BEGIN
  IF NULLIF(btrim(p_reason), '') IS NULL
     OR NOT EXISTS (
       SELECT 1 FROM runtime_contract_adoption_v2
       WHERE adoption_id = p_superseding_adoption_id AND status = 'PREPARED'
     ) THEN
    RAISE EXCEPTION 'ADOPTION_SUPERSEDE_INVALID';
  END IF;
  UPDATE runtime_contract_adoption_v2
  SET status = 'SUPERSEDED', deactivated_at = clock_timestamp(),
      deactivation_reason = p_reason
  WHERE adoption_id = p_adoption_id
    AND status IN ('PREPARED', 'DEACTIVATED', 'ROLLED_BACK')
  RETURNING * INTO result;
  IF result.adoption_id IS NULL THEN
    RAISE EXCEPTION 'ADOPTION_SUPERSEDE_INVALID_STATE';
  END IF;
  RETURN result;
END $$;

CREATE OR REPLACE FUNCTION is_existing_projected_c2_2_compatible(
  p_position_id BIGINT,
  p_environment TEXT,
  p_tolerance NUMERIC DEFAULT 0.000000000001
) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE AS $$
DECLARE
  p positions%ROWTYPE;
  entry_ok BOOLEAN := FALSE;
  exit_ok BOOLEAN := FALSE;
BEGIN
  SELECT * INTO p FROM positions WHERE id = p_position_id;
  IF p.id IS NULL
     OR p.inventory_evidence_status <> 'COMPLETE'
     OR p.gross_entry_executed_qty IS NULL
     OR p.entry_base_fee_qty IS NULL
     OR p.net_entry_inventory_qty IS NULL
     OR p.cumulative_exit_executed_qty IS NULL
     OR p.exit_inventory_reduction_qty IS NULL
     OR p.remaining_inventory_qty IS NULL
     OR p.inventory_calculated_at IS NULL
     OR abs(p.qty - p.remaining_inventory_qty) > p_tolerance
     OR abs(p.net_entry_inventory_qty -
       (p.gross_entry_executed_qty - p.entry_base_fee_qty)) > p_tolerance
     OR abs(p.remaining_inventory_qty -
       (p.net_entry_inventory_qty - p.exit_inventory_reduction_qty))
       > p_tolerance
     OR p.gross_entry_executed_qty < p.entry_base_fee_qty
     OR p.net_entry_inventory_qty < p.exit_inventory_reduction_qty THEN
    RETURN FALSE;
  END IF;

  IF lower(p_environment) = 'live' THEN
    IF to_regclass('public.binance_orders') IS NULL
       OR to_regclass('public.binance_order_fills') IS NULL THEN
      RETURN FALSE;
    END IF;
    EXECUTE $live_entry$
      SELECT EXISTS (
        SELECT 1 FROM binance_orders bo
        JOIN binance_order_fills f ON f.order_id = bo.order_id
        WHERE (bo.position_id = $1 OR bo.order_id = $2)
          AND bo.order_purpose = 'ENTRY' AND f.source = 'okx'
          AND f.account_identity_id IS NOT NULL
          AND f.instrument_snapshot_id IS NOT NULL
        GROUP BY bo.position_id
        HAVING abs(sum(f.executed_qty) - $3) <= $5
          AND abs(sum(CASE
            WHEN upper(f.commission_asset) = upper($4)
              THEN f.commission_amount ELSE 0 END
          ) - $6) <= $5
      )
    $live_entry$ INTO entry_ok USING
      p.id, p.entry_order_id, p.gross_entry_executed_qty,
      CASE
        WHEN p.symbol LIKE '%USDC' THEN left(p.symbol, length(p.symbol)-4)
        WHEN p.symbol LIKE '%USDT' THEN left(p.symbol, length(p.symbol)-4)
        ELSE ''
      END,
      p_tolerance, p.entry_base_fee_qty;
    IF p.cumulative_exit_executed_qty = 0 THEN
      exit_ok := TRUE;
    ELSE
      EXECUTE $live_exit$
        SELECT EXISTS (
          SELECT 1 FROM binance_orders bo
          JOIN binance_order_fills f ON f.order_id = bo.order_id
          WHERE bo.position_id = $1 AND bo.order_purpose = 'EXIT'
            AND f.source = 'okx'
          GROUP BY bo.position_id
          HAVING sum(f.executed_qty) + $3 >= $2
            AND abs((
              SELECT COALESCE(sum(x.reconciled_executed_qty), 0)
              FROM binance_orders x
              WHERE x.position_id = $1 AND x.order_purpose = 'EXIT'
            ) - $2) <= $3
        )
      $live_exit$ INTO exit_ok USING
        p.id, p.cumulative_exit_executed_qty, p_tolerance;
    END IF;
  ELSIF lower(p_environment) = 'paper' THEN
    IF to_regclass('public.simulated_execution_fills_v1') IS NULL THEN
      RETURN FALSE;
    END IF;
    SELECT EXISTS (
      SELECT 1 FROM simulated_execution_fills_v1 f
      WHERE f.position_id = p.id AND f.order_purpose = 'ENTRY'
      GROUP BY f.position_id
      HAVING abs(sum(f.fill_qty) - p.gross_entry_executed_qty) <= p_tolerance
        AND abs(sum(CASE
          WHEN upper(f.fee_asset) = upper(
            CASE
              WHEN p.symbol LIKE '%USDC' THEN left(p.symbol,length(p.symbol)-4)
              WHEN p.symbol LIKE '%USDT' THEN left(p.symbol,length(p.symbol)-4)
              ELSE ''
            END
          ) THEN COALESCE(f.fee_qty,0) ELSE 0 END
        ) - p.entry_base_fee_qty) <= p_tolerance
    ) INTO entry_ok;
    IF p.cumulative_exit_executed_qty = 0 THEN
      exit_ok := TRUE;
    ELSE
      SELECT EXISTS (
        SELECT 1 FROM simulated_execution_fills_v1 f
        WHERE f.position_id = p.id AND f.order_purpose = 'EXIT'
        GROUP BY f.position_id
        HAVING sum(f.fill_qty) + p_tolerance >=
          p.cumulative_exit_executed_qty
          AND EXISTS (
            SELECT 1 FROM position_lifecycle_events_c2_2 event
            WHERE event.position_id = p.id
            GROUP BY event.position_id
            HAVING abs(max(event.mutation_high_water) -
              p.cumulative_exit_executed_qty) <= p_tolerance
          )
      ) INTO exit_ok;
    END IF;
  ELSE
    RETURN FALSE;
  END IF;
  RETURN COALESCE(entry_ok, FALSE) AND COALESCE(exit_ok, FALSE);
END;
$$;

COMMENT ON TABLE runtime_contract_adoption_v2 IS
  'Explicit generation lifecycle; migrations never prepare or activate rows.';
COMMENT ON FUNCTION is_existing_projected_c2_2_compatible IS
  'Mode-neutral, evidence-backed compatibility gate for pre-boundary C2.2 rows.';

COMMIT;

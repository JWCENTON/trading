BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION waltrade_uuid_v5_v1(
    p_namespace UUID,
    p_name TEXT
)
RETURNS UUID
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    v_bytes BYTEA;
    v_hex TEXT;
BEGIN
    v_bytes := substring(
        digest(uuid_send(p_namespace) || convert_to(p_name, 'UTF8'), 'sha1')
        FROM 1 FOR 16
    );
    v_bytes := set_byte(v_bytes, 6, (get_byte(v_bytes, 6) & 15) | 80);
    v_bytes := set_byte(v_bytes, 8, (get_byte(v_bytes, 8) & 63) | 128);
    v_hex := encode(v_bytes, 'hex');
    RETURN (
        substring(v_hex, 1, 8) || '-' ||
        substring(v_hex, 9, 4) || '-' ||
        substring(v_hex, 13, 4) || '-' ||
        substring(v_hex, 17, 4) || '-' ||
        substring(v_hex, 21, 12)
    )::UUID;
END;
$$;

CREATE TABLE IF NOT EXISTS decision_identity_runs_v1 (
    run_id UUID PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    lookback_hours INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    source_rows BIGINT NOT NULL DEFAULT 0,
    inserted_rows BIGINT NOT NULL DEFAULT 0,
    updated_rows BIGINT NOT NULL DEFAULT 0,
    duplicate_rows BIGINT NOT NULL DEFAULT 0,
    outcomes_inserted BIGINT NOT NULL DEFAULT 0,
    outcomes_updated BIGINT NOT NULL DEFAULT 0,
    error_text TEXT,
    stats_json JSONB NOT NULL DEFAULT '{}'::JSONB,
    CONSTRAINT ck_decision_identity_run_deployment
        CHECK (deployment_id IN ('LOCAL', 'VPS', 'UNKNOWN')),
    CONSTRAINT ck_decision_identity_run_environment
        CHECK (environment IN ('trading_live', 'trading_paper')),
    CONSTRAINT ck_decision_identity_run_status
        CHECK (status IN ('RUNNING', 'OK', 'FAILED', 'SKIPPED_LOCKED')),
    CONSTRAINT ck_decision_identity_run_lookback
        CHECK (lookback_hours > 0),
    CONSTRAINT ck_decision_identity_run_counts
        CHECK (
            source_rows >= 0 AND inserted_rows >= 0 AND updated_rows >= 0
            AND duplicate_rows >= 0 AND outcomes_inserted >= 0
            AND outcomes_updated >= 0
        )
);

CREATE TABLE IF NOT EXISTS decision_registry_v1 (
    decision_id UUID PRIMARY KEY,
    legacy_decision_key TEXT,
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    decision_source TEXT NOT NULL,
    symbol TEXT,
    interval TEXT,
    strategy TEXT,
    market_regime TEXT,
    decision_timestamp TIMESTAMPTZ NOT NULL,
    source_table TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source_natural_key TEXT NOT NULL,
    source_created_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    engine_name TEXT,
    engine_version TEXT,
    schema_version TEXT NOT NULL,
    decision_action TEXT,
    decision_reason TEXT,
    decision_payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    position_id BIGINT,
    recommendation_id TEXT,
    run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_decision_registry_source_identity
        UNIQUE (
            deployment_id, environment, source_table,
            source_record_id, decision_type
        ),
    CONSTRAINT fk_decision_registry_run
        FOREIGN KEY (run_id) REFERENCES decision_identity_runs_v1(run_id),
    CONSTRAINT ck_decision_registry_deployment
        CHECK (deployment_id IN ('LOCAL', 'VPS', 'UNKNOWN')),
    CONSTRAINT ck_decision_registry_environment
        CHECK (environment IN ('trading_live', 'trading_paper')),
    CONSTRAINT ck_decision_registry_type
        CHECK (decision_type IN (
            'TRADE_EXECUTED', 'NO_TRADE', 'SIGNAL_REJECTED',
            'ENTRY_BLOCKED', 'ENTRY_SUPPRESSED', 'PAPER_SIMULATION'
        )),
    CONSTRAINT ck_decision_registry_source_identity_present
        CHECK (
            btrim(source_table) <> ''
            AND btrim(source_record_id) <> ''
            AND btrim(source_natural_key) <> ''
        ),
    CONSTRAINT ck_decision_registry_trade_position
        CHECK (decision_type <> 'TRADE_EXECUTED' OR position_id IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_decision_registry_trade_position_v1
ON decision_registry_v1(deployment_id, environment, position_id)
WHERE decision_type = 'TRADE_EXECUTED';

CREATE INDEX IF NOT EXISTS ix_decision_registry_slot_time_v1
ON decision_registry_v1(
    deployment_id, environment, symbol, interval, strategy,
    decision_timestamp DESC
);

CREATE INDEX IF NOT EXISTS ix_decision_registry_legacy_key_v1
ON decision_registry_v1(environment, legacy_decision_key)
WHERE legacy_decision_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS decision_outcomes_v1 (
    outcome_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL,
    deployment_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    outcome_type TEXT NOT NULL,
    horizon_minutes INTEGER,
    actual_trade BOOLEAN NOT NULL,
    position_id BIGINT,
    gross_pnl_usdc NUMERIC,
    fees_usdc NUMERIC,
    net_pnl_usdc NUMERIC,
    market_return_pct NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    giveback_pct NUMERIC,
    outcome_status TEXT NOT NULL,
    outcome_reason TEXT,
    source_table TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    engine_name TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
    calculated_at TIMESTAMPTZ NOT NULL,
    run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fk_decision_outcome_decision
        FOREIGN KEY (decision_id) REFERENCES decision_registry_v1(decision_id),
    CONSTRAINT fk_decision_outcome_run
        FOREIGN KEY (run_id) REFERENCES decision_identity_runs_v1(run_id),
    CONSTRAINT uq_decision_outcome_identity
        UNIQUE NULLS NOT DISTINCT (decision_id, outcome_type, horizon_minutes),
    CONSTRAINT uq_decision_outcome_source
        UNIQUE (
            deployment_id, environment, source_table,
            source_record_id, outcome_type
        ),
    CONSTRAINT ck_decision_outcome_deployment
        CHECK (deployment_id IN ('LOCAL', 'VPS', 'UNKNOWN')),
    CONSTRAINT ck_decision_outcome_environment
        CHECK (environment IN ('trading_live', 'trading_paper')),
    CONSTRAINT ck_decision_outcome_type
        CHECK (outcome_type IN (
            'ACTUAL_TRADE', 'FORWARD_15M', 'FORWARD_30M', 'FORWARD_60M'
        )),
    CONSTRAINT ck_decision_outcome_status
        CHECK (outcome_status IN ('COMPLETE', 'PARTIAL', 'PENDING_REPLAY')),
    CONSTRAINT ck_decision_outcome_horizon
        CHECK (
            (outcome_type = 'ACTUAL_TRADE' AND horizon_minutes IS NULL)
            OR (outcome_type = 'FORWARD_15M' AND horizon_minutes = 15)
            OR (outcome_type = 'FORWARD_30M' AND horizon_minutes = 30)
            OR (outcome_type = 'FORWARD_60M' AND horizon_minutes = 60)
        ),
    CONSTRAINT ck_decision_outcome_actual_trade
        CHECK (
            outcome_type <> 'ACTUAL_TRADE'
            OR (actual_trade IS TRUE AND position_id IS NOT NULL)
        ),
    CONSTRAINT ck_decision_outcome_source_present
        CHECK (btrim(source_table) <> '' AND btrim(source_record_id) <> '')
);

CREATE INDEX IF NOT EXISTS ix_decision_outcomes_decision_v1
ON decision_outcomes_v1(decision_id, outcome_type);

CREATE INDEX IF NOT EXISTS ix_decision_outcomes_status_v1
ON decision_outcomes_v1(deployment_id, environment, outcome_status);

CREATE OR REPLACE FUNCTION refresh_decision_identity_outcome_v1(
    p_lookback_hours INTEGER,
    p_environment TEXT,
    p_deployment_id TEXT,
    p_run_id UUID DEFAULT gen_random_uuid()
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_namespace CONSTANT UUID := 'c966214a-6a82-50e9-913b-5144237cdf44';
    v_outcome_namespace CONSTANT UUID := 'f955f1a5-e3ea-51cf-9178-b7c838b609a4';
    v_lock_key BIGINT;
    v_source_rows BIGINT := 0;
    v_inserted BIGINT := 0;
    v_updated BIGINT := 0;
    v_duplicates BIGINT := 0;
    v_outcomes_inserted BIGINT := 0;
    v_outcomes_updated BIGINT := 0;
    v_result JSONB;
BEGIN
    IF p_lookback_hours IS NULL OR p_lookback_hours <= 0 THEN
        RAISE EXCEPTION 'lookback_hours must be positive';
    END IF;
    IF p_environment NOT IN ('trading_live', 'trading_paper') THEN
        RAISE EXCEPTION 'unsupported environment: %', p_environment;
    END IF;
    IF p_deployment_id NOT IN ('LOCAL', 'VPS', 'UNKNOWN') THEN
        RAISE EXCEPTION 'unsupported deployment_id: %', p_deployment_id;
    END IF;
    IF current_database() <> p_environment THEN
        RAISE EXCEPTION 'environment/database mismatch: parameter=% database=%',
            p_environment, current_database();
    END IF;

    v_lock_key := hashtextextended(
        'decision_identity_outcome_v1|' || p_deployment_id || '|' || p_environment,
        0
    );

    IF NOT pg_try_advisory_xact_lock(v_lock_key) THEN
        INSERT INTO decision_identity_runs_v1 (
            run_id, deployment_id, environment, engine_version,
            schema_version, lookback_hours, finished_at, status, stats_json
        ) VALUES (
            p_run_id, p_deployment_id, p_environment,
            'DECISION_IDENTITY_OUTCOME_V1', 'DECISION_SSOT_V1',
            p_lookback_hours, clock_timestamp(), 'SKIPPED_LOCKED',
            jsonb_build_object('status', 'SKIPPED_LOCKED')
        );
        RETURN jsonb_build_object('status', 'SKIPPED_LOCKED', 'run_id', p_run_id);
    END IF;

    INSERT INTO decision_identity_runs_v1 (
        run_id, deployment_id, environment, engine_version,
        schema_version, lookback_hours, status
    ) VALUES (
        p_run_id, p_deployment_id, p_environment,
        'DECISION_IDENTITY_OUTCOME_V1', 'DECISION_SSOT_V1',
        p_lookback_hours, 'RUNNING'
    );

    BEGIN
    SELECT count(*) INTO v_source_rows
    FROM positions p
    WHERE p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours);

    WITH source_rows AS (
        SELECT
            p.*,
            p.id::TEXT AS source_id,
            concat_ws('|', p_deployment_id, p_environment, 'positions',
                p.id::TEXT, 'TRADE_EXECUTED') AS identity_payload,
            (
                SELECT CASE WHEN count(DISTINCT w.decision_key) = 1
                    THEN min(w.decision_key) END
                FROM learning_feature_warehouse_v1 w
                WHERE w.environment = p_environment
                  AND w.position_id = p.id
            ) AS legacy_key,
            EXISTS (
                SELECT 1 FROM decision_registry_v1 d
                WHERE d.deployment_id = p_deployment_id
                  AND d.environment = p_environment
                  AND d.source_table = 'positions'
                  AND d.source_record_id = p.id::TEXT
                  AND d.decision_type = 'TRADE_EXECUTED'
            ) AS existed
        FROM positions p
        WHERE p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)
    ), upserted AS (
        INSERT INTO decision_registry_v1 (
            decision_id, legacy_decision_key, deployment_id, environment,
            decision_type, decision_source, symbol, interval, strategy,
            market_regime, decision_timestamp, source_table, source_record_id,
            source_natural_key, source_created_at, observed_at, engine_name,
            engine_version, schema_version, decision_action, decision_reason,
            decision_payload, position_id, run_id, refreshed_at
        )
        SELECT
            waltrade_uuid_v5_v1(v_namespace, s.identity_payload),
            s.legacy_key, p_deployment_id, p_environment,
            'TRADE_EXECUTED', 'POSITION', s.symbol, s.interval, s.strategy,
            s.market_regime, s.entry_time, 'positions', s.source_id,
            s.identity_payload, s.entry_time, clock_timestamp(),
            'POSITION_REGISTRY_ADAPTER', 'DECISION_IDENTITY_OUTCOME_V1',
            'DECISION_SSOT_V1', s.side, s.exit_reason,
            jsonb_build_object(
                'position_status', s.status,
                'entry_order_id', s.entry_order_id,
                'entry_client_order_id', s.entry_client_order_id,
                'entry_price', s.entry_price,
                'exit_time', s.exit_time
            ),
            s.id, p_run_id, clock_timestamp()
        FROM source_rows s
        ON CONFLICT (
            deployment_id, environment, source_table,
            source_record_id, decision_type
        ) DO UPDATE SET
            legacy_decision_key = EXCLUDED.legacy_decision_key,
            symbol = EXCLUDED.symbol,
            interval = EXCLUDED.interval,
            strategy = EXCLUDED.strategy,
            market_regime = EXCLUDED.market_regime,
            decision_action = EXCLUDED.decision_action,
            decision_reason = EXCLUDED.decision_reason,
            decision_payload = EXCLUDED.decision_payload,
            position_id = EXCLUDED.position_id,
            run_id = EXCLUDED.run_id,
            refreshed_at = clock_timestamp()
        RETURNING (xmax = 0) AS inserted
    )
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted)
    INTO v_inserted, v_updated
    FROM upserted;

    SELECT count(*) INTO v_duplicates
    FROM (
        SELECT deployment_id, environment, source_table, source_record_id,
               decision_type
        FROM decision_registry_v1
        GROUP BY 1, 2, 3, 4, 5
        HAVING count(*) > 1
    ) q;

    WITH source_outcomes AS (
        SELECT
            d.decision_id,
            d.deployment_id,
            d.environment,
            d.position_id,
            p.gross_pnl_usdc,
            p.fees_usdc,
            p.net_pnl_usdc,
            e.mfe_pct,
            e.mae_pct,
            e.giveback_pct,
            p.exit_time,
            p.exit_reason,
            concat_ws('|', d.decision_id::TEXT, 'ACTUAL_TRADE',
                'positions', p.id::TEXT) AS outcome_payload
        FROM decision_registry_v1 d
        JOIN positions p ON p.id = d.position_id
        LEFT JOIN exit_trace_v1 e ON e.position_id = p.id
        WHERE d.deployment_id = p_deployment_id
          AND d.environment = p_environment
          AND d.decision_type = 'TRADE_EXECUTED'
          AND p.exit_time IS NOT NULL
          AND p.entry_time >= clock_timestamp() - make_interval(hours => p_lookback_hours)
    ), upserted AS (
        INSERT INTO decision_outcomes_v1 (
            outcome_id, decision_id, deployment_id, environment,
            outcome_type, horizon_minutes, actual_trade, position_id,
            gross_pnl_usdc, fees_usdc, net_pnl_usdc, mfe_pct, mae_pct,
            giveback_pct, outcome_status, outcome_reason, source_table,
            source_record_id, engine_name, engine_version, schema_version,
            evidence, calculated_at, run_id, refreshed_at
        )
        SELECT
            waltrade_uuid_v5_v1(v_outcome_namespace, s.outcome_payload),
            s.decision_id, s.deployment_id, s.environment,
            'ACTUAL_TRADE', NULL, TRUE, s.position_id,
            s.gross_pnl_usdc, s.fees_usdc, s.net_pnl_usdc,
            s.mfe_pct, s.mae_pct, s.giveback_pct,
            CASE WHEN s.net_pnl_usdc IS NULL THEN 'PARTIAL' ELSE 'COMPLETE' END,
            CASE WHEN s.net_pnl_usdc IS NULL
                THEN 'Closed position has incomplete net PnL'
                ELSE s.exit_reason END,
            'positions', s.position_id::TEXT,
            'ACTUAL_TRADE_OUTCOME_ADAPTER', 'DECISION_IDENTITY_OUTCOME_V1',
            'DECISION_SSOT_V1',
            jsonb_build_object(
                'exit_time', s.exit_time,
                'exit_reason', s.exit_reason,
                'path_source', CASE WHEN s.mfe_pct IS NULL AND s.mae_pct IS NULL
                    THEN 'missing' ELSE 'exit_trace_v1' END
            ),
            clock_timestamp(), p_run_id, clock_timestamp()
        FROM source_outcomes s
        ON CONFLICT (decision_id, outcome_type, horizon_minutes) DO UPDATE SET
            gross_pnl_usdc = EXCLUDED.gross_pnl_usdc,
            fees_usdc = EXCLUDED.fees_usdc,
            net_pnl_usdc = EXCLUDED.net_pnl_usdc,
            mfe_pct = EXCLUDED.mfe_pct,
            mae_pct = EXCLUDED.mae_pct,
            giveback_pct = EXCLUDED.giveback_pct,
            outcome_status = EXCLUDED.outcome_status,
            outcome_reason = EXCLUDED.outcome_reason,
            evidence = EXCLUDED.evidence,
            calculated_at = EXCLUDED.calculated_at,
            run_id = EXCLUDED.run_id,
            refreshed_at = clock_timestamp()
        RETURNING (xmax = 0) AS inserted
    )
    SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted)
    INTO v_outcomes_inserted, v_outcomes_updated
    FROM upserted;

    v_result := jsonb_build_object(
        'status', 'OK', 'run_id', p_run_id,
        'deployment_id', p_deployment_id, 'environment', p_environment,
        'lookback_hours', p_lookback_hours,
        'source_rows', v_source_rows,
        'inserted_rows', v_inserted, 'updated_rows', v_updated,
        'duplicate_rows', v_duplicates,
        'outcomes_inserted', v_outcomes_inserted,
        'outcomes_updated', v_outcomes_updated
    );

    UPDATE decision_identity_runs_v1
    SET finished_at = clock_timestamp(), status = 'OK',
        source_rows = v_source_rows, inserted_rows = v_inserted,
        updated_rows = v_updated, duplicate_rows = v_duplicates,
        outcomes_inserted = v_outcomes_inserted,
        outcomes_updated = v_outcomes_updated, stats_json = v_result
    WHERE run_id = p_run_id;

    RETURN v_result;
EXCEPTION WHEN OTHERS THEN
    IF EXISTS (SELECT 1 FROM decision_identity_runs_v1 WHERE run_id = p_run_id) THEN
        UPDATE decision_identity_runs_v1
        SET finished_at = clock_timestamp(), status = 'FAILED',
            error_text = SQLSTATE || ': ' || SQLERRM,
            stats_json = jsonb_build_object(
                'status', 'FAILED', 'sqlstate', SQLSTATE, 'error', SQLERRM
            )
        WHERE run_id = p_run_id;
        RETURN jsonb_build_object(
            'status', 'FAILED', 'run_id', p_run_id,
            'sqlstate', SQLSTATE, 'error', SQLERRM
        );
    END IF;
    RAISE;
    END;
END;
$$;

CREATE OR REPLACE VIEW v_decision_registry_v1_summary AS
SELECT
    d.deployment_id,
    d.environment,
    d.decision_type,
    count(*) AS decisions,
    count(*) FILTER (WHERE legacy_decision_key IS NOT NULL) AS with_legacy_key,
    count(*) FILTER (WHERE o.decision_id IS NOT NULL) AS with_outcome,
    count(*) FILTER (WHERE o.decision_id IS NULL) AS without_outcome,
    min(decision_timestamp) AS oldest_decision,
    max(decision_timestamp) AS newest_decision
FROM decision_registry_v1 d
LEFT JOIN decision_outcomes_v1 o USING (decision_id)
GROUP BY d.deployment_id, d.environment, d.decision_type;

CREATE OR REPLACE VIEW v_decision_registry_v1_duplicates AS
SELECT
    'SOURCE_IDENTITY'::TEXT AS violation_type,
    deployment_id,
    environment,
    source_table,
    source_record_id,
    decision_type,
    NULL::BIGINT AS position_id,
    count(*) AS duplicate_count
FROM decision_registry_v1
GROUP BY 2, 3, 4, 5, 6
HAVING count(*) > 1
UNION ALL
SELECT
    'TRADE_POSITION', deployment_id, environment, 'positions',
    position_id::TEXT, decision_type, position_id, count(*)
FROM decision_registry_v1
WHERE decision_type = 'TRADE_EXECUTED'
GROUP BY deployment_id, environment, position_id, decision_type
HAVING count(*) > 1;

CREATE OR REPLACE VIEW v_decision_outcome_coverage_v1 AS
SELECT
    d.deployment_id,
    d.environment,
    d.decision_type,
    count(*) AS decisions,
    count(*) FILTER (WHERE o.outcome_id IS NOT NULL) AS outcomes,
    count(*) FILTER (WHERE o.outcome_id IS NULL) AS missing_outcomes,
    count(*) FILTER (WHERE o.outcome_status = 'COMPLETE') AS complete_outcomes,
    count(*) FILTER (WHERE o.outcome_status = 'PARTIAL') AS partial_outcomes
FROM decision_registry_v1 d
LEFT JOIN decision_outcomes_v1 o ON o.decision_id = d.decision_id
GROUP BY d.deployment_id, d.environment, d.decision_type;

CREATE OR REPLACE VIEW v_decision_identity_environment_audit_v1 AS
SELECT 'UNKNOWN_DEPLOYMENT'::TEXT AS violation_type, decision_id,
       deployment_id, environment, source_table, source_record_id,
       'deployment_id is UNKNOWN'::TEXT AS detail
FROM decision_registry_v1 WHERE deployment_id = 'UNKNOWN'
UNION ALL
SELECT 'DATABASE_ENVIRONMENT_MISMATCH', decision_id,
       deployment_id, environment, source_table, source_record_id,
       format('row=%s database=%s', environment, current_database())
FROM decision_registry_v1 WHERE environment <> current_database()
UNION ALL
SELECT 'OUTCOME_ENVIRONMENT_MISMATCH', d.decision_id,
       d.deployment_id, d.environment, d.source_table, d.source_record_id,
       format('outcome deployment/environment=%s/%s', o.deployment_id, o.environment)
FROM decision_registry_v1 d
JOIN decision_outcomes_v1 o ON o.decision_id = d.decision_id
WHERE (o.deployment_id, o.environment) IS DISTINCT FROM
      (d.deployment_id, d.environment)
UNION ALL
SELECT 'MISSING_SOURCE_IDENTITY', decision_id,
       deployment_id, environment, source_table, source_record_id,
       'source identity is blank'
FROM decision_registry_v1
WHERE btrim(source_table) = '' OR btrim(source_record_id) = ''
   OR btrim(source_natural_key) = '';

COMMENT ON TABLE decision_registry_v1 IS
'Neutral universal decision registry. V1 ingests TRADE_EXECUTED from positions only; it is independent of recommendations.';
COMMENT ON TABLE decision_outcomes_v1 IS
'Independent decision outcome SSOT. V1 stores ACTUAL_TRADE outcomes only and never writes source/runtime tables.';
COMMENT ON FUNCTION refresh_decision_identity_outcome_v1(INTEGER, TEXT, TEXT, UUID) IS
'Idempotently imports positions and closed-position outcomes into new SSOT tables. Transaction-scoped advisory lock; no internal commit.';

COMMIT;

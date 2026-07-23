BEGIN;

DO $prerequisites$
BEGIN
    IF current_database() IS DISTINCT FROM 'trading_live'
       OR current_setting(
            'waltrade.deployment_instance_id', true
          ) IS DISTINCT FROM 'local'
       OR current_setting(
            'waltrade.environment', true
          ) IS DISTINCT FROM 'live' THEN
        RAISE EXCEPTION
            'LEARNING_98B4_REPAIR_RUNTIME_IDENTITY_MISMATCH database=% instance=% environment=%',
            current_database(),
            COALESCE(current_setting(
                'waltrade.deployment_instance_id', true), '<missing>'),
            COALESCE(current_setting(
                'waltrade.environment', true), '<missing>');
    END IF;
    IF to_regclass('public.positions') IS NULL
       OR to_regclass('public.binance_order_fills') IS NULL
       OR to_regclass('public.exit_trace_v1') IS NULL
       OR to_regclass('public.learning_feature_warehouse_v1') IS NULL
       OR to_regclass('public.decision_registry_v1') IS NULL
       OR to_regclass('public.decision_outcomes_v1') IS NULL THEN
        RAISE EXCEPTION
            'LEARNING_98B4_REPAIR_PREREQUISITE_MISSING';
    END IF;
END;
$prerequisites$;

CREATE TABLE IF NOT EXISTS learning_decision_identity_repairs_v1 (
    repair_id UUID PRIMARY KEY,
    repair_migration TEXT NOT NULL,
    repair_version TEXT NOT NULL,
    decision_key TEXT NOT NULL UNIQUE,
    decision_id UUID NOT NULL UNIQUE,
    outcome_id UUID NOT NULL UNIQUE,
    position_id BIGINT NOT NULL,
    source_natural_key TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL CHECK (length(source_fingerprint) = 64),
    registry_fingerprint TEXT NOT NULL CHECK (length(registry_fingerprint) = 64),
    outcome_fingerprint TEXT NOT NULL CHECK (length(outcome_fingerprint) = 64),
    deployment_instance_id TEXT NOT NULL,
    runtime_environment TEXT NOT NULL,
    runtime_deployment_id TEXT NOT NULL,
    legacy_deployment_id TEXT NOT NULL,
    legacy_environment TEXT NOT NULL,
    classification TEXT NOT NULL,
    idempotency_status TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION prevent_learning_decision_identity_repair_mutation_v1()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'learning decision identity repair audit is immutable';
END;
$$;

DROP TRIGGER IF EXISTS learning_decision_identity_repairs_immutable_v1
    ON learning_decision_identity_repairs_v1;
CREATE TRIGGER learning_decision_identity_repairs_immutable_v1
BEFORE UPDATE OR DELETE ON learning_decision_identity_repairs_v1
FOR EACH ROW
EXECUTE FUNCTION prevent_learning_decision_identity_repair_mutation_v1();

DO $repair$
DECLARE
    v_repair_id CONSTANT UUID :=
        '72e73dc9-8b2d-572f-bef9-1fc18a877adf';
    v_decision_key CONSTANT TEXT :=
        '98b4eb54128ca4800d8cc91499026e7f';
    v_decision_id CONSTANT UUID :=
        '2cf22538-41ff-5be3-ab51-40cbb9f468e1';
    v_outcome_id CONSTANT UUID :=
        '46821b51-7075-593b-8166-3d39f923e391';
    v_position_id CONSTANT BIGINT := 3078;
    v_source_natural_key CONSTANT TEXT :=
        'LOCAL|trading_live|positions|3078|TRADE_EXECUTED';
    v_expected_source_fingerprint CONSTANT TEXT :=
        '6f0f4eac62fa11101db0c4e461f80cc7241e79d81538ac339f02187d24e4ac5c';
    v_expected_registry_fingerprint CONSTANT TEXT :=
        '0e2757c85002470fb88460f547e7bf0c52fba781e8d619d177e63f2c30606f17';
    v_expected_outcome_fingerprint CONSTANT TEXT :=
        'bcac6eefec889aa4603fe247d1045a7a28797fce72900872122a71e1469758d4';
    v_position positions%ROWTYPE;
    v_mfe NUMERIC;
    v_mae NUMERIC;
    v_giveback NUMERIC;
    v_source_fingerprint TEXT;
    v_registry_fingerprint TEXT;
    v_outcome_fingerprint TEXT;
    v_registry_inserted BOOLEAN := false;
    v_outcome_inserted BOOLEAN := false;
BEGIN
    SELECT * INTO STRICT v_position
      FROM positions WHERE id = v_position_id;

    IF v_position.status <> 'CLOSED'
       OR v_position.symbol <> 'SOLUSDC'
       OR v_position.interval <> '5m'
       OR v_position.strategy <> 'TREND'
       OR v_position.side <> 'LONG'
       OR v_position.entry_time IS DISTINCT FROM
            '2026-07-12 16:10:26.711057+00'::TIMESTAMPTZ
       OR v_position.exit_time IS DISTINCT FROM
            '2026-07-12 17:55:59.341506+00'::TIMESTAMPTZ
       OR v_position.entry_order_id <> '3736964691072163840'
       OR v_position.exit_order_id <> '3737177178304454656'
       OR v_position.entry_client_order_id <>
            'ORC-L-SOLUSDC-TREN-5m-E-4d06da4e'
       OR v_position.exit_client_order_id <> 'ORC-L-SOLUSDC-P3078-X'
       OR v_position.net_pnl_usdc IS DISTINCT FROM (-0.07277221)::NUMERIC
       OR v_position.gross_pnl_usdc IS DISTINCT FROM (-0.05679520)::NUMERIC
       OR v_position.fees_usdc IS DISTINCT FROM 0.01597701::NUMERIC THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_SOURCE_POSITION_MISMATCH';
    END IF;

    IF (SELECT count(*) FROM binance_order_fills
         WHERE order_id = '3737177178304454656'
           AND trade_id = 905390
           AND symbol = 'SOLUSDC'
           AND side = 'SELL') <> 1 THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_SOURCE_TRADE_MISMATCH';
    END IF;

    IF (SELECT count(DISTINCT decision_key)
          FROM learning_feature_warehouse_v1
         WHERE environment = 'trading_live'
           AND position_id = v_position_id) <> 1
       OR NOT EXISTS (
           SELECT 1 FROM learning_feature_warehouse_v1
            WHERE environment = 'trading_live'
              AND position_id = v_position_id
              AND decision_key = v_decision_key
              AND net_pnl_usdc IS NOT DISTINCT FROM
                    v_position.net_pnl_usdc
       ) THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_WAREHOUSE_LINK_MISMATCH';
    END IF;

    SELECT mfe_pct, mae_pct, giveback_pct
      INTO STRICT v_mfe, v_mae, v_giveback
      FROM exit_trace_v1
     WHERE position_id = v_position_id;

    v_source_fingerprint := encode(digest(
        jsonb_build_array(
            v_position.id, v_position.symbol, v_position.interval,
            v_position.strategy, v_position.side, v_position.entry_time,
            v_position.exit_time, v_position.gross_pnl_usdc,
            v_position.fees_usdc, v_position.net_pnl_usdc,
            v_decision_key
        )::TEXT, 'sha256'), 'hex');
    v_registry_fingerprint := encode(digest(jsonb_build_object(
        'decision_id', v_decision_id,
        'legacy_decision_key', v_decision_key,
        'deployment_id', 'LOCAL',
        'environment', 'trading_live',
        'source_natural_key', v_source_natural_key,
        'position_id', v_position_id
    )::TEXT, 'sha256'), 'hex');
    v_outcome_fingerprint := encode(digest(jsonb_build_object(
        'outcome_id', v_outcome_id,
        'decision_id', v_decision_id,
        'position_id', v_position_id,
        'gross_pnl_usdc', v_position.gross_pnl_usdc,
        'fees_usdc', v_position.fees_usdc,
        'net_pnl_usdc', v_position.net_pnl_usdc,
        'mfe_pct', v_mfe, 'mae_pct', v_mae,
        'giveback_pct', v_giveback
    )::TEXT, 'sha256'), 'hex');

    IF v_source_fingerprint <> v_expected_source_fingerprint
       OR v_registry_fingerprint <> v_expected_registry_fingerprint
       OR v_outcome_fingerprint <> v_expected_outcome_fingerprint THEN
        RAISE EXCEPTION
            'LEARNING_98B4_REPAIR_SOURCE_FINGERPRINT_MISMATCH source=% registry=% outcome=%',
            v_source_fingerprint, v_registry_fingerprint,
            v_outcome_fingerprint;
    END IF;

    IF EXISTS (
        SELECT 1 FROM decision_registry_v1 r
         WHERE (
             r.decision_id = v_decision_id
             OR r.legacy_decision_key = v_decision_key
             OR r.position_id = v_position_id
             OR r.source_natural_key = v_source_natural_key
         )
           AND NOT (
             r.decision_id = v_decision_id
             AND r.legacy_decision_key = v_decision_key
             AND r.deployment_id = 'LOCAL'
             AND r.environment = 'trading_live'
             AND r.position_id = v_position_id
             AND r.source_natural_key = v_source_natural_key
           )
    ) THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_REGISTRY_CONFLICT';
    END IF;

    IF EXISTS (
        SELECT 1 FROM decision_outcomes_v1 o
         WHERE (
             o.outcome_id = v_outcome_id
             OR o.decision_id = v_decision_id
             OR o.position_id = v_position_id
         )
           AND NOT (
             o.outcome_id = v_outcome_id
             AND o.decision_id = v_decision_id
             AND o.deployment_id = 'LOCAL'
             AND o.environment = 'trading_live'
             AND o.position_id = v_position_id
             AND o.outcome_type = 'ACTUAL_TRADE'
             AND o.net_pnl_usdc IS NOT DISTINCT FROM
                    v_position.net_pnl_usdc
           )
    ) THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_OUTCOME_CONFLICT';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM decision_registry_v1
         WHERE decision_id = v_decision_id
    ) THEN
        INSERT INTO decision_registry_v1 (
            decision_id, legacy_decision_key, deployment_id, environment,
            decision_type, decision_source, symbol, interval, strategy,
            market_regime, decision_timestamp, source_table, source_record_id,
            source_natural_key, source_created_at, observed_at, ingested_at,
            engine_name, engine_version, schema_version, decision_action,
            decision_reason, decision_payload, position_id, refreshed_at
        ) VALUES (
            v_decision_id, v_decision_key, 'LOCAL', 'trading_live',
            'TRADE_EXECUTED', 'POSITION', v_position.symbol,
            v_position.interval, v_position.strategy,
            v_position.market_regime, v_position.entry_time,
            'positions', v_position_id::TEXT, v_source_natural_key,
            v_position.entry_time, clock_timestamp(), clock_timestamp(),
            'EXPLICIT_ORPHAN_REPAIR_V1',
            'LEARNING_DECISION_98B4_REPAIR_V1', 'DECISION_SSOT_V1',
            v_position.side, v_position.exit_reason,
            jsonb_build_object(
                'position_status', v_position.status,
                'entry_order_id', v_position.entry_order_id,
                'entry_client_order_id', v_position.entry_client_order_id,
                'entry_price', v_position.entry_price,
                'exit_time', v_position.exit_time,
                'repair_source_fingerprint', v_source_fingerprint
            ), v_position_id, clock_timestamp()
        );
        v_registry_inserted := true;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM decision_outcomes_v1
         WHERE outcome_id = v_outcome_id
    ) THEN
        INSERT INTO decision_outcomes_v1 (
            outcome_id, decision_id, deployment_id, environment,
            outcome_type, actual_trade, position_id, gross_pnl_usdc,
            fees_usdc, net_pnl_usdc, mfe_pct, mae_pct, giveback_pct,
            outcome_status, outcome_reason, source_table, source_record_id,
            engine_name, engine_version, schema_version, evidence,
            calculated_at, refreshed_at
        ) VALUES (
            v_outcome_id, v_decision_id, 'LOCAL', 'trading_live',
            'ACTUAL_TRADE', true, v_position_id,
            v_position.gross_pnl_usdc, v_position.fees_usdc,
            v_position.net_pnl_usdc, v_mfe, v_mae, v_giveback,
            'COMPLETE', v_position.exit_reason, 'positions',
            v_position_id::TEXT, 'EXPLICIT_ORPHAN_REPAIR_V1',
            'LEARNING_DECISION_98B4_REPAIR_V1', 'DECISION_SSOT_V1',
            jsonb_build_object(
                'exit_time', v_position.exit_time,
                'exit_reason', v_position.exit_reason,
                'path_source', 'exit_trace_v1',
                'repair_source_fingerprint', v_source_fingerprint
            ), clock_timestamp(), clock_timestamp()
        );
        v_outcome_inserted := true;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM decision_registry_v1 r
         WHERE r.decision_id = v_decision_id
           AND r.legacy_decision_key = v_decision_key
           AND r.deployment_id = 'LOCAL'
           AND r.environment = 'trading_live'
           AND r.decision_type = 'TRADE_EXECUTED'
           AND r.decision_source = 'POSITION'
           AND r.symbol = v_position.symbol
           AND r.interval = v_position.interval
           AND r.strategy = v_position.strategy
           AND r.market_regime IS NOT DISTINCT FROM v_position.market_regime
           AND r.decision_timestamp IS NOT DISTINCT FROM v_position.entry_time
           AND r.source_table = 'positions'
           AND r.source_record_id = v_position_id::TEXT
           AND r.source_natural_key = v_source_natural_key
           AND r.source_created_at IS NOT DISTINCT FROM v_position.entry_time
           AND r.engine_name = 'EXPLICIT_ORPHAN_REPAIR_V1'
           AND r.engine_version = 'LEARNING_DECISION_98B4_REPAIR_V1'
           AND r.schema_version = 'DECISION_SSOT_V1'
           AND r.decision_action = v_position.side
           AND r.decision_reason IS NOT DISTINCT FROM v_position.exit_reason
           AND r.position_id = v_position_id
           AND r.decision_payload @> jsonb_build_object(
                'position_status', v_position.status,
                'entry_order_id', v_position.entry_order_id,
                'entry_client_order_id', v_position.entry_client_order_id,
                'entry_price', v_position.entry_price,
                'exit_time', v_position.exit_time,
                'repair_source_fingerprint', v_source_fingerprint
           )
    ) THEN
        RAISE EXCEPTION
            'LEARNING_98B4_REPAIR_EXISTING_REGISTRY_NOT_IDENTICAL';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM decision_outcomes_v1 o
         WHERE o.outcome_id = v_outcome_id
           AND o.decision_id = v_decision_id
           AND o.deployment_id = 'LOCAL'
           AND o.environment = 'trading_live'
           AND o.outcome_type = 'ACTUAL_TRADE'
           AND o.actual_trade
           AND o.position_id = v_position_id
           AND o.gross_pnl_usdc IS NOT DISTINCT FROM
                v_position.gross_pnl_usdc
           AND o.fees_usdc IS NOT DISTINCT FROM v_position.fees_usdc
           AND o.net_pnl_usdc IS NOT DISTINCT FROM v_position.net_pnl_usdc
           AND o.mfe_pct IS NOT DISTINCT FROM v_mfe
           AND o.mae_pct IS NOT DISTINCT FROM v_mae
           AND o.giveback_pct IS NOT DISTINCT FROM v_giveback
           AND o.outcome_status = 'COMPLETE'
           AND o.outcome_reason IS NOT DISTINCT FROM v_position.exit_reason
           AND o.source_table = 'positions'
           AND o.source_record_id = v_position_id::TEXT
           AND o.engine_name = 'EXPLICIT_ORPHAN_REPAIR_V1'
           AND o.engine_version = 'LEARNING_DECISION_98B4_REPAIR_V1'
           AND o.schema_version = 'DECISION_SSOT_V1'
           AND o.evidence @> jsonb_build_object(
                'exit_time', v_position.exit_time,
                'exit_reason', v_position.exit_reason,
                'path_source', 'exit_trace_v1',
                'repair_source_fingerprint', v_source_fingerprint
           )
    ) THEN
        RAISE EXCEPTION
            'LEARNING_98B4_REPAIR_EXISTING_OUTCOME_NOT_IDENTICAL';
    END IF;

    INSERT INTO learning_decision_identity_repairs_v1 (
        repair_id, repair_migration, repair_version, decision_key,
        decision_id, outcome_id, position_id, source_natural_key,
        source_fingerprint, registry_fingerprint, outcome_fingerprint,
        deployment_instance_id, runtime_environment,
        runtime_deployment_id, legacy_deployment_id,
        legacy_environment, classification, idempotency_status
    ) VALUES (
        v_repair_id,
        '20260723_learning_decision_98b4_repair_v1.sql',
        'LEARNING_DECISION_98B4_REPAIR_V1', v_decision_key,
        v_decision_id, v_outcome_id, v_position_id,
        v_source_natural_key, v_source_fingerprint,
        v_registry_fingerprint, v_outcome_fingerprint,
        'local', 'live', 'local-live', 'LOCAL', 'trading_live',
        'PRODUCER_PIPELINE_INTEGRITY_FAILURE',
        CASE WHEN v_registry_inserted AND v_outcome_inserted
             THEN 'INSERTED'
             ELSE 'EXISTING_IDENTICAL' END
    ) ON CONFLICT (repair_id) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1 FROM learning_decision_identity_repairs_v1 a
         WHERE a.repair_id = v_repair_id
           AND a.decision_key = v_decision_key
           AND a.decision_id = v_decision_id
           AND a.outcome_id = v_outcome_id
           AND a.source_fingerprint = v_source_fingerprint
           AND a.registry_fingerprint = v_registry_fingerprint
           AND a.outcome_fingerprint = v_outcome_fingerprint
           AND a.deployment_instance_id = 'local'
           AND a.runtime_environment = 'live'
           AND a.runtime_deployment_id = 'local-live'
    ) THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_AUDIT_CONFLICT';
    END IF;

    IF (SELECT count(*) FROM decision_registry_v1
         WHERE decision_id = v_decision_id) <> 1
       OR (SELECT count(*) FROM decision_outcomes_v1
            WHERE outcome_id = v_outcome_id) <> 1 THEN
        RAISE EXCEPTION 'LEARNING_98B4_REPAIR_POSTCONDITION_FAILED';
    END IF;
END;
$repair$;

COMMIT;

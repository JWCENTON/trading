\set ON_ERROR_STOP on

CREATE TABLE automation_kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE decision_registry_v1 (
    decision_id UUID PRIMARY KEY,
    deployment_id TEXT,
    environment TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    strategy TEXT NOT NULL,
    market_regime TEXT,
    decision_timestamp TIMESTAMPTZ NOT NULL,
    position_id BIGINT,
    recommendation_id TEXT
);

CREATE TABLE decision_replay_v1 (
    environment TEXT NOT NULL,
    position_id BIGINT
);

CREATE TABLE learning_feature_warehouse_v1 (
    environment TEXT NOT NULL,
    position_id BIGINT
);

INSERT INTO decision_registry_v1 (
    decision_id, environment, symbol, interval, strategy, market_regime,
    decision_timestamp
) VALUES (
    '20000000-0000-0000-0000-000000000000', 'trading_live', 'BTCUSDC',
    '1m', 'RSI', NULL, '2026-07-01T00:00:00Z'
);

\ir ../../db/migrations/20260716_causal_learning_telemetry_v1.sql
\ir ../../db/migrations/20260716_causal_learning_telemetry_v1.sql

INSERT INTO learning_recommendation_snapshots_v1 (
    recommendation_id, recommendation_version, environment, slot_key,
    strategy, symbol, interval, market_regime, recommendation_action,
    recommendation_type, confidence, evidence_decisions, evidence_start_at,
    evidence_cutoff_at, valid_from, expires_at, policy_version, payload_hash,
    payload
) VALUES (
    'rec-range-v1', 'v1', 'trading_paper',
    'TRADING_PAPER|BBRANGE|SOLUSDC|1M|RANGE_LOWVOL',
    'BBRANGE', 'SOLUSDC', '1m', 'RANGE_LOWVOL', 'BLOCK_CANDIDATE',
    'EDGE', 0.8, 100, '2026-07-01T00:00:00Z',
    '2026-07-10T00:00:00Z', '2026-07-11T00:00:00Z',
    '2026-08-01T00:00:00Z', 'policy-v1', 'payload-v1', '{"edge": 0.8}'
);

INSERT INTO learning_recommendation_activations_v1 (
    activation_id, recommendation_id, experiment_id, environment, slot_key,
    experiment_arm, baseline_policy_version, candidate_policy_version,
    promotion_event_id, promotion_payload_hash, promotion_policy_version,
    promotion_candidate_id, activated_at, effective_from, expires_at
) VALUES (
    '10000000-0000-0000-0000-000000000001', 'rec-range-v1', 'exp-1',
    'trading_paper', 'TRADING_PAPER|BBRANGE|SOLUSDC|1M|RANGE_LOWVOL',
    'TREATMENT', 'baseline-v1', 'candidate-v2', 42, 'promotion-hash',
    'promotion-v3', 'candidate-42', '2026-07-11T00:00:00Z',
    '2026-07-12T00:00:00Z', '2026-07-20T00:00:00Z'
);

INSERT INTO decision_registry_v1 (
    decision_id, environment, symbol, interval, strategy, market_regime,
    decision_timestamp
) VALUES
    ('20000000-0000-0000-0000-000000000001', 'trading_paper', 'SOLUSDC',
     '1m', 'BBRANGE', 'RANGE_LOWVOL', '2026-07-11T23:59:59Z'),
    ('20000000-0000-0000-0000-000000000002', 'trading_paper', 'SOLUSDC',
     '1m', 'BBRANGE', 'RANGE_LOWVOL', '2026-07-12T00:00:01Z'),
    ('20000000-0000-0000-0000-000000000003', 'trading_paper', 'SOLUSDC',
     '1m', 'BBRANGE', 'RANGE_LOWVOL', '2026-07-20T00:00:00Z'),
    ('20000000-0000-0000-0000-000000000004', 'trading_live', 'SOLUSDC',
     '1m', 'BBRANGE', 'RANGE_LOWVOL', '2026-07-12T00:00:01Z'),
    ('20000000-0000-0000-0000-000000000005', 'trading_paper', 'SOLUSDC',
     '1m', 'BBRANGE', 'TREND_UP', '2026-07-12T00:00:01Z');

DO $$
BEGIN
    IF (SELECT causal_linkage_status FROM decision_registry_v1
        WHERE decision_id = '20000000-0000-0000-0000-000000000001')
       <> 'NO_ACTIVE_RECOMMENDATION' THEN
        RAISE EXCEPTION 'decision before effective_from was attributed';
    END IF;
    IF (SELECT causal_linkage_status FROM decision_registry_v1
        WHERE decision_id = '20000000-0000-0000-0000-000000000002')
       <> 'ATTRIBUTED' THEN
        RAISE EXCEPTION 'future decision was not attributed';
    END IF;
    IF (SELECT consumed_promotion_hash FROM decision_registry_v1
        WHERE decision_id = '20000000-0000-0000-0000-000000000002')
       <> 'promotion-hash' THEN
        RAISE EXCEPTION 'promotion consumption was not persisted';
    END IF;
    IF EXISTS (
        SELECT 1 FROM decision_registry_v1
        WHERE decision_id IN (
            '20000000-0000-0000-0000-000000000003',
            '20000000-0000-0000-0000-000000000004',
            '20000000-0000-0000-0000-000000000005'
        ) AND causal_linkage_status <> 'NO_ACTIVE_RECOMMENDATION'
    ) THEN
        RAISE EXCEPTION 'expiry/environment/regime isolation failed';
    END IF;
    IF (SELECT causal_linkage_status FROM decision_registry_v1
        WHERE decision_id = '20000000-0000-0000-0000-000000000000')
       <> 'LEGACY_NOT_ATTRIBUTABLE' THEN
        RAISE EXCEPTION 'legacy decision was rewritten';
    END IF;
END;
$$;

UPDATE learning_recommendation_snapshots_v1
SET reset_at = '2026-07-15T00:00:00Z', status = 'RESET'
WHERE recommendation_id = 'rec-range-v1';

INSERT INTO decision_registry_v1 (
    decision_id, environment, symbol, interval, strategy, market_regime,
    decision_timestamp
) VALUES (
    '20000000-0000-0000-0000-000000000006', 'trading_paper', 'SOLUSDC',
    '1m', 'BBRANGE', 'RANGE_LOWVOL', '2026-07-16T00:00:00Z'
);

DO $$
BEGIN
    IF (SELECT causal_linkage_status FROM decision_registry_v1
        WHERE decision_id = '20000000-0000-0000-0000-000000000006')
       <> 'NO_ACTIVE_RECOMMENDATION' THEN
        RAISE EXCEPTION 'reset recommendation was attributed';
    END IF;
END;
$$;

DO $$
BEGIN
    BEGIN
        UPDATE learning_recommendation_snapshots_v1
        SET payload_hash = 'rewritten'
        WHERE recommendation_id = 'rec-range-v1';
        RAISE EXCEPTION 'snapshot rewrite unexpectedly succeeded';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM = 'snapshot rewrite unexpectedly succeeded' THEN RAISE; END IF;
    END;

    BEGIN
        UPDATE decision_registry_v1
        SET recommendation_id = NULL
        WHERE decision_id = '20000000-0000-0000-0000-000000000002';
        RAISE EXCEPTION 'causal rewrite unexpectedly succeeded';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM = 'causal rewrite unexpectedly succeeded' THEN RAISE; END IF;
    END;
END;
$$;

SELECT record_learning_would_trade_v1(
    'would-1', '20000000-0000-0000-0000-000000000002', 'rec-range-v1',
    'v1', '10000000-0000-0000-0000-000000000001', 'exp-1', 'TREATMENT',
    'trading_paper', 'TRADING_PAPER|BBRANGE|SOLUSDC|1M|RANGE_LOWVOL',
    'BBRANGE', 'SOLUSDC', '1m', 'RANGE_LOWVOL', true, 'BUY', 150, 1,
    150, 145, 160, '2026-07-12T00:00:01Z', 'blocked by candidate',
    'baseline-v1', 'candidate-v2', 'would-payload-hash'
);

SELECT record_learning_would_trade_v1(
    'would-1', '20000000-0000-0000-0000-000000000002', 'rec-range-v1',
    'v1', '10000000-0000-0000-0000-000000000001', 'exp-1', 'TREATMENT',
    'trading_paper', 'TRADING_PAPER|BBRANGE|SOLUSDC|1M|RANGE_LOWVOL',
    'BBRANGE', 'SOLUSDC', '1m', 'RANGE_LOWVOL', true, 'BUY', 150, 1,
    150, 145, 160, '2026-07-12T00:00:01Z', 'blocked by candidate',
    'baseline-v1', 'candidate-v2', 'would-payload-hash'
);

SELECT record_learning_counterfactual_outcome_v1(
    'would-1', 'rec-range-v1',
    '10000000-0000-0000-0000-000000000001', 'exp-1', 'TREATMENT',
    'DIRECTIONAL_ONLY', 60, 150, 5, -2, 153, 3, 0.2, 2.8,
    '2026-07-12T01:00:01Z', 'fixed-horizon-v1', '{"source":"candles"}'
);

DO $$
BEGIN
    IF (SELECT count(*) FROM learning_would_trade_decisions_v1) <> 1 THEN
        RAISE EXCEPTION 'would-trade idempotency failed';
    END IF;
    IF (SELECT evaluation_status FROM learning_counterfactual_outcomes_v1
        WHERE decision_key = 'would-1') <> 'DIRECTIONAL_ONLY' THEN
        RAISE EXCEPTION 'counterfactual outcome missing';
    END IF;
    IF (SELECT value FROM automation_kv
        WHERE key = 'causal_learning_auto_apply_enabled') <> '0' THEN
        RAISE EXCEPTION 'auto apply is not disabled';
    END IF;
END;
$$;

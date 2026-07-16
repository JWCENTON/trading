BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS learning_recommendation_snapshots_v1 (
    recommendation_id TEXT PRIMARY KEY,
    recommendation_version TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('trading_live', 'trading_paper')),
    slot_key TEXT NOT NULL,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    market_regime TEXT,
    recommendation_action TEXT NOT NULL CHECK (
        recommendation_action IN (
            'OBSERVE', 'BLOCK_CANDIDATE', 'REDUCE_CONFIDENCE',
            'INCREASE_CONFIDENCE', 'PROMOTE_CANDIDATE'
        )
    ),
    recommendation_type TEXT NOT NULL,
    confidence NUMERIC,
    evidence_decisions INTEGER NOT NULL CHECK (evidence_decisions >= 0),
    evidence_start_at TIMESTAMPTZ,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_from TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    reset_at TIMESTAMPTZ,
    policy_version TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'FROZEN' CHECK (
        status IN ('FROZEN', 'ACTIVE', 'RESET', 'EXPIRED')
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    schema_version TEXT NOT NULL DEFAULT 'CAUSAL_LEARNING_TELEMETRY_V1',
    CHECK (expires_at > valid_from),
    CHECK (evidence_start_at IS NULL OR evidence_start_at <= evidence_cutoff_at),
    CHECK (evidence_cutoff_at <= valid_from),
    CHECK (reset_at IS NULL OR reset_at >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_learning_recommendation_identity_v1
ON learning_recommendation_snapshots_v1 (
    environment, strategy, symbol, interval,
    COALESCE(market_regime, ''), recommendation_version
);

CREATE OR REPLACE FUNCTION prevent_causal_snapshot_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.recommendation_id, NEW.recommendation_version, NEW.environment,
        NEW.slot_key, NEW.strategy, NEW.symbol, NEW.interval,
        NEW.market_regime, NEW.recommendation_action,
        NEW.recommendation_type, NEW.confidence, NEW.evidence_decisions,
        NEW.evidence_start_at, NEW.evidence_cutoff_at, NEW.created_at,
        NEW.valid_from, NEW.expires_at, NEW.policy_version,
        NEW.payload_hash, NEW.payload, NEW.schema_version)
       IS DISTINCT FROM
       (OLD.recommendation_id, OLD.recommendation_version, OLD.environment,
        OLD.slot_key, OLD.strategy, OLD.symbol, OLD.interval,
        OLD.market_regime, OLD.recommendation_action,
        OLD.recommendation_type, OLD.confidence, OLD.evidence_decisions,
        OLD.evidence_start_at, OLD.evidence_cutoff_at, OLD.created_at,
        OLD.valid_from, OLD.expires_at, OLD.policy_version,
        OLD.payload_hash, OLD.payload, OLD.schema_version)
    THEN
        RAISE EXCEPTION 'frozen recommendation snapshot is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS learning_snapshot_immutable_v1
ON learning_recommendation_snapshots_v1;
CREATE TRIGGER learning_snapshot_immutable_v1
BEFORE UPDATE ON learning_recommendation_snapshots_v1
FOR EACH ROW EXECUTE FUNCTION prevent_causal_snapshot_mutation_v1();

CREATE TABLE IF NOT EXISTS learning_recommendation_activations_v1 (
    activation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recommendation_id TEXT NOT NULL REFERENCES learning_recommendation_snapshots_v1(recommendation_id),
    experiment_id TEXT,
    environment TEXT NOT NULL CHECK (environment IN ('trading_live', 'trading_paper')),
    slot_key TEXT NOT NULL,
    experiment_arm TEXT NOT NULL DEFAULT 'BASELINE' CHECK (
        experiment_arm IN ('BASELINE', 'TREATMENT', 'SHADOW_COUNTERFACTUAL')
    ),
    baseline_policy_version TEXT NOT NULL,
    candidate_policy_version TEXT NOT NULL,
    promotion_event_id BIGINT,
    promotion_payload_hash TEXT,
    promotion_policy_version TEXT,
    promotion_candidate_id TEXT,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_from TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    deactivated_at TIMESTAMPTZ,
    deactivation_reason TEXT,
    apply_mode TEXT NOT NULL DEFAULT 'SHADOW_OBSERVATION' CHECK (
        apply_mode IN ('SHADOW_OBSERVATION', 'PAPER_EXPERIMENT')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at > effective_from),
    CHECK (deactivated_at IS NULL OR deactivated_at >= effective_from),
    CHECK (apply_mode <> 'PAPER_EXPERIMENT' OR environment = 'trading_paper')
);

CREATE INDEX IF NOT EXISTS ix_learning_activation_lookup_v1
ON learning_recommendation_activations_v1 (
    environment, slot_key, effective_from, expires_at
);

CREATE OR REPLACE FUNCTION prevent_causal_activation_mutation_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.recommendation_id, NEW.experiment_id, NEW.environment, NEW.slot_key,
        NEW.experiment_arm, NEW.baseline_policy_version,
        NEW.candidate_policy_version, NEW.activated_at, NEW.effective_from,
        NEW.expires_at, NEW.apply_mode, NEW.created_at)
       IS DISTINCT FROM
       (OLD.recommendation_id, OLD.experiment_id, OLD.environment, OLD.slot_key,
        OLD.experiment_arm, OLD.baseline_policy_version,
        OLD.candidate_policy_version, OLD.activated_at, OLD.effective_from,
        OLD.expires_at, OLD.apply_mode, OLD.created_at)
    THEN
        RAISE EXCEPTION 'causal activation identity is append-only';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS learning_activation_append_only_v1
ON learning_recommendation_activations_v1;
CREATE TRIGGER learning_activation_append_only_v1
BEFORE UPDATE ON learning_recommendation_activations_v1
FOR EACH ROW EXECUTE FUNCTION prevent_causal_activation_mutation_v1();

ALTER TABLE decision_registry_v1
    ADD COLUMN IF NOT EXISTS recommendation_version TEXT,
    ADD COLUMN IF NOT EXISTS activation_id UUID,
    ADD COLUMN IF NOT EXISTS experiment_id TEXT,
    ADD COLUMN IF NOT EXISTS experiment_arm TEXT,
    ADD COLUMN IF NOT EXISTS baseline_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS candidate_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS promotion_event_id BIGINT,
    ADD COLUMN IF NOT EXISTS promotion_candidate_id TEXT,
    ADD COLUMN IF NOT EXISTS consumed_promotion_hash TEXT,
    ADD COLUMN IF NOT EXISTS consumed_promotion_version TEXT,
    ADD COLUMN IF NOT EXISTS causal_linkage_status TEXT NOT NULL
        DEFAULT 'LEGACY_NOT_ATTRIBUTABLE',
    ADD COLUMN IF NOT EXISTS causal_attributed_at TIMESTAMPTZ;

ALTER TABLE decision_registry_v1
    DROP CONSTRAINT IF EXISTS ck_decision_registry_experiment_arm_v1;
ALTER TABLE decision_registry_v1
    ADD CONSTRAINT ck_decision_registry_experiment_arm_v1 CHECK (
        experiment_arm IS NULL OR experiment_arm IN (
            'BASELINE', 'TREATMENT', 'SHADOW_COUNTERFACTUAL'
        )
    );

ALTER TABLE decision_registry_v1
    DROP CONSTRAINT IF EXISTS ck_decision_registry_causal_status_v1;
ALTER TABLE decision_registry_v1
    ADD CONSTRAINT ck_decision_registry_causal_status_v1 CHECK (
        causal_linkage_status IN (
            'LEGACY_NOT_ATTRIBUTABLE', 'NO_ACTIVE_RECOMMENDATION',
            'ATTRIBUTED', 'EXPIRED', 'RESET'
        )
    );

CREATE OR REPLACE FUNCTION attribute_decision_causally_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_match RECORD;
    v_slot_key TEXT;
BEGIN
    IF NEW.causal_attributed_at IS NOT NULL THEN
        RETURN NEW;
    END IF;

    v_slot_key := upper(concat_ws('|', NEW.environment, NEW.strategy,
        NEW.symbol, NEW.interval, COALESCE(NEW.market_regime, '*')));

    SELECT a.activation_id, a.experiment_id, a.experiment_arm,
           a.baseline_policy_version, a.candidate_policy_version,
           a.promotion_event_id, a.promotion_candidate_id,
           a.promotion_payload_hash, a.promotion_policy_version,
           r.recommendation_id, r.recommendation_version
    INTO v_match
    FROM learning_recommendation_activations_v1 a
    JOIN learning_recommendation_snapshots_v1 r
      ON r.recommendation_id = a.recommendation_id
    WHERE a.environment = NEW.environment
      AND a.slot_key = v_slot_key
      AND a.effective_from <= NEW.decision_timestamp
      AND a.expires_at > NEW.decision_timestamp
      AND (a.deactivated_at IS NULL OR a.deactivated_at > NEW.decision_timestamp)
      AND r.status IN ('FROZEN', 'ACTIVE')
      AND r.evidence_cutoff_at < NEW.decision_timestamp
      AND r.reset_at IS NULL
    ORDER BY a.effective_from DESC, a.created_at DESC
    LIMIT 1;

    IF FOUND THEN
        NEW.recommendation_id := v_match.recommendation_id;
        NEW.recommendation_version := v_match.recommendation_version;
        NEW.activation_id := v_match.activation_id;
        NEW.experiment_id := v_match.experiment_id;
        NEW.experiment_arm := v_match.experiment_arm;
        NEW.baseline_policy_version := v_match.baseline_policy_version;
        NEW.candidate_policy_version := v_match.candidate_policy_version;
        NEW.promotion_event_id := v_match.promotion_event_id;
        NEW.promotion_candidate_id := v_match.promotion_candidate_id;
        NEW.consumed_promotion_hash := v_match.promotion_payload_hash;
        NEW.consumed_promotion_version := v_match.promotion_policy_version;
        NEW.causal_linkage_status := 'ATTRIBUTED';
    ELSE
        NEW.causal_linkage_status := 'NO_ACTIVE_RECOMMENDATION';
        NEW.experiment_arm := 'BASELINE';
    END IF;
    NEW.causal_attributed_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS decision_registry_causal_attribution_v1
ON decision_registry_v1;
CREATE TRIGGER decision_registry_causal_attribution_v1
BEFORE INSERT ON decision_registry_v1
FOR EACH ROW EXECUTE FUNCTION attribute_decision_causally_v1();

CREATE OR REPLACE FUNCTION prevent_decision_causal_rewrite_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (NEW.recommendation_id, NEW.recommendation_version, NEW.activation_id,
        NEW.experiment_id, NEW.experiment_arm, NEW.baseline_policy_version,
        NEW.candidate_policy_version, NEW.promotion_event_id,
        NEW.promotion_candidate_id, NEW.consumed_promotion_hash,
        NEW.consumed_promotion_version, NEW.causal_linkage_status,
        NEW.causal_attributed_at)
       IS DISTINCT FROM
       (OLD.recommendation_id, OLD.recommendation_version, OLD.activation_id,
        OLD.experiment_id, OLD.experiment_arm, OLD.baseline_policy_version,
        OLD.candidate_policy_version, OLD.promotion_event_id,
        OLD.promotion_candidate_id, OLD.consumed_promotion_hash,
        OLD.consumed_promotion_version, OLD.causal_linkage_status,
        OLD.causal_attributed_at)
    THEN
        RAISE EXCEPTION 'historical causal attribution is immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS decision_registry_causal_immutable_v1
ON decision_registry_v1;
CREATE TRIGGER decision_registry_causal_immutable_v1
BEFORE UPDATE ON decision_registry_v1
FOR EACH ROW EXECUTE FUNCTION prevent_decision_causal_rewrite_v1();

CREATE TABLE IF NOT EXISTS learning_would_trade_decisions_v1 (
    decision_key TEXT PRIMARY KEY,
    decision_id UUID,
    recommendation_id TEXT,
    recommendation_version TEXT,
    activation_id UUID,
    experiment_id TEXT,
    experiment_arm TEXT NOT NULL CHECK (
        experiment_arm IN ('BASELINE', 'TREATMENT', 'SHADOW_COUNTERFACTUAL')
    ),
    environment TEXT NOT NULL CHECK (environment IN ('trading_live', 'trading_paper')),
    slot_key TEXT NOT NULL,
    strategy TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    market_regime TEXT,
    would_trade BOOLEAN NOT NULL,
    would_side TEXT,
    would_entry_price NUMERIC,
    would_qty NUMERIC,
    would_notional NUMERIC,
    would_stop NUMERIC,
    would_take_profit NUMERIC,
    would_signal_at TIMESTAMPTZ NOT NULL,
    would_reason TEXT NOT NULL,
    baseline_policy_version TEXT NOT NULL,
    candidate_policy_version TEXT,
    payload_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (environment = 'trading_paper' OR experiment_id IS NULL),
    CHECK (would_qty IS NULL OR would_qty >= 0),
    CHECK (would_notional IS NULL OR would_notional >= 0)
);

CREATE TABLE IF NOT EXISTS learning_counterfactual_outcomes_v1 (
    decision_key TEXT PRIMARY KEY REFERENCES learning_would_trade_decisions_v1(decision_key),
    recommendation_id TEXT,
    activation_id UUID,
    experiment_id TEXT,
    experiment_arm TEXT NOT NULL CHECK (
        experiment_arm IN ('BASELINE', 'TREATMENT', 'SHADOW_COUNTERFACTUAL')
    ),
    evaluation_status TEXT NOT NULL CHECK (
        evaluation_status IN ('PENDING', 'DIRECTIONAL_ONLY', 'COMPLETE', 'EXPIRED', 'ERROR')
    ),
    evaluation_horizon_minutes INTEGER NOT NULL CHECK (evaluation_horizon_minutes > 0),
    entry_reference_price NUMERIC NOT NULL,
    max_favorable_excursion NUMERIC,
    max_adverse_excursion NUMERIC,
    fixed_horizon_exit_price NUMERIC,
    gross_pnl NUMERIC,
    estimated_fees NUMERIC,
    net_pnl NUMERIC,
    outcome_at TIMESTAMPTZ,
    method_version TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION record_learning_would_trade_v1(
    p_decision_key TEXT,
    p_decision_id UUID,
    p_recommendation_id TEXT,
    p_recommendation_version TEXT,
    p_activation_id UUID,
    p_experiment_id TEXT,
    p_experiment_arm TEXT,
    p_environment TEXT,
    p_slot_key TEXT,
    p_strategy TEXT,
    p_symbol TEXT,
    p_interval TEXT,
    p_market_regime TEXT,
    p_would_trade BOOLEAN,
    p_would_side TEXT,
    p_would_entry_price NUMERIC,
    p_would_qty NUMERIC,
    p_would_notional NUMERIC,
    p_would_stop NUMERIC,
    p_would_take_profit NUMERIC,
    p_would_signal_at TIMESTAMPTZ,
    p_would_reason TEXT,
    p_baseline_policy_version TEXT,
    p_candidate_policy_version TEXT,
    p_payload_hash TEXT
)
RETURNS learning_would_trade_decisions_v1
LANGUAGE plpgsql
AS $$
DECLARE
    v_row learning_would_trade_decisions_v1;
BEGIN
    INSERT INTO learning_would_trade_decisions_v1 (
        decision_key, decision_id, recommendation_id, recommendation_version,
        activation_id, experiment_id, experiment_arm, environment, slot_key,
        strategy, symbol, interval, market_regime, would_trade, would_side,
        would_entry_price, would_qty, would_notional, would_stop,
        would_take_profit, would_signal_at, would_reason,
        baseline_policy_version, candidate_policy_version, payload_hash
    ) VALUES (
        p_decision_key, p_decision_id, p_recommendation_id,
        p_recommendation_version, p_activation_id, p_experiment_id,
        p_experiment_arm, p_environment, p_slot_key, p_strategy, p_symbol,
        p_interval, p_market_regime, p_would_trade, p_would_side,
        p_would_entry_price, p_would_qty, p_would_notional, p_would_stop,
        p_would_take_profit, p_would_signal_at, p_would_reason,
        p_baseline_policy_version, p_candidate_policy_version, p_payload_hash
    )
    ON CONFLICT (decision_key) DO NOTHING;

    SELECT * INTO STRICT v_row
    FROM learning_would_trade_decisions_v1
    WHERE decision_key = p_decision_key;
    RETURN v_row;
END;
$$;

CREATE OR REPLACE FUNCTION record_learning_counterfactual_outcome_v1(
    p_decision_key TEXT,
    p_recommendation_id TEXT,
    p_activation_id UUID,
    p_experiment_id TEXT,
    p_experiment_arm TEXT,
    p_evaluation_status TEXT,
    p_evaluation_horizon_minutes INTEGER,
    p_entry_reference_price NUMERIC,
    p_max_favorable_excursion NUMERIC,
    p_max_adverse_excursion NUMERIC,
    p_fixed_horizon_exit_price NUMERIC,
    p_gross_pnl NUMERIC,
    p_estimated_fees NUMERIC,
    p_net_pnl NUMERIC,
    p_outcome_at TIMESTAMPTZ,
    p_method_version TEXT,
    p_evidence JSONB
)
RETURNS learning_counterfactual_outcomes_v1
LANGUAGE plpgsql
AS $$
DECLARE
    v_row learning_counterfactual_outcomes_v1;
BEGIN
    INSERT INTO learning_counterfactual_outcomes_v1 (
        decision_key, recommendation_id, activation_id, experiment_id,
        experiment_arm, evaluation_status, evaluation_horizon_minutes,
        entry_reference_price, max_favorable_excursion,
        max_adverse_excursion, fixed_horizon_exit_price, gross_pnl,
        estimated_fees, net_pnl, outcome_at, method_version, evidence
    ) VALUES (
        p_decision_key, p_recommendation_id, p_activation_id, p_experiment_id,
        p_experiment_arm, p_evaluation_status, p_evaluation_horizon_minutes,
        p_entry_reference_price, p_max_favorable_excursion,
        p_max_adverse_excursion, p_fixed_horizon_exit_price, p_gross_pnl,
        p_estimated_fees, p_net_pnl, p_outcome_at, p_method_version,
        COALESCE(p_evidence, '{}'::jsonb)
    )
    ON CONFLICT (decision_key) DO UPDATE SET
        evaluation_status = EXCLUDED.evaluation_status,
        max_favorable_excursion = EXCLUDED.max_favorable_excursion,
        max_adverse_excursion = EXCLUDED.max_adverse_excursion,
        fixed_horizon_exit_price = EXCLUDED.fixed_horizon_exit_price,
        gross_pnl = EXCLUDED.gross_pnl,
        estimated_fees = EXCLUDED.estimated_fees,
        net_pnl = EXCLUDED.net_pnl,
        outcome_at = EXCLUDED.outcome_at,
        method_version = EXCLUDED.method_version,
        evidence = EXCLUDED.evidence,
        refreshed_at = clock_timestamp()
    RETURNING * INTO v_row;
    RETURN v_row;
END;
$$;

ALTER TABLE decision_replay_v1
    ADD COLUMN IF NOT EXISTS recommendation_id TEXT,
    ADD COLUMN IF NOT EXISTS recommendation_version TEXT,
    ADD COLUMN IF NOT EXISTS activation_id UUID,
    ADD COLUMN IF NOT EXISTS experiment_id TEXT,
    ADD COLUMN IF NOT EXISTS experiment_arm TEXT,
    ADD COLUMN IF NOT EXISTS baseline_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS candidate_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS causal_linkage_status TEXT NOT NULL
        DEFAULT 'LEGACY_NOT_ATTRIBUTABLE',
    ADD COLUMN IF NOT EXISTS counterfactual_status TEXT;

ALTER TABLE learning_feature_warehouse_v1
    ADD COLUMN IF NOT EXISTS recommendation_id TEXT,
    ADD COLUMN IF NOT EXISTS recommendation_version TEXT,
    ADD COLUMN IF NOT EXISTS activation_id UUID,
    ADD COLUMN IF NOT EXISTS experiment_id TEXT,
    ADD COLUMN IF NOT EXISTS experiment_arm TEXT,
    ADD COLUMN IF NOT EXISTS baseline_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS candidate_policy_version TEXT,
    ADD COLUMN IF NOT EXISTS causal_linkage_status TEXT NOT NULL
        DEFAULT 'LEGACY_NOT_ATTRIBUTABLE',
    ADD COLUMN IF NOT EXISTS counterfactual_status TEXT;

CREATE OR REPLACE FUNCTION propagate_decision_causal_linkage_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.position_id IS NULL THEN
        RETURN NEW;
    END IF;
    UPDATE decision_replay_v1 SET
        recommendation_id = NEW.recommendation_id,
        recommendation_version = NEW.recommendation_version,
        activation_id = NEW.activation_id,
        experiment_id = NEW.experiment_id,
        experiment_arm = NEW.experiment_arm,
        baseline_policy_version = NEW.baseline_policy_version,
        candidate_policy_version = NEW.candidate_policy_version,
        causal_linkage_status = NEW.causal_linkage_status
    WHERE environment = NEW.environment AND position_id = NEW.position_id
      AND causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE';

    UPDATE learning_feature_warehouse_v1 SET
        recommendation_id = NEW.recommendation_id,
        recommendation_version = NEW.recommendation_version,
        activation_id = NEW.activation_id,
        experiment_id = NEW.experiment_id,
        experiment_arm = NEW.experiment_arm,
        baseline_policy_version = NEW.baseline_policy_version,
        candidate_policy_version = NEW.candidate_policy_version,
        causal_linkage_status = NEW.causal_linkage_status
    WHERE environment = NEW.environment AND position_id = NEW.position_id
      AND causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE';
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS decision_registry_causal_propagation_v1
ON decision_registry_v1;
CREATE TRIGGER decision_registry_causal_propagation_v1
AFTER INSERT ON decision_registry_v1
FOR EACH ROW EXECUTE FUNCTION propagate_decision_causal_linkage_v1();

CREATE OR REPLACE VIEW v_learning_causal_attribution_v1 AS
SELECT d.decision_id, d.deployment_id, d.environment, d.symbol, d.interval,
       d.strategy, d.market_regime, d.decision_timestamp,
       d.recommendation_id, d.recommendation_version, d.activation_id,
       d.experiment_id, d.experiment_arm, d.baseline_policy_version,
       d.candidate_policy_version, d.promotion_event_id,
       d.promotion_candidate_id, d.consumed_promotion_hash,
       d.consumed_promotion_version, d.causal_linkage_status,
       d.causal_attributed_at
FROM decision_registry_v1 d;

CREATE OR REPLACE VIEW v_learning_causal_coverage_v1 AS
SELECT environment,
       count(*) AS decisions,
       count(*) FILTER (WHERE causal_linkage_status = 'ATTRIBUTED') AS attributed,
       count(*) FILTER (WHERE causal_linkage_status = 'NO_ACTIVE_RECOMMENDATION') AS no_active,
       count(*) FILTER (WHERE causal_linkage_status = 'LEGACY_NOT_ATTRIBUTABLE') AS legacy,
       round(100.0 * count(*) FILTER (WHERE causal_linkage_status = 'ATTRIBUTED')
             / NULLIF(count(*), 0), 4) AS attributed_pct
FROM decision_registry_v1
GROUP BY environment;

CREATE OR REPLACE VIEW v_learning_counterfactual_outcomes_v1 AS
SELECT w.decision_key, w.decision_id, w.recommendation_id,
       w.recommendation_version, w.activation_id, w.experiment_id,
       w.experiment_arm, w.environment, w.slot_key, w.strategy, w.symbol,
       w.interval, w.market_regime, w.would_trade, w.would_side,
       w.would_entry_price, w.would_qty, w.would_notional,
       w.would_signal_at, w.would_reason, o.evaluation_status,
       o.evaluation_horizon_minutes, o.entry_reference_price,
       o.max_favorable_excursion, o.max_adverse_excursion,
       o.fixed_horizon_exit_price, o.gross_pnl, o.estimated_fees,
       o.net_pnl, o.outcome_at, o.method_version
FROM learning_would_trade_decisions_v1 w
LEFT JOIN learning_counterfactual_outcomes_v1 o USING (decision_key);

CREATE OR REPLACE VIEW v_learning_experiment_readiness_v1 AS
SELECT r.recommendation_id, r.recommendation_version, r.environment,
       r.slot_key, r.recommendation_action, r.evidence_cutoff_at,
       r.expires_at, a.activation_id, a.experiment_id, a.experiment_arm,
       a.baseline_policy_version, a.candidate_policy_version, a.apply_mode,
       EXISTS (
           SELECT 1 FROM decision_registry_v1 d
           WHERE d.activation_id = a.activation_id
             AND d.causal_linkage_status = 'ATTRIBUTED'
       ) AS future_decision_attribution_present,
       EXISTS (
           SELECT 1 FROM learning_would_trade_decisions_v1 w
           WHERE w.activation_id = a.activation_id
       ) AS counterfactual_telemetry_present,
       (r.environment = 'trading_paper') AS paper_only,
       (a.apply_mode = 'SHADOW_OBSERVATION') AS auto_apply_off,
       (r.status IN ('FROZEN', 'ACTIVE')
        AND r.evidence_cutoff_at IS NOT NULL
        AND a.activation_id IS NOT NULL
        AND a.baseline_policy_version IS NOT NULL
        AND r.expires_at IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM automation_kv k
            WHERE k.key = 'causal_learning_kill_switch_available'
              AND k.value = '1'
        )
        AND EXISTS (
            SELECT 1 FROM automation_kv k
            WHERE k.key = 'causal_learning_auto_apply_enabled'
              AND k.value = '0'
        )
        AND r.environment = 'trading_paper'
        AND a.apply_mode = 'SHADOW_OBSERVATION'
        AND EXISTS (
            SELECT 1 FROM learning_would_trade_decisions_v1 w
            WHERE w.activation_id = a.activation_id
        )
       ) AS experiment_ready
FROM learning_recommendation_snapshots_v1 r
LEFT JOIN learning_recommendation_activations_v1 a
  ON a.recommendation_id = r.recommendation_id;

INSERT INTO automation_kv(key, value, updated_at)
VALUES
    ('causal_learning_telemetry_v1_enabled', '1', now()),
    ('causal_learning_auto_apply_enabled', '0', now()),
    ('causal_learning_kill_switch_available', '1', now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;

COMMENT ON TABLE learning_recommendation_snapshots_v1 IS
'Immutable frozen recommendation evidence snapshots; telemetry-only.';
COMMENT ON TABLE learning_recommendation_activations_v1 IS
'Append-only shadow activation ledger. V1 does not authorize trading changes.';
COMMENT ON TABLE learning_would_trade_decisions_v1 IS
'Shadow would-trade telemetry; never creates orders or positions.';
COMMENT ON TABLE learning_counterfactual_outcomes_v1 IS
'Bounded fixed-horizon/DIRECTIONAL_ONLY counterfactual outcomes.';

COMMIT;

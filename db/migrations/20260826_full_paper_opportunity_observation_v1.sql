\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE IF NOT EXISTS public.paper_opportunity_observation_v1 (
    observation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_key TEXT NOT NULL UNIQUE,
    causal_event_id UUID NOT NULL UNIQUE
        REFERENCES public.causal_decision_observation_v1(event_id) ON DELETE RESTRICT,
    decision_key TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment='trading_paper'),
    deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper')),
    strategy TEXT NOT NULL CHECK (strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')),
    symbol TEXT NOT NULL CHECK (symbol IN ('BTCUSDC','ETHUSDC','SOLUSDC','BNBUSDC')),
    interval TEXT NOT NULL CHECK (interval IN ('1m','5m')),
    observed_at TIMESTAMPTZ NOT NULL,
    candle_open_time TIMESTAMPTZ NOT NULL,
    evaluation_started_at TIMESTAMPTZ NOT NULL,
    observation_type TEXT NOT NULL CHECK (observation_type IN (
        'EXECUTED','SIGNAL_REJECTED','GATE_BLOCKED','NO_SIGNAL',
        'ALREADY_OPEN_BLOCK','CONTAINMENT_BLOCK','POLICY_BLOCK',
        'DATA_NOT_READY','INDICATOR_NOT_READY','NO_NEW_CANDLE'
    )),
    decision_type TEXT NOT NULL,
    decision_subtype TEXT NOT NULL,
    decision_reason TEXT NOT NULL,
    reason_text TEXT,
    raw_signal_state TEXT NOT NULL CHECK (raw_signal_state IN ('PRESENT','ABSENT','UNKNOWN')),
    base_decision TEXT NOT NULL,
    final_decision TEXT NOT NULL,
    data_readiness TEXT NOT NULL CHECK (data_readiness IN ('READY','NOT_READY','UNKNOWN')),
    indicator_readiness TEXT NOT NULL CHECK (indicator_readiness IN ('READY','NOT_READY','UNKNOWN')),
    gate_state TEXT NOT NULL CHECK (gate_state IN ('PASS','BLOCKED','UNKNOWN')),
    gate_reason TEXT,
    already_open_state TEXT NOT NULL CHECK (already_open_state IN ('OPEN','CLEAR','UNKNOWN')),
    containment_state TEXT NOT NULL CHECK (containment_state IN ('ACTIVE','CLEAR','UNKNOWN')),
    outcome_eligible BOOLEAN NOT NULL,
    opportunity_direction TEXT NOT NULL CHECK (opportunity_direction IN ('LONG','SHORT')),
    reference_price NUMERIC,
    runtime_enabled BOOLEAN,
    live_orders_enabled BOOLEAN,
    treatment_name TEXT,
    treatment_status TEXT NOT NULL,
    treatment_base_decision TEXT,
    treatment_decision TEXT,
    treatment_reason TEXT,
    fee_rate_entry NUMERIC NOT NULL,
    fee_rate_exit NUMERIC NOT NULL,
    full_cost_hurdle_pct NUMERIC NOT NULL,
    fee_model_version TEXT NOT NULL CHECK (
        fee_model_version='PAPER_SIMULATOR_FINANCIAL_MODEL_V2'
    ),
    fee_config_source TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    engine_name TEXT NOT NULL,
    engine_version TEXT,
    position_id BIGINT,
    strategy_event_id BIGINT,
    simulated_order_id BIGINT,
    contract_version TEXT NOT NULL CHECK (
        contract_version='FULL_PAPER_OPPORTUNITY_OBSERVATION_V1'
    ),
    entry_trace_event_id BIGINT
        REFERENCES public.entry_trace_events(id) ON DELETE RESTRICT,
    entry_opportunity_snapshot_id UUID
        REFERENCES public.entry_opportunity_evidence_v1(snapshot_id) ON DELETE RESTRICT,
    realtime_availability_status TEXT NOT NULL,
    mme_availability_status TEXT NOT NULL,
    mme_direction TEXT,
    mme_sequence_stage TEXT,
    mme_source_refreshed_at TIMESTAMPTZ,
    orc_availability_status TEXT NOT NULL,
    orc_run_id UUID,
    observation_payload_hash TEXT NOT NULL CHECK (
        observation_payload_hash ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_paper_opportunity_price_v1 CHECK (
        reference_price IS NULL OR reference_price>0
    ),
    CONSTRAINT ck_paper_opportunity_cost_v1 CHECK (
        fee_rate_entry>=0 AND fee_rate_exit>=0 AND full_cost_hurdle_pct>=0
    )
);

CREATE INDEX IF NOT EXISTS ix_paper_opportunity_slot_time_v1
    ON public.paper_opportunity_observation_v1(
        deployment_id,strategy,symbol,interval,candle_open_time DESC
    );
CREATE INDEX IF NOT EXISTS ix_paper_opportunity_type_time_v1
    ON public.paper_opportunity_observation_v1(
        deployment_id,observation_type,observed_at DESC
    );
CREATE INDEX IF NOT EXISTS ix_paper_opportunity_outcome_queue_v1
    ON public.paper_opportunity_observation_v1(
        deployment_id,outcome_eligible,observed_at
    );

CREATE OR REPLACE FUNCTION public.guard_paper_opportunity_observation_immutable_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    RAISE EXCEPTION 'PAPER_OPPORTUNITY_OBSERVATION_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_paper_opportunity_observation_immutable_v1
    ON public.paper_opportunity_observation_v1;
CREATE TRIGGER trg_paper_opportunity_observation_immutable_v1
BEFORE UPDATE OR DELETE ON public.paper_opportunity_observation_v1
FOR EACH ROW EXECUTE FUNCTION public.guard_paper_opportunity_observation_immutable_v1();

CREATE TABLE IF NOT EXISTS public.paper_opportunity_outcome_v1 (
    outcome_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    observation_id UUID NOT NULL
        REFERENCES public.paper_opportunity_observation_v1(observation_id)
        ON DELETE RESTRICT,
    horizon_minutes SMALLINT NOT NULL CHECK (horizon_minutes IN (15,30,60,240)),
    evaluation_start_at TIMESTAMPTZ NOT NULL,
    evaluation_end_at TIMESTAMPTZ NOT NULL,
    market_data_source TEXT NOT NULL,
    market_data_granularity TEXT NOT NULL CHECK (market_data_granularity='1m'),
    market_rows_expected INTEGER NOT NULL,
    market_rows_used INTEGER NOT NULL,
    duplicate_market_rows INTEGER NOT NULL,
    market_data_gaps INTEGER NOT NULL,
    first_market_timestamp TIMESTAMPTZ,
    last_market_timestamp TIMESTAMPTZ,
    reference_price NUMERIC,
    opportunity_direction TEXT NOT NULL CHECK (opportunity_direction IN ('LONG','SHORT')),
    full_cost_hurdle_pct NUMERIC NOT NULL,
    maximum_favorable_price NUMERIC,
    maximum_adverse_price NUMERIC,
    mfe_pct NUMERIC,
    mae_pct NUMERIC,
    covered_full_costs BOOLEAN,
    time_to_full_cost_cover_seconds NUMERIC,
    time_to_mfe_seconds NUMERIC,
    economic_label TEXT NOT NULL CHECK (economic_label IN (
        'ECONOMICALLY_VIABLE','NEVER_COVERED_FULL_COSTS','EVIDENCE_INCOMPLETE'
    )),
    evidence_status TEXT NOT NULL CHECK (evidence_status IN ('COMPLETE','INCOMPLETE')),
    status_reason TEXT,
    source_revision TEXT NOT NULL,
    producer_version TEXT NOT NULL CHECK (
        producer_version='FULL_PAPER_OPPORTUNITY_OUTCOME_V1'
    ),
    payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(observation_id,horizon_minutes),
    CONSTRAINT ck_paper_opportunity_outcome_window_v1 CHECK (
        evaluation_end_at=evaluation_start_at+make_interval(mins=>horizon_minutes)
        AND market_rows_expected=horizon_minutes
        AND market_rows_used>=0
        AND duplicate_market_rows>=0
        AND market_data_gaps>=0
    ),
    CONSTRAINT ck_paper_opportunity_complete_v1 CHECK (
        (evidence_status='COMPLETE'
         AND reference_price>0
         AND market_rows_used=market_rows_expected
         AND duplicate_market_rows=0
         AND market_data_gaps=0
         AND mfe_pct IS NOT NULL AND mae_pct IS NOT NULL
         AND covered_full_costs IS NOT NULL
         AND economic_label IN ('ECONOMICALLY_VIABLE','NEVER_COVERED_FULL_COSTS'))
        OR
        (evidence_status='INCOMPLETE'
         AND economic_label='EVIDENCE_INCOMPLETE'
         AND mfe_pct IS NULL AND mae_pct IS NULL
         AND covered_full_costs IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_paper_opportunity_outcome_label_v1
    ON public.paper_opportunity_outcome_v1(
        economic_label,horizon_minutes,created_at DESC
    );

CREATE OR REPLACE FUNCTION public.guard_paper_opportunity_outcome_immutable_v1()
RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    RAISE EXCEPTION 'PAPER_OPPORTUNITY_OUTCOME_IMMUTABLE';
END
$function$;

DROP TRIGGER IF EXISTS trg_paper_opportunity_outcome_immutable_v1
    ON public.paper_opportunity_outcome_v1;
CREATE TRIGGER trg_paper_opportunity_outcome_immutable_v1
BEFORE UPDATE OR DELETE ON public.paper_opportunity_outcome_v1
FOR EACH ROW EXECUTE FUNCTION public.guard_paper_opportunity_outcome_immutable_v1();

CREATE OR REPLACE FUNCTION public.refresh_paper_opportunity_outcomes_v1(
    p_deployment_id TEXT DEFAULT 'local-paper',
    p_limit INTEGER DEFAULT 500
)
RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
AS $function$
DECLARE
    inserted_count BIGINT;
BEGIN
    IF p_deployment_id NOT IN ('local-paper','vps-paper') THEN
        RAISE EXCEPTION 'FULL_PAPER_OPPORTUNITY_OUTCOME_DEPLOYMENT_NOT_ALLOWED';
    END IF;
    IF p_limit IS NULL OR p_limit<1 OR p_limit>5000 THEN
        RAISE EXCEPTION 'FULL_PAPER_OPPORTUNITY_OUTCOME_INVALID_LIMIT';
    END IF;

    WITH candidates AS (
        SELECT
            observation.*,
            horizon.horizon_minutes,
            CASE
              WHEN observation.observed_at=date_trunc('minute',observation.observed_at)
                THEN observation.observed_at
              ELSE date_trunc('minute',observation.observed_at)+interval '1 minute'
            END AS evaluation_start_at
        FROM public.paper_opportunity_observation_v1 observation
        CROSS JOIN (VALUES(15),(30),(60),(240)) horizon(horizon_minutes)
        WHERE observation.deployment_id=p_deployment_id
          AND observation.outcome_eligible
          AND NOT EXISTS (
              SELECT 1 FROM public.paper_opportunity_outcome_v1 existing
              WHERE existing.observation_id=observation.observation_id
                AND existing.horizon_minutes=horizon.horizon_minutes
          )
        ORDER BY observation.observed_at,observation.observation_id,
                 horizon.horizon_minutes
        LIMIT p_limit
    ), finalizable AS (
        SELECT candidate.*,
               candidate.evaluation_start_at
                 +make_interval(mins=>candidate.horizon_minutes) AS evaluation_end_at
        FROM candidates candidate
        WHERE clock_timestamp()>=candidate.evaluation_start_at
              +make_interval(mins=>candidate.horizon_minutes)
    ), market AS (
        SELECT
            candidate.observation_id,candidate.horizon_minutes,
            candidate.evaluation_start_at,candidate.evaluation_end_at,
            candidate.reference_price,candidate.opportunity_direction,
            candidate.full_cost_hurdle_pct,candidate.symbol,
            candidate.source_revision,
            count(candle.open_time)::integer AS market_rows_used,
            (count(candle.open_time)-count(DISTINCT candle.open_time))::integer
                AS duplicate_market_rows,
            (candidate.horizon_minutes-count(DISTINCT candle.open_time))::integer
                AS market_data_gaps,
            count(*) FILTER (WHERE candle.open_time IS NOT NULL
                AND (candle.high IS NULL OR candle.low IS NULL))::integer
                AS null_extrema_rows,
            min(candle.open_time) AS first_market_timestamp,
            max(candle.open_time) AS last_market_timestamp,
            max(candle.high) AS maximum_high,
            min(candle.low) AS minimum_low
        FROM finalizable candidate
        CROSS JOIN LATERAL generate_series(
            candidate.evaluation_start_at,
            candidate.evaluation_end_at-interval '1 minute',interval '1 minute'
        ) expected(open_time)
        LEFT JOIN public.candles candle
          ON candle.symbol=candidate.symbol AND candle.interval='1m'
         AND candle.open_time=expected.open_time
        GROUP BY candidate.observation_id,candidate.horizon_minutes,
            candidate.evaluation_start_at,candidate.evaluation_end_at,
            candidate.reference_price,candidate.opportunity_direction,
            candidate.full_cost_hurdle_pct,candidate.symbol,
            candidate.source_revision
    ), classified AS (
        SELECT market.*,
          CASE WHEN reference_price IS NULL OR reference_price<=0
                 THEN 'INVALID_REFERENCE'
               WHEN market_rows_used<>horizon_minutes
                 OR duplicate_market_rows<>0 OR market_data_gaps<>0
                 OR null_extrema_rows<>0
                 OR first_market_timestamp<>evaluation_start_at
                 OR last_market_timestamp<>evaluation_end_at-interval '1 minute'
                 THEN 'INCOMPLETE_MARKET_DATA'
               ELSE 'COMPLETE' END AS evidence_status_raw
        FROM market
    ), metrics AS (
        SELECT classified.*,
          CASE WHEN evidence_status_raw='COMPLETE' AND opportunity_direction='LONG'
                 THEN maximum_high
               WHEN evidence_status_raw='COMPLETE' THEN minimum_low END
                 AS maximum_favorable_price,
          CASE WHEN evidence_status_raw='COMPLETE' AND opportunity_direction='LONG'
                 THEN minimum_low
               WHEN evidence_status_raw='COMPLETE' THEN maximum_high END
                 AS maximum_adverse_price,
          CASE WHEN evidence_status_raw='COMPLETE' AND opportunity_direction='LONG'
                 THEN greatest(0,(maximum_high-reference_price)/reference_price*100)
               WHEN evidence_status_raw='COMPLETE'
                 THEN greatest(0,(reference_price-minimum_low)/reference_price*100)
               END AS mfe_pct,
          CASE WHEN evidence_status_raw='COMPLETE' AND opportunity_direction='LONG'
                 THEN least(0,(minimum_low-reference_price)/reference_price*100)
               WHEN evidence_status_raw='COMPLETE'
                 THEN least(0,(reference_price-maximum_high)/reference_price*100)
               END AS mae_pct
        FROM classified
    ), timed AS (
        SELECT metrics.*,
          cover.covered_at,mfe_time.mfe_at
        FROM metrics
        LEFT JOIN LATERAL (
          SELECT min(c.open_time) AS covered_at
          FROM public.candles c
          WHERE metrics.evidence_status_raw='COMPLETE'
            AND c.symbol=metrics.symbol AND c.interval='1m'
            AND c.open_time>=metrics.evaluation_start_at
            AND c.open_time<metrics.evaluation_end_at
            AND CASE metrics.opportunity_direction
                  WHEN 'LONG' THEN (c.high-metrics.reference_price)
                    /metrics.reference_price*100>=metrics.full_cost_hurdle_pct
                  ELSE (metrics.reference_price-c.low)
                    /metrics.reference_price*100>=metrics.full_cost_hurdle_pct
                END
        ) cover ON true
        LEFT JOIN LATERAL (
          SELECT min(c.open_time) AS mfe_at
          FROM public.candles c
          WHERE metrics.evidence_status_raw='COMPLETE'
            AND c.symbol=metrics.symbol AND c.interval='1m'
            AND c.open_time>=metrics.evaluation_start_at
            AND c.open_time<metrics.evaluation_end_at
            AND CASE metrics.opportunity_direction
                  WHEN 'LONG' THEN c.high=metrics.maximum_favorable_price
                  ELSE c.low=metrics.maximum_favorable_price
                END
        ) mfe_time ON true
    ), inserted AS (
      INSERT INTO public.paper_opportunity_outcome_v1(
        observation_id,horizon_minutes,evaluation_start_at,evaluation_end_at,
        market_data_source,market_data_granularity,market_rows_expected,
        market_rows_used,duplicate_market_rows,market_data_gaps,
        first_market_timestamp,last_market_timestamp,reference_price,
        opportunity_direction,full_cost_hurdle_pct,maximum_favorable_price,
        maximum_adverse_price,mfe_pct,mae_pct,covered_full_costs,
        time_to_full_cost_cover_seconds,time_to_mfe_seconds,economic_label,
        evidence_status,status_reason,source_revision,producer_version,payload_hash
      )
      SELECT observation_id,horizon_minutes,evaluation_start_at,evaluation_end_at,
        'candles','1m',horizon_minutes,market_rows_used,duplicate_market_rows,
        market_data_gaps,first_market_timestamp,last_market_timestamp,
        reference_price,opportunity_direction,full_cost_hurdle_pct,
        maximum_favorable_price,maximum_adverse_price,mfe_pct,mae_pct,
        CASE WHEN evidence_status_raw='COMPLETE'
               THEN mfe_pct>=full_cost_hurdle_pct END,
        CASE WHEN covered_at IS NOT NULL
               THEN extract(epoch FROM covered_at-evaluation_start_at) END,
        CASE WHEN mfe_at IS NOT NULL
               THEN extract(epoch FROM mfe_at-evaluation_start_at) END,
        CASE WHEN evidence_status_raw<>'COMPLETE' THEN 'EVIDENCE_INCOMPLETE'
             WHEN mfe_pct>=full_cost_hurdle_pct THEN 'ECONOMICALLY_VIABLE'
             ELSE 'NEVER_COVERED_FULL_COSTS' END,
        CASE WHEN evidence_status_raw='COMPLETE' THEN 'COMPLETE' ELSE 'INCOMPLETE' END,
        CASE WHEN evidence_status_raw='COMPLETE' THEN NULL ELSE evidence_status_raw END,
        source_revision,'FULL_PAPER_OPPORTUNITY_OUTCOME_V1',
        encode(digest(concat_ws('|',observation_id::text,horizon_minutes::text,
          evaluation_start_at::text,evaluation_end_at::text,
          coalesce(reference_price::text,''),coalesce(mfe_pct::text,''),
          coalesce(mae_pct::text,''),evidence_status_raw),'sha256'),'hex')
      FROM timed
      ON CONFLICT (observation_id,horizon_minutes) DO NOTHING
      RETURNING 1
    ) SELECT count(*) INTO inserted_count FROM inserted;
    RETURN inserted_count;
END
$function$;

CREATE OR REPLACE VIEW public.v_paper_opportunity_outcome_v1 AS
SELECT observation.observation_id,observation.observation_key,
       observation.deployment_id,observation.strategy,observation.symbol,
       observation.interval,observation.observation_type,
       observation.decision_reason,observation.candle_open_time,
       horizon.horizon_minutes,
       COALESCE(outcome.economic_label,
         CASE WHEN clock_timestamp()<
           (CASE WHEN observation.observed_at=date_trunc('minute',observation.observed_at)
              THEN observation.observed_at
              ELSE date_trunc('minute',observation.observed_at)+interval '1 minute'
            END)+make_interval(mins=>horizon.horizon_minutes)
           THEN 'NOT_YET_MATURE' ELSE 'EVIDENCE_INCOMPLETE' END
       ) AS economic_label,
       outcome.evidence_status,outcome.mfe_pct,outcome.mae_pct,
       outcome.covered_full_costs,outcome.time_to_full_cost_cover_seconds,
       outcome.time_to_mfe_seconds,observation.full_cost_hurdle_pct,
       outcome.status_reason,outcome.created_at AS outcome_created_at
FROM public.paper_opportunity_observation_v1 observation
CROSS JOIN (VALUES(15),(30),(60),(240)) horizon(horizon_minutes)
LEFT JOIN public.paper_opportunity_outcome_v1 outcome
  ON outcome.observation_id=observation.observation_id
 AND outcome.horizon_minutes=horizon.horizon_minutes
WHERE observation.outcome_eligible;

INSERT INTO public.automation_kv(key,value,updated_at) VALUES
 ('full_paper_opportunity_observation_v1_last_status','NOT_RUN',now()),
 ('full_paper_opportunity_observation_v1_last_success_at','never',now()),
 ('full_paper_opportunity_observation_v1_last_inserted','0',now())
ON CONFLICT(key) DO NOTHING;

COMMIT;

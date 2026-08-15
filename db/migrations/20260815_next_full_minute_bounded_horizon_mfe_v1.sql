\set ON_ERROR_STOP on

BEGIN;

DO $prerequisites$
BEGIN
    IF to_regclass('public.entry_opportunity_evidence_v1') IS NULL
       OR to_regclass('public.candles') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION 'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1_PREREQUISITE_MISSING';
    END IF;
END
$prerequisites$;

CREATE TABLE IF NOT EXISTS public.entry_opportunity_bounded_horizon_labels_v1 (
    label_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL
        REFERENCES public.entry_opportunity_evidence_v1(snapshot_id) ON DELETE RESTRICT,
    decision_id UUID NOT NULL,
    environment TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    runtime_deployment_id TEXT NOT NULL,

    target_version TEXT NOT NULL,
    horizon_minutes SMALLINT NOT NULL,
    prediction_anchor_at TIMESTAMPTZ NOT NULL,
    evaluation_start_at TIMESTAMPTZ NOT NULL,
    evaluation_end_at TIMESTAMPTZ NOT NULL,
    alignment_delay_ms INTEGER NOT NULL,

    direction TEXT,
    reference_price NUMERIC,
    reference_price_source TEXT NOT NULL,
    reference_price_timestamp TIMESTAMPTZ,

    market_data_source TEXT NOT NULL,
    market_data_granularity TEXT NOT NULL,
    market_data_start_at TIMESTAMPTZ NOT NULL,
    market_data_end_at TIMESTAMPTZ NOT NULL,
    market_rows_expected INTEGER NOT NULL,
    market_rows_used INTEGER NOT NULL,
    duplicate_market_rows INTEGER NOT NULL,
    market_data_gaps INTEGER NOT NULL,
    first_market_timestamp TIMESTAMPTZ,
    last_market_timestamp TIMESTAMPTZ,

    bounded_mfe_pct NUMERIC,
    label_status TEXT NOT NULL,
    status_reason TEXT,
    source_revision TEXT,
    producer_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    payload_hash TEXT NOT NULL,

    CONSTRAINT uq_entry_opportunity_bounded_horizon_label_v1
        UNIQUE(snapshot_id,target_version,horizon_minutes),
    CONSTRAINT ck_bounded_horizon_target_v1 CHECK (
        target_version='NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1'
    ),
    CONSTRAINT ck_bounded_horizon_minutes_v1 CHECK (
        horizon_minutes IN (15,30,60)
    ),
    CONSTRAINT ck_bounded_horizon_environment_v1 CHECK (
        environment='trading_paper'
    ),
    CONSTRAINT ck_bounded_horizon_deployment_v1 CHECK (
        (deployment_id='LOCAL' AND runtime_deployment_id='local-paper')
        OR (deployment_id='VPS' AND runtime_deployment_id='vps-paper')
    ),
    CONSTRAINT ck_bounded_horizon_alignment_v1 CHECK (
        alignment_delay_ms>=0 AND alignment_delay_ms<60000
        AND evaluation_start_at>=prediction_anchor_at
        AND evaluation_end_at=evaluation_start_at
            + make_interval(mins=>horizon_minutes)
        AND market_data_start_at=evaluation_start_at
        AND market_data_end_at=evaluation_end_at
    ),
    CONSTRAINT ck_bounded_horizon_counts_v1 CHECK (
        market_rows_expected=horizon_minutes
        AND market_rows_used>=0
        AND duplicate_market_rows>=0
        AND market_data_gaps>=0
    ),
    CONSTRAINT ck_bounded_horizon_status_v1 CHECK (
        label_status IN (
            'PENDING_HORIZON','COMPLETE','INCOMPLETE_MARKET_DATA',
            'INVALID_REFERENCE','UNSUPPORTED_DIRECTION','SOURCE_MISSING','ERROR'
        )
    ),
    CONSTRAINT ck_bounded_horizon_complete_payload_v1 CHECK (
        (label_status='COMPLETE'
         AND direction IN ('LONG','SHORT')
         AND reference_price>0
         AND reference_price_timestamp IS NOT NULL
         AND market_rows_used=horizon_minutes
         AND duplicate_market_rows=0
         AND market_data_gaps=0
         AND first_market_timestamp=evaluation_start_at
         AND last_market_timestamp=evaluation_end_at-interval '1 minute'
         AND bounded_mfe_pct IS NOT NULL
         AND bounded_mfe_pct>=0)
        OR
        (label_status<>'COMPLETE' AND bounded_mfe_pct IS NULL)
    ),
    CONSTRAINT ck_bounded_horizon_hash_v1 CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    )
);

COMMENT ON TABLE public.entry_opportunity_bounded_horizon_labels_v1 IS
    'Immutable terminal labels for favorable excursion across H complete canonical 1-minute candles beginning at the first full minute boundary at or after an immutable Entry Opportunity snapshot.';
COMMENT ON COLUMN public.entry_opportunity_bounded_horizon_labels_v1.alignment_delay_ms IS
    'Diagnostic only. It must not weight, correct, gate, or reject an Expected Move prediction.';

CREATE INDEX IF NOT EXISTS ix_entry_opportunity_bounded_labels_queue_v1
    ON public.entry_opportunity_bounded_horizon_labels_v1(
        deployment_id,label_status,horizon_minutes,prediction_anchor_at
    );

CREATE OR REPLACE FUNCTION public.guard_entry_opportunity_bounded_label_immutable_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'ENTRY_OPPORTUNITY_BOUNDED_HORIZON_LABEL_IMMUTABLE';
END
$function$;

CREATE OR REPLACE FUNCTION public.validate_entry_opportunity_bounded_label_identity_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    snapshot_decision_id UUID;
    snapshot_environment TEXT;
    snapshot_deployment_id TEXT;
BEGIN
    SELECT
        snapshot.decision_id,snapshot.environment,snapshot.deployment_id
    INTO
        snapshot_decision_id,snapshot_environment,snapshot_deployment_id
    FROM public.entry_opportunity_evidence_v1 snapshot
    WHERE snapshot.snapshot_id=NEW.snapshot_id;

    IF snapshot_decision_id IS NULL
       OR NEW.decision_id IS DISTINCT FROM snapshot_decision_id
       OR NEW.environment IS DISTINCT FROM snapshot_environment
       OR NEW.deployment_id IS DISTINCT FROM snapshot_deployment_id THEN
        RAISE EXCEPTION 'ENTRY_OPPORTUNITY_BOUNDED_HORIZON_LABEL_IDENTITY_MISMATCH';
    END IF;
    RETURN NEW;
END
$function$;

DROP TRIGGER IF EXISTS trg_entry_opportunity_bounded_label_identity_v1
    ON public.entry_opportunity_bounded_horizon_labels_v1;
CREATE TRIGGER trg_entry_opportunity_bounded_label_identity_v1
BEFORE INSERT ON public.entry_opportunity_bounded_horizon_labels_v1
FOR EACH ROW EXECUTE FUNCTION public.validate_entry_opportunity_bounded_label_identity_v1();

DROP TRIGGER IF EXISTS trg_entry_opportunity_bounded_label_immutable_v1
    ON public.entry_opportunity_bounded_horizon_labels_v1;
CREATE TRIGGER trg_entry_opportunity_bounded_label_immutable_v1
BEFORE UPDATE OR DELETE ON public.entry_opportunity_bounded_horizon_labels_v1
FOR EACH ROW EXECUTE FUNCTION public.guard_entry_opportunity_bounded_label_immutable_v1();

CREATE OR REPLACE FUNCTION public.refresh_entry_opportunity_bounded_horizon_labels_v1(
    p_environment TEXT DEFAULT 'trading_paper',
    p_deployment_id TEXT DEFAULT 'LOCAL',
    p_limit INTEGER DEFAULT 500
)
RETURNS BIGINT
LANGUAGE plpgsql
VOLATILE
AS $function$
DECLARE
    inserted_count BIGINT;
BEGIN
    IF p_environment IS DISTINCT FROM 'trading_paper'
       OR p_deployment_id NOT IN ('LOCAL','VPS') THEN
        RAISE EXCEPTION 'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1_DEPLOYMENT_NOT_ALLOWED';
    END IF;
    IF p_limit IS NULL OR p_limit<1 OR p_limit>5000 THEN
        RAISE EXCEPTION 'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1_INVALID_LIMIT';
    END IF;

    WITH snapshot_base AS (
        SELECT
            snapshot.snapshot_id,
            snapshot.decision_id,
            snapshot.environment,
            snapshot.deployment_id,
            CASE snapshot.deployment_id
                WHEN 'LOCAL' THEN 'local-paper'
                WHEN 'VPS' THEN 'vps-paper'
            END AS runtime_deployment_id,
            snapshot.symbol,
            snapshot.captured_at AS prediction_anchor_at,
            CASE
                WHEN snapshot.captured_at=date_trunc('minute',snapshot.captured_at)
                    THEN snapshot.captured_at
                ELSE date_trunc('minute',snapshot.captured_at)+interval '1 minute'
            END AS evaluation_start_at,
            CASE
                WHEN jsonb_typeof(snapshot.strategy_features->'price')='number'
                    THEN (snapshot.strategy_features->>'price')::numeric
            END AS reference_price,
            CASE
                WHEN pg_input_is_valid(
                    snapshot.strategy_features->>'signal_created_at',
                    'timestamp with time zone'
                ) THEN (snapshot.strategy_features->>'signal_created_at')::timestamptz
            END AS reference_price_timestamp,
            CASE snapshot.signal_action
                WHEN 'BUY' THEN 'LONG'
                WHEN 'SELL' THEN 'SHORT'
            END AS direction,
            snapshot.source_revision
        FROM public.entry_opportunity_evidence_v1 snapshot
        WHERE snapshot.environment=p_environment
          AND snapshot.deployment_id=p_deployment_id
    ),
    candidate_universe AS (
        SELECT
            base.*,
            horizon.horizon_minutes,
            base.evaluation_start_at
                + make_interval(mins=>horizon.horizon_minutes) AS evaluation_end_at,
            floor(extract(epoch FROM(
                base.evaluation_start_at-base.prediction_anchor_at
            ))*1000)::integer AS alignment_delay_ms
        FROM snapshot_base base
        CROSS JOIN (VALUES(15),(30),(60)) horizon(horizon_minutes)
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.entry_opportunity_bounded_horizon_labels_v1 existing
            WHERE existing.snapshot_id=base.snapshot_id
              AND existing.target_version='NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1'
              AND existing.horizon_minutes=horizon.horizon_minutes
        )
        ORDER BY base.prediction_anchor_at,base.snapshot_id,horizon.horizon_minutes
        LIMIT p_limit
    ),
    finalizable AS (
        SELECT candidate.*
        FROM candidate_universe candidate
        WHERE clock_timestamp()>=candidate.evaluation_end_at
          AND (
              SELECT max(candle.open_time)
              FROM public.candles candle
              WHERE candle.symbol=candidate.symbol
                AND candle.interval='1m'
          )>=candidate.evaluation_end_at
    ),
    market_evidence AS (
        SELECT
            candidate.*,
            count(candle.open_time)::integer AS market_rows_used,
            (
                count(candle.open_time)
                - count(DISTINCT candle.open_time)
            )::integer AS duplicate_market_rows,
            (
                candidate.horizon_minutes
                - count(DISTINCT candle.open_time)
            )::integer AS market_data_gaps,
            count(*) FILTER (
                WHERE candle.open_time IS NOT NULL
                  AND (candle.high IS NULL OR candle.low IS NULL)
            )::integer AS null_extrema_rows,
            min(candle.open_time) AS first_market_timestamp,
            max(candle.open_time) AS last_market_timestamp,
            max(candle.high) AS maximum_high,
            min(candle.low) AS minimum_low
        FROM finalizable candidate
        CROSS JOIN LATERAL generate_series(
            candidate.evaluation_start_at,
            candidate.evaluation_end_at-interval '1 minute',
            interval '1 minute'
        ) expected(open_time)
        LEFT JOIN public.candles candle
          ON candle.symbol=candidate.symbol
         AND candle.interval='1m'
         AND candle.open_time=expected.open_time
        GROUP BY
            candidate.snapshot_id,candidate.decision_id,candidate.environment,
            candidate.deployment_id,candidate.runtime_deployment_id,
            candidate.symbol,candidate.prediction_anchor_at,
            candidate.evaluation_start_at,candidate.reference_price,
            candidate.reference_price_timestamp,candidate.direction,
            candidate.source_revision,candidate.horizon_minutes,
            candidate.evaluation_end_at,candidate.alignment_delay_ms
    ),
    classified AS (
        SELECT
            evidence.*,
            CASE
                WHEN evidence.reference_price IS NULL
                  OR evidence.reference_price<=0
                  OR evidence.reference_price_timestamp IS NULL
                    THEN 'INVALID_REFERENCE'
                WHEN evidence.direction IS NULL
                    THEN 'UNSUPPORTED_DIRECTION'
                WHEN evidence.market_rows_used<>evidence.horizon_minutes
                  OR evidence.duplicate_market_rows<>0
                  OR evidence.market_data_gaps<>0
                  OR evidence.null_extrema_rows<>0
                  OR evidence.first_market_timestamp<>evidence.evaluation_start_at
                  OR evidence.last_market_timestamp<>
                     evidence.evaluation_end_at-interval '1 minute'
                    THEN 'INCOMPLETE_MARKET_DATA'
                ELSE 'COMPLETE'
            END AS label_status
        FROM market_evidence evidence
    ),
    payload AS (
        SELECT
            classified.*,
            CASE
                WHEN classified.label_status='COMPLETE'
                 AND classified.direction='LONG' THEN greatest(
                    0::numeric,
                    (classified.maximum_high-classified.reference_price)
                    /classified.reference_price*100
                 )
                WHEN classified.label_status='COMPLETE'
                 AND classified.direction='SHORT' THEN greatest(
                    0::numeric,
                    (classified.reference_price-classified.minimum_low)
                    /classified.reference_price*100
                 )
            END AS bounded_mfe_pct,
            CASE classified.label_status
                WHEN 'INVALID_REFERENCE' THEN 'FROZEN_SIGNAL_REFERENCE_INVALID'
                WHEN 'UNSUPPORTED_DIRECTION' THEN 'FROZEN_SIGNAL_DIRECTION_UNSUPPORTED'
                WHEN 'INCOMPLETE_MARKET_DATA' THEN 'EXPECTED_1M_CANDLE_IDENTITY_INCOMPLETE'
            END AS status_reason
        FROM classified
    ),
    inserted AS (
        INSERT INTO public.entry_opportunity_bounded_horizon_labels_v1(
            snapshot_id,decision_id,environment,deployment_id,
            runtime_deployment_id,target_version,horizon_minutes,
            prediction_anchor_at,evaluation_start_at,evaluation_end_at,
            alignment_delay_ms,direction,reference_price,
            reference_price_source,reference_price_timestamp,
            market_data_source,market_data_granularity,
            market_data_start_at,market_data_end_at,
            market_rows_expected,market_rows_used,duplicate_market_rows,
            market_data_gaps,first_market_timestamp,last_market_timestamp,
            bounded_mfe_pct,label_status,status_reason,source_revision,
            producer_version,payload_hash
        )
        SELECT
            payload.snapshot_id,payload.decision_id,payload.environment,
            payload.deployment_id,payload.runtime_deployment_id,
            'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1',
            payload.horizon_minutes,payload.prediction_anchor_at,
            payload.evaluation_start_at,payload.evaluation_end_at,
            payload.alignment_delay_ms,payload.direction,
            payload.reference_price,'FROZEN_STRATEGY_SIGNAL_EVENT_PRICE',
            payload.reference_price_timestamp,'candles','1m',
            payload.evaluation_start_at,payload.evaluation_end_at,
            payload.horizon_minutes,payload.market_rows_used,
            payload.duplicate_market_rows,payload.market_data_gaps,
            payload.first_market_timestamp,payload.last_market_timestamp,
            payload.bounded_mfe_pct,payload.label_status,payload.status_reason,
            payload.source_revision,
            'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_PRODUCER_V1',
            encode(digest(concat_ws('|',
                payload.snapshot_id::text,payload.decision_id::text,
                payload.deployment_id,payload.horizon_minutes::text,
                payload.prediction_anchor_at::text,
                payload.evaluation_start_at::text,
                payload.evaluation_end_at::text,payload.alignment_delay_ms::text,
                coalesce(payload.direction,'<NULL>'),
                coalesce(payload.reference_price::text,'<NULL>'),
                coalesce(payload.reference_price_timestamp::text,'<NULL>'),
                payload.market_rows_used::text,
                payload.duplicate_market_rows::text,payload.market_data_gaps::text,
                coalesce(payload.bounded_mfe_pct::text,'<NULL>'),
                payload.label_status,coalesce(payload.source_revision,'<NULL>')
            ),'sha256'),'hex')
        FROM payload
        ON CONFLICT(snapshot_id,target_version,horizon_minutes) DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO inserted_count FROM inserted;

    RETURN inserted_count;
END
$function$;

COMMENT ON FUNCTION public.refresh_entry_opportunity_bounded_horizon_labels_v1(TEXT,TEXT,INTEGER) IS
    'Bounded, deployment-aware, idempotent producer. It reads immutable Entry Opportunity snapshots and canonical 1m candles and writes only terminal bounded-horizon label evidence.';

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,
    schema_baseline_version
)
SELECT
    '20260815_next_full_minute_bounded_horizon_mfe_v1.sql',
    COALESCE(
        NULLIF(current_setting('waltrade.migration_checksum',true),''),
        repeat('0',64)
    ),
    'PAPER',
    COALESCE(
        NULLIF(current_setting('waltrade.target_deployment_id',true),''),
        'LOCAL'
    ),
    current_database(),'operator-migration','APPLIED',TRUE,0,
    COALESCE(
        NULLIF(current_setting('waltrade.git_sha',true),''),repeat('0',40)
    ),
    'NEXT_FULL_MINUTE_BOUNDED_HORIZON_MFE_V1'
WHERE NOT EXISTS (
    SELECT 1
    FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260815_next_full_minute_bounded_horizon_mfe_v1.sql'
);

COMMIT;

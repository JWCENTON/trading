-- LONG HORIZON L3 DIRECT LOCAL PAPER V1 (LOCAL PAPER ONLY)
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';
DO $guard$
BEGIN
  IF current_database()<>'trading_paper'
     AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_LOCAL_PAPER_ONLY';
  END IF;
  IF current_setting('waltrade.target_deployment_id',true) IS DISTINCT FROM 'local-paper' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_LOCAL_PAPER_DEPLOYMENT_REQUIRED';
  END IF;
END $guard$;

CREATE TABLE IF NOT EXISTS public.long_horizon_l3_contract_v1(
 contract_id bigserial PRIMARY KEY, contract_version text NOT NULL UNIQUE,
 treatment_fingerprint text NOT NULL, source_revision text NOT NULL,
 start_cutoff timestamptz NOT NULL, status text NOT NULL,
 contract_payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.long_horizon_l3_admission_v1(
 admission_id bigserial PRIMARY KEY, gate_event_id bigint NOT NULL UNIQUE REFERENCES regime_gate_events(id),
 cohort text NOT NULL, sampling_identity text NOT NULL, sampling_bucket integer NOT NULL,
 same_thesis_identity text NOT NULL, symbol text NOT NULL, interval text NOT NULL,
 strategy text NOT NULL, side text NOT NULL, entry_candle_open_time timestamptz NOT NULL,
 entry_notional numeric NOT NULL, status text NOT NULL, decision_id uuid,
 snapshot_id uuid, simulated_order_id bigint REFERENCES simulated_orders(id),
 position_id bigint REFERENCES positions(id), linked_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK(cohort IN ('L3_REGIME_WOULD_ALLOW','L3_REGIME_WOULD_BLOCK_SAMPLE')),
 CHECK(sampling_bucket BETWEEN 0 AND 9)
);
CREATE INDEX IF NOT EXISTS ix_l3_same_thesis
 ON public.long_horizon_l3_admission_v1(same_thesis_identity,status);
CREATE TABLE IF NOT EXISTS public.long_horizon_l3_event_v1(
 event_id bigserial PRIMARY KEY, admission_id bigint NOT NULL REFERENCES long_horizon_l3_admission_v1,
 position_id bigint NOT NULL REFERENCES positions(id), event_type text NOT NULL,
 source_candle_id text NOT NULL, source_close_time timestamptz NOT NULL,
 mark_price numeric NOT NULL, realizable_net numeric NOT NULL, entry_capital numeric NOT NULL,
 target_rate numeric NOT NULL, target_reached boolean NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(position_id,source_candle_id)
);
CREATE TABLE IF NOT EXISTS public.long_horizon_l3_l0_comparator_v1(
 comparator_id bigserial PRIMARY KEY, admission_id bigint NOT NULL REFERENCES long_horizon_l3_admission_v1,
 position_id bigint NOT NULL UNIQUE REFERENCES positions(id), first_exit_at timestamptz NOT NULL,
 exit_reason text NOT NULL, exit_price numeric NOT NULL, source_candle_open_time timestamptz NOT NULL,
 status text NOT NULL, realizable_net_at_l0_exit numeric,
 fee_contract_fingerprint text, final_status text, final_net numeric, completed_at timestamptz
);

WITH frozen AS (
 SELECT clock_timestamp() AS cutoff,
        jsonb_build_object(
          'contract_version','LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1',
          'environment','PAPER','deployment_id','local-paper',
          'source_revision',current_setting('waltrade.migration_git_sha',true),
          'regime_decision','COMPUTE_AND_PERSIST',
          'regime_entry_authority','NON_BLOCKING',
          'effective_research_mode','DRY_RUN_NON_BLOCKING',
          'primary_cohort','L3_REGIME_WOULD_ALLOW',
          'secondary_cohort','L3_REGIME_WOULD_BLOCK_SAMPLE',
          'sampling','SHA256(canonical gate-event opportunity identity) as integer mod 10 = 0',
          'sampling_version','L3_SHA256_GATE_EVENT_ID_MOD10_EQ0_V1',
          'same_thesis','P4_15M_SYMBOL_SIDE_REGIME_V1',
          'target_realizable_net_rate','0.03',
          'fee_model','PAPER_SIMULATION_FEE_V2_TAKER_TAKER_0.0035_PER_SIDE',
          'primary_sleeve_rate','0.40','secondary_sleeve_rate','0.20',
          'global_heat_rate','0.60','minimum_free_cash_rate','0.20',
          'entry_notional_usdc','20',
          'l0_comparator_version','LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1',
          'disabled_exits',jsonb_build_array('TAKE_PROFIT','PROFIT_LOCK_TRAIL_DROP',
             'PROFIT_LOCK_FLOOR','ECONOMIC_FLOOR_V1','ECONOMIC_FLOOR_V2',
             'SOFT_EXIT','EARLY_CUT','GUARDED_PROFIT_EXIT','TIME_EXIT'),
          'preserved_risk_exits',jsonb_build_array('HARD_STOP_LOSS','PANIC',
             'MANUAL_EMERGENCY','INTEGRITY_EMERGENCY','ACTIVE_FORCED_RISK_BUDGET'),
          'checkpoint_horizons',jsonb_build_array('24H','3D','7D','14D','30D','90D'),
          'evidence_fields',jsonb_build_array(
             'regime_gate_event_id','decision_id','snapshot_id','simulated_order_id',
             'position_id','fee_model','financial_truth','l3_outcome','paired_l0_outcome',
             'mark_to_market_truth','mfe','mae','time_to_break_even','time_to_net_1pct',
             'time_to_net_2pct','time_to_net_3pct','time_to_net_5pct',
             'outcome_1d','outcome_3d','outcome_7d','outcome_14d','outcome_30d',
             'outcome_90d','capital_hours','trapped_capital','capital_rejections',
             'same_thesis_rejections','realized_net','unrealized_net','total_equity'),
          'horizons_are_time_exits',false,
          'prior_evidence_burned',true,
          'no_causal_thesis_invalidation_rule',true
        ) AS payload
), identified AS (
 SELECT cutoff,payload,
        encode(digest(convert_to(payload::text,'UTF8'),'sha256'),'hex') AS fp
 FROM frozen
)
INSERT INTO public.long_horizon_l3_contract_v1(
 contract_version,treatment_fingerprint,source_revision,start_cutoff,status,contract_payload)
SELECT 'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1',fp,
       current_setting('waltrade.migration_git_sha',true),cutoff,'ACTIVE',
       payload || jsonb_build_object('start_cutoff',cutoff,'treatment_fingerprint',fp)
FROM identified
ON CONFLICT(contract_version) DO NOTHING;

UPDATE public.bot_control SET regime_enabled=true,regime_mode='DRY_RUN',updated_at=clock_timestamp()
 WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE');
DO $slots$ BEGIN
 IF (SELECT count(*) FROM public.bot_control WHERE regime_enabled AND regime_mode='DRY_RUN'
     AND strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
  RAISE EXCEPTION 'L3_REGIME_DRY_RUN_SLOT_COVERAGE_NOT_32';
 END IF;
END $slots$;

INSERT INTO public.schema_migration_ledger_v1(
 migration_id,checksum_sha256,environment,deployment_id,database_name,
 applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version)
SELECT '20260907_long_horizon_l3_direct_local_paper_v1',
 current_setting('waltrade.migration_checksum',true),'PAPER','local-paper',
 current_database(),current_user,'APPLIED',true,0,
 current_setting('waltrade.migration_git_sha',true),'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1'
WHERE NOT EXISTS (SELECT 1 FROM public.schema_migration_ledger_v1
 WHERE migration_id='20260907_long_horizon_l3_direct_local_paper_v1'
 AND environment='PAPER' AND deployment_id='local-paper' AND success);
COMMIT;

-- LONG HORIZON L3 DIRECT LOCAL PAPER V2 (LOCAL PAPER ONLY)
-- Idempotency correction for the terminated V1 activation.  This migration
-- deliberately reuses the V1 evidence schema and never mutates V1 history.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';

DO $guard$
BEGIN
  IF current_database()<>'trading_paper'
     AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V2_LOCAL_PAPER_ONLY';
  END IF;
  IF current_setting('waltrade.target_deployment_id',true) IS DISTINCT FROM 'local-paper' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V2_LOCAL_PAPER_DEPLOYMENT_REQUIRED';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2',0));
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1'
         AND status='TERMINATED')<>1 THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V1_TERMINATED_PRECONDITION_REQUIRED';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.positions
     WHERE id IN (13546,13547,13548,13549,13550) AND status='OPEN'
  ) THEN
    RAISE EXCEPTION 'L3_V2_TRANSITIONAL_POSITIONS_STILL_OPEN';
  END IF;
  IF (SELECT count(*) FROM public.bot_control
       WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
    RAISE EXCEPTION 'L3_V2_BOT_CONTROL_SLOT_COVERAGE_NOT_32';
  END IF;
END $guard$;

WITH semantic_contract AS (
  SELECT jsonb_build_object(
    'contract_version','LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2',
    'environment','PAPER','deployment_id','local-paper',
    'regime_decision','COMPUTE_AND_PERSIST',
    'regime_entry_authority','NON_BLOCKING',
    'effective_research_mode','DRY_RUN_NON_BLOCKING',
    'primary_cohort','L3_REGIME_WOULD_ALLOW',
    'secondary_cohort','L3_REGIME_WOULD_BLOCK_SAMPLE',
    'sampling','uint256(SHA256(canonical_opportunity_id|frozen_sampling_salt|cohort_name|contract_version)) < exact cohort threshold',
    'sampling_version','L3_POWER_CALIBRATED_SALTED_SHA256_THRESHOLD_V1',
    'sampling_salt','0487462154f625b36982d5437a9d039ff8ceb5db5e2835afc0d47e6908a16057',
    'allow_sampling_probability','0.13194281540',
    'block_sampling_probability','0.07920637611',
    'allow_sampling_threshold_uint256','15277934255019537564202551280000000000000000000000000000000000000000000000000',
    'block_sampling_threshold_uint256','9171471770693549621713624198000000000000000000000000000000000000000000000000',
    'allow_required_completed_episodes',33,
    'block_required_completed_episodes',53,
    'one_sided_alpha','0.05','power','0.80','enrollment_target_days',14,
    'allow_expected_censored','41/232','block_expected_censored','155/666',
    'independence_unit','CANONICAL_SAME_THESIS_EPISODE',
    'same_thesis','P4_15M_SYMBOL_SIDE_REGIME_V1',
    'target_realizable_net_rate','0.03',
    'fee_model','PAPER_SIMULATION_FEE_V2_TAKER_TAKER_0.0035_PER_SIDE',
    'primary_sleeve_rate','0.40','secondary_sleeve_rate','0.20',
    'global_heat_rate','0.60','minimum_free_cash_rate','0.20',
    'entry_notional_usdc','9',
    'minimum_equity_for_block_capacity_usdc','585',
    'instrument_minimum_guard','MIN_NOTIONAL_NOT_MET_FAIL_CLOSED_NO_AUTO_INCREASE',
    'capacity_pause_below_minimum_equity',true,
    'same_thesis_max_active',1,
    'pre_cutoff_classification','PRE_L3_TRANSITIONAL_EXCLUDED',
    'transitional_position_ids',jsonb_build_array(13546,13547,13548,13549,13550),
    'cohort_eligibility','REGIME_GATE_EVENT_CREATED_AT_GTE_CONTRACT_START_CUTOFF',
    'l0_comparator_version','LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1',
    'disabled_exits',jsonb_build_array('TAKE_PROFIT','PROFIT_LOCK_TRAIL_DROP',
       'PROFIT_LOCK_FLOOR','ECONOMIC_FLOOR_V1','ECONOMIC_FLOOR_V2',
       'SOFT_EXIT','EARLY_CUT','GUARDED_PROFIT_EXIT','TIME_EXIT'),
    'preserved_risk_exits',jsonb_build_array('HARD_STOP_LOSS','PANIC',
       'MANUAL_EMERGENCY','INTEGRITY_EMERGENCY','ACTIVE_FORCED_RISK_BUDGET'),
    'prior_evidence_burned',true,
    'no_causal_thesis_invalidation_rule',true
  ) AS payload
), pre_state AS (
  SELECT jsonb_agg(jsonb_build_object(
    'symbol',symbol,'interval',interval,'strategy',strategy,
    'regime_enabled',regime_enabled,'regime_mode',regime_mode,
    'updated_at',updated_at
  ) ORDER BY strategy,interval,symbol) AS snapshot
  FROM public.bot_control
  WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')
), identified AS (
  SELECT s.payload,p.snapshot,
         encode(digest(convert_to(s.payload::text,'UTF8'),'sha256'),'hex') AS fp
  FROM semantic_contract s CROSS JOIN pre_state p
), inserted AS (
  INSERT INTO public.long_horizon_l3_contract_v1(
    contract_version,treatment_fingerprint,source_revision,start_cutoff,status,contract_payload)
  SELECT 'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2',fp,
         current_setting('waltrade.migration_git_sha',true),clock_timestamp(),'ACTIVE',
         payload || jsonb_build_object(
           'source_revision',current_setting('waltrade.migration_git_sha',true),
           'pre_activation_bot_control',snapshot)
  FROM identified
  WHERE NOT EXISTS (
    SELECT 1 FROM public.long_horizon_l3_contract_v1
     WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2')
  RETURNING contract_id,start_cutoff
)
UPDATE public.long_horizon_l3_contract_v1 c
   SET contract_payload=c.contract_payload || jsonb_build_object('start_cutoff',i.start_cutoff)
  FROM inserted i WHERE c.contract_id=i.contract_id;

DO $contract$
BEGIN
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2'
         AND status='ACTIVE'
         AND treatment_fingerprint='5ac39665922d9fc5fa4b5d5a56482b760f72dfca61c601104ca1caa7ef1c5b15')<>1 THEN
    RAISE EXCEPTION 'L3_V2_ACTIVE_CONTRACT_NOT_EXACTLY_ONE';
  END IF;
END $contract$;

UPDATE public.bot_control
   SET regime_enabled=true,regime_mode='DRY_RUN',updated_at=clock_timestamp()
 WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')
   AND (regime_enabled IS DISTINCT FROM true OR regime_mode IS DISTINCT FROM 'DRY_RUN');

DO $slots$
BEGIN
  IF (SELECT count(*) FROM public.bot_control
       WHERE regime_enabled AND regime_mode='DRY_RUN'
         AND strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
    RAISE EXCEPTION 'L3_V2_REGIME_DRY_RUN_SLOT_COVERAGE_NOT_32';
  END IF;
END $slots$;

INSERT INTO public.schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version)
SELECT '20260907_long_horizon_l3_direct_local_paper_v2',
       current_setting('waltrade.migration_checksum',true),'PAPER','local-paper',
       current_database(),current_user,'APPLIED',true,0,
       current_setting('waltrade.migration_git_sha',true),
       'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V2'
WHERE NOT EXISTS (
  SELECT 1 FROM public.schema_migration_ledger_v1
   WHERE migration_id='20260907_long_horizon_l3_direct_local_paper_v2'
     AND environment='PAPER' AND deployment_id='local-paper' AND success);
COMMIT;

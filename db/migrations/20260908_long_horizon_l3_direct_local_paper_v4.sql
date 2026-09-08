-- LONG HORIZON L3 DIRECT LOCAL PAPER V4 (LOCAL PAPER ONLY)
-- Repairs evidence/exit integrity and starts a clean cohort. V3 remains audit-only.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';

DO $guard$
BEGIN
  IF current_database()<>'trading_paper'
     AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V4_LOCAL_PAPER_ONLY';
  END IF;
  IF current_setting('waltrade.target_deployment_id',true) IS DISTINCT FROM 'local-paper' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V4_LOCAL_PAPER_DEPLOYMENT_REQUIRED';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',0));
  IF (SELECT count(*) FROM public.bot_control
       WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
    RAISE EXCEPTION 'L3_V4_BOT_CONTROL_SLOT_COVERAGE_NOT_32';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM public.long_horizon_l3_contract_v1
     WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
  ) AND (SELECT count(*) FROM public.long_horizon_l3_contract_v1
          WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
            AND status='ACTIVE')<>1 THEN
    RAISE EXCEPTION 'L3_V3_ACTIVE_PRECONDITION_REQUIRED';
  END IF;
END $guard$;

ALTER TABLE public.long_horizon_l3_l0_comparator_v1
  ADD COLUMN IF NOT EXISTS gross_pnl_at_l0_exit numeric;
ALTER TABLE public.long_horizon_l3_l0_comparator_v1
  ADD COLUMN IF NOT EXISTS entry_fee_at_l0_exit numeric;
ALTER TABLE public.long_horizon_l3_l0_comparator_v1
  ADD COLUMN IF NOT EXISTS exit_fee_at_l0_exit numeric;

WITH semantic_contract AS (
  SELECT jsonb_build_object(
    'contract_version','LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',
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
    'instrument_minimum_guard','MIN_NOTIONAL_NOT_MET_FAIL_CLOSED_NO_AUTO_INCREASE',
    'same_thesis_max_active',1,
    'activation_requires_zero_open_positions',false,
    'pre_cutoff_classification','PRE_L3_V4_EXCLUDED',
    'pre_cutoff_positions_in_experimental_sleeves',false,
    'pre_cutoff_positions_in_global_exposure',true,
    'cohort_eligibility','V4_GATE_AND_ENTRY_DECISION_TIMESTAMP_GTE_CONTRACT_START_CUTOFF',
    'capacity_rejection','OPERATIONAL_OUTCOME_EXTENDS_ENROLLMENT',
    'l0_comparator_version','LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1',
    'environment_identity','CANONICAL_UPPERCASE_COMPARISON',
    'hard_risk_authority','TYPED_NORMALIZED_PRECEDENCE',
    'paired_l0_persistence','COMMIT_BEFORE_SUPPRESSED_ORDER_RETURN',
    'disabled_exits',jsonb_build_array('TAKE_PROFIT','PROFIT_LOCK_TRAIL_DROP',
       'PROFIT_LOCK_FLOOR','ECONOMIC_FLOOR_V1','ECONOMIC_FLOOR_V2',
       'SOFT_EXIT','EARLY_CUT','GUARDED_PROFIT_EXIT','TIME_EXIT'),
    'preserved_risk_exits',jsonb_build_array('HARD_STOP_LOSS','PANIC',
       'MANUAL_EMERGENCY','INTEGRITY_EMERGENCY','ACTIVE_FORCED_RISK_BUDGET'),
    'v3_formal_status','INVALID',
    'v3_invalidation_reason','MISSING_L3_LEDGER_PLUS_HARD_RISK_SUPPRESSION_PLUS_MISSING_PAIRED_L0',
    'prior_evidence_burned',true,
    'no_causal_thesis_invalidation_rule',true
  ) AS payload
), pre_control AS (
  SELECT jsonb_agg(jsonb_build_object(
    'symbol',symbol,'interval',interval,'strategy',strategy,
    'enabled',enabled,'reason',reason,'control_mode',control_mode,
    'regime_enabled',regime_enabled,'regime_mode',regime_mode,
    'updated_at',updated_at
  ) ORDER BY strategy,interval,symbol) AS snapshot
  FROM public.bot_control
  WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')
), excluded AS (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'position_id',p.id,'symbol',p.symbol,'interval',p.interval,
    'strategy',p.strategy,'side',p.side,'entry_time',p.entry_time,
    'status_at_v4_activation',p.status,
    'classification','PRE_L3_V4_EXCLUDED'
  ) ORDER BY p.id),'[]'::jsonb) AS snapshot
  FROM public.long_horizon_l3_admission_v1 a
  JOIN public.positions p ON p.id=a.position_id
  WHERE a.contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
), identified AS (
  SELECT s.payload,c.snapshot AS control_snapshot,e.snapshot AS exclusion_snapshot,
         encode(digest(convert_to(s.payload::text,'UTF8'),'sha256'),'hex') AS fp,
         clock_timestamp() AS cutoff
  FROM semantic_contract s CROSS JOIN pre_control c CROSS JOIN excluded e
)
INSERT INTO public.long_horizon_l3_contract_v1(
  contract_version,treatment_fingerprint,source_revision,start_cutoff,status,contract_payload)
SELECT 'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',fp,
       current_setting('waltrade.migration_git_sha',true),cutoff,'ACTIVE',
       payload || jsonb_build_object(
         'source_revision',current_setting('waltrade.migration_git_sha',true),
         'start_cutoff',cutoff,'treatment_fingerprint',fp,
         'pre_activation_bot_control',control_snapshot,
         'pre_v4_excluded_positions',exclusion_snapshot)
FROM identified
WHERE NOT EXISTS (
  SELECT 1 FROM public.long_horizon_l3_contract_v1
   WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4');

UPDATE public.long_horizon_l3_contract_v1
   SET status='INVALID',
       contract_payload=contract_payload || jsonb_build_object(
         'formal_status','INVALID',
         'invalidation_reason','MISSING_L3_LEDGER_PLUS_HARD_RISK_SUPPRESSION_PLUS_MISSING_PAIRED_L0',
         'invalidated_at',(SELECT start_cutoff FROM public.long_horizon_l3_contract_v1
                            WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'))
 WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
   AND status='ACTIVE';

UPDATE public.bot_control
   SET regime_enabled=true,regime_mode='DRY_RUN',updated_at=clock_timestamp()
 WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')
   AND (regime_enabled IS DISTINCT FROM true OR regime_mode IS DISTINCT FROM 'DRY_RUN');

DO $acceptance$
BEGIN
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
         AND status='ACTIVE')<>1 THEN
    RAISE EXCEPTION 'L3_V4_ACTIVE_CONTRACT_NOT_EXACTLY_ONE';
  END IF;
  IF EXISTS (SELECT 1 FROM public.long_horizon_l3_contract_v1
              WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
                AND status<>'INVALID') THEN
    RAISE EXCEPTION 'L3_V3_NOT_INVALIDATED';
  END IF;
END $acceptance$;

INSERT INTO public.schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version)
SELECT '20260908_long_horizon_l3_direct_local_paper_v4',
       current_setting('waltrade.migration_checksum',true),'PAPER','local-paper',
       current_database(),current_user,'APPLIED',true,0,
       current_setting('waltrade.migration_git_sha',true),
       'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
WHERE NOT EXISTS (
  SELECT 1 FROM public.schema_migration_ledger_v1
   WHERE migration_id='20260908_long_horizon_l3_direct_local_paper_v4'
     AND environment='PAPER' AND deployment_id='local-paper' AND success);
COMMIT;

-- LONG HORIZON L3 DIRECT LOCAL PAPER V3 (LOCAL PAPER ONLY)
-- Cutoff-based activation: pre-existing positions remain naturally managed,
-- count toward global exposure, and are excluded from both L3 cohorts.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';

DO $guard$
BEGIN
  IF current_database()<>'trading_paper'
     AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V3_LOCAL_PAPER_ONLY';
  END IF;
  IF current_setting('waltrade.target_deployment_id',true) IS DISTINCT FROM 'local-paper' THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V3_LOCAL_PAPER_DEPLOYMENT_REQUIRED';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3',0));
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V1'
         AND status='TERMINATED')<>1 THEN
    RAISE EXCEPTION 'LONG_HORIZON_L3_V1_TERMINATED_PRECONDITION_REQUIRED';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.long_horizon_l3_contract_v1
     WHERE status='ACTIVE'
       AND contract_version<>'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
  ) THEN
    RAISE EXCEPTION 'L3_OTHER_ACTIVE_CONTRACT_PRESENT';
  END IF;
  IF (SELECT count(*) FROM public.bot_control
       WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
    RAISE EXCEPTION 'L3_V3_BOT_CONTROL_SLOT_COVERAGE_NOT_32';
  END IF;
END $guard$;

ALTER TABLE public.long_horizon_l3_admission_v1
  ADD COLUMN IF NOT EXISTS contract_version text;
ALTER TABLE public.long_horizon_l3_admission_v1
  ADD COLUMN IF NOT EXISTS l0_comparator_version text;
CREATE INDEX IF NOT EXISTS ix_l3_admission_contract_status
  ON public.long_horizon_l3_admission_v1(contract_version,status);

WITH semantic_contract AS (
  SELECT jsonb_build_object(
    'contract_version','LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3',
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
    'pre_cutoff_classification','PRE_L3_EXCLUDED',
    'pre_cutoff_positions_in_experimental_sleeves',false,
    'pre_cutoff_positions_in_global_exposure',true,
    'cohort_eligibility','V3_GATE_AND_ENTRY_DECISION_TIMESTAMP_GTE_CONTRACT_START_CUTOFF',
    'capacity_rejection','OPERATIONAL_OUTCOME_EXTENDS_ENROLLMENT',
    'l0_comparator_version','LONG_HORIZON_L3_FROZEN_PRE_L3_EXIT_L0_V1',
    'disabled_exits',jsonb_build_array('TAKE_PROFIT','PROFIT_LOCK_TRAIL_DROP',
       'PROFIT_LOCK_FLOOR','ECONOMIC_FLOOR_V1','ECONOMIC_FLOOR_V2',
       'SOFT_EXIT','EARLY_CUT','GUARDED_PROFIT_EXIT','TIME_EXIT'),
    'preserved_risk_exits',jsonb_build_array('HARD_STOP_LOSS','PANIC',
       'MANUAL_EMERGENCY','INTEGRITY_EMERGENCY','ACTIVE_FORCED_RISK_BUDGET'),
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
), open_inventory AS (
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'position_id',p.id,'symbol',p.symbol,'interval',p.interval,
    'strategy',p.strategy,'side',p.side,'entry_time',p.entry_time,
    'entry_opportunity_snapshot_id',p.entry_opportunity_snapshot_id,
    'decision_id',entry_order.decision_id,
    'classification','PRE_L3_EXCLUDED'
  ) ORDER BY p.id),'[]'::jsonb) AS snapshot
  FROM public.positions p
  LEFT JOIN LATERAL (
    SELECT s.decision_id
      FROM public.simulated_orders s
     WHERE s.position_id=p.id AND NOT COALESCE(s.is_exit,false)
     ORDER BY s.id DESC LIMIT 1
  ) entry_order ON true
  WHERE p.status='OPEN'
), identified AS (
  SELECT s.payload,c.snapshot AS control_snapshot,o.snapshot AS inventory_snapshot,
         encode(digest(convert_to(s.payload::text,'UTF8'),'sha256'),'hex') AS fp,
         clock_timestamp() AS cutoff
  FROM semantic_contract s CROSS JOIN pre_control c CROSS JOIN open_inventory o
)
INSERT INTO public.long_horizon_l3_contract_v1(
  contract_version,treatment_fingerprint,source_revision,start_cutoff,status,contract_payload)
SELECT 'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3',fp,
       current_setting('waltrade.migration_git_sha',true),cutoff,'ACTIVE',
       payload || jsonb_build_object(
         'source_revision',current_setting('waltrade.migration_git_sha',true),
         'start_cutoff',cutoff,
         'treatment_fingerprint',fp,
         'pre_activation_bot_control',control_snapshot,
         'pre_cutoff_open_positions',inventory_snapshot)
FROM identified
WHERE NOT EXISTS (
  SELECT 1 FROM public.long_horizon_l3_contract_v1
   WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3');

DO $contract$
BEGIN
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
         AND status='ACTIVE')<>1 THEN
    RAISE EXCEPTION 'L3_V3_ACTIVE_CONTRACT_NOT_EXACTLY_ONE';
  END IF;
END $contract$;

UPDATE public.bot_control
   SET enabled=true,
       reason='LONG_HORIZON_L3_V3_ACTIVE',
       regime_enabled=true,
       regime_mode='DRY_RUN',
       updated_at=clock_timestamp()
 WHERE strategy IN ('RSI','TREND','SUPERTREND','BBRANGE')
   AND (enabled IS DISTINCT FROM true
        OR reason IS DISTINCT FROM 'LONG_HORIZON_L3_V3_ACTIVE'
        OR regime_enabled IS DISTINCT FROM true
        OR regime_mode IS DISTINCT FROM 'DRY_RUN');

DO $slots$
BEGIN
  IF (SELECT count(*) FROM public.bot_control
       WHERE enabled AND regime_enabled AND regime_mode='DRY_RUN'
         AND strategy IN ('RSI','TREND','SUPERTREND','BBRANGE'))<>32 THEN
    RAISE EXCEPTION 'L3_V3_ACTIVE_DRY_RUN_SLOT_COVERAGE_NOT_32';
  END IF;
END $slots$;

INSERT INTO public.schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version)
SELECT '20260908_long_horizon_l3_direct_local_paper_v3',
       current_setting('waltrade.migration_checksum',true),'PAPER','local-paper',
       current_database(),current_user,'APPLIED',true,0,
       current_setting('waltrade.migration_git_sha',true),
       'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V3'
WHERE NOT EXISTS (
  SELECT 1 FROM public.schema_migration_ledger_v1
   WHERE migration_id='20260908_long_horizon_l3_direct_local_paper_v3'
     AND environment='PAPER' AND deployment_id='local-paper' AND success);
COMMIT;

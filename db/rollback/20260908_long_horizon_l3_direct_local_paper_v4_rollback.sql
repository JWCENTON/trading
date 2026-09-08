-- Evidence-preserving rollback for LONG HORIZON L3 DIRECT LOCAL PAPER V4.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';

DO $guard$
BEGIN
  IF current_database()<>'trading_paper'
     AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
    RAISE EXCEPTION 'L3_V4_ROLLBACK_LOCAL_PAPER_ONLY';
  END IF;
  IF current_setting('waltrade.target_deployment_id',true) IS DISTINCT FROM 'local-paper' THEN
    RAISE EXCEPTION 'L3_V4_ROLLBACK_LOCAL_PAPER_DEPLOYMENT_REQUIRED';
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4',0));
  IF (SELECT count(*) FROM public.long_horizon_l3_contract_v1
       WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
         AND status='ACTIVE')<>1 THEN
    RAISE EXCEPTION 'L3_V4_ACTIVE_CONTRACT_NOT_EXACTLY_ONE';
  END IF;
END $guard$;

WITH snapshot AS (
  SELECT x.*
  FROM public.long_horizon_l3_contract_v1 c
  CROSS JOIN LATERAL jsonb_to_recordset(
    c.contract_payload->'pre_activation_bot_control') AS x(
      symbol text,interval text,strategy text,enabled boolean,reason text,
      control_mode text,regime_enabled boolean,regime_mode text,
      updated_at timestamptz)
  WHERE c.contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
    AND c.status='ACTIVE'
)
UPDATE public.bot_control b
   SET enabled=s.enabled,reason=s.reason,control_mode=s.control_mode,
       regime_enabled=s.regime_enabled,regime_mode=s.regime_mode,
       updated_at=s.updated_at
  FROM snapshot s
 WHERE b.symbol=s.symbol AND b.interval=s.interval AND b.strategy=s.strategy
   AND (b.enabled,b.reason,b.control_mode,b.regime_enabled,b.regime_mode,b.updated_at)
       IS DISTINCT FROM
       (s.enabled,s.reason,s.control_mode,s.regime_enabled,s.regime_mode,s.updated_at);

UPDATE public.long_horizon_l3_contract_v1
   SET status='TERMINATED',
       contract_payload=contract_payload || jsonb_build_object(
         'termination_reason','V4_ROLLBACK','terminated_at',clock_timestamp())
 WHERE contract_version='LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4'
   AND status='ACTIVE';

INSERT INTO public.schema_migration_ledger_v1(
  migration_id,checksum_sha256,environment,deployment_id,database_name,
  applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version)
SELECT '20260908_long_horizon_l3_direct_local_paper_v4_rollback',
       current_setting('waltrade.migration_checksum',true),'PAPER','local-paper',
       current_database(),current_user,'APPLIED',true,0,
       current_setting('waltrade.migration_git_sha',true),
       'LONG_HORIZON_L3_DIRECT_LOCAL_PAPER_V4_ROLLBACK'
WHERE NOT EXISTS (
  SELECT 1 FROM public.schema_migration_ledger_v1
   WHERE migration_id='20260908_long_horizon_l3_direct_local_paper_v4_rollback'
     AND environment='PAPER' AND deployment_id='local-paper' AND success);
COMMIT;

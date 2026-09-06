-- WALTRADE REGIME SSOT + DIRECT PAPER ENFORCEMENT V1
-- Idempotent LOCAL PAPER-only policy identity and authority cutover.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $paper_only$
BEGIN
    IF current_database() <> 'trading_paper'
       AND current_setting('waltrade.test_database',true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'REGIME_SSOT_DIRECT_ENFORCEMENT_PAPER_ONLY';
    END IF;
    IF COALESCE(current_setting('waltrade.migration_git_sha',true),'')
       !~ '^[0-9a-f]{40}$' THEN
        RAISE EXCEPTION 'REGIME_SSOT_MIGRATION_GIT_SHA_REQUIRED';
    END IF;
    IF COALESCE(current_setting('waltrade.migration_checksum',true),'')
       !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'REGIME_SSOT_MIGRATION_CHECKSUM_REQUIRED';
    END IF;
END;
$paper_only$;

-- Historical rows elsewhere remain immutable.  Only the active lookup table
-- is canonicalized from the retired SUPER_TREND spelling.
DELETE FROM public.regime_policy
WHERE strategy='SUPER_TREND'
  AND EXISTS (
      SELECT 1 FROM public.regime_policy p
      WHERE p.strategy='SUPERTREND' AND p.regime=regime_policy.regime
  );
UPDATE public.regime_policy SET strategy='SUPERTREND'
WHERE strategy='SUPER_TREND';

INSERT INTO public.regime_policy(strategy,regime,allow_entry,note,updated_at)
VALUES
 ('BBRANGE','RANGE_HIGHVOL',true, 'REGIME_POLICY_20260906_V1',now()),
 ('BBRANGE','RANGE_LOWVOL', false,'REGIME_POLICY_20260906_V1',now()),
 ('BBRANGE','SHOCK',        false,'REGIME_POLICY_20260906_V1',now()),
 ('BBRANGE','TREND_DOWN',   false,'REGIME_POLICY_20260906_V1',now()),
 ('BBRANGE','TREND_UP',     false,'REGIME_POLICY_20260906_V1',now()),
 ('RSI','RANGE_HIGHVOL',    true, 'REGIME_POLICY_20260906_V1',now()),
 ('RSI','RANGE_LOWVOL',     true, 'REGIME_POLICY_20260906_V1',now()),
 ('RSI','SHOCK',            false,'REGIME_POLICY_20260906_V1',now()),
 ('RSI','TREND_DOWN',       false,'REGIME_POLICY_20260906_V1',now()),
 ('RSI','TREND_UP',         false,'REGIME_POLICY_20260906_V1',now()),
 ('SUPERTREND','RANGE_HIGHVOL',true, 'REGIME_POLICY_20260906_V1',now()),
 ('SUPERTREND','RANGE_LOWVOL', false,'REGIME_POLICY_20260906_V1',now()),
 ('SUPERTREND','SHOCK',        false,'REGIME_POLICY_20260906_V1',now()),
 ('SUPERTREND','TREND_DOWN',   true, 'REGIME_POLICY_20260906_V1',now()),
 ('SUPERTREND','TREND_UP',     true, 'REGIME_POLICY_20260906_V1',now()),
 ('TREND','RANGE_HIGHVOL', false,'REGIME_POLICY_20260906_V1',now()),
 ('TREND','RANGE_LOWVOL',  false,'REGIME_POLICY_20260906_V1',now()),
 ('TREND','SHOCK',         false,'REGIME_POLICY_20260906_V1',now()),
 ('TREND','TREND_DOWN',    true, 'REGIME_POLICY_20260906_V1',now()),
 ('TREND','TREND_UP',      true, 'REGIME_POLICY_20260906_V1',now())
ON CONFLICT(strategy,regime) DO UPDATE
SET allow_entry=EXCLUDED.allow_entry,
    note=EXCLUDED.note,
    updated_at=EXCLUDED.updated_at;

DO $coverage$
DECLARE v_rows integer; v_fingerprint text;
BEGIN
    SELECT count(*), encode(digest(string_agg(
        strategy||'|'||regime||'|'||CASE WHEN allow_entry THEN 'ALLOW' ELSE 'BLOCK' END,
        E'\n' ORDER BY strategy,regime)||E'\n', 'sha256'),'hex')
      INTO v_rows,v_fingerprint
      FROM public.regime_policy
     WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND')
       AND regime IN ('RANGE_HIGHVOL','RANGE_LOWVOL','SHOCK','TREND_DOWN','TREND_UP');
    IF v_rows<>20 OR v_fingerprint<>'585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e' THEN
        RAISE EXCEPTION 'REGIME_POLICY_20_20_FINGERPRINT_MISMATCH rows=% fingerprint=%',v_rows,v_fingerprint;
    END IF;
    IF EXISTS (SELECT 1 FROM public.regime_policy WHERE strategy='SUPER_TREND') THEN
        RAISE EXCEPTION 'ACTIVE_SUPER_TREND_POLICY_REMAINS';
    END IF;
END;
$coverage$;

-- Direct PAPER enforcement; strategy entry code still preserves all exits.
UPDATE public.bot_control
SET regime_enabled=true,
    regime_mode='ENFORCE',
    updated_at=clock_timestamp()
WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND');

DO $slot_coverage$
BEGIN
    IF (SELECT count(*) FROM public.bot_control
        WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND')
          AND regime_enabled AND regime_mode='ENFORCE') <> 32 THEN
        RAISE EXCEPTION 'REGIME_ENFORCE_SLOT_COVERAGE_NOT_32';
    END IF;
END;
$slot_coverage$;

INSERT INTO public.schema_migration_ledger_v1(
    migration_id,checksum_sha256,environment,deployment_id,database_name,
    applied_by,status,success,execution_duration_ms,git_sha,schema_baseline_version
)
SELECT
    '20260906_regime_ssot_direct_paper_enforcement_v1',
    current_setting('waltrade.migration_checksum'),
    'PAPER','local-paper',current_database(),current_user,'APPLIED',true,0,
    current_setting('waltrade.migration_git_sha'),'REGIME_SSOT_DIRECT_ENFORCEMENT_V1'
WHERE NOT EXISTS (
    SELECT 1 FROM public.schema_migration_ledger_v1
    WHERE migration_id='20260906_regime_ssot_direct_paper_enforcement_v1'
      AND environment='PAPER' AND deployment_id='local-paper' AND success
);

COMMIT;

-- WALTRADE REGIME SSOT + DIRECT VPS PAPER ENFORCEMENT V1
-- Explicit operator inputs:
--   SET waltrade.target_environment = 'PAPER';
--   SET waltrade.target_deployment_id = 'vps-paper';
--   SET waltrade.target_runtime_deployment_id = 'vps-paper';
--   SET waltrade.migration_git_sha = '<approved exact SHA>';
--   SET waltrade.migration_checksum = '<this file SHA256>';
--
-- Companion to the immutable LOCAL PAPER-only migration.  This artifact is
-- valid only for VPS PAPER and never records LOCAL provenance.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

DO $vps_paper_only$
DECLARE
    v_environment text := current_setting('waltrade.target_environment', true);
    v_deployment_id text := current_setting('waltrade.target_deployment_id', true);
    v_runtime_deployment_id text := current_setting(
        'waltrade.target_runtime_deployment_id', true
    );
    v_git_sha text := current_setting('waltrade.migration_git_sha', true);
    v_checksum text := current_setting('waltrade.migration_checksum', true);
    v_existing_count integer;
    v_existing_checksum text;
    v_rows integer;
    v_fingerprint text;
BEGIN
    -- Validate deployment identity before inspecting or mutating application
    -- state. LOCAL PAPER and every LIVE identity fail transactionally.
    IF v_environment IS DISTINCT FROM 'PAPER'
       OR v_deployment_id IS DISTINCT FROM 'vps-paper'
       OR v_runtime_deployment_id IS DISTINCT FROM 'vps-paper' THEN
        RAISE EXCEPTION
            'REGIME_SSOT_DIRECT_ENFORCEMENT_VPS_PAPER_TARGET_NOT_ALLOWED: environment=% deployment_id=% runtime_deployment_id=%',
            COALESCE(v_environment, '<missing>'),
            COALESCE(v_deployment_id, '<missing>'),
            COALESCE(v_runtime_deployment_id, '<missing>');
    END IF;
    IF COALESCE(v_git_sha, '') !~ '^[0-9a-f]{40}$' THEN
        RAISE EXCEPTION 'REGIME_SSOT_VPS_MIGRATION_GIT_SHA_REQUIRED';
    END IF;
    IF COALESCE(v_checksum, '') !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'REGIME_SSOT_VPS_MIGRATION_CHECKSUM_REQUIRED';
    END IF;
    IF to_regclass('public.regime_policy') IS NULL
       OR to_regclass('public.bot_control') IS NULL
       OR to_regclass('public.schema_migration_ledger_v1') IS NULL THEN
        RAISE EXCEPTION 'REGIME_SSOT_VPS_PAPER_PREREQUISITE_MISSING';
    END IF;

    SELECT count(*), min(checksum_sha256)
      INTO v_existing_count, v_existing_checksum
      FROM public.schema_migration_ledger_v1
     WHERE migration_id=
           '20260906_regime_ssot_direct_vps_paper_enforcement_v1'
       AND environment='PAPER'
       AND deployment_id='vps-paper'
       AND success;
    IF v_existing_count > 1 THEN
        RAISE EXCEPTION 'REGIME_SSOT_VPS_MIGRATION_DUPLICATE_LEDGER_ROWS';
    ELSIF v_existing_count = 1 THEN
        IF v_existing_checksum IS DISTINCT FROM v_checksum THEN
            RAISE EXCEPTION
                'REGIME_SSOT_VPS_MIGRATION_CHECKSUM_CONFLICT stored=% incoming=%',
                v_existing_checksum, v_checksum;
        END IF;
        RETURN;
    END IF;

    -- Historical evidence remains immutable. Only the active policy lookup is
    -- normalized from the retired SUPER_TREND spelling.
    DELETE FROM public.regime_policy legacy
     WHERE legacy.strategy='SUPER_TREND'
       AND EXISTS (
           SELECT 1 FROM public.regime_policy canonical
            WHERE canonical.strategy='SUPERTREND'
              AND canonical.regime=legacy.regime
       );
    UPDATE public.regime_policy
       SET strategy='SUPERTREND'
     WHERE strategy='SUPER_TREND';

    INSERT INTO public.regime_policy(
        strategy, regime, allow_entry, note, updated_at
    ) VALUES
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

    SELECT count(*), encode(digest(string_agg(
        strategy||'|'||regime||'|'||
        CASE WHEN allow_entry THEN 'ALLOW' ELSE 'BLOCK' END,
        E'\n' ORDER BY strategy,regime)||E'\n', 'sha256'),'hex')
      INTO v_rows, v_fingerprint
      FROM public.regime_policy
     WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND')
       AND regime IN (
           'RANGE_HIGHVOL','RANGE_LOWVOL','SHOCK','TREND_DOWN','TREND_UP'
       );
    IF v_rows <> 20 OR v_fingerprint <>
       '585ab57f906dff274e5df344475eb24de6f4977a3985535427edb7852093eb3e' THEN
        RAISE EXCEPTION
            'REGIME_POLICY_20_20_FINGERPRINT_MISMATCH rows=% fingerprint=%',
            v_rows, v_fingerprint;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.regime_policy WHERE strategy='SUPER_TREND'
    ) THEN
        RAISE EXCEPTION 'ACTIVE_SUPER_TREND_POLICY_REMAINS';
    END IF;

    UPDATE public.bot_control
       SET regime_enabled=true,
           regime_mode='ENFORCE',
           updated_at=clock_timestamp()
     WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND');
    IF (SELECT count(*) FROM public.bot_control
         WHERE strategy IN ('BBRANGE','RSI','SUPERTREND','TREND')
           AND regime_enabled AND regime_mode='ENFORCE') <> 32 THEN
        RAISE EXCEPTION 'REGIME_ENFORCE_SLOT_COVERAGE_NOT_32';
    END IF;

    INSERT INTO public.schema_migration_ledger_v1(
        migration_id, checksum_sha256, environment, deployment_id,
        database_name, applied_by, status, success, execution_duration_ms,
        git_sha, schema_baseline_version
    ) VALUES (
        '20260906_regime_ssot_direct_vps_paper_enforcement_v1',
        v_checksum, 'PAPER', 'vps-paper', current_database(), current_user,
        'APPLIED', true, 0, v_git_sha,
        'REGIME_SSOT_DIRECT_ENFORCEMENT_V1'
    );
END;
$vps_paper_only$;

COMMIT;

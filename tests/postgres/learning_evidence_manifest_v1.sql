\set ON_ERROR_STOP on

-- PostgreSQL 16 offline harness. The caller mounts the repository at /repo.
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
\i /repo/db/migrations/20260721_learning_evidence_manifest_v1.sql

-- Migration COMMIT must clear every transaction-local identity/capability.
DO $$ BEGIN
  IF nullif(current_setting('waltrade.learning_manifest_construction_token',true),'') IS NOT NULL
     OR nullif(current_setting('waltrade.learning_manifest_capture_api_token',true),'') IS NOT NULL
  THEN RAISE EXCEPTION 'construction capability leaked'; END IF;
  BEGIN PERFORM * FROM learning_evidence_runtime_identity_v1();
    RAISE EXCEPTION 'runtime identity leaked';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE 'INVALID_LEARNING_EVIDENCE_RUNTIME_IDENTITY%' THEN RAISE; END IF;
  END;
END $$;

-- Canonical V1.2 profit-factor matrix: exact NUMERIC semantics and no
-- NULL/zero equivalence.
DO $$
DECLARE
  c record;
  actual numeric;
BEGIN
  FOR c IN
    SELECT * FROM (VALUES
      ('ratio',2,2,4::numeric,-2::numeric,2::numeric),
      ('all-win',1,1,4::numeric,NULL::numeric,999::numeric),
      ('all-loss',1,1,NULL::numeric,-2::numeric,0::numeric),
      ('zero-zero',1,1,0::numeric,0::numeric,0::numeric),
      ('empty',0,0,NULL::numeric,NULL::numeric,NULL::numeric),
      ('all-null-pnl',2,0,NULL::numeric,NULL::numeric,NULL::numeric),
      ('breakeven-only',2,2,0::numeric,0::numeric,0::numeric),
      ('exact-scale',3,3,1.000000000001::numeric,-3.000000000003::numeric,
       0.333333333333::numeric)
    ) v(label,decisions,coverage,gross_profit,gross_loss,expected)
  LOOP
    actual := learning_canonical_profit_factor_v1(
      c.decisions,c.coverage,c.gross_profit,c.gross_loss);
    IF actual IS DISTINCT FROM c.expected THEN
      RAISE EXCEPTION 'canonical PF case % expected=% actual=%',
        c.label,c.expected,actual;
    END IF;
  END LOOP;
  IF 0::numeric IS NOT DISTINCT FROM NULL::numeric THEN
    RAISE EXCEPTION 'NULL and zero collapsed';
  END IF;
END $$;

-- A session-local source fixture shadows only the analytical view in this
-- disposable database. Production relations and trigger functions are real.
CREATE TEMP TABLE v_decision_intelligence_v1 AS
SELECT * FROM (VALUES
 ('trading_live'::text,'BTCUSDC'::text,'1m'::text,'RSI'::text,
  'natural-001'::text,'CLOSED'::text,true,(-2.0)::numeric,
  '2026-07-09 10:00+00'::timestamptz,'2026-07-09 10:00+00'::timestamptz),
 ('trading_live','BTCUSDC','1m','RSI','natural-002','CLOSED',true,1.0,
  '2026-07-09 11:00+00','2026-07-09 11:00+00'),
 ('trading_live','BTCUSDC','1m','RSI','natural-003','CLOSED',true,0.0,
  '2026-07-09 12:00+00','2026-07-09 12:00+00'),
 ('trading_live','BTCUSDC','5m','TREND','trend-loss-001','CLOSED',true,-0.02827693,
  '2026-07-09 12:30+00','2026-07-09 12:30+00'),
 -- Backdated event time but unavailable until after the frozen run boundary.
 ('trading_live','BTCUSDC','1m','RSI','post-cutoff','CLOSED',true,99.0,
  '2026-07-09 13:00+00','2099-01-01 00:00+00')
) v(environment,symbol,interval,strategy,decision_key,decision_lifecycle_status,
    has_pnl,net_pnl_usdc,refreshed_at,created_at);

CREATE TEMP TABLE decision_registry_v1 AS
SELECT * FROM (VALUES
 ('00000000-0000-0000-0000-000000000001'::uuid,'natural-001','LOCAL','trading_live',
  '2026-07-09 10:00+00'::timestamptz,'2026-07-09 10:00+00'::timestamptz),
 ('00000000-0000-0000-0000-000000000002'::uuid,'natural-002','LOCAL','trading_live',
  '2026-07-09 11:00+00','2026-07-09 11:00+00'),
 ('00000000-0000-0000-0000-000000000003'::uuid,'natural-003','LOCAL','trading_live',
  '2026-07-09 12:00+00','2026-07-09 12:00+00'),
 ('00000000-0000-0000-0000-000000000005'::uuid,'trend-loss-001','LOCAL','trading_live',
  '2026-07-09 12:30+00','2026-07-09 12:30+00')
) v(decision_id,legacy_decision_key,deployment_id,environment,ingested_at,created_at);

CREATE TEMP TABLE learning_feature_warehouse_v1 AS
SELECT * FROM (VALUES
 (1,'natural-001',101,'2026-07-08 10:11+00'::timestamptz,'2026-07-08 10:24+00'::timestamptz,
  0.1::numeric,-1.9::numeric,'TREND_DOWN','2026-07-09 10:00+00'::timestamptz),
 (2,'natural-002',102,'2026-07-08 11:11+00','2026-07-08 11:24+00',
  0.1,1.1,'TREND_UP','2026-07-09 11:00+00'),
 (3,'natural-003',103,'2026-07-08 12:11+00','2026-07-08 12:24+00',
  0.0,0.0,'RANGE','2026-07-09 12:00+00'),
 (5,'trend-loss-001',105,'2026-07-08 12:31+00','2026-07-08 12:44+00',
  NULL,-0.02827693,NULL,'2026-07-09 12:30+00')
) v(id,decision_key,position_id,entry_time,exit_time,fees_usdc,gross_pnl_usdc,
    market_regime,created_at);

CREATE TEMP TABLE decision_outcomes_v1 AS
SELECT * FROM (VALUES
 ('00000000-0000-0000-0000-000000000001'::uuid,'ACTUAL_TRADE',
  '2026-07-12 13:07+00'::timestamptz,1.2::numeric,-0.8::numeric,'2026-07-12 13:08+00'::timestamptz),
 ('00000000-0000-0000-0000-000000000002'::uuid,'ACTUAL_TRADE',
  '2026-07-12 13:08+00',1.0,-0.5,'2026-07-12 13:09+00'),
 ('00000000-0000-0000-0000-000000000003'::uuid,'ACTUAL_TRADE',
  '2026-07-12 13:09+00',0.5,-0.4,'2026-07-12 13:10+00'),
 ('00000000-0000-0000-0000-000000000005'::uuid,'ACTUAL_TRADE',
  '2026-07-12 13:10+00',0.18548295103843888,-0.06141095177599535,
  '2026-07-12 13:11+00')
) v(decision_id,outcome_type,calculated_at,mfe_pct,mae_pct,created_at);

INSERT INTO learning_slot_statistics_v1
 (environment,symbol,interval,strategy,window_days,sample_from,sample_to,
  decisions,wins,losses,breakeven,gross_profit_usdc,gross_loss_usdc,
  net_pnl_usdc,learning_status,learning_reason)
VALUES ('trading_live','BTCUSDC','1m','RSI',30,
        '2026-07-05 16:12+00','2026-07-10 14:20+00',
        3,1,1,1,1.0,-2.0,-1.0,'NEGATIVE_EDGE','harness');
INSERT INTO learning_slot_statistics_v1
 (environment,symbol,interval,strategy,window_days,sample_from,sample_to,
  decisions,wins,losses,breakeven,gross_profit_usdc,gross_loss_usdc,
  net_pnl_usdc,profit_factor,expectancy_usdc,win_rate_pct,
  learning_status,learning_reason)
VALUES ('trading_live','BTCUSDC','5m','TREND',30,
        '2026-07-09 12:30+00','2026-07-09 12:30+00',
        1,0,1,0,NULL,-0.02827693,-0.02827693,0,-0.02827693,0,
        'INSUFFICIENT_SAMPLE','production PF=0 regression fixture');
UPDATE learning_slot_statistics_v1
   SET profit_factor=0.5,expectancy_usdc=(-1.0/3.0),
       win_rate_pct=33.3333
 WHERE environment='trading_live' AND symbol='BTCUSDC'
   AND interval='1m' AND strategy='RSI';

-- The V1.2 wrapper owns the required PL/pgSQL exception/subtransaction. Its V1.1
-- dependency is replaced only in this disposable harness so it emits one stable
-- observation for the source fixture; V1.2 and both V1.3/V1 manifest triggers
-- remain the production definitions.
CREATE OR REPLACE FUNCTION refresh_learning_feedback_engine_v1_1(
 p_window_days integer DEFAULT 30,p_min_observe_sample integer DEFAULT 10,
 p_min_action_sample integer DEFAULT 30)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE v_run bigint;
BEGIN
 SELECT id INTO STRICT v_run FROM learning_feedback_refresh_runs_v1
  WHERE status='RUNNING' ORDER BY id DESC LIMIT 1;
 INSERT INTO learning_proposal_observations_v1
  (refresh_run_id,environment,proposal_key,symbol,interval,strategy,window_days,
   proposal_type,proposal_action,confidence,priority,evidence_decisions,
   source_validation_stage,source_validation_status,reason)
 VALUES (v_run,'trading_live','natural-trigger-chain','BTCUSDC','1m','RSI',30,
   'RISK_CONTROL','BLOCK_CANDIDATE',0.9,'HIGH',3,'VALIDATION','STABLE','harness');
 INSERT INTO learning_proposal_observations_v1
  (refresh_run_id,environment,proposal_key,symbol,interval,strategy,window_days,
   proposal_type,proposal_action,confidence,priority,evidence_decisions,
   source_validation_stage,source_validation_status,reason)
 VALUES (v_run,'trading_live','pf-zero-regression','BTCUSDC','5m','TREND',30,
   'RISK_CONTROL','OBSERVE_ONLY',0.5,'NORMAL',1,'VALIDATION','OBSERVED',
   'production PF=0 regression fixture');
 RETURN jsonb_build_object('status','ok','observations',2);
END $$;

BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
DO $$
DECLARE v_result jsonb;
BEGIN
  v_result := refresh_learning_feedback_engine_v1_2_if_due(12,30,1,1,true,'HARNESS');
  IF v_result->>'status'<>'ok' THEN RAISE EXCEPTION 'V1.2 did not succeed: %',v_result; END IF;
END $$;
COMMIT;

DO $$ DECLARE m record; BEGIN
  SELECT * INTO STRICT m FROM learning_evidence_manifests_v1
   WHERE deployment_id='local-live' AND symbol='BTCUSDC'
     AND interval='1m' AND strategy='RSI';
  IF m.manifest_status<>'COMPLETE' OR m.evidence_decision_count<>3 THEN
    RAISE EXCEPTION 'natural manifest not COMPLETE/3'; END IF;
  IF m.evidence_window_end <> '2026-07-10 14:20+00'::timestamptz
     OR m.evidence_cutoff_at <= m.evidence_window_end
     OR m.source_snapshot_at <> m.evidence_cutoff_at THEN
    RAISE EXCEPTION 'event window/as-of cutoff contract failed'; END IF;
  IF (SELECT count(*) FROM learning_evidence_membership_v1
       WHERE evidence_manifest_id=m.evidence_manifest_id)<>3 THEN
    RAISE EXCEPTION 'natural child parity failed'; END IF;
  IF (SELECT count(*) FROM learning_evidence_aggregates_v1
       WHERE evidence_manifest_id=m.evidence_manifest_id)<>1 THEN
    RAISE EXCEPTION 'natural aggregate parity failed'; END IF;
  IF NOT EXISTS (SELECT 1 FROM learning_evidence_membership_v1
      WHERE evidence_manifest_id=m.evidence_manifest_id
        AND outcome_timestamp='2026-07-12 13:07+00'::timestamptz) THEN
    RAISE EXCEPTION 'outcome after window end but before cutoff was lost'; END IF;
  IF EXISTS (SELECT 1 FROM learning_evidence_membership_v1
      WHERE evidence_manifest_id=m.evidence_manifest_id
        AND decision_key='post-cutoff') THEN
    RAISE EXCEPTION 'post-cutoff availability entered frozen membership'; END IF;
  IF EXISTS (SELECT 1 FROM learning_evidence_manifests_v1 WHERE manifest_status='BUILDING')
  THEN RAISE EXCEPTION 'BUILDING residue'; END IF;
END $$;

-- Exact reproduction of the production all-loss slot: source PF and frozen PF
-- are both zero; every other shared aggregate is exact.
DO $$ DECLARE m record; a record; BEGIN
  SELECT * INTO STRICT m FROM learning_evidence_manifests_v1
   WHERE deployment_id='local-live' AND symbol='BTCUSDC'
     AND interval='5m' AND strategy='TREND';
  SELECT * INTO STRICT a FROM learning_evidence_aggregates_v1
   WHERE evidence_manifest_id=m.evidence_manifest_id;
  IF m.manifest_status<>'COMPLETE' OR m.evidence_decision_count<>1
     OR a.decisions<>1 OR a.wins<>0 OR a.losses<>1 OR a.breakeven<>0
     OR a.gross_profit_usdc IS NOT NULL
     OR a.gross_loss_usdc IS DISTINCT FROM (-0.02827693)::numeric
     OR a.net_pnl_usdc IS DISTINCT FROM (-0.02827693)::numeric
     OR a.expectancy_usdc IS DISTINCT FROM (-0.02827693)::numeric
     OR a.profit_factor IS DISTINCT FROM 0::numeric
     OR a.win_rate_pct IS DISTINCT FROM 0::numeric
  THEN RAISE EXCEPTION 'BTCUSDC/5m/TREND aggregate parity failed: %',row_to_json(a);
  END IF;
  IF (SELECT profit_factor FROM learning_slot_statistics_v1
      WHERE environment='trading_live' AND symbol='BTCUSDC'
        AND interval='5m' AND strategy='TREND' AND window_days=30)
       IS DISTINCT FROM a.profit_factor
  THEN RAISE EXCEPTION 'BTCUSDC/5m/TREND source/manifest PF mismatch'; END IF;
END $$;

-- Exact retry is a no-op and preserves immutable history.
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
DO $$ DECLARE r bigint; before_count bigint; BEGIN
 SELECT feedback_run_id INTO r FROM learning_evidence_manifests_v1 WHERE deployment_id='local-live';
 SELECT count(*) INTO before_count FROM learning_evidence_manifests_v1;
 PERFORM capture_learning_evidence_manifests_v1(r);
 IF (SELECT count(*) FROM learning_evidence_manifests_v1)<>before_count
 THEN RAISE EXCEPTION 'retry was not a no-op'; END IF;
END $$;
COMMIT;

-- A later source mutation cannot rewrite COMPLETE evidence. A retry sees the
-- changed ordered source set and fails explicitly as an idempotency conflict.
INSERT INTO v_decision_intelligence_v1 VALUES
 ('trading_live','BTCUSDC','1m','RSI','late-visible','CLOSED',true,2.0,
  '2026-07-09 13:30+00','2026-07-09 13:30+00');
INSERT INTO decision_registry_v1 VALUES
 ('00000000-0000-0000-0000-000000000004','late-visible','LOCAL','trading_live',
  '2026-07-09 13:30+00','2026-07-09 13:30+00');
INSERT INTO learning_feature_warehouse_v1 VALUES
 (4,'late-visible',104,'2026-07-08 13:11+00','2026-07-08 13:24+00',
  0.1,2.1,'TREND_UP','2026-07-09 13:30+00');
INSERT INTO decision_outcomes_v1 VALUES
 ('00000000-0000-0000-0000-000000000004','ACTUAL_TRADE',
  '2026-07-12 13:30+00',1.0,-0.2,'2026-07-12 13:31+00');
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
DO $$ DECLARE r bigint; BEGIN
 SELECT feedback_run_id INTO r FROM learning_evidence_manifests_v1
  WHERE deployment_id='local-live';
 BEGIN
   PERFORM capture_learning_evidence_manifests_v1(r);
   RAISE EXCEPTION 'changed source retry accepted';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM NOT LIKE 'LEARNING_EVIDENCE_IDEMPOTENCY_CONFLICT%' THEN RAISE; END IF;
 END;
END $$;
ROLLBACK;

-- Caller-selected/spoofed token is insufficient without the private capture API
-- token; children without a live matching capability also fail closed.
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
DO $$ DECLARE r bigint; t uuid:=gen_random_uuid(); BEGIN
 INSERT INTO learning_feedback_refresh_runs_v1
  (environment,engine_version,trigger_source,status,window_days,min_observe_sample,
   min_action_sample,interval_hours,result)
 VALUES('trading_live','H','H','SKIPPED_DISABLED',30,1,1,12,'{}') RETURNING id INTO r;
 PERFORM set_config('waltrade.learning_manifest_construction_token',t::text,true);
 BEGIN
   INSERT INTO learning_evidence_manifests_v1
    (evidence_manifest_id,deployment_id,deployment_instance_id,environment,
     feedback_run_id,symbol,interval,strategy,window_days,proposal_action,
     validation_status,manifest_status,construction_token,
     exact_membership_available,evidence_cutoff_at,evidence_decision_count,
     manifest_hash,aggregate_hash,engine_version,validation_version)
   VALUES(gen_random_uuid(),'local-live','local','live',r,'X','1m','RSI',30,
    'OBSERVE','OBSERVED','BUILDING',t,true,now(),0,
    encode(digest('','sha256'),'hex'),encode(digest('{}','sha256'),'hex'),'H','H');
   RAISE EXCEPTION 'spoofed token accepted';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM NOT LIKE 'LEARNING_MANIFEST_HEADER_CAPABILITY_REQUIRED%' THEN RAISE; END IF;
 END;
 PERFORM set_config('waltrade.learning_manifest_construction_token','',true);
 BEGIN
   INSERT INTO learning_evidence_membership_v1
    (evidence_manifest_id,ordinal,decision_key,source_table,source_version,
     pnl_available,fees_available,mfe_available,mae_available,regime_available,row_fingerprint)
   SELECT evidence_manifest_id,99,'forged','h','h',false,false,false,false,false,repeat('a',64)
   FROM learning_evidence_manifests_v1 LIMIT 1;
   RAISE EXCEPTION 'child without token accepted';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM NOT LIKE 'LEARNING_MANIFEST_CONSTRUCTION_TOKEN_REQUIRED%' THEN RAISE; END IF;
 END;
END $$;
ROLLBACK;

-- COMPLETE data is append-only; failed mutation leaves no residue.
DO $$ DECLARE m uuid; BEGIN
 SELECT evidence_manifest_id INTO m FROM learning_evidence_manifests_v1 WHERE deployment_id='local-live';
 BEGIN UPDATE learning_evidence_manifests_v1 SET manifest_status='BUILDING'
       WHERE evidence_manifest_id=m;
   RAISE EXCEPTION 'COMPLETE reopened';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM NOT LIKE 'learning evidence manifest is immutable%' THEN RAISE; END IF;
 END;
 BEGIN DELETE FROM learning_evidence_membership_v1 WHERE evidence_manifest_id=m;
   RAISE EXCEPTION 'child deleted';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM NOT LIKE 'learning evidence manifest is immutable%' THEN RAISE; END IF;
 END;
END $$;

-- Publisher uses a new transaction and fresh context, linked to COMPLETE only.
BEGIN;
SET LOCAL waltrade.deployment_instance_id='local';
SET LOCAL waltrade.environment='live';
INSERT INTO learning_shadow_confidence_proposals_v1
 (proposal_key,environment,symbol,interval,strategy,window_days,
  source_refresh_run_id,source_proposal_action,proposed_delta,
  calibration_confidence,status,reason,evidence)
SELECT 'publisher-'||interval||'-'||lower(strategy),'trading_live',
 symbol,interval,strategy,
 window_days,feedback_run_id,'REDUCE_CONFIDENCE',-0.01,0.9,'ACTIVE','harness','{}'
FROM learning_evidence_manifests_v1 WHERE deployment_id='local-live';
COMMIT;

DO $$ BEGIN
 IF nullif(current_setting('waltrade.learning_manifest_construction_token',true),'') IS NOT NULL
 THEN RAISE EXCEPTION 'token leaked after commit'; END IF;
 IF (SELECT count(*) FROM learning_shadow_confidence_proposals_v1
      WHERE proposal_key LIKE 'publisher-%')<>2
 THEN RAISE EXCEPTION 'publisher linkage missing'; END IF;
END $$;

SELECT 'LEARNING_EVIDENCE_MANIFEST_V1_POSTGRES16_NATURAL_TRIGGER_CHAIN_PASS' AS result;

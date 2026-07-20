\set ON_ERROR_STOP on
\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1.sql
\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1_1_counter_semantics.sql

INSERT INTO orc_apply_runs_v1 (
  run_id,deployment_id,environment,deployment_identity,writer_service,
  writer_instance,started_at,completed_at,apply_mode,integration_version,
  source_view,source_candidate_count,candidate_universe_count,
  slot_decision_count,source_excluded_count,desired_on_count,
  previous_live_on_count,resulting_live_on_count,touched_on_count,
  touched_off_count,unchanged_on_count,unchanged_off_count,picks_hash,
  transaction_outcome,error_classification,duration_ms,schema_version
) VALUES (
  '11111111-1111-4111-8111-111111111111','local-paper','trading_paper',
  'local-paper','automation-runner','legacy',now(),now(),'test','test','view',
  0,0,0,0,0,0,0,0,0,0,0,'','ROLLED_BACK','LEGACY_TEST',0,
  'ORC_APPLY_LEDGER_V1_1'
);

\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1_2_observe_only.sql
\i /repo/db/migrations/20260720_orc_immutable_apply_ledger_v1_2_observe_only.sql

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM orc_apply_runs_v1
     WHERE run_id='11111111-1111-4111-8111-111111111111'
       AND schema_version='ORC_APPLY_LEDGER_V1_1'
       AND execution_mode='LEGACY_APPLY'
  ) THEN
    RAISE EXCEPTION 'V1.1 history was not preserved';
  END IF;
END $$;

INSERT INTO orc_apply_runs_v1 (
  run_id,deployment_id,environment,deployment_identity,writer_service,
  writer_instance,started_at,completed_at,apply_mode,integration_version,
  source_view,source_candidate_count,candidate_universe_count,
  slot_decision_count,source_excluded_count,desired_on_count,
  previous_live_on_count,resulting_live_on_count,touched_on_count,
  touched_off_count,unchanged_on_count,unchanged_off_count,picks_hash,
  transaction_outcome,error_classification,duration_ms,schema_version,
  execution_mode
) VALUES (
  '22222222-2222-4222-8222-222222222222','local-paper','trading_paper',
  'local-paper','automation-runner','observe',now(),now(),'test','test','view',
  1,1,1,0,1,0,0,0,0,0,0,'','COMMITTED',NULL,1,
  'ORC_APPLY_LEDGER_V1_2','OBSERVE_ONLY'
);

INSERT INTO orc_apply_slot_decisions_v1 (
  run_id,deployment_id,environment,symbol,interval,strategy,slot_key,
  previous_live_orders_enabled,want_on,resulting_live_orders_enabled,
  transition_type,touched,control_mode,control_source,decision_reason,
  included_in_pick_set,slot_snapshot_hash,snapshot_json,writer_service,
  writer_instance,schema_version,decision_effect
) VALUES (
  '22222222-2222-4222-8222-222222222222','local-paper','trading_paper',
  'BTCUSDC','1m','RSI','BTCUSDC|1m|RSI',false,true,false,
  'WOULD_ENABLE',false,'AUTO','ORC','observe',true,
  repeat('a',64),'{}'::jsonb,'automation-runner','observe',
  'ORC_APPLY_LEDGER_V1_2','WOULD_ENABLE'
);

DO $$
BEGIN
  BEGIN
    INSERT INTO orc_apply_runs_v1 (
      run_id,deployment_id,environment,deployment_identity,writer_service,
      writer_instance,started_at,completed_at,apply_mode,integration_version,
      source_view,source_candidate_count,candidate_universe_count,
      slot_decision_count,source_excluded_count,desired_on_count,
      previous_live_on_count,resulting_live_on_count,touched_on_count,
      touched_off_count,unchanged_on_count,unchanged_off_count,picks_hash,
      transaction_outcome,error_classification,duration_ms,schema_version,
      execution_mode
    ) VALUES (
      gen_random_uuid(),'local-paper','trading_paper','local-paper','test','test',
      now(),now(),'test','test','view',1,1,1,0,1,0,1,1,0,0,0,'',
      'COMMITTED',NULL,1,'ORC_APPLY_LEDGER_V1_2','OBSERVE_ONLY'
    );
    RAISE EXCEPTION 'OBSERVE_ONLY touched count accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    INSERT INTO orc_apply_runs_v1 (
      run_id,deployment_id,environment,deployment_identity,writer_service,
      writer_instance,started_at,completed_at,apply_mode,integration_version,
      source_view,source_candidate_count,candidate_universe_count,
      slot_decision_count,source_excluded_count,desired_on_count,
      previous_live_on_count,resulting_live_on_count,touched_on_count,
      touched_off_count,unchanged_on_count,unchanged_off_count,picks_hash,
      transaction_outcome,error_classification,duration_ms,schema_version,
      execution_mode
    ) VALUES (
      gen_random_uuid(),'local-paper','trading_paper','local-paper','test','test',
      now(),now(),'test','test','view',1,1,1,0,1,0,1,0,0,0,0,'',
      'COMMITTED',NULL,1,'ORC_APPLY_LEDGER_V1_2','OBSERVE_ONLY'
    );
    RAISE EXCEPTION 'OBSERVE_ONLY resulting mismatch accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    INSERT INTO orc_apply_runs_v1 (
      run_id,deployment_id,environment,deployment_identity,writer_service,
      writer_instance,started_at,completed_at,apply_mode,integration_version,
      source_view,source_candidate_count,candidate_universe_count,
      slot_decision_count,source_excluded_count,desired_on_count,
      previous_live_on_count,resulting_live_on_count,touched_on_count,
      touched_off_count,unchanged_on_count,unchanged_off_count,picks_hash,
      transaction_outcome,error_classification,duration_ms,schema_version,
      execution_mode
    ) VALUES (
      gen_random_uuid(),'local-paper','trading_paper','local-paper','test','test',
      now(),now(),'test','test','view',0,1,1,-1,0,0,0,0,0,0,0,'',
      'COMMITTED',NULL,1,'ORC_APPLY_LEDGER_V1_2','OBSERVE_ONLY'
    );
    RAISE EXCEPTION 'negative excluded count accepted';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
END $$;

DO $$
BEGIN
  BEGIN
    UPDATE orc_apply_runs_v1 SET duration_ms=duration_ms
     WHERE run_id='22222222-2222-4222-8222-222222222222';
    RAISE EXCEPTION 'immutable header update accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'ORC apply ledger is immutable and append-only' THEN RAISE; END IF;
  END;
  BEGIN
    DELETE FROM orc_apply_slot_decisions_v1
     WHERE run_id='22222222-2222-4222-8222-222222222222';
    RAISE EXCEPTION 'immutable child delete accepted';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'ORC apply ledger is immutable and append-only' THEN RAISE; END IF;
  END;
END $$;

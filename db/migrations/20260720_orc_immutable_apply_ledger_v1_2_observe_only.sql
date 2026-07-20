BEGIN;

ALTER TABLE orc_apply_runs_v1
  ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'LEGACY_APPLY';
ALTER TABLE orc_apply_slot_decisions_v1
  ADD COLUMN IF NOT EXISTS decision_effect TEXT NOT NULL DEFAULT 'LEGACY';

ALTER TABLE orc_apply_runs_v1
  ALTER COLUMN execution_mode DROP DEFAULT,
  ALTER COLUMN schema_version SET DEFAULT 'ORC_APPLY_LEDGER_V1_2';
ALTER TABLE orc_apply_slot_decisions_v1
  ALTER COLUMN decision_effect DROP DEFAULT,
  ALTER COLUMN schema_version SET DEFAULT 'ORC_APPLY_LEDGER_V1_2';

ALTER TABLE orc_apply_slot_decisions_v1
  DROP CONSTRAINT IF EXISTS orc_apply_slot_decisions_v1_transition_type_check,
  DROP CONSTRAINT IF EXISTS orc_apply_slot_decisions_v1_check1,
  DROP CONSTRAINT IF EXISTS orc_apply_slot_decisions_v1_check2;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_runs_v1'::regclass AND conname='orc_apply_runs_counter_semantics_v1_2') THEN
    ALTER TABLE orc_apply_runs_v1 ADD CONSTRAINT orc_apply_runs_counter_semantics_v1_2
      CHECK (schema_version<>'ORC_APPLY_LEDGER_V1_2' OR
             (source_candidate_count>=candidate_universe_count AND
              source_excluded_count=source_candidate_count-candidate_universe_count AND
              (transaction_outcome<>'COMMITTED' OR candidate_universe_count=slot_decision_count)));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_runs_v1'::regclass AND conname='orc_apply_runs_execution_mode_v1_2') THEN
    ALTER TABLE orc_apply_runs_v1 ADD CONSTRAINT orc_apply_runs_execution_mode_v1_2
      CHECK ((schema_version='ORC_APPLY_LEDGER_V1_2' AND execution_mode IN ('APPLY','OBSERVE_ONLY')) OR
             (schema_version<>'ORC_APPLY_LEDGER_V1_2' AND execution_mode='LEGACY_APPLY'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_runs_v1'::regclass AND conname='orc_apply_runs_observe_only_effect_v1_2') THEN
    ALTER TABLE orc_apply_runs_v1 ADD CONSTRAINT orc_apply_runs_observe_only_effect_v1_2
      CHECK (schema_version<>'ORC_APPLY_LEDGER_V1_2' OR transaction_outcome<>'COMMITTED' OR execution_mode<>'OBSERVE_ONLY' OR
             (resulting_live_on_count=previous_live_on_count AND touched_on_count=0 AND touched_off_count=0));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_slot_decisions_v1'::regclass AND conname='orc_apply_slots_decision_effect_v1_2') THEN
    ALTER TABLE orc_apply_slot_decisions_v1 ADD CONSTRAINT orc_apply_slots_decision_effect_v1_2
      CHECK ((schema_version='ORC_APPLY_LEDGER_V1_2' AND decision_effect IN
              ('APPLIED_ENABLE','APPLIED_DISABLE','RETAINED_ON','RETAINED_OFF',
               'WOULD_ENABLE','WOULD_DISABLE','WOULD_RETAIN_ON','WOULD_RETAIN_OFF')) OR
             (schema_version<>'ORC_APPLY_LEDGER_V1_2' AND decision_effect='LEGACY'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_slot_decisions_v1'::regclass AND conname='orc_apply_slots_transition_type_v1_2') THEN
    ALTER TABLE orc_apply_slot_decisions_v1 ADD CONSTRAINT orc_apply_slots_transition_type_v1_2
      CHECK (transition_type IN ('ENABLED','DISABLED','RETAINED_ON','RETAINED_OFF',
                                 'WOULD_ENABLE','WOULD_DISABLE','WOULD_RETAIN_ON','WOULD_RETAIN_OFF'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_slot_decisions_v1'::regclass AND conname='orc_apply_slots_effect_consistency_v1_2') THEN
    ALTER TABLE orc_apply_slot_decisions_v1 ADD CONSTRAINT orc_apply_slots_effect_consistency_v1_2
      CHECK (schema_version<>'ORC_APPLY_LEDGER_V1_2' OR decision_effect=transition_type OR
             (decision_effect='APPLIED_ENABLE' AND transition_type='ENABLED') OR
             (decision_effect='APPLIED_DISABLE' AND transition_type='DISABLED'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='orc_apply_slot_decisions_v1'::regclass AND conname='orc_apply_slots_resulting_state_v1_2') THEN
    ALTER TABLE orc_apply_slot_decisions_v1 ADD CONSTRAINT orc_apply_slots_resulting_state_v1_2
      CHECK ((decision_effect LIKE 'WOULD_%' AND resulting_live_orders_enabled=previous_live_orders_enabled AND NOT touched) OR
             (decision_effect NOT LIKE 'WOULD_%' AND resulting_live_orders_enabled=want_on));
  END IF;
END $$;

COMMIT;

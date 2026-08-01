BEGIN;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM live_entry_position_projection_fills_v1)
     OR EXISTS (SELECT 1 FROM live_entry_position_projections_v1)
     OR EXISTS (SELECT 1 FROM live_entry_position_projection_diagnostics_v1)
     OR EXISTS (SELECT 1 FROM positions WHERE entry_intent_id IS NOT NULL)
     OR EXISTS (
       SELECT 1 FROM position_lifecycle_events_c2_2
       WHERE mutation_kind='POSITION_OPENED'
     ) THEN
    RAISE EXCEPTION 'LEI1D_ROLLBACK_BLOCKED_PROJECTION_EVIDENCE_EXISTS';
  END IF;
END $$;

DROP INDEX IF EXISTS ux_position_opened_once_lei1d;
DROP TABLE IF EXISTS live_entry_position_projection_fills_v1;
DROP TABLE IF EXISTS live_entry_position_projection_diagnostics_v1;
DROP TABLE IF EXISTS live_entry_position_projections_v1;
DROP INDEX IF EXISTS ux_positions_entry_intent_lei1d;
ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_entry_intent_id_fkey;
ALTER TABLE positions DROP COLUMN IF EXISTS entry_intent_id;

ALTER TABLE position_lifecycle_events_c2_2
  DROP CONSTRAINT position_lifecycle_events_c2_2_mutation_kind_check;
ALTER TABLE position_lifecycle_events_c2_2
  ADD CONSTRAINT position_lifecycle_events_c2_2_mutation_kind_check CHECK (
    mutation_kind IN (
      'POSITION_REDUCED','POSITION_CLOSED','POSITION_CLOSED_TERMINAL_DUST'
    )
  );
COMMIT;

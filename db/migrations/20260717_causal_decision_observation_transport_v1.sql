BEGIN;

CREATE TABLE IF NOT EXISTS causal_decision_observation_outbox_v1 (
 event_id UUID PRIMARY KEY,
 deployment_id TEXT NOT NULL CHECK (deployment_id IN ('local-live','local-paper','vps-live','vps-paper')),
 decision_key TEXT NOT NULL,
 event_schema_version TEXT NOT NULL,
 event_payload JSONB NOT NULL,
 event_payload_hash TEXT NOT NULL,
 semantic_digest TEXT NOT NULL,
 source_service TEXT NOT NULL,
 source_instance TEXT,
 decision_created_at TIMESTAMPTZ NOT NULL,
 inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 processing_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (processing_status IN
  ('PENDING','PROCESSING','RETRY','PROCESSED','DEAD_LETTER','IDEMPOTENCY_CONFLICT')),
 attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
 next_attempt_at TIMESTAMPTZ,
 claimed_at TIMESTAMPTZ,
 claimed_by TEXT,
 processed_at TIMESTAMPTZ,
 last_error_code TEXT,
 last_error_at TIMESTAMPTZ,
 UNIQUE(deployment_id,decision_key)
);

CREATE INDEX IF NOT EXISTS ix_causal_observation_outbox_claim_v1
 ON causal_decision_observation_outbox_v1(deployment_id,processing_status,next_attempt_at,decision_created_at);

CREATE OR REPLACE FUNCTION protect_causal_observation_outbox_event_v1() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'causal observation outbox event cannot be deleted'; END IF;
 IF ROW(NEW.event_id,NEW.deployment_id,NEW.decision_key,NEW.event_schema_version,NEW.event_payload,
        NEW.event_payload_hash,NEW.semantic_digest,NEW.source_service,NEW.source_instance,
        NEW.decision_created_at,NEW.inserted_at)
    IS DISTINCT FROM
    ROW(OLD.event_id,OLD.deployment_id,OLD.decision_key,OLD.event_schema_version,OLD.event_payload,
        OLD.event_payload_hash,OLD.semantic_digest,OLD.source_service,OLD.source_instance,
        OLD.decision_created_at,OLD.inserted_at)
 THEN RAISE EXCEPTION 'causal observation outbox event payload is immutable'; END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS causal_observation_outbox_event_immutable_v1 ON causal_decision_observation_outbox_v1;
CREATE TRIGGER causal_observation_outbox_event_immutable_v1 BEFORE UPDATE OR DELETE ON causal_decision_observation_outbox_v1
 FOR EACH ROW EXECUTE FUNCTION protect_causal_observation_outbox_event_v1();

INSERT INTO automation_kv(key,value,updated_at) VALUES
 ('causal_decision_observation_enabled','0',now()),
 ('causal_observation_consumer_last_poll','never',now()),
 ('causal_observation_consumer_last_batch_success','never',now()),
 ('causal_observation_consumer_current_batch','0',now())
ON CONFLICT(key) DO NOTHING;

COMMIT;

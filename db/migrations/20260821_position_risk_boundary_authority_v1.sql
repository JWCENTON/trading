BEGIN;

CREATE TABLE IF NOT EXISTS public.position_risk_boundary_event_v1 (
  event_id uuid PRIMARY KEY,
  boundary_id uuid NOT NULL,
  event_sequence bigint NOT NULL CHECK (event_sequence > 0),
  environment text NOT NULL CHECK (environment IN ('PAPER','LIVE')),
  deployment_id text NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper','local-live','vps-live')),
  account_identity_fingerprint text NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
  reservation_id uuid NOT NULL,
  position_id bigint,
  decision_id text NOT NULL CHECK (btrim(decision_id) <> ''),
  intent_id text,
  order_identity text,
  symbol text NOT NULL CHECK (symbol=upper(symbol) AND btrim(symbol)<>''),
  strategy text NOT NULL CHECK (strategy=upper(strategy) AND btrim(strategy)<>''),
  interval text NOT NULL CHECK (interval=lower(interval) AND btrim(interval)<>''),
  side text NOT NULL CHECK (side='LONG'),
  boundary_policy_type text NOT NULL CHECK (boundary_policy_type='ENTRY_BASIS_PERCENT_DISTANCE'),
  boundary_distance_pct numeric(20,12) NOT NULL CHECK (boundary_distance_pct>0 AND boundary_distance_pct<100),
  entry_basis_price numeric(38,18),
  entry_basis_authority text,
  boundary_price numeric(38,18),
  boundary_type text NOT NULL CHECK (boundary_type='SOFTWARE_TRIGGER'),
  execution_price_guarantee text NOT NULL CHECK (execution_price_guarantee='NO'),
  policy_id text NOT NULL CHECK (btrim(policy_id)<>''),
  policy_version text NOT NULL CHECK (btrim(policy_version)<>''),
  policy_fingerprint text NOT NULL CHECK (policy_fingerprint ~ '^[0-9a-f]{64}$'),
  state text NOT NULL CHECK (state IN ('BOUNDARY_POLICY_ACCEPTED','BOUNDARY_ACTIVATED','BOUNDARY_REVISED_ENTRY_BASIS')),
  effective_at timestamptz NOT NULL,
  source_authority text NOT NULL CHECK (btrim(source_authority)<>''),
  provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance)='object'),
  event_fingerprint text NOT NULL CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
  contract_version text NOT NULL CHECK (contract_version='POSITION_RISK_BOUNDARY_AUTHORITY_V1'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(boundary_id,event_sequence),
  UNIQUE(boundary_id,event_fingerprint),
  CHECK ((state='BOUNDARY_POLICY_ACCEPTED' AND position_id IS NULL AND entry_basis_price IS NULL AND boundary_price IS NULL)
      OR (state IN ('BOUNDARY_ACTIVATED','BOUNDARY_REVISED_ENTRY_BASIS') AND position_id IS NOT NULL AND entry_basis_price>0 AND boundary_price>0 AND boundary_price<entry_basis_price AND btrim(coalesce(entry_basis_authority,''))<>'')),
  CHECK ((environment='PAPER' AND deployment_id IN ('local-paper','vps-paper')) OR
         (environment='LIVE' AND deployment_id IN ('local-live','vps-live')))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_position_risk_boundary_reservation_v1
ON public.position_risk_boundary_event_v1(reservation_id)
WHERE event_sequence=1;

CREATE INDEX IF NOT EXISTS ix_position_risk_boundary_position_v1
ON public.position_risk_boundary_event_v1(position_id)
WHERE position_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.position_risk_boundary_event_v1_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prev public.position_risk_boundary_event_v1%ROWTYPE;
DECLARE conflicting_boundary uuid;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.boundary_id::text,0));
  SELECT * INTO prev FROM public.position_risk_boundary_event_v1
   WHERE boundary_id=NEW.boundary_id ORDER BY event_sequence DESC LIMIT 1 FOR UPDATE;
  IF NOT FOUND THEN
    IF NEW.event_sequence<>1 OR NEW.state<>'BOUNDARY_POLICY_ACCEPTED' THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_POLICY_ACCEPTED_REQUIRED';
    END IF;
  ELSE
    IF NEW.event_sequence<>prev.event_sequence+1 THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_EVENT_SEQUENCE_INVALID';
    END IF;
    IF (NEW.environment,NEW.deployment_id,NEW.account_identity_fingerprint,
        NEW.reservation_id,NEW.decision_id,NEW.intent_id,NEW.order_identity,
        NEW.symbol,NEW.strategy,NEW.interval,NEW.side,
        NEW.boundary_policy_type,NEW.boundary_distance_pct,NEW.boundary_type,
        NEW.execution_price_guarantee,NEW.policy_id,NEW.policy_version,
        NEW.policy_fingerprint)
       IS DISTINCT FROM
       (prev.environment,prev.deployment_id,prev.account_identity_fingerprint,
        prev.reservation_id,prev.decision_id,prev.intent_id,prev.order_identity,
        prev.symbol,prev.strategy,prev.interval,prev.side,
        prev.boundary_policy_type,prev.boundary_distance_pct,prev.boundary_type,
        prev.execution_price_guarantee,prev.policy_id,prev.policy_version,
        prev.policy_fingerprint) THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_ACCEPTED_POLICY_IMMUTABLE';
    END IF;
    IF NOT ((prev.state='BOUNDARY_POLICY_ACCEPTED' AND NEW.state='BOUNDARY_ACTIVATED') OR
            (prev.state IN ('BOUNDARY_ACTIVATED','BOUNDARY_REVISED_ENTRY_BASIS') AND NEW.state='BOUNDARY_REVISED_ENTRY_BASIS')) THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_STATE_TRANSITION_INVALID';
    END IF;
    IF prev.position_id IS NOT NULL AND NEW.position_id<>prev.position_id THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_POSITION_ID_IMMUTABLE';
    END IF;
  END IF;
  IF NEW.position_id IS NOT NULL THEN
    SELECT boundary_id INTO conflicting_boundary
      FROM public.v_position_risk_boundary_current_v1
     WHERE position_id=NEW.position_id
       AND environment=NEW.environment
       AND deployment_id=NEW.deployment_id
       AND account_identity_fingerprint=NEW.account_identity_fingerprint
       AND boundary_id<>NEW.boundary_id LIMIT 1;
    IF conflicting_boundary IS NOT NULL THEN
      RAISE EXCEPTION 'RISK_BOUNDARY_POSITION_CONFLICT';
    END IF;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS position_risk_boundary_event_v1_guard_trg ON public.position_risk_boundary_event_v1;
CREATE TRIGGER position_risk_boundary_event_v1_guard_trg BEFORE INSERT
ON public.position_risk_boundary_event_v1 FOR EACH ROW EXECUTE FUNCTION public.position_risk_boundary_event_v1_guard();

CREATE OR REPLACE FUNCTION public.position_risk_boundary_event_v1_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'POSITION_RISK_BOUNDARY_APPEND_ONLY'; END $$;

DROP TRIGGER IF EXISTS position_risk_boundary_event_v1_append_only_trg ON public.position_risk_boundary_event_v1;
CREATE TRIGGER position_risk_boundary_event_v1_append_only_trg BEFORE UPDATE OR DELETE
ON public.position_risk_boundary_event_v1 FOR EACH ROW EXECUTE FUNCTION public.position_risk_boundary_event_v1_append_only();

CREATE OR REPLACE VIEW public.v_position_risk_boundary_current_v1 AS
SELECT DISTINCT ON (boundary_id) * FROM public.position_risk_boundary_event_v1
ORDER BY boundary_id,event_sequence DESC;

COMMIT;

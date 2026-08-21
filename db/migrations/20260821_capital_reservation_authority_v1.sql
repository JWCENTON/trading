BEGIN;

CREATE TABLE IF NOT EXISTS public.capital_reservation_event_v1 (
    event_id uuid PRIMARY KEY,
    reservation_id uuid NOT NULL,
    event_sequence bigint NOT NULL CHECK (event_sequence > 0),
    logical_commitment_key text NOT NULL CHECK (btrim(logical_commitment_key) <> ''),
    source_event_identity text NOT NULL CHECK (btrim(source_event_identity) <> ''),
    environment text NOT NULL CHECK (environment IN ('PAPER','LIVE')),
    deployment_id text NOT NULL CHECK (deployment_id IN ('local-paper','vps-paper','local-live','vps-live')),
    account_identity_fingerprint text NOT NULL CHECK (account_identity_fingerprint ~ '^[0-9a-f]{64}$'),
    purpose text NOT NULL DEFAULT 'ENTRY' CHECK (purpose = 'ENTRY'),
    symbol text NOT NULL CHECK (symbol = upper(symbol) AND btrim(symbol) <> ''),
    strategy text NOT NULL CHECK (strategy = upper(strategy) AND btrim(strategy) <> ''),
    interval text NOT NULL CHECK (interval = lower(interval) AND btrim(interval) <> ''),
    decision_identity text,
    intent_identity text,
    order_identity text,
    position_id bigint,
    requested_notional numeric(38,18) NOT NULL CHECK (requested_notional > 0),
    remaining_reserved_notional numeric(38,18) NOT NULL CHECK (remaining_reserved_notional >= 0),
    deployed_notional numeric(38,18) NOT NULL CHECK (deployed_notional >= 0),
    released_notional numeric(38,18) NOT NULL CHECK (released_notional >= 0),
    state text NOT NULL CHECK (state IN (
      'ACCEPTED_COMMITMENT','INTERNAL_RESERVED','SUBMITTED','EXCHANGE_ACK',
      'EXCHANGE_LOCKED','PARTIALLY_DEPLOYED','DEPLOYED','RELEASED',
      'CANCELLED','EXPIRED','REJECTED'
    )),
    reflection_state text NOT NULL CHECK (reflection_state IN (
      'INTERNAL_UNREFLECTED','EXCHANGE_REFLECTED','PAPER_SIMULATED'
    )),
    reconciliation_status text NOT NULL CHECK (reconciliation_status IN (
      'CANONICAL','PENDING_EXCHANGE_REFLECTION','RECONCILIATION_FAILED'
    )),
    reconciliation_reason text,
    release_reason text,
    effective_at timestamptz NOT NULL,
    source_authority text NOT NULL CHECK (btrim(source_authority) <> ''),
    provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance) = 'object'),
    policy_fingerprint text CHECK (policy_fingerprint IS NULL OR policy_fingerprint ~ '^[0-9a-f]{64}$'),
    event_fingerprint text NOT NULL CHECK (event_fingerprint ~ '^[0-9a-f]{64}$'),
    contract_version text NOT NULL CHECK (contract_version = 'CAPITAL_RESERVATION_AUTHORITY_V1'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (reservation_id,event_sequence),
    UNIQUE (reservation_id,source_event_identity),
    UNIQUE (event_fingerprint),
    CHECK (requested_notional = remaining_reserved_notional + deployed_notional + released_notional),
    CHECK ((reconciliation_status <> 'RECONCILIATION_FAILED') OR btrim(coalesce(reconciliation_reason,'')) <> ''),
    CHECK ((state NOT IN ('RELEASED','CANCELLED','EXPIRED','REJECTED')) OR btrim(coalesce(release_reason,'')) <> ''),
    CHECK ((environment = 'PAPER' AND deployment_id IN ('local-paper','vps-paper') AND reflection_state = 'PAPER_SIMULATED') OR
           (environment = 'LIVE' AND deployment_id IN ('local-live','vps-live') AND reflection_state <> 'PAPER_SIMULATED')),
    CHECK ((state NOT IN ('DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR remaining_reserved_notional = 0),
    CHECK ((state <> 'DEPLOYED') OR deployed_notional = requested_notional)
);

CREATE INDEX IF NOT EXISTS capital_reservation_event_v1_scope_idx
ON public.capital_reservation_event_v1
(environment,deployment_id,account_identity_fingerprint,reservation_id,event_sequence DESC);

CREATE OR REPLACE FUNCTION public.capital_reservation_event_v1_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prev public.capital_reservation_event_v1%ROWTYPE;
DECLARE other_reservation uuid;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.logical_commitment_key,0));
  SELECT reservation_id INTO other_reservation
  FROM public.capital_reservation_event_v1
  WHERE logical_commitment_key=NEW.logical_commitment_key
  LIMIT 1;
  IF other_reservation IS NOT NULL AND other_reservation <> NEW.reservation_id THEN
    RAISE EXCEPTION 'CAPITAL_RESERVATION_LOGICAL_COMMITMENT_CONFLICT';
  END IF;

  SELECT * INTO prev FROM public.capital_reservation_event_v1
  WHERE reservation_id=NEW.reservation_id ORDER BY event_sequence DESC LIMIT 1 FOR UPDATE;
  IF NOT FOUND THEN
    IF NEW.event_sequence <> 1 OR NEW.state <> 'ACCEPTED_COMMITMENT' THEN
      RAISE EXCEPTION 'CAPITAL_RESERVATION_ACCEPTED_COMMITMENT_REQUIRED';
    END IF;
  ELSE
    IF NEW.event_sequence <> prev.event_sequence + 1 THEN
      RAISE EXCEPTION 'CAPITAL_RESERVATION_EVENT_SEQUENCE_INVALID';
    END IF;
    IF prev.state IN ('DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED') THEN
      RAISE EXCEPTION 'CAPITAL_RESERVATION_TERMINAL_REACTIVATION_FORBIDDEN';
    END IF;
    IF (NEW.logical_commitment_key,NEW.environment,NEW.deployment_id,
        NEW.account_identity_fingerprint,NEW.purpose,NEW.symbol,NEW.strategy,
        NEW.interval,NEW.requested_notional)
       IS DISTINCT FROM
       (prev.logical_commitment_key,prev.environment,prev.deployment_id,
        prev.account_identity_fingerprint,prev.purpose,prev.symbol,prev.strategy,
        prev.interval,prev.requested_notional) THEN
      RAISE EXCEPTION 'CAPITAL_RESERVATION_IMMUTABLE_IDENTITY_CHANGED';
    END IF;
    IF NEW.deployed_notional < prev.deployed_notional OR
       NEW.released_notional < prev.released_notional OR
       NEW.remaining_reserved_notional > prev.remaining_reserved_notional THEN
      RAISE EXCEPTION 'CAPITAL_RESERVATION_AMOUNT_REGRESSION';
    END IF;
    IF NOT (
      (prev.state='ACCEPTED_COMMITMENT' AND NEW.state IN ('INTERNAL_RESERVED','SUBMITTED','EXCHANGE_ACK','EXCHANGE_LOCKED','PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR
      (prev.state='INTERNAL_RESERVED' AND NEW.state IN ('SUBMITTED','EXCHANGE_ACK','EXCHANGE_LOCKED','PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR
      (prev.state='SUBMITTED' AND NEW.state IN ('EXCHANGE_ACK','EXCHANGE_LOCKED','PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR
      (prev.state='EXCHANGE_ACK' AND NEW.state IN ('EXCHANGE_LOCKED','PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR
      (prev.state='EXCHANGE_LOCKED' AND NEW.state IN ('PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED')) OR
      (prev.state='PARTIALLY_DEPLOYED' AND NEW.state IN ('PARTIALLY_DEPLOYED','DEPLOYED','RELEASED','CANCELLED','EXPIRED','REJECTED'))
    ) THEN RAISE EXCEPTION 'CAPITAL_RESERVATION_STATE_TRANSITION_INVALID'; END IF;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS capital_reservation_event_v1_guard_trg ON public.capital_reservation_event_v1;
CREATE TRIGGER capital_reservation_event_v1_guard_trg BEFORE INSERT
ON public.capital_reservation_event_v1 FOR EACH ROW EXECUTE FUNCTION public.capital_reservation_event_v1_guard();

CREATE OR REPLACE FUNCTION public.capital_reservation_event_v1_append_only()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'CAPITAL_RESERVATION_APPEND_ONLY'; END $$;

DROP TRIGGER IF EXISTS capital_reservation_event_v1_append_only_trg ON public.capital_reservation_event_v1;
CREATE TRIGGER capital_reservation_event_v1_append_only_trg BEFORE UPDATE OR DELETE
ON public.capital_reservation_event_v1 FOR EACH ROW EXECUTE FUNCTION public.capital_reservation_event_v1_append_only();

CREATE OR REPLACE VIEW public.v_capital_reservation_current_v1 AS
SELECT DISTINCT ON (reservation_id) *
FROM public.capital_reservation_event_v1
ORDER BY reservation_id,event_sequence DESC;

COMMIT;

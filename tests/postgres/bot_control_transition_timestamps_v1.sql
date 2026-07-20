\set ON_ERROR_STOP on

-- PAPER-like upgrade: existing rows, PK, lookup index and audit trigger.
CREATE TABLE public.bot_control (
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  interval TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, strategy, interval)
);
CREATE INDEX idx_bot_control_lookup
  ON public.bot_control (symbol, interval, strategy);
CREATE FUNCTION public.bot_control_audit_fn() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER bot_control_audit_trg
  AFTER UPDATE ON public.bot_control
  FOR EACH ROW EXECUTE FUNCTION public.bot_control_audit_fn();
INSERT INTO public.bot_control (symbol, strategy, interval)
SELECT 'SYM' || value, 'RSI', '1m' FROM generate_series(1, 32) AS value;

\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql
\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql

DO $$
DECLARE
  column_count INTEGER;
BEGIN
  IF (SELECT count(*) FROM public.bot_control) <> 32 THEN
    RAISE EXCEPTION 'PAPER-like rows were not preserved';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.bot_control
     WHERE live_since IS NOT NULL OR last_disabled_at IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'PAPER-like rows were backfilled';
  END IF;
  SELECT count(*) INTO column_count
    FROM information_schema.columns
   WHERE table_schema='public' AND table_name='bot_control'
     AND column_name IN ('live_since', 'last_disabled_at')
     AND data_type='timestamp with time zone'
     AND udt_name='timestamptz'
     AND is_nullable='YES'
     AND column_default IS NULL;
  IF column_count <> 2 THEN
    RAISE EXCEPTION 'transition timestamp column contract mismatch';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid='public.bot_control'::regclass
       AND conname='bot_control_pkey' AND contype='p'
  ) THEN
    RAISE EXCEPTION 'primary key was not preserved';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
     WHERE schemaname='public' AND tablename='bot_control'
       AND indexname='idx_bot_control_lookup'
  ) THEN
    RAISE EXCEPTION 'lookup index was not preserved';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
     WHERE tgrelid='public.bot_control'::regclass
       AND tgname='bot_control_audit_trg' AND NOT tgisinternal
  ) THEN
    RAISE EXCEPTION 'audit trigger was not preserved';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_indexes
     WHERE schemaname='public' AND tablename='bot_control'
       AND indexname='idx_bot_control_slot'
  ) THEN
    RAISE EXCEPTION 'redundant slot index was created';
  END IF;
END $$;

-- LIVE-like upgrade: columns already exist and retain their values.
DROP TABLE public.bot_control;
DROP FUNCTION public.bot_control_audit_fn();
CREATE TABLE public.bot_control (
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  interval TEXT NOT NULL,
  live_since TIMESTAMPTZ,
  last_disabled_at TIMESTAMPTZ,
  PRIMARY KEY (symbol, strategy, interval)
);
INSERT INTO public.bot_control VALUES
  ('BTCUSDC', 'RSI', '1m', '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z');
\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql
\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM public.bot_control
     WHERE live_since='2026-01-01T00:00:00Z'::timestamptz
       AND last_disabled_at='2026-01-02T00:00:00Z'::timestamptz
  ) THEN
    RAISE EXCEPTION 'LIVE-like timestamp values were not preserved';
  END IF;
END $$;

-- Fresh installation: canonical base table followed by the additive migration.
DROP TABLE public.bot_control;
CREATE TABLE public.bot_control (
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  interval TEXT NOT NULL,
  PRIMARY KEY (symbol, strategy, interval)
);
\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql
\i /repo/db/migrations/20260720_bot_control_transition_timestamps_v1.sql
DO $$
BEGIN
  IF (SELECT count(*) FROM information_schema.columns
       WHERE table_schema='public' AND table_name='bot_control'
         AND column_name IN ('live_since', 'last_disabled_at')) <> 2 THEN
    RAISE EXCEPTION 'fresh schema transition columns missing';
  END IF;
END $$;

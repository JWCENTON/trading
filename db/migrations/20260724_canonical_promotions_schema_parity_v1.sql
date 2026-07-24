BEGIN;

-- Canonical endpoint-compatible promotions schema. This migration intentionally
-- contains no environment routing, cross-database references, or data copies.

CREATE TABLE IF NOT EXISTS public.promoted_candidates (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,
  paper_score NUMERIC NOT NULL,
  n_trades INTEGER,
  win_rate NUMERIC,
  net_sum NUMERIC,
  window_name TEXT,
  policy_version TEXT,
  source_ts TIMESTAMPTZ,
  published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  eligible_live BOOLEAN NOT NULL DEFAULT false,
  elig_reason TEXT,
  PRIMARY KEY (symbol, interval, strategy)
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'promoted_candidates'
      AND column_name = 'window'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'promoted_candidates'
        AND column_name = 'window_name'
    ) THEN
      RAISE EXCEPTION
        'promoted_candidates has both window and window_name; refusing ambiguous migration';
    END IF;
    ALTER TABLE public.promoted_candidates RENAME COLUMN "window" TO window_name;
  END IF;
END
$$;

ALTER TABLE public.promoted_candidates
  ADD COLUMN IF NOT EXISTS eligible_live BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS elig_reason TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.promoted_candidates'::regclass
      AND conname = 'promoted_candidates_elig_consistency'
  ) THEN
    ALTER TABLE public.promoted_candidates
      ADD CONSTRAINT promoted_candidates_elig_consistency
      CHECK (
        eligible_live = false
        OR (n_trades IS NOT NULL AND n_trades > 0 AND policy_version IS NOT NULL)
      );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS ix_promoted_candidates_published_at
  ON public.promoted_candidates (published_at DESC);
CREATE INDEX IF NOT EXISTS ix_promoted_candidates_score
  ON public.promoted_candidates (paper_score DESC);

CREATE TABLE IF NOT EXISTS public.promotion_events (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_ts TIMESTAMPTZ,
  window_name TEXT,
  policy_version TEXT,
  n_rows INTEGER NOT NULL DEFAULT 0,
  hash TEXT NOT NULL,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'promotion_events'
      AND column_name = 'window'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'promotion_events'
        AND column_name = 'window_name'
    ) THEN
      RAISE EXCEPTION
        'promotion_events has both window and window_name; refusing ambiguous migration';
    END IF;
    ALTER TABLE public.promotion_events RENAME COLUMN "window" TO window_name;
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_promotion_events_hash
  ON public.promotion_events (hash);
CREATE INDEX IF NOT EXISTS ix_promotion_events_created_at
  ON public.promotion_events (created_at DESC);

CREATE TABLE IF NOT EXISTS public.promoted_regime_candidates (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  strategy TEXT NOT NULL,
  market_regime TEXT NOT NULL,
  paper_score NUMERIC NOT NULL,
  n_trades INTEGER,
  win_rate NUMERIC,
  net_sum NUMERIC,
  profit_factor NUMERIC,
  fee_pressure_pct NUMERIC,
  window_name TEXT,
  policy_version TEXT,
  source_ts TIMESTAMPTZ,
  published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  eligible_live BOOLEAN NOT NULL DEFAULT false,
  elig_reason TEXT,
  PRIMARY KEY (symbol, interval, strategy, market_regime)
);

CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_published_at
  ON public.promoted_regime_candidates (published_at DESC);
CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_score
  ON public.promoted_regime_candidates (paper_score DESC);
CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_lookup
  ON public.promoted_regime_candidates
  (symbol, interval, strategy, market_regime);

COMMIT;

BEGIN;

CREATE TABLE IF NOT EXISTS promoted_regime_candidates (
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

  PRIMARY KEY(symbol, interval, strategy, market_regime)
);

CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_published_at
ON promoted_regime_candidates(published_at DESC);

CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_score
ON promoted_regime_candidates(paper_score DESC);

CREATE INDEX IF NOT EXISTS ix_promoted_regime_candidates_lookup
ON promoted_regime_candidates(symbol, interval, strategy, market_regime);

INSERT INTO automation_kv(key, value)
VALUES
  ('regime_aware_promotions_enabled', '1'),
  ('regime_aware_promotions_version', 'v1'),
  ('regime_aware_promotions_source_view', 'v_slot_profile_v1_14d')
ON CONFLICT (key)
DO UPDATE SET value = EXCLUDED.value;

COMMIT;

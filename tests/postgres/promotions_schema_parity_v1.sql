\set ON_ERROR_STOP on

DO $$
DECLARE
  v_missing INTEGER;
BEGIN
  SELECT count(*) INTO v_missing
  FROM (
    VALUES
      ('promoted_candidates', 'symbol'),
      ('promoted_candidates', 'interval'),
      ('promoted_candidates', 'strategy'),
      ('promoted_candidates', 'paper_score'),
      ('promoted_candidates', 'window_name'),
      ('promoted_candidates', 'eligible_live'),
      ('promoted_candidates', 'elig_reason'),
      ('promotion_events', 'hash'),
      ('promotion_events', 'window_name'),
      ('promoted_regime_candidates', 'market_regime'),
      ('promoted_regime_candidates', 'profit_factor'),
      ('promoted_regime_candidates', 'fee_pressure_pct'),
      ('promoted_regime_candidates', 'eligible_live'),
      ('promoted_regime_candidates', 'elig_reason')
  ) AS required(table_name, column_name)
  WHERE NOT EXISTS (
    SELECT 1
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.table_name = required.table_name
      AND c.column_name = required.column_name
  );
  IF v_missing <> 0 THEN
    RAISE EXCEPTION 'missing % endpoint columns', v_missing;
  END IF;
END
$$;

INSERT INTO public.promoted_candidates (
  symbol, interval, strategy, paper_score, n_trades, win_rate, net_sum,
  window_name, policy_version, source_ts, published_at, meta,
  eligible_live, elig_reason
) VALUES (
  'VALIDATION', '1m', 'RSI', 1.0, 10, 0.5, 2.0,
  'schema-v1', 'validation-v1', now(), now(), '{"validation":true}',
  false, 'validation'
)
ON CONFLICT (symbol, interval, strategy) DO UPDATE SET
  paper_score = EXCLUDED.paper_score;

INSERT INTO public.promoted_candidates (
  symbol, interval, strategy, paper_score, n_trades, window_name,
  policy_version, source_ts, published_at, meta, eligible_live
) VALUES (
  'VALIDATION', '1m', 'RSI', 2.0, 11, 'schema-v1',
  'validation-v1', now(), now(), '{"validation":true}', false
)
ON CONFLICT (symbol, interval, strategy) DO UPDATE SET
  paper_score = EXCLUDED.paper_score,
  n_trades = EXCLUDED.n_trades;

INSERT INTO public.promoted_regime_candidates (
  symbol, interval, strategy, market_regime, paper_score, n_trades,
  win_rate, net_sum, profit_factor, fee_pressure_pct, window_name,
  policy_version, source_ts, published_at, meta, eligible_live, elig_reason
) VALUES (
  'VALIDATION', '1m', 'RSI', 'RANGING', 1.0, 10,
  0.5, 2.0, 1.2, 0.1, 'schema-v1',
  'validation-v1', now(), now(), '{"validation":true}', false, 'validation'
)
ON CONFLICT (symbol, interval, strategy, market_regime) DO UPDATE SET
  paper_score = EXCLUDED.paper_score;

INSERT INTO public.promotion_events (
  source_ts, window_name, policy_version, n_rows, hash, meta
) VALUES (
  now(), 'schema-v1', 'validation-v1', 1,
  'validation:promotions-schema-v1', '{"validation":true}'
);

DO $$
BEGIN
  BEGIN
    INSERT INTO public.promotion_events (
      source_ts, window_name, policy_version, n_rows, hash, meta
    ) VALUES (
      now(), 'schema-v1', 'validation-v1', 1,
      'validation:promotions-schema-v1', '{"validation":true}'
    );
    RAISE EXCEPTION 'duplicate event hash was accepted';
  EXCEPTION
    WHEN unique_violation THEN NULL;
  END;

  BEGIN
    INSERT INTO public.promoted_candidates (
      symbol, interval, strategy, paper_score, n_trades,
      policy_version, eligible_live
    ) VALUES ('INVALID', '1m', 'RSI', 1.0, 0, 'validation-v1', true);
    RAISE EXCEPTION 'eligible_live consistency violation was accepted';
  EXCEPTION
    WHEN check_violation THEN NULL;
  END;
END
$$;

DO $$
BEGIN
  IF (SELECT paper_score FROM public.promoted_candidates
      WHERE symbol = 'VALIDATION' AND interval = '1m' AND strategy = 'RSI')
      <> 2.0 THEN
    RAISE EXCEPTION 'candidate update contract failed';
  END IF;
  IF (SELECT count(*) FROM public.promotion_events
      WHERE hash = 'validation:promotions-schema-v1') <> 1 THEN
    RAISE EXCEPTION 'event idempotency contract failed';
  END IF;
END
$$;

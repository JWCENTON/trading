BEGIN;

CREATE OR REPLACE VIEW v_trade_entry_exit_replay_v1 AS
WITH trades AS (
  SELECT
    p.id,
    p.symbol,
    p.interval,
    p.strategy,
    p.side,
    p.status,
    p.entry_time,
    p.exit_time,
    p.entry_price::numeric AS entry_price,
    p.exit_price::numeric AS exit_price,
    p.qty::numeric AS qty,
    p.exit_reason,
    p.market_regime,
    p.gross_pnl_usdc::numeric AS gross_pnl_usdc,
    p.fees_usdc::numeric AS fees_usdc,
    p.net_pnl_usdc::numeric AS net_pnl_usdc,
    p.hold_minutes::numeric AS hold_minutes,
    NULLIF((p.qty::numeric * p.entry_price::numeric), 0) AS entry_notional_usdc
  FROM positions p
  WHERE p.status = 'CLOSED'
    AND p.entry_time IS NOT NULL
    AND p.exit_time IS NOT NULL
    AND p.entry_price IS NOT NULL
    AND p.exit_price IS NOT NULL
    AND p.qty IS NOT NULL
),
during_trade AS (
  SELECT
    t.id,
    MAX(c.high::numeric) AS max_high_during_trade,
    MIN(c.low::numeric) AS min_low_during_trade
  FROM trades t
  JOIN candles c
    ON c.symbol = t.symbol
   AND c.interval = t.interval
   AND c.open_time >= t.entry_time
   AND c.open_time <= t.exit_time
  GROUP BY t.id
),
after_exit AS (
  SELECT
    t.id,
    MAX(c.high::numeric) FILTER (WHERE c.open_time <= t.exit_time + interval '15 minutes') AS max_high_after_15m,
    MIN(c.low::numeric)  FILTER (WHERE c.open_time <= t.exit_time + interval '15 minutes') AS min_low_after_15m,
    MAX(c.high::numeric) FILTER (WHERE c.open_time <= t.exit_time + interval '30 minutes') AS max_high_after_30m,
    MIN(c.low::numeric)  FILTER (WHERE c.open_time <= t.exit_time + interval '30 minutes') AS min_low_after_30m,
    MAX(c.high::numeric) FILTER (WHERE c.open_time <= t.exit_time + interval '60 minutes') AS max_high_after_60m,
    MIN(c.low::numeric)  FILTER (WHERE c.open_time <= t.exit_time + interval '60 minutes') AS min_low_after_60m
  FROM trades t
  JOIN candles c
    ON c.symbol = t.symbol
   AND c.interval = t.interval
   AND c.open_time > t.exit_time
   AND c.open_time <= t.exit_time + interval '60 minutes'
  GROUP BY t.id
),
future_close AS (
  SELECT
    t.id,
    c15.close::numeric AS close_15m,
    c30.close::numeric AS close_30m,
    c60.close::numeric AS close_60m
  FROM trades t
  LEFT JOIN LATERAL (
    SELECT c.close
    FROM candles c
    WHERE c.symbol=t.symbol AND c.interval=t.interval
      AND c.open_time >= t.exit_time + interval '15 minutes'
    ORDER BY c.open_time ASC
    LIMIT 1
  ) c15 ON true
  LEFT JOIN LATERAL (
    SELECT c.close
    FROM candles c
    WHERE c.symbol=t.symbol AND c.interval=t.interval
      AND c.open_time >= t.exit_time + interval '30 minutes'
    ORDER BY c.open_time ASC
    LIMIT 1
  ) c30 ON true
  LEFT JOIN LATERAL (
    SELECT c.close
    FROM candles c
    WHERE c.symbol=t.symbol AND c.interval=t.interval
      AND c.open_time >= t.exit_time + interval '60 minutes'
    ORDER BY c.open_time ASC
    LIMIT 1
  ) c60 ON true
)
SELECT
  t.*,

  ROUND((t.fees_usdc / NULLIF(t.entry_notional_usdc,0)) * 100, 6) AS fee_pct_of_entry,

  CASE WHEN t.side='LONG'
    THEN ROUND(((dt.max_high_during_trade - t.entry_price) / t.entry_price) * 100, 6)
  END AS mfe_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((dt.min_low_during_trade - t.entry_price) / t.entry_price) * 100, 6)
  END AS mae_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((t.exit_price - t.entry_price) / t.entry_price) * 100, 6)
  END AS actual_exit_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND((((dt.max_high_during_trade - t.entry_price) / t.entry_price) * 100)
             - (((t.exit_price - t.entry_price) / t.entry_price) * 100), 6)
  END AS giveback_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((fc.close_15m - t.entry_price) / t.entry_price) * 100, 6)
  END AS hold_to_15m_exit_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((fc.close_30m - t.entry_price) / t.entry_price) * 100, 6)
  END AS hold_to_30m_exit_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((fc.close_60m - t.entry_price) / t.entry_price) * 100, 6)
  END AS hold_to_60m_exit_pct,

  CASE WHEN t.side='LONG'
    THEN ROUND(((ae.max_high_after_60m - t.entry_price) / t.entry_price) * 100, 6)
  END AS best_possible_after_exit_60m_pct,

  CASE
    WHEN t.side='LONG'
     AND ae.max_high_after_60m IS NOT NULL
     AND (((ae.max_high_after_60m - t.entry_price) / t.entry_price) * 100)
         > (((t.exit_price - t.entry_price) / t.entry_price) * 100)
           + GREATEST(0.05, ((t.fees_usdc / NULLIF(t.entry_notional_usdc,0)) * 100))
      THEN 'EXIT_TOO_EARLY_CANDIDATE'
    WHEN t.side='LONG'
     AND ae.min_low_after_60m IS NOT NULL
     AND (((ae.min_low_after_60m - t.entry_price) / t.entry_price) * 100)
         < (((t.exit_price - t.entry_price) / t.entry_price) * 100) - 0.20
      THEN 'EXIT_PROTECTED_CAPITAL_CANDIDATE'
    ELSE 'EXIT_NOT_PROVEN_BAD'
  END AS exit_replay_label

FROM trades t
LEFT JOIN during_trade dt ON dt.id=t.id
LEFT JOIN after_exit ae ON ae.id=t.id
LEFT JOIN future_close fc ON fc.id=t.id;

CREATE OR REPLACE VIEW v_trade_entry_exit_replay_summary_v1 AS
SELECT
  strategy,
  symbol,
  interval,
  exit_reason,
  COUNT(*) AS trades,
  ROUND(SUM(net_pnl_usdc), 6) AS net_sum,
  ROUND(AVG(net_pnl_usdc), 6) AS avg_net,
  ROUND(AVG(mfe_pct), 6) AS avg_mfe_pct,
  ROUND(AVG(mae_pct), 6) AS avg_mae_pct,
  ROUND(AVG(actual_exit_pct), 6) AS avg_actual_exit_pct,
  ROUND(AVG(giveback_pct), 6) AS avg_giveback_pct,
  COUNT(*) FILTER (WHERE exit_replay_label='EXIT_TOO_EARLY_CANDIDATE') AS too_early_candidates,
  COUNT(*) FILTER (WHERE exit_replay_label='EXIT_PROTECTED_CAPITAL_CANDIDATE') AS protected_capital_candidates,
  COUNT(*) FILTER (WHERE exit_replay_label='EXIT_NOT_PROVEN_BAD') AS not_proven_bad
FROM v_trade_entry_exit_replay_v1
WHERE exit_time >= now() - interval '14 days'
GROUP BY strategy, symbol, interval, exit_reason
ORDER BY too_early_candidates DESC, net_sum ASC;

COMMIT;

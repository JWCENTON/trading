CREATE OR REPLACE VIEW v_okx_orphan_fills_audit AS
WITH fills AS (
  SELECT
    f.id AS fill_id,
    f.source,
    f.order_id,
    f.trade_id,
    f.symbol,
    f.side,
    f.executed_qty,
    f.avg_price,
    f.quote_notional_usdc,
    f.commission_usdc,
    f.event_time,
    f.created_at,
    NULLIF(f.raw->'raw'->>'clOrdId','') AS clordid,
    CASE
      WHEN COALESCE(NULLIF(f.raw->'raw'->>'clOrdId',''), '') ILIKE 'OKXCANARY%'
        THEN true
      ELSE false
    END AS is_canary
  FROM binance_order_fills f
  WHERE f.source='okx'
),
matched AS (
  SELECT
    f.*,
    p_entry.id AS entry_position_id,
    p_exit.id AS exit_position_id
  FROM fills f
  LEFT JOIN positions p_entry
    ON f.side='BUY'
   AND (
        p_entry.entry_order_id=f.order_id
        OR p_entry.entry_client_order_id=f.clordid
   )
  LEFT JOIN positions p_exit
    ON f.side='SELL'
   AND (
        p_exit.exit_order_id=f.order_id
        OR p_exit.exit_client_order_id=f.clordid
   )
)
SELECT
  *,
  CASE
    WHEN is_canary THEN 'CANARY_IGNORE'
    WHEN side='BUY' AND entry_position_id IS NULL AND COALESCE(clordid,'') = '' THEN 'MANUAL_OR_EXTERNAL_BUY'
    WHEN side='BUY' AND entry_position_id IS NULL THEN 'ORPHAN_BUY_FILL'
    WHEN side='SELL' AND exit_position_id IS NULL AND COALESCE(clordid,'') = '' THEN 'MANUAL_OR_EXTERNAL_SELL'
    WHEN side='SELL' AND exit_position_id IS NULL THEN 'ORPHAN_SELL_FILL'
    ELSE 'MATCHED'
  END AS audit_status
FROM matched;

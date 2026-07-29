\set ON_ERROR_STOP on

INSERT INTO runtime_contract_adoption_v1(
  contract_name,environment,deployment_id,adopted_at,git_revision,
  migration_version
) VALUES (
  'FEE_AWARE_INVENTORY_C2_2','live','local-live',
  '2026-07-29 12:00:00+00','test-revision','C2.2.1'
);

INSERT INTO financial_truth_instrument_snapshot_v1(
  source_authority,exchange,symbol,base_asset,quote_asset,step_size,min_qty,
  quantity_precision,price_precision,min_notional,metadata_source,
  metadata_version,metadata_fingerprint,captured_at
) VALUES (
  'EXCHANGE_EXECUTION','OKX','SOLUSDC','SOL','USDC',0.00001,0.01,
  5,2,0,'TEST','V1','c221-runtime-fixture','2026-07-29 12:00:00+00'
);

INSERT INTO positions(
  id,symbol,strategy,interval,status,side,qty,entry_price,entry_time,
  entry_order_id,exit_order_id
) VALUES
  (3079,'BTCUSDC','RSI','1m','OPEN','LONG',0.00000110,63400.1,'2026-07-17 02:49:51+00','e3079','x3079'),
  (3080,'BNBUSDC','TREND','5m','OPEN','LONG',0.000123,566.1,'2026-07-17 18:35:18+00','e3080','x3080'),
  (3081,'ETHUSDC','TREND','5m','OPEN','LONG',0.000038,1880.79,'2026-07-20 01:25:59+00','e3081','x3081'),
  (3082,'ETHUSDC','TREND','1m','OPEN','LONG',0.000037,1889.58,'2026-07-20 11:47:33+00','e3082','x3082'),
  (3083,'SOLUSDC','TREND','1m','OPEN','LONG',0.00090,77.04,'2026-07-20 12:00:27+00','e3083','x3083'),
  (3084,'BTCUSDC','TREND','5m','OPEN','LONG',0.00000106,65912.4,'2026-07-21 07:30:16+00','e3084','x3084'),
  (3085,'SOLUSDC','TREND','5m','OPEN','LONG',0.00095,74.24,'2026-07-29 08:00:47+00','e3085','x3085'),
  (4000,'SOLUSDC','BBRANGE','5m','OPEN','LONG',0.999,73.71,'2026-07-29 13:00:00+00','e4000','x4000');

INSERT INTO binance_orders(
  symbol,side,order_type,order_id,status,position_id,order_purpose,
  exchange_source,reconciled_executed_qty
)
SELECT p.symbol,'BUY','MARKET',p.entry_order_id,'FILLED',p.id,'ENTRY','okx',0
FROM positions p
UNION ALL
SELECT p.symbol,'SELL','MARKET',p.exit_order_id,'FILLED',p.id,'EXIT','okx',0
FROM positions p;

INSERT INTO binance_order_fills(
  source,trade_id,order_id,symbol,side,executed_qty,avg_price,
  quote_notional_usdc,commission_amount,commission_asset,event_time,
  instrument_snapshot_id,raw
) VALUES
  ('okx',30791,'e3079','BTCUSDC','BUY',0.00031545,63400.1,20,0.000001104075,'BTC','2026-07-17 02:49:51+00',NULL,'{}'),
  ('okx',30792,'x3079','BTCUSDC','SELL',0.00031435,63500,20,0.069,'USDC','2026-07-17 03:05:09+00',NULL,'{}'),
  ('okx',30801,'e3080','BNBUSDC','BUY',0.035152,566.1,20,0.000123032,'BNB','2026-07-17 18:35:18+00',NULL,'{}'),
  ('okx',30802,'x3080','BNBUSDC','SELL',0.035029,567,20,0.069,'USDC','2026-07-17 18:55:33+00',NULL,'{}'),
  ('okx',30811,'e3081','ETHUSDC','BUY',0.010623,1880.79,20,0.0000371805,'ETH','2026-07-20 01:25:59+00',NULL,'{}'),
  ('okx',30812,'x3081','ETHUSDC','SELL',0.010585,1882,20,0.069,'USDC','2026-07-20 01:40:27+00',NULL,'{}'),
  ('okx',30821,'e3082','ETHUSDC','BUY',0.010584,1889.58,20,0.000037044,'ETH','2026-07-20 11:47:33+00',NULL,'{}'),
  ('okx',30822,'x3082','ETHUSDC','SELL',0.010547,1890,20,0.069,'USDC','2026-07-20 12:18:24+00',NULL,'{}'),
  ('okx',30831,'e3083','SOLUSDC','BUY',0.25921,77.04,20,0.000907235,'SOL','2026-07-20 12:00:27+00',NULL,'{}'),
  ('okx',30832,'x3083','SOLUSDC','SELL',0.25831,77.1,20,0.069,'USDC','2026-07-20 12:30:46+00',NULL,'{}'),
  ('okx',30841,'e3084','BTCUSDC','BUY',0.00030191,65912.4,20,0.000001056685,'BTC','2026-07-21 07:30:16+00',NULL,'{}'),
  ('okx',30842,'x3084','BTCUSDC','SELL',0.00030085,66000,20,0.069,'USDC','2026-07-21 09:05:50+00',NULL,'{}'),
  ('okx',30851,'e3085','SOLUSDC','BUY',0.26924,74.24,20,0.000942340,'SOL','2026-07-29 08:00:47+00',1,'{}'),
  ('okx',30852,'x3085','SOLUSDC','SELL',0.26829,74.3,20,0.069,'USDC','2026-07-29 08:31:07+00',1,'{}'),
  ('okx',40001,'e4000','SOLUSDC','BUY',1.0,73.71,73.71,0.001,'SOL','2026-07-29 13:00:00+00',1,'{}'),
  ('okx',40002,'x4000','SOLUSDC','SELL',0.998995,73.8,73.72,0.069,'USDC','2026-07-29 13:05:00+00',1,'{}');

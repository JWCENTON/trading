\set ON_ERROR_STOP on

BEGIN;
SELECT pg_advisory_xact_lock(987654321);

DO $$ BEGIN
  IF to_regclass('public.candles') IS NULL THEN
    CREATE TABLE candles (
      id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, interval TEXT NOT NULL,
      open_time TIMESTAMPTZ NOT NULL, open NUMERIC, high NUMERIC, low NUMERIC,
      close NUMERIC, volume NUMERIC, close_time TIMESTAMPTZ NOT NULL,
      trades INTEGER, ema_21 NUMERIC, rsi_14 NUMERIC,
      atr_14 DOUBLE PRECISION, supertrend DOUBLE PRECISION,
      supertrend_direction INTEGER, UNIQUE(symbol, interval, open_time)
    );
  END IF;
  IF to_regclass('public.simulated_orders') IS NULL THEN
    CREATE TABLE simulated_orders (
      id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      symbol TEXT NOT NULL, interval TEXT NOT NULL, strategy TEXT NOT NULL,
      side TEXT NOT NULL, price NUMERIC NOT NULL, quantity_btc NUMERIC NOT NULL,
      reason TEXT, rsi_14 NUMERIC, ema_21 NUMERIC,
      candle_open_time TIMESTAMPTZ NOT NULL,
      is_exit BOOLEAN NOT NULL DEFAULT false
    );
  END IF;
  IF to_regclass('public.positions') IS NULL THEN
    CREATE TABLE positions (
      id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
      interval TEXT NOT NULL, status TEXT NOT NULL, side TEXT NOT NULL,
      qty NUMERIC NOT NULL, entry_price NUMERIC NOT NULL,
      entry_time TIMESTAMPTZ NOT NULL DEFAULT now(), exit_price NUMERIC,
      exit_time TIMESTAMPTZ, exit_reason TEXT, entry_order_id TEXT,
      exit_order_id TEXT, entry_client_order_id TEXT, exit_client_order_id TEXT
    );
  END IF;
  IF to_regclass('public.strategy_params') IS NULL THEN
    CREATE TABLE strategy_params (
      id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
      interval TEXT NOT NULL DEFAULT '1m', param_name TEXT NOT NULL,
      param_value NUMERIC NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  END IF;
  IF to_regclass('public.strategy_params_history') IS NULL THEN
    CREATE TABLE strategy_params_history (
      id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
      interval TEXT NOT NULL DEFAULT '1m', param_name TEXT NOT NULL,
      old_value NUMERIC, new_value NUMERIC NOT NULL,
      changed_at TIMESTAMPTZ NOT NULL DEFAULT now(), source TEXT NOT NULL
    );
  END IF;
  IF to_regclass('public.bot_heartbeat') IS NULL THEN
    CREATE TABLE bot_heartbeat (
      id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
      interval TEXT NOT NULL, last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      info JSONB, UNIQUE(symbol, strategy, interval)
    );
  END IF;
  IF to_regclass('public.worker_heartbeats') IS NULL THEN
    CREATE TABLE worker_heartbeats (
      service_name TEXT NOT NULL, environment TEXT NOT NULL DEFAULT 'UNKNOWN',
      status TEXT NOT NULL DEFAULT 'unknown', last_tick TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_ok TIMESTAMPTZ, last_error TEXT, loop_duration_ms INTEGER,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY(service_name, environment)
    );
  END IF;
  IF to_regclass('public.panic_state') IS NULL THEN
    CREATE TABLE panic_state (
      id BOOLEAN PRIMARY KEY DEFAULT true CHECK(id=true),
      panic_enabled BOOLEAN NOT NULL DEFAULT false, reason TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  END IF;
  IF to_regclass('public.user_settings') IS NULL THEN
    CREATE TABLE user_settings (
      id BIGSERIAL PRIMARY KEY, user_id BIGINT,
      min_entry_usdc NUMERIC(18,8) NOT NULL DEFAULT 6,
      manual_entry_addon_usdc NUMERIC(18,8) NOT NULL DEFAULT 0,
      three_win_boost_usdc NUMERIC(18,8) NOT NULL DEFAULT 0,
      mode TEXT NOT NULL DEFAULT 'AUTO', updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  END IF;
  IF to_regclass('public.ui_audit_log') IS NULL THEN
    CREATE TABLE ui_audit_log (
      id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      actor TEXT NOT NULL, actor_role TEXT, action TEXT NOT NULL,
      target_type TEXT NOT NULL, target_key TEXT NOT NULL,
      before_json JSONB, after_json JSONB, source TEXT, note TEXT
    );
  END IF;
  IF to_regclass('public.bot_control') IS NULL THEN
    CREATE TABLE bot_control (
      symbol TEXT NOT NULL, strategy TEXT NOT NULL, interval TEXT NOT NULL,
      enabled BOOLEAN NOT NULL DEFAULT true, mode TEXT NOT NULL DEFAULT 'NORMAL',
      reason TEXT, live_orders_enabled BOOLEAN NOT NULL DEFAULT false,
      regime_enabled BOOLEAN NOT NULL DEFAULT false,
      regime_mode TEXT NOT NULL DEFAULT 'DRY_RUN',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      control_mode TEXT NOT NULL DEFAULT 'AUTO',
      control_source TEXT NOT NULL DEFAULT 'SYSTEM',
      manual_override_reason TEXT, manual_override_updated_at TIMESTAMPTZ,
      PRIMARY KEY(symbol, strategy, interval)
    );
  END IF;
  IF to_regclass('public.market_regime') IS NULL THEN
    CREATE TABLE market_regime (
      symbol TEXT NOT NULL, interval TEXT NOT NULL, ts TIMESTAMPTZ NOT NULL,
      regime TEXT, vol_regime TEXT, trend_dir INTEGER, trend_strength_pct NUMERIC,
      atr_pct NUMERIC, shock_z NUMERIC, ema_fast NUMERIC, ema_slow NUMERIC,
      score_trend NUMERIC, score_vol NUMERIC, score_shock NUMERIC,
      lookback INTEGER, meta JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY(symbol, interval, ts)
    );
  END IF;
  IF to_regclass('public.regime_gate_events') IS NULL THEN
    CREATE TABLE regime_gate_events (
      id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      symbol TEXT NOT NULL, interval TEXT NOT NULL, strategy TEXT NOT NULL,
      decision TEXT NOT NULL, allow BOOLEAN NOT NULL, regime TEXT, mode TEXT,
      would_block BOOLEAN, why TEXT, meta JSONB
    );
  END IF;
  IF to_regclass('public.watchdog_events') IS NULL THEN
    CREATE TABLE watchdog_events (
      id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      symbol TEXT NOT NULL, interval TEXT, strategy TEXT, severity TEXT NOT NULL,
      event TEXT NOT NULL, details JSONB
    );
  END IF;
END $$;

-- Upgrade legacy installations only when a required column is genuinely absent.
DO $$
DECLARE item TEXT[];
BEGIN
  FOREACH item SLICE 1 IN ARRAY ARRAY[
    ARRAY['candles','atr_14','double precision'],
    ARRAY['candles','supertrend','double precision'],
    ARRAY['candles','supertrend_direction','integer'],
    ARRAY['simulated_orders','is_exit','boolean NOT NULL DEFAULT false'],
    ARRAY['positions','entry_order_id','text'],
    ARRAY['positions','exit_order_id','text'],
    ARRAY['positions','entry_client_order_id','text'],
    ARRAY['positions','exit_client_order_id','text'],
    ARRAY['strategy_params','interval','text NOT NULL DEFAULT ''1m'''],
    ARRAY['strategy_params_history','interval','text NOT NULL DEFAULT ''1m'''],
    ARRAY['bot_control','control_mode','text NOT NULL DEFAULT ''AUTO'''],
    ARRAY['bot_control','control_source','text NOT NULL DEFAULT ''SYSTEM'''],
    ARRAY['bot_control','manual_override_reason','text'],
    ARRAY['bot_control','manual_override_updated_at','timestamptz'],
    ARRAY['market_regime','lookback','integer'],
    ARRAY['market_regime','meta','jsonb'],
    ARRAY['market_regime','created_at','timestamptz NOT NULL DEFAULT now()'],
    ARRAY['user_settings','manual_entry_addon_usdc','numeric(18,8) NOT NULL DEFAULT 0'],
    ARRAY['user_settings','three_win_boost_usdc','numeric(18,8) NOT NULL DEFAULT 0']
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name=item[1] AND column_name=item[2]
    ) THEN
      EXECUTE format('ALTER TABLE %I ADD COLUMN %I %s', item[1], item[2], item[3]);
    END IF;
  END LOOP;
END $$;

DO $$ BEGIN
  IF to_regclass('public.ux_sim_orders_one_per_candle_isexit') IS NULL THEN
    CREATE UNIQUE INDEX ux_sim_orders_one_per_candle_isexit
      ON simulated_orders(symbol, interval, strategy, candle_open_time, is_exit);
  END IF;
  IF to_regclass('public.ux_positions_open') IS NULL THEN
    CREATE UNIQUE INDEX ux_positions_open ON positions(symbol, strategy, interval)
      WHERE status='OPEN';
  END IF;
  IF to_regclass('public.ux_strategy_params_sym_strat_int_name') IS NULL THEN
    CREATE UNIQUE INDEX ux_strategy_params_sym_strat_int_name
      ON strategy_params(symbol, strategy, interval, param_name);
  END IF;
  IF to_regclass('public.ix_strategy_params_history_lookup') IS NULL THEN
    CREATE INDEX ix_strategy_params_history_lookup
      ON strategy_params_history(symbol, strategy, interval, param_name, changed_at DESC);
  END IF;
  IF to_regclass('public.uq_user_settings_user_id') IS NULL THEN
    CREATE UNIQUE INDEX uq_user_settings_user_id
      ON user_settings((COALESCE(user_id,-1)));
  END IF;
  IF to_regclass('public.ix_worker_heartbeats_status_updated') IS NULL THEN
    CREATE INDEX ix_worker_heartbeats_status_updated
      ON worker_heartbeats(status, updated_at DESC);
  END IF;
  IF to_regclass('public.ix_worker_heartbeats_last_tick') IS NULL THEN
    CREATE INDEX ix_worker_heartbeats_last_tick ON worker_heartbeats(last_tick DESC);
  END IF;
  IF to_regclass('public.ix_ui_audit_log_created_at') IS NULL THEN
    CREATE INDEX ix_ui_audit_log_created_at ON ui_audit_log(created_at DESC);
  END IF;
  IF to_regclass('public.ix_regime_gate_events_lookup') IS NULL THEN
    CREATE INDEX ix_regime_gate_events_lookup
      ON regime_gate_events(symbol, interval, strategy, created_at DESC);
  END IF;
  IF to_regclass('public.ix_candles_sym_int_open_time_desc') IS NULL THEN
    CREATE INDEX ix_candles_sym_int_open_time_desc
      ON candles(symbol, interval, open_time DESC);
  END IF;
  IF to_regclass('public.ix_sim_orders_sym_int_strat_created_at') IS NULL THEN
    CREATE INDEX ix_sim_orders_sym_int_strat_created_at
      ON simulated_orders(symbol, interval, strategy, created_at);
  END IF;
  IF to_regclass('public.ix_positions_open_lookup') IS NULL THEN
    CREATE INDEX ix_positions_open_lookup
      ON positions(symbol, strategy, interval, entry_time DESC) WHERE status='OPEN';
  END IF;
  IF to_regclass('public.ix_market_regime_latest') IS NULL THEN
    CREATE INDEX ix_market_regime_latest ON market_regime(symbol, interval, ts DESC);
  END IF;
  IF to_regclass('public.watchdog_events_symbol_interval_strategy_created_idx') IS NULL THEN
    CREATE INDEX watchdog_events_symbol_interval_strategy_created_idx
      ON watchdog_events(symbol, interval, strategy, created_at DESC);
  END IF;
END $$;

INSERT INTO panic_state(id,panic_enabled,reason)
VALUES(true,false,'bootstrap') ON CONFLICT(id) DO NOTHING;

INSERT INTO user_settings(
  user_id,min_entry_usdc,manual_entry_addon_usdc,three_win_boost_usdc,mode
)
VALUES(NULL,6,0,0,'AUTO')
ON CONFLICT((COALESCE(user_id,-1))) DO NOTHING;

DO $$ BEGIN
  IF to_regclass('public.automation_kv') IS NOT NULL THEN
    INSERT INTO automation_kv(key,value,updated_at)
    VALUES('strategy_runtime_schema_version','20260712_strategy_runtime_schema_consolidation_v1',now())
    ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at;
  END IF;
END $$;

COMMIT;

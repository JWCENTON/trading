# common/schema.py
import os
import logging
from common.db import get_db_conn

def ensure_schema():
    conn = get_db_conn()
    cur = conn.cursor()

    # Moved from TREND init db ()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            open_time TIMESTAMPTZ NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            close_time TIMESTAMPTZ NOT NULL,
            trades INTEGER,
            ema_21 NUMERIC,
            rsi_14 NUMERIC,
            UNIQUE(symbol, interval, open_time)
        );
        """
    )

    cur.execute("ALTER TABLE candles ADD COLUMN IF NOT EXISTS atr_14 double precision;")
    cur.execute("ALTER TABLE candles ADD COLUMN IF NOT EXISTS supertrend double precision;")
    cur.execute("ALTER TABLE candles ADD COLUMN IF NOT EXISTS supertrend_direction integer;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS simulated_orders (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            strategy TEXT NOT NULL,
            side TEXT NOT NULL,
            price NUMERIC NOT NULL,
            quantity_btc NUMERIC NOT NULL,
            reason TEXT,
            rsi_14 NUMERIC,
            ema_21 NUMERIC,
            candle_open_time TIMESTAMPTZ NOT NULL
        );
        """
    )

    # RSI SSOT: allow 1 ENTRY + 1 EXIT per candle (guarded by is_exit)
    cur.execute("ALTER TABLE simulated_orders ADD COLUMN IF NOT EXISTS is_exit boolean NOT NULL DEFAULT false;")

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_sim_orders_one_per_candle
        ON simulated_orders(symbol, interval, strategy, candle_open_time);
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_sim_orders_one_per_candle_isexit
        ON simulated_orders(symbol, interval, strategy, candle_open_time, is_exit);
        """
    )

    # RSI SSOT execution identifiers (LIVE/PAPER)
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_order_id text;")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_order_id text;")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_client_order_id text;")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_client_order_id text;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_params (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            param_name TEXT NOT NULL,
            param_value NUMERIC NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(symbol, strategy, param_name)
        );
        """
    )

    # RSI runtime params are per (symbol,strategy,interval)
    cur.execute("ALTER TABLE strategy_params ADD COLUMN IF NOT EXISTS interval text NOT NULL DEFAULT '1m';")

    # canonical uniqueness is per (symbol, strategy, interval, param_name);
    # drop legacy constraint if it still exists from older installs
    cur.execute("ALTER TABLE strategy_params DROP CONSTRAINT IF EXISTS strategy_params_symbol_strategy_param_name_key;")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_params_sym_strat_int_name
        ON strategy_params(symbol, strategy, interval, param_name);
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_params_history (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            interval TEXT NOT NULL DEFAULT '1m',
            param_name TEXT NOT NULL,
            old_value NUMERIC,
            new_value NUMERIC NOT NULL,
            changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            source TEXT NOT NULL
        );
        """
    )
    cur.execute("ALTER TABLE strategy_params_history ADD COLUMN IF NOT EXISTS interval text NOT NULL DEFAULT '1m';")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_strategy_params_history_lookup
        ON strategy_params_history(symbol, strategy, interval, param_name, changed_at DESC);
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_heartbeat (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          strategy TEXT NOT NULL,
          interval TEXT NOT NULL,
          last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
          info JSONB,
          UNIQUE(symbol, strategy, interval)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
          id SERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          strategy TEXT NOT NULL,
          interval TEXT NOT NULL,
          status TEXT NOT NULL,
          side TEXT NOT NULL,
          qty NUMERIC NOT NULL,
          entry_price NUMERIC NOT NULL,
          entry_time TIMESTAMPTZ NOT NULL DEFAULT now(),
          exit_price NUMERIC,
          exit_time TIMESTAMPTZ,
          exit_reason TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_positions_open
        ON positions(symbol, strategy, interval)
        WHERE status='OPEN';
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS panic_state (
          id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
          panic_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          reason TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    cur.execute(
        """
        INSERT INTO panic_state (id, panic_enabled, reason)
        VALUES (TRUE, FALSE, 'bootstrap')
        ON CONFLICT (id) DO NOTHING;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
          id BIGSERIAL PRIMARY KEY,
          user_id BIGINT NULL,
          min_entry_usdc NUMERIC(18,8) NOT NULL DEFAULT 6,
          mode TEXT NOT NULL DEFAULT 'AUTO',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_settings_user_id
        ON user_settings ((COALESCE(user_id, -1)));
        """
    )

    cur.execute(
        """
        INSERT INTO user_settings (user_id, min_entry_usdc, mode)
        VALUES (NULL, 6, 'AUTO')
        ON CONFLICT ((COALESCE(user_id, -1))) DO NOTHING;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ui_audit_log (
          id BIGSERIAL PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          actor TEXT NOT NULL,
          actor_role TEXT,
          action TEXT NOT NULL,
          target_type TEXT NOT NULL,
          target_key TEXT NOT NULL,
          before_json JSONB,
          after_json JSONB,
          source TEXT,
          note TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ui_audit_log_created_at
          ON ui_audit_log(created_at DESC);
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_control (
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        interval TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        mode TEXT NOT NULL DEFAULT 'NORMAL',
        reason TEXT,
        live_orders_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        regime_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        regime_mode TEXT NOT NULL DEFAULT 'DRY_RUN',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY(symbol, strategy, interval)
        );
        """
        )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_regime (
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL,
        regime TEXT,
        vol_regime TEXT,
        trend_dir INTEGER,
        trend_strength_pct NUMERIC,
        atr_pct NUMERIC,
        shock_z NUMERIC,
        ema_fast NUMERIC,
        ema_slow NUMERIC,
        score_trend NUMERIC,
        score_vol NUMERIC,
        score_shock NUMERIC,
        PRIMARY KEY(symbol, interval, ts)
        );
        """
        )
    
    cur.execute("ALTER TABLE market_regime ADD COLUMN IF NOT EXISTS lookback integer;")
    cur.execute("ALTER TABLE market_regime ADD COLUMN IF NOT EXISTS meta jsonb;")
    cur.execute("ALTER TABLE market_regime ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_gate_events (
        id SERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        symbol TEXT NOT NULL,
        interval TEXT NOT NULL,
        strategy TEXT NOT NULL,
        decision TEXT NOT NULL,
        allow BOOLEAN NOT NULL,
        regime TEXT,
        mode TEXT,
        would_block BOOLEAN,
        why TEXT,
        meta JSONB
        );
        """
        )
    
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS watchdog_events (
          id bigserial PRIMARY KEY,
          created_at timestamptz NOT NULL DEFAULT now(),
          symbol text NOT NULL,
          interval text,
          strategy text,
          severity text NOT NULL,
          event text NOT NULL,
          details jsonb
        );
        """
    )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS watchdog_events_symbol_interval_strategy_created_idx
          ON watchdog_events(symbol, interval, strategy, created_at DESC);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_regime_gate_events_lookup
        ON regime_gate_events(symbol, interval, strategy, created_at DESC);
        """
        )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candles_sym_int_open_time_desc
        ON candles(symbol, interval, open_time DESC);
        """
        )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_sim_orders_sym_int_strat_created_at
        ON simulated_orders(symbol, interval, strategy, created_at);
        """
        )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_positions_open_lookup
        ON positions(symbol, strategy, interval, entry_time DESC)
        WHERE status='OPEN';
        """
        )
    
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_market_regime_latest
        ON market_regime(symbol, interval, ts DESC);
        """
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("DB schema ensured.")
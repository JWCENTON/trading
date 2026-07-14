from __future__ import annotations


REQUIRED_COLUMNS = {
    "bot_control": {
        "symbol", "strategy", "interval", "enabled", "mode", "reason",
        "live_orders_enabled", "regime_enabled", "regime_mode", "updated_at",
        "control_mode", "control_source", "manual_override_reason",
        "manual_override_updated_at",
    },
    "bot_heartbeat": {"symbol", "strategy", "interval", "last_seen", "info"},
    "binance_orders": {
        "id", "created_at", "symbol", "side", "order_type", "client_order_id",
        "order_id", "status", "raw", "attempt",
        "position_id", "is_exit", "strategy", "interval", "order_purpose",
        "requested_qty", "order_accepted", "reconciliation_status",
        "reconciled_position_id", "reconciled_at", "reconciled_fill_count",
        "reconciled_executed_qty", "exchange_source", "unreconciled_qty",
        "reconciliation_error", "last_reconciliation_action",
    },
    "binance_order_fills": {
        "id", "source", "trade_id", "order_id", "symbol", "side", "role",
        "executed_qty", "avg_price", "quote_notional_usdc",
        "commission_amount", "commission_asset", "commission_usdc",
        "bnbusdc_price", "event_time", "fill_idx", "raw",
    },
    "automation_kv": {"key", "value", "updated_at"},
    "strategy_events": {
        "created_at", "symbol", "interval", "strategy", "event_type",
        "decision", "info",
    },
    "candles": {
        "symbol", "interval", "open_time", "open", "high", "low", "close",
        "volume", "close_time", "trades", "ema_21", "rsi_14", "atr_14",
        "supertrend", "supertrend_direction",
    },
    "market_regime": {
        "symbol", "interval", "ts", "regime", "vol_regime", "trend_dir",
        "trend_strength_pct", "atr_pct", "shock_z", "ema_fast", "ema_slow",
        "score_trend", "score_vol", "score_shock", "lookback", "meta",
        "created_at",
    },
    "panic_state": {"id", "panic_enabled", "reason", "updated_at"},
    "positions": {
        "symbol", "strategy", "interval", "status", "side", "qty",
        "entry_price", "entry_time", "exit_price", "exit_time", "exit_reason",
        "entry_order_id", "exit_order_id", "entry_client_order_id",
        "exit_client_order_id", "id", "fees_usdc", "entry_context_json",
        "entry_hour_utc", "entry_day_utc",
    },
    "regime_gate_events": {
        "created_at", "symbol", "interval", "strategy", "decision", "allow",
        "regime", "mode", "would_block", "why", "meta",
    },
    "simulated_orders": {
        "created_at", "symbol", "interval", "strategy", "side", "price",
        "quantity_btc", "reason", "rsi_14", "ema_21", "candle_open_time",
        "is_exit",
    },
    "strategy_params": {
        "symbol", "strategy", "interval", "param_name", "param_value",
        "updated_at",
    },
    "strategy_params_history": {
        "symbol", "strategy", "interval", "param_name", "old_value",
        "new_value", "changed_at", "source",
    },
    "ui_audit_log": {
        "created_at", "actor", "actor_role", "action", "target_type",
        "target_key", "before_json", "after_json", "source", "note",
    },
    "user_settings": {
        "user_id", "min_entry_usdc", "manual_entry_addon_usdc",
        "three_win_boost_usdc", "mode", "updated_at",
    },
    "watchdog_events": {
        "created_at", "symbol", "interval", "strategy", "severity", "event",
        "details",
    },
    "worker_heartbeats": {
        "service_name", "environment", "status", "last_tick", "last_ok",
        "last_error", "loop_duration_ms", "meta", "updated_at",
    },
}

REQUIRED_INDEXES = {
    "ux_positions_open",
    "ux_sim_orders_one_per_candle_isexit",
    "ux_strategy_params_sym_strat_int_name",
    "uq_user_settings_user_id",
    "ix_worker_heartbeats_status_updated",
    "ix_worker_heartbeats_last_tick",
    "ux_binance_orders_source_symbol_order_id",
    "ux_binance_orders_legacy_null_source_symbol_order_id",
    "ix_binance_orders_pending_entry_reconcile",
    "ix_binance_order_fills_entry_reconcile_lookup",
    "ux_binance_order_fill_trade",
}

PENDING_ENTRY_INDEX_CONTRACT = {
    "ux_positions_open": (
        "create unique index", "(symbol, strategy, \"interval\")", "where",
        "status = 'open'",
    ),
    "ux_binance_orders_source_symbol_order_id": (
        "create unique index", "(exchange_source, symbol, order_id)",
    ),
    "ux_binance_orders_legacy_null_source_symbol_order_id": (
        "create unique index", "(symbol, order_id)", "where",
        "exchange_source is null",
    ),
    "ix_binance_orders_pending_entry_reconcile": (
        "(reconciled_at, created_at, id)", "where", "order_purpose = 'entry'",
    ),
    "ix_binance_order_fills_entry_reconcile_lookup": (
        "(source, symbol, order_id)",
    ),
    "ux_binance_order_fill_trade": (
        "create unique index", "(source, trade_id)",
    ),
}

PENDING_ENTRY_FUNCTION = "mirror_live_order_sent_to_binance_orders"
PENDING_ENTRY_TRIGGER = "trg_mirror_live_orders"
PENDING_ENTRY_SCHEMA_MARKER = "pending_entry_reconciliation_schema_version"
PENDING_ENTRY_REQUIRED_KV = {
    PENDING_ENTRY_SCHEMA_MARKER,
    "pending_entry_reconciliation_enabled",
    "pending_entry_reconciliation_interval_seconds",
    "pending_entry_reconciliation_last_run",
}
PENDING_ENTRY_TABLES = {
    name: REQUIRED_COLUMNS[name]
    for name in (
        "binance_orders", "binance_order_fills", "positions", "automation_kv",
        "strategy_events",
    )
}


def validate_pending_entry_reconciliation_schema(conn) -> None:
    """Validate the complete read/write contract; performs catalog reads only."""
    missing = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            """,
            (list(PENDING_ENTRY_TABLES),),
        )
        actual_columns = {}
        for table_name, column_name in cur.fetchall():
            actual_columns.setdefault(table_name, set()).add(column_name)
        for table_name, required in PENDING_ENTRY_TABLES.items():
            absent = sorted(required - actual_columns.get(table_name, set()))
            if absent:
                missing.append(f"columns:{table_name}:{','.join(absent)}")

        cur.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = ANY(%s)
            """,
            (list(PENDING_ENTRY_INDEX_CONTRACT),),
        )
        index_defs = {str(name): " ".join(str(defn).lower().split()) for name, defn in cur.fetchall()}
        for name, fragments in PENDING_ENTRY_INDEX_CONTRACT.items():
            definition = index_defs.get(name)
            if definition is None:
                missing.append(f"index:{name}")
            elif any(fragment not in definition for fragment in fragments):
                missing.append(f"index_definition:{name}")

        cur.execute(
            """
            SELECT p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = %s
            """,
            (PENDING_ENTRY_FUNCTION,),
        )
        if not cur.fetchall():
            missing.append(f"function:{PENDING_ENTRY_FUNCTION}")

        cur.execute(
            """
            SELECT t.tgname, c.relname, p.proname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_proc p ON p.oid = t.tgfoid
            WHERE NOT t.tgisinternal AND n.nspname = 'public' AND t.tgname = %s
            """,
            (PENDING_ENTRY_TRIGGER,),
        )
        trigger_rows = cur.fetchall()
        if trigger_rows != [
            (PENDING_ENTRY_TRIGGER, "strategy_events", PENDING_ENTRY_FUNCTION)
        ]:
            missing.append(f"trigger:{PENDING_ENTRY_TRIGGER}")

        cur.execute(
            "SELECT key, value FROM automation_kv WHERE key = ANY(%s)",
            (list(PENDING_ENTRY_REQUIRED_KV),),
        )
        kv_rows = {str(key): str(value) for key, value in cur.fetchall()}
        if kv_rows.get(PENDING_ENTRY_SCHEMA_MARKER) != "1":
            missing.append(f"marker:{PENDING_ENTRY_SCHEMA_MARKER}=1")
        absent_kv = sorted(PENDING_ENTRY_REQUIRED_KV - set(kv_rows))
        if absent_kv:
            missing.append(f"automation_kv:{','.join(absent_kv)}")

    if missing:
        raise RuntimeError(
            "pending entry reconciliation schema is not ready: "
            + ", ".join(missing)
        )


def validate_strategy_runtime_schema(conn) -> None:
    """Fail fast when the migrated strategy schema is incomplete; never run DDL."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ANY(%s)
            """,
            (list(REQUIRED_COLUMNS),),
        )
        actual_columns: dict[str, set[str]] = {}
        for table_name, column_name in cur.fetchall():
            actual_columns.setdefault(table_name, set()).add(column_name)

        missing = []
        for table_name, required in REQUIRED_COLUMNS.items():
            absent = sorted(required - actual_columns.get(table_name, set()))
            if absent:
                missing.append(f"{table_name}: {','.join(absent)}")

        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = ANY(%s)
            """,
            (list(REQUIRED_INDEXES),),
        )
        actual_indexes = {row[0] for row in cur.fetchall()}
        absent_indexes = sorted(REQUIRED_INDEXES - actual_indexes)
        if absent_indexes:
            missing.append(f"indexes: {','.join(absent_indexes)}")

    if missing:
        raise RuntimeError(
            "strategy runtime schema is not ready; apply migrations: "
            + "; ".join(missing)
        )

    validate_pending_entry_reconciliation_schema(conn)

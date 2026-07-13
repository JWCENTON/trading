from __future__ import annotations


REQUIRED_COLUMNS = {
    "bot_control": {
        "symbol", "strategy", "interval", "enabled", "mode", "reason",
        "live_orders_enabled", "regime_enabled", "regime_mode", "updated_at",
        "control_mode", "control_source", "manual_override_reason",
        "manual_override_updated_at",
    },
    "bot_heartbeat": {"symbol", "strategy", "interval", "last_seen", "info"},
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
        "exit_client_order_id",
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
}


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

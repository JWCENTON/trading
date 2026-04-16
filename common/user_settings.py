from __future__ import annotations

from typing import Any, Dict, Optional

from common.db import get_db_conn

SYSTEM_MIN_ENTRY_USDC = 6.0
DEFAULT_MANUAL_ENTRY_ADDON_USDC = 0.0
DEFAULT_THREE_WIN_BOOST_USDC = 10.0


def ensure_user_settings_table(cur) -> None:
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
        ALTER TABLE user_settings
        ADD COLUMN IF NOT EXISTS manual_entry_addon_usdc NUMERIC(18,8) NOT NULL DEFAULT 0;
        """
    )
    cur.execute(
        """
        ALTER TABLE user_settings
        ADD COLUMN IF NOT EXISTS three_win_boost_usdc NUMERIC(18,8) NOT NULL DEFAULT 10;
        """
    )
    cur.execute(
        """
        INSERT INTO user_settings (user_id, min_entry_usdc, manual_entry_addon_usdc, three_win_boost_usdc, mode)
        VALUES (NULL, %s, %s, %s, 'AUTO')
        ON CONFLICT ((COALESCE(user_id, -1))) DO NOTHING;
        """,
        (SYSTEM_MIN_ENTRY_USDC, DEFAULT_MANUAL_ENTRY_ADDON_USDC, DEFAULT_THREE_WIN_BOOST_USDC),
    )


def _build_snapshot(row) -> Dict[str, Any]:
    legacy_min_entry = float(row[1]) if row and row[1] is not None else SYSTEM_MIN_ENTRY_USDC
    manual_addon = float(row[2]) if row and row[2] is not None else DEFAULT_MANUAL_ENTRY_ADDON_USDC
    three_win_boost = float(row[3]) if row and row[3] is not None else DEFAULT_THREE_WIN_BOOST_USDC
    mode = str(row[4]).upper() if row and row[4] else "AUTO"

    normal_entry_preview = SYSTEM_MIN_ENTRY_USDC + manual_addon
    boosted_entry_preview = normal_entry_preview + three_win_boost

    return {
        "user_id": row[0] if row else None,
        "configured_min_entry_usdc": legacy_min_entry,
        "system_min_entry_usdc": SYSTEM_MIN_ENTRY_USDC,
        "effective_min_entry_usdc": max(SYSTEM_MIN_ENTRY_USDC, legacy_min_entry),
        "base_runtime_notional_usdc": SYSTEM_MIN_ENTRY_USDC,
        "manual_entry_addon_usdc": manual_addon,
        "three_win_boost_usdc": three_win_boost,
        "normal_entry_preview_usdc": normal_entry_preview,
        "boosted_entry_preview_usdc": boosted_entry_preview,
        "mode": mode,
        "updated_at": row[5] if row else None,
    }


def get_user_settings_snapshot() -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        ensure_user_settings_table(cur)
        cur.execute(
            """
            SELECT user_id, min_entry_usdc, manual_entry_addon_usdc, three_win_boost_usdc, mode, updated_at
            FROM user_settings
            WHERE user_id IS NULL
            LIMIT 1
            """
        )
        row = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return _build_snapshot(row)


def upsert_user_settings(
    *,
    min_entry_usdc: Optional[float] = None,
    manual_entry_addon_usdc: Optional[float] = None,
    three_win_boost_usdc: Optional[float] = None,
    mode: Optional[str] = None,
) -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        ensure_user_settings_table(cur)

        cur.execute(
            """
            SELECT min_entry_usdc, manual_entry_addon_usdc, three_win_boost_usdc, mode
            FROM user_settings
            WHERE user_id IS NULL
            LIMIT 1
            """
        )
        current = cur.fetchone()

        current_min = float(current[0]) if current and current[0] is not None else SYSTEM_MIN_ENTRY_USDC
        current_manual_addon = float(current[1]) if current and current[1] is not None else DEFAULT_MANUAL_ENTRY_ADDON_USDC
        current_three_win_boost = float(current[2]) if current and current[2] is not None else DEFAULT_THREE_WIN_BOOST_USDC
        current_mode = str(current[3]).upper() if current and current[3] else "AUTO"

        legacy_min = current_min if min_entry_usdc is None else max(SYSTEM_MIN_ENTRY_USDC, float(min_entry_usdc))
        new_manual_addon = current_manual_addon if manual_entry_addon_usdc is None else float(manual_entry_addon_usdc)
        new_three_win_boost = current_three_win_boost if three_win_boost_usdc is None else float(three_win_boost_usdc)
        new_mode = current_mode if mode is None else str(mode).upper()

        cur.execute(
            """
            INSERT INTO user_settings (user_id, min_entry_usdc, manual_entry_addon_usdc, three_win_boost_usdc, mode, updated_at)
            VALUES (NULL, %s, %s, %s, %s, now())
            ON CONFLICT ((COALESCE(user_id, -1)))
            DO UPDATE SET
              min_entry_usdc = EXCLUDED.min_entry_usdc,
              manual_entry_addon_usdc = EXCLUDED.manual_entry_addon_usdc,
              three_win_boost_usdc = EXCLUDED.three_win_boost_usdc,
              mode = EXCLUDED.mode,
              updated_at = now()
            RETURNING user_id, min_entry_usdc, manual_entry_addon_usdc, three_win_boost_usdc, mode, updated_at
            """,
            (legacy_min, new_manual_addon, new_three_win_boost, new_mode),
        )
        row = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return _build_snapshot(row)

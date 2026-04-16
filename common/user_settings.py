from __future__ import annotations

from typing import Any, Dict, Optional

from common.db import get_db_conn

SYSTEM_MIN_ENTRY_USDC = 6.0


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
        INSERT INTO user_settings (user_id, min_entry_usdc, mode)
        VALUES (NULL, %s, 'AUTO')
        ON CONFLICT ((COALESCE(user_id, -1))) DO NOTHING;
        """,
        (SYSTEM_MIN_ENTRY_USDC,),
    )


def get_user_settings_snapshot() -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        ensure_user_settings_table(cur)
        cur.execute(
            """
            SELECT user_id, min_entry_usdc, mode, updated_at
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

    configured = float(row[1]) if row and row[1] is not None else SYSTEM_MIN_ENTRY_USDC
    effective = max(SYSTEM_MIN_ENTRY_USDC, configured)
    mode = str(row[2]).upper() if row and row[2] else "AUTO"

    return {
        "user_id": row[0] if row else None,
        "configured_min_entry_usdc": configured,
        "system_min_entry_usdc": SYSTEM_MIN_ENTRY_USDC,
        "effective_min_entry_usdc": effective,
        "mode": mode,
        "updated_at": row[3] if row else None,
    }


def upsert_user_settings(*, min_entry_usdc: Optional[float] = None, mode: Optional[str] = None) -> Dict[str, Any]:
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        ensure_user_settings_table(cur)

        cur.execute(
            """
            SELECT min_entry_usdc, mode
            FROM user_settings
            WHERE user_id IS NULL
            LIMIT 1
            """
        )
        current = cur.fetchone()

        current_min = float(current[0]) if current and current[0] is not None else SYSTEM_MIN_ENTRY_USDC
        current_mode = str(current[1]).upper() if current and current[1] else "AUTO"

        new_min = current_min if min_entry_usdc is None else float(min_entry_usdc)
        new_mode = current_mode if mode is None else str(mode).upper()

        cur.execute(
            """
            INSERT INTO user_settings (user_id, min_entry_usdc, mode, updated_at)
            VALUES (NULL, %s, %s, now())
            ON CONFLICT ((COALESCE(user_id, -1)))
            DO UPDATE SET
              min_entry_usdc = EXCLUDED.min_entry_usdc,
              mode = EXCLUDED.mode,
              updated_at = now()
            RETURNING user_id, min_entry_usdc, mode, updated_at
            """,
            (new_min, new_mode),
        )
        row = cur.fetchone()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    configured = float(row[1]) if row and row[1] is not None else SYSTEM_MIN_ENTRY_USDC
    effective = max(SYSTEM_MIN_ENTRY_USDC, configured)

    return {
        "user_id": row[0] if row else None,
        "configured_min_entry_usdc": configured,
        "system_min_entry_usdc": SYSTEM_MIN_ENTRY_USDC,
        "effective_min_entry_usdc": effective,
        "mode": str(row[2]).upper() if row and row[2] else "AUTO",
        "updated_at": row[3] if row else None,
    }

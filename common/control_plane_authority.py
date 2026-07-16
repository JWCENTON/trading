from __future__ import annotations


CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID = 917263003


def try_acquire_control_plane_apply_lock(cur) -> bool:
    """Acquire the transaction-scoped single-writer lock without waiting."""
    cur.execute(
        "SELECT pg_try_advisory_xact_lock(%s);",
        (CONTROL_PLANE_APPLY_ADVISORY_LOCK_ID,),
    )
    row = cur.fetchone()
    return bool(row and row[0])

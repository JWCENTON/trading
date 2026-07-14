"""Compatibility entry point for legacy callers.

Schema DDL belongs to versioned migrations. Runtime callers only validate that
the migrated strategy schema is ready.
"""

from common.db import get_db_conn
from common.flags import trading_mode
from common.schema_readiness import validate_strategy_runtime_schema


def ensure_schema() -> None:
    """Validate the strategy schema without creating or altering DB objects."""
    mode = trading_mode()
    conn = get_db_conn()
    try:
        validate_strategy_runtime_schema(conn, trading_mode=mode)
    finally:
        conn.close()

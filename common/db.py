# common/db.py
import os
import time
import logging
from contextlib import contextmanager

import psycopg2

def get_db_conn():
    host = os.environ.get("DB_HOST", "db")
    port = int(os.environ.get("DB_PORT", "5432"))
    dbname = os.environ.get("DB_NAME", os.environ.get("POSTGRES_DB", "trading"))
    user = os.environ.get("DB_USER", os.environ.get("POSTGRES_USER", "botuser"))
    password = os.environ.get("DB_PASS", os.environ.get("POSTGRES_PASSWORD", "botpass"))

    # retry/backoff na start DB (PAPER/LIVE)
    max_wait_s = int(os.environ.get("DB_CONNECT_MAX_WAIT_SECONDS", "60"))
    connect_timeout_s = int(os.environ.get("DB_CONNECT_TIMEOUT_SECONDS", "5"))

    delay_s = 1.0
    t0 = time.time()

    while True:
        try:
            return psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=connect_timeout_s,
            )
        except Exception:
            if time.time() - t0 >= max_wait_s:
                raise
            time.sleep(delay_s)
            delay_s = min(delay_s * 1.5, 5.0)


class _ReadOnlyCleanupDiagnostics(RuntimeError):
    def __init__(self, cleanup_errors):
        super().__init__("read-only DB connection cleanup failed")
        self.cleanup_errors = cleanup_errors


def _safe_log_cleanup_error(message, *args):
    try:
        logging.exception(message, *args)
    except BaseException:
        pass


def _cleanup_db_resources(operations):
    errors = []
    for label, operation in operations:
        try:
            operation()
        except BaseException as exc:
            errors.append(exc)
            _safe_log_cleanup_error(
                "DB resource cleanup %s failed", label
            )
    return tuple(errors)


def _cleanup_read_only_conn(conn):
    return _cleanup_db_resources(
        (("rollback", conn.rollback), ("connection close", conn.close))
    )


def _attach_cleanup_errors(primary, cleanup_errors):
    if not cleanup_errors:
        return
    try:
        primary.cleanup_errors = cleanup_errors
        return
    except BaseException:
        _safe_log_cleanup_error(
            "could not attach read-only DB cleanup diagnostics"
        )

    add_note = getattr(primary, "add_note", None)
    if callable(add_note):
        try:
            add_note(
                "read-only DB cleanup errors: "
                + "; ".join(repr(error) for error in cleanup_errors)
            )
            return
        except BaseException:
            _safe_log_cleanup_error(
                "could not add read-only DB cleanup diagnostic note"
            )

    try:
        diagnostics = _ReadOnlyCleanupDiagnostics(cleanup_errors)
        diagnostics.__context__ = primary.__context__
        primary.__context__ = diagnostics
    except BaseException:
        _safe_log_cleanup_error(
            "could not preserve read-only DB cleanup diagnostic context"
        )


@contextmanager
def read_only_db_conn(connection_factory=get_db_conn):
    """Yield a dedicated transaction-read-only connection for materialized SELECTs.

    PostgreSQL rejects DML and DDL on this connection. Callers must fully
    materialize results inside the context; chunksize, generators, and lazy
    cursors must not escape it. The helper never commits and always attempts
    rollback followed by close.
    """
    conn = connection_factory()
    try:
        set_session = getattr(conn, "set_session", None)
        if not callable(set_session):
            raise TypeError(
                "read-only DB connection must support "
                "set_session(readonly=True)"
            )
        set_session(readonly=True)
        yield conn
    except BaseException as primary:
        _attach_cleanup_errors(primary, _cleanup_read_only_conn(conn))
        raise
    else:
        cleanup_errors = _cleanup_read_only_conn(conn)
        if cleanup_errors:
            primary = cleanup_errors[0]
            _attach_cleanup_errors(primary, cleanup_errors)
            raise primary


@contextmanager
def db_write_conn(connection_factory=get_db_conn):
    """Yield one connection/cursor pair and safely clean up its transaction.

    The caller retains the existing transaction boundary and must commit after
    its write batch. On body failure, rollback is attempted before cursor and
    connection close. Cleanup failures never replace a body exception.
    """
    conn = connection_factory()
    cur = None
    try:
        cur = conn.cursor()
        yield conn, cur
    except BaseException as primary:
        operations = [("rollback", conn.rollback)]
        if cur is not None:
            operations.append(("cursor close", cur.close))
        operations.append(("connection close", conn.close))
        _attach_cleanup_errors(primary, _cleanup_db_resources(operations))
        raise
    else:
        cleanup_errors = _cleanup_db_resources(
            (("cursor close", cur.close), ("connection close", conn.close))
        )
        if cleanup_errors:
            primary = cleanup_errors[0]
            _attach_cleanup_errors(primary, cleanup_errors)
            raise primary


def get_latest_regime(symbol: str, interval: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ts, regime, vol_regime, trend_dir, trend_strength_pct, atr_pct, shock_z,
                   ema_fast, ema_slow, score_trend, score_vol, score_shock
            FROM market_regime
            WHERE symbol=%s AND interval=%s
            ORDER BY ts DESC
            LIMIT 1;
            """,
            (symbol, interval),
        )
        row = cur.fetchone()
        if not row:
            return None
        keys = [
            "ts", "regime", "vol_regime", "trend_dir", "trend_strength_pct", "atr_pct", "shock_z",
            "ema_fast", "ema_slow", "score_trend", "score_vol", "score_shock",
        ]
        return dict(zip(keys, row))
    finally:
        cur.close()
        conn.close()

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/20260724_runtime_ddl_schema_parity_v1.sql"
RUNTIME_TABLES = {
    "ui_audit_log", "api_key_safety_confirmations", "api_key_validation_events",
    "user_totp", "user_recovery_codes", "panic_state", "worker_heartbeats",
    "ui_notification_preferences", "ui_notifications",
}


def _prepare_users(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE public.users(id SERIAL PRIMARY KEY)")
    conn.commit()


def _schema_fingerprint(conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT c.relname, a.attnum, a.attname,
                      format_type(a.atttypid,a.atttypmod), a.attnotnull,
                      pg_get_expr(d.adbin,d.adrelid)
                 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                 JOIN pg_attribute a ON a.attrelid=c.oid
                 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
                WHERE n.nspname='public' AND c.relname=ANY(%s)
                  AND a.attnum>0 AND NOT a.attisdropped
                ORDER BY c.relname,a.attnum""",
            (sorted(RUNTIME_TABLES),),
        )
        return hashlib.sha256(repr(cur.fetchall()).encode()).hexdigest()


def _existing_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT tablename FROM pg_tables
                WHERE schemaname='public' AND tablename=ANY(%s) ORDER BY 1""",
            (sorted(RUNTIME_TABLES),),
        )
        return {row[0] for row in cur.fetchall()}


def test_clean_install_replay_and_rows_unchanged(disposable_postgres_v16):
    pg = disposable_postgres_v16
    database = "waltrade_baseline_test_runtime_clean"
    pg.create_database(database)
    conn = pg.connect(database)
    try:
        _prepare_users(conn)
        sql = MIGRATION.read_text()
        with conn.cursor() as cur:
            cur.execute(sql)
        assert _existing_tables(conn) == RUNTIME_TABLES
        first = _schema_fingerprint(conn)
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ui_notifications(
                     event_type,category,severity,title,message,meta)
                   VALUES ('test','CRITICAL','info','t','m','{}') RETURNING id"""
            )
            row_id = cur.fetchone()[0]
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(sql)
        assert _schema_fingerprint(conn) == first
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type,category,severity,title,message FROM ui_notifications WHERE id=%s",
                (row_id,),
            )
            assert cur.fetchone() == ("test", "CRITICAL", "info", "t", "m")
    finally:
        conn.close()


@pytest.mark.parametrize("broken_shape", [
    "id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
    "event_type TEXT NOT NULL, severity TEXT NOT NULL DEFAULT 'info', "
    "title TEXT NOT NULL, message TEXT NOT NULL, source TEXT, read_at TIMESTAMPTZ, "
    "meta JSONB NOT NULL DEFAULT '{}'::jsonb",
    "id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
    "event_type TEXT NOT NULL, category INTEGER NOT NULL DEFAULT 1, "
    "severity TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL, message TEXT NOT NULL, "
    "source TEXT, read_at TIMESTAMPTZ, meta JSONB NOT NULL DEFAULT '{}'::jsonb",
])
def test_bad_existing_category_fails_closed_without_partial_objects(
    disposable_postgres_v16, broken_shape,
):
    pg = disposable_postgres_v16
    token = hashlib.sha256(broken_shape.encode()).hexdigest()[:8]
    database = f"waltrade_baseline_test_runtime_bad_{token}"
    pg.create_database(database)
    conn = pg.connect(database)
    try:
        _prepare_users(conn)
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE public.ui_notifications({broken_shape})")
        conn.commit()
        with pytest.raises(Exception, match="RUNTIME_DDL_SHAPE_MISMATCH"):
            with conn.cursor() as cur:
                cur.execute(MIGRATION.read_text())
        conn.rollback()
        assert _existing_tables(conn) == {"ui_notifications"}
    finally:
        conn.close()


def test_forced_assertion_failure_rolls_back_all_new_objects(disposable_postgres_v16):
    pg = disposable_postgres_v16
    database = "waltrade_baseline_test_runtime_rollback"
    pg.create_database(database)
    conn = pg.connect(database)
    try:
        _prepare_users(conn)
        sql = MIGRATION.read_text().replace(
            "COMMIT;", "DO $$ BEGIN RAISE EXCEPTION 'forced'; END $$;\\nCOMMIT;"
        )
        with pytest.raises(Exception, match="forced"):
            with conn.cursor() as cur:
                cur.execute(sql)
        conn.rollback()
        assert _existing_tables(conn) == set()
    finally:
        conn.close()


@pytest.mark.parametrize("case_name,setup_sql,expected_existing", [
    (
        "wrong_default",
        """CREATE TABLE public.ui_notifications(
             id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             event_type TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'OTHER',
             severity TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL,
             message TEXT NOT NULL, source TEXT, read_at TIMESTAMPTZ,
             meta JSONB NOT NULL DEFAULT '{}'::jsonb)""",
        {"ui_notifications"},
    ),
    (
        "wrong_nullability",
        """CREATE TABLE public.ui_notifications(
             id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             event_type TEXT NOT NULL, category TEXT DEFAULT 'CRITICAL',
             severity TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL,
             message TEXT NOT NULL, source TEXT, read_at TIMESTAMPTZ,
             meta JSONB NOT NULL DEFAULT '{}'::jsonb)""",
        {"ui_notifications"},
    ),
    (
        "missing_fk",
        """CREATE TABLE public.user_totp(
             user_id INTEGER PRIMARY KEY, totp_secret TEXT NOT NULL,
             enabled BOOLEAN NOT NULL DEFAULT FALSE,
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             enabled_at TIMESTAMPTZ, disabled_at TIMESTAMPTZ,
             last_used_at TIMESTAMPTZ)""",
        {"user_totp"},
    ),
    (
        "wrong_index",
        """CREATE TABLE public.ui_notifications(
             id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
             event_type TEXT NOT NULL, category TEXT NOT NULL DEFAULT 'CRITICAL',
             severity TEXT NOT NULL DEFAULT 'info', title TEXT NOT NULL,
             message TEXT NOT NULL, source TEXT, read_at TIMESTAMPTZ,
             meta JSONB NOT NULL DEFAULT '{}'::jsonb);
           CREATE INDEX ix_ui_notifications_created_at
             ON public.ui_notifications(event_type)""",
        {"ui_notifications"},
    ),
])
def test_other_incorrect_existing_shapes_fail_closed(
    disposable_postgres_v16, case_name, setup_sql, expected_existing,
):
    pg = disposable_postgres_v16
    database = f"waltrade_baseline_test_runtime_{case_name}"
    pg.create_database(database)
    conn = pg.connect(database)
    try:
        _prepare_users(conn)
        with conn.cursor() as cur:
            cur.execute(setup_sql)
        conn.commit()
        with pytest.raises(Exception, match="RUNTIME_DDL_SHAPE_MISMATCH"):
            with conn.cursor() as cur:
                cur.execute(MIGRATION.read_text())
        conn.rollback()
        assert _existing_tables(conn) == expected_existing
    finally:
        conn.close()

from __future__ import annotations

import ast
from contextlib import contextmanager
from pathlib import Path
import types

import pytest

from common.totp_schema import (
    TOTP_COLUMNS,
    TotpSchemaReport,
    inspect_totp_schema,
)


ROOT = Path(__file__).resolve().parents[1]
API_SOURCE = (ROOT / "api/main.py").read_text()


class CatalogCursor:
    def __init__(self, *, missing_table=None, wrong_index=False):
        self.missing_table = missing_table
        self.wrong_index = wrong_index
        self.statements: list[str] = []
        self._rows = []

    def execute(self, sql, _params=None):
        normalized = " ".join(str(sql).split())
        self.statements.append(normalized)
        if "FROM information_schema.tables" in normalized:
            self._rows = [
                (table,)
                for table in sorted(TOTP_COLUMNS)
                if table != self.missing_table
            ]
        elif "FROM information_schema.columns" in normalized:
            rows = []
            for table in sorted(TOTP_COLUMNS):
                if table == self.missing_table:
                    continue
                for ordinal, (name, data_type, nullable, default) in enumerate(
                    TOTP_COLUMNS[table], start=1,
                ):
                    defaults = {
                        None: None,
                        "false": "false",
                        "now": "now()",
                        "sequence": (
                            "nextval('user_recovery_codes_id_seq'::regclass)"
                        ),
                    }
                    rows.append(
                        (
                            table, name, data_type,
                            "YES" if nullable else "NO",
                            defaults[default], ordinal,
                        )
                    )
            self._rows = rows
        elif "FROM pg_indexes" in normalized:
            definition = (
                "CREATE INDEX ix_user_recovery_codes_user_active ON "
                "public.user_recovery_codes USING btree (code_hash)"
                if self.wrong_index else
                "CREATE INDEX ix_user_recovery_codes_user_active ON "
                "public.user_recovery_codes USING btree (user_id, used_at)"
            )
            self._rows = [
                ("ix_user_recovery_codes_user_active", definition)
            ]
        elif "FROM pg_constraint" in normalized:
            self._rows = [
                ("user_totp", "f", (
                    "FOREIGN KEY (user_id) REFERENCES users(id) "
                    "ON DELETE CASCADE"
                )),
                ("user_totp", "p", "PRIMARY KEY (user_id)"),
                ("user_recovery_codes", "f", (
                    "FOREIGN KEY (user_id) REFERENCES users(id) "
                    "ON DELETE CASCADE"
                )),
                ("user_recovery_codes", "p", "PRIMARY KEY (id)"),
            ]
        else:  # pragma: no cover - makes any new query explicit in the fixture
            raise AssertionError(f"unexpected verifier query: {normalized}")

    def fetchall(self):
        return list(self._rows)


def _assert_select_only(statements):
    forbidden = {"CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE"}
    assert statements
    for statement in statements:
        first_word = statement.lstrip().split(maxsplit=1)[0].upper()
        assert first_word == "SELECT"
        assert first_word not in forbidden


def _load_api_functions(*names):
    source_tree = ast.parse(API_SOURCE)
    selected = []
    for node in source_tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name in names:
                node.decorator_list = []
                selected.append(node)
    assert {node.name for node in selected} == set(names)
    module = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {
        "TotpSchemaReport": TotpSchemaReport,
        "threading": types.SimpleNamespace(),
    }
    exec(compile(module, str(ROOT / "api/main.py"), "exec"), namespace)
    return namespace


def test_totp_object_matrix_accepts_complete_schema_with_selects_only():
    cur = CatalogCursor()
    report = inspect_totp_schema(cur)

    assert report == TotpSchemaReport(True, (), ())
    assert report.preflight_output() == (
        "TOTP_SCHEMA_READY=YES\n"
        "missing_objects=none\n"
        "mismatched_objects=none"
    )
    _assert_select_only(cur.statements)


def test_totp_object_matrix_reports_exact_missing_and_mismatched_objects():
    missing = CatalogCursor(missing_table="user_recovery_codes")
    missing_report = inspect_totp_schema(missing)
    assert not missing_report.ready
    assert "table:public.user_recovery_codes" in missing_report.missing_objects
    assert "index:public.ix_user_recovery_codes_user_active" not in (
        missing_report.missing_objects
    )
    _assert_select_only(missing.statements)

    mismatched = CatalogCursor(wrong_index=True)
    mismatch_report = inspect_totp_schema(mismatched)
    assert not mismatch_report.ready
    assert mismatch_report.mismatched_objects == (
        "index:public.ix_user_recovery_codes_user_active",
    )
    assert mismatch_report.preflight_output() == (
        "TOTP_SCHEMA_READY=NO\n"
        "missing_objects=none\n"
        "mismatched_objects="
        "index:public.ix_user_recovery_codes_user_active"
    )
    _assert_select_only(mismatched.statements)


def test_api_verifier_wrapper_never_commits():
    namespace = _load_api_functions("verify_totp_schema")
    cur = object()

    class Connection:
        commit_count = 0

        def commit(self):
            self.commit_count += 1

    conn = Connection()

    @contextmanager
    def db_cursor():
        yield conn, cur

    expected = TotpSchemaReport(True, (), ())
    calls = []
    namespace["db_cursor"] = db_cursor
    namespace["require_totp_schema"] = (
        lambda actual_cur: calls.append(actual_cur) or expected
    )

    assert namespace["verify_totp_schema"]() == expected
    assert calls == [cur]
    assert conn.commit_count == 0


def test_startup_complete_schema_has_zero_ddl_and_zero_commit():
    namespace = _load_api_functions("start_ai_tuner")
    events = []

    class Thread:
        def __init__(self, *, target, daemon):
            events.append(("thread", target, daemon))

        def start(self):
            events.append(("thread_started",))

    namespace["threading"] = types.SimpleNamespace(Thread=Thread)
    namespace["logging"] = types.SimpleNamespace(
        info=lambda *_args: events.append(("logged",))
    )
    namespace["ai_auto_tuner_loop"] = lambda: None
    namespace["verify_totp_schema"] = (
        lambda: events.append(("verified",))
    )

    namespace["start_ai_tuner"]()

    assert events[0] == ("verified",)
    assert ("thread_started",) in events


def test_startup_missing_schema_fails_closed_before_thread_start():
    namespace = _load_api_functions("start_ai_tuner")
    events = []

    class Thread:
        def __init__(self, **_kwargs):
            events.append("thread_created")

    namespace["threading"] = types.SimpleNamespace(Thread=Thread)
    namespace["logging"] = types.SimpleNamespace(info=lambda *_args: None)
    namespace["ai_auto_tuner_loop"] = lambda: None
    namespace["verify_totp_schema"] = lambda: (_ for _ in ()).throw(
        RuntimeError(
            "TOTP_SCHEMA_NOT_READY: "
            "missing_objects=table:public.user_totp; mismatched_objects=none"
        )
    )

    with pytest.raises(
        RuntimeError, match=r"missing_objects=table:public\.user_totp",
    ):
        namespace["start_ai_tuner"]()
    assert events == []


def test_import_and_shutdown_have_no_schema_mutation_path():
    import_probe = (
        ROOT / "tests/test_trading_mode_contract.py"
    ).read_text()
    assert 'events.append("db")' in import_probe
    assert "test_real_api_entrypoint_import_uses_canonical_mode" in import_probe
    assert '@app.on_event("shutdown")' not in API_SOURCE
    assert "ensure_totp_schema" not in API_SOURCE

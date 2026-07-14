from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

from common.entry_fill_reconciliation import (
    reconcile_pending_entry_fills,
    run_pending_entry_reconciliation_if_due,
)
from common.runtime import (
    RuntimeConfig,
    TradingModeConfigurationError,
    normalize_trading_mode,
)
from common.schema_readiness import (
    validate_pending_entry_reconciliation_schema,
)


ROOT = Path(__file__).resolve().parents[1]
UNSET = object()
VALID_MODES = (
    ("LIVE", "LIVE"),
    (" live ", "LIVE"),
    ("live", "LIVE"),
    ("PAPER", "PAPER"),
    (" paper ", "PAPER"),
    ("paper", "PAPER"),
)
INVALID_VALUES = (None, "", "   ", "unknown", "testnet")
INVALID_ENV_MODES = (UNSET, "", "   ", "unknown", "testnet")


def _set_mode(monkeypatch, value):
    if value is UNSET:
        monkeypatch.delenv("TRADING_MODE", raising=False)
    else:
        monkeypatch.setenv("TRADING_MODE", value)


class TrapConnection:
    def cursor(self):
        raise AssertionError("cursor called")

    def commit(self):
        raise AssertionError("commit called")

    def rollback(self):
        raise AssertionError("rollback called")


class TrapExchangeClient:
    def __getattr__(self, name):
        raise AssertionError(f"exchange method accessed: {name}")


class EmptyCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def execute(self, _sql, _params=None):
        pass

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class EmptyConnection:
    def cursor(self):
        return EmptyCursor()


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_canonical_helper_accepts_only_normalized_modes(raw, expected):
    assert normalize_trading_mode(raw) == expected


@pytest.mark.parametrize("raw", INVALID_VALUES)
def test_canonical_helper_rejects_missing_empty_and_unknown(raw):
    with pytest.raises(TradingModeConfigurationError):
        normalize_trading_mode(raw)


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_runtime_config_and_public_flags_share_valid_matrix(
    monkeypatch, raw, expected
):
    _set_mode(monkeypatch, raw)
    flags = importlib.import_module("common.flags")

    assert RuntimeConfig.from_env().trading_mode == expected
    assert flags.trading_mode() == expected


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_worker_heartbeat_environment_is_always_canonical_mode(
    monkeypatch, raw, expected
):
    _set_mode(monkeypatch, raw)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    heartbeat = importlib.import_module("common.worker_heartbeat")

    assert heartbeat.current_environment() == expected

    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("APP_ENV", "development")
    assert heartbeat.current_environment() == expected


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_runtime_config_and_public_flags_fail_closed(monkeypatch, raw):
    _set_mode(monkeypatch, raw)
    flags = importlib.import_module("common.flags")

    with pytest.raises(TradingModeConfigurationError):
        RuntimeConfig.from_env()
    with pytest.raises(TradingModeConfigurationError):
        flags.trading_mode()


def test_common_flags_is_lazy_and_does_not_cache_environment(monkeypatch):
    monkeypatch.delenv("TRADING_MODE", raising=False)
    sys.modules.pop("common.flags", None)
    flags = importlib.import_module("common.flags")

    with pytest.raises(TradingModeConfigurationError):
        flags.trading_mode()
    monkeypatch.setenv("TRADING_MODE", " paper ")
    assert flags.trading_mode() == "PAPER"
    monkeypatch.setenv("TRADING_MODE", " live ")
    assert flags.trading_mode() == "LIVE"
    monkeypatch.delenv("TRADING_MODE")
    with pytest.raises(TradingModeConfigurationError):
        flags.trading_mode()


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_worker_heartbeat_rejects_invalid_mode_even_with_environment_label(
    monkeypatch, raw
):
    _set_mode(monkeypatch, raw)
    monkeypatch.setenv("ENVIRONMENT", "local")
    heartbeat = importlib.import_module("common.worker_heartbeat")

    with pytest.raises(TradingModeConfigurationError):
        heartbeat.current_environment()


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_worker_heartbeat_writes_canonical_mode_before_db_work(
    monkeypatch, raw, expected
):
    _set_mode(monkeypatch, raw)
    monkeypatch.setenv("ENVIRONMENT", "LOCAL")
    monkeypatch.setenv("APP_ENV", "development")
    heartbeat = importlib.import_module("common.worker_heartbeat")
    operations = []

    class Cursor:
        def execute(self, sql, params=None):
            operations.append(("execute", sql, params))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        closed = False
        autocommit = True

        def cursor(self):
            operations.append(("cursor",))
            return Cursor()

        def commit(self):
            operations.append(("commit",))

        def rollback(self):
            operations.append(("rollback",))

        def close(self):
            operations.append(("close",))
            self.closed = True

    monkeypatch.setattr(
        heartbeat,
        "get_db_conn",
        lambda: operations.append(("connect",)) or Connection(),
    )

    heartbeat.record_worker_heartbeat("contract-test")

    insert = next(op for op in operations if op[0] == "execute" and op[2])
    assert insert[2][1] == expected
    assert json.loads(insert[2][6])["deployment"] == "LOCAL"
    assert ("commit",) in operations
    assert not any(op[0] == "rollback" for op in operations)


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_worker_heartbeat_invalid_mode_has_zero_db_side_effects(monkeypatch, raw):
    _set_mode(monkeypatch, raw)
    heartbeat = importlib.import_module("common.worker_heartbeat")
    monkeypatch.setattr(
        heartbeat,
        "get_db_conn",
        lambda: pytest.fail("invalid heartbeat mode opened DB"),
    )

    with pytest.raises(TradingModeConfigurationError):
        heartbeat.record_worker_heartbeat("contract-test")


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_readiness_and_reconciliation_entry_points_share_valid_matrix(
    raw, expected
):
    if expected == "PAPER":
        readiness = validate_pending_entry_reconciliation_schema(
            TrapConnection(), trading_mode=raw
        )
        direct = reconcile_pending_entry_fills(
            TrapConnection(), trading_mode=raw
        )
        due = run_pending_entry_reconciliation_if_due(
            TrapConnection(), trading_mode=raw
        )
        assert readiness.status == "NOT_APPLICABLE"
        assert direct.status == "NOT_APPLICABLE"
        assert due.status == "NOT_APPLICABLE"
        return

    with pytest.raises(AssertionError, match="cursor called"):
        validate_pending_entry_reconciliation_schema(
            TrapConnection(), trading_mode=raw
        )
    direct = reconcile_pending_entry_fills(
        EmptyConnection(), trading_mode=raw
    )
    due = run_pending_entry_reconciliation_if_due(
        EmptyConnection(), trading_mode=raw
    )
    assert direct.status == "OK"
    assert direct.ran is True
    assert due.status == "SCHEMA_NOT_READY"


@pytest.mark.parametrize("raw", INVALID_VALUES)
def test_readiness_and_reconciliation_reject_invalid_before_query(raw):
    for call in (
        lambda: validate_pending_entry_reconciliation_schema(
            TrapConnection(), trading_mode=raw
        ),
        lambda: reconcile_pending_entry_fills(
            TrapConnection(), trading_mode=raw
        ),
        lambda: run_pending_entry_reconciliation_if_due(
            TrapConnection(), trading_mode=raw
        ),
    ):
        with pytest.raises(TradingModeConfigurationError):
            call()


def test_readiness_and_reconciliation_have_no_implicit_mode():
    for call in (
        lambda: validate_pending_entry_reconciliation_schema(TrapConnection()),
        lambda: reconcile_pending_entry_fills(TrapConnection()),
        lambda: run_pending_entry_reconciliation_if_due(TrapConnection()),
    ):
        with pytest.raises(TradingModeConfigurationError):
            call()


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_fill_ingest_shares_valid_matrix(monkeypatch, raw, expected):
    ingest = importlib.import_module("common.exchange_ingest_trades")
    _set_mode(monkeypatch, raw)

    class LivePipelineReached(Exception):
        pass

    monkeypatch.setattr(
        ingest.psycopg2,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            LivePipelineReached
        ),
    )
    kwargs = dict(
        client=TrapExchangeClient(),
        symbols=["BTCUSDC"],
        db_host="local",
        db_port=5432,
        db_name="test",
        db_user="test",
        db_pass="test",
    )
    if expected == "LIVE":
        with pytest.raises(LivePipelineReached):
            ingest.ingest_my_trades(**kwargs)
    else:
        result = ingest.ingest_my_trades(**kwargs)
        assert result.status == "NOT_APPLICABLE"
        assert result.applicable is False


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_fill_ingest_rejects_invalid_before_db_or_exchange(monkeypatch, raw):
    ingest = importlib.import_module("common.exchange_ingest_trades")
    _set_mode(monkeypatch, raw)
    monkeypatch.setattr(
        ingest.psycopg2,
        "connect",
        lambda *_args, **_kwargs: pytest.fail("invalid mode opened DB"),
    )

    with pytest.raises(TradingModeConfigurationError):
        ingest.ingest_my_trades(
            client=TrapExchangeClient(),
            symbols=["BTCUSDC"],
            db_host="local",
            db_port=5432,
            db_name="test",
            db_user="test",
            db_pass="test",
        )


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_bot_runner_normalizes_before_children(monkeypatch, raw, expected):
    _set_mode(monkeypatch, raw)
    module = _load_module(
        ROOT / "services/bot_runner/main.py",
        f"waltrade_bot_runner_mode_{expected}_{abs(hash(raw))}",
    )
    assert module.TRADING_MODE == expected


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_bot_runner_rejects_invalid_before_spawn(monkeypatch, raw):
    _set_mode(monkeypatch, raw)
    subprocess = importlib.import_module("subprocess")
    psycopg2 = importlib.import_module("psycopg2")
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_a, **_k: pytest.fail("invalid bot runner mode spawned child"),
    )
    monkeypatch.setattr(
        psycopg2,
        "connect",
        lambda *_a, **_k: pytest.fail("invalid bot runner mode opened DB"),
    )
    with pytest.raises(TradingModeConfigurationError):
        _load_module(
            ROOT / "services/bot_runner/main.py",
            f"waltrade_bot_runner_invalid_{abs(hash(str(raw)))}",
        )


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_automation_configuration_uses_canonical_mode(
    monkeypatch, raw, expected
):
    _set_mode(monkeypatch, raw)
    exchange_client = importlib.import_module("common.exchange_client")
    monkeypatch.setattr(exchange_client, "get_market_data_client", object)
    module = _load_module(
        ROOT / "automation_runner/main.py",
        f"waltrade_automation_mode_{expected}_{abs(hash(raw))}",
    )
    assert module.cfg.trading_mode == expected


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_automation_rejects_invalid_before_client_factory(monkeypatch, raw):
    _set_mode(monkeypatch, raw)
    exchange_client = importlib.import_module("common.exchange_client")
    monkeypatch.setattr(
        exchange_client,
        "get_market_data_client",
        lambda: pytest.fail("invalid automation mode reached client factory"),
    )
    with pytest.raises(TradingModeConfigurationError):
        _load_module(
            ROOT / "automation_runner/main.py",
            f"waltrade_automation_invalid_{abs(hash(str(raw)))}",
        )


API_IMPORT_PROBE = textwrap.dedent(
    r'''
    import importlib
    import json
    import sys
    import types

    events = []

    class FastAPI:
        def __init__(self, *_args, **_kwargs):
            events.append("fastapi")
        def add_middleware(self, *_args, **_kwargs):
            events.append("middleware")
        def _decorator(self, *_args, **_kwargs):
            return lambda func: func
        get = post = put = delete = patch = on_event = middleware = _decorator

    class HTTPException(Exception):
        def __init__(self, *args, **kwargs):
            super().__init__(*args)

    def parameter(default=None, *_args, **_kwargs):
        return default

    fastapi = types.ModuleType("fastapi")
    fastapi.FastAPI = FastAPI
    fastapi.HTTPException = HTTPException
    fastapi.Query = parameter
    fastapi.Depends = parameter
    fastapi.Cookie = parameter
    fastapi.Header = parameter
    fastapi.Request = type("Request", (), {})
    fastapi.Response = type("Response", (), {})
    fastapi.status = types.SimpleNamespace(
        HTTP_401_UNAUTHORIZED=401,
        HTTP_403_FORBIDDEN=403,
        HTTP_429_TOO_MANY_REQUESTS=429,
    )
    sys.modules["fastapi"] = fastapi

    encoders = types.ModuleType("fastapi.encoders")
    encoders.jsonable_encoder = lambda value: value
    sys.modules["fastapi.encoders"] = encoders
    middleware = types.ModuleType("fastapi.middleware")
    cors = types.ModuleType("fastapi.middleware.cors")
    cors.CORSMiddleware = type("CORSMiddleware", (), {})
    sys.modules["fastapi.middleware"] = middleware
    sys.modules["fastapi.middleware.cors"] = cors

    pydantic = types.ModuleType("pydantic")
    pydantic.BaseModel = type("BaseModel", (), {})
    sys.modules["pydantic"] = pydantic

    class OpenAI:
        def __init__(self, *_args, **_kwargs):
            events.append("openai")
    openai = types.ModuleType("openai")
    openai.OpenAI = OpenAI
    sys.modules["openai"] = openai

    pyotp = types.ModuleType("pyotp")
    pyotp.TOTP = type("TOTP", (), {})
    pyotp.random_base32 = lambda: "stub"
    sys.modules["pyotp"] = pyotp

    import psycopg2
    psycopg2.connect = lambda *_a, **_k: events.append("db")
    import common.exchange_client as exchange_client
    exchange_client.get_market_data_client = (
        lambda: events.append("market_data") or object()
    )

    try:
        module = importlib.import_module("api.main")
    except Exception as exc:
        print(json.dumps({
            "ok": False,
            "error_type": type(exc).__name__,
            "events": events,
        }))
    else:
        print(json.dumps({
            "ok": True,
            "mode": module.TRADING_MODE,
            "events": events,
        }))
    '''
)


def _probe_real_api_import(raw):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["OPENAI_API_KEY"] = "trap-openai-key"
    env.pop("ALLOWED_ORIGINS", None)
    if raw is UNSET:
        env.pop("TRADING_MODE", None)
    else:
        env["TRADING_MODE"] = raw
    completed = subprocess.run(
        [sys.executable, "-c", API_IMPORT_PROBE],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_real_api_entrypoint_import_uses_canonical_mode(raw, expected):
    result = _probe_real_api_import(raw)
    assert result["ok"] is True
    assert result["mode"] == expected
    assert result["events"] == ["openai", "market_data", "fastapi"]


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_real_api_entrypoint_rejects_invalid_before_all_clients(raw):
    result = _probe_real_api_import(raw)
    assert result == {
        "ok": False,
        "error_type": "TradingModeConfigurationError",
        "events": [],
    }


@pytest.mark.parametrize(("raw", "expected"), VALID_MODES)
def test_orchestrator_entrypoint_uses_canonical_mode(monkeypatch, raw, expected):
    _set_mode(monkeypatch, raw)
    module = _load_module(
        ROOT / "services/bot_runner_orchestrator/main.py",
        f"waltrade_orchestrator_mode_{expected}_{abs(hash(raw))}",
    )
    assert module.TRADING_MODE == expected


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_orchestrator_rejects_invalid_before_runtime_side_effects(monkeypatch, raw):
    _set_mode(monkeypatch, raw)
    db = importlib.import_module("common.db")
    heartbeat = importlib.import_module("common.worker_heartbeat")
    monkeypatch.setattr(
        db, "get_db_conn", lambda: pytest.fail("invalid orchestrator mode opened DB")
    )
    monkeypatch.setattr(
        heartbeat,
        "record_worker_heartbeat",
        lambda *_a, **_k: pytest.fail("invalid orchestrator mode emitted heartbeat"),
    )
    monkeypatch.setattr(
        importlib.import_module("psycopg2"),
        "connect",
        lambda *_a, **_k: pytest.fail("invalid orchestrator mode connected DB"),
    )
    logging = importlib.import_module("logging")
    monkeypatch.setattr(
        logging,
        "basicConfig",
        lambda *_a, **_k: pytest.fail("invalid orchestrator configured logging"),
    )

    with pytest.raises(TradingModeConfigurationError):
        _load_module(
            ROOT / "services/bot_runner_orchestrator/main.py",
            f"waltrade_orchestrator_invalid_{abs(hash(str(raw)))}",
        )


@pytest.mark.parametrize("raw", INVALID_ENV_MODES)
def test_legacy_schema_entry_rejects_invalid_before_db_connect(
    monkeypatch, raw
):
    schema = importlib.import_module("common.schema")
    _set_mode(monkeypatch, raw)
    monkeypatch.setattr(
        schema,
        "get_db_conn",
        lambda: pytest.fail("invalid mode opened schema DB connection"),
    )

    with pytest.raises(TradingModeConfigurationError):
        schema.ensure_schema()

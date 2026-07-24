import ast
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "automation_runner/main.py"


def _load_functions():
    tree = ast.parse(RUNNER.read_text())
    names = {
        "get_kv",
        "set_kv",
        "_sha256_canon",
        "_resolve_promotions_api_base",
        "publish_promotions",
    }
    nodes = [
        item
        for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name in names
    ]
    namespace = {
        "datetime": datetime,
        "hashlib": hashlib,
        "json": json,
        "logging": logging,
        "os": os,
        "requests": None,
        "time": time,
        "timezone": timezone,
        "urlparse": urlparse,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(RUNNER), "exec"), namespace)
    return namespace


def _automation_environment(override_name):
    data = yaml.safe_load((ROOT / override_name).read_text())
    return data["services"]["automation-runner"]["environment"]


def test_environment_scoped_routing_contract():
    resolve = _load_functions()["_resolve_promotions_api_base"]
    assert resolve({
        "DEPLOYMENT_ID": "local-paper",
        "INTERNAL_API_BASE": "http://paper-api:8000",
    }) == "http://paper-api:8000"
    assert resolve({
        "DEPLOYMENT_ID": "vps-paper",
        "INTERNAL_API_BASE": "http://paper-api:8000",
    }) == "http://paper-api:8000"
    assert resolve({
        "DEPLOYMENT_ID": "local-live",
        "INTERNAL_API_BASE": "http://live-api:8000",
    }) == "http://live-api:8000"
    assert resolve({
        "DEPLOYMENT_ID": "vps-live",
        "INTERNAL_API_BASE": "http://live-api:8000",
    }) == "http://live-api:8000"


def test_missing_or_cross_environment_endpoint_is_fail_closed():
    resolve = _load_functions()["_resolve_promotions_api_base"]
    assert resolve({"DEPLOYMENT_ID": "local-paper"}) is None
    assert resolve({
        "DEPLOYMENT_ID": "local-paper",
        "INTERNAL_API_BASE": "http://live-api:8000",
    }) is None
    assert resolve({
        "DEPLOYMENT_ID": "vps-paper",
        "INTERNAL_API_BASE": "http://live-api:8000",
    }) is None
    assert resolve({
        "DEPLOYMENT_ID": "local-live",
        "INTERNAL_API_BASE": "http://paper-api:8000",
    }) is None


def test_compose_wires_each_automation_runner_to_own_api():
    live = _automation_environment("docker-compose.live.override.yaml")
    paper = _automation_environment("docker-compose.paper.override.yaml")
    assert live["INTERNAL_API_BASE"] == "http://live-api:8000"
    assert paper["INTERNAL_API_BASE"] == "http://paper-api:8000"
    assert "live-api" not in paper["INTERNAL_API_BASE"]


def test_both_promotion_paths_use_one_validated_base_without_legacy_fallback():
    source = RUNNER.read_text()
    assert 'url = promotions_api_base + "/internal/promotions/upsert"' in source
    assert (
        'f"{promotions_api_base}/internal/regime-promotions/upsert"'
        in source
    )
    assert 'os.getenv("LIVE_API_BASE"' not in source
    assert 'os.environ.get("PROMOTIONS_API_BASE"' not in source
    assert '.replace("paper-api", "live-api")' not in source
    assert "requests.post(url, json=payload, headers=headers, timeout=10)" in source


class _Cursor:
    def __init__(self):
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _params=None):
        self.query = query

    def fetchone(self):
        return None

    def fetchall(self):
        if "FROM v_ranking_v1" in self.query:
            return [("BTCUSDC", "1m", "RSI", 1.25, 60, 0.6, 2.5, 1.2)]
        if "FROM v_slot_profile_v1_14d" in self.query:
            return []
        raise AssertionError(self.query)


class _Connection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _Cursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _Response:
    status_code = 200
    text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return {"inserted": 1}


def test_publish_transport_targets_own_environment_without_payload_change(monkeypatch):
    for deployment_id, host in (
        ("local-paper", "paper-api"),
        ("local-live", "live-api"),
    ):
        functions = _load_functions()
        calls = []

        class _Requests:
            @staticmethod
            def post(url, json, headers, timeout):
                calls.append((url, json, headers, timeout))
                return _Response()

        functions["requests"] = _Requests
        monkeypatch.setenv("DEPLOYMENT_ID", deployment_id)
        monkeypatch.setenv("INTERNAL_API_BASE", f"http://{host}:8000")
        monkeypatch.setenv("PROMOTIONS_ENABLED", "1")
        monkeypatch.setenv("PROMOTIONS_INTERVAL_SECONDS", "300")
        monkeypatch.setenv("PROMOTIONS_MIN_TRADES", "5")
        connection = _Connection()

        assert functions["publish_promotions"](connection) is True
        assert len(calls) == 1
        url, payload, headers, timeout = calls[0]
        assert url == f"http://{host}:8000/internal/promotions/upsert"
        assert payload["rows"][0]["symbol"] == "BTCUSDC"
        assert payload["rows"][0]["eligible_live"] is True
        assert payload["hash"] == functions["_sha256_canon"]({
            key: value for key, value in payload.items() if key != "hash"
        })
        assert headers["Content-Type"] == "application/json"
        assert timeout == 10
        assert connection.commits == 1
        assert connection.rollbacks == 0

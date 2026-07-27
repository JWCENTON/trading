from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass

import psycopg2
import pytest


PRODUCTION_MARKERS = (
    "trading-live", "trading-paper", "trading_live", "trading_paper",
)


def _docker(*args: str) -> str:
    command = ["docker", *args]
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(
            f"docker command failed ({' '.join(command[:3])}): {result.stderr.strip()}"
        )
    return result.stdout.strip()


@dataclass
class DisposablePostgres:
    container: str
    volume: str
    network: str
    database: str
    user: str
    password: str
    port: int

    @property
    def dsn(self) -> str:
        return (
            f"host=127.0.0.1 port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def connect(self, database: str | None = None):
        target = database or self.database
        assert not any(marker in target.lower() for marker in PRODUCTION_MARKERS)
        return psycopg2.connect(
            host="127.0.0.1", port=self.port, dbname=target,
            user=self.user, password=self.password, connect_timeout=5,
        )

    def create_database(self, name: str) -> None:
        assert name.startswith("waltrade_baseline_test_")
        assert not any(marker in name.lower() for marker in PRODUCTION_MARKERS)
        conn = self.connect()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{name}"')
        finally:
            conn.close()


@pytest.fixture(scope="session")
def disposable_postgres_v16():
    if os.getenv("WALTRADE_RUN_DISPOSABLE_PG") != "1":
        pytest.skip("set WALTRADE_RUN_DISPOSABLE_PG=1 for isolated Docker PostgreSQL tests")
    token = os.getenv("WALTRADE_DISPOSABLE_TOKEN", uuid.uuid4().hex[:12])
    if len(token) != 12 or any(ch not in "0123456789abcdef" for ch in token):
        raise RuntimeError("WALTRADE_DISPOSABLE_TOKEN must be 12 lowercase hex characters")
    prefix = f"waltrade-baseline-v1-test-{token}"
    container = f"{prefix}-pg"
    volume = f"{prefix}-data"
    network = f"{prefix}-net"
    database = "waltrade_baseline_test_live"
    user = "baseline_test"
    password = f"test-only-{token}"
    for value in (container, volume, network, database):
        assert not any(marker in value.lower() for marker in PRODUCTION_MARKERS)
    created: list[tuple[str, str]] = []
    fixture = None
    try:
        _docker("network", "create", network)
        created.append(("network", network))
        _docker("volume", "create", volume)
        created.append(("volume", volume))
        _docker(
            "run", "-d", "--name", container, "--network", network,
            "--label", "waltrade.disposable=database-baseline-v1",
            "-e", f"POSTGRES_DB={database}", "-e", f"POSTGRES_USER={user}",
            "-e", f"POSTGRES_PASSWORD={password}",
            "-p", "127.0.0.1::5432",
            "-v", f"{volume}:/var/lib/postgresql/data",
            "postgres:16",
        )
        created.append(("container", container))
        mapping = json.loads(_docker("inspect", container))[0]["NetworkSettings"]["Ports"]
        port = int(mapping["5432/tcp"][0]["HostPort"])
        fixture = DisposablePostgres(
            container, volume, network, database, user, password, port,
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                conn = fixture.connect()
                conn.close()
                break
            except psycopg2.Error:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        yield fixture
    finally:
        errors = []
        if ("container", container) in created:
            try:
                _docker("rm", "-f", container)
            except Exception as exc:
                errors.append(str(exc))
        if ("volume", volume) in created:
            try:
                _docker("volume", "rm", volume)
            except Exception as exc:
                errors.append(str(exc))
        if ("network", network) in created:
            try:
                _docker("network", "rm", network)
            except Exception as exc:
                errors.append(str(exc))
        if errors:
            raise RuntimeError(
                "disposable cleanup failed; inspect only resources with prefix "
                f"{prefix}: {errors}"
            )

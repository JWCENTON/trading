from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass, field

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn


PRODUCTION_MARKERS = (
    "trading-live", "trading-paper", "trading_live", "trading_paper",
)
EXTERNAL_DSN_ENV = "WALTRADE_DISPOSABLE_PG_DSN"
DISPOSABLE_DATABASE_PREFIX = "waltrade_baseline_test_"
DISPOSABLE_MARKER_KEY = "waltrade_disposable_test_db"
KNOWN_NON_TEST_DATABASES = {
    "postgres", "template0", "template1", "trading", "trading_live",
    "trading_paper",
}


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
    host: str = "127.0.0.1"
    external_dsn: str | None = None
    run_token: str = ""
    _children: dict[str, str] = field(default_factory=dict, repr=False)
    _child_counter: int = field(default=0, repr=False)

    @property
    def dsn(self) -> str:
        if self.external_dsn:
            return make_dsn(self.external_dsn, dbname=self.database)
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def connect(self, database: str | None = None):
        requested = database or self.database
        target = self._children.get(requested, requested)
        assert not any(marker in target.lower() for marker in PRODUCTION_MARKERS)
        if self.external_dsn:
            if any(marker in target.lower() for marker in PRODUCTION_MARKERS):
                raise RuntimeError(
                    f"refusing production-marked PostgreSQL database: {target}"
                )
            if target != self.database and target not in self._children.values():
                raise RuntimeError(
                    "external disposable PostgreSQL may connect only to its root "
                    "or a child created by this fixture"
                )
            return psycopg2.connect(
                make_dsn(self.external_dsn, dbname=target), connect_timeout=5,
            )
        return psycopg2.connect(
            host=self.host, port=self.port, dbname=target,
            user=self.user, password=self.password, connect_timeout=5,
        )

    def create_database(self, name: str) -> None:
        target = name
        if self.external_dsn:
            normalized = name.lower()
            suffix_part = normalized[len(DISPOSABLE_DATABASE_PREFIX):]
            if (
                not normalized.startswith(DISPOSABLE_DATABASE_PREFIX)
                or not suffix_part
                or any(
                    ch not in "abcdefghijklmnopqrstuvwxyz0123456789_"
                    for ch in suffix_part
                )
            ):
                raise RuntimeError(
                    "external child database must match waltrade_baseline_test_*"
                )
            if any(marker in normalized for marker in PRODUCTION_MARKERS):
                raise RuntimeError(
                    f"refusing production-marked PostgreSQL database: {name}"
                )
            if name == self.database:
                raise RuntimeError("external disposable root database cannot be a child")
            if name in self._children:
                raise RuntimeError(f"logical child database already exists: {name}")
            self._child_counter += 1
            suffix = f"_{self.run_token}_{self._child_counter:x}"
            target = f"{name[:63 - len(suffix)]}{suffix}"
        else:
            assert name.startswith(DISPOSABLE_DATABASE_PREFIX)
            assert not any(marker in name.lower() for marker in PRODUCTION_MARKERS)
        conn = self.connect()
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target)))
        finally:
            conn.close()
        if self.external_dsn:
            self._children[name] = target

    def cleanup(self) -> None:
        """Drop only external child databases owned by this fixture instance."""
        if not self.external_dsn:
            return
        errors = []
        for logical_name, target in reversed(tuple(self._children.items())):
            if target == self.database or target not in self._children.values():
                errors.append(f"refusing unsafe child cleanup: {target}")
                continue
            try:
                conn = self.connect()
                conn.autocommit = True
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                                sql.Identifier(target)
                            )
                        )
                finally:
                    conn.close()
            except Exception as exc:
                errors.append(f"{logical_name}: {exc}")
            else:
                del self._children[logical_name]
        if errors:
            raise RuntimeError(f"external disposable child cleanup failed: {errors}")


def _validate_external_database_name(database: str) -> None:
    normalized = database.strip().lower()
    if normalized in KNOWN_NON_TEST_DATABASES:
        raise RuntimeError(f"refusing known non-test PostgreSQL database: {database}")
    if any(marker in normalized for marker in PRODUCTION_MARKERS):
        raise RuntimeError(f"refusing production-marked PostgreSQL database: {database}")
    if not normalized.startswith(DISPOSABLE_DATABASE_PREFIX):
        raise RuntimeError(
            "external disposable PostgreSQL database must match "
            "waltrade_baseline_test_*"
        )
    suffix = normalized[len(DISPOSABLE_DATABASE_PREFIX):]
    if not suffix or any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789_" for ch in suffix):
        raise RuntimeError(
            "external disposable PostgreSQL database must match "
            "waltrade_baseline_test_*"
        )


def _external_disposable_postgres(dsn: str, token: str) -> DisposablePostgres:
    try:
        parameters = parse_dsn(dsn)
    except psycopg2.Error as exc:
        raise RuntimeError(f"invalid {EXTERNAL_DSN_ENV}") from exc
    database = str(parameters.get("dbname") or "")
    _validate_external_database_name(database)
    fixture = DisposablePostgres(
        container="", volume="", network="", database=database,
        user=str(parameters.get("user") or ""),
        password=str(parameters.get("password") or ""),
        port=int(parameters.get("port") or 5432),
        host=str(parameters.get("host") or ""), external_dsn=dsn,
        run_token=f"{token}_{uuid.uuid4().hex[:8]}",
    )
    try:
        conn = fixture.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                current_database = str(cur.fetchone()[0])
                if current_database != database:
                    raise RuntimeError("unexpected external disposable database")
                cur.execute("SELECT to_regclass('public.automation_kv')")
                if cur.fetchone()[0] is None:
                    raise RuntimeError(
                        f"{DISPOSABLE_MARKER_KEY}=true marker is required"
                    )
                cur.execute(
                    "SELECT value FROM public.automation_kv WHERE key=%s",
                    (DISPOSABLE_MARKER_KEY,),
                )
                marker = cur.fetchone()
                if marker is None:
                    raise RuntimeError(
                        f"{DISPOSABLE_MARKER_KEY}=true marker is required"
                    )
                if str(marker[0]).strip().lower() != "true":
                    raise RuntimeError(
                        f"{DISPOSABLE_MARKER_KEY} marker must be true"
                    )
        finally:
            conn.close()
    except psycopg2.Error as exc:
        raise RuntimeError("external disposable PostgreSQL validation failed") from exc
    return fixture


@pytest.fixture(scope="session")
def disposable_postgres_v16():
    external_dsn = os.getenv(EXTERNAL_DSN_ENV, "").strip()
    if not external_dsn and os.getenv("WALTRADE_RUN_DISPOSABLE_PG") != "1":
        pytest.skip("set WALTRADE_RUN_DISPOSABLE_PG=1 for isolated Docker PostgreSQL tests")
    token = os.getenv("WALTRADE_DISPOSABLE_TOKEN", uuid.uuid4().hex[:12])
    if len(token) != 12 or any(ch not in "0123456789abcdef" for ch in token):
        raise RuntimeError("WALTRADE_DISPOSABLE_TOKEN must be 12 lowercase hex characters")
    if external_dsn:
        fixture = _external_disposable_postgres(external_dsn, token)
        try:
            yield fixture
        finally:
            fixture.cleanup()
        return

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
